//! WAL the `BASE_BACKUP` tar carries must not reach the shadow unfiltered.
//!
//! `tests/shadow_stays_catalog_scale_e2e.rs` proves the streaming filter keeps
//! the shadow catalog-scale, but it leaves the source quiescent while the
//! backup runs, so its tar carries almost no WAL. Direct mode requests
//! `wal = true`, those segments land in the shadow's `pg_wal/` verbatim, and
//! the shadow's own recovery replays them to reach consistency — before
//! walshadow's walsender exists and without passing the filter. Every
//! user-heap record in the backup window then redoes into `base/`, which is
//! how a deployed shadow grows without a single record reaching the filter.
//!
//! Writes throughout the backup window, then asserts the user relation is
//! still absent from the shadow and the landed-WAL pass actually had records
//! to drop.

#![cfg(target_os = "linux")]

#[path = "common/bootstrap_ch_fixture.rs"]
mod fx;

use std::fs;
use std::io::Write as _;
use std::path::Path;
use std::process::Command;
use std::time::Duration;

use anyhow::{Context, Result};
use walshadow::shadow::Shadow;

const ROWS: i32 = 30_000;
/// 4 MB/s holds the backup open long enough to write a real window
const MAX_RATE_KIB: &str = "4096";

/// `Shadow::initdb` plus `-k`: checksums make every hint-bit set log a page
/// image, the deployed source's configuration
fn initdb_with_checksums(sh: &Shadow) {
    let cfg = sh.config();
    fs::create_dir_all(cfg.data_dir.parent().unwrap()).unwrap();
    fs::create_dir_all(&cfg.socket_dir).unwrap();
    let out = Command::new("initdb")
        .args([
            "-D",
            cfg.data_dir.to_str().unwrap(),
            "-U",
            cfg.user.as_str(),
            "--auth=trust",
            "--encoding=UTF8",
            "--locale=C",
            "--no-instructions",
            "-k",
        ])
        .output()
        .expect("spawn initdb");
    assert!(
        out.status.success(),
        "initdb -k: {}",
        String::from_utf8_lossy(&out.stderr)
    );
}

fn append_hint_conf(sh: &Shadow) {
    let path = sh.config().data_dir.join("postgresql.conf");
    let mut f = fs::OpenOptions::new().append(true).open(&path).unwrap();
    writeln!(f, "\nwal_keep_size = 2GB").unwrap();
}

fn scalar(sh: &Shadow, sql: &str) -> u32 {
    sh.psql_one(sql)
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .trim()
        .parse()
        .unwrap_or_else(|e| panic!("{sql}: not an integer: {e}"))
}

/// Bytes the shadow holds for `node`, across every segment and fork
fn shadow_bytes(shadow_data: &Path, db: u32, node: u32) -> u64 {
    let dir = shadow_data.join("base").join(db.to_string());
    let Ok(rd) = fs::read_dir(&dir) else {
        return 0;
    };
    let want = node.to_string();
    rd.flatten()
        .filter(|e| {
            let name = e.file_name().to_string_lossy().into_owned();
            name.split(['.', '_']).next() == Some(want.as_str())
        })
        .map(|e| e.metadata().map(|m| m.len()).unwrap_or(0))
        .sum()
}

/// `dropped=N` off the daemon's `landed WAL filtered` line
fn dropped_from_log(log: &str) -> Option<u64> {
    let line = log.lines().find(|l| l.contains("landed WAL filtered"))?;
    let rest = line.split("dropped=").nth(1)?;
    rest.split(|c: char| !c.is_ascii_digit())
        .next()?
        .parse()
        .ok()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn backup_window_wal_never_materialises_the_shadow() {
    if !fx::requirements_available() {
        return;
    }

    let slot = fx::Ports::alloc();
    let tmp = tempfile::tempdir().unwrap();

    let source = fx::make_source(&tmp);
    initdb_with_checksums(&source);
    source.write_base_conf().expect("source base conf");
    fx::append_source_conf(&source).expect("append source conf");
    append_hint_conf(&source);
    source.start().expect("start source");
    let _src_stop = fx::StopOnDrop { sh: &source };
    source
        .apply_schema_dump(&format!(
            "CREATE TABLE public.big(id int PRIMARY KEY, payload text);\n\
             INSERT INTO public.big SELECT g, repeat('x', 300) FROM generate_series(1, {ROWS}) g;\n\
             CHECKPOINT;\nSELECT pg_switch_wal();\n"
        ))
        .expect("load pre-boot rows");

    let db = scalar(
        &source,
        "SELECT oid::int8 FROM pg_database WHERE datname = current_database()",
    );
    let node = scalar(&source, "SELECT pg_relation_filenode('public.big')::int8");

    let ch_tmp = tempfile::tempdir().unwrap();
    let ch = fx::ChServer::spawn(ch_tmp, slot.ch_tcp, slot.ch_http).expect("spawn ch");
    let ch_config_path = tmp.path().join("ch-config.toml");
    fs::write(
        &ch_config_path,
        format!(
            "[ch]\nhost = \"127.0.0.1\"\nport = {}\ndatabase = \"default\"\n\
             compression = \"lz4\"\n\n\
             [table.\"public\".\"big\"]\nreplicate = true\n",
            slot.ch_tcp
        ),
    )
    .expect("write ch-config");

    let daemon = fx::DaemonRun::prepare(tmp.path(), slot.metrics).expect("daemon layout");
    let child = daemon
        .spawn(
            &source,
            &ch_config_path,
            slot.walsender,
            &[
                "--bootstrap-max-rate-kib",
                MAX_RATE_KIB,
                "--bridge-lib-dir",
                fx::pgext_dir().to_str().unwrap(),
            ],
        )
        .expect("spawn walshadow-stream");
    let guard = fx::ChildGuard::new(child);

    let result = (|| -> Result<()> {
        fx::wait_for_backup_streaming(&source, Duration::from_secs(120))
            .context("BASE_BACKUP never opened")?;

        // Dirty distinct heap pages for as long as the backup runs. With
        // checksums on, each first touch since the checkpoint carries a page
        // image, so this is the exact WAL shape that re-materialises a shadow.
        let mut round = 0i32;
        while fx::backup_in_progress(&source) {
            let from = (round * 500) % ROWS + 1;
            source
                .apply_schema_dump(&format!(
                    "UPDATE public.big SET payload = repeat('z', 300) \
                       WHERE id BETWEEN {from} AND {};\n",
                    from + 499,
                ))
                .context("in-window writes")?;
            round += 1;
            std::thread::sleep(Duration::from_millis(50));
        }
        anyhow::ensure!(
            round > 0,
            "backup closed before a single in-window write landed; nothing to prove"
        );

        source
            .apply_schema_dump(
                "INSERT INTO public.big VALUES (2000001, 'marker');\n\
                 SELECT pg_switch_wal();\n",
            )
            .context("post-window marker")?;

        fx::wait_for_listen(daemon.metrics_addr, Duration::from_secs(120))
            .context("daemon metrics endpoint never came up")?;
        // The marker proves the pipeline consumed past the window, so the
        // byte assertions below are about a bootstrap that actually finished
        fx::wait_for_ch_value(
            &ch,
            "SELECT count() FROM default.big FINAL WHERE id = 2000001",
            "1",
            Duration::from_secs(180),
        )
        .context("post-window marker never reached CH")?;
        let src_count = source
            .psql_one("SELECT count(*) FROM public.big")
            .context("source count")?;
        fx::wait_for_ch_value(
            &ch,
            "SELECT count() FROM default.big FINAL WHERE _is_deleted = 0",
            src_count.trim(),
            Duration::from_secs(180),
        )
        .context("CH never matched the source row count")?;

        daemon
            .wait_for_log("landed WAL filtered", Duration::from_secs(30))
            .context("bootstrap never ran the landed-WAL pass")?;
        let dropped = dropped_from_log(&daemon.stderr())
            .context("landed WAL filtered line carries no dropped= count")?;
        anyhow::ensure!(
            dropped > 0,
            "the backup's WAL held no droppable records, so a clean shadow \
             proves nothing about filtering it"
        );

        // Positive control: the same measurement over a catalog must be
        // nonzero, or a zero for the user relation distinguishes nothing
        let catalog_node = scalar(&source, "SELECT pg_relation_filenode('pg_class')::int8");
        let catalog_bytes = shadow_bytes(&daemon.shadow_data_dir, db, catalog_node);
        anyhow::ensure!(
            catalog_bytes > 0,
            "pg_class (relfilenode {catalog_node}) reads 0 bytes on the shadow"
        );

        let bytes = shadow_bytes(&daemon.shadow_data_dir, db, node);
        anyhow::ensure!(
            bytes == 0,
            "relfilenode {node} materialised {bytes} bytes ({} pages) on the shadow; \
             redo replayed backup-window WAL naming its blocks",
            bytes / 8192,
        );
        Ok(())
    })();

    fx::finish_daemon(guard, &daemon, result);
}
