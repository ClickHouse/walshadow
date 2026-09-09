//! The shadow's data dir must stay catalog-scale while a user relation churns.
//!
//! Routing assertions are not enough. `tests/fpi_user_pages.rs` proves user page
//! images are classified away from the shadow, but it runs no shadow, so it
//! cannot see redo materialise a relation. Anything the shadow replays that
//! names a high block of a user relation extends that relation's file out to
//! that block and zero-fills the gap, so one such record is worth gigabytes on
//! a real table — which is why routing being correct does not settle the
//! question.
//!
//! Drives the real daemon against a real shadow and asserts the outcome: after a
//! hint-bit scan, a VACUUM and a VACUUM FULL on the source, the user relation's
//! files on the shadow are still absent or empty.
//!
//! Mirrors the deployment that keeps re-materialising: the source runs with
//! `data_checksums = on` (so every hint-bit set logs a page image) and the
//! table opts into `initial_load = "base_backup"`, which backfills through a
//! separate pass rather than the bootstrap page walk.

#![cfg(target_os = "linux")]

#[path = "common/bootstrap_ch_fixture.rs"]
mod fx;

use std::fs;
use std::io::Write as _;
use std::net::SocketAddr;
use std::os::unix::process::CommandExt;
use std::path::Path;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use walshadow::shadow::{Shadow, ShadowConfig};

const ROWS: i32 = 20_000;

fn make_source(tmp: &tempfile::TempDir) -> Shadow {
    let mut cfg = ShadowConfig::new(
        tmp.path().join("source-data"),
        tmp.path().join("source-filtered"),
    );
    cfg.port = fx::PG_SOURCE_PORT;
    cfg.socket_dir = tmp.path().join("source-sock");
    cfg.ctl_timeout = Duration::from_secs(60);
    fs::create_dir_all(&cfg.filter_out_dir).unwrap();
    fs::create_dir_all(&cfg.socket_dir).unwrap();
    Shadow::new(cfg)
}

/// `Shadow::initdb` plus `-k`: checksums are what make hint-bit sets emit page
/// images on the deployed source, and the shadow inherits them via base backup
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

/// The retention floor keeps VACUUM FULL's checkpoint from recycling the
/// slotless pump's segments
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

fn write_config(path: &Path, ch_port: u16) -> Result<()> {
    let body = format!(
        "[ch]\nhost = \"127.0.0.1\"\nport = {ch_port}\ndatabase = \"default\"\n\
         compression = \"lz4\"\n\n\
         [table.\"public\".\"big\"]\nreplicate = true\ninitial_load = \"base_backup\"\n"
    );
    fs::write(path, body).context("write ch-config")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn user_relation_never_materialises_on_the_shadow() {
    if !fx::pg_available() || !fx::pg_basebackup_available() || !fx::clickhouse_available() {
        eprintln!("skip: missing initdb / pg_basebackup / clickhouse");
        return;
    }

    let slot = fx::Ports::alloc();
    let tmp = tempfile::tempdir().unwrap();

    let source = make_source(&tmp);
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
    let node_before = scalar(&source, "SELECT pg_relation_filenode('public.big')::int8");

    let ch_tmp = tempfile::tempdir().unwrap();
    let ch = fx::ChServer::spawn(ch_tmp, slot.ch_tcp, slot.ch_http).expect("spawn ch");
    let ch_config_path = tmp.path().join("ch-config.toml");
    write_config(&ch_config_path, slot.ch_tcp).expect("write ch-config");

    let bootstrap_shadow_data_dir = tmp.path().join("shadow-data");
    let shadow_sock = tmp.path().join("shadow-sock");
    fs::create_dir_all(&shadow_sock).unwrap();
    let shadow_filter_dir = tmp.path().join("filtered");
    fs::create_dir_all(&shadow_filter_dir).unwrap();
    let spill_dir = tmp.path().join("spill");
    fs::create_dir_all(&spill_dir).unwrap();

    let bin = env!("CARGO_BIN_EXE_walshadow-stream");
    let stderr_path = tmp.path().join("daemon.stderr.log");
    let stderr_file = fs::File::create(&stderr_path).expect("open daemon stderr log");
    let metrics_addr: SocketAddr = format!("127.0.0.1:{}", slot.metrics).parse().unwrap();
    let child = Command::new(bin)
        .args([
            "--host",
            source.config().socket_dir.to_str().unwrap(),
            "--port",
            &fx::PG_SOURCE_PORT.to_string(),
            "--user",
            "postgres",
            "--dbname",
            "postgres",
            "--sslmode",
            "disable",
            "--out-dir",
            shadow_filter_dir.to_str().unwrap(),
            "--shadow-socket-dir",
            shadow_sock.to_str().unwrap(),
            "--shadow-port",
            &fx::PG_SHADOW_PORT.to_string(),
            "--shadow-user",
            "postgres",
            "--bridge-lib-dir",
            fx::pgext_dir().to_str().unwrap(),
            "--spill-dir",
            spill_dir.to_str().unwrap(),
            "--status-interval",
            "1",
            "--metrics-bind",
            &metrics_addr.to_string(),
            "--walsender-bind",
            &format!("127.0.0.1:{}", slot.walsender),
            "--retention-bytes",
            "0",
            "--ch-config",
            ch_config_path.to_str().unwrap(),
            "--bootstrap-mode",
            "direct",
            "--bootstrap-shadow-data-dir",
            bootstrap_shadow_data_dir.to_str().unwrap(),
            "--bootstrap-shadow-replay-timeout",
            "120",
        ])
        .env("RUST_LOG", "warn,walshadow=info")
        .stdout(Stdio::null())
        .stderr(Stdio::from(stderr_file))
        .process_group(0)
        .spawn()
        .expect("spawn walshadow-stream");
    let guard = fx::ChildGuard::new(child);

    let result = (|| -> Result<()> {
        fx::wait_for_listen(metrics_addr, Duration::from_secs(120))
            .context("daemon never bound its metrics endpoint")?;

        // Bootstrap page-walked the rows to CH without landing the heap
        let deadline = Instant::now() + Duration::from_secs(180);
        loop {
            let n = ch
                .query("SELECT count() FROM default.big")
                .unwrap_or_default();
            if n == ROWS.to_string() {
                break;
            }
            anyhow::ensure!(
                Instant::now() < deadline,
                "bootstrap rows never reached CH (count={n:?})"
            );
            std::thread::sleep(Duration::from_millis(250));
        }
        // Positive control: the same measurement over a catalog must be
        // nonzero, or a zero for the user relation proves nothing
        let catalog_node = scalar(&source, "SELECT pg_relation_filenode('pg_class')::int8");
        let catalog_bytes = shadow_bytes(&bootstrap_shadow_data_dir, db, catalog_node);
        anyhow::ensure!(
            catalog_bytes > 0,
            "pg_class (relfilenode {catalog_node}) reads 0 bytes on the shadow, so the \
             user-relation assertions below cannot distinguish anything"
        );

        let after_boot = shadow_bytes(&bootstrap_shadow_data_dir, db, node_before);
        anyhow::ensure!(
            after_boot == 0,
            "bootstrap landed {after_boot} bytes of the user relation on the shadow"
        );

        // Hint bits, then the two vacuum shapes seen re-materialising a
        // deployed shadow. VACUUM FULL rewrites into a fresh relfilenode.
        source
            .apply_schema_dump(
                "CHECKPOINT;\n\
                 SELECT count(*) FROM public.big;\n\
                 UPDATE public.big SET payload = repeat('y', 300) WHERE id % 4 = 0;\n\
                 DELETE FROM public.big WHERE id % 7 = 0;\n\
                 CHECKPOINT;\n\
                 VACUUM (FREEZE) public.big;\n\
                 VACUUM FULL public.big;\n\
                 INSERT INTO public.big VALUES (2000001, 'marker');\n\
                 SELECT pg_switch_wal();\n",
            )
            .context("churn workload")?;
        let node_after = scalar(&source, "SELECT pg_relation_filenode('public.big')::int8");

        // The marker proves the pipeline consumed past the vacuums, so the
        // byte assertions below are about a stream that actually arrived
        let deadline = Instant::now() + Duration::from_secs(180);
        loop {
            let n = ch
                .query("SELECT count() FROM default.big FINAL WHERE id = 2000001")
                .unwrap_or_default();
            if n == "1" {
                break;
            }
            anyhow::ensure!(
                Instant::now() < deadline,
                "post-vacuum marker never reached CH (count={n:?})"
            );
            std::thread::sleep(Duration::from_millis(250));
        }

        for (label, node) in [("pre-vacuum", node_before), ("post-vacuum", node_after)] {
            let bytes = shadow_bytes(&bootstrap_shadow_data_dir, db, node);
            anyhow::ensure!(
                bytes == 0,
                "{label} relfilenode {node} materialised {bytes} bytes on the shadow \
                 ({} pages); redo replayed something naming its blocks",
                bytes / 8192,
            );
        }
        Ok(())
    })();

    let _ = guard.into_inner().map(|mut c| {
        let _ = c.kill();
        let _ = c.wait();
    });
    if bootstrap_shadow_data_dir.join("postmaster.pid").exists() {
        let mut shadow_cfg =
            ShadowConfig::new(bootstrap_shadow_data_dir.clone(), shadow_filter_dir.clone());
        shadow_cfg.port = fx::PG_SHADOW_PORT;
        shadow_cfg.socket_dir = shadow_sock.clone();
        shadow_cfg.ctl_timeout = Duration::from_secs(60);
        let _ = Shadow::new(shadow_cfg).stop();
    }
    let _ = &ch;

    if let Err(e) = result {
        if let Ok(keep) = std::env::var("WS_KEEP_ARTIFACTS") {
            let _ = fs::create_dir_all(&keep);
            let _ = Command::new("cp")
                .args(["-a", shadow_filter_dir.to_str().unwrap(), &keep])
                .status();
            let _ = fs::copy(&stderr_path, Path::new(&keep).join("daemon.stderr.log"));
            eprintln!("artifacts kept in {keep}");
        }
        let stderr = fs::read_to_string(&stderr_path).unwrap_or_default();
        panic!("{e:#}\n--- daemon stderr ---\n{stderr}");
    }
}
