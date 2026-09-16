//! An `object_store` initial load that was interrupted re-extracts itself.
//!
//! `bootstrap_object_store_ch.rs` covers the clean run. This one seeds the
//! state a killed or OOM-ed bootstrap leaves — an incomplete marker, a
//! half-landed data dir, a stale visibility-gate spool — and asserts the
//! daemon discards it and loads the pinned backup without an operator.
//!
//! Seeded rather than killed on purpose: killing mid-extraction makes *where*
//! it dies the variable, and the resumable state is the same either way.
//!
//! A second, newer backup lands before the retry and the daemon is configured
//! with `LATEST`, so the run only passes if the pin beats the configuration.

#![cfg(target_os = "linux")]

#[path = "common/bootstrap_ch_fixture.rs"]
mod fx;

use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use walrus::compression;
use walrus::config::{Settings, StorageSettings, Vars};
use walrus::pg::backup::list;
use walrus::pg::backup::push::{self, PushArgs};
use walrus::pg::replication::conn::PgConfig;
use walrus::pg::wal;
use walrus::storage::DynStorage;
use walrus::storage::fs::FsStorage;
use walshadow::bootstrap_marker::{BootstrapMarker, MARKER_FILENAME};
use walshadow::mapping::TableTarget;
use walshadow::schema::RelName;
use walshadow::shadow::Shadow;

const N_ROWS: i32 = 64;

async fn push_completed_wal_segments(
    source: &Shadow,
    settings: &Settings,
    storage: DynStorage,
) -> Result<()> {
    let pg_wal = source.config().data_dir.join("pg_wal");
    for entry in fs::read_dir(&pg_wal).with_context(|| format!("read_dir {}", pg_wal.display()))? {
        let entry = entry?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        if name.len() != 24 || !name.chars().all(|c| c.is_ascii_hexdigit()) {
            continue;
        }
        let path = entry.path();
        wal::push::handle(settings, storage.clone(), &path)
            .await
            .with_context(|| format!("wal::push::handle {}", path.display()))?;
    }
    Ok(())
}

fn test_settings(storage_root: PathBuf) -> Settings {
    Settings {
        storage: StorageSettings::Fs {
            path: storage_root.to_string_lossy().into_owned(),
        },
        compression: compression::Method::None,
        compression_level: 0,
        ..Default::default()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn incomplete_object_store_bootstrap_retries_itself() {
    if !fx::requirements_available() {
        return;
    }

    let slot = fx::Ports::alloc();
    let tmp = tempfile::tempdir().unwrap();

    let source = fx::start_source(&tmp);
    let _src_stop = fx::StopOnDrop { sh: &source };
    fx::load_source_workload(&source, "s14", N_ROWS).expect("load source workload");

    let storage_root = tmp.path().join("wal-g");
    fs::create_dir_all(&storage_root).unwrap();
    let storage: DynStorage = Arc::new(FsStorage::new(&storage_root).unwrap());
    let settings = test_settings(storage_root.clone());

    let socket_host = source.config().socket_dir.to_str().unwrap().to_string();
    // SAFETY: single-test binary, so no other thread reads these while set;
    // the daemon receives them through `Command::env`
    unsafe {
        std::env::set_var("PGHOST", &socket_host);
        std::env::set_var("PGPORT", source.config().port.to_string());
        std::env::set_var("PGUSER", "postgres");
        std::env::set_var("PGDATABASE", "postgres");
        std::env::remove_var("PGPASSWORD");
    }
    let cfg = PgConfig::resolve(&Vars::default()).expect("resolve source PgConfig from libpq env");
    push::handle(&settings, storage.clone(), PushArgs::default(), cfg.clone())
        .await
        .expect("wal-rus push::handle against source PG");
    source
        .psql_one("SELECT pg_switch_wal()")
        .expect("force WAL rotation post-basebackup");
    push_completed_wal_segments(&source, &settings, storage.clone())
        .await
        .expect("push WAL segments to storage");

    let backup_name = list::collect(storage.clone())
        .await
        .expect("list backups on FsStorage")
        .into_iter()
        .next()
        .expect("one backup on fresh storage")
        .name;

    push::handle(&settings, storage.clone(), PushArgs::default(), cfg)
        .await
        .expect("push a newer backup for LATEST to resolve to");
    let latest = walrus::pg::backup::fetch::resolve_name(&storage, "LATEST")
        .await
        .expect("resolve latest backup");
    assert_ne!(latest, backup_name, "LATEST must have moved off the pin");
    source.psql_one("SELECT pg_switch_wal()").unwrap();
    push_completed_wal_segments(&source, &settings, storage.clone())
        .await
        .unwrap();

    let ch_tmp = tempfile::tempdir().unwrap();
    let ch = fx::ChServer::spawn(ch_tmp, slot.ch_tcp, slot.ch_http).expect("spawn ch");
    fx::create_ch_dest_table(&ch, "default", "t").expect("create ch table");

    let ch_config_path = tmp.path().join("ch-config.toml");
    fx::write_ch_config_toml(
        &ch_config_path,
        "127.0.0.1",
        slot.ch_tcp,
        "default",
        &RelName::new("s14", "t"),
        &TableTarget::new("default", "t"),
    )
    .expect("write ch-config");
    let archive_uri = format!("file://{}", storage_root.display());
    let mut ch_config_body = fs::read_to_string(&ch_config_path).expect("read ch-config");
    ch_config_body.push_str(&format!("\n[backup]\narchive = \"{archive_uri}\"\n"));
    fs::write(&ch_config_path, ch_config_body).expect("append [backup] to ch-config");

    let daemon = fx::DaemonRun::prepare(tmp.path(), slot.metrics).expect("daemon layout");

    // What a killed attempt leaves: the marker, a data dir far enough along
    // to hold a PG_VERSION, and a gate spool the retry must not inherit.
    // `attempts = 1` with the backup pinned is what the first attempt wrote
    fs::create_dir_all(daemon.shadow_data_dir.join("base")).unwrap();
    fs::write(daemon.shadow_data_dir.join("PG_VERSION"), b"17\n").unwrap();
    fs::write(
        daemon.shadow_data_dir.join(MARKER_FILENAME),
        format!("attempts = 1\nbackup_name = \"{backup_name}\"\n"),
    )
    .unwrap();
    let stale_spool = daemon.spill_dir.join("bootstrap_gate_deferred.0.bin");
    fs::write(&stale_spool, b"stale").unwrap();

    let child = daemon
        .spawn_mode(
            &source,
            &ch_config_path,
            slot.walsender,
            "object-store",
            &["--bootstrap-backup-name", "LATEST"],
            &[
                ("PGHOST", socket_host.clone()),
                ("PGPORT", source.config().port.to_string()),
                ("PGUSER", "postgres".into()),
                ("PGDATABASE", "postgres".into()),
            ],
        )
        .expect("spawn walshadow-stream");
    let guard = fx::ChildGuard::new(child);

    let result = (|| -> Result<()> {
        fx::wait_for_listen(daemon.metrics_addr, Duration::from_secs(30))
            .context("daemon metrics endpoint never came up")?;

        let src_count = source
            .psql_one("SELECT count(*) FROM s14.t")
            .context("source count")?;
        fx::wait_for_ch_value(
            &ch,
            "SELECT count() FROM default.t FINAL WHERE _is_deleted = 0",
            &src_count,
            Duration::from_secs(60),
        )?;
        fx::assert_ch_matches_source(&ch, &source, "s14.t", "default.t")
            .context("source vs CH parity after the retry")?;

        let deadline = std::time::Instant::now() + Duration::from_secs(60);
        while BootstrapMarker::read(&daemon.shadow_data_dir)?.is_some() {
            anyhow::ensure!(
                std::time::Instant::now() < deadline,
                "marker outlived a bootstrap that completed",
            );
            std::thread::sleep(Duration::from_millis(20));
        }
        let log = daemon.stderr();
        anyhow::ensure!(
            log.contains("discarding an incomplete bootstrap"),
            "daemon did not report the retry:\n{log}",
        );
        anyhow::ensure!(
            log.lines()
                .any(|line| line.contains("fetching") && line.contains(&backup_name)),
            "retry fetched something other than the pinned backup:\n{log}",
        );
        anyhow::ensure!(
            !stale_spool.exists(),
            "the previous attempt's gate spool survived the retry",
        );
        Ok(())
    })();

    fx::finish_daemon(guard, &daemon, result);
}
