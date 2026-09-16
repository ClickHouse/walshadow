//! Kill walshadow partway through an `object_store` initial load, restart it,
//! and require the load to finish with ClickHouse matching the source.
//!
//! Sibling of `bootstrap_object_store_retry_ch.rs`, which seeds the
//! interrupted state. This one produces it the way production does — SIGKILL
//! to a daemon mid-bootstrap, leaving a real partial data dir, a real marker,
//! and whatever rows the abandoned attempt already inserted.
//!
//! The row count is large enough that the marker is still on disk when the
//! kill lands: it clears only after extraction, ClickHouse durability, WAL
//! hydration, window replay and the visibility gate have all succeeded.

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
use walshadow::bootstrap_marker::BootstrapMarker;
use walshadow::mapping::TableTarget;
use walshadow::schema::RelName;
use walshadow::shadow::Shadow;

const N_ROWS: i32 = 300_000;

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
async fn killed_object_store_bootstrap_finishes_after_restart() {
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
    // SAFETY: single-test binary, so nothing else reads these while set; the
    // daemon receives them through `Command::env`
    unsafe {
        std::env::set_var("PGHOST", &socket_host);
        std::env::set_var("PGPORT", source.config().port.to_string());
        std::env::set_var("PGUSER", "postgres");
        std::env::set_var("PGDATABASE", "postgres");
        std::env::remove_var("PGPASSWORD");
    }
    let cfg = PgConfig::resolve(&Vars::default()).expect("resolve source PgConfig from libpq env");
    push::handle(&settings, storage.clone(), PushArgs::default(), cfg)
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
    let env = [
        ("PGHOST", socket_host.clone()),
        ("PGPORT", source.config().port.to_string()),
        ("PGUSER", "postgres".to_string()),
        ("PGDATABASE", "postgres".to_string()),
    ];

    let first = daemon
        .spawn_mode(
            &source,
            &ch_config_path,
            slot.walsender,
            "object-store",
            &["--bootstrap-backup-name", &backup_name],
            &env,
        )
        .expect("spawn walshadow-stream");
    let first = fx::ChildGuard::new(first);

    daemon
        .wait_for_log("draining tar partitions", Duration::from_secs(60))
        .expect("first run never reached extraction");
    let mid_flight = BootstrapMarker::read(&daemon.shadow_data_dir);

    let mut child = first.into_inner().expect("first run still held its child");
    child.kill().expect("SIGKILL the daemon mid-bootstrap");
    child.wait().expect("reap the killed daemon");
    daemon.stop_shadow();

    let mid_flight = mid_flight.expect(
        "killed before the marker appeared; the bootstrap is too small to catch \
         mid-flight, raise N_ROWS",
    );
    assert_eq!(mid_flight.attempts, 1, "first run is attempt 1");
    assert_eq!(
        mid_flight.backup_name.as_deref(),
        Some(backup_name.as_str()),
        "the first attempt must pin what it resolved, so the retry reloads it",
    );

    let second = daemon
        .spawn_mode(
            &source,
            &ch_config_path,
            slot.walsender,
            "object-store",
            &["--bootstrap-backup-name", &backup_name],
            &env,
        )
        .expect("respawn walshadow-stream");
    let guard = fx::ChildGuard::new(second);

    let result = (|| -> Result<()> {
        fx::wait_for_listen(daemon.metrics_addr, Duration::from_secs(60))
            .context("restarted daemon never came up")?;

        let src_count = source
            .psql_one("SELECT count(*) FROM s14.t")
            .context("source count")?;
        fx::wait_for_ch_value(
            &ch,
            "SELECT count() FROM default.t FINAL WHERE _is_deleted = 0",
            &src_count,
            Duration::from_secs(180),
        )?;
        fx::assert_ch_matches_source(&ch, &source, "s14.t", "default.t")
            .context("source vs CH parity after the crash")?;

        let log = daemon.stderr();
        anyhow::ensure!(
            log.contains("discarding an incomplete bootstrap"),
            "restart did not re-extract; it took some other path:\n{log}",
        );
        anyhow::ensure!(
            BootstrapMarker::read(&daemon.shadow_data_dir).is_none(),
            "marker outlived a bootstrap that completed",
        );
        Ok(())
    })();

    fx::finish_daemon(guard, &daemon, result);
}
