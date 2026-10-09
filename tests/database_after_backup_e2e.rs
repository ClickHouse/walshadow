//! A source database created after the base backup must still be followed.
//!
//! The backup carries `global/pg_database` as of its own LSN, so a database
//! the source made later exists only in WAL past it. The bridge for that
//! database is wired during startup, while the record that creates it arrives
//! on the live stream — so the daemon has to stream first and pick the
//! database up when the shadow replays its creation, the way
//! `PendingDatabases` already does for a secondary database.

#![cfg(target_os = "linux")]

#[path = "common/bootstrap_ch_fixture.rs"]
mod fx;

use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use walrus::config::{Settings, StorageSettings, Vars};
use walrus::pg::backup::list;
use walrus::pg::backup::push::{self, PushArgs};
use walrus::pg::replication::conn::PgConfig;
use walrus::pg::wal;
use walshadow::mapping::TableTarget;
use walshadow::schema::RelName;
use walshadow::shadow::Shadow;

const LATE_DB: &str = "latecomer";

fn test_settings(storage_root: PathBuf) -> Settings {
    Settings {
        storage: StorageSettings::Fs {
            path: storage_root.display().to_string(),
        },
        ..Default::default()
    }
}

/// Same shape the object-store bootstrap expects: completed segments only
async fn push_completed_wal_segments(
    source: &Shadow,
    settings: &Settings,
    storage: walrus::storage::DynStorage,
) -> Result<()> {
    let wal_dir = source.config().data_dir.join("pg_wal");
    let mut names: Vec<String> = Vec::new();
    for entry in fs::read_dir(&wal_dir)? {
        let name = entry?.file_name().to_string_lossy().to_string();
        if name.len() == 24 && name.chars().all(|c| c.is_ascii_hexdigit()) {
            names.push(name);
        }
    }
    names.sort();
    for name in names {
        wal::push::handle(settings, storage.clone(), &wal_dir.join(&name))
            .await
            .with_context(|| format!("push {name}"))?;
    }
    Ok(())
}

/// A handle onto another database of the same cluster, since `psql_one`
/// addresses whatever its config names
fn in_database(source: &Shadow, dbname: &str) -> Shadow {
    let mut cfg = source.config().clone();
    cfg.dbname = dbname.to_owned();
    Shadow::new(cfg)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_database_created_after_the_backup_is_followed_once_replay_creates_it() {
    if !fx::tools::requirements_available() {
        return;
    }
    let slot = fx::Ports::alloc();
    let tmp = tempfile::tempdir().unwrap();

    let source = fx::start_source(&tmp);
    let _src_stop = fx::StopOnDrop { sh: &source };

    // 1. Back the cluster up before the database exists
    let storage_root = tmp.path().join("wal-g");
    fs::create_dir_all(&storage_root).unwrap();
    let storage: walrus::storage::DynStorage =
        Arc::new(walrus::storage::fs::FsStorage::new(&storage_root).unwrap());
    let settings = test_settings(storage_root.clone());
    let socket_host = source.config().socket_dir.to_str().unwrap().to_string();
    // SAFETY: single-test binary; the daemon subprocess takes these through
    // `Command::env` rather than by re-reading the parent env
    unsafe {
        std::env::set_var("PGHOST", &socket_host);
        std::env::set_var("PGPORT", source.config().port.to_string());
        std::env::set_var("PGUSER", "postgres");
        std::env::set_var("PGDATABASE", "postgres");
        std::env::remove_var("PGPASSWORD");
    }
    let cfg = PgConfig::resolve(&Vars::default()).expect("resolve source PgConfig");
    push::handle(&settings, storage.clone(), PushArgs::default(), cfg)
        .await
        .expect("base backup into storage");
    let backup_name = list::collect(storage.clone())
        .await
        .expect("list backups")
        .into_iter()
        .next()
        .expect("one backup on fresh storage")
        .name;

    // 2. Only now does the database exist, so it lives in WAL past the backup
    source
        .psql_one(&format!("CREATE DATABASE {LATE_DB}"))
        .expect("create the database after the backup");
    let late = in_database(&source, LATE_DB);
    late.psql_one(
        "CREATE SCHEMA s1; \
         CREATE TABLE s1.t (id int4 PRIMARY KEY, name text NOT NULL); \
         ALTER TABLE s1.t REPLICA IDENTITY FULL; \
         INSERT INTO s1.t VALUES (1, 'after-backup')",
    )
    .expect("populate the new database");
    source
        .psql_one("SELECT pg_switch_wal()")
        .expect("seal the segment holding CREATE DATABASE");
    push_completed_wal_segments(&source, &settings, storage.clone())
        .await
        .expect("archive the post-backup WAL");

    let ch_tmp = tempfile::tempdir().unwrap();
    let ch = fx::ChServer::spawn(ch_tmp, slot.ch_tcp, slot.ch_http).expect("spawn ch");
    fx::create_ch_dest_table(&ch, "default", "t").expect("create ch table");

    let ch_config_path = tmp.path().join("ch-config.toml");
    fx::write_ch_config_toml(
        &ch_config_path,
        "127.0.0.1",
        slot.ch_tcp,
        "default",
        &RelName::new("s1", "t"),
        &TableTarget::new("default", "t"),
    )
    .expect("write ch-config");
    let mut body = fs::read_to_string(&ch_config_path).expect("read ch-config");
    // The file wins over the CLI, so this is what selects the database the
    // backup does not hold
    body.push_str(&format!(
        "\n[source]\ndbname = \"{LATE_DB}\"\n\n[backup]\narchive = \"file://{}\"\n",
        storage_root.display()
    ));
    fs::write(&ch_config_path, body).expect("append [source] + [backup]");

    let daemon = fx::DaemonRun::prepare(tmp.path(), slot.metrics).expect("daemon layout");
    let child = daemon
        .spawn_mode(
            &source,
            &ch_config_path,
            slot.walsender,
            "object-store",
            &["--bootstrap-backup-name", &backup_name],
            &[
                ("PGHOST", socket_host.clone()),
                ("PGPORT", source.config().port.to_string()),
                ("PGUSER", "postgres".into()),
                ("PGDATABASE", LATE_DB.into()),
            ],
        )
        .expect("spawn walshadow-stream");
    let guard = fx::ChildGuard::new(child);

    let result = (|| -> Result<()> {
        fx::wait_for_listen(daemon.metrics_addr, Duration::from_secs(180))
            .context("daemon metrics endpoint never came up")?;
        // Reaching CH at all means the bridge attached, so the daemon got past
        // a shadow that did not hold the database when it started
        fx::wait_for_ch_value(
            &ch,
            "SELECT count() FROM default.t FINAL WHERE id = 1",
            "1",
            Duration::from_secs(180),
        )
        .context("row from the post-backup database never reached CH")?;
        late.psql_one("INSERT INTO s1.t VALUES (2, 'post-boot')")
            .context("post-boot insert")?;
        fx::wait_for_ch_value(
            &ch,
            "SELECT count() FROM default.t FINAL WHERE _is_deleted = 0",
            "2",
            Duration::from_secs(90),
        )
        .context("live CDC on the post-backup database never reached CH")?;
        Ok(())
    })();

    fx::finish_daemon(guard, &daemon, result);
}
