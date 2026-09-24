//! A source already on timeline >= 2 must still feed the shadow.
//!
//! walshadow's walsender answers `TIMELINE_HISTORY` out of
//! `ShadowStreamState::histories`, which only `advertise_timeline` writes — and
//! only for branches this walsender *crossed onto*. The branch it boots on has
//! no history bytes, while `IDENTIFY_SYSTEM` names that very branch. PG's
//! walreceiver fetches history for every timeline in `[its own, the primary's]`
//! it lacks locally (`WalRcvFetchTimeLineHistoryFiles`), so it asks, gets
//! `could not open file "pg_wal/000000NN.history"`, dies FATAL, and redials
//! every `wal_retrieve_retry_interval` forever.
//!
//! Object-store mode is where this bites. Direct mode requests `wal = true`, so
//! the tar carries the source's `pg_wal/` — history files included, which
//! `DiskLanderSink` keeps — and the shadow never has to ask. The object-store
//! leg hydrates through `fetch_wal_into_pg_wal`, which enumerates
//! `segments_covering(..)` and fetches segments only, so the shadow lands
//! without any `.history` and asks on its first connection.

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
use walshadow::mapping::TableTarget;
use walshadow::schema::RelName;
use walshadow::shadow::Shadow;

const N_ROWS: i32 = 64;

/// Standby-signal + `pg_promote` bumps a standalone cluster one timeline and
/// writes the matching `.history`, with no second cluster to stream from
fn promote_once(sh: &Shadow) -> Result<()> {
    sh.stop().context("stop source before promotion")?;
    sh.write_standby_signal().context("write standby.signal")?;
    sh.start().context("restart source in standby mode")?;
    sh.psql_one("SELECT pg_promote(true, 60)")
        .context("pg_promote")?;
    Ok(())
}

fn timeline_of(sh: &Shadow) -> u32 {
    sh.psql_one("SELECT timeline_id FROM pg_control_checkpoint()")
        .expect("read source timeline")
        .trim()
        .parse()
        .expect("timeline is an integer")
}

/// Archive every completed segment, as `archive_command` would. History files
/// are deliberately not pushed here: the hydrate never asks for them, so
/// whether the archive holds them changes nothing
async fn push_completed_wal_segments(
    source: &Shadow,
    settings: &Settings,
    storage: DynStorage,
) -> Result<()> {
    let current = source
        .psql_one("SELECT pg_walfile_name(pg_current_wal_insert_lsn())")
        .context("read the in-progress segment")?;
    let current = current.trim();
    let pg_wal = source.config().data_dir.join("pg_wal");
    for entry in fs::read_dir(&pg_wal).with_context(|| format!("read_dir {}", pg_wal.display()))? {
        let entry = entry?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        if name.len() != 24 || !name.chars().all(|c| c.is_ascii_hexdigit()) {
            continue;
        }
        if name[8..] >= current[8..] {
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
async fn shadow_streams_from_a_source_booted_on_a_promoted_timeline() {
    if !fx::requirements_available() {
        return;
    }

    let slot = fx::Ports::alloc();
    let tmp = tempfile::tempdir().unwrap();

    let source = fx::start_source(&tmp);
    let _src_stop = fx::StopOnDrop { sh: &source };

    // Two promotions before walshadow ever connects: the daemon boots straight
    // onto timeline 3 with no crossing of its own to observe
    for _ in 0..2 {
        promote_once(&source).expect("promote source");
    }
    source
        .psql_one("CHECKPOINT")
        .expect("checkpoint promoted source");
    let tli = timeline_of(&source);
    assert_eq!(
        tli, 3,
        "test needs a source on timeline 3; promotion left it on {tli}"
    );

    fx::load_source_workload(&source, "s1", N_ROWS).expect("load source workload");

    let storage_root = tmp.path().join("wal-g");
    fs::create_dir_all(&storage_root).unwrap();
    let storage: DynStorage = Arc::new(FsStorage::new(&storage_root).unwrap());
    let settings = test_settings(storage_root.clone());

    let socket_host = source.config().socket_dir.to_str().unwrap().to_string();
    // SAFETY: single-test binary; the daemon subprocess receives these through
    // `Command::env` rather than by re-reading the parent env
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
        &RelName::new("s1", "t"),
        &TableTarget::new("default", "t"),
    )
    .expect("write ch-config");
    let mut body = fs::read_to_string(&ch_config_path).expect("read ch-config");
    body.push_str(&format!(
        "\n[backup]\narchive = \"file://{}\"\n",
        storage_root.display()
    ));
    fs::write(&ch_config_path, body).expect("append [backup] to ch-config");

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
                ("PGDATABASE", "postgres".into()),
            ],
        )
        .expect("spawn walshadow-stream");
    let guard = fx::ChildGuard::new(child);

    let result = (|| -> Result<()> {
        fx::wait_for_listen(daemon.metrics_addr, Duration::from_secs(180))
            .context("daemon metrics endpoint never came up")?;
        fx::wait_for_ch_value(
            &ch,
            "SELECT count() FROM default.t FINAL WHERE _is_deleted = 0",
            &N_ROWS.to_string(),
            Duration::from_secs(180),
        )
        .context("bootstrap rows never reached CH")?;

        // Live CDC is the discriminator: the bootstrap page walk reaches CH
        // without the shadow streaming at all, but a post-boot row needs the
        // shadow's catalog to advance, which needs its walreceiver to survive
        // the handshake against walshadow's walsender
        source
            .apply_schema_dump("INSERT INTO s1.t VALUES (900001, 'post-boot');\n")
            .context("post-boot insert")?;
        let streamed = fx::wait_for_ch_value(
            &ch,
            "SELECT count() FROM default.t FINAL WHERE id = 900001",
            "1",
            Duration::from_secs(90),
        );
        if streamed.is_err() {
            let log = daemon.stderr();
            let handshakes = log
                .lines()
                .filter(|l| l.contains("client closed during startup"))
                .count();
            anyhow::bail!(
                "post-boot row never reached CH; the shadow's walreceiver failed its \
                 handshake {handshakes} times. walshadow advertises timeline {tli} but \
                 seeds no history for the branch it booted on, so TIMELINE_HISTORY \
                 {tli} answers `could not open file`",
            );
        }
        Ok(())
    })();

    fx::finish_daemon(guard, &daemon, result);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bootstrap_from_an_ancestor_backup_crosses_onto_the_live_timeline() {
    if !fx::requirements_available() {
        return;
    }

    let slot = fx::Ports::alloc();
    let tmp = tempfile::tempdir().unwrap();

    let source = fx::start_source(&tmp);
    let _src_stop = fx::StopOnDrop { sh: &source };
    fx::load_source_workload(&source, "s1", N_ROWS).expect("load source workload");

    let storage_root = tmp.path().join("wal-g");
    fs::create_dir_all(&storage_root).unwrap();
    let storage: DynStorage = Arc::new(FsStorage::new(&storage_root).unwrap());
    let settings = test_settings(storage_root.clone());

    let socket_host = source.config().socket_dir.to_str().unwrap().to_string();
    // SAFETY: single-test binary; the daemon subprocess receives these through
    // `Command::env` rather than by re-reading the parent env
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

    promote_once(&source).expect("promote source");
    source
        .psql_one("CHECKPOINT")
        .expect("checkpoint promoted source");
    let tli = timeline_of(&source);
    assert_eq!(tli, 2, "promotion left the source on {tli}");
    source
        .apply_schema_dump("INSERT INTO s1.t VALUES (900000, 'after-promotion');\n")
        .expect("insert on the promoted timeline");

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
    body.push_str(&format!(
        "\n[backup]\narchive = \"file://{}\"\n",
        storage_root.display()
    ));
    fs::write(&ch_config_path, body).expect("append [backup] to ch-config");

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
                ("PGDATABASE", "postgres".into()),
            ],
        )
        .expect("spawn walshadow-stream");
    let guard = fx::ChildGuard::new(child);

    let result = (|| -> Result<()> {
        fx::wait_for_listen(daemon.metrics_addr, Duration::from_secs(180))
            .context("daemon metrics endpoint never came up")?;
        fx::wait_for_ch_value(
            &ch,
            "SELECT count() FROM default.t FINAL WHERE id = 900000",
            "1",
            Duration::from_secs(180),
        )
        .context("row written on the promoted timeline never reached CH")?;
        source
            .apply_schema_dump("INSERT INTO s1.t VALUES (900001, 'post-boot');\n")
            .context("post-boot insert")?;
        fx::wait_for_ch_value(
            &ch,
            "SELECT count() FROM default.t FINAL WHERE _is_deleted = 0",
            &(N_ROWS + 2).to_string(),
            Duration::from_secs(90),
        )
        .context("live CDC after the crossing never reached CH")?;
        Ok(())
    })();

    fx::finish_daemon(guard, &daemon, result);
}
