//! Bootstrap + CH end-to-end via the object-store
//! BASE_BACKUP source.
//!
//! Sibling of `bootstrap_direct_ch.rs` covering the wal-g
//! layout. Setup adds a `walrus::pg::backup::push::handle` call between
//! source workload load and daemon spawn — same fixture pattern as
//! `bootstrap_object_store_e2e.rs`, just with a live CH server + real
//! emitter pipeline replacing the `RecordingObserver`.
//!
//! Pipeline:
//!
//! ```text
//! Shadow(source).start()
//!   → schema + INSERT s14.t (64 rows) + CHECKPOINT + pg_switch_wal
//!   → walrus::pg::backup::push::handle → FsStorage(wal-g/)
//!   → walshadow-stream (subprocess) with
//!         --bootstrap-mode=object_store
//!         --bootstrap-object-store-prefix=file://<tmpdir>/wal-g (env)
//!         --bootstrap-backup-name=<resolved>
//!     → ObjectStoreSource → MultiplexSink → pipeline::bootstrap::drain
//!     → shared tail (batcher + inserter pool + ack) → default.t
//!     → start shadow PG against bootstrap_shadow_data_dir
//!     → WAL pump main loop
//!   → assert_ch_matches_source(ch, source, "s14.t", "default.t")
//! ```
//!
//! Linux-only — `file://<abs_path>` URI parsing on FsStorage is
//! POSIX-shaped; mirrors the `bin_stream_e2e.rs` posture.

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

/// Walk source's `pg_wal/` and push every completed 24-hex-digit WAL
/// segment into wal-rus storage. Skips `.partial`, `.history`,
/// `archive_status/`, and any subdirectory entry. Caller forces a
/// `pg_switch_wal` first so the segment containing the basebackup's
/// `end_lsn` is on disk in final form. In production this happens
/// asynchronously via `archive_command` wired to `wal-push`; tests
/// invoke it inline
async fn push_completed_wal_segments(
    source: &Shadow,
    settings: &walrus::config::Settings,
    storage: DynStorage,
) -> anyhow::Result<()> {
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

/// Minimal Settings for an uncompressed `FsStorage` root — matches
/// `bootstrap_object_store_e2e.rs::test_settings`.
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
async fn object_store_bootstrap_ch_end_to_end() {
    if !fx::requirements_available() {
        return;
    }

    let slot = fx::Ports::alloc();
    let tmp = tempfile::tempdir().unwrap();

    // 1. Source PG.
    let source = fx::start_source(&tmp);
    let _src_stop = fx::StopOnDrop { sh: &source };

    // 2. Schema + workload.
    fx::load_source_workload(&source, "s14", N_ROWS).expect("load source workload");

    // 3. wal-rus push::handle stages a base backup into FsStorage. The
    //    wal-rus CLI reads libpq env vars to find source PG; this test
    //    binary is single-test-fn so env-var writes don't race.
    let storage_root = tmp.path().join("wal-g");
    fs::create_dir_all(&storage_root).unwrap();
    let storage: DynStorage = Arc::new(FsStorage::new(&storage_root).unwrap());
    let settings = test_settings(storage_root.clone());

    let socket_host = source.config().socket_dir.to_str().unwrap().to_string();
    // SAFETY: single-writer; daemon subprocess inherits these via
    // `Command::env`, not by re-reading the parent's env after our
    // `set_var` call.
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

    // push::handle archives the basebackup files but leaves WAL to
    // archive_command (`wal: false` in its BaseBackupOpts), so the
    // segment containing the backup's `end_lsn` sits unrotated in
    // source's pg_wal/. Force a rotation, then push every completed
    // segment via wal-rus's wal::push so the daemon's object-store
    // hydrate path finds WAL covering [start_lsn, end_lsn] in storage.
    source
        .psql_one("SELECT pg_switch_wal()")
        .expect("force WAL rotation post-basebackup");
    push_completed_wal_segments(&source, &settings, storage.clone())
        .await
        .expect("push WAL segments to storage");

    let backup_summaries = list::collect(storage.clone())
        .await
        .expect("list backups on FsStorage");
    assert_eq!(
        backup_summaries.len(),
        1,
        "exactly one backup expected on fresh storage, got {}",
        backup_summaries.len(),
    );
    let backup_name = backup_summaries.into_iter().next().unwrap().name;

    // 4. CH server + dest table.
    let ch_tmp = tempfile::tempdir().unwrap();
    let ch = fx::ChServer::spawn(ch_tmp, slot.ch_tcp, slot.ch_http).expect("spawn ch");
    fx::create_ch_dest_table(&ch, "default", "t").expect("create ch table");

    // 5. CH-config TOML.
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
    // Object-store archive location is a `[backup]` TOML setting now, not
    // `WALG_*` env. `file://` → FsStorage at the same wal-g root.
    let archive_uri = format!("file://{}", storage_root.display());
    let mut ch_config_body = fs::read_to_string(&ch_config_path).expect("read ch-config");
    ch_config_body.push_str(&format!("\n[backup]\narchive = \"{archive_uri}\"\n"));
    fs::write(&ch_config_path, ch_config_body).expect("append [backup] to ch-config");

    // 6. Spawn walshadow-stream. The daemon reads the archive from the
    //    `[backup]` section of `--ch-config` (built into a
    //    `walrus::config::Settings`), not `WALG_*` env.
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
        // 7. Wait for the daemon's metrics endpoint (liveness). The daemon
        //    binds it before the bootstrap tail drains to CH, so it is not
        //    a bootstrap-complete signal on its own.
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

        // 8. Oracle. No `pg_switch_wal` + drain cycle since the surface is
        //    bootstrap correctness, not streaming.
        fx::assert_ch_matches_source(&ch, &source, "s14.t", "default.t")
            .context("source vs CH parity")?;

        Ok(())
    })();

    fx::finish_daemon(guard, &daemon, result);
}
