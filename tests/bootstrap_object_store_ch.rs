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
use std::net::SocketAddr;
use std::os::unix::process::CommandExt;
use std::path::PathBuf;
use std::process::{Command, Stdio};
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
use walshadow::shadow::{Shadow, ShadowConfig};

// Reserved port slot — 17320-range. Below the Linux ephemeral port
// range (32768-60999) so outbound TCP connects can't grab a port the
// daemon is about to bind. CH's `interserver_http_port = http_port + 1`
// must dodge METRICS/WALSENDER, so the two clusters are spaced apart.
const SOURCE_PORT: u16 = 17321;
const SHADOW_PORT: u16 = 17322;
const CH_TCP_PORT: u16 = 17329;
const CH_HTTP_PORT: u16 = 17330;
const METRICS_PORT: u16 = 17335;
const WALSENDER_PORT: u16 = 17336;

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

fn make_source(tmp: &tempfile::TempDir) -> Shadow {
    let mut cfg = ShadowConfig::new(
        tmp.path().join("source-data"),
        tmp.path().join("source-filtered"),
    );
    cfg.port = SOURCE_PORT;
    cfg.socket_dir = tmp.path().join("source-sock");
    cfg.ctl_timeout = Duration::from_secs(60);
    fs::create_dir_all(&cfg.filter_out_dir).unwrap();
    fs::create_dir_all(&cfg.socket_dir).unwrap();
    Shadow::new(cfg)
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
    if !fx::pg_available() {
        eprintln!("skip: no initdb on PATH");
        return;
    }
    if !fx::clickhouse_available() {
        eprintln!("skip: no clickhouse binary on PATH");
        return;
    }

    let tmp = tempfile::tempdir().unwrap();

    // 1. Source PG.
    let source = make_source(&tmp);
    source.initdb().expect("initdb source");
    source.write_base_conf().expect("source base conf");
    fx::append_source_conf(&source).expect("append source conf");
    source.start().expect("start source");
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
    let ch = fx::ChServer::spawn(ch_tmp, CH_TCP_PORT, CH_HTTP_PORT).expect("spawn ch");
    fx::create_ch_dest_table(&ch, "default", "t").expect("create ch table");

    // 5. CH-config TOML.
    let ch_config_path = tmp.path().join("ch-config.toml");
    fx::write_ch_config_toml(
        &ch_config_path,
        "127.0.0.1",
        CH_TCP_PORT,
        "default",
        &RelName::new("s14", "t"),
        &TableTarget::new("default", "t"),
    )
    .expect("write ch-config");

    // 6. Shadow layout. Daemon writes port and socket config, so test
    //    does not add source settings before bootstrap
    let bootstrap_shadow_data_dir = tmp.path().join("shadow-data");
    let shadow_sock = tmp.path().join("shadow-sock");
    fs::create_dir_all(&shadow_sock).unwrap();
    let shadow_filter_dir = tmp.path().join("filtered");
    fs::create_dir_all(&shadow_filter_dir).unwrap();
    let spill_dir = tmp.path().join("spill");
    fs::create_dir_all(&spill_dir).unwrap();

    // 7. Spawn walshadow-stream. The daemon reads `WALG_*` env vars
    //    via `walrus::config::Settings::from_env()`. WALG_FILE_PREFIX
    //    is the raw filesystem path (no `file://` prefix) — that's the
    //    wal-g CLI contract `detect_storage` mirrors.
    let bin = env!("CARGO_BIN_EXE_walshadow-stream");
    let stderr_path = tmp.path().join("daemon.stderr.log");
    let stderr_file = fs::File::create(&stderr_path).expect("open daemon stderr log");
    let metrics_addr: SocketAddr = format!("127.0.0.1:{METRICS_PORT}").parse().unwrap();
    let walg_path = storage_root.display().to_string();
    let child = Command::new(bin)
        .args([
            "--host",
            source.config().socket_dir.to_str().unwrap(),
            "--port",
            &SOURCE_PORT.to_string(),
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
            &SHADOW_PORT.to_string(),
            "--shadow-user",
            "postgres",
            "--shadow-dbname",
            "postgres",
            "--spill-dir",
            spill_dir.to_str().unwrap(),
            "--status-interval",
            "1",
            "--metrics-bind",
            &metrics_addr.to_string(),
            "--walsender-bind",
            &format!("127.0.0.1:{WALSENDER_PORT}"),
            "--retention-bytes",
            "0",
            "--ch-config",
            ch_config_path.to_str().unwrap(),
            "--bootstrap-mode",
            "object-store",
            "--bootstrap-shadow-data-dir",
            bootstrap_shadow_data_dir.to_str().unwrap(),
            "--bootstrap-backup-name",
            &backup_name,
            "--bootstrap-shadow-replay-timeout",
            "120",
        ])
        // WALG_FILE_PREFIX feeds Settings::from_env's FsStorage path;
        // the daemon's `bootstrap_mode=object_store` arm reads this
        // exactly the same way wal-rus's CLI does.
        .env("WALG_FILE_PREFIX", &walg_path)
        .env("PGHOST", source.config().socket_dir.to_str().unwrap())
        .env("PGPORT", SOURCE_PORT.to_string())
        .env("PGUSER", "postgres")
        .env("PGDATABASE", "postgres")
        .env("RUST_LOG", "warn,walshadow=info")
        .stdout(Stdio::null())
        .stderr(Stdio::from(stderr_file))
        .process_group(0)
        .spawn()
        .expect("spawn walshadow-stream");
    let guard = fx::ChildGuard::new(child);

    let result = (|| -> Result<()> {
        // 8. Wait for daemon's metrics endpoint, bootstrap done,
        //    daemon-owned shadow live, WAL pump alive. Bootstrap-emitter
        //    INSERTs flush to CH synchronously before the streaming
        //    pump starts, so the 64-row fixture lands on CH by this
        //    point.
        fx::wait_for_listen(metrics_addr, Duration::from_secs(30))
            .context("daemon metrics endpoint never came up")?;

        // 9. Oracle. ChildGuard's Drop SIGKILLs the daemon at end of
        //    scope; we don't need a `pg_switch_wal` + drain cycle since
        //    the test surface is bootstrap correctness, not streaming.
        fx::assert_ch_matches_source(&ch, &source, "s14.t", "default.t")
            .context("source vs CH parity")?;

        Ok(())
    })();

    // 12. Kill daemon before shadow so supervisor cannot restart it
    //     Stop any remaining postmaster
    let _ = guard.into_inner().map(|mut c| {
        let _ = c.kill();
        let _ = c.wait();
    });
    if bootstrap_shadow_data_dir.join("postmaster.pid").exists() {
        let mut shadow_cfg =
            ShadowConfig::new(bootstrap_shadow_data_dir.clone(), shadow_filter_dir.clone());
        shadow_cfg.port = SHADOW_PORT;
        shadow_cfg.socket_dir = shadow_sock.clone();
        shadow_cfg.ctl_timeout = Duration::from_secs(60);
        let shadow = Shadow::new(shadow_cfg);
        let _ = shadow.stop();
    }

    if let Err(e) = result {
        let stderr = fs::read_to_string(&stderr_path).unwrap_or_default();
        panic!("{e:#}\n--- daemon stderr ---\n{stderr}");
    }
}
