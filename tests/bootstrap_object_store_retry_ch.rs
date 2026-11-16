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
use walshadow::bootstrap_marker::{BootstrapMarker, ExtractedCheckpoint, MARKER_FILENAME};
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
    if !fx::tools::requirements_available() {
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

/// Every file under `dir`, recursively
fn files_under(dir: &std::path::Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    for entry in fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            out.extend(files_under(&path));
        } else {
            out.push(path);
        }
    }
    out
}

/// Archive each completed source segment straight into storage, `garbled`
/// with a bad page magic the window replay refuses
fn archive_wal(source: &Shadow, storage_root: &std::path::Path, garbled: bool) {
    let archive = storage_root.join("wal_005");
    fs::create_dir_all(&archive).unwrap();
    for entry in fs::read_dir(source.config().data_dir.join("pg_wal")).unwrap() {
        let entry = entry.unwrap();
        let name = entry.file_name().into_string().unwrap();
        if name.len() != 24 || !name.chars().all(|c| c.is_ascii_hexdigit()) {
            continue;
        }
        let mut bytes = fs::read(entry.path()).unwrap();
        if garbled {
            bytes[..3].copy_from_slice(&[0xFF, 0xFF, 1]);
        }
        fs::write(archive.join(&name), bytes).unwrap();
    }
}

/// Source with one pinned backup in FsStorage, a CH destination, and the
/// daemon layout aimed at both
struct Drill {
    source: Shadow,
    ch: fx::ChServer,
    storage_root: PathBuf,
    backup_name: String,
    ch_config_path: PathBuf,
    daemon: fx::DaemonRun,
    walsender: u16,
}

impl Drill {
    /// No reads after load: tuples land in the backup without hint bits, so
    /// the gate defers them to a spool
    async fn new(tmp: &tempfile::TempDir) -> Self {
        let slot = fx::Ports::alloc();
        let source = fx::start_source(tmp);
        fx::load_source_workload(&source, "s14", N_ROWS).expect("load source workload");
        let storage_root = tmp.path().join("wal-g");
        fs::create_dir_all(&storage_root).unwrap();
        let storage: DynStorage = Arc::new(FsStorage::new(&storage_root).unwrap());
        let settings = test_settings(storage_root.clone());
        // SAFETY: nextest runs each test in its own process; the daemon
        // receives these through `Command::env`
        unsafe {
            std::env::set_var("PGHOST", &source.config().socket_dir);
            std::env::set_var("PGPORT", source.config().port.to_string());
            std::env::set_var("PGUSER", "postgres");
            std::env::set_var("PGDATABASE", "postgres");
            std::env::remove_var("PGPASSWORD");
        }
        let cfg = PgConfig::resolve(&Vars::default()).expect("resolve source PgConfig");
        push::handle(&settings, storage.clone(), PushArgs::default(), cfg)
            .await
            .expect("wal-rus push::handle against source PG");
        source.psql_one("SELECT pg_switch_wal()").unwrap();
        let backup_name = list::collect(storage)
            .await
            .expect("list backups on FsStorage")
            .into_iter()
            .next()
            .expect("one backup on fresh storage")
            .name;

        let ch = fx::ChServer::spawn(tempfile::tempdir().unwrap(), slot.ch_tcp, slot.ch_http)
            .expect("spawn ch");
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
        let mut body = fs::read_to_string(&ch_config_path).unwrap();
        body.push_str(&format!(
            "\n[backup]\narchive = \"file://{}\"\n",
            storage_root.display()
        ));
        fs::write(&ch_config_path, body).unwrap();
        let daemon = fx::DaemonRun::prepare(tmp.path(), slot.metrics).expect("daemon layout");
        Self {
            source,
            ch,
            storage_root,
            backup_name,
            ch_config_path,
            daemon,
            walsender: slot.walsender,
        }
    }

    fn spawn(&self) -> Result<std::process::Child> {
        self.daemon.spawn_mode(
            &self.source,
            &self.ch_config_path,
            self.walsender,
            "object-store",
            &[
                "--bootstrap-backup-name",
                &self.backup_name,
                "--bootstrap-object-store-parallelism",
                "2",
            ],
            &[
                (
                    "PGHOST",
                    self.source.config().socket_dir.display().to_string(),
                ),
                ("PGPORT", self.source.config().port.to_string()),
                ("PGUSER", "postgres".into()),
                ("PGDATABASE", "postgres".into()),
            ],
        )
    }

    /// One attempt expected to fail, its log
    fn failed_run(&self) -> Result<String> {
        let mut child = self.spawn()?;
        let status = fx::wait_with_timeout(&mut child, Duration::from_secs(120))?;
        self.daemon.stop_shadow();
        let log = self.daemon.stderr();
        anyhow::ensure!(!status.success(), "attempt succeeded:\n{log}");
        Ok(log)
    }

    fn extracted(&self) -> Result<Option<ExtractedCheckpoint>> {
        ExtractedCheckpoint::read(&self.daemon.shadow_data_dir)
    }
}

impl Drop for Drill {
    fn drop(&mut self) {
        let _ = self.source.stop();
    }
}

/// Torn tar part fails extraction before anything licenses a resume
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn torn_backup_part_fails_extraction() {
    if !fx::tools::requirements_available() {
        return;
    }
    let tmp = tempfile::tempdir().unwrap();
    let drill = Drill::new(&tmp).await;
    archive_wal(&drill.source, &drill.storage_root, false);
    let part = files_under(&drill.storage_root.join("basebackups_005"))
        .into_iter()
        .filter(|p| p.to_string_lossy().contains("tar_partitions"))
        .max_by_key(|p| fs::metadata(p).unwrap().len())
        .expect("backup has a tar part");
    let intact = fs::read(&part).unwrap();
    fs::write(&part, &intact[..intact.len() / 2]).unwrap();

    let result = (|| -> Result<()> {
        let log = drill.failed_run()?;
        anyhow::ensure!(
            log.contains("bootstrap pump"),
            "torn part did not fail the extraction:\n{log}"
        );
        anyhow::ensure!(
            drill.extracted()?.is_none(),
            "failed extraction left a checkpoint"
        );
        let marker = BootstrapMarker::read(&drill.daemon.shadow_data_dir)?
            .context("failed extraction dropped its marker")?;
        anyhow::ensure!(
            marker.backup_name.as_deref() == Some(drill.backup_name.as_str()),
            "marker pins {:?}",
            marker.backup_name
        );
        Ok(())
    })();
    result.unwrap();
}

/// Window replay failing after extraction checkpointed licenses a resume;
/// a torn gate spool fails the resumed gate, and once restored the next
/// attempt resumes past extraction with the spools it inherited
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn object_store_bootstrap_resumes_past_extraction() {
    if !fx::tools::requirements_available() {
        return;
    }
    let tmp = tempfile::tempdir().unwrap();
    let drill = Drill::new(&tmp).await;
    archive_wal(&drill.source, &drill.storage_root, true);

    let result = (|| -> Result<()> {
        let log = drill.failed_run()?;
        anyhow::ensure!(
            log.contains("bootstrap: backup-window WAL leg"),
            "garbled archive did not fail the window replay:\n{log}"
        );
        let extracted = drill
            .extracted()?
            .context("replay failure lost the extraction checkpoint")?;
        let spool = &extracted
            .deferred_spools
            .first()
            .context("unhinted rows deferred nothing")?
            .path;
        let intact = fs::read(spool)?;
        fs::write(spool, &intact[..intact.len() - 1])?;
        let log = drill.failed_run()?;
        anyhow::ensure!(
            log.contains("bootstrap: reopen deferred spool: "),
            "torn gate spool did not fail the resumed gate:\n{log}"
        );
        fs::write(spool, &intact)?;
        Ok(())
    })();
    result.unwrap();

    archive_wal(&drill.source, &drill.storage_root, false);
    let guard = fx::ChildGuard::new(drill.spawn().expect("spawn walshadow-stream"));
    let result = (|| -> Result<()> {
        let src_count = drill.source.psql_one("SELECT count(*) FROM s14.t")?;
        fx::wait_for_ch_value(
            &drill.ch,
            "SELECT count() FROM default.t FINAL WHERE _is_deleted = 0",
            &src_count,
            Duration::from_secs(60),
        )?;
        fx::assert_ch_matches_source(&drill.ch, &drill.source, "s14.t", "default.t")?;
        drill
            .daemon
            .wait_for_log("bootstrap visibility gate settled", Duration::from_secs(60))?;
        let log = drill.daemon.stderr();
        anyhow::ensure!(
            log.contains("resuming an incomplete bootstrap past extraction"),
            "third attempt did not resume past extraction:\n{log}",
        );
        Ok(())
    })();
    fx::finish_daemon(guard, &drill.daemon, result);
}
