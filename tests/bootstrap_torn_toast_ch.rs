//! Object-store bootstrap with heap copied after insertion and TOAST copied
//! before its chunks were written
//!
//! Rewrite a backup tar entry to reproduce mismatched page copies. Recover
//! missing chunks from window WAL. Keep backup start as walked row version so
//! replayed replacements win. Drop unreadable rows only with replayed slot
//! replacements; abort loading if an untouched row's value is missing

#![cfg(target_os = "linux")]

#[path = "common/bootstrap_ch_fixture.rs"]
mod fx;

use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail, ensure};
use futures::StreamExt;
use tokio_tar::{Archive, Builder};
use walrus::compression;
use walrus::config::{Settings, StorageSettings, Vars};
use walrus::pg::backup::push::{self, PushArgs};
use walrus::pg::backup::{list, parse_pg_lsn, sentinel_key, tar_partitions_prefix};
use walrus::pg::replication::conn::PgConfig;
use walrus::pg::wal;
use walrus::storage::DynStorage;
use walrus::storage::fs::FsStorage;
use walshadow::shadow::Shadow;

const BLCKSZ: usize = 8192;
const BODY_REPEAT: u32 = 700;
const N_BASE: i32 = 4;
const LATE_ID: i32 = 100;
const STRADDLE_ID: i32 = 101;
const UPDATED_ID: i32 = 1;
const DELETED_ID: i32 = 2;
const SCHEMA: &str = "s21";

fn load_base_workload(source: &Shadow) -> Result<()> {
    let sql = format!(
        "CREATE SCHEMA {SCHEMA};\n\
         CREATE TABLE {SCHEMA}.t (id int4 PRIMARY KEY, body text NOT NULL) \
           WITH (autovacuum_enabled = false);\n\
         ALTER TABLE {SCHEMA}.t ALTER COLUMN body SET STORAGE EXTERNAL;\n\
         ALTER TABLE {SCHEMA}.t REPLICA IDENTITY FULL;\n\
         INSERT INTO {SCHEMA}.t \
           SELECT g, repeat('base-'||g::text||'---', {BODY_REPEAT}) \
           FROM generate_series(1, {N_BASE}) g;\n\
         CHECKPOINT;\n\
         SELECT pg_switch_wal();\n",
    );
    source.apply_schema_dump(&sql)?;
    Ok(())
}

struct HeldSession {
    child: Child,
}

impl HeldSession {
    fn open(source: &Shadow) -> Self {
        let child = Command::new("psql")
            .args([
                "-h",
                source.config().socket_dir.to_str().unwrap(),
                "-p",
                &source.config().port.to_string(),
                "-U",
                "postgres",
                "-d",
                "postgres",
                "-v",
                "ON_ERROR_STOP=1",
                "-f",
                "-",
            ])
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .spawn()
            .expect("spawn psql");
        Self { child }
    }

    fn send(&mut self, sql: &str) {
        self.child
            .stdin
            .as_mut()
            .expect("stdin piped")
            .write_all(sql.as_bytes())
            .expect("write to held psql");
    }

    fn finish(mut self) {
        drop(self.child.stdin.take());
        let status = self.child.wait().expect("wait psql");
        assert!(status.success(), "held psql session failed: {status}");
    }
}

fn wait_idle_in_txn(source: &Shadow) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while source
        .psql_one("SELECT count(*) FROM pg_stat_activity WHERE state = 'idle in transaction'")
        .expect("poll pg_stat_activity")
        != "1"
    {
        assert!(
            Instant::now() < deadline,
            "held transaction never went idle"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}

fn write_ch_config(path: &Path, ch_port: u16, target: &str, archive_uri: &str) -> Result<()> {
    let body = format!(
        "[ch]\n\
         host = \"127.0.0.1\"\n\
         port = {ch_port}\n\
         database = \"default\"\n\
         compression = \"lz4\"\n\
         \n\
         [toast]\n\
         mode = \"clickhouse\"\n\
         \n\
         [backup]\n\
         archive = \"{archive_uri}\"\n\
         \n\
         [table.\"{SCHEMA}\".\"t\"]\n\
         target_database = \"default\"\n\
         target_table = \"{target}\"\n\
         columns = [\n  \
           {{ attnum = 1, target = \"id\",   type = \"Int32\"  }},\n  \
           {{ attnum = 2, target = \"body\", type = \"String\" }},\n\
         ]\n",
    );
    fs::write(path, body).with_context(|| format!("write ch-config {}", path.display()))?;
    Ok(())
}

fn create_ch_dest_table(ch: &fx::ChServer, table: &str) -> Result<()> {
    ch.query("CREATE DATABASE IF NOT EXISTS default")?;
    ch.query(&format!(
        "CREATE OR REPLACE TABLE default.{table} (\
            id Int32,\
            body String,\
            _lsn UInt64,\
            _xid UInt32,\
            _commit_ts DateTime64(6, 'UTC'), _is_deleted Bool\
         ) ENGINE = ReplacingMergeTree(_lsn, _is_deleted) ORDER BY id"
    ))?;
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

fn copy_dir(src: &Path, dst: &Path) -> Result<()> {
    fs::create_dir_all(dst)?;
    for entry in fs::read_dir(src)? {
        let entry = entry?;
        let to = dst.join(entry.file_name());
        if entry.file_type()?.is_dir() {
            copy_dir(&entry.path(), &to)?;
        } else {
            fs::copy(entry.path(), &to)?;
        }
    }
    Ok(())
}

/// Replace tar entry at `rel`; return false if absent
async fn rewrite_tar_entry(tar_path: &Path, rel: &str, body: &[u8]) -> Result<bool> {
    let file = tokio::fs::File::open(tar_path)
        .await
        .with_context(|| format!("open {}", tar_path.display()))?;
    let mut archive = Archive::new(file);
    let mut entries = archive.entries()?;
    let mut out = Builder::new(Vec::new());
    let mut replaced = false;
    while let Some(entry) = entries.next().await {
        let mut entry = entry?;
        let path = entry.path()?.into_owned();
        let mut header = entry.header().clone();
        let bare = path.strip_prefix(".").unwrap_or(&path);
        if bare == Path::new(rel) {
            header.set_size(body.len() as u64);
            out.append_data(&mut header, &path, body).await?;
            replaced = true;
        } else {
            out.append_data(&mut header, &path, &mut entry).await?;
        }
    }
    let bytes = out.into_inner().await?;
    tokio::fs::write(tar_path, bytes)
        .await
        .with_context(|| format!("rewrite {}", tar_path.display()))?;
    Ok(replaced)
}

/// Replace `rel` in every backup data part under `root`
async fn tear_backup_file(root: &Path, backup: &str, rel: &str, body: &[u8]) -> Result<()> {
    let parts = root.join(tar_partitions_prefix(backup));
    let mut hit = false;
    for entry in fs::read_dir(&parts).with_context(|| format!("read_dir {}", parts.display()))? {
        let path = entry?.path();
        let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
        if !name.starts_with("part_") {
            continue;
        }
        hit |= rewrite_tar_entry(&path, rel, body).await?;
    }
    ensure!(
        hit,
        "{rel} not found in any tar part under {}",
        parts.display()
    );
    Ok(())
}

fn extend_sentinel_finish(root: &Path, backup: &str, finish_lsn: u64) -> Result<()> {
    let path = root.join(sentinel_key(backup));
    let raw = fs::read_to_string(&path).with_context(|| format!("read {}", path.display()))?;
    let mut v: serde_json::Value = serde_json::from_str(&raw).context("parse sentinel")?;
    let start = v["LSN"].as_u64().context("sentinel LSN")?;
    ensure!(
        finish_lsn > start,
        "finish {finish_lsn:#x} not past start {start:#x}"
    );
    v["FinishLSN"] = serde_json::json!(finish_lsn);
    fs::write(&path, serde_json::to_vec(&v)?)
        .with_context(|| format!("write {}", path.display()))?;
    Ok(())
}

fn source_md5(source: &Shadow, id: i32) -> Result<String> {
    Ok(source
        .psql_one(&format!("SELECT md5(body) FROM {SCHEMA}.t WHERE id = {id}"))
        .with_context(|| format!("source body digest id={id}"))?
        .trim()
        .into())
}

fn metric_at_least(
    daemon: &fx::DaemonRun,
    name: &str,
    want: u64,
    timeout: Duration,
) -> Result<u64> {
    let deadline = Instant::now() + timeout;
    loop {
        let body = fx::http_get(daemon.metrics_addr, "/metrics").context("scrape /metrics")?;
        let got = fx::parse_metric(&body, name).unwrap_or(0);
        if got >= want {
            return Ok(got);
        }
        if Instant::now() >= deadline {
            bail!("{name} stayed at {got}, want at least {want}");
        }
        std::thread::sleep(Duration::from_millis(200));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn torn_backup_drops_window_rows_and_stays_fatal_for_older_ones() {
    if !fx::tools::requirements_available() {
        return;
    }

    let slot = fx::Ports::alloc();
    let tmp = tempfile::tempdir().unwrap();

    let source = fx::start_source(&tmp);
    let _src_stop = fx::StopOnDrop { sh: &source };
    load_base_workload(&source).expect("load base workload");

    let heap_rel = source
        .psql_one(&format!("SELECT pg_relation_filepath('{SCHEMA}.t')"))
        .expect("heap filepath");
    let toast_rel = source
        .psql_one(&format!(
            "SELECT pg_relation_filepath(reltoastrelid) FROM pg_class WHERE oid = '{SCHEMA}.t'::regclass"
        ))
        .expect("toast filepath");
    assert!(!heap_rel.is_empty() && !toast_rel.is_empty());

    let storage_root = tmp.path().join("wal-g");
    fs::create_dir_all(&storage_root).unwrap();
    let storage: DynStorage = Arc::new(FsStorage::new(&storage_root).unwrap());
    let settings = test_settings(storage_root.clone());

    let socket_host = source.config().socket_dir.to_str().unwrap().to_string();
    // SAFETY: one test per binary; no concurrent reads of parent's environment.
    // Daemon inherits variables through `Command::env`
    unsafe {
        std::env::set_var("PGHOST", &socket_host);
        std::env::set_var("PGPORT", source.config().port.to_string());
        std::env::set_var("PGUSER", "postgres");
        std::env::set_var("PGDATABASE", "postgres");
        std::env::remove_var("PGPASSWORD");
    }
    let cfg = PgConfig::resolve(&Vars::default()).expect("resolve source PgConfig");
    // Allocate xid before backup checkpoint so only TOAST value ID dates row
    let mut straddler = HeldSession::open(&source);
    straddler.send("BEGIN;\nSELECT txid_current();\n");
    wait_idle_in_txn(&source);
    push::handle(&settings, storage.clone(), PushArgs::default(), cfg)
        .await
        .expect("push base backup");

    let backups = list::collect(storage.clone()).await.expect("list backups");
    assert_eq!(backups.len(), 1);
    let backup_name = backups.into_iter().next().unwrap().name;

    source
        .psql_one(&format!(
            "UPDATE {SCHEMA}.t SET body = repeat('updated-{UPDATED_ID}---', {BODY_REPEAT}) \
             WHERE id = {UPDATED_ID}"
        ))
        .expect("update base row");
    source
        .psql_one(&format!(
            "INSERT INTO {SCHEMA}.t VALUES ({LATE_ID}, repeat('late-{LATE_ID}---', {BODY_REPEAT}))"
        ))
        .expect("late insert");
    // Exhaust two OID prefetch ranges so NEXTOID records place TOAST value ID
    // after backup checkpoint
    straddler.send(&format!(
        "SELECT count(lo_create(0)) FROM generate_series(1, 16384);\n\
         INSERT INTO {SCHEMA}.t VALUES ({STRADDLE_ID}, repeat('straddle-{STRADDLE_ID}---', {BODY_REPEAT}));\n\
         COMMIT;\n"
    ));
    straddler.finish();
    source
        .psql_one(&format!("SELECT count(*) FROM {SCHEMA}.t"))
        .expect("set hint bits");
    source
        .psql_one("CHECKPOINT")
        .expect("checkpoint after late insert");
    // Copy heap before deletion removes its TOAST value during backup window
    let late_heap = fs::read(source.config().data_dir.join(&heap_rel)).expect("read heap file");
    source
        .psql_one(&format!("DELETE FROM {SCHEMA}.t WHERE id = {DELETED_ID}"))
        .expect("delete base row");
    let finish = source
        .psql_one("SELECT pg_current_wal_insert_lsn()")
        .expect("insert lsn");
    let finish_lsn = parse_pg_lsn(&finish).expect("parse finish lsn");
    source
        .psql_one("SELECT pg_switch_wal()")
        .expect("switch wal");
    push_completed_wal_segments(&source, &settings, storage.clone())
        .await
        .expect("push WAL segments");

    let fatal_root = tmp.path().join("wal-g-fatal");
    copy_dir(&storage_root, &fatal_root).expect("copy backup for the fatal scenario");

    tear_backup_file(&storage_root, &backup_name, &heap_rel, &late_heap)
        .await
        .expect("swap heap file for its post-insert copy");
    extend_sentinel_finish(&storage_root, &backup_name, finish_lsn).expect("extend window");

    // Keep one block so mirror exists but lacks later values
    let toast = fs::read(source.config().data_dir.join(&toast_rel)).expect("read TOAST file");
    tear_backup_file(&fatal_root, &backup_name, &toast_rel, &toast[..BLCKSZ])
        .await
        .expect("cut the TOAST file copy to its first block");

    let ch_tmp = tempfile::tempdir().unwrap();
    let ch = fx::ChServer::spawn(ch_tmp, slot.ch_tcp, slot.ch_http).expect("spawn ch");
    let env = [
        ("PGHOST", socket_host.clone()),
        ("PGPORT", source.config().port.to_string()),
        ("PGUSER", "postgres".to_string()),
        ("PGDATABASE", "postgres".to_string()),
    ];

    create_ch_dest_table(&ch, "t_fatal").expect("create fatal ch table");
    let fatal_cfg = tmp.path().join("ch-config-fatal.toml");
    write_ch_config(
        &fatal_cfg,
        slot.ch_tcp,
        "t_fatal",
        &format!("file://{}", fatal_root.display()),
    )
    .expect("write fatal ch-config");
    let fatal_dir = tmp.path().join("run-fatal");
    fs::create_dir_all(&fatal_dir).unwrap();
    let fatal_daemon =
        fx::DaemonRun::prepare(&fatal_dir, slot.metrics).expect("fatal daemon layout");
    let child = fatal_daemon
        .spawn_mode(
            &source,
            &fatal_cfg,
            slot.walsender,
            "object-store",
            &["--bootstrap-backup-name", &backup_name],
            &env,
        )
        .expect("spawn fatal walshadow-stream");
    let guard = fx::ChildGuard::new(child);
    let result = (|| -> Result<()> {
        let mut child = guard.into_inner().context("child taken")?;
        let status = fx::wait_with_timeout(&mut child, Duration::from_secs(60))
            .context("pre-checkpoint row with a missing value must abort the load")?;
        ensure!(
            !status.success(),
            "daemon exited cleanly after a fatal miss"
        );
        // Missing values and absent mirrors report different errors
        let stderr = fatal_daemon.stderr();
        ensure!(
            stderr.contains("remedy: fresher backup") || stderr.contains("no mirror for db"),
            "load aborted on something other than an unreadable value"
        );
        Ok(())
    })();
    fatal_daemon.stop_shadow();
    let fatal_failure = result.err().map(|e| {
        format!(
            "{e:#}\n--- fatal daemon stderr ---\n{}",
            fatal_daemon.stderr()
        )
    });

    create_ch_dest_table(&ch, "t_torn").expect("create torn ch table");
    // Keep walked copies beside their replacements
    ch.query("SYSTEM STOP MERGES default.t_torn")
        .expect("stop merges on torn table");
    let torn_cfg = tmp.path().join("ch-config-torn.toml");
    write_ch_config(
        &torn_cfg,
        slot.ch_tcp,
        "t_torn",
        &format!("file://{}", storage_root.display()),
    )
    .expect("write torn ch-config");
    let torn_dir = tmp.path().join("run-torn");
    fs::create_dir_all(&torn_dir).unwrap();
    let daemon = fx::DaemonRun::prepare(&torn_dir, slot.metrics).expect("torn daemon layout");
    let child = daemon
        .spawn_mode(
            &source,
            &torn_cfg,
            slot.walsender,
            "object-store",
            &["--bootstrap-backup-name", &backup_name],
            &env,
        )
        .expect("spawn torn walshadow-stream");
    let guard = fx::ChildGuard::new(child);

    let result = (|| -> Result<()> {
        fx::wait_for_listen(daemon.metrics_addr, Duration::from_secs(30))
            .context("daemon metrics endpoint never came up")?;
        fx::wait_for_ch_value(
            &ch,
            "SELECT count() FROM default.t_torn FINAL WHERE _is_deleted = 0",
            &(N_BASE + 1).to_string(),
            Duration::from_secs(180),
        )?;

        let survivors = (1..=N_BASE).filter(|id| *id != DELETED_ID);
        for id in survivors.chain([LATE_ID, STRADDLE_ID]) {
            let want = source_md5(&source, id)?;
            let got = ch
                .query(&format!(
                    "SELECT lower(hex(MD5(body))) FROM default.t_torn FINAL WHERE id = {id}"
                ))
                .with_context(|| format!("ch body digest id={id}"))?;
            ensure!(got == want, "body id={id} differs: ch={got} source={want}");
        }
        let nulls = ch
            .query("SELECT count() FROM default.t_torn WHERE body = ''")
            .context("empty body probe")?;
        ensure!(nulls == "0", "{nulls} rows landed without a body");
        // Untouched base rows carry the walk version
        let walk_lsn = ch
            .query(&format!(
                "SELECT max(_lsn) FROM default.t_torn WHERE id IN (3, {N_BASE})"
            ))
            .context("walk version probe")?;
        let walked = ch
            .query(&format!(
                "SELECT count() FROM default.t_torn WHERE id IN ({UPDATED_ID}, {LATE_ID}, \
                 {STRADDLE_ID}) AND _lsn = {walk_lsn} AND body != ''"
            ))
            .context("walked copy probe")?;
        ensure!(
            walked == "3",
            "{walked} of 3 window rows resolved from the walk past backup start"
        );
        let outranked = ch
            .query(&format!(
                "SELECT count() FROM default.t_torn FINAL WHERE id IN ({UPDATED_ID}, {LATE_ID}, \
                 {STRADDLE_ID}) AND _lsn > {walk_lsn}"
            ))
            .context("replacement priority probe")?;
        ensure!(
            outranked == "3",
            "{outranked} of 3 window rows outrank their walked copy"
        );
        let deleted = ch
            .query(&format!(
                "SELECT (countIf(_lsn = {walk_lsn}), countIf(_is_deleted)) \
                 FROM default.t_torn WHERE id = {DELETED_ID}"
            ))
            .context("deleted row probe")?;
        ensure!(
            deleted == "(0,1)",
            "deleted row: (walked copies, tombstones) = {deleted}"
        );

        let covered = metric_at_least(
            &daemon,
            "walshadow_bootstrap_replay_covered_rows_total",
            1,
            Duration::from_secs(30),
        )?;
        ensure!(
            covered == 1,
            "expected only the deleted referrer dropped, got {covered}"
        );
        let body = fx::http_get(daemon.metrics_addr, "/metrics")?;
        let misses = fx::parse_metric(&body, "walshadow_toast_fetch_miss_total").unwrap_or(0);
        ensure!(
            misses == 0,
            "a covered miss was still counted as a fetch miss: {misses}"
        );
        let stderr = daemon.stderr();
        ensure!(
            stderr.contains("deferred referrers dropped"),
            "no drop was logged"
        );
        Ok(())
    })();
    drop(guard);
    daemon.stop_shadow();
    let torn_failure = result
        .err()
        .map(|e| format!("{e:#}\n--- torn daemon stderr ---\n{}", daemon.stderr()));
    let failures: Vec<_> = fatal_failure.into_iter().chain(torn_failure).collect();
    assert!(failures.is_empty(), "{}", failures.join("\n\n"));
}
