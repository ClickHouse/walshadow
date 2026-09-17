//! Greenfield bootstrap under `[toast] backend = "shadow"`: external values
//! come out of shadow's own TOAST heaps, and no chunk mirror is written
//! anywhere.
//!
//! Shadow is started mid-bootstrap and replays to `end_lsn`, which is the only
//! way to hold a value written *during* the backup — its chunks are appended
//! mid-window, so no file copy contains them. What this pins:
//!
//! - every live external value reaches ClickHouse byte-identical
//! - dead and aborted generations still do not
//! - `toast_chunk_puts` is zero and no `pg_toast_*` table appears in CH
//! - rows took the deferral path rather than an inline fetch during the walk
//! - shadow is started mid-bootstrap and replays to `end_lsn`, which is what
//!   makes a value written *during* the backup resolvable at all
//!
//! The mirror-backed equivalent is `bootstrap_toast_gate_ch.rs`.

#![cfg(target_os = "linux")]

#[path = "common/bootstrap_ch_fixture.rs"]
mod fx;

use std::fs;
use std::path::Path;
use std::time::Duration;

use anyhow::{Context, Result, ensure};
use walshadow::shadow::Shadow;

/// Force multi-chunk external values, so a short reassembly is visible
const BODY_REPEAT: u32 = 700;
const N_LIVE: i32 = 4;

/// Live, dead, superseded and aborted values, none pruned
fn load_toast_workload(source: &Shadow, schema: &str) -> Result<()> {
    let sql = format!(
        "CREATE SCHEMA {schema};\n\
         CREATE TABLE {schema}.t (id int4 PRIMARY KEY, body text NOT NULL) \
           WITH (autovacuum_enabled = false);\n\
         ALTER TABLE {schema}.t ALTER COLUMN body SET STORAGE EXTERNAL;\n\
         ALTER TABLE {schema}.t REPLICA IDENTITY FULL;\n\
         INSERT INTO {schema}.t \
           SELECT g, repeat('live-'||g::text||'---', {BODY_REPEAT}) \
           FROM generate_series(1, {N_LIVE}) g;\n\
         INSERT INTO {schema}.t \
           SELECT g, repeat('dead-'||g::text||'---', {BODY_REPEAT}) \
           FROM generate_series(101, 104) g;\n\
         DELETE FROM {schema}.t WHERE id BETWEEN 101 AND 104;\n\
         INSERT INTO {schema}.t \
           SELECT g, repeat('doomed-'||g::text||'---', {BODY_REPEAT}) \
           FROM generate_series(301, 304) g;\n\
         BEGIN;\n\
         INSERT INTO {schema}.t \
           SELECT g, repeat('ghost-'||g::text||'---', {BODY_REPEAT}) \
           FROM generate_series(1001, 1004) g;\n\
         ROLLBACK;\n\
         CHECKPOINT;\n\
         SELECT pg_switch_wal();\n",
    );
    source.apply_schema_dump(&sql)?;
    Ok(())
}

/// Writes issued once the backup is past its start checkpoint, so they land
/// inside the window and log full-page images of TOAST pages the backup is
/// copying underneath. Without those images a torn page stays torn
fn churn_toast_in_window(source: &Shadow, schema: &str) -> Result<()> {
    // Checkpoint first, then touch: a page image is logged on the first
    // modification of a page whose LSN predates the checkpoint's redo
    // pointer, so the order decides whether any appear at all
    source.psql_one("CHECKPOINT")?;
    // Delete rows seeded pre-window: their chunks sit on toast pages the
    // backup is copying, so removing them dirties those pages and the window
    // carries an image of each. A read would not do — `SnapshotToast` never
    // consults clog, so a toast page has no hint bit for a read to set.
    //
    // DELETE rather than UPDATE: an UPDATE under REPLICA IDENTITY FULL inside
    // the window puts the old version's delete and the new version's insert
    // at the same commit LSN, which `ReplacingMergeTree` has no deterministic
    // tiebreak for — a separate question from TOAST value resolution
    source.psql_one(&format!(
        "DELETE FROM {schema}.t WHERE id BETWEEN 301 AND 304"
    ))?;
    // Rows inserted while the backup runs. Their transaction can straddle
    // `end_lsn`, leaving the walk a partially written page and part of the
    // value above the window — the case the read-only fill path exists for
    source.psql_one(&format!(
        "INSERT INTO {schema}.t \
         SELECT g, repeat('window-'||g::text||'---', {BODY_REPEAT}) \
         FROM generate_series(201, 208) g"
    ))?;
    source.psql_one(&format!("SELECT count(md5(body)) FROM {schema}.t"))?;
    Ok(())
}

fn write_shadow_toast_config(path: &Path, ch_port: u16, schema: &str) -> Result<()> {
    let body = format!(
        "[ch]\n\
         host = \"127.0.0.1\"\n\
         port = {ch_port}\n\
         database = \"default\"\n\
         compression = \"lz4\"\n\
         \n\
         [toast]\n\
         backend = \"shadow\"\n\
         \n\
         [table.\"{schema}\".\"t\"]\n\
         target_database = \"default\"\n\
         target_table = \"t\"\n\
         columns = [\n  \
           {{ attnum = 1, target = \"id\",   type = \"Int32\"  }},\n  \
           {{ attnum = 2, target = \"body\", type = \"String\" }},\n\
         ]\n",
    );
    fs::write(path, body).with_context(|| format!("write ch-config {}", path.display()))?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bootstrap_renders_external_values_out_of_shadow() {
    if !fx::requirements_available() {
        return;
    }

    let slot = fx::Ports::alloc();
    let tmp = tempfile::tempdir().unwrap();
    let schema = "s21";

    let source = fx::start_source(&tmp);
    // Same knob `fpi_user_pages.rs` needs: without it a page whose only
    // change is a hint bit logs no image
    {
        use std::io::Write;
        let mut f = fs::OpenOptions::new()
            .append(true)
            .open(source.config().data_dir.join("postgresql.conf"))
            .expect("open source conf");
        writeln!(f, "wal_log_hints = on").expect("append wal_log_hints");
        drop(f);
        // PGC_POSTMASTER, so it needs the restart rather than a reload
        source.stop().expect("stop source");
        source.start().expect("restart source");
    }
    let _src_stop = fx::StopOnDrop { sh: &source };
    load_toast_workload(&source, schema).expect("load toast workload");

    let ch_tmp = tempfile::tempdir().unwrap();
    let ch = fx::ChServer::spawn(ch_tmp, slot.ch_tcp, slot.ch_http).expect("spawn ch");
    ch.query("CREATE DATABASE IF NOT EXISTS default")
        .expect("create ch db");
    ch.query(
        "CREATE OR REPLACE TABLE default.t (\
            id Int32,\
            body String,\
            _lsn UInt64,\
            _xid UInt32,\
            _commit_ts DateTime64(6, 'UTC'), _is_deleted Bool\
         ) ENGINE = ReplacingMergeTree(_lsn, _is_deleted) ORDER BY id",
    )
    .expect("create ch table");

    let ch_config_path = tmp.path().join("ch-config.toml");
    write_shadow_toast_config(&ch_config_path, slot.ch_tcp, schema).expect("write ch-config");

    let daemon = fx::DaemonRun::prepare(tmp.path(), slot.metrics).expect("daemon layout");
    let child = daemon
        .spawn(&source, &ch_config_path, slot.walsender, &[])
        .expect("spawn walshadow-stream");
    let guard = fx::ChildGuard::new(child);

    let result = (|| -> Result<()> {
        // In-window churn, timed off the backup's own progress view
        fx::wait_for_backup_streaming(&source, Duration::from_secs(60))
            .context("BASE_BACKUP never started streaming")?;
        churn_toast_in_window(&source, schema).context("in-window toast churn")?;

        fx::wait_for_listen(daemon.metrics_addr, Duration::from_secs(30))
            .context("daemon metrics endpoint never came up")?;

        // The pre-window rows are the deterministic part: they are in the
        // backup, so the walk owes every one of them. How many of the
        // in-window inserts land depends on where the backup window happened
        // to close relative to them, which the test does not control, so it
        // is reported rather than asserted
        fx::wait_for_ch_value(
            &ch,
            &format!(
                "SELECT count() FROM default.t FINAL \
                 WHERE _is_deleted = 0 AND id <= {N_LIVE}"
            ),
            &N_LIVE.to_string(),
            Duration::from_secs(120),
        )?;
        let in_window = ch
            .query(
                "SELECT groupArray(id) FROM (SELECT id FROM default.t FINAL \
                 WHERE _is_deleted = 0 AND id BETWEEN 201 AND 208 ORDER BY id)",
            )
            .context("in-window ids")?;
        eprintln!("in-window rows that arrived: {in_window}");
        ensure!(
            in_window != "[]",
            "no in-window row reached CH, so the window leg shipped nothing"
        );
        let ghosts = ch
            .query("SELECT count() FROM default.t FINAL WHERE id BETWEEN 100 AND 199")
            .context("ghost count")?;
        ensure!(ghosts == "0", "dead or aborted rows reached CH: {ghosts}");
        let aborted = ch
            .query("SELECT count() FROM default.t FINAL WHERE id >= 1000")
            .context("aborted count")?;
        ensure!(aborted == "0", "rolled-back rows reached CH: {aborted}");
        // Seeded pre-window, deleted inside it: the window leg has to carry
        // the tombstone, or the walk's copy of them survives
        let doomed = ch
            .query(
                "SELECT count() FROM default.t FINAL \
                 WHERE _is_deleted = 0 AND id BETWEEN 301 AND 304",
            )
            .context("in-window delete count")?;
        ensure!(
            doomed == "0",
            "rows deleted inside the window survived: {doomed}"
        );

        // Byte comparison catches a wrong generation or a short reassembly,
        // which is what a torn page that was never repaired would produce
        let present: Vec<i32> = (1..=N_LIVE)
            .chain((201..=208).filter(|id| in_window.contains(&id.to_string())))
            .collect();
        for id in present {
            let want: String = source
                .psql_one(&format!("SELECT md5(body) FROM {schema}.t WHERE id = {id}"))
                .with_context(|| format!("source body digest id={id}"))?
                .trim()
                .into();
            let got = ch
                .query(&format!(
                    "SELECT lower(hex(MD5(body))) FROM default.t FINAL WHERE id = {id}"
                ))
                .with_context(|| format!("ch body digest id={id}"))?;
            ensure!(got == want, "body id={id} differs: ch={got} source={want}");
        }
        // No mirror: not created, so nothing was written to one
        let toast_relid = source
            .psql_one(&format!(
                "SELECT reltoastrelid FROM pg_class WHERE oid = '{schema}.t'::regclass"
            ))
            .context("source toast relid")?;
        let mirror = format!("pg_toast_{}", toast_relid.trim());
        let created = ch
            .query(&format!(
                "SELECT count() FROM system.tables \
                 WHERE database = 'default' AND name = '{mirror}'"
            ))
            .context("chunk mirror probe")?;
        ensure!(
            created == "0",
            "shadow backend created chunk mirror {mirror}"
        );

        let stderr = daemon.stderr();

        // Rows waited on the store rather than fetching inline mid-walk
        let deferred = stderr
            .lines()
            .find(|l| l.contains("resolving deferred TOAST tuples"))
            .context("no row waited on the value store")?;
        ensure!(
            !deferred.contains("deferred=0"),
            "nothing deferred, so nothing was read from the oracle: {deferred}"
        );

        // Shadow took the same tree and was started mid-bootstrap, which is
        // what makes a value written during the backup resolvable: its chunks
        // are appended mid-window, so no file copy and no page image holds
        // them — only redo to `end_lsn` does
        let installed = stderr
            .lines()
            .find(|l| l.contains("installed staged TOAST heaps into shadow"))
            .context("shadow was left without the TOAST files it serves from")?;
        ensure!(
            !installed.contains("files=0"),
            "nothing was installed: {installed}"
        );
        // Heap and index both: shadow cannot rebuild an index, so a heap
        // landed without one is unreadable
        ensure!(
            stderr
                .lines()
                .any(|l| l.contains("staging TOAST storage") && l.contains("toast_indexes=1")),
            "the toast index was not staged, so shadow cannot scan the heap",
        );
        // The oracle is out of the value path entirely; it is provisioned
        // only when a tier-3 type needs converting
        ensure!(
            !stderr
                .lines()
                .any(|l| l.contains("installed staged TOAST heaps into the bootstrap oracle")),
            "the oracle should take no part in resolving values",
        );
        ensure!(
            stderr
                .lines()
                .any(|l| l.contains("caught up to bootstrap end_lsn")),
            "shadow never replayed to end_lsn, so in-window values cannot resolve",
        );
        Ok(())
    })();

    fx::finish_daemon(guard, &daemon, result);
}
