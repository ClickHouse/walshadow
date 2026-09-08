//! Verify greenfield gate excludes dead and aborted fixed-width tuples

#![cfg(target_os = "linux")]

#[path = "common/bootstrap_ch_fixture.rs"]
mod fx;

use std::fs;
use std::path::Path;
use std::time::Duration;

use anyhow::{Context, Result, ensure};
use walshadow::shadow::Shadow;

const N_ROWS: i32 = 64;
/// Rows surviving DELETE
const N_LIVE: i32 = 32;
/// Dead and aborted tuples
const N_GATED: u64 = 64;

/// Leave dead and aborted tuples unpruned for backup
fn load_gated_workload(source: &Shadow, schema: &str) -> Result<()> {
    let sql = format!(
        "CREATE SCHEMA {schema};\n\
         CREATE TABLE {schema}.t (id int4 PRIMARY KEY, n int8 NOT NULL) \
           WITH (autovacuum_enabled = false);\n\
         ALTER TABLE {schema}.t REPLICA IDENTITY FULL;\n\
         INSERT INTO {schema}.t \
           SELECT g, g * 10 FROM generate_series(1, {N_ROWS}) g;\n\
         DELETE FROM {schema}.t WHERE id % 2 = 0;\n\
         BEGIN;\n\
         INSERT INTO {schema}.t \
           SELECT g, g * 10 FROM generate_series(1001, 1032) g;\n\
         ROLLBACK;\n\
         CHECKPOINT;\n\
         SELECT pg_switch_wal();\n",
    );
    source.apply_schema_dump(&sql)?;
    Ok(())
}

/// Keep relation on page-walk path with fixed-width mapping
fn write_gate_ch_config(path: &Path, ch_port: u16, schema: &str) -> Result<()> {
    let body = format!(
        "[ch]\n\
         host = \"127.0.0.1\"\n\
         port = {ch_port}\n\
         database = \"default\"\n\
         compression = \"lz4\"\n\
         \n\
         [table.\"{schema}\".\"t\"]\n\
         target_database = \"default\"\n\
         target_table = \"t\"\n\
         columns = [\n  \
           {{ attnum = 1, target = \"id\", type = \"Int32\" }},\n  \
           {{ attnum = 2, target = \"n\",  type = \"Int64\" }},\n\
         ]\n",
    );
    fs::write(path, body).with_context(|| format!("write ch-config {}", path.display()))?;
    Ok(())
}

fn create_ch_dest_table(ch: &fx::ChServer) -> Result<()> {
    ch.query("CREATE DATABASE IF NOT EXISTS default")?;
    ch.query(
        "CREATE OR REPLACE TABLE default.t (\
            id Int32,\
            n Int64,\
            _lsn UInt64,\
            _xid UInt32,\
            _commit_ts DateTime64(6, 'UTC'), _is_deleted Bool\
         ) ENGINE = ReplacingMergeTree(_lsn, _is_deleted) ORDER BY id",
    )?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dead_and_aborted_tuples_stay_out_of_ch() {
    if !fx::requirements_available() {
        return;
    }

    let slot = fx::Ports::alloc();
    let tmp = tempfile::tempdir().unwrap();

    let source = fx::start_source(&tmp);
    let _src_stop = fx::StopOnDrop { sh: &source };

    load_gated_workload(&source, "s17").expect("load gated workload");

    let ch_tmp = tempfile::tempdir().unwrap();
    let ch = fx::ChServer::spawn(ch_tmp, slot.ch_tcp, slot.ch_http).expect("spawn ch");
    create_ch_dest_table(&ch).expect("create ch table");

    let ch_config_path = tmp.path().join("ch-config.toml");
    write_gate_ch_config(&ch_config_path, slot.ch_tcp, "s17").expect("write ch-config");

    let daemon = fx::DaemonRun::prepare(tmp.path(), slot.metrics).expect("daemon layout");
    let child = daemon
        .spawn(&source, &ch_config_path, slot.walsender, &[])
        .expect("spawn walshadow-stream");
    let guard = fx::ChildGuard::new(child);

    let result = (|| -> Result<()> {
        fx::wait_for_listen(daemon.metrics_addr, Duration::from_secs(30))
            .context("daemon metrics endpoint never came up")?;

        // Live tuples ship during the walk, so CH parity runs ahead of the
        // gate. Wait on its verdict before reading either
        daemon
            .wait_for_log("bootstrap visibility gate settled", Duration::from_secs(60))
            .context("gate never logged its verdict")?;

        // Hold at live count to catch later ghost rows
        fx::wait_for_ch_value(
            &ch,
            "SELECT count() FROM default.t FINAL WHERE _is_deleted = 0",
            &N_LIVE.to_string(),
            Duration::from_secs(60),
        )?;
        // Ghost rows carry ids the source never committed
        let ghosts = ch
            .query("SELECT count() FROM default.t FINAL WHERE id >= 1000")
            .context("ghost count")?;
        ensure!(ghosts == "0", "aborted rows reached CH: {ghosts}");
        let src_sum = source
            .psql_one("SELECT coalesce(sum(n), 0)::text FROM s17.t")
            .context("source sum")?;
        let ch_sum = ch
            .query("SELECT sum(n) FROM default.t FINAL WHERE _is_deleted = 0")
            .context("ch sum")?;
        ensure!(
            src_sum == ch_sum,
            "sum(n) differs: ch={ch_sum} src={src_sum}"
        );

        // Dead rows require backup pg_xact verdict
        let stderr = daemon.stderr();
        let line = stderr
            .lines()
            .find(|l| l.contains("bootstrap visibility gate settled"))
            .context("gate verdict left the log")?;
        ensure!(
            line.contains(&format!("gated={N_GATED}")),
            "gate verdict off: {line}"
        );
        ensure!(!line.contains("deferred=0"), "nothing deferred: {line}");
        // Fixed-width relation bypasses repair
        ensure!(
            line.contains("pending_relations=0"),
            "relation left the walk: {line}"
        );
        Ok(())
    })();

    fx::finish_daemon(guard, &daemon, result);
}
