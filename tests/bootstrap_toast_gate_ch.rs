//! Verify greenfield repairs TOAST-owning relations and seeds chunk mirror
//! for later unchanged external pointers

#![cfg(target_os = "linux")]

#[path = "common/bootstrap_ch_fixture.rs"]
mod fx;

use std::fs;
use std::path::Path;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail, ensure};
use walshadow::shadow::Shadow;

/// Force multi-chunk external values
const BODY_REPEAT: u32 = 700;
const N_LIVE: i32 = 4;

/// Leave live, dead, superseded, and aborted values unpruned
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
         UPDATE {schema}.t SET body = repeat('fresh-1---', {BODY_REPEAT}) WHERE id = 1;\n\
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

/// Configure body mapping and ClickHouse chunk store
fn write_toast_ch_config(path: &Path, ch_port: u16, schema: &str) -> Result<()> {
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

fn create_ch_dest_table(ch: &fx::ChServer) -> Result<()> {
    ch.query("CREATE DATABASE IF NOT EXISTS default")?;
    ch.query(
        "CREATE OR REPLACE TABLE default.t (\
            id Int32,\
            body String,\
            _lsn UInt64,\
            _xid UInt32,\
            _commit_ts DateTime64(6, 'UTC'), _is_deleted Bool\
         ) ENGINE = ReplacingMergeTree(_lsn, _is_deleted) ORDER BY id",
    )?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dead_and_aborted_external_values_stay_out_of_ch() {
    if !fx::requirements_available() {
        return;
    }

    let slot = fx::Ports::alloc();
    let tmp = tempfile::tempdir().unwrap();

    let source = fx::start_source(&tmp);
    let _src_stop = fx::StopOnDrop { sh: &source };

    load_toast_workload(&source, "s19").expect("load toast workload");

    let ch_tmp = tempfile::tempdir().unwrap();
    let ch = fx::ChServer::spawn(ch_tmp, slot.ch_tcp, slot.ch_http).expect("spawn ch");
    create_ch_dest_table(&ch).expect("create ch table");

    let ch_config_path = tmp.path().join("ch-config.toml");
    write_toast_ch_config(&ch_config_path, slot.ch_tcp, "s19").expect("write ch-config");

    let daemon = fx::DaemonRun::prepare(tmp.path(), slot.metrics).expect("daemon layout");
    let child = daemon
        .spawn(&source, &ch_config_path, slot.walsender, &[])
        .expect("spawn walshadow-stream");
    let guard = fx::ChildGuard::new(child);

    let result = (|| -> Result<()> {
        fx::wait_for_listen(daemon.metrics_addr, Duration::from_secs(30))
            .context("daemon metrics endpoint never came up")?;

        fx::wait_for_ch_value(
            &ch,
            "SELECT count() FROM default.t FINAL WHERE _is_deleted = 0",
            &N_LIVE.to_string(),
            Duration::from_secs(60),
        )?;
        let ghosts = ch
            .query("SELECT count() FROM default.t FINAL WHERE id >= 100")
            .context("ghost count")?;
        ensure!(ghosts == "0", "dead or aborted rows reached CH: {ghosts}");

        // Compare body bytes to catch wrong generation or short reassembly
        for id in 1..=N_LIVE {
            let want: String = source
                .psql_one(&format!("SELECT md5(body) FROM s19.t WHERE id = {id}"))
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

        // Require latest body version
        let fresh = ch
            .query("SELECT startsWith(body, 'fresh-1---') FROM default.t FINAL WHERE id = 1")
            .context("updated body probe")?;
        ensure!(fresh == "1", "id=1 kept the superseded body");

        // Repair tally proves COPY path ran
        let stderr = daemon.stderr();
        let line = stderr
            .lines()
            .find(|l| l.contains("visibility repair read pending relations"))
            .context("repair never logged its read")?;
        ensure!(line.contains("relations=1"), "repair read nothing: {line}");
        ensure!(
            line.contains(&format!("rows={N_LIVE}")),
            "repair row count off: {line}"
        );

        // The rows came from COPY, the chunks still went to the mirror
        let toast_relid = source
            .psql_one("SELECT reltoastrelid FROM pg_class WHERE oid = 's19.t'::regclass")
            .context("source toast relid")?;
        let mirror = format!("pg_toast_{toast_relid}");
        let created = ch
            .query(&format!(
                "SELECT count() FROM system.tables \
                 WHERE database = 'default' AND name = '{mirror}'"
            ))
            .context("chunk mirror table probe")?;
        ensure!(created == "1", "walk seeded no chunk mirror {mirror}");
        let mirrored = ch
            .query(&format!("SELECT count() FROM default.`{mirror}`"))
            .context("chunk mirror row probe")?;
        ensure!(
            mirrored.parse::<u64>().unwrap_or(0) > 0,
            "chunk mirror {mirror} is empty"
        );

        // Non-TOAST update forces pointer resolution from seeded mirror
        let before = ch
            .query("SELECT max(_lsn) FROM default.t")
            .context("pre-update lsn")?;
        source
            .psql_one("UPDATE s19.t SET id = id WHERE id = 2")
            .context("post-bootstrap update")?;
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            let now = ch
                .query("SELECT max(_lsn) FROM default.t")
                .context("post-update lsn")?;
            if now != before {
                break;
            }
            if Instant::now() >= deadline {
                bail!("post-bootstrap update never reached CH: still at _lsn {before}");
            }
            std::thread::sleep(Duration::from_millis(200));
        }
        let want: String = source
            .psql_one("SELECT md5(body) FROM s19.t WHERE id = 2")
            .context("source body digest after update")?
            .trim()
            .into();
        let got = ch
            .query("SELECT lower(hex(MD5(body))) FROM default.t FINAL WHERE id = 2")
            .context("ch body digest after update")?;
        ensure!(
            got == want,
            "unchanged pointer lost its body: ch={got} source={want}"
        );
        Ok(())
    })();

    fx::finish_daemon(guard, &daemon, result);
}
