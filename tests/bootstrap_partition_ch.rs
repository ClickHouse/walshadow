//! Greenfield bootstrap loads partitions into their root's destination,
//! including rows still undecided at backup

#![cfg(target_os = "linux")]

#[path = "common/bootstrap_ch_fixture.rs"]
mod fx;

use std::time::Duration;

use anyhow::{Context, Result, ensure};
use walshadow::source_feed::open_sql_client;

const SCHEMA: &str = "s27";
const INSTALL_SQL: &str = include_str!("../sql/runtime_config_install.sql");
const LIVE: &str = "FROM default.m FINAL WHERE _is_deleted = 0";
const REGION: &str = "(ARRAY['eu', 'us', 'x'])[g % 3 + 1]";

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bootstrap_routes_partitions_to_root() {
    if !fx::tools::requirements_available() {
        return;
    }

    let slot = fx::Ports::alloc();
    let tmp = tempfile::tempdir().unwrap();

    let source = fx::start_source(&tmp);
    let _src_stop = fx::StopOnDrop { sh: &source };
    source
        .apply_schema_dump(&format!(
            "{INSTALL_SQL}
             INSERT INTO walshadow.config_global (detach_strategy) VALUES ('retain');
             INSERT INTO walshadow.config_namespace
               (namespace, target_database, auto_create, auto_create_name, detach_strategy)
               VALUES ('{SCHEMA}', 'default', true, '$table$', 'retain');
             INSERT INTO walshadow.config_table (namespace, relname, route)
               VALUES ('{SCHEMA}', 'm_eu', 'root');
             CREATE SCHEMA {SCHEMA};
             CREATE TABLE {SCHEMA}.m (id int, region text, v text, PRIMARY KEY (id, region))
               PARTITION BY LIST (region);
             CREATE TABLE {SCHEMA}.m_eu PARTITION OF {SCHEMA}.m FOR VALUES IN ('eu');
             CREATE TABLE {SCHEMA}.m_am PARTITION OF {SCHEMA}.m FOR VALUES IN ('us')
               PARTITION BY LIST (region);
             CREATE TABLE {SCHEMA}.m_us PARTITION OF {SCHEMA}.m_am FOR VALUES IN ('us');
             CREATE TABLE {SCHEMA}.m_x PARTITION OF {SCHEMA}.m FOR VALUES IN ('x');
             INSERT INTO {SCHEMA}.m
               SELECT g, {REGION}, 'v' || g
               FROM generate_series(1, 300) g;"
        ))
        .expect("load source workload");

    let ch_tmp = tempfile::tempdir().unwrap();
    let ch = fx::ChServer::spawn(ch_tmp, slot.ch_tcp, slot.ch_http).expect("spawn ch");
    let ch_config_path = tmp.path().join("ch-config.toml");
    std::fs::write(
        &ch_config_path,
        format!(
            "[ch]
             host = \"127.0.0.1\"
             port = {}
             database = \"default\"

             [runtime_config]
             schema = \"walshadow\"

             [namespace.{SCHEMA}]
             target_database = \"default\"
             auto_create = true
             auto_create_name = \"$table$\"

             [table.{SCHEMA}.m_x]
             replicate = false
            ",
            slot.ch_tcp
        ),
    )
    .unwrap();

    // Undecided at backup checkpoint, in a routed and an excluded partition
    let writer = open_sql_client(&fx::pg_cfg(&source, "pending-partitions"))
        .await
        .expect("writer connect");
    writer.batch_execute("BEGIN").await.expect("BEGIN");
    writer
        .batch_execute(&format!(
            "INSERT INTO {SCHEMA}.m SELECT g, {REGION}, 'pending' \
             FROM generate_series(1001, 1030) g"
        ))
        .await
        .expect("below-redo INSERT");
    source
        .apply_schema_dump("CHECKPOINT;\n")
        .expect("checkpoint uncommitted pages");

    let daemon = fx::DaemonRun::prepare(tmp.path(), slot.metrics).expect("daemon layout");
    let args = [
        "--bootstrap-wind-down-secs",
        "0",
        "--slot",
        "walshadow_partitions",
    ];
    let child = daemon
        .spawn(&source, &ch_config_path, slot.walsender, &args)
        .expect("spawn walshadow-stream");
    let guard = fx::ChildGuard::new(child);

    let result: Result<()> = async {
        fx::wait_for_listen(daemon.metrics_addr, Duration::from_secs(120))
            .context("daemon metrics endpoint never came up")?;
        let eu = source.psql_one(&format!("SELECT '{SCHEMA}.m_eu'::regclass::oid"))?;
        let us = source.psql_one(&format!("SELECT '{SCHEMA}.m_us'::regclass::oid"))?;
        fx::wait_for_ch_value(
            &ch,
            &format!("EXISTS TABLE default.m__wspending_{eu}"),
            "1",
            Duration::from_secs(60),
        )
        .context("partition rows never staged apart")?;

        writer.batch_execute("COMMIT").await.context("COMMIT")?;
        fx::wait_for_ch_value(
            &ch,
            &format!("SELECT count() {LIVE}"),
            "220",
            Duration::from_secs(90),
        )
        .context("routed partitions never reached root")?;
        let leaves = ch.query(&format!(
            "SELECT groupArray(DISTINCT _leaf_oid) FROM (SELECT _leaf_oid {LIVE} ORDER BY _leaf_oid)"
        ))?;
        ensure!(leaves == format!("[{eu},{us}]"), "leaf oids {leaves}");
        let tables = ch.query(
            "SELECT groupArray(name) FROM system.tables \
             WHERE database = 'default' AND name LIKE 'm%' AND name NOT LIKE '%wspending%'",
        )?;
        ensure!(tables == "['m']", "partitions own no destination: {tables}");
        Ok(())
    }
    .await;

    fx::finish_daemon(guard, &daemon, result);

    // Older logs lack partition ancestry
    let tail = daemon.spill_dir.join(walshadow::desc_log::TAIL_FILE);
    let mut bytes = std::fs::read(&tail).unwrap();
    bytes[2..4].copy_from_slice(&3u16.to_le_bytes());
    std::fs::write(&tail, bytes).unwrap();
    let child = daemon
        .spawn(&source, &ch_config_path, slot.walsender, &args)
        .expect("respawn walshadow-stream");
    let guard = fx::ChildGuard::new(child);
    let result = daemon
        .wait_for_log("predates partition routing", Duration::from_secs(60))
        .context("resume over a pre-partition descriptor log");
    fx::finish_daemon(guard, &daemon, result);
}
