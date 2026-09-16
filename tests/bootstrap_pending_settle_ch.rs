//! Retain rows below backup redo until commit or rollback, then drop pending tables

#![cfg(target_os = "linux")]

#[path = "common/bootstrap_ch_fixture.rs"]
mod fx;

use std::net::SocketAddr;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail, ensure};
use walshadow::mapping::TableTarget;
use walshadow::schema::RelName;
use walshadow::source_feed::open_sql_client;

const N_ROWS: i32 = 64;
/// Keep both writers open across backup window
const MAX_RATE_KIB: &str = "32768";
const SCHEMA: &str = "s25";

/// Exercise `CREATE … AS` with an inheritable primary key
fn create_keyed_dest_table(ch: &fx::ChServer) -> Result<()> {
    ch.query("CREATE DATABASE IF NOT EXISTS default")?;
    ch.query(
        "CREATE OR REPLACE TABLE default.t (\
            id Int32,\
            name String,\
            _lsn UInt64,\
            _xid UInt32,\
            _commit_ts DateTime64(6, 'UTC'), _is_deleted Bool\
         ) ENGINE = ReplacingMergeTree(_lsn, _is_deleted) ORDER BY id PRIMARY KEY id",
    )?;
    Ok(())
}

fn wait_for_metric(addr: SocketAddr, name: &str, least: u64, timeout: Duration) -> Result<u64> {
    let start = Instant::now();
    let mut seen = None;
    while start.elapsed() < timeout {
        let body = fx::http_get(addr, "/metrics")?;
        let value = fx::parse_metric(&body, name);
        if value.is_some_and(|v| v >= least) {
            return Ok(value.expect("checked"));
        }
        seen = value;
        std::thread::sleep(Duration::from_millis(200));
    }
    bail!("{name} never reached {least}, last {seen:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pending_rows_promote_on_commit_and_never_publish_on_rollback() {
    if !fx::requirements_available() {
        return;
    }

    let slot = fx::Ports::alloc();
    let tmp = tempfile::tempdir().unwrap();

    let source = fx::start_source(&tmp);
    let _src_stop = fx::StopOnDrop { sh: &source };

    fx::load_source_workload(&source, SCHEMA, N_ROWS).expect("load source workload");

    let ch_tmp = tempfile::tempdir().unwrap();
    let ch = fx::ChServer::spawn(ch_tmp, slot.ch_tcp, slot.ch_http).expect("spawn ch");
    create_keyed_dest_table(&ch).expect("create ch table");

    let ch_config_path = tmp.path().join("ch-config.toml");
    fx::write_ch_config_toml(
        &ch_config_path,
        "127.0.0.1",
        slot.ch_tcp,
        "default",
        &RelName::new(SCHEMA, "t"),
        &TableTarget::new("default", "t"),
    )
    .expect("write ch-config");

    // Backup checkpoint flushes open writers' tuples below redo point
    let committer = open_sql_client(&fx::pg_cfg(&source, "pending-committer"))
        .await
        .expect("committer connect");
    let aborter = open_sql_client(&fx::pg_cfg(&source, "pending-aborter"))
        .await
        .expect("aborter connect");
    for (client, tag, from) in [(&committer, "kept", 1001), (&aborter, "rolled", 2001)] {
        client.batch_execute("BEGIN").await.expect("BEGIN");
        client
            .batch_execute(&format!(
                "INSERT INTO {SCHEMA}.t SELECT g, '{tag}-'||g::text \
                 FROM generate_series({from}, {}) g",
                from + 9
            ))
            .await
            .expect("below-redo INSERT");
    }
    source
        .apply_schema_dump("CHECKPOINT;\n")
        .expect("checkpoint uncommitted pages");

    let daemon = fx::DaemonRun::prepare(tmp.path(), slot.metrics).expect("daemon layout");
    let child = daemon
        .spawn(
            &source,
            &ch_config_path,
            slot.walsender,
            &[
                "--bootstrap-max-rate-kib",
                MAX_RATE_KIB,
                "--bootstrap-wind-down-secs",
                "0",
                // Holds source WAL so the window leg reads live rather than
                // falling back to landed segments
                "--slot",
                "walshadow_pending",
            ],
        )
        .expect("spawn walshadow-stream");
    let guard = fx::ChildGuard::new(child);

    let result: Result<()> = async {
        fx::wait_for_listen(daemon.metrics_addr, Duration::from_secs(120))
            .context("daemon metrics endpoint never came up")?;

        let rows = wait_for_metric(
            daemon.metrics_addr,
            "walshadow_pending_rows_total",
            20,
            Duration::from_secs(60),
        )
        .context("pending rows never reached ClickHouse")?;
        ensure!(rows >= 20, "retained only {rows} of 20 undecided rows");
        let pending_columns = ch.query(
            "SELECT count() FROM system.columns WHERE database = 'default' \
             AND table = 't__wspending' AND name IN ('_ws_xmin', '_ws_xmax', '_ws_infomask')",
        )?;
        ensure!(
            pending_columns == "3",
            "pending metadata columns: {pending_columns}"
        );
        let published = ch.query("SELECT count() FROM default.t FINAL WHERE id >= 1001")?;
        ensure!(
            published == "0",
            "undecided rows published before their outcome: {published}"
        );

        committer.batch_execute("COMMIT").await.context("COMMIT")?;
        aborter
            .batch_execute("ROLLBACK")
            .await
            .context("ROLLBACK")?;

        fx::wait_for_ch_value(
            &ch,
            "SELECT count() FROM default.t FINAL WHERE id BETWEEN 1001 AND 1010",
            "10",
            Duration::from_secs(90),
        )
        .context("committed pending rows never promoted")?;

        fx::wait_for_ch_value(
            &ch,
            "SELECT count() FROM system.tables WHERE database = 'default' \
             AND name = 't__wspending'",
            "0",
            Duration::from_secs(90),
        )
        .context("pending table outlived its outstanding set")?;

        let rolled =
            ch.query("SELECT count() FROM default.t FINAL WHERE id BETWEEN 2001 AND 2010")?;
        ensure!(rolled == "0", "rolled-back rows published: {rolled}");

        // Counters publish on the status tick, so they trail the CH state
        wait_for_metric(
            daemon.metrics_addr,
            "walshadow_pending_xacts_settled_total",
            2,
            Duration::from_secs(30),
        )
        .context("both outcomes should be settled")?;
        wait_for_metric(
            daemon.metrics_addr,
            "walshadow_pending_tables_dropped_total",
            1,
            Duration::from_secs(30),
        )
        .context("pending table drop uncounted")?;

        let body = fx::http_get(daemon.metrics_addr, "/metrics")?;
        let undecidable = fx::parse_metric(&body, "walshadow_pending_undecidable_xids");
        ensure!(
            undecidable == Some(0),
            "shadow pg_xact covered every deciding xid, metric {undecidable:?}"
        );

        fx::assert_ch_matches_source(&ch, &source, &format!("{SCHEMA}.t"), "default.t")
            .context("source vs CH parity after settling")
    }
    .await;

    fx::finish_daemon(guard, &daemon, result);
}
