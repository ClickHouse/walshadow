//! `TRUNCATE` propagation.
//!
//! Verifies that source `TRUNCATE t` removes CH-side rows. Earlier decoder
//! dropped TRUNCATE silently (no block ref) and CH retained stale rows.
//!
//! Drill shape:
//!   1. CREATE TABLE s14.truncate_t (id bigint PRIMARY KEY, payload text)
//!   2. INSERT 20 pre-truncate rows
//!   3. TRUNCATE TABLE s14.truncate_t                — emits XLOG_HEAP_TRUNCATE
//!   4. INSERT 10 post-truncate rows (id >= 1000)
//!   5. pg_switch_wal
//!
//! Expected CH end state:
//!   `SELECT count() FROM target FINAL WHERE _op != 'delete'` == 10
//!   The 10 surviving ids match source's post-truncate set.

#![cfg(target_os = "linux")]

#[path = "common/inproc_harness.rs"]
mod fx;

use std::time::Duration;

use walshadow::ch_emitter::ColumnMapping;

const SOURCE_PORT: u16 = 17411;
const SHADOW_PORT: u16 = 17412;
const CH_TCP_PORT: u16 = 17413;
const CH_HTTP_PORT: u16 = 17414;
const WALSENDER_PORT: u16 = 17451;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn truncate_removes_ch_rows() {
    if !fx::pg_available() {
        eprintln!("skip: no initdb on PATH");
        return;
    }
    if !fx::pg_basebackup_available() {
        eprintln!("skip: no pg_basebackup on PATH");
        return;
    }
    if !fx::clickhouse_available() {
        eprintln!("skip: no clickhouse binary on PATH");
        return;
    }

    let tmp = tempfile::tempdir().unwrap();
    let (
        fx::BootstrappedClusters {
            source,
            shadow,
            shadow_filter_dir,
        },
        shadow_stream_state,
    ) = fx::bootstrap_clusters(
        &tmp,
        "CREATE SCHEMA s14;\n\
         CREATE TABLE s14.truncate_t (id bigint PRIMARY KEY, payload text);\n\
         ALTER TABLE s14.truncate_t REPLICA IDENTITY FULL;\n",
        SOURCE_PORT,
        SHADOW_PORT,
        WALSENDER_PORT,
    )
    .await;
    let _src_stop = fx::StopOnDrop { sh: &source };
    let _shd_stop = fx::StopOnDrop { sh: &shadow };

    let ch_tmp = tempfile::tempdir().unwrap();
    let ch = fx::ChServer::spawn(ch_tmp, CH_TCP_PORT, CH_HTTP_PORT).expect("spawn ch");
    ch.query("CREATE DATABASE IF NOT EXISTS walshadow_test")
        .expect("create db");
    ch.query(
        "CREATE OR REPLACE TABLE walshadow_test.s14_truncate_t (\
            id Int64,\
            payload Nullable(String),\
            _lsn UInt64,\
            _xid UInt32,\
            _op Enum8('insert' = 1, 'update' = 2, 'delete' = 3),\
            _commit_ts DateTime64(6, 'UTC')\
         ) ENGINE = ReplacingMergeTree(_lsn) ORDER BY id",
    )
    .expect("create dest table");

    let mappings = vec![fx::TableMappingSpec {
        source_table: "s14.truncate_t".into(),
        target_table: "walshadow_test.s14_truncate_t".into(),
        columns: vec![
            ColumnMapping {
                src_attnum: 1,
                target_name: "id".into(),
                target_type: "Int64".into(),
            },
            ColumnMapping {
                src_attnum: 2,
                target_name: "payload".into(),
                target_type: "Nullable(String)".into(),
            },
        ],
    }];

    let mut pipeline = fx::build_pipeline(fx::BuildPipelineArgs {
        tmp: &tmp,
        source: &source,
        shadow: &shadow,
        shadow_filter_dir: &shadow_filter_dir,
        shadow_stream_state,
        ch_database: "walshadow_test",
        ch_tcp_port: CH_TCP_PORT,
        mappings,
        app_name: "walshadow-truncate",
        ddl: None,
    })
    .await;

    // Workload: 20 inserts, TRUNCATE, 10 inserts (id >= 1000), rotate.
    let driver = fx::spawn_workload(
        &source,
        vec![
            "INSERT INTO s14.truncate_t SELECT g, 'pre-'||g::text \
             FROM generate_series(1, 20) g"
                .into(),
            "TRUNCATE TABLE s14.truncate_t".into(),
            "INSERT INTO s14.truncate_t SELECT 1000 + g, 'post-'||(1000+g)::text \
             FROM generate_series(1, 10) g"
                .into(),
            "SELECT pg_switch_wal()".into(),
        ],
    );

    let shipped = fx::pump_segments(&mut pipeline, 1, Duration::from_secs(45)).await;
    let _ = driver.join();
    assert!(
        shipped >= 1,
        "no segments shipped in 45s — pipeline didn't drain",
    );

    let target = pipeline.stream.dispatched_lsn();
    let observed = shadow
        .wait_for_replay(target, Duration::from_secs(30))
        .expect("shadow replay catches up");
    assert!(observed >= target);
    pipeline.shutdown().await.expect("pipeline drains clean");

    let src_count = source
        .psql_one("SELECT count(*) FROM s14.truncate_t")
        .expect("source count");
    assert_eq!(src_count, "10", "source post-truncate count");

    let ch_count = ch
        .query("SELECT count() FROM walshadow_test.s14_truncate_t FINAL WHERE _op != 'delete'")
        .expect("ch count");
    assert_eq!(
        src_count, ch_count,
        "row count mismatch after TRUNCATE: source={src_count}, ch={ch_count}",
    );

    // Surviving ids on CH must be the post-truncate set (1001..=1010).
    let ch_ids = ch
        .query(
            "SELECT groupArray(id) FROM (\
                SELECT id FROM walshadow_test.s14_truncate_t FINAL \
                WHERE _op != 'delete' ORDER BY id\
             )",
        )
        .expect("ch ids");
    assert_eq!(
        ch_ids,
        "[1001,1002,1003,1004,1005,1006,1007,1008,1009,1010]"
    );
}
