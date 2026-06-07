//! Read-time defaults (`atthasmissing` / `attmissingval`).
//!
//! Verifies that fast-path `ALTER TABLE ADD COLUMN c int DEFAULT 7`
//! lands pre-ALTER rows with c=7 on CH (not NULL), via the
//! `attmissingval` substitution path in `decode_tuple_payload`.
//!
//! Drill shape:
//!   1. CREATE TABLE s14.t (id bigint PRIMARY KEY, payload text)
//!   2. INSERT (1, 'pre')                              — pre-ALTER row
//!   3. ALTER TABLE s14.t ADD COLUMN c int DEFAULT 7   — fast-path
//!   4. INSERT (2, 'post', 42)                         — post-ALTER
//!   5. UPDATE s14.t SET payload = 'pre-touched' WHERE id = 1
//!      Row id=1 still has natts=2 in the heap (no rewrite); decoder
//!      substitutes attmissingval[1]=7 for column c on the new image.
//!
//! Expected CH end state:
//!   id=1: c = 7   — read-time default applied by decoder
//!   id=2: c = 42  — heap tuple carries explicit value

#![cfg(target_os = "linux")]

#[path = "common/inproc_harness.rs"]
mod fx;

use std::time::Duration;

use walshadow::ch_emitter::ColumnMapping;

const SOURCE_PORT: u16 = 17401;
const SHADOW_PORT: u16 = 17402;
const CH_TCP_PORT: u16 = 17409;
const CH_HTTP_PORT: u16 = 17410;
const WALSENDER_PORT: u16 = 17450;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn add_column_default_replicates_pre_alter_default() {
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
         CREATE TABLE s14.t (id bigint PRIMARY KEY, payload text);\n\
         ALTER TABLE s14.t REPLICA IDENTITY FULL;\n",
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
        "CREATE OR REPLACE TABLE walshadow_test.s14_t (\
            id Int64,\
            payload Nullable(String),\
            c Nullable(Int32),\
            _lsn UInt64,\
            _xid UInt32,\
            _op Enum8('insert' = 1, 'update' = 2, 'delete' = 3),\
            _commit_ts DateTime64(6, 'UTC')\
         ) ENGINE = ReplacingMergeTree(_lsn) ORDER BY id",
    )
    .expect("create dest table");

    let mappings = vec![fx::TableMappingSpec {
        source_table: "s14.t".into(),
        target_table: "walshadow_test.s14_t".into(),
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
            ColumnMapping {
                src_attnum: 3,
                target_name: "c".into(),
                target_type: "Nullable(Int32)".into(),
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
        app_name: "walshadow-add-col-default",
        ddl: None,
    })
    .await;

    // Workload: pre-ALTER INSERT, ALTER, post-ALTER INSERT, UPDATE on
    // the pre-ALTER row, then pg_switch_wal to seal the segment.
    let driver = fx::spawn_workload(
        &source,
        vec![
            "INSERT INTO s14.t (id, payload) VALUES (1, 'pre')".into(),
            "ALTER TABLE s14.t ADD COLUMN c int DEFAULT 7".into(),
            "INSERT INTO s14.t (id, payload, c) VALUES (2, 'post', 42)".into(),
            "UPDATE s14.t SET payload = 'pre-touched' WHERE id = 1".into(),
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
        .psql_one("SELECT count(*) FROM s14.t")
        .expect("source count");
    let ch_count = ch
        .query("SELECT count() FROM walshadow_test.s14_t FINAL WHERE _op != 'delete'")
        .expect("ch count");
    assert_eq!(src_count, ch_count, "row count mismatch");
    assert_eq!(src_count, "2");

    // Pre-ALTER row: catalog now reports natts=3 (post-ALTER), but the
    // UPDATE's new-image WAL record carries natts=2 (fast-path: heap is
    // not rewritten). Decoder's `decode_tuple_payload` fills attnum=3
    // from `attmissingval` → c=7.
    let ch_pre = ch
        .query(
            "SELECT argMax(c, _lsn) \
             FROM walshadow_test.s14_t \
             WHERE _op != 'delete' AND id = 1",
        )
        .expect("ch pre-alter c");
    assert_eq!(
        ch_pre, "7",
        "pre-ALTER row's c column must surface the attmissingval default after item 1",
    );
    let ch_post = ch
        .query(
            "SELECT argMax(c, _lsn) \
             FROM walshadow_test.s14_t \
             WHERE _op != 'delete' AND id = 2",
        )
        .expect("ch post-alter c");
    assert_eq!(ch_post, "42");
}
