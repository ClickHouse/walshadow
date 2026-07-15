//! Parallel decode+insert pipeline: barrier behaviour end-to-end.
//!
//! `tests/pipeline_parallel_e2e.rs` drives the `src/pipeline` fan-out for
//! pure DML. Production `--ch-config` also routes DDL and TRUNCATE through
//! the same reorder coordinator's `run_barrier`: each schema change /
//! truncate quiesces the pool, fences every earlier seq to durable, then
//! applies the catalog op against ClickHouse. These two drills cover that
//! barrier on the parallel path (M=2 decoders, N=2 inserters), one per
//! `run_barrier` arm:
//!
//! * `parallel_pipeline_schema_evolution_orders_after_data` — ALTER ADD
//!   COLUMN with rows on both sides. Exercises the `ordered_events` /
//!   `apply_event` arm. The post-ALTER INSERT references a column CH only
//!   gains via the barrier's `ALTER`, so a misordered barrier would fail
//!   the INSERT and trip the pipeline fatal (caught by `shutdown`).
//! * `parallel_pipeline_truncate_orders_after_data` — TRUNCATE with rows on
//!   both sides. Exercises the `HeapOp::Truncate` / `apply_truncate` arm.
//!   TRUNCATE carries no `_lsn`, so only correct ordering against the
//!   surrounding inserts yields the right surviving set.
//!
//! Both assert the durable watermark (ack-collector atomic) advanced, so
//! the barrier fence reached durable rather than hanging.

#![cfg(target_os = "linux")]

#[path = "common/inproc_harness.rs"]
mod fx;

use std::sync::atomic::Ordering;
use std::time::Duration;

use walshadow::mapping::ColumnMapping;
use walshadow::mapping::TableTarget;
use walshadow::schema::RelName;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn parallel_pipeline_schema_evolution_orders_after_data() {
    const SOURCE_PORT: u16 = 17561;
    const SHADOW_PORT: u16 = 17562;
    const CH_TCP_PORT: u16 = 17563;
    const CH_HTTP_PORT: u16 = 17564;
    const WALSENDER_PORT: u16 = 17568;

    if !fx::pg_available() || !fx::pg_basebackup_available() || !fx::clickhouse_available() {
        eprintln!("skip: missing initdb / pg_basebackup / clickhouse");
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
        "CREATE SCHEMA s19;\n\
         CREATE TABLE s19.orders (id bigint PRIMARY KEY, payload text);\n\
         ALTER TABLE s19.orders REPLICA IDENTITY FULL;\n",
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
    // Pre-create the CH dest with only the original two columns — the
    // applicator must add `c` when the ALTER event drains through the
    // barrier, before the post-ALTER INSERT can land.
    ch.query(
        "CREATE OR REPLACE TABLE walshadow_test.s19_orders (\
            id Int64,\
            payload Nullable(String),\
            _lsn UInt64,\
            _xid UInt32,\
            _commit_ts DateTime64(6, 'UTC'), _is_deleted Bool\
         ) ENGINE = ReplacingMergeTree(_lsn, _is_deleted) ORDER BY id",
    )
    .expect("create dest");

    let mappings = vec![fx::TableMappingSpec {
        source_table: RelName::new("s19", "orders"),
        target_table: TableTarget::new("walshadow_test", "s19_orders"),
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
            // No `c` — the barrier's applicator must auto-extend.
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
        app_name: "walshadow-parallel-ddl-alter",
        ddl: Some(fx::DdlPipelineArgs::default()),
    })
    .await;

    // Rows on both sides of the ALTER. Pre rows encode against the 2-column
    // shape (c → NULL on CH); post rows carry `c`, so their INSERT only
    // succeeds once the barrier's ALTER has reached CH.
    let driver = fx::spawn_workload(
        &source,
        vec![
            "INSERT INTO s19.orders (id, payload) VALUES (1, 'p1'), (2, 'p2'), (3, 'p3')".into(),
            "ALTER TABLE s19.orders ADD COLUMN c text".into(),
            "INSERT INTO s19.orders (id, payload, c) \
             VALUES (11, 'q1', 'c11'), (12, 'q2', 'c12'), (13, 'q3', 'c13')"
                .into(),
            "SELECT pg_switch_wal()".into(),
        ],
    );

    let shipped = fx::pump_until(
        &mut pipeline.feed,
        &mut pipeline.stream,
        &mut pipeline.sinks,
        &mut pipeline.segment_sink,
        &mut pipeline.chunk_buf,
        1,
        Duration::from_secs(45),
    )
    .await;
    let _ = driver.join();
    assert!(
        shipped >= 1,
        "no segments shipped in 45s — pipeline didn't drain"
    );

    let target = pipeline.stream.dispatched_lsn();
    let observed = shadow
        .wait_for_replay(target, Duration::from_secs(30))
        .expect("shadow replay catches up");
    assert!(observed >= target);

    let ack = pipeline.ack.clone();
    pipeline.shutdown().await.expect("pipeline drains clean");

    // Barrier applied the ALTER to CH.
    let cols = ch
        .query(
            "SELECT name FROM system.columns \
             WHERE database = 'walshadow_test' AND table = 's19_orders' AND name = 'c'",
        )
        .expect("ch system.columns");
    assert_eq!(cols, "c", "barrier must have added column `c` to CH");

    let src_count = source.psql_one("SELECT count(*) FROM s19.orders").unwrap();
    let ch_count = ch
        .query("SELECT count() FROM walshadow_test.s19_orders FINAL WHERE _is_deleted = 0")
        .expect("ch count");
    assert_eq!(src_count, "6", "source has all six rows");
    assert_eq!(src_count, ch_count, "row count mismatch after ALTER");

    // Post-ALTER rows carry `c`; their arrival proves the ALTER ordered
    // before them (else the INSERT would reference a missing column).
    let post = ch
        .query(
            "SELECT groupArray(c) FROM (\
                SELECT argMax(c, _lsn) AS c FROM walshadow_test.s19_orders \
                WHERE _is_deleted = 0 AND id IN (11, 12, 13) GROUP BY id ORDER BY id\
             )",
        )
        .expect("ch post-alter c");
    assert_eq!(post, "['c11','c12','c13']", "post-ALTER rows must carry c");

    // Pre-ALTER row predates the column — NULL on CH (no DEFAULT supplied).
    let pre = ch
        .query(
            "SELECT argMax(c, _lsn) FROM walshadow_test.s19_orders \
             WHERE _is_deleted = 0 AND id = 1",
        )
        .expect("ch pre-alter c");
    assert!(
        pre.is_empty() || pre == "\\N" || pre == "NULL",
        "pre-ALTER row's c should be NULL on CH, got {pre:?}"
    );

    assert!(
        ack.load(Ordering::Acquire) > 0,
        "durable watermark advanced through the barrier",
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn parallel_pipeline_truncate_orders_after_data() {
    const SOURCE_PORT: u16 = 17571;
    const SHADOW_PORT: u16 = 17572;
    const CH_TCP_PORT: u16 = 17573;
    const CH_HTTP_PORT: u16 = 17574;
    const WALSENDER_PORT: u16 = 17578;

    if !fx::pg_available() || !fx::pg_basebackup_available() || !fx::clickhouse_available() {
        eprintln!("skip: missing initdb / pg_basebackup / clickhouse");
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
        "CREATE SCHEMA s19t;\n\
         CREATE TABLE s19t.t (id bigint PRIMARY KEY, payload text);\n\
         ALTER TABLE s19t.t REPLICA IDENTITY FULL;\n",
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
        "CREATE OR REPLACE TABLE walshadow_test.s19t_t (\
            id Int64,\
            payload Nullable(String),\
            _lsn UInt64,\
            _xid UInt32,\
            _commit_ts DateTime64(6, 'UTC'), _is_deleted Bool\
         ) ENGINE = ReplacingMergeTree(_lsn, _is_deleted) ORDER BY id",
    )
    .expect("create dest table");

    let mappings = vec![fx::TableMappingSpec {
        source_table: RelName::new("s19t", "t"),
        target_table: TableTarget::new("walshadow_test", "s19t_t"),
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

    // TRUNCATE rides the barrier as a heap op — no schema-event subscription
    // needed, so the DML-only wiring (`ddl: None`) suffices.
    let mut pipeline = fx::build_pipeline(fx::BuildPipelineArgs {
        tmp: &tmp,
        source: &source,
        shadow: &shadow,
        shadow_filter_dir: &shadow_filter_dir,
        shadow_stream_state,
        ch_database: "walshadow_test",
        ch_tcp_port: CH_TCP_PORT,
        mappings,
        app_name: "walshadow-parallel-truncate",
        ddl: None,
    })
    .await;

    // 20 pre-truncate rows, TRUNCATE, 10 post-truncate rows (id >= 1000).
    // Only correct barrier ordering wipes the first set and keeps the last.
    let driver = fx::spawn_workload(
        &source,
        vec![
            "INSERT INTO s19t.t SELECT g, 'pre-'||g::text FROM generate_series(1, 20) g".into(),
            "TRUNCATE TABLE s19t.t".into(),
            "INSERT INTO s19t.t SELECT 1000 + g, 'post-'||(1000+g)::text \
             FROM generate_series(1, 10) g"
                .into(),
            "SELECT pg_switch_wal()".into(),
        ],
    );

    let shipped = fx::pump_until(
        &mut pipeline.feed,
        &mut pipeline.stream,
        &mut pipeline.sinks,
        &mut pipeline.segment_sink,
        &mut pipeline.chunk_buf,
        1,
        Duration::from_secs(45),
    )
    .await;
    let _ = driver.join();
    assert!(
        shipped >= 1,
        "no segments shipped in 45s — pipeline didn't drain"
    );

    let target = pipeline.stream.dispatched_lsn();
    let observed = shadow
        .wait_for_replay(target, Duration::from_secs(30))
        .expect("shadow replay catches up");
    assert!(observed >= target);

    let ack = pipeline.ack.clone();
    let stats = pipeline.stats.clone();
    pipeline.shutdown().await.expect("pipeline drains clean");

    let src_count = source
        .psql_one("SELECT count(*) FROM s19t.t")
        .expect("source count");
    assert_eq!(src_count, "10", "source post-truncate count");

    // The barrier's TRUNCATE bumps the emitter counter on the parallel path.
    assert!(
        stats.truncates_emitted.load(Ordering::Relaxed) >= 1,
        "emitter truncates_emitted live on pipeline (got {})",
        stats.truncates_emitted.load(Ordering::Relaxed),
    );

    let ch_count = ch
        .query("SELECT count() FROM walshadow_test.s19t_t FINAL WHERE _is_deleted = 0")
        .expect("ch count");
    assert_eq!(
        src_count, ch_count,
        "row count mismatch after TRUNCATE: source={src_count}, ch={ch_count}",
    );

    // Surviving ids must be exactly the post-truncate set, proving the
    // TRUNCATE ordered after the pre rows and before the post rows.
    let ch_ids = ch
        .query(
            "SELECT groupArray(id) FROM (\
                SELECT id FROM walshadow_test.s19t_t FINAL \
                WHERE _is_deleted = 0 ORDER BY id\
             )",
        )
        .expect("ch ids");
    assert_eq!(
        ch_ids,
        "[1001,1002,1003,1004,1005,1006,1007,1008,1009,1010]"
    );

    assert!(
        ack.load(Ordering::Acquire) > 0,
        "durable watermark advanced through the barrier",
    );
}
