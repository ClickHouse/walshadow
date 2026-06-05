//! Parallel decode+insert pipeline end-to-end.
//!
//! The `src/pipeline` fan-out (reorder coordinator → M decode workers →
//! per-table batcher → N inserters → ack collector) replaces the serial
//! `Emitter` tail behind `--ch-config`. The existing `*_e2e` drills all
//! exercise the serial path; this one drives the parallel path through
//! its production wiring: source PG → walshadow filter → shadow PG →
//! heap decoder → xact buffer → `ReorderSink` → decode pool → inserter
//! pool → spawned `clickhouse server`.
//!
//! Workload: INSERT three rows, UPDATE one, DELETE one (under REPLICA
//! IDENTITY FULL), then `pg_switch_wal`. Asserts the surviving rows reach
//! CH and the deleted row's tombstone hides it under `FINAL`.

#![cfg(target_os = "linux")]

#[path = "common/inproc_harness.rs"]
mod fx;

use std::time::Duration;

use walshadow::ch_emitter::ColumnMapping;

const SOURCE_PORT: u16 = 17501;
const SHADOW_PORT: u16 = 17502;
const CH_TCP_PORT: u16 = 17503;
const CH_HTTP_PORT: u16 = 17504;
const WALSENDER_PORT: u16 = 17552;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn parallel_pipeline_replicates_dml() {
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
        "CREATE TABLE public.foo (id int PRIMARY KEY, val text);\n\
         ALTER TABLE public.foo REPLICA IDENTITY FULL;\n",
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
        "CREATE OR REPLACE TABLE walshadow_test.foo (\
            id Int32,\
            val Nullable(String),\
            _lsn UInt64,\
            _xid UInt32,\
            _op Enum8('insert' = 1, 'update' = 2, 'delete' = 3),\
            _commit_ts DateTime64(6, 'UTC')\
         ) ENGINE = ReplacingMergeTree(_lsn) ORDER BY id",
    )
    .expect("create dest table");

    let mappings = vec![fx::TableMappingSpec {
        source_table: "public.foo".into(),
        target_table: "walshadow_test.foo".into(),
        columns: vec![
            ColumnMapping {
                src_attnum: 1,
                target_name: "id".into(),
                target_type: "Int32".into(),
            },
            ColumnMapping {
                src_attnum: 2,
                target_name: "val".into(),
                target_type: "Nullable(String)".into(),
            },
        ],
    }];

    let mut pipeline = fx::build_parallel_pipeline(fx::BuildPipelineArgs {
        tmp: &tmp,
        source: &source,
        shadow: &shadow,
        shadow_filter_dir: &shadow_filter_dir,
        shadow_stream_state,
        ch_database: "walshadow_test",
        ch_tcp_port: CH_TCP_PORT,
        mappings,
        app_name: "walshadow-pipeline-parallel",
        ddl: None,
    })
    .await;

    // Each `-c` is its own autocommit xact, so every COMMIT lands in the
    // same segment as its heap records — one reorder dispatch per stmt.
    let driver = fx::spawn_workload(
        &source,
        vec![
            "INSERT INTO public.foo VALUES (1, 'a'), (2, 'b'), (3, 'c')".into(),
            "UPDATE public.foo SET val = 'B' WHERE id = 2".into(),
            "DELETE FROM public.foo WHERE id = 3".into(),
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
        "no segments shipped in 45s — pipeline didn't drain",
    );

    let target = pipeline.stream.dispatched_lsn();
    let observed = shadow
        .wait_for_replay(target, Duration::from_secs(30))
        .expect("shadow replay catches up");
    assert!(observed >= target);

    // The pipeline's durable watermark lives in the ack-collector atomic;
    // capture it before `shutdown` consumes the pipeline.
    let ack = pipeline.ack.clone();

    // Drain the fan-out: decoders finish, batcher final-flushes, inserters
    // drain to EndOfStream, ack collector exits. A clean join proves no
    // stage tripped fatal mid-run.
    pipeline.shutdown().await.expect("pipeline drains clean");

    let src_count = source
        .psql_one("SELECT count(*) FROM public.foo")
        .expect("source count");
    let ch_count = ch
        .query("SELECT count() FROM walshadow_test.foo FINAL WHERE _op != 'delete'")
        .expect("ch count");
    assert_eq!(src_count, "2", "source has two surviving rows post-delete");
    assert_eq!(src_count, ch_count, "row count mismatched after DML");

    // id=2's UPDATE must win (higher _lsn) under ReplacingMergeTree.
    let id2_val = ch
        .query("SELECT val FROM walshadow_test.foo FINAL WHERE id = 2 AND _op != 'delete'")
        .expect("ch id=2 val");
    assert_eq!(id2_val, "B", "update replicated");

    // id=3 was deleted — its tombstone hides it under the _op filter.
    let id3_live = ch
        .query("SELECT count() FROM walshadow_test.foo FINAL WHERE id = 3 AND _op != 'delete'")
        .expect("ch id=3 count");
    assert_eq!(id3_live, "0", "delete replicated as tombstone");

    // Every dispatched seq drained, so the contiguous-done watermark
    // advanced past its initial 0.
    assert!(
        ack.load(std::sync::atomic::Ordering::Acquire) > 0,
        "durable watermark advanced",
    );
}
