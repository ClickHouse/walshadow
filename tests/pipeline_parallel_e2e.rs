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
//! Workload: INSERT three rows, UPDATE one, DELETE one (under default
//! REPLICA IDENTITY, keyed by the PK — FULL no longer required), then
//! `pg_switch_wal`. Asserts the surviving rows reach CH and the deleted
//! row's key-only tombstone hides it under `FINAL`.

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
        // `foo` stays on default REPLICA IDENTITY: its PK keys the delete
        // tombstone, so the WAL old image carries only `id`. `bar` is
        // intentionally left out of the CH mappings below so its rows
        // exercise the decode pool's unmapped-relation skip counter.
        "CREATE TABLE public.foo (id int PRIMARY KEY, val text);\n\
         CREATE TABLE public.bar (id int);\n",
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
            _commit_ts DateTime64(6, 'UTC'), _is_deleted Bool\
         ) ENGINE = ReplacingMergeTree(_lsn, _is_deleted) ORDER BY id \
           SETTINGS allow_experimental_replacing_merge_with_cleanup = 1",
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

    let mut pipeline = fx::build_pipeline(fx::BuildPipelineArgs {
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
            // Unmapped relation: decoded, then skipped (bumps the counter).
            "INSERT INTO public.bar VALUES (7)".into(),
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
    // capture it (and the emitter counters) before `shutdown` consumes the
    // pipeline.
    let ack = pipeline.ack.clone();
    let stats = pipeline.stats.clone();

    // Drain the fan-out: decoders finish, batcher final-flushes, inserters
    // drain to EndOfStream, ack collector exits. A clean join proves no
    // stage tripped fatal mid-run.
    pipeline.shutdown().await.expect("pipeline drains clean");

    let src_count = source
        .psql_one("SELECT count(*) FROM public.foo")
        .expect("source count");
    let ch_count = ch
        .query("SELECT count() FROM walshadow_test.foo FINAL WHERE _is_deleted = 0")
        .expect("ch count");
    assert_eq!(src_count, "2", "source has two surviving rows post-delete");
    assert_eq!(src_count, ch_count, "row count mismatched after DML");

    // id=2's UPDATE must win (higher _lsn) under ReplacingMergeTree.
    let id2_val = ch
        .query("SELECT val FROM walshadow_test.foo FINAL WHERE id = 2 AND _is_deleted = 0")
        .expect("ch id=2 val");
    assert_eq!(id2_val, "B", "update replicated");

    // id=3 was deleted — its tombstone hides it under the _is_deleted filter.
    let id3_live = ch
        .query("SELECT count() FROM walshadow_test.foo FINAL WHERE id = 3 AND _is_deleted = 0")
        .expect("ch id=3 count");
    assert_eq!(id3_live, "0", "delete replicated as tombstone");

    // Emitter codes the delete into ReplacingMergeTree's `_is_deleted`:
    // id=3's tombstone carries true. Can't observe via FINAL, with
    // `_is_deleted` as engine arg FINAL drops the deleted row at query time,
    // not only on CLEANUP. Read winning (max `_lsn`) version raw instead.
    let id3_flag = ch
        .query("SELECT argMax(_is_deleted, _lsn) FROM walshadow_test.foo WHERE id = 3")
        .expect("ch id=3 _is_deleted");
    assert_eq!(id3_flag, "true", "delete coded into _is_deleted");

    // Default identity logs only the PK in the delete's old image, so the
    // tombstone's non-key column lands NULL (clickhouse client prints \N).
    let id3_tombstone_val = ch
        .query("SELECT val FROM walshadow_test.foo WHERE id = 3 AND _is_deleted")
        .expect("ch id=3 tombstone val");
    assert_eq!(id3_tombstone_val, "\\N", "tombstone carries key only");

    // `OPTIMIZE … FINAL CLEANUP` (gated on the table's experimental
    // setting) physically drops the deleted row + its history, so an
    // unfiltered scan no longer sees id=3 — the end-to-end payoff of
    // wiring the deletion column.
    ch.query("OPTIMIZE TABLE walshadow_test.foo FINAL CLEANUP")
        .expect("optimize cleanup");
    let id3_physical = ch
        .query("SELECT count() FROM walshadow_test.foo WHERE id = 3")
        .expect("ch id=3 physical count");
    assert_eq!(id3_physical, "0", "cleanup purged the tombstone");
    // Survivors untouched by cleanup; no FINAL/filter needed post-purge.
    let survivors = ch
        .query("SELECT count() FROM walshadow_test.foo")
        .expect("ch survivor count");
    assert_eq!(survivors, "2", "cleanup kept the two live rows");

    // Every dispatched seq drained, so the contiguous-done watermark
    // advanced past its initial 0.
    assert!(
        ack.load(std::sync::atomic::Ordering::Acquire) > 0,
        "durable watermark advanced",
    );

    // Emitter Prometheus counters stay live on the parallel path (reorder
    // bumps xacts per commit; inserters bump rows/blocks post-EndOfStream).
    // INSERT + UPDATE + DELETE are three mapped commits, so the previously
    // stuck-at-0 xacts counter must clear.
    use std::sync::atomic::Ordering;
    assert!(
        stats.xacts_committed.load(Ordering::Relaxed) >= 3,
        "emitter_xacts_total live on pipeline (got {})",
        stats.xacts_committed.load(Ordering::Relaxed),
    );
    assert!(
        stats.rows_emitted.load(Ordering::Relaxed) > 0,
        "emitter_rows_total live on pipeline",
    );
    assert!(
        stats.blocks_sent.load(Ordering::Relaxed) > 0,
        "emitter_blocks_total live on pipeline",
    );
    // public.bar has no CH mapping, so its row lands on the decode pool's
    // unmapped skip — previously invisible on the parallel path.
    assert!(
        stats.unsupported_relations.load(Ordering::Relaxed) > 0,
        "emitter_unsupported_relations_total live on pipeline",
    );
}
