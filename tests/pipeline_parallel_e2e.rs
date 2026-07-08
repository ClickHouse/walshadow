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
use walshadow::ch_emitter::TableTarget;
use walshadow::shadow_catalog::RelName;

const SOURCE_PORT: u16 = 17501;
const SHADOW_PORT: u16 = 17502;
const CH_TCP_PORT: u16 = 17503;
const CH_HTTP_PORT: u16 = 17504;
const WALSENDER_PORT: u16 = 17552;

const SLICE_SOURCE_PORT: u16 = 17505;
const SLICE_SHADOW_PORT: u16 = 17506;
const SLICE_CH_TCP_PORT: u16 = 17507;
const SLICE_CH_HTTP_PORT: u16 = 17508;
const SLICE_WALSENDER_PORT: u16 = 17553;

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
        source_table: RelName::new("public", "foo"),
        target_table: TableTarget::new("walshadow_test", "foo"),
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

/// 8192 bytes, comfortably past the ~2KB toast threshold (see toast_e2e).
const TOAST_BODY_SQL: &str = "repeat('walshadow-toast-', 512)";
const TOAST_BODY_LEN: &str = "8192";
/// Fat replacement `meta` forcing the UPDATE's new version onto another
/// page, so the full tuple (incl. the unchanged toast pointer) is logged
/// rather than prefix/suffix-elided (`log_heap_update`); see toast_e2e.
const META2_SQL: &str = "repeat('v2-update-', 60)";

/// Commits sliced into many `DrainedBatch`es (`drain_batch_rows = 1`):
/// every row of a multi-row xact dispatches as its own seq, all but the
/// last registered partial. Proves slice plumbing end-to-end — rows land
/// once each, and the durable watermark still advances (final-slice-only
/// publication; a broken marker would pin the ack at 0).
///
/// The toasted xact drives cross-slice detoast through the decode pool:
/// toasted INSERT, page-packing fillers, then an unchanged-toast UPDATE,
/// all in ONE xact — the update's slice carries no chunks of its own, so
/// its `ExternalToast` pointer must resolve against the chunk generation
/// sealed with the insert's slice (`DecodeJob.chunks` → `detoast_heap`),
/// with no toast store to fall back on (disabled mode). A broken
/// generation lookup NULL-fills the value and the length assertion (on
/// the max-`_lsn` row, not NULL-skipping argMax) catches it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn parallel_pipeline_slices_multi_batch_commit() {
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
        "CREATE TABLE public.slices (id int PRIMARY KEY, val text, meta text);\n\
         ALTER TABLE public.slices ALTER COLUMN val SET STORAGE EXTERNAL;\n",
        SLICE_SOURCE_PORT,
        SLICE_SHADOW_PORT,
        SLICE_WALSENDER_PORT,
    )
    .await;
    let _src_stop = fx::StopOnDrop { sh: &source };
    let _shd_stop = fx::StopOnDrop { sh: &shadow };

    let ch_tmp = tempfile::tempdir().unwrap();
    let ch = fx::ChServer::spawn(ch_tmp, SLICE_CH_TCP_PORT, SLICE_CH_HTTP_PORT).expect("spawn ch");
    ch.query("CREATE DATABASE IF NOT EXISTS walshadow_test")
        .expect("create db");
    ch.query(
        "CREATE OR REPLACE TABLE walshadow_test.slices (\
            id Int32,\
            val Nullable(String),\
            meta Nullable(String),\
            _lsn UInt64,\
            _xid UInt32,\
            _commit_ts DateTime64(6, 'UTC'), _is_deleted Bool\
         ) ENGINE = ReplacingMergeTree(_lsn, _is_deleted) ORDER BY id",
    )
    .expect("create dest table");

    let mappings = vec![fx::TableMappingSpec {
        source_table: RelName::new("public", "slices"),
        target_table: TableTarget::new("walshadow_test", "slices"),
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
            ColumnMapping {
                src_attnum: 3,
                target_name: "meta".into(),
                target_type: "Nullable(String)".into(),
            },
        ],
    }];

    let mut pipeline = fx::build_pipeline_with(
        fx::BuildPipelineArgs {
            tmp: &tmp,
            source: &source,
            shadow: &shadow,
            shadow_filter_dir: &shadow_filter_dir,
            shadow_stream_state,
            ch_database: "walshadow_test",
            ch_tcp_port: SLICE_CH_TCP_PORT,
            mappings,
            app_name: "walshadow-pipeline-slices",
            ddl: None,
        },
        |cfg| cfg.drain_batch_rows = 1,
    )
    .await;

    // First xact: nine rows → nine slices; only the last slice's seq
    // publishes the commit LSN. Second xact (one multi-statement `-c` =
    // one implicit transaction): toasted INSERT, page-packing fillers,
    // unchanged-toast UPDATE — the fat replacement `meta` forces the new
    // version cross-page so the full tuple (with the toast pointer) is
    // logged, and with rows=1 slicing the update's slice holds no chunks.
    let driver = fx::spawn_workload(
        &source,
        vec![
            "INSERT INTO public.slices (id, val) \
             SELECT g, 'v' || g FROM generate_series(1, 9) g"
                .into(),
            format!(
                "INSERT INTO public.slices VALUES (100, {TOAST_BODY_SQL}, 'v1'); \
                 INSERT INTO public.slices (id, meta) \
                 SELECT g, repeat('f', 500) FROM generate_series(101, 116) g; \
                 UPDATE public.slices SET meta = {META2_SQL} WHERE id = 100"
            ),
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
    assert!(shipped >= 1, "no segments shipped in 45s");

    let target = pipeline.stream.dispatched_lsn();
    let observed = shadow
        .wait_for_replay(target, Duration::from_secs(30))
        .expect("shadow replay catches up");
    assert!(observed >= target);

    let ack = pipeline.ack.clone();
    pipeline.shutdown().await.expect("pipeline drains clean");

    let ch_count = ch
        .query("SELECT count() FROM walshadow_test.slices FINAL")
        .expect("ch count");
    assert_eq!(ch_count, "26", "all slices of both sliced commits landed");
    let ch_vals = ch
        .query("SELECT val FROM walshadow_test.slices FINAL ORDER BY id LIMIT 1")
        .expect("ch first val");
    assert_eq!(ch_vals, "v1");
    // Winning (max _lsn) version of id=100 is the UPDATE's. ORDER BY, not
    // argMax: Nullable aggregates skip NULL rows, so argMax(val, _lsn)
    // would silently fall back to the INSERT version on a NULL-filled
    // update (see toast_e2e).
    let winning_meta = ch
        .query(&format!(
            "SELECT meta = {META2_SQL} FROM walshadow_test.slices \
             WHERE id = 100 ORDER BY _lsn DESC LIMIT 1"
        ))
        .expect("ch id=100 meta");
    assert_eq!(winning_meta, "1", "update version won, full tuple logged");
    // Its slice carried no chunks: a full-length value proves the decode
    // pool resolved the pointer against the insert slice's generation.
    let winning_len = ch
        .query(
            "SELECT length(val) FROM walshadow_test.slices \
             WHERE id = 100 ORDER BY _lsn DESC LIMIT 1",
        )
        .expect("ch id=100 val length");
    assert_eq!(
        winning_len, TOAST_BODY_LEN,
        "cross-slice detoast rehydrated the unchanged-toast UPDATE",
    );
    assert!(
        ack.load(std::sync::atomic::Ordering::Acquire) > 0,
        "final-slice publication advanced the durable watermark",
    );
}
