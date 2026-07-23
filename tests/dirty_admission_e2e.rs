//! Dirty-admission end-to-end against real WAL: catalog touch inside a
//! transaction tree defers that tree's later rows (raw spill → decode at
//! commit resolution) without leaking onto interleaved clean
//! transactions; subxact touch defers top and child records; top-level
//! abort with DDL appends no descriptor metadata and emits nothing.
//!
//! Deferred rows decode against the commit-time descriptor and deliver —
//! the `BEGIN; DDL; DML; COMMIT` row-loss class raw decode exists to
//! kill.

#![cfg(target_os = "linux")]

#[path = "common/inproc_harness.rs"]
mod fx;

use fx::spawn_txn;
use std::sync::atomic::Ordering;
use std::time::Duration;

use walshadow::mapping::NamespaceMapping;
use walshadow::shadow::Shadow;

const SLOT_INTERLEAVE: PortSlot = PortSlot {
    source: 18030,
    shadow: 18031,
    ch_tcp: 18032,
    ch_http: 18033,
    walsender: 18037,
};
const SLOT_SUBXACT: PortSlot = PortSlot {
    source: 18040,
    shadow: 18041,
    ch_tcp: 18042,
    ch_http: 18043,
    walsender: 18047,
};
const SLOT_TOP_ABORT: PortSlot = PortSlot {
    source: 18050,
    shadow: 18051,
    ch_tcp: 18052,
    ch_http: 18053,
    walsender: 18057,
};
const SLOT_GATE: PortSlot = PortSlot {
    source: 18060,
    shadow: 18061,
    ch_tcp: 18062,
    ch_http: 18063,
    walsender: 18067,
};
const SLOT_CREATE_COPY: PortSlot = PortSlot {
    source: 18070,
    shadow: 18071,
    ch_tcp: 18072,
    ch_http: 18073,
    walsender: 18077,
};

struct PortSlot {
    source: u16,
    shadow: u16,
    ch_tcp: u16,
    ch_http: u16,
    walsender: u16,
}

fn skip_gate() -> bool {
    if !fx::pg_available() || !fx::pg_basebackup_available() || !fx::clickhouse_available() {
        eprintln!("skip: missing initdb / pg_basebackup / clickhouse on PATH");
        return true;
    }
    false
}

struct Drill {
    source: Shadow,
    shadow: Shadow,
    ch: fx::ChServer,
    pipeline: fx::Pipeline,
    _tmp: tempfile::TempDir,
}

/// Bootstrap clusters + CH + auto-create pipeline for one namespace.
async fn build_drill(slot: PortSlot, schema_sql: &str, namespace: &str, app_name: &str) -> Drill {
    let tmp = tempfile::tempdir().unwrap();
    let (
        fx::BootstrappedClusters {
            source,
            shadow,
            shadow_filter_dir,
        },
        shadow_stream_state,
    ) = fx::bootstrap_clusters(&tmp, schema_sql, slot.source, slot.shadow, slot.walsender).await;

    let ch_tmp = tempfile::tempdir().unwrap();
    let ch = fx::ChServer::spawn(ch_tmp, slot.ch_tcp, slot.ch_http).expect("spawn ch");
    ch.query("CREATE DATABASE IF NOT EXISTS walshadow_test")
        .expect("create db");

    let mut ddl_args = fx::DdlPipelineArgs::default();
    ddl_args.namespaces.insert(
        namespace.into(),
        NamespaceMapping {
            target_database: Some("walshadow_test".into()),
            auto_create: true,
            drop_table_strategy: None,
        },
    );

    let pipeline = fx::build_pipeline(fx::BuildPipelineArgs {
        tmp: &tmp,
        source: &source,
        shadow: &shadow,
        shadow_filter_dir: &shadow_filter_dir,
        shadow_stream_state,
        ch_database: "walshadow_test",
        ch_tcp_port: slot.ch_tcp,
        mappings: vec![],
        app_name,
        ddl: Some(ddl_args),
    })
    .await;

    Drill {
        source,
        shadow,
        ch,
        pipeline,
        _tmp: tmp,
    }
}

/// Pump one switched segment, wait shadow replay, drain pipeline.
async fn pump_and_drain(drill: &mut Drill) {
    let shipped = fx::pump_segments(&mut drill.pipeline, 1, Duration::from_secs(45)).await;
    assert!(shipped >= 1, "expected ≥1 shipped segment, got {shipped}");
    let target = drill.pipeline.stream.dispatched_lsn();
    let observed = drill
        .shadow
        .wait_for_replay(target, Duration::from_secs(30))
        .expect("shadow replay");
    assert!(observed >= target);
}

/// Clean transaction commits BETWEEN a prepared dirty xact's catalog touch
/// and its COMMIT PREPARED — single connection, so WAL interleave is
/// deterministic. Dirty state must fence only the prepared tree: clean
/// rows deliver, dirty post-touch row fences.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn interleaved_clean_xact_unaffected_by_dirty_tree() {
    if skip_gate() {
        return;
    }
    let mut drill = build_drill(
        SLOT_INTERLEAVE,
        "CREATE SCHEMA dai;\n\
         CREATE TABLE dai.dirty_t (id bigint PRIMARY KEY, v text);\n\
         CREATE TABLE dai.clean_t (id bigint PRIMARY KEY, v text);\n",
        "dai",
        "walshadow-dirty-interleave",
    )
    .await;

    let driver = spawn_txn(
        &drill.source,
        "BEGIN;\n\
         ALTER TABLE dai.dirty_t ADD COLUMN extra text;\n\
         INSERT INTO dai.dirty_t (id, v, extra) VALUES (1, 'd1', 'e1');\n\
         PREPARE TRANSACTION 'dirty_interleave';\n\
         INSERT INTO dai.clean_t (id, v) VALUES (1, 'c1'), (2, 'c2');\n\
         COMMIT PREPARED 'dirty_interleave';\n\
         SELECT pg_switch_wal();\n",
    );
    pump_and_drain(&mut drill).await;
    let _ = driver.join();

    let decoder_stats = drill.pipeline.sinks.decoder.stats_handle();
    let emitter_stats = drill.pipeline.stats.clone();
    drill.pipeline.shutdown().await.expect("pipeline drains");
    let _ = drill.shadow.stop();
    let _ = drill.source.stop();
    let ch = &drill.ch;

    fx::wait_query(
        ch,
        "SELECT count() FROM walshadow_test.clean_t WHERE _is_deleted = 0",
        "2",
        "clean xact rows deliver despite concurrent dirty tree",
    )
    .await;
    fx::wait_query(
        ch,
        "SELECT count() FROM system.columns \
         WHERE database = 'walshadow_test' AND table = 'dirty_t' AND name = 'extra'",
        "1",
        "prepared ALTER applies at COMMIT PREPARED",
    )
    .await;
    fx::wait_query(
        ch,
        "SELECT concat(v, '/', extra) FROM walshadow_test.dirty_t \
         WHERE id = 1 AND _is_deleted = 0",
        "d1/e1",
        "dirty post-touch row decodes raw and delivers with the added column",
    )
    .await;
    assert!(
        decoder_stats.raw_stash_deferred.load(Ordering::Relaxed) >= 1,
        "dirty row enters raw spill",
    );
    assert!(
        emitter_stats.raw_decode_rows_ops.load().iter().sum::<u64>() >= 1,
        "deferred row decodes at commit resolution",
    );
    assert!(
        emitter_stats.plan_rows.load(Ordering::Relaxed) >= 3,
        "clean and raw-decoded rows sealed into plans",
    );
    assert!(
        emitter_stats.plan_bytes_mem.load(Ordering::Relaxed) > 0,
        "small plans stay memory-resident",
    );
    assert!(
        emitter_stats.route_snapshots_mapped.load(Ordering::Relaxed) >= 1,
        "plan-time route resolution counted",
    );
}

/// Catalog touch inside a subxact dirties the whole known tree: child row
/// after the touch AND top-level row after RELEASE both defer; top row
/// written BEFORE the touch decodes with the predecessor descriptor and
/// delivers. Subxact's DDL observation survives RELEASE into the top
/// commit's capture boundary.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn subxact_catalog_touch_defers_top_and_child_rows() {
    if skip_gate() {
        return;
    }
    let mut drill = build_drill(
        SLOT_SUBXACT,
        "CREATE SCHEMA das;\n\
         CREATE TABLE das.t (id bigint PRIMARY KEY, v text);\n",
        "das",
        "walshadow-dirty-subxact",
    )
    .await;

    let driver = spawn_txn(
        &drill.source,
        "BEGIN;\n\
         INSERT INTO das.t (id, v) VALUES (1, 'pre');\n\
         SAVEPOINT sp;\n\
         ALTER TABLE das.t ADD COLUMN extra text;\n\
         INSERT INTO das.t (id, v, extra) VALUES (2, 'child', 'e2');\n\
         RELEASE SAVEPOINT sp;\n\
         INSERT INTO das.t (id, v) VALUES (3, 'top-post');\n\
         COMMIT;\n\
         SELECT pg_switch_wal();\n",
    );
    pump_and_drain(&mut drill).await;
    let _ = driver.join();

    let decoder_stats = drill.pipeline.sinks.decoder.stats_handle();
    let emitter_stats = drill.pipeline.stats.clone();
    drill.pipeline.shutdown().await.expect("pipeline drains");
    let _ = drill.shadow.stop();
    let _ = drill.source.stop();
    let ch = &drill.ch;

    fx::wait_query(
        ch,
        "SELECT count() FROM system.columns \
         WHERE database = 'walshadow_test' AND table = 't' AND name = 'extra'",
        "1",
        "subxact ALTER survives RELEASE into top commit boundary",
    )
    .await;
    fx::wait_query(
        ch,
        "SELECT arrayStringConcat(groupArray(c), ',') FROM (\
            SELECT concat(toString(id), '=', argMax(v, _lsn)) AS c \
            FROM walshadow_test.t WHERE _is_deleted = 0 \
            GROUP BY id ORDER BY id)",
        "1=pre,2=child,3=top-post",
        "pre-touch row decodes inline; child + post-RELEASE rows decode raw",
    )
    .await;
    assert!(
        decoder_stats.raw_stash_deferred.load(Ordering::Relaxed) >= 2,
        "child row and post-RELEASE top row both enter raw spill",
    );
    assert!(
        emitter_stats.raw_decode_rows_ops.load().iter().sum::<u64>() >= 2,
        "both deferred rows decode at commit resolution",
    );
}

/// EVERY post-touch ordinary record enters raw spill and decodes at
/// commit — exact record accounting across single INSERT (1 record),
/// multi-VALUES INSERT (per-row heap_insert, 2), COPY (one Heap2
/// MULTI_INSERT), UPDATE (1), DELETE (1) = 6 records fanning out 8 rows.
/// Pre-touch row decodes inline via predecessor; the merged end state
/// reflects all of them.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn phase_gate_every_post_touch_record_defers_and_fences() {
    if skip_gate() {
        return;
    }
    let mut drill = build_drill(
        SLOT_GATE,
        "CREATE SCHEMA dag;\n\
         CREATE TABLE dag.t (id bigint PRIMARY KEY, v text);\n\
         ALTER TABLE dag.t REPLICA IDENTITY FULL;\n",
        "dag",
        "walshadow-dirty-gate",
    )
    .await;

    let driver = spawn_txn(
        &drill.source,
        "BEGIN;\n\
         INSERT INTO dag.t (id, v) VALUES (1, 'pre');\n\
         ALTER TABLE dag.t ADD COLUMN extra text;\n\
         INSERT INTO dag.t (id, v, extra) VALUES (2, 'i2', 'e2');\n\
         INSERT INTO dag.t (id, v, extra) VALUES (3, 'i3', 'e3'), (4, 'i4', 'e4');\n\
         COPY dag.t (id, v, extra) FROM stdin;\n\
         5\tc5\te5\n\
         6\tc6\te6\n\
         7\tc7\te7\n\
         \\.\n\
         UPDATE dag.t SET v = 'u2' WHERE id = 2;\n\
         DELETE FROM dag.t WHERE id = 3;\n\
         COMMIT;\n\
         SELECT pg_switch_wal();\n",
    );
    pump_and_drain(&mut drill).await;
    let _ = driver.join();

    let decoder_stats = drill.pipeline.sinks.decoder.stats_handle();
    let emitter_stats = drill.pipeline.stats.clone();
    drill.pipeline.shutdown().await.expect("pipeline drains");
    let _ = drill.shadow.stop();
    let _ = drill.source.stop();
    let ch = &drill.ch;

    fx::wait_query(
        ch,
        "SELECT arrayStringConcat(groupArray(c), ',') FROM (\
            SELECT concat(toString(id), '=', argMax(v, _lsn)) AS c \
            FROM walshadow_test.t \
            GROUP BY id HAVING argMax(_is_deleted, _lsn) = 0 ORDER BY id)",
        "1=pre,2=u2,4=i4,5=c5,6=c6,7=c7",
        "raw-decoded inserts, update, and delete all reflect in the end state",
    )
    .await;
    fx::wait_query(
        ch,
        "SELECT count() FROM system.columns \
         WHERE database = 'walshadow_test' AND table = 't' AND name = 'extra'",
        "1",
        "ALTER applies at commit boundary",
    )
    .await;
    assert_eq!(
        decoder_stats.raw_stash_deferred.load(Ordering::Relaxed),
        6,
        "every post-touch ordinary record enters raw spill",
    );
    assert_eq!(
        emitter_stats
            .raw_decode_ordinary_ops
            .load()
            .iter()
            .sum::<u64>(),
        6,
        "commit decodes each deferred record",
    );
    assert_eq!(
        emitter_stats.raw_decode_rows_ops.load().iter().sum::<u64>(),
        8,
        "MULTI_INSERT fans out per row",
    );
    assert_eq!(
        decoder_stats.toast_stash_buffered.load(Ordering::Relaxed),
        0,
        "no marker-path admissions; defer branch owns dirty records",
    );
}

/// Primary regression target: rows COPYed into a table created in the
/// SAME transaction deliver to an auto-created CH table. Every record
/// after the CREATE stashes raw; commit resolution describes them with
/// the commit-time descriptor, auto-create Added applies before the
/// route snapshot, and the plan routes the fanned-out rows.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn create_table_and_copy_same_xact_delivers() {
    if skip_gate() {
        return;
    }
    let mut drill = build_drill(
        SLOT_CREATE_COPY,
        "CREATE SCHEMA dac;\n",
        "dac",
        "walshadow-dirty-create-copy",
    )
    .await;

    let driver = spawn_txn(
        &drill.source,
        "BEGIN;\n\
         CREATE TABLE dac.fresh (id bigint PRIMARY KEY, v text);\n\
         COPY dac.fresh (id, v) FROM stdin;\n\
         1\ta\n\
         2\tb\n\
         3\tc\n\
         \\.\n\
         COMMIT;\n\
         SELECT pg_switch_wal();\n",
    );
    pump_and_drain(&mut drill).await;
    let _ = driver.join();

    let decoder_stats = drill.pipeline.sinks.decoder.stats_handle();
    let emitter_stats = drill.pipeline.stats.clone();
    drill.pipeline.shutdown().await.expect("pipeline drains");
    let _ = drill.shadow.stop();
    let _ = drill.source.stop();
    let ch = &drill.ch;

    fx::wait_query(
        ch,
        "SELECT arrayStringConcat(groupArray(c), ',') FROM (\
            SELECT concat(toString(id), '=', v) AS c \
            FROM walshadow_test.fresh WHERE _is_deleted = 0 ORDER BY id)",
        "1=a,2=b,3=c",
        "same-xact CREATE + COPY rows deliver via raw decode",
    )
    .await;
    assert!(
        decoder_stats.raw_stash_deferred.load(Ordering::Relaxed) >= 1,
        "COPY records enter raw spill",
    );
    assert_eq!(
        emitter_stats.raw_decode_rows_ops.load().iter().sum::<u64>(),
        3,
        "COPY fans out three rows at commit resolution",
    );
}

/// Top-level ROLLBACK of a DDL + row transaction: no descriptor batch
/// appends, no rows emit, no fence counts, and the next transaction on
/// the same connection inherits no dirty state.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn top_abort_with_ddl_appends_no_metadata_and_emits_no_rows() {
    if skip_gate() {
        return;
    }
    let mut drill = build_drill(
        SLOT_TOP_ABORT,
        "CREATE SCHEMA daa;\n\
         CREATE TABLE daa.t (id bigint PRIMARY KEY, v text);\n",
        "daa",
        "walshadow-dirty-top-abort",
    )
    .await;
    let log_stats = drill.pipeline.desc_log.stats_handle();
    let batches_before = log_stats.batches_appended.load(Ordering::Relaxed);

    let driver = spawn_txn(
        &drill.source,
        "INSERT INTO daa.t (id, v) VALUES (99, 'sentinel');\n\
         BEGIN;\n\
         ALTER TABLE daa.t ADD COLUMN extra text;\n\
         INSERT INTO daa.t (id, v, extra) VALUES (1, 'doomed', 'e1');\n\
         ROLLBACK;\n\
         INSERT INTO daa.t (id, v) VALUES (2, 'after');\n\
         SELECT pg_switch_wal();\n",
    );
    pump_and_drain(&mut drill).await;
    let _ = driver.join();

    let decoder_stats = drill.pipeline.sinks.decoder.stats_handle();
    let emitter_stats = drill.pipeline.stats.clone();
    drill.pipeline.shutdown().await.expect("pipeline drains");
    let _ = drill.shadow.stop();
    let _ = drill.source.stop();
    let ch = &drill.ch;

    fx::wait_query(
        ch,
        "SELECT arrayStringConcat(groupArray(c), ',') FROM (\
            SELECT concat(toString(id), '=', argMax(v, _lsn)) AS c \
            FROM walshadow_test.t WHERE _is_deleted = 0 \
            GROUP BY id ORDER BY id)",
        "2=after,99=sentinel",
        "post-abort insert delivers; aborted xact's row does not",
    )
    .await;
    assert_eq!(
        ch.query(
            "SELECT count() FROM system.columns \
             WHERE database = 'walshadow_test' AND table = 't' AND name = 'extra'",
        )
        .unwrap(),
        "0",
        "rolled-back ALTER must not reach CH",
    );
    assert_eq!(
        log_stats.batches_appended.load(Ordering::Relaxed),
        batches_before,
        "aborted DDL appends no descriptor metadata",
    );
    assert!(
        decoder_stats.raw_stash_deferred.load(Ordering::Relaxed) >= 1,
        "doomed row deferred raw before abort",
    );
    assert_eq!(
        emitter_stats
            .raw_decode_ordinary_ops
            .load()
            .iter()
            .sum::<u64>(),
        0,
        "abort discards raw entries without decoding",
    );
}
