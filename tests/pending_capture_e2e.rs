//! Pending catalog capture end to end against real WAL: the writing
//! transaction's own uncommitted catalog rows, read at each command boundary
//! off the bridge worker, folded into the commit's descriptor-log batch.
//!
//! Drills:
//!
//! 1. `pending_timeline_decodes_the_post_ddl_row` — an in-place type change
//!    no single descriptor provably reads, then a row past it. The timeline
//!    answers from the command boundary on, so the fence shrinks to the run
//!    before it and the row lands.
//!    `dirty_admission_e2e::physical_in_place_alter_fences_deferred_rows` is
//!    the control: the same shape with the feature off fences the whole dirty
//!    interval and stops the stream
//! 2. `relation_born_in_xact_keeps_a_slot_per_boundary` — two shapes of a
//!    relation created in the same transaction, one slot each, the first at
//!    the generation's smgr marker
//! 3. `savepoint_rollback_drops_its_pending_slots` — a rolled-back savepoint's
//!    shapes never reach the durable log, and its column never reaches CH
//!
//! `varchar(10)` → `text` is binary coercible, so PG rewrites `atttypid` in
//! place: same filenode, same walk fields, different type. That is the
//! physically-unproven in-place transition, reachable without a rewrite.

#![cfg(target_os = "linux")]

#[path = "common/inproc_harness.rs"]
mod fx;

use fx::spawn_txn;
use std::sync::atomic::Ordering;
use std::time::Duration;

use walshadow::mapping::NamespaceMapping;
use walshadow::shadow::Shadow;

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

async fn build_drill(slot: fx::Ports, schema_sql: &str, app_name: &str) -> Drill {
    let tmp = tempfile::tempdir().unwrap();
    let (
        fx::BootstrappedClusters {
            source,
            shadow,
            shadow_filter_dir,
        },
        shadow_stream_state,
    ) = fx::bootstrap_clusters_with_bridge(
        &tmp,
        schema_sql,
        slot.source,
        slot.shadow,
        slot.walsender,
    )
    .await;

    let ch_tmp = tempfile::tempdir().unwrap();
    let ch = fx::ChServer::spawn(ch_tmp, slot.ch_tcp, slot.ch_http).expect("spawn ch");
    ch.query("CREATE DATABASE IF NOT EXISTS walshadow_test")
        .expect("create db");

    let mut ddl_args = fx::DdlPipelineArgs::default();
    ddl_args.namespaces.insert(
        "pc".into(),
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

fn capture_stats(drill: &Drill) -> std::sync::Arc<walshadow::catalog_capture::CaptureStats> {
    drill
        .pipeline
        .sinks
        .capture
        .as_ref()
        .expect("capture wired")
        .stats_handle()
}

fn load(c: &std::sync::atomic::AtomicU64) -> u64 {
    c.load(Ordering::Relaxed)
}

/// Row written after an unproven in-place change decodes under the shape the
/// command boundary captured, where without the timeline the fence would
/// cover it and stop the stream.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pending_timeline_decodes_the_post_ddl_row() {
    if skip_gate() {
        return;
    }
    let mut drill = build_drill(
        fx::Ports::alloc(),
        "CREATE SCHEMA pc;\n\
         CREATE TABLE pc.t (id bigint PRIMARY KEY, v varchar(10));\n",
        "walshadow-pending-covered",
    )
    .await;
    let capture = capture_stats(&drill);

    let driver = spawn_txn(
        &drill.source,
        "BEGIN;\n\
         INSERT INTO pc.t (id, v) VALUES (1, 'pre');\n\
         ALTER TABLE pc.t ALTER COLUMN v TYPE text;\n\
         INSERT INTO pc.t (id, v) VALUES (2, 'post');\n\
         COMMIT;\n\
         SELECT pg_switch_wal();\n",
    );
    pump_and_drain(&mut drill).await;
    let _ = driver.join();

    drill.pipeline.shutdown().await.expect("pipeline drains");
    let _ = drill.shadow.stop();
    let _ = drill.source.stop();

    fx::wait_query(
        &drill.ch,
        "SELECT arrayStringConcat(groupArray(c), ',') FROM (\
            SELECT concat(toString(id), '=', argMax(v, _lsn)) AS c \
            FROM walshadow_test.t WHERE _is_deleted = 0 \
            GROUP BY id ORDER BY id)",
        "1=pre,2=post",
        "row past the command boundary decodes under the shape it saw",
    )
    .await;
    assert!(
        load(&capture.pending_captures) >= 1,
        "command boundary read into the timeline",
    );
    assert!(
        load(&capture.pending_entries_promoted) >= 1,
        "its shape folded into the commit batch",
    );
    assert!(
        load(&capture.pending_holds) >= 1,
        "command boundary parked publication",
    );
    assert_eq!(
        capture.pending_degraded.iter().map(load).sum::<u64>(),
        0,
        "nothing degraded this transaction",
    );
    assert!(
        load(&capture.ambiguities_published) >= 1,
        "the run before the first boundary is still fenced",
    );
}

/// A relation born inside the transaction takes two shapes across two
/// command boundaries. The first claims the generation's smgr marker — rows
/// can't precede the create — and the second answers from its own boundary,
/// so both land in the batch instead of collapsing onto one position.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn relation_born_in_xact_keeps_a_slot_per_boundary() {
    if skip_gate() {
        return;
    }
    let mut drill = build_drill(
        fx::Ports::alloc(),
        "CREATE SCHEMA pc;\n",
        "walshadow-pending-born",
    )
    .await;
    let capture = capture_stats(&drill);

    let driver = spawn_txn(
        &drill.source,
        "BEGIN;\n\
         CREATE TABLE pc.born (id bigint PRIMARY KEY, v text);\n\
         INSERT INTO pc.born (id, v) VALUES (1, 'first');\n\
         ALTER TABLE pc.born ADD COLUMN extra text;\n\
         INSERT INTO pc.born (id, v, extra) VALUES (2, 'second', 'e2');\n\
         COMMIT;\n\
         SELECT pg_switch_wal();\n",
    );
    pump_and_drain(&mut drill).await;
    let _ = driver.join();

    drill.pipeline.shutdown().await.expect("pipeline drains");
    let _ = drill.shadow.stop();
    let _ = drill.source.stop();

    fx::wait_query(
        &drill.ch,
        // `concat` over a NULL yields NULL and `groupArray` then skips the
        // row, so the pre-ALTER row's absent column needs a substitute
        "SELECT arrayStringConcat(groupArray(c), ',') FROM (\
            SELECT concat(toString(id), '=', argMax(v, _lsn), '/', \
                          ifNull(argMax(extra, _lsn), '-')) AS c \
            FROM walshadow_test.born WHERE _is_deleted = 0 \
            GROUP BY id ORDER BY id)",
        "1=first/-,2=second/e2",
        "both shapes of a relation born in the xact reach CH",
    )
    .await;
    assert!(
        load(&capture.pending_captures) >= 2,
        "one boundary per command that touched the relation",
    );
    assert!(
        load(&capture.pending_entries_promoted) >= 2,
        "two shapes, two positions — not one slot overwriting the other",
    );
}

/// A rolled-back savepoint's shapes die on the pump, before the commit that
/// would promote them: the reverted column reaches neither the durable log
/// nor CH.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn savepoint_rollback_drops_its_pending_slots() {
    if skip_gate() {
        return;
    }
    let mut drill = build_drill(
        fx::Ports::alloc(),
        "CREATE SCHEMA pc;\n\
         CREATE TABLE pc.t (id bigint PRIMARY KEY, v text);\n",
        "walshadow-pending-savepoint",
    )
    .await;
    let capture = capture_stats(&drill);

    let driver = spawn_txn(
        &drill.source,
        "BEGIN;\n\
         SAVEPOINT sp;\n\
         ALTER TABLE pc.t ADD COLUMN reverted text;\n\
         ROLLBACK TO SAVEPOINT sp;\n\
         INSERT INTO pc.t (id, v) VALUES (1, 'after');\n\
         COMMIT;\n\
         SELECT pg_switch_wal();\n",
    );
    pump_and_drain(&mut drill).await;
    let _ = driver.join();

    drill.pipeline.shutdown().await.expect("pipeline drains");
    let _ = drill.shadow.stop();
    let _ = drill.source.stop();

    fx::wait_query(
        &drill.ch,
        "SELECT count() FROM system.columns \
         WHERE database = 'walshadow_test' AND table = 't' AND name = 'reverted'",
        "0",
        "rolled-back column never reaches CH",
    )
    .await;
    fx::wait_query(
        &drill.ch,
        "SELECT argMax(v, _lsn) FROM walshadow_test.t WHERE id = 1 AND _is_deleted = 0",
        "after",
        "post-rollback row delivers under the unmodified shape",
    )
    .await;
    assert!(
        load(&capture.pending_captures) >= 1,
        "the savepoint's command boundary was read",
    );
    assert!(
        load(&capture.pending_entries_dropped_abort) >= 1,
        "its slots dropped with the aborted subtree",
    );
    assert_eq!(
        load(&capture.pending_entries_promoted),
        0,
        "nothing from the aborted subtree became durable",
    );
}
