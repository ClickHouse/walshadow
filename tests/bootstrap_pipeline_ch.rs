//! Bootstrap page-walk drain → shared insert tail → ClickHouse, at N=2.
//!
//! `bootstrap_direct_ch.rs` / `bootstrap_object_store_ch.rs` drive the full
//! daemon-spawn bootstrap through the tail at the default N=1.
//! `pipeline_parallel_e2e.rs` drives the *WAL* producer through the tail at
//! N=2. This closes the remaining gap: the *bootstrap* producer
//! (`pipeline::bootstrap::drain`, synthetic per-rfn seqs) feeding the tail
//! across N=2 connections, with a small `row_budget` so each rfn's seq
//! spans many batches that fan out and ack out of order. If the per-seq
//! refcount accounting were wrong across the fan-out, `wait_through(K)`
//! would never complete (the test's timeout catches that); if a batch were
//! lost on an out-of-order connection, the CH row count would drop.
//!
//! No source PG needed — synthetic `BackfillTuple`s feed `drain` directly,
//! exactly as `PageWalkSink` would. Skipped silently without `clickhouse`.

#![cfg(target_os = "linux")]

#[path = "common/inproc_harness.rs"]
mod fx;

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use walrus::pg::walparser::RelFileNode;
use walshadow::backup_page_walk::{BackfillTuple, CatalogMap};
use walshadow::ch_emitter::{
    ColumnMapping, CompressionChoice, EmitterConfig, EmitterStats, MappingHandle, TableMapping,
    TableTarget,
};
use walshadow::heap_decoder::ColumnValue;
use walshadow::pipeline::batcher::BatcherMsg;
use walshadow::pipeline::{Fatal, bootstrap, tail};
use walshadow::shadow_catalog::{RelAttr, RelDescriptor, RelName, ReplIdent};
use walshadow::toast::ToastResolver;

const CH_TCP_PORT: u16 = 17571;
const CH_HTTP_PORT: u16 = 17572;

const START_LSN: u64 = 0x5000_0000;
const ROWS_PER_TABLE: i32 = 30;
const INSERTERS: usize = 2;

fn rel(rel_node: u32, name: &str) -> Arc<RelDescriptor> {
    RelDescriptor {
        rfn: RelFileNode {
            spc_node: 1663,
            db_node: 5,
            rel_node,
        },
        oid: rel_node,
        namespace_oid: 2200,
        rel_name: RelName::new("public", name),
        kind: 'r',
        persistence: 'p',
        replident: ReplIdent::Default { pk_attnums: None },
        attributes: vec![RelAttr {
            attnum: 1,
            name: "id".into(),
            type_oid: 23,
            typmod: -1,
            not_null: true,
            dropped: false,
            type_name: "int4".into(),
            type_byval: true,
            type_len: 4,
            type_align: 'i',
            type_storage: 'p',
            missing_text: None,
        }],
    }
    .into()
}

fn id_mapping(target_table: &str) -> TableMapping {
    TableMapping {
        target: TableTarget::new("walshadow_test", target_table),
        columns: vec![ColumnMapping {
            src_attnum: 1,
            target_name: "id".into(),
            target_type: "Int32".into(),
        }],
    }
}

fn tuple(rel_node: u32, id: i32) -> BackfillTuple {
    BackfillTuple {
        rfn: RelFileNode {
            spc_node: 1663,
            db_node: 5,
            rel_node,
        },
        xid: 1000 + id as u32,
        xmax: 0,
        infomask: 0,
        source_lsn: START_LSN,
        blkno: 0,
        offnum: 0,
        columns: vec![Some(ColumnValue::Int4(id))],
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bootstrap_tail_fans_out_n2() {
    if !fx::clickhouse_available() {
        eprintln!("skip: no clickhouse binary on PATH");
        return;
    }

    let ch_tmp = tempfile::tempdir().unwrap();
    let ch = fx::ChServer::spawn(ch_tmp, CH_TCP_PORT, CH_HTTP_PORT).expect("spawn ch");
    ch.query("CREATE DATABASE IF NOT EXISTS walshadow_test")
        .expect("create db");
    for t in ["foo", "baz"] {
        ch.query(&format!(
            "CREATE OR REPLACE TABLE walshadow_test.{t} (\
                id Int32,\
                _lsn UInt64,\
                _xid UInt32,\
                _commit_ts DateTime64(6, 'UTC'), _is_deleted Bool\
             ) ENGINE = ReplacingMergeTree(_lsn, _is_deleted) ORDER BY id"
        ))
        .expect("create dest table");
    }

    // Small row_budget so each rfn's single seq spans many batches that
    // fan across the two inserter connections and ack out of order.
    let mut cfg = EmitterConfig {
        host: "127.0.0.1".into(),
        port: CH_TCP_PORT,
        database: "walshadow_test".into(),
        compression: CompressionChoice::None,
        row_budget: 4,
        flush_timeout: Duration::from_millis(50),
        ..Default::default()
    };
    cfg.tables
        .insert(RelName::new("public", "foo"), id_mapping("foo"));
    cfg.tables
        .insert(RelName::new("public", "baz"), id_mapping("baz"));

    let mut catalog = CatalogMap::new();
    catalog.insert(rel(16400, "foo"));
    catalog.insert(rel(16401, "baz"));
    let mapping: MappingHandle = Arc::new(tokio::sync::RwLock::new(cfg.tables.clone()));

    let stats = Arc::new(EmitterStats::default());
    let emitter_ack = Arc::new(AtomicU64::new(0));
    let fatal = Fatal::new();
    let (msg_tx, ack, tail) = tail::spawn(
        &cfg,
        INSERTERS,
        stats.clone(),
        emitter_ack.clone(),
        fatal.clone(),
    )
    .await
    .expect("spawn tail");

    // Feed all foo rows then all baz rows — contiguous per rfn, as
    // PageWalkSink emits. Two seqs, each spanning ~8 budget-sized batches.
    // cap > 2*ROWS_PER_TABLE so the pre-send fits before the drain spawns
    let (tup_tx, tup_rx) = tokio::sync::mpsc::channel::<BackfillTuple>(128);
    for id in 0..ROWS_PER_TABLE {
        tup_tx.send(tuple(16400, id)).await.unwrap();
    }
    for id in 0..ROWS_PER_TABLE {
        tup_tx.send(tuple(16401, id)).await.unwrap();
    }
    drop(tup_tx);

    let drain = tokio::spawn(bootstrap::drain(
        tup_rx,
        catalog,
        mapping,
        msg_tx.clone(),
        ack.clone(),
        stats.clone(),
        ToastResolver::disabled(),
        walshadow::spool::DeferredSpool::new(
            std::env::temp_dir().join("ws-bootstrap-ch-unused.bin"),
            walshadow::spool::DEFERRED_SPOOL_MEM_MAX,
        ),
    ));
    let outcome = drain.await.expect("drain join").expect("drain ok");
    assert_eq!(outcome.next_seq, 2, "one seq per rfn");
    assert_eq!(outcome.rows_routed, (ROWS_PER_TABLE as u64) * 2);

    // Completion sequence: FlushAll seals the partial batches; wait_through
    // proves every bootstrap seq is durable across both connections. The
    // timeout is the accounting-correctness assertion — a miscounted per-seq
    // refcount would pin the frontier and hang here.
    let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
    msg_tx
        .send(BatcherMsg::FlushAll(reply_tx))
        .await
        .expect("send flush");
    reply_rx.await.expect("flush ack");
    tokio::time::timeout(Duration::from_secs(30), ack.wait_through(outcome.next_seq))
        .await
        .expect("wait_through(K) completed — per-seq acks reconciled across N=2");

    // Watermark saturates at start_lsn (every bootstrap commit_lsn is equal).
    assert_eq!(
        emitter_ack.load(Ordering::Acquire),
        START_LSN,
        "contiguous-done watermark at start_lsn",
    );

    drop(msg_tx);
    drop(ack);
    tail.join().await;
    assert!(fatal.message().is_none(), "no fatal: {:?}", fatal.message());

    // Every fed row landed; out-of-order batch completion across the two
    // connections lost nothing.
    for t in ["foo", "baz"] {
        let count = ch
            .query(&format!("SELECT count() FROM walshadow_test.{t} FINAL"))
            .expect("ch count");
        assert_eq!(count, ROWS_PER_TABLE.to_string(), "all {t} rows landed");
        let distinct = ch
            .query(&format!(
                "SELECT count() FROM (SELECT DISTINCT id FROM walshadow_test.{t} FINAL)"
            ))
            .expect("ch distinct");
        assert_eq!(distinct, ROWS_PER_TABLE.to_string(), "{t} ids distinct");
    }

    assert_eq!(
        stats.rows_emitted.load(Ordering::Relaxed),
        (ROWS_PER_TABLE as u64) * 2,
        "every routed row acked by an inserter",
    );
    assert!(
        stats.blocks_sent.load(Ordering::Relaxed) >= 2,
        "multiple batches fanned across the pool (got {})",
        stats.blocks_sent.load(Ordering::Relaxed),
    );
    assert_eq!(
        stats.reconnects.load(Ordering::Relaxed),
        0,
        "no inserter reconnects on a healthy CH",
    );
}
