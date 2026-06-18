//! F3 — budget-driven batch sealing, driven through the insert tail
//! (batcher → inserter) against a real ClickHouse.
//!
//! With `row_budget = 2`, routing five rows trips the batcher's budget
//! after the 2nd and 4th rows — each trip seals a complete,
//! independently-durable INSERT — and the final `FlushAll` seals the 5th.
//! All five must land: the batcher clears a table's buffer only after
//! handing the sealed block to an inserter, so the mid-stream seals
//! neither lose nor mangle rows.
//!
//! The serial emitter's per-xact seal + its `EmitterObserver` stats-handle
//! plumbing this file used to cover are gone; the batcher owns sealing
//! (also unit-tested hermetically in `pipeline::batcher`), and live stats
//! across the task boundary are asserted in `pipeline_parallel_e2e`.

#![cfg(target_os = "linux")]

#[path = "common/inproc_harness.rs"]
mod fx;

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use pgwalrs::pg::walparser::RelFileNode;
use walshadow::ch_emitter::{
    ColumnMapping, CompressionChoice, EmitterConfig, EmitterStats, TableMapping,
};
use walshadow::heap_decoder::{ColumnValue, CommittedTuple, DecodedHeap, DecodedTuple, HeapOp};
use walshadow::pipeline::batcher::{BatcherMsg, RoutedRow};
use walshadow::pipeline::{Fatal, tail};
use walshadow::shadow_catalog::{RelAttr, RelDescriptor, ReplIdent};

const CH_TCP_PORT: u16 = 17619;
const CH_HTTP_PORT: u16 = 17620;

const RFN: RelFileNode = RelFileNode {
    spc_node: 1663,
    db_node: 5,
    rel_node: 16385,
};

fn rel_descriptor() -> Arc<RelDescriptor> {
    Arc::new(RelDescriptor {
        rfn: RFN,
        oid: 16385,
        namespace_oid: 2200,
        namespace_name: "public".into(),
        name: "foo".into(),
        qualified_name: RelDescriptor::build_qualified_name("public", "foo"),
        kind: 'r',
        persistence: 'p',
        replident: ReplIdent::Default { pk_attnums: None },
        attributes: vec![
            RelAttr {
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
            },
            RelAttr {
                attnum: 2,
                name: "val".into(),
                type_oid: 25,
                typmod: -1,
                not_null: false,
                dropped: false,
                type_name: "text".into(),
                type_byval: false,
                type_len: -1,
                type_align: 'i',
                type_storage: 'x',
                missing_text: None,
            },
        ],
    })
}

fn mapping() -> Arc<TableMapping> {
    Arc::new(TableMapping {
        target: "walshadow_test.foo".into(),
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
    })
}

fn tuple(id: i32, source_lsn: u64, commit_lsn: u64) -> CommittedTuple {
    CommittedTuple {
        decoded: DecodedHeap {
            rfn: RFN,
            xid: 42,
            source_lsn,
            op: HeapOp::Insert,
            new: Some(DecodedTuple {
                columns: vec![
                    Some(ColumnValue::Int4(id)),
                    Some(ColumnValue::Text(format!("row-{id}"))),
                ],
                partial: false,
            }),
            old: None,
        },
        commit_ts: 1_000_000,
        commit_lsn,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn budget_trips_seal_complete_inserts() {
    if !fx::clickhouse_available() {
        eprintln!("skip: no clickhouse binary on PATH");
        return;
    }

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
         ) ENGINE = ReplacingMergeTree(_lsn, _is_deleted) ORDER BY id",
    )
    .expect("create dest table");

    // Trip the batcher after every 2 rows so a 5-row seq seals 3 blocks
    // (2 budget trips + the final FlushAll).
    let cfg = EmitterConfig {
        host: "127.0.0.1".into(),
        port: CH_TCP_PORT,
        database: "walshadow_test".into(),
        compression: CompressionChoice::Lz4,
        row_budget: 2,
        ..Default::default()
    };

    let stats = Arc::new(EmitterStats::default());
    let emitter_ack = Arc::new(AtomicU64::new(0));
    let fatal = Fatal::new();
    let (msg_tx, ack, tail_parts) =
        tail::spawn(&cfg, 1, stats.clone(), emitter_ack.clone(), fatal.clone())
            .await
            .expect("spawn tail");

    let rel = rel_descriptor();
    let mapping = mapping();
    const N: i32 = 5;
    let commit_lsn = 0xC0FFEE;
    ack.register(0, commit_lsn);
    for i in 0..N {
        msg_tx
            .send(BatcherMsg::Row(RoutedRow {
                seq: 0,
                rel: rel.clone(),
                mapping: mapping.clone(),
                committed: tuple(i, 0x1000 + i as u64, commit_lsn),
            }))
            .await
            .expect("route row");
    }
    ack.placed(0, N as u64);
    let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
    msg_tx
        .send(BatcherMsg::FlushAll(reply_tx))
        .await
        .expect("send flush");
    reply_rx.await.expect("flush ack");
    ack.wait_through(1).await;
    drop(msg_tx);
    drop(ack);
    tail_parts.join().await;
    assert!(fatal.message().is_none(), "no fatal: {:?}", fatal.message());

    // The contiguous-done watermark reaches the seq's commit lsn.
    assert_eq!(
        emitter_ack.load(Ordering::Acquire),
        commit_lsn,
        "durable horizon must reach the commit lsn",
    );

    // Two budget trips + the FlushAll seal = 3 sealed blocks.
    let blocks_sent = stats.blocks_sent.load(Ordering::Relaxed);
    assert!(
        blocks_sent >= 3,
        "expected ≥3 sealed blocks, got {blocks_sent}",
    );

    let count = ch
        .query("SELECT count() FROM walshadow_test.foo FINAL WHERE _is_deleted = 0")
        .expect("ch count");
    assert_eq!(count, N.to_string(), "every routed row must be durable");
    let distinct = ch
        .query("SELECT uniqExact(id) FROM walshadow_test.foo")
        .expect("ch distinct");
    assert_eq!(distinct, N.to_string());
}
