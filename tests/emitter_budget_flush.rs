//! F3 — atomic-flush wire lifecycle, driven at the emitter layer
//! against a real ClickHouse (no WAL pipeline, so the budget can be set
//! low enough to trip several times within one transaction).
//!
//! With `row_budget = 2`, routing five tuples in a single xact trips the
//! budget after the 2nd and 4th rows — each trip seals a complete,
//! independently-durable INSERT (open → block → `send_data(None)` →
//! `EndOfStream` → clear) — and `on_xact_end` seals the 5th. All five
//! must land: the buffer is cleared only after each `EndOfStream`, so
//! the mid-xact seals neither lose nor mangle rows.

#![cfg(target_os = "linux")]

#[path = "common/inproc_harness.rs"]
mod fx;

use std::net::TcpStream;
use std::sync::Arc;
use std::time::Duration;

use wal_rs::pg::walparser::RelFileNode;
use walshadow::backup_page_walk::CatalogMap;
use walshadow::ch_emitter::{
    ColumnMapping, CompressionChoice, Emitter, EmitterConfig, EmitterObserver, TableMapping,
};
use walshadow::decoder_sink::TupleObserver;
use walshadow::heap_decoder::{ColumnValue, CommittedTuple, DecodedHeap, DecodedTuple, HeapOp};
use walshadow::relation_resolver::CatalogMapResolver;
use walshadow::shadow_catalog::{RelAttr, RelDescriptor, ReplIdent};

const CH_TCP_PORT: u16 = 17619;
const CH_HTTP_PORT: u16 = 17620;

const RFN: RelFileNode = RelFileNode {
    spc_node: 1663,
    db_node: 5,
    rel_node: 16385,
};

fn rel_descriptor() -> RelDescriptor {
    RelDescriptor {
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
    }
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

/// Emitter config mapping `public.foo` → `walshadow_test.foo` with the
/// legacy seal-per-xact path (`flush_timeout = 0`).
fn emitter_cfg(tcp_port: u16, row_budget: usize) -> EmitterConfig {
    let mut cfg = EmitterConfig {
        host: "127.0.0.1".into(),
        port: tcp_port,
        database: "walshadow_test".into(),
        compression: CompressionChoice::Lz4,
        row_budget,
        flush_timeout: Duration::ZERO,
        ..Default::default()
    };
    cfg.tables.insert(
        "public.foo".into(),
        TableMapping {
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
        },
    );
    cfg
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
            _op Enum8('insert' = 1, 'update' = 2, 'delete' = 3),\
            _commit_ts DateTime64(6, 'UTC')\
         ) ENGINE = ReplacingMergeTree(_lsn) ORDER BY id",
    )
    .expect("create dest table");

    // Trip after every 2 rows so one 5-row xact seals 3 INSERTs.
    let cfg = emitter_cfg(CH_TCP_PORT, 2);

    let mut map = CatalogMap::new();
    map.insert(Arc::new(rel_descriptor()));
    let resolver = Arc::new(CatalogMapResolver::new(map));

    let tcp = TcpStream::connect(("127.0.0.1", CH_TCP_PORT)).expect("tcp connect ch");
    tcp.set_nodelay(true).ok();
    tcp.set_nonblocking(false).expect("blocking socket");
    let mut emitter = Emitter::new(cfg, resolver, tcp).expect("init emitter");

    const N: i32 = 5;
    for i in 0..N {
        emitter
            .route_with_retry(&tuple(i, 0x1000 + i as u64, 0xC0FFEE))
            .await
            .expect("route");
    }
    let ack = emitter
        .on_xact_end_with_retry(0xC0FFEE)
        .await
        .expect("xact end");
    assert_eq!(ack, 0xC0FFEE, "durable horizon must reach the commit lsn");

    // Two budget trips + the xact-end seal = 3 sealed INSERTs.
    assert!(
        emitter.stats.blocks_sent >= 3,
        "expected ≥3 sealed blocks, got {}",
        emitter.stats.blocks_sent,
    );

    let count = ch
        .query("SELECT count() FROM walshadow_test.foo FINAL WHERE _op != 'delete'")
        .expect("ch count");
    assert_eq!(count, N.to_string(), "every routed row must be durable");
    let distinct = ch
        .query("SELECT uniqExact(id) FROM walshadow_test.foo")
        .expect("ch distinct");
    assert_eq!(distinct, N.to_string());
}

const OBS_TCP_PORT: u16 = 17623;
const OBS_HTTP_PORT: u16 = 17624;

/// Regression: `EmitterObserver::stats_handle` must mirror live emitter
/// counters across the worker-task boundary. The daemon's status loop
/// reads only this handle (the emitter is buried inside the
/// `QueueingRecordSink` worker); before the fix `populate_metrics`
/// hardcoded `walshadow_emitter_{rows,blocks,xacts}_total` to 0.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn observer_handle_mirrors_emitter_counters() {
    if !fx::clickhouse_available() {
        eprintln!("skip: no clickhouse binary on PATH");
        return;
    }

    let ch_tmp = tempfile::tempdir().unwrap();
    let ch = fx::ChServer::spawn(ch_tmp, OBS_TCP_PORT, OBS_HTTP_PORT).expect("spawn ch");
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

    // High budget so all rows land in one block sealed at xact end.
    let cfg = emitter_cfg(OBS_TCP_PORT, 1024);

    let mut map = CatalogMap::new();
    map.insert(Arc::new(rel_descriptor()));
    let resolver = Arc::new(CatalogMapResolver::new(map));

    let tcp = TcpStream::connect(("127.0.0.1", OBS_TCP_PORT)).expect("tcp connect ch");
    tcp.set_nodelay(true).ok();
    tcp.set_nonblocking(false).expect("blocking socket");
    let emitter = Emitter::new(cfg, resolver, tcp).expect("init emitter");

    let mut observer = EmitterObserver::new(emitter);
    let handle = observer.stats_handle();
    assert_eq!(
        handle.lock().unwrap().rows_emitted,
        0,
        "fresh handle starts at zero",
    );

    const N: i32 = 3;
    for i in 0..N {
        observer
            .on_tuple(&tuple(i, 0x2000 + i as u64, 0xBEEF))
            .await
            .expect("on_tuple");
    }
    observer.on_xact_end(0xBEEF).await.expect("on_xact_end");

    let snap = handle.lock().unwrap().clone();
    assert_eq!(snap.rows_emitted, N as u64, "rows reach the polled handle");
    assert_eq!(snap.xacts_committed, 1, "xact commit reaches the handle");
    assert!(snap.blocks_sent >= 1, "block seal reaches the handle");
}
