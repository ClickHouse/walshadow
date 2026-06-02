//! F4 — native CH type encoding, driven at the emitter layer against a
//! real ClickHouse: `numeric(p≤76,s)` → `Decimal(p,s)` (scaled integer),
//! `time` → `Time64(6)` (microseconds since midnight), `timetz` →
//! `String` (lossless text with zone). Confirms the wire encoding the
//! bridge advertises matches what the server stores.
//!
//! `Time64` is gated behind `enable_time_time64_type=1`; the harness's
//! `ChServer::spawn` enables it in the default profile.

#![cfg(target_os = "linux")]

#[path = "common/inproc_harness.rs"]
mod fx;

use std::net::TcpStream;
use std::sync::Arc;
use std::time::Duration;

use wal_rs::pg::walparser::RelFileNode;
use walshadow::backup_page_walk::CatalogMap;
use walshadow::ch_emitter::{
    ColumnMapping, CompressionChoice, Emitter, EmitterConfig, TableMapping,
};
use walshadow::codecs::NumericKind;
use walshadow::heap_decoder::{ColumnValue, CommittedTuple, DecodedHeap, DecodedTuple, HeapOp};
use walshadow::relation_resolver::CatalogMapResolver;
use walshadow::shadow_catalog::{RelDescriptor, ReplIdent};

const CH_TCP_PORT: u16 = 17629;
const CH_HTTP_PORT: u16 = 17630;

const RFN: RelFileNode = RelFileNode {
    spc_node: 1663,
    db_node: 5,
    rel_node: 16385,
};

// 12:34:56 = 45296 s = 45_296_000_000 µs since midnight.
const MICROS: i64 = 45_296_000_000;

fn rel_descriptor() -> RelDescriptor {
    // Encoding reads the mapping's target types, not these attributes,
    // so a minimal descriptor (qualified_name + rfn) suffices.
    RelDescriptor {
        rfn: RFN,
        oid: 16385,
        namespace_oid: 2200,
        namespace_name: "public".into(),
        name: "things".into(),
        qualified_name: RelDescriptor::build_qualified_name("public", "things"),
        kind: 'r',
        persistence: 'p',
        replident: ReplIdent::Default { pk_attnums: None },
        attributes: vec![],
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_numeric_time_timetz_round_trip() {
    if !fx::clickhouse_available() {
        eprintln!("skip: no clickhouse binary on PATH");
        return;
    }

    let ch_tmp = tempfile::tempdir().unwrap();
    let ch = fx::ChServer::spawn(ch_tmp, CH_TCP_PORT, CH_HTTP_PORT).expect("spawn ch");
    ch.query("CREATE DATABASE IF NOT EXISTS walshadow_test")
        .expect("create db");
    ch.query(
        "CREATE OR REPLACE TABLE walshadow_test.things (\
            id Int32,\
            n Decimal(10, 2),\
            nw Decimal(50, 2),\
            t Time64(6),\
            tz String,\
            _lsn UInt64,\
            _xid UInt32,\
            _op Enum8('insert' = 1, 'update' = 2, 'delete' = 3),\
            _commit_ts DateTime64(6, 'UTC')\
         ) ENGINE = ReplacingMergeTree(_lsn) ORDER BY id",
    )
    .expect("create dest table");

    let mut cfg = EmitterConfig {
        host: "127.0.0.1".into(),
        port: CH_TCP_PORT,
        database: "walshadow_test".into(),
        compression: CompressionChoice::Lz4,
        flush_timeout: Duration::ZERO,
        ..Default::default()
    };
    cfg.tables.insert(
        "public.things".into(),
        TableMapping {
            target: "walshadow_test.things".into(),
            columns: vec![
                ColumnMapping {
                    src_attnum: 1,
                    target_name: "id".into(),
                    target_type: "Int32".into(),
                },
                ColumnMapping {
                    src_attnum: 2,
                    target_name: "n".into(),
                    target_type: "Decimal(10, 2)".into(),
                },
                ColumnMapping {
                    src_attnum: 3,
                    target_name: "nw".into(),
                    target_type: "Decimal(50, 2)".into(),
                },
                ColumnMapping {
                    src_attnum: 4,
                    target_name: "t".into(),
                    target_type: "Time64(6)".into(),
                },
                ColumnMapping {
                    src_attnum: 5,
                    target_name: "tz".into(),
                    target_type: "String".into(),
                },
            ],
        },
    );

    let mut map = CatalogMap::new();
    map.insert(Arc::new(rel_descriptor()));
    let resolver = Arc::new(CatalogMapResolver::new(map));

    let tcp = TcpStream::connect(("127.0.0.1", CH_TCP_PORT)).expect("tcp connect ch");
    tcp.set_nodelay(true).ok();
    tcp.set_nonblocking(false).expect("blocking socket");
    let mut emitter = Emitter::new(cfg, resolver, tcp).expect("init emitter");

    let tuple = CommittedTuple {
        decoded: DecodedHeap {
            rfn: RFN,
            xid: 7,
            source_lsn: 0x2000,
            op: HeapOp::Insert,
            new: Some(DecodedTuple {
                columns: vec![
                    Some(ColumnValue::Int4(1)),
                    Some(ColumnValue::Numeric(NumericKind::Finite("1.50".into()))),
                    Some(ColumnValue::Numeric(NumericKind::Finite(
                        "123456789012345678901234567890123456789012345678.12".into(),
                    ))),
                    Some(ColumnValue::Time(MICROS)),
                    // UTC+2 → PG stores tz_seconds west-positive = -7200.
                    Some(ColumnValue::TimeTz {
                        micros: MICROS,
                        tz_seconds: -7200,
                    }),
                ],
                partial: false,
            }),
            old: None,
        },
        commit_ts: 1_000_000,
        commit_lsn: 0xABCD,
    };
    emitter.route_with_retry(&tuple).await.expect("route");
    emitter
        .on_xact_end_with_retry(0xABCD)
        .await
        .expect("xact end");

    let row = ch
        .query(
            "SELECT toString(n), toString(t), tz \
             FROM walshadow_test.things FINAL WHERE id = 1",
        )
        .expect("ch select");
    let cols: Vec<&str> = row.trim().split('\t').collect();
    // CH's toString(Decimal) trims trailing zeros, so 1.50 → "1.5"
    // (confirms the scaled integer is 150, not 15 or 1500).
    assert_eq!(cols, vec!["1.5", "12:34:56.000000", "12:34:56+02"]);

    let row = ch
        .query(
            "SELECT toString(nw) \
             FROM walshadow_test.things FINAL WHERE id = 1",
        )
        .expect("ch select");
    assert_eq!(
        row.trim(),
        "123456789012345678901234567890123456789012345678.12"
    );
}
