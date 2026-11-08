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
use std::sync::atomic::Ordering;

use walrus::pg::walparser::RelFileNode;
use walshadow::ch::CompressionChoice;
use walshadow::ch_emitter::{EmitterConfig, EmitterStats, InstanceConfig};
use walshadow::datum::build;
use walshadow::heap_decoder::{CommittedTuple, DecodedHeap, DecodedTuple, HeapOp};
use walshadow::mapping::{ColumnMapping, TableMapping, TableTarget};
use walshadow::pipeline::ack::Publish;
use walshadow::pipeline::batcher::{BatcherMsg, PlacedSegment};
use walshadow::pipeline::{Fatal, tail};
use walshadow::pos::Pos;
use walshadow::pos::{EmitterAck, Monotone};
use walshadow::schema::{RelAttr, RelDescriptor, RelName, ReplIdent};

const RFN: RelFileNode = RelFileNode {
    spc_node: 1663,
    db_node: 5,
    rel_node: 16385,
};

fn rel_descriptor() -> Arc<RelDescriptor> {
    Arc::new(RelDescriptor {
        rfn: RFN,
        oid: 16385,
        toast_oid: 0,
        namespace_oid: 2200,
        rel_name: RelName::new("public", "foo"),
        kind: 'r',
        persistence: 'p',
        ancestors: Vec::new(),
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
                missing_default: None,
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
                missing_default: None,
            },
        ],
    })
}

fn mapping() -> Arc<TableMapping> {
    Arc::new(TableMapping {
        target: TableTarget::new("walshadow_test", "foo"),
        columns: vec![
            ColumnMapping {
                src_attnum: 1,
                target_name: "id".into(),
                target_type: "Int32".into(),
                type_pinned: false,
            },
            ColumnMapping {
                src_attnum: 2,
                target_name: "val".into(),
                target_type: "Nullable(String)".into(),
                type_pinned: false,
            },
        ],
        tees: Vec::new(),
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
                    Some(build::int4(id)),
                    Some(build::text(format!("row-{id}"))),
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
    if !fx::tools::clickhouse_available() {
        return;
    }

    let slot = fx::Ports::alloc();
    let ch_tmp = tempfile::tempdir().unwrap();
    let ch = fx::ChServer::spawn(ch_tmp, slot.ch_tcp, slot.ch_http).expect("spawn ch");
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
        conn: InstanceConfig {
            host: "127.0.0.1".into(),
            port: slot.ch_tcp,
            database: "walshadow_test".into(),
            compression: CompressionChoice::Lz4,
            ..Default::default()
        },
        row_budget: 2,
        ..Default::default()
    };

    let stats = Arc::new(EmitterStats::default());
    let emitter_ack = Arc::new(Monotone::<EmitterAck>::default());
    let fatal = Fatal::new();
    let (msg_tx, ack, tail_parts) = tail::spawn(
        walshadow::config::DestEmitter::new(std::sync::Arc::new(cfg), None),
        1,
        stats.clone(),
        emitter_ack.clone(),
        fatal.clone(),
    )
    .await
    .expect("spawn tail");

    let route = walshadow::emit::route::RouteSnapshot::freeze(
        mapping(),
        Arc::default(),
        Default::default(),
    );
    const N: i32 = 5;
    let commit_lsn = 0xC0FFEE;
    let mut seqs = ack.seqs(0);
    let seq = seqs.open(commit_lsn, Publish::Commit);
    let rel = rel_descriptor();
    let segments = (0..N)
        .map(|i| {
            let row = tuple(i, 0x1000 + i as u64, commit_lsn);
            PlacedSegment::of_rows(seq.seq(), &rel, &route, &[row]).expect("encode row")
        })
        .collect();
    msg_tx
        .send(BatcherMsg::Segments(segments))
        .await
        .expect("route rows");
    seqs.place(seq, N as u64);
    drop(seqs);
    let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
    msg_tx
        .send(BatcherMsg::FlushAll(reply_tx))
        .await
        .expect("send flush");
    reply_rx.await.expect("flush ack");
    ack.wait_through(1).await.expect("ack collector alive");
    tail_parts.close(msg_tx, ack).await;
    assert!(fatal.message().is_none(), "no fatal: {:?}", fatal.message());

    // The contiguous-done watermark reaches the seq's commit lsn.
    assert_eq!(
        emitter_ack.get(),
        Pos::new(commit_lsn),
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

/// Forward to `upstream`; client bytes wait while `gate` reads false, so
/// every INSERT stalls mid-request until it opens
async fn stall_proxy(upstream: u16, gate: tokio::sync::watch::Receiver<bool>) -> u16 {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        while let Ok((client, _)) = listener.accept().await {
            let server = tokio::net::TcpStream::connect(("127.0.0.1", upstream))
                .await
                .unwrap();
            let (mut client_rx, mut client_tx) = client.into_split();
            let (mut server_rx, mut server_tx) = server.into_split();
            let mut gate = gate.clone();
            tokio::spawn(async move {
                let mut buf = vec![0; 64 << 10];
                while let Ok(n @ 1..) = client_rx.read(&mut buf).await {
                    if gate.wait_for(|open| *open).await.is_err()
                        || server_tx.write_all(&buf[..n]).await.is_err()
                    {
                        break;
                    }
                }
            });
            tokio::spawn(async move {
                let _ = tokio::io::copy(&mut server_rx, &mut client_tx).await;
            });
        }
    });
    port
}

/// Rows of many small commits spread over many tables, sent while every
/// inserter stalls, exceed byte budget: producer must block, then all
/// rows land and output memory returns once inserts resume
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stalled_inserters_backpressure_many_tables_then_drain() {
    if !fx::tools::clickhouse_available() {
        return;
    }
    const TABLES: usize = 16;
    const ROWS: usize = 1_280;

    let slot = fx::Ports::alloc();
    let ch_tmp = tempfile::tempdir().unwrap();
    let ch = fx::ChServer::spawn(ch_tmp, slot.ch_tcp, slot.ch_http).expect("spawn ch");
    ch.query("CREATE DATABASE IF NOT EXISTS walshadow_test")
        .expect("create db");
    for t in 0..TABLES {
        ch.query(&format!(
            "CREATE TABLE walshadow_test.foo_{t} (id Int32, val Nullable(String), \
             _lsn UInt64, _xid UInt32, _commit_ts DateTime64(6, 'UTC'), _is_deleted Bool) \
             ENGINE = ReplacingMergeTree(_lsn, _is_deleted) ORDER BY id"
        ))
        .expect("create dest table");
    }

    let (gate_tx, gate_rx) = tokio::sync::watch::channel(true);
    let port = stall_proxy(slot.ch_tcp, gate_rx).await;
    let cfg = EmitterConfig {
        conn: InstanceConfig {
            host: "127.0.0.1".into(),
            port,
            database: "walshadow_test".into(),
            compression: CompressionChoice::Lz4,
            ..Default::default()
        },
        row_budget: 16,
        byte_budget: 16 << 10,
        ..Default::default()
    };
    let stats = Arc::new(EmitterStats::default());
    let emitter_ack = Arc::new(Monotone::<EmitterAck>::default());
    let fatal = Fatal::new();
    let (msg_tx, ack, tail_parts) = tail::spawn(
        walshadow::config::DestEmitter::new(Arc::new(cfg), None),
        2,
        stats.clone(),
        emitter_ack.clone(),
        fatal.clone(),
    )
    .await
    .expect("spawn tail");
    gate_tx.send(false).unwrap();

    let tables: Vec<_> = (0..TABLES)
        .map(|t| {
            let mut rel = (*rel_descriptor()).clone();
            rel.rfn.rel_node += t as u32;
            rel.oid += t as u32;
            rel.rel_name = RelName::new("public", &format!("foo_{t}"));
            let mut map = (*mapping()).clone();
            map.target = TableTarget::new("walshadow_test", &format!("foo_{t}"));
            let route = walshadow::emit::route::RouteSnapshot::freeze(
                Arc::new(map),
                Arc::default(),
                Default::default(),
            );
            (Arc::new(rel), route)
        })
        .collect();
    let commit = |i: usize| 0x10_0000 + 0x100 * i as u64;
    let producer = {
        let (msg_tx, ack) = (msg_tx.clone(), ack.clone());
        tokio::spawn(async move {
            let mut seqs = ack.seqs(0);
            for i in 0..ROWS {
                let (rel, route) = &tables[i % TABLES];
                let seq = seqs.open(commit(i), Publish::Commit);
                let mut row = tuple(i as i32, commit(i) - 1, commit(i));
                row.decoded.rfn = rel.rfn;
                let segment =
                    PlacedSegment::of_rows(seq.seq(), rel, route, &[row]).expect("encode row");
                msg_tx
                    .send(BatcherMsg::Segments(vec![segment]))
                    .await
                    .expect("route row");
                seqs.place(seq, 1);
            }
        })
    };

    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    assert!(
        !producer.is_finished(),
        "producer blocks behind stalled inserts"
    );
    assert_eq!(
        emitter_ack.get(),
        Pos::new(0),
        "nothing durable while stalled"
    );
    assert!(stats.output_resident_bytes.load(Ordering::Relaxed) > 0);

    gate_tx.send(true).unwrap();
    producer.await.unwrap();
    let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
    msg_tx
        .send(BatcherMsg::FlushAll(reply_tx))
        .await
        .expect("send flush");
    reply_rx.await.expect("flush ack");
    ack.wait_through(ROWS as u64)
        .await
        .expect("ack collector alive");
    tail_parts.close(msg_tx, ack).await;
    assert!(fatal.message().is_none(), "no fatal: {:?}", fatal.message());
    assert_eq!(emitter_ack.get(), Pos::new(commit(ROWS - 1)));
    for gauge in [&stats.batcher_open_bytes, &*stats.output_resident_bytes] {
        assert_eq!(gauge.load(Ordering::Relaxed), 0);
    }

    for t in 0..TABLES {
        let count = ch
            .query(&format!(
                "SELECT count() FROM walshadow_test.foo_{t} FINAL WHERE _is_deleted = 0"
            ))
            .expect("ch count");
        assert_eq!(count, (ROWS / TABLES).to_string(), "foo_{t}");
    }
}
