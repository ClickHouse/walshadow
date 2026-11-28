//! `ClickHouseChunkStore` against mirrors whose columns break the fetch
//! contract: each wrong shape fails the fetch naming the column, never a
//! misread value. Server-side failures retry once, then surface. Also drives
//! slot contention on a one-connection store.
//!
//! Skipped silently without the `clickhouse` multitool.

#![cfg(target_os = "linux")]

#[path = "common/inproc_harness.rs"]
mod fx;

use std::sync::Arc;

use walshadow::emitter_config::{EmitterConfig, InstanceConfig, RetryConfig};
use walshadow::toast::fetched::FetchedValue;
use walshadow::toast::{ChunkStore, ClickHouseChunkStore, ToastRow};

const DB: &str = "walshadow_toast_faults";
const SRC_DB: u32 = 16384;

fn store(port: u16) -> ClickHouseChunkStore {
    let cfg = EmitterConfig {
        conn: InstanceConfig {
            host: "127.0.0.1".into(),
            port,
            database: DB.into(),
            ..Default::default()
        },
        inserter_pool_size: 1,
        retry: RetryConfig {
            max_attempts: 1,
            initial_backoff: std::time::Duration::from_millis(1),
            max_backoff: std::time::Duration::from_millis(1),
        },
        ..EmitterConfig::default()
    };
    ClickHouseChunkStore::new(walshadow::config::DestEmitter::new(Arc::new(cfg), None))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fetch_rejects_mirrors_of_the_wrong_shape() {
    if !fx::tools::clickhouse_available() {
        return;
    }
    let slot = fx::Ports::alloc();
    let ch_tmp = tempfile::tempdir().unwrap();
    let ch = fx::ChServer::spawn(ch_tmp, slot.ch_tcp, slot.ch_http).expect("spawn ch");
    ch.query(&format!("CREATE DATABASE {DB}")).unwrap();
    let store = store(ch.port);

    // (relid, chunk_id, chunk_seq, chunk_data types, rows as (seq, data), error)
    let cases = [
        (
            16601,
            "UInt64",
            "UInt32",
            "String",
            "(0, 'ab')",
            "elem sizes 8/4 != 4",
        ),
        (
            16602,
            "Nullable(UInt32)",
            "UInt32",
            "String",
            "(0, 'ab')",
            "chunk_id not fixed",
        ),
        (
            16603,
            "UInt32",
            "String",
            "String",
            "('0', 'ab')",
            "chunk_seq not fixed",
        ),
        (
            16604,
            "UInt32",
            "UInt32",
            "UInt32",
            "(0, 1)",
            "chunk_data not String",
        ),
        // Signed seq sorts -2 before 0; read unsigned it runs backwards
        (
            16605,
            "UInt32",
            "Int32",
            "String",
            "(-2, 'a'), (0, 'b')",
            "order contract broken",
        ),
    ];
    for (relid, id_ty, seq_ty, data_ty, rows, want) in cases {
        let table = format!(
            "{DB}.{}",
            walshadow::toast::mirror_table_name(SRC_DB, relid)
        );
        ch.query(&format!(
            "CREATE TABLE {table} (blkno UInt32, offnum UInt16, chunk_id {id_ty}, \
             chunk_seq {seq_ty}, chunk_data {data_ty}, _lsn UInt64, _is_deleted UInt8) \
             ENGINE = MergeTree ORDER BY (blkno, offnum)"
        ))
        .unwrap();
        ch.query(&format!(
            "INSERT INTO {table} SELECT 1, toUInt16(rowNumberInAllBlocks() + 1), 7, \
             r.1, r.2, 1, 0 FROM (SELECT arrayJoin([{rows}]) AS r)"
        ))
        .unwrap();
        let err = store
            .fetch(SRC_DB, relid, 7, u64::MAX, 2)
            .await
            .unwrap_err();
        assert!(err.to_string().contains(want), "{relid}: {err}");
    }
}

/// Mirror lacking a column the store reads and writes: both directions
/// retry the server exception once, then fail
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn server_exceptions_retry_then_surface() {
    if !fx::tools::clickhouse_available() {
        return;
    }
    let slot = fx::Ports::alloc();
    let ch_tmp = tempfile::tempdir().unwrap();
    let ch = fx::ChServer::spawn(ch_tmp, slot.ch_tcp, slot.ch_http).expect("spawn ch");
    ch.query(&format!("CREATE DATABASE {DB}")).unwrap();
    let table = format!(
        "{DB}.{}",
        walshadow::toast::mirror_table_name(SRC_DB, 16620)
    );
    ch.query(&format!(
        "CREATE TABLE {table} (blkno UInt32, offnum UInt16, chunk_id UInt32, \
         chunk_seq UInt32, chunk_data String, _lsn UInt64) \
         ENGINE = MergeTree ORDER BY (blkno, offnum)"
    ))
    .unwrap();
    let store = store(ch.port);
    let err = store
        .fetch(SRC_DB, 16620, 7, u64::MAX, 2)
        .await
        .unwrap_err();
    assert!(err.to_string().contains("_is_deleted"), "{err}");
    let row = ToastRow {
        db_oid: SRC_DB,
        toast_relid: 16620,
        blkno: 1,
        offnum: 1,
        chunk_id: 7,
        chunk_seq: 0,
        chunk_data: bytes::Bytes::from_static(b"ab"),
        lsn: 1,
    };
    let err = store.put(&[row]).await.unwrap_err();
    assert!(err.to_string().contains("_is_deleted"), "{err}");
}

/// One connection, two concurrent fetches: the second waits its turn
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn one_connection_serializes_concurrent_fetches() {
    if !fx::tools::clickhouse_available() {
        return;
    }
    let slot = fx::Ports::alloc();
    let ch_tmp = tempfile::tempdir().unwrap();
    let ch = fx::ChServer::spawn(ch_tmp, slot.ch_tcp, slot.ch_http).expect("spawn ch");
    ch.query(&format!("CREATE DATABASE {DB}")).unwrap();
    let store = store(ch.port);
    let row = |chunk_id, offnum| ToastRow {
        db_oid: SRC_DB,
        toast_relid: 16610,
        blkno: 1,
        offnum,
        chunk_id,
        chunk_seq: 0,
        chunk_data: bytes::Bytes::from_static(b"ab"),
        lsn: 1,
    };
    store.put(&[row(7, 1), row(8, 2)]).await.unwrap();
    let (a, b) = tokio::join!(
        store.fetch(SRC_DB, 16610, 7, u64::MAX, 2),
        store.fetch(SRC_DB, 16610, 8, u64::MAX, 2),
    );
    let want = FetchedValue::Assembled(b"ab".to_vec());
    assert_eq!((a.unwrap(), b.unwrap()), (want.clone(), want));
}
