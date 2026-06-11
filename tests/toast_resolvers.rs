//! All three `[toast] mode` resolvers built via `from_config`, driven through
//! the same `ToastResolver` flow (`put_map` / `fetch_into`) the WAL +
//! bootstrap paths use: `clickhouse` and `disk` share one store-backed
//! scenario, `disabled` pins the no-store policy (put_map no-op, fetch_into
//! false, fill-on-miss).
//!
//! The ClickHouse chunk-store backend additionally runs against a real
//! ClickHouse: chunk rows mirror `pg_toast_<relid>` relations, `put` writes
//! them, `fetch` reads them back into the reassembler's `(seq -> bytes)` map.
//! Pins the schema (`ReplacingMergeTree(_lsn)` keyed on
//! `(chunk_id, chunk_seq)`), the byte-transparency of `chunk_data` (raw bytea,
//! non-UTF-8 included), the missing-table -> empty-map convention, and `_lsn`
//! convergence on re-put.
//!
//! Full pipeline coverage (PG WAL -> reassembly -> CH row) lives in
//! `toast_e2e.rs`.

#![cfg(target_os = "linux")]

#[path = "common/inproc_harness.rs"]
mod fx;

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use walshadow::ch_emitter::{EmitterConfig, EmitterStats};
use walshadow::spill::ToastChunk;
use walshadow::toast::{ChunkMap, ChunkStore, ClickHouseChunkStore, ToastMode, ToastResolver};

const CH_TCP_PORT: u16 = 17639;
const CH_HTTP_PORT: u16 = 17640;
const DB: &str = "walshadow_toast_test";

fn chunk(relid: u32, value_id: u32, seq: u32, lsn: u64, body: &[u8]) -> ToastChunk {
    ToastChunk {
        toast_relid: relid,
        value_id,
        chunk_seq: seq,
        source_lsn: lsn,
        chunk_data: body.to_vec(),
    }
}

fn config(port: u16) -> EmitterConfig {
    EmitterConfig {
        host: "127.0.0.1".into(),
        port,
        database: DB.into(),
        ..EmitterConfig::default()
    }
}

/// WAL-path scenario shared by both store-backed resolvers (disk, clickhouse):
/// stamp an in-xact chunk map at commit LSN, rehydrate a pre-window re-emit
/// via `fetch_into`, surface a genuine gap as `false`.
async fn drive_store_backed(resolver: &ToastResolver, stats: &EmitterStats) {
    assert!(resolver.stores_chunks());
    assert!(!resolver.fill_on_miss());

    // The WAL path's in-xact chunk map, stamped with the commit LSN.
    let mut map = ChunkMap::new();
    map.insert(
        (16700, 42),
        BTreeMap::from([(0u32, b"hello ".to_vec()), (1u32, b"world".to_vec())]),
    );
    resolver.put_map(&map, 0xABCD).await.unwrap();
    assert_eq!(stats.toast_chunks_stored.load(Ordering::Relaxed), 2);

    // Pre-window re-emit: the in-xact buffer missed these chunks, so the
    // reassembler rehydrates them from the store.
    let mut out = ChunkMap::new();
    assert!(resolver.fetch_into(16700, 42, &mut out).await.unwrap());
    let value = out.get(&(16700, 42)).unwrap();
    assert_eq!(value.get(&0).unwrap(), b"hello ");
    assert_eq!(value.get(&1).unwrap(), b"world");
    assert_eq!(stats.toast_values_fetched.load(Ordering::Relaxed), 1);

    // Genuine gap -> no hydration, false (caller surfaces the miss).
    let mut empty = ChunkMap::new();
    assert!(!resolver.fetch_into(16700, 404, &mut empty).await.unwrap());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ch_chunk_store_put_fetch_roundtrip() {
    if !fx::clickhouse_available() {
        eprintln!("skip: no clickhouse binary on PATH");
        return;
    }
    let ch_tmp = tempfile::tempdir().unwrap();
    let ch = fx::ChServer::spawn(ch_tmp, CH_TCP_PORT, CH_HTTP_PORT).expect("spawn ch");
    ch.query(&format!("CREATE DATABASE IF NOT EXISTS {DB}"))
        .expect("create db");

    let store = ClickHouseChunkStore::new(config(ch.port));

    // One value's chunks split across two puts (pages / commits); an
    // unrelated value in the same relation; a second relation -> a second
    // CH table. Last chunk carries non-UTF-8 bytes to prove `chunk_data`
    // is raw-byte transparent, not text.
    store
        .put(&[
            chunk(16500, 7, 0, 0x1000, b"abc"),
            chunk(16500, 7, 1, 0x1000, b"de"),
        ])
        .await
        .unwrap();
    store
        .put(&[chunk(16500, 7, 2, 0x1000, b"\x00\xff\x01")])
        .await
        .unwrap();
    store
        .put(&[chunk(16500, 9, 0, 0x1000, b"zz")])
        .await
        .unwrap();
    store
        .put(&[
            chunk(16600, 3, 0, 0x1000, b"xy"),
            chunk(16600, 3, 1, 0x1000, b"z"),
        ])
        .await
        .unwrap();

    // Reassembly-shaped fetch: dense seqs, in order.
    let got = store.fetch(16500, 7).await.unwrap();
    assert_eq!(got.len(), 3);
    assert_eq!(got.get(&0).unwrap(), b"abc");
    assert_eq!(got.get(&1).unwrap(), b"de");
    assert_eq!(got.get(&2).unwrap(), b"\x00\xff\x01");

    assert_eq!(
        store.fetch(16500, 9).await.unwrap(),
        BTreeMap::from([(0u32, b"zz".to_vec())])
    );
    assert_eq!(
        store.fetch(16600, 3).await.unwrap(),
        BTreeMap::from([(0u32, b"xy".to_vec()), (1u32, b"z".to_vec())])
    );

    // Table exists, no such value_id -> empty (caller decides fill vs error).
    assert!(store.fetch(16500, 999).await.unwrap().is_empty());
    // Relation never received a chunk -> no CH table -> empty, not error.
    assert!(store.fetch(70000, 1).await.unwrap().is_empty());

    // Rows really landed on CH, in the mirrored table.
    assert_eq!(
        ch.query(&format!("SELECT count() FROM {DB}.pg_toast_16500"))
            .unwrap(),
        "4"
    );

    // `_lsn` convergence: re-ship (16500, 7, 0) at a higher LSN with identical
    // bytes (PG re-emit). ReplacingMergeTree collapses on (chunk_id, chunk_seq)
    // to the max `_lsn`; FINAL count is unchanged and the value still rebuilds.
    store
        .put(&[chunk(16500, 7, 0, 0x2000, b"abc")])
        .await
        .unwrap();
    let after = store.fetch(16500, 7).await.unwrap();
    assert_eq!(after.get(&0).unwrap(), b"abc");
    assert_eq!(after.len(), 3);
    assert_eq!(
        ch.query(&format!("SELECT count() FROM {DB}.pg_toast_16500 FINAL"))
            .unwrap(),
        "4",
        "RMT dedups the re-shipped chunk on (chunk_id, chunk_seq)"
    );
    assert_eq!(
        ch.query(&format!(
            "SELECT max(_lsn) FROM {DB}.pg_toast_16500 WHERE chunk_id = 7 AND chunk_seq = 0"
        ))
        .unwrap(),
        "8192",
        "0x2000 wins the dedup"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ch_resolver_put_map_then_fetch_into() {
    if !fx::clickhouse_available() {
        eprintln!("skip: no clickhouse binary on PATH");
        return;
    }
    let ch_tmp = tempfile::tempdir().unwrap();
    // +4: sibling test's interserver port is its http_port + 1, so +2 collides
    let ch = fx::ChServer::spawn(ch_tmp, CH_TCP_PORT + 4, CH_HTTP_PORT + 4).expect("spawn ch");
    ch.query(&format!("CREATE DATABASE IF NOT EXISTS {DB}"))
        .expect("create db");

    let mut cfg = config(ch.port);
    cfg.toast.mode = ToastMode::ClickHouse;
    let stats = Arc::new(EmitterStats::default());
    let resolver = ToastResolver::from_config(&cfg, stats.clone()).unwrap();
    assert_eq!(resolver.mode(), ToastMode::ClickHouse);
    drive_store_backed(&resolver, &stats).await;
}

#[tokio::test]
async fn disk_resolver_put_map_then_fetch_into() {
    let mut cfg = EmitterConfig::default();
    cfg.toast.mode = ToastMode::Disk;
    // mode=disk without disk_dir is a config error, not a silent fallback.
    assert!(ToastResolver::from_config(&cfg, Arc::new(EmitterStats::default())).is_err());

    let dir = tempfile::tempdir().unwrap();
    cfg.toast.disk_dir = Some(dir.path().to_path_buf());
    let stats = Arc::new(EmitterStats::default());
    let resolver = ToastResolver::from_config(&cfg, stats.clone()).unwrap();
    assert_eq!(resolver.mode(), ToastMode::Disk);
    drive_store_backed(&resolver, &stats).await;
}

#[tokio::test]
async fn disabled_resolver_no_store_fills_on_miss() {
    // ToastMode::Disabled is the config default.
    let cfg = EmitterConfig::default();
    let stats = Arc::new(EmitterStats::default());
    let resolver = ToastResolver::from_config(&cfg, stats.clone()).unwrap();
    assert_eq!(resolver.mode(), ToastMode::Disabled);
    assert!(!resolver.stores_chunks());
    assert!(resolver.fill_on_miss());

    // put_map is a no-op: nothing persisted, nothing counted.
    let mut map = ChunkMap::new();
    map.insert((16700, 42), BTreeMap::from([(0u32, b"hello".to_vec())]));
    resolver.put_map(&map, 0xABCD).await.unwrap();
    assert_eq!(stats.toast_chunks_stored.load(Ordering::Relaxed), 0);

    // No store to hydrate from: false even for a just-put value, out untouched.
    let mut out = ChunkMap::new();
    assert!(!resolver.fetch_into(16700, 42, &mut out).await.unwrap());
    assert!(out.is_empty());
    assert_eq!(stats.toast_values_fetched.load(Ordering::Relaxed), 0);
}
