//! Both `[toast] mode` resolvers built via `from_config`, driven through the
//! same `ToastResolver` flow (`put` / `fetch_into`) the WAL + bootstrap paths
//! use: `clickhouse` runs a store-backed scenario, `disabled` pins the
//! no-store policy (put no-op, fetch_into false, fill-on-miss).
//!
//! The ClickHouse chunk-store backend additionally runs against a real
//! ClickHouse: TID-keyed rows mirror `pg_toast_<relid>` relations, `put`
//! writes chunk births + delete tombstones, `fetch` resolves per-TID as-of
//! state at the referring LSN into the reassembler's `(seq -> bytes)` map.
//! Pins the v2 schema (`ReplacingMergeTree(_lsn, _is_deleted)` ordered by
//! TID), byte-transparency of `chunk_data` (raw bytea, non-UTF-8 included),
//! the missing-mirror hard error, tombstone visibility, TID reuse,
//! reused-`va_valueid` generation separation, and fetch over
//! merge-collapsed parts.
//!
//! Full pipeline coverage (PG WAL -> reassembly -> CH row) lives in
//! `toast_e2e.rs`.

#![cfg(target_os = "linux")]

#[path = "common/inproc_harness.rs"]
mod fx;

use std::sync::Arc;
use std::sync::atomic::Ordering;

use walshadow::ch_emitter::{EmitterConfig, EmitterStats};
use walshadow::mapping::ToastMode;
use walshadow::spill::ToastDelete;
use walshadow::toast::{
    ChunkStore, ChunkStoreError, ClickHouseChunkStore, FetchedValue, ToastResolver, ToastRow,
};

fn assembled(body: &[u8]) -> FetchedValue {
    FetchedValue::Assembled(body.to_vec())
}

const CH_TCP_PORT: u16 = 17639;
const CH_HTTP_PORT: u16 = 17640;
const DB: &str = "walshadow_toast_test";

fn row(relid: u32, value_id: u32, seq: u32, tid: (u32, u16), lsn: u64, body: &[u8]) -> ToastRow {
    ToastRow {
        toast_relid: relid,
        blkno: tid.0,
        offnum: tid.1,
        chunk_id: value_id,
        chunk_seq: seq,
        chunk_data: bytes::Bytes::copy_from_slice(body),
        lsn,
    }
}

fn tomb(relid: u32, tid: (u32, u16), lsn: u64) -> ToastRow {
    ToastRow::tombstone(&ToastDelete {
        toast_relid: relid,
        blkno: tid.0,
        offnum: tid.1,
        source_lsn: lsn,
    })
}

fn config(port: u16) -> EmitterConfig {
    EmitterConfig {
        host: "127.0.0.1".into(),
        port,
        database: DB.into(),
        ..EmitterConfig::default()
    }
}

/// WAL-path scenario for the store-backed resolver: put chunk rows at their
/// record LSNs, rehydrate a pre-window re-emit via `fetch_value`, surface a
/// genuine miss as `Missing`, count tombstones separately.
async fn drive_store_backed(resolver: &ToastResolver, stats: &EmitterStats) {
    assert!(resolver.stores_chunks());
    assert!(!resolver.fill_on_miss());

    resolver
        .put(&[
            row(16700, 42, 0, (1, 1), 0xAB00, b"hello "),
            row(16700, 42, 1, (1, 2), 0xAB01, b"world"),
            tomb(16700, (9, 9), 0xAB02),
        ])
        .await
        .unwrap();
    assert_eq!(stats.toast_chunks_stored.load(Ordering::Relaxed), 2);
    assert_eq!(stats.toast_tombstones_stored.load(Ordering::Relaxed), 1);

    // Pre-window re-emit: the in-xact buffer missed these chunks, so the
    // reassembler rehydrates the value from the store.
    let got = resolver.fetch_value(16700, 42, u64::MAX, 11).await.unwrap();
    assert_eq!(got, Some(assembled(b"hello world")));
    assert_eq!(stats.toast_values_fetched.load(Ordering::Relaxed), 1);

    // Genuine miss -> Missing (caller fills + counts).
    let miss = resolver
        .fetch_value(16700, 404, u64::MAX, 11)
        .await
        .unwrap();
    assert_eq!(miss, Some(FetchedValue::Missing));
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
            row(16500, 7, 0, (1, 1), 0x1000, b"abc"),
            row(16500, 7, 1, (1, 2), 0x1001, b"de"),
        ])
        .await
        .unwrap();
    // Background merges collapse superseded versions on their own schedule
    // (the design's superseded-fill case). Freeze the mirror — targeted, and
    // only once the first put created it: a global STOP MERGES doesn't cover
    // tables created after it. Re-enabled before the merge stage.
    ch.query(&format!("SYSTEM STOP MERGES {DB}.pg_toast_16500"))
        .expect("stop merges");
    store
        .put(&[row(16500, 7, 2, (1, 3), 0x1002, b"\x00\xff\x01")])
        .await
        .unwrap();
    // Multi-relid put splits per table
    store
        .put(&[
            row(16500, 9, 0, (3, 1), 0x1100, b"zz"),
            row(16600, 3, 0, (1, 1), 0x1100, b"xy"),
            row(16600, 3, 1, (1, 2), 0x1101, b"z"),
        ])
        .await
        .unwrap();

    // Reassembly-shaped fetch: dense seqs assembled in order, raw-byte
    // transparent.
    let got = store.fetch(16500, 7, u64::MAX, 8).await.unwrap();
    assert_eq!(got, assembled(b"abcde\x00\xff\x01"));

    assert_eq!(
        store.fetch(16500, 9, u64::MAX, 2).await.unwrap(),
        assembled(b"zz")
    );
    assert_eq!(
        store.fetch(16600, 3, u64::MAX, 3).await.unwrap(),
        assembled(b"xyz")
    );

    // Bound below every row -> no generation visible yet.
    assert_eq!(
        store.fetch(16500, 7, 0x0fff, 8).await.unwrap(),
        FetchedValue::Missing
    );
    // Table exists, no such value_id -> Missing (caller decides fill vs error).
    assert_eq!(
        store.fetch(16500, 999, u64::MAX, 1).await.unwrap(),
        FetchedValue::Missing
    );
    // Relation never received a chunk -> no CH table -> hard error, fast
    // (no retry): mirror absence is never proof of supersession.
    assert!(matches!(
        store.fetch(70000, 1, u64::MAX, 1).await,
        Err(ChunkStoreError::MissingMirror(70000))
    ));

    // Rows really landed on CH, in the mirrored table.
    assert_eq!(
        ch.query(&format!("SELECT count() FROM {DB}.pg_toast_16500"))
            .unwrap(),
        "4"
    );

    // Re-put is byte-identical (replay re-emit): RMT collapses the copies.
    store
        .put(&[row(16500, 7, 0, (1, 1), 0x1000, b"abc")])
        .await
        .unwrap();
    assert_eq!(
        ch.query(&format!("SELECT count() FROM {DB}.pg_toast_16500 FINAL"))
            .unwrap(),
        "4",
        "re-emit dedups under (TID, _lsn)"
    );

    // Death of value 9: a tombstone at its TID. Visible history is as-of.
    store.put(&[tomb(16500, (3, 1), 0x1800)]).await.unwrap();
    assert_eq!(
        store.fetch(16500, 9, 0x17FF, 2).await.unwrap(),
        assembled(b"zz"),
        "referrer before the death still resolves"
    );
    assert_eq!(
        store.fetch(16500, 9, u64::MAX, 2).await.unwrap(),
        FetchedValue::Missing,
        "dead past its tombstone"
    );

    // Line-pointer reuse: a new value born at the dead TID supersedes the
    // tombstone; the old occupant stays dead, its window intact.
    store
        .put(&[row(16500, 13, 0, (3, 1), 0x2000, b"reborn")])
        .await
        .unwrap();
    assert_eq!(
        store.fetch(16500, 13, u64::MAX, 6).await.unwrap(),
        assembled(b"reborn")
    );
    assert_eq!(
        store.fetch(16500, 9, u64::MAX, 2).await.unwrap(),
        FetchedValue::Missing
    );
    assert_eq!(
        store.fetch(16500, 9, 0x17FF, 2).await.unwrap(),
        assembled(b"zz")
    );

    // Reused va_valueid: generation 1 dies (tombstones), generation 2 lands
    // at fresh TIDs, shorter. A lagging referrer keeps generation 1 whole; a
    // current one gets generation 2 only — never new seq 0 + stale suffix.
    store
        .put(&[
            row(16500, 11, 0, (5, 1), 0x3000, b"g1-0"),
            row(16500, 11, 1, (5, 2), 0x3001, b"g1-1"),
            row(16500, 11, 2, (5, 3), 0x3002, b"g1-2"),
        ])
        .await
        .unwrap();
    store
        .put(&[
            tomb(16500, (5, 1), 0x3800),
            tomb(16500, (5, 2), 0x3801),
            tomb(16500, (5, 3), 0x3802),
            row(16500, 11, 0, (6, 1), 0x4000, b"g2-0"),
            row(16500, 11, 1, (6, 2), 0x4001, b"g2-1"),
        ])
        .await
        .unwrap();
    let old = store.fetch(16500, 11, 0x3400, 12).await.unwrap();
    assert_eq!(
        old,
        assembled(b"g1-0g1-1g1-2"),
        "older referrer keeps first generation"
    );
    // Dead generation's seq 2 dropped: only g2 assembles at head
    let regen = store.fetch(16500, 11, u64::MAX, 8).await.unwrap();
    assert_eq!(regen, assembled(b"g2-0g2-1"));

    // Merge is the reclaimer: OPTIMIZE FINAL collapses superseded versions
    // per TID (dead chunk_data reclaimed, tombstones retained), and fetch
    // stays correct over the merged parts.
    ch.query(&format!("SYSTEM START MERGES {DB}.pg_toast_16500"))
        .expect("start merges");
    ch.query(&format!("OPTIMIZE TABLE {DB}.pg_toast_16500 FINAL"))
        .unwrap();
    assert_eq!(
        ch.query(&format!(
            "SELECT count() FROM {DB}.pg_toast_16500 WHERE chunk_id = 11 AND _is_deleted = 0"
        ))
        .unwrap(),
        "2",
        "dead generation's data rows merged away, no walshadow GC"
    );
    assert_eq!(
        ch.query(&format!(
            "SELECT count() FROM {DB}.pg_toast_16500 WHERE chunk_id = 9"
        ))
        .unwrap(),
        "0",
        "tombstoned value's data row merged away"
    );
    assert_eq!(
        store.fetch(16500, 11, u64::MAX, 8).await.unwrap(),
        assembled(b"g2-0g2-1"),
        "fetch over merged parts"
    );
    // Collapsed history: the lagging bound now misses — the documented
    // superseded-fill case, Missing not error.
    assert_eq!(
        store.fetch(16500, 11, 0x3400, 12).await.unwrap(),
        FetchedValue::Missing
    );
    assert_eq!(
        store.fetch(16500, 9, 0x17FF, 2).await.unwrap(),
        FetchedValue::Missing
    );

    // Mirror wipe (owner TRUNCATE / retired toast rel): rows gone, table
    // kept — fetch turns Missing (superseded fill), never MissingMirror.
    store.truncate_mirror(16600).await.unwrap();
    assert_eq!(
        store.fetch(16600, 3, u64::MAX, 3).await.unwrap(),
        FetchedValue::Missing
    );
    assert_eq!(
        ch.query(&format!("SELECT count() FROM {DB}.pg_toast_16600"))
            .unwrap(),
        "0"
    );
    // Never-created mirror stays missing (TRUNCATE ... IF EXISTS no-op).
    store.truncate_mirror(70000).await.unwrap();
    assert!(matches!(
        store.fetch(70000, 1, u64::MAX, 1).await,
        Err(ChunkStoreError::MissingMirror(70000))
    ));
    // Post-wipe births repopulate the kept table.
    store
        .put(&[row(16600, 5, 0, (9, 1), 0x5000, b"post-wipe")])
        .await
        .unwrap();
    assert_eq!(
        store.fetch(16600, 5, u64::MAX, 9).await.unwrap(),
        assembled(b"post-wipe")
    );

    // One value split over many result blocks: fetch_sql caps
    // max_block_size, the assembler validates seq order across block
    // boundaries. 3000 chunks > 2 blocks at 1024 rows each.
    let n = 3000u32;
    let rows: Vec<ToastRow> = (0..n)
        .map(|i| {
            row(
                16500,
                21,
                i,
                (100 + i / 100, 1 + (i % 100) as u16),
                0x6000 + u64::from(i),
                &[b'a' + (i % 26) as u8],
            )
        })
        .collect();
    for slice in rows.chunks(500) {
        store.put(slice).await.unwrap();
    }
    let expected: Vec<u8> = (0..n).map(|i| b'a' + (i % 26) as u8).collect();
    assert_eq!(
        store.fetch(16500, 21, u64::MAX, n as usize).await.unwrap(),
        FetchedValue::Assembled(expected),
        "value assembled across block boundaries"
    );
    // Wrong pointer size against the same run: deviation fills, not errors
    assert_eq!(
        store
            .fetch(16500, 21, u64::MAX, n as usize + 1)
            .await
            .unwrap(),
        FetchedValue::Mismatch { got: n as usize }
    );
}

/// `rewrite_barrier` against real CH: residual `O - B` tombstones at the
/// commit LSN for TIDs live as of the marker with no row past it; reused
/// TIDs and pre-marker deaths untouched; re-runs insert nothing (replay
/// convergence); missing mirror no-ops.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ch_chunk_store_rewrite_barrier_residuals() {
    if !fx::clickhouse_available() {
        eprintln!("skip: no clickhouse binary on PATH");
        return;
    }
    let ch_tmp = tempfile::tempdir().unwrap();
    // +8: sibling tests' interserver ports are their http_port + 1
    let ch = fx::ChServer::spawn(ch_tmp, CH_TCP_PORT + 8, CH_HTTP_PORT + 8).expect("spawn ch");
    ch.query(&format!("CREATE DATABASE IF NOT EXISTS {DB}"))
        .expect("create db");

    let store = ClickHouseChunkStore::new(config(ch.port));
    // Old generation: value 7 at (0,1)/(0,2), value 9 dead pre-marker at
    // (0,3), value 11 live at (1,1). The death rides its own put: RMT
    // collapses same-key rows within one inserted block regardless of
    // merge state, and the as-of assertions below need (0,3)'s history.
    store
        .put(&[
            row(16800, 7, 0, (0, 1), 0x1000, b"a"),
            row(16800, 7, 1, (0, 2), 0x1001, b"b"),
            row(16800, 9, 0, (0, 3), 0x1002, b"c"),
            row(16800, 11, 0, (1, 1), 0x1003, b"d"),
        ])
        .await
        .unwrap();
    ch.query(&format!("SYSTEM STOP MERGES {DB}.pg_toast_16800"))
        .expect("stop merges");
    store.put(&[tomb(16800, (0, 3), 0x1500)]).await.unwrap();
    // Rewrite generation reuses (0,1) for value 7's single chunk.
    store
        .put(&[row(16800, 7, 0, (0, 1), 0x3000, b"a2")])
        .await
        .unwrap();
    store.rewrite_barrier(16800, 0x2000, 0x4000).await.unwrap();

    // Residuals: (0,2) and (1,1) die at the commit LSN; reused (0,1) and
    // pre-marker-dead (0,3) untouched.
    assert_eq!(
        ch.query(&format!(
            "SELECT groupArray((blkno, offnum)) FROM (\
               SELECT blkno, offnum FROM {DB}.pg_toast_16800 \
               WHERE _lsn = 16384 AND _is_deleted = 1 ORDER BY blkno, offnum)"
        ))
        .unwrap(),
        "[(0,2),(1,1)]"
    );
    assert_eq!(
        store.fetch(16800, 7, u64::MAX, 2).await.unwrap(),
        assembled(b"a2"),
        "reused TID survives at its birth version"
    );
    assert_eq!(
        store.fetch(16800, 11, u64::MAX, 1).await.unwrap(),
        FetchedValue::Missing
    );
    // Pre-rewrite window intact: no destructive step ran.
    assert_eq!(
        store.fetch(16800, 7, 0x1fff, 2).await.unwrap(),
        assembled(b"ab")
    );
    assert_eq!(
        store.fetch(16800, 9, 0x14ff, 1).await.unwrap(),
        assembled(b"c")
    );

    // Replay re-run: prior residuals sit past the marker, nothing inserts.
    store.rewrite_barrier(16800, 0x2000, 0x4000).await.unwrap();
    assert_eq!(
        ch.query(&format!(
            "SELECT count() FROM {DB}.pg_toast_16800 WHERE _lsn = 16384"
        ))
        .unwrap(),
        "2",
        "barrier re-run must insert nothing"
    );
    // Never-populated mirror: no table, nothing lived, no-op.
    store.rewrite_barrier(70000, 0x10, 0x20).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ch_resolver_put_rows_then_fetch_into() {
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
    let resolver = ToastResolver::from_config(&cfg, stats.clone());
    assert!(resolver.stores_chunks());
    drive_store_backed(&resolver, &stats).await;
}

#[tokio::test]
async fn disabled_resolver_no_store_fills_on_miss() {
    // ToastMode::Disabled is the config default.
    let cfg = EmitterConfig::default();
    let stats = Arc::new(EmitterStats::default());
    let resolver = ToastResolver::from_config(&cfg, stats.clone());
    assert!(!resolver.stores_chunks());
    assert!(resolver.fill_on_miss());

    // put is a no-op: nothing persisted, nothing counted.
    resolver
        .put(&[row(16700, 42, 0, (1, 1), 0xAB00, b"hello")])
        .await
        .unwrap();
    assert_eq!(stats.toast_chunks_stored.load(Ordering::Relaxed), 0);

    // No store to hydrate from: None even for a just-put value.
    assert!(
        resolver
            .fetch_value(16700, 42, u64::MAX, 5)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(stats.toast_values_fetched.load(Ordering::Relaxed), 0);
}
