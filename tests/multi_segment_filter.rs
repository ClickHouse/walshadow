//! PRE5b1 regression: `CatalogTracker` state must survive a segment
//! boundary so that an `XLOG_RELMAP_UPDATE` (or a decoded pg_class
//! heap-tuple) in segment N can authorise a heap record in segment
//! N+1 against the rewritten filenode.
//!
//! Pre-fix, `filter_segment` constructed a fresh `Filter` each call;
//! every segment's tracker started empty. Two synthetic 1-page
//! segments (seg_size = 8 KiB) are pushed through `WalStream`; the
//! second segment's heap record targets the rewritten filenode from
//! the first. With the fix it lands as `Kept`; without it it would
//! drop.
//!
//! The test bypasses both the live-PG e2e setup in
//! `wal_stream_e2e.rs` and the on-disk fixtures in
//! `filter_round_trip.rs` — synthetic byte construction is the only
//! way to force a relmap update on segment N and a dependent heap
//! record on segment N+1 deterministically.

use wal_rs::pg::walparser::{
    RmId, XLP_LONG_HEADER, XLP_PAGE_MAGIC_PG15, XLR_BLOCK_ID_DATA_LONG, X_LOG_RECORD_HEADER_SIZE,
};
use walshadow::filter::Decision;
use walshadow::manifest::Kind;
use walshadow::rewrite::compute_crc;
use walshadow::wal_stream::{CollectingRecordSink, CollectingSegmentSink, WalStream};

/// Synthetic segment / page size: one 8 KiB page per segment. Math in
/// `WalStream::segment_for_lsn` requires `seg_size` to divide `2^32`;
/// `8192 = 2^13` qualifies.
const SEG_SIZE: u64 = 8192;

/// Mirror of `catalog_tracker::REL_MAP_FILE_SIZE` (`magic + n + 64 * 8 + crc`).
const MAX_MAPPINGS: usize = 64;
const REL_MAP_FILE_SIZE: i32 = 4 + 4 + (MAX_MAPPINGS as i32) * 8 + 4;
/// `RELMAPPER_FILEMAGIC` from `src/backend/utils/cache/relmapper.c`.
const RELMAPPER_FILEMAGIC: i32 = 0x592717;

fn build_relmap_main_data(dbid: u32, mappings: &[(u32, u32)]) -> Vec<u8> {
    let mut data = Vec::new();
    data.extend_from_slice(&dbid.to_le_bytes());
    data.extend_from_slice(&1664u32.to_le_bytes()); // tsid pg_global
    data.extend_from_slice(&REL_MAP_FILE_SIZE.to_le_bytes());
    data.extend_from_slice(&RELMAPPER_FILEMAGIC.to_le_bytes());
    data.extend_from_slice(&(mappings.len() as i32).to_le_bytes());
    for &(oid, fnum) in mappings {
        data.extend_from_slice(&oid.to_le_bytes());
        data.extend_from_slice(&fnum.to_le_bytes());
    }
    for _ in mappings.len()..MAX_MAPPINGS {
        data.extend_from_slice(&[0u8; 8]);
    }
    data.extend_from_slice(&0u32.to_le_bytes()); // crc, ignored by decoder
    data
}

fn write_header(v: &mut Vec<u8>, total: u32, info: u8, rmid: u8) {
    v.extend_from_slice(&total.to_le_bytes()); // xl_tot_len
    v.extend_from_slice(&0u32.to_le_bytes()); // xl_xid
    v.extend_from_slice(&0u64.to_le_bytes()); // xl_prev
    v.push(info);
    v.push(rmid);
    v.push(0); // pad
    v.push(0); // pad
    v.extend_from_slice(&0u32.to_le_bytes()); // crc placeholder
}

fn finalise_crc(v: &mut [u8]) {
    let crc = compute_crc(v);
    v[20..24].copy_from_slice(&crc.to_le_bytes());
}

/// Record carrying only main_data, encoded with `XLR_BLOCK_ID_DATA_LONG`
/// for payloads > 255 bytes (relmap updates are ~536 bytes).
fn build_record_with_main_data(rmid: u8, info: u8, main_data: &[u8]) -> Vec<u8> {
    let body_len = 1 + 4 + main_data.len();
    let total = X_LOG_RECORD_HEADER_SIZE + body_len;
    let mut v = Vec::with_capacity(total);
    write_header(&mut v, total as u32, info, rmid);
    v.push(XLR_BLOCK_ID_DATA_LONG);
    v.extend_from_slice(&(main_data.len() as u32).to_le_bytes());
    v.extend_from_slice(main_data);
    finalise_crc(&mut v);
    v
}

/// Record with one block reference, no block data, no main_data — just
/// enough to land in `Class::User` and exercise the tracker promotion.
fn build_record_with_block_ref(
    rmid: u8,
    info: u8,
    spc_node: u32,
    db_node: u32,
    rel_node: u32,
    block_no: u32,
) -> Vec<u8> {
    let body_len = 1 + 1 + 2 + 12 + 4; // block_id, fork_flags, data_length, rel, block_no
    let total = X_LOG_RECORD_HEADER_SIZE + body_len;
    let mut v = Vec::with_capacity(total);
    write_header(&mut v, total as u32, info, rmid);
    v.push(0); // block_id = 0
    v.push(0); // fork_flags = 0 (no has_data, no has_image, no same_rel)
    v.extend_from_slice(&0u16.to_le_bytes()); // data_length = 0
    v.extend_from_slice(&spc_node.to_le_bytes());
    v.extend_from_slice(&db_node.to_le_bytes());
    v.extend_from_slice(&rel_node.to_le_bytes());
    v.extend_from_slice(&block_no.to_le_bytes());
    finalise_crc(&mut v);
    v
}

/// Single-page segment: long header (40 bytes) + records (8-byte
/// aligned) + zero tail. `WalStream` accepts any `seg_size` that
/// divides `2^32`; the long-header `seg_size` field is informational
/// for the walker, not validated against the outer `seg_size`.
fn build_one_page_segment(records: &[&[u8]]) -> Vec<u8> {
    let mut page = Vec::with_capacity(SEG_SIZE as usize);
    page.extend_from_slice(&XLP_PAGE_MAGIC_PG15.to_le_bytes());
    page.extend_from_slice(&XLP_LONG_HEADER.to_le_bytes());
    page.extend_from_slice(&1u32.to_le_bytes()); // timeline
    page.extend_from_slice(&0u64.to_le_bytes()); // page_address
    page.extend_from_slice(&0u32.to_le_bytes()); // remaining_data_len
    page.extend_from_slice(&12345u64.to_le_bytes()); // sysid
    page.extend_from_slice(&(SEG_SIZE as u32).to_le_bytes()); // seg_size
    page.extend_from_slice(&8192u32.to_le_bytes()); // xlog_block_size
    page.extend_from_slice(&[0u8; 4]); // pad to 40
    for r in records {
        page.extend_from_slice(r);
        let pad = (8 - (page.len() % 8)) % 8;
        page.extend(std::iter::repeat_n(0u8, pad));
    }
    page.resize(SEG_SIZE as usize, 0);
    page
}

/// pg_class OID — mapped catalog, gets rewritten by relmap updates.
const PG_CLASS_OID: u32 = 1259;
const TEST_DB_NODE: u32 = 5;
const REWRITTEN_PG_CLASS_FILENODE: u32 = 50000; // > 16384, would look user without relmap

#[test]
fn catalog_tracker_state_survives_segment_boundary() {
    // Segment 1: a single XLOG_RELMAP_UPDATE that maps pg_class to a
    // filenode in the user range. Class::Special, kept unconditionally;
    // its side effect is that tracker.nodes gains (TEST_DB_NODE,
    // REWRITTEN_PG_CLASS_FILENODE).
    let relmap_main = build_relmap_main_data(
        TEST_DB_NODE,
        &[(PG_CLASS_OID, REWRITTEN_PG_CLASS_FILENODE)],
    );
    let relmap_rec = build_record_with_main_data(
        RmId::RelMap as u8,
        0x00, // XLOG_RELMAP_UPDATE
        &relmap_main,
    );
    let seg1 = build_one_page_segment(&[&relmap_rec]);

    // Segment 2: a heap insert touching (TEST_DB_NODE,
    // REWRITTEN_PG_CLASS_FILENODE). Class::User by classify (filenode
    // >= 16384), but the tracker has it as catalog after seg1, so the
    // Filter must keep it. Pre-fix, the per-segment Filter would
    // re-bootstrap and drop it.
    let heap_rec = build_record_with_block_ref(
        RmId::Heap as u8,
        0x00, // XLOG_HEAP_INSERT
        1663, // pg_default
        TEST_DB_NODE,
        REWRITTEN_PG_CLASS_FILENODE,
        0,
    );
    let seg2 = build_one_page_segment(&[&heap_rec]);

    let mut stream = WalStream::new(1, SEG_SIZE, 0).expect("WalStream::new");
    let mut records = CollectingRecordSink::default();
    let mut segs = CollectingSegmentSink::default();

    stream
        .push(0, &seg1, &mut records, &mut segs)
        .expect("push seg1");
    stream
        .push(SEG_SIZE, &seg2, &mut records, &mut segs)
        .expect("push seg2");

    assert_eq!(segs.segments.len(), 2, "two segments dispatched");
    assert_eq!(records.events.len(), 2, "two record events surfaced");

    // Segment 1's relmap update — kept (special rmgr).
    let seg1_manifest = &segs.segments[0].2;
    assert_eq!(seg1_manifest.records.len(), 1);
    assert_eq!(seg1_manifest.records[0].kind, Kind::Kept);
    assert_eq!(seg1_manifest.stats.relmap_updates, 1);

    // Segment 2's heap record — kept iff the tracker carried the
    // relmap update across the segment boundary. This is the
    // regression assertion.
    let seg2_manifest = &segs.segments[1].2;
    assert_eq!(seg2_manifest.records.len(), 1);
    assert_eq!(
        seg2_manifest.records[0].kind,
        Kind::Kept,
        "heap record on relmap-rewritten pg_class filenode must be kept; \
         a per-segment Filter would lose the seg-1 relmap and drop this",
    );
    // No new relmap updates landed in seg 2 — `ManifestStats` is
    // per-segment even though `FilterStats` on the long-lived `Filter`
    // is cumulative.
    assert_eq!(seg2_manifest.stats.relmap_updates, 0);

    // Cumulative filter stats sanity: 2 records seen total, both kept.
    let filter = stream.filter();
    assert_eq!(filter.stats.kept, 2);
    assert_eq!(filter.stats.dropped, 0);
    assert_eq!(filter.tracker.relmap_updates, 1);

    // RecordSink decisions reflect the same outcome.
    assert_eq!(records.events[0].decision, Decision::Keep);
    assert_eq!(records.events[1].decision, Decision::Keep);
}
