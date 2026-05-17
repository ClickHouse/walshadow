//! PRE5b10 item 5: feed a captured WAL segment through `WalStream` at
//! varying chunk sizes — one byte at a time, prime-sized chunks, single
//! bulk push — and assert the per-record event sequence is identical
//! across all paths.
//!
//! Backstop for the latency-contract refactor on [`WalStream::push`]
//! (PRE5b10 item 2): a future chunk-driven walker that yields records
//! before the segment fills must still emit the exact same record
//! sequence as today's "accumulate, then `filter_segment`" path.
//! Equivalence here means structural per-record fields, not byte
//! identity of the buffered segment (filter is deterministic, so any
//! drift would surface as differing record offsets or rmids).
//!
//! Skipped silently if the captured fixture is not present.

use std::io::Read;
use std::path::PathBuf;
use std::process::{Command, Stdio};

use walshadow::wal_stream::{CollectingRecordSink, CollectingSegmentSink, Record, WalStream};

fn fixture_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("fixtures/wal/classify/segments/000000010000000000000001.gz")
}

fn decompress_gz(path: &PathBuf) -> std::io::Result<Vec<u8>> {
    let mut child = Command::new("gunzip")
        .arg("-c")
        .arg(path)
        .stdout(Stdio::piped())
        .spawn()?;
    let mut out = Vec::new();
    child.stdout.as_mut().unwrap().read_to_end(&mut out)?;
    let status = child.wait()?;
    if !status.success() {
        return Err(std::io::Error::other(format!(
            "gunzip {:?} failed: {status}",
            path
        )));
    }
    Ok(out)
}

/// Stable fingerprint of a `Record`. Filter is deterministic so any
/// drift between bulk and chunked paths surfaces here as inequality.
#[derive(Debug, PartialEq, Eq)]
struct RecordKey {
    source_lsn: u64,
    rmid: u8,
    info: u8,
    xact_id: u32,
    total_len: u32,
    decision: walshadow::filter::Decision,
    page_magic: u16,
    block_locators: Vec<(u32, u32, u32)>,
}

fn key(r: &Record) -> RecordKey {
    RecordKey {
        source_lsn: r.source_lsn,
        rmid: r.parsed.header.resource_manager_id,
        info: r.parsed.header.info,
        xact_id: r.parsed.header.xact_id,
        total_len: r.parsed.header.total_record_length,
        decision: r.decision,
        page_magic: r.page_magic,
        block_locators: r
            .parsed
            .blocks
            .iter()
            .map(|b| {
                let rfn = b.header.location.rel;
                (rfn.spc_node, rfn.db_node, rfn.rel_node)
            })
            .collect(),
    }
}

async fn run_with_chunk_sizes(bytes: &[u8], seg_size: u64, chunks: &[usize]) -> Vec<RecordKey> {
    let mut stream = WalStream::new(1, seg_size, 0).expect("stream new");
    let mut recs = CollectingRecordSink::default();
    let mut segs = CollectingSegmentSink::default();
    let mut lsn = 0u64;
    let mut off = 0usize;
    let mut chunk_iter = chunks.iter().cycle();
    while off < bytes.len() {
        let take = (*chunk_iter.next().unwrap()).min(bytes.len() - off);
        stream
            .push(lsn, &bytes[off..off + take], &mut recs, &mut segs)
            .await
            .expect("push");
        lsn += take as u64;
        off += take;
    }
    recs.records.iter().map(key).collect()
}

#[tokio::test(flavor = "current_thread")]
async fn bulk_and_byte_chunks_emit_identical_record_sequence() {
    let path = fixture_path();
    if !path.exists() {
        eprintln!("skip: no captured segment at {path:?}");
        return;
    }
    let bytes = decompress_gz(&path).expect("gunzip fixture");
    assert!(
        bytes.len().is_multiple_of(8192),
        "fixture must be page-aligned"
    );
    let seg_size = bytes.len() as u64;

    // (a) one big push
    let bulk = run_with_chunk_sizes(&bytes, seg_size, &[bytes.len()]).await;
    assert!(!bulk.is_empty(), "fixture had zero records");

    // (b) one byte at a time
    let byte_at_a_time = run_with_chunk_sizes(&bytes, seg_size, &[1]).await;
    assert_eq!(
        bulk, byte_at_a_time,
        "byte-at-a-time record sequence diverged from bulk push",
    );

    // (c) varying chunk sizes that cross page (8192) and record-header
    // (24) boundaries non-trivially. Primes so the chunker doesn't
    // coincidentally land on the same offsets as the bulk path.
    let prime_chunks = run_with_chunk_sizes(&bytes, seg_size, &[1, 7, 13, 257, 8193, 65537]).await;
    assert_eq!(
        bulk, prime_chunks,
        "prime-chunked record sequence diverged from bulk push",
    );
}

/// Bulk vs chunked must also match on the segment-sink output bytes:
/// `current_buf` accumulates identically regardless of push cadence,
/// so the rewritten segment delivered to the segment sink must be
/// byte-equal across cadences.
#[tokio::test(flavor = "current_thread")]
async fn bulk_and_chunked_emit_identical_segment_bytes() {
    let path = fixture_path();
    if !path.exists() {
        eprintln!("skip: no captured segment at {path:?}");
        return;
    }
    let bytes = decompress_gz(&path).expect("gunzip fixture");
    let seg_size = bytes.len() as u64;

    let mut stream_bulk = WalStream::new(1, seg_size, 0).unwrap();
    let mut rec_bulk = CollectingRecordSink::default();
    let mut seg_bulk = CollectingSegmentSink::default();
    stream_bulk
        .push(0, &bytes, &mut rec_bulk, &mut seg_bulk)
        .await
        .expect("bulk push");

    let mut stream_chunked = WalStream::new(1, seg_size, 0).unwrap();
    let mut rec_chunked = CollectingRecordSink::default();
    let mut seg_chunked = CollectingSegmentSink::default();
    for (i, b) in bytes.iter().enumerate() {
        stream_chunked
            .push(
                i as u64,
                std::slice::from_ref(b),
                &mut rec_chunked,
                &mut seg_chunked,
            )
            .await
            .expect("chunked push");
    }

    assert_eq!(seg_bulk.segments.len(), 1);
    assert_eq!(seg_chunked.segments.len(), 1);
    let (_, bulk_bytes, bulk_mani) = &seg_bulk.segments[0];
    let (_, chunked_bytes, chunked_mani) = &seg_chunked.segments[0];
    assert_eq!(bulk_bytes, chunked_bytes, "segment bytes diverged");
    assert_eq!(
        bulk_mani.records.len(),
        chunked_mani.records.len(),
        "manifest record counts diverged",
    );
}
