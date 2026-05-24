//! Feed a captured WAL segment through `WalStream` at varying chunk
//! sizes — one byte at a time, prime-sized chunks, single bulk push —
//! and assert the per-record event sequence is identical across all
//! paths. With the streaming walker, records yield as their last
//! byte lands; this test pins the contract that record sequence and
//! segment bytes are cadence-invariant.
//!
//! Skipped silently if the captured fixture is not present.

use std::io::Read;
use std::path::PathBuf;
use std::process::{Command, Stdio};

use walshadow::shadow_stream::{ShadowStreamSink, ShadowStreamState};
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

    let bulk = run_with_chunk_sizes(&bytes, seg_size, &[bytes.len()]).await;
    assert!(!bulk.is_empty(), "fixture had zero records");

    let byte_at_a_time = run_with_chunk_sizes(&bytes, seg_size, &[1]).await;
    assert_eq!(
        bulk, byte_at_a_time,
        "byte-at-a-time record sequence diverged from bulk push",
    );

    let prime_chunks = run_with_chunk_sizes(&bytes, seg_size, &[1, 7, 13, 257, 8193, 65537]).await;
    assert_eq!(
        bulk, prime_chunks,
        "prime-chunked record sequence diverged from bulk push",
    );
}

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

/// Shadow-stream sink: a `ShadowStreamSink` installed on `WalStream`
/// dispatches the full byte-exact wire stream. Aggregate dispatched
/// bytes equal the segment's bytes.
#[tokio::test(flavor = "current_thread")]
async fn shadow_stream_sink_receives_byte_exact_wire_stream() {
    use std::sync::Arc;
    use tokio::sync::Mutex;

    let path = fixture_path();
    if !path.exists() {
        eprintln!("skip: no captured segment at {path:?}");
        return;
    }
    let bytes = decompress_gz(&path).expect("gunzip fixture");
    let seg_size = bytes.len() as u64;

    let mut stream = WalStream::new(1, seg_size, 0).expect("stream new");
    let mut rec_sink = CollectingRecordSink::default();
    let mut seg_sink = CollectingSegmentSink::default();

    let state = Arc::new(Mutex::new(ShadowStreamState::new(
        1,
        "1".into(),
        0,
        1024 * 1024 * 1024,
    )));
    let conn = state.lock().await.register_connection(0);
    let sink = ShadowStreamSink::new(state.clone());
    stream.set_bytes_sink(Box::new(sink));

    for (i, b) in bytes.iter().enumerate() {
        stream
            .push(
                i as u64,
                std::slice::from_ref(b),
                &mut rec_sink,
                &mut seg_sink,
            )
            .await
            .expect("push");
    }

    assert_eq!(seg_sink.segments.len(), 1, "one segment dispatched");
    assert!(!rec_sink.records.is_empty(), "records dispatched");

    // Each queued frame on the connection is a CopyData envelope
    // wrapping a 'w' XLogData. Strip the framing and reconstruct
    // the wire byte stream; it must match the segment buffer
    // byte-for-byte.
    let queued = state
        .lock()
        .await
        .drain_send_queue(conn)
        .unwrap_or_default();
    let reconstructed = strip_wal_frames(&queued);
    let (_, seg_bytes, _) = &seg_sink.segments[0];
    assert_eq!(
        reconstructed.len(),
        seg_bytes.len(),
        "wire stream length matches segment",
    );
    assert_eq!(&reconstructed, seg_bytes, "wire stream matches segment");
}

/// Strip CopyData ('d') envelopes from a concatenation of frames,
/// returning the WAL payload bytes from every 'w' XLogData inside.
fn strip_wal_frames(buf: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    let mut i = 0;
    while i + 5 <= buf.len() {
        if buf[i] != b'd' {
            break;
        }
        let len = u32::from_be_bytes(buf[i + 1..i + 5].try_into().unwrap()) as usize;
        let total = 1 + len;
        if i + total > buf.len() {
            break;
        }
        let body = &buf[i + 5..i + total];
        if body.first().copied() == Some(b'w') && body.len() >= 1 + 8 + 8 + 8 {
            let payload = &body[1 + 8 + 8 + 8..];
            out.extend_from_slice(payload);
        }
        i += total;
    }
    out
}
