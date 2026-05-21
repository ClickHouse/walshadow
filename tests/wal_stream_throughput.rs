//! Microbenchmark for the daemon's per-record pump cost.
//!
//! Generates a synthetic 16 MiB segment full of heap-insert records,
//! pumps it through `WalStream::push` with progressively heavier sink
//! combinations, and prints records-per-second + MB/s for each. Lets
//! us isolate which sink dominates the per-record cost the phase14
//! streaming tests trip over.
//!
//! Not a CI assertion (results are hardware-dependent). Run with
//! `cargo test --release --test wal_stream_throughput -- --nocapture`.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Instant;

use tokio::sync::Mutex;
use wal_rs::pg::walparser::{
    RmId, X_LOG_RECORD_HEADER_SIZE, XLP_LONG_HEADER, XLP_PAGE_MAGIC_PG15, XLR_BLOCK_ID_DATA_LONG,
};

use walshadow::queueing_record_sink::QueueingRecordSink;
use walshadow::rewrite::compute_crc;
use walshadow::shadow_stream::{ShadowStreamSink, ShadowStreamState};
use walshadow::wal_stream::{
    CollectingSegmentSink, CountingRecordSink, Record, RecordSink, SinkError, WalStream,
};

/// 8 KiB single-page segments. Multi-page WAL needs proper short page
/// headers between every PAGE_SIZE boundary; for a benchmark we keep
/// it to one page per segment and run many iterations to amortise.
const SEG_SIZE: u64 = 8192;
const ITERATIONS: usize = 4096;
const TEST_DB_NODE: u32 = 5;
const TEST_REL_NODE_BASE: u32 = 16_400; // user range

fn write_header(v: &mut Vec<u8>, total: u32, info: u8, rmid: u8, xid: u32) {
    v.extend_from_slice(&total.to_le_bytes());
    v.extend_from_slice(&xid.to_le_bytes());
    v.extend_from_slice(&0u64.to_le_bytes()); // xl_prev
    v.push(info);
    v.push(rmid);
    v.push(0);
    v.push(0);
    v.extend_from_slice(&0u32.to_le_bytes()); // crc placeholder
}

fn finalise_crc(v: &mut [u8]) {
    let crc = compute_crc(v);
    v[20..24].copy_from_slice(&crc.to_le_bytes());
}

/// Synthetic XLOG_HEAP_INSERT record: header + 1 block ref + a small
/// main_data payload (xl_heap_insert {offnum, flags}). Size ~64 bytes.
fn build_heap_insert(rel_node: u32, block_no: u32, xid: u32, main_data: &[u8]) -> Vec<u8> {
    let body_len = 1 + 1 + 2 + 12 + 4 // block ref: id, fork_flags, data_length, rel, block_no
        + 1 + 4 + main_data.len(); // XLR_BLOCK_ID_DATA_LONG + u32 len + data
    let total = X_LOG_RECORD_HEADER_SIZE + body_len;
    let mut v = Vec::with_capacity(total);
    write_header(
        &mut v,
        total as u32,
        0x00, /* XLOG_HEAP_INSERT */
        RmId::Heap as u8,
        xid,
    );
    // block ref
    v.push(0); // block_id = 0
    v.push(0); // fork_flags = 0
    v.extend_from_slice(&0u16.to_le_bytes()); // data_length = 0
    v.extend_from_slice(&1663u32.to_le_bytes()); // spc_node = pg_default
    v.extend_from_slice(&TEST_DB_NODE.to_le_bytes());
    v.extend_from_slice(&rel_node.to_le_bytes());
    v.extend_from_slice(&block_no.to_le_bytes());
    // main_data
    v.push(XLR_BLOCK_ID_DATA_LONG);
    v.extend_from_slice(&(main_data.len() as u32).to_le_bytes());
    v.extend_from_slice(main_data);
    finalise_crc(&mut v);
    v
}

/// Single-page synthetic segment packed with as many heap-insert
/// records as fit (page 0 = long header at 40 B, records 8-byte
/// aligned, zero tail).
fn build_segment(seg_size: usize) -> (Vec<u8>, u64) {
    let main_data = b"\x00\x00\x02\x00".to_vec();
    let mut buf = Vec::with_capacity(seg_size);
    buf.extend_from_slice(&XLP_PAGE_MAGIC_PG15.to_le_bytes());
    buf.extend_from_slice(&XLP_LONG_HEADER.to_le_bytes());
    buf.extend_from_slice(&1u32.to_le_bytes());
    buf.extend_from_slice(&0u64.to_le_bytes());
    buf.extend_from_slice(&0u32.to_le_bytes());
    buf.extend_from_slice(&12345u64.to_le_bytes());
    buf.extend_from_slice(&(seg_size as u32).to_le_bytes());
    buf.extend_from_slice(&8192u32.to_le_bytes());
    buf.extend_from_slice(&[0u8; 4]);

    let mut records = 0u64;
    let mut xid = 1u32;
    let mut block_no = 0u32;
    while buf.len() < seg_size - 80 {
        let rec = build_heap_insert(
            TEST_REL_NODE_BASE + (records as u32 % 16),
            block_no,
            xid,
            &main_data,
        );
        if buf.len() + rec.len() + 8 > seg_size {
            break;
        }
        buf.extend_from_slice(&rec);
        let pad = (8 - (buf.len() % 8)) % 8;
        buf.extend(std::iter::repeat_n(0u8, pad));
        records += 1;
        xid = xid.wrapping_add(1);
        block_no = block_no.wrapping_add(1);
    }
    buf.resize(seg_size, 0);
    (buf, records)
}

#[derive(Default)]
struct CounterSink {
    n: u64,
}

impl RecordSink for CounterSink {
    fn on_record<'a>(
        &'a mut self,
        _r: &'a Record<'a>,
    ) -> Pin<Box<dyn Future<Output = Result<(), SinkError>> + Send + 'a>> {
        Box::pin(async move {
            self.n += 1;
            Ok(())
        })
    }
}

struct OwnedCounterSink {
    counter: Arc<std::sync::atomic::AtomicU64>,
}

impl RecordSink for OwnedCounterSink {
    fn on_record<'a>(
        &'a mut self,
        _r: &'a Record<'a>,
    ) -> Pin<Box<dyn Future<Output = Result<(), SinkError>> + Send + 'a>> {
        let c = self.counter.clone();
        Box::pin(async move {
            c.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            Ok(())
        })
    }
}

async fn run_case(
    label: &str,
    seg: &[u8],
    records: u64,
    iterations: usize,
    record_sink: &mut dyn RecordSink,
    bytes_sink: Option<Box<dyn walshadow::wal_stream::RecordBytesSink + Send>>,
) {
    let mut stream = WalStream::new(1, SEG_SIZE, 0).unwrap();
    if let Some(bs) = bytes_sink {
        stream.set_bytes_sink(bs);
    }
    let mut seg_sink = CollectingSegmentSink::default();

    let start = Instant::now();
    for i in 0..iterations {
        let lsn = (i as u64) * SEG_SIZE;
        stream
            .push(lsn, seg, record_sink, &mut seg_sink)
            .await
            .unwrap();
    }
    let elapsed = start.elapsed();
    let secs = elapsed.as_secs_f64();
    let bytes = (seg.len() as u64 * iterations as u64) as f64;
    let total_records = records * iterations as u64;
    let rec_per_sec = total_records as f64 / secs;
    let mb_per_sec = bytes / secs / 1_048_576.0;
    println!(
        "  {label:30}  {total_records:>9} records  {elapsed:>10.3?}  {rec_per_sec:>11.0} rec/s  {mb_per_sec:>8.1} MiB/s",
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "diagnostic microbenchmark — run with `cargo test --release --test wal_stream_throughput -- --ignored --nocapture`"]
async fn pump_throughput_breakdown() {
    let (seg, records) = build_segment(SEG_SIZE as usize);
    println!(
        "\n synthetic segment: {} bytes, {} records, avg {:.0} bytes/record\n",
        seg.len(),
        records,
        seg.len() as f64 / records as f64,
    );

    // Case 1: baseline — counter sink, no bytes_sink (NoopBytesSink default).
    {
        let mut sink = CounterSink::default();
        run_case(
            "counter + noop bytes_sink",
            &seg,
            records,
            ITERATIONS,
            &mut sink,
            None,
        )
        .await;
        assert_eq!(sink.n, records * ITERATIONS as u64);
    }

    // Case 2: bytes_sink = ShadowStreamSink (lock + frame encode + enqueue).
    //         Counter record sink so we isolate the bytes_sink cost.
    {
        let state = Arc::new(Mutex::new(ShadowStreamState::new(
            1,
            "12345".into(),
            0,
            64 * 1024 * 1024,
        )));
        let mut sink = CounterSink::default();
        let bs: Box<dyn walshadow::wal_stream::RecordBytesSink + Send> =
            Box::new(ShadowStreamSink::new(state.clone()));
        run_case(
            "counter + shadow bytes_sink",
            &seg,
            records,
            ITERATIONS,
            &mut sink,
            Some(bs),
        )
        .await;
    }

    // Case 3: queueing wrapper around the counter sink, no bytes_sink.
    //         Isolates the per-record clone-into-owned + mpsc send cost.
    {
        let counter = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let inner = OwnedCounterSink {
            counter: counter.clone(),
        };
        let mut queueing = QueueingRecordSink::spawn(inner, 256, 16_384);
        run_case(
            "queueing(counter) + noop",
            &seg,
            records,
            ITERATIONS,
            &mut queueing,
            None,
        )
        .await;
        queueing.close().await.unwrap();
        assert_eq!(
            counter.load(std::sync::atomic::Ordering::Relaxed),
            records * ITERATIONS as u64,
        );
    }

    // Case 4: full daemon-shape — bytes_sink + queueing(counter).
    {
        let state = Arc::new(Mutex::new(ShadowStreamState::new(
            1,
            "12345".into(),
            0,
            64 * 1024 * 1024,
        )));
        let counter = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let inner = OwnedCounterSink {
            counter: counter.clone(),
        };
        let mut queueing = QueueingRecordSink::spawn(inner, 256, 16_384);
        let bs: Box<dyn walshadow::wal_stream::RecordBytesSink + Send> =
            Box::new(ShadowStreamSink::new(state.clone()));
        run_case(
            "queueing(counter) + shadow",
            &seg,
            records,
            ITERATIONS,
            &mut queueing,
            Some(bs),
        )
        .await;
        queueing.close().await.unwrap();
        assert_eq!(
            counter.load(std::sync::atomic::Ordering::Relaxed),
            records * ITERATIONS as u64,
        );
    }

    // Case 5: only the basic CountingRecordSink (no async future allocation
    //         beyond what walshadow already wraps).
    {
        let mut sink = CountingRecordSink::default();
        run_case(
            "CountingRecordSink",
            &seg,
            records,
            ITERATIONS,
            &mut sink,
            None,
        )
        .await;
    }

    // Case 6: clone-only — measure the clone-into-owned cost in
    //         isolation, no channel.
    {
        struct CloneOnly {
            store: Vec<Record<'static>>,
        }
        impl RecordSink for CloneOnly {
            fn on_record<'a>(
                &'a mut self,
                r: &'a Record<'a>,
            ) -> Pin<Box<dyn Future<Output = Result<(), SinkError>> + Send + 'a>> {
                Box::pin(async move {
                    let owned = Record {
                        parsed: r.parsed.clone().into_owned(),
                        source_lsn: r.source_lsn,
                        page_magic: r.page_magic,
                        decision: r.decision,
                    };
                    // Push then immediately pop to drop, so we measure
                    // clone+drop without growing memory unboundedly.
                    self.store.push(owned);
                    self.store.pop();
                    Ok(())
                })
            }
        }
        let mut sink = CloneOnly { store: Vec::new() };
        run_case(
            "clone-only (no channel)",
            &seg,
            records,
            ITERATIONS,
            &mut sink,
            None,
        )
        .await;
    }
}
