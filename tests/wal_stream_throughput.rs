//! Microbenchmark for the daemon's per-record pump cost.
//!
//! Generates a synthetic 16 MiB segment full of heap-insert records,
//! pumps it through `WalStream::push` with progressively heavier sink
//! combinations, and prints records-per-second + MB/s for each. Lets
//! us isolate which sink dominates the per-record cost the bootstrap
//! and kill-restart streaming tests trip over.
//!
//! Not a CI assertion (results are hardware-dependent). Run with
//! `cargo test --release --test wal_stream_throughput -- --nocapture`.

// Swap the global allocator to mimalloc: the pipeline allocates rows on the
// decode thread and frees them on the batcher thread, which thrashes glibc's
// arena lock. mimalloc's per-thread caches handle that produce-here/free-there
// pattern far better — this measures its effect on single-row throughput.
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

use std::borrow::Cow;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use clickhouse_c::Allocator;
use tokio::sync::{Mutex, RwLock, mpsc};
use walrus::pg::walparser::{
    BlockLocation, RelFileNode, RmId, X_LOG_RECORD_HEADER_SIZE, XLP_LONG_HEADER,
    XLP_PAGE_MAGIC_PG15, XLR_BLOCK_ID_DATA_LONG, XLogRecord, XLogRecordBlock,
    XLogRecordBlockHeader,
};

use walshadow::ch_ddl::DdlApplicator;
use walshadow::ch_emitter::{ColumnMapping, EmitterStats, MappingHandle, TableMapping};
use walshadow::filter::Route;
use walshadow::heap_decoder::{
    ColumnValue, CommittedTuple, DecodedHeap, DecodedTuple, HeapOp, SIZE_OF_HEAP_INSERT,
    XLOG_HEAP_INSERT,
};
use walshadow::pipeline::batcher::{BatcherConfig, BatcherMsg, InsertBatch, RoutedRow};
use walshadow::pipeline::decode::{self, DecodeCtx, DecodeJob, ToastChunks};
use walshadow::pipeline::reorder::ReorderSink;
use walshadow::pipeline::{Fatal, ack, batcher, mpmc};
use walshadow::queueing_record_sink::QueueingRecordSink;
use walshadow::rewrite::compute_crc;
use walshadow::shadow_catalog::{RelAttr, RelDescriptor, ReplIdent, ShadowCatalog};
use walshadow::shadow_stream::{ShadowStreamSink, ShadowStreamState};
use walshadow::toast::ToastResolver;
use walshadow::wal_stream::{
    CollectingSegmentSink, CountingRecordSink, Record, RecordSink, SinkError, WalStream,
};
use walshadow::xact_buffer::{BufferingDecoderSink, SubxactTracker, XactBuffer, XactBufferConfig};

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
        let mut queueing = QueueingRecordSink::spawn(inner, 256, 16_384, None);
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
        let mut queueing = QueueingRecordSink::spawn(inner, 256, 16_384, None);
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
                        route: r.route,
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

// ---------------------------------------------------------------------------
// Pipeline-tail breakdown (Stage 1): everything *after* WAL decode and
// *before* ClickHouse. Feeds synthetic, already-resolved `RoutedRow`s straight
// at the batcher (or just the ack collector), with a test-local "null
// inserter" that drains each sealed batch and acks it immediately — no CH, no
// network. Isolates the encode + block-build + channel + watermark
// coordination the on-CPU profile flagged (AckState::apply/advance,
// ColumnBuf::append, futex/eventfd wakeup churn). The decode pool and reorder
// stages need an offline catalog and are a follow-up (Stage 2/3).
//
// Run: cargo test --release --test wal_stream_throughput \
//        pipeline_tail_breakdown -- --ignored --nocapture

fn test_rfn() -> RelFileNode {
    RelFileNode {
        spc_node: 1663,
        db_node: 5,
        rel_node: 16385,
    }
}

fn rel_descriptor() -> RelDescriptor {
    RelDescriptor {
        rfn: test_rfn(),
        oid: 16385,
        namespace_oid: 2200,
        namespace_name: "public".into(),
        name: "t".into(),
        qualified_name: RelDescriptor::build_qualified_name("public", "t"),
        kind: 'r',
        persistence: 'p',
        replident: ReplIdent::Default { pk_attnums: None },
        attributes: vec![RelAttr {
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
        }],
    }
}

fn rel_desc() -> Arc<RelDescriptor> {
    Arc::new(rel_descriptor())
}

/// One synthetic single-column INSERT heap (no TOAST, so detoast is a no-op).
fn heap(xid: u32, id: u64) -> DecodedHeap {
    DecodedHeap {
        rfn: test_rfn(),
        xid,
        source_lsn: 0x1000 + id,
        op: HeapOp::Insert,
        new: Some(DecodedTuple {
            columns: vec![Some(ColumnValue::Int4(id as i32))],
            partial: false,
        }),
        old: None,
    }
}

/// A synthetic `XLOG_XACT_COMMIT` record for `xid`. `info = 0` ⇒ no subxacts;
/// the 8-byte `main_data` is the `xact_time`. `ReorderSink` only reads the
/// header rmid/info/xid and this payload.
fn commit_record(xid: u32, commit_lsn: u64) -> Record<'static> {
    let mut parsed = XLogRecord::default();
    parsed.header.resource_manager_id = RmId::Xact as u8;
    parsed.header.info = 0x00; // XLOG_XACT_COMMIT (op bits clear)
    parsed.header.xact_id = xid;
    parsed.main_data = Cow::Owned(0i64.to_le_bytes().to_vec());
    Record {
        parsed,
        source_lsn: commit_lsn,
        page_magic: 0,
        route: Route::default(),
    }
}

/// One synthetic *parsed* `XLOG_HEAP_INSERT` `Record` carrying **real tuple
/// bytes** for the single-`int4` `public.t` schema, so `BufferingDecoderSink`
/// runs the actual `decode_heap_record` byte parse (Stage 4) instead of the
/// `data_length=0` no-op `build_segment` produces. Route `ToDecoder` so the
/// decoder doesn't early-return.
///
/// Block data layout = `xl_heap_header(5) + bitmap/align pad + col data`:
/// `t_infomask2`(natts) `t_infomask`(flags) `t_hoff`, then `t_hoff −
/// SIZE_OF_HEAP_TUPLE_HEADER(23)` pad bytes, then the 4-byte int4. With no
/// nulls the bitmap is empty; `t_hoff=24` (MAXALIGN(8) of the 23-byte header)
/// leaves exactly one pad byte before col data at offset 6.
fn heap_insert_record(xid: u32, source_lsn: u64, id: u64) -> Record<'static> {
    let mut payload = Vec::with_capacity(10);
    payload.extend_from_slice(&1u16.to_le_bytes()); // t_infomask2: natts = 1
    payload.extend_from_slice(&0u16.to_le_bytes()); // t_infomask: no nulls
    payload.push(24); // t_hoff
    payload.push(0); // bitmap/align pad (t_hoff − 23)
    payload.extend_from_slice(&(id as i32).to_le_bytes());

    let mut parsed = XLogRecord::default();
    parsed.header.resource_manager_id = RmId::Heap as u8;
    parsed.header.info = XLOG_HEAP_INSERT;
    parsed.header.xact_id = xid;
    parsed.main_data = Cow::Owned(vec![0u8; SIZE_OF_HEAP_INSERT]);
    parsed.blocks = vec![XLogRecordBlock {
        header: XLogRecordBlockHeader {
            location: BlockLocation {
                rel: test_rfn(),
                block_no: 0,
            },
            ..Default::default()
        },
        data: Cow::Owned(payload),
        ..Default::default()
    }];
    Record {
        parsed,
        source_lsn,
        page_magic: 0,
        route: Route::ToDecoder,
    }
}

fn table_mapping() -> Arc<TableMapping> {
    Arc::new(TableMapping {
        target: "default.t".into(),
        columns: vec![ColumnMapping {
            src_attnum: 1,
            target_name: "id".into(),
            target_type: "Int32".into(),
        }],
    })
}

fn routed_row(
    rel: &Arc<RelDescriptor>,
    mapping: &Arc<TableMapping>,
    seq: u64,
    id: u64,
) -> RoutedRow {
    RoutedRow {
        seq,
        rel: rel.clone(),
        mapping: mapping.clone(),
        committed: CommittedTuple {
            decoded: DecodedHeap {
                rfn: rel.rfn,
                xid: 7,
                source_lsn: 0x1000 + id,
                op: HeapOp::Insert,
                new: Some(DecodedTuple {
                    columns: vec![Some(ColumnValue::Int4(id as i32))],
                    partial: false,
                }),
                old: None,
            },
            commit_ts: 0,
            commit_lsn: (seq + 1) * 100,
        },
    }
}

fn report(label: &str, rows: u64, elapsed: Duration) {
    println!(
        "  {label:34}  {rows:>9} rows  {elapsed:>10.3?}  {:>11.0} rows/s",
        rows as f64 / elapsed.as_secs_f64(),
    );
}

/// Only the ack collector: register → placed → acked per xact, no batcher, no
/// encode. Isolates the watermark machinery + its unbounded channel.
async fn run_ack_only(label: &str, n_xacts: u64, rows_per_xact: u64) {
    let emitter_ack = Arc::new(AtomicU64::new(0));
    let (ack, collector) = ack::spawn(emitter_ack.clone());
    let final_lsn = n_xacts * 100;

    let start = Instant::now();
    for s in 0..n_xacts {
        ack.register(s, (s + 1) * 100);
        ack.placed(s, rows_per_xact);
        ack.acked(vec![(s, rows_per_xact)]);
    }
    while emitter_ack.load(Ordering::Acquire) < final_lsn {
        tokio::task::yield_now().await;
    }
    let elapsed = start.elapsed();
    report(label, n_xacts * rows_per_xact, elapsed);

    drop(ack);
    let _ = collector.await;
}

/// Full tail minus CH: synthetic rows → batcher (encode + block build) → a null
/// inserter that acks each sealed batch immediately → ack collector.
async fn run_tail(label: &str, n_xacts: u64, rows_per_xact: u64) {
    let fatal = Fatal::new();
    let emitter_ack = Arc::new(AtomicU64::new(0));
    let (ack, collector) = ack::spawn(emitter_ack.clone());
    let (batches_tx, batches_rx) = mpmc::channel::<InsertBatch>(8);
    let (msg_tx, msg_rx) = mpsc::channel::<BatcherMsg>(256);
    let stats = Arc::new(EmitterStats::default());
    let cfg = BatcherConfig {
        row_budget: 65_536,
        byte_budget: 1 << 20,
        // Small deadline so partial batches seal during the feed rather than
        // pinning the watermark until the final drop-flush.
        flush_timeout: Duration::from_millis(5),
    };
    let batcher = batcher::spawn(
        msg_rx,
        batches_tx,
        cfg,
        Allocator::stdlib(),
        fatal.clone(),
        stats.clone(),
    );

    // Null inserter: drain sealed batches and ack as if durable (no CH).
    let ack_ni = ack.clone();
    let null_inserter = tokio::spawn(async move {
        while let Some(batch) = batches_rx.recv().await {
            ack_ni.acked(batch.per_seq);
        }
    });

    let rel = rel_desc();
    let mapping = table_mapping();
    let final_lsn = n_xacts * 100;

    let start = Instant::now();
    for s in 0..n_xacts {
        ack.register(s, (s + 1) * 100);
        let rows: Vec<RoutedRow> = (0..rows_per_xact)
            .map(|i| routed_row(&rel, &mapping, s, i))
            .collect();
        msg_tx.send(BatcherMsg::Rows(rows)).await.unwrap();
        ack.placed(s, rows_per_xact);
    }
    // Seal whatever is still buffered, then wait for the watermark to cover all.
    drop(msg_tx);
    while emitter_ack.load(Ordering::Acquire) < final_lsn {
        tokio::task::yield_now().await;
    }
    let elapsed = start.elapsed();
    report(label, n_xacts * rows_per_xact, elapsed);

    let _ = batcher.await;
    let _ = null_inserter.await;
    drop(ack);
    let _ = collector.await;
    assert!(fatal.message().is_none(), "fatal: {:?}", fatal.message());
}

/// Adds the decode pool: feed `DecodeJob`s through `decode::spawn_pool`
/// (`decode_and_route`: detoast + catalog resolve + mapping + `RoutedRow`) → the
/// same batcher + null inserter + ack. `m` = decode workers. This driver plays
/// reorder's role (assign seq + `register`); the pool calls `placed`.
async fn run_decode_pool(label: &str, n_xacts: u64, rows_per_xact: u64, m: usize) {
    let fatal = Fatal::new();
    let emitter_ack = Arc::new(AtomicU64::new(0));
    let (ack, collector) = ack::spawn(emitter_ack.clone());
    let (batches_tx, batches_rx) = mpmc::channel::<InsertBatch>(8);
    let (msg_tx, msg_rx) = mpsc::channel::<BatcherMsg>(256);
    let stats = Arc::new(EmitterStats::default());
    let cfg = BatcherConfig {
        row_budget: 65_536,
        byte_budget: 1 << 20,
        flush_timeout: Duration::from_millis(5),
    };
    let batcher = batcher::spawn(
        msg_rx,
        batches_tx,
        cfg,
        Allocator::stdlib(),
        fatal.clone(),
        stats.clone(),
    );

    let ack_ni = ack.clone();
    let null_inserter = tokio::spawn(async move {
        while let Some(batch) = batches_rx.recv().await {
            ack_ni.acked(batch.per_seq);
        }
    });

    // Offline catalog + mapping so decode_and_route resolves without a PG.
    let catalog = Arc::new(Mutex::new(ShadowCatalog::seeded_for_test(
        vec![rel_descriptor()],
        u64::MAX,
    )));
    let mut tables = HashMap::new();
    tables.insert("public.t".to_string(), (*table_mapping()).clone());
    let mapping: MappingHandle = Arc::new(RwLock::new(tables));

    let (jobs_tx, jobs_rx) = mpmc::channel::<DecodeJob>((m * 4).max(8));
    // `msg_tx` moves into ctx; spawn_pool clones it per worker and drops the
    // original, so the batcher closes once the decoders drain and exit.
    let ctx = DecodeCtx {
        catalog,
        mapping,
        oracle: None,
        msg_tx,
        stats: stats.clone(),
        // No-TOAST bench: single int4 col never detoasts.
        resolver: ToastResolver::disabled(),
    };
    let decoders = decode::spawn_pool(m, ctx, jobs_rx, ack.clone(), fatal.clone());

    let final_lsn = n_xacts * 100;
    let empty_chunks: Arc<ToastChunks> = Arc::new(ToastChunks::new());
    let start = Instant::now();
    for s in 0..n_xacts {
        ack.register(s, (s + 1) * 100);
        let heaps: Vec<DecodedHeap> = (0..rows_per_xact).map(|i| heap(7, i)).collect();
        let job = DecodeJob {
            seq: s,
            commit_ts: 0,
            commit_lsn: (s + 1) * 100,
            heaps,
            chunks: empty_chunks.clone(),
        };
        if jobs_tx.send(job).await.is_err() {
            panic!(
                "decode job queue closed early (fatal: {:?})",
                fatal.message()
            );
        }
        // the pool calls ack.placed(seq, rows) after routing.
    }
    // Decoders drain + exit + drop their msg_tx clones → batcher closes/flushes.
    drop(jobs_tx);
    while emitter_ack.load(Ordering::Acquire) < final_lsn {
        tokio::task::yield_now().await;
    }
    let elapsed = start.elapsed();
    report(label, n_xacts * rows_per_xact, elapsed);

    for h in decoders {
        let _ = h.await;
    }
    let _ = batcher.await;
    let _ = null_inserter.await;
    drop(ack);
    let _ = collector.await;
    assert!(fatal.message().is_none(), "fatal: {:?}", fatal.message());
}

/// Adds reorder: pre-buffer each xact's heaps into an `XactBuffer`, then drive
/// `ReorderSink` with a synthetic COMMIT → it assigns a seq, registers, drains
/// the buffer, and dispatches to the Stage-2 decode pool → batcher → null
/// inserter → ack. Adds the single-threaded commit-order coordination.
async fn run_reorder(label: &str, n_xacts: u64, rows_per_xact: u64, m: usize) {
    let fatal = Fatal::new();
    let emitter_ack = Arc::new(AtomicU64::new(0));
    let (ack, collector) = ack::spawn(emitter_ack.clone());
    let (batches_tx, batches_rx) = mpmc::channel::<InsertBatch>(8);
    let (msg_tx, msg_rx) = mpsc::channel::<BatcherMsg>(256);
    let stats = Arc::new(EmitterStats::default());
    let cfg = BatcherConfig {
        row_budget: 65_536,
        byte_budget: 1 << 20,
        flush_timeout: Duration::from_millis(5),
    };
    let batcher = batcher::spawn(
        msg_rx,
        batches_tx,
        cfg,
        Allocator::stdlib(),
        fatal.clone(),
        stats.clone(),
    );

    let ack_ni = ack.clone();
    let null_inserter = tokio::spawn(async move {
        while let Some(batch) = batches_rx.recv().await {
            ack_ni.acked(batch.per_seq);
        }
    });

    let catalog = Arc::new(Mutex::new(ShadowCatalog::seeded_for_test(
        vec![rel_descriptor()],
        u64::MAX,
    )));
    let mut tables = HashMap::new();
    tables.insert("public.t".to_string(), (*table_mapping()).clone());
    let mapping: MappingHandle = Arc::new(RwLock::new(tables));

    let (jobs_tx, jobs_rx) = mpmc::channel::<DecodeJob>((m * 4).max(8));
    let ctx = DecodeCtx {
        catalog: catalog.clone(),
        mapping: mapping.clone(),
        oracle: None,
        msg_tx: msg_tx.clone(),
        stats: stats.clone(),
        // No-TOAST bench: single int4 col never detoasts.
        resolver: ToastResolver::disabled(),
    };
    let decoders = decode::spawn_pool(m, ctx, jobs_rx, ack.clone(), fatal.clone());

    let spill = tempfile::tempdir().expect("tempdir");
    let buffer = Arc::new(Mutex::new(
        XactBuffer::new(XactBufferConfig::new(spill.path().to_path_buf())).expect("xact buffer"),
    ));
    let subxact = Arc::new(Mutex::new(SubxactTracker::new()));
    // `msg_tx` moves into reorder (barrier flush); `jobs_tx` too. Dropping the
    // ReorderSink closes both, cascading the drain.
    let mut reorder = ReorderSink::new(
        buffer.clone(),
        catalog,
        subxact,
        None,
        None,
        DdlApplicator::offline_for_test(mapping.clone()),
        ack.clone(),
        jobs_tx,
        msg_tx,
        stats.clone(),
        ToastResolver::disabled(),
        fatal.clone(),
        None,
    );

    let final_lsn = n_xacts * 100;
    let start = Instant::now();
    for s in 0..n_xacts {
        let xid = (s as u32).wrapping_add(1); // avoid xid 0
        {
            let mut buf = buffer.lock().await;
            for i in 0..rows_per_xact {
                buf.on_heap(heap(xid, i)).await.expect("on_heap");
            }
        }
        let rec = commit_record(xid, (s + 1) * 100);
        reorder.on_record(&rec).await.expect("reorder on_record");
    }
    drop(reorder);
    while emitter_ack.load(Ordering::Acquire) < final_lsn {
        tokio::task::yield_now().await;
    }
    let elapsed = start.elapsed();
    report(label, n_xacts * rows_per_xact, elapsed);

    for h in decoders {
        let _ = h.await;
    }
    let _ = batcher.await;
    let _ = null_inserter.await;
    drop(ack);
    let _ = collector.await;
    assert!(fatal.message().is_none(), "fatal: {:?}", fatal.message());
}

/// Stage 4's queueing-worker composite, mirroring `DecoderXactPair` in
/// `src/bin/stream.rs`: the decoder absorbs each heap into the xact buffer,
/// then the reorder drain flushes on COMMIT. Order matters — a heap and its
/// COMMIT can share a dispatch, so the heap must be buffered first. Fanning
/// every record to both is safe: the decoder skips non-`ToDecoder`/non-heap
/// records, the reorder skips non-`Xact` records.
struct DecodeReorderPair {
    decoder: BufferingDecoderSink,
    reorder: ReorderSink,
}

impl RecordSink for DecodeReorderPair {
    fn on_record<'a>(
        &'a mut self,
        record: &'a Record<'a>,
    ) -> Pin<Box<dyn Future<Output = Result<(), SinkError>> + Send + 'a>> {
        Box::pin(async move {
            self.decoder.on_record(record).await?;
            self.reorder.on_record(record).await?;
            Ok(())
        })
    }
}

/// Adds WAL heap decode (the front stage): drive `BufferingDecoderSink` with
/// synthetic *parsed* heap-insert `Record`s carrying real tuple bytes →
/// `decode_heap_record` + buffer absorb run inline on this (single) caller, the
/// production queueing worker's serialization point. The matching COMMIT then
/// drives `ReorderSink` → Stage-2 decode pool → batcher → null inserter → ack.
/// This is Stage 3 plus the actual tuple byte parse, so the delta isolates the
/// `decode_heap_record` cost the on-CPU profile couldn't see.
///
/// Driven directly (no `QueueingRecordSink`) to stay comparable with Stage 3's
/// direct-drive `run_reorder`; the queueing channel/clone tax is Stage 0.
async fn run_wal_decode(label: &str, n_xacts: u64, rows_per_xact: u64, m: usize) {
    let fatal = Fatal::new();
    let emitter_ack = Arc::new(AtomicU64::new(0));
    let (ack, collector) = ack::spawn(emitter_ack.clone());
    let (batches_tx, batches_rx) = mpmc::channel::<InsertBatch>(8);
    let (msg_tx, msg_rx) = mpsc::channel::<BatcherMsg>(256);
    let stats = Arc::new(EmitterStats::default());
    let cfg = BatcherConfig {
        row_budget: 65_536,
        byte_budget: 1 << 20,
        flush_timeout: Duration::from_millis(5),
    };
    let batcher = batcher::spawn(
        msg_rx,
        batches_tx,
        cfg,
        Allocator::stdlib(),
        fatal.clone(),
        stats.clone(),
    );

    let ack_ni = ack.clone();
    let null_inserter = tokio::spawn(async move {
        while let Some(batch) = batches_rx.recv().await {
            ack_ni.acked(batch.per_seq);
        }
    });

    let catalog = Arc::new(Mutex::new(ShadowCatalog::seeded_for_test(
        vec![rel_descriptor()],
        u64::MAX,
    )));
    let mut tables = HashMap::new();
    tables.insert("public.t".to_string(), (*table_mapping()).clone());
    let mapping: MappingHandle = Arc::new(RwLock::new(tables));

    let (jobs_tx, jobs_rx) = mpmc::channel::<DecodeJob>((m * 4).max(8));
    let ctx = DecodeCtx {
        catalog: catalog.clone(),
        mapping: mapping.clone(),
        oracle: None,
        msg_tx: msg_tx.clone(),
        stats: stats.clone(),
        // No-TOAST bench: single int4 col never detoasts.
        resolver: ToastResolver::disabled(),
    };
    let decoders = decode::spawn_pool(m, ctx, jobs_rx, ack.clone(), fatal.clone());

    let spill = tempfile::tempdir().expect("tempdir");
    let buffer = Arc::new(Mutex::new(
        XactBuffer::new(XactBufferConfig::new(spill.path().to_path_buf())).expect("xact buffer"),
    ));
    let subxact = Arc::new(Mutex::new(SubxactTracker::new()));
    let reorder = ReorderSink::new(
        buffer.clone(),
        catalog.clone(),
        subxact,
        None,
        None,
        DdlApplicator::offline_for_test(mapping.clone()),
        ack.clone(),
        jobs_tx,
        msg_tx,
        stats.clone(),
        ToastResolver::disabled(),
        fatal.clone(),
        None,
    );
    // `None` schema_events keeps the decoder schema-unaware (greenfield/test).
    let decoder = BufferingDecoderSink::new(catalog, buffer);
    let mut pair = DecodeReorderPair { decoder, reorder };

    let final_lsn = n_xacts * 100;
    let start = Instant::now();
    for s in 0..n_xacts {
        let xid = (s as u32).wrapping_add(1); // avoid xid 0
        for i in 0..rows_per_xact {
            let rec = heap_insert_record(xid, 0x1000 + i, i);
            pair.on_record(&rec).await.expect("decode on_record");
        }
        let commit = commit_record(xid, (s + 1) * 100);
        pair.on_record(&commit).await.expect("reorder on_record");
    }
    // Dropping the pair drops the ReorderSink → closes jobs_tx + msg_tx →
    // cascades the drain through the pool, batcher, and null inserter.
    drop(pair);
    while emitter_ack.load(Ordering::Acquire) < final_lsn {
        tokio::task::yield_now().await;
    }
    let elapsed = start.elapsed();
    report(label, n_xacts * rows_per_xact, elapsed);

    for h in decoders {
        let _ = h.await;
    }
    let _ = batcher.await;
    let _ = null_inserter.await;
    drop(ack);
    let _ = collector.await;
    assert!(fatal.message().is_none(), "fatal: {:?}", fatal.message());
}

/// Row count for the isolated profiling targets; override for longer runs
/// (more profiler samples), e.g. `BENCH_ROWS=20000000`.
fn bench_rows() -> u64 {
    std::env::var("BENCH_ROWS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1_000_000)
}

/// Isolated decode-pool target (M=1, 1 row/xact) for profiling. Runs only the
/// decode pool + tail with a null inserter — the single-row decode hotpath.
/// Run alone so a profiler attaches to just this:
///   cargo test --release --test wal_stream_throughput decode_pool_1row_m1 \
///     -- --ignored --nocapture --test-threads=1
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "profiling target"]
async fn decode_pool_1row_m1() {
    run_decode_pool("decode M=1: 1 row/xact", bench_rows(), 1, 1).await;
}

/// Isolated decode-pool target (M=4, 1 row/xact) — does the pool scale for
/// single-row, or is it pinned by the shared catalog mutex?
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "profiling target"]
async fn decode_pool_1row_m4() {
    run_decode_pool("decode M=4: 1 row/xact", bench_rows(), 1, 4).await;
}

/// Isolated Stage-4 target (M=1, 1 row/xact): the full front-stage WAL heap
/// decode (`decode_heap_record` + buffer absorb + reorder dispatch) running
/// inline on the single caller, plus the tail. Profile alone:
///   cargo test --release --test wal_stream_throughput wal_decode_1row_m1 \
///     -- --ignored --nocapture --test-threads=1
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "profiling target"]
async fn wal_decode_1row_m1() {
    run_wal_decode("wal-decode M=1: 1 row/xact", bench_rows(), 1, 1).await;
}

/// Isolated Stage-4 target (M=4, 1 row/xact) — does adding decode workers lift
/// the front-stage ceiling, or does the single-threaded heap decode pin it?
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "profiling target"]
async fn wal_decode_1row_m4() {
    run_wal_decode("wal-decode M=4: 1 row/xact", bench_rows(), 1, 4).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "diagnostic microbenchmark — run with `cargo test --release --test wal_stream_throughput pipeline_tail_breakdown -- --ignored --nocapture`"]
async fn pipeline_tail_breakdown() {
    const ROWS: u64 = 1_000_000;
    println!("\n pipeline tail (decode-pool output → CH), {ROWS} rows, null inserter (no CH)\n");

    // Watermark machinery alone — the per-xact register/placed/acked cost.
    run_ack_only("ack only: 1 row/xact", ROWS, 1).await;
    run_ack_only("ack only: 1000 rows/xact", ROWS / 1000, 1000).await;

    // + batcher encode + block build + the two channel hops + null inserter.
    run_tail("batcher+ack: 1 row/xact", ROWS, 1).await;
    run_tail("batcher+ack: 100 rows/xact", ROWS / 100, 100).await;
    run_tail("batcher+ack: 1000 rows/xact", ROWS / 1000, 1000).await;

    // + decode pool (detoast + catalog resolve + mapping + RoutedRow build).
    run_decode_pool("decode+tail M=1: 1 row/xact", ROWS, 1, 1).await;
    run_decode_pool("decode+tail M=4: 1 row/xact", ROWS, 1, 4).await;
    run_decode_pool("decode+tail M=1: 1000 rows/xact", ROWS / 1000, 1000, 1).await;
    run_decode_pool("decode+tail M=4: 1000 rows/xact", ROWS / 1000, 1000, 4).await;

    // + reorder (commit-order coordinator: seq + register + drain + dispatch).
    run_reorder("reorder+tail M=1: 1 row/xact", ROWS, 1, 1).await;
    run_reorder("reorder+tail M=1: 1000 rows/xact", ROWS / 1000, 1000, 1).await;
    run_reorder("reorder+tail M=4: 1000 rows/xact", ROWS / 1000, 1000, 4).await;

    // + WAL heap decode (BufferingDecoderSink: decode_heap_record + buffer
    //   absorb on the single worker — the front stage). The whole pipeline
    //   minus the pump's WAL byte parse and minus CH.
    run_wal_decode("wal-decode+tail M=1: 1 row/xact", ROWS, 1, 1).await;
    run_wal_decode("wal-decode+tail M=1: 1000 rows/xact", ROWS / 1000, 1000, 1).await;
    run_wal_decode("wal-decode+tail M=4: 1000 rows/xact", ROWS / 1000, 1000, 4).await;
}
