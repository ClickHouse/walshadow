//! Streaming filter pipeline. Wraps [`StreamingWalker`] in a
//! segment-aligned accumulator consuming WAL byte chunks from wal-rus's
//! `START_REPLICATION` CopyData stream, dispatching to per-record +
//! per-segment sinks at record cadence.
//!
//! ```text
//!   wal-rus CopyData('w') chunks
//!              v
//!     +-----------------+
//!     |  WalStream::push|  base_lsn aligned to segment boundary
//!     +--------+--------+  extends StreamingWalker buffer
//!              | per record completing
//!              v
//!         Filter::decide
//!         /     |        \
//!        v      v         v
//!   noop_replace (ToDecoder)  rewrite_record  manifest
//!              |              (in place)      entry
//!              v
//!         RecordBytesSink  (shadow wire — §3)
//!              v
//!         RecordSink       (BufferingDecoderSink, MetricsRecordSink, ...)
//!              |
//!              + segment fills (16 MiB accumulated)
//!              v
//!         SegmentSink       (DirSegmentSink — archive fallback + retention)
//! ```
//!
//! Per-record dispatch fires the moment a record's last byte arrives;
//! segment-level dispatch fires on segment boundary with already-filtered
//! bytes + accumulated manifest. No re-parse, no second walk.

use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;

use thiserror::Error;
use tokio::sync::mpsc;
use walrus::pg::wal::segment::SegmentName;
use walrus::pg::walparser::{ParseError, XLogRecord, parse_record_from_bytes};

use crate::classify::rmgr_label;
use crate::filter::{Filter, FilterStats, Route};
use crate::filter_segment::{FilterSegmentError, ParsedRecord, filter_segment};
use crate::manifest::{Entry, FILTER_VERSION, Kind, Manifest, ManifestStats};
use crate::rewrite::{RewriteError, noop_replace};
use crate::streaming_walker::{CompletedRecord, StreamingWalker, WalkError};

/// 16 MiB initdb default. Non-default seg sizes need operator
/// coordination: shadow's `initdb --wal-segsize` must match.
pub const WAL_SEG_SIZE: u64 = walrus::pg::wal::segment::DEFAULT_WAL_SEG_SIZE;

#[derive(Debug, Error)]
pub enum WalStreamError {
    #[error("filter segment {seg}: {source}")]
    Filter {
        seg: String,
        #[source]
        source: FilterSegmentError,
    },
    #[error("walk segment {seg}: {source}")]
    Walk {
        seg: String,
        #[source]
        source: WalkError,
    },
    #[error("parse record at offset {offset}: {source}")]
    Parse {
        offset: usize,
        #[source]
        source: ParseError,
    },
    #[error("rewrite record at offset {offset}: {source}")]
    Rewrite {
        offset: usize,
        #[source]
        source: RewriteError,
    },
    #[error("misaligned push: expected lsn {expected:#X}, got {got:#X}")]
    Misaligned { expected: u64, got: u64 },
    #[error("base lsn {0:#X} not segment-aligned")]
    UnalignedBase(u64),
    #[error("sink: {0}")]
    Sink(#[from] SinkError),
    #[error("stream poisoned by prior error; create a fresh WalStream to resume")]
    Poisoned,
}

#[derive(Debug, Error)]
pub enum SinkError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("serialize manifest: {0}")]
    Manifest(#[from] serde_json::Error),
    #[error("{0}")]
    Other(String),
}

/// Per-record hand-off. Heap-tuple decoder reads `parsed.header.xact_id`,
/// `parsed.blocks[i].header.location.rel`, `parsed.main_data`;
/// `page_magic` selects PG-15-vs-PG-14 FPI bit semantics.
///
/// `'a` ties `parsed`'s borrows to walker's segment buffer so the
/// streaming path dispatches without per-record byte copies. Sinks
/// storing records across `try_next` iterations call
/// `record.parsed.clone().into_owned()` to bump to `Record<'static>`.
#[derive(Debug, Clone, Default)]
pub struct Record<'a> {
    pub parsed: XLogRecord<'a>,
    pub source_lsn: u64,
    pub page_magic: u16,
    pub route: Route,
}

impl Record<'static> {
    /// Index alignment between `parsed_records` and `manifest.records`
    /// is the `filter_segment` contract.
    pub fn from_parsed(
        seg_start_lsn: u64,
        parsed: ParsedRecord,
        entry: &crate::manifest::Entry,
    ) -> Self {
        let route = match entry.kind {
            Kind::Kept => Route::ToShadow,
            Kind::Dropped => Route::ToDecoder,
        };
        Self {
            parsed: parsed.record,
            source_lsn: seg_start_lsn + entry.offset,
            page_magic: parsed.page_magic,
            route,
        }
    }
}

/// Observes every record decided by the filter; heap-tuple decoder
/// attaches here.
///
/// `record` borrows back into the walker's segment buffer; sinks
/// consume slices for the future's duration. Sinks that must store
/// records (eg test collectors) call `record.parsed.clone().into_owned()`
/// first.
pub trait RecordSink {
    fn on_record<'a>(
        &'a mut self,
        record: &'a Record<'a>,
    ) -> Pin<Box<dyn Future<Output = Result<(), SinkError>> + Send + 'a>>;

    /// Owned variant of [`Self::on_record`]: the queueing worker already holds
    /// each record `'static`, so a sink that forwards it onto another channel
    /// (the xid-shard router) can **move** it here instead of deep-cloning.
    /// Default borrows the owned record and delegates to `on_record`, so
    /// inline-consuming sinks pay nothing.
    fn on_record_owned<'a>(
        &'a mut self,
        record: Record<'static>,
    ) -> Pin<Box<dyn Future<Output = Result<(), SinkError>> + Send + 'a>>
    where
        Self: Send,
    {
        Box::pin(async move { self.on_record(&record).await })
    }

    /// Driver tick for time-based work that can't wait on the next
    /// record. CH emitter's hold-INSERT-open path gates close-and-ack
    /// on `flush_timeout`; without this an idle stream sits on rows
    /// indefinitely. Fired by
    /// [`crate::queueing_record_sink::QueueingRecordSink`]'s worker on
    /// receive-loop timeout. Default: no-op.
    fn on_idle<'a>(
        &'a mut self,
    ) -> Pin<Box<dyn Future<Output = Result<(), SinkError>> + Send + 'a>> {
        Box::pin(async { Ok(()) })
    }

    /// Final hook before drop, fired by the queueing worker after its
    /// channel closes. Lets CH emitter force-close hold-INSERT-open
    /// buffers regardless of deadline; without this the last flush
    /// window's rows stay non-durable on graceful shutdown. Default: no-op.
    fn on_close<'a>(
        &'a mut self,
    ) -> Pin<Box<dyn Future<Output = Result<(), SinkError>> + Send + 'a>> {
        Box::pin(async { Ok(()) })
    }

    /// Post-batch nudge from the queueing worker: every record with
    /// `source_lsn <= lsn` already dispatched through `on_record`. Xact
    /// buffer advances `emitter_ack_lsn` past trailing non-commit WAL
    /// (checkpoint, RUNNING_XACTS, post-COMMIT page-padding) when no
    /// xact in flight. Without it source's slot pins WAL at the last
    /// COMMIT and the kill-restart drill's post-catchup idle never
    /// resolves. Default: no-op.
    fn on_idle_advance<'a>(
        &'a mut self,
        _lsn: u64,
    ) -> Pin<Box<dyn Future<Output = Result<(), SinkError>> + Send + 'a>> {
        Box::pin(async { Ok(()) })
    }
}

/// Streaming sink for the post-filter WAL byte stream.
/// [`crate::shadow_stream::ShadowStreamSink`] pushes these bytes onto
/// each shadow connection's `'w'` send buffer at record cadence;
/// shadow's startup state machine needs both record bytes and the page
/// headers between them to parse the WAL.
///
/// `on_wire_chunk` fires after every finalized record. Returning `Err`
/// poisons the stream.
pub trait RecordBytesSink: Send {
    fn on_wire_chunk<'a>(
        &'a mut self,
        start_lsn: u64,
        bytes: &'a [u8],
    ) -> Pin<Box<dyn Future<Output = Result<(), SinkError>> + Send + 'a>>;

    /// Fires at segment finalization with trailing bytes (post-last-record
    /// padding / zero tail) + segment-end LSN so the sink advertises an
    /// accurate `server_wal_end` in keepalives.
    fn on_segment_boundary<'a>(
        &'a mut self,
        _start_lsn: u64,
        _trailing_bytes: &'a [u8],
    ) -> Pin<Box<dyn Future<Output = Result<(), SinkError>> + Send + 'a>> {
        Box::pin(async { Ok(()) })
    }
}

/// Receives one fully-filtered segment at a time. Shadow PG consumes
/// filtered segments via `restore_command`; production sink writes
/// bytes + manifest sidecar to that directory.
pub trait SegmentSink {
    fn on_segment<'a>(
        &'a mut self,
        seg: SegmentName,
        bytes: &'a [u8],
        manifest: &'a Manifest,
    ) -> Pin<Box<dyn Future<Output = Result<(), SinkError>> + Send + 'a>>;

    /// Receives a partial segment flushed at shutdown by
    /// [`WalStream::close`]. Default forwards to `on_segment`; production
    /// [`DirSegmentSink`] lands bytes under `<name>.partial` per
    /// `pg_receivewal` convention so a follow-up run doesn't mistake a
    /// partial for a complete segment.
    fn on_partial_segment<'a>(
        &'a mut self,
        seg: SegmentName,
        bytes: &'a [u8],
        manifest: &'a Manifest,
    ) -> Pin<Box<dyn Future<Output = Result<(), SinkError>> + Send + 'a>> {
        self.on_segment(seg, bytes, manifest)
    }
}

/// In-memory `RecordSink` for tests. Materialises records to `'static`
/// via [`XLogRecord::into_owned`] so the walker-buffer borrow doesn't
/// outlive the `on_record` future.
#[derive(Debug, Default)]
pub struct CollectingRecordSink {
    pub records: Vec<Record<'static>>,
}

impl RecordSink for CollectingRecordSink {
    fn on_record<'a>(
        &'a mut self,
        record: &'a Record<'a>,
    ) -> Pin<Box<dyn Future<Output = Result<(), SinkError>> + Send + 'a>> {
        Box::pin(async move {
            self.records.push(Record {
                parsed: record.parsed.clone().into_owned(),
                source_lsn: record.source_lsn,
                page_magic: record.page_magic,
                route: record.route,
            });
            Ok(())
        })
    }
}

#[derive(Debug, Default)]
pub struct CountingRecordSink {
    pub count: u64,
}

impl RecordSink for CountingRecordSink {
    fn on_record<'a>(
        &'a mut self,
        _record: &'a Record<'a>,
    ) -> Pin<Box<dyn Future<Output = Result<(), SinkError>> + Send + 'a>> {
        Box::pin(async move {
            self.count += 1;
            Ok(())
        })
    }
}

/// Per-`(rmid, route)` counter; discards the record.
#[derive(Debug, Default)]
pub struct MetricsRecordSink {
    pub by_rm_route: BTreeMap<(u8, Route), u64>,
    pub total: u64,
}

impl MetricsRecordSink {
    pub fn summary(&self) -> String {
        use std::fmt::Write as _;
        let mut s = String::new();
        write!(&mut s, "total={}", self.total).unwrap();
        for ((rm, route), n) in &self.by_rm_route {
            let r = match route {
                Route::ToShadow => "to_shadow",
                Route::ToDecoder => "to_decoder",
            };
            write!(&mut s, " {}/{}={}", rmgr_label(*rm), r, n).unwrap();
        }
        s
    }
}

impl RecordSink for MetricsRecordSink {
    fn on_record<'a>(
        &'a mut self,
        record: &'a Record<'a>,
    ) -> Pin<Box<dyn Future<Output = Result<(), SinkError>> + Send + 'a>> {
        Box::pin(async move {
            let rm = record.parsed.header.resource_manager_id;
            *self.by_rm_route.entry((rm, record.route)).or_insert(0) += 1;
            self.total += 1;
            Ok(())
        })
    }
}

/// Fan-out `RecordSink`: dispatches in `inner` order, short-circuits on
/// first `Err`.
pub struct CompositeRecordSink {
    pub inner: Vec<Box<dyn RecordSink + Send>>,
}

impl CompositeRecordSink {
    pub fn new(inner: Vec<Box<dyn RecordSink + Send>>) -> Self {
        Self { inner }
    }
}

impl RecordSink for CompositeRecordSink {
    fn on_record<'a>(
        &'a mut self,
        record: &'a Record<'a>,
    ) -> Pin<Box<dyn Future<Output = Result<(), SinkError>> + Send + 'a>> {
        Box::pin(async move {
            for s in &mut self.inner {
                s.on_record(record).await?;
            }
            Ok(())
        })
    }
}

/// In-memory `SegmentSink` for tests + smoke fixtures.
#[derive(Debug, Default)]
pub struct CollectingSegmentSink {
    pub segments: Vec<(SegmentName, Vec<u8>, Manifest)>,
}

impl SegmentSink for CollectingSegmentSink {
    fn on_segment<'a>(
        &'a mut self,
        seg: SegmentName,
        bytes: &'a [u8],
        manifest: &'a Manifest,
    ) -> Pin<Box<dyn Future<Output = Result<(), SinkError>> + Send + 'a>> {
        Box::pin(async move {
            self.segments.push((seg, bytes.to_vec(), manifest.clone()));
            Ok(())
        })
    }
}

/// In-memory `RecordBytesSink` for tests.
#[derive(Debug, Default)]
pub struct CollectingBytesSink {
    pub chunks: Vec<(u64, Vec<u8>)>,
    pub segment_boundaries: Vec<(u64, Vec<u8>)>,
}

impl RecordBytesSink for CollectingBytesSink {
    fn on_wire_chunk<'a>(
        &'a mut self,
        start_lsn: u64,
        bytes: &'a [u8],
    ) -> Pin<Box<dyn Future<Output = Result<(), SinkError>> + Send + 'a>> {
        Box::pin(async move {
            self.chunks.push((start_lsn, bytes.to_vec()));
            Ok(())
        })
    }

    fn on_segment_boundary<'a>(
        &'a mut self,
        start_lsn: u64,
        trailing_bytes: &'a [u8],
    ) -> Pin<Box<dyn Future<Output = Result<(), SinkError>> + Send + 'a>> {
        Box::pin(async move {
            self.segment_boundaries
                .push((start_lsn, trailing_bytes.to_vec()));
            Ok(())
        })
    }
}

/// No-op default; avoids an `Option` branch per record in `WalStream::push`.
#[derive(Debug, Default)]
pub struct NoopBytesSink;

impl RecordBytesSink for NoopBytesSink {
    fn on_wire_chunk<'a>(
        &'a mut self,
        _start_lsn: u64,
        _bytes: &'a [u8],
    ) -> Pin<Box<dyn Future<Output = Result<(), SinkError>> + Send + 'a>> {
        Box::pin(async { Ok(()) })
    }
}

/// A written-and-renamed segment (+ its manifest) awaiting fsync. Handed to
/// the background durability task; `end_lsn` is one past the bytes covered.
pub struct SegFsync {
    pub end_lsn: u64,
    pub seg_path: std::path::PathBuf,
    pub mani_path: std::path::PathBuf,
}

enum Durability {
    /// fsync (`sync_all` + dir fsync) inline — durable before `on_segment`
    /// returns. Used by tests and any synchronous-durability caller.
    Inline,
    /// Write + rename inline, fsync off the hot path: hand each segment to the
    /// background task draining `tx`. The caller advances its durable
    /// watermark from that task, not from this sink.
    Background {
        seg_size: u64,
        tx: mpsc::Sender<SegFsync>,
    },
}

/// Writes filtered segments + manifests to a directory shadow PG's
/// `restore_command` reads from.
pub struct DirSegmentSink {
    out_dir: std::path::PathBuf,
    durability: Durability,
}

impl DirSegmentSink {
    /// Inline-fsync sink: each segment is durable before `on_segment` returns.
    pub fn new(out_dir: std::path::PathBuf) -> Result<Self, SinkError> {
        std::fs::create_dir_all(&out_dir)?;
        Ok(Self {
            out_dir,
            durability: Durability::Inline,
        })
    }

    /// Off-hot-path sink: write + rename inline, fsync in the background task
    /// draining `tx`. The pump never blocks on fsync.
    pub fn with_durability(
        out_dir: std::path::PathBuf,
        seg_size: u64,
        tx: mpsc::Sender<SegFsync>,
    ) -> Result<Self, SinkError> {
        std::fs::create_dir_all(&out_dir)?;
        Ok(Self {
            out_dir,
            durability: Durability::Background { seg_size, tx },
        })
    }
}

impl SegmentSink for DirSegmentSink {
    fn on_segment<'a>(
        &'a mut self,
        seg: SegmentName,
        bytes: &'a [u8],
        manifest: &'a Manifest,
    ) -> Pin<Box<dyn Future<Output = Result<(), SinkError>> + Send + 'a>> {
        Box::pin(async move {
            let inline = matches!(self.durability, Durability::Inline);
            let name = seg.format();
            let seg_path = self.out_dir.join(&name);
            let tmp = seg_path.with_extension("partial");
            write_sync_rename(&tmp, &seg_path, bytes, inline).await?;
            let mani_path = self.out_dir.join(format!("{name}.manifest.json"));
            let mani_tmp = mani_path.with_extension("manifest.json.partial");
            let body = serde_json::to_vec_pretty(manifest)?;
            write_sync_rename(&mani_tmp, &mani_path, &body, inline).await?;
            self.durably(&seg, bytes.len(), seg_path, mani_path).await
        })
    }

    fn on_partial_segment<'a>(
        &'a mut self,
        seg: SegmentName,
        bytes: &'a [u8],
        manifest: &'a Manifest,
    ) -> Pin<Box<dyn Future<Output = Result<(), SinkError>> + Send + 'a>> {
        Box::pin(async move {
            let inline = matches!(self.durability, Durability::Inline);
            let name = seg.format();
            let partial_path = self.out_dir.join(format!("{name}.partial"));
            let tmp = self.out_dir.join(format!("{name}.partial.tmp"));
            write_sync_rename(&tmp, &partial_path, bytes, inline).await?;
            let mani_path = self.out_dir.join(format!("{name}.partial.manifest.json"));
            let mani_tmp = self
                .out_dir
                .join(format!("{name}.partial.manifest.json.tmp"));
            let body = serde_json::to_vec_pretty(manifest)?;
            write_sync_rename(&mani_tmp, &mani_path, &body, inline).await?;
            self.durably(&seg, bytes.len(), partial_path, mani_path)
                .await
        })
    }
}

impl DirSegmentSink {
    /// Finalize a written+renamed segment: inline fsyncs the dir, background
    /// hands it to the fsync task keyed on the LSN it covers.
    async fn durably(
        &self,
        seg: &SegmentName,
        bytes_len: usize,
        seg_path: std::path::PathBuf,
        mani_path: std::path::PathBuf,
    ) -> Result<(), SinkError> {
        match &self.durability {
            Durability::Inline => crate::cursor::fsync_dir(&self.out_dir).await?,
            Durability::Background { seg_size, tx } => {
                let end_lsn = seg.start_lsn(*seg_size) + bytes_len as u64;
                tx.send(SegFsync {
                    end_lsn,
                    seg_path,
                    mani_path,
                })
                .await
                .map_err(|_| SinkError::Other("segment fsync queue closed".into()))?;
            }
        }
        Ok(())
    }
}

async fn write_sync_rename(
    tmp: &std::path::Path,
    final_path: &std::path::Path,
    bytes: &[u8],
    fsync: bool,
) -> Result<(), SinkError> {
    use tokio::io::AsyncWriteExt as _;
    let mut f = tokio::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(tmp)
        .await?;
    f.write_all(bytes).await?;
    if fsync {
        f.sync_all().await?;
    }
    drop(f);
    tokio::fs::rename(tmp, final_path).await?;
    Ok(())
}

/// Segment-aligned record-cadence WAL filter.
///
/// Bytes pushed via [`push`](Self::push) must arrive in contiguous LSN
/// order from `base_lsn`. Owns the [`Filter`] so `CatalogTracker` state
/// (every `XLOG_RELMAP_UPDATE`, every decoded pg_class write) survives
/// segment boundaries, and the [`StreamingWalker`] so the per-page
/// parser carries pending state across `push` calls.
pub struct WalStream {
    timeline: u32,
    seg_size: u64,
    next_lsn: u64,
    /// LSN of `walker.buffer()[0]`. Always segment-aligned.
    current_lsn: u64,
    walker: StreamingWalker,
    filter: Filter,
    /// Reset on segment boundary when `segment_sink.on_segment` lands.
    pending_entries: Vec<Entry>,
    /// `Filter::stats` snapshot at segment start, for `ManifestStats` deltas.
    stats_at_segment_start: FilterStats,
    relmap_at_segment_start: u64,
    pg_class_undecoded_at_segment_start: u64,
    pg_class_oid_in_prefix_at_segment_start: u64,
    /// Defaults to [`NoopBytesSink`]; production swaps in
    /// [`crate::shadow_stream::ShadowStreamSink`].
    bytes_sink: Box<dyn RecordBytesSink + Send>,
    /// Segment-relative offset of the highest byte framed onto bytes_sink;
    /// reset to `0` at segment boundary.
    wire_offset: usize,
    poisoned: bool,
}

impl WalStream {
    pub fn new(timeline: u32, seg_size: u64, start_lsn: u64) -> Result<Self, WalStreamError> {
        if !start_lsn.is_multiple_of(seg_size) {
            return Err(WalStreamError::UnalignedBase(start_lsn));
        }
        let filter = Filter::new();
        Ok(Self {
            timeline,
            seg_size,
            next_lsn: start_lsn,
            current_lsn: start_lsn,
            walker: StreamingWalker::new(seg_size as usize),
            stats_at_segment_start: filter.stats,
            relmap_at_segment_start: filter.tracker.relmap_updates,
            pg_class_undecoded_at_segment_start: filter.tracker.pg_class_writes_undecoded,
            pg_class_oid_in_prefix_at_segment_start: filter.tracker.pg_class_writes_oid_in_prefix,
            filter,
            pending_entries: Vec::new(),
            bytes_sink: Box::new(NoopBytesSink),
            wire_offset: 0,
            poisoned: false,
        })
    }

    /// Stats here are cumulative across every segment this stream processed.
    pub fn filter(&self) -> &Filter {
        &self.filter
    }

    pub fn filter_mut(&mut self) -> &mut Filter {
        &mut self.filter
    }

    /// Must be called before the first [`push`](Self::push); swapping
    /// mid-stream leaves bytes already dispatched to the prior sink
    /// unreceived by the new one.
    pub fn set_bytes_sink(&mut self, sink: Box<dyn RecordBytesSink + Send>) {
        self.bytes_sink = sink;
    }

    pub fn align_down(lsn: u64, seg_size: u64) -> u64 {
        lsn - (lsn % seg_size)
    }

    pub fn next_lsn(&self) -> u64 {
        self.next_lsn
    }

    /// LSN one past the end of the last `on_segment` call.
    pub fn dispatched_lsn(&self) -> u64 {
        self.current_lsn
    }

    /// Append bytes starting at LSN `lsn`. Per-record cadence:
    /// filter+rewrite → `bytes_sink` (shadow wire) → `record_sink`
    /// (decoder + observers). `segment_sink` fires at the 16 MiB
    /// boundary as the archive fallback artifact.
    pub async fn push(
        &mut self,
        lsn: u64,
        bytes: &[u8],
        record_sink: &mut dyn RecordSink,
        segment_sink: &mut dyn SegmentSink,
    ) -> Result<u64, WalStreamError> {
        if self.poisoned {
            return Err(WalStreamError::Poisoned);
        }
        if lsn != self.next_lsn {
            return Err(WalStreamError::Misaligned {
                expected: self.next_lsn,
                got: lsn,
            });
        }
        let mut data = bytes;
        let mut cur_lsn = lsn;
        let chunk_cap = self.seg_size as usize;
        while !data.is_empty() {
            // Bound extend by one seg so multi-seg push buf growth stays
            // predictable; spanning records may push buf past seg_size
            // until they complete + flush back down.
            let take = chunk_cap.min(data.len());
            self.walker.extend(&data[..take]);
            cur_lsn += take as u64;
            data = &data[take..];

            if let Err(e) = self.drain_records(record_sink).await {
                self.poisoned = true;
                return Err(e);
            }

            loop {
                match self.try_flush_first_segment(segment_sink).await {
                    Ok(true) => continue,
                    Ok(false) => break,
                    Err(e) => {
                        self.poisoned = true;
                        return Err(e);
                    }
                }
            }
        }
        self.next_lsn = cur_lsn;
        Ok(cur_lsn)
    }

    /// Drain every now-completable record. Fires `bytes_sink` (shadow
    /// wire) before `record_sink` so the catalog gate
    /// `wait_for_replay(record.lsn)` clears against shadow's wire-driven
    /// apply LSN, not against `restore_command` segment landing.
    async fn drain_records(
        &mut self,
        record_sink: &mut dyn RecordSink,
    ) -> Result<(), WalStreamError> {
        loop {
            let completed: CompletedRecord = match self.walker.try_next() {
                Some(Ok(r)) => r,
                Some(Err(source)) => {
                    let seg = self.segment_for_lsn(self.current_lsn).format();
                    return Err(WalStreamError::Walk { seg, source });
                }
                None => return Ok(()),
            };
            let start_offset = completed.start_offset;
            let page_magic = completed.page_magic;
            let byte_ranges = completed.byte_ranges.clone();
            let last_range = byte_ranges.last().copied().unwrap_or((0, 0));
            let record_end = last_range.0 + last_range.1;

            // Inner scope confines walker-buffer borrows to the filter
            // call. Materialise to `'static` so the `ToDecoder` in-place
            // NOOP below can take `&mut self.walker` without conflict,
            // and so the decoder reads the original bytes after the
            // shadow stream is clobbered.
            let route;
            let parsed_for_sink: XLogRecord<'static>;
            {
                let parsed = parse_record_from_bytes(
                    completed.logical_bytes(self.walker.buffer()),
                    completed.page_magic,
                )
                .map_err(|source| WalStreamError::Parse {
                    offset: start_offset,
                    source,
                })?;
                route = self.filter.decide(&parsed);
                // `rewrite_record` below mutates walker.buf that `parsed`
                // views; dispatch needs the original parse, not post-rewrite.
                parsed_for_sink = parsed.into_owned();
            }
            let kind = match route {
                Route::ToShadow => Kind::Kept,
                Route::ToDecoder => Kind::Dropped,
            };
            if route == Route::ToDecoder {
                // `parsed_for_sink` owns the original bytes the decoder
                // reads, so clobbering the buffer with the NOOP is safe.
                match completed.stitched_bytes {
                    // Cross-page: stitch → NOOP → scatter back across
                    // `byte_ranges`.
                    Some(mut bytes) => {
                        noop_replace(&mut bytes).map_err(|source| WalStreamError::Rewrite {
                            offset: start_offset,
                            source,
                        })?;
                        self.walker.rewrite_record(&byte_ranges, &bytes);
                    }
                    // Single-page: contiguous, NOOP in place.
                    None => {
                        let (off, len) = byte_ranges[0];
                        self.walker
                            .rewrite_record_in_place(off, len, noop_replace)
                            .map_err(|source| WalStreamError::Rewrite {
                                offset: start_offset,
                                source,
                            })?;
                    }
                }
            }

            self.pending_entries.push(Entry {
                offset: start_offset as u64,
                len: parsed_for_sink.header.total_record_length,
                rmid: parsed_for_sink.header.resource_manager_id,
                info: parsed_for_sink.header.info,
                kind,
            });

            // Frame buffer[wire_offset..record_end] as one chunk: covers
            // page headers + inter-record padding so shadow's walreceiver
            // sees a stream byte-identical to disk `restore_command`.
            if record_end > self.wire_offset {
                let chunk = &self.walker.buffer()[self.wire_offset..record_end];
                let start_lsn = self.current_lsn + self.wire_offset as u64;
                self.bytes_sink.on_wire_chunk(start_lsn, chunk).await?;
                self.wire_offset = record_end;
            }

            let source_lsn = self.current_lsn + start_offset as u64;
            let record = Record {
                parsed: parsed_for_sink,
                source_lsn,
                page_magic,
                route,
            };
            record_sink.on_record(&record).await?;
        }
    }

    /// Flush the first `seg_size` bytes once a full segment accumulated
    /// AND no in-flight `pending` record straddles seg-0. Returns `true`
    /// if a seg shipped.
    ///
    /// Spanning case (pending seg-0 → seg-1): wait. Once the record
    /// completes, [`rewrite_record`](StreamingWalker::rewrite_record)
    /// applies the NOOP uniformly across both segs, then the next call
    /// ships seg-0 with rewritten partial bytes.
    async fn try_flush_first_segment(
        &mut self,
        segment_sink: &mut dyn SegmentSink,
    ) -> Result<bool, WalStreamError> {
        let seg_size = self.seg_size as usize;
        if self.walker.buffer_len() < seg_size {
            return Ok(false);
        }
        if let Some(pend_off) = self.walker.pending_start_offset()
            && pend_off < seg_size
        {
            return Ok(false);
        }
        let seg = self.segment_for_lsn(self.current_lsn);
        let relmap_delta = self.filter.tracker.relmap_updates - self.relmap_at_segment_start;
        let pgc_un_delta = self.filter.tracker.pg_class_writes_undecoded
            - self.pg_class_undecoded_at_segment_start;
        let pgc_oip_delta = self.filter.tracker.pg_class_writes_oid_in_prefix
            - self.pg_class_oid_in_prefix_at_segment_start;
        let stats_delta = self.filter.stats.delta_from(&self.stats_at_segment_start);
        // offset < seg_size → seg-0; remainder stays for seg-1's flush
        // with offsets rebased to post-truncate positions.
        let mut seg_entries = Vec::with_capacity(self.pending_entries.len());
        let mut future_entries = Vec::new();
        for entry in std::mem::take(&mut self.pending_entries) {
            if (entry.offset as usize) < seg_size {
                seg_entries.push(entry);
            } else {
                future_entries.push(Entry {
                    offset: entry.offset - self.seg_size,
                    ..entry
                });
            }
        }
        self.pending_entries = future_entries;
        let manifest = Manifest {
            source_segment: seg.format(),
            filter_version: FILTER_VERSION,
            records: seg_entries,
            stats: ManifestStats::from_filter(
                &stats_delta,
                relmap_delta,
                pgc_un_delta,
                pgc_oip_delta,
            ),
        };
        // Wire tail: if wire_offset < seg_size, the residual span
        // (alignment pad + page header + in-place-rewritten spanning bytes)
        // must ship before seg-0 closes.
        if self.wire_offset < seg_size {
            let trailing_start_lsn = self.current_lsn + self.wire_offset as u64;
            let trailing = &self.walker.buffer()[self.wire_offset..seg_size];
            self.bytes_sink
                .on_segment_boundary(trailing_start_lsn, trailing)
                .await?;
        }
        segment_sink
            .on_segment(seg, &self.walker.buffer()[..seg_size], &manifest)
            .await?;
        self.walker.truncate_first_segment();
        self.current_lsn += self.seg_size;
        self.wire_offset = self.wire_offset.saturating_sub(seg_size);
        self.stats_at_segment_start = self.filter.stats;
        self.relmap_at_segment_start = self.filter.tracker.relmap_updates;
        self.pg_class_undecoded_at_segment_start = self.filter.tracker.pg_class_writes_undecoded;
        self.pg_class_oid_in_prefix_at_segment_start =
            self.filter.tracker.pg_class_writes_oid_in_prefix;
        Ok(true)
    }

    /// Shutdown flush of the partial segment (no-op if empty). Lands a
    /// `.partial` via [`SegmentSink::on_partial_segment`] so shadow PG's
    /// `restore_command` doesn't pick it up as complete.
    pub async fn close(
        mut self,
        mut partial_sink: Option<&mut dyn SegmentSink>,
        record_sink: &mut dyn RecordSink,
    ) -> Result<(), WalStreamError> {
        // Drain full segments first: buf may exceed seg_size if a record
        // straddled the boundary at shutdown. These ship as normal (not
        // `.partial`) files.
        if let Some(sink) = partial_sink.as_deref_mut() {
            while self.walker.buffer_len() >= self.seg_size as usize {
                if !self.try_flush_first_segment(sink).await? {
                    break;
                }
            }
        }
        if self.walker.buffer_len() == 0 {
            return Ok(());
        }
        // Records already drained at push cadence; partial buffer may
        // hold an unparsed tail. Zero-pad + re-run filter_segment so the
        // `.partial` matches the shape `pg_receivewal` writes and any
        // still-undrained whole-segment records land in one more pass.
        let seg = self.segment_for_lsn(self.current_lsn);
        let seg_start_lsn = self.current_lsn;
        // Local copy acceptable since `close` fires once; hot-path
        // flushes hand the walker buffer to the sink directly.
        let mut buf: Vec<u8> = Vec::with_capacity(self.seg_size as usize);
        buf.extend_from_slice(self.walker.buffer());
        if buf.len() < self.seg_size as usize {
            buf.resize(self.seg_size as usize, 0);
        }
        self.walker.reset_segment();
        let name = seg.format();

        // Second pass picks up at most records that needed a tail-page to
        // complete (none on graceful shutdown; bounded by zero pad).
        let (filtered, manifest, parsed) =
            filter_segment(&buf, &name, &mut self.filter).map_err(|source| {
                WalStreamError::Filter {
                    seg: name.clone(),
                    source,
                }
            })?;
        // Do NOT re-dispatch through record_sink: records already fired
        // at push cadence, re-firing would double-count.
        let _ = parsed;
        let _ = seg_start_lsn;
        if let Some(sink) = partial_sink {
            sink.on_partial_segment(seg, &filtered, &manifest).await?;
        }
        let _ = record_sink;
        Ok(())
    }

    fn segment_for_lsn(&self, lsn: u64) -> SegmentName {
        let seg_no = lsn / self.seg_size;
        let xlog_segs_per_xlog_id = 0x1_0000_0000u64 / self.seg_size;
        SegmentName {
            timeline: self.timeline,
            log_id: (seg_no / xlog_segs_per_xlog_id) as u32,
            seg_no: (seg_no % xlog_segs_per_xlog_id) as u32,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manifest::{Entry, FILTER_VERSION, ManifestStats};
    use walrus::pg::walparser::RmId;

    fn dummy_manifest_entry(offset: u64, rmid: u8) -> Entry {
        Entry {
            offset,
            len: 32,
            rmid,
            info: 0,
            kind: Kind::Kept,
        }
    }

    fn dummy_manifest() -> Manifest {
        Manifest {
            source_segment: "test".into(),
            filter_version: FILTER_VERSION,
            records: vec![],
            stats: ManifestStats::default(),
        }
    }

    #[test]
    fn record_lsn_offset_is_seg_start_plus_entry_offset() {
        use walrus::pg::walparser::XLogRecordHeader;
        let entry = dummy_manifest_entry(40, RmId::Xact as u8);
        let parsed = ParsedRecord {
            record: XLogRecord {
                header: XLogRecordHeader {
                    resource_manager_id: RmId::Xact as u8,
                    xact_id: 42,
                    ..Default::default()
                },
                ..Default::default()
            },
            page_magic: 0xD110,
        };
        let record = Record::from_parsed(0x1000_0000, parsed, &entry);
        assert_eq!(record.source_lsn, 0x1000_0000 + 40);
        assert_eq!(record.parsed.header.resource_manager_id, RmId::Xact as u8);
        assert_eq!(record.parsed.header.xact_id, 42);
        assert_eq!(record.page_magic, 0xD110);
        assert_eq!(record.route, Route::ToShadow);
    }

    #[test]
    fn align_down_rounds_to_segment_boundary() {
        let s = WAL_SEG_SIZE;
        assert_eq!(WalStream::align_down(0, s), 0);
        assert_eq!(WalStream::align_down(s, s), s);
        assert_eq!(WalStream::align_down(s + 1, s), s);
        assert_eq!(WalStream::align_down(s * 2 - 1, s), s);
        assert_eq!(WalStream::align_down(s * 3, s), s * 3);
    }

    #[test]
    fn new_rejects_unaligned_base() {
        let r = WalStream::new(1, WAL_SEG_SIZE, 0x1234);
        assert!(matches!(r, Err(WalStreamError::UnalignedBase(_))));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn push_misaligned_errors() {
        let mut ws = WalStream::new(1, WAL_SEG_SIZE, 0).unwrap();
        let mut rec = CollectingRecordSink::default();
        let mut seg = CollectingSegmentSink::default();
        let err = ws
            .push(0x100, &[0u8; 1], &mut rec, &mut seg)
            .await
            .expect_err("misaligned push must error");
        match err {
            WalStreamError::Misaligned { expected, got } => {
                assert_eq!(expected, 0);
                assert_eq!(got, 0x100);
            }
            _ => panic!("wrong error variant"),
        }
    }

    struct ErrSegmentSink;
    impl SegmentSink for ErrSegmentSink {
        fn on_segment<'a>(
            &'a mut self,
            _seg: SegmentName,
            _bytes: &'a [u8],
            _manifest: &'a Manifest,
        ) -> Pin<Box<dyn Future<Output = Result<(), SinkError>> + Send + 'a>> {
            Box::pin(async { Err(SinkError::Other("synthetic segment-sink fail".into())) })
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn push_segment_sink_error_poisons_stream() {
        const SEG: u64 = 8192;
        let mut ws = WalStream::new(1, SEG, 0).unwrap();
        let mut rec = CollectingRecordSink::default();
        let mut seg = ErrSegmentSink;
        let bytes = vec![0u8; SEG as usize];
        let err = ws
            .push(0, &bytes, &mut rec, &mut seg)
            .await
            .expect_err("sink error must propagate");
        assert!(matches!(err, WalStreamError::Sink(_)), "{err:?}");
        let mut good_seg = CollectingSegmentSink::default();
        let err2 = ws
            .push(SEG, &[0u8; 1], &mut rec, &mut good_seg)
            .await
            .expect_err("subsequent push must short-circuit");
        assert!(matches!(err2, WalStreamError::Poisoned));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn push_walk_error_poisons_stream() {
        const SEG: u64 = 8192;
        let mut ws = WalStream::new(1, SEG, 0).unwrap();
        let mut rec = CollectingRecordSink::default();
        let mut seg = CollectingSegmentSink::default();
        let mut bytes = vec![0u8; SEG as usize];
        bytes[0] = 0xFF;
        bytes[1] = 0xFF;
        bytes[2] = 1;
        let err = ws
            .push(0, &bytes, &mut rec, &mut seg)
            .await
            .expect_err("walk error must propagate");
        assert!(matches!(err, WalStreamError::Walk { .. }), "{err:?}");
        let err2 = ws
            .push(SEG, &[0u8; 1], &mut rec, &mut seg)
            .await
            .expect_err("subsequent push must short-circuit");
        assert!(matches!(err2, WalStreamError::Poisoned));
    }

    fn synth_record(offset: u64, rmid: u8) -> Record<'static> {
        use walrus::pg::walparser::XLogRecordHeader;
        let entry = dummy_manifest_entry(offset, rmid);
        let parsed = ParsedRecord {
            record: XLogRecord {
                header: XLogRecordHeader {
                    resource_manager_id: rmid,
                    ..Default::default()
                },
                ..Default::default()
            },
            page_magic: 0xD110,
        };
        Record::from_parsed(0, parsed, &entry)
    }

    struct SharedRmidLog(std::sync::Arc<std::sync::Mutex<Vec<u8>>>);

    impl RecordSink for SharedRmidLog {
        fn on_record<'a>(
            &'a mut self,
            r: &'a Record<'a>,
        ) -> Pin<Box<dyn Future<Output = Result<(), SinkError>> + Send + 'a>> {
            Box::pin(async move {
                self.0
                    .lock()
                    .unwrap()
                    .push(r.parsed.header.resource_manager_id);
                Ok(())
            })
        }
    }

    struct ErrAt {
        seen: std::sync::Arc<std::sync::atomic::AtomicU64>,
        fail_at: u64,
    }

    impl RecordSink for ErrAt {
        fn on_record<'a>(
            &'a mut self,
            _record: &'a Record<'a>,
        ) -> Pin<Box<dyn Future<Output = Result<(), SinkError>> + Send + 'a>> {
            Box::pin(async move {
                let i = self.seen.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                if i == self.fail_at {
                    Err(SinkError::Other(format!("synthetic fail at #{i}")))
                } else {
                    Ok(())
                }
            })
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn composite_record_sink_fans_out_to_all_inner_sinks_in_order() {
        let recs = [
            synth_record(0, RmId::Heap as u8),
            synth_record(64, RmId::Xact as u8),
            synth_record(128, RmId::RelMap as u8),
        ];
        let log_a = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let log_b = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let mut comp = CompositeRecordSink::new(vec![
            Box::new(SharedRmidLog(log_a.clone())),
            Box::new(SharedRmidLog(log_b.clone())),
        ]);
        for r in &recs {
            comp.on_record(r).await.unwrap();
        }
        let expected = vec![RmId::Heap as u8, RmId::Xact as u8, RmId::RelMap as u8];
        assert_eq!(*log_a.lock().unwrap(), expected);
        assert_eq!(*log_b.lock().unwrap(), expected);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn composite_record_sink_short_circuits_on_first_err() {
        use std::sync::atomic::{AtomicU64, Ordering};
        let rec = synth_record(0, RmId::Heap as u8);
        let log_before = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let err_seen = std::sync::Arc::new(AtomicU64::new(0));
        let log_after = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let mut comp = CompositeRecordSink::new(vec![
            Box::new(SharedRmidLog(log_before.clone())),
            Box::new(ErrAt {
                seen: err_seen.clone(),
                fail_at: 1,
            }),
            Box::new(SharedRmidLog(log_after.clone())),
        ]);
        comp.on_record(&rec).await.expect("first record succeeds");
        let err = comp
            .on_record(&rec)
            .await
            .expect_err("err propagates from inner sink");
        match err {
            SinkError::Other(msg) => assert!(msg.contains("synthetic fail")),
            _ => panic!("expected SinkError::Other, got {err:?}"),
        }
        assert_eq!(log_before.lock().unwrap().len(), 2);
        assert_eq!(err_seen.load(Ordering::Relaxed), 2);
        assert_eq!(log_after.lock().unwrap().len(), 1);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn metrics_sink_counts_per_rm_route_and_discards() {
        let mut sink = MetricsRecordSink::default();
        let mut heap_keep = synth_record(0, RmId::Heap as u8);
        heap_keep.route = Route::ToShadow;
        let mut heap_drop = synth_record(64, RmId::Heap as u8);
        heap_drop.route = Route::ToDecoder;
        let mut xact_keep = synth_record(128, RmId::Xact as u8);
        xact_keep.route = Route::ToShadow;
        for r in [&heap_keep, &heap_keep, &heap_drop, &xact_keep] {
            sink.on_record(r).await.unwrap();
        }
        assert_eq!(sink.total, 4);
        assert_eq!(sink.by_rm_route[&(RmId::Heap as u8, Route::ToShadow)], 2,);
        assert_eq!(sink.by_rm_route[&(RmId::Heap as u8, Route::ToDecoder)], 1,);
        assert_eq!(sink.by_rm_route[&(RmId::Xact as u8, Route::ToShadow)], 1,);
        let summary = sink.summary();
        assert!(summary.starts_with("total=4"), "got {summary:?}");
        assert!(summary.contains("heap/to_shadow=2"), "got {summary:?}");
        assert!(summary.contains("heap/to_decoder=1"), "got {summary:?}");
        assert!(summary.contains("xact/to_shadow=1"), "got {summary:?}");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn dir_sink_writes_segment_and_manifest_atomically() {
        let tmp = tempfile::tempdir().unwrap();
        let mut sink = DirSegmentSink::new(tmp.path().to_path_buf()).unwrap();
        let seg = SegmentName::parse("000000010000000000000003").unwrap();
        let bytes = vec![0xAAu8; 64];
        let mani = dummy_manifest();
        sink.on_segment(seg, &bytes, &mani).await.unwrap();
        let seg_path = tmp.path().join(seg.format());
        let mani_path = tmp.path().join(format!("{}.manifest.json", seg.format()));
        assert!(seg_path.exists(), "segment file written");
        assert!(mani_path.exists(), "manifest sidecar written");
        let on_disk = std::fs::read(&seg_path).unwrap();
        assert_eq!(on_disk, bytes);
        assert!(
            !tmp.path()
                .join(format!("{}.partial", seg.format()))
                .exists()
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn dir_sink_partial_segment_lands_with_partial_suffix() {
        let tmp = tempfile::tempdir().unwrap();
        let mut sink = DirSegmentSink::new(tmp.path().to_path_buf()).unwrap();
        let seg = SegmentName::parse("000000010000000000000004").unwrap();
        let bytes = vec![0x77u8; 64];
        let mani = dummy_manifest();
        sink.on_partial_segment(seg, &bytes, &mani).await.unwrap();
        let name = seg.format();
        let partial_path = tmp.path().join(format!("{name}.partial"));
        let partial_mani_path = tmp.path().join(format!("{name}.partial.manifest.json"));
        assert!(
            !tmp.path().join(&name).exists(),
            "complete-segment path leaked: {name}",
        );
        assert!(partial_path.exists(), "partial path written");
        assert!(partial_mani_path.exists(), "partial manifest written");
        let on_disk = std::fs::read(&partial_path).unwrap();
        assert_eq!(on_disk, bytes);
        assert!(!tmp.path().join(format!("{name}.partial.tmp")).exists());
        assert!(
            !tmp.path()
                .join(format!("{name}.partial.manifest.json.tmp"))
                .exists()
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn dir_sink_with_durability_writes_inline_and_enqueues_fsync() {
        let tmp = tempfile::tempdir().unwrap();
        let (tx, mut rx) = mpsc::channel::<SegFsync>(4);
        let mut sink =
            DirSegmentSink::with_durability(tmp.path().to_path_buf(), WAL_SEG_SIZE, tx).unwrap();
        let seg = SegmentName::parse("000000010000000000000003").unwrap();
        let bytes = vec![0xAAu8; 64];
        sink.on_segment(seg, &bytes, &dummy_manifest())
            .await
            .unwrap();

        // Written + renamed inline; fsync deferred to the background task.
        let seg_path = tmp.path().join(seg.format());
        assert_eq!(std::fs::read(&seg_path).unwrap(), bytes);
        let msg = rx.try_recv().expect("segment enqueued for fsync");
        assert_eq!(
            msg.end_lsn,
            seg.start_lsn(WAL_SEG_SIZE) + bytes.len() as u64
        );
        assert_eq!(msg.seg_path, seg_path);
    }

    /// Contract: a `RecordBytesSink` sees the full wire stream (record
    /// images + page headers + inter-record padding); chunks + trailing
    /// sum to seg_size exactly.
    #[tokio::test(flavor = "current_thread")]
    async fn bytes_sink_receives_full_wire_stream() {
        const SEG: u64 = 8192;
        let mut rec = CollectingRecordSink::default();
        let mut seg = CollectingSegmentSink::default();

        type WireLog = std::sync::Arc<std::sync::Mutex<Vec<(u64, Vec<u8>)>>>;
        let collector_chunks: WireLog = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let collector_tails: WireLog = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        struct SharedCollector(WireLog, WireLog);
        impl RecordBytesSink for SharedCollector {
            fn on_wire_chunk<'a>(
                &'a mut self,
                start_lsn: u64,
                bytes: &'a [u8],
            ) -> Pin<Box<dyn Future<Output = Result<(), SinkError>> + Send + 'a>> {
                Box::pin(async move {
                    self.0.lock().unwrap().push((start_lsn, bytes.to_vec()));
                    Ok(())
                })
            }
            fn on_segment_boundary<'a>(
                &'a mut self,
                start_lsn: u64,
                trailing_bytes: &'a [u8],
            ) -> Pin<Box<dyn Future<Output = Result<(), SinkError>> + Send + 'a>> {
                Box::pin(async move {
                    self.1
                        .lock()
                        .unwrap()
                        .push((start_lsn, trailing_bytes.to_vec()));
                    Ok(())
                })
            }
        }

        let page = synth_xact_page();
        let mut ws = WalStream::new(1, SEG, 0).unwrap();
        ws.set_bytes_sink(Box::new(SharedCollector(
            collector_chunks.clone(),
            collector_tails.clone(),
        )));
        ws.push(0, &page, &mut rec, &mut seg).await.unwrap();
        assert!(!rec.records.is_empty(), "record_sink fired");

        let chunks = collector_chunks.lock().unwrap();
        let tails = collector_tails.lock().unwrap();
        let mut reconstructed = Vec::with_capacity(SEG as usize);
        let mut expected_lsn = 0u64;
        for (start, bytes) in chunks.iter() {
            assert_eq!(*start, expected_lsn, "wire chunks must be contiguous");
            reconstructed.extend_from_slice(bytes);
            expected_lsn = start + bytes.len() as u64;
        }
        assert_eq!(tails.len(), 1);
        let (tail_start, tail_bytes) = &tails[0];
        assert_eq!(*tail_start, expected_lsn);
        reconstructed.extend_from_slice(tail_bytes);
        assert_eq!(reconstructed.len(), SEG as usize, "covers full segment");
        let (_, seg_bytes, _) = &seg.segments[0];
        assert_eq!(&reconstructed, seg_bytes, "wire bytes match segment bytes");
    }

    fn synth_xact_page() -> Vec<u8> {
        use walrus::pg::walparser::{
            WAL_PAGE_SIZE, X_LOG_RECORD_HEADER_SIZE, XLP_LONG_HEADER, XLP_PAGE_MAGIC_PG15,
            XLR_BLOCK_ID_DATA_SHORT,
        };
        const PAGE_SIZE: usize = WAL_PAGE_SIZE as usize;
        fn rec(rmid: u8) -> Vec<u8> {
            let body_len = 1 + 1 + 4; // short marker + len + 4-byte payload
            let total = X_LOG_RECORD_HEADER_SIZE + body_len;
            let mut v = Vec::with_capacity(total);
            v.extend_from_slice(&(total as u32).to_le_bytes());
            v.extend_from_slice(&0u32.to_le_bytes()); // xact
            v.extend_from_slice(&0u64.to_le_bytes()); // prev
            v.push(0); // info
            v.push(rmid);
            v.push(0);
            v.push(0);
            v.extend_from_slice(&0u32.to_le_bytes()); // crc placeholder
            v.push(XLR_BLOCK_ID_DATA_SHORT);
            v.push(4u8);
            v.extend_from_slice(&[0xDEu8; 4]);
            let crc = crate::rewrite::compute_crc(&v);
            v[20..24].copy_from_slice(&crc.to_le_bytes());
            v
        }
        let r1 = rec(walrus::pg::walparser::RmId::Xact as u8);
        let r2 = rec(walrus::pg::walparser::RmId::Xact as u8);
        let mut page = Vec::with_capacity(PAGE_SIZE);
        page.extend_from_slice(&XLP_PAGE_MAGIC_PG15.to_le_bytes());
        page.extend_from_slice(&XLP_LONG_HEADER.to_le_bytes());
        page.extend_from_slice(&1u32.to_le_bytes()); // timeline
        page.extend_from_slice(&0u64.to_le_bytes()); // page_address
        page.extend_from_slice(&0u32.to_le_bytes()); // remaining_data_len
        page.extend_from_slice(&12345u64.to_le_bytes()); // sysid
        page.extend_from_slice(&(8192u32 * 1024).to_le_bytes()); // seg_size
        page.extend_from_slice(&8192u32.to_le_bytes()); // xlog_block_size
        page.extend_from_slice(&[0u8; 4]); // pad to 40
        for r in [&r1, &r2] {
            page.extend_from_slice(r);
            let pad = (8 - (page.len() % 8)) % 8;
            page.extend(std::iter::repeat_n(0u8, pad));
        }
        page.resize(PAGE_SIZE, 0);
        page
    }
}
