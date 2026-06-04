//! Streaming filter pipeline. Wraps the per-record
//! [`StreamingWalker`](crate::streaming_walker::StreamingWalker) in a
//! segment-aligned accumulator that consumes arbitrary WAL byte chunks
//! (from wal-rs's `START_REPLICATION` CopyData stream) and dispatches
//! to per-record + per-segment sinks at record cadence.
//!
//! ## Architecture
//!
//! ```text
//!   wal-rs CopyData('w') chunks
//!              v
//!     +-----------------+
//!     |  WalStream::push|  base_lsn aligned to segment boundary
//!     +--------+--------+  extends StreamingWalker buffer
//!              | per record completing
//!              v
//!         Filter::decide
//!         /     |        \
//!        v      v         v
//!   noop_replace (Drop)  rewrite_record  manifest
//!              |          (in place)     entry
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
//! Per-record dispatch happens the moment a record's last byte arrives.
//! Segment-level dispatch fires on segment boundary with the
//! already-filtered bytes + accumulated manifest. No re-parse, no
//! second walk.

use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;

use thiserror::Error;
use wal_rs::pg::wal::segment::SegmentName;
use wal_rs::pg::walparser::{ParseError, XLogRecord, parse_record_from_bytes};

use crate::classify::rmgr_label;
use crate::filter::{Decision, Filter, FilterStats};
use crate::filter_segment::{FilterSegmentError, ParsedRecord, filter_segment};
use crate::manifest::{Entry, FILTER_VERSION, Kind, Manifest, ManifestStats};
use crate::rewrite::{RewriteError, noop_replace};
use crate::streaming_walker::{CompletedRecord, StreamingWalker, WalkError};

/// Default PG WAL segment size — 16 MiB. walshadow assumes the
/// upstream initdb default since every supported PG version starts
/// there; non-default seg sizes need operator coordination (the
/// shadow's `initdb --wal-segsize` must match).
pub const WAL_SEG_SIZE: u64 = wal_rs::pg::wal::segment::DEFAULT_WAL_SEG_SIZE;

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

/// Per-record hand-off carrying the parsed `XLogRecord`, where it sits
/// in the source LSN stream, and the keep/drop decision the filter
/// computed. The heap-tuple decoder reads `parsed.header.xact_id`,
/// `parsed.blocks[i].header.location.rel`, and `parsed.main_data`;
/// `page_magic` selects PG-15-vs-PG-14 FPI bit semantics.
///
/// The `'a` lifetime ties `parsed`'s borrows back to the walker's
/// segment buffer — the streaming path constructs and dispatches a
/// `Record<'a>` inside one `RecordSink::on_record` future without
/// allocating per-record byte copies. Test sinks that store records
/// across `try_next` iterations call `record.parsed.clone().into_owned()`
/// to bump to `Record<'static>`.
#[derive(Debug, Clone, Default)]
pub struct Record<'a> {
    pub parsed: XLogRecord<'a>,
    /// Absolute source LSN where the record begins.
    pub source_lsn: u64,
    /// Magic of the page whose data area the record header sat on.
    pub page_magic: u16,
    /// Keep/drop decision the filter computed.
    pub decision: Decision,
}

impl Record<'static> {
    /// Zip one parsed record with its matching [`Manifest`] entry
    /// plus the segment's start LSN. Index alignment between
    /// `parsed_records` and `manifest.records` is the
    /// `filter_segment` contract.
    pub fn from_parsed(
        seg_start_lsn: u64,
        parsed: ParsedRecord,
        entry: &crate::manifest::Entry,
    ) -> Self {
        let decision = match entry.kind {
            Kind::Kept => Decision::Keep,
            Kind::Dropped => Decision::Drop,
        };
        Self {
            parsed: parsed.record,
            source_lsn: seg_start_lsn + entry.offset,
            page_magic: parsed.page_magic,
            decision,
        }
    }
}

/// Sink that observes every record decided by the filter. The
/// heap-tuple decoder attaches here.
///
/// `record` carries a `'b` borrow back into the streaming walker's
/// segment buffer — sinks consume slices for the duration of the
/// future and don't store the record. Sinks that must store records
/// (eg test collectors) call `record.parsed.clone().into_owned()` to
/// materialise an owned `XLogRecord<'static>` first.
pub trait RecordSink {
    fn on_record<'a>(
        &'a mut self,
        record: &'a Record<'a>,
    ) -> Pin<Box<dyn Future<Output = Result<(), SinkError>> + Send + 'a>>;

    /// Driver-initiated tick for time-based work that can't wait on
    /// the next record. Today's only consumer is the CH emitter's
    /// hold-INSERT-open path, where the close-and-ack handshake is
    /// gated on `flush_timeout` elapsing — an idle stream would
    /// otherwise sit on rows indefinitely. Fired by
    /// [`crate::queueing_record_sink::QueueingRecordSink`]'s worker
    /// when its receive loop times out. Default: no-op.
    fn on_idle<'a>(
        &'a mut self,
    ) -> Pin<Box<dyn Future<Output = Result<(), SinkError>> + Send + 'a>> {
        Box::pin(async { Ok(()) })
    }

    /// Final hook before the sink drops. Fired by the queueing worker
    /// after its channel closes (i.e. the daemon called `close()` on
    /// the wrapper). Lets the CH emitter force-close any
    /// hold-INSERT-open buffers regardless of deadline — without
    /// this, the last burst of rows in a flush window stays
    /// non-durable on graceful shutdown. Default: no-op.
    fn on_close<'a>(
        &'a mut self,
    ) -> Pin<Box<dyn Future<Output = Result<(), SinkError>> + Send + 'a>> {
        Box::pin(async { Ok(()) })
    }

    /// Post-batch nudge from the queueing worker: every record with
    /// `source_lsn <= lsn` has been dispatched through `on_record`.
    /// The xact buffer uses it to advance `emitter_ack_lsn` past
    /// trailing non-commit WAL (checkpoint, RUNNING_XACTS, post-COMMIT
    /// page-padding) when no xact is in flight. Without it source's
    /// slot pins WAL at the last COMMIT — the kill-restart drill's
    /// post-catchup idle never resolves. Default: no-op.
    fn on_idle_advance<'a>(
        &'a mut self,
        _lsn: u64,
    ) -> Pin<Box<dyn Future<Output = Result<(), SinkError>> + Send + 'a>> {
        Box::pin(async { Ok(()) })
    }
}

/// Streaming sink for the post-filter WAL byte stream.
///
/// [`crate::shadow_stream::ShadowStreamSink`] hooks in here:
/// the wire framer pushes these bytes onto every active shadow
/// connection's `'w'` send buffer at record cadence so shadow's
/// walreceiver replays a byte-exact stream (record bytes plus the
/// page headers that sit between them — both required for shadow's
/// startup state machine to parse the WAL).
///
/// `on_wire_chunk` fires after every finalized record carrying the
/// contiguous slice of the segment buffer from the last dispatched
/// byte up to the record's end. Returning `Err` poisons the stream.
pub trait RecordBytesSink: Send {
    fn on_wire_chunk<'a>(
        &'a mut self,
        start_lsn: u64,
        bytes: &'a [u8],
    ) -> Pin<Box<dyn Future<Output = Result<(), SinkError>> + Send + 'a>>;

    /// Called when a 16 MiB segment is finalized; carries any
    /// trailing bytes (post-last-record padding / zero tail) along
    /// with the segment-end LSN so the sink can advertise an
    /// accurate `server_wal_end` in subsequent keepalives.
    fn on_segment_boundary<'a>(
        &'a mut self,
        _start_lsn: u64,
        _trailing_bytes: &'a [u8],
    ) -> Pin<Box<dyn Future<Output = Result<(), SinkError>> + Send + 'a>> {
        Box::pin(async { Ok(()) })
    }
}

/// Sink that receives one fully-filtered segment at a time. Shadow PG
/// consumes filtered segments via `restore_command`; the production
/// sink writes the bytes plus a manifest sidecar to that directory.
pub trait SegmentSink {
    fn on_segment<'a>(
        &'a mut self,
        seg: SegmentName,
        bytes: &'a [u8],
        manifest: &'a Manifest,
    ) -> Pin<Box<dyn Future<Output = Result<(), SinkError>> + Send + 'a>>;

    /// Receives a partial segment flushed at shutdown by
    /// [`WalStream::close`]. Default forwards to [`on_segment`] for
    /// test sinks that don't care; production [`DirSegmentSink`]
    /// overrides to land bytes under `<name>.partial` per
    /// `pg_receivewal` convention so a follow-up daemon run does not
    /// confuse a partial for a complete segment.
    fn on_partial_segment<'a>(
        &'a mut self,
        seg: SegmentName,
        bytes: &'a [u8],
        manifest: &'a Manifest,
    ) -> Pin<Box<dyn Future<Output = Result<(), SinkError>> + Send + 'a>> {
        self.on_segment(seg, bytes, manifest)
    }
}

/// In-memory `RecordSink` for tests. Stores every record. Records
/// are materialised to `'static` via [`XLogRecord::into_owned`] before
/// storage so the borrow back into the walker buffer doesn't outlive
/// the `on_record` future.
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
                decision: record.decision,
            });
            Ok(())
        })
    }
}

/// Light-weight `RecordSink` that only counts.
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

/// `RecordSink` that maintains a per-`(rmid, decision)` counter and
/// discards the record.
#[derive(Debug, Default)]
pub struct MetricsRecordSink {
    pub by_rm_decision: BTreeMap<(u8, Decision), u64>,
    pub total: u64,
}

impl MetricsRecordSink {
    pub fn summary(&self) -> String {
        use std::fmt::Write as _;
        let mut s = String::new();
        write!(&mut s, "total={}", self.total).unwrap();
        for ((rm, decision), n) in &self.by_rm_decision {
            let d = match decision {
                Decision::Keep => "keep",
                Decision::Drop => "drop",
            };
            write!(&mut s, " {}/{}={}", rmgr_label(*rm), d, n).unwrap();
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
            *self
                .by_rm_decision
                .entry((rm, record.decision))
                .or_insert(0) += 1;
            self.total += 1;
            Ok(())
        })
    }
}

/// Fan-out `RecordSink` that dispatches each record to a chain of inner
/// sinks. Dispatches in `inner` order and short-circuits on first `Err`.
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

/// In-memory `RecordBytesSink` for tests. Captures each contiguous
/// wire chunk + the segment boundary tails.
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

/// `RecordBytesSink` no-op default. Avoids each `WalStream::push`
/// paying the cost of an `Option` branch on every record.
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

/// Production segment sink that writes filtered segments + manifests
/// to a target directory. Shadow PG's `restore_command` reads from the
/// same directory.
pub struct DirSegmentSink {
    out_dir: std::path::PathBuf,
}

impl DirSegmentSink {
    pub fn new(out_dir: std::path::PathBuf) -> Result<Self, SinkError> {
        std::fs::create_dir_all(&out_dir)?;
        Ok(Self { out_dir })
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
            let name = seg.format();
            let seg_path = self.out_dir.join(&name);
            let tmp = seg_path.with_extension("partial");
            write_sync_rename(&tmp, &seg_path, bytes).await?;
            let mani_path = self.out_dir.join(format!("{name}.manifest.json"));
            let mani_tmp = mani_path.with_extension("manifest.json.partial");
            let body = serde_json::to_vec_pretty(manifest)?;
            write_sync_rename(&mani_tmp, &mani_path, &body).await?;
            crate::cursor::fsync_dir(&self.out_dir).await?;
            Ok(())
        })
    }

    fn on_partial_segment<'a>(
        &'a mut self,
        seg: SegmentName,
        bytes: &'a [u8],
        manifest: &'a Manifest,
    ) -> Pin<Box<dyn Future<Output = Result<(), SinkError>> + Send + 'a>> {
        Box::pin(async move {
            let name = seg.format();
            let partial_path = self.out_dir.join(format!("{name}.partial"));
            let tmp = self.out_dir.join(format!("{name}.partial.tmp"));
            write_sync_rename(&tmp, &partial_path, bytes).await?;
            let mani_path = self.out_dir.join(format!("{name}.partial.manifest.json"));
            let mani_tmp = self
                .out_dir
                .join(format!("{name}.partial.manifest.json.tmp"));
            let body = serde_json::to_vec_pretty(manifest)?;
            write_sync_rename(&mani_tmp, &mani_path, &body).await?;
            crate::cursor::fsync_dir(&self.out_dir).await?;
            Ok(())
        })
    }
}

async fn write_sync_rename(
    tmp: &std::path::Path,
    final_path: &std::path::Path,
    bytes: &[u8],
) -> Result<(), SinkError> {
    use tokio::io::AsyncWriteExt as _;
    let mut f = tokio::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(tmp)
        .await?;
    f.write_all(bytes).await?;
    f.sync_all().await?;
    drop(f);
    tokio::fs::rename(tmp, final_path).await?;
    Ok(())
}

/// Segment-aligned record-cadence WAL filter.
///
/// Bytes pushed via [`push`](Self::push) must arrive in contiguous LSN
/// order starting from [`base_lsn`](Self::base_lsn).
///
/// Owns the long-lived [`Filter`]: `CatalogTracker` state — every
/// `XLOG_RELMAP_UPDATE`, every decoded pg_class write — must survive
/// segment boundaries.
///
/// Owns the [`StreamingWalker`] so the per-page parser carries pending
/// state across `push` calls without resampling bytes from disk.
pub struct WalStream {
    timeline: u32,
    seg_size: u64,
    /// LSN of the next byte expected by [`push`](Self::push).
    next_lsn: u64,
    /// LSN of the byte at `walker.buffer()[0]`. Always segment-aligned.
    current_lsn: u64,
    walker: StreamingWalker,
    filter: Filter,
    /// Per-segment accumulating manifest entries. Reset on segment
    /// boundary when `segment_sink.on_segment` lands.
    pending_entries: Vec<Entry>,
    /// `Filter::stats` snapshot at the start of the current segment.
    /// Used to compute `ManifestStats` deltas at segment boundary.
    stats_at_segment_start: FilterStats,
    relmap_at_segment_start: u64,
    pg_class_undecoded_at_segment_start: u64,
    pg_class_oid_in_prefix_at_segment_start: u64,
    /// Byte stream destination for the shadow wire.
    /// Defaults to [`NoopBytesSink`]; production swaps in
    /// [`crate::shadow_stream::ShadowStreamSink`] via
    /// [`set_bytes_sink`](Self::set_bytes_sink).
    bytes_sink: Box<dyn RecordBytesSink + Send>,
    /// Segment-relative offset of the highest byte already framed
    /// onto the bytes_sink. Advances as records complete; reset to
    /// `0` at segment boundary.
    wire_offset: usize,
    /// Set by [`push`](Self::push) when filter or sink dispatch fails.
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

    /// Borrow the long-lived filter. Stats here are cumulative across
    /// every segment processed by this stream.
    pub fn filter(&self) -> &Filter {
        &self.filter
    }

    /// Mutable access for pre-stream setup.
    pub fn filter_mut(&mut self) -> &mut Filter {
        &mut self.filter
    }

    /// Install a `RecordBytesSink` (eg
    /// [`crate::shadow_stream::ShadowStreamSink`]). Must be called
    /// before the first [`push`](Self::push); swapping mid-stream
    /// would leave bytes already dispatched to the previous sink
    /// unreceived by the new one. Default is `NoopBytesSink`.
    pub fn set_bytes_sink(&mut self, sink: Box<dyn RecordBytesSink + Send>) {
        self.bytes_sink = sink;
    }

    /// Round `lsn` down to the nearest segment boundary.
    pub fn align_down(lsn: u64, seg_size: u64) -> u64 {
        lsn - (lsn % seg_size)
    }

    /// LSN of the next byte expected by [`push`](Self::push).
    pub fn next_lsn(&self) -> u64 {
        self.next_lsn
    }

    /// LSN of the highest fully-dispatched byte (one past the end of
    /// the last `on_segment` call).
    pub fn dispatched_lsn(&self) -> u64 {
        self.current_lsn
    }

    /// Append bytes that start at LSN `lsn`. Dispatches at record
    /// cadence: per-record filter+rewrite → owned `bytes_sink` (shadow
    /// wire) → `record_sink` (decoder + observers). Segment-level
    /// `segment_sink` fires at the 16 MiB boundary as the archive
    /// fallback artifact.
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
            // Bound per-iteration extend by one seg so buf growth on
            // a multi-seg `push` call stays predictable; spanning
            // records may push buf temporarily past seg_size, but
            // each iter drains + flushes back down once the record
            // completes.
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

    /// Drain every now-completable record from the walker. Per-record
    /// dispatch fires the owned `bytes_sink.on_record_bytes` first
    /// (the shadow wire) so the catalog gate `wait_for_replay
    /// (record.lsn)` in `record_sink` clears against shadow's
    /// wire-driven apply LSN rather than against `restore_command`
    /// segment landing.
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

            // Parse + decide inside an inner scope so any slice
            // borrows back into the walker buffer (`parsed.main_data`,
            // per-block `image`/`data`) live only across the filter
            // call. For the `Drop` path we materialise the parse to
            // `'static` so the subsequent `rewrite_record` can take
            // `&mut self.walker` without conflicting; the `Keep` path
            // keeps the borrow zero-copy through `record_sink`.
            let decision;
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
                decision = self.filter.decide(&parsed);
                // Materialise here: `rewrite_record` below mutates
                // walker.buf which `parsed`'s slices view; the dispatch
                // below needs the *original* parse, not post-rewrite.
                parsed_for_sink = parsed.into_owned();
            }
            let kind = match decision {
                Decision::Keep => Kind::Kept,
                Decision::Drop => Kind::Dropped,
            };
            if decision == Decision::Drop {
                let mut bytes = match completed.stitched_bytes {
                    Some(v) => v,
                    None => {
                        let (off, len) = byte_ranges[0];
                        self.walker.buffer()[off..off + len].to_vec()
                    }
                };
                noop_replace(&mut bytes).map_err(|source| WalStreamError::Rewrite {
                    offset: start_offset,
                    source,
                })?;
                self.walker.rewrite_record(&byte_ranges, &bytes);
            }

            self.pending_entries.push(Entry {
                offset: start_offset as u64,
                len: parsed_for_sink.header.total_record_length,
                rmid: parsed_for_sink.header.resource_manager_id,
                info: parsed_for_sink.header.info,
                kind,
            });

            // Frame buffer[wire_offset..record_end] as one wire chunk
            // — covers page headers + inter-record padding so the
            // shadow walreceiver sees a byte-exact stream identical
            // to what `restore_command` would land on disk.
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
                decision,
            };
            record_sink.on_record(&record).await?;
        }
    }

    /// Flush the first `seg_size` bytes of the walker buffer if a
    /// segment's worth has accumulated AND no in-flight `pending`
    /// record still straddles seg-0. Returns `true` if a seg was
    /// flushed, `false` if the precondition wasn't met.
    ///
    /// Spanning case (pending seg-0 → seg-1): we wait. Once the record
    /// completes through `drain_records`, [`rewrite_record`](StreamingWalker::rewrite_record)
    /// applies the NOOP rewrite uniformly across both segs of the
    /// walker buf, and the next `try_flush_first_segment` call ships
    /// seg-0 with rewritten partial bytes.
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
        // Split manifest entries: those with offset < seg_size belong
        // to seg-0. Remaining stay in pending_entries for seg-1's
        // future flush. Rebase remainder offsets so they're correct
        // post-truncate.
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
        // Wire tail: bytes_sink saw on_wire_chunk per-record up to
        // wire_offset. If wire_offset < seg_size, the residual span
        // (alignment pad + page header + spanning-record bytes that
        // were rewritten in-place) still needs to ship before seg-0
        // closes.
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
        // Snapshot for the next segment's manifest deltas.
        self.stats_at_segment_start = self.filter.stats;
        self.relmap_at_segment_start = self.filter.tracker.relmap_updates;
        self.pg_class_undecoded_at_segment_start = self.filter.tracker.pg_class_writes_undecoded;
        self.pg_class_oid_in_prefix_at_segment_start =
            self.filter.tracker.pg_class_writes_oid_in_prefix;
        Ok(true)
    }

    /// Force-flush the current partial segment (does nothing if empty).
    /// Use on shutdown — leaves a `.partial` segment for the operator
    /// via [`SegmentSink::on_partial_segment`] so the file is
    /// distinguishable from a complete segment and shadow PG's
    /// `restore_command` does not pick it up.
    pub async fn close(
        mut self,
        mut partial_sink: Option<&mut dyn SegmentSink>,
        record_sink: &mut dyn RecordSink,
    ) -> Result<(), WalStreamError> {
        // Drain any fully-buffered segments first — buf may exceed
        // seg_size when a record was straddling the boundary at the
        // moment shutdown fired. Each completed seg ships as a normal
        // segment file (not `.partial`).
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
        // Records already drained at push cadence; the partial buffer
        // may still contain a tail of unparsed bytes (record straddling
        // the missing remainder). Zero-pad and run filter_segment over
        // the padded buffer so the `.partial` file matches the
        // shape `pg_receivewal` would write and so any records
        // belonging entirely to this segment but yet undrained land
        // through one more pass.
        let seg = self.segment_for_lsn(self.current_lsn);
        let seg_start_lsn = self.current_lsn;
        // `close` fires once per shutdown so the local copy is
        // acceptable. Hot-path flushes hand the walker buffer to the
        // sink directly.
        let mut buf: Vec<u8> = Vec::with_capacity(self.seg_size as usize);
        buf.extend_from_slice(self.walker.buffer());
        if buf.len() < self.seg_size as usize {
            buf.resize(self.seg_size as usize, 0);
        }
        self.walker.reset_segment();
        let name = seg.format();

        // Re-run the batch filter over the padded buffer. Because we
        // already drained records via the streaming path during
        // [`push`](Self::push), this second pass picks up at most the
        // records that needed a tail-page to complete (none on the
        // graceful-shutdown path; bound by zero pad in worst case).
        let (filtered, manifest, parsed) =
            filter_segment(&buf, &name, &mut self.filter).map_err(|source| {
                WalStreamError::Filter {
                    seg: name.clone(),
                    source,
                }
            })?;
        // Streamed records already dispatched at push cadence;
        // close()'s job is to land the partial bytes on disk so a
        // follow-up daemon run can locate them. We do NOT re-dispatch
        // the records through `record_sink` (would double-fire).
        // Suppress the unused-parameter warning while keeping the
        // signature symmetric with the historical contract that
        // included a per-record path.
        let _ = parsed;
        let _ = seg_start_lsn;
        if let Some(sink) = partial_sink {
            sink.on_partial_segment(seg, &filtered, &manifest).await?;
        }
        // Records we did NOT redrive on close, since per-record
        // dispatch fires at push cadence in the streaming path. Suppress unused
        // warning on record_sink to keep callers' shape unchanged.
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
    use wal_rs::pg::walparser::RmId;

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
        use wal_rs::pg::walparser::XLogRecordHeader;
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
        assert_eq!(record.decision, Decision::Keep);
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
        use wal_rs::pg::walparser::XLogRecordHeader;
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
    async fn metrics_sink_counts_per_rm_decision_and_discards() {
        let mut sink = MetricsRecordSink::default();
        let mut heap_keep = synth_record(0, RmId::Heap as u8);
        heap_keep.decision = Decision::Keep;
        let mut heap_drop = synth_record(64, RmId::Heap as u8);
        heap_drop.decision = Decision::Drop;
        let mut xact_keep = synth_record(128, RmId::Xact as u8);
        xact_keep.decision = Decision::Keep;
        for r in [&heap_keep, &heap_keep, &heap_drop, &xact_keep] {
            sink.on_record(r).await.unwrap();
        }
        assert_eq!(sink.total, 4);
        assert_eq!(sink.by_rm_decision[&(RmId::Heap as u8, Decision::Keep)], 2,);
        assert_eq!(sink.by_rm_decision[&(RmId::Heap as u8, Decision::Drop)], 1,);
        assert_eq!(sink.by_rm_decision[&(RmId::Xact as u8, Decision::Keep)], 1,);
        let summary = sink.summary();
        assert!(summary.starts_with("total=4"), "got {summary:?}");
        assert!(summary.contains("heap/keep=2"), "got {summary:?}");
        assert!(summary.contains("heap/drop=1"), "got {summary:?}");
        assert!(summary.contains("xact/keep=1"), "got {summary:?}");
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

    /// Contract: a `RecordBytesSink` sees the full wire
    /// byte stream — every record byte image plus the page headers
    /// and inter-record padding between them. Total bytes dispatched
    /// (chunks + trailing) match seg_size exactly.
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

        // Wire chunks plus the trailing-tail chunk should sum to a
        // contiguous byte stream from offset 0 to SEG, byte-identical
        // to the segment_sink output.
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
        use wal_rs::pg::walparser::{
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
        let r1 = rec(wal_rs::pg::walparser::RmId::Xact as u8);
        let r2 = rec(wal_rs::pg::walparser::RmId::Xact as u8);
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
