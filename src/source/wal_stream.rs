//! Streaming filter pipeline. Wraps `StreamingWalker` in a
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

use thiserror::Error;
use walrus::pg::wal::segment::SegmentName;
use walrus::pg::walparser::{
    ParseError, RmId, X_LOG_RECORD_ALIGNMENT, X_LOG_SWITCH, XLogRecord, parse_record_from_bytes,
};

use crate::filter::manifest::{Entry, FILTER_VERSION, Kind, Manifest};
use crate::filter::rewrite::{RewriteError, noop_replace};
use crate::filter::{Filter, FilterSnapshot};
#[cfg(test)]
use crate::record::{
    CollectingRecordSink, CollectingSegmentSink, CompositeRecordSink, MetricsRecordSink,
    WAL_SEG_SIZE,
};
use crate::record::{
    NoopBytesSink, Record, RecordBytesSink, RecordSink, Route, SegmentSink, SinkError,
};
#[cfg(test)]
use crate::source::segment_sink::{DirSegmentSink, SegFsync};
use crate::source::streaming_walker::{CompletedRecord, StreamingWalker, WalkError};

#[derive(Debug, Error)]
pub enum WalStreamError {
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
    #[error("commit payload at offset {offset}: {source}")]
    Payload {
        offset: usize,
        #[source]
        source: crate::decode::wal_xact::XactPayloadError,
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

/// Segment-aligned record-cadence WAL filter.
///
/// Bytes pushed via [`push`](Self::push) must arrive in contiguous LSN
/// order from `base_lsn`. Owns the [`Filter`] so `CatalogTracker` state
/// (every `XLOG_RELMAP_UPDATE`, every decoded pg_class write) survives
/// segment boundaries, and the `StreamingWalker` so the per-page
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
    /// Filter snapshot at segment start, for manifest deltas
    stats_at_segment_start: FilterSnapshot,
    /// Defaults to [`NoopBytesSink`]; production swaps in
    /// [`crate::source::shadow_stream::ShadowStreamSink`].
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
            stats_at_segment_start: filter.snapshot(),
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
        record_sink: &mut (dyn RecordSink + Send),
        segment_sink: &mut (dyn SegmentSink + Send),
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

            if let Err(e) = self.drain_records(Some(record_sink), true).await {
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
        mut record_sink: Option<&mut (dyn RecordSink + Send)>,
        emit_wire: bool,
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
            let verdict;
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
                verdict = self
                    .filter
                    .decide_record(&parsed, self.current_lsn + start_offset as u64, page_magic)
                    .map_err(|source| WalStreamError::Payload {
                        offset: start_offset,
                        source,
                    })?;
                // `rewrite_record` below mutates walker.buf that `parsed`
                // views; dispatch needs the original parse, not post-rewrite.
                parsed_for_sink = parsed.into_owned();
            }
            let route = verdict.route;
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
            if emit_wire && record_end > self.wire_offset {
                let chunk = &self.walker.buffer()[self.wire_offset..record_end];
                let start_lsn = self.current_lsn + self.wire_offset as u64;
                self.bytes_sink.on_wire_chunk(start_lsn, chunk).await?;
                self.wire_offset = record_end;
            }

            let source_lsn = self.current_lsn + start_offset as u64;
            let next_lsn = self.end_rec_ptr(&last_range, &parsed_for_sink);
            let record = Record {
                parsed: parsed_for_sink,
                source_lsn,
                next_lsn,
                page_magic,
                route,
                catalog_boundary: verdict.catalog_boundary,
                boundary_info: verdict.boundary,
            };
            if let Some(sink) = record_sink.as_deref_mut() {
                sink.on_record(&record).await?;
            }
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
        segment_sink: &mut (dyn SegmentSink + Send),
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
        let manifest = self.take_manifest(&seg, seg_size);
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
        self.bytes_sink.on_segment_retired(self.current_lsn).await?;
        self.wire_offset = self.wire_offset.saturating_sub(seg_size);
        self.stats_at_segment_start = self.filter.snapshot();
        Ok(true)
    }

    fn take_manifest(&mut self, seg: &SegmentName, len: usize) -> Manifest {
        let mut records = Vec::with_capacity(self.pending_entries.len());
        let mut future = Vec::new();
        for entry in std::mem::take(&mut self.pending_entries) {
            if entry.offset < len as u64 {
                records.push(entry);
            } else {
                future.push(Entry {
                    offset: entry.offset - len as u64,
                    ..entry
                });
            }
        }
        self.pending_entries = future;
        Manifest {
            source_segment: seg.format(),
            filter_version: FILTER_VERSION,
            records,
            stats: self
                .filter
                .manifest_stats_since(self.stats_at_segment_start),
        }
    }

    /// Shutdown flush of the partial segment (no-op if empty). Lands a
    /// `.partial` via [`SegmentSink::on_partial_segment`] so shadow PG's
    /// `restore_command` doesn't pick it up as complete.
    pub async fn close(
        mut self,
        mut partial_sink: Option<&mut (dyn SegmentSink + Send)>,
        _record_sink: &mut (dyn RecordSink + Send),
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
        // Zero padding lets shared routing core finish any pending record
        let seg = self.segment_for_lsn(self.current_lsn);
        let seg_size = self.seg_size as usize;
        if self.walker.buffer_len() < seg_size {
            self.walker
                .extend(&vec![0; seg_size - self.walker.buffer_len()]);
        }
        self.drain_records(None, false).await?;
        let manifest = self.take_manifest(&seg, seg_size);
        if let Some(sink) = partial_sink {
            sink.on_partial_segment(seg, &self.walker.buffer()[..seg_size], &manifest)
                .await?;
        }
        Ok(())
    }

    /// PG `XLogReaderState::EndRecPtr` for a record whose final byte range
    /// is `last_range` (walker-buffer offset + len, base `current_lsn`).
    ///
    /// Ports xlogreader.c's arithmetic: single-page records advance
    /// `RecPtr + MAXALIGN(xl_tot_len)`; page- and segment-spanning records
    /// advance `last_page_addr + page_header_size + MAXALIGN(rem_len)` —
    /// both collapse to aligning the final range's end, since a
    /// continuation range starts at its page's aligned data offset.
    /// `XLOG_SWITCH` pretends to extend to segment end.
    fn end_rec_ptr(&self, last_range: &(usize, usize), parsed: &XLogRecord) -> u64 {
        let (off, len) = *last_range;
        let aligned_len = (len + X_LOG_RECORD_ALIGNMENT - 1) & !(X_LOG_RECORD_ALIGNMENT - 1);
        let mut end = self.current_lsn + off as u64 + aligned_len as u64;
        if parsed.header.resource_manager_id == RmId::Xlog as u8
            && parsed.header.info & 0xF0 == X_LOG_SWITCH
        {
            end += self.seg_size - 1;
            end -= end % self.seg_size;
        }
        end
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
    use crate::filter::manifest::{FILTER_VERSION, ManifestStats};
    use std::pin::Pin;
    use tokio::sync::mpsc;
    use walrus::pg::walparser::RmId;

    fn dummy_manifest() -> Manifest {
        Manifest {
            source_segment: "test".into(),
            filter_version: FILTER_VERSION,
            records: vec![],
            stats: ManifestStats::default(),
        }
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
        Record {
            parsed: XLogRecord {
                header: XLogRecordHeader {
                    resource_manager_id: rmid,
                    ..Default::default()
                },
                ..Default::default()
            },
            source_lsn: offset,
            page_magic: 0xD110,
            ..Default::default()
        }
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

        let page = synth_two_record_page();
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

    fn synth_two_record_page() -> Vec<u8> {
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
            let crc = crate::filter::rewrite::compute_crc(&v);
            v[20..24].copy_from_slice(&crc.to_le_bytes());
            v
        }
        let r1 = rec(walrus::pg::walparser::RmId::Clog as u8);
        let r2 = rec(walrus::pg::walparser::RmId::Clog as u8);
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

    /// A single record that fills two full WAL pages, so it ends exactly on
    /// the second page's boundary. With one page per segment, this straddles
    /// the seg-0/seg-1 boundary and ends flush on the seg-1/seg-2 boundary —
    /// so both segment flushes see `wire_offset >= seg_size`.
    fn synth_two_page_spanning_record() -> (Vec<u8>, Vec<u8>) {
        use walrus::pg::walparser::{
            WAL_PAGE_SIZE, X_LOG_RECORD_HEADER_SIZE, XLP_LONG_HEADER, XLP_PAGE_MAGIC_PG15,
            XLR_BLOCK_ID_DATA_LONG,
        };
        const PAGE: usize = WAL_PAGE_SIZE as usize; // 8192
        const LONG_HDR: usize = 40;
        const SHORT_HDR: usize = 24;
        let p0_data = PAGE - LONG_HDR; // 8152
        let p1_data = PAGE - SHORT_HDR; // 8168
        let total = p0_data + p1_data; // 16320, record ends at end of page1
        let main_data_len = total - X_LOG_RECORD_HEADER_SIZE - 5; // 254 marker + u32 len

        let mut rec = Vec::with_capacity(total);
        rec.extend_from_slice(&(total as u32).to_le_bytes());
        rec.extend_from_slice(&0u32.to_le_bytes()); // xid
        rec.extend_from_slice(&0u64.to_le_bytes()); // prev
        rec.push(0); // info
        rec.push(walrus::pg::walparser::RmId::Xact as u8);
        rec.push(0);
        rec.push(0);
        rec.extend_from_slice(&0u32.to_le_bytes()); // crc (unvalidated)
        rec.push(XLR_BLOCK_ID_DATA_LONG);
        rec.extend_from_slice(&(main_data_len as u32).to_le_bytes());
        rec.resize(total, 0xAA);

        let mut page0 = Vec::with_capacity(PAGE);
        page0.extend_from_slice(&XLP_PAGE_MAGIC_PG15.to_le_bytes());
        page0.extend_from_slice(&XLP_LONG_HEADER.to_le_bytes());
        page0.extend_from_slice(&1u32.to_le_bytes()); // tli
        page0.extend_from_slice(&0u64.to_le_bytes()); // page_addr
        page0.extend_from_slice(&0u32.to_le_bytes()); // rem_len (record starts here)
        page0.extend_from_slice(&12345u64.to_le_bytes()); // sysid
        page0.extend_from_slice(&(8192u32 * 1024).to_le_bytes());
        page0.extend_from_slice(&8192u32.to_le_bytes());
        page0.extend_from_slice(&[0u8; 4]);
        page0.extend_from_slice(&rec[..p0_data]);
        assert_eq!(page0.len(), PAGE);

        let mut page1 = Vec::with_capacity(PAGE);
        page1.extend_from_slice(&XLP_PAGE_MAGIC_PG15.to_le_bytes());
        page1.extend_from_slice(&0u16.to_le_bytes()); // short header
        page1.extend_from_slice(&1u32.to_le_bytes()); // tli
        page1.extend_from_slice(&(PAGE as u64).to_le_bytes()); // page_addr
        page1.extend_from_slice(&(p1_data as u32).to_le_bytes()); // rem_len (continuation on this page)
        page1.extend_from_slice(&[0u8; 4]); // pad to 24
        page1.extend_from_slice(&rec[p0_data..]);
        assert_eq!(page1.len(), PAGE);

        (page0, page1)
    }

    /// General record-bytes builder: optional block ref (no data) +
    /// optional short main_data, CRC patched like `synth_xact_page`.
    fn raw_rec(
        rmid: u8,
        info: u8,
        xid: u32,
        block: Option<(u32, u32, u32)>,
        main_data: Option<&[u8]>,
    ) -> Vec<u8> {
        use walrus::pg::walparser::XLR_BLOCK_ID_DATA_SHORT;
        let mut v = Vec::new();
        v.extend_from_slice(&0u32.to_le_bytes()); // tot_len backpatched
        v.extend_from_slice(&xid.to_le_bytes());
        v.extend_from_slice(&0u64.to_le_bytes()); // prev
        v.push(info);
        v.push(rmid);
        v.push(0);
        v.push(0);
        v.extend_from_slice(&0u32.to_le_bytes()); // crc backpatched
        if let Some((spc, db, rel)) = block {
            v.push(0); // block_id 0
            v.push(0); // fork_flags: main fork, no image/data
            v.extend_from_slice(&0u16.to_le_bytes()); // data_length
            v.extend_from_slice(&spc.to_le_bytes());
            v.extend_from_slice(&db.to_le_bytes());
            v.extend_from_slice(&rel.to_le_bytes());
            v.extend_from_slice(&0u32.to_le_bytes()); // block_no
        }
        if let Some(md) = main_data {
            v.push(XLR_BLOCK_ID_DATA_SHORT);
            v.push(md.len() as u8);
            v.extend_from_slice(md);
        }
        let total = v.len() as u32;
        v[0..4].copy_from_slice(&total.to_le_bytes());
        let crc = crate::filter::rewrite::compute_crc(&v);
        v[20..24].copy_from_slice(&crc.to_le_bytes());
        v
    }

    fn page_of(records: &[Vec<u8>], page_len: usize) -> Vec<u8> {
        use walrus::pg::walparser::{XLP_LONG_HEADER, XLP_PAGE_MAGIC_PG15};
        let mut page = Vec::with_capacity(page_len);
        page.extend_from_slice(&XLP_PAGE_MAGIC_PG15.to_le_bytes());
        page.extend_from_slice(&XLP_LONG_HEADER.to_le_bytes());
        page.extend_from_slice(&1u32.to_le_bytes()); // timeline
        page.extend_from_slice(&0u64.to_le_bytes()); // page_address
        page.extend_from_slice(&0u32.to_le_bytes()); // remaining_data_len
        page.extend_from_slice(&12345u64.to_le_bytes()); // sysid
        page.extend_from_slice(&(page_len as u32).to_le_bytes()); // seg_size
        page.extend_from_slice(&8192u32.to_le_bytes()); // blcksz
        page.extend_from_slice(&[0u8; 4]); // pad to 40
        for r in records {
            page.extend_from_slice(r);
            let pad = (8 - (page.len() % 8)) % 8;
            page.extend(std::iter::repeat_n(0u8, pad));
        }
        page.resize(page_len, 0);
        page
    }

    /// PG EndRecPtr: single-page records advance by MAXALIGN(xl_tot_len);
    /// an unaligned length (30 → 32) must not leak into `next_lsn`.
    #[tokio::test(flavor = "current_thread")]
    async fn next_lsn_advances_by_maxaligned_total_len() {
        const SEG: u64 = 8192;
        let mut ws = WalStream::new(1, SEG, 0).unwrap();
        let mut rec = CollectingRecordSink::default();
        let mut seg = CollectingSegmentSink::default();
        ws.push(0, &synth_two_record_page(), &mut rec, &mut seg)
            .await
            .unwrap();
        // synth records are 30 bytes at offsets 40 and 72
        assert_eq!(rec.records[0].source_lsn, 40);
        assert_eq!(rec.records[0].next_lsn, 72);
        assert_eq!(rec.records[1].source_lsn, 72);
        assert_eq!(rec.records[1].next_lsn, 104);
    }

    /// Page-spanning record: EndRecPtr = continuation-page data start +
    /// MAXALIGN(rem_len), NOT record start + MAXALIGN(total) (page header
    /// bytes intervene).
    #[tokio::test(flavor = "current_thread")]
    async fn next_lsn_cross_page_record_uses_continuation_page_arithmetic() {
        use walrus::pg::walparser::{WAL_PAGE_SIZE, X_LOG_RECORD_HEADER_SIZE, XLP_PAGE_MAGIC_PG15};
        const PAGE: usize = WAL_PAGE_SIZE as usize;
        const SEG: u64 = 2 * PAGE as u64; // both pages in one segment
        // 8200-byte record from page-0 offset 40: 8152 on page 0, 48 on
        // page 1 whose data starts at PAGE + 24 (short header)
        let total = 8200usize;
        let main_data_len = total - X_LOG_RECORD_HEADER_SIZE - 5;
        let mut record = Vec::with_capacity(total);
        record.extend_from_slice(&(total as u32).to_le_bytes());
        record.extend_from_slice(&0u32.to_le_bytes());
        record.extend_from_slice(&0u64.to_le_bytes());
        record.push(0);
        record.push(RmId::Xact as u8);
        record.push(0);
        record.push(0);
        record.extend_from_slice(&0u32.to_le_bytes());
        record.push(walrus::pg::walparser::XLR_BLOCK_ID_DATA_LONG);
        record.extend_from_slice(&(main_data_len as u32).to_le_bytes());
        record.resize(total, 0xAA);

        let p0_data = PAGE - 40;
        let mut page0 = page_of(&[], PAGE);
        page0.truncate(40);
        page0.extend_from_slice(&record[..p0_data]);

        let mut page1 = Vec::with_capacity(PAGE);
        page1.extend_from_slice(&XLP_PAGE_MAGIC_PG15.to_le_bytes());
        page1.extend_from_slice(&0u16.to_le_bytes()); // short header
        page1.extend_from_slice(&1u32.to_le_bytes());
        page1.extend_from_slice(&(PAGE as u64).to_le_bytes());
        page1.extend_from_slice(&((total - p0_data) as u32).to_le_bytes()); // rem_len = 48
        page1.extend_from_slice(&[0u8; 4]); // pad to 24
        page1.extend_from_slice(&record[p0_data..]);
        page1.resize(PAGE, 0);

        let mut ws = WalStream::new(1, SEG, 0).unwrap();
        let mut rec = CollectingRecordSink::default();
        let mut seg = CollectingSegmentSink::default();
        ws.push(0, &page0, &mut rec, &mut seg).await.unwrap();
        assert!(rec.records.is_empty());
        ws.push(PAGE as u64, &page1, &mut rec, &mut seg)
            .await
            .unwrap();
        assert_eq!(rec.records[0].source_lsn, 40);
        // PAGE + 24 (continuation data start) + MAXALIGN(48)
        assert_eq!(rec.records[0].next_lsn, PAGE as u64 + 24 + 48);
    }

    /// Segment-spanning record ending flush on a page boundary: EndRecPtr
    /// is exactly the boundary address.
    #[tokio::test(flavor = "current_thread")]
    async fn next_lsn_segment_spanning_record_ends_on_boundary() {
        const SEG: u64 = walrus::pg::walparser::WAL_PAGE_SIZE as u64;
        let mut ws = WalStream::new(1, SEG, 0).unwrap();
        let mut rec = CollectingRecordSink::default();
        let mut seg = CollectingSegmentSink::default();
        let (page0, page1) = synth_two_page_spanning_record();
        ws.push(0, &page0, &mut rec, &mut seg).await.unwrap();
        ws.push(SEG, &page1, &mut rec, &mut seg).await.unwrap();
        assert_eq!(rec.records.len(), 1);
        assert_eq!(rec.records[0].next_lsn, 2 * SEG);
    }

    /// XLOG_SWITCH pretends to extend to segment end.
    #[tokio::test(flavor = "current_thread")]
    async fn next_lsn_xlog_switch_advances_to_segment_end() {
        const SEG: u64 = 8192;
        let switch = raw_rec(RmId::Xlog as u8, X_LOG_SWITCH, 0, None, None);
        let page = page_of(&[switch], SEG as usize);
        let mut ws = WalStream::new(1, SEG, 0).unwrap();
        let mut rec = CollectingRecordSink::default();
        let mut seg = CollectingSegmentSink::default();
        ws.push(0, &page, &mut rec, &mut seg).await.unwrap();
        assert_eq!(rec.records[0].source_lsn, 40);
        assert_eq!(rec.records[0].next_lsn, SEG);
    }

    /// Bare-DDL commit (catalog write + commit, no replicated rows) parks
    /// the pump: successor bytes reach neither the shadow wire nor the
    /// segment sink until shadow's apply LSN passes the commit's
    /// `next_lsn`; then everything flows and the hold releases exactly once.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn catalog_commit_parks_pump_until_shadow_replay() {
        use crate::record::CountingRecordSink;
        use crate::source::boundary_hold::{
            BoundaryGateConfig, BoundaryHoldSink, CatalogBoundaryGate,
        };
        use crate::source::queueing_record_sink::QueueingRecordSink;
        use crate::source::shadow_stream::ShadowStreamState;
        use std::sync::Arc;
        use std::time::Duration;
        use tokio::sync::Mutex;

        const SEG: u64 = 8192;
        // A: catalog heap insert (pg_class) xid 7 — 44 bytes at 40
        let a = raw_rec(RmId::Heap as u8, 0x00, 7, Some((1663, 5, 1259)), None);
        assert_eq!(a.len(), 44);
        // B: commit xid 7 — 34 bytes at 88, EndRecPtr 128
        let b = raw_rec(RmId::Xact as u8, 0x00, 7, None, Some(&0i64.to_le_bytes()));
        assert_eq!(b.len(), 34);
        // C: user heap insert xid 8 — successor bytes that must hold
        let c = raw_rec(RmId::Heap as u8, 0x00, 8, Some((1663, 5, 50000)), None);
        let page = page_of(&[a, b, c], SEG as usize);
        let (b_wire_end, b_next_lsn, c_wire_end) = (40 + 44 + 4 + 34, 128u64, 128 + 44);

        let state = Arc::new(Mutex::new(ShadowStreamState::new(1, "sys".into(), 0, 1024)));
        let conn = state.lock().await.register_connection(0);

        type Chunks = Arc<std::sync::Mutex<Vec<(u64, usize)>>>;
        let chunks: Chunks = Arc::default();
        struct SpanLog(Chunks);
        impl RecordBytesSink for SpanLog {
            fn on_wire_chunk<'a>(
                &'a mut self,
                start_lsn: u64,
                bytes: &'a [u8],
            ) -> Pin<Box<dyn Future<Output = Result<(), SinkError>> + Send + 'a>> {
                self.0.lock().unwrap().push((start_lsn, bytes.len()));
                Box::pin(std::future::ready(Ok(())))
            }
        }

        let mut ws = WalStream::new(1, SEG, 0).unwrap();
        ws.set_bytes_sink(Box::new(SpanLog(chunks.clone())));
        let q = QueueingRecordSink::spawn(CountingRecordSink::default(), 64, 1024, None);
        let gate = CatalogBoundaryGate::new(
            state.clone(),
            BoundaryGateConfig {
                hold_timeout: Duration::from_secs(10),
                poll_interval: Duration::from_millis(1),
            },
        );
        let stats = gate.stats.clone();
        let mut sink = BoundaryHoldSink::new(q, gate);
        let pump = tokio::spawn(async move {
            let mut seg = CollectingSegmentSink::default();
            ws.push(0, &page, &mut sink, &mut seg).await.unwrap();
            (sink, seg.segments.len())
        });

        // Wait for the pump to park: commit B's wire chunk dispatched…
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            let max_end = chunks
                .lock()
                .unwrap()
                .iter()
                .map(|(s, l)| s + *l as u64)
                .max()
                .unwrap_or(0);
            if max_end >= b_wire_end as u64 {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "commit bytes never reached the wire",
            );
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        // …and successor C's withheld while apply lags.
        tokio::time::sleep(Duration::from_millis(20)).await;
        let max_end = chunks
            .lock()
            .unwrap()
            .iter()
            .map(|(s, l)| s + *l as u64)
            .max()
            .unwrap();
        assert_eq!(
            max_end, b_wire_end as u64,
            "successor bytes published during hold",
        );
        assert_eq!(stats.holds.load(std::sync::atomic::Ordering::Relaxed), 0);

        // Shadow replays through the commit's EndRecPtr → release.
        state
            .lock()
            .await
            .observe_status(conn, b_next_lsn, b_next_lsn, b_next_lsn);
        let (sink, segs_shipped) = pump.await.unwrap();
        assert_eq!(stats.holds.load(std::sync::atomic::Ordering::Relaxed), 1);
        assert_eq!(stats.failures.load(std::sync::atomic::Ordering::Relaxed), 0);
        let max_end = chunks
            .lock()
            .unwrap()
            .iter()
            .map(|(s, l)| s + *l as u64)
            .max()
            .unwrap();
        assert!(
            max_end >= c_wire_end as u64,
            "successor flowed after release"
        );
        assert_eq!(segs_shipped, 1, "segment shipped only after release");
        sink.close().await.unwrap();
    }

    /// DML-only commit never parks: push completes with no walreceiver
    /// attached and no apply progress at all.
    #[tokio::test(flavor = "current_thread")]
    async fn dml_only_commit_does_not_park() {
        use crate::record::CountingRecordSink;
        use crate::source::boundary_hold::{
            BoundaryGateConfig, BoundaryHoldSink, CatalogBoundaryGate,
        };
        use crate::source::queueing_record_sink::QueueingRecordSink;
        use crate::source::shadow_stream::ShadowStreamState;
        use std::sync::Arc;
        use std::time::Duration;
        use tokio::sync::Mutex;

        const SEG: u64 = 8192;
        let ins = raw_rec(RmId::Heap as u8, 0x00, 8, Some((1663, 5, 50000)), None);
        let commit = raw_rec(RmId::Xact as u8, 0x00, 8, None, Some(&0i64.to_le_bytes()));
        let page = page_of(&[ins, commit], SEG as usize);

        let state = Arc::new(Mutex::new(ShadowStreamState::new(1, "sys".into(), 0, 1024)));
        let mut ws = WalStream::new(1, SEG, 0).unwrap();
        let q = QueueingRecordSink::spawn(CountingRecordSink::default(), 4, 16, None);
        let gate = CatalogBoundaryGate::new(state, BoundaryGateConfig::default());
        let stats = gate.stats.clone();
        let mut sink = BoundaryHoldSink::new(q, gate);
        let mut seg = CollectingSegmentSink::default();
        tokio::time::timeout(
            Duration::from_secs(5),
            ws.push(0, &page, &mut sink, &mut seg),
        )
        .await
        .expect("DML-only commit must not park")
        .unwrap();
        assert_eq!(stats.holds.load(std::sync::atomic::Ordering::Relaxed), 0);
        sink.close().await.unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn shadow_wire_buf_bounded_when_record_straddles_segment() {
        use crate::source::shadow_stream::{ShadowStreamSink, ShadowStreamState};
        use std::sync::Arc;
        use tokio::sync::Mutex;

        const SEG: u64 = walrus::pg::walparser::WAL_PAGE_SIZE as u64;
        let state = Arc::new(Mutex::new(ShadowStreamState::new(
            1,
            "sys".into(),
            0,
            64 * 1024 * 1024,
        )));
        let mut ws = WalStream::new(1, SEG, 0).unwrap();
        ws.set_bytes_sink(Box::new(ShadowStreamSink::new(state.clone())));
        let mut rec = CollectingRecordSink::default();
        let mut seg = CollectingSegmentSink::default();

        let (page0, page1) = synth_two_page_spanning_record();
        ws.push(0, &page0, &mut rec, &mut seg).await.unwrap();
        ws.push(SEG, &page1, &mut rec, &mut seg).await.unwrap();

        let len = state.lock().await.wire_buf_len() as u64;
        assert!(
            len <= SEG,
            "wire_buf retained {len} bytes across a straddling boundary (> {SEG})",
        );
    }
}
