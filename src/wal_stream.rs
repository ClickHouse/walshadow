//! Streaming filter pipeline. Wraps the batch [`filter_segment`] in a
//! segment-aligned accumulator that consumes arbitrary WAL byte chunks
//! (from wal-rs's `START_REPLICATION` CopyData stream) and dispatches
//! to per-record + per-segment sinks.
//!
//! Segment-level batching matches `pg_receivewal`'s storage cadence:
//! shadow PG's `restore_command` needs full segment files, so writing
//! sub-segment partials adds no headroom. Within a segment, records
//! see no extra latency from this design — they reach the
//! [`RecordSink`] as soon as the batch filter processes the segment.
//!
//! For per-record streaming (sub-segment latency for the decoder), a
//! future revision can switch [`WalStream::push`] from "accumulate
//! whole segment then call `filter_segment`" to a chunk-driven walker
//! that yields records as soon as they complete. The sink protocol
//! defined here is shape-compatible with both.
//!
//! ## Architecture
//!
//! ```text
//!   wal-rs CopyData('w') chunks
//!              v
//!     +-----------------+
//!     |  WalStream::push|  base_lsn aligned to segment boundary
//!     +--------+--------+  buffers bytes until segment full
//!              | segment complete (WAL_SEG_SIZE bytes accumulated)
//!              v
//!     filter_segment(bytes) -> (filtered_bytes, manifest)
//!              |
//!         dispatch
//!         /         \
//!        v           v
//!   RecordSink   SegmentSink
//!   (decoder)    (shadow PG pg_wal/, manifest sidecar)
//! ```

use std::collections::BTreeMap;

use thiserror::Error;
use wal_rs::pg::wal::segment::SegmentName;
use wal_rs::pg::walparser::XLogRecord;

use crate::classify::rmgr_label;
use crate::filter::{Decision, Filter};
use crate::filter_segment::{FilterSegmentError, ParsedRecord, filter_segment};
use crate::manifest::{Kind, Manifest};

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
/// computed. Phase 5's heap-tuple decoder reads `parsed.header.xact_id`,
/// `parsed.blocks[i].header.location.rel`, and `parsed.main_data`;
/// `page_magic` selects PG-15-vs-PG-14 FPI bit semantics.
#[derive(Debug, Clone)]
pub struct Record {
    pub parsed: XLogRecord,
    /// Absolute source LSN where the record begins.
    pub source_lsn: u64,
    /// Magic of the page whose data area the record header sat on.
    pub page_magic: u16,
    /// Keep/drop decision the filter computed.
    pub decision: Decision,
}

impl Record {
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

/// Sink that observes every record decided by the filter. Phase 5's
/// heap-tuple decoder attaches here.
///
/// **Error contract.** Returning `Err` from any sink call poisons the
/// owning [`WalStream`]: `next_lsn` and the partially-dispatched
/// `current_buf` are left as-is, and every subsequent
/// [`WalStream::push`] returns [`WalStreamError::Poisoned`]. Recovery
/// is to construct a fresh `WalStream` at the desired resume LSN —
/// the protocol does not roll back per-record state, so a sink that
/// wants exactly-once semantics must commit its own state durably
/// before returning `Ok`. See [PRE5b10.md](../plans/PRE5b10.md) item 4.
pub trait RecordSink {
    fn on_record(&mut self, record: &Record) -> Result<(), SinkError>;
}

/// Sink that receives one fully-filtered segment at a time. Shadow PG
/// consumes filtered segments via `restore_command`; the production
/// sink writes the bytes plus a manifest sidecar to that directory.
pub trait SegmentSink {
    fn on_segment(
        &mut self,
        seg: SegmentName,
        bytes: &[u8],
        manifest: &Manifest,
    ) -> Result<(), SinkError>;

    /// Receives a partial segment flushed at shutdown by
    /// [`WalStream::close`]. Default forwards to [`on_segment`] for
    /// test sinks that don't care; production [`DirSegmentSink`]
    /// overrides to land bytes under `<name>.partial` per
    /// `pg_receivewal` convention so a follow-up daemon run does not
    /// confuse a partial for a complete segment.
    fn on_partial_segment(
        &mut self,
        seg: SegmentName,
        bytes: &[u8],
        manifest: &Manifest,
    ) -> Result<(), SinkError> {
        self.on_segment(seg, bytes, manifest)
    }
}

/// In-memory `RecordSink` for tests. Stores every record.
#[derive(Debug, Default)]
pub struct CollectingRecordSink {
    pub records: Vec<Record>,
}

impl RecordSink for CollectingRecordSink {
    fn on_record(&mut self, record: &Record) -> Result<(), SinkError> {
        self.records.push(record.clone());
        Ok(())
    }
}

/// Light-weight `RecordSink` that only counts. Pairs with
/// [`CollectingRecordSink`] under [`CompositeRecordSink`] when a test
/// needs to prove a second observer fired without holding clones.
#[derive(Debug, Default)]
pub struct CountingRecordSink {
    pub count: u64,
}

impl RecordSink for CountingRecordSink {
    fn on_record(&mut self, _record: &Record) -> Result<(), SinkError> {
        self.count += 1;
        Ok(())
    }
}

/// `RecordSink` that maintains a per-`(rmid, decision)` counter and
/// discards the record. Production daemon binary uses this in place of
/// [`CollectingRecordSink`] to avoid an unbounded `Vec<Record>` over a
/// long-running stream. Cumulative; reset by replacing the sink.
#[derive(Debug, Default)]
pub struct MetricsRecordSink {
    /// (rmid, decision) → count. `BTreeMap` so the on-emit formatted
    /// line orders rmgrs deterministically.
    pub by_rm_decision: BTreeMap<(u8, Decision), u64>,
    pub total: u64,
}

impl MetricsRecordSink {
    /// Stable single-line summary suitable for periodic logging on
    /// segment emit. Empty buckets are skipped; rmgrs are reported by
    /// human-readable label via [`rmgr_label`]. `total` mirrors the
    /// `records=` counter the binary used to print from
    /// `CollectingRecordSink.records.len()`.
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
    fn on_record(&mut self, record: &Record) -> Result<(), SinkError> {
        let rm = record.parsed.header.resource_manager_id;
        *self
            .by_rm_decision
            .entry((rm, record.decision))
            .or_insert(0) += 1;
        self.total += 1;
        Ok(())
    }
}

/// Fan-out `RecordSink` that dispatches each record to a chain of inner
/// sinks. Phase 5's heap-tuple `DecoderSink` rides alongside the
/// segment writer through this surface; a metrics tap or oracle probe
/// can join the same chain without changing [`WalStream::push`].
///
/// Dispatches in `inner` order and short-circuits on the first `Err`.
/// **Post-error state**: inner sinks before the failing one have
/// observed the record, the failing sink may have observed it
/// partially, sinks after the failing one have not. The error
/// propagates as [`WalStreamError::Sink`] and `WalStream` treats the
/// stream as poisoned — do not call [`WalStream::push`] again on the
/// same instance. See `plans/PRE5b10.md` item 4 for the broader
/// `next_lsn` rollback policy.
pub struct CompositeRecordSink {
    pub inner: Vec<Box<dyn RecordSink + Send>>,
}

impl CompositeRecordSink {
    pub fn new(inner: Vec<Box<dyn RecordSink + Send>>) -> Self {
        Self { inner }
    }
}

impl RecordSink for CompositeRecordSink {
    fn on_record(&mut self, record: &Record) -> Result<(), SinkError> {
        for s in &mut self.inner {
            s.on_record(record)?;
        }
        Ok(())
    }
}

/// In-memory `SegmentSink` for tests + smoke fixtures. Concatenates
/// every observed segment so callers can re-parse the output.
#[derive(Debug, Default)]
pub struct CollectingSegmentSink {
    pub segments: Vec<(SegmentName, Vec<u8>, Manifest)>,
}

impl SegmentSink for CollectingSegmentSink {
    fn on_segment(
        &mut self,
        seg: SegmentName,
        bytes: &[u8],
        manifest: &Manifest,
    ) -> Result<(), SinkError> {
        self.segments.push((seg, bytes.to_vec(), manifest.clone()));
        Ok(())
    }
}

/// Production segment sink that writes filtered segments + manifests
/// to a target directory. Shadow PG's `restore_command` reads from
/// the same directory.
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
    fn on_segment(
        &mut self,
        seg: SegmentName,
        bytes: &[u8],
        manifest: &Manifest,
    ) -> Result<(), SinkError> {
        let name = seg.format();
        let seg_path = self.out_dir.join(&name);
        let tmp = seg_path.with_extension("partial");
        std::fs::write(&tmp, bytes)?;
        std::fs::rename(&tmp, &seg_path)?;
        let mani_path = self.out_dir.join(format!("{name}.manifest.json"));
        let mani_tmp = mani_path.with_extension("manifest.json.partial");
        let f = std::fs::File::create(&mani_tmp)?;
        serde_json::to_writer_pretty(f, manifest)?;
        std::fs::rename(&mani_tmp, &mani_path)?;
        Ok(())
    }

    /// Partial-on-shutdown lands at `<name>.partial` (with
    /// `<name>.partial.manifest.json` alongside) so shadow PG's
    /// `restore_command` — which matches by exact segment name — does
    /// not pick it up as a complete segment. Operator-facing artifact;
    /// resume on the next daemon run is via `--start-lsn` at the same
    /// segment boundary, not by reading the `.partial` bytes back.
    fn on_partial_segment(
        &mut self,
        seg: SegmentName,
        bytes: &[u8],
        manifest: &Manifest,
    ) -> Result<(), SinkError> {
        let name = seg.format();
        let partial_path = self.out_dir.join(format!("{name}.partial"));
        let tmp = self.out_dir.join(format!("{name}.partial.tmp"));
        std::fs::write(&tmp, bytes)?;
        std::fs::rename(&tmp, &partial_path)?;
        let mani_path = self.out_dir.join(format!("{name}.partial.manifest.json"));
        let mani_tmp = self
            .out_dir
            .join(format!("{name}.partial.manifest.json.tmp"));
        let f = std::fs::File::create(&mani_tmp)?;
        serde_json::to_writer_pretty(f, manifest)?;
        std::fs::rename(&mani_tmp, &mani_path)?;
        Ok(())
    }
}

/// Segment-aligned accumulator. Bytes pushed via [`push`](Self::push)
/// must arrive in contiguous LSN order starting from [`base_lsn`](Self::base_lsn).
///
/// Owns the long-lived [`Filter`]: `CatalogTracker` state — every
/// `XLOG_RELMAP_UPDATE`, every decoded pg_class write — must survive
/// segment boundaries, otherwise a relmap in segment N gets forgotten
/// before the heap write it authorised lands in segment N+1.
pub struct WalStream {
    timeline: u32,
    seg_size: u64,
    /// LSN of the next byte expected by [`push`](Self::push). Advances
    /// by `bytes.len()` on every successful push.
    next_lsn: u64,
    /// LSN of the byte at `current_buf[0]`. Always segment-aligned.
    current_lsn: u64,
    current_buf: Vec<u8>,
    filter: Filter,
    /// Set by [`push`](Self::push) when filter or sink dispatch fails.
    /// Subsequent calls short-circuit with [`WalStreamError::Poisoned`].
    /// The plan does not roll `next_lsn` back on error (see [PRE5b10.md
    /// ](../plans/PRE5b10.md) item 4); a fresh `WalStream` at the
    /// caller's chosen resume LSN is the recovery path.
    poisoned: bool,
}

impl WalStream {
    /// `start_lsn` must be aligned to `seg_size`. Use [`align_down`] /
    /// [`align_up`] if you have a non-aligned source LSN.
    pub fn new(timeline: u32, seg_size: u64, start_lsn: u64) -> Result<Self, WalStreamError> {
        if !start_lsn.is_multiple_of(seg_size) {
            return Err(WalStreamError::UnalignedBase(start_lsn));
        }
        Ok(Self {
            timeline,
            seg_size,
            next_lsn: start_lsn,
            current_lsn: start_lsn,
            current_buf: Vec::with_capacity(seg_size as usize),
            filter: Filter::new(),
            poisoned: false,
        })
    }

    /// Borrow the long-lived filter. Stats here are cumulative across
    /// every segment processed by this stream.
    pub fn filter(&self) -> &Filter {
        &self.filter
    }

    /// Mutable access for pre-stream setup: callers seed
    /// `filter.tracker` (eg [`CatalogTracker::seed_from_source`]) before
    /// any [`push`](Self::push). Not for hot-path use.
    pub fn filter_mut(&mut self) -> &mut Filter {
        &mut self.filter
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
    /// the last `on_segment` call). Stays at `start_lsn` until the
    /// first segment fills.
    pub fn dispatched_lsn(&self) -> u64 {
        self.current_lsn
    }

    /// Append bytes that start at LSN `lsn`. Calls `record_sink` /
    /// `segment_sink` synchronously once a segment fills. Returns the
    /// LSN of the next expected push.
    ///
    /// **Latency contract.** Per-segment, not per-record. A pushed
    /// record sees no [`RecordSink`] call until enough subsequent
    /// pushes accumulate `seg_size` bytes (16 MiB default), at which
    /// point every record in that segment fires in one burst. Phase 5's
    /// decoder tolerates segment-cadence latency. Phase 7's CH-native
    /// emitter will not — see [PRE5b10.md](../plans/PRE5b10.md) item 2:
    /// switching to a chunk-driven walker that yields records on the
    /// fly is the deferred refactor, the sink trait shape is already
    /// compatible.
    ///
    /// **Poisoned-on-error.** A sink (record or segment) returning
    /// `Err`, or a filter/parse failure inside `flush_current`, marks
    /// the stream as poisoned. Every subsequent call returns
    /// [`WalStreamError::Poisoned`]. `next_lsn` is not rolled back: the
    /// caller's recovery is to drop this `WalStream` and construct a
    /// fresh one at the desired resume LSN. See [PRE5b10.md
    /// ](../plans/PRE5b10.md) item 4.
    pub fn push(
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
        while !data.is_empty() {
            let space = self.seg_size as usize - self.current_buf.len();
            let take = space.min(data.len());
            self.current_buf.extend_from_slice(&data[..take]);
            cur_lsn += take as u64;
            data = &data[take..];
            if self.current_buf.len() == self.seg_size as usize
                && let Err(e) = self.flush_current(record_sink, segment_sink)
            {
                self.poisoned = true;
                return Err(e);
            }
        }
        self.next_lsn = cur_lsn;
        Ok(cur_lsn)
    }

    /// Force-flush the current partial segment (does nothing if empty).
    /// Use on shutdown — leaves a `.partial` segment for the operator
    /// via [`SegmentSink::on_partial_segment`] so the file is
    /// distinguishable from a complete segment and shadow PG's
    /// `restore_command` does not pick it up.
    pub fn close(
        mut self,
        partial_sink: Option<&mut dyn SegmentSink>,
        record_sink: &mut dyn RecordSink,
    ) -> Result<(), WalStreamError> {
        if self.current_buf.is_empty() {
            return Ok(());
        }
        let seg = self.segment_for_lsn(self.current_lsn);
        let seg_start_lsn = self.current_lsn;
        // Pad to full size so filter_segment can walk it; pages past
        // the actual write are zeroed (matching pg_receivewal partial
        // segment semantics).
        let pad = self.seg_size as usize - self.current_buf.len();
        self.current_buf.extend(std::iter::repeat_n(0u8, pad));
        let name = seg.format();
        let (filtered, manifest, parsed) =
            filter_segment(&self.current_buf, &name, &mut self.filter).map_err(|source| {
                WalStreamError::Filter {
                    seg: name.clone(),
                    source,
                }
            })?;
        for (entry, parsed) in manifest.records.iter().zip(parsed) {
            let record = Record::from_parsed(seg_start_lsn, parsed, entry);
            record_sink.on_record(&record)?;
        }
        if let Some(sink) = partial_sink {
            sink.on_partial_segment(seg, &filtered, &manifest)?;
        }
        Ok(())
    }

    /// Internal: dispatch the just-filled `current_buf` and reset.
    fn flush_current(
        &mut self,
        record_sink: &mut dyn RecordSink,
        segment_sink: &mut dyn SegmentSink,
    ) -> Result<(), WalStreamError> {
        let seg = self.segment_for_lsn(self.current_lsn);
        let seg_start_lsn = self.current_lsn;
        let name = seg.format();
        let (filtered, manifest, parsed) =
            filter_segment(&self.current_buf, &name, &mut self.filter).map_err(|source| {
                WalStreamError::Filter {
                    seg: name.clone(),
                    source,
                }
            })?;
        for (entry, parsed) in manifest.records.iter().zip(parsed) {
            let record = Record::from_parsed(seg_start_lsn, parsed, entry);
            record_sink.on_record(&record)?;
        }
        segment_sink.on_segment(seg, &filtered, &manifest)?;
        self.current_lsn += self.seg_size;
        self.current_buf.clear();
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

    #[test]
    fn push_misaligned_errors() {
        let mut ws = WalStream::new(1, WAL_SEG_SIZE, 0).unwrap();
        let mut rec = CollectingRecordSink::default();
        let mut seg = CollectingSegmentSink::default();
        let err = ws
            .push(0x100, &[0u8; 1], &mut rec, &mut seg)
            .expect_err("misaligned push must error");
        match err {
            WalStreamError::Misaligned { expected, got } => {
                assert_eq!(expected, 0);
                assert_eq!(got, 0x100);
            }
            _ => panic!("wrong error variant"),
        }
    }

    /// Adversarial segment sink that returns `Err` on every dispatch.
    /// Used to prove the poison-on-error contract fires for sink
    /// failures, not just filter failures.
    struct ErrSegmentSink;
    impl SegmentSink for ErrSegmentSink {
        fn on_segment(
            &mut self,
            _seg: SegmentName,
            _bytes: &[u8],
            _manifest: &Manifest,
        ) -> Result<(), SinkError> {
            Err(SinkError::Other("synthetic segment-sink fail".into()))
        }
    }

    /// Zero-byte page → filter_segment yields no records but still
    /// dispatches the segment; `ErrSegmentSink` returns `Err` → poison.
    #[test]
    fn push_segment_sink_error_poisons_stream() {
        const SEG: u64 = 8192;
        let mut ws = WalStream::new(1, SEG, 0).unwrap();
        let mut rec = CollectingRecordSink::default();
        let mut seg = ErrSegmentSink;
        let bytes = vec![0u8; SEG as usize];
        let err = ws
            .push(0, &bytes, &mut rec, &mut seg)
            .expect_err("sink error must propagate");
        assert!(matches!(err, WalStreamError::Sink(_)), "{err:?}");
        let mut good_seg = CollectingSegmentSink::default();
        let err2 = ws
            .push(SEG, &[0u8; 1], &mut rec, &mut good_seg)
            .expect_err("subsequent push must short-circuit");
        assert!(matches!(err2, WalStreamError::Poisoned));
    }

    /// Page-sized seg with bad magic → first push fills the segment,
    /// `flush_current` calls `filter_segment` which surfaces a walker
    /// error. Stream is poisoned; the next push must short-circuit
    /// with `Poisoned` regardless of its own validity.
    #[test]
    fn push_filter_error_poisons_stream() {
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
            .expect_err("filter error must propagate");
        assert!(matches!(err, WalStreamError::Filter { .. }), "{err:?}");
        let err2 = ws
            .push(SEG, &[0u8; 1], &mut rec, &mut seg)
            .expect_err("subsequent push must short-circuit");
        assert!(matches!(err2, WalStreamError::Poisoned));
    }

    /// Synthesise a `Record` directly so the fan-out tests exercise
    /// `CompositeRecordSink::on_record` without spinning up
    /// `filter_segment`. Caller picks `offset` + `rmid` so a sequence
    /// is ordered-distinguishable.
    fn synth_record(offset: u64, rmid: u8) -> Record {
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

    /// Inner sink that records each observed rmid into a shared vec
    /// so the test can retain an observation handle while the boxed
    /// trait object is moved into [`CompositeRecordSink`].
    struct SharedRmidLog(std::sync::Arc<std::sync::Mutex<Vec<u8>>>);

    impl RecordSink for SharedRmidLog {
        fn on_record(&mut self, r: &Record) -> Result<(), SinkError> {
            self.0
                .lock()
                .unwrap()
                .push(r.parsed.header.resource_manager_id);
            Ok(())
        }
    }

    /// Adversarial inner sink that errors on the Nth `on_record` call.
    /// Calls before `fail_at` succeed; the `fail_at` call returns
    /// `Err(SinkError::Other(_))`. `seen` is shared so the test can
    /// verify how many records the sink actually observed.
    struct ErrAt {
        seen: std::sync::Arc<std::sync::atomic::AtomicU64>,
        fail_at: u64,
    }

    impl RecordSink for ErrAt {
        fn on_record(&mut self, _record: &Record) -> Result<(), SinkError> {
            let i = self.seen.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            if i == self.fail_at {
                Err(SinkError::Other(format!("synthetic fail at #{i}")))
            } else {
                Ok(())
            }
        }
    }

    #[test]
    fn composite_record_sink_fans_out_to_all_inner_sinks_in_order() {
        // Three records with distinct rmids so the order claim isn't
        // symmetric under permutation.
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
            comp.on_record(r).unwrap();
        }
        let expected = vec![RmId::Heap as u8, RmId::Xact as u8, RmId::RelMap as u8];
        assert_eq!(*log_a.lock().unwrap(), expected);
        assert_eq!(*log_b.lock().unwrap(), expected);
    }

    #[test]
    fn composite_record_sink_short_circuits_on_first_err() {
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
        // First record: every sink runs, ErrAt's `seen` advances to 1.
        comp.on_record(&rec).expect("first record succeeds");
        // Second record: ErrAt fails (i==fail_at==1). The sink after
        // ErrAt must not observe it — that's the short-circuit claim.
        let err = comp
            .on_record(&rec)
            .expect_err("err propagates from inner sink");
        match err {
            SinkError::Other(msg) => assert!(msg.contains("synthetic fail")),
            _ => panic!("expected SinkError::Other, got {err:?}"),
        }
        // Post-error state: log_before saw both records, ErrAt saw two
        // (one Ok + one Err), log_after saw only the first.
        assert_eq!(log_before.lock().unwrap().len(), 2);
        assert_eq!(err_seen.load(Ordering::Relaxed), 2);
        assert_eq!(log_after.lock().unwrap().len(), 1);
    }

    #[test]
    fn metrics_sink_counts_per_rm_decision_and_discards() {
        let mut sink = MetricsRecordSink::default();
        let mut heap_keep = synth_record(0, RmId::Heap as u8);
        heap_keep.decision = Decision::Keep;
        let mut heap_drop = synth_record(64, RmId::Heap as u8);
        heap_drop.decision = Decision::Drop;
        let mut xact_keep = synth_record(128, RmId::Xact as u8);
        xact_keep.decision = Decision::Keep;
        for r in [&heap_keep, &heap_keep, &heap_drop, &xact_keep] {
            sink.on_record(r).unwrap();
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

    #[test]
    fn dir_sink_writes_segment_and_manifest_atomically() {
        let tmp = tempfile::tempdir().unwrap();
        let mut sink = DirSegmentSink::new(tmp.path().to_path_buf()).unwrap();
        let seg = SegmentName::parse("000000010000000000000003").unwrap();
        let bytes = vec![0xAAu8; 64];
        let mani = dummy_manifest();
        sink.on_segment(seg, &bytes, &mani).unwrap();
        let seg_path = tmp.path().join(seg.format());
        let mani_path = tmp.path().join(format!("{}.manifest.json", seg.format()));
        assert!(seg_path.exists(), "segment file written");
        assert!(mani_path.exists(), "manifest sidecar written");
        let on_disk = std::fs::read(&seg_path).unwrap();
        assert_eq!(on_disk, bytes);
        // No `.partial` leftovers — atomic rename cleaned up.
        assert!(
            !tmp.path()
                .join(format!("{}.partial", seg.format()))
                .exists()
        );
    }

    #[test]
    fn dir_sink_partial_segment_lands_with_partial_suffix() {
        let tmp = tempfile::tempdir().unwrap();
        let mut sink = DirSegmentSink::new(tmp.path().to_path_buf()).unwrap();
        let seg = SegmentName::parse("000000010000000000000004").unwrap();
        let bytes = vec![0x77u8; 64];
        let mani = dummy_manifest();
        sink.on_partial_segment(seg, &bytes, &mani).unwrap();
        let name = seg.format();
        let partial_path = tmp.path().join(format!("{name}.partial"));
        let partial_mani_path = tmp.path().join(format!("{name}.partial.manifest.json"));
        // Complete-segment name must NOT exist — otherwise shadow PG's
        // restore_command would pick up a partial as a real segment.
        assert!(
            !tmp.path().join(&name).exists(),
            "complete-segment path leaked: {name}",
        );
        assert!(partial_path.exists(), "partial path written");
        assert!(partial_mani_path.exists(), "partial manifest written");
        let on_disk = std::fs::read(&partial_path).unwrap();
        assert_eq!(on_disk, bytes);
        // Temp files cleaned up by atomic rename.
        assert!(!tmp.path().join(format!("{name}.partial.tmp")).exists());
        assert!(
            !tmp.path()
                .join(format!("{name}.partial.manifest.json.tmp"))
                .exists()
        );
    }
}
