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

use thiserror::Error;
use wal_rs::pg::wal::segment::SegmentName;

use crate::filter::Decision;
use crate::filter_segment::{FilterSegmentError, filter_segment};
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

/// Per-record event emitted as the segment-level filter walks a
/// completed segment. The downstream consumer (Phase 5 decoder, an
/// observability tap) decides what to do based on `decision` and the
/// record's parsed shape via [`Manifest`] entries.
#[derive(Debug, Clone)]
pub struct RecordEvent {
    /// Absolute source LSN where the record begins.
    pub source_lsn: u64,
    /// Total record length (`xl_tot_len`).
    pub len: u32,
    /// `XLogRecordHeader.resource_manager_id`.
    pub rmid: u8,
    /// `XLogRecordHeader.info` byte.
    pub info: u8,
    /// Keep/drop decision the filter computed.
    pub decision: Decision,
}

impl RecordEvent {
    /// Build from a [`Manifest`] entry plus the segment's start LSN.
    pub fn from_manifest_entry(seg_start_lsn: u64, entry: &crate::manifest::Entry) -> Self {
        let decision = match entry.kind {
            Kind::Kept => Decision::Keep,
            Kind::Dropped => Decision::Drop,
        };
        Self {
            source_lsn: seg_start_lsn + entry.offset,
            len: entry.len,
            rmid: entry.rmid,
            info: entry.info,
            decision,
        }
    }
}

/// Sink that observes every record decided by the filter. Phase 5's
/// heap-tuple decoder attaches here.
pub trait RecordSink {
    fn on_record(&mut self, event: &RecordEvent) -> Result<(), SinkError>;
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
}

/// In-memory `RecordSink` for tests. Stores every event.
#[derive(Debug, Default)]
pub struct CollectingRecordSink {
    pub events: Vec<RecordEvent>,
}

impl RecordSink for CollectingRecordSink {
    fn on_record(&mut self, event: &RecordEvent) -> Result<(), SinkError> {
        self.events.push(event.clone());
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
}

/// Segment-aligned accumulator. Bytes pushed via [`push`](Self::push)
/// must arrive in contiguous LSN order starting from [`base_lsn`](Self::base_lsn).
pub struct WalStream {
    timeline: u32,
    seg_size: u64,
    /// LSN of the next byte expected by [`push`](Self::push). Advances
    /// by `bytes.len()` on every successful push.
    next_lsn: u64,
    /// LSN of the byte at `current_buf[0]`. Always segment-aligned.
    current_lsn: u64,
    current_buf: Vec<u8>,
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
        })
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
    pub fn push(
        &mut self,
        lsn: u64,
        bytes: &[u8],
        record_sink: &mut dyn RecordSink,
        segment_sink: &mut dyn SegmentSink,
    ) -> Result<u64, WalStreamError> {
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
            if self.current_buf.len() == self.seg_size as usize {
                self.flush_current(record_sink, segment_sink)?;
            }
        }
        self.next_lsn = cur_lsn;
        Ok(cur_lsn)
    }

    /// Force-flush the current partial segment (does nothing if empty).
    /// Use on shutdown — leaves a `.partial` segment for the operator.
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
        let (filtered, manifest) =
            filter_segment(&self.current_buf, &name).map_err(|source| WalStreamError::Filter {
                seg: name.clone(),
                source,
            })?;
        for entry in &manifest.records {
            let event = RecordEvent::from_manifest_entry(seg_start_lsn, entry);
            record_sink.on_record(&event)?;
        }
        if let Some(sink) = partial_sink {
            sink.on_segment(seg, &filtered, &manifest)?;
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
        let (filtered, manifest) =
            filter_segment(&self.current_buf, &name).map_err(|source| WalStreamError::Filter {
                seg: name.clone(),
                source,
            })?;
        for entry in &manifest.records {
            let event = RecordEvent::from_manifest_entry(seg_start_lsn, entry);
            record_sink.on_record(&event)?;
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
    fn record_event_lsn_offset_is_seg_start_plus_entry_offset() {
        let entry = dummy_manifest_entry(40, RmId::Xact as u8);
        let event = RecordEvent::from_manifest_entry(0x1000_0000, &entry);
        assert_eq!(event.source_lsn, 0x1000_0000 + 40);
        assert_eq!(event.rmid, RmId::Xact as u8);
        assert_eq!(event.decision, Decision::Keep);
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
}
