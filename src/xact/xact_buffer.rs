//! Per-xid xact buffer + TOAST reassembly.
//!
//! Holds every heap record + TOAST chunk for an xid until the matching
//! `XLOG_XACT_COMMIT` / `XLOG_XACT_ABORT` lands. Commit drains in WAL
//! order, planning resolves external values from buffered chunks; abort
//! drops buffer + spill file.
//!
//! ## Why bundle TOAST chunks with heap tuples
//!
//! PG `toast_save_datum` writes chunk INSERTs in the same xact as the
//! referring tuple, so one `XactState` keyed by `xid` covers both. WAL
//! order is natural since heap + chunk records interleave on disk, and
//! detoast at drain means chunk-vs-tuple arrival order is moot.
//!
//! Cross-xact chunks would matter only for PG `streaming=on`, which
//! walshadow does not implement.
//!
//! ## Catalog access at drain
//!
//! Detoast needs the column's type OID to pick `Bytea` vs `Text`. Drain
//! calls
//! [`DescriptorLog::descriptor_at`](crate::catalog::desc_log::DescriptorLog::descriptor_at)
//! per heap needing detoast; the catalog's LRU covers repeat lookups so
//! a buffer-internal cache would duplicate it.
//!
//! ## Spill policy
//!
//! Once `memory_used > config.xact_buffer_max`, flush the largest
//! in-memory xact to a [`SpillWriter`]; the xact stays open and later
//! records accumulate another memory-budgeted tail before eviction.
//! Mirrors PG `ReorderBufferLargestTXN`
//! (`src/backend/replication/logical/reorderbuffer.c`).
//!
//! Drain: spilled entries first (older), then in-mem. Eviction always
//! flushes from front of `in_mem`, holding "spilled older than in-mem".
//!
//! Spill-to-ClickHouse (Option B) is deferred; v1 is local-disk-only.

use std::cmp::Reverse;
use std::collections::{BTreeSet, BinaryHeap, VecDeque, hash_map::Entry};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use thiserror::Error;
use tokio::sync::Mutex;
use tracing::Instrument;
use walrus::pg::walparser::{RelFileNode, RmId};

use crate::catalog::desc_log::{
    DescriptorLog, DescriptorLogs, LogEntry, LogValue, LookupMiss, RfnHistory,
};
use crate::decode::datum::{Datum, DatumRef};
use crate::decode::decoder_sink::{DecoderSinkError, DecoderStats, SkipReason};
#[cfg(any(test, feature = "test-support"))]
use crate::decode::heap_decoder::{DecodedHeap, DescribedHeap};
use crate::decode::heap_decoder::{
    HeapImages, HeapOp, ToastPointer, TupleView, decode_heap_record, record_shape, view_heap_record,
};
#[cfg(test)]
use crate::decode::wal_xact::{
    XACT_XINFO_HAS_DBINFO, XACT_XINFO_HAS_GID, XACT_XINFO_HAS_INVALS, XACT_XINFO_HAS_SUBXACTS,
    XACT_XINFO_HAS_TWOPHASE, XLOG_XACT_HAS_INFO, parse_xact_payload,
};
use crate::decode::wal_xact::{XLOG_XACT_ASSIGNMENT, XLOG_XACT_OPMASK, parse_xact_assignment};
use crate::emit::ch_emitter::EmitterStats;
use crate::ops::trace::{InflightSnapshotEntry, TxnSpanRegistry, new_txn_span};
use crate::pos::{Commit, Drain, EmitterAck, Pos, ResumeSafe, XactFirst};
use crate::record::{Record, RecordSink, Route, SinkError};
use crate::runtime_config::{ConfigEvent, ConfigTableKind};
use crate::schema::{RelDescriptor, SchemaEvent};
use crate::toast::{
    Body, ChunkRefMap, FetchedValue, ToastResolver, ToastRowRef, ToastValueError, ValueRef,
    check_value_caps, detoasted_sized, finish_value, pointer_extsize,
};
use crate::xact::spill::{
    BodySpoolFile, BodySpoolWriter, DescribedInput, HeapInput, RawRecord, SpillEntry, SpillError,
    SpillReader, SpillStore, SpillWriter, ToastChunk, ToastDelete, Truncate,
};
use foldhash::{HashMap, HashMapExt};
#[cfg(any(test, feature = "test-support"))]
use smallvec::SmallVec;

use std::pin::Pin;

/// Shared across every open xact, not per-xact
pub const DEFAULT_XACT_BUFFER_MAX: usize = 512 * 1024 * 1024;

/// Maps PG subxact xids to top-level xid, built from
/// `XLOG_XACT_ASSIGNMENT` (info `0x50`) records.
///
/// Hint, not correctness gate: PG batches first 64 subxacts under
/// `PGPROC_MAX_CACHED_SUBXIDS` and emits no assignment for that window.
/// Authoritative list arrives inline on commit / abort; tracker drives
/// early eviction policy only.
#[derive(Debug, Default)]
pub struct SubxactTracker {
    parent: HashMap<u32, u32>,
    children: HashMap<u32, Vec<u32>>,
}

impl SubxactTracker {
    pub fn new() -> Self {
        Self::default()
    }

    /// Repeated assignments for a subxid keep most recent top
    pub fn assign(&mut self, top_xid: u32, subxids: &[u32]) {
        if subxids.is_empty() {
            return;
        }
        // Two-phase: avoid holding `&mut children[top]` while walking
        // `children[prev_top]` on retargets
        for &s in subxids {
            if let Some(prev_top) = self.parent.insert(s, top_xid)
                && prev_top != top_xid
                && let Some(prev_bucket) = self.children.get_mut(&prev_top)
            {
                prev_bucket.retain(|&x| x != s);
            }
        }
        let bucket = self.children.entry(top_xid).or_default();
        for &s in subxids {
            if !bucket.contains(&s) {
                bucket.push(s);
            }
        }
    }

    /// Unmapped xids return themselves, matching PG "subxact's top is
    /// itself when no ASSIGNMENT landed yet"
    pub fn top_for(&self, xid: u32) -> u32 {
        self.parent.get(&xid).copied().unwrap_or(xid)
    }

    pub fn forget_tree(&mut self, top_xid: u32) {
        if let Some(subs) = self.children.remove(&top_xid) {
            for s in subs {
                self.parent.remove(&s);
            }
        }
        // top_xid might be a subxact in another tree (shouldn't happen on
        // commit / abort path, cheap to scrub)
        self.parent.remove(&top_xid);
    }

    pub fn subxids_of(&self, top_xid: u32) -> Vec<u32> {
        self.children.get(&top_xid).cloned().unwrap_or_default()
    }
}

/// `source_lsn` is the WAL LSN stamped at decode; merge-drain orders by it
fn entry_lsn(e: &SpillEntry) -> u64 {
    match e {
        SpillEntry::Truncate(t) => t.source_lsn,
        SpillEntry::Chunk(c) => c.source_lsn,
        SpillEntry::ToastDelete(d) => d.source_lsn,
        SpillEntry::Raw(r) => r.source_lsn,
        SpillEntry::Input(i) => i.input.source_lsn,
    }
}

/// One drained change: a truncate, or record bytes planning encodes in place
#[derive(Debug, Clone, PartialEq)]
pub enum DrainHeap {
    Truncate(Box<Truncate>),
    Input(Box<DescribedInput>),
}

impl DrainHeap {
    #[cfg(any(test, feature = "test-support"))]
    /// Owned cells per tuple, through the owned decoder
    pub fn into_cells(self) -> std::result::Result<SmallVec<[DescribedHeap; 1]>, XactBufferError> {
        match self {
            Self::Truncate(t) => Ok(SmallVec::from_buf([DescribedHeap {
                decoded: DecodedHeap {
                    rfn: t.descriptor.rfn,
                    xid: t.xid,
                    source_lsn: t.source_lsn,
                    op: HeapOp::Truncate,
                    new: None,
                    old: None,
                },
                descriptor: t.descriptor,
                descriptor_valid_from: t.descriptor_valid_from,
            }])),
            Self::Input(i) => input_heaps(&i),
        }
    }
}

#[cfg(any(test, feature = "test-support"))]
/// Cells for every tuple of a buffered input, under its pinned descriptor.
/// Record-time scan validated the bytes, so failure means corrupt storage
fn input_heaps(
    i: &DescribedInput,
) -> std::result::Result<SmallVec<[DescribedHeap; 1]>, XactBufferError> {
    let decoded = i
        .decode()
        .map_err(|e| XactBufferError::OrdinaryFailClosed {
            relid: i.descriptor.oid,
            lsn: i.input.source_lsn,
            rm: i.input.rm,
            info: i.input.info,
            reason: FailClosedReason::Malformed(e.to_string()),
        })?;
    Ok(decoded
        .into_iter()
        .map(|decoded| DescribedHeap {
            decoded,
            descriptor: i.descriptor.clone(),
            descriptor_valid_from: i.descriptor_valid_from,
        })
        .collect())
}

/// Resident cap on one commit drain's memory-held chunk bodies; bodies
/// past it spool to disk (`toastbody-*`), refs stay resident
pub const TOAST_BODY_SPOOL_MEM_MAX: usize = 16 << 20;

/// Resident approximation per indexed chunk: one `ValueRef`/tail entry or
/// one row ref, container overhead included
const CHUNK_REF_META: usize = 64;

#[derive(Debug, Clone)]
pub struct XactBufferConfig {
    /// In-memory budget across all active xacts before eviction
    pub xact_buffer_max: usize,
    /// Transaction spill, body spool and plan directory, cleared at startup
    pub scratch_dir: PathBuf,
    /// Per-drain memory-held chunk body budget before body spooling
    pub toast_body_mem_max: usize,
    /// Caller's commit-drain slice budget. A lone in-memory xact within it
    /// skips the merge, larger ones slice through it
    pub drain_batch_rows: usize,
    pub drain_batch_bytes: usize,
}

impl XactBufferConfig {
    pub fn new(scratch_dir: PathBuf) -> Self {
        Self {
            xact_buffer_max: DEFAULT_XACT_BUFFER_MAX,
            scratch_dir,
            toast_body_mem_max: TOAST_BODY_SPOOL_MEM_MAX,
            drain_batch_rows: crate::emit::ch_emitter::DEFAULT_DRAIN_BATCH_ROWS,
            drain_batch_bytes: crate::emit::ch_emitter::DEFAULT_DRAIN_BATCH_BYTES,
        }
    }
}

#[derive(Debug, Error)]
pub enum XactBufferError {
    #[error("spill: {0}")]
    Spill(#[from] SpillError),
    #[error("observer: {0}")]
    Observer(String),
    /// Descriptor log answered anything but Present for a heap that already
    /// decoded once against a covered descriptor — coverage bug, fail closed
    #[error("descriptor for {rfn:?} at {lsn:#X} not covered: {got}")]
    DescriptorNotCovered {
        rfn: RelFileNode,
        lsn: u64,
        got: String,
    },
    #[error("toast chunk for value_id={value_id} on rel={toast_relid} missing seq {missing}")]
    MissingToastChunk {
        toast_relid: u32,
        value_id: u32,
        missing: u32,
    },
    #[error("toast decompression: {0}")]
    Detoast(String),
    /// Value exceeded `inline_value_max` under error policy. Replay fails again,
    /// so raise limit or remove error policy
    #[error("toast value of {rawsize} bytes exceeds inline_value_max {max}")]
    ValueTooLarge { rawsize: usize, max: usize },
    /// Stashed set resolved to a toast heap without its `XLOG_SMGR_CREATE`
    /// marker: observation began mid-xact, so the generation cannot prove
    /// completeness and a silent partial decode would leave the mirror
    /// unauditable. Fail closed; operator takes a fresh snapshot
    #[error(
        "toast generation for rel {relid} observed without XLOG_SMGR_CREATE marker; fresh snapshot required"
    )]
    IncompleteToastGeneration { relid: u32 },
    /// Ordinary stash record refused decode (operation policy). Fatal at
    /// drain: dropping it would lose user rows, the exact class raw decode
    /// exists to kill. Carries the record's `rm`/`info` so an operator can
    /// name the unmodelled WAL op without re-reading the segment
    #[error(
        "ordinary raw decode for rel {relid} at {lsn:#X} \
         (op {} rm {rm} info {info:#04X}) failed closed: {reason}",
        op_label(.rm, .info)
    )]
    OrdinaryFailClosed {
        relid: u32,
        lsn: u64,
        rm: u8,
        info: u8,
        reason: FailClosedReason,
    },
    /// Stashed record falls inside a recorded ambiguity interval: no
    /// descriptor proven safe for its rows, neither decode nor discard is
    /// sound. Fail closed; operator takes a fresh snapshot
    #[error(
        "stash for filenode {rel_node} at {lsn:#X} inside ambiguity \
         [{from_lsn:#X}, {through_lsn:#X})"
    )]
    StashAmbiguous {
        rel_node: u32,
        lsn: u64,
        from_lsn: u64,
        through_lsn: u64,
    },
    /// No descriptor covers this buffered record, so decoding must stop.
    #[error("no descriptor for buffered record on filenode {rel_node} at {lsn:#X}")]
    StashUncovered { rel_node: u32, lsn: u64 },
    /// Raw entries drained under [`StashResolved::nothing_stashed`]: the
    /// discard arm would swallow every stashed row, fence included. Fail
    /// closed, resolve_stash must run for any xact that stashed
    #[error("drain for xact {top_xid} has stashed records but no resolution")]
    MissingStashResolution { top_xid: u32 },
    /// Merged entry carries a writer xid outside the owning xact +
    /// subxacts: spill corruption or buffer-key drift. Fail closed before
    /// a foreign row can emit under this commit
    #[error("drained heap xid {xid} outside owning xact {top}")]
    ForeignXid { xid: u32, top: u32 },
}

/// `rm`/`info` as the op name the raw decode counters label with
fn op_label(rm: &u8, info: &u8) -> &'static str {
    use crate::decode::heap_decoder::{HEAP_OP_LABELS, heap_op_index};
    HEAP_OP_LABELS[heap_op_index(*rm, *info)]
}

/// Operation-policy verdict for ordinary raw decode
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FailClosedReason {
    /// Tuple bytes live only in the block image: FPI without block data, or
    /// MULTI_INSERT lacking `XLH_INSERT_CONTAINS_NEW_TUPLE`. wal_level=logical
    /// retains registered data through FPIs (`REGBUF_KEEP_DATA`), so logical
    /// COPY never lands here regardless of checkpoint timing
    ImageOnly,
    /// Byte-level decode failure, carries decoder detail
    Malformed(String),
    /// Operation mutates user rows with no supported decode shape; unknown
    /// resource managers land here too
    UnsupportedOperation,
    /// UPDATE new-tuple prefix/suffix elision references a predecessor image
    /// raw decode cannot reconstruct; PG only elides below wal_level=logical
    /// (`log_heap_update`, PG src/backend/access/heap/heapam.c)
    PartialUpdate,
}

impl std::fmt::Display for FailClosedReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ImageOnly => f.write_str("image-only operation"),
            Self::Malformed(detail) => write!(f, "malformed payload: {detail}"),
            Self::UnsupportedOperation => {
                f.write_str("unsupported operation with user-row effects")
            }
            Self::PartialUpdate => {
                f.write_str("partial update without reconstructable predecessor")
            }
        }
    }
}

impl From<ToastValueError> for XactBufferError {
    fn from(value: ToastValueError) -> Self {
        match value {
            ToastValueError::Detoast(detail) => Self::Detoast(detail),
            ToastValueError::ValueTooLarge { rawsize, max } => Self::ValueTooLarge { rawsize, max },
        }
    }
}

impl From<XactBufferError> for SinkError {
    fn from(e: XactBufferError) -> Self {
        SinkError::Other(e.to_string())
    }
}

impl From<XactBufferError> for DecoderSinkError {
    fn from(e: XactBufferError) -> Self {
        DecoderSinkError::Observer(e.to_string())
    }
}

#[derive(Debug, Default, Clone)]
pub struct XactBufferStats {
    pub xacts_active: u64,
    /// Bookkeeping estimate; actual heap allocation may differ
    pub bytes_in_memory: u64,
    pub xacts_total: u64,
    pub spill_xacts_active: u64,
    /// Spilled bytes awaiting commit drain. Drops when a drain takes
    /// ownership, though the file unlinks only post-dispatch; resident
    /// drain bytes are the separate [`XactBuffer::drain_resident_bytes`]
    pub spill_bytes_active: u64,
    pub spill_evictions_total: u64,
    pub committed_xacts_total: u64,
    pub aborted_xacts_total: u64,
    /// `COMMIT` records for xids never buffered (read-only/filtered)
    pub commits_unknown_xid: u64,
    /// Aborts for xids never buffered. Runs higher than
    /// `commits_unknown_xid`: aborts often hit xacts that wrote nothing
    pub aborts_unknown_xid: u64,
    /// Highest commit-record LSN handed to a drain. Snapshot for the
    /// manifest `drain` role, monotonic. The durable-ack sibling lives in
    /// the pipeline ack collector, not here
    pub drain_lsn: Pos<Drain>,
    /// Cumulative raw-stash payload bytes by first landing; eviction moving
    /// a memory-resident entry to spill does not re-count
    pub raw_stash_bytes_mem: u64,
    pub raw_stash_bytes_spill: u64,
}

impl XactBufferStats {
    pub fn summary(&self) -> String {
        use std::fmt::Write as _;
        let mut s = format!(
            "xact_active={} bytes_in_mem={} spill_active={} spill_bytes={} commit={} abort={}",
            self.xacts_active,
            self.bytes_in_memory,
            self.spill_xacts_active,
            self.spill_bytes_active,
            self.committed_xacts_total,
            self.aborted_xacts_total,
        );
        if self.spill_evictions_total > 0 {
            write!(&mut s, " evictions={}", self.spill_evictions_total).unwrap();
        }
        if self.commits_unknown_xid > 0 {
            write!(&mut s, " commit_unk={}", self.commits_unknown_xid).unwrap();
        }
        if self.aborts_unknown_xid > 0 {
            write!(&mut s, " abort_unk={}", self.aborts_unknown_xid).unwrap();
        }
        s
    }
}

/// One non-tuple item interleaved into a committed xact's drain, ordered by
/// `source_lsn`. Heap tuples ride the sibling `heaps` vec (batched for
/// planning); a `DrainEntry` is applied in WAL order *between* heap
/// segments. `Catalog` fences DDL, `Config` refreshes runtime mapping,
/// `ToastBarrier` closes rewrite generations. All inherit merge tie-break:
/// control event sorts before heap at equal LSN.
#[derive(Debug, Clone)]
pub enum DrainEntry {
    Catalog(SchemaEvent),
    /// Source-PG config-table write, applied at its position to the resolver
    /// so trailing rows in the same xact route against the post-config shape.
    Config(Box<ConfigEvent>),
    /// Residual `O - B` deaths for one resolved rewrite generation, queued
    /// at commit LSN so it drains after every stashed birth. Applied via
    /// [`ToastResolver::rewrite_barrier`] once the generation's rows are put.
    ToastBarrier {
        toast_relid: u32,
        marker_lsn: u64,
    },
}

struct XactState {
    /// Sticky across spill rotations; distinguishes two xids that collide
    /// after a slot rebuild
    first_lsn: Pos<XactFirst>,
    /// WAL-order by arrival
    in_mem: Vec<SpillEntry>,
    in_mem_bytes: usize,
    /// Rows so far while every entry is a non-truncate heap
    plain_rows: Option<usize>,
    /// `None` until first eviction
    spill: Option<SpillWriter>,
    spill_bytes: u64,
    /// source_lsn ASC. Not spilled: typed control state would duplicate
    /// catalog/config encodings; events per xact stay small
    events: Vec<(u64, DrainEntry)>,
    /// Filenodes this xact wrote that were invisible at record time
    /// (same-xact CREATE / TRUNCATE / rewrite generations, or markerless
    /// tracking). Resolved at commit via `relation_at(rfn, commit_lsn)`.
    stash_rfns: HashMap<RelFileNode, StashMark>,
    /// Per-txn `txn` span; duration = WAL-record→durable latency.
    span: tracing::Span,
    /// Child of `span` covering first-buffered→COMMIT-observed (parked-for-
    /// commit wait); closes when the drain consumes the state.
    _wait_span: tracing::Span,
}

impl XactState {
    fn new(first_lsn: u64, span: tracing::Span, hasher: foldhash::fast::RandomState) -> Self {
        let wait_span = trace_span!(
            !span.is_none(),
            parent: &span,
            "buffer.wait",
            first_lsn = first_lsn,
        );
        Self {
            first_lsn: Pos::new(first_lsn),
            in_mem: Vec::new(),
            in_mem_bytes: 0,
            plain_rows: Some(0),
            spill: None,
            spill_bytes: 0,
            events: Vec::new(),
            stash_rfns: HashMap::with_hasher(hasher),
            span,
            _wait_span: wait_span,
        }
    }
}

/// What one stashed filenode's records span, for commit-time resolution
#[derive(Debug, Clone, Copy)]
pub struct StashMark {
    /// Lowest record LSN stashed or tracked for this filenode: the lower
    /// bound the fence query needs
    pub first_lsn: u64,
    /// Some record on this filenode was tracked without payload (no
    /// `XLOG_SMGR_CREATE` marker), so the set cannot prove completeness
    pub payload_free: bool,
}

impl StashMark {
    fn merge(&mut self, other: Self) {
        self.first_lsn = self.first_lsn.min(other.first_lsn);
        self.payload_free |= other.payload_free;
    }
}

/// Allocated bytes of an entry and its buffer slot, excluding allocator
/// overhead and spare slot capacity
fn approximate_size(entry: &SpillEntry) -> usize {
    std::mem::size_of::<SpillEntry>()
        + match entry {
            SpillEntry::Truncate(_) => std::mem::size_of::<Truncate>(),
            SpillEntry::Chunk(c) => std::mem::size_of::<ToastChunk>() + c.chunk_data.len(),
            SpillEntry::ToastDelete(_) => 0,
            SpillEntry::Raw(r) => r.approx_bytes(),
            SpillEntry::Input(i) => i.approx_bytes(),
        }
}

#[derive(Debug, Eq, Ord, PartialEq, PartialOrd)]
struct PendingDurableXact {
    first_lsn: Pos<XactFirst>,
    commit_lsn: Pos<Commit>,
}

#[derive(Debug, Default)]
struct PendingDurable {
    by_first_lsn: BinaryHeap<Reverse<PendingDurableXact>>,
}

impl PendingDurable {
    fn push(&mut self, first_lsn: Pos<XactFirst>, commit_lsn: Pos<Commit>) {
        self.by_first_lsn.push(Reverse(PendingDurableXact {
            first_lsn,
            commit_lsn,
        }));
    }

    fn prune(&mut self, durable_ack: Pos<EmitterAck>) {
        while self
            .by_first_lsn
            .peek()
            .is_some_and(|Reverse(xact)| xact.commit_lsn <= durable_ack.retag())
        {
            self.by_first_lsn.pop();
        }
    }

    fn min_first_lsn(&self) -> Option<Pos<XactFirst>> {
        self.by_first_lsn.peek().map(|Reverse(xact)| xact.first_lsn)
    }
}

/// Backstop on remembered `XLOG_SMGR_CREATE` markers (~20 B each). Markers
/// are consumed at their xact's commit resolution; leftovers come from
/// xid-less creates that never see stashed writes, so the cap only guards
/// against pathological churn
const MARKER_CAP: usize = 65536;

/// Commit-time verdict for one stashed filenode
#[derive(Debug, Clone)]
pub enum StashOutcome {
    /// Resolved toast heap: decode stashed records at drain
    Toast(Arc<RelDescriptor>),
    /// Decode each buffered heap record with the descriptor at its LSN.
    /// Capture saves the descriptors and ambiguity intervals before draining.
    Ordinary(RfnHistory),
}

/// Resolution map for a finishing tree; filenodes absent from `outcomes`
/// discard their records (dropped or rotated away, end-state-neutral)
#[derive(Default)]
pub struct StashResolution {
    outcomes: HashMap<RelFileNode, StashOutcome>,
    stats: Option<Arc<EmitterStats>>,
}

/// Commit-time verdict [`XactBuffer::drain_committed`] consumes, so a drain
/// cannot run against an unresolved tree or another xid's resolution
pub struct StashResolved {
    top_xid: u32,
    /// `None` asserts the tree stashed nothing, checked at drain
    res: Option<StashResolution>,
}

impl StashResolved {
    /// Drain without resolving; fails closed if the tree stashed after all
    pub const fn nothing_stashed(top_xid: u32) -> Self {
        Self { top_xid, res: None }
    }

    /// Build a result with one descriptor and no ambiguity intervals.
    #[cfg(any(test, feature = "test-support"))]
    pub fn ordinary(top_xid: u32, rel: Arc<RelDescriptor>, valid_from: u64) -> Self {
        let mut outcomes = HashMap::new();
        let rfn = rel.rfn;
        let outcome = StashOutcome::Ordinary(RfnHistory::fixture([(valid_from, rel)], Vec::new()));
        outcomes.insert(rfn, outcome);
        Self {
            top_xid,
            res: Some(StashResolution {
                outcomes,
                stats: None,
            }),
        }
    }
}

/// Resolve the finishing tree's stashed filenodes against the descriptor
/// log at the commit's `next_lsn` (capture ran inside the boundary hold, so
/// same-xact CREATE/rewrite descriptors are already covered), return
/// outcomes for the imminent drain, and queue `O - B` barriers for
/// marker-proven toast generations. A toast heap without its marker fails
/// closed ([`XactBufferError::IncompleteToastGeneration`]).
///
/// Use `next_lsn` to find which filenodes survive commit. Copy each surviving
/// heap's history so the drain can look up descriptors at each record's LSN.
pub async fn resolve_stash(
    buffer: &Arc<Mutex<XactBuffer>>,
    log: &DescriptorLog,
    top_xid: u32,
    subxids: &[u32],
    next_lsn: u64,
    stats: Arc<EmitterStats>,
) -> std::result::Result<StashResolved, XactBufferError> {
    let rfns = {
        let buf = buffer.lock().await;
        let mut xids: Vec<u32> = Vec::with_capacity(1 + subxids.len());
        xids.push(top_xid);
        xids.extend_from_slice(subxids);
        buf.stash_candidates(&xids)
    };
    if rfns.is_empty() {
        return Ok(StashResolved {
            top_xid,
            res: Some(StashResolution::default()),
        });
    }
    let mut outcomes: HashMap<RelFileNode, StashOutcome> = HashMap::with_capacity(rfns.len());
    let mut barriers: Vec<(u32, u64)> = Vec::with_capacity(rfns.len());
    for (rfn, mark) in &rfns {
        let (rfn, mark) = (*rfn, *mark);
        // Foreign db never stashes rows worth keeping; counted
        // cluster-level, once per filenode
        if log.refuses_db(rfn.db_node) {
            stats
                .stash_foreign_db_skipped
                .fetch_add(1, Ordering::Relaxed);
            continue;
        }
        match log.descriptor_at_spanned(rfn, next_lsn) {
            Ok((rel, _)) if rel.kind == b't' => {
                // Chunk layout is fixed, so an interval on a toast heap is an
                // unmodelled shape; there is no per-record verdict before
                // decode_stashed_toast either way
                let fenced =
                    log.ambiguities_intersecting(rfn, Some(rel.oid), mark.first_lsn, next_lsn);
                if let Some(a) = fenced.first() {
                    return Err(XactBufferError::StashAmbiguous {
                        rel_node: rfn.rel_node,
                        lsn: next_lsn,
                        from_lsn: a.from_lsn,
                        through_lsn: a.through_lsn,
                    });
                }
                // Only a rotation leaves a dead generation to sweep, and only
                // a marker dates the sweep
                if let Some(marker_lsn) = buffer.lock().await.marker_lsn(rfn) {
                    barriers.push((rel.oid, marker_lsn));
                } else if superseded_generation(log, rel.oid, rfn, mark.first_lsn) {
                    return Err(XactBufferError::IncompleteToastGeneration { relid: rel.oid });
                } else {
                    stats.toast_stash_in_place.fetch_add(1, Ordering::Relaxed);
                }
                outcomes.insert(rfn, StashOutcome::Toast(rel));
            }
            Ok((rel, _)) => {
                if mark.payload_free {
                    // Markerless records were tracked without payload before
                    // the filenode resolved decodable; documented residual,
                    // `plans/catalog.md`
                    tracing::warn!(
                        target: "walshadow::xact_buffer",
                        relid = rel.oid,
                        rel_node = rfn.rel_node,
                        from = format_args!("{:#X}", mark.first_lsn),
                        "ordinary stash resolved with payload-free records tracked; \
                         those rows are not mirrored",
                    );
                }
                let history = log.rfn_history(rfn, rel.oid, mark.first_lsn, next_lsn);
                outcomes.insert(rfn, StashOutcome::Ordinary(history));
            }
            // Point lookup at next_lsn sits past every interval this commit
            // published, so this arm answers only for an interval a later
            // covered commit opened over the same filenode
            Err(LookupMiss::Ambiguous(a)) => {
                return Err(XactBufferError::StashAmbiguous {
                    rel_node: rfn.rel_node,
                    lsn: next_lsn,
                    from_lsn: a.from_lsn,
                    through_lsn: a.through_lsn,
                });
            }
            // Dropped / rotated away by this xid or a later covered commit;
            // AEL supersession makes the discard end-state-neutral.
            // NotCovered = rel never reached the log: born + gone inside
            // this xact's family (capture tombstones only predecessors, a
            // commit-time survivor would be Present), so no surviving rows
            Err(LookupMiss::Dropped | LookupMiss::Retired | LookupMiss::NotCovered) => {}
        }
    }
    let mut buf = buffer.lock().await;
    for (toast_relid, marker_lsn) in barriers {
        buf.on_toast_barrier(top_xid, next_lsn, toast_relid, marker_lsn);
    }
    let resolved: Vec<RelFileNode> = rfns.iter().map(|(rfn, _)| *rfn).collect();
    buf.forget_markers(&resolved);
    Ok(StashResolved {
        top_xid,
        res: Some(StashResolution {
            outcomes,
            stats: Some(stats),
        }),
    })
}

/// Whether `rfn` replaced a different live filenode for `oid`, read before
/// the xact's first stashed record so this commit cannot answer for itself
fn superseded_generation(log: &DescriptorLog, oid: u32, rfn: RelFileNode, first_lsn: u64) -> bool {
    matches!(
        log.predecessor_before(oid, first_lsn).as_deref(),
        Some(LogEntry { value: LogValue::Present(prev), .. }) if prev.rfn != rfn
    )
}

/// Per-xact + TOAST buffer with spill-to-disk overflow, keyed by `xid`
pub struct XactBuffer {
    config: XactBufferConfig,
    store: SpillStore,
    inflight: HashMap<u32, XactState>,
    /// Live `stash_rfns` across all inflight xacts, readable without the lock
    /// so the per-record path can skip taking it while zero
    stash_live: Arc<AtomicU64>,

    /// `XLOG_SMGR_CREATE` main-fork markers by filenode. Global, not
    /// per-xid: the record can precede its xact's xid assignment (header
    /// carries `GetCurrentTransactionIdIfAny`). Presence proves every
    /// record on that filenode was observable, gating both stash admission
    /// and the `O - B` completeness claim; the LSN is the barrier's as-of
    /// point for `O`
    markers: HashMap<RelFileNode, u64>,
    /// Insertion order for the cap prune, tagged with each generation's
    /// marker lsn: eviction skips entries whose lsn no longer matches
    /// `markers` (consumed out-of-band, or filenode reused by a later
    /// generation holding its own queue entry)
    marker_order: VecDeque<(RelFileNode, u64)>,
    /// Committed transactions waiting for durable acknowledgment
    pending_durable: PendingDurable,
    bytes_in_memory: usize,
    stats: XactBufferStats,
    /// Shared with the WAL pump: the pump opens a `txn` span here at first
    /// sighting of an xid, and `absorb` adopts it so the span starts at
    /// WAL-read rather than at buffering. `None` with tracing off
    span_registry: Option<TxnSpanRegistry>,
    /// Bytes resident inside an active commit drain (merge heads + in-mem
    /// tail + chunk generations + mirror rows, until each consumer drops
    /// its share). Arc'd so a detached [`CommittedDrain`] keeps accounting
    /// after the buffer lock releases; distinct from `spill_bytes_active`
    /// (on-disk bytes awaiting drain).
    drain_resident: Arc<AtomicU64>,
    /// High-water mark of `drain_resident`, monotonic per process.
    drain_resident_peak: Arc<AtomicU64>,
    /// Category shares of `drain_resident`
    drain_head_resident: Arc<AtomicU64>,
    drain_chunk_resident: Arc<AtomicU64>,
    drain_row_resident: Arc<AtomicU64>,
    /// Bytes in transaction body spool files (disk, not resident)
    toast_spool_bytes: Arc<AtomicU64>,
}

impl XactBuffer {
    pub fn new(config: XactBufferConfig) -> std::result::Result<Self, XactBufferError> {
        let store = SpillStore::new(config.scratch_dir.clone())?;
        Ok(Self {
            config,
            store,
            inflight: HashMap::new(),
            stash_live: Arc::new(AtomicU64::new(0)),
            markers: HashMap::new(),
            marker_order: VecDeque::new(),
            pending_durable: PendingDurable::default(),
            bytes_in_memory: 0,
            stats: XactBufferStats::default(),
            span_registry: None,
            drain_resident: Arc::new(AtomicU64::new(0)),
            drain_resident_peak: Arc::new(AtomicU64::new(0)),
            drain_head_resident: Arc::new(AtomicU64::new(0)),
            drain_chunk_resident: Arc::new(AtomicU64::new(0)),
            drain_row_resident: Arc::new(AtomicU64::new(0)),
            toast_spool_bytes: Arc::new(AtomicU64::new(0)),
        })
    }

    /// Current bytes resident in an active drain; `0` when no drain runs
    /// and no consumer still holds a sealed generation or row batch.
    pub fn drain_resident_bytes(&self) -> u64 {
        self.drain_resident.load(Ordering::Relaxed)
    }

    /// Bytes in sealed chunk generations, counted until the last holder
    /// (drain batch / decode job) drops its `Arc`.
    pub fn drain_chunk_resident_bytes(&self) -> u64 {
        self.drain_chunk_resident.load(Ordering::Relaxed)
    }

    /// Bytes in collected mirror rows, counted until the row batch drops
    /// after its store put.
    pub fn drain_row_resident_bytes(&self) -> u64 {
        self.drain_row_resident.load(Ordering::Relaxed)
    }

    /// Bytes in transaction body spool files: disk, not resident; drops
    /// with the drain (unlink at finish, wipe on error path restart)
    pub fn toast_spool_bytes(&self) -> u64 {
        self.toast_spool_bytes.load(Ordering::Relaxed)
    }

    /// Monotonic high-water mark of [`Self::drain_resident_bytes`]. For a
    /// spilled xact this stays near merge-heads + chunk bytes, far below the
    /// xact's decoded size — the drain-streaming bound.
    pub fn drain_resident_peak(&self) -> u64 {
        self.drain_resident_peak.load(Ordering::Relaxed)
    }

    /// Adopt `txn` spans the pump opens into `registry` at WAL read
    pub fn with_span_registry(mut self, registry: TxnSpanRegistry) -> Self {
        self.span_registry = Some(registry);
        self
    }

    /// Clear scratch files at startup. Cursor guarantees data is already
    /// written to ClickHouse or can be replayed from `decoder_lsn`.
    /// Call once before any `on_*` method
    pub fn clear_scratch_dir(&self) -> std::result::Result<(), XactBufferError> {
        Ok(self.store.clear()?)
    }

    /// Directory for transaction spill, body spool and plan files
    pub fn scratch_dir(&self) -> &Path {
        self.store.dir()
    }

    pub fn stats(&self) -> &XactBufferStats {
        &self.stats
    }

    /// Sorted by xid. Diagnostic only: pump-side `populate_metrics` feeds
    /// it into `walshadow_xact_inflight` when xacts pile up
    pub fn inflight_snapshot(&self) -> Vec<InflightSnapshotEntry> {
        let mut out: Vec<InflightSnapshotEntry> = self
            .inflight
            .iter()
            .map(|(xid, st)| {
                let mut last_lsn = st.first_lsn.get();
                let mut rels: BTreeSet<(u32, u32)> = BTreeSet::new();
                let mut heap_count = 0u64;
                let mut chunk_count = 0u64;
                for e in &st.in_mem {
                    match e {
                        SpillEntry::Truncate(t) => {
                            heap_count += 1;
                            last_lsn = last_lsn.max(t.source_lsn);
                            rels.insert((t.descriptor.rfn.db_node, t.descriptor.rfn.rel_node));
                        }
                        SpillEntry::Chunk(c) => {
                            chunk_count += 1;
                            last_lsn = last_lsn.max(c.source_lsn);
                            rels.insert((0, c.toast_relid));
                        }
                        SpillEntry::ToastDelete(d) => {
                            chunk_count += 1;
                            last_lsn = last_lsn.max(d.source_lsn);
                            rels.insert((0, d.toast_relid));
                        }
                        SpillEntry::Raw(r) => {
                            chunk_count += 1;
                            last_lsn = last_lsn.max(r.source_lsn);
                            rels.extend(r.rfn().map(|rfn| (rfn.db_node, rfn.rel_node)));
                        }
                        SpillEntry::Input(i) => {
                            heap_count += u64::from(i.input.tuples);
                            last_lsn = last_lsn.max(i.input.source_lsn);
                            rels.insert((i.descriptor.rfn.db_node, i.descriptor.rfn.rel_node));
                        }
                    }
                }
                for (lsn, _) in &st.events {
                    last_lsn = last_lsn.max(*lsn);
                }
                let rels_str = rels
                    .into_iter()
                    .map(|(db, rel)| format!("{db}/{rel}"))
                    .collect::<Vec<_>>()
                    .join(",");
                InflightSnapshotEntry {
                    xid: *xid,
                    first_lsn: st.first_lsn.get(),
                    last_lsn,
                    heap_count,
                    chunk_count,
                    in_mem_bytes: st.in_mem_bytes as u64,
                    spilled: st.spill.is_some(),
                    catalog_events: st.events.len() as u64,
                    rels: rels_str,
                }
            })
            .collect();
        out.sort_by_key(|e| e.xid);
        out
    }

    /// Envelope retains the decode-time descriptor through buffer/spill;
    /// routing reads it attached, never a live lookup
    pub async fn on_truncate(&mut self, t: Truncate) -> std::result::Result<(), XactBufferError> {
        let (xid, first_lsn) = (t.xid, t.source_lsn);
        self.absorb(xid, first_lsn, SpillEntry::Truncate(Box::new(t)))
            .await
    }

    #[cfg(any(test, feature = "test-support"))]
    /// Buffer an owned change as its record bytes
    pub async fn on_heap(&mut self, h: DescribedHeap) -> std::result::Result<(), XactBufferError> {
        match DescribedInput::try_from(h) {
            Ok(input) => self.on_input(input).await,
            Err(truncate) => self.on_truncate(truncate).await,
        }
    }

    /// Ordinary heap record kept undecoded until commit
    pub async fn on_input(
        &mut self,
        described: DescribedInput,
    ) -> std::result::Result<(), XactBufferError> {
        let (xid, first_lsn) = (described.input.xid, described.input.source_lsn);
        self.absorb(xid, first_lsn, SpillEntry::Input(Box::new(described)))
            .await
    }

    /// Built from `pg_toast.pg_toast_<rel>` INSERTs by the decoder sink
    pub async fn on_toast_chunk(
        &mut self,
        chunk: ToastChunk,
        xid: u32,
    ) -> std::result::Result<(), XactBufferError> {
        let first_lsn = chunk.source_lsn;
        let entry = SpillEntry::Chunk(Box::new(chunk));
        self.absorb(xid, first_lsn, entry).await
    }

    /// TID-keyed toast DELETE, a store tombstone row at commit drain.
    /// Buffered like chunks so aborts discard it and the drain merge keeps
    /// WAL order against same-xact births.
    pub async fn on_toast_delete(
        &mut self,
        delete: crate::xact::spill::ToastDelete,
        xid: u32,
    ) -> std::result::Result<(), XactBufferError> {
        let first_lsn = delete.source_lsn;
        self.absorb(xid, first_lsn, SpillEntry::ToastDelete(Box::new(delete)))
            .await
    }

    /// Lazily insert the xid's state (adopting its `txn` span) and queue a
    /// control event at `source_lsn` for the commit-drain k-way merge.
    fn push_drain_entry(&mut self, xid: u32, source_lsn: u64, entry: DrainEntry) {
        self.state_for(xid, source_lsn)
            .events
            .push((source_lsn, entry));
    }

    /// Get-or-create the xid's state, adopting its `txn` span
    fn state_for(&mut self, xid: u32, first_lsn: u64) -> &mut XactState {
        let is_new = !self.inflight.contains_key(&xid);
        if is_new {
            self.stats.xacts_active += 1;
            self.stats.xacts_total += 1;
        }
        let registry = &self.span_registry;
        // Stash map reuses inflight map's seeds, drawing fresh ones per xact
        // shows in profiles
        let hasher = self.inflight.hasher().clone();
        self.inflight.entry(xid).or_insert_with(|| {
            let span = registry
                .as_ref()
                .and_then(|registry| {
                    registry.adopt(xid).or_else(|| {
                        registry
                            .is_sampled(xid)
                            .then(|| new_txn_span(xid, first_lsn))
                    })
                })
                .unwrap_or_else(tracing::Span::none);
            XactState::new(first_lsn, span, hasher)
        })
    }

    /// Record a main-fork `XLOG_SMGR_CREATE`. With a valid xid the filenode
    /// also joins that xact's stash candidates, so a zero-record generation
    /// (rewrite of an empty toast heap) still resolves at commit and emits
    /// its residual `O - B` deaths
    pub fn note_smgr_create(&mut self, xid: u32, rfn: RelFileNode, lsn: u64) {
        if self.markers.insert(rfn, lsn) != Some(lsn) {
            self.marker_order.push_back((rfn, lsn));
            while self.marker_order.len() > MARKER_CAP {
                if let Some((old, old_lsn)) = self.marker_order.pop_front()
                    && self.markers.get(&old) == Some(&old_lsn)
                {
                    self.markers.remove(&old);
                }
            }
        }
        if xid != 0 {
            self.mark_stash(xid, lsn, rfn, false);
        }
    }

    pub fn marker_lsn(&self, rfn: RelFileNode) -> Option<u64> {
        self.markers.get(&rfn).copied()
    }

    /// Stash raw decode inputs for a record whose filenode was invisible at
    /// record time; rides the per-xid spill so subxact/abort discard and
    /// commit-merge ordering come for free
    pub async fn stash_raw(
        &mut self,
        xid: u32,
        raw: RawRecord,
    ) -> std::result::Result<(), XactBufferError> {
        let Some(rfn) = raw.rfn() else {
            return Ok(());
        };
        let lsn = raw.source_lsn;
        self.mark_stash(xid, lsn, rfn, false);
        self.absorb(xid, lsn, SpillEntry::Raw(Box::new(raw))).await
    }

    /// Track an unresolvable filenode without payload: no marker means the
    /// set can't prove completeness, so entries aren't kept, but commit
    /// resolution must still fail closed if the filenode turns out toast
    pub fn track_unresolvable(&mut self, xid: u32, lsn: u64, rfn: RelFileNode) {
        self.mark_stash(xid, lsn, rfn, true);
    }

    /// Note a filenode in `xid`'s resolution set. Records arrive in WAL
    /// order per xid, so the first mark carries the lowest LSN
    fn mark_stash(&mut self, xid: u32, lsn: u64, rfn: RelFileNode, payload_free: bool) {
        let mark = StashMark {
            first_lsn: lsn,
            payload_free,
        };
        let live = self.stash_live.clone();
        self.state_for(xid, lsn)
            .stash_rfns
            .entry(rfn)
            .and_modify(|m| m.merge(mark))
            .or_insert_with(|| {
                live.fetch_add(1, Ordering::Relaxed);
                mark
            });
    }

    pub fn stash_live(&self) -> Arc<AtomicU64> {
        self.stash_live.clone()
    }

    /// Fast path for the decoder: a filenode already stashed under `xid`
    /// (or marker-registered to it) can never resolve for that xact's own
    /// records — its pg_class row is MVCC-invisible until commit — so the
    /// replay-gated lookup is skippable
    pub fn is_stash_candidate(&self, xid: u32, rfn: RelFileNode) -> bool {
        self.inflight
            .get(&xid)
            .is_some_and(|st| st.stash_rfns.contains_key(&rfn))
    }

    /// Union of stash candidates across the finishing tree, rfn-ordered for
    /// deterministic resolution; marks merge (min LSN, sticky payload_free)
    pub fn stash_candidates(&self, xids: &[u32]) -> Vec<(RelFileNode, StashMark)> {
        let stashed = |x: &u32| {
            self.inflight
                .get(x)
                .is_some_and(|st| !st.stash_rfns.is_empty())
        };
        if !xids.iter().any(stashed) {
            return Vec::new();
        }
        let mut merged: HashMap<RelFileNode, StashMark> = HashMap::new();
        for x in xids {
            if let Some(st) = self.inflight.get(x) {
                for (rfn, mark) in &st.stash_rfns {
                    merged
                        .entry(*rfn)
                        .and_modify(|m| m.merge(*mark))
                        .or_insert(*mark);
                }
            }
        }
        let mut out: Vec<(RelFileNode, StashMark)> = merged.into_iter().collect();
        out.sort_unstable_by_key(|(rfn, _)| *rfn);
        out
    }

    /// Drop consumed markers post-resolution (abort drops via its states)
    pub fn forget_markers(&mut self, rfns: &[RelFileNode]) {
        for rfn in rfns {
            self.markers.remove(rfn);
        }
    }

    /// Drains in `source_lsn` order at commit, so a DDL's `Added`/`Changed`
    /// event lands BEFORE the heap writes that follow it
    pub fn on_schema_event(&mut self, xid: u32, source_lsn: u64, event: SchemaEvent) {
        self.push_drain_entry(xid, source_lsn, DrainEntry::Catalog(event));
    }

    /// Config-table write, interleaved into the drain at its `source_lsn` so it
    /// applies before the heap writes it precedes in WAL (plan §6)
    pub fn on_config_event(&mut self, xid: u32, source_lsn: u64, event: ConfigEvent) {
        self.push_drain_entry(xid, source_lsn, DrainEntry::Config(Box::new(event)));
    }

    /// Queue a rewrite generation's residual-death barrier at commit LSN,
    /// after every stashed birth in the merge order
    pub fn on_toast_barrier(
        &mut self,
        xid: u32,
        commit_lsn: u64,
        toast_relid: u32,
        marker_lsn: u64,
    ) {
        self.push_drain_entry(
            xid,
            commit_lsn,
            DrainEntry::ToastBarrier {
                toast_relid,
                marker_lsn,
            },
        );
    }

    async fn absorb(
        &mut self,
        xid: u32,
        first_lsn: u64,
        entry: SpillEntry,
    ) -> std::result::Result<(), XactBufferError> {
        let sz = approximate_size(&entry);
        let raw_sz = matches!(&entry, SpillEntry::Raw(_)).then_some(sz as u64);
        let input_rows = entry.input().map(|i| usize::from(i.input.tuples));
        let st = self.state_for(xid, first_lsn);
        st.plain_rows = st.plain_rows.zip(input_rows).map(|(n, rows)| n + rows);
        st.in_mem.push(entry);
        st.in_mem_bytes += sz;
        self.bytes_in_memory += sz;
        self.stats.raw_stash_bytes_mem += raw_sz.unwrap_or(0);
        self.stats.bytes_in_memory = self.bytes_in_memory as u64;
        if self.bytes_in_memory > self.config.xact_buffer_max {
            // Boxed: eviction spills, kilobytes deep, off every record future
            Box::pin(self.maybe_evict()).await?;
        }
        Ok(())
    }

    async fn maybe_evict(&mut self) -> std::result::Result<(), XactBufferError> {
        while self.bytes_in_memory > self.config.xact_buffer_max
            && let Some(xid) = self
                .inflight
                .iter()
                .filter(|(_, s)| !s.in_mem.is_empty())
                .max_by_key(|(_, s)| s.in_mem_bytes)
                .map(|(xid, _)| *xid)
        {
            self.evict_xact(xid).await?;
        }
        Ok(())
    }

    async fn evict_xact(&mut self, xid: u32) -> std::result::Result<(), XactBufferError> {
        let st = self.inflight.get_mut(&xid).expect("xid present");
        let first_spill = st.spill.is_none();
        let writer = match &mut st.spill {
            Some(writer) => writer,
            None => st
                .spill
                .insert(self.store.writer(xid, st.first_lsn.get()).await?),
        };
        let drained: Vec<SpillEntry> = std::mem::take(&mut st.in_mem);
        let freed = std::mem::take(&mut st.in_mem_bytes);
        writer.write_batch(drained).await?;
        let bc = writer.byte_count();
        let new_spill_bytes = bc - st.spill_bytes;
        st.spill_bytes = bc;
        self.bytes_in_memory = self.bytes_in_memory.saturating_sub(freed);
        self.stats.bytes_in_memory = self.bytes_in_memory as u64;
        self.stats.spill_evictions_total += 1;
        self.stats.spill_bytes_active += new_spill_bytes;
        if first_spill {
            self.stats.spill_xacts_active += 1;
        }
        Ok(())
    }

    /// Convert removed states into a lazy k-way merge, releasing their
    /// spill accounting (`spill_bytes_active` counts bytes awaiting drain;
    /// the drain owns them from here). Files stay on disk until
    /// [`MergedDrain::finish`] unlinks post-dispatch.
    async fn open_drain(
        &mut self,
        states: Vec<XactState>,
        collect_rows: bool,
        stash: StashResolution,
        // Body spool identity (`toastbody-{xid}-{lsn}.bin`), lazily
        // created once memory-held chunk bytes cross `toast_body_mem_max`
        top_xid: u32,
        commit_lsn: u64,
        allowed_xids: foldhash::HashSet<u32>,
    ) -> std::result::Result<MergedDrain, XactBufferError> {
        let mut gauge = self.drain_gauge(&self.drain_head_resident);
        let mut sources = Vec::with_capacity(states.len());
        let mut events = Vec::with_capacity(states.len());
        // States split first so none stays live across the spill await below
        let mut parts = Vec::with_capacity(states.len());
        for mut st in states {
            if let Some(writer) = &st.spill {
                let bc = writer.byte_count();
                self.stats.spill_bytes_active = self.stats.spill_bytes_active.saturating_sub(bc);
                self.stats.spill_xacts_active = self.stats.spill_xacts_active.saturating_sub(1);
            }
            // Two producers push events: the worker at observe order and
            // the pump at capture time keyed bias-early valid_from — LSN
            // order is not arrival order. Stable sort keeps same-LSN
            // arrival order (Added before dependent Changed)
            let mut evs = std::mem::take(&mut st.events);
            evs.sort_by_key(|(lsn, _)| *lsn);
            events.push(evs.into());
            parts.push((st.spill.take(), std::mem::take(&mut st.in_mem)));
        }
        for (spill, in_mem) in parts {
            sources.push(match spill {
                None => MergeSource::open_mem(in_mem, &mut gauge),
                // Boxed: spill flush and reads stay off the per-commit future
                Some(writer) => {
                    Box::pin(MergeSource::open_spilled(writer, in_mem, &mut gauge)).await?
                }
            });
        }
        Ok(MergedDrain {
            sources,
            events,
            allowed_xids,
            chunks: ChunkRefMap::new(),
            chunk_bytes: 0,
            collect_rows,
            rows: Vec::new(),
            row_bytes: 0,
            stash,
            gauge,
            chunk_gauge: self.drain_gauge(&self.drain_chunk_resident),
            row_gauge: self.drain_gauge(&self.drain_row_resident),
            spool: None,
            spool_dir: self.store.dir().to_path_buf(),
            spool_xid: top_xid,
            spool_lsn: commit_lsn,
            spool_gauge: self.toast_spool_bytes.clone(),
            mem_body_bytes: 0,
            body_mem_max: self.config.toast_body_mem_max,
        })
    }

    fn drain_gauge(&self, cat: &Arc<AtomicU64>) -> ResidentGauge {
        ResidentGauge {
            cur: self.drain_resident.clone(),
            peak: self.drain_resident_peak.clone(),
            cat: cat.clone(),
            held: 0,
        }
    }

    /// Commit drain: hand back a [`CommittedDrain`] that streams bounded
    /// [`DrainedBatch`] slices from a lazy k-way merge. Detoast and dispatch
    /// run in the reorder planner / barrier coordinator; pipeline ack collector
    /// owns `emitter_ack_lsn`.
    pub async fn drain_committed(
        &mut self,
        stash: StashResolved,
        commit_ts: i64,
        commit_lsn: u64,
        subxids: &[u32],
        // `resolver.stores_chunks()` at the caller: collect store rows only
        // when a put consumer exists
        collect_rows: bool,
    ) -> std::result::Result<CommittedDrain, XactBufferError> {
        let top_xid = stash.top_xid;
        let mut xids: Vec<u32> = Vec::with_capacity(1 + subxids.len());
        xids.push(top_xid);
        xids.extend_from_slice(subxids);
        let mut states: Vec<XactState> = Vec::with_capacity(xids.len());
        for x in &xids {
            if let Some(st) = self.inflight.remove(x) {
                self.stash_live
                    .fetch_sub(st.stash_rfns.len() as u64, Ordering::Relaxed);
                states.push(st);
            }
        }
        self.stats.drain_lsn = self.stats.drain_lsn.max(Pos::new(commit_lsn));
        let Some(first) = states.iter().map(|st| st.first_lsn).min() else {
            // Read-only / filter-dropped: reorder coordinator still
            // registers a seq so the contiguous watermark passes commit_lsn
            self.stats.commits_unknown_xid += 1;
            return Ok(CommittedDrain {
                commit_ts,
                commit_lsn,
                merged: None,
                generations: Vec::new(),
            });
        };
        // Preserve floor while decoded slices remain undurable
        self.pending_durable.push(first, Pos::new(commit_lsn));
        for st in &states {
            self.stats.xacts_active = self.stats.xacts_active.saturating_sub(1);
            self.bytes_in_memory = self.bytes_in_memory.saturating_sub(st.in_mem_bytes);
        }
        self.stats.bytes_in_memory = self.bytes_in_memory as u64;
        // Absent resolution would send every Raw entry through fold_raw's
        // discard arm, fence included
        let stash = match stash.res {
            Some(res) => res,
            None if states.iter().all(|st| st.stash_rfns.is_empty()) => StashResolution::default(),
            None => return Err(XactBufferError::MissingStashResolution { top_xid }),
        };
        // Lone in-memory xact of plain heaps fitting one slice: arrival is
        // WAL order, nothing to merge, fold, fence or slice
        if let [st] = states.as_mut_slice()
            && st
                .plain_rows
                .is_some_and(|rows| rows <= self.config.drain_batch_rows)
            && st.spill.is_none()
            && st.events.is_empty()
            && st.in_mem_bytes <= self.config.drain_batch_bytes
        {
            // Each input buffers under its own xid, one of `xids`
            let heaps = std::mem::take(&mut st.in_mem)
                .into_iter()
                .map(|e| DrainHeap::Input(e.into_input().expect("plain state holds inputs only")))
                .collect();
            self.stats.committed_xacts_total += 1;
            return Ok(CommittedDrain {
                commit_ts,
                commit_lsn,
                merged: Some(Drained::Heaps(heaps)),
                generations: Vec::new(),
            });
        }
        let allowed_xids = xids.iter().copied().collect();
        // Boxed: merge setup would bloat every commit future
        let merged = Box::pin(self.open_drain(
            states,
            collect_rows,
            stash,
            top_xid,
            commit_lsn,
            allowed_xids,
        ))
        .await?;
        self.stats.committed_xacts_total += 1;
        Ok(CommittedDrain {
            commit_ts,
            commit_lsn,
            merged: Some(Drained::Merged(Box::new(merged))),
            generations: Vec::new(),
        })
    }

    /// When no xact in flight, advance `drain_lsn` to `lsn`: trailing
    /// post-COMMIT WAL (page padding, RUNNING_XACTS, CHECKPOINT) counts as
    /// drained when quiescent. The durable ack side lives in the ack
    /// collector (`AckHandle::trailing`).
    pub fn advance_idle(&mut self, lsn: Pos<Drain>) {
        if self.stats.xacts_active != 0 {
            return;
        }
        self.stats.drain_lsn = self.stats.drain_lsn.max(lsn);
    }

    /// Floor durable acknowledgment at first record of each undurable transaction
    pub fn resume_safe_lsn(&mut self, durable_ack: Pos<EmitterAck>) -> Pos<ResumeSafe> {
        self.pending_durable.prune(durable_ack);
        Pos::new(
            self.inflight
                .values()
                .map(|st| st.first_lsn)
                .chain(self.pending_durable.min_first_lsn())
                .fold(durable_ack.get(), |acc, first| acc.min(first.get())),
        )
    }

    /// Discard xact `xid` + spill file. No-op if unknown. `abort_lsn` is
    /// the `XLOG_XACT_ABORT` record LSN; advances `drain_lsn` so aborts
    /// count as fully consumed (the ack side rides the reorder's rows=0
    /// abort seq through the collector)
    pub async fn abort(
        &mut self,
        xid: u32,
        abort_lsn: Pos<Drain>,
        subxids: &[u32],
    ) -> std::result::Result<(), XactBufferError> {
        self.stats.drain_lsn = self.stats.drain_lsn.max(abort_lsn);
        // `xid` is header xact_id: top abort or subxact standalone
        // rollback. Drop `xid` + every sub. For mid-xact subxact rollback
        // (PG `RecordSubTransactionAbort` writes a separate
        // `XLOG_XACT_ABORT` keyed on the sub), the top's pre-savepoint
        // entries stay keyed on top_xid and flush at the top's COMMIT.
        let mut xids: Vec<u32> = Vec::with_capacity(1 + subxids.len());
        xids.push(xid);
        xids.extend_from_slice(subxids);
        // Drop any pump-opened span handles for the aborted tree. The
        // per-xid XactState (with its own clone) is removed + dropped in
        // the loop below, closing the span as aborted.
        if let Some(registry) = &self.span_registry {
            registry.prune(&xids);
        }

        let mut any = false;
        for x in xids {
            let Some(mut st) = self.inflight.remove(&x) else {
                continue;
            };
            // Close the per-txn span as aborted; nothing ships, so it has
            // no commit.drain child — just a short span tagged accordingly.
            st.span.record("outcome", "aborted");
            // Aborted creates leave their filenodes forever unresolvable;
            // drop the markers with the states
            for rfn in st.stash_rfns.keys() {
                self.markers.remove(rfn);
            }
            self.stash_live
                .fetch_sub(st.stash_rfns.len() as u64, Ordering::Relaxed);
            any = true;
            self.stats.xacts_active = self.stats.xacts_active.saturating_sub(1);
            self.bytes_in_memory = self.bytes_in_memory.saturating_sub(st.in_mem_bytes);
            if let Some(writer) = st.spill.take() {
                let bc = writer.byte_count();
                self.stats.spill_bytes_active = self.stats.spill_bytes_active.saturating_sub(bc);
                self.stats.spill_xacts_active = self.stats.spill_xacts_active.saturating_sub(1);
                writer.unlink().await?;
            }
        }
        self.stats.bytes_in_memory = self.bytes_in_memory as u64;
        if !any {
            self.stats.aborts_unknown_xid += 1;
            return Ok(());
        }
        // One bump per abort record, not per subxid
        self.stats.aborted_xacts_total += 1;
        Ok(())
    }

    #[cfg(test)]
    pub fn active_xids(&self) -> Vec<u32> {
        let mut v: Vec<u32> = self.inflight.keys().copied().collect();
        v.sort_unstable();
        v
    }
}

/// Tracks bytes resident inside an active drain. One instance per
/// category (merge heads / sealed chunk generations / mirror rows), all
/// feeding one total. Contribution subtracts on drop so an abandoned
/// drain (observer error) can't leave the gauge stuck.
struct ResidentGauge {
    cur: Arc<AtomicU64>,
    peak: Arc<AtomicU64>,
    /// Category share of `cur`
    cat: Arc<AtomicU64>,
    held: u64,
}

impl ResidentGauge {
    fn add(&mut self, n: usize) {
        self.held += n as u64;
        self.cat.fetch_add(n as u64, Ordering::Relaxed);
        let now = self.cur.fetch_add(n as u64, Ordering::Relaxed) + n as u64;
        self.peak.fetch_max(now, Ordering::Relaxed);
    }

    fn sub(&mut self, n: usize) {
        let n = (n as u64).min(self.held);
        self.held -= n;
        self.cat.fetch_sub(n, Ordering::Relaxed);
        self.cur.fetch_sub(n, Ordering::Relaxed);
    }

    /// Move `n` held bytes into a share the new owner drops when done.
    /// Totals unchanged: ownership transfer is not release.
    fn split(&mut self, n: usize) -> ResidentGauge {
        let n = (n as u64).min(self.held);
        self.held -= n;
        ResidentGauge {
            cur: self.cur.clone(),
            peak: self.peak.clone(),
            cat: self.cat.clone(),
            held: n,
        }
    }
}

impl Drop for ResidentGauge {
    fn drop(&mut self) {
        self.cat.fetch_sub(self.held, Ordering::Relaxed);
        self.cur.fetch_sub(self.held, Ordering::Relaxed);
    }
}

/// Lazy merge source for one xid: spill-reader head (older in WAL order)
/// chained with the in-mem tail, one decoded entry resident. `pop` refills
/// from the reader until EOF, then drains `in_mem`. The EOF reader parks as
/// `spent` so the file unlinks only at [`MergedDrain::finish`],
/// post-dispatch.
struct MergeSource {
    head: Option<SpillEntry>,
    spill: SpillSide,
    in_mem: VecDeque<SpillEntry>,
}

enum SpillSide {
    Unspilled,
    Spilled { reader: SpillReader, spent: bool },
}

impl MergeSource {
    /// Tail entries are resident from the start (they left the buffer's
    /// `bytes_in_memory` at commit); spill entries join at decode
    fn open_mem(in_mem: Vec<SpillEntry>, gauge: &mut ResidentGauge) -> Self {
        for e in &in_mem {
            gauge.add(approximate_size(e));
        }
        let mut in_mem: VecDeque<SpillEntry> = in_mem.into();
        Self {
            head: in_mem.pop_front(),
            spill: SpillSide::Unspilled,
            in_mem,
        }
    }

    async fn open_spilled(
        writer: SpillWriter,
        in_mem: Vec<SpillEntry>,
        gauge: &mut ResidentGauge,
    ) -> std::result::Result<Self, SpillError> {
        let reader = writer.finish().await?;
        for e in &in_mem {
            gauge.add(approximate_size(e));
        }
        gauge.add(reader.buffered_bytes());
        let mut src = Self {
            head: None,
            spill: SpillSide::Spilled {
                reader,
                spent: false,
            },
            in_mem: in_mem.into(),
        };
        src.head = src.next_entry(gauge).await?;
        Ok(src)
    }

    async fn next_entry(
        &mut self,
        gauge: &mut ResidentGauge,
    ) -> std::result::Result<Option<SpillEntry>, SpillError> {
        if let SpillSide::Spilled { reader, spent } = &mut self.spill
            && !*spent
        {
            if let Some(entry) = reader.next().await? {
                gauge.add(approximate_size(&entry));
                return Ok(Some(entry));
            }
            *spent = true;
        }
        // Already counted at `open`; moving to head keeps it resident
        Ok(self.in_mem.pop_front())
    }

    fn head_lsn(&self) -> Option<u64> {
        self.head.as_ref().map(entry_lsn)
    }

    /// Caller peeked a head through [`Self::head_lsn`]
    async fn pop(
        &mut self,
        gauge: &mut ResidentGauge,
    ) -> std::result::Result<SpillEntry, SpillError> {
        let entry = self.head.take().expect("caller peeked head");
        gauge.sub(approximate_size(&entry));
        self.head = self.next_entry(gauge).await?;
        Ok(entry)
    }
}

enum MergeItem {
    Truncate(Box<Truncate>),
    Input(Box<DescribedInput>),
    /// Boxed: a config row carries every per-relation setting, so the variant
    /// dwarfs the heap pointer the merge yields per row
    Event(Box<DrainEntry>),
}

/// Lazy k-way merge over per-xid sources + event queues, `source_lsn` ASC.
/// k = 1 + nsubxacts, typically <= 4, so linear head-pick beats a heap.
///
/// Control events (`DrainEntry`) win ties against ANY same-LSN data entry:
/// PG writes a DDL's catalog mutation before the dependent heap, and the
/// lazy refetch stamps the schema event with the triggering heap's
/// source_lsn, so they share an LSN. Event-first lands the `ALTER` on CH
/// before the dependent INSERT encodes against the post-DDL shape. The
/// events loop runs before the data loop and uses `<=` (data uses `<`), so
/// any `DrainEntry` — Catalog now, Config/Signal per runtime-config §3/§6 —
/// inherits the tie-break.
///
/// Toast chunks fold into `chunks` and never surface as items: a chunk's
/// WAL position precedes its referrer, so the map is complete for every
/// heap yielded after it. Drop-after-first-use would be wrong — one value
/// can be referenced by several row versions in one xact (unchanged-toast
/// UPDATE chain) — so entries live until [`Self::take_chunks`] / drop.
///
/// Bodies stay in memory until their cumulative bytes cross
/// `body_mem_max`, then append once to a transaction body spool;
/// resolution refs and mirror row refs share the range, so resident state
/// past the threshold is metadata plus read buffers (M2).
struct MergedDrain {
    sources: Vec<MergeSource>,
    events: Vec<VecDeque<(u64, DrainEntry)>>,
    /// Live (unsealed) generation
    chunks: ChunkRefMap,
    chunk_bytes: usize,
    // Skip duplicated mirror refs when no store consumes rows
    collect_rows: bool,
    rows: Vec<ToastRowRef>,
    row_bytes: usize,
    /// Commit-time verdicts for stashed filenodes; empty when nothing stashed
    stash: StashResolution,
    /// Owning xact + subxacts: every yielded heap's writer xid must be a
    /// member (spec validation "decoded xid matches owning xact/subxact");
    /// a stranger means spill corruption or buffer-key drift, fail closed
    allowed_xids: foldhash::HashSet<u32>,
    /// Merge heads + in-mem tail
    gauge: ResidentGauge,
    /// Unsealed chunk map (memory bodies + ref metadata, never file
    /// bodies); shares split off with each sealed generation
    chunk_gauge: ResidentGauge,
    /// Collected mirror rows; shares split off with each taken batch
    row_gauge: ResidentGauge,
    /// Lazily created at threshold crossing; None while memory-resident
    spool: Option<BodySpoolWriter>,
    spool_dir: PathBuf,
    spool_xid: u32,
    spool_lsn: u64,
    /// Spool file bytes, `walshadow_toast_xact_spool_bytes`; charged by
    /// the writer's shared file owner, released with its last holder
    spool_gauge: Arc<AtomicU64>,
    /// Cumulative memory-held body bytes, monotone (spooling never reverts)
    mem_body_bytes: usize,
    body_mem_max: usize,
}

impl MergedDrain {
    async fn next(&mut self) -> std::result::Result<Option<MergeItem>, XactBufferError> {
        loop {
            enum Pick {
                Data(usize),
                Event(usize),
            }
            let mut best: Option<(Pick, u64)> = None;
            for (i, q) in self.events.iter().enumerate() {
                let Some(&(lsn, _)) = q.front() else {
                    continue;
                };
                if best.as_ref().is_none_or(|&(_, b)| lsn <= b) {
                    best = Some((Pick::Event(i), lsn));
                }
            }
            for (i, s) in self.sources.iter().enumerate() {
                let Some(lsn) = s.head_lsn() else { continue };
                if best.as_ref().is_none_or(|&(_, b)| lsn < b) {
                    best = Some((Pick::Data(i), lsn));
                }
            }
            let Some((pick, _)) = best else {
                return Ok(None);
            };
            match pick {
                Pick::Event(i) => {
                    let (_lsn, ev) = self.events[i].pop_front().expect("just peeked head");
                    return Ok(Some(MergeItem::Event(Box::new(ev))));
                }
                Pick::Data(i) => {
                    let entry = self.sources[i].pop(&mut self.gauge).await?;
                    match entry {
                        SpillEntry::Truncate(t) => {
                            self.check_owned(t.xid)?;
                            return Ok(Some(MergeItem::Truncate(t)));
                        }
                        SpillEntry::Chunk(c) => self.fold_chunk(*c)?,
                        SpillEntry::ToastDelete(d) => self.fold_delete(*d),
                        SpillEntry::Raw(raw) => {
                            if let Some(i) = self.fold_raw(&raw)? {
                                self.check_owned(i.input.xid)?;
                                return Ok(Some(MergeItem::Input(i)));
                            }
                        }
                        SpillEntry::Input(i) => {
                            self.check_owned(i.input.xid)?;
                            return Ok(Some(MergeItem::Input(i)));
                        }
                    }
                }
            }
        }
    }

    fn check_owned(&self, xid: u32) -> std::result::Result<(), XactBufferError> {
        if self.allowed_xids.contains(&xid) {
            return Ok(());
        }
        Err(XactBufferError::ForeignXid {
            xid,
            top: self.spool_xid,
        })
    }

    /// Body kept memory-resident below `body_mem_max` cumulative bytes,
    /// appended once to the body spool past it (resolution map and mirror
    /// rows share either form)
    fn fold_body(&mut self, data: &bytes::Bytes) -> std::result::Result<Body, XactBufferError> {
        let spool = match &mut self.spool {
            Some(spool) => spool,
            None if self.mem_body_bytes + data.len() <= self.body_mem_max => {
                self.mem_body_bytes += data.len();
                return Ok(Body::Mem(data.clone()));
            }
            None => self.spool.insert(BodySpoolWriter::create(
                &self.spool_dir,
                self.spool_xid,
                self.spool_lsn,
                Some(self.spool_gauge.clone()),
            )?),
        };
        let r = spool.append(data)?;
        Ok(Body::File(r))
    }

    fn fold_chunk(&mut self, c: ToastChunk) -> std::result::Result<(), XactBufferError> {
        // InvalidOffsetNumber cannot key mirror rows
        let collect_row = self.collect_rows && c.offnum != 0;
        let body = self.fold_body(&c.chunk_data)?;
        // File bodies live on disk, outside resident shares (M7)
        let mem_len = match &body {
            Body::Mem(b) => b.len(),
            Body::File(_) => 0,
        };
        if collect_row {
            // Mem body is the same allocation the ref map holds, charged
            // once under the chunk gauge (M7); rows carry metadata only
            self.row_bytes += CHUNK_REF_META;
            self.row_gauge.add(CHUNK_REF_META);
            self.rows.push(ToastRowRef::with_body(&c, body.clone()));
        }
        self.chunk_bytes += mem_len + CHUNK_REF_META;
        self.chunk_gauge.add(mem_len + CHUNK_REF_META);
        match self.chunks.entry((c.toast_relid, c.value_id)) {
            Entry::Occupied(mut o) => {
                o.get_mut().push(c.chunk_seq, body);
            }
            Entry::Vacant(v) => {
                v.insert(ValueRef::new(c.chunk_seq, body));
            }
        }
        Ok(())
    }

    fn fold_delete(&mut self, d: ToastDelete) {
        if !self.collect_rows {
            return;
        }
        self.row_bytes += CHUNK_REF_META;
        self.row_gauge.add(CHUNK_REF_META);
        self.rows.push(ToastRowRef::tombstone(&d));
    }

    /// Decode a stashed record against its commit-time verdict: toast heaps
    /// fold chunks/tombstones into the same maps as live-path entries (so
    /// same-xact referrers detoast and mirror rows flow), ordinary heaps
    /// yield one input, unresolvable filenodes (end-state-neutral verdicts
    /// only) count and drop
    fn fold_raw(
        &mut self,
        raw: &RawRecord,
    ) -> std::result::Result<Option<Box<DescribedInput>>, XactBufferError> {
        let bump = |c: &dyn Fn(&EmitterStats) -> &AtomicU64| {
            if let Some(s) = &self.stash.stats {
                c(s).fetch_add(1, Ordering::Relaxed);
            }
        };
        let outcome = raw
            .rfn()
            .and_then(|rfn| Some((rfn.rel_node, self.stash.outcomes.get(&rfn)?)));
        let rel = match outcome {
            Some((_, StashOutcome::Toast(rel))) => rel.clone(),
            Some((rel_node, StashOutcome::Ordinary(history))) => {
                let lsn = raw.source_lsn;
                let (rel, valid_from) = history.at(lsn).map_err(|miss| match miss {
                    LookupMiss::Ambiguous(a) => XactBufferError::StashAmbiguous {
                        rel_node,
                        lsn,
                        from_lsn: a.from_lsn,
                        through_lsn: a.through_lsn,
                    },
                    _ => XactBufferError::StashUncovered { rel_node, lsn },
                })?;
                return self.fold_raw_ordinary(raw, rel, valid_from);
            }
            // No outcome = the filenode resolved Dropped / Retired /
            // NotCovered: rotated or dropped by this commit or a later
            // covered one, so under AccessExclusiveLock no row on it
            // outlives the commit. That argument, not the fence, is what
            // makes this discard sound
            None => {
                bump(&|s| &s.toast_stash_discarded);
                return Ok(None);
            }
        };
        bump(&|s| &s.toast_stash_decoded);
        let ops = decode_stashed_toast(raw, &rel)?;
        if let Some(s) = &self.stash.stats {
            s.raw_decode_toast_ops.bump(raw.rm, raw.info);
            s.raw_decode_rows_ops
                .add(raw.rm, raw.info, ops.len() as u64);
        }
        for op in ops {
            match op {
                StashedToastOp::Chunk(c) => self.fold_chunk(c)?,
                StashedToastOp::Delete(d) => self.fold_delete(d),
            }
        }
        Ok(None)
    }

    /// Validate one ordinary raw record into an input under its resolved
    /// descriptor, `None` when it yields no rows. Rebuilt header carries the
    /// writer xid, so rows keep the source `_xid`.
    ///
    /// Decode records with complete row data and skip page maintenance.
    /// Stop on any other operation to avoid losing rows.
    fn fold_raw_ordinary(
        &mut self,
        raw: &RawRecord,
        rel: Arc<RelDescriptor>,
        valid_from: u64,
    ) -> std::result::Result<Option<Box<DescribedInput>>, XactBufferError> {
        use crate::decode::heap_decoder::{
            XLOG_HEAP_CONFIRM, XLOG_HEAP_DELETE, XLOG_HEAP_HOT_UPDATE, XLOG_HEAP_INSERT,
            XLOG_HEAP_LOCK, XLOG_HEAP_OPMASK, XLOG_HEAP_UPDATE, XLOG_HEAP2_MULTI_INSERT,
        };
        let fail = |reason: FailClosedReason| XactBufferError::OrdinaryFailClosed {
            relid: rel.oid,
            lsn: raw.source_lsn,
            rm: raw.rm,
            info: raw.info,
            reason,
        };
        let rec = raw.to_xlog_record();
        // FPI consumed the registered tuple data; wal_level=logical retains
        // it (REGBUF_KEEP_DATA), so this means a non-logical writer
        let image_only = rec
            .blocks
            .first()
            .is_some_and(|b| b.header.has_image() && !b.header.has_data());
        let op = raw.info & XLOG_HEAP_OPMASK;
        if raw.rm == RmId::Heap as u8 {
            match op {
                XLOG_HEAP_INSERT | XLOG_HEAP_UPDATE | XLOG_HEAP_HOT_UPDATE if image_only => {
                    return Err(fail(FailClosedReason::ImageOnly));
                }
                // DELETE carries its whole payload in main_data
                XLOG_HEAP_INSERT | XLOG_HEAP_UPDATE | XLOG_HEAP_HOT_UPDATE | XLOG_HEAP_DELETE => {}
                // LOCK changes no row; CONFIRM finalizes a speculative
                // insert whose tuple decoded from its own record
                XLOG_HEAP_LOCK | XLOG_HEAP_CONFIRM => return Ok(None),
                // TRUNCATE intercepts at the sink, INPLACE never targets
                // ordinary heaps: reaching either breaks the verdict
                _ => return Err(fail(FailClosedReason::UnsupportedOperation)),
            }
        } else if raw.rm == RmId::Heap2 as u8 {
            if op != XLOG_HEAP2_MULTI_INSERT {
                // PRUNE/VACUUM/FREEZE/VISIBLE/LOCK_UPDATED/NEW_CID/REWRITE:
                // page maintenance, no logical row change
                return Ok(None);
            }
            if image_only {
                return Err(fail(FailClosedReason::ImageOnly));
            }
        } else {
            // Sink stashes heap/heap2 only; anything else broke that invariant
            return Err(fail(FailClosedReason::UnsupportedOperation));
        }
        // Only a MULTI_INSERT lacking XLH_INSERT_CONTAINS_NEW_TUPLE has no
        // shape: a sub-logical wal_level wrote it, and an FPI may have
        // consumed its tuple data
        let Some(shape) =
            record_shape(&rec).map_err(|e| fail(FailClosedReason::Malformed(e.to_string())))?
        else {
            return Err(fail(FailClosedReason::ImageOnly));
        };
        if let Some(s) = &self.stash.stats {
            s.raw_decode_ordinary_ops.bump(raw.rm, raw.info);
            s.raw_decode_rows_ops
                .add(raw.rm, raw.info, u64::from(shape.tuples));
        }
        // New-tuple prefix/suffix elision references a same-page predecessor
        // image this path cannot reconstruct
        if shape.partial {
            return Err(fail(FailClosedReason::PartialUpdate));
        }
        Ok(Some(Box::new(DescribedInput {
            input: HeapInput::from_parsed(&rec, raw.source_lsn, shape.tuples),
            descriptor: rel,
            descriptor_valid_from: valid_from,
        })))
    }

    /// Make appended bodies readable through the spool handle; no-op
    /// while memory-resident or when nothing buffered
    fn flush_spool(&mut self) -> std::result::Result<(), XactBufferError> {
        if let Some(s) = self.spool.as_mut() {
            s.flush()?;
        }
        Ok(())
    }

    fn spool_handle(&self) -> Option<Arc<BodySpoolFile>> {
        self.spool.as_ref().map(|s| s.shared().clone())
    }

    /// Chunks accumulated since the last take, sealed into a generation.
    /// Bytes stay gauged until the generation's last holder drops; spool
    /// flush makes the sealed refs readable to planning detoast.
    fn take_chunks(&mut self) -> std::result::Result<ChunkGeneration, XactBufferError> {
        self.flush_spool()?;
        let resident = self.chunk_gauge.split(self.chunk_bytes);
        self.chunk_bytes = 0;
        Ok(ChunkGeneration {
            map: std::mem::take(&mut self.chunks),
            spool: self.spool_handle(),
            _resident: resident,
            _permit: None,
        })
    }

    /// Rows collected since the last take; bytes stay gauged until the
    /// batch drops after its store put.
    fn take_rows(&mut self) -> ToastRowBatch {
        let resident = self.row_gauge.split(self.row_bytes);
        self.row_bytes = 0;
        ToastRowBatch {
            rows: std::mem::take(&mut self.rows),
            spool: self.spool_handle(),
            _resident: Some(resident),
        }
    }

    /// Every head empty and every event queue drained.
    fn is_exhausted(&self) -> bool {
        self.sources.iter().all(|s| s.head.is_none()) && self.events.iter().all(VecDeque::is_empty)
    }

    /// Unlink spill files + body spool; call only after dispatch
    /// completes. In-flight decode/store readers keep the spool via
    /// `Arc<BodySpoolFile>` open fds. An error path drops `self` instead,
    /// leaving files for inspection (startup wipe + redecode-from-ack
    /// cover replay).
    async fn finish(self) -> std::result::Result<(), XactBufferError> {
        if let Some(s) = self.spool {
            s.unlink()?;
        }
        for s in self.sources {
            if let SpillSide::Spilled { reader, .. } = s.spill {
                reader.unlink().await?;
            }
        }
        Ok(())
    }
}

/// Sealed, immutable chunk generation. Carries its resident-gauge share
/// (memory bodies + ref metadata, never file bodies) and, under an active
/// budget, its admission permit — both released when the last `Arc`
/// drops, since the generation outlives the slice that first shipped it
/// (retained by the drain and every later decode job). Container
/// hand-off is not release. Spool handle backs `File` refs; may mix with
/// `Mem` bodies in the generation sealed at the threshold crossing.
pub struct ChunkGeneration {
    map: ChunkRefMap,
    spool: Option<Arc<BodySpoolFile>>,
    _resident: ResidentGauge,
    _permit: Option<crate::budget::MemoryPermit>,
}

impl ChunkGeneration {
    pub fn map(&self) -> &ChunkRefMap {
        &self.map
    }

    pub fn spool(&self) -> Option<&BodySpoolFile> {
        self.spool.as_deref()
    }

    /// Gauged resident bytes (Mem bodies + ref metadata), the admission
    /// share this generation contributes
    pub fn resident_bytes(&self) -> usize {
        self._resident.held as usize
    }
}

impl std::ops::Deref for ChunkGeneration {
    type Target = ChunkRefMap;
    fn deref(&self) -> &ChunkRefMap {
        &self.map
    }
}

/// WAL-ordered mirror row refs with their resident-gauge share (ref
/// metadata only — Mem bodies are shared with the generation's map and
/// charged there); bytes count until the batch drops after its store put.
#[derive(Default)]
pub struct ToastRowBatch {
    rows: Vec<ToastRowRef>,
    spool: Option<Arc<BodySpoolFile>>,
    _resident: Option<ResidentGauge>,
}

impl ToastRowBatch {
    pub fn spool(&self) -> Option<&BodySpoolFile> {
        self.spool.as_deref()
    }
}

impl std::ops::Deref for ToastRowBatch {
    type Target = [ToastRowRef];
    fn deref(&self) -> &[ToastRowRef] {
        &self.rows
    }
}

/// Event ordered before `heaps[heap_idx]`, after `new_rows[..row_idx]`
pub struct OrderedEvent {
    pub heap_idx: usize,
    pub row_idx: usize,
    pub event: DrainEntry,
}

/// One bounded slice of a committed xact for the parallel pipeline. Heaps
/// still TOAST-toasted; the planner handles detoast + routing.
/// Non-empty `ordered_events` (or a `HeapOp::Truncate` heap) makes the
/// slice a barrier the reorder coordinator serializes against ClickHouse.
pub struct DrainedBatch {
    /// `source_lsn` ASC within the slice; later slices strictly follow.
    pub heaps: Vec<DrainHeap>,
    pub ordered_events: Vec<OrderedEvent>,
    /// Chunk generations sealed so far, oldest first. A chunk's WAL position
    /// precedes its referrer, so every heap's value lives in exactly one
    /// generation sealed no later than this slice; slices share payloads via
    /// `Arc` instead of copying per batch, and each generation is immutable
    /// once sealed (planning reads while later slices load).
    pub chunks: Vec<Arc<ChunkGeneration>>,
    /// WAL-ordered births and tombstones, empty without store
    pub new_rows: ToastRowBatch,
    /// `new_rows` cursor for each TRUNCATE heap
    pub truncate_rows: Vec<usize>,
    /// Last slice of the commit. Only its seq may publish `commit_lsn` in
    /// the ack (`Publish::Commit` vs `Partial`): an earlier slice
    /// publishing would claim durability for rows still in flight.
    pub is_final: bool,
}

impl DrainedBatch {
    #[cfg(any(test, feature = "test-support"))]
    /// Owned cells of every row change, inputs decoded through the cell
    /// adapter
    pub fn cells(&self) -> std::result::Result<Vec<DescribedHeap>, XactBufferError> {
        let mut out = Vec::with_capacity(self.heaps.len());
        for h in &self.heaps {
            out.extend(h.clone().into_cells()?);
        }
        Ok(out)
    }
}

/// One step of a [`DrainedBatch`] apply plan (see [`DrainedBatch::into_walk`]).
pub enum WalkStep {
    /// Seal store rows: put `new_rows[cursor..upto]` before the next step
    /// (`upto` may equal the cursor — nothing to put).
    Rows {
        upto: usize,
    },
    Event(DrainEntry),
    /// Follows the `Rows` step sealing `new_rows[..upto]`
    Truncate {
        truncate: Box<Truncate>,
        upto: usize,
    },
    Heap(Box<DescribedInput>),
}

/// [`DrainedBatch`] decomposed into its apply plan plus the payload fields
/// consumers read alongside the steps.
pub struct DrainWalk {
    pub steps: Vec<WalkStep>,
    pub chunks: Vec<Arc<ChunkGeneration>>,
    pub new_rows: ToastRowBatch,
    pub is_final: bool,
}

impl DrainedBatch {
    /// Cursor-ordered apply plan — the single implementation of the
    /// `ordered_events` / `truncate_rows` interleave: an event fires before
    /// the heap it sorts ahead of, `Rows` seals store births/deaths before
    /// each event / truncate and once at the tail. Reorder barriers and
    /// backup gap replay both consume this.
    pub fn into_walk(self) -> DrainWalk {
        let DrainedBatch {
            heaps,
            ordered_events,
            chunks,
            new_rows,
            truncate_rows,
            is_final,
        } = self;
        let mut steps =
            Vec::with_capacity(heaps.len() + 2 * ordered_events.len() + truncate_rows.len() + 1);
        let mut events = ordered_events.into_iter().peekable();
        let mut trunc = truncate_rows.into_iter();
        for (heap_idx, heap) in heaps.into_iter().enumerate() {
            while let Some(e) = events.next_if(|e| e.heap_idx <= heap_idx) {
                steps.push(WalkStep::Rows { upto: e.row_idx });
                steps.push(WalkStep::Event(e.event));
            }
            match heap {
                DrainHeap::Truncate(truncate) => {
                    let upto = trunc
                        .next()
                        .expect("truncate_rows cursor per Truncate heap");
                    steps.push(WalkStep::Rows { upto });
                    steps.push(WalkStep::Truncate { truncate, upto });
                }
                DrainHeap::Input(input) => steps.push(WalkStep::Heap(input)),
            }
        }
        for e in events {
            steps.push(WalkStep::Rows { upto: e.row_idx });
            steps.push(WalkStep::Event(e.event));
        }
        steps.push(WalkStep::Rows {
            upto: new_rows.len(),
        });
        DrainWalk {
            steps,
            chunks,
            new_rows,
            is_final,
        }
    }
}

/// Streaming handle for one committed xact: pull [`DrainedBatch`] slices,
/// then [`Self::finish`] to unlink spill files once dispatch completes.
pub struct CommittedDrain {
    pub commit_ts: i64,
    pub commit_lsn: u64,
    /// `None` for read-only / filter-dropped / unknown xid
    merged: Option<Drained>,
    generations: Vec<Arc<ChunkGeneration>>,
}

/// Next slice of a merged drain, see [`CommittedDrain::next_batch_admitting`]
async fn next_merged<E: From<XactBufferError>>(
    m: &mut MergedDrain,
    generations: &mut Vec<Arc<ChunkGeneration>>,
    max_rows: usize,
    max_bytes: usize,
    admit: impl AsyncFnOnce(usize) -> std::result::Result<Option<crate::budget::MemoryPermit>, E>,
) -> std::result::Result<Option<DrainedBatch>, E> {
    let mut heaps: Vec<DrainHeap> = Vec::new();
    let mut ordered_events: Vec<OrderedEvent> = Vec::new();
    let mut truncate_rows: Vec<usize> = Vec::new();
    let mut bytes = 0usize;
    while heaps.len() < max_rows.max(1) && bytes < max_bytes.max(1) {
        match m.next().await? {
            None => break,
            Some(MergeItem::Event(event)) => ordered_events.push(OrderedEvent {
                heap_idx: heaps.len(),
                row_idx: m.rows.len(),
                event: *event,
            }),
            Some(MergeItem::Truncate(t)) => {
                truncate_rows.push(m.rows.len());
                heaps.push(DrainHeap::Truncate(t));
            }
            Some(MergeItem::Input(i)) => {
                bytes += i.approx_bytes();
                heaps.push(DrainHeap::Input(i));
            }
        }
    }
    let is_final = m.is_exhausted();
    let mut sealed = m.take_chunks()?;
    let sealed_empty = sealed.map.is_empty();
    let new_rows = m.take_rows();
    if !sealed_empty {
        sealed._permit = admit(sealed.resident_bytes()).await?;
        generations.push(Arc::new(sealed));
    }
    if heaps.is_empty() && ordered_events.is_empty() && sealed_empty && new_rows.is_empty() {
        return Ok(None);
    }
    Ok(Some(DrainedBatch {
        heaps,
        ordered_events,
        chunks: generations.clone(),
        new_rows,
        truncate_rows,
        is_final,
    }))
}

enum Drained {
    /// Boxed so the commit path moves a pointer, not the merge state
    Merged(Box<MergedDrain>),
    /// WAL-ordered heaps needing no merge, within one slice's budget
    Heaps(Vec<DrainHeap>),
}

impl CommittedDrain {
    pub fn had_states(&self) -> bool {
        self.merged.is_some()
    }

    /// Next slice, `None` once exhausted. The slice closes at the first
    /// heap reaching `max_rows` / `max_bytes` (budget is a trigger, not a
    /// hard cap: one oversized row still ships alone). Slices only cut at
    /// heap boundaries, so a value's contiguous chunk run never splits
    /// across generations.
    pub async fn next_batch(
        &mut self,
        max_rows: usize,
        max_bytes: usize,
        budget: Option<&crate::budget::MemoryBudget>,
    ) -> std::result::Result<Option<DrainedBatch>, XactBufferError> {
        self.next_batch_admitting(max_rows, max_bytes, async |bytes| {
            Ok(crate::budget::admit_opt(budget, bytes).await)
        })
        .await
    }

    /// [`Self::next_batch`] with caller admitting a sealed chunk generation.
    /// Caller holding admission permits must release them before waiting,
    /// else a request its own permits block never returns
    pub async fn next_batch_admitting<E: From<XactBufferError>>(
        &mut self,
        max_rows: usize,
        max_bytes: usize,
        admit: impl AsyncFnOnce(usize) -> std::result::Result<Option<crate::budget::MemoryPermit>, E>,
    ) -> std::result::Result<Option<DrainedBatch>, E> {
        let m = match self.merged.as_mut() {
            None => return Ok(None),
            Some(Drained::Merged(m)) => m,
            Some(Drained::Heaps(heaps)) if heaps.is_empty() => return Ok(None),
            Some(Drained::Heaps(heaps)) => {
                return Ok(Some(DrainedBatch {
                    heaps: std::mem::take(heaps),
                    ordered_events: Vec::new(),
                    chunks: Vec::new(),
                    new_rows: ToastRowBatch::default(),
                    truncate_rows: Vec::new(),
                    is_final: true,
                }));
            }
        };
        // Boxed: merge state would bloat every commit future
        Box::pin(next_merged(
            m,
            &mut self.generations,
            max_rows,
            max_bytes,
            admit,
        ))
        .await
    }

    /// Unlink spill files; call after the final slice dispatches. On an
    /// error path drop instead: files stay for inspection, startup wipe +
    /// redecode-from-ack cover replay.
    pub async fn finish(mut self) -> std::result::Result<(), XactBufferError> {
        if let Some(Drained::Merged(m)) = self.merged.take() {
            m.finish().await?;
        }
        Ok(())
    }

    /// Files on disk for [`Self::finish`] to unlink; without any, dropping
    /// the drain is the whole finish
    pub fn spilled(&self) -> bool {
        let Some(Drained::Merged(m)) = &self.merged else {
            return false;
        };
        m.spool.is_some()
            || m.sources
                .iter()
                .any(|s| !matches!(s.spill, SpillSide::Unspilled))
    }
}

/// External value of one tuple image attribute
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ToastSite {
    /// Caller's image index
    pub image: usize,
    /// Attribute index, attnum - 1
    pub idx: usize,
    pub p: ToastPointer,
}

#[cfg(any(test, feature = "test-support"))]
/// Resolve owned cells' external values in place, see [`detoast_sites`]
pub async fn detoast_heap(
    heap: &mut DescribedHeap,
    spool: Option<&BodySpoolFile>,
    chunk_maps: &[&ChunkRefMap],
    resolver: &ToastResolver,
) -> std::result::Result<Option<crate::budget::MemoryPermit>, XactBufferError> {
    let mut images = [heap.decoded.new.as_mut(), heap.decoded.old.as_mut()];
    let mut sites = Vec::new();
    for (image, t) in images.iter().enumerate() {
        for (idx, c) in t.iter().flat_map(|t| t.columns.iter().enumerate()) {
            if let Some(Datum::External(p)) = *c {
                sites.push(ToastSite { image, idx, p });
            }
        }
    }
    let (lsn, db) = (heap.decoded.source_lsn, heap.decoded.rfn.db_node);
    let (values, permit) = detoast_sites(&sites, db, lsn, spool, chunk_maps, resolver).await?;
    for (site, value) in sites.iter().zip(values) {
        let t = images[site.image]
            .as_mut()
            .expect("site of a present image");
        t.columns[site.idx] = Some(value);
    }
    Ok(permit)
}

/// Resolve every site of one heap change's images, values aligned with
/// `sites`. Returns the leaf permit shrunk to the decoded bytes retained;
/// the caller rides it with the routed row to insert ack so decoded values
/// (and their encoder slab copy) stay covered past this call. `None`
/// without budget or external values
pub async fn detoast_sites(
    sites: &[ToastSite],
    db: u32,
    lsn: u64,
    // Xact body spool backing `File` refs; None while memory-resident
    spool: Option<&BodySpoolFile>,
    // Ref-map generations, oldest first; a value lives in exactly one
    // (live map on the serial path, sealed drain-batch generations on the
    // parallel path)
    chunk_maps: &[&ChunkRefMap],
    resolver: &ToastResolver,
) -> std::result::Result<(Vec<Datum>, Option<crate::budget::MemoryPermit>), XactBufferError> {
    // Transfer speculative values before admitting foreground resolution
    let mut ahead = None;
    let mut values: Vec<Option<Datum>> = Vec::with_capacity(sites.len());
    for site in sites {
        values.push(resolver.take_ahead(db, &site.p, lsn, &mut ahead).await);
    }
    let mut pointers: Vec<ToastPointer> = sites
        .iter()
        .zip(&values)
        .filter(|(_, v)| v.is_none())
        .map(|(site, _)| site.p)
        .collect();
    if pointers.is_empty() {
        return Ok((values.into_iter().flatten().collect(), ahead));
    }
    let leaf_need = check_value_caps(
        pointers.iter().copied(),
        resolver.inline_value_max(),
        resolver.overflow(),
    )?;
    // Oversized values need no fetch under null policy
    pointers.retain(|p| !resolver.value_oversize(p));
    // One leaf at a time per worker: reserved for the heap's aggregate
    // resolution peak (every retained decoded value + the largest
    // single-value transient), shrunk to retained bytes before return
    let mut leaf = crate::budget::acquire_opt(resolver.budget(), leaf_need).await;
    let cache = prefetch_store_values(&pointers, chunk_maps, resolver, db, lsn).await?;
    let mut res = ValueResolution {
        spool,
        xact_maps: chunk_maps,
        resolver,
        cache,
        retained: 0,
    };
    let values = sites
        .iter()
        .zip(values)
        .map(|(site, v)| match v {
            Some(v) => Ok(v),
            None => res.resolve(&site.p),
        })
        .collect::<std::result::Result<Vec<_>, _>>()?;
    if let Some(p) = leaf.as_mut() {
        p.shrink(res.retained as u64);
    }
    if let Some(ahead) = ahead {
        crate::budget::absorb_into(&mut leaf, ahead);
    }
    Ok((values, leaf))
}

/// External datums of tuple images, image index by position in `images`
pub fn external_sites<'v, 'a: 'v>(
    images: impl IntoIterator<Item = Option<&'v TupleView<'a>>>,
) -> Vec<ToastSite> {
    let mut sites = Vec::new();
    for (image, t) in images.into_iter().enumerate() {
        for (idx, d) in t.iter().flat_map(|t| t.datums.iter().enumerate()) {
            if let DatumRef::External(p) = *d {
                sites.push(ToastSite { image, idx, p });
            }
        }
    }
    sites
}

/// Fetch every pointer the xact's chunk maps can't cover in one round
/// trip per toast rel, so resolution never waits a value at a time. Ids
/// dedup: a key used by both tuples fetches once.
///
/// `bound` is the referrer's LSN, one per heap. Batching a wider unit
/// would mix bounds under one key, so it needs per-value bounds in the
/// store query
async fn prefetch_store_values(
    pointers: &[ToastPointer],
    chunk_maps: &[&ChunkRefMap],
    resolver: &ToastResolver,
    db_oid: u32,
    bound: u64,
) -> std::result::Result<HashMap<(u32, u32), CachedValue>, XactBufferError> {
    let mut cache = HashMap::new();
    if resolver.fill_on_miss() {
        return Ok(cache);
    }
    let mut wanted: HashMap<u32, HashMap<u32, (ToastPointer, u32)>> = HashMap::new();
    for p in pointers {
        let key = (p.va_toastrelid, p.va_valueid);
        if let Some(v) = chunk_maps.iter().find_map(|m| m.get(&key)) {
            if !observed_mid_value(v) {
                continue;
            }
            tracing::warn!(
                target: "walshadow::toast",
                toast_relid = p.va_toastrelid,
                value_id = p.va_valueid,
                first_seq = v.tail.keys().next().copied(),
                "toast value's leading chunks precede the stream start; resolving it from the store",
            );
        }
        let (pointer, uses) = wanted
            .entry(p.va_toastrelid)
            .or_default()
            .entry(p.va_valueid)
            .or_insert((*p, 0));
        *pointer = *p;
        *uses += 1;
    }
    for (toast_relid, ptrs) in wanted {
        let ptrs: Vec<_> = ptrs.into_values().collect();
        let batch: Vec<(u32, usize)> = ptrs
            .iter()
            .map(|(p, _)| (p.va_valueid, pointer_extsize(p)))
            .collect();
        let got = resolver
            .fetch_values(db_oid, toast_relid, &batch, bound)
            .await
            .map_err(|e| XactBufferError::Detoast(format!("toast store fetch: {e}")))?
            .expect("store checked via fill_on_miss");
        for ((p, uses), fetched) in ptrs.iter().zip(got) {
            let cached = match StoreMiss::split(Some(fetched)) {
                Ok(stored) => Ok(finish_value(p, stored)?),
                Err(miss) => Err(miss),
            };
            cache.insert(
                (p.va_toastrelid, p.va_valueid),
                CachedValue {
                    remaining: *uses,
                    value: cached,
                },
            );
        }
    }
    Ok(cache)
}

/// Store-fetched value decoded once per key; cloned for all but the last
/// use, which moves the buffer
struct CachedValue {
    remaining: u32,
    value: std::result::Result<Vec<u8>, StoreMiss>,
}

/// Who owns chunk store contents, deciding what an unresolved value means
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MissPolicy {
    /// Referrer outlived its value: safe only after supersession or replayed
    /// owner TRUNCATE, later row version carries current value
    Streaming,
    /// Walk-seeded store must contain chunks written during this pass, so a
    /// miss is a gap
    BootstrapOwned,
    /// Read-only store holds state at end of backup: missing value was
    /// removed after copied row and a later row version supersedes it
    BootstrapReadOnly,
}

impl MissPolicy {
    pub(crate) fn bootstrap(resolver: &ToastResolver) -> Self {
        if resolver.stores_chunks() {
            Self::BootstrapOwned
        } else {
            Self::BootstrapReadOnly
        }
    }
}

/// Store fetch outcome without assembled bytes
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StoreMiss {
    /// No store to consult
    NoStore,
    /// No live chunk visible at bound
    Missing,
    Mismatch {
        got: usize,
    },
    /// Value id now holds a later generation, original is unreadable
    Generation,
}

impl StoreMiss {
    /// Split assembled bytes from misses; `None` is a store-less fetch
    pub(crate) fn split(fetched: Option<FetchedValue>) -> std::result::Result<Vec<u8>, Self> {
        match fetched {
            None => Err(Self::NoStore),
            Some(FetchedValue::Assembled(stored)) => Ok(stored),
            Some(FetchedValue::Missing) => Err(Self::Missing),
            Some(FetchedValue::Mismatch { got }) => Err(Self::Mismatch { got }),
            Some(FetchedValue::Generation) => Err(Self::Generation),
        }
    }
}

/// Fill an unresolved value with NULL per `policy`, counting its cause.
/// `Err` carries miss detail when policy fails closed
pub(crate) fn fill_store_miss(
    miss: StoreMiss,
    p: &ToastPointer,
    resolver: &ToastResolver,
    policy: MissPolicy,
) -> std::result::Result<Datum, String> {
    match (miss, policy) {
        (StoreMiss::NoStore, _) => resolver.note_filled_default(),
        (StoreMiss::Generation, _) => resolver.note_filled_generation(),
        (StoreMiss::Mismatch { .. }, MissPolicy::Streaming) => resolver.note_filled_mismatch(),
        (StoreMiss::Missing, MissPolicy::Streaming) | (_, MissPolicy::BootstrapReadOnly) => {
            resolver.note_filled_superseded()
        }
        (StoreMiss::Missing, MissPolicy::BootstrapOwned) => {
            resolver.note_fetch_miss();
            return Err("has no chunks in the store".into());
        }
        (StoreMiss::Mismatch { got }, MissPolicy::BootstrapOwned) => {
            resolver.note_fetch_miss();
            return Err(format!(
                "chunks sum to {got} bytes, pointer says {}",
                pointer_extsize(p)
            ));
        }
    }
    Ok(Datum::Null)
}

/// Per-heap value resolution over prefetched store values, decoded bytes
/// tallied in `retained` for the leaf-permit shrink
struct ValueResolution<'a> {
    spool: Option<&'a BodySpoolFile>,
    xact_maps: &'a [&'a ChunkRefMap],
    resolver: &'a ToastResolver,
    cache: HashMap<(u32, u32), CachedValue>,
    retained: usize,
}

impl ValueResolution<'_> {
    fn resolve(&mut self, p: &ToastPointer) -> std::result::Result<Datum, XactBufferError> {
        if self.resolver.fills_oversize(p) {
            return Ok(Datum::Null);
        }
        let key = (p.va_toastrelid, p.va_valueid);
        let Some(v) = self
            .xact_maps
            .iter()
            .find_map(|m| m.get(&key))
            .filter(|v| !observed_mid_value(v))
        else {
            return self.resolve_store(p);
        };
        match reassemble_value_ref(p, self.spool, v)? {
            Reassembled::Bytes(raw) => {
                let (value, held) = detoasted_sized(raw);
                self.retained += held;
                Ok(value)
            }
            // Disabled mode: surface the unresolvable value as
            // NULL/default downstream (`append_default`), counted,
            // never an error.
            Reassembled::Missing { .. } if self.resolver.fill_on_miss() => {
                self.resolver.note_filled_default();
                Ok(Datum::Null)
            }
            // In-xact chunks gapped or short: decode bug, surfaced loud
            Reassembled::SizeMismatch { got, want } => {
                self.resolver.note_fetch_miss();
                Err(XactBufferError::Detoast(format!(
                    "toast value {}/{}: chunks sum to {got} bytes, pointer says {want}",
                    p.va_toastrelid, p.va_valueid
                )))
            }
            Reassembled::Missing { seq } => {
                self.resolver.note_fetch_miss();
                Err(XactBufferError::MissingToastChunk {
                    toast_relid: p.va_toastrelid,
                    value_id: p.va_valueid,
                    missing: seq,
                })
            }
        }
    }

    /// Pre-window / bootstrap value whose chunks aren't in this xact,
    /// assembled by [`prefetch_store_values`] against the pointer's
    /// stored size
    fn resolve_store(&mut self, p: &ToastPointer) -> std::result::Result<Datum, XactBufferError> {
        let key = (p.va_toastrelid, p.va_valueid);
        let cached = if self.resolver.fill_on_miss() {
            Err(StoreMiss::NoStore)
        } else {
            let Entry::Occupied(mut entry) = self.cache.entry(key) else {
                panic!("prefetched with the sites");
            };
            entry.get_mut().remaining -= 1;
            if entry.get().remaining > 0 {
                entry.get().value.clone()
            } else {
                entry.remove().value
            }
        };
        match cached {
            Ok(raw) => {
                let (value, held) = detoasted_sized(raw);
                self.retained += held;
                Ok(value)
            }
            Err(miss) => fill_store_miss(miss, p, self.resolver, MissPolicy::Streaming)
                .map_err(XactBufferError::Detoast),
        }
    }
}

fn observed_mid_value(v: &ValueRef) -> bool {
    if v.run_chunks > 0 {
        return false;
    }
    let mut seqs = v.tail.keys().copied();
    seqs.next()
        .is_some_and(|first| first > 0 && seqs.zip(first + 1..).all(|(seq, want)| seq == want))
}

/// Chunk coverage outcome, decompression failures remain errors
#[cfg_attr(test, derive(Debug, PartialEq))]
pub(crate) enum Reassembled {
    Bytes(Vec<u8>),
    /// First chunk seq absent from dense coverage
    Missing {
        seq: u32,
    },
    /// Dense run failing PostgreSQL stored-size check
    SizeMismatch {
        got: usize,
        want: usize,
    },
}

/// Reassemble one in-xact value from its refs: validate dense coverage
/// and pointer size BEFORE any read, then copy memory bodies /
/// positional-read spool ranges into one exact-size buffer, decompress
/// per the pointer's method. Tail keys below `run_chunks` are
/// byte-identical duplicates of run chunks (PG chunk immutability),
/// skipped.
pub(crate) fn reassemble_value_ref(
    p: &ToastPointer,
    spool: Option<&BodySpoolFile>,
    v: &ValueRef,
) -> std::result::Result<Reassembled, XactBufferError> {
    let mut total = v.run.len as usize;
    for (next, (&seq, b)) in (v.run_chunks..).zip(v.tail.range(v.run_chunks..)) {
        if seq != next {
            return Ok(Reassembled::Missing { seq: next });
        }
        total += b.len();
    }
    let extsize = pointer_extsize(p);
    if total != extsize {
        return Ok(Reassembled::SizeMismatch {
            got: total,
            want: extsize,
        });
    }
    let need_spool = || {
        spool.ok_or_else(|| XactBufferError::Detoast("file chunk refs without body spool".into()))
    };
    let read_err = |e: std::io::Error| XactBufferError::Detoast(format!("body spool read: {e}"));
    let mut concat = vec![0u8; total];
    let mut off = 0usize;
    if v.run.len > 0 {
        need_spool()?
            .read_at(v.run.offset, &mut concat[..v.run.len as usize])
            .map_err(read_err)?;
        off = v.run.len as usize;
    }
    for (_, b) in v.tail.range(v.run_chunks..) {
        match b {
            Body::Mem(bytes) => concat[off..off + bytes.len()].copy_from_slice(bytes),
            Body::File(r) => need_spool()?
                .read_at(r.offset, &mut concat[off..off + r.len as usize])
                .map_err(read_err)?,
        }
        off += b.len();
    }
    Ok(Reassembled::Bytes(finish_value(p, concat)?))
}

/// Chunk births keyed by value, with count in window and newest birth xid
#[derive(Default)]
struct RecentToastBirths {
    order: VecDeque<(u32, u32)>,
    counts: foldhash::HashMap<(u32, u32), (usize, u32)>,
    /// Subxid → top xid, from WAL naming a subtransaction's parent. Empty
    /// without savepoints
    tops: foldhash::HashMap<u32, u32>,
    tops_order: VecDeque<u32>,
}

impl RecentToastBirths {
    const WINDOW: usize = 1024;

    fn insert(&mut self, key: (u32, u32), xid: u32) {
        if self.order.len() == Self::WINDOW {
            let oldest = self.order.pop_front().unwrap();
            let (count, _) = self.counts.get_mut(&oldest).unwrap();
            *count -= 1;
            if *count == 0 {
                self.counts.remove(&oldest);
            }
        }
        self.order.push_back(key);
        let entry = self.counts.entry(key).or_default();
        *entry = (entry.0 + 1, xid);
    }

    fn link(&mut self, sub: u32, top: u32) {
        if self.tops.insert(sub, top).is_none() {
            if self.tops_order.len() == Self::WINDOW
                && let Some(oldest) = self.tops_order.pop_front()
            {
                self.tops.remove(&oldest);
            }
            self.tops_order.push_back(sub);
        }
    }

    fn top(&self, xid: u32) -> u32 {
        self.tops.get(&xid).copied().unwrap_or(xid)
    }

    /// Chunks born in referring transaction resolve from its own WAL. Values
    /// born in another transaction need store. Subtransactions share their
    /// top's WAL, recognized only once WAL links them, see PG
    /// `XLR_BLOCK_ID_TOPLEVEL_XID` (wal_level=logical) and
    /// `XLOG_XACT_ASSIGNMENT` (every 64 subxacts)
    fn born_in(&self, key: (u32, u32), xid: u32) -> bool {
        self.counts
            .get(&key)
            .is_some_and(|&(_, born)| born == xid || self.top(born) == self.top(xid))
    }
}

/// Start store reads of the values updated and deleted rows keep. An insert,
/// and a value born just before its row, carries its chunks in its own WAL
fn read_ahead_kept(
    ahead: &ToastResolver,
    rel: &RelDescriptor,
    (source_lsn, xid): (u64, u32),
    images: &[HeapImages<TupleView<'_>>],
    born: &std::sync::Mutex<RecentToastBirths>,
) {
    for d in images {
        if rel.kind == b't' {
            // `chunk_id oid` leads every toast tuple
            if rel
                .attributes
                .first()
                .is_some_and(|a| a.type_oid == crate::schema::OIDOID)
                && let Some(DatumRef::Fixed(id)) = d.new.as_ref().and_then(|t| t.datums.first())
                && let Ok(id) = <[u8; 4]>::try_from(*id)
            {
                born.lock()
                    .expect("recent toast births")
                    .insert((rel.oid, u32::from_le_bytes(id)), xid);
            }
            continue;
        }
        for t in [&d.new, &d.old].into_iter().flatten() {
            for datum in &t.datums {
                if let &DatumRef::External(p) = datum {
                    let own = born
                        .lock()
                        .expect("recent toast births")
                        .born_in((p.va_toastrelid, p.va_valueid), xid);
                    if !own {
                        ahead.read_ahead(rel.rfn.db_node, p, source_lsn);
                    }
                }
            }
        }
    }
}

/// Decodes `Route::ToDecoder` user-heap records into the xact buffer.
/// Toast-relation INSERTs (`rel.kind == 't'`) reinterpret as
/// [`ToastChunk`]; semantic errors absorb into [`DecoderStats`] rather
/// than poison the stream.
pub struct BufferingDecoderSink {
    logs: DescriptorLogs,
    buffer: Arc<Mutex<XactBuffer>>,
    /// `None` keeps the unconditional locked check
    stash_live: Option<Arc<AtomicU64>>,
    stats: Arc<DecoderStats>,
    /// `txn` span registry. When set (tracing on), the decoder parents its
    /// per-record `decode` spans under the xact's `txn` span (via
    /// `decode_parent`, set only for the first record). `None` ⇒ those
    /// spans are skipped (no parent to attach to).
    span_registry: Option<TxnSpanRegistry>,
    /// Source-PG schema holding the `config_*` overlay tables. `Some` diverts
    /// their heap writes to `on_config_event` (never CH); `None` = overlay off.
    config_schema: Option<Arc<str>>,
}

impl BufferingDecoderSink {
    /// Timed acquire of the shared buffer mutex, so contention with the commit
    /// drain is measurable rather than inferred
    async fn lock_buffer(&self) -> tokio::sync::MutexGuard<'_, XactBuffer> {
        self.stats
            .buffer_lock_acquires
            .fetch_add(1, Ordering::Relaxed);
        // Uncontended acquire blocks for nothing, so skip the clock
        if let Ok(guard) = self.buffer.try_lock() {
            return guard;
        }
        self.stats.buffer_lock_waits.fetch_add(1, Ordering::Relaxed);
        let started = std::time::Instant::now();
        let guard = self.buffer.lock().await;
        self.stats
            .buffer_lock_wait_nanos
            .fetch_add(started.elapsed().as_nanos() as u64, Ordering::Relaxed);
        guard
    }

    /// Scan half of [`RecordSink::on_record`], for a dispatcher to run ahead
    /// of the worker: starts store reads of values updated rows keep,
    /// through `ahead`, borrowing record bytes and building no cells
    pub fn prescan(&self, ahead: ToastResolver) -> impl Fn(&Record<'_>) + Send + Sync + 'static {
        use crate::decode::heap_decoder::{
            XLOG_HEAP_INSERT, XLOG_HEAP_OPMASK, XLOG_HEAP_TRUNCATE, XLOG_HEAP2_MULTI_INSERT,
        };
        let logs = self.logs.clone();
        let born = std::sync::Mutex::new(RecentToastBirths::default());
        move |record| {
            let header = &record.parsed.header;
            if record.parsed.toplevel_xid != 0 {
                born.lock()
                    .expect("recent toast births")
                    .link(header.xact_id, record.parsed.toplevel_xid);
            }
            let rm = header.resource_manager_id;
            let op = header.info & XLOG_HEAP_OPMASK;
            let insert = if rm == RmId::Heap as u8 {
                op == XLOG_HEAP_INSERT
            } else if rm == RmId::Heap2 as u8 {
                op == XLOG_HEAP2_MULTI_INSERT
            } else {
                if rm == RmId::Xact as u8
                    && header.info & XLOG_XACT_OPMASK == XLOG_XACT_ASSIGNMENT
                    && let Some((top, subs)) = parse_xact_assignment(&record.parsed.main_data)
                {
                    let mut born = born.lock().expect("recent toast births");
                    for sub in subs {
                        born.link(sub, top);
                    }
                }
                return;
            };
            if rm == RmId::Heap as u8 && op == XLOG_HEAP_TRUNCATE
                || !matches!(record.route, Route::ToDecoder | Route::ToBoth)
                || record.defer_catalog_decode
            {
                return;
            }
            let Some(Ok((rel, _))) = record.parsed.blocks.first().and_then(|b| {
                let rfn = b.header.location.rel;
                logs.get(rfn.db_node)
                    .map(|log| log.peek_spanned(rfn, record.source_lsn))
            }) else {
                return;
            };
            // Ordinary inserts keep no older value; toast inserts are births
            if insert && rel.kind != b't' {
                return;
            }
            let images = view_heap_record(&record.parsed, &rel).unwrap_or_default();
            let at = (record.source_lsn, record.parsed.header.xact_id);
            read_ahead_kept(&ahead, &rel, at, &images, &born);
        }
    }

    pub fn new(logs: DescriptorLogs, buffer: Arc<Mutex<XactBuffer>>) -> Self {
        Self {
            logs,
            buffer,
            stash_live: None,
            stats: Arc::new(DecoderStats::default()),
            span_registry: None,
            config_schema: None,
        }
    }

    pub fn with_stash_live(mut self, live: Arc<AtomicU64>) -> Self {
        self.stash_live = Some(live);
        self
    }

    /// Names the source-PG schema whose `config_*` tables carry the runtime
    /// config overlay (`[runtime_config] schema`). Their heap writes divert to
    /// `on_config_event` instead of CH routing (plan §2); `None` keeps the
    /// decoder overlay-unaware.
    pub fn with_config_schema(mut self, schema: Arc<str>) -> Self {
        self.config_schema = Some(schema);
        self
    }

    /// Wire the [`TxnSpanRegistry`] so per-record decode spans nest under the
    /// xact's `txn` span. Pass the same registry the WAL pump registers xids
    /// into ([`XactBuffer::with_span_registry`]).
    pub fn with_span_registry(mut self, registry: TxnSpanRegistry) -> Self {
        self.span_registry = Some(registry);
        self
    }

    pub fn stats(&self) -> &DecoderStats {
        &self.stats
    }

    pub fn stats_handle(&self) -> Arc<DecoderStats> {
        self.stats.clone()
    }

    #[inline(always)]
    fn skip(&self, reason: SkipReason, record: &Record<'_>, rfn: RelFileNode) {
        let header = &record.parsed.header;
        let (db, rel) = (rfn.db_node, rfn.rel_node);
        self.stats.skip(reason, header, record.source_lsn, db, rel);
    }

    /// Stash raw inputs for a record whose filenode is invisible at record
    /// time. Marker-proven filenodes keep payload for commit-time decode
    /// ([`resolve_stash`]); markerless ones are tracked payload-free so a
    /// toast resolution can fail closed on the incomplete set.
    async fn stash_invisible(
        &mut self,
        record: &Record<'_>,
        rfn: walrus::pg::walparser::RelFileNode,
    ) -> std::result::Result<(), SinkError> {
        let xid = record.parsed.header.xact_id;
        let mut buf = self.lock_buffer().await;
        if buf.marker_lsn(rfn).is_some() {
            let raw = crate::xact::spill::RawRecord::from_parsed(
                &record.parsed,
                record.source_lsn,
                record.page_magic,
            );
            let (rm, info) = (raw.rm, raw.info);
            buf.stash_raw(xid, raw).await.map_err(SinkError::from)?;
            self.stats
                .toast_stash_buffered
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            self.stats.raw_stash_marker_ops.bump(rm, info);
        } else {
            buf.track_unresolvable(xid, record.source_lsn, rfn);
            self.skip(SkipReason::Unresolvable, record, rfn);
        }
        Ok(())
    }

    /// Push one `HeapOp::Truncate` per relation. TRUNCATE uniquely
    /// carries pg_class OIDs (not relfilenodes) and no block ref, so the
    /// standard by-rfn lookup doesn't fit.
    async fn handle_truncate(&mut self, record: &Record<'_>) -> std::result::Result<(), SinkError> {
        let Some(parsed) =
            crate::filter::main_data::parse_xl_heap_truncate(&record.parsed.main_data)
        else {
            self.stats.skip(
                SkipReason::TruncateUnparsed,
                &record.parsed.header,
                record.source_lsn,
                0,
                0,
            );
            return Ok(());
        };
        let xid = record.parsed.header.xact_id;
        let source_lsn = record.source_lsn;
        for relid in parsed.relids {
            // Same-xact CREATE + TRUNCATE: the rel's Added has no batch yet
            // (capture runs at commit) → NotCovered, nothing lives to wipe.
            // Ambiguous folds into the same skip: TRUNCATE reads no tuple, so
            // the fence has nothing to protect, and an interval covering this
            // LSN is unreachable anyway — TRUNCATE rotates the filenode (so
            // its own commit publishes no in-place verdict) and a concurrent
            // xact cannot hold this rel's AccessExclusiveLock
            // Relid array belongs to a database this daemon does not
            // follow: its OIDs name nothing here
            let Some(log) = self.logs.lookup(parsed.db_oid) else {
                break;
            };
            let (rel, valid_from) =
                match log.descriptor_by_oid_in_db_at_spanned(parsed.db_oid, relid, source_lsn) {
                    Ok(found) => found,
                    Err(_) => {
                        self.stats.skip(
                            SkipReason::TruncateRelMissing,
                            &record.parsed.header,
                            record.source_lsn,
                            parsed.db_oid,
                            relid,
                        );
                        continue;
                    }
                };
            // CH has no per-table internal toast; only user heap
            // ('r'/'p') TRUNCATE propagates
            if rel.kind != b'r' && rel.kind != b'p' {
                continue;
            }
            self.stats.record_op(HeapOp::Truncate, 1, false);
            let mut buf = self.lock_buffer().await;
            buf.on_truncate(Truncate {
                xid,
                source_lsn,
                descriptor: rel,
                descriptor_valid_from: valid_from,
            })
            .await
            .map_err(SinkError::from)?;
        }
        Ok(())
    }
}

impl BufferingDecoderSink {
    async fn handle(&mut self, record: &Record<'_>) -> std::result::Result<(), SinkError> {
        if !Self::wants(record) {
            return Ok(());
        }
        let rm = record.parsed.header.resource_manager_id;
        if rm == RmId::Heap as u8
            && record.parsed.header.info & crate::decode::heap_decoder::XLOG_HEAP_OPMASK
                == crate::decode::heap_decoder::XLOG_HEAP_TRUNCATE
        {
            return Box::pin(self.handle_truncate(record)).await;
        }
        // Gates stash admission and proves generation completeness for the
        // rewrite barrier (records on a filenode cannot precede its creation)
        if rm == RmId::Smgr as u8 {
            if let Some((rfn, fork)) =
                crate::filter::main_data::parse_xl_smgr_create(&record.parsed.main_data)
                && fork == crate::filter::main_data::MAIN_FORKNUM
            {
                self.lock_buffer().await.note_smgr_create(
                    record.parsed.header.xact_id,
                    rfn,
                    record.source_lsn,
                );
            }
            return Ok(());
        }
        let Some(rfn) = record.parsed.blocks.first().map(|b| b.header.location.rel) else {
            self.skip(SkipReason::NoBlock, record, RelFileNode::default());
            return Ok(());
        };
        let txn_xid = record.parsed.header.xact_id;
        // Catalog-dirty tree: hold raw for commit-time resolution.
        // Unlike marker-gated stash admission below, payload is always
        // retained — a Present predecessor descriptor doesn't prove
        // decodability inside the dirty interval
        if record.defer_catalog_decode && txn_xid != 0 {
            let mut raw = crate::xact::spill::RawRecord::from_parsed(
                &record.parsed,
                record.source_lsn,
                record.page_magic,
            );
            raw.drop_redundant_images();
            let (rm, info) = (raw.rm, raw.info);
            let mut buf = self.lock_buffer().await;
            buf.stash_raw(txn_xid, raw).await.map_err(SinkError::from)?;
            self.stats
                .raw_stash_deferred
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            self.stats.raw_stash_dirty_ops.bump(rm, info);
            return Ok(());
        }
        // Known-invisible filenode for this xact (already stashed or
        // marker-registered): its pg_class row stays MVCC-invisible
        // until commit, so the log has no entry yet either
        let maybe_stashed = self
            .stash_live
            .as_ref()
            .is_none_or(|live| live.load(Ordering::Relaxed) != 0);
        if txn_xid != 0
            && maybe_stashed
            && self.lock_buffer().await.is_stash_candidate(txn_xid, rfn)
        {
            return Box::pin(self.stash_invisible(record, rfn)).await;
        }
        let sampled = self
            .span_registry
            .as_ref()
            .is_some_and(|r| r.is_sampled(txn_xid));
        let decode_parent = self
            .span_registry
            .as_ref()
            .and_then(|r| r.decode_parent(txn_xid));
        // Unfollowed database: same skip a foreign-db lookup takes
        let Some(log) = self.logs.lookup(rfn.db_node) else {
            self.skip(SkipReason::UnfollowedDb, record, rfn);
            return Ok(());
        };
        // Wait-free interval lookup: every record reaching this worker
        // already has log coverage (capture runs inside the boundary
        // hold, before successor bytes publish)
        let (rel, rel_valid_from) = match log.descriptor_at_spanned(rfn, record.source_lsn) {
            Ok(pair) => pair,
            // Rel that died before coverage horizon: counted row skip, never
            // a stash or a fatal
            Err(LookupMiss::NotCovered)
                if record.source_lsn <= log.covered_through() || txn_xid == 0 =>
            {
                self.skip(SkipReason::NotCovered, record, rfn);
                return Ok(());
            }
            // Filenode invisible at record LSN: created by this
            // still-open xact (same-xact CREATE / TRUNCATE / rewrite
            // generation) or already superseded — resolve at commit
            Err(LookupMiss::NotCovered | LookupMiss::Dropped) => {
                return Box::pin(self.stash_invisible(record, rfn)).await;
            }
            // Inside a published interval: reachable on a re-read whose
            // start sits past the dirty tree's first touch, so the
            // record never took the defer arm. Marker-proven filenodes
            // keep payload and meet the fence again at commit
            // resolution; a markerless one would be dropped payload-free
            // there, which is the silent row loss the fence exists to
            // stop
            Err(LookupMiss::Ambiguous(a)) => {
                let marker = self.lock_buffer().await.marker_lsn(rfn);
                if marker.is_none() {
                    return Err(XactBufferError::StashAmbiguous {
                        rel_node: rfn.rel_node,
                        lsn: record.source_lsn,
                        from_lsn: a.from_lsn,
                        through_lsn: a.through_lsn,
                    }
                    .into());
                }
                return Box::pin(self.stash_invisible(record, rfn)).await;
            }
            // Rotated away: every record on this rfn precedes the
            // rotation (AccessExclusiveLock), so a Retired answer means
            // the row never outlives the commit — skip
            Err(LookupMiss::Retired) => {
                self.skip(SkipReason::Retired, record, rfn);
                return Ok(());
            }
        };
        let decode_span = || {
            decode_parent.as_ref().map(|p| {
                tracing::info_span!(target: "walshadow::trace", parent: p, "decode").entered()
            })
        };
        // Runtime-config overlay: config-table heap writes never reach CH.
        // Detect by resolved qualified name (rotation-proof — a rewritten
        // relfilenode still resolves to the same schema.name), interpret each
        // tuple into a ConfigEvent stamped (xid, source_lsn) so it drains in
        // WAL order and applies at its commit LSN (plan §2/§6).
        let config_kind = self
            .config_schema
            .as_deref()
            .filter(|schema| &*rel.rel_name.namespace == *schema)
            .and_then(|_| ConfigTableKind::from_relname(&rel.rel_name.name));
        let Some(shape) = record_shape(&record.parsed).map_err(DecoderSinkError::from)? else {
            self.skip(SkipReason::NoTuple, record, rfn);
            return Ok(());
        };
        if rel.kind != b't' && config_kind.is_none() {
            // Ordinary rows stay as WAL bytes until commit, tuple bytes
            // validate where planning reads them
            self.stats.record_op(shape.op, shape.tuples, shape.partial);
            let input = DescribedInput {
                input: HeapInput::from_parsed(&record.parsed, record.source_lsn, shape.tuples),
                descriptor: rel,
                descriptor_valid_from: rel_valid_from,
            };
            let buffer_span = trace_span!(sampled, "buffer", rows = shape.tuples);
            return async { self.lock_buffer().await.on_input(input).await }
                .instrument(buffer_span)
                .await
                .map_err(SinkError::from);
        }
        if let Some(kind) = config_kind {
            let decoded_set = {
                let _decode = decode_span();
                decode_heap_record(&record.parsed, record.source_lsn, &rel)
                    .map_err(DecoderSinkError::from)?
            };
            let mut buf = self.lock_buffer().await;
            for decoded in &decoded_set {
                if let Some(ev) = crate::runtime_config::interpret(kind, decoded, &rel) {
                    buf.on_config_event(decoded.xid, decoded.source_lsn, ev);
                }
            }
            return Ok(());
        }
        let images = {
            let _decode = decode_span();
            view_heap_record(&record.parsed, &rel).map_err(DecoderSinkError::from)?
        };
        let n_decoded = images.len();
        let buffer_span = trace_span!(sampled, "buffer", rows = n_decoded);
        // TID for the toast branch: single-tuple INSERT/DELETE only
        // (toast_save_datum never multi-inserts). blkno rides block ref
        // 0; offnum sits in xl_heap_insert[0..2] / xl_heap_delete[4..6].
        let tid = (n_decoded == 1)
            .then(|| toast_record_tid(&record.parsed))
            .flatten();
        async move {
            // Lock once per record (not per tuple); on_toast_chunk never
            // touches the catalog, so no buffer→catalog inversion.
            let mut buf = self.lock_buffer().await;
            for img in &images {
                let partial = [&img.new, &img.old]
                    .into_iter()
                    .flatten()
                    .any(|t| t.partial);
                self.stats.record_op(img.op, 1, partial);
                if img.op == HeapOp::Delete
                    && let Some((blkno, offnum)) = tid
                {
                    // heap_toast_delete's DELETE: TID-keyed. Buffered
                    // as a store tombstone row applied at commit drain
                    self.stats
                        .toast_chunk_deletes
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    buf.on_toast_delete(
                        crate::xact::spill::ToastDelete {
                            db_oid: rel.rfn.db_node,
                            toast_relid: rel.oid,
                            blkno,
                            offnum,
                            source_lsn: record.source_lsn,
                        },
                        txn_xid,
                    )
                    .await
                    .map_err(SinkError::from)?;
                } else if img.op != HeapOp::Insert {
                    // Non-delete non-insert (TRUNCATE fan-out never
                    // reaches here — kind 't' is filtered there):
                    // nothing to apply against the store
                    self.stats
                        .toast_chunk_deletes
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                } else if let Some(chunk) = img
                    .new
                    .as_ref()
                    .and_then(|t| toast_chunk(t, &rel, record.source_lsn, tid))
                {
                    if tid.is_none() {
                        // No TID → no store row key: the chunk still
                        // serves same-xact resolution, but its birth
                        // never reaches the mirror (a later referrer
                        // superseded-fills). toast_save_datum never
                        // multi-inserts, so this shape is unexpected.
                        tracing::warn!(
                            target: "walshadow::xact_buffer",
                            toast_relid = chunk.toast_relid,
                            value_id = chunk.value_id,
                            chunk_seq = chunk.chunk_seq,
                            "toast chunk without TID; not mirrored",
                        );
                        self.stats
                            .toast_chunks_malformed
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    }
                    self.stats
                        .toast_chunks_buffered
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    buf.on_toast_chunk(chunk, txn_xid)
                        .await
                        .map_err(SinkError::from)?;
                } else {
                    self.stats
                        .toast_chunks_malformed
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
            }
            Ok::<(), SinkError>(())
        }
        .instrument(buffer_span)
        .await?;
        Ok(())
    }
}

impl RecordSink for BufferingDecoderSink {
    fn wants(record: &Record<'_>) -> bool {
        use crate::decode::heap_decoder::{
            XLOG_HEAP_CONFIRM, XLOG_HEAP_LOCK, XLOG_HEAP_OPMASK, XLOG_HEAP_TRUNCATE,
            XLOG_HEAP2_MULTI_INSERT,
        };
        let header = &record.parsed.header;
        let rm = header.resource_manager_id;
        let op = header.info & XLOG_HEAP_OPMASK;
        let routed = matches!(record.route, Route::ToDecoder | Route::ToBoth);
        if rm == RmId::Heap as u8 {
            // TRUNCATE rides Route::ToShadow (shadow replays it) but still
            // fans out per-relid HeapOp::Truncate for CH. LOCK and CONFIRM
            // change no row; other ops reach the stash's fail-closed check
            op == XLOG_HEAP_TRUNCATE || routed && op != XLOG_HEAP_LOCK && op != XLOG_HEAP_CONFIRM
        } else if rm == RmId::Heap2 as u8 {
            // Other Heap2 ops are page maintenance
            routed && op == XLOG_HEAP2_MULTI_INSERT
        } else {
            // Main-fork creation marker, also Route::ToShadow
            rm == RmId::Smgr as u8
                && header.info & 0xF0 == crate::filter::main_data::XLOG_SMGR_CREATE
        }
    }

    fn on_record<'a>(
        &'a mut self,
        record: &'a Record<'a>,
    ) -> Pin<Box<dyn std::future::Future<Output = std::result::Result<(), SinkError>> + Send + 'a>>
    {
        Box::pin(self.handle(record))
    }

    fn on_records<'a>(
        &'a mut self,
        batch: &'a [Record<'a>],
    ) -> Pin<Box<dyn std::future::Future<Output = std::result::Result<(), SinkError>> + Send + 'a>>
    {
        Box::pin(async move {
            for record in batch.iter().filter(|r| Self::wants(r)) {
                self.handle(record).await?;
            }
            Ok(())
        })
    }
}

/// TID of a single-tuple toast-rel INSERT / DELETE: blkno from block ref 0,
/// offnum from the record's `xl_heap_insert` / `xl_heap_delete` main data
/// (PG `access/heapam_xlog.h`). `None` for other shapes.
fn toast_record_tid(record: &walrus::pg::walparser::XLogRecord) -> Option<(u32, u16)> {
    use crate::decode::heap_decoder::{XLOG_HEAP_DELETE, XLOG_HEAP_INSERT, XLOG_HEAP_OPMASK};
    if record.header.resource_manager_id != walrus::pg::walparser::RmId::Heap as u8 {
        return None;
    }
    let md = &record.main_data;
    let off = match record.header.info & XLOG_HEAP_OPMASK {
        // xl_heap_insert: offnum:u16 + flags:u8
        XLOG_HEAP_INSERT => 0,
        // xl_heap_delete: xmax:u32 + offnum:u16 + ...
        XLOG_HEAP_DELETE => 4,
        _ => return None,
    };
    let offnum = u16::from_le_bytes(md.get(off..off + 2)?.try_into().ok()?);
    let blkno = record.blocks.first()?.header.location.block_no;
    Some((blkno, offnum))
}

enum StashedToastOp {
    Chunk(ToastChunk),
    Delete(ToastDelete),
}

/// Decode one stashed record against its resolved toast descriptor,
/// mirroring the live decoder-sink reinterpretation: INSERT → chunk birth,
/// DELETE → TID tombstone, other ops carry nothing for the mirror.
/// Malformed bytes are fatal (`Detoast`), matching the plan's
/// "catalog, replay, and malformed-record errors are fatal, not absence".
fn decode_stashed_toast(
    raw: &RawRecord,
    rel: &Arc<RelDescriptor>,
) -> std::result::Result<Vec<StashedToastOp>, XactBufferError> {
    use crate::decode::heap_decoder::{XLOG_HEAP_INSERT, XLOG_HEAP_OPMASK};
    let rec = raw.to_xlog_record();
    // Rewrite-path inserts are HEAP_INSERT_NO_LOGICAL (no REGBUF_KEEP_DATA,
    // PG src/backend/access/heap/rewriteheap.c): a checkpoint mid-rewrite
    // leaves the chunk tuple only inside the block image
    if raw.rm == RmId::Heap as u8
        && raw.info & XLOG_HEAP_OPMASK == XLOG_HEAP_INSERT
        && rec
            .blocks
            .first()
            .is_some_and(|b| b.header.has_image() && !b.header.has_data())
    {
        return decode_image_insert(raw, rel);
    }
    let images = view_heap_record(&rec, rel)
        .map_err(|e| XactBufferError::Detoast(format!("stashed record decode: {e}")))?;
    let tid = (images.len() == 1)
        .then(|| toast_record_tid(&rec))
        .flatten();
    let mut out = Vec::with_capacity(images.len());
    for img in images {
        match img.op {
            HeapOp::Insert => {
                if let Some(chunk) = img
                    .new
                    .as_ref()
                    .and_then(|t| toast_chunk(t, rel, raw.source_lsn, tid))
                {
                    if tid.is_none() {
                        tracing::warn!(
                            target: "walshadow::xact_buffer",
                            toast_relid = chunk.toast_relid,
                            value_id = chunk.value_id,
                            "stashed toast chunk without TID; not mirrored",
                        );
                    }
                    out.push(StashedToastOp::Chunk(chunk));
                }
            }
            HeapOp::Delete => {
                if let Some((blkno, offnum)) = tid {
                    out.push(StashedToastOp::Delete(ToastDelete {
                        db_oid: rel.rfn.db_node,
                        toast_relid: rel.oid,
                        blkno,
                        offnum,
                        source_lsn: raw.source_lsn,
                    }));
                }
            }
            _ => {}
        }
    }
    Ok(out)
}

/// Image-carried chunk tuple: restore the FPI and read the tuple behind
/// the record's offnum, reusing the bootstrap on-page decoder
fn decode_image_insert(
    raw: &RawRecord,
    rel: &Arc<RelDescriptor>,
) -> std::result::Result<Vec<StashedToastOp>, XactBufferError> {
    let rec = raw.to_xlog_record();
    let Some((blkno, offnum)) = toast_record_tid(&rec) else {
        return Err(XactBufferError::Detoast(
            "stashed image insert lacks offnum".into(),
        ));
    };
    let block = rec.blocks.first().expect("image checked by caller");
    let page = crate::decode::fpi::restore_block_image(block, raw.page_magic)
        .map_err(|e| XactBufferError::Detoast(format!("stashed FPI restore: {e}")))?;
    let Some(tuple) = crate::backfill::backup_page_walk::page_tuple_bytes(&page, offnum) else {
        return Err(XactBufferError::Detoast(format!(
            "stashed image insert: no LP_NORMAL tuple at ({blkno},{offnum})"
        )));
    };
    let Some(view) = crate::backfill::backup_page_walk::on_page_tuple(tuple, rel)
        .and_then(|(_, data)| crate::decode::heap_decoder::view_block_data(data, rel).ok())
    else {
        return Err(XactBufferError::Detoast(format!(
            "stashed image insert: malformed tuple at ({blkno},{offnum})"
        )));
    };
    let Some(chunk) = toast_chunk(&view, rel, raw.source_lsn, Some((blkno, offnum))) else {
        tracing::warn!(
            target: "walshadow::xact_buffer",
            toast_relid = rel.oid,
            blkno,
            offnum,
            "stashed image tuple not a toast chunk shape",
        );
        return Ok(Vec::new());
    };
    Ok(vec![StashedToastOp::Chunk(chunk)])
}

/// [`ToastChunk`] of a TOAST table tuple; `None` for shapes that don't fit
///
/// Keyed on the toast rel's pg_class OID ([`RelDescriptor::oid`]), not
/// `rel_node`: the referring tuple's `va_toastrelid` is the OID. They
/// diverge after `VACUUM FULL` / `CLUSTER` on the toast rel.
fn toast_chunk(
    view: &TupleView<'_>,
    rel: &RelDescriptor,
    source_lsn: u64,
    tid: Option<(u32, u16)>,
) -> Option<ToastChunk> {
    let (value_id, chunk_seq, chunk_data) =
        crate::decode::heap_decoder::toast_chunk_datums(&view.datums)?;
    let (blkno, offnum) = tid.unwrap_or((0, 0));
    Some(ToastChunk {
        db_oid: rel.rfn.db_node,
        toast_relid: rel.oid,
        value_id,
        chunk_seq,
        source_lsn,
        blkno,
        offnum,
        chunk_data: bytes::Bytes::copy_from_slice(chunk_data),
    })
}

/// Raw-record fixtures shared with planner preflight tests: crafted WAL
/// bytes + direct `Ordinary` verdict injection ahead of the enable flip
#[cfg(test)]
pub(crate) mod raw_fixtures {
    use super::*;

    pub(crate) fn int4_descriptor(rel_node: u32) -> Arc<crate::schema::RelDescriptor> {
        Arc::new(crate::schema::RelDescriptor {
            rfn: RelFileNode {
                spc_node: 1663,
                db_node: 5,
                rel_node,
            },
            oid: rel_node,
            toast_oid: 0,
            namespace_oid: 2200,
            rel_name: crate::schema::RelName::new("public", "copy_t"),
            kind: b'r',
            persistence: b'p',
            ancestors: Vec::new(),
            replident: crate::schema::ReplIdent::Full { pk_attnums: None },
            attributes: vec![crate::schema::RelAttr {
                attnum: 1,
                name: "id".into(),
                type_oid: crate::schema::INT4OID,
                typmod: -1,
                not_null: false,
                dropped: false,
                type_name: "int4".into(),
                type_byval: true,
                type_len: 4,
                type_align: b'i',
                type_storage: b'p',
                missing_default: None,
            }],
        })
    }

    /// Heap2 MULTI_INSERT raw record, byte layout per PG
    /// `heap_xlog_multi_insert` (mirrors the heap_decoder unit fixture)
    pub(crate) fn multi_insert_raw(xid: u32, lsn: u64, rel_node: u32, values: &[i32]) -> RawRecord {
        use crate::decode::heap_decoder::{XLH_INSERT_CONTAINS_NEW_TUPLE, XLOG_HEAP2_MULTI_INSERT};
        use crate::xact::spill::RawBlock;
        let mut main_data = vec![XLH_INSERT_CONTAINS_NEW_TUPLE, 0];
        main_data.extend_from_slice(&(values.len() as u16).to_le_bytes());
        for off in 1..=values.len() as u16 {
            main_data.extend_from_slice(&off.to_le_bytes());
        }
        let mut data = Vec::new();
        for v in values {
            data.extend_from_slice(&5u16.to_le_bytes()); // datalen: pad + int4
            data.extend_from_slice(&1u16.to_le_bytes()); // t_infomask2 = natts
            data.extend_from_slice(&0u16.to_le_bytes()); // t_infomask
            data.push(24); // t_hoff
            data.push(0); // bitmap pad
            data.extend_from_slice(&v.to_le_bytes());
        }
        RawRecord {
            xid,
            rm: RmId::Heap2 as u8,
            info: XLOG_HEAP2_MULTI_INSERT,
            source_lsn: lsn,
            page_magic: 0xD114,
            main_data,
            blocks: vec![RawBlock {
                block_id: 0,
                fork_flags: 0x20,
                data_length: data.len() as u16,
                image_length: 0,
                hole_offset: 0,
                hole_length: 0,
                bimg_info: 0,
                spc_node: 1663,
                db_node: 5,
                rel_node,
                block_no: 0,
                image: Vec::new(),
                data,
            }],
        }
    }

    pub(crate) fn inject_ordinary_with_stats(
        rfn: RelFileNode,
        rel: Arc<crate::schema::RelDescriptor>,
        stats: Option<Arc<EmitterStats>>,
    ) -> StashResolved {
        inject_ordinary_fenced(rfn, rel, stats, Vec::new())
    }

    /// `Ordinary` verdict carrying a fence, as `resolve_stash` would attach
    /// it for a filenode with overlapping ambiguity intervals
    pub(crate) fn inject_ordinary_fenced(
        rfn: RelFileNode,
        rel: Arc<crate::schema::RelDescriptor>,
        stats: Option<Arc<EmitterStats>>,
        fence: Vec<Arc<crate::catalog::desc_log::Ambiguity>>,
    ) -> StashResolved {
        let mut outcomes = HashMap::new();
        let history = RfnHistory::fixture([(0x50, rel)], fence);
        outcomes.insert(rfn, StashOutcome::Ordinary(history));
        StashResolved {
            top_xid: 1,
            res: Some(StashResolution { outcomes, stats }),
        }
    }

    /// Build a result for a filenode with several recorded descriptors.
    pub(crate) fn inject_ordinary_shapes(
        rfn: RelFileNode,
        shapes: Vec<(u64, Arc<crate::schema::RelDescriptor>)>,
    ) -> StashResolved {
        let mut outcomes = HashMap::new();
        let history = RfnHistory::fixture(shapes, Vec::new());
        outcomes.insert(rfn, StashOutcome::Ordinary(history));
        StashResolved {
            top_xid: 1,
            res: Some(StashResolution {
                outcomes,
                stats: None,
            }),
        }
    }

    /// `[from, through)` interval over one filenode, the shape a physical
    /// in-place verdict publishes
    pub(crate) fn rfn_ambiguity(
        rfn: RelFileNode,
        from_lsn: u64,
        through_lsn: u64,
    ) -> Arc<crate::catalog::desc_log::Ambiguity> {
        use crate::catalog::desc_log::{Ambiguity, AmbiguityReason, AmbiguityScope};
        Arc::new(Ambiguity {
            scope: AmbiguityScope::Rfn(rfn),
            from_lsn,
            through_lsn,
            reason: AmbiguityReason::UnknownMutationPosition,
        })
    }
}

#[cfg(test)]
mod tests {
    //! Catalog-free paths only. Commit-drain + detoast +
    //! `XactRecordSink::commit` live in `tests/xact_buffer.rs` against a
    //! real shadow PG: they need `ShadowCatalog::relation_at`, and a
    //! unit-test stub catalog would duplicate the production cache.

    use super::raw_fixtures::*;
    use super::*;
    use crate::decode::datum::{Datum, build};
    use crate::decode::heap_decoder::{DecodedTuple, HeapOp, VARLENA_EXTSIZE_BITS};
    use crate::emit::ch_emitter::InlineValueOverflow;
    use tempfile::tempdir;
    use walrus::pg::walparser::RelFileNode;

    #[test]
    fn xact_buffer_config_new_uses_default_max() {
        let c = XactBufferConfig::new(PathBuf::from("/tmp/walshadow-test-spill"));
        assert_eq!(c.xact_buffer_max, DEFAULT_XACT_BUFFER_MAX);
    }

    #[test]
    fn txn_span_registry_full_lifecycle() {
        let _g = crate::ops::trace::SAMPLE_TEST_GUARD.blocking_lock();
        crate::ops::trace::set_sample_ratio(1.0);
        let reg = TxnSpanRegistry::new();

        reg.open(0, 1);
        reg.note_shipped(0);
        assert!(!reg.note_popped(0));
        assert!(!reg.note_popped(7), "unopened xid");
        assert!(!reg.is_sampled(0));

        reg.open(42, 100);
        assert!(reg.is_sampled(42));
        assert!(reg.note_popped(42));
        assert!(reg.txn_span(42).is_none());

        reg.note_shipped(42);
        assert!(reg.note_popped(42));
        assert!(reg.txn_span(42).is_some());
        assert!(reg.decode_parent(42).is_some());

        assert!(reg.adopt(42).is_some());
        assert!(reg.decode_parent(42).is_none());
        assert!(reg.adopt(999).is_none());

        reg.prune(&[42]);
        assert!(reg.txn_span(42).is_none());
        assert!(!reg.is_sampled(42));
    }

    fn cfg(dir: PathBuf) -> XactBufferConfig {
        XactBufferConfig {
            xact_buffer_max: 1024,
            ..XactBufferConfig::new(dir)
        }
    }

    fn heap_with_value(xid: u32, lsn: u64, payload_size: usize) -> DescribedHeap {
        DescribedHeap {
            decoded: DecodedHeap {
                rfn: RelFileNode {
                    spc_node: 1663,
                    db_node: 5,
                    rel_node: 16385,
                },
                xid,
                source_lsn: lsn,
                op: HeapOp::Insert,
                new: Some(DecodedTuple {
                    columns: vec![Some(build::bytes(vec![0u8; payload_size]))],
                    partial: false,
                }),
                old: None,
            },
            descriptor: fixture_descriptor(16385),
            descriptor_valid_from: 0x40,
        }
    }

    fn fixture_descriptor(rel_node: u32) -> Arc<crate::schema::RelDescriptor> {
        Arc::new(crate::schema::RelDescriptor {
            rfn: RelFileNode {
                spc_node: 1663,
                db_node: 5,
                rel_node,
            },
            oid: rel_node,
            toast_oid: 0,
            namespace_oid: 2200,
            rel_name: crate::schema::RelName::new("public", "buf_t"),
            kind: b'r',
            persistence: b'p',
            ancestors: Vec::new(),
            replident: crate::schema::ReplIdent::Full { pk_attnums: None },
            attributes: vec![],
        })
    }

    /// Heap UPDATE raw record, full new tuple (no prefix/suffix elision)
    fn update_raw(xid: u32, lsn: u64, rel_node: u32, value: i32) -> RawRecord {
        use crate::decode::heap_decoder::{SIZE_OF_HEAP_UPDATE, XLOG_HEAP_UPDATE};
        use crate::xact::spill::RawBlock;
        let mut data = Vec::new();
        data.extend_from_slice(&1u16.to_le_bytes()); // natts
        data.extend_from_slice(&0u16.to_le_bytes()); // infomask
        data.push(24); // t_hoff
        data.push(0); // bitmap pad
        data.extend_from_slice(&value.to_le_bytes());
        RawRecord {
            xid,
            rm: RmId::Heap as u8,
            info: XLOG_HEAP_UPDATE,
            source_lsn: lsn,
            page_magic: 0xD114,
            main_data: vec![0u8; SIZE_OF_HEAP_UPDATE],
            blocks: vec![RawBlock {
                block_id: 0,
                fork_flags: 0x20,
                data_length: data.len() as u16,
                image_length: 0,
                hole_offset: 0,
                hole_length: 0,
                bimg_info: 0,
                spc_node: 1663,
                db_node: 5,
                rel_node,
                block_no: 0,
                image: Vec::new(),
                data,
            }],
        }
    }

    #[test]
    fn fail_closed_reasons_and_stats_render() {
        let rendered = [
            FailClosedReason::Malformed("short".into()),
            FailClosedReason::UnsupportedOperation,
            FailClosedReason::PartialUpdate,
        ]
        .map(|r| r.to_string());
        assert_eq!(
            rendered,
            [
                "malformed payload: short",
                "unsupported operation with user-row effects",
                "partial update without reconstructable predecessor",
            ]
        );
        let err = XactBufferError::from(ToastValueError::Detoast("bad lz4".into()));
        assert!(
            matches!(&err, XactBufferError::Detoast(d) if d == "bad lz4"),
            "{err}"
        );
        let stats = XactBufferStats {
            aborts_unknown_xid: 3,
            ..Default::default()
        };
        assert!(
            stats.summary().ends_with(" abort_unk=3"),
            "{}",
            stats.summary()
        );
    }

    #[test]
    fn inflight_snapshot_empty_when_nothing_buffered() {
        let tmp = tempdir().unwrap();
        let b = XactBuffer::new(cfg(tmp.path().to_path_buf())).unwrap();
        assert!(b.inflight_snapshot().is_empty());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn inflight_snapshot_reports_single_parked_xid() {
        let tmp = tempdir().unwrap();
        let mut b = XactBuffer::new(cfg(tmp.path().to_path_buf())).unwrap();
        b.on_heap(heap_with_value(7, 100, 16)).await.unwrap();
        let mut trunc = heap_with_value(7, 105, 16);
        trunc.decoded.op = HeapOp::Truncate;
        trunc.decoded.new = None;
        b.on_heap(trunc).await.unwrap();
        let snap = b.inflight_snapshot();
        assert_eq!(snap.len(), 1);
        let e = &snap[0];
        assert_eq!(e.xid, 7);
        assert_eq!(e.first_lsn, 100);
        assert_eq!(e.last_lsn, 105);
        assert_eq!(e.heap_count, 2);
        assert_eq!(e.chunk_count, 0);
        assert!(e.in_mem_bytes > 0);
        assert!(
            !e.spilled,
            "16-byte tuple stays in memory under the 1 KiB cap"
        );
        assert_eq!(e.rels, "5/16385");
    }

    /// Chunks and tombstones count as toast ops under rel 0; events extend
    /// the LSN span; a blockless raw names no filenode, so stashes nothing
    #[tokio::test(flavor = "current_thread")]
    async fn inflight_snapshot_counts_toast_ops_and_events() {
        let tmp = tempdir().unwrap();
        let mut b = XactBuffer::new(cfg(tmp.path().to_path_buf())).unwrap();
        b.on_toast_chunk(chunk(55, 0, 100, b"ab"), 7).await.unwrap();
        let delete = crate::xact::spill::ToastDelete {
            db_oid: 0,
            toast_relid: 16401,
            blkno: 0,
            offnum: 1,
            source_lsn: 110,
        };
        b.on_toast_delete(delete, 7).await.unwrap();
        b.on_schema_event(7, 130, dropped_event(9));
        let mut blockless = multi_insert_raw(7, 140, 16420, &[1]);
        blockless.blocks.clear();
        b.stash_raw(7, blockless).await.unwrap();
        let snap = b.inflight_snapshot();
        let [e] = &snap[..] else { panic!() };
        assert_eq!((e.heap_count, e.chunk_count, e.catalog_events), (0, 2, 1));
        assert_eq!((e.first_lsn, e.last_lsn), (100, 130));
        assert_eq!(e.rels, "0/16400,0/16401");
    }

    /// Sampled xact opens its own span off the registry; abort prunes it
    #[tokio::test(flavor = "current_thread")]
    async fn abort_prunes_sampled_xact_span() {
        let _g = crate::ops::trace::SAMPLE_TEST_GUARD.lock().await;
        crate::ops::trace::set_sample_ratio(1.0);
        let reg = TxnSpanRegistry::new();
        reg.open(7, 100);
        let tmp = tempdir().unwrap();
        let mut b = XactBuffer::new(cfg(tmp.path().to_path_buf()))
            .unwrap()
            .with_span_registry(reg.clone());
        b.on_heap(heap_with_value(7, 100, 16)).await.unwrap();
        assert!(reg.is_sampled(7));
        b.abort(7, Pos::new(200), &[]).await.unwrap();
        assert!(!reg.is_sampled(7));
    }

    #[test]
    fn repeated_smgr_create_queues_once_and_candidates_skip_unknown_xids() {
        let tmp = tempdir().unwrap();
        let mut b = XactBuffer::new(cfg(tmp.path().to_path_buf())).unwrap();
        let a = rfn_of(90_000);
        b.note_smgr_create(7, a, 10);
        b.note_smgr_create(7, a, 10);
        assert_eq!(b.marker_order.len(), 1);
        let marks = b.stash_candidates(&[7, 99]);
        assert_eq!(marks.len(), 1);
        assert_eq!((marks[0].0, marks[0].1.first_lsn), (a, 10));
    }

    #[test]
    fn marker_cap_eviction_skips_stale_entry_of_reused_filenode() {
        let tmp = tempdir().unwrap();
        let mut b = XactBuffer::new(cfg(tmp.path().to_path_buf())).unwrap();
        let rfn = |rel_node| RelFileNode {
            spc_node: 1663,
            db_node: 5,
            rel_node,
        };
        let a = rfn(90_000);
        // Generation 1 consumed at resolution; queue entry stays behind
        b.note_smgr_create(0, a, 10);
        b.forget_markers(&[a]);
        // Generation 2 reuses the filenode
        b.note_smgr_create(0, a, 20);
        // Churn pops the stale (a, 10) queue entry; live marker survives
        for i in 0..(MARKER_CAP - 1) as u32 {
            b.note_smgr_create(0, rfn(100_000 + i), 100 + u64::from(i));
        }
        assert_eq!(b.marker_lsn(a), Some(20));
        // Next churn pops (a, 20) itself; cap evicts live marker
        b.note_smgr_create(0, rfn(200_000), 999_999);
        assert_eq!(b.marker_lsn(a), None);
    }

    #[test]
    fn xact_buffer_error_converts_to_sink_and_decoder_errors() {
        let s: SinkError = XactBufferError::Observer("boom".into()).into();
        assert_eq!(format!("{s:?}"), r#"Other("observer: boom")"#);
        let d: DecoderSinkError = XactBufferError::Observer("boom".into()).into();
        assert_eq!(format!("{d:?}"), r#"Observer("observer: boom")"#);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn repeated_eviction_preserves_spilled_prefix_and_memory_tail() {
        let tmp = tempdir().unwrap();
        let mut config = cfg(tmp.path().to_path_buf());
        let row_bytes = approximate_size(&SpillEntry::Input(Box::new(
            heap_with_value(7, 1, 128).try_into().unwrap(),
        )));
        config.xact_buffer_max = 4 * row_bytes;
        let mut buffer = XactBuffer::new(config).unwrap();
        for lsn in 1..=12 {
            for xid in [7, 8] {
                buffer
                    .on_heap(heap_with_value(xid, 2 * lsn + u64::from(xid == 8), 128))
                    .await
                    .unwrap();
                assert!(buffer.bytes_in_memory <= 4 * row_bytes);
            }
        }
        assert!(buffer.stats().spill_evictions_total > 2);
        assert!(
            buffer
                .inflight
                .values()
                .any(|s| s.spill.is_some() && !s.in_mem.is_empty())
        );
        let mut drain = buffer
            .drain_committed(StashResolved::nothing_stashed(7), 0, 100, &[8], false)
            .await
            .unwrap();
        assert_eq!(buffer.stats().bytes_in_memory, 0);
        let mut lsns = Vec::new();
        while let Some(batch) = drain.next_batch(3, usize::MAX, None).await.unwrap() {
            lsns.extend(
                batch
                    .cells()
                    .unwrap()
                    .into_iter()
                    .map(|heap| heap.decoded.source_lsn),
            );
        }
        assert_eq!(lsns, (2..=25).collect::<Vec<_>>());
        drain.finish().await.unwrap();
        assert_eq!(buffer.drain_resident_bytes(), 0);
        assert!(spill_files(tmp.path()).is_empty());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn abort_drops_xact_and_unlinks_spill() {
        let tmp = tempdir().unwrap();
        let mut b = XactBuffer::new(cfg(tmp.path().to_path_buf())).unwrap();
        for i in 0..10 {
            b.on_heap(heap_with_value(11, 100 + i, 256)).await.unwrap();
        }
        assert!(b.stats().spill_xacts_active >= 1, "spill must engage");
        let before: Vec<_> = std::fs::read_dir(tmp.path()).unwrap().collect();
        assert!(!before.is_empty(), "spill file present");
        b.abort(11, Pos::new(200), &[]).await.unwrap();
        let after = std::fs::read_dir(tmp.path()).unwrap().count();
        assert_eq!(after, 0, "abort must remove spill file");
        assert_eq!(b.stats().aborted_xacts_total, 1);
        assert_eq!(b.stats().spill_xacts_active, 0);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn advance_idle_moves_drain_lsn_monotonically() {
        let tmp = tempdir().unwrap();
        let mut b = XactBuffer::new(cfg(tmp.path().to_path_buf())).unwrap();
        b.advance_idle(Pos::new(100));
        assert_eq!(b.stats().drain_lsn, 100);
        // Regressing input never lowers the field
        b.advance_idle(Pos::new(50));
        assert_eq!(b.stats().drain_lsn, 100);
        // Inflight xact parks the advance
        b.on_heap(heap_with_value(7, 150, 16)).await.unwrap();
        b.advance_idle(Pos::new(300));
        assert_eq!(b.stats().drain_lsn, 100);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn resume_safe_lsn_floors_at_undurable_xacts() {
        let tmp = tempdir().unwrap();
        let mut b = XactBuffer::new(cfg(tmp.path().to_path_buf())).unwrap();
        // Use acknowledgment when no transactions remain
        assert_eq!(b.resume_safe_lsn(Pos::new(500)), 500);
        // Open transactions lower resume point
        b.on_heap(heap_with_value(7, 100, 16)).await.unwrap();
        b.on_heap(heap_with_value(8, 150, 16)).await.unwrap();
        assert_eq!(b.resume_safe_lsn(Pos::new(500)), 100);
        // Keep floor while committed slices remain undurable
        let drain = b
            .drain_committed(StashResolved::nothing_stashed(8), 0, 200, &[], false)
            .await
            .unwrap();
        assert!(drain.had_states());
        drop(drain);
        assert_eq!(
            b.resume_safe_lsn(Pos::new(180)),
            100,
            "open xid 7 still floors"
        );
        b.abort(7, Pos::new(210), &[]).await.unwrap();
        assert_eq!(
            b.resume_safe_lsn(Pos::new(180)),
            150,
            "xid 8 undurable at ack 180"
        );
        // Drop floor once acknowledgment reaches commit
        assert_eq!(b.resume_safe_lsn(Pos::new(200)), 200);
        // Ignore commits without buffered rows
        let empty = b
            .drain_committed(StashResolved::nothing_stashed(9), 0, 300, &[], false)
            .await
            .unwrap();
        assert!(!empty.had_states());
        assert_eq!(b.resume_safe_lsn(Pos::new(300)), 300);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn resume_safe_lsn_uses_tree_min_over_subxacts() {
        let tmp = tempdir().unwrap();
        let mut b = XactBuffer::new(cfg(tmp.path().to_path_buf())).unwrap();
        // Use earliest record across transaction tree
        b.on_heap(heap_with_value(20, 400, 16)).await.unwrap();
        b.on_heap(heap_with_value(21, 420, 16)).await.unwrap();
        let drain = b
            .drain_committed(StashResolved::nothing_stashed(21), 0, 450, &[20], false)
            .await
            .unwrap();
        drop(drain);
        assert_eq!(b.resume_safe_lsn(Pos::new(440)), 400);
        assert_eq!(b.resume_safe_lsn(Pos::new(450)), 450);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn abort_unknown_xid_counts() {
        let tmp = tempdir().unwrap();
        let mut b = XactBuffer::new(cfg(tmp.path().to_path_buf())).unwrap();
        b.abort(101, Pos::ZERO, &[]).await.unwrap();
        assert_eq!(b.stats().aborts_unknown_xid, 1);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn spill_eviction_picks_largest_xact() {
        let tmp = tempdir().unwrap();
        let cfg = XactBufferConfig {
            xact_buffer_max: 4096,
            ..XactBufferConfig::new(tmp.path().to_path_buf())
        };
        let mut b = XactBuffer::new(cfg).unwrap();
        b.on_heap(heap_with_value(1, 100, 8192)).await.unwrap();
        for i in 0..3 {
            b.on_heap(heap_with_value(2, 200 + i, 128)).await.unwrap();
        }
        let by_filename: Vec<String> = std::fs::read_dir(tmp.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .filter(|n| n.starts_with("xid-"))
            .collect();
        assert!(
            by_filename.iter().any(|n| n.contains("xid-0000000001-")),
            "xid=1 spill file expected, saw {by_filename:?}"
        );
        assert!(
            !by_filename.iter().any(|n| n.contains("xid-0000000002-")),
            "xid=2 must remain in-memory, saw {by_filename:?}"
        );
        b.abort(1, Pos::new(300), &[]).await.unwrap();
        b.abort(2, Pos::new(300), &[]).await.unwrap();
    }

    /// Aborts must advance `drain_lsn`, else an all-abort workload never
    /// advances the slot (the ack side rides the reorder's abort seq)
    #[tokio::test(flavor = "current_thread")]
    async fn abort_advances_drain_lsn() {
        let tmp = tempdir().unwrap();
        let mut b = XactBuffer::new(cfg(tmp.path().to_path_buf())).unwrap();
        b.on_heap(heap_with_value(7, 100, 16)).await.unwrap();
        b.abort(7, Pos::new(0x4000), &[]).await.unwrap();
        assert_eq!(b.stats().drain_lsn, 0x4000);
        // Lower-LSN abort must not regress the monotonic mark
        b.abort(99, Pos::new(0x100), &[]).await.unwrap();
        assert_eq!(b.stats().drain_lsn, 0x4000);
        b.abort(101, Pos::new(0x8000), &[]).await.unwrap();
        assert_eq!(b.stats().drain_lsn, 0x8000);
    }

    #[test]
    fn stats_summary_includes_evictions_only_when_nonzero() {
        let mut s = XactBufferStats {
            xacts_active: 2,
            bytes_in_memory: 1024,
            committed_xacts_total: 5,
            aborted_xacts_total: 1,
            ..Default::default()
        };
        let q = s.summary();
        assert!(q.contains("xact_active=2"));
        assert!(q.contains("commit=5"));
        assert!(q.contains("abort=1"));
        assert!(!q.contains("evictions="));
        s.spill_evictions_total = 3;
        assert!(s.summary().contains("evictions=3"));
    }

    #[test]
    fn toast_chunk_recognises_three_col_shape() {
        use crate::schema::{RelAttr, RelName, ReplIdent};
        let rel = RelDescriptor {
            rfn: RelFileNode {
                spc_node: 1663,
                db_node: 5,
                rel_node: 16400,
            },
            oid: 99,
            toast_oid: 0,
            namespace_oid: 99,
            rel_name: RelName::new("pg_toast", "pg_toast_16385"),
            kind: b't',
            persistence: b'p',
            ancestors: Vec::new(),
            replident: ReplIdent::Default { pk_attnums: None },
            attributes: vec![
                RelAttr {
                    attnum: 1,
                    name: "chunk_id".into(),
                    type_oid: crate::schema::OIDOID,
                    typmod: -1,
                    not_null: true,
                    dropped: false,
                    type_name: "oid".into(),
                    type_byval: true,
                    type_len: 4,
                    type_align: b'i',
                    type_storage: b'p',
                    missing_default: None,
                },
                RelAttr {
                    attnum: 2,
                    name: "chunk_seq".into(),
                    type_oid: crate::schema::INT4OID,
                    typmod: -1,
                    not_null: true,
                    dropped: false,
                    type_name: "int4".into(),
                    type_byval: true,
                    type_len: 4,
                    type_align: b'i',
                    type_storage: b'p',
                    missing_default: None,
                },
                RelAttr {
                    attnum: 3,
                    name: "chunk_data".into(),
                    type_oid: crate::schema::BYTEAOID,
                    typmod: -1,
                    not_null: true,
                    dropped: false,
                    type_name: "bytea".into(),
                    type_byval: false,
                    type_len: -1,
                    type_align: b'i',
                    type_storage: b'x',
                    missing_default: None,
                },
            ],
        };
        use crate::decode::heap_decoder::{form_block_data, view_block_data};
        let full = form_block_data(
            &rel.attributes,
            &[
                Some(build::oid(55)),
                Some(build::int4(2)),
                Some(build::bytes(b"hello")),
            ],
        );
        let view = view_block_data(&full, &rel).unwrap();
        let chunk = toast_chunk(&view, &rel, 0x1234, Some((7, 3))).expect("recognised toast shape");
        assert_eq!(chunk.toast_relid, 99); // pg_class.oid, not rel_node
        assert_eq!(chunk.value_id, 55);
        assert_eq!(chunk.chunk_seq, 2);
        assert_eq!(&chunk.chunk_data[..], b"hello");
        assert_eq!((chunk.blkno, chunk.offnum), (7, 3));
        let short = form_block_data(
            &rel.attributes,
            &[Some(build::oid(55)), Some(build::int4(2))],
        );
        let view = view_block_data(&short, &rel).unwrap();
        assert!(toast_chunk(&view, &rel, 0, None).is_none());
    }

    /// Detoasted bodies stay stored bytes, whatever their type, and hold
    /// exactly their length
    #[test]
    fn detoasted_values_stay_stored() {
        assert_eq!(detoasted_sized(b"raw".to_vec()), (build::bytes(b"raw"), 3));
        let jsonb = 0x2000_0000u32.to_le_bytes().to_vec();
        assert_eq!(
            crate::toast::detoasted_within(jsonb.clone(), |held| {
                assert_eq!(held, 4);
                false
            }),
            None
        );
        assert_eq!(
            crate::toast::detoasted_within(jsonb.clone(), |_| true),
            Some((build::bytes(&jsonb), 4))
        );
    }

    fn bytea_rel() -> RelDescriptor {
        use crate::schema::{RelName, ReplIdent};
        RelDescriptor {
            rfn: RelFileNode {
                spc_node: 1663,
                db_node: 5,
                rel_node: 16400,
            },
            oid: 16400,
            toast_oid: 0,
            namespace_oid: 2200,
            rel_name: RelName::new("public", "t"),
            kind: b'r',
            persistence: b'p',
            ancestors: Vec::new(),
            replident: ReplIdent::Default { pk_attnums: None },
            attributes: vec![crate::schema::RelAttr {
                attnum: 1,
                name: "b".into(),
                type_oid: crate::schema::BYTEAOID,
                typmod: -1,
                not_null: false,
                dropped: false,
                type_name: "bytea".into(),
                type_byval: false,
                type_len: -1,
                type_align: b'i',
                type_storage: b'x',
                missing_default: None,
            }],
        }
    }

    fn toast_ptr(value_id: u32) -> ToastPointer {
        ToastPointer {
            va_rawsize: 8,
            va_extinfo: 4, // 4 bytes, uncompressed
            va_valueid: value_id,
            va_toastrelid: 16500,
        }
    }

    fn toast_ptr_tuple(value_id: u32) -> DecodedTuple {
        DecodedTuple {
            columns: vec![Some(Datum::External(toast_ptr(value_id)))],
            partial: false,
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn read_ahead_transfers_into_foreground_admission() {
        use crate::toast::{ChunkStore, MemChunkStore, ToastRow};
        let store = Arc::new(MemChunkStore::new());
        store
            .put(&[ToastRow {
                db_oid: 5,
                toast_relid: 16500,
                blkno: 1,
                offnum: 1,
                chunk_id: 55,
                chunk_seq: 0,
                chunk_data: bytes::Bytes::from_static(b"abcd"),
                lsn: 1,
            }])
            .await
            .unwrap();
        let stats = Arc::new(crate::emit::ch_emitter::EmitterStats::default());
        let budget = crate::budget::MemoryBudget::with_leaf_reserve(2048, 1024);
        let resolver = ToastResolver::with_store(store, stats.clone())
            .with_budget(budget.clone())
            .with_read_ahead();
        let p = toast_ptr(55);
        resolver.read_ahead(5, p, 0x100);
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while stats.toast_values_fetched.load(Ordering::Relaxed) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let held = budget.acquire(1024).await;
        let mut heap = heap_with_value(1, 0x100, 0);
        heap.descriptor = Arc::new(bytea_rel());
        heap.decoded.new = Some(toast_ptr_tuple(55));
        let resolve = tokio::spawn(async move {
            let permit = detoast_heap(&mut heap, None, &[], &resolver).await.unwrap();
            (heap, permit)
        });
        let (heap, permit) = resolve.await.unwrap();
        assert_eq!(
            heap.decoded.new.as_ref().unwrap().columns[0],
            Some(build::bytes(b"abcd"))
        );
        assert!(budget.resident_bytes() >= 1024 + 4);
        assert_eq!(budget.waits_total(), 0);
        assert_eq!(stats.toast_values_fetched.load(Ordering::Relaxed), 1);
        drop(held);
        drop(heap);
        drop(permit);
        assert_eq!(budget.resident_bytes(), 0);
    }

    /// Foreground fetch of a value read-ahead missed keeps the read-ahead
    /// permit in the one it returns
    #[tokio::test(flavor = "current_thread")]
    async fn read_ahead_permit_joins_foreground_fetch() {
        use crate::toast::{ChunkStore, MemChunkStore, ToastRow};
        let store = Arc::new(MemChunkStore::new());
        let row = |chunk_id, data| ToastRow {
            db_oid: 5,
            toast_relid: 16500,
            blkno: 1,
            offnum: chunk_id as u16,
            chunk_id,
            chunk_seq: 0,
            chunk_data: bytes::Bytes::from_static(data),
            lsn: 1,
        };
        store
            .put(&[row(55, b"abcd"), row(56, b"efgh")])
            .await
            .unwrap();
        let stats = Arc::new(crate::emit::ch_emitter::EmitterStats::default());
        let budget = crate::budget::MemoryBudget::with_leaf_reserve(2048, 1024);
        let resolver = ToastResolver::with_store(store, stats.clone())
            .with_budget(budget.clone())
            .with_read_ahead();
        resolver.read_ahead(5, toast_ptr(55), 0x100);
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while stats.toast_values_fetched.load(Ordering::Relaxed) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let mut heap = heap_with_value(1, 0x100, 0);
        heap.descriptor = Arc::new(bytea_rel());
        heap.decoded.new = Some(toast_ptr_tuple(55));
        heap.decoded.old = Some(toast_ptr_tuple(56));
        let permit = detoast_heap(&mut heap, None, &[], &resolver).await.unwrap();
        let value = |t: &Option<DecodedTuple>| t.as_ref().unwrap().columns[0].clone();
        assert_eq!(
            [value(&heap.decoded.new), value(&heap.decoded.old)],
            [Some(build::bytes(b"abcd")), Some(build::bytes(b"efgh"))]
        );
        assert_eq!(stats.toast_values_fetched.load(Ordering::Relaxed), 2);
        assert!(budget.resident_bytes() >= 8);
        drop(heap);
        drop(permit);
        assert_eq!(budget.resident_bytes(), 0);
    }

    /// Read-ahead declines without a store, a budget, or an inline-sized
    /// pointer; a stored miss yields no value, leaving the pointer for
    /// foreground resolution
    #[tokio::test(flavor = "current_thread")]
    async fn read_ahead_declines_and_miss_leaves_pointer() {
        use std::sync::atomic::Ordering::Relaxed;
        let p = toast_ptr(55);
        let stats = Arc::new(crate::emit::ch_emitter::EmitterStats::default());
        let budget = crate::budget::MemoryBudget::with_leaf_reserve(1 << 20, 64 << 10);
        let store = Arc::new(crate::toast::MemChunkStore::new());
        let other = crate::toast::ToastRow {
            db_oid: 5,
            toast_relid: 16500,
            blkno: 1,
            offnum: 1,
            chunk_id: 99,
            chunk_seq: 0,
            chunk_data: bytes::Bytes::from_static(b"abcd"),
            lsn: 1,
        };
        crate::toast::ChunkStore::put(store.as_ref(), &[other])
            .await
            .unwrap();
        let stored = || ToastResolver::with_store(store.clone(), stats.clone());
        let declining = [
            ToastResolver::disabled()
                .with_budget(budget.clone())
                .with_read_ahead(),
            stored().with_read_ahead(),
            stored()
                .with_budget(budget.clone())
                .with_read_ahead()
                .with_inline_value_max(1),
        ];
        for r in &declining {
            r.read_ahead(5, p, 0x100);
        }
        assert_eq!(stats.toast_values_read_ahead.load(Relaxed), 0);
        let r = stored().with_budget(budget).with_read_ahead();
        r.read_ahead(5, p, 0x100);
        while stats.toast_values_read_ahead.load(Relaxed) == 0 {
            tokio::task::yield_now().await;
        }
        let mut permit = None;
        let miss = r.take_ahead(5, &p, 0x100, &mut permit);
        assert_eq!(miss.await, None);
    }

    /// Ref map of memory bodies for one value's `(seq, body)` chunks
    fn mem_refs(key: (u32, u32), chunks: &[(u32, &'static [u8])]) -> ChunkRefMap {
        let mut map = ChunkRefMap::new();
        for &(seq, body) in chunks {
            let body = Body::Mem(bytes::Bytes::from_static(body));
            match map.entry(key) {
                Entry::Occupied(mut o) => {
                    o.get_mut().push(seq, body);
                }
                Entry::Vacant(v) => {
                    v.insert(ValueRef::new(seq, body));
                }
            }
        }
        map
    }

    /// V3 cap fires before allocation with a typed non-retryable error;
    /// leaf need sizes for the worst per-value transient
    #[test]
    fn value_cap_rejects_before_allocation() {
        let ptr = |rawsize, extinfo| ToastPointer {
            va_rawsize: rawsize,
            va_extinfo: extinfo,
            va_valueid: 1,
            va_toastrelid: 16500,
        };
        let strict = InlineValueOverflow::Error;
        // Uncompressed: leaf need = extsize
        assert_eq!(
            check_value_caps([ptr(104, 100)], 1000, strict).unwrap(),
            100
        );
        // Compressed (method bits set): extsize + rawsize
        let compressed = 80u32 | (1 << VARLENA_EXTSIZE_BITS);
        assert_eq!(
            check_value_caps([ptr(104, compressed)], 1000, strict).unwrap(),
            180
        );
        // Decode target over cap: typed error before allocation
        let err = check_value_caps([ptr(2000, 100)], 1000, strict).unwrap_err();
        assert!(matches!(
            err,
            ToastValueError::ValueTooLarge {
                rawsize: 1996,
                max: 1000
            }
        ));
        // Stored form over cap trips too (caps ChunkAssembler expected_size)
        let err = check_value_caps([ptr(104, 1500)], 1000, strict).unwrap_err();
        assert!(matches!(err, ToastValueError::ValueTooLarge { .. }));
        // Null policy ignores oversized values
        assert_eq!(
            check_value_caps(
                [ptr(2000, 100), ptr(104, 1500), ptr(104, 100)],
                1000,
                InlineValueOverflow::Null
            )
            .unwrap(),
            100
        );
    }

    /// Replace oversized values without reading transaction or stored chunks
    #[tokio::test(flavor = "current_thread")]
    async fn oversize_pointer_fills_null_under_null_overflow() {
        use std::sync::atomic::Ordering::Relaxed;
        let stats = Arc::new(crate::emit::ch_emitter::EmitterStats::default());
        let key = (16500u32, 55u32);
        let whole = mem_refs(key, &[(0, b"ab"), (1, b"cd")]);
        let maps = [&whole];
        let capped = |overflow| {
            ToastResolver::with_store(Arc::new(crate::toast::MemChunkStore::new()), stats.clone())
                .with_inline_value_max(3)
                .with_overflow(overflow)
        };

        let mut heap = heap_with_value(1, 0x100, 8);
        heap.decoded.new = Some(toast_ptr_tuple(55));
        heap.decoded.old = Some(toast_ptr_tuple(55));
        let resolver = capped(InlineValueOverflow::Null);
        detoast_heap(&mut heap, None, &maps, &resolver)
            .await
            .expect("fills instead of failing");
        for t in [&heap.decoded.new, &heap.decoded.old] {
            assert_eq!(t.as_ref().unwrap().columns[0], Some(Datum::Null));
        }
        assert_eq!(stats.toast_values_filled_oversize.load(Relaxed), 2);
        assert_eq!(stats.toast_values_filled_default.load(Relaxed), 0);
        assert_eq!(stats.toast_values_fetched.load(Relaxed), 0);

        let mut heap = heap_with_value(1, 0x100, 8);
        heap.decoded.new = Some(toast_ptr_tuple(55));
        let result =
            detoast_heap(&mut heap, None, &maps, &capped(InlineValueOverflow::Error)).await;
        assert!(matches!(
            result,
            Err(XactBufferError::ValueTooLarge { rawsize: 4, max: 3 })
        ));
        assert_eq!(stats.toast_values_filled_oversize.load(Relaxed), 2);
    }

    /// Spool + file-ref map for one value's `(seq, body)` chunks; `lsn`
    /// keys the spool filename so one tempdir hosts several
    fn file_refs(
        dir: &std::path::Path,
        lsn: u64,
        key: (u32, u32),
        chunks: &[(u32, &[u8])],
    ) -> (BodySpoolWriter, ChunkRefMap) {
        let mut w = BodySpoolWriter::create(dir, 1, lsn, None).unwrap();
        let mut map = ChunkRefMap::new();
        for &(seq, body) in chunks {
            let r = Body::File(w.append(body).unwrap());
            match map.entry(key) {
                Entry::Occupied(mut o) => {
                    o.get_mut().push(seq, r);
                }
                Entry::Vacant(v) => {
                    v.insert(ValueRef::new(seq, r));
                }
            }
        }
        w.flush().unwrap();
        (w, map)
    }

    /// One-key resolution scope with cache pre-seeded to a store outcome
    impl ValueResolution<'_> {
        /// Resolve every external cell of `t` in place
        fn resolve_tuple(
            &mut self,
            t: &mut crate::decode::heap_decoder::DecodedTuple,
        ) -> std::result::Result<(), XactBufferError> {
            for col in t.columns.iter_mut() {
                if let Some(Datum::External(p)) = *col {
                    *col = Some(self.resolve(&p)?);
                }
            }
            Ok(())
        }
    }

    fn seeded<'a>(
        resolver: &'a ToastResolver,
        spool: Option<&'a BodySpoolFile>,
        xact_maps: &'a [&'a ChunkRefMap],
        cache: HashMap<(u32, u32), std::result::Result<Vec<u8>, StoreMiss>>,
    ) -> ValueResolution<'a> {
        ValueResolution {
            spool,
            xact_maps,
            resolver,
            cache: cache
                .into_iter()
                .map(|(key, value)| {
                    (
                        key,
                        CachedValue {
                            remaining: 1,
                            value,
                        },
                    )
                })
                .collect(),
            retained: 0,
        }
    }

    /// One miss matrix for streaming and both bootstrap store owners: each
    /// cell's fill counter, or fail-closed detail plus fetch-miss count
    #[test]
    fn store_less_fetch_splits_as_no_store() {
        assert_eq!(StoreMiss::split(None), Err(StoreMiss::NoStore));
    }

    #[test]
    fn fill_store_miss_policy_matrix() {
        use std::sync::atomic::Ordering::Relaxed;
        let p = ToastPointer {
            va_rawsize: 8,
            va_extinfo: 4,
            va_valueid: 55,
            va_toastrelid: 16500,
        };
        let misses = [
            StoreMiss::NoStore,
            StoreMiss::Missing,
            StoreMiss::Mismatch { got: 3 },
            StoreMiss::Generation,
        ];
        // Columns: default, superseded, mismatch, generation, fetch_miss
        let cases = [
            (MissPolicy::Streaming, [1, 1, 1, 1, 0]),
            (MissPolicy::BootstrapReadOnly, [1, 2, 0, 1, 0]),
            (MissPolicy::BootstrapOwned, [1, 0, 0, 1, 2]),
        ];
        for (policy, want) in cases {
            let stats = Arc::new(crate::emit::ch_emitter::EmitterStats::default());
            let resolver = ToastResolver::disabled().with_stats(stats.clone());
            let mut errs = Vec::new();
            for miss in misses {
                match fill_store_miss(miss, &p, &resolver, policy) {
                    Ok(v) => assert_eq!(v, Datum::Null),
                    Err(detail) => errs.push(detail),
                }
            }
            let counts = [
                stats.toast_values_filled_default.load(Relaxed),
                stats.toast_values_filled_superseded.load(Relaxed),
                stats.toast_values_filled_mismatch.load(Relaxed),
                stats.toast_values_filled_generation.load(Relaxed),
                stats.toast_fetch_miss.load(Relaxed),
            ];
            assert_eq!(counts, want, "{policy:?}");
            if policy == MissPolicy::BootstrapOwned {
                assert_eq!(
                    errs,
                    [
                        "has no chunks in the store",
                        "chunks sum to 3 bytes, pointer says 4",
                    ]
                );
            } else {
                assert!(errs.is_empty(), "{policy:?}: {errs:?}");
            }
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn value_observed_mid_write_resolves_from_store() {
        let stats = Arc::new(crate::emit::ch_emitter::EmitterStats::default());
        let resolver =
            ToastResolver::with_store(Arc::new(crate::toast::MemChunkStore::new()), stats.clone());
        let key = (16500u32, 55u32);

        let tail_only = mem_refs(key, &[(1, b"cd")]);
        let maps = [&tail_only];
        let mut t = toast_ptr_tuple(55);
        let cache = HashMap::from_iter([(key, Ok(b"abcd".to_vec()))]);
        seeded(&resolver, None, &maps, cache)
            .resolve_tuple(&mut t)
            .unwrap();
        assert_eq!(t.columns[0], Some(build::bytes(b"abcd")));

        let gapped_tail = mem_refs(key, &[(1, b"c"), (3, b"d")]);
        let maps = [&gapped_tail];
        let mut t = toast_ptr_tuple(55);
        let err = seeded(&resolver, None, &maps, HashMap::new())
            .resolve_tuple(&mut t)
            .expect_err("a gap after the first observed chunk stays loud");
        assert!(
            matches!(err, XactBufferError::MissingToastChunk { .. }),
            "{err}"
        );
    }

    #[test]
    fn observed_mid_value_needs_a_dense_tail_after_seq_zero() {
        let key = (16500u32, 55u32);
        let v = |chunks: &[(u32, &'static [u8])]| mem_refs(key, chunks).remove(&key).unwrap();
        assert!(observed_mid_value(&v(&[(2, b"a"), (3, b"b")])));
        assert!(!observed_mid_value(&v(&[(0, b"a"), (1, b"b")])));
        assert!(!observed_mid_value(&v(&[(2, b"a"), (4, b"b")])));
        let tmp = tempdir().unwrap();
        let (_w, run) = file_refs(tmp.path(), 0x10, key, &[(0, b"a"), (1, b"b")]);
        assert!(
            !observed_mid_value(&run[&key]),
            "spooled run starts at seq 0"
        );
    }

    /// Miss policy split: in-xact gap stays a hard error; a store-side miss
    /// (key absent from every xact map) NULL-fills + counts superseded;
    /// disabled mode NULL-fills + counts default.
    #[tokio::test(flavor = "current_thread")]
    async fn resolve_tuple_splits_in_xact_gap_from_store_miss() {
        let tmp = tempdir().unwrap();
        let stats = Arc::new(crate::emit::ch_emitter::EmitterStats::default());
        let store_resolver =
            ToastResolver::with_store(Arc::new(crate::toast::MemChunkStore::new()), stats.clone());

        let key = (16500u32, 55u32);

        // Store miss: no xact map holds the key → superseded fill
        let mut t = toast_ptr_tuple(55);
        let cache = HashMap::from_iter([(key, Err(StoreMiss::Missing))]);
        let mut r = seeded(&store_resolver, None, &[], cache);
        r.resolve_tuple(&mut t).unwrap();
        assert_eq!(t.columns[0], Some(Datum::Null));
        assert_eq!(r.retained, 0, "fills retain nothing");
        assert_eq!(
            stats
                .toast_values_filled_superseded
                .load(std::sync::atomic::Ordering::Relaxed),
            1
        );

        // In-xact gap: key present but seq 1 missing → hard error
        let gapped = mem_refs(key, &[(0, b"ab"), (2, b"cd")]);
        let maps = [&gapped];
        let mut t = toast_ptr_tuple(55);
        let err = seeded(&store_resolver, None, &maps, HashMap::new())
            .resolve_tuple(&mut t)
            .expect_err("in-xact gap surfaces");
        assert!(matches!(
            err,
            XactBufferError::MissingToastChunk { missing: 1, .. }
        ));
        assert_eq!(
            stats
                .toast_fetch_miss
                .load(std::sync::atomic::Ordering::Relaxed),
            1
        );

        // In-xact memory refs resolve without spool, inline cells kept
        let whole = mem_refs(key, &[(0, b"ab"), (1, b"cd")]);
        let maps = [&whole];
        let mut t = toast_ptr_tuple(55);
        t.columns.push(Some(build::int4(7)));
        let mut r = seeded(&store_resolver, None, &maps, HashMap::new());
        r.resolve_tuple(&mut t).unwrap();
        assert_eq!(
            t.columns,
            [Some(build::bytes(b"abcd")), Some(build::int4(7))]
        );
        assert_eq!(r.retained, 4, "decoded bytes tally for the permit shrink");

        // In-xact file refs resolve through the spool
        let (w, spooled) = file_refs(tmp.path(), 0x10, key, &[(0, b"ab"), (1, b"cd")]);
        let maps = [&spooled];
        let mut t = toast_ptr_tuple(55);
        seeded(
            &store_resolver,
            Some(w.shared().as_ref()),
            &maps,
            HashMap::new(),
        )
        .resolve_tuple(&mut t)
        .unwrap();
        assert_eq!(t.columns[0], Some(build::bytes(b"abcd")));

        // Out-of-order file refs land in the tail, read one by one
        let (w, tail) = file_refs(tmp.path(), 0x20, key, &[(1, b"cd"), (0, b"ab")]);
        assert_eq!(tail[&key].run_chunks, 0);
        let maps = [&tail];
        let mut t = toast_ptr_tuple(55);
        seeded(
            &store_resolver,
            Some(w.shared().as_ref()),
            &maps,
            HashMap::new(),
        )
        .resolve_tuple(&mut t)
        .unwrap();
        assert_eq!(t.columns[0], Some(build::bytes(b"abcd")));

        // Store-resolved hit lands assembled bytes
        let mut t = toast_ptr_tuple(55);
        let cache = HashMap::from_iter([(key, Ok(b"abcd".to_vec()))]);
        let mut r = seeded(&store_resolver, None, &[], cache);
        r.resolve_tuple(&mut t).unwrap();
        assert_eq!(t.columns[0], Some(build::bytes(b"abcd")));
        assert_eq!(r.retained, 4);
        assert!(r.cache.is_empty(), "last use moves the buffer out");

        // Store-side run deviation (partial merge collapse): fills,
        // counted mismatch — not superseded, not a hard error
        let mut t = toast_ptr_tuple(55);
        let cache = HashMap::from_iter([(key, Err(StoreMiss::Mismatch { got: 3 }))]);
        seeded(&store_resolver, None, &[], cache)
            .resolve_tuple(&mut t)
            .unwrap();
        assert_eq!(t.columns[0], Some(Datum::Null));
        assert_eq!(
            stats
                .toast_values_filled_mismatch
                .load(std::sync::atomic::Ordering::Relaxed),
            1
        );
        assert_eq!(
            stats
                .toast_values_filled_superseded
                .load(std::sync::atomic::Ordering::Relaxed),
            1,
            "short run counts mismatch, not superseded"
        );

        // In-xact dense-but-short: decode bug, hard error. Size check
        // fires before any spool read
        let xact_short = mem_refs(key, &[(0, b"ab")]);
        let maps = [&xact_short];
        let mut t = toast_ptr_tuple(55);
        let err = seeded(&store_resolver, None, &maps, HashMap::new())
            .resolve_tuple(&mut t)
            .expect_err("in-xact size mismatch surfaces");
        assert!(matches!(err, XactBufferError::Detoast(_)));

        // Disabled mode: any miss NULL-fills as before, counted default,
        // in-xact gap included
        let disabled_stats = Arc::new(crate::emit::ch_emitter::EmitterStats::default());
        let disabled = ToastResolver::disabled().with_stats(disabled_stats.clone());
        let mut t = toast_ptr_tuple(55);
        seeded(&disabled, None, &[], HashMap::new())
            .resolve_tuple(&mut t)
            .unwrap();
        assert_eq!(t.columns[0], Some(Datum::Null));
        let maps = [&gapped];
        let mut t = toast_ptr_tuple(55);
        seeded(&disabled, None, &maps, HashMap::new())
            .resolve_tuple(&mut t)
            .unwrap();
        assert_eq!(t.columns[0], Some(Datum::Null));
        let filled = &disabled_stats.toast_values_filled_default;
        assert_eq!(filled.load(std::sync::atomic::Ordering::Relaxed), 2);
    }

    /// Store-bound pointer resolved without prefetch breaks caller contract
    #[test]
    #[should_panic(expected = "prefetched with the sites")]
    fn resolve_store_without_prefetch_panics() {
        let stats = Arc::new(crate::emit::ch_emitter::EmitterStats::default());
        let resolver =
            ToastResolver::with_store(Arc::new(crate::toast::MemChunkStore::new()), stats);
        let mut r = ValueResolution {
            spool: None,
            xact_maps: &[],
            resolver: &resolver,
            cache: HashMap::new(),
            retained: 0,
        };
        let p = ToastPointer {
            va_rawsize: 8,
            va_extinfo: 4,
            va_valueid: 55,
            va_toastrelid: 16500,
        };
        let _ = r.resolve_store(&p);
    }

    /// Store fetch runs once per key in one round trip: decode once,
    /// clone for earlier duplicate uses (old/new tuple reuse), move the
    /// buffer on the last
    #[tokio::test(flavor = "current_thread")]
    async fn resolve_store_fetches_once_and_moves_last_use() {
        use crate::toast::{ChunkStore, MemChunkStore, ToastRow};
        let store = Arc::new(MemChunkStore::new());
        store
            .put(&[ToastRow {
                db_oid: 0,
                toast_relid: 16500,
                blkno: 1,
                offnum: 1,
                chunk_id: 55,
                chunk_seq: 0,
                chunk_data: bytes::Bytes::from_static(b"abcd"),
                lsn: 1,
            }])
            .await
            .unwrap();
        let stats = Arc::new(crate::emit::ch_emitter::EmitterStats::default());
        let resolver = ToastResolver::with_store(store, stats.clone());
        let p = ToastPointer {
            va_rawsize: 8,
            va_extinfo: 4,
            va_valueid: 55,
            va_toastrelid: 16500,
        };
        let cache = prefetch_store_values(&[p, p], &[], &resolver, 0, 10)
            .await
            .unwrap();
        let stored = cache[&(16500, 55)].value.as_ref().unwrap().as_ptr();
        let mut r = ValueResolution {
            spool: None,
            xact_maps: &[],
            resolver: &resolver,
            cache,
            retained: 0,
        };
        let first = r.resolve_store(&p).unwrap();
        assert!(!r.cache.is_empty(), "pending duplicate use stays cached");
        let second = r.resolve_store(&p).unwrap();
        assert_eq!(first, build::bytes(b"abcd"));
        assert_eq!(second, build::bytes(b"abcd"));
        assert!(matches!(&first, Datum::Varlena(b) if b.as_ptr() != stored));
        assert!(matches!(&second, Datum::Varlena(b) if b.as_ptr() == stored));
        assert!(r.cache.is_empty(), "last use moves the buffer out");
        assert_eq!(r.retained, 8, "both uses retain their copy");
        assert_eq!(
            stats
                .toast_values_fetched
                .load(std::sync::atomic::Ordering::Relaxed),
            1,
            "one fetch serves both uses"
        );
        assert_eq!(
            stats
                .toast_value_fetch_batches
                .load(std::sync::atomic::Ordering::Relaxed),
            1,
            "gathered into one round trip"
        );
    }

    /// Every store-bound pointer of a heap gathers into one round trip;
    /// keys the xact's chunk map holds whole never reach the store, keys it
    /// saw only past seq 0 still do
    #[tokio::test(flavor = "current_thread")]
    async fn prefetch_gathers_store_values_and_skips_in_xact_keys() {
        use crate::toast::{ChunkStore, MemChunkStore, ToastRow};
        let store = Arc::new(MemChunkStore::new());
        let row = |chunk_id: u32, blkno: u32| ToastRow {
            db_oid: 0,
            toast_relid: 16500,
            blkno,
            offnum: 1,
            chunk_id,
            chunk_seq: 0,
            chunk_data: bytes::Bytes::from_static(b"abcd"),
            lsn: 1,
        };
        store.put(&[row(55, 1), row(56, 2)]).await.unwrap();
        let stats = Arc::new(crate::emit::ch_emitter::EmitterStats::default());
        let resolver = ToastResolver::with_store(store, stats.clone());
        let ptr = |va_valueid| ToastPointer {
            va_rawsize: 8,
            va_extinfo: 4,
            va_valueid,
            va_toastrelid: 16500,
        };
        let in_xact = mem_refs((16500, 57), &[(0, b"abcd")]);
        let mid = mem_refs((16500, 56), &[(1, b"cd")]);
        let ptrs = [ptr(55), ptr(56), ptr(57)];
        let cache = prefetch_store_values(&ptrs, &[&in_xact, &mid], &resolver, 0, 10)
            .await
            .unwrap();
        assert_eq!(cache.len(), 2, "in-xact key stays out of the cache");
        assert_eq!(
            stats
                .toast_value_fetch_batches
                .load(std::sync::atomic::Ordering::Relaxed),
            1,
            "one round trip per toast rel"
        );
        assert_eq!(
            stats
                .toast_values_fetched
                .load(std::sync::atomic::Ordering::Relaxed),
            2
        );
    }

    // ── subxact tracking ──────────────────────────────────────────────

    #[test]
    fn subxact_tracker_round_trip() {
        let mut t = SubxactTracker::new();
        t.assign(100, &[]);
        assert!(t.children.is_empty(), "empty assignment adds no edge");
        t.assign(100, &[101, 102]);
        assert_eq!(t.top_for(101), 100);
        assert_eq!(t.top_for(102), 100);
        // Unknown xid returns itself, PG "sub's top is itself pre-ASSIGNMENT"
        assert_eq!(t.top_for(100), 100);
        assert_eq!(t.top_for(999), 999);
        let subs = t.subxids_of(100);
        assert!(subs.contains(&101) && subs.contains(&102) && subs.len() == 2);
        // Idempotent: no duplicate edges
        t.assign(100, &[101]);
        assert_eq!(t.subxids_of(100).len(), 2);
        t.forget_tree(100);
        assert_eq!(t.top_for(101), 101);
        assert_eq!(t.top_for(102), 102);
        assert!(t.subxids_of(100).is_empty());
    }

    #[test]
    fn subxact_tracker_retargets_subxid_to_new_top() {
        // Reassign a subxid to a new top; old children edge must drop
        let mut t = SubxactTracker::new();
        t.assign(10, &[20]);
        t.assign(30, &[20]);
        assert_eq!(t.top_for(20), 30);
        assert!(t.subxids_of(10).is_empty());
        assert_eq!(t.subxids_of(30), [20]);
    }

    #[test]
    fn parse_xact_assignment_decodes_xtop_and_subs() {
        // xtop=0x11223344, nsub=2, subs=[0x55, 0x66].
        let mut buf = Vec::new();
        buf.extend_from_slice(&0x11223344u32.to_le_bytes());
        buf.extend_from_slice(&2i32.to_le_bytes());
        buf.extend_from_slice(&0x55u32.to_le_bytes());
        buf.extend_from_slice(&0x66u32.to_le_bytes());
        let (xtop, subs) = parse_xact_assignment(&buf).expect("parses");
        assert_eq!(xtop, 0x11223344);
        assert_eq!(subs, [0x55, 0x66]);
        // Short main_data → None
        assert!(parse_xact_assignment(&buf[..6]).is_none());
        // Negative nsub → reject
        let mut bad = Vec::new();
        bad.extend_from_slice(&1u32.to_le_bytes());
        bad.extend_from_slice(&(-1i32).to_le_bytes());
        assert!(parse_xact_assignment(&bad).is_none());
    }

    #[test]
    fn parse_xact_payload_extracts_xact_time_without_xinfo() {
        // No HAS_INFO: body is just the 8-byte timestamp
        let ts = 0x0123_4567_89AB_CDEFi64;
        let body = ts.to_le_bytes();
        let p = parse_xact_payload(0x00, &body, 0xD116).unwrap();
        assert_eq!(p.xact_time, ts);
        assert!(p.subxacts.is_empty());
    }

    #[test]
    fn parse_xact_payload_reads_subxacts_with_dbinfo_skip() {
        // xinfo = DBINFO | SUBXACTS: skip-walk 8-byte dbInfo (dbOid+tsOid)
        // to reach the subxacts header
        let mut body = Vec::new();
        body.extend_from_slice(&42i64.to_le_bytes()); // xact_time
        body.extend_from_slice(&(XACT_XINFO_HAS_DBINFO | XACT_XINFO_HAS_SUBXACTS).to_le_bytes());
        body.extend_from_slice(&5u32.to_le_bytes()); // dbId
        body.extend_from_slice(&1663u32.to_le_bytes()); // tsId
        body.extend_from_slice(&3i32.to_le_bytes()); // nsubxacts
        body.extend_from_slice(&0xAAu32.to_le_bytes());
        body.extend_from_slice(&0xBBu32.to_le_bytes());
        body.extend_from_slice(&0xCCu32.to_le_bytes());
        let p = parse_xact_payload(XLOG_XACT_HAS_INFO, &body, 0xD116).unwrap();
        assert_eq!(p.xact_time, 42);
        assert_eq!(p.subxacts, [0xAA, 0xBB, 0xCC]);
    }

    #[test]
    fn parse_xact_payload_handles_no_has_info() {
        // HAS_INFO unset: parser must not consume bytes past the timestamp
        let mut body = 7i64.to_le_bytes().to_vec();
        body.extend_from_slice(&[0xFF; 16]);
        let p = parse_xact_payload(0x00, &body, 0xD116).unwrap();
        assert_eq!(p.xact_time, 7);
        assert!(p.subxacts.is_empty());
    }

    #[test]
    fn parse_xact_payload_short_main_data_errors() {
        assert!(parse_xact_payload(XLOG_XACT_HAS_INFO, &[1, 2, 3, 4], 0xD116).is_err());
    }

    #[test]
    fn parse_xact_payload_extracts_twophase_xid_past_inval_skip() {
        // COMMIT PREPARED shape: xinfo = INVALS | TWOPHASE | GID; the
        // prepared xid keys DROP-sweep disarm (header xact_id is the
        // finishing backend's, not the prepared xact's)
        let mut body = Vec::new();
        body.extend_from_slice(&42i64.to_le_bytes()); // xact_time
        body.extend_from_slice(
            &(XACT_XINFO_HAS_INVALS | XACT_XINFO_HAS_TWOPHASE | XACT_XINFO_HAS_GID).to_le_bytes(),
        );
        body.extend_from_slice(&1i32.to_le_bytes()); // nmsgs
        body.extend_from_slice(&[0u8; 16]); // SharedInvalidationMessage
        body.extend_from_slice(&0x1234u32.to_le_bytes()); // xl_xact_twophase
        body.extend_from_slice(b"gid\0");
        let p = parse_xact_payload(XLOG_XACT_HAS_INFO, &body, 0xD116).unwrap();
        assert_eq!(p.twophase_xid, Some(0x1234));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn abort_with_subxids_drops_each_buffer() {
        let tmp = tempdir().unwrap();
        let mut b = XactBuffer::new(cfg(tmp.path().to_path_buf())).unwrap();
        b.on_heap(heap_with_value(300, 100, 16)).await.unwrap();
        b.on_heap(heap_with_value(301, 200, 16)).await.unwrap();
        b.on_heap(heap_with_value(302, 300, 16)).await.unwrap();
        b.abort(300, Pos::new(0x500), &[301, 302]).await.unwrap();
        assert!(b.active_xids().is_empty());
        // One bump per terminator record, not per subxid
        assert_eq!(b.stats().aborted_xacts_total, 1);
    }

    // ── drain streaming ───────────────────────────────────────────────

    fn chunk(value_id: u32, seq: u32, lsn: u64, body: &[u8]) -> ToastChunk {
        ToastChunk {
            db_oid: 0,
            toast_relid: 16400,
            value_id,
            chunk_seq: seq,
            source_lsn: lsn,
            blkno: 0,
            offnum: 1 + seq as u16,
            chunk_data: bytes::Bytes::copy_from_slice(body),
        }
    }

    fn dropped_event(oid: u32) -> SchemaEvent {
        use crate::schema::RelName;
        SchemaEvent::Dropped {
            oid,
            rel_name: RelName::new("public", &format!("t{oid}")),
            partition: None,
        }
    }

    /// Order label of [`dropped_event`] as [`flatten_batch`] renders it
    fn dropped_label(oid: u32) -> String {
        format!("{:?}", DrainEntry::Catalog(dropped_event(oid)))
    }

    /// Interleave a batch's events at their local indices with its heaps:
    /// event Debug / `h<lsn>` labels for order assertions.
    fn flatten_batch(batch: &DrainedBatch) -> Vec<String> {
        let label = |e: &DrainEntry| format!("{e:?}");
        let mut out = Vec::new();
        let mut ev = 0usize;
        for (i, h) in batch.cells().unwrap().iter().enumerate() {
            while ev < batch.ordered_events.len() && batch.ordered_events[ev].heap_idx <= i {
                out.push(label(&batch.ordered_events[ev].event));
                ev += 1;
            }
            out.push(format!("h{}", h.decoded.source_lsn));
        }
        while ev < batch.ordered_events.len() {
            out.push(label(&batch.ordered_events[ev].event));
            ev += 1;
        }
        out
    }

    /// Events pushed out of LSN order (pump-side capture keys bias-early
    /// valid_from, worker pushes at observe order) still drain LSN ASC.
    #[tokio::test(flavor = "current_thread")]
    async fn drain_sorts_out_of_order_event_pushes() {
        let tmp = tempdir().unwrap();
        let mut b = XactBuffer::new(cfg(tmp.path().to_path_buf())).unwrap();
        b.on_heap(heap_with_value(1, 120, 16)).await.unwrap();
        // Arrival order 150, 100: 100 must still precede the heap@120
        b.on_schema_event(1, 150, dropped_event(9));
        b.on_schema_event(1, 100, dropped_event(7));
        let mut drain = b
            .drain_committed(StashResolved::nothing_stashed(1), 42, 0x2000, &[], false)
            .await
            .unwrap();
        let mut order: Vec<String> = Vec::new();
        while let Some(batch) = drain.next_batch(8, usize::MAX, None).await.unwrap() {
            order.extend(flatten_batch(&batch));
        }
        assert_eq!(order, [dropped_label(7), "h120".into(), dropped_label(9)]);
        drain.finish().await.unwrap();
    }

    /// One raw MULTI_INSERT (stashed under a subxact) fans out to every
    /// tuple in order — each carrying the WRITER xid through the merge —
    /// before anything past its LSN yields; an event at the same LSN keeps
    /// the event-first tie break; a later heap follows the whole fanout.
    /// `Ordinary` verdict injected directly (no descriptor log here)
    #[tokio::test(flavor = "current_thread")]
    async fn raw_multi_insert_fanout_orders_and_keeps_xid() {
        let tmp = tempdir().unwrap();
        let mut b = XactBuffer::new(cfg(tmp.path().to_path_buf())).unwrap();
        let rel = int4_descriptor(16410);
        b.stash_raw(8, multi_insert_raw(8, 100, 16410, &[100, 200, 300]))
            .await
            .unwrap();
        b.on_heap(heap_with_value(1, 200, 16)).await.unwrap();
        b.on_schema_event(1, 100, dropped_event(7));
        let stash = StashResolved::ordinary(1, rel, 0x50);

        let mut drain = b
            .drain_committed(stash, 42, 0x2000, &[8], false)
            .await
            .unwrap();
        let batch = drain
            .next_batch(8, usize::MAX, None)
            .await
            .unwrap()
            .expect("one slice");
        assert!(batch.is_final);
        assert_eq!(batch.ordered_events.len(), 1);
        assert_eq!(
            batch.ordered_events[0].heap_idx, 0,
            "same-LSN event precedes the fanout"
        );
        assert_eq!(
            batch.cells().unwrap().len(),
            4,
            "3 fanned-out tuples + later heap"
        );
        for (i, expected) in [100i32, 200, 300].iter().enumerate() {
            let h = &batch.cells().unwrap()[i];
            assert_eq!(h.decoded.source_lsn, 100);
            assert_eq!(h.decoded.xid, 8, "writer xid survives subxact merge");
            assert_eq!(h.descriptor_valid_from, 0x50);
            let new = h.decoded.new.as_ref().unwrap();
            assert_eq!(new.columns[0], Some(build::int4(*expected)));
        }
        assert_eq!(
            batch.cells().unwrap()[3].decoded.source_lsn,
            200,
            "fanout first"
        );
        drain.finish().await.unwrap();
    }

    /// Decode counters label the record's op; one record drains as one input
    #[tokio::test(flavor = "current_thread")]
    async fn raw_fanout_counts_decode_ops() {
        let tmp = tempdir().unwrap();
        let mut b = XactBuffer::new(cfg(tmp.path().to_path_buf())).unwrap();
        let rel = int4_descriptor(16415);
        let rfn = rel.rfn;
        b.stash_raw(1, multi_insert_raw(1, 100, 16415, &[1, 2, 3]))
            .await
            .unwrap();
        let stats = Arc::new(EmitterStats::default());
        let stash = inject_ordinary_with_stats(rfn, rel, Some(stats.clone()));
        let mut drain = b
            .drain_committed(stash, 42, 0x2000, &[], false)
            .await
            .unwrap();
        let batch = drain
            .next_batch(2, usize::MAX, None)
            .await
            .unwrap()
            .expect("one slice");
        assert!(batch.is_final);
        assert_eq!(batch.heaps.len(), 1);
        assert_eq!(batch.cells().unwrap().len(), 3);
        let ops = stats.raw_decode_ordinary_ops.load();
        assert_eq!(ops[5], 1, "one multi_insert record decoded");
        assert_eq!(stats.raw_decode_rows_ops.load()[5], 3, "three rows fanned");
        drain.finish().await.unwrap();
    }

    /// MULTI_INSERT lacking `XLH_INSERT_CONTAINS_NEW_TUPLE` cannot prove its
    /// tuple data survived FPIs: typed ImageOnly, drain halts
    #[tokio::test(flavor = "current_thread")]
    async fn raw_image_only_fails_closed() {
        let tmp = tempdir().unwrap();
        let mut b = XactBuffer::new(cfg(tmp.path().to_path_buf())).unwrap();
        let rel = int4_descriptor(16411);
        let mut raw = multi_insert_raw(1, 100, 16411, &[1]);
        raw.main_data[0] = 0;
        b.stash_raw(1, raw).await.unwrap();
        let stash = StashResolved::ordinary(1, rel, 0x50);
        let mut drain = b
            .drain_committed(stash, 42, 0x2000, &[], false)
            .await
            .unwrap();
        let err = drain
            .next_batch(8, usize::MAX, None)
            .await
            .err()
            .expect("fail-closed error");
        assert!(
            matches!(
                err,
                XactBufferError::OrdinaryFailClosed {
                    reason: FailClosedReason::ImageOnly,
                    ..
                }
            ),
            "{err}"
        );
    }

    /// The fence is per record: a stashed record inside a published interval
    /// fails closed at its own LSN while a sibling past `through_lsn` decodes.
    /// Resolution happens at the commit's `next_lsn`, which no interval
    /// covers, so this is the only place the fence can apply
    #[tokio::test(flavor = "current_thread")]
    async fn fence_fails_closed_per_record() {
        let tmp = tempdir().unwrap();
        let mut b = XactBuffer::new(cfg(tmp.path().to_path_buf())).unwrap();
        let rel = int4_descriptor(16420);
        let rfn = rel.rfn;
        // Inside [100, 200): no descriptor proven to read it
        b.stash_raw(1, multi_insert_raw(1, 150, 16420, &[1]))
            .await
            .unwrap();
        let stash = inject_ordinary_fenced(rfn, rel, None, vec![rfn_ambiguity(rfn, 100, 200)]);
        let mut drain = b
            .drain_committed(stash, 42, 0x2000, &[], false)
            .await
            .unwrap();
        let err = drain
            .next_batch(8, usize::MAX, None)
            .await
            .err()
            .expect("fenced record to fail closed");
        assert!(
            matches!(
                err,
                XactBufferError::StashAmbiguous {
                    lsn: 150,
                    from_lsn: 100,
                    through_lsn: 200,
                    ..
                }
            ),
            "{err}"
        );
    }

    /// Keep earlier ambiguity intervals in the history even when the
    /// descriptor lookup at commit succeeds.
    #[tokio::test(flavor = "current_thread")]
    async fn resolve_stash_attaches_fence_from_log() {
        use crate::catalog::desc_log::{
            Ambiguity, AmbiguityReason, AmbiguityScope, BatchRecord, DescLogIdentity, LogEntry,
            LogValue,
        };
        let log_dir = tempdir().unwrap();
        let log = DescriptorLog::open(
            log_dir.path(),
            DescLogIdentity {
                pg_major: 17,
                system_id: "7".into(),
                timeline: 1,
                db_oid: 5,
                wal_seg_size: 16 * 1024 * 1024,
            },
        )
        .await
        .unwrap();
        let rel = int4_descriptor(16430);
        let mut batch = BatchRecord {
            captured_at: 0x300,
            commit_lsn: 0x2F0,
            observations: Vec::new(),
            ambiguities: Vec::new(),
            entries: vec![Arc::new(LogEntry {
                valid_from: 0x300,
                oid: rel.oid,
                rfn: rel.rfn,
                value: LogValue::Present(rel.clone()),
            })],
        };
        for scope in [AmbiguityScope::Rfn(rel.rfn), AmbiguityScope::Oid(rel.oid)] {
            batch.ambiguities.push(Arc::new(Ambiguity {
                scope,
                from_lsn: 0x100,
                through_lsn: 0x300,
                reason: AmbiguityReason::UnknownMutationPosition,
            }));
        }
        log.append_batch(batch).await.unwrap();

        let spill_dir = tempdir().unwrap();
        let buffer = Arc::new(Mutex::new(
            XactBuffer::new(cfg(spill_dir.path().to_path_buf())).unwrap(),
        ));
        buffer
            .lock()
            .await
            .stash_raw(1, multi_insert_raw(1, 0x200, 16430, &[1]))
            .await
            .unwrap();
        let stash = resolve_stash(
            &buffer,
            &log,
            1,
            &[],
            0x300,
            Arc::new(EmitterStats::default()),
        )
        .await
        .unwrap();
        let mut b = buffer.lock().await;
        let mut drain = b
            .drain_committed(stash, 42, 0x2F0, &[], false)
            .await
            .unwrap();
        let err = drain
            .next_batch(8, usize::MAX, None)
            .await
            .err()
            .expect("the log's interval to fence the stashed record");
        assert!(
            matches!(
                err,
                XactBufferError::StashAmbiguous {
                    lsn: 0x200,
                    from_lsn: 0x100,
                    through_lsn: 0x300,
                    ..
                }
            ),
            "{err}"
        );
    }

    /// Log for db 5 holding `rel` from 0x100
    async fn single_rel_log(dir: &std::path::Path, rel: &Arc<RelDescriptor>) -> Arc<DescriptorLog> {
        use crate::catalog::desc_log::{BatchRecord, DescLogIdentity, LogEntry, LogValue};
        let log = Arc::new(
            DescriptorLog::open(
                dir,
                DescLogIdentity {
                    pg_major: 17,
                    system_id: "7".into(),
                    timeline: 1,
                    db_oid: 5,
                    wal_seg_size: 16 * 1024 * 1024,
                },
            )
            .await
            .unwrap(),
        );
        log.append_batch(BatchRecord {
            captured_at: 0x100,
            commit_lsn: 0xF0,
            observations: Vec::new(),
            ambiguities: Vec::new(),
            entries: vec![Arc::new(LogEntry {
                valid_from: 0x100,
                oid: rel.oid,
                rfn: rel.rfn,
                value: LogValue::Present(rel.clone()),
            })],
        })
        .await
        .unwrap();
        log
    }

    /// Relation OIDs can match across databases. Verify TRUNCATE records only
    /// affect relations in configured database
    #[tokio::test(flavor = "current_thread")]
    async fn truncate_of_foreign_db_never_fans_out() {
        use crate::record::Route;
        use walrus::pg::walparser::{XLogRecord, XLogRecordHeader};

        let log_dir = tempdir().unwrap();
        let rel = int4_descriptor(16440);
        let log = single_rel_log(log_dir.path(), &rel).await;

        let truncate_record = |db_oid: u32| {
            let mut md = db_oid.to_le_bytes().to_vec();
            md.extend_from_slice(&1u32.to_le_bytes());
            md.extend_from_slice(&[0u8; 4]); // Flags and alignment padding
            md.extend_from_slice(&rel.oid.to_le_bytes());
            Record {
                parsed: XLogRecord {
                    header: XLogRecordHeader {
                        resource_manager_id: RmId::Heap as u8,
                        info: crate::decode::heap_decoder::XLOG_HEAP_TRUNCATE,
                        xact_id: 7,
                        ..Default::default()
                    },
                    main_data: std::borrow::Cow::Owned(md),
                    ..Default::default()
                },
                source_lsn: 0x200,
                route: Route::ToShadow,
                ..Default::default()
            }
        };

        let spill_dir = tempdir().unwrap();
        let buffer = Arc::new(Mutex::new(
            XactBuffer::new(cfg(spill_dir.path().to_path_buf())).unwrap(),
        ));
        let mut sink = BufferingDecoderSink::new(DescriptorLogs::single(log.clone()), buffer);

        sink.on_record(&truncate_record(6)).await.unwrap();
        assert_eq!(
            sink.stats().truncates.load(Ordering::Relaxed),
            0,
            "foreign-db TRUNCATE must not reach a local oid"
        );
        assert_eq!(
            log.stats_handle()
                .lookups_foreign_db
                .load(Ordering::Relaxed),
            1
        );

        sink.on_record(&truncate_record(5)).await.unwrap();
        assert_eq!(sink.stats().truncates.load(Ordering::Relaxed), 1);
    }

    /// Per-record path skips land in the per-relation ledger by reason,
    /// unexpected ones even with attribution off
    #[tokio::test(flavor = "current_thread")]
    async fn heap_skips_attribute_by_reason() {
        use crate::decode::decoder_sink::SkipKey;
        use crate::record::Route;
        use walrus::pg::walparser::{
            BlockLocation, XLogRecord, XLogRecordBlock, XLogRecordBlockHeader, XLogRecordHeader,
        };

        let log_dir = tempdir().unwrap();
        let log = single_rel_log(log_dir.path(), &int4_descriptor(16450)).await;
        let insert = |db_node: u32, rel_node: u32| Record {
            parsed: XLogRecord {
                header: XLogRecordHeader {
                    resource_manager_id: RmId::Heap as u8,
                    info: crate::decode::heap_decoder::XLOG_HEAP_INSERT,
                    ..Default::default()
                },
                blocks: vec![XLogRecordBlock {
                    header: XLogRecordBlockHeader {
                        location: BlockLocation {
                            rel: RelFileNode {
                                spc_node: 1663,
                                db_node,
                                rel_node,
                            },
                            block_no: 0,
                        },
                        ..Default::default()
                    },
                    ..Default::default()
                }],
                ..Default::default()
            },
            source_lsn: 0x200,
            route: Route::ToDecoder,
            ..Default::default()
        };
        let spill_dir = tempdir().unwrap();
        let buffer = Arc::new(Mutex::new(
            XactBuffer::new(cfg(spill_dir.path().to_path_buf())).unwrap(),
        ));
        let mut sink = BufferingDecoderSink::new(DescriptorLogs::single(log), buffer);
        let key = |db_oid, rel_node, reason| SkipKey {
            db_oid,
            rel_node,
            op: "insert",
            reason,
        };

        sink.on_record(&insert(9, 16450)).await.unwrap();
        sink.on_record(&insert(5, 16499)).await.unwrap();
        assert_eq!(
            sink.stats().skipped_by_rel.snapshot(),
            vec![(key(5, 16499, SkipReason::NotCovered), 1)],
            "flag off attributes only unexpected reasons",
        );

        sink.stats().attribute_skips.store(true, Ordering::Relaxed);
        sink.on_record(&insert(9, 16450)).await.unwrap();
        sink.on_record(&insert(5, 16499)).await.unwrap();
        assert_eq!(sink.stats().catalog_not_found.load(Ordering::Relaxed), 4);
        assert_eq!(
            sink.stats().skipped_by_rel.snapshot(),
            vec![
                (key(5, 16499, SkipReason::NotCovered), 2),
                (key(9, 16450, SkipReason::UnfollowedDb), 1),
            ],
        );
    }

    /// Uncontended acquires count as acquires only; a held mutex also counts
    /// one wait and accrues blocked time
    #[tokio::test(flavor = "current_thread")]
    async fn lock_buffer_counts_waits_only_when_contended() {
        let spill_dir = tempdir().unwrap();
        let buffer = Arc::new(Mutex::new(
            XactBuffer::new(cfg(spill_dir.path().to_path_buf())).unwrap(),
        ));
        let log_dir = tempdir().unwrap();
        let log = single_rel_log(log_dir.path(), &int4_descriptor(16430)).await;
        let sink = BufferingDecoderSink::new(DescriptorLogs::single(log), buffer.clone());
        let stats = sink.stats();

        drop(sink.lock_buffer().await);
        assert_eq!(stats.buffer_lock_acquires.load(Ordering::Relaxed), 1);
        assert_eq!(stats.buffer_lock_waits.load(Ordering::Relaxed), 0);

        // yield_now pends once, so lock_buffer sees mutex held whichever branch polls first
        let held = buffer.lock().await;
        tokio::join!(async { drop(sink.lock_buffer().await) }, async {
            tokio::task::yield_now().await;
            drop(held);
        });
        assert_eq!(stats.buffer_lock_acquires.load(Ordering::Relaxed), 2);
        assert_eq!(stats.buffer_lock_waits.load(Ordering::Relaxed), 1);
        assert!(stats.buffer_lock_wait_nanos.load(Ordering::Relaxed) > 0);
    }

    /// Half-open: `through_lsn` is the first LSN the final descriptor proves,
    /// so a record there decodes and one just below does not
    #[tokio::test(flavor = "current_thread")]
    async fn fence_admits_record_at_through_lsn() {
        let tmp = tempdir().unwrap();
        let mut b = XactBuffer::new(cfg(tmp.path().to_path_buf())).unwrap();
        let rel = int4_descriptor(16421);
        let rfn = rel.rfn;
        b.stash_raw(1, multi_insert_raw(1, 200, 16421, &[7, 8]))
            .await
            .unwrap();
        let stash = inject_ordinary_fenced(
            rfn,
            rel,
            None,
            vec![rfn_ambiguity(rfn, 100, 200), rfn_ambiguity(rfn, 20, 40)],
        );
        let mut drain = b
            .drain_committed(stash, 42, 0x2000, &[], false)
            .await
            .unwrap();
        let batch = drain
            .next_batch(8, usize::MAX, None)
            .await
            .unwrap()
            .expect("one slice");
        assert_eq!(
            batch.cells().unwrap().len(),
            2,
            "record at through_lsn decodes"
        );
        drain.finish().await.unwrap();
    }

    /// Draining stashed raws with no verdict installed would send every one
    /// through the discard arm, fence included — fail closed instead
    #[tokio::test(flavor = "current_thread")]
    async fn missing_stash_resolution_fails_closed() {
        let tmp = tempdir().unwrap();
        let mut b = XactBuffer::new(cfg(tmp.path().to_path_buf())).unwrap();
        b.stash_raw(1, multi_insert_raw(1, 100, 16422, &[1]))
            .await
            .unwrap();
        let err = b
            .drain_committed(StashResolved::nothing_stashed(1), 42, 0x2000, &[], false)
            .await
            .err()
            .expect("fail-closed with no resolution installed");
        assert!(
            matches!(err, XactBufferError::MissingStashResolution { top_xid: 1 }),
            "{err}"
        );
    }

    /// INPLACE mutates tuple bytes with no decode shape: typed reject, not
    /// a silent skip
    #[tokio::test(flavor = "current_thread")]
    async fn raw_unsupported_op_fails_closed() {
        use crate::decode::heap_decoder::{XLOG_HEAP_INPLACE, XLOG_HEAP_INSERT};
        let rel = int4_descriptor(16412);
        let heap = |info| RawRecord {
            rm: RmId::Heap as u8,
            info,
            ..multi_insert_raw(1, 100, 16412, &[1])
        };
        let mut image_only = heap(XLOG_HEAP_INSERT);
        image_only.blocks[0].fork_flags = 0x10;
        let foreign_rm = RawRecord {
            rm: RmId::Btree as u8,
            ..multi_insert_raw(1, 100, 16412, &[1])
        };
        let mut multi_image_only = multi_insert_raw(1, 100, 16412, &[1]);
        multi_image_only.blocks[0].fork_flags = 0x10;
        let cases = [
            (
                heap(XLOG_HEAP_INPLACE),
                FailClosedReason::UnsupportedOperation,
            ),
            (foreign_rm, FailClosedReason::UnsupportedOperation),
            (image_only, FailClosedReason::ImageOnly),
            (multi_image_only, FailClosedReason::ImageOnly),
        ];
        for (raw, want) in cases {
            let tmp = tempdir().unwrap();
            let mut b = XactBuffer::new(cfg(tmp.path().to_path_buf())).unwrap();
            b.stash_raw(1, raw).await.unwrap();
            let stash = StashResolved::ordinary(1, rel.clone(), 0x50);
            let mut drain = b
                .drain_committed(stash, 42, 0x2000, &[], false)
                .await
                .unwrap();
            let err = drain.next_batch(8, usize::MAX, None).await.err().unwrap();
            assert!(
                matches!(&err, XactBufferError::OrdinaryFailClosed { reason, .. } if *reason == want),
                "{want}: {err}"
            );
        }
    }

    /// Stashed toast-heap DELETE folds into a tombstone row at commit
    #[tokio::test(flavor = "current_thread")]
    async fn raw_toast_delete_folds_tombstone() {
        use crate::decode::heap_decoder::XLOG_HEAP_DELETE;
        let tmp = tempdir().unwrap();
        let mut b = XactBuffer::new(cfg(tmp.path().to_path_buf())).unwrap();
        let rel = Arc::new(crate::backfill::backup_page_walk::toast_chunk_rel());
        let block = toast_block(0x20, Vec::new(), Vec::new());
        let main_data = vec![0, 0, 0, 0, 6, 0, 0, 0];
        let raw = toast_raw(RmId::Heap, XLOG_HEAP_DELETE, main_data, block);
        let rfn = raw.rfn().unwrap();
        b.stash_raw(7, raw).await.unwrap();
        let mut outcomes = HashMap::new();
        outcomes.insert(rfn, StashOutcome::Toast(rel.clone()));
        let res = StashResolution {
            outcomes,
            stats: None,
        };
        let stash = StashResolved {
            top_xid: 7,
            res: Some(res),
        };
        let mut drain = b.drain_committed(stash, 0, 0x300, &[], true).await.unwrap();
        let batch = drain
            .next_batch(8, usize::MAX, None)
            .await
            .unwrap()
            .unwrap();
        assert!(batch.heaps.is_empty());
        let [row] = &batch.new_rows[..] else { panic!() };
        assert_eq!(
            (row.toast_relid, row.blkno, row.offnum, row.lsn),
            (rel.oid, 9, 6, 0x200)
        );
        drain.finish().await.unwrap();
    }

    /// Raw record whose writer xid is outside the owning xact + subxacts
    /// (spill corruption / buffer-key drift shape): merge ownership check
    /// fails the drain before the foreign row can emit
    #[tokio::test(flavor = "current_thread")]
    async fn raw_foreign_xid_fails_closed() {
        let tmp = tempdir().unwrap();
        let mut b = XactBuffer::new(cfg(tmp.path().to_path_buf())).unwrap();
        let rel = int4_descriptor(16417);
        b.stash_raw(1, multi_insert_raw(99, 100, 16417, &[5]))
            .await
            .unwrap();
        let stash = StashResolved::ordinary(1, rel, 0x50);
        let mut drain = b
            .drain_committed(stash, 42, 0x2000, &[], false)
            .await
            .unwrap();
        let err = drain
            .next_batch(8, usize::MAX, None)
            .await
            .err()
            .expect("foreign-xid error");
        assert!(
            matches!(err, XactBufferError::ForeignXid { xid: 99, top: 1 }),
            "{err}"
        );
    }

    /// Page maintenance stashed alongside user rows (heap2 PRUNE, heap LOCK)
    /// skips without failing the drain; the user row still lands
    #[tokio::test(flavor = "current_thread")]
    async fn raw_maintenance_ops_skip() {
        let tmp = tempdir().unwrap();
        let mut b = XactBuffer::new(cfg(tmp.path().to_path_buf())).unwrap();
        let rel = int4_descriptor(16413);
        let mut prune = multi_insert_raw(1, 90, 16413, &[1]);
        prune.info = 0x10; // XLOG_HEAP2_PRUNE*
        let mut lock = multi_insert_raw(1, 95, 16413, &[1]);
        lock.rm = RmId::Heap as u8;
        lock.info = crate::decode::heap_decoder::XLOG_HEAP_LOCK;
        b.stash_raw(1, prune).await.unwrap();
        b.stash_raw(1, lock).await.unwrap();
        b.stash_raw(1, multi_insert_raw(1, 100, 16413, &[7]))
            .await
            .unwrap();
        let stash = StashResolved::ordinary(1, rel, 0x50);
        let mut drain = b
            .drain_committed(stash, 42, 0x2000, &[], false)
            .await
            .unwrap();
        let batch = drain
            .next_batch(8, usize::MAX, None)
            .await
            .unwrap()
            .expect("one slice");
        assert!(batch.is_final);
        assert_eq!(
            batch.cells().unwrap().len(),
            1,
            "maintenance skipped, row kept"
        );
        let cells = batch.cells().unwrap();
        let new = cells[0].decoded.new.as_ref().unwrap();
        assert_eq!(new.columns[0], Some(build::int4(7)));
        drain.finish().await.unwrap();
    }

    /// Prefix-elided UPDATE needs a predecessor image raw decode lacks:
    /// typed PartialUpdate. PG only elides below wal_level=logical
    #[tokio::test(flavor = "current_thread")]
    async fn raw_partial_update_fails_closed() {
        use crate::decode::heap_decoder::{
            SIZE_OF_HEAP_UPDATE, XLH_UPDATE_PREFIX_FROM_OLD, XLOG_HEAP_UPDATE,
        };
        use crate::xact::spill::RawBlock;
        let tmp = tempdir().unwrap();
        let mut b = XactBuffer::new(cfg(tmp.path().to_path_buf())).unwrap();
        let rel = int4_descriptor(16414);
        let mut main_data = vec![0u8; SIZE_OF_HEAP_UPDATE];
        main_data[7] = XLH_UPDATE_PREFIX_FROM_OLD;
        // block 0: [prefixlen=4 covers the whole int4][xl_heap_header][pad]
        let mut data = Vec::new();
        data.extend_from_slice(&4u16.to_le_bytes());
        data.extend_from_slice(&1u16.to_le_bytes()); // natts
        data.extend_from_slice(&0u16.to_le_bytes()); // infomask
        data.push(24); // t_hoff
        data.push(0); // bitmap pad
        let raw = RawRecord {
            xid: 1,
            rm: RmId::Heap as u8,
            info: XLOG_HEAP_UPDATE,
            source_lsn: 100,
            page_magic: 0xD114,
            main_data,
            blocks: vec![RawBlock {
                block_id: 0,
                fork_flags: 0x20,
                data_length: data.len() as u16,
                image_length: 0,
                hole_offset: 0,
                hole_length: 0,
                bimg_info: 0,
                spc_node: 1663,
                db_node: 5,
                rel_node: 16414,
                block_no: 0,
                image: Vec::new(),
                data,
            }],
        };
        b.stash_raw(1, raw).await.unwrap();
        let stash = StashResolved::ordinary(1, rel, 0x50);
        let mut drain = b
            .drain_committed(stash, 42, 0x2000, &[], false)
            .await
            .unwrap();
        let err = drain
            .next_batch(8, usize::MAX, None)
            .await
            .err()
            .expect("fail-closed error");
        assert!(
            matches!(
                err,
                XactBufferError::OrdinaryFailClosed {
                    reason: FailClosedReason::PartialUpdate,
                    ..
                }
            ),
            "{err}"
        );
    }

    /// COPY fanout precedes a later UPDATE raw: LSN order holds across the
    /// pending-queue / merge-source boundary
    #[tokio::test(flavor = "current_thread")]
    async fn raw_multi_insert_then_update_lsn_order() {
        let tmp = tempdir().unwrap();
        let mut b = XactBuffer::new(cfg(tmp.path().to_path_buf())).unwrap();
        let rel = int4_descriptor(16415);
        b.stash_raw(1, multi_insert_raw(1, 100, 16415, &[1, 2, 3]))
            .await
            .unwrap();
        b.stash_raw(1, update_raw(1, 150, 16415, 9)).await.unwrap();
        let stash = StashResolved::ordinary(1, rel, 0x50);
        let mut drain = b
            .drain_committed(stash, 42, 0x2000, &[], false)
            .await
            .unwrap();
        let batch = drain
            .next_batch(8, usize::MAX, None)
            .await
            .unwrap()
            .expect("one slice");
        assert!(batch.is_final);
        let ops: Vec<_> = batch
            .cells()
            .unwrap()
            .iter()
            .map(|h| (h.decoded.source_lsn, h.decoded.op))
            .collect();
        assert_eq!(
            ops,
            vec![
                (100, HeapOp::Insert),
                (100, HeapOp::Insert),
                (100, HeapOp::Insert),
                (150, HeapOp::Update),
            ],
        );
        assert_eq!(
            batch.cells().unwrap()[3]
                .decoded
                .new
                .as_ref()
                .unwrap()
                .columns[0],
            Some(build::int4(9)),
        );
        drain.finish().await.unwrap();
    }

    /// Decode buffered records using the descriptor in effect at each LSN.
    #[tokio::test(flavor = "current_thread")]
    async fn raw_records_fold_under_the_shape_at_their_lsn() {
        let tmp = tempdir().unwrap();
        let mut b = XactBuffer::new(cfg(tmp.path().to_path_buf())).unwrap();
        // Commit shape has a second column the boundary shape lacks, so
        // which descriptor decoded a record is visible in its column count
        let boundary_shape = int4_descriptor(16417);
        let mut commit_shape = (*boundary_shape).clone();
        let mut extra = commit_shape.attributes[0].clone();
        extra.attnum = 2;
        extra.name = "added".into();
        commit_shape.attributes.push(extra);
        let rfn = boundary_shape.rfn;
        b.stash_raw(1, multi_insert_raw(1, 100, 16417, &[1]))
            .await
            .unwrap();
        b.stash_raw(1, multi_insert_raw(1, 300, 16417, &[2]))
            .await
            .unwrap();
        let stash = inject_ordinary_shapes(
            rfn,
            vec![(0x50, boundary_shape), (200, Arc::new(commit_shape))],
        );
        let mut drain = b
            .drain_committed(stash, 42, 0x2000, &[], false)
            .await
            .unwrap();
        let batch = drain
            .next_batch(8, usize::MAX, None)
            .await
            .unwrap()
            .expect("one slice");
        let shapes: Vec<(u64, usize, u64)> = batch
            .cells()
            .unwrap()
            .iter()
            .map(|h| {
                (
                    h.decoded.source_lsn,
                    h.descriptor.attributes.len(),
                    h.descriptor_valid_from,
                )
            })
            .collect();
        assert_eq!(
            shapes,
            vec![(100, 1, 0x50), (300, 2, 200)],
            "each record uses the descriptor at its LSN",
        );
        drain.finish().await.unwrap();
    }

    /// Stop if a buffered record predates the first known descriptor.
    #[tokio::test(flavor = "current_thread")]
    async fn raw_record_before_any_shape_fails_closed() {
        let tmp = tempdir().unwrap();
        let mut b = XactBuffer::new(cfg(tmp.path().to_path_buf())).unwrap();
        let rel = int4_descriptor(16418);
        b.stash_raw(1, multi_insert_raw(1, 0x40, 16418, &[7]))
            .await
            .unwrap();
        let stash = StashResolved::ordinary(1, rel, 0x50);
        let mut drain = b
            .drain_committed(stash, 42, 0x2000, &[], false)
            .await
            .unwrap();
        let err = drain
            .next_batch(8, usize::MAX, None)
            .await
            .err()
            .expect("record before the first descriptor must fail");
        assert!(
            matches!(
                err,
                XactBufferError::StashUncovered {
                    rel_node: 16418,
                    lsn: 0x40
                }
            ),
            "{err:?}"
        );
    }

    /// Writer xid reaches fanned-out heaps identically whether the raw
    /// stash drained from memory or from spill (v6 keeps xid on disk), and
    /// the pending queue's resident accounting fully releases
    #[tokio::test(flavor = "current_thread")]
    async fn raw_xid_survives_memory_and_spill() {
        let tmp = tempdir().unwrap();
        let mut b = XactBuffer::new(cfg(tmp.path().to_path_buf())).unwrap();
        let rel = int4_descriptor(16416);
        // ~3.6 KiB of tuple data blows the 1 KiB budget: subxact 8 spills
        let big: Vec<i32> = (0..300).collect();
        b.stash_raw(8, multi_insert_raw(8, 100, 16416, &big))
            .await
            .unwrap();
        b.stash_raw(9, multi_insert_raw(9, 200, 16416, &[7]))
            .await
            .unwrap();
        let spilled: Vec<(u32, bool)> = b
            .inflight_snapshot()
            .iter()
            .map(|e| (e.xid, e.spilled))
            .collect();
        assert!(spilled.contains(&(8, true)), "{spilled:?}");
        assert!(spilled.contains(&(9, false)), "{spilled:?}");
        let stash = StashResolved::ordinary(1, rel, 0x50);
        let mut drain = b
            .drain_committed(stash, 42, 0x2000, &[8, 9], false)
            .await
            .unwrap();
        let mut heaps = Vec::new();
        while let Some(batch) = drain.next_batch(64, usize::MAX, None).await.unwrap() {
            heaps.extend(
                batch
                    .cells()
                    .unwrap()
                    .iter()
                    .map(|h| (h.decoded.source_lsn, h.decoded.xid)),
            );
        }
        assert_eq!(heaps.len(), 301);
        assert!(heaps[..300].iter().all(|&(l, x)| l == 100 && x == 8));
        assert_eq!(heaps[300], (200, 9));
        assert!(b.drain_resident_peak() > 0, "fanout charged the gauge");
        drain.finish().await.unwrap();
        assert_eq!(b.drain_resident_bytes(), 0, "pending accounting released");
    }

    /// Batched drain must reproduce the serial merge order: spilled + in-mem
    /// across top/subxact by `source_lsn` ASC, events winning ties, trailing
    /// event after the last heap. `is_final` only on the last slice.
    #[tokio::test(flavor = "current_thread")]
    async fn drain_batches_merge_lsn_order_events_first() {
        let tmp = tempdir().unwrap();
        // 1 KiB budget: xid 1's 512-byte payloads spill, xid 2 stays in memory
        let mut b = XactBuffer::new(cfg(tmp.path().to_path_buf())).unwrap();
        for lsn in [100u64, 120, 140] {
            b.on_heap(heap_with_value(1, lsn, 512)).await.unwrap();
        }
        b.on_heap(heap_with_value(2, 110, 16)).await.unwrap();
        b.on_heap(heap_with_value(2, 130, 16)).await.unwrap();
        // Ties heap@120; event-first tie-break puts it before the heap
        b.on_schema_event(1, 120, dropped_event(7));
        // Trailing, no heap after it
        b.on_schema_event(2, 150, dropped_event(8));
        assert!(b.stats().spill_xacts_active >= 1, "xid 1 must spill");

        let mut drain = b
            .drain_committed(StashResolved::nothing_stashed(1), 42, 0x2000, &[2], false)
            .await
            .unwrap();
        assert!(drain.had_states());
        let mut order: Vec<String> = Vec::new();
        let mut finals = Vec::new();
        while let Some(batch) = drain.next_batch(2, usize::MAX, None).await.unwrap() {
            order.extend(flatten_batch(&batch));
            finals.push(batch.is_final);
            if batch.is_final {
                break;
            }
        }
        let h = |lsn: u64| format!("h{lsn}");
        assert_eq!(
            order,
            [
                h(100),
                h(110),
                dropped_label(7),
                h(120),
                h(130),
                h(140),
                dropped_label(8)
            ]
        );
        assert!(finals.pop().unwrap(), "last slice flags final");
        assert!(finals.iter().all(|f| !f), "earlier slices non-final");
        assert!(
            drain
                .next_batch(2, usize::MAX, None)
                .await
                .unwrap()
                .is_none()
        );
        drain.finish().await.unwrap();
        assert_eq!(b.stats().committed_xacts_total, 1);
        assert!(b.active_xids().is_empty());
    }

    /// Preserve chunk generations across slice boundaries
    #[tokio::test(flavor = "current_thread")]
    async fn drain_batch_chunk_generations_cover_cross_batch_referrer() {
        let tmp = tempdir().unwrap();
        let mut b = XactBuffer::new(cfg(tmp.path().to_path_buf())).unwrap();
        b.on_toast_chunk(chunk(55, 0, 100, b"ab"), 9).await.unwrap();
        b.on_toast_chunk(chunk(55, 1, 105, b"cd"), 9).await.unwrap();
        b.on_heap(heap_with_value(9, 110, 16)).await.unwrap();
        b.on_toast_delete(
            crate::xact::spill::ToastDelete {
                db_oid: 0,
                toast_relid: 16400,
                blkno: 7,
                offnum: 3,
                source_lsn: 200,
            },
            9,
        )
        .await
        .unwrap();
        b.on_heap(heap_with_value(9, 300, 16)).await.unwrap();

        let mut drain = b
            .drain_committed(StashResolved::nothing_stashed(9), 0, 0x1000, &[], true)
            .await
            .unwrap();
        let b1 = drain
            .next_batch(1, usize::MAX, None)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(b1.heaps.len(), 1);
        assert_eq!(b1.new_rows.len(), 2);
        assert_eq!(b1.new_rows[0].lsn, 100);
        assert_eq!(b1.new_rows[0].offnum, 1);
        assert_eq!(b1.new_rows[1].lsn, 105);
        assert!(!b1.new_rows[1].is_tombstone());
        assert!(b1.chunks.last().unwrap().contains_key(&(16400, 55)));
        // Mirror row refs materialize below threshold without a spool
        let row0 = b1.new_rows[0].materialize(b1.new_rows.spool()).unwrap();
        assert_eq!(&row0.chunk_data[..], b"ab");
        let b2 = drain
            .next_batch(1, usize::MAX, None)
            .await
            .unwrap()
            .unwrap();
        assert!(b2.is_final);
        assert_eq!(b2.new_rows.len(), 1);
        let t = &b2.new_rows[0];
        assert!(t.is_tombstone());
        assert_eq!((t.blkno, t.offnum, t.chunk_id, t.lsn), (7, 3, 0, 200));
        assert!(t.chunk_data.is_empty());
        let p = ToastPointer {
            va_rawsize: 8,
            va_extinfo: 4, // extsize = "abcd", no compression
            va_valueid: 55,
            va_toastrelid: 16400,
        };
        let v = b2.chunks.iter().find_map(|g| g.get(&(16400, 55))).unwrap();
        // Below threshold both bodies stay memory-resident in the tail
        assert_eq!((v.run_chunks, v.tail.len()), (0, 2));
        let spool = b2.chunks.iter().find_map(|g| g.spool());
        assert!(spool.is_none(), "no spool below threshold");
        assert_eq!(
            reassemble_value_ref(&p, spool, v).unwrap(),
            Reassembled::Bytes(b"abcd".to_vec())
        );
        drain.finish().await.unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn drain_batch_seals_row_cursors_at_events_and_truncates() {
        let tmp = tempdir().unwrap();
        let mut b = XactBuffer::new(cfg(tmp.path().to_path_buf())).unwrap();
        b.on_toast_chunk(chunk(55, 0, 100, b"ab"), 9).await.unwrap();
        let mut trunc = heap_with_value(9, 110, 16);
        trunc.decoded.op = HeapOp::Truncate;
        trunc.decoded.new = None;
        b.on_heap(trunc).await.unwrap();
        b.on_toast_chunk(chunk(56, 0, 120, b"cd"), 9).await.unwrap();
        b.on_schema_event(9, 130, dropped_event(7));
        b.on_toast_chunk(chunk(57, 0, 140, b"ef"), 9).await.unwrap();
        b.on_heap(heap_with_value(9, 150, 16)).await.unwrap();

        let mut drain = b
            .drain_committed(StashResolved::nothing_stashed(9), 0, 0x1000, &[], true)
            .await
            .unwrap();
        let batch = drain
            .next_batch(usize::MAX, usize::MAX, None)
            .await
            .unwrap()
            .unwrap();
        assert!(batch.is_final);
        assert_eq!(batch.new_rows.len(), 3);
        assert_eq!(batch.truncate_rows, [1]);
        assert_eq!(batch.ordered_events.len(), 1);
        let ev = &batch.ordered_events[0];
        assert_eq!((ev.heap_idx, ev.row_idx), (1, 2));
        let ops: Vec<HeapOp> = batch
            .cells()
            .unwrap()
            .iter()
            .map(|h| h.decoded.op)
            .collect();
        assert_eq!(ops, [HeapOp::Truncate, HeapOp::Insert]);
        drain.finish().await.unwrap();
    }

    fn spill_files(dir: &std::path::Path) -> Vec<String> {
        std::fs::read_dir(dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .filter(|n| n.starts_with("xid-"))
            .collect()
    }

    /// Spill files survive the whole batch pull (redecode-from-disk on an
    /// abandoned drain) and unlink only at `finish`.
    #[tokio::test(flavor = "current_thread")]
    async fn drain_spill_unlinks_only_at_finish() {
        let tmp = tempdir().unwrap();
        let mut b = XactBuffer::new(cfg(tmp.path().to_path_buf())).unwrap();
        for i in 0..8u64 {
            b.on_heap(heap_with_value(3, 100 + i, 512)).await.unwrap();
        }
        assert_eq!(spill_files(tmp.path()).len(), 1);
        let mut drain = b
            .drain_committed(StashResolved::nothing_stashed(3), 0, 0x5000, &[], false)
            .await
            .unwrap();
        assert_eq!(
            b.stats().spill_bytes_active,
            0,
            "drain owns the bytes once opened"
        );
        while let Some(batch) = drain.next_batch(2, usize::MAX, None).await.unwrap() {
            if batch.is_final {
                break;
            }
        }
        assert_eq!(
            spill_files(tmp.path()).len(),
            1,
            "file persists until finish"
        );
        assert!(drain.spilled(), "reorder finishes only a spilled drain");
        drain.finish().await.unwrap();
        assert!(spill_files(tmp.path()).is_empty());
    }

    /// Reorder drops an unspilled drain without `finish`, so a memory-only
    /// merge of top and subxact must leave nothing on disk
    #[tokio::test(flavor = "current_thread")]
    async fn unspilled_merge_leaves_no_files_on_drop() {
        let tmp = tempdir().unwrap();
        let mut b = XactBuffer::new(cfg(tmp.path().to_path_buf())).unwrap();
        b.on_heap(heap_with_value(3, 100, 16)).await.unwrap();
        b.on_heap(heap_with_value(4, 110, 16)).await.unwrap();
        let mut drain = b
            .drain_committed(StashResolved::nothing_stashed(3), 0, 0x5000, &[4], false)
            .await
            .unwrap();
        assert!(matches!(drain.merged, Some(Drained::Merged(_))));
        let batch = drain
            .next_batch(8, usize::MAX, None)
            .await
            .unwrap()
            .expect("one slice");
        assert_eq!(batch.cells().unwrap().len(), 2);
        assert!(!drain.spilled());
        drop(drain);
        assert!(spill_files(tmp.path()).is_empty());
    }

    /// Lone in-memory xact skips the merge only within the slice budget;
    /// past it, slicing and drain-resident gauge follow the merge
    #[tokio::test(flavor = "current_thread")]
    async fn lone_xact_over_slice_budget_merges() {
        let tmp = tempdir().unwrap();
        let mut b = XactBuffer::new(XactBufferConfig {
            drain_batch_rows: 4,
            ..XactBufferConfig::new(tmp.path().to_path_buf())
        })
        .unwrap();
        for i in 0..4 {
            b.on_heap(heap_with_value(5, 100 + i, 16)).await.unwrap();
        }
        let drain = b
            .drain_committed(StashResolved::nothing_stashed(5), 0, 0x6000, &[], false)
            .await
            .unwrap();
        assert!(matches!(drain.merged, Some(Drained::Heaps(_))));

        for i in 0..10 {
            b.on_heap(heap_with_value(6, 200 + i, 16)).await.unwrap();
        }
        let mut drain = b
            .drain_committed(StashResolved::nothing_stashed(6), 0, 0x7000, &[], false)
            .await
            .unwrap();
        assert!(matches!(drain.merged, Some(Drained::Merged(_))));
        assert!(b.drain_resident_bytes() > 0, "gauge holds unsliced heaps");
        let mut lsns = Vec::new();
        let mut finals = Vec::new();
        while let Some(batch) = drain.next_batch(4, usize::MAX, None).await.unwrap() {
            lsns.extend(batch.cells().unwrap().iter().map(|h| h.decoded.source_lsn));
            finals.push(batch.is_final);
            if batch.is_final {
                break;
            }
        }
        assert_eq!(lsns, (200..210).collect::<Vec<_>>());
        assert_eq!(finals, [false, false, true]);
        drain.finish().await.unwrap();
        assert_eq!(b.drain_resident_bytes(), 0);
    }

    /// The drain-resident gauge bounds what the merge holds: for a fully
    /// spilled xact, peak stays near merge-heads, far below the xact size.
    #[tokio::test(flavor = "current_thread")]
    async fn drain_resident_gauge_stays_bounded() {
        let tmp = tempdir().unwrap();
        let mut b = XactBuffer::new(cfg(tmp.path().to_path_buf())).unwrap();
        let n = 64u64;
        for i in 0..n {
            b.on_heap(heap_with_value(4, 100 + i, 512)).await.unwrap();
        }
        let total = n * 512;
        let mut drain = b
            .drain_committed(StashResolved::nothing_stashed(4), 0, 0x9000, &[], false)
            .await
            .unwrap();
        use SpillSide::Spilled;
        let merged = drain.merged.as_ref().unwrap();
        let Drained::Merged(m) = merged else { panic!() };
        let read_buffers = m
            .sources
            .iter()
            .map(|src| {
                let s = &src.spill;
                let Spilled { reader, .. } = s else { panic!() };
                reader.buffered_bytes() as u64
            })
            .sum::<u64>();
        let mut rows = 0usize;
        while let Some(batch) = drain.next_batch(4, usize::MAX, None).await.unwrap() {
            rows += batch.cells().unwrap().len();
            let resident = b.drain_resident_bytes();
            assert!(
                resident < read_buffers + total / 4,
                "resident {resident} vs xact {total}"
            );
            if batch.is_final {
                break;
            }
        }
        assert_eq!(rows as u64, n);
        assert!(b.drain_resident_peak() > 0, "gauge saw the merge heads");
        let peak = b.drain_resident_peak();
        assert!(
            peak < read_buffers + total / 4,
            "peak {peak} vs xact {total}"
        );
        drain.finish().await.unwrap();
        assert_eq!(b.drain_resident_bytes(), 0, "gauge drains with the drain");
    }

    /// Ownership accounting in the memory-threshold regime: sealed chunk
    /// generations and taken mirror rows stay gauged while any consumer
    /// holds them — container hand-off is not release. Below
    /// `toast_body_mem_max` resident chunk bytes scale with the xact's
    /// total TOAST bytes (plus ref metadata) until every batch drops.
    #[tokio::test(flavor = "current_thread")]
    async fn drain_resident_counts_generations_and_rows_until_drop() {
        let tmp = tempdir().unwrap();
        let mut b = XactBuffer::new(cfg(tmp.path().to_path_buf())).unwrap();
        let n = 16u32;
        let body = [7u8; 512];
        for i in 0..n {
            let lsn = 100 + 2 * u64::from(i);
            b.on_toast_chunk(chunk(50 + i, 0, lsn, &body), 9)
                .await
                .unwrap();
            b.on_heap(heap_with_value(9, lsn + 1, 16)).await.unwrap();
        }
        let total = u64::from(n) * (512 + CHUNK_REF_META as u64);
        let mut drain = b
            .drain_committed(StashResolved::nothing_stashed(9), 0, 0x9000, &[], true)
            .await
            .unwrap();
        let mut held: Vec<DrainedBatch> = Vec::new();
        while let Some(batch) = drain.next_batch(2, usize::MAX, None).await.unwrap() {
            let is_final = batch.is_final;
            held.push(batch);
            if is_final {
                break;
            }
        }
        assert!(
            held.last().unwrap().chunks.len() > 1,
            "slices sealed multiple generations"
        );
        assert_eq!(b.drain_chunk_resident_bytes(), total);
        // Rows share the generation's Mem bodies, gauging metadata only
        assert_eq!(
            b.drain_row_resident_bytes(),
            u64::from(n) * CHUNK_REF_META as u64
        );
        assert_eq!(b.toast_spool_bytes(), 0, "below threshold, no spool");
        drain.finish().await.unwrap();
        assert_eq!(
            b.drain_chunk_resident_bytes(),
            total,
            "finish releases spill files, not held generations"
        );
        held.clear();
        assert_eq!(b.drain_chunk_resident_bytes(), 0);
        assert_eq!(b.drain_row_resident_bytes(), 0);
        assert_eq!(b.drain_resident_bytes(), 0);
    }

    /// Sealed chunk generations admit through the caller, whose error ends
    /// the pull
    #[tokio::test(flavor = "current_thread")]
    async fn drain_admits_sealed_generations_through_caller() {
        let tmp = tempdir().unwrap();
        let mut b = XactBuffer::new(cfg(tmp.path().to_path_buf())).unwrap();
        let body = [7u8; 512];
        for i in 0..4u32 {
            let lsn = 100 + 2 * u64::from(i);
            b.on_toast_chunk(chunk(50 + i, 0, lsn, &body), 9)
                .await
                .unwrap();
            b.on_heap(heap_with_value(9, lsn + 1, 16)).await.unwrap();
        }
        let mut drain = b
            .drain_committed(StashResolved::nothing_stashed(9), 0, 0x9000, &[], true)
            .await
            .unwrap();
        let budget = crate::budget::MemoryBudget::new(1 << 20);
        let mut asked = Vec::new();
        let batch = drain
            .next_batch_admitting(2, usize::MAX, async |bytes| {
                asked.push(bytes);
                Ok::<_, XactBufferError>(Some(budget.admit(bytes).await))
            })
            .await
            .unwrap()
            .unwrap();
        assert_eq!(batch.chunks.len(), 1);
        assert_eq!(asked, [2 * (512 + CHUNK_REF_META)]);
        assert_eq!(budget.resident_bytes(), asked[0] as u64);
        let err = drain
            .next_batch_admitting(2, usize::MAX, async |_| {
                Err(SinkError::Other("pool closed".into()))
            })
            .await
            .err()
            .expect("admission error ignored");
        assert!(matches!(err, SinkError::Other(m) if m == "pool closed"));
    }

    fn toastbody_files(dir: &std::path::Path) -> Vec<String> {
        std::fs::read_dir(dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .filter(|n| n.starts_with("toastbody-"))
            .collect()
    }

    /// Flip of the retention test: past `toast_body_mem_max` bodies spool
    /// to disk, so resident chunk bytes stay ≤ threshold + ref metadata
    /// even while every batch of a multi-generation drain is held. Mixed
    /// Mem/File generations resolve; spool unlinks at finish with held
    /// readers surviving via fd.
    #[tokio::test(flavor = "current_thread")]
    async fn drain_spools_bodies_past_mem_threshold() {
        let tmp = tempdir().unwrap();
        let mut c = cfg(tmp.path().to_path_buf());
        c.toast_body_mem_max = 1024;
        let mut b = XactBuffer::new(c).unwrap();
        let n = 8u32;
        let body = [7u8; 512];
        for i in 0..n {
            let lsn = 100 + 2 * u64::from(i);
            b.on_toast_chunk(chunk(50 + i, 0, lsn, &body), 9)
                .await
                .unwrap();
            b.on_heap(heap_with_value(9, lsn + 1, 16)).await.unwrap();
        }
        let mut drain = b
            .drain_committed(StashResolved::nothing_stashed(9), 0, 0x9000, &[], true)
            .await
            .unwrap();
        let mut held: Vec<DrainedBatch> = Vec::new();
        while let Some(batch) = drain.next_batch(2, usize::MAX, None).await.unwrap() {
            let is_final = batch.is_final;
            held.push(batch);
            if is_final {
                break;
            }
        }
        // First two bodies fill the 1024-byte budget, rest hit disk
        let meta = u64::from(n) * CHUNK_REF_META as u64;
        assert_eq!(b.drain_chunk_resident_bytes(), 1024 + meta);
        assert_eq!(b.drain_row_resident_bytes(), meta);
        assert_eq!(b.toast_spool_bytes(), u64::from(n - 2) * 512);
        assert_eq!(toastbody_files(tmp.path()).len(), 1);
        let last = held.last().unwrap();
        let spool = last.chunks.iter().find_map(|g| g.spool());
        assert!(spool.is_some(), "post-threshold generations carry spool");
        let p = |value_id| ToastPointer {
            va_rawsize: 516,
            va_extinfo: 512, // uncompressed
            va_valueid: value_id,
            va_toastrelid: 16400,
        };
        // Mem value (gen 0) and File value (late gen) both resolve; File
        // run held compact (whole value one contiguous range)
        let mem_v = last
            .chunks
            .iter()
            .find_map(|g| g.get(&(16400, 50)))
            .unwrap();
        assert_eq!((mem_v.run_chunks, mem_v.tail.len()), (0, 1));
        let whole = Reassembled::Bytes(body.to_vec());
        assert_eq!(reassemble_value_ref(&p(50), spool, mem_v).unwrap(), whole);
        let file_v = last
            .chunks
            .iter()
            .find_map(|g| g.get(&(16400, 50 + n - 1)))
            .unwrap();
        assert_eq!(
            (file_v.run_chunks, file_v.run.len, file_v.tail.len()),
            (1, 512, 0)
        );
        assert_eq!(
            reassemble_value_ref(&p(50 + n - 1), spool, file_v).unwrap(),
            whole
        );
        let mut past_end = file_v.clone();
        past_end.run.offset += 1 << 20;
        let e = reassemble_value_ref(&p(50 + n - 1), spool, &past_end).unwrap_err();
        assert!(e.to_string().contains("body spool read: "), "{e}");
        // Mirror row refs materialize from the same spool
        let rows = &held.last().unwrap().new_rows;
        for r in rows.iter() {
            assert_eq!(r.materialize(rows.spool()).unwrap().chunk_data.len(), 512);
        }
        assert!(drain.spilled());
        drain.finish().await.unwrap();
        assert_eq!(
            b.toast_spool_bytes(),
            u64::from(n - 2) * 512,
            "held readers pin unlinked disk bytes in gauge + quota"
        );
        assert!(
            toastbody_files(tmp.path()).is_empty(),
            "finish unlinks spool"
        );
        // Held readers survive unlink via open fd
        let spool = last.chunks.iter().find_map(|g| g.spool());
        assert_eq!(
            reassemble_value_ref(&p(50 + n - 1), spool, file_v).unwrap(),
            whole
        );
        held.clear();
        assert_eq!(b.drain_chunk_resident_bytes(), 0);
        assert_eq!(b.drain_row_resident_bytes(), 0);
        assert_eq!(b.drain_resident_bytes(), 0);
        assert_eq!(b.toast_spool_bytes(), 0, "gauge drops with the last reader");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn drain_committed_unknown_xid_yields_no_batches() {
        let tmp = tempdir().unwrap();
        let mut b = XactBuffer::new(cfg(tmp.path().to_path_buf())).unwrap();
        let mut drain = b
            .drain_committed(StashResolved::nothing_stashed(999), 0, 0x100, &[], false)
            .await
            .unwrap();
        assert!(!drain.had_states());
        assert!(drain.next_batch(10, 1000, None).await.unwrap().is_none());
        drain.finish().await.unwrap();
        assert_eq!(b.stats().commits_unknown_xid, 1);
        assert_eq!(b.stats().drain_lsn, 0x100);
    }

    /// Decoder skips the stash lock while the lock-free count reads zero, so
    /// every commit, abort and subxact abort must give back what it marked
    #[tokio::test(flavor = "current_thread")]
    async fn stash_live_balances_across_commit_and_aborts() {
        let tmp = tempdir().unwrap();
        let mut b = XactBuffer::new(cfg(tmp.path().to_path_buf())).unwrap();
        let live = b.stash_live();
        let rfn = |rel_node| RelFileNode {
            spc_node: 1663,
            db_node: 5,
            rel_node,
        };
        let rel = int4_descriptor(16470);
        let rel_rfn = rel.rfn;
        b.stash_raw(1, multi_insert_raw(1, 100, 16470, &[1]))
            .await
            .unwrap();
        b.stash_raw(1, multi_insert_raw(1, 110, 16470, &[2]))
            .await
            .unwrap();
        b.track_unresolvable(1, 120, rfn(16471));
        assert_eq!(
            live.load(Ordering::Relaxed),
            2,
            "repeat filenode counts once"
        );
        b.track_unresolvable(2, 130, rfn(16472));
        b.track_unresolvable(3, 140, rfn(16473));
        b.track_unresolvable(4, 150, rfn(16474));
        b.track_unresolvable(4, 160, rfn(16475));
        assert_eq!(live.load(Ordering::Relaxed), 6);

        b.abort(4, Pos::new(0x200), &[]).await.unwrap();
        assert_eq!(live.load(Ordering::Relaxed), 4, "subxact abort");
        b.abort(2, Pos::new(0x210), &[]).await.unwrap();
        assert_eq!(live.load(Ordering::Relaxed), 3, "top abort");

        let mut drain = b
            .drain_committed(StashResolved::ordinary(1, rel, 0x50), 0, 0x300, &[], false)
            .await
            .unwrap();
        let batch = drain
            .next_batch(8, usize::MAX, None)
            .await
            .unwrap()
            .expect("stashed rows drain");
        assert_eq!(batch.cells().unwrap().len(), 2);
        drain.finish().await.unwrap();
        assert_eq!(live.load(Ordering::Relaxed), 1, "commit");

        let stash = StashResolved {
            top_xid: 3,
            res: Some(StashResolution::default()),
        };
        b.drain_committed(stash, 0, 0x310, &[], false)
            .await
            .unwrap()
            .finish()
            .await
            .unwrap();
        assert_eq!(live.load(Ordering::Relaxed), 0);
        assert!(!b.is_stash_candidate(1, rel_rfn));
    }

    #[test]
    fn recent_toast_births_keep_duplicate_until_last_entry_evicted() {
        let mut born = RecentToastBirths::default();
        born.insert((1, 1), 7);
        born.insert((1, 1), 8);
        for id in 2..=1024 {
            born.insert((1, id), 8);
        }
        assert!(born.born_in((1, 1), 8));
        assert!(!born.born_in((1, 1), 7), "newest birth xid decides");
        assert_eq!(born.order.len(), 1024);
        born.insert((2, 1), 8);
        assert!(!born.counts.contains_key(&(1, 1)));
        assert!(born.counts.contains_key(&(2, 1)));
        assert_eq!(born.counts.len(), 1024);
    }

    /// Savepoint's value stays own for its top and siblings once WAL links
    /// them, unlinked xids keep exact match
    #[test]
    fn recent_toast_births_follow_subxact_links() {
        let mut born = RecentToastBirths::default();
        born.insert((1, 1), 8);
        assert!(!born.born_in((1, 1), 9), "unlinked sibling");
        born.link(8, 7);
        born.link(9, 7);
        born.link(9, 7);
        assert!(born.born_in((1, 1), 9) && born.born_in((1, 1), 7));
        assert!(!born.born_in((1, 1), 10), "other transaction");
        for sub in 100..100 + 1023 {
            born.link(sub, 50);
        }
        assert_eq!(born.tops_order.len(), 1024);
        assert!(!born.born_in((1, 1), 9), "link left window");
        assert_eq!(born.tops.len(), 1024);
    }

    /// Ordinary rows buffer as WAL bytes and decode at commit against the
    /// descriptor version covering them at record time
    #[tokio::test(flavor = "current_thread")]
    async fn ordinary_records_buffer_undecoded() {
        use crate::record::Route;

        let log_dir = tempdir().unwrap();
        let rel = int4_descriptor(16480);
        let log = single_rel_log(log_dir.path(), &rel).await;
        let spill_dir = tempdir().unwrap();
        let buffer = Arc::new(Mutex::new(
            XactBuffer::new(cfg(spill_dir.path().to_path_buf())).unwrap(),
        ));
        let mut sink = BufferingDecoderSink::new(DescriptorLogs::single(log), buffer.clone());
        let raws: Vec<RawRecord> = (1..=3)
            .map(|v| multi_insert_raw(1, 0x200 + v as u64, 16480, &[v, v * 10]))
            .collect();
        let batch: Vec<Record<'_>> = raws
            .iter()
            .map(|raw| Record {
                parsed: raw.to_xlog_record(),
                source_lsn: raw.source_lsn,
                route: Route::ToDecoder,
                ..Default::default()
            })
            .collect();
        let prescan = sink.prescan(ToastResolver::disabled());
        prescan(&batch[0]);
        let mut truncate = raws[0].to_xlog_record();
        truncate.header.resource_manager_id = RmId::Heap as u8;
        truncate.header.info = crate::decode::heap_decoder::XLOG_HEAP_TRUNCATE;
        prescan(&Record {
            parsed: truncate,
            route: Route::ToDecoder,
            ..Default::default()
        });
        let mut linked = raws[0].to_xlog_record();
        linked.header.xact_id = 2;
        linked.toplevel_xid = 1;
        prescan(&Record {
            parsed: linked,
            route: Route::ToDecoder,
            ..Default::default()
        });
        let mut assignment = walrus::pg::walparser::XLogRecord::default();
        assignment.header.resource_manager_id = RmId::Xact as u8;
        assignment.header.info = XLOG_XACT_ASSIGNMENT;
        let md = [1u32.to_le_bytes(), 1i32.to_le_bytes(), 3u32.to_le_bytes()].concat();
        assignment.main_data = md.into();
        prescan(&Record {
            parsed: assignment,
            ..Default::default()
        });
        let uncovered_raw = multi_insert_raw(1, 0x201, 16999, &[1]);
        prescan(&Record {
            parsed: uncovered_raw.to_xlog_record(),
            source_lsn: uncovered_raw.source_lsn,
            route: Route::ToDecoder,
            ..Default::default()
        });
        sink.on_records(&batch).await.unwrap();
        assert_eq!(sink.stats().inserts.load(Ordering::Relaxed), 6);
        {
            let buf = buffer.lock().await;
            let st = &buf.inflight[&1];
            assert!(
                st.in_mem
                    .iter()
                    .all(|e| matches!(e, SpillEntry::Input(i) if i.input.tuples == 2))
            );
            assert_eq!(buf.inflight_snapshot()[0].heap_count, 6);
        }

        let mut drain = buffer
            .lock()
            .await
            .drain_committed(StashResolved::nothing_stashed(1), 0, 0x300, &[], false)
            .await
            .unwrap();
        let out = drain
            .next_batch(8, usize::MAX, None)
            .await
            .unwrap()
            .expect("one slice");
        let values: Vec<_> = out
            .cells()
            .unwrap()
            .iter()
            .map(|h| h.decoded.new.as_ref().unwrap().columns[0].clone())
            .collect();
        let int4 = |v| Some(build::int4(v));
        assert_eq!(
            values,
            [int4(1), int4(10), int4(2), int4(20), int4(3), int4(30)]
        );
        assert!(
            out.cells()
                .unwrap()
                .iter()
                .all(|h| Arc::ptr_eq(&h.descriptor, &out.cells().unwrap()[0].descriptor))
        );
        drain.finish().await.unwrap();
    }

    fn rfn_of(rel_node: u32) -> RelFileNode {
        RelFileNode {
            spc_node: 1663,
            db_node: 5,
            rel_node,
        }
    }

    fn heap_record(
        rm: RmId,
        info: u8,
        xid: u32,
        rel_node: u32,
        main_data: Vec<u8>,
        data: Vec<u8>,
    ) -> Record<'static> {
        use crate::record::Route;
        use walrus::pg::walparser::{
            BlockLocation, XLogRecord, XLogRecordBlock, XLogRecordBlockHeader, XLogRecordHeader,
        };
        Record {
            parsed: XLogRecord {
                header: XLogRecordHeader {
                    resource_manager_id: rm as u8,
                    info,
                    xact_id: xid,
                    ..Default::default()
                },
                blocks: vec![XLogRecordBlock {
                    header: XLogRecordBlockHeader {
                        location: BlockLocation {
                            rel: rfn_of(rel_node),
                            block_no: 3,
                        },
                        ..Default::default()
                    },
                    data: std::borrow::Cow::Owned(data),
                    ..Default::default()
                }],
                main_data: std::borrow::Cow::Owned(main_data),
                ..Default::default()
            },
            source_lsn: 0x200,
            route: Route::ToDecoder,
            ..Default::default()
        }
    }

    fn smgr_create(xid: u32, rel_node: u32, fork: i32) -> Record<'static> {
        use walrus::pg::walparser::{XLogRecord, XLogRecordHeader};
        let mut md = Vec::new();
        for v in [1663u32, 5, rel_node] {
            md.extend_from_slice(&v.to_le_bytes());
        }
        md.extend_from_slice(&fork.to_le_bytes());
        Record {
            parsed: XLogRecord {
                header: XLogRecordHeader {
                    resource_manager_id: RmId::Smgr as u8,
                    info: crate::filter::main_data::XLOG_SMGR_CREATE,
                    xact_id: xid,
                    ..Default::default()
                },
                main_data: std::borrow::Cow::Owned(md),
                ..Default::default()
            },
            source_lsn: 0x180,
            ..Default::default()
        }
    }

    /// Encode `xl_heap_header`, alignment padding, and one int4
    fn int4_tuple(v: i32) -> Vec<u8> {
        let mut t = vec![1, 0, 0, 0, 24, 0];
        t.extend_from_slice(&v.to_le_bytes());
        t
    }

    /// Encode `xl_heap_header`, null bitmap, and TOAST chunk columns
    fn chunk_tuple(value_id: u32, seq: i32, body: Option<&[u8]>) -> Vec<u8> {
        use crate::decode::heap_decoder::{HEAP_HASNULL, short_varlena};
        let infomask = if body.is_some() { 0 } else { HEAP_HASNULL };
        let mut t = 3u16.to_le_bytes().to_vec();
        t.extend_from_slice(&infomask.to_le_bytes());
        t.extend_from_slice(&[24, 0b011]);
        t.extend_from_slice(&value_id.to_le_bytes());
        t.extend_from_slice(&seq.to_le_bytes());
        if let Some(body) = body {
            t.extend_from_slice(&short_varlena(body));
        }
        t
    }

    /// Use INIT_PAGE to omit MULTI_INSERT's offsets array
    fn multi_insert_parts(flags: u8, tuples: &[Vec<u8>]) -> (u8, Vec<u8>, Vec<u8>) {
        use crate::decode::heap_decoder::{XLOG_HEAP_INIT_PAGE, XLOG_HEAP2_MULTI_INSERT};
        let mut md = vec![flags, 0];
        md.extend_from_slice(&(tuples.len() as u16).to_le_bytes());
        let mut data = Vec::new();
        for t in tuples {
            data.resize(data.len().next_multiple_of(2), 0);
            let body = &t[5..];
            data.extend_from_slice(&(body.len() as u16).to_le_bytes());
            data.extend_from_slice(&t[..5]);
            data.extend_from_slice(body);
        }
        (XLOG_HEAP2_MULTI_INSERT | XLOG_HEAP_INIT_PAGE, md, data)
    }

    async fn open_log(
        dir: &std::path::Path,
        batches: Vec<crate::catalog::desc_log::BatchRecord>,
    ) -> Arc<DescriptorLog> {
        use crate::catalog::desc_log::DescLogIdentity;
        let log = Arc::new(
            DescriptorLog::open(
                dir,
                DescLogIdentity {
                    pg_major: 17,
                    system_id: "7".into(),
                    timeline: 1,
                    db_oid: 5,
                    wal_seg_size: 16 * 1024 * 1024,
                },
            )
            .await
            .unwrap(),
        );
        for batch in batches {
            log.append_batch(batch).await.unwrap();
        }
        log
    }

    fn log_entry(
        valid_from: u64,
        rel: &Arc<RelDescriptor>,
        value: crate::catalog::desc_log::LogValue,
    ) -> Arc<crate::catalog::desc_log::LogEntry> {
        Arc::new(crate::catalog::desc_log::LogEntry {
            valid_from,
            oid: rel.oid,
            rfn: rel.rfn,
            value,
        })
    }

    fn skip_count(sink: &BufferingDecoderSink, rel_node: u32, reason: SkipReason) -> u64 {
        sink.stats()
            .skipped_by_rel
            .snapshot()
            .into_iter()
            .filter(|(k, _)| k.rel_node == rel_node && k.reason == reason)
            .map(|(_, n)| n)
            .sum()
    }

    #[tokio::test(flavor = "current_thread")]
    async fn sink_admits_stash_by_lookup_outcome() {
        use crate::catalog::desc_log::{
            Ambiguity, AmbiguityReason, AmbiguityScope, BatchRecord, LogValue,
        };
        use crate::decode::heap_decoder::{XLH_INSERT_CONTAINS_NEW_TUPLE, XLOG_HEAP_INSERT};
        let (live, dropped, retired) = (16510, 16511, 16512);
        let (fenced_bare, fenced_marked, created, unknown) = (16513, 16514, 16515, 16516);
        let rels = [live, dropped, retired].map(int4_descriptor);
        let batch = |captured_at, entries, ambiguities| BatchRecord {
            captured_at,
            commit_lsn: captured_at - 0x10,
            observations: Vec::new(),
            ambiguities,
            entries,
        };
        let fence = |rel_node| {
            Arc::new(Ambiguity {
                scope: AmbiguityScope::Rfn(rfn_of(rel_node)),
                from_lsn: 0x150,
                through_lsn: 0x300,
                reason: AmbiguityReason::UnknownMutationPosition,
            })
        };
        let log_dir = tempdir().unwrap();
        let log = open_log(
            log_dir.path(),
            vec![
                batch(
                    0x100,
                    rels.iter()
                        .map(|r| log_entry(0x100, r, LogValue::Present(r.clone())))
                        .collect(),
                    Vec::new(),
                ),
                batch(
                    0x140,
                    vec![
                        log_entry(0x140, &rels[1], LogValue::Dropped),
                        log_entry(0x140, &rels[2], LogValue::Retired),
                    ],
                    Vec::new(),
                ),
                batch(
                    0x300,
                    Vec::new(),
                    vec![fence(fenced_bare), fence(fenced_marked)],
                ),
            ],
        )
        .await;
        let spill_dir = tempdir().unwrap();
        let buffer = Arc::new(Mutex::new(
            XactBuffer::new(cfg(spill_dir.path().to_path_buf())).unwrap(),
        ));
        let mut sink = BufferingDecoderSink::new(DescriptorLogs::single(log), buffer.clone());
        sink.stats().attribute_skips.store(true, Ordering::Relaxed);
        let insert = |rel_node| {
            heap_record(
                RmId::Heap,
                XLOG_HEAP_INSERT,
                7,
                rel_node,
                vec![1, 0, 0],
                int4_tuple(1),
            )
        };

        sink.on_record(&smgr_create(7, created, 0)).await.unwrap();
        sink.on_record(&smgr_create(0, fenced_marked, 0))
            .await
            .unwrap();
        sink.on_record(&smgr_create(7, unknown, 1)).await.unwrap();
        {
            let buf = buffer.lock().await;
            assert_eq!(buf.marker_lsn(rfn_of(created)), Some(0x180));
            assert_eq!(
                buf.marker_lsn(rfn_of(unknown)),
                None,
                "fsm fork marks nothing"
            );
            assert!(buf.is_stash_candidate(7, rfn_of(created)));
            assert!(!buf.is_stash_candidate(7, rfn_of(fenced_marked)));
        }

        for rel_node in [created, fenced_marked, unknown, dropped, retired] {
            sink.on_record(&insert(rel_node)).await.unwrap();
        }
        let err = sink.on_record(&insert(fenced_bare)).await.unwrap_err();
        assert!(
            err.to_string().contains("inside ambiguity [0x150, 0x300)"),
            "{err}"
        );
        let stats = sink.stats_handle();
        assert_eq!(stats.toast_stash_buffered.load(Ordering::Relaxed), 2);
        assert_eq!(
            stats.raw_stash_marker_ops.load()[0],
            2,
            "both stashes are inserts"
        );
        assert_eq!(skip_count(&sink, unknown, SkipReason::Unresolvable), 1);
        assert_eq!(skip_count(&sink, dropped, SkipReason::Unresolvable), 1);
        assert_eq!(skip_count(&sink, retired, SkipReason::Retired), 1);
        {
            let buf = buffer.lock().await;
            for rel_node in [created, fenced_marked, unknown, dropped] {
                assert!(buf.is_stash_candidate(7, rfn_of(rel_node)), "{rel_node}");
            }
            assert!(!buf.is_stash_candidate(7, rfn_of(retired)));
            let marks: HashMap<_, _> = buf.stash_candidates(&[7]).into_iter().collect();
            assert!(!marks[&rfn_of(created)].payload_free);
            assert!(!marks[&rfn_of(fenced_marked)].payload_free);
            assert!(marks[&rfn_of(unknown)].payload_free);
            assert!(marks[&rfn_of(dropped)].payload_free);
        }

        let mut blockless = insert(live);
        blockless.parsed.blocks.clear();
        sink.on_record(&blockless).await.unwrap();
        assert_eq!(stats.skipped_no_block.load(Ordering::Relaxed), 1);

        let (info, md, data) = multi_insert_parts(0, &[int4_tuple(1)]);
        sink.on_record(&heap_record(RmId::Heap2, info, 7, live, md, data))
            .await
            .unwrap();
        assert_eq!(skip_count(&sink, live, SkipReason::NoTuple), 1);
        // Sink reads headers only, tuple bytes are checked at decode
        let (info, md, data) = multi_insert_parts(XLH_INSERT_CONTAINS_NEW_TUPLE, &[int4_tuple(1)]);
        let err = sink
            .on_record(&heap_record(
                RmId::Heap2,
                info,
                7,
                live,
                md[..2].to_vec(),
                data,
            ))
            .await
            .unwrap_err();
        assert!(err.to_string().contains("truncated"), "{err}");
        assert_eq!(stats.decoded.load(Ordering::Relaxed), 0);
    }

    /// Sampled txn decodes under its decode parent span; config-table
    /// tuples the overlay cannot interpret buffer nothing
    #[tokio::test(flavor = "current_thread")]
    async fn sink_decodes_under_span_and_drops_uninterpretable_config() {
        use crate::catalog::desc_log::{BatchRecord, LogValue};
        use crate::decode::heap_decoder::XLOG_HEAP_INSERT;
        let _g = crate::ops::trace::SAMPLE_TEST_GUARD.lock().await;
        crate::ops::trace::set_sample_ratio(1.0);
        let live = int4_descriptor(16530);
        let mut config = (*int4_descriptor(16531)).clone();
        config.rel_name =
            crate::schema::RelName::new("walshadow", crate::runtime_config::CONFIG_NAMESPACE);
        let config = Arc::new(config);
        let entries = [&live, &config]
            .map(|r| log_entry(0x100, r, LogValue::Present(r.clone())))
            .to_vec();
        let batch = BatchRecord {
            captured_at: 0x100,
            commit_lsn: 0xF0,
            observations: Vec::new(),
            ambiguities: Vec::new(),
            entries,
        };
        let log_dir = tempdir().unwrap();
        let log = open_log(log_dir.path(), vec![batch]).await;
        let spill_dir = tempdir().unwrap();
        let buffer = Arc::new(Mutex::new(
            XactBuffer::new(cfg(spill_dir.path().to_path_buf())).unwrap(),
        ));
        let reg = TxnSpanRegistry::new();
        reg.open(7, 0x200);
        reg.note_shipped(7);
        assert!(reg.note_popped(7));
        let mut sink = BufferingDecoderSink::new(DescriptorLogs::single(log), buffer.clone())
            .with_span_registry(reg)
            .with_config_schema("walshadow".into());
        let insert = |rel_node| {
            heap_record(
                RmId::Heap,
                XLOG_HEAP_INSERT,
                7,
                rel_node,
                vec![1, 0, 0],
                int4_tuple(1),
            )
        };
        sink.on_record(&insert(16531)).await.unwrap();
        assert!(buffer.lock().await.inflight_snapshot().is_empty());
        sink.on_record(&insert(16530)).await.unwrap();
        let snap = buffer.lock().await.inflight_snapshot();
        let [e] = &snap[..] else { panic!() };
        assert_eq!((e.xid, e.heap_count), (7, 1));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn sink_toast_heap_odd_shapes_count() {
        use crate::catalog::desc_log::{BatchRecord, LogValue};
        use crate::decode::heap_decoder::{
            SIZE_OF_HEAP_UPDATE, XLH_INSERT_CONTAINS_NEW_TUPLE, XLOG_HEAP_DELETE, XLOG_HEAP_INSERT,
            XLOG_HEAP_UPDATE,
        };
        let toast = Arc::new(crate::backfill::backup_page_walk::toast_chunk_rel());
        let node = toast.rfn.rel_node;
        let log_dir = tempdir().unwrap();
        let log = open_log(
            log_dir.path(),
            vec![BatchRecord {
                captured_at: 0x100,
                commit_lsn: 0xF0,
                observations: Vec::new(),
                ambiguities: Vec::new(),
                entries: vec![log_entry(0x100, &toast, LogValue::Present(toast.clone()))],
            }],
        )
        .await;
        let spill_dir = tempdir().unwrap();
        let buffer = Arc::new(Mutex::new(
            XactBuffer::new(cfg(spill_dir.path().to_path_buf())).unwrap(),
        ));
        let mut sink = BufferingDecoderSink::new(DescriptorLogs::single(log), buffer.clone());
        let stats = sink.stats_handle();
        let load = |c: &AtomicU64| c.load(Ordering::Relaxed);

        let (info, md, data) = multi_insert_parts(
            XLH_INSERT_CONTAINS_NEW_TUPLE,
            &[
                chunk_tuple(900, 0, Some(b"ab")),
                chunk_tuple(900, 1, Some(b"cd")),
            ],
        );
        sink.on_record(&heap_record(RmId::Heap2, info, 7, node, md, data))
            .await
            .unwrap();
        assert_eq!(load(&stats.toast_chunks_buffered), 2);
        assert_eq!(
            load(&stats.toast_chunks_malformed),
            2,
            "no TID, not mirrored"
        );

        let update = heap_record(
            RmId::Heap,
            XLOG_HEAP_UPDATE,
            7,
            node,
            vec![0; SIZE_OF_HEAP_UPDATE],
            chunk_tuple(900, 0, Some(b"zz")),
        );
        sink.on_record(&update).await.unwrap();
        assert_eq!(load(&stats.toast_chunk_deletes), 1);

        let null_chunk = heap_record(
            RmId::Heap,
            XLOG_HEAP_INSERT,
            7,
            node,
            vec![1, 0, 0],
            chunk_tuple(901, 0, None),
        );
        sink.on_record(&null_chunk).await.unwrap();
        assert_eq!(load(&stats.toast_chunks_malformed), 3);
        assert_eq!(load(&stats.toast_chunks_buffered), 2);

        let delete = heap_record(
            RmId::Heap,
            XLOG_HEAP_DELETE,
            7,
            node,
            vec![0, 0, 0, 0, 4, 0, 0, 0],
            Vec::new(),
        );
        sink.on_record(&delete).await.unwrap();
        assert_eq!(load(&stats.toast_chunk_deletes), 2);
        assert_eq!(load(&stats.decoded), 5);

        let snap = buffer.lock().await.inflight_snapshot();
        let [e] = snap.as_slice() else { panic!() };
        assert_eq!(
            (
                e.xid,
                e.heap_count,
                e.chunk_count,
                e.last_lsn,
                e.rels.as_str()
            ),
            (7, 0, 3, 0x200, "0/16401")
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn truncate_skips_unparsed_missing_and_toast() {
        use crate::catalog::desc_log::{BatchRecord, LogValue};
        use crate::decode::heap_decoder::XLOG_HEAP_TRUNCATE;
        use walrus::pg::walparser::{XLogRecord, XLogRecordHeader};
        let toast = Arc::new(crate::backfill::backup_page_walk::toast_chunk_rel());
        let plain = int4_descriptor(16520);
        let log_dir = tempdir().unwrap();
        let log = open_log(
            log_dir.path(),
            vec![BatchRecord {
                captured_at: 0x100,
                commit_lsn: 0xF0,
                observations: Vec::new(),
                ambiguities: Vec::new(),
                entries: [&toast, &plain]
                    .into_iter()
                    .map(|r| log_entry(0x100, r, LogValue::Present(r.clone())))
                    .collect(),
            }],
        )
        .await;
        let truncate = |md: Vec<u8>| Record {
            parsed: XLogRecord {
                header: XLogRecordHeader {
                    resource_manager_id: RmId::Heap as u8,
                    info: XLOG_HEAP_TRUNCATE,
                    xact_id: 7,
                    ..Default::default()
                },
                main_data: std::borrow::Cow::Owned(md),
                ..Default::default()
            },
            source_lsn: 0x200,
            ..Default::default()
        };
        let spill_dir = tempdir().unwrap();
        let buffer = Arc::new(Mutex::new(
            XactBuffer::new(cfg(spill_dir.path().to_path_buf())).unwrap(),
        ));
        let mut sink = BufferingDecoderSink::new(DescriptorLogs::single(log), buffer);
        sink.stats().attribute_skips.store(true, Ordering::Relaxed);

        sink.on_record(&truncate(vec![5, 0])).await.unwrap();
        assert_eq!(skip_count(&sink, 0, SkipReason::TruncateUnparsed), 1);

        let relids = [16599, toast.oid, plain.oid];
        let mut md = 5u32.to_le_bytes().to_vec();
        md.extend_from_slice(&(relids.len() as u32).to_le_bytes());
        md.extend_from_slice(&[0u8; 4]);
        for relid in relids {
            md.extend_from_slice(&relid.to_le_bytes());
        }
        sink.on_record(&truncate(md)).await.unwrap();
        assert_eq!(skip_count(&sink, 16599, SkipReason::TruncateRelMissing), 1);
        assert_eq!(
            sink.stats().truncates.load(Ordering::Relaxed),
            1,
            "only the plain heap propagates"
        );
    }

    fn toast_raw(
        rm: RmId,
        info: u8,
        main_data: Vec<u8>,
        block: crate::xact::spill::RawBlock,
    ) -> RawRecord {
        RawRecord {
            xid: 7,
            rm: rm as u8,
            info,
            source_lsn: 0x200,
            page_magic: 0xD114,
            main_data,
            blocks: vec![block],
        }
    }

    fn toast_block(fork_flags: u8, data: Vec<u8>, image: Vec<u8>) -> crate::xact::spill::RawBlock {
        crate::xact::spill::RawBlock {
            block_id: 0,
            fork_flags,
            data_length: data.len() as u16,
            image_length: image.len() as u16,
            hole_offset: 0,
            hole_length: 0,
            bimg_info: 0,
            spc_node: 1663,
            db_node: 5,
            rel_node: 16401,
            block_no: 9,
            image,
            data,
        }
    }

    fn chunk_of(op: &StashedToastOp) -> &ToastChunk {
        use StashedToastOp::Chunk;
        let Chunk(c) = op else { panic!() };
        c
    }

    /// PostgreSQL rewrites can store inserted tuples solely in full-page images
    #[test]
    fn stashed_toast_image_insert_reads_page_tuple() {
        use crate::backfill::backup_page_walk::{
            make_rel, synth_toast_chunk_page, toast_chunk_rel,
        };
        use crate::decode::heap_decoder::XLOG_HEAP_INSERT;
        const HAS_IMAGE: u8 = 0x10;
        let rel = Arc::new(toast_chunk_rel());
        let page = synth_toast_chunk_page(&[(900, 0, b"ab"), (900, 1, b"cdef")], 0x100);
        let image_insert = |offnum: u16, image: Vec<u8>| {
            let mut md = offnum.to_le_bytes().to_vec();
            md.push(0);
            toast_raw(
                RmId::Heap,
                XLOG_HEAP_INSERT,
                md,
                toast_block(HAS_IMAGE, Vec::new(), image),
            )
        };

        let ops = decode_stashed_toast(&image_insert(2, page.to_vec()), &rel).unwrap();
        assert_eq!(ops.len(), 1);
        let c = chunk_of(&ops[0]);
        assert_eq!(
            (
                c.toast_relid,
                c.value_id,
                c.chunk_seq,
                c.blkno,
                c.offnum,
                c.source_lsn
            ),
            (16401, 900, 1, 9, 2, 0x200)
        );
        assert_eq!(&c.chunk_data[..], b"cdef");

        let plain = Arc::new(make_rel());
        assert!(
            decode_stashed_toast(&image_insert(1, page.to_vec()), &plain)
                .unwrap()
                .is_empty()
        );

        let mut bad_hoff = page;
        let lp = u32::from_le_bytes(bad_hoff[24..28].try_into().unwrap());
        bad_hoff[(lp & 0x7FFF) as usize + 22] = 8;
        let mut no_offnum = image_insert(1, page.to_vec());
        no_offnum.main_data.clear();
        let cases = [
            (no_offnum, "lacks offnum"),
            (image_insert(1, vec![0; 100]), "FPI restore"),
            (
                image_insert(3, page.to_vec()),
                "no LP_NORMAL tuple at (9,3)",
            ),
            (
                image_insert(1, bad_hoff.to_vec()),
                "malformed tuple at (9,1)",
            ),
        ];
        for (raw, want) in cases {
            let err = decode_stashed_toast(&raw, &rel).err().expect(want);
            assert!(matches!(err, XactBufferError::Detoast(_)), "{err}");
            assert!(err.to_string().contains(want), "{want}: {err}");
        }
    }

    #[test]
    fn stashed_toast_data_records_by_op() {
        use crate::backfill::backup_page_walk::{make_rel, toast_chunk_rel};
        use crate::decode::heap_decoder::{
            SIZE_OF_HEAP_UPDATE, XLH_INSERT_CONTAINS_NEW_TUPLE, XLOG_HEAP_DELETE, XLOG_HEAP_INSERT,
            XLOG_HEAP_UPDATE,
        };
        const HAS_DATA: u8 = 0x20;
        let rel = Arc::new(toast_chunk_rel());
        let data_raw =
            |rm, info, md, data| toast_raw(rm, info, md, toast_block(HAS_DATA, data, Vec::new()));

        // xl_heap_delete: xmax, offnum 6, infobits, flags
        let delete = data_raw(
            RmId::Heap,
            XLOG_HEAP_DELETE,
            vec![0, 0, 0, 0, 6, 0, 0, 0],
            Vec::new(),
        );
        let ops = decode_stashed_toast(&delete, &rel).unwrap();
        use StashedToastOp::Delete;
        let [Delete(d)] = &ops[..] else { panic!() };
        assert_eq!(
            (d.db_oid, d.toast_relid, d.blkno, d.offnum, d.source_lsn),
            (5, 16401, 9, 6, 0x200)
        );

        let (info, md, data) = multi_insert_parts(
            XLH_INSERT_CONTAINS_NEW_TUPLE,
            &[
                chunk_tuple(900, 0, Some(b"ab")),
                chunk_tuple(900, 1, Some(b"cd")),
            ],
        );
        let ops = decode_stashed_toast(&data_raw(RmId::Heap2, info, md, data), &rel).unwrap();
        let chunks: Vec<_> = ops
            .iter()
            .map(|op| {
                let c = chunk_of(op);
                (c.chunk_seq, c.blkno, c.offnum, c.chunk_data.to_vec())
            })
            .collect();
        assert_eq!(
            chunks,
            [(0, 0, 0, b"ab".to_vec()), (1, 0, 0, b"cd".to_vec())]
        );
        // MULTI_INSERT omits tuple offsets on INIT_PAGE, even for one tuple
        let (info, md, data) = multi_insert_parts(
            XLH_INSERT_CONTAINS_NEW_TUPLE,
            &[chunk_tuple(900, 2, Some(b"ef"))],
        );
        let ops = decode_stashed_toast(&data_raw(RmId::Heap2, info, md, data), &rel).unwrap();
        assert_eq!((chunk_of(&ops[0]).blkno, chunk_of(&ops[0]).offnum), (0, 0));
        let mut blockless = delete;
        blockless.blocks.clear();
        assert!(decode_stashed_toast(&blockless, &rel).unwrap().is_empty());

        let insert = data_raw(
            RmId::Heap,
            XLOG_HEAP_INSERT,
            vec![1, 0, 0],
            chunk_tuple(900, 0, Some(b"ab")),
        );
        assert!(
            decode_stashed_toast(&insert, &Arc::new(make_rel()))
                .unwrap()
                .is_empty()
        );
        let update = data_raw(
            RmId::Heap,
            XLOG_HEAP_UPDATE,
            vec![0; SIZE_OF_HEAP_UPDATE],
            chunk_tuple(900, 0, Some(b"ab")),
        );
        assert!(decode_stashed_toast(&update, &rel).unwrap().is_empty());

        let (info, md, _) =
            multi_insert_parts(XLH_INSERT_CONTAINS_NEW_TUPLE, &[chunk_tuple(1, 0, None)]);
        let raw = data_raw(RmId::Heap2, info, md, vec![0; 3]);
        let err = decode_stashed_toast(&raw, &rel).err().expect("truncated");
        assert!(err.to_string().contains("stashed record decode"), "{err}");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn resolve_stash_fail_closed_verdicts() {
        use crate::catalog::desc_log::{
            Ambiguity, AmbiguityReason, AmbiguityScope, BatchRecord, LogValue,
        };
        let toast_at = |oid, rel_node| {
            let mut rel = crate::backfill::backup_page_walk::toast_chunk_rel();
            rel.oid = oid;
            rel.rfn.rel_node = rel_node;
            Arc::new(rel)
        };
        let (rotated, fenced) = (toast_at(16401, 16401), toast_at(16700, 16700));
        let rotated_next = toast_at(16401, 16601);
        let (ambiguous, payload_free) = (int4_descriptor(16800), int4_descriptor(16900));
        let fence = |rel_node, from_lsn, through_lsn| {
            Arc::new(Ambiguity {
                scope: AmbiguityScope::Rfn(rfn_of(rel_node)),
                from_lsn,
                through_lsn,
                reason: AmbiguityReason::UnknownMutationPosition,
            })
        };
        let batch = |captured_at, entries, ambiguities| BatchRecord {
            captured_at,
            commit_lsn: captured_at - 0x10,
            observations: Vec::new(),
            ambiguities,
            entries,
        };
        let present = |at, r: &Arc<RelDescriptor>| log_entry(at, r, LogValue::Present(r.clone()));
        let log_dir = tempdir().unwrap();
        let log = open_log(
            log_dir.path(),
            vec![
                batch(
                    0x100,
                    [&rotated, &fenced, &ambiguous, &payload_free]
                        .into_iter()
                        .map(|r| present(0x100, r))
                        .collect(),
                    Vec::new(),
                ),
                batch(
                    0x300,
                    vec![present(0x300, &rotated_next)],
                    vec![fence(16700, 0x150, 0x250)],
                ),
                batch(0x500, Vec::new(), vec![fence(16800, 0x350, 0x500)]),
            ],
        )
        .await;
        let spill_dir = tempdir().unwrap();
        let buffer = Arc::new(Mutex::new(
            XactBuffer::new(cfg(spill_dir.path().to_path_buf())).unwrap(),
        ));
        {
            let mut b = buffer.lock().await;
            for (xid, rel_node) in [(1, 16601), (2, 16700), (3, 16800), (4, 16900)] {
                b.track_unresolvable(xid, 0x200, rfn_of(rel_node));
            }
        }
        let stats = Arc::new(EmitterStats::default());
        let resolve = |xid| resolve_stash(&buffer, &log, xid, &[], 0x400, stats.clone());

        let err = resolve(1).await.err().expect("rotated toast generation");
        assert!(
            matches!(
                err,
                XactBufferError::IncompleteToastGeneration { relid: 16401 }
            ),
            "{err}"
        );
        for (xid, from, through) in [(2, 0x150, 0x250), (3, 0x350, 0x500)] {
            let err = resolve(xid).await.err().expect("fenced filenode");
            assert!(
                matches!(
                    err,
                    XactBufferError::StashAmbiguous { lsn: 0x400, from_lsn, through_lsn, .. }
                        if from_lsn == from && through_lsn == through
                ),
                "xid {xid}: {err}"
            );
        }
        let resolved = resolve(4).await.unwrap();
        let outcomes = &resolved.res.as_ref().unwrap().outcomes;
        assert!(
            matches!(
                outcomes.get(&rfn_of(16900)),
                Some(StashOutcome::Ordinary(history))
                    if history.at(0x200).is_ok_and(|(rel, at)| rel.oid == 16900 && at == 0x100)
            ),
            "{:?}",
            outcomes.keys().collect::<Vec<_>>()
        );
    }
}
