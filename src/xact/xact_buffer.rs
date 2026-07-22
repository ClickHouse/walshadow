//! Per-xid xact buffer + TOAST reassembly.
//!
//! Holds every [`DecodedHeap`] + TOAST chunk for an xid until the
//! matching `XLOG_XACT_COMMIT` / `XLOG_XACT_ABORT` lands. Commit drains
//! in WAL order substituting each `ColumnValue::ExternalToast` with its
//! reassembled `Bytea` / `Text`; abort drops buffer + spill file.
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
//! [`ShadowCatalog::relation_at`](crate::catalog::shadow_catalog::ShadowCatalog::relation_at)
//! per heap needing detoast; the catalog's LRU covers repeat lookups so
//! a buffer-internal cache would duplicate it.
//!
//! ## Spill policy
//!
//! Once `memory_used > config.xact_buffer_max`, flush the largest
//! in-memory xact to a [`SpillWriter`]; the xact stays open and later
//! records append to the file. Mirrors PG `ReorderBufferLargestTXN`
//! (`src/backend/replication/logical/reorderbuffer.c`).
//!
//! Drain: spilled entries first (older), then in-mem. Eviction always
//! flushes from front of `in_mem`, holding "spilled older than in-mem".
//!
//! Spill-to-ClickHouse (Option B) is deferred; v1 is local-disk-only.

use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap, HashSet, VecDeque};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use thiserror::Error;
use tokio::sync::Mutex;
use tracing::Instrument;
use walrus::pg::walparser::{RelFileNode, RmId};

use crate::catalog::desc_log::{DescriptorLog, LookupResult};
use crate::decode::decoder_sink::{DecoderSinkError, DecoderStats};
use crate::decode::heap_decoder::{
    ColumnValue, DecodedHeap, HeapOp, ToastPointer, decode_heap_record,
};
#[cfg(test)]
use crate::decode::wal_xact::{
    XACT_XINFO_HAS_DBINFO, XACT_XINFO_HAS_GID, XACT_XINFO_HAS_INVALS, XACT_XINFO_HAS_SUBXACTS,
    XACT_XINFO_HAS_TWOPHASE, XLOG_XACT_HAS_INFO, parse_xact_assignment, parse_xact_payload,
};
use crate::emit::ch_emitter::EmitterStats;
use crate::ops::trace::{InflightSnapshotEntry, TxnSpanRegistry, new_txn_span};
use crate::record::{Record, RecordSink, Route, SinkError};
use crate::runtime_config::{ConfigEvent, ConfigTableKind};
use crate::schema::{RelDescriptor, SchemaEvent};
use crate::toast::{
    Body, ChunkRefMap, FetchedValue, ToastResolver, ToastRowRef, ToastValueError, ValueRef,
    check_value_caps, detoasted_value, finish_value, pointer_extsize,
};
use crate::xact::spill::{
    BodySpoolFile, BodySpoolWriter, RawRecord, SpillEntry, SpillError, SpillReader, SpillStore,
    SpillWriter, ToastChunk, ToastDelete,
};

use std::pin::Pin;

/// Matches PG `logical_decoding_work_mem` default 64 MiB
/// (`src/backend/utils/misc/guc_tables.c`)
pub const DEFAULT_XACT_BUFFER_MAX: usize = 64 * 1024 * 1024;

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
        SpillEntry::Heap(h) => h.source_lsn,
        SpillEntry::Chunk(c) => c.source_lsn,
        SpillEntry::ToastDelete(d) => d.source_lsn,
        SpillEntry::Raw(r) => r.source_lsn,
    }
}

/// Resident cap on one commit drain's memory-held chunk bodies; bodies
/// past it spool to disk (`toastbody-*`), refs stay resident
pub const TOAST_BODY_SPOOL_MEM_MAX: usize = 16 << 20;

/// Cap on resident chunk-index + mirror-row-ref metadata per commit
/// drain; breach fails the drain with a typed non-retryable error before
/// further allocation. ~`CHUNK_REF_META`/chunk ⇒ default admits ~500k
/// chunks ≈ 1 GB TOAST payload per xact
pub const TOAST_INDEX_MEM_MAX: usize = 64 << 20;

/// Resident approximation per indexed chunk: one `ValueRef`/tail entry or
/// one row ref, container overhead included
const CHUNK_REF_META: usize = 64;

#[derive(Debug, Clone)]
pub struct XactBufferConfig {
    /// In-memory budget across all active xacts before eviction
    pub xact_buffer_max: usize,
    /// Per-xid spill files land here
    pub spill_dir: PathBuf,
    /// Per-drain memory-held chunk body budget before body spooling
    pub toast_body_mem_max: usize,
    /// Per-drain chunk/row-ref metadata cap
    pub toast_index_mem_max: usize,
}

impl XactBufferConfig {
    pub fn new(spill_dir: PathBuf) -> Self {
        Self {
            xact_buffer_max: DEFAULT_XACT_BUFFER_MAX,
            spill_dir,
            toast_body_mem_max: TOAST_BODY_SPOOL_MEM_MAX,
            toast_index_mem_max: TOAST_INDEX_MEM_MAX,
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
    /// Non-retryable: replay hits the same cardinality. Raise
    /// `toast_index_mem_max` or reduce per-xact TOAST chunk count
    #[error("toast index metadata {bytes} bytes exceeds cap {max}")]
    ToastIndexOverflow { bytes: usize, max: usize },
    /// Hard value cap, checked before allocation. Non-retryable:
    /// replay decodes same value; raise `inline_value_max`
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
    pub drain_lsn: u64,
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
/// `source_lsn`. Heap tuples ride the sibling `heaps` vec (batched for the
/// decode pool); a `DrainEntry` is applied in WAL order *between* heap
/// segments. `Catalog` fences DDL, `Config` refreshes runtime mapping,
/// `ToastBarrier` closes rewrite generations. All inherit merge tie-break:
/// control event sorts before heap at equal LSN.
#[derive(Debug, Clone)]
pub enum DrainEntry {
    Catalog(SchemaEvent),
    /// Source-PG config-table write, applied at its position to the resolver
    /// so trailing rows in the same xact route against the post-config shape.
    Config(ConfigEvent),
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
    first_lsn: u64,
    /// WAL-order by arrival
    in_mem: Vec<SpillEntry>,
    in_mem_bytes: usize,
    /// `None` until first eviction
    spill: Option<SpillWriter>,
    spill_bytes: u64,
    /// source_lsn ASC. Not spilled: typed control state would duplicate
    /// catalog/config encodings; events per xact stay small
    events: Vec<(u64, DrainEntry)>,
    /// Filenodes this xact wrote that were invisible at record time
    /// (same-xact CREATE / TRUNCATE / rewrite generations, or markerless
    /// tracking). Resolved at commit via `relation_at(rfn, commit_lsn)`.
    stash_rfns: HashSet<RelFileNode>,
    /// Per-txn `txn` span; duration = WAL-record→durable latency.
    span: tracing::Span,
    /// Child of `span` covering first-buffered→COMMIT-observed (parked-for-
    /// commit wait); closes when the drain consumes the state.
    _wait_span: tracing::Span,
}

impl XactState {
    fn new(first_lsn: u64, span: tracing::Span) -> Self {
        let wait_span = trace_span!(
            !span.is_none(),
            parent: &span,
            "buffer.wait",
            first_lsn = first_lsn,
        );
        Self {
            first_lsn,
            in_mem: Vec::new(),
            in_mem_bytes: 0,
            spill: None,
            spill_bytes: 0,
            events: Vec::new(),
            stash_rfns: HashSet::new(),
            span,
            _wait_span: wait_span,
        }
    }
}

/// Approximate byte cost for in-memory accounting; estimate, not exact
/// heap allocation. Good enough for the eviction threshold
fn approximate_size(entry: &SpillEntry) -> usize {
    match entry {
        SpillEntry::Heap(h) => {
            let mut sz = std::mem::size_of::<DecodedHeap>();
            if let Some(t) = &h.new {
                sz += tuple_size(t);
            }
            if let Some(t) = &h.old {
                sz += tuple_size(t);
            }
            sz
        }
        SpillEntry::Chunk(c) => std::mem::size_of::<ToastChunk>() + c.chunk_data.len(),
        SpillEntry::ToastDelete(_) => std::mem::size_of::<crate::xact::spill::ToastDelete>(),
        SpillEntry::Raw(r) => r.approx_bytes(),
    }
}

fn tuple_size(t: &crate::decode::heap_decoder::DecodedTuple) -> usize {
    let mut sz = std::mem::size_of::<crate::decode::heap_decoder::DecodedTuple>()
        + t.columns.capacity() * std::mem::size_of::<Option<ColumnValue>>();
    for v in t.columns.iter().flatten() {
        sz += value_size(v);
    }
    sz
}

fn value_size(v: &ColumnValue) -> usize {
    match v {
        ColumnValue::Bytea(b) => b.len(),
        ColumnValue::Text(s) | ColumnValue::Name(s) => s.len(),
        ColumnValue::Unsupported { raw, .. } => raw.len(),
        _ => 0,
    }
}

#[derive(Debug, Eq, Ord, PartialEq, PartialOrd)]
struct PendingDurableXact {
    first_lsn: u64,
    commit_lsn: u64,
}

#[derive(Debug, Default)]
struct PendingDurable {
    by_first_lsn: BinaryHeap<Reverse<PendingDurableXact>>,
}

impl PendingDurable {
    fn push(&mut self, first_lsn: u64, commit_lsn: u64) {
        self.by_first_lsn.push(Reverse(PendingDurableXact {
            first_lsn,
            commit_lsn,
        }));
    }

    fn prune(&mut self, durable_ack: u64) {
        while self
            .by_first_lsn
            .peek()
            .is_some_and(|Reverse(xact)| xact.commit_lsn <= durable_ack)
        {
            self.by_first_lsn.pop();
        }
    }

    fn min_first_lsn(&self) -> Option<u64> {
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
    /// Resolved ordinary heap/index: decode stays fenced off (lower-bound
    /// `relation_at` can return a later same-filenode shape), counted
    Skip,
}

/// Resolution map for a finishing tree; filenodes absent from `outcomes`
/// discard their records (dropped or rotated away, end-state-neutral)
#[derive(Default)]
pub struct StashResolution {
    outcomes: HashMap<RelFileNode, StashOutcome>,
    stats: Option<Arc<EmitterStats>>,
}

/// Resolve the finishing tree's stashed filenodes against the descriptor
/// log at the commit's `next_lsn` (capture ran inside the boundary hold, so
/// same-xact CREATE/rewrite descriptors are already covered), install
/// outcomes for the imminent drain, and queue `O - B` barriers for
/// marker-proven toast generations. A toast heap without its marker fails
/// closed ([`XactBufferError::IncompleteToastGeneration`]).
pub async fn resolve_stash(
    buffer: &Arc<Mutex<XactBuffer>>,
    log: &DescriptorLog,
    top_xid: u32,
    subxids: &[u32],
    next_lsn: u64,
    stats: Arc<EmitterStats>,
) -> std::result::Result<(), XactBufferError> {
    let rfns = {
        let buf = buffer.lock().await;
        let mut xids: Vec<u32> = Vec::with_capacity(1 + subxids.len());
        xids.push(top_xid);
        xids.extend_from_slice(subxids);
        buf.stash_candidates(&xids)
    };
    if rfns.is_empty() {
        return Ok(());
    }
    let mut outcomes: HashMap<RelFileNode, StashOutcome> = HashMap::new();
    let mut barriers: Vec<(u32, u64)> = Vec::new();
    for rfn in &rfns {
        match log.descriptor_at(*rfn, next_lsn) {
            LookupResult::Present(rel) if rel.kind == 't' => {
                let marker = buffer.lock().await.marker_lsn(*rfn);
                let Some(marker_lsn) = marker else {
                    return Err(XactBufferError::IncompleteToastGeneration { relid: rel.oid });
                };
                barriers.push((rel.oid, marker_lsn));
                outcomes.insert(*rfn, StashOutcome::Toast(rel));
            }
            LookupResult::Present(_) => {
                outcomes.insert(*rfn, StashOutcome::Skip);
            }
            // Dropped / rotated away by this xid or a later covered commit;
            // AEL supersession makes the discard end-state-neutral. Foreign
            // db never stashes rows worth keeping; NotCovered = rel never
            // reached the log (born + gone inside this xact's family)
            LookupResult::Dropped
            | LookupResult::Retired
            | LookupResult::NotCovered
            | LookupResult::ForeignDb => {}
        }
    }
    let mut buf = buffer.lock().await;
    for (toast_relid, marker_lsn) in barriers {
        buf.on_toast_barrier(top_xid, next_lsn, toast_relid, marker_lsn);
    }
    buf.forget_markers(&rfns);
    buf.install_stash_resolution(
        top_xid,
        StashResolution {
            outcomes,
            stats: Some(stats),
        },
    );
    Ok(())
}

/// Per-xact + TOAST buffer with spill-to-disk overflow, keyed by `xid`
pub struct XactBuffer {
    config: XactBufferConfig,
    store: SpillStore,
    inflight: HashMap<u32, XactState>,
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
    /// Commit-time resolution installed by [`resolve_stash`] just before
    /// the drain pops it, keyed by top xid
    pending_stash: HashMap<u32, StashResolution>,
    /// Committed transactions waiting for durable acknowledgment
    pending_durable: PendingDurable,
    bytes_in_memory: usize,
    stats: XactBufferStats,
    /// Shared with the WAL pump: the pump opens a `txn` span here at first
    /// sighting of an xid, and `absorb` adopts it so the span starts at
    /// WAL-read rather than at buffering. Empty (and unused) when tracing
    /// is off — `absorb` then mints its own span.
    span_registry: TxnSpanRegistry,
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
        let store = SpillStore::new(config.spill_dir.clone())?;
        Ok(Self {
            config,
            store,
            inflight: HashMap::new(),
            markers: HashMap::new(),
            marker_order: VecDeque::new(),
            pending_stash: HashMap::new(),
            pending_durable: PendingDurable::default(),
            bytes_in_memory: 0,
            stats: XactBufferStats::default(),
            span_registry: TxnSpanRegistry::new(),
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

    /// Clone of the [`TxnSpanRegistry`] the pump pushes `txn` spans into.
    /// Wire this into the pump-side record sink so spans open at WAL read.
    pub fn span_registry(&self) -> TxnSpanRegistry {
        self.span_registry.clone()
    }

    /// Clear leftover spill files from a prior crash. Cursor file
    /// guarantees on-disk state was either drained-to-CH or
    /// replayable from `decoder_lsn`, so the spill dir is always
    /// safe to wipe at startup. Caller invokes once before any `on_*`.
    pub async fn clear_spill_dir(&self) -> std::result::Result<(), XactBufferError> {
        self.store.clear().await?;
        Ok(())
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
                let mut last_lsn = st.first_lsn;
                let mut rels: std::collections::BTreeSet<(u32, u32)> =
                    std::collections::BTreeSet::new();
                let mut heap_count = 0u64;
                let mut chunk_count = 0u64;
                for e in &st.in_mem {
                    match e {
                        SpillEntry::Heap(h) => {
                            heap_count += 1;
                            last_lsn = last_lsn.max(h.source_lsn);
                            rels.insert((h.rfn.db_node, h.rfn.rel_node));
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
                            if let Some(rfn) = r.rfn() {
                                rels.insert((rfn.db_node, rfn.rel_node));
                            }
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
                    first_lsn: st.first_lsn,
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

    /// Detoast descriptor is fetched in [`detoast_heap`] after drain:
    /// no per-xact rel cache here, catalog's LRU covers repeat lookups
    pub async fn on_heap(
        &mut self,
        decoded: DecodedHeap,
    ) -> std::result::Result<(), XactBufferError> {
        let xid = decoded.xid;
        let first_lsn = decoded.source_lsn;
        let entry = SpillEntry::Heap(Box::new(decoded));
        self.absorb(xid, first_lsn, entry).await
    }

    /// Built from `pg_toast.pg_toast_<rel>` INSERTs by the decoder sink
    pub async fn on_toast_chunk(
        &mut self,
        chunk: ToastChunk,
        xid: u32,
    ) -> std::result::Result<(), XactBufferError> {
        let first_lsn = chunk.source_lsn;
        let entry = SpillEntry::Chunk(chunk);
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
        self.absorb(xid, first_lsn, SpillEntry::ToastDelete(delete))
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
        self.inflight.entry(xid).or_insert_with(|| {
            let span = registry.adopt(xid).unwrap_or_else(|| {
                if registry.is_sampled(xid) {
                    new_txn_span(xid, first_lsn)
                } else {
                    tracing::Span::none()
                }
            });
            XactState::new(first_lsn, span)
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
            self.state_for(xid, lsn).stash_rfns.insert(rfn);
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
        self.state_for(xid, lsn).stash_rfns.insert(rfn);
        self.absorb(xid, lsn, SpillEntry::Raw(Box::new(raw))).await
    }

    /// Track an unresolvable filenode without payload: no marker means the
    /// set can't prove completeness, so entries aren't kept, but commit
    /// resolution must still fail closed if the filenode turns out toast
    pub fn track_unresolvable(&mut self, xid: u32, lsn: u64, rfn: RelFileNode) {
        self.state_for(xid, lsn).stash_rfns.insert(rfn);
    }

    /// Fast path for the decoder: a filenode already stashed under `xid`
    /// (or marker-registered to it) can never resolve for that xact's own
    /// records — its pg_class row is MVCC-invisible until commit — so the
    /// replay-gated lookup is skippable
    pub fn is_stash_candidate(&self, xid: u32, rfn: RelFileNode) -> bool {
        self.inflight
            .get(&xid)
            .is_some_and(|st| st.stash_rfns.contains(&rfn))
    }

    /// Union of stash candidates across the finishing tree
    pub fn stash_candidates(&self, xids: &[u32]) -> Vec<RelFileNode> {
        let mut out: Vec<RelFileNode> = Vec::new();
        for x in xids {
            if let Some(st) = self.inflight.get(x) {
                out.extend(st.stash_rfns.iter().copied());
            }
        }
        out.sort_unstable();
        out.dedup();
        out
    }

    /// Drop consumed markers post-resolution (abort drops via its states)
    pub fn forget_markers(&mut self, rfns: &[RelFileNode]) {
        for rfn in rfns {
            self.markers.remove(rfn);
        }
    }

    /// Install commit-time resolution for `top_xid`'s imminent drain
    pub fn install_stash_resolution(&mut self, top_xid: u32, res: StashResolution) {
        self.pending_stash.insert(top_xid, res);
    }

    /// Drains in `source_lsn` order at commit, so a DDL's `Added`/`Changed`
    /// event lands BEFORE the heap writes that follow it
    pub fn on_schema_event(&mut self, xid: u32, source_lsn: u64, event: SchemaEvent) {
        self.push_drain_entry(xid, source_lsn, DrainEntry::Catalog(event));
    }

    /// Config-table write, interleaved into the drain at its `source_lsn` so it
    /// applies before the heap writes it precedes in WAL (plan §6)
    pub fn on_config_event(&mut self, xid: u32, source_lsn: u64, event: ConfigEvent) {
        self.push_drain_entry(xid, source_lsn, DrainEntry::Config(event));
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
        let st = self.state_for(xid, first_lsn);
        if let Some(spill) = st.spill.as_mut() {
            // Already spilling: append straight to disk
            spill.write(&entry).await?;
            let bc = spill.byte_count();
            let prev = std::mem::replace(&mut st.spill_bytes, bc);
            self.stats.spill_bytes_active += bc - prev;
        } else {
            st.in_mem.push(entry);
            st.in_mem_bytes += sz;
            self.bytes_in_memory += sz;
        }
        self.stats.bytes_in_memory = self.bytes_in_memory as u64;
        self.maybe_evict().await?;
        Ok(())
    }

    async fn maybe_evict(&mut self) -> std::result::Result<(), XactBufferError> {
        while self.bytes_in_memory > self.config.xact_buffer_max {
            let largest = self
                .inflight
                .iter()
                .filter(|(_, s)| !s.in_mem.is_empty())
                .max_by_key(|(_, s)| s.in_mem_bytes)
                .map(|(xid, _)| *xid);
            let Some(xid) = largest else {
                // All active xacts already on disk; caller pushing into
                // spilled xacts faster than budget allows
                break;
            };
            self.evict_xact(xid).await?;
        }
        Ok(())
    }

    async fn evict_xact(&mut self, xid: u32) -> std::result::Result<(), XactBufferError> {
        let st = self.inflight.get_mut(&xid).expect("xid present");
        let first_spill = st.spill.is_none();
        if first_spill {
            st.spill = Some(self.store.writer(xid, st.first_lsn).await?);
        }
        let writer = st.spill.as_mut().unwrap();
        let drained: Vec<SpillEntry> = std::mem::take(&mut st.in_mem);
        let freed = std::mem::take(&mut st.in_mem_bytes);
        for entry in drained {
            writer.write(&entry).await?;
        }
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
    ) -> std::result::Result<MergedDrain, XactBufferError> {
        let mut gauge = self.drain_gauge(&self.drain_head_resident);
        let mut sources = Vec::with_capacity(states.len());
        let mut events = Vec::with_capacity(states.len());
        for mut st in states {
            let reader = match st.spill.take() {
                Some(writer) => {
                    let bc = writer.byte_count();
                    self.stats.spill_bytes_active =
                        self.stats.spill_bytes_active.saturating_sub(bc);
                    self.stats.spill_xacts_active = self.stats.spill_xacts_active.saturating_sub(1);
                    Some(writer.finish().await?)
                }
                None => None,
            };
            let in_mem = std::mem::take(&mut st.in_mem);
            sources.push(MergeSource::open(reader, in_mem, &mut gauge).await?);
            // Two producers push events: the worker at observe order and
            // the pump at capture time keyed bias-early valid_from — LSN
            // order is not arrival order. Stable sort keeps same-LSN
            // arrival order (Added before dependent Changed)
            let mut evs = std::mem::take(&mut st.events);
            evs.sort_by_key(|(lsn, _)| *lsn);
            events.push(evs.into());
        }
        Ok(MergedDrain {
            sources,
            events,
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
            index_meta_bytes: 0,
            index_mem_max: self.config.toast_index_mem_max,
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
    /// run in the decode pool / barrier coordinator; pipeline ack collector
    /// owns `emitter_ack_lsn`.
    pub async fn drain_committed(
        &mut self,
        top_xid: u32,
        commit_ts: i64,
        commit_lsn: u64,
        subxids: &[u32],
        // `resolver.stores_chunks()` at the caller: collect store rows only
        // when a put consumer exists
        collect_rows: bool,
    ) -> std::result::Result<CommittedDrain, XactBufferError> {
        let mut xids: Vec<u32> = Vec::with_capacity(1 + subxids.len());
        xids.push(top_xid);
        xids.extend_from_slice(subxids);
        let mut states: Vec<XactState> = Vec::with_capacity(xids.len());
        for x in &xids {
            if let Some(st) = self.inflight.remove(x) {
                states.push(st);
            }
        }
        self.stats.drain_lsn = self.stats.drain_lsn.max(commit_lsn);
        if states.is_empty() {
            // Read-only / filter-dropped: reorder coordinator still
            // registers a seq so the contiguous watermark passes commit_lsn
            self.stats.commits_unknown_xid += 1;
            return Ok(CommittedDrain {
                commit_ts,
                commit_lsn,
                had_states: false,
                merged: None,
                generations: Vec::new(),
            });
        }
        // Preserve floor while decoded slices remain undurable
        if let Some(first) = states.iter().map(|st| st.first_lsn).min() {
            self.pending_durable.push(first, commit_lsn);
        }
        for st in &states {
            self.stats.xacts_active = self.stats.xacts_active.saturating_sub(1);
            self.bytes_in_memory = self.bytes_in_memory.saturating_sub(st.in_mem_bytes);
        }
        self.stats.bytes_in_memory = self.bytes_in_memory as u64;
        let stash = self.pending_stash.remove(&top_xid).unwrap_or_default();
        let merged = self
            .open_drain(states, collect_rows, stash, top_xid, commit_lsn)
            .await?;
        self.stats.committed_xacts_total += 1;
        Ok(CommittedDrain {
            commit_ts,
            commit_lsn,
            had_states: true,
            merged: Some(merged),
            generations: Vec::new(),
        })
    }

    /// When no xact in flight, advance `drain_lsn` to `lsn`: trailing
    /// post-COMMIT WAL (page padding, RUNNING_XACTS, CHECKPOINT) counts as
    /// drained when quiescent. The durable ack side lives in the ack
    /// collector (`AckHandle::trailing`).
    pub fn advance_idle(&mut self, lsn: u64) {
        if self.stats.xacts_active != 0 {
            return;
        }
        self.stats.drain_lsn = self.stats.drain_lsn.max(lsn);
    }

    /// Floor durable acknowledgment at first record of each undurable transaction
    pub fn resume_safe_lsn(&mut self, durable_ack: u64) -> u64 {
        self.pending_durable.prune(durable_ack);
        self.inflight
            .values()
            .map(|st| st.first_lsn)
            .chain(self.pending_durable.min_first_lsn())
            .fold(durable_ack, u64::min)
    }

    /// Discard xact `xid` + spill file. No-op if unknown. `abort_lsn` is
    /// the `XLOG_XACT_ABORT` record LSN; advances `drain_lsn` so aborts
    /// count as fully consumed (the ack side rides the reorder's rows=0
    /// abort seq through the collector)
    pub async fn abort(
        &mut self,
        xid: u32,
        abort_lsn: u64,
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
        self.span_registry.prune(&xids);

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
            for rfn in &st.stash_rfns {
                self.markers.remove(rfn);
            }
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
/// from the reader until EOF, then drains `in_mem`. The EOF reader parks in
/// `spent` so the file unlinks only at [`MergedDrain::finish`],
/// post-dispatch.
struct MergeSource {
    head: Option<SpillEntry>,
    reader: Option<SpillReader>,
    spent: Option<SpillReader>,
    in_mem: VecDeque<SpillEntry>,
}

impl MergeSource {
    async fn open(
        reader: Option<SpillReader>,
        in_mem: Vec<SpillEntry>,
        gauge: &mut ResidentGauge,
    ) -> std::result::Result<Self, SpillError> {
        // Tail entries are resident from the start (they left the buffer's
        // `bytes_in_memory` at commit); spill entries join at decode.
        for e in &in_mem {
            gauge.add(approximate_size(e));
        }
        let mut src = Self {
            head: None,
            reader,
            spent: None,
            in_mem: in_mem.into(),
        };
        src.refill(gauge).await?;
        Ok(src)
    }

    async fn refill(&mut self, gauge: &mut ResidentGauge) -> std::result::Result<(), SpillError> {
        debug_assert!(self.head.is_none());
        if let Some(r) = self.reader.as_mut() {
            if let Some(entry) = r.next().await? {
                gauge.add(approximate_size(&entry));
                self.head = Some(entry);
                return Ok(());
            }
            self.spent = self.reader.take();
        }
        // Already counted at `open`; moving to head keeps it resident
        self.head = self.in_mem.pop_front();
        Ok(())
    }

    fn head_lsn(&self) -> Option<u64> {
        self.head.as_ref().map(entry_lsn)
    }

    async fn pop(
        &mut self,
        gauge: &mut ResidentGauge,
    ) -> std::result::Result<Option<SpillEntry>, SpillError> {
        let Some(entry) = self.head.take() else {
            return Ok(None);
        };
        gauge.sub(approximate_size(&entry));
        self.refill(gauge).await?;
        Ok(Some(entry))
    }
}

enum MergeItem {
    Heap(Box<DecodedHeap>),
    Event(DrainEntry),
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
    /// Cumulative ref metadata, checked against `index_mem_max`
    index_meta_bytes: usize,
    index_mem_max: usize,
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
                    return Ok(Some(MergeItem::Event(ev)));
                }
                Pick::Data(i) => {
                    let entry = self.sources[i]
                        .pop(&mut self.gauge)
                        .await?
                        .expect("just peeked head");
                    match entry {
                        SpillEntry::Heap(h) => return Ok(Some(MergeItem::Heap(h))),
                        SpillEntry::Chunk(c) => self.fold_chunk(c)?,
                        SpillEntry::ToastDelete(d) => self.fold_delete(d)?,
                        SpillEntry::Raw(raw) => self.fold_raw(&raw)?,
                    }
                }
            }
        }
    }

    /// Cap resident ref metadata before allocating more; typed
    /// non-retryable error fails the drain loud, replay-safe
    fn reserve_meta(&mut self, n: usize) -> std::result::Result<(), XactBufferError> {
        if self.index_meta_bytes + n > self.index_mem_max {
            return Err(XactBufferError::ToastIndexOverflow {
                bytes: self.index_meta_bytes + n,
                max: self.index_mem_max,
            });
        }
        self.index_meta_bytes += n;
        Ok(())
    }

    /// Body kept memory-resident below `body_mem_max` cumulative bytes,
    /// appended once to the body spool past it (resolution map and mirror
    /// rows share either form)
    fn fold_body(&mut self, data: &bytes::Bytes) -> std::result::Result<Body, XactBufferError> {
        if self.spool.is_none() {
            if self.mem_body_bytes + data.len() <= self.body_mem_max {
                self.mem_body_bytes += data.len();
                return Ok(Body::Mem(data.clone()));
            }
            self.spool = Some(BodySpoolWriter::create(
                &self.spool_dir,
                self.spool_xid,
                self.spool_lsn,
                Some(self.spool_gauge.clone()),
            )?);
        }
        let spool = self.spool.as_mut().expect("just created");
        let r = spool.append(data)?;
        Ok(Body::File(r))
    }

    fn fold_chunk(&mut self, c: ToastChunk) -> std::result::Result<(), XactBufferError> {
        // InvalidOffsetNumber cannot key mirror rows
        let collect_row = self.collect_rows && c.offnum != 0;
        self.reserve_meta(CHUNK_REF_META * (1 + usize::from(collect_row)))?;
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
            std::collections::hash_map::Entry::Occupied(mut o) => {
                o.get_mut().push(c.chunk_seq, body);
            }
            std::collections::hash_map::Entry::Vacant(v) => {
                v.insert(ValueRef::new(c.chunk_seq, body));
            }
        }
        Ok(())
    }

    fn fold_delete(&mut self, d: ToastDelete) -> std::result::Result<(), XactBufferError> {
        if !self.collect_rows {
            return Ok(());
        }
        self.reserve_meta(CHUNK_REF_META)?;
        self.row_bytes += CHUNK_REF_META;
        self.row_gauge.add(CHUNK_REF_META);
        self.rows.push(ToastRowRef::tombstone(&d));
        Ok(())
    }

    /// Decode a stashed record against its commit-time verdict: toast heaps
    /// fold chunks/tombstones into the same maps as live-path entries (so
    /// same-xact referrers detoast and mirror rows flow), fenced heaps and
    /// unresolvable filenodes count and drop
    fn fold_raw(&mut self, raw: &RawRecord) -> std::result::Result<(), XactBufferError> {
        let Some(rfn) = raw.rfn() else {
            return Ok(());
        };
        let bump = |c: &dyn Fn(&EmitterStats) -> &AtomicU64| {
            if let Some(s) = &self.stash.stats {
                c(s).fetch_add(1, Ordering::Relaxed);
            }
        };
        let rel = match self.stash.outcomes.get(&rfn) {
            Some(StashOutcome::Toast(rel)) => rel.clone(),
            Some(StashOutcome::Skip) => {
                bump(&|s| &s.toast_stash_skipped);
                return Ok(());
            }
            None => {
                bump(&|s| &s.toast_stash_discarded);
                return Ok(());
            }
        };
        bump(&|s| &s.toast_stash_decoded);
        for op in decode_stashed_toast(raw, &rel)? {
            match op {
                StashedToastOp::Chunk(c) => self.fold_chunk(c)?,
                StashedToastOp::Delete(d) => self.fold_delete(d)?,
            }
        }
        Ok(())
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
    /// flush makes the sealed refs readable to decode workers.
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
            _resident: resident,
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
            if let Some(r) = s.spent {
                r.unlink().await?;
            }
            if let Some(r) = s.reader {
                r.unlink().await?;
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
pub struct ToastRowBatch {
    rows: Vec<ToastRowRef>,
    spool: Option<Arc<BodySpoolFile>>,
    _resident: ResidentGauge,
}

impl ToastRowBatch {
    pub fn spool(&self) -> Option<&BodySpoolFile> {
        self.spool.as_deref()
    }

    /// Gauged resident bytes (ref metadata; bodies count under the
    /// owning generation)
    pub fn resident_bytes(&self) -> usize {
        self._resident.held as usize
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
/// still TOAST-toasted; the decode pool handles detoast + routing.
/// Non-empty `ordered_events` (or a `HeapOp::Truncate` heap) makes the
/// slice a barrier the reorder coordinator serializes against ClickHouse.
pub struct DrainedBatch {
    /// `source_lsn` ASC within the slice; later slices strictly follow.
    pub heaps: Vec<DecodedHeap>,
    pub ordered_events: Vec<OrderedEvent>,
    /// Chunk generations sealed so far, oldest first. A chunk's WAL position
    /// precedes its referrer, so every heap's value lives in exactly one
    /// generation sealed no later than this slice; slices share payloads via
    /// `Arc` instead of copying per batch, and each generation is immutable
    /// once sealed (decode pool reads while later slices load).
    pub chunks: Vec<Arc<ChunkGeneration>>,
    /// WAL-ordered births and tombstones, empty without store
    pub new_rows: ToastRowBatch,
    /// `new_rows` cursor for each TRUNCATE heap
    pub truncate_rows: Vec<usize>,
    /// Last slice of the commit. Only its seq may publish `commit_lsn` in
    /// the ack (`register` vs `register_partial`): an earlier slice
    /// publishing would claim durability for rows still in flight.
    pub is_final: bool,
}

/// One step of a [`DrainedBatch`] apply plan (see [`DrainedBatch::into_walk`]).
pub enum WalkStep {
    /// Seal store rows: put `new_rows[cursor..upto]` before the next step
    /// (`upto` may equal the cursor — nothing to put).
    Rows {
        upto: usize,
    },
    Event(DrainEntry),
    Truncate(DecodedHeap),
    Heap(DecodedHeap),
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
            if matches!(heap.op, HeapOp::Truncate) {
                let upto = trunc
                    .next()
                    .expect("truncate_rows cursor per Truncate heap");
                steps.push(WalkStep::Rows { upto });
                steps.push(WalkStep::Truncate(heap));
            } else {
                steps.push(WalkStep::Heap(heap));
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
    /// False for read-only / filter-dropped / unknown xid.
    pub had_states: bool,
    merged: Option<MergedDrain>,
    generations: Vec<Arc<ChunkGeneration>>,
}

impl CommittedDrain {
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
        let Some(m) = self.merged.as_mut() else {
            return Ok(None);
        };
        let mut heaps: Vec<DecodedHeap> = Vec::new();
        let mut ordered_events: Vec<OrderedEvent> = Vec::new();
        let mut truncate_rows: Vec<usize> = Vec::new();
        let mut bytes = 0usize;
        while heaps.len() < max_rows.max(1) && bytes < max_bytes.max(1) {
            match m.next().await? {
                None => break,
                Some(MergeItem::Event(event)) => ordered_events.push(OrderedEvent {
                    heap_idx: heaps.len(),
                    row_idx: m.rows.len(),
                    event,
                }),
                Some(MergeItem::Heap(h)) => {
                    if h.op == HeapOp::Truncate {
                        truncate_rows.push(m.rows.len());
                    }
                    bytes += h.approx_bytes();
                    heaps.push(*h);
                }
            }
        }
        let is_final = m.is_exhausted();
        let mut sealed = m.take_chunks()?;
        let sealed_empty = sealed.map.is_empty();
        let new_rows = m.take_rows();
        if !sealed_empty {
            if let Some(budget) = budget {
                sealed._permit = Some(budget.admit(sealed.resident_bytes()).await);
            }
            self.generations.push(Arc::new(sealed));
        }
        if heaps.is_empty() && ordered_events.is_empty() && sealed_empty && new_rows.is_empty() {
            return Ok(None);
        }
        Ok(Some(DrainedBatch {
            heaps,
            ordered_events,
            chunks: self.generations.clone(),
            new_rows,
            truncate_rows,
            is_final,
        }))
    }

    /// Unlink spill files; call after the final slice dispatches. On an
    /// error path drop instead: files stay for inspection, startup wipe +
    /// redecode-from-ack cover replay.
    pub async fn finish(mut self) -> std::result::Result<(), XactBufferError> {
        if let Some(m) = self.merged.take() {
            m.finish().await?;
        }
        Ok(())
    }
}

/// Returns the leaf permit shrunk to the decoded bytes retained in the
/// heap's tuples; the caller rides it with the routed row to insert ack
/// so decoded values (and their encoder slab copy) stay covered past
/// this call. `None` without budget or external values.
/// Pub for the decode pool, gap replay, and tests.
pub async fn detoast_heap(
    heap: &mut DecodedHeap,
    // Xact body spool backing `File` refs; None while memory-resident
    spool: Option<&BodySpoolFile>,
    // Ref-map generations, oldest first; a value lives in exactly one
    // (live map on the serial path, sealed drain-batch generations on the
    // parallel path)
    chunk_maps: &[&ChunkRefMap],
    log: &DescriptorLog,
    resolver: &ToastResolver,
) -> std::result::Result<Option<crate::budget::MemoryPermit>, XactBufferError> {
    let mut pointers: Vec<ToastPointer> = Vec::new();
    collect_toast_pointers(heap.new.as_ref(), &mut pointers);
    collect_toast_pointers(heap.old.as_ref(), &mut pointers);
    if pointers.is_empty() {
        return Ok(None);
    }
    let leaf_need = check_value_caps(&pointers, resolver.inline_value_max())?;
    // One leaf at a time per worker: reserved for the heap's aggregate
    // resolution peak (every retained decoded value + the largest
    // single-value transient), shrunk to retained bytes before return
    let mut leaf = match resolver.budget() {
        Some(b) => Some(b.acquire(leaf_need).await),
        None => None,
    };
    // A heap reaching detoast already decoded once against a covered
    // descriptor; anything but Present here is a coverage bug
    let rel: Arc<RelDescriptor> = match log.descriptor_at(heap.rfn, heap.source_lsn) {
        LookupResult::Present(rel) => rel,
        other => {
            return Err(XactBufferError::DescriptorNotCovered {
                rfn: heap.rfn,
                lsn: heap.source_lsn,
                got: format!("{other:?}"),
            });
        }
    };
    let mut uses: HashMap<(u32, u32), u32> = HashMap::new();
    for p in &pointers {
        *uses.entry((p.va_toastrelid, p.va_valueid)).or_default() += 1;
    }
    let mut res = ValueResolution {
        spool,
        xact_maps: chunk_maps,
        resolver,
        source_lsn: heap.source_lsn,
        uses,
        cache: HashMap::new(),
        retained: 0,
    };
    if let Some(t) = heap.new.as_mut() {
        res.resolve_tuple(t, &rel).await?;
    }
    if let Some(t) = heap.old.as_mut() {
        res.resolve_tuple(t, &rel).await?;
    }
    if let Some(p) = leaf.as_mut() {
        p.shrink(res.retained as u64);
    }
    Ok(leaf)
}

/// Append every on-disk toast pointer; carries `va_extinfo`/`va_rawsize`
/// so store fetch can cap allocation.
fn collect_toast_pointers(
    t: Option<&crate::decode::heap_decoder::DecodedTuple>,
    out: &mut Vec<ToastPointer>,
) {
    let Some(t) = t else {
        return;
    };
    for c in &t.columns {
        if let Some(ColumnValue::ExternalToast(p)) = c {
            out.push(*p);
        }
    }
}

/// Store-fetched value decoded once per key; cloned for all but the last
/// use, which moves the buffer
enum CachedValue {
    Decoded(Vec<u8>),
    /// Safe only after supersession or replayed owner TRUNCATE
    Missing,
    Mismatch,
}

/// Per-heap value resolution: on-demand store fetch (never all values at
/// once), decoded bytes tallied in `retained` for the leaf-permit shrink
struct ValueResolution<'a> {
    spool: Option<&'a BodySpoolFile>,
    xact_maps: &'a [&'a ChunkRefMap],
    resolver: &'a ToastResolver,
    source_lsn: u64,
    /// Pointer occurrences per key across both tuples, sizing cache
    /// retention (last use moves instead of cloning)
    uses: HashMap<(u32, u32), u32>,
    cache: HashMap<(u32, u32), CachedValue>,
    retained: usize,
}

impl ValueResolution<'_> {
    async fn resolve_tuple(
        &mut self,
        t: &mut crate::decode::heap_decoder::DecodedTuple,
        rel: &RelDescriptor,
    ) -> std::result::Result<(), XactBufferError> {
        for (idx, col) in t.columns.iter_mut().enumerate() {
            let Some(ColumnValue::ExternalToast(p)) = col else {
                continue;
            };
            // `ToastPointer: Copy` frees the borrow on `col` before reassign
            let p: ToastPointer = *p;
            let type_oid = rel.attributes.get(idx).map(|a| a.type_oid).unwrap_or(0);
            let key = (p.va_toastrelid, p.va_valueid);
            if let Some(v) = self.xact_maps.iter().find_map(|m| m.get(&key)) {
                match reassemble_value_ref(&p, self.spool, v)? {
                    Reassembled::Bytes(raw) => {
                        self.retained += raw.len();
                        *col = Some(detoasted_value(raw, type_oid));
                    }
                    // Disabled mode: surface the unresolvable value as
                    // NULL/default downstream (`append_default`), counted,
                    // never an error.
                    Reassembled::Missing if self.resolver.fill_on_miss() => {
                        self.resolver.note_filled_default();
                        *col = Some(ColumnValue::Null);
                    }
                    // In-xact chunks gapped or short: decode bug, surfaced loud
                    outcome => {
                        self.resolver.note_fetch_miss();
                        return Err(match outcome {
                            Reassembled::SizeMismatch { got, want } => {
                                XactBufferError::Detoast(format!(
                                    "toast value {}/{}: chunks sum to {got} bytes, pointer says {want}",
                                    p.va_toastrelid, p.va_valueid
                                ))
                            }
                            _ => XactBufferError::MissingToastChunk {
                                toast_relid: p.va_toastrelid,
                                value_id: p.va_valueid,
                                missing: first_missing_seq_ref(v),
                            },
                        });
                    }
                }
                continue;
            }
            *col = Some(self.resolve_store(&p, type_oid).await?);
        }
        Ok(())
    }

    /// Pre-window / bootstrap value whose chunks aren't in this xact,
    /// fetched assembled against the pointer's stored size
    async fn resolve_store(
        &mut self,
        p: &ToastPointer,
        type_oid: u32,
    ) -> std::result::Result<ColumnValue, XactBufferError> {
        if self.resolver.fill_on_miss() {
            // Disabled mode: no store to consult
            self.resolver.note_filled_default();
            return Ok(ColumnValue::Null);
        }
        let key = (p.va_toastrelid, p.va_valueid);
        if !self.cache.contains_key(&key) {
            let fetched = self
                .resolver
                .fetch_value(key.0, key.1, self.source_lsn, pointer_extsize(p))
                .await
                .map_err(|e| XactBufferError::Detoast(format!("toast store fetch: {e}")))?
                .expect("store checked via fill_on_miss");
            let cached = match fetched {
                FetchedValue::Assembled(stored) => CachedValue::Decoded(finish_value(p, stored)?),
                FetchedValue::Missing => CachedValue::Missing,
                FetchedValue::Mismatch { .. } => CachedValue::Mismatch,
            };
            self.cache.insert(key, cached);
        }
        match self.cache.get(&key).expect("just primed") {
            CachedValue::Missing => {
                self.resolver.note_filled_superseded();
                return Ok(ColumnValue::Null);
            }
            CachedValue::Mismatch => {
                self.resolver.note_filled_mismatch();
                return Ok(ColumnValue::Null);
            }
            CachedValue::Decoded(_) => {}
        }
        let uses = self.uses.get_mut(&key).expect("counted in detoast_heap");
        *uses -= 1;
        let raw = if *uses > 0 {
            let Some(CachedValue::Decoded(v)) = self.cache.get(&key) else {
                unreachable!("matched Decoded above")
            };
            v.clone()
        } else {
            let Some(CachedValue::Decoded(v)) = self.cache.remove(&key) else {
                unreachable!("matched Decoded above")
            };
            v
        };
        self.retained += raw.len();
        Ok(detoasted_value(raw, type_oid))
    }
}

/// First seq missing from a ref value's dense coverage, for the error
/// message. Only walked on the error path.
fn first_missing_seq_ref(v: &ValueRef) -> u32 {
    let mut next = v.run_chunks;
    for (&seq, _) in v.tail.range(v.run_chunks..) {
        if seq != next {
            return next;
        }
        next += 1;
    }
    next
}

/// Chunk coverage outcome, decompression failures remain errors
pub(crate) enum Reassembled {
    Bytes(Vec<u8>),
    Missing,
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
            return Ok(Reassembled::Missing);
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

/// Decodes `Route::ToDecoder` user-heap records into the xact buffer.
/// Toast-relation INSERTs (`rel.kind == 't'`) reinterpret as
/// [`ToastChunk`]; semantic errors absorb into [`DecoderStats`] rather
/// than poison the stream.
pub struct BufferingDecoderSink {
    log: Arc<DescriptorLog>,
    buffer: Arc<Mutex<XactBuffer>>,
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
    pub fn new(log: Arc<DescriptorLog>, buffer: Arc<Mutex<XactBuffer>>) -> Self {
        Self {
            log,
            buffer,
            stats: Arc::new(DecoderStats::default()),
            span_registry: None,
            config_schema: None,
        }
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
    /// into ([`XactBuffer::span_registry`]).
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
        let mut buf = self.buffer.lock().await;
        if buf.marker_lsn(rfn).is_some() {
            let raw = crate::xact::spill::RawRecord::from_parsed(
                &record.parsed,
                record.source_lsn,
                record.page_magic,
            );
            buf.stash_raw(xid, raw).await.map_err(SinkError::from)?;
            self.stats
                .toast_stash_buffered
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        } else {
            buf.track_unresolvable(xid, record.source_lsn, rfn);
            self.stats
                .catalog_not_found
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
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
            self.stats
                .skipped_op
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            return Ok(());
        };
        let xid = record.parsed.header.xact_id;
        let source_lsn = record.source_lsn;
        for relid in parsed.relids {
            // Same-xact CREATE + TRUNCATE: the rel's Added has no batch yet
            // (capture runs at commit) → NotCovered, nothing lives to wipe
            let rel = match self.log.descriptor_by_oid_at(relid, source_lsn) {
                LookupResult::Present(r) => r,
                _ => {
                    self.stats
                        .catalog_not_found
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    continue;
                }
            };
            // CH has no per-table internal toast; only user heap
            // ('r'/'p') TRUNCATE propagates
            if rel.kind != 'r' && rel.kind != 'p' {
                continue;
            }
            let decoded = DecodedHeap {
                rfn: rel.rfn,
                xid,
                source_lsn,
                op: HeapOp::Truncate,
                new: None,
                old: None,
            };
            self.stats.record(&decoded);
            let mut buf = self.buffer.lock().await;
            buf.on_heap(decoded).await.map_err(SinkError::from)?;
        }
        Ok(())
    }
}

impl RecordSink for BufferingDecoderSink {
    fn on_record<'a>(
        &'a mut self,
        record: &'a Record<'a>,
    ) -> Pin<Box<dyn std::future::Future<Output = std::result::Result<(), SinkError>> + Send + 'a>>
    {
        Box::pin(async move {
            let rm = record.parsed.header.resource_manager_id;
            // TRUNCATE rides Route::ToShadow (shadow replays it) but the
            // decoder still fans out per-relid HeapOp::Truncate for CH.
            // Handle before the Drop gate, regardless of filter score.
            if rm == RmId::Heap as u8 {
                let info_op =
                    record.parsed.header.info & crate::decode::heap_decoder::XLOG_HEAP_OPMASK;
                if info_op == crate::decode::heap_decoder::XLOG_HEAP_TRUNCATE {
                    return self.handle_truncate(record).await;
                }
            }
            // Main-fork creation marker, also Route::ToShadow: gates stash
            // admission and proves generation completeness for the rewrite
            // barrier (records on a filenode cannot precede its creation)
            if rm == RmId::Smgr as u8
                && record.parsed.header.info & 0xF0 == crate::filter::main_data::XLOG_SMGR_CREATE
                && let Some((rfn, fork)) =
                    crate::filter::main_data::parse_xl_smgr_create(&record.parsed.main_data)
                && fork == crate::filter::main_data::MAIN_FORKNUM
            {
                self.buffer.lock().await.note_smgr_create(
                    record.parsed.header.xact_id,
                    rfn,
                    record.source_lsn,
                );
                return Ok(());
            }
            if record.route != Route::ToDecoder {
                return Ok(());
            }
            if rm != RmId::Heap as u8 && rm != RmId::Heap2 as u8 {
                return Ok(());
            }
            let Some(rfn) = record.parsed.blocks.first().map(|b| b.header.location.rel) else {
                self.stats
                    .skipped_no_block
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                return Ok(());
            };
            let txn_xid = record.parsed.header.xact_id;
            // Known-invisible filenode for this xact (already stashed or
            // marker-registered): its pg_class row stays MVCC-invisible
            // until commit, so the log has no entry yet either
            if txn_xid != 0 && self.buffer.lock().await.is_stash_candidate(txn_xid, rfn) {
                return self.stash_invisible(record, rfn).await;
            }
            let sampled = self
                .span_registry
                .as_ref()
                .is_some_and(|r| r.is_sampled(txn_xid));
            let decode_parent = self
                .span_registry
                .as_ref()
                .and_then(|r| r.decode_parent(txn_xid));
            let _ = sampled;
            // Wait-free interval lookup: every record reaching this worker
            // already has log coverage (capture runs inside the boundary
            // hold, before successor bytes publish)
            let rel = match self.log.descriptor_at(rfn, record.source_lsn) {
                LookupResult::Present(rel) => rel,
                // Foreign db / rel that died before the coverage horizon:
                // counted row skip, never a stash or a fatal
                LookupResult::ForeignDb => {
                    self.stats
                        .catalog_not_found
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    return Ok(());
                }
                LookupResult::NotCovered
                    if record.source_lsn <= self.log.covered_through() || txn_xid == 0 =>
                {
                    self.stats
                        .catalog_not_found
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    return Ok(());
                }
                // Filenode invisible at record LSN: created by this
                // still-open xact (same-xact CREATE / TRUNCATE / rewrite
                // generation) or already superseded — resolve at commit
                LookupResult::NotCovered | LookupResult::Dropped => {
                    return self.stash_invisible(record, rfn).await;
                }
                // Rotated away: every record on this rfn precedes the
                // rotation (AccessExclusiveLock), so a Retired answer means
                // the row never outlives the commit — skip
                LookupResult::Retired => {
                    self.stats
                        .catalog_not_found
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    return Ok(());
                }
            };
            let decoded_set = {
                let _decode = decode_parent.as_ref().map(|p| {
                    tracing::info_span!(target: "walshadow::trace", parent: p, "decode").entered()
                });
                match decode_heap_record(&record.parsed, record.source_lsn, &rel) {
                    Ok(set) => set,
                    Err(e) => return Err(DecoderSinkError::from(e).into()),
                }
            };
            if decoded_set.is_empty() {
                self.stats
                    .skipped_op
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                return Ok(());
            }
            // Runtime-config overlay: config-table heap writes never reach CH.
            // Detect by resolved qualified name (rotation-proof — a rewritten
            // relfilenode still resolves to the same schema.name), interpret each
            // tuple into a ConfigEvent stamped (xid, source_lsn) so it drains in
            // WAL order and applies at its commit LSN (plan §2/§6).
            if let Some(schema) = self.config_schema.as_deref()
                && &*rel.rel_name.namespace == schema
                && let Some(kind) = ConfigTableKind::from_relname(&rel.rel_name.name)
            {
                let mut buf = self.buffer.lock().await;
                for decoded in &decoded_set {
                    if let Some(ev) = crate::runtime_config::interpret(kind, decoded, &rel) {
                        buf.on_config_event(decoded.xid, decoded.source_lsn, ev);
                    }
                }
                return Ok(());
            }
            let n_decoded = decoded_set.len();
            let buffer_span = trace_span!(sampled, "buffer", rows = n_decoded);
            // TID for the toast branch: single-tuple INSERT/DELETE only
            // (toast_save_datum never multi-inserts). blkno rides block ref
            // 0; offnum sits in xl_heap_insert[0..2] / xl_heap_delete[4..6].
            let tid = (rel.kind == 't' && n_decoded == 1)
                .then(|| toast_record_tid(&record.parsed))
                .flatten();
            async move {
                // Lock once per record (not per tuple); on_heap/on_toast_chunk
                // never touch the catalog, so no buffer→catalog inversion.
                let mut buf = self.buffer.lock().await;
                for decoded in decoded_set {
                    self.stats.record(&decoded);
                    if rel.kind == 't' {
                        let xid = decoded.xid;
                        if decoded.op == HeapOp::Delete
                            && let Some((blkno, offnum)) = tid
                        {
                            // heap_toast_delete's DELETE: TID-keyed. Buffered
                            // as a store tombstone row applied at commit drain
                            self.stats
                                .toast_chunk_deletes
                                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            buf.on_toast_delete(
                                crate::xact::spill::ToastDelete {
                                    toast_relid: rel.oid,
                                    blkno,
                                    offnum,
                                    source_lsn: decoded.source_lsn,
                                },
                                xid,
                            )
                            .await
                            .map_err(SinkError::from)?;
                        } else if decoded.op != HeapOp::Insert {
                            // Non-delete non-insert (TRUNCATE fan-out never
                            // reaches here — kind 't' is filtered there):
                            // nothing to apply against the store
                            self.stats
                                .toast_chunk_deletes
                                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        } else if let Some(chunk) = toast_chunk_from_decoded(decoded, &rel, tid) {
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
                            buf.on_toast_chunk(chunk, xid)
                                .await
                                .map_err(SinkError::from)?;
                        } else {
                            self.stats
                                .toast_chunks_malformed
                                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        }
                    } else {
                        buf.on_heap(decoded).await.map_err(SinkError::from)?;
                    }
                }
                Ok::<(), SinkError>(())
            }
            .instrument(buffer_span)
            .await?;
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
    let decoded_set = decode_heap_record(&rec, raw.source_lsn, rel)
        .map_err(|e| XactBufferError::Detoast(format!("stashed record decode: {e}")))?;
    let tid = (decoded_set.len() == 1)
        .then(|| toast_record_tid(&rec))
        .flatten();
    let mut out = Vec::with_capacity(decoded_set.len());
    for decoded in decoded_set {
        match decoded.op {
            HeapOp::Insert => {
                if let Some(chunk) = toast_chunk_from_decoded(decoded, rel, tid) {
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
    let Some((_, _, _, mut columns)) =
        crate::backfill::backup_page_walk::decode_on_page_tuple(tuple, rel)
    else {
        return Err(XactBufferError::Detoast(format!(
            "stashed image insert: malformed tuple at ({blkno},{offnum})"
        )));
    };
    let Some((value_id, chunk_seq, chunk_data)) =
        crate::decode::heap_decoder::take_toast_chunk_columns(&mut columns)
    else {
        tracing::warn!(
            target: "walshadow::xact_buffer",
            toast_relid = rel.oid,
            blkno,
            offnum,
            "stashed image tuple not a toast chunk shape",
        );
        return Ok(Vec::new());
    };
    Ok(vec![StashedToastOp::Chunk(ToastChunk {
        toast_relid: rel.oid,
        value_id,
        chunk_seq,
        source_lsn: raw.source_lsn,
        blkno,
        offnum,
        chunk_data: bytes::Bytes::from(chunk_data),
    })])
}

/// Repack a TOAST table INSERT into a [`ToastChunk`]; `None` for shapes
/// that don't fit.
///
/// Keyed on the toast rel's pg_class OID ([`RelDescriptor::oid`]), not
/// `rel_node`: the referring tuple's `va_toastrelid` is the OID. They
/// diverge after `VACUUM FULL` / `CLUSTER` on the toast rel.
fn toast_chunk_from_decoded(
    mut d: DecodedHeap,
    rel: &RelDescriptor,
    tid: Option<(u32, u16)>,
) -> Option<ToastChunk> {
    if d.op != HeapOp::Insert {
        return None;
    }
    let (value_id, chunk_seq, chunk_data) =
        crate::decode::heap_decoder::take_toast_chunk_columns(&mut d.new.as_mut()?.columns)?;
    let (blkno, offnum) = tid.unwrap_or((0, 0));
    Some(ToastChunk {
        toast_relid: rel.oid,
        value_id,
        chunk_seq,
        source_lsn: d.source_lsn,
        blkno,
        offnum,
        chunk_data: bytes::Bytes::from(chunk_data),
    })
}

#[cfg(test)]
mod tests {
    //! Catalog-free paths only. Commit-drain + detoast +
    //! `XactRecordSink::commit` live in `tests/xact_buffer.rs` against a
    //! real shadow PG: they need `ShadowCatalog::relation_at`, and a
    //! unit-test stub catalog would duplicate the production cache.

    use super::*;
    use crate::decode::heap_decoder::{DecodedTuple, HeapOp, VARLENA_EXTSIZE_BITS};
    use tempfile::tempdir;
    use walrus::pg::walparser::RelFileNode;

    #[test]
    fn xact_buffer_config_new_uses_default_max() {
        let c = XactBufferConfig::new(PathBuf::from("/tmp/walshadow-test-spill"));
        assert_eq!(c.xact_buffer_max, DEFAULT_XACT_BUFFER_MAX);
    }

    #[test]
    fn txn_span_registry_full_lifecycle() {
        crate::ops::trace::set_sample_ratio(1.0);
        let reg = TxnSpanRegistry::new();

        reg.open(0, 1);
        reg.note_shipped(0);
        assert!(!reg.note_popped(0));
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

    #[test]
    fn xact_buffer_exposes_span_registry() {
        crate::ops::trace::set_sample_ratio(1.0);
        let tmp = tempdir().unwrap();
        let buf = XactBuffer::new(XactBufferConfig::new(tmp.path().to_path_buf())).unwrap();
        let reg = buf.span_registry();
        reg.open(7, 1);
        assert!(reg.is_sampled(7));
    }

    fn cfg(dir: PathBuf) -> XactBufferConfig {
        XactBufferConfig {
            xact_buffer_max: 1024,
            ..XactBufferConfig::new(dir)
        }
    }

    fn heap_with_value(xid: u32, lsn: u64, payload_size: usize) -> DecodedHeap {
        DecodedHeap {
            rfn: RelFileNode {
                spc_node: 1663,
                db_node: 5,
                rel_node: 16385,
            },
            xid,
            source_lsn: lsn,
            op: HeapOp::Insert,
            new: Some(DecodedTuple {
                columns: vec![Some(ColumnValue::Bytea(vec![0u8; payload_size]))],
                partial: false,
            }),
            old: None,
        }
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
        let snap = b.inflight_snapshot();
        assert_eq!(snap.len(), 1);
        let e = &snap[0];
        assert_eq!(e.xid, 7);
        assert_eq!(e.first_lsn, 100);
        assert_eq!(e.last_lsn, 100);
        assert_eq!(e.heap_count, 1);
        assert_eq!(e.chunk_count, 0);
        assert!(e.in_mem_bytes > 0);
        assert!(
            !e.spilled,
            "16-byte tuple stays in memory under the 1 KiB cap"
        );
        assert_eq!(e.rels, "5/16385");
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
        match s {
            SinkError::Other(msg) => assert!(msg.contains("boom"), "{msg}"),
            other => panic!("expected SinkError::Other, got {other:?}"),
        }
        let d: DecoderSinkError = XactBufferError::Observer("boom".into()).into();
        match d {
            DecoderSinkError::Observer(msg) => assert!(msg.contains("boom"), "{msg}"),
            other => panic!("expected DecoderSinkError::Observer, got {other:?}"),
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn abort_drops_xact_and_unlinks_spill() {
        let tmp = tempdir().unwrap();
        let mut b = XactBuffer::new(cfg(tmp.path().to_path_buf())).unwrap();
        for i in 0..10 {
            b.on_heap(heap_with_value(11, 100 + i, 256)).await.unwrap();
        }
        assert!(b.stats().spill_xacts_active >= 1, "spill must engage");
        let spill_dir = tmp.path().to_path_buf();
        let before: Vec<_> = std::fs::read_dir(&spill_dir).unwrap().collect();
        assert!(!before.is_empty(), "spill file present");
        b.abort(11, 200, &[]).await.unwrap();
        let after: Vec<_> = std::fs::read_dir(&spill_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_name().to_string_lossy().starts_with("xid-"))
            .collect();
        assert!(after.is_empty(), "abort must remove spill file");
        assert_eq!(b.stats().aborted_xacts_total, 1);
        assert_eq!(b.stats().spill_xacts_active, 0);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn advance_idle_moves_drain_lsn_monotonically() {
        let tmp = tempdir().unwrap();
        let mut b = XactBuffer::new(cfg(tmp.path().to_path_buf())).unwrap();
        b.advance_idle(100);
        assert_eq!(b.stats().drain_lsn, 100);
        // Regressing input never lowers the field
        b.advance_idle(50);
        assert_eq!(b.stats().drain_lsn, 100);
        // Inflight xact parks the advance
        b.on_heap(heap_with_value(7, 150, 16)).await.unwrap();
        b.advance_idle(300);
        assert_eq!(b.stats().drain_lsn, 100);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn resume_safe_lsn_floors_at_undurable_xacts() {
        let tmp = tempdir().unwrap();
        let mut b = XactBuffer::new(cfg(tmp.path().to_path_buf())).unwrap();
        // Use acknowledgment when no transactions remain
        assert_eq!(b.resume_safe_lsn(500), 500);
        // Open transactions lower resume point
        b.on_heap(heap_with_value(7, 100, 16)).await.unwrap();
        b.on_heap(heap_with_value(8, 150, 16)).await.unwrap();
        assert_eq!(b.resume_safe_lsn(500), 100);
        // Keep floor while committed slices remain undurable
        let drain = b.drain_committed(8, 0, 200, &[], false).await.unwrap();
        assert!(drain.had_states);
        drop(drain);
        assert_eq!(b.resume_safe_lsn(180), 100, "open xid 7 still floors");
        b.abort(7, 210, &[]).await.unwrap();
        assert_eq!(b.resume_safe_lsn(180), 150, "xid 8 undurable at ack 180");
        // Drop floor once acknowledgment reaches commit
        assert_eq!(b.resume_safe_lsn(200), 200);
        // Ignore commits without buffered rows
        let empty = b.drain_committed(9, 0, 300, &[], false).await.unwrap();
        assert!(!empty.had_states);
        assert_eq!(b.resume_safe_lsn(300), 300);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn resume_safe_lsn_uses_tree_min_over_subxacts() {
        let tmp = tempdir().unwrap();
        let mut b = XactBuffer::new(cfg(tmp.path().to_path_buf())).unwrap();
        // Use earliest record across transaction tree
        b.on_heap(heap_with_value(20, 400, 16)).await.unwrap();
        b.on_heap(heap_with_value(21, 420, 16)).await.unwrap();
        let drain = b.drain_committed(21, 0, 450, &[20], false).await.unwrap();
        drop(drain);
        assert_eq!(b.resume_safe_lsn(440), 400);
        assert_eq!(b.resume_safe_lsn(450), 450);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn abort_unknown_xid_counts() {
        let tmp = tempdir().unwrap();
        let mut b = XactBuffer::new(cfg(tmp.path().to_path_buf())).unwrap();
        b.abort(101, 0, &[]).await.unwrap();
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
        b.abort(1, 300, &[]).await.unwrap();
        b.abort(2, 300, &[]).await.unwrap();
    }

    /// Aborts must advance `drain_lsn`, else an all-abort workload never
    /// advances the slot (the ack side rides the reorder's abort seq)
    #[tokio::test(flavor = "current_thread")]
    async fn abort_advances_drain_lsn() {
        let tmp = tempdir().unwrap();
        let mut b = XactBuffer::new(cfg(tmp.path().to_path_buf())).unwrap();
        b.on_heap(heap_with_value(7, 100, 16)).await.unwrap();
        b.abort(7, 0x4000, &[]).await.unwrap();
        assert_eq!(b.stats().drain_lsn, 0x4000);
        // Lower-LSN abort must not regress the monotonic mark
        b.abort(99, 0x100, &[]).await.unwrap();
        assert_eq!(b.stats().drain_lsn, 0x4000);
        b.abort(101, 0x8000, &[]).await.unwrap();
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
    fn toast_chunk_from_decoded_recognises_three_col_shape() {
        use crate::decode::heap_decoder::{DecodedTuple, HeapOp};
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
            kind: 't',
            persistence: 'p',
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
                    type_align: 'i',
                    type_storage: 'p',
                    missing_text: None,
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
                    type_align: 'i',
                    type_storage: 'p',
                    missing_text: None,
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
                    type_align: 'i',
                    type_storage: 'x',
                    missing_text: None,
                },
            ],
        };
        let d = DecodedHeap {
            rfn: rel.rfn,
            xid: 5,
            source_lsn: 0x1234,
            op: HeapOp::Insert,
            new: Some(DecodedTuple {
                columns: vec![
                    Some(ColumnValue::Oid(55)),
                    Some(ColumnValue::Int4(2)),
                    Some(ColumnValue::Bytea(b"hello".to_vec())),
                ],
                partial: false,
            }),
            old: None,
        };
        let chunk = toast_chunk_from_decoded(d.clone(), &rel, Some((7, 3)))
            .expect("recognised toast shape");
        assert_eq!(chunk.toast_relid, 99); // pg_class.oid, not rel_node
        assert_eq!(chunk.value_id, 55);
        assert_eq!(chunk.chunk_seq, 2);
        assert_eq!(&chunk.chunk_data[..], b"hello");
        assert_eq!((chunk.blkno, chunk.offnum), (7, 3));
        let mut d2 = d.clone();
        d2.op = HeapOp::Update;
        assert!(toast_chunk_from_decoded(d2, &rel, None).is_none());
        let mut d3 = d.clone();
        d3.new.as_mut().unwrap().columns.pop();
        assert!(toast_chunk_from_decoded(d3, &rel, None).is_none());
    }

    #[test]
    fn detoasted_value_routes_tier3_like_inline() {
        use crate::schema::{BYTEAOID, JSONBOID, TEXTOID};
        assert!(
            matches!(detoasted_value(b"raw".to_vec(), BYTEAOID), ColumnValue::Bytea(b) if b == b"raw")
        );
        assert!(
            matches!(detoasted_value(b"hi".to_vec(), TEXTOID), ColumnValue::Text(s) if s == "hi")
        );
        // Tier 3 (jsonb) lands as PgPending carrying the body so the oracle
        // resolves it like an inline jsonb, not Unsupported
        match detoasted_value(b"\x01body".to_vec(), JSONBOID) {
            ColumnValue::PgPending { type_oid, raw } => {
                assert_eq!(type_oid, JSONBOID);
                assert_eq!(raw, b"\x01body");
            }
            other => panic!("expected PgPending, got {other:?}"),
        }
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
            kind: 'r',
            persistence: 'p',
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
                type_align: 'i',
                type_storage: 'x',
                missing_text: None,
            }],
        }
    }

    fn toast_ptr_tuple(value_id: u32) -> DecodedTuple {
        DecodedTuple {
            columns: vec![Some(ColumnValue::ExternalToast(ToastPointer {
                va_rawsize: 8,
                va_extinfo: 4, // 4 bytes, uncompressed
                va_valueid: value_id,
                va_toastrelid: 16500,
            }))],
            partial: false,
        }
    }

    /// Ref map of memory bodies for one value's `(seq, body)` chunks
    fn mem_refs(key: (u32, u32), chunks: &[(u32, &'static [u8])]) -> ChunkRefMap {
        let mut map = ChunkRefMap::new();
        for &(seq, body) in chunks {
            let body = Body::Mem(bytes::Bytes::from_static(body));
            match map.entry(key) {
                std::collections::hash_map::Entry::Occupied(mut o) => {
                    o.get_mut().push(seq, body);
                }
                std::collections::hash_map::Entry::Vacant(v) => {
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
        // Uncompressed: leaf need = extsize
        assert_eq!(check_value_caps(&[ptr(104, 100)], 1000).unwrap(), 100);
        // Compressed (method bits set): extsize + rawsize
        let compressed = 80u32 | (1 << VARLENA_EXTSIZE_BITS);
        assert_eq!(
            check_value_caps(&[ptr(104, compressed)], 1000).unwrap(),
            180
        );
        // Decode target over cap: typed error before allocation
        let err = check_value_caps(&[ptr(2000, 100)], 1000).unwrap_err();
        assert!(matches!(
            err,
            ToastValueError::ValueTooLarge {
                rawsize: 1996,
                max: 1000
            }
        ));
        // Stored form over cap trips too (caps ChunkAssembler expected_size)
        let err = check_value_caps(&[ptr(104, 1500)], 1000).unwrap_err();
        assert!(matches!(err, ToastValueError::ValueTooLarge { .. }));
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
                std::collections::hash_map::Entry::Occupied(mut o) => {
                    o.get_mut().push(seq, r);
                }
                std::collections::hash_map::Entry::Vacant(v) => {
                    v.insert(ValueRef::new(seq, r));
                }
            }
        }
        w.flush().unwrap();
        (w, map)
    }

    /// One-key resolution scope with cache pre-seeded to a store outcome
    fn seeded<'a>(
        resolver: &'a ToastResolver,
        spool: Option<&'a BodySpoolFile>,
        xact_maps: &'a [&'a ChunkRefMap],
        cache: HashMap<(u32, u32), CachedValue>,
    ) -> ValueResolution<'a> {
        ValueResolution {
            spool,
            xact_maps,
            resolver,
            source_lsn: 0,
            uses: HashMap::from([((16500u32, 55u32), 1)]),
            cache,
            retained: 0,
        }
    }

    /// Miss policy split: in-xact gap stays a hard error; a store-side miss
    /// (key absent from every xact map) NULL-fills + counts superseded;
    /// disabled mode NULL-fills + counts default.
    #[tokio::test(flavor = "current_thread")]
    async fn resolve_tuple_splits_in_xact_gap_from_store_miss() {
        let rel = bytea_rel();
        let tmp = tempdir().unwrap();
        let stats = Arc::new(crate::emit::ch_emitter::EmitterStats::default());
        let store_resolver =
            ToastResolver::with_store(Arc::new(crate::toast::MemChunkStore::new()), stats.clone());

        let key = (16500u32, 55u32);

        // Store miss: no xact map holds the key → superseded fill
        let mut t = toast_ptr_tuple(55);
        let cache = HashMap::from([(key, CachedValue::Missing)]);
        let mut r = seeded(&store_resolver, None, &[], cache);
        r.resolve_tuple(&mut t, &rel).await.unwrap();
        assert_eq!(t.columns[0], Some(ColumnValue::Null));
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
            .resolve_tuple(&mut t, &rel)
            .await
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

        // In-xact memory refs resolve without spool
        let whole = mem_refs(key, &[(0, b"ab"), (1, b"cd")]);
        let maps = [&whole];
        let mut t = toast_ptr_tuple(55);
        let mut r = seeded(&store_resolver, None, &maps, HashMap::new());
        r.resolve_tuple(&mut t, &rel).await.unwrap();
        assert_eq!(t.columns[0], Some(ColumnValue::Bytea(b"abcd".to_vec())));
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
        .resolve_tuple(&mut t, &rel)
        .await
        .unwrap();
        assert_eq!(t.columns[0], Some(ColumnValue::Bytea(b"abcd".to_vec())));

        // Store-resolved hit lands assembled bytes
        let mut t = toast_ptr_tuple(55);
        let cache = HashMap::from([(key, CachedValue::Decoded(b"abcd".to_vec()))]);
        let mut r = seeded(&store_resolver, None, &[], cache);
        r.resolve_tuple(&mut t, &rel).await.unwrap();
        assert_eq!(t.columns[0], Some(ColumnValue::Bytea(b"abcd".to_vec())));
        assert_eq!(r.retained, 4);
        assert!(r.cache.is_empty(), "last use moves the buffer out");

        // Store-side run deviation (partial merge collapse): fills,
        // counted mismatch — not superseded, not a hard error
        let mut t = toast_ptr_tuple(55);
        let cache = HashMap::from([(key, CachedValue::Mismatch)]);
        seeded(&store_resolver, None, &[], cache)
            .resolve_tuple(&mut t, &rel)
            .await
            .unwrap();
        assert_eq!(t.columns[0], Some(ColumnValue::Null));
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
            .resolve_tuple(&mut t, &rel)
            .await
            .expect_err("in-xact size mismatch surfaces");
        assert!(matches!(err, XactBufferError::Detoast(_)));

        // Disabled mode: any miss NULL-fills as before, counted default
        let disabled = ToastResolver::disabled();
        let mut t = toast_ptr_tuple(55);
        seeded(&disabled, None, &[], HashMap::new())
            .resolve_tuple(&mut t, &rel)
            .await
            .unwrap();
        assert_eq!(t.columns[0], Some(ColumnValue::Null));
    }

    /// Store fetch runs once per key: decode once, clone for earlier
    /// duplicate uses (old/new tuple reuse), move the buffer on the last
    #[tokio::test(flavor = "current_thread")]
    async fn resolve_store_fetches_once_and_moves_last_use() {
        use crate::toast::{ChunkStore, MemChunkStore, ToastRow};
        let store = Arc::new(MemChunkStore::new());
        store
            .put(&[ToastRow {
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
        let mut r = ValueResolution {
            spool: None,
            xact_maps: &[],
            resolver: &resolver,
            source_lsn: 10,
            uses: HashMap::from([((16500u32, 55u32), 2)]),
            cache: HashMap::new(),
            retained: 0,
        };
        let first = r.resolve_store(&p, 17).await.unwrap();
        assert!(!r.cache.is_empty(), "pending duplicate use stays cached");
        let second = r.resolve_store(&p, 17).await.unwrap();
        assert_eq!(first, ColumnValue::Bytea(b"abcd".to_vec()));
        assert_eq!(second, ColumnValue::Bytea(b"abcd".to_vec()));
        assert!(r.cache.is_empty(), "last use moves the buffer out");
        assert_eq!(r.retained, 8, "both uses retain their copy");
        assert_eq!(
            stats
                .toast_values_fetched
                .load(std::sync::atomic::Ordering::Relaxed),
            1,
            "one fetch serves both uses"
        );
    }

    // ── subxact tracking ──────────────────────────────────────────────

    #[test]
    fn subxact_tracker_round_trip() {
        let mut t = SubxactTracker::new();
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
        assert_eq!(t.subxids_of(30), vec![20]);
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
        assert_eq!(subs, vec![0x55, 0x66]);
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
        assert_eq!(p.subxacts, vec![0xAA, 0xBB, 0xCC]);
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
        b.abort(300, 0x500, &[301, 302]).await.unwrap();
        assert!(b.active_xids().is_empty());
        // One bump per terminator record, not per subxid
        assert_eq!(b.stats().aborted_xacts_total, 1);
    }

    // ── drain streaming ───────────────────────────────────────────────

    fn chunk(value_id: u32, seq: u32, lsn: u64, body: &[u8]) -> ToastChunk {
        ToastChunk {
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
        }
    }

    /// Interleave a batch's events at their local indices with its heaps:
    /// `e<oid>` / `h<lsn>` labels for order assertions.
    fn flatten_batch(batch: &DrainedBatch) -> Vec<String> {
        let label = |e: &DrainEntry| match e {
            DrainEntry::Catalog(SchemaEvent::Dropped { oid, .. }) => format!("e{oid}"),
            other => panic!("unexpected event {other:?}"),
        };
        let mut out = Vec::new();
        let mut ev = 0usize;
        for (i, h) in batch.heaps.iter().enumerate() {
            while ev < batch.ordered_events.len() && batch.ordered_events[ev].heap_idx <= i {
                out.push(label(&batch.ordered_events[ev].event));
                ev += 1;
            }
            out.push(format!("h{}", h.source_lsn));
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
        let mut drain = b.drain_committed(1, 42, 0x2000, &[], false).await.unwrap();
        let mut order: Vec<String> = Vec::new();
        while let Some(batch) = drain.next_batch(8, usize::MAX, None).await.unwrap() {
            order.extend(flatten_batch(&batch));
            if batch.is_final {
                break;
            }
        }
        assert_eq!(order, vec!["e7", "h120", "e9"]);
        drain.finish().await.unwrap();
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

        let mut drain = b.drain_committed(1, 42, 0x2000, &[2], false).await.unwrap();
        assert!(drain.had_states);
        let mut order: Vec<String> = Vec::new();
        let mut finals = Vec::new();
        while let Some(batch) = drain.next_batch(2, usize::MAX, None).await.unwrap() {
            order.extend(flatten_batch(&batch));
            finals.push(batch.is_final);
            if batch.is_final {
                break;
            }
        }
        assert_eq!(
            order,
            ["h100", "h110", "e7", "h120", "h130", "h140", "e8"]
                .map(str::to_string)
                .to_vec(),
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

        let mut drain = b.drain_committed(9, 0, 0x1000, &[], true).await.unwrap();
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
        let Reassembled::Bytes(raw) = reassemble_value_ref(&p, spool, v).unwrap() else {
            panic!("value visible");
        };
        assert_eq!(raw, b"abcd");
        drain.finish().await.unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn drain_batch_seals_row_cursors_at_events_and_truncates() {
        let tmp = tempdir().unwrap();
        let mut b = XactBuffer::new(cfg(tmp.path().to_path_buf())).unwrap();
        b.on_toast_chunk(chunk(55, 0, 100, b"ab"), 9).await.unwrap();
        let mut trunc = heap_with_value(9, 110, 16);
        trunc.op = HeapOp::Truncate;
        trunc.new = None;
        b.on_heap(trunc).await.unwrap();
        b.on_toast_chunk(chunk(56, 0, 120, b"cd"), 9).await.unwrap();
        b.on_schema_event(9, 130, dropped_event(7));
        b.on_toast_chunk(chunk(57, 0, 140, b"ef"), 9).await.unwrap();
        b.on_heap(heap_with_value(9, 150, 16)).await.unwrap();

        let mut drain = b.drain_committed(9, 0, 0x1000, &[], true).await.unwrap();
        let batch = drain
            .next_batch(usize::MAX, usize::MAX, None)
            .await
            .unwrap()
            .unwrap();
        assert!(batch.is_final);
        assert_eq!(batch.new_rows.len(), 3);
        assert_eq!(batch.truncate_rows, vec![1]);
        assert_eq!(batch.ordered_events.len(), 1);
        let ev = &batch.ordered_events[0];
        assert_eq!((ev.heap_idx, ev.row_idx), (1, 2));
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
        let mut drain = b.drain_committed(3, 0, 0x5000, &[], false).await.unwrap();
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
        drain.finish().await.unwrap();
        assert!(spill_files(tmp.path()).is_empty());
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
        let mut drain = b.drain_committed(4, 0, 0x9000, &[], false).await.unwrap();
        let mut rows = 0usize;
        while let Some(batch) = drain.next_batch(4, usize::MAX, None).await.unwrap() {
            rows += batch.heaps.len();
            assert!(
                b.drain_resident_bytes() < total / 4,
                "resident {} vs xact {total}",
                b.drain_resident_bytes(),
            );
            if batch.is_final {
                break;
            }
        }
        assert_eq!(rows as u64, n);
        assert!(b.drain_resident_peak() > 0, "gauge saw the merge heads");
        assert!(
            b.drain_resident_peak() < total / 4,
            "peak {} vs xact {total}",
            b.drain_resident_peak(),
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
        let mut drain = b.drain_committed(9, 0, 0x9000, &[], true).await.unwrap();
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
        let mut drain = b.drain_committed(9, 0, 0x9000, &[], true).await.unwrap();
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
        let Reassembled::Bytes(raw) = reassemble_value_ref(&p(50), spool, mem_v).unwrap() else {
            panic!("mem value visible");
        };
        assert_eq!(raw.len(), 512);
        let file_v = last
            .chunks
            .iter()
            .find_map(|g| g.get(&(16400, 50 + n - 1)))
            .unwrap();
        assert_eq!(
            (file_v.run_chunks, file_v.run.len, file_v.tail.len()),
            (1, 512, 0)
        );
        let Reassembled::Bytes(raw) = reassemble_value_ref(&p(50 + n - 1), spool, file_v).unwrap()
        else {
            panic!("file value visible");
        };
        assert_eq!(raw.len(), 512);
        // Mirror row refs materialize from the same spool
        let rows = &held.last().unwrap().new_rows;
        for r in rows.iter() {
            assert_eq!(r.materialize(rows.spool()).unwrap().chunk_data.len(), 512);
        }
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
        let Reassembled::Bytes(raw) = reassemble_value_ref(&p(50 + n - 1), spool, file_v).unwrap()
        else {
            panic!("read-after-unlink via open fd");
        };
        assert_eq!(raw.len(), 512);
        held.clear();
        assert_eq!(b.drain_chunk_resident_bytes(), 0);
        assert_eq!(b.drain_row_resident_bytes(), 0);
        assert_eq!(b.drain_resident_bytes(), 0);
        assert_eq!(b.toast_spool_bytes(), 0, "gauge drops with the last reader");
    }

    /// Metadata cap: typed non-retryable error before further allocation
    #[tokio::test(flavor = "current_thread")]
    async fn drain_fails_loud_past_index_meta_cap() {
        let tmp = tempdir().unwrap();
        let mut c = cfg(tmp.path().to_path_buf());
        // Chunk + row ref cost 2×CHUNK_REF_META per fold; second fold trips
        c.toast_index_mem_max = 3 * CHUNK_REF_META;
        let mut b = XactBuffer::new(c).unwrap();
        b.on_toast_chunk(chunk(50, 0, 100, b"aa"), 9).await.unwrap();
        b.on_toast_chunk(chunk(51, 0, 102, b"bb"), 9).await.unwrap();
        b.on_heap(heap_with_value(9, 110, 16)).await.unwrap();
        let mut drain = b.drain_committed(9, 0, 0x1000, &[], true).await.unwrap();
        let Err(err) = drain.next_batch(usize::MAX, usize::MAX, None).await else {
            panic!("cap breach surfaces");
        };
        assert!(matches!(
            err,
            XactBufferError::ToastIndexOverflow { max, .. } if max == 3 * CHUNK_REF_META
        ));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn drain_committed_unknown_xid_yields_no_batches() {
        let tmp = tempdir().unwrap();
        let mut b = XactBuffer::new(cfg(tmp.path().to_path_buf())).unwrap();
        let mut drain = b.drain_committed(999, 0, 0x100, &[], false).await.unwrap();
        assert!(!drain.had_states);
        assert!(drain.next_batch(10, 1000, None).await.unwrap().is_none());
        drain.finish().await.unwrap();
        assert_eq!(b.stats().commits_unknown_xid, 1);
        assert_eq!(b.stats().drain_lsn, 0x100);
    }
}
