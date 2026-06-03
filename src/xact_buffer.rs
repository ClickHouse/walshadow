//! Per-xid xact buffer + TOAST reassembly.
//!
//! Sits between [`BufferingDecoderSink`]'s per-record output and the
//! downstream emitter. Holds every
//! [`DecodedHeap`] for a given xid plus every TOAST chunk
//! `(toast_relid, value_id, chunk_seq)` until the matching
//! `XLOG_XACT_COMMIT` / `XLOG_XACT_ABORT` lands. Commit drains in WAL
//! order with each `ColumnValue::ExternalToast` substituted by its
//! reassembled `Bytea` / `Text` payload; abort drops the buffer plus
//! any spill file.
//!
//! ## Why bundle TOAST chunks into the same buffer as heap tuples
//!
//! PG's `toast_save_datum` writes chunk INSERTs in the same xact as
//! the referring tuple. Keeping both inside one [`XactState`] gives:
//!
//! * Single key (`xid`) for spill, eviction, drain, abort cleanup.
//! * WAL-order natural — heap and chunk records interleave on disk;
//!   sequential drain matches what downstream `ReplacingMergeTree`
//!   expects.
//! * Chunks arriving before / after the referring tuple are a
//!   non-issue: detoast happens at drain, by which point every chunk
//!   for every value in the xact is already buffered.
//!
//! Cross-xact chunks would matter only for PG's `streaming=on` mode,
//! which walshadow does not implement (streaming mid-xact is
//! deferred).
//!
//! ## Catalog access at drain
//!
//! Detoasting needs the original column's type OID to decide
//! `Bytea` vs `Text`. Drain calls
//! [`ShadowCatalog::relation_at`](crate::shadow_catalog::ShadowCatalog::relation_at)
//! on each heap whose `tuple_needs_detoast` returns true; the
//! catalog's own LRU caches the descriptor across repeat lookups,
//! so a buffer-internal cache would just duplicate that surface.
//! Heaps without TOAST columns never hit the catalog at drain.
//!
//! ## Spill policy
//!
//! Once `memory_used > config.xact_buffer_max`, [`XactBuffer`] picks
//! the largest in-memory xact and flushes its entries to a
//! [`SpillWriter`](crate::spill::SpillWriter) under `spill_dir`. The
//! xact stays "open" — subsequent records keep appending to the spill
//! file. Mirrors PG `ReorderBufferLargestTXN` (logical-decoding's same
//! problem in `~/s/postgresql/src/backend/replication/logical/reorderbuffer.c`).
//!
//! Drain pass: spilled entries first (older in WAL order), then
//! in-memory entries (newer). Eviction always flushes from the front
//! of `in_mem`, so the invariant "spilled is older than in-mem" holds.
//!
//! Spill-to-ClickHouse is reserved as design space (Option B) —
//! config knob, schema, and code path are left for a
//! follow-up when a diskless walshadow operator asks. v1 is
//! local-disk-only.
//!
//! ## Status counters
//!
//! `xact_buffer_active`, `xacts_buffered_total`, `spill_bytes_active`,
//! `spill_xacts_active`, `spill_evictions_total`,
//! `aborted_xacts_total`, `committed_xacts_total`. Surfaced via
//! [`XactBufferStats`] and rendered in the daemon's status line by
//! [`XactBufferStats::summary`].

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::Arc;

use thiserror::Error;
use tokio::sync::Mutex;
use wal_rs::pg::walparser::RmId;

use crate::decoder_sink::{DecoderSinkError, DecoderStats, TupleObserver};
use crate::filter::Decision;
use crate::heap_decoder::{
    ColumnValue, CommittedTuple, DecodedHeap, HeapOp, ToastPointer, decode_heap_record,
};
use crate::shadow_catalog::{CatalogError, RelDescriptor, SchemaEvent, ShadowCatalog};
use crate::spill::{SpillEntry, SpillError, SpillStore, SpillWriter, ToastChunk};
use crate::wal_stream::{Record, RecordSink, SinkError};

use std::pin::Pin;

/// Default in-memory budget: matches PG's `logical_decoding_work_mem`
/// default (64 MiB, `~/s/postgresql/src/backend/utils/misc/guc_tables.c`
/// L2611). Large enough that small xacts never spill, small enough
/// that one runaway xact doesn't OOM the daemon.
pub const DEFAULT_XACT_BUFFER_MAX: usize = 64 * 1024 * 1024;

/// XLOG_XACT info-op constants. Mirror PG `access/xact.h` L169-179.
const XLOG_XACT_OPMASK: u8 = 0x70;
const XLOG_XACT_COMMIT: u8 = 0x00;
const XLOG_XACT_ABORT: u8 = 0x20;
const XLOG_XACT_COMMIT_PREPARED: u8 = 0x30;
const XLOG_XACT_ABORT_PREPARED: u8 = 0x40;
const XLOG_XACT_ASSIGNMENT: u8 = 0x50;
/// `xinfo` field follows the leading `xact_time` when this bit is set
/// in the record header's `info`. `access/xact.h` L182.
const XLOG_XACT_HAS_INFO: u8 = 0x80;

/// `xinfo` bits driving xl_xact_commit / xl_xact_abort tail layout.
/// `access/xact.h` L188-196. The commit/abort parser only consumes
/// `HAS_SUBXACTS`; remaining flags drive skip-walk.
const XACT_XINFO_HAS_DBINFO: u32 = 1 << 0;
const XACT_XINFO_HAS_SUBXACTS: u32 = 1 << 1;
const XACT_XINFO_HAS_RELFILELOCATORS: u32 = 1 << 2;
const XACT_XINFO_HAS_INVALS: u32 = 1 << 3;
const XACT_XINFO_HAS_TWOPHASE: u32 = 1 << 4;
const XACT_XINFO_HAS_ORIGIN: u32 = 1 << 5;
const XACT_XINFO_HAS_GID: u32 = 1 << 7;
const XACT_XINFO_HAS_DROPPED_STATS: u32 = 1 << 8;

/// Maps PG subxact xids to their top-level xid. Built from
/// `XLOG_XACT_ASSIGNMENT` (info `0x50`) records arriving on the xact
/// resource manager. The tracker keeps both directions so
/// `forget_tree` runs O(k) over actual children rather than scanning
/// every entry in `parent`.
///
/// Tracker is a hint, not a correctness gate: PG batches the first 64
/// subxacts under `PGPROC_MAX_CACHED_SUBXIDS` and emits no assignment
/// for that window. The authoritative list arrives inline with
/// commit / abort records; tracker drives early eviction policy only.
#[derive(Debug, Default)]
pub struct SubxactTracker {
    parent: HashMap<u32, u32>,
    children: HashMap<u32, Vec<u32>>,
}

impl SubxactTracker {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record that every `subxid` belongs to `top_xid`. Repeated
    /// assignments for the same subxid keep the most recent top.
    pub fn assign(&mut self, top_xid: u32, subxids: &[u32]) {
        if subxids.is_empty() {
            return;
        }
        // Two-phase to avoid holding a `&mut Vec` from `children[top]`
        // while also walking `children[prev_top]` on retargets.
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

    /// Resolve `xid` to its top. Unmapped xids return themselves —
    /// matches PG's "subxact's top is itself when no ASSIGNMENT
    /// landed yet" semantics so callers can treat top + sub uniformly.
    pub fn top_for(&self, xid: u32) -> u32 {
        self.parent.get(&xid).copied().unwrap_or(xid)
    }

    /// Drop every mapping rooted at `top_xid`. Called once the top
    /// commits or aborts and the tracker's hint is no longer useful.
    pub fn forget_tree(&mut self, top_xid: u32) {
        if let Some(subs) = self.children.remove(&top_xid) {
            for s in subs {
                self.parent.remove(&s);
            }
        }
        // top_xid might itself be a subxact in another tree (shouldn't
        // happen on the commit / abort path but cheap to scrub).
        self.parent.remove(&top_xid);
    }

    /// Return the recorded subxids for `top_xid`. Caller's slice for
    /// drain ordering; tracker keeps its own buckets intact.
    pub fn subxids_of(&self, top_xid: u32) -> Vec<u32> {
        self.children.get(&top_xid).cloned().unwrap_or_default()
    }
}

/// Parsed body of `xl_xact_commit` / `xl_xact_abort`. Today's consumer
/// only needs the timestamp + subxact list; remaining xinfo tails are
/// skip-walked through but unread.
#[derive(Debug, Default)]
struct XactCommitPayload {
    xact_time: i64,
    subxacts: Vec<u32>,
}

/// `xl_xact_assignment` payload (`access/xact.h` L218-225). Returns
/// `(xtop, subxids)` from `main_data`. `xtop` is canonical — the
/// record header's `xact_id` is the same value in steady state, but
/// the payload is the documented source of truth.
fn parse_xact_assignment(main_data: &[u8]) -> Option<(u32, Vec<u32>)> {
    if main_data.len() < 8 {
        return None;
    }
    let xtop = u32::from_le_bytes(main_data[0..4].try_into().unwrap());
    let nsub = i32::from_le_bytes(main_data[4..8].try_into().unwrap());
    if nsub < 0 {
        return None;
    }
    let nsub = nsub as usize;
    let need = 8 + nsub * 4;
    if main_data.len() < need {
        return None;
    }
    let mut subs = Vec::with_capacity(nsub);
    for i in 0..nsub {
        let off = 8 + i * 4;
        subs.push(u32::from_le_bytes(
            main_data[off..off + 4].try_into().unwrap(),
        ));
    }
    Some((xtop, subs))
}

/// Walk `xl_xact_commit` / `xl_xact_abort` main_data following the
/// `xinfo` tail order from `xactdesc.c::ParseCommitRecord` /
/// `ParseAbortRecord`. Returns `XactCommitPayload::default()` on any
/// short read so the decoder degrades to "commit_ts unknown, no
/// subxact list" rather than poisoning the stream.
///
/// `info` is the record header's `info` byte. `XLOG_XACT_HAS_INFO`
/// (`0x80`) gates the `xinfo` u32 immediately after `xact_time`. The
/// commit-prepared / abort-prepared codepaths set the same flag.
fn parse_xact_payload(info: u8, main_data: &[u8]) -> XactCommitPayload {
    let mut out = XactCommitPayload::default();
    if main_data.len() < 8 {
        return out;
    }
    out.xact_time = i64::from_le_bytes(main_data[0..8].try_into().unwrap());
    let mut p = 8usize;
    let xinfo: u32 = if info & XLOG_XACT_HAS_INFO != 0 {
        if main_data.len() < p + 4 {
            return out;
        }
        let v = u32::from_le_bytes(main_data[p..p + 4].try_into().unwrap());
        p += 4;
        v
    } else {
        0
    };
    if xinfo & XACT_XINFO_HAS_DBINFO != 0 {
        // dbId + tsId, 2x Oid (4 bytes each).
        if main_data.len() < p + 8 {
            return out;
        }
        p += 8;
    }
    if xinfo & XACT_XINFO_HAS_SUBXACTS != 0 {
        if main_data.len() < p + 4 {
            return out;
        }
        let n = i32::from_le_bytes(main_data[p..p + 4].try_into().unwrap());
        p += 4;
        if n < 0 {
            return out;
        }
        let n = n as usize;
        if main_data.len() < p + n * 4 {
            return out;
        }
        let mut subs = Vec::with_capacity(n);
        for i in 0..n {
            let off = p + i * 4;
            subs.push(u32::from_le_bytes(
                main_data[off..off + 4].try_into().unwrap(),
            ));
        }
        p += n * 4;
        out.subxacts = subs;
    }
    // Remaining tails are skip-walked. None of them feed the buffer
    // today; the loop exists so the caller's `p` would stay sane if a
    // future change reads beyond subxacts.
    if xinfo & XACT_XINFO_HAS_RELFILELOCATORS != 0 {
        // int32 nrels + RelFileLocator (spc Oid, db Oid, rel Oid) =
        // 4 bytes + 12 per entry.
        if main_data.len() < p + 4 {
            return out;
        }
        let n = i32::from_le_bytes(main_data[p..p + 4].try_into().unwrap());
        p += 4;
        if n < 0 {
            return out;
        }
        let skip = (n as usize).saturating_mul(12);
        if main_data.len() < p + skip {
            return out;
        }
        p += skip;
    }
    if xinfo & XACT_XINFO_HAS_DROPPED_STATS != 0 {
        // int32 nitems + xl_xact_stats_item (int kind + Oid dboid +
        // 2x uint32 objid) = 4 + 16 per entry.
        if main_data.len() < p + 4 {
            return out;
        }
        let n = i32::from_le_bytes(main_data[p..p + 4].try_into().unwrap());
        p += 4;
        if n < 0 {
            return out;
        }
        let skip = (n as usize).saturating_mul(16);
        if main_data.len() < p + skip {
            return out;
        }
        p += skip;
    }
    if xinfo & XACT_XINFO_HAS_INVALS != 0 {
        // commit-only tail; abort never sets this bit per xactdesc.c.
        // int32 nmsgs + SharedInvalidationMessage (16 bytes each).
        if main_data.len() < p + 4 {
            return out;
        }
        let n = i32::from_le_bytes(main_data[p..p + 4].try_into().unwrap());
        p += 4;
        if n < 0 {
            return out;
        }
        let skip = (n as usize).saturating_mul(16);
        if main_data.len() < p + skip {
            return out;
        }
        p += skip;
    }
    if xinfo & XACT_XINFO_HAS_TWOPHASE != 0 {
        // xl_xact_twophase: TransactionId (4 bytes).
        if main_data.len() < p + 4 {
            return out;
        }
        p += 4;
        if xinfo & XACT_XINFO_HAS_GID != 0 {
            // null-terminated GID; walk to the terminator.
            let rest = &main_data[p..];
            let nul = rest.iter().position(|&b| b == 0);
            match nul {
                Some(n) => p += n + 1,
                None => return out,
            }
        }
    }
    if xinfo & XACT_XINFO_HAS_ORIGIN != 0 {
        // xl_xact_origin: XLogRecPtr (8) + TimestampTz (8). Unaligned
        // per the comment in xactdesc.c.
        if main_data.len() < p + 16 {
            return out;
        }
        // not read, just consume.
        let _ = p;
    }
    out
}

/// Pull the `source_lsn` off a `SpillEntry`. Heaps and chunks both
/// stamp the WAL LSN at decode time; the buffer's merge-drain across
/// `top + subxids` orders entries by this field.
fn entry_lsn(e: &SpillEntry) -> u64 {
    match e {
        SpillEntry::Heap(h) => h.source_lsn,
        SpillEntry::Chunk(c) => c.source_lsn,
    }
}

#[derive(Debug, Clone)]
pub struct XactBufferConfig {
    /// In-memory budget across every active xact before eviction
    /// kicks in. Sum of [`XactState::in_mem_bytes`] over [`XactBuffer`].
    pub xact_buffer_max: usize,
    /// Per-xid spill files land here. Created on [`XactBuffer::new`]
    /// if missing; cleared by [`XactBuffer::clear_spill_dir`] at
    /// startup.
    pub spill_dir: PathBuf,
}

impl XactBufferConfig {
    pub fn new(spill_dir: PathBuf) -> Self {
        Self {
            xact_buffer_max: DEFAULT_XACT_BUFFER_MAX,
            spill_dir,
        }
    }
}

#[derive(Debug, Error)]
pub enum XactBufferError {
    #[error("spill: {0}")]
    Spill(#[from] SpillError),
    #[error("catalog: {0}")]
    Catalog(#[from] CatalogError),
    #[error("observer: {0}")]
    Observer(String),
    #[error("toast chunk for value_id={value_id} on rel={toast_relid} missing seq {missing}")]
    MissingToastChunk {
        toast_relid: u32,
        value_id: u32,
        missing: u32,
    },
    #[error("toast decompression: {0}")]
    Detoast(String),
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
    /// Xacts currently buffered (in-memory or partly spilled).
    pub xacts_active: u64,
    /// Bytes of [`DecodedHeap`] / [`ToastChunk`] held in memory.
    /// Bookkeeping-only — actual heap allocation may differ.
    pub bytes_in_memory: u64,
    /// Total xacts buffered since startup.
    pub xacts_total: u64,
    /// Xacts with a non-empty spill file right now.
    pub spill_xacts_active: u64,
    /// Bytes written to spill files for active xacts. Drops as
    /// commits/aborts drain the files.
    pub spill_bytes_active: u64,
    /// Total spill evictions since startup.
    pub spill_evictions_total: u64,
    /// Total xacts committed (drained successfully).
    pub committed_xacts_total: u64,
    /// Total xacts aborted (buffer dropped).
    pub aborted_xacts_total: u64,
    /// Counts of `COMMIT` records observed for xids the buffer
    /// never saw — i.e. read-only or filtered-out xacts.
    pub commits_unknown_xid: u64,
    /// Same shape for aborts: xids we never buffered. Higher counts
    /// here than for `commits_unknown_xid` are expected since aborts
    /// often happen on xacts that never wrote anything.
    pub aborts_unknown_xid: u64,
    /// Highest commit-record LSN drained out of the buffer
    /// into the observer's `on_tuple` chain. Snapshot for the cursor
    /// file's `drain_lsn`. Monotonic.
    pub drain_lsn: u64,
    /// Highest commit-record LSN where the observer's
    /// `on_xact_end` reported durable on the downstream sink. Snapshot
    /// for `cursor.emitter_ack_lsn`. Monotonic. Lags `drain_lsn`
    /// whenever the observer (CH emitter with `flush_timeout > 0`)
    /// holds rows in still-open INSERTs.
    pub emitter_ack_lsn: u64,
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

struct XactState {
    /// First WAL LSN of the xact — sticky across spill rotations,
    /// distinguishes two xids that collide post slot rebuild.
    first_lsn: u64,
    /// Entries pending memory→spill. WAL-order by arrival.
    in_mem: Vec<SpillEntry>,
    /// Approximate bytes held by `in_mem`.
    in_mem_bytes: usize,
    /// Spill file. `None` until first eviction.
    spill: Option<SpillWriter>,
    /// Bytes written to spill so far. Mirrors `spill.as_ref().byte_count()`
    /// to keep the stat updater branch-free.
    spill_bytes: u64,
    /// Catalog events buffered with the xact, ordered
    /// by source_lsn ASC. Not spilled (the data inside an `Added`
    /// / `Changed` event is rich enough that a spill format would
    /// duplicate `RelDescriptor` decoding; the practical case has at
    /// most a handful of DDL events per xact). If memory pressure
    /// becomes a concern, a later phase can encode the events as
    /// SpillEntry variants and reuse the eviction path.
    catalog_events: Vec<(u64, SchemaEvent)>,
}

impl XactState {
    fn new(first_lsn: u64) -> Self {
        Self {
            first_lsn,
            in_mem: Vec::new(),
            in_mem_bytes: 0,
            spill: None,
            spill_bytes: 0,
            catalog_events: Vec::new(),
        }
    }
}

/// Approximate byte cost of one [`DecodedHeap`] / [`ToastChunk`] for
/// the in-memory accounting. Exact heap allocation is hard to count
/// without re-walking every `Vec` / `String`; this estimate is good
/// enough for the threshold decision and lets eviction kick in before
/// process RSS blows up.
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
    }
}

fn tuple_size(t: &crate::heap_decoder::DecodedTuple) -> usize {
    let mut sz = std::mem::size_of::<crate::heap_decoder::DecodedTuple>()
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

/// One entry per inflight xid produced by
/// [`XactBuffer::inflight_snapshot`]. Diagnostic surface for "a commit
/// for this xid never arrived" investigations — fields cover the four
/// pre-commit absorption paths (heap, chunk, schema event, spill).
#[derive(Debug, Clone)]
pub struct InflightSnapshotEntry {
    pub xid: u32,
    pub first_lsn: u64,
    /// Max source_lsn across heaps + chunks + catalog events for this
    /// xid. Distance from `first_lsn` shows how long the xact has been
    /// open in WAL terms.
    pub last_lsn: u64,
    pub heap_count: u64,
    pub chunk_count: u64,
    pub in_mem_bytes: u64,
    pub spilled: bool,
    pub catalog_events: u64,
    /// `(db_node, rel_node)` pairs touched by this xact, comma-joined.
    /// Cross-reference against shadow's `pg_class.relfilenode` to name
    /// the table without paying for a catalog lookup on every snapshot.
    pub rels: String,
}

/// Per-xact + TOAST buffer with spill-to-disk overflow. Holds
/// everything keyed by `xid`; chunk lookups inside an xact happen via
/// a `(toast_relid, value_id)` walk at drain.
pub struct XactBuffer {
    config: XactBufferConfig,
    store: SpillStore,
    inflight: HashMap<u32, XactState>,
    bytes_in_memory: usize,
    stats: XactBufferStats,
}

impl XactBuffer {
    pub fn new(config: XactBufferConfig) -> std::result::Result<Self, XactBufferError> {
        let store = SpillStore::new(config.spill_dir.clone())?;
        Ok(Self {
            config,
            store,
            inflight: HashMap::new(),
            bytes_in_memory: 0,
            stats: XactBufferStats::default(),
        })
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

    /// Snapshot every xid currently parked in `inflight`, with its
    /// first-seen `source_lsn` and the (heap, chunk, catalog) sizes the
    /// drain would process. Sorted by xid for deterministic dumps.
    /// Diagnostic only — pump-side `populate_metrics` uses it for the
    /// `walshadow_xact_inflight` text exposition when xacts pile up
    /// past a quiescent source.
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
                    }
                }
                for (lsn, _) in &st.catalog_events {
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
                    catalog_events: st.catalog_events.len() as u64,
                    rels: rels_str,
                }
            })
            .collect();
        out.sort_by_key(|e| e.xid);
        out
    }

    /// Buffer a decoded heap tuple. The descriptor needed to detoast
    /// `ExternalToast` columns at drain is fetched from
    /// [`ShadowCatalog`] on demand inside
    /// [`XactBuffer::commit`] — the buffer deliberately does not keep
    /// its own per-xact rel cache, the catalog's own LRU already
    /// covers the repeat-lookup path.
    pub async fn on_heap(
        &mut self,
        decoded: DecodedHeap,
    ) -> std::result::Result<(), XactBufferError> {
        let xid = decoded.xid;
        let first_lsn = decoded.source_lsn;
        let entry = SpillEntry::Heap(Box::new(decoded));
        self.absorb(xid, first_lsn, entry).await
    }

    /// Buffer one TOAST chunk. Decoder sink builds these from
    /// `pg_toast.pg_toast_<rel>` INSERTs that the filter classified
    /// as `User`.
    pub async fn on_toast_chunk(
        &mut self,
        chunk: ToastChunk,
        xid: u32,
    ) -> std::result::Result<(), XactBufferError> {
        let first_lsn = chunk.source_lsn;
        let entry = SpillEntry::Chunk(chunk);
        self.absorb(xid, first_lsn, entry).await
    }

    /// Buffer a [`SchemaEvent`] keyed on `xid`. Drains
    /// alongside heap tuples + chunks at `commit` time in `source_lsn`
    /// order, so an `Added`/`Changed` event triggered by a DDL within
    /// the same xact lands BEFORE the heap writes that follow it.
    /// Aborted xacts drop their events automatically with the rest of
    /// the per-xid buffer.
    pub fn on_schema_event(&mut self, xid: u32, source_lsn: u64, event: SchemaEvent) {
        let is_new = !self.inflight.contains_key(&xid);
        let st = self
            .inflight
            .entry(xid)
            .or_insert_with(|| XactState::new(source_lsn));
        st.catalog_events.push((source_lsn, event));
        if is_new {
            self.stats.xacts_active += 1;
            self.stats.xacts_total += 1;
        }
    }

    async fn absorb(
        &mut self,
        xid: u32,
        first_lsn: u64,
        entry: SpillEntry,
    ) -> std::result::Result<(), XactBufferError> {
        let sz = approximate_size(&entry);
        let is_new = !self.inflight.contains_key(&xid);
        let st = self
            .inflight
            .entry(xid)
            .or_insert_with(|| XactState::new(first_lsn));
        if let Some(spill) = st.spill.as_mut() {
            // Xact already spilling — append straight to disk to keep
            // memory pressure flat.
            spill.write(&entry).await?;
            let bc = spill.byte_count();
            self.stats.spill_bytes_active += bc - st.spill_bytes;
            st.spill_bytes = bc;
        } else {
            st.in_mem.push(entry);
            st.in_mem_bytes += sz;
            self.bytes_in_memory += sz;
        }
        if is_new {
            self.stats.xacts_active += 1;
            self.stats.xacts_total += 1;
        }
        self.stats.bytes_in_memory = self.bytes_in_memory as u64;
        self.maybe_evict().await?;
        Ok(())
    }

    async fn maybe_evict(&mut self) -> std::result::Result<(), XactBufferError> {
        while self.bytes_in_memory > self.config.xact_buffer_max {
            // Pick the largest live in-memory xact.
            let largest = self
                .inflight
                .iter()
                .filter(|(_, s)| !s.in_mem.is_empty())
                .max_by_key(|(_, s)| s.in_mem_bytes)
                .map(|(xid, _)| *xid);
            let Some(xid) = largest else {
                // Nothing left to evict — every active xact already on
                // disk. Caller pushing into spilled xacts faster than
                // the budget allows; in-memory part stays at floor.
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

    /// Drain xact `xid` to `observer` in WAL order. Substitutes every
    /// `ExternalToast` column with its reassembled `Bytea` / `Text`
    /// value via [`ShadowCatalog::relation_at`] on the catalog passed
    /// by the caller. Heaps without TOAST columns never hit the
    /// catalog. No-op if `xid` is unknown (read-only xact, or one
    /// whose records the filter dropped before reaching the buffer).
    ///
    /// `commit_lsn` is the LSN of the `XLOG_XACT_COMMIT` record itself.
    /// Stamped into [`CommittedTuple::commit_lsn`] for the
    /// emitter's ack tracker, and bumped into
    /// [`XactBufferStats::drain_lsn`] / `emitter_ack_lsn` on the
    /// successful-drain path so the cursor file's resume gate has a
    /// monotonic high-water mark.
    pub async fn commit<O: TupleObserver>(
        &mut self,
        top_xid: u32,
        commit_ts: i64,
        commit_lsn: u64,
        subxids: &[u32],
        catalog: &Arc<Mutex<ShadowCatalog>>,
        observer: &mut O,
    ) -> std::result::Result<(), XactBufferError> {
        // Pull every xid in the commit tree out of `inflight`. Subxids
        // we never buffered (catalog-only writes, filter-dropped) skip
        // silently — only the top counts for `commits_unknown_xid` so
        // the metric stays a per-xact rate, not per-subxid.
        let mut xids: Vec<u32> = Vec::with_capacity(1 + subxids.len());
        xids.push(top_xid);
        xids.extend_from_slice(subxids);
        let mut states: Vec<XactState> = Vec::with_capacity(xids.len());
        for x in &xids {
            if let Some(st) = self.inflight.remove(x) {
                states.push(st);
            }
        }

        if states.is_empty() {
            self.stats.commits_unknown_xid += 1;
            // Read-only / filter-dropped xacts still advance the
            // emitter-ack ceiling — source's slot can recycle WAL up to
            // their commit LSN without losing anything we'd have shipped.
            // Route through `on_xact_end` anyway so an observer that
            // holds prior xacts' rows in still-open inserts can clamp
            // the ack at its own durable horizon (otherwise we'd claim
            // durability for rows still buffered client-side).
            self.stats.drain_lsn = self.stats.drain_lsn.max(commit_lsn);
            let ack_lsn = observer
                .on_xact_end(commit_lsn)
                .await
                .map_err(|e| XactBufferError::Observer(e.to_string()))?;
            self.stats.emitter_ack_lsn = self.stats.emitter_ack_lsn.max(ack_lsn);
            return Ok(());
        }
        // Active counter ticks down once per drained xact (top + subs).
        for st in &states {
            self.stats.xacts_active = self.stats.xacts_active.saturating_sub(1);
            self.bytes_in_memory = self.bytes_in_memory.saturating_sub(st.in_mem_bytes);
        }
        self.stats.bytes_in_memory = self.bytes_in_memory as u64;

        // Drain spill files first (older in WAL order) per xid, then
        // tack on in-mem entries. Result: one `VecDeque<SpillEntry>` per
        // xid already sorted by `source_lsn` ASC. Catalog events ride a
        // sibling `VecDeque<(u64, SchemaEvent)>` per xid for the k-way
        // merge below — they don't spill.
        let mut per_xid: Vec<VecDeque<SpillEntry>> = Vec::with_capacity(states.len());
        let mut per_xid_catalog: Vec<VecDeque<(u64, SchemaEvent)>> =
            Vec::with_capacity(states.len());
        for mut st in states.drain(..) {
            let in_mem = std::mem::take(&mut st.in_mem);
            let mut entries: VecDeque<SpillEntry> = VecDeque::with_capacity(in_mem.len());
            if let Some(writer) = st.spill.take() {
                let bc = writer.byte_count();
                self.stats.spill_bytes_active = self.stats.spill_bytes_active.saturating_sub(bc);
                self.stats.spill_xacts_active = self.stats.spill_xacts_active.saturating_sub(1);
                let mut reader = writer.finish().await?;
                while let Some(entry) = reader.next().await? {
                    entries.push_back(entry);
                }
                reader.unlink().await?;
            }
            entries.extend(in_mem);
            per_xid.push(entries);
            // Catalog events accumulate in arrival order; arrival order
            // matches WAL order because the decoder sink pushes
            // immediately on observe, so we can treat the Vec as already
            // sorted ASC by source_lsn.
            let cat: VecDeque<(u64, SchemaEvent)> = std::mem::take(&mut st.catalog_events).into();
            per_xid_catalog.push(cat);
        }

        // k-way merge over per_xid heads by `source_lsn` ASC. k = 1 +
        // nsubxacts, typically <= 4; linear-scan head pick beats a
        // tournament heap at this size. Each item routes into one of
        // three sinks:
        //   * Heap entries collect into `heaps` for post-pass detoast +
        //     dispatch.
        //   * Chunks fold into `chunks`, keyed by (toast_relid, value_id).
        //   * Catalog events accumulate into `ordered_events` with the
        //     heap-index they sort BEFORE, so dispatch interleaves
        //     catalog events with tuples in source_lsn order.
        let mut heaps: Vec<DecodedHeap> = Vec::new();
        let mut chunks: HashMap<(u32, u32), BTreeMap<u32, Vec<u8>>> = HashMap::new();
        let mut ordered_events: Vec<(usize, SchemaEvent)> = Vec::new();
        loop {
            #[derive(Clone, Copy)]
            enum Pick {
                Spill(usize, u64),
                Catalog(usize, u64),
            }
            let mut best: Option<Pick> = None;
            let best_lsn = |p: Pick| match p {
                Pick::Spill(_, l) | Pick::Catalog(_, l) => l,
            };
            // Pick catalog events FIRST at any tie: PG always writes
            // the DDL's catalog mutation BEFORE the heap write that
            // depends on it. When [`BufferingDecoderSink`] stamps a
            // schema event with the triggering heap's source_lsn (the
            // catalog refetch is lazy), the two share an LSN; tie-
            // break catalog first so the applicator's `ALTER` lands
            // on CH before the dependent INSERT encodes against the
            // post-DDL shape.
            for (i, q) in per_xid_catalog.iter().enumerate() {
                let Some(&(lsn, _)) = q.front() else {
                    continue;
                };
                if best.is_none_or(|b| lsn <= best_lsn(b)) {
                    best = Some(Pick::Catalog(i, lsn));
                }
            }
            for (i, q) in per_xid.iter().enumerate() {
                let Some(head) = q.front() else { continue };
                let lsn = entry_lsn(head);
                if best.is_none_or(|b| lsn < best_lsn(b)) {
                    best = Some(Pick::Spill(i, lsn));
                }
            }
            let Some(pick) = best else {
                break;
            };
            match pick {
                Pick::Spill(i, _) => {
                    let entry = per_xid[i].pop_front().expect("just peeked head");
                    accumulate(entry, &mut heaps, &mut chunks);
                }
                Pick::Catalog(i, _) => {
                    let (_lsn, ev) = per_xid_catalog[i].pop_front().expect("just peeked head");
                    // The catalog event sorts BEFORE the heap that
                    // follows in source_lsn order. Record the heap
                    // index that this event sorts in front of so the
                    // dispatch loop flushes the event first.
                    ordered_events.push((heaps.len(), ev));
                }
            }
        }

        let mut event_cursor = 0usize;
        for (heap_idx, mut heap) in heaps.into_iter().enumerate() {
            // Flush any catalog events that sort BEFORE this heap.
            while event_cursor < ordered_events.len() && ordered_events[event_cursor].0 <= heap_idx
            {
                let ev = &ordered_events[event_cursor].1;
                observer
                    .on_schema_event(ev)
                    .await
                    .map_err(|e| XactBufferError::Observer(e.to_string()))?;
                event_cursor += 1;
            }
            detoast_heap(&mut heap, &chunks, catalog).await?;
            let committed = CommittedTuple {
                decoded: heap,
                commit_ts,
                commit_lsn,
            };
            observer
                .on_tuple(&committed)
                .await
                .map_err(|e| XactBufferError::Observer(e.to_string()))?;
        }
        // Flush trailing catalog events (events with no heap after them
        // in the merge).
        while event_cursor < ordered_events.len() {
            let ev = &ordered_events[event_cursor].1;
            observer
                .on_schema_event(ev)
                .await
                .map_err(|e| XactBufferError::Observer(e.to_string()))?;
            event_cursor += 1;
        }
        // drain_lsn ticks before the on_xact_end ack so an observer
        // failure leaves drain_lsn ahead of emitter_ack_lsn — exactly
        // the gap the cursor file is designed to surface. With CH
        // emitter's flush_timeout > 0, on_xact_end returns the last
        // durable commit_lsn (possibly < commit_lsn for held-open
        // inserts), so emitter_ack_lsn lags drain_lsn deliberately.
        self.stats.drain_lsn = self.stats.drain_lsn.max(commit_lsn);
        let ack_lsn = observer
            .on_xact_end(commit_lsn)
            .await
            .map_err(|e| XactBufferError::Observer(e.to_string()))?;
        let prev_ack = self.stats.emitter_ack_lsn;
        self.stats.emitter_ack_lsn = self.stats.emitter_ack_lsn.max(ack_lsn);
        // Trace ack progression — if observer keeps returning a stale
        // value while commit_lsn marches forward, the ack pin shows up
        // here.
        tracing::trace!(
            target: "walshadow::xact_buffer",
            top_xid,
            commit_lsn = format!("{:X}/{:X}", commit_lsn >> 32, commit_lsn as u32),
            ack_lsn = format!("{:X}/{:X}", ack_lsn >> 32, ack_lsn as u32),
            prev_ack = format!("{:X}/{:X}", prev_ack >> 32, prev_ack as u32),
            "drain complete",
        );
        // One bump per top, not per subxid: a top with N subs is a
        // single user-facing transaction at COMMIT time.
        self.stats.committed_xacts_total += 1;
        Ok(())
    }

    /// Idle-tick ack: advance `drain_lsn` to `lsn` (dispatched marker)
    /// when no xact is in flight, and `emitter_ack_lsn` to
    /// `min(lsn, ack_ceiling)`. Pump loop drives this after the queueing
    /// worker drains a batch so source's slot can recycle past trailing
    /// post-COMMIT WAL (page padding, RUNNING_XACTS, CHECKPOINT) when
    /// quiescent. `ack_ceiling` is the observer's durable horizon: in
    /// hold-open mode rows can sit in open INSERTs between commits, so
    /// the ack must not jump past what the observer has made durable —
    /// otherwise source recycles WAL the emitter hasn't written.
    pub fn advance_idle(&mut self, lsn: u64, ack_ceiling: u64) {
        if self.stats.xacts_active != 0 {
            return;
        }
        self.stats.drain_lsn = self.stats.drain_lsn.max(lsn);
        self.stats.emitter_ack_lsn = self.stats.emitter_ack_lsn.max(lsn.min(ack_ceiling));
    }

    /// Fold an observer-reported durable LSN (e.g. from a deadline-
    /// triggered idle close) into `emitter_ack_lsn`. Only advances the
    /// ack; `drain_lsn` already covers commit boundaries.
    pub fn note_idle_durable(&mut self, lsn: u64) {
        self.stats.emitter_ack_lsn = self.stats.emitter_ack_lsn.max(lsn);
    }

    /// Discard xact `xid`. No-op if unknown. Wipes any spill file.
    /// `abort_lsn` is the LSN of the `XLOG_XACT_ABORT` record itself;
    /// advances `drain_lsn` / `emitter_ack_lsn` so the cursor file
    /// records aborted xacts as fully consumed (nothing to ship).
    pub async fn abort(
        &mut self,
        xid: u32,
        abort_lsn: u64,
        subxids: &[u32],
    ) -> std::result::Result<(), XactBufferError> {
        self.stats.drain_lsn = self.stats.drain_lsn.max(abort_lsn);
        self.stats.emitter_ack_lsn = self.stats.emitter_ack_lsn.max(abort_lsn);
        // `xid` is the header xact_id — top-xact abort, or subxact
        // standalone-rollback. Either way, drop `xid` itself and every
        // sub in `subxids`. For mid-xact subxact rollback (PG
        // `RecordSubTransactionAbort` writes a separate `XLOG_XACT_ABORT`
        // keyed on the sub xid), top's pre-savepoint entries stay
        // keyed on top_xid in `inflight` and flush at the top's COMMIT.
        let mut xids: Vec<u32> = Vec::with_capacity(1 + subxids.len());
        xids.push(xid);
        xids.extend_from_slice(subxids);

        let mut any = false;
        for x in xids {
            let Some(mut st) = self.inflight.remove(&x) else {
                continue;
            };
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
        // One bump per abort record, not per subxid — matches `commit`'s
        // per-top accounting.
        self.stats.aborted_xacts_total += 1;
        Ok(())
    }

    /// xact ids currently held — test helper.
    #[cfg(test)]
    pub fn active_xids(&self) -> Vec<u32> {
        let mut v: Vec<u32> = self.inflight.keys().copied().collect();
        v.sort_unstable();
        v
    }
}

fn accumulate(
    entry: SpillEntry,
    heaps: &mut Vec<DecodedHeap>,
    chunks: &mut HashMap<(u32, u32), BTreeMap<u32, Vec<u8>>>,
) {
    match entry {
        SpillEntry::Heap(h) => heaps.push(*h),
        SpillEntry::Chunk(c) => {
            chunks
                .entry((c.toast_relid, c.value_id))
                .or_default()
                .insert(c.chunk_seq, c.chunk_data);
        }
    }
}

async fn detoast_heap(
    heap: &mut DecodedHeap,
    chunks: &HashMap<(u32, u32), BTreeMap<u32, Vec<u8>>>,
    catalog: &Arc<Mutex<ShadowCatalog>>,
) -> std::result::Result<(), XactBufferError> {
    let needs = tuple_needs_detoast(heap.new.as_ref()) || tuple_needs_detoast(heap.old.as_ref());
    if !needs {
        return Ok(());
    }
    let rel: Arc<RelDescriptor> = {
        let mut cat = catalog.lock().await;
        cat.relation_at(heap.rfn, heap.source_lsn).await?
    };
    if let Some(t) = heap.new.as_mut() {
        detoast_tuple(t, &rel, chunks)?;
    }
    if let Some(t) = heap.old.as_mut() {
        detoast_tuple(t, &rel, chunks)?;
    }
    Ok(())
}

fn tuple_needs_detoast(t: Option<&crate::heap_decoder::DecodedTuple>) -> bool {
    let Some(t) = t else {
        return false;
    };
    t.columns
        .iter()
        .any(|c| matches!(c, Some(ColumnValue::ExternalToast(_))))
}

fn detoast_tuple(
    t: &mut crate::heap_decoder::DecodedTuple,
    rel: &RelDescriptor,
    chunks: &HashMap<(u32, u32), BTreeMap<u32, Vec<u8>>>,
) -> std::result::Result<(), XactBufferError> {
    for (idx, col) in t.columns.iter_mut().enumerate() {
        let Some(ColumnValue::ExternalToast(p)) = col else {
            continue;
        };
        // `ToastPointer: Copy` so the read-out frees the borrow on
        // `col` before reassemble + assignment.
        let p: ToastPointer = *p;
        let raw = reassemble(&p, chunks)?;
        // Look up the original column's type to decide Bytea vs Text.
        let type_oid = rel.attributes.get(idx).map(|a| a.type_oid).unwrap_or(0);
        let new_value = match type_oid {
            crate::heap_decoder::BYTEAOID => ColumnValue::Bytea(raw),
            crate::heap_decoder::TEXTOID
            | crate::heap_decoder::VARCHAROID
            | crate::heap_decoder::BPCHAROID => match String::from_utf8(raw) {
                Ok(s) => ColumnValue::Text(s),
                Err(e) => ColumnValue::Bytea(e.into_bytes()),
            },
            _ => ColumnValue::Unsupported { type_oid, raw },
        };
        *col = Some(new_value);
    }
    Ok(())
}

/// PG `VARLENA_EXTSIZE_BITS` from `~/s/postgresql/src/include/varatt.h`.
const VARLENA_EXTSIZE_BITS: u32 = 30;
const VARLENA_EXTSIZE_MASK: u32 = (1u32 << VARLENA_EXTSIZE_BITS) - 1;
/// PG `VARHDRSZ` — 4-byte varlena header.
const VARHDRSZ: i32 = 4;

const TOAST_COMPRESSION_PGLZ: u8 = 0;
const TOAST_COMPRESSION_LZ4: u8 = 1;

fn reassemble(
    p: &ToastPointer,
    chunks: &HashMap<(u32, u32), BTreeMap<u32, Vec<u8>>>,
) -> std::result::Result<Vec<u8>, XactBufferError> {
    let key = (p.va_toastrelid, p.va_valueid);
    let map = chunks.get(&key).ok_or(XactBufferError::MissingToastChunk {
        toast_relid: p.va_toastrelid,
        value_id: p.va_valueid,
        missing: 0,
    })?;
    let total: usize = map.values().map(Vec::len).sum();
    let mut concat: Vec<u8> = Vec::with_capacity(total);
    for (expected, (seq, body)) in map.iter().enumerate() {
        let expected = expected as u32;
        if *seq != expected {
            return Err(XactBufferError::MissingToastChunk {
                toast_relid: p.va_toastrelid,
                value_id: p.va_valueid,
                missing: expected,
            });
        }
        concat.extend_from_slice(body);
    }
    let compressed = (p.va_extinfo & !VARLENA_EXTSIZE_MASK) != 0;
    if !compressed {
        return Ok(concat);
    }
    let method = ((p.va_extinfo >> VARLENA_EXTSIZE_BITS) & 0x3) as u8;
    let raw_len = (p.va_rawsize - VARHDRSZ).max(0) as usize;
    match method {
        TOAST_COMPRESSION_PGLZ => {
            let mut out = vec![0u8; raw_len];
            let n = pglz::decompress_into(&concat, &mut out, true)
                .ok_or_else(|| XactBufferError::Detoast("pglz: stream truncated/corrupt".into()))?;
            out.truncate(n);
            Ok(out)
        }
        TOAST_COMPRESSION_LZ4 => {
            let out = lz4_flex::decompress(&concat, raw_len)
                .map_err(|e| XactBufferError::Detoast(format!("lz4: {e}")))?;
            Ok(out)
        }
        other => Err(XactBufferError::Detoast(format!(
            "unknown compression method {other}"
        ))),
    }
}

/// `RecordSink` that observes `RM_XACT_ID` records and drives the
/// buffer's commit/abort path. Separate from [`BufferingDecoderSink`]
/// because xact records are `Decision::Keep` (shadow PG needs them
/// for CLOG) so the decoder sink skips them by contract.
pub struct XactRecordSink<O: TupleObserver + Send> {
    buffer: Arc<Mutex<XactBuffer>>,
    /// Shared with `BufferingDecoderSink`. Drain calls
    /// `relation_at` only for heaps with TOAST columns; everything
    /// else doesn't touch the catalog.
    catalog: Arc<Mutex<ShadowCatalog>>,
    /// Maps subxids to their top via
    /// `XLOG_XACT_ASSIGNMENT`. Hint surface only — the canonical
    /// subxact list arrives inline on commit / abort records and
    /// drives the drain merge directly. Tracker covers eviction-
    /// policy decisions that need to know the family before COMMIT.
    subxact_tracker: Arc<Mutex<SubxactTracker>>,
    /// Where committed tuples land. `XactBuffer::commit` calls
    /// `observer.on_tuple` per drained tuple; production wires this
    /// to the same `MetricsTupleObserver` the metrics path uses and the
    /// CH emitter observer in production.
    observer: O,
    /// Schema-event consumer. Same `Arc<Mutex<…>>` as the
    /// one [`BufferingDecoderSink`] holds; this sink only drains it
    /// post-`sweep_dropped` to route `Dropped` events into the xact
    /// buffer.
    schema_events: Option<SchemaEventRx>,
    /// DROP-only epoch counter (shared with
    /// `ShadowCatalog::set_pg_class_delete_epoch` +
    /// `CatalogTracker::set_pg_class_delete_epoch`). Atomic-load on
    /// every commit boundary so the per-commit catalog-lock acquire
    /// is skipped when no DROP TABLE has fired. The lock is contended
    /// with `BufferingDecoderSink::on_record`'s long-running
    /// `wait_for_replay` calls; holding it per commit serialised the
    /// drain pipeline behind heap-record fetches in pgbench-rate
    /// workloads.
    pg_class_delete_epoch: Option<Arc<std::sync::atomic::AtomicU64>>,
    /// Last `pg_class_delete_epoch` value already processed by the
    /// sink. Bumped after a successful sweep.
    last_seen_delete_epoch: u64,
}

impl<O: TupleObserver + Send> XactRecordSink<O> {
    pub fn new(
        buffer: Arc<Mutex<XactBuffer>>,
        catalog: Arc<Mutex<ShadowCatalog>>,
        observer: O,
    ) -> Self {
        Self {
            buffer,
            catalog,
            subxact_tracker: Arc::new(Mutex::new(SubxactTracker::new())),
            observer,
            schema_events: None,
            pg_class_delete_epoch: None,
            last_seen_delete_epoch: 0,
        }
    }

    /// Wire a shared `SubxactTracker` (e.g. one owned by the daemon's
    /// eviction policy). When unset the sink owns a private tracker.
    pub fn with_subxact_tracker(mut self, tracker: Arc<Mutex<SubxactTracker>>) -> Self {
        self.subxact_tracker = tracker;
        self
    }

    /// Share the catalog's schema-event receiver. The
    /// sink runs [`ShadowCatalog::sweep_dropped`] at every commit
    /// boundary; the resulting `Dropped` events land in the channel
    /// and need to flow into the xact buffer keyed on the commit's
    /// xid + LSN. Pass the same [`SchemaEventRx`] also handed to
    /// [`BufferingDecoderSink::with_schema_events`].
    pub fn with_schema_events(mut self, rx: SchemaEventRx) -> Self {
        self.schema_events = Some(rx);
        self
    }

    /// Install the DROP-only epoch counter. Pair with
    /// [`crate::catalog_tracker::CatalogTracker::set_pg_class_delete_epoch`]
    /// and [`ShadowCatalog::set_pg_class_delete_epoch`]; the sink uses
    /// this counter to decide whether [`ShadowCatalog::sweep_dropped`]
    /// has any work without acquiring the (contended) catalog lock.
    pub fn with_pg_class_delete_epoch(mut self, epoch: Arc<std::sync::atomic::AtomicU64>) -> Self {
        self.last_seen_delete_epoch = epoch.load(std::sync::atomic::Ordering::Acquire);
        self.pg_class_delete_epoch = Some(epoch);
        self
    }

    pub fn observer_mut(&mut self) -> &mut O {
        &mut self.observer
    }

    pub fn subxact_tracker(&self) -> &Arc<Mutex<SubxactTracker>> {
        &self.subxact_tracker
    }

    /// Drain any [`SchemaEvent`]s queued by a recent
    /// `ShadowCatalog::sweep_dropped` call (or other producer) and
    /// route them into the xact buffer keyed on `(xid, source_lsn)`.
    async fn route_pending_schema_events(
        &mut self,
        xid: u32,
        source_lsn: u64,
    ) -> std::result::Result<(), SinkError> {
        let Some(rx) = self.schema_events.as_ref() else {
            return Ok(());
        };
        let pending = drain_pending_schema_events(rx);
        if pending.is_empty() {
            return Ok(());
        }
        let mut buf = self.buffer.lock().await;
        for ev in pending {
            buf.on_schema_event(xid, source_lsn, ev);
        }
        Ok(())
    }
}

impl<O: TupleObserver + Send> RecordSink for XactRecordSink<O> {
    fn on_record<'a>(
        &'a mut self,
        record: &'a Record<'a>,
    ) -> Pin<Box<dyn std::future::Future<Output = std::result::Result<(), SinkError>> + Send + 'a>>
    {
        Box::pin(async move {
            if record.parsed.header.resource_manager_id != RmId::Xact as u8 {
                return Ok(());
            }
            let info = record.parsed.header.info;
            let op = info & XLOG_XACT_OPMASK;
            let xid = record.parsed.header.xact_id;
            match op {
                XLOG_XACT_COMMIT | XLOG_XACT_COMMIT_PREPARED => {
                    let payload = parse_xact_payload(info, &record.parsed.main_data);
                    // Poll-based DROP TABLE discovery at
                    // the commit boundary. PG's system catalogs default
                    // to `relreplident = 'n'` so heap_delete WAL records
                    // don't carry the dying tuple's oid; the only safe
                    // way to detect a drop is to ask shadow whether the
                    // previously-known oids still exist. `has_pending_sweep`
                    // gates on the narrower `pg_class_delete_epoch` so
                    // ADD COLUMN / CREATE INDEX / VACUUM (catalog
                    // INSERT / UPDATE flood) don't trigger the sweep —
                    // only heap_delete on pg_class (the WAL signature
                    // of DROP TABLE) tips this branch.
                    if self.schema_events.is_some() {
                        // Atomic-load gate: skip the catalog lock when
                        // no DROP TABLE has fired since the last sweep.
                        // The lock contends with `BufferingDecoderSink`'s
                        // long-running `wait_for_replay` calls; holding
                        // it per commit serialised pgbench-rate workloads
                        // behind heap-record fetches.
                        let current_delete_epoch = self
                            .pg_class_delete_epoch
                            .as_ref()
                            .map(|e| e.load(std::sync::atomic::Ordering::Acquire))
                            .unwrap_or(self.last_seen_delete_epoch);
                        if current_delete_epoch != self.last_seen_delete_epoch {
                            let mut cat = self.catalog.lock().await;
                            if record.source_lsn > 0 {
                                cat.wait_for_replay(record.source_lsn)
                                    .await
                                    .map_err(|e| SinkError::from(DecoderSinkError::from(e)))?;
                            }
                            let dropped_count = cat
                                .sweep_dropped()
                                .await
                                .map_err(|e| SinkError::from(DecoderSinkError::from(e)))?;
                            drop(cat);
                            self.last_seen_delete_epoch = current_delete_epoch;
                            if dropped_count > 0 {
                                self.route_pending_schema_events(xid, record.source_lsn)
                                    .await?;
                            }
                        }
                    }
                    let mut buf = self.buffer.lock().await;
                    // Trace every commit so daemon stderr can correlate
                    // a stuck `xact_active` against the commits that
                    // ARE arriving; in particular surfaces subxact-id
                    // mismatches between heap-record xact_id and the
                    // top's commit-record subxact list.
                    tracing::trace!(
                        target: "walshadow::xact_buffer",
                        xid,
                        commit_lsn = format!(
                            "{:X}/{:X}",
                            record.source_lsn >> 32,
                            record.source_lsn as u32,
                        ),
                        nsubxacts = payload.subxacts.len(),
                        "xact commit",
                    );
                    buf.commit(
                        xid,
                        payload.xact_time,
                        record.source_lsn,
                        &payload.subxacts,
                        &self.catalog,
                        &mut self.observer,
                    )
                    .await
                    .map_err(SinkError::from)?;
                    drop(buf);
                    // Drop the tracker's hint for this family —
                    // commit terminates the top. Cheap O(k) cleanup.
                    self.subxact_tracker.lock().await.forget_tree(xid);
                }
                XLOG_XACT_ABORT | XLOG_XACT_ABORT_PREPARED => {
                    let payload = parse_xact_payload(info, &record.parsed.main_data);
                    tracing::trace!(
                        target: "walshadow::xact_buffer",
                        xid,
                        abort_lsn = format!(
                            "{:X}/{:X}",
                            record.source_lsn >> 32,
                            record.source_lsn as u32,
                        ),
                        nsubxacts = payload.subxacts.len(),
                        "xact abort",
                    );
                    let mut buf = self.buffer.lock().await;
                    buf.abort(xid, record.source_lsn, &payload.subxacts)
                        .await
                        .map_err(SinkError::from)?;
                    drop(buf);
                    // forget_tree is harmless for standalone subxact
                    // abort (xid is the sub itself, tracker drops the
                    // single edge); for top abort it clears the whole
                    // family.
                    self.subxact_tracker.lock().await.forget_tree(xid);
                }
                XLOG_XACT_ASSIGNMENT => {
                    // Record subxact → top edges so
                    // eviction policy can fold sibling buffers under
                    // memory pressure. Correctness rides on the commit
                    // / abort record's authoritative subxact list, not
                    // on this hint.
                    if let Some((xtop, subs)) = parse_xact_assignment(&record.parsed.main_data) {
                        self.subxact_tracker.lock().await.assign(xtop, &subs);
                    }
                }
                _ => {
                    // XLOG_XACT_PREPARE / INVALIDATIONS: not this
                    // buffer's territory. PREPARE without COMMIT PREPARED would
                    // leave the xact stuck — defer 2PC proper handling
                    // to a follow-up, today the xact stays buffered
                    // until COMMIT_PREPARED lands.
                }
            }
            Ok(())
        })
    }

    /// Forward idle ticks straight to the observer. The xact buffer
    /// itself has no time-based work; the hook exists so the CH
    /// emitter's hold-INSERT-open deadline can fire without waiting
    /// on the next commit record.
    fn on_idle<'a>(
        &'a mut self,
    ) -> Pin<Box<dyn std::future::Future<Output = std::result::Result<(), SinkError>> + Send + 'a>>
    {
        Box::pin(async move {
            let durable = self
                .observer
                .on_idle()
                .await
                .map_err(|e| SinkError::Other(e.to_string()))?;
            // A deadline-triggered close promotes the emitter's durable
            // horizon; fold it into the ack so retention advances even
            // when no further commit follows.
            if durable != 0 {
                self.buffer.lock().await.note_idle_durable(durable);
            }
            Ok(())
        })
    }

    /// Forward close to the observer. Final force-flush hook for the
    /// CH emitter's hold-INSERT-open path; without it, any rows
    /// buffered when the daemon shuts down would stay non-durable.
    fn on_close<'a>(
        &'a mut self,
    ) -> Pin<Box<dyn std::future::Future<Output = std::result::Result<(), SinkError>> + Send + 'a>>
    {
        Box::pin(async move {
            self.observer
                .on_close()
                .await
                .map_err(|e| SinkError::Other(e.to_string()))
        })
    }

    fn on_idle_advance<'a>(
        &'a mut self,
        lsn: u64,
    ) -> Pin<Box<dyn std::future::Future<Output = std::result::Result<(), SinkError>> + Send + 'a>>
    {
        Box::pin(async move {
            // Cap the ack at the observer's durable horizon before
            // locking the buffer (the nudge must not promote past rows
            // still held in open INSERTs).
            let ceiling = self.observer.idle_ack_ceiling(lsn);
            let mut buf = self.buffer.lock().await;
            buf.advance_idle(lsn, ceiling);
            Ok(())
        })
    }
}

/// `RecordSink` that decodes user-heap records and routes them into
/// the xact buffer keyed by `xid`. Toast-relation INSERTs
/// (`rel.kind == 't'`) are reinterpreted as
/// [`ToastChunk`](crate::spill::ToastChunk)s and parked under their
/// `(toast_relid, value_id)` slot for drain-time reassembly. Only
/// `Decision::Drop` records reach this sink (catalog-keep stays on
/// the shadow-replay path); semantic errors absorb into
/// [`DecoderStats`] rather than poisoning the stream.
/// Shared schema-event receiver — wraps the catalog's
/// [`tokio::sync::mpsc::UnboundedReceiver`] behind a `std::sync::Mutex`
/// so both [`BufferingDecoderSink`] (drain after every `relation_at`)
/// and [`XactRecordSink`] (drain after every `sweep_dropped` at commit
/// boundaries) can pull events out of the same queue.
pub type SchemaEventRx = Arc<std::sync::Mutex<tokio::sync::mpsc::UnboundedReceiver<SchemaEvent>>>;

pub struct BufferingDecoderSink {
    catalog: Arc<Mutex<ShadowCatalog>>,
    buffer: Arc<Mutex<XactBuffer>>,
    /// Shared so the daemon's status loop (or a `QueueingRecordSink`
    /// wrapper running this sink on a worker task) can read counters
    /// without locking. Mutations are `fetch_add(_, Relaxed)`; readers
    /// `.load(Relaxed)` at the use site.
    stats: Arc<DecoderStats>,
    /// Schema events the catalog emits at descriptor
    /// fetch time. Drained inline after every `relation_at` so events
    /// land in the same xact buffer keyed on the current record's
    /// xid + source_lsn. `None` keeps the sink schema-unaware
    /// (greenfield bootstrap, tests).
    schema_events: Option<SchemaEventRx>,
}

impl BufferingDecoderSink {
    pub fn new(catalog: Arc<Mutex<ShadowCatalog>>, buffer: Arc<Mutex<XactBuffer>>) -> Self {
        Self {
            catalog,
            buffer,
            stats: Arc::new(DecoderStats::default()),
            schema_events: None,
        }
    }

    /// Attach a [`SchemaEvent`] receiver. Wrap a freshly-subscribed
    /// [`tokio::sync::mpsc::UnboundedReceiver`] in a [`SchemaEventRx`]
    /// (`Arc<std::sync::Mutex<…>>`) so the same handle can also be
    /// shared with [`XactRecordSink::with_schema_events`]: the channel
    /// drains from both sides (decoder for Added/Changed
    /// events at fetch time, xact-record sink for Dropped events at
    /// commit time via [`ShadowCatalog::sweep_dropped`]).
    pub fn with_schema_events(mut self, rx: SchemaEventRx) -> Self {
        self.schema_events = Some(rx);
        self
    }

    /// Borrow the live counters (for `.summary()` and ad-hoc field
    /// `.load(Relaxed)` reads).
    pub fn stats(&self) -> &DecoderStats {
        &self.stats
    }

    /// Shared handle a wrapping `QueueingRecordSink` can hand back to
    /// the daemon's status loop so it polls live counters while the
    /// sink itself runs on a separate worker task. Reads via
    /// `.load(Relaxed)` on the returned struct's fields.
    pub fn stats_handle(&self) -> Arc<DecoderStats> {
        self.stats.clone()
    }

    /// Drain any [`SchemaEvent`]s the catalog accumulated
    /// during the most recent fetch. Routes each into the xact buffer
    /// stamped with the current record's `(xid, source_lsn)` so the
    /// k-way drain in [`XactBuffer::commit`] sorts them with the heap
    /// writes that triggered the refetch.
    async fn drain_schema_events(
        &mut self,
        xid: u32,
        source_lsn: u64,
    ) -> std::result::Result<(), SinkError> {
        let Some(rx) = self.schema_events.as_ref() else {
            return Ok(());
        };
        // Heap2 VACUUM / FREEZE records carry xact_id=0 (non-
        // transactional) but still drive `relation_at` lookups that
        // can push Added/Changed events. Buffering them under xid=0
        // creates an inflight entry that never commits, pinning
        // `emitter_ack_lsn` behind a phantom xact. Leave the events
        // in the channel — the next real-xid heap record or commit
        // will drain them in-order.
        if xid == 0 {
            return Ok(());
        }
        let pending = drain_pending_schema_events(rx);
        if pending.is_empty() {
            return Ok(());
        }
        let mut buf = self.buffer.lock().await;
        for ev in pending {
            buf.on_schema_event(xid, source_lsn, ev);
        }
        Ok(())
    }

    /// Parse `xl_heap_truncate` main_data, resolve each relid through
    /// `ShadowCatalog`, and push one `HeapOp::Truncate` per relation
    /// into the xact buffer. TRUNCATE is unique in carrying pg_class
    /// OIDs (not relfilenodes) and no block ref, so the standard
    /// `relation_at(rfn)` path doesn't fit.
    async fn handle_truncate(&mut self, record: &Record<'_>) -> std::result::Result<(), SinkError> {
        let Some(parsed) = crate::main_data::parse_xl_heap_truncate(&record.parsed.main_data)
        else {
            self.stats
                .skipped_op
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            return Ok(());
        };
        let xid = record.parsed.header.xact_id;
        let source_lsn = record.source_lsn;
        // Gate on shadow having replayed past source_lsn so the
        // catalog's pg_class entry for each relid is fresh.
        if source_lsn > 0 {
            let mut cat = self.catalog.lock().await;
            cat.wait_for_replay(source_lsn)
                .await
                .map_err(|e| SinkError::from(DecoderSinkError::from(e)))?;
        }
        for relid in parsed.relids {
            let rel = {
                let mut cat = self.catalog.lock().await;
                match cat.relation_by_oid(relid).await {
                    Ok(r) => r,
                    Err(CatalogError::NotFoundByOid(_)) => {
                        self.stats
                            .catalog_not_found
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        continue;
                    }
                    Err(e) => return Err(DecoderSinkError::from(e).into()),
                }
            };
            // TRUNCATE may trigger an Added/Changed event
            // if the relation was rotated since last fetch. Drain the
            // channel inline so events land in the xact buffer.
            self.drain_schema_events(xid, source_lsn).await?;
            // Toast / index / sequence: TRUNCATE doesn't propagate
            // (CH side has no per-table-internal toast). Filter to
            // user heap relations.
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
            // TRUNCATE rides Decision::Keep (filter leaves it intact so
            // shadow can replay the truncate against its own copy), but
            // the decoder still needs to fan out per-relid HeapOp::Truncate
            // for CH emission. Handle before the Drop gate so the
            // SchemaEvent path fires regardless of how the filter scored
            // the record.
            if rm == RmId::Heap as u8 {
                let info_op = record.parsed.header.info & crate::heap_decoder::XLOG_HEAP_OPMASK;
                if info_op == crate::heap_decoder::XLOG_HEAP_TRUNCATE {
                    return self.handle_truncate(record).await;
                }
            }
            if record.decision != Decision::Drop {
                return Ok(());
            }
            if rm != RmId::Heap as u8 && rm != RmId::Heap2 as u8 {
                return Ok(());
            }
            let rfn = match record.parsed.blocks.first() {
                Some(b) => b.header.location.rel,
                None => {
                    self.stats
                        .skipped_no_block
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    return Ok(());
                }
            };
            let rel = {
                let mut cat = self.catalog.lock().await;
                match cat.relation_at(rfn, record.source_lsn).await {
                    Ok(r) => r,
                    Err(CatalogError::NotFoundByFilenode(_)) => {
                        self.stats
                            .catalog_not_found
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        return Ok(());
                    }
                    Err(e) => {
                        // ReplayTimeout used to absorb
                        // into stats.replay_timeout. Under streaming-
                        // fed shadow the gate clears in ms — a timeout
                        // means shadow stalled, the walsender wire
                        // froze, or walshadow backed up against
                        // socket buffers. Silent skip would shed
                        // user-heap writes invisibly. Poison the
                        // stream so the daemon exits and the
                        // cursor resumes on the next boot.
                        return Err(DecoderSinkError::from(e).into());
                    }
                }
            };
            // Drain any schema events the catalog pushed
            // during the refetch (Added on first sight, Changed on
            // diff). Route into the xact buffer keyed on the current
            // record's xid so they drain in WAL order with the heap
            // writes the refetch resolves. Empty in steady state.
            self.drain_schema_events(record.parsed.header.xact_id, record.source_lsn)
                .await?;
            let decoded_set = match decode_heap_record(&record.parsed, record.source_lsn, &rel) {
                Ok(set) => set,
                Err(e) => return Err(DecoderSinkError::from(e).into()),
            };
            if decoded_set.is_empty() {
                self.stats
                    .skipped_op
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                return Ok(());
            }
            for decoded in decoded_set {
                self.stats.record(&decoded);
                if rel.kind == 't' {
                    let xid = decoded.xid;
                    if let Some(chunk) = toast_chunk_from_decoded(decoded, &rel) {
                        self.stats
                            .toast_chunks_buffered
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        let mut buf = self.buffer.lock().await;
                        buf.on_toast_chunk(chunk, xid)
                            .await
                            .map_err(SinkError::from)?;
                    } else {
                        self.stats
                            .toast_chunks_malformed
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    }
                } else {
                    let mut buf = self.buffer.lock().await;
                    buf.on_heap(decoded).await.map_err(SinkError::from)?;
                }
            }
            Ok(())
        })
    }
}

/// Drain every queued [`SchemaEvent`] into a `Vec`. Caller decides
/// where to route them (xact buffer, applicator, etc). Uses
/// `try_recv` in a tight loop — the channel is unbounded + same-task,
/// so no contention.
fn drain_pending_schema_events(rx: &SchemaEventRx) -> Vec<SchemaEvent> {
    let mut out = Vec::new();
    let mut guard = rx.lock().expect("schema event rx mutex poisoned");
    while let Ok(ev) = guard.try_recv() {
        out.push(ev);
    }
    out
}

/// Repack a TOAST table INSERT (op=Insert, exactly 3 columns:
/// `chunk_id oid`, `chunk_seq int4`, `chunk_data bytea`) into a
/// [`ToastChunk`]. Returns `None` for shapes that don't fit — caller
/// counts the malformed event so silent loss is visible.
///
/// Keyed on the toast relation's pg_class OID
/// ([`RelDescriptor::oid`]) rather than its on-disk `rel_node`
/// because the referring tuple's `va_toastrelid` is the OID, not the
/// relfilenode. The two diverge after `VACUUM FULL` / `CLUSTER` on
/// the toast relation.
fn toast_chunk_from_decoded(mut d: DecodedHeap, rel: &RelDescriptor) -> Option<ToastChunk> {
    if d.op != HeapOp::Insert {
        return None;
    }
    let new = d.new.as_mut()?;
    if new.columns.len() < 3 {
        return None;
    }
    let value_id = match new.columns[0].as_ref()? {
        ColumnValue::Oid(v) => *v,
        _ => return None,
    };
    let chunk_seq = match new.columns[1].as_ref()? {
        ColumnValue::Int4(v) => *v as u32,
        _ => return None,
    };
    let chunk_data = match new.columns[2].take()? {
        ColumnValue::Bytea(b) => b,
        // Detoasted text-typed toast wouldn't be a normal flow but
        // tolerate by re-encoding back to bytes.
        ColumnValue::Text(s) => s.into_bytes(),
        _ => return None,
    };
    Some(ToastChunk {
        toast_relid: rel.oid,
        value_id,
        chunk_seq,
        source_lsn: d.source_lsn,
        chunk_data,
    })
}

#[cfg(test)]
mod tests {
    //! Unit tests cover the catalog-free paths:
    //! * On-heap / on-chunk absorption.
    //! * Abort cleanup (no detoast).
    //! * Largest-xact eviction (no detoast).
    //! * `parse_xact_payload` shape coverage.
    //! * `SubxactTracker` round-trip.
    //! * `XactBufferStats::summary` conditional rendering.
    //!
    //! Commit-drain + detoast + `XactRecordSink::commit` paths live in
    //! `tests/xact_buffer.rs` against a real shadow PG — they need
    //! `ShadowCatalog::relation_at` to resolve `rfn` → `RelDescriptor`,
    //! and a stub-catalog seam in unit-test land would just duplicate
    //! the production cache surface (the user instruction: the
    //! per-xact relfilenode cache was misguided, drain reuses
    //! ShadowCatalog's own LRU).

    use super::*;
    use crate::heap_decoder::{DecodedTuple, HeapOp};
    use tempfile::tempdir;
    use wal_rs::pg::walparser::RelFileNode;

    fn cfg(dir: PathBuf) -> XactBufferConfig {
        XactBufferConfig {
            xact_buffer_max: 1024,
            spill_dir: dir,
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
    async fn advance_idle_caps_ack_at_ceiling() {
        let tmp = tempdir().unwrap();
        let mut b = XactBuffer::new(cfg(tmp.path().to_path_buf())).unwrap();
        // Hold-open: ceiling (durable horizon) below the dispatched lsn.
        // drain_lsn tracks dispatch, emitter_ack capped at the ceiling.
        b.advance_idle(100, 50);
        assert_eq!(b.stats().drain_lsn, 100);
        assert_eq!(b.stats().emitter_ack_lsn, 50);
        // Nothing buffered: ceiling == lsn, ack advances fully.
        b.advance_idle(200, 200);
        assert_eq!(b.stats().drain_lsn, 200);
        assert_eq!(b.stats().emitter_ack_lsn, 200);
        // Stale/regressing inputs never lower either field.
        b.advance_idle(150, 100);
        assert_eq!(b.stats().drain_lsn, 200);
        assert_eq!(b.stats().emitter_ack_lsn, 200);
        // Deadline-close durable feedback advances only the ack.
        b.note_idle_durable(260);
        assert_eq!(b.stats().drain_lsn, 200);
        assert_eq!(b.stats().emitter_ack_lsn, 260);
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
            spill_dir: tmp.path().to_path_buf(),
        };
        let mut b = XactBuffer::new(cfg).unwrap();
        // Two xacts: xid=1 with one fat tuple, xid=2 with three small.
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

    /// `abort()` must bump `drain_lsn` and `emitter_ack_lsn`
    /// to the abort-record LSN so the cursor file (and the standby-
    /// status apply ceiling) cover aborted xacts as "fully consumed".
    /// Without this, an all-abort workload would never advance the slot.
    #[tokio::test(flavor = "current_thread")]
    async fn abort_advances_ack_lsns_for_resume_cursor() {
        let tmp = tempdir().unwrap();
        let mut b = XactBuffer::new(cfg(tmp.path().to_path_buf())).unwrap();
        b.on_heap(heap_with_value(7, 100, 16)).await.unwrap();
        b.abort(7, 0x4000, &[]).await.unwrap();
        assert_eq!(b.stats().drain_lsn, 0x4000);
        assert_eq!(b.stats().emitter_ack_lsn, 0x4000);
        // A second abort at a lower LSN must not regress the monotonic
        // high-water marks.
        b.abort(99, 0x100, &[]).await.unwrap();
        assert_eq!(b.stats().drain_lsn, 0x4000);
        assert_eq!(b.stats().emitter_ack_lsn, 0x4000);
        // A later abort advances.
        b.abort(101, 0x8000, &[]).await.unwrap();
        assert_eq!(b.stats().drain_lsn, 0x8000);
        assert_eq!(b.stats().emitter_ack_lsn, 0x8000);
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
        use crate::heap_decoder::{DecodedTuple, HeapOp};
        use crate::shadow_catalog::{RelAttr, ReplIdent};
        let rel = RelDescriptor {
            rfn: RelFileNode {
                spc_node: 1663,
                db_node: 5,
                rel_node: 16400,
            },
            oid: 99,
            namespace_oid: 99,
            namespace_name: "pg_toast".into(),
            name: "pg_toast_16385".into(),
            qualified_name: RelDescriptor::build_qualified_name("pg_toast", "pg_toast_16385"),
            kind: 't',
            persistence: 'p',
            replident: ReplIdent::Default { pk_attnums: None },
            attributes: vec![
                RelAttr {
                    attnum: 1,
                    name: "chunk_id".into(),
                    type_oid: crate::heap_decoder::OIDOID,
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
                    type_oid: crate::heap_decoder::INT4OID,
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
                    type_oid: crate::heap_decoder::BYTEAOID,
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
        let chunk = toast_chunk_from_decoded(d.clone(), &rel).expect("recognised toast shape");
        assert_eq!(chunk.toast_relid, 99); // pg_class.oid, not rel_node
        assert_eq!(chunk.value_id, 55);
        assert_eq!(chunk.chunk_seq, 2);
        assert_eq!(chunk.chunk_data, b"hello");
        // Non-Insert ops fail the shape check.
        let mut d2 = d.clone();
        d2.op = HeapOp::Update;
        assert!(toast_chunk_from_decoded(d2, &rel).is_none());
        // Two-column shape (truncated) fails.
        let mut d3 = d.clone();
        d3.new.as_mut().unwrap().columns.pop();
        assert!(toast_chunk_from_decoded(d3, &rel).is_none());
    }

    // ── subxact tracking ──────────────────────────────────────────────

    #[test]
    fn subxact_tracker_round_trip() {
        let mut t = SubxactTracker::new();
        t.assign(100, &[101, 102]);
        assert_eq!(t.top_for(101), 100);
        assert_eq!(t.top_for(102), 100);
        // Unknown xid returns itself per PG's "sub's top is itself
        // when no ASSIGNMENT record landed yet" semantics.
        assert_eq!(t.top_for(100), 100);
        assert_eq!(t.top_for(999), 999);
        // subxids_of mirrors assign's input ordering.
        let subs = t.subxids_of(100);
        assert!(subs.contains(&101) && subs.contains(&102) && subs.len() == 2);
        // Repeated assignment is idempotent — no duplicate edges.
        t.assign(100, &[101]);
        assert_eq!(t.subxids_of(100).len(), 2);
        t.forget_tree(100);
        assert_eq!(t.top_for(101), 101);
        assert_eq!(t.top_for(102), 102);
        assert!(t.subxids_of(100).is_empty());
    }

    #[test]
    fn subxact_tracker_retargets_subxid_to_new_top() {
        // Defensive case: a subxid that was previously assigned to one
        // top gets reassigned to another. Old children edge must drop.
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
        // Short main_data → None, doesn't panic.
        assert!(parse_xact_assignment(&buf[..6]).is_none());
        // Negative nsub → reject.
        let mut bad = Vec::new();
        bad.extend_from_slice(&1u32.to_le_bytes());
        bad.extend_from_slice(&(-1i32).to_le_bytes());
        assert!(parse_xact_assignment(&bad).is_none());
    }

    #[test]
    fn parse_xact_payload_extracts_xact_time_without_xinfo() {
        // No HAS_INFO bit → only the 8-byte timestamp lives in the body.
        let ts = 0x0123_4567_89AB_CDEFi64;
        let body = ts.to_le_bytes();
        let p = parse_xact_payload(0x00, &body);
        assert_eq!(p.xact_time, ts);
        assert!(p.subxacts.is_empty());
    }

    #[test]
    fn parse_xact_payload_reads_subxacts_with_dbinfo_skip() {
        // HAS_INFO bit set, xinfo = DBINFO | SUBXACTS. Skip-walks
        // through the dbInfo (8 bytes: dbOid + tsOid) to land on the
        // subxacts header.
        let mut body = Vec::new();
        body.extend_from_slice(&42i64.to_le_bytes()); // xact_time
        body.extend_from_slice(&(XACT_XINFO_HAS_DBINFO | XACT_XINFO_HAS_SUBXACTS).to_le_bytes());
        body.extend_from_slice(&5u32.to_le_bytes()); // dbId
        body.extend_from_slice(&1663u32.to_le_bytes()); // tsId
        body.extend_from_slice(&3i32.to_le_bytes()); // nsubxacts
        body.extend_from_slice(&0xAAu32.to_le_bytes());
        body.extend_from_slice(&0xBBu32.to_le_bytes());
        body.extend_from_slice(&0xCCu32.to_le_bytes());
        let p = parse_xact_payload(XLOG_XACT_HAS_INFO, &body);
        assert_eq!(p.xact_time, 42);
        assert_eq!(p.subxacts, vec![0xAA, 0xBB, 0xCC]);
    }

    #[test]
    fn parse_xact_payload_handles_no_has_info() {
        // HAS_INFO unset → xinfo defaults to 0 (no tails). Even when
        // bytes follow the timestamp, parser must not consume them.
        let mut body = 7i64.to_le_bytes().to_vec();
        body.extend_from_slice(&[0xFF; 16]);
        let p = parse_xact_payload(0x00, &body);
        assert_eq!(p.xact_time, 7);
        assert!(p.subxacts.is_empty());
    }

    #[test]
    fn parse_xact_payload_short_main_data_returns_default() {
        let p = parse_xact_payload(XLOG_XACT_HAS_INFO, &[1, 2, 3, 4]);
        assert_eq!(p.xact_time, 0);
        assert!(p.subxacts.is_empty());
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
        // One aborted_xacts_total bump per terminator record, not per
        // subxid in the list.
        assert_eq!(b.stats().aborted_xacts_total, 1);
    }
}
