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
//! [`ShadowCatalog::relation_at`](crate::shadow_catalog::ShadowCatalog::relation_at)
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

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use thiserror::Error;
use tokio::sync::Mutex;
use tracing::Instrument;
use walrus::pg::backup::format_pg_lsn;
use walrus::pg::walparser::RmId;

use crate::decoder_sink::{DecoderSinkError, DecoderStats, TupleObserver};
use crate::filter::Route;
use crate::heap_decoder::{
    ColumnValue, CommittedTuple, DecodedHeap, HeapOp, ToastPointer, decode_heap_record,
};
use crate::pipeline::decode::RelCache;
use crate::shadow_catalog::{CatalogError, RelDescriptor, SchemaEvent, ShadowCatalog};
use crate::spill::{SpillEntry, SpillError, SpillStore, SpillWriter, ToastChunk};
use crate::toast::{ChunkMap, ToastResolver};
use crate::wal_stream::{Record, RecordSink, SinkError};

use std::pin::Pin;

/// Matches PG `logical_decoding_work_mem` default 64 MiB
/// (`src/backend/utils/misc/guc_tables.c`)
pub const DEFAULT_XACT_BUFFER_MAX: usize = 64 * 1024 * 1024;

/// XLOG_XACT info-op constants, PG `access/xact.h`
pub(crate) const XLOG_XACT_OPMASK: u8 = 0x70;
pub(crate) const XLOG_XACT_COMMIT: u8 = 0x00;
pub(crate) const XLOG_XACT_ABORT: u8 = 0x20;
pub(crate) const XLOG_XACT_COMMIT_PREPARED: u8 = 0x30;
pub(crate) const XLOG_XACT_ABORT_PREPARED: u8 = 0x40;
pub(crate) const XLOG_XACT_ASSIGNMENT: u8 = 0x50;
/// When set in record header `info`, `xinfo` u32 follows `xact_time`.
/// PG `access/xact.h`
pub(crate) const XLOG_XACT_HAS_INFO: u8 = 0x80;

/// `xinfo` bits driving xl_xact_commit / xl_xact_abort tail layout, PG
/// `access/xact.h`. Parser consumes `HAS_SUBXACTS`; rest drive skip-walk
const XACT_XINFO_HAS_DBINFO: u32 = 1 << 0;
const XACT_XINFO_HAS_SUBXACTS: u32 = 1 << 1;
const XACT_XINFO_HAS_RELFILELOCATORS: u32 = 1 << 2;
const XACT_XINFO_HAS_INVALS: u32 = 1 << 3;
const XACT_XINFO_HAS_TWOPHASE: u32 = 1 << 4;
const XACT_XINFO_HAS_ORIGIN: u32 = 1 << 5;
const XACT_XINFO_HAS_GID: u32 = 1 << 7;
const XACT_XINFO_HAS_DROPPED_STATS: u32 = 1 << 8;

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

/// Parsed body of `xl_xact_commit` / `xl_xact_abort`. Consumer needs
/// timestamp + subxact list + prepared xid; remaining xinfo tails are
/// skip-walked
#[derive(Debug, Default)]
pub(crate) struct XactCommitPayload {
    pub(crate) xact_time: i64,
    pub(crate) subxacts: Vec<u32>,
    /// `xl_xact_twophase` xid on COMMIT/ABORT PREPARED; the header's
    /// `xact_id` is the finishing backend's, not the prepared xact's
    pub(crate) twophase_xid: Option<u32>,
}

/// Parse `xl_xact_assignment` (PG `access/xact.h`) into `(xtop,
/// subxids)`. `xtop` is canonical: header `xact_id` matches in steady
/// state but payload is documented source of truth.
pub(crate) fn parse_xact_assignment(main_data: &[u8]) -> Option<(u32, Vec<u32>)> {
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

/// Walk `xl_xact_commit` / `xl_xact_abort` main_data per the `xinfo`
/// tail order in PG `xactdesc.c::ParseCommitRecord` / `ParseAbortRecord`.
/// Short read returns default so decoder degrades to "commit_ts unknown,
/// no subxacts" rather than poisoning the stream.
///
/// `info` is record header `info`; `XLOG_XACT_HAS_INFO` (`0x80`) gates
/// the `xinfo` u32 after `xact_time`. Commit/abort-prepared set it too.
pub(crate) fn parse_xact_payload(info: u8, main_data: &[u8]) -> XactCommitPayload {
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
        // dbId + tsId, 2x Oid
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
    // Remaining tails skip-walked only to keep `p` valid for a future
    // reader; none feed the buffer today
    if xinfo & XACT_XINFO_HAS_RELFILELOCATORS != 0 {
        // int32 nrels + RelFileLocator (spc/db/rel Oid): 4 + 12 per entry
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
        // int32 nitems + xl_xact_stats_item (kind + dboid + 2x objid):
        // 4 + 16 per entry
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
        // commit-only per xactdesc.c; int32 nmsgs +
        // SharedInvalidationMessage (16 bytes each)
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
        // xl_xact_twophase: TransactionId (4 bytes)
        if main_data.len() < p + 4 {
            return out;
        }
        out.twophase_xid = Some(u32::from_le_bytes(main_data[p..p + 4].try_into().unwrap()));
        p += 4;
        if xinfo & XACT_XINFO_HAS_GID != 0 {
            // null-terminated GID
            let rest = &main_data[p..];
            let nul = rest.iter().position(|&b| b == 0);
            match nul {
                Some(n) => p += n + 1,
                None => return out,
            }
        }
    }
    if xinfo & XACT_XINFO_HAS_ORIGIN != 0 {
        // xl_xact_origin: XLogRecPtr (8) + TimestampTz (8), unaligned per
        // xactdesc.c
        if main_data.len() < p + 16 {
            return out;
        }
        let _ = p;
    }
    out
}

/// `source_lsn` is the WAL LSN stamped at decode; merge-drain orders by it
fn entry_lsn(e: &SpillEntry) -> u64 {
    match e {
        SpillEntry::Heap(h) => h.source_lsn,
        SpillEntry::Chunk(c) => c.source_lsn,
    }
}

#[derive(Debug, Clone)]
pub struct XactBufferConfig {
    /// In-memory budget across all active xacts before eviction
    pub xact_buffer_max: usize,
    /// Per-xid spill files land here
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
    pub xacts_active: u64,
    /// Bookkeeping estimate; actual heap allocation may differ
    pub bytes_in_memory: u64,
    pub xacts_total: u64,
    pub spill_xacts_active: u64,
    pub spill_bytes_active: u64,
    pub spill_evictions_total: u64,
    pub committed_xacts_total: u64,
    pub aborted_xacts_total: u64,
    /// `COMMIT` records for xids never buffered (read-only/filtered)
    pub commits_unknown_xid: u64,
    /// Aborts for xids never buffered. Runs higher than
    /// `commits_unknown_xid`: aborts often hit xacts that wrote nothing
    pub aborts_unknown_xid: u64,
    /// Highest commit-record LSN drained into observer `on_tuple`.
    /// Snapshot for cursor `drain_lsn`, monotonic
    pub drain_lsn: u64,
    /// Highest commit-record LSN observer `on_xact_end` reported durable.
    /// Snapshot for `cursor.emitter_ack_lsn`, monotonic. Lags `drain_lsn`
    /// when observer (CH emitter `flush_timeout > 0`) holds rows in open
    /// INSERTs
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
    /// Sticky across spill rotations; distinguishes two xids that collide
    /// after a slot rebuild
    first_lsn: u64,
    /// WAL-order by arrival
    in_mem: Vec<SpillEntry>,
    in_mem_bytes: usize,
    /// `None` until first eviction
    spill: Option<SpillWriter>,
    spill_bytes: u64,
    /// source_lsn ASC. Not spilled: events carry rich `RelDescriptor`
    /// data a spill format would duplicate, and DDL per xact is rare
    catalog_events: Vec<(u64, SchemaEvent)>,
    /// Per-txn `txn` span; duration = WAL-record→durable latency.
    span: tracing::Span,
    /// Child of `span` covering first-buffered→COMMIT-observed (parked-for-
    /// commit wait); closed atop `commit`, so `txn = buffer.wait + commit.drain`.
    wait_span: tracing::Span,
}

/// Root `txn` span, opened at `note_popped` so it starts after the pump→worker
/// wait (kept as `fill_ms`/`queue_ms` tags). `parent: None` forces a root so it
/// doesn't collapse into the worker's `batch` span.
fn new_txn_span(xid: u32, first_lsn: u64) -> tracing::Span {
    tracing::info_span!(
        target: "walshadow::trace",
        parent: None,
        "txn",
        xid = xid,
        first_lsn = first_lsn,
        fill_ms = tracing::field::Empty,
        queue_ms = tracing::field::Empty,
        top_xid = tracing::field::Empty,
        rows = tracing::field::Empty,
        spilled = tracing::field::Empty,
        outcome = tracing::field::Empty,
    )
}

/// Per-xid hand-off of `txn` spans between the pump (registers at first
/// sighting) and the worker (opens the span at dequeue; buffer then adopts).
/// Cheap `Arc` clones shared by the pump and [`XactBuffer`].
struct TxnSpans {
    /// Root `txn` span; `None` until `note_popped` opens it.
    txn: Option<tracing::Span>,
    first_lsn: u64,
    /// WAL-read instant; with `shipped_at` yields the `fill_ms` tag.
    read_at: Instant,
    /// Ship instant (`note_shipped`); yields the `queue_ms` tag.
    shipped_at: Option<Instant>,
    /// Clone of `txn` for the first record's `catalog.gate`/`decode`; nulled at
    /// `adopt` so only the first record carries them.
    decode_parent: Option<tracing::Span>,
    /// Head-sampling verdict, decided once at `open`. `false` skips the `txn`
    /// span and the per-record spans for this xact.
    sampled: bool,
}

#[derive(Clone, Default)]
pub struct TxnSpanRegistry {
    inner: Arc<std::sync::Mutex<HashMap<u32, TxnSpans>>>,
}

impl TxnSpanRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Whether `xid`'s trace tree is sampled. `false` for an unknown xid.
    pub fn is_sampled(&self, xid: u32) -> bool {
        self.inner
            .lock()
            .expect("txn span registry poisoned")
            .get(&xid)
            .is_some_and(|e| e.sampled)
    }

    /// Pump side: register `xid` at first sighting, stamping the WAL-read
    /// instant for `fill_ms` and taking the head-sampling decision. The `txn`
    /// span opens later at `note_popped`. No-op for `xid == 0` and for an
    /// already-registered xid (records 2..N).
    pub fn open(&self, xid: u32, first_lsn: u64) {
        if xid == 0 {
            return;
        }
        self.inner
            .lock()
            .expect("txn span registry poisoned")
            .entry(xid)
            .or_insert_with(|| TxnSpans {
                txn: None,
                first_lsn,
                read_at: Instant::now(),
                shipped_at: None,
                decode_parent: None,
                sampled: crate::trace::should_sample(),
            });
    }

    /// Pump-flush side: stamp the ship instant for the `queue_ms` tag.
    /// Idempotent; no-op for `xid == 0`.
    pub fn note_shipped(&self, xid: u32) {
        if xid == 0 {
            return;
        }
        let mut m = self.inner.lock().expect("txn span registry poisoned");
        let Some(e) = m.get_mut(&xid) else { return };
        if e.shipped_at.is_none() {
            e.shipped_at = Some(Instant::now());
        }
    }

    /// Worker side: open the `txn` span (recording the transport wait as
    /// `fill_ms`/`queue_ms`) and stash `decode_parent`. Idempotent; no-op until
    /// shipped. Returns the head-sampling verdict for gating per-record spans.
    pub fn note_popped(&self, xid: u32) -> bool {
        if xid == 0 {
            return false;
        }
        let mut m = self.inner.lock().expect("txn span registry poisoned");
        let Some(e) = m.get_mut(&xid) else {
            return false;
        };
        if !e.sampled || e.txn.is_some() {
            return e.sampled;
        }
        let Some(shipped) = e.shipped_at else {
            return true;
        };
        let txn = new_txn_span(xid, e.first_lsn);
        txn.record(
            "fill_ms",
            shipped.duration_since(e.read_at).as_secs_f64() * 1e3,
        );
        txn.record("queue_ms", shipped.elapsed().as_secs_f64() * 1e3);
        e.decode_parent = Some(txn.clone());
        e.txn = Some(txn);
        true
    }

    /// Reorder side: clone of the open `txn` span to parent commit spans.
    /// `None` if never opened (tracing off / unsampled / not yet dequeued).
    pub fn txn_span(&self, xid: u32) -> Option<tracing::Span> {
        self.inner
            .lock()
            .expect("txn span registry poisoned")
            .get(&xid)
            .and_then(|e| e.txn.clone())
    }

    /// Worker side: hand the `txn` span to the buffer and null `decode_parent`
    /// so only the first record carries `catalog.gate`/`decode`. `None` if
    /// never opened — caller falls back to `new_txn_span`.
    pub fn adopt(&self, xid: u32) -> Option<tracing::Span> {
        let mut m = self.inner.lock().expect("txn span registry poisoned");
        let entry = m.get_mut(&xid)?;
        entry.decode_parent = None;
        entry.txn.clone()
    }

    /// Clone of `txn` for the first record's `catalog.gate`/`decode`; `None`
    /// once adopted, so only the first record gets them.
    pub fn decode_parent(&self, xid: u32) -> Option<tracing::Span> {
        self.inner
            .lock()
            .expect("txn span registry poisoned")
            .get(&xid)?
            .decode_parent
            .clone()
    }

    /// Drop the registry's span handles for a finished xact tree at
    /// commit/abort (buffer's own clones keep the span alive through drain).
    pub fn prune(&self, xids: &[u32]) {
        let mut m = self.inner.lock().expect("txn span registry poisoned");
        for x in xids {
            m.remove(x);
        }
    }
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
            catalog_events: Vec::new(),
            span,
            wait_span,
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

/// Diagnostic surface for "a commit for this xid never arrived";
/// fields cover the four pre-commit absorption paths
#[derive(Debug, Clone)]
pub struct InflightSnapshotEntry {
    pub xid: u32,
    pub first_lsn: u64,
    /// Max source_lsn over heaps + chunks + catalog events; distance from
    /// `first_lsn` shows how long the xact has been open in WAL terms
    pub last_lsn: u64,
    pub heap_count: u64,
    pub chunk_count: u64,
    pub in_mem_bytes: u64,
    pub spilled: bool,
    pub catalog_events: u64,
    /// `(db_node, rel_node)` comma-joined; cross-reference shadow's
    /// `pg_class.relfilenode` to name the table without a catalog lookup
    pub rels: String,
}

/// Per-xact + TOAST buffer with spill-to-disk overflow, keyed by `xid`
pub struct XactBuffer {
    config: XactBufferConfig,
    store: SpillStore,
    inflight: HashMap<u32, XactState>,
    bytes_in_memory: usize,
    stats: XactBufferStats,
    /// Shared with the WAL pump: the pump opens a `txn` span here at first
    /// sighting of an xid, and `absorb` adopts it so the span starts at
    /// WAL-read rather than at buffering. Empty (and unused) when tracing
    /// is off — `absorb` then mints its own span.
    span_registry: TxnSpanRegistry,
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
            span_registry: TxnSpanRegistry::new(),
        })
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

    /// Detoast descriptor is fetched in [`XactBuffer::commit`] on demand:
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

    /// Drains in `source_lsn` order at commit, so a DDL's `Added`/`Changed`
    /// event lands BEFORE the heap writes that follow it
    pub fn on_schema_event(&mut self, xid: u32, source_lsn: u64, event: SchemaEvent) {
        let is_new = !self.inflight.contains_key(&xid);
        let registry = &self.span_registry;
        let st = self.inflight.entry(xid).or_insert_with(|| {
            let span = registry.adopt(xid).unwrap_or_else(|| {
                if registry.is_sampled(xid) {
                    new_txn_span(xid, source_lsn)
                } else {
                    tracing::Span::none()
                }
            });
            XactState::new(source_lsn, span)
        });
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
        let registry = &self.span_registry;
        let st = self.inflight.entry(xid).or_insert_with(|| {
            let span = registry.adopt(xid).unwrap_or_else(|| {
                if registry.is_sampled(xid) {
                    new_txn_span(xid, first_lsn)
                } else {
                    tracing::Span::none()
                }
            });
            XactState::new(first_lsn, span)
        });
        if let Some(spill) = st.spill.as_mut() {
            // Already spilling: append straight to disk
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

    /// Drain xact `xid` to `observer` in WAL order, substituting each
    /// `ExternalToast` with its reassembled `Bytea` / `Text` via
    /// [`ShadowCatalog::relation_at`]. No-op for unknown `xid`
    /// (read-only or filter-dropped).
    ///
    /// `commit_lsn` is the `XLOG_XACT_COMMIT` record LSN, stamped into
    /// [`CommittedTuple::commit_lsn`] and bumped into `drain_lsn` /
    /// `emitter_ack_lsn` as the cursor resume gate's monotonic mark.
    #[allow(clippy::too_many_arguments)]
    pub async fn commit<O: TupleObserver>(
        &mut self,
        top_xid: u32,
        commit_ts: i64,
        commit_lsn: u64,
        subxids: &[u32],
        catalog: &Arc<Mutex<ShadowCatalog>>,
        observer: &mut O,
        resolver: &ToastResolver,
    ) -> std::result::Result<(), XactBufferError> {
        // Only the top counts for `commits_unknown_xid` so the metric
        // stays per-xact, not per-subxid
        let mut xids: Vec<u32> = Vec::with_capacity(1 + subxids.len());
        xids.push(top_xid);
        xids.extend_from_slice(subxids);
        // XactState clones carry the span through the drain; just bound the map.
        self.span_registry.prune(&xids);
        let mut states: Vec<XactState> = Vec::with_capacity(xids.len());
        for x in &xids {
            if let Some(st) = self.inflight.remove(x) {
                states.push(st);
            }
        }

        if states.is_empty() {
            self.stats.commits_unknown_xid += 1;
            // Read-only / filter-dropped still advances the ack ceiling so
            // the slot can recycle WAL up to commit_lsn. Route through
            // `on_xact_end` anyway so an observer holding prior rows in
            // open inserts clamps the ack at its own durable horizon,
            // else we'd claim durability for client-buffered rows
            self.stats.drain_lsn = self.stats.drain_lsn.max(commit_lsn);
            let ack_lsn = observer
                .on_xact_end(commit_lsn)
                .await
                .map_err(|e| XactBufferError::Observer(e.to_string()))?;
            self.stats.emitter_ack_lsn = self.stats.emitter_ack_lsn.max(ack_lsn);
            return Ok(());
        }
        // Concurrency at commit time, counted before we tick this xact's
        // ids back out. A high value sitting next to a long `commit.drain`
        // is the multi-source-connection head-of-line-blocking signature:
        // many xacts live, one fat INSERT draining while the rest wait.
        let xacts_active_at_commit = self.stats.xacts_active;
        // Active counter ticks down once per drained xact (top + subs).
        for st in &states {
            self.stats.xacts_active = self.stats.xacts_active.saturating_sub(1);
            self.bytes_in_memory = self.bytes_in_memory.saturating_sub(st.in_mem_bytes);
        }
        self.stats.bytes_in_memory = self.bytes_in_memory as u64;

        // Clone the per-txn span so it survives the `states` drain and stays
        // open until `outcome` is recorded; `commit.drain` is its child.
        let txn_span = states
            .first()
            .map(|s| s.span.clone())
            .unwrap_or_else(tracing::Span::none);
        txn_span.record("top_xid", top_xid);
        // Close each `buffer.wait`, so it spans exactly first-record → commit.
        for st in &mut states {
            st.wait_span = tracing::Span::none();
        }
        // Wall-clock age of PG's commit at the moment we process it. The
        // WAL commit record carries PG's commit timestamp (PG-epoch µs);
        // shifted to unix µs it's directly comparable to our clock (all
        // demo containers share the host clock). This is the PG-commit →
        // walshadow-processes-commit leg — large here means the wait is
        // upstream (reading WAL / queue backlog), not in our drain.
        let pg_commit_age_ms = {
            let now_us = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_micros() as i64)
                .unwrap_or(0);
            let commit_unix_us =
                commit_ts.saturating_add(crate::ch_emitter::DATETIME64_PG_EPOCH_US);
            (now_us - commit_unix_us) as f64 / 1000.0
        };
        let drain_span = trace_span!(
            !txn_span.is_none(),
            parent: &txn_span,
            "commit.drain",
            top_xid = top_xid,
            commit_lsn = commit_lsn,
            subxids = subxids.len(),
            xacts_active = xacts_active_at_commit,
            pg_commit_age_ms = pg_commit_age_ms,
            rows = tracing::field::Empty,
            spilled = states.iter().any(|s| s.spill.is_some()),
        );

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
            // Arrival order == WAL order: decoder sink pushes on observe,
            // so the Vec is already source_lsn ASC
            let cat: VecDeque<(u64, SchemaEvent)> = std::mem::take(&mut st.catalog_events).into();
            per_xid_catalog.push(cat);
        }

        // k-way merge of per_xid heads by `source_lsn` ASC. k = 1 +
        // nsubxacts, typically <= 4, so linear head-pick beats a heap
        let mut heaps: Vec<DecodedHeap> = Vec::new();
        let mut chunks: HashMap<(u32, u32), BTreeMap<u32, Vec<u8>>> = HashMap::new();
        let mut ordered_events: Vec<(usize, SchemaEvent)> = Vec::new();
        // `merge` brackets the (fully synchronous) k-way merge — confirms
        // it's cheap rather than an O(n²) surprise on subxact-heavy xacts.
        // Entered only across sync work; dropped before the dispatch awaits
        // below (an entered guard held across `.await` would be unsound).
        let merge_guard = trace_span!(
            !drain_span.is_none(),
            parent: &drain_span,
            "merge",
            xacts = per_xid.len(),
            entries = per_xid.iter().map(VecDeque::len).sum::<usize>(),
        )
        .entered();
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
            // Catalog wins ties: PG writes the DDL catalog mutation before
            // the dependent heap, and the lazy refetch stamps the schema
            // event with the triggering heap's source_lsn, so they share
            // an LSN. Catalog-first lands the `ALTER` on CH before the
            // dependent INSERT encodes against the post-DDL shape
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
                    // Event sorts before heaps[heaps.len()]
                    ordered_events.push((heaps.len(), ev));
                }
            }
        }
        drop(merge_guard);
        drain_span.record("rows", heaps.len());

        let mut event_cursor = 0usize;
        for (heap_idx, mut heap) in heaps.into_iter().enumerate() {
            while event_cursor < ordered_events.len() && ordered_events[event_cursor].0 <= heap_idx
            {
                let ev = &ordered_events[event_cursor].1;
                observer
                    .on_schema_event(ev)
                    .instrument(drain_span.clone())
                    .await
                    .map_err(|e| XactBufferError::Observer(e.to_string()))?;
                event_cursor += 1;
            }
            detoast_heap(&mut heap, &chunks, catalog, false, resolver).await?;
            let committed = CommittedTuple {
                decoded: heap,
                commit_ts,
                commit_lsn,
            };
            // Under `drain_span` so the emitter's `emit.insert` (the
            // blocking ClickHouse round-trip, sealed on a budget trip or at
            // xact end) attaches as a child of this transaction's drain.
            observer
                .on_tuple(&committed)
                .instrument(drain_span.clone())
                .await
                .map_err(|e| XactBufferError::Observer(e.to_string()))?;
        }
        // Trailing events with no heap after them
        while event_cursor < ordered_events.len() {
            let ev = &ordered_events[event_cursor].1;
            observer
                .on_schema_event(ev)
                .instrument(drain_span.clone())
                .await
                .map_err(|e| XactBufferError::Observer(e.to_string()))?;
            event_cursor += 1;
        }
        // drain_lsn ticks before the ack so an observer failure leaves
        // drain_lsn ahead of emitter_ack_lsn, the gap the cursor surfaces.
        // With CH flush_timeout > 0, on_xact_end returns last durable
        // commit_lsn (<= commit_lsn), so emitter_ack_lsn lags by design
        self.stats.drain_lsn = self.stats.drain_lsn.max(commit_lsn);
        // `ack` covers the durability handshake. In hold-open mode this is
        // also where a still-open INSERT gets sealed, so `emit.insert` can
        // appear under it; `held_open` flags an ack that lagged commit_lsn.
        let ack_span = trace_span!(
            !drain_span.is_none(),
            parent: &drain_span,
            "ack",
            commit_lsn = commit_lsn,
            ack_lsn = tracing::field::Empty,
            held_open = tracing::field::Empty,
        );
        let ack_lsn = observer
            .on_xact_end(commit_lsn)
            .instrument(ack_span.clone())
            .await
            .map_err(|e| XactBufferError::Observer(e.to_string()))?;
        ack_span.record("ack_lsn", ack_lsn);
        ack_span.record("held_open", ack_lsn < commit_lsn);
        let prev_ack = self.stats.emitter_ack_lsn;
        self.stats.emitter_ack_lsn = self.stats.emitter_ack_lsn.max(ack_lsn);
        tracing::trace!(
            target: "walshadow::xact_buffer",
            top_xid,
            commit_lsn = format_pg_lsn(commit_lsn).to_string(),
            ack_lsn = format_pg_lsn(ack_lsn).to_string(),
            prev_ack = format_pg_lsn(prev_ack).to_string(),
            "drain complete",
        );
        // One bump per top, not per subxid
        self.stats.committed_xacts_total += 1;
        txn_span.record("outcome", "committed");
        Ok(())
    }

    /// Parallel-pipeline drain: k-way merge by `source_lsn`, return
    /// still-toasted heaps + chunk map + interleaved events *without*
    /// detoast or dispatch (those move to the decode pool / barrier
    /// coordinator). Unlike [`Self::commit`], leaves `emitter_ack_lsn`
    /// to the pipeline ack collector.
    pub async fn drain_committed(
        &mut self,
        top_xid: u32,
        commit_ts: i64,
        commit_lsn: u64,
        subxids: &[u32],
    ) -> std::result::Result<DrainedXact, XactBufferError> {
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
            return Ok(DrainedXact {
                commit_ts,
                commit_lsn,
                heaps: Vec::new(),
                chunks: HashMap::new(),
                ordered_events: Vec::new(),
                had_states: false,
            });
        }
        for st in &states {
            self.stats.xacts_active = self.stats.xacts_active.saturating_sub(1);
            self.bytes_in_memory = self.bytes_in_memory.saturating_sub(st.in_mem_bytes);
        }
        self.stats.bytes_in_memory = self.bytes_in_memory as u64;

        // Spill (older) then in-mem per xid, source_lsn-ASC; see [`Self::commit`]
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
            let cat: VecDeque<(u64, SchemaEvent)> = std::mem::take(&mut st.catalog_events).into();
            per_xid_catalog.push(cat);
        }

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
            // Catalog events tie-break first: PG writes the DDL's catalog
            // mutation before the dependent heap, and they can share an LSN.
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
                    ordered_events.push((heaps.len(), ev));
                }
            }
        }
        self.stats.committed_xacts_total += 1;
        Ok(DrainedXact {
            commit_ts,
            commit_lsn,
            heaps,
            chunks,
            ordered_events,
            had_states: true,
        })
    }

    /// When no xact in flight, advance `drain_lsn` to `lsn` and
    /// `emitter_ack_lsn` to `min(lsn, ack_ceiling)`. Lets the slot recycle
    /// trailing post-COMMIT WAL (page padding, RUNNING_XACTS, CHECKPOINT)
    /// when quiescent. `ack_ceiling` is the observer's durable horizon: in
    /// hold-open mode rows sit in open INSERTs, so the ack must not pass
    /// what's durable else source recycles unwritten WAL.
    pub fn advance_idle(&mut self, lsn: u64, ack_ceiling: u64) {
        if self.stats.xacts_active != 0 {
            return;
        }
        self.stats.drain_lsn = self.stats.drain_lsn.max(lsn);
        self.stats.emitter_ack_lsn = self.stats.emitter_ack_lsn.max(lsn.min(ack_ceiling));
    }

    /// Fold an observer-reported durable LSN into `emitter_ack_lsn` only;
    /// `drain_lsn` already covers commit boundaries
    pub fn note_idle_durable(&mut self, lsn: u64) {
        self.stats.emitter_ack_lsn = self.stats.emitter_ack_lsn.max(lsn);
    }

    /// Discard xact `xid` + spill file. No-op if unknown. `abort_lsn` is
    /// the `XLOG_XACT_ABORT` record LSN; advances `drain_lsn` /
    /// `emitter_ack_lsn` so the cursor counts aborts as fully consumed
    pub async fn abort(
        &mut self,
        xid: u32,
        abort_lsn: u64,
        subxids: &[u32],
    ) -> std::result::Result<(), XactBufferError> {
        self.stats.drain_lsn = self.stats.drain_lsn.max(abort_lsn);
        self.stats.emitter_ack_lsn = self.stats.emitter_ack_lsn.max(abort_lsn);
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

/// Committed xact for the parallel pipeline. Heaps still TOAST-toasted;
/// decode pool handles detoast + routing. Non-empty `ordered_events` (or
/// a `HeapOp::Truncate` heap) makes this a barrier the reorder
/// coordinator serializes against ClickHouse.
pub struct DrainedXact {
    pub commit_ts: i64,
    pub commit_lsn: u64,
    pub heaps: Vec<DecodedHeap>,
    pub chunks: HashMap<(u32, u32), BTreeMap<u32, Vec<u8>>>,
    /// Event sorts before `heaps[heap_index]`
    pub ordered_events: Vec<(usize, SchemaEvent)>,
    /// False for read-only / filter-dropped / unknown xid
    pub had_states: bool,
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

pub(crate) async fn detoast_heap(
    heap: &mut DecodedHeap,
    chunks: &ChunkMap,
    catalog: &Arc<Mutex<ShadowCatalog>>,
    // Async decode pool can lag past a DDL and re-resolve an older heap,
    // so it reuses the inline cached descriptor without mutating cache or
    // emitting events (`ShadowCatalog::relation_at_pooled`). WAL-ordered
    // observer drain passes `false` for the normal cache-populating path.
    pooled: bool,
    resolver: &ToastResolver,
) -> std::result::Result<(), XactBufferError> {
    let needs = tuple_needs_detoast(heap.new.as_ref()) || tuple_needs_detoast(heap.old.as_ref());
    if !needs {
        return Ok(());
    }
    let rel: Arc<RelDescriptor> = {
        let mut cat = catalog.lock().await;
        if pooled {
            cat.relation_at_pooled(heap.rfn, heap.source_lsn).await?
        } else {
            cat.relation_at(heap.rfn, heap.source_lsn).await?
        }
    };
    // Pre-window / bootstrap values whose chunks aren't in this xact: pull
    // them from the durable store into a per-heap supplemental map. Disabled
    // mode (no store) leaves `extra` empty; a miss then NULL/default-fills.
    let mut extra = ChunkMap::new();
    if resolver.stores_chunks() {
        let mut keys: Vec<(u32, u32)> = Vec::new();
        collect_toast_keys(heap.new.as_ref(), &mut keys);
        collect_toast_keys(heap.old.as_ref(), &mut keys);
        for (relid, value_id) in keys {
            if chunks.contains_key(&(relid, value_id)) || extra.contains_key(&(relid, value_id)) {
                continue;
            }
            resolver
                .fetch_into(relid, value_id, &mut extra)
                .await
                .map_err(|e| XactBufferError::Detoast(format!("toast store fetch: {e}")))?;
        }
    }
    let maps: [&ChunkMap; 2] = [chunks, &extra];
    if let Some(t) = heap.new.as_mut() {
        detoast_tuple(t, &rel, &maps, resolver)?;
    }
    if let Some(t) = heap.old.as_mut() {
        detoast_tuple(t, &rel, &maps, resolver)?;
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

/// Append `(va_toastrelid, va_valueid)` for every on-disk toast pointer.
fn collect_toast_keys(t: Option<&crate::heap_decoder::DecodedTuple>, out: &mut Vec<(u32, u32)>) {
    let Some(t) = t else {
        return;
    };
    for c in &t.columns {
        if let Some(ColumnValue::ExternalToast(p)) = c {
            out.push((p.va_toastrelid, p.va_valueid));
        }
    }
}

fn detoast_tuple(
    t: &mut crate::heap_decoder::DecodedTuple,
    rel: &RelDescriptor,
    maps: &[&ChunkMap],
    resolver: &ToastResolver,
) -> std::result::Result<(), XactBufferError> {
    for (idx, col) in t.columns.iter_mut().enumerate() {
        let Some(ColumnValue::ExternalToast(p)) = col else {
            continue;
        };
        // `ToastPointer: Copy` frees the borrow on `col` before reassign
        let p: ToastPointer = *p;
        let type_oid = rel.attributes.get(idx).map(|a| a.type_oid).unwrap_or(0);
        match try_reassemble(&p, maps)? {
            Some(raw) => *col = Some(detoasted_value(raw, type_oid)),
            // Disabled mode: surface the unresolvable value as NULL/default
            // downstream (`append_default`), counted, never an error.
            None if resolver.fill_on_miss() => {
                resolver.note_filled_default();
                *col = Some(ColumnValue::Null);
            }
            // Active store still couldn't rebuild it: a real chunk gap.
            None => {
                resolver.note_fetch_miss();
                return Err(XactBufferError::MissingToastChunk {
                    toast_relid: p.va_toastrelid,
                    value_id: p.va_valueid,
                    missing: first_missing_seq(&p, maps),
                });
            }
        }
    }
    Ok(())
}

/// Reassembled `ExternalToast` bytes → typed value, routed through the same
/// `varlena_to_value` the inline decoder uses so a detoasted value resolves
/// identically to an inline one. Tier 3 types (jsonb, arrays, large numerics)
/// land as `PgPending`, resolved at emit by the oracle. `raw` is the
/// header-stripped, decompressed varlena body, matching the inline `body`;
/// passed owned so the (large) reassembled buffer moves into the value with
/// no copy.
pub(crate) fn detoasted_value(raw: Vec<u8>, type_oid: u32) -> ColumnValue {
    crate::heap_decoder::varlena_to_value(type_oid, std::borrow::Cow::Owned(raw))
}

/// First seq missing from the dense 0..N run, for the error message. `0` when
/// no chunks at all. Only walked on the error path.
fn first_missing_seq(p: &ToastPointer, maps: &[&ChunkMap]) -> u32 {
    let key = (p.va_toastrelid, p.va_valueid);
    match maps.iter().find_map(|m| m.get(&key)) {
        None => 0,
        Some(map) => {
            for (expected, seq) in map.keys().enumerate() {
                if *seq != expected as u32 {
                    return expected as u32;
                }
            }
            map.len() as u32
        }
    }
}

use crate::heap_decoder::{VARLENA_EXTSIZE_BITS, VARLENA_EXTSIZE_MASK, decompress_varlena};

/// PG `VARHDRSZ`, 4-byte varlena header
const VARHDRSZ: i32 = 4;

/// Concatenate a value's chunks (seq 0..N dense) then decompress per the
/// pointer's method. Looks the value up across `maps` in order (in-xact
/// chunk map first, then a store-fetched supplement). `Ok(None)` when chunks
/// are absent or non-dense — the caller decides fill vs error; `Err` only on
/// a genuine decompression failure.
pub(crate) fn try_reassemble(
    p: &ToastPointer,
    maps: &[&ChunkMap],
) -> std::result::Result<Option<Vec<u8>>, XactBufferError> {
    let key = (p.va_toastrelid, p.va_valueid);
    let Some(map) = maps.iter().find_map(|m| m.get(&key)) else {
        return Ok(None);
    };
    let total: usize = map.values().map(Vec::len).sum();
    let mut concat: Vec<u8> = Vec::with_capacity(total);
    for (expected, (seq, body)) in map.iter().enumerate() {
        if *seq != expected as u32 {
            // Gap in the 0..N run: incomplete, treat as a miss.
            return Ok(None);
        }
        concat.extend_from_slice(body);
    }
    let compressed = (p.va_extinfo & !VARLENA_EXTSIZE_MASK) != 0;
    if !compressed {
        return Ok(Some(concat));
    }
    let method = ((p.va_extinfo >> VARLENA_EXTSIZE_BITS) & 0x3) as u8;
    let raw_len = (p.va_rawsize - VARHDRSZ).max(0) as usize;
    match decompress_varlena(method, &concat, raw_len) {
        Some(out) => Ok(Some(out)),
        None => Err(XactBufferError::Detoast(format!(
            "decompress failed (method {method}, {} bytes → {raw_len})",
            concat.len()
        ))),
    }
}

/// `RecordSink` for `RM_XACT_ID` records. Separate from
/// [`BufferingDecoderSink`] because xact records are `Route::ToShadow`
/// (shadow PG needs them for CLOG) so the decoder sink skips them.
pub struct XactRecordSink<O: TupleObserver + Send> {
    buffer: Arc<Mutex<XactBuffer>>,
    catalog: Arc<Mutex<ShadowCatalog>>,
    /// Hint only: canonical subxact list arrives inline on commit / abort.
    /// Tracker covers eviction policy needing the family before COMMIT
    subxact_tracker: Arc<Mutex<SubxactTracker>>,
    observer: O,
    /// Same handle [`BufferingDecoderSink`] holds; this sink drains it
    /// post-`sweep_dropped` to route `Dropped` events into the buffer
    schema_events: Option<SchemaEventRx>,
    /// Armed by [`BufferingDecoderSink`] at pg_class heap_delete records,
    /// consumed here only at the arming xact's own commit — an epoch
    /// consumed at an interleaved commit sweeps before the drop is
    /// commit-visible in shadow and loses the Dropped event. Also skips
    /// the per-commit catalog-lock acquire when no DROP fired; that lock
    /// contends with `wait_for_replay` and serialised the drain at
    /// pgbench rates
    pending_sweeps: Option<crate::catalog_tracker::PendingSweeps>,
    /// Serial drain is metrics-only (no `--ch-config`), so this stays
    /// disabled by default: same-xact values reassemble inline, misses
    /// NULL/default-fill.
    resolver: ToastResolver,
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
            pending_sweeps: None,
            resolver: ToastResolver::disabled(),
        }
    }

    pub fn with_toast_resolver(mut self, resolver: ToastResolver) -> Self {
        self.resolver = resolver;
        self
    }

    /// Unset: sink owns a private tracker
    pub fn with_subxact_tracker(mut self, tracker: Arc<Mutex<SubxactTracker>>) -> Self {
        self.subxact_tracker = tracker;
        self
    }

    /// Sink runs [`ShadowCatalog::sweep_dropped`] at armed commits;
    /// resulting `Dropped` events flow into the buffer keyed on the
    /// commit's xid + LSN. Pass the same `rx` as
    /// [`BufferingDecoderSink::with_schema_events`].
    pub fn with_schema_events(mut self, rx: SchemaEventRx) -> Self {
        self.schema_events = Some(rx);
        self
    }

    /// Pass the same handle as
    /// [`BufferingDecoderSink::with_catalog_signals`]; gates
    /// [`ShadowCatalog::sweep_dropped`] on the arming xact's commit
    /// without the contended catalog lock.
    pub fn with_pending_sweeps(mut self, pending: crate::catalog_tracker::PendingSweeps) -> Self {
        self.pending_sweeps = Some(pending);
        self
    }

    pub fn observer_mut(&mut self) -> &mut O {
        &mut self.observer
    }

    pub fn subxact_tracker(&self) -> &Arc<Mutex<SubxactTracker>> {
        &self.subxact_tracker
    }

    /// Route queued [`SchemaEvent`]s into the buffer keyed on `(xid, source_lsn)`
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
                    // Poll-based DROP TABLE discovery at commit. PG system
                    // catalogs default to `relreplident = 'n'`, so
                    // heap_delete WAL omits the dying oid; only way to
                    // detect a drop is asking shadow if known oids still
                    // exist. Arming fires only on heap_delete against
                    // pg_class (DROP TABLE's WAL signature), not on the
                    // ADD COLUMN / CREATE INDEX / VACUUM catalog flood;
                    // consumption only at the arming xact's own commit,
                    // whose replay gate makes the drop visible in shadow.
                    if self.schema_events.is_some()
                        && let Some(pending) = &self.pending_sweeps
                        && pending.disarm(xid, payload.twophase_xid, &payload.subxacts)
                    {
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
                        if dropped_count > 0 {
                            self.route_pending_schema_events(xid, record.source_lsn)
                                .await?;
                        }
                    }
                    let mut buf = self.buffer.lock().await;
                    // Surfaces subxact-id mismatches between heap-record
                    // xact_id and the top's commit-record subxact list
                    tracing::trace!(
                        target: "walshadow::xact_buffer",
                        xid,
                        commit_lsn = format_pg_lsn(record.source_lsn).to_string(),
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
                        &self.resolver,
                    )
                    .await
                    .map_err(SinkError::from)?;
                    drop(buf);
                    self.subxact_tracker.lock().await.forget_tree(xid);
                }
                XLOG_XACT_ABORT | XLOG_XACT_ABORT_PREPARED => {
                    let payload = parse_xact_payload(info, &record.parsed.main_data);
                    // Rolled-back pg_class heap_delete resurrects the row;
                    // no sweep, drop the arm
                    if let Some(pending) = &self.pending_sweeps {
                        pending.disarm(xid, payload.twophase_xid, &payload.subxacts);
                    }
                    tracing::trace!(
                        target: "walshadow::xact_buffer",
                        xid,
                        abort_lsn = format_pg_lsn(record.source_lsn).to_string(),
                        nsubxacts = payload.subxacts.len(),
                        "xact abort",
                    );
                    let mut buf = self.buffer.lock().await;
                    buf.abort(xid, record.source_lsn, &payload.subxacts)
                        .await
                        .map_err(SinkError::from)?;
                    drop(buf);
                    // Harmless for standalone subxact abort (drops the
                    // single edge); top abort clears the family
                    self.subxact_tracker.lock().await.forget_tree(xid);
                }
                XLOG_XACT_ASSIGNMENT => {
                    // Hint for eviction policy; correctness rides on the
                    // commit / abort record's authoritative subxact list
                    if let Some((xtop, subs)) = parse_xact_assignment(&record.parsed.main_data) {
                        self.subxact_tracker.lock().await.assign(xtop, &subs);
                    }
                }
                _ => {
                    // XLOG_XACT_PREPARE / INVALIDATIONS unhandled. 2PC
                    // deferred: xact stays buffered until COMMIT_PREPARED
                }
            }
            Ok(())
        })
    }

    /// Buffer has no time-based work; hook exists so the CH emitter's
    /// hold-INSERT-open deadline fires without waiting on next commit
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
            // Deadline close promotes the durable horizon; fold into ack
            // so retention advances with no further commit
            if durable != 0 {
                self.buffer.lock().await.note_idle_durable(durable);
            }
            Ok(())
        })
    }

    /// Final force-flush for the CH emitter's hold-INSERT-open path;
    /// without it, rows buffered at shutdown stay non-durable
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
            // Cap ack at the durable horizon: nudge must not promote past
            // rows held in open INSERTs
            let ceiling = self.observer.idle_ack_ceiling(lsn);
            let mut buf = self.buffer.lock().await;
            buf.advance_idle(lsn, ceiling);
            Ok(())
        })
    }
}

/// Shared schema-event receiver. `std::sync::Mutex` wrapper lets both
/// [`BufferingDecoderSink`] (drain after every `relation_at`) and
/// [`XactRecordSink`] (drain after every `sweep_dropped`) pull from the
/// same queue.
pub type SchemaEventRx = Arc<std::sync::Mutex<tokio::sync::mpsc::UnboundedReceiver<SchemaEvent>>>;

/// Decodes `Route::ToDecoder` user-heap records into the xact buffer.
/// Toast-relation INSERTs (`rel.kind == 't'`) reinterpret as
/// [`ToastChunk`]; semantic errors absorb into [`DecoderStats`] rather
/// than poison the stream.
pub struct BufferingDecoderSink {
    catalog: Arc<Mutex<ShadowCatalog>>,
    buffer: Arc<Mutex<XactBuffer>>,
    stats: Arc<DecoderStats>,
    /// `None` keeps the sink schema-unaware (greenfield bootstrap, tests)
    schema_events: Option<SchemaEventRx>,
    /// `txn` span registry. When set (tracing on), the decoder parents its
    /// per-record `catalog.gate`/`decode` spans under the xact's `txn` span
    /// (via `decode_parent`, set only for the first record), so the
    /// shadow-replay gate shows up nested inside the transaction's span.
    /// `None` ⇒ those spans are skipped (no parent to attach to).
    span_registry: Option<TxnSpanRegistry>,
    /// Per-`rfn` descriptor memo (see [`RelCache`]). Lazily created on the
    /// first lookup, since `new` can't take the async catalog lock.
    rel_cache: Option<RelCache<Arc<RelDescriptor>>>,
    /// Bump target for [`Record::catalog_signal`], the sole record-ordered
    /// bump site (mapping writes + SIGHUP reload bump out-of-band). This
    /// sink runs on the queueing worker, which can lag the pump by
    /// thousands of records; an observe-time bump would be consumable by a
    /// pre-DDL record's lookup, which fetches from a shadow that hasn't
    /// replayed the DDL's commit and caches the pre-DDL descriptor as
    /// fresh with no later bump to flush it. Bumping when the DDL record
    /// itself passes through keeps consumption in-order: any later lookup
    /// of the altered relation is triggered by a record past the DDL's
    /// commit (AccessExclusive excludes interleaved rows), so its replay
    /// gate guarantees a fresh fetch. `None` (bootstrap, tests without an
    /// epoch) skips the bump
    invalidation_epoch: Option<Arc<std::sync::atomic::AtomicU64>>,
    /// [`CatalogSignal::InvalidateSweep`](crate::catalog_tracker::CatalogSignal::InvalidateSweep)
    /// sibling of `invalidation_epoch`: arms `ShadowCatalog::sweep_dropped`
    /// at worker position, keyed by the record's xid so the commit sink
    /// consumes it only at that xact's own commit
    pending_sweeps: Option<crate::catalog_tracker::PendingSweeps>,
}

impl BufferingDecoderSink {
    pub fn new(catalog: Arc<Mutex<ShadowCatalog>>, buffer: Arc<Mutex<XactBuffer>>) -> Self {
        Self {
            catalog,
            buffer,
            stats: Arc::new(DecoderStats::default()),
            schema_events: None,
            span_registry: None,
            rel_cache: None,
            invalidation_epoch: None,
            pending_sweeps: None,
        }
    }

    /// Wire [`Record::catalog_signal`] targets: the invalidation-epoch
    /// bump (same `Arc` as `ShadowCatalog::set_invalidation_epoch`)
    /// and the DROP-sweep armer (same handle as the commit sink's
    /// `with_pending_sweeps`). Production must set these whenever the
    /// sink runs behind a `QueueingRecordSink`.
    pub fn with_catalog_signals(
        mut self,
        invalidation: Arc<std::sync::atomic::AtomicU64>,
        pending_sweeps: Option<crate::catalog_tracker::PendingSweeps>,
    ) -> Self {
        self.invalidation_epoch = Some(invalidation);
        self.pending_sweeps = pending_sweeps;
        self
    }

    /// Wire the [`TxnSpanRegistry`] so per-record decode spans nest under the
    /// xact's `txn` span. Pass the same registry the WAL pump registers xids
    /// into ([`XactBuffer::span_registry`]).
    pub fn with_span_registry(mut self, registry: TxnSpanRegistry) -> Self {
        self.span_registry = Some(registry);
        self
    }

    /// Share the same `rx` with [`XactRecordSink::with_schema_events`]:
    /// channel drains from both sides (decoder for Added/Changed at fetch
    /// time, xact-record sink for Dropped at commit via `sweep_dropped`).
    pub fn with_schema_events(mut self, rx: SchemaEventRx) -> Self {
        self.schema_events = Some(rx);
        self
    }

    pub fn stats(&self) -> &DecoderStats {
        &self.stats
    }

    pub fn stats_handle(&self) -> Arc<DecoderStats> {
        self.stats.clone()
    }

    /// Route catalog-fetch [`SchemaEvent`]s into the buffer stamped
    /// `(xid, source_lsn)` so the commit drain sorts them with the heap
    /// writes that triggered the refetch.
    async fn drain_schema_events(
        &mut self,
        xid: u32,
        source_lsn: u64,
    ) -> std::result::Result<(), SinkError> {
        let Some(rx) = self.schema_events.as_ref() else {
            return Ok(());
        };
        // Heap2 VACUUM / FREEZE carry xact_id=0 (non-transactional) but
        // still drive `relation_at`. Buffering under xid=0 creates an
        // inflight entry that never commits, pinning `emitter_ack_lsn`
        // behind a phantom xact; leave events for the next real-xid drain.
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

    /// Push one `HeapOp::Truncate` per relation. TRUNCATE uniquely
    /// carries pg_class OIDs (not relfilenodes) and no block ref, so the
    /// standard `relation_at(rfn)` path doesn't fit.
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
        // Gate on shadow replaying past source_lsn so each relid's
        // pg_class entry is fresh
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
            // relation_by_oid may emit Added/Changed if the rel rotated
            self.drain_schema_events(xid, source_lsn).await?;
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
            // Bump before anything else so subsequent records' lookups
            // (this record routes ToShadow, no lookups of its own) see the
            // signal in stream order (see `invalidation_epoch` field doc)
            if record.catalog_signal != crate::catalog_tracker::CatalogSignal::None {
                if let Some(e) = &self.invalidation_epoch {
                    e.fetch_add(1, std::sync::atomic::Ordering::Release);
                }
                if record.catalog_signal == crate::catalog_tracker::CatalogSignal::InvalidateSweep
                    && let Some(p) = &self.pending_sweeps
                {
                    p.arm(record.parsed.header.xact_id);
                }
            }
            let rm = record.parsed.header.resource_manager_id;
            // TRUNCATE rides Route::ToShadow (shadow replays it) but the
            // decoder still fans out per-relid HeapOp::Truncate for CH.
            // Handle before the Drop gate, regardless of filter score.
            if rm == RmId::Heap as u8 {
                let info_op = record.parsed.header.info & crate::heap_decoder::XLOG_HEAP_OPMASK;
                if info_op == crate::heap_decoder::XLOG_HEAP_TRUNCATE {
                    return self.handle_truncate(record).await;
                }
            }
            if record.route != Route::ToDecoder {
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
            // `decode_parent` set only for the first record (holds the replay
            // gate): `catalog.gate` wraps `relation_at`, `decode` the parse.
            let txn_xid = record.parsed.header.xact_id;
            let sampled = self
                .span_registry
                .as_ref()
                .is_some_and(|r| r.is_sampled(txn_xid));
            let decode_parent = self
                .span_registry
                .as_ref()
                .and_then(|r| r.decode_parent(txn_xid));
            // A hit skips the catalog lock + `relation_at` replay gate; safe
            // because an unchanged epoch means no DDL invalidated the descriptor.
            let cached = self.rel_cache.as_mut().and_then(|c| {
                c.refresh();
                c.get(rfn).cloned()
            });
            let rel = if let Some(desc) = cached {
                desc
            } else {
                let gate_span = decode_parent
                    .as_ref()
                    .map(|p| {
                        tracing::info_span!(
                            target: "walshadow::trace",
                            parent: p,
                            "catalog.gate",
                            lsn = record.source_lsn,
                        )
                    })
                    .unwrap_or_else(tracing::Span::none);
                // Per-record sibling of `catalog.gate` for the batch view.
                let catalog_span = trace_span!(sampled, "catalog", lsn = record.source_lsn);
                let resolved = {
                    let mut cat = self.catalog.lock().await;
                    if self.rel_cache.is_none() {
                        self.rel_cache = Some(RelCache::new(cat.invalidation_epoch_handle()));
                    }
                    match cat
                        .relation_at(rfn, record.source_lsn)
                        .instrument(gate_span)
                        .instrument(catalog_span)
                        .await
                    {
                        Ok(r) => r,
                        Err(CatalogError::NotFoundByFilenode(_)) => {
                            self.stats
                                .catalog_not_found
                                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            return Ok(());
                        }
                        Err(e) => {
                            // ReplayTimeout means shadow stalled or the wire
                            // froze; silent skip would shed user-heap writes
                            // invisibly. Poison the stream so the daemon exits
                            // and the cursor resumes on next boot.
                            return Err(DecoderSinkError::from(e).into());
                        }
                    }
                };
                if let Some(c) = self.rel_cache.as_mut()
                    && c.enabled()
                {
                    c.insert(rfn, resolved.clone());
                }
                resolved
            };
            // Empty in steady state
            self.drain_schema_events(record.parsed.header.xact_id, record.source_lsn)
                .await?;
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
            let n_decoded = decoded_set.len();
            let buffer_span = trace_span!(sampled, "buffer", rows = n_decoded);
            async move {
                // Lock once per record (not per tuple); on_heap/on_toast_chunk
                // never touch the catalog, so no buffer→catalog inversion.
                let mut buf = self.buffer.lock().await;
                for decoded in decoded_set {
                    self.stats.record(&decoded);
                    if rel.kind == 't' {
                        let xid = decoded.xid;
                        if let Some(chunk) = toast_chunk_from_decoded(decoded, &rel) {
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

/// Drain queued [`SchemaEvent`]s; channel is unbounded + same-task
pub(crate) fn drain_pending_schema_events(rx: &SchemaEventRx) -> Vec<SchemaEvent> {
    let mut out = Vec::new();
    let mut guard = rx.lock().expect("schema event rx mutex poisoned");
    while let Ok(ev) = guard.try_recv() {
        out.push(ev);
    }
    out
}

/// Repack a TOAST table INSERT (Insert, 3 columns: `chunk_id oid`,
/// `chunk_seq int4`, `chunk_data bytea`) into a [`ToastChunk`]; `None`
/// for shapes that don't fit.
///
/// Keyed on the toast rel's pg_class OID ([`RelDescriptor::oid`]), not
/// `rel_node`: the referring tuple's `va_toastrelid` is the OID. They
/// diverge after `VACUUM FULL` / `CLUSTER` on the toast rel.
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
        // Text-typed toast chunk: re-encode to bytes (not a normal flow)
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
    //! Catalog-free paths only. Commit-drain + detoast +
    //! `XactRecordSink::commit` live in `tests/xact_buffer.rs` against a
    //! real shadow PG: they need `ShadowCatalog::relation_at`, and a
    //! unit-test stub catalog would duplicate the production cache.

    use super::*;
    use crate::heap_decoder::{DecodedTuple, HeapOp};
    use tempfile::tempdir;
    use walrus::pg::walparser::RelFileNode;

    #[test]
    fn xact_buffer_config_new_uses_default_max() {
        let c = XactBufferConfig::new(PathBuf::from("/tmp/walshadow-test-spill"));
        assert_eq!(c.xact_buffer_max, DEFAULT_XACT_BUFFER_MAX);
    }

    #[test]
    fn txn_span_registry_full_lifecycle() {
        crate::trace::set_sample_ratio(1.0);
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
        crate::trace::set_sample_ratio(1.0);
        let tmp = tempdir().unwrap();
        let buf = XactBuffer::new(XactBufferConfig::new(tmp.path().to_path_buf())).unwrap();
        let reg = buf.span_registry();
        reg.open(7, 1);
        assert!(reg.is_sampled(7));
    }

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
    async fn advance_idle_caps_ack_at_ceiling() {
        let tmp = tempdir().unwrap();
        let mut b = XactBuffer::new(cfg(tmp.path().to_path_buf())).unwrap();
        // Hold-open: ceiling below dispatched lsn caps emitter_ack
        b.advance_idle(100, 50);
        assert_eq!(b.stats().drain_lsn, 100);
        assert_eq!(b.stats().emitter_ack_lsn, 50);
        b.advance_idle(200, 200);
        assert_eq!(b.stats().drain_lsn, 200);
        assert_eq!(b.stats().emitter_ack_lsn, 200);
        // Regressing inputs never lower either field
        b.advance_idle(150, 100);
        assert_eq!(b.stats().drain_lsn, 200);
        assert_eq!(b.stats().emitter_ack_lsn, 200);
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

    /// Aborts must advance the LSNs, else an all-abort workload never
    /// advances the slot
    #[tokio::test(flavor = "current_thread")]
    async fn abort_advances_ack_lsns_for_resume_cursor() {
        let tmp = tempdir().unwrap();
        let mut b = XactBuffer::new(cfg(tmp.path().to_path_buf())).unwrap();
        b.on_heap(heap_with_value(7, 100, 16)).await.unwrap();
        b.abort(7, 0x4000, &[]).await.unwrap();
        assert_eq!(b.stats().drain_lsn, 0x4000);
        assert_eq!(b.stats().emitter_ack_lsn, 0x4000);
        // Lower-LSN abort must not regress the monotonic marks
        b.abort(99, 0x100, &[]).await.unwrap();
        assert_eq!(b.stats().drain_lsn, 0x4000);
        assert_eq!(b.stats().emitter_ack_lsn, 0x4000);
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
        let mut d2 = d.clone();
        d2.op = HeapOp::Update;
        assert!(toast_chunk_from_decoded(d2, &rel).is_none());
        let mut d3 = d.clone();
        d3.new.as_mut().unwrap().columns.pop();
        assert!(toast_chunk_from_decoded(d3, &rel).is_none());
    }

    #[test]
    fn detoasted_value_routes_tier3_like_inline() {
        use crate::heap_decoder::{BYTEAOID, JSONBOID, TEXTOID};
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
        let p = parse_xact_payload(0x00, &body);
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
        let p = parse_xact_payload(XLOG_XACT_HAS_INFO, &body);
        assert_eq!(p.xact_time, 42);
        assert_eq!(p.subxacts, vec![0xAA, 0xBB, 0xCC]);
    }

    #[test]
    fn parse_xact_payload_handles_no_has_info() {
        // HAS_INFO unset: parser must not consume bytes past the timestamp
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
        assert_eq!(p.twophase_xid, None);
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
        let p = parse_xact_payload(XLOG_XACT_HAS_INFO, &body);
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
}
