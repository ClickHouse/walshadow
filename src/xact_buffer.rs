//! Phase 6 — per-xid xact buffer + TOAST reassembly.
//!
//! Sits between [`DecoderSink`](crate::decoder_sink::DecoderSink)'s
//! per-record output and the downstream emitter. Holds every
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
//! which walshadow does not implement (see [PHASE6disk.md "What this
//! defers — Streaming mid-xact"]).
//!
//! ## Catalog access avoided at drain
//!
//! Detoasting needs the original column's type OID to decide
//! `Bytea` vs `Text`. Caching one `Arc<RelDescriptor>` per
//! `(xid, rfn)` keeps the type info inside the buffer, so commit
//! drain never blocks on [`ShadowCatalog`]. The descriptor cost is
//! one [`Arc`] bump per first-touch in the xact; the Arc is dropped
//! when the xact commits / aborts.
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
//! Spill-to-ClickHouse is reserved as design space ([PHASE6disk.md]
//! Option B) — config knob, schema, and code path are left for a
//! follow-up phase when a diskless walshadow operator asks. v1 is
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
use std::path::PathBuf;
use std::sync::Arc;

use thiserror::Error;
use tokio::sync::Mutex;
use wal_rs::pg::walparser::{RelFileNode, RmId};

use crate::decoder_sink::{DecoderSinkError, DecoderStats, TupleObserver};
use crate::filter::Decision;
use crate::heap_decoder::{ColumnValue, DecodedHeap, HeapOp, ToastPointer, decode_heap_record};
use crate::shadow_catalog::{CatalogError, RelDescriptor, ShadowCatalog};
use crate::spill::{SpillEntry, SpillError, SpillStore, SpillWriter, ToastChunk};
use crate::wal_stream::{Record, RecordSink, SinkError};

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
    #[error("missing relation descriptor for rfn={0:?} at drain — buffer mis-keyed")]
    MissingDescriptor(RelFileNode),
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

/// One drained tuple, fully reassembled. Phase 7's CH emitter
/// consumes this as `(rfn, xid, source_lsn, commit_ts, op, new, old)`.
/// `commit_ts` carries the commit-record timestamp; today only the
/// `XactRecordSink` drain path populates it.
#[derive(Debug, Clone)]
pub struct CommittedTuple {
    pub decoded: DecodedHeap,
    /// PG `TimestampTz` from the xact commit record (microseconds
    /// since PG epoch 2000-01-01). 0 when the upstream commit record
    /// lacked the field (eg. fixture-only synthetic xacts).
    pub commit_ts: i64,
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
    /// Per-rfn descriptor cache for this xact. Populated lazily by
    /// [`XactBuffer::on_heap`] — detoast at commit-drain reads from
    /// here instead of round-tripping through [`ShadowCatalog`].
    rel_cache: HashMap<RelFileNode, Arc<RelDescriptor>>,
}

impl XactState {
    fn new(first_lsn: u64) -> Self {
        Self {
            first_lsn,
            in_mem: Vec::new(),
            in_mem_bytes: 0,
            spill: None,
            spill_bytes: 0,
            rel_cache: HashMap::new(),
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

    /// Buffer a decoded heap tuple. `rel` is the descriptor the
    /// decoder already fetched via
    /// [`ShadowCatalog::relation_at`](crate::shadow_catalog::ShadowCatalog::relation_at) —
    /// reused at drain to detoast `ExternalToast` columns into
    /// `Bytea` / `Text` per `att.type_oid`.
    pub async fn on_heap(
        &mut self,
        decoded: DecodedHeap,
        rel: Arc<RelDescriptor>,
    ) -> std::result::Result<(), XactBufferError> {
        let xid = decoded.xid;
        let first_lsn = decoded.source_lsn;
        let rfn = decoded.rfn;
        let entry = SpillEntry::Heap(Box::new(decoded));
        self.absorb(xid, first_lsn, entry, Some((rfn, rel))).await
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
        self.absorb(xid, first_lsn, entry, None).await
    }

    async fn absorb(
        &mut self,
        xid: u32,
        first_lsn: u64,
        entry: SpillEntry,
        rel_hint: Option<(RelFileNode, Arc<RelDescriptor>)>,
    ) -> std::result::Result<(), XactBufferError> {
        let sz = approximate_size(&entry);
        let is_new = !self.inflight.contains_key(&xid);
        let st = self
            .inflight
            .entry(xid)
            .or_insert_with(|| XactState::new(first_lsn));
        if let Some((rfn, rel)) = rel_hint {
            st.rel_cache.entry(rfn).or_insert(rel);
        }
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
    /// value. No-op if `xid` is unknown (read-only xact, or one whose
    /// records the filter dropped before reaching the buffer).
    pub async fn commit<O: TupleObserver>(
        &mut self,
        xid: u32,
        commit_ts: i64,
        observer: &mut O,
    ) -> std::result::Result<(), XactBufferError> {
        let Some(mut st) = self.inflight.remove(&xid) else {
            self.stats.commits_unknown_xid += 1;
            return Ok(());
        };
        self.stats.xacts_active = self.stats.xacts_active.saturating_sub(1);
        self.bytes_in_memory = self.bytes_in_memory.saturating_sub(st.in_mem_bytes);
        self.stats.bytes_in_memory = self.bytes_in_memory as u64;

        // Collect heaps + chunks. Spilled half drained first to keep
        // WAL-order intact (eviction always flushes from the front of
        // in_mem, so spilled is older than anything still in memory).
        let mut heaps: Vec<DecodedHeap> = Vec::with_capacity(st.in_mem.len());
        let mut chunks: HashMap<(u32, u32), BTreeMap<u32, Vec<u8>>> = HashMap::new();

        let in_mem = std::mem::take(&mut st.in_mem);
        if let Some(writer) = st.spill.take() {
            let bc = writer.byte_count();
            self.stats.spill_bytes_active = self.stats.spill_bytes_active.saturating_sub(bc);
            self.stats.spill_xacts_active = self.stats.spill_xacts_active.saturating_sub(1);
            let mut reader = writer.finish().await?;
            while let Some(entry) = reader.next().await? {
                accumulate(entry, &mut heaps, &mut chunks);
            }
            reader.unlink().await?;
        }
        for entry in in_mem {
            accumulate(entry, &mut heaps, &mut chunks);
        }

        for mut heap in heaps {
            detoast_heap(&mut heap, &chunks, &st.rel_cache)?;
            let committed = CommittedTuple {
                decoded: heap,
                commit_ts,
            };
            observer
                .on_tuple(&committed.decoded)
                .await
                .map_err(|e| XactBufferError::Observer(e.to_string()))?;
        }
        self.stats.committed_xacts_total += 1;
        Ok(())
    }

    /// Discard xact `xid`. No-op if unknown. Wipes any spill file.
    pub async fn abort(&mut self, xid: u32) -> std::result::Result<(), XactBufferError> {
        let Some(mut st) = self.inflight.remove(&xid) else {
            self.stats.aborts_unknown_xid += 1;
            return Ok(());
        };
        self.stats.xacts_active = self.stats.xacts_active.saturating_sub(1);
        self.bytes_in_memory = self.bytes_in_memory.saturating_sub(st.in_mem_bytes);
        self.stats.bytes_in_memory = self.bytes_in_memory as u64;
        if let Some(writer) = st.spill.take() {
            let bc = writer.byte_count();
            self.stats.spill_bytes_active = self.stats.spill_bytes_active.saturating_sub(bc);
            self.stats.spill_xacts_active = self.stats.spill_xacts_active.saturating_sub(1);
            writer.unlink().await?;
        }
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

fn detoast_heap(
    heap: &mut DecodedHeap,
    chunks: &HashMap<(u32, u32), BTreeMap<u32, Vec<u8>>>,
    rel_cache: &HashMap<RelFileNode, Arc<RelDescriptor>>,
) -> std::result::Result<(), XactBufferError> {
    let needs = tuple_needs_detoast(heap.new.as_ref()) || tuple_needs_detoast(heap.old.as_ref());
    if !needs {
        return Ok(());
    }
    let rel = rel_cache
        .get(&heap.rfn)
        .ok_or(XactBufferError::MissingDescriptor(heap.rfn))?;
    if let Some(t) = heap.new.as_mut() {
        detoast_tuple(t, rel, chunks)?;
    }
    if let Some(t) = heap.old.as_mut() {
        detoast_tuple(t, rel, chunks)?;
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
        let p_clone = p.clone();
        let raw = reassemble(&p_clone, chunks)?;
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
    let mut concat: Vec<u8> = Vec::new();
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
/// buffer's commit/abort path. Separate from
/// [`DecoderSink`](crate::decoder_sink::DecoderSink) because xact
/// records are `Decision::Keep` (shadow PG needs them for CLOG) so
/// the decoder sink skips them by contract.
pub struct XactRecordSink<O: TupleObserver + Send> {
    buffer: Arc<Mutex<XactBuffer>>,
    /// Where committed tuples land. `XactBuffer::commit` calls
    /// `observer.on_tuple` per drained tuple; production wires this
    /// to the same `MetricsTupleObserver` Phase 5 uses and Phase 7
    /// will swap for the CH emitter observer.
    observer: O,
}

impl<O: TupleObserver + Send> XactRecordSink<O> {
    pub fn new(buffer: Arc<Mutex<XactBuffer>>, observer: O) -> Self {
        Self { buffer, observer }
    }

    pub fn observer_mut(&mut self) -> &mut O {
        &mut self.observer
    }
}

impl<O: TupleObserver + Send> RecordSink for XactRecordSink<O> {
    fn on_record<'a>(
        &'a mut self,
        record: &'a Record,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = std::result::Result<(), SinkError>> + Send + 'a>>
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
                    let commit_ts = parse_xact_time(&record.parsed.main_data);
                    let mut buf = self.buffer.lock().await;
                    buf.commit(xid, commit_ts, &mut self.observer)
                        .await
                        .map_err(SinkError::from)?;
                }
                XLOG_XACT_ABORT | XLOG_XACT_ABORT_PREPARED => {
                    let mut buf = self.buffer.lock().await;
                    buf.abort(xid).await.map_err(SinkError::from)?;
                }
                _ => {
                    // XLOG_XACT_PREPARE / ASSIGNMENT / INVALIDATIONS:
                    // not Phase 6 territory. PREPARE without COMMIT
                    // PREPARED would leave the xact stuck — defer 2PC
                    // proper handling to a follow-up, today the xact
                    // stays buffered until COMMIT_PREPARED lands.
                }
            }
            Ok(())
        })
    }
}

/// `RecordSink` that decodes user-heap records and routes them into
/// the xact buffer keyed by `xid`. Toast-relation INSERTs
/// (`rel.kind == 't'`) are reinterpreted as
/// [`ToastChunk`](crate::spill::ToastChunk)s and parked under their
/// `(toast_relid, value_id)` slot for drain-time reassembly. Mirrors
/// [`DecoderSink`](crate::decoder_sink::DecoderSink)'s contract: only
/// `Decision::Drop` records reach this sink (catalog-keep stays on
/// the shadow-replay path); semantic errors absorb into
/// [`DecoderStats`] rather than poisoning the stream.
pub struct BufferingDecoderSink {
    catalog: Arc<Mutex<ShadowCatalog>>,
    buffer: Arc<Mutex<XactBuffer>>,
    pub stats: DecoderStats,
    /// Counts of TOAST chunks routed to the buffer. Distinct from
    /// `stats.inserts`, which only counts non-toast user-table writes.
    pub toast_chunks_buffered: u64,
    /// Toast inserts the decoder couldn't reinterpret as a chunk
    /// (missing chunk_id/seq/data columns, type mismatch). Surfaces
    /// so a corrupt catalog or a future TOAST layout shows up as a
    /// counter, not silent loss.
    pub toast_chunks_malformed: u64,
}

impl BufferingDecoderSink {
    pub fn new(catalog: Arc<Mutex<ShadowCatalog>>, buffer: Arc<Mutex<XactBuffer>>) -> Self {
        Self {
            catalog,
            buffer,
            stats: DecoderStats::default(),
            toast_chunks_buffered: 0,
            toast_chunks_malformed: 0,
        }
    }

    pub fn stats(&self) -> &DecoderStats {
        &self.stats
    }
}

impl RecordSink for BufferingDecoderSink {
    fn on_record<'a>(
        &'a mut self,
        record: &'a Record,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = std::result::Result<(), SinkError>> + Send + 'a>>
    {
        Box::pin(async move {
            if record.decision != Decision::Drop {
                return Ok(());
            }
            let rm = record.parsed.header.resource_manager_id;
            if rm != RmId::Heap as u8 && rm != RmId::Heap2 as u8 {
                return Ok(());
            }
            let rfn = match record.parsed.blocks.first() {
                Some(b) => b.header.location.rel,
                None => {
                    self.stats.skipped_no_block += 1;
                    return Ok(());
                }
            };
            let rel = {
                let mut cat = self.catalog.lock().await;
                match cat.relation_at(rfn, record.source_lsn).await {
                    Ok(r) => r,
                    Err(CatalogError::NotFoundByFilenode(_)) => {
                        self.stats.catalog_not_found += 1;
                        return Ok(());
                    }
                    Err(CatalogError::ReplayTimeout { .. }) => {
                        self.stats.replay_timeout += 1;
                        return Ok(());
                    }
                    Err(e) => return Err(DecoderSinkError::from(e).into()),
                }
            };
            let decoded =
                match decode_heap_record(&record.parsed, record.source_lsn, &rel) {
                    Ok(Some(d)) => d,
                    Ok(None) => {
                        self.stats.skipped_op += 1;
                        return Ok(());
                    }
                    Err(e) => return Err(DecoderSinkError::from(e).into()),
                };
            self.stats.decoded += 1;
            match decoded.op {
                HeapOp::Insert => self.stats.inserts += 1,
                HeapOp::Update => self.stats.updates += 1,
                HeapOp::HotUpdate => self.stats.hot_updates += 1,
                HeapOp::Delete => self.stats.deletes += 1,
            }
            if decoded.new.as_ref().is_some_and(|t| t.partial)
                || decoded.old.as_ref().is_some_and(|t| t.partial)
            {
                self.stats.partial += 1;
            }
            if rel.kind == 't' {
                if let Some(chunk) = toast_chunk_from_decoded(&decoded, &rel) {
                    self.toast_chunks_buffered += 1;
                    let mut buf = self.buffer.lock().await;
                    buf.on_toast_chunk(chunk, decoded.xid)
                        .await
                        .map_err(SinkError::from)?;
                } else {
                    self.toast_chunks_malformed += 1;
                }
            } else {
                let mut buf = self.buffer.lock().await;
                buf.on_heap(decoded, rel.clone())
                    .await
                    .map_err(SinkError::from)?;
            }
            Ok(())
        })
    }
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
fn toast_chunk_from_decoded(d: &DecodedHeap, rel: &RelDescriptor) -> Option<ToastChunk> {
    if d.op != HeapOp::Insert {
        return None;
    }
    let new = d.new.as_ref()?;
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
    let chunk_data = match new.columns[2].as_ref()? {
        ColumnValue::Bytea(b) => b.clone(),
        // Detoasted text-typed toast wouldn't be a normal flow but
        // tolerate by re-encoding back to bytes.
        ColumnValue::Text(s) => s.as_bytes().to_vec(),
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

/// Pull `xact_time: TimestampTz` off the leading 8 bytes of an
/// `xl_xact_commit` / `xl_xact_abort` record. Returns 0 when
/// `main_data` is shorter — surfaces as "commit_ts unknown"
/// downstream rather than failing the stream.
fn parse_xact_time(main_data: &[u8]) -> i64 {
    if main_data.len() < 8 {
        return 0;
    }
    i64::from_le_bytes(main_data[0..8].try_into().unwrap())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::filter::Decision;
    use crate::heap_decoder::{DecodedTuple, HeapOp, ToastPointer};
    use crate::shadow_catalog::{RelAttr, ReplIdent};
    use crate::wal_stream::Record;
    use tempfile::tempdir;
    use wal_rs::pg::walparser::{RelFileNode, XLogRecord, XLogRecordHeader};

    fn cfg(dir: PathBuf) -> XactBufferConfig {
        XactBufferConfig {
            xact_buffer_max: 1024,
            spill_dir: dir,
        }
    }

    fn dummy_rel(rel_oid: u32, type_oid: u32) -> Arc<RelDescriptor> {
        Arc::new(RelDescriptor {
            rfn: RelFileNode {
                spc_node: 1663,
                db_node: 5,
                rel_node: rel_oid,
            },
            oid: 16500,
            namespace_oid: 2200,
            namespace_name: "public".into(),
            name: "t".into(),
            kind: 'r',
            persistence: 'p',
            replident: ReplIdent::Default { pk_attnums: None },
            attributes: vec![RelAttr {
                attnum: 1,
                name: "body".into(),
                type_oid,
                typmod: -1,
                not_null: false,
                dropped: false,
                type_name: "text".into(),
                type_byval: false,
                type_len: -1,
                type_align: 'i',
                type_storage: 'x',
            }],
        })
    }

    fn heap(xid: u32, lsn: u64, op: HeapOp) -> DecodedHeap {
        DecodedHeap {
            rfn: RelFileNode {
                spc_node: 1663,
                db_node: 5,
                rel_node: 16385,
            },
            xid,
            source_lsn: lsn,
            op,
            new: Some(DecodedTuple {
                columns: vec![Some(ColumnValue::Int4(1))],
                partial: false,
            }),
            old: None,
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

    #[derive(Default)]
    struct CollectObs {
        seen: Vec<DecodedHeap>,
    }

    impl TupleObserver for CollectObs {
        fn on_tuple<'a>(
            &'a mut self,
            d: &'a DecodedHeap,
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = std::result::Result<(), DecoderSinkError>> + Send + 'a>>
        {
            Box::pin(async move {
                self.seen.push(d.clone());
                Ok(())
            })
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn commit_drains_in_arrival_order_and_clears_state() {
        let tmp = tempdir().unwrap();
        let mut b = XactBuffer::new(cfg(tmp.path().to_path_buf())).unwrap();
        let rel = dummy_rel(16385, crate::heap_decoder::INT4OID);
        let mut obs = CollectObs::default();
        b.on_heap(heap(7, 100, HeapOp::Insert), rel.clone()).await.unwrap();
        b.on_heap(heap(7, 200, HeapOp::Update), rel.clone()).await.unwrap();
        b.on_heap(heap(8, 110, HeapOp::Insert), rel.clone()).await.unwrap();
        assert_eq!(b.active_xids(), vec![7, 8]);
        b.commit(7, 12345, &mut obs).await.unwrap();
        assert_eq!(obs.seen.len(), 2);
        assert_eq!(obs.seen[0].source_lsn, 100);
        assert_eq!(obs.seen[1].source_lsn, 200);
        assert_eq!(b.active_xids(), vec![8]);
        assert_eq!(b.stats().committed_xacts_total, 1);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn abort_drops_xact_and_unlinks_spill() {
        let tmp = tempdir().unwrap();
        let mut b = XactBuffer::new(cfg(tmp.path().to_path_buf())).unwrap();
        let rel = dummy_rel(16385, crate::heap_decoder::BYTEAOID);
        for i in 0..10 {
            b.on_heap(heap_with_value(11, 100 + i, 256), rel.clone()).await.unwrap();
        }
        assert!(b.stats().spill_xacts_active >= 1, "spill must engage");
        let spill_dir = tmp.path().to_path_buf();
        let before: Vec<_> = std::fs::read_dir(&spill_dir).unwrap().collect();
        assert!(!before.is_empty(), "spill file present");
        b.abort(11).await.unwrap();
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
    async fn commit_unknown_xid_no_ops_and_counts() {
        let tmp = tempdir().unwrap();
        let mut b = XactBuffer::new(cfg(tmp.path().to_path_buf())).unwrap();
        let mut obs = CollectObs::default();
        b.commit(99, 0, &mut obs).await.unwrap();
        assert_eq!(b.stats().commits_unknown_xid, 1);
        b.abort(101).await.unwrap();
        assert_eq!(b.stats().aborts_unknown_xid, 1);
        assert!(obs.seen.is_empty());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn spill_eviction_picks_largest_xact() {
        let tmp = tempdir().unwrap();
        let cfg = XactBufferConfig {
            xact_buffer_max: 4096,
            spill_dir: tmp.path().to_path_buf(),
        };
        let mut b = XactBuffer::new(cfg).unwrap();
        let rel = dummy_rel(16385, crate::heap_decoder::BYTEAOID);
        // Two xacts: xid=1 with one fat tuple, xid=2 with three small.
        b.on_heap(heap_with_value(1, 100, 8192), rel.clone()).await.unwrap();
        for i in 0..3 {
            b.on_heap(heap_with_value(2, 200 + i, 128), rel.clone()).await.unwrap();
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
        b.abort(1).await.unwrap();
        b.abort(2).await.unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn commit_drains_spilled_then_in_memory_entries() {
        let tmp = tempdir().unwrap();
        let cfg = XactBufferConfig {
            xact_buffer_max: 1024,
            spill_dir: tmp.path().to_path_buf(),
        };
        let mut b = XactBuffer::new(cfg).unwrap();
        let rel = dummy_rel(16385, crate::heap_decoder::BYTEAOID);
        let mut obs = CollectObs::default();
        // Three big tuples first → spill engages on the second one.
        for i in 0..3 {
            b.on_heap(heap_with_value(5, 100 + i, 700), rel.clone()).await.unwrap();
        }
        // Then small ones that stay in-memory.
        for i in 0..2 {
            b.on_heap(heap(5, 200 + i, HeapOp::Update), rel.clone()).await.unwrap();
        }
        b.commit(5, 0, &mut obs).await.unwrap();
        assert_eq!(obs.seen.len(), 5);
        for (i, h) in obs.seen.iter().enumerate() {
            if i < 3 {
                assert!(
                    h.source_lsn < 200,
                    "entry {i} expected spilled (lsn<200), got {}",
                    h.source_lsn
                );
            } else {
                assert!(
                    h.source_lsn >= 200,
                    "entry {i} expected in-memory (lsn≥200), got {}",
                    h.source_lsn
                );
            }
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn detoast_concatenates_uncompressed_chunks_into_text() {
        let tmp = tempdir().unwrap();
        let mut b = XactBuffer::new(cfg(tmp.path().to_path_buf())).unwrap();
        let rel = dummy_rel(16385, crate::heap_decoder::TEXTOID);
        let mut obs = CollectObs::default();
        let mut h = heap(33, 100, HeapOp::Insert);
        let ext_size: u32 = 4 + 3 + 3; // "Hell" + "o, " + "wor"
        h.new.as_mut().unwrap().columns[0] = Some(ColumnValue::ExternalToast(ToastPointer {
            va_rawsize: ext_size as i32 + 4,
            va_extinfo: ext_size, // no compression bits
            va_valueid: 55,
            va_toastrelid: 16400,
        }));
        b.on_heap(h, rel.clone()).await.unwrap();
        for (seq, body) in [(0u32, &b"Hell"[..]), (1, b"o, "), (2, b"wor")] {
            b.on_toast_chunk(
                ToastChunk {
                    toast_relid: 16400,
                    value_id: 55,
                    chunk_seq: seq,
                    source_lsn: 102 + seq as u64,
                    chunk_data: body.to_vec(),
                },
                33,
            )
            .await
            .unwrap();
        }
        b.commit(33, 12345, &mut obs).await.unwrap();
        assert_eq!(obs.seen.len(), 1);
        let col = &obs.seen[0].new.as_ref().unwrap().columns[0];
        match col {
            Some(ColumnValue::Text(s)) => assert_eq!(s, "Hello, wor"),
            other => panic!("expected Text after detoast, got {other:?}"),
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn detoast_missing_chunk_seq_errors_clearly() {
        let tmp = tempdir().unwrap();
        let mut b = XactBuffer::new(cfg(tmp.path().to_path_buf())).unwrap();
        let rel = dummy_rel(16385, crate::heap_decoder::TEXTOID);
        let mut obs = CollectObs::default();
        let mut h = heap(42, 100, HeapOp::Insert);
        h.new.as_mut().unwrap().columns[0] = Some(ColumnValue::ExternalToast(ToastPointer {
            va_rawsize: 8,
            va_extinfo: 6,
            va_valueid: 1,
            va_toastrelid: 16400,
        }));
        b.on_heap(h, rel).await.unwrap();
        // Only chunk 0 + chunk 2 — seq 1 missing.
        b.on_toast_chunk(
            ToastChunk {
                toast_relid: 16400,
                value_id: 1,
                chunk_seq: 0,
                source_lsn: 101,
                chunk_data: b"AAA".to_vec(),
            },
            42,
        )
        .await
        .unwrap();
        b.on_toast_chunk(
            ToastChunk {
                toast_relid: 16400,
                value_id: 1,
                chunk_seq: 2,
                source_lsn: 103,
                chunk_data: b"CCC".to_vec(),
            },
            42,
        )
        .await
        .unwrap();
        let err = b.commit(42, 0, &mut obs).await.expect_err("missing chunk surfaces");
        match err {
            XactBufferError::MissingToastChunk {
                value_id, missing, ..
            } => {
                assert_eq!(value_id, 1);
                assert_eq!(missing, 1);
            }
            other => panic!("expected MissingToastChunk, got {other:?}"),
        }
    }

    fn xact_record(info_op: u8, xid: u32, xact_time: i64) -> Record {
        let mut main_data = Vec::with_capacity(8);
        main_data.extend_from_slice(&xact_time.to_le_bytes());
        Record {
            parsed: XLogRecord {
                header: XLogRecordHeader {
                    resource_manager_id: RmId::Xact as u8,
                    info: info_op,
                    xact_id: xid,
                    ..Default::default()
                },
                main_data,
                ..Default::default()
            },
            source_lsn: 0,
            page_magic: 0xD110,
            decision: Decision::Keep,
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn xact_record_sink_routes_commit_and_abort() {
        let tmp = tempdir().unwrap();
        let buf = Arc::new(Mutex::new(XactBuffer::new(cfg(tmp.path().to_path_buf())).unwrap()));
        let rel = dummy_rel(16385, crate::heap_decoder::INT4OID);
        {
            let mut b = buf.lock().await;
            b.on_heap(heap(7, 100, HeapOp::Insert), rel.clone()).await.unwrap();
            b.on_heap(heap(8, 110, HeapOp::Insert), rel.clone()).await.unwrap();
            b.on_heap(heap(9, 120, HeapOp::Insert), rel.clone()).await.unwrap();
        }
        let mut sink = XactRecordSink::new(buf.clone(), CollectObs::default());
        let commit = xact_record(XLOG_XACT_COMMIT, 7, 0x1234);
        sink.on_record(&commit).await.unwrap();
        let abort = xact_record(XLOG_XACT_ABORT, 8, 0);
        sink.on_record(&abort).await.unwrap();
        // PREPARE — buffer must keep xid=9 untouched.
        let prepare = xact_record(0x10, 9, 0);
        sink.on_record(&prepare).await.unwrap();
        assert_eq!(sink.observer.seen.len(), 1);
        assert_eq!(sink.observer.seen[0].xid, 7);
        let b = buf.lock().await;
        assert_eq!(b.active_xids(), vec![9], "abort drops 8, prepare keeps 9");
        assert_eq!(b.stats().committed_xacts_total, 1);
        assert_eq!(b.stats().aborted_xacts_total, 1);
    }

    #[test]
    fn parse_xact_time_short_main_data_returns_zero() {
        assert_eq!(parse_xact_time(&[]), 0);
        assert_eq!(parse_xact_time(&[1, 2, 3, 4]), 0);
        let ts = 0x0123_4567_89AB_CDEFi64;
        assert_eq!(parse_xact_time(&ts.to_le_bytes()), ts);
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
}
