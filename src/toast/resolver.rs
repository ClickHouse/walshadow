//! Durable backstop for TOAST chunks outside current transaction buffer
//!
//! ClickHouse mirrors line-pointer occupancy by heap TID, versioned by WAL
//! record LSN. `ReplacingMergeTree` reclaims tombstoned chunk bodies

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use async_trait::async_trait;
use clickhouse_c::{Allocator, Block, BlockBuilder, ColumnBuilder, Event, TypeAst};
use futures::stream::StreamExt;
use thiserror::Error;
use tokio::sync::Mutex;

use bytes::Bytes;

use crate::ch::{
    ChConn, EmitterError, drain_to_end_of_stream, is_retryable, quote_ident, with_timeout,
};
use crate::decode::heap_decoder::{
    ColumnValue, ToastPointer, VARLENA_EXTSIZE_BITS, VARLENA_EXTSIZE_MASK, decompress_varlena,
};
use crate::emit::ch_emitter::{EmitterConfig, EmitterStats, InlineValueOverflow, ToastMode};
use crate::xact::spill::{BodyRef, BodySpoolFile, ToastChunk, ToastDelete};
use ahash::{HashMap, HashMapExt, HashSet, HashSetExt};

/// `(toast_relid, value_id) -> chunk_seq -> bytes`; bodies shared with
/// mirror rows via `Bytes`
pub type ChunkMap = HashMap<(u32, u32), BTreeMap<u32, Bytes>>;

/// Row seal for one store put slice. One `INSERT` is one CH part and a part
/// commit costs the same whatever it holds, so small parts waste it
pub const CHUNK_PUT_BATCH: usize = 65_536;
/// Byte seal for one store put slice; typical chunks trip the row seal
/// first, this bounds atypically fat bodies
pub const CHUNK_PUT_BYTES: usize = 64 << 20;
/// Fetch result rows per block: bounds one block's buffer to ~2 MiB at
/// `TOAST_MAX_CHUNK_SIZE`, ordering validated across block boundaries
const FETCH_BLOCK_ROWS: usize = 1024;
const FETCH_QUERY_IDS: usize = 1024;

const CHUNK_ID_INDEX_FP: f64 = 0.000001;
/// PG `VARHDRSZ`, 4-byte varlena header
const VARHDRSZ: i32 = 4;

/// `toast_save_datum` writes a compressed datum out from `VARDATA` (`ptr + 4`),
/// so the chunks begin with the inline varlena's `va_tcinfo` word, which is not
/// part of the compressed payload
const TCINFO_LEN: usize = 4;

#[derive(Debug, Error)]
pub enum ChunkStoreError {
    #[error("toast store clickhouse: {0}")]
    Clickhouse(String),
    /// Mirror absence does not prove supersession, never fill
    #[error("toast store: no mirror for toast relid {0}")]
    MissingMirror(u32),
    /// Body spool read at row materialization
    #[error("toast store io: {0}")]
    Io(#[from] std::io::Error),
    /// Shadow read failed in bridge or worker
    #[error("toast store shadow: {0}")]
    Shadow(String),
    /// Backend is read-only
    #[error("toast store shadow is read-only, refused {0}")]
    ReadOnly(&'static str),
}

#[derive(Debug, Error)]
pub enum ToastValueError {
    #[error("toast decompression: {0}")]
    Detoast(String),
    #[error("toast value of {rawsize} bytes exceeds inline_value_max {max}")]
    ValueTooLarge { rawsize: usize, max: usize },
}

/// Bytes stored in toast relation
pub(crate) fn pointer_extsize(p: &ToastPointer) -> usize {
    (p.va_extinfo & VARLENA_EXTSIZE_MASK) as usize
}

/// `VARATT_EXTERNAL_IS_COMPRESSED`: stored fewer bytes than the value holds.
/// Not the method bits — pglz is method 0, so those are zero for a pglz datum
fn pointer_is_compressed(p: &ToastPointer) -> bool {
    (pointer_extsize(p) as i64) < i64::from(p.va_rawsize - VARHDRSZ)
}

/// Return larger of decoded and stored sizes
pub(crate) fn pointer_footprint(p: &ToastPointer) -> usize {
    ((p.va_rawsize - VARHDRSZ).max(0) as usize).max(pointer_extsize(p))
}

/// Check value limits and return memory needed for decoding
///
/// Ignore oversized values under [`InlineValueOverflow::Null`], caller replaces
/// them without fetching
pub(crate) fn check_value_caps(
    pointers: impl IntoIterator<Item = ToastPointer>,
    max: usize,
    overflow: InlineValueOverflow,
) -> Result<usize, ToastValueError> {
    let mut retained = 0usize;
    let mut transient = 0usize;
    for p in pointers {
        let footprint = pointer_footprint(&p);
        if footprint > max {
            if overflow == InlineValueOverflow::Null {
                continue;
            }
            return Err(ToastValueError::ValueTooLarge {
                rawsize: footprint,
                max,
            });
        }
        retained += footprint;
        if pointer_is_compressed(&p) {
            transient = transient.max(pointer_extsize(&p));
        }
    }
    Ok(retained + transient)
}

/// Convert raw TOAST bytes through inline varlena decoder
pub(crate) fn detoasted_value(raw: Vec<u8>, type_oid: u32) -> ColumnValue {
    crate::decode::heap_decoder::varlena_to_value(type_oid, std::borrow::Cow::Owned(raw))
}

/// Convert stored bytes to raw bytes using pointer compression method
pub(crate) fn finish_value(p: &ToastPointer, stored: Vec<u8>) -> Result<Vec<u8>, ToastValueError> {
    if !pointer_is_compressed(p) {
        return Ok(stored);
    }
    let method = ((p.va_extinfo >> VARLENA_EXTSIZE_BITS) & 0x3) as u8;
    let raw_len = (p.va_rawsize - VARHDRSZ).max(0) as usize;
    let payload = stored.get(TCINFO_LEN..).unwrap_or(&[]);
    match decompress_varlena(method, payload, raw_len) {
        Some(out) => Ok(out),
        None => Err(ToastValueError::Detoast(format!(
            "decompress failed (method {method}, {} bytes \u{2192} {raw_len}) for value {} in \
             pg_toast_{}",
            payload.len(),
            p.va_valueid,
            p.va_toastrelid,
        ))),
    }
}

/// Chunk birth or TID tombstone, keyed by heap TID and record LSN
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToastRow<B = Bytes> {
    pub toast_relid: u32,
    pub blkno: u32,
    pub offnum: u16,
    /// `va_valueid`, InvalidOid marks tombstones
    pub chunk_id: u32,
    pub chunk_seq: u32,
    pub chunk_data: B,
    /// Record LSN orders same-commit birth and death at one TID
    pub lsn: u64,
}

impl<B: From<Bytes>> ToastRow<B> {
    pub fn from_chunk(c: &ToastChunk) -> Self {
        Self::with_body(c, c.chunk_data.clone().into())
    }

    pub fn tombstone(d: &ToastDelete) -> Self {
        Self {
            toast_relid: d.toast_relid,
            blkno: d.blkno,
            offnum: d.offnum,
            chunk_id: 0,
            chunk_seq: 0,
            chunk_data: Bytes::new().into(),
            lsn: d.source_lsn,
        }
    }
}

impl<B> ToastRow<B> {
    pub fn with_body(c: &ToastChunk, chunk_data: B) -> Self {
        Self {
            toast_relid: c.toast_relid,
            blkno: c.blkno,
            offnum: c.offnum,
            chunk_id: c.value_id,
            chunk_seq: c.chunk_seq,
            chunk_data,
            lsn: c.source_lsn,
        }
    }

    pub fn is_tombstone(&self) -> bool {
        self.chunk_id == 0
    }
}

/// Chunk body location: memory until a drain's cumulative chunk bytes
/// cross the spool threshold, file-backed past it
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Body {
    Mem(Bytes),
    File(BodyRef),
}

impl Body {
    pub fn len(&self) -> usize {
        match self {
            Body::Mem(b) => b.len(),
            Body::File(r) => r.len as usize,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Body bytes: `Mem` is a refcount clone, `File` positional-reads
    pub fn load(&self, spool: Option<&BodySpoolFile>) -> std::io::Result<Bytes> {
        match self {
            Body::Mem(b) => Ok(b.clone()),
            Body::File(r) => spool
                .ok_or_else(|| std::io::Error::other("file body without spool"))?
                .read(*r)
                .map(Bytes::from),
        }
    }
}

impl From<Bytes> for Body {
    fn from(value: Bytes) -> Self {
        Self::Mem(value)
    }
}

/// Per-value chunk coverage: dense contiguous spool-prefix run plus
/// out-of-pattern tail. PG `toast_save_datum` writes one value's chunk
/// INSERTs consecutively from a single backend, so file-backed values
/// normally collapse to one run; a run records no chunk boundaries, so
/// deviations (and all memory bodies) land in `tail`
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValueRef {
    /// Concatenated spool bodies of seq `0..run_chunks`; `len` 0 when
    /// value is memory-resident or started out of pattern
    pub run: BodyRef,
    pub run_chunks: u32,
    /// Reassembly expects dense `run_chunks..N` here; keys below
    /// `run_chunks` are byte-identical re-inserts (PG chunk rows immutable
    /// per value id), ignored
    pub tail: BTreeMap<u32, Body>,
}

impl ValueRef {
    pub fn new(seq: u32, body: Body) -> Self {
        let mut v = Self {
            run: BodyRef { offset: 0, len: 0 },
            run_chunks: 0,
            tail: BTreeMap::new(),
        };
        v.push(seq, body);
        v
    }

    /// Extend run on dense contiguous file append, else tail (last-wins)
    pub fn push(&mut self, seq: u32, body: Body) {
        if let Body::File(r) = body
            && self.tail.is_empty()
            && seq == self.run_chunks
            && (self.run_chunks == 0 || r.offset == self.run.offset + u64::from(self.run.len))
        {
            if self.run_chunks == 0 {
                self.run = r;
            } else {
                self.run.len += r.len;
            }
            self.run_chunks += 1;
            return;
        }
        self.tail.insert(seq, body);
    }
}

/// File-backed generation map: [`ChunkMap`] keying, bodies as
/// memory bytes or spool ranges
pub type ChunkRefMap = HashMap<(u32, u32), ValueRef>;

/// TOAST row with body deferred behind memory or spool reference
pub type ToastRowRef = ToastRow<Body>;

impl ToastRow<Body> {
    /// Just-in-time body load for bounded store puts
    pub fn materialize(&self, spool: Option<&BodySpoolFile>) -> std::io::Result<ToastRow> {
        Ok(ToastRow {
            toast_relid: self.toast_relid,
            blkno: self.blkno,
            offnum: self.offnum,
            chunk_id: self.chunk_id,
            chunk_seq: self.chunk_seq,
            chunk_data: self.chunk_data.load(spool)?,
            lsn: self.lsn,
        })
    }
}

/// Store-side value fetch outcome. Ordering violations are transport
/// errors, not outcomes: ascending dense feed is part of the fetch
/// contract.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FetchedValue {
    /// No live chunk visible at bound
    Missing,
    /// Chunk run deviates from pointer size (partial merge collapse or
    /// generation mixing): gapped, short, or over-long. Fills per miss
    /// policy, counted distinctly
    Mismatch { got: usize },
    /// Exactly `expected_size` bytes, seq-dense from 0
    Assembled(Vec<u8>),
    /// Chunks postdate referring record, indicating value-ID reuse
    /// Applies to complete and partial replacements, shadow store only
    Generation,
}

/// Ordered chunk assembler: seqs must arrive ascending, bytes append into
/// one exact-capacity buffer. Seq regression (disorder / duplicate) is a
/// contract violation; gap, short run, and overrun resolve to
/// [`FetchedValue::Mismatch`] at finish.
pub struct ChunkAssembler {
    expected: usize,
    next_seq: u32,
    gapped: bool,
    got: usize,
    buf: Vec<u8>,
}

impl ChunkAssembler {
    pub fn new(expected: usize) -> Self {
        Self {
            expected,
            next_seq: 0,
            gapped: false,
            got: 0,
            buf: Vec::new(),
        }
    }

    /// `Err` on non-ascending seq: fetch order contract broken
    pub fn push(&mut self, seq: u32, body: &[u8]) -> Result<(), String> {
        if seq < self.next_seq {
            return Err(format!(
                "chunk_seq {seq} after {}: fetch order contract broken",
                self.next_seq.wrapping_sub(1)
            ));
        }
        if seq > self.next_seq {
            self.gapped = true;
        }
        self.next_seq = seq + 1;
        self.got += body.len();
        if !self.gapped && self.got <= self.expected {
            if self.buf.capacity() == 0 {
                self.buf.reserve_exact(self.expected);
            }
            self.buf.extend_from_slice(body);
        }
        Ok(())
    }

    pub fn finish(self) -> FetchedValue {
        if self.next_seq == 0 {
            FetchedValue::Missing
        } else if self.gapped || self.got != self.expected {
            FetchedValue::Mismatch { got: self.got }
        } else {
            FetchedValue::Assembled(self.buf)
        }
    }
}

/// Durable TID-keyed chunk store
#[async_trait]
pub trait ChunkStore: Send + Sync {
    /// Whether backend accepts decoded chunk rows
    fn accepts_writes(&self) -> bool {
        true
    }
    /// Replay emits byte-identical rows at equal key and version
    async fn put(&self, rows: &[ToastRow]) -> Result<(), ChunkStoreError>;
    /// Assemble newest live row per sequence at `max_lsn` against each
    /// pointer's stored size (`va_extsize`) over one mirror's
    /// `(value_id, expected_size)` batch, results aligned with `values`.
    /// Ids must be unique: a store resolves each one once
    ///
    /// [`FetchedValue::Missing`] when no live row remains at bound,
    /// [`ChunkStoreError::MissingMirror`] when mirror is absent
    async fn fetch_many(
        &self,
        toast_relid: u32,
        values: &[(u32, usize)],
        max_lsn: u64,
    ) -> Result<Vec<FetchedValue>, ChunkStoreError>;
    /// [`Self::fetch_many`] for one value
    async fn fetch(
        &self,
        toast_relid: u32,
        value_id: u32,
        max_lsn: u64,
        expected_size: usize,
    ) -> Result<FetchedValue, ChunkStoreError> {
        let mut got = self
            .fetch_many(toast_relid, &[(value_id, expected_size)], max_lsn)
            .await?;
        Ok(got.pop().unwrap_or(FetchedValue::Missing))
    }
    /// Empty mirror without dropping it
    ///
    /// Owner TRUNCATE orders destination wipe after replayed fills. DROP callers
    /// wait until persisted replay floor passes dropping commit
    async fn truncate_mirror(&self, toast_relid: u32) -> Result<(), ChunkStoreError>;
    /// Rewrite-generation residual deaths `O - B`: tombstone at `commit_lsn`
    /// every TID live as of `marker_lsn` (generation's `XLOG_SMGR_CREATE`)
    /// with no row past it. Caller puts the generation's births first.
    /// Missing mirror is a no-op: nothing lived
    async fn rewrite_barrier(
        &self,
        toast_relid: u32,
        marker_lsn: u64,
        commit_lsn: u64,
    ) -> Result<(), ChunkStoreError>;
}

/// In-memory implementation of ClickHouse as-of algorithm
#[derive(Default)]
pub struct MemChunkStore {
    mirrors: std::sync::Mutex<HashMap<u32, Vec<ToastRow>>>,
}

impl MemChunkStore {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl ChunkStore for MemChunkStore {
    async fn put(&self, rows: &[ToastRow]) -> Result<(), ChunkStoreError> {
        let mut mirrors = self.mirrors.lock().unwrap();
        for r in rows {
            mirrors.entry(r.toast_relid).or_default().push(r.clone());
        }
        Ok(())
    }

    /// One pass over the mirror for the whole batch, as the CH store's one
    /// query is: a per-value pass would make a batch quadratic
    async fn fetch_many(
        &self,
        toast_relid: u32,
        values: &[(u32, usize)],
        max_lsn: u64,
    ) -> Result<Vec<FetchedValue>, ChunkStoreError> {
        let mirrors = self.mirrors.lock().unwrap();
        let Some(rows) = mirrors.get(&toast_relid) else {
            return Err(ChunkStoreError::MissingMirror(toast_relid));
        };
        let wanted: HashSet<u32> = values.iter().map(|&(id, _)| id).collect();
        let mut latest: HashMap<(u32, u16), &ToastRow> = HashMap::new();
        for r in rows.iter().filter(|r| r.lsn <= max_lsn) {
            latest
                .entry((r.blkno, r.offnum))
                .and_modify(|e| {
                    if r.lsn >= e.lsn {
                        *e = r;
                    }
                })
                .or_insert(r);
        }
        let mut newest: HashMap<u32, BTreeMap<u32, (u64, &[u8])>> = HashMap::new();
        for r in latest.into_values() {
            if r.is_tombstone() || !wanted.contains(&r.chunk_id) {
                continue;
            }
            newest
                .entry(r.chunk_id)
                .or_default()
                .entry(r.chunk_seq)
                .and_modify(|e| {
                    if r.lsn >= e.0 {
                        *e = (r.lsn, &r.chunk_data);
                    }
                })
                .or_insert((r.lsn, &r.chunk_data));
        }
        values
            .iter()
            .map(|&(value_id, expected_size)| {
                let mut asm = ChunkAssembler::new(expected_size);
                for (&seq, (_, body)) in newest.get(&value_id).into_iter().flatten() {
                    asm.push(seq, body).map_err(ChunkStoreError::Clickhouse)?;
                }
                Ok(asm.finish())
            })
            .collect()
    }

    async fn truncate_mirror(&self, toast_relid: u32) -> Result<(), ChunkStoreError> {
        if let Some(rows) = self.mirrors.lock().unwrap().get_mut(&toast_relid) {
            rows.clear();
        }
        Ok(())
    }

    async fn rewrite_barrier(
        &self,
        toast_relid: u32,
        marker_lsn: u64,
        commit_lsn: u64,
    ) -> Result<(), ChunkStoreError> {
        let mut mirrors = self.mirrors.lock().unwrap();
        let Some(rows) = mirrors.get_mut(&toast_relid) else {
            return Ok(());
        };
        let mut latest_below: HashMap<(u32, u16), &ToastRow> = HashMap::new();
        let mut past_marker: HashSet<(u32, u16)> = HashSet::new();
        for r in rows.iter() {
            let tid = (r.blkno, r.offnum);
            if r.lsn > marker_lsn {
                past_marker.insert(tid);
                continue;
            }
            latest_below
                .entry(tid)
                .and_modify(|e| {
                    if r.lsn >= e.lsn {
                        *e = r;
                    }
                })
                .or_insert(r);
        }
        let residual: Vec<ToastRow> = latest_below
            .into_iter()
            .filter(|(tid, r)| !r.is_tombstone() && !past_marker.contains(tid))
            .map(|((blkno, offnum), _)| {
                ToastRow::tombstone(&ToastDelete {
                    toast_relid,
                    blkno,
                    offnum,
                    source_lsn: commit_lsn,
                })
            })
            .collect();
        rows.extend(residual);
        Ok(())
    }
}

/// Missing database/table errors bypass retry as [`ChunkStoreError::MissingMirror`]
const CH_UNKNOWN_TABLE: i32 = 60;
const CH_UNKNOWN_DATABASE: i32 = 81;

struct ChState {
    client: ChConn,
    created: HashSet<u32>,
}

/// TID-keyed ClickHouse mirror, one table per TOAST relation
///
/// Fetch aggregates version history explicitly, independent of merge state
pub struct ClickHouseChunkStore {
    conn: EmitterConfig,
    alloc: Allocator,
    /// Taken round-robin: a restore is often one relid, so keying on it
    /// would serialize the whole thing
    states: Vec<Mutex<ChState>>,
    next: AtomicUsize,
}

impl ClickHouseChunkStore {
    pub fn new(conn: EmitterConfig) -> Self {
        let slots = conn
            .toast
            .connections
            .map_or_else(|| conn.inserter_pool_size.max(1), |n| n.get());
        Self {
            alloc: Allocator::global(&mimalloc::MiMalloc),
            states: (0..slots)
                .map(|_| {
                    Mutex::new(ChState {
                        client: ChConn::default(),
                        created: HashSet::new(),
                    })
                })
                .collect(),
            next: AtomicUsize::new(0),
            conn,
        }
    }

    async fn slot(&self) -> tokio::sync::MutexGuard<'_, ChState> {
        let start = self.next.fetch_add(1, Ordering::Relaxed);
        for i in 0..self.states.len() {
            let at = (start + i) % self.states.len();
            if let Ok(guard) = self.states[at].try_lock() {
                return guard;
            }
        }
        self.states[start % self.states.len()].lock().await
    }

    async fn slot_for(&self, toast_relid: u32) -> tokio::sync::MutexGuard<'_, ChState> {
        let at = toast_relid as usize % self.states.len();
        self.states[at].lock().await
    }

    fn toast_table(&self, toast_relid: u32) -> String {
        format!(
            "{}.{}",
            quote_ident(&self.conn.database),
            quote_ident(&format!("pg_toast_{toast_relid}"))
        )
    }

    fn create_sql(&self, toast_relid: u32) -> String {
        format!(
            "CREATE TABLE IF NOT EXISTS {} (\n  \
             `blkno` UInt32,\n  `offnum` UInt16,\n  `chunk_id` UInt32,\n  `chunk_seq` UInt32,\n  \
             `chunk_data` String,\n  `_lsn` UInt64,\n  `_is_deleted` UInt8,\n  \
             INDEX `idx_chunk_id` `chunk_id` \
             TYPE bloom_filter({CHUNK_ID_INDEX_FP}) GRANULARITY 1\n\
             ) ENGINE = ReplacingMergeTree(`_lsn`, `_is_deleted`)\nORDER BY (`blkno`, `offnum`)",
            self.toast_table(toast_relid)
        )
    }

    fn insert_sql(&self, toast_relid: u32) -> String {
        format!(
            "INSERT INTO {} (`blkno`, `offnum`, `chunk_id`, `chunk_seq`, `chunk_data`, \
             `_lsn`, `_is_deleted`) FORMAT Native",
            self.toast_table(toast_relid)
        )
    }

    fn truncate_sql(&self, toast_relid: u32) -> String {
        format!("TRUNCATE TABLE IF EXISTS {}", self.toast_table(toast_relid))
    }

    /// `O - B` server-side in one scan: a TID survives when its newest row
    /// is a birth at or below the marker. Any row past the marker excludes
    /// it, generation births and residuals an earlier run inserted alike, so
    /// re-runs insert nothing. `GROUP BY` matches the table's `ORDER BY`, so
    /// aggregation streams in key order instead of hashing every mirrored TID
    fn rewrite_barrier_sql(&self, toast_relid: u32, marker_lsn: u64, commit_lsn: u64) -> String {
        let table = self.toast_table(toast_relid);
        format!(
            "INSERT INTO {table} (`blkno`, `offnum`, `chunk_id`, `chunk_seq`, `chunk_data`, \
             `_lsn`, `_is_deleted`)\n\
             SELECT `blkno`, `offnum`, 0, 0, '', {commit_lsn}, 1\n\
             FROM {table}\n\
             GROUP BY `blkno`, `offnum`\n\
             HAVING max(`_lsn`) <= {marker_lsn} AND argMax(`_is_deleted`, `_lsn`) = 0\n\
             SETTINGS optimize_aggregation_in_order = 1"
        )
    }

    /// Break equal-version ties deterministically; TID order cannot identify
    /// newer generations. `chunk_id` leads the projection so one query
    /// assembles a batch of values, each seq-ordered
    fn fetch_sql(&self, toast_relid: u32, ids: &[u32], max_lsn: u64) -> String {
        let table = self.toast_table(toast_relid);
        let ids = ids
            .iter()
            .map(u32::to_string)
            .collect::<Vec<_>>()
            .join(", ");
        format!(
            "SELECT `chunk_id`, `chunk_seq`, \
             argMax(`chunk_data`, (`ver`, `blkno`, `offnum`)) AS `chunk_data`\n\
             FROM (\n  \
             SELECT `blkno`, `offnum`,\n         \
             argMax(`chunk_id`, `_lsn`) AS `chunk_id`,\n         \
             argMax(`chunk_seq`, `_lsn`) AS `chunk_seq`,\n         \
             argMax(`chunk_data`, `_lsn`) AS `chunk_data`,\n         \
             max(`_lsn`) AS `ver`,\n         \
             argMax(`_is_deleted`, `_lsn`) AS `dead`\n  \
             FROM {table}\n  \
             WHERE `_lsn` <= {max_lsn}\n    \
             AND (`blkno`, `offnum`) IN (\n      \
             SELECT `blkno`, `offnum` FROM {table}\n      \
             WHERE `chunk_id` IN ({ids}) AND `_lsn` <= {max_lsn})\n  \
             GROUP BY `blkno`, `offnum`\n\
             )\n\
             WHERE `chunk_id` IN ({ids}) AND `dead` = 0\n\
             GROUP BY `chunk_id`, `chunk_seq`\n\
             ORDER BY `chunk_id`, `chunk_seq`\n\
             SETTINGS max_block_size = {FETCH_BLOCK_ROWS}"
        )
    }

    async fn exec_write(
        &self,
        state: &mut ChState,
        sql: &str,
        bb: Option<&BlockBuilder<'_>>,
    ) -> Result<(), EmitterError> {
        state
            .client
            .retry(
                &self.conn,
                self.conn.retry.backoff(),
                |mut client| async move {
                    let result = with_timeout(self.conn.insert_timeout, async {
                        client.send_query(sql, None).await?;
                        if let Some(bb) = bb {
                            client.send_data(Some(bb)).await?;
                            client.send_data_end().await?;
                        }
                        drain_to_end_of_stream(&mut client).await
                    })
                    .await;
                    (client, result)
                },
                |_, _| {},
            )
            .await
    }

    async fn put_locked(&self, state: &mut ChState, rows: &[ToastRow]) -> Result<(), EmitterError> {
        let mut by_relid: HashMap<u32, Vec<&ToastRow>> = HashMap::new();
        for r in rows {
            by_relid.entry(r.toast_relid).or_default().push(r);
        }
        for (relid, group) in by_relid {
            if !state.created.contains(&relid) {
                let create = self.create_sql(relid);
                self.exec_write(state, &create, None).await?;
                state.created.insert(relid);
            }
            let n = group.len();
            let mut blkno = Vec::with_capacity(n * 4);
            let mut offnum = Vec::with_capacity(n * 2);
            let mut chunk_id = Vec::with_capacity(n * 4);
            let mut chunk_seq = Vec::with_capacity(n * 4);
            let mut lsn = Vec::with_capacity(n * 8);
            let mut is_deleted = Vec::with_capacity(n);
            let mut offsets = Vec::with_capacity(n);
            let data_len = group.iter().map(|r| r.chunk_data.len()).sum();
            let mut data = Vec::with_capacity(data_len);
            for r in &group {
                blkno.extend_from_slice(&r.blkno.to_le_bytes());
                offnum.extend_from_slice(&r.offnum.to_le_bytes());
                chunk_id.extend_from_slice(&r.chunk_id.to_le_bytes());
                chunk_seq.extend_from_slice(&r.chunk_seq.to_le_bytes());
                lsn.extend_from_slice(&r.lsn.to_le_bytes());
                is_deleted.push(r.is_tombstone() as u8);
                data.extend_from_slice(&r.chunk_data);
                offsets.push(data.len() as u64);
            }
            let u8_ast = TypeAst::parse("UInt8", self.alloc)?;
            let u16_ast = TypeAst::parse("UInt16", self.alloc)?;
            let u32_ast = TypeAst::parse("UInt32", self.alloc)?;
            let u64_ast = TypeAst::parse("UInt64", self.alloc)?;
            let string_ast = TypeAst::parse("String", self.alloc)?;
            let u8_w = u8_ast.view().elem_size();
            let u16_w = u16_ast.view().elem_size();
            let u32_w = u32_ast.view().elem_size();
            let u64_w = u64_ast.view().elem_size();
            let blkno_col = ColumnBuilder::fixed(&blkno, u32_w, n)?;
            let offnum_col = ColumnBuilder::fixed(&offnum, u16_w, n)?;
            let chunk_id_col = ColumnBuilder::fixed(&chunk_id, u32_w, n)?;
            let chunk_seq_col = ColumnBuilder::fixed(&chunk_seq, u32_w, n)?;
            let chunk_data_col = ColumnBuilder::string(&offsets, &data, n)?;
            let lsn_col = ColumnBuilder::fixed(&lsn, u64_w, n)?;
            let is_deleted_col = ColumnBuilder::fixed(&is_deleted, u8_w, n)?;
            let mut bb = BlockBuilder::new();
            bb.append("blkno", u32_ast.view(), &blkno_col)?;
            bb.append("offnum", u16_ast.view(), &offnum_col)?;
            bb.append("chunk_id", u32_ast.view(), &chunk_id_col)?;
            bb.append("chunk_seq", u32_ast.view(), &chunk_seq_col)?;
            bb.append("chunk_data", string_ast.view(), &chunk_data_col)?;
            bb.append("_lsn", u64_ast.view(), &lsn_col)?;
            bb.append("_is_deleted", u8_ast.view(), &is_deleted_col)?;
            let insert = self.insert_sql(relid);
            self.exec_write(state, &insert, Some(&bb)).await?;
        }
        Ok(())
    }

    async fn query_locked<A: Default>(
        &self,
        state: &mut ChState,
        sql: &str,
        parse: impl Fn(&Block, &mut A) -> Result<(), EmitterError>,
    ) -> Result<A, EmitterError> {
        state
            .client
            .retry_when(
                &self.conn,
                self.conn.retry.backoff(),
                |e| is_retryable(e) && !is_missing_mirror(e),
                |mut client| async {
                    let result = with_timeout(self.conn.insert_timeout, async {
                        client.send_query(sql, None).await?;
                        let mut out = A::default();
                        loop {
                            match client.recv_event().await? {
                                Event::Data(block) => parse(&block, &mut out)?,
                                Event::EndOfStream => break,
                                Event::Exception(exc) => {
                                    return Err(EmitterError::ServerException {
                                        code: exc.code(),
                                        message: String::from_utf8_lossy(exc.display_text())
                                            .into_owned(),
                                    });
                                }
                                _ => {}
                            }
                        }
                        Ok(out)
                    })
                    .await;
                    (client, result)
                },
                |_, _| {},
            )
            .await
    }

    /// One query, one connection: the batch's values assembled off a single
    /// `chunk_id IN (…)` scan
    async fn fetch_batch(
        &self,
        toast_relid: u32,
        ids: &[u32],
        max_lsn: u64,
        expected: &HashMap<u32, usize>,
    ) -> Result<HashMap<u32, ChunkAssembler>, ChunkStoreError> {
        let sql = self.fetch_sql(toast_relid, ids, max_lsn);
        let mut state = self.slot().await;
        self.query_locked(&mut state, &sql, |block, out| {
            read_value_block(block, expected, out)
        })
        .await
        .map_err(|e| match e {
            EmitterError::ServerException { code, .. }
                if code == CH_UNKNOWN_TABLE || code == CH_UNKNOWN_DATABASE =>
            {
                ChunkStoreError::MissingMirror(toast_relid)
            }
            e => ChunkStoreError::Clickhouse(e.to_string()),
        })
    }
}

fn is_missing_mirror(error: &EmitterError) -> bool {
    matches!(
        error,
        EmitterError::ServerException { code, .. }
            if *code == CH_UNKNOWN_TABLE || *code == CH_UNKNOWN_DATABASE
    )
}

/// Feed one result block into per-value assemblers, sized off `expected` at
/// a value's first chunk. Seq order is validated per value across block
/// boundaries (final `ORDER BY chunk_id, chunk_seq` is the contract)
fn read_value_block(
    block: &Block,
    expected: &HashMap<u32, usize>,
    out: &mut HashMap<u32, ChunkAssembler>,
) -> Result<(), EmitterError> {
    let n = block.n_rows();
    if n == 0 {
        return Ok(());
    }
    let (id_elem, id_bytes) = block
        .column(0)
        .and_then(|c| c.fixed())
        .ok_or_else(|| EmitterError::Type("toast fetch: chunk_id not fixed-width".into()))?;
    let (seq_elem, seq_bytes) = block
        .column(1)
        .and_then(|c| c.fixed())
        .ok_or_else(|| EmitterError::Type("toast fetch: chunk_seq not fixed-width".into()))?;
    if id_elem != 4 || seq_elem != 4 {
        return Err(EmitterError::Type(format!(
            "toast fetch: chunk_id/chunk_seq elem sizes {id_elem}/{seq_elem} != 4"
        )));
    }
    let (offsets, data) = block
        .column(2)
        .and_then(|c| c.string())
        .ok_or_else(|| EmitterError::Type("toast fetch: chunk_data not String".into()))?;
    for i in 0..n {
        let id = u32::from_le_bytes(id_bytes[i * 4..i * 4 + 4].try_into().unwrap());
        let seq = u32::from_le_bytes(seq_bytes[i * 4..i * 4 + 4].try_into().unwrap());
        let start = if i == 0 { 0 } else { offsets[i - 1] as usize };
        let end = offsets[i] as usize;
        out.entry(id)
            .or_insert_with(|| ChunkAssembler::new(expected.get(&id).copied().unwrap_or_default()))
            .push(seq, &data[start..end])
            .map_err(|e| EmitterError::Type(format!("toast fetch: {e}")))?;
    }
    Ok(())
}

#[async_trait]
impl ChunkStore for ClickHouseChunkStore {
    async fn put(&self, rows: &[ToastRow]) -> Result<(), ChunkStoreError> {
        if rows.is_empty() {
            return Ok(());
        }
        // One batch, one INSERT: splitting across slots multiplies CH parts,
        // and a part commit costs the same whatever it holds
        let mut state = self.slot().await;
        self.put_locked(&mut state, rows)
            .await
            .map_err(|e| ChunkStoreError::Clickhouse(e.to_string()))
    }

    async fn fetch_many(
        &self,
        toast_relid: u32,
        values: &[(u32, usize)],
        max_lsn: u64,
    ) -> Result<Vec<FetchedValue>, ChunkStoreError> {
        if values.is_empty() {
            return Ok(Vec::new());
        }
        let mut expected: HashMap<u32, usize> = HashMap::with_capacity(values.len());
        for &(id, size) in values {
            let prev = expected.insert(id, size);
            debug_assert!(
                prev.is_none_or(|prev| prev == size),
                "value {id} of pg_toast_{toast_relid} batched at two stored sizes",
            );
        }
        let mut ids: Vec<u32> = expected.keys().copied().collect();
        // Ascending: a split's ids stay adjacent in the mirror's granules
        ids.sort_unstable();
        let mut assembled: HashMap<u32, ChunkAssembler> = HashMap::with_capacity(ids.len());
        let queries = ids.len().div_ceil(FETCH_QUERY_IDS).max(1);
        let width = ids.len().div_ceil(queries);
        let concurrency = queries.min(self.states.len());
        let split: Vec<_> = ids
            .chunks(width)
            .map(|ids| self.fetch_batch(toast_relid, ids, max_lsn, &expected))
            .collect();
        let mut batches = futures::stream::iter(split).buffer_unordered(concurrency);
        while let Some(batch) = batches.next().await {
            assembled.extend(batch?);
        }
        Ok(values
            .iter()
            .map(|(id, _)| {
                assembled
                    .remove(id)
                    .map_or(FetchedValue::Missing, ChunkAssembler::finish)
            })
            .collect())
    }

    async fn truncate_mirror(&self, toast_relid: u32) -> Result<(), ChunkStoreError> {
        let sql = self.truncate_sql(toast_relid);
        let mut state = self.slot_for(toast_relid).await;
        self.exec_write(&mut state, &sql, None)
            .await
            .map_err(|e| ChunkStoreError::Clickhouse(e.to_string()))
    }

    async fn rewrite_barrier(
        &self,
        toast_relid: u32,
        marker_lsn: u64,
        commit_lsn: u64,
    ) -> Result<(), ChunkStoreError> {
        let sql = self.rewrite_barrier_sql(toast_relid, marker_lsn, commit_lsn);
        let mut state = self.slot_for(toast_relid).await;
        match self.exec_write(&mut state, &sql, None).await {
            Ok(()) => Ok(()),
            // Never-populated mirror: no table, nothing lived, nothing to
            // tombstone
            Err(EmitterError::ServerException { code, .. })
                if code == CH_UNKNOWN_TABLE || code == CH_UNKNOWN_DATABASE =>
            {
                Ok(())
            }
            Err(e) => Err(ChunkStoreError::Clickhouse(e.to_string())),
        }
    }
}

/// TOAST resolution policy and optional store
#[derive(Clone)]
pub struct ToastResolver {
    store: Option<Arc<dyn ChunkStore>>,
    stats: Arc<EmitterStats>,
    put_batch_rows: usize,
    put_batch_bytes: usize,
    /// V3 hard per-value decode-target cap, checked before allocation
    inline_value_max: usize,
    /// Action for values over `inline_value_max`
    overflow: InlineValueOverflow,
    /// Leaf permits for per-value transients (assembly, decompress, JIT
    /// materialization); `None` = unmetered (serial/metrics-only paths)
    budget: Option<crate::budget::MemoryBudget>,
}

impl ToastResolver {
    pub fn disabled() -> Self {
        Self {
            store: None,
            stats: Arc::new(EmitterStats::default()),
            put_batch_rows: CHUNK_PUT_BATCH,
            put_batch_bytes: CHUNK_PUT_BYTES,
            inline_value_max: usize::MAX,
            overflow: InlineValueOverflow::default(),
            budget: None,
        }
    }

    /// Build resolver without a bridge
    ///
    /// Disabled mode keeps no store, other modes use ClickHouse mirror
    pub fn from_config(emitter: &EmitterConfig, stats: Arc<EmitterStats>) -> Self {
        let store: Option<Arc<dyn ChunkStore>> = match emitter.toast.mode {
            ToastMode::Disabled => None,
            _ => Some(Arc::new(ClickHouseChunkStore::new(emitter.clone()))),
        };
        Self::with_limits(store, emitter, stats)
    }

    /// Build resolver for configured mode. Shadow requires `read`
    /// Bootstrap binds its bridge after starting PostgreSQL
    pub fn for_mode(
        emitter: &EmitterConfig,
        stats: Arc<EmitterStats>,
        read: Option<crate::toast::shadow_store::ShadowRead>,
    ) -> Result<Self, String> {
        if !emitter.toast.mode.is_shadow() {
            return Ok(Self::from_config(emitter, stats));
        }
        let read = read.ok_or_else(|| {
            "[toast] mode = \"shadow\" needs the pgext bridge; \
             pass --bridge-lib-dir and let walshadow manage shadow"
                .to_string()
        })?;
        Ok(Self::with_limits(
            Some(Arc::new(
                crate::toast::shadow_store::ShadowToastStore::late(read),
            )),
            emitter,
            stats,
        ))
    }

    fn with_limits(
        store: Option<Arc<dyn ChunkStore>>,
        emitter: &EmitterConfig,
        stats: Arc<EmitterStats>,
    ) -> Self {
        Self {
            store,
            stats,
            put_batch_rows: emitter
                .toast
                .put_batch_rows
                .map_or(CHUNK_PUT_BATCH, |n| n.get()),
            put_batch_bytes: emitter
                .toast
                .put_batch_bytes
                .map_or(CHUNK_PUT_BYTES, |n| n.get()),
            inline_value_max: emitter.inline_value_max,
            overflow: emitter.inline_value_overflow,
            budget: None,
        }
    }

    /// Store-backed resolver for tests
    pub fn with_store(store: Arc<dyn ChunkStore>, stats: Arc<EmitterStats>) -> Self {
        Self {
            store: Some(store),
            stats,
            put_batch_rows: CHUNK_PUT_BATCH,
            put_batch_bytes: CHUNK_PUT_BYTES,
            inline_value_max: usize::MAX,
            overflow: InlineValueOverflow::default(),
            budget: None,
        }
    }

    /// Leaf-permit pool, attached at pipeline spawn
    pub fn with_budget(mut self, budget: crate::budget::MemoryBudget) -> Self {
        self.budget = Some(budget);
        self
    }

    pub fn with_stats(mut self, stats: Arc<EmitterStats>) -> Self {
        self.stats = stats;
        self
    }

    /// Per-value cap override (tests)
    pub fn with_inline_value_max(mut self, max: usize) -> Self {
        self.inline_value_max = max;
        self
    }

    pub fn inline_value_max(&self) -> usize {
        self.inline_value_max
    }

    /// Override oversized value action
    pub fn with_overflow(mut self, overflow: InlineValueOverflow) -> Self {
        self.overflow = overflow;
        self
    }

    pub fn overflow(&self) -> InlineValueOverflow {
        self.overflow
    }

    /// Return whether value exceeds limit
    pub fn value_oversize(&self, p: &ToastPointer) -> bool {
        pointer_footprint(p) > self.inline_value_max
    }

    /// Replace an oversized value in place, counting the fill. True when the
    /// pointer is settled and needs no fetch
    pub fn fill_oversize(&self, col: &mut Option<ColumnValue>, p: &ToastPointer) -> bool {
        if !self.value_oversize(p) {
            return false;
        }
        self.note_filled_oversize();
        *col = Some(ColumnValue::Null);
        true
    }

    pub fn budget(&self) -> Option<&crate::budget::MemoryBudget> {
        self.budget.as_ref()
    }

    /// Whether resolver store accepts decoded chunks
    pub fn stores_chunks(&self) -> bool {
        self.store.as_ref().is_some_and(|s| s.accepts_writes())
    }

    pub fn put_limit_reached(&self, rows: usize, bytes: usize) -> bool {
        rows >= self.put_batch_rows || bytes >= self.put_batch_bytes
    }

    /// Shared counters, for commit-time stash resolution off the serial path
    pub fn stats_handle(&self) -> Arc<EmitterStats> {
        self.stats.clone()
    }

    /// Fill unresolved pointers only without store
    pub fn fill_on_miss(&self) -> bool {
        self.store.is_none()
    }

    /// As-of store fetch assembled against the pointer's stored size;
    /// `None` without store
    pub async fn fetch_value(
        &self,
        toast_relid: u32,
        value_id: u32,
        max_lsn: u64,
        expected_size: usize,
    ) -> Result<Option<FetchedValue>, ChunkStoreError> {
        let Some(v) = self
            .fetch_values(toast_relid, &[(value_id, expected_size)], max_lsn)
            .await?
        else {
            return Ok(None);
        };
        Ok(Some(v.into_iter().next().unwrap_or(FetchedValue::Missing)))
    }

    /// Fetch `values` from the store, results aligned with it; `None` without
    /// store. Value ids must be unique
    pub async fn fetch_values(
        &self,
        toast_relid: u32,
        values: &[(u32, usize)],
        max_lsn: u64,
    ) -> Result<Option<Vec<FetchedValue>>, ChunkStoreError> {
        let Some(store) = &self.store else {
            return Ok(None);
        };
        if values.is_empty() {
            return Ok(Some(Vec::new()));
        }
        let started = std::time::Instant::now();
        let got = store.fetch_many(toast_relid, values, max_lsn).await?;
        self.stats
            .toast_value_fetch_nanos
            .fetch_add(started.elapsed().as_nanos() as u64, Ordering::Relaxed);
        self.stats
            .toast_value_fetch_batches
            .fetch_add(1, Ordering::Relaxed);
        let assembled = got
            .iter()
            .filter(|v| matches!(v, FetchedValue::Assembled(_)))
            .count();
        self.stats
            .toast_values_fetched
            .fetch_add(assembled as u64, Ordering::Relaxed);
        Ok(Some(got))
    }

    /// Persist births and tombstones, no-op without store
    pub async fn put(&self, rows: &[ToastRow]) -> Result<(), ChunkStoreError> {
        let Some(store) = &self.store else {
            return Ok(());
        };
        if rows.is_empty() {
            return Ok(());
        }
        let started = std::time::Instant::now();
        store.put(rows).await?;
        self.stats
            .toast_chunk_put_nanos
            .fetch_add(started.elapsed().as_nanos() as u64, Ordering::Relaxed);
        self.stats.toast_chunk_puts.fetch_add(1, Ordering::Relaxed);
        let tombstones = rows.iter().filter(|r| r.is_tombstone()).count() as u64;
        self.stats
            .toast_chunks_stored
            .fetch_add(rows.len() as u64 - tombstones, Ordering::Relaxed);
        self.stats
            .toast_tombstones_stored
            .fetch_add(tombstones, Ordering::Relaxed);
        Ok(())
    }

    /// [`Self::put_batched`] over row refs: materialize each slice just in
    /// time so resident bodies peak at one sealed slice, covered by one
    /// leaf permit acquired before the reads
    pub async fn put_row_refs(
        &self,
        spool: Option<&BodySpoolFile>,
        rows: &[ToastRowRef],
    ) -> Result<(), ChunkStoreError> {
        if self.store.is_none() || rows.is_empty() {
            return Ok(());
        }
        let mut start = 0usize;
        while start < rows.len() {
            let mut end = start;
            let mut bytes = 0usize;
            while end < rows.len() && !self.put_limit_reached(end - start, bytes) {
                bytes += rows[end].chunk_data.len();
                end += 1;
            }
            let _leaf = crate::budget::acquire_opt(self.budget.as_ref(), bytes).await;
            let mut batch: Vec<ToastRow> = Vec::with_capacity(end - start);
            for r in &rows[start..end] {
                batch.push(r.materialize(spool)?);
            }
            self.put(&batch).await?;
            start = end;
        }
        Ok(())
    }

    /// [`Self::put`] in WAL-order slices sealed at configured row or byte limit
    pub async fn put_batched(&self, rows: &[ToastRow]) -> Result<(), ChunkStoreError> {
        let mut start = 0usize;
        let mut bytes = 0usize;
        for (i, r) in rows.iter().enumerate() {
            if i > start && self.put_limit_reached(i - start, bytes) {
                self.put(&rows[start..i]).await?;
                start = i;
                bytes = 0;
            }
            bytes += r.chunk_data.len();
        }
        self.put(&rows[start..]).await
    }

    async fn clear_mirror(
        &self,
        toast_relid: u32,
        metric: &AtomicU64,
    ) -> Result<(), ChunkStoreError> {
        let Some(store) = &self.store.as_ref().filter(|s| s.accepts_writes()) else {
            return Ok(());
        };
        store.truncate_mirror(toast_relid).await?;
        metric.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Empty owner mirror at TRUNCATE barrier
    pub async fn truncate_mirror(&self, toast_relid: u32) -> Result<(), ChunkStoreError> {
        self.clear_mirror(toast_relid, &self.stats.toast_mirror_truncates)
            .await
    }

    /// Empty retired mirror without dropping it, no-op without store
    pub async fn retire_mirror(&self, toast_relid: u32) -> Result<(), ChunkStoreError> {
        self.clear_mirror(toast_relid, &self.stats.toast_mirror_retires)
            .await
    }

    /// Residual `O - B` tombstones after a rewrite generation's births are
    /// put; no-op without store
    pub async fn rewrite_barrier(
        &self,
        toast_relid: u32,
        marker_lsn: u64,
        commit_lsn: u64,
    ) -> Result<(), ChunkStoreError> {
        let Some(store) = &self.store.as_ref().filter(|s| s.accepts_writes()) else {
            return Ok(());
        };
        store
            .rewrite_barrier(toast_relid, marker_lsn, commit_lsn)
            .await?;
        self.stats
            .toast_rewrite_barriers
            .fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    pub fn note_filled_default(&self) {
        self.stats
            .toast_values_filled_default
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Count oversized value replacement
    pub fn note_filled_oversize(&self) {
        self.stats
            .toast_values_filled_oversize
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Count fill from collapsed history or replayed owner TRUNCATE
    pub fn note_filled_superseded(&self) {
        self.stats
            .toast_values_filled_superseded
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Count dense store run shorter than pointer size
    pub fn note_filled_mismatch(&self) {
        self.stats
            .toast_values_filled_mismatch
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Count fill caused by value-ID reuse
    pub fn note_filled_generation(&self) {
        self.stats
            .toast_values_filled_generation
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn note_fetch_miss(&self) {
        self.stats.toast_fetch_miss.fetch_add(1, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(value_id: u32, seq: u32, tid: (u32, u16), lsn: u64, body: &[u8]) -> ToastRow {
        ToastRow {
            toast_relid: 16500,
            blkno: tid.0,
            offnum: tid.1,
            chunk_id: value_id,
            chunk_seq: seq,
            chunk_data: Bytes::copy_from_slice(body),
            lsn,
        }
    }

    fn assembled(body: &[u8]) -> FetchedValue {
        FetchedValue::Assembled(body.to_vec())
    }

    const PG_LZ4_CHUNKS: &[u8] = include_bytes!("testdata/compressed_external_lz4.bin");
    const PG_PGLZ_CHUNKS: &[u8] = include_bytes!("testdata/compressed_external_pglz.bin");
    const PG_PLAIN: &str = include_str!("testdata/compressed_external_plain.txt");

    /// Fixtures are the concatenated `pg_toast_*.chunk_data` of one
    /// compressed-external `text` value, taken verbatim from PostgreSQL 18
    /// with `default_toast_compression` set each way. Every other TOAST test
    /// uses `SET STORAGE EXTERNAL`, which turns compression off, so nothing
    /// else covers this path
    #[test]
    fn pg_compressed_external_chunks_carry_a_tcinfo_prefix() {
        for (method, chunks) in [(1u8, PG_LZ4_CHUNKS), (0, PG_PGLZ_CHUNKS)] {
            let raw_len = PG_PLAIN.len();
            let tcinfo = u32::from_le_bytes(chunks[..4].try_into().unwrap());
            assert_eq!(tcinfo & VARLENA_EXTSIZE_MASK, raw_len as u32);
            assert_eq!(tcinfo >> VARLENA_EXTSIZE_BITS, u32::from(method));

            let p = ToastPointer {
                va_rawsize: raw_len as i32 + VARHDRSZ,
                va_extinfo: chunks.len() as u32 | (u32::from(method) << VARLENA_EXTSIZE_BITS),
                va_valueid: 229503,
                va_toastrelid: 16550,
            };
            let out = finish_value(&p, chunks.to_vec()).expect("detoasts");
            assert_eq!(String::from_utf8(out).unwrap(), PG_PLAIN);

            assert!(
                decompress_varlena(method, chunks, raw_len).is_none(),
                "method {method}: the tcinfo prefix has to be skipped",
            );
        }
    }

    #[test]
    fn detoast_failure_names_the_value() {
        let p = ToastPointer {
            va_rawsize: 314008 + VARHDRSZ,
            va_extinfo: 79453 | (1 << VARLENA_EXTSIZE_BITS),
            va_valueid: 229503,
            va_toastrelid: 16550,
        };
        let err = finish_value(&p, vec![0u8; 79453]).expect_err("garbage fails");
        assert_eq!(
            err.to_string(),
            "toast decompression: decompress failed (method 1, 79449 bytes \u{2192} 314008) \
             for value 229503 in pg_toast_16550",
        );
    }

    fn tomb(tid: (u32, u16), lsn: u64) -> ToastRow {
        ToastRow::tombstone(&ToastDelete {
            toast_relid: 16500,
            blkno: tid.0,
            offnum: tid.1,
            source_lsn: lsn,
        })
    }

    #[tokio::test]
    async fn mem_store_roundtrip_and_absent_value() {
        let store = MemChunkStore::new();
        store
            .put(&[
                row(7, 0, (1, 1), 0x1000, b"abc"),
                row(7, 1, (1, 2), 0x1001, b"de"),
                row(9, 0, (1, 3), 0x1002, b"zz"),
            ])
            .await
            .unwrap();
        let got = store.fetch(16500, 7, u64::MAX, 5).await.unwrap();
        assert_eq!(got, assembled(b"abcde"));
        assert_eq!(
            store.fetch(16500, 404, u64::MAX, 3).await.unwrap(),
            FetchedValue::Missing
        );
        assert!(matches!(
            store.fetch(404, 7, u64::MAX, 3).await,
            Err(ChunkStoreError::MissingMirror(404))
        ));
    }

    #[tokio::test]
    async fn mem_store_tombstone_hides_value_as_of_death() {
        let store = MemChunkStore::new();
        store
            .put(&[row(7, 0, (1, 1), 0x1000, b"abc"), tomb((1, 1), 0x2000)])
            .await
            .unwrap();
        let live = store.fetch(16500, 7, 0x1fff, 3).await.unwrap();
        assert_eq!(live, assembled(b"abc"));
        assert_eq!(
            store.fetch(16500, 7, 0x2000, 3).await.unwrap(),
            FetchedValue::Missing
        );
        assert_eq!(
            store.fetch(16500, 7, u64::MAX, 3).await.unwrap(),
            FetchedValue::Missing
        );
    }

    #[tokio::test]
    async fn mem_store_tid_reuse_supersedes_tombstone_and_old_value() {
        let store = MemChunkStore::new();
        store
            .put(&[
                row(7, 0, (1, 1), 0x1000, b"old"),
                tomb((1, 1), 0x2000),
                row(9, 0, (1, 1), 0x3000, b"new"),
            ])
            .await
            .unwrap();
        assert_eq!(
            store.fetch(16500, 7, u64::MAX, 3).await.unwrap(),
            FetchedValue::Missing
        );
        let new = store.fetch(16500, 9, u64::MAX, 3).await.unwrap();
        assert_eq!(new, assembled(b"new"));
        let old = store.fetch(16500, 7, 0x1fff, 3).await.unwrap();
        assert_eq!(old, assembled(b"old"));
    }

    #[tokio::test]
    async fn mem_store_lagging_bound_excludes_future_generation() {
        let store = MemChunkStore::new();
        store
            .put(&[
                row(7, 0, (1, 1), 0x1000, b"g1-0"),
                row(7, 1, (1, 2), 0x1001, b"g1-1"),
                tomb((1, 1), 0x2000),
                tomb((1, 2), 0x2001),
                row(7, 0, (2, 1), 0x3000, b"g2-0"),
            ])
            .await
            .unwrap();
        let old = store.fetch(16500, 7, 0x1fff, 8).await.unwrap();
        assert_eq!(old, assembled(b"g1-0g1-1"));
        // Dead generation's seq 1 dropped: only g2's seq 0 assembles
        let new = store.fetch(16500, 7, u64::MAX, 4).await.unwrap();
        assert_eq!(new, assembled(b"g2-0"));
    }

    #[tokio::test]
    async fn mem_store_newest_per_seq_under_duplicate_live_copies() {
        let store = MemChunkStore::new();
        store
            .put(&[
                row(7, 0, (1, 1), 0x1000, b"copy-a"),
                row(7, 0, (2, 1), 0x2000, b"copy-b"),
            ])
            .await
            .unwrap();
        let got = store.fetch(16500, 7, u64::MAX, 6).await.unwrap();
        assert_eq!(got, assembled(b"copy-b"));
    }

    #[tokio::test]
    async fn mem_store_same_commit_birth_then_death() {
        let store = MemChunkStore::new();
        store
            .put(&[row(7, 0, (1, 1), 0x1000, b"x"), tomb((1, 1), 0x1001)])
            .await
            .unwrap();
        assert_eq!(
            store.fetch(16500, 7, u64::MAX, 1).await.unwrap(),
            FetchedValue::Missing
        );
        assert_eq!(
            store.fetch(16500, 7, 0x1000, 1).await.unwrap(),
            assembled(b"x")
        );
    }

    #[tokio::test]
    async fn mem_store_truncate_empties_without_uncreating() {
        let store = MemChunkStore::new();
        store
            .put(&[row(7, 0, (1, 1), 0x1000, b"abc"), {
                let mut other = row(9, 0, (1, 1), 0x1000, b"zz");
                other.toast_relid = 200;
                other
            }])
            .await
            .unwrap();
        store.truncate_mirror(16500).await.unwrap();
        assert_eq!(
            store.fetch(16500, 7, u64::MAX, 3).await.unwrap(),
            FetchedValue::Missing
        );
        assert_eq!(
            store.fetch(200, 9, u64::MAX, 2).await.unwrap(),
            assembled(b"zz")
        );
        store.truncate_mirror(404).await.unwrap();
        assert!(matches!(
            store.fetch(404, 7, u64::MAX, 3).await,
            Err(ChunkStoreError::MissingMirror(404))
        ));
        store
            .put(&[row(11, 0, (0, 1), 0x2000, b"new")])
            .await
            .unwrap();
        assert_eq!(
            store.fetch(16500, 11, u64::MAX, 3).await.unwrap(),
            assembled(b"new")
        );
    }

    #[tokio::test]
    async fn mem_store_rewrite_barrier_tombstones_residual_tids() {
        let store = MemChunkStore::new();
        // Old generation: value 7 at (1,1)/(1,2), value 9 at (2,1) dead
        // pre-marker, (3,1) live pre-marker
        store
            .put(&[
                row(7, 0, (1, 1), 0x1000, b"a"),
                row(7, 1, (1, 2), 0x1001, b"b"),
                row(9, 0, (2, 1), 0x1002, b"c"),
                tomb((2, 1), 0x1500),
                row(11, 0, (3, 1), 0x1003, b"d"),
            ])
            .await
            .unwrap();
        // Rewrite generation reuses (1,1) for value 7's single chunk
        store
            .put(&[row(7, 0, (1, 1), 0x3000, b"a2")])
            .await
            .unwrap();
        store.rewrite_barrier(16500, 0x2000, 0x4000).await.unwrap();
        // Reused TID survives at the birth version
        let v7 = store.fetch(16500, 7, u64::MAX, 2).await.unwrap();
        assert_eq!(v7, assembled(b"a2"));
        // Residual live TIDs (1,2) and (3,1) tombstoned; dead (2,1) untouched
        assert_eq!(
            store.fetch(16500, 11, u64::MAX, 1).await.unwrap(),
            FetchedValue::Missing
        );
        // As-of before the rewrite still resolves the old generation whole
        let old = store.fetch(16500, 7, 0x1fff, 2).await.unwrap();
        assert_eq!(old, assembled(b"ab"));
        // Re-run converges: prior residuals sit past the marker, no new rows
        let before = store.mirrors.lock().unwrap().get(&16500).unwrap().len();
        store.rewrite_barrier(16500, 0x2000, 0x4000).await.unwrap();
        let after = store.mirrors.lock().unwrap().get(&16500).unwrap().len();
        assert_eq!(before, after, "barrier re-run must insert nothing");
        // Empty generation: nothing past marker, every live TID dies
        store.rewrite_barrier(16500, 0x5000, 0x6000).await.unwrap();
        assert_eq!(
            store.fetch(16500, 7, u64::MAX, 2).await.unwrap(),
            FetchedValue::Missing
        );
        // Missing mirror is a no-op
        store.rewrite_barrier(404, 0x10, 0x20).await.unwrap();
    }

    #[tokio::test]
    async fn resolver_mirror_ops_count_and_noop_without_store() {
        let stats = Arc::new(EmitterStats::default());
        let r = ToastResolver::with_store(Arc::new(MemChunkStore::new()), stats.clone());
        r.put(&[row(7, 0, (1, 1), 0x1000, b"x")]).await.unwrap();
        r.truncate_mirror(16500).await.unwrap();
        r.retire_mirror(16500).await.unwrap();
        assert_eq!(stats.toast_mirror_truncates.load(Ordering::Relaxed), 1);
        assert_eq!(stats.toast_mirror_retires.load(Ordering::Relaxed), 1);

        let disabled = ToastResolver::disabled();
        disabled.truncate_mirror(16500).await.unwrap();
        disabled.retire_mirror(16500).await.unwrap();
    }

    #[test]
    fn value_ref_extends_run_only_on_dense_contiguous_file_append() {
        let f = |offset, len| Body::File(BodyRef { offset, len });
        // Dense contiguous file appends stay one compact run
        let mut v = ValueRef::new(0, f(0, 4));
        v.push(1, f(4, 4));
        v.push(2, f(8, 2));
        assert_eq!((v.run.offset, v.run.len, v.run_chunks), (0, 10, 3));
        assert!(v.tail.is_empty());
        // Non-contiguous append (another value interleaved) → tail
        v.push(3, f(20, 4));
        assert_eq!(v.run_chunks, 3);
        assert_eq!(v.tail.get(&3), Some(&f(20, 4)));
        // Later dense chunk still lands in tail once degraded
        v.push(4, f(24, 4));
        assert_eq!(v.tail.len(), 2);
        // Out-of-order start: empty run, tail from the get-go
        let v2 = ValueRef::new(2, f(0, 4));
        assert_eq!((v2.run_chunks, v2.run.len), (0, 0));
        assert_eq!(v2.tail.get(&2), Some(&f(0, 4)));
        // Memory bodies never extend a run
        let mut v3 = ValueRef::new(0, Body::Mem(Bytes::from_static(b"abcd")));
        v3.push(1, f(0, 4));
        assert_eq!(v3.run_chunks, 0);
        assert_eq!(v3.tail.len(), 2);
    }

    #[tokio::test]
    async fn put_row_refs_materializes_mem_and_file_bodies() {
        let tmp = tempfile::tempdir().unwrap();
        let mut w = crate::xact::spill::BodySpoolWriter::create(tmp.path(), 1, 0x40, None).unwrap();
        let store = Arc::new(MemChunkStore::new());
        let stats = Arc::new(EmitterStats::default());
        let r = ToastResolver::with_store(store.clone(), stats);
        let mut refs = vec![ToastRowRef {
            toast_relid: 16500,
            blkno: 1,
            offnum: 1,
            chunk_id: 7,
            chunk_seq: 0,
            chunk_data: Body::Mem(Bytes::from_static(b"aaaa")),
            lsn: 0x1000,
        }];
        for seq in 1..3u32 {
            let body = [b'a' + seq as u8; 4];
            refs.push(ToastRowRef {
                toast_relid: 16500,
                blkno: 1,
                offnum: 1 + seq as u16,
                chunk_id: 7,
                chunk_seq: seq,
                chunk_data: Body::File(w.append(&body).unwrap()),
                lsn: 0x1000 + u64::from(seq),
            });
        }
        refs.push(ToastRowRef::tombstone(&ToastDelete {
            toast_relid: 16500,
            blkno: 9,
            offnum: 9,
            source_lsn: 0x2000,
        }));
        w.flush().unwrap();
        r.put_row_refs(Some(w.shared().as_ref()), &refs)
            .await
            .unwrap();
        let got = store.fetch(16500, 7, u64::MAX, 12).await.unwrap();
        assert_eq!(got, assembled(b"aaaabbbbcccc"));
        // Tombstone materializes bodiless, no spool needed
        let tomb = refs[3].materialize(None).unwrap();
        assert!(tomb.is_tombstone() && tomb.chunk_data.is_empty());
        // File body without spool is an error, not a panic
        assert!(refs[1].materialize(None).is_err());
    }

    /// Read-only backend neither stores chunks nor fills missing values
    #[tokio::test(flavor = "current_thread")]
    async fn read_only_store_reads_without_accepting_writes() {
        struct ReadOnly(MemChunkStore);

        #[async_trait]
        impl ChunkStore for ReadOnly {
            fn accepts_writes(&self) -> bool {
                false
            }
            async fn put(&self, _rows: &[ToastRow]) -> Result<(), ChunkStoreError> {
                Err(ChunkStoreError::ReadOnly("put"))
            }
            async fn fetch_many(
                &self,
                relid: u32,
                values: &[(u32, usize)],
                max_lsn: u64,
            ) -> Result<Vec<FetchedValue>, ChunkStoreError> {
                self.0.fetch_many(relid, values, max_lsn).await
            }
            async fn truncate_mirror(&self, _relid: u32) -> Result<(), ChunkStoreError> {
                Err(ChunkStoreError::ReadOnly("truncate_mirror"))
            }
            async fn rewrite_barrier(
                &self,
                _relid: u32,
                _marker: u64,
                _commit: u64,
            ) -> Result<(), ChunkStoreError> {
                Err(ChunkStoreError::ReadOnly("rewrite_barrier"))
            }
        }

        let inner = MemChunkStore::new();
        inner
            .put(&[ToastRow {
                toast_relid: 16500,
                blkno: 1,
                offnum: 1,
                chunk_id: 7,
                chunk_seq: 0,
                chunk_data: Bytes::from_static(b"body"),
                lsn: 0x1000,
            }])
            .await
            .unwrap();

        let r =
            ToastResolver::with_store(Arc::new(ReadOnly(inner)), Arc::new(EmitterStats::default()));
        assert!(!r.stores_chunks(), "nothing should collect rows for it");
        assert!(!r.fill_on_miss(), "a miss still has to be explained");
        assert_eq!(
            r.fetch_value(16500, 7, u64::MAX, 4).await.unwrap(),
            Some(assembled(b"body")),
        );
        // Reject writes instead of dropping rows
        assert!(
            r.put(&[ToastRow {
                toast_relid: 16500,
                blkno: 2,
                offnum: 1,
                chunk_id: 8,
                chunk_seq: 0,
                chunk_data: Bytes::from_static(b"x"),
                lsn: 0x2000,
            }])
            .await
            .is_err()
        );
        r.truncate_mirror(16500)
            .await
            .expect("no mirror to empty, so a TRUNCATE barrier has nothing to do");
        r.rewrite_barrier(16500, 1, 2)
            .await
            .expect("no mirror to tombstone, so a rewrite barrier has nothing to do");
    }

    /// Shadow mode requires bridge, disabled mode keeps no store
    #[test]
    fn for_mode_honours_the_setting() {
        let stats = || Arc::new(EmitterStats::default());
        let ch = EmitterConfig::from_toml_str("[toast]\nmode = \"clickhouse\"\n").unwrap();
        assert!(!ch.toast.mode.is_shadow());
        let r = ToastResolver::for_mode(&ch, stats(), None).expect("clickhouse needs no bridge");
        assert!(r.stores_chunks(), "the mirror takes writes");

        let default = EmitterConfig::from_toml_str("").unwrap();
        assert_eq!(default.toast.mode, ToastMode::Clickhouse);

        let shadow = EmitterConfig::from_toml_str("[toast]\nmode = \"shadow\"\n").unwrap();
        assert!(shadow.toast.mode.is_shadow());
        let err = match ToastResolver::for_mode(&shadow, stats(), None) {
            Err(e) => e,
            Ok(_) => panic!("shadow without a bridge must be a config error"),
        };
        assert!(err.contains("bridge"), "{err}");

        let disabled = EmitterConfig::from_toml_str(
            "[memory]\ninline_value_max = 4096\n[toast]\nmode = \"disabled\"\n",
        )
        .unwrap();
        assert_eq!(disabled.toast.mode, ToastMode::Disabled);
        for r in [
            ToastResolver::for_mode(&disabled, stats(), None).expect("disabled needs no bridge"),
            ToastResolver::from_config(&disabled, stats()),
        ] {
            assert!(!r.stores_chunks() && r.fill_on_miss());
            assert_eq!(
                r.inline_value_max(),
                4096,
                "values in current transaction keep size limit"
            );
        }

        assert!(
            EmitterConfig::from_toml_str("[toast]\nmode = \"elsewhere\"\n").is_err(),
            "an unknown mode must not fall back to the mirror",
        );
    }

    /// Oversized under a tiny budget: overshoots and stores, never an
    /// error, OOM, or forever-wait
    #[tokio::test(flavor = "current_thread")]
    async fn put_row_refs_oversized_slice_overshoots_under_tiny_budget() {
        let store = Arc::new(MemChunkStore::new());
        let stats = Arc::new(EmitterStats::default());
        let budget = crate::budget::MemoryBudget::new(1 << 10);
        let r = ToastResolver::with_store(store.clone(), stats).with_budget(budget.clone());
        let refs = [ToastRowRef {
            toast_relid: 16500,
            blkno: 1,
            offnum: 1,
            chunk_id: 7,
            chunk_seq: 0,
            chunk_data: Body::Mem(Bytes::from(vec![0u8; 2 << 10])),
            lsn: 0x1000,
        }];
        r.put_row_refs(None, &refs).await.unwrap();
        assert_eq!(
            store.fetch(16500, 7, u64::MAX, 2 << 10).await.unwrap(),
            assembled(&[0u8; 2 << 10])
        );
        assert_eq!(budget.overshoots_total(), 1);
        assert_eq!(budget.resident_bytes(), 0, "nothing leaked");
    }

    #[tokio::test]
    async fn resolver_disabled_fills_on_miss_no_store() {
        let r = ToastResolver::disabled();
        assert!(r.fill_on_miss());
        assert!(!r.stores_chunks());
        assert!(r.fetch_value(1, 2, u64::MAX, 3).await.unwrap().is_none());
        r.put(&[row(2, 0, (1, 1), 0x1000, b"x")]).await.unwrap();
    }

    #[tokio::test]
    async fn resolver_mem_stores_and_fetches() {
        let stats = Arc::new(EmitterStats::default());
        let r = ToastResolver::with_store(Arc::new(MemChunkStore::new()), stats.clone());
        assert!(!r.fill_on_miss());
        assert!(r.stores_chunks());

        r.put(&[row(7, 0, (1, 1), 0x2000, b"hi"), tomb((9, 9), 0x2001)])
            .await
            .unwrap();
        assert_eq!(stats.toast_chunks_stored.load(Ordering::Relaxed), 1);
        assert_eq!(stats.toast_tombstones_stored.load(Ordering::Relaxed), 1);

        let got = r.fetch_value(16500, 7, u64::MAX, 2).await.unwrap();
        assert_eq!(got, Some(assembled(b"hi")));
        assert_eq!(stats.toast_values_fetched.load(Ordering::Relaxed), 1);

        let miss = r.fetch_value(16500, 404, u64::MAX, 2).await.unwrap();
        assert_eq!(miss, Some(FetchedValue::Missing));
        assert_eq!(
            stats.toast_values_fetched.load(Ordering::Relaxed),
            1,
            "miss does not count as fetched"
        );
    }

    #[tokio::test]
    async fn assembler_maps_deviations_per_miss_policy() {
        // Gap: seqs 0,2 (partial collapse can leave any subset)
        let mut asm = ChunkAssembler::new(4);
        asm.push(0, b"ab").unwrap();
        asm.push(2, b"cd").unwrap();
        assert_eq!(asm.finish(), FetchedValue::Mismatch { got: 4 });
        // Dense but short
        let mut asm = ChunkAssembler::new(4);
        asm.push(0, b"ab").unwrap();
        assert_eq!(asm.finish(), FetchedValue::Mismatch { got: 2 });
        // Overrun stops copying, reports full got
        let mut asm = ChunkAssembler::new(3);
        asm.push(0, b"ab").unwrap();
        asm.push(1, b"cd").unwrap();
        assert_eq!(asm.finish(), FetchedValue::Mismatch { got: 4 });
        // Disorder / duplicate is a contract error, not an outcome
        let mut asm = ChunkAssembler::new(4);
        asm.push(1, b"cd").unwrap();
        assert!(asm.push(0, b"ab").is_err());
        let mut asm = ChunkAssembler::new(4);
        asm.push(0, b"ab").unwrap();
        assert!(asm.push(0, b"ab").is_err());
        // Empty is Missing
        assert_eq!(ChunkAssembler::new(4).finish(), FetchedValue::Missing);
        // Exact assembles
        let mut asm = ChunkAssembler::new(4);
        asm.push(0, b"ab").unwrap();
        asm.push(1, b"cd").unwrap();
        assert_eq!(asm.finish(), assembled(b"abcd"));
    }

    /// Fanning one batch across slots spends N part commits to move the
    /// bytes of one
    #[tokio::test]
    async fn one_sealed_batch_is_one_insert() {
        let store = Arc::new(MemChunkStore::new());
        let stats = Arc::new(EmitterStats::default());
        let r = ToastResolver::with_store(store.clone(), stats.clone());
        let rows: Vec<ToastRow> = (0..64u32)
            .map(|i| row(7, i, (1, 1 + i as u16), 0x1000 + u64::from(i), b"aa"))
            .collect();

        r.put(&rows).await.unwrap();

        assert_eq!(
            stats.toast_chunk_puts.load(Ordering::Relaxed),
            1,
            "a batch that fits one seal must cost exactly one INSERT",
        );
        assert_eq!(stats.toast_chunks_stored.load(Ordering::Relaxed), 64);
    }

    /// A restore is often one relid, so slots must not be keyed on it
    #[tokio::test]
    async fn chunk_store_slots_are_concurrent_for_one_relid() {
        let store = ClickHouseChunkStore::new(EmitterConfig {
            inserter_pool_size: 4,
            ..EmitterConfig::default()
        });
        assert_eq!(store.states.len(), 4);

        let mut held = Vec::new();
        for _ in 0..4 {
            held.push(store.slot().await);
        }
        assert_eq!(held.len(), 4, "every slot handed out without blocking");
        drop(held);

        let pinned = 16505usize % store.states.len();
        let a = store.slot_for(16505).await;
        assert!(
            store.states[pinned].try_lock().is_err(),
            "slot_for must pin a relid to one slot",
        );
        drop(a);
    }

    #[tokio::test]
    async fn put_batched_preserves_order_across_slices() {
        let store = Arc::new(MemChunkStore::new());
        let stats = Arc::new(EmitterStats::default());
        let r = ToastResolver::with_store(store.clone(), stats);
        // offnum is u16 and the seal exceeds it; carry into blkno
        let rows: Vec<ToastRow> = (0..CHUNK_PUT_BATCH as u32 + 3)
            .map(|i| {
                let tid = (1 + i / 1000, 1 + (i % 1000) as u16);
                row(7, i, tid, 0x1000 + u64::from(i), b"aa")
            })
            .collect();
        r.put_batched(&rows).await.unwrap();
        let expected = 2 * rows.len();
        let got = store.fetch(16500, 7, u64::MAX, expected).await.unwrap();
        assert_eq!(
            got,
            assembled(&b"aa".repeat(rows.len())),
            "all slices landed, in order"
        );
    }

    #[test]
    fn toast_config_keeps_connections_independent_of_inserters() {
        let cfg = EmitterConfig::from_toml_str(
            "[ch]\ninserter_pool_size = 8\n[toast]\nconnections = 2\n",
        )
        .unwrap();
        assert_eq!(cfg.inserter_pool_size, 8);
        assert_eq!(ClickHouseChunkStore::new(cfg).states.len(), 2);
        for key in ["put_batch_rows", "put_batch_bytes", "connections"] {
            assert!(
                EmitterConfig::from_toml_str(&format!("[toast]\n{key} = 0\n")).is_err(),
                "{key} must reject zero"
            );
        }
    }

    #[tokio::test]
    async fn configured_put_limits_preserve_materialized_and_referenced_rows() {
        let refs: Vec<_> = (0..7)
            .map(|seq| ToastRowRef {
                toast_relid: 16500,
                blkno: 1,
                offnum: 1 + seq as u16,
                chunk_id: 7,
                chunk_seq: seq,
                chunk_data: Body::Mem(Bytes::from_static(b"ab")),
                lsn: 0x1000 + u64::from(seq),
            })
            .collect();
        for (rows, bytes, puts) in [(3, 1024, 3), (100, 3, 4)] {
            let cfg = EmitterConfig::from_toml_str(&format!(
                "[toast]\nput_batch_rows = {rows}\nput_batch_bytes = {bytes}\n"
            ))
            .unwrap();
            for referenced in [false, true] {
                let store = Arc::new(MemChunkStore::new());
                let stats = Arc::new(EmitterStats::default());
                let mut resolver = ToastResolver::from_config(&cfg, stats.clone());
                resolver.store = Some(store.clone());
                if referenced {
                    resolver.put_row_refs(None, &refs).await.unwrap();
                } else {
                    let rows: Vec<_> = refs.iter().map(|r| r.materialize(None).unwrap()).collect();
                    resolver.put_batched(&rows).await.unwrap();
                }
                assert_eq!(stats.toast_chunk_puts.load(Ordering::Relaxed), puts);
                assert_eq!(
                    store.fetch(16500, 7, u64::MAX, 14).await.unwrap(),
                    assembled(&b"ab".repeat(7))
                );
            }
        }
    }

    #[test]
    fn chunk_id_index_stays_selective_at_the_widths_reads_use() {
        let effective = |fp: f64, n: i32| 1.0 - (1.0 - fp).powi(n);

        assert!(effective(0.025, FETCH_QUERY_IDS as i32) > 0.9);
        assert!((effective(0.025, 33) - 0.566).abs() < 0.01);

        for n in [1, 8, 33, FETCH_QUERY_IDS as i32] {
            let e = effective(CHUNK_ID_INDEX_FP, n);
            assert!(
                e < 0.15,
                "fp {CHUNK_ID_INDEX_FP} admits {e:.3} of granules at {n} ids",
            );
        }
    }

    #[test]
    fn ch_store_renders_toast_schema_and_sql() {
        let cfg = EmitterConfig {
            database: "wh".into(),
            ..Default::default()
        };
        let store = ClickHouseChunkStore::new(cfg);

        assert_eq!(store.toast_table(16500), "`wh`.`pg_toast_16500`");

        assert_eq!(
            store.create_sql(16500),
            "CREATE TABLE IF NOT EXISTS `wh`.`pg_toast_16500` (\n  \
             `blkno` UInt32,\n  `offnum` UInt16,\n  `chunk_id` UInt32,\n  `chunk_seq` UInt32,\n  \
             `chunk_data` String,\n  `_lsn` UInt64,\n  `_is_deleted` UInt8,\n  \
             INDEX `idx_chunk_id` `chunk_id` \
             TYPE bloom_filter(0.000001) GRANULARITY 1\n\
             ) ENGINE = ReplacingMergeTree(`_lsn`, `_is_deleted`)\nORDER BY (`blkno`, `offnum`)"
        );

        assert_eq!(
            store.insert_sql(16500),
            "INSERT INTO `wh`.`pg_toast_16500` \
             (`blkno`, `offnum`, `chunk_id`, `chunk_seq`, `chunk_data`, `_lsn`, `_is_deleted`) \
             FORMAT Native"
        );

        assert_eq!(
            store.truncate_sql(16500),
            "TRUNCATE TABLE IF EXISTS `wh`.`pg_toast_16500`"
        );

        assert_eq!(
            store.rewrite_barrier_sql(16500, 0x2000, 0x4000),
            "INSERT INTO `wh`.`pg_toast_16500` (`blkno`, `offnum`, `chunk_id`, `chunk_seq`, \
             `chunk_data`, `_lsn`, `_is_deleted`)\n\
             SELECT `blkno`, `offnum`, 0, 0, '', 16384, 1\n\
             FROM `wh`.`pg_toast_16500`\n\
             GROUP BY `blkno`, `offnum`\n\
             HAVING max(`_lsn`) <= 8192 AND argMax(`_is_deleted`, `_lsn`) = 0\n\
             SETTINGS optimize_aggregation_in_order = 1"
        );

        assert_eq!(
            store.fetch_sql(16500, &[7, 9], 0x2000),
            "SELECT `chunk_id`, `chunk_seq`, \
             argMax(`chunk_data`, (`ver`, `blkno`, `offnum`)) AS `chunk_data`\n\
             FROM (\n  \
             SELECT `blkno`, `offnum`,\n         \
             argMax(`chunk_id`, `_lsn`) AS `chunk_id`,\n         \
             argMax(`chunk_seq`, `_lsn`) AS `chunk_seq`,\n         \
             argMax(`chunk_data`, `_lsn`) AS `chunk_data`,\n         \
             max(`_lsn`) AS `ver`,\n         \
             argMax(`_is_deleted`, `_lsn`) AS `dead`\n  \
             FROM `wh`.`pg_toast_16500`\n  \
             WHERE `_lsn` <= 8192\n    \
             AND (`blkno`, `offnum`) IN (\n      \
             SELECT `blkno`, `offnum` FROM `wh`.`pg_toast_16500`\n      \
             WHERE `chunk_id` IN (7, 9) AND `_lsn` <= 8192)\n  \
             GROUP BY `blkno`, `offnum`\n\
             )\n\
             WHERE `chunk_id` IN (7, 9) AND `dead` = 0\n\
             GROUP BY `chunk_id`, `chunk_seq`\n\
             ORDER BY `chunk_id`, `chunk_seq`\n\
             SETTINGS max_block_size = 1024"
        );
    }

    #[test]
    fn ch_mode_builds_a_store() {
        let cfg = EmitterConfig::default();
        let r = ToastResolver::from_config(&cfg, Arc::new(EmitterStats::default()));
        assert!(r.stores_chunks());
        assert!(!r.fill_on_miss());
    }
}
