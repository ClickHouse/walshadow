//! Durable backstop for TOAST chunks outside current transaction buffer
//!
//! ClickHouse mirrors line-pointer occupancy by heap TID, versioned by WAL
//! record LSN. `ReplacingMergeTree` reclaims tombstoned chunk bodies

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use clickhouse_c::{Allocator, AsyncClient, Block, BlockBuilder, ColumnBuilder, Event, TypeAst};
use thiserror::Error;
use tokio::sync::Mutex;

use bytes::Bytes;

use crate::ch::{
    EmitterError, backoff_step, connect_client, drain_to_end_of_stream, is_retryable, quote_ident,
    with_timeout,
};
use crate::decode::heap_decoder::{
    ColumnValue, ToastPointer, VARLENA_EXTSIZE_BITS, VARLENA_EXTSIZE_MASK, decompress_varlena,
};
use crate::emit::ch_emitter::{EmitterConfig, EmitterStats};
#[cfg(test)]
use crate::mapping::ToastConfig;
use crate::mapping::ToastMode;
use crate::xact::spill::{BodyRef, BodySpoolFile, ToastChunk, ToastDelete};

/// `(toast_relid, value_id) -> chunk_seq -> bytes`; bodies shared with
/// mirror rows via `Bytes`
pub type ChunkMap = HashMap<(u32, u32), BTreeMap<u32, Bytes>>;

/// Row seal for one store put slice
pub const CHUNK_PUT_BATCH: usize = 256;
/// Byte seal for one store put slice; typical chunks trip the row seal
/// first, this bounds atypically fat bodies
pub const CHUNK_PUT_BYTES: usize = 4 << 20;
/// Fetch result rows per block: bounds one block's buffer to ~2 MiB at
/// `TOAST_MAX_CHUNK_SIZE`, ordering validated across block boundaries
const FETCH_BLOCK_ROWS: usize = 1024;
/// PG `VARHDRSZ`, 4-byte varlena header
const VARHDRSZ: i32 = 4;

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

/// Validate value caps, return heap leaf-permit need
pub(crate) fn check_value_caps(
    pointers: &[ToastPointer],
    max: usize,
) -> Result<usize, ToastValueError> {
    let mut retained = 0usize;
    let mut transient = 0usize;
    for p in pointers {
        let raw = (p.va_rawsize - VARHDRSZ).max(0) as usize;
        let ext = pointer_extsize(p);
        if raw.max(ext) > max {
            return Err(ToastValueError::ValueTooLarge {
                rawsize: raw.max(ext),
                max,
            });
        }
        let compressed = (p.va_extinfo & !VARLENA_EXTSIZE_MASK) != 0;
        retained += if compressed { raw } else { ext };
        if compressed {
            transient = transient.max(ext);
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
    let compressed = (p.va_extinfo & !VARLENA_EXTSIZE_MASK) != 0;
    if !compressed {
        return Ok(stored);
    }
    let method = ((p.va_extinfo >> VARLENA_EXTSIZE_BITS) & 0x3) as u8;
    let raw_len = (p.va_rawsize - VARHDRSZ).max(0) as usize;
    match decompress_varlena(method, &stored, raw_len) {
        Some(out) => Ok(out),
        None => Err(ToastValueError::Detoast(format!(
            "decompress failed (method {method}, {} bytes → {raw_len})",
            stored.len()
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
#[derive(Debug, PartialEq, Eq)]
pub enum FetchedValue {
    /// No live chunk visible at bound
    Missing,
    /// Chunk run deviates from pointer size (partial merge collapse or
    /// generation mixing): gapped, short, or over-long. Fills per miss
    /// policy, counted distinctly
    Mismatch { got: usize },
    /// Exactly `expected_size` bytes, seq-dense from 0
    Assembled(Vec<u8>),
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
    /// Replay emits byte-identical rows at equal key and version
    async fn put(&self, rows: &[ToastRow]) -> Result<(), ChunkStoreError>;
    /// Assemble newest live row per sequence at `max_lsn` against the
    /// pointer's stored size (`va_extsize`)
    ///
    /// [`FetchedValue::Missing`] when no live row remains at bound,
    /// [`ChunkStoreError::MissingMirror`] when mirror is absent
    async fn fetch(
        &self,
        toast_relid: u32,
        value_id: u32,
        max_lsn: u64,
        expected_size: usize,
    ) -> Result<FetchedValue, ChunkStoreError>;
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

    async fn fetch(
        &self,
        toast_relid: u32,
        value_id: u32,
        max_lsn: u64,
        expected_size: usize,
    ) -> Result<FetchedValue, ChunkStoreError> {
        let mirrors = self.mirrors.lock().unwrap();
        let Some(rows) = mirrors.get(&toast_relid) else {
            return Err(ChunkStoreError::MissingMirror(toast_relid));
        };
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
        let mut newest: BTreeMap<u32, (u64, &[u8])> = BTreeMap::new();
        for r in latest.into_values() {
            if r.is_tombstone() || r.chunk_id != value_id {
                continue;
            }
            newest
                .entry(r.chunk_seq)
                .and_modify(|e| {
                    if r.lsn >= e.0 {
                        *e = (r.lsn, &r.chunk_data);
                    }
                })
                .or_insert((r.lsn, &r.chunk_data));
        }
        let mut asm = ChunkAssembler::new(expected_size);
        for (seq, (_, body)) in newest {
            asm.push(seq, body).map_err(ChunkStoreError::Clickhouse)?;
        }
        Ok(asm.finish())
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
    client: Option<AsyncClient>,
    created: HashSet<u32>,
}

/// TID-keyed ClickHouse mirror, one table per TOAST relation
///
/// Fetch aggregates version history explicitly, independent of merge state
pub struct ClickHouseChunkStore {
    conn: EmitterConfig,
    alloc: Allocator,
    state: Mutex<ChState>,
}

impl ClickHouseChunkStore {
    pub fn new(conn: EmitterConfig) -> Self {
        Self {
            conn,
            alloc: Allocator::global(&mimalloc::MiMalloc),
            state: Mutex::new(ChState {
                client: None,
                created: HashSet::new(),
            }),
        }
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
             INDEX `idx_chunk_id` `chunk_id` TYPE bloom_filter GRANULARITY 1\n\
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

    /// `O - B` server-side: TIDs live as of the marker minus TIDs with any
    /// row past it (generation births + previously inserted residuals, so
    /// re-runs insert nothing). Aggregate alias must differ from the source
    /// column (CH error 184)
    fn rewrite_barrier_sql(&self, toast_relid: u32, marker_lsn: u64, commit_lsn: u64) -> String {
        let table = self.toast_table(toast_relid);
        format!(
            "INSERT INTO {table} (`blkno`, `offnum`, `chunk_id`, `chunk_seq`, `chunk_data`, \
             `_lsn`, `_is_deleted`)\n\
             SELECT `blkno`, `offnum`, 0, 0, '', {commit_lsn}, 1\n\
             FROM (\n  \
             SELECT `blkno`, `offnum`, argMax(`_is_deleted`, `_lsn`) AS `dead`\n  \
             FROM {table}\n  \
             WHERE `_lsn` <= {marker_lsn}\n  \
             GROUP BY `blkno`, `offnum`\n\
             )\n\
             WHERE `dead` = 0\n  \
             AND (`blkno`, `offnum`) NOT IN (\n    \
             SELECT DISTINCT `blkno`, `offnum` FROM {table} WHERE `_lsn` > {marker_lsn})"
        )
    }

    /// Bloom-prune candidate TIDs, then aggregate full history before filtering
    fn fetch_sql(&self, toast_relid: u32, value_id: u32, max_lsn: u64) -> String {
        let table = self.toast_table(toast_relid);
        format!(
            "SELECT `chunk_seq`, argMax(`chunk_data`, `ver`) AS `chunk_data`\n\
             FROM (\n  \
             SELECT argMax(`chunk_id`, `_lsn`) AS `chunk_id`,\n         \
             argMax(`chunk_seq`, `_lsn`) AS `chunk_seq`,\n         \
             argMax(`chunk_data`, `_lsn`) AS `chunk_data`,\n         \
             max(`_lsn`) AS `ver`,\n         \
             argMax(`_is_deleted`, `_lsn`) AS `dead`\n  \
             FROM {table}\n  \
             WHERE `_lsn` <= {max_lsn}\n    \
             AND (`blkno`, `offnum`) IN (\n      \
             SELECT `blkno`, `offnum` FROM {table}\n      \
             WHERE `chunk_id` = {value_id} AND `_lsn` <= {max_lsn})\n  \
             GROUP BY `blkno`, `offnum`\n\
             )\n\
             WHERE `chunk_id` = {value_id} AND `dead` = 0\n\
             GROUP BY `chunk_seq`\n\
             ORDER BY `chunk_seq`\n\
             SETTINGS max_block_size = {FETCH_BLOCK_ROWS}"
        )
    }

    async fn exec_write(
        &self,
        state: &mut ChState,
        sql: &str,
        bb: Option<&BlockBuilder<'_>>,
    ) -> Result<(), EmitterError> {
        let mut attempt = 0u32;
        let mut backoff = self.conn.retry.initial_backoff;
        loop {
            if state.client.is_none() {
                state.client = Some(connect_client(&self.conn).await?);
            }
            let res = {
                let client = state.client.as_mut().expect("just connected");
                with_timeout(self.conn.insert_timeout, async {
                    client.send_query(sql, None).await?;
                    if let Some(bb) = bb {
                        client.send_data(Some(bb)).await?;
                        client.send_data_end().await?;
                    }
                    drain_to_end_of_stream(client).await
                })
                .await
            };
            match res {
                Ok(()) => return Ok(()),
                Err(e) if is_retryable(&e) && attempt < self.conn.retry.max_attempts => {
                    attempt += 1;
                    state.client = None;
                    backoff_step(&mut backoff, self.conn.retry.max_backoff).await;
                }
                Err(e) => return Err(e),
            }
        }
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
            let mut data = Vec::new();
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
        let mut attempt = 0u32;
        let mut backoff = self.conn.retry.initial_backoff;
        loop {
            if state.client.is_none() {
                state.client = Some(connect_client(&self.conn).await?);
            }
            let res = {
                let client = state.client.as_mut().expect("just connected");
                with_timeout(self.conn.insert_timeout, async {
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
                    Ok::<_, EmitterError>(out)
                })
                .await
            };
            match res {
                Ok(out) => return Ok(out),
                Err(e @ EmitterError::ServerException { code, .. })
                    if code == CH_UNKNOWN_TABLE || code == CH_UNKNOWN_DATABASE =>
                {
                    return Err(e);
                }
                Err(e) if is_retryable(&e) && attempt < self.conn.retry.max_attempts => {
                    attempt += 1;
                    state.client = None;
                    backoff_step(&mut backoff, self.conn.retry.max_backoff).await;
                }
                Err(e) => return Err(e),
            }
        }
    }
}

/// Feed one result block into the assembler; seq order validated across
/// block boundaries (final `ORDER BY chunk_seq` is the contract)
fn read_chunk_block(block: &Block, asm: &mut ChunkAssembler) -> Result<(), EmitterError> {
    let n = block.n_rows();
    if n == 0 {
        return Ok(());
    }
    let seq_col = block
        .column(0)
        .ok_or_else(|| EmitterError::Type("toast fetch: missing chunk_seq column".into()))?;
    let data_col = block
        .column(1)
        .ok_or_else(|| EmitterError::Type("toast fetch: missing chunk_data column".into()))?;
    let (elem, seq_bytes) = seq_col
        .fixed()
        .ok_or_else(|| EmitterError::Type("toast fetch: chunk_seq not fixed-width".into()))?;
    if elem != 4 {
        return Err(EmitterError::Type(format!(
            "toast fetch: chunk_seq elem size {elem} != 4"
        )));
    }
    let (offsets, data) = data_col
        .string()
        .ok_or_else(|| EmitterError::Type("toast fetch: chunk_data not String".into()))?;
    for i in 0..n {
        let seq = u32::from_le_bytes(seq_bytes[i * 4..i * 4 + 4].try_into().unwrap());
        let start = if i == 0 { 0 } else { offsets[i - 1] as usize };
        let end = offsets[i] as usize;
        asm.push(seq, &data[start..end])
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
        let mut state = self.state.lock().await;
        self.put_locked(&mut state, rows)
            .await
            .map_err(|e| ChunkStoreError::Clickhouse(e.to_string()))
    }

    async fn fetch(
        &self,
        toast_relid: u32,
        value_id: u32,
        max_lsn: u64,
        expected_size: usize,
    ) -> Result<FetchedValue, ChunkStoreError> {
        let sql = self.fetch_sql(toast_relid, value_id, max_lsn);
        let mut state = self.state.lock().await;
        let asm: Option<ChunkAssembler> = self
            .query_locked(
                &mut state,
                &sql,
                |block, out: &mut Option<ChunkAssembler>| {
                    read_chunk_block(
                        block,
                        out.get_or_insert_with(|| ChunkAssembler::new(expected_size)),
                    )
                },
            )
            .await
            .map_err(|e| match e {
                EmitterError::ServerException { code, .. }
                    if code == CH_UNKNOWN_TABLE || code == CH_UNKNOWN_DATABASE =>
                {
                    ChunkStoreError::MissingMirror(toast_relid)
                }
                e => ChunkStoreError::Clickhouse(e.to_string()),
            })?;
        Ok(asm.map_or(FetchedValue::Missing, ChunkAssembler::finish))
    }

    async fn truncate_mirror(&self, toast_relid: u32) -> Result<(), ChunkStoreError> {
        let sql = self.truncate_sql(toast_relid);
        let mut state = self.state.lock().await;
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
        let mut state = self.state.lock().await;
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
    /// V3 hard per-value decode-target cap, checked before allocation
    inline_value_max: usize,
    /// Leaf permits for per-value transients (assembly, decompress, JIT
    /// materialization); `None` = unmetered (serial/metrics-only paths)
    budget: Option<crate::budget::MemoryBudget>,
}

impl ToastResolver {
    pub fn disabled() -> Self {
        Self {
            store: None,
            stats: Arc::new(EmitterStats::default()),
            inline_value_max: usize::MAX,
            budget: None,
        }
    }

    pub fn from_config(emitter: &EmitterConfig, stats: Arc<EmitterStats>) -> Self {
        let store: Option<Arc<dyn ChunkStore>> = match emitter.toast.mode {
            ToastMode::Disabled => None,
            ToastMode::ClickHouse => Some(Arc::new(ClickHouseChunkStore::new(emitter.clone()))),
        };
        Self {
            store,
            stats,
            inline_value_max: emitter.inline_value_max,
            budget: None,
        }
    }

    /// Store-backed resolver for tests
    pub fn with_store(store: Arc<dyn ChunkStore>, stats: Arc<EmitterStats>) -> Self {
        Self {
            store: Some(store),
            stats,
            inline_value_max: usize::MAX,
            budget: None,
        }
    }

    /// Leaf-permit pool, attached at pipeline spawn
    pub fn with_budget(mut self, budget: crate::budget::MemoryBudget) -> Self {
        self.budget = Some(budget);
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

    pub fn budget(&self) -> Option<&crate::budget::MemoryBudget> {
        self.budget.as_ref()
    }

    pub fn stores_chunks(&self) -> bool {
        self.store.is_some()
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
        let Some(store) = &self.store else {
            return Ok(None);
        };
        let v = store
            .fetch(toast_relid, value_id, max_lsn, expected_size)
            .await?;
        if matches!(v, FetchedValue::Assembled(_)) {
            self.stats
                .toast_values_fetched
                .fetch_add(1, Ordering::Relaxed);
        }
        Ok(Some(v))
    }

    /// Persist births and tombstones, no-op without store
    pub async fn put(&self, rows: &[ToastRow]) -> Result<(), ChunkStoreError> {
        let Some(store) = &self.store else {
            return Ok(());
        };
        if rows.is_empty() {
            return Ok(());
        }
        store.put(rows).await?;
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
            while end < rows.len() && end - start < CHUNK_PUT_BATCH && bytes < CHUNK_PUT_BYTES {
                bytes += rows[end].chunk_data.len();
                end += 1;
            }
            let _leaf = match &self.budget {
                Some(b) => Some(b.acquire(bytes).await),
                None => None,
            };
            let mut batch: Vec<ToastRow> = Vec::with_capacity(end - start);
            for r in &rows[start..end] {
                batch.push(r.materialize(spool)?);
            }
            self.put(&batch).await?;
            start = end;
        }
        Ok(())
    }

    /// [`Self::put`] in WAL-order slices sealed at [`CHUNK_PUT_BATCH`] rows
    /// or [`CHUNK_PUT_BYTES`], bounding one ClickHouse block build
    pub async fn put_batched(&self, rows: &[ToastRow]) -> Result<(), ChunkStoreError> {
        let mut start = 0usize;
        let mut bytes = 0usize;
        for (i, r) in rows.iter().enumerate() {
            if i > start && (i - start >= CHUNK_PUT_BATCH || bytes >= CHUNK_PUT_BYTES) {
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
        let Some(store) = &self.store else {
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
        let Some(store) = &self.store else {
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
            let body = vec![b'a' + seq as u8; 4];
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
            assembled(&vec![0u8; 2 << 10])
        );
        assert_eq!(budget.overshoots_total(), 1);
        assert_eq!(budget.resident_bytes(), 0, "nothing leaked");
    }

    #[test]
    fn toast_mode_parse_rejects_disk() {
        assert!(ToastMode::parse("disk").is_err());
        assert!(ToastMode::parse("local").is_err());
        assert_eq!(ToastMode::parse("ch").unwrap(), ToastMode::ClickHouse);
        assert_eq!(ToastMode::parse("").unwrap(), ToastMode::Disabled);
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

    #[tokio::test]
    async fn put_batched_preserves_order_across_slices() {
        let store = Arc::new(MemChunkStore::new());
        let stats = Arc::new(EmitterStats::default());
        let r = ToastResolver::with_store(store.clone(), stats);
        let rows: Vec<ToastRow> = (0..CHUNK_PUT_BATCH as u32 + 3)
            .map(|i| row(7, i, (1, 1 + i as u16), 0x1000 + u64::from(i), b"aa"))
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
             INDEX `idx_chunk_id` `chunk_id` TYPE bloom_filter GRANULARITY 1\n\
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
             FROM (\n  \
             SELECT `blkno`, `offnum`, argMax(`_is_deleted`, `_lsn`) AS `dead`\n  \
             FROM `wh`.`pg_toast_16500`\n  \
             WHERE `_lsn` <= 8192\n  \
             GROUP BY `blkno`, `offnum`\n\
             )\n\
             WHERE `dead` = 0\n  \
             AND (`blkno`, `offnum`) NOT IN (\n    \
             SELECT DISTINCT `blkno`, `offnum` FROM `wh`.`pg_toast_16500` WHERE `_lsn` > 8192)"
        );

        assert_eq!(
            store.fetch_sql(16500, 7, 0x2000),
            "SELECT `chunk_seq`, argMax(`chunk_data`, `ver`) AS `chunk_data`\n\
             FROM (\n  \
             SELECT argMax(`chunk_id`, `_lsn`) AS `chunk_id`,\n         \
             argMax(`chunk_seq`, `_lsn`) AS `chunk_seq`,\n         \
             argMax(`chunk_data`, `_lsn`) AS `chunk_data`,\n         \
             max(`_lsn`) AS `ver`,\n         \
             argMax(`_is_deleted`, `_lsn`) AS `dead`\n  \
             FROM `wh`.`pg_toast_16500`\n  \
             WHERE `_lsn` <= 8192\n    \
             AND (`blkno`, `offnum`) IN (\n      \
             SELECT `blkno`, `offnum` FROM `wh`.`pg_toast_16500`\n      \
             WHERE `chunk_id` = 7 AND `_lsn` <= 8192)\n  \
             GROUP BY `blkno`, `offnum`\n\
             )\n\
             WHERE `chunk_id` = 7 AND `dead` = 0\n\
             GROUP BY `chunk_seq`\n\
             ORDER BY `chunk_seq`\n\
             SETTINGS max_block_size = 1024"
        );
    }

    #[test]
    fn ch_mode_builds_a_store() {
        let cfg = EmitterConfig {
            toast: ToastConfig {
                mode: ToastMode::ClickHouse,
            },
            ..Default::default()
        };
        let r = ToastResolver::from_config(&cfg, Arc::new(EmitterStats::default()));
        assert!(r.stores_chunks());
        assert!(!r.fill_on_miss());
    }
}
