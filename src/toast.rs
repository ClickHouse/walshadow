//! Config-driven TOAST handling: where externally-stored `pg_toast` chunks
//! live and what happens when a value can't be rebuilt.
//!
//! Detoast within a single xact stays inline (the WAL fast path in
//! [`crate::xact_buffer`]); this module is the durable backstop for values
//! whose chunks fall outside the in-xact buffer (bootstrap baseline,
//! pre-window re-emits). Three modes:
//!
//! * [`ToastMode::Disabled`] — no chunk store. Same-xact values still
//!   reassemble inline (free, no data loss); a value needing a store is
//!   NULL/default-filled at the emitter and counted, never an error.
//! * [`ToastMode::Disk`] — chunks persisted to a local [`DiskChunkStore`],
//!   read back on a miss. Dir is persistent (NOT the wiped `--spill-dir`).
//! * [`ToastMode::ClickHouse`] — chunks mirrored to CH `pg_toast_<relid>`
//!   tables via [`ClickHouseChunkStore`]. `put` writes chunk rows, `fetch`
//!   is a CH `SELECT`. Same control flow as `disk` (the store is the only
//!   swap, see `plans/future/TOAST.md`).
//!
//! Chunk identity is `(toast_relid, value_id)` then `chunk_seq`, matching the
//! WAL key and PG's on-disk model.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use async_trait::async_trait;
use clickhouse_c::{Allocator, AsyncClient, Block, BlockBuilder, Event, TypeAst};
use thiserror::Error;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::Mutex;

use crate::ch_emitter::{
    EmitterConfig, EmitterError, EmitterStats, RetryConfig, connect_client, drain_to_end_of_stream,
    is_retryable, quote_ident,
};
use crate::spill::ToastChunk;

/// `(toast_relid, value_id) -> chunk_seq -> bytes`. Same shape as the WAL
/// in-xact chunk map ([`crate::pipeline::decode::ToastChunks`]).
pub type ChunkMap = HashMap<(u32, u32), BTreeMap<u32, Vec<u8>>>;

#[derive(Debug, Error)]
pub enum ChunkStoreError {
    #[error("toast store io: {0}")]
    Io(#[from] std::io::Error),
    #[error("toast store format: {0}")]
    Format(String),
    #[error("toast store clickhouse: {0}")]
    Clickhouse(String),
}

/// How TOAST chunks are stored and recovered. Parsed from `[toast] mode`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ToastMode {
    /// No store; unrecoverable toasted values NULL/default-filled.
    #[default]
    Disabled,
    /// Local persistent chunk store ([`DiskChunkStore`]).
    Disk,
    /// Mirror chunks to ClickHouse `pg_toast_<relid>` tables.
    ClickHouse,
}

impl ToastMode {
    pub fn parse(s: &str) -> Result<Self, String> {
        Ok(match s.trim().to_ascii_lowercase().as_str() {
            "disabled" | "off" | "none" | "" => Self::Disabled,
            "disk" | "local" => Self::Disk,
            "clickhouse" | "ch" => Self::ClickHouse,
            other => {
                return Err(format!(
                    "unknown toast mode `{other}` (expected disabled / disk / clickhouse)"
                ));
            }
        })
    }
}

/// `[toast]` TOML block.
#[derive(Debug, Clone, Default)]
pub struct ToastConfig {
    pub mode: ToastMode,
    /// Required when `mode = disk`. Persistent across restarts; must not be
    /// the wiped `--spill-dir`.
    pub disk_dir: Option<PathBuf>,
}

/// Durable chunk store: persist chunks, read them back by value.
#[async_trait]
pub trait ChunkStore: Send + Sync {
    /// Persist chunks. Idempotent: a value's chunks are immutable in PG
    /// (written once under a fresh `va_valueid`), so a re-`put` of the same
    /// `(relid, value_id, seq)` is byte-identical.
    async fn put(&self, chunks: &[ToastChunk]) -> Result<(), ChunkStoreError>;
    /// All chunks for one value, by `chunk_seq`. Empty map = no such value.
    async fn fetch(
        &self,
        toast_relid: u32,
        value_id: u32,
    ) -> Result<BTreeMap<u32, Vec<u8>>, ChunkStoreError>;
}

/// On-disk chunk store. One file per value at
/// `<dir>/<toast_relid>/<value_id>.chunks`, a sequence of framed records
/// `[seq u32 LE][len u32 LE][body]`. Appends are idempotent (fetch dedups by
/// seq, last wins); a torn trailing record from a crash mid-append is
/// tolerated on read (truncated tail ignored) and re-appended on the next
/// bootstrap walk.
pub struct DiskChunkStore {
    dir: PathBuf,
}

impl DiskChunkStore {
    /// Create the root dir. Synchronous: called once at daemon start.
    pub fn new(dir: PathBuf) -> Result<Self, ChunkStoreError> {
        std::fs::create_dir_all(&dir)?;
        Ok(Self { dir })
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    fn value_path(&self, toast_relid: u32, value_id: u32) -> PathBuf {
        self.dir
            .join(toast_relid.to_string())
            .join(format!("{value_id}.chunks"))
    }
}

/// Record header: `seq` then `len`, both u32 LE.
const REC_HEADER: usize = 8;

#[async_trait]
impl ChunkStore for DiskChunkStore {
    async fn put(&self, chunks: &[ToastChunk]) -> Result<(), ChunkStoreError> {
        if chunks.is_empty() {
            return Ok(());
        }
        // Group by value so one file open covers all its incoming chunks.
        let mut by_value: HashMap<(u32, u32), Vec<&ToastChunk>> = HashMap::new();
        for c in chunks {
            by_value
                .entry((c.toast_relid, c.value_id))
                .or_default()
                .push(c);
        }
        for ((relid, value_id), group) in by_value {
            let path = self.value_path(relid, value_id);
            if let Some(parent) = path.parent() {
                tokio::fs::create_dir_all(parent).await?;
            }
            let mut buf = Vec::new();
            for c in group {
                let len = u32::try_from(c.chunk_data.len()).map_err(|_| {
                    ChunkStoreError::Format(format!(
                        "chunk relid={relid} value={value_id} seq={} too large",
                        c.chunk_seq
                    ))
                })?;
                buf.extend_from_slice(&c.chunk_seq.to_le_bytes());
                buf.extend_from_slice(&len.to_le_bytes());
                buf.extend_from_slice(&c.chunk_data);
            }
            let mut file = OpenOptions::new()
                .create(true)
                .append(true)
                .open(&path)
                .await?;
            file.write_all(&buf).await?;
            file.sync_all().await?;
        }
        Ok(())
    }

    async fn fetch(
        &self,
        toast_relid: u32,
        value_id: u32,
    ) -> Result<BTreeMap<u32, Vec<u8>>, ChunkStoreError> {
        let path = self.value_path(toast_relid, value_id);
        let mut file = match File::open(&path).await {
            Ok(f) => f,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(BTreeMap::new()),
            Err(e) => return Err(e.into()),
        };
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes).await?;
        let mut out: BTreeMap<u32, Vec<u8>> = BTreeMap::new();
        let mut off = 0usize;
        while off + REC_HEADER <= bytes.len() {
            let seq = u32::from_le_bytes(bytes[off..off + 4].try_into().unwrap());
            let len = u32::from_le_bytes(bytes[off + 4..off + 8].try_into().unwrap()) as usize;
            let body_start = off + REC_HEADER;
            let body_end = body_start + len;
            if body_end > bytes.len() {
                // Torn trailing record from a crash mid-append: stop at the
                // last clean boundary. Earlier records remain valid.
                break;
            }
            out.insert(seq, bytes[body_start..body_end].to_vec());
            off = body_end;
        }
        Ok(out)
    }
}

/// CH server error codes treated as "no chunks for this relid" rather than a
/// hard error: a `fetch` against a toast relation that never received a chunk
/// finds no such table/database, mirroring the disk store's missing-file path.
const CH_UNKNOWN_TABLE: i32 = 60;
const CH_UNKNOWN_DATABASE: i32 = 81;

/// Connection + per-process table-creation cache, behind one mutex. A single
/// client suffices: toast ops are off the hot path (bootstrap baseline +
/// pre-window re-emits), so serializing them on one socket is fine.
struct ChState {
    /// Lazily connected, dropped to `None` to force a reconnect after a
    /// retryable fault.
    client: Option<AsyncClient>,
    /// Toast relids whose `CREATE TABLE IF NOT EXISTS` already ran this
    /// process; avoids a redundant round-trip per `put`. Server-side tables
    /// are durable, so a stale-after-restart entry just re-runs the idempotent
    /// CREATE.
    created: HashSet<u32>,
}

/// Mirror `pg_toast_<relid>` relations to ClickHouse, one CH table per toast
/// relation, chunks as rows. Backs [`ToastMode::ClickHouse`].
///
/// Schema (minimal mirror — under R2 in `plans/future/TOAST.md` the table is a
/// chunk *source* for the reassembler, not a query-time JOIN target, so it
/// stores only what the reassembler needs plus the `_lsn` convergence key; the
/// [`ChunkStore`] layer carries no `_xid`/`_commit_ts`/`_op`, so those doc
/// columns are omitted):
///
/// ```text
/// CREATE TABLE `<db>`.`pg_toast_<relid>` (
///   `chunk_id`   UInt32,   -- PG oid == va_valueid
///   `chunk_seq`  UInt32,   -- 0-based, dense
///   `chunk_data` String,   -- raw bytea body, compressed as PG wrote it
///   `_lsn`       UInt64    -- ReplacingMergeTree dedup / convergence key
/// ) ENGINE = ReplacingMergeTree(`_lsn`)
/// ORDER BY (`chunk_id`, `chunk_seq`)
/// ```
///
/// `ORDER BY (chunk_id, chunk_seq)` keeps a value's chunks contiguous so a
/// range read rebuilds it in order. Re-shipped chunks (bootstrap baseline then
/// WAL re-emit) collapse on `_lsn`; PG chunk rows are immutable per
/// `va_valueid`, so duplicates are byte-identical and `fetch`'s last-write-wins
/// `BTreeMap` is correct without `FINAL`.
pub struct ClickHouseChunkStore {
    /// Connection params (host/port/tls/compression/auth) reused from the main
    /// emitter; toast tables land in `conn.database`.
    conn: EmitterConfig,
    database: String,
    /// For [`BlockBuilder`] + [`TypeAst`] on the `put` path. `Copy`, `Sync`.
    alloc: Allocator,
    state: Mutex<ChState>,
    retry: RetryConfig,
    query_timeout: Duration,
}

impl ClickHouseChunkStore {
    /// Lazy: stores params, connects on first `put`/`fetch`. The main inserter
    /// pool eagerly validates the same `EmitterConfig`, so a bad endpoint fails
    /// there first.
    pub fn new(conn: EmitterConfig) -> Self {
        let database = conn.database.clone();
        let retry = conn.retry.clone();
        let query_timeout = conn.insert_timeout;
        Self {
            conn,
            database,
            alloc: Allocator::stdlib(),
            state: Mutex::new(ChState {
                client: None,
                created: HashSet::new(),
            }),
            retry,
            query_timeout,
        }
    }

    fn toast_table(&self, toast_relid: u32) -> String {
        format!(
            "{}.{}",
            quote_ident(&self.database),
            quote_ident(&format!("pg_toast_{toast_relid}"))
        )
    }

    fn create_sql(&self, toast_relid: u32) -> String {
        format!(
            "CREATE TABLE IF NOT EXISTS {} (\n  \
             `chunk_id` UInt32,\n  `chunk_seq` UInt32,\n  `chunk_data` String,\n  `_lsn` UInt64\n\
             ) ENGINE = ReplacingMergeTree(`_lsn`)\nORDER BY (`chunk_id`, `chunk_seq`)",
            self.toast_table(toast_relid)
        )
    }

    fn insert_sql(&self, toast_relid: u32) -> String {
        format!(
            "INSERT INTO {} (`chunk_id`, `chunk_seq`, `chunk_data`, `_lsn`) FORMAT Native",
            self.toast_table(toast_relid)
        )
    }

    /// One `send_query` (+ optional Native data block) drained to EndOfStream,
    /// with the inserter pool's bounded reconnect/retry + per-attempt timeout
    /// so a half-open socket can't pin the caller. `bb = None` is a DDL.
    async fn exec_write(
        &self,
        state: &mut ChState,
        sql: &str,
        bb: Option<&BlockBuilder<'_>>,
    ) -> Result<(), EmitterError> {
        let mut attempt = 0u32;
        let mut backoff = self.retry.initial_backoff;
        loop {
            if state.client.is_none() {
                state.client = Some(connect_client(&self.conn).await?);
            }
            let res = {
                let client = state.client.as_mut().expect("just connected");
                match tokio::time::timeout(self.query_timeout, async {
                    client.send_query(sql, None).await?;
                    if let Some(bb) = bb {
                        client.send_data(Some(bb)).await?;
                        client.send_data_end().await?;
                    }
                    drain_to_end_of_stream(client).await
                })
                .await
                {
                    Ok(r) => r,
                    Err(_elapsed) => Err(EmitterError::Timeout {
                        secs: self.query_timeout.as_secs(),
                    }),
                }
            };
            match res {
                Ok(()) => return Ok(()),
                Err(e) if is_retryable(&e) && attempt < self.retry.max_attempts => {
                    attempt += 1;
                    state.client = None;
                    tokio::time::sleep(backoff).await;
                    backoff = backoff.saturating_mul(2).min(self.retry.max_backoff);
                }
                Err(e) => return Err(e),
            }
        }
    }

    async fn put_locked(
        &self,
        state: &mut ChState,
        chunks: &[ToastChunk],
    ) -> Result<(), EmitterError> {
        // A single put may span toast relids (bootstrap batches across files);
        // each relid is its own CH table.
        let mut by_relid: HashMap<u32, Vec<&ToastChunk>> = HashMap::new();
        for c in chunks {
            by_relid.entry(c.toast_relid).or_default().push(c);
        }
        for (relid, group) in by_relid {
            if !state.created.contains(&relid) {
                let create = self.create_sql(relid);
                self.exec_write(state, &create, None).await?;
                state.created.insert(relid);
            }
            let n = group.len();
            let mut chunk_id = Vec::with_capacity(n * 4);
            let mut chunk_seq = Vec::with_capacity(n * 4);
            let mut lsn = Vec::with_capacity(n * 8);
            let mut offsets = Vec::with_capacity(n);
            let mut data = Vec::new();
            for c in &group {
                chunk_id.extend_from_slice(&c.value_id.to_le_bytes());
                chunk_seq.extend_from_slice(&c.chunk_seq.to_le_bytes());
                lsn.extend_from_slice(&c.source_lsn.to_le_bytes());
                data.extend_from_slice(&c.chunk_data);
                offsets.push(data.len() as u64);
            }
            let u32_ast = TypeAst::parse("UInt32", self.alloc)?;
            let u64_ast = TypeAst::parse("UInt64", self.alloc)?;
            let mut bb = BlockBuilder::new(self.alloc)?;
            bb.append_fixed("chunk_id", u32_ast.view(), &chunk_id, n)?;
            bb.append_fixed("chunk_seq", u32_ast.view(), &chunk_seq, n)?;
            bb.append_string("chunk_data", &offsets, &data, n)?;
            bb.append_fixed("_lsn", u64_ast.view(), &lsn, n)?;
            let insert = self.insert_sql(relid);
            self.exec_write(state, &insert, Some(&bb)).await?;
        }
        Ok(())
    }

    async fn fetch_locked(
        &self,
        state: &mut ChState,
        toast_relid: u32,
        value_id: u32,
    ) -> Result<BTreeMap<u32, Vec<u8>>, EmitterError> {
        let sql = format!(
            "SELECT `chunk_seq`, `chunk_data` FROM {} WHERE `chunk_id` = {} ORDER BY `chunk_seq`",
            self.toast_table(toast_relid),
            value_id
        );
        let mut attempt = 0u32;
        let mut backoff = self.retry.initial_backoff;
        loop {
            if state.client.is_none() {
                state.client = Some(connect_client(&self.conn).await?);
            }
            let res = {
                let client = state.client.as_mut().expect("just connected");
                match tokio::time::timeout(self.query_timeout, async {
                    client.send_query(&sql, None).await?;
                    let mut out: BTreeMap<u32, Vec<u8>> = BTreeMap::new();
                    loop {
                        match client.recv_event().await? {
                            // Native streams the result one block at a time, so
                            // a large value pages in naturally — never one
                            // unbounded buffered read.
                            Event::Data(block) => read_chunk_block(&block, &mut out)?,
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
                {
                    Ok(r) => r,
                    Err(_elapsed) => Err(EmitterError::Timeout {
                        secs: self.query_timeout.as_secs(),
                    }),
                }
            };
            match res {
                Ok(out) => return Ok(out),
                // No table/db => no chunks ever stored for this relid; surface
                // an empty map (caller decides fill vs. error), not a fault.
                Err(EmitterError::ServerException { code, .. })
                    if code == CH_UNKNOWN_TABLE || code == CH_UNKNOWN_DATABASE =>
                {
                    return Ok(BTreeMap::new());
                }
                Err(e) if is_retryable(&e) && attempt < self.retry.max_attempts => {
                    attempt += 1;
                    state.client = None;
                    tokio::time::sleep(backoff).await;
                    backoff = backoff.saturating_mul(2).min(self.retry.max_backoff);
                }
                Err(e) => return Err(e),
            }
        }
    }
}

/// Append one fetched Data block's `(chunk_seq UInt32, chunk_data String)`
/// rows into `out`. Column order matches the `SELECT`. The header block
/// (0 rows) contributes nothing.
fn read_chunk_block(block: &Block, out: &mut BTreeMap<u32, Vec<u8>>) -> Result<(), EmitterError> {
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
        out.insert(seq, data[start..end].to_vec());
    }
    Ok(())
}

#[async_trait]
impl ChunkStore for ClickHouseChunkStore {
    async fn put(&self, chunks: &[ToastChunk]) -> Result<(), ChunkStoreError> {
        if chunks.is_empty() {
            return Ok(());
        }
        let mut state = self.state.lock().await;
        self.put_locked(&mut state, chunks)
            .await
            .map_err(|e| ChunkStoreError::Clickhouse(e.to_string()))
    }

    async fn fetch(
        &self,
        toast_relid: u32,
        value_id: u32,
    ) -> Result<BTreeMap<u32, Vec<u8>>, ChunkStoreError> {
        let mut state = self.state.lock().await;
        self.fetch_locked(&mut state, toast_relid, value_id)
            .await
            .map_err(|e| ChunkStoreError::Clickhouse(e.to_string()))
    }
}

/// Resolution policy + optional store, threaded into the detoast paths and
/// the bootstrap drain. Cheap to clone (everything behind `Arc`).
#[derive(Clone)]
pub struct ToastResolver {
    mode: ToastMode,
    store: Option<Arc<dyn ChunkStore>>,
    stats: Arc<EmitterStats>,
}

impl ToastResolver {
    /// No store; misses NULL/default-fill. Used for the metrics-only serial
    /// drain (no `--ch-config`) and as the default.
    pub fn disabled() -> Self {
        Self {
            mode: ToastMode::Disabled,
            store: None,
            stats: Arc::new(EmitterStats::default()),
        }
    }

    /// Build from the emitter config (CH mode reuses its connection params),
    /// sharing the emitter's live counters. Reads `emitter.toast` for mode +
    /// disk dir.
    pub fn from_config(emitter: &EmitterConfig, stats: Arc<EmitterStats>) -> Result<Self, String> {
        let cfg = &emitter.toast;
        match cfg.mode {
            ToastMode::Disabled => Ok(Self {
                mode: ToastMode::Disabled,
                store: None,
                stats,
            }),
            ToastMode::Disk => {
                let dir = cfg
                    .disk_dir
                    .clone()
                    .ok_or("toast mode=disk requires [toast] disk_dir")?;
                let store =
                    DiskChunkStore::new(dir).map_err(|e| format!("toast disk store: {e}"))?;
                Ok(Self {
                    mode: ToastMode::Disk,
                    store: Some(Arc::new(store)),
                    stats,
                })
            }
            ToastMode::ClickHouse => {
                let store = ClickHouseChunkStore::new(emitter.clone());
                Ok(Self {
                    mode: ToastMode::ClickHouse,
                    store: Some(Arc::new(store)),
                    stats,
                })
            }
        }
    }

    #[cfg(test)]
    pub fn with_store(store: Arc<dyn ChunkStore>, stats: Arc<EmitterStats>) -> Self {
        Self {
            mode: ToastMode::Disk,
            store: Some(store),
            stats,
        }
    }

    pub fn mode(&self) -> ToastMode {
        self.mode
    }

    /// Whether chunks should be persisted (a store exists). False for
    /// disabled mode.
    pub fn stores_chunks(&self) -> bool {
        self.store.is_some()
    }

    /// Whether an unresolved pointer should NULL/default-fill rather than
    /// error. True only for disabled mode; disk/CH treat a miss as a real
    /// gap (opt-in durable storage means a miss is data loss, surfaced loud).
    pub fn fill_on_miss(&self) -> bool {
        matches!(self.mode, ToastMode::Disabled)
    }

    /// Fetch a value's chunks from the store into `into`, returning whether
    /// any were found. No-op (false) when there is no store.
    pub async fn fetch_into(
        &self,
        toast_relid: u32,
        value_id: u32,
        into: &mut ChunkMap,
    ) -> Result<bool, ChunkStoreError> {
        let Some(store) = &self.store else {
            return Ok(false);
        };
        let chunks = store.fetch(toast_relid, value_id).await?;
        if chunks.is_empty() {
            return Ok(false);
        }
        self.stats
            .toast_values_fetched
            .fetch_add(1, Ordering::Relaxed);
        into.insert((toast_relid, value_id), chunks);
        Ok(true)
    }

    /// Persist chunk rows. No-op when there is no store.
    pub async fn put(&self, chunks: &[ToastChunk]) -> Result<(), ChunkStoreError> {
        let Some(store) = &self.store else {
            return Ok(());
        };
        if chunks.is_empty() {
            return Ok(());
        }
        store.put(chunks).await?;
        self.stats
            .toast_chunks_stored
            .fetch_add(chunks.len() as u64, Ordering::Relaxed);
        Ok(())
    }

    /// Persist an in-xact chunk map, stamping `source_lsn` (per-chunk LSN is
    /// dropped at merge; `commit_lsn` is the convergence `_lsn`). No-op when
    /// there is no store or the map is empty.
    pub async fn put_map(&self, map: &ChunkMap, source_lsn: u64) -> Result<(), ChunkStoreError> {
        if self.store.is_none() || map.is_empty() {
            return Ok(());
        }
        let mut rows = Vec::new();
        for ((relid, value_id), seqs) in map {
            for (seq, body) in seqs {
                rows.push(ToastChunk {
                    toast_relid: *relid,
                    value_id: *value_id,
                    chunk_seq: *seq,
                    source_lsn,
                    chunk_data: body.clone(),
                });
            }
        }
        self.put(&rows).await
    }

    /// Count one value that was NULL/default-filled because it couldn't be
    /// rebuilt (disabled mode only).
    pub fn note_filled_default(&self) {
        self.stats
            .toast_values_filled_default
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Count one value whose chunks were genuinely absent from an active
    /// store (disk/CH gap).
    pub fn note_fetch_miss(&self) {
        self.stats.toast_fetch_miss.fetch_add(1, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn chunk(relid: u32, value_id: u32, seq: u32, body: &[u8]) -> ToastChunk {
        ToastChunk {
            toast_relid: relid,
            value_id,
            chunk_seq: seq,
            source_lsn: 0x1000,
            chunk_data: body.to_vec(),
        }
    }

    #[tokio::test]
    async fn disk_store_roundtrip_across_puts() {
        let tmp = tempfile::tempdir().unwrap();
        let store = DiskChunkStore::new(tmp.path().to_path_buf()).unwrap();
        // Two puts for the same value (chunks split across pages/commits).
        store
            .put(&[chunk(16500, 7, 0, b"abc"), chunk(16500, 7, 1, b"de")])
            .await
            .unwrap();
        store.put(&[chunk(16500, 7, 2, b"f")]).await.unwrap();
        // Unrelated value in the same relation.
        store.put(&[chunk(16500, 9, 0, b"zz")]).await.unwrap();

        let got = store.fetch(16500, 7).await.unwrap();
        assert_eq!(got.len(), 3);
        assert_eq!(got.get(&0).unwrap(), b"abc");
        assert_eq!(got.get(&1).unwrap(), b"de");
        assert_eq!(got.get(&2).unwrap(), b"f");

        let other = store.fetch(16500, 9).await.unwrap();
        assert_eq!(other.len(), 1);
        assert_eq!(other.get(&0).unwrap(), b"zz");

        // Absent value → empty, not error.
        assert!(store.fetch(16500, 999).await.unwrap().is_empty());
        assert!(store.fetch(404, 1).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn disk_store_tolerates_torn_trailing_record() {
        let tmp = tempfile::tempdir().unwrap();
        let store = DiskChunkStore::new(tmp.path().to_path_buf()).unwrap();
        store
            .put(&[chunk(1, 1, 0, b"good"), chunk(1, 1, 1, b"alsogood")])
            .await
            .unwrap();
        // Append a torn record (header claims 100 bytes, only 2 present).
        let path = store.value_path(1, 1);
        let mut f = OpenOptions::new().append(true).open(&path).await.unwrap();
        f.write_all(&2u32.to_le_bytes()).await.unwrap();
        f.write_all(&100u32.to_le_bytes()).await.unwrap();
        f.write_all(b"xx").await.unwrap();
        f.sync_all().await.unwrap();
        drop(f);

        let got = store.fetch(1, 1).await.unwrap();
        assert_eq!(got.len(), 2, "torn tail ignored, clean prefix kept");
        assert_eq!(got.get(&0).unwrap(), b"good");
        assert_eq!(got.get(&1).unwrap(), b"alsogood");
    }

    #[tokio::test]
    async fn resolver_disabled_fills_on_miss_no_store() {
        let r = ToastResolver::disabled();
        assert!(r.fill_on_miss());
        assert!(!r.stores_chunks());
        let mut map = ChunkMap::new();
        assert!(!r.fetch_into(1, 2, &mut map).await.unwrap());
        assert!(map.is_empty());
        // put is a no-op
        r.put(&[chunk(1, 2, 0, b"x")]).await.unwrap();
    }

    #[tokio::test]
    async fn resolver_disk_stores_and_fetches() {
        let tmp = tempfile::tempdir().unwrap();
        let stats = Arc::new(EmitterStats::default());
        let store = Arc::new(DiskChunkStore::new(tmp.path().to_path_buf()).unwrap());
        let r = ToastResolver::with_store(store, stats.clone());
        assert!(!r.fill_on_miss());
        assert!(r.stores_chunks());

        let mut src = ChunkMap::new();
        src.insert((16500, 7), BTreeMap::from([(0u32, b"hi".to_vec())]));
        r.put_map(&src, 0x2000).await.unwrap();
        assert_eq!(stats.toast_chunks_stored.load(Ordering::Relaxed), 1);

        let mut out = ChunkMap::new();
        assert!(r.fetch_into(16500, 7, &mut out).await.unwrap());
        assert_eq!(out.get(&(16500, 7)).unwrap().get(&0).unwrap(), b"hi");
        assert_eq!(stats.toast_values_fetched.load(Ordering::Relaxed), 1);

        let mut empty = ChunkMap::new();
        assert!(!r.fetch_into(16500, 404, &mut empty).await.unwrap());
    }

    #[test]
    fn ch_store_renders_toast_schema_and_sql() {
        let cfg = EmitterConfig {
            database: "wh".into(),
            ..Default::default()
        };
        let store = ClickHouseChunkStore::new(cfg);

        assert_eq!(store.toast_table(16500), "`wh`.`pg_toast_16500`");

        let create = store.create_sql(16500);
        assert!(create.starts_with("CREATE TABLE IF NOT EXISTS `wh`.`pg_toast_16500`"));
        assert!(create.contains("`chunk_id` UInt32"));
        assert!(create.contains("`chunk_seq` UInt32"));
        assert!(create.contains("`chunk_data` String"));
        assert!(create.contains("`_lsn` UInt64"));
        assert!(create.contains("ENGINE = ReplacingMergeTree(`_lsn`)"));
        assert!(create.ends_with("ORDER BY (`chunk_id`, `chunk_seq`)"));

        assert_eq!(
            store.insert_sql(16500),
            "INSERT INTO `wh`.`pg_toast_16500` \
             (`chunk_id`, `chunk_seq`, `chunk_data`, `_lsn`) FORMAT Native"
        );
    }

    #[test]
    fn ch_mode_builds_a_store() {
        let cfg = EmitterConfig {
            toast: ToastConfig {
                mode: ToastMode::ClickHouse,
                ..Default::default()
            },
            ..Default::default()
        };
        let r = ToastResolver::from_config(&cfg, Arc::new(EmitterStats::default())).unwrap();
        assert_eq!(r.mode(), ToastMode::ClickHouse);
        assert!(r.stores_chunks());
        // A miss in an active CH store is a real gap, never a silent fill.
        assert!(!r.fill_on_miss());
    }
}
