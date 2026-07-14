//! Durable backstop for TOAST chunks outside current transaction buffer
//!
//! ClickHouse mirrors line-pointer occupancy by heap TID, versioned by WAL
//! record LSN. `ReplacingMergeTree` reclaims tombstoned chunk bodies

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use clickhouse_c::{Allocator, AsyncClient, Block, BlockBuilder, Event, TypeAst};
use thiserror::Error;
use tokio::sync::Mutex;

use crate::ch_emitter::{
    EmitterConfig, EmitterError, EmitterStats, connect_client, drain_to_end_of_stream,
    is_retryable, quote_ident,
};
use crate::spill::{ToastChunk, ToastDelete};

/// `(toast_relid, value_id) -> chunk_seq -> bytes`
pub type ChunkMap = HashMap<(u32, u32), BTreeMap<u32, Vec<u8>>>;

#[derive(Debug, Error)]
pub enum ChunkStoreError {
    #[error("toast store clickhouse: {0}")]
    Clickhouse(String),
    /// Mirror absence does not prove supersession, never fill
    #[error("toast store: no mirror for toast relid {0}")]
    MissingMirror(u32),
}

/// `[toast] mode`
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ToastMode {
    /// NULL/default-fill values unavailable in current transaction
    #[default]
    Disabled,
    /// Mirror chunks to ClickHouse
    ClickHouse,
}

impl ToastMode {
    pub fn parse(s: &str) -> Result<Self, String> {
        Ok(match s.trim().to_ascii_lowercase().as_str() {
            "disabled" | "off" | "none" | "" => Self::Disabled,
            "clickhouse" | "ch" => Self::ClickHouse,
            other => {
                return Err(format!(
                    "unknown toast mode `{other}` (expected disabled / clickhouse)"
                ));
            }
        })
    }
}

/// `[toast]` configuration
#[derive(Debug, Clone, Default)]
pub struct ToastConfig {
    pub mode: ToastMode,
}

/// Chunk birth or TID tombstone, keyed by heap TID and record LSN
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToastRow {
    pub toast_relid: u32,
    pub blkno: u32,
    pub offnum: u16,
    /// `va_valueid`, InvalidOid marks tombstones
    pub chunk_id: u32,
    pub chunk_seq: u32,
    pub chunk_data: Vec<u8>,
    /// Record LSN orders same-commit birth and death at one TID
    pub lsn: u64,
}

impl ToastRow {
    pub fn from_chunk(c: &ToastChunk) -> Self {
        Self {
            toast_relid: c.toast_relid,
            blkno: c.blkno,
            offnum: c.offnum,
            chunk_id: c.value_id,
            chunk_seq: c.chunk_seq,
            chunk_data: c.chunk_data.clone(),
            lsn: c.source_lsn,
        }
    }

    pub fn tombstone(d: &ToastDelete) -> Self {
        Self {
            toast_relid: d.toast_relid,
            blkno: d.blkno,
            offnum: d.offnum,
            chunk_id: 0,
            chunk_seq: 0,
            chunk_data: Vec::new(),
            lsn: d.source_lsn,
        }
    }

    pub fn is_tombstone(&self) -> bool {
        self.chunk_id == 0
    }
}

/// Durable TID-keyed chunk store
#[async_trait]
pub trait ChunkStore: Send + Sync {
    /// Replay emits byte-identical rows at equal key and version
    async fn put(&self, rows: &[ToastRow]) -> Result<(), ChunkStoreError>;
    /// Return newest live row per sequence at `max_lsn`
    ///
    /// Return empty when no live row remains at bound,
    /// [`ChunkStoreError::MissingMirror`] when mirror is absent
    async fn fetch(
        &self,
        toast_relid: u32,
        value_id: u32,
        max_lsn: u64,
    ) -> Result<BTreeMap<u32, Vec<u8>>, ChunkStoreError>;
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
    ) -> Result<BTreeMap<u32, Vec<u8>>, ChunkStoreError> {
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
        Ok(newest
            .into_iter()
            .map(|(seq, (_, body))| (seq, body.to_vec()))
            .collect())
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
             ORDER BY `chunk_seq`"
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
                match tokio::time::timeout(self.conn.insert_timeout, async {
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
                        secs: self.conn.insert_timeout.as_secs(),
                    }),
                }
            };
            match res {
                Ok(()) => return Ok(()),
                Err(e) if is_retryable(&e) && attempt < self.conn.retry.max_attempts => {
                    attempt += 1;
                    state.client = None;
                    tokio::time::sleep(backoff).await;
                    backoff = backoff.saturating_mul(2).min(self.conn.retry.max_backoff);
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
            let mut bb = BlockBuilder::new(self.alloc)?;
            bb.append_fixed("blkno", u32_ast.view(), &blkno, n)?;
            bb.append_fixed("offnum", u16_ast.view(), &offnum, n)?;
            bb.append_fixed("chunk_id", u32_ast.view(), &chunk_id, n)?;
            bb.append_fixed("chunk_seq", u32_ast.view(), &chunk_seq, n)?;
            bb.append_string("chunk_data", &offsets, &data, n)?;
            bb.append_fixed("_lsn", u64_ast.view(), &lsn, n)?;
            bb.append_fixed("_is_deleted", u8_ast.view(), &is_deleted, n)?;
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
                match tokio::time::timeout(self.conn.insert_timeout, async {
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
                {
                    Ok(r) => r,
                    Err(_elapsed) => Err(EmitterError::Timeout {
                        secs: self.conn.insert_timeout.as_secs(),
                    }),
                }
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
                    tokio::time::sleep(backoff).await;
                    backoff = backoff.saturating_mul(2).min(self.conn.retry.max_backoff);
                }
                Err(e) => return Err(e),
            }
        }
    }
}

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
    ) -> Result<BTreeMap<u32, Vec<u8>>, ChunkStoreError> {
        let sql = self.fetch_sql(toast_relid, value_id, max_lsn);
        let mut state = self.state.lock().await;
        self.query_locked(&mut state, &sql, read_chunk_block)
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
}

impl ToastResolver {
    pub fn disabled() -> Self {
        Self {
            store: None,
            stats: Arc::new(EmitterStats::default()),
        }
    }

    pub fn from_config(emitter: &EmitterConfig, stats: Arc<EmitterStats>) -> Self {
        let store: Option<Arc<dyn ChunkStore>> = match emitter.toast.mode {
            ToastMode::Disabled => None,
            ToastMode::ClickHouse => Some(Arc::new(ClickHouseChunkStore::new(emitter.clone()))),
        };
        Self { store, stats }
    }

    /// Store-backed resolver for tests
    pub fn with_store(store: Arc<dyn ChunkStore>, stats: Arc<EmitterStats>) -> Self {
        Self {
            store: Some(store),
            stats,
        }
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

    /// Fetch chunks into resolution map, return false without rows or store
    pub async fn fetch_into(
        &self,
        toast_relid: u32,
        value_id: u32,
        max_lsn: u64,
        into: &mut ChunkMap,
    ) -> Result<bool, ChunkStoreError> {
        let Some(store) = &self.store else {
            return Ok(false);
        };
        let chunks = store.fetch(toast_relid, value_id, max_lsn).await?;
        if chunks.is_empty() {
            return Ok(false);
        }
        self.stats
            .toast_values_fetched
            .fetch_add(1, Ordering::Relaxed);
        into.insert((toast_relid, value_id), chunks);
        Ok(true)
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
            chunk_data: body.to_vec(),
            lsn,
        }
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
        let got = store.fetch(16500, 7, u64::MAX).await.unwrap();
        assert_eq!(got.len(), 2);
        assert_eq!(got.get(&0).unwrap(), b"abc");
        assert_eq!(got.get(&1).unwrap(), b"de");
        assert!(store.fetch(16500, 404, u64::MAX).await.unwrap().is_empty());
        assert!(matches!(
            store.fetch(404, 7, u64::MAX).await,
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
        let live = store.fetch(16500, 7, 0x1fff).await.unwrap();
        assert_eq!(live.get(&0).unwrap(), b"abc");
        assert!(store.fetch(16500, 7, 0x2000).await.unwrap().is_empty());
        assert!(store.fetch(16500, 7, u64::MAX).await.unwrap().is_empty());
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
        assert!(store.fetch(16500, 7, u64::MAX).await.unwrap().is_empty());
        let new = store.fetch(16500, 9, u64::MAX).await.unwrap();
        assert_eq!(new.get(&0).unwrap(), b"new");
        let old = store.fetch(16500, 7, 0x1fff).await.unwrap();
        assert_eq!(old.get(&0).unwrap(), b"old");
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
        let old = store.fetch(16500, 7, 0x1fff).await.unwrap();
        assert_eq!(old.len(), 2);
        assert_eq!(old.get(&0).unwrap(), b"g1-0");
        let new = store.fetch(16500, 7, u64::MAX).await.unwrap();
        assert_eq!(new.len(), 1, "dead generation's seq 1 dropped");
        assert_eq!(new.get(&0).unwrap(), b"g2-0");
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
        let got = store.fetch(16500, 7, u64::MAX).await.unwrap();
        assert_eq!(got.len(), 1);
        assert_eq!(got.get(&0).unwrap(), b"copy-b");
    }

    #[tokio::test]
    async fn mem_store_same_commit_birth_then_death() {
        let store = MemChunkStore::new();
        store
            .put(&[row(7, 0, (1, 1), 0x1000, b"x"), tomb((1, 1), 0x1001)])
            .await
            .unwrap();
        assert!(store.fetch(16500, 7, u64::MAX).await.unwrap().is_empty());
        assert_eq!(
            store
                .fetch(16500, 7, 0x1000)
                .await
                .unwrap()
                .get(&0)
                .unwrap(),
            b"x"
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
        assert!(store.fetch(16500, 7, u64::MAX).await.unwrap().is_empty());
        assert_eq!(store.fetch(200, 9, u64::MAX).await.unwrap().len(), 1);
        store.truncate_mirror(404).await.unwrap();
        assert!(matches!(
            store.fetch(404, 7, u64::MAX).await,
            Err(ChunkStoreError::MissingMirror(404))
        ));
        store
            .put(&[row(11, 0, (0, 1), 0x2000, b"new")])
            .await
            .unwrap();
        assert_eq!(
            store
                .fetch(16500, 11, u64::MAX)
                .await
                .unwrap()
                .get(&0)
                .unwrap(),
            b"new"
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
        let v7 = store.fetch(16500, 7, u64::MAX).await.unwrap();
        assert_eq!(v7.len(), 1);
        assert_eq!(v7.get(&0).unwrap(), b"a2");
        // Residual live TIDs (1,2) and (3,1) tombstoned; dead (2,1) untouched
        assert!(store.fetch(16500, 11, u64::MAX).await.unwrap().is_empty());
        // As-of before the rewrite still resolves the old generation whole
        let old = store.fetch(16500, 7, 0x1fff).await.unwrap();
        assert_eq!(old.len(), 2);
        // Re-run converges: prior residuals sit past the marker, no new rows
        let before = store.mirrors.lock().unwrap().get(&16500).unwrap().len();
        store.rewrite_barrier(16500, 0x2000, 0x4000).await.unwrap();
        let after = store.mirrors.lock().unwrap().get(&16500).unwrap().len();
        assert_eq!(before, after, "barrier re-run must insert nothing");
        // Empty generation: nothing past marker, every live TID dies
        store.rewrite_barrier(16500, 0x5000, 0x6000).await.unwrap();
        assert!(store.fetch(16500, 7, u64::MAX).await.unwrap().is_empty());
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
        let mut map = ChunkMap::new();
        assert!(!r.fetch_into(1, 2, u64::MAX, &mut map).await.unwrap());
        assert!(map.is_empty());
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

        let mut out = ChunkMap::new();
        assert!(r.fetch_into(16500, 7, u64::MAX, &mut out).await.unwrap());
        assert_eq!(out.get(&(16500, 7)).unwrap().get(&0).unwrap(), b"hi");
        assert_eq!(stats.toast_values_fetched.load(Ordering::Relaxed), 1);

        let mut empty = ChunkMap::new();
        assert!(
            !r.fetch_into(16500, 404, u64::MAX, &mut empty)
                .await
                .unwrap()
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
             ORDER BY `chunk_seq`"
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
