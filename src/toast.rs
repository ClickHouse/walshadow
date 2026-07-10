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
use crate::toast_tid::{TidEvent, TidTracker};

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
    /// `tid_journal`: TID death-tracking journal path
    /// ([`crate::toast_tid`]). Defaults to `<disk_dir>/tids.journal` in
    /// disk mode; required explicitly for `clickhouse` mode (local durable
    /// state). Absent ⇒ no tracking, GC has nothing to collect.
    pub tid_journal: Option<PathBuf>,
    /// `gc_interval_secs`: cadence of the dead-chunk sweep
    /// ([`crate::toast_gc`]). Zero (default) disables GC.
    pub gc_interval: Duration,
}

/// Durable chunk store: persist chunks, read them back by value.
#[async_trait]
pub trait ChunkStore: Send + Sync {
    /// Persist chunks. Idempotent: re-`put`s of a `(relid, value_id, seq)`
    /// under the same LSN are byte-identical (replay re-emit); a reused
    /// `va_valueid` re-puts under a higher LSN and `fetch`'s newest-
    /// generation selection keeps reads coherent.
    async fn put(&self, chunks: &[ToastChunk]) -> Result<(), ChunkStoreError>;
    /// Newest generation at or before `max_lsn`, by `chunk_seq`. Bound keeps
    /// lagging decode from reading a future reused-OID generation. Empty map
    /// means no eligible generation.
    async fn fetch(
        &self,
        toast_relid: u32,
        value_id: u32,
        max_lsn: u64,
    ) -> Result<BTreeMap<u32, Vec<u8>>, ChunkStoreError>;
    /// Delete dead generations: for each `(value_id, death_lsn)`, rows of
    /// that value with `lsn <= death_lsn`. A rebirth (reused OID) carries
    /// `lsn > death_lsn` and survives. Idempotent; returns values that had
    /// rows to delete.
    async fn gc_values(
        &self,
        toast_relid: u32,
        deaths: &[(u32, u64)],
    ) -> Result<u64, ChunkStoreError>;
}

/// Newest-generation fold shared by both stores: keep rows at the value's
/// max LSN, last-wins per seq within it. Order-independent over the input.
struct GenFold {
    max_allowed_lsn: u64,
    generation_lsn: Option<u64>,
    out: BTreeMap<u32, Vec<u8>>,
}

impl GenFold {
    fn at(max_allowed_lsn: u64) -> Self {
        Self {
            max_allowed_lsn,
            generation_lsn: None,
            out: BTreeMap::new(),
        }
    }

    fn add(&mut self, seq: u32, lsn: u64, body: Vec<u8>) {
        if lsn > self.max_allowed_lsn {
            return;
        }
        match self.generation_lsn {
            None => self.generation_lsn = Some(lsn),
            Some(current) if lsn > current => {
                self.out.clear();
                self.generation_lsn = Some(lsn);
            }
            Some(current) if lsn < current => return,
            Some(_) => {}
        }
        self.out.insert(seq, body);
    }
}

impl Default for GenFold {
    fn default() -> Self {
        Self::at(u64::MAX)
    }
}

/// On-disk chunk store. One file per value at
/// `<dir>/<toast_relid>/<value_id>.chunks`: an 8-byte header
/// (`WSTC` + version u16 + reserved u16) then framed records
/// `[seq u32 LE][len u32 LE][lsn u64 LE][body]`. Headerless files are the
/// pre-LSN v1 format (`[seq][len][body]`), read as `lsn = 0` and upgraded
/// in place on the next append. Appends are idempotent (fetch keeps the
/// max-LSN generation, last-wins per seq); a torn trailing record from a
/// crash mid-append is tolerated on read (truncated tail ignored) and
/// re-appended by replay from ack.
pub struct DiskChunkStore {
    dir: PathBuf,
    /// Serializes `put`'s open-append against `gc_values`' read-rewrite: an
    /// unlink/rename between them would strand the append on an orphaned
    /// inode (silent chunk loss).
    gc_lock: Mutex<()>,
}

impl DiskChunkStore {
    /// Create the root dir. Synchronous: called once at daemon start.
    pub fn new(dir: PathBuf) -> Result<Self, ChunkStoreError> {
        std::fs::create_dir_all(&dir)?;
        Ok(Self {
            dir,
            gc_lock: Mutex::new(()),
        })
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

/// v1 record header: `seq` then `len`, both u32 LE. No file header.
const REC_HEADER_V1: usize = 8;
/// v2 record header: `seq` u32, `len` u32, `lsn` u64.
const REC_HEADER_V2: usize = 16;
/// v2 file header: magic + version u16 + reserved u16.
const FILE_MAGIC: [u8; 4] = *b"WSTC";
const FILE_VERSION: u16 = 2;
const FILE_HEADER: usize = 8;

fn file_header() -> [u8; FILE_HEADER] {
    let mut h = [0u8; FILE_HEADER];
    h[..4].copy_from_slice(&FILE_MAGIC);
    h[4..6].copy_from_slice(&FILE_VERSION.to_le_bytes());
    h
}

/// A v1 file starts with its first record's `seq` (small LE int), never
/// the magic.
fn is_v2(bytes: &[u8]) -> bool {
    bytes.len() >= FILE_HEADER && bytes[..4] == FILE_MAGIC
}

fn push_frame(buf: &mut Vec<u8>, seq: u32, lsn: u64, body: &[u8]) {
    buf.extend_from_slice(&seq.to_le_bytes());
    buf.extend_from_slice(&(body.len() as u32).to_le_bytes());
    buf.extend_from_slice(&lsn.to_le_bytes());
    buf.extend_from_slice(body);
}

/// Parse frames of either format, torn tail ignored. `f` sees
/// `(seq, lsn, body)`; v1 frames carry `lsn = 0`.
fn walk_frames(bytes: &[u8], mut f: impl FnMut(u32, u64, &[u8])) {
    let (mut off, header) = if is_v2(bytes) {
        (FILE_HEADER, REC_HEADER_V2)
    } else {
        (0, REC_HEADER_V1)
    };
    while off + header <= bytes.len() {
        let seq = u32::from_le_bytes(bytes[off..off + 4].try_into().unwrap());
        let len = u32::from_le_bytes(bytes[off + 4..off + 8].try_into().unwrap()) as usize;
        let lsn = if header == REC_HEADER_V2 {
            u64::from_le_bytes(bytes[off + 8..off + 16].try_into().unwrap())
        } else {
            0
        };
        let body_start = off + header;
        let body_end = body_start + len;
        if body_end > bytes.len() {
            // Torn trailing record from a crash mid-append: stop at the
            // last clean boundary. Earlier records remain valid.
            break;
        }
        f(seq, lsn, &bytes[body_start..body_end]);
        off = body_end;
    }
}

impl DiskChunkStore {
    /// Read a value file, `None` when absent.
    async fn read_value(&self, path: &Path) -> Result<Option<Vec<u8>>, ChunkStoreError> {
        let mut file = match File::open(path).await {
            Ok(f) => f,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e.into()),
        };
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes).await?;
        Ok(Some(bytes))
    }

    /// Atomic whole-file replace, fsynced before rename.
    async fn rewrite(&self, path: &Path, contents: &[u8]) -> Result<(), ChunkStoreError> {
        let tmp = path.with_extension("chunks.tmp");
        let mut f = File::create(&tmp).await?;
        f.write_all(contents).await?;
        f.sync_all().await?;
        drop(f);
        tokio::fs::rename(&tmp, path).await?;
        Ok(())
    }
}

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
                u32::try_from(c.chunk_data.len()).map_err(|_| {
                    ChunkStoreError::Format(format!(
                        "chunk relid={relid} value={value_id} seq={} too large",
                        c.chunk_seq
                    ))
                })?;
                push_frame(&mut buf, c.chunk_seq, c.source_lsn, &c.chunk_data);
            }
            let _guard = self.gc_lock.lock().await;
            match self.read_value(&path).await? {
                // v1 file: upgrade in place (old frames as lsn 0), then the
                // new frames — one atomic rewrite
                Some(existing) if !is_v2(&existing) => {
                    let mut upgraded = file_header().to_vec();
                    walk_frames(&existing, |seq, lsn, body| {
                        push_frame(&mut upgraded, seq, lsn, body);
                    });
                    upgraded.extend_from_slice(&buf);
                    self.rewrite(&path, &upgraded).await?;
                }
                existing => {
                    let mut file = OpenOptions::new()
                        .create(true)
                        .append(true)
                        .open(&path)
                        .await?;
                    if existing.is_none() {
                        file.write_all(&file_header()).await?;
                    }
                    file.write_all(&buf).await?;
                    file.sync_all().await?;
                }
            }
        }
        Ok(())
    }

    async fn fetch(
        &self,
        toast_relid: u32,
        value_id: u32,
        max_lsn: u64,
    ) -> Result<BTreeMap<u32, Vec<u8>>, ChunkStoreError> {
        let path = self.value_path(toast_relid, value_id);
        let Some(bytes) = self.read_value(&path).await? else {
            return Ok(BTreeMap::new());
        };
        let mut fold = GenFold::at(max_lsn);
        walk_frames(&bytes, |seq, lsn, body| fold.add(seq, lsn, body.to_vec()));
        Ok(fold.out)
    }

    async fn gc_values(
        &self,
        toast_relid: u32,
        deaths: &[(u32, u64)],
    ) -> Result<u64, ChunkStoreError> {
        let mut deleted = 0u64;
        for &(value_id, death_lsn) in deaths {
            let path = self.value_path(toast_relid, value_id);
            let _guard = self.gc_lock.lock().await;
            let Some(bytes) = self.read_value(&path).await? else {
                continue;
            };
            // Keep frames past the death LSN (rebirth of a reused OID)
            let mut kept = file_header().to_vec();
            let mut dropped = false;
            walk_frames(&bytes, |seq, lsn, body| {
                if lsn > death_lsn {
                    push_frame(&mut kept, seq, lsn, body);
                } else {
                    dropped = true;
                }
            });
            if !dropped {
                continue;
            }
            if kept.len() == FILE_HEADER {
                tokio::fs::remove_file(&path).await?;
            } else {
                self.rewrite(&path, &kept).await?;
            }
            deleted += 1;
        }
        Ok(deleted)
    }
}

/// CH server error codes treated as "no chunks for this relid" rather than a
/// hard error: a `fetch` against a toast relation that never received a chunk
/// finds no such table/database, mirroring the disk store's missing-file path.
const CH_UNKNOWN_TABLE: i32 = 60;
const CH_UNKNOWN_DATABASE: i32 = 81;

/// Deaths per GC `DELETE` statement, bounding SQL size (each carries a
/// `(chunk_id, _lsn)` predicate).
const GC_DELETE_BATCH: usize = 1024;

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
/// Schema (minimal mirror — under R2 in `plans/TOAST.md` the table is a
/// chunk *source* for the reassembler, not a query-time JOIN target, so it
/// stores only what the reassembler needs plus the `_lsn` convergence key; the
/// [`ChunkStore`] layer carries no `_xid`/`_commit_ts`/`_is_deleted`, so those
/// doc columns are omitted):
///
/// ```text
/// CREATE TABLE `<db>`.`pg_toast_<relid>` (
///   `chunk_id`   UInt32,   -- PG oid == va_valueid
///   `chunk_seq`  UInt32,   -- 0-based, dense
///   `chunk_data` String,   -- raw bytea body, compressed as PG wrote it
///   `_lsn`       UInt64    -- ReplacingMergeTree dedup / convergence key
/// ) ENGINE = ReplacingMergeTree(`_lsn`)
/// ORDER BY (`chunk_id`, `_lsn`, `chunk_seq`)
/// ```
///
/// Generation LSN belongs to sorting key so merges retain older generations
/// until ack-gated GC. This lets lagging decode fetch newest generation no
/// later than referring WAL record. Re-ships of same generation collapse;
/// `GenFold` keeps reads coherent without `FINAL`.
pub struct ClickHouseChunkStore {
    /// Connection params (host/port/tls/compression/auth) reused from the main
    /// emitter; toast tables land in `conn.database`.
    conn: EmitterConfig,
    database: String,
    /// For [`BlockBuilder`] + [`TypeAst`] on the `put` path. `Copy`, `Sync`.
    alloc: Allocator,
    state: Mutex<ChState>,
    /// GC's own connection: a sweep's synchronous lightweight DELETE must
    /// not hold `state` against commit-drain `put`s.
    gc_state: Mutex<ChState>,
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
            alloc: Allocator::global(&mimalloc::MiMalloc),
            state: Mutex::new(ChState {
                client: None,
                created: HashSet::new(),
            }),
            gc_state: Mutex::new(ChState {
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
             ) ENGINE = ReplacingMergeTree(`_lsn`)\nORDER BY (`chunk_id`, `_lsn`, `chunk_seq`)",
            self.toast_table(toast_relid)
        )
    }

    fn insert_sql(&self, toast_relid: u32) -> String {
        format!(
            "INSERT INTO {} (`chunk_id`, `chunk_seq`, `chunk_data`, `_lsn`) FORMAT Native",
            self.toast_table(toast_relid)
        )
    }

    /// Lightweight DELETE, not tombstone rows: RMT reclaims tombstones only
    /// on merge, and parts holding dead-forever values may never merge again.
    /// The per-death `_lsn` bound is the generation boundary: a reused-OID
    /// rebirth carries `_lsn` past its predecessor's death and survives,
    /// with no coordination against `put`.
    fn gc_delete_sql(&self, toast_relid: u32, deaths: &[(u32, u64)]) -> String {
        format!(
            "DELETE FROM {} WHERE {}",
            self.toast_table(toast_relid),
            death_predicate(deaths)
        )
    }

    /// Values the paired DELETE actually holds rows for. Read before the
    /// DELETE — nothing else deletes, and interleaved puts (rebirths) carry
    /// `_lsn` past the death bound, so the count is exact.
    fn gc_count_sql(&self, toast_relid: u32, deaths: &[(u32, u64)]) -> String {
        format!(
            "SELECT countDistinct(`chunk_id`) FROM {} WHERE {}",
            self.toast_table(toast_relid),
            death_predicate(deaths)
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

    /// One SELECT drained to EndOfStream under the pool's bounded
    /// reconnect/retry + per-attempt timeout, rows accumulated via `parse`
    /// (Native streams one block at a time, never one unbounded buffered
    /// read). No table/db => empty rows, not a fault — mirrors the disk
    /// store's missing-file path.
    async fn query_locked<A: Default>(
        &self,
        state: &mut ChState,
        sql: &str,
        parse: impl Fn(&Block, &mut A) -> Result<(), EmitterError>,
    ) -> Result<A, EmitterError> {
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
                        secs: self.query_timeout.as_secs(),
                    }),
                }
            };
            match res {
                Ok(out) => return Ok(out),
                Err(EmitterError::ServerException { code, .. })
                    if code == CH_UNKNOWN_TABLE || code == CH_UNKNOWN_DATABASE =>
                {
                    return Ok(A::default());
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

    async fn fetch_locked(
        &self,
        state: &mut ChState,
        toast_relid: u32,
        value_id: u32,
        max_lsn: u64,
    ) -> Result<BTreeMap<u32, Vec<u8>>, EmitterError> {
        let sql = format!(
            "SELECT `chunk_seq`, `chunk_data`, `_lsn` FROM {} WHERE `chunk_id` = {} \
             AND `_lsn` <= {} \
             ORDER BY `chunk_seq`",
            self.toast_table(toast_relid),
            value_id,
            max_lsn,
        );
        let fold = self.query_locked(state, &sql, read_chunk_block).await?;
        Ok(fold.out)
    }
}

/// OR-joined per-death generation bounds.
fn death_predicate(deaths: &[(u32, u64)]) -> String {
    use std::fmt::Write as _;
    let mut s = String::with_capacity(deaths.len() * 40);
    for (i, (value_id, death_lsn)) in deaths.iter().enumerate() {
        if i > 0 {
            s.push_str(" OR ");
        }
        write!(s, "(`chunk_id` = {value_id} AND `_lsn` <= {death_lsn})").unwrap();
    }
    s
}

/// Fold one fetched Data block's `(chunk_seq UInt32, chunk_data String,
/// _lsn UInt64)` rows into the newest-generation selection. Column order
/// matches the `SELECT`. The header block (0 rows) contributes nothing.
fn read_chunk_block(block: &Block, out: &mut GenFold) -> Result<(), EmitterError> {
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
    let lsn_col = block
        .column(2)
        .ok_or_else(|| EmitterError::Type("toast fetch: missing _lsn column".into()))?;
    let (elem, seq_bytes) = seq_col
        .fixed()
        .ok_or_else(|| EmitterError::Type("toast fetch: chunk_seq not fixed-width".into()))?;
    if elem != 4 {
        return Err(EmitterError::Type(format!(
            "toast fetch: chunk_seq elem size {elem} != 4"
        )));
    }
    let (lsn_elem, lsn_bytes) = lsn_col
        .fixed()
        .ok_or_else(|| EmitterError::Type("toast fetch: _lsn not fixed-width".into()))?;
    if lsn_elem != 8 {
        return Err(EmitterError::Type(format!(
            "toast fetch: _lsn elem size {lsn_elem} != 8"
        )));
    }
    let (offsets, data) = data_col
        .string()
        .ok_or_else(|| EmitterError::Type("toast fetch: chunk_data not String".into()))?;
    for i in 0..n {
        let seq = u32::from_le_bytes(seq_bytes[i * 4..i * 4 + 4].try_into().unwrap());
        let lsn = u64::from_le_bytes(lsn_bytes[i * 8..i * 8 + 8].try_into().unwrap());
        let start = if i == 0 { 0 } else { offsets[i - 1] as usize };
        let end = offsets[i] as usize;
        out.add(seq, lsn, data[start..end].to_vec());
    }
    Ok(())
}

/// Single `UInt64` aggregate row (count).
fn read_count_block(block: &Block, out: &mut Vec<u64>) -> Result<(), EmitterError> {
    let n = block.n_rows();
    if n == 0 {
        return Ok(());
    }
    let (elem, bytes) = block
        .column(0)
        .and_then(|c| c.fixed())
        .ok_or_else(|| EmitterError::Type("toast gc: count not fixed-width".into()))?;
    if elem != 8 {
        return Err(EmitterError::Type(format!(
            "toast gc: count elem size {elem} != 8"
        )));
    }
    for i in 0..n {
        out.push(u64::from_le_bytes(
            bytes[i * 8..i * 8 + 8].try_into().unwrap(),
        ));
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
        max_lsn: u64,
    ) -> Result<BTreeMap<u32, Vec<u8>>, ChunkStoreError> {
        let mut state = self.state.lock().await;
        self.fetch_locked(&mut state, toast_relid, value_id, max_lsn)
            .await
            .map_err(|e| ChunkStoreError::Clickhouse(e.to_string()))
    }

    async fn gc_values(
        &self,
        toast_relid: u32,
        deaths: &[(u32, u64)],
    ) -> Result<u64, ChunkStoreError> {
        if deaths.is_empty() {
            return Ok(0);
        }
        let mut state = self.gc_state.lock().await;
        // Batch to bound statement size; each DELETE is idempotent.
        let mut deleted = 0u64;
        for batch in deaths.chunks(GC_DELETE_BATCH) {
            // Count first: DELETE reports no affected rows, and the death
            // bounds freeze the counted set (interleaved puts land past them).
            let counts = self
                .query_locked(
                    &mut state,
                    &self.gc_count_sql(toast_relid, batch),
                    read_count_block,
                )
                .await
                .map_err(|e| ChunkStoreError::Clickhouse(e.to_string()))?;
            let batch_hits = counts.iter().sum::<u64>();
            if batch_hits == 0 {
                // Nothing to delete (replayed deaths, or no such table);
                // skipping also spares the mutation
                continue;
            }
            let sql = self.gc_delete_sql(toast_relid, batch);
            self.exec_write(&mut state, &sql, None)
                .await
                .map_err(|e| ChunkStoreError::Clickhouse(e.to_string()))?;
            deleted += batch_hits;
        }
        Ok(deleted)
    }
}

/// Resolution policy + optional store + TID death tracker, threaded into
/// the detoast paths and the bootstrap drain. Cheap to clone (everything
/// behind `Arc`).
#[derive(Clone)]
pub struct ToastResolver {
    mode: ToastMode,
    store: Option<Arc<dyn ChunkStore>>,
    tracker: Option<Arc<TidTracker>>,
    stats: Arc<EmitterStats>,
}

impl ToastResolver {
    /// No store; misses NULL/default-fill. Used for the metrics-only serial
    /// drain (no `--ch-config`) and as the default.
    pub fn disabled() -> Self {
        Self {
            mode: ToastMode::Disabled,
            store: None,
            tracker: None,
            stats: Arc::new(EmitterStats::default()),
        }
    }

    /// Build from the emitter config (CH mode reuses its connection params),
    /// sharing the emitter's live counters. Reads `emitter.toast` for mode,
    /// disk dir + TID journal.
    pub fn from_config(emitter: &EmitterConfig, stats: Arc<EmitterStats>) -> Result<Self, String> {
        let cfg = &emitter.toast;
        let open_tracker = |path: PathBuf| {
            TidTracker::open(path, stats.clone())
                .map(Arc::new)
                .map_err(|e| format!("toast tid journal: {e}"))
        };
        let resolver = match cfg.mode {
            ToastMode::Disabled => Self {
                mode: ToastMode::Disabled,
                store: None,
                tracker: None,
                stats,
            },
            ToastMode::Disk => {
                let dir = cfg
                    .disk_dir
                    .clone()
                    .ok_or("toast mode=disk requires [toast] disk_dir")?;
                let journal = cfg
                    .tid_journal
                    .clone()
                    .unwrap_or_else(|| dir.join("tids.journal"));
                let store =
                    DiskChunkStore::new(dir).map_err(|e| format!("toast disk store: {e}"))?;
                Self {
                    mode: ToastMode::Disk,
                    store: Some(Arc::new(store)),
                    tracker: Some(open_tracker(journal)?),
                    stats,
                }
            }
            ToastMode::ClickHouse => {
                let store = ClickHouseChunkStore::new(emitter.clone());
                // Journal is local durable state; without a configured path
                // deaths go untracked and GC has nothing to collect
                let tracker = cfg.tid_journal.clone().map(open_tracker).transpose()?;
                if tracker.is_none() {
                    tracing::warn!(
                        target: "walshadow::toast",
                        "[toast] mode=clickhouse without tid_journal: dead chunks accumulate",
                    );
                }
                Self {
                    mode: ToastMode::ClickHouse,
                    store: Some(Arc::new(store)),
                    tracker,
                    stats,
                }
            }
        };
        // Tracking without GC never collects: pending deaths + journal grow
        // with churn (deferred work — enabling GC later drains them)
        if resolver.tracker.is_some() && cfg.gc_interval.is_zero() {
            tracing::warn!(
                target: "walshadow::toast",
                "[toast] TID tracking armed with gc_interval_secs = 0: \
                 resolved deaths accumulate until GC is enabled",
            );
        }
        Ok(resolver)
    }

    #[cfg(test)]
    pub fn with_store(store: Arc<dyn ChunkStore>, stats: Arc<EmitterStats>) -> Self {
        Self {
            mode: ToastMode::Disk,
            store: Some(store),
            tracker: None,
            stats,
        }
    }

    #[cfg(test)]
    pub fn with_store_and_tracker(
        store: Arc<dyn ChunkStore>,
        tracker: Arc<TidTracker>,
        stats: Arc<EmitterStats>,
    ) -> Self {
        Self {
            mode: ToastMode::Disk,
            store: Some(store),
            tracker: Some(tracker),
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

    /// Shared store handle for the GC sweep ([`crate::toast_gc`]); sharing
    /// (not a second instance) is what serializes disk-mode delete against
    /// put. `None` for disabled mode — nothing to collect.
    pub fn store(&self) -> Option<Arc<dyn ChunkStore>> {
        self.store.clone()
    }

    /// Shared TID tracker handle for the GC sweep. `None` when tracking is
    /// off (disabled mode, or CH mode without `tid_journal`).
    pub fn tracker(&self) -> Option<Arc<TidTracker>> {
        self.tracker.clone()
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
    /// dropped at merge; `commit_lsn` is the convergence `_lsn` and the
    /// value's generation LSN). No-op when there is no store or the map is
    /// empty.
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
                    // TIDs ride tid_events, not the merged map
                    blkno: 0,
                    offnum: 0,
                    chunk_data: body.clone(),
                });
            }
        }
        self.put(&rows).await
    }

    /// Persist one commit's chunks then apply its TID events (births +
    /// death resolution, [`crate::toast_tid`]). Chunks first: a crash
    /// between the two re-observes both from ack, and a journaled birth
    /// must never precede its chunks' durability.
    pub async fn apply_commit(
        &self,
        map: &ChunkMap,
        tid_events: &[TidEvent],
        commit_lsn: u64,
    ) -> Result<(), ChunkStoreError> {
        self.put_map(map, commit_lsn).await?;
        if let Some(tracker) = &self.tracker {
            tracker.apply(tid_events, commit_lsn).await?;
        }
        Ok(())
    }

    /// Apply walk-side TID births (bootstrap page walk). No-op without a
    /// tracker.
    pub async fn apply_tid_events(
        &self,
        tid_events: &[TidEvent],
        lsn: u64,
    ) -> Result<(), ChunkStoreError> {
        if let Some(tracker) = &self.tracker {
            tracker.apply(tid_events, lsn).await?;
        }
        Ok(())
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

    fn chunk_at(relid: u32, value_id: u32, seq: u32, lsn: u64, body: &[u8]) -> ToastChunk {
        ToastChunk {
            toast_relid: relid,
            value_id,
            chunk_seq: seq,
            source_lsn: lsn,
            blkno: 0,
            offnum: 0,
            chunk_data: body.to_vec(),
        }
    }

    fn chunk(relid: u32, value_id: u32, seq: u32, body: &[u8]) -> ToastChunk {
        chunk_at(relid, value_id, seq, 0x1000, body)
    }

    #[tokio::test]
    async fn disk_store_roundtrip_across_puts() {
        let tmp = tempfile::tempdir().unwrap();
        let store = DiskChunkStore::new(tmp.path().to_path_buf()).unwrap();
        // Two puts for the same value under one generation LSN (bootstrap
        // batches split a value's chunks across put calls).
        store
            .put(&[chunk(16500, 7, 0, b"abc"), chunk(16500, 7, 1, b"de")])
            .await
            .unwrap();
        store.put(&[chunk(16500, 7, 2, b"f")]).await.unwrap();
        // Unrelated value in the same relation.
        store.put(&[chunk(16500, 9, 0, b"zz")]).await.unwrap();

        let got = store.fetch(16500, 7, u64::MAX).await.unwrap();
        assert_eq!(got.len(), 3);
        assert_eq!(got.get(&0).unwrap(), b"abc");
        assert_eq!(got.get(&1).unwrap(), b"de");
        assert_eq!(got.get(&2).unwrap(), b"f");

        let other = store.fetch(16500, 9, u64::MAX).await.unwrap();
        assert_eq!(other.len(), 1);
        assert_eq!(other.get(&0).unwrap(), b"zz");

        // Absent value → empty, not error.
        assert!(store.fetch(16500, 999, u64::MAX).await.unwrap().is_empty());
        assert!(store.fetch(404, 1, u64::MAX).await.unwrap().is_empty());
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

        let got = store.fetch(1, 1, u64::MAX).await.unwrap();
        assert_eq!(got.len(), 2, "torn tail ignored, clean prefix kept");
        assert_eq!(got.get(&0).unwrap(), b"good");
        assert_eq!(got.get(&1).unwrap(), b"alsogood");
    }

    /// Pre-LSN v1 file (headerless `[seq][len][body]` frames): read as
    /// lsn 0, upgraded in place by the next append, newer generation wins.
    #[tokio::test]
    async fn disk_store_reads_and_upgrades_v1_files() {
        let tmp = tempfile::tempdir().unwrap();
        let store = DiskChunkStore::new(tmp.path().to_path_buf()).unwrap();
        let path = store.value_path(1, 7);
        tokio::fs::create_dir_all(path.parent().unwrap())
            .await
            .unwrap();
        let mut v1 = Vec::new();
        for (seq, body) in [(0u32, b"old0".as_slice()), (1, b"old1")] {
            v1.extend_from_slice(&seq.to_le_bytes());
            v1.extend_from_slice(&(body.len() as u32).to_le_bytes());
            v1.extend_from_slice(body);
        }
        tokio::fs::write(&path, &v1).await.unwrap();

        let got = store.fetch(1, 7, u64::MAX).await.unwrap();
        assert_eq!(got.len(), 2);
        assert_eq!(got.get(&0).unwrap(), b"old0");

        // Append (reused OID regeneration, shorter): upgrades the file,
        // fetch returns only the new generation
        store
            .put(&[chunk_at(1, 7, 0, 0x2000, b"new0")])
            .await
            .unwrap();
        let got = store.fetch(1, 7, u64::MAX).await.unwrap();
        assert_eq!(got.len(), 1, "stale v1 suffix dropped");
        assert_eq!(got.get(&0).unwrap(), b"new0");
    }

    /// Reused-OID regeneration: fetch returns the newest generation whole,
    /// never a chimera of new seq 0 + stale suffix.
    #[tokio::test]
    async fn disk_store_fetch_selects_newest_generation() {
        let tmp = tempfile::tempdir().unwrap();
        let store = DiskChunkStore::new(tmp.path().to_path_buf()).unwrap();
        store
            .put(&[
                chunk_at(1, 7, 0, 0x1000, b"g1-0"),
                chunk_at(1, 7, 1, 0x1000, b"g1-1"),
                chunk_at(1, 7, 2, 0x1000, b"g1-2"),
            ])
            .await
            .unwrap();
        // Shorter regeneration under a later commit
        store
            .put(&[
                chunk_at(1, 7, 0, 0x2000, b"g2-0"),
                chunk_at(1, 7, 1, 0x2000, b"g2-1"),
            ])
            .await
            .unwrap();
        let old = store.fetch(1, 7, 0x1fff).await.unwrap();
        assert_eq!(old.len(), 3);
        assert_eq!(old.get(&0).unwrap(), b"g1-0");
        let got = store.fetch(1, 7, u64::MAX).await.unwrap();
        assert_eq!(got.len(), 2, "old generation's seq 2 dropped");
        assert_eq!(got.get(&0).unwrap(), b"g2-0");
        assert_eq!(got.get(&1).unwrap(), b"g2-1");
    }

    #[tokio::test]
    async fn disk_store_gc_deletes_dead_generation_only() {
        let tmp = tempfile::tempdir().unwrap();
        let store = DiskChunkStore::new(tmp.path().to_path_buf()).unwrap();
        store
            .put(&[chunk_at(16500, 7, 0, 0x1000, b"abc")])
            .await
            .unwrap();
        store
            .put(&[chunk_at(16500, 9, 0, 0x1000, b"zz")])
            .await
            .unwrap();

        // 7 died at 0x1800; 9 stays live
        assert_eq!(store.gc_values(16500, &[(7, 0x1800)]).await.unwrap(), 1);
        assert!(store.fetch(16500, 7, u64::MAX).await.unwrap().is_empty());
        assert!(!store.fetch(16500, 9, u64::MAX).await.unwrap().is_empty());
        // Idempotent: already gone
        assert_eq!(store.gc_values(16500, &[(7, 0x1800)]).await.unwrap(), 0);

        // Rebirth past the death LSN survives its predecessor's GC
        store
            .put(&[chunk_at(16500, 9, 0, 0x3000, b"reborn")])
            .await
            .unwrap();
        assert_eq!(store.gc_values(16500, &[(9, 0x2000)]).await.unwrap(), 1);
        let got = store.fetch(16500, 9, u64::MAX).await.unwrap();
        assert_eq!(got.get(&0).unwrap(), b"reborn", "rebirth survives");
    }

    #[test]
    fn ch_store_renders_gc_sql() {
        let cfg = EmitterConfig {
            database: "wh".into(),
            ..Default::default()
        };
        let store = ClickHouseChunkStore::new(cfg);
        assert_eq!(
            store.gc_delete_sql(16500, &[(7, 0x2000), (9, 0x3000)]),
            "DELETE FROM `wh`.`pg_toast_16500` WHERE \
             (`chunk_id` = 7 AND `_lsn` <= 8192) OR (`chunk_id` = 9 AND `_lsn` <= 12288)"
        );
        assert_eq!(
            store.gc_count_sql(16500, &[(7, 0x2000)]),
            "SELECT countDistinct(`chunk_id`) FROM `wh`.`pg_toast_16500` \
             WHERE (`chunk_id` = 7 AND `_lsn` <= 8192)"
        );
    }

    /// Order-independent newest-generation fold: same-LSN duplicates merge,
    /// a higher LSN resets, stragglers below the max drop.
    #[test]
    fn gen_fold_keeps_max_lsn_group() {
        let mut fold = GenFold::default();
        fold.add(0, 0x1000, b"g1-0".to_vec());
        fold.add(1, 0x1000, b"g1-1".to_vec());
        fold.add(0, 0x2000, b"g2-0".to_vec());
        fold.add(1, 0x1000, b"late-stale".to_vec());
        fold.add(0, 0x2000, b"g2-0".to_vec());
        assert_eq!(fold.out.len(), 1);
        assert_eq!(fold.out.get(&0).unwrap(), b"g2-0");
    }

    #[tokio::test]
    async fn resolver_disabled_fills_on_miss_no_store() {
        let r = ToastResolver::disabled();
        assert!(r.fill_on_miss());
        assert!(!r.stores_chunks());
        let mut map = ChunkMap::new();
        assert!(!r.fetch_into(1, 2, u64::MAX, &mut map).await.unwrap());
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

        let create = store.create_sql(16500);
        assert!(create.starts_with("CREATE TABLE IF NOT EXISTS `wh`.`pg_toast_16500`"));
        assert!(create.contains("`chunk_id` UInt32"));
        assert!(create.contains("`chunk_seq` UInt32"));
        assert!(create.contains("`chunk_data` String"));
        assert!(create.contains("`_lsn` UInt64"));
        assert!(create.contains("ENGINE = ReplacingMergeTree(`_lsn`)"));
        assert!(create.ends_with("ORDER BY (`chunk_id`, `_lsn`, `chunk_seq`)"));

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
