//! Per-table `initial_load` backfiller. Owns the resume ledger and dispatches
//! per mode: `'copy'` (this module's COPY path, below) runs one detached task
//! per table; `'base_backup'` / `'object_store'` coalesce into one
//! [`crate::backfill::backup_backfill`] pass per mode, loading per-rel staging tables
//! that publish via `EXCHANGE TABLES` + live-window copy-back on success
//! ([`crate::backfill::backfill_staging`], architecture/bootstrap.md)
//! (docs/configuration.md).
//!
//! ## COPY mode
//!
//! Snapshot-free initial load for a non-empty table opted in via
//! `config_table (replicate=true, initial_load='copy')`.
//! Correctness rests on walshadow's convergence model, not a snapshot cut:
//! the opt-in commits at LSN `S`; WAL-driven rows apply from `S` on, and a
//! lone `COPY (SELECT …) TO STDOUT (FORMAT binary)` issued after the opt-in
//! applies runs under a statement snapshot `P ≥ S`, so it covers exactly the
//! xacts that committed before `S` (and re-covers `(S, P]`, absorbed by
//! `ReplacingMergeTree(_lsn)` dedup: COPY rows carry `_lsn = S`, every WAL
//! mutation carries its real `commit_lsn > S`). COPY must run against the
//! node walshadow streams WAL from.
//!
//! Rows ship through the same insert tail as greenfield bootstrap
//! ([`crate::emit::pipeline::tail`] + [`crate::emit::pipeline::bootstrap::drain`]), on a
//! dedicated CH connection, so a backfill never blocks the live pipeline.
//! COPY output is fully detoasted, so its rows need no TOAST resolver. Live
//! updates past `S` can still carry a pre-load value's pointer unchanged, so
//! in `clickhouse` value mode the load first copies the TOAST heap into the
//! chunk mirror, before live apply passes `S` (see [`copy_toast_into`]).
//!
//! The COPY session pins the text output settings the shadow bridge worker
//! pins (`pgext/worker.c`): values cast to `text` must render as the WAL
//! path renders them, whatever role or database defaults say.
//!
//! ## Field decode
//!
//! Binary COPY carries each field in `typsend` wire form (big-endian), not
//! the on-disk datum form the WAL heap decoder reads. Fixed-width types and
//! byte/text strings decode natively into the same [`ColumnValue`] variants
//! the WAL path produces; `numeric` and `jsonb` select as `::text`
//! (`numeric_out` and `jsonb_out` are the forms the WAL path renders too);
//! every out-of-matrix type also selects
//! as `::text` and ships as [`ColumnValue::PgPendingText`], so the oracle
//! converts it through `typinput` exactly as it does a WAL-path default
//! (architecture/values.md).
//!
//! ## Resume ledger
//!
//! `{spill_dir}/backfills.toml` persists per-qname `{s_lsn, done, mode}`,
//! stamped with the source system identifier. The
//! opt-in's WAL event is not re-delivered after the ack passes `S`, so the
//! ledger is what carries an unfinished backfill across a restart: boot
//! re-seeds opt-ins from `config_table` and re-runs the *recorded* mode for
//! pending entries at their original `S` (dedup makes the re-run idempotent).
//! A `done` entry stops every later boot from re-running (the daemon never
//! writes `initial_load` back to source). An absent ledger is empty; an
//! unreadable or foreign one stops boot, since its entries name staging
//! tables and swap phases nothing else records. Completion is observability:
//! convergence is reported once WAL apply passes `P_hi = pg_current_wal_lsn()`
//! read at COPY EOF; nothing is gated on it.

use std::num::NonZeroU32;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use anyhow::Context as _;
use async_trait::async_trait;
use futures::StreamExt as _;
use tokio::sync::{Mutex, mpsc, watch};
use tokio_postgres::binary_copy::BinaryCopyOutStream;
use tokio_postgres::types::Type;
use walrus::pg::backup::format_pg_lsn;
use walrus::pg::replication::conn::PgConfig;

use crate::backfill::backfill_staging::{self, StagingPlan, StagingRel, StagingSession};
use crate::backfill::backfill_types::{BackupRequest, PassContext, PassOutcome};
use crate::backfill::backup_checkpoint::BackupCheckpoint;
use crate::backfill::backup_page_walk::{BOOTSTRAP_TUPLE_CHANNEL_CAP, BackfillTuple, CatalogMap};
use crate::backfill::opt_in::Backfiller;
use crate::backfill::visibility_pending::{self, SharedPendingLedger};
use crate::catalog::shadow_catalog::ShadowCatalog;
use crate::config::{DestEmitter, ResolvedConfig};
use crate::decode::codecs::NumericKind;
use crate::decode::heap_decoder::ColumnValue;
use crate::emit::ch_emitter::{EmitterStats, ToastMode};
use crate::emit::pipeline::tail::OwnedTail;
use crate::emit::pipeline::{Fatal, bootstrap};
use crate::mapping::MappingHandle;
use crate::ops::oracle::Oracle;
use crate::pg::{current_wal_lsn, quote_ident};
use crate::pos::{Pos, Snapshot};
use crate::runtime_config::InitialLoadMode;
use crate::schema::{
    BOOLOID, BPCHAROID, BYTEAOID, CHAROID, DATEOID, FLOAT4OID, FLOAT8OID, INT2OID, INT4OID,
    INT8OID, JSONBOID, JSONOID, NAMEOID, NUMERICOID, OIDOID, RelDescriptor, RelName, TEXTOID,
    TIMEOID, TIMESTAMPOID, TIMESTAMPTZOID, UUIDOID, VARCHAROID,
};
use crate::source::source_feed::open_sql_client;
use crate::toast::{ToastResolver, ToastRow};

/// Rows per COPY-backfill channel hop. Byte trigger below bounds the wide-row
/// case, so this only caps the narrow-row hop rate
const COPY_SLAB_ROWS: usize = 1024;
/// Decoded value bytes per hop. With
/// [`BOOTSTRAP_TUPLE_CHANNEL_CAP`] this is the resident payload ceiling
pub const COPY_SLAB_BYTES: usize = 1 << 20;
use ahash::{HashMap, HashMapExt, HashSet, HashSetExt};

const LEDGER_FILENAME: &str = "backfills.toml";
const LEDGER_VERSION: u32 = 1;

/// Output settings `pgext/worker.c` pins for typoutput, plus row security:
/// a filtered COPY would silently miss rows
const COPY_SESSION_SETUP: &str = "SET row_security = off; SET TimeZone = 'UTC'; \
     SET DateStyle = 'ISO,MDY'; SET IntervalStyle = 'postgres'; \
     SET extra_float_digits = 1; SET bytea_output = 'hex'";

/// Status of 32-bit xids at source, widened against the current epoch.
/// NULL when source `pg_xact` no longer holds the xid
const XID_STATUS_SQL: &str = "SELECT x, pg_xact_status((n - ((n - x) & 4294967295))::text::xid8) \
     FROM unnest($1::int8[]) AS x, \
     (SELECT pg_snapshot_xmax(pg_current_snapshot())::text::int8 AS n) AS cur";

/// Backup-mode opt-ins wait this long for siblings before the pass fires, so
/// an opt-in burst (several rows in one xact, or a boot seed) coalesces into
/// one cluster-sized backup pass instead of one per table.
const BACKUP_COALESCE_WINDOW: Duration = Duration::from_millis(1000);

// ---------------------------------------------------------------------------
// Resume ledger
// ---------------------------------------------------------------------------

#[derive(serde::Serialize, serde::Deserialize)]
struct LedgerFile {
    version: u32,
    #[serde(default)]
    system_id: Option<u64>,
    #[serde(default)]
    backfill: Vec<LedgerEntry>,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct LedgerEntry {
    namespace: String,
    relname: String,
    s_lsn: Pos<Snapshot>,
    done: bool,
    /// [`InitialLoadMode`] string; absent ⇒ `copy`
    #[serde(default = "default_ledger_mode")]
    mode: String,
    /// Backup-mode staging swap phase (architecture/bootstrap.md):
    /// pass rows durable, `EXCHANGE TABLES` issued or about to be — boot
    /// resumes the swap tail (copy-back + drop) instead of re-loading
    #[serde(default)]
    swapped: bool,
    /// Staging table uuid recorded just before the exchange; recovery
    /// compares it against the uuid now under the staging name to tell
    /// whether the exchange applied
    #[serde(default)]
    staging_uuid: Option<String>,
    /// Resumable COPY cursor: filenode the cursor was measured against, and
    /// the next heap block to read. A rewrite changes the filenode, which
    /// retires the cursor instead of skipping relocated rows
    #[serde(default)]
    copy_relfilenode: Option<u32>,
    #[serde(default)]
    copy_next_block: Option<u32>,
    /// COPY load copied the TOAST heap into the chunk mirror
    #[serde(default)]
    toast_seeded: bool,
}

fn default_ledger_mode() -> String {
    "copy".into()
}

/// One backfill's durable state; boot re-runs `mode` at `s_lsn` while
/// `!done`, or resumes the swap tail while `swapped`.
#[derive(Debug, Clone)]
struct LedgerRec {
    s_lsn: Pos<Snapshot>,
    done: bool,
    mode: InitialLoadMode,
    swapped: bool,
    staging_uuid: Option<String>,
    copy: Option<CopyCursor>,
    toast_seeded: bool,
}

/// Where a chunked COPY stopped. Every chunk below `next_block` proved its
/// rows durable before this was written, so resume replays at most the chunk
/// in flight when the daemon died
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct CopyCursor {
    relfilenode: u32,
    next_block: u32,
}

struct Ledger {
    dir: PathBuf,
    system_id: u64,
    entries: HashMap<RelName, LedgerRec>,
}

impl Ledger {
    /// Absent file is an empty ledger. An unreadable one is an error: its
    /// entries name staging tables and swap phases nothing else records
    async fn load(spill_dir: &Path, system_id: u64) -> std::io::Result<Self> {
        let mut ledger = Self {
            dir: spill_dir.to_path_buf(),
            system_id,
            entries: HashMap::new(),
        };
        let path = spill_dir.join(LEDGER_FILENAME);
        let invalid = |detail: String| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "backfill ledger {}: {detail}; restore it, removing it re-runs every \
                     load and can orphan `__wsstg` staging tables",
                    path.display()
                ),
            )
        };
        let text = match tokio::fs::read_to_string(&path).await {
            Ok(text) => text,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(ledger),
            Err(e) => return Err(invalid(e.to_string())),
        };
        let file = toml::from_str::<LedgerFile>(&text).map_err(|e| invalid(e.to_string()))?;
        if file.version != LEDGER_VERSION {
            return Err(invalid(format!("unsupported version {}", file.version)));
        }
        let unstamped = crate::fs::check_source(&path, file.system_id, system_id)?;
        ledger.entries = file
            .backfill
            .into_iter()
            .map(|e| {
                // Only this daemon writes modes; an unparseable one
                // degrades to re-COPY (idempotent)
                let mode = e.mode.parse().unwrap_or(InitialLoadMode::Copy);
                (
                    RelName::new(&e.namespace, &e.relname),
                    LedgerRec {
                        s_lsn: e.s_lsn,
                        done: e.done,
                        mode,
                        swapped: e.swapped,
                        staging_uuid: e.staging_uuid,
                        copy: e.copy_relfilenode.zip(e.copy_next_block).map(
                            |(relfilenode, next_block)| CopyCursor {
                                relfilenode,
                                next_block,
                            },
                        ),
                        toast_seeded: e.toast_seeded,
                    },
                )
            })
            .collect();
        if unstamped {
            ledger.persist().await?;
        }
        Ok(ledger)
    }

    /// Crash-safe persist via [`crate::fs::write_atomic`].
    async fn persist(&self) -> std::io::Result<()> {
        let file = LedgerFile {
            version: LEDGER_VERSION,
            system_id: Some(self.system_id),
            backfill: self
                .entries
                .iter()
                .map(|(rel, rec)| LedgerEntry {
                    namespace: rel.namespace.to_string(),
                    relname: rel.name.to_string(),
                    s_lsn: rec.s_lsn,
                    done: rec.done,
                    mode: rec.mode.as_str().into(),
                    swapped: rec.swapped,
                    staging_uuid: rec.staging_uuid.clone(),
                    copy_relfilenode: rec.copy.map(|c| c.relfilenode),
                    copy_next_block: rec.copy.map(|c| c.next_block),
                    toast_seeded: rec.toast_seeded,
                })
                .collect(),
        };
        let text = toml::to_string(&file).expect("ledger serialize");
        crate::fs::write_atomic(&self.dir, LEDGER_FILENAME, text.as_bytes()).await
    }

    async fn fallback_to_copy(
        &mut self,
        rel: &RelName,
        mode: InitialLoadMode,
        s_lsn: u64,
    ) -> std::io::Result<bool> {
        let Some(rec) = self.entries.get_mut(rel) else {
            return Ok(false);
        };
        if rec.done
            || rec.swapped
            || rec.staging_uuid.is_some()
            || rec.mode != mode
            || rec.s_lsn.get() != s_lsn
        {
            return Ok(false);
        }
        rec.mode = InitialLoadMode::Copy;
        if let Err(e) = self.persist().await {
            self.entries.get_mut(rel).unwrap().mode = mode;
            return Err(e);
        }
        Ok(true)
    }

    /// Advance the COPY cursor. Callers write it only once the chunk below
    /// `next_block` proved durable
    async fn note_copy(&mut self, rel: &RelName, cursor: CopyCursor) -> std::io::Result<()> {
        let Some(rec) = self.entries.get_mut(rel) else {
            return Ok(());
        };
        rec.copy = Some(cursor);
        self.persist().await
    }

    fn pending_count(&self) -> u64 {
        self.entries.values().filter(|r| !r.done).count() as u64
    }

    fn pending_count_for(&self, mode: InitialLoadMode) -> u64 {
        self.entries
            .values()
            .filter(|r| !r.done && r.mode == mode)
            .count() as u64
    }
}

// ---------------------------------------------------------------------------
// Per-column decode plan
// ---------------------------------------------------------------------------

/// How one selected column decodes from the wire. Native kinds read `typsend`
/// output; `NumericText`/`CastText` columns were cast to `text` in the SELECT.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WireKind {
    Bool,
    Char,
    Int2,
    Int4,
    Int8,
    Oid,
    Float4,
    Float8,
    Date,
    Time,
    Timestamp,
    TimestampTz,
    Uuid,
    Bytea,
    Text,
    Name,
    Json,
    NumericText,
    CastText,
}

struct ColPlan {
    attnum: i16,
    kind: WireKind,
    type_oid: u32,
}

struct CopyPlan {
    select: String,
    cols: Vec<ColPlan>,
    natts: usize,
}

fn wire_kind(type_oid: u32) -> Option<WireKind> {
    Some(match type_oid {
        BOOLOID => WireKind::Bool,
        CHAROID => WireKind::Char,
        INT2OID => WireKind::Int2,
        INT4OID => WireKind::Int4,
        INT8OID => WireKind::Int8,
        OIDOID => WireKind::Oid,
        FLOAT4OID => WireKind::Float4,
        FLOAT8OID => WireKind::Float8,
        DATEOID => WireKind::Date,
        TIMEOID => WireKind::Time,
        TIMESTAMPOID => WireKind::Timestamp,
        TIMESTAMPTZOID => WireKind::TimestampTz,
        UUIDOID => WireKind::Uuid,
        BYTEAOID => WireKind::Bytea,
        TEXTOID | VARCHAROID | BPCHAROID => WireKind::Text,
        NAMEOID => WireKind::Name,
        JSONOID => WireKind::Json,
        _ => return None,
    })
}

fn column_plan(desc: &RelDescriptor) -> CopyPlan {
    let mut select = String::new();
    let mut cols = Vec::with_capacity(desc.attributes.len());
    let mut natts = 0usize;
    for a in &desc.attributes {
        natts = natts.max(a.attnum.max(0) as usize);
        if a.dropped {
            continue;
        }
        let ident = quote_ident(&a.name);
        let (expr, kind) = match wire_kind(a.type_oid) {
            Some(k) => (ident, k),
            // jsonb's send form prefixes a version byte; its text is the
            // document the WAL path renders, so take that instead
            None if a.type_oid == JSONBOID => (format!("{ident}::text"), WireKind::Json),
            None if a.type_oid == NUMERICOID => (format!("{ident}::text"), WireKind::NumericText),
            None => (format!("{ident}::text"), WireKind::CastText),
        };
        if !select.is_empty() {
            select.push_str(", ");
        }
        select.push_str(&expr);
        cols.push(ColPlan {
            attnum: a.attnum,
            kind,
            type_oid: a.type_oid,
        });
    }
    CopyPlan {
        select,
        cols,
        natts,
    }
}

/// Heap pages a COPY chunk spans when `[bootstrap] copy_chunk_blocks` is
/// unset. Matches PostgreSQL's 1 GiB segment at the default page size, so a
/// restart re-reads at most one segment's worth
pub const COPY_CHUNK_BLOCKS: u32 = 131_072;

/// Half-open heap block range for one COPY chunk. `end` absent reads to the
/// end of the relation, covering pages appended while the copy ran
#[derive(Debug, Clone, Copy)]
pub struct BlockRange {
    pub start: u32,
    pub end: Option<u32>,
}

/// `WHERE` clause pinning a chunk to its pages. A TID range scan reads
/// exactly those pages; the whole-relation range emits nothing so a table
/// small enough for one chunk plans as it always did
fn ctid_range(range: BlockRange) -> String {
    match (range.start, range.end) {
        (0, None) => String::new(),
        (start, None) => format!(" WHERE ctid >= '({start},0)'::tid"),
        (0, Some(end)) => format!(" WHERE ctid < '({end},0)'::tid"),
        (start, Some(end)) => {
            format!(" WHERE ctid >= '({start},0)'::tid AND ctid < '({end},0)'::tid")
        }
    }
}

/// Copy visible, detoasted rows in `range` from `desc` into `tx`, tagged `lsn`
///
/// Slabs the channel: one hop per [`COPY_SLAB_BYTES`] of decoded values, not
/// one per row. Byte-triggered so wide rows keep the resident bound whatever
/// the row count
pub async fn copy_rows_into(
    client: &tokio_postgres::Client,
    desc: &RelDescriptor,
    lsn: u64,
    tx: &mpsc::Sender<Vec<BackfillTuple>>,
    stats: &EmitterStats,
    range: BlockRange,
) -> anyhow::Result<u64> {
    let plan = column_plan(desc);
    let sql = format!(
        "COPY (SELECT {} FROM ONLY {}.{}{}) TO STDOUT (FORMAT binary)",
        plan.select,
        quote_ident(&desc.rel_name.namespace),
        quote_ident(&desc.rel_name.name),
        ctid_range(range),
    );
    let byte_fields = vec![Type::BYTEA; plan.cols.len()];
    let copy = client.copy_out(&sql).await.context("backfill: COPY out")?;
    let stream = BinaryCopyOutStream::new(copy, &byte_fields);
    futures::pin_mut!(stream);
    let mut rows = 0u64;
    let mut slab: Vec<BackfillTuple> = Vec::new();
    let mut slab_bytes = 0usize;
    while let Some(row) = stream.next().await {
        let row = row.context("backfill: COPY stream")?;
        let mut payload_bytes = 0u64;
        let mut columns: Vec<Option<ColumnValue>> = vec![None; plan.natts];
        for (i, cp) in plan.cols.iter().enumerate() {
            let raw: Option<&[u8]> = row.try_get(i).context("backfill: COPY field")?;
            payload_bytes += raw.map_or(0, |bytes| bytes.len() as u64);
            let v = raw
                .map(|raw| decode_field(cp.kind, cp.type_oid, raw))
                .transpose()
                .map_err(anyhow::Error::msg)?
                .unwrap_or(ColumnValue::Null);
            columns[(cp.attnum - 1).max(0) as usize] = Some(v);
        }
        slab_bytes += columns
            .iter()
            .flatten()
            .map(ColumnValue::approx_bytes)
            .sum::<usize>();
        slab.push(BackfillTuple {
            rfn: desc.rfn,
            xid: 0,
            xmax: 0,
            infomask: 0,
            source_lsn: lsn,
            // COPY rows have no on-page TID
            blkno: 0,
            offnum: 0,
            columns,
        });
        rows += 1;
        stats.backfill_copy_rows.fetch_add(1, Ordering::Relaxed);
        stats
            .backfill_copy_bytes
            .fetch_add(payload_bytes, Ordering::Relaxed);
        if slab.len() >= COPY_SLAB_ROWS || slab_bytes >= COPY_SLAB_BYTES {
            slab_bytes = 0;
            tx.send(std::mem::take(&mut slab))
                .await
                .map_err(|_| anyhow::anyhow!("backfill: drain closed early"))?;
        }
    }
    if !slab.is_empty() {
        tx.send(slab)
            .await
            .map_err(|_| anyhow::anyhow!("backfill: drain closed early"))?;
    }
    Ok(rows)
}

/// Copy `desc`'s TOAST heap into the chunk mirror, rows keyed by TID and
/// versioned `lsn`; returns chunks copied, 0 without a TOAST heap
///
/// With `lsn = S`, run after `S` and before live apply resolves anything
/// past it, the mirror holds every value live at the scan, so each pointer a
/// later update carries unchanged. WAL writes to a TID after `S` outrank
/// these rows, so chunks written or removed after the scan stay current
pub async fn copy_toast_into(
    client: &tokio_postgres::Client,
    resolver: &ToastResolver,
    desc: &RelDescriptor,
    lsn: u64,
) -> anyhow::Result<u64> {
    let Some(row) = client
        .query_opt(
            "SELECT t.oid, n.nspname::text, t.relname::text FROM pg_class c \
             JOIN pg_class t ON t.oid = c.reltoastrelid \
             JOIN pg_namespace n ON n.oid = t.relnamespace WHERE c.oid = $1",
            &[&desc.oid],
        )
        .await
        .context("backfill: resolve TOAST heap")?
    else {
        return Ok(0);
    };
    let toast_relid: u32 = row.get(0);
    let (nsp, rel): (String, String) = (row.get(1), row.get(2));
    let sql = format!(
        "COPY (SELECT ctid, chunk_id, chunk_seq, chunk_data FROM {}.{}) TO STDOUT (FORMAT binary)",
        quote_ident(&nsp),
        quote_ident(&rel),
    );
    let copy = client.copy_out(&sql).await.map_err(|e| {
        if e.code() == Some(&tokio_postgres::error::SqlState::INSUFFICIENT_PRIVILEGE) {
            anyhow::anyhow!(
                "backfill: source role cannot read {nsp}.{rel} of {}: seeding the chunk mirror \
                 needs USAGE on schema pg_toast and SELECT on its TOAST heaps; grant \
                 pg_read_all_data to the source role: {e}",
                desc.rel_name,
            )
        } else {
            anyhow::Error::new(e).context("backfill: TOAST COPY out")
        }
    })?;
    let stream = BinaryCopyOutStream::new(copy, &[Type::BYTEA; 4]);
    futures::pin_mut!(stream);
    let mut chunks = 0u64;
    let mut batch: Vec<ToastRow> = Vec::new();
    let mut batch_bytes = 0usize;
    while let Some(row) = stream.next().await {
        let row = row.context("backfill: TOAST COPY stream")?;
        let field = |i: usize| -> anyhow::Result<&[u8]> {
            row.try_get::<Option<&[u8]>>(i)
                .context("backfill: TOAST COPY field")?
                .context("backfill: NULL TOAST column")
        };
        let tid = fixed::<6>(field(0)?, "ctid").map_err(anyhow::Error::msg)?;
        let chunk_data = bytes::Bytes::copy_from_slice(field(3)?);
        batch_bytes += chunk_data.len();
        batch.push(ToastRow {
            toast_relid,
            blkno: u32::from_be_bytes(tid[..4].try_into().expect("4 bytes")),
            offnum: u16::from_be_bytes(tid[4..].try_into().expect("2 bytes")),
            chunk_id: u32::from_be_bytes(fixed(field(1)?, "chunk_id").map_err(anyhow::Error::msg)?),
            chunk_seq: u32::from_be_bytes(
                fixed(field(2)?, "chunk_seq").map_err(anyhow::Error::msg)?,
            ),
            chunk_data,
            lsn,
        });
        chunks += 1;
        if resolver.put_limit_reached(batch.len(), batch_bytes) {
            resolver.put(&batch).await?;
            batch.clear();
            batch_bytes = 0;
        }
    }
    resolver.put(&batch).await?;
    Ok(chunks)
}

fn fixed<const N: usize>(raw: &[u8], what: &str) -> Result<[u8; N], String> {
    raw.try_into()
        .map_err(|_| format!("{what}: expected {N} bytes, got {}", raw.len()))
}

fn text_or_bytea(raw: &[u8], wrap: fn(String) -> ColumnValue) -> ColumnValue {
    std::str::from_utf8(raw).map_or_else(|_| ColumnValue::Bytea(raw.to_vec()), |s| wrap(s.into()))
}

/// Decode non-NULL wire field into heap-decoder value shape
fn decode_field(kind: WireKind, type_oid: u32, raw: &[u8]) -> Result<ColumnValue, String> {
    Ok(match kind {
        WireKind::Bool => ColumnValue::Bool(fixed::<1>(raw, "bool")?[0] != 0),
        WireKind::Char => ColumnValue::Char(fixed::<1>(raw, "char")?[0] as i8),
        WireKind::Int2 => ColumnValue::Int2(i16::from_be_bytes(fixed(raw, "int2")?)),
        WireKind::Int4 => ColumnValue::Int4(i32::from_be_bytes(fixed(raw, "int4")?)),
        WireKind::Int8 => ColumnValue::Int8(i64::from_be_bytes(fixed(raw, "int8")?)),
        WireKind::Oid => ColumnValue::Oid(u32::from_be_bytes(fixed(raw, "oid")?)),
        WireKind::Float4 => ColumnValue::Float4(f32::from_be_bytes(fixed(raw, "float4")?)),
        WireKind::Float8 => ColumnValue::Float8(f64::from_be_bytes(fixed(raw, "float8")?)),
        WireKind::Date => ColumnValue::Date(i32::from_be_bytes(fixed(raw, "date")?)),
        WireKind::Time => ColumnValue::Time(i64::from_be_bytes(fixed(raw, "time")?)),
        WireKind::Timestamp => ColumnValue::Timestamp(i64::from_be_bytes(fixed(raw, "timestamp")?)),
        WireKind::TimestampTz => {
            ColumnValue::TimestampTz(i64::from_be_bytes(fixed(raw, "timestamptz")?))
        }
        WireKind::Uuid => ColumnValue::Uuid(fixed(raw, "uuid")?),
        WireKind::Bytea => ColumnValue::Bytea(raw.to_vec()),
        WireKind::Text => text_or_bytea(raw, ColumnValue::Text),
        // Preserve source type for typinput
        WireKind::CastText => match std::str::from_utf8(raw) {
            Ok(s) => ColumnValue::PgPendingText {
                type_oid,
                text: s.to_owned(),
            },
            Err(_) => ColumnValue::Bytea(raw.to_vec()),
        },
        WireKind::Name => text_or_bytea(raw, ColumnValue::Name),
        WireKind::Json => text_or_bytea(raw, ColumnValue::Json),
        // numeric_out text form; specials carry their flag
        WireKind::NumericText => {
            let s = std::str::from_utf8(raw).map_err(|_| "numeric::text not utf8".to_string())?;
            ColumnValue::Numeric(match s {
                "NaN" => NumericKind::NaN,
                "Infinity" => NumericKind::PInf,
                "-Infinity" => NumericKind::NInf,
                _ => NumericKind::Finite(s.into()),
            })
        }
    })
}

// ---------------------------------------------------------------------------
// Backfiller
// ---------------------------------------------------------------------------

struct Inner {
    ledger: Ledger,
    /// qnames with a task running or queued this boot; stops a re-upserted
    /// config row (or a boot-seed + WAL-replay double-fire) from starting a
    /// second backfill.
    active: HashSet<RelName>,
    /// Backup-mode opt-ins awaiting their coalesce window; a mode's first
    /// enqueue spawns the pass runner, later ones ride the same pass.
    queued: HashMap<InitialLoadMode, Vec<BackupRequest>>,
}

/// Owns the resume ledger and dispatches per-mode backfills: `'copy'` spawns
/// one detached COPY task per table, backup modes coalesce into one
/// [`crate::backfill::backup_backfill`] pass per mode. Shared by the reorder
/// coordinator (live opt-ins) and the boot seed (restart resume /
/// pre-installed config rows / TOML-pinned loads).
pub struct CopyBackfiller {
    /// Boot source endpoint; [`Self::source_pg`] prefers the live one
    pg: PgConfig,
    dest: Arc<DestEmitter>,
    mapping: MappingHandle,
    stats: Arc<EmitterStats>,
    /// Shadow catalog for backup passes: toast-rel descriptors by name
    catalog: Arc<Mutex<ShadowCatalog>>,
    /// Descriptor log for gap-replay record decode
    log: Arc<crate::catalog::desc_log::DescriptorLog>,
    spill_dir: PathBuf,
    /// Live resolved config for the dedicated backfill tails, so backfilled
    /// rows encode under the same `config_column` overrides WAL-driven rows
    /// use. `None` == boot values only.
    config_rx: Option<watch::Receiver<Arc<ResolvedConfig>>>,
    /// Branch the stream proved, for backup passes replaying archived WAL
    history_rx: watch::Receiver<Arc<crate::source::timeline::TimelineHistory>>,
    /// Pipeline's resident-payload pool: backup passes run concurrently
    /// with live streaming and draw from the same budget
    budget: Option<crate::budget::MemoryBudget>,
    oracle: Option<Arc<Oracle>>,
    /// Source PG major: picks the backup's pg_multixact offsets width
    source_major: u32,
    /// A cluster backup walks the pages of the database `[source] dbname`
    /// names, so only that database can load from one
    backup_loads: bool,
    /// Fixed scratch paths require one cluster backup pass at a time
    backup_pass_lock: Mutex<()>,
    inner: Mutex<Inner>,
    /// Ledger entries not yet `done` (gauge; mirrors the ledger under the lock).
    pending: AtomicU64,
    /// Per-mode split of `pending`: copy / base_backup / object_store.
    pending_by_mode: [AtomicU64; 3],
    coalesce_window: Duration,
    /// Pending visibility ledger live apply folds commits into; backup passes
    /// add their tables to this same instance
    pending_rows: SharedPendingLedger,
}

impl CopyBackfiller {
    #[allow(clippy::too_many_arguments)]
    pub async fn new(
        pg: PgConfig,
        dest: Arc<DestEmitter>,
        mapping: MappingHandle,
        stats: Arc<EmitterStats>,
        catalog: Arc<Mutex<ShadowCatalog>>,
        log: Arc<crate::catalog::desc_log::DescriptorLog>,
        spill_dir: &Path,
        config_rx: Option<watch::Receiver<Arc<ResolvedConfig>>>,
        history_rx: watch::Receiver<Arc<crate::source::timeline::TimelineHistory>>,
        budget: Option<crate::budget::MemoryBudget>,
        oracle: Option<Arc<Oracle>>,
        source_major: u32,
        system_id: u64,
        pending_rows: SharedPendingLedger,
    ) -> std::io::Result<Self> {
        let ledger = Ledger::load(spill_dir, system_id).await?;
        let pending = AtomicU64::new(ledger.pending_count());
        let pending_by_mode = [
            AtomicU64::new(ledger.pending_count_for(InitialLoadMode::Copy)),
            AtomicU64::new(ledger.pending_count_for(InitialLoadMode::BaseBackup)),
            AtomicU64::new(ledger.pending_count_for(InitialLoadMode::ObjectStore)),
        ];
        Ok(Self {
            pg,
            dest,
            mapping,
            stats,
            catalog,
            log,
            spill_dir: spill_dir.to_path_buf(),
            config_rx,
            history_rx,
            budget,
            oracle,
            source_major,
            backup_loads: true,
            backup_pass_lock: Mutex::new(()),
            inner: Mutex::new(Inner {
                ledger,
                active: HashSet::new(),
                queued: HashMap::new(),
            }),
            pending,
            pending_by_mode,
            coalesce_window: BACKUP_COALESCE_WINDOW,
            pending_rows,
        })
    }

    /// Ledger entries awaiting backfill completion.
    pub fn pending_count(&self) -> u64 {
        self.pending.load(Ordering::Relaxed)
    }

    /// `pending` split `[copy, base_backup, object_store]`.
    pub fn pending_by_mode(&self) -> [u64; 3] {
        [
            self.pending_by_mode[0].load(Ordering::Relaxed),
            self.pending_by_mode[1].load(Ordering::Relaxed),
            self.pending_by_mode[2].load(Ordering::Relaxed),
        ]
    }

    /// Source endpoint for a backfill's own PG session. `[source]` is
    /// live-reloadable, so a pass starting after a move dials the new address;
    /// an empty live host means no resolver layer supplied one, keep boot's.
    fn source_pg(&self) -> PgConfig {
        let live = self.config_rx.as_ref().map(|rx| rx.borrow().source.clone());
        match live {
            Some(conn) if !conn.host.is_empty() => conn.to_pg_config(),
            _ => self.pg.clone(),
        }
    }

    /// Boot emitter carrying the live CH connection, shared by every session a
    /// pass opens. A backfill's own tail and staging session connect eagerly,
    /// so they take the moved destination at spawn rather than dialling boot's
    /// address and reconnecting off the watch at the first batch.
    /// Live per-relation rules, for destination names the staging promote
    /// has to match. `None` without a resolver (tests): the boot set stands
    fn table_rules(&self) -> Option<Arc<crate::table_rules::TableRules>> {
        self.config_rx.as_ref().map(|rx| rx.borrow().rules.clone())
    }

    fn refresh_gauges(&self, ledger: &Ledger) {
        self.pending
            .store(ledger.pending_count(), Ordering::Relaxed);
        for (i, m) in [
            InitialLoadMode::Copy,
            InitialLoadMode::BaseBackup,
            InitialLoadMode::ObjectStore,
        ]
        .into_iter()
        .enumerate()
        {
            self.pending_by_mode[i].store(ledger.pending_count_for(m), Ordering::Relaxed);
        }
    }

    /// An `initial_load` opt-in applied for a known rel. First sight records
    /// `{S = opt_in_lsn, mode, pending}` durably (the WAL event is not
    /// re-delivered once the ack passes `S`, so the ledger write must precede
    /// the barrier release) and dispatches per mode; a pending entry resumes
    /// its *recorded* mode at its persisted `S`; a `done` entry or an
    /// already-running task no-ops.
    pub async fn note_opt_in(
        self: &Arc<Self>,
        desc: &Arc<RelDescriptor>,
        mode: InitialLoadMode,
        opt_in_lsn: u64,
    ) {
        if mode == InitialLoadMode::None {
            return;
        }
        let rel = desc.rel_name.clone();
        let (s_lsn, mode, spawn_pass, resume) = {
            let mut inner = self.inner.lock().await;
            let (s_lsn, mode) = match inner.ledger.entries.get(&rel) {
                Some(rec) if rec.done => return,
                // Boot re-runs the recorded mode at the recorded S; the
                // config row's current mode applies only to a fresh entry
                Some(rec) => (rec.s_lsn, rec.mode),
                None => {
                    inner.ledger.entries.insert(
                        rel.clone(),
                        LedgerRec {
                            s_lsn: Pos::new(opt_in_lsn),
                            done: false,
                            mode,
                            swapped: false,
                            staging_uuid: None,
                            copy: None,
                            toast_seeded: false,
                        },
                    );
                    if let Err(e) = inner.ledger.persist().await {
                        tracing::warn!(
                            target: "walshadow::backfill",
                            qname = %rel,
                            error = %e,
                            "backfill ledger persist failed; a crash before completion re-streams without backfill",
                        );
                    }
                    (Pos::new(opt_in_lsn), mode)
                }
            };
            if !inner.active.insert(rel.clone()) {
                return;
            }
            self.refresh_gauges(&inner.ledger);
            // Swapped entry: pass rows already durable, exchange issued or
            // withheld — resume the swap tail, never re-load (the staging
            // name may hold the only copy of the live-window rows)
            let resume = inner
                .ledger
                .entries
                .get(&rel)
                .filter(|r| r.swapped)
                .cloned();
            let mut spawn_pass = false;
            if resume.is_none()
                && matches!(
                    mode,
                    InitialLoadMode::BaseBackup | InitialLoadMode::ObjectStore
                )
            {
                let q = inner.queued.entry(mode).or_default();
                // First request of a window owns spawning the pass runner
                spawn_pass = q.is_empty();
                q.push(BackupRequest {
                    desc: desc.clone(),
                    s_lsn: s_lsn.get(),
                });
            }
            (s_lsn, mode, spawn_pass, resume)
        };
        if let Some(rec) = resume {
            let this = self.clone();
            tokio::spawn(async move { this.resume_swap(rel, rec).await });
            return;
        }
        match mode {
            InitialLoadMode::None => {}
            InitialLoadMode::Copy => {
                // Caller releases live apply past `S` once this returns
                if let Err(e) = self.seed_toast(desc, s_lsn).await {
                    tracing::error!(
                        target: "walshadow::backfill",
                        qname = %rel,
                        error = %format!("{e:#}"),
                        "backfill failed; entry stays pending (re-COPY on next boot)",
                    );
                    let mut inner = self.inner.lock().await;
                    inner.active.remove(&rel);
                    self.refresh_gauges(&inner.ledger);
                    return;
                }
                let this = self.clone();
                let desc = desc.clone();
                tokio::spawn(async move { this.run(desc, s_lsn).await });
            }
            InitialLoadMode::BaseBackup | InitialLoadMode::ObjectStore => {
                if spawn_pass {
                    let this = self.clone();
                    tokio::spawn(async move { this.run_backup_pass(mode).await });
                }
            }
        }
    }

    /// A cluster backup covers one database, so every other one loads by COPY
    pub fn with_backup_loads(mut self, allowed: bool) -> Self {
        self.backup_loads = allowed;
        self
    }

    /// Opt-out / row removal: drop the ledger entry so a later re-insert
    /// re-triggers a fresh backfill. A COPY/walk already in flight drains
    /// against the shared routing map, so its remaining rows skip once the
    /// mapping is gone; its completion mark no-ops (entry absent). A queued
    /// backup request is withdrawn before its pass fires.
    pub async fn note_opt_out(&self, rel: &RelName) {
        let mut inner = self.inner.lock().await;
        for q in inner.queued.values_mut() {
            if let Some(i) = q.iter().position(|r| r.desc.rel_name == *rel) {
                q.swap_remove(i);
                inner.active.remove(rel);
                break;
            }
        }
        if inner.ledger.entries.remove(rel).is_some()
            && let Err(e) = inner.ledger.persist().await
        {
            tracing::warn!(
                target: "walshadow::backfill",
                qname = %rel,
                error = %e,
                "backfill ledger persist failed on opt-out",
            );
        }
        self.refresh_gauges(&inner.ledger);
    }

    /// Coalesced backup pass: wait out the window, drain the mode's queue,
    /// run one cluster-sized pass for every queued rel. Regime A: a failed
    /// pass falls back to COPY when enabled, otherwise stays pending.
    /// Failure never poisons the pump.
    async fn run_backup_pass(self: Arc<Self>, mode: InitialLoadMode) {
        tokio::time::sleep(self.coalesce_window).await;
        let reqs: Vec<BackupRequest> = {
            let mut inner = self.inner.lock().await;
            inner.queued.remove(&mode).unwrap_or_default()
        };
        if reqs.is_empty() {
            return;
        }
        let outcome = {
            let _pass = self.backup_pass_lock.lock().await;
            self.staged_pass(mode, &reqs).await
        };
        match outcome {
            Ok(outcome) => {
                tracing::info!(
                    target: "walshadow::backfill",
                    mode = mode.as_str(),
                    tables = reqs.len(),
                    rows_walked = outcome.counts.walked,
                    rows_gated = outcome.counts.gated,
                    rows_deferred = outcome.counts.deferred,
                    rows_pending = outcome.rows_pending,
                    pending_tables = outcome.pending_tables.len(),
                    multixact_emitted = outcome.counts.multixact,
                    rows_replayed = outcome.rows_replayed,
                    replay_commits_past_s = outcome.replay_commits_past_s,
                    gap_segments = outcome.gap_segments,
                    pg_xact_segments = outcome.counts.pg_xact_segments,
                    pg_xact_patch = outcome.pg_xact_patch_len,
                    b_redo = %format_pg_lsn(outcome.b_redo),
                    "backup backfill pass complete",
                );
            }
            Err(e) => {
                tracing::error!(
                    target: "walshadow::backfill",
                    mode = mode.as_str(),
                    tables = reqs.len(),
                    error = %format!("{e:#}"),
                    "backup backfill pass failed",
                );
                if self.dest.base().bootstrap.copy_fallback.unwrap_or(true) {
                    for req in &reqs {
                        if self.prepare_copy_fallback(mode, req).await {
                            self.clone()
                                .run(req.desc.clone(), Pos::new(req.s_lsn))
                                .await;
                        }
                    }
                }
            }
        }
        let mut inner = self.inner.lock().await;
        for r in &reqs {
            if inner
                .ledger
                .entries
                .get(&r.desc.rel_name)
                .is_none_or(|rec| rec.mode == mode && rec.s_lsn.get() == r.s_lsn)
            {
                inner.active.remove(&r.desc.rel_name);
            }
        }
        self.refresh_gauges(&inner.ledger);
    }

    async fn prepare_copy_fallback(&self, mode: InitialLoadMode, req: &BackupRequest) -> bool {
        let mut inner = self.inner.lock().await;
        let rel = &req.desc.rel_name;
        match inner.ledger.fallback_to_copy(rel, mode, req.s_lsn).await {
            Ok(true) => {}
            Ok(false) => return false,
            Err(e) => {
                tracing::error!(
                    target: "walshadow::backfill",
                    qname = %rel,
                    error = %e,
                    "COPY fallback ledger persist failed; entry stays pending",
                );
                return false;
            }
        }
        self.refresh_gauges(&inner.ledger);
        tracing::warn!(
            target: "walshadow::backfill",
            qname = %rel,
            previous_mode = mode.as_str(),
            s_lsn = %format_pg_lsn(req.s_lsn),
            "retrying failed backup load through COPY",
        );
        true
    }

    /// Staged pass (architecture/bootstrap.md): rows land in per-rel
    /// staging tables, success publishes each rel via EXCHANGE + copy-back.
    /// Per-rel ledger transitions ride [`Self::publish_staged`]; this frame
    /// only reports the load itself.
    async fn staged_pass(
        &self,
        mode: InitialLoadMode,
        reqs: &[BackupRequest],
    ) -> anyhow::Result<PassOutcome> {
        let emitter = self.dest.current();
        let scratch_dir = self.spill_dir.join("backup_backfill");
        let config = self.config_rx.as_ref().map(|rx| rx.borrow().clone());
        let mut checkpoint = BackupCheckpoint::new(
            mode,
            reqs,
            &self.mapping.snapshot().await,
            &emitter,
            config.as_deref(),
        );
        // Mid-walk progress names files whose rows are only in staging, so it
        // keeps the tables just as a completed walk does
        let resumed = match BackupCheckpoint::load(&scratch_dir).await? {
            Some(saved) if saved.matches(&checkpoint) && saved.resuming() => {
                let mut session =
                    backfill_staging::StagingSession::connect(self.dest.clone()).await?;
                saved.staging_intact(&mut session).await?.then_some(saved)
            }
            _ => None,
        };
        match resumed {
            Some(saved) => checkpoint = saved,
            None => BackupCheckpoint::discard(&scratch_dir).await?,
        }
        let resuming = checkpoint.resuming();
        let staging = backfill_staging::prepare(self.dest.clone(), &self.mapping, reqs, resuming)
            .await
            .context("staging prepare")?;
        if !resuming {
            let mut session = backfill_staging::StagingSession::connect(self.dest.clone()).await?;
            checkpoint.capture_staging(&staging, &mut session).await?;
        }
        let ctx = PassContext {
            pg: self.source_pg(),
            dest: self.dest.clone(),
            mapping: staging.mapping.clone(),
            published: self.mapping.clone(),
            stats: self.stats.clone(),
            catalog: self.catalog.clone(),
            log: self.log.clone(),
            scratch_dir,
            config_rx: self.config_rx.clone(),
            history_rx: self.history_rx.clone(),
            budget: self.budget.clone(),
            oracle: self.oracle.clone(),
            source_major: self.source_major,
            checkpoint,
        };
        let outcome = crate::backfill::backup_backfill::run_pass(&ctx, mode, reqs).await?;
        self.publish_staged(&staging, reqs).await;
        self.record_pending(&outcome).await;
        BackupCheckpoint::discard(&ctx.scratch_dir).await?;
        tokio::fs::remove_file(ctx.scratch_dir.join("bootstrap_deferred.bin"))
            .await
            .ok();
        Ok(outcome)
    }

    /// Record after publication so EXCHANGE cannot discard promoted rows.
    /// Persist retries rather than failing: the backfill entry is already
    /// done, so this ledger alone names the pending tables, and every live
    /// settle persists the shared instance too
    async fn record_pending(&self, outcome: &PassOutcome) {
        if outcome.pending_tables.is_empty() {
            return;
        }
        {
            let mut ledger = self.pending_rows.lock().await;
            for m in &outcome.pending_tables {
                ledger.stage(m);
            }
        }
        let mut backoff = Duration::from_millis(100);
        while let Err(e) = self.pending_rows.lock().await.persist().await {
            tracing::error!(
                target: "walshadow::backfill",
                error = %e,
                retry_in = ?backoff,
                "pending visibility ledger persist failed; retrying",
            );
            tokio::time::sleep(backoff).await;
            backoff = (backoff * 2).min(Duration::from_secs(30));
        }
        if let Err(e) = self.settle_ended_pending().await {
            tracing::error!(
                target: "walshadow::backfill",
                error = %format!("{e:#}"),
                "pending visibility source check failed; rows promote as their xids end \
                 or at next boot",
            );
        }
    }

    /// Live apply folds only outcomes arriving after an entry exists. Xids
    /// ending between the pass's replay cut and [`Self::record_pending`]
    /// went by already, so source `pg_xact` decides them
    async fn settle_ended_pending(&self) -> anyhow::Result<()> {
        let xids: Vec<u32> = self
            .pending_rows
            .lock()
            .await
            .outstanding()
            .into_iter()
            .collect();
        if xids.is_empty() {
            return Ok(());
        }
        let client = open_sql_client(&self.source_pg())
            .await
            .context("pending visibility: source sql connect")?;
        let ended = xid_outcomes(&client, &xids).await?;
        if ended.is_empty() {
            return Ok(());
        }
        // Connect before locking: live apply folds every commit under it
        let mut sess = StagingSession::connect(self.dest.clone()).await?;
        let mut ledger = self.pending_rows.lock().await;
        let mut settled = 0;
        for (xid, committed) in ended {
            settled += ledger.note(xid, &[], committed);
        }
        if settled == 0 {
            return Ok(());
        }
        self.stats
            .pending_xacts_settled
            .fetch_add(settled, Ordering::Relaxed);
        visibility_pending::settle(&mut ledger, &mut sess, &self.stats)
            .await
            .map_err(anyhow::Error::msg)
    }

    /// Publish a successful pass. Per rel: schema-equality gate, persist
    /// `swapped` + staging uuid, EXCHANGE; then one straggler wait; then
    /// copy-back + drop + done. A rel failing a step stays pending or
    /// swapped — boot resumes the right phase.
    async fn publish_staged(&self, plan: &StagingPlan, reqs: &[BackupRequest]) {
        // Rels skipped at prepare had no mapping, so their rows had nowhere
        // to route; mark done exactly as an unstaged pass would have
        let staged: HashSet<&RelName> = plan.rels.iter().map(|r| &r.rel).collect();
        for r in reqs {
            if !staged.contains(&r.desc.rel_name) {
                self.mark_done_entry(&r.desc.rel_name).await;
            }
        }
        if plan.rels.is_empty() {
            return;
        }
        let mut sess = match StagingSession::connect(self.dest.clone())
            .await
            .map(|s| s.with_rules(self.table_rules()))
        {
            Ok(s) => s,
            Err(e) => {
                tracing::error!(
                    target: "walshadow::backfill",
                    error = %format!("{e:#}"),
                    "staging publish connect failed; entries stay pending",
                );
                return;
            }
        };
        let mut swapped: Vec<&StagingRel> = Vec::with_capacity(plan.rels.len());
        for rel in &plan.rels {
            match self.swap_rel(&mut sess, rel).await {
                Ok(true) => swapped.push(rel),
                // Withdrawn (opt-out mid-pass) or discarded (DDL moved the
                // destination shape); already logged
                Ok(false) => {}
                Err(e) => {
                    tracing::error!(
                        target: "walshadow::backfill",
                        qname = %rel.rel,
                        error = %format!("{e:#}"),
                        "staging swap failed; boot resumes from the recorded phase",
                    );
                }
            }
        }
        if swapped.is_empty() {
            return;
        }
        // In-flight live INSERTs that resolved the pre-swap storage finish
        // within one attempt cap (later attempts re-resolve the name to the
        // swapped-in table); copy-back may start only once they've landed
        tokio::time::sleep(self.dest.current().insert_timeout).await;
        for rel in swapped {
            if let Err(e) = self.finish_swapped(&mut sess, rel).await {
                tracing::error!(
                    target: "walshadow::backfill",
                    qname = %rel.rel,
                    error = %format!("{e:#}"),
                    "staging copy-back failed; entry stays swapped (boot resumes)",
                );
            }
        }
    }

    /// `Ok(true)` = exchanged. `Ok(false)` = load discarded: rel withdrawn,
    /// or DDL moved the destination shape mid-pass (the loaded copy has the
    /// pre-DDL shape; entry stays pending, next boot re-loads).
    async fn swap_rel(&self, sess: &mut StagingSession, rel: &StagingRel) -> anyhow::Result<bool> {
        if !self.mapping.with(|m| m.contains_key(&rel.rel)).await {
            sess.drop_staging(rel).await?;
            tracing::info!(
                target: "walshadow::backfill",
                qname = %rel.rel,
                "rel unmapped at publish (opt-out mid-pass); staging discarded",
            );
            return Ok(false);
        }
        let real_fp = sess.schema_fingerprint(&rel.database, &rel.table).await?;
        let staging_fp = sess
            .schema_fingerprint(&rel.database, &rel.staging_table())
            .await?;
        if real_fp != staging_fp {
            sess.drop_staging(rel).await?;
            tracing::warn!(
                target: "walshadow::backfill",
                qname = %rel.rel,
                "destination schema changed mid-pass; staging discarded, entry stays pending",
            );
            return Ok(false);
        }
        let uuid = sess
            .table_uuid(&rel.database, &rel.staging_table())
            .await?
            .context("staging table missing before exchange")?;
        // Persist precedes EXCHANGE: post-swap the staging name holds the
        // only copy of the live-window rows, and a pending-looking entry
        // would re-run the pass and rebuild staging over it
        if !self.mark_swapped(&rel.rel, &uuid).await {
            anyhow::bail!("ledger persist failed; exchange withheld");
        }
        crate::ops::stages::PUBLISH
            .measure(sess.exchange(rel))
            .await?;
        Ok(true)
    }

    async fn finish_swapped(
        &self,
        sess: &mut StagingSession,
        rel: &StagingRel,
    ) -> anyhow::Result<()> {
        let timing = crate::ops::stages::SETTLE.start();
        sess.copy_back(rel).await?;
        sess.drop_staging(rel).await?;
        self.mark_done_entry(&rel.rel).await;
        timing.finish();
        Ok(())
    }

    /// Boot resume for a `swapped` entry. The staging name's uuid tells the
    /// phase apart: unchanged = exchange never applied (staging still holds
    /// the load), changed = exchange applied (staging holds the pre-swap
    /// storage), missing = copy-back + drop ran, only the done mark is owed.
    async fn resume_swap(self: Arc<Self>, name: RelName, rec: LedgerRec) {
        if let Err(e) = self.resume_swap_inner(&name, &rec).await {
            tracing::error!(
                target: "walshadow::backfill",
                qname = %name,
                error = %format!("{e:#}"),
                "swap resume failed; entry stays swapped (retry next boot)",
            );
        }
        let mut inner = self.inner.lock().await;
        inner.active.remove(&name);
        self.refresh_gauges(&inner.ledger);
    }

    async fn resume_swap_inner(&self, name: &RelName, rec: &LedgerRec) -> anyhow::Result<()> {
        let target = self
            .mapping
            .with(|m| m.get(name).map(|t| t.target.clone()))
            .await
            .with_context(|| format!("swapped entry {name} unmapped; staging table orphaned"))?;
        let rel = StagingRel {
            rel: name.clone(),
            database: target.database,
            table: target.table,
            s_lsn: rec.s_lsn.get(),
        };
        let mut sess = StagingSession::connect(self.dest.clone())
            .await?
            .with_rules(self.table_rules());
        match sess.table_uuid(&rel.database, &rel.staging_table()).await? {
            None => {
                self.mark_done_entry(name).await;
                return Ok(());
            }
            Some(u) if Some(&u) == rec.staging_uuid.as_ref() => {
                // Schema may have moved while down — same gate as the pass
                let real_fp = sess.schema_fingerprint(&rel.database, &rel.table).await?;
                let staging_fp = sess
                    .schema_fingerprint(&rel.database, &rel.staging_table())
                    .await?;
                if real_fp != staging_fp {
                    sess.drop_staging(&rel).await?;
                    self.clear_swapped(name).await;
                    anyhow::bail!(
                        "destination schema changed before exchange; load discarded, entry re-pends"
                    );
                }
                sess.exchange(&rel).await?;
            }
            Some(_) => {}
        }
        tokio::time::sleep(self.dest.current().insert_timeout).await;
        sess.copy_back(&rel).await?;
        sess.drop_staging(&rel).await?;
        self.mark_done_entry(name).await;
        Ok(())
    }

    /// `false` (caller must not exchange) when the entry vanished (opt-out
    /// raced the publish) or the persist failed.
    async fn mark_swapped(&self, name: &RelName, uuid: &str) -> bool {
        let mut inner = self.inner.lock().await;
        let Some(rec) = inner.ledger.entries.get_mut(name) else {
            return false;
        };
        rec.swapped = true;
        rec.staging_uuid = Some(uuid.to_owned());
        if let Err(e) = inner.ledger.persist().await {
            tracing::warn!(
                target: "walshadow::backfill",
                qname = %name,
                error = %e,
                "ledger persist failed; exchange withheld, entry stays pending",
            );
            let rec = inner.ledger.entries.get_mut(name).expect("just present");
            rec.swapped = false;
            rec.staging_uuid = None;
            return false;
        }
        true
    }

    async fn mark_done_entry(&self, name: &RelName) {
        let mut inner = self.inner.lock().await;
        if let Some(rec) = inner.ledger.entries.get_mut(name) {
            rec.done = true;
            rec.swapped = false;
            rec.staging_uuid = None;
            rec.copy = None;
            if let Err(e) = inner.ledger.persist().await {
                tracing::warn!(
                    target: "walshadow::backfill",
                    qname = %name,
                    error = %e,
                    "backfill ledger persist failed; next boot resumes (idempotent)",
                );
            }
        }
        self.refresh_gauges(&inner.ledger);
    }

    async fn clear_swapped(&self, name: &RelName) {
        let mut inner = self.inner.lock().await;
        if let Some(rec) = inner.ledger.entries.get_mut(name) {
            rec.swapped = false;
            rec.staging_uuid = None;
            if let Err(e) = inner.ledger.persist().await {
                tracing::warn!(
                    target: "walshadow::backfill",
                    qname = %name,
                    error = %e,
                    "backfill ledger persist failed on swap clear",
                );
            }
        }
        self.refresh_gauges(&inner.ledger);
    }

    async fn run(self: Arc<Self>, desc: Arc<RelDescriptor>, s_lsn: Pos<Snapshot>) {
        let res = self.copy_once(&desc, s_lsn).await;
        let dropped = res.is_err() && self.source_dropped(desc.oid).await;
        let mut inner = self.inner.lock().await;
        inner.active.remove(&desc.rel_name);
        match res {
            Err(e) if !dropped => {
                // Regime A: a failed backfill never poisons the pump. Entry
                // stays pending; the next boot's seed re-issues COPY at same S
                tracing::error!(
                    target: "walshadow::backfill",
                    qname = %desc.rel_name,
                    error = %format!("{e:#}"),
                    "backfill failed; entry stays pending (re-COPY on next boot)",
                );
            }
            res => {
                if let Some(entry) = inner.ledger.entries.get_mut(&desc.rel_name)
                    && entry.s_lsn == s_lsn
                {
                    entry.done = true;
                    entry.copy = None;
                    if let Err(e) = inner.ledger.persist().await {
                        tracing::warn!(
                            target: "walshadow::backfill",
                            qname = %desc.rel_name,
                            error = %e,
                            "backfill ledger persist failed; next boot re-COPYs (idempotent)",
                        );
                    }
                }
                if let Ok(outcome) = res {
                    tracing::info!(
                        target: "walshadow::backfill",
                        qname = %desc.rel_name,
                        rows = outcome.rows,
                        s_lsn = %s_lsn,
                        p_hi = %format_pg_lsn(outcome.p_hi),
                        copied = !outcome.skipped_empty,
                        "backfill complete; converged once WAL apply passes p_hi",
                    );
                } else {
                    tracing::info!(
                        target: "walshadow::backfill",
                        qname = %desc.rel_name,
                        "source table dropped; skipping backfill",
                    );
                }
            }
        }
        self.refresh_gauges(&inner.ledger);
    }

    /// Probe failure reads as present, so entry stays pending
    async fn source_dropped(&self, oid: u32) -> bool {
        let Ok(client) = self.open_copy_client().await else {
            return false;
        };
        client
            .query_one(
                "SELECT NOT EXISTS (SELECT 1 FROM pg_class WHERE oid = $1)",
                &[&oid],
            )
            .await
            .is_ok_and(|row| row.get(0))
    }

    async fn copy_once(
        &self,
        desc: &Arc<RelDescriptor>,
        s_lsn: Pos<Snapshot>,
    ) -> anyhow::Result<CopyOutcome> {
        let qtable = format!(
            "{}.{}",
            quote_ident(&desc.rel_name.namespace),
            quote_ident(&desc.rel_name.name)
        );
        self.seed_toast(desc, s_lsn).await?;
        let client = self.open_copy_client().await?;

        // Empty table ⇒ streaming alone suffices, skip COPY + tail entirely
        let nonempty: bool = client
            .query_one(&format!("SELECT EXISTS (SELECT 1 FROM ONLY {qtable})"), &[])
            .await
            .context("backfill: emptiness probe")?
            .get(0);
        if !nonempty {
            let p_hi = current_wal_lsn(&client).await?;
            return Ok(CopyOutcome {
                rows: 0,
                skipped_empty: true,
                p_hi,
            });
        }

        let rows = self.copy_chunks(&client, desc, s_lsn).await?;
        // Upper bound on the COPY snapshot; WAL apply past it = converged
        let p_hi = current_wal_lsn(&client).await?;
        Ok(CopyOutcome {
            rows,
            skipped_empty: false,
            p_hi,
        })
    }

    /// Walk the relation in heap-block chunks, persisting the cursor once each
    /// chunk's rows are durable. A rewrite between chunks relocates rows, so
    /// the cursor it invalidates restarts the table rather than skipping pages
    async fn copy_chunks(
        &self,
        client: &tokio_postgres::Client,
        desc: &Arc<RelDescriptor>,
        s_lsn: Pos<Snapshot>,
    ) -> anyhow::Result<u64> {
        let chunk = self
            .dest
            .current()
            .bootstrap
            .copy_chunk_blocks
            .map_or(COPY_CHUNK_BLOCKS, NonZeroU32::get);
        // A rewrite is the only way past the first pass, and it changes the
        // filenode, so the cursor a restart reads never matches twice
        for _ in 0..2 {
            let (relfilenode, blocks) = relation_extent(client, desc.oid).await?;
            let mut start = self
                .copy_cursor(&desc.rel_name)
                .await
                .filter(|c| c.relfilenode == relfilenode)
                .map_or(0, |c| c.next_block);
            if start > 0 {
                tracing::info!(
                    target: "walshadow::backfill",
                    qname = %desc.rel_name,
                    start_block = start,
                    blocks,
                    "resuming COPY from persisted cursor",
                );
            }
            let mut rows = 0;
            loop {
                let end = start.checked_add(chunk).filter(|e| *e < blocks);
                rows += self
                    .copy_chunk(client, desc, s_lsn, BlockRange { start, end })
                    .await?;
                let Some(end) = end else { return Ok(rows) };
                if relation_extent(client, desc.oid).await?.0 != relfilenode {
                    tracing::warn!(
                        target: "walshadow::backfill",
                        qname = %desc.rel_name,
                        "relation rewritten mid-COPY; restarting from first block",
                    );
                    break;
                }
                start = end;
                self.note_copy_progress(
                    &desc.rel_name,
                    CopyCursor {
                        relfilenode,
                        next_block: start,
                    },
                )
                .await;
            }
        }
        anyhow::bail!("backfill: relation rewritten twice during COPY")
    }

    /// One chunk on its own tail: proving `finish` is what licenses the cursor
    /// write, so each chunk owns a seq space a restart can discard whole
    async fn copy_chunk(
        &self,
        client: &tokio_postgres::Client,
        desc: &Arc<RelDescriptor>,
        s_lsn: Pos<Snapshot>,
        range: BlockRange,
    ) -> anyhow::Result<u64> {
        // Dedicated tail: own CH connection, own seq space, own fatal.
        let tail = OwnedTail::spawn(
            self.dest.clone(),
            1,
            self.stats.clone(),
            Fatal::new(),
            self.config_rx.clone(),
            self.oracle.clone(),
            "backfill",
        )
        .await
        .map_err(anyhow::Error::msg)?;

        let mut catalog = CatalogMap::new();
        catalog.insert(desc.clone());
        let (tup_tx, tup_rx) = mpsc::channel::<Vec<BackfillTuple>>(BOOTSTRAP_TUPLE_CHANNEL_CAP);
        let drain = tokio::spawn(bootstrap::drain(
            tup_rx,
            catalog,
            self.mapping.snapshot().await,
            tail.msg_tx.clone(),
            tail.ack.clone(),
            self.stats.clone(),
            ToastResolver::disabled(),
            bootstrap::Deferral::Rejected,
            self.dest.current().row_policy(),
            self.config_rx.as_ref().map(|rx| rx.borrow().clone()),
            HashSet::new(),
            None,
        ));

        let copied = crate::ops::stages::COPY
            .measure(copy_rows_into(
                client,
                desc,
                s_lsn.get(),
                &tup_tx,
                &self.stats,
                range,
            ))
            .await;
        drop(tup_tx);
        let outcome = drain
            .await
            .context("backfill: drain join")?
            .map_err(anyhow::Error::msg);
        let (rows, outcome) = match (copied, outcome) {
            (Ok(rows), Ok(outcome)) => (rows, outcome),
            (Err(e), _) | (_, Err(e)) => {
                tail.quiesce().await;
                return Err(e);
            }
        };
        crate::ops::stages::INSERT_FLUSH
            .measure(tail.finish(outcome.next_seq))
            .await
            .map_err(anyhow::Error::msg)?;
        Ok(rows)
    }

    async fn open_copy_client(&self) -> anyhow::Result<tokio_postgres::Client> {
        let client = open_sql_client(&self.source_pg())
            .await
            .context("backfill: source sql connect")?;
        client
            .batch_execute(COPY_SESSION_SETUP)
            .await
            .context("backfill: source session setup")?;
        Ok(client)
    }

    /// Copy the TOAST heap into the chunk mirror once per load, in
    /// `clickhouse` value mode only: shadow mode reads values from shadow,
    /// disabled mode keeps none
    async fn seed_toast(&self, desc: &RelDescriptor, s_lsn: Pos<Snapshot>) -> anyhow::Result<()> {
        let emitter = self.dest.current();
        if emitter.toast.mode != ToastMode::Clickhouse || desc.toast_oid == 0 {
            return Ok(());
        }
        let seeded = {
            let inner = self.inner.lock().await;
            inner
                .ledger
                .entries
                .get(&desc.rel_name)
                .is_none_or(|r| r.toast_seeded)
        };
        if seeded {
            return Ok(());
        }
        let resolver = ToastResolver::from_config(self.dest.clone(), self.stats.clone());
        let client = self.open_copy_client().await?;
        let chunks = copy_toast_into(&client, &resolver, desc, s_lsn.get()).await?;
        tracing::info!(
            target: "walshadow::backfill",
            qname = %desc.rel_name,
            chunks,
            "chunk mirror seeded from source TOAST heap",
        );
        let mut inner = self.inner.lock().await;
        if let Some(rec) = inner.ledger.entries.get_mut(&desc.rel_name) {
            rec.toast_seeded = true;
            // Loss only repeats the seed, idempotent at equal TID and version
            if let Err(e) = inner.ledger.persist().await {
                tracing::warn!(
                    target: "walshadow::backfill",
                    qname = %desc.rel_name,
                    error = %e,
                    "backfill ledger persist failed; restart re-seeds the chunk mirror",
                );
            }
        }
        Ok(())
    }

    async fn copy_cursor(&self, rel: &RelName) -> Option<CopyCursor> {
        self.inner.lock().await.ledger.entries.get(rel)?.copy
    }

    /// Cursor loss only costs a repeated chunk, so a failed persist logs
    async fn note_copy_progress(&self, rel: &RelName, cursor: CopyCursor) {
        let mut inner = self.inner.lock().await;
        if let Err(e) = inner.ledger.note_copy(rel, cursor).await {
            tracing::warn!(
                target: "walshadow::backfill",
                qname = %rel,
                error = %e,
                "COPY cursor persist failed; restart repeats the chunk",
            );
        }
    }
}

/// Ended xids among `xids` with their outcome, `true` for commit. Source
/// omits xids still running and ones its `pg_xact` no longer holds
pub async fn xid_outcomes(
    client: &tokio_postgres::Client,
    xids: &[u32],
) -> anyhow::Result<Vec<(u32, bool)>> {
    let wide: Vec<i64> = xids.iter().copied().map(i64::from).collect();
    let rows = client
        .query(XID_STATUS_SQL, &[&wide])
        .await
        .context("source xid status")?;
    Ok(rows
        .iter()
        .filter_map(|row| {
            let committed = match row.get::<_, Option<&str>>(1)? {
                "committed" => true,
                "aborted" => false,
                _ => return None,
            };
            Some((row.get::<_, i64>(0) as u32, committed))
        })
        .collect())
}

/// Current filenode and heap block count. Both come from one read so the
/// count belongs to the filenode the chunk loop pins
async fn relation_extent(client: &tokio_postgres::Client, oid: u32) -> anyhow::Result<(u32, u32)> {
    let row = client
        .query_one(
            &format!(
                "SELECT relfilenode, \
                 (pg_relation_size(oid) / current_setting('block_size')::int8)::int8 \
                 FROM pg_class WHERE oid = {oid}"
            ),
            &[],
        )
        .await
        .context("backfill: relation extent")?;
    let blocks: i64 = row.get(1);
    Ok((
        row.get::<_, u32>(0),
        u32::try_from(blocks).unwrap_or(u32::MAX),
    ))
}

#[async_trait]
impl Backfiller for CopyBackfiller {
    async fn note_opt_in(
        self: Arc<Self>,
        desc: Arc<RelDescriptor>,
        mode: InitialLoadMode,
        opt_in_lsn: u64,
    ) {
        CopyBackfiller::note_opt_in(&self, &desc, mode, opt_in_lsn).await;
    }

    async fn note_opt_out(&self, rel: &RelName) {
        CopyBackfiller::note_opt_out(self, rel).await;
    }

    fn refuses(&self, mode: InitialLoadMode) -> Option<&'static str> {
        let from_backup = matches!(
            mode,
            InitialLoadMode::BaseBackup | InitialLoadMode::ObjectStore
        );
        (from_backup && !self.backup_loads).then_some(
            "a cluster backup covers only the database `[source] dbname` names, use \"copy\"",
        )
    }
}

struct CopyOutcome {
    rows: u64,
    skipped_empty: bool,
    p_hi: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::schema::{RelAttr, RelName, ReplIdent};
    use walrus::pg::walparser::RelFileNode;

    fn attr(attnum: i16, name: &str, type_oid: u32, dropped: bool) -> RelAttr {
        RelAttr {
            attnum,
            name: name.into(),
            type_oid,
            typmod: -1,
            not_null: false,
            dropped,
            type_name: String::new(),
            type_byval: true,
            type_len: 8,
            type_align: 'd',
            type_storage: 'p',
            missing_default: None,
        }
    }

    fn desc(attrs: Vec<RelAttr>) -> RelDescriptor {
        RelDescriptor {
            rfn: RelFileNode {
                spc_node: 1663,
                db_node: 5,
                rel_node: 16400,
            },
            oid: 16400,
            toast_oid: 0,
            namespace_oid: 2200,
            rel_name: RelName::new("app", "orders"),
            kind: 'r',
            persistence: 'p',
            replident: ReplIdent::Default { pk_attnums: None },
            attributes: attrs,
        }
    }

    #[test]
    fn plan_casts_out_of_matrix_and_skips_dropped() {
        let d = desc(vec![
            attr(1, "id", INT8OID, false),
            attr(2, "gone", TEXTOID, true),
            attr(3, "price", NUMERICOID, false),
            attr(4, "tags", 1009, false), // text[] — out of matrix
            attr(5, "doc", JSONBOID, false),
        ]);
        let plan = column_plan(&d);
        assert_eq!(
            plan.select,
            "\"id\", \"price\"::text, \"tags\"::text, \"doc\"::text"
        );
        assert_eq!(plan.natts, 5);
        assert_eq!(plan.cols.len(), 4, "dropped column not selected");
        assert_eq!(plan.cols[0].kind, WireKind::Int8);
        assert_eq!(plan.cols[1].kind, WireKind::NumericText);
        assert_eq!(plan.cols[2].kind, WireKind::CastText);
        assert_eq!(plan.cols[2].attnum, 4);
        // jsonb_out text, the document shape the WAL path yields
        assert_eq!(plan.cols[3].kind, WireKind::Json);
    }

    #[test]
    fn decode_matches_wal_variants() {
        assert_eq!(
            decode_field(WireKind::Int8, 0, &7i64.to_be_bytes()).unwrap(),
            ColumnValue::Int8(7),
        );
        assert_eq!(
            decode_field(WireKind::Bool, 0, &[1]).unwrap(),
            ColumnValue::Bool(true),
        );
        assert_eq!(
            decode_field(WireKind::Date, 0, &8000i32.to_be_bytes()).unwrap(),
            ColumnValue::Date(8000),
        );
        assert_eq!(
            decode_field(WireKind::Text, 0, b"abc").unwrap(),
            ColumnValue::Text("abc".into()),
        );
        assert_eq!(
            decode_field(WireKind::Name, 0, b"orders").unwrap(),
            ColumnValue::Name("orders".into()),
        );
        assert_eq!(
            decode_field(WireKind::Json, 0, b"{\"a\": 1}").unwrap(),
            ColumnValue::Json("{\"a\": 1}".into()),
        );
        assert_eq!(
            decode_field(WireKind::NumericText, 0, b"12.50").unwrap(),
            ColumnValue::Numeric(NumericKind::Finite("12.50".into())),
        );
        assert_eq!(
            decode_field(WireKind::NumericText, 0, b"NaN").unwrap(),
            ColumnValue::Numeric(NumericKind::NaN),
        );
        assert_eq!(
            decode_field(WireKind::NumericText, 0, b"Infinity").unwrap(),
            ColumnValue::Numeric(NumericKind::PInf),
        );
        assert_eq!(
            decode_field(WireKind::NumericText, 0, b"-Infinity").unwrap(),
            ColumnValue::Numeric(NumericKind::NInf),
        );
        assert_eq!(
            decode_field(WireKind::CastText, 1007, b"{1,2}").unwrap(),
            ColumnValue::PgPendingText {
                type_oid: 1007,
                text: "{1,2}".into(),
            },
        );
        // Wrong width is an error, not a silent misread
        assert!(decode_field(WireKind::Int4, 0, &[0, 1]).is_err());
        // Invalid UTF-8 degrades to Bytea like the heap decoder
        assert_eq!(
            decode_field(WireKind::Text, 0, &[0xFF, 0xFE]).unwrap(),
            ColumnValue::Bytea(vec![0xFF, 0xFE]),
        );
    }

    #[tokio::test]
    async fn fallback_ledger_guards_and_persistence() {
        let tmp = tempfile::tempdir().unwrap();
        let rel = RelName::new("app", "orders");
        let mode = InitialLoadMode::ObjectStore;
        let mut ledger = Ledger::load(tmp.path(), 7).await.unwrap();
        let pending = LedgerRec {
            s_lsn: 100.into(),
            done: false,
            mode,
            swapped: false,
            staging_uuid: None,
            copy: None,
            toast_seeded: false,
        };
        assert!(!ledger.fallback_to_copy(&rel, mode, 100).await.unwrap());
        for rec in [
            LedgerRec {
                done: true,
                ..pending.clone()
            },
            LedgerRec {
                swapped: true,
                ..pending.clone()
            },
            LedgerRec {
                staging_uuid: Some("uuid".into()),
                ..pending.clone()
            },
            LedgerRec {
                s_lsn: 200.into(),
                ..pending.clone()
            },
            LedgerRec {
                mode: InitialLoadMode::BaseBackup,
                ..pending.clone()
            },
        ] {
            ledger.entries.insert(rel.clone(), rec);
            assert!(!ledger.fallback_to_copy(&rel, mode, 100).await.unwrap());
        }
        ledger.entries.insert(rel.clone(), pending);
        ledger.persist().await.unwrap();
        let original_dir = ledger.dir.clone();
        let blocker = tmp.path().join("not-a-directory");
        std::fs::write(&blocker, "blocked").unwrap();
        ledger.dir = blocker;
        assert!(ledger.fallback_to_copy(&rel, mode, 100).await.is_err());
        assert_eq!(ledger.entries[&rel].mode, mode);
        assert_eq!(
            Ledger::load(tmp.path(), 7).await.unwrap().entries[&rel].mode,
            mode
        );
        ledger.dir = original_dir;
        assert!(ledger.fallback_to_copy(&rel, mode, 100).await.unwrap());
        let resumed = Ledger::load(tmp.path(), 7).await.unwrap();
        assert_eq!(resumed.entries[&rel].mode, InitialLoadMode::Copy);
        assert_eq!(resumed.entries[&rel].s_lsn.get(), 100);
        assert!(!resumed.entries[&rel].done);
    }

    #[test]
    fn ctid_range_bounds_each_chunk_and_leaves_a_lone_chunk_unqualified() {
        let clause = |start, end| ctid_range(BlockRange { start, end });
        assert_eq!(clause(0, None), "");
        assert_eq!(clause(0, Some(8)), " WHERE ctid < '(8,0)'::tid");
        assert_eq!(clause(8, None), " WHERE ctid >= '(8,0)'::tid");
        assert_eq!(
            clause(8, Some(16)),
            " WHERE ctid >= '(8,0)'::tid AND ctid < '(16,0)'::tid"
        );
    }

    #[tokio::test]
    async fn ledger_round_trips_and_rejects_corruption() {
        let tmp = tempfile::tempdir().unwrap();
        let mut ledger = Ledger::load(tmp.path(), 7).await.unwrap();
        assert_eq!(ledger.pending_count(), 0);
        ledger.entries.insert(
            RelName::new("app", "orders"),
            LedgerRec {
                s_lsn: 0x1000.into(),
                done: false,
                mode: InitialLoadMode::Copy,
                swapped: false,
                staging_uuid: None,
                copy: Some(CopyCursor {
                    relfilenode: 16400,
                    next_block: 512,
                }),
                toast_seeded: true,
            },
        );
        ledger.entries.insert(
            RelName::new("app", "done"),
            LedgerRec {
                s_lsn: 0x800.into(),
                done: true,
                mode: InitialLoadMode::ObjectStore,
                swapped: false,
                staging_uuid: None,
                copy: None,
                toast_seeded: false,
            },
        );
        ledger.entries.insert(
            RelName::new("app", "mid_swap"),
            LedgerRec {
                s_lsn: 0x2000.into(),
                done: false,
                mode: InitialLoadMode::ObjectStore,
                swapped: true,
                staging_uuid: Some("a-uuid".into()),
                copy: None,
                toast_seeded: false,
            },
        );
        ledger.persist().await.unwrap();

        let again = Ledger::load(tmp.path(), 7).await.unwrap();
        let orders = again.entries.get(&RelName::new("app", "orders")).unwrap();
        assert_eq!((orders.s_lsn.get(), orders.done), (0x1000, false));
        assert_eq!(orders.mode, InitialLoadMode::Copy);
        assert!(!orders.swapped);
        assert_eq!(
            orders.copy,
            Some(CopyCursor {
                relfilenode: 16400,
                next_block: 512
            }),
            "COPY cursor round-trips",
        );
        let done = again.entries.get(&RelName::new("app", "done")).unwrap();
        assert_eq!(done.copy, None);
        assert_eq!((done.s_lsn.get(), done.done), (0x800, true));
        assert_eq!(done.mode, InitialLoadMode::ObjectStore, "mode round-trips");
        let mid = again.entries.get(&RelName::new("app", "mid_swap")).unwrap();
        assert!(mid.swapped, "swap phase round-trips");
        assert_eq!(mid.staging_uuid.as_deref(), Some("a-uuid"));
        assert_eq!(again.pending_count(), 2, "swapped counts as pending");
        assert_eq!(again.pending_count_for(InitialLoadMode::Copy), 1);
        assert_eq!(again.pending_count_for(InitialLoadMode::ObjectStore), 1);

        assert!(again.entries[&RelName::new("app", "orders")].toast_seeded);

        let foreign = Ledger::load(tmp.path(), 8).await.err().unwrap();
        assert!(
            foreign.to_string().contains("belongs to source system 7"),
            "{foreign}"
        );

        tokio::fs::write(tmp.path().join(LEDGER_FILENAME), b"not json")
            .await
            .unwrap();
        let corrupt = Ledger::load(tmp.path(), 7).await.err().unwrap();
        assert!(
            corrupt.to_string().contains("orphan `__wsstg`"),
            "corrupt ledger stops boot rather than orphaning staging: {corrupt}"
        );
    }

    #[tokio::test]
    async fn unstamped_ledger_upgrades_in_place() {
        let tmp = tempfile::tempdir().unwrap();
        tokio::fs::write(
            tmp.path().join(LEDGER_FILENAME),
            "version = 1\n[[backfill]]\nnamespace = 'app'\nrelname = 'orders'\n\
             s_lsn = '0/64'\ndone = false\n",
        )
        .await
        .unwrap();
        let ledger = Ledger::load(tmp.path(), 7).await.unwrap();
        let rec = &ledger.entries[&RelName::new("app", "orders")];
        assert_eq!((rec.s_lsn.get(), rec.mode), (0x64, InitialLoadMode::Copy));
        let text = std::fs::read_to_string(tmp.path().join(LEDGER_FILENAME)).unwrap();
        assert!(text.contains("system_id = 7"), "{text}");
        assert!(Ledger::load(tmp.path(), 8).await.is_err());
    }
}
