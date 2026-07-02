//! Per-table COPY backfiller — snapshot-free initial load for a non-empty
//! table opted in via `config_table (replicate=true, initial_load='copy')`
//! (plans/future/runtime_config_from_pg.md §Per-table opt-in).
//!
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
//! ([`crate::pipeline::tail`] + [`crate::pipeline::bootstrap::drain`]), on a
//! dedicated CH connection, so a backfill never blocks the live pipeline.
//! COPY output is fully detoasted, so the disabled TOAST resolver suffices.
//!
//! ## Field decode
//!
//! Binary COPY carries each field in `typsend` wire form (big-endian), not
//! the on-disk datum form the WAL heap decoder reads. Fixed-width types and
//! byte/text strings decode natively into the same [`ColumnValue`] variants
//! the WAL path produces; `numeric` selects as `::text` (numeric_out is the
//! exact `NumericKind::Finite` form); every out-of-matrix type also selects
//! as `::text` and ships as [`ColumnValue::Text`], mirroring the WAL path's
//! `PgPending`→oracle→text resolution and the type bridge's `String` mapping.
//!
//! ## Resume ledger
//!
//! `{spill_dir}/backfills.json` persists per-qname `{s_lsn, done}`. The
//! opt-in's WAL event is not re-delivered after the ack passes `S`, so the
//! ledger is what carries an unfinished backfill across a restart: boot
//! re-seeds opt-ins from `config_table` and re-issues COPY for pending
//! entries at their original `S` (dedup makes the re-COPY idempotent). A
//! `done` entry stops every later boot from re-copying (the daemon never
//! writes `initial_load` back to source). Corrupt/absent ledger degrades to
//! re-COPY, never to data loss. Completion is observability: convergence is
//! reported once WAL apply passes `P_hi = pg_current_wal_lsn()` read at COPY
//! EOF; nothing is gated on it.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::Context as _;
use futures::StreamExt as _;
use tokio::io::AsyncWriteExt as _;
use tokio::sync::{Mutex, mpsc};
use walrus::pg::backup::{format_pg_lsn, parse_pg_lsn};
use walrus::pg::replication::conn::PgConfig;

use crate::backup_page_walk::{BOOTSTRAP_TUPLE_CHANNEL_CAP, BackfillTuple, CatalogMap};
use crate::ch_emitter::{EmitterConfig, EmitterStats, MappingHandle};
use crate::codecs::NumericKind;
use crate::heap_decoder::{
    BOOLOID, BPCHAROID, BYTEAOID, CHAROID, ColumnValue, DATEOID, FLOAT4OID, FLOAT8OID, INT2OID,
    INT4OID, INT8OID, JSONOID, NAMEOID, NUMERICOID, OIDOID, TEXTOID, TIMEOID, TIMESTAMPOID,
    TIMESTAMPTZOID, UUIDOID, VARCHAROID,
};
use crate::pipeline::{Fatal, bootstrap, tail};
use crate::shadow_catalog::RelDescriptor;
use crate::source_feed::open_sql_client;
use crate::toast::ToastResolver;

const LEDGER_FILENAME: &str = "backfills.json";

// ---------------------------------------------------------------------------
// Resume ledger
// ---------------------------------------------------------------------------

#[derive(serde::Serialize, serde::Deserialize)]
struct LedgerEntry {
    qname: String,
    s_lsn: u64,
    done: bool,
}

struct Ledger {
    dir: PathBuf,
    /// qname → (S, done)
    entries: HashMap<String, (u64, bool)>,
}

impl Ledger {
    async fn load(spill_dir: &Path) -> Self {
        let path = spill_dir.join(LEDGER_FILENAME);
        let entries = match tokio::fs::read(&path).await {
            Ok(bytes) => match serde_json::from_slice::<Vec<LedgerEntry>>(&bytes) {
                Ok(list) => list
                    .into_iter()
                    .map(|e| (e.qname, (e.s_lsn, e.done)))
                    .collect(),
                Err(e) => {
                    // Degrades to re-COPY (idempotent), never to data loss
                    tracing::warn!(
                        target: "walshadow::backfill",
                        path = %path.display(),
                        error = %e,
                        "backfill ledger unreadable; treating as empty",
                    );
                    HashMap::new()
                }
            },
            Err(_) => HashMap::new(),
        };
        Self {
            dir: spill_dir.to_path_buf(),
            entries,
        }
    }

    /// Crash-safe persist: write+fsync `.tmp`, rename, fsync dir.
    async fn persist(&self) -> std::io::Result<()> {
        let list: Vec<LedgerEntry> = self
            .entries
            .iter()
            .map(|(qname, (s_lsn, done))| LedgerEntry {
                qname: qname.clone(),
                s_lsn: *s_lsn,
                done: *done,
            })
            .collect();
        let bytes = serde_json::to_vec(&list).expect("ledger serialize");
        let tmp = self.dir.join(format!("{LEDGER_FILENAME}.tmp"));
        let mut f = tokio::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&tmp)
            .await?;
        f.write_all(&bytes).await?;
        f.sync_all().await?;
        drop(f);
        tokio::fs::rename(&tmp, self.dir.join(LEDGER_FILENAME)).await?;
        crate::cursor::fsync_dir(&self.dir).await
    }

    fn pending_count(&self) -> u64 {
        self.entries.values().filter(|(_, done)| !done).count() as u64
    }
}

// ---------------------------------------------------------------------------
// Binary COPY wire parser
// ---------------------------------------------------------------------------

/// `PGCOPY\n\xff\r\n\0`
const COPY_SIGNATURE: &[u8; 11] = b"PGCOPY\n\xff\r\n\0";

/// Incremental parser over `CopyOutStream` chunks (chunk boundaries are
/// arbitrary relative to rows). Yields one raw-field row at a time; a `-1`
/// field count is the trailer.
struct CopyBinaryParser {
    buf: Vec<u8>,
    pos: usize,
    header_parsed: bool,
    done: bool,
}

impl CopyBinaryParser {
    fn new() -> Self {
        Self {
            buf: Vec::new(),
            pos: 0,
            header_parsed: false,
            done: false,
        }
    }

    fn push(&mut self, chunk: &[u8]) {
        if self.pos > 0 {
            self.buf.drain(..self.pos);
            self.pos = 0;
        }
        self.buf.extend_from_slice(chunk);
    }

    fn avail(&self) -> &[u8] {
        &self.buf[self.pos..]
    }

    /// `Ok(Some(fields))` per complete row; `Ok(None)` when more input is
    /// needed or the trailer was consumed.
    fn next_row(&mut self) -> Result<Option<Vec<Option<Vec<u8>>>>, String> {
        if self.done {
            return Ok(None);
        }
        if !self.header_parsed {
            let b = self.avail();
            if b.len() < COPY_SIGNATURE.len() + 8 {
                return Ok(None);
            }
            if &b[..COPY_SIGNATURE.len()] != COPY_SIGNATURE {
                return Err("bad binary COPY signature".into());
            }
            // flags (4) + header extension length (4) + extension bytes
            let ext_off = COPY_SIGNATURE.len() + 4;
            let ext_len = u32::from_be_bytes(b[ext_off..ext_off + 4].try_into().unwrap()) as usize;
            let hdr = ext_off + 4 + ext_len;
            if b.len() < hdr {
                return Ok(None);
            }
            self.pos += hdr;
            self.header_parsed = true;
        }

        // Scan a whole row before consuming, so a row split across chunks
        // never half-advances.
        let b = self.avail();
        if b.len() < 2 {
            return Ok(None);
        }
        let nfields = i16::from_be_bytes(b[..2].try_into().unwrap());
        if nfields == -1 {
            self.pos += 2;
            self.done = true;
            return Ok(None);
        }
        if nfields < 0 {
            return Err(format!("bad binary COPY field count {nfields}"));
        }
        let mut cur = 2usize;
        let mut fields: Vec<Option<Vec<u8>>> = Vec::with_capacity(nfields as usize);
        for _ in 0..nfields {
            if b.len() < cur + 4 {
                return Ok(None);
            }
            let len = i32::from_be_bytes(b[cur..cur + 4].try_into().unwrap());
            cur += 4;
            if len == -1 {
                fields.push(None);
                continue;
            }
            let len = usize::try_from(len).map_err(|_| format!("bad field length {len}"))?;
            if b.len() < cur + len {
                return Ok(None);
            }
            fields.push(Some(b[cur..cur + len].to_vec()));
            cur += len;
        }
        self.pos += cur;
        Ok(Some(fields))
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
}

/// PG identifier quoting (double-quote, double embedded quotes).
fn quote_pg_ident(name: &str) -> String {
    let mut out = String::with_capacity(name.len() + 2);
    out.push('"');
    for c in name.chars() {
        if c == '"' {
            out.push('"');
        }
        out.push(c);
    }
    out.push('"');
    out
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

/// SELECT list + decode plan + column-slot count (max attnum; dropped attrs
/// stay `None`, matching the WAL heap decoder's attnum-1 indexing).
fn column_plan(desc: &RelDescriptor) -> (String, Vec<ColPlan>, usize) {
    let mut select = String::new();
    let mut plan = Vec::new();
    let mut natts = 0usize;
    for a in &desc.attributes {
        natts = natts.max(a.attnum.max(0) as usize);
        if a.dropped {
            continue;
        }
        let ident = quote_pg_ident(&a.name);
        let (expr, kind) = match wire_kind(a.type_oid) {
            Some(k) => (ident, k),
            None if a.type_oid == NUMERICOID => (format!("{ident}::text"), WireKind::NumericText),
            None => (format!("{ident}::text"), WireKind::CastText),
        };
        if !select.is_empty() {
            select.push_str(", ");
        }
        select.push_str(&expr);
        plan.push(ColPlan {
            attnum: a.attnum,
            kind,
        });
    }
    (select, plan, natts)
}

fn fixed<const N: usize>(raw: &[u8], what: &str) -> Result<[u8; N], String> {
    raw.try_into()
        .map_err(|_| format!("{what}: expected {N} bytes, got {}", raw.len()))
}

fn utf8(raw: Vec<u8>) -> Result<String, Vec<u8>> {
    String::from_utf8(raw).map_err(|e| e.into_bytes())
}

/// Decode one non-NULL wire field into the same [`ColumnValue`] variant the
/// WAL heap decoder produces for that column.
fn decode_field(kind: WireKind, raw: Vec<u8>) -> Result<ColumnValue, String> {
    Ok(match kind {
        WireKind::Bool => ColumnValue::Bool(fixed::<1>(&raw, "bool")?[0] != 0),
        WireKind::Char => ColumnValue::Char(fixed::<1>(&raw, "char")?[0] as i8),
        WireKind::Int2 => ColumnValue::Int2(i16::from_be_bytes(fixed(&raw, "int2")?)),
        WireKind::Int4 => ColumnValue::Int4(i32::from_be_bytes(fixed(&raw, "int4")?)),
        WireKind::Int8 => ColumnValue::Int8(i64::from_be_bytes(fixed(&raw, "int8")?)),
        WireKind::Oid => ColumnValue::Oid(u32::from_be_bytes(fixed(&raw, "oid")?)),
        WireKind::Float4 => ColumnValue::Float4(f32::from_be_bytes(fixed(&raw, "float4")?)),
        WireKind::Float8 => ColumnValue::Float8(f64::from_be_bytes(fixed(&raw, "float8")?)),
        WireKind::Date => ColumnValue::Date(i32::from_be_bytes(fixed(&raw, "date")?)),
        WireKind::Time => ColumnValue::Time(i64::from_be_bytes(fixed(&raw, "time")?)),
        WireKind::Timestamp => {
            ColumnValue::Timestamp(i64::from_be_bytes(fixed(&raw, "timestamp")?))
        }
        WireKind::TimestampTz => {
            ColumnValue::TimestampTz(i64::from_be_bytes(fixed(&raw, "timestamptz")?))
        }
        WireKind::Uuid => ColumnValue::Uuid(fixed(&raw, "uuid")?),
        WireKind::Bytea => ColumnValue::Bytea(raw),
        // Invalid UTF-8 surfaces as Bytea, same as the heap decoder
        WireKind::Text | WireKind::CastText => match utf8(raw) {
            Ok(s) => ColumnValue::Text(s),
            Err(b) => ColumnValue::Bytea(b),
        },
        WireKind::Name => match utf8(raw) {
            Ok(s) => ColumnValue::Name(s),
            Err(b) => ColumnValue::Bytea(b),
        },
        WireKind::Json => match utf8(raw) {
            Ok(s) => ColumnValue::Json(s),
            Err(b) => ColumnValue::Bytea(b),
        },
        // numeric_out text form; specials carry their flag
        WireKind::NumericText => {
            let s = utf8(raw).map_err(|_| "numeric::text not utf8".to_string())?;
            ColumnValue::Numeric(match s.as_str() {
                "NaN" => NumericKind::NaN,
                "Infinity" => NumericKind::PInf,
                "-Infinity" => NumericKind::NInf,
                _ => NumericKind::Finite(s),
            })
        }
    })
}

// ---------------------------------------------------------------------------
// Backfiller
// ---------------------------------------------------------------------------

struct Inner {
    ledger: Ledger,
    /// qnames with a task running this boot; stops a re-upserted config row
    /// (or a boot-seed + WAL-replay double-fire) from starting a second COPY.
    active: HashSet<String>,
}

/// Owns the resume ledger and spawns one detached task per backfilling
/// table. Shared by the reorder coordinator (live opt-ins) and the boot seed
/// (restart resume / pre-installed config rows).
pub struct CopyBackfiller {
    pg: PgConfig,
    emitter: EmitterConfig,
    mapping: MappingHandle,
    stats: Arc<EmitterStats>,
    inner: Mutex<Inner>,
    /// Ledger entries not yet `done` (gauge; mirrors the ledger under the lock).
    pending: AtomicU64,
}

impl CopyBackfiller {
    pub async fn new(
        pg: PgConfig,
        emitter: EmitterConfig,
        mapping: MappingHandle,
        stats: Arc<EmitterStats>,
        spill_dir: &Path,
    ) -> Self {
        let ledger = Ledger::load(spill_dir).await;
        let pending = AtomicU64::new(ledger.pending_count());
        Self {
            pg,
            emitter,
            mapping,
            stats,
            inner: Mutex::new(Inner {
                ledger,
                active: HashSet::new(),
            }),
            pending,
        }
    }

    /// Ledger entries awaiting COPY completion.
    pub fn pending_count(&self) -> u64 {
        self.pending.load(Ordering::Relaxed)
    }

    /// An `initial_load='copy'` opt-in applied for a known rel. First sight
    /// records `{S = opt_in_lsn, pending}` durably (the WAL event is not
    /// re-delivered once the ack passes `S`, so the ledger write must precede
    /// the barrier release) and spawns the COPY; a pending entry resumes at
    /// its persisted `S`; a `done` entry or an already-running task no-ops.
    pub async fn note_opt_in(self: &Arc<Self>, desc: &Arc<RelDescriptor>, opt_in_lsn: u64) {
        let qname = desc.qualified_name.as_ref().to_owned();
        let s_lsn = {
            let mut inner = self.inner.lock().await;
            let s_lsn = match inner.ledger.entries.get(&qname) {
                Some((_, true)) => return,
                Some((s, false)) => *s,
                None => {
                    inner
                        .ledger
                        .entries
                        .insert(qname.clone(), (opt_in_lsn, false));
                    if let Err(e) = inner.ledger.persist().await {
                        tracing::warn!(
                            target: "walshadow::backfill",
                            qname = %qname,
                            error = %e,
                            "backfill ledger persist failed; a crash before completion re-streams without backfill",
                        );
                    }
                    opt_in_lsn
                }
            };
            if !inner.active.insert(qname.clone()) {
                return;
            }
            self.pending
                .store(inner.ledger.pending_count(), Ordering::Relaxed);
            s_lsn
        };
        let this = self.clone();
        let desc = desc.clone();
        tokio::spawn(async move { this.run(desc, s_lsn).await });
    }

    /// Opt-out / row removal: drop the ledger entry so a later re-insert
    /// re-triggers a fresh backfill. A COPY already in flight drains against
    /// the shared routing map, so its remaining rows skip once the mapping is
    /// gone; its completion mark no-ops (entry absent).
    pub async fn note_opt_out(&self, qname: &str) {
        let mut inner = self.inner.lock().await;
        if inner.ledger.entries.remove(qname).is_some()
            && let Err(e) = inner.ledger.persist().await
        {
            tracing::warn!(
                target: "walshadow::backfill",
                qname,
                error = %e,
                "backfill ledger persist failed on opt-out",
            );
        }
        self.pending
            .store(inner.ledger.pending_count(), Ordering::Relaxed);
    }

    async fn run(self: Arc<Self>, desc: Arc<RelDescriptor>, s_lsn: u64) {
        let qname = desc.qualified_name.as_ref().to_owned();
        let res = self.copy_once(&desc, s_lsn).await;
        let mut inner = self.inner.lock().await;
        inner.active.remove(&qname);
        match res {
            Ok(outcome) => {
                if let Some(entry) = inner.ledger.entries.get_mut(&qname) {
                    entry.1 = true;
                    if let Err(e) = inner.ledger.persist().await {
                        tracing::warn!(
                            target: "walshadow::backfill",
                            qname = %qname,
                            error = %e,
                            "backfill ledger persist failed; next boot re-COPYs (idempotent)",
                        );
                    }
                }
                tracing::info!(
                    target: "walshadow::backfill",
                    qname = %qname,
                    rows = outcome.rows,
                    s_lsn = %format_pg_lsn(s_lsn),
                    p_hi = %format_pg_lsn(outcome.p_hi),
                    copied = !outcome.skipped_empty,
                    "backfill complete; converged once WAL apply passes p_hi",
                );
            }
            // Regime A: a failed backfill never poisons the pump. Entry stays
            // pending; the next boot's seed re-issues COPY at the same S.
            Err(e) => {
                tracing::error!(
                    target: "walshadow::backfill",
                    qname = %qname,
                    error = %format!("{e:#}"),
                    "backfill failed; entry stays pending (re-COPY on next boot)",
                );
            }
        }
        self.pending
            .store(inner.ledger.pending_count(), Ordering::Relaxed);
    }

    async fn copy_once(
        &self,
        desc: &Arc<RelDescriptor>,
        s_lsn: u64,
    ) -> anyhow::Result<CopyOutcome> {
        let qtable = format!(
            "{}.{}",
            quote_pg_ident(&desc.namespace_name),
            quote_pg_ident(&desc.name)
        );
        let client = open_sql_client(&self.pg)
            .await
            .context("backfill: source sql connect")?;

        // Empty table ⇒ streaming alone suffices, skip COPY + tail entirely
        let nonempty: bool = client
            .query_one(&format!("SELECT EXISTS (SELECT 1 FROM {qtable})"), &[])
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

        // Dedicated tail: own CH connection, own seq space, own fatal.
        let fatal = Fatal::new();
        let (msg_tx, ack, tail) = tail::spawn(
            &self.emitter,
            1,
            self.stats.clone(),
            Arc::new(AtomicU64::new(0)),
            fatal.clone(),
        )
        .await
        .map_err(|e| anyhow::anyhow!("backfill: spawn insert tail: {e}"))?;

        let mut catalog = CatalogMap::new();
        catalog.insert(desc.clone());
        let (tup_tx, tup_rx) = mpsc::channel::<BackfillTuple>(BOOTSTRAP_TUPLE_CHANNEL_CAP);
        let drain = tokio::spawn(bootstrap::drain(
            tup_rx,
            catalog,
            self.mapping.clone(),
            msg_tx.clone(),
            ack.clone(),
            self.stats.clone(),
            ToastResolver::disabled(),
        ));

        let (select_list, plan, natts) = column_plan(desc);
        let sql = format!("COPY (SELECT {select_list} FROM {qtable}) TO STDOUT (FORMAT binary)");
        let stream = client.copy_out(&sql).await.context("backfill: COPY out")?;
        futures::pin_mut!(stream);

        let mut parser = CopyBinaryParser::new();
        let mut rows = 0u64;
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.context("backfill: COPY stream")?;
            parser.push(&chunk);
            while let Some(fields) = parser.next_row().map_err(anyhow::Error::msg)? {
                if fields.len() != plan.len() {
                    anyhow::bail!(
                        "backfill: row has {} fields, plan expects {}",
                        fields.len(),
                        plan.len()
                    );
                }
                let mut columns: Vec<Option<ColumnValue>> = vec![None; natts];
                for (raw, cp) in fields.into_iter().zip(&plan) {
                    let v = match raw {
                        None => ColumnValue::Null,
                        Some(raw) => decode_field(cp.kind, raw).map_err(anyhow::Error::msg)?,
                    };
                    columns[(cp.attnum - 1).max(0) as usize] = Some(v);
                }
                tup_tx
                    .send(BackfillTuple {
                        rfn: desc.rfn,
                        xid: 0,
                        source_lsn: s_lsn,
                        columns,
                    })
                    .await
                    .map_err(|_| anyhow::anyhow!("backfill: drain closed early"))?;
                rows += 1;
            }
        }
        drop(tup_tx);

        let outcome = drain
            .await
            .context("backfill: drain join")?
            .map_err(anyhow::Error::msg)?;
        // Upper bound on the COPY snapshot; WAL apply past it = converged
        let p_hi = current_wal_lsn(&client).await?;
        tail.finish(msg_tx, ack, outcome.next_seq, &fatal)
            .await
            .map_err(anyhow::Error::msg)?;
        Ok(CopyOutcome {
            rows,
            skipped_empty: false,
            p_hi,
        })
    }
}

struct CopyOutcome {
    rows: u64,
    skipped_empty: bool,
    p_hi: u64,
}

async fn current_wal_lsn(client: &tokio_postgres::Client) -> anyhow::Result<u64> {
    let text: String = client
        .query_one("SELECT pg_current_wal_lsn()::text", &[])
        .await
        .context("backfill: pg_current_wal_lsn")?
        .get(0);
    parse_pg_lsn(&text).context("backfill: parse pg_current_wal_lsn")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shadow_catalog::{RelAttr, ReplIdent};
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
            missing_text: None,
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
            namespace_oid: 2200,
            namespace_name: "app".into(),
            name: "orders".into(),
            qualified_name: RelDescriptor::build_qualified_name("app", "orders"),
            kind: 'r',
            persistence: 'p',
            replident: ReplIdent::Default { pk_attnums: None },
            attributes: attrs,
        }
    }

    /// One-row binary COPY payload: header, row of `fields`, trailer.
    fn copy_payload(rows: &[Vec<Option<&[u8]>>]) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(COPY_SIGNATURE);
        out.extend_from_slice(&0u32.to_be_bytes()); // flags
        out.extend_from_slice(&0u32.to_be_bytes()); // no header extension
        for fields in rows {
            out.extend_from_slice(&(fields.len() as i16).to_be_bytes());
            for f in fields {
                match f {
                    None => out.extend_from_slice(&(-1i32).to_be_bytes()),
                    Some(b) => {
                        out.extend_from_slice(&(b.len() as i32).to_be_bytes());
                        out.extend_from_slice(b);
                    }
                }
            }
        }
        out.extend_from_slice(&(-1i16).to_be_bytes());
        out
    }

    #[test]
    fn parser_handles_arbitrary_chunk_splits() {
        let v42 = 42i64.to_be_bytes();
        let payload = copy_payload(&[
            vec![Some(&v42[..]), Some(b"hello"), None],
            vec![Some(&v42[..]), None, Some(b"x")],
        ]);
        // Feed byte-by-byte: worst-case splits everywhere
        let mut parser = CopyBinaryParser::new();
        let mut rows = Vec::new();
        for b in &payload {
            parser.push(std::slice::from_ref(b));
            while let Some(row) = parser.next_row().unwrap() {
                rows.push(row);
            }
        }
        assert!(parser.done, "trailer consumed");
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0][0].as_deref(), Some(&v42[..]));
        assert_eq!(rows[0][1].as_deref(), Some(&b"hello"[..]));
        assert_eq!(rows[0][2], None);
        assert_eq!(rows[1][2].as_deref(), Some(&b"x"[..]));
    }

    #[test]
    fn parser_rejects_bad_signature() {
        let mut parser = CopyBinaryParser::new();
        parser.push(b"NOTACOPYSTREAM------");
        assert!(parser.next_row().is_err());
    }

    #[test]
    fn plan_casts_out_of_matrix_and_skips_dropped() {
        let d = desc(vec![
            attr(1, "id", INT8OID, false),
            attr(2, "gone", TEXTOID, true),
            attr(3, "price", NUMERICOID, false),
            attr(4, "tags", 1009, false), // text[] — out of matrix
        ]);
        let (select, plan, natts) = column_plan(&d);
        assert_eq!(select, "\"id\", \"price\"::text, \"tags\"::text");
        assert_eq!(natts, 4);
        assert_eq!(plan.len(), 3, "dropped column not selected");
        assert_eq!(plan[0].kind, WireKind::Int8);
        assert_eq!(plan[1].kind, WireKind::NumericText);
        assert_eq!(plan[2].kind, WireKind::CastText);
    }

    #[test]
    fn quote_pg_ident_doubles_quotes() {
        assert_eq!(quote_pg_ident("plain"), "\"plain\"");
        assert_eq!(quote_pg_ident("we\"ird"), "\"we\"\"ird\"");
    }

    #[test]
    fn decode_matches_wal_variants() {
        assert_eq!(
            decode_field(WireKind::Int8, 7i64.to_be_bytes().to_vec()).unwrap(),
            ColumnValue::Int8(7),
        );
        assert_eq!(
            decode_field(WireKind::Bool, vec![1]).unwrap(),
            ColumnValue::Bool(true),
        );
        assert_eq!(
            decode_field(WireKind::Date, 8000i32.to_be_bytes().to_vec()).unwrap(),
            ColumnValue::Date(8000),
        );
        assert_eq!(
            decode_field(WireKind::Text, b"abc".to_vec()).unwrap(),
            ColumnValue::Text("abc".into()),
        );
        assert_eq!(
            decode_field(WireKind::NumericText, b"12.50".to_vec()).unwrap(),
            ColumnValue::Numeric(NumericKind::Finite("12.50".into())),
        );
        assert_eq!(
            decode_field(WireKind::NumericText, b"NaN".to_vec()).unwrap(),
            ColumnValue::Numeric(NumericKind::NaN),
        );
        assert_eq!(
            decode_field(WireKind::CastText, b"{1,2}".to_vec()).unwrap(),
            ColumnValue::Text("{1,2}".into()),
        );
        // Wrong width is an error, not a silent misread
        assert!(decode_field(WireKind::Int4, vec![0, 1]).is_err());
        // Invalid UTF-8 degrades to Bytea like the heap decoder
        assert_eq!(
            decode_field(WireKind::Text, vec![0xFF, 0xFE]).unwrap(),
            ColumnValue::Bytea(vec![0xFF, 0xFE]),
        );
    }

    #[tokio::test]
    async fn ledger_round_trips_and_survives_corruption() {
        let tmp = tempfile::tempdir().unwrap();
        let mut ledger = Ledger::load(tmp.path()).await;
        assert_eq!(ledger.pending_count(), 0);
        ledger.entries.insert("app.orders".into(), (0x1000, false));
        ledger.entries.insert("app.done".into(), (0x800, true));
        ledger.persist().await.unwrap();

        let again = Ledger::load(tmp.path()).await;
        assert_eq!(again.entries.get("app.orders"), Some(&(0x1000, false)));
        assert_eq!(again.entries.get("app.done"), Some(&(0x800, true)));
        assert_eq!(again.pending_count(), 1);

        tokio::fs::write(tmp.path().join(LEDGER_FILENAME), b"not json")
            .await
            .unwrap();
        let corrupt = Ledger::load(tmp.path()).await;
        assert!(
            corrupt.entries.is_empty(),
            "corrupt ledger degrades to re-COPY"
        );
    }
}
