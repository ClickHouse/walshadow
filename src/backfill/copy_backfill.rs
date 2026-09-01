//! Per-table `initial_load` backfiller. Owns the resume ledger and dispatches
//! per mode: `'copy'` (this module's COPY path, below) runs one detached task
//! per table; `'base_backup'` / `'object_store'` coalesce into one
//! [`crate::backfill::backup_backfill`] pass per mode, loading per-rel staging tables
//! that publish via `EXCHANGE TABLES` + live-window copy-back on success
//! ([`crate::backfill::backfill_staging`], plans/add_table.md §Staging swap)
//! (plans/future/runtime_config_from_pg.md §Per-table opt-in).
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
//! `{spill_dir}/backfills.toml` persists per-qname `{s_lsn, done, mode}`. The
//! opt-in's WAL event is not re-delivered after the ack passes `S`, so the
//! ledger is what carries an unfinished backfill across a restart: boot
//! re-seeds opt-ins from `config_table` and re-runs the *recorded* mode for
//! pending entries at their original `S` (dedup makes the re-run idempotent).
//! A `done` entry stops every later boot from re-running (the daemon never
//! writes `initial_load` back to source). Corrupt/absent ledger degrades to
//! re-COPY, never to data loss. Completion is observability: convergence is
//! reported once WAL apply passes `P_hi = pg_current_wal_lsn()` read at COPY
//! EOF; nothing is gated on it.

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
use crate::backfill::backup_page_walk::{BOOTSTRAP_TUPLE_CHANNEL_CAP, BackfillTuple, CatalogMap};
use crate::backfill::opt_in::Backfiller;
use crate::catalog::shadow_catalog::ShadowCatalog;
use crate::config::ResolvedConfig;
use crate::decode::codecs::NumericKind;
use crate::decode::heap_decoder::ColumnValue;
use crate::emit::ch_emitter::{EmitterConfig, EmitterStats};
use crate::emit::pipeline::{Fatal, bootstrap, tail};
use crate::mapping::MappingHandle;
use crate::ops::oracle::Oracle;
use crate::pg::{current_wal_lsn, quote_ident};
use crate::pos::{Pos, Snapshot};
use crate::runtime_config::InitialLoadMode;
use crate::schema::{
    BOOLOID, BPCHAROID, BYTEAOID, CHAROID, DATEOID, FLOAT4OID, FLOAT8OID, INT2OID, INT4OID,
    INT8OID, JSONOID, NAMEOID, NUMERICOID, OIDOID, RelDescriptor, RelName, TEXTOID, TIMEOID,
    TIMESTAMPOID, TIMESTAMPTZOID, UUIDOID, VARCHAROID,
};
use crate::source::source_feed::open_sql_client;
use crate::toast::ToastResolver;
use ahash::{HashMap, HashMapExt, HashSet, HashSetExt};

const LEDGER_FILENAME: &str = "backfills.toml";
const LEDGER_VERSION: u32 = 1;

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
    /// Backup-mode staging swap phase (plans/add_table.md §Staging swap):
    /// pass rows durable, `EXCHANGE TABLES` issued or about to be — boot
    /// resumes the swap tail (copy-back + drop) instead of re-loading
    #[serde(default)]
    swapped: bool,
    /// Staging table uuid recorded just before the exchange; recovery
    /// compares it against the uuid now under the staging name to tell
    /// whether the exchange applied
    #[serde(default)]
    staging_uuid: Option<String>,
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
}

struct Ledger {
    dir: PathBuf,
    entries: HashMap<RelName, LedgerRec>,
}

impl Ledger {
    async fn load(spill_dir: &Path) -> Self {
        let mut ledger = Self {
            dir: spill_dir.to_path_buf(),
            entries: HashMap::new(),
        };
        let path = spill_dir.join(LEDGER_FILENAME);
        if let Ok(text) = tokio::fs::read_to_string(&path).await {
            match toml::from_str::<LedgerFile>(&text) {
                Ok(file) if file.version == LEDGER_VERSION => {
                    ledger.entries = file
                        .backfill
                        .into_iter()
                        .map(|e| {
                            // Only this daemon writes modes; an unparseable one
                            // degrades to re-COPY like a corrupt ledger would
                            let mode = e.mode.parse().unwrap_or(InitialLoadMode::Copy);
                            (
                                RelName::new(&e.namespace, &e.relname),
                                LedgerRec {
                                    s_lsn: e.s_lsn,
                                    done: e.done,
                                    mode,
                                    swapped: e.swapped,
                                    staging_uuid: e.staging_uuid,
                                },
                            )
                        })
                        .collect();
                }
                // Degrades to re-COPY (idempotent), never to data loss
                Ok(file) => {
                    tracing::warn!(
                        target: "walshadow::backfill",
                        path = %path.display(),
                        version = file.version,
                        "backfill ledger version unsupported; treating as empty",
                    );
                }
                Err(e) => {
                    tracing::warn!(
                        target: "walshadow::backfill",
                        path = %path.display(),
                        error = %e,
                        "backfill ledger unreadable; treating as empty",
                    );
                }
            }
        }
        ledger
    }

    /// Crash-safe persist via [`crate::fs::write_atomic`].
    async fn persist(&self) -> std::io::Result<()> {
        let file = LedgerFile {
            version: LEDGER_VERSION,
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
                })
                .collect(),
        };
        let text = toml::to_string(&file).expect("ledger serialize");
        crate::fs::write_atomic(&self.dir, LEDGER_FILENAME, text.as_bytes()).await
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
    let mut cols = Vec::new();
    let mut natts = 0usize;
    for a in &desc.attributes {
        natts = natts.max(a.attnum.max(0) as usize);
        if a.dropped {
            continue;
        }
        let ident = quote_ident(&a.name);
        let (expr, kind) = match wire_kind(a.type_oid) {
            Some(k) => (ident, k),
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
        });
    }
    CopyPlan {
        select,
        cols,
        natts,
    }
}

fn fixed<const N: usize>(raw: &[u8], what: &str) -> Result<[u8; N], String> {
    raw.try_into()
        .map_err(|_| format!("{what}: expected {N} bytes, got {}", raw.len()))
}

fn text_or_bytea(raw: &[u8], wrap: fn(String) -> ColumnValue) -> ColumnValue {
    std::str::from_utf8(raw).map_or_else(|_| ColumnValue::Bytea(raw.to_vec()), |s| wrap(s.into()))
}

/// Decode one non-NULL wire field into the same [`ColumnValue`] variant the
/// WAL heap decoder produces for that column.
fn decode_field(kind: WireKind, raw: &[u8]) -> Result<ColumnValue, String> {
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
        WireKind::Text | WireKind::CastText => text_or_bytea(raw, ColumnValue::Text),
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
    /// Boot emitter; [`Self::dest_emitter`] overlays the live CH connection
    emitter: Arc<EmitterConfig>,
    /// Last [`Self::dest_emitter`] snapshot, rebuilt only when the destination
    /// moves. The mapping tables ride along in an `EmitterConfig`, so a
    /// per-relation rebuild would deep-copy every mapped table.
    dest: std::sync::Mutex<Arc<EmitterConfig>>,
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
    /// Pipeline's resident-payload pool: backup passes run concurrently
    /// with live streaming and draw from the same budget
    budget: Option<crate::budget::MemoryBudget>,
    oracle: Option<Arc<Oracle>>,
    /// Fixed scratch paths require one cluster backup pass at a time
    backup_pass_lock: Mutex<()>,
    inner: Mutex<Inner>,
    /// Ledger entries not yet `done` (gauge; mirrors the ledger under the lock).
    pending: AtomicU64,
    /// Per-mode split of `pending`: copy / base_backup / object_store.
    pending_by_mode: [AtomicU64; 3],
    coalesce_window: Duration,
}

impl CopyBackfiller {
    #[allow(clippy::too_many_arguments)]
    pub async fn new(
        pg: PgConfig,
        emitter: EmitterConfig,
        mapping: MappingHandle,
        stats: Arc<EmitterStats>,
        catalog: Arc<Mutex<ShadowCatalog>>,
        log: Arc<crate::catalog::desc_log::DescriptorLog>,
        spill_dir: &Path,
        config_rx: Option<watch::Receiver<Arc<ResolvedConfig>>>,
        budget: Option<crate::budget::MemoryBudget>,
        oracle: Option<Arc<Oracle>>,
    ) -> Self {
        let ledger = Ledger::load(spill_dir).await;
        let emitter = Arc::new(emitter);
        let pending = AtomicU64::new(ledger.pending_count());
        let pending_by_mode = [
            AtomicU64::new(ledger.pending_count_for(InitialLoadMode::Copy)),
            AtomicU64::new(ledger.pending_count_for(InitialLoadMode::BaseBackup)),
            AtomicU64::new(ledger.pending_count_for(InitialLoadMode::ObjectStore)),
        ];
        Self {
            pg,
            dest: std::sync::Mutex::new(emitter.clone()),
            emitter,
            mapping,
            stats,
            catalog,
            log,
            spill_dir: spill_dir.to_path_buf(),
            config_rx,
            budget,
            oracle,
            backup_pass_lock: Mutex::new(()),
            inner: Mutex::new(Inner {
                ledger,
                active: HashSet::new(),
                queued: HashMap::new(),
            }),
            pending,
            pending_by_mode,
            coalesce_window: BACKUP_COALESCE_WINDOW,
        }
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
    fn dest_emitter(&self) -> Arc<EmitterConfig> {
        let mut dest = self.dest.lock().expect("dest emitter poisoned");
        let Some(rc) = self.config_rx.as_ref().map(|rx| rx.borrow().clone()) else {
            return dest.clone();
        };
        if !rc.dest_conn_eq(&dest) {
            *dest = Arc::new(rc.overlay_dest(&self.emitter));
        }
        dest.clone()
    }

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
                            s_lsn: opt_in_lsn.into(),
                            done: false,
                            mode,
                            swapped: false,
                            staging_uuid: None,
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
                    (opt_in_lsn.into(), mode)
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
    /// pass leaves every entry pending (next boot's seed re-queues them) and
    /// never poisons the pump.
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
                    rows_walked = outcome.rows_walked,
                    rows_gated = outcome.rows_gated,
                    rows_deferred = outcome.rows_deferred,
                    multixact_emitted = outcome.multixact_emitted,
                    rows_replayed = outcome.rows_replayed,
                    replay_commits_past_s = outcome.replay_commits_past_s,
                    gap_segments = outcome.gap_segments,
                    pg_xact_segments = outcome.pg_xact_segments,
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
                    "backup backfill pass failed; entries stay pending (re-run on next boot)",
                );
            }
        }
        let mut inner = self.inner.lock().await;
        for r in &reqs {
            inner.active.remove(&r.desc.rel_name);
        }
        self.refresh_gauges(&inner.ledger);
    }

    /// Staged pass (plans/add_table.md §Staging swap): rows land in per-rel
    /// staging tables, success publishes each rel via EXCHANGE + copy-back.
    /// Per-rel ledger transitions ride [`Self::publish_staged`]; this frame
    /// only reports the load itself.
    async fn staged_pass(
        &self,
        mode: InitialLoadMode,
        reqs: &[BackupRequest],
    ) -> anyhow::Result<PassOutcome> {
        let dest = self.dest_emitter();
        let staging = backfill_staging::prepare(dest.clone(), &self.mapping, reqs)
            .await
            .context("staging prepare")?;
        let ctx = PassContext {
            pg: self.source_pg(),
            emitter: dest,
            mapping: staging.mapping.clone(),
            stats: self.stats.clone(),
            catalog: self.catalog.clone(),
            log: self.log.clone(),
            scratch_dir: self.spill_dir.join("backup_backfill"),
            config_rx: self.config_rx.clone(),
            budget: self.budget.clone(),
            oracle: self.oracle.clone(),
        };
        let outcome = crate::backfill::backup_backfill::run_pass(&ctx, mode, reqs).await?;
        self.publish_staged(&staging, reqs).await;
        Ok(outcome)
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
        let mut sess = match StagingSession::connect(self.dest_emitter())
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
        let mut swapped: Vec<&StagingRel> = Vec::new();
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
        tokio::time::sleep(self.emitter.insert_timeout).await;
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
        sess.exchange(rel).await?;
        Ok(true)
    }

    async fn finish_swapped(
        &self,
        sess: &mut StagingSession,
        rel: &StagingRel,
    ) -> anyhow::Result<()> {
        sess.copy_back(rel).await?;
        sess.drop_staging(rel).await?;
        self.mark_done_entry(&rel.rel).await;
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
        let mut sess = StagingSession::connect(self.dest_emitter())
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
        tokio::time::sleep(self.emitter.insert_timeout).await;
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
        let mut inner = self.inner.lock().await;
        inner.active.remove(&desc.rel_name);
        match res {
            Ok(outcome) => {
                if let Some(entry) = inner.ledger.entries.get_mut(&desc.rel_name) {
                    entry.done = true;
                    if let Err(e) = inner.ledger.persist().await {
                        tracing::warn!(
                            target: "walshadow::backfill",
                            qname = %desc.rel_name,
                            error = %e,
                            "backfill ledger persist failed; next boot re-COPYs (idempotent)",
                        );
                    }
                }
                tracing::info!(
                    target: "walshadow::backfill",
                    qname = %desc.rel_name,
                    rows = outcome.rows,
                    s_lsn = %s_lsn,
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
                    qname = %desc.rel_name,
                    error = %format!("{e:#}"),
                    "backfill failed; entry stays pending (re-COPY on next boot)",
                );
            }
        }
        self.refresh_gauges(&inner.ledger);
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
        let client = open_sql_client(&self.source_pg())
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
        let (msg_tx, ack, tail) = tail::spawn_with_config(
            &self.dest_emitter(),
            1,
            self.stats.clone(),
            Arc::new(crate::pos::Monotone::new(0)),
            fatal.clone(),
            self.config_rx.clone(),
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
            // Disabled resolver never defers; spool stays empty
            ToastResolver::disabled(),
            crate::backfill::spool::DeferredSpool::new(
                self.spill_dir.join("copy_deferred.bin"),
                crate::backfill::spool::DEFERRED_SPOOL_MEM_MAX,
            ),
            self.emitter.row_policy(),
            self.config_rx.as_ref().map(|rx| rx.borrow().clone()),
            None,
            std::collections::HashSet::new(),
        ));

        let plan = column_plan(desc);
        let sql = format!(
            "COPY (SELECT {} FROM {qtable}) TO STDOUT (FORMAT binary)",
            plan.select
        );
        let rows = {
            let byte_fields = vec![Type::BYTEA; plan.cols.len()];
            let copy = client.copy_out(&sql).await.context("backfill: COPY out")?;
            let stream = BinaryCopyOutStream::new(copy, &byte_fields);
            futures::pin_mut!(stream);
            let mut rows = 0u64;
            while let Some(row) = stream.next().await {
                let row = row.context("backfill: COPY stream")?;
                let mut columns: Vec<Option<ColumnValue>> = vec![None; plan.natts];
                for (i, cp) in plan.cols.iter().enumerate() {
                    let raw: Option<&[u8]> = row.try_get(i).context("backfill: COPY field")?;
                    let v = raw
                        .map(|raw| decode_field(cp.kind, raw))
                        .transpose()
                        .map_err(anyhow::Error::msg)?
                        .unwrap_or(ColumnValue::Null);
                    columns[(cp.attnum - 1).max(0) as usize] = Some(v);
                }
                tup_tx
                    .send(BackfillTuple {
                        rfn: desc.rfn,
                        xid: 0,
                        xmax: 0,
                        infomask: 0,
                        source_lsn: s_lsn.get(),
                        // COPY text rows have no on-page TID (values arrive
                        // detoasted, no chunks flow here)
                        blkno: 0,
                        offnum: 0,
                        columns,
                    })
                    .await
                    .map_err(|_| anyhow::anyhow!("backfill: drain closed early"))?;
                rows += 1;
            }
            rows
        };
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
        ]);
        let plan = column_plan(&d);
        assert_eq!(plan.select, "\"id\", \"price\"::text, \"tags\"::text");
        assert_eq!(plan.natts, 4);
        assert_eq!(plan.cols.len(), 3, "dropped column not selected");
        assert_eq!(plan.cols[0].kind, WireKind::Int8);
        assert_eq!(plan.cols[1].kind, WireKind::NumericText);
        assert_eq!(plan.cols[2].kind, WireKind::CastText);
        assert_eq!(plan.cols[2].attnum, 4);
    }

    #[test]
    fn decode_matches_wal_variants() {
        assert_eq!(
            decode_field(WireKind::Int8, &7i64.to_be_bytes()).unwrap(),
            ColumnValue::Int8(7),
        );
        assert_eq!(
            decode_field(WireKind::Bool, &[1]).unwrap(),
            ColumnValue::Bool(true),
        );
        assert_eq!(
            decode_field(WireKind::Date, &8000i32.to_be_bytes()).unwrap(),
            ColumnValue::Date(8000),
        );
        assert_eq!(
            decode_field(WireKind::Text, b"abc").unwrap(),
            ColumnValue::Text("abc".into()),
        );
        assert_eq!(
            decode_field(WireKind::Name, b"orders").unwrap(),
            ColumnValue::Name("orders".into()),
        );
        assert_eq!(
            decode_field(WireKind::Json, b"{\"a\": 1}").unwrap(),
            ColumnValue::Json("{\"a\": 1}".into()),
        );
        assert_eq!(
            decode_field(WireKind::NumericText, b"12.50").unwrap(),
            ColumnValue::Numeric(NumericKind::Finite("12.50".into())),
        );
        assert_eq!(
            decode_field(WireKind::NumericText, b"NaN").unwrap(),
            ColumnValue::Numeric(NumericKind::NaN),
        );
        assert_eq!(
            decode_field(WireKind::NumericText, b"Infinity").unwrap(),
            ColumnValue::Numeric(NumericKind::PInf),
        );
        assert_eq!(
            decode_field(WireKind::NumericText, b"-Infinity").unwrap(),
            ColumnValue::Numeric(NumericKind::NInf),
        );
        assert_eq!(
            decode_field(WireKind::CastText, b"{1,2}").unwrap(),
            ColumnValue::Text("{1,2}".into()),
        );
        // Wrong width is an error, not a silent misread
        assert!(decode_field(WireKind::Int4, &[0, 1]).is_err());
        // Invalid UTF-8 degrades to Bytea like the heap decoder
        assert_eq!(
            decode_field(WireKind::Text, &[0xFF, 0xFE]).unwrap(),
            ColumnValue::Bytea(vec![0xFF, 0xFE]),
        );
    }

    #[tokio::test]
    async fn ledger_round_trips_and_survives_corruption() {
        let tmp = tempfile::tempdir().unwrap();
        let mut ledger = Ledger::load(tmp.path()).await;
        assert_eq!(ledger.pending_count(), 0);
        ledger.entries.insert(
            RelName::new("app", "orders"),
            LedgerRec {
                s_lsn: 0x1000.into(),
                done: false,
                mode: InitialLoadMode::Copy,
                swapped: false,
                staging_uuid: None,
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
            },
        );
        ledger.persist().await.unwrap();

        let again = Ledger::load(tmp.path()).await;
        let orders = again.entries.get(&RelName::new("app", "orders")).unwrap();
        assert_eq!((orders.s_lsn.get(), orders.done), (0x1000, false));
        assert_eq!(orders.mode, InitialLoadMode::Copy);
        assert!(!orders.swapped);
        let done = again.entries.get(&RelName::new("app", "done")).unwrap();
        assert_eq!((done.s_lsn.get(), done.done), (0x800, true));
        assert_eq!(done.mode, InitialLoadMode::ObjectStore, "mode round-trips");
        let mid = again.entries.get(&RelName::new("app", "mid_swap")).unwrap();
        assert!(mid.swapped, "swap phase round-trips");
        assert_eq!(mid.staging_uuid.as_deref(), Some("a-uuid"));
        assert_eq!(again.pending_count(), 2, "swapped counts as pending");
        assert_eq!(again.pending_count_for(InitialLoadMode::Copy), 1);
        assert_eq!(again.pending_count_for(InitialLoadMode::ObjectStore), 1);

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
