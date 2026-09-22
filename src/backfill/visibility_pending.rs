//! Retain undecided backup rows in ClickHouse until commit or abort
//!
//! Row WAL can predate backup redo while its transaction remains open at
//! handoff. Store these rows in `<table>__wspending` on plain MergeTree:
//! ReplacingMergeTree could collapse competing versions before visibility
//! is known. Promote survivors at original load version so later WAL wins
//!
//! Keep outcomes across settlement rounds for rows waiting on both insert
//! and delete xids. Promote before persisting ledger, allowing destination
//! dedup to absorb crash retries. Missing `pg_xact` history proves no outcome
//!
//! See plans/bootstrap.md for storage format and recovery ordering

use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::Ordering;

use ahash::{HashMap, HashMapExt, HashSet, HashSetExt};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::mpsc;

use crate::backfill::backfill_staging::{StagingSession, sql_str};
use crate::backfill::backup_page_walk::{BOOTSTRAP_TUPLE_CHANNEL_CAP, BackfillTuple, CatalogMap};
use crate::backfill::spool::{DEFERRED_SPOOL_MEM_MAX, DeferredSpool};
use crate::ch::quote_ident;
use crate::config::ResolvedConfig;
use crate::decode::heap_decoder::ColumnValue;
use crate::decode::visibility::{PendingXids, PgXactView, XidStatus};
use crate::emit::ch_emitter::{EmitterConfig, EmitterStats};
use crate::emit::pipeline::tail::OwnedTail;
use crate::emit::pipeline::{Fatal, bootstrap};
use crate::mapping::{ColumnMapping, MappingSnapshot, TableMapping, TableTarget};
use crate::ops::oracle::Oracle;
use crate::pos::{Pos, Snapshot};
use crate::schema::{RelDescriptor, RelName};
use crate::toast::ToastResolver;

/// Stable names let retries and startup find existing pending tables
pub const PENDING_SUFFIX: &str = "__wspending";

// Preserve existing restart state filename and serialized entry key
pub const PENDING_LEDGER_FILENAME: &str = "visibility_carry.toml";

/// Bump on any schema change; load rejects mismatched versions
pub const PENDING_LEDGER_VERSION: u32 = 1;

/// Xid whose commit the row waits on
const XMIN_COLUMN: &str = "_ws_xmin";
/// Xid whose abort the row waits on
const XMAX_COLUMN: &str = "_ws_xmax";
/// Raw `t_infomask` behind both reductions
const INFOMASK_COLUMN: &str = "_ws_infomask";

/// Pending rows per replay slab
const PENDING_SLAB_ROWS: usize = 256;

/// One destination and its pending table
#[derive(Debug, Clone)]
pub struct PendingRel {
    pub rel: RelName,
    pub database: String,
    pub table: String,
}

impl PendingRel {
    pub fn pending_table(&self) -> String {
        format!("{}{PENDING_SUFFIX}", self.table)
    }

    pub fn target_sql(&self) -> String {
        format!(
            "{}.{}",
            quote_ident(&self.database),
            quote_ident(&self.table)
        )
    }

    pub fn pending_sql(&self) -> String {
        format!(
            "{}.{}",
            quote_ident(&self.database),
            quote_ident(&self.pending_table())
        )
    }
}

fn metadata_base(rel: &RelDescriptor) -> usize {
    rel.attributes
        .iter()
        .map(|a| a.attnum.max(0) as usize)
        .max()
        .unwrap_or(0)
}

fn pending_table_mapping(
    mapping: &TableMapping,
    rel: &RelDescriptor,
    pending: &PendingRel,
) -> TableMapping {
    let base = metadata_base(rel) as i16;
    let meta = [
        (XMIN_COLUMN, "UInt32"),
        (XMAX_COLUMN, "UInt32"),
        (INFOMASK_COLUMN, "UInt16"),
    ];
    let mut columns = mapping.columns.clone();
    columns.extend(
        meta.iter()
            .enumerate()
            .map(|(i, (name, ty))| ColumnMapping {
                src_attnum: base + 1 + i as i16,
                target_name: (*name).into(),
                target_type: (*ty).into(),
            }),
    );
    TableMapping {
        target: TableTarget::new(&pending.database, &pending.pending_table()),
        columns,
    }
}

fn inject_metadata(tuple: &mut BackfillTuple, rel: &RelDescriptor, pending: PendingXids) {
    let base = metadata_base(rel);
    let infomask = tuple.infomask as i16;
    debug_assert!(
        tuple.columns.len() <= base,
        "walk indexes columns by attnum-1 against this descriptor",
    );
    tuple.columns.resize(base, None);
    tuple.columns.push(Some(ColumnValue::Oid(pending.insert)));
    tuple.columns.push(Some(ColumnValue::Oid(pending.delete)));
    tuple.columns.push(Some(ColumnValue::Int2(infomask)));
}

#[derive(Debug)]
struct RelTally {
    xids: HashSet<u32>,
    start_lsn: u64,
    rows: u64,
}

/// Spool undecided tuples with relation and transaction metadata
pub struct PendingSpool {
    spool: DeferredSpool,
    catalog: CatalogMap,
    tally: HashMap<RelName, RelTally>,
}

impl PendingSpool {
    pub fn new(path: PathBuf, catalog: CatalogMap) -> Self {
        Self {
            spool: DeferredSpool::new(path, DEFERRED_SPOOL_MEM_MAX),
            catalog,
            tally: HashMap::new(),
        }
    }

    pub fn rows(&self) -> u64 {
        self.spool.records()
    }

    /// Return false when descriptor is absent
    pub async fn push(
        &mut self,
        mut tuple: BackfillTuple,
        pending: PendingXids,
    ) -> Result<bool, String> {
        let Some(rel) = self.catalog.get(tuple.rfn.db_node, tuple.rfn.rel_node) else {
            return Ok(false);
        };
        let tally = self
            .tally
            .entry(rel.rel_name.clone())
            .or_insert_with(|| RelTally {
                xids: HashSet::new(),
                start_lsn: u64::MAX,
                rows: 0,
            });
        for xid in [pending.insert, pending.delete] {
            if xid != 0 {
                tally.xids.insert(xid);
            }
        }
        tally.start_lsn = tally.start_lsn.min(tuple.source_lsn);
        tally.rows += 1;
        inject_metadata(&mut tuple, &rel, pending);
        self.spool
            .push(tuple)
            .await
            .map_err(|e| format!("pending visibility: spool: {e}"))?;
        Ok(true)
    }

    pub async fn discard(self) {
        self.spool.discard().await;
    }
}

/// Pending rows for one relation, recorded after the pass publishes
#[derive(Debug, Clone)]
pub struct PendingManifest {
    pub rel: PendingRel,
    pub start_lsn: u64,
    pub xids: Vec<u32>,
    pub rows: u64,
}

/// Rebuild pending tables on retry so failed passes leave no stale rows
#[allow(clippy::too_many_arguments)]
pub async fn ship(
    pending: PendingSpool,
    live: &MappingSnapshot,
    emitter: Arc<EmitterConfig>,
    stats: Arc<EmitterStats>,
    resolver: ToastResolver,
    config: Option<Arc<ResolvedConfig>>,
    oracle: Option<Arc<Oracle>>,
    scratch_dir: &Path,
) -> Result<Vec<PendingManifest>, String> {
    let PendingSpool {
        spool,
        catalog,
        tally,
    } = pending;
    if spool.records() == 0 {
        spool.discard().await;
        return Ok(Vec::new());
    }

    let mut routes: HashMap<RelName, TableMapping> = HashMap::with_capacity(tally.len());
    let mut manifests = Vec::with_capacity(tally.len());
    let mut sess = StagingSession::connect(emitter.clone())
        .await
        .map_err(|e| format!("pending visibility: connect: {e}"))?;
    for desc in catalog.descriptors() {
        let Some(counted) = tally.get(&desc.rel_name) else {
            continue;
        };
        let Some(m) = live.get(&desc.rel_name) else {
            // Unmapped at resolution: rows had nowhere to publish either
            tracing::warn!(
                target: "walshadow::visibility_pending",
                qname = %desc.rel_name,
                "no mapping for pending relation; undecided rows discarded",
            );
            continue;
        };
        let rel = PendingRel {
            rel: desc.rel_name.clone(),
            database: m.target.database.clone(),
            table: m.target.table.clone(),
        };
        rebuild_pending(&mut sess, &rel).await?;
        routes.insert(desc.rel_name.clone(), pending_table_mapping(m, desc, &rel));
        manifests.push(PendingManifest {
            rel,
            start_lsn: counted.start_lsn,
            xids: counted.xids.iter().copied().collect(),
            rows: counted.rows,
        });
    }
    if routes.is_empty() {
        spool.discard().await;
        return Ok(Vec::new());
    }

    let tail = OwnedTail::spawn(
        &emitter,
        1,
        stats.clone(),
        Fatal::new(),
        None,
        oracle,
        "pending visibility",
    )
    .await?;
    // Stale file from a crashed pass blocks create_new
    let deferred = scratch_dir.join("pending_deferred.bin");
    tokio::fs::remove_file(&deferred).await.ok();
    let (tx, rx) = mpsc::channel::<Vec<BackfillTuple>>(BOOTSTRAP_TUPLE_CHANNEL_CAP);
    let drain = tokio::spawn(bootstrap::drain(
        rx,
        catalog,
        Arc::new(routes),
        tail.msg_tx.clone(),
        tail.ack.clone(),
        stats.clone(),
        resolver,
        bootstrap::Deferral::Local(DeferredSpool::new(deferred, DEFERRED_SPOOL_MEM_MAX)),
        emitter.row_policy(),
        config,
        HashSet::new(),
        None,
    ));

    let replayed = replay(spool, &tx).await;
    drop(tx);
    let drained = drain
        .await
        .map_err(|e| format!("pending visibility: drain join: {e}"))?;
    match (replayed, drained) {
        (Ok(()), Ok(outcome)) => tail.finish(outcome.next_seq).await?,
        (Err(e), _) | (_, Err(e)) => {
            tail.quiesce().await;
            return Err(e);
        }
    }

    let rows: u64 = manifests.iter().map(|m| m.rows).sum();
    stats.pending_rows.fetch_add(rows, Ordering::Relaxed);
    stats
        .pending_tables
        .fetch_add(manifests.len() as u64, Ordering::Relaxed);
    Ok(manifests)
}

async fn replay(spool: DeferredSpool, tx: &mpsc::Sender<Vec<BackfillTuple>>) -> Result<(), String> {
    let mut reader = spool
        .into_reader()
        .await
        .map_err(|e| format!("pending visibility: spool seal: {e}"))?;
    let mut slab = Vec::with_capacity(PENDING_SLAB_ROWS);
    while let Some(t) = reader
        .next()
        .await
        .map_err(|e| format!("pending visibility: spool replay: {e}"))?
    {
        slab.push(t);
        if slab.len() >= PENDING_SLAB_ROWS {
            let full = std::mem::replace(&mut slab, Vec::with_capacity(PENDING_SLAB_ROWS));
            if tx.send(full).await.is_err() {
                break;
            }
        }
    }
    if !slab.is_empty() {
        let _ = tx.send(slab).await;
    }
    reader
        .finish()
        .await
        .map_err(|e| format!("pending visibility: spool cleanup: {e}"))?;
    Ok(())
}

/// Use plain MergeTree to retain competing UPDATE versions
/// Name every key clause: ClickHouse `CREATE … AS` inherits omitted keys
/// (src/Interpreters/InterpreterCreateQuery.cpp)
fn pending_create_sql(rel: &PendingRel) -> String {
    format!(
        "CREATE TABLE IF NOT EXISTS {} AS {} ENGINE = MergeTree \
         ORDER BY tuple() PRIMARY KEY tuple() PARTITION BY tuple()",
        rel.pending_sql(),
        rel.target_sql()
    )
}

async fn rebuild_pending(sess: &mut StagingSession, rel: &PendingRel) -> Result<(), String> {
    let pending = rel.pending_sql();
    sess.exec_retry(&format!("DROP TABLE IF EXISTS {pending}"))
        .await
        .map_err(|e| e.to_string())?;
    sess.exec_retry(&pending_create_sql(rel))
        .await
        .map_err(|e| e.to_string())?;
    for (name, ty) in [
        (XMIN_COLUMN, "UInt32"),
        (XMAX_COLUMN, "UInt32"),
        (INFOMASK_COLUMN, "UInt16"),
    ] {
        sess.exec_retry(&format!(
            "ALTER TABLE {pending} ADD COLUMN IF NOT EXISTS {} {ty}",
            quote_ident(name)
        ))
        .await
        .map_err(|e| e.to_string())?;
    }
    Ok(())
}

#[derive(Debug, Error)]
pub enum PendingLedgerError {
    #[error("io: {0}")]
    Io(#[from] io::Error),
    #[error("ledger parse: {0}")]
    Parse(#[from] toml::de::Error),
    #[error("unsupported ledger schema version {0} (this build expects {PENDING_LEDGER_VERSION})")]
    Version(u32),
}

#[derive(Serialize, Deserialize)]
struct PendingFile {
    version: u32,
    #[serde(default)]
    system_id: Option<u64>,
    #[serde(default, rename = "carry")]
    entries: Vec<PendingEntry>,
}

/// Durable state for one pending table
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PendingEntry {
    pub namespace: String,
    pub relname: String,
    pub database: String,
    pub table: String,
    /// Preserve load version through promotion
    pub start_lsn: Pos<Snapshot>,
    /// Deciding xids whose outcome is still unknown
    #[serde(default)]
    pub outstanding: Vec<u32>,
    #[serde(default)]
    pub committed: Vec<u32>,
    #[serde(default)]
    pub aborted: Vec<u32>,
    /// Limit promotion to newly resolved rows; relearn after crash for retry
    #[serde(skip)]
    fresh_committed: Vec<u32>,
    #[serde(skip)]
    fresh_aborted: Vec<u32>,
}

impl PendingEntry {
    fn rel(&self) -> PendingRel {
        PendingRel {
            rel: RelName::new(&self.namespace, &self.relname),
            database: self.database.clone(),
            table: self.table.clone(),
        }
    }

    fn note(&mut self, xid: u32, committed: bool) -> bool {
        let Some(i) = self.outstanding.iter().position(|x| *x == xid) else {
            return false;
        };
        self.outstanding.swap_remove(i);
        self.decided(xid, committed);
        true
    }

    fn decided(&mut self, xid: u32, committed: bool) {
        let (side, fresh) = if committed {
            (&mut self.committed, &mut self.fresh_committed)
        } else {
            (&mut self.aborted, &mut self.fresh_aborted)
        };
        side.push(xid);
        fresh.push(xid);
    }

    fn has_fresh(&self) -> bool {
        !self.fresh_committed.is_empty() || !self.fresh_aborted.is_empty()
    }
}

/// Persist pending tables and transaction outcomes for restart recovery
#[derive(Debug)]
pub struct PendingLedger {
    dir: PathBuf,
    system_id: u64,
    entries: Vec<PendingEntry>,
}

/// One ledger shared by live commit folding and backup passes adding
/// tables, so neither persists a copy missing the other's state
pub type SharedPendingLedger = Arc<tokio::sync::Mutex<PendingLedger>>;

pub fn ledger_path(spill_dir: &Path) -> PathBuf {
    spill_dir.join(PENDING_LEDGER_FILENAME)
}

impl PendingLedger {
    /// Treat missing file as empty; reject corrupt state and state another
    /// source system wrote
    pub async fn load(spill_dir: &Path, system_id: u64) -> Result<Self, PendingLedgerError> {
        let mut ledger = Self {
            dir: spill_dir.to_path_buf(),
            system_id,
            entries: Vec::new(),
        };
        let path = ledger_path(spill_dir);
        match tokio::fs::read_to_string(&path).await {
            Ok(text) => {
                let file: PendingFile = toml::from_str(&text)?;
                if file.version != PENDING_LEDGER_VERSION {
                    return Err(PendingLedgerError::Version(file.version));
                }
                let unstamped = crate::fs::check_source(&path, file.system_id, system_id)?;
                ledger.entries = file.entries;
                if unstamped {
                    ledger.persist().await?;
                }
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(e.into()),
        }
        Ok(ledger)
    }

    /// Use without ClickHouse; skip disk writes
    pub fn empty() -> Self {
        Self {
            dir: PathBuf::new(),
            system_id: 0,
            entries: Vec::new(),
        }
    }

    pub fn shared(self) -> SharedPendingLedger {
        Arc::new(tokio::sync::Mutex::new(self))
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn entries(&self) -> &[PendingEntry] {
        &self.entries
    }

    pub fn outstanding(&self) -> HashSet<u32> {
        let mut out = HashSet::new();
        for e in &self.entries {
            out.extend(e.outstanding.iter().copied());
        }
        out
    }

    pub async fn persist(&self) -> io::Result<()> {
        if self.dir.as_os_str().is_empty() {
            return Ok(());
        }
        let text = toml::to_string(&PendingFile {
            version: PENDING_LEDGER_VERSION,
            system_id: Some(self.system_id),
            entries: self.entries.clone(),
        })
        .expect("pending ledger serialize");
        crate::fs::write_atomic(&self.dir, PENDING_LEDGER_FILENAME, text.as_bytes()).await
    }

    /// Replace previous pass state after rebuilding and publishing tables
    pub async fn push(&mut self, manifest: &PendingManifest) -> io::Result<()> {
        self.stage(manifest);
        self.persist().await
    }

    /// [`Self::push`] without persisting
    pub fn stage(&mut self, manifest: &PendingManifest) {
        let mut xids = manifest.xids.clone();
        xids.sort_unstable();
        let entry = PendingEntry {
            namespace: manifest.rel.rel.namespace.to_string(),
            relname: manifest.rel.rel.name.to_string(),
            database: manifest.rel.database.clone(),
            table: manifest.rel.table.clone(),
            start_lsn: Pos::new(manifest.start_lsn),
            outstanding: xids,
            committed: Vec::new(),
            aborted: Vec::new(),
            fresh_committed: Vec::new(),
            fresh_aborted: Vec::new(),
        };
        self.entries
            .retain(|e| e.namespace != entry.namespace || e.relname != entry.relname);
        self.entries.push(entry);
    }

    /// Record transaction and subtransaction outcomes; return settled count
    pub fn note(&mut self, xid: u32, subxacts: &[u32], committed: bool) -> u64 {
        let mut settled = 0;
        for e in &mut self.entries {
            for x in std::iter::once(&xid).chain(subxacts) {
                settled += u64::from(e.note(*x, committed));
            }
        }
        settled
    }

    /// Recover outcomes absent from resumed WAL; retain unknown xids
    pub fn note_view(&mut self, view: &PgXactView) -> ViewFold {
        let mut fold = ViewFold::default();
        for e in &mut self.entries {
            for xid in std::mem::take(&mut e.outstanding) {
                match view.xid_status(xid) {
                    XidStatus::Committed => {
                        e.decided(xid, true);
                        fold.settled += 1;
                    }
                    XidStatus::Aborted => {
                        e.decided(xid, false);
                        fold.settled += 1;
                    }
                    // Copied rows lack vacuum evidence for aged-out xids
                    XidStatus::Unknown => {
                        e.outstanding.push(xid);
                        fold.undecidable += 1;
                    }
                    XidStatus::InProgress => e.outstanding.push(xid),
                }
            }
        }
        fold
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct ViewFold {
    /// Deciding xids the view settled
    pub settled: u64,
    /// Outstanding xids absent from transaction history
    pub undecidable: u64,
}

/// Promote before persisting so crash retries deduplicate instead of losing rows
pub async fn settle(
    ledger: &mut PendingLedger,
    sess: &mut StagingSession,
    stats: &EmitterStats,
) -> Result<(), String> {
    let mut done = Vec::new();
    for (i, e) in ledger.entries.iter_mut().enumerate() {
        let rel = e.rel();
        // Crash may follow DROP but precede ledger persist
        if sess
            .table_uuid(&e.database, &rel.pending_table())
            .await
            .map_err(|err| format!("pending visibility: {}: {err}", rel.pending_sql()))?
            .is_none()
        {
            done.push(i);
            continue;
        }
        if e.has_fresh() {
            promote(sess, &rel, e).await?;
            e.fresh_committed.clear();
            e.fresh_aborted.clear();
        }
        if !e.outstanding.is_empty() {
            continue;
        }
        sess.exec_retry(&format!("DROP TABLE IF EXISTS {}", rel.pending_sql()))
            .await
            .map_err(|err| err.to_string())?;
        stats.pending_tables_dropped.fetch_add(1, Ordering::Relaxed);
        done.push(i);
    }
    for i in done.into_iter().rev() {
        ledger.entries.remove(i);
    }
    stats
        .pending_outstanding_xids
        .store(ledger.outstanding().len() as u64, Ordering::Relaxed);
    ledger
        .persist()
        .await
        .map_err(|e| format!("pending visibility: ledger persist: {e}"))
}

/// Intersect destination and pending columns to exclude metadata and later DDL
async fn promote(
    sess: &mut StagingSession,
    rel: &PendingRel,
    entry: &PendingEntry,
) -> Result<(), String> {
    let list = shared_columns(sess, rel).await?;
    let sql = promote_sql(rel, entry, &list);
    sess.exec_retry(&sql).await.map_err(|e| e.to_string())
}

fn promote_sql(rel: &PendingRel, entry: &PendingEntry, list: &str) -> String {
    format!(
        "INSERT INTO {} ({list}) SELECT {list} FROM {} WHERE {} AND {} AND {}",
        rel.target_sql(),
        rel.pending_sql(),
        settled_side(XMIN_COLUMN, &entry.committed),
        settled_side(XMAX_COLUMN, &entry.aborted),
        fresh_side(entry),
    )
}

async fn shared_columns(sess: &mut StagingSession, rel: &PendingRel) -> Result<String, String> {
    let target_columns = sess
        .query_strings(&format!(
            "SELECT name FROM system.columns WHERE database = {} AND table = {} \
             ORDER BY position",
            sql_str(&rel.database),
            sql_str(&rel.table)
        ))
        .await
        .map_err(|e| e.to_string())?;
    let pending_columns: HashSet<String> = sess
        .query_strings(&format!(
            "SELECT name FROM system.columns WHERE database = {} AND table = {} \
             ORDER BY position",
            sql_str(&rel.database),
            sql_str(&rel.pending_table())
        ))
        .await
        .map_err(|e| e.to_string())?
        .into_iter()
        .collect();
    let cols: Vec<String> = target_columns
        .iter()
        .filter(|c| pending_columns.contains(*c))
        .map(|c| quote_ident(c))
        .collect();
    if cols.is_empty() {
        return Err(format!(
            "pending visibility: no shared columns between {} and {}",
            rel.target_sql(),
            rel.pending_sql()
        ));
    }
    Ok(cols.join(", "))
}

fn settled_side(column: &str, decided: &[u32]) -> String {
    let c = quote_ident(column);
    if decided.is_empty() {
        return format!("{c} = 0");
    }
    format!("({c} = 0 OR {c} IN ({}))", xid_list(decided))
}

/// Exclude rows promoted in earlier rounds
fn fresh_side(entry: &PendingEntry) -> String {
    let parts: Vec<String> = [
        (XMIN_COLUMN, &entry.fresh_committed),
        (XMAX_COLUMN, &entry.fresh_aborted),
    ]
    .into_iter()
    .filter(|(_, xids)| !xids.is_empty())
    .map(|(col, xids)| format!("{} IN ({})", quote_ident(col), xid_list(xids)))
    .collect();
    if parts.is_empty() {
        return "0".into();
    }
    format!("({})", parts.join(" OR "))
}

fn xid_list(xids: &[u32]) -> String {
    xids.iter()
        .map(u32::to_string)
        .collect::<Vec<_>>()
        .join(", ")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backfill::backup_page_walk::make_rel_named;

    fn manifest(rel: &str, xids: Vec<u32>) -> PendingManifest {
        PendingManifest {
            rel: PendingRel {
                rel: RelName::new("public", rel),
                database: "db".into(),
                table: rel.into(),
            },
            start_lsn: 0x5000,
            xids,
            rows: 3,
        }
    }

    #[test]
    fn pending_rel_renders_sql_names() {
        let rel = manifest("orders", Vec::new()).rel;
        assert_eq!(rel.target_sql(), "`db`.`orders`");
        assert_eq!(rel.pending_table(), "orders__wspending");
        assert_eq!(rel.pending_sql(), "`db`.`orders__wspending`");
    }

    #[test]
    fn settled_side_without_outcomes_demands_a_zero() {
        assert_eq!(settled_side(XMIN_COLUMN, &[]), "`_ws_xmin` = 0");
        assert_eq!(
            settled_side(XMAX_COLUMN, &[7, 9]),
            "(`_ws_xmax` = 0 OR `_ws_xmax` IN (7, 9))"
        );
    }

    #[test]
    fn metadata_columns_align_with_their_pseudo_attnums() {
        let desc = make_rel_named(16400, 16400, 0, RelName::new("public", "t"));
        let m = TableMapping {
            target: TableTarget::new("db", "t"),
            columns: vec![ColumnMapping {
                src_attnum: 1,
                target_name: "id".into(),
                target_type: "Int32".into(),
            }],
        };
        let rel = manifest("t", Vec::new()).rel;
        let pending = pending_table_mapping(&m, &desc, &rel);
        assert_eq!(pending.target.table, "t__wspending");

        let mut tuple = BackfillTuple {
            rfn: desc.rfn,
            xid: 100,
            xmax: 200,
            infomask: 0x0100,
            source_lsn: 0x5000,
            blkno: 0,
            offnum: 0,
            columns: vec![Some(ColumnValue::Int4(1))],
        };
        inject_metadata(
            &mut tuple,
            &desc,
            PendingXids {
                insert: 100,
                delete: 200,
            },
        );
        for c in &pending.columns[1..] {
            let idx = (c.src_attnum - 1) as usize;
            let got = tuple.columns[idx].as_ref().expect("metadata value");
            let want = match c.target_name.as_str() {
                XMIN_COLUMN => ColumnValue::Oid(100),
                XMAX_COLUMN => ColumnValue::Oid(200),
                _ => ColumnValue::Int2(0x0100),
            };
            assert_eq!(got, &want, "{}", c.target_name);
        }
    }

    #[tokio::test]
    async fn ledger_round_trips_and_settles_across_rounds() {
        let tmp = tempfile::tempdir().unwrap();
        let mut ledger = PendingLedger::load(tmp.path(), 7).await.unwrap();
        assert!(ledger.is_empty());
        ledger
            .push(&manifest("orders", vec![101, 102]))
            .await
            .unwrap();

        // Insert commits while delete remains open
        assert_eq!(ledger.note(101, &[], true), 1);
        assert_eq!(ledger.note(999, &[], true), 0);
        ledger.persist().await.unwrap();

        let mut reloaded = PendingLedger::load(tmp.path(), 7).await.unwrap();
        assert_eq!(reloaded.entries()[0].committed, [101]);
        assert_eq!(reloaded.entries()[0].outstanding, [102]);
        assert_eq!(reloaded.outstanding(), [102].into_iter().collect());

        // Savepoint tuples use subtransaction xids
        assert_eq!(reloaded.note(500, &[102], false), 1);
        assert!(reloaded.entries()[0].outstanding.is_empty());
        assert_eq!(reloaded.entries()[0].aborted, [102]);
    }

    #[tokio::test]
    async fn promote_selects_only_settled_rows() {
        let tmp = tempfile::tempdir().unwrap();
        let mut ledger = PendingLedger::load(tmp.path(), 7).await.unwrap();
        ledger
            .push(&manifest("orders", vec![101, 102]))
            .await
            .unwrap();
        ledger.note(101, &[], true);
        ledger.note(102, &[], false);
        let entry = &ledger.entries()[0];
        assert_eq!(
            promote_sql(&entry.rel(), entry, "`id`, `_lsn`"),
            "INSERT INTO `db`.`orders` (`id`, `_lsn`) \
             SELECT `id`, `_lsn` FROM `db`.`orders__wspending` \
             WHERE (`_ws_xmin` = 0 OR `_ws_xmin` IN (101)) \
             AND (`_ws_xmax` = 0 OR `_ws_xmax` IN (102)) \
             AND (`_ws_xmin` IN (101) OR `_ws_xmax` IN (102))"
        );
    }

    #[tokio::test]
    async fn promote_names_only_the_current_round() {
        let tmp = tempfile::tempdir().unwrap();
        let mut ledger = PendingLedger::load(tmp.path(), 7).await.unwrap();
        ledger
            .push(&manifest("orders", vec![101, 102]))
            .await
            .unwrap();

        ledger.note(101, &[], true);
        let first = {
            let e = &ledger.entries()[0];
            promote_sql(&e.rel(), e, "`id`")
        };
        assert!(first.ends_with("AND (`_ws_xmin` IN (101))"), "{first}");

        ledger.entries[0].fresh_committed.clear();
        ledger.note(102, &[], false);
        let second = {
            let e = &ledger.entries()[0];
            promote_sql(&e.rel(), e, "`id`")
        };
        assert!(
            second.contains("(`_ws_xmin` = 0 OR `_ws_xmin` IN (101))"),
            "{second}"
        );
        assert!(second.ends_with("AND (`_ws_xmax` IN (102))"), "{second}");
    }

    #[test]
    fn pending_create_names_every_key_clause() {
        assert_eq!(
            pending_create_sql(&manifest("orders", Vec::new()).rel),
            "CREATE TABLE IF NOT EXISTS `db`.`orders__wspending` AS `db`.`orders` \
             ENGINE = MergeTree ORDER BY tuple() PRIMARY KEY tuple() PARTITION BY tuple()"
        );
    }

    #[tokio::test]
    async fn inverted_outcomes_promote_nothing() {
        let tmp = tempfile::tempdir().unwrap();
        let mut ledger = PendingLedger::load(tmp.path(), 7).await.unwrap();
        ledger
            .push(&manifest("orders", vec![101, 102]))
            .await
            .unwrap();
        ledger.note(101, &[], false);
        ledger.note(102, &[], true);
        let entry = &ledger.entries()[0];
        let sql = promote_sql(&entry.rel(), entry, "`id`");
        // Rows waiting on 101's commit or 102's abort match neither side
        assert!(
            sql.contains("(`_ws_xmin` = 0 OR `_ws_xmin` IN (102))"),
            "{sql}"
        );
        assert!(
            sql.contains("(`_ws_xmax` = 0 OR `_ws_xmax` IN (101))"),
            "{sql}"
        );
    }

    #[tokio::test]
    async fn note_view_keeps_aged_out_status_outstanding() {
        use crate::decode::visibility::{PgXactAccum, PgXactPatch};

        let tmp = tempfile::tempdir().unwrap();
        let mut ledger = PendingLedger::load(tmp.path(), 7).await.unwrap();
        ledger.push(&manifest("orders", vec![101])).await.unwrap();
        let accum = PgXactAccum::new();
        let patch = PgXactPatch::new();
        let fold = ledger.note_view(&PgXactView::new(&accum, &patch));
        assert_eq!(fold.settled, 0);
        assert_eq!(fold.undecidable, 1, "an aged-out xid is visible as such");
        assert_eq!(ledger.entries()[0].outstanding, [101]);
    }

    #[tokio::test]
    async fn corrupt_ledger_is_an_error() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::write(ledger_path(tmp.path()), "version = 1\n[[carry").unwrap();
        assert!(PendingLedger::load(tmp.path(), 7).await.is_err());
        std::fs::write(ledger_path(tmp.path()), "version = 999\n").unwrap();
        assert!(matches!(
            PendingLedger::load(tmp.path(), 7).await,
            Err(PendingLedgerError::Version(999))
        ));
    }

    #[tokio::test]
    async fn unstamped_ledger_upgrades_and_foreign_source_is_an_error() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::write(
            ledger_path(tmp.path()),
            "version = 1\n[[carry]]\nnamespace = 'public'\nrelname = 'orders'\n\
             database = 'db'\ntable = 'orders'\nstart_lsn = '0/5000'\noutstanding = [101]\n",
        )
        .unwrap();
        let ledger = PendingLedger::load(tmp.path(), 7).await.unwrap();
        assert_eq!(ledger.entries()[0].outstanding, [101]);
        let text = std::fs::read_to_string(ledger_path(tmp.path())).unwrap();
        assert!(text.contains("system_id = 7"), "{text}");
        let err = PendingLedger::load(tmp.path(), 8).await.unwrap_err();
        assert!(
            err.to_string().contains("belongs to source system 7"),
            "{err}"
        );
    }
}
