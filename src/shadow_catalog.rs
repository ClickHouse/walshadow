//! Shadow PG catalog cache.
//!
//! [`ShadowCatalog::relation_at`] resolution:
//! 1. Block until shadow's `pg_last_wal_replay_lsn()` ≥ `at_lsn`, so
//!    shadow's catalog reflects every catalog write source issued at or
//!    before that LSN
//! 2. Check cache keyed by `(rfn, generation)`
//! 3. Miss → resolve `rfn` via `pg_relation_filenode(oid)` (uniform over
//!    mapped catalogs and regular tables), fan-out to pg_attribute +
//!    pg_type + pg_namespace
//!
//! Generation invalidation: a
//! [`CatalogTracker`](crate::catalog_tracker::CatalogTracker) bumps a shared
//! `AtomicU64` on every catalog-touching record. Lookups read it at entry and
//! invalidate in-line BEFORE the cache check, so a DDL in the same batch as a
//! dependent heap INSERT can't race past the cache.
//!
//! Concurrency: methods take `&mut self`; cache state could be interior-mutable
//! (RwLock + atomics) but that refactor is deferred until a real lookup-rate hot
//! path exists. Concurrent callers wrap in `Arc<tokio::sync::Mutex<_>>`.
//!
//! Single-database model: instance bound to one DB. Shared catalogs
//! (`db_node == 0`) resolve from any connection via `pg_relation_filenode`.
//! Cross-user-database replay needs one cache per DB; out of scope.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use backon::{ExponentialBuilder, RetryableWithContext};
use thiserror::Error;
use tokio::sync::{Mutex, mpsc};
use tokio_postgres::types::{Oid, ToSql};
use tokio_postgres::{Client, NoTls, Row};
use tracing::Instrument;
use walrus::pg::walparser::RelFileNode;

use crate::shadow::parse_pg_lsn;

#[derive(Debug, Error)]
pub enum CatalogError {
    #[error("pg: {0}")]
    Pg(#[from] tokio_postgres::Error),
    #[error("relation not found by filenode {0:?}")]
    NotFoundByFilenode(RelFileNode),
    #[error("relation in foreign database {0:?} (not the shadow DB)")]
    ForeignDatabase(RelFileNode),
    #[error("relation not found by oid {0}")]
    NotFoundByOid(Oid),
    #[error("timeout after {elapsed:?} waiting for replay ≥ {target:#X} (last observed: {last:?})")]
    ReplayTimeout {
        target: u64,
        last: Option<u64>,
        elapsed: Duration,
    },
    #[error("parse: {0}")]
    Parse(String),
}

pub type Result<T> = std::result::Result<T, CatalogError>;

/// Fully-resolved description of one PG relation, sized for heap-tuple decoding.
#[derive(Debug, Clone, PartialEq)]
pub struct RelDescriptor {
    pub rfn: RelFileNode,
    pub oid: Oid,
    pub namespace_oid: Oid,
    pub namespace_name: String,
    pub name: String,
    /// `"{namespace_name}.{name}"` cached so hot-path consumers (CH emitter
    /// routing, qualified-name observers) skip per-row reformatting
    pub qualified_name: Arc<str>,
    /// `pg_class.relkind`: `'r'` table, `'i'` index, `'S'` sequence,
    /// `'t'` toast, `'v'` view, `'m'` matview, `'c'` composite,
    /// `'f'` foreign, `'p'` partitioned
    pub kind: char,
    /// `pg_class.relpersistence`: `'p'` permanent, `'u'` unlogged, `'t'` temporary
    pub persistence: char,
    /// `pg_class.relreplident` resolved with the key columns inlined, so
    /// old-tuple decode under `XLH_UPDATE_CONTAINS_OLD_KEY` needs no second
    /// catalog round-trip
    pub replident: ReplIdent,
    pub attributes: Vec<RelAttr>,
}

impl RelDescriptor {
    /// For callers building a descriptor manually (tests, bootstrap).
    pub fn build_qualified_name(namespace_name: &str, name: &str) -> Arc<str> {
        let mut s = String::with_capacity(namespace_name.len() + 1 + name.len());
        s.push_str(namespace_name);
        s.push('.');
        s.push_str(name);
        Arc::<str>::from(s)
    }
}

/// Resolved `pg_class.relreplident` (stored as a single char d/n/f/i). The
/// key-carrying variants need `pg_index.indkey` to interpret
/// `XLH_UPDATE_CONTAINS_OLD_KEY` / `XLH_UPDATE_CONTAINS_OLD_TUPLE` payloads.
#[derive(Debug, Clone, PartialEq)]
pub enum ReplIdent {
    /// `'d'`. Old-tuple payload carries PK columns under
    /// `XLH_UPDATE_CONTAINS_OLD_KEY` (UPDATE) or every DELETE. `pk_attnums` is
    /// `pg_index.indkey` for `indisprimary`, `None` when no PK → decoder yields
    /// `old = None`
    Default { pk_attnums: Option<Vec<i16>> },
    /// `'n'`. Old-tuple payload empty; emitter drops UPDATE/DELETE
    Nothing,
    /// `'f'`. Old-tuple payload mirrors every non-dropped column
    Full,
    /// `'i'`. Old-tuple payload carries `indexrelid`'s columns at `key_attnums`
    /// (`pg_index.indkey`)
    UsingIndex {
        index_oid: Oid,
        key_attnums: Vec<i16>,
    },
}

/// One column on a relation.
#[derive(Debug, Clone, PartialEq)]
pub struct RelAttr {
    pub attnum: i16,
    pub name: String,
    pub type_oid: Oid,
    pub typmod: i32,
    pub not_null: bool,
    pub dropped: bool,
    pub type_name: String,
    pub type_byval: bool,
    pub type_len: i16,
    /// `pg_type.typalign`: `'c'` 1, `'s'` 2, `'i'` 4, `'d'` 8
    pub type_align: char,
    /// `pg_type.typstorage`: `'p'` plain, `'e'` external (toast),
    /// `'m'` main (in-line, never compressed), `'x'` extended
    pub type_storage: char,
    /// `attmissingval[1]` typoutput text when `pg_attribute.atthasmissing`.
    /// PG's fast-path `ALTER TABLE ADD COLUMN ... DEFAULT k`
    /// (heaptuple.c `getmissingattr`) emits this for pre-ALTER rows whose
    /// physical tuple has fewer columns than the catalog
    pub missing_text: Option<String>,
}

/// Schema-mutation events for downstream consumers (CH DDL applicator + tests).
///
/// `Added` first time catalog learns an oid; `Changed` on a non-trivial shape
/// diff; `Dropped` when the decoder saw a pg_class heap_delete. Events flow in
/// fetch/drop order; callers stamp each onto the triggering WAL record's
/// xact-buffer position.
#[derive(Debug, Clone)]
pub enum SchemaEvent {
    Added {
        desc: Arc<RelDescriptor>,
    },
    Changed {
        old: Arc<RelDescriptor>,
        new: Arc<RelDescriptor>,
        diff: SchemaDiff,
    },
    Dropped {
        oid: Oid,
        qualified_name: Arc<str>,
    },
}

/// Diff `old → new` for one relation oid. Emitter runs one CH `ALTER` per entry.
///
/// `type_changes` is `(attnum, new_attr)`; old type recoverable from
/// `Changed.old.attributes`. Type changes currently rejected via
/// [`crate::type_bridge::BridgeError::UnsupportedType`]; widening is a follow-up.
#[derive(Debug, Default, Clone, PartialEq)]
pub struct SchemaDiff {
    pub added_columns: Vec<RelAttr>,
    pub dropped_columns: Vec<i16>,
    /// `(attnum, old_name, new_name)`. PG's `RENAME COLUMN` keeps attnum, so
    /// attnum-match + name-diff detects it with no heuristic
    pub renamed_columns: Vec<(i16, String, String)>,
    pub type_changes: Vec<(i16, RelAttr)>,
}

impl SchemaDiff {
    pub fn is_empty(&self) -> bool {
        self.added_columns.is_empty()
            && self.dropped_columns.is_empty()
            && self.renamed_columns.is_empty()
            && self.type_changes.is_empty()
    }
}

/// Compute diff `old → new` (caller passes descriptors sharing `oid`). Filters
/// `attisdropped = true` columns: PG retains them in pg_attribute for physical
/// layout, but CH sees only live columns.
pub fn compute_schema_diff(old: &RelDescriptor, new: &RelDescriptor) -> SchemaDiff {
    let mut diff = SchemaDiff::default();
    let mut old_by_num: HashMap<i16, &RelAttr> = old
        .attributes
        .iter()
        .filter(|a| !a.dropped)
        .map(|a| (a.attnum, a))
        .collect();
    for n_att in new.attributes.iter().filter(|a| !a.dropped) {
        match old_by_num.remove(&n_att.attnum) {
            None => diff.added_columns.push(n_att.clone()),
            Some(o_att) => {
                if o_att.name != n_att.name {
                    diff.renamed_columns.push((
                        n_att.attnum,
                        o_att.name.clone(),
                        n_att.name.clone(),
                    ));
                }
                if o_att.type_oid != n_att.type_oid
                    || o_att.typmod != n_att.typmod
                    || o_att.not_null != n_att.not_null
                {
                    diff.type_changes.push((n_att.attnum, n_att.clone()));
                }
            }
        }
    }
    let mut dropped: Vec<i16> = old_by_num.into_keys().collect();
    dropped.sort_unstable();
    diff.dropped_columns = dropped;
    diff
}

#[derive(Debug, Clone)]
pub struct ShadowCatalogConfig {
    /// `pg_last_wal_replay_lsn()` poll interval
    pub replay_poll: Duration,
    /// [`ShadowCatalog::relation_at`] gives up after this if shadow hasn't
    /// passed `at_lsn`; also bounds [`with_transient_retry`]'s window
    pub replay_timeout: Duration,
    /// `None` = unbounded
    pub max_entries: Option<usize>,
    pub reconnect_backoff_initial: Duration,
    pub reconnect_backoff_max: Duration,
}

impl Default for ShadowCatalogConfig {
    fn default() -> Self {
        Self {
            // 1 ms, not 50 ms: at 50 ms the fixed tick dominated worker
            // throughput in `pgbench_acceptance` when shadow apply lagged
            // pump dispatch by O(records); 1 ms keeps each wait_for_replay
            // miss bounded by SQL round-trip cost instead
            replay_poll: Duration::from_millis(1),
            replay_timeout: Duration::from_secs(30),
            max_entries: Some(4096),
            reconnect_backoff_initial: Duration::from_millis(100),
            reconnect_backoff_max: Duration::from_secs(1),
        }
    }
}

struct CacheEntry {
    generation: u64,
    insert_order: u64,
    desc: Arc<RelDescriptor>,
}

#[derive(Debug, Default, Clone)]
pub struct ShadowCatalogStats {
    pub hits: u64,
    pub misses: u64,
    pub fetches: u64,
    pub generation_bumps: u64,
    pub replay_waits: u64,
    pub evictions: u64,
    /// Records whose `db_node` is neither shadow DB nor shared catalog (0).
    /// Physical replication ships the whole cluster's WAL; rejected before the
    /// filenode query
    pub foreign_db_skips: u64,
    pub reconnects: u64,
}

/// tokio-postgres conninfo for a unix-socket shadow.
pub fn socket_conninfo(socket_dir: &str, port: u16, user: &str, dbname: &str) -> String {
    format!("host={socket_dir} port={port} user={user} dbname={dbname}")
}

/// FIFO insert-order index for cache eviction: O(log n) `pop_first` instead of
/// an O(n) `min_by_key` scan. Re-inserting a cached filenode rotates via
/// `unregister` + `register` to stay 1:1 with `by_filenode`.
#[derive(Debug, Default)]
struct EvictionIndex {
    by_order: BTreeMap<u64, RelFileNode>,
    next: u64,
}

impl EvictionIndex {
    fn register(&mut self, rfn: RelFileNode) -> u64 {
        self.next += 1;
        self.by_order.insert(self.next, rfn);
        self.next
    }

    fn unregister(&mut self, prev_order: u64) {
        self.by_order.remove(&prev_order);
    }

    fn pop_oldest(&mut self) -> Option<RelFileNode> {
        self.by_order.pop_first().map(|(_, r)| r)
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.by_order.len()
    }
}

pub struct ShadowCatalog {
    client: Client,
    conninfo: String,
    config: ShadowCatalogConfig,
    generation: u64,
    by_filenode: HashMap<RelFileNode, CacheEntry>,
    by_oid: HashMap<Oid, CacheEntry>,
    /// Last-seen descriptor per oid, retained across generation bumps (which
    /// only logically invalidate by_filenode/by_oid). Source of truth for the
    /// shape `compute_schema_diff` diffs against.
    prev_known: HashMap<Oid, Arc<RelDescriptor>>,
    eviction: EvictionIndex,
    last_replay_lsn: Option<u64>,
    /// Bumped by the decoder worker off each record's
    /// [`CatalogSignal`](crate::catalog_tracker::CatalogSignal) and by
    /// mapping writes / SIGHUP reload; an advance triggers `invalidate`.
    /// `None` standalone (tests, batch tools).
    invalidation_epoch: Option<Arc<AtomicU64>>,
    /// Latest epoch already folded into `generation`
    last_seen_epoch: u64,
    /// `None` keeps the producer side a no-op (standalone catalog, pre-applicator tests)
    event_tx: Option<mpsc::UnboundedSender<SchemaEvent>>,
    /// DB oid this client is connected to. Rejects foreign-DB filenodes before
    /// the relfilenode query (relfilenodes unique only within a DB). Survives
    /// `reconnect` since `conninfo` pins the DB.
    current_db_oid: Option<Oid>,
    stats: ShadowCatalogStats,
}

/// `query`/`query_one`/`query_opt` with a single transparent reconnect-retry on
/// closed-connection errors. Macro over `$method` shares one body across the
/// three arities without boxing the future.
macro_rules! query_with_reconnect {
    ($self:ident, $method:ident, $statement:expr, $params:expr) => {{
        $self.ensure_open().await?;
        match $self.client.$method($statement, $params).await {
            Ok(r) => Ok(r),
            Err(e) => {
                if $self.client.is_closed() {
                    $self.reconnect().await?;
                    Ok($self.client.$method($statement, $params).await?)
                } else {
                    Err(e.into())
                }
            }
        }
    }};
}

impl ShadowCatalog {
    /// Connect over a libpq key=value conninfo. One-shot; wrap in
    /// [`with_transient_retry`] for retry-on-PG-coming-up. `conninfo` is stashed
    /// so the client can be rebuilt when shadow PG bounces.
    pub async fn connect(conninfo: &str, config: ShadowCatalogConfig) -> Result<Self> {
        let (client, conn) = tokio_postgres::connect(conninfo, NoTls).await?;
        tokio::spawn(async move {
            let _ = conn.await;
        });
        Ok(Self {
            client,
            conninfo: conninfo.to_string(),
            config,
            generation: 0,
            by_filenode: HashMap::new(),
            by_oid: HashMap::new(),
            prev_known: HashMap::new(),
            eviction: EvictionIndex::default(),
            last_replay_lsn: None,
            invalidation_epoch: None,
            last_seen_epoch: 0,
            event_tx: None,
            current_db_oid: None,
            stats: ShadowCatalogStats::default(),
        })
    }

    /// Install the schema-event sink, returning its `Receiver`. Single
    /// subscriber by design; a later `subscribe` overwrites the prior sink.
    pub fn subscribe(&mut self) -> mpsc::UnboundedReceiver<SchemaEvent> {
        let (tx, rx) = mpsc::unbounded_channel();
        self.event_tx = Some(tx);
        rx
    }

    /// Bootstrap fan-out: resolve every relation in the named (`auto_create`)
    /// namespaces, emit `Added` for each unseen oid. Idempotent across daemon
    /// restarts via the applicator's `CREATE TABLE IF NOT EXISTS`. Other
    /// namespaces stay undisclosed until first WAL touch.
    pub async fn seed_from_source(&mut self, namespaces: &[String]) -> Result<usize> {
        if namespaces.is_empty() {
            return Ok(0);
        }
        let rows = self
            .query_retry(
                "SELECT c.oid::oid \
                 FROM pg_class c \
                 JOIN pg_namespace n ON n.oid = c.relnamespace \
                 WHERE n.nspname = ANY($1::text[]) \
                   AND c.relkind IN ('r', 'p')",
                &[&namespaces],
            )
            .await?;
        let mut added = 0usize;
        for row in rows {
            let oid: Oid = row.get(0);
            if self.prev_known.contains_key(&oid) {
                continue;
            }
            // Regular path so `Added` flows through record_descriptor into the
            // subscriber queue.
            match self.relation_by_oid(oid).await {
                Ok(_) => added += 1,
                Err(CatalogError::NotFoundByOid(_)) => continue,
                Err(e) => return Err(e),
            }
        }
        Ok(added)
    }

    /// Warm `prev_known` with operator-pinned relations' boot-time shape, so a
    /// first post-start `ALTER`/`RENAME`/`DROP` diffs as `Changed` (→ CH
    /// `ALTER`) not cold `Added` — which the applicator skips for pinned dests,
    /// leaving CH a column behind. Must run BEFORE [`Self::subscribe`]:
    /// `send_event` no-ops while `event_tx` is `None`, so seeding emits no
    /// `Added` and does zero CH work at boot.
    ///
    /// `qualified_names` are `"namespace.relname"` pinned-mapping keys, resolved
    /// via `to_regclass($1)` (NULL skipped, defensive: preflight guarantees
    /// existence). Oids already in `prev_known` skipped → idempotent across
    /// `--start-lsn` resume.
    ///
    /// Records the *full* source descriptor, not the pinned subset, so unmapped
    /// columns sit in the baseline as "excluded", never "added since". See
    /// `plans/future/pinned_ddl_baseline.md`.
    pub async fn seed_baseline(&mut self, qualified_names: &[String]) -> Result<usize> {
        let mut seeded = 0usize;
        for name in qualified_names {
            let row = self
                .query_one_retry("SELECT to_regclass($1)::oid", &[&name.as_str()])
                .await?;
            let Some(oid) = row.get::<_, Option<Oid>>(0) else {
                continue;
            };
            if self.prev_known.contains_key(&oid) {
                continue;
            }
            match self.relation_by_oid(oid).await {
                Ok(_) => seeded += 1,
                Err(CatalogError::NotFoundByOid(_)) => continue,
                Err(e) => return Err(e),
            }
        }
        Ok(seeded)
    }

    /// Resolve a `"namespace.relname"` to its current source descriptor via
    /// shadow's `pg_class` (`to_regclass`, same path as [`Self::seed_baseline`]),
    /// or `None` when the rel isn't known yet — the forward-declared case the
    /// per-table opt-in dispatch parks in `pending_decl`.
    pub async fn descriptor_by_qname(&mut self, qname: &str) -> Result<Option<Arc<RelDescriptor>>> {
        let row = self
            .query_one_retry("SELECT to_regclass($1)::oid", &[&qname])
            .await?;
        let Some(oid) = row.get::<_, Option<Oid>>(0) else {
            return Ok(None);
        };
        match self.relation_by_oid(oid).await {
            Ok(desc) => Ok(Some(desc)),
            Err(CatalogError::NotFoundByOid(_)) => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// Resolve a table's TOAST relation descriptor (`pg_class.reltoastrelid`
    /// → `pg_toast.pg_toast_<oid>`), `None` when the rel has no TOAST table.
    /// Backup-sourced backfills seed the page-walk filter with it so a
    /// filtered walk carries the rel's external chunks.
    pub async fn toast_descriptor_for(&mut self, oid: Oid) -> Result<Option<Arc<RelDescriptor>>> {
        let row = self
            .query_one_retry(
                "SELECT coalesce((SELECT reltoastrelid FROM pg_class WHERE oid = $1), 0)::oid",
                &[&oid],
            )
            .await?;
        let toast_oid: Oid = row.get(0);
        if toast_oid == 0 {
            return Ok(None);
        }
        match self.relation_by_oid(toast_oid).await {
            Ok(desc) => Ok(Some(desc)),
            Err(CatalogError::NotFoundByOid(_)) => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// Emit `Dropped` for an oid seen via pg_class `heap_delete`. Returns
    /// `false` for an oid the catalog never saw (CH never learned it either,
    /// nothing for the applicator to do).
    pub fn emit_dropped(&mut self, oid: Oid) -> bool {
        let Some(prev) = self.prev_known.remove(&oid) else {
            return false;
        };
        self.by_oid.remove(&oid);
        // Filenode entry stays; evicted lazily on next access
        self.send_event(SchemaEvent::Dropped {
            oid,
            qualified_name: prev.qualified_name.clone(),
        });
        true
    }

    /// Poll-based DROP TABLE discovery: any `prev_known` oid no longer in
    /// shadow's pg_class gets a `Dropped` event and is removed. Returns count
    /// surfaced.
    ///
    /// Needed because the natural path (decoder sees pg_class `heap_delete`)
    /// doesn't fire for `relreplident = 'n'` system catalogs — PG omits the old
    /// tuple from WAL, so the dropped oid is unextractable. Callers gate on
    /// [`PendingSweeps`](crate::catalog_tracker::PendingSweeps): the sweep
    /// runs only at the commit of an xact that wrote pg_class heap_delete,
    /// after `wait_for_replay` past that commit, so the drop is MVCC-visible.
    /// No internal throttle: epoch/generation comparisons here re-created the
    /// consume-early race (an earlier sweep folding a later DROP's bump made
    /// the drop's own commit no-op and lost the event).
    ///
    /// A shadow replaying ahead can surface a LATER armed xact's drop at an
    /// earlier armed commit; the later xact's own sweep then finds nothing
    /// and no-ops. Benign: attribution shifts to an earlier commit LSN,
    /// never lost, and the end state (relation dropped) is identical.
    pub async fn sweep_dropped(&mut self) -> Result<usize> {
        if self.prev_known.is_empty() {
            return Ok(0);
        }
        let known: Vec<Oid> = self.prev_known.keys().copied().collect();
        let rows = self
            .query_retry(
                "SELECT oid::oid FROM pg_class WHERE oid = ANY($1::oid[])",
                &[&known],
            )
            .await?;
        let alive: std::collections::HashSet<Oid> = rows.iter().map(|r| r.get(0)).collect();
        let mut emitted = 0usize;
        for oid in known {
            if !alive.contains(&oid) && self.emit_dropped(oid) {
                emitted += 1;
            }
        }
        Ok(emitted)
    }

    fn send_event(&self, ev: SchemaEvent) {
        if let Some(tx) = &self.event_tx {
            // Send fails only if the receiver dropped (daemon shutdown)
            let _ = tx.send(ev);
        }
    }

    fn record_descriptor(&mut self, new: &Arc<RelDescriptor>) {
        let oid = new.oid;
        match self.prev_known.get(&oid).cloned() {
            None => {
                self.send_event(SchemaEvent::Added { desc: new.clone() });
            }
            Some(old) => {
                if Arc::ptr_eq(&old, new) {
                    return;
                }
                let diff = compute_schema_diff(&old, new);
                if !diff.is_empty() {
                    self.send_event(SchemaEvent::Changed {
                        old,
                        new: new.clone(),
                        diff,
                    });
                }
            }
        }
        self.prev_known.insert(oid, new.clone());
    }

    /// Pass the same `Arc` clone as the decoder sink's
    /// `with_catalog_signals` and the DDL applicator's
    /// `with_invalidation_epoch`.
    pub fn set_invalidation_epoch(&mut self, epoch: Arc<AtomicU64>) {
        // Adopt current value as seen so a non-zero epoch (catalog opened
        // mid-stream) doesn't spuriously invalidate on first lookup
        self.last_seen_epoch = epoch.load(Ordering::Acquire);
        self.invalidation_epoch = Some(epoch);
    }

    fn drain_invalidations(&mut self) {
        let Some(e) = &self.invalidation_epoch else {
            return;
        };
        let cur = e.load(Ordering::Acquire);
        if cur != self.last_seen_epoch {
            self.last_seen_epoch = cur;
            self.invalidate();
        }
    }

    pub fn generation(&self) -> u64 {
        self.generation
    }

    /// Lock-free handle to the invalidation epoch (bumped on DDL); `None`
    /// standalone. The decode pool flushes its cache on a bump.
    pub fn invalidation_epoch_handle(&self) -> Option<Arc<AtomicU64>> {
        self.invalidation_epoch.clone()
    }

    pub fn stats(&self) -> &ShadowCatalogStats {
        &self.stats
    }

    pub fn cached(&self) -> usize {
        self.by_filenode.len()
    }

    /// Bump generation, marking every cached entry stale. Lazy eviction (old
    /// entries retained until next access), cheap regardless of commit size.
    pub fn invalidate(&mut self) -> u64 {
        self.generation = self.generation.wrapping_add(1);
        self.stats.generation_bumps += 1;
        self.generation
    }

    /// Rebuild the client from stashed `conninfo`. One-shot; retry via
    /// [`with_transient_retry`]. Bumps generation because catalog mutations may
    /// have landed in the down window without an upstream `invalidate`; resets
    /// `last_replay_lsn` since a restarted instance's replay LSN starts fresh.
    async fn reconnect(&mut self) -> Result<()> {
        let (client, conn) = tokio_postgres::connect(&self.conninfo, NoTls).await?;
        tokio::spawn(async move {
            let _ = conn.await;
        });
        self.client = client;
        self.stats.reconnects += 1;
        self.generation = self.generation.wrapping_add(1);
        self.stats.generation_bumps += 1;
        self.last_replay_lsn = None;
        Ok(())
    }

    async fn ensure_open(&mut self) -> Result<()> {
        if self.client.is_closed() {
            self.reconnect().await?;
        }
        Ok(())
    }

    async fn query_one_retry(
        &mut self,
        statement: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Row> {
        query_with_reconnect!(self, query_one, statement, params)
    }

    async fn query_opt_retry(
        &mut self,
        statement: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Option<Row>> {
        query_with_reconnect!(self, query_opt, statement, params)
    }

    async fn query_retry(
        &mut self,
        statement: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Vec<Row>> {
        query_with_reconnect!(self, query, statement, params)
    }

    /// Last observed `pg_last_wal_replay_lsn()` (None until shadow replays
    /// anything, e.g. fresh standby start).
    pub fn last_observed_replay(&self) -> Option<u64> {
        self.last_replay_lsn
    }

    /// Wait until shadow's replay LSN ≥ `target`, returning the deciding poll's
    /// LSN. `target = 0` returns on the first non-NULL LSN.
    pub async fn wait_for_replay(&mut self, target: u64) -> Result<u64> {
        if let Some(seen) = self.last_replay_lsn
            && seen >= target
            && target != 0
        {
            return Ok(seen);
        }
        self.stats.replay_waits += 1;
        let start = Instant::now();
        loop {
            let row = self
                .query_one_retry("SELECT pg_last_wal_replay_lsn()::text", &[])
                .await?;
            let raw: Option<String> = row.get(0);
            if let Some(s) = raw {
                let lsn = parse_pg_lsn(&s).map_err(|e| {
                    CatalogError::Parse(format!("pg_last_wal_replay_lsn {s:?}: {e}"))
                })?;
                self.last_replay_lsn = Some(self.last_replay_lsn.map_or(lsn, |old| old.max(lsn)));
                if lsn >= target {
                    return Ok(lsn);
                }
            }
            let elapsed = start.elapsed();
            if elapsed >= self.config.replay_timeout {
                return Err(CatalogError::ReplayTimeout {
                    target,
                    last: self.last_replay_lsn,
                    elapsed,
                });
            }
            tokio::time::sleep(self.config.replay_poll).await;
        }
    }

    /// Look up by `RelFileNode`, gated on shadow replay past `at_lsn`. Decoder's
    /// standard call shape. `at_lsn = 0` skips the gate (caller proved freshness
    /// otherwise, e.g. preceding `wait_for_replay`).
    pub async fn relation_at(
        &mut self,
        rfn: RelFileNode,
        at_lsn: u64,
    ) -> Result<Arc<RelDescriptor>> {
        self.drain_invalidations();
        if at_lsn > 0 {
            // `replay.wait` — the poll loop blocking on the shadow PG
            // replaying up to `at_lsn`. Nests under `catalog.gate` when
            // the decoder instruments this call; this is where a stalled
            // shadow shows up (up to the 30s replay timeout). `target_lsn`
            // is the LSN we need; `replay_lsn` is where the shadow actually
            // is once the wait returns (== target on the cached fast path).
            let replay_span = trace_span!(
                !tracing::Span::current().is_none(),
                "replay.wait",
                target_lsn = at_lsn,
                replay_lsn = tracing::field::Empty,
            );
            let replayed = self
                .wait_for_replay(at_lsn)
                .instrument(replay_span.clone())
                .await?;
            replay_span.record("replay_lsn", replayed);
            // Re-check after the await: a concurrent mapping write /
            // SIGHUP reload can have bumped the epoch while
            // wait_for_replay yielded.
            self.drain_invalidations();
        }
        if let Some(entry) = self.by_filenode.get(&rfn)
            && entry.generation == self.generation
        {
            self.stats.hits += 1;
            return Ok(entry.desc.clone());
        }
        self.stats.misses += 1;
        // `descriptor.fetch` — the pg_class/pg_attribute round-trip to the
        // shadow PG, only on a cache miss. Sibling of `replay.wait`.
        let desc = self
            .fetch_by_filenode(rfn)
            .instrument(trace_span!(
                !tracing::Span::current().is_none(),
                "descriptor.fetch",
                spc_node = rfn.spc_node,
                db_node = rfn.db_node,
                rel_node = rfn.rel_node,
            ))
            .await?
            .ok_or(CatalogError::NotFoundByFilenode(rfn))?;
        Ok(self.insert(desc))
    }

    /// Filenode resolution for the async decode pool. Serves the inline path's
    /// ([`BufferingDecoderSink`](crate::xact_buffer::BufferingDecoderSink))
    /// cached entry at ANY generation; fetches transiently (no cache write, no
    /// events) only when the filenode is absent.
    ///
    /// The pool lags asynchronously, so a worker can fall behind a DDL that
    /// bumped generation. Re-resolving via [`Self::relation_at`] is wrong:
    ///
    /// * ADD COLUMN keeps the filenode: a lagging fetch reads pre-DDL shape but
    ///   inserts it at the post-DDL generation, poisoning the entry so the
    ///   inline path's later post-DDL resolution hits and never fires `Changed`
    ///   (barrier never reaches CH).
    /// * TRUNCATE rotates the filenode: once replayed, the old filenode no
    ///   longer resolves, so a fetch returns `None` and pre-TRUNCATE rows fail —
    ///   the cached entry is their only surviving descriptor.
    ///
    /// Reusing the cached shape keeps schema-change detection and cache
    /// maintenance solely on the inline path.
    pub async fn relation_at_pooled(
        &mut self,
        rfn: RelFileNode,
        at_lsn: u64,
    ) -> Result<Arc<RelDescriptor>> {
        self.drain_invalidations();
        if at_lsn > 0 {
            self.wait_for_replay(at_lsn).await?;
            self.drain_invalidations();
        }
        if let Some(entry) = self.by_filenode.get(&rfn) {
            self.stats.hits += 1;
            return Ok(entry.desc.clone());
        }
        // Absent (never inline-resolved, or evicted): fetch transiently, no
        // insert, to keep the pool out of cache state
        self.stats.misses += 1;
        let desc = self
            .fetch_by_filenode(rfn)
            .await?
            .ok_or(CatalogError::NotFoundByFilenode(rfn))?;
        Ok(Arc::new(desc))
    }

    /// Look up by oid, no replay gate (oid-only references: xact records,
    /// shared-catalog probes).
    pub async fn relation_by_oid(&mut self, oid: Oid) -> Result<Arc<RelDescriptor>> {
        self.drain_invalidations();
        if let Some(entry) = self.by_oid.get(&oid)
            && entry.generation == self.generation
        {
            self.stats.hits += 1;
            return Ok(entry.desc.clone());
        }
        self.stats.misses += 1;
        let desc = self
            .fetch_by_oid(oid)
            .await?
            .ok_or(CatalogError::NotFoundByOid(oid))?;
        Ok(self.insert(desc))
    }

    fn insert(&mut self, desc: RelDescriptor) -> Arc<RelDescriptor> {
        let arc = Arc::new(desc);
        if let Some(prev) = self.by_filenode.get(&arc.rfn) {
            self.eviction.unregister(prev.insert_order);
        }
        let order = self.eviction.register(arc.rfn);
        let entry = CacheEntry {
            generation: self.generation,
            insert_order: order,
            desc: arc.clone(),
        };
        self.by_filenode.insert(
            arc.rfn,
            CacheEntry {
                generation: entry.generation,
                insert_order: entry.insert_order,
                desc: arc.clone(),
            },
        );
        self.by_oid.insert(arc.oid, entry);
        self.record_descriptor(&arc);
        self.evict_if_over_cap();
        arc
    }

    fn evict_if_over_cap(&mut self) {
        let Some(cap) = self.config.max_entries else {
            return;
        };
        while self.by_filenode.len() > cap {
            let Some(victim_rfn) = self.eviction.pop_oldest() else {
                break;
            };
            if let Some(e) = self.by_filenode.remove(&victim_rfn) {
                self.by_oid.remove(&e.desc.oid);
                self.stats.evictions += 1;
            }
        }
    }

    async fn fetch_by_filenode(&mut self, rfn: RelFileNode) -> Result<Option<RelDescriptor>> {
        // Reject foreign-DB filenodes first: relfilenode is unique only per
        // database (regardless of tablespace, so spc_node need not match), so a
        // foreign db_node's rel_node could collide with a local relation.
        // db_node 0 = shared catalog (visible from any DB), let through.
        if is_foreign_db(rfn.db_node, self.current_db_oid().await?) {
            self.stats.foreign_db_skips += 1;
            return Err(CatalogError::ForeignDatabase(rfn));
        }
        self.stats.fetches += 1;
        // pg_relation_filenode(oid) abstracts mapped (pg_filenode.map) vs
        // unmapped (pg_class.relfilenode)
        let row = self
            .query_opt_retry(
                "SELECT \
                    c.oid::oid, \
                    c.relnamespace::oid, \
                    n.nspname::text, \
                    c.relname::text, \
                    c.relkind::text, \
                    c.relpersistence::text, \
                    c.relreplident::text \
                 FROM pg_class c \
                 JOIN pg_namespace n ON n.oid = c.relnamespace \
                 WHERE pg_relation_filenode(c.oid) = $1 \
                 LIMIT 1",
                &[&rfn.rel_node],
            )
            .await?;
        let Some(row) = row else { return Ok(None) };
        Ok(Some(self.descriptor_from_row(&row, rfn).await?))
    }

    async fn fetch_by_oid(&mut self, oid: Oid) -> Result<Option<RelDescriptor>> {
        self.stats.fetches += 1;
        let row = self
            .query_opt_retry(
                "SELECT \
                    c.oid::oid, \
                    c.relnamespace::oid, \
                    n.nspname::text, \
                    c.relname::text, \
                    c.relkind::text, \
                    c.relpersistence::text, \
                    c.relreplident::text, \
                    c.reltablespace::oid, \
                    coalesce(pg_relation_filenode(c.oid), 0)::oid \
                 FROM pg_class c \
                 JOIN pg_namespace n ON n.oid = c.relnamespace \
                 WHERE c.oid = $1",
                &[&oid],
            )
            .await?;
        let Some(row) = row else { return Ok(None) };
        let spc_node: Oid = row.get(7);
        let rel_node: Oid = row.get(8);
        let db_node = self.current_database_oid().await?;
        let rfn = RelFileNode {
            spc_node,
            db_node,
            rel_node,
        };
        Ok(Some(self.descriptor_from_row(&row, rfn).await?))
    }

    /// Build from a pg_class⋈pg_namespace row whose first 7 columns are
    /// (oid, relnamespace, nspname, relname, relkind, relpersistence,
    /// relreplident), paired with a resolved `rfn`.
    async fn descriptor_from_row(&mut self, row: &Row, rfn: RelFileNode) -> Result<RelDescriptor> {
        let oid: Oid = row.get(0);
        let namespace_oid: Oid = row.get(1);
        let namespace_name: String = row.get(2);
        let name: String = row.get(3);
        let kind = one_char(row.get::<_, String>(4), "relkind")?;
        let persistence = one_char(row.get::<_, String>(5), "relpersistence")?;
        let replident_char = one_char(row.get::<_, String>(6), "relreplident")?;
        let replident = self.fetch_replident(replident_char, oid).await?;
        let attributes = self.fetch_attributes(oid).await?;
        let qualified_name = RelDescriptor::build_qualified_name(&namespace_name, &name);
        Ok(RelDescriptor {
            rfn,
            oid,
            namespace_oid,
            namespace_name,
            name,
            qualified_name,
            kind,
            persistence,
            replident,
            attributes,
        })
    }

    async fn fetch_replident(&mut self, c: char, rel_oid: Oid) -> Result<ReplIdent> {
        match c {
            'd' => {
                // indkey is int2vector; cast to int2[] for tokio-postgres'
                // Kind::Array(int2) decode. Missing row → no PK → old = None.
                let row = self
                    .query_opt_retry(
                        "SELECT indkey::int2[] \
                         FROM pg_index \
                         WHERE indrelid = $1 AND indisprimary = true \
                         LIMIT 1",
                        &[&rel_oid],
                    )
                    .await?;
                let pk_attnums = row.map(|r| r.get::<_, Vec<i16>>(0));
                Ok(ReplIdent::Default { pk_attnums })
            }
            'n' => Ok(ReplIdent::Nothing),
            'f' => Ok(ReplIdent::Full),
            'i' => {
                // indkey is int2vector; cast to int2[] for tokio-postgres'
                // Kind::Array(int2) → Vec<i16> decode
                let row = self
                    .query_opt_retry(
                        "SELECT indexrelid::oid, indkey::int2[] \
                         FROM pg_index \
                         WHERE indrelid = $1 AND indisreplident = true \
                         LIMIT 1",
                        &[&rel_oid],
                    )
                    .await?
                    .ok_or_else(|| {
                        CatalogError::Parse(format!(
                            "relreplident='i' but no pg_index row with indisreplident=true for relation {rel_oid}",
                        ))
                    })?;
                let index_oid: Oid = row.get(0);
                let key_attnums: Vec<i16> = row.get(1);
                Ok(ReplIdent::UsingIndex {
                    index_oid,
                    key_attnums,
                })
            }
            other => Err(CatalogError::Parse(format!(
                "unknown relreplident {other:?} (expected one of d/n/f/i)",
            ))),
        }
    }

    async fn fetch_attributes(&mut self, rel_oid: Oid) -> Result<Vec<RelAttr>> {
        // `attmissingval` is `anyarray` (no subscript or unnest); `::text` casts
        // to PG's array_out literal `{val}`, which parse_array_one_element
        // strips back to the typoutput text form for getmissingattr
        let rows = self
            .query_retry(
                "SELECT \
                    a.attnum::int2, \
                    a.attname::text, \
                    a.atttypid::oid, \
                    a.atttypmod::int4, \
                    a.attnotnull::bool, \
                    a.attisdropped::bool, \
                    t.typname::text, \
                    t.typbyval::bool, \
                    t.typlen::int2, \
                    t.typalign::text, \
                    t.typstorage::text, \
                    CASE WHEN a.atthasmissing THEN a.attmissingval::text END \
                 FROM pg_attribute a \
                 JOIN pg_type t ON t.oid = a.atttypid \
                 WHERE a.attrelid = $1 AND a.attnum >= 1 \
                 ORDER BY a.attnum",
                &[&rel_oid],
            )
            .await?;
        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            let raw_missing: Option<String> = row.get(11);
            out.push(RelAttr {
                attnum: row.get(0),
                name: row.get(1),
                type_oid: row.get(2),
                typmod: row.get(3),
                not_null: row.get(4),
                dropped: row.get(5),
                type_name: row.get(6),
                type_byval: row.get(7),
                type_len: row.get(8),
                type_align: one_char(row.get::<_, String>(9), "typalign")?,
                type_storage: one_char(row.get::<_, String>(10), "typstorage")?,
                missing_text: raw_missing.as_deref().and_then(parse_array_one_element),
            });
        }
        Ok(out)
    }

    async fn current_database_oid(&mut self) -> Result<Oid> {
        let row = self
            .query_one_retry(
                "SELECT oid::oid FROM pg_database WHERE datname = current_database()",
                &[],
            )
            .await?;
        Ok(row.get(0))
    }

    /// Memoized [`Self::current_database_oid`], valid across `reconnect` since
    /// `conninfo` pins the DB.
    async fn current_db_oid(&mut self) -> Result<Oid> {
        if let Some(oid) = self.current_db_oid {
            return Ok(oid);
        }
        let oid = self.current_database_oid().await?;
        self.current_db_oid = Some(oid);
        Ok(oid)
    }
}

/// Resolve a WAL-observed filenode under the shared mutex.
pub async fn resolve_at(
    catalog: &Mutex<ShadowCatalog>,
    rfn: RelFileNode,
    at_lsn: u64,
) -> Result<Arc<RelDescriptor>> {
    let mut cat = catalog.lock().await;
    cat.relation_at(rfn, at_lsn).await
}

/// [`resolve_at`] for the async decode pool. See
/// [`ShadowCatalog::relation_at_pooled`].
pub async fn resolve_at_pooled(
    catalog: &Mutex<ShadowCatalog>,
    rfn: RelFileNode,
    at_lsn: u64,
) -> Result<Arc<RelDescriptor>> {
    let mut cat = catalog.lock().await;
    cat.relation_at_pooled(rfn, at_lsn).await
}

/// Strip + dequote the single element of a PG array literal `{val}`, recovering
/// `attmissingval[1]`'s typoutput form.
///
/// PG array_out quoting (`src/backend/utils/adt/arrayfuncs.c`):
/// elements with braces/commas/quotes/backslashes/whitespace get `"`-wrapped,
/// internal `"` and `\` backslash-escaped; NULL renders as bare `NULL`. Returns
/// `None` on NULL, empty `{}`, or any shape outside single-element 1-D.
pub(crate) fn parse_array_one_element(raw: &str) -> Option<String> {
    let inner = raw.strip_prefix('{')?.strip_suffix('}')?;
    if inner.is_empty() {
        return None;
    }
    if inner == "NULL" {
        return None;
    }
    if let Some(rest) = inner.strip_prefix('"') {
        // Quoted: unescape, require trailing `"`
        let mut out = String::with_capacity(rest.len());
        let mut chars = rest.chars().peekable();
        loop {
            match chars.next() {
                Some('"') => return (chars.next().is_none()).then_some(out),
                Some('\\') => match chars.next() {
                    Some(c) => out.push(c),
                    None => return None,
                },
                Some(c) => out.push(c),
                None => return None,
            }
        }
    } else {
        // Unquoted scalar: no special chars, per array_out
        Some(inner.to_owned())
    }
}

fn one_char(s: String, what: &str) -> Result<char> {
    let mut chars = s.chars();
    match (chars.next(), chars.next()) {
        (Some(c), None) => Ok(c),
        _ => Err(CatalogError::Parse(format!(
            "expected single-char {what}, got {s:?}"
        ))),
    }
}

/// Exponential-backoff retry on transient PG errors (closed connection, "system
/// is starting up", connect refused). Non-PG errors (parse, not-found, replay
/// timeout) surface immediately.
///
/// Outside `ShadowCatalog` on purpose: the catalog's invalidation and
/// replay-LSN bookkeeping stay unaware of in-flight retries, seeing only the
/// final outcome.
pub async fn with_transient_retry<R, F>(
    timeout: Duration,
    initial_backoff: Duration,
    max_backoff: Duration,
    op: F,
) -> Result<R>
where
    F: AsyncFnMut() -> Result<R>,
{
    let deadline = Instant::now() + timeout;
    let (_op, result) = (|mut op: F| async move {
        let r = op().await;
        (op, r)
    })
    .retry(
        ExponentialBuilder::default()
            .with_min_delay(initial_backoff)
            .with_max_delay(max_backoff)
            .without_max_times(),
    )
    .context(op)
    .when(|e: &CatalogError| is_transient(e) && Instant::now() < deadline)
    .await;
    result
}

/// Any [`CatalogError::Pg`] qualifies: connect-refused and `CANNOT_CONNECT_NOW`
/// both surface that way, and steady-state SQL errors against well-known queries
/// aren't expected.
fn is_transient(err: &CatalogError) -> bool {
    matches!(err, CatalogError::Pg(_))
}

/// True when `db_node` is neither the connected shadow DB nor a shared catalog
/// (db_node 0). Such filenodes come from other DBs in the cluster's physical WAL
/// and must not resolve locally.
fn is_foreign_db(db_node: Oid, current_db_oid: Oid) -> bool {
    db_node != 0 && db_node != current_db_oid
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    #[test]
    fn foreign_db_predicate() {
        // db_node 0 = shared catalog, never foreign
        assert!(!is_foreign_db(0, 16384));
        assert!(!is_foreign_db(16384, 16384));
        assert!(is_foreign_db(16385, 16384));
    }

    #[test]
    fn parse_array_one_element_scalars() {
        assert_eq!(parse_array_one_element("{7}").as_deref(), Some("7"));
        assert_eq!(parse_array_one_element("{t}").as_deref(), Some("t"));
        assert_eq!(parse_array_one_element("{3.14}").as_deref(), Some("3.14"),);
        assert_eq!(
            parse_array_one_element("{-9223372036854775808}").as_deref(),
            Some("-9223372036854775808"),
        );
    }

    #[test]
    fn parse_array_one_element_quoted_text() {
        assert_eq!(
            parse_array_one_element("{\"hello\"}").as_deref(),
            Some("hello"),
        );
        assert_eq!(
            parse_array_one_element("{\"hello, world\"}").as_deref(),
            Some("hello, world"),
        );
        assert_eq!(
            parse_array_one_element("{\"a\\\"b\"}").as_deref(),
            Some("a\"b"),
        );
    }

    #[test]
    fn parse_array_one_element_empty_and_null() {
        assert!(parse_array_one_element("{}").is_none());
        assert!(parse_array_one_element("{NULL}").is_none());
        assert!(parse_array_one_element("nope").is_none());
    }

    #[test]
    fn one_char_accepts_single() {
        assert_eq!(one_char("r".into(), "relkind").unwrap(), 'r');
        assert_eq!(one_char("p".into(), "relpersistence").unwrap(), 'p');
    }

    #[test]
    fn one_char_rejects_multi_or_empty() {
        assert!(one_char("".into(), "x").is_err());
        assert!(one_char("rr".into(), "x").is_err());
    }

    #[test]
    fn socket_conninfo_includes_all_fields() {
        let s = socket_conninfo("/tmp/sock", 55434, "postgres", "postgres");
        assert!(s.contains("host=/tmp/sock"));
        assert!(s.contains("port=55434"));
        assert!(s.contains("user=postgres"));
        assert!(s.contains("dbname=postgres"));
    }

    #[test]
    fn config_default_is_sane() {
        let c = ShadowCatalogConfig::default();
        assert!(c.replay_poll < c.replay_timeout);
        assert!(c.max_entries.is_some());
        assert!(c.reconnect_backoff_initial < c.reconnect_backoff_max);
    }

    #[test]
    fn is_transient_classifies_known_variants() {
        assert!(!is_transient(&CatalogError::Parse("x".into())));
        assert!(!is_transient(&CatalogError::NotFoundByOid(42)));
        assert!(!is_transient(&CatalogError::ReplayTimeout {
            target: 0,
            last: None,
            elapsed: Duration::from_secs(0),
        }));
    }

    fn rfn(rel: u32) -> RelFileNode {
        RelFileNode {
            spc_node: 1663,
            db_node: 5,
            rel_node: rel,
        }
    }

    #[test]
    fn eviction_index_pops_oldest_first() {
        let mut ix = EvictionIndex::default();
        let o1 = ix.register(rfn(10));
        let o2 = ix.register(rfn(20));
        let o3 = ix.register(rfn(30));
        assert!(o1 < o2 && o2 < o3);
        assert_eq!(ix.len(), 3);
        assert_eq!(ix.pop_oldest(), Some(rfn(10)));
        assert_eq!(ix.pop_oldest(), Some(rfn(20)));
        assert_eq!(ix.pop_oldest(), Some(rfn(30)));
        assert_eq!(ix.pop_oldest(), None);
    }

    #[test]
    fn eviction_index_unregister_drops_stale_order() {
        let mut ix = EvictionIndex::default();
        let o1 = ix.register(rfn(10));
        ix.register(rfn(20));
        ix.unregister(o1);
        let _ = ix.register(rfn(10));
        assert_eq!(ix.len(), 2);
        assert_eq!(ix.pop_oldest(), Some(rfn(20)));
        assert_eq!(ix.pop_oldest(), Some(rfn(10)));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn with_transient_retry_returns_immediately_on_success() {
        let calls = Arc::new(AtomicUsize::new(0));
        let calls_c = calls.clone();
        let r: Result<u32> = with_transient_retry(
            Duration::from_secs(5),
            Duration::from_millis(1),
            Duration::from_millis(5),
            async move || {
                calls_c.fetch_add(1, Ordering::SeqCst);
                Ok(7)
            },
        )
        .await;
        assert_eq!(r.unwrap(), 7);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    fn mk_attr(attnum: i16, name: &str, oid: Oid, not_null: bool) -> RelAttr {
        RelAttr {
            attnum,
            name: name.into(),
            type_oid: oid,
            typmod: -1,
            not_null,
            dropped: false,
            type_name: "test".into(),
            type_byval: true,
            type_len: 4,
            type_align: 'i',
            type_storage: 'p',
            missing_text: None,
        }
    }

    fn mk_desc(oid: Oid, attrs: Vec<RelAttr>) -> RelDescriptor {
        RelDescriptor {
            rfn: rfn(oid),
            oid,
            namespace_oid: 2200,
            namespace_name: "public".into(),
            name: format!("t{oid}"),
            qualified_name: RelDescriptor::build_qualified_name("public", &format!("t{oid}")),
            kind: 'r',
            persistence: 'p',
            replident: ReplIdent::Default { pk_attnums: None },
            attributes: attrs,
        }
    }

    #[test]
    fn schema_diff_detects_added_columns() {
        let old = mk_desc(16400, vec![mk_attr(1, "id", 23, true)]);
        let new = mk_desc(
            16400,
            vec![mk_attr(1, "id", 23, true), mk_attr(2, "name", 25, false)],
        );
        let d = compute_schema_diff(&old, &new);
        assert_eq!(d.added_columns.len(), 1);
        assert_eq!(d.added_columns[0].attnum, 2);
        assert!(d.dropped_columns.is_empty());
        assert!(d.renamed_columns.is_empty());
        assert!(d.type_changes.is_empty());
    }

    #[test]
    fn schema_diff_detects_dropped_columns() {
        let old = mk_desc(
            16400,
            vec![mk_attr(1, "id", 23, true), mk_attr(2, "name", 25, false)],
        );
        let new = mk_desc(16400, vec![mk_attr(1, "id", 23, true)]);
        let d = compute_schema_diff(&old, &new);
        assert_eq!(d.dropped_columns, vec![2]);
        assert!(d.added_columns.is_empty());
    }

    #[test]
    fn schema_diff_detects_rename_at_same_attnum() {
        let old = mk_desc(
            16400,
            vec![
                mk_attr(1, "id", 23, true),
                mk_attr(2, "old_name", 25, false),
            ],
        );
        let new = mk_desc(
            16400,
            vec![
                mk_attr(1, "id", 23, true),
                mk_attr(2, "new_name", 25, false),
            ],
        );
        let d = compute_schema_diff(&old, &new);
        assert_eq!(
            d.renamed_columns,
            vec![(2, "old_name".into(), "new_name".into())]
        );
        assert!(d.added_columns.is_empty());
        assert!(d.dropped_columns.is_empty());
        assert!(d.type_changes.is_empty());
    }

    #[test]
    fn schema_diff_detects_type_change_at_same_attnum() {
        let old = mk_desc(16400, vec![mk_attr(1, "c", 23, true)]); // int4
        let new = mk_desc(16400, vec![mk_attr(1, "c", 20, true)]); // int8
        let d = compute_schema_diff(&old, &new);
        assert_eq!(d.type_changes.len(), 1);
        assert_eq!(d.type_changes[0].0, 1);
        assert_eq!(d.type_changes[0].1.type_oid, 20);
    }

    #[test]
    fn schema_diff_skips_pg_dropped_columns_in_old() {
        // PG retains DROP COLUMN as attisdropped=true in pg_attribute; diff must
        // ignore them, not re-surface as still-present on the new side
        let mut a = mk_attr(2, "x", 25, false);
        a.dropped = true;
        let old = mk_desc(16400, vec![mk_attr(1, "id", 23, true), a]);
        let new = mk_desc(16400, vec![mk_attr(1, "id", 23, true)]);
        let d = compute_schema_diff(&old, &new);
        assert!(d.dropped_columns.is_empty());
        assert!(d.added_columns.is_empty());
    }

    #[test]
    fn schema_diff_is_empty_when_shapes_match() {
        let a = mk_desc(
            16400,
            vec![mk_attr(1, "id", 23, true), mk_attr(2, "name", 25, false)],
        );
        let b = a.clone();
        let d = compute_schema_diff(&a, &b);
        assert!(d.is_empty());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn with_transient_retry_fails_fast_on_non_transient() {
        let calls = Arc::new(AtomicUsize::new(0));
        let calls_c = calls.clone();
        let r: Result<()> = with_transient_retry(
            Duration::from_secs(10),
            Duration::from_millis(1),
            Duration::from_millis(5),
            async move || {
                calls_c.fetch_add(1, Ordering::SeqCst);
                Err(CatalogError::Parse("nope".into()))
            },
        )
        .await;
        assert!(matches!(r, Err(CatalogError::Parse(_))));
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "non-transient must not retry",
        );
    }
}
