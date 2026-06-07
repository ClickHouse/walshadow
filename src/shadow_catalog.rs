//! Shadow PG catalog cache.
//!
//! Decoder calls [`ShadowCatalog::relation_at`] with a [`RelFileNode`]
//! observed in a source-WAL record plus the record's source LSN.
//! Implementation:
//!
//! 1. Block until `pg_last_wal_replay_lsn()` on shadow ≥ `at_lsn`, so
//!    shadow's catalog already reflects every catalog write the source
//!    issued at or before that LSN.
//! 2. Check the in-process cache keyed by `(rfn, generation)`. Hit →
//!    return immediately.
//! 3. Miss → resolve `rfn` to a `pg_class` row via
//!    `pg_relation_filenode(oid)` (handles both mapped catalogs and
//!    regular tables uniformly), then fan-out to `pg_attribute` +
//!    `pg_type` + `pg_namespace`.
//!
//! Generation invalidation: a
//! [`CatalogTracker`](crate::catalog_tracker::CatalogTracker) wired with
//! [`set_invalidation_epoch`](crate::catalog_tracker::CatalogTracker::set_invalidation_epoch)
//! bumps a shared `AtomicU64` on every catalog-touching record. The catalog
//! reads the atomic at the top of every relation lookup
//! ([`ShadowCatalog::set_invalidation_epoch`] installs the matching
//! `Arc` clone). An advance triggers an in-line
//! [`ShadowCatalog::invalidate`] before the cache check — synchronous
//! so a DDL observed in the same batch as the dependent heap INSERT
//! can't race past the cache.
//!
//! Concurrency: every mutating-looking method on `ShadowCatalog`
//! ([`relation_at`](ShadowCatalog::relation_at),
//! [`relation_by_oid`](ShadowCatalog::relation_by_oid),
//! [`wait_for_replay`](ShadowCatalog::wait_for_replay),
//! `invalidate`) takes `&mut self`. The
//! cache state is technically interior-mutable (an `RwLock` over the
//! two `HashMap`s + atomics for stats would suffice for the hit path)
//! but the `&self` shape is deferred. Callers that need concurrent
//! access (drain task, [`BufferingDecoderSink`](crate::xact_buffer::BufferingDecoderSink),
//! oracle) wrap the catalog in `Arc<tokio::sync::Mutex<_>>` at the daemon
//! level and share clones. Single-task lookups today are cheap enough
//! that mutex serialisation costs nothing measurable; the lock-free
//! refactor lands when the lookup-rate hot path actually exists.
//!
//! Single-database model: a `ShadowCatalog` instance is bound to one
//! database. Shared catalogs (`db_node == 0`) are visible from any
//! connection and resolve correctly via `pg_relation_filenode`.
//! Cross-user-database replay needs one cache per database; out of
//! scope here.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use thiserror::Error;
use tokio::sync::{Mutex, mpsc};
use tokio_postgres::types::{Oid, ToSql};
use tokio_postgres::{Client, NoTls, Row};
use wal_rs::pg::walparser::RelFileNode;

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

/// Fully-resolved description of one PG relation. Sized for what the
/// decoder needs: enough to drive heap-tuple decoding and report what
/// each column is.
#[derive(Debug, Clone, PartialEq)]
pub struct RelDescriptor {
    pub rfn: RelFileNode,
    pub oid: Oid,
    pub namespace_oid: Oid,
    pub namespace_name: String,
    pub name: String,
    /// `format!("{namespace_name}.{name}")` cached at construction
    /// time. Hot-path consumers (CH emitter routing, observers keyed
    /// by qualified name) read this directly instead of re-formatting
    /// per row. Wrap in `Arc<str>` so descriptor clones don't recopy
    /// the bytes.
    pub qualified_name: Arc<str>,
    /// `pg_class.relkind`: `'r'` table, `'i'` index, `'S'` sequence,
    /// `'t'` toast, `'v'` view, `'m'` matview, `'c'` composite,
    /// `'f'` foreign, `'p'` partitioned.
    pub kind: char,
    /// `pg_class.relpersistence`: `'p'` permanent, `'u'` unlogged,
    /// `'t'` temporary.
    pub persistence: char,
    /// `pg_class.relreplident` resolved to the form the decoder
    /// needs: `UsingIndex` carries the replica-identity index's oid
    /// and the column-number list (`pg_index.indkey`); `Default`
    /// carries the primary key's `indkey` (or `None` when no PK), so
    /// old-tuple decode under `XLH_UPDATE_CONTAINS_OLD_KEY` resolves
    /// without a second catalog round-trip.
    pub replident: ReplIdent,
    pub attributes: Vec<RelAttr>,
}

impl RelDescriptor {
    /// Build the cached qualified-name `Arc<str>` for callers
    /// constructing a descriptor manually (tests + bootstrap paths).
    pub fn build_qualified_name(namespace_name: &str, name: &str) -> Arc<str> {
        let mut s = String::with_capacity(namespace_name.len() + 1 + name.len());
        s.push_str(namespace_name);
        s.push('.');
        s.push_str(name);
        Arc::<str>::from(s)
    }
}

/// Resolved `pg_class.relreplident`. `pg_class` stores a single char
/// (`'d'`, `'n'`, `'f'`, `'i'`); the `i` variant carries the
/// replica-identity index's oid and the indexed-column attnum list
/// (`pg_index.indkey`) because the decoder needs both to
/// interpret `XLH_UPDATE_CONTAINS_OLD_KEY` / `XLH_UPDATE_CONTAINS_OLD_TUPLE`
/// payloads.
#[derive(Debug, Clone, PartialEq)]
pub enum ReplIdent {
    /// `'d'`. Old-tuple payload contains primary-key columns when
    /// `XLH_UPDATE_CONTAINS_OLD_KEY` rides on UPDATE, or on every
    /// DELETE. `pk_attnums` is `Some(pg_index.indkey)` for the
    /// table's primary key (resolved at descriptor build via
    /// `indisprimary = true`), or `None` when no PK exists — in
    /// which case the decoder yields `old = None` always. See the
    /// relreplident behaviour notes alongside the heap decoder.
    Default { pk_attnums: Option<Vec<i16>> },
    /// `'n'`. Old-tuple payload is empty; UPDATE/DELETE rows lack a
    /// key. The emitter drops these.
    Nothing,
    /// `'f'`. Old-tuple payload mirrors every non-dropped column.
    Full,
    /// `'i'`. Old-tuple payload contains the columns of
    /// `indexrelid` indexed by `key_attnums` (`pg_index.indkey`).
    UsingIndex {
        index_oid: Oid,
        key_attnums: Vec<i16>,
    },
}

/// One column on a relation, fields chosen to match what walhouse's
/// decoder pulls from PG catalog today.
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
    /// `pg_type.typalign`: `'c'` 1, `'s'` 2, `'i'` 4, `'d'` 8.
    pub type_align: char,
    /// `pg_type.typstorage`: `'p'` plain, `'e'` external (toast),
    /// `'m'` main (in-line, never compressed), `'x'` extended.
    pub type_storage: char,
    /// `pg_attribute.atthasmissing` + text form of `attmissingval[1]`
    /// when set. PG's fast-path `ALTER TABLE ADD COLUMN ... DEFAULT k`
    /// (heaptuple.c `getmissingattr`) emits this for pre-ALTER rows
    /// whose physical tuple has fewer columns than the catalog. The
    /// text form is the type's `typoutput` rendering; heap_decoder
    /// converts back to `ColumnValue` via the Tier 1/2 type matrix,
    /// with `PgPending` fallback through the oracle for Tier 3.
    pub missing_text: Option<String>,
}

/// Schema mutation surface published by [`ShadowCatalog`]
/// to downstream consumers (the CH DDL applicator + integration tests).
///
/// One event per resolved descriptor change. `Added` fires the first
/// time the catalog learns about an oid; `Changed` fires when a refetch
/// returns a non-trivially-different shape; `Dropped` fires for
/// relations the decoder saw heap_delete on a pg_class row for. Events
/// flow in `ShadowCatalog` internal order (the order the fetches /
/// drops happened); callers stamp them onto an xact-buffer position by
/// pairing each event with the WAL record that triggered the fetch.
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

/// Resolved diff between two [`RelDescriptor`] snapshots for the same
/// relation oid. Built by [`compute_schema_diff`]; emitter consumers
/// run one CH `ALTER` per entry without re-walking attributes.
///
/// `type_changes` lists `(attnum, new_attr)` — old type is recoverable
/// from `Changed.old.attributes`. Type changes are rejected via
/// [`crate::type_bridge::BridgeError::UnsupportedType`] when emitted to
/// CH; widening lands in a follow-up.
#[derive(Debug, Default, Clone, PartialEq)]
pub struct SchemaDiff {
    pub added_columns: Vec<RelAttr>,
    pub dropped_columns: Vec<i16>,
    /// `(attnum, old_name, new_name)`. Renames are detected by attnum
    /// match + name diff; PG's `RENAME COLUMN` keeps attnum intact, so
    /// the natural case lands here without a heuristic.
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

/// Compute the diff `old → new` for the same relation. Caller's
/// responsibility to pass descriptors that share `oid`. Dropped
/// attributes on either side (`attisdropped = true`) are filtered out
/// before diffing because PG retains them in `pg_attribute` for
/// physical-layout reasons; CH sees only live columns.
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

/// Tunables for the cache; defaults match the daemon's
/// human-cadence access pattern.
#[derive(Debug, Clone)]
pub struct ShadowCatalogConfig {
    /// `pg_last_wal_replay_lsn()` poll interval.
    pub replay_poll: Duration,
    /// [`ShadowCatalog::relation_at`] gives up after this long if shadow
    /// has not advanced past `at_lsn`. Also bounds the retry window in
    /// [`with_transient_retry`] when callers pass it in.
    pub replay_timeout: Duration,
    /// Hard cap on cache entries. `None` = unbounded.
    pub max_entries: Option<usize>,
    /// First sleep when [`with_transient_retry`] backs off.
    pub reconnect_backoff_initial: Duration,
    /// Backoff ceiling — exponential growth saturates here.
    pub reconnect_backoff_max: Duration,
}

impl Default for ShadowCatalogConfig {
    fn default() -> Self {
        Self {
            // 1 ms keeps the worker's per-record floor in line with
            // the catalog's SQL round-trip cost rather than the prior
            // 50 ms sleep. Under sustained workload where shadow's
            // apply lags pump's dispatch by O(records), each
            // `wait_for_replay` cache miss costs one round-trip
            // instead of a fixed 50 ms tick, which dominated the
            // worker's throughput in `pgbench_acceptance`.
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

/// Per-instance counters, surfaced for tests and operator metrics.
#[derive(Debug, Default, Clone)]
pub struct ShadowCatalogStats {
    pub hits: u64,
    pub misses: u64,
    pub fetches: u64,
    pub generation_bumps: u64,
    pub replay_waits: u64,
    pub evictions: u64,
    /// Records whose `db_node` is neither the shadow DB nor a shared
    /// catalog (db_node 0). Physical replication ships the whole
    /// cluster's WAL; these are rejected before the filenode query.
    pub foreign_db_skips: u64,
    /// Successful `tokio_postgres::connect` calls past the first; each one
    /// drives a generation bump and a `last_replay_lsn` reset.
    pub reconnects: u64,
}

/// Build a tokio-postgres connection string for a unix-socket shadow.
pub fn socket_conninfo(socket_dir: &str, port: u16, user: &str, dbname: &str) -> String {
    format!("host={socket_dir} port={port} user={user} dbname={dbname}")
}

/// FIFO insert-order index for the `ShadowCatalog` cache. Replaces an
/// O(n) `min_by_key` scan over the live entries with an O(log n)
/// `BTreeMap::pop_first`. Re-inserting an
/// already-cached filenode rotates via `unregister` + `register` so
/// the BTreeMap stays 1:1 with `by_filenode`.
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
    /// Last-seen descriptor per oid, retained across
    /// generation bumps. `by_filenode` / `by_oid` get logically
    /// invalidated by generation but the entries stay; `prev_known`
    /// is the source of truth for "what shape did the consumer last
    /// know about this oid?" which `compute_schema_diff` consults.
    /// Holds an `Arc` so descriptor clones stay cheap.
    prev_known: HashMap<Oid, Arc<RelDescriptor>>,
    eviction: EvictionIndex,
    last_replay_lsn: Option<u64>,
    /// Shared with the upstream [`CatalogTracker`](crate
    /// ::catalog_tracker::CatalogTracker). Acquire-load on every
    /// relation lookup; an advance triggers `invalidate`. `None` when
    /// the catalog is used standalone (tests, batch tools).
    invalidation_epoch: Option<Arc<AtomicU64>>,
    /// Latest epoch already folded into `generation`. Compared against
    /// `invalidation_epoch` on each lookup; updated to the loaded
    /// value after invalidation runs.
    last_seen_epoch: u64,
    /// Schema-event sink. Set by [`Self::subscribe`]; every
    /// descriptor fetch that resolves to a new oid or a diff against
    /// the previously-known shape pushes one [`SchemaEvent`] here.
    /// `None` keeps the producer side a no-op (standalone catalog,
    /// pre-DDL-applicator tests).
    event_tx: Option<mpsc::UnboundedSender<SchemaEvent>>,
    /// Last `generation` value [`Self::sweep_dropped`]
    /// processed. Throttle marker so high-frequency commit-boundary
    /// callers (pgbench-rate workloads) skip the sweep's SQL round-
    /// trip when no DDL has fired since the prior sweep.
    last_swept_generation: u64,
    /// Narrower than [`Self::invalidation_epoch`]: bumps
    /// only on pg_class `heap_delete` records. Catalog tracker
    /// publishes via `set_pg_class_delete_epoch`; [`Self::sweep_dropped`]
    /// gates off this counter so ADD COLUMN / CREATE INDEX etc. don't
    /// drive per-commit shadow PG sweeps in pgbench-rate workloads.
    pg_class_delete_epoch: Option<Arc<AtomicU64>>,
    /// Last `pg_class_delete_epoch` value already swept. Same shape as
    /// `last_seen_epoch` but for the narrower counter.
    last_seen_delete_epoch: u64,
    /// OID of the database this catalog's client is connected to. Lazily
    /// fetched once; used to reject foreign-DB filenodes before the
    /// relfilenode query (relfilenodes are unique only within a DB).
    /// Survives `reconnect` — `conninfo` (hence the DB) is fixed.
    current_db_oid: Option<Oid>,
    stats: ShadowCatalogStats,
}

/// `query`/`query_one`/`query_opt` with a single transparent
/// reconnect-and-retry on closed-connection errors. Other errors
/// propagate. A macro parametrizes the client `$method` so the three
/// arities share one body without boxing the future.
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
    /// Connect over an already-built connection string (key=value form,
    /// libpq style). Spawns the connection's I/O driver onto the current
    /// tokio runtime; the returned `ShadowCatalog` owns the client side.
    /// One-shot — callers that need retry-on-PG-coming-up wrap this in
    /// [`with_transient_retry`].
    ///
    /// `conninfo` is stashed so the client can be rebuilt transparently
    /// when shadow PG bounces underneath a long-lived `ShadowCatalog`.
    pub async fn connect(conninfo: &str, config: ShadowCatalogConfig) -> Result<Self> {
        let (client, conn) = tokio_postgres::connect(conninfo, NoTls).await?;
        tokio::spawn(async move {
            // Driver loop; drops out when the client side is dropped.
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
            last_swept_generation: 0,
            pg_class_delete_epoch: None,
            last_seen_delete_epoch: 0,
            current_db_oid: None,
            stats: ShadowCatalogStats::default(),
        })
    }

    /// Install the DROP-only epoch counter. Pair with
    /// [`crate::catalog_tracker::CatalogTracker::set_pg_class_delete_epoch`].
    pub fn set_pg_class_delete_epoch(&mut self, epoch: Arc<AtomicU64>) {
        self.last_seen_delete_epoch = epoch.load(Ordering::Acquire);
        self.pg_class_delete_epoch = Some(epoch);
    }

    /// Install the schema-event sink. Returns the matching
    /// `Receiver` so the caller (typically the worker task owning
    /// [`crate::xact_buffer::BufferingDecoderSink`]) can drain events
    /// as the catalog discovers descriptor shapes. Subsequent
    /// `subscribe` calls overwrite the prior sink — only one
    /// subscriber is supported by design.
    pub fn subscribe(&mut self) -> mpsc::UnboundedReceiver<SchemaEvent> {
        let (tx, rx) = mpsc::unbounded_channel();
        self.event_tx = Some(tx);
        rx
    }

    /// Bootstrap-time fan-out. Resolves every relation
    /// in the named source namespaces and emits one `Added` event per
    /// relation that the catalog hadn't seen before. Idempotent across
    /// daemon restarts because the applicator's `CREATE TABLE IF NOT
    /// EXISTS` ack-skips a second run.
    ///
    /// Caller passes the list of namespace names that have
    /// `auto_create = true`; relations in other namespaces stay
    /// undisclosed until the decoder fetches them on first WAL touch.
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
            // Fetch the descriptor through the regular path so the
            // resulting `Added` event flows through `record_descriptor`
            // and lands in the subscriber's mpsc queue.
            match self.relation_by_oid(oid).await {
                Ok(_) => added += 1,
                Err(CatalogError::NotFoundByOid(_)) => continue,
                Err(e) => return Err(e),
            }
        }
        Ok(added)
    }

    /// Seed the schema-diff baseline (`prev_known`) for the
    /// operator-pinned relations before [`Self::subscribe`]. Warms
    /// `prev_known` with each relation's boot-time shape so its first
    /// post-start `ALTER ADD COLUMN` (or RENAME / DROP) diffs against
    /// that shape and surfaces as `Changed` (→ CH `ALTER`) rather than
    /// cold-`prev_known` `Added` — which the applicator skips for
    /// operator-pinned dests, leaving CH a column behind.
    ///
    /// `qualified_names` are `"namespace.relname"` source identifiers
    /// (the pinned mapping keys). Each resolves to a shadow oid via
    /// `to_regclass($1)`; a NULL resolution is skipped (preflight already
    /// guarantees mapped rels exist, so a miss here is purely defensive).
    /// Oids already in `prev_known` are skipped, keeping the seed
    /// idempotent across `--start-lsn` resume.
    ///
    /// Fetches each descriptor through [`Self::relation_by_oid`], which
    /// flows through `insert` → `record_descriptor` and warms
    /// `prev_known` (plus `by_oid` / `by_filenode`). Must run before
    /// `subscribe()`: `send_event` is a no-op while `event_tx` is `None`,
    /// so seeding emits no `Added` to the applicator and does zero CH
    /// work at boot. Returns the count of relations seeded.
    ///
    /// Records the *full* source descriptor, not the pinned mapping, so a
    /// later `ALTER` diffs against the whole source shape: columns the
    /// operator deliberately left unmapped sit in the baseline and read
    /// as "excluded", never "added since". See the pinned-subset reasoning
    /// in `plans/future/pinned_ddl_baseline.md`.
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

    /// Emit a `Dropped` event for an oid the decoder observed a
    /// pg_class `heap_delete` for. `qualified_name` is resolved from the
    /// last-known descriptor in `prev_known`; returns `false`
    /// when the catalog has never seen the oid (the relation was never
    /// queried via shadow, so CH never learned about it either —
    /// nothing for the applicator to do).
    pub fn emit_dropped(&mut self, oid: Oid) -> bool {
        let Some(prev) = self.prev_known.remove(&oid) else {
            return false;
        };
        self.by_oid.remove(&oid);
        // Filenode entry can stay until the next access; cheap eviction.
        self.send_event(SchemaEvent::Dropped {
            oid,
            qualified_name: prev.qualified_name.clone(),
        });
        true
    }

    /// Poll-based DROP TABLE discovery.
    ///
    /// Polls shadow's `pg_class` for every oid currently in
    /// `prev_known`; any oid that no longer exists in shadow
    /// gets a `Dropped` event emitted (via [`Self::emit_dropped`]) and
    /// is removed from `prev_known`. Returns the count of dropped oids
    /// surfaced.
    ///
    /// The natural detection path (decoder observes pg_class
    /// `heap_delete` for the dropped relation) doesn't fire for system
    /// catalogs with `relreplident = 'n'` — PG doesn't include the old
    /// tuple in WAL, so the decoder can't extract the dropped oid.
    /// Poll-based discovery sidesteps that limitation at the cost of
    /// one SQL round-trip per sweep.
    ///
    /// Throttled by `last_swept_generation`: a sweep that finds no
    /// drops bumps the marker so subsequent calls within the same
    /// generation no-op without hitting the SQL layer. High-frequency
    /// commit-boundary callers (pgbench-rate workloads) thus avoid
    /// pessimising shadow PG with one query per commit; the per-commit
    /// cost reduces to an atomic load on the invalidation epoch.
    pub async fn sweep_dropped(&mut self) -> Result<usize> {
        // Fold any pending invalidations FIRST so a DDL between this
        // sweep and the prior one bumps `generation` to a value past
        // `last_swept_generation`.
        self.drain_invalidations();
        // Snapshot the DROP-only epoch BEFORE the query so any
        // concurrent advance doesn't get swept-under by the
        // post-query update; the next sweep would re-fire.
        let snapshot_delete_epoch = self
            .pg_class_delete_epoch
            .as_ref()
            .map(|e| e.load(Ordering::Acquire))
            .unwrap_or(self.last_seen_delete_epoch);
        if self.prev_known.is_empty()
            || (self.last_swept_generation == self.generation
                && snapshot_delete_epoch == self.last_seen_delete_epoch)
        {
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
        self.last_swept_generation = self.generation;
        self.last_seen_delete_epoch = snapshot_delete_epoch;
        Ok(emitted)
    }

    fn send_event(&self, ev: SchemaEvent) {
        if let Some(tx) = &self.event_tx {
            // Unbounded channel: send only fails if the receiver was
            // dropped — at daemon shutdown. Silently drop in that case.
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
                    // Re-insert of the exact same Arc — no shape change.
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

    /// Install the shared epoch counter. Pass the same `Arc` clone as
    /// the upstream
    /// [`CatalogTracker::set_invalidation_epoch`](crate::catalog_tracker::CatalogTracker::set_invalidation_epoch)
    /// call.
    pub fn set_invalidation_epoch(&mut self, epoch: Arc<AtomicU64>) {
        // Adopt the current value as already-seen so a non-zero epoch
        // (e.g., catalog opened mid-stream) doesn't trigger a spurious
        // initial invalidate on the first lookup.
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

    /// Current generation. Bumps every `invalidate` call.
    pub fn generation(&self) -> u64 {
        self.generation
    }

    /// True when [`Self::sweep_dropped`] would do real
    /// work (catalog tracker observed a pg_class `heap_delete` since
    /// the last sweep AND there's something to sweep). Read-only +
    /// cheap (one atomic load + integer compare); callers gate the
    /// per-commit sweep on this to keep pgbench-rate workloads off
    /// the catalog's SQL path when nothing's been dropped.
    pub fn has_pending_sweep(&self) -> bool {
        if self.prev_known.is_empty() {
            return false;
        }
        // When the daemon hasn't wired the DROP-only counter, fall
        // back to the conservative path (any catalog invalidation
        // triggers a sweep). Costs an extra SQL/commit but
        // correctness over throughput when wiring is partial.
        let current = match &self.pg_class_delete_epoch {
            Some(e) => e.load(Ordering::Acquire),
            None => {
                // Conservative fallback path.
                let inv = self
                    .invalidation_epoch
                    .as_ref()
                    .map(|e| e.load(Ordering::Acquire))
                    .unwrap_or(self.last_seen_epoch);
                return inv != self.last_seen_epoch
                    || self.last_swept_generation != self.generation;
            }
        };
        current != self.last_seen_delete_epoch
    }

    pub fn stats(&self) -> &ShadowCatalogStats {
        &self.stats
    }

    /// Cumulative count of entries currently held in the filenode cache.
    pub fn cached(&self) -> usize {
        self.by_filenode.len()
    }

    /// Bump the generation counter, marking every cached entry stale.
    /// Old entries are retained until next access (lazy eviction) — a
    /// cheap operation regardless of how many catalog writes a commit
    /// produced.
    pub fn invalidate(&mut self) -> u64 {
        self.generation = self.generation.wrapping_add(1);
        self.stats.generation_bumps += 1;
        self.generation
    }

    /// Drop the current client and rebuild from stashed `conninfo`. One-shot;
    /// retry-on-failure is the caller's job via [`with_transient_retry`].
    ///
    /// Catalog mutations may have landed during the down window without
    /// producing an `invalidate` call from the upstream catalog tracker, so
    /// generation is bumped to mark every cache entry stale on next access.
    /// `last_replay_lsn` is reset because PG's replay LSN starts fresh
    /// against a restarted instance.
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

    /// `query_one` with a single transparent reconnect-and-retry on
    /// closed-connection errors. Other errors propagate as-is.
    async fn query_one_retry(
        &mut self,
        statement: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Row> {
        query_with_reconnect!(self, query_one, statement, params)
    }

    /// `query_opt` with a single transparent reconnect-and-retry on
    /// closed-connection errors.
    async fn query_opt_retry(
        &mut self,
        statement: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Option<Row>> {
        query_with_reconnect!(self, query_opt, statement, params)
    }

    /// `query` (multi-row) with a single transparent reconnect-and-retry on
    /// closed-connection errors.
    async fn query_retry(
        &mut self,
        statement: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Vec<Row>> {
        query_with_reconnect!(self, query, statement, params)
    }

    /// Last observed `pg_last_wal_replay_lsn()` value (may be NULL when
    /// shadow has not replayed anything yet, e.g. fresh standby start).
    pub fn last_observed_replay(&self) -> Option<u64> {
        self.last_replay_lsn
    }

    /// Wait until shadow's replay LSN ≥ `target`. Returns the LSN
    /// observed at the deciding poll. `target = 0` returns as soon as
    /// any non-NULL LSN is observed.
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

    /// Look up a relation by its `RelFileNode`, gated on shadow having
    /// replayed past `at_lsn`. The decoder's standard call shape.
    ///
    /// `at_lsn = 0` skips the replay-LSN gate entirely; use when the
    /// caller already proved the catalog is fresh enough by other means
    /// (e.g. an immediately preceding `wait_for_replay`).
    pub async fn relation_at(
        &mut self,
        rfn: RelFileNode,
        at_lsn: u64,
    ) -> Result<Arc<RelDescriptor>> {
        self.drain_invalidations();
        if at_lsn > 0 {
            self.wait_for_replay(at_lsn).await?;
            // Re-check after the await: a DDL observed concurrently can
            // have bumped the epoch while wait_for_replay yielded.
            self.drain_invalidations();
        }
        if let Some(entry) = self.by_filenode.get(&rfn)
            && entry.generation == self.generation
        {
            self.stats.hits += 1;
            return Ok(entry.desc.clone());
        }
        self.stats.misses += 1;
        let desc = self
            .fetch_by_filenode(rfn)
            .await?
            .ok_or(CatalogError::NotFoundByFilenode(rfn))?;
        Ok(self.insert(desc))
    }

    /// Filenode resolution for the async decode pool. Reuses the descriptor
    /// the WAL-ordered inline path
    /// ([`BufferingDecoderSink`](crate::xact_buffer::BufferingDecoderSink))
    /// already resolved + cached for `rfn` — serving the cached entry at ANY
    /// generation — and only fetches (transiently, never writing the cache or
    /// emitting events) when the filenode is absent entirely.
    ///
    /// The pool routes heaps that the inline path already decoded against a
    /// specific descriptor, but does so asynchronously: a worker can lag past
    /// a later DDL that bumped the generation. Re-resolving through
    /// [`Self::relation_at`] there is doubly wrong, as two drills show:
    ///
    /// * ADD COLUMN keeps the filenode. A lagging fetch at an older heap's
    ///   `at_lsn` (replay gate already satisfied) reads shadow's pre-DDL shape
    ///   but `insert`s it at the post-DDL generation — poisoning the
    ///   entry so the inline path's own later resolution of the post-DDL rows
    ///   gets a hit and never fires `Changed` (barrier never reaches CH).
    /// * TRUNCATE rotates the filenode. Once shadow replays it, the old
    ///   filenode no longer resolves from `pg_class`, so a fetch returns
    ///   `None` and the pre-TRUNCATE rows fail outright — the cached entry is
    ///   the only surviving descriptor for them.
    ///
    /// Reusing the cached shape sidesteps both: schema-change detection and
    /// cache maintenance stay solely on the inline path.
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
        // Absent (never inline-resolved, or evicted under cache pressure):
        // fetch transiently. No insert — keep the pool out of cache state.
        self.stats.misses += 1;
        let desc = self
            .fetch_by_filenode(rfn)
            .await?
            .ok_or(CatalogError::NotFoundByFilenode(rfn))?;
        Ok(Arc::new(desc))
    }

    /// Look up a relation by oid (no replay-LSN gate; intended for
    /// oid-only references like xact records or shared-catalog probes).
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
        // Emit Added/Changed against the previously-known
        // descriptor (kept in `prev_known` across generation bumps).
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
        // Reject foreign-DB filenodes before querying: relfilenodes are
        // unique only within a (database, tablespace), so a foreign
        // db_node's rel_node can collide with a real local relation and
        // resolve to the wrong descriptor. spc_node need not match —
        // relfilenode is unique per database regardless of tablespace.
        // db_node 0 = shared catalog (visible from any DB), left through.
        if is_foreign_db(rfn.db_node, self.current_db_oid().await?) {
            self.stats.foreign_db_skips += 1;
            return Err(CatalogError::ForeignDatabase(rfn));
        }
        self.stats.fetches += 1;
        // pg_relation_filenode(oid) abstracts mapped vs unmapped: for
        // mapped catalogs it reads pg_filenode.map, for regular tables
        // it reads pg_class.relfilenode.
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
        // db_node is the current database. Resolve via current_database()'s oid.
        let db_node = self.current_database_oid().await?;
        let rfn = RelFileNode {
            spc_node,
            db_node,
            rel_node,
        };
        Ok(Some(self.descriptor_from_row(&row, rfn).await?))
    }

    /// Build a [`RelDescriptor`] from a pg_class⋈pg_namespace row whose
    /// first 7 columns are (oid, relnamespace, nspname, relname, relkind,
    /// relpersistence, relreplident), pairing it with an already-resolved
    /// `rfn`. Fans out to `fetch_replident` + `fetch_attributes`.
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
                // indkey is int2vector internally; cast to int2[] for
                // tokio-postgres' standard Kind::Array(int2) decode.
                // Missing row → table has no PK, decoder emits old = None.
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
                // indkey is int2vector internally, cast to int2[] so the
                // tokio-postgres array decode path lifts it into Vec<i16>
                // through the standard Kind::Array(int2) branch.
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
        // `attmissingval` is `anyarray`, which doesn't support
        // subscript or `unnest`. Cast through `::text` surfaces PG's
        // array_out literal (`{val}` for a single-element array);
        // strip braces + dequote in `parse_array_one_element` to
        // recover the typoutput text form for `getmissingattr`.
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

    /// Memoized [`Self::current_database_oid`]. Cached across `reconnect`
    /// since `conninfo` pins the database.
    async fn current_db_oid(&mut self) -> Result<Oid> {
        if let Some(oid) = self.current_db_oid {
            return Ok(oid);
        }
        let oid = self.current_database_oid().await?;
        self.current_db_oid = Some(oid);
        Ok(oid)
    }
}

/// Resolve a WAL-observed filenode under the shared mutex. `at_lsn` gates on
/// shadow replay (pg_last_wal_replay_lsn); lock releases before return
pub async fn resolve_at(
    catalog: &Mutex<ShadowCatalog>,
    rfn: RelFileNode,
    at_lsn: u64,
) -> Result<Arc<RelDescriptor>> {
    let mut cat = catalog.lock().await;
    cat.relation_at(rfn, at_lsn).await
}

/// [`resolve_at`] for the async decode pool — reuses the inline path's cached
/// descriptor and never mutates the cache or emits schema events. See
/// [`ShadowCatalog::relation_at_pooled`].
pub async fn resolve_at_pooled(
    catalog: &Mutex<ShadowCatalog>,
    rfn: RelFileNode,
    at_lsn: u64,
) -> Result<Arc<RelDescriptor>> {
    let mut cat = catalog.lock().await;
    cat.relation_at_pooled(rfn, at_lsn).await
}

/// Strip the single element from a PG array text literal `{val}` and
/// dequote it. Used to recover `attmissingval[1]`'s typoutput form
/// after `array_out` rendering of `anyarray`.
///
/// PG array_out quoting rules (from `~/s/postgresql/src/backend/utils/adt/arrayfuncs.c`
/// `array_out`): elements containing braces, commas, double-quotes,
/// backslashes, or whitespace get wrapped in `"`; internal `"` and
/// `\` are escaped with `\`. NULL elements render as the literal
/// `NULL` (no quotes). Returns `None` on `NULL`, empty array `{}`,
/// or any shape outside single-element 1-D.
pub(crate) fn parse_array_one_element(raw: &str) -> Option<String> {
    let inner = raw.strip_prefix('{')?.strip_suffix('}')?;
    if inner.is_empty() {
        return None;
    }
    if inner == "NULL" {
        return None;
    }
    if let Some(rest) = inner.strip_prefix('"') {
        // Quoted form: walk and unescape; ensure trailing `"`.
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
        // Unquoted scalar; no internal special chars by PG's array_out
        // guarantee.
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

/// Wrap an async operation that calls into [`ShadowCatalog`] with
/// exponential-backoff retry on transient PG errors (closed connection,
/// "the database system is starting up", connect-time refused). Non-PG
/// errors (parse failures, not-found, replay timeouts) bypass retry and
/// surface immediately.
///
/// Sits outside `ShadowCatalog` on purpose: cache invalidation and
/// replay-LSN bookkeeping inside the catalog stay unaware of in-flight
/// retries, observing only the final outcome.
///
/// `timeout` caps total wall time, `initial_backoff` is the first sleep,
/// `max_backoff` caps the exponential growth.
pub async fn with_transient_retry<R, F>(
    timeout: Duration,
    initial_backoff: Duration,
    max_backoff: Duration,
    mut op: F,
) -> Result<R>
where
    F: AsyncFnMut() -> Result<R>,
{
    let start = Instant::now();
    let mut delay = initial_backoff;
    loop {
        match op().await {
            Ok(r) => return Ok(r),
            Err(e) => {
                if !is_transient(&e) || start.elapsed() >= timeout {
                    return Err(e);
                }
                tokio::time::sleep(delay).await;
                delay = delay.saturating_mul(2).min(max_backoff);
            }
        }
    }
}

/// True for errors that indicate "PG isn't reachable right now, try
/// again". Currently any [`CatalogError::Pg`] qualifies — connect-refused
/// and `CANNOT_CONNECT_NOW` both surface that way, and steady-state SQL
/// errors against well-known queries are not expected.
fn is_transient(err: &CatalogError) -> bool {
    matches!(err, CatalogError::Pg(_))
}

/// True when `db_node` belongs to neither the connected shadow DB nor a
/// shared catalog (db_node 0). Such filenodes come from other databases
/// in the cluster's physical WAL and must not resolve locally.
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
        // Shared catalog (db_node 0) is never foreign.
        assert!(!is_foreign_db(0, 16384));
        // Same DB resolves locally.
        assert!(!is_foreign_db(16384, 16384));
        // Different DB in the cluster's WAL is rejected.
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
        // Reinserting an already-cached filenode rotates it to the back:
        // callers unregister the old order before registering a fresh one.
        // Without this, the BTreeMap accumulates entries that no longer
        // match the live cache and eviction picks ghost victims.
        let mut ix = EvictionIndex::default();
        let o1 = ix.register(rfn(10));
        ix.register(rfn(20));
        ix.unregister(o1);
        let _ = ix.register(rfn(10));
        assert_eq!(ix.len(), 2);
        // Oldest is now rfn(20), not rfn(10).
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
        // PG retains dropped columns in pg_attribute with attisdropped=true.
        // Diff must ignore them so DROP COLUMN doesn't re-surface as
        // pseudo-still-present on the new side.
        let mut a = mk_attr(2, "x", 25, false);
        a.dropped = true;
        let old = mk_desc(16400, vec![mk_attr(1, "id", 23, true), a]);
        let new = mk_desc(16400, vec![mk_attr(1, "id", 23, true)]);
        let d = compute_schema_diff(&old, &new);
        // Already dropped on old side; nothing to fire.
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
