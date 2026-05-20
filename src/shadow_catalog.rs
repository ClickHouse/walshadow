//! Phase 4: shadow PG catalog cache.
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
//! Generation invalidation: a [`CatalogTracker`](crate::catalog_tracker
//! ::CatalogTracker) wired with [`set_invalidation_epoch`](crate
//! ::catalog_tracker::CatalogTracker::set_invalidation_epoch) bumps a
//! shared `AtomicU64` on every catalog-touching record. The catalog
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
//! [`invalidate`](ShadowCatalog::invalidate)) takes `&mut self`. The
//! cache state is technically interior-mutable (an `RwLock` over the
//! two `HashMap`s + atomics for stats would suffice for the hit path)
//! but [PLAN.md §Phase 5](../plans/PLAN.md)'s spec'd `&self` shape is
//! deferred — see [PRE5b7](../plans/PRE5b7.md). Callers that need
//! concurrent access (drain task, Phase 5 [`DecoderSink`], oracle)
//! wrap the catalog in `Arc<tokio::sync::Mutex<_>>` at the daemon
//! level and share clones. Single-task lookups today are cheap enough
//! that mutex serialisation costs nothing measurable; the lock-free
//! refactor lands when the lookup-rate hot path actually exists.
//!
//! Single-database model: a `ShadowCatalog` instance is bound to one
//! database. Shared catalogs (`db_node == 0`) are visible from any
//! connection and resolve correctly via `pg_relation_filenode`.
//! Cross-user-database replay needs one cache per database; out of
//! scope for Phase 4.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use thiserror::Error;
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
    /// `pg_class.relkind`: `'r'` table, `'i'` index, `'S'` sequence,
    /// `'t'` toast, `'v'` view, `'m'` matview, `'c'` composite,
    /// `'f'` foreign, `'p'` partitioned.
    pub kind: char,
    /// `pg_class.relpersistence`: `'p'` permanent, `'u'` unlogged,
    /// `'t'` temporary.
    pub persistence: char,
    /// `pg_class.relreplident` resolved to the form Phase 5's decoder
    /// needs: `UsingIndex` carries the replica-identity index's oid
    /// and the column-number list (`pg_index.indkey`); `Default`
    /// carries the primary key's `indkey` (or `None` when no PK), so
    /// old-tuple decode under `XLH_UPDATE_CONTAINS_OLD_KEY` resolves
    /// without a second catalog round-trip.
    pub replident: ReplIdent,
    pub attributes: Vec<RelAttr>,
}

/// Resolved `pg_class.relreplident`. `pg_class` stores a single char
/// (`'d'`, `'n'`, `'f'`, `'i'`); the `i` variant carries the
/// replica-identity index's oid and the indexed-column attnum list
/// (`pg_index.indkey`) because Phase 5's decoder needs both to
/// interpret `XLH_UPDATE_CONTAINS_OLD_KEY` / `XLH_UPDATE_CONTAINS_OLD_TUPLE`
/// payloads.
#[derive(Debug, Clone, PartialEq)]
pub enum ReplIdent {
    /// `'d'`. Old-tuple payload contains primary-key columns when
    /// `XLH_UPDATE_CONTAINS_OLD_KEY` rides on UPDATE, or on every
    /// DELETE. `pk_attnums` is `Some(pg_index.indkey)` for the
    /// table's primary key (resolved at descriptor build via
    /// `indisprimary = true`), or `None` when no PK exists — in
    /// which case Phase 5 decodes `old = None` always. See Phase 5
    /// relreplident behaviour table in PLAN.md.
    Default { pk_attnums: Option<Vec<i16>> },
    /// `'n'`. Old-tuple payload is empty; UPDATE/DELETE rows lack a
    /// key. Phase 5's emitter drops these.
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
}

/// Tunables for the cache; defaults match the Phase 4 daemon's
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
            replay_poll: Duration::from_millis(50),
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
/// `BTreeMap::pop_first` (PRE5b10 item 7). Re-inserting an
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
    stats: ShadowCatalogStats,
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
            eviction: EvictionIndex::default(),
            last_replay_lsn: None,
            invalidation_epoch: None,
            last_seen_epoch: 0,
            stats: ShadowCatalogStats::default(),
        })
    }

    /// Install the shared epoch counter. Pass the same `Arc` clone as
    /// the upstream [`CatalogTracker::set_invalidation_epoch`](crate
    /// ::catalog_tracker::CatalogTracker::set_invalidation_epoch) call.
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

    /// Current generation. Bumps every [`invalidate`] call.
    pub fn generation(&self) -> u64 {
        self.generation
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
        self.ensure_open().await?;
        match self.client.query_one(statement, params).await {
            Ok(r) => Ok(r),
            Err(e) => {
                if self.client.is_closed() {
                    self.reconnect().await?;
                    Ok(self.client.query_one(statement, params).await?)
                } else {
                    Err(e.into())
                }
            }
        }
    }

    /// `query_opt` with a single transparent reconnect-and-retry on
    /// closed-connection errors.
    async fn query_opt_retry(
        &mut self,
        statement: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Option<Row>> {
        self.ensure_open().await?;
        match self.client.query_opt(statement, params).await {
            Ok(r) => Ok(r),
            Err(e) => {
                if self.client.is_closed() {
                    self.reconnect().await?;
                    Ok(self.client.query_opt(statement, params).await?)
                } else {
                    Err(e.into())
                }
            }
        }
    }

    /// `query` (multi-row) with a single transparent reconnect-and-retry on
    /// closed-connection errors.
    async fn query_retry(
        &mut self,
        statement: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Vec<Row>> {
        self.ensure_open().await?;
        match self.client.query(statement, params).await {
            Ok(r) => Ok(r),
            Err(e) => {
                if self.client.is_closed() {
                    self.reconnect().await?;
                    Ok(self.client.query(statement, params).await?)
                } else {
                    Err(e.into())
                }
            }
        }
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
        let oid: Oid = row.get(0);
        let namespace_oid: Oid = row.get(1);
        let namespace_name: String = row.get(2);
        let name: String = row.get(3);
        let kind = one_char(row.get::<_, String>(4), "relkind")?;
        let persistence = one_char(row.get::<_, String>(5), "relpersistence")?;
        let replident_char = one_char(row.get::<_, String>(6), "relreplident")?;
        let replident = self.fetch_replident(replident_char, oid).await?;
        let attributes = self.fetch_attributes(oid).await?;
        Ok(Some(RelDescriptor {
            rfn,
            oid,
            namespace_oid,
            namespace_name,
            name,
            kind,
            persistence,
            replident,
            attributes,
        }))
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
        let oid: Oid = row.get(0);
        let namespace_oid: Oid = row.get(1);
        let namespace_name: String = row.get(2);
        let name: String = row.get(3);
        let kind = one_char(row.get::<_, String>(4), "relkind")?;
        let persistence = one_char(row.get::<_, String>(5), "relpersistence")?;
        let replident_char = one_char(row.get::<_, String>(6), "relreplident")?;
        let spc_node: Oid = row.get(7);
        let rel_node: Oid = row.get(8);
        // db_node is the current database. Resolve via current_database()'s oid.
        let db_node = self.current_database_oid().await?;
        let rfn = RelFileNode {
            spc_node,
            db_node,
            rel_node,
        };
        let replident = self.fetch_replident(replident_char, oid).await?;
        let attributes = self.fetch_attributes(oid).await?;
        Ok(Some(RelDescriptor {
            rfn,
            oid,
            namespace_oid,
            namespace_name,
            name,
            kind,
            persistence,
            replident,
            attributes,
        }))
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
                    t.typstorage::text \
                 FROM pg_attribute a \
                 JOIN pg_type t ON t.oid = a.atttypid \
                 WHERE a.attrelid = $1 AND a.attnum >= 1 \
                 ORDER BY a.attnum",
                &[&rel_oid],
            )
            .await?;
        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

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
