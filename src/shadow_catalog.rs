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
//! Generation invalidation: when an upstream observer (the filter's
//! catalog-tracker, Phase 5/7 glue) sees a commit landing writes into
//! `pg_catalog`, it calls [`ShadowCatalog::invalidate`]. The counter
//! bumps; stale cache entries (whose stored generation no longer
//! matches) are rejected on next access and re-fetched lazily.
//!
//! Single-database model: a `ShadowCatalog` instance is bound to one
//! database. Shared catalogs (`db_node == 0`) are visible from any
//! connection and resolve correctly via `pg_relation_filenode`.
//! Cross-user-database replay needs one cache per database; out of
//! scope for Phase 4.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use thiserror::Error;
use tokio_postgres::types::Oid;
use tokio_postgres::{Client, NoTls};
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
    pub attributes: Vec<RelAttr>,
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
    /// has not advanced past `at_lsn`.
    pub replay_timeout: Duration,
    /// Hard cap on cache entries. `None` = unbounded.
    pub max_entries: Option<usize>,
}

impl Default for ShadowCatalogConfig {
    fn default() -> Self {
        Self {
            replay_poll: Duration::from_millis(50),
            replay_timeout: Duration::from_secs(30),
            max_entries: Some(4096),
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
}

/// Build a tokio-postgres connection string for a unix-socket shadow.
pub fn socket_conninfo(socket_dir: &str, port: u16, user: &str, dbname: &str) -> String {
    format!("host={socket_dir} port={port} user={user} dbname={dbname}")
}

pub struct ShadowCatalog {
    client: Client,
    config: ShadowCatalogConfig,
    generation: u64,
    by_filenode: HashMap<RelFileNode, CacheEntry>,
    by_oid: HashMap<Oid, CacheEntry>,
    insert_seq: u64,
    last_replay_lsn: Option<u64>,
    stats: ShadowCatalogStats,
}

impl ShadowCatalog {
    /// Connect over an already-built connection string (key=value form,
    /// libpq style). Spawns the connection's I/O driver onto the current
    /// tokio runtime; the returned `ShadowCatalog` owns the client side.
    pub async fn connect(conninfo: &str, config: ShadowCatalogConfig) -> Result<Self> {
        let (client, conn) = tokio_postgres::connect(conninfo, NoTls).await?;
        tokio::spawn(async move {
            // Driver loop; drops out when the client side is dropped.
            let _ = conn.await;
        });
        Ok(Self {
            client,
            config,
            generation: 0,
            by_filenode: HashMap::new(),
            by_oid: HashMap::new(),
            insert_seq: 0,
            last_replay_lsn: None,
            stats: ShadowCatalogStats::default(),
        })
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
                .client
                .query_one("SELECT pg_last_wal_replay_lsn()::text", &[])
                .await?;
            let raw: Option<String> = row.get(0);
            if let Some(s) = raw {
                let lsn = parse_pg_lsn(&s)
                    .map_err(|e| CatalogError::Parse(format!("pg_last_wal_replay_lsn {s:?}: {e}")))?;
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
        if at_lsn > 0 {
            self.wait_for_replay(at_lsn).await?;
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
        self.insert_seq += 1;
        let entry = CacheEntry {
            generation: self.generation,
            insert_order: self.insert_seq,
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
            let Some(victim_rfn) = self
                .by_filenode
                .iter()
                .min_by_key(|(_, e)| e.insert_order)
                .map(|(k, _)| *k)
            else {
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
            .client
            .query_opt(
                "SELECT \
                    c.oid::oid, \
                    c.relnamespace::oid, \
                    n.nspname::text, \
                    c.relname::text, \
                    c.relkind::text, \
                    c.relpersistence::text \
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
        let attributes = self.fetch_attributes(oid).await?;
        Ok(Some(RelDescriptor {
            rfn,
            oid,
            namespace_oid,
            namespace_name,
            name,
            kind,
            persistence,
            attributes,
        }))
    }

    async fn fetch_by_oid(&mut self, oid: Oid) -> Result<Option<RelDescriptor>> {
        self.stats.fetches += 1;
        let row = self
            .client
            .query_opt(
                "SELECT \
                    c.oid::oid, \
                    c.relnamespace::oid, \
                    n.nspname::text, \
                    c.relname::text, \
                    c.relkind::text, \
                    c.relpersistence::text, \
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
        let spc_node: Oid = row.get(6);
        let rel_node: Oid = row.get(7);
        // db_node is the current database. Resolve via current_database()'s oid.
        let db_node = self.current_database_oid().await?;
        let rfn = RelFileNode {
            spc_node,
            db_node,
            rel_node,
        };
        let attributes = self.fetch_attributes(oid).await?;
        Ok(Some(RelDescriptor {
            rfn,
            oid,
            namespace_oid,
            namespace_name,
            name,
            kind,
            persistence,
            attributes,
        }))
    }

    async fn fetch_attributes(&self, rel_oid: Oid) -> Result<Vec<RelAttr>> {
        let rows = self
            .client
            .query(
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

    async fn current_database_oid(&self) -> Result<Oid> {
        let row = self
            .client
            .query_one(
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

#[cfg(test)]
mod tests {
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
    }
}
