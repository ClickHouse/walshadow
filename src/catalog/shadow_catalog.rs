//! Shadow PG descriptor capture + name-keyed resolution.
//!
//! Decode never reads this: interval-scoped answers come from the durable
//! [`DescriptorLog`](crate::catalog::desc_log::DescriptorLog), which capture
//! populates from here at catalog boundaries (batched
//! [`ShadowCatalog::fetch_descriptors_batch`] /
//! [`ShadowCatalog::fetch_all_descriptors`] round trips). Name-keyed reads
//! ([`ShadowCatalog::descriptor_by_name`], toast resolution) serve opt-in
//! dispatch, backfill standup, and preflight.
//!
//! One descriptor definition, `descriptor_from_rows`, over projections
//! `pgext/overlay.c` names. Bridge worker's `SCAN` supplies committed and
//! uncommitted catalog rows; one mirroring statement emits the same
//! projections off one MVCC snapshot, for committed reads the worker cannot
//! answer for a single replay position.
//!
//! Single-database model: instance bound to one DB.

use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, LazyLock};
use std::time::{Duration, Instant};

use backon::{ExponentialBuilder, RetryableWithContext};
use thiserror::Error;
use tokio_postgres::types::{Oid, PgLsn, ToSql};
use tokio_postgres::{Client, NoTls, Row};
use walrus::pg::walparser::RelFileNode;

use crate::ops::bridge::{
    AttributeRow, Bridge, BridgeError, Catalog, ClassRow, IndexRow, MAX_SCAN_OIDS, NamespaceRow,
    ScanRow, TypeRow,
};
#[cfg(test)]
use crate::pg::socket_conninfo;
use crate::schema::{FIRST_NORMAL_OBJECT_ID, RelDescriptor, RelName, ReplIdent};

#[derive(Debug, Error)]
pub enum CatalogError {
    #[error("pg: {0}")]
    Pg(#[from] tokio_postgres::Error),
    /// The bridge owns its own redial, so a failure that reaches here is not
    /// worth another catalog-level retry
    #[error("bridge: {0}")]
    Bridge(#[from] BridgeError),
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

#[derive(Debug, Clone)]
pub struct ShadowCatalogConfig {
    /// `pg_last_wal_replay_lsn()` poll interval
    pub replay_poll: Duration,
    /// [`ShadowCatalog::wait_for_replay`] gives up after this; also bounds
    /// [`with_transient_retry`]'s window
    pub replay_timeout: Duration,
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
            reconnect_backoff_initial: Duration::from_millis(100),
            reconnect_backoff_max: Duration::from_secs(1),
        }
    }
}

#[derive(Debug, Default, Clone)]
pub struct ShadowCatalogStats {
    pub fetches: u64,
    /// Committed reads answered off the mirroring statement, not the worker
    pub mirror_fetches: u64,
    pub replay_waits: u64,
    pub reconnects: u64,
}

pub struct ShadowCatalog {
    client: Client,
    conninfo: String,
    config: ShadowCatalogConfig,
    last_replay_lsn: Option<u64>,
    /// DB oid this client is connected to; survives `reconnect` since
    /// `conninfo` pins the DB
    current_db_oid: Option<Oid>,
    /// `pg_database.dattablespace`, memoized for the same reason
    default_tablespace: Option<Oid>,
    bridge: Arc<Bridge>,
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
    pub async fn connect(
        conninfo: &str,
        config: ShadowCatalogConfig,
        bridge: Arc<Bridge>,
    ) -> Result<Self> {
        let (client, conn) = tokio_postgres::connect(conninfo, NoTls).await?;
        tokio::spawn(async move {
            let _ = conn.await;
        });
        Ok(Self {
            client,
            conninfo: conninfo.to_string(),
            config,
            last_replay_lsn: None,
            current_db_oid: None,
            default_tablespace: None,
            bridge,
            stats: ShadowCatalogStats::default(),
        })
    }

    async fn oid_by_name(&mut self, rel: &RelName) -> Result<Option<Oid>> {
        let (ns, name): (&str, &str) = (&rel.namespace, &rel.name);
        let row = self
            .query_opt_retry(
                "SELECT c.oid FROM pg_class c \
                 JOIN pg_namespace n ON n.oid = c.relnamespace \
                 WHERE n.nspname = $1 AND c.relname = $2",
                &[&ns, &name],
            )
            .await?;
        Ok(row.map(|r| r.get(0)))
    }

    /// Resolve a relation name to its current source descriptor via shadow's
    /// `pg_class`, or `None` when the rel isn't known yet — the
    /// forward-declared case the per-table opt-in dispatch parks in
    /// `pending_decl`.
    pub async fn descriptor_by_name(
        &mut self,
        rel: &RelName,
    ) -> Result<Option<Arc<RelDescriptor>>> {
        let Some(oid) = self.oid_by_name(rel).await? else {
            return Ok(None);
        };
        Ok(self.fetch_one(oid).await?.map(Arc::new))
    }

    async fn fetch_one(&mut self, oid: Oid) -> Result<Option<RelDescriptor>> {
        let (_, descs) = self.fetch_descriptors_batch(&[oid]).await?;
        Ok(descs.into_iter().next())
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
        Ok(self.fetch_one(toast_oid).await?.map(Arc::new))
    }

    pub fn stats(&self) -> &ShadowCatalogStats {
        &self.stats
    }

    /// Rebuild the client from stashed `conninfo`. One-shot; retry via
    /// [`with_transient_retry`]. Resets `last_replay_lsn` since a restarted
    /// instance's replay LSN starts fresh.
    async fn reconnect(&mut self) -> Result<()> {
        let (client, conn) = tokio_postgres::connect(&self.conninfo, NoTls).await?;
        tokio::spawn(async move {
            let _ = conn.await;
        });
        self.client = client;
        self.stats.reconnects += 1;
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
                .query_one_retry("SELECT pg_last_wal_replay_lsn()", &[])
                .await?;
            let lsn = row.get::<_, Option<PgLsn>>(0).map(u64::from);
            if let Some(lsn) = lsn {
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

    /// Batched descriptor fetch: N oids plus the shadow's replay position.
    /// Oids absent from pg_class are absent from the result (dropped rels).
    /// Zero-column rels yield empty attribute vecs.
    pub async fn fetch_descriptors_batch(
        &mut self,
        oids: &[Oid],
    ) -> Result<(u64, Vec<RelDescriptor>)> {
        self.fetch_committed(Scope::Oids(oids)).await
    }

    /// Every eligible user relation: capture-all + descriptor-log boot seed.
    pub async fn fetch_all_descriptors(&mut self) -> Result<(u64, Vec<RelDescriptor>)> {
        self.fetch_committed(Scope::Eligible).await
    }

    /// Committed catalog at one replay position.
    async fn fetch_committed(&mut self, scope: Scope<'_>) -> Result<(u64, Vec<RelDescriptor>)> {
        self.stats.fetches += 1;
        let rows = self.committed_rows(scope).await?;
        let replay_lsn = rows.replay_lsn;
        let db_node = self.current_db_oid().await?;
        let default_tablespace = self.default_tablespace_oid().await?;
        Ok((replay_lsn, rows.assemble(db_node, default_tablespace)?))
    }

    /// Worker while it can answer for one replay position, the mirroring
    /// statement otherwise. Replay only sits still inside the publication
    /// hold; away from one it moves between requests, so no sequence of scans
    /// answers for a single position and the statement's one snapshot always
    /// does.
    async fn committed_rows(&mut self, scope: Scope<'_>) -> Result<DescriptorRows> {
        let bridge = self.bridge.clone();
        match self.scan_rows(&bridge, scope, 0, None).await {
            Err(e) if worker_cannot_answer(&e) => self.mirror_rows(scope).await,
            other => other,
        }
    }

    /// Descriptors as transaction `top_xid` sees them, read off shadow's pages
    /// at `boundary` — the LSN the caller parked replay at. Rows the
    /// transaction wrote and has not committed are included; rows it deleted
    /// are not.
    ///
    /// Oids absent from `pg_class` are absent from the result, as in
    /// [`Self::fetch_descriptors_batch`].
    pub async fn fetch_overlay_descriptors(
        &mut self,
        oids: &[Oid],
        top_xid: u32,
        boundary: u64,
    ) -> Result<Vec<RelDescriptor>> {
        let bridge = self.bridge.clone();
        self.stats.fetches += 1;
        let rows = self
            .scan_rows(&bridge, Scope::Oids(oids), top_xid, Some(boundary))
            .await?;
        let db_node = self.current_db_oid().await?;
        let default_tablespace = self.default_tablespace_oid().await?;
        rows.assemble(db_node, default_tablespace)
    }

    /// Projection rows off the worker. `boundary` is the LSN the caller parked
    /// replay at; `None` takes the first scan's position and pins the rest to
    /// it, so a replay move mid-read fails instead of tearing the descriptor.
    async fn scan_rows(
        &mut self,
        bridge: &Bridge,
        scope: Scope<'_>,
        top_xid: u32,
        boundary: Option<u64>,
    ) -> Result<DescriptorRows> {
        // The worker runs one scan per oid, where `= ANY($1)` on the SQL path
        // folds repeats
        let scoped = match scope {
            Scope::Oids(oids) => {
                let mut scoped = oids.to_vec();
                scoped.sort_unstable();
                scoped.dedup();
                scoped
            }
            Scope::Eligible => Vec::new(),
        };
        // An empty oid list is the whole catalog on the wire, never "no
        // relations", so the caller's empty list has to stop here
        if matches!(scope, Scope::Oids(_)) && scoped.is_empty() {
            return Ok(DescriptorRows {
                replay_lsn: match boundary {
                    Some(b) => b,
                    None => bridge.replay_lsn().await?,
                },
                ..Default::default()
            });
        }

        let mut pin = boundary;
        let mut class: Vec<ClassRow> = scan_pinned(bridge, top_xid, &scoped, &mut pin).await?;
        if matches!(scope, Scope::Eligible) {
            class.retain(eligible);
        }
        let pinned = pin.expect("the pg_class scan pins the position");
        if class.is_empty() {
            return Ok(DescriptorRows {
                replay_lsn: pinned,
                ..Default::default()
            });
        }
        // Whole-catalog pg_attribute is a seqscan of the biggest catalog there
        // is, and pg_class already named every relation worth scoping to
        let oids: Vec<Oid> = match scope {
            Scope::Oids(_) => scoped,
            Scope::Eligible => class.iter().map(|c| c.oid).collect(),
        };
        let attrs: Vec<AttributeRow> = scan_pinned(bridge, top_xid, &oids, &mut pin).await?;
        let indexes: Vec<IndexRow> = scan_pinned(bridge, top_xid, &oids, &mut pin).await?;

        let namespaces = self
            .resolve_names(
                bridge,
                "SELECT oid::oid, nspname::text FROM pg_namespace \
                 WHERE oid = ANY($1::oid[])",
                &class.iter().map(|c| c.relnamespace).collect(),
                |r: NamespaceRow| (r.oid, r.nspname),
                top_xid,
                pinned,
            )
            .await?;
        // DROP COLUMN zeroes atttypid, and no pg_type row for it is what leaves
        // the slot's type_name empty
        let types = self
            .resolve_names(
                bridge,
                "SELECT oid::oid, typname::text FROM pg_type \
                 WHERE oid = ANY($1::oid[])",
                &attrs
                    .iter()
                    .map(|a| a.atttypid)
                    .filter(|oid| *oid != 0)
                    .collect(),
                |r: TypeRow| (r.oid, r.typname),
                top_xid,
                pinned,
            )
            .await?;

        Ok(DescriptorRows {
            class,
            attrs,
            indexes,
            namespaces,
            types,
            replay_lsn: pinned,
        })
    }

    /// The same projections off one MVCC snapshot. Committed rows only, which
    /// is all the statement can see and all this path is asked for.
    async fn mirror_rows(&mut self, scope: Scope<'_>) -> Result<DescriptorRows> {
        self.stats.mirror_fetches += 1;
        let rows = match scope {
            Scope::Oids(oids) => self.query_retry(&MIRROR_BATCH_SQL, &[&oids]).await?,
            Scope::Eligible => self.query_retry(&MIRROR_ALL_SQL, &[]).await?,
        };
        DescriptorRows::from_mirror(&rows)
    }

    /// Oid → name for one whole-catalog projection, committed read first and
    /// the overlay only for what it missed.
    ///
    /// The overlay scan behind it has no oid list and so no lock argument, and
    /// refuses to answer while any foreign writer is mid-DDL. Running it only
    /// for what the committed read lacks keeps that exposure to names the
    /// requesting transaction created itself; a committed read never gets
    /// there at all.
    async fn resolve_names<R: ScanRow>(
        &mut self,
        bridge: &Bridge,
        sql: &str,
        wanted: &BTreeSet<Oid>,
        name_of: fn(R) -> (Oid, String),
        top_xid: u32,
        boundary: u64,
    ) -> Result<HashMap<Oid, String>> {
        if wanted.is_empty() {
            return Ok(HashMap::new());
        }
        let list: Vec<Oid> = wanted.iter().copied().collect();
        let rows = self.query_retry(sql, &[&list]).await?;
        let mut names: HashMap<Oid, String> = rows.iter().map(|r| (r.get(0), r.get(1))).collect();
        if names.len() == wanted.len() {
            return Ok(names);
        }
        let scan = bridge.scan_at(R::CATALOG, top_xid, &[], boundary).await?;
        for row in scan.parse::<R>()? {
            let (oid, name) = name_of(row);
            if wanted.contains(&oid) {
                names.insert(oid, name);
            }
        }
        Ok(names)
    }

    pub async fn current_database_oid(&mut self) -> Result<Oid> {
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

    /// What `pg_class.reltablespace = 0` means, and what WAL locators carry.
    /// Memoized alongside [`Self::current_db_oid`].
    async fn default_tablespace_oid(&mut self) -> Result<Oid> {
        if let Some(oid) = self.default_tablespace {
            return Ok(oid);
        }
        let row = self
            .query_one_retry(
                "SELECT dattablespace::oid FROM pg_database WHERE datname = current_database()",
                &[],
            )
            .await?;
        let oid = row.get(0);
        self.default_tablespace = Some(oid);
        Ok(oid)
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

/// Committed reads the worker cannot answer, so the statement does instead.
/// A worker that answered and said no is not here: that error is the caller's.
fn worker_cannot_answer(err: &CatalogError) -> bool {
    matches!(
        err,
        CatalogError::Bridge(
            BridgeError::ReplayMismatch { .. } | BridgeError::Io(_) | BridgeError::Protocol(_)
        )
    )
}

/// Not a catalog: the read's replay position, and the one row every mirror
/// read carries whether or not the projections matched anything.
const MIRROR_POSITION: i32 = 0;

/// `pg_last_wal_replay_lsn()` is null off a standby, where the worker reports
/// `0` for the same reason.
const NO_REPLAY: &str = "0/0";

/// The projections `pgext/overlay.c` emits, in the text output forms it uses,
/// as `(catalog id, text[])` rows so both sources reach the same [`ScanRow`]
/// parsers. One statement, so one snapshot covers every projection.
///
/// `format('%s', v)` renders through the type's own output function: a
/// `::text` cast says `true` where `boolout` says `t`, and would take
/// `int2vector` out of the space-separated form the worker sends. `attnum >=
/// 1` drops the system columns the descriptor never wants; `attmissingval`
/// carries `anyarray_out` form only when `atthasmissing`.
///
/// Position branch is first so its `pg_last_wal_replay_lsn()` is read as close
/// to snapshot acquisition as one statement allows.
fn mirror_sql(scope_pred: &str) -> String {
    format!(
        "WITH scoped AS MATERIALIZED (\
             SELECT c.* FROM pg_class c WHERE {scope_pred}\
         ), att AS MATERIALIZED (\
             SELECT a.* FROM pg_attribute a JOIN scoped s ON s.oid = a.attrelid \
             WHERE a.attnum >= 1\
         ) \
         SELECT {position}, ARRAY[coalesce(pg_last_wal_replay_lsn()::text, '{no_replay}')] \
         UNION ALL SELECT {class}, ARRAY[\
             format('%s', c.oid), format('%s', c.relnamespace), format('%s', c.relname), \
             format('%s', c.relkind), format('%s', c.relpersistence), \
             format('%s', c.relreplident), format('%s', c.reltoastrelid), \
             format('%s', c.reltablespace), format('%s', c.relfilenode)] \
           FROM scoped c \
         UNION ALL SELECT {attribute}, ARRAY[\
             format('%s', a.attrelid), format('%s', a.attnum), format('%s', a.attname), \
             format('%s', a.atttypid), format('%s', a.atttypmod), format('%s', a.attnotnull), \
             format('%s', a.attisdropped), format('%s', a.attbyval), format('%s', a.attlen), \
             format('%s', a.attalign), format('%s', a.attstorage), \
             CASE WHEN a.atthasmissing THEN a.attmissingval::text END] \
           FROM att a \
         UNION ALL SELECT {index}, ARRAY[\
             format('%s', i.indexrelid), format('%s', i.indrelid), \
             format('%s', i.indisprimary), format('%s', i.indisreplident), \
             format('%s', i.indkey)] \
           FROM pg_index i JOIN scoped s ON s.oid = i.indrelid \
         UNION ALL SELECT {namespace}, ARRAY[format('%s', n.oid), format('%s', n.nspname)] \
           FROM pg_namespace n WHERE n.oid IN (SELECT relnamespace FROM scoped) \
         UNION ALL SELECT {typ}, ARRAY[format('%s', t.oid), format('%s', t.typname)] \
           FROM pg_type t WHERE t.oid IN (SELECT atttypid FROM att)",
        position = MIRROR_POSITION,
        no_replay = NO_REPLAY,
        class = Catalog::Class as u8,
        attribute = Catalog::Attribute as u8,
        index = Catalog::Index as u8,
        namespace = Catalog::Namespace as u8,
        typ = Catalog::Type as u8,
    )
}

static MIRROR_BATCH_SQL: LazyLock<String> = LazyLock::new(|| mirror_sql("c.oid = ANY($1::oid[])"));

/// [`MIRROR_BATCH_SQL`] over every eligible user relation. Predicate is
/// [`eligible`] in SQL, which the scan path applies to whole-catalog rows
/// instead: scoping there is what keeps `pg_attribute` off every system rel.
static MIRROR_ALL_SQL: LazyLock<String> = LazyLock::new(|| {
    mirror_sql(&format!(
        "c.oid >= {FIRST_NORMAL_OBJECT_ID} AND c.relkind IN ('r', 'p', 'm', 't')"
    ))
});

/// Which relations one descriptor read covers.
#[derive(Clone, Copy)]
enum Scope<'a> {
    Oids(&'a [Oid]),
    /// Capture-all fallback + descriptor-log boot seed
    Eligible,
}

/// Relations decode can use: user oids, and the kinds that carry heap tuples
/// (heap, partitioned parent, matview, toast). Indexes, sequences and views
/// never decode.
fn eligible(class: &ClassRow) -> bool {
    class.oid >= FIRST_NORMAL_OBJECT_ID && matches!(class.relkind, 'r' | 'p' | 'm' | 't')
}

/// One catalog's projection rows off the worker. First scan of a read fixes
/// the replay position when the caller has none of its own; every later scan
/// asserts against it.
///
/// Chunked at [`MAX_SCAN_OIDS`]: a capture-all over a partition-heavy schema
/// names more relations than one request may carry, and every chunk shares the
/// one position anyway.
async fn scan_pinned<R: ScanRow>(
    bridge: &Bridge,
    top_xid: u32,
    oids: &[Oid],
    pin: &mut Option<u64>,
) -> Result<Vec<R>> {
    // An empty list is the whole catalog on the wire, which `chunks` would
    // skip asking for rather than ask once
    let chunks: Vec<&[Oid]> = if oids.is_empty() {
        vec![&[]]
    } else {
        oids.chunks(MAX_SCAN_OIDS).collect()
    };
    let mut out = Vec::new();
    for chunk in chunks {
        let res = match *pin {
            Some(boundary) => bridge.scan_at(R::CATALOG, top_xid, chunk, boundary).await?,
            None => bridge.scan_pinning(R::CATALOG, top_xid, chunk).await?,
        };
        *pin = Some(res.replay_lsn_end);
        out.extend(res.parse::<R>()?);
    }
    Ok(out)
}

/// One descriptor read's rows, laid out as `SCAN` projections in
/// `pgext/overlay.c`.
#[derive(Default)]
struct DescriptorRows {
    class: Vec<ClassRow>,
    attrs: Vec<AttributeRow>,
    indexes: Vec<IndexRow>,
    namespaces: HashMap<Oid, String>,
    types: HashMap<Oid, String>,
    replay_lsn: u64,
}

impl DescriptorRows {
    /// Rows off [`mirror_sql`], keyed by the catalog ids `SCAN` requests carry
    /// in their own header.
    fn from_mirror(rows: &[Row]) -> Result<Self> {
        let mut out = Self::default();
        let mut position = None;
        for row in rows {
            let id: i32 = row.get(0);
            let cols: Vec<Option<String>> = row.get(1);
            if id == MIRROR_POSITION {
                let text = cols.first().and_then(Option::as_deref).unwrap_or(NO_REPLAY);
                let lsn: PgLsn = text
                    .parse()
                    .map_err(|_| CatalogError::Parse(format!("mirror replay position {text:?}")))?;
                position = Some(u64::from(lsn));
                continue;
            }
            let catalog = u8::try_from(id)
                .ok()
                .and_then(Catalog::from_id)
                .ok_or_else(|| CatalogError::Parse(format!("mirror catalog id {id}")))?;
            match catalog {
                Catalog::Class => out.class.push(ClassRow::parse(&cols)?),
                Catalog::Attribute => out.attrs.push(AttributeRow::parse(&cols)?),
                Catalog::Index => out.indexes.push(IndexRow::parse(&cols)?),
                Catalog::Namespace => {
                    let row = NamespaceRow::parse(&cols)?;
                    out.namespaces.insert(row.oid, row.nspname);
                }
                Catalog::Type => {
                    let row = TypeRow::parse(&cols)?;
                    out.types.insert(row.oid, row.typname);
                }
            }
        }
        out.replay_lsn = position
            .ok_or_else(|| CatalogError::Parse("mirror read carried no position".into()))?;
        Ok(out)
    }

    fn assemble(self, db_node: Oid, default_tablespace: Oid) -> Result<Vec<RelDescriptor>> {
        let mut attrs_by_rel: HashMap<Oid, Vec<AttributeRow>> = HashMap::new();
        for attr in self.attrs {
            attrs_by_rel.entry(attr.attrelid).or_default().push(attr);
        }
        let mut indexes_by_rel: HashMap<Oid, Vec<IndexRow>> = HashMap::new();
        for index in self.indexes {
            indexes_by_rel
                .entry(index.indrelid)
                .or_default()
                .push(index);
        }
        let mut seen = BTreeSet::new();
        let mut out = Vec::with_capacity(self.class.len());
        for row in &self.class {
            // Two rows for one oid is the overlay's visibility predicate
            // failing to apply the transaction's own delete, ie a descriptor
            // built from a superseded pg_class version
            if !seen.insert(row.oid) {
                return Err(CatalogError::Parse(format!(
                    "two pg_class rows for oid {}",
                    row.oid
                )));
            }
            out.push(descriptor_from_rows(
                row,
                attrs_by_rel.get(&row.oid).map_or(&[][..], Vec::as_slice),
                indexes_by_rel.get(&row.oid).map_or(&[][..], Vec::as_slice),
                &self.namespaces,
                &self.types,
                db_node,
                default_tablespace,
            )?);
        }
        Ok(out)
    }
}

/// One descriptor out of projection rows already scoped to `class.oid`. The
/// single definition of the shape, so a committed read and an overlay read of
/// an unchanged relation are equal.
fn descriptor_from_rows(
    class: &ClassRow,
    attrs: &[AttributeRow],
    indexes: &[IndexRow],
    namespaces: &HashMap<Oid, String>,
    types: &HashMap<Oid, String>,
    db_node: Oid,
    default_tablespace: Oid,
) -> Result<RelDescriptor> {
    let namespace_name = namespaces.get(&class.relnamespace).ok_or_else(|| {
        CatalogError::Parse(format!(
            "no pg_namespace row for relnamespace {} of relation {}",
            class.relnamespace, class.oid
        ))
    })?;
    let replident = replident_from_parts(
        class.relreplident,
        class.oid,
        indexes
            .iter()
            .find(|i| i.indisprimary)
            .map(|i| i.indkey.clone()),
        indexes
            .iter()
            .find(|i| i.indisreplident)
            .map(|i| (i.indexrelid, i.indkey.clone())),
    )?;

    let mut ordered: Vec<&AttributeRow> = attrs.iter().collect();
    ordered.sort_unstable_by_key(|a| a.attnum);
    let mut attributes = Vec::with_capacity(ordered.len());
    for (i, attr) in ordered.iter().enumerate() {
        // Two rows for one attnum is an ALTER's superseded version surviving
        // the overlay's visibility predicate, which would shift every later
        // column
        if i > 0 && ordered[i - 1].attnum == attr.attnum {
            return Err(CatalogError::Parse(format!(
                "two pg_attribute rows for relation {} attnum {}",
                class.oid, attr.attnum
            )));
        }
        let raw = crate::pg::RawAttr {
            attnum: attr.attnum,
            name: attr.attname.clone(),
            type_oid: attr.atttypid,
            typmod: attr.atttypmod,
            not_null: attr.attnotnull,
            dropped: attr.attisdropped,
            type_name: types.get(&attr.atttypid).cloned(),
            type_byval: attr.attbyval,
            type_len: attr.attlen,
            type_align: attr.attalign.to_string(),
            type_storage: attr.attstorage.to_string(),
            missing: attr.attmissingval.clone(),
        };
        attributes.push(raw.build().map_err(CatalogError::Parse)?);
    }

    Ok(RelDescriptor {
        rfn: RelFileNode {
            // 0 is the database-default sentinel; WAL locators carry the
            // resolved tablespace
            spc_node: if class.reltablespace == 0 {
                default_tablespace
            } else {
                class.reltablespace
            },
            db_node,
            rel_node: class.relfilenode,
        },
        oid: class.oid,
        toast_oid: class.reltoastrelid,
        namespace_oid: class.relnamespace,
        rel_name: RelName::new(namespace_name, &class.relname),
        kind: class.relkind,
        persistence: class.relpersistence,
        replident,
        attributes,
    })
}

fn replident_from_parts(
    c: char,
    rel_oid: Oid,
    pk_attnums: Option<Vec<i16>>,
    using_index: Option<(Oid, Vec<i16>)>,
) -> Result<ReplIdent> {
    match c {
        'd' => Ok(ReplIdent::Default { pk_attnums }),
        'n' => Ok(ReplIdent::Nothing),
        'f' => Ok(ReplIdent::Full { pk_attnums }),
        'i' => {
            let (index_oid, key_attnums) = using_index.ok_or_else(|| {
                CatalogError::Parse(format!(
                    "relreplident='i' but no pg_index row with indisreplident=true for relation {rel_oid}",
                ))
            })?;
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

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
        assert!(c.reconnect_backoff_initial < c.reconnect_backoff_max);
    }

    #[test]
    fn eligible_takes_user_oids_of_heap_bearing_kinds() {
        let user = |kind| ClassRow {
            relkind: kind,
            ..class_row()
        };
        for kind in ['r', 'p', 'm', 't'] {
            assert!(eligible(&user(kind)), "{kind}");
        }
        for kind in ['i', 'S', 'v', 'c'] {
            assert!(!eligible(&user(kind)), "{kind}");
        }
        assert!(!eligible(&ClassRow {
            oid: FIRST_NORMAL_OBJECT_ID - 1,
            ..class_row()
        }));
    }

    /// pg_default, the `reltablespace = 0` sentinel's usual resolution
    const DEFAULT_TABLESPACE: Oid = 1663;

    fn class_row() -> ClassRow {
        ClassRow {
            oid: 16384,
            relnamespace: 2200,
            relname: "t".into(),
            relkind: 'r',
            relpersistence: 'p',
            relreplident: 'd',
            reltoastrelid: 16387,
            reltablespace: 0,
            relfilenode: 16384,
        }
    }

    fn attr_row(attnum: i16, name: &str, type_oid: Oid) -> AttributeRow {
        AttributeRow {
            attrelid: 16384,
            attnum,
            attname: name.into(),
            atttypid: type_oid,
            atttypmod: -1,
            attnotnull: false,
            attisdropped: type_oid == 0,
            attbyval: type_oid == 23,
            attlen: if type_oid == 23 { 4 } else { -1 },
            attalign: 'i',
            attstorage: if type_oid == 23 { 'p' } else { 'x' },
            attmissingval: None,
        }
    }

    fn names(pairs: &[(Oid, &str)]) -> HashMap<Oid, String> {
        pairs.iter().map(|(o, n)| (*o, (*n).to_owned())).collect()
    }

    fn overlay(
        class: &ClassRow,
        attrs: &[AttributeRow],
        indexes: &[IndexRow],
        namespaces: &HashMap<Oid, String>,
    ) -> Result<RelDescriptor> {
        descriptor_from_rows(
            class,
            attrs,
            indexes,
            namespaces,
            &names(&[(23, "int4"), (25, "text")]),
            5,
            DEFAULT_TABLESPACE,
        )
    }

    #[test]
    fn overlay_descriptor_resolves_sentinels_and_keys() {
        let attrs = [
            attr_row(2, "a", 25),
            attr_row(1, "id", 23),
            AttributeRow {
                attmissingval: Some("{7}".into()),
                ..attr_row(3, "c", 23)
            },
        ];
        let indexes = [IndexRow {
            indexrelid: 16390,
            indrelid: 16384,
            indisprimary: true,
            indisreplident: false,
            indkey: vec![1],
        }];
        let desc = overlay(&class_row(), &attrs, &indexes, &names(&[(2200, "public")])).unwrap();

        assert_eq!(desc.rfn.spc_node, DEFAULT_TABLESPACE);
        assert_eq!(desc.rfn.db_node, 5);
        assert_eq!(desc.rfn.rel_node, 16384);
        assert_eq!(desc.rel_name, RelName::new("public", "t"));
        assert_eq!(
            desc.replident,
            ReplIdent::Default {
                pk_attnums: Some(vec![1]),
            }
        );
        // Scan order is not the descriptor's order
        let cols: Vec<&str> = desc.attributes.iter().map(|a| a.name.as_str()).collect();
        assert_eq!(cols, ["id", "a", "c"]);
        assert_eq!(desc.attributes[1].type_name, "text");
        assert_eq!(desc.attributes[2].missing_text.as_deref(), Some("7"));
    }

    #[test]
    fn overlay_descriptor_keeps_dropped_slot_layout() {
        let attrs = [attr_row(1, "id", 23), attr_row(2, "gone", 0)];
        let desc = overlay(&class_row(), &attrs, &[], &names(&[(2200, "public")])).unwrap();
        let slot = &desc.attributes[1];
        assert!(slot.dropped);
        assert_eq!(slot.type_name, "", "no pg_type row for a zeroed atttypid");
        assert_eq!(slot.type_len, -1);
        assert_eq!(slot.type_storage, 'x');
    }

    #[test]
    fn overlay_descriptor_picks_replident_index() {
        let class = ClassRow {
            relreplident: 'i',
            ..class_row()
        };
        let indexes = [
            IndexRow {
                indexrelid: 16390,
                indrelid: 16384,
                indisprimary: true,
                indisreplident: false,
                indkey: vec![1],
            },
            IndexRow {
                indexrelid: 16392,
                indrelid: 16384,
                indisprimary: false,
                indisreplident: true,
                indkey: vec![2, 3],
            },
        ];
        let desc = overlay(
            &class,
            &[attr_row(1, "id", 23)],
            &indexes,
            &names(&[(2200, "public")]),
        )
        .unwrap();
        assert_eq!(
            desc.replident,
            ReplIdent::UsingIndex {
                index_oid: 16392,
                key_attnums: vec![2, 3],
            }
        );
    }

    #[test]
    fn overlay_descriptor_rejects_superseded_attribute() {
        let attrs = [attr_row(1, "id", 23), attr_row(1, "id", 25)];
        let err = overlay(&class_row(), &attrs, &[], &names(&[(2200, "public")])).unwrap_err();
        assert!(
            matches!(&err, CatalogError::Parse(m) if m.contains("attnum 1")),
            "{err}"
        );
    }

    #[test]
    fn overlay_descriptor_needs_its_namespace_name() {
        let err = overlay(&class_row(), &[], &[], &HashMap::new()).unwrap_err();
        assert!(
            matches!(&err, CatalogError::Parse(m) if m.contains("pg_namespace")),
            "{err}"
        );
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
