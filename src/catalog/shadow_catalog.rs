//! Shadow PG SQL client for descriptor capture + name-keyed resolution.
//!
//! Decode never reads this: interval-scoped answers come from the durable
//! [`DescriptorLog`](crate::catalog::desc_log::DescriptorLog), which capture
//! populates from here at catalog boundaries (batched
//! [`ShadowCatalog::fetch_descriptors_batch`] /
//! [`ShadowCatalog::fetch_all_descriptors`] round trips). Name-keyed reads
//! ([`ShadowCatalog::descriptor_by_name`], toast resolution) serve opt-in
//! dispatch, backfill standup, and preflight.
//!
//! Single-database model: instance bound to one DB. Shared catalogs
//! (`db_node == 0`) resolve from any connection via `pg_relation_filenode`.

use std::sync::Arc;
use std::time::{Duration, Instant};

use backon::{ExponentialBuilder, RetryableWithContext};
use thiserror::Error;
use tokio_postgres::types::{Oid, PgLsn, ToSql};
use tokio_postgres::{Client, NoTls, Row};
use walrus::pg::walparser::RelFileNode;

#[cfg(test)]
use crate::pg::socket_conninfo;
use crate::schema::{RelAttr, RelDescriptor, RelName, ReplIdent};

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
            last_replay_lsn: None,
            current_db_oid: None,
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
        Ok(self.fetch_by_oid(oid).await?.map(Arc::new))
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
        Ok(self.fetch_by_oid(toast_oid).await?.map(Arc::new))
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
                    c.reltoastrelid::oid, \
                    coalesce(nullif(c.reltablespace, 0), \
                             (SELECT dattablespace FROM pg_database \
                              WHERE datname = current_database()))::oid, \
                    coalesce(pg_relation_filenode(c.oid), 0)::oid \
                 FROM pg_class c \
                 JOIN pg_namespace n ON n.oid = c.relnamespace \
                 WHERE c.oid = $1",
                &[&oid],
            )
            .await?;
        let Some(row) = row else { return Ok(None) };
        let spc_node: Oid = row.get(8);
        let rel_node: Oid = row.get(9);
        let db_node = self.current_database_oid().await?;
        let rfn = RelFileNode {
            spc_node,
            db_node,
            rel_node,
        };
        Ok(Some(self.descriptor_from_row(&row, rfn).await?))
    }

    /// Build from a pg_class⋈pg_namespace row whose first 8 columns are
    /// (oid, relnamespace, nspname, relname, relkind, relpersistence,
    /// relreplident, reltoastrelid), paired with a resolved `rfn`.
    async fn descriptor_from_row(&mut self, row: &Row, rfn: RelFileNode) -> Result<RelDescriptor> {
        let oid: Oid = row.get(0);
        let namespace_oid: Oid = row.get(1);
        let namespace_name: String = row.get(2);
        let name: String = row.get(3);
        let kind = one_char(row.get::<_, String>(4), "relkind")?;
        let persistence = one_char(row.get::<_, String>(5), "relpersistence")?;
        let replident_char = one_char(row.get::<_, String>(6), "relreplident")?;
        let toast_oid: Oid = row.get(7);
        let replident = self.fetch_replident(replident_char, oid).await?;
        let attributes = self.fetch_attributes(oid).await?;
        Ok(RelDescriptor {
            rfn,
            oid,
            toast_oid,
            namespace_oid,
            rel_name: RelName::new(&namespace_name, &name),
            kind,
            persistence,
            replident,
            attributes,
        })
    }

    /// Batched descriptor fetch: one round trip for N oids plus the shadow's
    /// replay position off the same connection. Oids absent from pg_class are
    /// absent from the result (dropped rels). Zero-column rels yield empty
    /// attribute vecs.
    pub async fn fetch_descriptors_batch(
        &mut self,
        oids: &[Oid],
    ) -> Result<(u64, Vec<RelDescriptor>)> {
        let oids: Vec<Oid> = oids.to_vec();
        self.fetch_descriptor_rows(DESCRIPTOR_BATCH_SQL, &[&oids])
            .await
    }

    /// Every eligible user relation: capture-all + descriptor-log boot seed.
    pub async fn fetch_all_descriptors(&mut self) -> Result<(u64, Vec<RelDescriptor>)> {
        self.fetch_descriptor_rows(&DESCRIPTOR_ALL_SQL, &[]).await
    }

    async fn fetch_descriptor_rows(
        &mut self,
        sql: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<(u64, Vec<RelDescriptor>)> {
        let db_node = self.current_db_oid().await?;
        let rows = self.query_retry(sql, params).await?;
        let mut replay_lsn = 0u64;
        let mut out = Vec::with_capacity(rows.len());
        for row in &rows {
            replay_lsn = row.get::<_, Option<PgLsn>>(25).map(u64::from).unwrap_or(0);
            out.push(descriptor_from_batch_row(row, db_node)?);
        }
        if out.is_empty() {
            let row = self
                .query_one_retry("SELECT pg_last_wal_replay_lsn()", &[])
                .await?;
            replay_lsn = row.get::<_, Option<PgLsn>>(0).map(u64::from).unwrap_or(0);
        }
        Ok((replay_lsn, out))
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
            'f' => {
                // FULL logs all columns but names no key index; still capture
                // the PK so the CH ORDER BY uses it instead of `_lsn`.
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
                Ok(ReplIdent::Full { pk_attnums })
            }
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
        let rows = self.query_retry(crate::pg::ATTR_SQL, &[&rel_oid]).await?;
        rows.iter()
            .map(|row| {
                crate::pg::RawAttr::from_row(row)
                    .build()
                    .map_err(CatalogError::Parse)
            })
            .collect()
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

/// One row per live oid. Columns:
/// 0-7 pg_class scalars as in [`ShadowCatalog::descriptor_from_row`]
/// (oid, relnamespace, nspname, relname, relkind, relpersistence,
/// relreplident, reltoastrelid); 8 physical tablespace (reltablespace with
/// the 0 = database-default sentinel resolved to `dattablespace`, matching
/// WAL locators' spcOid);
/// 9 filenode (0 = no storage); 10 pk indkey; 11-12 replident index
/// (indexrelid, indkey); 13-24 pg_attribute arrays parallel by attnum,
/// physical cols direct + LEFT JOIN pg_type per [`crate::pg::ATTR_SQL`];
/// 25 `pg_last_wal_replay_lsn()`.
const DESCRIPTOR_BATCH_SQL: &str = "SELECT \
        c.oid::oid, \
        c.relnamespace::oid, \
        n.nspname::text, \
        c.relname::text, \
        c.relkind::text, \
        c.relpersistence::text, \
        c.relreplident::text, \
        c.reltoastrelid::oid, \
        coalesce(nullif(c.reltablespace, 0), \
                 (SELECT dattablespace FROM pg_database \
                  WHERE datname = current_database()))::oid, \
        coalesce(pg_relation_filenode(c.oid), 0)::oid, \
        pk.attnums, \
        ri.index_oid, \
        ri.attnums, \
        att.attnums, att.names, att.type_oids, att.typmods, att.not_nulls, \
        att.droppeds, att.type_names, att.byvals, att.lens, att.aligns, \
        att.storages, att.missings, \
        pg_last_wal_replay_lsn() \
     FROM pg_class c \
     JOIN pg_namespace n ON n.oid = c.relnamespace \
     LEFT JOIN LATERAL ( \
        SELECT indkey::int2[] AS attnums FROM pg_index \
        WHERE indrelid = c.oid AND indisprimary LIMIT 1) pk ON true \
     LEFT JOIN LATERAL ( \
        SELECT indexrelid::oid AS index_oid, indkey::int2[] AS attnums \
        FROM pg_index \
        WHERE indrelid = c.oid AND indisreplident LIMIT 1) ri ON true \
     LEFT JOIN LATERAL ( \
        SELECT \
            array_agg(a.attnum ORDER BY a.attnum) AS attnums, \
            array_agg(a.attname::text ORDER BY a.attnum) AS names, \
            array_agg(a.atttypid ORDER BY a.attnum) AS type_oids, \
            array_agg(a.atttypmod ORDER BY a.attnum) AS typmods, \
            array_agg(a.attnotnull ORDER BY a.attnum) AS not_nulls, \
            array_agg(a.attisdropped ORDER BY a.attnum) AS droppeds, \
            array_agg(t.typname::text ORDER BY a.attnum) AS type_names, \
            array_agg(a.attbyval ORDER BY a.attnum) AS byvals, \
            array_agg(a.attlen ORDER BY a.attnum) AS lens, \
            array_agg(a.attalign::text ORDER BY a.attnum) AS aligns, \
            array_agg(a.attstorage::text ORDER BY a.attnum) AS storages, \
            array_agg(CASE WHEN a.atthasmissing THEN a.attmissingval::text END \
                      ORDER BY a.attnum) AS missings \
        FROM pg_attribute a \
        LEFT JOIN pg_type t ON t.oid = a.atttypid \
        WHERE a.attrelid = c.oid AND a.attnum >= 1) att ON true \
     WHERE c.oid = ANY($1::oid[])";

/// [`DESCRIPTOR_BATCH_SQL`] over every eligible user relation instead of an
/// oid list: capture-all fallback + descriptor-log boot seed. Kinds match
/// the decodable set (heap 'r', partitioned parent 'p', matview 'm', toast
/// 't'); indexes/sequences/views never decode.
static DESCRIPTOR_ALL_SQL: std::sync::LazyLock<String> = std::sync::LazyLock::new(|| {
    let base = DESCRIPTOR_BATCH_SQL
        .strip_suffix("WHERE c.oid = ANY($1::oid[])")
        .expect("batch SQL suffix");
    format!("{base}WHERE c.oid >= 16384 AND c.relkind IN ('r', 'p', 'm', 't')")
});

/// See [`DESCRIPTOR_BATCH_SQL`] for the column plan.
fn descriptor_from_batch_row(row: &Row, db_node: Oid) -> Result<RelDescriptor> {
    let oid: Oid = row.get(0);
    let namespace_oid: Oid = row.get(1);
    let namespace_name: String = row.get(2);
    let name: String = row.get(3);
    let kind = one_char(row.get::<_, String>(4), "relkind")?;
    let persistence = one_char(row.get::<_, String>(5), "relpersistence")?;
    let replident_char = one_char(row.get::<_, String>(6), "relreplident")?;
    let toast_oid: Oid = row.get(7);
    let spc_node: Oid = row.get(8);
    let rel_node: Oid = row.get(9);
    let pk_attnums: Option<Vec<i16>> = row.get(10);
    let ri_index_oid: Option<Oid> = row.get(11);
    let ri_attnums: Option<Vec<i16>> = row.get(12);
    let replident = replident_from_parts(
        replident_char,
        oid,
        pk_attnums,
        ri_index_oid.zip(ri_attnums),
    )?;
    let attributes = attrs_from_arrays(row)?;
    Ok(RelDescriptor {
        rfn: RelFileNode {
            spc_node,
            db_node,
            rel_node,
        },
        oid,
        toast_oid,
        namespace_oid,
        rel_name: RelName::new(&namespace_name, &name),
        kind,
        persistence,
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

/// Zip [`DESCRIPTOR_BATCH_SQL`] columns 13-24 into attrs.
fn attrs_from_arrays(row: &Row) -> Result<Vec<RelAttr>> {
    let Some(attnums) = row.get::<_, Option<Vec<i16>>>(13) else {
        return Ok(Vec::new());
    };
    let names: Vec<String> = row.get(14);
    let type_oids: Vec<Oid> = row.get(15);
    let typmods: Vec<i32> = row.get(16);
    let not_nulls: Vec<bool> = row.get(17);
    let droppeds: Vec<bool> = row.get(18);
    let type_names: Vec<Option<String>> = row.get(19);
    let byvals: Vec<bool> = row.get(20);
    let lens: Vec<i16> = row.get(21);
    let aligns: Vec<String> = row.get(22);
    let storages: Vec<String> = row.get(23);
    let missings: Vec<Option<String>> = row.get(24);
    let n = attnums.len();
    let lens_match = [
        names.len(),
        type_oids.len(),
        typmods.len(),
        not_nulls.len(),
        droppeds.len(),
        type_names.len(),
        byvals.len(),
        lens.len(),
        aligns.len(),
        storages.len(),
        missings.len(),
    ]
    .iter()
    .all(|&l| l == n);
    if !lens_match {
        return Err(CatalogError::Parse(
            "descriptor batch: attribute array length mismatch".into(),
        ));
    }
    let mut out = Vec::with_capacity(n);
    for i in 0..n {
        let raw = crate::pg::RawAttr {
            attnum: attnums[i],
            name: names[i].clone(),
            type_oid: type_oids[i],
            typmod: typmods[i],
            not_null: not_nulls[i],
            dropped: droppeds[i],
            type_name: type_names[i].clone(),
            type_byval: byvals[i],
            type_len: lens[i],
            type_align: aligns[i].clone(),
            type_storage: storages[i].clone(),
            missing: missings[i].clone(),
        };
        out.push(raw.build().map_err(CatalogError::Parse)?);
    }
    Ok(out)
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
