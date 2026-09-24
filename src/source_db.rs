//! Databases replicated by one daemon
//!
//! Read cluster WAL once, keep catalog connections, bridge workers, table
//! metadata history, and mapping rules for each database
//! Select database from `RelFileNode::db_node` or commit's `xl_xact_dbinfo`
//!
//! Assign each database a consecutive group of bridge sockets in config order
//! Check database OID returned by `HELLO` to reject connections to wrong database

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use tokio::sync::{Mutex, watch};
use tokio_postgres::types::Oid;

use crate::catalog::desc_log::DescriptorLog;
use crate::catalog::shadow_catalog::{ShadowCatalog, ShadowCatalogConfig, with_transient_retry};
use crate::config::{ConfigResolver, ResolvedConfig};
use crate::emit::ch_emitter::EmitterConfig;
use crate::emit::route::RowPolicy;
use crate::mapping::MappingHandle;
use crate::ops::bridge::Bridge;
use ahash::{HashMap, HashMapExt};

/// Bridge pool and shadow catalog for one source database, opened before the
/// descriptor log and routing map that complete a [`SourceDb`]
pub struct DbLink {
    pub name: String,
    pub oid: Oid,
    pub bridge: Arc<Bridge>,
    pub catalog: Arc<Mutex<ShadowCatalog>>,
    /// Mapping rules scoped to this database, `None` in metrics-only runs
    pub emitter: Option<EmitterConfig>,
}

pub struct DbLinkConfig<'a> {
    pub name: &'a str,
    /// Position in the configured database list, ie which bridge sockets
    pub index: usize,
    pub workers: usize,
    pub bridge_path: &'a Path,
    pub shadow_conninfo: &'a str,
    /// Wall-clock allowance while shadow reaches consistency
    pub budget: Duration,
    pub emitter: Option<EmitterConfig>,
}

impl DbLink {
    /// Dial one database's bridge workers and shadow catalog. `HELLO` reports
    /// the worker's own database, so a socket serving another one fails here
    /// rather than answering catalog reads from the wrong database
    pub async fn connect(cfg: DbLinkConfig<'_>) -> anyhow::Result<Self> {
        let bridge = Arc::new(
            crate::ops::bridge::connect_database_with_budget(
                cfg.bridge_path,
                cfg.index,
                cfg.workers,
                cfg.budget,
            )
            .await
            .with_context(|| {
                format!(
                    "connect bridge for database {} at {}",
                    cfg.name,
                    cfg.bridge_path.display()
                )
            })?,
        );
        let info = bridge.info();
        tracing::info!(
            target: "walshadow::bridge",
            socket = %bridge.path().display(),
            dbname = %cfg.name,
            workers = bridge.pool_size(),
            pg_version = info.map(|i| i.pg_version_num).unwrap_or(0),
            in_recovery = info.map(|i| i.in_recovery).unwrap_or(false),
            "bridge connected",
        );
        let cat_cfg = ShadowCatalogConfig::default();
        let mut catalog = with_transient_retry(
            cfg.budget,
            cat_cfg.reconnect_backoff_initial,
            cat_cfg.reconnect_backoff_max,
            async || {
                ShadowCatalog::connect(cfg.shadow_conninfo, cat_cfg.clone(), bridge.clone()).await
            },
        )
        .await
        .with_context(|| format!("connect to shadow PG database {}", cfg.name))?;
        let oid = catalog
            .current_database_oid()
            .await
            .with_context(|| format!("shadow database oid for {}", cfg.name))?;
        if let Some(hello) = info {
            anyhow::ensure!(
                hello.datid == oid,
                "bridge socket {} serves database oid {}, but {} is oid {oid}; \
                 shadow's walshadow.databases disagrees with this config",
                bridge.path().display(),
                hello.datid,
                cfg.name,
            );
        }
        tracing::info!(
            target: "walshadow",
            conninfo = %cfg.shadow_conninfo,
            db_oid = oid,
            "shadow connected",
        );
        Ok(Self {
            name: cfg.name.to_owned(),
            oid,
            bridge,
            catalog: Arc::new(Mutex::new(catalog)),
            emitter: cfg.emitter,
        })
    }
}

pub struct SourceDb {
    pub name: String,
    pub oid: Oid,
    pub bridge: Arc<Bridge>,
    pub catalog: Arc<Mutex<ShadowCatalog>>,
    pub desc_log: Arc<DescriptorLog>,
    /// Mapping rules for this database, other settings are shared across databases
    pub emitter: Arc<EmitterConfig>,
    pub dest: Arc<crate::config::DestEmitter>,
    pub mapping: MappingHandle,
    /// Without `--ch-config`, live config updates are disabled
    pub resolver: Option<Arc<ConfigResolver>>,
    pub config_rx: Option<watch::Receiver<Arc<ResolvedConfig>>>,
}

impl SourceDb {
    /// Complete a connected database with the state its pipeline routes through
    pub fn new(
        link: &DbLink,
        desc_log: Arc<DescriptorLog>,
        emitter: EmitterConfig,
        mapping: MappingHandle,
        config: Option<(Arc<ConfigResolver>, watch::Receiver<Arc<ResolvedConfig>>)>,
    ) -> Self {
        let (resolver, config_rx) = config.map_or((None, None), |(r, rx)| (Some(r), Some(rx)));
        let emitter = Arc::new(emitter);
        Self {
            name: link.name.clone(),
            oid: link.oid,
            bridge: link.bridge.clone(),
            catalog: link.catalog.clone(),
            desc_log,
            dest: crate::config::DestEmitter::new(emitter.clone(), config_rx.clone()),
            emitter,
            mapping,
            resolver,
            config_rx,
        }
    }

    /// Row format settings fixed at startup
    pub fn row_policy(&self) -> RowPolicy {
        self.emitter.row_policy()
    }
}

impl std::fmt::Debug for SourceDb {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SourceDb")
            .field("name", &self.name)
            .field("oid", &self.oid)
            .finish_non_exhaustive()
    }
}

/// Look up configured databases by OID from WAL records
#[derive(Debug)]
pub struct SourceDbs {
    /// Config order, which is also bridge socket order
    order: Vec<Arc<SourceDb>>,
    by_oid: HashMap<Oid, Arc<SourceDb>>,
    /// `[source] dbname`: probe connections, shared settings, and the one
    /// database a single-database path uses
    primary: Arc<SourceDb>,
}

impl SourceDbs {
    /// Use config order, fall back to first database if primary OID is missing
    pub fn new(dbs: Vec<Arc<SourceDb>>, primary: Oid) -> Self {
        let mut by_oid = HashMap::with_capacity(dbs.len());
        for db in &dbs {
            by_oid.insert(db.oid, db.clone());
        }
        let primary = by_oid
            .get(&primary)
            .cloned()
            .or_else(|| dbs.first().cloned())
            .expect("a daemon follows at least one database");
        Self {
            order: dbs,
            by_oid,
            primary,
        }
    }

    pub fn single(db: Arc<SourceDb>) -> Self {
        let primary = db.oid;
        Self::new(vec![db], primary)
    }

    /// Return `None` for databases this daemon does not replicate
    pub fn get(&self, oid: Oid) -> Option<&Arc<SourceDb>> {
        self.by_oid.get(&oid)
    }

    pub fn primary(&self) -> &Arc<SourceDb> {
        &self.primary
    }

    pub fn all(&self) -> &[Arc<SourceDb>] {
        &self.order
    }

    pub fn len(&self) -> usize {
        self.order.len()
    }

    pub fn is_empty(&self) -> bool {
        self.order.is_empty()
    }

    pub fn oids(&self) -> impl Iterator<Item = Oid> + '_ {
        self.order.iter().map(|db| db.oid)
    }

    /// Select affected databases, zero means all configured databases
    /// PostgreSQL uses `dbId == 0` to invalidate relation caches in every database
    pub fn scoped(&self, db_oid: Oid) -> &[Arc<SourceDb>] {
        if let Some(db) = self.by_oid.get(&db_oid) {
            std::slice::from_ref(db)
        } else if db_oid == 0 {
            &self.order
        } else {
            // Ignore databases this daemon does not replicate
            &[]
        }
    }
}
