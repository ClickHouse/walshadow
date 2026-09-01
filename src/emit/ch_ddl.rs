//! CH-side DDL applicator. Translates each [`SchemaEvent`] into CH SQL:
//!
//! | event | CH SQL |
//! |---|---|
//! | `Added` | `CREATE TABLE IF NOT EXISTS …` (namespace `auto_create = true`; a mapped rel re-creates its dest when strategy = drop) |
//! | `Changed.added_columns` | `ALTER TABLE … ADD COLUMN IF NOT EXISTS …` per column in attnum order |
//! | `Changed.renamed_columns` | `ALTER TABLE … RENAME COLUMN IF EXISTS … TO …` first |
//! | `Changed.dropped_columns` | `ALTER TABLE … DROP COLUMN IF EXISTS …` |
//! | `Changed.type_changes` | rejected — logged, not applied (open question) |
//! | `Dropped` | `DROP TABLE IF EXISTS …` gated on [`DropTableStrategy`] |
//!
//! Opens its own `BoxedAsyncClient` (separate from the INSERT pump) so DDL
//! doesn't ride the INSERT backpressure path.
//!
//! ## Coordination with the INSERT pump
//!
//! The reorder coordinator ([`crate::emit::pipeline::reorder`]) drives DDL
//! ordering: within a barrier xact it dispatches pending data, fences
//! (seals the batcher, waits until every earlier row is durable on CH),
//! then applies the schema change, then resumes. Post-DDL rows encode
//! against the new shape.

use std::sync::Arc;
use std::time::Duration;

use clickhouse_c::BoxedAsyncClient;
use tokio::sync::watch;

use crate::catalog::type_bridge::{self, ResolvedColumn};
use crate::ch::{
    EmitterError, backoff_step, connect_client, exec_drain, is_retryable, quote_ident,
    reconnect_if_idle,
};
use crate::column_rules::ColumnRules;
use crate::config::{ConfigResolver, ResolvedConfig};
use crate::emit::ch_emitter::{EmitterConfig, RetryConfig};
use crate::mapping::{
    ColumnMapping, DropTableStrategy, MappingHandle, MappingSnapshot, NamespaceMapping,
    SystemColumns, TableMapping, TableTarget, apply_column_rule, derive_columns_for_mapping,
    fold_diff_into_mapping,
};
use crate::schema::{RelDescriptor, RelName, SchemaDiff, SchemaEvent, replident_key_attnums};
use crate::table_rules::{TableRule, TableRules};
use ahash::{HashMap, HashSet, HashSetExt};

/// Knobs that don't ride the INSERT pump. [`DdlApplicator`] rebuilds them
/// from a republished [`ResolvedConfig`] snapshot at each apply, so SIGHUP
/// (and the future overlay) retarget namespaces + drop strategy without a
/// restart.
#[derive(Debug, Clone)]
pub struct DdlConfig {
    pub drop_table_strategy: DropTableStrategy,
    /// Namespaces whose `Added` events run `CREATE TABLE IF NOT EXISTS`
    /// automatically (`auto_create = true`)
    pub auto_create_namespaces: HashSet<String>,
    pub replicate_all: bool,
    /// Excluded from `replicate_all` so the overlay's `config_*` tables aren't swept in
    pub runtime_config_schema: Option<String>,
    /// CH database DDL targets when neither per-table mapping nor source
    /// namespace overrides the destination
    pub target_database: String,
    /// Per-namespace overrides, fallback to the global fields above when
    /// a namespace has none
    pub namespaces: HashMap<String, NamespaceMapping>,
    /// Keep the delete marker out of `ReplacingMergeTree`'s args so deletes
    /// stay queryable; mirrors [`EmitterConfig::soft_delete`]
    pub soft_delete: bool,
    /// Cluster-wide names of the appended columns, and whether the delete
    /// marker exists; mirrors [`EmitterConfig::system_columns`]. A per-relation
    /// rule renames over it
    pub system: Arc<SystemColumns>,
    pub rules: Arc<TableRules>,
    pub column_rules: Arc<ColumnRules>,
}

impl DdlConfig {
    /// Build from a resolved snapshot. `target_database`, `soft_delete`,
    /// `system`, `replicate_all` and `runtime_config_schema` are boot-only
    /// knobs the resolver does not republish, so callers thread them through
    /// unchanged.
    pub fn from_resolved(
        resolved: &ResolvedConfig,
        target_database: String,
        soft_delete: bool,
        system: Arc<SystemColumns>,
        replicate_all: bool,
        runtime_config_schema: Option<String>,
    ) -> Self {
        let auto_create_namespaces: HashSet<String> = resolved
            .namespaces
            .iter()
            .filter(|(_, v)| v.auto_create)
            .map(|(k, _)| k.clone())
            .collect();
        Self {
            drop_table_strategy: resolved.drop_table_strategy,
            auto_create_namespaces,
            replicate_all,
            runtime_config_schema,
            target_database,
            namespaces: resolved.namespaces.clone(),
            soft_delete,
            system,
            rules: resolved.rules.clone(),
            column_rules: resolved.column_rules.clone(),
        }
    }

    fn target_database_for(&self, namespace: &str) -> &str {
        self.namespaces
            .get(namespace)
            .and_then(|n| n.target_database.as_deref())
            .unwrap_or(&self.target_database)
    }

    fn drop_strategy_for(&self, namespace: &str) -> DropTableStrategy {
        self.namespaces
            .get(namespace)
            .and_then(|n| n.drop_table_strategy)
            .unwrap_or(self.drop_table_strategy)
    }

    pub fn with_drop_strategy(mut self, s: DropTableStrategy) -> Self {
        self.drop_table_strategy = s;
        self
    }

    fn create_target(&self, settings: &TableRule, rel: &RelName) -> TableTarget {
        TableTarget {
            database: settings
                .target_database
                .clone()
                .unwrap_or_else(|| self.target_database_for(&rel.namespace).to_owned()),
            table: settings
                .target_table
                .clone()
                .unwrap_or_else(|| rel.name.to_string()),
        }
    }

    /// Destination shape for one relation's `CREATE TABLE`
    pub(crate) fn create_shape<'a>(&'a self, settings: &'a TableRule) -> CreateShape<'a> {
        CreateShape {
            system: settings.system_columns(&self.system),
            soft_delete: self.soft_delete,
            order_by: settings.order_by.as_deref().unwrap_or_default(),
            primary_key: settings.primary_key.as_deref().unwrap_or_default(),
        }
    }

    /// Resolve explicit scope without opting in system relations
    pub fn declared_scope(&self, rel: &RelName) -> Option<bool> {
        match self.rules.settings(rel).replicate {
            Some(true)
                if is_system_namespace(&rel.namespace, self.runtime_config_schema.as_deref()) =>
            {
                None
            }
            other => other,
        }
    }

    fn auto_creates(&self, rel: &RelName) -> bool {
        if let Some(replicate) = self.declared_scope(rel) {
            return replicate;
        }
        self.auto_create_namespaces.contains(&*rel.namespace)
            || (self.replicate_all
                && !is_system_namespace(&rel.namespace, self.runtime_config_schema.as_deref()))
    }
}

/// Destination-shape inputs for one `CREATE TABLE`
pub struct CreateShape<'a> {
    pub system: Arc<SystemColumns>,
    /// Keep the delete marker out of `ReplacingMergeTree`'s args
    pub soft_delete: bool,
    /// Operator `ORDER BY` (destination column names); empty derives the key
    /// from replica identity
    pub order_by: &'a [String],
    /// Operator `PRIMARY KEY`: the sparse-index prefix CH indexes, not a
    /// uniqueness constraint. Empty leaves it equal to `ORDER BY`
    pub primary_key: &'a [String],
}

pub(crate) fn is_system_namespace(ns: &str, runtime_config_schema: Option<&str>) -> bool {
    ns == "pg_catalog"
        || ns == "information_schema"
        || ns == "pg_toast"
        || ns.starts_with("pg_")
        || runtime_config_schema.is_some_and(|s| !s.is_empty() && s == ns)
}

/// CH-side DDL writer. Owns one BoxedAsyncClient over its own TCP.
pub struct DdlApplicator {
    client: BoxedAsyncClient,
    config: DdlConfig,
    /// Live config layers. `refresh_config` folds a republished snapshot
    /// into `config` (namespaces + drop strategy) at each apply, so SIGHUP
    /// and the future overlay retarget DDL without a restart.
    config_rx: watch::Receiver<Arc<ResolvedConfig>>,
    mapping: MappingHandle,
    /// Reconnect params; `refresh_config` updates the connection fields live
    /// from a republished snapshot and re-dials on change.
    conn_cfg: EmitterConfig,
    retry: RetryConfig,
    /// Per-attempt cap (shares `EmitterConfig::insert_timeout`); a
    /// half-open CH socket can't park the reorder barrier past this
    query_timeout: Duration,
    last_used: std::time::Instant,
    /// Owner of runtime-derived mapping state. Set: auto-created mappings,
    /// diff folds, and DROP forgets record into the resolver so the
    /// republish full-swap preserves them. Unset (bootstrap drain, tests
    /// without a resolver): mutate the live handle directly — no republish
    /// runs in those contexts, so nothing clobbers the write.
    resolver: Option<Arc<ConfigResolver>>,
    ensured_databases: HashSet<String>,
    pub stats: DdlStats,
}

#[derive(Debug, Default, Clone)]
pub struct DdlStats {
    pub alters_applied: u64,
    pub creates_applied: u64,
    pub drops_applied: u64,
    /// Events received but skipped (no mapping, no auto_create, type
    /// change rejected, drop strategy = Retain)
    pub skipped: u64,
    pub type_changes_rejected: u64,
}

impl DdlApplicator {
    pub async fn new(
        emitter_cfg: &EmitterConfig,
        ddl_cfg: DdlConfig,
        mapping: MappingHandle,
        config_rx: watch::Receiver<Arc<ResolvedConfig>>,
    ) -> Result<Self, EmitterError> {
        let client = connect_client(emitter_cfg).await?;
        Ok(Self {
            client,
            config: ddl_cfg,
            config_rx,
            mapping,
            conn_cfg: emitter_cfg.clone(),
            retry: emitter_cfg.retry.clone(),
            query_timeout: emitter_cfg.insert_timeout,
            last_used: std::time::Instant::now(),
            resolver: None,
            ensured_databases: HashSet::new(),
            stats: DdlStats::default(),
        })
    }

    /// Route mapping writes through resolver so republished snapshots retain them
    pub fn with_resolver(mut self, resolver: Arc<ConfigResolver>) -> Self {
        self.resolver = Some(resolver);
        self
    }

    pub fn config(&self) -> &DdlConfig {
        &self.config
    }

    /// Fold a republished snapshot into `config` (namespaces, drop strategy,
    /// table + column rules). `target_database`, `soft_delete` and the
    /// system-column names are boot-only, so they carry over. No-op until the
    /// resolver sends a new value; called at each apply so DDL runs against
    /// the current config.
    async fn refresh_config(&mut self) -> Result<(), EmitterError> {
        if !self.config_rx.has_changed().unwrap_or(false) {
            return Ok(());
        }
        let (cfg, conn) = {
            let snap = self.config_rx.borrow_and_update();
            let cfg = DdlConfig::from_resolved(
                &snap,
                self.config.target_database.clone(),
                self.config.soft_delete,
                self.config.system.clone(),
                self.config.replicate_all,
                self.config.runtime_config_schema.clone(),
            );
            let conn = (
                snap.host.clone(),
                snap.port,
                snap.database.clone(),
                snap.user.clone(),
                snap.password.clone(),
                snap.secure,
            );
            (cfg, conn)
        };
        self.config = cfg;
        let (host, port, database, user, password, secure) = conn;
        if host != self.conn_cfg.host
            || port != self.conn_cfg.port
            || database != self.conn_cfg.database
            || user != self.conn_cfg.user
            || password != self.conn_cfg.password
            || secure != self.conn_cfg.secure
        {
            self.conn_cfg.host = host;
            self.conn_cfg.port = port;
            self.conn_cfg.database = database;
            self.conn_cfg.user = user;
            self.conn_cfg.password = password;
            self.conn_cfg.secure = secure;
            self.client = connect_client(&self.conn_cfg).await?;
            self.last_used = std::time::Instant::now();
        }
        Ok(())
    }

    /// Errors propagate; the worker task turns them into
    /// `DecoderSinkError` so the daemon poisons the stream cleanly.
    pub async fn apply(&mut self, event: &SchemaEvent) -> Result<(), EmitterError> {
        self.refresh_config().await?;
        match event {
            SchemaEvent::Added { desc } => self.apply_added(desc).await,
            SchemaEvent::Changed { old, new, diff } => self.apply_changed(old, new, diff).await,
            SchemaEvent::Dropped { oid: _, rel_name } => self.apply_dropped(rel_name).await,
        }
    }

    async fn apply_added(&mut self, desc: &RelDescriptor) -> Result<(), EmitterError> {
        // Mapped dest created from the mapping when missing; IF NOT EXISTS
        // no-ops an operator-managed table and re-creates after strategy=drop.
        if let Some(m) = self.mapping_for(&desc.rel_name).await {
            self.ensure_database(&m.target.database).await?;
            let settings = self.config.rules.settings(&desc.rel_name);
            let sql =
                render_create_table_from_mapping(desc, &m, &self.config.create_shape(&settings));
            self.execute(&sql).await?;
            self.stats.creates_applied += 1;
            return Ok(());
        }
        // Operator opt-out (`replicate=false`) beats namespace auto_create:
        // no CH mirror, no mapping
        if self.is_excluded(&desc.rel_name).await {
            self.stats.skipped += 1;
            return Ok(());
        }
        if !self.config.auto_creates(&desc.rel_name) {
            self.stats.skipped += 1;
            return Ok(());
        }
        // Drives both CREATE TABLE and the row-routing mapping below so
        // rows and DDL land in the same place
        let settings = self.config.rules.settings(&desc.rel_name);
        let target = self.config.create_target(&settings, &desc.rel_name);
        let shape = self.config.create_shape(&settings);
        let Some(sql) = render_create_table(desc, &target, &shape, &self.config.column_rules)?
        else {
            self.stats.skipped += 1;
            return Ok(());
        };
        self.ensure_database(&target.database).await?;
        self.execute(&sql).await?;
        self.stats.creates_applied += 1;
        // Auto-derive a TableMapping so the emitter ships rows against
        // the new CH table without TOML edits
        let columns = derive_columns_for_mapping(desc, &self.config.column_rules);
        let mapping = TableMapping { target, columns };
        self.register_mapping(&desc.rel_name, mapping).await;
        Ok(())
    }

    /// `CREATE TABLE IF NOT EXISTS` for an opted-in rel, regardless of
    /// `auto_create` or an existing mapping. Unlike `apply_added`, does
    /// not gate on the namespace opt-in set and does not write the routing map
    /// — the [`crate::config::ConfigResolver`] owns the opt-in mapping so it
    /// survives the republish full-swap. Returns `false` when the descriptor
    /// has no bridgeable shape (nothing created; caller should not map it).
    /// Idempotent: `IF NOT EXISTS` no-ops a re-create.
    pub async fn ensure_ch_table(&mut self, desc: &RelDescriptor) -> Result<bool, EmitterError> {
        self.refresh_config().await?;
        let settings = self.config.rules.settings(&desc.rel_name);
        let target = self.config.create_target(&settings, &desc.rel_name);
        let shape = self.config.create_shape(&settings);
        let Some(sql) = render_create_table(desc, &target, &shape, &self.config.column_rules)?
        else {
            tracing::warn!(
                target: "walshadow::ch_ddl",
                qname = %desc.rel_name,
                "opt-in skipped: no bridgeable CH shape",
            );
            self.stats.skipped += 1;
            return Ok(false);
        };
        self.ensure_database(&target.database).await?;
        self.execute(&sql).await?;
        self.stats.creates_applied += 1;
        Ok(true)
    }

    async fn apply_changed(
        &mut self,
        _old: &RelDescriptor,
        new: &RelDescriptor,
        diff: &SchemaDiff,
    ) -> Result<(), EmitterError> {
        let key = new.rel_name.clone();
        let Some((mapped_at, target)) = self.mapping_target(&key).await else {
            // No target, can't ALTER; `Added` handles the
            // not-yet-learned case
            self.stats.skipped += 1;
            return Ok(());
        };
        let target = target.sql();
        // RENAME before ADD/DROP so position-matched renames don't trip
        // a later diff into a drop+add pair
        for (_attnum, old_name, new_name) in &diff.renamed_columns {
            // Operator TOML rename makes the source rename a no-op from
            // CH's POV (TOML still maps src_attnum to the same CH name);
            // detect via whether the CH column name changed
            let columns = mapping_columns_at(&self.mapping, &key, &mapped_at).await?;
            let already_renamed = columns.iter().any(|c| &c.target_name == new_name)
                || !columns.iter().any(|c| &c.target_name == old_name);
            if already_renamed {
                // Pre-declared TOML mapping already encodes the rename
                continue;
            }
            // IF EXISTS keeps rename idempotent: reconnect+resend or
            // daemon-restart re-fire no-ops once CH has the renamed column
            let sql = format!(
                "ALTER TABLE {} RENAME COLUMN IF EXISTS {} TO {}",
                target,
                quote_ident(old_name),
                quote_ident(new_name)
            );
            self.execute(&sql).await?;
            self.stats.alters_applied += 1;
        }
        for att in &diff.added_columns {
            let pk_member = replident_key_attnums(new).contains(&att.attnum);
            let Ok(resolved) = type_bridge::map(att, pk_member) else {
                // Unbridged type; operator TOML override is the recovery path
                self.stats.skipped += 1;
                continue;
            };
            let (name, resolved) = apply_column_rule(
                &att.name,
                resolved,
                self.config.column_rules.settings(&new.rel_name, &att.name),
            );
            let sql = render_add_column(&target, &name, &resolved);
            self.execute(&sql).await?;
            self.stats.alters_applied += 1;
        }
        for attnum in &diff.dropped_columns {
            // diff lists attnums only; resolve CH column name from old descriptor
            let name = _old
                .attributes
                .iter()
                .find(|a| a.attnum == *attnum)
                .map(|a| a.name.clone());
            let Some(name) = name else {
                self.stats.skipped += 1;
                continue;
            };
            // Surface the drop on CH even if TOML still references the
            // column; emitter then encodes NULL for the vanished attnum
            let sql = format!(
                "ALTER TABLE {} DROP COLUMN IF EXISTS {}",
                target,
                quote_ident(&name)
            );
            self.execute(&sql).await?;
            self.stats.alters_applied += 1;
        }
        if !diff.type_changes.is_empty() {
            self.stats.type_changes_rejected += diff.type_changes.len() as u64;
            tracing::warn!(
                target: "walshadow::ch_ddl",
                relation = %new.rel_name,
                type_changes = diff.type_changes.len(),
                "unsupported schema change: type widening / domain change \
                 (manual operator migration required)"
            );
        }
        // Auto-extend the TableMapping so the emitter ships post-DDL
        // rows against the new shape without TOML edits; operator-pinned
        // `target_name` overrides survive (only touch entries the
        // applicator could have produced, by src_attnum match)
        self.fold_mapping_diff(new, diff).await;
        Ok(())
    }

    async fn apply_dropped(&mut self, rel: &RelName) -> Result<(), EmitterError> {
        let Some((_, target)) = self.mapping_target(rel).await else {
            self.stats.skipped += 1;
            return Ok(());
        };
        match self.config.drop_strategy_for(&rel.namespace) {
            DropTableStrategy::Retain => {
                self.stats.skipped += 1;
                tracing::info!(
                    target: "walshadow::ch_ddl",
                    source = %rel,
                    dest = %target,
                    "source DROP TABLE; CH dest retained per strategy=retain",
                );
                Ok(())
            }
            DropTableStrategy::Warn => {
                self.stats.skipped += 1;
                tracing::warn!(
                    target: "walshadow::ch_ddl",
                    source = %rel,
                    dest = %target,
                    "source DROP TABLE; CH dest retained per strategy=warn",
                );
                Ok(())
            }
            DropTableStrategy::Drop => {
                let sql = format!("DROP TABLE IF EXISTS {}", target.sql());
                self.execute(&sql).await?;
                self.stats.drops_applied += 1;
                // Forget the runtime-derived entry so a future Added
                // re-derives columns. A TOML-pinned mapping stays (operator
                // owns it; republish would resurrect it anyway) — a source
                // re-create restores its dest via apply_added's
                // strategy=drop path
                self.forget_mapping(rel).await;
                Ok(())
            }
        }
    }

    /// `TRUNCATE TABLE <target>`, no-op for unmapped relations. Reorder
    /// coordinator calls this inside a barrier (after earlier data is
    /// durable) so the truncate orders correctly against inserts despite
    /// the otherwise out-of-order pipeline.
    pub async fn truncate(&mut self, rel: &RelName) -> Result<(), EmitterError> {
        let Some((_, target)) = self.mapping_target(rel).await else {
            return Ok(());
        };
        self.execute(&format!("TRUNCATE TABLE {}", target.sql()))
            .await
    }

    /// Resolve target against a held snapshot, which later ALTER steps
    /// re-check for displacement
    async fn mapping_target(&mut self, rel: &RelName) -> Option<(MappingSnapshot, TableTarget)> {
        let at = self.mapping.snapshot().await;
        let target = at.get(rel).map(|t| t.target.clone())?;
        Some((at, target))
    }

    async fn mapping_for(&mut self, rel: &RelName) -> Option<TableMapping> {
        self.mapping.with(|m| m.get(rel).cloned()).await
    }

    /// Operator opt-out (`replicate=false`); nothing excluded without a
    /// resolver. `&mut self` like siblings: `&self` across await would
    /// demand `DdlApplicator: Sync`, blocked by chc client's raw pointer
    async fn is_excluded(&mut self, rel: &RelName) -> bool {
        if let Some(r) = &self.resolver {
            r.is_excluded(rel).await
        } else {
            false
        }
    }

    /// Route writes through resolver so derived mappings survive republish
    async fn register_mapping(&mut self, rel: &RelName, mapping: TableMapping) {
        if let Some(r) = &self.resolver {
            r.register_derived_mapping(rel, mapping).await;
        } else {
            self.mapping
                .mutate(|m| Arc::make_mut(m).insert(rel.clone(), mapping))
                .await;
        }
    }

    pub async fn predict_route_mapping(
        &mut self,
        event: &SchemaEvent,
        mapping: &MappingSnapshot,
        config: Option<&ResolvedConfig>,
    ) -> Result<Option<(RelName, Option<TableMapping>)>, EmitterError> {
        // Resolver owns opt-outs, frozen routing state does not include them
        let excluded = match event {
            SchemaEvent::Added { desc } if !mapping.contains_key(&desc.rel_name) => {
                self.is_excluded(&desc.rel_name).await
            }
            _ => false,
        };
        predict_route_effect(&self.plan_config(config), mapping, event, excluded)
    }

    fn plan_config(&self, frozen: Option<&ResolvedConfig>) -> DdlConfig {
        frozen
            .map(|rc| {
                DdlConfig::from_resolved(
                    rc,
                    self.config.target_database.clone(),
                    self.config.soft_delete,
                    self.config.system.clone(),
                    self.config.replicate_all,
                    self.config.runtime_config_schema.clone(),
                )
            })
            .unwrap_or_else(|| self.config.clone())
    }

    async fn fold_mapping_diff(&mut self, new: &RelDescriptor, diff: &SchemaDiff) {
        if let Some(r) = &self.resolver {
            r.apply_schema_diff(new, diff).await;
        } else {
            mutate_mapping_for_diff(&self.mapping, new, diff, &self.config.column_rules).await;
        }
    }

    async fn forget_mapping(&mut self, rel: &RelName) {
        if let Some(r) = &self.resolver {
            r.forget_derived_mapping(rel).await;
        } else {
            self.mapping.mutate(|m| Arc::make_mut(m).remove(rel)).await;
        }
    }

    /// Run one DDL statement with the same bounded timeout +
    /// reconnect/retry as the INSERT pump. DDL applies inside the
    /// reorder barrier, so a half-open socket would otherwise park the
    /// barrier and ack frontier indefinitely. Every emitted statement is
    /// idempotent (`IF [NOT] EXISTS`, `RENAME COLUMN IF EXISTS`,
    /// `TRUNCATE`), so a reconnect resends and CH no-ops the second apply.
    async fn ensure_database(&mut self, db: &str) -> Result<(), EmitterError> {
        if self.ensured_databases.contains(db) {
            return Ok(());
        }
        let sql = format!("CREATE DATABASE IF NOT EXISTS {}", quote_ident(db));
        self.execute(&sql).await?;
        self.ensured_databases.insert(db.to_owned());
        Ok(())
    }

    async fn execute(&mut self, sql: &str) -> Result<(), EmitterError> {
        tracing::debug!(target: "walshadow::ch_ddl", sql = %sql, "applying");
        let mut attempt = 0u32;
        let mut backoff = self.retry.initial_backoff;
        reconnect_if_idle(&mut self.client, &self.conn_cfg, self.last_used).await?;
        loop {
            let attempt_result = exec_drain(&mut self.client, sql, self.query_timeout).await;
            match attempt_result {
                Ok(()) => {
                    self.last_used = std::time::Instant::now();
                    return Ok(());
                }
                Err(e) if is_retryable(&e) && attempt < self.retry.max_attempts => {
                    tracing::warn!(
                        target: "walshadow::ch_ddl",
                        error = %e, attempt, sql = %sql,
                        "DDL attempt failed; reconnecting + retrying",
                    );
                    attempt += 1;
                    backoff_step(&mut backoff, self.retry.max_backoff).await;
                    self.client = connect_client(&self.conn_cfg).await?;
                }
                Err(e) => return Err(e),
            }
        }
    }
}

/// Predict mapping edit without executing DDL
///
/// Return `None` to keep map, `Some((rel, None))` to remove relation
fn predict_route_effect(
    cfg: &DdlConfig,
    mapping: &MappingSnapshot,
    event: &SchemaEvent,
    excluded: bool,
) -> Result<Option<(RelName, Option<TableMapping>)>, EmitterError> {
    match event {
        SchemaEvent::Added { desc } => {
            // `replicate_all` is not predicted: it maps on first sight, which
            // the executor's own apply covers
            if mapping.contains_key(&desc.rel_name)
                || excluded
                || !(cfg
                    .auto_create_namespaces
                    .contains(&*desc.rel_name.namespace)
                    || cfg.declared_scope(&desc.rel_name) == Some(true))
            {
                return Ok(None);
            }
            let settings = cfg.rules.settings(&desc.rel_name);
            let target = cfg.create_target(&settings, &desc.rel_name);
            let shape = cfg.create_shape(&settings);
            if render_create_table(desc, &target, &shape, &cfg.column_rules)?.is_none() {
                return Ok(None);
            }
            let columns = derive_columns_for_mapping(desc, &cfg.column_rules);
            Ok(Some((
                desc.rel_name.clone(),
                Some(TableMapping { target, columns }),
            )))
        }
        SchemaEvent::Changed { new, diff, .. } => {
            let Some(mut m) = mapping.get(&new.rel_name).cloned() else {
                return Ok(None);
            };
            fold_diff_into_mapping(&mut m, new, diff, &cfg.column_rules);
            Ok(Some((new.rel_name.clone(), Some(m))))
        }
        SchemaEvent::Dropped { rel_name, .. } => {
            if !mapping.contains_key(rel_name)
                || !matches!(
                    cfg.drop_strategy_for(&rel_name.namespace),
                    DropTableStrategy::Drop
                )
            {
                return Ok(None);
            }
            Ok(Some((rel_name.clone(), None)))
        }
    }
}

/// Reject ALTER continuation after concurrent route republish
async fn mapping_columns_at(
    mapping: &MappingHandle,
    rel: &RelName,
    at: &MappingSnapshot,
) -> Result<Vec<ColumnMapping>, EmitterError> {
    if !mapping.unmoved(at).await {
        return Err(EmitterError::Config(format!(
            "routing map for `{rel}` republished mid-ALTER"
        )));
    }
    Ok(at.get(rel).map(|t| t.columns.clone()).unwrap_or_default())
}

/// Fold a `Changed` diff into the live mapping. Renames touch only entries
/// whose `target_name` still equals the OLD source name; an operator-pinned
/// different name is left alone (CH runs no ALTER for it either, see
/// `apply_changed`)
async fn mutate_mapping_for_diff(
    mapping: &MappingHandle,
    new: &RelDescriptor,
    diff: &SchemaDiff,
    rules: &ColumnRules,
) {
    mapping
        .mutate(|m| {
            if let Some(target_mapping) = Arc::make_mut(m).get_mut(&new.rel_name) {
                fold_diff_into_mapping(target_mapping, new, diff, rules);
            }
        })
        .await;
}

/// The fold itself, shared with `ConfigResolver::apply_schema_diff` (the
/// resolver-owned path, where the folded mapping must live in a layer the
/// republish full-swap rebuilds from).
/// `ALTER TABLE <t> ADD COLUMN IF NOT EXISTS <n> <ty> [DEFAULT <expr>]`.
/// IF NOT EXISTS keeps it idempotent across a daemon-restart re-fire.
pub fn render_add_column(target: &str, name: &str, resolved: &ResolvedColumn) -> String {
    let mut s = format!(
        "ALTER TABLE {target} ADD COLUMN IF NOT EXISTS {} {}",
        quote_ident(name),
        resolved.ch_type
    );
    if let Some(d) = &resolved.default_sql {
        s.push_str(" DEFAULT ");
        s.push_str(d);
    }
    s
}

/// Shared CREATE tail: synthetic columns (mirror `TablePlan::build`),
/// engine, `ORDER BY` key names (else the LSN column)
fn render_create_sql(
    target: &str,
    mut col_defs: Vec<String>,
    key_names: Vec<String>,
    shape: &CreateShape<'_>,
) -> String {
    let sys = &shape.system;
    let lsn = quote_ident(&sys.lsn);
    col_defs.push(format!("{lsn} UInt64"));
    col_defs.push(format!("{} UInt32", quote_ident(&sys.xid)));
    col_defs.push(format!(
        "{} DateTime64(6, 'UTC')",
        quote_ident(&sys.commit_ts)
    ));
    // soft_delete keeps the delete marker out of the engine args; without the
    // marker column there is nothing to pass either
    let engine_args = match &sys.is_deleted {
        Some(name) => {
            let marker = quote_ident(name);
            col_defs.push(format!("{marker} Bool"));
            if shape.soft_delete {
                lsn.clone()
            } else {
                format!("{lsn}, {marker}")
            }
        }
        None => lsn.clone(),
    };
    let keys = if key_names.is_empty() {
        vec![lsn]
    } else {
        key_names
    };
    // CH indexes the PRIMARY KEY prefix of the sorting key; a non-prefix is a
    // CREATE-time error there, so drop it and let the key default to ORDER BY
    let primary_key = match shape.primary_key {
        [] => String::new(),
        pk if pk
            .iter()
            .map(|n| quote_ident(n))
            .eq(keys.iter().take(pk.len()).cloned()) =>
        {
            format!("\nPRIMARY KEY ({})", keys[..pk.len()].join(", "))
        }
        pk => {
            tracing::warn!(
                target: "walshadow::ch_ddl",
                table = %target,
                primary_key = ?pk,
                "primary_key ignored: not a prefix of ORDER BY",
            );
            String::new()
        }
    };
    let order_by = keys.join(", ");
    format!(
        "CREATE TABLE IF NOT EXISTS {target} (\n  {}\n) ENGINE = ReplacingMergeTree({engine_args})\nORDER BY ({order_by}){primary_key}",
        col_defs.join(",\n  ")
    )
}

/// Operator `ORDER BY` names → quoted key list. `orderable` maps every
/// destination column name a sort key may use to whether CH would reject it
/// (`Nullable` sort keys are illegal). An override naming an absent or
/// nullable column is dropped whole with a WARN, so one bad config row
/// degrades to the replica-identity key instead of failing every CREATE.
fn resolve_order_by(
    target: &str,
    order_by: &[String],
    orderable: &HashMap<&str, bool>,
    derived: Vec<String>,
) -> Vec<String> {
    if order_by.is_empty() {
        return derived;
    }
    let mut keys = Vec::with_capacity(order_by.len());
    for name in order_by {
        let Some(false) = orderable.get(name.as_str()) else {
            tracing::warn!(
                target: "walshadow::ch_ddl",
                table = %target,
                column = %name,
                reason = if orderable.contains_key(name.as_str()) { "nullable" } else { "absent" },
                "order_by ignored; keying on replica identity",
            );
            return derived;
        };
        keys.push(quote_ident(name));
    }
    keys
}

/// System columns are always non-nullable, so any of them may sort
fn orderable_system<'a>(sys: &'a SystemColumns, orderable: &mut HashMap<&'a str, bool>) {
    for name in sys.names() {
        orderable.insert(name, false);
    }
}

/// ClickHouse rejects Nullable columns in a sorting key
fn is_nullable(ch_type: &str) -> bool {
    ch_type.starts_with("Nullable(")
}

/// `CREATE TABLE IF NOT EXISTS` for an autodiscovered relation. `None`
/// when a column's type can't be bridged; caller logs + skips.
pub fn render_create_table(
    desc: &RelDescriptor,
    target: &TableTarget,
    shape: &CreateShape<'_>,
    rules: &ColumnRules,
) -> Result<Option<String>, EmitterError> {
    let target = target.sql();
    let pk_attnums = replident_key_attnums(desc);
    let mut cols = Vec::with_capacity(desc.attributes.len());
    for att in &desc.attributes {
        if att.dropped {
            continue;
        }
        let pk_member = pk_attnums.contains(&att.attnum);
        let Ok(resolved) = type_bridge::map(att, pk_member) else {
            // Skip the half-renderable CREATE; operator installs a
            // TOML override and re-triggers via Added on next refetch
            return Ok(None);
        };
        let (name, resolved) = apply_column_rule(
            &att.name,
            resolved,
            rules.settings(&desc.rel_name, &att.name),
        );
        cols.push((att.attnum, name, resolved));
    }
    let col_defs: Vec<String> = cols
        .iter()
        .map(|(_, name, r)| {
            let name = quote_ident(name);
            r.default_sql.as_ref().map_or_else(
                || format!("{name} {}", r.ch_type),
                |d| format!("{name} {} DEFAULT {d}", r.ch_type),
            )
        })
        .collect();
    let mut orderable: HashMap<&str, bool> = HashMap::default();
    orderable_system(&shape.system, &mut orderable);
    for (_, name, r) in &cols {
        orderable.insert(name, is_nullable(&r.ch_type));
    }
    // ClickHouse rejects Nullable columns in ORDER BY
    let derived: Vec<String> = pk_attnums
        .iter()
        .filter_map(|a| {
            cols.iter()
                .find(|(attnum, _, r)| attnum == a && !is_nullable(&r.ch_type))
                .map(|(_, name, _)| quote_ident(name))
        })
        .collect();
    let key_names = resolve_order_by(&target, shape.order_by, &orderable, derived);
    Ok(Some(render_create_sql(&target, col_defs, key_names, shape)))
}

/// CH `UNKNOWN_DATABASE`
const UNKNOWN_DATABASE: i32 = 81;

/// Create `[ch] database` when the server doesn't have it, over a session on
/// `default` — every other client names it in the handshake, which CH refuses
/// outright for an absent database, so no connected client can create its own.
/// `Ok(false)` when it was already there. Sibling databases (per-namespace
/// `target_database`) go through the applicator's own `ensure_database` instead
pub async fn ensure_boot_database(cfg: &EmitterConfig) -> Result<bool, EmitterError> {
    match connect_client(cfg).await {
        Ok(_) => Ok(false),
        Err(EmitterError::Client(e)) if e.server_code == UNKNOWN_DATABASE => {
            let mut on_default = cfg.clone();
            on_default.database = "default".into();
            let mut client = connect_client(&on_default).await?;
            exec_drain(
                &mut client,
                &format!(
                    "CREATE DATABASE IF NOT EXISTS {}",
                    quote_ident(&cfg.database)
                ),
                cfg.insert_timeout,
            )
            .await?;
            tracing::info!(
                target: "walshadow::ch_ddl",
                database = %cfg.database,
                "created destination database",
            );
            Ok(true)
        }
        Err(e) => Err(e),
    }
}

/// `CREATE TABLE IF NOT EXISTS` rendered from an existing mapping — the
/// re-create path for a mapped dest dropped under strategy=drop. Columns
/// come from the mapping (the emitter's INSERT contract), not the
/// descriptor; `ORDER BY` takes the operator override when it names
/// non-nullable destination columns, else resolves the descriptor's key
/// attnums through the mapping (skipping Nullable targets, which CH rejects
/// as sort keys), else the LSN column
pub fn render_create_table_from_mapping(
    desc: &RelDescriptor,
    mapping: &TableMapping,
    shape: &CreateShape<'_>,
) -> String {
    let col_defs: Vec<String> = mapping
        .columns
        .iter()
        .map(|c| format!("{} {}", quote_ident(&c.target_name), c.target_type))
        .collect();
    let mut orderable: HashMap<&str, bool> = HashMap::default();
    orderable_system(&shape.system, &mut orderable);
    for c in &mapping.columns {
        orderable.insert(&c.target_name, is_nullable(&c.target_type));
    }
    let derived: Vec<String> = replident_key_attnums(desc)
        .iter()
        .filter_map(|a| {
            mapping
                .columns
                .iter()
                .find(|c| c.src_attnum == *a && !is_nullable(&c.target_type))
                .map(|c| quote_ident(&c.target_name))
        })
        .collect();
    let target = mapping.target.sql();
    let key_names = resolve_order_by(&target, shape.order_by, &orderable, derived);
    render_create_sql(&target, col_defs, key_names, shape)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mapping::{ColumnMapping, TableMapping};
    use std::sync::LazyLock;

    static SYS: LazyLock<Arc<SystemColumns>> = LazyLock::new(Arc::default);

    fn dest(database: &str, desc: &RelDescriptor) -> TableTarget {
        TableTarget::new(database, &desc.rel_name.name)
    }

    /// Default system columns, no operator key override
    fn shape(soft_delete: bool) -> CreateShape<'static> {
        CreateShape {
            system: SYS.clone(),
            soft_delete,
            order_by: &[],
            primary_key: &[],
        }
    }
    use crate::schema::{INT4OID, TEXTOID, TIMESTAMPTZOID};
    use crate::schema::{RelAttr, RelDescriptor, ReplIdent, SchemaDiff};
    use crate::table_rules::MatchKind;

    #[test]
    fn system_namespaces_excluded_from_replicate_all() {
        assert!(is_system_namespace("pg_catalog", None));
        assert!(is_system_namespace("pg_toast", None));
        assert!(is_system_namespace("information_schema", None));
        assert!(is_system_namespace("pg_temp_3", None));
        assert!(!is_system_namespace("public", None));
        assert!(is_system_namespace("walshadow", Some("walshadow")));
        assert!(!is_system_namespace("walshadow", Some("")));
        assert!(!is_system_namespace("public", Some("walshadow")));
    }

    #[test]
    fn per_namespace_target_and_drop_override_global() {
        use crate::mapping::NamespaceMapping;
        use ahash::{HashMap, HashMapExt};
        let mut namespaces = HashMap::new();
        namespaces.insert(
            "analytics".to_string(),
            NamespaceMapping {
                target_database: Some("warehouse".into()),
                auto_create: true,
                drop_table_strategy: Some(DropTableStrategy::Drop),
                initial_load: None,
            },
        );
        namespaces.insert(
            "logs".to_string(),
            NamespaceMapping {
                target_database: None,
                auto_create: true,
                drop_table_strategy: None,
                initial_load: None,
            },
        );
        let cfg = DdlConfig {
            drop_table_strategy: DropTableStrategy::Retain,
            auto_create_namespaces: HashSet::new(),
            replicate_all: false,
            runtime_config_schema: None,
            target_database: "default".into(),
            namespaces,
            soft_delete: false,
            system: Arc::default(),
            rules: Arc::default(),
            column_rules: Arc::default(),
        };
        assert_eq!(cfg.target_database_for("analytics"), "warehouse");
        assert_eq!(cfg.target_database_for("logs"), "default");
        assert_eq!(cfg.target_database_for("unconfigured"), "default");
        assert_eq!(cfg.drop_strategy_for("analytics"), DropTableStrategy::Drop);
        assert_eq!(cfg.drop_strategy_for("logs"), DropTableStrategy::Retain);
        assert_eq!(
            cfg.drop_strategy_for("unconfigured"),
            DropTableStrategy::Retain
        );
    }

    #[test]
    fn with_drop_strategy_overrides_global_default() {
        use ahash::{HashMap, HashMapExt};
        let cfg = DdlConfig {
            drop_table_strategy: DropTableStrategy::Retain,
            auto_create_namespaces: HashSet::new(),
            replicate_all: false,
            runtime_config_schema: None,
            target_database: "default".into(),
            namespaces: HashMap::new(),
            soft_delete: false,
            system: Arc::default(),
            rules: Arc::default(),
            column_rules: Arc::default(),
        };
        assert_eq!(cfg.drop_table_strategy, DropTableStrategy::Retain);
        let cfg = cfg.with_drop_strategy(DropTableStrategy::Drop);
        assert_eq!(cfg.drop_table_strategy, DropTableStrategy::Drop);
        // Global override drives the per-namespace fallback
        assert_eq!(
            cfg.drop_strategy_for("unconfigured"),
            DropTableStrategy::Drop
        );
    }

    fn att(attnum: i16, name: &str, oid: u32, not_null: bool, missing: Option<&str>) -> RelAttr {
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
            missing_text: missing.map(String::from),
        }
    }

    fn desc(name: &str, attrs: Vec<RelAttr>, pk: Option<Vec<i16>>) -> RelDescriptor {
        RelDescriptor {
            rfn: walrus::pg::walparser::RelFileNode {
                spc_node: 1663,
                db_node: 5,
                rel_node: 16400,
            },
            oid: 16400,
            toast_oid: 0,
            namespace_oid: 2200,
            rel_name: RelName::new("public", name),
            kind: 'r',
            persistence: 'p',
            replident: ReplIdent::Default { pk_attnums: pk },
            attributes: attrs,
        }
    }

    #[test]
    fn pattern_entry_decides_scope_and_destination() {
        use crate::table_rules::{TableRule, TableRulesBuilder};
        let mut b = TableRulesBuilder::new();
        b.add(
            &RelName::new("app", "events_*"),
            MatchKind::Glob,
            TableRule {
                replicate: Some(true),
                target_database: Some("warehouse".into()),
                system: crate::mapping::SystemColumnNames {
                    lsn: Some("_peerdb_version".into()),
                    is_deleted: Some(String::new()),
                    ..Default::default()
                },
                ..TableRule::default()
            },
        );
        b.add(
            &RelName::new("app", "*_audit"),
            MatchKind::Glob,
            TableRule {
                replicate: Some(false),
                ..TableRule::default()
            },
        );
        b.add(
            &RelName::new("*", "*"),
            MatchKind::Glob,
            TableRule {
                replicate: Some(true),
                ..TableRule::default()
            },
        );
        let (rules, rejected) = b.finish();
        assert_eq!(rejected, 0);
        let cfg = DdlConfig {
            drop_table_strategy: DropTableStrategy::Retain,
            auto_create_namespaces: HashSet::new(),
            replicate_all: false,
            runtime_config_schema: Some("walshadow".into()),
            target_database: "default".into(),
            namespaces: ahash::HashMap::default(),
            soft_delete: false,
            system: Arc::default(),
            rules: Arc::new(rules),
            column_rules: Arc::default(),
        };
        let events = RelName::new("app", "events_1");
        assert!(cfg.auto_creates(&events));
        assert_eq!(
            cfg.create_target(&cfg.rules.settings(&events), &events),
            TableTarget::new("warehouse", "events_1")
        );
        assert_eq!(
            cfg.declared_scope(&RelName::new("app", "events_audit")),
            Some(false),
            "guardrail beats the matching opt-in"
        );
        assert!(!cfg.auto_creates(&RelName::new("app", "events_audit")));
        assert_eq!(cfg.declared_scope(&RelName::new("walshadow", "x")), None);
        assert!(!cfg.auto_creates(&RelName::new("pg_catalog", "pg_class")));
        let settings = cfg.rules.settings(&events);
        let shape = cfg.create_shape(&settings);
        assert_eq!(shape.system.lsn, "_peerdb_version");
        assert!(shape.system.is_deleted.is_none(), "marker dropped");
        assert_eq!(shape.system.xid, "_xid", "unnamed column inherits");
        let other = RelName::new("other", "t");
        assert_eq!(
            cfg.create_target(&cfg.rules.settings(&other), &other),
            TableTarget::new("default", "t")
        );
        let settings = cfg.rules.settings(&other);
        assert_eq!(
            cfg.create_shape(&settings).system.lsn,
            "_lsn",
            "a relation no entry renames keeps the cluster-wide names"
        );
    }

    #[test]
    fn render_add_column_emits_idempotent_alter_with_default() {
        let resolved = ResolvedColumn {
            ch_type: "Nullable(Int32)".into(),
            default_sql: Some("7".into()),
        };
        let sql = render_add_column("default.orders", "ship_at", &resolved);
        assert_eq!(
            sql,
            "ALTER TABLE default.orders ADD COLUMN IF NOT EXISTS `ship_at` Nullable(Int32) DEFAULT 7"
        );
    }

    #[test]
    fn render_add_column_without_default_skips_default_clause() {
        let resolved = ResolvedColumn {
            ch_type: "String".into(),
            default_sql: None,
        };
        let sql = render_add_column("default.t", "c", &resolved);
        assert_eq!(
            sql,
            "ALTER TABLE default.t ADD COLUMN IF NOT EXISTS `c` String"
        );
    }

    #[test]
    fn render_create_table_states_the_rule_name_and_type() {
        let d = desc(
            "orders",
            vec![
                att(1, "id", INT4OID, true, None),
                att(2, "net_amount", INT4OID, false, None),
            ],
            Some(vec![1]),
        );
        let mut b = crate::column_rules::ColumnRulesBuilder::new();
        b.add(
            &RelName::new("public", "*"),
            MatchKind::Glob,
            "*_amount",
            MatchKind::Glob,
            crate::column_rules::ColumnRule {
                target_name: None,
                target_type: Some("Decimal(38, 9)".into()),
            },
        );
        b.add(
            &RelName::new("public", "orders"),
            MatchKind::Exact,
            "id",
            MatchKind::Exact,
            crate::column_rules::ColumnRule {
                target_name: Some("order_id".into()),
                target_type: None,
            },
        );
        let sql = render_create_table(&d, &dest("db", &d), &shape(false), &b.finish().0)
            .unwrap()
            .expect("renderable");
        assert!(sql.contains("`order_id` Int32"), "{sql}");
        assert!(sql.contains("`net_amount` Decimal(38, 9)"), "{sql}");
        assert!(sql.ends_with("ORDER BY (`order_id`)"), "{sql}");
    }

    #[test]
    fn render_create_table_drops_a_key_a_rule_made_nullable() {
        let d = desc("t", vec![att(1, "id", INT4OID, true, None)], Some(vec![1]));
        let mut b = crate::column_rules::ColumnRulesBuilder::new();
        b.add(
            &RelName::new("public", "t"),
            MatchKind::Exact,
            "id",
            MatchKind::Exact,
            crate::column_rules::ColumnRule {
                target_name: None,
                target_type: Some("Nullable(Int32)".into()),
            },
        );
        let sql = render_create_table(&d, &dest("db", &d), &shape(false), &b.finish().0)
            .unwrap()
            .expect("renderable");
        assert!(sql.ends_with("ORDER BY (`_lsn`)"), "{sql}");
    }

    #[test]
    fn render_create_table_uses_pk_for_order_by() {
        let d = desc(
            "orders",
            vec![
                att(1, "id", INT4OID, true, None),
                att(2, "body", TEXTOID, false, None),
            ],
            Some(vec![1]),
        );
        let sql = render_create_table(
            &d,
            &dest("default", &d),
            &shape(false),
            &ColumnRules::default(),
        )
        .unwrap()
        .unwrap();
        assert!(sql.contains("CREATE TABLE IF NOT EXISTS `default`.`orders`"));
        assert!(sql.contains("`id` Int32"));
        assert!(sql.contains("`body` Nullable(String)"));
        assert!(sql.contains("`_lsn` UInt64"));
        assert!(sql.contains("`_is_deleted` Bool"));
        assert!(sql.contains("ENGINE = ReplacingMergeTree(`_lsn`, `_is_deleted`)"));
        assert!(sql.ends_with("ORDER BY (`id`)"));
    }

    #[test]
    fn render_create_table_full_replica_identity_orders_by_pk_not_lsn() {
        // REPLICA IDENTITY FULL exposes no key *index* but the table still has a
        // PK; ORDER BY must use it, else `ORDER BY _lsn` collapses every row
        // sharing an `_lsn` (e.g. a whole backfill tagged one LSN).
        let mut d = desc(
            "orders",
            vec![
                att(1, "id", INT4OID, true, None),
                att(2, "body", TEXTOID, false, None),
            ],
            None,
        );
        d.replident = ReplIdent::Full {
            pk_attnums: Some(vec![1]),
        };
        let sql = render_create_table(
            &d,
            &dest("default", &d),
            &shape(false),
            &ColumnRules::default(),
        )
        .unwrap()
        .unwrap();
        assert!(sql.ends_with("ORDER BY (`id`)"), "{sql}");
        assert!(!sql.contains("ORDER BY _lsn"), "{sql}");
    }

    #[test]
    fn render_create_table_composite_pk_preserves_order_and_forces_non_null() {
        // ORDER BY follows declared PK order (b, a); PK members forced non-null.
        let d = desc(
            "orders",
            vec![
                att(1, "a", INT4OID, false, None),
                att(2, "b", INT4OID, false, None),
                att(3, "body", TEXTOID, false, None),
            ],
            Some(vec![2, 1]),
        );
        let sql = render_create_table(
            &d,
            &dest("default", &d),
            &shape(false),
            &ColumnRules::default(),
        )
        .unwrap()
        .unwrap();
        assert!(sql.contains("`a` Int32"), "{sql}");
        assert!(sql.contains("`b` Int32"), "{sql}");
        assert!(!sql.contains("`a` Nullable"), "{sql}");
        assert!(!sql.contains("`b` Nullable"), "{sql}");
        assert!(sql.contains("`body` Nullable(String)"), "{sql}");
        assert!(sql.ends_with("ORDER BY (`b`, `a`)"), "{sql}");
    }

    #[test]
    fn soft_delete_keeps_is_deleted_out_of_engine_args() {
        let d = desc(
            "orders",
            vec![
                att(1, "id", INT4OID, true, None),
                att(2, "body", TEXTOID, false, None),
            ],
            Some(vec![1]),
        );
        let sql = render_create_table(
            &d,
            &dest("default", &d),
            &shape(true),
            &ColumnRules::default(),
        )
        .unwrap()
        .unwrap();
        // Column always present; soft_delete only drops it from the engine
        assert!(sql.contains("`_is_deleted` Bool"));
        assert!(sql.contains("ENGINE = ReplacingMergeTree(`_lsn`)"));
        assert!(!sql.contains("ReplacingMergeTree(`_lsn`, `_is_deleted`)"));
        assert!(sql.ends_with("ORDER BY (`id`)"));
    }

    #[test]
    fn render_create_table_falls_back_to_lsn_when_no_pk() {
        let d = desc("events", vec![att(1, "body", TEXTOID, false, None)], None);
        let sql = render_create_table(
            &d,
            &dest("default", &d),
            &shape(false),
            &ColumnRules::default(),
        )
        .unwrap()
        .unwrap();
        assert!(sql.ends_with("ORDER BY (`_lsn`)"));
    }

    #[test]
    fn render_create_table_order_by_from_replica_identity_index() {
        // REPLICA IDENTITY USING INDEX: ORDER BY follows the index key cols.
        let mut d = desc(
            "events",
            vec![
                att(1, "tenant", INT4OID, false, None),
                att(2, "key", TEXTOID, false, None),
                att(3, "body", TEXTOID, false, None),
            ],
            None,
        );
        d.replident = ReplIdent::UsingIndex {
            index_oid: 16500,
            key_attnums: vec![2, 1],
        };
        let sql = render_create_table(
            &d,
            &dest("default", &d),
            &shape(false),
            &ColumnRules::default(),
        )
        .unwrap()
        .unwrap();
        assert!(!sql.contains("`key` Nullable"), "{sql}");
        assert!(!sql.contains("`tenant` Nullable"), "{sql}");
        assert!(sql.contains("`body` Nullable(String)"), "{sql}");
        assert!(sql.ends_with("ORDER BY (`key`, `tenant`)"), "{sql}");
    }

    #[test]
    fn render_create_table_falls_back_to_lsn_when_pk_cols_all_dropped() {
        // PK attnum references a dropped column → empty name list → `_lsn`.
        let d = desc(
            "events",
            vec![att(2, "body", TEXTOID, false, None)],
            Some(vec![1]),
        );
        let sql = render_create_table(
            &d,
            &dest("default", &d),
            &shape(false),
            &ColumnRules::default(),
        )
        .unwrap()
        .unwrap();
        assert!(sql.ends_with("ORDER BY (`_lsn`)"), "{sql}");
    }

    #[test]
    fn render_create_table_handles_timestamp_precision() {
        let mut a = att(1, "ship_at", TIMESTAMPTZOID, false, None);
        a.typmod = 3;
        let d = desc("t", vec![a], None);
        let sql = render_create_table(&d, &dest("db", &d), &shape(false), &ColumnRules::default())
            .unwrap()
            .unwrap();
        assert!(
            sql.contains("`ship_at` Nullable(DateTime64(3, 'UTC'))"),
            "{sql}"
        );
    }

    #[test]
    fn render_create_table_from_mapping_uses_pinned_shape() {
        let d = desc(
            "orders",
            vec![
                att(1, "id", INT4OID, true, None),
                att(2, "body", TEXTOID, false, None),
            ],
            Some(vec![1]),
        );
        let m = TableMapping {
            target: TableTarget::new("warehouse", "orders_pinned"),
            columns: vec![
                ColumnMapping {
                    src_attnum: 1,
                    target_name: "order_id".into(),
                    target_type: "Int64".into(),
                },
                ColumnMapping {
                    src_attnum: 2,
                    target_name: "payload".into(),
                    target_type: "Nullable(String)".into(),
                },
            ],
        };
        let sql = render_create_table_from_mapping(&d, &m, &shape(false));
        assert!(
            sql.contains("CREATE TABLE IF NOT EXISTS `warehouse`.`orders_pinned`"),
            "{sql}"
        );
        assert!(sql.contains("`order_id` Int64"), "{sql}");
        assert!(sql.contains("`payload` Nullable(String)"), "{sql}");
        assert!(sql.contains("`_lsn` UInt64"), "{sql}");
        // ORDER BY resolves the descriptor's pk attnum to the mapped name
        assert!(sql.ends_with("ORDER BY (`order_id`)"), "{sql}");
    }

    #[test]
    fn render_create_table_from_mapping_nullable_key_falls_back_to_lsn() {
        let d = desc("t", vec![att(1, "id", INT4OID, false, None)], Some(vec![1]));
        let m = TableMapping {
            target: TableTarget::new("db", "t"),
            columns: vec![ColumnMapping {
                src_attnum: 1,
                target_name: "id".into(),
                target_type: "Nullable(Int32)".into(),
            }],
        };
        let sql = render_create_table_from_mapping(&d, &m, &shape(false));
        assert!(sql.ends_with("ORDER BY (`_lsn`)"), "{sql}");
    }

    #[test]
    fn render_create_table_renames_system_columns() {
        let sys = SystemColumns {
            lsn: "_peerdb_version".into(),
            xid: "_xid".into(),
            commit_ts: "_peerdb_synced_at".into(),
            is_deleted: Some("_peerdb_is_deleted".into()),
        };
        let d = desc("t", vec![att(1, "id", INT4OID, true, None)], Some(vec![1]));
        let sql = render_create_table(
            &d,
            &dest("default", &d),
            &CreateShape {
                system: sys.clone().into(),
                soft_delete: false,
                order_by: &[],
                primary_key: &[],
            },
            &ColumnRules::default(),
        )
        .unwrap()
        .unwrap();
        assert!(sql.contains("`_peerdb_version` UInt64"), "{sql}");
        assert!(
            sql.contains("`_peerdb_synced_at` DateTime64(6, 'UTC')"),
            "{sql}"
        );
        assert!(sql.contains("`_peerdb_is_deleted` Bool"), "{sql}");
        assert!(
            sql.contains("ReplacingMergeTree(`_peerdb_version`, `_peerdb_is_deleted`)"),
            "{sql}"
        );
        assert!(!sql.contains("`_lsn`"), "{sql}");
    }

    #[test]
    fn render_create_table_without_delete_marker() {
        // No marker column, so nothing to hand ReplacingMergeTree as its
        // deletion arg; keyless tables still sort on the LSN column
        let sys = SystemColumns {
            is_deleted: None,
            ..SystemColumns::default()
        };
        let d = desc("t", vec![att(1, "id", INT4OID, true, None)], None);
        let sql = render_create_table(
            &d,
            &dest("default", &d),
            &CreateShape {
                system: sys.clone().into(),
                soft_delete: false,
                order_by: &[],
                primary_key: &[],
            },
            &ColumnRules::default(),
        )
        .unwrap()
        .unwrap();
        assert!(!sql.contains("_is_deleted"), "{sql}");
        assert!(sql.contains("ENGINE = ReplacingMergeTree(`_lsn`)"), "{sql}");
        assert!(sql.ends_with("ORDER BY (`_lsn`)"), "{sql}");
    }

    #[test]
    fn render_create_table_takes_operator_order_by_over_pk() {
        let d = desc(
            "t",
            vec![
                att(1, "id", INT4OID, true, None),
                att(2, "tenant", INT4OID, true, None),
            ],
            Some(vec![1]),
        );
        let order_by = vec!["tenant".to_string(), "id".to_string()];
        let sql = render_create_table(
            &d,
            &dest("default", &d),
            &CreateShape {
                system: SYS.clone(),
                soft_delete: false,
                order_by: &order_by,
                primary_key: &[],
            },
            &ColumnRules::default(),
        )
        .unwrap()
        .unwrap();
        assert!(sql.ends_with("ORDER BY (`tenant`, `id`)"), "{sql}");
    }

    #[test]
    fn render_create_table_order_by_ignored_when_column_absent_or_nullable() {
        let d = desc(
            "t",
            vec![
                att(1, "id", INT4OID, true, None),
                att(2, "body", TEXTOID, false, None),
            ],
            Some(vec![1]),
        );
        for keys in [vec!["nope".to_string()], vec!["body".to_string()]] {
            let sql = render_create_table(
                &d,
                &dest("default", &d),
                &CreateShape {
                    system: SYS.clone(),
                    soft_delete: false,
                    order_by: &keys,
                    primary_key: &[],
                },
                &ColumnRules::default(),
            )
            .unwrap()
            .unwrap();
            assert!(sql.ends_with("ORDER BY (`id`)"), "{keys:?}: {sql}");
        }
    }

    #[test]
    fn render_create_table_primary_key_prefix_renders_non_prefix_drops() {
        let d = desc(
            "t",
            vec![
                att(1, "id", INT4OID, true, None),
                att(2, "tenant", INT4OID, true, None),
            ],
            Some(vec![1]),
        );
        let order_by = vec!["tenant".to_string(), "id".to_string()];
        let mk = |primary_key: &[String]| {
            render_create_table(
                &d,
                &dest("default", &d),
                &CreateShape {
                    system: SYS.clone(),
                    soft_delete: false,
                    order_by: &order_by,
                    primary_key,
                },
                &ColumnRules::default(),
            )
            .unwrap()
            .unwrap()
        };
        let sql = mk(&["tenant".to_string()]);
        assert!(
            sql.ends_with("ORDER BY (`tenant`, `id`)\nPRIMARY KEY (`tenant`)"),
            "{sql}"
        );
        // `id` is not a prefix of (tenant, id): CH would reject the CREATE
        let sql = mk(&["id".to_string()]);
        assert!(sql.ends_with("ORDER BY (`tenant`, `id`)"), "{sql}");
    }

    #[test]
    fn render_create_table_from_mapping_takes_operator_order_by() {
        let d = desc("t", vec![att(1, "id", INT4OID, true, None)], Some(vec![1]));
        let m = TableMapping {
            target: TableTarget::new("db", "t"),
            columns: vec![
                ColumnMapping {
                    src_attnum: 1,
                    target_name: "order_id".into(),
                    target_type: "Int32".into(),
                },
                ColumnMapping {
                    src_attnum: 2,
                    target_name: "tenant".into(),
                    target_type: "Int32".into(),
                },
            ],
        };
        let order_by = vec!["tenant".to_string(), "order_id".to_string()];
        let sql = render_create_table_from_mapping(
            &d,
            &m,
            &CreateShape {
                system: SYS.clone(),
                soft_delete: false,
                order_by: &order_by,
                primary_key: &["tenant".to_string()],
            },
        );
        assert!(
            sql.ends_with("ORDER BY (`tenant`, `order_id`)\nPRIMARY KEY (`tenant`)"),
            "{sql}"
        );
    }

    #[test]
    fn drop_table_strategy_parses() {
        assert_eq!(
            "retain".parse::<DropTableStrategy>().unwrap(),
            DropTableStrategy::Retain
        );
        assert_eq!(
            "Drop".parse::<DropTableStrategy>().unwrap(),
            DropTableStrategy::Drop
        );
        assert_eq!(
            "warn".parse::<DropTableStrategy>().unwrap(),
            DropTableStrategy::Warn
        );
        assert!("bogus".parse::<DropTableStrategy>().is_err());
    }

    #[test]
    fn render_create_table_skips_when_type_unbridged() {
        // type_bridge falls back to String for unknown OIDs today, so
        // this never hits None; revisit if the bridge grows strictness
        let d = desc("t", vec![att(1, "id", 99999, true, None)], None);
        let sql = render_create_table(&d, &dest("db", &d), &shape(false), &ColumnRules::default())
            .unwrap();
        assert!(sql.is_some(), "fallback path keeps the CREATE renderable");
    }

    #[test]
    fn diff_renamed_then_added_then_dropped_in_correct_order() {
        // RENAME before ADD/DROP so position-match diffs don't trip into
        // a drop+add pair; functional verification in the integration test
        let diff = SchemaDiff {
            added_columns: vec![att(3, "c3", INT4OID, false, None)],
            dropped_columns: vec![2],
            renamed_columns: vec![(1, "old".into(), "new".into())],
            type_changes: vec![],
        };
        assert_eq!(diff.renamed_columns[0].0, 1);
        assert_eq!(diff.added_columns[0].attnum, 3);
        assert_eq!(diff.dropped_columns[0], 2);
    }

    fn orders_mapping() -> (RelName, ahash::HashMap<RelName, TableMapping>) {
        let rel = RelName::new("public", "orders");
        let map = [(
            rel.clone(),
            TableMapping {
                target: TableTarget::new("default", "orders"),
                columns: vec![ColumnMapping {
                    src_attnum: 1,
                    target_name: "id".into(),
                    target_type: "Int32".into(),
                }],
            },
        )]
        .into_iter()
        .collect();
        (rel, map)
    }

    #[tokio::test]
    async fn mapping_target_returns_pinned_table() {
        let (rel, map) = orders_mapping();
        let handle = crate::mapping::mapping_handle(map);
        let target = handle.with(|m| m.get(&rel).map(|t| t.target.clone())).await;
        assert_eq!(target, Some(TableTarget::new("default", "orders")));
    }

    #[tokio::test]
    async fn republish_mid_alter_fails_instead_of_panicking() {
        let (rel, map) = orders_mapping();
        let handle = crate::mapping::mapping_handle(map);
        let at = handle.snapshot().await;
        assert_eq!(
            mapping_columns_at(&handle, &rel, &at).await.unwrap().len(),
            1
        );
        handle.publish(Arc::new(ahash::HashMap::default())).await;
        let err = mapping_columns_at(&handle, &rel, &at)
            .await
            .expect_err("republish invalidates the resolved target");
        assert!(matches!(err, EmitterError::Config(_)), "{err}");
    }

    #[tokio::test]
    async fn prediction_ignores_a_republish_under_the_frozen_version() {
        let (rel, map) = orders_mapping();
        let handle = crate::mapping::mapping_handle(map);
        let frozen = handle.snapshot().await;
        handle.publish(Arc::new(ahash::HashMap::default())).await;
        let cfg = DdlConfig {
            drop_table_strategy: DropTableStrategy::Drop,
            auto_create_namespaces: HashSet::new(),
            replicate_all: false,
            runtime_config_schema: None,
            target_database: "default".into(),
            namespaces: ahash::HashMap::default(),
            soft_delete: false,
            system: Arc::default(),
            rules: Arc::default(),
            column_rules: Arc::default(),
        };
        let dropped = SchemaEvent::Dropped {
            oid: 16400,
            rel_name: rel.clone(),
        };
        let predicted = predict_route_effect(&cfg, &frozen, &dropped, false).unwrap();
        assert!(
            matches!(predicted, Some((r, None)) if r == rel),
            "frozen version still maps the rel"
        );
        let live = handle.snapshot().await;
        assert!(
            predict_route_effect(&cfg, &live, &dropped, false)
                .unwrap()
                .is_none(),
            "live handle moved on"
        );
    }

    #[tokio::test]
    async fn diff_fold_adds_column_and_skips_unmapped_rel() {
        let (_, map) = orders_mapping();
        let handle = crate::mapping::mapping_handle(map);
        let new = desc(
            "orders",
            vec![
                att(1, "id", INT4OID, true, None),
                att(2, "c", TEXTOID, false, None),
            ],
            Some(vec![1]),
        );
        let diff = SchemaDiff {
            added_columns: vec![att(2, "c", TEXTOID, false, None)],
            dropped_columns: vec![],
            renamed_columns: vec![],
            type_changes: vec![],
        };
        mutate_mapping_for_diff(&handle, &new, &diff, &ColumnRules::default()).await;
        let folded = handle
            .with(|m| {
                m.get(&RelName::new("public", "orders"))
                    .unwrap()
                    .columns
                    .iter()
                    .any(|c| c.src_attnum == 2 && c.target_name == "c")
            })
            .await;
        assert!(folded);

        // Unmapped relation: early return
        let ghost = desc("ghost", vec![att(1, "id", INT4OID, true, None)], None);
        mutate_mapping_for_diff(&handle, &ghost, &diff, &ColumnRules::default()).await;
    }
}
