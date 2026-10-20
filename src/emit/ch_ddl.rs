//! CH-side DDL applicator. Translates each [`SchemaEvent`] into CH SQL:
//!
//! | event | CH SQL |
//! |---|---|
//! | `Added` | `CREATE TABLE IF NOT EXISTS …` (namespace `auto_create = true`; a mapped rel re-creates its dest when strategy = drop, then `ALTER TABLE … ADD COLUMN IF NOT EXISTS …` per mapped column so a dest predating the mapping gains columns routed since) |
//! | `Changed.added_columns` | `ALTER TABLE … ADD COLUMN IF NOT EXISTS …` per column in attnum order |
//! | `Changed.renamed_columns` | `ALTER TABLE … RENAME COLUMN IF EXISTS … TO …` after retypes |
//! | `Changed.dropped_columns` | `ALTER TABLE … DROP COLUMN IF EXISTS …` |
//! | `Changed.type_changes` | `MODIFY COLUMN` before renames; `DROP` then `ADD` for fallible casts during rewrite |
//! | `Dropped` | `DROP TABLE IF EXISTS …` gated on [`DropTableStrategy`] |
//!
//! Each statement also runs against every tee of the relation.
//!
//! A partition routed to its root has no separate destination. Apply row
//! removal for `DETACH`, `DROP TABLE`, and `TRUNCATE` to root's destination
//! using source partition OID.
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

use std::borrow::Cow;
use std::sync::Arc;

use clickhouse_c::Kind;
use tokio::sync::watch;

use crate::catalog::type_bridge::{self, ResolvedColumn};
use crate::ch::{
    ChConn, EmitterError, connect_creating_database, exec_drain, is_retryable, quote_ident, types,
};
use crate::column_rules::ColumnRules;
use crate::config::{ConfigResolver, DestEmitter, ResolvedConfig};
#[cfg(feature = "test-support")]
use crate::crash_test;
use crate::decode::heap_decoder::{self, ColumnValue};
use crate::emit::ch_emitter::{DATETIME64_PG_EPOCH_US, EmitterConfig};
use crate::emit::route::{Routing, routing};
use crate::mapping::{
    ColumnMapping, DropTableStrategy, MappingHandle, MappingSnapshot, NameTemplate,
    NamespaceMapping, Retype, RetypeKind, SystemColumns, TableMapping, TableTarget, Tee,
    apply_column_rule, fold_diff_into_mapping, named_instance, retyped_target,
};
use crate::ops::oracle::{Oracle, OracleCell};
use crate::runtime_config::InitialLoadMode;
use crate::schema::{
    DroppedPartition, INT2OID, INT4OID, INT8OID, MissingDefault, RelAttr, RelDescriptor, RelName,
    ReplIdent, SchemaDiff, SchemaEvent, replident_key_attnums,
};
use crate::table_rules::{PartitionRoute, TableRule, TableRules};
use ahash::{HashMap, HashSet};
use tokio_postgres::types::Oid;

/// Knobs that don't ride the INSERT pump. [`DdlApplicator`] rebuilds them
/// from a republished [`ResolvedConfig`] snapshot at each apply, so SIGHUP
/// (and the future overlay) retarget namespaces + drop strategy without a
/// restart.
#[derive(Debug, Clone)]
pub struct DdlConfig {
    pub instances: std::collections::BTreeMap<String, crate::emit::ch_emitter::InstanceConfig>,
    pub drop_table_strategy: DropTableStrategy,
    pub detach_strategy: DropTableStrategy,
    /// Namespaces whose `Added` events run `CREATE TABLE IF NOT EXISTS`
    /// automatically (`auto_create = true`)
    pub auto_create_namespaces: HashSet<String>,
    pub replicate_all: bool,
    /// `[stream] auto_create_name_all`, the template a namespace with no
    /// entry of its own derives from
    pub auto_create_name_all: Option<NameTemplate>,
    /// Excluded from `replicate_all` so the overlay's `config_*` tables aren't swept in
    pub runtime_config_schema: Option<String>,
    /// CH database DDL targets when neither per-table mapping nor source
    /// namespace overrides the destination
    pub target_database: String,
    /// Source database name, for `$database$` in a namespace's
    /// `auto_create_name`
    pub source_database: String,
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

/// Knobs the resolver does not republish, so a caller threads them through
/// each [`DdlConfig::from_resolved`] unchanged.
#[derive(Debug, Clone)]
pub struct DdlBoot {
    pub target_database: String,
    pub source_database: String,
    pub soft_delete: bool,
    pub system: Arc<SystemColumns>,
    pub replicate_all: bool,
    pub auto_create_name_all: Option<NameTemplate>,
    pub runtime_config_schema: Option<String>,
}

impl DdlBoot {
    pub fn from_emitter(cfg: &crate::emit::ch_emitter::EmitterConfig) -> Self {
        Self {
            target_database: cfg.conn.database.clone(),
            source_database: cfg.source.dbname.clone(),
            soft_delete: cfg.soft_delete,
            system: cfg.system_columns.clone(),
            replicate_all: cfg.replicate_all,
            auto_create_name_all: cfg.auto_create_name_all.clone(),
            runtime_config_schema: cfg.runtime_config_schema.clone(),
        }
    }
}

impl DdlConfig {
    /// Build from a resolved snapshot and the boot-only knobs beside it.
    pub fn from_resolved(resolved: &ResolvedConfig, boot: DdlBoot) -> Self {
        let DdlBoot {
            target_database,
            source_database,
            soft_delete,
            system,
            replicate_all,
            auto_create_name_all,
            runtime_config_schema,
        } = boot;
        let auto_create_namespaces: HashSet<String> = resolved
            .namespaces
            .iter()
            .filter(|(_, v)| v.auto_create)
            .map(|(k, _)| k.clone())
            .collect();
        Self {
            instances: resolved.instances.clone(),
            drop_table_strategy: resolved.drop_table_strategy,
            detach_strategy: resolved.detach_strategy,
            auto_create_namespaces,
            replicate_all,
            auto_create_name_all,
            runtime_config_schema,
            target_database,
            source_database,
            namespaces: resolved.namespaces.clone(),
            soft_delete,
            system,
            rules: resolved.rules.clone(),
            column_rules: resolved.column_rules.clone(),
        }
    }

    pub fn boot(&self) -> DdlBoot {
        DdlBoot {
            target_database: self.target_database.clone(),
            source_database: self.source_database.clone(),
            soft_delete: self.soft_delete,
            system: self.system.clone(),
            replicate_all: self.replicate_all,
            auto_create_name_all: self.auto_create_name_all.clone(),
            runtime_config_schema: self.runtime_config_schema.clone(),
        }
    }

    fn target_database_for(&self, namespace: &str, instance: Option<&str>) -> &str {
        crate::emit::ch_emitter::default_database(
            &self.namespaces,
            &self.instances,
            &self.target_database,
            namespace,
            instance,
        )
    }

    fn derived_table_for(&self, rel: &RelName) -> String {
        self.namespaces
            .get(&*rel.namespace)
            .and_then(|n| n.auto_create_name.clone())
            .or_else(|| self.auto_create_name_all.clone())
            .unwrap_or_default()
            .render(&self.source_database, &rel.namespace, &rel.name)
    }

    fn drop_strategy_for(&self, namespace: &str) -> DropTableStrategy {
        self.namespaces
            .get(namespace)
            .and_then(|n| n.drop_table_strategy)
            .unwrap_or(self.drop_table_strategy)
    }

    fn detach_strategy_for(&self, namespace: &str) -> DropTableStrategy {
        self.namespaces
            .get(namespace)
            .and_then(|n| n.detach_strategy)
            .unwrap_or(self.detach_strategy)
    }

    /// Resolve rules for a relation with its own destination. Partitions
    /// get separate destinations through `route = "self"`
    pub(crate) fn rule_for(&self, desc: &RelDescriptor) -> TableRule {
        self.rules.settings_for(desc)
    }

    /// Resolve rules for destination owner `root`. Ancestor descriptors
    /// omit their own ancestors; use `ancestors` to identify nested roots
    fn owner_rule(&self, ancestors: &[Arc<RelDescriptor>], root: &RelDescriptor) -> TableRule {
        if ancestors.last().is_some_and(|top| top.oid != root.oid) {
            self.rules.partition_settings(&root.rel_name)
        } else {
            self.rules.settings(&root.rel_name)
        }
    }

    pub fn with_drop_strategy(mut self, s: DropTableStrategy) -> Self {
        self.drop_table_strategy = s;
        self
    }

    fn create_target(&self, settings: &TableRule, rel: &RelName) -> TableTarget {
        TableTarget {
            instance: settings.target_instance.clone().and_then(named_instance),
            database: settings.target_database.clone().unwrap_or_else(|| {
                self.target_database_for(&rel.namespace, settings.target_instance.as_deref())
                    .to_owned()
            }),
            table: settings
                .target_table
                .clone()
                .unwrap_or_else(|| self.derived_table_for(rel)),
        }
    }

    /// Destination shape for one relation's `CREATE TABLE`
    pub(crate) fn create_shape<'a>(&self, settings: &'a TableRule) -> CreateShape<'a> {
        CreateShape {
            system: settings.system_columns(&self.system),
            soft_delete: self.soft_delete,
            order_by: settings.order_by.as_deref().unwrap_or_default(),
            primary_key: settings.primary_key.as_deref().unwrap_or_default(),
            engine: settings.engine.as_deref(),
            partition_by: settings.partition_by.as_deref(),
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

    /// Explicit `replicate` wins, except system relations never opt in.
    /// Partitions need `route = "self"` to get their own destinations
    fn auto_creates(&self, desc: &RelDescriptor) -> bool {
        if !matches!(routing(desc, Some(&self.rules)), Routing::Own) {
            return false;
        }
        let rel = &desc.rel_name;
        let system = is_system_namespace(&rel.namespace, self.runtime_config_schema.as_deref());
        match self.rule_for(desc).replicate {
            Some(true) if !system => true,
            Some(false) => false,
            _ => {
                self.auto_create_namespaces.contains(&*rel.namespace)
                    || (self.replicate_all && !system)
            }
        }
    }
}

/// Source position used to version delete markers from schema changes
#[derive(Debug, Clone, Copy, Default)]
pub struct Stamp {
    pub lsn: u64,
    pub xid: u32,
    /// Microseconds since the PostgreSQL epoch
    pub commit_ts: i64,
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
    /// Operator engine, `None` for `ReplacingMergeTree`
    pub engine: Option<&'a str>,
    /// Configured `PARTITION BY` expression, used as written
    pub partition_by: Option<&'a str>,
}

impl CreateShape<'_> {
    /// Reuse main destination system columns with tee engine and keys
    fn for_tee<'b>(&self, tee: &'b Tee) -> CreateShape<'b> {
        CreateShape {
            system: self.system.clone(),
            soft_delete: self.soft_delete,
            order_by: &tee.order_by,
            primary_key: &tee.primary_key,
            engine: tee.engine.as_deref(),
            partition_by: None,
        }
    }
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
    client: ChConn,
    config: DdlConfig,
    /// Live config layers. `refresh_config` folds a republished snapshot
    /// into `config` (namespaces + drop strategy) at each apply, so SIGHUP
    /// and the future overlay retarget DDL without a restart.
    config_rx: watch::Receiver<Arc<ResolvedConfig>>,
    mapping: MappingHandle,
    /// Owner of runtime-derived mapping state. Set: auto-created mappings,
    /// diff folds, and DROP forgets record into the resolver so the
    /// republish full-swap preserves them. Unset (bootstrap drain, tests
    /// without a resolver): mutate the live handle directly — no republish
    /// runs in those contexts, so nothing clobbers the write.
    resolver: Option<Arc<ConfigResolver>>,
    /// Shadow PG renderer for tier-3 fast defaults
    oracle: Option<Arc<Oracle>>,
    ensured_databases: HashMap<(Option<String>, String), Arc<EmitterConfig>>,
    /// Last destination truncated in full, with source record position.
    /// Partitions share this record and need no separate delete markers
    truncated: Option<(u64, TableTarget)>,
    pub stats: DdlStats,
}

#[derive(Debug, Default, Clone)]
pub struct DdlStats {
    pub alters_applied: u64,
    pub creates_applied: u64,
    pub drops_applied: u64,
    /// Events received but skipped (no mapping, no auto_create, drop
    /// strategy = Retain)
    pub skipped: u64,
}

impl DdlApplicator {
    pub async fn new(
        dest: Arc<DestEmitter>,
        ddl_cfg: DdlConfig,
        mapping: MappingHandle,
        config_rx: watch::Receiver<Arc<ResolvedConfig>>,
    ) -> Result<Self, EmitterError> {
        Ok(Self {
            client: ChConn::connect(dest).await?,
            config: ddl_cfg,
            config_rx,
            mapping,
            resolver: None,
            oracle: None,
            ensured_databases: HashMap::default(),
            truncated: None,
            stats: DdlStats::default(),
        })
    }

    /// Route mapping writes through resolver so republished snapshots retain them
    pub fn with_resolver(mut self, resolver: Arc<ConfigResolver>) -> Self {
        self.resolver = Some(resolver);
        self
    }

    /// Use shadow PG to render tier-3 fast defaults for `ADD COLUMN`
    pub fn with_oracle(mut self, oracle: Option<Arc<Oracle>>) -> Self {
        self.oracle = oracle;
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
    fn refresh_config(&mut self) {
        if !self.config_rx.has_changed().unwrap_or(false) {
            return;
        }
        let snap = self.config_rx.borrow_and_update();
        self.config = DdlConfig::from_resolved(&snap, self.config.boot());
    }

    /// Create destination for a relation present before streaming
    pub async fn add(&mut self, desc: &RelDescriptor) -> Result<(), EmitterError> {
        self.refresh_config();
        let cfg = self.config.clone();
        self.apply_added(desc, &cfg).await
    }

    /// Apply under frozen config `frozen`, the version
    /// [`Self::predict_route_mapping`] planned with; `None` uses live config.
    /// `stamp` versions partition delete markers
    pub async fn apply_at(
        &mut self,
        event: &SchemaEvent,
        frozen: Option<&ResolvedConfig>,
        stamp: Stamp,
    ) -> Result<(), EmitterError> {
        self.refresh_config();
        let cfg = self.plan_config(frozen);
        match event {
            SchemaEvent::Added { desc } => self.apply_added(desc, &cfg).await,
            SchemaEvent::Changed { old, new, diff } => {
                self.apply_changed(old, new, diff, &cfg, stamp).await
            }
            SchemaEvent::Dropped {
                rel_name,
                partition,
                ..
            } => {
                self.apply_dropped(rel_name, partition.as_ref(), &cfg, stamp)
                    .await
            }
        }
    }

    async fn apply_added(
        &mut self,
        desc: &RelDescriptor,
        cfg: &DdlConfig,
    ) -> Result<(), EmitterError> {
        match routing(desc, Some(&cfg.rules)) {
            Routing::Own => {}
            Routing::Ancestor(root) => return self.admit_partition(desc, root, cfg).await,
            Routing::Excluded => {
                self.stats.skipped += 1;
                return Ok(());
            }
        }
        // Mapped dest created from the mapping when missing; IF NOT EXISTS
        // no-ops an operator-managed table and re-creates after strategy=drop.
        if let Some(m) = self.mapping_for(&desc.rel_name).await {
            let settings = cfg.rule_for(desc);
            let shape = cfg.create_shape(&settings);
            let cols = mapping_columns(&m);
            self.create_dest(desc, &cols, &m.target, &shape).await?;
            self.create_tees(desc, &m, &cols, &shape).await?;
            return Ok(());
        }
        // Operator opt-out (`replicate=false`) beats namespace auto_create:
        // no CH mirror, no mapping
        if self.is_excluded(desc).await {
            self.stats.skipped += 1;
            return Ok(());
        }
        let Some((cols, mapping)) = derive_added(cfg, desc) else {
            self.stats.skipped += 1;
            return Ok(());
        };
        let settings = cfg.rule_for(desc);
        let shape = cfg.create_shape(&settings);
        self.ensure_database(&mapping.target).await?;
        self.execute(
            &mapping.target,
            &render_create_table(desc, &cols, &mapping.target, &shape),
        )
        .await?;
        self.stats.creates_applied += 1;
        self.create_tees(desc, &mapping, &cols, &shape).await?;
        self.register_mapping(desc, mapping).await;
        Ok(())
    }

    /// Create destination if missing, then add any newly mapped columns
    async fn create_dest(
        &mut self,
        desc: &RelDescriptor,
        cols: &[DestColumn],
        target: &TableTarget,
        shape: &CreateShape<'_>,
    ) -> Result<(), EmitterError> {
        self.ensure_database(target).await?;
        self.execute(target, &render_create_table(desc, cols, target, shape))
            .await?;
        self.stats.creates_applied += 1;
        if let Some(sql) = render_add_columns(cols, target) {
            self.execute(target, &sql).await?;
            self.stats.alters_applied += 1;
        }
        if let Some(leaf) = leaf_column(desc, shape) {
            let sql = format!(
                "ALTER TABLE {} ADD COLUMN IF NOT EXISTS {} UInt32",
                target.sql(),
                quote_ident(leaf)
            );
            self.execute(target, &sql).await?;
            self.stats.alters_applied += 1;
        }
        Ok(())
    }

    /// Use identical columns so each destination accepts same INSERT block
    async fn create_tees(
        &mut self,
        desc: &RelDescriptor,
        mapping: &TableMapping,
        cols: &[DestColumn],
        shape: &CreateShape<'_>,
    ) -> Result<(), EmitterError> {
        for tee in &mapping.tees {
            let target = tee.target(&mapping.target);
            self.create_dest(desc, cols, &target, &shape.for_tee(tee))
                .await?;
        }
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
        self.refresh_config();
        let settings = self.config.rule_for(desc);
        let target = self.config.create_target(&settings, &desc.rel_name);
        let shape = self.config.create_shape(&settings);
        let Some(cols) = descriptor_columns(desc, &self.config.column_rules) else {
            tracing::warn!(
                target: "walshadow::ch_ddl",
                qname = %desc.rel_name,
                "opt-in skipped: no bridgeable CH shape",
            );
            self.stats.skipped += 1;
            return Ok(false);
        };
        self.ensure_database(&target).await?;
        self.execute(&target, &render_create_table(desc, &cols, &target, &shape))
            .await?;
        self.stats.creates_applied += 1;
        let mapping = TableMapping {
            target,
            columns: cols.iter().map(DestColumn::mapping).collect(),
            tees: settings.tee.clone().unwrap_or_default(),
        };
        self.create_tees(desc, &mapping, &cols, &shape).await?;
        Ok(true)
    }

    async fn apply_changed(
        &mut self,
        old: &RelDescriptor,
        new: &RelDescriptor,
        diff: &SchemaDiff,
        cfg: &DdlConfig,
        stamp: Stamp,
    ) -> Result<(), EmitterError> {
        if old.is_partition() || new.is_partition() {
            let before = routing(old, Some(&cfg.rules));
            let after = routing(new, Some(&cfg.rules));
            if let Routing::Ancestor(left) = before
                && !matches!(after, Routing::Ancestor(root) if root.oid == left.oid)
            {
                let strategy = cfg.detach_strategy_for(&left.rel_name.namespace);
                self.leave_root(old, left, strategy, "DETACH", cfg, stamp)
                    .await?;
            }
            match after {
                // Keep previous destination without further writes
                Routing::Ancestor(root) => return self.admit_partition(new, root, cfg).await,
                Routing::Excluded => {
                    self.stats.skipped += 1;
                    return Ok(());
                }
                // Schema changes while attached did not update old mapping
                Routing::Own if !matches!(before, Routing::Own) => {
                    if self.mapping_for(&new.rel_name).await.is_some() {
                        self.forget_mapping(&new.rel_name).await;
                    }
                    return self.apply_added(new, cfg).await;
                }
                Routing::Own => {}
            }
        }
        let key = new.rel_name.clone();
        let Some((mapped_at, mapping)) = self.mapping_snapshot(&key).await else {
            // No target, can't ALTER; `Added` handles the
            // not-yet-learned case
            self.stats.skipped += 1;
            return Ok(());
        };
        let targets: Vec<TableTarget> = mapping.targets().collect();
        // ClickHouse can reject key retypes; defer renames, additions, and drops
        ensure_unmoved(&self.mapping, &key, &mapped_at).await?;
        let columns = &mapping.columns;
        let retypes = plan_retypes(old, new, diff, columns, &cfg.column_rules);
        for clause in retypes.iter().flat_map(retype_clauses) {
            self.alter(&targets, &clause).await.map_err(|e| match e {
                EmitterError::ServerException {
                    code: ALTER_OF_COLUMN_IS_FORBIDDEN,
                    message,
                } => EmitterError::Config(format!(
                    "`{key}`: destination refused column type change, likely a \
                     sorting key column; rebuild destination manually: {message}"
                )),
                e => e,
            })?;
        }
        // RENAME before ADD/DROP so position-matched renames don't trip
        // a later diff into a drop+add pair
        for (_attnum, old_name, new_name) in &diff.renamed_columns {
            // Operator TOML rename makes the source rename a no-op from
            // CH's POV (TOML still maps src_attnum to the same CH name);
            // detect via whether the CH column name changed
            ensure_unmoved(&self.mapping, &key, &mapped_at).await?;
            let already_renamed = columns.iter().any(|c| &c.target_name == new_name)
                || !columns.iter().any(|c| &c.target_name == old_name);
            if already_renamed {
                // Pre-declared TOML mapping already encodes the rename
                continue;
            }
            // IF EXISTS keeps rename idempotent: reconnect+resend or
            // daemon-restart re-fire no-ops once CH has the renamed column
            let clause = format!(
                "RENAME COLUMN IF EXISTS {} TO {}",
                quote_ident(old_name),
                quote_ident(new_name)
            );
            self.alter(&targets, &clause).await?;
        }
        let oracle = self.oracle.clone();
        for att in &diff.added_columns {
            let pk_member = replident_key_attnums(new).contains(&att.attnum);
            let att = resolve_disk_default(oracle.as_deref(), new.rfn.db_node, att).await;
            let Ok(resolved) = type_bridge::map(&att, pk_member) else {
                // Unbridged type; operator TOML override is the recovery path
                self.stats.skipped += 1;
                continue;
            };
            let (name, resolved) = apply_column_rule(
                &att.name,
                resolved,
                cfg.column_rules.settings(&new.rel_name, &att.name),
            );
            self.alter(&targets, &add_column_clause(&name, &resolved))
                .await?;
        }
        // diff lists attnums only; resolve CH column name from old descriptor
        let dropped = old
            .attributes
            .iter()
            .filter(|a| diff.dropped_columns.contains(&a.attnum));
        for att in dropped {
            // Surface the drop on CH even if TOML still references the
            // column; emitter then encodes NULL for the vanished attnum
            let clause = format!("DROP COLUMN IF EXISTS {}", quote_ident(&att.name));
            self.alter(&targets, &clause).await?;
        }
        // Auto-extend the TableMapping so the emitter ships post-DDL
        // rows against the new shape without TOML edits; operator-pinned
        // `target_name` overrides survive (only touch entries the
        // applicator could have produced, by src_attnum match)
        self.fold_mapping_diff(new, diff, &retypes, cfg).await;
        Ok(())
    }

    async fn alter(&mut self, targets: &[TableTarget], clause: &str) -> Result<(), EmitterError> {
        for target in targets {
            self.execute(target, &format!("ALTER TABLE {} {clause}", target.sql()))
                .await?;
            self.stats.alters_applied += 1;
        }
        Ok(())
    }

    async fn apply_dropped(
        &mut self,
        rel: &RelName,
        partition: Option<&DroppedPartition>,
        cfg: &DdlConfig,
        stamp: Stamp,
    ) -> Result<(), EmitterError> {
        if let Some(p) = partition {
            match routing(&p.desc, Some(&cfg.rules)) {
                Routing::Own => {}
                Routing::Excluded => {
                    self.stats.skipped += 1;
                    return Ok(());
                }
                // Apply root's drop policy once for entire destination
                Routing::Ancestor(root) if p.dropped.contains(&root.oid) => {
                    self.stats.skipped += 1;
                    return Ok(());
                }
                Routing::Ancestor(root) => {
                    let strategy = cfg.drop_strategy_for(&root.rel_name.namespace);
                    return self
                        .leave_root(&p.desc, root, strategy, "DROP TABLE", cfg, stamp)
                        .await;
                }
            }
        }
        let Some((_, mapping)) = self.mapping_snapshot(rel).await else {
            self.stats.skipped += 1;
            return Ok(());
        };
        let target = &mapping.target;
        match cfg.drop_strategy_for(&rel.namespace) {
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
                for target in mapping.targets() {
                    let sql = format!("DROP TABLE IF EXISTS {}", target.sql());
                    self.execute(&target, &sql).await?;
                    self.stats.drops_applied += 1;
                }
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
    /// the otherwise out-of-order pipeline. A partition routed to its root
    /// adds delete markers for its rows there
    pub async fn truncate(&mut self, desc: &RelDescriptor, at: Stamp) -> Result<(), EmitterError> {
        self.refresh_config();
        let cfg = self.config.clone();
        match routing(desc, Some(&cfg.rules)) {
            Routing::Own => {}
            Routing::Excluded => return Ok(()),
            // A partitioned relation holds no rows; its partitions share
            // the record
            Routing::Ancestor(_) if desc.kind == 'p' => return Ok(()),
            Routing::Ancestor(root) => {
                let Some(mapping) = self.mapping_for(&root.rel_name).await else {
                    return Ok(());
                };
                if self
                    .truncated
                    .as_ref()
                    .is_some_and(|(lsn, t)| *lsn == at.lsn && *t == mapping.target)
                {
                    return Ok(());
                }
                return self
                    .remove_partition_rows(root, desc, &mapping, at, "TRUNCATE", &cfg)
                    .await;
            }
        }
        let Some((_, mapping)) = self.mapping_snapshot(&desc.rel_name).await else {
            return Ok(());
        };
        for target in mapping.targets() {
            self.execute(&target, &format!("TRUNCATE TABLE {}", target.sql()))
                .await?;
        }
        self.truncated = Some((at.lsn, mapping.target));
        Ok(())
    }

    /// Check routing requirements. Return `false` for excluded partitions
    /// and partitions using root's destination
    pub async fn owns_destination(&mut self, desc: &RelDescriptor) -> Result<bool, EmitterError> {
        self.refresh_config();
        let cfg = self.config.clone();
        match routing(desc, Some(&cfg.rules)) {
            Routing::Own => Ok(true),
            Routing::Excluded => Ok(false),
            Routing::Ancestor(root) => self.admit_partition(desc, root, &cfg).await.map(|()| false),
        }
    }

    /// Use root's `initial_load` for attached rows, defaulting to `copy`
    pub async fn partition_load(&mut self, desc: &RelDescriptor) -> InitialLoadMode {
        self.refresh_config();
        let cfg = self.config.clone();
        let Routing::Ancestor(root) = routing(desc, Some(&cfg.rules)) else {
            return InitialLoadMode::None;
        };
        if self.mapping_for(&root.rel_name).await.is_none() {
            return InitialLoadMode::None;
        }
        cfg.owner_rule(&desc.ancestors, root)
            .initial_load
            .and_then(|m| m.parse().ok())
            .or_else(|| {
                cfg.namespaces
                    .get(&*root.rel_name.namespace)
                    .and_then(|n| n.initial_load)
            })
            .unwrap_or(InitialLoadMode::Copy)
    }

    /// Check whether a partition can use `root`'s destination.
    /// Skip check until that destination exists
    async fn admit_partition(
        &mut self,
        desc: &RelDescriptor,
        root: &RelDescriptor,
        cfg: &DdlConfig,
    ) -> Result<(), EmitterError> {
        if self.mapping_for(&root.rel_name).await.is_none() {
            self.stats.skipped += 1;
            return Ok(());
        }
        check_partition(desc, root, &cfg.rules)
    }

    /// Retain or remove departing partition's rows according to `strategy`
    async fn leave_root(
        &mut self,
        leaving: &RelDescriptor,
        root: &RelDescriptor,
        strategy: DropTableStrategy,
        what: &str,
        cfg: &DdlConfig,
        at: Stamp,
    ) -> Result<(), EmitterError> {
        let rel = &leaving.rel_name;
        let Some(mapping) = self.mapping_for(&root.rel_name).await else {
            self.stats.skipped += 1;
            return Ok(());
        };
        match strategy {
            DropTableStrategy::Retain => {
                self.stats.skipped += 1;
                tracing::info!(
                    target: "walshadow::ch_ddl",
                    partition = %rel,
                    root = %root.rel_name,
                    dest = %mapping.target,
                    "{what} of partition; rows retained in root destination",
                );
                Ok(())
            }
            DropTableStrategy::Warn => {
                self.stats.skipped += 1;
                tracing::warn!(
                    target: "walshadow::ch_ddl",
                    partition = %rel,
                    root = %root.rel_name,
                    dest = %mapping.target,
                    "{what} of partition; rows retained in root destination",
                );
                Ok(())
            }
            DropTableStrategy::Drop => {
                self.remove_partition_rows(root, leaving, &mapping, at, what, cfg)
                    .await
            }
        }
    }

    /// Remove partition `leaf`'s rows from `root`'s destinations before `at`.
    /// Insert delete markers where supported; use lightweight `DELETE` on
    /// other MergeTree engines. Limit removal to `at.lsn` so replay writes
    /// identical markers and preserves later rows
    async fn remove_partition_rows(
        &mut self,
        root: &RelDescriptor,
        leaving: &RelDescriptor,
        mapping: &TableMapping,
        at: Stamp,
        what: &str,
        cfg: &DdlConfig,
    ) -> Result<(), EmitterError> {
        let settings = cfg.owner_rule(&leaving.ancestors, root);
        let (root, leaf) = (&root.rel_name, leaving.oid);
        let system = settings.system_columns(&cfg.system);
        let Some(leaf_col) = system.leaf.as_deref() else {
            return Err(EmitterError::Config(format!(
                "{what} of partition {leaf} of `{root}` needs the partition column; \
                 re-enable `leaf` in [system_columns] or on `{root}`"
            )));
        };
        let destinations = std::iter::once((mapping.target.clone(), settings.engine.clone()))
            .chain(
                mapping
                    .tees
                    .iter()
                    .map(|t| (t.target(&mapping.target), t.engine.clone())),
            );
        for (target, engine) in destinations {
            let Some(sql) = partition_removal_sql(
                &target,
                engine.as_deref(),
                mapping,
                &system,
                leaf_col,
                leaf,
                at,
            ) else {
                continue;
            };
            self.execute(&target, &sql).await?;
            self.stats.alters_applied += 1;
        }
        Ok(())
    }

    /// Resolve mapping against a held snapshot, which later ALTER steps
    /// re-check for displacement
    async fn mapping_snapshot(&mut self, rel: &RelName) -> Option<(MappingSnapshot, TableMapping)> {
        let at = self.mapping.snapshot().await;
        let mapping = at.get(rel).map(|m| m.as_ref().clone())?;
        Some((at, mapping))
    }

    async fn mapping_for(&mut self, rel: &RelName) -> Option<TableMapping> {
        self.mapping
            .with(|m| m.get(rel).map(|t| t.as_ref().clone()))
            .await
    }

    /// Operator opt-out (`replicate=false`); nothing excluded without a
    /// resolver. `&mut self` like siblings: `&self` across await would
    /// demand `DdlApplicator: Sync`, blocked by chc client's raw pointer
    async fn is_excluded(&mut self, desc: &RelDescriptor) -> bool {
        if let Some(r) = &self.resolver {
            r.is_excluded(desc).await
        } else {
            false
        }
    }

    /// Route writes through resolver so derived mappings survive republish
    async fn register_mapping(&mut self, desc: &RelDescriptor, mapping: TableMapping) {
        if let Some(r) = &self.resolver {
            r.register_derived_mapping(desc, mapping).await;
        } else {
            self.mapping
                .mutate(|m| Arc::make_mut(m).insert(desc.rel_name.clone(), Arc::new(mapping)))
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
            SchemaEvent::Added { desc } | SchemaEvent::Changed { new: desc, .. }
                if !mapping.contains_key(&desc.rel_name) =>
            {
                self.is_excluded(desc).await
            }
            _ => false,
        };
        predict_route_effect(&self.plan_config(config), mapping, event, excluded)
    }

    fn plan_config(&self, frozen: Option<&ResolvedConfig>) -> DdlConfig {
        frozen
            .map(|rc| DdlConfig::from_resolved(rc, self.config.boot()))
            .unwrap_or_else(|| self.config.clone())
    }

    async fn fold_mapping_diff(
        &mut self,
        new: &RelDescriptor,
        diff: &SchemaDiff,
        retypes: &[Retype],
        cfg: &DdlConfig,
    ) {
        if let Some(r) = &self.resolver {
            r.apply_schema_diff(new, diff, retypes).await;
        } else {
            mutate_mapping_for_diff(&self.mapping, new, diff, retypes, &cfg.column_rules).await;
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
    async fn ensure_database(&mut self, target: &TableTarget) -> Result<(), EmitterError> {
        self.client.select(target.instance.as_deref())?;
        let config = self.client.config();
        let key = (target.instance.clone(), target.database.clone());
        if self
            .ensured_databases
            .get(&key)
            .is_some_and(|old| Arc::ptr_eq(old, &config))
        {
            return Ok(());
        }
        let sql = format!(
            "CREATE DATABASE IF NOT EXISTS {}",
            quote_ident(&target.database)
        );
        self.execute_selected(&sql).await?;
        self.ensured_databases.insert(key, config);
        Ok(())
    }

    async fn execute(&mut self, target: &TableTarget, sql: &str) -> Result<(), EmitterError> {
        self.client.select(target.instance.as_deref())?;
        self.execute_selected(sql).await
    }

    async fn execute_selected(&mut self, sql: &str) -> Result<(), EmitterError> {
        tracing::debug!(target: "walshadow::ch_ddl", sql = %sql, "applying");
        let query_timeout = self.client.config().insert_timeout;
        self.client
            .retry_when(
                |e| {
                    is_retryable(e)
                        && !matches!(
                            e,
                            EmitterError::ServerException {
                                code: ALTER_OF_COLUMN_IS_FORBIDDEN,
                                ..
                            }
                        )
                },
                |mut client| async move {
                    let result = exec_drain(&mut client, sql, query_timeout).await;
                    (client, result)
                },
                |e, attempt| {
                    tracing::warn!(
                        target: "walshadow::ch_ddl",
                        error = %e, attempt, sql = %sql,
                        "DDL attempt failed; reconnecting + retrying",
                    );
                },
            )
            .await?;
        #[cfg(feature = "test-support")]
        if sql.starts_with("ALTER TABLE ") {
            crash_test::hit(crash_test::Point::AfterAlter, u64::MAX);
        }
        Ok(())
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
            if mapping.contains_key(&desc.rel_name) || excluded {
                return Ok(None);
            }
            Ok(derive_added(cfg, desc).map(|(_, m)| (desc.rel_name.clone(), Some(m))))
        }
        SchemaEvent::Changed { old, new, diff } => {
            if old.is_partition() || new.is_partition() {
                match routing(new, Some(&cfg.rules)) {
                    Routing::Ancestor(_) | Routing::Excluded => return Ok(None),
                    Routing::Own if !matches!(routing(old, Some(&cfg.rules)), Routing::Own) => {
                        if mapping.contains_key(&new.rel_name) || excluded {
                            return Ok(None);
                        }
                        return Ok(
                            derive_added(cfg, new).map(|(_, m)| (new.rel_name.clone(), Some(m)))
                        );
                    }
                    Routing::Own => {}
                }
            }
            let Some(mut m) = mapping.get(&new.rel_name).map(|m| m.as_ref().clone()) else {
                return Ok(None);
            };
            let retypes = plan_retypes(old, new, diff, &m.columns, &cfg.column_rules);
            fold_diff_into_mapping(&mut m, new, diff, &retypes, &cfg.column_rules);
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

/// `CREATE TABLE` plus row-routing mapping `Added` derives for an unmapped,
/// unexcluded rel, so rows and DDL land in same place. `None` when out of
/// scope or without a bridgeable shape
fn derive_added(cfg: &DdlConfig, desc: &RelDescriptor) -> Option<(Vec<DestColumn>, TableMapping)> {
    if !cfg.auto_creates(desc) {
        return None;
    }
    let settings = cfg.rule_for(desc);
    let cols = descriptor_columns(desc, &cfg.column_rules)?;
    let mapping = TableMapping {
        target: cfg.create_target(&settings, &desc.rel_name),
        columns: cols.iter().map(DestColumn::mapping).collect(),
        tees: settings.tee.clone().unwrap_or_default(),
    };
    Some((cols, mapping))
}

/// Reject partitions with separate destination settings, missing WAL, or
/// delete records missing columns required by root's destination key
fn check_partition(
    desc: &RelDescriptor,
    root: &RelDescriptor,
    rules: &TableRules,
) -> Result<(), EmitterError> {
    let below = std::iter::once(desc).chain(
        desc.ancestors
            .iter()
            .map(Arc::as_ref)
            .take_while(|a| a.oid != root.oid),
    );
    for node in below {
        let exact = rules.exact_settings(&node.rel_name);
        if exact.route != Some(PartitionRoute::Self_) && exact.shapes_destination() {
            return Err(EmitterError::Config(format!(
                "`{}` routes to partitioned table `{}`; its entry may set only `replicate` \
                 and `route`, or `route = \"self\"` to own a destination",
                node.rel_name, root.rel_name
            )));
        }
    }
    if desc.persistence == 'u' {
        return Err(EmitterError::Config(format!(
            "unlogged partition `{}` of `{}` writes no WAL; make it logged or exclude it \
             with `replicate = false`",
            desc.rel_name, root.rel_name
        )));
    }
    let covered = match (key_columns(root), logged_columns(desc)) {
        (KeyColumns::Absent, _) | (_, KeyColumns::WholeRow) => true,
        (KeyColumns::WholeRow, _) | (_, KeyColumns::Absent) => false,
        (KeyColumns::Named(key), KeyColumns::Named(logged)) => {
            key.iter().all(|k| logged.contains(k))
        }
    };
    if covered {
        return Ok(());
    }
    let hint = if matches!(root.replident, ReplIdent::Full { pk_attnums: None }) {
        "; REPLICA IDENTITY does not recurse, so set FULL on each partition too"
    } else {
        ""
    };
    Err(EmitterError::Config(format!(
        "partition `{}` logs deletes by fewer columns than `{}` keys its destination on{hint}",
        desc.rel_name, root.rel_name
    )))
}

enum KeyColumns<'a> {
    Absent,
    WholeRow,
    Named(Vec<&'a str>),
}

fn named_columns<'a>(desc: &'a RelDescriptor, attnums: &[i16]) -> KeyColumns<'a> {
    KeyColumns::Named(
        attnums
            .iter()
            .filter_map(|n| desc.live_attr(*n).map(|a| a.name.as_str()))
            .collect(),
    )
}

/// Sort columns derived from `desc`'s replica identity
fn key_columns(desc: &RelDescriptor) -> KeyColumns<'_> {
    match &desc.replident {
        ReplIdent::Full { pk_attnums: None } => KeyColumns::WholeRow,
        ReplIdent::Default { pk_attnums: None } | ReplIdent::Nothing => KeyColumns::Absent,
        _ => named_columns(desc, replident_key_attnums(desc)),
    }
}

/// Columns PostgreSQL logs in `desc`'s delete and key-changing update records
fn logged_columns(desc: &RelDescriptor) -> KeyColumns<'_> {
    if matches!(desc.replident, ReplIdent::Full { .. }) {
        KeyColumns::WholeRow
    } else {
        key_columns(desc)
    }
}

/// Extract engine name, defaulting to `ReplacingMergeTree`
fn engine_family(engine: Option<&str>) -> &str {
    engine.map_or("ReplacingMergeTree", |e| {
        e.split('(').next().unwrap_or_default().trim()
    })
}

/// Build SQL to remove partition `leaf`'s rows before `at.lsn` from `target`.
/// Return `None` for engines without row removal, such as `Null`
fn partition_removal_sql(
    target: &TableTarget,
    engine: Option<&str>,
    mapping: &TableMapping,
    system: &SystemColumns,
    leaf_col: &str,
    leaf: Oid,
    at: Stamp,
) -> Option<String> {
    let family = engine_family(engine);
    if !family.ends_with("MergeTree") {
        return None;
    }
    let t = target.sql();
    let leaf_col = quote_ident(leaf_col);
    let lsn = quote_ident(&system.lsn);
    let scope = format!("{leaf_col} = {leaf} AND {lsn} < {}", at.lsn);
    let Some(marker) = system
        .is_deleted
        .as_deref()
        .filter(|_| family.ends_with("ReplacingMergeTree"))
    else {
        return Some(format!("DELETE FROM {t} WHERE {scope}"));
    };
    let marker = quote_ident(marker);
    let list = mapping
        .columns
        .iter()
        .map(|c| quote_ident(&c.target_name))
        .chain([leaf_col])
        .collect::<Vec<_>>()
        .join(", ");
    let unix_us = at.commit_ts.saturating_add(DATETIME64_PG_EPOCH_US);
    Some(format!(
        "INSERT INTO {t} ({list}, {lsn}, {}, {}, {marker}) \
         SELECT {list}, toUInt64({}), toUInt32({}), fromUnixTimestamp64Micro(toInt64({unix_us}), 'UTC'), true \
         FROM {t} FINAL WHERE {scope} AND NOT {marker}",
        quote_ident(&system.xid),
        quote_ident(&system.commit_ts),
        at.lsn,
        at.xid,
    ))
}

/// Name of `UInt32` partition column in a partitioned table's destination
fn leaf_column<'a>(desc: &RelDescriptor, shape: &'a CreateShape<'_>) -> Option<&'a str> {
    shape.system.leaf.as_deref().filter(|_| desc.kind == 'p')
}

/// ClickHouse `ALTER_OF_COLUMN_IS_FORBIDDEN`, including rejected key retypes
const ALTER_OF_COLUMN_IS_FORBIDDEN: i32 = 524;

/// PostgreSQL rewrite rows supersede stored values by `_lsn`.
/// Avoid fallible ClickHouse mutations: one unparseable value can stall them.
fn plan_retypes(
    old_desc: &RelDescriptor,
    new: &RelDescriptor,
    diff: &SchemaDiff,
    columns: &[ColumnMapping],
    rules: &ColumnRules,
) -> Vec<Retype> {
    let rewritten = old_desc.rfn != new.rfn;
    let is_int = |a: &RelAttr| matches!(a.type_oid, INT2OID | INT4OID | INT8OID);
    let mut retypes = Vec::new();
    for (old, attr) in &diff.type_changes {
        let Some(column) = columns.iter().find(|c| c.src_attnum == attr.attnum) else {
            continue;
        };
        if column.type_pinned {
            tracing::warn!(
                target: "walshadow::ch_ddl",
                relation = %new.rel_name,
                column = %column.target_name,
                "source type change keeps pinned destination type"
            );
            continue;
        }
        let key = replident_key_attnums(new).contains(&attr.attnum);
        let Some(target_type) = retyped_target(&new.rel_name, column, attr, key, rules) else {
            continue;
        };
        let infallible =
            (is_int(old) && is_int(attr)) || types::inner_kind(&target_type) == Some(Kind::String);
        retypes.push(Retype {
            src_attnum: attr.attnum,
            target_name: column.target_name.clone(),
            target_type,
            kind: if rewritten && !infallible {
                RetypeKind::Readd
            } else {
                RetypeKind::Modify
            },
        });
    }
    retypes
}

fn retype_clauses(retype: &Retype) -> Vec<String> {
    let name = quote_ident(&retype.target_name);
    let ty = &retype.target_type;
    match retype.kind {
        RetypeKind::Modify => vec![format!("MODIFY COLUMN IF EXISTS {name} {ty}")],
        RetypeKind::Readd => vec![
            format!("DROP COLUMN IF EXISTS {name}"),
            format!("ADD COLUMN IF NOT EXISTS {name} {ty}"),
        ],
    }
}

/// Reject ALTER continuation after concurrent route republish
async fn ensure_unmoved(
    mapping: &MappingHandle,
    rel: &RelName,
    at: &MappingSnapshot,
) -> Result<(), EmitterError> {
    if mapping.unmoved(at).await {
        Ok(())
    } else {
        Err(EmitterError::Config(format!(
            "routing map for `{rel}` republished mid-ALTER"
        )))
    }
}

/// Fold a `Changed` diff into the live mapping. Renames touch only entries
/// whose `target_name` still equals the OLD source name; an operator-pinned
/// different name is left alone (CH runs no ALTER for it either, see
/// `apply_changed`)
async fn mutate_mapping_for_diff(
    mapping: &MappingHandle,
    new: &RelDescriptor,
    diff: &SchemaDiff,
    retypes: &[Retype],
    rules: &ColumnRules,
) {
    mapping
        .mutate(|m| {
            if let Some(target_mapping) = Arc::make_mut(m).get_mut(&new.rel_name) {
                fold_diff_into_mapping(Arc::make_mut(target_mapping), new, diff, retypes, rules);
            }
        })
        .await;
}

/// `ADD COLUMN IF NOT EXISTS <n> <ty> [DEFAULT <expr>]`. IF NOT EXISTS keeps
/// it idempotent across a daemon-restart re-fire
fn add_column_clause(name: &str, resolved: &ResolvedColumn) -> String {
    format!("ADD COLUMN IF NOT EXISTS {}", col_def(name, resolved))
}

fn col_def(name: &str, resolved: &ResolvedColumn) -> String {
    let name = quote_ident(name);
    resolved.default_sql.as_ref().map_or_else(
        || format!("{name} {}", resolved.ch_type),
        |d| format!("{name} {} DEFAULT {d}", resolved.ch_type),
    )
}

/// Destination column as CREATE and ADD COLUMN render it
struct DestColumn {
    attnum: i16,
    name: String,
    resolved: ResolvedColumn,
}

impl DestColumn {
    fn def(&self) -> String {
        col_def(&self.name, &self.resolved)
    }

    fn mapping(&self) -> ColumnMapping {
        ColumnMapping {
            src_attnum: self.attnum,
            target_name: self.name.clone(),
            target_type: self.resolved.ch_type.clone(),
            type_pinned: false,
        }
    }
}

/// Bridged descriptor columns, `None` when a type can't be bridged
fn descriptor_columns(desc: &RelDescriptor, rules: &ColumnRules) -> Option<Vec<DestColumn>> {
    let pk_attnums = replident_key_attnums(desc);
    let mut cols = Vec::with_capacity(desc.attributes.len());
    for att in desc.attributes.iter().filter(|a| !a.dropped) {
        let pk_member = pk_attnums.contains(&att.attnum);
        // Skip the half-renderable CREATE; operator installs a
        // TOML override and re-triggers via Added on next refetch
        let resolved = type_bridge::map(att, pk_member).ok()?;
        let (name, resolved) = apply_column_rule(
            &att.name,
            resolved,
            rules.settings(&desc.rel_name, &att.name),
        );
        cols.push(DestColumn {
            attnum: att.attnum,
            name,
            resolved,
        });
    }
    Some(cols)
}

/// Mapped columns, the emitter's INSERT contract. Mapping keeps no defaults
fn mapping_columns(mapping: &TableMapping) -> Vec<DestColumn> {
    mapping
        .columns
        .iter()
        .map(|c| DestColumn {
            attnum: c.src_attnum,
            name: c.target_name.clone(),
            resolved: ResolvedColumn {
                ch_type: c.target_type.clone(),
                default_sql: None,
            },
        })
        .collect()
}

/// `ALTER TABLE <t> ADD COLUMN IF NOT EXISTS …` with one clause per column,
/// `None` when there are none
fn render_add_columns(cols: &[DestColumn], target: &TableTarget) -> Option<String> {
    if cols.is_empty() {
        return None;
    }
    let clauses: Vec<String> = cols
        .iter()
        .map(|c| add_column_clause(&c.name, &c.resolved))
        .collect();
    Some(format!(
        "ALTER TABLE {} {}",
        target.sql(),
        clauses.join(", ")
    ))
}

/// Pinned worker sends raw defaults to avoid `pg_type` locks during replay
/// Resolve through shadow PG so rows predating `ADD COLUMN` read PG's fast default
async fn resolve_disk_default<'a>(
    oracle: Option<&Oracle>,
    db: Oid,
    att: &'a RelAttr,
) -> Cow<'a, RelAttr> {
    let ColumnValue::PgPending { raw, .. } = heap_decoder::missing_value_for(att) else {
        return Cow::Borrowed(att);
    };
    let Some(oracle) = oracle else {
        return Cow::Borrowed(att);
    };
    match oracle
        .text_value(db, att.type_oid, att.typmod, OracleCell::DiskRaw(raw))
        .await
    {
        Ok(text) => {
            let mut resolved = att.clone();
            resolved.missing_default = Some(MissingDefault::Text(text));
            Cow::Owned(resolved)
        }
        Err(e) => {
            tracing::warn!(
                target: "walshadow::ch_ddl",
                column = %att.name,
                type_oid = att.type_oid,
                error = %e,
                "fast default left off ADD COLUMN: shadow did not render it",
            );
            Cow::Borrowed(att)
        }
    }
}

/// Shared CREATE tail: synthetic columns (mirror `TablePlan::build`),
/// engine, `ORDER BY` key names (else the LSN column)
fn render_create_sql(
    target: &str,
    mut col_defs: Vec<String>,
    key: SortKey,
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
    let engine = shape.engine.map_or("ReplacingMergeTree", str::trim);
    // Bare `*ReplacingMergeTree` gets version and delete marker args; any
    // other engine renders verbatim
    let engine = if engine.ends_with("ReplacingMergeTree") {
        format!("{engine}({engine_args})")
    } else {
        engine.to_owned()
    };
    let head = format!(
        "CREATE TABLE IF NOT EXISTS {target} (\n  {}\n) ENGINE = {engine}",
        col_defs.join(",\n  ")
    );
    // Only MergeTree family takes sorting key clauses, eg Null rejects them
    if !engine_family(Some(&engine)).ends_with("MergeTree") {
        return head;
    }
    let keys = if key.names.is_empty() {
        vec![lsn]
    } else {
        key.names
    };
    // CH indexes the PRIMARY KEY prefix of the sorting key; a non-prefix is a
    // CREATE-time error there, so drop it and let the key default to ORDER BY
    let primary_key = match shape.primary_key {
        // Indexing whole rows stores every column value at each granule boundary
        [] if key.whole_row => format!("\nPRIMARY KEY ({})", keys[0]),
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
    let settings = if key.nullable {
        "\nSETTINGS allow_nullable_key = 1"
    } else {
        ""
    };
    let partition_by = shape
        .partition_by
        .map(|p| format!("\nPARTITION BY {p}"))
        .unwrap_or_default();
    format!("{head}{partition_by}\nORDER BY ({order_by}){primary_key}{settings}")
}

/// Quoted `ORDER BY` columns; use LSN when empty
#[derive(Debug, Default)]
struct SortKey {
    names: Vec<String>,
    /// Use whole row for `REPLICA IDENTITY FULL` without primary key
    whole_row: bool,
    /// Nullable columns require `allow_nullable_key`
    nullable: bool,
}

impl SortKey {
    /// Derive sort key from replica identity and destination columns. Exclude
    /// nullable columns unless using whole row with `allow_nullable_key`.
    /// Whole-row keys exclude types ClickHouse cannot sort
    fn derive<'a>(
        desc: &RelDescriptor,
        cols: impl Iterator<Item = (i16, &'a str, &'a str)> + Clone,
    ) -> Self {
        if matches!(desc.replident, ReplIdent::Full { pk_attnums: None }) {
            let mut key = SortKey {
                whole_row: true,
                ..SortKey::default()
            };
            for (_, name, ty) in cols.filter(|(_, _, ty)| types::sortable(ty)) {
                key.nullable |= types::is_nullable(ty);
                key.names.push(quote_ident(name));
            }
            return key;
        }
        let names = replident_key_attnums(desc)
            .iter()
            .filter_map(|a| {
                cols.clone()
                    .find(|(attnum, _, ty)| attnum == a && !types::is_nullable(ty))
                    .map(|(_, name, _)| quote_ident(name))
            })
            .collect();
        SortKey {
            names,
            ..SortKey::default()
        }
    }
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
    derived: SortKey,
) -> SortKey {
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
    SortKey {
        names: keys,
        ..SortKey::default()
    }
}

/// System columns are always non-nullable, so any of them may sort
fn orderable_system<'a>(sys: &'a SystemColumns, orderable: &mut HashMap<&'a str, bool>) {
    for name in sys.names() {
        orderable.insert(name, false);
    }
}

/// `CREATE TABLE IF NOT EXISTS` from destination columns. `ORDER BY` takes
/// the operator override when it names non-nullable destination columns,
/// else resolves the descriptor's key attnums through `cols` (skipping
/// Nullable targets, which CH rejects as sort keys), else the LSN column
fn render_create_table(
    desc: &RelDescriptor,
    cols: &[DestColumn],
    target: &TableTarget,
    shape: &CreateShape<'_>,
) -> String {
    let target = target.sql();
    let mut col_defs: Vec<String> = cols.iter().map(DestColumn::def).collect();
    let mut orderable: HashMap<&str, bool> = HashMap::default();
    orderable_system(&shape.system, &mut orderable);
    if let Some(leaf) = leaf_column(desc, shape) {
        col_defs.push(format!("{} UInt32", quote_ident(leaf)));
        orderable.insert(leaf, false);
    }
    for c in cols {
        orderable.insert(&c.name, types::is_nullable(&c.resolved.ch_type));
    }
    let derived = SortKey::derive(
        desc,
        cols.iter()
            .map(|c| (c.attnum, c.name.as_str(), c.resolved.ch_type.as_str())),
    );
    if desc.kind == 'p' && !shape.order_by.is_empty() {
        warn_partition_order_by(&target, desc, cols, shape.order_by);
    }
    let key = resolve_order_by(&target, shape.order_by, &orderable, derived);
    render_create_sql(&target, col_defs, key, shape)
}

/// Unique keys include every partition column. Omitting root key columns
/// from sort key can merge rows from different partitions
fn warn_partition_order_by(
    target: &str,
    desc: &RelDescriptor,
    cols: &[DestColumn],
    order_by: &[String],
) {
    let missing: Vec<&str> = replident_key_attnums(desc)
        .iter()
        .filter_map(|a| cols.iter().find(|c| c.attnum == *a))
        .map(|c| c.name.as_str())
        .filter(|name| !order_by.iter().any(|o| o == name))
        .collect();
    if !missing.is_empty() {
        tracing::warn!(
            target: "walshadow::ch_ddl",
            table = %target,
            missing = ?missing,
            "order_by omits key columns of a partitioned table; rows of different \
             partitions sharing order_by values collapse into one",
        );
    }
}

/// Create `[ch] database` when the server doesn't have it, over a session on
/// `default` — every other client names it in the handshake, which CH refuses
/// outright for an absent database, so no connected client can create its own.
/// `Ok(false)` when it was already there. Sibling databases (per-namespace
/// `target_database`) go through the applicator's own `ensure_database` instead
pub async fn ensure_boot_database(cfg: &EmitterConfig) -> Result<bool, EmitterError> {
    let mut created = ensure_instance_database(cfg).await?;
    for instance in cfg.instances.values() {
        created |= ensure_instance_database(&instance.apply(cfg)).await?;
    }
    Ok(created)
}

async fn ensure_instance_database(cfg: &EmitterConfig) -> Result<bool, EmitterError> {
    connect_creating_database(cfg)
        .await
        .map(|(_, created)| created)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mapping::{ColumnMapping, TableMapping, derive_columns_for_mapping};
    use ahash::HashSetExt;
    use std::sync::LazyLock;

    static SYS: LazyLock<Arc<SystemColumns>> = LazyLock::new(Arc::default);

    #[tokio::test]
    async fn ddl_retry_includes_failed_reconnect() {
        let sql = "CREATE DATABASE IF NOT EXISTS retry_test";
        for (retries, idle) in [(0, false), (1, false), (2, false), (1, true)] {
            let (config, server) =
                crate::ch::test_support::retry_server(retries, sql, false, idle).await;
            let resolved = Arc::new(ResolvedConfig::default());
            let ddl = DdlConfig::from_resolved(
                &resolved,
                DdlBoot {
                    target_database: config.conn.database.clone(),
                    source_database: "app".into(),
                    soft_delete: false,
                    system: SYS.clone(),
                    replicate_all: false,
                    auto_create_name_all: None,
                    runtime_config_schema: None,
                },
            );
            let mut applicator = DdlApplicator::new(
                DestEmitter::new(Arc::new(config), None),
                ddl,
                crate::mapping::mapping_handle(HashMap::default()),
                watch::channel(resolved).1,
            )
            .await
            .unwrap();
            assert_eq!(
                applicator
                    .execute(&TableTarget::new("default", "t"), sql)
                    .await
                    .is_ok(),
                retries == 2 || idle
            );
            server.await.unwrap();
        }
    }

    fn render_from_desc(
        desc: &RelDescriptor,
        target: &TableTarget,
        shape: &CreateShape<'_>,
        rules: &ColumnRules,
    ) -> Option<String> {
        descriptor_columns(desc, rules).map(|cols| render_create_table(desc, &cols, target, shape))
    }

    fn render_from_mapping(
        desc: &RelDescriptor,
        mapping: &TableMapping,
        shape: &CreateShape<'_>,
    ) -> String {
        render_create_table(desc, &mapping_columns(mapping), &mapping.target, shape)
    }

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
            engine: None,
            partition_by: None,
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

    fn namespaces_with_bare_prefix(cfg: &mut DdlConfig) {
        cfg.namespaces.insert(
            "audit".to_string(),
            crate::mapping::NamespaceMapping {
                target_database: None,
                auto_create: true,
                auto_create_name: Some(NameTemplate::parse("$table$").unwrap()),
                drop_table_strategy: None,
                detach_strategy: None,
                initial_load: None,
            },
        );
    }

    #[test]
    fn auto_create_name_names_derived_destinations_only() {
        use crate::mapping::NamespaceMapping;
        use ahash::{HashMap, HashMapExt};
        let mut namespaces = HashMap::new();
        namespaces.insert(
            "public".to_string(),
            NamespaceMapping {
                target_database: None,
                auto_create: true,
                auto_create_name: Some(NameTemplate::parse("wh_").unwrap()),
                drop_table_strategy: None,
                detach_strategy: None,
                initial_load: None,
            },
        );
        let mut cfg = DdlConfig {
            instances: Default::default(),
            drop_table_strategy: DropTableStrategy::Retain,
            detach_strategy: DropTableStrategy::Retain,
            auto_create_namespaces: HashSet::new(),
            replicate_all: false,
            auto_create_name_all: None,
            runtime_config_schema: None,
            target_database: "default".into(),
            source_database: "app".into(),
            namespaces,
            soft_delete: false,
            system: Arc::default(),
            rules: Arc::default(),
            column_rules: Arc::default(),
        };
        let derived = cfg.create_target(&TableRule::default(), &RelName::new("public", "orders"));
        assert_eq!(derived.database, "default");
        assert_eq!(
            derived.table, "wh_orders",
            "an explicit namespace prefix beats the namespace name",
        );

        let pinned = cfg.create_target(
            &TableRule {
                target_table: Some("orders_v2".into()),
                ..TableRule::default()
            },
            &RelName::new("public", "orders"),
        );
        assert_eq!(
            pinned.table, "orders_v2",
            "an explicit target_table names the destination outright",
        );

        let unconfigured =
            cfg.create_target(&TableRule::default(), &RelName::new("audit", "orders"));
        assert_eq!(
            unconfigured.table, "audit_orders",
            "a namespace that sets no prefix derives from its own name",
        );

        namespaces_with_bare_prefix(&mut cfg);
        let bare = cfg.create_target(&TableRule::default(), &RelName::new("audit", "orders"));
        assert_eq!(bare.table, "orders", "an empty prefix opts out");
    }

    #[test]
    fn auto_create_name_all_covers_namespaces_with_no_entry() {
        use crate::mapping::NameTemplate;
        let mut cfg = DdlConfig {
            instances: Default::default(),
            drop_table_strategy: DropTableStrategy::Retain,
            detach_strategy: DropTableStrategy::Retain,
            auto_create_namespaces: HashSet::new(),
            replicate_all: true,
            auto_create_name_all: Some(NameTemplate::parse("$database$_$schema$_$table$").unwrap()),
            runtime_config_schema: None,
            target_database: "default".into(),
            source_database: "app".into(),
            namespaces: HashMap::default(),
            soft_delete: false,
            system: Arc::default(),
            rules: Arc::default(),
            column_rules: Arc::default(),
        };
        let derived = cfg.create_target(&TableRule::default(), &RelName::new("audit", "orders"));
        assert_eq!(
            derived.table, "app_audit_orders",
            "a namespace with no entry takes the cluster-wide template",
        );

        namespaces_with_bare_prefix(&mut cfg);
        let overridden = cfg.create_target(&TableRule::default(), &RelName::new("audit", "orders"));
        assert_eq!(
            overridden.table, "orders",
            "a namespace entry beats the cluster-wide template",
        );

        let pinned = cfg.create_target(
            &TableRule {
                target_table: Some("orders_v2".into()),
                ..TableRule::default()
            },
            &RelName::new("shop", "orders"),
        );
        assert_eq!(pinned.table, "orders_v2", "target_table still wins");
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
                auto_create_name: None,
                drop_table_strategy: Some(DropTableStrategy::Drop),
                detach_strategy: None,
                initial_load: None,
            },
        );
        namespaces.insert(
            "logs".to_string(),
            NamespaceMapping {
                target_database: None,
                auto_create: true,
                auto_create_name: None,
                drop_table_strategy: None,
                detach_strategy: None,
                initial_load: None,
            },
        );
        let cfg = DdlConfig {
            instances: Default::default(),
            drop_table_strategy: DropTableStrategy::Retain,
            detach_strategy: DropTableStrategy::Retain,
            auto_create_namespaces: HashSet::new(),
            replicate_all: false,
            auto_create_name_all: None,
            runtime_config_schema: None,
            target_database: "default".into(),
            source_database: "app".into(),
            namespaces,
            soft_delete: false,
            system: Arc::default(),
            rules: Arc::default(),
            column_rules: Arc::default(),
        };
        assert_eq!(cfg.target_database_for("analytics", None), "warehouse");
        assert_eq!(cfg.target_database_for("logs", None), "default");
        assert_eq!(cfg.target_database_for("unconfigured", None), "default");
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
            instances: Default::default(),
            drop_table_strategy: DropTableStrategy::Retain,
            detach_strategy: DropTableStrategy::Retain,
            auto_create_namespaces: HashSet::new(),
            replicate_all: false,
            auto_create_name_all: None,
            runtime_config_schema: None,
            target_database: "default".into(),
            source_database: "app".into(),
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
            missing_default: missing.map(|s| crate::schema::MissingDefault::Text(s.into())),
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
            ancestors: Vec::new(),
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
            instances: Default::default(),
            drop_table_strategy: DropTableStrategy::Retain,
            detach_strategy: DropTableStrategy::Retain,
            auto_create_namespaces: HashSet::new(),
            replicate_all: false,
            auto_create_name_all: None,
            runtime_config_schema: Some("walshadow".into()),
            target_database: "default".into(),
            source_database: "app".into(),
            namespaces: ahash::HashMap::default(),
            soft_delete: false,
            system: Arc::default(),
            rules: Arc::new(rules),
            column_rules: Arc::default(),
        };
        let auto_creates = |namespace: &str, name: &str| {
            let mut desc = crate::schema::int4_rel(16384, name, 'r', &[]);
            desc.rel_name = RelName::new(namespace, name);
            cfg.auto_creates(&desc)
        };
        let events = RelName::new("app", "events_1");
        assert!(auto_creates("app", "events_1"));
        assert_eq!(
            cfg.create_target(&cfg.rules.settings(&events), &events),
            TableTarget::new("warehouse", "app_events_1")
        );
        assert_eq!(
            cfg.declared_scope(&RelName::new("app", "events_audit")),
            Some(false),
            "guardrail beats the matching opt-in"
        );
        assert!(!auto_creates("app", "events_audit"));
        assert_eq!(cfg.declared_scope(&RelName::new("walshadow", "x")), None);
        assert!(!auto_creates("pg_catalog", "pg_class"));
        let settings = cfg.rules.settings(&events);
        let shape = cfg.create_shape(&settings);
        assert_eq!(shape.system.lsn, "_peerdb_version");
        assert!(shape.system.is_deleted.is_none(), "marker dropped");
        assert_eq!(shape.system.xid, "_xid", "unnamed column inherits");
        let other = RelName::new("other", "t");
        assert_eq!(
            cfg.create_target(&cfg.rules.settings(&other), &other),
            TableTarget::new("default", "other_t")
        );
        let settings = cfg.rules.settings(&other);
        assert_eq!(
            cfg.create_shape(&settings).system.lsn,
            "_lsn",
            "a relation no entry renames keeps the cluster-wide names"
        );
    }

    #[test]
    fn add_column_clause_carries_default() {
        let resolved = ResolvedColumn {
            ch_type: "Nullable(Int32)".into(),
            default_sql: Some("7".into()),
        };
        assert_eq!(
            add_column_clause("ship_at", &resolved),
            "ADD COLUMN IF NOT EXISTS `ship_at` Nullable(Int32) DEFAULT 7"
        );
    }

    #[test]
    fn add_column_clause_without_default_skips_default_clause() {
        let resolved = ResolvedColumn {
            ch_type: "String".into(),
            default_sql: None,
        };
        assert_eq!(
            add_column_clause("c", &resolved),
            "ADD COLUMN IF NOT EXISTS `c` String"
        );
    }

    #[test]
    fn render_add_columns_reconciles_routed_shape() {
        let mut mapping = TableMapping {
            target: TableTarget::new("warehouse", "orders"),
            columns: vec![
                ColumnMapping {
                    src_attnum: 1,
                    target_name: "order_id".into(),
                    target_type: "Int64".into(),
                    type_pinned: false,
                },
                ColumnMapping {
                    src_attnum: 2,
                    target_name: "description".into(),
                    target_type: "Nullable(String)".into(),
                    type_pinned: false,
                },
            ],
            tees: Vec::new(),
        };

        assert_eq!(
            render_add_columns(&mapping_columns(&mapping), &mapping.target).unwrap(),
            "ALTER TABLE `warehouse`.`orders` ADD COLUMN IF NOT EXISTS `order_id` Int64, \
             ADD COLUMN IF NOT EXISTS `description` Nullable(String)"
        );
        mapping.columns.clear();
        assert!(render_add_columns(&mapping_columns(&mapping), &mapping.target).is_none());
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
                target_type: Some("Decimal(38, 9)".into()),
                ..Default::default()
            },
        );
        b.add(
            &RelName::new("public", "orders"),
            MatchKind::Exact,
            "id",
            MatchKind::Exact,
            crate::column_rules::ColumnRule {
                target_name: Some("order_id".into()),
                ..Default::default()
            },
        );
        let sql = render_from_desc(&d, &dest("db", &d), &shape(false), &b.finish().0)
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
                target_type: Some("Nullable(Int32)".into()),
                ..Default::default()
            },
        );
        let sql = render_from_desc(&d, &dest("db", &d), &shape(false), &b.finish().0)
            .expect("renderable");
        assert!(sql.ends_with("ORDER BY (`_lsn`)"), "{sql}");
    }

    /// `CREATE TABLE` starts empty, only `ADD COLUMN` needs defaults for existing rows
    #[test]
    fn unresolved_disk_default_renders_no_default_clause() {
        use crate::decode::heap_decoder::{raw_missing_array, short_varlena};
        use crate::schema::MissingDefault;
        /// Fringe varlena type with no local codec
        const TSVECTOROID: u32 = 3614;

        let mut labels = att(2, "labels", TSVECTOROID, true, None);
        labels.type_name = "tsvector".into();
        labels.type_byval = false;
        labels.type_len = -1;
        labels.type_storage = 'x';
        labels.missing_default = Some(MissingDefault::Raw(raw_missing_array(
            TSVECTOROID,
            &short_varlena(&[0x01, 0x20, 0x00]),
        )));
        let resolved = type_bridge::map(&labels, false).unwrap();
        assert_eq!(
            add_column_clause("labels", &resolved),
            "ADD COLUMN IF NOT EXISTS `labels` String"
        );
        let d = desc(
            "t",
            vec![att(1, "id", INT4OID, true, None), labels],
            Some(vec![1]),
        );
        let sql = render_from_desc(
            &d,
            &dest("default", &d),
            &shape(false),
            &ColumnRules::default(),
        )
        .unwrap();
        assert!(sql.contains("`labels` String,"), "{sql}");
        assert!(!sql.contains("DEFAULT"), "{sql}");
    }

    #[test]
    fn jsonb_disk_default_renders_on_add_column() {
        use crate::schema::JSONBOID;

        let mut labels = att(2, "labels", JSONBOID, true, Some(r#"{"a": 1}"#));
        labels.type_byval = false;
        labels.type_len = -1;
        labels.type_storage = 'x';
        let resolved = type_bridge::map(&labels, false).unwrap();
        assert_eq!(
            add_column_clause("labels", &resolved),
            r#"ADD COLUMN IF NOT EXISTS `labels` String DEFAULT '{"a": 1}'"#
        );
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
        let sql = render_from_desc(
            &d,
            &dest("default", &d),
            &shape(false),
            &ColumnRules::default(),
        )
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
        let sql = render_from_desc(
            &d,
            &dest("default", &d),
            &shape(false),
            &ColumnRules::default(),
        )
        .unwrap();
        assert!(sql.ends_with("ORDER BY (`id`)"), "{sql}");
        assert!(!sql.contains("ORDER BY _lsn"), "{sql}");
    }

    #[test]
    fn render_create_table_full_replica_identity_without_pk_orders_by_whole_row() {
        let mut d = desc(
            "ledger",
            vec![
                att(1, "k", INT4OID, true, None),
                att(2, "v", TEXTOID, false, None),
            ],
            None,
        );
        d.replident = ReplIdent::Full { pk_attnums: None };
        let sql = render_from_desc(
            &d,
            &dest("default", &d),
            &shape(false),
            &ColumnRules::default(),
        )
        .unwrap();
        assert!(sql.contains("`v` Nullable(String)"), "{sql}");
        assert!(
            sql.ends_with(
                "ORDER BY (`k`, `v`)\nPRIMARY KEY (`k`)\nSETTINGS allow_nullable_key = 1"
            ),
            "{sql}"
        );

        // Exclude JSON from sort key; use configured primary key
        let m = TableMapping {
            target: dest("default", &d),
            columns: vec![
                ColumnMapping {
                    src_attnum: 1,
                    target_name: "k".into(),
                    target_type: "Int32".into(),
                    type_pinned: false,
                },
                ColumnMapping {
                    src_attnum: 2,
                    target_name: "v".into(),
                    target_type: "JSON".into(),
                    type_pinned: true,
                },
            ],
            tees: Vec::new(),
        };
        let primary_key = vec!["k".to_string()];
        let sql = render_from_mapping(
            &d,
            &m,
            &CreateShape {
                primary_key: &primary_key,
                engine: None,
                ..shape(false)
            },
        );
        assert!(sql.ends_with("ORDER BY (`k`)\nPRIMARY KEY (`k`)"), "{sql}");
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
        let sql = render_from_desc(
            &d,
            &dest("default", &d),
            &shape(false),
            &ColumnRules::default(),
        )
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
        let sql = render_from_desc(
            &d,
            &dest("default", &d),
            &shape(true),
            &ColumnRules::default(),
        )
        .unwrap();
        // Column always present; soft_delete only drops it from the engine
        assert!(sql.contains("`_is_deleted` Bool"));
        assert!(sql.contains("ENGINE = ReplacingMergeTree(`_lsn`)"));
        assert!(!sql.contains("ReplacingMergeTree(`_lsn`, `_is_deleted`)"));
        assert!(sql.ends_with("ORDER BY (`id`)"));
    }

    #[test]
    fn engine_override_renders_verbatim() {
        let d = desc(
            "orders",
            vec![
                att(1, "id", INT4OID, true, None),
                att(2, "body", TEXTOID, false, None),
            ],
            Some(vec![1]),
        );
        let render = |engine| {
            let shape = CreateShape {
                engine: Some(engine),
                ..shape(false)
            };
            render_from_desc(&d, &dest("default", &d), &shape, &ColumnRules::default()).unwrap()
        };
        let sql = render("CoalescingMergeTree");
        assert!(
            sql.contains("ENGINE = CoalescingMergeTree\nORDER BY (`id`)"),
            "{sql}"
        );
        let sql = render("ReplicatedReplacingMergeTree");
        assert!(
            sql.contains("ENGINE = ReplicatedReplacingMergeTree(`_lsn`, `_is_deleted`)\n"),
            "{sql}"
        );
        let sql = render("ReplacingMergeTree(`_lsn`)");
        assert!(
            sql.contains("ENGINE = ReplacingMergeTree(`_lsn`)\nORDER BY"),
            "{sql}"
        );
        let sql = render("Null");
        assert!(sql.ends_with(") ENGINE = Null"), "{sql}");
    }

    #[test]
    fn render_create_table_falls_back_to_lsn_when_no_pk() {
        let d = desc("events", vec![att(1, "body", TEXTOID, false, None)], None);
        let sql = render_from_desc(
            &d,
            &dest("default", &d),
            &shape(false),
            &ColumnRules::default(),
        )
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
        let sql = render_from_desc(
            &d,
            &dest("default", &d),
            &shape(false),
            &ColumnRules::default(),
        )
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
        let sql = render_from_desc(
            &d,
            &dest("default", &d),
            &shape(false),
            &ColumnRules::default(),
        )
        .unwrap();
        assert!(sql.ends_with("ORDER BY (`_lsn`)"), "{sql}");
    }

    #[test]
    fn render_create_table_handles_timestamp_precision() {
        let mut a = att(1, "ship_at", TIMESTAMPTZOID, false, None);
        a.typmod = 3;
        let d = desc("t", vec![a], None);
        let sql =
            render_from_desc(&d, &dest("db", &d), &shape(false), &ColumnRules::default()).unwrap();
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
                    type_pinned: false,
                },
                ColumnMapping {
                    src_attnum: 2,
                    target_name: "payload".into(),
                    target_type: "Nullable(String)".into(),
                    type_pinned: false,
                },
            ],
            tees: Vec::new(),
        };
        let sql = render_from_mapping(&d, &m, &shape(false));
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
                type_pinned: false,
            }],
            tees: Vec::new(),
        };
        let sql = render_from_mapping(&d, &m, &shape(false));
        assert!(sql.ends_with("ORDER BY (`_lsn`)"), "{sql}");
    }

    #[test]
    fn render_create_table_renames_system_columns() {
        let sys = SystemColumns {
            lsn: "_peerdb_version".into(),
            xid: "_xid".into(),
            commit_ts: "_peerdb_synced_at".into(),
            is_deleted: Some("_peerdb_is_deleted".into()),
            leaf: None,
        };
        let d = desc("t", vec![att(1, "id", INT4OID, true, None)], Some(vec![1]));
        let sql = render_from_desc(
            &d,
            &dest("default", &d),
            &CreateShape {
                system: sys.clone().into(),
                soft_delete: false,
                order_by: &[],
                primary_key: &[],
                engine: None,
                partition_by: None,
            },
            &ColumnRules::default(),
        )
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
        let sql = render_from_desc(
            &d,
            &dest("default", &d),
            &CreateShape {
                system: sys.clone().into(),
                soft_delete: false,
                order_by: &[],
                primary_key: &[],
                engine: None,
                partition_by: None,
            },
            &ColumnRules::default(),
        )
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
        let sql = render_from_desc(
            &d,
            &dest("default", &d),
            &CreateShape {
                system: SYS.clone(),
                soft_delete: false,
                order_by: &order_by,
                primary_key: &[],
                engine: None,
                partition_by: None,
            },
            &ColumnRules::default(),
        )
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
            let sql = render_from_desc(
                &d,
                &dest("default", &d),
                &CreateShape {
                    system: SYS.clone(),
                    soft_delete: false,
                    order_by: &keys,
                    primary_key: &[],
                    engine: None,
                    partition_by: None,
                },
                &ColumnRules::default(),
            )
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
            render_from_desc(
                &d,
                &dest("default", &d),
                &CreateShape {
                    system: SYS.clone(),
                    soft_delete: false,
                    order_by: &order_by,
                    primary_key,
                    engine: None,
                    partition_by: None,
                },
                &ColumnRules::default(),
            )
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
                    type_pinned: false,
                },
                ColumnMapping {
                    src_attnum: 2,
                    target_name: "tenant".into(),
                    target_type: "Int32".into(),
                    type_pinned: false,
                },
            ],
            tees: Vec::new(),
        };
        let order_by = vec!["tenant".to_string(), "order_id".to_string()];
        let sql = render_from_mapping(
            &d,
            &m,
            &CreateShape {
                system: SYS.clone(),
                soft_delete: false,
                order_by: &order_by,
                primary_key: &["tenant".to_string()],
                engine: None,
                partition_by: None,
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
        let sql = render_from_desc(&d, &dest("db", &d), &shape(false), &ColumnRules::default());
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

    #[test]
    fn plan_retypes_modifies_or_readds() {
        use crate::schema::{TEXTOID, VARCHAROID, compute_schema_diff};
        let old = desc(
            "t",
            vec![
                att(1, "id", INT4OID, true, None),
                att(2, "n", INT8OID, false, None),
                att(3, "m", INT4OID, true, None),
                att(4, "s", VARCHAROID, false, None),
                att(5, "p", INT4OID, true, None),
            ],
            Some(vec![1]),
        );
        let rules = ColumnRules::default();
        let mut columns = derive_columns_for_mapping(&old, &rules);
        columns[4].type_pinned = true;
        let new = desc(
            "t",
            vec![
                att(1, "id", INT8OID, true, None),
                att(2, "n", INT4OID, false, None),
                att(3, "m", INT4OID, false, None),
                att(4, "s", TEXTOID, true, None),
                att(5, "p", INT8OID, true, None),
            ],
            Some(vec![1]),
        );
        let diff = compute_schema_diff(&old, &new);
        let clauses = |retypes: &[Retype]| -> Vec<String> {
            retypes.iter().flat_map(retype_clauses).collect()
        };
        let retypes = plan_retypes(&old, &new, &diff, &columns, &rules);
        assert_eq!(
            clauses(&retypes),
            [
                "MODIFY COLUMN IF EXISTS `id` Int64",
                "MODIFY COLUMN IF EXISTS `n` Nullable(Int32)",
                "MODIFY COLUMN IF EXISTS `m` Nullable(Int32)",
            ],
            "SET NOT NULL keeps Nullable, varchar to text and pinned type stay"
        );
        let mut mapping = TableMapping {
            target: TableTarget::new("default", "t"),
            columns: columns.clone(),
            tees: Vec::new(),
        };
        fold_diff_into_mapping(&mut mapping, &new, &diff, &retypes, &rules);
        let types: Vec<&str> = mapping
            .columns
            .iter()
            .map(|c| c.target_type.as_str())
            .collect();
        assert_eq!(
            types,
            [
                "Int64",
                "Nullable(Int32)",
                "Nullable(Int32)",
                "Nullable(String)",
                "Int32"
            ]
        );
        let mut retyped = desc(
            "t",
            vec![
                att(1, "id", INT4OID, true, None),
                att(3, "m", TEXTOID, true, None),
                att(4, "s", INT4OID, false, None),
            ],
            Some(vec![1]),
        );
        retyped.rfn.rel_node += 1;
        let diff = compute_schema_diff(&old, &retyped);
        assert_eq!(
            clauses(&plan_retypes(&old, &retyped, &diff, &columns, &rules)),
            [
                "MODIFY COLUMN IF EXISTS `m` String",
                "DROP COLUMN IF EXISTS `s`",
                "ADD COLUMN IF NOT EXISTS `s` Nullable(Int32)",
            ],
            "rewrite rows restore values a fallible cast would lose"
        );
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
                    type_pinned: false,
                }],
                tees: Vec::new(),
            },
        )]
        .into_iter()
        .collect();
        (rel, map)
    }

    #[tokio::test]
    async fn mapping_snapshot_returns_pinned_table() {
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
        ensure_unmoved(&handle, &rel, &at).await.unwrap();
        handle.publish(Arc::new(ahash::HashMap::default())).await;
        let err = ensure_unmoved(&handle, &rel, &at)
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
            instances: Default::default(),
            drop_table_strategy: DropTableStrategy::Drop,
            detach_strategy: DropTableStrategy::Retain,
            auto_create_namespaces: HashSet::new(),
            replicate_all: false,
            auto_create_name_all: None,
            runtime_config_schema: None,
            target_database: "default".into(),
            source_database: "app".into(),
            namespaces: ahash::HashMap::default(),
            soft_delete: false,
            system: Arc::default(),
            rules: Arc::default(),
            column_rules: Arc::default(),
        };
        let dropped = SchemaEvent::Dropped {
            oid: 16400,
            rel_name: rel.clone(),
            partition: None,
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

    /// Plan-time prediction shares apply's scope: `replicate_all` alone maps
    /// a fresh user table, so its same-xact rows route
    #[test]
    fn prediction_maps_added_under_replicate_all() {
        let mut cfg = DdlConfig {
            instances: Default::default(),
            drop_table_strategy: DropTableStrategy::Retain,
            detach_strategy: DropTableStrategy::Retain,
            auto_create_namespaces: HashSet::new(),
            replicate_all: true,
            auto_create_name_all: None,
            runtime_config_schema: None,
            target_database: "default".into(),
            source_database: "app".into(),
            namespaces: ahash::HashMap::default(),
            soft_delete: false,
            system: Arc::default(),
            rules: Arc::default(),
            column_rules: Arc::default(),
        };
        let added = SchemaEvent::Added {
            desc: Arc::new(desc(
                "fresh",
                vec![att(1, "id", INT4OID, true, None)],
                Some(vec![1]),
            )),
        };
        let empty = MappingSnapshot::default();
        let predicted = predict_route_effect(&cfg, &empty, &added, false).unwrap();
        assert!(
            matches!(&predicted, Some((r, Some(m))) if &*r.name == "fresh" && m.target.table == "public_fresh"),
            "{predicted:?}"
        );
        assert!(
            predict_route_effect(&cfg, &empty, &added, true)
                .unwrap()
                .is_none(),
            "operator opt-out wins"
        );
        cfg.replicate_all = false;
        assert!(
            predict_route_effect(&cfg, &empty, &added, false)
                .unwrap()
                .is_none()
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
        mutate_mapping_for_diff(&handle, &new, &diff, &[], &ColumnRules::default()).await;
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
        mutate_mapping_for_diff(&handle, &ghost, &diff, &[], &ColumnRules::default()).await;
    }

    /// `m (id, region)` keyed on both, partitioned into `m_eu`
    fn partition_pair() -> (RelDescriptor, RelDescriptor) {
        let mut m = crate::schema::int4_rel(16480, "m", 'p', &[(1, "id"), (2, "region")]);
        m.replident = ReplIdent::Default {
            pk_attnums: Some(vec![1, 2]),
        };
        let mut leaf = crate::schema::int4_rel(16500, "m_eu", 'r', &[(1, "region"), (2, "id")]);
        leaf.replident = ReplIdent::Default {
            pk_attnums: Some(vec![2, 1]),
        };
        leaf.ancestors = vec![m.as_ancestor()];
        (m, leaf)
    }

    fn rules_with(name: &str, rule: TableRule) -> TableRules {
        let mut b = crate::table_rules::TableRulesBuilder::new();
        b.add(&RelName::new("public", name), MatchKind::Exact, rule);
        b.finish().0
    }

    #[test]
    fn partition_admission_checks_entry_storage_and_key() {
        let (m, leaf) = partition_pair();
        let none = TableRules::default();
        check_partition(&leaf, &m, &none).expect("matching key by name");

        let shaped = rules_with(
            "m_eu",
            TableRule {
                order_by: Some(vec!["id".into()]),
                ..TableRule::default()
            },
        );
        let err = check_partition(&leaf, &m, &shaped).unwrap_err().to_string();
        assert!(err.contains("may set only"), "{err}");

        let unlogged = RelDescriptor {
            persistence: 'u',
            ..leaf.clone()
        };
        let err = check_partition(&unlogged, &m, &none)
            .unwrap_err()
            .to_string();
        assert!(err.contains("unlogged partition"), "{err}");

        let narrow = RelDescriptor {
            replident: ReplIdent::UsingIndex {
                index_oid: 1,
                key_attnums: vec![2],
            },
            ..leaf.clone()
        };
        let err = check_partition(&narrow, &m, &none).unwrap_err().to_string();
        assert!(err.contains("fewer columns"), "{err}");
        let keyless = RelDescriptor {
            replident: ReplIdent::Default { pk_attnums: None },
            ..m.clone()
        };
        check_partition(&narrow, &keyless, &none).expect("keyless root needs no logged key");
        let full = RelDescriptor {
            replident: ReplIdent::Full { pk_attnums: None },
            ..narrow
        };
        check_partition(&full, &m, &none).expect("FULL logs every column");

        let whole_root = RelDescriptor {
            replident: ReplIdent::Full { pk_attnums: None },
            ..m.clone()
        };
        let err = check_partition(&leaf, &whole_root, &none)
            .unwrap_err()
            .to_string();
        assert!(err.contains("does not recurse"), "{err}");
    }

    #[test]
    fn partition_removal_tombstones_or_deletes_per_engine() {
        let mapping = TableMapping {
            target: TableTarget::new("db", "m"),
            columns: vec![
                DestColumn {
                    attnum: 1,
                    name: "id".into(),
                    resolved: ResolvedColumn {
                        ch_type: "Int32".into(),
                        default_sql: None,
                    },
                }
                .mapping(),
            ],
            tees: Vec::new(),
        };
        let at = Stamp {
            lsn: 0x500,
            xid: 7,
            commit_ts: 0,
        };
        let sys = SystemColumns::default();
        let target = mapping.target.clone();
        let tombstone =
            partition_removal_sql(&target, None, &mapping, &sys, "_leaf_oid", 16500, at).unwrap();
        assert_eq!(
            tombstone,
            "INSERT INTO `db`.`m` (`id`, `_leaf_oid`, `_lsn`, `_xid`, `_commit_ts`, `_is_deleted`) \
             SELECT `id`, `_leaf_oid`, toUInt64(1280), toUInt32(7), \
             fromUnixTimestamp64Micro(toInt64(946684800000000), 'UTC'), true \
             FROM `db`.`m` FINAL WHERE `_leaf_oid` = 16500 AND `_lsn` < 1280 AND NOT `_is_deleted`"
        );
        let delete = |engine, sys: &SystemColumns| {
            partition_removal_sql(&target, engine, &mapping, sys, "_leaf_oid", 16500, at)
        };
        let expected = "DELETE FROM `db`.`m` WHERE `_leaf_oid` = 16500 AND `_lsn` < 1280";
        assert_eq!(delete(Some("MergeTree"), &sys).as_deref(), Some(expected));
        let no_marker = SystemColumns {
            is_deleted: None,
            ..SystemColumns::default()
        };
        assert_eq!(delete(None, &no_marker).as_deref(), Some(expected));
        assert!(delete(Some("Null"), &sys).is_none());
        assert!(
            delete(Some("ReplacingMergeTree(_lsn, _is_deleted)"), &sys)
                .unwrap()
                .starts_with("INSERT"),
            "verbatim replacing engine still resolves by marker"
        );
    }

    #[test]
    fn partitioned_root_destination_carries_partition_column() {
        let (m, _) = partition_pair();
        let partitioned = CreateShape {
            partition_by: Some("intDiv(id, 1000)"),
            ..shape(false)
        };
        let sql =
            render_from_desc(&m, &dest("db", &m), &partitioned, &ColumnRules::default()).unwrap();
        assert!(sql.contains("`_leaf_oid` UInt32"), "{sql}");
        assert!(
            sql.contains("\nPARTITION BY intDiv(id, 1000)\nORDER BY (`id`, `region`)"),
            "{sql}"
        );
        let off = CreateShape {
            system: Arc::new(SystemColumns {
                leaf: None,
                ..SystemColumns::default()
            }),
            ..partitioned
        };
        let sql = render_from_desc(&m, &dest("db", &m), &off, &ColumnRules::default()).unwrap();
        assert!(!sql.contains("_leaf_oid"), "{sql}");
        let plain = desc("t", vec![att(1, "id", INT4OID, true, None)], Some(vec![1]));
        let sql = render_from_desc(
            &plain,
            &dest("db", &plain),
            &shape(false),
            &ColumnRules::default(),
        )
        .unwrap();
        assert!(
            !sql.contains("_leaf_oid"),
            "only partitioned tables carry it: {sql}"
        );
    }

    #[test]
    fn partition_order_by_may_name_the_partition_column() {
        let (m, _) = partition_pair();
        let render = |order_by: &[String]| {
            let shape = CreateShape {
                order_by,
                ..shape(false)
            };
            render_from_desc(&m, &dest("db", &m), &shape, &ColumnRules::default()).unwrap()
        };
        let sql = render(&["_leaf_oid".to_owned(), "id".to_owned(), "region".to_owned()]);
        assert!(
            sql.contains("ORDER BY (`_leaf_oid`, `id`, `region`)"),
            "{sql}"
        );
        let sql = render(&["id".to_owned()]);
        assert!(
            sql.ends_with("ORDER BY (`id`)"),
            "key column omitted with a warning: {sql}"
        );
    }

    /// `ATTACH` uses root's mapping; `DETACH` creates an independent mapping
    #[test]
    fn prediction_follows_partition_relinks() {
        let (m, leaf) = partition_pair();
        let cfg = DdlConfig {
            instances: Default::default(),
            drop_table_strategy: DropTableStrategy::Retain,
            detach_strategy: DropTableStrategy::Retain,
            auto_create_namespaces: HashSet::new(),
            replicate_all: true,
            auto_create_name_all: None,
            runtime_config_schema: None,
            target_database: "default".into(),
            source_database: "app".into(),
            namespaces: ahash::HashMap::default(),
            soft_delete: false,
            system: Arc::default(),
            rules: Arc::default(),
            column_rules: Arc::default(),
        };
        let standalone = Arc::new(RelDescriptor {
            ancestors: Vec::new(),
            ..leaf.clone()
        });
        let empty = MappingSnapshot::default();
        let added = SchemaEvent::Added {
            desc: Arc::new(leaf.clone()),
        };
        assert!(
            predict_route_effect(&cfg, &empty, &added, false)
                .unwrap()
                .is_none()
        );
        let root = SchemaEvent::Added {
            desc: Arc::new(m.clone()),
        };
        assert!(
            predict_route_effect(&cfg, &empty, &root, false)
                .unwrap()
                .is_some()
        );
        let attach = SchemaEvent::Changed {
            old: standalone.clone(),
            new: Arc::new(leaf.clone()),
            diff: SchemaDiff::default(),
        };
        assert!(
            predict_route_effect(&cfg, &empty, &attach, false)
                .unwrap()
                .is_none()
        );
        let own = DdlConfig {
            rules: Arc::new(rules_with(
                "m_eu",
                TableRule {
                    route: Some(PartitionRoute::Self_),
                    ..TableRule::default()
                },
            )),
            ..cfg.clone()
        };
        let altered = SchemaEvent::Changed {
            old: Arc::new(leaf.clone()),
            new: Arc::new(leaf.clone()),
            diff: SchemaDiff::default(),
        };
        assert!(
            predict_route_effect(&own, &empty, &altered, false)
                .unwrap()
                .is_none(),
            "unmapped partition with its own destination"
        );
        let detach = SchemaEvent::Changed {
            old: Arc::new(leaf),
            new: standalone,
            diff: SchemaDiff::default(),
        };
        let predicted = predict_route_effect(&cfg, &empty, &detach, false).unwrap();
        assert!(
            matches!(&predicted, Some((r, Some(_))) if &*r.name == "m_eu"),
            "{predicted:?}"
        );
        assert!(
            predict_route_effect(&cfg, &empty, &detach, true)
                .unwrap()
                .is_none()
        );
    }
}
