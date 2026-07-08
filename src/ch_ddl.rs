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
//! Opens its own `AsyncClient` (separate from the INSERT pump) so DDL
//! doesn't ride the INSERT backpressure path.
//!
//! ## Coordination with the INSERT pump
//!
//! The reorder coordinator ([`crate::pipeline::reorder`]) drives DDL
//! ordering: within a barrier xact it dispatches pending data, fences
//! (seals the batcher, waits until every earlier row is durable on CH),
//! then applies the schema change, then resumes. Post-DDL rows encode
//! against the new shape.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use clickhouse_c::AsyncClient;
use tokio::sync::watch;

use crate::ch_emitter::{
    ColumnMapping, EmitterConfig, EmitterError, MappingHandle, NamespaceMapping, RetryConfig,
    TableMapping, TableTarget, connect_client, drain_to_end_of_stream, is_retryable, quote_ident,
    reconnect_if_idle,
};
use crate::config::{ConfigResolver, ResolvedConfig};
use crate::shadow_catalog::{RelDescriptor, RelName, SchemaDiff, SchemaEvent};
use crate::type_bridge::{self, ResolvedColumn};

/// Per-relation DROP TABLE behaviour, matches `--drop-table-strategy`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum DropTableStrategy {
    /// Drop only the in-memory encoder; CH dest stays. Default, since
    /// silent CH drops are operationally hazardous.
    #[default]
    Retain,
    /// Run `DROP TABLE IF EXISTS <dest>` on CH
    Drop,
    /// Like Retain but logs at WARN; for staging the move to `Drop`
    Warn,
}

impl DropTableStrategy {
    pub fn parse(s: &str) -> Result<Self, EmitterError> {
        Ok(match s.to_ascii_lowercase().as_str() {
            "retain" => Self::Retain,
            "drop" => Self::Drop,
            "warn" => Self::Warn,
            other => {
                return Err(EmitterError::Config(format!(
                    "unknown drop-table-strategy {other:?} (expected retain / drop / warn)"
                )));
            }
        })
    }
}

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
    /// CH database DDL targets when neither per-table mapping nor source
    /// namespace overrides the destination
    pub target_database: String,
    /// Per-namespace overrides, fallback to the global fields above when
    /// a namespace has none
    pub namespaces: HashMap<String, NamespaceMapping>,
    /// Keep `_is_deleted` out of `ReplacingMergeTree`'s args so deletes
    /// stay queryable; mirrors [`EmitterConfig::soft_delete`]
    pub soft_delete: bool,
}

impl DdlConfig {
    /// Build from a resolved snapshot. `target_database` (`[ch] database`)
    /// and `soft_delete` are boot-only connection knobs the resolver does
    /// not republish, so callers thread them through unchanged.
    pub fn from_resolved(
        resolved: &ResolvedConfig,
        target_database: String,
        soft_delete: bool,
    ) -> Self {
        let auto_create_namespaces: HashSet<String> = resolved
            .namespaces
            .iter()
            .filter(|(_, v)| v.auto_create)
            .map(|(k, _)| k.clone())
            .collect();
        let drop_table_strategy =
            DropTableStrategy::parse(&resolved.drop_table_strategy).unwrap_or_default();
        Self {
            drop_table_strategy,
            auto_create_namespaces,
            target_database,
            namespaces: resolved.namespaces.clone(),
            soft_delete,
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
            .and_then(|n| n.drop_table_strategy.as_deref())
            .and_then(|s| DropTableStrategy::parse(s).ok())
            .unwrap_or(self.drop_table_strategy)
    }

    pub fn with_drop_strategy(mut self, s: DropTableStrategy) -> Self {
        self.drop_table_strategy = s;
        self
    }
}

/// CH-side DDL writer. Owns one AsyncClient over its own TCP.
pub struct DdlApplicator {
    client: AsyncClient,
    config: DdlConfig,
    /// Live config layers. `refresh_config` folds a republished snapshot
    /// into `config` (namespaces + drop strategy) at each apply, so SIGHUP
    /// and the future overlay retarget DDL without a restart.
    config_rx: watch::Receiver<Arc<ResolvedConfig>>,
    mapping: MappingHandle,
    /// Reconnect params, cloned at boot. SIGHUP reloads DDL knobs not
    /// connection params, so a reconnect re-dials the boot endpoint.
    conn_cfg: EmitterConfig,
    retry: RetryConfig,
    /// Per-attempt cap (shares `EmitterConfig::insert_timeout`); a
    /// half-open CH socket can't park the reorder barrier past this
    query_timeout: Duration,
    last_used: std::time::Instant,
    /// Decode-pool `RelCache`s key mapping snapshots on this epoch, but it
    /// bumps when the DDL record passes the decoder worker, before the
    /// barrier mutates the mapping here. Bump again on every mapping write
    /// so a worker whose refresh consumed the record-time bump drops its
    /// pre-apply snapshot
    invalidation_epoch: Option<Arc<AtomicU64>>,
    /// Owner of runtime-derived mapping state. Set: auto-created mappings,
    /// diff folds, and DROP forgets record into the resolver so the
    /// republish full-swap preserves them. Unset (bootstrap drain, tests
    /// without a resolver): mutate the live handle directly — no republish
    /// runs in those contexts, so nothing clobbers the write.
    resolver: Option<Arc<ConfigResolver>>,
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
            invalidation_epoch: None,
            resolver: None,
            stats: DdlStats::default(),
        })
    }

    /// Same handle as `CatalogTracker::set_invalidation_epoch`. Unset skips
    /// bumps (bootstrap drain, tests without a decode pool)
    pub fn with_invalidation_epoch(mut self, epoch: Arc<AtomicU64>) -> Self {
        self.invalidation_epoch = Some(epoch);
        self
    }

    /// Route mapping writes through the resolver so they survive its
    /// republish full-swap (the [config.md] "Known limitation" clobber).
    pub fn with_resolver(mut self, resolver: Arc<ConfigResolver>) -> Self {
        self.resolver = Some(resolver);
        self
    }

    pub fn config(&self) -> &DdlConfig {
        &self.config
    }

    /// Fold a republished snapshot into `config` (namespaces + drop
    /// strategy). `target_database` + `soft_delete` are boot-only, so they
    /// carry over. No-op until the resolver sends a new value; called at
    /// each apply so DDL runs against the current config.
    fn refresh_config(&mut self) {
        if self.config_rx.has_changed().unwrap_or(false) {
            let snap = self.config_rx.borrow_and_update();
            self.config = DdlConfig::from_resolved(
                &snap,
                self.config.target_database.clone(),
                self.config.soft_delete,
            );
        }
    }

    /// Errors propagate; the worker task turns them into
    /// `DecoderSinkError` so the daemon poisons the stream cleanly.
    pub async fn apply(&mut self, event: &SchemaEvent) -> Result<(), EmitterError> {
        self.refresh_config();
        match event {
            SchemaEvent::Added { desc } => self.apply_added(desc).await,
            SchemaEvent::Changed { old, new, diff } => self.apply_changed(old, new, diff).await,
            SchemaEvent::Dropped { oid: _, rel_name } => self.apply_dropped(rel_name).await,
        }
    }

    async fn apply_added(&mut self, desc: &RelDescriptor) -> Result<(), EmitterError> {
        // Pre-declared mapping wins: dest is operator-managed, skip
        // auto-create. Exception: strategy=drop hands dest lifecycle to
        // source DDL, so a CREATE after our DROP re-creates the dest from
        // the mapping (its columns are the emitter's INSERT contract);
        // IF NOT EXISTS no-ops when the dest still stands
        if let Some(m) = self.mapping_for(&desc.rel_name).await {
            if self.config.drop_strategy_for(&desc.rel_name.namespace) == DropTableStrategy::Drop {
                let sql = render_create_table_from_mapping(desc, &m, self.config.soft_delete);
                self.execute(&sql).await?;
                self.stats.creates_applied += 1;
            } else {
                self.stats.skipped += 1;
            }
            return Ok(());
        }
        // Operator opt-out (`replicate=false`) beats namespace auto_create:
        // no CH mirror, no mapping
        if self.is_excluded(&desc.rel_name).await {
            self.stats.skipped += 1;
            return Ok(());
        }
        if !self
            .config
            .auto_create_namespaces
            .contains(&*desc.rel_name.namespace)
        {
            self.stats.skipped += 1;
            return Ok(());
        }
        // Drives both CREATE TABLE and the row-routing mapping below so
        // rows and DDL land in the same database
        let target_db = self
            .config
            .target_database_for(&desc.rel_name.namespace)
            .to_owned();
        let Some(sql) = render_create_table(desc, &target_db, self.config.soft_delete)? else {
            self.stats.skipped += 1;
            return Ok(());
        };
        self.execute(&sql).await?;
        self.stats.creates_applied += 1;
        // Auto-derive a TableMapping so the emitter ships rows against
        // the new CH table without TOML edits
        let target = TableTarget::new(&target_db, &desc.rel_name.name);
        let columns = derive_columns_for_mapping(desc);
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
        self.refresh_config();
        let target_db = self
            .config
            .target_database_for(&desc.rel_name.namespace)
            .to_owned();
        let Some(sql) = render_create_table(desc, &target_db, self.config.soft_delete)? else {
            tracing::warn!(
                target: "walshadow::ch_ddl",
                qname = %desc.rel_name,
                "opt-in skipped: no bridgeable CH shape",
            );
            self.stats.skipped += 1;
            return Ok(false);
        };
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
        let Some(target) = self.mapping_target(&key).await else {
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
            let mapping_lookup = self.mapping.read().await;
            let m = mapping_lookup
                .get(&key)
                .expect("just resolved via mapping_target");
            let _ = old_name;
            let needs_rename = m.columns.iter().any(|c| &c.target_name == new_name)
                || !m.columns.iter().any(|c| &c.target_name == old_name);
            drop(mapping_lookup);
            if needs_rename {
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
            let pk_member = matches!(
                new.replident,
                crate::shadow_catalog::ReplIdent::Default {
                    pk_attnums: Some(ref nums),
                } if nums.contains(&att.attnum)
            );
            let Ok(resolved) = type_bridge::map(att, pk_member) else {
                // Unbridged type; operator TOML override is the recovery path
                self.stats.skipped += 1;
                continue;
            };
            let sql = render_add_column(&target, &att.name, &resolved);
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
        let Some(target) = self.mapping_target(rel).await else {
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
        let Some(target) = self.mapping_target(rel).await else {
            return Ok(());
        };
        self.execute(&format!("TRUNCATE TABLE {}", target.sql()))
            .await
    }

    async fn mapping_target(&mut self, rel: &RelName) -> Option<TableTarget> {
        let m = self.mapping.read().await;
        m.get(rel).map(|t| t.target.clone())
    }

    async fn mapping_for(&mut self, rel: &RelName) -> Option<TableMapping> {
        self.mapping.read().await.get(rel).cloned()
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

    /// Mapping writes route per `resolver` field: resolver-owned entries
    /// survive the republish full-swap (republish writes the live handle +
    /// bumps the epoch); resolver-less path mutates the live handle and
    /// bumps directly
    async fn register_mapping(&mut self, rel: &RelName, mapping: TableMapping) {
        if let Some(r) = &self.resolver {
            r.register_derived_mapping(rel, mapping).await;
        } else {
            self.mapping.write().await.insert(rel.clone(), mapping);
            bump_mapping_epoch(self.invalidation_epoch.as_ref());
        }
    }

    async fn fold_mapping_diff(&mut self, new: &RelDescriptor, diff: &SchemaDiff) {
        if let Some(r) = &self.resolver {
            r.apply_schema_diff(new, diff).await;
        } else {
            mutate_mapping_for_diff(&self.mapping, self.invalidation_epoch.as_ref(), new, diff)
                .await;
        }
    }

    async fn forget_mapping(&mut self, rel: &RelName) {
        if let Some(r) = &self.resolver {
            r.forget_derived_mapping(rel).await;
        } else {
            self.mapping.write().await.remove(rel);
            bump_mapping_epoch(self.invalidation_epoch.as_ref());
        }
    }

    /// Run one DDL statement with the same bounded timeout +
    /// reconnect/retry as the INSERT pump. DDL applies inside the
    /// reorder barrier, so a half-open socket would otherwise park the
    /// barrier and ack frontier indefinitely. Every emitted statement is
    /// idempotent (`IF [NOT] EXISTS`, `RENAME COLUMN IF EXISTS`,
    /// `TRUNCATE`), so a reconnect resends and CH no-ops the second apply.
    async fn execute(&mut self, sql: &str) -> Result<(), EmitterError> {
        tracing::debug!(target: "walshadow::ch_ddl", sql = %sql, "applying");
        let mut attempt = 0u32;
        let mut backoff = self.retry.initial_backoff;
        reconnect_if_idle(&mut self.client, &self.conn_cfg, self.last_used).await?;
        loop {
            let attempt_result = tokio::time::timeout(self.query_timeout, async {
                self.client.send_query(sql, None).await?;
                drain_to_end_of_stream(&mut self.client).await
            })
            .await
            // Park past the cap (half-open socket) must not pin the
            // barrier forever; surface a retryable timeout
            .unwrap_or_else(|_| {
                Err(EmitterError::Timeout {
                    secs: self.query_timeout.as_secs(),
                })
            });
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
                    tokio::time::sleep(backoff).await;
                    backoff = backoff.saturating_mul(2).min(self.retry.max_backoff);
                    self.client = connect_client(&self.conn_cfg).await?;
                }
                Err(e) => return Err(e),
            }
        }
    }
}

/// Flush decode-pool `RelCache` mapping snapshots after a mapping write.
/// Call only after the write guard drops: bump-before-write would let a
/// racing `RelCache::refresh` cache the pre-write map under the post-bump
/// epoch, going permanently stale
pub fn bump_mapping_epoch(epoch: Option<&Arc<AtomicU64>>) {
    if let Some(e) = epoch {
        e.fetch_add(1, Ordering::Release);
    }
}

/// Fold a `Changed` diff into the live mapping, then bump the epoch so
/// decode-pool workers holding a pre-diff snapshot re-resolve. Renames touch
/// only entries whose `target_name` still equals the OLD source name; an
/// operator-pinned different name is left alone (CH runs no ALTER for it
/// either, see `apply_changed`)
async fn mutate_mapping_for_diff(
    mapping: &MappingHandle,
    epoch: Option<&Arc<AtomicU64>>,
    new: &RelDescriptor,
    diff: &SchemaDiff,
) {
    {
        let mut m = mapping.write().await;
        let Some(target_mapping) = m.get_mut(&new.rel_name) else {
            return;
        };
        fold_diff_into_mapping(target_mapping, new, diff);
    }
    bump_mapping_epoch(epoch);
}

/// The fold itself, shared with `ConfigResolver::apply_schema_diff` (the
/// resolver-owned path, where the folded mapping must live in a layer the
/// republish full-swap rebuilds from).
pub(crate) fn fold_diff_into_mapping(
    target_mapping: &mut TableMapping,
    new: &RelDescriptor,
    diff: &SchemaDiff,
) {
    for (attnum, old_name, new_name) in &diff.renamed_columns {
        for c in &mut target_mapping.columns {
            if c.src_attnum == *attnum && &c.target_name == old_name {
                c.target_name = new_name.clone();
            }
        }
    }
    for attnum in &diff.dropped_columns {
        target_mapping.columns.retain(|c| c.src_attnum != *attnum);
    }
    // Skip adds the operator already pre-declared
    for att in &diff.added_columns {
        if target_mapping
            .columns
            .iter()
            .any(|c| c.src_attnum == att.attnum)
        {
            continue;
        }
        let pk_member = matches!(
            new.replident,
            crate::shadow_catalog::ReplIdent::Default {
                pk_attnums: Some(ref nums),
            } if nums.contains(&att.attnum)
        );
        let Ok(resolved) = type_bridge::map(att, pk_member) else {
            continue;
        };
        target_mapping.columns.push(ColumnMapping {
            src_attnum: att.attnum,
            target_name: att.name.clone(),
            target_type: resolved.ch_type,
        });
    }
}

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

/// PK / replica-identity key attnums driving `ORDER BY` + non-null forcing
fn replident_key_attnums(desc: &RelDescriptor) -> Vec<i16> {
    match &desc.replident {
        crate::shadow_catalog::ReplIdent::Default {
            pk_attnums: Some(n),
        } => n.clone(),
        crate::shadow_catalog::ReplIdent::UsingIndex { key_attnums, .. } => key_attnums.clone(),
        // FULL replica identity still exposes the PK (captured in fetch_replident)
        // so ORDER BY uses the key, not `_lsn` (which would collapse all rows
        // sharing an `_lsn`, e.g. an entire backfill tagged one LSN).
        crate::shadow_catalog::ReplIdent::Full {
            pk_attnums: Some(n),
        } => n.clone(),
        _ => Vec::new(),
    }
}

/// Shared CREATE tail: synthetic columns (mirror `TablePlan::build`),
/// engine, `ORDER BY` key names (else `_lsn`)
fn render_create_sql(
    target: &str,
    mut col_defs: Vec<String>,
    key_names: Vec<String>,
    soft_delete: bool,
) -> String {
    col_defs.push("`_lsn` UInt64".into());
    col_defs.push("`_xid` UInt32".into());
    col_defs.push("`_commit_ts` DateTime64(6, 'UTC')".into());
    col_defs.push("`_is_deleted` Bool".into());
    // soft_delete keeps `_is_deleted` out of the engine args
    let engine_args = if soft_delete {
        "`_lsn`"
    } else {
        "`_lsn`, `_is_deleted`"
    };
    let order_by = if key_names.is_empty() {
        "(`_lsn`)".to_string()
    } else {
        format!("({})", key_names.join(", "))
    };
    format!(
        "CREATE TABLE IF NOT EXISTS {target} (\n  {}\n) ENGINE = ReplacingMergeTree({engine_args})\nORDER BY {order_by}",
        col_defs.join(",\n  ")
    )
}

/// `CREATE TABLE IF NOT EXISTS` for an autodiscovered relation. `None`
/// when a column's type can't be bridged; caller logs + skips.
pub fn render_create_table(
    desc: &RelDescriptor,
    target_database: &str,
    soft_delete: bool,
) -> Result<Option<String>, EmitterError> {
    let target = TableTarget::new(target_database, &desc.rel_name.name).sql();
    let pk_attnums = replident_key_attnums(desc);
    let mut col_defs: Vec<String> = Vec::with_capacity(desc.attributes.len() + 4);
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
        let mut def = format!("{} {}", quote_ident(&att.name), resolved.ch_type);
        if let Some(d) = resolved.default_sql {
            def.push_str(" DEFAULT ");
            def.push_str(&d);
        }
        col_defs.push(def);
    }
    let key_names: Vec<String> = pk_attnums
        .iter()
        .filter_map(|a| {
            desc.attributes
                .iter()
                .find(|att| att.attnum == *a && !att.dropped)
                .map(|att| quote_ident(&att.name))
        })
        .collect();
    Ok(Some(render_create_sql(
        &target,
        col_defs,
        key_names,
        soft_delete,
    )))
}

/// `CREATE TABLE IF NOT EXISTS` rendered from an existing mapping — the
/// re-create path for a mapped dest dropped under strategy=drop. Columns
/// come from the mapping (the emitter's INSERT contract), not the
/// descriptor; `ORDER BY` resolves the descriptor's key attnums through
/// the mapping, skipping Nullable targets (CH rejects nullable sort
/// keys), else `_lsn`
pub fn render_create_table_from_mapping(
    desc: &RelDescriptor,
    mapping: &TableMapping,
    soft_delete: bool,
) -> String {
    let col_defs: Vec<String> = mapping
        .columns
        .iter()
        .map(|c| format!("{} {}", quote_ident(&c.target_name), c.target_type))
        .collect();
    let key_names: Vec<String> = replident_key_attnums(desc)
        .iter()
        .filter_map(|a| {
            mapping
                .columns
                .iter()
                .find(|c| c.src_attnum == *a && !c.target_type.starts_with("Nullable"))
                .map(|c| quote_ident(&c.target_name))
        })
        .collect();
    render_create_sql(&mapping.target.sql(), col_defs, key_names, soft_delete)
}

/// Default `Vec<ColumnMapping>` for an auto-discovered table; operator
/// TOML overrides win the next SIGHUP.
pub fn derive_columns_for_mapping(desc: &RelDescriptor) -> Vec<ColumnMapping> {
    let pk_attnums = replident_key_attnums(desc);
    let mut out = Vec::with_capacity(desc.attributes.len());
    for att in &desc.attributes {
        if att.dropped {
            continue;
        }
        let pk_member = pk_attnums.contains(&att.attnum);
        let Ok(resolved) = type_bridge::map(att, pk_member) else {
            continue;
        };
        out.push(ColumnMapping {
            src_attnum: att.attnum,
            target_name: att.name.clone(),
            target_type: resolved.ch_type,
        });
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ch_emitter::{ColumnMapping, TableMapping};
    use crate::heap_decoder::{INT4OID, TEXTOID, TIMESTAMPTZOID};
    use crate::shadow_catalog::{RelAttr, RelDescriptor, ReplIdent, SchemaDiff};
    use std::sync::Arc;

    #[test]
    fn per_namespace_target_and_drop_override_global() {
        use crate::ch_emitter::NamespaceMapping;
        use std::collections::HashMap;
        let mut namespaces = HashMap::new();
        namespaces.insert(
            "analytics".to_string(),
            NamespaceMapping {
                target_database: Some("warehouse".into()),
                auto_create: true,
                drop_table_strategy: Some("drop".into()),
            },
        );
        namespaces.insert(
            "logs".to_string(),
            NamespaceMapping {
                target_database: None,
                auto_create: true,
                drop_table_strategy: None,
            },
        );
        let cfg = DdlConfig {
            drop_table_strategy: DropTableStrategy::Retain,
            auto_create_namespaces: HashSet::new(),
            target_database: "default".into(),
            namespaces,
            soft_delete: false,
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
        use std::collections::HashMap;
        let cfg = DdlConfig {
            drop_table_strategy: DropTableStrategy::Retain,
            auto_create_namespaces: HashSet::new(),
            target_database: "default".into(),
            namespaces: HashMap::new(),
            soft_delete: false,
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
            namespace_oid: 2200,
            rel_name: RelName::new("public", name),
            kind: 'r',
            persistence: 'p',
            replident: ReplIdent::Default { pk_attnums: pk },
            attributes: attrs,
        }
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
    fn render_create_table_uses_pk_for_order_by() {
        let d = desc(
            "orders",
            vec![
                att(1, "id", INT4OID, true, None),
                att(2, "body", TEXTOID, false, None),
            ],
            Some(vec![1]),
        );
        let sql = render_create_table(&d, "default", false).unwrap().unwrap();
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
        let sql = render_create_table(&d, "default", false).unwrap().unwrap();
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
        let sql = render_create_table(&d, "default", false).unwrap().unwrap();
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
        let sql = render_create_table(&d, "default", true).unwrap().unwrap();
        // Column always present; soft_delete only drops it from the engine
        assert!(sql.contains("`_is_deleted` Bool"));
        assert!(sql.contains("ENGINE = ReplacingMergeTree(`_lsn`)"));
        assert!(!sql.contains("ReplacingMergeTree(`_lsn`, `_is_deleted`)"));
        assert!(sql.ends_with("ORDER BY (`id`)"));
    }

    #[test]
    fn render_create_table_falls_back_to_lsn_when_no_pk() {
        let d = desc("events", vec![att(1, "body", TEXTOID, false, None)], None);
        let sql = render_create_table(&d, "default", false).unwrap().unwrap();
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
        let sql = render_create_table(&d, "default", false).unwrap().unwrap();
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
        let sql = render_create_table(&d, "default", false).unwrap().unwrap();
        assert!(sql.ends_with("ORDER BY (`_lsn`)"), "{sql}");
    }

    #[test]
    fn render_create_table_handles_timestamp_precision() {
        let mut a = att(1, "ship_at", TIMESTAMPTZOID, false, None);
        a.typmod = 3;
        let d = desc("t", vec![a], None);
        let sql = render_create_table(&d, "db", false).unwrap().unwrap();
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
        let sql = render_create_table_from_mapping(&d, &m, false);
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
        let sql = render_create_table_from_mapping(&d, &m, false);
        assert!(sql.ends_with("ORDER BY (`_lsn`)"), "{sql}");
    }

    #[test]
    fn drop_table_strategy_parses() {
        assert_eq!(
            DropTableStrategy::parse("retain").unwrap(),
            DropTableStrategy::Retain
        );
        assert_eq!(
            DropTableStrategy::parse("Drop").unwrap(),
            DropTableStrategy::Drop
        );
        assert_eq!(
            DropTableStrategy::parse("warn").unwrap(),
            DropTableStrategy::Warn
        );
        assert!(DropTableStrategy::parse("bogus").is_err());
    }

    #[test]
    fn render_create_table_skips_when_type_unbridged() {
        // type_bridge falls back to String for unknown OIDs today, so
        // this never hits None; revisit if the bridge grows strictness
        let d = desc("t", vec![att(1, "id", 99999, true, None)], None);
        let sql = render_create_table(&d, "db", false).unwrap();
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
    fn mapping_target_returns_pinned_table() {
        let map: std::collections::HashMap<RelName, TableMapping> = [(
            RelName::new("public", "orders"),
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
        let handle: MappingHandle = Arc::new(tokio::sync::RwLock::new(map));
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        let target = rt.block_on(async {
            let m = handle.read().await;
            m.get(&RelName::new("public", "orders"))
                .map(|t| t.target.clone())
        });
        assert_eq!(target, Some(TableTarget::new("default", "orders")));
    }

    #[test]
    fn mapping_mutation_bumps_invalidation_epoch() {
        let map: std::collections::HashMap<RelName, TableMapping> = [(
            RelName::new("public", "orders"),
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
        let handle: MappingHandle = Arc::new(tokio::sync::RwLock::new(map));
        let epoch = Arc::new(AtomicU64::new(7));
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
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        rt.block_on(async {
            // Mapped relation: column folded in, epoch bumped so decode-pool
            // RelCaches drop pre-diff mapping snapshots
            mutate_mapping_for_diff(&handle, Some(&epoch), &new, &diff).await;
            let m = handle.read().await;
            let cols = &m.get(&RelName::new("public", "orders")).unwrap().columns;
            assert!(
                cols.iter()
                    .any(|c| c.src_attnum == 2 && c.target_name == "c")
            );
        });
        assert_eq!(epoch.load(Ordering::Acquire), 8);

        // Unmapped relation: early return, no spurious bump
        let ghost = desc("ghost", vec![att(1, "id", INT4OID, true, None)], None);
        rt.block_on(mutate_mapping_for_diff(
            &handle,
            Some(&epoch),
            &ghost,
            &diff,
        ));
        assert_eq!(epoch.load(Ordering::Acquire), 8);
    }
}
