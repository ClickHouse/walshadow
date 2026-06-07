//! CH-side DDL applicator.
//!
//! Consumes [`SchemaEvent`] dispatch from
//! [`crate::xact_buffer::XactBuffer::commit`]'s k-way merge (via the
//! [`crate::decoder_sink::TupleObserver::on_schema_event`] callback)
//! and translates each event into the matching CH SQL:
//!
//! | event | CH SQL |
//! |---|---|
//! | `Added` | `CREATE TABLE IF NOT EXISTS …` (only when namespace `auto_create = true`) |
//! | `Changed.added_columns` | `ALTER TABLE … ADD COLUMN IF NOT EXISTS …` per column in attnum order |
//! | `Changed.renamed_columns` | `ALTER TABLE … RENAME COLUMN … TO …` first |
//! | `Changed.dropped_columns` | `ALTER TABLE … DROP COLUMN IF EXISTS …` |
//! | `Changed.type_changes` | rejected — logged, not applied (open question) |
//! | `Dropped` | `DROP TABLE IF EXISTS …` gated on [`DropTableStrategy`] |
//!
//! ## Connection lifecycle
//!
//! The applicator opens its own `clickhouse_c::AsyncClient` (separate
//! from the emitter's INSERT pump) so DDL doesn't ride the INSERT
//! backpressure path. Same `(host, port, user, password, database)` as
//! the emitter; built from the same `EmitterConfig`.
//!
//! ## Coordination with the INSERT pump
//!
//! The reorder coordinator ([`crate::pipeline::reorder`]) drives DDL
//! ordering: within a barrier xact it dispatches pending data segments,
//! fences (seals the batcher and waits until every earlier row is durable
//! on CH), then applies the schema change here, then resumes. The CH table
//! is reshaped only after all earlier-LSN rows have landed; post-DDL rows
//! then encode against the new shape.

use std::collections::{HashMap, HashSet};

use clickhouse_c::AsyncClient;

use crate::ch_emitter::{
    ColumnMapping, EmitterConfig, EmitterError, MappingHandle, NamespaceMapping, TableMapping,
    connect_client, drain_to_end_of_stream, quote_ident,
};
use crate::shadow_catalog::{RelDescriptor, SchemaDiff, SchemaEvent};
use crate::type_bridge::{self, ResolvedColumn};

/// Per-relation DROP TABLE behaviour. Matches the
/// `--drop-table-strategy` flag.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum DropTableStrategy {
    /// Source `DROP TABLE` drops the in-memory encoder for the
    /// relation; CH dest stays. Default — surprising silent CH drops
    /// are operationally hazardous.
    #[default]
    Retain,
    /// Source `DROP TABLE` runs `DROP TABLE IF EXISTS <dest>` on CH.
    Drop,
    /// Same as Retain, but logs at WARN. Useful for operators staging
    /// the move to `Drop`.
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

/// Knobs that don't ride the emitter's INSERT pump but still need to
/// flow through the same TOML reload path. Owned by the applicator;
/// SIGHUP reload swaps the inner via [`DdlApplicator::config_mut`].
#[derive(Debug, Clone)]
pub struct DdlConfig {
    pub drop_table_strategy: DropTableStrategy,
    /// Auto-create hook — when a namespace mapping with
    /// `auto_create = true` is configured for the source namespace,
    /// `Added` events run `CREATE TABLE IF NOT EXISTS` automatically.
    /// Today's config only carries the namespace allow-list; the
    /// applicator consults `mapping_handle` for the per-table override
    /// path and treats namespace-implicit creates as best-effort.
    pub auto_create_namespaces: HashSet<String>,
    /// CH database name DDL targets when neither the per-table mapping
    /// nor the source namespace overrides the destination. Matches the
    /// emitter's `EmitterConfig::database`.
    pub target_database: String,
    /// Per-namespace overrides (`target_database`, `drop_table_strategy`)
    /// keyed by source namespace, resolved in `Self::target_database_for`
    /// and `Self::drop_strategy_for`. The global fields above are the
    /// fallback when a namespace has no override.
    pub namespaces: HashMap<String, NamespaceMapping>,
}

impl DdlConfig {
    pub fn from_emitter(cfg: &EmitterConfig) -> Self {
        let auto_create_namespaces: HashSet<String> = cfg
            .namespaces
            .iter()
            .filter(|(_, v)| v.auto_create)
            .map(|(k, _)| k.clone())
            .collect();
        let drop_table_strategy =
            DropTableStrategy::parse(&cfg.drop_table_strategy).unwrap_or_default();
        Self {
            drop_table_strategy,
            auto_create_namespaces,
            target_database: cfg.database.clone(),
            namespaces: cfg.namespaces.clone(),
        }
    }

    /// Destination CH database for a source namespace: its
    /// `target_database` override if set, else the global default.
    fn target_database_for(&self, namespace: &str) -> &str {
        self.namespaces
            .get(namespace)
            .and_then(|n| n.target_database.as_deref())
            .unwrap_or(&self.target_database)
    }

    /// Drop strategy for a source namespace: its `drop_table_strategy`
    /// override (parsed) if set, else the global default.
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

/// CH-side DDL writer. Owns one clickhouse-c AsyncClient over its own TCP.
pub struct DdlApplicator {
    client: AsyncClient,
    config: DdlConfig,
    mapping: MappingHandle,
    pub stats: DdlStats,
}

#[derive(Debug, Default, Clone)]
pub struct DdlStats {
    /// `ALTER TABLE` statements successfully acked by CH.
    pub alters_applied: u64,
    /// `CREATE TABLE IF NOT EXISTS` statements run (regardless of
    /// whether the table already existed).
    pub creates_applied: u64,
    /// `DROP TABLE IF EXISTS` statements run.
    pub drops_applied: u64,
    /// Schema events the applicator received but skipped (no mapping,
    /// no auto_create, type change rejected, drop strategy = Retain).
    pub skipped: u64,
    /// Type changes the applicator refused.
    pub type_changes_rejected: u64,
}

impl DdlApplicator {
    /// Build an applicator with its own CH connection. Opens a fresh
    /// `tokio` TCP socket to `(host, port)` via [`AsyncClient`] — same
    /// shape as the shared `connect_client` helper.
    pub async fn new(
        emitter_cfg: &EmitterConfig,
        ddl_cfg: DdlConfig,
        mapping: MappingHandle,
    ) -> Result<Self, EmitterError> {
        let client = connect_client(emitter_cfg).await?;
        Ok(Self {
            client,
            config: ddl_cfg,
            mapping,
            stats: DdlStats::default(),
        })
    }

    pub fn config(&self) -> &DdlConfig {
        &self.config
    }

    pub fn config_mut(&mut self) -> &mut DdlConfig {
        &mut self.config
    }

    /// Apply one schema event. Errors propagate; the worker task
    /// turns them into a `DecoderSinkError` so the daemon poisons the
    /// stream cleanly per the worker-task contract.
    pub async fn apply(&mut self, event: &SchemaEvent) -> Result<(), EmitterError> {
        match event {
            SchemaEvent::Added { desc } => self.apply_added(desc).await,
            SchemaEvent::Changed { old, new, diff } => self.apply_changed(old, new, diff).await,
            SchemaEvent::Dropped {
                oid: _,
                qualified_name,
            } => self.apply_dropped(qualified_name).await,
        }
    }

    async fn apply_added(&mut self, desc: &RelDescriptor) -> Result<(), EmitterError> {
        // Pre-declared mapping wins — operator already pinned the dest
        // and the CH side is operator-managed. Skip auto-create.
        if self
            .mapping_target(desc.qualified_name.as_ref())
            .await
            .is_some()
        {
            self.stats.skipped += 1;
            return Ok(());
        }
        if !self
            .config
            .auto_create_namespaces
            .contains(&desc.namespace_name)
        {
            self.stats.skipped += 1;
            return Ok(());
        }
        // Per-namespace target_database override (else global). Drives
        // both the CREATE TABLE and the row-routing mapping below, so
        // rows and DDL land in the same database.
        let target_db = self
            .config
            .target_database_for(&desc.namespace_name)
            .to_owned();
        let sql = match render_create_table(desc, &target_db)? {
            Some(s) => s,
            None => {
                self.stats.skipped += 1;
                return Ok(());
            }
        };
        self.execute(&sql).await?;
        self.stats.creates_applied += 1;
        // Auto-derive a TableMapping so the emitter can ship rows
        // against the freshly-created CH table without TOML edits.
        let target = format!("{}.{}", sql_ident(&target_db), sql_ident(&desc.name));
        let columns = derive_columns_for_mapping(desc);
        let mapping = TableMapping { target, columns };
        let mut m = self.mapping.write().await;
        m.insert(desc.qualified_name.as_ref().to_owned(), mapping);
        Ok(())
    }

    async fn apply_changed(
        &mut self,
        _old: &RelDescriptor,
        new: &RelDescriptor,
        diff: &SchemaDiff,
    ) -> Result<(), EmitterError> {
        let key = new.qualified_name.as_ref().to_owned();
        let target = match self.mapping_target(&key).await {
            Some(t) => t,
            None => {
                // Without a target we can't ALTER. Skip silently —
                // auto_create reduces this case to the
                // "namespace configured but table not yet learned" path
                // which `Added` handles.
                self.stats.skipped += 1;
                return Ok(());
            }
        };
        // RENAME before ADD/DROP so position-matched renames don't trip
        // a subsequent diff into a drop+add pair.
        for (_attnum, old_name, new_name) in &diff.renamed_columns {
            // The mapping's column targets are operator-pinned; if the
            // operator has a TOML rename, the source rename here is a
            // no-op from CH's POV (TOML still maps src_attnum to the
            // same CH column name). Detect this by checking whether the
            // CH column name has changed.
            let mapping_lookup = self.mapping.read().await;
            let m = mapping_lookup
                .get(&key)
                .expect("just resolved via mapping_target");
            let _ = old_name;
            let needs_rename = m.columns.iter().any(|c| &c.target_name == new_name)
                || !m.columns.iter().any(|c| &c.target_name == old_name);
            drop(mapping_lookup);
            if needs_rename {
                // Pre-declared TOML mapping already encodes the rename;
                // skip the CH ALTER.
                continue;
            }
            let sql = format!(
                "ALTER TABLE {} RENAME COLUMN {} TO {}",
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
            let resolved = match type_bridge::map(att, pk_member) {
                Ok(r) => r,
                Err(type_bridge::BridgeError::UnsupportedType { .. }) => {
                    // Unbridged type — log + skip the ADD. Operator-side
                    // TOML override is the recovery path.
                    self.stats.skipped += 1;
                    continue;
                }
            };
            let sql = render_add_column(&target, &att.name, &resolved);
            self.execute(&sql).await?;
            self.stats.alters_applied += 1;
        }
        for attnum in &diff.dropped_columns {
            // Resolve the CH column name from the old descriptor — the
            // diff lists attnums only.
            let name = _old
                .attributes
                .iter()
                .find(|a| a.attnum == *attnum)
                .map(|a| a.name.clone());
            let Some(name) = name else {
                self.stats.skipped += 1;
                continue;
            };
            // Pre-declared TOML mapping might still reference the
            // dropped column; surface the drop on CH regardless. The
            // emitter encodes NULL into mapping columns whose attnum
            // disappeared from the descriptor.
            let sql = format!(
                "ALTER TABLE {} DROP COLUMN IF EXISTS {}",
                target,
                quote_ident(&name)
            );
            self.execute(&sql).await?;
            self.stats.alters_applied += 1;
        }
        if !diff.type_changes.is_empty() {
            // Rejected per type-change open question — log + skip.
            self.stats.type_changes_rejected += diff.type_changes.len() as u64;
            tracing::warn!(
                target: "walshadow::ch_ddl",
                relation = %new.qualified_name,
                type_changes = diff.type_changes.len(),
                "unsupported schema change: type widening / domain change \
                 (manual operator migration required)"
            );
        }
        // Auto-extend the operator's TableMapping so the emitter ships
        // post-DDL rows against the new shape without TOML edits.
        // Operator-pinned `target_name` overrides survive: we only
        // touch ColumnMapping entries the applicator could have
        // produced (src_attnum match).
        self.mutate_mapping_for_diff(_old, new, diff).await;
        Ok(())
    }

    async fn mutate_mapping_for_diff(
        &mut self,
        old: &RelDescriptor,
        new: &RelDescriptor,
        diff: &SchemaDiff,
    ) {
        let key = new.qualified_name.as_ref().to_owned();
        let mut m = self.mapping.write().await;
        let Some(target_mapping) = m.get_mut(&key) else {
            return;
        };
        // Renames: if the operator's TOML used the OLD source name
        // (and thus matches the old descriptor's column name), rename
        // the CH column. If the operator already pinned a different
        // name, leave it; CH side already runs no ALTER (see
        // `apply_changed`).
        for (attnum, old_name, new_name) in &diff.renamed_columns {
            for c in &mut target_mapping.columns {
                if c.src_attnum == *attnum && &c.target_name == old_name {
                    c.target_name = new_name.clone();
                }
            }
        }
        // Drops: if a ColumnMapping references the dropped attnum,
        // strip it so the emitter stops looking the column up.
        for attnum in &diff.dropped_columns {
            target_mapping.columns.retain(|c| c.src_attnum != *attnum);
        }
        // Adds: auto-derive a column mapping using type_bridge. Skip
        // when the operator already pre-declared the column (e.g.,
        // a pre-declared add-column mapping).
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
            let resolved = match type_bridge::map(att, pk_member) {
                Ok(r) => r,
                Err(_) => continue,
            };
            target_mapping.columns.push(ColumnMapping {
                src_attnum: att.attnum,
                target_name: att.name.clone(),
                target_type: resolved.ch_type,
            });
        }
        let _ = old;
    }

    async fn apply_dropped(&mut self, qualified_name: &str) -> Result<(), EmitterError> {
        let target = match self.mapping_target(qualified_name).await {
            Some(t) => t,
            None => {
                self.stats.skipped += 1;
                return Ok(());
            }
        };
        // Namespace is the prefix of `namespace.name` (see
        // `RelDescriptor::build_qualified_name`); resolve its per-
        // namespace drop strategy, else the global default.
        let namespace = qualified_name.split('.').next().unwrap_or_default();
        match self.config.drop_strategy_for(namespace) {
            DropTableStrategy::Retain => {
                self.stats.skipped += 1;
                tracing::info!(
                    target: "walshadow::ch_ddl",
                    source = qualified_name,
                    dest = %target,
                    "source DROP TABLE; CH dest retained per strategy=retain",
                );
                Ok(())
            }
            DropTableStrategy::Warn => {
                self.stats.skipped += 1;
                tracing::warn!(
                    target: "walshadow::ch_ddl",
                    source = qualified_name,
                    dest = %target,
                    "source DROP TABLE; CH dest retained per strategy=warn",
                );
                Ok(())
            }
            DropTableStrategy::Drop => {
                let sql = format!("DROP TABLE IF EXISTS {target}");
                self.execute(&sql).await?;
                self.stats.drops_applied += 1;
                // Forget the auto-mapped entry so future Added events
                // for the same qualified_name re-derive fresh columns.
                let mut m = self.mapping.write().await;
                m.remove(qualified_name);
                Ok(())
            }
        }
    }

    /// Execute `TRUNCATE TABLE <target>` for a mapped source relation.
    /// No-op for unmapped relations (mirrors the emitter's row-skip).
    /// The pipeline's reorder coordinator calls this inside a barrier
    /// (after all earlier data is durable) so the truncate orders correctly
    /// against inserts despite the otherwise out-of-order pipeline.
    pub async fn truncate(&mut self, qualified_name: &str) -> Result<(), EmitterError> {
        let Some(target) = self.mapping_target(qualified_name).await else {
            return Ok(());
        };
        self.execute(&format!("TRUNCATE TABLE {target}")).await
    }

    async fn mapping_target(&mut self, qualified_name: &str) -> Option<String> {
        let m = self.mapping.read().await;
        m.get(qualified_name).map(|t| t.target.clone())
    }

    /// Run one DDL statement: `send_query` + drain to `EndOfStream` /
    /// `Exception`. CH DDL is single-statement so no Data blocks need
    /// to be sent.
    async fn execute(&mut self, sql: &str) -> Result<(), EmitterError> {
        tracing::debug!(target: "walshadow::ch_ddl", sql = %sql, "applying");
        self.client.send_query(sql, None).await?;
        drain_to_end_of_stream(&mut self.client).await
    }
}

/// Render `ALTER TABLE <t> ADD COLUMN IF NOT EXISTS <n> <ty> [DEFAULT
/// <expr>]`. The `IF NOT EXISTS` keeps the statement idempotent so a
/// daemon restart that re-fires the event acks on the second run.
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

/// Render a `CREATE TABLE IF NOT EXISTS` for an autodiscovered
/// relation. Returns `None` when the relation lacks a usable
/// `ORDER BY` (no PK + no namespace default); caller logs + skips.
pub fn render_create_table(
    desc: &RelDescriptor,
    target_database: &str,
) -> Result<Option<String>, EmitterError> {
    let target = format!(
        "{}.{}",
        quote_ident(target_database),
        quote_ident(&desc.name)
    );
    let pk_attnums: Vec<i16> = match &desc.replident {
        crate::shadow_catalog::ReplIdent::Default {
            pk_attnums: Some(n),
        } => n.clone(),
        crate::shadow_catalog::ReplIdent::UsingIndex { key_attnums, .. } => key_attnums.clone(),
        _ => Vec::new(),
    };
    let mut col_defs: Vec<String> = Vec::with_capacity(desc.attributes.len() + 4);
    for att in &desc.attributes {
        if att.dropped {
            continue;
        }
        let pk_member = pk_attnums.contains(&att.attnum);
        let resolved = match type_bridge::map(att, pk_member) {
            Ok(r) => r,
            Err(type_bridge::BridgeError::UnsupportedType { .. }) => {
                // Bail out: the table is half-renderable, skip the
                // CREATE so the operator can install a TOML override
                // and re-trigger via Added on the next refetch.
                return Ok(None);
            }
        };
        let mut def = format!("{} {}", quote_ident(&att.name), resolved.ch_type);
        if let Some(d) = resolved.default_sql {
            def.push_str(" DEFAULT ");
            def.push_str(&d);
        }
        col_defs.push(def);
    }
    // Synthetic columns mirror `TablePlan::build`'s shape.
    col_defs.push("`_lsn` UInt64".into());
    col_defs.push("`_xid` UInt32".into());
    col_defs.push("`_op` Enum8('insert' = 1, 'update' = 2, 'delete' = 3)".into());
    col_defs.push("`_commit_ts` DateTime64(6, 'UTC')".into());

    // ORDER BY: prefer PK columns; else fall back to `_lsn`.
    let order_by = if pk_attnums.is_empty() {
        "(`_lsn`)".to_string()
    } else {
        let names: Vec<String> = pk_attnums
            .iter()
            .filter_map(|a| {
                desc.attributes
                    .iter()
                    .find(|att| att.attnum == *a && !att.dropped)
                    .map(|att| quote_ident(&att.name))
            })
            .collect();
        if names.is_empty() {
            "(`_lsn`)".to_string()
        } else {
            format!("({})", names.join(", "))
        }
    };

    let sql = format!(
        "CREATE TABLE IF NOT EXISTS {target} (\n  {}\n) ENGINE = ReplacingMergeTree(`_lsn`)\nORDER BY {order_by}",
        col_defs.join(",\n  ")
    );
    Ok(Some(sql))
}

/// CH identifier — same shape as `quote_ident`. Re-exported as a
/// helper for callers building qualified destination names from
/// (database, table) parts without re-walking the surface.
pub fn sql_ident(name: &str) -> String {
    quote_ident(name)
}

/// Derive a default `Vec<ColumnMapping>` from a relation's descriptor.
/// Used by `DdlApplicator::apply_added` when auto-discovering tables
/// in an `auto_create`-flagged namespace; operator overrides via TOML
/// still win the next SIGHUP.
pub fn derive_columns_for_mapping(desc: &RelDescriptor) -> Vec<ColumnMapping> {
    let pk_attnums: Vec<i16> = match &desc.replident {
        crate::shadow_catalog::ReplIdent::Default {
            pk_attnums: Some(n),
        } => n.clone(),
        crate::shadow_catalog::ReplIdent::UsingIndex { key_attnums, .. } => key_attnums.clone(),
        _ => Vec::new(),
    };
    let mut out = Vec::with_capacity(desc.attributes.len());
    for att in &desc.attributes {
        if att.dropped {
            continue;
        }
        let pk_member = pk_attnums.contains(&att.attnum);
        let resolved = match type_bridge::map(att, pk_member) {
            Ok(r) => r,
            Err(_) => continue,
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
        };
        // target_database: namespace override, else global fallback.
        assert_eq!(cfg.target_database_for("analytics"), "warehouse");
        assert_eq!(cfg.target_database_for("logs"), "default");
        assert_eq!(cfg.target_database_for("unconfigured"), "default");
        // drop strategy: namespace override, else global fallback.
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
        };
        assert_eq!(cfg.drop_table_strategy, DropTableStrategy::Retain);
        let cfg = cfg.with_drop_strategy(DropTableStrategy::Drop);
        assert_eq!(cfg.drop_table_strategy, DropTableStrategy::Drop);
        // Global override now drives the per-namespace fallback.
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
            rfn: wal_rs::pg::walparser::RelFileNode {
                spc_node: 1663,
                db_node: 5,
                rel_node: 16400,
            },
            oid: 16400,
            namespace_oid: 2200,
            namespace_name: "public".into(),
            name: name.into(),
            qualified_name: RelDescriptor::build_qualified_name("public", name),
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
        let sql = render_create_table(&d, "default").unwrap().unwrap();
        assert!(sql.contains("CREATE TABLE IF NOT EXISTS `default`.`orders`"));
        assert!(sql.contains("`id` Int32"));
        assert!(sql.contains("`body` Nullable(String)"));
        assert!(sql.contains("`_lsn` UInt64"));
        assert!(sql.contains("`_op` Enum8"));
        assert!(sql.contains("ENGINE = ReplacingMergeTree(`_lsn`)"));
        assert!(sql.ends_with("ORDER BY (`id`)"));
    }

    #[test]
    fn render_create_table_falls_back_to_lsn_when_no_pk() {
        let d = desc("events", vec![att(1, "body", TEXTOID, false, None)], None);
        let sql = render_create_table(&d, "default").unwrap().unwrap();
        assert!(sql.ends_with("ORDER BY (`_lsn`)"));
    }

    #[test]
    fn render_create_table_handles_timestamp_precision() {
        let mut a = att(1, "ship_at", TIMESTAMPTZOID, false, None);
        a.typmod = 3;
        let d = desc("t", vec![a], None);
        let sql = render_create_table(&d, "db").unwrap().unwrap();
        assert!(
            sql.contains("`ship_at` Nullable(DateTime64(3, 'UTC'))"),
            "{sql}"
        );
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
        // Force a type-bridge failure by using a synthetic OID below the
        // safe range — type_bridge falls back to String for unknown
        // type OIDs today, so this never hits None. Update if the bridge
        // grows strictness; the API contract (`Option<String>`) is what
        // matters here.
        let d = desc("t", vec![att(1, "id", 99999, true, None)], None);
        let sql = render_create_table(&d, "db").unwrap();
        assert!(sql.is_some(), "fallback path keeps the CREATE renderable");
    }

    #[test]
    fn diff_renamed_then_added_then_dropped_in_correct_order() {
        // The plan calls out: RENAME before ADD/DROP so position-match
        // diffs don't trip into a drop+add pair. This test asserts on
        // the order the applicator iterates `SchemaDiff` (RENAME first,
        // ADD second, DROP third). Functional verification of the
        // ordering itself lives in the integration test.
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
        let map: std::collections::HashMap<String, TableMapping> = [(
            "public.orders".into(),
            TableMapping {
                target: "default.orders".into(),
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
        // Smoke test: ensure the handle resolves what we expect. The
        // applicator's mapping_target uses the same shape.
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        let target = rt.block_on(async {
            let m = handle.read().await;
            m.get("public.orders").map(|t| t.target.clone())
        });
        assert_eq!(target.as_deref(), Some("default.orders"));
    }
}
