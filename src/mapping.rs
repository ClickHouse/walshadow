//! ClickHouse destination mapping vocabulary

use std::sync::Arc;

use crate::catalog::type_bridge::{self, ResolvedColumn};
use crate::column_rules::{ColumnRule, ColumnRules};
use crate::schema::{RelAttr, RelDescriptor, RelName, SchemaDiff, replident_key_attnums};
use ahash::HashMap;
use tokio::sync::RwLock;

#[derive(Debug, Clone)]
pub struct TableMapping {
    pub target: TableTarget,
    pub columns: Vec<ColumnMapping>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableTarget {
    pub database: String,
    pub table: String,
}

impl TableTarget {
    pub fn new(database: &str, table: &str) -> Self {
        Self {
            database: database.into(),
            table: table.into(),
        }
    }

    pub fn sql(&self) -> String {
        format!(
            "{}.{}",
            quote_ident(&self.database),
            quote_ident(&self.table)
        )
    }
}

impl std::fmt::Display for TableTarget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}", self.database, self.table)
    }
}

#[derive(Debug, Clone, Default)]
pub struct NamespaceMapping {
    pub target_database: Option<String>,
    pub auto_create: bool,
    pub drop_table_strategy: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum DropTableStrategy {
    #[default]
    Retain,
    Drop,
    Warn,
}

impl DropTableStrategy {
    pub fn parse(s: &str) -> Result<Self, String> {
        match s.to_ascii_lowercase().as_str() {
            "retain" => Ok(Self::Retain),
            "drop" => Ok(Self::Drop),
            "warn" => Ok(Self::Warn),
            other => Err(format!(
                "unknown drop-table-strategy {other:?} (expected retain / drop / warn)"
            )),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ColumnMapping {
    pub src_attnum: i16,
    pub target_name: String,
    pub target_type: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ToastMode {
    #[default]
    Disabled,
    ClickHouse,
}

impl ToastMode {
    pub fn parse(s: &str) -> Result<Self, String> {
        match s.trim().to_ascii_lowercase().as_str() {
            "disabled" | "off" | "none" | "" => Ok(Self::Disabled),
            "clickhouse" | "ch" => Ok(Self::ClickHouse),
            other => Err(format!(
                "unknown toast mode `{other}` (expected disabled / clickhouse)"
            )),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct ToastConfig {
    pub mode: ToastMode,
}

/// Immutable routing-map version. Planners snapshot one per transaction so a
/// concurrent republish can't split a transaction across mapping versions
pub type MappingSnapshot = Arc<HashMap<RelName, TableMapping>>;

pub type MappingHandle = Arc<MappingCell>;

/// Copy-on-write routing map. Writers `Arc::make_mut` or swap the inner
/// snapshot, so a snapshot taken for one unit of work stays frozen
#[derive(Debug)]
pub struct MappingCell {
    inner: RwLock<MappingSnapshot>,
}

impl MappingCell {
    pub async fn with<R>(&self, f: impl FnOnce(&MappingSnapshot) -> R) -> R {
        f(&*self.inner.read().await)
    }

    pub async fn snapshot(&self) -> MappingSnapshot {
        self.inner.read().await.clone()
    }

    /// `false` once a fold or republish displaced `at`. Holding `at` keeps
    /// its allocation alive and forces `Arc::make_mut` to clone, so pointer
    /// identity answers "has the map moved since I looked?"
    pub async fn unmoved(&self, at: &MappingSnapshot) -> bool {
        Arc::ptr_eq(&*self.inner.read().await, at)
    }

    pub async fn mutate<R>(&self, f: impl FnOnce(&mut MappingSnapshot) -> R) -> R {
        f(&mut *self.inner.write().await)
    }

    pub async fn publish(&self, tables: MappingSnapshot) {
        *self.inner.write().await = tables;
    }
}

pub fn mapping_handle(tables: HashMap<RelName, TableMapping>) -> MappingHandle {
    Arc::new(MappingCell {
        inner: RwLock::new(Arc::new(tables)),
    })
}

/// CH name and type a rule states for an attribute, falling back to the
/// bridge. A stated type drops the bridge default, which belonged to the
/// type the rule just replaced
pub fn apply_column_rule(
    attname: &str,
    resolved: ResolvedColumn,
    rule: ColumnRule,
) -> (String, ResolvedColumn) {
    (
        rule.target_name.unwrap_or_else(|| attname.to_owned()),
        rule.target_type.map_or(resolved, |ch_type| ResolvedColumn {
            ch_type,
            default_sql: None,
        }),
    )
}

pub fn map_column(
    rel: &RelName,
    attr: &RelAttr,
    pk_member: bool,
    rules: &ColumnRules,
) -> Option<ColumnMapping> {
    let resolved = type_bridge::map(attr, pk_member).ok()?;
    let (target_name, resolved) =
        apply_column_rule(&attr.name, resolved, rules.settings(rel, &attr.name));
    Some(ColumnMapping {
        src_attnum: attr.attnum,
        target_name,
        target_type: resolved.ch_type,
    })
}

pub fn derive_columns_for_mapping(desc: &RelDescriptor, rules: &ColumnRules) -> Vec<ColumnMapping> {
    let keys = replident_key_attnums(desc);
    desc.attributes
        .iter()
        .filter(|attr| !attr.dropped)
        .filter_map(|attr| map_column(&desc.rel_name, attr, keys.contains(&attr.attnum), rules))
        .collect()
}

pub fn fold_diff_into_mapping(
    target: &mut TableMapping,
    new: &RelDescriptor,
    diff: &SchemaDiff,
    rules: &ColumnRules,
) {
    for (attnum, old_name, new_name) in &diff.renamed_columns {
        // Preserve configured target name after source rename
        let renamed_to = rules
            .settings(&new.rel_name, new_name)
            .target_name
            .unwrap_or_else(|| new_name.clone());
        for column in &mut target.columns {
            if column.src_attnum == *attnum && column.target_name == *old_name {
                column.target_name.clone_from(&renamed_to);
            }
        }
    }
    target
        .columns
        .retain(|column| !diff.dropped_columns.contains(&column.src_attnum));
    for attr in &diff.added_columns {
        if target.columns.iter().any(|c| c.src_attnum == attr.attnum) {
            continue;
        }
        let key = replident_key_attnums(new).contains(&attr.attnum);
        if let Some(column) = map_column(&new.rel_name, attr, key, rules) {
            target.columns.push(column);
        }
    }
}

fn quote_ident(name: &str) -> String {
    format!("`{}`", name.replace('`', "``"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn one_table() -> (RelName, HashMap<RelName, TableMapping>) {
        let rel = RelName::new("public", "t");
        let map = HashMap::from_iter([(
            rel.clone(),
            TableMapping {
                target: TableTarget::new("db", "t"),
                columns: vec![],
            },
        )]);
        (rel, map)
    }

    /// Held snapshot stays frozen under both writer shapes — a planned
    /// transaction's route state can't be altered by a later mapping write
    #[tokio::test]
    async fn snapshot_immune_to_later_writes() {
        let (rel, map) = one_table();
        let handle = mapping_handle(map);
        // Applicator shape: make_mut clones out from under held snapshots
        let planned: MappingSnapshot = handle.snapshot().await;
        handle.mutate(|m| Arc::make_mut(m).remove(&rel)).await;
        assert!(planned.contains_key(&rel), "snapshot keeps its version");
        assert!(
            !handle.with(|m| m.contains_key(&rel)).await,
            "handle moved on"
        );
        // Republish shape: full inner-Arc swap
        let planned = handle.snapshot().await;
        let (rel2, map2) = one_table();
        handle.publish(Arc::new(map2)).await;
        assert!(!planned.contains_key(&rel2), "snapshot predates the swap");
        assert!(handle.with(|m| m.contains_key(&rel2)).await);
    }

    fn attr(attnum: i16, name: &str, type_oid: u32) -> crate::schema::RelAttr {
        crate::schema::RelAttr {
            attnum,
            name: name.into(),
            type_oid,
            typmod: -1,
            not_null: true,
            dropped: false,
            type_name: String::new(),
            type_byval: true,
            type_len: 4,
            type_align: 'i',
            type_storage: 'p',
            missing_text: None,
        }
    }

    fn events_desc(attrs: Vec<crate::schema::RelAttr>) -> RelDescriptor {
        RelDescriptor {
            rfn: walrus::pg::walparser::RelFileNode {
                spc_node: 1663,
                db_node: 5,
                rel_node: 16385,
            },
            oid: 16385,
            toast_oid: 0,
            namespace_oid: 2200,
            rel_name: RelName::new("app", "events"),
            kind: 'r',
            persistence: 'p',
            replident: crate::schema::ReplIdent::Default { pk_attnums: None },
            attributes: attrs,
        }
    }

    fn amount_rules() -> ColumnRules {
        let mut b = crate::column_rules::ColumnRulesBuilder::new();
        b.add(
            &RelName::new("app", "events"),
            crate::table_rules::MatchKind::Exact,
            "legacy_id",
            crate::table_rules::MatchKind::Exact,
            crate::column_rules::ColumnRule {
                target_name: Some("id".into()),
                target_type: None,
            },
        );
        b.add(
            &RelName::new("app", "*"),
            crate::table_rules::MatchKind::Glob,
            "*_amount",
            crate::table_rules::MatchKind::Glob,
            crate::column_rules::ColumnRule {
                target_name: None,
                target_type: Some("Decimal(38, 9)".into()),
            },
        );
        b.finish().0
    }

    #[test]
    fn derived_columns_take_rule_name_and_type() {
        let desc = events_desc(vec![
            attr(1, "legacy_id", crate::schema::INT4OID),
            attr(2, "net_amount", crate::schema::NUMERICOID),
            attr(3, "note", crate::schema::TEXTOID),
        ]);
        let columns = derive_columns_for_mapping(&desc, &amount_rules());
        assert_eq!(columns[0].src_attnum, 1);
        assert_eq!(columns[0].target_name, "id", "rule renames");
        assert_eq!(columns[1].target_type, "Decimal(38, 9)", "glob retypes");
        assert_eq!(columns[2].target_name, "note");
        assert_eq!(columns[2].target_type, "String", "unmatched keeps bridge");
    }

    #[test]
    fn folded_column_takes_the_rule_its_name_matches() {
        let rules = amount_rules();
        let old = events_desc(vec![attr(1, "legacy_id", crate::schema::INT4OID)]);
        let mut mapping = TableMapping {
            target: TableTarget::new("db", "events"),
            columns: derive_columns_for_mapping(&old, &rules),
        };
        let added = attr(2, "gross_amount", crate::schema::NUMERICOID);
        let new = events_desc(vec![
            attr(1, "legacy_id", crate::schema::INT4OID),
            added.clone(),
        ]);
        fold_diff_into_mapping(
            &mut mapping,
            &new,
            &SchemaDiff {
                added_columns: vec![added],
                dropped_columns: vec![],
                renamed_columns: vec![],
                type_changes: vec![],
            },
            &rules,
        );
        assert_eq!(mapping.columns[1].target_type, "Decimal(38, 9)");
    }

    #[test]
    fn rename_lands_on_the_name_the_rule_states() {
        let rules = amount_rules();
        let mut mapping = TableMapping {
            target: TableTarget::new("db", "events"),
            columns: vec![ColumnMapping {
                src_attnum: 1,
                target_name: "id_v1".into(),
                target_type: "Int32".into(),
            }],
        };
        let new = events_desc(vec![attr(1, "legacy_id", crate::schema::INT4OID)]);
        fold_diff_into_mapping(
            &mut mapping,
            &new,
            &SchemaDiff {
                added_columns: vec![],
                dropped_columns: vec![],
                renamed_columns: vec![(1, "id_v1".into(), "legacy_id".into())],
                type_changes: vec![],
            },
            &rules,
        );
        assert_eq!(mapping.columns[0].target_name, "id");
    }
}
