//! ClickHouse destination mapping vocabulary

use std::collections::HashMap;
use std::sync::Arc;

use crate::catalog::type_bridge;
use crate::schema::{RelDescriptor, RelName, SchemaDiff, replident_key_attnums};

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

/// Shared copy-on-write routing map: writers swap or `Arc::make_mut` the
/// inner snapshot, held snapshots stay frozen
pub type MappingHandle = Arc<tokio::sync::RwLock<MappingSnapshot>>;

pub fn mapping_handle(tables: HashMap<RelName, TableMapping>) -> MappingHandle {
    Arc::new(tokio::sync::RwLock::new(Arc::new(tables)))
}

pub fn derive_columns_for_mapping(desc: &RelDescriptor) -> Vec<ColumnMapping> {
    let keys = replident_key_attnums(desc);
    desc.attributes
        .iter()
        .filter(|attr| !attr.dropped)
        .filter_map(|attr| {
            type_bridge::map(attr, keys.contains(&attr.attnum))
                .ok()
                .map(|resolved| ColumnMapping {
                    src_attnum: attr.attnum,
                    target_name: attr.name.clone(),
                    target_type: resolved.ch_type,
                })
        })
        .collect()
}

pub fn fold_diff_into_mapping(target: &mut TableMapping, new: &RelDescriptor, diff: &SchemaDiff) {
    for (attnum, old_name, new_name) in &diff.renamed_columns {
        for column in &mut target.columns {
            if column.src_attnum == *attnum && column.target_name == *old_name {
                column.target_name.clone_from(new_name);
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
        if let Ok(resolved) = type_bridge::map(attr, key) {
            target.columns.push(ColumnMapping {
                src_attnum: attr.attnum,
                target_name: attr.name.clone(),
                target_type: resolved.ch_type,
            });
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
        let map = HashMap::from([(
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
        let planned: MappingSnapshot = handle.read().await.clone();
        Arc::make_mut(&mut *handle.write().await).remove(&rel);
        assert!(planned.contains_key(&rel), "snapshot keeps its version");
        assert!(!handle.read().await.contains_key(&rel), "handle moved on");
        // Republish shape: full inner-Arc swap
        let planned = handle.read().await.clone();
        let (rel2, map2) = one_table();
        *handle.write().await = Arc::new(map2);
        assert!(!planned.contains_key(&rel2), "snapshot predates the swap");
        assert!(handle.read().await.contains_key(&rel2));
    }
}
