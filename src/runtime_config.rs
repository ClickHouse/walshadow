//! Source-PG runtime config overlay: the typed in-memory state layer 2 of the
//! [`crate::config::ConfigResolver`] merges (CLI > PG-row > TOML), plus the WAL
//! tuple interpreter that feeds it.
//!
//! Config rows live in operator-owned `<schema>.config_*` tables on source PG
//! (see `sql/runtime_config_install.sql`). The daemon reads them at boot
//! (`SELECT *`, [`crate::config::ConfigResolver::seed_overlay`]) and tracks
//! live edits off the WAL stream: a config-table heap write is detected in the
//! decode path by resolved qualified name, interpreted here into a
//! [`ConfigEvent`], and applied at the row's commit LSN.
//!
//! **Full-row events, no delta merge.** The install script sets
//! `REPLICA IDENTITY FULL`. At walshadow's `wal_level=logical` floor PG logs
//! the new tuple whole (prefix/suffix compression is off for logically-logged
//! relations), so INSERT/UPDATE already carry every column; FULL adds the
//! complete old image, so DELETE always carries the key columns regardless of
//! the table's primary-key shape. [`interpret`] thus builds each event from the
//! single record with no dependency on prior daemon state, so events carry whole
//! typed rows and [`ConfigOverlay::apply`] just replaces the entry. Values are
//! validated late, at resolver merge time, not here.

use std::collections::HashMap;

use crate::heap_decoder::{ColumnValue, DecodedHeap, HeapOp};
use crate::shadow_catalog::{RelDescriptor, RelName};

pub const CONFIG_GLOBAL: &str = "config_global";
pub const CONFIG_NAMESPACE: &str = "config_namespace";
pub const CONFIG_TABLE: &str = "config_table";
pub const CONFIG_COLUMN: &str = "config_column";

/// Which live-tracked overlay table a config-schema relation is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfigTableKind {
    Global,
    Namespace,
    Table,
    Column,
}

impl ConfigTableKind {
    pub fn from_relname(relname: &str) -> Option<Self> {
        match relname {
            CONFIG_GLOBAL => Some(Self::Global),
            CONFIG_NAMESPACE => Some(Self::Namespace),
            CONFIG_TABLE => Some(Self::Table),
            CONFIG_COLUMN => Some(Self::Column),
            _ => None,
        }
    }
}

/// `config_global` row (singleton). Raw values; validated at resolver merge.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct GlobalRow {
    pub row_budget: Option<i64>,
    pub byte_budget: Option<i64>,
    pub flush_timeout_ms: Option<i64>,
    pub compression: Option<String>,
    pub retry_max_attempts: Option<i64>,
    pub drop_table_strategy: Option<String>,
}

/// `config_namespace` row (key = `namespace`).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct NamespaceRow {
    pub target_database: Option<String>,
    pub auto_create: Option<bool>,
    pub drop_table_strategy: Option<String>,
}

/// `config_table` row (key = `(namespace, relname)`).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct TableRow {
    /// CH destination override, one part per column; NULL = that part
    /// derived (namespace target_database / source relname)
    pub target_database: Option<String>,
    pub target_table: Option<String>,
    /// Inclusion switch: `Some(true)` opt-in, `Some(false)` opt-out, `None`
    /// leaves scope unchanged (legacy target-override-only behavior).
    pub replicate: Option<bool>,
    /// One-time backfill mode for pre-opt-in rows, raw ([`InitialLoadMode`]
    /// parses at dispatch in [`crate::opt_in`], validate-late like every
    /// overlay value); absent / `none` streams from opt-in LSN.
    pub initial_load: Option<String>,
}

/// Parsed `config_table.initial_load` mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum InitialLoadMode {
    /// No backfill, stream from opt-in LSN only.
    None,
    /// Snapshot-free COPY at `_lsn = S` ([`crate::copy_backfill`]).
    Copy,
    /// Fresh `BASE_BACKUP` page-walk filtered to the opted-in rels
    /// (plans/add_table.md).
    BaseBackup,
    /// Object-store base backup + archive-WAL gap replay, filtered
    /// (plans/add_table.md).
    ObjectStore,
}

impl InitialLoadMode {
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "none" => Some(Self::None),
            "copy" => Some(Self::Copy),
            "base_backup" => Some(Self::BaseBackup),
            "object_store" => Some(Self::ObjectStore),
            _ => None,
        }
    }

    /// Inverse of [`Self::parse`]; ledger serialization + metrics labels.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Copy => "copy",
            Self::BaseBackup => "base_backup",
            Self::ObjectStore => "object_store",
        }
    }
}

/// `config_column` row (key = `(namespace.relname, attname)`).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ColumnRow {
    pub target_type: Option<String>,
}

/// One applied config change, interpreted from a config-table heap write and
/// carried through [`crate::xact_buffer::DrainEntry::Config`] to apply at the
/// row's commit LSN.
#[derive(Debug, Clone, PartialEq)]
pub enum ConfigEvent {
    GlobalUpserted(GlobalRow),
    /// The singleton row was deleted; global knobs fall back to TOML/CLI.
    GlobalCleared,
    NamespaceUpserted {
        namespace: String,
        row: NamespaceRow,
    },
    NamespaceRemoved {
        namespace: String,
    },
    TableUpserted {
        rel: RelName,
        row: TableRow,
    },
    TableRemoved {
        rel: RelName,
    },
    ColumnUpserted {
        rel: RelName,
        attname: String,
        row: ColumnRow,
    },
    ColumnRemoved {
        rel: RelName,
        attname: String,
    },
}

impl ConfigEvent {
    /// Terse label for tracing/metrics.
    pub fn kind_str(&self) -> &'static str {
        match self {
            Self::GlobalUpserted(_) | Self::GlobalCleared => "global",
            Self::NamespaceUpserted { .. } | Self::NamespaceRemoved { .. } => "namespace",
            Self::TableUpserted { .. } | Self::TableRemoved { .. } => "table",
            Self::ColumnUpserted { .. } | Self::ColumnRemoved { .. } => "column",
        }
    }
}

/// Typed in-memory overlay: the config_* rows as the resolver's layer-2 input.
/// Re-derivable (boot `SELECT *` + WAL replay), so it holds no checkpoint.
#[derive(Debug, Clone, Default)]
pub struct ConfigOverlay {
    pub global: Option<GlobalRow>,
    pub namespaces: HashMap<String, NamespaceRow>,
    pub tables: HashMap<RelName, TableRow>,
    pub columns: HashMap<(RelName, String), ColumnRow>,
}

impl ConfigOverlay {
    /// Apply one event. Full-row upserts replace; removes drop the entry.
    pub fn apply(&mut self, event: ConfigEvent) {
        match event {
            ConfigEvent::GlobalUpserted(row) => self.global = Some(row),
            ConfigEvent::GlobalCleared => self.global = None,
            ConfigEvent::NamespaceUpserted { namespace, row } => {
                self.namespaces.insert(namespace, row);
            }
            ConfigEvent::NamespaceRemoved { namespace } => {
                self.namespaces.remove(&namespace);
            }
            ConfigEvent::TableUpserted { rel, row } => {
                self.tables.insert(rel, row);
            }
            ConfigEvent::TableRemoved { rel } => {
                self.tables.remove(&rel);
            }
            ConfigEvent::ColumnUpserted { rel, attname, row } => {
                self.columns.insert((rel, attname), row);
            }
            ConfigEvent::ColumnRemoved { rel, attname } => {
                self.columns.remove(&(rel, attname));
            }
        }
    }
}

/// Reconstruct the full row image touched by a config-table heap write.
///
/// INSERT/UPDATE: the new tuple. At `wal_level=logical` PG logs it whole, so
/// every column is present; the per-column else-old-image arm is defensive
/// backfill that does not engage for these logically-logged tables. DELETE:
/// the old image, for the row key (`REPLICA IDENTITY FULL` keeps it complete).
/// `None` for ops with no usable image (TRUNCATE, or an UPDATE lacking a new
/// image).
fn full_image(decoded: &DecodedHeap) -> Option<Vec<Option<ColumnValue>>> {
    match decoded.op {
        HeapOp::Insert => decoded.new.as_ref().map(|t| t.columns.clone()),
        HeapOp::Update | HeapOp::HotUpdate => {
            let new = decoded.new.as_ref()?;
            let old = decoded.old.as_ref();
            Some(
                new.columns
                    .iter()
                    .enumerate()
                    .map(|(i, nv)| match nv {
                        Some(v) => Some(v.clone()),
                        None => old.and_then(|o| o.columns.get(i).cloned().flatten()),
                    })
                    .collect(),
            )
        }
        HeapOp::Delete => decoded.old.as_ref().map(|t| t.columns.clone()),
        HeapOp::Truncate => None,
    }
}

/// `Some(&value)` when the named column is present in the image (including an
/// explicit SQL NULL as `ColumnValue::Null`); `None` when absent.
fn column<'a>(
    rel: &RelDescriptor,
    cols: &'a [Option<ColumnValue>],
    name: &str,
) -> Option<&'a ColumnValue> {
    let att = rel
        .attributes
        .iter()
        .find(|a| a.name == name && !a.dropped)?;
    cols.get((att.attnum - 1).max(0) as usize)?.as_ref()
}

fn field_i64(rel: &RelDescriptor, cols: &[Option<ColumnValue>], name: &str) -> Option<i64> {
    match column(rel, cols, name)? {
        ColumnValue::Int8(v) => Some(*v),
        ColumnValue::Int4(v) => Some(*v as i64),
        ColumnValue::Int2(v) => Some(*v as i64),
        _ => None,
    }
}

fn field_bool(rel: &RelDescriptor, cols: &[Option<ColumnValue>], name: &str) -> Option<bool> {
    match column(rel, cols, name)? {
        ColumnValue::Bool(v) => Some(*v),
        _ => None,
    }
}

fn field_string(rel: &RelDescriptor, cols: &[Option<ColumnValue>], name: &str) -> Option<String> {
    match column(rel, cols, name)? {
        ColumnValue::Text(v) | ColumnValue::Name(v) | ColumnValue::Json(v) => Some(v.clone()),
        _ => None,
    }
}

/// Interpret a config-table heap write into a [`ConfigEvent`]. `rel` must
/// describe the same relation `decoded` targets. `None` when the write carries
/// no usable image or the row key is missing.
pub fn interpret(
    kind: ConfigTableKind,
    decoded: &DecodedHeap,
    rel: &RelDescriptor,
) -> Option<ConfigEvent> {
    let removed = matches!(decoded.op, HeapOp::Delete);
    let cols = full_image(decoded)?;

    match kind {
        ConfigTableKind::Global => {
            if removed {
                return Some(ConfigEvent::GlobalCleared);
            }
            Some(ConfigEvent::GlobalUpserted(GlobalRow {
                row_budget: field_i64(rel, &cols, "row_budget"),
                byte_budget: field_i64(rel, &cols, "byte_budget"),
                flush_timeout_ms: field_i64(rel, &cols, "flush_timeout_ms"),
                compression: field_string(rel, &cols, "compression"),
                retry_max_attempts: field_i64(rel, &cols, "retry_max_attempts"),
                drop_table_strategy: field_string(rel, &cols, "drop_table_strategy"),
            }))
        }
        ConfigTableKind::Namespace => {
            let namespace = field_string(rel, &cols, "namespace")?;
            if removed {
                return Some(ConfigEvent::NamespaceRemoved { namespace });
            }
            Some(ConfigEvent::NamespaceUpserted {
                namespace,
                row: NamespaceRow {
                    target_database: field_string(rel, &cols, "target_database"),
                    auto_create: field_bool(rel, &cols, "auto_create"),
                    drop_table_strategy: field_string(rel, &cols, "drop_table_strategy"),
                },
            })
        }
        ConfigTableKind::Table => {
            let key = RelName::new(
                &field_string(rel, &cols, "namespace")?,
                &field_string(rel, &cols, "relname")?,
            );
            if removed {
                return Some(ConfigEvent::TableRemoved { rel: key });
            }
            Some(ConfigEvent::TableUpserted {
                rel: key,
                row: TableRow {
                    target_database: field_string(rel, &cols, "target_database"),
                    target_table: field_string(rel, &cols, "target_table"),
                    replicate: field_bool(rel, &cols, "replicate"),
                    initial_load: field_string(rel, &cols, "initial_load"),
                },
            })
        }
        ConfigTableKind::Column => {
            let key = RelName::new(
                &field_string(rel, &cols, "namespace")?,
                &field_string(rel, &cols, "relname")?,
            );
            let attname = field_string(rel, &cols, "attname")?;
            if removed {
                return Some(ConfigEvent::ColumnRemoved { rel: key, attname });
            }
            Some(ConfigEvent::ColumnUpserted {
                rel: key,
                attname,
                row: ColumnRow {
                    target_type: field_string(rel, &cols, "target_type"),
                },
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::heap_decoder::{ColumnValue, DecodedHeap, DecodedTuple, HeapOp};
    use crate::shadow_catalog::{RelAttr, RelDescriptor, ReplIdent};
    use walrus::pg::walparser::RelFileNode;

    fn attr(attnum: i16, name: &str, type_oid: u32) -> RelAttr {
        RelAttr {
            attnum,
            name: name.into(),
            type_oid,
            typmod: -1,
            not_null: false,
            dropped: false,
            type_name: String::new(),
            type_byval: true,
            type_len: 8,
            type_align: 'd',
            type_storage: 'p',
            missing_text: None,
        }
    }

    fn rel(name: &str, attrs: Vec<RelAttr>) -> RelDescriptor {
        RelDescriptor {
            rfn: RelFileNode {
                spc_node: 1663,
                db_node: 5,
                rel_node: 20000,
            },
            oid: 20000,
            namespace_oid: 2200,
            rel_name: RelName::new("walshadow", name),
            kind: 'r',
            persistence: 'p',
            replident: ReplIdent::Full { pk_attnums: None },
            attributes: attrs,
        }
    }

    fn heap(
        op: HeapOp,
        new: Option<Vec<Option<ColumnValue>>>,
        old: Option<Vec<Option<ColumnValue>>>,
    ) -> DecodedHeap {
        DecodedHeap {
            rfn: RelFileNode {
                spc_node: 1663,
                db_node: 5,
                rel_node: 20000,
            },
            xid: 42,
            source_lsn: 0x9000,
            op,
            new: new.map(|columns| DecodedTuple {
                columns,
                partial: false,
            }),
            old: old.map(|columns| DecodedTuple {
                columns,
                partial: false,
            }),
        }
    }

    fn global_rel() -> RelDescriptor {
        rel(
            CONFIG_GLOBAL,
            vec![
                attr(1, "id", 21),
                attr(2, "row_budget", 20),
                attr(3, "byte_budget", 20),
                attr(4, "flush_timeout_ms", 20),
                attr(5, "compression", 25),
                attr(6, "retry_max_attempts", 23),
                attr(7, "drop_table_strategy", 25),
            ],
        )
    }

    #[test]
    fn global_insert_reads_all_fields() {
        let d = heap(
            HeapOp::Insert,
            Some(vec![
                Some(ColumnValue::Int2(1)),
                Some(ColumnValue::Int8(1000)),
                Some(ColumnValue::Null), // byte_budget NULL → daemon default
                Some(ColumnValue::Int8(250)),
                Some(ColumnValue::Text("zstd".into())),
                Some(ColumnValue::Int4(9)),
                Some(ColumnValue::Text("drop".into())),
            ]),
            None,
        );
        match interpret(ConfigTableKind::Global, &d, &global_rel()).unwrap() {
            ConfigEvent::GlobalUpserted(r) => {
                assert_eq!(r.row_budget, Some(1000));
                assert_eq!(r.byte_budget, None);
                assert_eq!(r.flush_timeout_ms, Some(250));
                assert_eq!(r.compression.as_deref(), Some("zstd"));
                assert_eq!(r.retry_max_attempts, Some(9));
                assert_eq!(r.drop_table_strategy.as_deref(), Some("drop"));
            }
            other => panic!("expected GlobalUpserted, got {other:?}"),
        }
    }

    /// `full_image` takes each column from the new image, falling back to the
    /// FULL old image where absent. Defensive: at `wal_level=logical` the new
    /// image is whole for these tables, so this exercises it with synthetic gaps.
    #[test]
    fn global_update_backfills_absent_columns_from_old() {
        let new = vec![
            None,
            None,
            Some(ColumnValue::Int8(2048)),
            None,
            None,
            None,
            None,
        ];
        let old = vec![
            Some(ColumnValue::Int2(1)),
            Some(ColumnValue::Int8(1000)),
            Some(ColumnValue::Int8(999)),
            Some(ColumnValue::Int8(250)),
            Some(ColumnValue::Text("lz4".into())),
            Some(ColumnValue::Int4(5)),
            Some(ColumnValue::Text("retain".into())),
        ];
        match interpret(
            ConfigTableKind::Global,
            &heap(HeapOp::Update, Some(new), Some(old)),
            &global_rel(),
        )
        .unwrap()
        {
            ConfigEvent::GlobalUpserted(r) => {
                assert_eq!(r.byte_budget, Some(2048), "changed column from new image");
                assert_eq!(r.row_budget, Some(1000), "absent column filled from old");
                assert_eq!(
                    r.compression.as_deref(),
                    Some("lz4"),
                    "absent, filled from old"
                );
            }
            other => panic!("expected GlobalUpserted, got {other:?}"),
        }
    }

    #[test]
    fn global_delete_clears() {
        let old = vec![
            Some(ColumnValue::Int2(1)),
            Some(ColumnValue::Int8(1)),
            Some(ColumnValue::Int8(1)),
            Some(ColumnValue::Int8(1)),
            Some(ColumnValue::Text("lz4".into())),
            Some(ColumnValue::Int4(1)),
            Some(ColumnValue::Text("retain".into())),
        ];
        assert_eq!(
            interpret(
                ConfigTableKind::Global,
                &heap(HeapOp::Delete, None, Some(old)),
                &global_rel()
            ),
            Some(ConfigEvent::GlobalCleared),
        );
    }

    #[test]
    fn namespace_delete_recovers_key_from_old() {
        let r = rel(
            CONFIG_NAMESPACE,
            vec![
                attr(1, "namespace", 25),
                attr(2, "target_database", 25),
                attr(3, "auto_create", 16),
                attr(4, "drop_table_strategy", 25),
            ],
        );
        let old = vec![
            Some(ColumnValue::Text("public".into())),
            Some(ColumnValue::Null),
            Some(ColumnValue::Bool(true)),
            Some(ColumnValue::Null),
        ];
        assert_eq!(
            interpret(
                ConfigTableKind::Namespace,
                &heap(HeapOp::Delete, None, Some(old)),
                &r
            ),
            Some(ConfigEvent::NamespaceRemoved {
                namespace: "public".into()
            }),
        );
    }

    #[test]
    fn table_upsert_builds_structured_key() {
        let r = rel(
            CONFIG_TABLE,
            vec![
                attr(1, "namespace", 25),
                attr(2, "relname", 25),
                attr(3, "target_database", 25),
                attr(4, "target_table", 25),
                attr(5, "replicate", 16),
                attr(6, "initial_load", 25),
            ],
        );
        let new = vec![
            Some(ColumnValue::Text("public".into())),
            Some(ColumnValue::Text("events".into())),
            Some(ColumnValue::Text("default".into())),
            Some(ColumnValue::Text("events".into())),
            Some(ColumnValue::Bool(true)),
            Some(ColumnValue::Text("copy".into())),
        ];
        match interpret(
            ConfigTableKind::Table,
            &heap(HeapOp::Insert, Some(new), None),
            &r,
        )
        .unwrap()
        {
            ConfigEvent::TableUpserted { rel, row } => {
                assert_eq!(rel, RelName::new("public", "events"));
                assert_eq!(row.target_database.as_deref(), Some("default"));
                assert_eq!(row.target_table.as_deref(), Some("events"));
                assert_eq!(row.replicate, Some(true));
                assert_eq!(row.initial_load.as_deref(), Some("copy"));
            }
            other => panic!("expected TableUpserted, got {other:?}"),
        }
    }

    /// A pre-opt-in `config_table` row (only target columns, no `replicate`/
    /// `initial_load`) still interprets, with the new fields `None`.
    #[test]
    fn table_upsert_absent_switches_default_none() {
        let r = rel(
            CONFIG_TABLE,
            vec![
                attr(1, "namespace", 25),
                attr(2, "relname", 25),
                attr(3, "target_table", 25),
            ],
        );
        let new = vec![
            Some(ColumnValue::Text("public".into())),
            Some(ColumnValue::Text("events".into())),
            Some(ColumnValue::Text("events".into())),
        ];
        match interpret(
            ConfigTableKind::Table,
            &heap(HeapOp::Insert, Some(new), None),
            &r,
        )
        .unwrap()
        {
            ConfigEvent::TableUpserted { row, .. } => {
                assert_eq!(row.replicate, None);
                assert_eq!(row.initial_load, None);
            }
            other => panic!("expected TableUpserted, got {other:?}"),
        }
    }

    #[test]
    fn initial_load_parse_accepts_explicit_none() {
        assert_eq!(InitialLoadMode::parse("none"), Some(InitialLoadMode::None));
        assert_eq!(InitialLoadMode::parse("copy"), Some(InitialLoadMode::Copy));
        assert_eq!(
            InitialLoadMode::parse("base_backup"),
            Some(InitialLoadMode::BaseBackup)
        );
        assert_eq!(
            InitialLoadMode::parse("object_store"),
            Some(InitialLoadMode::ObjectStore)
        );
        assert_eq!(InitialLoadMode::parse("null"), None);
    }
}
