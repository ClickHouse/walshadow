//! Source-PG runtime config overlay: the typed in-memory state layer 2 of the
//! [`crate::config::ConfigResolver`] merges (CLI > PG-row > TOML), plus the WAL
//! tuple interpreter that feeds it.
//!
//! Config rows live in operator-owned `<schema>.config_*` tables on source PG
//! (see `sql/runtime_config_install.sql`). The daemon reads them at boot
//! (`SELECT *`, [`crate::config::ResolverBoot::overlay`]) and tracks
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

use crate::decode::heap_decoder::{ColumnValue, DecodedHeap, HeapOp};
use crate::schema::{RelDescriptor, RelName};
use crate::table_rules::MatchKind;
use ahash::HashMap;

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
    /// parses at dispatch in [`crate::backfill::opt_in`], validate-late like every
    /// overlay value); absent / `none` streams from opt-in LSN.
    pub initial_load: Option<String>,
    /// CH `ORDER BY` for this table's `CREATE`. NULL keeps startup TOML value;
    /// empty array uses replica identity
    pub order_by: Option<Vec<String>>,
    /// CH `PRIMARY KEY` sparse-index prefix. Ignored unless prefix of `ORDER BY`
    pub primary_key: Option<Vec<String>>,
    /// Per-relation renames of the columns walshadow appends. NULL on a column
    /// inherits `[system_columns]`; `is_deleted = ''` drops the marker
    pub system: crate::mapping::SystemColumnNames,
    pub match_kind: Option<String>,
}

impl TableRow {
    pub fn is_pattern(&self) -> bool {
        !matches!(
            self.match_kind.as_deref().unwrap_or_default().parse(),
            Ok(MatchKind::Exact)
        )
    }
}

/// Parsed `config_table.initial_load` mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum InitialLoadMode {
    /// No backfill, stream from opt-in LSN only.
    None,
    /// Snapshot-free COPY at `_lsn = S` ([`crate::backfill::copy_backfill`]).
    Copy,
    /// Fresh `BASE_BACKUP` page-walk filtered to the opted-in rels
    /// (architecture/bootstrap.md).
    BaseBackup,
    /// Object-store base backup + archive-WAL gap replay, filtered
    /// (architecture/bootstrap.md).
    ObjectStore,
}

impl std::str::FromStr for InitialLoadMode {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, String> {
        match s {
            "none" => Ok(Self::None),
            "copy" => Ok(Self::Copy),
            "base_backup" => Ok(Self::BaseBackup),
            "object_store" => Ok(Self::ObjectStore),
            other => Err(format!(
                "unknown initial_load mode `{other}` (expected none / copy / \
                 base_backup / object_store)"
            )),
        }
    }
}

impl InitialLoadMode {
    /// Canonical ledger and metric label
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
    pub match_kind: Option<String>,
    /// Raw [`crate::column_rules::Substitute`] per non-finite numeric, NULL inherits
    pub nan: Option<String>,
    pub pos_inf: Option<String>,
    pub neg_inf: Option<String>,
}

/// One applied config change, interpreted from a config-table heap write and
/// carried through [`crate::xact::xact_buffer::DrainEntry::Config`] to apply at the
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
        pattern: bool,
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
            ConfigEvent::TableRemoved { rel, .. } => {
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

fn field_string_array(
    rel: &RelDescriptor,
    cols: &[Option<ColumnValue>],
    name: &str,
) -> Option<Vec<String>> {
    match column(rel, cols, name)? {
        ColumnValue::PgPending { type_oid, raw } if *type_oid == crate::schema::TEXTARRAYOID => {
            crate::decode::codecs::decode_text_array(raw)
        }
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
                let pattern = TableRow {
                    match_kind: field_string(rel, &cols, "match"),
                    ..TableRow::default()
                }
                .is_pattern();
                return Some(ConfigEvent::TableRemoved { rel: key, pattern });
            }
            Some(ConfigEvent::TableUpserted {
                rel: key,
                row: TableRow {
                    target_database: field_string(rel, &cols, "target_database"),
                    target_table: field_string(rel, &cols, "target_table"),
                    replicate: field_bool(rel, &cols, "replicate"),
                    initial_load: field_string(rel, &cols, "initial_load"),
                    order_by: field_string_array(rel, &cols, "order_by"),
                    primary_key: field_string_array(rel, &cols, "primary_key"),
                    system: crate::mapping::SystemColumnNames {
                        lsn: field_string(rel, &cols, "lsn"),
                        xid: field_string(rel, &cols, "xid"),
                        commit_ts: field_string(rel, &cols, "commit_ts"),
                        is_deleted: field_string(rel, &cols, "is_deleted"),
                    },
                    match_kind: field_string(rel, &cols, "match"),
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
                    match_kind: field_string(rel, &cols, "match"),
                    nan: field_string(rel, &cols, "nan"),
                    pos_inf: field_string(rel, &cols, "pos_inf"),
                    neg_inf: field_string(rel, &cols, "neg_inf"),
                },
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::decode::heap_decoder::{ColumnValue, DecodedHeap, DecodedTuple, HeapOp};
    use crate::schema::{RelAttr, RelDescriptor, ReplIdent};
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
            missing_default: None,
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
            toast_oid: 0,
            namespace_oid: 2200,
            rel_name: RelName::new("walshadow", name),
            kind: 'r',
            persistence: 'p',
            replident: ReplIdent::Full { pk_attnums: None },
            attributes: attrs,
        }
    }

    fn text_array(values: &[&str]) -> ColumnValue {
        ColumnValue::PgPending {
            type_oid: crate::schema::TEXTARRAYOID,
            raw: crate::decode::codecs::text_array_body(values),
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
        assert_eq!(
            interpret(ConfigTableKind::Global, &d, &global_rel()),
            Some(ConfigEvent::GlobalUpserted(GlobalRow {
                row_budget: Some(1000),
                byte_budget: None,
                flush_timeout_ms: Some(250),
                compression: Some("zstd".into()),
                retry_max_attempts: Some(9),
                drop_table_strategy: Some("drop".into()),
            })),
        );
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
        // byte_budget changed in new image, rest filled from old
        assert_eq!(
            interpret(
                ConfigTableKind::Global,
                &heap(HeapOp::Update, Some(new), Some(old)),
                &global_rel(),
            ),
            Some(ConfigEvent::GlobalUpserted(GlobalRow {
                row_budget: Some(1000),
                byte_budget: Some(2048),
                flush_timeout_ms: Some(250),
                compression: Some("lz4".into()),
                retry_max_attempts: Some(5),
                drop_table_strategy: Some("retain".into()),
            })),
        );
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
                attr(7, "order_by", crate::schema::TEXTARRAYOID),
                attr(8, "primary_key", crate::schema::TEXTARRAYOID),
            ],
        );
        let new = vec![
            Some(ColumnValue::Text("public".into())),
            Some(ColumnValue::Text("events".into())),
            Some(ColumnValue::Text("default".into())),
            Some(ColumnValue::Text("events".into())),
            Some(ColumnValue::Bool(true)),
            Some(ColumnValue::Text("copy".into())),
            Some(text_array(&["tenant", "id"])),
            Some(text_array(&["tenant"])),
        ];
        assert_eq!(
            interpret(
                ConfigTableKind::Table,
                &heap(HeapOp::Insert, Some(new), None),
                &r,
            ),
            Some(ConfigEvent::TableUpserted {
                rel: RelName::new("public", "events"),
                row: TableRow {
                    target_database: Some("default".into()),
                    target_table: Some("events".into()),
                    replicate: Some(true),
                    initial_load: Some("copy".into()),
                    order_by: Some(vec!["tenant".into(), "id".into()]),
                    primary_key: Some(vec!["tenant".into()]),
                    ..TableRow::default()
                },
            }),
        );
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
        assert_eq!(
            interpret(
                ConfigTableKind::Table,
                &heap(HeapOp::Insert, Some(new), None),
                &r,
            ),
            Some(ConfigEvent::TableUpserted {
                rel: RelName::new("public", "events"),
                row: TableRow {
                    target_table: Some("events".into()),
                    ..TableRow::default()
                },
            }),
        );
    }

    #[test]
    fn table_upsert_reads_match_kind() {
        let r = rel(
            CONFIG_TABLE,
            vec![
                attr(1, "namespace", 25),
                attr(2, "relname", 25),
                attr(3, "match", 25),
                attr(4, "replicate", 16),
                attr(5, "lsn", 25),
                attr(6, "is_deleted", 25),
            ],
        );
        let new = vec![
            Some(ColumnValue::Text("app".into())),
            Some(ColumnValue::Text("events_.*".into())),
            Some(ColumnValue::Text("regex".into())),
            Some(ColumnValue::Bool(true)),
            Some(ColumnValue::Text("_peerdb_version".into())),
            Some(ColumnValue::Text(String::new())),
        ];
        // Absent system columns (xid, commit_ts) inherit
        assert_eq!(
            interpret(
                ConfigTableKind::Table,
                &heap(HeapOp::Insert, Some(new.clone()), None),
                &r,
            ),
            Some(ConfigEvent::TableUpserted {
                rel: RelName::new("app", "events_.*"),
                row: TableRow {
                    replicate: Some(true),
                    system: crate::mapping::SystemColumnNames {
                        lsn: Some("_peerdb_version".into()),
                        is_deleted: Some(String::new()),
                        ..Default::default()
                    },
                    match_kind: Some("regex".into()),
                    ..TableRow::default()
                },
            }),
        );
        assert_eq!(
            interpret(
                ConfigTableKind::Table,
                &heap(HeapOp::Delete, None, Some(new)),
                &r,
            ),
            Some(ConfigEvent::TableRemoved {
                rel: RelName::new("app", "events_.*"),
                pattern: true,
            }),
        );
    }

    #[test]
    fn column_upsert_reads_match_kind() {
        let r = rel(
            CONFIG_COLUMN,
            vec![
                attr(1, "namespace", 25),
                attr(2, "relname", 25),
                attr(3, "attname", 25),
                attr(4, "match", 25),
                attr(5, "target_type", 25),
                attr(6, "nan", 25),
                attr(7, "pos_inf", 25),
                attr(8, "neg_inf", 25),
            ],
        );
        let new = vec![
            Some(ColumnValue::Text("app".into())),
            Some(ColumnValue::Text("*".into())),
            Some(ColumnValue::Text("*_amount".into())),
            Some(ColumnValue::Text("glob".into())),
            Some(ColumnValue::Text("Decimal(38, 9)".into())),
            Some(ColumnValue::Text("0".into())),
            Some(ColumnValue::Text("max".into())),
            Some(ColumnValue::Null),
        ];
        // NULL neg_inf inherits
        assert_eq!(
            interpret(
                ConfigTableKind::Column,
                &heap(HeapOp::Insert, Some(new.clone()), None),
                &r,
            ),
            Some(ConfigEvent::ColumnUpserted {
                rel: RelName::new("app", "*"),
                attname: "*_amount".into(),
                row: ColumnRow {
                    target_type: Some("Decimal(38, 9)".into()),
                    match_kind: Some("glob".into()),
                    nan: Some("0".into()),
                    pos_inf: Some("max".into()),
                    neg_inf: None,
                },
            }),
        );
        assert_eq!(
            interpret(
                ConfigTableKind::Column,
                &heap(HeapOp::Delete, None, Some(new)),
                &r,
            ),
            Some(ConfigEvent::ColumnRemoved {
                rel: RelName::new("app", "*"),
                attname: "*_amount".into(),
            }),
        );
    }

    /// Operator-narrowed integer columns still read
    #[test]
    fn global_smallint_budget_widens() {
        let r = rel(CONFIG_GLOBAL, vec![attr(1, "row_budget", 21)]);
        let d = heap(HeapOp::Insert, Some(vec![Some(ColumnValue::Int2(5))]), None);
        assert_eq!(
            interpret(ConfigTableKind::Global, &d, &r),
            Some(ConfigEvent::GlobalUpserted(GlobalRow {
                row_budget: Some(5),
                ..Default::default()
            })),
        );
    }

    #[test]
    fn truncate_and_unknown_relations_interpret_nothing() {
        assert_eq!(
            interpret(
                ConfigTableKind::Global,
                &heap(HeapOp::Truncate, None, None),
                &global_rel()
            ),
            None
        );
        assert_eq!(
            ConfigTableKind::from_relname(CONFIG_COLUMN),
            Some(ConfigTableKind::Column)
        );
        assert_eq!(ConfigTableKind::from_relname("config_other"), None);
    }

    #[test]
    fn overlay_applies_upserts_and_removals() {
        let rel = RelName::new("public", "t");
        let mut overlay = ConfigOverlay::default();
        for event in [
            ConfigEvent::GlobalUpserted(GlobalRow {
                row_budget: Some(7),
                ..Default::default()
            }),
            ConfigEvent::NamespaceUpserted {
                namespace: "public".into(),
                row: NamespaceRow::default(),
            },
            ConfigEvent::TableUpserted {
                rel: rel.clone(),
                row: TableRow::default(),
            },
            ConfigEvent::ColumnUpserted {
                rel: rel.clone(),
                attname: "c".into(),
                row: ColumnRow::default(),
            },
        ] {
            overlay.apply(event);
        }
        assert_eq!(overlay.global.as_ref().unwrap().row_budget, Some(7));
        assert!(overlay.namespaces.contains_key("public"));
        assert!(overlay.tables.contains_key(&rel));
        assert!(overlay.columns.contains_key(&(rel.clone(), "c".into())));

        for event in [
            ConfigEvent::GlobalCleared,
            ConfigEvent::NamespaceRemoved {
                namespace: "public".into(),
            },
            ConfigEvent::TableRemoved {
                rel: rel.clone(),
                pattern: false,
            },
            ConfigEvent::ColumnRemoved {
                rel: rel.clone(),
                attname: "c".into(),
            },
        ] {
            overlay.apply(event);
        }
        assert!(overlay.global.is_none());
        assert!(overlay.namespaces.is_empty());
        assert!(overlay.tables.is_empty());
        assert!(overlay.columns.is_empty());
    }

    #[test]
    fn initial_load_parse_accepts_explicit_none() {
        assert_eq!("none".parse(), Ok(InitialLoadMode::None));
        assert_eq!("copy".parse(), Ok(InitialLoadMode::Copy));
        assert_eq!("base_backup".parse(), Ok(InitialLoadMode::BaseBackup));
        assert_eq!("object_store".parse(), Ok(InitialLoadMode::ObjectStore));
        assert!("null".parse::<InitialLoadMode>().is_err());
        for mode in [
            InitialLoadMode::None,
            InitialLoadMode::Copy,
            InitialLoadMode::BaseBackup,
            InitialLoadMode::ObjectStore,
        ] {
            assert_eq!(mode.as_str().parse(), Ok(mode));
        }
    }
}
