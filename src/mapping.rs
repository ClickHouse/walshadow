//! ClickHouse destination mapping vocabulary

use std::sync::{Arc, PoisonError, RwLock, RwLockReadGuard, RwLockWriteGuard};

use crate::ch::quote_ident;
use crate::schema::RelName;
use foldhash::HashMap;

pub mod derive;

#[derive(Debug, Clone, PartialEq)]
pub struct TableMapping {
    pub target: TableTarget,
    pub columns: Vec<ColumnMapping>,
    /// Copy rows and schema changes from `target` to these destinations
    pub tees: Vec<Tee>,
}

impl TableMapping {
    pub fn tee_targets(&self) -> impl Iterator<Item = TableTarget> + '_ {
        self.tees.iter().map(|t| t.target(&self.target))
    }

    /// Return main destination followed by each tee
    pub fn targets(&self) -> impl Iterator<Item = TableTarget> + '_ {
        std::iter::once(self.target.clone()).chain(self.tee_targets())
    }
}

/// Copy rows into an extra table, such as a MergeTree audit log.
/// Share columns with main destination; apply engine and keys only
/// when creating table
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Tee {
    pub instance: Option<String>,
    /// When unset, use main destination database or explicit `instance` default
    pub database: Option<String>,
    pub table: String,
    pub engine: Option<String>,
    #[serde(default)]
    pub order_by: Vec<String>,
    #[serde(default)]
    pub primary_key: Vec<String>,
}

impl Tee {
    pub fn target(&self, primary: &TableTarget) -> TableTarget {
        TableTarget {
            instance: self
                .instance
                .clone()
                .map_or_else(|| primary.instance.clone(), named_instance),
            database: self
                .database
                .clone()
                .unwrap_or_else(|| primary.database.clone()),
            table: self.table.clone(),
        }
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

/// Overwrite only where the narrower layer states a value
pub(crate) fn set_if<T: Clone>(dst: &mut Option<T>, src: &Option<T>) {
    if src.is_some() {
        dst.clone_from(src);
    }
}

/// Map explicit `default` to None so TableTarget spells default instance one way
pub fn named_instance(name: String) -> Option<String> {
    (name != "default").then_some(name)
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct TableTarget {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instance: Option<String>,
    pub database: String,
    pub table: String,
}

impl TableTarget {
    pub fn new(database: &str, table: &str) -> Self {
        Self {
            instance: None,
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
    pub auto_create_name: Option<NameTemplate>,
    pub drop_table_strategy: Option<DropTableStrategy>,
    /// What `DETACH PARTITION` does to rows the partition routed to its root
    pub detach_strategy: Option<DropTableStrategy>,
    pub initial_load: Option<InitialLoadMode>,
}

/// Destination name for a table no `target_table` names, as `$database$` /
/// `$schema$` / `$table$` placeholders around literal text (`$$` is a literal
/// `$`). A template naming no `$table$` is read as a prefix, so `wh_` means
/// `wh_$table$` and `""` means `$table$`
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NameTemplate(Vec<NamePart>);

#[derive(Debug, Clone, PartialEq, Eq)]
enum NamePart {
    Literal(String),
    Database,
    Schema,
    Table,
}

impl Default for NameTemplate {
    fn default() -> Self {
        Self(vec![
            NamePart::Schema,
            NamePart::Literal("_".into()),
            NamePart::Table,
        ])
    }
}

impl NameTemplate {
    pub fn parse(src: &str) -> Result<Self, String> {
        let mut parts = Vec::new();
        let mut literal = String::new();
        let mut rest = src;
        while let Some(at) = rest.find('$') {
            literal.push_str(&rest[..at]);
            rest = &rest[at + 1..];
            if let Some(tail) = rest.strip_prefix('$') {
                literal.push('$');
                rest = tail;
                continue;
            }
            let Some(end) = rest.find('$') else {
                return Err(format!(
                    "`{src}`: unterminated `$`; write `$$` for a literal `$`"
                ));
            };
            let part = match &rest[..end] {
                "database" => NamePart::Database,
                "schema" => NamePart::Schema,
                "table" => NamePart::Table,
                unknown => {
                    return Err(format!(
                        "`{src}`: unknown placeholder `${unknown}$`; expected `$database$`, `$schema$` or `$table$`"
                    ));
                }
            };
            if !literal.is_empty() {
                parts.push(NamePart::Literal(std::mem::take(&mut literal)));
            }
            parts.push(part);
            rest = &rest[end + 1..];
        }
        literal.push_str(rest);
        if !literal.is_empty() {
            parts.push(NamePart::Literal(literal));
        }
        if !parts.contains(&NamePart::Table) {
            parts.push(NamePart::Table);
        }
        Ok(Self(parts))
    }

    pub fn render(&self, database: &str, schema: &str, table: &str) -> String {
        let mut out = String::new();
        for part in &self.0 {
            match part {
                NamePart::Literal(s) => out.push_str(s),
                NamePart::Database => out.push_str(database),
                NamePart::Schema => out.push_str(schema),
                NamePart::Table => out.push_str(table),
            }
        }
        out
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum DropTableStrategy {
    #[default]
    Retain,
    Drop,
    Warn,
}

impl std::str::FromStr for DropTableStrategy {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, String> {
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

#[derive(Debug, Clone, PartialEq)]
pub struct ColumnMapping {
    pub src_attnum: i16,
    pub target_name: String,
    pub target_type: String,
    /// Operator stated type by attnum; source type changes leave it alone
    pub type_pinned: bool,
}

/// Names of the columns walshadow appends to every replicated CH table, and
/// whether the delete marker exists at all. `[system_columns]` sets the
/// cluster-wide default; a `[table.*]` block or `config_table` row renames per
/// relation ([`SystemColumnNames`]). A rename does not ALTER tables already
/// created, and every INSERT column list rebuilds from these, so an operator
/// renaming after first sync must ALTER the destination themselves. TOAST
/// mirror tables are walshadow-internal and keep their own fixed names.
#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize)]
#[serde(default)]
pub struct SystemColumns {
    pub lsn: String,
    pub xid: String,
    pub commit_ts: String,
    /// `None` leaves the delete marker out of both DDL and INSERT. DELETE rows
    /// then have nowhere to land, so they are discarded (counted in
    /// `emitter_deletes_discarded`) — for append-only destinations
    #[serde(deserialize_with = "de_delete_marker")]
    pub is_deleted: Option<String>,
    /// Source partition OID, appended only on partitioned-root destinations.
    /// `None` leaves it out, which rules out partition-scoped row removal
    #[serde(deserialize_with = "de_leaf")]
    pub leaf: Option<String>,
}

/// Row-shape knobs, cloned into every frozen route so execution never reads
/// live config. `soft_delete` is boot-only; the system columns are the
/// cluster-wide `[system_columns]` set until [`Self::for_rel`] layers a
/// per-relation rename over it
#[derive(Debug, Clone, Default)]
pub struct RowPolicy {
    /// CH-side delete retention policy (the delete marker stays queryable)
    pub soft_delete: bool,
    /// Names + presence of the columns walshadow appends per row
    pub system: Arc<SystemColumns>,
}

impl Default for SystemColumns {
    fn default() -> Self {
        Self {
            lsn: "_lsn".into(),
            xid: "_xid".into(),
            commit_ts: "_commit_ts".into(),
            is_deleted: Some("_is_deleted".into()),
            leaf: Some("_leaf_oid".into()),
        }
    }
}

/// `is_deleted = false` (or an empty name) drops the delete marker
fn de_delete_marker<'de, D: serde::Deserializer<'de>>(d: D) -> Result<Option<String>, D::Error> {
    de_optional_column(d, SystemColumns::default().is_deleted)
}

/// `leaf = false` (or an empty name) drops the partition column
fn de_leaf<'de, D: serde::Deserializer<'de>>(d: D) -> Result<Option<String>, D::Error> {
    de_optional_column(d, SystemColumns::default().leaf)
}

fn de_optional_column<'de, D: serde::Deserializer<'de>>(
    d: D,
    default: Option<String>,
) -> Result<Option<String>, D::Error> {
    use serde::Deserialize;
    match toml::Value::deserialize(d)? {
        toml::Value::Boolean(false) => Ok(None),
        toml::Value::Boolean(true) => Ok(default),
        toml::Value::String(name) => Ok((!name.is_empty()).then_some(name)),
        v => Err(serde::de::Error::custom(format!(
            "expected a string or false, got {}",
            v.type_str()
        ))),
    }
}

impl SystemColumns {
    /// A blank rename or a name colliding with another system column yields a
    /// CH table walshadow cannot INSERT into, so both are config errors
    pub fn validate(&self) -> Result<(), String> {
        for (key, name) in [
            ("lsn", &self.lsn),
            ("xid", &self.xid),
            ("commit_ts", &self.commit_ts),
        ] {
            if name.is_empty() {
                return Err(format!("system_columns.{key}: name must not be empty"));
            }
        }
        let mut names = self.names();
        names.extend(self.leaf.as_deref());
        for (i, a) in names.iter().enumerate() {
            if names[i + 1..].contains(a) {
                return Err(format!("system_columns: `{a}` named twice"));
            }
        }
        Ok(())
    }

    /// Every system column present, in the order the emitter appends them
    pub fn names(&self) -> Vec<&str> {
        let mut v = vec![
            self.lsn.as_str(),
            self.xid.as_str(),
            self.commit_ts.as_str(),
        ];
        v.extend(self.is_deleted.as_deref());
        v
    }

    /// Apply per-relation renames. Total rather than fallible: a rename onto a
    /// name another system column holds is skipped, so no merge of layers can
    /// render a CH table with two identically named columns. Entries are
    /// name-checked where they are parsed, so a skip here means two layers
    /// collided
    pub fn renamed(&self, names: &SystemColumnNames) -> Self {
        let mut out = self.clone();
        for (i, over) in [&names.lsn, &names.xid, &names.commit_ts]
            .into_iter()
            .enumerate()
        {
            let Some(name) = over.as_deref().filter(|n| !n.is_empty()) else {
                continue;
            };
            if out
                .names()
                .iter()
                .enumerate()
                .any(|(j, n)| j != i && *n == name)
            {
                continue;
            }
            let field = match i {
                0 => &mut out.lsn,
                1 => &mut out.xid,
                _ => &mut out.commit_ts,
            };
            *field = name.into();
        }
        match names.is_deleted.as_deref() {
            Some("") => out.is_deleted = None,
            Some(name) if !out.names()[..3].contains(&name) => out.is_deleted = Some(name.into()),
            _ => {}
        }
        if out
            .leaf
            .as_deref()
            .is_some_and(|l| out.names().contains(&l))
        {
            out.leaf = None;
        }
        match names.leaf.as_deref() {
            Some("") => out.leaf = None,
            Some(name) if !out.names().contains(&name) => out.leaf = Some(name.into()),
            _ => {}
        }
        out
    }
}

/// Per-relation renames layered over [`SystemColumns`]. `None` inherits the
/// cluster-wide name. An empty string inherits too — a blank `text` overlay
/// column must not blank a column name — except on `is_deleted`, where it drops
/// the marker (and with it every DELETE row), like `[system_columns]
/// is_deleted = false`
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SystemColumnNames {
    pub lsn: Option<String>,
    pub xid: Option<String>,
    pub commit_ts: Option<String>,
    pub is_deleted: Option<String>,
    /// Blank drops the partition column, as for `is_deleted`
    pub leaf: Option<String>,
}

impl SystemColumnNames {
    pub fn is_empty(&self) -> bool {
        self.lsn.is_none()
            && self.xid.is_none()
            && self.commit_ts.is_none()
            && self.is_deleted.is_none()
            && self.leaf.is_none()
    }

    pub fn overlay(&mut self, other: &Self) {
        set_if(&mut self.lsn, &other.lsn);
        set_if(&mut self.xid, &other.xid);
        set_if(&mut self.commit_ts, &other.commit_ts);
        set_if(&mut self.is_deleted, &other.is_deleted);
        set_if(&mut self.leaf, &other.leaf);
    }

    /// Reject a set naming one column twice, or renaming onto a name another
    /// system column holds by default: both yield a CH table walshadow cannot
    /// INSERT into. `ctx` names the config entry
    pub fn validate(&self, ctx: &str) -> Result<(), String> {
        let renamed = SystemColumns::default().renamed(self);
        let landed = renamed.names();
        let wanted = [
            self.lsn.as_deref(),
            self.xid.as_deref(),
            self.commit_ts.as_deref(),
            self.is_deleted.as_deref(),
        ];
        for (i, want) in wanted.into_iter().enumerate() {
            let Some(want) = want.filter(|n| !n.is_empty()) else {
                continue;
            };
            if landed.get(i) != Some(&want) {
                return Err(format!("{ctx}: `{want}` names two system columns"));
            }
        }
        if let Some(want) = self.leaf.as_deref().filter(|n| !n.is_empty())
            && renamed.leaf.as_deref() != Some(want)
        {
            return Err(format!("{ctx}: `{want}` names two system columns"));
        }
        Ok(())
    }
}

/// Per-relation delete marker: `false` (or a blank name) drops it for this
/// relation alone, `true` restores the cluster-wide name
pub(crate) fn de_marker_override<'de, D: serde::Deserializer<'de>>(
    d: D,
) -> Result<Option<String>, D::Error> {
    de_column_override(d, SystemColumns::default().is_deleted)
}

/// Per-relation partition column, read like [`de_marker_override`]
pub(crate) fn de_leaf_override<'de, D: serde::Deserializer<'de>>(
    d: D,
) -> Result<Option<String>, D::Error> {
    de_column_override(d, SystemColumns::default().leaf)
}

fn de_column_override<'de, D: serde::Deserializer<'de>>(
    d: D,
    default: Option<String>,
) -> Result<Option<String>, D::Error> {
    use serde::Deserialize;
    match toml::Value::deserialize(d)? {
        toml::Value::Boolean(false) => Ok(Some(String::new())),
        toml::Value::Boolean(true) => Ok(default),
        toml::Value::String(name) => Ok(Some(name)),
        v => Err(serde::de::Error::custom(format!(
            "expected a string or false, got {}",
            v.type_str()
        ))),
    }
}

/// Immutable routing-map version. Planners snapshot one per transaction so a
/// concurrent republish can't split a transaction across mapping versions
pub type MappingSnapshot = Arc<HashMap<RelName, Arc<TableMapping>>>;

pub type MappingHandle = Arc<MappingCell>;

/// Copy-on-write routing map. Writers `Arc::make_mut` or swap the inner
/// snapshot, so a snapshot taken for one unit of work stays frozen
#[derive(Debug)]
pub struct MappingCell {
    inner: RwLock<MappingSnapshot>,
}

impl MappingCell {
    pub fn with<R>(&self, f: impl FnOnce(&MappingSnapshot) -> R) -> R {
        f(&self.read())
    }

    pub fn snapshot(&self) -> MappingSnapshot {
        self.read().clone()
    }

    /// `false` once a fold or republish displaced `at`. Holding `at` keeps
    /// its allocation alive and forces `Arc::make_mut` to clone, so pointer
    /// identity answers "has the map moved since I looked?"
    pub fn unmoved(&self, at: &MappingSnapshot) -> bool {
        Arc::ptr_eq(&self.read(), at)
    }

    pub fn mutate<R>(&self, f: impl FnOnce(&mut MappingSnapshot) -> R) -> R {
        f(&mut self.write())
    }

    pub fn publish(&self, tables: MappingSnapshot) {
        *self.write() = tables;
    }

    fn read(&self) -> RwLockReadGuard<'_, MappingSnapshot> {
        self.inner.read().unwrap_or_else(PoisonError::into_inner)
    }

    fn write(&self) -> RwLockWriteGuard<'_, MappingSnapshot> {
        self.inner.write().unwrap_or_else(PoisonError::into_inner)
    }
}

pub fn mapping_handle(tables: HashMap<RelName, TableMapping>) -> MappingHandle {
    Arc::new(MappingCell {
        inner: RwLock::new(Arc::new(shared(tables))),
    })
}

/// Wrap each mapping once at publish so routing clones a refcount rather than
/// the column vector on every commit
pub fn shared(tables: HashMap<RelName, TableMapping>) -> HashMap<RelName, Arc<TableMapping>> {
    tables.into_iter().map(|(k, v)| (k, Arc::new(v))).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn name_template_renders_placeholders_literals_and_escapes() {
        let render = |t: &str| {
            NameTemplate::parse(t)
                .expect(t)
                .render("app", "audit", "orders")
        };
        assert_eq!(
            NameTemplate::default().render("app", "audit", "orders"),
            "audit_orders"
        );
        assert_eq!(render("$schema$_$table$"), "audit_orders");
        assert_eq!(render("$database$_$schema$_$table$"), "app_audit_orders");
        assert_eq!(render("$table$_suffix"), "orders_suffix");
        assert_eq!(render("$table$"), "orders");
        assert_eq!(render("$table$$table$"), "ordersorders");
        assert_eq!(render("$$"), "$orders", "`$$` is a literal `$`");
        assert_eq!(render("a$$b_$table$"), "a$b_orders");
        assert_eq!(render("$$table$$"), "$table$orders");
    }

    #[test]
    fn name_template_without_table_reads_as_a_prefix() {
        let render = |t: &str| {
            NameTemplate::parse(t)
                .expect(t)
                .render("app", "audit", "orders")
        };
        assert_eq!(render(""), "orders");
        assert_eq!(render("wh_"), "wh_orders");
        assert_eq!(render("$schema$_"), "audit_orders");
        assert_eq!(render("$database$_"), "app_orders");
    }

    #[test]
    fn name_template_rejects_unknown_and_unterminated_placeholders() {
        let err = NameTemplate::parse("$relname$_$table$").expect_err("unknown");
        assert!(err.contains("`$relname$`"), "{err}");
        assert!(err.contains("$database$"), "names the alternatives: {err}");
        let err = NameTemplate::parse("$table$_$half").expect_err("unterminated");
        assert!(err.contains("unterminated"), "{err}");
        assert!(err.contains("`$$`"), "points at the escape: {err}");
    }

    /// Parse a `[system_columns]` body the way `ConfigDocument` does
    fn system_columns(body: &str) -> Result<SystemColumns, String> {
        let sys: SystemColumns = toml::from_str(body).map_err(crate::toml_de::message)?;
        sys.validate()?;
        Ok(sys)
    }

    #[test]
    fn system_columns_default_when_section_absent() {
        let sys = system_columns("").unwrap();
        assert_eq!(sys, SystemColumns::default());
        assert_eq!(sys.names(), ["_lsn", "_xid", "_commit_ts", "_is_deleted"]);
    }

    #[test]
    fn system_columns_rename_and_disable_marker() {
        let sys = system_columns("lsn = \"_peerdb_version\"\ncommit_ts = \"_peerdb_synced_at\"\n")
            .unwrap();
        assert_eq!(sys.lsn, "_peerdb_version");
        assert_eq!(sys.commit_ts, "_peerdb_synced_at");
        assert_eq!(sys.xid, "_xid");
        assert_eq!(sys.is_deleted.as_deref(), Some("_is_deleted"));

        let off = system_columns("is_deleted = false\n").unwrap();
        assert!(off.is_deleted.is_none());
        assert_eq!(off.names(), ["_lsn", "_xid", "_commit_ts"]);
        let renamed = system_columns("is_deleted = \"gone\"\n").unwrap();
        assert_eq!(renamed.is_deleted.as_deref(), Some("gone"));
        let on = system_columns("is_deleted = true\n").unwrap();
        assert_eq!(on.is_deleted.as_deref(), Some("_is_deleted"));
    }

    #[test]
    fn system_columns_reject_empty_and_colliding_names() {
        // Both yield a CH table walshadow cannot INSERT into
        assert!(system_columns("lsn = \"\"\n").is_err());
        assert!(system_columns("xid = \"_lsn\"\n").is_err());
        assert!(system_columns("lsn = 7\n").is_err());
        let err = system_columns("is_deleted = 1\n").unwrap_err();
        assert!(err.contains("expected a string or false"), "{err}");
    }

    #[test]
    fn partition_column_renames_disables_and_refuses_collisions() {
        assert_eq!(
            system_columns("").unwrap().leaf.as_deref(),
            Some("_leaf_oid")
        );
        assert!(system_columns("leaf = false\n").unwrap().leaf.is_none());
        assert_eq!(
            system_columns("leaf = \"_part\"\n")
                .unwrap()
                .leaf
                .as_deref(),
            Some("_part")
        );
        assert!(system_columns("leaf = \"_lsn\"\n").is_err());

        let renamed = SystemColumns::default().renamed(&SystemColumnNames {
            leaf: Some("_p".into()),
            ..SystemColumnNames::default()
        });
        assert_eq!(renamed.leaf.as_deref(), Some("_p"));
        let dropped = SystemColumns::default().renamed(&SystemColumnNames {
            leaf: Some(String::new()),
            ..SystemColumnNames::default()
        });
        assert!(dropped.leaf.is_none());
        let onto_lsn = SystemColumnNames {
            leaf: Some("_lsn".into()),
            ..SystemColumnNames::default()
        };
        assert!(onto_lsn.validate("table.public.t").is_err());
        assert_eq!(
            SystemColumns::default().renamed(&onto_lsn).leaf.as_deref(),
            Some("_leaf_oid"),
            "a colliding rename is skipped"
        );
        let lsn_takes_leaf = SystemColumns::default().renamed(&SystemColumnNames {
            lsn: Some("_leaf_oid".into()),
            ..SystemColumnNames::default()
        });
        assert!(
            lsn_takes_leaf.leaf.is_none(),
            "never two columns of one name"
        );
    }

    #[test]
    fn per_relation_rename_inherits_unnamed_columns() {
        let names = SystemColumnNames {
            lsn: Some("_peerdb_version".into()),
            is_deleted: Some(String::new()),
            ..SystemColumnNames::default()
        };
        names.validate("table.public.t").unwrap();
        let sys = SystemColumns::default().renamed(&names);
        assert_eq!(sys.lsn, "_peerdb_version");
        assert_eq!(sys.xid, "_xid", "unnamed column inherits");
        assert!(sys.is_deleted.is_none(), "blank marker drops it");

        let sys = SystemColumns::default().renamed(&SystemColumnNames {
            xid: Some("_txid".into()),
            is_deleted: Some("_gone".into()),
            ..SystemColumnNames::default()
        });
        assert_eq!(sys.names(), ["_lsn", "_txid", "_commit_ts", "_gone"]);
    }

    #[test]
    fn per_relation_marker_override_parses_bool_or_name() {
        #[derive(serde::Deserialize)]
        struct Patch {
            #[serde(default, deserialize_with = "de_marker_override")]
            is_deleted: Option<String>,
        }
        let parse = |body: &str| {
            toml::from_str::<Patch>(body)
                .map(|p| p.is_deleted)
                .map_err(crate::toml_de::message)
        };
        assert_eq!(parse("").unwrap(), None);
        assert_eq!(parse("is_deleted = false").unwrap().as_deref(), Some(""));
        assert_eq!(
            parse("is_deleted = true").unwrap().as_deref(),
            Some("_is_deleted")
        );
        assert_eq!(parse("is_deleted = \"x\"").unwrap().as_deref(), Some("x"));
        let err = parse("is_deleted = 1").unwrap_err();
        assert!(err.contains("expected a string or false"), "{err}");
    }

    #[test]
    fn per_relation_blank_name_inherits() {
        let sys = SystemColumns::default().renamed(&SystemColumnNames {
            lsn: Some(String::new()),
            ..SystemColumnNames::default()
        });
        assert_eq!(
            sys.lsn, "_lsn",
            "a blank overlay column must not blank a name"
        );
    }

    #[test]
    fn per_relation_rename_onto_another_system_column_rejected() {
        let onto = SystemColumnNames {
            lsn: Some("_xid".into()),
            ..SystemColumnNames::default()
        };
        assert!(onto.validate("t").is_err());
        assert!(
            SystemColumnNames {
                lsn: Some("_v".into()),
                xid: Some("_v".into()),
                ..SystemColumnNames::default()
            }
            .validate("t")
            .is_err()
        );
        // Layers can still collide after their own checks pass; the merge
        // keeps the name it holds rather than rendering two columns as one
        let sys = SystemColumns::default().renamed(&onto);
        assert_eq!(sys.names().len(), 4);
        assert_eq!(sys.lsn, "_lsn");
    }

    fn one_table() -> (RelName, HashMap<RelName, TableMapping>) {
        let rel = RelName::new("public", "t");
        let map = HashMap::from_iter([(
            rel.clone(),
            TableMapping {
                target: TableTarget::new("db", "t"),
                columns: vec![],
                tees: Vec::new(),
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
        let planned: MappingSnapshot = handle.snapshot();
        handle.mutate(|m| Arc::make_mut(m).remove(&rel));
        assert!(planned.contains_key(&rel), "snapshot keeps its version");
        assert!(!handle.with(|m| m.contains_key(&rel)), "handle moved on");
        // Republish shape: full inner-Arc swap
        let planned = handle.snapshot();
        let (rel2, map2) = one_table();
        handle.publish(Arc::new(shared(map2)));
        assert!(!planned.contains_key(&rel2), "snapshot predates the swap");
        assert!(handle.with(|m| m.contains_key(&rel2)));
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
