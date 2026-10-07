//! ClickHouse destination mapping vocabulary

use std::sync::Arc;

use crate::catalog::type_bridge::{self, ResolvedColumn};
use crate::ch::types;
use crate::column_rules::{ColumnRule, ColumnRules};
use crate::runtime_config::InitialLoadMode;
use crate::schema::{RelAttr, RelDescriptor, RelName, SchemaDiff, replident_key_attnums};
use crate::table_rules::set_if;
use ahash::HashMap;
use tokio::sync::RwLock;

#[derive(Debug, Clone, PartialEq)]
pub struct TableMapping {
    pub target: TableTarget,
    pub columns: Vec<ColumnMapping>,
    /// Copy rows and schema changes from `target` to these destinations
    pub tees: Vec<Tee>,
}

impl TableMapping {
    pub fn tee_targets(&self) -> impl Iterator<Item = TableTarget> + '_ {
        self.tees.iter().map(|t| t.target(&self.target.database))
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
    /// `None` shares primary's database
    pub database: Option<String>,
    pub table: String,
    pub engine: Option<String>,
    #[serde(default)]
    pub order_by: Vec<String>,
    #[serde(default)]
    pub primary_key: Vec<String>,
}

impl Tee {
    pub fn target(&self, primary_database: &str) -> TableTarget {
        TableTarget::new(
            self.database.as_deref().unwrap_or(primary_database),
            &self.table,
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
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
    pub auto_create_name: Option<NameTemplate>,
    pub drop_table_strategy: Option<DropTableStrategy>,
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
}

impl Default for SystemColumns {
    fn default() -> Self {
        Self {
            lsn: "_lsn".into(),
            xid: "_xid".into(),
            commit_ts: "_commit_ts".into(),
            is_deleted: Some("_is_deleted".into()),
        }
    }
}

/// `is_deleted = false` (or an empty name) drops the delete marker
fn de_delete_marker<'de, D: serde::Deserializer<'de>>(d: D) -> Result<Option<String>, D::Error> {
    use serde::Deserialize;
    match toml::Value::deserialize(d)? {
        toml::Value::Boolean(false) => Ok(None),
        toml::Value::Boolean(true) => Ok(SystemColumns::default().is_deleted),
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
        let names = self.names();
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
}

impl SystemColumnNames {
    pub fn is_empty(&self) -> bool {
        self.lsn.is_none()
            && self.xid.is_none()
            && self.commit_ts.is_none()
            && self.is_deleted.is_none()
    }

    pub fn overlay(&mut self, other: &Self) {
        set_if(&mut self.lsn, &other.lsn);
        set_if(&mut self.xid, &other.xid);
        set_if(&mut self.commit_ts, &other.commit_ts);
        set_if(&mut self.is_deleted, &other.is_deleted);
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
        Ok(())
    }
}

/// Per-relation delete marker: `false` (or a blank name) drops it for this
/// relation alone, `true` restores the cluster-wide name
pub(crate) fn de_marker_override<'de, D: serde::Deserializer<'de>>(
    d: D,
) -> Result<Option<String>, D::Error> {
    use serde::Deserialize;
    match toml::Value::deserialize(d)? {
        toml::Value::Boolean(false) => Ok(Some(String::new())),
        toml::Value::Boolean(true) => Ok(SystemColumns::default().is_deleted),
        toml::Value::String(name) => Ok(Some(name)),
        v => Err(serde::de::Error::custom(format!(
            "expected a string or false, got {}",
            v.type_str()
        ))),
    }
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
        type_pinned: false,
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

/// Derive changed target type; preserve existing nullability
pub fn retyped_target(
    rel: &RelName,
    column: &ColumnMapping,
    new: &RelAttr,
    key: bool,
    rules: &ColumnRules,
) -> Option<String> {
    let mut ty = map_column(rel, new, key, rules)?.target_type;
    // ClickHouse cannot cast retained NULLs after PostgreSQL SET NOT NULL
    if types::is_nullable(&column.target_type) && !types::is_nullable(&ty) {
        ty = format!("Nullable({ty})");
    }
    (ty != column.target_type).then_some(ty)
}

/// Destination column type change planned from a source type change
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Retype {
    pub src_attnum: i16,
    pub target_name: String,
    pub target_type: String,
    pub kind: RetypeKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetypeKind {
    Modify,
    /// Drop then add, rewrite rows refill values
    Readd,
}

pub fn fold_diff_into_mapping(
    target: &mut TableMapping,
    new: &RelDescriptor,
    diff: &SchemaDiff,
    retypes: &[Retype],
    rules: &ColumnRules,
) {
    for retype in retypes {
        for column in &mut target.columns {
            if column.src_attnum == retype.src_attnum {
                column.target_type.clone_from(&retype.target_type);
            }
        }
    }
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
            missing_default: None,
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
                ..Default::default()
            },
        );
        b.add(
            &RelName::new("app", "*"),
            crate::table_rules::MatchKind::Glob,
            "*_amount",
            crate::table_rules::MatchKind::Glob,
            crate::column_rules::ColumnRule {
                target_type: Some("Decimal(38, 9)".into()),
                ..Default::default()
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
            tees: Vec::new(),
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
            &[],
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
                type_pinned: false,
            }],
            tees: Vec::new(),
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
            &[],
            &rules,
        );
        assert_eq!(mapping.columns[0].target_name, "id");
    }
}
