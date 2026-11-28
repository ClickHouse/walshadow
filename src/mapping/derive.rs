//! Derive destination columns from source descriptors and column rules

use super::{ColumnMapping, TableMapping};
use crate::catalog::type_bridge::{self, ResolvedColumn};
use crate::ch::types;
use crate::column_rules::{ColumnRule, ColumnRules};
use crate::schema::{RelAttr, RelDescriptor, RelName, SchemaDiff, replident_key_attnums};

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
) -> ColumnMapping {
    let resolved = type_bridge::map(attr, pk_member);
    let (target_name, resolved) =
        apply_column_rule(&attr.name, resolved, rules.settings(rel, &attr.name));
    ColumnMapping {
        src_attnum: attr.attnum,
        target_name,
        target_type: resolved.ch_type,
        type_pinned: false,
    }
}

pub fn derive_columns_for_mapping(desc: &RelDescriptor, rules: &ColumnRules) -> Vec<ColumnMapping> {
    let keys = replident_key_attnums(desc);
    desc.attributes
        .iter()
        .filter(|attr| !attr.dropped)
        .map(|attr| map_column(&desc.rel_name, attr, keys.contains(&attr.attnum), rules))
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
    let mut ty = map_column(rel, new, key, rules).target_type;
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
        target
            .columns
            .push(map_column(&new.rel_name, attr, key, rules));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mapping::TableTarget;

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
            type_align: b'i',
            type_storage: b'p',
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
            kind: b'r',
            persistence: b'p',
            ancestors: Vec::new(),
            replident: crate::schema::ReplIdent::Default { pk_attnums: None },
            attributes: attrs,
        }
    }

    fn amount_rules() -> ColumnRules {
        let mut b = crate::column_rules::ColumnRulesBuilder::new();
        b.add(
            &RelName::new("app", "events"),
            crate::runtime_config::MatchKind::Exact,
            "legacy_id",
            crate::runtime_config::MatchKind::Exact,
            crate::column_rules::ColumnRule {
                target_name: Some("id".into()),
                ..Default::default()
            },
        );
        b.add(
            &RelName::new("app", "*"),
            crate::runtime_config::MatchKind::Glob,
            "*_amount",
            crate::runtime_config::MatchKind::Glob,
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

    #[test]
    fn predeclared_added_column_keeps_operator_mapping() {
        let added = attr(1, "legacy_id", crate::schema::INT4OID);
        let pinned = ColumnMapping {
            src_attnum: 1,
            target_name: "pinned".into(),
            target_type: "Int64".into(),
            type_pinned: true,
        };
        let mut mapping = TableMapping {
            target: TableTarget::new("db", "events"),
            columns: vec![pinned.clone()],
            tees: Vec::new(),
        };
        let diff = SchemaDiff {
            added_columns: vec![added.clone()],
            dropped_columns: vec![],
            renamed_columns: vec![],
            type_changes: vec![],
        };
        let new = events_desc(vec![added]);
        fold_diff_into_mapping(&mut mapping, &new, &diff, &[], &amount_rules());
        assert_eq!(mapping.columns, vec![pinned]);
        assert_eq!(mapping.target.to_string(), "db.events");
    }
}
