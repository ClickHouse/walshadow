//! Postgres relation and schema-change vocabulary

use std::collections::HashMap;
use std::sync::Arc;

use tokio_postgres::types::Oid;
use walrus::pg::walparser::RelFileNode;

/// `FirstNormalObjectId`, PG src/include/access/transam.h
pub const FIRST_NORMAL_OBJECT_ID: u32 = 16384;

pub const BOOLOID: u32 = 16;
pub const BYTEAOID: u32 = 17;
pub const CHAROID: u32 = 18;
pub const NAMEOID: u32 = 19;
pub const INT8OID: u32 = 20;
pub const INT2OID: u32 = 21;
pub const INT4OID: u32 = 23;
pub const TEXTOID: u32 = 25;
pub const OIDOID: u32 = 26;
pub const JSONOID: u32 = 114;
pub const CIDROID: u32 = 650;
pub const FLOAT4OID: u32 = 700;
pub const FLOAT8OID: u32 = 701;
pub const INETOID: u32 = 869;
pub const BPCHAROID: u32 = 1042;
pub const VARCHAROID: u32 = 1043;
pub const DATEOID: u32 = 1082;
pub const TIMEOID: u32 = 1083;
pub const TIMESTAMPOID: u32 = 1114;
pub const TIMESTAMPTZOID: u32 = 1184;
pub const INTERVALOID: u32 = 1186;
pub const TIMETZOID: u32 = 1266;
pub const NUMERICOID: u32 = 1700;
pub const UUIDOID: u32 = 2950;
pub const JSONBOID: u32 = 3802;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RelName {
    pub namespace: Arc<str>,
    pub name: Arc<str>,
}

impl RelName {
    pub fn new(namespace: &str, name: &str) -> Self {
        Self {
            namespace: Arc::from(namespace),
            name: Arc::from(name),
        }
    }
}

impl std::fmt::Display for RelName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}", self.namespace, self.name)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct RelDescriptor {
    pub rfn: RelFileNode,
    pub oid: Oid,
    /// `pg_class.reltoastrelid`, 0 = no TOAST table
    pub toast_oid: Oid,
    pub namespace_oid: Oid,
    pub rel_name: RelName,
    pub kind: char,
    pub persistence: char,
    pub replident: ReplIdent,
    pub attributes: Vec<RelAttr>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ReplIdent {
    Default {
        pk_attnums: Option<Vec<i16>>,
    },
    Nothing,
    Full {
        pk_attnums: Option<Vec<i16>>,
    },
    UsingIndex {
        index_oid: Oid,
        key_attnums: Vec<i16>,
    },
}

/// Resolve stored primary/index keys, including primary key metadata under `Full`
pub fn replident_key_attnums(desc: &RelDescriptor) -> &[i16] {
    match &desc.replident {
        ReplIdent::Default {
            pk_attnums: Some(keys),
        }
        | ReplIdent::Full {
            pk_attnums: Some(keys),
        }
        | ReplIdent::UsingIndex {
            key_attnums: keys, ..
        } => keys,
        _ => &[],
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct RelAttr {
    pub attnum: i16,
    pub name: String,
    pub type_oid: Oid,
    pub typmod: i32,
    pub not_null: bool,
    pub dropped: bool,
    pub type_name: String,
    pub type_byval: bool,
    pub type_len: i16,
    pub type_align: char,
    pub type_storage: char,
    pub missing_text: Option<String>,
}

#[derive(Debug, Clone)]
pub enum SchemaEvent {
    Added {
        desc: Arc<RelDescriptor>,
    },
    Changed {
        old: Arc<RelDescriptor>,
        new: Arc<RelDescriptor>,
        diff: SchemaDiff,
    },
    Dropped {
        oid: Oid,
        rel_name: RelName,
    },
}

#[derive(Debug, Default, Clone, PartialEq)]
pub struct SchemaDiff {
    pub added_columns: Vec<RelAttr>,
    pub dropped_columns: Vec<i16>,
    pub renamed_columns: Vec<(i16, String, String)>,
    pub type_changes: Vec<(i16, RelAttr)>,
}

impl SchemaDiff {
    pub fn is_empty(&self) -> bool {
        self.added_columns.is_empty()
            && self.dropped_columns.is_empty()
            && self.renamed_columns.is_empty()
            && self.type_changes.is_empty()
    }
}

pub fn compute_schema_diff(old: &RelDescriptor, new: &RelDescriptor) -> SchemaDiff {
    let mut diff = SchemaDiff::default();
    let mut old_by_num: HashMap<i16, &RelAttr> = old
        .attributes
        .iter()
        .filter(|a| !a.dropped)
        .map(|a| (a.attnum, a))
        .collect();
    for new_attr in new.attributes.iter().filter(|a| !a.dropped) {
        match old_by_num.remove(&new_attr.attnum) {
            None => diff.added_columns.push(new_attr.clone()),
            Some(old_attr) => {
                if old_attr.name != new_attr.name {
                    diff.renamed_columns.push((
                        new_attr.attnum,
                        old_attr.name.clone(),
                        new_attr.name.clone(),
                    ));
                }
                if old_attr.type_oid != new_attr.type_oid
                    || old_attr.typmod != new_attr.typmod
                    || old_attr.not_null != new_attr.not_null
                {
                    diff.type_changes.push((new_attr.attnum, new_attr.clone()));
                }
            }
        }
    }
    diff.dropped_columns = old_by_num.into_keys().collect();
    diff.dropped_columns.sort_unstable();
    diff
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio_postgres::types::Oid;

    fn mk_attr(attnum: i16, name: &str, oid: Oid, not_null: bool) -> RelAttr {
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
            missing_text: None,
        }
    }

    fn mk_desc(oid: Oid, attrs: Vec<RelAttr>) -> RelDescriptor {
        RelDescriptor {
            rfn: RelFileNode {
                spc_node: 1663,
                db_node: 5,
                rel_node: oid,
            },
            oid,
            toast_oid: 0,
            namespace_oid: 2200,
            rel_name: RelName::new("public", &format!("t{oid}")),
            kind: 'r',
            persistence: 'p',
            replident: ReplIdent::Default { pk_attnums: None },
            attributes: attrs,
        }
    }

    #[test]
    fn schema_diff_detects_added_columns() {
        let old = mk_desc(16400, vec![mk_attr(1, "id", 23, true)]);
        let new = mk_desc(
            16400,
            vec![mk_attr(1, "id", 23, true), mk_attr(2, "name", 25, false)],
        );
        let d = compute_schema_diff(&old, &new);
        assert_eq!(d.added_columns.len(), 1);
        assert_eq!(d.added_columns[0].attnum, 2);
        assert!(d.dropped_columns.is_empty());
        assert!(d.renamed_columns.is_empty());
        assert!(d.type_changes.is_empty());
    }

    #[test]
    fn schema_diff_detects_dropped_columns() {
        let old = mk_desc(
            16400,
            vec![mk_attr(1, "id", 23, true), mk_attr(2, "name", 25, false)],
        );
        let new = mk_desc(16400, vec![mk_attr(1, "id", 23, true)]);
        let d = compute_schema_diff(&old, &new);
        assert_eq!(d.dropped_columns, vec![2]);
        assert!(d.added_columns.is_empty());
    }

    #[test]
    fn schema_diff_detects_rename_at_same_attnum() {
        let old = mk_desc(
            16400,
            vec![
                mk_attr(1, "id", 23, true),
                mk_attr(2, "old_name", 25, false),
            ],
        );
        let new = mk_desc(
            16400,
            vec![
                mk_attr(1, "id", 23, true),
                mk_attr(2, "new_name", 25, false),
            ],
        );
        let d = compute_schema_diff(&old, &new);
        assert_eq!(
            d.renamed_columns,
            vec![(2, "old_name".into(), "new_name".into())]
        );
        assert!(d.added_columns.is_empty());
        assert!(d.dropped_columns.is_empty());
        assert!(d.type_changes.is_empty());
    }

    #[test]
    fn schema_diff_detects_type_change_at_same_attnum() {
        let old = mk_desc(16400, vec![mk_attr(1, "c", 23, true)]); // int4
        let new = mk_desc(16400, vec![mk_attr(1, "c", 20, true)]); // int8
        let d = compute_schema_diff(&old, &new);
        assert_eq!(d.type_changes.len(), 1);
        assert_eq!(d.type_changes[0].0, 1);
        assert_eq!(d.type_changes[0].1.type_oid, 20);
    }

    #[test]
    fn schema_diff_skips_pg_dropped_columns_in_old() {
        // PG retains DROP COLUMN as attisdropped=true in pg_attribute; diff must
        // ignore them, not re-surface as still-present on the new side
        let mut a = mk_attr(2, "x", 25, false);
        a.dropped = true;
        let old = mk_desc(16400, vec![mk_attr(1, "id", 23, true), a]);
        let new = mk_desc(16400, vec![mk_attr(1, "id", 23, true)]);
        let d = compute_schema_diff(&old, &new);
        assert!(d.dropped_columns.is_empty());
        assert!(d.added_columns.is_empty());
    }

    #[test]
    fn schema_diff_is_empty_when_shapes_match() {
        let a = mk_desc(
            16400,
            vec![mk_attr(1, "id", 23, true), mk_attr(2, "name", 25, false)],
        );
        let b = a.clone();
        let d = compute_schema_diff(&a, &b);
        assert!(d.is_empty());
    }
}
