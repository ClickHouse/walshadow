//! Postgres relation and schema-change vocabulary

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use tokio::sync::mpsc;
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

pub type SchemaEventRx = Arc<Mutex<mpsc::UnboundedReceiver<SchemaEvent>>>;
