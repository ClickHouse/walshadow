//! Source-PG sidecar SQL helpers shared across sweep/backfill/config paths.

use ahash::HashMap;
use tokio_postgres::types::{Oid, PgLsn};
use tokio_postgres::{Client, Row};

use crate::ops::bridge::TypeRow;
use crate::schema::RelAttr;

pub use walrus::pg::backup::parse_pg_lsn;

pub fn parse_array_one_element(raw: &str) -> Option<String> {
    let inner = raw.strip_prefix('{')?.strip_suffix('}')?;
    if inner.is_empty() || inner == "NULL" {
        return None;
    }
    let Some(rest) = inner.strip_prefix('"') else {
        return Some(inner.to_owned());
    };
    let mut output = String::with_capacity(rest.len());
    let mut chars = rest.chars();
    loop {
        match chars.next()? {
            '"' => return chars.next().is_none().then_some(output),
            '\\' => output.push(chars.next()?),
            character => output.push(character),
        }
    }
}

/// Attribute rows shared by catalog fetchers. Physical layout comes straight
/// off pg_attribute: DROP COLUMN zeroes atttypid but preserves
/// attlen/attalign/attbyval/attstorage (PG `src/backend/catalog/heap.c`
/// `RemoveAttributeById`). Type names come from [`TYPE_SQL`] rows at
/// [`RawAttr::build`]
pub const ATTR_SQL: &str = "SELECT \
        attnum::int2, \
        attname::text, \
        atttypid::oid, \
        atttypmod::int4, \
        attnotnull::bool, \
        attisdropped::bool, \
        attbyval::bool, \
        attlen::int2, \
        attalign::text, \
        attstorage::text, \
        CASE WHEN atthasmissing THEN attmissingval::text END \
     FROM pg_attribute \
     WHERE attrelid = $1 AND attnum >= 1 \
     ORDER BY attnum";

pub const TYPE_SQL: &str =
    "SELECT oid, typname::text, typbasetype, typtypmod, typelem, typarray FROM pg_type";

/// Key [`TYPE_SQL`] shaped rows by oid
pub fn type_rows(rows: &[Row]) -> HashMap<Oid, TypeRow> {
    rows.iter()
        .map(|r| {
            let row = TypeRow {
                oid: r.get(0),
                typname: r.get(1),
                typbasetype: r.get(2),
                typtypmod: r.get(3),
                typelem: r.get(4),
                typarray: r.get(5),
            };
            (row.oid, row)
        })
        .collect()
}

/// One [`ATTR_SQL`] row before type resolution and char-field validation.
pub struct RawAttr {
    pub attnum: i16,
    pub name: String,
    /// atttypid, 0 for dropped slots
    pub type_oid: Oid,
    pub typmod: i32,
    pub not_null: bool,
    pub dropped: bool,
    pub type_byval: bool,
    pub type_len: i16,
    pub type_align: String,
    pub type_storage: String,
    /// `attmissingval::text` array literal
    pub missing: Option<String>,
}

impl RawAttr {
    pub fn from_row(row: &Row) -> Self {
        Self {
            attnum: row.get(0),
            name: row.get(1),
            type_oid: row.get(2),
            typmod: row.get(3),
            not_null: row.get(4),
            dropped: row.get(5),
            type_byval: row.get(6),
            type_len: row.get(7),
            type_align: row.get(8),
            type_storage: row.get(9),
            missing: row.get(10),
        }
    }

    /// `types` must hold atttypid, its domain bases, and for arrays the
    /// element's domain bases and their array types
    pub fn build(mut self, types: &HashMap<Oid, TypeRow>) -> Result<RelAttr, String> {
        let mut type_name = String::new();
        if self.type_oid != 0 {
            let (mut typ, mut typmod) = base_type(types, self.type_oid, self.typmod)?;
            // Array of domain becomes array of base, unless base is itself an
            // array and so has no array type
            if let Some(elem) = types.get(&typ.typelem)
                && elem.typbasetype != 0
            {
                let (elem_base, elem_typmod) = base_type(types, elem.oid, typmod)?;
                if let Some(array) = types.get(&elem_base.typarray) {
                    typ = array;
                    typmod = elem_typmod;
                }
            }
            self.type_oid = typ.oid;
            self.typmod = typmod;
            type_name.clone_from(&typ.typname);
        }
        Ok(RelAttr {
            attnum: self.attnum,
            name: self.name,
            type_oid: self.type_oid,
            typmod: self.typmod,
            not_null: self.not_null,
            dropped: self.dropped,
            type_name,
            type_byval: self.type_byval,
            type_len: self.type_len,
            type_align: single_char(&self.type_align, "attalign")?,
            type_storage: single_char(&self.type_storage, "attstorage")?,
            missing_default: self
                .missing
                .as_deref()
                .and_then(parse_array_one_element)
                .map(crate::schema::MissingDefault::Text),
        })
    }
}

/// Domain to base type and typmod, as PG `getBaseTypeAndTypmod`
fn base_type(
    types: &HashMap<Oid, TypeRow>,
    mut oid: Oid,
    mut typmod: i32,
) -> Result<(&TypeRow, i32), String> {
    loop {
        let typ = types
            .get(&oid)
            .ok_or_else(|| format!("no pg_type row for type {oid}"))?;
        if typ.typbasetype == 0 {
            return Ok((typ, typmod));
        }
        oid = typ.typbasetype;
        typmod = typ.typtypmod;
    }
}

fn single_char(s: &str, what: &str) -> Result<char, String> {
    let mut it = s.chars();
    match (it.next(), it.next()) {
        (Some(c), None) => Ok(c),
        _ => Err(format!("expected single char for {what}, got {s:?}")),
    }
}

pub fn socket_conninfo(socket_dir: &str, port: u16, user: &str, dbname: &str) -> String {
    format!("host={socket_dir} port={port} user={user} dbname={dbname}")
}

/// PG identifier: double-quoted, embedded quotes doubled.
pub fn quote_ident(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}

/// `pg_current_wal_lsn()` of the connected server.
pub async fn current_wal_lsn(client: &Client) -> anyhow::Result<u64> {
    let row = client.query_one("SELECT pg_current_wal_lsn()", &[]).await?;
    let lsn: PgLsn = row.get(0);
    Ok(lsn.into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quote_ident_doubles_embedded_quotes() {
        assert_eq!(quote_ident("plain"), "\"plain\"");
        assert_eq!(quote_ident("we\"ird"), "\"we\"\"ird\"");
    }

    #[test]
    fn parse_array_one_element_scalars() {
        assert_eq!(parse_array_one_element("{7}").as_deref(), Some("7"));
        assert_eq!(parse_array_one_element("{t}").as_deref(), Some("t"));
        assert_eq!(parse_array_one_element("{3.14}").as_deref(), Some("3.14"),);
        assert_eq!(
            parse_array_one_element("{-9223372036854775808}").as_deref(),
            Some("-9223372036854775808"),
        );
    }

    #[test]
    fn parse_array_one_element_quoted_text() {
        assert_eq!(
            parse_array_one_element("{\"hello\"}").as_deref(),
            Some("hello"),
        );
        assert_eq!(
            parse_array_one_element("{\"hello, world\"}").as_deref(),
            Some("hello, world"),
        );
        assert_eq!(
            parse_array_one_element("{\"a\\\"b\"}").as_deref(),
            Some("a\"b"),
        );
    }

    #[test]
    fn parse_array_one_element_empty_and_null() {
        assert!(parse_array_one_element("{}").is_none());
        assert!(parse_array_one_element("{NULL}").is_none());
        assert!(parse_array_one_element("nope").is_none());
    }

    #[test]
    fn raw_attr_build_dropped_slot() {
        let raw = RawAttr {
            attnum: 2,
            name: "........pg.dropped.2........".into(),
            type_oid: 0,
            typmod: -1,
            not_null: false,
            dropped: true,
            type_byval: false,
            type_len: -1,
            type_align: "i".into(),
            type_storage: "x".into(),
            missing: None,
        };
        let attr = raw.build(&HashMap::default()).unwrap();
        assert!(attr.dropped);
        assert_eq!(attr.type_name, "");
        assert_eq!(attr.type_len, -1);
        assert_eq!(attr.type_align, 'i');
        assert_eq!(attr.type_storage, 'x');
    }

    #[test]
    fn raw_attr_build_rejects_multichar() {
        let raw = RawAttr {
            attnum: 1,
            name: "id".into(),
            type_oid: 23,
            typmod: -1,
            not_null: true,
            dropped: false,
            type_byval: true,
            type_len: 4,
            type_align: "ii".into(),
            type_storage: "p".into(),
            missing: None,
        };
        assert!(raw.build(&domain_types()).is_err());
    }

    /// (oid, typname, typbasetype, typtypmod, typelem, typarray)
    fn types(rows: &[(Oid, &str, Oid, i32, Oid, Oid)]) -> HashMap<Oid, TypeRow> {
        rows.iter()
            .map(
                |&(oid, typname, typbasetype, typtypmod, typelem, typarray)| {
                    let row = TypeRow {
                        oid,
                        typname: typname.into(),
                        typbasetype,
                        typtypmod,
                        typelem,
                        typarray,
                    };
                    (oid, row)
                },
            )
            .collect()
    }

    const NUMERIC_10_2: i32 = (10 << 16) + 2 + 4;

    /// `cents` over `price` over numeric(10,2), `numbers` over int4[], and
    /// `cents_array` over cents[]
    fn domain_types() -> HashMap<Oid, TypeRow> {
        types(&[
            (23, "int4", 0, -1, 0, 1007),
            (1007, "_int4", 0, -1, 23, 0),
            (1700, "numeric", 0, -1, 0, 1231),
            (1231, "_numeric", 0, -1, 1700, 0),
            (16400, "price", 1700, NUMERIC_10_2, 0, 16401),
            (16401, "_price", 0, -1, 16400, 0),
            (16402, "cents", 16400, -1, 0, 16403),
            (16403, "_cents", 0, -1, 16402, 0),
            (16404, "numbers", 1007, -1, 0, 16405),
            (16405, "_numbers", 0, -1, 16404, 0),
            (16406, "cents_array", 16403, -1, 0, 16407),
        ])
    }

    fn resolve(type_oid: Oid) -> (Oid, i32, String) {
        let raw = RawAttr {
            attnum: 1,
            name: "c".into(),
            type_oid,
            typmod: -1,
            not_null: false,
            dropped: false,
            type_byval: false,
            type_len: -1,
            type_align: "i".into(),
            type_storage: "x".into(),
            missing: None,
        };
        let attr = raw.build(&domain_types()).unwrap();
        (attr.type_oid, attr.typmod, attr.type_name)
    }

    #[test]
    fn raw_attr_build_resolves_domains() {
        let numeric = (1700, NUMERIC_10_2, "numeric".into());
        let numeric_array = (1231, NUMERIC_10_2, "_numeric".into());
        assert_eq!(resolve(16402), numeric);
        assert_eq!(resolve(16403), numeric_array, "array of domain");
        assert_eq!(resolve(16406), numeric_array, "domain over array of domain");
        assert_eq!(resolve(16404), (1007, -1, "_int4".into()));
        assert_eq!(
            resolve(16405),
            (16405, -1, "_numbers".into()),
            "array of domain over array has no base array type"
        );
    }
}
