//! Source-PG sidecar SQL helpers shared across sweep/backfill/config paths.

use tokio_postgres::types::{FromSql, Oid, PgLsn, Type};
use tokio_postgres::{Client, Row};

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
/// `RemoveAttributeById`), so pg_type joins LEFT and supplies typname only —
/// an INNER join loses dropped slots and misaligns attnum-1 indexed decode.
pub const ATTR_SQL: &str = "SELECT \
        a.attnum::int2, \
        a.attname::text, \
        a.atttypid::oid, \
        a.atttypmod::int4, \
        a.attnotnull::bool, \
        a.attisdropped::bool, \
        t.typname::text, \
        a.attbyval::bool, \
        a.attlen::int2, \
        a.attalign::text, \
        a.attstorage::text, \
        CASE WHEN a.atthasmissing THEN a.attmissingval::text END \
     FROM pg_attribute a \
     LEFT JOIN pg_type t ON t.oid = a.atttypid \
     WHERE a.attrelid = $1 AND a.attnum >= 1 \
     ORDER BY a.attnum";

/// One [`ATTR_SQL`] row before char-field validation.
pub struct RawAttr {
    pub attnum: i16,
    pub name: String,
    pub type_oid: Oid,
    pub typmod: i32,
    pub not_null: bool,
    pub dropped: bool,
    /// `None` for dropped slots (atttypid = 0)
    pub type_name: Option<String>,
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
            type_name: row.get(6),
            type_byval: row.get(7),
            type_len: row.get(8),
            type_align: row.get(9),
            type_storage: row.get(10),
            missing: row.get(11),
        }
    }

    pub fn build(self) -> Result<RelAttr, String> {
        Ok(RelAttr {
            attnum: self.attnum,
            name: self.name,
            type_oid: self.type_oid,
            typmod: self.typmod,
            not_null: self.not_null,
            dropped: self.dropped,
            type_name: self.type_name.unwrap_or_default(),
            type_byval: self.type_byval,
            type_len: self.type_len,
            type_align: single_char(&self.type_align, "attalign")?,
            type_storage: single_char(&self.type_storage, "attstorage")?,
            missing_text: self.missing.as_deref().and_then(parse_array_one_element),
        })
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

/// `pg_snapshot_xmax(pg_current_snapshot())`; statement's active snapshot,
/// taken before target-list eval (PG `src/backend/utils/adt/xid8funcs.c`,
/// `src/backend/utils/time/snapmgr.c`).
pub async fn snapshot_xmax(client: &Client) -> anyhow::Result<u64> {
    snapshot_bound(client, "pg_snapshot_xmax").await
}

/// `pg_snapshot_xmin(pg_current_snapshot())`; same snapshot semantics as
/// [`snapshot_xmax`].
pub async fn snapshot_xmin(client: &Client) -> anyhow::Result<u64> {
    snapshot_bound(client, "pg_snapshot_xmin").await
}

/// xid8 wire format: BE u64 (PG `src/backend/utils/adt/xid.c` `xid8send`);
/// epoch-qualified, so compares monotonically, no wraparound.
struct Xid8(u64);

impl FromSql<'_> for Xid8 {
    fn from_sql(_: &Type, raw: &[u8]) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        postgres_protocol::types::int8_from_sql(raw).map(|v| Xid8(v as u64))
    }

    fn accepts(ty: &Type) -> bool {
        *ty == Type::XID8
    }
}

async fn snapshot_bound(client: &Client, func: &str) -> anyhow::Result<u64> {
    let row = client
        .query_one(&format!("SELECT {func}(pg_current_snapshot())"), &[])
        .await?;
    let Xid8(bound) = row.get(0);
    Ok(bound)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn xid8_decodes_be_u64() {
        let raw = 0x0000_0001_0000_002au64.to_be_bytes();
        let Xid8(v) = Xid8::from_sql(&Type::XID8, &raw).unwrap();
        assert_eq!(v, 0x0000_0001_0000_002a);
        assert!(Xid8::accepts(&Type::XID8));
        assert!(!Xid8::accepts(&Type::INT8));
    }

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
            type_name: None,
            type_byval: false,
            type_len: -1,
            type_align: "i".into(),
            type_storage: "x".into(),
            missing: None,
        };
        let attr = raw.build().unwrap();
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
            type_name: Some("int4".into()),
            type_byval: true,
            type_len: 4,
            type_align: "ii".into(),
            type_storage: "p".into(),
            missing: None,
        };
        assert!(raw.build().is_err());
    }
}
