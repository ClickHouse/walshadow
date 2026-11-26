//! Source-PG sidecar SQL helpers shared across sweep/backfill/config paths.

use std::error::Error;

use tokio_postgres::types::{FromSql, Oid, PgLsn, Type, accepts};
use tokio_postgres::{Client, Row};

use crate::schema::{MissingDefault, RelAttr};

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
        a.attalign, \
        a.attstorage, \
        CASE WHEN a.atthasmissing THEN a.attmissingval::text END, \
        CASE WHEN a.atthasmissing AND t.typsend::oid <> 0 THEN anyarray_send(a.attmissingval) END \
     FROM pg_attribute a \
     LEFT JOIN pg_type t ON t.oid = a.atttypid \
     WHERE a.attrelid = $1 AND a.attnum >= 1 \
     ORDER BY a.attnum";

/// One [`ATTR_SQL`] row before missing-default decoding
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
    pub type_align: char,
    pub type_storage: char,
    /// `attmissingval::text` array literal
    pub missing: Option<String>,
    /// `anyarray_send(attmissingval)`, preferred over `missing` where the
    /// type's send form converts to on-disk bytes
    pub missing_send: Option<Vec<u8>>,
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
            type_align: char_at(row, 9),
            type_storage: char_at(row, 10),
            missing: row.get(11),
            missing_send: row.get(12),
        }
    }

    pub fn build(self) -> RelAttr {
        let missing_default = self
            .missing_send
            .as_deref()
            .and_then(|send| crate::backfill::copy_backfill::missing_from_send(self.type_oid, send))
            .map(MissingDefault::Raw)
            .or_else(|| {
                self.missing
                    .as_deref()
                    .and_then(parse_array_one_element)
                    .map(MissingDefault::Text)
            });
        RelAttr {
            attnum: self.attnum,
            name: self.name,
            type_oid: self.type_oid,
            typmod: self.typmod,
            not_null: self.not_null,
            dropped: self.dropped,
            type_name: self.type_name.unwrap_or_default(),
            type_byval: self.type_byval,
            type_len: self.type_len,
            type_align: self.type_align,
            type_storage: self.type_storage,
            missing_default,
        }
    }
}

pub fn socket_conninfo(socket_dir: &str, port: u16, user: &str, dbname: &str) -> String {
    format!("host={socket_dir} port={port} user={user} dbname={dbname}")
}

/// PG identifier: double-quoted, embedded quotes doubled.
pub fn quote_ident(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}

type SqlError = Box<dyn Error + Sync + Send>;

/// `xid8`, epoch-qualified transaction id
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Xid8(pub u64);

impl<'a> FromSql<'a> for Xid8 {
    fn from_sql(_: &Type, raw: &'a [u8]) -> Result<Self, SqlError> {
        Ok(Self(u64::from_be_bytes(raw.try_into()?)))
    }

    accepts!(XID8);
}

/// `tid`, heap tuple slot as (block, offset)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Tid(pub u32, pub u16);

impl<'a> FromSql<'a> for Tid {
    fn from_sql(_: &Type, raw: &'a [u8]) -> Result<Self, SqlError> {
        let raw: [u8; 6] = raw.try_into()?;
        Ok(Self(
            u32::from_be_bytes([raw[0], raw[1], raw[2], raw[3]]),
            u16::from_be_bytes([raw[4], raw[5]]),
        ))
    }

    accepts!(TID);
}

/// `"char"` column at `idx`
pub fn char_at(row: &Row, idx: usize) -> char {
    char::from(row.get::<_, i8>(idx) as u8)
}

/// Next xid to assign on the connected server
pub async fn next_xid(client: &Client) -> Result<Xid8, tokio_postgres::Error> {
    let row = client
        .query_one("SELECT pg_snapshot_xmax(pg_current_snapshot())", &[])
        .await?;
    Ok(row.get(0))
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
            type_name: None,
            type_byval: false,
            type_len: -1,
            type_align: 'i',
            type_storage: 'x',
            missing: None,
            missing_send: None,
        };
        let attr = raw.build();
        assert!(attr.dropped);
        assert_eq!(attr.type_name, "");
        assert_eq!(attr.type_len, -1);
        assert_eq!(attr.type_align, 'i');
        assert_eq!(attr.type_storage, 'x');
    }

    #[test]
    fn xid8_reads_big_endian() {
        let raw = [0, 0, 0, 7, 0, 0, 0, 9];
        assert_eq!(
            Xid8::from_sql(&Type::XID8, &raw).unwrap(),
            Xid8((7 << 32) | 9)
        );
        assert!(Xid8::from_sql(&Type::XID8, &raw[1..]).is_err());
        assert!(<Xid8 as FromSql>::accepts(&Type::XID8));
        assert!(!<Xid8 as FromSql>::accepts(&Type::INT8));
    }

    #[test]
    fn tid_reads_block_then_offset() {
        let raw = [0, 0, 1, 2, 0, 3];
        assert_eq!(Tid::from_sql(&Type::TID, &raw).unwrap(), Tid(258, 3));
        assert!(Tid::from_sql(&Type::TID, &raw[1..]).is_err());
        assert!(<Tid as FromSql>::accepts(&Type::TID));
    }
}
