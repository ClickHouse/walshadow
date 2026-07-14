//! Source-PG sidecar SQL helpers shared across sweep/backfill/config paths.

use tokio_postgres::Client;
use tokio_postgres::types::{FromSql, PgLsn, Type};

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
}
