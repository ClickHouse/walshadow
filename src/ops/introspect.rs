//! Source-PG catalog reads shared by the control socket and `init`
//!
//! Plain SQL over an ordinary (non-replication) connection: what a
//! table picker needs to show, and what pre-flight needs to judge a
//! relation replicable

use tokio_postgres::Client;

use crate::schema::RelName;

/// `pg_class` row as the picker sees it
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceTable {
    pub rel: RelName,
    /// `pg_class.relreplident`: `d` default, `n` nothing, `f` full, `i` index
    pub replica_identity: char,
    pub has_pk: bool,
}

impl SourceTable {
    /// DELETE needs a row key, and CH needs an `ORDER BY` to collapse on
    pub fn has_row_key(&self) -> bool {
        match self.replica_identity {
            'd' => self.has_pk,
            'n' => false,
            _ => true,
        }
    }

    /// Why the picker refuses it, or how it identifies rows
    pub fn row_key_note(&self) -> &'static str {
        match self.replica_identity {
            'd' if self.has_pk => "primary key",
            'd' => "no primary key — add one, or SET REPLICA IDENTITY FULL",
            'n' => "REPLICA IDENTITY NOTHING — deletes cannot be replicated",
            'f' => "REPLICA IDENTITY FULL",
            'i' => "REPLICA IDENTITY USING INDEX",
            _ => "unknown replica identity",
        }
    }
}

/// User relations, ordered by (namespace, name). `namespace` filters to one
/// schema; `None` lists every non-system schema
pub async fn tables(
    client: &Client,
    namespace: Option<&str>,
) -> Result<Vec<SourceTable>, tokio_postgres::Error> {
    const BASE: &str = "SELECT n.nspname, c.relname, c.relreplident::text, \
         EXISTS (SELECT 1 FROM pg_index i WHERE i.indrelid = c.oid AND i.indisprimary) \
         FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace \
         WHERE c.relkind = 'r' AND n.nspname NOT IN ('pg_catalog','information_schema') \
           AND n.nspname NOT LIKE 'pg\\_%'";
    let rows = match namespace {
        Some(ns) => {
            client
                .query(&format!("{BASE} AND n.nspname = $1 ORDER BY 1,2"), &[&ns])
                .await?
        }
        None => client.query(&format!("{BASE} ORDER BY 1,2"), &[]).await?,
    };
    Ok(rows
        .iter()
        .map(|r| {
            let ident: String = r.get(2);
            SourceTable {
                rel: RelName::new(r.get(0), r.get(1)),
                replica_identity: ident.chars().next().unwrap_or('?'),
                has_pk: r.get(3),
            }
        })
        .collect())
}

/// Non-system schema names
pub async fn schemas(client: &Client) -> Result<Vec<String>, tokio_postgres::Error> {
    let rows = client
        .query(
            "SELECT nspname FROM pg_namespace \
             WHERE nspname NOT IN ('pg_catalog','information_schema') \
               AND nspname NOT LIKE 'pg\\_%' ORDER BY 1",
            &[],
        )
        .await?;
    Ok(rows.iter().map(|r| r.get(0)).collect())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceColumn {
    pub name: String,
    pub pg_type: String,
    pub notnull: bool,
}

/// Live columns of one relation in `attnum` order
pub async fn columns(
    client: &Client,
    rel: &RelName,
) -> Result<Vec<SourceColumn>, tokio_postgres::Error> {
    let (ns, name): (&str, &str) = (&rel.namespace, &rel.name);
    let rows = client
        .query(
            "SELECT a.attname, format_type(a.atttypid, a.atttypmod), a.attnotnull \
             FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid \
             JOIN pg_namespace n ON n.oid = c.relnamespace \
             WHERE n.nspname = $1 AND c.relname = $2 AND a.attnum > 0 \
               AND NOT a.attisdropped ORDER BY a.attnum",
            &[&ns, &name],
        )
        .await?;
    Ok(rows
        .iter()
        .map(|r| SourceColumn {
            name: r.get(0),
            pg_type: r.get(1),
            notnull: r.get(2),
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn table(ident: char, has_pk: bool) -> SourceTable {
        SourceTable {
            rel: RelName::new("public", "t"),
            replica_identity: ident,
            has_pk,
        }
    }

    #[test]
    fn default_identity_needs_a_primary_key() {
        assert!(table('d', true).has_row_key());
        assert!(!table('d', false).has_row_key());
    }

    #[test]
    fn full_and_index_identity_carry_their_own_key() {
        assert!(table('f', false).has_row_key());
        assert!(table('i', false).has_row_key());
    }

    #[test]
    fn nothing_identity_never_has_a_key() {
        assert!(!table('n', true).has_row_key());
    }
}
