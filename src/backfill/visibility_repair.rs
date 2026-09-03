//! Repair external values and unresolved visibility through source CTID reads
//!
//! Preserve chunk mirrors for WAL rows carrying old external pointers

use anyhow::{Context, Result, bail};
use tokio::sync::mpsc;
use walrus::pg::replication::conn::PgConfig;
use walrus::pg::walparser::Oid;

use crate::backfill::backfill_bootstrap::seed_relation_from_source;
use crate::backfill::backup_page_walk::{BackfillTuple, CatalogMap};
use crate::backfill::copy_backfill::{CopyRate, copy_selected_rows_into};
use crate::mapping::{MappingSnapshot, TableMapping};
use crate::pg::{current_wal_lsn, quote_ident};
use crate::schema::{RelDescriptor, RelName};
use crate::source::source_feed::open_sql_client;
use ahash::HashMap;

/// CTIDs per `COPY`, bounding the `ctid = ANY (ARRAY[...])` literal
const CTID_BATCH: usize = 256;

#[derive(Debug, Default, Clone)]
pub struct RepairScope {
    mapped: HashMap<(Oid, Oid), TableMapping>,
}

impl RepairScope {
    /// Repair reads follow the drain's routes: `walked` narrows them to the
    /// relations the page walk ships, so a read never lands rows the drain drops
    pub fn mapped(
        catalog: &CatalogMap,
        mapping: &MappingSnapshot,
        walked: impl Fn(&RelName) -> bool,
    ) -> Self {
        Self {
            mapped: catalog
                .descriptors()
                .filter(|d| walked(&d.rel_name))
                .filter_map(|d| {
                    mapping
                        .get(&d.rel_name)
                        .map(|m| ((d.rfn.db_node, d.rfn.rel_node), m.clone()))
                })
                .collect(),
        }
    }

    fn get(&self, tuple: &BackfillTuple) -> Option<&TableMapping> {
        self.mapped.get(&(tuple.rfn.db_node, tuple.rfn.rel_node))
    }

    pub fn has_external(&self, tuple: &BackfillTuple) -> bool {
        self.get(tuple)
            .is_some_and(|m| tuple.has_mapped_external(m))
    }
}

pub struct RepairBatch {
    pub rows: Vec<BackfillTuple>,
    /// Backup SLRUs left visibility undecidable, so a source read replaces
    /// every in-scope row rather than only the externally toasted ones
    pub unresolved: bool,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct RepairStats {
    pub rows: u64,
    pub p_hi: u64,
}

pub struct RowRepair {
    pub source: PgConfig,
    pub catalog: CatalogMap,
    pub scope: RepairScope,
    pub rate: CopyRate,
}

impl RowRepair {
    pub async fn run(
        self,
        mut rx: mpsc::Receiver<RepairBatch>,
        tx: mpsc::Sender<Vec<BackfillTuple>>,
    ) -> Result<RepairStats> {
        let mut stats = RepairStats::default();
        let mut connection = None;
        while let Some(RepairBatch { rows, unresolved }) = rx.recv().await {
            let mut pass = Vec::new();
            let mut reads: HashMap<_, Vec<_>> = HashMap::default();
            // Unresolved rows out of scope get no read and have no route, so
            // they reach neither branch
            for t in rows {
                if self
                    .scope
                    .get(&t)
                    .is_some_and(|m| unresolved || t.has_mapped_external(m))
                {
                    reads
                        .entry((t.rfn.db_node, t.rfn.rel_node, t.source_lsn))
                        .or_default()
                        .push((t.blkno, t.offnum));
                } else if !unresolved {
                    pass.push(t);
                }
            }
            if !pass.is_empty() {
                tx.send(pass)
                    .await
                    .context("row repair: drain closed early")?;
            }
            if reads.is_empty() {
                continue;
            }
            if connection.is_none() {
                let client = open_sql_client(&self.source)
                    .await
                    .context("row repair: source sql connect")?;
                client.batch_execute("SET row_security = off").await?;
                connection = Some(client);
            }
            let client = connection.as_ref().unwrap();
            for ((db, rel, lsn), tids) in reads {
                let desc = self
                    .catalog
                    .get(db, rel)
                    .with_context(|| format!("row repair: unknown filenode {db}/{rel}"))?;
                stats.rows += copy_locked(client, &desc, lsn, &tx, &tids, &self.rate).await?;
            }
        }
        if let Some(client) = connection {
            stats.p_hi = current_wal_lsn(&client).await?;
        }
        Ok(stats)
    }
}

/// Hold `ACCESS SHARE` across descriptor validation and every `COPY`, so no
/// rewrite slips between the shape check and the reads it authorises
async fn copy_locked(
    client: &tokio_postgres::Client,
    desc: &RelDescriptor,
    lsn: u64,
    tx: &mpsc::Sender<Vec<BackfillTuple>>,
    tids: &[(u32, u16)],
    rate: &CopyRate,
) -> Result<u64> {
    client.batch_execute("BEGIN").await?;
    let result: Result<u64> = async {
        client
            .batch_execute(&format!(
                "LOCK TABLE ONLY {}.{} IN ACCESS SHARE MODE",
                quote_ident(&desc.rel_name.namespace),
                quote_ident(&desc.rel_name.name),
            ))
            .await
            .with_context(|| format!("row repair: lock {}", desc.rel_name))?;
        let fresh = seed_relation_from_source(client, desc.oid).await?;
        assert_unchanged(&fresh, desc)?;
        let mut rows = 0;
        // PostgreSQL CTIDs identify physical versions, WAL covers moved or deleted rows
        for batch in tids.chunks(CTID_BATCH) {
            rows += copy_selected_rows_into(client, desc, lsn, tx, Some(batch), rate)
                .await
                .with_context(|| format!("row repair: COPY {}", desc.rel_name))?;
        }
        Ok(rows)
    }
    .await;
    // Release relation lock on both success and failure
    let released = client.batch_execute("ROLLBACK").await;
    let rows = result?;
    released?;
    Ok(rows)
}

/// Verify COPY target still matches walked descriptor
///
/// Coarse by design: bootstrap does not support DDL inside the backup
/// window, so any drift ends the pass
fn assert_unchanged(fresh: &CatalogMap, desc: &RelDescriptor) -> Result<()> {
    match fresh.get(desc.rfn.db_node, desc.rfn.rel_node) {
        Some(now) if *now == *desc => Ok(()),
        Some(now) => bail!(
            "visibility repair: relation {} changed inside the backup window \
             ({} to {}); rerun bootstrap against a quiesced source",
            desc.rel_name,
            shape(desc),
            shape(&now),
        ),
        // Filenode is the map key, so a rewrite moved the oid elsewhere
        None => match fresh.descriptors().find(|d| d.oid == desc.oid) {
            Some(moved) => bail!(
                "visibility repair: relation {} was rewritten inside the backup window \
                 (filenode {} to {}); rerun bootstrap against a quiesced source",
                desc.rel_name,
                desc.rfn.rel_node,
                moved.rfn.rel_node,
            ),
            None => bail!(
                "visibility repair: relation {} (oid {}) is gone from the source; \
                 bootstrap does not support DDL inside the backup window",
                desc.rel_name,
                desc.oid,
            ),
        },
    }
}

/// Descriptor shape drift reports name
fn shape(d: &RelDescriptor) -> String {
    let cols: Vec<&str> = d
        .attributes
        .iter()
        .filter(|a| !a.dropped)
        .map(|a| a.name.as_str())
        .collect();
    format!(
        "{}/{}/{} [{}]",
        d.rel_name,
        d.kind,
        d.replident.to_char(),
        cols.join(","),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backfill::backup_page_walk::make_rel_named;
    use crate::mapping::TableTarget;

    #[tokio::test]
    async fn out_of_scope_rows_never_connect() {
        let mut catalog = CatalogMap::new();
        let d = make_rel_named(16400, 16400, 0, RelName::new("public", "t"));
        catalog.insert(d.clone());
        let mapping = [(
            d.rel_name.clone(),
            TableMapping {
                target: TableTarget::new("default", "t"),
                columns: Vec::new(),
            },
        )]
        .into_iter()
        .collect::<HashMap<_, _>>()
        .into();
        let scope = RepairScope::mapped(&catalog, &mapping, |_| false);
        let (tx, rx) = mpsc::channel(1);
        tx.send(RepairBatch {
            rows: vec![BackfillTuple {
                rfn: d.rfn,
                xid: 0,
                xmax: 0,
                infomask: 0,
                source_lsn: 0x1000,
                blkno: 0,
                offnum: 1,
                columns: Vec::new(),
            }],
            unresolved: true,
        })
        .await
        .unwrap();
        drop(tx);
        let (out_tx, mut out_rx) = mpsc::channel(1);
        let stats = RowRepair {
            source: crate::config::SourceConn::default().to_pg_config(),
            catalog,
            scope,
            rate: CopyRate::new(None),
        }
        .run(rx, out_tx)
        .await
        .unwrap();
        assert_eq!(stats, RepairStats::default());
        assert!(out_rx.recv().await.is_none());
    }
}
