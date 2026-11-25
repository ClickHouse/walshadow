//! Which files shadow must hold for `[toast] mode = "shadow"`
//!
//! Shadow keeps TOAST heaps and their indexes the way it keeps catalogs:
//! landed in place by the backup, then replayed. Index filenodes are absent
//! from the catalog seed, so the source answers for them.

use anyhow::{Context, Result};
use foldhash::{HashMap, HashSet};
use tokio::sync::Mutex;
use walrus::pg::walparser::RelFileNode;

use crate::catalog::shadow_catalog::ShadowCatalog;
use crate::filter::shadow_relations::ShadowHeld;
use crate::schema::{RelDescriptor, RelName};

/// Find TOAST heaps shadow cannot replay, paired with their parent relations
/// Use one catalog read for entire batch
///
/// TOAST heaps must be seeded during bootstrap or admitted at `CREATE`
/// A running standby cannot be seeded with older heaps added later
async fn unheld_toasts<'a>(
    catalog: &Mutex<ShadowCatalog>,
    held: &ShadowHeld,
    descs: &'a [RelDescriptor],
) -> Result<Vec<(&'a RelDescriptor, RelDescriptor)>> {
    let toasted: Vec<&RelDescriptor> = descs.iter().filter(|d| d.toast_oid != 0).collect();
    if toasted.is_empty() {
        return Ok(Vec::new());
    }
    // `toast_oid` is the `reltoastrelid` a descriptor lookup would re-read
    let oids: Vec<u32> = toasted.iter().map(|d| d.toast_oid).collect();
    let (_, fetched) = catalog.lock().await.fetch_descriptors_batch(&oids).await?;
    let mut by_oid: HashMap<u32, RelDescriptor> = fetched.into_iter().map(|t| (t.oid, t)).collect();
    Ok(toasted
        .into_iter()
        .filter_map(|desc| {
            let toast = by_oid.remove(&desc.toast_oid)?;
            (!held.contains(toast.rfn)).then_some((desc, toast))
        })
        .collect())
}

/// TOAST heap shadow cannot replay for `desc`, if any. Batch form is
/// [`unserved_rels`]
pub async fn unheld_toast(
    catalog: &Mutex<ShadowCatalog>,
    held: &ShadowHeld,
    desc: &RelDescriptor,
) -> Result<Option<RelDescriptor>> {
    Ok(unheld_toasts(catalog, held, std::slice::from_ref(desc))
        .await?
        .pop()
        .map(|(_, toast)| toast))
}

/// Check whether shadow holds external values for `desc`, warning if absent
/// Reject missing TOAST heaps to avoid replacing their values with NULL
pub async fn serves_toast(
    catalog: &Mutex<ShadowCatalog>,
    held: &ShadowHeld,
    desc: &RelDescriptor,
) -> Result<bool> {
    let Some(toast) = unheld_toast(catalog, held, desc).await? else {
        return Ok(true);
    };
    warn_unserved(desc, &toast);
    Ok(false)
}

/// Return relations rejected by [`serves_toast`], warning once per relation
pub async fn unserved_rels(
    catalog: &Mutex<ShadowCatalog>,
    held: &ShadowHeld,
    descs: &[RelDescriptor],
) -> Result<Vec<RelName>> {
    Ok(unheld_toasts(catalog, held, descs)
        .await?
        .into_iter()
        .map(|(desc, toast)| {
            warn_unserved(desc, &toast);
            desc.rel_name.clone()
        })
        .collect())
}

fn warn_unserved(desc: &RelDescriptor, toast: &RelDescriptor) {
    tracing::warn!(
        target: "walshadow::config",
        qname = %desc.rel_name,
        toast = %toast.rel_name,
        filenode = toast.rfn.rel_node,
        "[toast] mode = shadow cannot replicate this table: TOAST heap is missing \
         and a running shadow cannot be seeded after bootstrap. \
         Re-bootstrap with table included, or use [toast] mode = \"clickhouse\"",
    );
}

/// Opted-in TOAST heaps
fn toast_heaps<'a>(
    descriptors: impl Iterator<Item = &'a RelDescriptor>,
    tap_filenodes: Option<&HashSet<RelFileNode>>,
) -> Vec<&'a RelDescriptor> {
    descriptors
        .filter(|d| d.kind == 't')
        .filter(|d| tap_filenodes.is_none_or(|set| set.contains(&d.rfn)))
        .collect()
}

/// Filenodes to land and replay: every opted-in TOAST heap plus its indexes
pub async fn toast_relations<'a>(
    sql: &tokio_postgres::Client,
    descriptors: impl Iterator<Item = &'a RelDescriptor>,
    tap_filenodes: Option<&HashSet<RelFileNode>>,
) -> Result<HashSet<RelFileNode>> {
    let heaps: HashSet<RelFileNode> = toast_heaps(descriptors, tap_filenodes)
        .into_iter()
        .map(|d| d.rfn)
        .collect();
    let mut out = heaps.clone();
    // Reuse authenticated sidecar client from catalog seed
    // Interpolate because client has no `oid[]` Rust mapping
    let (rels, spcs): (Vec<_>, Vec<_>) = heaps
        .iter()
        .map(|r| (r.rel_node.to_string(), r.spc_node.to_string()))
        .unzip();
    let rows = sql
        .query(
            &format!(
                "SELECT ic.relfilenode::int8, \
                 coalesce(nullif(ic.reltablespace, 0), d.dattablespace)::int8, \
                 d.oid::int8 \
                 FROM unnest('{{{rels}}}'::oid[], '{{{spcs}}}'::oid[]) h(rel, spc) \
                 JOIN pg_database d ON d.datname = current_database() \
                 JOIN pg_class t ON t.relfilenode = h.rel \
                 AND coalesce(nullif(t.reltablespace, 0), d.dattablespace) = h.spc \
                 JOIN pg_index i ON i.indrelid = t.oid \
                 JOIN pg_class ic ON ic.oid = i.indexrelid \
                 WHERE t.relkind = 't'",
                rels = rels.join(","),
                spcs = spcs.join(","),
            ),
            &[],
        )
        .await
        .context("bootstrap: read toast index filenodes")?;
    let oid = |row: &tokio_postgres::Row, i| row.get::<_, i64>(i) as u32;
    out.extend(rows.iter().map(|row| RelFileNode {
        rel_node: oid(row, 0),
        spc_node: oid(row, 1),
        db_node: oid(row, 2),
    }));
    tracing::info!(
        target: "walshadow::bootstrap",
        toast_heaps = heaps.len(),
        toast_indexes = rows.len(),
        "landing TOAST storage in shadow",
    );
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{DEFAULT_TABLESPACE_OID, RelName, ReplIdent};

    fn desc(kind: char, rel_node: u32, spc_node: u32) -> RelDescriptor {
        RelDescriptor {
            rfn: RelFileNode {
                spc_node,
                db_node: 5,
                rel_node,
            },
            oid: rel_node,
            toast_oid: 0,
            namespace_oid: 99,
            rel_name: RelName::new("pg_toast", &format!("pg_toast_{rel_node}")),
            kind,
            persistence: 'p',
            ancestors: Vec::new(),
            replident: ReplIdent::Default { pk_attnums: None },
            attributes: Vec::new(),
        }
    }

    #[test]
    fn heaps_take_opted_in_toast_from_any_tablespace() {
        let all = [
            desc('t', 16400, DEFAULT_TABLESPACE_OID),
            desc('t', 16401, 16385),
            desc('r', 16402, DEFAULT_TABLESPACE_OID),
        ];
        let picked = toast_heaps(all.iter(), None);
        assert_eq!(picked.len(), 2, "user heaps are not TOAST storage");

        let tap = [all[1].rfn].into_iter().collect();
        let picked = toast_heaps(all.iter(), Some(&tap));
        assert_eq!(picked.len(), 1);
        assert_eq!(picked[0].rfn, all[1].rfn, "opt-in narrows the set");
    }
}
