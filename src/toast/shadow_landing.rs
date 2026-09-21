//! Which files shadow must hold for `[toast] mode = "shadow"`
//!
//! Shadow keeps TOAST heaps and their indexes the way it keeps catalogs:
//! landed in place by the backup, then replayed. Index filenodes are absent
//! from the catalog seed, so the source answers for them.

use ahash::{HashMap, HashSet, HashSetExt};
use anyhow::{Context, Result, bail, ensure};
use tokio::sync::Mutex;

use crate::catalog::shadow_catalog::ShadowCatalog;
use crate::filter::shadow_relations::ShadowHeld;
use crate::schema::{RelDescriptor, RelName};

/// `pg_default`, PG `catalog/pg_tablespace.dat`
pub const DEFAULT_TABLESPACE_OID: u32 = 1663;

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
            (!held.contains((toast.rfn.db_node, toast.rfn.rel_node))).then_some((desc, toast))
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

/// Opted-in TOAST heaps, refusing any the lander cannot place. It keys on
/// `base/<db>/` paths, so a non-default tablespace has to fail here rather
/// than at shadow's first read
fn toast_heaps<'a>(
    descriptors: impl Iterator<Item = &'a RelDescriptor>,
    tap_filenodes: Option<&HashSet<(u32, u32)>>,
) -> Result<Vec<&'a RelDescriptor>> {
    let heaps: Vec<_> = descriptors
        .filter(|d| d.kind == 't')
        .filter(|d| tap_filenodes.is_none_or(|set| set.contains(&(d.rfn.db_node, d.rfn.rel_node))))
        .collect();
    if let Some(d) = heaps
        .iter()
        .find(|d| d.rfn.spc_node != DEFAULT_TABLESPACE_OID)
    {
        bail!(
            "bootstrap: TOAST relation {} (filenode {}) is in tablespace {}; \
             [toast] mode = shadow keeps files from default tablespace only",
            d.rel_name,
            d.rfn.rel_node,
            d.rfn.spc_node,
        );
    }
    Ok(heaps)
}

/// Filenodes to land and replay: every opted-in TOAST heap plus its indexes
pub async fn toast_relations<'a>(
    sql: &tokio_postgres::Client,
    descriptors: impl Iterator<Item = &'a RelDescriptor>,
    tap_filenodes: Option<&HashSet<(u32, u32)>>,
) -> Result<HashSet<(u32, u32)>> {
    let mut out = HashSet::new();
    let mut heaps = Vec::new();
    let mut db_oid = 0;
    for d in toast_heaps(descriptors, tap_filenodes)? {
        db_oid = d.rfn.db_node;
        heaps.push(d.rfn.rel_node);
        out.insert((db_oid, d.rfn.rel_node));
    }
    if heaps.is_empty() {
        return Ok(out);
    }
    // Reuse authenticated sidecar client from catalog seed
    // Interpolate because client has no `oid[]` Rust mapping
    let list = heaps
        .iter()
        .map(u32::to_string)
        .collect::<Vec<_>>()
        .join(",");
    let rows = sql
        .query(
            &format!(
                "SELECT ic.relfilenode::int8, ic.relname::text, \
                 coalesce(nullif(ic.reltablespace, 0), \
                          (SELECT dattablespace FROM pg_database \
                           WHERE datname = current_database()))::int8 \
                 FROM pg_class t \
                 JOIN pg_index i ON i.indrelid = t.oid \
                 JOIN pg_class ic ON ic.oid = i.indexrelid \
                 WHERE t.relkind = 't' \
                 AND t.relfilenode = ANY('{{{list}}}'::oid[])"
            ),
            &[],
        )
        .await
        .context("bootstrap: read toast index filenodes")?;
    for row in &rows {
        let spc = row.get::<_, i64>(2) as u32;
        ensure!(
            spc == DEFAULT_TABLESPACE_OID,
            "bootstrap: TOAST index {} is in tablespace {spc}; \
             [toast] mode = shadow keeps files from default tablespace only",
            row.get::<_, String>(1),
        );
        out.insert((db_oid, row.get::<_, i64>(0) as u32));
    }
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
    use crate::schema::{RelName, ReplIdent};
    use walrus::pg::walparser::RelFileNode;

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
            replident: ReplIdent::Default { pk_attnums: None },
            attributes: Vec::new(),
        }
    }

    #[test]
    fn heaps_take_opted_in_toast_and_refuse_another_tablespace() {
        let all = [
            desc('t', 16400, DEFAULT_TABLESPACE_OID),
            desc('t', 16401, DEFAULT_TABLESPACE_OID),
            desc('r', 16402, DEFAULT_TABLESPACE_OID),
        ];
        let picked = toast_heaps(all.iter(), None).unwrap();
        assert_eq!(picked.len(), 2, "user heaps are not TOAST storage");

        let tap = [(5u32, 16400u32)].into_iter().collect();
        let picked = toast_heaps(all.iter(), Some(&tap)).unwrap();
        assert_eq!(picked[0].rfn.rel_node, 16400, "opt-in narrows the set");

        let elsewhere = [desc('t', 16403, 99)];
        let err = toast_heaps(elsewhere.iter(), None).unwrap_err();
        assert!(err.to_string().contains("tablespace 99"), "{err}");
        // Out of the opted-in set, so its tablespace never matters
        assert!(
            toast_heaps(elsewhere.iter(), Some(&tap))
                .unwrap()
                .is_empty()
        );
    }
}
