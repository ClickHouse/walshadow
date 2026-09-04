//! Replace relations whose backup-page visibility cannot be proven
//!
//! Read each pending relation through PostgreSQL `COPY`, emit visible,
//! detoasted rows at page-walk coverage LSN. Keep chunk pages in mirror for
//! later WAL rows carrying old external pointers

use anyhow::{Context, Result, bail};
use tokio::sync::mpsc;
use walrus::pg::replication::conn::PgConfig;
use walrus::pg::walparser::{Oid, RelFileNode};

use crate::backfill::backfill_bootstrap::seed_in_snapshot;
use crate::backfill::backup_page_walk::{BackfillTuple, CatalogMap};
use crate::backfill::copy_backfill::copy_rows_into;
use crate::pg::current_wal_lsn;
use crate::schema::{RelDescriptor, RelName};
use crate::source::source_feed::open_sql_client;
use ahash::{HashMap, HashSet};

/// Reason for authoritative relation read
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PendingReason {
    /// `xmax` multixact outside backup coverage
    UnresolvedMultiXact,
    /// Relation owns TOAST storage with ambiguous chunk generations
    ExternalToast,
}

/// Relations handed from page walk to repair
#[derive(Debug, Default)]
pub struct PendingSet {
    rels: HashMap<(Oid, Oid), PendingReason>,
    /// Filenodes repair may read, `None` unrestricted. Greenfield scopes to
    /// the mapping snapshot: the drain routes by mapping, so reading an
    /// unmapped relation ships rows it drops
    scope: Option<HashSet<(Oid, Oid)>>,
}

impl PendingSet {
    /// Empty set for per-table loads without repair
    pub fn empty() -> Self {
        Self::default()
    }

    /// Pre-mark relations owning TOAST storage
    pub fn toast_capable(catalog: &CatalogMap) -> Self {
        let mut set = Self::default();
        for desc in catalog.descriptors().filter(|d| d.toast_oid != 0) {
            set.rels.insert(
                (desc.rfn.db_node, desc.rfn.rel_node),
                PendingReason::ExternalToast,
            );
        }
        set
    }

    /// Restrict repair to `mapped` filenodes, dropping pre-marks outside it
    pub fn scoped_to(mut self, mapped: HashSet<(Oid, Oid)>) -> Self {
        self.rels.retain(|k, _| mapped.contains(k));
        self.scope = Some(mapped);
        self
    }

    /// Hand a relation over mid-walk. First reason wins. Out of scope is
    /// dropped: the tuple was headed for the drain's discard either way
    pub fn mark(&mut self, rfn: RelFileNode, reason: PendingReason) {
        let key = (rfn.db_node, rfn.rel_node);
        if self.scope.as_ref().is_some_and(|s| !s.contains(&key)) {
            return;
        }
        self.rels.entry(key).or_insert(reason);
    }

    /// Check whether repair replaces relation main pages
    pub fn holds(&self, db_node: Oid, rel_node: Oid) -> bool {
        self.rels.contains_key(&(db_node, rel_node))
    }

    pub fn is_empty(&self) -> bool {
        self.rels.is_empty()
    }

    pub fn len(&self) -> usize {
        self.rels.len()
    }

    pub fn count_for(&self, reason: PendingReason) -> u64 {
        self.rels.values().filter(|r| **r == reason).count() as u64
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct RepairStats {
    pub relations: u64,
    pub rows: u64,
    /// Relations `initial_load = "none"` opted out of
    pub skipped: u64,
    /// Source write head after final read
    pub p_hi: u64,
}

/// Read pending relations through PostgreSQL at `coverage_lsn`
///
/// Reject descriptors changed since page walk
pub async fn repair(
    pending: &PendingSet,
    catalog: &CatalogMap,
    skip_initial: &HashSet<RelName>,
    source: &PgConfig,
    coverage_lsn: u64,
    tx: &mpsc::Sender<BackfillTuple>,
) -> Result<RepairStats> {
    let mut stats = RepairStats::default();
    if pending.is_empty() {
        return Ok(stats);
    }
    let client = open_sql_client(source)
        .await
        .context("visibility repair: source sql connect")?;
    // Re-read the same seed the walk's catalog came from, so an unchanged
    // relation compares equal field for field
    let fresh = seed_in_snapshot(&client)
        .await
        .context("visibility repair: re-seed source catalog")?;

    for &(db_node, rel_node) in pending.rels.keys() {
        let desc = catalog.get(db_node, rel_node).with_context(|| {
            format!("visibility repair: filenode {db_node}/{rel_node} left the catalog map")
        })?;
        if skip_initial.contains(&desc.rel_name) {
            stats.skipped += 1;
            continue;
        }
        assert_unchanged(&fresh, &desc)?;
        stats.relations += 1;
        stats.rows += copy_rows_into(&client, &desc, coverage_lsn, tx)
            .await
            .with_context(|| format!("visibility repair: COPY {}", desc.rel_name))?;
    }
    // Sample after reads to bound every snapshot
    stats.p_hi = current_wal_lsn(&client)
        .await
        .context("visibility repair: source write head")?;
    Ok(stats)
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
    use ahash::HashSetExt;
    use std::sync::Arc;

    fn desc(
        oid: Oid,
        rel_node: Oid,
        toast_oid: Oid,
        namespace: &str,
        name: &str,
    ) -> Arc<RelDescriptor> {
        make_rel_named(oid, rel_node, toast_oid, RelName::new(namespace, name))
    }

    /// A relation with a toast relation is pending before the walk. Its
    /// chunk store is not: the mirror is a cache the walk keeps filling
    #[test]
    fn toast_capable_scope_takes_the_parent_but_not_its_chunks() {
        let mut catalog = CatalogMap::new();
        catalog.insert(desc(16400, 16400, 16402, "public", "with_text"));
        catalog.insert(desc(16402, 16403, 0, "pg_toast", "pg_toast_16400"));
        catalog.insert(desc(16500, 16500, 0, "public", "fixed_width"));

        let set = PendingSet::toast_capable(&catalog);

        assert_eq!(set.len(), 1);
        assert!(set.holds(5, 16400), "parent is pending");
        assert!(!set.holds(5, 16403), "its chunk filenode still walks");
        assert!(!set.holds(5, 16500), "a fixed-width relation still walks");
        assert_eq!(set.count_for(PendingReason::ExternalToast), 1);
    }

    /// Repair reads the source, so scope follows the drain's routes: an
    /// unmapped relation's rows would be dropped on arrival
    #[test]
    fn scope_drops_unmapped_relations_and_their_later_marks() {
        let mut catalog = CatalogMap::new();
        catalog.insert(desc(16400, 16400, 16402, "public", "mapped"));
        catalog.insert(desc(16500, 16500, 16502, "public", "unmapped"));

        let mut set =
            PendingSet::toast_capable(&catalog).scoped_to([(5, 16400)].into_iter().collect());

        assert_eq!(set.len(), 1);
        assert!(set.holds(5, 16400));
        assert!(!set.holds(5, 16500), "pre-mark outside the mapping drops");

        let unmapped = catalog.get(5, 16500).unwrap();
        set.mark(unmapped.rfn, PendingReason::UnresolvedMultiXact);
        assert!(!set.holds(5, 16500), "so does a walk-time mark");
    }

    /// Marking mid-walk keeps the first reason
    #[test]
    fn walk_time_mark_keeps_the_first_reason() {
        let mut catalog = CatalogMap::new();
        catalog.insert(desc(16400, 16400, 16402, "public", "t"));
        let mut set = PendingSet::empty();
        assert!(!set.holds(5, 16400));

        let parent = catalog.get(5, 16400).unwrap();
        set.mark(parent.rfn, PendingReason::UnresolvedMultiXact);
        set.mark(parent.rfn, PendingReason::ExternalToast);

        assert_eq!(set.len(), 1);
        assert!(set.holds(5, 16400));
        assert_eq!(set.count_for(PendingReason::UnresolvedMultiXact), 1);
        assert_eq!(set.count_for(PendingReason::ExternalToast), 0);
    }

    /// Nothing pending means no source connection at all
    #[tokio::test]
    async fn repair_without_pending_relations_touches_nothing() {
        let (tx, _rx) = mpsc::channel(1);
        let source = crate::config::SourceConn::default().to_pg_config();
        let stats = repair(
            &PendingSet::empty(),
            &CatalogMap::new(),
            &HashSet::new(),
            &source,
            0x1000,
            &tx,
        )
        .await
        .unwrap();
        assert_eq!(stats, RepairStats::default());
    }
}
