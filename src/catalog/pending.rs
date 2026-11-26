//! Xid-scoped speculative catalog timeline.
//!
//! A pending descriptor is visible only to records of the transaction that
//! wrote it, and becomes durable only at that transaction's commit. Nothing
//! here reaches the [`DescriptorLog`](crate::catalog::desc_log::DescriptorLog)
//! until [`crate::source::catalog_capture`] promotes it into the commit
//! batch, so abort is a map removal with nothing to compensate.
//!
//! Slots come from the bridge worker's `SCAN` at a command boundary, where
//! shadow has replayed exactly through the boundary LSN and the writing
//! transaction's catalog rows sit on-page uncommitted. Replay parked at that
//! LSN is what does the temporal filtering: the latest uncommitted row
//! version on the page *is* the state as of the boundary.
//!
//! If capture cannot read a command boundary, record a miss for
//! each relation it names. PostgreSQL registers a relation cache invalidation
//! for writes to `pg_class`, `pg_attribute`, `pg_index`, and `pg_constraint`
//! (`CacheInvalidateHeapTupleCommon` in `src/backend/utils/cache/inval.c`).
//! A skipped read leaves those relations' definitions unknown until the next
//! successful read or commit.
//!
//! Until `XLOG_XACT_ASSIGNMENT` links a subtransaction to its parent, capture
//! stores its reads under its own transaction ID. At commit,
//! [`PendingCatalog::consolidate`] uses the full member list to combine them.
//! A boundary that refreshes names can only re-read relations stored under
//! its own key. At commit, record a miss for earlier reads under other keys.

use std::sync::{Arc, RwLock};

use tokio_postgres::types::Oid;

use walrus::pg::walparser::RelFileNode;

use crate::schema::RelDescriptor;
use foldhash::HashMap;

/// Why capture could not read a command boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DegradeReason {
    /// Shadow replay was not parked where the boundary said it was
    ReplayMismatch,
    /// Overlay read failed
    QueryError,
}

impl DegradeReason {
    pub const ALL: [DegradeReason; 2] = [DegradeReason::ReplayMismatch, DegradeReason::QueryError];

    pub fn label(self) -> &'static str {
        match self {
            DegradeReason::ReplayMismatch => "replay_mismatch",
            DegradeReason::QueryError => "query_error",
        }
    }
}

/// One relation's shape at one command boundary of one transaction
#[derive(Debug, Clone)]
pub struct PendingSlot {
    /// LSN where capture read this descriptor.
    pub boundary: u64,
    /// (Sub)xid current at capture; a subxact abort drops its slots
    pub writer_xid: u32,
    pub desc: Arc<RelDescriptor>,
}

/// A command boundary where a relation's definition went unread.
#[derive(Debug, Clone, Copy)]
struct Miss {
    at: u64,
    writer_xid: u32,
}

/// One transaction tree's timeline
#[derive(Debug, Default)]
struct PendingXact {
    /// Descriptors by relation OID, sorted by LSN.
    by_oid: HashMap<Oid, Vec<PendingSlot>>,
    /// Misses by relation OID, sorted by LSN.
    misses: HashMap<Oid, Vec<Miss>>,
    /// Reason for the first miss, used to count each transaction once.
    missed: Option<DegradeReason>,
    /// Namespace changes and relation cache flushes, sorted by LSN.
    /// Each can refresh names only for relations stored under this key.
    name_refreshes: Vec<Miss>,
}

impl PendingXact {
    fn push(&mut self, slot: PendingSlot) {
        let chain = self.by_oid.entry(slot.desc.oid).or_default();
        // Keep one descriptor per LSN, using the latest read.
        match chain.binary_search_by_key(&slot.boundary, |s| s.boundary) {
            Ok(at) => chain[at] = slot,
            Err(at) => chain.insert(at, slot),
        }
    }

    /// Keep the first missed read between successful reads for the same
    /// writer. Keep other writers' misses for savepoint rollback.
    fn push_miss(&mut self, oid: Oid, miss: Miss) {
        let last_read = self
            .by_oid
            .get(&oid)
            .and_then(|c| c.last())
            .map(|s| s.boundary);
        let list = self.misses.entry(oid).or_default();
        let redundant = list
            .last()
            .is_some_and(|m| m.writer_xid == miss.writer_xid && last_read.is_none_or(|b| b < m.at));
        if !redundant {
            let at = list.partition_point(|m| m.at <= miss.at);
            list.insert(at, miss);
        }
    }

    fn absorb(&mut self, other: Self) {
        let stale: Vec<(Oid, Miss)> = unrefreshed(&self.name_refreshes, &other)
            .chain(unrefreshed(&other.name_refreshes, self))
            .collect();
        self.name_refreshes.extend(other.name_refreshes);
        self.missed = self.missed.or(other.missed);
        for slot in other.by_oid.into_values().flatten() {
            self.push(slot);
        }
        for (oid, misses) in other.misses {
            let list = self.misses.entry(oid).or_default();
            list.extend(misses);
            list.sort_unstable_by_key(|m| m.at);
        }
        for (oid, miss) in stale {
            let list = self.misses.entry(oid).or_default();
            let at = list.partition_point(|m| m.at <= miss.at);
            list.insert(at, miss);
        }
    }

    fn slots(&self) -> usize {
        self.by_oid.values().map(Vec::len).sum()
    }

    /// Drop reads and misses from the aborted subtransaction and its children.
    fn drop_writers(&mut self, members: &[u32]) -> usize {
        let before = self.slots();
        self.by_oid
            .values_mut()
            .for_each(|s| s.retain(|s| !members.contains(&s.writer_xid)));
        self.by_oid.retain(|_, slots| !slots.is_empty());
        self.misses
            .values_mut()
            .for_each(|m| m.retain(|m| !members.contains(&m.writer_xid)));
        self.misses.retain(|_, misses| !misses.is_empty());
        self.name_refreshes
            .retain(|r| !members.contains(&r.writer_xid));
        before - self.slots()
    }
}

/// Find earlier reads in `side` that these name refreshes could not reach.
fn unrefreshed<'a>(
    refreshes: &'a [Miss],
    side: &'a PendingXact,
) -> impl Iterator<Item = (Oid, Miss)> + 'a {
    refreshes.iter().flat_map(move |r| {
        side.by_oid
            .iter()
            .filter(move |(_, chain)| chain.first().is_some_and(|s| s.boundary < r.at))
            .map(move |(oid, _)| (*oid, *r))
    })
}

/// Catalog reads and misses for transactions with uncommitted catalog changes.
/// Clear them on abort or after draining the commit.
#[derive(Debug, Default)]
pub struct PendingCatalog {
    by_xid: RwLock<HashMap<u32, PendingXact>>,
}

/// Catalog reads and misses ready to include in a commit batch.
pub struct PromotedXact {
    /// Descriptors sorted by relation OID, then LSN.
    by_oid: Vec<(Oid, Vec<PendingSlot>)>,
    /// Misses sorted by relation OID, then LSN.
    misses: Vec<(Oid, Vec<u64>)>,
}

impl PromotedXact {
    /// Descriptors read for this relation, sorted by LSN.
    pub fn slots_for(&self, oid: Oid) -> &[PendingSlot] {
        self.by_oid
            .binary_search_by_key(&oid, |(o, _)| *o)
            .map_or(&[][..], |at| &self.by_oid[at].1)
    }

    /// Missed reads for this relation, sorted by LSN.
    pub fn misses_for(&self, oid: Oid) -> &[u64] {
        self.misses
            .binary_search_by_key(&oid, |(o, _)| *o)
            .map_or(&[][..], |at| &self.misses[at].1)
    }
}

impl PendingCatalog {
    /// Record a missed read at `at` for each OID. Return `true` on the first
    /// miss for this transaction.
    pub fn miss(
        &self,
        top_xid: u32,
        writer_xid: u32,
        oids: &[Oid],
        at: u64,
        reason: DegradeReason,
    ) -> bool {
        let mut map = self.by_xid.write().expect("pending catalog poisoned");
        let xact = map.entry(top_xid).or_default();
        for oid in oids {
            xact.push_miss(*oid, Miss { at, writer_xid });
        }
        let first = xact.missed.is_none();
        xact.missed = xact.missed.or(Some(reason));
        first
    }

    /// Install one boundary's shapes
    pub fn record(&self, top_xid: u32, slots: Vec<PendingSlot>) {
        if slots.is_empty() {
            return;
        }
        let mut map = self.by_xid.write().expect("pending catalog poisoned");
        let xact = map.entry(top_xid).or_default();
        for slot in slots {
            xact.push(slot);
        }
    }

    /// Record a name refresh. Multiple capture workers can report the same LSN.
    pub fn name_refresh(&self, top_xid: u32, writer_xid: u32, at: u64) {
        let mut map = self.by_xid.write().expect("pending catalog poisoned");
        let refreshes = &mut map.entry(top_xid).or_default().name_refreshes;
        if refreshes.last().is_none_or(|r| r.at != at) {
            refreshes.push(Miss { at, writer_xid });
        }
    }

    /// Relation a transaction's read placed on this filenode
    pub fn rfn_owner(&self, rfn: RelFileNode) -> Option<Oid> {
        let map = self.by_xid.read().expect("pending catalog poisoned");
        map.values()
            .flat_map(|x| x.by_oid.values().flatten())
            .find(|s| s.desc.rfn == rfn)
            .map(|s| s.desc.oid)
    }

    /// OIDs of relations already read for this transaction, in order.
    pub fn slot_oids(&self, top_xid: u32) -> Vec<Oid> {
        let map = self.by_xid.read().expect("pending catalog poisoned");
        let mut oids: Vec<Oid> = map
            .get(&top_xid)
            .map(|x| x.by_oid.keys().copied().collect())
            .unwrap_or_default();
        oids.sort_unstable();
        oids
    }

    /// Fold every member key into the top. Links arrive late, so a subxact's
    /// boundaries may have keyed under the subxid; the commit's drained
    /// member list is the first place the whole tree is named
    pub fn consolidate(&self, top_xid: u32, members: &[u32]) {
        let mut map = self.by_xid.write().expect("pending catalog poisoned");
        let mut merged: Option<PendingXact> = map.remove(&top_xid);
        for m in members.iter().filter(|m| **m != top_xid) {
            let Some(other) = map.remove(m) else { continue };
            match &mut merged {
                Some(acc) => acc.absorb(other),
                None => merged = Some(other),
            }
        }
        if let Some(merged) = merged {
            map.insert(top_xid, merged);
        }
    }

    /// Copy the combined catalog history for a transaction.
    pub fn promoted(&self, top_xid: u32) -> Option<PromotedXact> {
        let map = self.by_xid.read().expect("pending catalog poisoned");
        let xact = map.get(&top_xid)?;
        let mut by_oid: Vec<(Oid, Vec<PendingSlot>)> = xact
            .by_oid
            .iter()
            .map(|(oid, slots)| (*oid, slots.clone()))
            .collect();
        by_oid.sort_unstable_by_key(|(oid, _)| *oid);
        let mut misses: Vec<(Oid, Vec<u64>)> = xact
            .misses
            .iter()
            .map(|(oid, m)| (*oid, m.iter().map(|m| m.at).collect()))
            .collect();
        misses.sort_unstable_by_key(|(oid, _)| *oid);
        Some(PromotedXact { by_oid, misses })
    }

    /// Drop a finished tree, returning the slots dropped
    pub fn forget_tree(&self, top_xid: u32) -> usize {
        let mut map = self.by_xid.write().expect("pending catalog poisoned");
        map.remove(&top_xid).map_or(0, |x| x.slots())
    }

    /// Drop an aborted subtree: whole trees keyed under a member, plus slots
    /// any member wrote under another key. Runs on the pump at the abort
    /// record, ahead of any later boundary that would promote them
    pub fn forget_members(&self, members: &[u32]) -> usize {
        let mut map = self.by_xid.write().expect("pending catalog poisoned");
        let mut dropped = 0;
        for m in members {
            if let Some(x) = map.remove(m) {
                dropped += x.slots();
            }
        }
        for xact in map.values_mut() {
            dropped += xact.drop_writers(members);
        }
        // Keep the parent's first miss after savepoint rollback.
        map.retain(|_, x| x.slots() > 0 || x.missed.is_some() || !x.name_refreshes.is_empty());
        dropped
    }

    /// Transactions with live timelines; a gauge, not a cap
    pub fn tracked_xacts(&self) -> usize {
        self.by_xid.read().expect("pending catalog poisoned").len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{RelName, ReplIdent};

    fn desc(oid: Oid, rel_node: u32, name: &str) -> Arc<RelDescriptor> {
        Arc::new(RelDescriptor {
            rfn: RelFileNode {
                spc_node: 1663,
                db_node: 5,
                rel_node,
            },
            oid,
            toast_oid: 0,
            namespace_oid: 2200,
            rel_name: RelName::new("public", name),
            kind: b'r',
            persistence: b'p',
            ancestors: Vec::new(),
            replident: ReplIdent::Default { pk_attnums: None },
            attributes: Vec::new(),
        })
    }

    fn slot(boundary: u64, writer_xid: u32, desc: Arc<RelDescriptor>) -> PendingSlot {
        PendingSlot {
            boundary,
            writer_xid,
            desc,
        }
    }

    fn names(p: &PromotedXact, oid: Oid) -> Vec<&str> {
        p.slots_for(oid)
            .iter()
            .map(|s| &*s.desc.rel_name.name)
            .collect()
    }

    #[test]
    fn slots_order_by_boundary_and_re_read_replaces() {
        let p = PendingCatalog::default();
        p.record(100, vec![slot(400, 100, desc(16400, 7000, "late"))]);
        p.record(100, vec![slot(200, 100, desc(16400, 7000, "first"))]);
        p.record(100, vec![slot(200, 100, desc(16400, 7000, "second"))]);
        p.record(100, Vec::new());
        let promoted = p.promoted(100).expect("tree");
        assert_eq!(names(&promoted, 16400), ["second", "late"]);
        assert!(promoted.slots_for(16401).is_empty());
        assert_eq!(p.slot_oids(100), [16400]);
        assert!(
            p.slot_oids(101).is_empty(),
            "another transaction has no catalog reads"
        );
        assert!(p.promoted(101).is_none());
    }

    #[test]
    fn subxact_abort_drops_only_its_writers() {
        let p = PendingCatalog::default();
        p.record(100, vec![slot(200, 100, desc(16400, 7000, "pre"))]);
        p.record(100, vec![slot(300, 101, desc(16400, 7000, "in-savepoint"))]);
        p.miss(100, 101, &[16400], 350, DegradeReason::QueryError);
        p.miss(100, 100, &[16401], 360, DegradeReason::QueryError);
        assert_eq!(p.forget_members(&[101]), 1);
        let promoted = p.promoted(100).expect("tree");
        assert_eq!(names(&promoted, 16400), ["pre"], "keep the parent's read");
        assert!(promoted.misses_for(16400).is_empty(), "drop savepoint miss");
        assert_eq!(promoted.misses_for(16401), [360], "keep the parent's miss");
    }

    #[test]
    fn abort_drops_a_tree_keyed_under_an_unlinked_subxid() {
        let p = PendingCatalog::default();
        p.record(101, vec![slot(300, 101, desc(16400, 7000, "t"))]);
        assert_eq!(p.forget_members(&[100, 101]), 1);
        assert_eq!(p.tracked_xacts(), 0);
        p.consolidate(100, &[100, 101]);
        assert_eq!(p.tracked_xacts(), 0, "aborted members have no history");
    }

    #[test]
    fn abort_keeps_a_tree_that_already_missed() {
        let p = PendingCatalog::default();
        p.record(100, vec![slot(300, 101, desc(16400, 7000, "t"))]);
        p.miss(100, 101, &[16400], 400, DegradeReason::ReplayMismatch);
        assert_eq!(p.forget_members(&[101]), 1);
        assert!(
            !p.miss(100, 100, &[16400], 500, DegradeReason::ReplayMismatch),
            "already counted a miss for this transaction",
        );
    }

    #[test]
    fn consolidate_folds_member_keys_into_the_top() {
        let p = PendingCatalog::default();
        // Child captured before its assignment named the top
        p.record(101, vec![slot(200, 101, desc(16400, 7000, "child"))]);
        p.miss(101, 101, &[16400], 300, DegradeReason::QueryError);
        p.record(100, vec![slot(400, 100, desc(16400, 7000, "top"))]);
        p.miss(100, 100, &[16400], 250, DegradeReason::ReplayMismatch);
        p.consolidate(100, &[100, 101]);
        assert_eq!(p.tracked_xacts(), 1);
        let promoted = p.promoted(100).expect("tree");
        assert_eq!(names(&promoted, 16400), ["child", "top"]);
        assert_eq!(
            promoted.misses_for(16400),
            [250, 300],
            "merge missed reads in LSN order"
        );
    }

    /// When combining histories, record a miss for earlier reads a namespace
    /// change could not refresh.
    #[test]
    fn consolidate_marks_relations_a_name_refresh_missed() {
        let p = PendingCatalog::default();
        p.record(101, vec![slot(200, 101, desc(16400, 7000, "child"))]);
        p.name_refresh(101, 101, 250);
        p.record(100, vec![slot(300, 100, desc(16401, 7001, "top"))]);
        p.name_refresh(100, 100, 400);
        p.name_refresh(100, 100, 400);
        p.record(102, vec![slot(500, 102, desc(16402, 7002, "late"))]);
        p.consolidate(100, &[100, 101, 102]);
        let promoted = p.promoted(100).expect("tree");
        assert_eq!(
            promoted.misses_for(16400),
            [400],
            "parent's refresh missed the child's relation"
        );
        assert!(
            promoted.misses_for(16401).is_empty(),
            "child's refresh came before the parent's read",
        );
        assert!(
            promoted.misses_for(16402).is_empty(),
            "read after both refreshes"
        );
    }

    #[test]
    fn subxact_abort_drops_its_name_refreshes() {
        let p = PendingCatalog::default();
        p.name_refresh(100, 101, 250);
        p.name_refresh(100, 100, 260);
        p.forget_members(&[101]);
        assert_eq!(p.tracked_xacts(), 1, "keep the parent's name refresh");
        p.record(102, vec![slot(200, 102, desc(16400, 7000, "child"))]);
        p.consolidate(100, &[100, 102]);
        assert_eq!(p.promoted(100).expect("tree").misses_for(16400), [260]);
        assert_eq!(p.forget_members(&[100]), 1);
        assert_eq!(p.tracked_xacts(), 0);
    }

    #[test]
    fn consolidate_moves_a_tree_only_its_child_captured() {
        let p = PendingCatalog::default();
        p.record(101, vec![slot(200, 101, desc(16400, 7000, "child"))]);
        p.consolidate(100, &[100, 101]);
        assert_eq!(p.tracked_xacts(), 1);
        assert_eq!(names(&p.promoted(100).expect("tree"), 16400), ["child"]);
        assert_eq!(p.forget_tree(100), 1);
        assert_eq!(p.tracked_xacts(), 0);
    }

    #[test]
    fn filenode_owner_comes_from_any_transaction() {
        let p = PendingCatalog::default();
        p.record(100, vec![slot(200, 100, desc(16400, 7000, "a"))]);
        p.record(100, vec![slot(300, 100, desc(16400, 7001, "b"))]);
        p.record(101, vec![slot(250, 101, desc(16401, 7002, "c"))]);
        let rfn = |rel_node| RelFileNode {
            spc_node: 1663,
            db_node: 5,
            rel_node,
        };
        assert_eq!(p.rfn_owner(rfn(7000)), Some(16400));
        assert_eq!(p.rfn_owner(rfn(7002)), Some(16401));
        assert_eq!(p.rfn_owner(rfn(7003)), None);
    }

    #[test]
    fn first_miss_counts_once_and_repeats_collapse_per_run() {
        let p = PendingCatalog::default();
        assert!(p.miss(
            100,
            100,
            &[16400, 16401],
            200,
            DegradeReason::ReplayMismatch
        ));
        assert!(!p.miss(100, 100, &[16400], 300, DegradeReason::QueryError));
        p.record(100, vec![slot(400, 100, desc(16400, 7000, "t"))]);
        p.miss(100, 100, &[16400], 500, DegradeReason::ReplayMismatch);
        p.miss(100, 102, &[16400], 600, DegradeReason::ReplayMismatch);
        let promoted = p.promoted(100).expect("tree");
        assert_eq!(
            promoted.misses_for(16400),
            [200, 500, 600],
            "keep one miss per writer between reads",
        );
        assert_eq!(promoted.misses_for(16401), [200]);
        assert!(promoted.misses_for(16402).is_empty());
    }
}
