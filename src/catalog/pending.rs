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
//! Keys are the tree root as the pump knew it at capture, which for a subxact
//! whose `XLOG_XACT_ASSIGNMENT` has not arrived is the subxid itself.
//! [`PendingCatalog::consolidate`] folds those keys into the top at the
//! commit, where the filter's drained member list names them all.

use std::sync::{Arc, RwLock};

use tokio_postgres::types::Oid;
use walrus::pg::walparser::RelFileNode;

use crate::schema::RelDescriptor;
use ahash::HashMap;

/// Why one transaction's pending coverage stopped being trustworthy. Every
/// reason falls back to commit-time capture, which is sound
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DegradeReason {
    /// Boundary could not enumerate the relations it touched (whole-relcache
    /// flush, pg_namespace catcache)
    CaptureAll,
    /// Transaction passed `pending_max_boundaries_per_xact`
    CapExceeded,
    /// Transaction passed its cumulative hold budget
    HoldBudget,
    /// Shadow replay was not parked where the boundary said it was
    ReplayMismatch,
    /// Overlay read failed
    QueryError,
}

impl DegradeReason {
    pub const ALL: [DegradeReason; 5] = [
        DegradeReason::CaptureAll,
        DegradeReason::CapExceeded,
        DegradeReason::HoldBudget,
        DegradeReason::ReplayMismatch,
        DegradeReason::QueryError,
    ];

    pub fn label(self) -> &'static str {
        match self {
            DegradeReason::CaptureAll => "capture_all",
            DegradeReason::CapExceeded => "cap_exceeded",
            DegradeReason::HoldBudget => "hold_budget",
            DegradeReason::ReplayMismatch => "replay_mismatch",
            DegradeReason::QueryError => "query_error",
        }
    }
}

/// One relation's shape at one command boundary of one transaction
#[derive(Debug, Clone)]
pub struct PendingSlot {
    /// First LSN this shape answers for: the boundary, or the generation's
    /// `XLOG_SMGR_CREATE` marker when the relation was born in this xact
    pub valid_from: u64,
    /// (Sub)xid current at capture; a subxact abort drops its slots
    pub writer_xid: u32,
    pub desc: Arc<RelDescriptor>,
}

/// One transaction tree's timeline
#[derive(Debug, Default)]
struct PendingXact {
    /// Per-filenode chains, ascending by `valid_from`
    chains: HashMap<RelFileNode, Vec<PendingSlot>>,
    by_oid: HashMap<Oid, Vec<PendingSlot>>,
    /// Boundaries admitted, against `pending_max_boundaries_per_xact`
    boundaries: u32,
    /// Cumulative parked nanos, against the per-xact hold budget
    hold_nanos: u64,
    degraded: Option<DegradeReason>,
}

impl PendingXact {
    fn push(&mut self, slot: PendingSlot) {
        let oid = slot.desc.oid;
        let rfn = slot.desc.rfn;
        insert_sorted(self.chains.entry(rfn).or_default(), slot.clone());
        insert_sorted(self.by_oid.entry(oid).or_default(), slot);
    }

    fn absorb(&mut self, other: Self) {
        self.boundaries += other.boundaries;
        self.hold_nanos += other.hold_nanos;
        self.degraded = self.degraded.or(other.degraded);
        for slot in other.chains.into_values().flatten() {
            let rfn = slot.desc.rfn;
            insert_sorted(self.chains.entry(rfn).or_default(), slot);
        }
        for (oid, slots) in other.by_oid {
            let chain = self.by_oid.entry(oid).or_default();
            for slot in slots {
                insert_sorted(chain, slot);
            }
        }
    }

    fn slots(&self) -> usize {
        self.chains.values().map(Vec::len).sum()
    }

    /// Drop every slot written by a member of the aborting subtree
    fn drop_writers(&mut self, members: &[u32]) -> usize {
        let before = self.slots();
        let keep =
            |slots: &mut Vec<PendingSlot>| slots.retain(|s| !members.contains(&s.writer_xid));
        self.chains.values_mut().for_each(keep);
        self.chains.retain(|_, slots| !slots.is_empty());
        self.by_oid.values_mut().for_each(keep);
        self.by_oid.retain(|_, slots| !slots.is_empty());
        before - self.slots()
    }
}

/// Chains stay `valid_from`-ascending. A re-capture at the same position
/// (same command boundary re-read) replaces rather than appends: two shapes
/// at one LSN have no order between them
fn insert_sorted(chain: &mut Vec<PendingSlot>, slot: PendingSlot) {
    match chain.binary_search_by_key(&slot.valid_from, |s| s.valid_from) {
        Ok(at) => chain[at] = slot,
        Err(at) => chain.insert(at, slot),
    }
}

/// Speculative catalog state for every in-flight catalog-dirty transaction.
/// Pump writes at command boundaries, the commit drain reads, and both the
/// abort record (pump) and the finished drain (reorder) remove
#[derive(Debug, Default)]
pub struct PendingCatalog {
    by_xid: RwLock<HashMap<u32, PendingXact>>,
}

/// What one transaction's timeline says at promotion
pub struct PromotedXact {
    /// Per-oid chains, ascending by `valid_from`, oids ascending
    pub by_oid: Vec<(Oid, Vec<PendingSlot>)>,
    pub degraded: Option<DegradeReason>,
}

impl PromotedXact {
    /// LSN from which the timeline answers for `oid`, `None` when it does
    /// not. From the first boundary that named the relation on, each slot
    /// covers up to the next; before it, the transaction had not reached a
    /// `CommandCounterIncrement`, so the pre-transaction shape still reads
    /// its rows.
    ///
    /// A degraded transaction missed at least one boundary, and a missed
    /// boundary is a shape change nothing recorded, so none of its interval
    /// is covered
    pub fn coverage_from(&self, oid: Oid) -> Option<u64> {
        if self.degraded.is_some() {
            return None;
        }
        self.slots_for(oid).first().map(|s| s.valid_from)
    }

    /// One relation's shapes, ascending by `valid_from`
    pub fn slots_for(&self, oid: Oid) -> &[PendingSlot] {
        self.by_oid
            .binary_search_by_key(&oid, |(o, _)| *o)
            .map_or(&[][..], |at| &self.by_oid[at].1)
    }
}

impl PendingCatalog {
    /// Admit one command boundary against the transaction's caps. Refusal
    /// degrades the transaction: a boundary the daemon declines to read is a
    /// shape change it cannot account for. `Err(Some(reason))` when this
    /// call is what degraded it, `Err(None)` when it already was
    pub fn admit(
        &self,
        top_xid: u32,
        max_boundaries: u32,
        hold_budget_nanos: u64,
    ) -> Result<(), Option<DegradeReason>> {
        let mut map = self.by_xid.write().expect("pending catalog poisoned");
        let xact = map.entry(top_xid).or_default();
        if xact.degraded.is_some() {
            return Err(None);
        }
        let over = if xact.boundaries >= max_boundaries {
            Some(DegradeReason::CapExceeded)
        } else if xact.hold_nanos >= hold_budget_nanos {
            Some(DegradeReason::HoldBudget)
        } else {
            None
        };
        if let Some(reason) = over {
            xact.degraded = Some(reason);
            return Err(Some(reason));
        }
        xact.boundaries += 1;
        Ok(())
    }

    /// Charge a released hold against the transaction's cumulative budget
    pub fn charge_hold(&self, top_xid: u32, nanos: u64) {
        let mut map = self.by_xid.write().expect("pending catalog poisoned");
        map.entry(top_xid).or_default().hold_nanos += nanos;
    }

    /// First reason sticks; `true` when this call is what degraded it
    pub fn degrade(&self, top_xid: u32, reason: DegradeReason) -> bool {
        let mut map = self.by_xid.write().expect("pending catalog poisoned");
        let xact = map.entry(top_xid).or_default();
        if xact.degraded.is_some() {
            return false;
        }
        xact.degraded = Some(reason);
        true
    }

    pub fn degraded(&self, top_xid: u32) -> Option<DegradeReason> {
        let map = self.by_xid.read().expect("pending catalog poisoned");
        map.get(&top_xid).and_then(|x| x.degraded)
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

    /// Shape `top_xid` sees for `rfn` at `lsn`, `None` before its first
    /// boundary — where the committed chain is the answer
    pub fn descriptor_at(
        &self,
        top_xid: u32,
        rfn: RelFileNode,
        lsn: u64,
    ) -> Option<Arc<RelDescriptor>> {
        let map = self.by_xid.read().expect("pending catalog poisoned");
        let chain = map.get(&top_xid)?.chains.get(&rfn)?;
        chain
            .iter()
            .rev()
            .find(|s| s.valid_from <= lsn)
            .map(|s| s.desc.clone())
    }

    /// Whether an earlier boundary of this transaction already recorded the
    /// filenode. A generation's smgr marker is a lower bound only for the
    /// first shape seen on it; later boundaries answer from themselves
    pub fn has_slots(&self, top_xid: u32, rfn: RelFileNode) -> bool {
        let map = self.by_xid.read().expect("pending catalog poisoned");
        map.get(&top_xid)
            .is_some_and(|x| x.chains.contains_key(&rfn))
    }

    /// One filenode's whole chain, for a caller folding many records against
    /// it. Empty when the transaction has no pending coverage of it
    pub fn chain(&self, top_xid: u32, rfn: RelFileNode) -> Vec<PendingSlot> {
        let map = self.by_xid.read().expect("pending catalog poisoned");
        map.get(&top_xid)
            .and_then(|x| x.chains.get(&rfn))
            .cloned()
            .unwrap_or_default()
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

    /// The consolidated tree's timeline, left in place for the drain's
    /// per-record fold
    pub fn promoted(&self, top_xid: u32) -> Option<PromotedXact> {
        let map = self.by_xid.read().expect("pending catalog poisoned");
        let xact = map.get(&top_xid)?;
        let mut by_oid: Vec<(Oid, Vec<PendingSlot>)> = xact
            .by_oid
            .iter()
            .map(|(oid, slots)| (*oid, slots.clone()))
            .collect();
        by_oid.sort_unstable_by_key(|(oid, _)| *oid);
        Some(PromotedXact {
            by_oid,
            degraded: xact.degraded,
        })
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
        // A tree whose every slot the subtree wrote still owes its caps and
        // its degrade verdict to the commit
        map.retain(|_, x| x.slots() > 0 || x.boundaries > 0 || x.degraded.is_some());
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
            kind: 'r',
            persistence: 'p',
            replident: ReplIdent::Default { pk_attnums: None },
            attributes: Vec::new(),
        })
    }

    fn slot(valid_from: u64, writer_xid: u32, desc: Arc<RelDescriptor>) -> PendingSlot {
        PendingSlot {
            valid_from,
            writer_xid,
            desc,
        }
    }

    #[test]
    fn descriptor_at_takes_latest_slot_not_past_lsn() {
        let p = PendingCatalog::default();
        let rfn = desc(16400, 7000, "t").rfn;
        p.record(100, vec![slot(200, 100, desc(16400, 7000, "t"))]);
        p.record(100, vec![slot(400, 100, desc(16400, 7000, "renamed"))]);
        assert!(p.descriptor_at(100, rfn, 199).is_none(), "before coverage");
        assert_eq!(
            &*p.descriptor_at(100, rfn, 200).unwrap().rel_name.name,
            "t",
            "coverage starts at valid_from"
        );
        assert_eq!(&*p.descriptor_at(100, rfn, 399).unwrap().rel_name.name, "t");
        assert_eq!(
            &*p.descriptor_at(100, rfn, 400).unwrap().rel_name.name,
            "renamed"
        );
        assert!(
            p.descriptor_at(101, rfn, 500).is_none(),
            "another transaction sees nothing"
        );
    }

    #[test]
    fn re_capture_at_one_position_replaces() {
        let p = PendingCatalog::default();
        let rfn = desc(16400, 7000, "t").rfn;
        p.record(100, vec![slot(200, 100, desc(16400, 7000, "first"))]);
        p.record(100, vec![slot(200, 100, desc(16400, 7000, "second"))]);
        assert_eq!(p.chain(100, rfn).len(), 1);
        assert_eq!(
            &*p.descriptor_at(100, rfn, 200).unwrap().rel_name.name,
            "second"
        );
    }

    #[test]
    fn subxact_abort_drops_only_its_writers() {
        let p = PendingCatalog::default();
        let rfn = desc(16400, 7000, "t").rfn;
        p.record(100, vec![slot(200, 100, desc(16400, 7000, "pre"))]);
        p.record(100, vec![slot(300, 101, desc(16400, 7000, "in-savepoint"))]);
        assert_eq!(p.forget_members(&[101]), 1);
        let chain = p.chain(100, rfn);
        assert_eq!(chain.len(), 1);
        assert_eq!(&*chain[0].desc.rel_name.name, "pre", "parent slot survives");
    }

    #[test]
    fn abort_drops_a_tree_keyed_under_an_unlinked_subxid() {
        let p = PendingCatalog::default();
        p.record(101, vec![slot(300, 101, desc(16400, 7000, "t"))]);
        assert_eq!(p.forget_members(&[100, 101]), 1);
        assert_eq!(p.tracked_xacts(), 0);
    }

    #[test]
    fn consolidate_folds_member_keys_into_the_top() {
        let p = PendingCatalog::default();
        let rfn = desc(16400, 7000, "t").rfn;
        // Child captured before its assignment named the top
        p.record(101, vec![slot(200, 101, desc(16400, 7000, "child"))]);
        p.record(100, vec![slot(400, 100, desc(16400, 7000, "top"))]);
        p.degrade(101, DegradeReason::QueryError);
        p.consolidate(100, &[100, 101]);
        assert_eq!(p.tracked_xacts(), 1);
        let chain = p.chain(100, rfn);
        assert_eq!(chain.len(), 2);
        assert_eq!(&*chain[0].desc.rel_name.name, "child");
        assert_eq!(
            p.degraded(100),
            Some(DegradeReason::QueryError),
            "a member's degrade degrades the tree"
        );
    }

    #[test]
    fn admit_caps_boundaries_then_degrades() {
        let p = PendingCatalog::default();
        assert_eq!(p.admit(100, 2, u64::MAX), Ok(()));
        assert_eq!(p.admit(100, 2, u64::MAX), Ok(()));
        assert_eq!(
            p.admit(100, 2, u64::MAX),
            Err(Some(DegradeReason::CapExceeded))
        );
        assert_eq!(p.degraded(100), Some(DegradeReason::CapExceeded));
        assert_eq!(p.admit(100, 8, u64::MAX), Err(None), "degrade is sticky");
    }

    #[test]
    fn admit_stops_at_the_hold_budget() {
        let p = PendingCatalog::default();
        assert_eq!(p.admit(100, 64, 1_000), Ok(()));
        p.charge_hold(100, 1_500);
        assert_eq!(
            p.admit(100, 64, 1_000),
            Err(Some(DegradeReason::HoldBudget))
        );
        assert_eq!(p.degraded(100), Some(DegradeReason::HoldBudget));
    }

    #[test]
    fn coverage_starts_at_the_first_boundary_naming_the_oid() {
        let p = PendingCatalog::default();
        p.record(100, vec![slot(400, 100, desc(16400, 7000, "t"))]);
        p.record(100, vec![slot(200, 100, desc(16400, 7000, "t"))]);
        let promoted = p.promoted(100).expect("tree");
        assert_eq!(promoted.coverage_from(16400), Some(200));
        assert_eq!(
            promoted.coverage_from(16401),
            None,
            "oid with no slot is uncovered"
        );
        p.degrade(100, DegradeReason::CaptureAll);
        assert_eq!(
            p.promoted(100).expect("tree").coverage_from(16400),
            None,
            "a missed boundary is a shape change nothing recorded",
        );
        assert_eq!(p.forget_tree(100), 2);
        assert!(p.promoted(100).is_none());
    }
}
