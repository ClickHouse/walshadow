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
use foldhash::HashMap;

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
    /// Boundary the degrade happened at: the first position no slot
    /// accounts for. Slots before it stay exact; a late degrade must not
    /// void the coverage earlier boundaries already read
    degraded_at: Option<u64>,
}

impl PendingXact {
    fn degrade(&mut self, reason: DegradeReason, at: u64) -> bool {
        // Earliest position wins: a member's earlier miss bounds the tree
        self.degraded_at = Some(self.degraded_at.map_or(at, |d| d.min(at)));
        if self.degraded.is_some() {
            return false;
        }
        self.degraded = Some(reason);
        true
    }

    /// Slots a degrade has not voided
    fn live<'a>(&self, slots: &'a [PendingSlot]) -> &'a [PendingSlot] {
        let Some(at) = self.degraded_at else {
            return slots;
        };
        &slots[..slots.partition_point(|s| s.valid_from < at)]
    }

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
        self.degraded_at = match (self.degraded_at, other.degraded_at) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        };
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
    /// Per-oid chains, ascending by `valid_from`, oids ascending. Slots at
    /// or past `degraded_at` are already cut
    pub by_oid: Vec<(Oid, Vec<PendingSlot>)>,
    pub degraded: Option<DegradeReason>,
    pub degraded_at: Option<u64>,
}

/// Span of one relation's dirty interval the timeline answers for
#[derive(Debug, Clone, Copy)]
pub struct Coverage<'a> {
    /// First boundary that named the relation
    pub from: u64,
    /// Where a degrade cut the timeline; `None` runs to commit
    pub until: Option<u64>,
    /// Last shape recorded before `until`: what the uncovered tail was
    /// written under, up to whatever the missed boundary changed
    pub last: &'a RelDescriptor,
}

impl PromotedXact {
    /// Span the timeline answers for `oid`, `None` when it names no slot.
    /// From the first boundary that named the relation on, each slot covers
    /// up to the next; before it, the transaction had not reached a
    /// `CommandCounterIncrement`, so the pre-transaction shape still reads
    /// its rows.
    ///
    /// A degrade missed a boundary, a shape change nothing recorded, so
    /// coverage stops there. Slots read before it stay exact: rows they
    /// cover were written under them whatever came later
    pub fn coverage(&self, oid: Oid) -> Option<Coverage<'_>> {
        let slots = self.slots_for(oid);
        Some(Coverage {
            from: slots.first()?.valid_from,
            until: self.degraded_at,
            last: &slots.last()?.desc,
        })
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
        at: u64,
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
            xact.degrade(reason, at);
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

    /// Degrade at boundary `at`. First reason sticks, earliest position
    /// wins; `true` when this call is what degraded it
    pub fn degrade(&self, top_xid: u32, reason: DegradeReason, at: u64) -> bool {
        let mut map = self.by_xid.write().expect("pending catalog poisoned");
        map.entry(top_xid).or_default().degrade(reason, at)
    }

    /// First position the timeline no longer answers for, `None` while it
    /// answers through commit
    pub fn degraded_at(&self, top_xid: u32) -> Option<u64> {
        let map = self.by_xid.read().expect("pending catalog poisoned");
        map.get(&top_xid).and_then(|x| x.degraded_at)
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
        let xact = map.get(&top_xid)?;
        xact.live(xact.chains.get(&rfn)?)
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

    /// One filenode's chain up to any degrade, for a caller folding many
    /// records against it. Empty when the transaction has no pending
    /// coverage of it
    pub fn chain(&self, top_xid: u32, rfn: RelFileNode) -> Vec<PendingSlot> {
        let map = self.by_xid.read().expect("pending catalog poisoned");
        map.get(&top_xid)
            .and_then(|x| Some(x.live(x.chains.get(&rfn)?).to_vec()))
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
            .map(|(oid, slots)| (*oid, xact.live(slots).to_vec()))
            .filter(|(_, slots)| !slots.is_empty())
            .collect();
        by_oid.sort_unstable_by_key(|(oid, _)| *oid);
        Some(PromotedXact {
            by_oid,
            degraded: xact.degraded,
            degraded_at: xact.degraded_at,
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
            kind: b'r',
            persistence: b'p',
            ancestors: Vec::new(),
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
        p.degrade(101, DegradeReason::QueryError, 500);
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
    fn consolidate_moves_a_tree_only_its_child_captured() {
        let p = PendingCatalog::default();
        let rfn = desc(16400, 7000, "t").rfn;
        p.record(101, vec![slot(200, 101, desc(16400, 7000, "child"))]);
        p.consolidate(100, &[100, 101]);
        assert_eq!(p.tracked_xacts(), 1);
        assert_eq!(&*p.chain(100, rfn)[0].desc.rel_name.name, "child");
    }

    #[test]
    fn admit_caps_boundaries_then_degrades() {
        let p = PendingCatalog::default();
        assert_eq!(p.admit(100, 0, 2, u64::MAX), Ok(()));
        assert_eq!(p.admit(100, 0, 2, u64::MAX), Ok(()));
        assert_eq!(
            p.admit(100, 0, 2, u64::MAX),
            Err(Some(DegradeReason::CapExceeded))
        );
        assert_eq!(p.degraded(100), Some(DegradeReason::CapExceeded));
        assert_eq!(p.admit(100, 0, 8, u64::MAX), Err(None), "degrade is sticky");
    }

    #[test]
    fn admit_stops_at_the_hold_budget() {
        let p = PendingCatalog::default();
        assert_eq!(p.admit(100, 0, 64, 1_000), Ok(()));
        p.charge_hold(100, 1_500);
        assert_eq!(
            p.admit(100, 0, 64, 1_000),
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
        let cov = promoted.coverage(16400).expect("covered");
        assert_eq!((cov.from, cov.until), (200, None));
        assert!(
            promoted.coverage(16401).is_none(),
            "oid with no slot is uncovered"
        );
        assert_eq!(p.forget_tree(100), 2);
        assert!(p.promoted(100).is_none());
    }

    /// A degrade cuts the timeline at its boundary: slots read before it
    /// keep answering, nothing at or past it does
    #[test]
    fn late_degrade_keeps_earlier_slots() {
        let p = PendingCatalog::default();
        let rfn = desc(16400, 7000, "t").rfn;
        p.record(100, vec![slot(200, 100, desc(16400, 7000, "added"))]);
        p.record(100, vec![slot(400, 100, desc(16400, 7000, "not_null"))]);
        assert!(p.degrade(100, DegradeReason::CaptureAll, 600));
        let promoted = p.promoted(100).expect("tree");
        let cov = promoted.coverage(16400).expect("pre-degrade slots stay");
        assert_eq!((cov.from, cov.until), (200, Some(600)));
        assert_eq!(&*cov.last.rel_name.name, "not_null");
        assert_eq!(p.chain(100, rfn).len(), 2);

        // A member's earlier miss bounds the whole tree, and voids the slot
        // past it
        p.record(101, vec![]);
        assert!(p.degrade(101, DegradeReason::QueryError, 300));
        p.consolidate(100, &[100, 101]);
        assert_eq!(p.degraded_at(100), Some(300));
        assert_eq!(p.degraded(100), Some(DegradeReason::CaptureAll));
        let promoted = p.promoted(100).expect("tree");
        let cov = promoted.coverage(16400).expect("slot before 300 stays");
        assert_eq!((cov.from, cov.until), (200, Some(300)));
        assert_eq!(&*cov.last.rel_name.name, "added");
        assert_eq!(p.chain(100, rfn).len(), 1);
        assert!(
            p.descriptor_at(100, rfn, 450)
                .is_some_and(|d| &*d.rel_name.name == "added")
        );
    }

    /// Degrade before any boundary named the relation leaves nothing
    #[test]
    fn early_degrade_leaves_no_coverage() {
        let p = PendingCatalog::default();
        assert!(p.degrade(100, DegradeReason::ReplayMismatch, 100));
        assert_eq!(p.admit(100, 200, 64, u64::MAX), Err(None), "sticky");
        p.record(100, vec![slot(200, 100, desc(16400, 7000, "t"))]);
        assert!(p.promoted(100).expect("tree").coverage(16400).is_none());
    }
}
