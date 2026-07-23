//! Pump-side catalog-dirty transaction tree.
//!
//! State stays keyed by writing xid, never merged eagerly into the top:
//! subxact abort must drop exactly its subtree's observations while sibling
//! and top dirt survive. Tree links (subxid → top) arrive from record-inline
//! toplevel xids (`XLR_BLOCK_ID_TOPLEVEL_XID`, first record of each assigned
//! subxact at `wal_level=logical`) and batched `XLOG_XACT_ASSIGNMENT`
//! records (every `PGPROC_MAX_CACHED_SUBXIDS` assignments); both name the
//! top xid directly, so links never chain. Admission asks "is this record's
//! tree dirty at this position" via a per-root dirty-member count. A subxid
//! whose top is still unknown counts as its own root: only its own later
//! records defer, tree-wide dirt applies once a link lands (spec: prefer
//! excess raw buffering over predecessor decode)

use std::collections::HashMap;
use std::collections::hash_map::Entry;

/// One catalog-dirty xid's accumulated capture inputs
#[derive(Debug)]
pub(crate) struct DirtyState {
    /// First catalog-touching record LSN under this xid
    pub(crate) first_touch: u64,
    /// User oid → first pg_class touch LSN under this xid
    pub(crate) oids: HashMap<u32, u64>,
    /// Wrote a capture-all catalog (pg_namespace)
    pub(crate) unenumerated: bool,
}

impl DirtyState {
    pub(crate) fn new(first_touch: u64) -> Self {
        Self {
            first_touch,
            oids: HashMap::new(),
            unenumerated: false,
        }
    }

    fn absorb(&mut self, other: Self) {
        self.first_touch = self.first_touch.min(other.first_touch);
        self.unenumerated |= other.unenumerated;
        for (oid, lsn) in other.oids {
            self.oids
                .entry(oid)
                .and_modify(|l| *l = (*l).min(lsn))
                .or_insert(lsn);
        }
    }
}

#[derive(Debug, Default)]
pub(crate) struct DirtyTree {
    /// subxid → top xid; entries drop at their tree's commit / abort.
    /// Crash-orphaned xids linger, bounded by workload (no xact end ever
    /// arrives to hold on)
    top_by_xid: HashMap<u32, u32>,
    /// Dirty capture state per writing xid (top or sub)
    state_by_xid: HashMap<u32, DirtyState>,
    /// Tree root → dirty member count. Invariant: every `state_by_xid`
    /// entry is counted once under its current root (`top_by_xid` value,
    /// else itself); late links move the credit
    dirty_members: HashMap<u32, u32>,
}

impl DirtyTree {
    fn root(&self, xid: u32) -> u32 {
        self.top_by_xid.get(&xid).copied().unwrap_or(xid)
    }

    /// Record subxid → top. Late link (child dirtied before its top was
    /// known) re-credits the child's dirty mark to the true top
    pub(crate) fn link(&mut self, sub: u32, top: u32) {
        if sub == 0 || top == 0 || sub == top || self.top_by_xid.insert(sub, top) == Some(top) {
            return;
        }
        // insert returned None: prior credit for sub's own state sat at sub
        if self.state_by_xid.contains_key(&sub) {
            decrement(&mut self.dirty_members, sub);
            *self.dirty_members.entry(top).or_default() += 1;
        }
    }

    /// Mark writing xid dirty at `lsn`; caller updates observation fields
    /// on the returned state
    pub(crate) fn touch(&mut self, xid: u32, lsn: u64) -> &mut DirtyState {
        let root = self.root(xid);
        match self.state_by_xid.entry(xid) {
            Entry::Occupied(e) => e.into_mut(),
            Entry::Vacant(e) => {
                *self.dirty_members.entry(root).or_default() += 1;
                e.insert(DirtyState::new(lsn))
            }
        }
    }

    /// Any member of `xid`'s known tree wrote catalog state still pending
    /// at this stream position
    pub(crate) fn is_dirty(&self, xid: u32) -> bool {
        xid != 0 && self.dirty_members.contains_key(&self.root(xid))
    }

    /// Xact end for `header_xid`'s tree: drop every known member's state
    /// and link, return the merge (commit boundary input; abort discards).
    /// Commit records list all committed children
    /// (`xactGetCommittedChildren`); aborted children already dropped their
    /// own state at their abort records. Linked-member sweep clears links
    /// the payload cannot name
    pub(crate) fn drain_tree(
        &mut self,
        header_xid: u32,
        twophase_xid: Option<u32>,
        subxacts: &[u32],
    ) -> Option<DirtyState> {
        let roots = [Some(header_xid), twophase_xid];
        let mut members: Vec<u32> = roots
            .iter()
            .flatten()
            .copied()
            .chain(subxacts.iter().copied())
            .collect();
        members.extend(
            self.top_by_xid
                .iter()
                .filter(|(_, top)| roots.contains(&Some(**top)))
                .map(|(x, _)| *x),
        );
        let mut merged: Option<DirtyState> = None;
        for x in members {
            let Some(state) = self.state_by_xid.remove(&x) else {
                self.top_by_xid.remove(&x);
                continue;
            };
            let root = self.root(x);
            decrement(&mut self.dirty_members, root);
            self.top_by_xid.remove(&x);
            match &mut merged {
                None => merged = Some(state),
                Some(m) => m.absorb(state),
            }
        }
        merged
    }
}

fn decrement(counts: &mut HashMap<u32, u32>, key: u32) {
    if let Some(n) = counts.get_mut(&key) {
        *n -= 1;
        if *n == 0 {
            counts.remove(&key);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn touch_dirties_own_xid_only() {
        let mut t = DirtyTree::default();
        t.touch(7, 100);
        assert!(t.is_dirty(7));
        assert!(!t.is_dirty(8), "unrelated xid never inherits");
        assert!(!t.is_dirty(0), "invalid xid is never dirty");
    }

    #[test]
    fn linked_child_dirt_covers_whole_tree() {
        let mut t = DirtyTree::default();
        t.link(101, 100);
        t.touch(101, 100);
        assert!(t.is_dirty(100), "top dirty via child");
        assert!(t.is_dirty(101));
        t.link(102, 100);
        assert!(t.is_dirty(102), "sibling dirty via shared root");
        assert!(!t.is_dirty(103), "unlinked xid stays clean");
    }

    #[test]
    fn late_link_recredits_child_dirt() {
        let mut t = DirtyTree::default();
        // Child dirtied before assignment named its top
        t.touch(101, 100);
        assert!(t.is_dirty(101));
        assert!(!t.is_dirty(100), "top unknown yet");
        t.link(101, 100);
        assert!(t.is_dirty(100), "assignment merges retained state");
        assert!(t.is_dirty(101));
        // Idempotent re-link keeps single credit
        t.link(101, 100);
        t.drain_tree(100, None, &[101]);
        assert!(!t.is_dirty(100));
        assert!(!t.is_dirty(101));
    }

    #[test]
    fn subxact_drain_keeps_sibling_and_top_dirt() {
        let mut t = DirtyTree::default();
        t.link(101, 100);
        t.link(102, 100);
        t.touch(101, 10);
        t.touch(102, 20);
        t.touch(100, 30);
        // ROLLBACK TO SAVEPOINT: abort record for 101 alone
        let dropped = t.drain_tree(101, None, &[]);
        assert_eq!(dropped.expect("state").first_touch, 10);
        assert!(t.is_dirty(100), "sibling + top dirt survives");
        assert!(!t.is_dirty(101), "aborted child link cleared");
        let merged = t.drain_tree(100, None, &[102]).expect("merge");
        assert_eq!(merged.first_touch, 20);
        assert!(!t.is_dirty(100));
        assert!(!t.is_dirty(102));
    }

    #[test]
    fn drain_merges_oids_and_flags() {
        let mut t = DirtyTree::default();
        t.touch(7, 100).oids.insert(16400, 100);
        let s = t.touch(101, 50);
        s.oids.insert(16400, 50);
        s.oids.insert(16500, 60);
        s.unenumerated = true;
        let m = t.drain_tree(7, None, &[101]).expect("merge");
        assert_eq!(m.first_touch, 50);
        assert!(m.unenumerated);
        assert_eq!(m.oids[&16400], 50, "min lsn wins");
        assert_eq!(m.oids[&16500], 60);
    }

    #[test]
    fn drain_sweeps_unlisted_linked_members() {
        let mut t = DirtyTree::default();
        t.link(101, 100);
        t.touch(101, 10);
        // Payload names no children; linked sweep still clears the tree
        let m = t.drain_tree(100, None, &[]).expect("swept state");
        assert_eq!(m.first_touch, 10);
        assert!(!t.is_dirty(101));
        assert!(t.top_by_xid.is_empty(), "link table drained");
    }

    #[test]
    fn twophase_drain_matches_prepared_root() {
        let mut t = DirtyTree::default();
        t.link(301, 300);
        t.touch(301, 10);
        // COMMIT PREPARED: header xid differs from prepared tree root
        let m = t.drain_tree(0, Some(300), &[]).expect("prepared tree");
        assert_eq!(m.first_touch, 10);
        assert!(!t.is_dirty(300));
        assert!(!t.is_dirty(301));
    }

    #[test]
    fn interleaved_trees_stay_isolated() {
        let mut t = DirtyTree::default();
        t.link(101, 100);
        t.touch(101, 10);
        t.link(201, 200);
        t.touch(201, 20);
        assert!(t.is_dirty(100));
        assert!(t.is_dirty(200));
        t.drain_tree(100, None, &[101]);
        assert!(!t.is_dirty(100));
        assert!(t.is_dirty(200), "draining one tree leaves the other");
        assert!(t.is_dirty(201));
    }
}
