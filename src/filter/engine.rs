//! Per-record routing decision for the WAL rewriter: where each record
//! goes, not whether it survives. Records fed in source order.
//!
//! Route policy:
//! * `Special` rmgr → ToShadow (recovery plumbing shadow needs verbatim)
//! * `Catalog` → ToShadow
//! * `User` → ToDecoder (XLOG_NOOP placeholder on shadow; original bytes
//!   feed the heap decoder)
//! * `Empty` → reclassify via `main_data::relation_for_empty` against
//!   `CatalogTracker`. Unrecognised → ToShadow: correctness over bytes,
//!   wrongly suppressing a catalog record breaks shadow.

use std::collections::HashSet;

use walrus::pg::walparser::{RmId, XLogRecord, XLogRecordBlock};

use crate::decode::wal_xact::{
    XLOG_XACT_ABORT, XLOG_XACT_ABORT_PREPARED, XLOG_XACT_COMMIT, XLOG_XACT_COMMIT_PREPARED,
    XLOG_XACT_OPMASK, parse_xact_payload,
};
use crate::filter::catalog_tracker::{CatalogTracker, CatalogTrackerStats};
use crate::filter::classify::{Class, classify};
use crate::filter::main_data;
use crate::filter::manifest::ManifestStats;
use crate::record::{CatalogSignal, Route, rmgr_label};

#[derive(Debug, Default, Clone, Copy)]
pub struct FilterStats {
    pub kept: u64,
    pub dropped: u64,
    pub kept_bytes: u64,
    pub dropped_bytes: u64,
    pub kept_catalog: u64,
    pub kept_user: u64,
    pub kept_special: u64,
    pub kept_empty: u64,
}

impl FilterStats {
    /// Field-wise difference; per-segment manifest carves a window out of
    /// a long-lived [`Filter`]'s cumulative `stats`.
    pub fn delta_from(&self, prev: &Self) -> Self {
        Self {
            kept: self.kept - prev.kept,
            dropped: self.dropped - prev.dropped,
            kept_bytes: self.kept_bytes - prev.kept_bytes,
            dropped_bytes: self.dropped_bytes - prev.dropped_bytes,
            kept_catalog: self.kept_catalog - prev.kept_catalog,
            kept_user: self.kept_user - prev.kept_user,
            kept_special: self.kept_special - prev.kept_special,
            kept_empty: self.kept_empty - prev.kept_empty,
        }
    }

    pub fn record(&mut self, class: Class, route: Route, bytes: u64) {
        match route {
            Route::ToShadow => {
                self.kept += 1;
                self.kept_bytes += bytes;
                match class {
                    Class::Catalog => self.kept_catalog += 1,
                    Class::User => self.kept_user += 1,
                    Class::Special => self.kept_special += 1,
                    Class::Empty => self.kept_empty += 1,
                }
            }
            Route::ToDecoder => {
                self.dropped += 1;
                self.dropped_bytes += bytes;
            }
        }
    }
}

impl ManifestStats {
    pub(crate) fn from_filter(stats: FilterStats, catalog: CatalogTrackerStats) -> Self {
        Self {
            records: stats.kept + stats.dropped,
            kept: stats.kept,
            dropped: stats.dropped,
            kept_bytes: stats.kept_bytes,
            dropped_bytes: stats.dropped_bytes,
            catalog_keeps: stats.kept_catalog,
            user_keeps: stats.kept_user,
            special_keeps: stats.kept_special,
            empty_keeps: stats.kept_empty,
            relmap_updates: catalog.relmap_updates,
            pg_class_writes_undecoded: catalog.pg_class_writes_undecoded,
            pg_class_writes_oid_in_prefix: catalog.pg_class_writes_oid_in_prefix,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct FilterSnapshot {
    stats: FilterStats,
    catalog: CatalogTrackerStats,
}

/// Full routing verdict for one record; see [`Filter::decide_record`].
#[derive(Debug, Clone, Copy)]
pub struct Verdict {
    pub route: Route,
    pub signal: CatalogSignal,
    /// Commit record of a catalog-mutating xact (top, subxact, or prepared
    /// xid wrote a catalog-touching record). Pump holds shadow publication
    /// here until replay passes the commit's `next_lsn`
    pub catalog_boundary: bool,
}

/// Routes against the *post-update* catalog set so an XLOG_RELMAP_UPDATE
/// introducing a new mapped-catalog filenumber immediately routes later
/// records on that filenumber to shadow.
pub struct Filter {
    tracker: CatalogTracker,
    stats: FilterStats,
    /// Xids (top or sub) that wrote a catalog-touching record; drained at
    /// their commit / abort. Crash-orphaned xids linger, bounded by
    /// workload (no commit ever arrives to hold on)
    catalog_dirty_xids: HashSet<u32>,
}

impl Filter {
    pub fn new() -> Self {
        Self {
            tracker: CatalogTracker::new(),
            stats: FilterStats::default(),
            catalog_dirty_xids: HashSet::new(),
        }
    }

    pub fn decide(&mut self, record: &XLogRecord) -> Route {
        self.decide_record(record).route
    }

    pub fn tracker(&self) -> &CatalogTracker {
        &self.tracker
    }

    pub fn tracker_mut(&mut self) -> &mut CatalogTracker {
        &mut self.tracker
    }

    pub fn stats(&self) -> &FilterStats {
        &self.stats
    }

    pub(crate) fn snapshot(&self) -> FilterSnapshot {
        FilterSnapshot {
            stats: self.stats,
            catalog: self.tracker.stats(),
        }
    }

    pub(crate) fn manifest_stats_since(&self, previous: FilterSnapshot) -> ManifestStats {
        ManifestStats::from_filter(
            self.stats.delta_from(&previous.stats),
            self.tracker.stats().delta_from(previous.catalog),
        )
    }

    /// [`Self::decide_record`] narrowed to `(route, signal)`.
    pub fn decide_with_signal(&mut self, record: &XLogRecord) -> (Route, CatalogSignal) {
        let v = self.decide_record(record);
        (v.route, v.signal)
    }

    /// Route plus the tracker's [`CatalogSignal`] verdict, stamped on the
    /// outgoing [`Record`](crate::record::Record) so the decoder worker
    /// bumps invalidation epochs at its own stream position (a
    /// pump-position bump would be consumable before pre-DDL records finish
    /// decoding; see `catalog_tracker` module doc), plus the
    /// catalog-boundary verdict driving the pump's publication hold.
    pub fn decide_record(&mut self, record: &XLogRecord) -> Verdict {
        let signal = self.tracker.observe(record);
        let class = classify(record);
        // `catalog_touch` marks the record's xid dirty: only paths proving a
        // catalog relation was written qualify. `Empty`'s None → ToShadow
        // safe default must not dirty (would hold at unrelated commits)
        let (route, catalog_touch) = match class {
            Class::Catalog => (Route::ToShadow, true),
            // Relmap update (VACUUM FULL mapped catalog) is Special-class
            Class::Special => (Route::ToShadow, signal != CatalogSignal::None),
            Class::User => {
                if any_block_is_catalog(&self.tracker, &record.blocks) {
                    // tracker has filenodes the bootstrap classify rule misses
                    (Route::ToShadow, true)
                } else {
                    (Route::ToDecoder, false)
                }
            }
            Class::Empty => match main_data::relation_for_empty(record) {
                Some(rel) => {
                    if self.tracker.is_catalog(rel.db_node, rel.rel_node) {
                        (Route::ToShadow, true)
                    } else {
                        (Route::ToDecoder, false)
                    }
                }
                None => (Route::ToShadow, false), // safe default
            },
        };
        let xid = record.header.xact_id;
        if catalog_touch && xid != 0 {
            self.catalog_dirty_xids.insert(xid);
        }
        let catalog_boundary = self.observe_xact_end(record);
        self.stats
            .record(class, route, record.header.total_record_length as u64);
        Verdict {
            route,
            signal,
            catalog_boundary,
        }
    }

    /// Drain dirty xids at commit / abort. Commit of any dirty xid (top,
    /// listed subxact, or prepared xid) is a catalog boundary; abort clears
    /// without holding — rolled-back catalog changes never become visible
    /// in shadow. Commit records carry the full committed-subxact list
    /// (`xactGetCommittedChildren`), so no ASSIGNMENT tracking is needed.
    fn observe_xact_end(&mut self, record: &XLogRecord) -> bool {
        if record.header.resource_manager_id != RmId::Xact as u8
            || self.catalog_dirty_xids.is_empty()
        {
            return false;
        }
        let info = record.header.info;
        let op = info & XLOG_XACT_OPMASK;
        let is_commit = op == XLOG_XACT_COMMIT || op == XLOG_XACT_COMMIT_PREPARED;
        let is_abort = op == XLOG_XACT_ABORT || op == XLOG_XACT_ABORT_PREPARED;
        if !is_commit && !is_abort {
            return false;
        }
        let payload = parse_xact_payload(info, &record.main_data);
        let mut hit = self.catalog_dirty_xids.remove(&record.header.xact_id);
        if let Some(x) = payload.twophase_xid {
            hit |= self.catalog_dirty_xids.remove(&x);
        }
        for x in &payload.subxacts {
            hit |= self.catalog_dirty_xids.remove(x);
        }
        hit && is_commit
    }

    pub fn rmgr_label(record: &XLogRecord) -> String {
        rmgr_label(record.header.resource_manager_id)
    }
}

impl Default for Filter {
    fn default() -> Self {
        Self::new()
    }
}

fn any_block_is_catalog(tracker: &CatalogTracker, blocks: &[XLogRecordBlock]) -> bool {
    blocks.iter().any(|b| {
        let r = b.header.location.rel;
        tracker.is_catalog(r.db_node, r.rel_node)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use walrus::pg::walparser::{
        BlockLocation, RelFileNode, RmId, XLogRecordBlockHeader, XLogRecordHeader,
    };

    fn rec(rm: RmId, rels: &[(u32, u32)]) -> XLogRecord<'static> {
        XLogRecord {
            header: XLogRecordHeader {
                resource_manager_id: rm as u8,
                total_record_length: 64,
                ..Default::default()
            },
            blocks: rels
                .iter()
                .map(|&(db, rel)| XLogRecordBlock {
                    header: XLogRecordBlockHeader {
                        location: BlockLocation {
                            rel: RelFileNode {
                                spc_node: 1663,
                                db_node: db,
                                rel_node: rel,
                            },
                            block_no: 0,
                        },
                        ..Default::default()
                    },
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        }
    }

    #[test]
    fn catalog_record_is_kept() {
        let mut f = Filter::new();
        let r = rec(RmId::Heap, &[(5, 1259)]);
        assert_eq!(f.decide(&r), Route::ToShadow);
    }

    #[test]
    fn user_record_is_dropped() {
        let mut f = Filter::new();
        let r = rec(RmId::Heap, &[(5, 20000)]);
        assert_eq!(f.decide(&r), Route::ToDecoder);
    }

    #[test]
    fn special_rmgr_is_kept() {
        let mut f = Filter::new();
        let r = rec(RmId::Xact, &[]);
        assert_eq!(f.decide(&r), Route::ToShadow);
    }

    #[test]
    fn empty_unknown_is_kept_safe_default() {
        let mut f = Filter::new();
        let r = rec(RmId::Heap, &[]);
        assert_eq!(f.decide(&r), Route::ToShadow);
    }

    #[test]
    fn tracker_promotes_user_to_catalog_post_relmap() {
        let mut f = Filter::new();
        // Learned mapping: catalog on db 5 rewritten to filenode 50000.
        f.tracker.add(5, 50000);
        let r = rec(RmId::Heap, &[(5, 50000)]);
        assert_eq!(f.decide(&r), Route::ToShadow);
    }

    #[test]
    fn empty_class_with_known_relation_is_classified_against_tracker() {
        use crate::filter::main_data::XLOG_HEAP2_NEW_CID;
        // XLOG_HEAP2_NEW_CID carries a locator in main_data (Class::Empty).
        // Catalog filenode (oid < 16384) → Keep; user filenode → Drop.
        fn new_cid_main_data(db: u32, rel: u32) -> Vec<u8> {
            let mut md = Vec::with_capacity(34);
            md.extend_from_slice(&100u32.to_le_bytes()); // top_xid
            md.extend_from_slice(&1u32.to_le_bytes()); // cmin
            md.extend_from_slice(&2u32.to_le_bytes()); // cmax
            md.extend_from_slice(&0u32.to_le_bytes()); // combocid
            md.extend_from_slice(&1663u32.to_le_bytes()); // spc
            md.extend_from_slice(&db.to_le_bytes());
            md.extend_from_slice(&rel.to_le_bytes());
            md.extend_from_slice(&[0u8; 6]); // target_tid
            md
        }
        fn new_cid_record(db: u32, rel: u32) -> XLogRecord<'static> {
            XLogRecord {
                header: walrus::pg::walparser::XLogRecordHeader {
                    resource_manager_id: RmId::Heap2 as u8,
                    info: XLOG_HEAP2_NEW_CID,
                    total_record_length: 64,
                    ..Default::default()
                },
                main_data: std::borrow::Cow::Owned(new_cid_main_data(db, rel)),
                ..Default::default()
            }
        }
        let mut f = Filter::new();
        // catalog filenode (1259 = pg_class) → Keep
        assert_eq!(f.decide(&new_cid_record(5, 1259)), Route::ToShadow);
        // user filenode → Drop
        assert_eq!(f.decide(&new_cid_record(5, 20000)), Route::ToDecoder);
    }

    #[test]
    fn decide_with_signal_surfaces_tracker_verdict() {
        use crate::record::CatalogSignal;
        let mut f = Filter::new();
        // pg_class heap insert (info 0x00, undecodable empty body: coarse
        // signal still fires)
        let (route, signal) = f.decide_with_signal(&rec(RmId::Heap, &[(5, 1259)]));
        assert_eq!(route, Route::ToShadow);
        assert_eq!(signal, CatalogSignal::Invalidate);
        let (route, signal) = f.decide_with_signal(&rec(RmId::Heap, &[(5, 20000)]));
        assert_eq!(route, Route::ToDecoder);
        assert_eq!(signal, CatalogSignal::None);
    }

    #[test]
    fn default_matches_new_and_rmgr_label_round_trips() {
        let _: Filter = Filter::default();
        let r = rec(RmId::Heap, &[]);
        let label = Filter::rmgr_label(&r);
        assert!(!label.is_empty());
    }

    #[test]
    fn stats_track_kept_dropped() {
        let mut f = Filter::new();
        f.decide(&rec(RmId::Heap, &[(5, 1259)]));
        f.decide(&rec(RmId::Heap, &[(5, 20000)]));
        f.decide(&rec(RmId::Heap, &[(5, 20001)]));
        assert_eq!(f.stats.kept, 1);
        assert_eq!(f.stats.dropped, 2);
    }

    fn rec_with_xid(rm: RmId, rels: &[(u32, u32)], xid: u32) -> XLogRecord<'static> {
        let mut r = rec(rm, rels);
        r.header.xact_id = xid;
        r
    }

    /// `xl_xact_commit` / `xl_xact_abort` main_data: xact_time, then
    /// optional xinfo + subxact / twophase sections.
    fn xact_end(op: u8, xid: u32, subxacts: &[u32], twophase: Option<u32>) -> XLogRecord<'static> {
        let mut info = op;
        let mut md: Vec<u8> = 0i64.to_le_bytes().to_vec();
        if !subxacts.is_empty() || twophase.is_some() {
            info |= XLOG_XACT_HAS_INFO;
            let mut xinfo = 0u32;
            if !subxacts.is_empty() {
                xinfo |= 1 << 1; // XACT_XINFO_HAS_SUBXACTS
            }
            if twophase.is_some() {
                xinfo |= 1 << 4; // XACT_XINFO_HAS_TWOPHASE
            }
            md.extend_from_slice(&xinfo.to_le_bytes());
            if !subxacts.is_empty() {
                md.extend_from_slice(&(subxacts.len() as i32).to_le_bytes());
                for x in subxacts {
                    md.extend_from_slice(&x.to_le_bytes());
                }
            }
            if let Some(x) = twophase {
                md.extend_from_slice(&x.to_le_bytes());
            }
        }
        let mut r = rec_with_xid(RmId::Xact, &[], xid);
        r.header.info = info;
        r.main_data = std::borrow::Cow::Owned(md);
        r
    }

    use crate::decode::wal_xact::XLOG_XACT_HAS_INFO;

    #[test]
    fn catalog_commit_is_boundary_dml_commit_is_not() {
        let mut f = Filter::new();
        f.decide_record(&rec_with_xid(RmId::Heap, &[(5, 1259)], 7));
        f.decide_record(&rec_with_xid(RmId::Heap, &[(5, 20000)], 8));
        // DML-only xid 8 commit: never parks
        let v = f.decide_record(&xact_end(XLOG_XACT_COMMIT, 8, &[], None));
        assert!(!v.catalog_boundary);
        // Catalog-dirty xid 7 commit: boundary, drained after
        let v = f.decide_record(&xact_end(XLOG_XACT_COMMIT, 7, &[], None));
        assert!(v.catalog_boundary);
        let v = f.decide_record(&xact_end(XLOG_XACT_COMMIT, 7, &[], None));
        assert!(!v.catalog_boundary, "dirty mark consumed once");
    }

    #[test]
    fn abort_clears_dirty_without_boundary() {
        let mut f = Filter::new();
        f.decide_record(&rec_with_xid(RmId::Heap, &[(5, 1259)], 7));
        let v = f.decide_record(&xact_end(XLOG_XACT_ABORT, 7, &[], None));
        assert!(!v.catalog_boundary, "rolled-back DDL never holds");
        let v = f.decide_record(&xact_end(XLOG_XACT_COMMIT, 7, &[], None));
        assert!(!v.catalog_boundary, "abort drained the mark");
    }

    #[test]
    fn subxact_catalog_write_marks_top_commit() {
        let mut f = Filter::new();
        // DDL under savepoint: catalog record carries subxid 101
        f.decide_record(&rec_with_xid(RmId::Heap, &[(5, 1259)], 101));
        let v = f.decide_record(&xact_end(XLOG_XACT_COMMIT, 100, &[101, 102], None));
        assert!(v.catalog_boundary);
    }

    #[test]
    fn commit_prepared_matches_prepared_xid() {
        let mut f = Filter::new();
        f.decide_record(&rec_with_xid(RmId::Heap, &[(5, 1259)], 300));
        // COMMIT PREPARED: header xid differs, prepared xid in payload
        let v = f.decide_record(&xact_end(XLOG_XACT_COMMIT_PREPARED, 0, &[], Some(300)));
        assert!(v.catalog_boundary);
    }

    #[test]
    fn relmap_update_marks_writing_xid() {
        use crate::filter::catalog_tracker::test_relmap_record as relmap;
        let mut f = Filter::new();
        let mut r = relmap(5, &[(1259, 50000)]);
        r.header.xact_id = 9;
        f.decide_record(&r);
        let v = f.decide_record(&xact_end(XLOG_XACT_COMMIT, 9, &[], None));
        assert!(
            v.catalog_boundary,
            "VACUUM FULL relmap write holds at commit"
        );
    }

    #[test]
    fn empty_safe_default_route_does_not_dirty() {
        let mut f = Filter::new();
        // Class::Empty, unrecognised main_data → ToShadow safe default
        let r = rec_with_xid(RmId::Heap, &[], 7);
        assert_eq!(f.decide_record(&r).route, Route::ToShadow);
        let v = f.decide_record(&xact_end(XLOG_XACT_COMMIT, 7, &[], None));
        assert!(
            !v.catalog_boundary,
            "safe-default keep is not a catalog touch"
        );
    }

    #[test]
    fn tracker_promoted_user_record_dirties() {
        let mut f = Filter::new();
        f.tracker.add(5, 50000); // rotated mapped catalog above 16384
        f.decide_record(&rec_with_xid(RmId::Heap, &[(5, 50000)], 7));
        let v = f.decide_record(&xact_end(XLOG_XACT_COMMIT, 7, &[], None));
        assert!(v.catalog_boundary);
    }
}
