//! Per-record routing decision used by the WAL rewriter: where each
//! record goes, not whether it survives.
//!
//! Wraps `classify` + `CatalogTracker` + `main_data` reclassifier into
//! a stateful `Filter` that callers feed each record in source order.
//!
//! Route policy:
//! * `Special` rmgr → ToShadow (recovery plumbing shadow needs verbatim)
//! * `Catalog` class → ToShadow
//! * `User` class → ToDecoder (XLOG_NOOP placeholder of same length on
//!   the shadow stream; original bytes feed the heap decoder)
//! * `Empty` class → reclassify via `main_data::relation_for_empty`,
//!   re-checking against `CatalogTracker`. Unrecognised Empty records
//!   default to ToShadow (correctness over efficiency: false-routing a
//!   non-catalog record to shadow is wasted bytes; wrongly suppressing
//!   a catalog record breaks shadow).

use serde::{Deserialize, Serialize};
use wal_rs::pg::walparser::{XLogRecord, XLogRecordBlock};

use crate::catalog_tracker::CatalogTracker;
use crate::classify::{Class, classify, rmgr_label};
use crate::main_data;

#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
pub enum Route {
    /// Replay on shadow verbatim — catalog + recovery-plumbing records
    /// shadow PG needs in its redo stream.
    #[default]
    ToShadow,
    /// Suppress on the shadow stream as an `XLOG_NOOP` of identical
    /// length; the original bytes route to the heap decoder for CH
    /// emission (heap rmgrs) or to nowhere (other user rmgrs, e.g.
    /// user-index records, which are simply not replayed).
    ToDecoder,
}

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
    /// Field-wise difference. Used by per-segment manifest emission
    /// against a long-lived [`Filter`] whose `stats` are cumulative.
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

/// Stateful filter. Updates the catalog tracker on every record then
/// returns the `Route` based on the *post-update* catalog set so an
/// XLOG_RELMAP_UPDATE that introduces a new mapped-catalog filenumber
/// can immediately route subsequent records on that filenumber to shadow.
pub struct Filter {
    pub tracker: CatalogTracker,
    pub stats: FilterStats,
}

impl Filter {
    pub fn new() -> Self {
        Self {
            tracker: CatalogTracker::new(),
            stats: FilterStats::default(),
        }
    }

    pub fn decide(&mut self, record: &XLogRecord) -> Route {
        self.tracker.observe(record);
        let class = classify(record);
        let route = match class {
            Class::Special | Class::Catalog => Route::ToShadow,
            Class::User => {
                if any_block_is_catalog(&self.tracker, &record.blocks) {
                    // classify under-counted because tracker has newer
                    // filenodes than the bootstrap rule knew about
                    Route::ToShadow
                } else {
                    Route::ToDecoder
                }
            }
            Class::Empty => match main_data::relation_for_empty(record) {
                Some(rel) => {
                    if self.tracker.is_catalog(rel.db_node, rel.rel_node) {
                        Route::ToShadow
                    } else {
                        Route::ToDecoder
                    }
                }
                None => Route::ToShadow, // safe default
            },
        };
        self.stats
            .record(class, route, record.header.total_record_length as u64);
        route
    }

    /// Per-rmgr label string for diagnostics.
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
    use wal_rs::pg::walparser::{
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
        // Manually inject a learned mapping (pg_class on db 5 rewritten to 50000)
        f.tracker.add(5, 50000);
        let r = rec(RmId::Heap, &[(5, 50000)]);
        assert_eq!(f.decide(&r), Route::ToShadow);
    }

    #[test]
    fn empty_class_with_known_relation_is_classified_against_tracker() {
        use crate::main_data::XLOG_HEAP2_NEW_CID;
        // XLOG_HEAP2_NEW_CID carries a locator in main_data — Class::Empty
        // with `relation_for_empty(Some(rel))`. Build one targeting a
        // catalog filenode (oid < 16384) → Keep; one targeting a user
        // filenode → Drop.
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
                header: wal_rs::pg::walparser::XLogRecordHeader {
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
}
