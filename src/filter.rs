//! Per-record keep/drop decision used by the WAL rewriter.
//!
//! Wraps `classify` + `CatalogTracker` + `main_data` reclassifier into
//! a stateful `Filter` that callers feed each record in source order.
//!
//! Decision policy:
//! * `Special` rmgr → Keep (recovery plumbing shadow needs verbatim)
//! * `Catalog` class → Keep
//! * `User` class → Drop (XLOG_NOOP placeholder of same length)
//! * `Empty` class → reclassify via `main_data::relation_for_empty`,
//!   re-checking against `CatalogTracker`. Unrecognised Empty records
//!   default to Keep (correctness over efficiency: false-keeping a
//!   non-catalog record is wasted bytes; false-dropping breaks shadow).

use serde::{Deserialize, Serialize};
use wal_rs::pg::walparser::{XLogRecord, XLogRecordBlock};

use crate::catalog_tracker::CatalogTracker;
use crate::classify::{Class, classify, rmgr_label};
use crate::main_data;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum Decision {
    Keep,
    Drop,
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

    pub fn record(&mut self, class: Class, decision: Decision, bytes: u64) {
        match decision {
            Decision::Keep => {
                self.kept += 1;
                self.kept_bytes += bytes;
                match class {
                    Class::Catalog => self.kept_catalog += 1,
                    Class::User => self.kept_user += 1,
                    Class::Special => self.kept_special += 1,
                    Class::Empty => self.kept_empty += 1,
                }
            }
            Decision::Drop => {
                self.dropped += 1;
                self.dropped_bytes += bytes;
            }
        }
    }
}

/// Stateful filter. Updates the catalog tracker on every record then
/// returns Keep/Drop based on the *post-update* catalog set so an
/// XLOG_RELMAP_UPDATE that introduces a new mapped-catalog filenumber
/// can immediately keep subsequent records on that filenumber.
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

    pub fn decide(&mut self, record: &XLogRecord) -> Decision {
        self.tracker.observe(record);
        let class = classify(record);
        let decision = match class {
            Class::Special | Class::Catalog => Decision::Keep,
            Class::User => {
                if any_block_is_catalog(&self.tracker, &record.blocks) {
                    // classify under-counted because tracker has newer
                    // filenodes than the bootstrap rule knew about
                    Decision::Keep
                } else {
                    Decision::Drop
                }
            }
            Class::Empty => match main_data::relation_for_empty(record) {
                Some(rel) => {
                    if self.tracker.is_catalog(rel.db_node, rel.rel_node) {
                        Decision::Keep
                    } else {
                        Decision::Drop
                    }
                }
                None => Decision::Keep, // safe default
            },
        };
        self.stats
            .record(class, decision, record.header.total_record_length as u64);
        decision
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

    fn rec(rm: RmId, rels: &[(u32, u32)]) -> XLogRecord {
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
        assert_eq!(f.decide(&r), Decision::Keep);
    }

    #[test]
    fn user_record_is_dropped() {
        let mut f = Filter::new();
        let r = rec(RmId::Heap, &[(5, 20000)]);
        assert_eq!(f.decide(&r), Decision::Drop);
    }

    #[test]
    fn special_rmgr_is_kept() {
        let mut f = Filter::new();
        let r = rec(RmId::Xact, &[]);
        assert_eq!(f.decide(&r), Decision::Keep);
    }

    #[test]
    fn empty_unknown_is_kept_safe_default() {
        let mut f = Filter::new();
        let r = rec(RmId::Heap, &[]);
        assert_eq!(f.decide(&r), Decision::Keep);
    }

    #[test]
    fn tracker_promotes_user_to_catalog_post_relmap() {
        let mut f = Filter::new();
        // Manually inject a learned mapping (pg_class on db 5 rewritten to 50000)
        f.tracker.add(5, 50000);
        let r = rec(RmId::Heap, &[(5, 50000)]);
        assert_eq!(f.decide(&r), Decision::Keep);
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
