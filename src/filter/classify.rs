//! Per-record classification of WAL into catalog / user / special buckets.
//!
//! Catalog detection uses `rel_node < FirstNormalObjectId` (16384, see
//! PG `src/include/access/transam.h`). Catches non-mapped
//! catalogs unconditionally + mapped catalogs (pg_class, pg_attribute, …)
//! only at their initial relfilenode; VACUUM FULL rewrites a mapped
//! catalog above 16384, so RM_RELMAP_ID + pg_class heap tracking keeps the
//! mapped set current.
//!
//! Special rmgrs are recovery plumbing shadow Postgres needs regardless of
//! which relations replay.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use walrus::pg::walparser::{RmId, XLogRecord};

use crate::record::rmgr_label;
use crate::schema::FIRST_NORMAL_OBJECT_ID;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Class {
    /// Touches a catalog relfilenode; replay on shadow
    Catalog,
    /// User relfilenodes only; drop for shadow replay
    User,
    /// Recovery plumbing rmgr; keep
    Special,
    /// No block refs, non-special rmgr (e.g. some btree meta). A later
    /// pass inspects main_data; for now neither keep nor drop, just count.
    Empty,
}

pub fn rmgr_is_special(rm: u8) -> bool {
    matches!(
        rm,
        x if x == RmId::Xlog as u8
            || x == RmId::Xact as u8
            || x == RmId::Clog as u8
            || x == RmId::MultiXact as u8
            || x == RmId::Standby as u8
            || x == RmId::RelMap as u8
            || x == RmId::CommitTs as u8
            || x == RmId::ReplOrigin as u8
            || x == RmId::Dbase as u8
            || x == RmId::Tblspc as u8
            || x == RmId::Smgr as u8
    )
}

/// `XLOG_FPI_FOR_HINT` / `XLOG_FPI` info bytes (PG `access/xlog_internal.h`)
pub const XLOG_FPI_FOR_HINT: u8 = 0xA0;
pub const XLOG_FPI: u8 = 0xB0;

/// Xlog-rmgr record carrying a page image rather than recovery plumbing.
pub fn is_page_image(record: &XLogRecord) -> bool {
    record.header.resource_manager_id == RmId::Xlog as u8
        && matches!(record.header.info & 0xF0, XLOG_FPI | XLOG_FPI_FOR_HINT)
}

pub fn is_catalog_relnode(rel_node: u32) -> bool {
    rel_node != 0 && rel_node < FIRST_NORMAL_OBJECT_ID
}

pub fn classify(record: &XLogRecord) -> Class {
    if rmgr_is_special(record.header.resource_manager_id) && !is_page_image(record) {
        return Class::Special;
    }
    if record.blocks.is_empty() {
        return Class::Empty;
    }
    let mut saw_user = false;
    for blk in &record.blocks {
        let rel = blk.header.location.rel.rel_node;
        if is_catalog_relnode(rel) {
            return Class::Catalog;
        }
        if rel != 0 {
            saw_user = true;
        }
    }
    if saw_user { Class::User } else { Class::Empty }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Summary {
    pub records: u64,
    pub bytes: u64,
    pub by_class: BTreeMap<String, ClassCount>,
    pub by_rmgr: BTreeMap<String, RmgrCount>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ClassCount {
    pub records: u64,
    pub bytes: u64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RmgrCount {
    pub records: u64,
    pub bytes: u64,
    pub catalog: u64,
    pub user: u64,
    pub special: u64,
    pub empty: u64,
}

impl Summary {
    pub fn observe(&mut self, record: &XLogRecord) {
        let class = classify(record);
        let bytes = record.header.total_record_length as u64;
        self.records += 1;
        self.bytes += bytes;

        let class_key = match class {
            Class::Catalog => "catalog",
            Class::User => "user",
            Class::Special => "special",
            Class::Empty => "empty",
        };
        let cc = self.by_class.entry(class_key.into()).or_default();
        cc.records += 1;
        cc.bytes += bytes;

        let rmgr = rmgr_label(record.header.resource_manager_id);
        let rc = self.by_rmgr.entry(rmgr).or_default();
        rc.records += 1;
        rc.bytes += bytes;
        match class {
            Class::Catalog => rc.catalog += 1,
            Class::User => rc.user += 1,
            Class::Special => rc.special += 1,
            Class::Empty => rc.empty += 1,
        }
    }

    /// Catalog fraction by record count; zero on empty summary
    pub fn catalog_fraction(&self) -> f64 {
        if self.records == 0 {
            return 0.0;
        }
        let cat = self.by_class.get("catalog").map(|c| c.records).unwrap_or(0);
        cat as f64 / self.records as f64
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use walrus::pg::walparser::{
        BlockLocation, RelFileNode, XLogRecordBlock, XLogRecordBlockHeader, XLogRecordHeader,
    };

    fn record_with(rm: RmId, rel_nodes: &[u32]) -> XLogRecord<'static> {
        let blocks = rel_nodes
            .iter()
            .map(|&rn| XLogRecordBlock {
                header: XLogRecordBlockHeader {
                    location: BlockLocation {
                        rel: RelFileNode {
                            spc_node: 1663,
                            db_node: 5,
                            rel_node: rn,
                        },
                        block_no: 0,
                    },
                    ..Default::default()
                },
                ..Default::default()
            })
            .collect();
        XLogRecord {
            header: XLogRecordHeader {
                resource_manager_id: rm as u8,
                total_record_length: 64,
                ..Default::default()
            },
            blocks,
            ..Default::default()
        }
    }

    #[test]
    fn catalog_relnode_threshold() {
        assert!(is_catalog_relnode(1259)); // pg_class oid
        assert!(is_catalog_relnode(16383));
        assert!(!is_catalog_relnode(16384));
        assert!(!is_catalog_relnode(0)); // InvalidOid sentinel
    }

    #[test]
    fn heap_on_catalog_classifies_catalog() {
        let r = record_with(RmId::Heap, &[1259]);
        assert_eq!(classify(&r), Class::Catalog);
    }

    #[test]
    fn heap_on_user_relation_classifies_user() {
        let r = record_with(RmId::Heap, &[16500]);
        assert_eq!(classify(&r), Class::User);
    }

    #[test]
    fn mixed_blocks_with_any_catalog_keeps_catalog() {
        let r = record_with(RmId::Heap, &[16500, 1259]);
        assert_eq!(classify(&r), Class::Catalog);
    }

    #[test]
    fn xact_commit_classifies_special() {
        let r = record_with(RmId::Xact, &[]);
        assert_eq!(classify(&r), Class::Special);
    }

    #[test]
    fn xlog_record_classifies_special_even_with_blocks() {
        let r = record_with(RmId::Xlog, &[16500]);
        assert_eq!(classify(&r), Class::Special);
    }

    fn fpi_record(info: u8, rel_nodes: &[u32]) -> XLogRecord<'_> {
        let mut r = record_with(RmId::Xlog, rel_nodes);
        r.header.info = info;
        r
    }

    #[test]
    fn fpi_for_hint_on_user_relation_is_dropped() {
        let r = fpi_record(XLOG_FPI_FOR_HINT, &[16500]);
        assert!(is_page_image(&r));
        assert_eq!(classify(&r), Class::User);
    }

    #[test]
    fn fpi_on_user_relation_is_dropped() {
        assert_eq!(classify(&fpi_record(XLOG_FPI, &[16500])), Class::User);
    }

    #[test]
    fn fpi_on_catalog_relation_still_replays() {
        assert_eq!(classify(&fpi_record(XLOG_FPI, &[1259])), Class::Catalog);
        assert_eq!(
            classify(&fpi_record(XLOG_FPI_FOR_HINT, &[1259])),
            Class::Catalog
        );
    }

    #[test]
    fn non_image_xlog_records_stay_special() {
        for info in [0x00, 0x10, 0x40, 0x60, 0x90] {
            let r = fpi_record(info, &[16500]);
            assert!(!is_page_image(&r), "info {info:#04x}");
            assert_eq!(classify(&r), Class::Special, "info {info:#04x}");
        }
    }

    #[test]
    fn heap_no_blocks_classifies_empty() {
        let r = record_with(RmId::Heap, &[]);
        assert_eq!(classify(&r), Class::Empty);
    }

    #[test]
    fn summary_tracks_counts_and_fraction() {
        let mut s = Summary::default();
        s.observe(&record_with(RmId::Heap, &[1259]));
        s.observe(&record_with(RmId::Heap, &[16500]));
        s.observe(&record_with(RmId::Heap, &[16501]));
        s.observe(&record_with(RmId::Xact, &[]));
        assert_eq!(s.records, 4);
        assert!((s.catalog_fraction() - 0.25).abs() < 1e-9);
        assert_eq!(s.by_class["user"].records, 2);
        assert_eq!(s.by_class["special"].records, 1);
        assert_eq!(s.by_rmgr["heap"].catalog, 1);
        assert_eq!(s.by_rmgr["heap"].user, 2);
    }
}
