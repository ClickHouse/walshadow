//! Per-record classification of WAL into catalog / user / special buckets.
//!
//! Catalog detection uses `rel_node < FirstNormalObjectId` (16384, see
//! `~/s/postgresql/src/include/access/transam.h`). This catches non-mapped
//! catalogs unconditionally + mapped catalogs (pg_class, pg_attribute,
//! pg_type, pg_proc, ...) only at their initial relfilenode. A subsequent
//! VACUUM FULL on a mapped catalog rewrites it to a relfilenode >= 16384;
//! Phase 1 will track RM_RELMAP_ID + heap writes to pg_class to keep the
//! mapped-catalog set current. Until then the heuristic stays good enough
//! for the Phase-0 goal: confirm catalog fraction is small.
//!
//! Special rmgrs (XLOG / XACT / CLOG / MULTIXACT / STANDBY / RELMAP /
//! COMMIT_TS / REPL_ORIGIN / DBASE / TBLSPC / SMGR) are always kept —
//! they are recovery plumbing shadow Postgres needs regardless of which
//! relations get replayed.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use wal_rs::pg::walparser::{RmId, XLogRecord};

/// pg src/include/access/transam.h FirstNormalObjectId.
pub const FIRST_NORMAL_OBJECT_ID: u32 = 16384;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Class {
    /// Touches a catalog relfilenode. Replay on shadow.
    Catalog,
    /// Touches only user relfilenodes. Drop for shadow replay.
    User,
    /// Recovery plumbing rmgr (xlog control, xact status, etc.). Keep.
    Special,
    /// No block refs, non-special rmgr (e.g. some btree meta records).
    /// Phase 1 will inspect main_data to bucket these; for now they
    /// neither keep nor drop, just count.
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

pub fn is_catalog_relnode(rel_node: u32) -> bool {
    rel_node != 0 && rel_node < FIRST_NORMAL_OBJECT_ID
}

pub fn classify(record: &XLogRecord) -> Class {
    if rmgr_is_special(record.header.resource_manager_id) {
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

/// Human-readable rmgr label for reports. Falls back to numeric id for
/// out-of-range values (forward compat with future PG majors).
pub fn rmgr_label(rm: u8) -> String {
    let named = match rm {
        x if x == RmId::Xlog as u8 => "xlog",
        x if x == RmId::Xact as u8 => "xact",
        x if x == RmId::Smgr as u8 => "smgr",
        x if x == RmId::Clog as u8 => "clog",
        x if x == RmId::Dbase as u8 => "dbase",
        x if x == RmId::Tblspc as u8 => "tblspc",
        x if x == RmId::MultiXact as u8 => "multixact",
        x if x == RmId::RelMap as u8 => "relmap",
        x if x == RmId::Standby as u8 => "standby",
        x if x == RmId::Heap2 as u8 => "heap2",
        x if x == RmId::Heap as u8 => "heap",
        x if x == RmId::Btree as u8 => "btree",
        x if x == RmId::Hash as u8 => "hash",
        x if x == RmId::Gin as u8 => "gin",
        x if x == RmId::Gist as u8 => "gist",
        x if x == RmId::Seq as u8 => "seq",
        x if x == RmId::Spgist as u8 => "spgist",
        x if x == RmId::Brin as u8 => "brin",
        x if x == RmId::CommitTs as u8 => "commit_ts",
        x if x == RmId::ReplOrigin as u8 => "repl_origin",
        x if x == RmId::Generic as u8 => "generic",
        x if x == RmId::LogicalMsg as u8 => "logical_msg",
        _ => return format!("rmgr_{rm}"),
    };
    named.into()
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

    /// Catalog fraction by record count. Zero on empty summary.
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
    use wal_rs::pg::walparser::{
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
        assert!(!is_catalog_relnode(0)); // skip InvalidOid sentinel
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
