//! Reclassifier for records that carry the target relation in
//! `main_data` rather than a block reference.
//!
//! Phase 0 buckets these as `Class::Empty`. On real captures the only
//! observed Empty-class records are `XLOG_HEAP2_NEW_CID` (logical
//! decoding bookkeeping, ~0.3% of records in DDL-heavy workloads).
//! `xl_heap_new_cid.target_locator` holds the relfilenode whose tuple
//! was just touched, so the record is catalog-relevant exactly when
//! that locator is.
//!
//! Layout from PG's `src/include/access/heapam_xlog.h`:
//! ```text
//! struct xl_heap_new_cid {
//!     TransactionId top_xid;        // 4
//!     CommandId     cmin;           // 4
//!     CommandId     cmax;           // 4
//!     CommandId     combocid;       // 4
//!     RelFileLocator target_locator;// 12 (spcOid + dbOid + relNumber)
//!     ItemPointerData target_tid;   //  6 (block_hi+block_lo+posid)
//! }
//! ```
//! Total 34 bytes. Locator at byte offset 16.

use wal_rs::pg::walparser::{RelFileNode, RmId, XLogRecord};

/// `XLOG_HEAP2_NEW_CID` info byte (high nibble).
pub const XLOG_HEAP2_NEW_CID: u8 = 0x70;
const NEW_CID_LOCATOR_OFFSET: usize = 16;
const NEW_CID_MIN_SIZE: usize = 34;

/// Pull `RelFileLocator` out of an Empty-class record's main_data when
/// the rmgr+info pair is one we know. Returns `None` otherwise.
pub fn relation_for_empty(record: &XLogRecord) -> Option<RelFileNode> {
    if record.header.resource_manager_id != RmId::Heap2 as u8 {
        return None;
    }
    let info_high = record.header.info & 0xF0;
    if info_high != XLOG_HEAP2_NEW_CID {
        return None;
    }
    let md = &record.main_data;
    if md.len() < NEW_CID_MIN_SIZE {
        return None;
    }
    let off = NEW_CID_LOCATOR_OFFSET;
    Some(RelFileNode {
        spc_node: u32::from_le_bytes(md[off..off + 4].try_into().unwrap()),
        db_node: u32::from_le_bytes(md[off + 4..off + 8].try_into().unwrap()),
        rel_node: u32::from_le_bytes(md[off + 8..off + 12].try_into().unwrap()),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use wal_rs::pg::walparser::XLogRecordHeader;

    fn new_cid_record(spc: u32, db: u32, rel: u32) -> XLogRecord {
        let mut md = Vec::with_capacity(NEW_CID_MIN_SIZE);
        md.extend_from_slice(&100u32.to_le_bytes()); // top_xid
        md.extend_from_slice(&1u32.to_le_bytes()); // cmin
        md.extend_from_slice(&2u32.to_le_bytes()); // cmax
        md.extend_from_slice(&0u32.to_le_bytes()); // combocid
        md.extend_from_slice(&spc.to_le_bytes());
        md.extend_from_slice(&db.to_le_bytes());
        md.extend_from_slice(&rel.to_le_bytes());
        md.extend_from_slice(&[0u8; 6]); // target_tid
        XLogRecord {
            header: XLogRecordHeader {
                resource_manager_id: RmId::Heap2 as u8,
                info: XLOG_HEAP2_NEW_CID,
                ..Default::default()
            },
            main_data: md,
            ..Default::default()
        }
    }

    #[test]
    fn new_cid_locator_extracted() {
        let r = new_cid_record(1663, 5, 1259);
        let loc = relation_for_empty(&r).unwrap();
        assert_eq!(loc.rel_node, 1259);
        assert_eq!(loc.db_node, 5);
    }

    #[test]
    fn wrong_rmgr_returns_none() {
        let mut r = new_cid_record(1663, 5, 1259);
        r.header.resource_manager_id = RmId::Heap as u8;
        assert!(relation_for_empty(&r).is_none());
    }

    #[test]
    fn wrong_info_returns_none() {
        let mut r = new_cid_record(1663, 5, 1259);
        r.header.info = 0x10; // PRUNE
        assert!(relation_for_empty(&r).is_none());
    }

    #[test]
    fn truncated_main_data_returns_none() {
        let mut r = new_cid_record(1663, 5, 1259);
        r.main_data.truncate(8);
        assert!(relation_for_empty(&r).is_none());
    }
}
