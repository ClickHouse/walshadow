//! Reclassifier for records that carry the target relation in
//! `main_data` rather than a block reference.
//!
//! The per-record classifier buckets these as `Class::Empty`. Known reachable cases on
//! PG 15+ captures, cross-checked against
//! `~/s/postgresql/src/backend/access/`:
//!
//! | rmgr  | info               | source                                           | block ref? | reclassified |
//! |-------|--------------------|--------------------------------------------------|-----------|--------------|
//! | HEAP2 | NEW_CID (0x70)     | heapam.c `log_heap_new_cid`                      | none      | yes          |
//! | HEAP2 | VISIBLE (0x40)     | heapam.c `log_heap_visible`                      | vm+heap   | n/a          |
//! | HEAP2 | PRUNE_* (0x10-0x30)| pruneheap.c `log_heap_prune_and_freeze`          | heap buf  | n/a          |
//! | BTREE | VACUUM (0xC0)      | nbtpage.c `_bt_delitems_vacuum`                  | leaf buf  | n/a          |
//! | BTREE | REUSE_PAGE (0xD0)  | nbtpage.c `_bt_getbuf` (recyclable branch)       | **none**  | yes          |
//! | HEAP  | TRUNCATE (0x30)    | tablecmds.c (only when wal_level=logical)        | none      | no (oid arr) |
//!
//! `XLOG_HEAP_TRUNCATE` carries an array of oids (not relfilenodes)
//! and only fires under `wal_level=logical`. Walshadow targets
//! `wal_level=replica` for source PG, so the truncate record either
//! does not appear or — under logical — falls through to the
//! safe-default Keep with negligible cost.
//!
//! `XLOG_BTREE_REUSE_PAGE` exists solely to give hot-standby recovery a
//! conflict horizon (see PG nbtpage.c:933-953 comment). No block is
//! registered with the record, so it lands as Empty without
//! reclassification.
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
//!
//! Layout from PG's `src/include/access/nbtxlog.h`:
//! ```text
//! struct xl_btree_reuse_page {
//!     RelFileLocator    locator;                  // 12
//!     BlockNumber       block;                    //  4
//!     FullTransactionId snapshotConflictHorizon;  //  8
//!     bool              isCatalogRel;             //  1
//! }
//! ```
//! Total 25 bytes (no trailing padding once serialised by `XLogRegisterData`).
//! Locator at byte offset 0.

use wal_rs::pg::walparser::{RelFileNode, RmId, XLogRecord};

/// `XLOG_HEAP2_NEW_CID` info byte (high nibble).
pub const XLOG_HEAP2_NEW_CID: u8 = 0x70;
/// `XLOG_BTREE_REUSE_PAGE` info byte (high nibble).
pub const XLOG_BTREE_REUSE_PAGE: u8 = 0xD0;
const NEW_CID_LOCATOR_OFFSET: usize = 16;
const NEW_CID_MIN_SIZE: usize = 34;
const BTREE_REUSE_PAGE_MIN_SIZE: usize = 25;

/// `xl_heap_truncate` header size before the relids array. PG
/// `SizeOfHeapTruncate = offsetof(xl_heap_truncate, relids) = 12` — 9
/// bytes of dbId+nrelids+flags plus 3 bytes of trailing padding so
/// the relids array (4-byte aligned `Oid`) starts on a u32 boundary.
const HEAP_TRUNCATE_HEADER_SIZE: usize = 12;

/// PG `xl_heap_truncate.flags` bits (heapam_xlog.h).
pub const XLH_TRUNCATE_CASCADE: u8 = 1 << 0;
pub const XLH_TRUNCATE_RESTART_SEQS: u8 = 1 << 1;

/// Parsed body of an `XLOG_HEAP_TRUNCATE` main_data record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeapTruncate {
    pub db_oid: u32,
    pub flags: u8,
    /// pg_class OIDs of the relations being truncated. Note these are
    /// **OIDs**, not relfilenodes — TRUNCATE may rewrite the relation
    /// on commit (new filenode), so the WAL carries the stable id.
    pub relids: Vec<u32>,
}

/// Parse `xl_heap_truncate` main_data. Layout (PG `heapam_xlog.h`):
/// `Oid dbId; uint32 nrelids; uint8 flags; Oid relids[nrelids]`.
pub fn parse_xl_heap_truncate(md: &[u8]) -> Option<HeapTruncate> {
    if md.len() < HEAP_TRUNCATE_HEADER_SIZE {
        return None;
    }
    let db_oid = u32::from_le_bytes(md[0..4].try_into().unwrap());
    let nrelids = u32::from_le_bytes(md[4..8].try_into().unwrap()) as usize;
    let flags = md[8];
    let expected = HEAP_TRUNCATE_HEADER_SIZE + nrelids * 4;
    if md.len() < expected {
        return None;
    }
    let mut relids = Vec::with_capacity(nrelids);
    for i in 0..nrelids {
        let off = HEAP_TRUNCATE_HEADER_SIZE + i * 4;
        relids.push(u32::from_le_bytes(md[off..off + 4].try_into().unwrap()));
    }
    Some(HeapTruncate {
        db_oid,
        flags,
        relids,
    })
}

fn read_locator(md: &[u8], off: usize) -> RelFileNode {
    RelFileNode {
        spc_node: u32::from_le_bytes(md[off..off + 4].try_into().unwrap()),
        db_node: u32::from_le_bytes(md[off + 4..off + 8].try_into().unwrap()),
        rel_node: u32::from_le_bytes(md[off + 8..off + 12].try_into().unwrap()),
    }
}

/// Pull `RelFileLocator` out of an Empty-class record's main_data when
/// the rmgr+info pair is one we know. Returns `None` otherwise.
pub fn relation_for_empty(record: &XLogRecord) -> Option<RelFileNode> {
    let rmid = record.header.resource_manager_id;
    let info_high = record.header.info & 0xF0;
    let md = &record.main_data;
    if rmid == RmId::Heap2 as u8 && info_high == XLOG_HEAP2_NEW_CID {
        if md.len() < NEW_CID_MIN_SIZE {
            return None;
        }
        return Some(read_locator(md, NEW_CID_LOCATOR_OFFSET));
    }
    if rmid == RmId::Btree as u8 && info_high == XLOG_BTREE_REUSE_PAGE {
        if md.len() < BTREE_REUSE_PAGE_MIN_SIZE {
            return None;
        }
        return Some(read_locator(md, 0));
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use wal_rs::pg::walparser::XLogRecordHeader;

    fn new_cid_record(spc: u32, db: u32, rel: u32) -> XLogRecord<'static> {
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
            main_data: std::borrow::Cow::Owned(md),
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
        r.main_data.to_mut().truncate(8);
        assert!(relation_for_empty(&r).is_none());
    }

    fn btree_reuse_record(spc: u32, db: u32, rel: u32) -> XLogRecord<'static> {
        let mut md = Vec::with_capacity(BTREE_REUSE_PAGE_MIN_SIZE);
        md.extend_from_slice(&spc.to_le_bytes());
        md.extend_from_slice(&db.to_le_bytes());
        md.extend_from_slice(&rel.to_le_bytes());
        md.extend_from_slice(&0u32.to_le_bytes()); // block
        md.extend_from_slice(&0u64.to_le_bytes()); // snapshotConflictHorizon
        md.push(0); // isCatalogRel
        XLogRecord {
            header: XLogRecordHeader {
                resource_manager_id: RmId::Btree as u8,
                info: XLOG_BTREE_REUSE_PAGE,
                ..Default::default()
            },
            main_data: std::borrow::Cow::Owned(md),
            ..Default::default()
        }
    }

    #[test]
    fn btree_reuse_page_locator_extracted() {
        let r = btree_reuse_record(1663, 5, 1259);
        let loc = relation_for_empty(&r).unwrap();
        assert_eq!(loc.spc_node, 1663);
        assert_eq!(loc.db_node, 5);
        assert_eq!(loc.rel_node, 1259);
    }

    #[test]
    fn btree_reuse_truncated_returns_none() {
        let mut r = btree_reuse_record(1663, 5, 1259);
        r.main_data.to_mut().truncate(4);
        assert!(relation_for_empty(&r).is_none());
    }

    #[test]
    fn btree_non_reuse_info_returns_none() {
        let mut r = btree_reuse_record(1663, 5, 1259);
        r.header.info = 0xC0; // XLOG_BTREE_VACUUM — block-ref bearing, not Empty
        assert!(relation_for_empty(&r).is_none());
    }

    /// PG `XLogRegisterData(&xlrec, SizeOfHeapTruncate)` writes the C
    /// struct including its trailing 3-byte padding so the following
    /// relids array lands on a 4-byte boundary. Tests mirror the same
    /// layout.
    fn write_truncate_header(md: &mut Vec<u8>, db_oid: u32, nrelids: u32, flags: u8) {
        md.extend_from_slice(&db_oid.to_le_bytes());
        md.extend_from_slice(&nrelids.to_le_bytes());
        md.push(flags);
        md.extend_from_slice(&[0u8; 3]); // alignment padding
    }

    #[test]
    fn parse_xl_heap_truncate_extracts_relids() {
        let mut md = Vec::new();
        write_truncate_header(&mut md, 5, 3, XLH_TRUNCATE_CASCADE);
        for oid in [16400u32, 16401, 16402] {
            md.extend_from_slice(&oid.to_le_bytes());
        }
        let r = parse_xl_heap_truncate(&md).expect("parse");
        assert_eq!(r.db_oid, 5);
        assert_eq!(r.flags, XLH_TRUNCATE_CASCADE);
        assert_eq!(r.relids, vec![16400, 16401, 16402]);
    }

    #[test]
    fn parse_xl_heap_truncate_truncated_returns_none() {
        let mut md = Vec::new();
        write_truncate_header(&mut md, 5, 2, 0); // claims 2 relids
        md.extend_from_slice(&16400u32.to_le_bytes()); // only one
        assert!(parse_xl_heap_truncate(&md).is_none());
    }

    #[test]
    fn parse_xl_heap_truncate_zero_relids_ok() {
        let mut md = Vec::new();
        write_truncate_header(&mut md, 7, 0, 0);
        let r = parse_xl_heap_truncate(&md).expect("parse");
        assert_eq!(r.db_oid, 7);
        assert!(r.relids.is_empty());
    }
}
