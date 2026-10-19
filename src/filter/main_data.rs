//! Reclassifier for records carrying the target relation in `main_data`
//! rather than a block reference; the per-record classifier buckets these
//! as `Class::Empty`.
//!
//! Known reachable cases on PG 15+, cross-checked against
//! PG `src/backend/access/`:
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
//! `XLOG_HEAP_TRUNCATE` carries oids (not relfilenodes) and fires only
//! under `wal_level=logical`. Walshadow targets `wal_level=replica`, so it
//! either never appears or falls through to safe-default Keep.
//!
//! `XLOG_BTREE_REUSE_PAGE` exists only to give hot-standby recovery a
//! conflict horizon (PG nbtpage.c). No block registered, so it lands Empty.
//!
//! `xl_heap_new_cid` (`src/include/access/heapam_xlog.h`):
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
//! 34 bytes, locator at offset 16.
//!
//! `xl_btree_reuse_page` (`src/include/access/nbtxlog.h`):
//! ```text
//! struct xl_btree_reuse_page {
//!     RelFileLocator    locator;                  // 12
//!     BlockNumber       block;                    //  4
//!     FullTransactionId snapshotConflictHorizon;  //  8
//!     bool              isCatalogRel;             //  1
//! }
//! ```
//! 25 bytes (no trailing pad after `XLogRegisterData`), locator at offset 0.

use walrus::pg::walparser::{RelFileNode, RmId, XLogRecord};

use crate::cursor::Cursor;

/// `XLOG_HEAP2_NEW_CID` info byte
pub const XLOG_HEAP2_NEW_CID: u8 = 0x70;
/// `XLOG_BTREE_REUSE_PAGE` info byte
pub const XLOG_BTREE_REUSE_PAGE: u8 = 0xD0;
const NEW_CID_LOCATOR_OFFSET: usize = 16;
const NEW_CID_MIN_SIZE: usize = 34;
const BTREE_REUSE_PAGE_MIN_SIZE: usize = 25;

/// `xl_heap_truncate.flags` bits (heapam_xlog.h)
pub const XLH_TRUNCATE_CASCADE: u8 = 1 << 0;
pub const XLH_TRUNCATE_RESTART_SEQS: u8 = 1 << 1;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeapTruncate {
    pub db_oid: u32,
    pub flags: u8,
    /// OIDs, not relfilenodes: TRUNCATE may rewrite to a new filenode on
    /// commit, so WAL carries the stable id
    pub relids: Vec<u32>,
}

/// Layout (PG `heapam_xlog.h`):
/// `Oid dbId; uint32 nrelids; uint8 flags; Oid relids[nrelids]`
pub fn parse_xl_heap_truncate(mut md: &[u8]) -> Option<HeapTruncate> {
    let db_oid = md.u32()?;
    let nrelids = md.u32()?;
    let flags = md.u8()?;
    md.skip(3)?; // pad so 4-byte relids align, part of `SizeOfHeapTruncate`
    let relids = (0..nrelids).map(|_| md.u32()).collect::<Option<_>>()?;
    Some(HeapTruncate {
        db_oid,
        flags,
        relids,
    })
}

/// Info byte for running-xacts records
pub const XLOG_RUNNING_XACTS: u8 = 0x10;

/// Offset of next xid in running-xacts payload
const RUNNING_XACTS_NEXT_XID_OFFSET: usize = 12;
/// Minimum running-xacts payload size
const MIN_SIZE_OF_XACT_RUNNING_XACTS: usize = 24;

/// Read next xid from a running-xacts payload
pub fn parse_running_xacts_next_xid(md: &[u8]) -> Option<u32> {
    if md.len() < MIN_SIZE_OF_XACT_RUNNING_XACTS {
        return None;
    }
    let at = RUNNING_XACTS_NEXT_XID_OFFSET;
    Some(u32::from_le_bytes(md[at..at + 4].try_into().unwrap()))
}

/// `xl_relmap_update` header (PG `src/include/utils/relmapper.h`):
/// `Oid dbid; Oid tsid; int32 nbytes; char data[FLEXIBLE_ARRAY_MEMBER]`.
/// `dbid == 0` is the shared map (`global/pg_filenode.map`)
pub const XL_RELMAP_UPDATE_HEADER_SIZE: usize = 12;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RelmapUpdate {
    pub dbid: u32,
    pub tsid: u32,
    /// Declared `RelMapFile` body length; the body follows the header
    pub nbytes: usize,
}

pub fn parse_xl_relmap_update(mut md: &[u8]) -> Option<RelmapUpdate> {
    Some(RelmapUpdate {
        dbid: md.u32()?,
        tsid: md.u32()?,
        nbytes: usize::try_from(md.i32()?).ok()?,
    })
}

/// `XLOG_SMGR_CREATE` info byte (PG `catalog/storage_xlog.h`)
pub const XLOG_SMGR_CREATE: u8 = 0x10;
/// PG `MAIN_FORKNUM`
pub const MAIN_FORKNUM: i32 = 0;

/// Layout (PG `catalog/storage_xlog.h`):
/// `xl_smgr_create { RelFileLocator rlocator; ForkNumber forknum }`
pub fn parse_xl_smgr_create(mut md: &[u8]) -> Option<(RelFileNode, i32)> {
    Some((md.rel_file_node()?, md.i32()?))
}

/// Pull `RelFileLocator` from an Empty-class record's main_data for known
/// rmgr+info pairs; `None` otherwise.
pub fn relation_for_empty(record: &XLogRecord) -> Option<RelFileNode> {
    let rmid = record.header.resource_manager_id;
    let info_high = record.header.info & 0xF0;
    let mut md: &[u8] = &record.main_data;
    if rmid == RmId::Heap2 as u8 && info_high == XLOG_HEAP2_NEW_CID {
        if md.len() < NEW_CID_MIN_SIZE {
            return None;
        }
        return md.get(NEW_CID_LOCATOR_OFFSET..)?.rel_file_node();
    }
    if rmid == RmId::Btree as u8 && info_high == XLOG_BTREE_REUSE_PAGE {
        if md.len() < BTREE_REUSE_PAGE_MIN_SIZE {
            return None;
        }
        return md.rel_file_node();
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use walrus::pg::walparser::XLogRecordHeader;

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
        r.header.info = 0x10; // HEAP2 PRUNE
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
        r.header.info = 0xC0; // XLOG_BTREE_VACUUM, block-ref bearing not Empty
        assert!(relation_for_empty(&r).is_none());
    }

    /// PG `XLogRegisterData(&xlrec, SizeOfHeapTruncate)` includes the
    /// trailing 3-byte pad so relids lands 4-byte aligned
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
        assert_eq!(r.relids, [16400, 16401, 16402]);
    }

    #[test]
    fn parse_xl_heap_truncate_truncated_returns_none() {
        let mut md = Vec::new();
        write_truncate_header(&mut md, 5, 2, 0); // claims 2
        md.extend_from_slice(&16400u32.to_le_bytes()); // supplies 1
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
