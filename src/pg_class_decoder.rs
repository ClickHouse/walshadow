//! Narrow heap-tuple decoder for `pg_class` block data.
//!
//! Extracts `(oid, relfilenode)` from a WAL heap block targeting
//! pg_class so [`CatalogTracker`](crate::catalog_tracker::CatalogTracker)
//! can track non-mapped-catalog filenode rewrites
//! (`VACUUM FULL` / `REINDEX` / `CLUSTER` on `pg_depend`, `pg_namespace`,
//! `pg_constraint`, ...). Mapped catalogs (`pg_class`, `pg_attribute`,
//! `pg_type`, `pg_proc`, shared catalogs) are handled separately via
//! `XLOG_RELMAP_UPDATE`.
//!
//! ## Block-data layout for new-tuple records
//!
//! For `XLOG_HEAP_INSERT` / `XLOG_HEAP_UPDATE` / `XLOG_HEAP_HOT_UPDATE`,
//! PG's `heap_xlog_insert` / `_update` (`src/backend/access/heap/heapam.c`)
//! writes block-0 data as:
//!
//! ```text
//! +--- xl_heap_header (5 bytes) ---+
//! | t_infomask2 | t_infomask | hoff|
//! +--------------------------------+
//! | payload (bytes from offset    |
//! |   SizeofHeapTupleHeader=23 of |
//! |   the reconstructed tuple)    |
//! +-------------------------------+
//! ```
//!
//! Recovery code reconstructs the full tuple by zeroing a 23-byte
//! `HeapTupleHeaderData`, patching `t_infomask2 / t_infomask / t_hoff`
//! from the WAL header, then copying payload to offset 23 of the
//! reconstructed buffer. Column data begins at offset `t_hoff` of the
//! reconstructed tuple, i.e. at byte `t_hoff - 18` of `block.data`
//! (5 header bytes + (t_hoff - 23) into payload).
//!
//! `XLOG_HEAP_INPLACE` writes a different shape (full HeapTupleHeader +
//! tuple); skipped via [`info_carries_new_tuple_heap`]. `XLOG_HEAP2_*`
//! info codes (MULTI_INSERT, NEW_CID, ...) are likewise skipped — pg_class
//! is single-row-INSERT and UPDATE territory.
//!
//! ## pg_class column offsets
//!
//! PG ≥ 16 pg_class column layout (`src/include/catalog/pg_class.h`,
//! stable across PG 16/17/18):
//!
//! | col | name | type | width |
//! |-----|------|------|------|
//! | 1 | oid | oid | 4 |
//! | 2 | relname | name | 64 (NAMEDATALEN) |
//! | 3 | relnamespace | oid | 4 |
//! | 4 | reltype | oid | 4 |
//! | 5 | reloftype | oid | 4 |
//! | 6 | relowner | oid | 4 |
//! | 7 | relam | oid | 4 |
//! | 8 | relfilenode | oid | 4 |
//!
//! Decoder reads col 1 and col 8 only. Columns 1–8 are NOT NULL by
//! catalog schema, so a null bitmap (if HEAP_HASNULL is set for later
//! nullable columns like relacl / reloptions) doesn't shift these.
//! `t_hoff` already accounts for the bitmap + alignment.

/// `sizeof(xl_heap_header)` from PG `heapam_xlog.h`.
const XL_HEAP_HEADER_SIZE: usize = 5;
/// `SizeofHeapTupleHeader` — PG stable at 23 bytes since PG 7.x.
const SIZE_OF_HEAP_TUPLE_HEADER: usize = 23;
/// pg_class col 1 (oid) offset within the column-data region.
const PG_CLASS_OID_OFFSET: usize = 0;
/// pg_class col 8 (relfilenode) offset within the column-data region.
/// Sum of column widths 1..=7: 4 + 64 + 4*5 = 88.
const PG_CLASS_RELFILENODE_OFFSET: usize = 88;
/// `XLOG_HEAP_OPMASK` — masks out `XLOG_HEAP_INIT_PAGE` (0x80) so info
/// values compare cleanly against the canonical op codes.
const HEAP_OPMASK: u8 = 0x70;

/// pg_class info values that carry block-0 data as
/// `xl_heap_header + payload`. INPLACE / DELETE / LOCK / CONFIRM /
/// TRUNCATE do not.
const HEAP_INFO_NEW_TUPLE_OPS: &[u8] = &[0x00, 0x20, 0x40]; // INSERT, UPDATE, HOT_UPDATE

/// Extracted slice of a pg_class row that the catalog tracker needs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PgClassRow {
    pub oid: u32,
    pub relfilenode: u32,
}

/// Decode a pg_class heap-write WAL block. Returns `None` when the data
/// is too short to be a pg_class tuple's xl_heap_header + payload of
/// the needed columns. Caller is responsible for filtering on rmgr +
/// info via [`info_carries_new_tuple_heap`] /
/// [`info_carries_new_tuple_heap2`] first.
pub fn decode_pg_class_tuple(block_data: &[u8]) -> Option<PgClassRow> {
    if block_data.len() < XL_HEAP_HEADER_SIZE {
        return None;
    }
    let t_hoff = block_data[XL_HEAP_HEADER_SIZE - 1] as usize;
    if t_hoff < SIZE_OF_HEAP_TUPLE_HEADER {
        return None;
    }
    // Offset of column data within block_data:
    //   reconstructed tuple offset t_hoff
    // = XL_HEAP_HEADER_SIZE + (t_hoff - SIZE_OF_HEAP_TUPLE_HEADER)
    let cds = XL_HEAP_HEADER_SIZE + (t_hoff - SIZE_OF_HEAP_TUPLE_HEADER);
    let oid_off = cds + PG_CLASS_OID_OFFSET;
    let rfn_off = cds + PG_CLASS_RELFILENODE_OFFSET;
    if block_data.len() < rfn_off + 4 {
        return None;
    }
    let oid = u32::from_le_bytes(block_data[oid_off..oid_off + 4].try_into().ok()?);
    let relfilenode = u32::from_le_bytes(block_data[rfn_off..rfn_off + 4].try_into().ok()?);
    Some(PgClassRow { oid, relfilenode })
}

/// True iff `RM_HEAP` `info` (the full byte, including `XLOG_HEAP_INIT_PAGE`
/// flag) names an operation whose block-0 data is shaped like
/// `xl_heap_header + payload`. Caller masks with HEAP_OPMASK internally
/// so init-page flag has no effect.
pub fn info_carries_new_tuple_heap(info: u8) -> bool {
    HEAP_INFO_NEW_TUPLE_OPS.contains(&(info & HEAP_OPMASK))
}

/// `RM_HEAP2` has no info code with the xl_heap_header + payload shape
/// — MULTI_INSERT uses xl_multi_insert_tuple per row, NEW_CID is
/// metadata-only, VISIBLE / LOCK_UPDATED / PRUNE_* don't carry tuples
/// at all. Reserved for future expansion; today returns `false`
/// unconditionally so the tracker doesn't try to decode HEAP2 block
/// data as pg_class tuples.
pub fn info_carries_new_tuple_heap2(_info: u8) -> bool {
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build block-data shaped like `xl_heap_header + payload` for a
    /// pg_class row with the given oid and relfilenode. No nulls,
    /// t_hoff = 24 (minimal MAXALIGN'd HeapTupleHeader).
    fn pg_class_block_data(oid: u32, relfilenode: u32) -> Vec<u8> {
        let mut v = Vec::new();
        v.extend_from_slice(&33u16.to_le_bytes()); // t_infomask2
        v.extend_from_slice(&0u16.to_le_bytes()); // t_infomask (no nulls)
        v.push(24); // t_hoff
        // 1 byte padding (offset 23 of reconstructed tuple)
        v.push(0);
        // col 1: oid
        v.extend_from_slice(&oid.to_le_bytes());
        // col 2: relname (64 bytes, name type)
        v.extend_from_slice(&[0u8; 64]);
        // cols 3-7: 4 bytes each, 20 bytes total
        v.extend_from_slice(&[0u8; 20]);
        // col 8: relfilenode
        v.extend_from_slice(&relfilenode.to_le_bytes());
        v
    }

    #[test]
    fn decodes_minimal_pg_class_tuple() {
        let data = pg_class_block_data(2615, 30000);
        let row = decode_pg_class_tuple(&data).unwrap();
        assert_eq!(row.oid, 2615);
        assert_eq!(row.relfilenode, 30000);
    }

    #[test]
    fn decodes_with_null_bitmap_present_in_t_hoff() {
        // pg_class has nullable columns at the tail (relacl, reloptions,
        // relpartbound). With HEAP_HASNULL set, t_hoff bumps up by the
        // bitmap size + MAXALIGN. With 33 attributes, bitmap = 5 bytes,
        // t_hoff = MAXALIGN(23 + 5) = 32.
        let t_hoff: u8 = 32;
        let mut v = Vec::new();
        v.extend_from_slice(&33u16.to_le_bytes()); // t_infomask2
        v.extend_from_slice(&1u16.to_le_bytes()); // t_infomask = HEAP_HASNULL
        v.push(t_hoff);
        // Payload starts at offset 5 of block_data = offset 23 of
        // reconstructed tuple. Bytes 23..32 of reconstructed tuple are
        // null bitmap (5) + padding (4) — 9 bytes here.
        v.extend_from_slice(&[0xff; 9]); // 5 bitmap + 4 padding bytes
        // Column data starts at offset 32 of tuple = offset 14 of
        // block_data.
        v.extend_from_slice(&1234u32.to_le_bytes()); // oid
        v.extend_from_slice(&[0u8; 64]); // relname
        v.extend_from_slice(&[0u8; 20]); // cols 3-7
        v.extend_from_slice(&77777u32.to_le_bytes()); // relfilenode
        let row = decode_pg_class_tuple(&v).unwrap();
        assert_eq!(row.oid, 1234);
        assert_eq!(row.relfilenode, 77777);
    }

    #[test]
    fn rejects_truncated_block_data() {
        assert!(decode_pg_class_tuple(&[]).is_none());
        assert!(decode_pg_class_tuple(&[0u8; 4]).is_none());
        // Header present but payload truncated before col 8.
        let mut v = Vec::new();
        v.extend_from_slice(&33u16.to_le_bytes());
        v.extend_from_slice(&0u16.to_le_bytes());
        v.push(24);
        v.extend_from_slice(&[0u8; 10]);
        assert!(decode_pg_class_tuple(&v).is_none());
    }

    #[test]
    fn rejects_invalid_t_hoff() {
        // t_hoff < SizeofHeapTupleHeader is malformed.
        let mut v = Vec::new();
        v.extend_from_slice(&33u16.to_le_bytes());
        v.extend_from_slice(&0u16.to_le_bytes());
        v.push(16); // < 23
        v.extend_from_slice(&[0u8; 200]);
        assert!(decode_pg_class_tuple(&v).is_none());
    }

    #[test]
    fn info_filter_heap() {
        assert!(info_carries_new_tuple_heap(0x00)); // INSERT
        assert!(info_carries_new_tuple_heap(0x20)); // UPDATE
        assert!(info_carries_new_tuple_heap(0x40)); // HOT_UPDATE
        // Init-page bit set together with INSERT must still match.
        assert!(info_carries_new_tuple_heap(0x80));
        assert!(info_carries_new_tuple_heap(0xA0)); // INIT_PAGE | UPDATE
        assert!(!info_carries_new_tuple_heap(0x10)); // DELETE
        assert!(!info_carries_new_tuple_heap(0x30)); // TRUNCATE
        assert!(!info_carries_new_tuple_heap(0x60)); // LOCK
        assert!(!info_carries_new_tuple_heap(0x70)); // INPLACE
    }

    #[test]
    fn info_filter_heap2_returns_false() {
        for op in 0..=0x70u8 {
            assert!(!info_carries_new_tuple_heap2(op));
        }
    }
}
