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
//! ## Block-data layout
//!
//! For `XLOG_HEAP_INSERT` (info 0x00), block 0 carries
//! `xl_heap_header + payload`:
//!
//! ```text
//! +--- xl_heap_header (5 bytes) ---+
//! | t_infomask2 | t_infomask | hoff|
//! +--------------------------------+
//! | bitmap [+ pad] [+ oid] +       |
//! |   column data from offset 23   |
//! |   of reconstructed tuple       |
//! +--------------------------------+
//! ```
//!
//! Recovery zeroes a 23-byte `HeapTupleHeaderData`, patches
//! `t_infomask2 / t_infomask / t_hoff` from the WAL header, then copies
//! payload to offset 23 of the reconstructed buffer. Column data begins
//! at reconstructed tuple offset `t_hoff`.
//!
//! For `XLOG_HEAP_UPDATE` / `XLOG_HEAP_HOT_UPDATE` (info 0x20 / 0x40),
//! PG's `heap_update` (`src/backend/access/heap/heapam.c`) compresses
//! away byte prefixes/suffixes shared with the old tuple. Block 0 is:
//!
//! ```text
//! [prefixlen u16 if XLH_UPDATE_PREFIX_FROM_OLD]
//! [suffixlen u16 if XLH_UPDATE_SUFFIX_FROM_OLD]
//! [xl_heap_header (5 bytes)]
//! [bitmap + padding (t_hoff - 23 bytes)]
//! [column data starting at reconstructed offset t_hoff + prefixlen,
//!  ending at reconstructed offset t_len - suffixlen]
//! ```
//!
//! `xl_heap_update.flags` lives in `record.main_data`, byte offset 7
//! (`SizeOfHeapUpdate = 14` on disk; in-memory sizeof is 16 due to C
//! struct trailing pad which PG strips via `XLogRegisterData`).
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
//!
//! ## VACUUM FULL on non-mapped catalogs
//!
//! `VACUUM FULL pg_depend` (etc) issues `heap_update` against pg_class
//! to change just `relfilenode`. Columns 1–7 occupy 88 bytes, all
//! unchanged, so `prefixlen ≈ 88` and the OID column lives entirely in
//! the prefix region — the WAL record alone cannot reconstruct it.
//! These records surface as [`DecodeOutcome::OidInPrefix`]; the caller
//! must learn the rotated filenode through another channel (subsequent
//! `XLOG_RELMAP_UPDATE` for any shared/mapped catalog touch, or the
//! `seed_from_source` snapshot taken at attach time).

use wal_rs::pg::walparser::XLogRecord;

/// `sizeof(xl_heap_header)` from PG `heapam_xlog.h`.
const XL_HEAP_HEADER_SIZE: usize = 5;
/// `SizeofHeapTupleHeader` — PG stable at 23 bytes since PG 7.x.
const SIZE_OF_HEAP_TUPLE_HEADER: usize = 23;
/// `SizeOfHeapUpdate` from PG `heapam_xlog.h`: on-disk size of the
/// `xl_heap_update` struct that lives in `main_data` for HEAP_UPDATE /
/// HEAP_HOT_UPDATE records. C-struct `sizeof` is 16 (trailing pad);
/// PG's `XLogRegisterData(&xlrec, SizeOfHeapUpdate)` strips that pad.
const SIZE_OF_HEAP_UPDATE: usize = 14;
/// Byte offset of `xl_heap_update.flags` within the struct (after
/// `old_xmax`(4) + `old_offnum`(2) + `old_infobits_set`(1)).
const XL_HEAP_UPDATE_FLAGS_OFFSET: usize = 7;
/// pg_class col 1 (oid) offset within the column-data region.
const PG_CLASS_OID_OFFSET: usize = 0;
/// pg_class col 8 (relfilenode) offset within the column-data region.
/// Sum of column widths 1..=7: 4 + 64 + 4*5 = 88.
const PG_CLASS_RELFILENODE_OFFSET: usize = 88;
/// `XLOG_HEAP_OPMASK` — masks out `XLOG_HEAP_INIT_PAGE` (0x80) so info
/// values compare cleanly against the canonical op codes.
const HEAP_OPMASK: u8 = 0x70;

const HEAP_INSERT_OP: u8 = 0x00;
const HEAP_UPDATE_OP: u8 = 0x20;
const HEAP_HOT_UPDATE_OP: u8 = 0x40;

/// `XLH_UPDATE_PREFIX_FROM_OLD` from PG `heapam_xlog.h`.
const XLH_UPDATE_PREFIX_FROM_OLD: u8 = 1 << 5;
/// `XLH_UPDATE_SUFFIX_FROM_OLD`.
const XLH_UPDATE_SUFFIX_FROM_OLD: u8 = 1 << 6;

/// pg_class info values that carry block-0 data as
/// `xl_heap_header + payload`. INPLACE / DELETE / LOCK / CONFIRM /
/// TRUNCATE do not.
const HEAP_INFO_NEW_TUPLE_OPS: &[u8] = &[HEAP_INSERT_OP, HEAP_UPDATE_OP, HEAP_HOT_UPDATE_OP];

/// Extracted slice of a pg_class row that the catalog tracker needs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PgClassRow {
    pub oid: u32,
    pub relfilenode: u32,
}

/// Outcome of decoding a pg_class heap-write block.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DecodeOutcome {
    /// `(oid, relfilenode)` extracted cleanly.
    Decoded(PgClassRow),
    /// Update record with `XLH_UPDATE_PREFIX_FROM_OLD` set and
    /// `prefixlen > 0`. OID lives entirely (or partially, when
    /// `prefixlen < 4`) inside the compressed prefix that the WAL does
    /// not carry; reconstructing it would require the old tuple. Caller
    /// counts these but cannot derive the catalog filenode from the
    /// record alone — must rely on `XLOG_RELMAP_UPDATE` or
    /// `seed_from_source` to track the rotation.
    OidInPrefix,
    /// Block data too short or malformed for the expected shape.
    Undecoded,
}

/// Decode the pg_class heap-write block at `block_idx` of `record`.
/// Caller is responsible for filtering on rmgr + info via
/// [`info_carries_new_tuple_heap`] / [`info_carries_new_tuple_heap2`]
/// first.
pub fn decode_pg_class_tuple(record: &XLogRecord, block_idx: usize) -> DecodeOutcome {
    let Some(block) = record.blocks.get(block_idx) else {
        return DecodeOutcome::Undecoded;
    };
    let info_high = record.header.info & HEAP_OPMASK;
    let (has_prefix, has_suffix) = match info_high {
        HEAP_INSERT_OP => (false, false),
        HEAP_UPDATE_OP | HEAP_HOT_UPDATE_OP => {
            if record.main_data.len() < SIZE_OF_HEAP_UPDATE {
                return DecodeOutcome::Undecoded;
            }
            let flags = record.main_data[XL_HEAP_UPDATE_FLAGS_OFFSET];
            (
                flags & XLH_UPDATE_PREFIX_FROM_OLD != 0,
                flags & XLH_UPDATE_SUFFIX_FROM_OLD != 0,
            )
        }
        _ => return DecodeOutcome::Undecoded,
    };

    let data = &block.data;
    let prefix_bytes = if has_prefix { 2 } else { 0 };
    let suffix_bytes = if has_suffix { 2 } else { 0 };
    let skip = prefix_bytes + suffix_bytes;
    if data.len() < skip + XL_HEAP_HEADER_SIZE {
        return DecodeOutcome::Undecoded;
    }

    let prefixlen = if has_prefix {
        u16::from_le_bytes(data[0..2].try_into().unwrap()) as usize
    } else {
        0
    };
    // suffixlen read for symmetry / future use; relfilenode at offset 88
    // is well below any plausible pg_class suffix so we do not gate on it.
    let _suffixlen = if has_suffix {
        u16::from_le_bytes(data[prefix_bytes..prefix_bytes + 2].try_into().unwrap()) as usize
    } else {
        0
    };

    if prefixlen > 0 {
        // OID is at reconstructed tuple offset t_hoff + 0. Block data
        // carries reconstructed offsets >= t_hoff + prefixlen, so any
        // prefixlen > 0 leaves the OID's 4 bytes (fully or partially)
        // in the un-logged prefix region.
        return DecodeOutcome::OidInPrefix;
    }

    let header_off = skip;
    let t_hoff = data[header_off + XL_HEAP_HEADER_SIZE - 1] as usize;
    if t_hoff < SIZE_OF_HEAP_TUPLE_HEADER {
        return DecodeOutcome::Undecoded;
    }
    // Column data offset within block.data:
    //   reconstructed tuple offset t_hoff
    // = header_off + XL_HEAP_HEADER_SIZE + (t_hoff - SIZE_OF_HEAP_TUPLE_HEADER)
    let cds = header_off + XL_HEAP_HEADER_SIZE + (t_hoff - SIZE_OF_HEAP_TUPLE_HEADER);
    let oid_off = cds + PG_CLASS_OID_OFFSET;
    let rfn_off = cds + PG_CLASS_RELFILENODE_OFFSET;
    if data.len() < rfn_off + 4 {
        return DecodeOutcome::Undecoded;
    }
    let oid = u32::from_le_bytes(data[oid_off..oid_off + 4].try_into().unwrap());
    let relfilenode = u32::from_le_bytes(data[rfn_off..rfn_off + 4].try_into().unwrap());
    DecodeOutcome::Decoded(PgClassRow { oid, relfilenode })
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
    use wal_rs::pg::walparser::{
        BlockLocation, RelFileNode, RmId, XLogRecordBlock, XLogRecordBlockHeader, XLogRecordHeader,
    };

    /// Reconstructed pg_class row (no nulls, t_hoff=24, MAXALIGN'd to
    /// SizeofHeapTupleHeader). Returns the bytes from offset 23
    /// onwards: bitmap/padding (1 byte) + 8 columns. 116 bytes total
    /// when `extra_cols == 0`. `extra_cols` adds trailing bytes so
    /// suffix tests have something to chop off.
    fn pg_class_tuple_tail(oid: u32, relfilenode: u32, extra_cols: usize) -> Vec<u8> {
        let mut v = Vec::new();
        // 1 byte MAXALIGN padding (reconstructed offset 23 -> 24).
        v.push(0);
        // cols 1..8
        v.extend_from_slice(&oid.to_le_bytes());
        v.extend_from_slice(&[0u8; 64]); // relname
        v.extend_from_slice(&0u32.to_le_bytes()); // relnamespace
        v.extend_from_slice(&0u32.to_le_bytes()); // reltype
        v.extend_from_slice(&0u32.to_le_bytes()); // reloftype
        v.extend_from_slice(&0u32.to_le_bytes()); // relowner
        v.extend_from_slice(&0u32.to_le_bytes()); // relam
        v.extend_from_slice(&relfilenode.to_le_bytes());
        // Filler for trailing columns (cols 9+; their content doesn't
        // matter, only their presence so suffix compression has bytes
        // to chop).
        v.extend(std::iter::repeat_n(0u8, extra_cols));
        v
    }

    /// Build block data for a HEAP_INSERT carrying `(oid, relfilenode)`.
    /// Shape: xl_heap_header (5 bytes) + bitmap/pad/col data (full).
    fn pg_class_insert_block(oid: u32, relfilenode: u32) -> Vec<u8> {
        let mut v = Vec::new();
        v.extend_from_slice(&33u16.to_le_bytes()); // t_infomask2
        v.extend_from_slice(&0u16.to_le_bytes()); // t_infomask
        v.push(24); // t_hoff
        v.extend_from_slice(&pg_class_tuple_tail(oid, relfilenode, 0));
        v
    }

    /// Build block data for a HEAP_UPDATE with the given prefix/suffix
    /// lengths. Prefix bytes are stripped from the front of the column
    /// data; suffix bytes from the back. `extra_cols` controls how
    /// many trailing bytes the reconstructed tuple has past relfilenode
    /// (so `suffixlen` has somewhere to eat).
    fn pg_class_update_block(
        oid: u32,
        relfilenode: u32,
        prefixlen: usize,
        suffixlen: usize,
        extra_cols: usize,
    ) -> Vec<u8> {
        let mut v = Vec::new();
        if prefixlen > 0 {
            v.extend_from_slice(&(prefixlen as u16).to_le_bytes());
        }
        if suffixlen > 0 {
            v.extend_from_slice(&(suffixlen as u16).to_le_bytes());
        }
        v.extend_from_slice(&33u16.to_le_bytes()); // t_infomask2
        v.extend_from_slice(&0u16.to_le_bytes()); // t_infomask
        v.push(24); // t_hoff
        let tail = pg_class_tuple_tail(oid, relfilenode, extra_cols);
        // tail[0] is the 1-byte MAXALIGN pad (reconstructed offset
        // 23..24 = bitmap+pad region). Bitmap/padding is always
        // present in WAL block data regardless of prefix compression
        // (PG `heap_update` writes it as a separate rdata chunk when
        // prefixlen > 0). Column data follows; the WAL only contains
        // `[t_hoff + prefixlen .. t_len - suffixlen]` of the column
        // region.
        let header_part_len = 24 - 23; // bitmap+padding bytes = 1
        v.extend_from_slice(&tail[..header_part_len]);
        let cols = &tail[header_part_len..];
        let cols_end = cols.len() - suffixlen;
        v.extend_from_slice(&cols[prefixlen..cols_end]);
        v
    }

    /// Wrap block data into an XLogRecord with the given rmgr + info.
    /// `main_data` is supplied verbatim (HEAP_UPDATE needs an
    /// xl_heap_update; HEAP_INSERT leaves it empty).
    fn record(rm: RmId, info: u8, main_data: Vec<u8>, block_data: Vec<u8>) -> XLogRecord<'static> {
        XLogRecord {
            header: XLogRecordHeader {
                resource_manager_id: rm as u8,
                info,
                ..Default::default()
            },
            blocks: vec![XLogRecordBlock {
                header: XLogRecordBlockHeader {
                    location: BlockLocation {
                        rel: RelFileNode {
                            spc_node: 1663,
                            db_node: 5,
                            rel_node: 1259,
                        },
                        block_no: 0,
                    },
                    ..Default::default()
                },
                data: std::borrow::Cow::Owned(block_data),
                ..Default::default()
            }],
            main_data: std::borrow::Cow::Owned(main_data),
            ..Default::default()
        }
    }

    /// xl_heap_update main_data with the given flags byte. Other fields
    /// stay zero — only `flags` matters for the decoder.
    fn xl_heap_update_main_data(flags: u8) -> Vec<u8> {
        let mut md = vec![0u8; SIZE_OF_HEAP_UPDATE];
        md[XL_HEAP_UPDATE_FLAGS_OFFSET] = flags;
        md
    }

    #[test]
    fn decodes_minimal_pg_class_insert() {
        let data = pg_class_insert_block(2615, 30000);
        let rec = record(RmId::Heap, HEAP_INSERT_OP, Vec::new(), data);
        let row = match decode_pg_class_tuple(&rec, 0) {
            DecodeOutcome::Decoded(r) => r,
            other => panic!("expected Decoded, got {other:?}"),
        };
        assert_eq!(row.oid, 2615);
        assert_eq!(row.relfilenode, 30000);
    }

    #[test]
    fn decodes_with_null_bitmap_present_in_t_hoff() {
        // pg_class has nullable columns at the tail. HEAP_HASNULL set,
        // t_hoff = 32 (MAXALIGN(23 + 5-byte bitmap for 33 attrs)).
        let t_hoff: u8 = 32;
        let mut v = Vec::new();
        v.extend_from_slice(&33u16.to_le_bytes());
        v.extend_from_slice(&1u16.to_le_bytes()); // HEAP_HASNULL
        v.push(t_hoff);
        v.extend_from_slice(&[0xff; 9]); // 5 bitmap + 4 padding bytes
        v.extend_from_slice(&1234u32.to_le_bytes()); // oid
        v.extend_from_slice(&[0u8; 64]); // relname
        v.extend_from_slice(&[0u8; 20]); // cols 3-7
        v.extend_from_slice(&77777u32.to_le_bytes()); // relfilenode
        let rec = record(RmId::Heap, HEAP_INSERT_OP, Vec::new(), v);
        let row = match decode_pg_class_tuple(&rec, 0) {
            DecodeOutcome::Decoded(r) => r,
            other => panic!("expected Decoded, got {other:?}"),
        };
        assert_eq!(row.oid, 1234);
        assert_eq!(row.relfilenode, 77777);
    }

    #[test]
    fn rejects_truncated_block_data() {
        let cases: Vec<Vec<u8>> = vec![vec![], vec![0u8; 4]];
        for data in cases {
            let rec = record(RmId::Heap, HEAP_INSERT_OP, Vec::new(), data);
            assert!(matches!(
                decode_pg_class_tuple(&rec, 0),
                DecodeOutcome::Undecoded
            ));
        }
        // Header present but payload truncated before col 8.
        let mut v = Vec::new();
        v.extend_from_slice(&33u16.to_le_bytes());
        v.extend_from_slice(&0u16.to_le_bytes());
        v.push(24);
        v.extend_from_slice(&[0u8; 10]);
        let rec = record(RmId::Heap, HEAP_INSERT_OP, Vec::new(), v);
        assert!(matches!(
            decode_pg_class_tuple(&rec, 0),
            DecodeOutcome::Undecoded
        ));
    }

    #[test]
    fn rejects_invalid_t_hoff() {
        // t_hoff < SizeofHeapTupleHeader is malformed.
        let mut v = Vec::new();
        v.extend_from_slice(&33u16.to_le_bytes());
        v.extend_from_slice(&0u16.to_le_bytes());
        v.push(16); // < 23
        v.extend_from_slice(&[0u8; 200]);
        let rec = record(RmId::Heap, HEAP_INSERT_OP, Vec::new(), v);
        assert!(matches!(
            decode_pg_class_tuple(&rec, 0),
            DecodeOutcome::Undecoded
        ));
    }

    #[test]
    fn missing_block_returns_undecoded() {
        let rec = record(RmId::Heap, HEAP_INSERT_OP, Vec::new(), Vec::new());
        assert!(matches!(
            decode_pg_class_tuple(&rec, 1),
            DecodeOutcome::Undecoded
        ));
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

    #[test]
    fn update_prefix_zero_suffix_zero_decodes() {
        // HEAP_UPDATE with flags=0 (no prefix/suffix). Same shape as
        // INSERT but in main_data we have an xl_heap_update.
        let data = pg_class_update_block(2608, 40000, 0, 0, 0);
        let rec = record(
            RmId::Heap,
            HEAP_UPDATE_OP,
            xl_heap_update_main_data(0),
            data,
        );
        match decode_pg_class_tuple(&rec, 0) {
            DecodeOutcome::Decoded(r) => {
                assert_eq!(r.oid, 2608);
                assert_eq!(r.relfilenode, 40000);
            }
            other => panic!("expected Decoded, got {other:?}"),
        }
    }

    #[test]
    fn update_prefix_eq_2_is_oid_in_prefix() {
        // prefixlen ∈ (0, 4): OID straddles prefix/column boundary.
        // Cannot recover; signal as OidInPrefix.
        let data = pg_class_update_block(2608, 40000, 2, 0, 0);
        let rec = record(
            RmId::Heap,
            HEAP_UPDATE_OP,
            xl_heap_update_main_data(XLH_UPDATE_PREFIX_FROM_OLD),
            data,
        );
        assert!(matches!(
            decode_pg_class_tuple(&rec, 0),
            DecodeOutcome::OidInPrefix
        ));
    }

    #[test]
    fn update_prefix_eq_4_is_oid_in_prefix() {
        // OID entirely in prefix.
        let data = pg_class_update_block(2608, 40000, 4, 0, 0);
        let rec = record(
            RmId::Heap,
            HEAP_UPDATE_OP,
            xl_heap_update_main_data(XLH_UPDATE_PREFIX_FROM_OLD),
            data,
        );
        assert!(matches!(
            decode_pg_class_tuple(&rec, 0),
            DecodeOutcome::OidInPrefix
        ));
    }

    #[test]
    fn update_prefix_eq_88_is_oid_in_prefix() {
        // VACUUM FULL pg_<non-mapped> shape: cols 1..7 unchanged, so
        // PG's prefix-compute yields prefixlen ≈ 88. OID at offset 0
        // of col region is fully in the un-logged prefix.
        let data = pg_class_update_block(2608, 40000, 88, 0, 0);
        let rec = record(
            RmId::Heap,
            HEAP_UPDATE_OP,
            xl_heap_update_main_data(XLH_UPDATE_PREFIX_FROM_OLD),
            data,
        );
        assert!(matches!(
            decode_pg_class_tuple(&rec, 0),
            DecodeOutcome::OidInPrefix
        ));
    }

    #[test]
    fn update_suffix_only_decodes() {
        // prefixlen=0, suffixlen=4 — suffix never overlaps OID (offset
        // 0) or relfilenode (offset 88). Tail past relfilenode shrinks
        // by 4 bytes; decode still succeeds.
        let data = pg_class_update_block(2608, 40000, 0, 4, 8);
        let rec = record(
            RmId::Heap,
            HEAP_UPDATE_OP,
            xl_heap_update_main_data(XLH_UPDATE_SUFFIX_FROM_OLD),
            data,
        );
        match decode_pg_class_tuple(&rec, 0) {
            DecodeOutcome::Decoded(r) => {
                assert_eq!(r.oid, 2608);
                assert_eq!(r.relfilenode, 40000);
            }
            other => panic!("expected Decoded, got {other:?}"),
        }
    }

    #[test]
    fn update_both_flags_uses_two_uint16s() {
        // When PG sets both XLH_UPDATE_PREFIX_FROM_OLD and
        // XLH_UPDATE_SUFFIX_FROM_OLD, block 0 starts with two u16s.
        // Decoder must skip both before reading xl_heap_header.
        let data = pg_class_update_block(2608, 40000, 88, 4, 8);
        let rec = record(
            RmId::Heap,
            HEAP_UPDATE_OP,
            xl_heap_update_main_data(XLH_UPDATE_PREFIX_FROM_OLD | XLH_UPDATE_SUFFIX_FROM_OLD),
            data,
        );
        assert!(matches!(
            decode_pg_class_tuple(&rec, 0),
            DecodeOutcome::OidInPrefix
        ));
    }

    #[test]
    fn update_with_short_main_data_is_undecoded() {
        // xl_heap_update missing entirely.
        let data = pg_class_update_block(2608, 40000, 0, 0, 0);
        let rec = record(RmId::Heap, HEAP_UPDATE_OP, Vec::new(), data);
        assert!(matches!(
            decode_pg_class_tuple(&rec, 0),
            DecodeOutcome::Undecoded
        ));
    }

    #[test]
    fn hot_update_treated_like_update() {
        // HOT_UPDATE shares xl_heap_update layout; decoder must apply
        // the same flags lookup.
        let data = pg_class_update_block(2608, 40000, 88, 0, 0);
        let rec = record(
            RmId::Heap,
            HEAP_HOT_UPDATE_OP,
            xl_heap_update_main_data(XLH_UPDATE_PREFIX_FROM_OLD),
            data,
        );
        assert!(matches!(
            decode_pg_class_tuple(&rec, 0),
            DecodeOutcome::OidInPrefix
        ));
    }
}
