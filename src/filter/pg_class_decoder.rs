//! Narrow heap-tuple decoder for `pg_class` block data.
//!
//! Extracts `(oid, relfilenode)` from a WAL heap block targeting pg_class
//! so [`CatalogTracker`](crate::filter::catalog_tracker::CatalogTracker) tracks
//! non-mapped-catalog filenode rewrites (VACUUM FULL / REINDEX / CLUSTER
//! on pg_depend, pg_namespace, …). Mapped catalogs go via
//! `XLOG_RELMAP_UPDATE` instead.
//!
//! ## Block-data layout
//!
//! `XLOG_HEAP_INSERT` (info 0x00), block 0 = `xl_heap_header + payload`:
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
//! `t_infomask2 / t_infomask / t_hoff` from the WAL header, copies payload
//! to offset 23. Column data begins at reconstructed offset `t_hoff`.
//!
//! `XLOG_HEAP_UPDATE` / `XLOG_HEAP_HOT_UPDATE` (info 0x20 / 0x40): PG's
//! `heap_update` (`src/backend/access/heap/heapam.c`) compresses away byte
//! prefixes/suffixes shared with the old tuple. Block 0:
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
//! `xl_heap_update.flags` lives in `main_data` byte offset 7
//! (`SizeOfHeapUpdate = 14` on disk; in-memory sizeof 16 has trailing pad
//! PG strips via `XLogRegisterData`).
//!
//! `XLOG_HEAP_INPLACE` block data is column data alone, no header.
//! `XLOG_HEAP2_*` skipped — pg_class is single-row INSERT/UPDATE territory.
//! Block carrying a full-page image omits its data; tuple is read from the
//! restored page at the record's offnum instead.
//!
//! ## pg_class column offsets
//!
//! PG ≥ 16 layout (`src/include/catalog/pg_class.h`, stable 16/17/18):
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
//! Decoder reads cols 1 and 8. Cols 1–8 are NOT NULL, so a null bitmap
//! (HEAP_HASNULL for later nullable cols like relacl) doesn't shift them;
//! `t_hoff` already covers bitmap + alignment.
//!
//! ## VACUUM FULL on non-mapped catalogs
//!
//! `VACUUM FULL pg_depend` issues a pg_class `heap_update` changing only
//! `relfilenode`. Cols 1–7 (88 bytes) unchanged, so `prefixlen ≈ 88` and
//! OID lives entirely in the un-logged prefix. Surfaces as
//! [`DecodeOutcome::OidInPrefix`]; caller resolves OID through the old
//! tuple's slot, see [`tuple_slots`].

use walrus::pg::walparser::XLogRecord;

use crate::backfill::backup_page_walk::{page_max_offnum, page_tuple_bytes};
use crate::decode::fpi::restore_block_image;
use crate::schema::FIRST_NORMAL_OBJECT_ID;

/// `sizeof(xl_heap_header)`, PG `heapam_xlog.h`
const XL_HEAP_HEADER_SIZE: usize = 5;
/// `SizeofHeapTupleHeader`, stable 23 since PG 7.x
const SIZE_OF_HEAP_TUPLE_HEADER: usize = 23;
/// `HeapTupleHeaderData.t_hoff` offset
const T_HOFF_OFFSET: usize = 22;
/// `SizeOfHeapUpdate`, PG `heapam_xlog.h` on-disk size. C-struct sizeof
/// is 16; `XLogRegisterData(&xlrec, SizeOfHeapUpdate)` strips trailing pad.
const SIZE_OF_HEAP_UPDATE: usize = 14;
/// `xl_heap_update.flags` offset: old_xmax(4) + old_offnum(2) +
/// old_infobits_set(1)
const XL_HEAP_UPDATE_FLAGS_OFFSET: usize = 7;
/// `xl_heap_update.old_offnum` and `xl_heap_delete.offnum`, after a 4-byte xid
const XL_HEAP_OLD_OFFNUM_OFFSET: usize = 4;
/// `xl_heap_update.new_offnum`
const XL_HEAP_UPDATE_NEW_OFFNUM_OFFSET: usize = 12;
/// `HEAP_UPDATE_BLKREF_HEAP_OLD`, present only when old page differs
const HEAP_UPDATE_OLD_BLOCK: usize = 1;
const PG_CLASS_OID_OFFSET: usize = 0;
const PG_CLASS_RELNAME_OFFSET: usize = 4;
/// `NAMEDATALEN`
const NAME_LEN: usize = 64;
const PG_CLASS_RELNAMESPACE_OFFSET: usize = 68;
/// Sum of pg_class col widths 1..=7: 4 + 64 + 4*5
const PG_CLASS_RELFILENODE_OFFSET: usize = 88;
/// `XLOG_HEAP_OPMASK`, masks out `XLOG_HEAP_INIT_PAGE` (0x80)
const HEAP_OPMASK: u8 = 0x70;

const HEAP_INSERT_OP: u8 = 0x00;
const HEAP_DELETE_OP: u8 = 0x10;
const HEAP_UPDATE_OP: u8 = 0x20;
const HEAP_HOT_UPDATE_OP: u8 = 0x40;
const HEAP_INPLACE_OP: u8 = 0x70;

/// `XLH_UPDATE_PREFIX_FROM_OLD`, PG `heapam_xlog.h`
const XLH_UPDATE_PREFIX_FROM_OLD: u8 = 1 << 5;
/// `XLH_UPDATE_SUFFIX_FROM_OLD`
const XLH_UPDATE_SUFFIX_FROM_OLD: u8 = 1 << 6;

/// Ops carrying block-0 `xl_heap_header + payload`. INPLACE / DELETE /
/// LOCK / CONFIRM / TRUNCATE do not.
const HEAP_INFO_NEW_TUPLE_OPS: &[u8] = &[HEAP_INSERT_OP, HEAP_UPDATE_OP, HEAP_HOT_UPDATE_OP];

/// `public` namespace oid, only initdb namespace users create tables in
const PG_PUBLIC_NAMESPACE: u32 = 2200;

/// Heap tuple slot `(block, offnum)`
pub type Slot = (u32, u16);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PgClassRow {
    pub oid: u32,
    pub relname: [u8; NAME_LEN],
    pub relnamespace: u32,
    pub relfilenode: u32,
}

impl PgClassRow {
    /// Row from tuple column data, which starts at `t_hoff`
    fn from_columns(cols: &[u8]) -> Option<Self> {
        let u32_at =
            |off: usize| Some(u32::from_le_bytes(cols.get(off..off + 4)?.try_into().ok()?));
        Some(Self {
            oid: u32_at(PG_CLASS_OID_OFFSET)?,
            relname: cols
                .get(PG_CLASS_RELNAME_OFFSET..PG_CLASS_RELNAME_OFFSET + NAME_LEN)?
                .try_into()
                .ok()?,
            relnamespace: u32_at(PG_CLASS_RELNAMESPACE_OFFSET)?,
            relfilenode: u32_at(PG_CLASS_RELFILENODE_OFFSET)?,
        })
    }

    /// Row from an on-page heap tuple
    fn from_tuple(tuple: &[u8]) -> Option<Self> {
        let t_hoff = *tuple.get(T_HOFF_OFFSET)? as usize;
        Self::from_columns(tuple.get(t_hoff..)?)
    }

    /// Catalog a VACUUM FULL / CLUSTER rebuilds into this row's storage.
    /// `make_new_heap` (PG `src/backend/commands/cluster.c`) names the
    /// transient heap `pg_temp_<parent oid>` in the parent's namespace;
    /// users create in no initdb namespace but `public`
    pub fn rebuilt_catalog(&self) -> Option<u32> {
        if self.relnamespace >= FIRST_NORMAL_OBJECT_ID || self.relnamespace == PG_PUBLIC_NAMESPACE {
            return None;
        }
        let len = self.relname.iter().position(|&b| b == 0)?;
        let digits = std::str::from_utf8(&self.relname[..len])
            .ok()?
            .strip_prefix("pg_temp_")?;
        let parent: u32 = digits.parse().ok()?;
        (parent != 0 && parent < FIRST_NORMAL_OBJECT_ID && digits == parent.to_string())
            .then_some(parent)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DecodeOutcome {
    Decoded(PgClassRow),
    /// `XLH_UPDATE_PREFIX_FROM_OLD` with `prefixlen > 0`: OID lives wholly
    /// (or partly, `prefixlen < 4`) in the un-logged prefix. Carries the
    /// relfilenode bytes the record logged; `None` bytes equal the old
    /// tuple's, in shared prefix or suffix
    OidInPrefix([Option<u8>; 4]),
    Undecoded,
}

/// Caller must pre-filter rmgr + info via [`info_carries_new_tuple_heap`]
/// or [`info_is_inplace`]. Full-page image stands in for block data PG
/// omits once it takes one
pub fn decode_pg_class_tuple(
    record: &XLogRecord,
    block_idx: usize,
    page_magic: u16,
) -> DecodeOutcome {
    let Some(block) = record.blocks.get(block_idx) else {
        return DecodeOutcome::Undecoded;
    };
    if block.header.has_image() {
        let Some((_, offnum)) = tuple_slots(record).new else {
            return DecodeOutcome::Undecoded;
        };
        return restore_block_image(block, page_magic)
            .ok()
            .and_then(|page| PgClassRow::from_tuple(page_tuple_bytes(&page, offnum)?))
            .map_or(DecodeOutcome::Undecoded, DecodeOutcome::Decoded);
    }
    let (has_prefix, has_suffix) = match record.header.info & HEAP_OPMASK {
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
        // In-place block data is column data alone
        HEAP_INPLACE_OP => {
            return PgClassRow::from_columns(&block.data)
                .map_or(DecodeOutcome::Undecoded, DecodeOutcome::Decoded);
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
    let t_hoff = data[skip + XL_HEAP_HEADER_SIZE - 1] as usize;
    if t_hoff < SIZE_OF_HEAP_TUPLE_HEADER {
        return DecodeOutcome::Undecoded;
    }
    // block offset of reconstructed-tuple offset t_hoff + prefixlen
    let cds = skip + XL_HEAP_HEADER_SIZE + (t_hoff - SIZE_OF_HEAP_TUPLE_HEADER);
    let Some(logged) = data.get(cds..) else {
        return DecodeOutcome::Undecoded;
    };
    if prefixlen > 0 {
        // OID at column offset 0 always falls in the prefix; logged bytes
        // cover column offsets from prefixlen until any suffix
        let rfn = std::array::from_fn(|i| {
            (PG_CLASS_RELFILENODE_OFFSET + i)
                .checked_sub(prefixlen)
                .and_then(|at| logged.get(at).copied())
        });
        return DecodeOutcome::OidInPrefix(rfn);
    }
    // relfilenode at offset 88 is below any plausible suffix
    PgClassRow::from_columns(logged).map_or(DecodeOutcome::Undecoded, DecodeOutcome::Decoded)
}

/// Old and new tuple slots a heap record names; `None` where op has none
/// or main data is short
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct TupleSlots {
    pub old: Option<Slot>,
    pub new: Option<Slot>,
}

pub fn tuple_slots(record: &XLogRecord) -> TupleSlots {
    let md = &record.main_data;
    let slot = |block: usize, at: usize| {
        let block_no = record.blocks.get(block)?.header.location.block_no;
        let offnum = u16::from_le_bytes(md.get(at..at + 2)?.try_into().ok()?);
        Some((block_no, offnum))
    };
    match record.header.info & HEAP_OPMASK {
        HEAP_INSERT_OP | HEAP_INPLACE_OP => TupleSlots {
            old: None,
            new: slot(0, 0),
        },
        HEAP_DELETE_OP => TupleSlots {
            old: slot(0, XL_HEAP_OLD_OFFNUM_OFFSET),
            new: None,
        },
        HEAP_UPDATE_OP | HEAP_HOT_UPDATE_OP => {
            let old_block = if record.blocks.len() > HEAP_UPDATE_OLD_BLOCK {
                HEAP_UPDATE_OLD_BLOCK
            } else {
                0
            };
            TupleSlots {
                old: slot(old_block, XL_HEAP_OLD_OFFNUM_OFFSET),
                new: slot(0, XL_HEAP_UPDATE_NEW_OFFNUM_OFFSET),
            }
        }
        _ => TupleSlots::default(),
    }
}

/// Every pg_class row on a heap page, by offnum
pub fn page_rows(page: &[u8]) -> impl Iterator<Item = (u16, PgClassRow)> + '_ {
    (1..=page_max_offnum(page))
        .filter_map(|off| Some((off, PgClassRow::from_tuple(page_tuple_bytes(page, off)?)?)))
}

/// True iff `RM_HEAP` op (init-page flag masked off) carries block-0
/// `xl_heap_header + payload`.
pub fn info_carries_new_tuple_heap(info: u8) -> bool {
    HEAP_INFO_NEW_TUPLE_OPS.contains(&(info & HEAP_OPMASK))
}

/// True iff `RM_HEAP` op overwrites a tuple in place
pub fn info_is_inplace(info: u8) -> bool {
    info & HEAP_OPMASK == HEAP_INPLACE_OP
}

/// No `RM_HEAP2` info has the `xl_heap_header + payload` shape:
/// MULTI_INSERT uses xl_multi_insert_tuple per row, NEW_CID is metadata,
/// VISIBLE / LOCK_UPDATED / PRUNE_* carry no tuple. Always `false`.
pub fn info_carries_new_tuple_heap2(_info: u8) -> bool {
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use walrus::pg::walparser::{
        BKP_BLOCK_HAS_IMAGE, BlockLocation, RelFileNode, RmId, XLP_PAGE_MAGIC_PG15,
        XLogRecordBlock, XLogRecordBlockHeader, XLogRecordBlockImageHeader, XLogRecordHeader,
    };

    const MAGIC: u16 = XLP_PAGE_MAGIC_PG15;

    /// Reconstructed pg_class tuple from offset 23 on: bitmap/pad (1 byte)
    /// + 8 cols. `extra_cols` adds trailing bytes for suffix tests.
    fn pg_class_tuple_tail(oid: u32, relfilenode: u32, extra_cols: usize) -> Vec<u8> {
        let mut v = Vec::new();
        v.push(0); // MAXALIGN pad, offset 23 -> 24
        v.extend_from_slice(&oid.to_le_bytes());
        v.extend_from_slice(&[0u8; 64]); // relname
        v.extend_from_slice(&0u32.to_le_bytes()); // relnamespace
        v.extend_from_slice(&0u32.to_le_bytes()); // reltype
        v.extend_from_slice(&0u32.to_le_bytes()); // reloftype
        v.extend_from_slice(&0u32.to_le_bytes()); // relowner
        v.extend_from_slice(&0u32.to_le_bytes()); // relam
        v.extend_from_slice(&relfilenode.to_le_bytes());
        v.extend(std::iter::repeat_n(0u8, extra_cols)); // cols 9+, suffix fodder
        v
    }

    fn pg_class_insert_block(oid: u32, relfilenode: u32) -> Vec<u8> {
        let mut v = Vec::new();
        v.extend_from_slice(&33u16.to_le_bytes()); // t_infomask2
        v.extend_from_slice(&0u16.to_le_bytes()); // t_infomask
        v.push(24); // t_hoff
        v.extend_from_slice(&pg_class_tuple_tail(oid, relfilenode, 0));
        v
    }

    /// HEAP_UPDATE block data; prefix bytes stripped from front of column
    /// data, suffix from back. `extra_cols` gives suffix bytes to eat.
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
        // tail[0] is the bitmap/pad byte (offsets 23..24); PG heap_update
        // always emits it as a separate rdata chunk even when prefix-
        // compressing, then logs only [t_hoff+prefixlen .. t_len-suffixlen]
        let header_part_len = 24 - 23; // bitmap+padding bytes = 1
        v.extend_from_slice(&tail[..header_part_len]);
        let cols = &tail[header_part_len..];
        let cols_end = cols.len() - suffixlen;
        v.extend_from_slice(&cols[prefixlen..cols_end]);
        v
    }

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

    /// Only `flags` matters to the decoder; other fields stay zero.
    fn xl_heap_update_main_data(flags: u8) -> Vec<u8> {
        let mut md = vec![0u8; SIZE_OF_HEAP_UPDATE];
        md[XL_HEAP_UPDATE_FLAGS_OFFSET] = flags;
        md
    }

    #[test]
    fn decodes_minimal_pg_class_insert() {
        let data = pg_class_insert_block(2615, 30000);
        let rec = record(RmId::Heap, HEAP_INSERT_OP, Vec::new(), data);
        let row = match decode_pg_class_tuple(&rec, 0, MAGIC) {
            DecodeOutcome::Decoded(r) => r,
            other => panic!("expected Decoded, got {other:?}"),
        };
        assert_eq!(row.oid, 2615);
        assert_eq!(row.relfilenode, 30000);
    }

    #[test]
    fn decodes_with_null_bitmap_present_in_t_hoff() {
        // HEAP_HASNULL set, t_hoff = 32 = MAXALIGN(23 + 5-byte bitmap for
        // 33 attrs)
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
        let row = match decode_pg_class_tuple(&rec, 0, MAGIC) {
            DecodeOutcome::Decoded(r) => r,
            other => panic!("expected Decoded, got {other:?}"),
        };
        assert_eq!(row.oid, 1234);
        assert_eq!(row.relfilenode, 77777);
    }

    #[test]
    fn rejects_truncated_block_data() {
        let cases = [Vec::new(), vec![0u8; 4]];
        for data in cases {
            let rec = record(RmId::Heap, HEAP_INSERT_OP, Vec::new(), data);
            assert!(matches!(
                decode_pg_class_tuple(&rec, 0, MAGIC),
                DecodeOutcome::Undecoded
            ));
        }
        // Header present, payload truncated before col 8
        let mut v = Vec::new();
        v.extend_from_slice(&33u16.to_le_bytes());
        v.extend_from_slice(&0u16.to_le_bytes());
        v.push(24);
        v.extend_from_slice(&[0u8; 10]);
        let rec = record(RmId::Heap, HEAP_INSERT_OP, Vec::new(), v);
        assert!(matches!(
            decode_pg_class_tuple(&rec, 0, MAGIC),
            DecodeOutcome::Undecoded
        ));
    }

    #[test]
    fn rejects_invalid_t_hoff() {
        let mut v = Vec::new();
        v.extend_from_slice(&33u16.to_le_bytes());
        v.extend_from_slice(&0u16.to_le_bytes());
        v.push(16); // < 23
        v.extend_from_slice(&[0u8; 200]);
        let rec = record(RmId::Heap, HEAP_INSERT_OP, Vec::new(), v);
        assert!(matches!(
            decode_pg_class_tuple(&rec, 0, MAGIC),
            DecodeOutcome::Undecoded
        ));
    }

    #[test]
    fn missing_block_returns_undecoded() {
        let rec = record(RmId::Heap, HEAP_INSERT_OP, Vec::new(), Vec::new());
        assert!(matches!(
            decode_pg_class_tuple(&rec, 1, MAGIC),
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
        // flags=0: INSERT-shaped block, but main_data has xl_heap_update
        let data = pg_class_update_block(2608, 40000, 0, 0, 0);
        let rec = record(
            RmId::Heap,
            HEAP_UPDATE_OP,
            xl_heap_update_main_data(0),
            data,
        );
        match decode_pg_class_tuple(&rec, 0, MAGIC) {
            DecodeOutcome::Decoded(r) => {
                assert_eq!(r.oid, 2608);
                assert_eq!(r.relfilenode, 40000);
            }
            other => panic!("expected Decoded, got {other:?}"),
        }
    }

    #[test]
    fn update_prefix_eq_2_is_oid_in_prefix() {
        // prefixlen ∈ (0, 4): OID straddles prefix/column boundary
        let data = pg_class_update_block(2608, 40000, 2, 0, 0);
        let rec = record(
            RmId::Heap,
            HEAP_UPDATE_OP,
            xl_heap_update_main_data(XLH_UPDATE_PREFIX_FROM_OLD),
            data,
        );
        assert!(matches!(
            decode_pg_class_tuple(&rec, 0, MAGIC),
            DecodeOutcome::OidInPrefix(_)
        ));
    }

    #[test]
    fn update_prefix_eq_4_is_oid_in_prefix() {
        // OID entirely in prefix
        let data = pg_class_update_block(2608, 40000, 4, 0, 0);
        let rec = record(
            RmId::Heap,
            HEAP_UPDATE_OP,
            xl_heap_update_main_data(XLH_UPDATE_PREFIX_FROM_OLD),
            data,
        );
        assert!(matches!(
            decode_pg_class_tuple(&rec, 0, MAGIC),
            DecodeOutcome::OidInPrefix(_)
        ));
    }

    #[test]
    fn update_prefix_eq_88_is_oid_in_prefix() {
        // VACUUM FULL non-mapped catalog: cols 1..7 unchanged, prefixlen
        // ≈ 88, OID fully in un-logged prefix
        let data = pg_class_update_block(2608, 40000, 88, 0, 0);
        let rec = record(
            RmId::Heap,
            HEAP_UPDATE_OP,
            xl_heap_update_main_data(XLH_UPDATE_PREFIX_FROM_OLD),
            data,
        );
        assert_eq!(
            decode_pg_class_tuple(&rec, 0, MAGIC),
            DecodeOutcome::OidInPrefix(40000u32.to_le_bytes().map(Some))
        );
    }

    #[test]
    fn update_prefix_into_relfilenode_leaves_shared_bytes_unknown() {
        // Low relfilenode byte unchanged extends the prefix one byte
        let data = pg_class_update_block(2608, 40000, 89, 0, 0);
        let rec = record(
            RmId::Heap,
            HEAP_UPDATE_OP,
            xl_heap_update_main_data(XLH_UPDATE_PREFIX_FROM_OLD),
            data,
        );
        let [_, b1, b2, b3] = 40000u32.to_le_bytes();
        assert_eq!(
            decode_pg_class_tuple(&rec, 0, MAGIC),
            DecodeOutcome::OidInPrefix([None, Some(b1), Some(b2), Some(b3)])
        );
    }

    #[test]
    fn update_suffix_only_decodes() {
        // suffix never overlaps OID (offset 0) or relfilenode (offset 88)
        let data = pg_class_update_block(2608, 40000, 0, 4, 8);
        let rec = record(
            RmId::Heap,
            HEAP_UPDATE_OP,
            xl_heap_update_main_data(XLH_UPDATE_SUFFIX_FROM_OLD),
            data,
        );
        match decode_pg_class_tuple(&rec, 0, MAGIC) {
            DecodeOutcome::Decoded(r) => {
                assert_eq!(r.oid, 2608);
                assert_eq!(r.relfilenode, 40000);
            }
            other => panic!("expected Decoded, got {other:?}"),
        }
    }

    #[test]
    fn update_both_flags_uses_two_uint16s() {
        // Both PREFIX and SUFFIX flags: block 0 leads with two u16s,
        // decoder must skip both before xl_heap_header
        let data = pg_class_update_block(2608, 40000, 88, 4, 8);
        let rec = record(
            RmId::Heap,
            HEAP_UPDATE_OP,
            xl_heap_update_main_data(XLH_UPDATE_PREFIX_FROM_OLD | XLH_UPDATE_SUFFIX_FROM_OLD),
            data,
        );
        assert!(matches!(
            decode_pg_class_tuple(&rec, 0, MAGIC),
            DecodeOutcome::OidInPrefix(_)
        ));
    }

    #[test]
    fn update_with_short_main_data_is_undecoded() {
        let data = pg_class_update_block(2608, 40000, 0, 0, 0);
        let rec = record(RmId::Heap, HEAP_UPDATE_OP, Vec::new(), data);
        assert!(matches!(
            decode_pg_class_tuple(&rec, 0, MAGIC),
            DecodeOutcome::Undecoded
        ));
    }

    #[test]
    fn hot_update_treated_like_update() {
        // HOT_UPDATE shares xl_heap_update layout, same flags lookup
        let data = pg_class_update_block(2608, 40000, 88, 0, 0);
        let rec = record(
            RmId::Heap,
            HEAP_HOT_UPDATE_OP,
            xl_heap_update_main_data(XLH_UPDATE_PREFIX_FROM_OLD),
            data,
        );
        assert!(matches!(
            decode_pg_class_tuple(&rec, 0, MAGIC),
            DecodeOutcome::OidInPrefix(_)
        ));
    }

    fn named_row(name: &str, relnamespace: u32) -> PgClassRow {
        let mut relname = [0u8; NAME_LEN];
        relname[..name.len()].copy_from_slice(name.as_bytes());
        PgClassRow {
            oid: 50000,
            relname,
            relnamespace,
            relfilenode: 50000,
        }
    }

    #[test]
    fn rebuilt_catalog_reads_transient_heap_name() {
        assert_eq!(named_row("pg_temp_2608", 11).rebuilt_catalog(), Some(2608));
        assert_eq!(named_row("pg_temp_2840", 99).rebuilt_catalog(), Some(2840));
        // user rebuild, user namespace, or user table in public
        assert_eq!(named_row("pg_temp_16500", 11).rebuilt_catalog(), None);
        assert_eq!(named_row("pg_temp_2608", 16390).rebuilt_catalog(), None);
        assert_eq!(named_row("pg_temp_2608", 2200).rebuilt_catalog(), None);
        assert_eq!(named_row("pg_temp_02608", 11).rebuilt_catalog(), None);
        assert_eq!(named_row("pg_depend", 11).rebuilt_catalog(), None);
    }

    #[test]
    fn tuple_slots_follow_op_layout() {
        let mut update = record(RmId::Heap, HEAP_UPDATE_OP, vec![0u8; 14], vec![]);
        update.blocks[0].header.location.block_no = 7;
        update.main_data.to_mut()[4..6].copy_from_slice(&3u16.to_le_bytes());
        update.main_data.to_mut()[12..14].copy_from_slice(&9u16.to_le_bytes());
        assert_eq!(
            tuple_slots(&update),
            TupleSlots {
                old: Some((7, 3)),
                new: Some((7, 9))
            }
        );
        let mut old_page = update.blocks[0].clone();
        old_page.header.location.block_no = 2;
        update.blocks.push(old_page);
        assert_eq!(tuple_slots(&update).old, Some((2, 3)));

        let insert = record(
            RmId::Heap,
            HEAP_INSERT_OP,
            5u16.to_le_bytes().to_vec(),
            vec![],
        );
        assert_eq!(tuple_slots(&insert).new, Some((0, 5)));
        let delete = record(RmId::Heap, HEAP_DELETE_OP, vec![0, 0, 0, 0, 4, 0], vec![]);
        assert_eq!(tuple_slots(&delete).old, Some((0, 4)));
        let short = record(RmId::Heap, HEAP_INSERT_OP, vec![], vec![]);
        assert_eq!(tuple_slots(&short), TupleSlots::default());
    }

    #[test]
    fn inplace_block_data_is_bare_columns() {
        let tail = pg_class_tuple_tail(2608, 40000, 0);
        let rec = record(RmId::Heap, HEAP_INPLACE_OP, vec![1, 0], tail[1..].to_vec());
        let DecodeOutcome::Decoded(row) = decode_pg_class_tuple(&rec, 0, MAGIC) else {
            panic!("inplace must decode");
        };
        assert_eq!((row.oid, row.relfilenode), (2608, 40000));
    }

    /// Page holding `rows` at offnums 1.., tuples with t_hoff 24
    fn page_with(rows: &[(u32, u32)]) -> Vec<u8> {
        let mut page = vec![0u8; 8192];
        let mut upper = page.len();
        for (i, &(oid, rfn)) in rows.iter().enumerate() {
            let mut tuple = vec![0u8; 23];
            tuple[22] = 24;
            tuple.extend_from_slice(&pg_class_tuple_tail(oid, rfn, 0));
            upper -= tuple.len();
            page[upper..upper + tuple.len()].copy_from_slice(&tuple);
            let lp = upper as u32 | (1 << 15) | ((tuple.len() as u32) << 17);
            let at = 24 + i * 4;
            page[at..at + 4].copy_from_slice(&lp.to_le_bytes());
        }
        let lower = (24 + rows.len() * 4) as u16;
        page[12..14].copy_from_slice(&lower.to_le_bytes());
        page
    }

    #[test]
    fn page_rows_lists_every_tuple() {
        let page = page_with(&[(2608, 40000), (50000, 50001)]);
        let rows: Vec<_> = page_rows(&page)
            .map(|(off, r)| (off, r.oid, r.relfilenode))
            .collect();
        assert_eq!(rows, [(1, 2608, 40000), (2, 50000, 50001)]);
    }

    #[test]
    fn image_block_decodes_tuple_behind_offnum() {
        let mut rec = record(
            RmId::Heap,
            HEAP_INSERT_OP,
            2u16.to_le_bytes().to_vec(),
            vec![],
        );
        let block = &mut rec.blocks[0];
        block.header.fork_flags = BKP_BLOCK_HAS_IMAGE;
        block.header.image_header = XLogRecordBlockImageHeader {
            image_length: 8192,
            hole_offset: 0,
            hole_length: 0,
            info: 0,
        };
        block.image = std::borrow::Cow::Owned(page_with(&[(2608, 40000), (2615, 41000)]));
        let DecodeOutcome::Decoded(row) = decode_pg_class_tuple(&rec, 0, MAGIC) else {
            panic!("image must decode");
        };
        assert_eq!((row.oid, row.relfilenode), (2615, 41000));
    }
}
