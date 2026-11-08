//! User-heap tuple decoder + Tier 1/2 type matrix.
//!
//! Tier 1 (fixed-width) + Tier 2 (length-prefixed mechanical) types decode
//! inline, `numeric` and `jsonb` through [`crate::decode::codecs`]; Tier 3
//! (arrays, extension types) surfaces raw bytes for the oracle.
//!
//! ## WAL layout (PG `access/heapam_xlog.h`, `heapam.c::heap_xlog_*`)
//!
//! ### INSERT (`info & 0x70 == 0x00`)
//!
//! - `main_data`: `xl_heap_insert` (3 bytes: `offnum:u16`, `flags:u8`)
//! - block 0 data: `xl_heap_header (5)` + bitmap[+pad] + col data
//!
//! `t_hoff` (5th byte of `xl_heap_header`) is col-data offset within the
//! reconstructed `HeapTupleHeaderData`, so col-data offset inside
//! `block.data` is `5 + (t_hoff - 23)`. `5` = `SizeOfHeapHeader`; `23` =
//! `SizeofHeapTupleHeader`, the fixed-header bytes PG strips per
//! `XLogRegisterBufData(0, tup->t_data + SizeofHeapTupleHeader, ...)`
//!
//! ### DELETE (`0x10`)
//!
//! - `main_data`: `xl_heap_delete` (8 bytes) [+ `xl_heap_header (5)` +
//!   old tuple bytes when `flags & XLH_DELETE_CONTAINS_OLD`]
//! - no block-0 tuple bytes
//!
//! ### UPDATE / HOT_UPDATE (`0x20` / `0x40`)
//!
//! - `main_data`: `xl_heap_update` (14 bytes) [+ `xl_heap_header (5)` +
//!   old tuple bytes when `flags & XLH_UPDATE_CONTAINS_OLD`]
//! - block 0 data: `[prefixlen:u16 if XLH_UPDATE_PREFIX_FROM_OLD]
//!   [suffixlen:u16 if XLH_UPDATE_SUFFIX_FROM_OLD] + xl_heap_header
//!   (5) + bitmap[+pad] + col data` spanning reconstructed offsets
//!   `t_hoff + prefixlen` .. `t_len - suffixlen`. PG prefix/suffix-
//!   compresses WAL when new shares contiguous head/tail bytes with old
//!   (`heap_update` in heapam.c). Compressed-away bytes aren't in WAL;
//!   reconstruction needs old tuple. Decoder marks those columns `None`
//!   + sets [`DecodedTuple::partial`]; xact buffer backfills from prev image.
//!
//! ## Replica-identity matrix (PG `ExtractReplicaIdentity`, heapam.c)
//!
//! | `relreplident` | old payload | this module's behaviour |
//! |---|---|---|
//! | `Full` (`'f'`) | every non-dropped column | decode every column; `old` populated |
//! | `UsingIndex` (`'i'`) | indexed columns only, others written as NULL via bitmap | decode all; non-indexed columns surface as `Some(Datum::Null)` |
//! | `Default` (`'d'`) with PK | PK columns when `XLH_UPDATE_CONTAINS_OLD_KEY` set | same as `UsingIndex` shape, with PK attnums populated |
//! | `Default` (`'d'`) no PK | nothing (PG never sets OLD_KEY) | `old = None` |
//! | `Nothing` (`'n'`) | empty | `old = None`; partial = true noted in stats |
//!
//! ## What lives in `block.data` vs the FPI
//!
//! PG `heap_insert`/`heap_update` set `REGBUF_KEEP_DATA` under
//! `RelationIsLogicallyLogged(rel)` (holds at `wal_level=logical`,
//! walshadow's source requirement, see docs/limitations.md). With
//! `KEEP_DATA` tuple bytes are always in `block.data`, even when an FPI
//! replaces the page at recovery (`heap_xlog_insert`). Decoder reads tuple
//! bytes off `block.data` exclusively; FPI-restore lives in [`crate::decode::fpi`]
//! for xact-buffer / BASEBACKUP use.
//!
//! ## Roll-back ghost rows
//!
//! Decoder emits eagerly per heap record: no per-xact buffer, no
//! `XLOG_XACT_ABORT` retraction. Aborted xacts produce ghost rows
//! downstream by design. Every output stamped with `xid` so xact buffer
//! can key on it.

use smallvec::{SmallVec, smallvec};

use crate::decode::datum::{Datum, DatumRef};
use thiserror::Error;
use walrus::pg::walparser::{RelFileNode, RmId, XLogRecord};

use crate::schema::{
    BOOLOID, BPCHAROID, BYTEAOID, CHAROID, CIDOID, CIDROID, DATEOID, FLOAT4OID, FLOAT8OID, INETOID,
    INT2OID, INT4OID, INT8OID, INTERVALOID, JSONBOID, JSONOID, MACADDR8OID, MACADDROID, MONEYOID,
    MissingDefault, NAMEOID, NUMERICOID, OID8OID, OIDOID, PG_LSNOID, RelAttr, RelDescriptor,
    ReplIdent, TEXTOID, TIDOID, TIMEOID, TIMESTAMPOID, TIMESTAMPTZOID, TIMETZOID, UUIDOID,
    VARCHAROID, XID8OID, XIDOID,
};

/// SmallVec sized at 1: single-row INSERT/UPDATE/DELETE stays stack-allocated,
/// only MULTI_INSERT with ntuples > 1 spills.
pub type DecodedHeaps = SmallVec<[DecodedHeap; 1]>;

/// PG `SizeOfHeapHeader`.
pub const SIZE_OF_HEAP_HEADER: usize = 5;
/// PG `SizeofHeapTupleHeader`, stable at 23 since PG 7.x.
pub const SIZE_OF_HEAP_TUPLE_HEADER: usize = 23;
/// PG `SizeOfHeapInsert` (offnum:u16 + flags:u8).
pub const SIZE_OF_HEAP_INSERT: usize = 3;
/// PG `SizeOfHeapDelete` (xmax:u32 + offnum:u16 + infobits_set:u8 + flags:u8).
pub const SIZE_OF_HEAP_DELETE: usize = 8;
/// PG `SizeOfHeapUpdate` (old_xmax:u32 + old_offnum:u16 + old_infobits:u8 +
/// flags:u8 + new_xmax:u32 + new_offnum:u16). C-struct `sizeof` is 16 with
/// trailing pad; PG `XLogRegisterData` strips it.
pub const SIZE_OF_HEAP_UPDATE: usize = 14;

/// Op portion of `info`, strips `XLOG_HEAP_INIT_PAGE`.
pub const XLOG_HEAP_OPMASK: u8 = 0x70;

pub const XLOG_HEAP_INSERT: u8 = 0x00;
pub const XLOG_HEAP_DELETE: u8 = 0x10;
pub const XLOG_HEAP_UPDATE: u8 = 0x20;
pub const XLOG_HEAP_TRUNCATE: u8 = 0x30;
pub const XLOG_HEAP_HOT_UPDATE: u8 = 0x40;
pub const XLOG_HEAP_CONFIRM: u8 = 0x50;
pub const XLOG_HEAP_LOCK: u8 = 0x60;
pub const XLOG_HEAP_INPLACE: u8 = 0x70;

pub const XLOG_HEAP2_MULTI_INSERT: u8 = 0x50;

/// `op=` label vocabulary for raw stash/decode counters, index-aligned
/// with [`OpCounters`]
pub const HEAP_OP_LABELS: [&str; 7] = [
    "insert",
    "delete",
    "update",
    "truncate",
    "hot_update",
    "multi_insert",
    "other",
];

pub fn heap_op_index(rm: u8, info: u8) -> usize {
    use walrus::pg::walparser::RmId;
    let op = info & XLOG_HEAP_OPMASK;
    if rm == RmId::Heap as u8 {
        match op {
            XLOG_HEAP_INSERT => 0,
            XLOG_HEAP_DELETE => 1,
            XLOG_HEAP_UPDATE => 2,
            XLOG_HEAP_TRUNCATE => 3,
            XLOG_HEAP_HOT_UPDATE => 4,
            _ => 6,
        }
    } else if rm == RmId::Heap2 as u8 && op == XLOG_HEAP2_MULTI_INSERT {
        5
    } else {
        6
    }
}

/// Per-op atomic counters, index-aligned with [`HEAP_OP_LABELS`]
#[derive(Debug, Default)]
pub struct OpCounters([std::sync::atomic::AtomicU64; 7]);

impl OpCounters {
    pub fn add(&self, rm: u8, info: u8, n: u64) {
        self.0[heap_op_index(rm, info)].fetch_add(n, std::sync::atomic::Ordering::Relaxed);
    }

    pub fn bump(&self, rm: u8, info: u8) {
        self.add(rm, info, 1);
    }

    pub fn load(&self) -> [u64; 7] {
        std::array::from_fn(|i| self.0[i].load(std::sync::atomic::Ordering::Relaxed))
    }
}

// xl_heap_update.flags bit positions (heapam_xlog.h).
pub const XLH_UPDATE_CONTAINS_OLD_TUPLE: u8 = 1 << 2;
pub const XLH_UPDATE_CONTAINS_OLD_KEY: u8 = 1 << 3;
pub const XLH_UPDATE_CONTAINS_NEW_TUPLE: u8 = 1 << 4;
pub const XLH_UPDATE_PREFIX_FROM_OLD: u8 = 1 << 5;
pub const XLH_UPDATE_SUFFIX_FROM_OLD: u8 = 1 << 6;

// xl_heap_delete.flags bit positions.
pub const XLH_DELETE_CONTAINS_OLD_TUPLE: u8 = 1 << 1;
pub const XLH_DELETE_CONTAINS_OLD_KEY: u8 = 1 << 2;
pub const XLH_DELETE_IS_SUPER: u8 = 1 << 3;

// xl_heap_insert.flags bits — only XLH_INSERT_CONTAINS_NEW_TUPLE is read.
pub const XLH_INSERT_CONTAINS_NEW_TUPLE: u8 = 1 << 3;

// HeapTupleHeader infomask/infomask2 (htup_details.h).
pub const HEAP_HASNULL: u16 = 0x0001;
pub const HEAP_NATTS_MASK: u16 = 0x07FF;

#[derive(Debug, Error)]
pub enum DecodeError {
    #[error("truncated tuple at offset {offset}: need {need} bytes, have {have}")]
    Truncated {
        offset: usize,
        need: usize,
        have: usize,
    },
    #[error(
        "xl_heap_header at offset {offset} declares t_hoff={t_hoff} < SIZE_OF_HEAP_TUPLE_HEADER"
    )]
    BadHoff { offset: usize, t_hoff: u8 },
    #[error("nominal alignment char {0:?} not one of c/s/i/d")]
    BadAlign(char),
    #[error("xl_heap_update prefix/suffix bytes exceed block.data ({need} > {have})")]
    BadPrefixSuffix { need: usize, have: usize },
}

/// On-disk TOAST pointer (PG `varatt.h` `struct varatt_external`), 16 bytes,
/// unaligned in source tuple.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ToastPointer {
    pub va_rawsize: i32,
    pub va_extinfo: u32,
    pub va_valueid: u32,
    pub va_toastrelid: u32,
}

/// One decoded WAL heap record. Column count + ordering matches
/// `RelDescriptor.attributes`, i.e. attnum-1 indexed.
#[derive(Debug, Clone, PartialEq)]
pub struct DecodedHeap {
    pub rfn: RelFileNode,
    pub xid: u32,
    pub source_lsn: u64,
    pub op: HeapOp,
    pub new: Option<DecodedTuple>,
    pub old: Option<DecodedTuple>,
}

#[cfg(any(test, feature = "test-support"))]
impl DecodedHeap {
    /// Sum across both images (REPLICA IDENTITY FULL keeps a fat `old` too).
    pub fn approx_bytes(&self) -> usize {
        let image = |t: &Option<DecodedTuple>| {
            t.as_ref().map_or(0, |t| {
                t.columns
                    .iter()
                    .flatten()
                    .map(Datum::approx_bytes)
                    .sum::<usize>()
            })
        };
        image(&self.new) + image(&self.old)
    }
}

/// Decoded heap frozen to the descriptor it decoded against. Downstream
/// (detoast, replica identity, routing, truncate apply) reads the attached
/// descriptor; no `descriptor_at` after construction, so a capture landing
/// between decode and drain can't reinterpret the row. Spill serializes
/// `descriptor_valid_from` instead of the `Arc`; rehydrate re-obtains the
/// descriptor by key and fails closed on span mismatch
#[cfg(any(test, feature = "test-support"))]
#[derive(Debug, Clone, PartialEq)]
pub struct DescribedHeap {
    pub decoded: DecodedHeap,
    pub descriptor: std::sync::Arc<crate::schema::RelDescriptor>,
    /// Log entry `valid_from` of `descriptor` at attach
    pub descriptor_valid_from: u64,
}

#[cfg(any(test, feature = "test-support"))]
impl DescribedHeap {
    /// Resident accounting counts the retained `Arc` reference (contents
    /// shared, not per-row) plus the span key
    pub fn approx_bytes(&self) -> usize {
        self.decoded.approx_bytes()
            + std::mem::size_of::<std::sync::Arc<crate::schema::RelDescriptor>>()
            + std::mem::size_of::<u64>()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HeapOp {
    Insert,
    Update,
    /// `XLOG_HEAP_HOT_UPDATE`, separate op so downstream may skip: HOT
    /// updates touch no logged index, visible row identity unchanged.
    HotUpdate,
    Delete,
    /// `XLOG_HEAP_TRUNCATE`, relation-wide, no tuple payload. PG emits one
    /// record per relid in the truncated set; walshadow fans out one
    /// `DecodedHeap` per relid before reaching the buffer.
    Truncate,
}

#[cfg(any(test, feature = "test-support"))]
/// One drained tuple, fully reassembled. `commit_ts` is the commit-record
/// `xact_time` carried through
/// [`crate::xact::xact_buffer::XactBuffer::drain_committed`].
#[derive(Debug, Clone, PartialEq)]
pub struct CommittedTuple {
    pub decoded: DecodedHeap,
    /// PG `TimestampTz` from xact commit record (micros since PG epoch
    /// 2000-01-01). 0 when commit record lacked field or hasn't arrived
    /// (decoder unbuffered path).
    pub commit_ts: i64,
    /// Source LSN of matching `XLOG_XACT_COMMIT`. CH emitter snapshots onto
    /// its ack-LSN gauge once the xact's `send_data(None)` finishes; daemon
    /// status loop writes it to cursor file `emitter_ack_lsn`. 0 for
    /// decoder unbuffered path.
    pub commit_lsn: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DecodedTuple {
    /// Indexed by attnum-1. `None` = absent from WAL (PG UPDATE prefix/suffix
    /// compression skipped it, or this `relreplident` shape never carries it).
    /// `Some(Datum::Null)` is explicit NULL (bitmap bit clear).
    pub columns: Vec<Option<Datum>>,
    /// True iff a column was elided by PG prefix/suffix compression; xact
    /// buffer backfills from previous tuple image.
    pub partial: bool,
}

/// Builds one tuple image from walked attributes: owned datums for
/// [`DecodedTuple`], borrowed for [`TupleView`]
pub trait TupleBuild<'a>: Sized {
    fn with_capacity(n: usize) -> Self;
    fn push(&mut self, att: &RelAttr, datum: DatumRef<'a>);
    fn set_partial(&mut self);
}

impl<'a> TupleBuild<'a> for DecodedTuple {
    fn with_capacity(n: usize) -> Self {
        Self {
            columns: Vec::with_capacity(n),
            partial: false,
        }
    }

    #[inline(always)]
    fn push(&mut self, att: &RelAttr, datum: DatumRef<'a>) {
        self.columns.push(resolve(att, datum));
    }

    fn set_partial(&mut self) {
        self.partial = true;
    }
}

/// Tuple image borrowing its record. Indexed by attnum-1 like
/// [`DecodedTuple::columns`]
#[derive(Clone, Debug, Default, PartialEq)]
pub struct TupleView<'a> {
    pub datums: Vec<DatumRef<'a>>,
    pub partial: bool,
}

impl<'a> TupleBuild<'a> for TupleView<'a> {
    fn with_capacity(n: usize) -> Self {
        Self {
            datums: Vec::with_capacity(n),
            partial: false,
        }
    }

    fn push(&mut self, _: &RelAttr, datum: DatumRef<'a>) {
        self.datums.push(datum);
    }

    fn set_partial(&mut self) {
        self.partial = true;
    }
}

#[cfg(test)]
impl TupleView<'_> {
    /// Owned datums, as [`decode_heap_record`] builds them
    pub fn to_tuple(&self, rel: &RelDescriptor) -> DecodedTuple {
        DecodedTuple {
            columns: self
                .datums
                .iter()
                .zip(&rel.attributes)
                .map(|(d, att)| resolve(att, *d))
                .collect(),
            partial: self.partial,
        }
    }
}

/// Tuple walked for validity and shape only, building nothing
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TupleShape {
    pub partial: bool,
}

impl TupleBuild<'_> for TupleShape {
    fn with_capacity(_: usize) -> Self {
        Self::default()
    }

    fn push(&mut self, _: &RelAttr, _: DatumRef<'_>) {}

    fn set_partial(&mut self) {
        self.partial = true;
    }
}

/// Images of one heap tuple change, before identity and position attach
#[derive(Clone, Debug, PartialEq)]
pub struct HeapImages<T> {
    pub op: HeapOp,
    pub new: Option<T>,
    pub old: Option<T>,
}

/// Top-level decode entry point. One-element [`DecodedHeaps`] for
/// INSERT/UPDATE/HOT_UPDATE/DELETE, N-element for `XLOG_HEAP2_MULTI_INSERT`,
/// empty for ops with no tuple payload (LOCK, INPLACE, TRUNCATE, other
/// `RM_HEAP2_ID` ops, MULTI_INSERT lacking `XLH_INSERT_CONTAINS_NEW_TUPLE`).
/// Unrecognised op codes skip silently; only malformed bytes return `Err`.
///
/// `rel` must describe `record.blocks[0].header.location.rel`, fetched via
/// [`DescriptorLog::descriptor_at`](crate::catalog::desc_log::DescriptorLog::descriptor_at).
pub fn decode_heap_record(
    record: &XLogRecord,
    source_lsn: u64,
    rel: &RelDescriptor,
) -> Result<DecodedHeaps, DecodeError> {
    let rfn = record
        .blocks
        .first()
        .map(|b| b.header.location.rel)
        .unwrap_or_default();
    let xid = record.header.xact_id;
    Ok(parse_heap_record::<DecodedTuple>(record.into(), rel)?
        .into_iter()
        .map(|images| DecodedHeap {
            rfn,
            xid,
            source_lsn,
            op: images.op,
            new: images.new,
            old: images.old,
        })
        .collect())
}

/// [`decode_heap_record`] borrowing attribute bytes from `record`
pub fn view_heap_record<'a>(
    record: impl Into<HeapRecordBytes<'a>>,
    rel: &RelDescriptor,
) -> Result<SmallVec<[HeapImages<TupleView<'a>>; 1]>, DecodeError> {
    parse_heap_record(record.into(), rel)
}

/// Record fields heap decode reads
#[derive(Clone, Copy, Debug)]
pub struct HeapRecordBytes<'a> {
    pub rm: u8,
    pub info: u8,
    pub main_data: &'a [u8],
    /// Block 0 registered data
    pub block: Option<&'a [u8]>,
}

impl<'a> From<&'a XLogRecord<'_>> for HeapRecordBytes<'a> {
    fn from(record: &'a XLogRecord<'_>) -> Self {
        Self {
            rm: record.header.resource_manager_id,
            info: record.header.info,
            main_data: &record.main_data,
            block: record.blocks.first().map(|b| &b.data[..]),
        }
    }
}

/// Heap record summary read from its headers, tuple bytes unchecked
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RecordShape {
    pub op: HeapOp,
    pub tuples: u16,
    /// PG elided an update's prefix or suffix
    pub partial: bool,
}

/// Shape of a record [`view_heap_record`] yields images for, `None` when it
/// yields none
pub fn record_shape<'a>(
    record: impl Into<HeapRecordBytes<'a>>,
) -> Result<Option<RecordShape>, DecodeError> {
    let record = record.into();
    let md = record.main_data;
    let heap2 = record.rm == RmId::Heap2 as u8;
    let (op, need) = match record.info & XLOG_HEAP_OPMASK {
        XLOG_HEAP2_MULTI_INSERT if heap2 => (HeapOp::Insert, SIZE_OF_HEAP_MULTI_INSERT),
        _ if heap2 || record.rm != RmId::Heap as u8 => return Ok(None),
        XLOG_HEAP_INSERT => (HeapOp::Insert, 0),
        XLOG_HEAP_UPDATE => (HeapOp::Update, SIZE_OF_HEAP_UPDATE),
        XLOG_HEAP_HOT_UPDATE => (HeapOp::HotUpdate, SIZE_OF_HEAP_UPDATE),
        XLOG_HEAP_DELETE => (HeapOp::Delete, SIZE_OF_HEAP_DELETE),
        _ => return Ok(None),
    };
    if md.len() < need {
        return Err(DecodeError::Truncated {
            offset: 0,
            need,
            have: md.len(),
        });
    }
    let mut shape = RecordShape {
        op,
        tuples: 1,
        partial: false,
    };
    if heap2 {
        shape.tuples = u16::from_le_bytes([md[2], md[3]]);
        if shape.tuples == 0 {
            return Err(DecodeError::Truncated {
                offset: 0,
                need: 1,
                have: 0,
            });
        }
        if md[0] & XLH_INSERT_CONTAINS_NEW_TUPLE == 0 {
            return Ok(None);
        }
    } else if matches!(op, HeapOp::Update | HeapOp::HotUpdate) {
        shape.partial = md[7] & (XLH_UPDATE_PREFIX_FROM_OLD | XLH_UPDATE_SUFFIX_FROM_OLD) != 0;
    }
    Ok(Some(shape))
}

fn parse_heap_record<'a, T: TupleBuild<'a>>(
    record: HeapRecordBytes<'a>,
    rel: &RelDescriptor,
) -> Result<SmallVec<[HeapImages<T>; 1]>, DecodeError> {
    let rm = record.rm;
    let info_op = record.info & XLOG_HEAP_OPMASK;
    if rm == RmId::Heap2 as u8 && info_op == XLOG_HEAP2_MULTI_INSERT {
        return decode_multi_insert(record, rel);
    }
    if rm != RmId::Heap as u8 {
        return Ok(SmallVec::new());
    }
    match info_op {
        XLOG_HEAP_INSERT => Ok(smallvec![decode_insert(record, rel)?]),
        XLOG_HEAP_UPDATE => Ok(smallvec![decode_update(record, rel, false)?]),
        XLOG_HEAP_HOT_UPDATE => Ok(smallvec![decode_update(record, rel, true)?]),
        XLOG_HEAP_DELETE => Ok(smallvec![decode_delete(record, rel)?]),
        _ => Ok(SmallVec::new()),
    }
}

/// PG `SizeOfHeapMultiInsert = offsetof(xl_heap_multi_insert, offsets)`:
/// `flags:u8 + pad:u8 + ntuples:u16`. 1-byte pad 2-byte-aligns ntuples.
pub const SIZE_OF_HEAP_MULTI_INSERT: usize = 4;
/// PG `SizeOfMultiInsertTuple`, `xl_multi_insert_tuple` on-wire:
/// `datalen:u16 + t_infomask2:u16 + t_infomask:u16 + t_hoff:u8`.
pub const SIZE_OF_MULTI_INSERT_TUPLE: usize = 7;

/// Layered on the op nibble. Set => writer omits offset array (tuples land at
/// sequential `FirstOffsetNumber + i`), PG `heapam.c::heap_multi_insert`.
pub const XLOG_HEAP_INIT_PAGE: u8 = 0x80;

fn decode_multi_insert<'a, T: TupleBuild<'a>>(
    record: HeapRecordBytes<'a>,
    rel: &RelDescriptor,
) -> Result<SmallVec<[HeapImages<T>; 1]>, DecodeError> {
    let md = record.main_data;
    if md.len() < SIZE_OF_HEAP_MULTI_INSERT {
        return Err(DecodeError::Truncated {
            offset: 0,
            need: SIZE_OF_HEAP_MULTI_INSERT,
            have: md.len(),
        });
    }
    let flags = md[0];
    // md[1] is pad
    let ntuples = u16::from_le_bytes([md[2], md[3]]) as usize;
    if ntuples == 0 {
        // Malformed per PG assert path; bail so corrupt stream surfaces
        return Err(DecodeError::Truncated {
            offset: 0,
            need: 1,
            have: 0,
        });
    }
    // No CONTAINS_NEW_TUPLE => tuple bytes stripped (writer not at
    // wal_level=logical). Skip; walshadow floor is logical so paranoia check
    if flags & XLH_INSERT_CONTAINS_NEW_TUPLE == 0 {
        return Ok(SmallVec::new());
    }
    let init_page = record.info & XLOG_HEAP_INIT_PAGE != 0;
    // Non-init_page: main_data continues with offsets[ntuples] of u16.
    // Values unneeded (block 0 carries tuples in order); length-check only
    let expected_md_min = if init_page {
        SIZE_OF_HEAP_MULTI_INSERT
    } else {
        SIZE_OF_HEAP_MULTI_INSERT + ntuples * 2
    };
    if md.len() < expected_md_min {
        return Err(DecodeError::Truncated {
            offset: SIZE_OF_HEAP_MULTI_INSERT,
            need: expected_md_min,
            have: md.len(),
        });
    }

    let Some(data) = record.block else {
        return Ok(SmallVec::new());
    };
    let mut cursor = 0usize;
    let mut out = SmallVec::with_capacity(ntuples);
    for _ in 0..ntuples {
        // Each xl_multi_insert_tuple SHORTALIGN'd (2-byte) at write time,
        // PG `heap_xlog_multi_insert`
        cursor = align_up(cursor, 2);
        if data.len() < cursor + SIZE_OF_MULTI_INSERT_TUPLE {
            return Err(DecodeError::Truncated {
                offset: cursor,
                need: SIZE_OF_MULTI_INSERT_TUPLE,
                have: data.len().saturating_sub(cursor),
            });
        }
        let datalen = u16::from_le_bytes([data[cursor], data[cursor + 1]]) as usize;
        // Multi-insert strips the 5-byte xl_heap_header at write time and
        // stores (t_infomask2, t_infomask, t_hoff) on xl_multi_insert_tuple
        let header = TupleHeader {
            offset: cursor,
            infomask2: u16::from_le_bytes([data[cursor + 2], data[cursor + 3]]),
            infomask: u16::from_le_bytes([data[cursor + 4], data[cursor + 5]]),
            hoff: data[cursor + 6],
        };
        cursor += SIZE_OF_MULTI_INSERT_TUPLE;
        if data.len() < cursor + datalen {
            return Err(DecodeError::Truncated {
                offset: cursor,
                need: datalen,
                have: data.len().saturating_sub(cursor),
            });
        }
        let mut tup = TupleBuild::with_capacity(rel.attributes.len());
        walk_tuple(&data[..cursor + datalen], header, cursor, rel, 0, &mut tup)?;
        cursor += datalen;
        out.push(HeapImages {
            op: HeapOp::Insert,
            new: Some(tup),
            old: None,
        });
    }
    Ok(out)
}

fn decode_insert<'a, T: TupleBuild<'a>>(
    record: HeapRecordBytes<'a>,
    rel: &RelDescriptor,
) -> Result<HeapImages<T>, DecodeError> {
    // PG always references target page; emit empty new rather than fail
    // the stream
    let new = match record.block {
        Some(data) => Some(decode_tuple_payload(data, 0, rel, 0)?),
        None => None,
    };
    Ok(HeapImages {
        op: HeapOp::Insert,
        new,
        old: None,
    })
}

fn decode_update<'a, T: TupleBuild<'a>>(
    record: HeapRecordBytes<'a>,
    rel: &RelDescriptor,
    hot: bool,
) -> Result<HeapImages<T>, DecodeError> {
    if record.main_data.len() < SIZE_OF_HEAP_UPDATE {
        return Err(DecodeError::Truncated {
            offset: 0,
            need: SIZE_OF_HEAP_UPDATE,
            have: record.main_data.len(),
        });
    }
    let flags = record.main_data[7]; // xl_heap_update.flags offset
    let has_prefix = flags & XLH_UPDATE_PREFIX_FROM_OLD != 0;
    let has_suffix = flags & XLH_UPDATE_SUFFIX_FROM_OLD != 0;
    let has_old_tuple = flags & XLH_UPDATE_CONTAINS_OLD_TUPLE != 0;
    let has_old_key = flags & XLH_UPDATE_CONTAINS_OLD_KEY != 0;

    let new = if let Some(data) = record.block {
        let mut cursor = 0;
        let prefixlen = if has_prefix {
            read_u16(data, &mut cursor)? as usize
        } else {
            0
        };
        // Suffix bytes are past the WAL slice, so its length goes unread
        if has_suffix {
            read_u16(data, &mut cursor)?;
        }
        let mut t: T = decode_tuple_payload(data, cursor, rel, prefixlen)?;
        if has_prefix || has_suffix {
            t.set_partial();
        }
        Some(t)
    } else {
        None
    };

    let old = if has_old_tuple || has_old_key {
        decode_old_tuple_from_main_data(record.main_data, SIZE_OF_HEAP_UPDATE, rel)?
    } else {
        None
    };

    let op = if hot {
        HeapOp::HotUpdate
    } else {
        HeapOp::Update
    };
    Ok(HeapImages { op, new, old })
}

fn decode_delete<'a, T: TupleBuild<'a>>(
    record: HeapRecordBytes<'a>,
    rel: &RelDescriptor,
) -> Result<HeapImages<T>, DecodeError> {
    if record.main_data.len() < SIZE_OF_HEAP_DELETE {
        return Err(DecodeError::Truncated {
            offset: 0,
            need: SIZE_OF_HEAP_DELETE,
            have: record.main_data.len(),
        });
    }
    let flags = record.main_data[7]; // xl_heap_delete.flags (after xmax:4 + offnum:2 + infobits:1)
    let has_old = flags & (XLH_DELETE_CONTAINS_OLD_TUPLE | XLH_DELETE_CONTAINS_OLD_KEY) != 0;
    let old = if has_old {
        decode_old_tuple_from_main_data(record.main_data, SIZE_OF_HEAP_DELETE, rel)?
    } else {
        None
    };
    Ok(HeapImages {
        op: HeapOp::Delete,
        new: None,
        old,
    })
}

fn decode_old_tuple_from_main_data<'a, T: TupleBuild<'a>>(
    main_data: &'a [u8],
    header_off: usize,
    rel: &RelDescriptor,
) -> Result<Option<T>, DecodeError> {
    Ok(Some(decode_tuple_payload(main_data, header_off, rel, 0)?))
}

/// Owned cells of a `block.data`-shaped tuple
#[cfg(any(test, feature = "test-support"))]
pub(crate) fn decode_block_data(
    block_data: &[u8],
    rel: &RelDescriptor,
) -> Result<DecodedTuple, DecodeError> {
    decode_tuple_payload(block_data, 0, rel, 0)
}

/// Tuple of a `block.data`-shaped buffer, borrowing attribute bytes
pub fn view_block_data<'a>(
    block_data: &'a [u8],
    rel: &RelDescriptor,
) -> Result<TupleView<'a>, DecodeError> {
    decode_tuple_payload(block_data, 0, rel, 0)
}

/// [`view_block_data`] refilling `out`, sparing a bulk loop the return copy
pub fn view_block_data_into<'a>(
    block_data: &'a [u8],
    rel: &RelDescriptor,
    out: &mut TupleView<'a>,
) -> Result<(), DecodeError> {
    out.datums.clear();
    out.partial = false;
    decode_tuple_payload_into(block_data, 0, rel, 0, out)
}

/// Validate a `block.data`-shaped buffer without building attributes
pub fn scan_block_data(block_data: &[u8], rel: &RelDescriptor) -> Result<TupleShape, DecodeError> {
    decode_tuple_payload(block_data, 0, rel, 0)
}

/// `block.data` image of `values` as `heap_fill_tuple` lays them out,
/// aligned per `attrs`. Values past them align by width
#[cfg(any(test, feature = "test-support"))]
pub fn form_block_data(attrs: &[RelAttr], values: &[Option<Datum>]) -> Vec<u8> {
    let natts = values.len();
    let has_null = values.iter().any(|v| matches!(v, None | Some(Datum::Null)));
    let bitmap_len = if has_null { natts.div_ceil(8) } else { 0 };
    let hoff = (SIZE_OF_HEAP_TUPLE_HEADER + bitmap_len).next_multiple_of(8);
    let mut bitmap = vec![0u8; bitmap_len];
    let mut data = Vec::new();
    for (i, v) in values.iter().enumerate() {
        let align_of = |len: usize| {
            attrs
                .get(i)
                .map_or(if len.is_power_of_two() { len.min(8) } else { 1 }, |a| {
                    align_bytes(a.type_align)
                })
        };
        let (bytes, align) = match v {
            None | Some(Datum::Null) => continue,
            Some(Datum::Fixed(b)) => (b.to_vec(), align_of(b.len())),
            Some(Datum::Varlena(b) | Datum::BadTag(b) | Datum::Text(b)) if b.len() < 127 => {
                let mut v = vec![(((b.len() + 1) << 1) | 1) as u8];
                v.extend_from_slice(b);
                (v, 1)
            }
            Some(Datum::Varlena(b) | Datum::BadTag(b) | Datum::Text(b)) => {
                let mut v = (((b.len() + 4) as u32) << 2).to_le_bytes().to_vec();
                v.extend_from_slice(b);
                (v, align_of(4).max(4))
            }
            Some(Datum::Cstring(b)) => {
                let mut v = b.clone();
                v.push(0);
                (v, 1)
            }
            Some(Datum::External(p)) => {
                let mut v = vec![0x01, 18];
                for word in [
                    p.va_rawsize as u32,
                    p.va_extinfo,
                    p.va_valueid,
                    p.va_toastrelid,
                ] {
                    v.extend_from_slice(&word.to_le_bytes());
                }
                (v, 1)
            }
        };
        if has_null {
            bitmap[i / 8] |= 1 << (i % 8);
        }
        data.resize(data.len().next_multiple_of(align), 0);
        data.extend_from_slice(&bytes);
    }
    let mut out = (natts as u16).to_le_bytes().to_vec();
    out.extend_from_slice(&if has_null { HEAP_HASNULL } else { 0 }.to_le_bytes());
    out.push(hoff as u8);
    out.extend_from_slice(&bitmap);
    out.resize(SIZE_OF_HEAP_HEADER + hoff - SIZE_OF_HEAP_TUPLE_HEADER, 0);
    out.extend_from_slice(&data);
    out
}

/// Fields PG strips into `xl_heap_header` or `xl_multi_insert_tuple`
#[derive(Clone, Copy)]
struct TupleHeader {
    /// Header position, for errors
    offset: usize,
    infomask2: u16,
    infomask: u16,
    hoff: u8,
}

/// Walk a tuple payload (`xl_heap_header` at `header_off`, then bitmap,
/// padding, col data). `prefixlen` is PG `XLH_UPDATE_PREFIX_FROM_OLD` byte
/// count elided from front of logical col-data region; columns wholly inside
/// it or past end of `buf` surface absent and caller flips `partial`.
/// Bitmap + xl_heap_header precede prefix elision so are always present at
/// `header_off`.
#[inline]
fn decode_tuple_payload<'a, T: TupleBuild<'a>>(
    buf: &'a [u8],
    header_off: usize,
    rel: &RelDescriptor,
    prefixlen: usize,
) -> Result<T, DecodeError> {
    let mut out = T::with_capacity(rel.attributes.len());
    decode_tuple_payload_into(buf, header_off, rel, prefixlen, &mut out)?;
    Ok(out)
}

fn decode_tuple_payload_into<'a, T: TupleBuild<'a>>(
    buf: &'a [u8],
    header_off: usize,
    rel: &RelDescriptor,
    prefixlen: usize,
    out: &mut T,
) -> Result<(), DecodeError> {
    if buf.len() < header_off + SIZE_OF_HEAP_HEADER {
        return Err(DecodeError::Truncated {
            offset: header_off,
            need: SIZE_OF_HEAP_HEADER,
            have: buf.len().saturating_sub(header_off),
        });
    }
    let header = TupleHeader {
        offset: header_off,
        infomask2: u16::from_le_bytes([buf[header_off], buf[header_off + 1]]),
        infomask: u16::from_le_bytes([buf[header_off + 2], buf[header_off + 3]]),
        hoff: buf[header_off + 4],
    };
    walk_tuple(
        buf,
        header,
        header_off + SIZE_OF_HEAP_HEADER,
        rel,
        prefixlen,
        out,
    )
}

/// Walk bitmap, padding and col data starting at `bitmap_off`, one datum per
/// attribute of `rel`. `buf` ends where tuple ends
fn walk_tuple<'a, T: TupleBuild<'a>>(
    buf: &'a [u8],
    header: TupleHeader,
    bitmap_off: usize,
    rel: &RelDescriptor,
    prefixlen: usize,
    out: &mut T,
) -> Result<(), DecodeError> {
    let t_hoff = usize::from(header.hoff);
    if t_hoff < SIZE_OF_HEAP_TUPLE_HEADER {
        return Err(DecodeError::BadHoff {
            offset: header.offset,
            t_hoff: header.hoff,
        });
    }
    let natts = (header.infomask2 & HEAP_NATTS_MASK) as usize;
    let has_null = header.infomask & HEAP_HASNULL != 0;

    let bitmap_bytes = if has_null { natts.div_ceil(8) } else { 0 };
    // PG tuple: header(23) -> bitmap -> MAXALIGN(8) pad -> col data at t_hoff.
    // WAL slice elides the 23-byte fixed header, so bitmap leads the slice
    if buf.len() < bitmap_off + bitmap_bytes {
        return Err(DecodeError::Truncated {
            offset: bitmap_off,
            need: bitmap_bytes,
            have: buf.len().saturating_sub(bitmap_off),
        });
    }
    let bitmap = &buf[bitmap_off..bitmap_off + bitmap_bytes];

    // Bytes between SizeofHeapTupleHeader and t_hoff (bitmap + align pad) are
    // carried verbatim in WAL
    let col_data_off = bitmap_off + (t_hoff - SIZE_OF_HEAP_TUPLE_HEADER);

    let attrs = &rel.attributes;
    // Mark this + every later col absent
    let absent_from = |out: &mut T, idx: usize| {
        for att in &attrs[idx..] {
            out.push(att, DatumRef::Absent);
        }
        out.set_partial();
    };
    // `cur` is offset within the *logical* col-data region (relative to
    // t_hoff); align matches PG `att_align_nominal` against this offset. t_hoff
    // is MAXALIGN'd(8) so col 1 starts clean. WAL offset of logical `cur` is
    // `col_data_off + (cur - prefixlen)` when `cur >= prefixlen`
    let mut cur: usize = 0;
    let wal_col_data_avail = buf.len().saturating_sub(col_data_off);
    let logical_end = wal_col_data_avail + prefixlen;
    for (idx, att) in attrs.iter().enumerate() {
        if idx >= natts {
            // wal natts < catalog natts: trailing cols added via ALTER ADD
            // COLUMN after the record. Mirror PG `getmissingattr`
            // (heaptuple.c) `attmissingval[1]` fast-path default
            out.push(att, DatumRef::Missing);
            continue;
        }
        if has_null && !bitmap_is_set(bitmap, idx) {
            out.push(att, DatumRef::Null);
            continue;
        }
        let col_size_hint = att.type_len;
        // Varlena align needs a peek at the WAL byte; in-prefix varlena has no
        // byte to peek so fall back to nominal align (PG worst-case budget)
        if col_size_hint == -1 && cur >= prefixlen && col_data_off + cur - prefixlen < buf.len() {
            cur = att_align_nominal(
                cur,
                att.type_align,
                col_size_hint,
                buf,
                col_data_off + cur - prefixlen,
            );
        } else {
            // Can't peek (in prefix / past EOF); prefix bytes are themselves
            // aligned in the source tuple so nominal align matches PG
            cur = align_for(cur, att.type_align);
        }

        if cur < prefixlen || cur >= logical_end {
            // In prefix, or suffix / past EOF. Advance cur for fixed-width
            // accounting only; varlena width needs its bytes so later cols
            // stay absent too
            let Some(n) = decoded_size_or_skip(att) else {
                absent_from(out, idx);
                return Ok(());
            };
            out.push(att, DatumRef::Absent);
            cur += n;
            continue;
        }

        let abs = col_data_off + (cur - prefixlen);
        if att.type_len > 0 && !att.dropped {
            let len = att.type_len as usize;
            if let Some(body) = buf.get(abs..abs + len) {
                out.push(att, DatumRef::Fixed(body));
                cur += len;
                continue;
            }
        }
        let (datum, consumed) = match read_datum(att, buf, abs) {
            Ok(v) => v,
            Err(DecodeError::Truncated { .. }) => {
                absent_from(out, idx);
                return Ok(());
            }
            Err(e) => return Err(e),
        };
        out.push(att, datum);
        cur += consumed;
    }
    Ok(())
}

/// Column's fast default ([`RelAttr::missing_default`]) as stored or as
/// `typoutput` text; `Null` when there is none
pub fn missing_datum(att: &RelAttr) -> DatumRef<'_> {
    match &att.missing_default {
        Some(MissingDefault::Raw(arr)) => missing_array_element(att, arr),
        Some(MissingDefault::Text(text)) => DatumRef::Text(text.as_bytes()),
        None => DatumRef::Null,
    }
}

/// Raw and text spell one default differently, so compare parsed datums.
/// Text no local parser reads stays pending; equal source type counts as equal
pub fn missing_defaults_equivalent(a: &RelAttr, b: &RelAttr) -> bool {
    #[derive(PartialEq)]
    enum Image {
        Value(Option<Datum>),
        Pending(u32),
    }
    let image = |att: &RelAttr| match missing_datum(att) {
        DatumRef::Text(text) => match text_datum(att.type_oid, text) {
            TextDatum::Datum(d) => Image::Value(Some(d)),
            TextDatum::Null => Image::Value(Some(Datum::Null)),
            TextDatum::Opaque if is_text_type(att.type_oid) => {
                Image::Value(Some(Datum::Varlena(text.to_vec())))
            }
            TextDatum::Opaque => Image::Pending(att.type_oid),
        },
        // Text-typed defaults compare as their text
        d if local_matrix_covers(att.type_oid, att.type_len) => {
            Image::Value(resolve(att, d).map(|d| {
                match (d, att.type_oid) {
                    (Datum::Fixed(b), NAMEOID) => {
                        let end = b.iter().position(|&c| c == 0).unwrap_or(b.len());
                        Datum::Varlena(b[..end].to_vec())
                    }
                    (Datum::Varlena(b), JSONBOID) => crate::decode::codecs::decode_jsonb(&b)
                        .map_or(Datum::BadTag(b), |text| Datum::Varlena(text.into_bytes())),
                    (d, _) => d,
                }
            }))
        }
        DatumRef::Null => Image::Value(Some(Datum::Null)),
        _ => Image::Pending(att.type_oid),
    };
    image(a) == image(b)
}

/// `attmissingval[1]`, an `ArrayType` with one non-null element
fn missing_array_element<'a>(att: &RelAttr, arr: &'a [u8]) -> DatumRef<'a> {
    // ArrayType: vl_len_(4) ndim(4) dataoffset(4) elemtype(4) dims[] lbounds[] [nulls] data
    let element = || -> Option<DatumRef<'a>> {
        let ndim = i32::from_le_bytes(arr.get(4..8)?.try_into().ok()?);
        let dataoffset = i32::from_le_bytes(arr.get(8..12)?.try_into().ok()?);
        if ndim != 1 {
            return None;
        }
        let abs = if dataoffset != 0 {
            usize::try_from(dataoffset).ok()?
        } else {
            // MAXALIGN(16 + 8*ndim), no null bitmap
            (16 + 8 * ndim as usize).next_multiple_of(8)
        };
        read_datum(att, arr, abs).ok().map(|(d, _)| d)
    };
    element().unwrap_or(DatumRef::Null)
}

fn is_text_type(type_oid: u32) -> bool {
    matches!(
        type_oid,
        TEXTOID | VARCHAROID | BPCHAROID | NAMEOID | JSONOID | JSONBOID
    )
}

/// `typoutput` text read back into a stored datum
#[derive(Debug, PartialEq)]
pub enum TextDatum {
    Datum(Datum),
    /// Text the type's parser rejects
    Null,
    /// No local parser; String targets take the text as is
    Opaque,
}

/// Parse `typoutput` text of types with a local reader into stored form
pub fn text_datum(type_oid: u32, text: &[u8]) -> TextDatum {
    let Ok(text) = simdutf8::basic::from_utf8(text) else {
        return TextDatum::Opaque;
    };
    fn fixed<B: AsRef<[u8]>>(b: Option<B>) -> TextDatum {
        b.map_or(TextDatum::Null, |b| {
            TextDatum::Datum(Datum::fixed(b.as_ref()))
        })
    }
    match type_oid {
        BOOLOID => fixed(Some([u8::from(matches!(
            text,
            "t" | "true" | "yes" | "on" | "1"
        ))])),
        CHAROID => fixed(text.as_bytes().first().map(|b| [*b])),
        INT2OID => fixed(text.parse::<i16>().ok().map(i16::to_le_bytes)),
        INT4OID => fixed(text.parse::<i32>().ok().map(i32::to_le_bytes)),
        INT8OID => fixed(text.parse::<i64>().ok().map(i64::to_le_bytes)),
        FLOAT4OID => fixed(text.parse::<f32>().ok().map(f32::to_le_bytes)),
        FLOAT8OID => fixed(text.parse::<f64>().ok().map(f64::to_le_bytes)),
        OIDOID | XIDOID | CIDOID => fixed(text.parse::<u32>().ok().map(u32::to_le_bytes)),
        OID8OID | XID8OID => fixed(text.parse::<u64>().ok().map(u64::to_le_bytes)),
        PG_LSNOID => fixed(crate::pg::parse_pg_lsn(text).ok().map(u64::to_le_bytes)),
        MACADDROID => fixed(crate::decode::codecs::parse_macaddr::<6>(text)),
        MACADDR8OID => fixed(crate::decode::codecs::parse_macaddr::<8>(text)),
        TIDOID => fixed(
            crate::decode::codecs::parse_tid(text)
                .map(|(block, offset)| crate::decode::codecs::encode_tid(block, offset)),
        ),
        NUMERICOID => crate::decode::codecs::numeric_from_text(text)
            .map_or(TextDatum::Null, |b| TextDatum::Datum(Datum::Varlena(b))),
        _ => TextDatum::Opaque,
    }
}

/// Byte width for cursor accounting when bytes are unreadable (in prefix /
/// past EOF). `None` for varlena (width is content-dependent), caller stops.
fn decoded_size_or_skip(att: &RelAttr) -> Option<usize> {
    if att.type_len > 0 {
        Some(att.type_len as usize)
    } else {
        None
    }
}

/// MAXALIGN dispatch over `pg_type.typalign`, for in-prefix / past-EOF where
/// we can't peek to detect a short varlena header (`att_align_pointer`).
#[cfg(any(test, feature = "test-support"))]
fn align_bytes(attalign: char) -> usize {
    match attalign {
        's' => 2,
        'i' => 4,
        'd' => 8,
        _ => 1,
    }
}

fn align_for(cur: usize, attalign: char) -> usize {
    match attalign {
        's' => align_up(cur, 2),
        'i' => align_up(cur, 4),
        'd' => align_up(cur, 8),
        // TYPALIGN_CHAR, unknown: no-align rather than panic
        _ => cur,
    }
}

/// PG `att_align_nominal` (tupmacs.h). For varlena, PG `att_align_pointer`
/// skips alignment when next byte is a 1-byte short-header datum
/// (`!VARATT_NOT_PAD_BYTE`); mirror by peeking `buf[abs]` before aligning.
fn att_align_nominal(
    cur_offset: usize,
    attalign: char,
    attlen: i16,
    buf: &[u8],
    abs_offset: usize,
) -> usize {
    if attlen == -1 && abs_offset < buf.len() && buf[abs_offset] != 0 {
        // Non-zero first byte = short-header or aligned 4B header, both safe
        // unaligned; zero byte must be padding
        return cur_offset;
    }
    match attalign {
        's' => align_up(cur_offset, 2), // TYPALIGN_SHORT
        'i' => align_up(cur_offset, 4), // TYPALIGN_INT
        'd' => align_up(cur_offset, 8), // TYPALIGN_DOUBLE
        _ => cur_offset,                // TYPALIGN_CHAR, unknown: no-align
    }
}

fn align_up(n: usize, by: usize) -> usize {
    (n + by - 1) & !(by - 1)
}

fn bitmap_is_set(bitmap: &[u8], bit: usize) -> bool {
    bitmap
        .get(bit / 8)
        .map(|byte| (byte >> (bit & 7)) & 1 == 1)
        .unwrap_or(false)
}

fn read_u16(buf: &[u8], cur: &mut usize) -> Result<u16, DecodeError> {
    if buf.len() < *cur + 2 {
        return Err(DecodeError::Truncated {
            offset: *cur,
            need: 2,
            have: buf.len().saturating_sub(*cur),
        });
    }
    let v = u16::from_le_bytes(buf[*cur..*cur + 2].try_into().unwrap());
    *cur += 2;
    Ok(v)
}

/// Read one attribute at `abs`, returning datum and on-disk length. Varlena
/// reports header + body, including TOAST pointer's full 18 bytes
pub(crate) fn read_datum<'a>(
    att: &RelAttr,
    buf: &'a [u8],
    abs: usize,
) -> Result<(DatumRef<'a>, usize), DecodeError> {
    let (datum, len) = match att.type_len {
        -1 => read_varlena(buf, abs)?,
        -2 => read_cstring(buf, abs)?,
        len => {
            let len = len as usize;
            let body = buf.get(abs..abs + len).ok_or(DecodeError::Truncated {
                offset: abs,
                need: len,
                have: buf.len().saturating_sub(abs),
            })?;
            (DatumRef::Fixed(body), len)
        }
    };
    // Dropped cols retain attlen/attalign/typbyval per catalog convention, so
    // reading still advances cursor
    Ok((
        if att.dropped {
            DatumRef::Dropped
        } else {
            datum
        },
        len,
    ))
}

/// Owned copy of `datum`, `None` when absent from WAL image. Fast defaults
/// resolve, stored where a local reader parses their text, and inline
/// compressed bodies inflate
pub fn resolve(att: &RelAttr, datum: DatumRef<'_>) -> Option<Datum> {
    Some(match datum {
        DatumRef::Absent => return None,
        DatumRef::Null | DatumRef::Dropped => Datum::Null,
        DatumRef::Missing => match missing_datum(att) {
            DatumRef::Text(t) => match text_datum(att.type_oid, t) {
                TextDatum::Datum(d) => d,
                TextDatum::Null => Datum::Null,
                TextDatum::Opaque => Datum::Text(t.to_vec()),
            },
            d => return resolve(att, d),
        },
        DatumRef::Fixed(b) => Datum::fixed(b),
        DatumRef::Varlena(b) => Datum::Varlena(b.to_vec()),
        DatumRef::Compressed(datum) => {
            inflate(datum).map_or_else(|| Datum::BadTag(datum.to_vec()), Datum::Varlena)
        }
        DatumRef::External(p) => Datum::External(p),
        DatumRef::Cstring(b) => Datum::Cstring(b.to_vec()),
        DatumRef::BadTag(b) => Datum::BadTag(b.to_vec()),
        DatumRef::Text(b) => Datum::Text(b.to_vec()),
    })
}

/// Body of an inline compressed varlena, `None` on a corrupt stream
pub(crate) fn inflate(datum: &[u8]) -> Option<Vec<u8>> {
    // Reader checked datum spans header and va_tcinfo
    let tcinfo = u32::from_le_bytes(datum.get(4..8)?.try_into().ok()?);
    let method = (tcinfo >> VARLENA_EXTSIZE_BITS) as u8;
    let raw_len = (tcinfo & VARLENA_EXTSIZE_MASK) as usize;
    decompress_varlena(method, &datum[8..], raw_len)
}

/// PG `va_tcinfo`/`va_extinfo`: codec method in the top 2 bits, size in the low 30.
pub(crate) const VARLENA_EXTSIZE_BITS: u32 = 30;
pub(crate) const VARLENA_EXTSIZE_MASK: u32 = (1u32 << VARLENA_EXTSIZE_BITS) - 1;
const TOAST_COMPRESSION_PGLZ: u8 = 0;
const TOAST_COMPRESSION_LZ4: u8 = 1;

/// Decompress a varlena codec body. `None` on a corrupt stream or unknown method.
pub(crate) fn decompress_varlena(method: u8, src: &[u8], raw_len: usize) -> Option<Vec<u8>> {
    match method {
        TOAST_COMPRESSION_PGLZ => {
            let mut out = vec![0u8; raw_len];
            let n = pglz::decompress_into(src, &mut out, true)?;
            out.truncate(n);
            Some(out)
        }
        TOAST_COMPRESSION_LZ4 => lz4::block::decompress(src, Some(raw_len as i32)).ok(),
        _ => None,
    }
}

/// Read a varlena attribute (`typlen == -1`). PG varlena formats (`varatt.h`):
///
/// - `0bxxxxxx00` 4-byte length, uncompressed.
/// - `0bxxxxxx10` 4-byte length, in-line compressed.
/// - `0b00000001` TOAST pointer (`varattrib_1b_e`, 18 bytes on disk).
/// - `0bxxxxxxx1` 1-byte length, uncompressed short header.
///
/// Little-endian only (`WORDS_BIGENDIAN` dead on x86/aarch64).
fn read_varlena(buf: &[u8], abs: usize) -> Result<(DatumRef<'_>, usize), DecodeError> {
    if buf.len() <= abs {
        return Err(DecodeError::Truncated {
            offset: abs,
            need: 1,
            have: 0,
        });
    }
    let first = buf[abs];
    if first == 0x01 {
        // varattrib_1b_e: va_tag follows the 0x01 byte
        if buf.len() < abs + 2 {
            return Err(DecodeError::Truncated {
                offset: abs,
                need: 2,
                have: buf.len() - abs,
            });
        }
        let tag = buf[abs + 1];
        // VARTAG_ONDISK = 18: tag byte + 16-byte struct varatt_external
        if tag == 18 {
            if buf.len() < abs + 2 + 16 {
                return Err(DecodeError::Truncated {
                    offset: abs,
                    need: 18,
                    have: buf.len() - abs,
                });
            }
            let body = &buf[abs + 2..abs + 2 + 16];
            let pointer = ToastPointer {
                va_rawsize: i32::from_le_bytes(body[0..4].try_into().unwrap()),
                va_extinfo: u32::from_le_bytes(body[4..8].try_into().unwrap()),
                va_valueid: u32::from_le_bytes(body[8..12].try_into().unwrap()),
                va_toastrelid: u32::from_le_bytes(body[12..16].try_into().unwrap()),
            };
            return Ok((DatumRef::External(pointer), 18));
        }
        // INDIRECT/EXPANDED tags are in-memory only, never on disk (varatt.h);
        // forward Unsupported so a corrupt stream surfaces
        return Ok((DatumRef::BadTag(&buf[abs..]), buf.len() - abs));
    }
    if first & 0x01 != 0 {
        // Short header: low bit set, total length (incl header byte) in upper 7
        let total = (first >> 1) as usize;
        if total < 1 {
            return Err(DecodeError::Truncated {
                offset: abs,
                need: 1,
                have: 0,
            });
        }
        if buf.len() < abs + total {
            return Err(DecodeError::Truncated {
                offset: abs,
                need: total,
                have: buf.len() - abs,
            });
        }
        return Ok((DatumRef::Varlena(&buf[abs + 1..abs + total]), total));
    }
    // 4-byte length: bits [1:0] = 00 uncompressed, 10 compressed
    if buf.len() < abs + 4 {
        return Err(DecodeError::Truncated {
            offset: abs,
            need: 4,
            have: buf.len() - abs,
        });
    }
    let header = u32::from_le_bytes(buf[abs..abs + 4].try_into().unwrap());
    let compressed = header & 0b10 != 0;
    let total = (header >> 2) as usize;
    if total < 4 {
        return Err(DecodeError::Truncated {
            offset: abs,
            need: 4,
            have: 0,
        });
    }
    if buf.len() < abs + total {
        return Err(DecodeError::Truncated {
            offset: abs,
            need: total,
            have: buf.len() - abs,
        });
    }
    if compressed {
        // inline-compressed varlena: 4-byte header, va_tcinfo (size + method), body
        if total < 8 {
            return Err(DecodeError::Truncated {
                offset: abs,
                need: 8,
                have: total,
            });
        }
        return Ok((DatumRef::Compressed(&buf[abs..abs + total]), total));
    }
    Ok((DatumRef::Varlena(&buf[abs + 4..abs + total]), total))
}

/// Match decoder paths producing typed values
pub(crate) fn local_matrix_covers(type_oid: u32, type_len: i16) -> bool {
    match type_len {
        -1 => matches!(
            type_oid,
            BYTEAOID
                | TEXTOID
                | VARCHAROID
                | BPCHAROID
                | NUMERICOID
                | INETOID
                | CIDROID
                | JSONOID
                | JSONBOID
        ),
        -2 => true,
        _ => matches!(
            type_oid,
            BOOLOID
                | CHAROID
                | INT2OID
                | INT4OID
                | INT8OID
                | FLOAT4OID
                | FLOAT8OID
                | OIDOID
                | XIDOID
                | CIDOID
                | OID8OID
                | XID8OID
                | PG_LSNOID
                | MONEYOID
                | NAMEOID
                | MACADDROID
                | MACADDR8OID
                | TIDOID
                | DATEOID
                | TIMEOID
                | TIMESTAMPOID
                | TIMESTAMPTZOID
                | TIMETZOID
                | UUIDOID
                | INTERVALOID
        ),
    }
}

/// Read `typlen=-2` `\0`-terminated cstring (regtype/regclass typoutput
/// intermediates). Not in the type matrix, but tracking it lets dropped
/// cstring columns walk correctly. (`name`, typlen=64, goes the fixed path.)
fn read_cstring(buf: &[u8], abs: usize) -> Result<(DatumRef<'_>, usize), DecodeError> {
    let end = buf
        .get(abs..)
        .and_then(|rest| rest.iter().position(|&b| b == 0))
        .ok_or(DecodeError::Truncated {
            offset: abs,
            need: 1,
            have: 0,
        })?;
    // include trailing NUL
    Ok((DatumRef::Cstring(&buf[abs..abs + end]), end + 1))
}

/// `(chunk_id, chunk_seq, chunk_data)` of a `pg_toast_*` tuple's 3 datums;
/// `None` for shapes that don't fit
pub(crate) fn toast_chunk_datums<'a>(datums: &[DatumRef<'a>]) -> Option<(u32, u32, &'a [u8])> {
    let [
        DatumRef::Fixed(id),
        DatumRef::Fixed(seq),
        DatumRef::Varlena(data),
        ..,
    ] = *datums
    else {
        return None;
    };
    Some((
        u32::from_le_bytes(id.try_into().ok()?),
        u32::from_le_bytes(seq.try_into().ok()?),
        data,
    ))
}

/// True iff `attnum` is part of the relation's replica identity. Emitter gates
/// delete/update old-key propagation on this, staying agnostic to [`ReplIdent`].
pub fn is_replica_identity_attr(replident: &ReplIdent, attnum: i16) -> bool {
    match replident {
        ReplIdent::Full { .. } => true,
        ReplIdent::Nothing => false,
        ReplIdent::Default { pk_attnums } => pk_attnums
            .as_ref()
            .map(|v| v.contains(&attnum))
            .unwrap_or(false),
        ReplIdent::UsingIndex { key_attnums, .. } => key_attnums.contains(&attnum),
    }
}

/// PG `attmissingval`: one-element, no-null `ArrayType`, payload at `MAXALIGN(24)`
#[cfg(test)]
pub(crate) fn raw_missing_array(elemtype: u32, elem: &[u8]) -> Vec<u8> {
    let mut v = Vec::with_capacity(24 + elem.len());
    v.extend_from_slice(&0u32.to_le_bytes()); // vl_len_ (ignored)
    v.extend_from_slice(&1i32.to_le_bytes()); // ndim
    v.extend_from_slice(&0i32.to_le_bytes()); // dataoffset
    v.extend_from_slice(&elemtype.to_le_bytes());
    v.extend_from_slice(&1i32.to_le_bytes()); // dims[0]
    v.extend_from_slice(&1i32.to_le_bytes()); // lbounds[0]
    assert_eq!(v.len(), 24);
    v.extend_from_slice(elem);
    v
}

/// PG short varlena: 1-byte header, payload up to 126 bytes
#[cfg(test)]
pub(crate) fn short_varlena(content: &[u8]) -> Vec<u8> {
    let total = 1 + content.len();
    let mut v = vec![((total as u8) << 1) | 1];
    v.extend_from_slice(content);
    v
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::decode::datum::{Datum, build};
    use crate::schema::RelName;
    use walrus::pg::walparser::{
        BlockLocation, XLogRecordBlock, XLogRecordBlockHeader, XLogRecordHeader,
    };

    /// Fringe varlena type with no local codec, standing in for anything the
    /// oracle converts
    const TSVECTOROID: u32 = 3614;

    fn rel_attr(
        attnum: i16,
        name: &str,
        type_oid: u32,
        type_len: i16,
        type_align: char,
    ) -> RelAttr {
        RelAttr {
            attnum,
            name: name.into(),
            type_oid,
            typmod: -1,
            not_null: false,
            dropped: false,
            type_name: String::new(),
            type_byval: type_len > 0 && type_len <= 8,
            type_len,
            type_align,
            type_storage: 'p',
            missing_default: None,
        }
    }

    fn descriptor(rel: u32, attrs: Vec<RelAttr>) -> RelDescriptor {
        RelDescriptor {
            rfn: RelFileNode {
                spc_node: 1663,
                db_node: 5,
                rel_node: rel,
            },
            oid: 16384,
            toast_oid: 0,
            namespace_oid: 2200,
            rel_name: RelName::new("public", "t"),
            kind: 'r',
            persistence: 'p',
            ancestors: Vec::new(),
            replident: ReplIdent::Default { pk_attnums: None },
            attributes: attrs,
        }
    }

    #[test]
    fn approx_bytes_counts_scalars_and_varlena() {
        assert_eq!(Datum::Null.approx_bytes(), 0);
        assert_eq!(build::int2(0).approx_bytes(), 2);
        assert_eq!(build::int4(0).approx_bytes(), 4);
        assert_eq!(build::int8(0).approx_bytes(), 8);
        assert_eq!(build::uuid([0; 16]).approx_bytes(), 16);
        assert_eq!(build::text("héllo").approx_bytes(), 6);
        assert_eq!(build::bytes(vec![0u8; 100]).approx_bytes(), 100);

        let heap = DecodedHeap {
            rfn: RelFileNode {
                spc_node: 1663,
                db_node: 5,
                rel_node: 16385,
            },
            xid: 1,
            source_lsn: 0,
            op: HeapOp::Update,
            new: Some(DecodedTuple {
                columns: vec![Some(build::int4(1)), Some(build::bytes(vec![0u8; 10]))],
                partial: false,
            }),
            old: Some(DecodedTuple {
                columns: vec![None, Some(build::text("abc"))],
                partial: false,
            }),
        };
        assert_eq!(heap.approx_bytes(), 17);
    }

    /// Build `xl_heap_header (5) + bitmap[+pad] + col data`.
    fn build_tuple_payload(natts: u16, has_null: bool, t_hoff: u8, col_data: &[u8]) -> Vec<u8> {
        let mut v = Vec::new();
        v.extend_from_slice(&natts.to_le_bytes()); // t_infomask2
        let infomask = if has_null { HEAP_HASNULL } else { 0 };
        v.extend_from_slice(&infomask.to_le_bytes()); // t_infomask
        v.push(t_hoff);
        let bitmap_pad = (t_hoff as usize) - SIZE_OF_HEAP_TUPLE_HEADER;
        v.extend_from_slice(&vec![0xFFu8; bitmap_pad]); // all-set bitmap + pad
        v.extend_from_slice(col_data);
        v
    }

    fn record_with(
        rm: RmId,
        info: u8,
        main_data: Vec<u8>,
        block_data: Vec<u8>,
    ) -> XLogRecord<'static> {
        XLogRecord {
            header: XLogRecordHeader {
                resource_manager_id: rm as u8,
                info,
                xact_id: 42,
                ..Default::default()
            },
            blocks: vec![XLogRecordBlock {
                header: XLogRecordBlockHeader {
                    location: BlockLocation {
                        rel: RelFileNode {
                            spc_node: 1663,
                            db_node: 5,
                            rel_node: 16385,
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

    /// Every decode here also checks borrowed views build equal cells
    fn decode_heap_record(
        record: &XLogRecord,
        source_lsn: u64,
        rel: &RelDescriptor,
    ) -> Result<DecodedHeaps, DecodeError> {
        let owned = super::decode_heap_record(record, source_lsn, rel);
        let cells = |t: &Option<TupleView<'_>>| t.as_ref().map(|t| t.to_tuple(rel));
        match (&owned, view_heap_record(record, rel)) {
            (Ok(heaps), Ok(views)) => assert_eq!(
                heaps
                    .iter()
                    .map(|h| (h.op, h.new.clone(), h.old.clone()))
                    .collect::<Vec<_>>(),
                views
                    .iter()
                    .map(|v| (v.op, cells(&v.new), cells(&v.old)))
                    .collect::<Vec<_>>(),
            ),
            (Err(a), Err(b)) => assert_eq!(a.to_string(), b.to_string()),
            (a, b) => panic!("owned {a:?} vs view {b:?}"),
        }
        owned
    }

    /// Owned datum at `abs` and its stored length
    fn decode_one_value(
        att: &RelAttr,
        buf: &[u8],
        abs: usize,
    ) -> Result<(Datum, usize), DecodeError> {
        let (datum, len) = read_datum(att, buf, abs)?;
        Ok((resolve(att, datum).unwrap_or(Datum::Null), len))
    }

    fn missing_value_for(att: &RelAttr) -> Datum {
        resolve(att, DatumRef::Missing).expect("fast default resolves")
    }

    #[test]
    fn decode_cstring_bytes_and_truncated() {
        let att = rel_attr(1, "c", 0, -2, 'c');
        let cstring = |s: &[u8]| Datum::Cstring(s.to_vec());
        let (v, n) = decode_one_value(&att, b"hello\0", 0).unwrap();
        assert_eq!(v, cstring(b"hello"));
        assert_eq!(n, 6);
        let (v, n) = decode_one_value(&att, &[0xFF, 0xFE, 0x00], 0).unwrap();
        assert_eq!(v, cstring(&[0xFF, 0xFE]));
        assert_eq!(n, 3);
        // non-zero offset within multi-string buffer
        let (v, n) = decode_one_value(&att, b"ab\0cd\0", 3).unwrap();
        assert_eq!(v, cstring(b"cd"));
        assert_eq!(n, 3);
        // no terminator before buffer end
        let err = decode_one_value(&att, b"hello", 0).unwrap_err();
        assert_eq!(
            format!("{err:?}"),
            "Truncated { offset: 0, need: 1, have: 0 }"
        );
    }

    #[test]
    fn decode_insert_int4_int8() {
        let rel = descriptor(
            16385,
            vec![
                rel_attr(1, "a", INT4OID, 4, 'i'),
                rel_attr(2, "b", INT8OID, 8, 'd'),
            ],
        );
        // int4 at 0..4, 4-byte pad, int8 8-aligned at 8..16
        let mut col_data = Vec::new();
        col_data.extend_from_slice(&12345i32.to_le_bytes());
        col_data.extend_from_slice(&[0u8; 4]); // 8-align pad
        col_data.extend_from_slice(&999_999i64.to_le_bytes());
        let payload = build_tuple_payload(2, false, 24, &col_data);
        let main_data = vec![0u8; SIZE_OF_HEAP_INSERT];
        let rec = record_with(RmId::Heap, XLOG_HEAP_INSERT, main_data, payload);
        let out = decode_heap_record(&rec, 0x1000, &rel).unwrap().remove(0);
        assert_eq!(out.op, HeapOp::Insert);
        assert_eq!(out.xid, 42);
        let new = out.new.unwrap();
        assert!(!new.partial);
        assert_eq!(new.columns.len(), 2);
        assert_eq!(new.columns[0], Some(build::int4(12345)));
        assert_eq!(new.columns[1], Some(build::int8(999_999)));
    }

    #[test]
    fn decode_insert_bool_and_text() {
        let rel = descriptor(
            16386,
            vec![
                rel_attr(1, "flag", BOOLOID, 1, 'c'),
                rel_attr(2, "msg", TEXTOID, -1, 'i'),
            ],
        );
        let mut col_data = Vec::new();
        col_data.push(1u8); // bool true
        col_data.extend_from_slice(&[0u8; 3]); // pad to 'i'-align(4)
        // 4-byte uncompressed varlena: header = total << 2
        let body = b"hi";
        let total = 4 + body.len();
        let header_u32 = (total as u32) << 2;
        col_data.extend_from_slice(&header_u32.to_le_bytes());
        col_data.extend_from_slice(body);
        let payload = build_tuple_payload(2, false, 24, &col_data);
        let main_data = vec![0u8; SIZE_OF_HEAP_INSERT];
        let rec = record_with(RmId::Heap, XLOG_HEAP_INSERT, main_data, payload);
        let out = decode_heap_record(&rec, 0, &rel).unwrap().remove(0);
        let new = out.new.unwrap();
        assert_eq!(new.columns[0], Some(build::bool_(true)));
        assert_eq!(new.columns[1], Some(build::text("hi")));
    }

    #[test]
    fn decode_insert_short_varlena() {
        let rel = descriptor(16387, vec![rel_attr(1, "msg", TEXTOID, -1, 'i')]);
        // Short header: bit 0 set, total (incl header) in upper 7 bits
        let header = (3u8 << 1) | 0x01;
        let mut col_data = Vec::new();
        col_data.push(header);
        col_data.extend_from_slice(b"hi");
        // att_align_pointer skips alignment before a non-zero short-header byte,
        // so no pad; t_hoff=24 is just bitmap_pad=1
        let payload = build_tuple_payload(1, false, 24, &col_data);
        let main_data = vec![0u8; SIZE_OF_HEAP_INSERT];
        let rec = record_with(RmId::Heap, XLOG_HEAP_INSERT, main_data, payload);
        let out = decode_heap_record(&rec, 0, &rel).unwrap().remove(0);
        let new = out.new.unwrap();
        assert_eq!(new.columns[0], Some(build::text("hi")));
    }

    fn inline_compressed_varlena(method: u8, body: &[u8], raw_len: usize) -> Vec<u8> {
        let total = 8 + body.len();
        let tcinfo = ((method as u32) << VARLENA_EXTSIZE_BITS) | raw_len as u32;
        let mut buf = Vec::with_capacity(total);
        buf.extend_from_slice(&(((total as u32) << 2) | 0b10).to_le_bytes());
        buf.extend_from_slice(&tcinfo.to_le_bytes());
        buf.extend_from_slice(body);
        buf
    }

    #[test]
    fn decode_inline_compressed_pglz_text() {
        let text = "z".repeat(11000);
        let comp = pglz::compress(text.as_bytes(), &pglz::Strategy::ALWAYS).expect("pglz compress");
        assert!(comp.len() < text.len(), "must actually compress");
        let buf = inline_compressed_varlena(0, &comp, text.len());
        let att = rel_attr(1, "val", TEXTOID, -1, 'i');
        let (v, consumed) = decode_one_value(&att, &buf, 0).unwrap();
        assert_eq!(consumed, buf.len());
        assert_eq!(v, build::text(&text));
    }

    #[test]
    fn decode_inline_compressed_lz4_text() {
        let text = "lz4-".repeat(4000);
        let comp = lz4::block::compress(text.as_bytes(), None, false).expect("lz4 compress");
        let buf = inline_compressed_varlena(1, &comp, text.len());
        let att = rel_attr(1, "val", TEXTOID, -1, 'i');
        let (v, consumed) = decode_one_value(&att, &buf, 0).unwrap();
        assert_eq!(consumed, buf.len());
        assert_eq!(v, build::text(&text));
    }

    #[test]
    fn decode_inline_compressed_corrupt_surfaces_unsupported() {
        let buf = inline_compressed_varlena(0, &[0xff, 0xff, 0xff, 0xff], 4096);
        let att = rel_attr(1, "val", TEXTOID, -1, 'i');
        let (v, consumed) = decode_one_value(&att, &buf, 0).unwrap();
        assert_eq!(consumed, buf.len());
        assert!(matches!(v, Datum::BadTag(_)));
    }

    #[test]
    fn decompress_varlena_roundtrips_and_rejects_unknown() {
        let data = b"the quick brown fox jumped, jumped, jumped".to_vec();
        let pglz = pglz::compress(&data, &pglz::Strategy::ALWAYS).expect("pglz compress");
        assert_eq!(
            decompress_varlena(0, &pglz, data.len()).as_deref(),
            Some(data.as_slice())
        );
        let lz4_data = lz4::block::compress(&data, None, false).expect("lz4 compress");
        assert_eq!(
            decompress_varlena(1, &lz4_data, data.len()).as_deref(),
            Some(data.as_slice())
        );
        assert_eq!(decompress_varlena(2, &lz4_data, data.len()), None);
    }

    #[test]
    fn decode_insert_with_null_bitmap() {
        let rel = descriptor(
            16388,
            vec![
                rel_attr(1, "a", INT4OID, 4, 'i'),
                rel_attr(2, "b", INT4OID, 4, 'i'),
                rel_attr(3, "c", INT4OID, 4, 'i'),
            ],
        );
        // bits 0,2 set; bit 1 clear (NULL)
        let bitmap = 0b00000101u8;
        // t_hoff = MAXALIGN(23 + 1 bitmap byte) = 24
        let mut payload = Vec::new();
        payload.extend_from_slice(&3u16.to_le_bytes());
        payload.extend_from_slice(&HEAP_HASNULL.to_le_bytes());
        payload.push(24);
        payload.push(bitmap);
        // col 2 absent from data (NULL)
        payload.extend_from_slice(&100i32.to_le_bytes());
        payload.extend_from_slice(&300i32.to_le_bytes());
        let main_data = vec![0u8; SIZE_OF_HEAP_INSERT];
        let rec = record_with(RmId::Heap, XLOG_HEAP_INSERT, main_data, payload);
        let out = decode_heap_record(&rec, 0, &rel).unwrap().remove(0);
        let new = out.new.unwrap();
        assert_eq!(new.columns.len(), 3);
        assert_eq!(new.columns[0], Some(build::int4(100)));
        assert_eq!(new.columns[1], Some(Datum::Null));
        assert_eq!(new.columns[2], Some(build::int4(300)));
    }

    #[test]
    fn decode_delete_with_old_key_emits_old() {
        let rel = descriptor(16389, vec![rel_attr(1, "id", INT4OID, 4, 'i')]);
        // main_data: xl_heap_delete(8) + xl_heap_header(5) + bitmap_pad(1) + col data
        let mut main_data = vec![0u8; SIZE_OF_HEAP_DELETE];
        main_data[7] = XLH_DELETE_CONTAINS_OLD_KEY;
        main_data.extend_from_slice(&1u16.to_le_bytes()); // natts
        main_data.extend_from_slice(&0u16.to_le_bytes()); // infomask
        main_data.push(24); // t_hoff
        main_data.push(0); // bitmap pad
        main_data.extend_from_slice(&7777i32.to_le_bytes());
        let rec = record_with(RmId::Heap, XLOG_HEAP_DELETE, main_data, Vec::new());
        let out = decode_heap_record(&rec, 0, &rel).unwrap().remove(0);
        assert_eq!(out.op, HeapOp::Delete);
        let old = out.old.unwrap();
        assert_eq!(old.columns[0], Some(build::int4(7777)));
    }

    #[test]
    fn decode_update_with_prefix_marks_partial() {
        let rel = descriptor(
            16390,
            vec![
                rel_attr(1, "a", INT4OID, 4, 'i'),
                rel_attr(2, "b", INT4OID, 4, 'i'),
            ],
        );
        // PREFIX_FROM_OLD, prefixlen=4 (col 1 elided). block 0:
        // [prefixlen:u16=4][xl_heap_header(5)][bitmap_pad(1)][col 2 int4]
        let mut block_data = Vec::new();
        block_data.extend_from_slice(&4u16.to_le_bytes());
        block_data.extend_from_slice(&2u16.to_le_bytes()); // natts
        block_data.extend_from_slice(&0u16.to_le_bytes()); // infomask
        block_data.push(24); // t_hoff
        block_data.push(0); // bitmap pad
        block_data.extend_from_slice(&999i32.to_le_bytes()); // col 2
        let mut main_data = vec![0u8; SIZE_OF_HEAP_UPDATE];
        main_data[7] = XLH_UPDATE_PREFIX_FROM_OLD; // flags
        let rec = record_with(RmId::Heap, XLOG_HEAP_UPDATE, main_data, block_data);
        let out = decode_heap_record(&rec, 0, &rel).unwrap().remove(0);
        assert_eq!(out.op, HeapOp::Update);
        let new = out.new.unwrap();
        assert!(new.partial, "prefix-compressed UPDATE flagged partial");
        assert_eq!(new.columns[0], None);
        assert_eq!(new.columns[1], Some(build::int4(999)));
    }

    #[test]
    fn decode_hot_update_without_old_emits_no_old() {
        let rel = descriptor(16391, vec![rel_attr(1, "id", INT4OID, 4, 'i')]);
        let mut block_data = Vec::new();
        block_data.extend_from_slice(&1u16.to_le_bytes());
        block_data.extend_from_slice(&0u16.to_le_bytes());
        block_data.push(24);
        block_data.push(0);
        block_data.extend_from_slice(&55i32.to_le_bytes());
        let main_data = vec![0u8; SIZE_OF_HEAP_UPDATE]; // flags=0
        let rec = record_with(RmId::Heap, XLOG_HEAP_HOT_UPDATE, main_data, block_data);
        let out = decode_heap_record(&rec, 0, &rel).unwrap().remove(0);
        assert_eq!(out.op, HeapOp::HotUpdate);
        assert!(out.old.is_none());
        let new = out.new.unwrap();
        assert_eq!(new.columns[0], Some(build::int4(55)));
    }

    #[test]
    fn decode_dropped_column_emits_null_and_advances() {
        let mut dropped = rel_attr(1, "old", INT4OID, 4, 'i');
        dropped.dropped = true;
        let rel = descriptor(16392, vec![dropped, rel_attr(2, "live", INT4OID, 4, 'i')]);
        let mut col_data = Vec::new();
        col_data.extend_from_slice(&0i32.to_le_bytes()); // dropped col still present
        col_data.extend_from_slice(&8888i32.to_le_bytes());
        let payload = build_tuple_payload(2, false, 24, &col_data);
        let main_data = vec![0u8; SIZE_OF_HEAP_INSERT];
        let rec = record_with(RmId::Heap, XLOG_HEAP_INSERT, main_data, payload);
        let out = decode_heap_record(&rec, 0, &rel).unwrap().remove(0);
        let new = out.new.unwrap();
        assert_eq!(new.columns[0], Some(Datum::Null));
        assert_eq!(new.columns[1], Some(build::int4(8888)));
    }

    #[test]
    fn decode_skips_lock_inplace_truncate() {
        let rel = descriptor(16393, vec![rel_attr(1, "id", INT4OID, 4, 'i')]);
        for op in [XLOG_HEAP_LOCK, XLOG_HEAP_INPLACE, 0x30] {
            let rec = record_with(RmId::Heap, op, Vec::new(), Vec::new());
            let out = decode_heap_record(&rec, 0, &rel).unwrap();
            assert!(out.is_empty(), "op {op:#x} should skip");
        }
    }

    #[test]
    fn decode_skips_heap2_lock_updated_silently() {
        // 0x60 = Heap2 LOCK_UPDATED, skip path (MULTI_INSERT 0x50 tested apart)
        let rel = descriptor(16394, vec![rel_attr(1, "id", INT4OID, 4, 'i')]);
        let rec = record_with(RmId::Heap2, 0x60, Vec::new(), Vec::new());
        let out = decode_heap_record(&rec, 0, &rel).unwrap();
        assert!(out.is_empty(), "non-multi-insert heap2 ops skip");
    }

    #[test]
    fn decode_uuid_round_trip() {
        let rel = descriptor(16395, vec![rel_attr(1, "u", UUIDOID, 16, 'c')]);
        let uuid_bytes = [
            0xDE, 0xAD, 0xBE, 0xEF, 0xCA, 0xFE, 0xBA, 0xBE, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66,
            0x77, 0x88,
        ];
        let payload = build_tuple_payload(1, false, 24, &uuid_bytes);
        let main_data = vec![0u8; SIZE_OF_HEAP_INSERT];
        let rec = record_with(RmId::Heap, XLOG_HEAP_INSERT, main_data, payload);
        let out = decode_heap_record(&rec, 0, &rel).unwrap().remove(0);
        let new = out.new.unwrap();
        assert_eq!(new.columns[0], Some(build::uuid(uuid_bytes)));
    }

    #[test]
    fn decode_external_toast_pointer() {
        let rel = descriptor(16396, vec![rel_attr(1, "blob", BYTEAOID, -1, 'i')]);
        // varattrib_1b_e: 0x01, va_tag=18, then va_rawsize/extinfo/valueid/toastrelid
        let mut col_data = Vec::new();
        col_data.push(0x01);
        col_data.push(18);
        col_data.extend_from_slice(&12345i32.to_le_bytes());
        col_data.extend_from_slice(&678u32.to_le_bytes());
        col_data.extend_from_slice(&12u32.to_le_bytes());
        col_data.extend_from_slice(&99u32.to_le_bytes());
        let payload = build_tuple_payload(1, false, 24, &col_data);
        let main_data = vec![0u8; SIZE_OF_HEAP_INSERT];
        let rec = record_with(RmId::Heap, XLOG_HEAP_INSERT, main_data, payload);
        let out = decode_heap_record(&rec, 0, &rel).unwrap().remove(0);
        let new = out.new.unwrap();
        assert_eq!(
            new.columns[0],
            Some(Datum::External(ToastPointer {
                va_rawsize: 12345,
                va_extinfo: 678,
                va_valueid: 12,
                va_toastrelid: 99,
            }))
        );
    }

    #[test]
    fn decode_unsupported_type_emits_pending() {
        // tsvector is outside the local matrix; its stored body survives for
        // the shadow extension to convert at emit
        let rel = descriptor(16397, vec![rel_attr(1, "t", TSVECTOROID, -1, 'i')]);
        let body = b"\x01opaque";
        let total = 4 + body.len();
        let header_u32 = (total as u32) << 2;
        let mut col_data = Vec::new();
        col_data.extend_from_slice(&header_u32.to_le_bytes());
        col_data.extend_from_slice(body);
        let payload = build_tuple_payload(1, false, 24, &col_data);
        let main_data = vec![0u8; SIZE_OF_HEAP_INSERT];
        let rec = record_with(RmId::Heap, XLOG_HEAP_INSERT, main_data, payload);
        let out = decode_heap_record(&rec, 0, &rel).unwrap().remove(0);
        let new = out.new.unwrap();
        assert_eq!(new.columns[0], Some(build::bytes(body)));
    }

    #[test]
    fn missing_value_substitutes_when_natts_below_catalog() {
        // Catalog natts=3, wal natts=2 (pre-ALTER row); col 3 missing_default="7"
        // must surface 7 not NULL
        let mut a3 = rel_attr(3, "c", INT4OID, 4, 'i');
        a3.missing_default = Some("7".into());
        let rel = descriptor(
            16500,
            vec![
                rel_attr(1, "id", INT4OID, 4, 'i'),
                rel_attr(2, "payload", INT4OID, 4, 'i'),
                a3,
            ],
        );
        let mut col_data = Vec::new();
        col_data.extend_from_slice(&100i32.to_le_bytes());
        col_data.extend_from_slice(&200i32.to_le_bytes());
        let payload = build_tuple_payload(2, false, 24, &col_data);
        let main_data = vec![0u8; SIZE_OF_HEAP_INSERT];
        let rec = record_with(RmId::Heap, XLOG_HEAP_INSERT, main_data, payload);
        let out = decode_heap_record(&rec, 0, &rel).unwrap().remove(0);
        let new = out.new.unwrap();
        assert_eq!(new.columns.len(), 3);
        assert_eq!(new.columns[0], Some(build::int4(100)));
        assert_eq!(new.columns[1], Some(build::int4(200)));
        assert_eq!(new.columns[2], Some(build::int4(7)));
    }

    #[test]
    fn missing_value_absent_falls_back_to_null() {
        // Catalog natts=3, wal natts=2, col 3 no missing_default => NULL
        let rel = descriptor(
            16501,
            vec![
                rel_attr(1, "id", INT4OID, 4, 'i'),
                rel_attr(2, "payload", INT4OID, 4, 'i'),
                rel_attr(3, "c", INT4OID, 4, 'i'),
            ],
        );
        let mut col_data = Vec::new();
        col_data.extend_from_slice(&1i32.to_le_bytes());
        col_data.extend_from_slice(&2i32.to_le_bytes());
        let payload = build_tuple_payload(2, false, 24, &col_data);
        let main_data = vec![0u8; SIZE_OF_HEAP_INSERT];
        let rec = record_with(RmId::Heap, XLOG_HEAP_INSERT, main_data, payload);
        let out = decode_heap_record(&rec, 0, &rel).unwrap().remove(0);
        let new = out.new.unwrap();
        assert_eq!(new.columns[2], Some(Datum::Null));
    }

    #[test]
    fn missing_value_physical_null_unaffected_by_default() {
        // Bitmap-clear NULL on a missing_default col stays NULL: substitution
        // applies only for natts below catalog, never to an explicit WAL NULL
        let mut a2 = rel_attr(2, "b", INT4OID, 4, 'i');
        a2.missing_default = Some("99".into());
        let rel = descriptor(16502, vec![rel_attr(1, "a", INT4OID, 4, 'i'), a2]);
        let bitmap = 0b00000001u8; // bit 1 clear = NULL
        let mut payload = Vec::new();
        payload.extend_from_slice(&2u16.to_le_bytes());
        payload.extend_from_slice(&HEAP_HASNULL.to_le_bytes());
        payload.push(24);
        payload.push(bitmap);
        payload.extend_from_slice(&55i32.to_le_bytes());
        let main_data = vec![0u8; SIZE_OF_HEAP_INSERT];
        let rec = record_with(RmId::Heap, XLOG_HEAP_INSERT, main_data, payload);
        let out = decode_heap_record(&rec, 0, &rel).unwrap().remove(0);
        let new = out.new.unwrap();
        assert_eq!(new.columns[0], Some(build::int4(55)));
        assert_eq!(new.columns[1], Some(Datum::Null));
    }

    #[test]
    fn record_shape_reads_headers() {
        let shape = |rm: RmId, info, main_data: &[u8]| {
            record_shape(HeapRecordBytes {
                rm: rm as u8,
                info,
                main_data,
                block: None,
            })
        };
        let mut update = [0u8; SIZE_OF_HEAP_UPDATE];
        update[7] = XLH_UPDATE_SUFFIX_FROM_OLD;
        let s = shape(RmId::Heap, XLOG_HEAP_HOT_UPDATE, &update)
            .unwrap()
            .unwrap();
        assert_eq!((s.op, s.tuples, s.partial), (HeapOp::HotUpdate, 1, true));
        let multi = [XLH_INSERT_CONTAINS_NEW_TUPLE, 0, 3, 0];
        let s = shape(RmId::Heap2, XLOG_HEAP2_MULTI_INSERT, &multi)
            .unwrap()
            .unwrap();
        assert_eq!((s.op, s.tuples, s.partial), (HeapOp::Insert, 3, false));
        assert!(matches!(
            shape(RmId::Heap2, XLOG_HEAP2_MULTI_INSERT, &[0, 0, 3, 0]),
            Ok(None)
        ));
        assert!(matches!(shape(RmId::Heap, XLOG_HEAP_LOCK, &[]), Ok(None)));
        assert!(shape(RmId::Heap2, XLOG_HEAP2_MULTI_INSERT, &[0, 0, 0, 0]).is_err());
        assert!(shape(RmId::Heap, XLOG_HEAP_DELETE, &[0; 4]).is_err());
    }

    #[test]
    fn decode_multi_insert_three_rows() {
        let rel = descriptor(16410, vec![rel_attr(1, "id", INT4OID, 4, 'i')]);
        // main_data: flags=CONTAINS_NEW_TUPLE, pad, ntuples=3,
        // offsets[3]=[1,2,3] (info != INIT_PAGE)
        let mut main_data = Vec::new();
        main_data.push(XLH_INSERT_CONTAINS_NEW_TUPLE);
        main_data.push(0); // pad
        main_data.extend_from_slice(&3u16.to_le_bytes());
        for off in 1u16..=3 {
            main_data.extend_from_slice(&off.to_le_bytes());
        }
        // 3 x (xl_multi_insert_tuple(7) + bitmap_pad(1) + int4(4)) = 12B/row,
        // no inter-tuple pad (12 even)
        let mut block_data = Vec::new();
        for v in [100i32, 200, 300] {
            // datalen=5: bitmap_pad(1) + int4(4), tuple body after stripping
            // the 23-byte fixed header writer-side
            block_data.extend_from_slice(&5u16.to_le_bytes()); // datalen
            block_data.extend_from_slice(&1u16.to_le_bytes()); // t_infomask2 = natts
            block_data.extend_from_slice(&0u16.to_le_bytes()); // t_infomask
            block_data.push(24); // t_hoff
            block_data.push(0); // bitmap pad
            block_data.extend_from_slice(&v.to_le_bytes());
        }
        let rec = record_with(RmId::Heap2, XLOG_HEAP2_MULTI_INSERT, main_data, block_data);
        let out = decode_heap_record(&rec, 0x2000, &rel).unwrap();
        assert_eq!(out.len(), 3);
        for (i, expected) in [100i32, 200, 300].iter().enumerate() {
            assert_eq!(out[i].op, HeapOp::Insert);
            assert_eq!(out[i].xid, 42);
            assert_eq!(out[i].source_lsn, 0x2000);
            let new = out[i].new.as_ref().unwrap();
            assert_eq!(new.columns.len(), 1);
            assert_eq!(new.columns[0], Some(build::int4(*expected)));
        }
    }

    fn multi_insert_main_data(ntuples: u16) -> Vec<u8> {
        let mut main_data = vec![XLH_INSERT_CONTAINS_NEW_TUPLE, 0];
        main_data.extend_from_slice(&ntuples.to_le_bytes());
        main_data
    }

    fn push_multi_insert_tuple(block_data: &mut Vec<u8>, infomask: u16, body: &[u8]) {
        block_data.resize(block_data.len().next_multiple_of(2), 0xFF);
        block_data.extend_from_slice(&(body.len() as u16).to_le_bytes());
        block_data.extend_from_slice(&2u16.to_le_bytes()); // t_infomask2 = natts
        block_data.extend_from_slice(&infomask.to_le_bytes());
        block_data.push(24); // t_hoff
        block_data.extend_from_slice(body);
    }

    #[test]
    fn decode_multi_insert_init_page_nulls_varlena_and_pad() {
        let rel = descriptor(
            16413,
            vec![
                rel_attr(1, "id", INT4OID, 4, 'i'),
                rel_attr(2, "msg", TEXTOID, -1, 'i'),
            ],
        );
        let mut block_data = Vec::new();
        // pad(1) + int4 + short varlena "ab"; odd tuple end forces SHORTALIGN pad
        let mut body = vec![0];
        body.extend_from_slice(&7i32.to_le_bytes());
        body.extend_from_slice(&[0x07, b'a', b'b']);
        push_multi_insert_tuple(&mut block_data, 0, &body);
        assert_eq!(block_data.len() % 2, 1);
        // bitmap 0b01 leaves msg NULL
        let mut body = vec![0b01];
        body.extend_from_slice(&8i32.to_le_bytes());
        push_multi_insert_tuple(&mut block_data, HEAP_HASNULL, &body);
        let rec = record_with(
            RmId::Heap2,
            XLOG_HEAP2_MULTI_INSERT | XLOG_HEAP_INIT_PAGE,
            multi_insert_main_data(2),
            block_data,
        );
        let out = decode_heap_record(&rec, 0, &rel).unwrap();
        let cols: Vec<_> = out.iter().map(|h| h.new.clone().unwrap()).collect();
        assert_eq!(
            cols[0].columns,
            vec![Some(build::int4(7)), Some(build::text("ab"))]
        );
        assert_eq!(
            cols[1].columns,
            vec![Some(build::int4(8)), Some(Datum::Null)]
        );
        assert!(!cols[0].partial && !cols[1].partial);
    }

    #[test]
    fn decode_multi_insert_varlena_stays_inside_its_tuple() {
        let rel = descriptor(
            16414,
            vec![
                rel_attr(1, "id", INT4OID, 4, 'i'),
                rel_attr(2, "msg", TEXTOID, -1, 'i'),
            ],
        );
        let mut block_data = Vec::new();
        // Varlena header claims 4 data bytes, tuple carries 2
        let mut body = vec![0];
        body.extend_from_slice(&7i32.to_le_bytes());
        body.extend_from_slice(&[0x0B, b'a', b'b']);
        push_multi_insert_tuple(&mut block_data, 0, &body);
        let mut body = vec![0];
        body.extend_from_slice(&8i32.to_le_bytes());
        body.extend_from_slice(&[0x05, b'z']);
        push_multi_insert_tuple(&mut block_data, 0, &body);
        let rec = record_with(
            RmId::Heap2,
            XLOG_HEAP2_MULTI_INSERT | XLOG_HEAP_INIT_PAGE,
            multi_insert_main_data(2),
            block_data,
        );
        let out = decode_heap_record(&rec, 0, &rel).unwrap();
        let first = out[0].new.as_ref().unwrap();
        assert!(first.partial);
        assert_eq!(first.columns, vec![Some(build::int4(7)), None]);
        let second = out[1].new.as_ref().unwrap();
        assert_eq!(
            second.columns,
            vec![Some(build::int4(8)), Some(build::text("z"))]
        );
    }

    #[test]
    fn decode_multi_insert_truncated_body_errors() {
        let rel = descriptor(16415, vec![rel_attr(1, "id", INT4OID, 4, 'i')]);
        let mut block_data = Vec::new();
        push_multi_insert_tuple(&mut block_data, 0, &[0; 5]);
        block_data.truncate(block_data.len() - 1);
        let rec = record_with(
            RmId::Heap2,
            XLOG_HEAP2_MULTI_INSERT | XLOG_HEAP_INIT_PAGE,
            multi_insert_main_data(1),
            block_data,
        );
        assert!(matches!(
            decode_heap_record(&rec, 0, &rel),
            Err(DecodeError::Truncated {
                offset: SIZE_OF_MULTI_INSERT_TUPLE,
                need: 5,
                have: 4
            })
        ));
    }

    #[test]
    fn decode_multi_insert_zero_ntuples_errors() {
        let rel = descriptor(16411, vec![rel_attr(1, "id", INT4OID, 4, 'i')]);
        let mut main_data = Vec::new();
        main_data.push(XLH_INSERT_CONTAINS_NEW_TUPLE);
        main_data.push(0); // pad
        main_data.extend_from_slice(&0u16.to_le_bytes());
        let rec = record_with(RmId::Heap2, XLOG_HEAP2_MULTI_INSERT, main_data, Vec::new());
        let r = decode_heap_record(&rec, 0, &rel);
        assert!(r.is_err(), "ntuples=0 must surface as DecodeError");
    }

    #[test]
    fn decode_multi_insert_skips_without_new_tuple_flag() {
        let rel = descriptor(16412, vec![rel_attr(1, "id", INT4OID, 4, 'i')]);
        // flags=0 (no CONTAINS_NEW_TUPLE): no tuple bytes => empty smallvec
        let mut main_data = Vec::new();
        main_data.push(0);
        main_data.push(0); // pad
        main_data.extend_from_slice(&2u16.to_le_bytes());
        main_data.extend_from_slice(&1u16.to_le_bytes()); // offset[0]
        main_data.extend_from_slice(&2u16.to_le_bytes()); // offset[1]
        let rec = record_with(RmId::Heap2, XLOG_HEAP2_MULTI_INSERT, main_data, Vec::new());
        let out = decode_heap_record(&rec, 0, &rel).unwrap();
        assert!(out.is_empty(), "missing CONTAINS_NEW_TUPLE => skip");
    }

    #[test]
    fn fixed_system_types_decode_values() {
        let cases: [(u32, Vec<u8>, char, Datum); 7] = [
            (XIDOID, 7u32.to_le_bytes().to_vec(), 'i', build::oid(7)),
            (
                PG_LSNOID,
                0x16_B374_D848u64.to_le_bytes().to_vec(),
                'd',
                build::oid8(0x16_B374_D848),
            ),
            (
                MONEYOID,
                (-123_456i64).to_le_bytes().to_vec(),
                'd',
                build::int8(-123_456),
            ),
            (
                NAMEOID,
                {
                    let mut b = b"pg_class".to_vec();
                    b.resize(64, 0);
                    b
                },
                'c',
                build::name("pg_class"),
            ),
            (
                MACADDROID,
                vec![0x08, 0x00, 0x2b, 0x01, 0x02, 0xab],
                'i',
                Datum::fixed(&[0x08, 0x00, 0x2b, 0x01, 0x02, 0xab]),
            ),
            (
                MACADDR8OID,
                vec![0x08, 0x00, 0x2b, 0xff, 0xfe, 0x01, 0x02, 0x03],
                'i',
                Datum::fixed(&[0x08, 0x00, 0x2b, 0xff, 0xfe, 0x01, 0x02, 0x03]),
            ),
            (
                TIDOID,
                vec![0, 0, 5, 0, 3, 0],
                's',
                Datum::fixed(&crate::decode::codecs::encode_tid(5, 3)),
            ),
        ];
        for (oid, body, align, want) in cases {
            let att = rel_attr(1, "c", oid, body.len() as i16, align);
            let (v, n) = decode_one_value(&att, &body, 0).expect("decodes");
            assert_eq!((v, n), (want, body.len()), "oid {oid}");
        }
    }

    #[test]
    fn missing_value_text_parses_system_types() {
        let mut lsn = rel_attr(1, "l", PG_LSNOID, 8, 'd');
        lsn.missing_default = Some("16/B374D848".into());
        assert_eq!(missing_value_for(&lsn), build::oid8(0x16_B374_D848));
        let mut x8 = rel_attr(1, "x", XID8OID, 8, 'd');
        x8.missing_default = Some("18446744073709551615".into());
        assert_eq!(missing_value_for(&x8), build::oid8(u64::MAX));
        let mut mac = rel_attr(1, "m", MACADDROID, 6, 'i');
        mac.missing_default = Some("08:00:2b:01:02:03".into());
        assert_eq!(
            missing_value_for(&mac),
            Datum::fixed(&[0x08, 0x00, 0x2b, 0x01, 0x02, 0x03])
        );
    }

    #[test]
    fn missing_value_for_type_matrix() {
        let mut bool_att = rel_attr(1, "b", BOOLOID, 1, 'c');
        bool_att.missing_default = Some("t".into());
        assert_eq!(missing_value_for(&bool_att), build::bool_(true));
        bool_att.missing_default = Some("false".into());
        assert_eq!(missing_value_for(&bool_att), build::bool_(false));

        let mut i4 = rel_attr(1, "x", INT4OID, 4, 'i');
        i4.missing_default = Some("42".into());
        assert_eq!(missing_value_for(&i4), build::int4(42));

        let mut i8 = rel_attr(1, "x", INT8OID, 8, 'd');
        i8.missing_default = Some("-9223372036854775808".into());
        assert_eq!(missing_value_for(&i8), build::int8(i64::MIN));

        let mut txt = rel_attr(1, "n", TEXTOID, -1, 'i');
        txt.missing_default = Some("hello".into());
        assert_eq!(missing_value_for(&txt), build::out_text("hello"));

        let mut num = rel_attr(1, "n", NUMERICOID, -1, 'i');
        num.missing_default = Some("3.14".into());
        assert_eq!(missing_value_for(&num), build::numeric("3.14"));
        num.missing_default = Some("pi".into());
        assert_eq!(missing_value_for(&num), Datum::Null);
        let mut ts = rel_attr(1, "t", TSVECTOROID, -1, 'i');
        ts.missing_default = Some("'a'".into());
        assert_eq!(missing_value_for(&ts), build::out_text("'a'"));

        let none = rel_attr(1, "x", INT4OID, 4, 'i');
        assert_eq!(missing_value_for(&none), Datum::Null);
    }

    #[test]
    fn fast_default_raw_scalar_decodes_to_value() {
        let mut a = rel_attr(1, "x", INT4OID, 4, 'i');
        a.missing_default = Some(MissingDefault::Raw(raw_missing_array(
            INT4OID,
            &7i32.to_le_bytes(),
        )));
        assert_eq!(missing_value_for(&a), build::int4(7));
    }

    #[test]
    fn fast_default_raw_text_decodes_to_value() {
        let mut a = rel_attr(1, "n", TEXTOID, -1, 'i');
        a.missing_default = Some(MissingDefault::Raw(raw_missing_array(
            TEXTOID,
            &short_varlena(b"hi"),
        )));
        assert_eq!(missing_value_for(&a), build::text("hi"));
    }

    #[test]
    fn fast_default_raw_tier3_stays_pending_not_dropped() {
        // tsvector has no local text form: raw must survive as pending, not NULL
        let mut a = rel_attr(1, "t", TSVECTOROID, -1, 'i');
        let body = [0x01u8, 0x20, 0x00];
        a.missing_default = Some(MissingDefault::Raw(raw_missing_array(
            TSVECTOROID,
            &short_varlena(&body),
        )));
        // Tier-3 fast default stays pending
        assert_eq!(missing_value_for(&a), build::bytes(body));
    }

    #[test]
    fn fast_default_raw_jsonb_stays_stored() {
        // `{}`: bare container header, JB_FOBJECT with no pairs
        let mut a = rel_attr(1, "j", JSONBOID, -1, 'i');
        a.missing_default = Some(MissingDefault::Raw(raw_missing_array(
            JSONBOID,
            &short_varlena(&0x2000_0000u32.to_le_bytes()),
        )));
        assert_eq!(
            missing_value_for(&a),
            build::bytes(0x2000_0000u32.to_le_bytes())
        );
    }

    #[test]
    fn fast_default_malformed_raw_is_null_not_panic() {
        let mut a = rel_attr(1, "x", INT4OID, 4, 'i');
        a.missing_default = Some(MissingDefault::Raw(vec![0, 1, 2]));
        assert_eq!(missing_value_for(&a), Datum::Null);
    }

    #[test]
    fn fast_default_equivalent_across_raw_and_text() {
        let mut raw7 = rel_attr(1, "x", INT4OID, 4, 'i');
        raw7.missing_default = Some(MissingDefault::Raw(raw_missing_array(
            INT4OID,
            &7i32.to_le_bytes(),
        )));
        let mut text7 = rel_attr(1, "x", INT4OID, 4, 'i');
        text7.missing_default = Some("7".into());
        assert!(missing_defaults_equivalent(&raw7, &text7));

        let mut text8 = rel_attr(1, "x", INT4OID, 4, 'i');
        text8.missing_default = Some("8".into());
        assert!(!missing_defaults_equivalent(&raw7, &text8));

        let mut raw_ts = rel_attr(1, "t", TSVECTOROID, -1, 'i');
        raw_ts.missing_default = Some(MissingDefault::Raw(raw_missing_array(
            TSVECTOROID,
            &short_varlena(&[0x01, 0x20, 0x00]),
        )));
        let mut text_ts = rel_attr(1, "t", TSVECTOROID, -1, 'i');
        text_ts.missing_default = Some("'a' 'b'".into());
        assert!(missing_defaults_equivalent(&raw_ts, &text_ts));
    }

    /// Raw and text both render, so jsonb equivalence is by document
    #[test]
    fn fast_default_equivalent_across_raw_and_text_jsonb() {
        let mut raw_j = rel_attr(1, "j", JSONBOID, -1, 'i');
        raw_j.missing_default = Some(MissingDefault::Raw(raw_missing_array(
            JSONBOID,
            &short_varlena(&0x2000_0000u32.to_le_bytes()),
        )));
        let mut text_j = rel_attr(1, "j", JSONBOID, -1, 'i');
        text_j.missing_default = Some("{}".into());
        assert!(missing_defaults_equivalent(&raw_j, &text_j));

        let mut other = rel_attr(1, "j", JSONBOID, -1, 'i');
        other.missing_default = Some(r#"{"a": 1}"#.into());
        assert!(!missing_defaults_equivalent(&raw_j, &other));
    }

    #[test]
    fn local_matrix_agrees_with_codecs() {
        use crate::emit::ch_emitter::Codec;
        let fixed: [(u32, i16); 26] = [
            (BOOLOID, 1),
            (CHAROID, 1),
            (INT2OID, 2),
            (INT4OID, 4),
            (INT8OID, 8),
            (FLOAT4OID, 4),
            (FLOAT8OID, 8),
            (OIDOID, 4),
            (DATEOID, 4),
            (TIMEOID, 8),
            (TIMESTAMPOID, 8),
            (TIMESTAMPTZOID, 8),
            (TIMETZOID, 12),
            (UUIDOID, 16),
            (INTERVALOID, 16),
            (NAMEOID, 64),
            (XIDOID, 4),
            (CIDOID, 4),
            (OID8OID, 8),
            (XID8OID, 8),
            (PG_LSNOID, 8),
            (MONEYOID, 8),
            (MACADDROID, 6),
            (MACADDR8OID, 8),
            (TIDOID, 6),
            (2202, 4),
        ];
        for (oid, len) in fixed {
            let att = rel_attr(1, "c", oid, len, 'i');
            assert_eq!(
                Codec::choose(&att) != Codec::Pending,
                local_matrix_covers(oid, len),
                "oid {oid} typlen {len}"
            );
        }

        let varlena: [(u32, Vec<u8>); 9] = [
            (BYTEAOID, vec![1, 2]),
            (TEXTOID, b"x".to_vec()),
            (VARCHAROID, b"x".to_vec()),
            (BPCHAROID, b"x".to_vec()),
            (NUMERICOID, {
                let mut n = 0x8000u16.to_le_bytes().to_vec();
                n.extend_from_slice(&1i16.to_le_bytes());
                n
            }),
            (
                INETOID,
                vec![crate::decode::codecs::PGSQL_AF_INET, 32, 1, 2, 3, 4],
            ),
            (
                CIDROID,
                vec![crate::decode::codecs::PGSQL_AF_INET, 32, 1, 2, 3, 4],
            ),
            (JSONOID, b"{}".to_vec()),
            (3802, vec![0, 0, 0, 0]),
        ];
        for (oid, _) in varlena {
            let att = rel_attr(1, "c", oid, -1, 'i');
            assert_eq!(
                Codec::choose(&att) != Codec::Pending,
                local_matrix_covers(oid, -1),
                "varlena oid {oid}"
            );
        }
    }

    #[test]
    fn is_replica_identity_attr_matrix() {
        let full = ReplIdent::Full { pk_attnums: None };
        let nothing = ReplIdent::Nothing;
        let default_pk = ReplIdent::Default {
            pk_attnums: Some(vec![1, 2]),
        };
        let default_no_pk = ReplIdent::Default { pk_attnums: None };
        let idx = ReplIdent::UsingIndex {
            index_oid: 50000,
            key_attnums: vec![3],
        };
        assert!(is_replica_identity_attr(&full, 1));
        assert!(is_replica_identity_attr(&full, 99));
        assert!(!is_replica_identity_attr(&nothing, 1));
        assert!(is_replica_identity_attr(&default_pk, 1));
        assert!(is_replica_identity_attr(&default_pk, 2));
        assert!(!is_replica_identity_attr(&default_pk, 3));
        assert!(!is_replica_identity_attr(&default_no_pk, 1));
        assert!(is_replica_identity_attr(&idx, 3));
        assert!(!is_replica_identity_attr(&idx, 1));
    }
}
