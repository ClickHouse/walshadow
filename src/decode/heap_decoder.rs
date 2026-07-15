//! User-heap tuple decoder + Tier 1/2 type matrix.
//!
//! Tier 1 (fixed-width) + Tier 2 (length-prefixed mechanical) types decode
//! inline; Tier 3 (`numeric`, `jsonb`, arrays) defers to the codecs.
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
//! | `UsingIndex` (`'i'`) | indexed columns only, others written as NULL via bitmap | decode all; non-indexed columns surface as `Some(ColumnValue::Null)` |
//! | `Default` (`'d'`) with PK | PK columns when `XLH_UPDATE_CONTAINS_OLD_KEY` set | same as `UsingIndex` shape, with PK attnums populated |
//! | `Default` (`'d'`) no PK | nothing (PG never sets OLD_KEY) | `old = None` |
//! | `Nothing` (`'n'`) | empty | `old = None`; partial = true noted in stats |
//!
//! ## What lives in `block.data` vs the FPI
//!
//! PG `heap_insert`/`heap_update` set `REGBUF_KEEP_DATA` under
//! `RelationIsLogicallyLogged(rel)` (holds at `wal_level=logical`,
//! walshadow's hard floor, PLAN.md "Pitfalls/wal_level on source"). With
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

use std::borrow::Cow;

use smallvec::{SmallVec, smallvec};
use thiserror::Error;
use walrus::pg::walparser::{RelFileNode, RmId, XLogRecord};

use crate::schema::{
    BOOLOID, BPCHAROID, BYTEAOID, CHAROID, CIDROID, DATEOID, FLOAT4OID, FLOAT8OID, INETOID,
    INT2OID, INT4OID, INT8OID, INTERVALOID, JSONOID, NAMEOID, NUMERICOID, OIDOID, RelAttr,
    RelDescriptor, ReplIdent, TEXTOID, TIMEOID, TIMESTAMPOID, TIMESTAMPTZOID, TIMETZOID, UUIDOID,
    VARCHAROID,
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
pub const XLOG_HEAP_LOCK: u8 = 0x60;
pub const XLOG_HEAP_INPLACE: u8 = 0x70;

pub const XLOG_HEAP2_MULTI_INSERT: u8 = 0x50;

// xl_heap_update.flags bit positions (heapam_xlog.h).
pub const XLH_UPDATE_CONTAINS_OLD_TUPLE: u8 = 1 << 2;
pub const XLH_UPDATE_CONTAINS_OLD_KEY: u8 = 1 << 3;
pub const XLH_UPDATE_CONTAINS_NEW_TUPLE: u8 = 1 << 4;
pub const XLH_UPDATE_PREFIX_FROM_OLD: u8 = 1 << 5;
pub const XLH_UPDATE_SUFFIX_FROM_OLD: u8 = 1 << 6;

// xl_heap_delete.flags bit positions.
pub const XLH_DELETE_CONTAINS_OLD_TUPLE: u8 = 1 << 1;
pub const XLH_DELETE_CONTAINS_OLD_KEY: u8 = 1 << 2;

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
    BadHoff { offset: usize, t_hoff: usize },
    #[error("nominal alignment char {0:?} not one of c/s/i/d")]
    BadAlign(char),
    #[error("xl_heap_update prefix/suffix bytes exceed block.data ({need} > {have})")]
    BadPrefixSuffix { need: usize, have: usize },
}

/// Tier 1/2 decoded value space. Out-of-matrix types surface as
/// [`ColumnValue::Unsupported`] or [`ColumnValue::ExternalToast`].
#[derive(Debug, Clone, PartialEq)]
pub enum ColumnValue {
    Null,
    Bool(bool),
    /// `char` (typname='char'). PG stores as `int8`; surface `i8` to
    /// distinguish from `int2`.
    Char(i8),
    Int2(i16),
    Int4(i32),
    Int8(i64),
    Float4(f32),
    Float8(f64),
    Oid(u32),
    /// `date`, days since PG epoch 2000-01-01, negative for pre-epoch.
    Date(i32),
    /// `time`, microseconds since midnight. Legacy float8 storage
    /// (`--disable-integer-datetimes`) removed in PG 10.
    Time(i64),
    /// `timestamp`, microseconds since PG epoch 2000-01-01 00:00:00 UTC.
    Timestamp(i64),
    /// `timestamptz`, same storage as `timestamp`; tz is presentation-only
    /// in PG, bytes identical.
    TimestampTz(i64),
    /// `timetz`, `time` storage + 4-byte UTC offset (seconds, negative for
    /// east of UTC per PG sign convention).
    TimeTz {
        micros: i64,
        tz_seconds: i32,
    },
    /// `uuid`, 16 raw bytes network byte order on disk (PG `uuid_send`, memcpy).
    Uuid([u8; 16]),
    /// `name`, NUL-padded NAMEDATALEN(64) C string, no varlena header;
    /// surface trimmed.
    Name(String),
    Bytea(Vec<u8>),
    /// `text`/`varchar`/`bpchar`, varlena + UTF-8 decode. Invalid UTF-8
    /// surfaces as [`ColumnValue::Bytea`].
    Text(String),
    /// `numeric` Tier 3. PG-text form for finite; NaN/Infinity carry their
    /// flag. Emitter maps to CH `String` (PG precision is per-row, no fixed
    /// CH `Decimal` fits without operator config).
    Numeric(crate::decode::codecs::NumericKind),
    /// `inet`/`cidr` Tier 3. Emit via [`crate::decode::codecs::InetValue::to_text`].
    Inet(crate::decode::codecs::InetValue),
    /// `interval` Tier 3, 16-byte fixed-width (months, days, micros).
    Interval(crate::decode::codecs::IntervalValue),
    /// `json` Tier 3, varlena text on disk, passed through unchanged.
    Json(String),
    /// Tier 3 deferred (not numeric/inet/interval/json). Carries raw
    /// on-disk body; resolved to text at emit via
    /// `walshadow_decode_disk(oid, bytea) -> text` against shadow PG
    /// (`walshadow` extension). Absent extension: emitter writes `<oid:N>`,
    /// bumps `unsupported_values`.
    PgPending {
        type_oid: u32,
        raw: Vec<u8>,
    },
    /// On-disk TOAST pointer; xact buffer's TOAST reassembly dereferences.
    ExternalToast(ToastPointer),
    /// Type OID outside decoder matrix, carries raw bytes.
    Unsupported {
        type_oid: u32,
        raw: Vec<u8>,
    },
}

impl ColumnValue {
    /// Approximate in-memory payload bytes, to bound the decode pool's
    /// coalescing buffer. Detoast has already run by route time so
    /// `ExternalToast` pointers are resolved into `Text`/`Bytea`. Exhaustive
    /// so a new variant forces a width.
    pub fn approx_bytes(&self) -> usize {
        use crate::decode::codecs::NumericKind;
        match self {
            ColumnValue::Null => 0,
            ColumnValue::Bool(_) | ColumnValue::Char(_) => 1,
            ColumnValue::Int2(_) => 2,
            ColumnValue::Int4(_)
            | ColumnValue::Float4(_)
            | ColumnValue::Oid(_)
            | ColumnValue::Date(_) => 4,
            ColumnValue::Int8(_)
            | ColumnValue::Float8(_)
            | ColumnValue::Time(_)
            | ColumnValue::Timestamp(_)
            | ColumnValue::TimestampTz(_) => 8,
            ColumnValue::TimeTz { .. } => 12,
            ColumnValue::Uuid(_) => 16,
            ColumnValue::Interval(_) => 16,
            ColumnValue::Inet(v) => 3 + v.addr.len(),
            ColumnValue::Numeric(NumericKind::Finite(s)) => s.len(),
            ColumnValue::Numeric(_) => 0,
            ColumnValue::Name(s) | ColumnValue::Text(s) | ColumnValue::Json(s) => s.len(),
            ColumnValue::Bytea(b) => b.len(),
            ColumnValue::PgPending { raw, .. } | ColumnValue::Unsupported { raw, .. } => raw.len(),
            ColumnValue::ExternalToast(_) => 16,
        }
    }
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

impl DecodedHeap {
    /// Sum across both images (REPLICA IDENTITY FULL keeps a fat `old` too).
    pub fn approx_bytes(&self) -> usize {
        let image = |t: &Option<DecodedTuple>| {
            t.as_ref().map_or(0, |t| {
                t.columns
                    .iter()
                    .flatten()
                    .map(ColumnValue::approx_bytes)
                    .sum::<usize>()
            })
        };
        image(&self.new) + image(&self.old)
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

/// One drained tuple, fully reassembled. `commit_ts` is the commit-record
/// `xact_time` carried through [`crate::xact::xact_buffer::XactBuffer::commit`].
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
    /// `Some(ColumnValue::Null)` is explicit NULL (bitmap bit clear).
    pub columns: Vec<Option<ColumnValue>>,
    /// True iff a column was elided by PG prefix/suffix compression; xact
    /// buffer backfills from previous tuple image.
    pub partial: bool,
}

/// Top-level decode entry point. One-element [`DecodedHeaps`] for
/// INSERT/UPDATE/HOT_UPDATE/DELETE, N-element for `XLOG_HEAP2_MULTI_INSERT`,
/// empty for ops with no tuple payload (LOCK, INPLACE, TRUNCATE, other
/// `RM_HEAP2_ID` ops, MULTI_INSERT lacking `XLH_INSERT_CONTAINS_NEW_TUPLE`).
/// Unrecognised op codes skip silently; only malformed bytes return `Err`.
///
/// `rel` must describe `record.blocks[0].header.location.rel`, fetched via
/// [`ShadowCatalog::relation_at`](crate::catalog::shadow_catalog::ShadowCatalog::relation_at).
pub fn decode_heap_record(
    record: &XLogRecord,
    source_lsn: u64,
    rel: &RelDescriptor,
) -> Result<DecodedHeaps, DecodeError> {
    let rm = record.header.resource_manager_id;
    if rm != RmId::Heap as u8 && rm != RmId::Heap2 as u8 {
        return Ok(SmallVec::new());
    }
    let info_op = record.header.info & XLOG_HEAP_OPMASK;
    let rfn = record
        .blocks
        .first()
        .map(|b| b.header.location.rel)
        .unwrap_or_default();
    let xid = record.header.xact_id;

    if rm == RmId::Heap as u8 {
        match info_op {
            XLOG_HEAP_INSERT => Ok(smallvec![decode_insert(record, source_lsn, rfn, xid, rel)?]),
            XLOG_HEAP_UPDATE => Ok(smallvec![decode_update(
                record, source_lsn, rfn, xid, rel, false,
            )?]),
            XLOG_HEAP_HOT_UPDATE => Ok(smallvec![decode_update(
                record, source_lsn, rfn, xid, rel, true,
            )?]),
            XLOG_HEAP_DELETE => Ok(smallvec![
                decode_delete(record, source_lsn, rfn, xid, rel,)?
            ]),
            _ => Ok(SmallVec::new()),
        }
    } else {
        match info_op {
            XLOG_HEAP2_MULTI_INSERT => decode_multi_insert(record, source_lsn, rfn, xid, rel),
            _ => Ok(SmallVec::new()),
        }
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

fn decode_multi_insert(
    record: &XLogRecord,
    source_lsn: u64,
    rfn: RelFileNode,
    xid: u32,
    rel: &RelDescriptor,
) -> Result<DecodedHeaps, DecodeError> {
    let md = &record.main_data;
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
    let init_page = record.header.info & XLOG_HEAP_INIT_PAGE != 0;
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

    let Some(block) = record.blocks.first() else {
        return Ok(SmallVec::new());
    };
    let data = &block.data;
    let mut cursor = 0usize;
    let mut out: DecodedHeaps = SmallVec::with_capacity(ntuples);
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
        let t_infomask2 = u16::from_le_bytes([data[cursor + 2], data[cursor + 3]]);
        let t_infomask = u16::from_le_bytes([data[cursor + 4], data[cursor + 5]]);
        let t_hoff = data[cursor + 6];
        cursor += SIZE_OF_MULTI_INSERT_TUPLE;
        if data.len() < cursor + datalen {
            return Err(DecodeError::Truncated {
                offset: cursor,
                need: datalen,
                have: data.len().saturating_sub(cursor),
            });
        }
        // Multi-insert strips the 5-byte xl_heap_header at write time and
        // stores (t_infomask2, t_infomask, t_hoff) on xl_multi_insert_tuple.
        // Re-synthesize those 5 bytes + tuple body for decode_tuple_payload
        let mut synth = Vec::with_capacity(SIZE_OF_HEAP_HEADER + datalen);
        synth.extend_from_slice(&t_infomask2.to_le_bytes());
        synth.extend_from_slice(&t_infomask.to_le_bytes());
        synth.push(t_hoff);
        synth.extend_from_slice(&data[cursor..cursor + datalen]);
        cursor += datalen;
        let tup = decode_tuple_payload(&synth, 0, rel, 0, 0)?;
        out.push(DecodedHeap {
            rfn,
            xid,
            source_lsn,
            op: HeapOp::Insert,
            new: Some(tup),
            old: None,
        });
    }
    Ok(out)
}

fn decode_insert(
    record: &XLogRecord,
    source_lsn: u64,
    rfn: RelFileNode,
    xid: u32,
    rel: &RelDescriptor,
) -> Result<DecodedHeap, DecodeError> {
    let Some(block) = record.blocks.first() else {
        // PG always references target page; emit empty new rather than
        // fail the stream
        return Ok(DecodedHeap {
            rfn,
            xid,
            source_lsn,
            op: HeapOp::Insert,
            new: None,
            old: None,
        });
    };
    let new = decode_new_tuple_block(&block.data, 0, rel)?;
    Ok(DecodedHeap {
        rfn,
        xid,
        source_lsn,
        op: HeapOp::Insert,
        new: Some(new),
        old: None,
    })
}

fn decode_update(
    record: &XLogRecord,
    source_lsn: u64,
    rfn: RelFileNode,
    xid: u32,
    rel: &RelDescriptor,
    hot: bool,
) -> Result<DecodedHeap, DecodeError> {
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

    let new = if let Some(b) = record.blocks.first() {
        let mut cursor = 0;
        let prefixlen = if has_prefix {
            read_u16(&b.data, &mut cursor)? as usize
        } else {
            0
        };
        let suffixlen = if has_suffix {
            read_u16(&b.data, &mut cursor)? as usize
        } else {
            0
        };
        let mut t = decode_tuple_payload(&b.data, cursor, rel, prefixlen, suffixlen)?;
        if has_prefix || has_suffix {
            t.partial = true;
        }
        Some(t)
    } else {
        None
    };

    let old = if has_old_tuple || has_old_key {
        decode_old_tuple_from_main_data(&record.main_data, SIZE_OF_HEAP_UPDATE, rel)?
    } else {
        None
    };

    let op = if hot {
        HeapOp::HotUpdate
    } else {
        HeapOp::Update
    };
    Ok(DecodedHeap {
        rfn,
        xid,
        source_lsn,
        op,
        new,
        old,
    })
}

fn decode_delete(
    record: &XLogRecord,
    source_lsn: u64,
    rfn: RelFileNode,
    xid: u32,
    rel: &RelDescriptor,
) -> Result<DecodedHeap, DecodeError> {
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
        decode_old_tuple_from_main_data(&record.main_data, SIZE_OF_HEAP_DELETE, rel)?
    } else {
        None
    };
    Ok(DecodedHeap {
        rfn,
        xid,
        source_lsn,
        op: HeapOp::Delete,
        new: None,
        old,
    })
}

/// Block-0 data shape: `xl_heap_header (5)` + bitmap[+pad] + col data.
fn decode_new_tuple_block(
    data: &[u8],
    data_start: usize,
    rel: &RelDescriptor,
) -> Result<DecodedTuple, DecodeError> {
    decode_tuple_payload(data, data_start, rel, 0, 0)
}

fn decode_old_tuple_from_main_data(
    main_data: &[u8],
    header_off: usize,
    rel: &RelDescriptor,
) -> Result<Option<DecodedTuple>, DecodeError> {
    if main_data.len() < header_off + SIZE_OF_HEAP_HEADER {
        return Err(DecodeError::Truncated {
            offset: header_off,
            need: SIZE_OF_HEAP_HEADER,
            have: main_data.len() - header_off,
        });
    }
    Ok(Some(decode_tuple_payload(
        main_data, header_off, rel, 0, 0,
    )?))
}

/// Decode a `block.data`-shaped tuple for the bootstrap page-walk sink,
/// which reshapes on-disk `HeapTupleHeaderData`-prefixed tuples into this
/// form. `prefixlen=suffixlen=0`: WAL prefix/suffix-from-old compression
/// doesn't apply at backup time.
pub(crate) fn decode_block_data(
    block_data: &[u8],
    rel: &RelDescriptor,
) -> Result<DecodedTuple, DecodeError> {
    decode_tuple_payload(block_data, 0, rel, 0, 0)
}

/// Walk a tuple payload (`xl_heap_header` at `header_off`, then bitmap,
/// padding, col data). `prefixlen`/`suffixlen` are PG `XLH_UPDATE_*_FROM_OLD`
/// byte counts elided from front/back of the logical col-data region; columns
/// wholly inside those regions surface `None` and caller flips `partial`.
/// Bitmap + xl_heap_header precede prefix elision so are always present at
/// `header_off`.
fn decode_tuple_payload(
    buf: &[u8],
    header_off: usize,
    rel: &RelDescriptor,
    prefixlen: usize,
    suffixlen: usize,
) -> Result<DecodedTuple, DecodeError> {
    if buf.len() < header_off + SIZE_OF_HEAP_HEADER {
        return Err(DecodeError::Truncated {
            offset: header_off,
            need: SIZE_OF_HEAP_HEADER,
            have: buf.len().saturating_sub(header_off),
        });
    }
    let t_infomask2 = u16::from_le_bytes([buf[header_off], buf[header_off + 1]]);
    let t_infomask = u16::from_le_bytes([buf[header_off + 2], buf[header_off + 3]]);
    let t_hoff = buf[header_off + 4] as usize;
    if t_hoff < SIZE_OF_HEAP_TUPLE_HEADER {
        return Err(DecodeError::BadHoff {
            offset: header_off,
            t_hoff,
        });
    }
    let natts = (t_infomask2 & HEAP_NATTS_MASK) as usize;
    let has_null = t_infomask & HEAP_HASNULL != 0;

    let bitmap_bytes = if has_null { natts.div_ceil(8) } else { 0 };
    // PG tuple: header(23) -> bitmap -> MAXALIGN(8) pad -> col data at t_hoff.
    // WAL slice elides the 23-byte fixed header, so bitmap is at header_off + 5
    let bitmap_off = header_off + SIZE_OF_HEAP_HEADER;
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
    let col_data_off = header_off + SIZE_OF_HEAP_HEADER + (t_hoff - SIZE_OF_HEAP_TUPLE_HEADER);

    let mut columns = Vec::with_capacity(rel.attributes.len());
    // `cur` is offset within the *logical* col-data region (relative to
    // t_hoff); align matches PG `att_align_nominal` against this offset. t_hoff
    // is MAXALIGN'd(8) so col 1 starts clean. WAL offset of logical `cur` is
    // `col_data_off + (cur - prefixlen)` when `cur >= prefixlen`
    let mut cur: usize = 0;
    let attrs = effective_attrs(rel, natts);
    let wal_col_data_avail = buf.len().saturating_sub(col_data_off);
    // suffix bytes are past the WAL slice; buf len already accounts for them so
    // suffixlen goes unconsulted
    let _ = suffixlen;
    let logical_end = wal_col_data_avail + prefixlen;
    for (idx, att) in attrs.iter().enumerate() {
        if idx >= natts {
            // wal natts < catalog natts: trailing cols added via ALTER ADD
            // COLUMN after the record. Mirror PG `getmissingattr`
            // (heaptuple.c) `attmissingval[1]` fast-path default
            columns.push(Some(missing_value_for(att)));
            continue;
        }
        if has_null && !bitmap_is_set(bitmap, idx) {
            columns.push(Some(ColumnValue::Null));
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

        if cur < prefixlen {
            let len = decoded_size_or_skip(att);
            match len {
                Some(n) => {
                    columns.push(None);
                    cur += n;
                }
                None => {
                    // Varlena in prefix: can't advance cur without the bytes.
                    // Mark this + every later col absent, bail; xact buffer
                    // reconstructs
                    columns.push(None);
                    for _ in (idx + 1)..attrs.len() {
                        columns.push(None);
                    }
                    return Ok(DecodedTuple {
                        columns,
                        partial: true,
                    });
                }
            }
            continue;
        }

        if cur >= logical_end {
            // In suffix / past EOF
            columns.push(None);
            // Advance cur for fixed-width accounting only; all emit None
            if let Some(n) = decoded_size_or_skip(att) {
                cur += n;
            } else {
                // Varlena tail; remaining cols stay absent
                for _ in (idx + 1)..attrs.len() {
                    columns.push(None);
                }
                return Ok(DecodedTuple {
                    columns,
                    partial: true,
                });
            }
            continue;
        }

        let abs = col_data_off + (cur - prefixlen);
        let (value, consumed) = match decode_one_value(att, buf, abs) {
            Ok(v) => v,
            Err(DecodeError::Truncated { .. }) => {
                columns.push(None);
                for _ in (idx + 1)..attrs.len() {
                    columns.push(None);
                }
                return Ok(DecodedTuple {
                    columns,
                    partial: true,
                });
            }
            Err(e) => return Err(e),
        };
        columns.push(Some(value));
        cur += consumed;
    }
    Ok(DecodedTuple {
        columns,
        partial: false,
    })
}

/// Resolve `RelAttr::missing_text` (PG `attmissingval[1]::text`) via the
/// Tier 1/2 matrix; Tier 3 falls to `PgPending` (oracle resolves at emit).
/// `Null` when attribute has no missing default.
pub fn missing_value_for(att: &RelAttr) -> ColumnValue {
    let Some(text) = att.missing_text.as_deref() else {
        return ColumnValue::Null;
    };
    match att.type_oid {
        BOOLOID => match text {
            "t" | "true" | "yes" | "on" | "1" => ColumnValue::Bool(true),
            _ => ColumnValue::Bool(false),
        },
        CHAROID => text
            .as_bytes()
            .first()
            .map(|b| ColumnValue::Char(*b as i8))
            .unwrap_or(ColumnValue::Null),
        INT2OID => text
            .parse::<i16>()
            .map(ColumnValue::Int2)
            .unwrap_or(ColumnValue::Null),
        INT4OID => text
            .parse::<i32>()
            .map(ColumnValue::Int4)
            .unwrap_or(ColumnValue::Null),
        INT8OID => text
            .parse::<i64>()
            .map(ColumnValue::Int8)
            .unwrap_or(ColumnValue::Null),
        FLOAT4OID => text
            .parse::<f32>()
            .map(ColumnValue::Float4)
            .unwrap_or(ColumnValue::Null),
        FLOAT8OID => text
            .parse::<f64>()
            .map(ColumnValue::Float8)
            .unwrap_or(ColumnValue::Null),
        OIDOID => text
            .parse::<u32>()
            .map(ColumnValue::Oid)
            .unwrap_or(ColumnValue::Null),
        TEXTOID | VARCHAROID | BPCHAROID | NAMEOID => ColumnValue::Text(text.to_owned()),
        JSONOID => ColumnValue::Json(text.to_owned()),
        // typoutput text is what typinput expects, so shadow recovers bytea
        // via text -> oid -> typinput -> typsend
        _ => ColumnValue::PgPending {
            type_oid: att.type_oid,
            raw: text.as_bytes().to_vec(),
        },
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
fn align_for(cur: usize, attalign: char) -> usize {
    match attalign {
        'c' => cur,
        's' => align_up(cur, 2),
        'i' => align_up(cur, 4),
        'd' => align_up(cur, 8),
        _ => cur,
    }
}

/// Always `rel.attributes`. Replica-identity-only writes (UsingIndex/Default)
/// carry NULL placeholders for non-indexed cols, so wire natts equals rel
/// natts. Post-ALTER-ADD-COLUMN trailing attrs (`natts_in_wal < natts`)
/// surface via PG missing-column handling.
fn effective_attrs(rel: &RelDescriptor, _natts: usize) -> &[RelAttr] {
    &rel.attributes
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
        'c' => cur_offset,              // TYPALIGN_CHAR
        's' => align_up(cur_offset, 2), // TYPALIGN_SHORT
        'i' => align_up(cur_offset, 4), // TYPALIGN_INT
        'd' => align_up(cur_offset, 8), // TYPALIGN_DOUBLE
        _ => cur_offset,                // unknown: no-align rather than panic
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

/// Returns `(value, bytes_consumed)`. Varlena reports total on-disk length
/// (header + body, including TOAST pointer's full 18 bytes).
fn decode_one_value(
    att: &RelAttr,
    buf: &[u8],
    abs: usize,
) -> Result<(ColumnValue, usize), DecodeError> {
    if att.dropped {
        // Dropped cols retain attlen/attalign/typbyval per catalog
        // convention; still read varlena header to advance cursor
        let consumed = consume_dropped(att, buf, abs)?;
        return Ok((ColumnValue::Null, consumed));
    }

    if att.type_len == -1 {
        return decode_varlena(att, buf, abs);
    }
    if att.type_len == -2 {
        return decode_cstring(buf, abs);
    }
    let len = att.type_len as usize;
    if buf.len() < abs + len {
        return Err(DecodeError::Truncated {
            offset: abs,
            need: len,
            have: buf.len().saturating_sub(abs),
        });
    }
    let body = &buf[abs..abs + len];

    let v = match att.type_oid {
        BOOLOID => ColumnValue::Bool(body[0] != 0),
        CHAROID => ColumnValue::Char(body[0] as i8),
        INT2OID => ColumnValue::Int2(i16::from_le_bytes(body.try_into().unwrap())),
        INT4OID => ColumnValue::Int4(i32::from_le_bytes(body.try_into().unwrap())),
        INT8OID => ColumnValue::Int8(i64::from_le_bytes(body.try_into().unwrap())),
        FLOAT4OID => ColumnValue::Float4(f32::from_le_bytes(body.try_into().unwrap())),
        FLOAT8OID => ColumnValue::Float8(f64::from_le_bytes(body.try_into().unwrap())),
        OIDOID => ColumnValue::Oid(u32::from_le_bytes(body.try_into().unwrap())),
        DATEOID => ColumnValue::Date(i32::from_le_bytes(body.try_into().unwrap())),
        TIMEOID => ColumnValue::Time(i64::from_le_bytes(body.try_into().unwrap())),
        TIMESTAMPOID => ColumnValue::Timestamp(i64::from_le_bytes(body.try_into().unwrap())),
        TIMESTAMPTZOID => ColumnValue::TimestampTz(i64::from_le_bytes(body.try_into().unwrap())),
        TIMETZOID => {
            // 12 bytes: 8-byte time + 4-byte tz offset
            let micros = i64::from_le_bytes(body[..8].try_into().unwrap());
            let tz_seconds = i32::from_le_bytes(body[8..12].try_into().unwrap());
            ColumnValue::TimeTz { micros, tz_seconds }
        }
        UUIDOID => ColumnValue::Uuid(body.try_into().unwrap()),
        INTERVALOID => match crate::decode::codecs::decode_interval(body) {
            Ok(v) => ColumnValue::Interval(v),
            Err(_) => ColumnValue::Unsupported {
                type_oid: att.type_oid,
                raw: body.to_vec(),
            },
        },
        _ => ColumnValue::Unsupported {
            type_oid: att.type_oid,
            raw: body.to_vec(),
        },
    };
    Ok((v, len))
}

/// Skip a dropped column; still read varlena header to advance correctly.
fn consume_dropped(att: &RelAttr, buf: &[u8], abs: usize) -> Result<usize, DecodeError> {
    if att.type_len == -1 {
        let (_, consumed) = decode_varlena(att, buf, abs)?;
        Ok(consumed)
    } else if att.type_len == -2 {
        let (_, consumed) = decode_cstring(buf, abs)?;
        Ok(consumed)
    } else {
        let len = att.type_len as usize;
        if buf.len() < abs + len {
            return Err(DecodeError::Truncated {
                offset: abs,
                need: len,
                have: buf.len().saturating_sub(abs),
            });
        }
        Ok(len)
    }
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
        TOAST_COMPRESSION_LZ4 => lz4_flex::decompress(src, raw_len).ok(),
        _ => None,
    }
}

/// Decode a varlena attribute (`typlen == -1`). PG varlena formats (`varatt.h`):
///
/// - `0bxxxxxx00` 4-byte length, uncompressed.
/// - `0bxxxxxx10` 4-byte length, in-line compressed (surfaced `Unsupported`;
///   later codec detoasts pglz/lz4).
/// - `0b00000001` TOAST pointer (`varattrib_1b_e`, 18 bytes on disk).
/// - `0bxxxxxxx1` 1-byte length, uncompressed short header.
///
/// Little-endian only (`WORDS_BIGENDIAN` dead on x86/aarch64).
fn decode_varlena(
    att: &RelAttr,
    buf: &[u8],
    abs: usize,
) -> Result<(ColumnValue, usize), DecodeError> {
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
            let va_rawsize = i32::from_le_bytes(body[0..4].try_into().unwrap());
            let va_extinfo = u32::from_le_bytes(body[4..8].try_into().unwrap());
            let va_valueid = u32::from_le_bytes(body[8..12].try_into().unwrap());
            let va_toastrelid = u32::from_le_bytes(body[12..16].try_into().unwrap());
            return Ok((
                ColumnValue::ExternalToast(ToastPointer {
                    va_rawsize,
                    va_extinfo,
                    va_valueid,
                    va_toastrelid,
                }),
                18,
            ));
        }
        // INDIRECT/EXPANDED tags are in-memory only, never on disk (varatt.h);
        // forward Unsupported so a corrupt stream surfaces
        return Ok((
            ColumnValue::Unsupported {
                type_oid: att.type_oid,
                raw: buf[abs..].to_vec(),
            },
            buf.len() - abs,
        ));
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
        let body = &buf[abs + 1..abs + total];
        let v = varlena_to_value(att.type_oid, Cow::Borrowed(body));
        return Ok((v, total));
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
        let tcinfo = u32::from_le_bytes(buf[abs + 4..abs + 8].try_into().unwrap());
        let raw_len = (tcinfo & VARLENA_EXTSIZE_MASK) as usize;
        let method = (tcinfo >> VARLENA_EXTSIZE_BITS) as u8;
        let body = &buf[abs + 8..abs + total];
        let v = decompress_varlena(method, body, raw_len)
            .map(|out| varlena_to_value(att.type_oid, Cow::Owned(out)))
            .unwrap_or_else(|| ColumnValue::Unsupported {
                type_oid: att.type_oid,
                raw: buf[abs..abs + total].to_vec(),
            });
        return Ok((v, total));
    }
    let body = &buf[abs + 4..abs + total];
    Ok((varlena_to_value(att.type_oid, Cow::Borrowed(body)), total))
}

/// Map a varlena body (header stripped, decompressed) to a typed value by the
/// column's type OID. Shared by the inline decoder (`Cow::Borrowed` into the
/// WAL buffer) and the TOAST reassembler (`Cow::Owned`, so `detoasted_value`
/// moves the reassembled buffer into the value instead of copying it). A
/// detoasted value resolves identically to an inline one — Tier 3 types land
/// as `PgPending`, resolved at emit by the oracle.
pub(crate) fn varlena_to_value(type_oid: u32, body: Cow<[u8]>) -> ColumnValue {
    match type_oid {
        BYTEAOID => ColumnValue::Bytea(body.into_owned()),
        TEXTOID | VARCHAROID | BPCHAROID => match String::from_utf8(body.into_owned()) {
            Ok(s) => ColumnValue::Text(s),
            // PG validates UTF-8 on input; surface bytea not crash if ever invalid
            Err(e) => ColumnValue::Bytea(e.into_bytes()),
        },
        NUMERICOID => match crate::decode::codecs::decode_numeric(&body) {
            Ok(v) => ColumnValue::Numeric(v),
            Err(_) => ColumnValue::Unsupported {
                type_oid,
                raw: body.into_owned(),
            },
        },
        INETOID | CIDROID => match crate::decode::codecs::decode_inet(&body, type_oid == CIDROID) {
            Ok(v) => ColumnValue::Inet(v),
            Err(_) => ColumnValue::Unsupported {
                type_oid,
                raw: body.into_owned(),
            },
        },
        JSONOID => match String::from_utf8(body.into_owned()) {
            Ok(s) => ColumnValue::Json(s),
            Err(e) => ColumnValue::Bytea(e.into_bytes()),
        },
        // Tier 3 deferred: jsonb, range types, arrays (typcategory='A'),
        // tsvector etc. Carries full on-disk body so the SQL bridge
        // reconstructs the varlena Datum; walshadow extension resolves at emit
        _ => ColumnValue::PgPending {
            type_oid,
            raw: body.into_owned(),
        },
    }
}

/// Handle `typlen=-2` `\0`-terminated cstring (regtype/regclass typoutput
/// intermediates). Not in the type matrix, but tracking it lets dropped
/// cstring columns walk correctly. (`name`, typlen=64, goes the fixed path.)
fn decode_cstring(buf: &[u8], abs: usize) -> Result<(ColumnValue, usize), DecodeError> {
    let mut end = abs;
    while end < buf.len() && buf[end] != 0 {
        end += 1;
    }
    if end >= buf.len() {
        return Err(DecodeError::Truncated {
            offset: abs,
            need: 1,
            have: 0,
        });
    }
    let body = &buf[abs..end];
    let value = std::str::from_utf8(body)
        .map(|s| ColumnValue::Text(s.to_owned()))
        .unwrap_or_else(|_| ColumnValue::Bytea(body.to_vec()));
    Ok((value, (end - abs) + 1)) // include trailing NUL
}

/// Take `(chunk_id, chunk_seq, chunk_data)` out of a `pg_toast_*` tuple's
/// 3 columns (`chunk_id oid`, `chunk_seq int4`, `chunk_data bytea`);
/// `None` for shapes that don't fit
pub(crate) fn take_toast_chunk_columns(
    cols: &mut [Option<ColumnValue>],
) -> Option<(u32, u32, Vec<u8>)> {
    if cols.len() < 3 {
        return None;
    }
    let &ColumnValue::Oid(chunk_id) = cols[0].as_ref()? else {
        return None;
    };
    let &ColumnValue::Int4(chunk_seq) = cols[1].as_ref()? else {
        return None;
    };
    let chunk_data = match cols[2].take()? {
        ColumnValue::Bytea(b) => b,
        // Text-typed toast chunk: re-encode to bytes (not a normal flow)
        ColumnValue::Text(s) => s.into_bytes(),
        _ => return None,
    };
    Some((chunk_id, chunk_seq as u32, chunk_data))
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::RelName;
    use walrus::pg::walparser::{
        BlockLocation, XLogRecordBlock, XLogRecordBlockHeader, XLogRecordHeader,
    };

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
            missing_text: None,
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
            namespace_oid: 2200,
            rel_name: RelName::new("public", "t"),
            kind: 'r',
            persistence: 'p',
            replident: ReplIdent::Default { pk_attnums: None },
            attributes: attrs,
        }
    }

    #[test]
    fn approx_bytes_counts_scalars_and_varlena() {
        assert_eq!(ColumnValue::Null.approx_bytes(), 0);
        assert_eq!(ColumnValue::Int2(0).approx_bytes(), 2);
        assert_eq!(ColumnValue::Int4(0).approx_bytes(), 4);
        assert_eq!(ColumnValue::Int8(0).approx_bytes(), 8);
        assert_eq!(ColumnValue::Uuid([0; 16]).approx_bytes(), 16);
        assert_eq!(ColumnValue::Text("héllo".into()).approx_bytes(), 6);
        assert_eq!(ColumnValue::Bytea(vec![0u8; 100]).approx_bytes(), 100);
        assert_eq!(
            ColumnValue::PgPending {
                type_oid: 1,
                raw: vec![0u8; 7],
            }
            .approx_bytes(),
            7
        );

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
                columns: vec![
                    Some(ColumnValue::Int4(1)),
                    Some(ColumnValue::Bytea(vec![0u8; 10])),
                ],
                partial: false,
            }),
            old: Some(DecodedTuple {
                columns: vec![None, Some(ColumnValue::Text("abc".into()))],
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

    #[test]
    fn decode_cstring_text_bytea_and_truncated() {
        let (v, n) = decode_cstring(b"hello\0", 0).unwrap();
        assert_eq!(v, ColumnValue::Text("hello".into()));
        assert_eq!(n, 6);
        let (v, n) = decode_cstring(&[0xFF, 0xFE, 0x00], 0).unwrap();
        assert_eq!(v, ColumnValue::Bytea(vec![0xFF, 0xFE]));
        assert_eq!(n, 3);
        // non-zero offset within multi-string buffer
        let (v, n) = decode_cstring(b"ab\0cd\0", 3).unwrap();
        assert_eq!(v, ColumnValue::Text("cd".into()));
        assert_eq!(n, 3);
        // no terminator before buffer end
        match decode_cstring(b"hello", 0) {
            Err(DecodeError::Truncated { offset, need, have }) => {
                assert_eq!((offset, need, have), (0, 1, 0));
            }
            other => panic!("expected Truncated, got {other:?}"),
        }
    }

    #[test]
    fn decode_one_value_maps_char_to_signed_i8() {
        let att = rel_attr(1, "c", CHAROID, 1, 'c');
        let (v, n) = decode_one_value(&att, &[65u8], 0).unwrap();
        assert_eq!(v, ColumnValue::Char(65));
        assert_eq!(n, 1);
        // 0xFF -> i8 -1
        let (v, _) = decode_one_value(&att, &[0xFFu8], 0).unwrap();
        assert_eq!(v, ColumnValue::Char(-1));
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
        assert_eq!(new.columns[0], Some(ColumnValue::Int4(12345)));
        assert_eq!(new.columns[1], Some(ColumnValue::Int8(999_999)));
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
        assert_eq!(new.columns[0], Some(ColumnValue::Bool(true)));
        assert_eq!(new.columns[1], Some(ColumnValue::Text("hi".into())));
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
        assert_eq!(new.columns[0], Some(ColumnValue::Text("hi".into())));
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
        let (v, consumed) = decode_varlena(&att, &buf, 0).unwrap();
        assert_eq!(consumed, buf.len());
        assert_eq!(v, ColumnValue::Text(text));
    }

    #[test]
    fn decode_inline_compressed_lz4_text() {
        let text = "lz4-".repeat(4000);
        let comp = lz4_flex::compress(text.as_bytes());
        let buf = inline_compressed_varlena(1, &comp, text.len());
        let att = rel_attr(1, "val", TEXTOID, -1, 'i');
        let (v, consumed) = decode_varlena(&att, &buf, 0).unwrap();
        assert_eq!(consumed, buf.len());
        assert_eq!(v, ColumnValue::Text(text));
    }

    #[test]
    fn decode_inline_compressed_corrupt_surfaces_unsupported() {
        let buf = inline_compressed_varlena(0, &[0xff, 0xff, 0xff, 0xff], 4096);
        let att = rel_attr(1, "val", TEXTOID, -1, 'i');
        let (v, consumed) = decode_varlena(&att, &buf, 0).unwrap();
        assert_eq!(consumed, buf.len());
        assert!(matches!(
            v,
            ColumnValue::Unsupported {
                type_oid: TEXTOID,
                ..
            }
        ));
    }

    #[test]
    fn decompress_varlena_roundtrips_and_rejects_unknown() {
        let data = b"the quick brown fox jumped, jumped, jumped".to_vec();
        let pglz = pglz::compress(&data, &pglz::Strategy::ALWAYS).expect("pglz compress");
        assert_eq!(
            decompress_varlena(0, &pglz, data.len()).as_deref(),
            Some(data.as_slice())
        );
        let lz4 = lz4_flex::compress(&data);
        assert_eq!(
            decompress_varlena(1, &lz4, data.len()).as_deref(),
            Some(data.as_slice())
        );
        assert_eq!(decompress_varlena(2, &lz4, data.len()), None);
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
        assert_eq!(new.columns[0], Some(ColumnValue::Int4(100)));
        assert_eq!(new.columns[1], Some(ColumnValue::Null));
        assert_eq!(new.columns[2], Some(ColumnValue::Int4(300)));
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
        assert_eq!(old.columns[0], Some(ColumnValue::Int4(7777)));
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
        assert_eq!(new.columns[1], Some(ColumnValue::Int4(999)));
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
        assert_eq!(new.columns[0], Some(ColumnValue::Int4(55)));
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
        assert_eq!(new.columns[0], Some(ColumnValue::Null));
        assert_eq!(new.columns[1], Some(ColumnValue::Int4(8888)));
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
        assert_eq!(new.columns[0], Some(ColumnValue::Uuid(uuid_bytes)));
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
        match &new.columns[0] {
            Some(ColumnValue::ExternalToast(p)) => {
                assert_eq!(p.va_rawsize, 12345);
                assert_eq!(p.va_extinfo, 678);
                assert_eq!(p.va_valueid, 12);
                assert_eq!(p.va_toastrelid, 99);
            }
            other => panic!("expected ExternalToast, got {other:?}"),
        }
    }

    #[test]
    fn decode_unsupported_type_emits_pending() {
        // jsonb=3802 is outside the local matrix; varlena fall-through yields
        // PgPending preserving raw bytes (shadow extension resolves at emit)
        let rel = descriptor(16397, vec![rel_attr(1, "j", 3802, -1, 'i')]);
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
        match &new.columns[0] {
            Some(ColumnValue::PgPending { type_oid, raw }) => {
                assert_eq!(*type_oid, 3802);
                assert_eq!(raw.as_slice(), body);
            }
            other => panic!("expected PgPending, got {other:?}"),
        }
    }

    #[test]
    fn missing_value_substitutes_when_natts_below_catalog() {
        // Catalog natts=3, wal natts=2 (pre-ALTER row); col 3 missing_text="7"
        // must surface 7 not NULL
        let mut a3 = rel_attr(3, "c", INT4OID, 4, 'i');
        a3.missing_text = Some("7".into());
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
        assert_eq!(new.columns[0], Some(ColumnValue::Int4(100)));
        assert_eq!(new.columns[1], Some(ColumnValue::Int4(200)));
        assert_eq!(new.columns[2], Some(ColumnValue::Int4(7)));
    }

    #[test]
    fn missing_value_absent_falls_back_to_null() {
        // Catalog natts=3, wal natts=2, col 3 no missing_text => NULL
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
        assert_eq!(new.columns[2], Some(ColumnValue::Null));
    }

    #[test]
    fn missing_value_physical_null_unaffected_by_default() {
        // Bitmap-clear NULL on a missing_text col stays NULL: substitution
        // applies only for natts below catalog, never to an explicit WAL NULL
        let mut a2 = rel_attr(2, "b", INT4OID, 4, 'i');
        a2.missing_text = Some("99".into());
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
        assert_eq!(new.columns[0], Some(ColumnValue::Int4(55)));
        assert_eq!(new.columns[1], Some(ColumnValue::Null));
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
            assert_eq!(new.columns[0], Some(ColumnValue::Int4(*expected)));
        }
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
    fn missing_value_for_type_matrix() {
        let mut bool_att = rel_attr(1, "b", BOOLOID, 1, 'c');
        bool_att.missing_text = Some("t".into());
        assert_eq!(missing_value_for(&bool_att), ColumnValue::Bool(true));
        bool_att.missing_text = Some("false".into());
        assert_eq!(missing_value_for(&bool_att), ColumnValue::Bool(false));

        let mut i4 = rel_attr(1, "x", INT4OID, 4, 'i');
        i4.missing_text = Some("42".into());
        assert_eq!(missing_value_for(&i4), ColumnValue::Int4(42));

        let mut i8 = rel_attr(1, "x", INT8OID, 8, 'd');
        i8.missing_text = Some("-9223372036854775808".into());
        assert_eq!(missing_value_for(&i8), ColumnValue::Int8(i64::MIN));

        let mut txt = rel_attr(1, "n", TEXTOID, -1, 'i');
        txt.missing_text = Some("hello".into());
        assert_eq!(missing_value_for(&txt), ColumnValue::Text("hello".into()));

        let mut num = rel_attr(1, "n", NUMERICOID, -1, 'i');
        num.missing_text = Some("3.14".into());
        match missing_value_for(&num) {
            ColumnValue::PgPending { type_oid, raw } => {
                assert_eq!(type_oid, NUMERICOID);
                assert_eq!(raw, b"3.14");
            }
            other => panic!("expected PgPending for numeric, got {other:?}"),
        }

        let none = rel_attr(1, "x", INT4OID, 4, 'i');
        assert_eq!(missing_value_for(&none), ColumnValue::Null);
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
