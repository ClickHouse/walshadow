//! Phase 5 — user-heap tuple decoder + Tier 1/2 type matrix.
//!
//! Walks `RM_HEAP_ID` / `RM_HEAP2_ID` records the filter classified as
//! `User`, projects the WAL payload through a per-relation
//! [`RelDescriptor`](crate::shadow_catalog::RelDescriptor) fetched from
//! [`ShadowCatalog`](crate::shadow_catalog::ShadowCatalog), and emits a
//! structured [`DecodedHeap`] per record. Tier 1 (fixed-width) + Tier 2
//! (length-prefixed mechanical) types are decoded inline; Tier 3
//! (`numeric`, `jsonb`, arrays, ...) is deferred to Phase 9.
//!
//! ## WAL layout (PG `src/include/access/heapam_xlog.h`,
//! `src/backend/access/heap/heapam.c::heap_xlog_*`)
//!
//! ### INSERT (`info & 0x70 == 0x00`)
//!
//! - `main_data`: `xl_heap_insert` (3 bytes — `offnum:u16`, `flags:u8`)
//! - block 0 data: `xl_heap_header (5)` + bitmap[+pad] + col data
//!
//! `t_hoff` (5th byte of `xl_heap_header`) names the offset of column
//! data **within the reconstructed `HeapTupleHeaderData`**, so the
//! column-data offset inside `block.data` is `5 + (t_hoff - 23)`. The
//! `5` is `SizeOfHeapHeader`; `23` is `SizeofHeapTupleHeader` (the
//! fixed-header bytes PG strips per `XLogRegisterBufData(0,
//! tup->t_data + SizeofHeapTupleHeader, ...)`).
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
//!   (5) + bitmap[+pad] + col data starting at reconstructed offset
//!   t_hoff + prefixlen, ending at reconstructed offset
//!   t_len - suffixlen`. PG emits prefix/suffix-compressed WAL when
//!   the new tuple shares contiguous head/tail bytes with the old
//!   tuple — see `heap_update` in heapam.c lines ≈ 8985 (prefix), 8997
//!   (suffix). The "compressed away" bytes are *not* in WAL; reconstruction
//!   needs the old tuple. Phase 5 marks those columns as
//!   [`None`] in [`DecodedTuple::columns`] and flips
//!   [`DecodedTuple::partial`] = true; Phase 6's xact buffer can fill
//!   them from the previous tuple image.
//!
//! ## Replica-identity matrix
//!
//! Per PG `ExtractReplicaIdentity` (heapam.c ≈ line 9150):
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
//! PG's `heap_insert` / `heap_update` set `bufflags |= REGBUF_KEEP_DATA`
//! when `RelationIsLogicallyLogged(rel)` (which holds under
//! `wal_level=logical`, walshadow's hard floor — see PLAN.md
//! "Pitfalls/wal_level on source"). With `KEEP_DATA`, the tuple bytes
//! are always present in `block.data`, even when an FPI replaces the
//! page at recovery (`heap_xlog_insert` line ≈ 1130:
//! `XLogReadBufferForRedoExtended` then `XLogRecGetBlockData`). So
//! Phase 5 reads tuple bytes off `block.data` exclusively; the
//! FPI-restore path lives in [`crate::fpi`] for the Phase-6 / BASEBACKUP
//! use cases.
//!
//! ## Roll-back ghost rows
//!
//! Phase 5 emits eagerly the moment the heap record arrives — no
//! per-xact buffer, no `XLOG_XACT_ABORT` retraction. Aborted xacts
//! produce ghost rows downstream; PLAN.md §Phase 5 "Rollback status,
//! explicit" documents the limitation. The decoder is unaware of
//! xact state; it stamps every output with `xid` so Phase 6's buffer
//! can key on it.

use thiserror::Error;
use wal_rs::pg::walparser::{RelFileNode, RmId, XLogRecord};

use crate::shadow_catalog::{RelAttr, RelDescriptor, ReplIdent};

/// `SizeOfHeapHeader` from PG `heapam_xlog.h`.
pub const SIZE_OF_HEAP_HEADER: usize = 5;
/// `SizeofHeapTupleHeader` — stable at 23 bytes since PG 7.x.
pub const SIZE_OF_HEAP_TUPLE_HEADER: usize = 23;
/// `SizeOfHeapInsert` from PG `heapam_xlog.h` (offnum:u16 + flags:u8).
pub const SIZE_OF_HEAP_INSERT: usize = 3;
/// `SizeOfHeapDelete` (xmax:u32 + offnum:u16 + infobits_set:u8 + flags:u8).
pub const SIZE_OF_HEAP_DELETE: usize = 8;
/// `SizeOfHeapUpdate` (old_xmax:u32 + old_offnum:u16 + old_infobits:u8 +
/// flags:u8 + new_xmax:u32 + new_offnum:u16). Note the C-struct
/// `sizeof` is 16 with trailing pad; PG's `XLogRegisterData` strips it.
pub const SIZE_OF_HEAP_UPDATE: usize = 14;

/// Mask for the op portion of `info` (strips off `XLOG_HEAP_INIT_PAGE`).
pub const XLOG_HEAP_OPMASK: u8 = 0x70;

pub const XLOG_HEAP_INSERT: u8 = 0x00;
pub const XLOG_HEAP_DELETE: u8 = 0x10;
pub const XLOG_HEAP_UPDATE: u8 = 0x20;
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

// PG built-in type OIDs (pg_type.dat).
pub const BOOLOID: u32 = 16;
pub const BYTEAOID: u32 = 17;
pub const CHAROID: u32 = 18;
pub const NAMEOID: u32 = 19;
pub const INT8OID: u32 = 20;
pub const INT2OID: u32 = 21;
pub const INT4OID: u32 = 23;
pub const TEXTOID: u32 = 25;
pub const OIDOID: u32 = 26;
pub const FLOAT4OID: u32 = 700;
pub const FLOAT8OID: u32 = 701;
pub const BPCHAROID: u32 = 1042;
pub const VARCHAROID: u32 = 1043;
pub const DATEOID: u32 = 1082;
pub const TIMEOID: u32 = 1083;
pub const TIMESTAMPOID: u32 = 1114;
pub const TIMESTAMPTZOID: u32 = 1184;
pub const TIMETZOID: u32 = 1266;
pub const INTERVALOID: u32 = 1186;
pub const UUIDOID: u32 = 2950;
// Tier 3 type OIDs — Phase 9 codecs dispatch off these.
pub const NUMERICOID: u32 = 1700;
pub const INETOID: u32 = 869;
pub const CIDROID: u32 = 650;
pub const JSONBOID: u32 = 3802;
pub const JSONOID: u32 = 114;

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

/// Tier 1/2 decoded value space. One variant per type in PHASE5.md's
/// type matrix; everything else surfaces as
/// [`ColumnValue::Unsupported`] (covers Tier 3 codecs PHASE 9 will
/// ship) or [`ColumnValue::ExternalToast`] (Phase 6 will detoast).
#[derive(Debug, Clone, PartialEq)]
pub enum ColumnValue {
    /// Explicit SQL NULL — bitmap bit clear.
    Null,
    Bool(bool),
    /// `char` (typname='char'), 1 byte. PG stores as `int8` but
    /// surface as `i8` so callers can distinguish from `int2`.
    Char(i8),
    Int2(i16),
    Int4(i32),
    Int8(i64),
    Float4(f32),
    Float8(f64),
    /// `oid`, `regproc`, etc. — 4-byte unsigned.
    Oid(u32),
    /// `date` — days since PG epoch (2000-01-01). Negative for pre-epoch.
    Date(i32),
    /// `time` — microseconds since midnight. PG built with
    /// `--disable-integer-datetimes` (legacy float8 storage) was
    /// removed in PG 10; everything we target is integer microseconds.
    Time(i64),
    /// `timestamp` — microseconds since PG epoch (2000-01-01 00:00:00 UTC).
    Timestamp(i64),
    /// `timestamptz` — same storage as `timestamp`; the suffix is a
    /// presentation-only flag in PG, the bytes are identical.
    TimestampTz(i64),
    /// `timetz` — `time` storage + 4-byte UTC offset (seconds; negative
    /// for east of UTC, per PG's sign convention).
    TimeTz {
        micros: i64,
        tz_seconds: i32,
    },
    /// `uuid` — 16 raw bytes, network byte order on disk per PG's
    /// `uuid_send` (no swap, just memcpy).
    Uuid([u8; 16]),
    /// `name` — NAMEDATALEN-bounded (64) C string, no varlena header.
    /// Stored as fixed-size 64 bytes with NUL padding; surface trimmed.
    Name(String),
    /// `bytea` — raw bytes, varlena unwrapped.
    Bytea(Vec<u8>),
    /// `text` / `varchar` / `bpchar` — varlena unwrapped + UTF-8 decode.
    /// Invalid UTF-8 surfaces as [`ColumnValue::Bytea`] with a stats counter.
    Text(String),
    /// `numeric` — Phase 9. Rendered as its PG-text form for finite
    /// values; NaN / Infinity carry their flag directly. Emitter
    /// downstream maps to CH `String` (precision is per-row in PG; no
    /// fixed CH `Decimal` type fits without operator config).
    Numeric(crate::codecs::NumericKind),
    /// `inet` / `cidr` — Phase 9. Carries family/bits/cidr-flag/addr;
    /// emit via [`crate::codecs::InetValue::to_text`].
    Inet(crate::codecs::InetValue),
    /// `interval` — Phase 9. 16-byte fixed-width tuple (months, days,
    /// micros).
    Interval(crate::codecs::IntervalValue),
    /// `json` — Phase 9. Stored as varlena text directly on disk; we
    /// pass it through unchanged.
    Json(String),
    /// Phase 9 deferred decode: the type isn't in walshadow's Tier 1/2
    /// matrix and isn't one of the locally-implemented Tier 3 hot types
    /// (numeric / inet / interval / json). Carries the raw on-disk
    /// body; resolution to text happens at emit time via a
    /// `walshadow_decode_disk(oid, bytea) -> text` SQL call against
    /// shadow PG (the `walshadow_oracle` extension). When the extension
    /// is unavailable, the emitter falls back to writing `<oid:N>` and
    /// bumping `unsupported_values`.
    PgPending {
        type_oid: u32,
        raw: Vec<u8>,
    },
    /// On-disk TOAST pointer — Phase 6's TOAST reassembly will
    /// dereference. Until then, callers see opaque metadata and can
    /// either skip or emit a placeholder downstream.
    ExternalToast(ToastPointer),
    /// Type OID outside the Phase 5 matrix. Carries the raw bytes so
    /// downstream stages can either treat as opaque or punt.
    Unsupported {
        type_oid: u32,
        raw: Vec<u8>,
    },
}

/// On-disk TOAST pointer (`struct varatt_external` in PG `varatt.h`).
/// 16 bytes total, unaligned in the source tuple.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToastPointer {
    pub va_rawsize: i32,
    pub va_extinfo: u32,
    pub va_valueid: u32,
    pub va_toastrelid: u32,
}

/// One decoded WAL heap record. Op + LSN + xid + new image + old image
/// (per `relreplident`). Column count + ordering matches
/// `RelDescriptor.attributes` — i.e. attnum-1 indexed.
#[derive(Debug, Clone, PartialEq)]
pub struct DecodedHeap {
    pub rfn: RelFileNode,
    pub xid: u32,
    pub source_lsn: u64,
    pub op: HeapOp,
    pub new: Option<DecodedTuple>,
    pub old: Option<DecodedTuple>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HeapOp {
    Insert,
    Update,
    /// `XLOG_HEAP_HOT_UPDATE` — emitted as a separate op because
    /// downstream may want to skip them entirely (HOT updates by
    /// definition touch no logged index; the visible row identity
    /// is unchanged). PG distinction matters for Phase 9's index
    /// drill but not for CH emission today.
    HotUpdate,
    Delete,
}

/// One drained tuple, fully reassembled. Phase 7's CH emitter consumes
/// this as `(rfn, xid, source_lsn, commit_ts, op, new, old)`. The
/// `commit_ts` half is the commit-record `xact_time`; Phase 5's
/// pre-buffer [`DecoderSink`](crate::decoder_sink::DecoderSink) path
/// wraps with `commit_ts = 0` since the commit record hasn't landed
/// yet at that hop.
#[derive(Debug, Clone, PartialEq)]
pub struct CommittedTuple {
    pub decoded: DecodedHeap,
    /// PG `TimestampTz` from the xact commit record (microseconds
    /// since PG epoch 2000-01-01). 0 when the upstream commit record
    /// lacked the field or hasn't arrived yet (Phase 5 unbuffered
    /// path).
    pub commit_ts: i64,
    /// Phase 11. Source LSN of the matching `XLOG_XACT_COMMIT` record.
    /// The CH emitter snapshots this onto its ack-LSN gauge once the
    /// containing xact's `send_data(None)` finishes; the daemon's
    /// status loop writes the value into the cursor file's
    /// `emitter_ack_lsn` slot. `0` for the Phase 5 unbuffered path.
    pub commit_lsn: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DecodedTuple {
    /// 0-based by attnum-1. `None` means "absent from WAL" — either
    /// PG's UPDATE prefix/suffix compression skipped it, or this
    /// `relreplident` shape never carries it. `Some(ColumnValue::Null)`
    /// is explicit NULL (bitmap bit clear).
    pub columns: Vec<Option<ColumnValue>>,
    /// True iff at least one [`Some`] in `columns` was elided due to
    /// PG's prefix/suffix compression. Downstream (Phase 6 xact
    /// buffer) can backfill from the previous tuple image.
    pub partial: bool,
}

/// Top-level decode entry point. Returns:
///
/// - `Ok(Some(DecodedHeap))` for INSERT / UPDATE / HOT_UPDATE / DELETE
///   on this `RelDescriptor`.
/// - `Ok(None)` for ops with no tuple payload to emit
///   (LOCK, INPLACE, TRUNCATE, MULTI_INSERT, every `RM_HEAP2_ID`
///   except MULTI_INSERT, every record on a non-User relation).
///   Callers count these for metrics but don't ship them.
/// - `Err` only on malformed bytes — Phase 5 contract is silent skip
///   beats noisy failure for unrecognised op codes, but malformed
///   `xl_heap_header` / block data short reads still surface.
///
/// `rel` must describe the relation that `record.blocks[0].header.location.rel`
/// names — caller's responsibility to fetch via
/// [`ShadowCatalog::relation_at`](crate::shadow_catalog::ShadowCatalog::relation_at).
pub fn decode_heap_record(
    record: &XLogRecord,
    source_lsn: u64,
    rel: &RelDescriptor,
) -> Result<Option<DecodedHeap>, DecodeError> {
    let rm = record.header.resource_manager_id;
    if rm != RmId::Heap as u8 && rm != RmId::Heap2 as u8 {
        return Ok(None);
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
            XLOG_HEAP_INSERT => decode_insert(record, source_lsn, rfn, xid, rel).map(Some),
            XLOG_HEAP_UPDATE => decode_update(record, source_lsn, rfn, xid, rel, false).map(Some),
            XLOG_HEAP_HOT_UPDATE => {
                decode_update(record, source_lsn, rfn, xid, rel, true).map(Some)
            }
            XLOG_HEAP_DELETE => decode_delete(record, source_lsn, rfn, xid, rel).map(Some),
            // LOCK / INPLACE / CONFIRM / TRUNCATE / out-of-band: skip silently.
            _ => Ok(None),
        }
    } else {
        // RmId::Heap2 — multi-insert covered by Phase 6 (xact buffer
        // needs to fan out per-tuple offsets). Phase 5 skips silently.
        Ok(None)
    }
}

fn decode_insert(
    record: &XLogRecord,
    source_lsn: u64,
    rfn: RelFileNode,
    xid: u32,
    rel: &RelDescriptor,
) -> Result<DecodedHeap, DecodeError> {
    let block = match record.blocks.first() {
        Some(b) => b,
        None => {
            // INSERT with no block ref is malformed; PG always references the
            // target page. Emit empty new tuple rather than failing the stream.
            return Ok(DecodedHeap {
                rfn,
                xid,
                source_lsn,
                op: HeapOp::Insert,
                new: None,
                old: None,
            });
        }
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
    let flags = record.main_data[7]; // xl_heap_update.flags offset, see file header
    let has_prefix = flags & XLH_UPDATE_PREFIX_FROM_OLD != 0;
    let has_suffix = flags & XLH_UPDATE_SUFFIX_FROM_OLD != 0;
    let has_old_tuple = flags & XLH_UPDATE_CONTAINS_OLD_TUPLE != 0;
    let has_old_key = flags & XLH_UPDATE_CONTAINS_OLD_KEY != 0;

    let new = match record.blocks.first() {
        Some(b) => {
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
        }
        None => None,
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
    let flags = record.main_data[7]; // xl_heap_delete.flags offset (after xmax:4 + offnum:2 + infobits_set:1)
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

/// Walk a tuple payload that starts with `xl_heap_header` at `header_off`
/// of `buf`, then bitmap, padding, and column data. `prefixlen` /
/// `suffixlen` are PG's `XLH_UPDATE_*_FROM_OLD` byte counts: bytes
/// elided from the front (prefix) and back (suffix) of the logical
/// column-data region. Columns whose byte ranges fall entirely inside
/// those regions surface as `None` in the output and the caller flips
/// `partial=true`. Bitmap + xl_heap_header are written *before* prefix
/// elision so they're always present at `header_off` regardless of
/// prefix/suffix.
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
    // Tuple header offset 23 → bitmap → MAXALIGN(8) padding → col data at
    // logical tuple offset t_hoff. The WAL block-data slice elides the
    // 23-byte fixed header. Bitmap starts at buf[header_off + 5].
    let bitmap_off = header_off + SIZE_OF_HEAP_HEADER;
    if buf.len() < bitmap_off + bitmap_bytes {
        return Err(DecodeError::Truncated {
            offset: bitmap_off,
            need: bitmap_bytes,
            have: buf.len().saturating_sub(bitmap_off),
        });
    }
    let bitmap = &buf[bitmap_off..bitmap_off + bitmap_bytes];

    // Column data in WAL block: starts after `xl_heap_header + (t_hoff - 23)`
    // bytes (the bytes between SizeofHeapTupleHeader and t_hoff are bitmap +
    // alignment pad, all carried verbatim in WAL).
    let col_data_off = header_off + SIZE_OF_HEAP_HEADER + (t_hoff - SIZE_OF_HEAP_TUPLE_HEADER);

    let mut columns = Vec::with_capacity(rel.attributes.len());
    // `cur` is the byte offset within the *logical* column-data region
    // (i.e. relative to logical-tuple offset t_hoff). Alignment matches
    // PG's `att_align_nominal` against this logical offset. t_hoff is
    // MAXALIGN'd (8) so column 1 starts cleanly. WAL block-data offset
    // for the bytes of column at logical offset `cur` is
    // `col_data_off + (cur - prefixlen)` when `cur >= prefixlen`.
    let mut cur: usize = 0;
    let attrs = effective_attrs(rel, natts);
    let wal_col_data_avail = buf.len().saturating_sub(col_data_off);
    // Logical end-of-column-data: bytes available in WAL plus prefix
    // (suffix bytes are past the WAL slice). suffixlen is accepted
    // explicitly so callers can independently bound the walk; for now
    // we just consult buffer length, which already accounts for it.
    let _ = suffixlen;
    let logical_end = wal_col_data_avail + prefixlen;
    for (idx, att) in attrs.iter().enumerate() {
        if idx >= natts {
            // Writer's natts is smaller than catalog natts: trailing
            // columns added via `ALTER TABLE ADD COLUMN` since the WAL
            // record was written. Per PG "missing column" rules they
            // surface as default/NULL on read.
            columns.push(Some(ColumnValue::Null));
            continue;
        }
        if has_null && !bitmap_is_set(bitmap, idx) {
            columns.push(Some(ColumnValue::Null));
            continue;
        }
        // Compute the WAL byte position this column would land at if
        // present in WAL. We need it before we can decide "in prefix"
        // for varlena (whose peek-byte depends on the WAL bytes).
        let col_size_hint = att.type_len;
        // Apply nominal alignment. For varlena (typlen=-1), if the
        // column falls inside the prefix we don't have its byte to
        // peek at, so we fall back to nominal alignment unconditionally
        // — matches PG's worst-case alignment when computing the
        // prefix-byte budget.
        if col_size_hint == -1 && cur >= prefixlen && col_data_off + cur - prefixlen < buf.len() {
            cur = att_align_nominal(
                cur,
                att.type_align,
                col_size_hint,
                buf,
                col_data_off + cur - prefixlen,
            );
        } else {
            // Force nominal alignment when we can't peek (in prefix /
            // past EOF). PG's behaviour matches because prefix bytes
            // are themselves aligned in the source tuple.
            cur = align_for(cur, att.type_align);
        }

        // Column is in prefix?
        if cur < prefixlen {
            let len = decoded_size_or_skip(att);
            match len {
                Some(n) => {
                    columns.push(None);
                    cur += n;
                }
                None => {
                    // Varlena in prefix: no way to advance cur without the
                    // bytes. Mark this and every subsequent column as
                    // absent and bail out; Phase 6's buffer reconstructs.
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

        // Column is in WAL slice?
        if cur >= logical_end {
            // Entirely in suffix / past EOF.
            columns.push(None);
            // Advance cur by an attlen guess so subsequent fixed-width
            // columns in suffix still get accounted for (purely
            // accounting; we emit None for all).
            if let Some(n) = decoded_size_or_skip(att) {
                cur += n;
            } else {
                // Varlena tail; remaining columns stay absent.
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

/// Byte width to attribute to a column for cursor accounting when we
/// can't read its bytes (in prefix or past EOF). `None` for varlena —
/// caller must stop walking, since varlena width depends on content.
fn decoded_size_or_skip(att: &RelAttr) -> Option<usize> {
    if att.type_len > 0 {
        Some(att.type_len as usize)
    } else {
        None
    }
}

/// Plain MAXALIGN dispatch over `pg_type.typalign`. Used in the
/// in-prefix / past-EOF branches where we can't peek at bytes to
/// detect a short varlena header (`att_align_pointer`'s optimisation).
fn align_for(cur: usize, attalign: char) -> usize {
    match attalign {
        'c' => cur,
        's' => align_up(cur, 2),
        'i' => align_up(cur, 4),
        'd' => align_up(cur, 8),
        _ => cur,
    }
}

/// PG's natts (`t_infomask2 & HEAP_NATTS_MASK`) reflects the writer's
/// view. For replica-identity-only writes (UsingIndex / Default), the
/// writer constructs a `heap_form_tuple` with NULL placeholders in
/// non-indexed columns — those still occupy bitmap slots, so natts on
/// the wire equals the relation's natts. Returned attribute list is
/// just `rel.attributes` (every attnum). If the relation has been
/// extended with `ALTER TABLE ADD COLUMN ... DEFAULT NULL` since the
/// WAL was written (`natts_in_wal < rel.attributes.len()`), the extra
/// trailing attrs come through as `Some(Null)` per PG's
/// "missing-column" handling.
fn effective_attrs(rel: &RelDescriptor, _natts: usize) -> &[RelAttr] {
    &rel.attributes
}

/// PG `att_align_nominal` (tupmacs.h ≈ line 150). For varlena (typlen=-1),
/// PG's `att_align_pointer` skips alignment when the next byte is a
/// 1-byte short-header datum (`!VARATT_NOT_PAD_BYTE`); we mirror that
/// by peeking at `buf[abs]` before aligning.
fn att_align_nominal(
    cur_offset: usize,
    attalign: char,
    attlen: i16,
    buf: &[u8],
    abs_offset: usize,
) -> usize {
    if attlen == -1 && abs_offset < buf.len() && buf[abs_offset] != 0 {
        // varlena, peek: non-zero first byte = short-header or aligned 4B
        // header, both safe to read unaligned. Zero byte must be padding.
        return cur_offset;
    }
    match attalign {
        'c' => cur_offset,              // TYPALIGN_CHAR (1)
        's' => align_up(cur_offset, 2), // TYPALIGN_SHORT
        'i' => align_up(cur_offset, 4), // TYPALIGN_INT
        'd' => align_up(cur_offset, 8), // TYPALIGN_DOUBLE
        _ => cur_offset,                // unknown: fall back to no-align rather than panic
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

/// Decode one column. Returns `(value, bytes_consumed)` so the caller
/// can advance its cursor. Varlena types report total on-disk length
/// (varlena header + body, including TOAST pointer's full 18 bytes).
fn decode_one_value(
    att: &RelAttr,
    buf: &[u8],
    abs: usize,
) -> Result<(ColumnValue, usize), DecodeError> {
    if att.dropped {
        // Dropped columns retain attlen/attalign/typbyval per
        // catalog convention (so heap scans still walk correctly).
        // For Tier 1/2 fixed types this is well-defined; for varlena
        // we still need to read the header to advance the cursor.
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
            // `timetz` is 12 bytes: 8-byte time + 4-byte tz offset.
            let micros = i64::from_le_bytes(body[..8].try_into().unwrap());
            let tz_seconds = i32::from_le_bytes(body[8..12].try_into().unwrap());
            ColumnValue::TimeTz { micros, tz_seconds }
        }
        // PG's `uuid_send` emits raw 16 bytes, network byte order (no
        // swap). On-disk is the same.
        UUIDOID => ColumnValue::Uuid(body.try_into().unwrap()),
        INTERVALOID => match crate::codecs::decode_interval(body) {
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

/// Skip a dropped column. Same shape as a live column but no value
/// emitted (caller maps to ColumnValue::Null). For varlena we still
/// must read the header to advance correctly.
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

/// Decode a varlena attribute (`typlen == -1`). PG varlena formats
/// (`varatt.h` ≈ line 142):
///
/// - `0bxxxxxx00` 4-byte length, uncompressed.
/// - `0bxxxxxx10` 4-byte length, compressed in-line (we hand back as
///   `Unsupported` for now — Phase 9 will detoast the in-line
///   compression with `pglz`/`lz4` once Tier-3 arrays need it; PG
///   `wal_compression` already covers FPI compression).
/// - `0b00000001` TOAST pointer (`varattrib_1b_e`, 18 bytes on disk).
/// - `0bxxxxxxx1` 1-byte length, uncompressed short header.
///
/// We're little-endian only (every supported arch + PG's
/// `WORDS_BIGENDIAN` is dead on x86/aarch64 — see PG's `pg_config.h`).
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
        // TOAST pointer: varattrib_1b_e with va_tag = 18 (VARTAG_ONDISK).
        if buf.len() < abs + 2 {
            return Err(DecodeError::Truncated {
                offset: abs,
                need: 2,
                have: buf.len() - abs,
            });
        }
        let tag = buf[abs + 1];
        // On-disk TOAST pointer is tag 18 + 16 bytes of struct varatt_external.
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
        // Other tags (INDIRECT/EXPANDED) are in-memory only — they
        // never appear on disk per varatt.h docs. Forward as
        // unsupported so a corrupt stream surfaces visibly.
        return Ok((
            ColumnValue::Unsupported {
                type_oid: att.type_oid,
                raw: buf[abs..].to_vec(),
            },
            buf.len() - abs,
        ));
    }
    if first & 0x01 != 0 {
        // 1-byte length short header: low bit set, length in upper 7
        // bits including the header byte. Body starts at abs+1.
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
        let v = varlena_to_value(att, body, false);
        return Ok((v, total));
    }
    // 4-byte length: bits [1:0] = 00 (uncompressed) or 10 (compressed).
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
        // In-line compressed: 4-byte header + 4-byte va_tcinfo (extsize +
        // method) + compressed body. Phase 5 surfaces opaque; Phase 9
        // ships the pglz / lz4 unwrap.
        return Ok((
            ColumnValue::Unsupported {
                type_oid: att.type_oid,
                raw: buf[abs..abs + total].to_vec(),
            },
            total,
        ));
    }
    let body = &buf[abs + 4..abs + total];
    Ok((varlena_to_value(att, body, false), total))
}

fn varlena_to_value(att: &RelAttr, body: &[u8], _short: bool) -> ColumnValue {
    match att.type_oid {
        BYTEAOID => ColumnValue::Bytea(body.to_vec()),
        TEXTOID | VARCHAROID | BPCHAROID => match std::str::from_utf8(body) {
            Ok(s) => ColumnValue::Text(s.to_owned()),
            // Invalid UTF-8 — PG validates on input so this should never
            // happen on disk, but surface as bytea rather than crashing.
            Err(_) => ColumnValue::Bytea(body.to_vec()),
        },
        // Tier 3 hot types — local decoders (Phase 9).
        NUMERICOID => match crate::codecs::decode_numeric(body) {
            Ok(v) => ColumnValue::Numeric(v),
            Err(_) => ColumnValue::Unsupported {
                type_oid: att.type_oid,
                raw: body.to_vec(),
            },
        },
        INETOID | CIDROID => match crate::codecs::decode_inet(body, att.type_oid == CIDROID) {
            Ok(v) => ColumnValue::Inet(v),
            Err(_) => ColumnValue::Unsupported {
                type_oid: att.type_oid,
                raw: body.to_vec(),
            },
        },
        JSONOID => match std::str::from_utf8(body) {
            Ok(s) => ColumnValue::Json(s.to_owned()),
            Err(_) => ColumnValue::Bytea(body.to_vec()),
        },
        // Tier 3 deferred — resolved by the walshadow_oracle extension
        // on shadow PG at emit time. JSONBOID, range types, arrays
        // (typcategory='A'), tsvector etc. all route here. Carries the
        // full on-disk body so the SQL bridge can reconstruct the
        // varlena Datum.
        _ => ColumnValue::PgPending {
            type_oid: att.type_oid,
            raw: body.to_vec(),
        },
    }
}

/// `name` columns store as NUL-padded `NAMEDATALEN`-bound C strings
/// (typlen=NAMEDATALEN=64, typbyval=false, typalign='c'). Decoded by
/// the fixed-width path above; this helper handles the `typlen=-2`
/// cstring variant (regtype, regclass typoutput intermediates) which
/// is `\0`-terminated. None of the Phase 5 type matrix uses typlen=-2,
/// but tracking it lets dropped cstring columns walk correctly.
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
    let value = match std::str::from_utf8(body) {
        Ok(s) => ColumnValue::Text(s.to_owned()),
        Err(_) => ColumnValue::Bytea(body.to_vec()),
    };
    Ok((value, (end - abs) + 1)) // include the trailing NUL
}

/// True iff this column is part of the relation's replica identity
/// (used by Phase 7 emitter to gate `_op=delete/update` propagation).
/// Phase 5 ships the predicate here so downstream stages stay agnostic
/// to the [`ReplIdent`] variants.
pub fn is_replica_identity_attr(replident: &ReplIdent, attnum: i16) -> bool {
    match replident {
        ReplIdent::Full => true,
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
    use wal_rs::pg::walparser::{
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
            namespace_name: "public".into(),
            name: "t".into(),
            kind: 'r',
            persistence: 'p',
            replident: ReplIdent::Default { pk_attnums: None },
            attributes: attrs,
        }
    }

    /// Build a heap-tuple-shaped byte buffer: `xl_heap_header (5) +
    /// bitmap[+pad] + column data`. Caller supplies the column-data
    /// payload; helper computes natts, bitmap, and t_hoff to fit.
    fn build_tuple_payload(natts: u16, has_null: bool, t_hoff: u8, col_data: &[u8]) -> Vec<u8> {
        let mut v = Vec::new();
        v.extend_from_slice(&natts.to_le_bytes()); // t_infomask2
        let infomask = if has_null { HEAP_HASNULL } else { 0 };
        v.extend_from_slice(&infomask.to_le_bytes()); // t_infomask
        v.push(t_hoff);
        let bitmap_pad = (t_hoff as usize) - SIZE_OF_HEAP_TUPLE_HEADER;
        v.extend_from_slice(&vec![0xFFu8; bitmap_pad]); // bitmap_bits + padding (all-set bitmap for no-null)
        v.extend_from_slice(col_data);
        v
    }

    fn record_with(rm: RmId, info: u8, main_data: Vec<u8>, block_data: Vec<u8>) -> XLogRecord {
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
                data: block_data,
                ..Default::default()
            }],
            main_data,
            ..Default::default()
        }
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
        // col_data: int4 at logical offset 0..4, then 4 bytes pad to 8-align
        // for int8 at logical offset 8..16. Matches `att_align_nominal`.
        let mut col_data = Vec::new();
        col_data.extend_from_slice(&12345i32.to_le_bytes());
        col_data.extend_from_slice(&[0u8; 4]); // 8-align pad before int8
        col_data.extend_from_slice(&999_999i64.to_le_bytes());
        let payload = build_tuple_payload(2, false, 24, &col_data);
        let main_data = vec![0u8; SIZE_OF_HEAP_INSERT];
        let rec = record_with(RmId::Heap, XLOG_HEAP_INSERT, main_data, payload);
        let out = decode_heap_record(&rec, 0x1000, &rel).unwrap().unwrap();
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
        // Align varlena to 'i' (4). offset = 1, need next multiple of 4 = 4
        col_data.extend_from_slice(&[0u8; 3]); // pad
        // 4-byte uncompressed varlena: header = len << 2, body = "hi"
        let body = b"hi";
        let total = 4 + body.len();
        let header_u32 = (total as u32) << 2;
        col_data.extend_from_slice(&header_u32.to_le_bytes());
        col_data.extend_from_slice(body);
        let payload = build_tuple_payload(2, false, 24, &col_data);
        let main_data = vec![0u8; SIZE_OF_HEAP_INSERT];
        let rec = record_with(RmId::Heap, XLOG_HEAP_INSERT, main_data, payload);
        let out = decode_heap_record(&rec, 0, &rel).unwrap().unwrap();
        let new = out.new.unwrap();
        assert_eq!(new.columns[0], Some(ColumnValue::Bool(true)));
        assert_eq!(new.columns[1], Some(ColumnValue::Text("hi".into())));
    }

    #[test]
    fn decode_insert_short_varlena() {
        let rel = descriptor(16387, vec![rel_attr(1, "msg", TEXTOID, -1, 'i')]);
        // Short header: bit 0 set, len in upper 7 bits (total bytes incl. header).
        // Body = "hi" (2 bytes), total = 3.
        let header = (3u8 << 1) | 0x01;
        let mut col_data = Vec::new();
        col_data.push(header);
        col_data.extend_from_slice(b"hi");
        // For varlena typlen=-1 alignment, PG's att_align_pointer SKIPS
        // alignment when the next byte is a non-zero short-header byte.
        // So no padding before col_data; t_hoff = 24 (just bitmap_pad=1).
        let payload = build_tuple_payload(1, false, 24, &col_data);
        let main_data = vec![0u8; SIZE_OF_HEAP_INSERT];
        let rec = record_with(RmId::Heap, XLOG_HEAP_INSERT, main_data, payload);
        let out = decode_heap_record(&rec, 0, &rel).unwrap().unwrap();
        let new = out.new.unwrap();
        assert_eq!(new.columns[0], Some(ColumnValue::Text("hi".into())));
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
        // Bitmap: 3 bits, 1 byte. Set bits 0 and 2; clear bit 1 (NULL).
        let bitmap = 0b00000101u8;
        // t_hoff = MAXALIGN(23 + 1 byte bitmap) = 24
        let mut payload = Vec::new();
        payload.extend_from_slice(&3u16.to_le_bytes());
        payload.extend_from_slice(&HEAP_HASNULL.to_le_bytes());
        payload.push(24);
        payload.push(bitmap);
        // Column data: int4 then int4 (col 2 skipped because NULL).
        payload.extend_from_slice(&100i32.to_le_bytes());
        payload.extend_from_slice(&300i32.to_le_bytes());
        let main_data = vec![0u8; SIZE_OF_HEAP_INSERT];
        let rec = record_with(RmId::Heap, XLOG_HEAP_INSERT, main_data, payload);
        let out = decode_heap_record(&rec, 0, &rel).unwrap().unwrap();
        let new = out.new.unwrap();
        assert_eq!(new.columns.len(), 3);
        assert_eq!(new.columns[0], Some(ColumnValue::Int4(100)));
        assert_eq!(new.columns[1], Some(ColumnValue::Null));
        assert_eq!(new.columns[2], Some(ColumnValue::Int4(300)));
    }

    #[test]
    fn decode_delete_with_old_key_emits_old() {
        let rel = descriptor(16389, vec![rel_attr(1, "id", INT4OID, 4, 'i')]);
        // main_data: xl_heap_delete (8) + xl_heap_header (5) + bitmap_pad (1) + col data.
        let mut main_data = vec![0u8; SIZE_OF_HEAP_DELETE];
        main_data[7] = XLH_DELETE_CONTAINS_OLD_KEY;
        main_data.extend_from_slice(&1u16.to_le_bytes()); // natts
        main_data.extend_from_slice(&0u16.to_le_bytes()); // infomask
        main_data.push(24); // t_hoff
        main_data.push(0); // bitmap pad
        main_data.extend_from_slice(&7777i32.to_le_bytes());
        let rec = record_with(RmId::Heap, XLOG_HEAP_DELETE, main_data, Vec::new());
        let out = decode_heap_record(&rec, 0, &rel).unwrap().unwrap();
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
        // UPDATE with PREFIX_FROM_OLD, prefixlen=4 (col 1 elided from WAL).
        // Block 0 data: [prefixlen:u16=4][xl_heap_header(5)][bitmap_pad(1)][col 2 = int4]
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
        let out = decode_heap_record(&rec, 0, &rel).unwrap().unwrap();
        assert_eq!(out.op, HeapOp::Update);
        let new = out.new.unwrap();
        assert!(new.partial, "prefix-compressed UPDATE flagged partial");
        // col 1 elided into the un-logged prefix → None
        assert_eq!(new.columns[0], None);
        // col 2 present at offset 0 of the WAL col-data
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
        let out = decode_heap_record(&rec, 0, &rel).unwrap().unwrap();
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
        col_data.extend_from_slice(&0i32.to_le_bytes()); // dropped col still in bytes
        col_data.extend_from_slice(&8888i32.to_le_bytes());
        let payload = build_tuple_payload(2, false, 24, &col_data);
        let main_data = vec![0u8; SIZE_OF_HEAP_INSERT];
        let rec = record_with(RmId::Heap, XLOG_HEAP_INSERT, main_data, payload);
        let out = decode_heap_record(&rec, 0, &rel).unwrap().unwrap();
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
            assert!(out.is_none(), "op {op:#x} should skip");
        }
    }

    #[test]
    fn decode_skips_heap2_silently() {
        let rel = descriptor(16394, vec![rel_attr(1, "id", INT4OID, 4, 'i')]);
        let rec = record_with(RmId::Heap2, XLOG_HEAP2_MULTI_INSERT, Vec::new(), Vec::new());
        let out = decode_heap_record(&rec, 0, &rel).unwrap();
        assert!(out.is_none(), "multi-insert is Phase 6 territory");
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
        let out = decode_heap_record(&rec, 0, &rel).unwrap().unwrap();
        let new = out.new.unwrap();
        assert_eq!(new.columns[0], Some(ColumnValue::Uuid(uuid_bytes)));
    }

    #[test]
    fn decode_external_toast_pointer() {
        let rel = descriptor(16396, vec![rel_attr(1, "blob", BYTEAOID, -1, 'i')]);
        // varattrib_1b_e: 0x01, va_tag=18, va_rawsize=12345, va_extinfo=678,
        // va_valueid=12, va_toastrelid=99.
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
        let out = decode_heap_record(&rec, 0, &rel).unwrap().unwrap();
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
        // Pick a varlena type OID that's outside walshadow's local
        // matrix (jsonb = 3802). Post-Phase-9 the varlena fall-through
        // produces a `PgPending` with raw bytes preserved — the
        // walshadow_oracle shadow extension resolves the text form at
        // emit time, falling back to `unsupported_values` when the
        // extension is absent.
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
        let out = decode_heap_record(&rec, 0, &rel).unwrap().unwrap();
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
    fn is_replica_identity_attr_matrix() {
        let full = ReplIdent::Full;
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
