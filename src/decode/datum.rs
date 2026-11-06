//! Borrowed tuple attributes and values, so encoders read source bytes in
//! place instead of building owned cells per row

use crate::decode::codecs::{InetValue, IntervalValue, NumericKind};
use crate::decode::heap_decoder::{ColumnValue, ToastPointer};

/// One attribute as stored in a tuple image, storage framing stripped
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum DatumRef<'a> {
    /// Not in WAL image: prefix/suffix elision or truncated tail
    Absent,
    /// Bitmap bit clear
    Null,
    /// Past tuple `natts`, resolves through attribute's fast default
    Missing,
    Dropped,
    /// `typlen > 0` bytes as stored
    Fixed(&'a [u8]),
    /// Uncompressed varlena body
    Varlena(&'a [u8]),
    /// Inline compressed varlena; `datum` spans header and body
    Compressed {
        method: u8,
        raw_len: usize,
        body: &'a [u8],
        datum: &'a [u8],
    },
    External(ToastPointer),
    /// `typlen = -2` body without trailing NUL
    Cstring(&'a [u8]),
    /// Varlena tag PostgreSQL never writes to disk; rest of buffer
    BadTag(&'a [u8]),
}

/// Borrowed counterpart of [`ColumnValue`], one variant per variant
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum ValueRef<'a> {
    Null,
    Bool(bool),
    Char(i8),
    Int2(i16),
    Int4(i32),
    Int8(i64),
    Float4(f32),
    Float8(f64),
    Oid(u32),
    Oid8(u64),
    MacAddr([u8; 6]),
    MacAddr8([u8; 8]),
    Tid { block: u32, offset: u16 },
    Date(i32),
    Time(i64),
    Timestamp(i64),
    TimestampTz(i64),
    TimeTz { micros: i64, tz_seconds: i32 },
    Uuid([u8; 16]),
    Name(&'a str),
    Bytea(&'a [u8]),
    Text(&'a str),
    Numeric(&'a NumericKind),
    Inet(InetValue),
    Interval(IntervalValue),
    Json(&'a str),
    PgPending { type_oid: u32, raw: &'a [u8] },
    PgPendingText { type_oid: u32, text: &'a str },
    ExternalToast(ToastPointer),
    Unsupported { type_oid: u32, raw: &'a [u8] },
}

impl ColumnValue {
    pub fn view(&self) -> ValueRef<'_> {
        match self {
            Self::Null => ValueRef::Null,
            Self::Bool(v) => ValueRef::Bool(*v),
            Self::Char(v) => ValueRef::Char(*v),
            Self::Int2(v) => ValueRef::Int2(*v),
            Self::Int4(v) => ValueRef::Int4(*v),
            Self::Int8(v) => ValueRef::Int8(*v),
            Self::Float4(v) => ValueRef::Float4(*v),
            Self::Float8(v) => ValueRef::Float8(*v),
            Self::Oid(v) => ValueRef::Oid(*v),
            Self::Oid8(v) => ValueRef::Oid8(*v),
            Self::MacAddr(v) => ValueRef::MacAddr(*v),
            Self::MacAddr8(v) => ValueRef::MacAddr8(*v),
            Self::Tid { block, offset } => ValueRef::Tid {
                block: *block,
                offset: *offset,
            },
            Self::Date(v) => ValueRef::Date(*v),
            Self::Time(v) => ValueRef::Time(*v),
            Self::Timestamp(v) => ValueRef::Timestamp(*v),
            Self::TimestampTz(v) => ValueRef::TimestampTz(*v),
            Self::TimeTz { micros, tz_seconds } => ValueRef::TimeTz {
                micros: *micros,
                tz_seconds: *tz_seconds,
            },
            Self::Uuid(v) => ValueRef::Uuid(*v),
            Self::Name(s) => ValueRef::Name(s),
            Self::Bytea(b) => ValueRef::Bytea(b),
            Self::Text(s) => ValueRef::Text(s),
            Self::Numeric(n) => ValueRef::Numeric(n),
            Self::Inet(v) => ValueRef::Inet(*v),
            Self::Interval(v) => ValueRef::Interval(*v),
            Self::Json(s) => ValueRef::Json(s),
            Self::PgPending { type_oid, raw } => ValueRef::PgPending {
                type_oid: *type_oid,
                raw,
            },
            Self::PgPendingText { type_oid, text } => ValueRef::PgPendingText {
                type_oid: *type_oid,
                text,
            },
            Self::ExternalToast(p) => ValueRef::ExternalToast(*p),
            Self::Unsupported { type_oid, raw } => ValueRef::Unsupported {
                type_oid: *type_oid,
                raw,
            },
        }
    }
}

impl ValueRef<'_> {
    pub fn to_owned_value(self) -> ColumnValue {
        match self {
            Self::Null => ColumnValue::Null,
            Self::Bool(v) => ColumnValue::Bool(v),
            Self::Char(v) => ColumnValue::Char(v),
            Self::Int2(v) => ColumnValue::Int2(v),
            Self::Int4(v) => ColumnValue::Int4(v),
            Self::Int8(v) => ColumnValue::Int8(v),
            Self::Float4(v) => ColumnValue::Float4(v),
            Self::Float8(v) => ColumnValue::Float8(v),
            Self::Oid(v) => ColumnValue::Oid(v),
            Self::Oid8(v) => ColumnValue::Oid8(v),
            Self::MacAddr(v) => ColumnValue::MacAddr(v),
            Self::MacAddr8(v) => ColumnValue::MacAddr8(v),
            Self::Tid { block, offset } => ColumnValue::Tid { block, offset },
            Self::Date(v) => ColumnValue::Date(v),
            Self::Time(v) => ColumnValue::Time(v),
            Self::Timestamp(v) => ColumnValue::Timestamp(v),
            Self::TimestampTz(v) => ColumnValue::TimestampTz(v),
            Self::TimeTz { micros, tz_seconds } => ColumnValue::TimeTz { micros, tz_seconds },
            Self::Uuid(v) => ColumnValue::Uuid(v),
            Self::Name(s) => ColumnValue::Name(s.to_owned()),
            Self::Bytea(b) => ColumnValue::Bytea(b.to_vec()),
            Self::Text(s) => ColumnValue::Text(s.to_owned()),
            Self::Numeric(n) => ColumnValue::Numeric(n.clone()),
            Self::Inet(v) => ColumnValue::Inet(v),
            Self::Interval(v) => ColumnValue::Interval(v),
            Self::Json(s) => ColumnValue::Json(s.to_owned()),
            Self::PgPending { type_oid, raw } => ColumnValue::PgPending {
                type_oid,
                raw: raw.to_vec(),
            },
            Self::PgPendingText { type_oid, text } => ColumnValue::PgPendingText {
                type_oid,
                text: text.to_owned(),
            },
            Self::ExternalToast(p) => ColumnValue::ExternalToast(p),
            Self::Unsupported { type_oid, raw } => ColumnValue::Unsupported {
                type_oid,
                raw: raw.to_vec(),
            },
        }
    }
}
