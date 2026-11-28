//! ClickHouse Native column buffers and the codecs that fill them from PG
//! datums

use clickhouse_c::{Allocator, ColumnBuilder, Kind, TypeAst};

use crate::ch::{EmitterError, types};
use crate::column_rules::{ColumnRule, Substitute};
use crate::decode::codecs::{Numeric, parse_numeric};
use crate::decode::datum::DatumRef;
use crate::decode::heap_decoder::{TextDatum, inflate, missing_datum, text_datum};
use crate::mimalloc;
use crate::ops::oracle::{OracleCell, OracleColumnBuf};
use crate::schema::RelAttr;
use smallvec::SmallVec;

/// Microseconds between PG `TimestampTz` epoch (2000-01-01 UTC) and Unix
/// epoch. CH `DateTime64(6)` is Unix microseconds; PG commit-record
/// `xact_time` and tuple `TimestampTz` are PG-epoch microseconds.
pub(crate) const DATETIME64_PG_EPOCH_US: i64 = walrus::pg::replication::PG_EPOCH_USEC;

/// Days between PG `date` epoch (2000-01-01) and the Unix epoch.
pub(crate) const DATE32_PG_EPOCH_DAYS: i32 = (DATETIME64_PG_EPOCH_US / 1_000_000 / 86_400) as i32;

pub(crate) struct ColumnPlan {
    pub(crate) name: String,
    /// Canonical CH type shared by oracle request and response
    pub(crate) type_repr: String,
    pub(crate) ast: TypeAst,
    pub(crate) decimal: Option<DecimalWire>,
    pub(crate) datetime64_scale: u32,
    pub(crate) non_finite: NonFinite,
    pub(crate) encoding: ColumnEncoding,
    /// Source attribute feeding this column, `None` when descriptor holds
    /// it dropped
    pub(crate) source: Option<SourceColumn>,
}

/// Per-row handling of a source attribute, compiled with its target
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct SourceColumn {
    /// attnum - 1, indexes tuple images
    pub(crate) index: usize,
    pub(crate) codec: Codec,
    /// Zero for values patched past the descriptor's attributes
    pub(crate) type_oid: u32,
}

/// Conversion of one source type's stored bytes into its column
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Codec {
    /// Little-endian storage copied as the column holds it
    Le,
    Date,
    Time,
    /// `timestamp` and `timestamptz`
    Timestamp,
    TimeTz,
    Uuid,
    Name,
    /// Body bytes as String: text, bytea, json, cstring
    Bytes,
    Numeric,
    Jsonb,
    Inet,
    Cidr,
    MacAddr,
    MacAddr8,
    Tid,
    Interval,
    /// PostGIS point rendered as WKT, typoutput yields HEXEWKB
    Wkt,
    /// No local decode: typoutput text lands as String, stored bytes fail
    Pending,
}

impl Codec {
    pub(crate) fn choose(att: &RelAttr) -> Self {
        use crate::schema::{
            BOOLOID, BPCHAROID, BYTEAOID, CHAROID, CIDOID, CIDROID, DATEOID, FLOAT4OID, FLOAT8OID,
            INETOID, INT2OID, INT4OID, INT8OID, INTERVALOID, JSONBOID, JSONOID, MACADDR8OID,
            MACADDROID, MONEYOID, NAMEOID, NUMERICOID, OID8OID, OIDOID, PG_LSNOID, TEXTOID, TIDOID,
            TIMEOID, TIMESTAMPOID, TIMESTAMPTZOID, TIMETZOID, UUIDOID, VARCHAROID, XID8OID, XIDOID,
        };
        if matches!(att.type_name.as_str(), "geography" | "geometry") {
            return Self::Wkt;
        }
        if att.type_len == -2 {
            return Self::Bytes;
        }
        match att.type_oid {
            BOOLOID | CHAROID | INT2OID | INT4OID | INT8OID | MONEYOID | FLOAT4OID | FLOAT8OID
            | OIDOID | XIDOID | CIDOID | OID8OID | XID8OID | PG_LSNOID => Self::Le,
            DATEOID => Self::Date,
            TIMEOID => Self::Time,
            TIMESTAMPOID | TIMESTAMPTZOID => Self::Timestamp,
            TIMETZOID => Self::TimeTz,
            UUIDOID => Self::Uuid,
            NAMEOID => Self::Name,
            BYTEAOID | TEXTOID | VARCHAROID | BPCHAROID | JSONOID => Self::Bytes,
            NUMERICOID => Self::Numeric,
            JSONBOID => Self::Jsonb,
            INETOID => Self::Inet,
            CIDROID => Self::Cidr,
            MACADDROID => Self::MacAddr,
            MACADDR8OID => Self::MacAddr8,
            TIDOID => Self::Tid,
            INTERVALOID => Self::Interval,
            _ => Self::Pending,
        }
    }
}

/// Values replacing non-finite numerics or temporal infinities, `None` rejects
#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct NonFinite {
    nan: Option<Fill>,
    pos_inf: Option<Fill>,
    neg_inf: Option<Fill>,
}

/// Substitute cell, encoded for its column
#[derive(Clone, Debug, PartialEq)]
enum Fill {
    Null,
    Le(SmallVec<[u8; 32]>),
}

impl NonFinite {
    /// Resolve settings against Decimal or temporal destination once per plan
    pub(crate) fn compile(
        rule: &ColumnRule,
        ast: &TypeAst,
        zero_default: bool,
    ) -> Result<Self, String> {
        let nullable = ast.view().kind() == Some(Kind::Nullable);
        let inner = types::strip_nullable(ast.view());
        let decimal = decimal_wire_of(ast);
        if decimal.is_none() && !matches!(inner.kind(), Some(Kind::Date32 | Kind::DateTime64)) {
            return Ok(Self::default());
        }
        let precision = inner.decimal_precision().unwrap_or(0) as usize;
        let scale = usize::from(decimal.map_or(0, |w| w.scale));
        let bound = format!("{}.{}", "9".repeat(precision - scale), "9".repeat(scale));
        let cell = |key: &str, sub: &Option<Substitute>| {
            let text = match sub {
                None if zero_default && decimal.is_some() => "0".to_owned(),
                None | Some(Substitute::Reject) => return Ok(None),
                Some(Substitute::Null) if nullable => return Ok(Some(Fill::Null)),
                Some(Substitute::Null) => {
                    return Err(format!("{key} = \"null\" needs a Nullable destination"));
                }
                Some(Substitute::Min) => format!("-{bound}"),
                Some(Substitute::Max) => bound.clone(),
                Some(Substitute::Literal(lit)) => lit.clone(),
            };
            let Some(decimal) = decimal else {
                return Err(format!(
                    "{key}: temporal infinity supports only reject or null"
                ));
            };
            let (int, frac) = crate::column_rules::decimal_parts(&text).unwrap_or_default();
            if frac.len() > scale && frac[scale..].bytes().any(|b| b != b'0') {
                return Err(format!(
                    "{key} = {text:?}: more than {scale} fractional digits"
                ));
            }
            if int.trim_start_matches('0').len() + scale > precision {
                return Err(format!(
                    "{key} = {text:?}: exceeds Decimal({precision}, {scale})"
                ));
            }
            let scaled = decimal_text_to_scaled_le(&text, decimal.scale, decimal.width)
                .map_err(|e| format!("{key} = {text:?}: {e}"))?;
            Ok(Some(Fill::Le(SmallVec::from_slice(
                &scaled[..decimal.width.bytes()],
            ))))
        };
        Ok(Self {
            // Temporal values have no NaN
            nan: if decimal.is_some() {
                cell("nan", &rule.nan)?
            } else {
                None
            },
            pos_inf: cell("pos_inf", &rule.pos_inf)?,
            neg_inf: cell("neg_inf", &rule.neg_inf)?,
        })
    }

    /// Substitute for a non-finite `body`, `None` to encode it as is
    fn fill(&self, codec: Codec, body: &[u8]) -> Option<&Fill> {
        use crate::decode::codecs::Special;
        let sub = match codec {
            Codec::Numeric => match parse_numeric(body) {
                Ok(Numeric::Special(Special::NaN)) => &self.nan,
                Ok(Numeric::Special(Special::PInf)) => &self.pos_inf,
                Ok(Numeric::Special(Special::NInf)) => &self.neg_inf,
                _ => return None,
            },
            Codec::Date => match i32::from_le_bytes(body.try_into().ok()?) {
                i32::MAX => &self.pos_inf,
                i32::MIN => &self.neg_inf,
                _ => return None,
            },
            Codec::Timestamp => match i64::from_le_bytes(body.try_into().ok()?) {
                i64::MAX => &self.pos_inf,
                i64::MIN => &self.neg_inf,
                _ => return None,
            },
            _ => return None,
        };
        sub.as_ref()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ColumnEncoding {
    Local,
    Oracle {
        /// Zero when every cell defaults
        source_type_oid: u32,
        source_typmod: i32,
    },
}

impl ColumnEncoding {
    pub(crate) fn label(&self) -> &'static str {
        match self {
            Self::Local => "local",
            Self::Oracle { .. } => "oracle",
        }
    }

    pub(crate) fn choose(att: Option<&RelAttr>, ast: &TypeAst) -> Self {
        let Some(att) = att else {
            // Predeclared post-ALTER column has only default cells
            return if composite_target(ast) {
                Self::Oracle {
                    source_type_oid: 0,
                    source_typmod: -1,
                }
            } else {
                Self::Local
            };
        };
        if composite_target(ast)
            || !crate::decode::heap_decoder::local_matrix_covers(att.type_oid, att.type_len)
        {
            Self::Oracle {
                source_type_oid: att.type_oid,
                source_typmod: att.typmod,
            }
        } else {
            Self::Local
        }
    }
}

/// Render type identically on both protocol sides
pub(crate) fn canonical_type(ast: &TypeAst, configured: &str) -> String {
    ast.view()
        .name()
        .and_then(|b| std::str::from_utf8(b).ok())
        .unwrap_or(configured)
        .to_owned()
}

/// CH JSON serialization v1 accepts strings, legacy Object needs a kind prefix
fn composite_target(ast: &TypeAst) -> bool {
    matches!(
        types::strip_nullable(ast.view()).kind(),
        Some(Kind::Array | Kind::Map | Kind::Object | Kind::QBit)
    )
}

/// Physical wire width of a CH `Decimal`: one of four signed-integer
/// backings. Discriminants are the byte widths, so `as usize` recovers
/// the size.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum DecimalWidth {
    D32 = 4,
    D64 = 8,
    D128 = 16,
    D256 = 32,
}

impl DecimalWidth {
    fn bytes(self) -> usize {
        self as usize
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct DecimalWire {
    pub(crate) scale: u8,
    pub(crate) width: DecimalWidth,
}

/// On-the-wire-shape column buffer. [`BlockBuilder`] borrows these
/// slices at flush time; cleared after `send_data`.
pub(crate) enum ColumnBuf {
    /// `width` bytes per row, packed little-endian
    Fixed {
        width: usize,
        bytes: Vec<u8>,
    },
    /// `offsets[i]` is the cumulative exclusive end of row `i` in `data`
    String {
        offsets: Vec<u64>,
        data: Vec<u8>,
        absent: &'static [u8],
    },
    /// `null_map[i] = 1` means NULL; zero bytes go into `inner` for null
    /// rows so the slab stays dense
    NullableFixed {
        width: usize,
        null_map: Vec<u8>,
        inner: Vec<u8>,
    },
    NullableString {
        offsets: Vec<u64>,
        data: Vec<u8>,
        null_map: Vec<u8>,
        absent: &'static [u8],
    },
    Oracle(OracleColumnBuf),
}

impl ColumnBuf {
    pub(crate) fn new_for_ast(ast: &TypeAst) -> Result<Self, EmitterError> {
        let view = ast.view();
        let nullable = view.kind() == Some(Kind::Nullable);
        let inner = types::strip_nullable(view);
        // CH parses JSON cells even when null_map marks them NULL
        let absent: &[u8] = if inner.kind() == Some(Kind::Json) {
            b"{}"
        } else {
            b""
        };
        Ok(match (nullable, inner.elem_size()) {
            (false, 0) => Self::String {
                offsets: Vec::new(),
                data: Vec::new(),
                absent,
            },
            (true, 0) => Self::NullableString {
                offsets: Vec::new(),
                data: Vec::new(),
                null_map: Vec::new(),
                absent,
            },
            (false, w) => Self::Fixed {
                width: w,
                bytes: Vec::new(),
            },
            (true, w) => Self::NullableFixed {
                width: w,
                null_map: Vec::new(),
                inner: Vec::new(),
            },
        })
    }

    /// Empty buffer of this shape
    pub(crate) fn empty_like(&self) -> Self {
        match self {
            Self::Fixed { width, .. } => Self::Fixed {
                width: *width,
                bytes: Vec::new(),
            },
            Self::String { absent, .. } => Self::String {
                offsets: Vec::new(),
                data: Vec::new(),
                absent,
            },
            Self::NullableFixed { width, .. } => Self::NullableFixed {
                width: *width,
                null_map: Vec::new(),
                inner: Vec::new(),
            },
            Self::NullableString { absent, .. } => Self::NullableString {
                offsets: Vec::new(),
                data: Vec::new(),
                null_map: Vec::new(),
                absent,
            },
            Self::Oracle(o) => Self::Oracle(o.empty_like()),
        }
    }

    pub(crate) fn allocated_bytes(&self) -> usize {
        match self {
            Self::Fixed { bytes, .. } => bytes.capacity(),
            Self::String { offsets, data, .. } => offsets.capacity() * 8 + data.capacity(),
            Self::NullableFixed {
                null_map, inner, ..
            } => null_map.capacity() + inner.capacity(),
            Self::NullableString {
                offsets,
                data,
                null_map,
                ..
            } => offsets.capacity() * 8 + data.capacity() + null_map.capacity(),
            Self::Oracle(o) => o.allocated_bytes(),
        }
    }

    pub(crate) fn approx_size(&self) -> usize {
        match self {
            Self::Fixed { bytes, .. } => bytes.len(),
            Self::String { offsets, data, .. } => offsets.len() * 8 + data.len(),
            Self::NullableFixed {
                null_map, inner, ..
            } => null_map.len() + inner.len(),
            Self::NullableString {
                offsets,
                data,
                null_map,
                ..
            } => offsets.len() * 8 + data.len() + null_map.len(),
            Self::Oracle(o) => o.approx_size(),
        }
    }

    /// Use CH type defaults for non-nullable columns, NULL otherwise
    fn append_default(&mut self) {
        match self {
            Self::Fixed { width, bytes } => bytes.extend(std::iter::repeat_n(0u8, *width)),
            Self::String {
                offsets,
                data,
                absent,
            } => {
                data.extend_from_slice(absent);
                offsets.push(data.len() as u64);
            }
            Self::NullableFixed {
                width,
                null_map,
                inner,
            } => {
                null_map.push(1);
                inner.extend(std::iter::repeat_n(0u8, *width));
            }
            Self::NullableString {
                offsets,
                data,
                null_map,
                absent,
            } => {
                null_map.push(1);
                data.extend_from_slice(absent);
                offsets.push(data.len() as u64);
            }
            Self::Oracle(o) => o.push(OracleCell::Default),
        }
    }

    fn append_fixed_bytes(&mut self, le: &[u8]) -> Result<(), EmitterError> {
        match self {
            Self::Fixed { width, bytes } => {
                if le.len() != *width {
                    return Err(EmitterError::Type(format!(
                        "fixed-width mismatch: expected {} bytes, got {}",
                        *width,
                        le.len()
                    )));
                }
                bytes.extend_from_slice(le);
                Ok(())
            }
            Self::NullableFixed {
                width,
                null_map,
                inner,
            } => {
                if le.len() != *width {
                    return Err(EmitterError::Type(format!(
                        "nullable-fixed-width mismatch: expected {} bytes, got {}",
                        *width,
                        le.len()
                    )));
                }
                null_map.push(0);
                inner.extend_from_slice(le);
                Ok(())
            }
            _ => Err(unsupported("fixed-width value into string-shaped buffer")),
        }
    }

    /// Same wire layout, so `other` can follow these rows
    pub(crate) fn same_shape(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Fixed { width: a, .. }, Self::Fixed { width: b, .. })
            | (Self::NullableFixed { width: a, .. }, Self::NullableFixed { width: b, .. }) => {
                a == b
            }
            (Self::String { absent: a, .. }, Self::String { absent: b, .. })
            | (Self::NullableString { absent: a, .. }, Self::NullableString { absent: b, .. }) => {
                a == b
            }
            (Self::Oracle(a), Self::Oracle(b)) => a.same_column(b),
            _ => false,
        }
    }

    /// Append `other`'s rows; caller checked [`Self::same_shape`]
    pub(crate) fn extend(&mut self, other: Self) {
        let rebase = |offsets: &mut Vec<u64>, base: usize, more: Vec<u64>| {
            offsets.extend(more.into_iter().map(|o| o + base as u64));
        };
        match (self, other) {
            (Self::Fixed { bytes, .. }, Self::Fixed { bytes: more, .. }) => {
                bytes.extend_from_slice(&more);
            }
            (
                Self::String { offsets, data, .. },
                Self::String {
                    offsets: more,
                    data: more_data,
                    ..
                },
            ) => {
                rebase(offsets, data.len(), more);
                data.extend_from_slice(&more_data);
            }
            (
                Self::NullableFixed {
                    null_map, inner, ..
                },
                Self::NullableFixed {
                    null_map: more_nulls,
                    inner: more,
                    ..
                },
            ) => {
                null_map.extend_from_slice(&more_nulls);
                inner.extend_from_slice(&more);
            }
            (
                Self::NullableString {
                    offsets,
                    data,
                    null_map,
                    ..
                },
                Self::NullableString {
                    offsets: more,
                    data: more_data,
                    null_map: more_nulls,
                    ..
                },
            ) => {
                rebase(offsets, data.len(), more);
                data.extend_from_slice(&more_data);
                null_map.extend_from_slice(&more_nulls);
            }
            (Self::Oracle(a), Self::Oracle(b)) => a.append(b),
            _ => unreachable!("chunk shape checked before merge"),
        }
    }

    pub(crate) fn encode_into(&self, out: &mut Vec<u8>) {
        use crate::xact::spill::{push_bytes, push_u8, push_u32};
        let offsets = |out: &mut Vec<u8>, offsets: &[u64]| {
            push_u32(out, offsets.len() as u32);
            for o in offsets {
                out.extend_from_slice(&o.to_le_bytes());
            }
        };
        match self {
            Self::Fixed { width, bytes } => {
                push_u8(out, 0);
                push_u32(out, *width as u32);
                push_bytes(out, bytes);
            }
            Self::String {
                offsets: o,
                data,
                absent,
            } => {
                push_u8(out, 1);
                push_u8(out, u8::from(!absent.is_empty()));
                offsets(out, o);
                push_bytes(out, data);
            }
            Self::NullableFixed {
                width,
                null_map,
                inner,
            } => {
                push_u8(out, 2);
                push_u32(out, *width as u32);
                push_bytes(out, null_map);
                push_bytes(out, inner);
            }
            Self::NullableString {
                offsets: o,
                data,
                null_map,
                absent,
            } => {
                push_u8(out, 3);
                push_u8(out, u8::from(!absent.is_empty()));
                offsets(out, o);
                push_bytes(out, data);
                push_bytes(out, null_map);
            }
            Self::Oracle(o) => {
                push_u8(out, 4);
                o.encode_into(out);
            }
        }
    }

    pub(crate) fn decode(
        c: &mut crate::xact::spill::Cursor<'_>,
    ) -> Result<Self, crate::xact::spill::SpillError> {
        let absent = |c: &mut crate::xact::spill::Cursor<'_>| {
            Ok::<&'static [u8], crate::xact::spill::SpillError>(if c.u8()? != 0 {
                b"{}"
            } else {
                b""
            })
        };
        let offsets = |c: &mut crate::xact::spill::Cursor<'_>| {
            let n = c.u32()? as usize;
            Ok::<_, crate::xact::spill::SpillError>(
                c.need(n * 8)?
                    .as_chunks::<8>()
                    .0
                    .iter()
                    .map(|b| u64::from_le_bytes(*b))
                    .collect::<Vec<_>>(),
            )
        };
        Ok(match c.u8()? {
            0 => Self::Fixed {
                width: c.u32()? as usize,
                bytes: c.bytes()?,
            },
            1 => {
                let absent = absent(c)?;
                Self::String {
                    offsets: offsets(c)?,
                    data: c.bytes()?,
                    absent,
                }
            }
            2 => Self::NullableFixed {
                width: c.u32()? as usize,
                null_map: c.bytes()?,
                inner: c.bytes()?,
            },
            3 => {
                let absent = absent(c)?;
                Self::NullableString {
                    offsets: offsets(c)?,
                    data: c.bytes()?,
                    null_map: c.bytes()?,
                    absent,
                }
            }
            4 => Self::Oracle(OracleColumnBuf::decode(c)?),
            tag => {
                return Err(crate::xact::spill::SpillError::Format {
                    offset: 0,
                    detail: format!("unknown column buffer tag {tag}"),
                });
            }
        })
    }

    fn append_string_bytes(&mut self, raw: &[u8]) -> Result<(), EmitterError> {
        match self {
            Self::String { offsets, data, .. } => {
                data.extend_from_slice(raw);
                offsets.push(data.len() as u64);
                Ok(())
            }
            Self::NullableString {
                offsets,
                data,
                null_map,
                ..
            } => {
                null_map.push(0);
                data.extend_from_slice(raw);
                offsets.push(data.len() as u64);
                Ok(())
            }
            _ => Err(unsupported("string value into fixed-shaped buffer")),
        }
    }
}

/// PostgreSQL returns literal String cells unchanged
pub(crate) fn literal_column(buf: &OracleColumnBuf, n_rows: usize) -> Option<ColumnBuf> {
    if !buf.resolves_batch_locally(n_rows) {
        return None;
    }
    let offsets = Vec::with_capacity(n_rows);
    let data = Vec::new();
    let mut local = if buf.nullable_target() {
        ColumnBuf::NullableString {
            offsets,
            data,
            null_map: Vec::with_capacity(n_rows),
            absent: b"",
        }
    } else {
        ColumnBuf::String {
            offsets,
            data,
            absent: b"",
        }
    };
    for cell in buf.cells() {
        if let OracleCell::Literal(bytes) = cell {
            local.append_string_bytes(bytes).ok()?;
        } else {
            local.append_default();
        }
    }
    Some(local)
}

pub(crate) fn build_leaf(
    buf: &ColumnBuf,
    n_rows: usize,
) -> Result<Option<ColumnBuilder<'_>>, EmitterError> {
    Ok(match buf {
        ColumnBuf::NullableFixed { width, inner, .. } => {
            Some(ColumnBuilder::fixed(inner, *width, n_rows)?)
        }
        ColumnBuf::NullableString { offsets, data, .. } => {
            Some(ColumnBuilder::string(offsets, data, n_rows)?)
        }
        _ => None,
    })
}

/// Borrow leaf from storage that outlives returned root, `None` for oracle
/// columns which carry no local wire shape
pub(crate) fn build_root<'b>(
    buf: &'b ColumnBuf,
    leaf: Option<&'b ColumnBuilder<'b>>,
    n_rows: usize,
) -> Result<Option<ColumnBuilder<'b>>, EmitterError> {
    let wrap = |null_map: &'b [u8]| -> Result<ColumnBuilder<'b>, EmitterError> {
        let leaf = leaf.ok_or_else(|| EmitterError::Type("nullable column without leaf".into()))?;
        Ok(leaf.nullable(null_map)?)
    };
    Ok(Some(match buf {
        ColumnBuf::Fixed { width, bytes } => ColumnBuilder::fixed(bytes, *width, n_rows)?,
        ColumnBuf::String { offsets, data, .. } => ColumnBuilder::string(offsets, data, n_rows)?,
        ColumnBuf::NullableFixed { null_map, .. } | ColumnBuf::NullableString { null_map, .. } => {
            wrap(null_map)?
        }
        ColumnBuf::Oracle(_) => return Ok(None),
    }))
}

pub(crate) fn push_fixed(buf: &mut ColumnBuf, le: &[u8]) -> Result<(), EmitterError> {
    buf.append_fixed_bytes(le)
}

/// Wire metadata for a (possibly `Nullable`) CH `Decimal`, else `None`.
/// Peels one `Nullable` layer like [`ColumnBuf::new_for_ast`].
pub(crate) fn decimal_wire_of(ast: &TypeAst) -> Option<DecimalWire> {
    let inner = types::strip_nullable(ast.view());
    let width = match inner.kind()? {
        Kind::Decimal32 => DecimalWidth::D32,
        Kind::Decimal64 => DecimalWidth::D64,
        Kind::Decimal128 => DecimalWidth::D128,
        Kind::Decimal256 => DecimalWidth::D256,
        _ => return None,
    };
    Some(DecimalWire {
        scale: u8::try_from(inner.decimal_scale()?).ok()?,
        width,
    })
}

/// Buffer shape a type encodes into, Nullable-transparent (the null map is
/// orthogonal to the value wire).
enum WireShape {
    Fixed(usize),
    Str,
}

fn wire_shape_of(ast: &TypeAst) -> Option<(WireShape, Kind)> {
    let inner = types::strip_nullable(ast.view());
    let shape = match inner.elem_size() {
        0 => WireShape::Str,
        w => WireShape::Fixed(w),
    };
    Some((shape, inner.kind()?))
}

/// Keep temporal kinds compatible; Decimal conversion also permits String
/// or signed integers. Other scalar overrides require matching wire shape.
pub(crate) fn override_wire(default: &TypeAst, over: &TypeAst) -> Option<Option<DecimalWire>> {
    let source_kind = types::strip_nullable(default.view()).kind();
    let target_kind = types::strip_nullable(over.view()).kind();
    let temporal = |k| matches!(k, Some(Kind::Date32 | Kind::DateTime64 | Kind::Time64));
    if temporal(source_kind) || temporal(target_kind) {
        return (source_kind == target_kind).then_some(None);
    }
    if decimal_wire_of(default).is_some() {
        if let Some(w) = decimal_wire_of(over) {
            return Some(Some(w));
        }
        return match wire_shape_of(over)? {
            (WireShape::Str, _) => Some(None),
            (WireShape::Fixed(_), kind) => {
                let width = match kind {
                    Kind::Int32 => DecimalWidth::D32,
                    Kind::Int64 => DecimalWidth::D64,
                    Kind::Int128 => DecimalWidth::D128,
                    Kind::Int256 => DecimalWidth::D256,
                    _ => return None,
                };
                Some(Some(DecimalWire { scale: 0, width }))
            }
        };
    }
    match (wire_shape_of(default)?.0, wire_shape_of(over)?.0) {
        (WireShape::Str, WireShape::Str) => Some(None),
        (WireShape::Fixed(a), WireShape::Fixed(b)) if a == b => {
            if decimal_wire_of(over).is_some() {
                None
            } else {
                Some(None)
            }
        }
        _ => None,
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct U256([u64; 4]);

impl U256 {
    fn is_zero(self) -> bool {
        self.0 == [0; 4]
    }

    fn one_shl(bit: usize) -> Self {
        let mut limbs = [0; 4];
        limbs[bit / 64] = 1u64 << (bit % 64);
        Self(limbs)
    }

    fn checked_mul_small(&mut self, rhs: u32) -> bool {
        let mut carry = 0u128;
        for limb in &mut self.0 {
            let v = (*limb as u128) * (rhs as u128) + carry;
            *limb = v as u64;
            carry = v >> 64;
        }
        carry == 0
    }

    fn checked_add_small(&mut self, rhs: u32) -> bool {
        let mut carry = rhs as u128;
        for limb in &mut self.0 {
            let v = (*limb as u128) + carry;
            *limb = v as u64;
            carry = v >> 64;
            if carry == 0 {
                return true;
            }
        }
        false
    }

    fn div_small(&mut self, rhs: u32) -> u32 {
        let mut rem = 0u128;
        let rhs = rhs as u128;
        for limb in self.0.iter_mut().rev() {
            let v = (rem << 64) | (*limb as u128);
            *limb = (v / rhs) as u64;
            rem = v % rhs;
        }
        rem as u32
    }

    fn from_le_bytes(b: [u8; 32]) -> Self {
        Self(std::array::from_fn(|i| {
            u64::from_le_bytes(b[i * 8..][..8].try_into().expect("8 bytes"))
        }))
    }

    fn to_le_bytes(self) -> [u8; 32] {
        let mut out = [0u8; 32];
        for (i, limb) in self.0.iter().enumerate() {
            out[i * 8..][..8].copy_from_slice(&limb.to_le_bytes());
        }
        out
    }
}

impl Ord for U256 {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        for (a, b) in self.0.iter().rev().zip(other.0.iter().rev()) {
            match a.cmp(b) {
                std::cmp::Ordering::Equal => {}
                ord => return ord,
            }
        }
        std::cmp::Ordering::Equal
    }
}

impl PartialOrd for U256 {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// Decimal text of a CH `Decimal` cell, a two's-complement little-endian
/// integer scaled by `10^scale`
pub(crate) fn decimal_le_to_text(le: &[u8], scale: u8) -> String {
    let neg = le.last().is_some_and(|b| b & 0x80 != 0);
    let mut wide = [if neg { 0xFF } else { 0 }; 32];
    wide[..le.len()].copy_from_slice(le);
    if neg {
        let mut carry = 1u16;
        for b in &mut wide {
            let v = u16::from(!*b) + carry;
            *b = v as u8;
            carry = v >> 8;
        }
    }
    let mut mag = U256::from_le_bytes(wide);
    let mut digits = Vec::new();
    loop {
        digits.push(b'0' + mag.div_small(10) as u8);
        if mag.is_zero() {
            break;
        }
    }
    let scale = usize::from(scale);
    digits.resize(digits.len().max(scale + 1), b'0');
    digits.reverse();
    let (int, frac) = digits.split_at(digits.len() - scale);
    let mut out = String::with_capacity(digits.len() + 2);
    if neg {
        out.push('-');
    }
    out.extend(int.iter().map(|&b| char::from(b)));
    if scale > 0 {
        out.push('.');
        out.extend(frac.iter().map(|&b| char::from(b)));
    }
    out
}

fn decimal_oob() -> EmitterError {
    unsupported("numeric out of range for Decimal column")
}

/// PG `numeric_out` text (eg `-12.340`) to the scaled integer a CH
/// `Decimal(_, scale)` stores: `value * 10^scale`, two's-complement
/// little-endian at the Decimal wire width. PG values conform to column
/// typmod so text dscale normally equals `scale`; rescale handles
/// integer-valued numerics and defensive target-scale overrides.
fn decimal_text_to_scaled_le(
    text: &str,
    scale: u8,
    width: DecimalWidth,
) -> Result<[u8; 32], EmitterError> {
    let width = width.bytes();
    let scale = i32::from(scale);

    let neg = text.starts_with('-');
    // numeric_out emits a single optional '-'; a residual sign in `body`
    // is malformed and the parse loop rejects it as a non-digit
    let body = text.strip_prefix(['-', '+']).unwrap_or(text);
    let mut mag = U256::default();
    let mut frac_digits = 0i32;
    let mut seen_dot = false;
    let mut saw_digit = false;

    for b in body.bytes() {
        match b {
            b'.' if !seen_dot => seen_dot = true,
            b'0'..=b'9' => {
                saw_digit = true;
                if !mag.checked_mul_small(10) || !mag.checked_add_small((b - b'0') as u32) {
                    return Err(decimal_oob());
                }
                if seen_dot {
                    frac_digits += 1;
                }
            }
            _ => return Err(decimal_oob()),
        }
    }
    if !saw_digit {
        return Err(decimal_oob());
    }

    let diff = scale - frac_digits;
    if diff > 0 {
        for _ in 0..diff {
            if !mag.checked_mul_small(10) {
                return Err(decimal_oob());
            }
        }
    } else if diff < 0 {
        // More fractional digits than column scale: PG would have
        // rounded on store, so defensive trunc, shouldn't occur for
        // conforming values
        for _ in 0..-diff {
            mag.div_small(10);
        }
    }

    signed_le(mag, neg, width)
}

/// Stored `numeric` body to the scaled integer a CH `Decimal(_, scale)`
/// stores, read from its base-10000 digits. Digits past `scale` drop
fn numeric_to_scaled_le(
    body: &[u8],
    scale: u8,
    width: DecimalWidth,
) -> Result<[u8; 32], EmitterError> {
    let scale = u32::from(scale);
    let (neg, weight, digits) = match parse_numeric(body) {
        Ok(Numeric::Finite {
            neg,
            weight,
            digits,
            ..
        }) => (neg, weight, digits),
        Ok(Numeric::Special(_)) => {
            return Err(unsupported(
                "non-finite numeric (NaN/Inf) into Decimal column, set nan/pos_inf/neg_inf or map to String",
            ));
        }
        Err(_) => return Err(unsupported("malformed numeric")),
    };
    let mut mag = U256::default();
    if !crate::decode::codecs::scaled_groups(weight, digits, scale, |m, a| {
        mag.checked_mul_small(m) && mag.checked_add_small(a)
    }) {
        return Err(decimal_oob());
    }
    signed_le(mag, neg, width.bytes())
}

/// Two's-complement little-endian `mag`, bounded by the Decimal wire width
fn signed_le(mag: U256, neg: bool, width: usize) -> Result<[u8; 32], EmitterError> {
    // Bound by physical wire width (signed Int{32,64,128,256} range),
    // not logical Decimal(p,s) precision 10^p. Backstop turning a
    // too-wide value (eg operator override onto a narrower Decimal) into
    // a clean error instead of a silently truncated store.
    let limit = U256::one_shl(width * 8 - 1);
    if (!neg && mag >= limit) || (neg && mag > limit) {
        return Err(decimal_oob());
    }

    let mut out = mag.to_le_bytes();
    if neg && !mag.is_zero() {
        for b in &mut out[..width] {
            *b = !*b;
        }
        let mut carry = 1u16;
        for b in &mut out[..width] {
            let v = (*b as u16) + carry;
            *b = v as u8;
            carry = v >> 8;
            if carry == 0 {
                break;
            }
        }
    }
    Ok(out)
}

fn unsupported(kind: &'static str) -> EmitterError {
    EmitterError::UnsupportedValue {
        target_column: String::new(),
        kind,
    }
}

pub(crate) fn timestamp_ticks(pg_us: i64, scale: u32) -> Result<i64, EmitterError> {
    micros_to_ticks(
        i128::from(pg_us) + i128::from(DATETIME64_PG_EPOCH_US),
        scale,
    )
}

/// Convert microseconds to `DateTime64(scale)` or `Time64(scale)` ticks
fn micros_to_ticks(us: i128, scale: u32) -> Result<i64, EmitterError> {
    if scale > 9 {
        return Err(EmitterError::Type("invalid temporal precision".into()));
    }
    let ticks = if scale < 6 {
        us / 10i128.pow(6 - scale)
    } else {
        us * 10i128.pow(scale - 6)
    };
    i64::try_from(ticks).map_err(|_| unsupported("finite temporal value overflows storage"))
}

pub(crate) fn fixed<const N: usize>(body: &[u8]) -> Result<[u8; N], EmitterError> {
    body.try_into()
        .map_err(|_| unsupported("stored width differs from source type"))
}

/// Encode one source datum into its column per the plan's codec
pub(crate) fn encode_datum(
    buf: &mut ColumnBuf,
    plan: &ColumnPlan,
    datum: DatumRef<'_>,
    text: &mut String,
) -> Result<(), EmitterError> {
    let (codec, type_oid) = plan
        .source
        .map_or((Codec::Le, 0), |s| (s.codec, s.type_oid));
    let body = match datum {
        // Absent coerces like NULL, covers key-only delete tombstones under
        // non-FULL replica identity. Non-Nullable target takes type default
        DatumRef::Absent | DatumRef::Null | DatumRef::Dropped => {
            buf.append_default();
            return Ok(());
        }
        DatumRef::Missing => return Err(unsupported("fast default left unresolved")),
        DatumRef::External(_) => {
            return Err(unsupported(
                "unresolved TOAST pointer (xact buffer should have reassembled)",
            ));
        }
        DatumRef::BadTag(_) | DatumRef::Send(_) => {
            return Err(unsupported(
                "unresolved source value routed to a local column",
            ));
        }
        DatumRef::Compressed(d) => {
            let raw = inflate(d).ok_or_else(|| unsupported("corrupt compressed varlena"))?;
            return encode_datum(buf, plan, DatumRef::Varlena(&raw), text);
        }
        DatumRef::Text(t) => {
            return match text_datum(type_oid, t) {
                TextDatum::Datum(d) => encode_datum(buf, plan, d.view(), text),
                TextDatum::Null => {
                    buf.append_default();
                    Ok(())
                }
                TextDatum::Opaque => buf.append_string_bytes(t),
            };
        }
        DatumRef::Fixed(b) | DatumRef::Varlena(b) | DatumRef::Cstring(b) => b,
    };
    if let Some(fill) = plan.non_finite.fill(codec, body) {
        return match fill {
            Fill::Null => {
                buf.append_default();
                Ok(())
            }
            Fill::Le(le) => buf.append_fixed_bytes(le),
        };
    }
    let rendered = |buf: &mut ColumnBuf, s: &str| buf.append_string_bytes(s.as_bytes());
    match codec {
        Codec::Le => buf.append_fixed_bytes(body),
        Codec::Date => match i32::from_le_bytes(fixed(body)?) {
            i32::MIN | i32::MAX => Err(temporal_infinity()),
            n => {
                let days = n
                    .checked_add(DATE32_PG_EPOCH_DAYS)
                    .ok_or_else(|| unsupported("finite date overflows Date32 epoch conversion"))?;
                buf.append_fixed_bytes(&days.to_le_bytes())
            }
        },
        // PG time counts microseconds since midnight, without an epoch offset
        Codec::Time => {
            let us = i64::from_le_bytes(fixed(body)?);
            let ticks = micros_to_ticks(i128::from(us), plan.datetime64_scale)?;
            buf.append_fixed_bytes(&ticks.to_le_bytes())
        }
        Codec::Timestamp => match i64::from_le_bytes(fixed(body)?) {
            i64::MIN | i64::MAX => Err(temporal_infinity()),
            us => {
                let ticks = timestamp_ticks(us, plan.datetime64_scale)?;
                buf.append_fixed_bytes(&ticks.to_le_bytes())
            }
        },
        // CH has no zone-aware time type, text keeps the offset
        Codec::TimeTz => {
            let b: [u8; 12] = fixed(body)?;
            let micros = i64::from_le_bytes(b[..8].try_into().expect("8 bytes"));
            let tz_seconds = i32::from_le_bytes(b[8..].try_into().expect("4 bytes"));
            rendered(
                buf,
                &crate::decode::codecs::timetz_to_text(micros, tz_seconds),
            )
        }
        Codec::Uuid => {
            buf.append_fixed_bytes(&crate::decode::codecs::uuid_to_ch_wire(&fixed(body)?))
        }
        // NUL-padded to NAMEDATALEN
        Codec::Name => {
            let end = body.iter().position(|&b| b == 0).unwrap_or(body.len());
            simdutf8::basic::from_utf8(&body[..end])
                .map_err(|_| unsupported("unresolved source value routed to a local column"))?;
            buf.append_string_bytes(&body[..end])
        }
        // PG validates UTF-8 on input, so bytes pass as they are
        Codec::Bytes => buf.append_string_bytes(body),
        Codec::Numeric => match plan.decimal {
            // Non-finite is unrepresentable in Decimal: error rather than
            // corrupt unless the column sets a substitute or maps to String
            Some(decimal) => {
                let scaled = numeric_to_scaled_le(body, decimal.scale, decimal.width)?;
                buf.append_fixed_bytes(&scaled[..decimal.width.bytes()])
            }
            None => {
                text.clear();
                crate::decode::codecs::render_numeric_text(body, text)
                    .map_err(|_| unsupported("malformed numeric"))?;
                rendered(buf, text)
            }
        },
        Codec::Jsonb => {
            text.clear();
            crate::decode::codecs::render_jsonb_text(body, text)
                .map_err(|_| unsupported("malformed jsonb"))?;
            rendered(buf, text)
        }
        Codec::Inet | Codec::Cidr => {
            let v = crate::decode::codecs::decode_inet(body, codec == Codec::Cidr)
                .map_err(|_| unsupported("malformed inet"))?;
            rendered(buf, &v.to_text())
        }
        Codec::MacAddr => rendered(buf, &crate::decode::codecs::macaddr_to_text(&fixed(body)?)),
        Codec::MacAddr8 => rendered(buf, &crate::decode::codecs::macaddr8_to_text(&fixed(body)?)),
        Codec::Tid => {
            let (block, offset) = crate::decode::codecs::decode_tid(&fixed(body)?);
            rendered(buf, &crate::decode::codecs::tid_to_text(block, offset))
        }
        Codec::Interval => {
            let v = crate::decode::codecs::decode_interval(body)
                .map_err(|_| unsupported("malformed interval"))?;
            rendered(buf, &v.to_text())
        }
        Codec::Wkt => match crate::ops::oracle::point_wkt(body) {
            Some(wkt) => rendered(buf, &wkt),
            None => Err(unsupported(
                "unresolved source value routed to a local column",
            )),
        },
        // Never interpret raw Datum bytes as local String data
        Codec::Pending => Err(unsupported(
            "unresolved source value routed to a local column",
        )),
    }
}

fn temporal_infinity() -> EmitterError {
    unsupported("temporal infinity, set pos_inf/neg_inf to null on a Nullable destination")
}

/// Map NULL, absent, and unlogged fields to default cells. Stored bodies go
/// to typinput's inverse, text bypasses typoutput
pub(crate) fn oracle_cell(
    datum: DatumRef<'_>,
    source: Option<SourceColumn>,
    source_type_oid: u32,
) -> Result<OracleCell, EmitterError> {
    use crate::schema::{BPCHAROID, TEXTOID, VARCHAROID};
    let unresolved = |kind| EmitterError::UnsupportedValue {
        target_column: String::new(),
        kind,
    };
    let (codec, type_oid) = source.map_or((Codec::Le, 0), |s| (s.codec, s.type_oid));
    let body = match datum {
        DatumRef::Absent | DatumRef::Null | DatumRef::Dropped => return Ok(OracleCell::Default),
        _ if type_oid != source_type_oid || type_oid == 0 => {
            return Err(unresolved(if type_oid == 0 {
                "decoded value on an oracle column"
            } else {
                "source type oid differs from the planned one"
            }));
        }
        DatumRef::Text(t) => return Ok(OracleCell::TextInput(t.to_vec())),
        DatumRef::Send(b) => return Ok(OracleCell::BinaryInput(b.to_vec())),
        DatumRef::Missing => return Err(unresolved("fast default left unresolved")),
        DatumRef::External(_) | DatumRef::BadTag(_) => {
            return Err(unresolved("unresolved source value on an oracle column"));
        }
        DatumRef::Compressed(d) => {
            let raw = inflate(d).ok_or_else(|| unresolved("corrupt compressed varlena"))?;
            return oracle_cell(DatumRef::Varlena(&raw), source, source_type_oid);
        }
        DatumRef::Fixed(b) | DatumRef::Varlena(b) | DatumRef::Cstring(b) => b,
    };
    Ok(match codec {
        // PostGIS WKT must bypass HEXEWKB typoutput
        Codec::Wkt => crate::ops::oracle::point_wkt(body).map_or_else(
            || OracleCell::DiskRaw(body.to_vec()),
            |wkt| OracleCell::Literal(wkt.into_bytes()),
        ),
        Codec::Name => {
            let end = body.iter().position(|&b| b == 0).unwrap_or(body.len());
            OracleCell::Literal(body[..end].to_vec())
        }
        Codec::Bytes if matches!(datum, DatumRef::Cstring(_)) => OracleCell::Literal(body.to_vec()),
        Codec::Bytes if matches!(type_oid, TEXTOID | VARCHAROID | BPCHAROID) => {
            OracleCell::Literal(body.to_vec())
        }
        _ => OracleCell::DiskRaw(body.to_vec()),
    })
}

/// Default of `att` encoded for a one-row `ch_inner` column; `None` without
/// a fast default or when the column can't hold it locally
pub(crate) fn encode_default(att: &RelAttr, ch_inner: &str) -> Option<ColumnBuf> {
    let datum = missing_datum(att);
    if datum == DatumRef::Null {
        return None;
    }
    let ast = TypeAst::parse(ch_inner, Allocator::global(&mimalloc::MiMalloc)).ok()?;
    if composite_target(&ast) {
        return None;
    }
    let mut buf = ColumnBuf::new_for_ast(&ast).ok()?;
    let plan = ColumnPlan {
        name: att.name.clone(),
        type_repr: ch_inner.into(),
        decimal: decimal_wire_of(&ast),
        datetime64_scale: types::strip_nullable(ast.view())
            .datetime64_scale()
            .unwrap_or(0),
        ast,
        non_finite: NonFinite::default(),
        encoding: ColumnEncoding::Local,
        source: Some(SourceColumn {
            index: 0,
            codec: Codec::choose(att),
            type_oid: att.type_oid,
        }),
    };
    encode_datum(&mut buf, &plan, datum, &mut String::new()).ok()?;
    Some(buf)
}

/// Comparable view of a buffer's wire content
#[cfg(test)]
#[derive(Debug, PartialEq)]
pub(crate) enum Wire<'a> {
    Fixed {
        width: usize,
        bytes: &'a [u8],
    },
    String {
        offsets: &'a [u64],
        data: &'a [u8],
        absent: &'a [u8],
    },
    NullableFixed {
        width: usize,
        null_map: &'a [u8],
        inner: &'a [u8],
    },
    NullableString {
        offsets: &'a [u64],
        data: &'a [u8],
        null_map: &'a [u8],
        absent: &'a [u8],
    },
    Oracle(&'a [OracleCell]),
}

#[cfg(test)]
impl ColumnBuf {
    pub(crate) fn wire(&self) -> Wire<'_> {
        match self {
            Self::Fixed { width, bytes } => Wire::Fixed {
                width: *width,
                bytes,
            },
            Self::String {
                offsets,
                data,
                absent,
            } => Wire::String {
                offsets,
                data,
                absent,
            },
            Self::NullableFixed {
                width,
                null_map,
                inner,
            } => Wire::NullableFixed {
                width: *width,
                null_map,
                inner,
            },
            Self::NullableString {
                offsets,
                data,
                null_map,
                absent,
            } => Wire::NullableString {
                offsets,
                data,
                null_map,
                absent,
            },
            Self::Oracle(o) => Wire::Oracle(o.cells()),
        }
    }
}

impl std::fmt::Debug for ColumnBuf {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Fixed { width, bytes } => f
                .debug_struct("Fixed")
                .field("width", width)
                .field("bytes_len", &bytes.len())
                .finish(),
            Self::String { offsets, data, .. } => f
                .debug_struct("String")
                .field("rows", &offsets.len())
                .field("data_len", &data.len())
                .finish(),
            Self::NullableFixed {
                width,
                null_map,
                inner,
            } => f
                .debug_struct("NullableFixed")
                .field("width", width)
                .field("rows", &null_map.len())
                .field("inner_len", &inner.len())
                .finish(),
            Self::NullableString {
                offsets,
                data,
                null_map,
                ..
            } => f
                .debug_struct("NullableString")
                .field("rows", &null_map.len())
                .field("offsets_len", &offsets.len())
                .field("data_len", &data.len())
                .finish(),
            Self::Oracle(o) => f
                .debug_struct("Oracle")
                .field("rows", &o.cells().len())
                .field("source_oid", &o.source_type_oid)
                .field("wire_bytes", &o.approx_size())
                .finish(),
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::decode::datum::{Datum, build};

    /// Encode `v` alone under `codec`, as a column of `decimal` or
    /// `timestamp_scale` would
    fn encode_value(
        buf: &mut ColumnBuf,
        codec: Codec,
        v: &Datum,
        decimal: Option<DecimalWire>,
        timestamp_scale: u32,
    ) -> Result<(), EmitterError> {
        let source = SourceColumn {
            index: 0,
            codec,
            type_oid: 0,
        };
        encode_ref(buf, source, v.view(), decimal, timestamp_scale)
    }

    /// [`encode_value`] of a borrowed `v` from a `source` column
    fn encode_ref(
        buf: &mut ColumnBuf,
        source: SourceColumn,
        v: DatumRef<'_>,
        decimal: Option<DecimalWire>,
        timestamp_scale: u32,
    ) -> Result<(), EmitterError> {
        let plan = ColumnPlan {
            name: String::new(),
            type_repr: String::new(),
            ast: TypeAst::parse("String", Allocator::global(&mimalloc::MiMalloc)).unwrap(),
            decimal,
            datetime64_scale: timestamp_scale,
            non_finite: NonFinite::default(),
            encoding: ColumnEncoding::Local,
            source: Some(source),
        };
        encode_datum(buf, &plan, v, &mut String::new())
    }

    /// Confirm upstream `chc_type_elem_size` returns match what the
    /// encoder needs for fixed-shape ColumnBufs.
    #[test]
    fn elem_size_covers_tier1() {
        let alloc = Allocator::global(&mimalloc::MiMalloc);
        let cases = [
            ("UInt8", 1usize),
            ("Int32", 4),
            ("UInt64", 8),
            ("Float64", 8),
            ("DateTime64(6, 'UTC')", 8),
            ("Decimal32(4)", 4),
            ("FixedString(16)", 16),
        ];
        for (name, expected) in cases {
            let ast = TypeAst::parse(name, alloc).expect("parses");
            assert_eq!(ast.view().elem_size(), expected, "{name}");
        }
        // Varlen + composite types report 0 (varlen on-wire shape)
        for name in ["String", "Array(UInt32)"] {
            let ast = TypeAst::parse(name, alloc).expect("parses");
            assert_eq!(ast.view().elem_size(), 0, "{name}");
        }
    }

    #[test]
    fn literal_column_requires_string_target_and_rendered_cells() {
        let mut buf = OracleColumnBuf::string(0, -1);
        buf.push(OracleCell::Literal(b"a".to_vec()));
        buf.push(OracleCell::Default);
        let lit = literal_column(&buf, 2).expect("renders");
        assert_eq!(
            lit.wire(),
            Wire::String {
                offsets: &[1, 1],
                data: b"a",
                absent: b"",
            }
        );
        for reject in ["Array(String)", "LowCardinality(String)", "JSON", "Int32"] {
            let mut buf = OracleColumnBuf::new(
                0,
                -1,
                TypeAst::parse(reject, Allocator::global(&mimalloc::MiMalloc))
                    .unwrap()
                    .view(),
            );
            buf.push(OracleCell::Literal(b"a".to_vec()));
            buf.push(OracleCell::Default);
            assert!(
                literal_column(&buf, 2).is_none(),
                "{reject} needs the worker",
            );
        }
        // Cell count must match the batch, else offsets would not
        assert!(literal_column(&buf, 3).is_none());
        for cell in [
            OracleCell::DiskRaw(b"a".to_vec()),
            OracleCell::TextInput(b"a".to_vec()),
        ] {
            for target in ["String", "Nullable(String)"] {
                let mut buf = OracleColumnBuf::new(
                    0,
                    -1,
                    TypeAst::parse(target, Allocator::global(&mimalloc::MiMalloc))
                        .unwrap()
                        .view(),
                );
                buf.push(OracleCell::Literal(b"a".to_vec()));
                buf.push(cell.clone());
                assert!(literal_column(&buf, 2).is_none());
            }
        }
    }

    /// COPY `typsend` bytes take the oracle's binary input under the planned
    /// source type only; NULL stays a default cell
    #[test]
    fn send_datums_become_binary_input_cells() {
        let source = Some(SourceColumn {
            index: 0,
            codec: Codec::Pending,
            type_oid: 16_500,
        });
        assert_eq!(
            oracle_cell(DatumRef::Send(&[0, 1]), source, 16_500).unwrap(),
            OracleCell::BinaryInput(vec![0, 1])
        );
        assert_eq!(
            oracle_cell(DatumRef::Null, source, 16_500).unwrap(),
            OracleCell::Default
        );
        assert!(oracle_cell(DatumRef::Send(b"x"), source, 16_501).is_err());
    }

    #[test]
    fn new_for_ast_picks_shape_from_chc_type_kind() {
        let alloc = Allocator::global(&mimalloc::MiMalloc);
        let cases = [
            ("Int32", "Fixed { width: 4, bytes_len: 0 }"),
            ("String", "String { rows: 0, data_len: 0 }"),
            (
                "Nullable(Int64)",
                "NullableFixed { width: 8, rows: 0, inner_len: 0 }",
            ),
            (
                "Nullable(String)",
                "NullableString { rows: 0, offsets_len: 0, data_len: 0 }",
            ),
            ("FixedString(7)", "Fixed { width: 7, bytes_len: 0 }"),
            (
                "Nullable(FixedString(7))",
                "NullableFixed { width: 7, rows: 0, inner_len: 0 }",
            ),
        ];
        for (name, shape) in cases {
            let ast = TypeAst::parse(name, alloc).expect("parses");
            let buf = ColumnBuf::new_for_ast(&ast).expect("shape");
            assert_eq!(format!("{buf:?}"), shape, "{name}");
        }
    }

    #[test]
    fn decimal_text_scales_to_integer() {
        fn le_i64(text: &str, scale: u8) -> [u8; 8] {
            let le = decimal_text_to_scaled_le(text, scale, DecimalWidth::D64).unwrap();
            le[..8].try_into().unwrap()
        }

        assert_eq!(le_i64("0", 2), 0i64.to_le_bytes());
        assert_eq!(le_i64("12", 2), 1200i64.to_le_bytes());
        assert_eq!(le_i64("1.50", 2), 150i64.to_le_bytes());
        assert_eq!(le_i64("-12.34", 2), (-1234i64).to_le_bytes());
        assert_eq!(le_i64("0.001", 3), 1i64.to_le_bytes());
        assert_eq!(le_i64("123.456", 2), 12345i64.to_le_bytes());
        assert!(decimal_text_to_scaled_le("--5", 0, DecimalWidth::D64).is_err());
        assert!(decimal_text_to_scaled_le("", 0, DecimalWidth::D64).is_err());
        assert!(decimal_text_to_scaled_le("1.2.3", 0, DecimalWidth::D64).is_err());
    }

    #[test]
    fn decimal_text_rejects_signed_width_overflow() {
        let max = i128::MAX.to_string();
        let le = decimal_text_to_scaled_le(&max, 0, DecimalWidth::D128).unwrap();
        assert_eq!(&le[..16], &i128::MAX.to_le_bytes());

        let min_mag = "170141183460469231731687303715884105728";
        assert!(decimal_text_to_scaled_le(min_mag, 0, DecimalWidth::D128).is_err());

        let le = decimal_text_to_scaled_le(&format!("-{min_mag}"), 0, DecimalWidth::D128).unwrap();
        assert_eq!(&le[..16], &i128::MIN.to_le_bytes());
    }

    #[test]
    fn decimal_text_encodes_decimal256_width() {
        let le = decimal_text_to_scaled_le("-1", 0, DecimalWidth::D256).unwrap();
        assert_eq!(&le[..32], &[0xff; 32]);

        let wide39 = "9".repeat(39);
        assert!(decimal_text_to_scaled_le(&wide39, 0, DecimalWidth::D128).is_err());
        let le = decimal_text_to_scaled_le(&wide39, 0, DecimalWidth::D256).unwrap();
        assert!(le[16..32].iter().any(|b| *b != 0));

        let wide76 = "9".repeat(76);
        assert!(decimal_text_to_scaled_le(&wide76, 0, DecimalWidth::D256).is_ok());
    }

    #[test]
    fn encode_numeric_into_decimal_and_string() {
        let alloc = Allocator::global(&mimalloc::MiMalloc);
        let ast = TypeAst::parse("Decimal(10, 2)", alloc).unwrap();
        let decimal = decimal_wire_of(&ast);
        assert_eq!(
            decimal,
            Some(DecimalWire {
                scale: 2,
                width: DecimalWidth::D64
            })
        );
        let mut buf = ColumnBuf::new_for_ast(&ast).unwrap();
        encode_value(
            &mut buf,
            Codec::Numeric,
            &build::numeric("1.50"),
            decimal,
            6,
        )
        .unwrap();
        assert_eq!(
            buf.wire(),
            Wire::Fixed {
                width: 8,
                bytes: &150i64.to_le_bytes(),
            }
        );
        let mut buf_nan = ColumnBuf::new_for_ast(&ast).unwrap();
        assert!(
            encode_value(
                &mut buf_nan,
                Codec::Numeric,
                &build::numeric("NaN"),
                decimal,
                6,
            )
            .is_err()
        );

        let wide_ast = TypeAst::parse("Decimal(50, 2)", alloc).unwrap();
        let wide_decimal = decimal_wire_of(&wide_ast);
        assert_eq!(
            wide_decimal,
            Some(DecimalWire {
                scale: 2,
                width: DecimalWidth::D256
            })
        );
        let mut wide_buf = ColumnBuf::new_for_ast(&wide_ast).unwrap();
        let wide_text = "123456789012345678901234567890123456789012345678.12";
        encode_value(
            &mut wide_buf,
            Codec::Numeric,
            &build::numeric(wide_text),
            wide_decimal,
            6,
        )
        .unwrap();
        let wide_le = decimal_text_to_scaled_le(wide_text, 2, DecimalWidth::D256).unwrap();
        assert!(wide_le[16..32].iter().any(|b| *b != 0));
        assert_eq!(
            wide_buf.wire(),
            Wire::Fixed {
                width: 32,
                bytes: &wide_le,
            }
        );

        let sast = TypeAst::parse("String", alloc).unwrap();
        let mut sbuf = ColumnBuf::new_for_ast(&sast).unwrap();
        encode_value(&mut sbuf, Codec::Numeric, &build::numeric("NaN"), None, 6).unwrap();
        assert_eq!(
            sbuf.wire(),
            Wire::String {
                offsets: &[3],
                data: b"NaN",
                absent: b"",
            }
        );
    }

    #[test]
    fn encode_time_native_and_timetz_text() {
        let alloc = Allocator::global(&mimalloc::MiMalloc);
        let micros = 45_296_000_000i64; // 12:34:56
        let ast = TypeAst::parse("Time64(6)", alloc).unwrap();
        let mut buf = ColumnBuf::new_for_ast(&ast).unwrap();
        encode_value(&mut buf, Codec::Time, &build::micros(micros), None, 6).unwrap();
        assert_eq!(
            buf.wire(),
            Wire::Fixed {
                width: 8,
                bytes: &micros.to_le_bytes(),
            }
        );
        let ast = TypeAst::parse("Time64(3)", alloc).unwrap();
        let mut buf = ColumnBuf::new_for_ast(&ast).unwrap();
        encode_value(
            &mut buf,
            Codec::Time,
            &build::micros(micros + 7_999),
            None,
            3,
        )
        .unwrap();
        assert_eq!(
            buf.wire(),
            Wire::Fixed {
                width: 8,
                bytes: &(micros / 1_000 + 7).to_le_bytes(),
            }
        );
        let sast = TypeAst::parse("String", alloc).unwrap();
        let mut sbuf = ColumnBuf::new_for_ast(&sast).unwrap();
        encode_value(
            &mut sbuf,
            Codec::TimeTz,
            &build::timetz(micros, -7200),
            None,
            6,
        )
        .unwrap();
        assert_eq!(
            sbuf.wire(),
            Wire::String {
                offsets: &[11],
                data: b"12:34:56+02",
                absent: b"",
            }
        );
    }

    #[test]
    fn decimal_text_rejects_u256_overflow() {
        let mut max = U256([u64::MAX; 4]);
        assert!(!max.checked_add_small(1));
        let wide78 = "9".repeat(78);
        assert!(decimal_text_to_scaled_le(&wide78, 0, DecimalWidth::D256).is_err());
        let wide76 = "9".repeat(76);
        assert!(decimal_text_to_scaled_le(&wide76, 4, DecimalWidth::D256).is_err());
    }

    #[test]
    fn micros_to_ticks_rejects_precision_past_nanoseconds() {
        assert!(matches!(micros_to_ticks(1, 10), Err(EmitterError::Type(_))));
        assert_eq!(micros_to_ticks(1, 9).unwrap(), 1_000);
    }

    pub(crate) fn buf(ty: &str) -> ColumnBuf {
        let ast = TypeAst::parse(ty, Allocator::global(&mimalloc::MiMalloc)).unwrap();
        ColumnBuf::new_for_ast(&ast).unwrap()
    }

    fn rejects(r: Result<(), EmitterError>, needle: &str) -> bool {
        matches!(r, Err(EmitterError::UnsupportedValue { kind, .. }) if kind.contains(needle))
    }

    #[test]
    fn column_buffers_reject_mismatched_shapes() {
        let mut fixed = buf("Int8");
        encode_value(
            &mut fixed,
            Codec::Le,
            &Datum::fixed(&(-3i8).to_le_bytes()),
            None,
            0,
        )
        .unwrap();
        fixed.append_default();
        assert_eq!(format!("{fixed:?}"), "Fixed { width: 1, bytes_len: 2 }");
        assert!(matches!(
            fixed.append_fixed_bytes(&[0; 4]),
            Err(EmitterError::Type(_))
        ));
        let mut nullable = buf("Nullable(Int32)");
        assert!(matches!(
            nullable.append_fixed_bytes(&[0; 2]),
            Err(EmitterError::Type(_))
        ));
        let mut string = buf("String");
        assert!(rejects(string.append_fixed_bytes(&[0; 4]), "string-shaped"));
        assert!(rejects(fixed.append_string_bytes(b"x"), "fixed-shaped"));
        let mut date = buf("Date32");
        let overflow = encode_value(&mut date, Codec::Date, &build::date(i32::MAX - 1), None, 0);
        assert!(rejects(overflow, "Date32"));
        let toast = Datum::External(crate::decode::datum::ToastPointer {
            va_rawsize: 0,
            va_extinfo: 0,
            va_valueid: 0,
            va_toastrelid: 0,
        });
        let toast = encode_value(&mut string, Codec::Bytes, &toast, None, 0);
        assert!(rejects(toast, "TOAST"));
        let r = encode_value(
            &mut string,
            Codec::Pending,
            &Datum::BadTag(vec![1]),
            None,
            0,
        );
        assert!(rejects(r, "unresolved source value"));
    }

    #[test]
    fn local_codecs_reject_unreadable_bodies() {
        let alloc = Allocator::global(&mimalloc::MiMalloc);
        let d76 = decimal_wire_of(&TypeAst::parse("Decimal(76, 0)", alloc).unwrap());
        let huge = build::numeric(format!("1{}", "0".repeat(80)));
        let cases = [
            (
                Codec::Numeric,
                Datum::Varlena(vec![0]),
                d76,
                "malformed numeric",
            ),
            (Codec::Numeric, huge, d76, "out of range"),
            (
                Codec::Numeric,
                Datum::Varlena(vec![0]),
                None,
                "malformed numeric",
            ),
            (
                Codec::Jsonb,
                Datum::Varlena(Vec::new()),
                None,
                "malformed jsonb",
            ),
            (
                Codec::Interval,
                Datum::fixed(&[0; 3]),
                None,
                "malformed interval",
            ),
            (Codec::Date, Datum::fixed(&[0; 3]), None, "stored width"),
            (
                Codec::Wkt,
                Datum::Varlena(vec![0; 16]),
                None,
                "unresolved source",
            ),
        ];
        for (codec, v, decimal, want) in cases {
            let r = encode_value(&mut buf("String"), codec, &v, decimal, 0);
            assert!(rejects(r, want), "{codec:?} {v:?}");
        }
        let mut point = buf("String");
        let body = Datum::Varlena(crate::ops::oracle::point_body(1.0, 2.0));
        encode_value(&mut point, Codec::Wkt, &body, None, 0).unwrap();
        assert_eq!(
            point.wire(),
            Wire::String {
                offsets: &[10],
                data: b"POINT(1 2)",
                absent: b"",
            }
        );
        let int2 = SourceColumn {
            index: 0,
            codec: Codec::Le,
            type_oid: crate::schema::INT2OID,
        };
        let mut n = buf("Nullable(Int16)");
        let missing = encode_ref(&mut n, int2, DatumRef::Missing, None, 0);
        assert!(rejects(missing, "fast default left unresolved"));
        // Text no int2 parses reads as NULL
        encode_ref(&mut n, int2, DatumRef::Text(b"70000"), None, 0).unwrap();
        assert_eq!(
            n.wire(),
            Wire::NullableFixed {
                width: 2,
                null_map: &[1],
                inner: &[0, 0],
            }
        );
    }

    #[test]
    fn json_buffers_keep_absent_cell_through_spill() {
        for ty in ["JSON", "Nullable(JSON)"] {
            let mut b = buf(ty);
            b.append_default();
            let mut out = Vec::new();
            b.encode_into(&mut out);
            let back = ColumnBuf::decode(&mut crate::xact::spill::Cursor::new(&out)).unwrap();
            assert_eq!(back.wire(), b.wire(), "{ty}");
        }
    }

    #[test]
    #[should_panic(expected = "chunk shape checked before merge")]
    fn extend_across_shapes_panics() {
        buf("Int32").extend(buf("String"));
    }

    #[test]
    fn oracle_column_defaults_and_debug_shape() {
        let ast = TypeAst::parse("Array(Int32)", Allocator::global(&mimalloc::MiMalloc)).unwrap();
        let mut oracle = ColumnBuf::Oracle(OracleColumnBuf::new(1007, -1, ast.view()));
        oracle.append_default();
        let shape = format!("{oracle:?}");
        assert!(
            shape.starts_with("Oracle { rows: 1, source_oid: 1007,"),
            "{shape}"
        );
    }

    #[test]
    fn oracle_cell_rejects_values_off_the_planned_type() {
        let source = |type_oid| {
            Some(SourceColumn {
                index: 0,
                codec: Codec::Pending,
                type_oid,
            })
        };
        let rejects = |v: DatumRef<'_>, type_oid, want: &str| matches!(oracle_cell(v, source(type_oid), 1007), Err(EmitterError::UnsupportedValue { kind, .. }) if kind == want);
        let moved = "source type oid differs from the planned one";
        assert!(rejects(DatumRef::Varlena(&[0; 4]), 23, moved));
        assert!(rejects(DatumRef::Text(b"{1}"), 23, moved));
        assert!(rejects(
            DatumRef::Fixed(&1i32.to_le_bytes()),
            0,
            "decoded value on an oracle column"
        ));
        assert!(
            matches!(oracle_cell(DatumRef::Text(b"{1}"), source(1007), 1007), Ok(OracleCell::TextInput(t)) if t == b"{1}")
        );
        assert!(rejects(
            DatumRef::Missing,
            1007,
            "fast default left unresolved"
        ));
        let name = Some(SourceColumn {
            index: 0,
            codec: Codec::Name,
            type_oid: crate::schema::NAMEOID,
        });
        assert_eq!(
            oracle_cell(DatumRef::Fixed(b"ab\0\0"), name, crate::schema::NAMEOID).unwrap(),
            OracleCell::Literal(b"ab".to_vec())
        );
    }

    fn non_finite(ty: &str, nan: &str, pos_inf: &str, neg_inf: &str) -> Result<NonFinite, String> {
        let ast = TypeAst::parse(ty, Allocator::global(&mimalloc::MiMalloc)).unwrap();
        let rule = ColumnRule {
            nan: Some(nan.parse()?),
            pos_inf: Some(pos_inf.parse()?),
            neg_inf: Some(neg_inf.parse()?),
            ..ColumnRule::default()
        };
        NonFinite::compile(&rule, &ast, true)
    }

    /// Substitute `text` encoded for Decimal type `ty`
    fn finite(ty: &str, text: &str) -> Option<Fill> {
        let ast = TypeAst::parse(ty, Allocator::global(&mimalloc::MiMalloc)).unwrap();
        let wire = decimal_wire_of(&ast).unwrap();
        let le = decimal_text_to_scaled_le(text, wire.scale, wire.width).unwrap();
        Some(Fill::Le(SmallVec::from_slice(&le[..wire.width.bytes()])))
    }

    #[test]
    fn temporal_infinity_policies_validate_destination() {
        let alloc = Allocator::global(&mimalloc::MiMalloc);
        for ty in ["Date32", "DateTime64(3, 'UTC')"] {
            let ast = TypeAst::parse(ty, alloc).unwrap();
            for action in ["null", "min", "max", "0"] {
                let rule = ColumnRule {
                    pos_inf: Some(action.parse().unwrap()),
                    ..ColumnRule::default()
                };
                assert!(NonFinite::compile(&rule, &ast, true).is_err());
            }
            let nullable = TypeAst::parse(&format!("Nullable({ty})"), alloc).unwrap();
            let nf = NonFinite::compile(
                &ColumnRule {
                    pos_inf: Some(Substitute::Null),
                    neg_inf: Some(Substitute::Reject),
                    ..ColumnRule::default()
                },
                &nullable,
                true,
            )
            .unwrap();
            assert_eq!(nf.pos_inf, Some(Fill::Null));
            assert_eq!(nf.neg_inf, None);
            for (codec, body) in [
                (Codec::Date, (i32::MIN + 1).to_le_bytes().to_vec()),
                (Codec::Date, (i32::MAX - 1).to_le_bytes().to_vec()),
                (Codec::Timestamp, (i64::MIN + 1).to_le_bytes().to_vec()),
                (Codec::Timestamp, (i64::MAX - 1).to_le_bytes().to_vec()),
            ] {
                assert_eq!(nf.fill(codec, &body), None);
            }
            assert_eq!(
                nf.fill(Codec::Date, &i32::MAX.to_le_bytes()),
                Some(&Fill::Null)
            );
        }
    }

    #[test]
    fn non_finite_substitutes_resolve_against_destination() {
        let zero_tail = format!("1.{}", "0".repeat(80));
        let nf = non_finite("Decimal(10, 2)", "0", "1.230", "reject").unwrap();
        assert_eq!(nf.nan, finite("Decimal(10, 2)", "0"));
        assert_eq!(nf.pos_inf, finite("Decimal(10, 2)", "1.230"));
        assert_eq!(nf.neg_inf, None);

        let nf = non_finite("Nullable(Decimal(10, 2))", "null", "reject", "reject").unwrap();
        assert_eq!(nf.nan, Some(Fill::Null));

        for (ty, nan) in [
            ("Decimal(10, 2)", "null"),
            ("Decimal(10, 2)", "1.234"),
            ("Decimal(10, 2)", "123456789.00"),
            ("Decimal(10, 2)", "NaN"),
            ("Decimal(10, 2)", &zero_tail),
        ] {
            assert!(
                non_finite(ty, nan, "reject", "reject").is_err(),
                "{ty} nan = {nan:?}"
            );
        }
    }

    #[test]
    fn bounds_apply_independently_of_non_finite_reason() {
        let wide = format!("{}.9999", "9".repeat(34));
        for (ty, bound) in [
            ("Decimal(10, 2)", "99999999.99"),
            ("Decimal(38, 4)", &wide),
            ("Decimal(2, 2)", ".99"),
            ("Nullable(Decimal(2, 0))", "99."),
        ] {
            for (action, literal) in [("min", format!("-{bound}")), ("max", bound.into())] {
                let nf = non_finite(ty, action, action, action).unwrap();
                let expected = finite(ty, &literal);
                assert_eq!(nf.nan, expected);
                assert_eq!(nf.pos_inf, expected);
                assert_eq!(nf.neg_inf, expected);
            }
        }
    }

    #[test]
    fn override_wire_admissibility() {
        let alloc = Allocator::global(&mimalloc::MiMalloc);
        let p = |s: &str| TypeAst::parse(s, alloc).unwrap();
        assert!(override_wire(&p("Date32"), &p("Int32")).is_none());
        assert!(override_wire(&p("Int64"), &p("DateTime64(6)")).is_none());
        assert!(override_wire(&p("DateTime64(6)"), &p("Nullable(DateTime64(3))")).is_some());
        assert!(override_wire(&p("Time64(6)"), &p("Time64(3)")).is_some());
        assert!(override_wire(&p("Time64(6)"), &p("Int64")).is_none());
        let vector = p("Array(Float32)");
        assert!(override_wire(&vector, &p("QBit(Float32, 3)")).is_some());
        assert!(override_wire(&vector, &p("Array(BFloat16)")).is_some());
        // Decimal-encoded source: Decimal / String / signed ints convert
        let dec = p("Decimal(38, 0)");
        assert!(override_wire(&dec, &p("Decimal(38, 2)")).is_some());
        assert!(override_wire(&dec, &p("String")).is_some());
        assert_eq!(
            override_wire(&dec, &p("Int128")),
            Some(Some(DecimalWire {
                scale: 0,
                width: DecimalWidth::D128
            }))
        );
        // Unsigned wraps negatives, floats reinterpret bits: rejected
        assert!(override_wire(&dec, &p("UInt128")).is_none());
        assert!(override_wire(&dec, &p("Float32")).is_none());
        for (ty, width) in [
            ("Int32", DecimalWidth::D32),
            ("Int64", DecimalWidth::D64),
            ("Int256", DecimalWidth::D256),
        ] {
            let wire = DecimalWire { scale: 0, width };
            assert_eq!(override_wire(&dec, &p(ty)), Some(Some(wire)), "{ty}");
        }
        // Fixed-width source: same-width reinterpretation only, no Decimal
        // (a nonzero scale would silently rescale)
        let i = p("Int32");
        assert!(override_wire(&i, &p("UInt32")).is_some());
        assert!(override_wire(&i, &p("Int64")).is_none());
        assert!(override_wire(&i, &p("Decimal32(2)")).is_none());
        // String-shaped source: string-shaped override only
        let s = p("Nullable(String)");
        assert!(override_wire(&s, &p("String")).is_some());
        assert!(override_wire(&s, &p("Int64")).is_none());
    }
}
