//! Backfill tuples from backup pages or COPY, and the COPY row codec

use walrus::pg::walparser::RelFileNode;

use crate::decode::datum::{Datum, DatumRef};
#[cfg(any(test, feature = "test-support"))]
use crate::decode::heap_decoder::decode_block_data;
use crate::decode::heap_decoder::{DecodeError, HEAP_HASEXTERNAL, TupleView, view_block_data};
use crate::decode::visibility::PendingXids;
use crate::schema::{
    BOOLOID, BPCHAROID, BYTEAOID, CHAROID, CIDOID, DATEOID, FLOAT4OID, FLOAT8OID, INT2OID, INT4OID,
    INT8OID, JSONOID, MONEYOID, NAMEOID, NUMERICOID, OID8OID, OIDOID, PG_LSNOID, RelDescriptor,
    TEXTOID, TIMEOID, TIMESTAMPOID, TIMESTAMPTZOID, UUIDOID, VARCHAROID, XID8OID, XIDOID,
};

/// One tuple from a backup page or COPY; a synthetic INSERT at
/// `_lsn = start_lsn`. Carries row bytes, decoded only at encode
#[derive(Debug, Clone)]
pub struct BackfillTuple {
    pub rfn: RelFileNode,
    /// `t_xmin`; sequencing not load-bearing, every backfill row shares
    /// the same `_lsn = start_lsn`
    pub xid: u32,
    /// `t_xmax`, read with `infomask` by bootstrap visibility gate
    pub xmax: u32,
    /// `t_infomask` hint bits
    pub infomask: u16,
    pub source_lsn: u64,
    /// Copied heap page's `pd_lsn`, or 0 without a page. TOAST writes precede
    /// heap inserts; TOAST deletes follow heap deletes, so reads here match heap state
    pub page_lsn: u64,
    /// On-page TID; toast tuples become store rows keyed on it
    /// ([`crate::toast::ToastRow`])
    pub blkno: u32,
    pub offnum: u16,
    pub data: TupleData,
    /// Xids an undecided row waits on, set once it enters a pending table
    pub pending: Option<PendingXids>,
}

/// Row bytes of a [`BackfillTuple`]
#[derive(Debug, Clone, PartialEq)]
pub enum TupleData {
    /// On-page tuple from `t_infomask2` on: the `block.data` shape heap
    /// decode reads
    Heap(Box<[u8]>),
    Copy(CopyRow),
}

impl TupleData {
    /// Heap image of `values`, see
    /// [`form_block_data`](crate::decode::heap_decoder::form_block_data)
    #[cfg(any(test, feature = "test-support"))]
    pub fn from_cells(values: &[Option<Datum>]) -> Self {
        Self::Heap(crate::decode::heap_decoder::form_block_data(&[], values).into())
    }

    fn heap(&self) -> Option<&[u8]> {
        if let Self::Heap(b) = self {
            Some(b)
        } else {
            None
        }
    }

    pub(crate) fn len(&self) -> usize {
        match self {
            Self::Heap(b) => b.len(),
            Self::Copy(r) => r.bytes().len(),
        }
    }
}

impl BackfillTuple {
    /// Whether the heap image's `t_infomask` flags an external TOAST pointer
    pub fn has_external(&self) -> bool {
        self.data
            .heap()
            .and_then(|b| b.get(2..4))
            .is_some_and(|m| u16::from_le_bytes([m[0], m[1]]) & HEAP_HASEXTERNAL != 0)
    }

    /// Tuple image in place; `None` for COPY rows
    pub fn view<'a>(&'a self, rel: &RelDescriptor) -> Result<Option<TupleView<'a>>, DecodeError> {
        self.data
            .heap()
            .map(|b| view_block_data(b, rel))
            .transpose()
    }

    /// Owned cells, attnum-1 indexed
    #[cfg(any(test, feature = "test-support"))]
    pub fn cells(&self, rel: &RelDescriptor) -> Result<Vec<Option<Datum>>, DecodeError> {
        match &self.data {
            TupleData::Heap(b) => decode_block_data(b, rel).map(|t| t.columns),
            TupleData::Copy(row) => Ok(copy_cells(row, rel)),
        }
    }
}

pub(crate) fn approx_bytes(value: &BackfillTuple) -> usize {
    std::mem::size_of::<BackfillTuple>() + value.data.len()
}

pub(crate) fn metadata_base(rel: &RelDescriptor) -> usize {
    rel.attributes
        .iter()
        .map(|a| a.attnum.max(0) as usize)
        .max()
        .unwrap_or(0)
}

/// Values a pending row adds past `rel`'s attributes, see
/// [`pending_table_mapping`]
pub(crate) fn metadata_patch(
    tuple: &BackfillTuple,
    rel: &RelDescriptor,
    out: &mut Vec<(usize, Datum)>,
) {
    let Some(pending) = tuple.pending else {
        return;
    };
    let base = metadata_base(rel);
    out.extend([
        (base, Datum::fixed(&pending.insert.to_le_bytes())),
        (base + 1, Datum::fixed(&pending.delete.to_le_bytes())),
        (base + 2, Datum::fixed(&tuple.infomask.to_le_bytes())),
    ]);
}

/// How one selected column decodes from the wire. Native kinds read `typsend`
/// output; `Cast` columns were cast to `text` in the SELECT; `Send` columns
/// keep `typsend` bytes for the oracle's `typreceive`
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WireKind {
    Bool,
    Char,
    Int2,
    Int4,
    Int8,
    Oid,
    Oid8,
    Float4,
    Float8,
    Date,
    Time,
    Timestamp,
    TimestampTz,
    Uuid,
    Bytea,
    Text,
    Name,
    Json,
    Numeric,
    Cast,
    Send,
}

pub(crate) fn wire_kind(type_oid: u32) -> Option<WireKind> {
    Some(match type_oid {
        BOOLOID => WireKind::Bool,
        CHAROID => WireKind::Char,
        INT2OID => WireKind::Int2,
        INT4OID => WireKind::Int4,
        INT8OID | MONEYOID => WireKind::Int8,
        OIDOID | XIDOID | CIDOID => WireKind::Oid,
        OID8OID | XID8OID | PG_LSNOID => WireKind::Oid8,
        FLOAT4OID => WireKind::Float4,
        FLOAT8OID => WireKind::Float8,
        DATEOID => WireKind::Date,
        TIMEOID => WireKind::Time,
        TIMESTAMPOID => WireKind::Timestamp,
        TIMESTAMPTZOID => WireKind::TimestampTz,
        UUIDOID => WireKind::Uuid,
        BYTEAOID => WireKind::Bytea,
        TEXTOID | VARCHAROID | BPCHAROID => WireKind::Text,
        NAMEOID => WireKind::Name,
        JSONOID => WireKind::Json,
        NUMERICOID => WireKind::Numeric,
        _ => return None,
    })
}

/// One binary COPY row: per attribute, `typsend` bytes or NULL. Laid out
/// `[natts: u16][end: u32 per attribute][field bytes]`, an end's high bit
/// marking NULL and the next marking a field kept in `typsend` form
#[derive(Clone, Debug, PartialEq)]
pub struct CopyRow(Box<[u8]>);

const COPY_NULL: u32 = 1 << 31;

const COPY_SEND: u32 = 1 << 30;

/// PostgreSQL builds each COPY row in one palloc, so 1 GiB bounds it
const COPY_END: u32 = COPY_SEND - 1;

impl CopyRow {
    /// `fields` indexed by attnum - 1
    pub fn new(fields: &[Option<&[u8]>]) -> Self {
        let head = 2 + 4 * fields.len();
        let body = fields.iter().flatten().map(|f| f.len()).sum::<usize>();
        let mut out = Vec::with_capacity(head + body);
        out.extend_from_slice(&(fields.len() as u16).to_le_bytes());
        let mut end = 0u32;
        for f in fields {
            end += f.map_or(0, |f| f.len() as u32);
            let mark = if f.is_none() { COPY_NULL } else { 0 };
            out.extend_from_slice(&(end | mark).to_le_bytes());
        }
        for f in fields.iter().flatten() {
            out.extend_from_slice(f);
        }
        Self(out.into())
    }

    /// Row of `typsend` fields indexed by attnum - 1, each converted to its
    /// on-disk form per `kinds`
    pub(crate) fn from_send(kinds: &[WireKind], fields: &[Option<&[u8]>]) -> Result<Self, String> {
        let head = 2 + 4 * fields.len();
        let body = fields.iter().flatten().map(|f| f.len()).sum::<usize>();
        let mut out = Vec::with_capacity(head + body);
        out.extend_from_slice(&(fields.len() as u16).to_le_bytes());
        out.resize(head, 0);
        for (i, f) in fields.iter().enumerate() {
            let kind = kinds.get(i).copied().unwrap_or(WireKind::Cast);
            if let Some(raw) = f {
                store_field(kind, raw, &mut out)?;
            }
            let end = u32::try_from(out.len() - head)
                .ok()
                .filter(|&end| end <= COPY_END)
                .ok_or("COPY row past 1 GiB")?;
            let mark = match (f, kind) {
                (None, _) => COPY_NULL,
                (Some(_), WireKind::Send) => COPY_SEND,
                _ => 0,
            };
            out[2 + 4 * i..][..4].copy_from_slice(&(end | mark).to_le_bytes());
        }
        Ok(Self(out.into()))
    }

    /// Spooled bytes, see [`Self::from_bytes`]
    pub fn bytes(&self) -> &[u8] {
        &self.0
    }

    /// Row [`Self::bytes`] wrote; `None` when the header overruns them
    pub fn from_bytes(bytes: Box<[u8]>) -> Option<Self> {
        let row = Self(bytes);
        let natts = row.natts()?;
        let last = natts.checked_sub(1).map_or(Some(0), |i| row.end(i))?;
        (row.0.len() == 2 + 4 * natts + last as usize).then_some(row)
    }

    fn natts(&self) -> Option<usize> {
        Some(u16::from_le_bytes(self.0.get(..2)?.try_into().ok()?) as usize)
    }

    fn end(&self, i: usize) -> Option<u32> {
        let at = 2 + 4 * i;
        let word = u32::from_le_bytes(self.0.get(at..at + 4)?.try_into().ok()?);
        Some(word & COPY_END)
    }

    /// Field of attribute index `i`; `None` past the row or NULL
    pub fn field(&self, i: usize) -> Option<&[u8]> {
        self.slot(i).map(|(raw, _)| raw)
    }

    /// Field of attribute index `i` and whether it kept `typsend` form
    fn slot(&self, i: usize) -> Option<(&[u8], bool)> {
        let natts = self.natts()?;
        if i >= natts {
            return None;
        }
        let at = 2 + 4 * i;
        let word = u32::from_le_bytes(self.0[at..at + 4].try_into().ok()?);
        if word & COPY_NULL != 0 {
            return None;
        }
        let start = if i == 0 { 0 } else { self.end(i - 1)? };
        let base = 2 + 4 * natts;
        let raw = self
            .0
            .get(base + start as usize..base + (word & COPY_END) as usize)?;
        Some((raw, word & COPY_SEND != 0))
    }
}

/// Attribute index `i` of a row [`CopyRow::from_send`] stored; NULL past the row
pub(crate) fn copy_slot<'a>(row: &'a CopyRow, rel: &RelDescriptor, i: usize) -> DatumRef<'a> {
    let (Some(att), Some((raw, send))) = (rel.attributes.get(i), row.slot(i)) else {
        return DatumRef::Null;
    };
    if send {
        return DatumRef::Send(raw);
    }
    match wire_kind(att.type_oid) {
        Some(WireKind::Bytea | WireKind::Text | WireKind::Json | WireKind::Numeric) => {
            DatumRef::Varlena(raw)
        }
        Some(_) => DatumRef::Fixed(raw),
        None => DatumRef::Text(raw),
    }
}

/// Owned datums of a COPY row, attnum-1 indexed
#[cfg(any(test, feature = "test-support"))]
pub(crate) fn copy_cells(
    row: &CopyRow,
    rel: &RelDescriptor,
) -> Vec<Option<crate::decode::datum::Datum>> {
    (0..rel.attributes.len())
        .map(|i| crate::decode::heap_decoder::resolve(&rel.attributes[i], copy_slot(row, rel, i)))
        .collect()
}

/// Append non-NULL wire field `raw` in its on-disk form
pub(crate) fn store_field(kind: WireKind, raw: &[u8], out: &mut Vec<u8>) -> Result<(), String> {
    let swapped = |out: &mut Vec<u8>, width: usize, name: &str| {
        if raw.len() != width {
            return Err(format!("{name}: want {width} bytes, got {}", raw.len()));
        }
        out.extend(raw.iter().rev());
        Ok(())
    };
    match kind {
        WireKind::Bool => swapped(out, 1, "bool"),
        WireKind::Char => swapped(out, 1, "char"),
        WireKind::Int2 => swapped(out, 2, "int2"),
        WireKind::Int4 => swapped(out, 4, "int4"),
        WireKind::Oid => swapped(out, 4, "oid"),
        WireKind::Float4 => swapped(out, 4, "float4"),
        WireKind::Date => swapped(out, 4, "date"),
        WireKind::Int8 => swapped(out, 8, "int8"),
        WireKind::Oid8 => swapped(out, 8, "oid8"),
        WireKind::Float8 => swapped(out, 8, "float8"),
        WireKind::Time => swapped(out, 8, "time"),
        WireKind::Timestamp => swapped(out, 8, "timestamp"),
        WireKind::TimestampTz => swapped(out, 8, "timestamptz"),
        // Network byte order on disk too
        WireKind::Uuid if raw.len() == 16 => {
            out.extend_from_slice(raw);
            Ok(())
        }
        WireKind::Uuid => Err(format!("uuid: want 16 bytes, got {}", raw.len())),
        WireKind::Name if raw.len() < NAMEDATALEN => {
            out.extend_from_slice(raw);
            out.resize(out.len() + NAMEDATALEN - raw.len(), 0);
            Ok(())
        }
        WireKind::Name => Err(format!("name: {} bytes reach NAMEDATALEN", raw.len())),
        WireKind::Numeric => {
            out.extend_from_slice(&numeric_from_send(raw)?);
            Ok(())
        }
        WireKind::Bytea | WireKind::Text | WireKind::Json | WireKind::Cast | WireKind::Send => {
            out.extend_from_slice(raw);
            Ok(())
        }
    }
}

/// PostgreSQL `NAMEDATALEN`, `name`'s stored width
const NAMEDATALEN: usize = 64;

/// `numeric_send` (`ndigits`, `weight`, `sign`, `dscale`, digits, all
/// big-endian 16-bit) to the stored body
fn numeric_from_send(raw: &[u8]) -> Result<Vec<u8>, String> {
    use crate::decode::codecs::{Special, numeric_body, numeric_special_body};
    let word = |i: usize| {
        raw.get(2 * i..2 * i + 2)
            .map(|b| u16::from_be_bytes([b[0], b[1]]))
            .ok_or_else(|| format!("numeric: {} bytes truncate word {i}", raw.len()))
    };
    let ndigits = usize::from(word(0)?);
    let weight = i32::from(word(1)? as i16);
    let sign = word(2)?;
    let dscale = word(3)?;
    if raw.len() != 8 + 2 * ndigits {
        return Err(format!(
            "numeric: {ndigits} digits need {} bytes, got {}",
            8 + 2 * ndigits,
            raw.len()
        ));
    }
    let special = |s| Ok(numeric_special_body(s));
    match sign {
        0xC000 => special(Special::NaN),
        0xD000 => special(Special::PInf),
        0xF000 => special(Special::NInf),
        0x0000 | 0x4000 => {
            let digits: Vec<i16> = raw[8..]
                .as_chunks::<2>()
                .0
                .iter()
                .map(|&b| i16::from_be_bytes(b))
                .collect();
            Ok(numeric_body(sign == 0x4000, weight, dscale, &digits))
        }
        other => Err(format!("numeric: sign word {other:#06x}")),
    }
}

/// On-disk `attmissingval` from its `anyarray_send` form, `None` for types
/// outside the wire matrix or an array other than one non-null element
pub(crate) fn missing_from_send(type_oid: u32, send: &[u8]) -> Option<Vec<u8>> {
    let kind = wire_kind(type_oid)?;
    // array_send: ndim, hasnull, elemtype, (dim, lbound) per dim, (len, bytes) per element
    let word = |i: usize| {
        Some(i32::from_be_bytes(
            send.get(4 * i..4 * i + 4)?.try_into().ok()?,
        ))
    };
    if word(0)? != 1 || word(3)? != 1 {
        return None;
    }
    let len = usize::try_from(word(5)?).ok()?;
    let raw = send.get(24..)?;
    if raw.len() != len {
        return None;
    }
    let mut elem = Vec::new();
    store_field(kind, raw, &mut elem).ok()?;
    if matches!(
        kind,
        WireKind::Bytea | WireKind::Text | WireKind::Json | WireKind::Numeric
    ) {
        // 4-byte uncompressed varlena header, VARSIZE in upper 30 bits
        let size = u32::try_from(elem.len() + 4).ok()?;
        elem.splice(0..0, (size << 2).to_le_bytes());
    }
    Some(crate::decode::heap_decoder::raw_missing_array(
        type_oid, &elem,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_from_send_matches_stored_default() {
        use crate::decode::datum::{Datum, build};
        use crate::decode::heap_decoder::resolve;
        use crate::schema::{MissingDefault, RelAttr};
        let hex = |s: &str| {
            (0..s.len())
                .step_by(2)
                .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
                .collect::<Vec<u8>>()
        };
        // anyarray_send(attmissingval) from PG 17 after ALTER TABLE ADD ... DEFAULT
        let cases = [
            (
                NUMERICOID,
                -1,
                "0000000100000000000006a400000001000000010000000c0002000000000002000c1388",
                build::numeric("12.50"),
            ),
            (
                TEXTOID,
                -1,
                "0000000100000000000000190000000100000001000000026869",
                Datum::Varlena(b"hi".to_vec()),
            ),
            (
                NAMEOID,
                64,
                "0000000100000000000000130000000100000001000000026e6d",
                build::name("nm"),
            ),
            (
                INT8OID,
                8,
                "000000010000000000000014000000010000000100000008fffffffffffffffd",
                Datum::fixed(&(-3i64).to_le_bytes()),
            ),
        ];
        for (type_oid, type_len, send, want) in cases {
            let att = RelAttr {
                attnum: 1,
                name: "c".into(),
                type_oid,
                typmod: -1,
                not_null: false,
                dropped: false,
                type_name: String::new(),
                type_byval: true,
                type_len,
                type_align: b'd',
                type_storage: b'p',
                missing_default: missing_from_send(type_oid, &hex(send)).map(MissingDefault::Raw),
            };
            assert_eq!(
                resolve(&att, DatumRef::Missing),
                Some(want),
                "type {type_oid}"
            );
        }
        const TSVECTOROID: u32 = 3614;
        let tsvector = hex("000000010000000000000e1e0000000100000001000000080000000178000000");
        assert_eq!(missing_from_send(TSVECTOROID, &tsvector), None);
        // Two elements, then a length word disagreeing with the bytes
        let text = hex("0000000100000000000000190000000100000001000000026869");
        let mut two = text.clone();
        two[15] = 2;
        assert_eq!(missing_from_send(TEXTOID, &two), None);
        assert_eq!(missing_from_send(TEXTOID, &text[..text.len() - 1]), None);
    }

    #[test]
    fn send_fields_store_as_wal_datums() {
        use crate::decode::datum::{Datum, build};
        let store = |kind, raw: &[u8]| {
            let mut out = Vec::new();
            store_field(kind, raw, &mut out).map(|()| out)
        };
        assert_eq!(
            store(WireKind::Int8, &7i64.to_be_bytes()).unwrap(),
            7i64.to_le_bytes()
        );
        assert_eq!(store(WireKind::Bool, &[1]).unwrap(), [1]);
        assert_eq!(
            store(WireKind::Date, &8000i32.to_be_bytes()).unwrap(),
            8000i32.to_le_bytes()
        );
        assert_eq!(store(WireKind::Text, b"abc").unwrap(), b"abc");
        assert_eq!(
            Datum::fixed(&store(WireKind::Name, b"orders").unwrap()),
            build::name("orders")
        );
        // numeric_send of 12.50: 2 digits, weight 0, positive, dscale 2
        let send: Vec<u8> = [2u16, 0, 0, 2, 12, 5000]
            .iter()
            .flat_map(|w| w.to_be_bytes())
            .collect();
        assert_eq!(
            Datum::Varlena(store(WireKind::Numeric, &send).unwrap()),
            build::numeric("12.50")
        );
        for (sign, text) in [
            (0xC000u16, "NaN"),
            (0xD000, "Infinity"),
            (0xF000, "-Infinity"),
        ] {
            let send: Vec<u8> = [0u16, 0, sign, 0]
                .iter()
                .flat_map(|w| w.to_be_bytes())
                .collect();
            assert_eq!(
                Datum::Varlena(store(WireKind::Numeric, &send).unwrap()),
                build::numeric(text),
                "{text}"
            );
        }
        // Wrong width is an error, not a silent misread
        assert!(store(WireKind::Int4, &[0, 1]).is_err());
        assert!(store(WireKind::Numeric, &[0, 1, 0]).is_err());
        // Cast columns store text output verbatim, invalid UTF-8 included
        assert_eq!(store(WireKind::Cast, &[0xFF]).unwrap(), [0xFF]);
        assert!(store(WireKind::Numeric, &[0, 1, 0, 0, 0, 0, 0, 0]).is_err());
        assert!(store(WireKind::Numeric, &[0, 0, 0, 0, 0x12, 0x34, 0, 0]).is_err());
        use WireKind::*;
        for (kind, width) in [
            (Char, 1),
            (Int2, 2),
            (Oid, 4),
            (Float4, 4),
            (Oid8, 8),
            (Time, 8),
        ] {
            let raw: Vec<u8> = (1..=width).collect();
            let want: Vec<u8> = raw.iter().rev().copied().collect();
            assert_eq!(store(kind, &raw).unwrap(), want, "{kind:?}");
            assert!(store(kind, &raw[1..]).is_err(), "{kind:?}");
        }
        let uuid: Vec<u8> = (0..16).collect();
        assert_eq!(store(Uuid, &uuid).unwrap(), uuid);
        assert!(store(Uuid, &uuid[1..]).is_err());
        assert!(store(Name, &[b'n'; NAMEDATALEN]).is_err());
    }

    /// PostgreSQL binary COPY uses each type's `typsend` format
    #[test]
    fn every_native_kind_stores_its_send_bytes() {
        let store = |kind, raw: &[u8]| {
            let mut out = Vec::new();
            store_field(kind, raw, &mut out).map(|()| out)
        };
        let uuid = *b"0123456789abcdef";
        let name = [b"rel".as_slice(), &[0; NAMEDATALEN - 3]].concat();
        for (oid, kind, raw, want) in [
            (BOOLOID, WireKind::Bool, vec![0], vec![0]),
            (CHAROID, WireKind::Char, vec![b'x'], vec![b'x']),
            (
                INT2OID,
                WireKind::Int2,
                (-2i16).to_be_bytes().to_vec(),
                (-2i16).to_le_bytes().to_vec(),
            ),
            (
                INT4OID,
                WireKind::Int4,
                5i32.to_be_bytes().to_vec(),
                5i32.to_le_bytes().to_vec(),
            ),
            (
                MONEYOID,
                WireKind::Int8,
                1234i64.to_be_bytes().to_vec(),
                1234i64.to_le_bytes().to_vec(),
            ),
            (
                XIDOID,
                WireKind::Oid,
                77u32.to_be_bytes().to_vec(),
                77u32.to_le_bytes().to_vec(),
            ),
            (
                PG_LSNOID,
                WireKind::Oid8,
                (1u64 << 40).to_be_bytes().to_vec(),
                (1u64 << 40).to_le_bytes().to_vec(),
            ),
            (
                FLOAT4OID,
                WireKind::Float4,
                1.5f32.to_be_bytes().to_vec(),
                1.5f32.to_le_bytes().to_vec(),
            ),
            (
                FLOAT8OID,
                WireKind::Float8,
                (-0.25f64).to_be_bytes().to_vec(),
                (-0.25f64).to_le_bytes().to_vec(),
            ),
            (
                DATEOID,
                WireKind::Date,
                (-1i32).to_be_bytes().to_vec(),
                (-1i32).to_le_bytes().to_vec(),
            ),
            (
                TIMEOID,
                WireKind::Time,
                3_600_000_000i64.to_be_bytes().to_vec(),
                3_600_000_000i64.to_le_bytes().to_vec(),
            ),
            (
                TIMESTAMPOID,
                WireKind::Timestamp,
                9i64.to_be_bytes().to_vec(),
                9i64.to_le_bytes().to_vec(),
            ),
            (
                TIMESTAMPTZOID,
                WireKind::TimestampTz,
                10i64.to_be_bytes().to_vec(),
                10i64.to_le_bytes().to_vec(),
            ),
            (UUIDOID, WireKind::Uuid, uuid.to_vec(), uuid.to_vec()),
            (BYTEAOID, WireKind::Bytea, vec![0, 0xFF], vec![0, 0xFF]),
            (BPCHAROID, WireKind::Text, b"ab ".to_vec(), b"ab ".to_vec()),
            (NAMEOID, WireKind::Name, b"rel".to_vec(), name),
            (JSONOID, WireKind::Json, b"[]".to_vec(), b"[]".to_vec()),
        ] {
            assert_eq!(wire_kind(oid), Some(kind), "oid {oid}");
            assert_eq!(store(kind, &raw).unwrap(), want, "oid {oid}");
            let variable = matches!(
                kind,
                WireKind::Bytea | WireKind::Text | WireKind::Name | WireKind::Json
            );
            assert_eq!(
                store(kind, &[0; 17]).is_ok(),
                variable,
                "oid {oid}: only fixed-width kinds reject a wrong width"
            );
        }
        for oid in [OIDOID, CIDOID] {
            assert_eq!(wire_kind(oid), Some(WireKind::Oid));
        }
        for oid in [OID8OID, XID8OID] {
            assert_eq!(wire_kind(oid), Some(WireKind::Oid8));
        }
        assert_eq!(wire_kind(NUMERICOID), Some(WireKind::Numeric));
    }
}
