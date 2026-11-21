//! Built-in arrays: stored `ArrayType` bodies read in place, and `array_send`
//! bytes rebuilt into stored form

use super::CodecError;
use crate::schema::{
    BOOLOID, BPCHAROID, FLOAT4OID, FLOAT8OID, INT2OID, INT4OID, INT8OID, NAMEOID, OIDOID,
    TEXTARRAYOID, TEXTOID, UUIDOID, VARCHAROID,
};

/// PostgreSQL `MAXDIM`
const MAXDIM: usize = 6;
/// Varlena length word callers strip; array offsets still count it
const VARHDRSZ: usize = 4;
/// `sizeof(ArrayType)`, length word included
const ARRAY_HEADER: usize = 16;
/// `MAXALIGN` on 64-bit hosts
const MAXALIGN: usize = 8;

/// Element storage of a built-in array type, from PostgreSQL
/// `src/include/catalog/pg_type.dat`
#[derive(Debug, PartialEq, Eq)]
pub struct ArrayElem {
    pub array_oid: u32,
    pub elem_oid: u32,
    /// `typlen`, -1 for varlena
    pub len: i16,
    pub byval: bool,
    /// `typalign` in bytes
    pub align: usize,
}

const fn elem(array_oid: u32, elem_oid: u32, len: i16, byval: bool, align: usize) -> ArrayElem {
    ArrayElem {
        array_oid,
        elem_oid,
        len,
        byval,
        align,
    }
}

/// Arrays whose elements decode without PostgreSQL
pub static ARRAY_ELEMS: [ArrayElem; 12] = [
    elem(1000, BOOLOID, 1, true, 1),
    elem(1003, NAMEOID, 64, false, 1),
    elem(1005, INT2OID, 2, true, 2),
    elem(1007, INT4OID, 4, true, 4),
    elem(TEXTARRAYOID, TEXTOID, -1, false, 4),
    elem(1014, BPCHAROID, -1, false, 4),
    elem(1015, VARCHAROID, -1, false, 4),
    elem(1016, INT8OID, 8, true, 8),
    elem(1021, FLOAT4OID, 4, true, 4),
    elem(1022, FLOAT8OID, 8, true, 8),
    elem(1028, OIDOID, 4, true, 4),
    elem(2951, UUIDOID, 16, false, 1),
];

/// Element of array type `array_oid` when local decoding covers it
pub fn array_elem(array_oid: u32) -> Option<&'static ArrayElem> {
    ARRAY_ELEMS.iter().find(|e| e.array_oid == array_oid)
}

fn truncated(body: &[u8], offset: usize, need: usize) -> CodecError {
    CodecError::Truncated {
        offset,
        need,
        have: body.len().saturating_sub(offset),
    }
}

fn le_word(body: &[u8], at: usize) -> Result<i32, CodecError> {
    body.get(at..at + 4)
        .map(|b| i32::from_le_bytes(b.try_into().expect("4 bytes")))
        .ok_or_else(|| truncated(body, at, 4))
}

/// Body offset `at` advanced to `align`, counted from stripped length word
fn align_body(at: usize, align: usize) -> usize {
    (at + VARHDRSZ).next_multiple_of(align) - VARHDRSZ
}

/// Stored array body, varlena length word stripped, header checked against
/// its element type
#[derive(Debug)]
pub struct ArrayBody<'a> {
    body: &'a [u8],
    elem: &'static ArrayElem,
    ndim: usize,
    nitems: usize,
    /// Bit set per present element
    nulls: Option<&'a [u8]>,
    data: usize,
}

impl<'a> ArrayBody<'a> {
    pub fn parse(body: &'a [u8], elem: &'static ArrayElem) -> Result<Self, CodecError> {
        let ndim = usize::try_from(le_word(body, 0)?)
            .ok()
            .filter(|&n| n <= MAXDIM)
            .ok_or(CodecError::BadArray("dimension count"))?;
        let dataoffset = le_word(body, 4)?;
        let elemtype = le_word(body, 8)? as u32;
        if elemtype != elem.elem_oid {
            return Err(CodecError::ArrayElemType {
                want: elem.elem_oid,
                got: elemtype,
            });
        }
        let mut nitems = usize::from(ndim > 0);
        for d in 0..ndim {
            let dim = usize::try_from(le_word(body, 12 + 4 * d)?)
                .map_err(|_| CodecError::BadArray("negative dimension"))?;
            // Lower bounds only shift subscripts, elements keep stored order
            le_word(body, 12 + 4 * (ndim + d))?;
            nitems = nitems
                .checked_mul(dim)
                .ok_or(CodecError::BadArray("element count"))?;
        }
        let bitmap = 12 + 8 * ndim;
        let (nulls, data) = if dataoffset == 0 {
            (None, bitmap)
        } else {
            let len = nitems.div_ceil(8);
            let data = align_body(bitmap + len, MAXALIGN);
            if usize::try_from(dataoffset).ok() != Some(data + VARHDRSZ) {
                return Err(CodecError::BadArray("data offset"));
            }
            let nulls = body
                .get(bitmap..bitmap + len)
                .ok_or_else(|| truncated(body, bitmap, len))?;
            (Some(nulls), data)
        };
        Ok(Self {
            body,
            elem,
            ndim,
            nitems,
            nulls,
            data,
        })
    }

    pub fn ndim(&self) -> usize {
        self.ndim
    }

    /// Element count across all dimensions
    pub fn len(&self) -> usize {
        self.nitems
    }

    pub fn is_empty(&self) -> bool {
        self.nitems == 0
    }

    pub fn has_nulls(&self) -> bool {
        self.nulls
            .is_some_and(|b| (0..self.nitems).any(|i| b[i / 8] & (1 << (i % 8)) == 0))
    }

    /// All elements as one slice when by-value and NULL-free. By-value
    /// widths equal their alignment, so such elements pack without padding
    pub fn packed(&self) -> Result<Option<&'a [u8]>, CodecError> {
        if !self.elem.byval || self.has_nulls() {
            return Ok(None);
        }
        let len = self.nitems.saturating_mul(self.elem.len as usize);
        self.body
            .get(self.data..self.data.saturating_add(len))
            .map(Some)
            .ok_or_else(|| truncated(self.body, self.data, len))
    }

    /// Elements in stored order, `None` for NULL. Fixed-width elements as
    /// stored, varlenas without their length word
    pub fn elements(&self) -> Elements<'a> {
        Elements {
            body: self.body,
            elem: self.elem,
            nulls: self.nulls,
            at: self.data,
            index: 0,
            nitems: self.nitems,
        }
    }
}

pub struct Elements<'a> {
    body: &'a [u8],
    elem: &'static ArrayElem,
    nulls: Option<&'a [u8]>,
    at: usize,
    index: usize,
    nitems: usize,
}

impl<'a> Elements<'a> {
    /// Element at aligned `at` and the bytes it spans
    fn read(&self, at: usize) -> Result<(&'a [u8], usize), CodecError> {
        let body = self.body;
        if let Ok(len) = usize::try_from(self.elem.len) {
            let value = body
                .get(at..at + len)
                .ok_or_else(|| truncated(body, at, len))?;
            return Ok((value, len));
        }
        let first = *body.get(at).ok_or_else(|| truncated(body, at, 1))?;
        let (header, total) = match first {
            0x01 => return Err(CodecError::BadArray("external element")),
            b if b & 1 == 1 => (1, usize::from(b >> 1)),
            _ => {
                let word = le_word(body, at)? as u32;
                if word & 0b10 != 0 {
                    return Err(CodecError::BadArray("compressed element"));
                }
                (VARHDRSZ, (word >> 2) as usize)
            }
        };
        let value = body
            .get(at + header..at + total)
            .ok_or_else(|| truncated(body, at, total))?;
        Ok((value, total))
    }
}

impl<'a> Iterator for Elements<'a> {
    type Item = Result<Option<&'a [u8]>, CodecError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.nitems {
            return None;
        }
        let i = self.index;
        self.index += 1;
        if self.nulls.is_some_and(|b| b[i / 8] & (1 << (i % 8)) == 0) {
            return Some(Ok(None));
        }
        // Nominal alignment, short varlena headers included
        let at = align_body(self.at, self.elem.align);
        Some(match self.read(at) {
            Ok((value, len)) => {
                self.at = at + len;
                Ok(Some(value))
            }
            Err(e) => {
                self.index = self.nitems;
                Err(e)
            }
        })
    }
}

/// Big-endian reader over `array_send` bytes
struct SendCursor<'a> {
    send: &'a [u8],
    at: usize,
}

impl<'a> SendCursor<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8], CodecError> {
        let out = self
            .send
            .get(self.at..self.at + n)
            .ok_or_else(|| truncated(self.send, self.at, n))?;
        self.at += n;
        Ok(out)
    }

    fn word(&mut self) -> Result<i32, CodecError> {
        Ok(i32::from_be_bytes(
            self.take(4)?.try_into().expect("4 bytes"),
        ))
    }

    /// Element bytes, `None` for NULL
    fn field(&mut self) -> Result<Option<&'a [u8]>, CodecError> {
        match usize::try_from(self.word()?) {
            Ok(len) => self.take(len).map(Some),
            Err(_) => Ok(None),
        }
    }
}

/// Append stored body of `array_send` bytes `send`, without length word, laid
/// out as `array_recv` stores it
pub fn array_from_send(
    send: &[u8],
    elem: &'static ArrayElem,
    out: &mut Vec<u8>,
) -> Result<(), CodecError> {
    let mut c = SendCursor { send, at: 0 };
    let ndim = usize::try_from(c.word()?)
        .ok()
        .filter(|&n| n <= MAXDIM)
        .ok_or(CodecError::BadArray("dimension count"))?;
    if !matches!(c.word()?, 0 | 1) {
        return Err(CodecError::BadArray("null flag"));
    }
    let elemtype = c.word()? as u32;
    if elemtype != elem.elem_oid {
        return Err(CodecError::ArrayElemType {
            want: elem.elem_oid,
            got: elemtype,
        });
    }
    let mut dims = [(0i32, 0i32); MAXDIM];
    let mut nitems = usize::from(ndim > 0);
    for dim in &mut dims[..ndim] {
        *dim = (c.word()?, c.word()?);
        let n = usize::try_from(dim.0).map_err(|_| CodecError::BadArray("negative dimension"))?;
        nitems = nitems
            .checked_mul(n)
            .ok_or(CodecError::BadArray("element count"))?;
    }
    let elements = c.at;
    let mut has_null = false;
    for _ in 0..nitems {
        has_null |= c.field()?.is_none();
    }
    if c.at != send.len() {
        return Err(CodecError::BadArray("trailing bytes"));
    }
    // array_recv stores a zero-element array dimensionless
    let ndim = if nitems == 0 { 0 } else { ndim };
    let bitmap_len = if has_null { nitems.div_ceil(8) } else { 0 };
    let data = (ARRAY_HEADER + 8 * ndim + bitmap_len).next_multiple_of(MAXALIGN);
    let base = out.len();
    out.extend_from_slice(&(ndim as i32).to_le_bytes());
    out.extend_from_slice(&(if has_null { data as i32 } else { 0 }).to_le_bytes());
    out.extend_from_slice(&elemtype.to_le_bytes());
    for (dim, _) in &dims[..ndim] {
        out.extend_from_slice(&dim.to_le_bytes());
    }
    for (_, lbound) in &dims[..ndim] {
        out.extend_from_slice(&lbound.to_le_bytes());
    }
    let bitmap = out.len();
    out.resize(base + data - VARHDRSZ, 0);
    c.at = elements;
    for i in 0..nitems {
        let Some(raw) = c.field()? else { continue };
        if has_null {
            out[bitmap + i / 8] |= 1 << (i % 8);
        }
        out.resize(base + align_body(out.len() - base, elem.align), 0);
        store_elem(elem, raw, out)?;
    }
    Ok(())
}

/// Element `typsend` bytes in stored form
fn store_elem(elem: &ArrayElem, raw: &[u8], out: &mut Vec<u8>) -> Result<(), CodecError> {
    let Ok(len) = usize::try_from(elem.len) else {
        out.extend_from_slice(&(((raw.len() + VARHDRSZ) as u32) << 2).to_le_bytes());
        out.extend_from_slice(raw);
        return Ok(());
    };
    // name_send omits the NUL padding out to NAMEDATALEN
    let fits = if elem.elem_oid == NAMEOID {
        raw.len() < len
    } else {
        raw.len() == len
    };
    if !fits {
        return Err(CodecError::BadArray("element width"));
    }
    if elem.byval {
        out.extend(raw.iter().rev());
    } else {
        out.extend_from_slice(raw);
        out.resize(out.len() + len - raw.len(), 0);
    }
    Ok(())
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    fn el(array_oid: u32) -> &'static ArrayElem {
        array_elem(array_oid).unwrap()
    }

    /// `array_send` bytes of `dims` as (length, lower bound) over `items`
    pub(crate) fn send(elem_oid: u32, dims: &[(i32, i32)], items: &[Option<&[u8]>]) -> Vec<u8> {
        let mut out = Vec::new();
        let has_null = items.iter().any(Option::is_none);
        for w in [dims.len() as i32, has_null.into(), elem_oid as i32] {
            out.extend_from_slice(&w.to_be_bytes());
        }
        for (dim, lbound) in dims {
            out.extend_from_slice(&dim.to_be_bytes());
            out.extend_from_slice(&lbound.to_be_bytes());
        }
        for item in items {
            match item {
                Some(b) => {
                    out.extend_from_slice(&(b.len() as i32).to_be_bytes());
                    out.extend_from_slice(b);
                }
                None => out.extend_from_slice(&(-1i32).to_be_bytes()),
            }
        }
        out
    }

    /// Stored body `array_recv` would build from `items`
    pub(crate) fn stored(array_oid: u32, dims: &[(i32, i32)], items: &[Option<&[u8]>]) -> Vec<u8> {
        let elem = el(array_oid);
        let mut out = Vec::new();
        array_from_send(&send(elem.elem_oid, dims, items), elem, &mut out).unwrap();
        out
    }

    fn collect(body: &[u8], elem: &'static ArrayElem) -> Result<Vec<Option<Vec<u8>>>, CodecError> {
        ArrayBody::parse(body, elem)?
            .elements()
            .map(|e| e.map(|e| e.map(<[u8]>::to_vec)))
            .collect()
    }

    fn words(ws: &[i32]) -> Vec<u8> {
        ws.iter().flat_map(|w| w.to_le_bytes()).collect()
    }

    #[test]
    fn layouts_follow_pg_type() {
        let want = [
            (1000, BOOLOID, 1, true, 1),
            (1003, NAMEOID, 64, false, 1),
            (1005, INT2OID, 2, true, 2),
            (1007, INT4OID, 4, true, 4),
            (1009, TEXTOID, -1, false, 4),
            (1014, BPCHAROID, -1, false, 4),
            (1015, VARCHAROID, -1, false, 4),
            (1016, INT8OID, 8, true, 8),
            (1021, FLOAT4OID, 4, true, 4),
            (1022, FLOAT8OID, 8, true, 8),
            (1028, OIDOID, 4, true, 4),
            (2951, UUIDOID, 16, false, 1),
        ];
        for (array, elem_oid, len, byval, align) in want {
            assert_eq!(el(array), &elem(array, elem_oid, len, byval, align));
        }
        assert_eq!(array_elem(INT4OID), None);
    }

    #[test]
    fn send_rebuilds_stored_layout() {
        let one = 1i32.to_be_bytes();
        let three = 3i32.to_be_bytes();
        // ndim, dataoffset, elemtype, dim, lbound, elements
        assert_eq!(
            stored(1007, &[(2, 1)], &[Some(&one), Some(&three)]),
            words(&[1, 0, 23, 2, 1, 1, 3])
        );
        // Bitmap after bounds, data at MAXALIGN(16 + 8 + 1) = 32
        let mut with_null = words(&[1, 32, 23, 3, 1]);
        with_null.extend_from_slice(&[0b101, 0, 0, 0, 0, 0, 0, 0]);
        with_null.extend_from_slice(&words(&[1, 3]));
        assert_eq!(
            stored(1007, &[(3, 1)], &[Some(&one), None, Some(&three)]),
            with_null
        );
        assert_eq!(stored(1007, &[], &[]), words(&[0, 0, 23]));
        assert_eq!(stored(1007, &[(0, 1)], &[]), words(&[0, 0, 23]));
        // Data at struct offset 24 is already 8-aligned for int8
        let mut wide = words(&[1, 0, 20, 1, 1]);
        wide.extend_from_slice(&(-2i64).to_le_bytes());
        assert_eq!(
            stored(1016, &[(1, 1)], &[Some(&(-2i64).to_be_bytes())]),
            wide
        );
        // Varlenas carry 4-byte headers, each aligned to 4
        let mut text = words(&[1, 0, 25, 2, 1]);
        text.extend_from_slice(&[20, 0, 0, 0, b'a', 0, 0, 0, 24, 0, 0, 0, b'b', b'c']);
        assert_eq!(stored(1009, &[(2, 1)], &[Some(b"a"), Some(b"bc")]), text);
        let mut name = words(&[1, 0, 19, 1, 1]);
        name.extend_from_slice(b"nm");
        name.resize(name.len() + 62, 0);
        assert_eq!(stored(1003, &[(1, 1)], &[Some(b"nm")]), name);
        let uuid: Vec<u8> = (0..16).collect();
        assert_eq!(stored(2951, &[(1, 1)], &[Some(&uuid)])[20..], uuid[..]);
    }

    #[test]
    fn walk_reads_stored_elements() {
        let ints = |vals: &[Option<i32>]| -> Vec<Option<Vec<u8>>> {
            vals.iter()
                .map(|v| v.map(|v| v.to_le_bytes().to_vec()))
                .collect()
        };
        let one = 1i32.to_be_bytes();
        let body = stored(1007, &[(3, -4)], &[Some(&one), None, Some(&one)]);
        let arr = ArrayBody::parse(&body, el(1007)).unwrap();
        assert_eq!((arr.ndim(), arr.len(), arr.is_empty()), (1, 3, false));
        assert!(arr.has_nulls());
        assert_eq!(
            collect(&body, el(1007)),
            Ok(ints(&[Some(1), None, Some(1)]))
        );

        let grid: Vec<_> = (1..=4i32).map(i32::to_be_bytes).collect();
        let items: Vec<_> = grid.iter().map(|b| Some(&b[..])).collect();
        let body = stored(1007, &[(2, 1), (2, 1)], &items);
        let arr = ArrayBody::parse(&body, el(1007)).unwrap();
        assert_eq!((arr.ndim(), arr.len()), (2, 4));
        assert!(!arr.has_nulls());
        let packed: Vec<u8> = (1..=4i32).flat_map(i32::to_le_bytes).collect();
        assert_eq!(arr.packed(), Ok(Some(&packed[..])));
        let cut = ArrayBody::parse(&body[..body.len() - 1], el(1007)).unwrap();
        assert!(cut.packed().is_err());
        let nulls = stored(1007, &[(2, 1)], &[Some(&one), None]);
        assert_eq!(
            ArrayBody::parse(&nulls, el(1007)).unwrap().packed(),
            Ok(None)
        );
        let uuid = stored(2951, &[(1, 1)], &[Some(&[0; 16])]);
        assert_eq!(
            ArrayBody::parse(&uuid, el(2951)).unwrap().packed(),
            Ok(None)
        );

        let empty = stored(1009, &[], &[]);
        let arr = ArrayBody::parse(&empty, el(1009)).unwrap();
        assert!(arr.is_empty() && !arr.has_nulls());
        assert_eq!(arr.elements().count(), 0);

        let wide = stored(1022, &[(2, 1)], &[None, Some(&2.5f64.to_be_bytes())]);
        assert_eq!(
            collect(&wide, el(1022)),
            Ok(vec![None, Some(2.5f64.to_le_bytes().to_vec())])
        );

        // Short headers still start nominal-aligned
        let mut short = words(&[1, 0, 25, 2, 1]);
        short.extend_from_slice(&[7, b'a', b'b', 0, 5, b'c']);
        assert_eq!(
            collect(&short, el(1009)),
            Ok(vec![Some(b"ab".to_vec()), Some(b"c".to_vec())])
        );
    }

    #[test]
    fn walk_rejects_malformed_headers() {
        let bad = |body: Vec<u8>, want: CodecError| {
            assert_eq!(
                ArrayBody::parse(&body, el(1007)).unwrap_err(),
                want,
                "{body:?}"
            );
        };
        bad(
            words(&[1, 0]),
            CodecError::Truncated {
                offset: 8,
                need: 4,
                have: 0,
            },
        );
        bad(words(&[7, 0, 23]), CodecError::BadArray("dimension count"));
        bad(words(&[-1, 0, 23]), CodecError::BadArray("dimension count"));
        bad(
            words(&[1, 0, 25, 1, 1]),
            CodecError::ArrayElemType { want: 23, got: 25 },
        );
        bad(
            words(&[1, 0, 23, -1, 1]),
            CodecError::BadArray("negative dimension"),
        );
        bad(
            words(&[1, 0, 23, 1]),
            CodecError::Truncated {
                offset: 16,
                need: 4,
                have: 0,
            },
        );
        let mut huge = vec![6, 0, 23];
        huge.extend([i32::MAX; 6]);
        huge.extend([1; 6]);
        bad(words(&huge), CodecError::BadArray("element count"));
        bad(
            words(&[1, 28, 23, 1, 1, 0]),
            CodecError::BadArray("data offset"),
        );
        bad(
            words(&[1, 32, 23, 1, 1]),
            CodecError::Truncated {
                offset: 20,
                need: 1,
                have: 0,
            },
        );
    }

    #[test]
    fn walk_rejects_malformed_elements() {
        let first_err = |array_oid: u32, body: Vec<u8>| {
            let arr = ArrayBody::parse(&body, el(array_oid)).unwrap();
            let mut it = arr.elements();
            let err = it.find_map(Result::err).unwrap();
            assert!(it.next().is_none(), "walk stops after an error");
            err
        };
        assert_eq!(
            first_err(1007, words(&[1, 0, 23, 2, 1, 5])),
            CodecError::Truncated {
                offset: 24,
                need: 4,
                have: 0,
            }
        );
        let text = |tail: &[u8]| {
            let mut v = words(&[1, 0, 25, 1, 1]);
            v.extend_from_slice(tail);
            v
        };
        assert_eq!(
            first_err(1009, text(&[0x01, 18])),
            CodecError::BadArray("external element")
        );
        assert_eq!(
            first_err(1009, text(&(((12u32) << 2) | 0b10).to_le_bytes())),
            CodecError::BadArray("compressed element")
        );
        assert_eq!(
            first_err(1009, text(&[7, b'a'])),
            CodecError::Truncated {
                offset: 20,
                need: 3,
                have: 2,
            }
        );
        assert_eq!(
            first_err(1009, text(&[])),
            CodecError::Truncated {
                offset: 20,
                need: 1,
                have: 0,
            }
        );
        assert_eq!(
            first_err(1009, text(&[0, 0])),
            CodecError::Truncated {
                offset: 20,
                need: 4,
                have: 2,
            }
        );
    }

    #[test]
    fn send_rejects_malformed_input() {
        let bad = |array_oid: u32, send: Vec<u8>| {
            let mut out = Vec::new();
            array_from_send(&send, el(array_oid), &mut out).unwrap_err()
        };
        let be = |ws: &[i32]| ws.iter().flat_map(|w| w.to_be_bytes()).collect::<Vec<u8>>();
        assert_eq!(
            bad(1007, be(&[7, 0, 23])),
            CodecError::BadArray("dimension count")
        );
        assert_eq!(
            bad(1007, be(&[1, 2, 23])),
            CodecError::BadArray("null flag")
        );
        assert_eq!(
            bad(1007, be(&[0, 0, 25])),
            CodecError::ArrayElemType { want: 23, got: 25 }
        );
        assert_eq!(
            bad(1007, be(&[1, 0, 23, -1, 1])),
            CodecError::BadArray("negative dimension")
        );
        let mut huge = vec![6, 0, 23];
        huge.extend([i32::MAX, 1].repeat(6));
        assert_eq!(bad(1007, be(&huge)), CodecError::BadArray("element count"));
        assert_eq!(
            bad(1007, be(&[1, 0, 23, 1, 1, 4])),
            CodecError::Truncated {
                offset: 24,
                need: 4,
                have: 0,
            }
        );
        assert_eq!(
            bad(1007, be(&[1, 0, 23, 1, 1, 4, 1, 0])),
            CodecError::BadArray("trailing bytes")
        );
        assert_eq!(
            bad(1007, send(23, &[(1, 1)], &[Some(&[1, 2])])),
            CodecError::BadArray("element width")
        );
        assert_eq!(
            bad(1003, send(19, &[(1, 1)], &[Some(&[b'n'; 64])])),
            CodecError::BadArray("element width")
        );
        assert_eq!(
            bad(2951, send(2950, &[(1, 1)], &[Some(&[0; 15])])),
            CodecError::BadArray("element width")
        );
    }
}
