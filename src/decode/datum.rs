//! Tuple attributes as PostgreSQL stores or sends them. Encoders read source
//! bytes in place; owned copies hold only bytes that outlive their record

use crate::decode::heap_decoder::ToastPointer;
use smallvec::SmallVec;

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
    /// Inline compressed varlena: 4-byte header, `va_tcinfo`, codec body
    Compressed(&'a [u8]),
    External(ToastPointer),
    /// `typlen = -2` body without trailing NUL
    Cstring(&'a [u8]),
    /// Varlena tag PostgreSQL never writes to disk; rest of buffer
    BadTag(&'a [u8]),
    /// `typoutput` text: fast defaults read over SQL, COPY fields cast to text
    Text(&'a [u8]),
    /// `typsend` bytes of a COPY field, read back through `typreceive`
    Send(&'a [u8]),
}

/// Owned [`DatumRef`] for values outliving their record: TOAST values,
/// synthetic columns, COPY and test cells
#[derive(Clone, Debug, PartialEq)]
pub enum Datum {
    Null,
    Fixed(SmallVec<[u8; 16]>),
    Varlena(Vec<u8>),
    External(ToastPointer),
    Cstring(Vec<u8>),
    BadTag(Vec<u8>),
    Text(Vec<u8>),
    Send(Vec<u8>),
}

impl Datum {
    pub fn view(&self) -> DatumRef<'_> {
        match self {
            Self::Null => DatumRef::Null,
            Self::Fixed(b) => DatumRef::Fixed(b),
            Self::Varlena(b) => DatumRef::Varlena(b),
            Self::External(p) => DatumRef::External(*p),
            Self::Cstring(b) => DatumRef::Cstring(b),
            Self::BadTag(b) => DatumRef::BadTag(b),
            Self::Text(b) => DatumRef::Text(b),
            Self::Send(b) => DatumRef::Send(b),
        }
    }

    /// Payload bytes held, for resident accounting
    pub fn approx_bytes(&self) -> usize {
        match self {
            Self::Null => 0,
            Self::Fixed(b) => b.len(),
            Self::External(_) => std::mem::size_of::<ToastPointer>(),
            Self::Varlena(b)
            | Self::Cstring(b)
            | Self::BadTag(b)
            | Self::Text(b)
            | Self::Send(b) => b.len(),
        }
    }

    pub fn fixed(le: &[u8]) -> Self {
        Self::Fixed(SmallVec::from_slice(le))
    }
}

/// Cells in their on-disk form, for tests to state tuples
#[cfg(any(test, feature = "test-support"))]
pub mod build {
    use super::Datum;
    use crate::decode::heap_decoder::ToastPointer;

    pub fn null() -> Datum {
        Datum::Null
    }

    pub fn bool_(v: bool) -> Datum {
        Datum::fixed(&[u8::from(v)])
    }

    pub fn char_(v: i8) -> Datum {
        Datum::fixed(&v.to_le_bytes())
    }

    pub fn int2(v: i16) -> Datum {
        Datum::fixed(&v.to_le_bytes())
    }

    pub fn int4(v: i32) -> Datum {
        Datum::fixed(&v.to_le_bytes())
    }

    pub fn int8(v: i64) -> Datum {
        Datum::fixed(&v.to_le_bytes())
    }

    pub fn float4(v: f32) -> Datum {
        Datum::fixed(&v.to_le_bytes())
    }

    pub fn float8(v: f64) -> Datum {
        Datum::fixed(&v.to_le_bytes())
    }

    pub fn oid(v: u32) -> Datum {
        Datum::fixed(&v.to_le_bytes())
    }

    pub fn oid8(v: u64) -> Datum {
        Datum::fixed(&v.to_le_bytes())
    }

    /// Days since 2000-01-01
    pub fn date(v: i32) -> Datum {
        Datum::fixed(&v.to_le_bytes())
    }

    /// `time` or `timestamp[tz]` microseconds
    pub fn micros(v: i64) -> Datum {
        Datum::fixed(&v.to_le_bytes())
    }

    pub fn timetz(micros: i64, tz_seconds: i32) -> Datum {
        let mut b = micros.to_le_bytes().to_vec();
        b.extend_from_slice(&tz_seconds.to_le_bytes());
        Datum::fixed(&b)
    }

    pub fn uuid(v: [u8; 16]) -> Datum {
        Datum::fixed(&v)
    }

    pub fn name(v: impl AsRef<str>) -> Datum {
        let mut b = v.as_ref().as_bytes().to_vec();
        b.resize(64, 0);
        Datum::fixed(&b)
    }

    pub fn text(v: impl AsRef<str>) -> Datum {
        Datum::Varlena(v.as_ref().as_bytes().to_vec())
    }

    pub fn bytes(v: impl AsRef<[u8]>) -> Datum {
        Datum::Varlena(v.as_ref().to_vec())
    }

    pub fn numeric(v: impl AsRef<str>) -> Datum {
        Datum::Varlena(crate::decode::codecs::numeric_from_text(v.as_ref()).expect("numeric text"))
    }

    pub fn external(p: ToastPointer) -> Datum {
        Datum::External(p)
    }

    /// `typoutput` text
    pub fn out_text(v: impl AsRef<str>) -> Datum {
        Datum::Text(v.as_ref().as_bytes().to_vec())
    }
}
