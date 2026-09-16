//! PG `numeric`, rendered as `numeric_out` text.
//!
//! Varlena arbitrary-precision decimal. On-disk layout per
//! `src/backend/utils/adt/numeric.c`:
//!
//! ```text
//! * Short form (top bit of n_header set):
//!     uint16  n_header   = NUMERIC_SHORT | (sign?0x2000:0)
//!                          | ((dscale & 0x3F) << 7)
//!                          | (weight & 0x7F sign-extended via 0x40)
//!     NumericDigit  digits[ndigits]   (int16, base-10000)
//!
//! * Long form (top bit clear, flag bits != NUMERIC_SPECIAL):
//!     uint16  n_sign_dscale = sign(POS/NEG) | (dscale & 0x3FFF)
//!     int16   weight
//!     NumericDigit  digits[ndigits]
//!
//! * Special form (flag bits == NUMERIC_SPECIAL):
//!     uint16  n_header   = NUMERIC_NAN | NUMERIC_PINF | NUMERIC_NINF
//!     no digits.
//! ```

use super::CodecError;

const NUMERIC_SIGN_MASK: u16 = 0xC000;
const NUMERIC_POS: u16 = 0x0000;
const NUMERIC_NEG: u16 = 0x4000;
const NUMERIC_SHORT: u16 = 0x8000;
const NUMERIC_SPECIAL: u16 = 0xC000;

const NUMERIC_NAN: u16 = 0xC000;
const NUMERIC_PINF: u16 = 0xD000;
const NUMERIC_NINF: u16 = 0xF000;

const NUMERIC_SHORT_SIGN_MASK: u16 = 0x2000;
const NUMERIC_SHORT_DSCALE_MASK: u16 = 0x1F80;
const NUMERIC_SHORT_DSCALE_SHIFT: u16 = 7;
const NUMERIC_SHORT_WEIGHT_SIGN_MASK: u16 = 0x0040;
const NUMERIC_SHORT_WEIGHT_MASK: u16 = 0x003F;

const NUMERIC_DSCALE_MASK: u16 = 0x3FFF;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NumericKind {
    NaN,
    /// `+Infinity` (PG 14+)
    PInf,
    /// `-Infinity` (PG 14+)
    NInf,
    /// PG-text form
    Finite(String),
}

impl NumericKind {
    pub(crate) fn as_text(&self) -> &str {
        match self {
            Self::Finite(s) => s,
            Self::NaN => "NaN",
            Self::PInf => "Infinity",
            Self::NInf => "-Infinity",
        }
    }
}

/// `numeric` header fields plus its base-10000 digits, still in the datum
enum Numeric<'a> {
    Special(NumericKind),
    Finite {
        neg: bool,
        weight: i32,
        dscale: i32,
        /// `NumericDigit`s, little-endian
        digits: &'a [[u8; 2]],
    },
}

fn parse_numeric(body: &[u8]) -> Result<Numeric<'_>, CodecError> {
    if body.len() < 2 {
        return Err(CodecError::Truncated {
            offset: 0,
            need: 2,
            have: body.len(),
        });
    }
    let n_header = u16::from_le_bytes([body[0], body[1]]);
    let flag = n_header & NUMERIC_SIGN_MASK;
    let is_short = flag == NUMERIC_SHORT;
    let is_special = flag == NUMERIC_SPECIAL;

    if is_special {
        return Ok(Numeric::Special(match n_header {
            NUMERIC_NAN => NumericKind::NaN,
            NUMERIC_PINF => NumericKind::PInf,
            NUMERIC_NINF => NumericKind::NInf,
            _ => NumericKind::NaN, // unknown special, treat as NaN (lossy-safe)
        }));
    }

    let (sign, weight, dscale, digits_off) = if is_short {
        let sign = if n_header & NUMERIC_SHORT_SIGN_MASK != 0 {
            NUMERIC_NEG
        } else {
            NUMERIC_POS
        };
        let dscale = ((n_header & NUMERIC_SHORT_DSCALE_MASK) >> NUMERIC_SHORT_DSCALE_SHIFT) as i32;
        let mut w = (n_header & NUMERIC_SHORT_WEIGHT_MASK) as i32;
        if n_header & NUMERIC_SHORT_WEIGHT_SIGN_MASK != 0 {
            // 7-bit sign extension
            w |= !(NUMERIC_SHORT_WEIGHT_MASK as i32);
        }
        (sign, w, dscale, 2usize)
    } else {
        if body.len() < 4 {
            return Err(CodecError::Truncated {
                offset: 2,
                need: 4,
                have: body.len(),
            });
        }
        let sign = n_header & NUMERIC_SIGN_MASK;
        let dscale = (n_header & NUMERIC_DSCALE_MASK) as i32;
        let weight = i16::from_le_bytes([body[2], body[3]]) as i32;
        (sign, weight, dscale, 4usize)
    };

    let digits = body[digits_off..].as_chunks::<2>().0;
    let ndigits = digits.len();
    if dscale < 0 || ndigits > (NUMERIC_DSCALE_MASK as usize) + 4 {
        return Err(CodecError::BadNumeric {
            weight,
            ndigits,
            dscale,
        });
    }

    Ok(Numeric::Finite {
        neg: sign == NUMERIC_NEG,
        weight,
        dscale,
        digits,
    })
}

/// Decode `numeric` varlena body to PG `numeric_out` text or special value
pub fn decode_numeric(body: &[u8]) -> Result<NumericKind, CodecError> {
    match parse_numeric(body)? {
        Numeric::Special(kind) => Ok(kind),
        Numeric::Finite {
            neg,
            weight,
            dscale,
            digits,
        } => {
            let mut out = String::with_capacity(numeric_text_len(weight, dscale));
            render_numeric(neg, weight, dscale, digits, &mut out);
            Ok(NumericKind::Finite(out))
        }
    }
}

/// `numeric_out` text into a caller's buffer, for callers holding one already:
/// `jsonb`'s numeric children render straight into the enclosing document
pub(crate) fn render_numeric_text(body: &[u8], out: &mut String) -> Result<(), CodecError> {
    match parse_numeric(body)? {
        Numeric::Special(kind) => out.push_str(kind.as_text()),
        Numeric::Finite {
            neg,
            weight,
            dscale,
            digits,
        } => render_numeric(neg, weight, dscale, digits, out),
    }
    Ok(())
}

/// `get_str_from_var`'s own sizing: sign, the integer groups, point, `dscale`
fn numeric_text_len(weight: i32, dscale: i32) -> usize {
    let int_chars = ((weight + 1) * 4).max(1);
    (int_chars + dscale) as usize + 6
}

/// 4 decimal chars for one base-10000 `NumericDigit` (DEC_DIGITS == 4)
fn dec_digits(dig: u16) -> [u8; 4] {
    [
        b'0' + (dig / 1000 % 10) as u8,
        b'0' + (dig / 100 % 10) as u8,
        b'0' + (dig / 10 % 10) as u8,
        b'0' + (dig % 10) as u8,
    ]
}

fn push_ascii(out: &mut String, chars: &[u8]) {
    for c in chars {
        out.push(char::from(*c));
    }
}

/// Mirrors `get_str_from_var` in `numeric.c` (DEC_DIGITS == 4 branch)
fn render_numeric(neg: bool, weight: i32, dscale: i32, digits: &[[u8; 2]], out: &mut String) {
    // digits past the stored ones read as zero, as do the negative indexes a
    // negative weight asks of the fraction
    let group = |d: i32| -> [u8; 4] {
        let dig = usize::try_from(d)
            .ok()
            .and_then(|i| digits.get(i))
            .map_or(0, |raw| i16::from_le_bytes(*raw).max(0) as u16);
        dec_digits(dig)
    };

    if neg {
        out.push('-');
    }

    if weight < 0 {
        out.push('0');
    } else {
        // leading group prints unpadded, so all but its last zero drops
        let lead = group(0);
        let zeros = lead.iter().take(3).take_while(|&&c| c == b'0').count();
        push_ascii(out, &lead[zeros..]);
        for d in 1..=weight {
            push_ascii(out, &group(d));
        }
    }

    if dscale > 0 {
        out.push('.');
        let mut written = 0;
        let mut d = weight + 1;
        while written < dscale {
            for c in group(d) {
                if written == dscale {
                    break;
                }
                out.push(char::from(c));
                written += 1;
            }
            d += 1;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn short_numeric(neg: bool, weight: i8, dscale: u8, digits: &[i16]) -> Vec<u8> {
        let sign_bit = if neg { NUMERIC_SHORT_SIGN_MASK } else { 0 };
        let dscale_bits =
            ((dscale as u16) << NUMERIC_SHORT_DSCALE_SHIFT) & NUMERIC_SHORT_DSCALE_MASK;
        let weight_bits = if weight < 0 {
            NUMERIC_SHORT_WEIGHT_SIGN_MASK | ((weight as i32) as u16 & NUMERIC_SHORT_WEIGHT_MASK)
        } else {
            (weight as u16) & NUMERIC_SHORT_WEIGHT_MASK
        };
        let header = NUMERIC_SHORT | sign_bit | dscale_bits | weight_bits;
        let mut out = header.to_le_bytes().to_vec();
        for d in digits {
            out.extend_from_slice(&d.to_le_bytes());
        }
        out
    }

    fn long_numeric(neg: bool, weight: i16, dscale: u16, digits: &[i16]) -> Vec<u8> {
        let sign = if neg { NUMERIC_NEG } else { NUMERIC_POS };
        let n_sign_dscale = sign | (dscale & NUMERIC_DSCALE_MASK);
        let mut out = n_sign_dscale.to_le_bytes().to_vec();
        out.extend_from_slice(&weight.to_le_bytes());
        for d in digits {
            out.extend_from_slice(&d.to_le_bytes());
        }
        out
    }

    #[test]
    fn numeric_short_one_digit() {
        let body = short_numeric(false, 0, 0, &[42]);
        assert_eq!(
            decode_numeric(&body).unwrap(),
            NumericKind::Finite("42".into())
        );
    }

    #[test]
    fn numeric_short_negative() {
        let body = short_numeric(true, 0, 0, &[7]);
        assert_eq!(
            decode_numeric(&body).unwrap(),
            NumericKind::Finite("-7".into())
        );
    }

    #[test]
    fn numeric_short_with_scale() {
        // 1.5: digits base-10000 → [1, 5000]
        let body = short_numeric(false, 0, 1, &[1, 5000]);
        assert_eq!(
            decode_numeric(&body).unwrap(),
            NumericKind::Finite("1.5".into())
        );
    }

    #[test]
    fn numeric_zero() {
        let body = short_numeric(false, 0, 0, &[]);
        assert_eq!(
            decode_numeric(&body).unwrap(),
            NumericKind::Finite("0".into())
        );
    }

    #[test]
    fn numeric_long_form_large() {
        // 12345: base-10000 blocks [1, 2345], weight 1
        let body = long_numeric(false, 1, 0, &[1, 2345]);
        assert_eq!(
            decode_numeric(&body).unwrap(),
            NumericKind::Finite("12345".into())
        );
    }

    #[test]
    fn numeric_specials() {
        let nan = NUMERIC_NAN.to_le_bytes();
        assert_eq!(decode_numeric(&nan).unwrap(), NumericKind::NaN);
        let pinf = NUMERIC_PINF.to_le_bytes();
        assert_eq!(decode_numeric(&pinf).unwrap(), NumericKind::PInf);
        let ninf = NUMERIC_NINF.to_le_bytes();
        assert_eq!(decode_numeric(&ninf).unwrap(), NumericKind::NInf);
    }

    #[test]
    fn numeric_truncated_returns_error() {
        let one_byte = [0u8];
        assert!(matches!(
            decode_numeric(&one_byte),
            Err(CodecError::Truncated { .. })
        ));
    }

    #[test]
    fn numeric_short_negative_weight_renders_leading_zero() {
        let body = short_numeric(false, -1, 1, &[5000]);
        assert_eq!(
            decode_numeric(&body).unwrap(),
            NumericKind::Finite("0.5".into())
        );
    }

    #[test]
    fn numeric_long_form_truncated_body() {
        let body = NUMERIC_POS.to_le_bytes().to_vec();
        match decode_numeric(&body) {
            Err(CodecError::Truncated { offset: 2, .. }) => (),
            other => panic!("expected Truncated at offset 2, got {other:?}"),
        }
    }

    #[test]
    fn numeric_unknown_special_falls_back_to_nan() {
        let header: u16 = NUMERIC_SPECIAL | 0x0001;
        let body = header.to_le_bytes();
        assert_eq!(decode_numeric(&body).unwrap(), NumericKind::NaN);
    }

    #[test]
    fn numeric_dscale_trailing_zero_pad() {
        let body = short_numeric(false, 0, 4, &[5]);
        assert_eq!(
            decode_numeric(&body).unwrap(),
            NumericKind::Finite("5.0000".into())
        );
    }

    #[test]
    fn numeric_rejects_oversized_digit_array() {
        let mut body = NUMERIC_POS.to_le_bytes().to_vec();
        body.extend_from_slice(&0i16.to_le_bytes()); // weight
        body.extend(std::iter::repeat_n(
            0u8,
            ((NUMERIC_DSCALE_MASK as usize) + 5) * 2,
        ));
        assert!(matches!(
            decode_numeric(&body),
            Err(CodecError::BadNumeric { .. })
        ));
    }
}
