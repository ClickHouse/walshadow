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

/// Non-finite `numeric`, a header with no digits
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Special {
    NaN,
    /// `+Infinity` (PG 14+)
    PInf,
    /// `-Infinity` (PG 14+)
    NInf,
}

impl Special {
    pub(crate) fn as_text(self) -> &'static str {
        match self {
            Self::NaN => "NaN",
            Self::PInf => "Infinity",
            Self::NInf => "-Infinity",
        }
    }

    fn header(self) -> u16 {
        match self {
            Self::NaN => NUMERIC_NAN,
            Self::PInf => NUMERIC_PINF,
            Self::NInf => NUMERIC_NINF,
        }
    }
}

/// `numeric` header fields plus its base-10000 digits, still in the datum
pub(crate) enum Numeric<'a> {
    Special(Special),
    Finite {
        neg: bool,
        weight: i32,
        dscale: i32,
        /// `NumericDigit`s, little-endian
        digits: &'a [[u8; 2]],
    },
}

pub(crate) fn parse_numeric(body: &[u8]) -> Result<Numeric<'_>, CodecError> {
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
            NUMERIC_PINF => Special::PInf,
            NUMERIC_NINF => Special::NInf,
            // Unknown special reads as NaN, lossy but safe
            _ => Special::NaN,
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

/// `numeric_out` text appended to `out`
pub fn render_numeric_text(body: &[u8], out: &mut String) -> Result<(), CodecError> {
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

/// [`render_numeric_text`] length without rendering
pub fn numeric_text_len(body: &[u8]) -> Result<usize, CodecError> {
    Ok(match parse_numeric(body)? {
        Numeric::Special(kind) => kind.as_text().len(),
        Numeric::Finite {
            neg,
            weight,
            dscale,
            digits,
        } => finite_text_len(neg, weight, dscale, digits),
    })
}

/// [`render_numeric`] length: sign, integer digits, point, `dscale`
fn finite_text_len(neg: bool, weight: i32, dscale: i32, digits: &[[u8; 2]]) -> usize {
    let int_chars = match usize::try_from(weight) {
        Ok(weight) => 4 - lead_zeros(&digit_group(digits, 0)) + 4 * weight,
        Err(_) => 1,
    };
    let frac_chars = if dscale > 0 { 1 + dscale as usize } else { 0 };
    usize::from(neg) + int_chars + frac_chars
}

/// Group `d` as 4 decimal chars. Digits past stored ones read as zero, as do
/// negative indexes a negative weight asks of the fraction
fn digit_group(digits: &[[u8; 2]], d: i32) -> [u8; 4] {
    let dig = usize::try_from(d)
        .ok()
        .and_then(|i| digits.get(i))
        .map_or(0, |raw| i16::from_le_bytes(*raw).max(0) as u16);
    dec_digits(dig)
}

/// Leading group prints unpadded, so all but its last zero drops
fn lead_zeros(lead: &[u8; 4]) -> usize {
    lead.iter().take(3).take_while(|&&c| c == b'0').count()
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
    let group = |d: i32| digit_group(digits, d);

    if neg {
        out.push('-');
    }

    if weight < 0 {
        out.push('0');
    } else {
        let lead = group(0);
        push_ascii(out, &lead[lead_zeros(&lead)..]);
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

const NUMERIC_SHORT_DSCALE_MAX: i32 = 0x3F;
const NUMERIC_SHORT_WEIGHT_MAX: i32 = 63;
const NUMERIC_SHORT_WEIGHT_MIN: i32 = -64;
const NBASE: u32 = 10_000;

/// Body PostgreSQL stores for a non-finite value
pub fn numeric_special_body(special: Special) -> Vec<u8> {
    special.header().to_le_bytes().to_vec()
}

/// Body `make_result` stores: zero groups trimmed from both ends, short
/// header when weight and dscale fit
pub fn numeric_body(neg: bool, weight: i32, dscale: u16, digits: &[i16]) -> Vec<u8> {
    let lead = digits.iter().take_while(|&&d| d == 0).count();
    let digits = &digits[lead..];
    let digits = &digits[..digits.len() - digits.iter().rev().take_while(|&&d| d == 0).count()];
    let (neg, weight) = if digits.is_empty() {
        (false, 0)
    } else {
        (neg, weight - lead as i32)
    };
    let dscale = i32::from(dscale & NUMERIC_DSCALE_MASK);
    let mut out = Vec::with_capacity(4 + 2 * digits.len());
    if dscale <= NUMERIC_SHORT_DSCALE_MAX
        && (NUMERIC_SHORT_WEIGHT_MIN..=NUMERIC_SHORT_WEIGHT_MAX).contains(&weight)
    {
        let sign = if neg { NUMERIC_SHORT_SIGN_MASK } else { 0 };
        let weight_sign = if weight < 0 {
            NUMERIC_SHORT_WEIGHT_SIGN_MASK
        } else {
            0
        };
        let header = NUMERIC_SHORT
            | sign
            | ((dscale as u16) << NUMERIC_SHORT_DSCALE_SHIFT)
            | weight_sign
            | (weight as u16 & NUMERIC_SHORT_WEIGHT_MASK);
        out.extend_from_slice(&header.to_le_bytes());
    } else {
        let sign = if neg { NUMERIC_NEG } else { NUMERIC_POS };
        out.extend_from_slice(&(sign | dscale as u16).to_le_bytes());
        out.extend_from_slice(&(weight as i16).to_le_bytes());
    }
    for d in digits {
        out.extend_from_slice(&d.to_le_bytes());
    }
    out
}

/// `numeric_in` for plain decimal text and the special spellings
pub fn numeric_from_text(text: &str) -> Option<Vec<u8>> {
    let text = text.trim();
    match text.to_ascii_lowercase().as_str() {
        "nan" => return Some(numeric_special_body(Special::NaN)),
        "infinity" | "+infinity" | "inf" | "+inf" => {
            return Some(numeric_special_body(Special::PInf));
        }
        "-infinity" | "-inf" => return Some(numeric_special_body(Special::NInf)),
        _ => {}
    }
    let (neg, body) = match text.as_bytes().first()? {
        b'-' => (true, &text[1..]),
        b'+' => (false, &text[1..]),
        _ => (false, text),
    };
    let (int, frac) = body.split_once('.').unwrap_or((body, ""));
    let all_digits = |s: &str| s.bytes().all(|b| b.is_ascii_digit());
    if (int.is_empty() && frac.is_empty()) || !all_digits(int) || !all_digits(frac) {
        return None;
    }
    let dscale = u16::try_from(frac.len()).ok()?;
    let int_pad = (4 - int.len() % 4) % 4;
    let frac_pad = (4 - frac.len() % 4) % 4;
    let decimal: Vec<u8> = std::iter::repeat_n(b'0', int_pad)
        .chain(int.bytes())
        .chain(frac.bytes())
        .chain(std::iter::repeat_n(b'0', frac_pad))
        .collect();
    let digits: Vec<i16> = decimal
        .chunks(4)
        .map(|g| g.iter().fold(0i16, |acc, b| acc * 10 + i16::from(b - b'0')))
        .collect();
    let weight = i32::try_from((int_pad + int.len()) / 4).ok()? - 1;
    Some(numeric_body(neg, weight, dscale, &digits))
}

/// Finite value times `10^scale`, fractional digits past `scale` dropped.
/// `step(m, a)` sets the caller's accumulator to `acc * m + a`, `false` on
/// overflow
pub(crate) fn scaled_groups(
    weight: i32,
    digits: &[[u8; 2]],
    scale: u32,
    mut step: impl FnMut(u32, u32) -> bool,
) -> bool {
    let digit = |g: i32| {
        usize::try_from(g)
            .ok()
            .and_then(|i| digits.get(i))
            .map_or(0, |raw| i16::from_le_bytes(*raw).max(0) as u32)
    };
    let whole = (scale / 4) as i32;
    let rem = scale % 4;
    for e in (-whole..=weight.max(0)).rev() {
        if !step(NBASE, digit(weight - e)) {
            return false;
        }
    }
    if rem == 0 {
        return true;
    }
    let tail = digit(weight + whole + 1) / 10u32.pow(4 - rem);
    step(10u32.pow(rem), tail)
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

    fn text(body: &[u8]) -> Result<String, CodecError> {
        let mut out = String::new();
        render_numeric_text(body, &mut out)?;
        Ok(out)
    }

    #[test]
    fn numeric_short_one_digit() {
        let body = short_numeric(false, 0, 0, &[42]);
        assert_eq!(text(&body).unwrap(), "42");
    }

    #[test]
    fn numeric_short_negative() {
        let body = short_numeric(true, 0, 0, &[7]);
        assert_eq!(text(&body).unwrap(), "-7");
    }

    #[test]
    fn numeric_short_with_scale() {
        // 1.5: digits base-10000 → [1, 5000]
        let body = short_numeric(false, 0, 1, &[1, 5000]);
        assert_eq!(text(&body).unwrap(), "1.5");
    }

    #[test]
    fn numeric_zero() {
        let body = short_numeric(false, 0, 0, &[]);
        assert_eq!(text(&body).unwrap(), "0");
    }

    #[test]
    fn numeric_long_form_large() {
        // 12345: base-10000 blocks [1, 2345], weight 1
        let body = long_numeric(false, 1, 0, &[1, 2345]);
        assert_eq!(text(&body).unwrap(), "12345");
    }

    #[test]
    fn numeric_specials() {
        let nan = NUMERIC_NAN.to_le_bytes();
        assert_eq!(text(&nan).unwrap(), "NaN");
        let pinf = NUMERIC_PINF.to_le_bytes();
        assert_eq!(text(&pinf).unwrap(), "Infinity");
        let ninf = NUMERIC_NINF.to_le_bytes();
        assert_eq!(text(&ninf).unwrap(), "-Infinity");
    }

    #[test]
    fn numeric_truncated_returns_error() {
        let one_byte = [0u8];
        assert!(matches!(text(&one_byte), Err(CodecError::Truncated { .. })));
    }

    #[test]
    fn numeric_short_negative_weight_renders_leading_zero() {
        let body = short_numeric(false, -1, 1, &[5000]);
        assert_eq!(text(&body).unwrap(), "0.5");
    }

    #[test]
    fn numeric_long_form_truncated_body() {
        let body = NUMERIC_POS.to_le_bytes().to_vec();
        assert_eq!(
            text(&body),
            Err(CodecError::Truncated {
                offset: 2,
                need: 4,
                have: 2,
            })
        );
    }

    #[test]
    fn numeric_unknown_special_falls_back_to_nan() {
        let header: u16 = NUMERIC_SPECIAL | 0x0001;
        let body = header.to_le_bytes();
        assert_eq!(text(&body).unwrap(), "NaN");
    }

    #[test]
    fn numeric_dscale_trailing_zero_pad() {
        let body = short_numeric(false, 0, 4, &[5]);
        assert_eq!(text(&body).unwrap(), "5.0000");
    }

    #[test]
    fn numeric_text_len_matches_render() {
        let bodies = [
            short_numeric(false, 0, 0, &[42]),
            short_numeric(true, 0, 0, &[7]),
            short_numeric(false, 0, 1, &[1, 5000]),
            short_numeric(false, 0, 0, &[]),
            short_numeric(false, -1, 1, &[5000]),
            short_numeric(true, -2, 6, &[12]),
            short_numeric(false, 0, 4, &[5]),
            long_numeric(false, 1, 0, &[1, 2345]),
            long_numeric(false, 2, 0, &[1]),
            long_numeric(true, 0, 0, &[9999]),
            NUMERIC_NAN.to_le_bytes().to_vec(),
            NUMERIC_NINF.to_le_bytes().to_vec(),
        ];
        for body in bodies {
            let rendered = text(&body).unwrap();
            assert_eq!(numeric_text_len(&body), Ok(rendered.len()), "{rendered:?}");
        }
    }

    #[test]
    fn numeric_from_text_round_trips_canonically() {
        for (input, want) in [
            ("0", "0"),
            ("-0.00", "0.00"),
            ("12345", "12345"),
            ("-12.340", "-12.340"),
            ("0.0005", "0.0005"),
            ("100000000", "100000000"),
            (".5", "0.5"),
            ("NaN", "NaN"),
            ("-Infinity", "-Infinity"),
        ] {
            let body = numeric_from_text(input).unwrap();
            assert_eq!(text(&body).unwrap(), want, "{input}");
        }
        assert_eq!(
            numeric_from_text("12345").unwrap(),
            short_numeric(false, 1, 0, &[1, 2345])
        );
        assert_eq!(
            numeric_from_text("1.5").unwrap(),
            short_numeric(false, 0, 1, &[1, 5000])
        );
        assert_eq!(
            numeric_from_text("0").unwrap(),
            short_numeric(false, 0, 0, &[])
        );
        let wide = format!("1.{}", "0".repeat(70));
        assert_eq!(
            numeric_from_text(&wide).unwrap(),
            long_numeric(false, 0, 70, &[1])
        );
        for bad in ["", "-", ".", "1e5", "1.2.3", "abc"] {
            assert_eq!(numeric_from_text(bad), None, "{bad}");
        }
    }

    #[test]
    fn scaled_groups_truncates_past_scale() {
        let scaled = |input: &str, scale: u32| {
            let body = numeric_from_text(input).unwrap();
            let Numeric::Finite { weight, digits, .. } = parse_numeric(&body).unwrap() else {
                panic!("finite");
            };
            let mut acc = 0u128;
            assert!(scaled_groups(weight, digits, scale, |m, a| {
                acc = acc * u128::from(m) + u128::from(a);
                true
            }));
            acc
        };
        assert_eq!(scaled("12.34", 2), 1234);
        assert_eq!(scaled("12.3456789", 3), 12345);
        assert_eq!(scaled("12", 5), 1_200_000);
        assert_eq!(scaled("0.0005", 4), 5);
        assert_eq!(scaled("0.0005", 3), 0);
        assert_eq!(scaled("123456789.5", 0), 123_456_789);
    }

    #[test]
    fn numeric_rejects_oversized_digit_array() {
        let mut body = NUMERIC_POS.to_le_bytes().to_vec();
        body.extend_from_slice(&0i16.to_le_bytes()); // weight
        body.extend(std::iter::repeat_n(
            0u8,
            ((NUMERIC_DSCALE_MASK as usize) + 5) * 2,
        ));
        assert!(matches!(text(&body), Err(CodecError::BadNumeric { .. })));
    }
}
