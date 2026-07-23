//! Tier 3 codecs: small fixed-layout types decoded locally.
//!
//! - **Local**: `numeric`, `inet` / `cidr`, `interval`. Stable layout,
//!   mechanical decoders, per-row hot-path latency would dominate over libpq.
//! - **Deferred to the shadow extension** (`walshadow`): `jsonb`, arrays,
//!   `tsvector`, every other Tier 3 type. Surfaced as
//!   [`crate::decode::heap_decoder::ColumnValue::PgPending`] carrying raw on-disk
//!   bytes; resolved at emit time via `walshadow_decode_disk(oid, bytea) ->
//!   text` against shadow PG. One source of truth, no codec drift.
//!
//! Each decoder takes the varlena *body* (or raw fixed-width bytes for
//! `interval`) and produces a tagged value whose `text` matches PG
//! `typoutput`.

use thiserror::Error;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum CodecError {
    #[error("truncated body at offset {offset}: need {need} bytes, have {have}")]
    Truncated {
        offset: usize,
        need: usize,
        have: usize,
    },
    #[error("malformed numeric: weight={weight} ndigits={ndigits} dscale={dscale}")]
    BadNumeric {
        weight: i32,
        ndigits: usize,
        dscale: i32,
    },
    #[error("malformed inet: family={family:#x} bits={bits} addr_len={addr_len}")]
    BadInet {
        family: u8,
        bits: u8,
        addr_len: usize,
    },
    #[error("malformed jsonb: version={version}")]
    BadJsonbVersion { version: u8 },
    #[error("malformed jsonb container: {0}")]
    BadJsonbContainer(&'static str),
    #[error("malformed array header: ndim={ndim} dataoffset={dataoffset}")]
    BadArrayHeader { ndim: i32, dataoffset: i32 },
    #[error("unsupported array element type oid {0}")]
    UnsupportedArrayElement(u32),
}

// numeric — varlena arbitrary-precision decimal. On-disk layout per
// `src/backend/utils/adt/numeric.c`:
//
// * Short form (top bit of n_header set):
//     uint16  n_header   = NUMERIC_SHORT | (sign?0x2000:0)
//                          | ((dscale & 0x3F) << 7)
//                          | (weight & 0x7F sign-extended via 0x40)
//     NumericDigit  digits[ndigits]   (int16, base-10000)
//
// * Long form (top bit clear, flag bits != NUMERIC_SPECIAL):
//     uint16  n_sign_dscale = sign(POS/NEG) | (dscale & 0x3FFF)
//     int16   weight
//     NumericDigit  digits[ndigits]
//
// * Special form (flag bits == NUMERIC_SPECIAL):
//     uint16  n_header   = NUMERIC_NAN | NUMERIC_PINF | NUMERIC_NINF
//     no digits.

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

/// Decode `numeric` varlena body; `Finite(s)` matches PG `numeric_out`
/// exactly for finite inputs, specials carry their flag
pub fn decode_numeric(body: &[u8]) -> Result<NumericKind, CodecError> {
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
        return Ok(match n_header {
            NUMERIC_NAN => NumericKind::NaN,
            NUMERIC_PINF => NumericKind::PInf,
            NUMERIC_NINF => NumericKind::NInf,
            _ => NumericKind::NaN, // unknown special, treat as NaN (lossy-safe)
        });
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

    let mut digits = Vec::new();
    let mut cur = digits_off;
    while cur + 2 <= body.len() {
        digits.push(i16::from_le_bytes([body[cur], body[cur + 1]]));
        cur += 2;
    }
    let ndigits = digits.len();

    if dscale < 0 || ndigits > (NUMERIC_DSCALE_MASK as usize) + 4 {
        return Err(CodecError::BadNumeric {
            weight,
            ndigits,
            dscale,
        });
    }

    Ok(NumericKind::Finite(render_numeric(
        sign == NUMERIC_NEG,
        weight,
        dscale,
        &digits,
    )))
}

/// Mirrors `get_str_from_var` in `numeric.c` (DEC_DIGITS == 4 branch)
fn render_numeric(neg: bool, weight: i32, dscale: i32, digits: &[i16]) -> String {
    let ndigits = digits.len() as i32;
    let mut out = String::new();
    if neg {
        out.push('-');
    }

    if weight < 0 {
        out.push('0');
    } else {
        let mut first_block = true;
        for d in 0..=weight {
            let dig = if d < ndigits { digits[d as usize] } else { 0 };
            if first_block {
                // Suppress leading zeros in highest-order NBASE digit
                let s = format!("{dig}");
                out.push_str(&s);
                first_block = false;
            } else {
                // Subsequent NBASE digits take DEC_DIGITS chars
                out.push_str(&format!("{:0>4}", dig.max(0)));
            }
        }
    }

    if dscale > 0 {
        out.push('.');
        let mut written = 0i32;
        let mut d = weight + 1;
        while written < dscale {
            let dig = if (0..ndigits).contains(&d) {
                digits[d as usize]
            } else {
                0
            };
            // 4 decimal chars per NBASE digit (DEC_DIGITS == 4)
            let s = format!("{:0>4}", dig.max(0));
            for ch in s.chars() {
                if written >= dscale {
                    break;
                }
                out.push(ch);
                written += 1;
            }
            d += 1;
        }
    }
    out
}

// inet / cidr — on-disk `inet_struct` (utils/inet.h):
//   uint8 family   (2 = AF_INET, 3 = AF_INET6)
//   uint8 bits     (netmask bits)
//   uint8 ipaddr[nb]  (nb = 4 for AF_INET, 16 for AF_INET6)
//
// PG wire format (`inet_send`) adds is_cidr flag + addr byte count, but those
// are NOT on disk: is_cidr comes from the column type OID (INETOID vs
// CIDROID), addr count is implied by family.

pub const PGSQL_AF_INET: u8 = 2;
pub const PGSQL_AF_INET6: u8 = 3;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InetValue {
    pub family: u8,
    pub bits: u8,
    pub is_cidr: bool,
    pub addr: Vec<u8>,
}

/// `is_cidr` is **not** in the bytes; caller passes it from the column type OID
pub fn decode_inet(body: &[u8], is_cidr: bool) -> Result<InetValue, CodecError> {
    if body.len() < 2 {
        return Err(CodecError::Truncated {
            offset: 0,
            need: 2,
            have: body.len(),
        });
    }
    let family = body[0];
    let bits = body[1];
    let nb = match family {
        PGSQL_AF_INET => 4,
        PGSQL_AF_INET6 => 16,
        _ => {
            return Err(CodecError::BadInet {
                family,
                bits,
                addr_len: 0,
            });
        }
    };
    if body.len() < 2 + nb {
        return Err(CodecError::Truncated {
            offset: 2,
            need: nb,
            have: body.len() - 2,
        });
    }
    Ok(InetValue {
        family,
        bits,
        is_cidr,
        addr: body[2..2 + nb].to_vec(),
    })
}

impl InetValue {
    /// PG `inet_out` / `cidr_out`: dotted-quad or colon-hex with optional
    /// `/bits` suffix. `inet` omits suffix when bits == family max; `cidr`
    /// always emits it
    pub fn to_text(&self) -> String {
        let addr_text = match self.family {
            PGSQL_AF_INET => format!(
                "{}.{}.{}.{}",
                self.addr[0], self.addr[1], self.addr[2], self.addr[3]
            ),
            PGSQL_AF_INET6 => format_ipv6(&self.addr),
            _ => String::from("?"),
        };
        let max_bits = if self.family == PGSQL_AF_INET {
            32
        } else {
            128
        };
        if self.is_cidr || self.bits != max_bits {
            format!("{addr_text}/{}", self.bits)
        } else {
            addr_text
        }
    }
}

/// IPv6 matching PG `inet_net_ntop`: RFC 5952 canonical form (lower-case hex,
/// no per-group leading zeros, `::` collapses longest run of ≥2 zero groups)
fn format_ipv6(bytes: &[u8]) -> String {
    let mut groups = [0u16; 8];
    for (i, g) in groups.iter_mut().enumerate() {
        *g = ((bytes[i * 2] as u16) << 8) | bytes[i * 2 + 1] as u16;
    }
    let mut best_start = None;
    let mut best_len = 1usize;
    let mut i = 0;
    while i < 8 {
        if groups[i] == 0 {
            let mut j = i;
            while j < 8 && groups[j] == 0 {
                j += 1;
            }
            let run = j - i;
            if run > best_len {
                best_len = run;
                best_start = Some(i);
            }
            i = j;
        } else {
            i += 1;
        }
    }
    let mut out = String::new();
    let mut k = 0;
    while k < 8 {
        if Some(k) == best_start {
            out.push_str("::");
            k += best_len;
            continue;
        }
        if !out.is_empty() && !out.ends_with(':') {
            out.push(':');
        }
        out.push_str(&format!("{:x}", groups[k]));
        k += 1;
    }
    out
}

// interval — fixed 16 bytes: i64 micros + i32 days + i32 months

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IntervalValue {
    pub months: i32,
    pub days: i32,
    pub micros: i64,
}

pub fn decode_interval(body: &[u8]) -> Result<IntervalValue, CodecError> {
    if body.len() < 16 {
        return Err(CodecError::Truncated {
            offset: 0,
            need: 16,
            have: body.len(),
        });
    }
    let micros = i64::from_le_bytes(body[0..8].try_into().unwrap());
    let days = i32::from_le_bytes(body[8..12].try_into().unwrap());
    let months = i32::from_le_bytes(body[12..16].try_into().unwrap());
    Ok(IntervalValue {
        months,
        days,
        micros,
    })
}

impl IntervalValue {
    /// PG `interval_out`: e.g. "1 year 2 mons 3 days 04:05:06.7". Zero
    /// components omitted; sub-second as `.ffffff` (≤6 digits) trailing-zero
    /// trimmed
    pub fn to_text(&self) -> String {
        if self.months == 0 && self.days == 0 && self.micros == 0 {
            return "00:00:00".to_string();
        }
        let mut parts: Vec<String> = Vec::new();
        let years = self.months / 12;
        let mons = self.months % 12;
        if years != 0 {
            parts.push(format!(
                "{years} {}",
                if years.abs() == 1 { "year" } else { "years" }
            ));
        }
        if mons != 0 {
            parts.push(format!(
                "{mons} {}",
                if mons.abs() == 1 { "mon" } else { "mons" }
            ));
        }
        if self.days != 0 {
            parts.push(format!(
                "{} {}",
                self.days,
                if self.days.abs() == 1 { "day" } else { "days" }
            ));
        }
        if self.micros != 0 || parts.is_empty() {
            parts.push(format_time_us(self.micros));
        }
        parts.join(" ")
    }
}

pub(crate) fn format_time_us(mut us: i64) -> String {
    let neg = us < 0;
    if neg {
        us = -us;
    }
    let hours = us / 3_600_000_000;
    us %= 3_600_000_000;
    let mins = us / 60_000_000;
    us %= 60_000_000;
    let secs = us / 1_000_000;
    let frac = us % 1_000_000;
    let prefix = if neg { "-" } else { "" };
    if frac == 0 {
        format!("{prefix}{hours:02}:{mins:02}:{secs:02}")
    } else {
        let mut frac_s = format!("{frac:06}");
        while frac_s.ends_with('0') {
            frac_s.pop();
        }
        format!("{prefix}{hours:02}:{mins:02}:{secs:02}.{frac_s}")
    }
}

/// PG `timetz_out`: time-of-day plus zone offset. PG stores zone as seconds
/// *west* of UTC (negative east), so displayed offset is `-tz_seconds`. `±HH`,
/// appends `:MM` then `:SS` only when nonzero
pub(crate) fn timetz_to_text(micros: i64, tz_seconds: i32) -> String {
    let mut s = format_time_us(micros);
    let off = -tz_seconds;
    let sign = if off < 0 { '-' } else { '+' };
    let abs = off.unsigned_abs();
    let hh = abs / 3600;
    let mm = (abs % 3600) / 60;
    let ss = abs % 60;
    s.push(sign);
    s.push_str(&format!("{hh:02}"));
    if mm != 0 || ss != 0 {
        s.push_str(&format!(":{mm:02}"));
        if ss != 0 {
            s.push_str(&format!(":{ss:02}"));
        }
    }
    s
}

/// CH stores a UUID as two little-endian `UInt64` halves, so each 8-byte half
/// of PG's network-order bytes is reversed.
pub(crate) fn uuid_to_ch_wire(b: &[u8; 16]) -> [u8; 16] {
    let mut out = *b;
    out[..8].reverse();
    out[8..].reverse();
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uuid_ch_wire_reverses_each_half() {
        let pg = [
            0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd,
            0xee, 0xff,
        ];
        let want = [
            0x77, 0x66, 0x55, 0x44, 0x33, 0x22, 0x11, 0x00, 0xff, 0xee, 0xdd, 0xcc, 0xbb, 0xaa,
            0x99, 0x88,
        ];
        assert_eq!(uuid_to_ch_wire(&pg), want);
    }

    #[test]
    fn interval_decode_and_to_text() {
        let mut b = Vec::new();
        b.extend_from_slice(&14_706_700_000i64.to_le_bytes());
        b.extend_from_slice(&3i32.to_le_bytes());
        b.extend_from_slice(&14i32.to_le_bytes());
        let v = decode_interval(&b).unwrap();
        assert_eq!(
            v,
            IntervalValue {
                months: 14,
                days: 3,
                micros: 14_706_700_000,
            }
        );
        assert_eq!(v.to_text(), "1 year 2 mons 3 days 04:05:06.7");

        assert_eq!(
            IntervalValue {
                months: 0,
                days: 0,
                micros: 0,
            }
            .to_text(),
            "00:00:00",
        );
        assert_eq!(
            IntervalValue {
                months: 13,
                days: 1,
                micros: 0,
            }
            .to_text(),
            "1 year 1 mon 1 day",
        );
        assert_eq!(
            IntervalValue {
                months: 0,
                days: 0,
                micros: 90_000_000,
            }
            .to_text(),
            "00:01:30",
        );
        assert_eq!(
            IntervalValue {
                months: -1,
                days: 0,
                micros: 0,
            }
            .to_text(),
            "-1 mon",
        );

        assert!(decode_interval(&[0u8; 8]).is_err());
    }

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
        // 42
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
        // 0: no digits
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
        let one_byte = vec![0u8];
        assert!(matches!(
            decode_numeric(&one_byte),
            Err(CodecError::Truncated { .. })
        ));
    }

    #[test]
    fn inet_ipv4_simple() {
        let body = vec![PGSQL_AF_INET, 32, 192, 168, 0, 1];
        let v = decode_inet(&body, false).unwrap();
        assert_eq!(v.family, PGSQL_AF_INET);
        assert_eq!(v.bits, 32);
        assert!(!v.is_cidr);
        assert_eq!(v.addr, vec![192, 168, 0, 1]);
        assert_eq!(v.to_text(), "192.168.0.1");
    }

    #[test]
    fn inet_ipv4_cidr() {
        let body = vec![PGSQL_AF_INET, 24, 10, 0, 0, 0];
        let v = decode_inet(&body, true).unwrap();
        assert!(v.is_cidr);
        assert_eq!(v.to_text(), "10.0.0.0/24");
    }

    #[test]
    fn inet_ipv4_with_short_mask() {
        // non-cidr but bits != max, suffix appears
        let body = vec![PGSQL_AF_INET, 24, 192, 168, 0, 1];
        let v = decode_inet(&body, false).unwrap();
        assert_eq!(v.to_text(), "192.168.0.1/24");
    }

    #[test]
    fn inet_ipv6_loopback() {
        let mut body = vec![PGSQL_AF_INET6, 128];
        body.extend_from_slice(&[0u8; 15]);
        body.push(1);
        let v = decode_inet(&body, false).unwrap();
        assert_eq!(v.to_text(), "::1");
    }

    #[test]
    fn inet_ipv6_compressed_middle() {
        let mut body = vec![PGSQL_AF_INET6, 128];
        body.extend_from_slice(&[0xfe, 0x80, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x01]);
        let v = decode_inet(&body, false).unwrap();
        assert_eq!(v.to_text(), "fe80::1");
    }

    #[test]
    fn inet_rejects_unknown_family() {
        let body = vec![99u8, 32, 1, 2, 3, 4];
        assert!(matches!(
            decode_inet(&body, false),
            Err(CodecError::BadInet { .. })
        ));
    }

    #[test]
    fn timetz_text_renders_zone() {
        // UTC+2 stored west-positive as tz_seconds=-7200, displayed +02
        let micros = ((12 * 3600 + 34 * 60 + 56) as i64) * 1_000_000;
        assert_eq!(timetz_to_text(micros, -7200), "12:34:56+02");
        assert_eq!(timetz_to_text(micros, 0), "12:34:56+00");
        // UTC-5:30 → tz_seconds=19800
        assert_eq!(timetz_to_text(micros, 19800), "12:34:56-05:30");
        assert_eq!(timetz_to_text(micros + 500_000, -7200), "12:34:56.5+02");
    }

    #[test]
    fn interval_decode_basic() {
        // on-disk order: micros, days, months
        let mut body = Vec::new();
        body.extend_from_slice(&3i64.to_le_bytes());
        body.extend_from_slice(&2i32.to_le_bytes());
        body.extend_from_slice(&1i32.to_le_bytes());
        let v = decode_interval(&body).unwrap();
        assert_eq!(v.months, 1);
        assert_eq!(v.days, 2);
        assert_eq!(v.micros, 3);
        assert!(v.to_text().contains("1 mon"));
        assert!(v.to_text().contains("2 days"));
        assert!(v.to_text().ends_with("00:00:00.000003"));
    }

    #[test]
    fn interval_one_year_plus_one_hour() {
        let mut body = Vec::new();
        body.extend_from_slice(&3_600_000_000i64.to_le_bytes());
        body.extend_from_slice(&0i32.to_le_bytes());
        body.extend_from_slice(&12i32.to_le_bytes());
        let v = decode_interval(&body).unwrap();
        assert_eq!(v.to_text(), "1 year 01:00:00");
    }

    #[test]
    fn interval_zero() {
        let body = vec![0u8; 16];
        let v = decode_interval(&body).unwrap();
        assert_eq!(v.to_text(), "00:00:00");
    }

    #[test]
    fn numeric_short_negative_weight_renders_leading_zero() {
        // 0.5: weight=-1 exercises short-form sign extension + `weight < 0` arm
        let body = short_numeric(false, -1, 1, &[5000]);
        assert_eq!(
            decode_numeric(&body).unwrap(),
            NumericKind::Finite("0.5".into())
        );
    }

    #[test]
    fn numeric_long_form_truncated_body() {
        // Long form, body < 4 bytes → Truncated at offset 2
        let body = NUMERIC_POS.to_le_bytes().to_vec();
        match decode_numeric(&body) {
            Err(CodecError::Truncated { offset: 2, .. }) => (),
            other => panic!("expected Truncated at offset 2, got {other:?}"),
        }
    }

    #[test]
    fn numeric_unknown_special_falls_back_to_nan() {
        // Special flag set, tag != NaN/PInf/NInf → NaN fallback
        let header: u16 = NUMERIC_SPECIAL | 0x0001;
        let body = header.to_le_bytes();
        assert_eq!(decode_numeric(&body).unwrap(), NumericKind::NaN);
    }

    #[test]
    fn numeric_dscale_trailing_zero_pad() {
        // 5.0000: post-decimal loop runs past ndigits, hits `0` fallback
        let body = short_numeric(false, 0, 4, &[5]);
        assert_eq!(
            decode_numeric(&body).unwrap(),
            NumericKind::Finite("5.0000".into())
        );
    }

    #[test]
    fn numeric_rejects_oversized_digit_array() {
        // ndigits > NUMERIC_DSCALE_MASK + 4 → BadNumeric
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

    #[test]
    fn inet_ipv4_truncated_addr() {
        // AF_INET expects 4 addr bytes, only 2 supplied
        let body = vec![PGSQL_AF_INET, 32, 1, 2];
        assert!(matches!(
            decode_inet(&body, false),
            Err(CodecError::Truncated { offset: 2, .. })
        ));
    }

    #[test]
    fn inet_truncated_header() {
        let body = vec![PGSQL_AF_INET];
        assert!(matches!(
            decode_inet(&body, false),
            Err(CodecError::Truncated { offset: 0, .. })
        ));
    }

    #[test]
    fn inet_unknown_family_text_renders_placeholder() {
        // Direct construction bypasses decode_inet's family check → `_ => "?"`
        let v = InetValue {
            family: 99,
            bits: 0,
            is_cidr: false,
            addr: Vec::new(),
        };
        assert!(v.to_text().starts_with('?'));
    }

    #[test]
    fn inet_ipv6_full_expansion_no_collapse() {
        // No zero run ≥2, exercises explicit `:` push between every group
        let mut body = vec![PGSQL_AF_INET6, 128];
        for i in 1..=8u16 {
            body.extend_from_slice(&i.to_be_bytes());
        }
        let v = decode_inet(&body, false).unwrap();
        assert_eq!(v.to_text(), "1:2:3:4:5:6:7:8");
    }

    #[test]
    fn interval_truncated_body() {
        let body = vec![0u8; 10];
        assert!(matches!(
            decode_interval(&body),
            Err(CodecError::Truncated { .. })
        ));
    }

    #[test]
    fn interval_negative_time_renders_with_sign() {
        let v = IntervalValue {
            months: 0,
            days: 0,
            micros: -3_600_000_000,
        };
        assert_eq!(v.to_text(), "-01:00:00");
    }

    #[test]
    fn interval_trims_trailing_fraction_zeros() {
        // 1000 us: frac "001000" trims to "001"
        let v = IntervalValue {
            months: 0,
            days: 0,
            micros: 1_000,
        };
        assert_eq!(v.to_text(), "00:00:00.001");
    }
}
