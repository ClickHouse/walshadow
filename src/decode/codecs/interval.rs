//! PG `interval`, rendered as `interval_out` text.
//!
//! Fixed 16 bytes: `i64` micros + `i32` days + `i32` months.

use super::CodecError;
use super::time::write_time_us;
use crate::ascii_buf::AsciiBuf;

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

/// One `years`/`mons`/`days` field, space-separated from what precedes it
fn push_unit<const N: usize>(out: &mut AsciiBuf<N>, n: i32, singular: &str, plural: &str) {
    if !out.is_empty() {
        out.push(b' ');
    }
    out.push_int(n.into());
    out.push(b' ');
    out.push_str(if n.unsigned_abs() == 1 {
        singular
    } else {
        plural
    });
}

impl IntervalValue {
    /// PG `interval_out` with `IntervalStyle = postgres`
    pub fn to_text(&self) -> AsciiBuf<80> {
        let mut out = AsciiBuf::new();
        let years = self.months / 12;
        let mons = self.months % 12;
        if years != 0 {
            push_unit(&mut out, years, "year", "years");
        }
        if mons != 0 {
            push_unit(&mut out, mons, "mon", "mons");
        }
        if self.days != 0 {
            push_unit(&mut out, self.days, "day", "days");
        }
        if self.micros != 0 || out.is_empty() {
            if !out.is_empty() {
                out.push(b' ');
            }
            write_time_us(&mut out, self.micros);
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
        assert_eq!(v.to_text().as_str(), "1 year 2 mons 3 days 04:05:06.7");

        assert_eq!(
            IntervalValue {
                months: 0,
                days: 0,
                micros: 0,
            }
            .to_text()
            .as_str(),
            "00:00:00",
        );
        assert_eq!(
            IntervalValue {
                months: 13,
                days: 1,
                micros: 0,
            }
            .to_text()
            .as_str(),
            "1 year 1 mon 1 day",
        );
        assert_eq!(
            IntervalValue {
                months: 0,
                days: 0,
                micros: 90_000_000,
            }
            .to_text()
            .as_str(),
            "00:01:30",
        );
        assert_eq!(
            IntervalValue {
                months: -1,
                days: 0,
                micros: 0,
            }
            .to_text()
            .as_str(),
            "-1 mon",
        );

        assert!(decode_interval(&[0u8; 8]).is_err());
    }

    #[test]
    fn interval_decode_basic() {
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
        assert_eq!(v.to_text().as_str(), "1 year 01:00:00");
    }

    #[test]
    fn interval_zero() {
        let body = [0u8; 16];
        let v = decode_interval(&body).unwrap();
        assert_eq!(v.to_text().as_str(), "00:00:00");
    }

    #[test]
    fn interval_truncated_body() {
        let body = [0u8; 10];
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
        assert_eq!(v.to_text().as_str(), "-01:00:00");
    }

    #[test]
    fn interval_trims_trailing_fraction_zeros() {
        let v = IntervalValue {
            months: 0,
            days: 0,
            micros: 1_000,
        };
        assert_eq!(v.to_text().as_str(), "00:00:00.001");
    }

    /// Longest output `interval_out` can produce, against the buffer bound
    #[test]
    fn extreme_datum_fits_its_buffer() {
        assert_eq!(
            IntervalValue {
                months: i32::MIN,
                days: i32::MIN,
                micros: i64::MIN,
            }
            .to_text()
            .as_str(),
            "-178956970 years -8 mons -2147483648 days -2562047788:00:54.775808",
        );
    }
}
