//! Time-of-day text shared by `interval`, `time` and `timetz`.

use crate::ascii_buf::AsciiBuf;

/// `HH:MM:SS`, plus a fraction less its trailing zeros when nonzero
pub(crate) fn write_time_us<const N: usize>(out: &mut AsciiBuf<N>, us: i64) {
    if us < 0 {
        out.push(b'-');
    }
    // unsigned so a corrupt datum's i64::MIN micros stays in range
    let us = us.unsigned_abs();
    out.push_uint(us / 3_600_000_000, 2);
    out.push(b':');
    out.push_uint(us / 60_000_000 % 60, 2);
    out.push(b':');
    out.push_uint(us / 1_000_000 % 60, 2);
    let mut frac = us % 1_000_000;
    if frac != 0 {
        out.push(b'.');
        let mut digits = 6;
        while frac.is_multiple_of(10) {
            frac /= 10;
            digits -= 1;
        }
        out.push_uint(frac, digits);
    }
}

pub(crate) fn format_time_us(us: i64) -> AsciiBuf<32> {
    let mut out = AsciiBuf::new();
    write_time_us(&mut out, us);
    out
}

/// PG `timetz_out`: time-of-day plus zone offset. PG stores zone as seconds
/// *west* of UTC (negative east), so displayed offset flips sign. `±HH`,
/// appends `:MM` then `:SS` only when nonzero
pub(crate) fn timetz_to_text(micros: i64, tz_seconds: i32) -> AsciiBuf<48> {
    let mut out = AsciiBuf::new();
    write_time_us(&mut out, micros);
    out.push(if tz_seconds > 0 { b'-' } else { b'+' });
    let abs = u64::from(tz_seconds.unsigned_abs());
    out.push_uint(abs / 3600, 2);
    let (mm, ss) = (abs % 3600 / 60, abs % 60);
    if mm != 0 || ss != 0 {
        out.push(b':');
        out.push_uint(mm, 2);
        if ss != 0 {
            out.push(b':');
            out.push_uint(ss, 2);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timetz_text_renders_zone() {
        let micros = ((12 * 3600 + 34 * 60 + 56) as i64) * 1_000_000;
        assert_eq!(timetz_to_text(micros, -7200).as_str(), "12:34:56+02");
        assert_eq!(timetz_to_text(micros, 0).as_str(), "12:34:56+00");
        assert_eq!(timetz_to_text(micros, 19800).as_str(), "12:34:56-05:30");
        assert_eq!(
            timetz_to_text(micros + 500_000, -7200).as_str(),
            "12:34:56.5+02"
        );
    }

    #[test]
    fn time_text_keeps_fraction_digits() {
        assert_eq!(format_time_us(0).as_str(), "00:00:00");
        assert_eq!(format_time_us(1).as_str(), "00:00:00.000001");
        assert_eq!(format_time_us(-90_000_000).as_str(), "-00:01:30");
        assert_eq!(format_time_us(100_000).as_str(), "00:00:00.1");
    }

    /// Longest output either renderer can produce, against the buffer bounds
    #[test]
    fn extreme_datums_fit_their_buffers() {
        assert_eq!(
            format_time_us(i64::MIN).as_str(),
            "-2562047788:00:54.775808"
        );
        assert_eq!(
            timetz_to_text(i64::MIN, i32::MIN).as_str(),
            "-2562047788:00:54.775808+596523:14:08"
        );
    }
}
