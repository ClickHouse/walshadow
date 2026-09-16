//! Time-of-day text shared by `interval`, `time` and `timetz`.

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timetz_text_renders_zone() {
        let micros = ((12 * 3600 + 34 * 60 + 56) as i64) * 1_000_000;
        assert_eq!(timetz_to_text(micros, -7200), "12:34:56+02");
        assert_eq!(timetz_to_text(micros, 0), "12:34:56+00");
        assert_eq!(timetz_to_text(micros, 19800), "12:34:56-05:30");
        assert_eq!(timetz_to_text(micros + 500_000, -7200), "12:34:56.5+02");
    }
}
