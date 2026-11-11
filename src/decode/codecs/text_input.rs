//! PG text output parsed back to stored datums, for values that arrive only
//! as text (fast defaults read over SQL). Accepts what `date_out`,
//! `time_out`, `timestamp_out`, `timestamptz_out` render under DateStyle ISO,
//! plus `uuid_out` and hex `bytea_out`; anything else is `None`

/// Days from 1970-01-01 to 2000-01-01, PG's date epoch
const PG_EPOCH_UNIX_DAYS: i64 = 10_957;
const US_PER_DAY: i64 = 86_400_000_000;

/// `date`: days since 2000-01-01; ±infinity as `i32::MAX`/`i32::MIN`
pub(crate) fn parse_date(text: &str) -> Option<i32> {
    match text {
        "infinity" => return Some(i32::MAX),
        "-infinity" => return Some(i32::MIN),
        _ => {}
    }
    let (body, bc) = strip_bc(text);
    let days = parse_ymd(body.as_bytes(), bc)?;
    i32::try_from(days).ok()
}

/// `time`: microseconds since midnight, `24:00:00` included
pub(crate) fn parse_time(text: &str) -> Option<i64> {
    let us = parse_hms(text.as_bytes())?;
    (us <= US_PER_DAY).then_some(us)
}

/// `timestamp`: microseconds since 2000-01-01 00:00:00; ±infinity as
/// `i64::MAX`/`i64::MIN`
pub(crate) fn parse_timestamp(text: &str) -> Option<i64> {
    parse_stamp(text, false)
}

/// `timestamptz`: microseconds since 2000-01-01 00:00:00 UTC, offset applied
pub(crate) fn parse_timestamptz(text: &str) -> Option<i64> {
    parse_stamp(text, true)
}

/// `uuid`: canonical 8-4-4-4-12 hex, either case, as stored bytes
pub(crate) fn parse_uuid(text: &str) -> Option<[u8; 16]> {
    let b = text.as_bytes();
    if b.len() != 36 || [8, 13, 18, 23].iter().any(|&i| b[i] != b'-') {
        return None;
    }
    let mut out = [0u8; 16];
    let mut hex = b.iter().copied().filter(|&c| c != b'-');
    for byte in &mut out {
        *byte = hex_nibble(hex.next()?)? << 4 | hex_nibble(hex.next()?)?;
    }
    Some(out)
}

/// `bytea` in `bytea_output = 'hex'` form (`\x0a1b`)
pub(crate) fn parse_bytea_hex(text: &str) -> Option<Vec<u8>> {
    let hex = text.strip_prefix("\\x")?.as_bytes();
    if hex.len() % 2 != 0 {
        return None;
    }
    hex.as_chunks::<2>()
        .0
        .iter()
        .map(|&[hi, lo]| Some(hex_nibble(hi)? << 4 | hex_nibble(lo)?))
        .collect()
}

fn parse_stamp(text: &str, with_zone: bool) -> Option<i64> {
    match text {
        "infinity" => return Some(i64::MAX),
        "-infinity" => return Some(i64::MIN),
        _ => {}
    }
    let (body, bc) = strip_bc(text);
    let (date, rest) = body.split_once(' ')?;
    let days = parse_ymd(date.as_bytes(), bc)?;
    let (time, offset_s) = if with_zone {
        let at = rest.rfind(['+', '-'])?;
        (&rest[..at], parse_offset(&rest[at..])?)
    } else {
        (rest, 0)
    };
    let us = parse_hms(time.as_bytes())?;
    if us >= US_PER_DAY {
        return None;
    }
    days.checked_mul(US_PER_DAY)?
        .checked_add(us)?
        .checked_sub(offset_s.checked_mul(1_000_000)?)
}

fn strip_bc(text: &str) -> (&str, bool) {
    match text.strip_suffix(" BC") {
        Some(body) => (body, true),
        None => (text, false),
    }
}

/// `YYYY-MM-DD` (4+ digit year) to days since 2000-01-01
fn parse_ymd(b: &[u8], bc: bool) -> Option<i64> {
    let mut parts = b.split(|&c| c == b'-');
    let (y, m, d) = (parts.next()?, parts.next()?, parts.next()?);
    if parts.next().is_some() || y.len() < 4 || m.len() != 2 || d.len() != 2 {
        return None;
    }
    let (y, m, d) = (digits(y)?, digits(m)?, digits(d)?);
    // Year 1 BC is astronomical year 0, and there is no year 0 AD
    if y == 0 {
        return None;
    }
    let y = if bc { 1 - y } else { y };
    if !(1..=12).contains(&m) || d < 1 || d > days_in_month(y, m) {
        return None;
    }
    Some(days_from_civil(y, m, d) - PG_EPOCH_UNIX_DAYS)
}

/// `HH:MM:SS[.ffffff]` to microseconds
fn parse_hms(b: &[u8]) -> Option<i64> {
    if b.len() < 8 || b[2] != b':' || b[5] != b':' {
        return None;
    }
    let (h, m, s) = (digits(&b[0..2])?, digits(&b[3..5])?, digits(&b[6..8])?);
    let frac = match &b[8..] {
        [] => 0,
        [b'.', f @ ..] if (1..=6).contains(&f.len()) => digits(f)? * 10i64.pow(6 - f.len() as u32),
        _ => return None,
    };
    if h > 24 || m > 59 || s > 59 {
        return None;
    }
    Some(((h * 60 + m) * 60 + s) * 1_000_000 + frac)
}

/// `±HH[:MM[:SS]]` to seconds east of UTC
fn parse_offset(t: &str) -> Option<i64> {
    let (sign, body) = match t.as_bytes().first()? {
        b'+' => (1, &t[1..]),
        b'-' => (-1, &t[1..]),
        _ => return None,
    };
    let mut secs = 0;
    for (i, part) in body.split(':').enumerate() {
        if i > 2 || part.len() != 2 {
            return None;
        }
        let v = digits(part.as_bytes())?;
        if i > 0 && v > 59 {
            return None;
        }
        secs += v * [3600, 60, 1][i];
    }
    Some(sign * secs)
}

fn digits(b: &[u8]) -> Option<i64> {
    if b.is_empty() || b.len() > 9 {
        return None;
    }
    b.iter().try_fold(0i64, |acc, &c| {
        c.is_ascii_digit().then(|| acc * 10 + i64::from(c - b'0'))
    })
}

fn hex_nibble(c: u8) -> Option<u8> {
    (c as char).to_digit(16).map(|v| v as u8)
}

fn days_in_month(y: i64, m: i64) -> i64 {
    match m {
        2 if y % 4 == 0 && (y % 100 != 0 || y % 400 == 0) => 29,
        2 => 28,
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    }
}

/// Proleptic Gregorian civil date to days since 1970-01-01 (Hinnant)
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let doy = (153 * ((m + 9) % 12) + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dates_count_from_pg_epoch() {
        assert_eq!(parse_date("2000-01-01"), Some(0));
        assert_eq!(parse_date("1999-12-31"), Some(-1));
        assert_eq!(parse_date("2024-02-29"), Some(8825));
        assert_eq!(parse_date("2026-08-27"), Some(9735));
        assert_eq!(parse_date("0044-03-15 BC"), Some(-746_117));
        assert_eq!(parse_date("infinity"), Some(i32::MAX));
        assert_eq!(parse_date("-infinity"), Some(i32::MIN));
        for bad in [
            "2023-02-29",
            "2026-13-01",
            "2026-1-01",
            "0000-01-01",
            "26-08-27",
            "x",
        ] {
            assert_eq!(parse_date(bad), None, "{bad}");
        }
    }

    #[test]
    fn times_keep_microseconds() {
        assert_eq!(parse_time("00:00:00"), Some(0));
        assert_eq!(parse_time("12:47:00.842306"), Some(46_020_842_306));
        assert_eq!(parse_time("00:00:00.5"), Some(500_000));
        assert_eq!(parse_time("24:00:00"), Some(US_PER_DAY));
        for bad in [
            "24:00:01",
            "12:60:00",
            "12:00:00.1234567",
            "12:00",
            "12:00:00.",
        ] {
            assert_eq!(parse_time(bad), None, "{bad}");
        }
    }

    #[test]
    fn timestamps_count_from_pg_epoch() {
        assert_eq!(parse_timestamp("2000-01-01 00:00:00"), Some(0));
        assert_eq!(
            parse_timestamp("2026-08-27 12:47:00.842306"),
            Some(841_150_020_842_306)
        );
        assert_eq!(parse_timestamp("1999-12-31 23:59:59.999999"), Some(-1));
        assert_eq!(parse_timestamp("infinity"), Some(i64::MAX));
        assert_eq!(parse_timestamp("2026-08-27 12:47:00+00"), None);
    }

    #[test]
    fn timestamptz_applies_offset() {
        let utc = Some(841_150_020_842_306);
        assert_eq!(parse_timestamptz("2026-08-27 12:47:00.842306+00"), utc);
        assert_eq!(parse_timestamptz("2026-08-27 18:17:00.842306+05:30"), utc);
        assert_eq!(parse_timestamptz("2026-08-27 09:47:00.842306-03"), utc);
        assert_eq!(
            parse_timestamptz("2000-01-01 00:00:00+00:00:30"),
            Some(-30_000_000)
        );
        assert_eq!(parse_timestamptz("-infinity"), Some(i64::MIN));
        for bad in [
            "2026-08-27 12:47:00",
            "2026-08-27 12:47:00+0",
            "2026-08-27 12:47:00+00:60",
        ] {
            assert_eq!(parse_timestamptz(bad), None, "{bad}");
        }
    }

    #[test]
    fn uuid_and_bytea_parse_to_bytes() {
        let u = parse_uuid("A0EEBC99-9c0b-4ef8-bb6d-6bb9bd380a11").unwrap();
        assert_eq!(u[0], 0xa0);
        assert_eq!(u[15], 0x11);
        assert_eq!(parse_uuid("a0eebc999c0b4ef8bb6d6bb9bd380a11"), None);
        assert_eq!(parse_bytea_hex("\\x00ff1A"), Some(vec![0, 0xff, 0x1a]));
        assert_eq!(parse_bytea_hex("\\x"), Some(vec![]));
        assert_eq!(parse_bytea_hex("\\x0"), None);
        assert_eq!(parse_bytea_hex("abc"), None);
    }
}
