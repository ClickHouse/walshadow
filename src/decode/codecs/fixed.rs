//! Fixed-width datums with no CH counterpart, rendered as their typoutput text

use crate::ascii_buf::{AsciiArray, AsciiBuf};

/// `macaddr` as `macaddr_out` prints it
pub(crate) fn macaddr_to_text(b: &[u8; 6]) -> AsciiArray<17> {
    colon_hex(b)
}

/// `macaddr8` as `macaddr8_out` prints it
pub(crate) fn macaddr8_to_text(b: &[u8; 8]) -> AsciiArray<23> {
    colon_hex(b)
}

/// `N` must be `3 * b.len() - 1`
fn colon_hex<const N: usize>(b: &[u8]) -> AsciiArray<N> {
    let mut out = [b':'; N];
    for (i, byte) in b.iter().enumerate() {
        out[3 * i] = b"0123456789abcdef"[(byte >> 4) as usize];
        out[3 * i + 1] = b"0123456789abcdef"[(byte & 0xf) as usize];
    }
    AsciiArray::new(out)
}

/// Inverse of [`macaddr_to_text`] and [`macaddr8_to_text`], for `attmissingval` text
pub(crate) fn parse_macaddr<const N: usize>(text: &str) -> Option<[u8; N]> {
    let mut out = [0u8; N];
    let mut parts = text.split(':');
    for slot in &mut out {
        *slot = u8::from_str_radix(parts.next()?, 16).ok()?;
    }
    parts.next().is_none().then_some(out)
}

/// `ItemPointerData` as `(block, offset)`. `BlockIdData` stores high half first
pub(crate) fn decode_tid(b: &[u8; 6]) -> (u32, u16) {
    let hi = u16::from_le_bytes([b[0], b[1]]);
    let lo = u16::from_le_bytes([b[2], b[3]]);
    (
        (u32::from(hi) << 16) | u32::from(lo),
        u16::from_le_bytes([b[4], b[5]]),
    )
}

/// `tid` as `tidout` prints it
pub(crate) fn tid_to_text(block: u32, offset: u16) -> AsciiBuf<18> {
    let mut out = AsciiBuf::new();
    out.push(b'(');
    out.push_uint(u64::from(block), 1);
    out.push(b',');
    out.push_uint(u64::from(offset), 1);
    out.push(b')');
    out
}

/// Inverse of [`tid_to_text`], for `attmissingval` text
pub(crate) fn parse_tid(text: &str) -> Option<(u32, u16)> {
    let (block, offset) = text.strip_prefix('(')?.strip_suffix(')')?.split_once(',')?;
    Some((block.parse().ok()?, offset.parse().ok()?))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn macaddr_renders_lowercase_colon_hex() {
        let mac = [0x08, 0x00, 0x2b, 0x01, 0x02, 0xAB];
        assert_eq!(macaddr_to_text(&mac).as_str(), "08:00:2b:01:02:ab");
        assert_eq!(parse_macaddr("08:00:2b:01:02:ab"), Some(mac));
        let mac8 = [0x08, 0x00, 0x2b, 0xff, 0xfe, 0x01, 0x02, 0x03];
        assert_eq!(macaddr8_to_text(&mac8).as_str(), "08:00:2b:ff:fe:01:02:03");
        assert_eq!(parse_macaddr("08:00:2b:ff:fe:01:02:03"), Some(mac8));
        assert_eq!(parse_macaddr::<6>("08:00:2b:ff:fe:01:02:03"), None);
        assert_eq!(parse_macaddr::<8>("08:00:2b:01:02:ab"), None);
    }

    #[test]
    fn tid_joins_block_halves() {
        assert_eq!(decode_tid(&[1, 0, 2, 0, 3, 0]), (65538, 3));
        let (block, offset) = decode_tid(&[0xff; 6]);
        assert_eq!(tid_to_text(block, offset).as_str(), "(4294967295,65535)");
        assert_eq!(parse_tid("(4294967295,65535)"), Some((block, offset)));
        assert_eq!(parse_tid("(1,65536)"), None);
    }
}
