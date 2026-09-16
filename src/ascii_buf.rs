//! Fixed-capacity ASCII buffer. `inet`, `interval`, `time`, `timetz` and
//! `timestamp` text all have a length bound, so their cells render off the
//! stack. Every write funnels through `push`, which takes ASCII only, keeping
//! the contents valid UTF-8 for [`AsciiBuf::as_str`]. A byte past the bound
//! drops rather than panics: each renderer's longest output is pinned by a
//! test.

use std::ops::Deref;

pub struct AsciiBuf<const N: usize> {
    buf: [u8; N],
    len: usize,
}

impl<const N: usize> AsciiBuf<N> {
    pub(crate) fn new() -> Self {
        Self {
            buf: [0; N],
            len: 0,
        }
    }

    /// Non-ASCII ignored so buffer stays valid UTF-8
    pub(crate) fn push(&mut self, byte: u8) {
        if byte.is_ascii()
            && let Some(slot) = self.buf.get_mut(self.len)
        {
            *slot = byte;
            self.len += 1;
        }
    }

    pub(crate) fn push_str(&mut self, s: &str) {
        for byte in s.bytes() {
            self.push(byte);
        }
    }

    /// Decimal, zero-padded up to `pad` digits
    pub(crate) fn push_uint(&mut self, value: u64, pad: usize) {
        let mut digits = [0u8; 20];
        let mut count = 0;
        let mut rest = value;
        loop {
            digits[count] = b'0' + (rest % 10) as u8;
            rest /= 10;
            count += 1;
            if rest == 0 {
                break;
            }
        }
        for _ in count..pad {
            self.push(b'0');
        }
        for digit in digits[..count].iter().rev() {
            self.push(*digit);
        }
    }

    pub(crate) fn push_int(&mut self, value: i64) {
        if value < 0 {
            self.push(b'-');
        }
        self.push_uint(value.unsigned_abs(), 1);
    }

    /// Lower-case hex without leading zeros, as `inet_net_ntop` prints a group
    pub(crate) fn push_hex(&mut self, value: u16) {
        let mut significant = false;
        for shift in [12, 8, 4, 0] {
            let nibble = (value >> shift) & 0xf;
            significant |= nibble != 0;
            if significant || shift == 0 {
                self.push(b"0123456789abcdef"[nibble as usize]);
            }
        }
    }

    pub fn as_str(&self) -> &str {
        unsafe { std::str::from_utf8_unchecked(&self.buf[..self.len]) }
    }
}

impl<const N: usize> Deref for AsciiBuf<N> {
    type Target = str;

    fn deref(&self) -> &str {
        self.as_str()
    }
}

/// Exactly `N` ASCII bytes, for renderers whose width never varies, so they
/// carry no length
pub struct AsciiArray<const N: usize>([u8; N]);

impl<const N: usize> AsciiArray<N> {
    pub(crate) fn new(bytes: [u8; N]) -> Self {
        debug_assert!(bytes.is_ascii());
        Self(bytes)
    }

    pub fn as_str(&self) -> &str {
        unsafe { std::str::from_utf8_unchecked(&self.0) }
    }
}

impl<const N: usize> Deref for AsciiArray<N> {
    type Target = str;

    fn deref(&self) -> &str {
        self.as_str()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ascii_buf_writes_ascii_forms() {
        let mut b = AsciiBuf::<16>::new();
        b.push_uint(7, 2);
        b.push(b':');
        b.push_int(-42);
        b.push_str(" x");
        b.push_hex(0x0fe0);
        assert_eq!(b.as_str(), "07:-42 xfe0");
    }

    #[test]
    fn ascii_buf_drops_writes_past_capacity() {
        let mut b = AsciiBuf::<4>::new();
        b.push_str("abcdef");
        assert_eq!(b.as_str(), "abcd");
    }

    #[test]
    fn ascii_buf_drops_non_ascii() {
        let mut b = AsciiBuf::<16>::new();
        b.push_str("a\u{e9}b");
        b.push(0xff);
        b.push(b'c');
        assert_eq!(b.as_str(), "abc");
    }

    #[test]
    fn ascii_array_reads_back_as_str() {
        assert_eq!(AsciiArray::new(*b"abc").as_str(), "abc");
    }

    #[test]
    fn ascii_buf_hex_keeps_a_zero_group() {
        let mut b = AsciiBuf::<4>::new();
        b.push_hex(0);
        assert_eq!(b.as_str(), "0");
    }
}
