//! PG `uuid` bytes in CH wire order.

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
}
