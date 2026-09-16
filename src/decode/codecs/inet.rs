//! PG `inet` / `cidr`, rendered as `inet_out` text.
//!
//! On-disk `inet_struct` (utils/inet.h):
//!
//! ```text
//!   uint8 family   (2 = AF_INET, 3 = AF_INET6)
//!   uint8 bits     (netmask bits)
//!   uint8 ipaddr[nb]  (nb = 4 for AF_INET, 16 for AF_INET6)
//! ```
//!
//! PG wire format (`inet_send`) adds is_cidr flag + addr byte count, but those
//! are NOT on disk: is_cidr comes from the column type OID (INETOID vs
//! CIDROID), addr count is implied by family.

use super::{CodecError, TextBuf};

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
    pub fn to_text(&self) -> TextBuf<48> {
        let mut out = TextBuf::new();
        match self.family {
            PGSQL_AF_INET => {
                for (i, octet) in self.addr.iter().take(4).enumerate() {
                    if i > 0 {
                        out.push(b'.');
                    }
                    out.push_uint(u64::from(*octet), 1);
                }
            }
            PGSQL_AF_INET6 => write_ipv6(&mut out, &self.addr),
            _ => out.push(b'?'),
        }
        let max_bits = if self.family == PGSQL_AF_INET {
            32
        } else {
            128
        };
        if self.is_cidr || self.bits != max_bits {
            out.push(b'/');
            out.push_uint(u64::from(self.bits), 1);
        }
        out
    }
}

/// IPv6 matching PG `inet_net_ntop`: RFC 5952 canonical form (lower-case hex,
/// no per-group leading zeros, `::` collapses longest run of >= 2 zero groups)
fn write_ipv6<const N: usize>(out: &mut TextBuf<N>, bytes: &[u8]) {
    let Some(addr) = bytes.first_chunk::<16>() else {
        out.push(b'?');
        return;
    };
    let mut groups = [0u16; 8];
    for (group, raw) in groups.iter_mut().zip(addr.as_chunks::<2>().0) {
        *group = u16::from_be_bytes(*raw);
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
    let mut k = 0;
    while k < 8 {
        if Some(k) == best_start {
            out.push_str("::");
            k += best_len;
            continue;
        }
        if !out.is_empty() && !out.ends_with(':') {
            out.push(b':');
        }
        out.push_hex(groups[k]);
        k += 1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inet_ipv4_simple() {
        let body = [PGSQL_AF_INET, 32, 192, 168, 0, 1];
        let v = decode_inet(&body, false).unwrap();
        assert_eq!(v.family, PGSQL_AF_INET);
        assert_eq!(v.bits, 32);
        assert!(!v.is_cidr);
        assert_eq!(v.addr, [192, 168, 0, 1]);
        assert_eq!(v.to_text().as_str(), "192.168.0.1");
    }

    #[test]
    fn inet_ipv4_cidr() {
        let body = [PGSQL_AF_INET, 24, 10, 0, 0, 0];
        let v = decode_inet(&body, true).unwrap();
        assert!(v.is_cidr);
        assert_eq!(v.to_text().as_str(), "10.0.0.0/24");
    }

    #[test]
    fn inet_ipv4_with_short_mask() {
        let body = [PGSQL_AF_INET, 24, 192, 168, 0, 1];
        let v = decode_inet(&body, false).unwrap();
        assert_eq!(v.to_text().as_str(), "192.168.0.1/24");
    }

    #[test]
    fn inet_ipv6_loopback() {
        let mut body = vec![PGSQL_AF_INET6, 128];
        body.extend_from_slice(&[0u8; 15]);
        body.push(1);
        let v = decode_inet(&body, false).unwrap();
        assert_eq!(v.to_text().as_str(), "::1");
    }

    #[test]
    fn inet_ipv6_compressed_middle() {
        let mut body = vec![PGSQL_AF_INET6, 128];
        body.extend_from_slice(&[0xfe, 0x80, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x01]);
        let v = decode_inet(&body, false).unwrap();
        assert_eq!(v.to_text().as_str(), "fe80::1");
    }

    #[test]
    fn inet_rejects_unknown_family() {
        let body = [99u8, 32, 1, 2, 3, 4];
        assert!(matches!(
            decode_inet(&body, false),
            Err(CodecError::BadInet { .. })
        ));
    }

    #[test]
    fn inet_ipv4_truncated_addr() {
        let body = [PGSQL_AF_INET, 32, 1, 2];
        assert!(matches!(
            decode_inet(&body, false),
            Err(CodecError::Truncated { offset: 2, .. })
        ));
    }

    #[test]
    fn inet_truncated_header() {
        let body = [PGSQL_AF_INET];
        assert!(matches!(
            decode_inet(&body, false),
            Err(CodecError::Truncated { offset: 0, .. })
        ));
    }

    #[test]
    fn inet_unknown_family_text_renders_placeholder() {
        let v = InetValue {
            family: 99,
            bits: 0,
            is_cidr: false,
            addr: Vec::new(),
        };
        assert!(v.to_text().as_str().starts_with('?'));
    }

    #[test]
    fn inet_ipv6_full_expansion_no_collapse() {
        let mut body = vec![PGSQL_AF_INET6, 128];
        for i in 1..=8u16 {
            body.extend_from_slice(&i.to_be_bytes());
        }
        let v = decode_inet(&body, false).unwrap();
        assert_eq!(v.to_text().as_str(), "1:2:3:4:5:6:7:8");
    }

    /// Longest output `inet_out` can produce, against the buffer bound
    #[test]
    fn extreme_datum_fits_its_buffer() {
        let mut body = vec![PGSQL_AF_INET6, 128];
        body.extend_from_slice(&[0xff; 16]);
        let v = decode_inet(&body, true).unwrap();
        assert_eq!(
            v.to_text().as_str(),
            "ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff/128"
        );
    }
}
