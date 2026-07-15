//! `Authorization` header vs a PEERDB_PASSWORD-style shared secret,
//! mirroring PeerDB gateway behavior: unauthenticated when unset,
//! `Bearer ` prefix stripped, constant-time compare

use hyper::HeaderMap;

use crate::error::{Code, GrpcError};

pub fn require_auth(password: Option<&str>, headers: &HeaderMap) -> Result<(), GrpcError> {
    let Some(password) = password else {
        return Ok(());
    };
    let headers: Vec<_> = headers.get_all("authorization").iter().collect();
    let header = match headers.as_slice() {
        [] => {
            return Err(GrpcError::new(
                Code::Unauthenticated,
                "missing Authorization header",
            ));
        }
        [one] => *one,
        _ => {
            return Err(GrpcError::new(
                Code::Unauthenticated,
                "multiple Authorization headers supplied, request rejected",
            ));
        }
    };
    let value = header.as_bytes();
    let token = value.strip_prefix(b"Bearer ").unwrap_or(value);
    if ct_eq(token, password.as_bytes()) {
        Ok(())
    } else {
        Err(GrpcError::new(Code::Unauthenticated, "invalid token"))
    }
}

fn ct_eq(a: &[u8], b: &[u8]) -> bool {
    let mut diff = a.len() ^ b.len();
    for i in 0..a.len().max(b.len()) {
        let x = a.get(i).copied().unwrap_or(0);
        let y = b.get(i).copied().unwrap_or(0);
        diff |= usize::from(x ^ y);
    }
    diff == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ct_eq_basics() {
        assert!(ct_eq(b"secret", b"secret"));
        assert!(!ct_eq(b"secret", b"secret2"));
        assert!(!ct_eq(b"", b"secret"));
        assert!(ct_eq(b"", b""));
    }
}
