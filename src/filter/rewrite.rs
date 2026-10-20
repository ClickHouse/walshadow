//! `XLOG_NOOP` records covering the bytes of dropped records, so the
//! `xl_prev` chain and every LSN stay valid, with PG's CRC32C.
//!
//! CRC32C scheme matches PG `src/backend/access/transam/xlog.c`:
//! ```c
//! INIT_CRC32C(crc);
//! COMP_CRC32C(crc, ((char *) record) + SizeOfXLogRecord, xl_tot_len - SizeOfXLogRecord);
//! COMP_CRC32C(crc, (char *) record, offsetof(XLogRecord, xl_crc));
//! FIN_CRC32C(crc);
//! ```
//! body bytes first, then header[0..20] (xl_tot_len through xl_rmid + 2 pad).

use std::sync::LazyLock;
use std::sync::atomic::{AtomicU64, Ordering};

use walrus::pg::walparser::{
    RmId, X_LOG_RECORD_HEADER_SIZE, XLR_BLOCK_ID_DATA_LONG, XLR_BLOCK_ID_DATA_SHORT,
};

/// `XLogRecordHeader.info` for XLOG_NOOP (high nibble 0x20 in xlog.c).
pub const XLOG_NOOP: u8 = 0x20;

/// Offset of `xl_crc` in the 24-byte header.
pub const CRC_OFFSET: usize = 20;

/// SHORT main_data marker: 1-byte marker + 1-byte length.
const MIN_SHORT_BODY: usize = 2;
/// LONG main_data marker: 1-byte marker + 4-byte length.
const MIN_LONG_BODY: usize = 5;
/// SHORT length is 8-bit, so body must fit `2 + 255`.
const MAX_SHORT_BODY: usize = MIN_SHORT_BODY + u8::MAX as usize;

/// CRC32C over a NOOP body, by body length: the body is a function of its
/// length, so a hit leaves only the header to hash. High bit marks filled
static NOOP_BODY_CRC: [AtomicU64; 8192] = [const { AtomicU64::new(0) }; 8192];

/// CRC32C of a NOOP body of `len` bytes starting with `marker`, the rest
/// zero: cached below 8 KiB, extended over the zeros on a miss and past it
fn noop_body_crc(marker: &[u8], len: usize) -> u32 {
    let extend = || crc32c_zeros(crc32c::crc32c(marker), len - marker.len());
    let Some(slot) = NOOP_BODY_CRC.get(len) else {
        return extend();
    };
    let cached = slot.load(Ordering::Relaxed);
    if cached >> 32 != 0 {
        return cached as u32;
    }
    let crc = extend();
    slot.store(1 << 32 | u64::from(crc), Ordering::Relaxed);
    crc
}

/// Header and longest main-data marker: every later NOOP byte is zero
pub const NOOP_PREFIX: usize = X_LOG_RECORD_HEADER_SIZE + MIN_LONG_BODY;

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum RewriteError {
    #[error("record too small to NOOP-replace: xl_tot_len={0}, need >= {min}", min = X_LOG_RECORD_HEADER_SIZE + MIN_SHORT_BODY)]
    TooSmall(u32),
}

/// First bytes of an `XLOG_NOOP` of `total` logical bytes pointing back at
/// `xl_prev`; the bytes past its marker, and past `total`, are zero
pub fn noop_prefix(total: usize, xl_prev: u64) -> Result<[u8; NOOP_PREFIX], RewriteError> {
    let body_len = total.saturating_sub(X_LOG_RECORD_HEADER_SIZE);
    if body_len < MIN_SHORT_BODY {
        return Err(RewriteError::TooSmall(total as u32));
    }
    let mut p = [0u8; NOOP_PREFIX];
    p[0..4].copy_from_slice(&(total as u32).to_le_bytes());
    p[8..16].copy_from_slice(&xl_prev.to_le_bytes());
    p[16] = XLOG_NOOP;
    p[17] = RmId::Xlog as u8;
    let marker = if body_len <= MAX_SHORT_BODY {
        p[24] = XLR_BLOCK_ID_DATA_SHORT;
        p[25] = (body_len - MIN_SHORT_BODY) as u8;
        MIN_SHORT_BODY
    } else {
        p[24] = XLR_BLOCK_ID_DATA_LONG;
        p[25..29].copy_from_slice(&((body_len - MIN_LONG_BODY) as u32).to_le_bytes());
        MIN_LONG_BODY
    };
    let header = X_LOG_RECORD_HEADER_SIZE;
    let body = noop_body_crc(&p[header..header + marker], body_len);
    let crc = crc32c::crc32c_append(body, &p[..CRC_OFFSET]);
    p[CRC_OFFSET..CRC_OFFSET + 4].copy_from_slice(&crc.to_le_bytes());
    Ok(p)
}

/// Rewrite in place to an XLOG_NOOP of the same `xl_tot_len`, preserving
/// `xl_prev` so the chain stays intact.
///
/// `record_bytes` is the logical record (no page boundaries).
pub fn noop_replace(record_bytes: &mut [u8]) -> Result<(), RewriteError> {
    let xl_tot_len = u32::from_le_bytes(record_bytes[0..4].try_into().unwrap());
    if xl_tot_len as usize != record_bytes.len() {
        // Incomplete record, treat as malformed.
        return Err(RewriteError::TooSmall(xl_tot_len));
    }
    let xl_prev = u64::from_le_bytes(record_bytes[8..16].try_into().unwrap());
    let prefix = noop_prefix(record_bytes.len(), xl_prev)?;
    let n = NOOP_PREFIX.min(record_bytes.len());
    record_bytes.fill(0);
    record_bytes[..n].copy_from_slice(&prefix[..n]);
    Ok(())
}

/// Point a record header at `xl_prev`. CRC32C is linear and `xl_prev` ends
/// 4 bytes before the hashed bytes do, so the CRC moves by the CRC of the
/// change alone, without rehashing the body
pub fn patch_prev(header: &mut [u8; X_LOG_RECORD_HEADER_SIZE], xl_prev: u64) {
    let old = u64::from_le_bytes(header[8..16].try_into().unwrap());
    let mut delta = [0u8; 12];
    delta[..8].copy_from_slice(&(old ^ xl_prev).to_le_bytes());
    // From a zero register: `crc32c_append` inverts on entry and exit
    let shift = !crc32c::crc32c_append(!0, &delta);
    let crc = u32::from_le_bytes(header[CRC_OFFSET..].try_into().unwrap()) ^ shift;
    header[8..16].copy_from_slice(&xl_prev.to_le_bytes());
    header[CRC_OFFSET..].copy_from_slice(&crc.to_le_bytes());
}

/// PG-compatible CRC32C over a record: body then header[0..20].
pub fn compute_crc(record_bytes: &[u8]) -> u32 {
    let mut crc = 0u32;
    crc = crc32c::crc32c_append(crc, &record_bytes[X_LOG_RECORD_HEADER_SIZE..]);
    crc = crc32c::crc32c_append(crc, &record_bytes[..CRC_OFFSET]);
    crc
}

/// CRC32C of the bytes `crc` covers followed by `n` zero bytes, one table
/// step per set bit of `n`
pub fn crc32c_zeros(crc: u32, n: usize) -> u32 {
    let mut reg = !crc;
    let mut bits = n;
    while bits != 0 {
        let t = &ZEROS[bits.trailing_zeros() as usize];
        reg = t[0][reg as u8 as usize]
            ^ t[1][(reg >> 8) as u8 as usize]
            ^ t[2][(reg >> 16) as u8 as usize]
            ^ t[3][(reg >> 24) as usize];
        bits &= bits - 1;
    }
    !reg
}

/// GF(2) operator on a CRC32C register: column i is the image of bit i
type Operator = [u32; 32];

/// Operators appending 2^k zero bytes to a register, as one table per
/// register byte so applying one is four lookups
static ZEROS: LazyLock<Vec<[[u32; 256]; 4]>> = LazyLock::new(|| {
    // One zero bit shifts the reflected register right, folding in the
    // polynomial on a carry; three squarings make it one zero byte
    let mut op: Operator = std::array::from_fn(|i| if i == 0 { 0x82F6_3B78 } else { 1 << (i - 1) });
    for _ in 0..3 {
        op = square(&op);
    }
    (0..u32::BITS)
        .map(|_| {
            let tables =
                std::array::from_fn(|j| std::array::from_fn(|b| apply(&op, (b as u32) << (8 * j))));
            op = square(&op);
            tables
        })
        .collect()
});

fn apply(op: &Operator, v: u32) -> u32 {
    (0..32)
        .filter(|i| v >> i & 1 != 0)
        .fold(0, |sum, i| sum ^ op[i])
}

fn square(op: &Operator) -> Operator {
    std::array::from_fn(|i| apply(op, op[i]))
}

#[cfg(test)]
mod tests {
    use super::*;
    use walrus::pg::walparser::{XLP_PAGE_MAGIC_PG15, parse_record_from_bytes};

    fn header_le(xl_tot_len: u32, xl_xid: u32, xl_prev: u64, info: u8, rmid: u8) -> Vec<u8> {
        let mut v = Vec::with_capacity(X_LOG_RECORD_HEADER_SIZE);
        v.extend_from_slice(&xl_tot_len.to_le_bytes());
        v.extend_from_slice(&xl_xid.to_le_bytes());
        v.extend_from_slice(&xl_prev.to_le_bytes());
        v.push(info);
        v.push(rmid);
        v.push(0);
        v.push(0);
        v.extend_from_slice(&0u32.to_le_bytes()); // crc placeholder
        v
    }

    fn build_record(body_len: usize, xl_prev: u64) -> Vec<u8> {
        let total = X_LOG_RECORD_HEADER_SIZE + body_len;
        let mut bytes = header_le(total as u32, 99, xl_prev, 0, RmId::Heap as u8);
        bytes.extend(std::iter::repeat_n(0xAAu8, body_len));
        bytes
    }

    /// splitmix64 step: deterministic sample stream
    fn mix(x: &mut u64) -> u64 {
        *x = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let z = (*x ^ (*x >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        let z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Lengths around every marker switch and power of two, plus a spread
    fn lengths() -> impl Iterator<Item = usize> {
        let mut seed = 7;
        (0..20)
            .flat_map(|k| [(1 << k) - 1, 1 << k, (1 << k) + 1])
            .chain(250..270)
            .chain((0..2000).map(move |_| (mix(&mut seed) % (1 << 20)) as usize))
    }

    #[test]
    fn zero_extension_matches_hashing_the_zeros() {
        let zeros = vec![0u8; 1 << 20];
        let mut seed = 1;
        for n in lengths() {
            let crc = mix(&mut seed) as u32;
            assert_eq!(
                crc32c_zeros(crc, n),
                crc32c::crc32c_append(crc, &zeros[..n]),
                "n={n}"
            );
        }
    }

    #[test]
    fn noop_of_any_length_parses_with_a_valid_crc() {
        let mut seed = 2;
        // The second pass reads the cache
        let lengths = lengths().chain(lengths());
        for len in lengths.filter(|l| *l >= X_LOG_RECORD_HEADER_SIZE + MIN_SHORT_BODY) {
            let prev = mix(&mut seed);
            let mut bytes = build_record(len - X_LOG_RECORD_HEADER_SIZE, prev);
            noop_replace(&mut bytes).unwrap();
            let parsed = parse_record_from_bytes(&bytes, XLP_PAGE_MAGIC_PG15).unwrap();
            assert_eq!(parsed.header.info, XLOG_NOOP);
            assert_eq!(parsed.header.resource_manager_id, RmId::Xlog as u8);
            assert_eq!(parsed.header.prev_record_ptr, prev);
            assert_eq!(parsed.header.total_record_length as usize, len);
            assert!(parsed.blocks.is_empty());
            assert_eq!(parsed.header.crc32_hash, compute_crc(&bytes), "len={len}");
        }
    }

    #[test]
    fn patched_prev_keeps_the_crc_valid() {
        let mut seed = 3;
        for len in lengths().take(200).filter(|l| *l >= 2) {
            let mut bytes = build_record(len, mix(&mut seed));
            for b in &mut bytes[X_LOG_RECORD_HEADER_SIZE..] {
                *b = mix(&mut seed) as u8;
            }
            let crc = compute_crc(&bytes);
            bytes[CRC_OFFSET..CRC_OFFSET + 4].copy_from_slice(&crc.to_le_bytes());
            let prev = mix(&mut seed);
            let header: &mut [u8; X_LOG_RECORD_HEADER_SIZE] =
                (&mut bytes[..X_LOG_RECORD_HEADER_SIZE]).try_into().unwrap();
            patch_prev(header, prev);
            assert_eq!(bytes[8..16], prev.to_le_bytes());
            assert_eq!(
                bytes[CRC_OFFSET..CRC_OFFSET + 4],
                compute_crc(&bytes).to_le_bytes()
            );
        }
    }

    #[test]
    fn rejects_too_small_record() {
        let mut bytes = header_le(25, 0, 0, 0, RmId::Heap as u8);
        bytes.push(0); // body 1 byte
        assert!(matches!(
            noop_replace(&mut bytes),
            Err(RewriteError::TooSmall(_))
        ));
    }

    #[test]
    fn rejects_short_buffer() {
        let mut bytes = header_le(100, 0, 0, 0, RmId::Heap as u8); // claims 100 but only has 24
        assert!(matches!(
            noop_replace(&mut bytes),
            Err(RewriteError::TooSmall(_))
        ));
    }

    #[test]
    fn crc_matches_pg_when_record_is_unchanged() {
        let body_len = 16;
        let mut bytes = build_record(body_len, 0);
        // SHORT marker so the record is parseable
        bytes[X_LOG_RECORD_HEADER_SIZE] = XLR_BLOCK_ID_DATA_SHORT;
        bytes[X_LOG_RECORD_HEADER_SIZE + 1] = (body_len - MIN_SHORT_BODY) as u8;
        for b in bytes[X_LOG_RECORD_HEADER_SIZE + 2..].iter_mut() {
            *b = 0;
        }
        let crc = compute_crc(&bytes);
        bytes[CRC_OFFSET..CRC_OFFSET + 4].copy_from_slice(&crc.to_le_bytes());
        let parsed = parse_record_from_bytes(&bytes, XLP_PAGE_MAGIC_PG15).unwrap();
        assert_eq!(parsed.header.crc32_hash, crc);
    }
}
