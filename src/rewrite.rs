//! Replace a dropped record's body with an `XLOG_NOOP` of identical
//! `xl_tot_len` so the `xl_prev` chain stays valid, then recompute CRC32C.
//!
//! CRC32C scheme matches PG `src/backend/access/transam/xlog.c`:
//! ```c
//! INIT_CRC32C(crc);
//! COMP_CRC32C(crc, ((char *) record) + SizeOfXLogRecord, xl_tot_len - SizeOfXLogRecord);
//! COMP_CRC32C(crc, (char *) record, offsetof(XLogRecord, xl_crc));
//! FIN_CRC32C(crc);
//! ```
//! body bytes first, then header[0..20] (xl_tot_len through xl_rmid + 2 pad).

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

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum RewriteError {
    #[error("record too small to NOOP-replace: xl_tot_len={0}, need >= {min}", min = X_LOG_RECORD_HEADER_SIZE + MIN_SHORT_BODY)]
    TooSmall(u32),
}

/// Rewrite in place to an XLOG_NOOP of the same `xl_tot_len`, preserving
/// `xl_prev` so the chain stays intact. Recomputes CRC32C.
///
/// `record_bytes` is the logical record (no page boundaries); caller
/// scatters it back into the segment if the record spans pages.
pub fn noop_replace(record_bytes: &mut [u8]) -> Result<(), RewriteError> {
    let xl_tot_len = u32::from_le_bytes(record_bytes[0..4].try_into().unwrap());
    let total = xl_tot_len as usize;
    if total != record_bytes.len() {
        // Incomplete record, treat as malformed.
        return Err(RewriteError::TooSmall(xl_tot_len));
    }
    let body_len = total.saturating_sub(X_LOG_RECORD_HEADER_SIZE);
    if body_len < MIN_SHORT_BODY {
        return Err(RewriteError::TooSmall(xl_tot_len));
    }

    // Keep xl_tot_len (0..4) and xl_prev (8..16); rewrite the rest.
    record_bytes[4..8].fill(0); // xl_xid
    record_bytes[16] = XLOG_NOOP; // info
    record_bytes[17] = RmId::Xlog as u8; // rmid
    record_bytes[18] = 0; // pad
    record_bytes[19] = 0; // pad
    record_bytes[20..24].fill(0); // crc

    let body = &mut record_bytes[X_LOG_RECORD_HEADER_SIZE..];
    body.fill(0);
    if body_len <= MAX_SHORT_BODY {
        body[0] = XLR_BLOCK_ID_DATA_SHORT;
        body[1] = (body_len - MIN_SHORT_BODY) as u8;
    } else {
        // body_len >= 258 > MIN_LONG_BODY, so the LONG length always fits.
        body[0] = XLR_BLOCK_ID_DATA_LONG;
        body[1..5].copy_from_slice(&((body_len - MIN_LONG_BODY) as u32).to_le_bytes());
    }

    let crc = compute_crc(record_bytes);
    record_bytes[CRC_OFFSET..CRC_OFFSET + 4].copy_from_slice(&crc.to_le_bytes());
    Ok(())
}

/// PG-compatible CRC32C over a record: body then header[0..20].
pub fn compute_crc(record_bytes: &[u8]) -> u32 {
    let mut crc = 0u32;
    crc = crc32c::crc32c_append(crc, &record_bytes[X_LOG_RECORD_HEADER_SIZE..]);
    crc = crc32c::crc32c_append(crc, &record_bytes[..CRC_OFFSET]);
    crc
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

    #[test]
    fn noop_replaces_user_record_short_body() {
        let mut bytes = build_record(50, 0xdeadbeefcafebabe);
        let prev_orig = u64::from_le_bytes(bytes[8..16].try_into().unwrap());
        noop_replace(&mut bytes).unwrap();
        assert_eq!(
            u64::from_le_bytes(bytes[8..16].try_into().unwrap()),
            prev_orig
        );
        assert_eq!(bytes[16], XLOG_NOOP);
        assert_eq!(bytes[17], RmId::Xlog as u8);
        let parsed = parse_record_from_bytes(&bytes, XLP_PAGE_MAGIC_PG15).unwrap();
        assert_eq!(parsed.header.info, XLOG_NOOP);
        assert_eq!(parsed.header.total_record_length, bytes.len() as u32);
        assert_eq!(parsed.blocks.len(), 0);
        assert_eq!(parsed.main_data_len as usize, 50 - MIN_SHORT_BODY);
    }

    #[test]
    fn noop_replaces_user_record_long_body() {
        // body_len > 257 forces the LONG marker
        let mut bytes = build_record(1000, 0x42);
        noop_replace(&mut bytes).unwrap();
        let parsed = parse_record_from_bytes(&bytes, XLP_PAGE_MAGIC_PG15).unwrap();
        assert_eq!(parsed.header.info, XLOG_NOOP);
        assert_eq!(parsed.main_data_len as usize, 1000 - MIN_LONG_BODY);
    }

    #[test]
    fn noop_recomputes_crc() {
        let mut bytes = build_record(50, 0x1234);
        bytes[CRC_OFFSET..CRC_OFFSET + 4].copy_from_slice(&0xDEADBEEFu32.to_le_bytes());
        noop_replace(&mut bytes).unwrap();
        let crc = u32::from_le_bytes(bytes[CRC_OFFSET..CRC_OFFSET + 4].try_into().unwrap());
        assert_eq!(crc, compute_crc(&bytes));
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
