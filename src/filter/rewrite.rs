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

fn noop_body_crc(body: &[u8]) -> u32 {
    let Some(slot) = NOOP_BODY_CRC.get(body.len()) else {
        return crc32c::crc32c(body);
    };
    let cached = slot.load(Ordering::Relaxed);
    if cached >> 32 != 0 {
        return cached as u32;
    }
    let crc = crc32c::crc32c(body);
    slot.store(1 << 32 | u64::from(crc), Ordering::Relaxed);
    crc
}

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

    let crc = crc32c::crc32c_append(noop_body_crc(body), &record_bytes[..CRC_OFFSET]);
    record_bytes[CRC_OFFSET..CRC_OFFSET + 4].copy_from_slice(&crc.to_le_bytes());
    Ok(())
}

/// `XLOG_TBLSPC_CREATE`, PG `src/include/commands/tablespace.h`
pub const XLOG_TBLSPC_CREATE: u8 = 0x00;
/// `xl_tblspc_create_rec.ts_path` offset, past `ts_id`
const TS_PATH_OFFSET: usize = 4;
/// Body headers preceding main data: replication origin, top-level xid
const XLR_BLOCK_ID_ORIGIN: u8 = 253;
const XLR_BLOCK_ID_TOPLEVEL_XID: u8 = 252;

pub fn is_tablespace_create(rmid: u8, info: u8) -> bool {
    rmid == RmId::Tblspc as u8 && info & 0xF0 == XLOG_TBLSPC_CREATE
}

/// Point a `CREATE TABLESPACE` at an in-place directory: redo given an empty
/// path creates `pg_tblspc/<oid>` itself, while any source path is absent on
/// this host. Path only shrinks, so record length and successor LSNs hold.
/// Returns whether bytes changed; caller checks [`is_tablespace_create`]
pub fn blank_tablespace_path(record_bytes: &mut [u8]) -> bool {
    let Some(path) = main_data_range(record_bytes)
        .filter(|data| data.len() > TS_PATH_OFFSET)
        .map(|data| data.start + TS_PATH_OFFSET)
    else {
        return false;
    };
    if record_bytes[path] == 0 {
        return false;
    }
    record_bytes[path..].fill(0);
    let crc = compute_crc(record_bytes);
    record_bytes[CRC_OFFSET..CRC_OFFSET + 4].copy_from_slice(&crc.to_le_bytes());
    true
}

/// Main data of a record without block references, which always ends it
fn main_data_range(record_bytes: &[u8]) -> Option<std::ops::Range<usize>> {
    let mut at = X_LOG_RECORD_HEADER_SIZE;
    let start = loop {
        match *record_bytes.get(at)? {
            XLR_BLOCK_ID_DATA_SHORT => break at + MIN_SHORT_BODY,
            XLR_BLOCK_ID_DATA_LONG => break at + MIN_LONG_BODY,
            XLR_BLOCK_ID_ORIGIN => at += 3,
            XLR_BLOCK_ID_TOPLEVEL_XID => at += 5,
            _ => return None,
        }
    };
    (start <= record_bytes.len()).then_some(start..record_bytes.len())
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
    fn cached_body_crc_holds_across_headers() {
        for (body_len, prev) in [(300, 1), (300, 0xFEED), (9000, 2), (9000, 3)] {
            let mut bytes = build_record(body_len, prev);
            noop_replace(&mut bytes).unwrap();
            let crc = u32::from_le_bytes(bytes[CRC_OFFSET..CRC_OFFSET + 4].try_into().unwrap());
            assert_eq!(crc, compute_crc(&bytes), "body {body_len} prev {prev:#x}");
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

    /// `XLOG_TBLSPC_CREATE` body: optional preamble, SHORT main data
    fn tablespace_create(preamble: &[u8], path: &str) -> Vec<u8> {
        let mut data = 16385u32.to_le_bytes().to_vec();
        data.extend_from_slice(path.as_bytes());
        data.push(0);
        let mut body = preamble.to_vec();
        body.extend([XLR_BLOCK_ID_DATA_SHORT, data.len() as u8]);
        body.extend(data);
        let total = X_LOG_RECORD_HEADER_SIZE + body.len();
        let mut bytes = header_le(
            total as u32,
            7,
            0x42,
            XLOG_TBLSPC_CREATE,
            RmId::Tblspc as u8,
        );
        bytes.extend(body);
        let crc = compute_crc(&bytes);
        bytes[CRC_OFFSET..CRC_OFFSET + 4].copy_from_slice(&crc.to_le_bytes());
        bytes
    }

    #[test]
    fn tablespace_create_lands_in_place() {
        let origin = [XLR_BLOCK_ID_ORIGIN, 1, 0];
        let xid = [XLR_BLOCK_ID_TOPLEVEL_XID, 9, 0, 0, 0];
        for preamble in [&[][..], &origin, &xid] {
            let mut bytes = tablespace_create(preamble, "/srv/source/ts");
            let len = bytes.len();
            assert!(is_tablespace_create(bytes[17], bytes[16]));
            assert!(blank_tablespace_path(&mut bytes));
            assert_eq!(bytes.len(), len);
            let parsed = parse_record_from_bytes(&bytes, XLP_PAGE_MAGIC_PG15).unwrap();
            assert_eq!(parsed.header.crc32_hash, compute_crc(&bytes));
            assert_eq!(&parsed.main_data[..4], &16385u32.to_le_bytes());
            assert!(parsed.main_data[4..].iter().all(|b| *b == 0));
            // Already in place
            assert!(!blank_tablespace_path(&mut bytes));
        }
    }

    #[test]
    fn tablespace_rewrite_declines_other_shapes() {
        assert!(!is_tablespace_create(RmId::Tblspc as u8, 0x10));
        assert!(!is_tablespace_create(RmId::Heap as u8, XLOG_TBLSPC_CREATE));
        // Block reference where main data should start
        let mut bytes = tablespace_create(&[0], "/srv/ts");
        assert!(!blank_tablespace_path(&mut bytes));
        // Main data too short to hold a path
        let mut bytes = header_le(28, 0, 0, XLOG_TBLSPC_CREATE, RmId::Tblspc as u8);
        bytes.extend([XLR_BLOCK_ID_DATA_SHORT, 2, 1, 2]);
        assert!(!blank_tablespace_path(&mut bytes));
        // Header only
        let mut bytes = header_le(24, 0, 0, XLOG_TBLSPC_CREATE, RmId::Tblspc as u8);
        assert!(!blank_tablespace_path(&mut bytes));
        // LONG marker claiming more than the record holds
        let mut bytes = header_le(26, 0, 0, XLOG_TBLSPC_CREATE, RmId::Tblspc as u8);
        bytes.extend([XLR_BLOCK_ID_DATA_LONG, 0]);
        assert!(!blank_tablespace_path(&mut bytes));
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
