//! Synthetic WAL for property tests: real page headers, contrecords,
//! `xl_prev` chain and CRCs, over records the filter keeps or drops

use walrus::pg::walparser::{
    RmId, X_LOG_RECORD_ALIGNMENT, X_LOG_RECORD_HEADER_SIZE, XLP_FIRST_IS_CONT_RECORD,
    XLP_LONG_HEADER, XLP_PAGE_MAGIC_PG15, XLR_BLOCK_ID_DATA_LONG,
};

use crate::filter::rewrite::compute_crc;
use crate::source::wal_page::PAGE_SIZE;

/// Database whose user relation records the filter drops once targeted
pub const TARGET_DB: u32 = 5;

pub struct Wal {
    pub bytes: Vec<u8>,
    /// `(lsn, len, dropped)` of every record, in order
    pub records: Vec<(u64, usize, bool)>,
}

/// splitmix64 step
pub fn mix(x: &mut u64) -> u64 {
    *x = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let z = (*x ^ (*x >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    let z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// At least `segments` whole segments of WAL starting at LSN 0
pub fn wal(seed: u64, seg_size: usize, segments: usize) -> Wal {
    let mut seed = seed;
    let mut wal = Wal {
        bytes: Vec::new(),
        records: Vec::new(),
    };
    let mut prev = 0u64;
    while wal.bytes.len() < seg_size * segments {
        let roll = mix(&mut seed) % 100;
        let data = match roll {
            0..2 => 30_000 + mix(&mut seed) % 40_000,
            2..12 => 1_000 + mix(&mut seed) % 20_000,
            _ => mix(&mut seed) % 400,
        } as usize;
        let dropped = !mix(&mut seed).is_multiple_of(4);
        let mut rec = record(dropped, data, &mut seed);
        let lsn = place(&mut wal.bytes, seg_size, rec.len());
        rec[8..16].copy_from_slice(&prev.to_le_bytes());
        let crc = compute_crc(&rec);
        rec[20..24].copy_from_slice(&crc.to_le_bytes());
        write(&mut wal.bytes, seg_size, &rec);
        wal.records.push((lsn, rec.len(), dropped));
        prev = lsn;
    }
    wal
}

/// Heap insert into a user relation of [`TARGET_DB`], or a CLOG record the
/// filter keeps, with `data` bytes of main data
fn record(dropped: bool, data: usize, seed: &mut u64) -> Vec<u8> {
    let mut v = vec![0u8; X_LOG_RECORD_HEADER_SIZE];
    if dropped {
        v[17] = RmId::Heap as u8;
        v.extend_from_slice(&[0, 0, 0, 0]); // block 0, main fork, no data
        for field in [1663u32, TARGET_DB, 50_000, 0] {
            v.extend_from_slice(&field.to_le_bytes());
        }
    } else {
        v[17] = RmId::Clog as u8;
    }
    v.push(XLR_BLOCK_ID_DATA_LONG);
    v.extend_from_slice(&(data as u32).to_le_bytes());
    v.extend((0..data).map(|_| mix(seed) as u8));
    let len = v.len() as u32;
    v[0..4].copy_from_slice(&len.to_le_bytes());
    v
}

/// LSN a record starting after the bytes so far lands on
fn place(bytes: &mut Vec<u8>, seg_size: usize, len: usize) -> u64 {
    let aligned = bytes.len().next_multiple_of(X_LOG_RECORD_ALIGNMENT);
    bytes.resize(aligned, 0);
    if bytes.len().is_multiple_of(PAGE_SIZE) {
        page_header(bytes, seg_size, 0);
    }
    debug_assert!(len >= X_LOG_RECORD_HEADER_SIZE);
    bytes.len() as u64
}

fn write(bytes: &mut Vec<u8>, seg_size: usize, mut rec: &[u8]) {
    loop {
        let take = rec.len().min(PAGE_SIZE - bytes.len() % PAGE_SIZE);
        bytes.extend_from_slice(&rec[..take]);
        rec = &rec[take..];
        if rec.is_empty() {
            return;
        }
        page_header(bytes, seg_size, rec.len());
    }
}

fn page_header(bytes: &mut Vec<u8>, seg_size: usize, rem_len: usize) {
    let lsn = bytes.len() as u64;
    let long = bytes.len().is_multiple_of(seg_size);
    let mut info = if rem_len > 0 {
        XLP_FIRST_IS_CONT_RECORD
    } else {
        0
    };
    if long {
        info |= XLP_LONG_HEADER;
    }
    bytes.extend_from_slice(&XLP_PAGE_MAGIC_PG15.to_le_bytes());
    bytes.extend_from_slice(&info.to_le_bytes());
    bytes.extend_from_slice(&1u32.to_le_bytes());
    bytes.extend_from_slice(&lsn.to_le_bytes());
    bytes.extend_from_slice(&(rem_len as u32).to_le_bytes());
    if long {
        bytes.extend_from_slice(&12345u64.to_le_bytes());
        bytes.extend_from_slice(&(seg_size as u32).to_le_bytes());
        bytes.extend_from_slice(&(PAGE_SIZE as u32).to_le_bytes());
    }
    let aligned = bytes.len().next_multiple_of(X_LOG_RECORD_ALIGNMENT);
    bytes.resize(aligned, 0);
}
