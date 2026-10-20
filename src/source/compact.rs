//! Filtered WAL with its zero runs elided. Dropped records leave NOOP bodies
//! of zeros, most of each segment; a compact copy keeps every other byte
//! and the length of each zero run, and expands back to the identical bytes.
//!
//! The payload is pairs of LEB128 lengths, `literal` then `zeros`, each
//! literal followed by its bytes, until the expanded length is reached.
//!
//! The shadow stream carries it too: a walreceiver wrapped by the shadow's
//! `walshadow.so` names [`STREAM_SLOT`] in `START_REPLICATION` and gets `'z'`
//! frames, each an XLogData header, the expanded length and the payload,
//! which the wrapper expands back into the `'w'` frame PostgreSQL reads.

use std::io;
use std::path::Path;

use walrus::pg::replication::stream::now_pg_microseconds;

/// Leads a compact segment file. A WAL segment starts with page magic
/// `0xD1xx`, so neither is mistaken for the other
pub const MAGIC: &[u8; 4] = b"WSZ1";

/// Slot a shadow's wrapped walreceiver names to ask for `'z'` frames. Kept
/// in step with `WS_COMPACT_SLOT` in `pgext/walshadow.h`
pub const STREAM_SLOT: &str = "walshadow_compact";

/// Shorter zero runs stay literal: a pair costs at least two bytes
const MIN_ZEROS: usize = 16;

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
#[error("compact WAL is corrupt at byte {0}")]
pub struct Corrupt(pub usize);

/// Append the compact form of `src` to `out`
pub fn encode(src: &[u8], out: &mut Vec<u8>) {
    let mut from = 0;
    while from < src.len() {
        let (start, end) = next_zeros(src, from).unwrap_or((src.len(), src.len()));
        put(out, (start - from) as u64);
        out.extend_from_slice(&src[from..start]);
        put(out, (end - start) as u64);
        from = end;
    }
}

/// Append `len` expanded bytes from `src` to `out`, consuming all of `src`
pub fn expand(src: &[u8], len: usize, out: &mut Vec<u8>) -> Result<(), Corrupt> {
    let end = out.len() + len;
    let mut at = 0;
    while out.len() < end {
        let literal = take(src, &mut at)?;
        let bytes = src
            .get(at..at + literal)
            .filter(|b| out.len() + b.len() <= end)
            .ok_or(Corrupt(at))?;
        out.extend_from_slice(bytes);
        at += literal;
        let zeros = take(src, &mut at)?;
        if out.len() + zeros > end {
            return Err(Corrupt(at));
        }
        out.resize(out.len() + zeros, 0);
    }
    if at != src.len() {
        return Err(Corrupt(at));
    }
    Ok(())
}

/// Contents of a filtered WAL file as written: a compact segment expanded,
/// anything else, such as a plain segment an older release wrote, as is
pub fn unpack(file: Vec<u8>, seg_size: usize) -> Result<Vec<u8>, Corrupt> {
    let Some(payload) = file.strip_prefix(MAGIC) else {
        return Ok(file);
    };
    let mut out = Vec::with_capacity(seg_size);
    expand(payload, seg_size, &mut out)?;
    Ok(out)
}

/// Append a `'z'` frame carrying `data` from `start_lsn`
pub fn encode_frame_into(out: &mut Vec<u8>, start_lsn: u64, server_wal_end: u64, data: &[u8]) {
    out.push(b'z');
    out.extend_from_slice(&start_lsn.to_be_bytes());
    out.extend_from_slice(&server_wal_end.to_be_bytes());
    out.extend_from_slice(&now_pg_microseconds().to_be_bytes());
    out.extend_from_slice(&(data.len() as u32).to_be_bytes());
    encode(data, out);
}

/// Compact segment file contents
pub fn segment_file(segment: &[u8], out: &mut Vec<u8>) {
    out.clear();
    out.extend_from_slice(MAGIC);
    encode(segment, out);
}

/// `restore_command` for a shadow fed from compact segments
pub fn restore(src: &Path, dst: &Path, seg_size: usize) -> io::Result<()> {
    let file = std::fs::read(src)?;
    let bytes = unpack(file, seg_size).map_err(|e| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("{}: {e}", src.display()),
        )
    })?;
    std::fs::write(dst, bytes)
}

/// First zero run of at least [`MIN_ZEROS`] at or after `from`, found a
/// word at a time
fn next_zeros(src: &[u8], from: usize) -> Option<(usize, usize)> {
    let word = |at: usize| u64::from_ne_bytes(src[at..at + 8].try_into().unwrap());
    let mut at = from.next_multiple_of(8);
    while at + 8 <= src.len() {
        if word(at) != 0 {
            at += 8;
            continue;
        }
        let mut end = at + 8;
        while end + 8 <= src.len() && word(end) == 0 {
            end += 8;
        }
        while end < src.len() && src[end] == 0 {
            end += 1;
        }
        let mut start = at;
        while start > from && src[start - 1] == 0 {
            start -= 1;
        }
        if end - start >= MIN_ZEROS {
            return Some((start, end));
        }
        at = end.next_multiple_of(8);
    }
    None
}

fn put(out: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        out.push(v as u8 | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

fn take(src: &[u8], at: &mut usize) -> Result<usize, Corrupt> {
    let mut v = 0u64;
    for shift in (0..64).step_by(7) {
        let b = *src.get(*at).ok_or(Corrupt(*at))?;
        *at += 1;
        v |= u64::from(b & 0x7F) << shift;
        if b < 0x80 {
            return usize::try_from(v).map_err(|_| Corrupt(*at));
        }
    }
    Err(Corrupt(*at))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pos::Pos;
    use crate::record::{CollectingRecordSink, CollectingSegmentSink};
    use crate::source::wal_gen::{TARGET_DB, mix, wal};
    use crate::source::wal_page::PAGE_SIZE;
    use crate::source::wal_stream::WalStream;

    const SEG: usize = 4 * PAGE_SIZE;

    /// Filtered synthetic WAL, NOOP runs and all
    async fn filtered(seed: u64) -> Vec<u8> {
        let wal = wal(seed, SEG, 8);
        let mut stream = WalStream::builder(1, SEG as u64, Pos::ZERO).unwrap();
        stream.filter_mut().set_target_db(TARGET_DB);
        let mut stream = stream.start();
        let mut segments = CollectingSegmentSink::default();
        stream
            .push(
                0,
                &wal.bytes,
                &mut CollectingRecordSink::default(),
                &mut segments,
            )
            .await
            .unwrap();
        segments.segments.into_iter().flat_map(|s| s.1).collect()
    }

    /// Cut points on and around page and segment boundaries, and anywhere
    fn cuts(len: usize, seed: &mut u64) -> Vec<usize> {
        let mut cuts: Vec<usize> = (0..len)
            .step_by(PAGE_SIZE)
            .flat_map(|p| [p, p + 1, p + 20, p + 40])
            .chain((0..64).map(|_| mix(seed) as usize % len))
            .filter(|c| *c <= len)
            .collect();
        cuts.push(len);
        cuts
    }

    #[tokio::test]
    async fn expands_any_slice_of_filtered_wal_to_the_same_bytes() {
        let mut seed = 9;
        for s in 0..3 {
            let wal = filtered(s).await;
            let mut compact = Vec::new();
            encode(&wal, &mut compact);
            assert!(
                compact.len() < wal.len() / 2,
                "{} of {}",
                compact.len(),
                wal.len()
            );
            let cuts = cuts(wal.len(), &mut seed);
            for (&a, &b) in cuts.iter().zip(cuts.iter().skip(5)) {
                let (a, b) = (a.min(b), a.max(b));
                compact.clear();
                encode(&wal[a..b], &mut compact);
                let mut out = vec![7];
                expand(&compact, b - a, &mut out).unwrap();
                assert!(out[1..] == wal[a..b], "slice {a:#X}..{b:#X}");
            }
        }
    }

    #[test]
    fn round_trips_bytes_of_any_zero_layout() {
        let mut seed = 11;
        for _ in 0..500 {
            let len = mix(&mut seed) as usize % 4000;
            let bytes: Vec<u8> = (0..len)
                .map(|i| {
                    if (i / 13) % 3 == 0 {
                        mix(&mut seed) as u8
                    } else {
                        0
                    }
                })
                .collect();
            let mut compact = Vec::new();
            encode(&bytes, &mut compact);
            let mut out = Vec::new();
            expand(&compact, len, &mut out).unwrap();
            assert_eq!(out, bytes);
            if let Some(cut) = compact.len().checked_sub(1).filter(|_| len > 0) {
                let mut out = Vec::new();
                assert!(expand(&compact[..cut], len, &mut out).is_err(), "truncated");
            }
        }
    }

    #[test]
    fn unpacks_compact_files_and_passes_others_through() {
        let segment: Vec<u8> = (0..SEG).map(|i| (i % 1000 < 40) as u8).collect();
        let mut file = Vec::new();
        segment_file(&segment, &mut file);
        assert!(file.len() < SEG / 10);
        assert_eq!(unpack(file.clone(), SEG).unwrap(), segment);
        assert_eq!(unpack(segment.clone(), SEG).unwrap(), segment);
        file.push(0);
        assert!(unpack(file, SEG).is_err(), "trailing bytes");
    }
}
