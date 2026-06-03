//! Byte-positioned WAL segment walker.
//!
//! `WalParser` from wal-rs surfaces parsed `XLogRecord`s but discards
//! where each record physically sat in the segment. The rewriter
//! needs that mapping so it can replace dropped records in place,
//! preserving every other record's `xl_prev` chain.
//!
//! Walks pages sequentially, accumulating continuations across page
//! boundaries. Yields `WalkedRecord { logical_bytes, byte_ranges }`
//! tuples — `logical_bytes` is the contiguous record (header + body, no
//! page-header interruptions), `byte_ranges` is where to write it back.

use smallvec::{SmallVec, smallvec};
use thiserror::Error;
use wal_rs::pg::walparser::{
    WAL_PAGE_SIZE, X_LOG_RECORD_ALIGNMENT, X_LOG_RECORD_HEADER_SIZE, XLP_LONG_HEADER,
    XLP_PAGE_MAGIC_PG15,
};

/// Inline-1 byte-range vector: records almost always live on one WAL
/// page, multi-range only when a record straddles the page boundary.
/// Allocated per walked record (millions per segment) so the inline
/// case skips a `Vec` heap alloc on the hot path.
pub type ByteRanges = SmallVec<[(usize, usize); 1]>;

const PAGE_SIZE: usize = WAL_PAGE_SIZE as usize;
const SHORT_HEADER_SIZE: usize = 20;
const LONG_HEADER_SIZE: usize = SHORT_HEADER_SIZE + 16;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum WalkError {
    #[error("page header at offset {0} has bad magic 0x{1:04x}")]
    BadPageMagic(usize, u16),
    #[error(
        "page header at offset {0} has magic 0x{1:04x} (PG ≤ 14); walshadow requires PG 15+ (FPI bit layout)"
    )]
    UnsupportedSourceVersion(usize, u16),
    #[error("page header at offset {0} declares invalid info flags 0x{1:04x}")]
    BadPageInfo(usize, u16),
    #[error("record at offset {offset} has zero xl_tot_len")]
    ZeroRecord { offset: usize },
    #[error("record at offset {offset} has xl_tot_len < {min}")]
    ShortRecord { offset: usize, min: usize },
    #[error("page at offset {0} ends mid-record but next page header missing")]
    Truncated(usize),
}

/// One walked record's physical footprint in the segment.
#[derive(Debug, Clone)]
pub struct WalkedRecord {
    /// Logical record bytes (header + body), exactly `xl_tot_len` long.
    pub logical_bytes: Vec<u8>,
    /// File-offset / length pairs the logical bytes occupy in the
    /// segment, in order. `byte_ranges.iter().map(|(_, l)| l).sum() == logical_bytes.len()`.
    pub byte_ranges: ByteRanges,
    /// First byte offset in the segment (= byte_ranges[0].0).
    pub start_offset: usize,
    /// Page magic from the page where the record header lives.
    pub page_magic: u16,
}

/// Iterate every complete record on a segment, in source order.
///
/// `bytes` is the raw segment file. Trailing zero pages (post-CHECKPOINT
/// padding) terminate iteration silently.
pub struct SegmentWalker<'a> {
    bytes: &'a [u8],
    /// Cursor within the current page's data area.
    cursor: usize,
    /// File offset of the current page's start.
    page_start: usize,
    /// Magic from the current page's header (for FPI bit interpretation
    /// of records starting on this page).
    page_magic: u16,
    /// Partial record currently being stitched.
    pending: Option<Pending>,
    /// Set once we've returned an Err or hit clean EOF.
    done: bool,
}

#[derive(Debug)]
struct Pending {
    /// File offset where the record header begins.
    start_offset: usize,
    /// `xl_tot_len` once we have all 24 header bytes; `None` until then
    /// (a record's header can itself straddle the page boundary).
    total_len: Option<u32>,
    /// Bytes accumulated so far.
    accumulated: Vec<u8>,
    /// Byte ranges spanned so far.
    byte_ranges: ByteRanges,
    /// Magic of the page the record header was read on.
    page_magic: u16,
}

impl Pending {
    fn try_resolve_total_len(&mut self) {
        if self.total_len.is_none() && self.accumulated.len() >= X_LOG_RECORD_HEADER_SIZE {
            let t = u32::from_le_bytes(self.accumulated[0..4].try_into().unwrap());
            self.total_len = Some(t);
        }
    }
    fn fully_loaded(&self) -> bool {
        match self.total_len {
            Some(t) => self.accumulated.len() == t as usize,
            None => false,
        }
    }
}

impl<'a> SegmentWalker<'a> {
    pub fn new(bytes: &'a [u8]) -> Self {
        Self {
            bytes,
            cursor: 0,
            page_start: 0,
            page_magic: 0,
            pending: None,
            done: false,
        }
    }

    fn read_page_header(&mut self) -> Result<Option<()>, WalkError> {
        if self.page_start + SHORT_HEADER_SIZE > self.bytes.len() {
            self.done = true;
            return Ok(None);
        }
        let buf = &self.bytes[self.page_start..];
        let magic = u16::from_le_bytes(buf[0..2].try_into().unwrap());
        let info = u16::from_le_bytes(buf[2..4].try_into().unwrap());
        if magic == 0 && info == 0 {
            // Zero page = end of valid data
            self.done = true;
            return Ok(None);
        }
        if magic & 0xFF00 != 0xD100 {
            return Err(WalkError::BadPageMagic(self.page_start, magic));
        }
        if magic < XLP_PAGE_MAGIC_PG15 {
            return Err(WalkError::UnsupportedSourceVersion(self.page_start, magic));
        }
        // Validate flags
        const XLP_ALL_FLAGS: u16 = 0x0007;
        if info & !XLP_ALL_FLAGS != 0 {
            return Err(WalkError::BadPageInfo(self.page_start, info));
        }
        let is_long = (info & XLP_LONG_HEADER) != 0;
        let header_size = if is_long {
            LONG_HEADER_SIZE
        } else {
            SHORT_HEADER_SIZE
        };
        if self.page_start + header_size > self.bytes.len() {
            return Err(WalkError::Truncated(self.page_start));
        }
        let remaining_data_len = u32::from_le_bytes(buf[16..20].try_into().unwrap()) as usize;
        self.page_magic = magic;

        let data_start = self.page_start + align_up(header_size, X_LOG_RECORD_ALIGNMENT);
        let page_end = self.page_start + PAGE_SIZE;
        if data_start > self.bytes.len() {
            return Err(WalkError::Truncated(self.page_start));
        }
        let page_data_area = page_end - data_start;

        // Handle continuation of a record from previous page. The
        // `remaining_data_len` is bytes still owed; if it exceeds this
        // page's data area, the record spans yet another page.
        if remaining_data_len > 0 {
            let contributes = remaining_data_len.min(page_data_area);
            self.consume_continuation(data_start, contributes)?;
            // Cursor: after the continuation bytes, aligned. If the
            // record didn't complete on this page, no more records on
            // this page either — jump cursor to page_end.
            if remaining_data_len > page_data_area {
                self.cursor = page_end;
            } else {
                self.cursor = align_up(data_start + remaining_data_len, X_LOG_RECORD_ALIGNMENT);
            }
        } else {
            // No continuation. If we still have pending state, drop it
            // (segment corrupt at boundary; be lenient at EOF).
            self.pending = None;
            self.cursor = data_start;
        }
        Ok(Some(()))
    }

    fn consume_continuation(&mut self, data_start: usize, len: usize) -> Result<(), WalkError> {
        let cont_end = data_start + len;
        if cont_end > self.bytes.len() {
            return Err(WalkError::Truncated(self.page_start));
        }
        // If no pending, the segment started mid-record (continuation
        // from a previous segment). Skip those bytes silently — they
        // belong to a record we never saw the header of.
        let p = match self.pending.as_mut() {
            Some(p) => p,
            None => return Ok(()),
        };
        let chunk = &self.bytes[data_start..cont_end];
        p.accumulated.extend_from_slice(chunk);
        p.byte_ranges.push((data_start, chunk.len()));
        p.try_resolve_total_len();
        Ok(())
    }

    /// Read one record starting at `self.cursor`. Returns `None` if the
    /// remainder of the page is zeros (end-of-valid-data marker).
    fn try_read_record_on_page(&mut self) -> Result<Option<WalkedRecord>, WalkError> {
        let page_end = self.page_start + PAGE_SIZE;
        // Skip alignment pad
        self.cursor = align_up(self.cursor, X_LOG_RECORD_ALIGNMENT);
        if self.cursor >= page_end {
            return Ok(None);
        }
        let avail = page_end - self.cursor;
        if avail < X_LOG_RECORD_HEADER_SIZE {
            // Header doesn't fit on this page. Either:
            //  * trailing zeros → end of valid data, done.
            //  * non-zero partial header → record continues on next
            //    page; buffer what we have and let `consume_continuation`
            //    finish stitching.
            if self.bytes[self.cursor..page_end].iter().all(|&b| b == 0) {
                self.done = true;
                return Ok(None);
            }
            let chunk = &self.bytes[self.cursor..page_end];
            self.pending = Some(Pending {
                start_offset: self.cursor,
                total_len: None,
                accumulated: chunk.to_vec(),
                byte_ranges: smallvec![(self.cursor, chunk.len())],
                page_magic: self.page_magic,
            });
            self.cursor = page_end;
            return Ok(None);
        }
        let xl_tot_len =
            u32::from_le_bytes(self.bytes[self.cursor..self.cursor + 4].try_into().unwrap());
        if xl_tot_len == 0 {
            // Zero header = post-WAL-switch padding / EOF
            if self.bytes[self.cursor..page_end].iter().all(|&b| b == 0) {
                self.done = true;
                return Ok(None);
            }
            return Err(WalkError::ZeroRecord {
                offset: self.cursor,
            });
        }
        let total = xl_tot_len as usize;
        if total < X_LOG_RECORD_HEADER_SIZE {
            return Err(WalkError::ShortRecord {
                offset: self.cursor,
                min: X_LOG_RECORD_HEADER_SIZE,
            });
        }

        let take_this_page = total.min(avail);
        let range = (self.cursor, take_this_page);
        let mut accumulated = Vec::with_capacity(total);
        accumulated.extend_from_slice(&self.bytes[self.cursor..self.cursor + take_this_page]);

        if take_this_page == total {
            // Whole record on this page
            self.cursor += take_this_page;
            return Ok(Some(WalkedRecord {
                logical_bytes: accumulated,
                byte_ranges: smallvec![range],
                start_offset: range.0,
                page_magic: self.page_magic,
            }));
        }

        // Record continues onto next page
        self.pending = Some(Pending {
            start_offset: self.cursor,
            total_len: Some(xl_tot_len),
            accumulated,
            byte_ranges: smallvec![range],
            page_magic: self.page_magic,
        });
        self.cursor = page_end; // force next iteration to read next page
        Ok(None)
    }
}

impl<'a> Iterator for SegmentWalker<'a> {
    type Item = Result<WalkedRecord, WalkError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }

        loop {
            let page_end = self.page_start + PAGE_SIZE;
            // Need to read a new page if cursor is at/past page boundary
            // or this is the very first call (cursor == 0 and page_magic == 0).
            if self.cursor >= page_end || self.page_magic == 0 {
                self.page_start = self.cursor.max(self.page_start);
                if self.cursor >= page_end {
                    self.page_start = page_end;
                }
                match self.read_page_header() {
                    Ok(Some(())) => {}
                    Ok(None) => return None,
                    Err(e) => {
                        self.done = true;
                        return Some(Err(e));
                    }
                }
                // After processing continuation, the pending may have
                // completed; if so, emit it now.
                if let Some(p) = self.pending.as_ref()
                    && p.fully_loaded()
                {
                    let p = self.pending.take().unwrap();
                    return Some(Ok(WalkedRecord {
                        logical_bytes: p.accumulated,
                        byte_ranges: p.byte_ranges,
                        start_offset: p.start_offset,
                        page_magic: p.page_magic,
                    }));
                }
            }

            match self.try_read_record_on_page() {
                Ok(Some(rec)) => return Some(Ok(rec)),
                Ok(None) if self.done => return None,
                Ok(None) => continue,
                Err(e) => {
                    self.done = true;
                    return Some(Err(e));
                }
            }
        }
    }
}

fn align_up(n: usize, align: usize) -> usize {
    (n + align - 1) & !(align - 1)
}

#[cfg(test)]
mod tests {
    use super::*;
    use wal_rs::pg::walparser::{RmId, WalParser};

    /// Build a page with `body` starting after a long header (40 byte
    /// prefix). Zero-pads to PAGE_SIZE.
    fn build_single_page(body: &[u8], magic: u16, remaining_data_len: u32) -> Vec<u8> {
        let mut page = Vec::with_capacity(PAGE_SIZE);
        page.extend_from_slice(&magic.to_le_bytes());
        page.extend_from_slice(&XLP_LONG_HEADER.to_le_bytes());
        page.extend_from_slice(&1u32.to_le_bytes()); // timeline
        page.extend_from_slice(&0u64.to_le_bytes()); // page_address
        page.extend_from_slice(&remaining_data_len.to_le_bytes());
        page.extend_from_slice(&12345u64.to_le_bytes()); // sysid
        page.extend_from_slice(&(16u32 * 1024 * 1024).to_le_bytes()); // seg_size
        page.extend_from_slice(&8192u32.to_le_bytes()); // xlog_block_size
        // 4 bytes pad to 40
        page.extend_from_slice(&[0u8; 4]);
        page.extend_from_slice(body);
        page.resize(PAGE_SIZE, 0);
        page
    }

    fn header_le(xl_tot_len: u32) -> Vec<u8> {
        let mut v = Vec::with_capacity(X_LOG_RECORD_HEADER_SIZE);
        v.extend_from_slice(&xl_tot_len.to_le_bytes());
        v.extend_from_slice(&0u32.to_le_bytes()); // xact
        v.extend_from_slice(&0u64.to_le_bytes()); // prev
        v.push(0); // info
        v.push(RmId::Heap as u8); // rmid
        v.push(0);
        v.push(0);
        v.extend_from_slice(&0u32.to_le_bytes()); // crc
        v
    }

    #[test]
    fn walks_single_in_page_record() {
        let mut body = header_le(50);
        body.extend_from_slice(&[0u8; 26]);
        let page = build_single_page(&body, XLP_PAGE_MAGIC_PG15, 0);

        let mut walker = SegmentWalker::new(&page);
        let r = walker.next().unwrap().unwrap();
        assert_eq!(r.logical_bytes.len(), 50);
        assert_eq!(r.byte_ranges.len(), 1);
        assert_eq!(r.start_offset, 40); // long header + 4 pad
        assert_eq!(r.page_magic, XLP_PAGE_MAGIC_PG15);
        // Walker terminates after zero-padded tail
        assert!(walker.next().is_none());
    }

    #[test]
    fn walks_two_records_on_one_page() {
        let mut body = header_le(50);
        body.extend_from_slice(&[0u8; 26]);
        // pad to 8-byte alignment (50 → 56)
        body.extend_from_slice(&[0u8; 6]);
        let mut h2 = header_le(60);
        h2.extend_from_slice(&[0u8; 36]);
        body.extend_from_slice(&h2);
        let page = build_single_page(&body, XLP_PAGE_MAGIC_PG15, 0);

        let mut walker = SegmentWalker::new(&page);
        let r1 = walker.next().unwrap().unwrap();
        let r2 = walker.next().unwrap().unwrap();
        assert_eq!(r1.logical_bytes.len(), 50);
        assert_eq!(r2.logical_bytes.len(), 60);
        assert_eq!(r1.start_offset + 56, r2.start_offset);
        assert!(walker.next().is_none());
    }

    #[test]
    fn walks_record_spanning_two_pages() {
        // Record header on page 1, body finishes on page 2.
        // Layout: long header (40) + record header (24) + body 60 bytes
        // page data area = 8192 - 40 = 8152 bytes
        // Make record = 8200 bytes so it overflows the page by 48 bytes.
        let record_total = 8200u32;
        let body_after_header = record_total as usize - X_LOG_RECORD_HEADER_SIZE;
        let mut record = header_le(record_total);
        record.extend_from_slice(&vec![0xAAu8; body_after_header]);

        // page 1: long header + the part of record that fits (8152 bytes)
        let p1_data_area = PAGE_SIZE - 40;
        let p1_record_bytes = &record[..p1_data_area];
        let p1_remainder_bytes = &record[p1_data_area..]; // 48 bytes
        let page1 = build_single_page(p1_record_bytes, XLP_PAGE_MAGIC_PG15, 0);

        // page 2: short header + remaining_data_len = 48
        let mut page2 = Vec::with_capacity(PAGE_SIZE);
        page2.extend_from_slice(&XLP_PAGE_MAGIC_PG15.to_le_bytes());
        page2.extend_from_slice(&0u16.to_le_bytes()); // no flags
        page2.extend_from_slice(&1u32.to_le_bytes()); // timeline
        page2.extend_from_slice(&(PAGE_SIZE as u64).to_le_bytes()); // page_address
        page2.extend_from_slice(&(p1_remainder_bytes.len() as u32).to_le_bytes());
        // short header = 20 bytes, then pad to 24
        page2.extend_from_slice(&[0u8; 4]);
        page2.extend_from_slice(p1_remainder_bytes);
        page2.resize(PAGE_SIZE, 0);

        let mut segment = Vec::with_capacity(PAGE_SIZE * 2);
        segment.extend_from_slice(&page1);
        segment.extend_from_slice(&page2);

        let mut walker = SegmentWalker::new(&segment);
        let r = walker.next().unwrap().unwrap();
        assert_eq!(r.logical_bytes.len(), record_total as usize);
        assert_eq!(r.byte_ranges.len(), 2);
        assert_eq!(r.byte_ranges[0].1, p1_data_area);
        assert_eq!(r.byte_ranges[1].1, p1_remainder_bytes.len());
        assert_eq!(r.logical_bytes, record);
        assert!(walker.next().is_none());
    }

    #[test]
    fn walks_terminates_on_zero_padded_page() {
        // Empty body, expect immediate termination
        let page = build_single_page(&[], XLP_PAGE_MAGIC_PG15, 0);
        let mut walker = SegmentWalker::new(&page);
        assert!(walker.next().is_none());
    }

    #[test]
    fn rejects_garbage_magic() {
        let mut page = vec![0u8; PAGE_SIZE];
        page[0] = 0xFF;
        page[1] = 0xFF;
        page[2] = 1; // info nonzero so it's not zero-page early-out
        let mut walker = SegmentWalker::new(&page);
        let e = walker.next().unwrap().unwrap_err();
        assert!(matches!(e, WalkError::BadPageMagic(_, _)));
    }

    /// PG ≤ 14 capture: magic 0xD10D (PG 14). Reject before the parser
    /// hits any FPI record, since wal-rs's FPI dispatch keys off
    /// `magic >= XLP_PAGE_MAGIC_PG15`.
    #[test]
    fn rejects_pre_pg15_magic() {
        let body = header_le(40);
        let page = build_single_page(&body, 0xD10D, 0);
        let mut walker = SegmentWalker::new(&page);
        let e = walker.next().unwrap().unwrap_err();
        assert!(
            matches!(e, WalkError::UnsupportedSourceVersion(_, 0xD10D)),
            "expected UnsupportedSourceVersion, got {e:?}"
        );
    }

    /// Boundaries the walker reports must match wal-rs `WalParser`'s
    /// boundaries on the same bytes. Cross-validated against real
    /// captured segments in `tests/filter_round_trip.rs`; this
    /// synthetic test just keeps the two structures linked.
    #[test]
    fn walker_record_boundary_matches_xl_tot_len() {
        let mut body = header_le(40);
        body.extend_from_slice(&[0u8; 16]);
        let mut h2 = header_le(72);
        h2.extend_from_slice(&[0u8; 48]);
        body.extend_from_slice(&h2);
        let page = build_single_page(&body, XLP_PAGE_MAGIC_PG15, 0);

        let mut walker = SegmentWalker::new(&page);
        let r1 = walker.next().unwrap().unwrap();
        let r2 = walker.next().unwrap().unwrap();
        assert_eq!(r1.logical_bytes.len(), 40);
        assert_eq!(r2.logical_bytes.len(), 72);
        let _ = WalParser::new(); // silence unused import
        let _ = RmId::Heap;
    }
}
