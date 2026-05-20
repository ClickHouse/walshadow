//! Record-cadence WAL segment walker.
//!
//! Driven by [`WalStream::push`](crate::wal_stream::WalStream::push) as
//! bytes arrive on the source replication socket. Each call to
//! [`extend`](StreamingWalker::extend) appends to the accumulating
//! segment buffer; [`try_next`](StreamingWalker::try_next) returns the
//! next completed record if its last byte has landed.
//!
//! Mirrors [`crate::segment::SegmentWalker`]'s page state machine —
//! same `Pending` cross-page stitching, same byte-range bookkeeping —
//! but lets the caller dispatch records the moment they complete rather
//! than at segment boundary. The buffer continues to accumulate so
//! [`DirSegmentSink`](crate::wal_stream::DirSegmentSink) sees a full
//! 16 MiB segment byte-for-byte at segment end, including any
//! in-place noop rewrites the caller applied via
//! [`rewrite_record`](StreamingWalker::rewrite_record).
//!
//! Cross-segment record straddling matches the per-segment walker:
//! `Pending` is dropped at segment reset
//! ([`take_segment`](StreamingWalker::take_segment)). Records whose
//! last byte sits in segment N+1 are lost; PG's WAL emits an
//! `XLOG_SWITCH` on operator-driven boundaries so the demo case is
//! aligned by construction.

use smallvec::smallvec;
use thiserror::Error;
use wal_rs::pg::walparser::{
    WAL_PAGE_SIZE, X_LOG_RECORD_ALIGNMENT, X_LOG_RECORD_HEADER_SIZE, XLP_LONG_HEADER,
    XLP_PAGE_MAGIC_PG15,
};

use crate::segment::ByteRanges;

const PAGE_SIZE: usize = WAL_PAGE_SIZE as usize;
const SHORT_HEADER_SIZE: usize = 20;
const LONG_HEADER_SIZE: usize = SHORT_HEADER_SIZE + 16;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum StreamingWalkError {
    #[error("page header at offset {0} has bad magic 0x{1:04x}")]
    BadPageMagic(usize, u16),
    #[error("page header at offset {0} has magic 0x{1:04x} (PG ≤ 14); walshadow requires PG 15+")]
    UnsupportedSourceVersion(usize, u16),
    #[error("page header at offset {0} declares invalid info flags 0x{1:04x}")]
    BadPageInfo(usize, u16),
    #[error("record at offset {offset} has zero xl_tot_len")]
    ZeroRecord { offset: usize },
    #[error("record at offset {offset} has xl_tot_len < {min}")]
    ShortRecord { offset: usize, min: usize },
}

/// One completed record's physical footprint within the current segment.
#[derive(Debug, Clone)]
pub struct CompletedRecord {
    /// Logical record bytes (header + body), exactly `xl_tot_len` long.
    pub logical_bytes: Vec<u8>,
    /// `(offset, len)` pairs the logical bytes occupy in the segment
    /// buffer, in order. `byte_ranges.iter().map(|(_, l)| l).sum() ==
    /// logical_bytes.len()`.
    pub byte_ranges: ByteRanges,
    /// First byte offset in the segment (= `byte_ranges[0].0`).
    pub start_offset: usize,
    /// Magic of the page where the record header lives. PG-15 vs PG-14
    /// FPI bit semantics key off this.
    pub page_magic: u16,
}

#[derive(Debug)]
struct Pending {
    start_offset: usize,
    total_len: Option<u32>,
    accumulated: Vec<u8>,
    byte_ranges: ByteRanges,
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

/// Streaming variant of [`SegmentWalker`](crate::segment::SegmentWalker).
/// Owns the accumulating segment buffer; records yield as soon as
/// their last byte arrives.
pub struct StreamingWalker {
    seg_size: usize,
    buf: Vec<u8>,
    /// Next byte to parse. Always `>= page_start`. May exceed
    /// `buf.len()` only while the walker is waiting for more bytes
    /// inside a multi-page record continuation (handled by stashing
    /// progress on `pending`).
    cursor: usize,
    /// Offset of the page the walker is currently parsing.
    page_start: usize,
    /// Magic of the current page; `0` before its header has been read.
    page_magic: u16,
    /// Offset where the data area starts on `page_start` (page_start +
    /// aligned header size). `0` until the header lands.
    page_data_start: usize,
    /// Cursor within the page once continuation bytes have been
    /// consumed; tracks where the next record header can begin.
    page_cursor: usize,
    /// Did we already attempt to consume continuation from the current
    /// page? Drives idempotency across [`try_next`] calls when bytes
    /// arrive in mid-page chunks.
    page_header_consumed: bool,
    pending: Option<Pending>,
    /// Set when a zero-padded page or zero-tail record marks
    /// end-of-valid-data in the current segment.
    done_in_segment: bool,
}

impl StreamingWalker {
    pub fn new(seg_size: usize) -> Self {
        let mut buf = Vec::new();
        buf.reserve_exact(seg_size);
        Self {
            seg_size,
            buf,
            cursor: 0,
            page_start: 0,
            page_magic: 0,
            page_data_start: 0,
            page_cursor: 0,
            page_header_consumed: false,
            pending: None,
            done_in_segment: false,
        }
    }

    /// Bytes accumulated in the current segment buffer.
    pub fn buffer_len(&self) -> usize {
        self.buf.len()
    }

    /// Read-only view of the buffer.
    pub fn buffer(&self) -> &[u8] {
        &self.buf
    }

    pub fn seg_size(&self) -> usize {
        self.seg_size
    }

    /// `true` once `extend` has accumulated a full segment.
    pub fn segment_full(&self) -> bool {
        self.buf.len() == self.seg_size
    }

    /// Append source bytes onto the buffer. Caller must not exceed
    /// `seg_size`; [`WalStream::push`](crate::wal_stream::WalStream::push)
    /// splits chunks at segment boundary before forwarding.
    pub fn extend(&mut self, bytes: &[u8]) {
        debug_assert!(self.buf.len() + bytes.len() <= self.seg_size);
        self.buf.extend_from_slice(bytes);
    }

    /// Apply a per-record rewrite (eg [`crate::rewrite::noop_replace`])
    /// back into the segment buffer at `byte_ranges`. Total `len` of
    /// `new_logical` must equal the original record length.
    pub fn rewrite_record(&mut self, byte_ranges: &ByteRanges, new_logical: &[u8]) {
        let mut cursor = 0;
        for &(off, len) in byte_ranges {
            self.buf[off..off + len].copy_from_slice(&new_logical[cursor..cursor + len]);
            cursor += len;
        }
        debug_assert_eq!(cursor, new_logical.len());
    }

    /// Drop the current segment buffer + reset walker state. Returns
    /// the completed segment bytes for downstream dispatch. Cross
    /// segment `Pending` is dropped (matches per-segment-walker
    /// semantics).
    pub fn take_segment(&mut self) -> Vec<u8> {
        let mut new_buf = Vec::new();
        new_buf.reserve_exact(self.seg_size);
        let out = std::mem::replace(&mut self.buf, new_buf);
        self.cursor = 0;
        self.page_start = 0;
        self.page_magic = 0;
        self.page_data_start = 0;
        self.page_cursor = 0;
        self.page_header_consumed = false;
        self.pending = None;
        self.done_in_segment = false;
        out
    }

    /// Yield the next completed record if available. Returns `None`
    /// when the walker needs more bytes; the caller's pattern is
    /// `while let Some(r) = walker.try_next()? { ... }` after every
    /// `extend`.
    pub fn try_next(&mut self) -> Option<Result<CompletedRecord, StreamingWalkError>> {
        if self.done_in_segment {
            return None;
        }
        loop {
            // Step 1: ensure we have a page header for `page_start`.
            if self.page_magic == 0 {
                match self.try_read_page_header() {
                    Ok(true) => {}
                    Ok(false) => return None, // need more bytes
                    Err(e) => {
                        self.done_in_segment = true;
                        return Some(Err(e));
                    }
                }
            }

            // Step 2: if a pending record is waiting on bytes from this
            // page, consume up to what's available.
            if let Some(p) = self.pending.as_mut()
                && let Some(total) = p.total_len
            {
                let remaining = total as usize - p.accumulated.len();
                let page_end = self.page_start + PAGE_SIZE;
                let avail_on_page = page_end.saturating_sub(self.page_cursor);
                let take_now = remaining
                    .min(avail_on_page)
                    .min(self.buf.len().saturating_sub(self.page_cursor));
                if take_now > 0 {
                    let chunk = &self.buf[self.page_cursor..self.page_cursor + take_now];
                    p.accumulated.extend_from_slice(chunk);
                    p.byte_ranges.push((self.page_cursor, take_now));
                    self.page_cursor += take_now;
                }
                if p.fully_loaded() {
                    let p = self.pending.take().unwrap();
                    return Some(Ok(CompletedRecord {
                        logical_bytes: p.accumulated,
                        byte_ranges: p.byte_ranges,
                        start_offset: p.start_offset,
                        page_magic: p.page_magic,
                    }));
                }
                // Need more bytes or need to cross page boundary.
                if self.page_cursor >= page_end {
                    // Page exhausted; roll over.
                    self.advance_to_next_page();
                    continue;
                }
                return None; // waiting on more bytes
            }

            // Step 3: pending without resolved total_len (partial
            // header straddling the page boundary). Same shape as
            // step 2 but without `remaining` cap until `total_len`
            // resolves.
            if let Some(p) = self.pending.as_mut() {
                let needed = X_LOG_RECORD_HEADER_SIZE - p.accumulated.len();
                let page_end = self.page_start + PAGE_SIZE;
                let avail_on_page = page_end.saturating_sub(self.page_cursor);
                let take_now = needed
                    .min(avail_on_page)
                    .min(self.buf.len().saturating_sub(self.page_cursor));
                if take_now > 0 {
                    let chunk = &self.buf[self.page_cursor..self.page_cursor + take_now];
                    p.accumulated.extend_from_slice(chunk);
                    p.byte_ranges.push((self.page_cursor, take_now));
                    self.page_cursor += take_now;
                    p.try_resolve_total_len();
                }
                // Loop back; resolved or still-resolving handled by
                // step 2 / next iteration.
                if p.fully_loaded() {
                    let p = self.pending.take().unwrap();
                    return Some(Ok(CompletedRecord {
                        logical_bytes: p.accumulated,
                        byte_ranges: p.byte_ranges,
                        start_offset: p.start_offset,
                        page_magic: p.page_magic,
                    }));
                }
                if self.page_cursor >= page_end {
                    self.advance_to_next_page();
                    continue;
                }
                if take_now == 0 {
                    return None;
                }
                continue;
            }

            // Step 4: try to read a fresh record at page_cursor.
            match self.try_read_record() {
                Ok(Some(r)) => return Some(Ok(r)),
                Ok(None) => {
                    if self.done_in_segment {
                        return None;
                    }
                    // Page exhausted or need more bytes; loop or wait.
                    let page_end = self.page_start + PAGE_SIZE;
                    if self.page_cursor >= page_end {
                        self.advance_to_next_page();
                        continue;
                    }
                    return None;
                }
                Err(e) => {
                    self.done_in_segment = true;
                    return Some(Err(e));
                }
            }
        }
    }

    /// Parse the page header at `self.page_start`. Returns
    /// `Ok(true)` on success, `Ok(false)` when the buffer lacks the
    /// header bytes yet, `Err` on a malformed header.
    fn try_read_page_header(&mut self) -> Result<bool, StreamingWalkError> {
        if self.buf.len() < self.page_start + SHORT_HEADER_SIZE {
            return Ok(false);
        }
        let buf = &self.buf[self.page_start..];
        let magic = u16::from_le_bytes(buf[0..2].try_into().unwrap());
        let info = u16::from_le_bytes(buf[2..4].try_into().unwrap());
        if magic == 0 && info == 0 {
            self.done_in_segment = true;
            return Ok(true);
        }
        if magic & 0xFF00 != 0xD100 {
            return Err(StreamingWalkError::BadPageMagic(self.page_start, magic));
        }
        if magic < XLP_PAGE_MAGIC_PG15 {
            return Err(StreamingWalkError::UnsupportedSourceVersion(
                self.page_start,
                magic,
            ));
        }
        const XLP_ALL_FLAGS: u16 = 0x0007;
        if info & !XLP_ALL_FLAGS != 0 {
            return Err(StreamingWalkError::BadPageInfo(self.page_start, info));
        }
        let is_long = (info & XLP_LONG_HEADER) != 0;
        let header_size = if is_long {
            LONG_HEADER_SIZE
        } else {
            SHORT_HEADER_SIZE
        };
        if self.buf.len() < self.page_start + header_size {
            return Ok(false);
        }
        let remaining_data_len = u32::from_le_bytes(buf[16..20].try_into().unwrap()) as usize;
        self.page_magic = magic;
        let data_start = self.page_start + align_up(header_size, X_LOG_RECORD_ALIGNMENT);
        self.page_data_start = data_start;
        self.page_cursor = data_start;
        self.page_header_consumed = false;

        // If no pending record is being stitched, a non-zero
        // remaining_data_len means continuation from a record we
        // didn't see (segment-start mid-record). Skip those bytes.
        if remaining_data_len > 0 && self.pending.is_none() {
            let page_end = self.page_start + PAGE_SIZE;
            let skip = remaining_data_len.min(page_end - data_start);
            self.page_cursor = align_up(data_start + skip, X_LOG_RECORD_ALIGNMENT);
            if remaining_data_len >= page_end - data_start {
                self.page_cursor = page_end;
            }
        }
        Ok(true)
    }

    /// Read one record at `self.page_cursor` if enough bytes are in
    /// the buffer. Returns `Ok(None)` if more bytes are needed or the
    /// page boundary was hit before a record landed.
    fn try_read_record(&mut self) -> Result<Option<CompletedRecord>, StreamingWalkError> {
        let page_end = self.page_start + PAGE_SIZE;
        // Skip record-alignment pad.
        self.page_cursor = align_up(self.page_cursor, X_LOG_RECORD_ALIGNMENT);
        if self.page_cursor >= page_end {
            return Ok(None);
        }
        let avail_on_page = page_end - self.page_cursor;
        let avail_in_buf = self.buf.len().saturating_sub(self.page_cursor);
        if avail_in_buf == 0 {
            return Ok(None);
        }
        if avail_on_page < X_LOG_RECORD_HEADER_SIZE {
            // Header doesn't fit on this page. Either:
            // * trailing zeros → end of valid data.
            // * non-zero partial header → buffer it as Pending,
            //   walker continues stitching on the next page.
            if avail_in_buf < avail_on_page {
                // Not enough bytes yet to decide; wait.
                return Ok(None);
            }
            let chunk_end = self.page_cursor + avail_on_page;
            if self.buf[self.page_cursor..chunk_end]
                .iter()
                .all(|&b| b == 0)
            {
                self.done_in_segment = true;
                return Ok(None);
            }
            let chunk = &self.buf[self.page_cursor..chunk_end];
            self.pending = Some(Pending {
                start_offset: self.page_cursor,
                total_len: None,
                accumulated: chunk.to_vec(),
                byte_ranges: smallvec![(self.page_cursor, chunk.len())],
                page_magic: self.page_magic,
            });
            self.page_cursor = page_end;
            return Ok(None);
        }
        if avail_in_buf < 4 {
            return Ok(None);
        }
        let xl_tot_len = u32::from_le_bytes(
            self.buf[self.page_cursor..self.page_cursor + 4]
                .try_into()
                .unwrap(),
        );
        if xl_tot_len == 0 {
            // Need full page-tail to confirm zero pad.
            let tail = (page_end - self.page_cursor).min(avail_in_buf);
            if tail < page_end - self.page_cursor {
                return Ok(None);
            }
            if self.buf[self.page_cursor..self.page_cursor + tail]
                .iter()
                .all(|&b| b == 0)
            {
                self.done_in_segment = true;
                return Ok(None);
            }
            return Err(StreamingWalkError::ZeroRecord {
                offset: self.page_cursor,
            });
        }
        let total = xl_tot_len as usize;
        if total < X_LOG_RECORD_HEADER_SIZE {
            return Err(StreamingWalkError::ShortRecord {
                offset: self.page_cursor,
                min: X_LOG_RECORD_HEADER_SIZE,
            });
        }

        let take_this_page = total.min(avail_on_page);
        let need_in_buf = take_this_page;
        if avail_in_buf < need_in_buf {
            return Ok(None);
        }
        let range = (self.page_cursor, take_this_page);
        if take_this_page == total {
            // Whole record sits on this page.
            let mut accumulated = Vec::with_capacity(total);
            accumulated
                .extend_from_slice(&self.buf[self.page_cursor..self.page_cursor + take_this_page]);
            self.page_cursor += take_this_page;
            return Ok(Some(CompletedRecord {
                logical_bytes: accumulated,
                byte_ranges: smallvec![range],
                start_offset: range.0,
                page_magic: self.page_magic,
            }));
        }
        // Record will continue onto subsequent pages.
        let mut accumulated = Vec::with_capacity(total);
        accumulated
            .extend_from_slice(&self.buf[self.page_cursor..self.page_cursor + take_this_page]);
        self.pending = Some(Pending {
            start_offset: self.page_cursor,
            total_len: Some(xl_tot_len),
            accumulated,
            byte_ranges: smallvec![range],
            page_magic: self.page_magic,
        });
        self.page_cursor = page_end;
        Ok(None)
    }

    fn advance_to_next_page(&mut self) {
        self.page_start += PAGE_SIZE;
        self.page_magic = 0;
        self.page_data_start = 0;
        self.page_cursor = self.page_start;
        self.page_header_consumed = false;
    }
}

fn align_up(n: usize, align: usize) -> usize {
    (n + align - 1) & !(align - 1)
}

#[cfg(test)]
mod tests {
    use super::*;
    use wal_rs::pg::walparser::RmId;

    const PG_SEG: usize = 16 * 1024 * 1024;

    fn header_le(xl_tot_len: u32) -> Vec<u8> {
        let mut v = Vec::with_capacity(X_LOG_RECORD_HEADER_SIZE);
        v.extend_from_slice(&xl_tot_len.to_le_bytes());
        v.extend_from_slice(&0u32.to_le_bytes());
        v.extend_from_slice(&0u64.to_le_bytes());
        v.push(0);
        v.push(RmId::Heap as u8);
        v.push(0);
        v.push(0);
        v.extend_from_slice(&0u32.to_le_bytes());
        v
    }

    fn build_single_page(body: &[u8], magic: u16, remaining_data_len: u32) -> Vec<u8> {
        let mut page = Vec::with_capacity(PAGE_SIZE);
        page.extend_from_slice(&magic.to_le_bytes());
        page.extend_from_slice(&XLP_LONG_HEADER.to_le_bytes());
        page.extend_from_slice(&1u32.to_le_bytes());
        page.extend_from_slice(&0u64.to_le_bytes());
        page.extend_from_slice(&remaining_data_len.to_le_bytes());
        page.extend_from_slice(&12345u64.to_le_bytes());
        page.extend_from_slice(&(PG_SEG as u32).to_le_bytes());
        page.extend_from_slice(&8192u32.to_le_bytes());
        page.extend_from_slice(&[0u8; 4]);
        page.extend_from_slice(body);
        page.resize(PAGE_SIZE, 0);
        page
    }

    #[test]
    fn yields_single_in_page_record_as_bytes_arrive() {
        let mut body = header_le(50);
        body.extend_from_slice(&[0u8; 26]);
        let page = build_single_page(&body, XLP_PAGE_MAGIC_PG15, 0);

        let mut walker = StreamingWalker::new(PAGE_SIZE);

        // First push: header + half of the body, walker has no record yet.
        walker.extend(&page[..40 + 30]);
        assert!(walker.try_next().is_none());

        // Second push: enough to complete the record.
        walker.extend(&page[40 + 30..40 + 50]);
        let r = walker.try_next().unwrap().unwrap();
        assert_eq!(r.logical_bytes.len(), 50);
        assert_eq!(r.byte_ranges.len(), 1);
        assert_eq!(r.start_offset, 40);
        assert_eq!(r.page_magic, XLP_PAGE_MAGIC_PG15);

        // Third push: zero-padded tail. Walker terminates this segment.
        walker.extend(&page[40 + 50..]);
        assert!(walker.try_next().is_none());
    }

    #[test]
    fn yields_record_byte_by_byte_drip_feed() {
        let mut body = header_le(80);
        body.extend_from_slice(&[0u8; 56]);
        let page = build_single_page(&body, XLP_PAGE_MAGIC_PG15, 0);

        let mut walker = StreamingWalker::new(PAGE_SIZE);
        for (i, b) in page.iter().take(40 + 80).enumerate() {
            walker.extend(std::slice::from_ref(b));
            let r = walker.try_next();
            if i < 40 + 80 - 1 {
                assert!(r.is_none(), "premature yield at byte {i}");
            } else {
                let rec = r.unwrap().unwrap();
                assert_eq!(rec.logical_bytes.len(), 80);
            }
        }
    }

    #[test]
    fn yields_two_records_one_page() {
        let mut body = header_le(50);
        body.extend_from_slice(&[0u8; 26]);
        body.extend_from_slice(&[0u8; 6]); // pad to 56 (next 8-aligned)
        let mut h2 = header_le(60);
        h2.extend_from_slice(&[0u8; 36]);
        body.extend_from_slice(&h2);
        let page = build_single_page(&body, XLP_PAGE_MAGIC_PG15, 0);

        let mut walker = StreamingWalker::new(PAGE_SIZE);
        walker.extend(&page);
        let r1 = walker.try_next().unwrap().unwrap();
        let r2 = walker.try_next().unwrap().unwrap();
        assert!(walker.try_next().is_none());
        assert_eq!(r1.logical_bytes.len(), 50);
        assert_eq!(r2.logical_bytes.len(), 60);
        assert_eq!(r2.start_offset - r1.start_offset, 56);
    }

    #[test]
    fn stitches_record_across_two_pages() {
        let record_total = 8200u32;
        let body_after_header = record_total as usize - X_LOG_RECORD_HEADER_SIZE;
        let mut record = header_le(record_total);
        record.extend_from_slice(&vec![0xAAu8; body_after_header]);

        let p1_data_area = PAGE_SIZE - 40;
        let p1_record_bytes = &record[..p1_data_area];
        let p1_remainder_bytes = &record[p1_data_area..]; // 48
        let page1 = build_single_page(p1_record_bytes, XLP_PAGE_MAGIC_PG15, 0);

        let mut page2 = Vec::with_capacity(PAGE_SIZE);
        page2.extend_from_slice(&XLP_PAGE_MAGIC_PG15.to_le_bytes());
        page2.extend_from_slice(&0u16.to_le_bytes());
        page2.extend_from_slice(&1u32.to_le_bytes());
        page2.extend_from_slice(&(PAGE_SIZE as u64).to_le_bytes());
        page2.extend_from_slice(&(p1_remainder_bytes.len() as u32).to_le_bytes());
        page2.extend_from_slice(&[0u8; 4]); // pad to 24 (short header)
        page2.extend_from_slice(p1_remainder_bytes);
        page2.resize(PAGE_SIZE, 0);

        let mut walker = StreamingWalker::new(PAGE_SIZE * 2);
        walker.extend(&page1);
        // Record not yet complete — half-page header not crossed.
        assert!(walker.try_next().is_none());
        walker.extend(&page2);
        let r = walker.try_next().unwrap().unwrap();
        assert!(walker.try_next().is_none());
        assert_eq!(r.logical_bytes.len(), record_total as usize);
        assert_eq!(r.byte_ranges.len(), 2);
        assert_eq!(r.byte_ranges[0].1, p1_data_area);
        assert_eq!(r.byte_ranges[1].1, p1_remainder_bytes.len());
    }

    #[test]
    fn rewrite_record_scatters_back_into_buffer() {
        let mut body = header_le(40);
        body.extend_from_slice(&[0u8; 16]);
        let page = build_single_page(&body, XLP_PAGE_MAGIC_PG15, 0);

        let mut walker = StreamingWalker::new(PAGE_SIZE);
        walker.extend(&page);
        let r = walker.try_next().unwrap().unwrap();
        let new_logical = vec![0xCCu8; 40];
        walker.rewrite_record(&r.byte_ranges, &new_logical);
        assert!(walker.buffer()[40..80].iter().all(|&b| b == 0xCC));
        // Bytes outside the record's byte_ranges remain page-header.
        assert_eq!(&walker.buffer()[0..2], &XLP_PAGE_MAGIC_PG15.to_le_bytes(),);
    }

    #[test]
    fn rejects_garbage_page_magic() {
        let mut walker = StreamingWalker::new(PAGE_SIZE);
        let mut page = vec![0u8; PAGE_SIZE];
        page[0] = 0xFF;
        page[1] = 0xFF;
        page[2] = 1;
        walker.extend(&page);
        let e = walker.try_next().unwrap().unwrap_err();
        assert!(matches!(e, StreamingWalkError::BadPageMagic(_, _)));
        // After error walker stays done.
        assert!(walker.try_next().is_none());
    }

    #[test]
    fn rejects_pre_pg15_magic() {
        let body = header_le(40);
        let page = build_single_page(&body, 0xD10D, 0);
        let mut walker = StreamingWalker::new(PAGE_SIZE);
        walker.extend(&page);
        let e = walker.try_next().unwrap().unwrap_err();
        assert!(matches!(
            e,
            StreamingWalkError::UnsupportedSourceVersion(_, 0xD10D)
        ));
    }

    #[test]
    fn zero_padded_page_terminates_segment_cleanly() {
        let page = build_single_page(&[], XLP_PAGE_MAGIC_PG15, 0);
        let mut walker = StreamingWalker::new(PAGE_SIZE);
        walker.extend(&page);
        assert!(walker.try_next().is_none());
    }

    #[test]
    fn take_segment_resets_state() {
        let mut body = header_le(40);
        body.extend_from_slice(&[0u8; 16]);
        let page = build_single_page(&body, XLP_PAGE_MAGIC_PG15, 0);

        let mut walker = StreamingWalker::new(PAGE_SIZE);
        walker.extend(&page);
        let _r = walker.try_next().unwrap().unwrap();
        let taken = walker.take_segment();
        assert_eq!(taken.len(), PAGE_SIZE);
        assert_eq!(walker.buffer_len(), 0);
        assert!(!walker.segment_full());
        // Cursor / page state reset so a fresh segment starts clean.
        walker.extend(&page);
        let r2 = walker.try_next().unwrap().unwrap();
        assert_eq!(r2.start_offset, 40);
    }
}
