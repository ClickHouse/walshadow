//! Record-cadence WAL segment walker.
//!
//! Driven by [`WalStream::push`](crate::wal_stream::WalStream::push) as
//! bytes arrive on the replication socket. Mirrors
//! [`crate::segment::SegmentWalker`]'s page state machine (same `Pending`
//! cross-page stitching, same byte-range bookkeeping) but dispatches
//! records the moment they complete, not at segment boundary. Buffer
//! keeps accumulating so [`DirSegmentSink`](crate::wal_stream::DirSegmentSink)
//! sees a full 16 MiB segment byte-for-byte at segment end, including
//! in-place noop rewrites applied via
//! [`rewrite_record`](StreamingWalker::rewrite_record).
//!
//! Cross-segment straddling matches the per-segment walker: `Pending`
//! drops at segment reset, so records whose last byte sits in segment
//! N+1 are lost. PG emits `XLOG_SWITCH` on operator-driven boundaries,
//! aligning the demo case by construction.

use smallvec::smallvec;
use wal_rs::pg::walparser::{X_LOG_RECORD_ALIGNMENT, X_LOG_RECORD_HEADER_SIZE};

use crate::segment::ByteRanges;
pub use crate::wal_page::WalkError;
use crate::wal_page::{PAGE_SIZE, PageHeaderParse, align_up, parse_page_header};

/// One completed record's physical footprint within the current segment.
///
/// Single-page (common) carries no bytes: caller reads a slice off
/// `walker.buffer()` at `byte_ranges[0]`. Cross-page records are
/// stitched into `stitched_bytes`. [`logical_bytes`](CompletedRecord::logical_bytes)
/// hides the distinction.
#[derive(Debug, Clone)]
pub struct CompletedRecord {
    /// `Some` for cross-page (materialised contiguous Vec); `None` for
    /// single-page (bytes stay in walker buffer at `byte_ranges[0]`).
    pub stitched_bytes: Option<Vec<u8>>,
    /// `(offset, len)` in segment buffer, in order. Sum == `xl_tot_len`.
    pub byte_ranges: ByteRanges,
    pub start_offset: usize,
    /// PG-15 vs PG-14 FPI bit semantics key off this.
    pub page_magic: u16,
}

impl CompletedRecord {
    /// Caller must pass `walker.buffer()`.
    pub fn logical_bytes<'a>(&'a self, walker_buf: &'a [u8]) -> &'a [u8] {
        if let Some(v) = &self.stitched_bytes {
            return v.as_slice();
        }
        let (off, len) = self.byte_ranges[0];
        &walker_buf[off..off + len]
    }

    /// == `xl_tot_len`.
    pub fn total_len(&self) -> usize {
        self.byte_ranges.iter().map(|(_, l)| *l).sum()
    }
}

#[derive(Debug)]
struct Pending {
    start_offset: usize,
    total_len: Option<u32>,
    /// `(offset, len)` into [`StreamingWalker::buf`]; sole source of
    /// truth, byte values never duplicated. `accumulated_len` mirrors
    /// the range-len sum so hot-path completion checks dodge an iter walk.
    byte_ranges: ByteRanges,
    accumulated_len: usize,
    page_magic: u16,
}

impl Pending {
    /// Read `xl_tot_len` (first 4 bytes) once landed across `byte_ranges`,
    /// walking ranges into `buf` rather than a duplicated Vec.
    fn try_resolve_total_len(&mut self, buf: &[u8]) {
        if self.total_len.is_some() || self.accumulated_len < X_LOG_RECORD_HEADER_SIZE {
            return;
        }
        let mut hdr = [0u8; 4];
        let mut cursor = 0;
        for &(off, len) in &self.byte_ranges {
            if cursor >= 4 {
                break;
            }
            let take = (4 - cursor).min(len);
            hdr[cursor..cursor + take].copy_from_slice(&buf[off..off + take]);
            cursor += take;
        }
        self.total_len = Some(u32::from_le_bytes(hdr));
    }

    fn fully_loaded(&self) -> bool {
        match self.total_len {
            Some(t) => self.accumulated_len == t as usize,
            None => false,
        }
    }

    /// Materialise the `byte_ranges` from `buf` into an owned Vec.
    fn materialise(&self, buf: &[u8]) -> Vec<u8> {
        let mut out = Vec::with_capacity(self.accumulated_len);
        for &(off, len) in &self.byte_ranges {
            out.extend_from_slice(&buf[off..off + len]);
        }
        out
    }
}

/// Streaming variant of [`SegmentWalker`](crate::segment::SegmentWalker);
/// records yield as soon as their last byte arrives.
pub struct StreamingWalker {
    seg_size: usize,
    buf: Vec<u8>,
    /// Always `>= page_start`. May exceed `buf.len()` only while waiting
    /// on more bytes inside a multi-page continuation (progress stashed
    /// on `pending`).
    cursor: usize,
    page_start: usize,
    /// `0` before the current page's header is read.
    page_magic: u16,
    /// `page_start + aligned header size`; `0` until the header lands.
    page_data_start: usize,
    page_cursor: usize,
    pending: Option<Pending>,
    /// Set when a zero-padded page or zero-tail record marks
    /// end-of-valid-data in the segment.
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
            pending: None,
            done_in_segment: false,
        }
    }

    pub fn buffer_len(&self) -> usize {
        self.buf.len()
    }

    pub fn buffer(&self) -> &[u8] {
        &self.buf
    }

    pub fn seg_size(&self) -> usize {
        self.seg_size
    }

    pub fn segment_full(&self) -> bool {
        self.buf.len() == self.seg_size
    }

    /// May grow past `seg_size` while a record straddles the boundary:
    /// cross-seg `pending` needs the next seg's continuation bytes to
    /// complete + rewrite uniformly.
    /// [`WalStream`](crate::wal_stream::WalStream) calls
    /// [`truncate_first_segment`](Self::truncate_first_segment) once the
    /// spanning record completes & no pending remains in seg 0.
    pub fn extend(&mut self, bytes: &[u8]) {
        self.buf.extend_from_slice(bytes);
    }

    /// Gates first-segment flush: pending with `start_offset < seg_size`
    /// must complete before the seg ships, so
    /// [`rewrite_record`](Self::rewrite_record) applies the NOOP across
    /// both segs uniformly.
    pub fn pending_start_offset(&self) -> Option<usize> {
        self.pending.as_ref().map(|p| p.start_offset)
    }

    /// Drop the first `seg_size` bytes, rebasing every walker offset by
    /// `-seg_size`. Pre: `buf.len() >= seg_size`, any in-flight `pending`
    /// lives in `[seg_size, buf.len())`.
    pub fn truncate_first_segment(&mut self) {
        let n = self.seg_size;
        debug_assert!(self.buf.len() >= n);
        if let Some(p) = &self.pending {
            debug_assert!(p.start_offset >= n);
        }
        self.buf.drain(0..n);
        if let Some(p) = self.pending.as_mut() {
            p.start_offset -= n;
            for r in p.byte_ranges.iter_mut() {
                r.0 -= n;
            }
        }
        if self.page_start >= n {
            // Walker already in seg 1; rebase page state. page_magic
            // describes the page at page_start, stays valid post-shift.
            self.page_start -= n;
            self.page_data_start = self.page_data_start.saturating_sub(n);
            self.page_cursor = self.page_cursor.saturating_sub(n);
        } else {
            // Still parking on seg 0 (zero-pad tail or no seg-1 bytes
            // yet); restart parse from new buf[0] for a fresh header read.
            self.page_start = 0;
            self.page_magic = 0;
            self.page_data_start = 0;
            self.page_cursor = 0;
        }
        self.cursor = self.cursor.saturating_sub(n);
        self.done_in_segment = false;
    }

    /// Scatter `new_logical` back into the buffer at `byte_ranges`. Its
    /// length must equal the original record length.
    pub fn rewrite_record(&mut self, byte_ranges: &ByteRanges, new_logical: &[u8]) {
        let mut cursor = 0;
        for &(off, len) in byte_ranges {
            self.buf[off..off + len].copy_from_slice(&new_logical[cursor..cursor + len]);
            cursor += len;
        }
        debug_assert_eq!(cursor, new_logical.len());
    }

    /// In-place variant for a single-page record contiguous at `off`
    /// (`stitched_bytes: None`); skips the cross-page copy-out + scatter-back.
    pub fn rewrite_record_in_place<E>(
        &mut self,
        off: usize,
        len: usize,
        rewrite: impl FnOnce(&mut [u8]) -> Result<(), E>,
    ) -> Result<(), E> {
        rewrite(&mut self.buf[off..off + len])
    }

    /// Reset for the next segment. `Vec::clear` retains the 16 MiB
    /// capacity so segment-cadence flushes don't churn the allocator.
    /// Cross-segment `Pending` is dropped (per-segment-walker semantics).
    pub fn reset_segment(&mut self) {
        self.buf.clear();
        self.cursor = 0;
        self.page_start = 0;
        self.page_magic = 0;
        self.page_data_start = 0;
        self.page_cursor = 0;
        self.pending = None;
        self.done_in_segment = false;
    }

    /// Yield the next completed record, or `None` when more bytes are
    /// needed. Caller pattern: `while let Some(r) = walker.try_next()? {}`
    /// after every `extend`. Single-page records come back with
    /// `stitched_bytes: None` (read off [`buffer`](Self::buffer) via
    /// [`CompletedRecord::logical_bytes`], no per-record alloc); cross-page
    /// carry an owned stitched Vec.
    pub fn try_next(&mut self) -> Option<Result<CompletedRecord, WalkError>> {
        if self.done_in_segment {
            return None;
        }
        loop {
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

            // Pending record with resolved total_len: consume what's
            // available on this page.
            if let Some(p) = self.pending.as_mut()
                && let Some(total) = p.total_len
            {
                let remaining = total as usize - p.accumulated_len;
                let page_end = self.page_start + PAGE_SIZE;
                let avail_on_page = page_end.saturating_sub(self.page_cursor);
                let take_now = remaining
                    .min(avail_on_page)
                    .min(self.buf.len().saturating_sub(self.page_cursor));
                if take_now > 0 {
                    p.byte_ranges.push((self.page_cursor, take_now));
                    p.accumulated_len += take_now;
                    self.page_cursor += take_now;
                }
                if p.fully_loaded() {
                    let p = self.pending.take().unwrap();
                    let stitched = p.materialise(&self.buf);
                    return Some(Ok(CompletedRecord {
                        stitched_bytes: Some(stitched),
                        byte_ranges: p.byte_ranges,
                        start_offset: p.start_offset,
                        page_magic: p.page_magic,
                    }));
                }
                if self.page_cursor >= page_end {
                    self.advance_to_next_page();
                    continue;
                }
                return None; // waiting on more bytes
            }

            // Pending without resolved total_len: header straddles the
            // page boundary, so no `remaining` cap until it resolves.
            if let Some(p) = self.pending.as_mut() {
                let needed = X_LOG_RECORD_HEADER_SIZE - p.accumulated_len;
                let page_end = self.page_start + PAGE_SIZE;
                let avail_on_page = page_end.saturating_sub(self.page_cursor);
                let take_now = needed
                    .min(avail_on_page)
                    .min(self.buf.len().saturating_sub(self.page_cursor));
                if take_now > 0 {
                    p.byte_ranges.push((self.page_cursor, take_now));
                    p.accumulated_len += take_now;
                    self.page_cursor += take_now;
                    p.try_resolve_total_len(&self.buf);
                }
                if p.fully_loaded() {
                    let p = self.pending.take().unwrap();
                    let stitched = p.materialise(&self.buf);
                    return Some(Ok(CompletedRecord {
                        stitched_bytes: Some(stitched),
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

            // Fresh record at page_cursor.
            match self.try_read_record() {
                Ok(Some(r)) => return Some(Ok(r)),
                Ok(None) => {
                    if self.done_in_segment {
                        return None;
                    }
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

    /// `Ok(false)` when the buffer lacks the header bytes yet.
    fn try_read_page_header(&mut self) -> Result<bool, WalkError> {
        let (magic, data_start, remaining_data_len) =
            match parse_page_header(&self.buf, self.page_start)? {
                PageHeaderParse::ShortHeaderIncomplete | PageHeaderParse::LongHeaderIncomplete => {
                    return Ok(false);
                }
                PageHeaderParse::ZeroPage => {
                    self.done_in_segment = true;
                    return Ok(true);
                }
                PageHeaderParse::Valid {
                    magic,
                    data_start,
                    remaining_data_len,
                } => (magic, data_start, remaining_data_len),
            };
        self.page_magic = magic;
        self.page_data_start = data_start;
        self.page_cursor = data_start;

        // No pending + non-zero remaining_data_len means continuation
        // from an unseen record (segment-start mid-record); skip it.
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

    /// `Ok(None)` if more bytes are needed or the page boundary was hit
    /// before a record landed.
    fn try_read_record(&mut self) -> Result<Option<CompletedRecord>, WalkError> {
        let page_end = self.page_start + PAGE_SIZE;
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
            // Header doesn't fit on this page: trailing zeros → EOF;
            // non-zero partial header → buffer as Pending, stitch on
            // the next page.
            if avail_in_buf < avail_on_page {
                return Ok(None); // not enough bytes yet to decide
            }
            let chunk_end = self.page_cursor + avail_on_page;
            if self.buf[self.page_cursor..chunk_end]
                .iter()
                .all(|&b| b == 0)
            {
                self.done_in_segment = true;
                return Ok(None);
            }
            let chunk_len = chunk_end - self.page_cursor;
            self.pending = Some(Pending {
                start_offset: self.page_cursor,
                total_len: None,
                byte_ranges: smallvec![(self.page_cursor, chunk_len)],
                accumulated_len: chunk_len,
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
            // Need the full page-tail to confirm zero pad.
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
            return Err(WalkError::ZeroRecord {
                offset: self.page_cursor,
            });
        }
        let total = xl_tot_len as usize;
        if total < X_LOG_RECORD_HEADER_SIZE {
            return Err(WalkError::ShortRecord {
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
            // Whole record on this page; no per-record alloc.
            self.page_cursor += take_this_page;
            return Ok(Some(CompletedRecord {
                stitched_bytes: None,
                byte_ranges: smallvec![range],
                start_offset: range.0,
                page_magic: self.page_magic,
            }));
        }
        // Continues onto subsequent pages.
        self.pending = Some(Pending {
            start_offset: self.page_cursor,
            total_len: Some(xl_tot_len),
            byte_ranges: smallvec![range],
            accumulated_len: take_this_page,
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
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use wal_rs::pg::walparser::{RmId, XLP_LONG_HEADER, XLP_PAGE_MAGIC_PG15};

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
        build_long_header_page(body, magic, remaining_data_len, 0)
    }

    #[test]
    fn seg_size_reports_construction_value() {
        let w = StreamingWalker::new(PG_SEG);
        assert_eq!(w.seg_size(), PG_SEG);
    }

    #[test]
    fn yields_single_in_page_record_as_bytes_arrive() {
        let mut body = header_le(50);
        body.extend_from_slice(&[0u8; 26]);
        let page = build_single_page(&body, XLP_PAGE_MAGIC_PG15, 0);

        let mut walker = StreamingWalker::new(PAGE_SIZE);

        walker.extend(&page[..40 + 30]);
        assert!(walker.try_next().is_none());

        walker.extend(&page[40 + 30..40 + 50]);
        let r = walker.try_next().unwrap().unwrap();
        assert_eq!(r.total_len(), 50);
        assert_eq!(r.byte_ranges.len(), 1);
        assert_eq!(r.start_offset, 40);
        assert_eq!(r.page_magic, XLP_PAGE_MAGIC_PG15);
        assert!(r.stitched_bytes.is_none(), "single-page borrow path");

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
                assert_eq!(rec.total_len(), 80);
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
        assert_eq!(r1.total_len(), 50);
        assert_eq!(r2.total_len(), 60);
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
        assert!(walker.try_next().is_none());
        walker.extend(&page2);
        let r = walker.try_next().unwrap().unwrap();
        assert_eq!(r.total_len(), record_total as usize);
        assert_eq!(r.byte_ranges.len(), 2);
        assert_eq!(r.byte_ranges[0].1, p1_data_area);
        assert_eq!(r.byte_ranges[1].1, p1_remainder_bytes.len());
        assert!(
            r.stitched_bytes.is_some(),
            "cross-page records stitched into owned Vec",
        );
        // Stitched bytes match the byte_ranges concat off the walker
        // buffer, confirming `Pending.accumulated` not re-introduced.
        let stitched = r.stitched_bytes.unwrap();
        let expected: Vec<u8> = r
            .byte_ranges
            .iter()
            .flat_map(|(o, l)| walker.buffer()[*o..*o + *l].to_vec())
            .collect();
        assert_eq!(stitched, expected);
        assert!(walker.try_next().is_none());
    }

    #[test]
    fn rewrite_record_scatters_back_into_buffer() {
        let mut body = header_le(40);
        body.extend_from_slice(&[0u8; 16]);
        let page = build_single_page(&body, XLP_PAGE_MAGIC_PG15, 0);

        let mut walker = StreamingWalker::new(PAGE_SIZE);
        walker.extend(&page);
        let r = walker.try_next().unwrap().unwrap();
        let byte_ranges = r.byte_ranges.clone();
        drop(r);
        let new_logical = vec![0xCCu8; 40];
        walker.rewrite_record(&byte_ranges, &new_logical);
        assert!(walker.buffer()[40..80].iter().all(|&b| b == 0xCC));
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
        assert!(matches!(e, WalkError::BadPageMagic(_, _)));
        assert!(walker.try_next().is_none());
    }

    #[test]
    fn rejects_pre_pg15_magic() {
        let body = header_le(40);
        let page = build_single_page(&body, 0xD10D, 0);
        let mut walker = StreamingWalker::new(PAGE_SIZE);
        walker.extend(&page);
        let e = walker.try_next().unwrap().unwrap_err();
        assert!(matches!(e, WalkError::UnsupportedSourceVersion(_, 0xD10D)));
    }

    #[test]
    fn zero_padded_page_terminates_segment_cleanly() {
        let page = build_single_page(&[], XLP_PAGE_MAGIC_PG15, 0);
        let mut walker = StreamingWalker::new(PAGE_SIZE);
        walker.extend(&page);
        assert!(walker.try_next().is_none());
    }

    fn build_long_header_page(
        body: &[u8],
        magic: u16,
        remaining_data_len: u32,
        page_address: u64,
    ) -> Vec<u8> {
        let mut page = Vec::with_capacity(PAGE_SIZE);
        page.extend_from_slice(&magic.to_le_bytes());
        page.extend_from_slice(&XLP_LONG_HEADER.to_le_bytes());
        page.extend_from_slice(&1u32.to_le_bytes());
        page.extend_from_slice(&page_address.to_le_bytes());
        page.extend_from_slice(&remaining_data_len.to_le_bytes());
        page.extend_from_slice(&12345u64.to_le_bytes());
        page.extend_from_slice(&(PG_SEG as u32).to_le_bytes());
        page.extend_from_slice(&8192u32.to_le_bytes());
        page.extend_from_slice(&[0u8; 4]);
        page.extend_from_slice(body);
        page.resize(PAGE_SIZE, 0);
        page
    }

    /// Spanning-record regression: buf grows past `seg_size` so
    /// `pending` completes across the boundary; `rewrite_record` scatters
    /// NOOP into both seg portions; `truncate_first_segment` drops seg-0
    /// and rebases indices to seg-1-relative offsets.
    #[test]
    fn handles_record_spanning_segment_boundary() {
        // seg_size = one page; record fills seg-0's data area + 16 bytes
        // of seg-1's.
        let overflow: usize = 16;
        let in_seg0 = PAGE_SIZE - 40;
        let record_total = in_seg0 + overflow;
        let body_after_header = record_total - X_LOG_RECORD_HEADER_SIZE;
        let mut record = header_le(record_total as u32);
        record.extend_from_slice(&vec![0xAAu8; body_after_header]);

        let seg0 = build_long_header_page(&record[..in_seg0], XLP_PAGE_MAGIC_PG15, 0, 0);
        // Seg-1 page: remaining_data_len = overflow, body = spanning
        // continuation + zero pad.
        let seg1_body = record[in_seg0..].to_vec();
        let seg1 = build_long_header_page(
            &seg1_body,
            XLP_PAGE_MAGIC_PG15,
            overflow as u32,
            PAGE_SIZE as u64,
        );

        let mut walker = StreamingWalker::new(PAGE_SIZE);
        walker.extend(&seg0);
        assert!(walker.try_next().is_none());
        assert!(walker.pending_start_offset().is_some());
        let pend_off = walker.pending_start_offset().unwrap();
        assert!(pend_off < PAGE_SIZE, "pending lives in seg-0");

        walker.extend(&seg1);
        let r = walker.try_next().unwrap().unwrap();
        assert_eq!(r.total_len(), record_total);
        assert_eq!(r.byte_ranges.len(), 2);
        assert_eq!(r.byte_ranges[0].1, in_seg0);
        assert_eq!(r.byte_ranges[1].1, overflow);
        assert!(r.byte_ranges[0].0 < PAGE_SIZE, "first range in seg-0");
        assert!(
            r.byte_ranges[1].0 >= PAGE_SIZE,
            "second range in seg-1 past page header"
        );

        // Sentinel mimics noop_replace; must scatter into both seg portions.
        let new_logical = vec![0xCCu8; record_total];
        let byte_ranges = r.byte_ranges.clone();
        walker.rewrite_record(&byte_ranges, &new_logical);
        for (off, len) in byte_ranges.iter().copied() {
            assert!(
                walker.buffer()[off..off + len].iter().all(|&b| b == 0xCC),
                "rewrite landed at off={off} len={len}",
            );
        }

        assert!(walker.buffer_len() >= PAGE_SIZE);
        walker.truncate_first_segment();
        assert_eq!(walker.buffer_len(), PAGE_SIZE);
        assert!(walker.pending_start_offset().is_none());
        // Sentinel survives at the seg-1 continuation post-truncate.
        assert!(
            walker.buffer()[40..40 + overflow]
                .iter()
                .all(|&b| b == 0xCC)
        );
    }

    #[test]
    fn reset_segment_clears_state_and_keeps_capacity() {
        let mut body = header_le(40);
        body.extend_from_slice(&[0u8; 16]);
        let page = build_single_page(&body, XLP_PAGE_MAGIC_PG15, 0);

        let mut walker = StreamingWalker::new(PAGE_SIZE);
        walker.extend(&page);
        let _r = walker.try_next().unwrap().unwrap();
        assert_eq!(walker.buffer().len(), PAGE_SIZE);
        let cap_before = walker.buf.capacity();
        walker.reset_segment();
        assert_eq!(walker.buffer_len(), 0);
        assert!(!walker.segment_full());
        assert_eq!(walker.buf.capacity(), cap_before);
        walker.extend(&page);
        let r2 = walker.try_next().unwrap().unwrap();
        assert_eq!(r2.start_offset, 40);
    }
}
