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
//! (`take_segment`). Records whose
//! last byte sits in segment N+1 are lost; PG's WAL emits an
//! `XLOG_SWITCH` on operator-driven boundaries so the demo case is
//! aligned by construction.

use smallvec::smallvec;
use wal_rs::pg::walparser::{X_LOG_RECORD_ALIGNMENT, X_LOG_RECORD_HEADER_SIZE};

use crate::segment::ByteRanges;
pub use crate::wal_page::WalkError;
use crate::wal_page::{PAGE_SIZE, PageHeaderParse, align_up, parse_page_header};

/// One completed record's physical footprint within the current segment.
///
/// For the overwhelmingly common single-page case the walker carries
/// no bytes at all — the caller reads them as a `&[u8]` slice of
/// `walker.buffer()[byte_ranges[0]]`. Cross-page records have already
/// been stitched (assembled across `byte_ranges` into one contiguous
/// view) by the walker, so `stitched_bytes` is `Some(Vec<u8>)`. The
/// [`logical_bytes`](CompletedRecord::logical_bytes) helper hides the
/// distinction.
#[derive(Debug, Clone)]
pub struct CompletedRecord {
    /// `Some` if the record straddled a page boundary and the walker
    /// had to materialise it into a contiguous Vec; `None` for the
    /// common single-page case where the bytes still sit inside the
    /// walker buffer at `byte_ranges[0]` and don't need copying.
    pub stitched_bytes: Option<Vec<u8>>,
    /// `(offset, len)` pairs the logical bytes occupy in the segment
    /// buffer, in order. Sum equals the record's `xl_tot_len`.
    pub byte_ranges: ByteRanges,
    /// First byte offset in the segment (= `byte_ranges[0].0`).
    pub start_offset: usize,
    /// Magic of the page where the record header lives. PG-15 vs PG-14
    /// FPI bit semantics key off this.
    pub page_magic: u16,
}

impl CompletedRecord {
    /// Hand back the record's logical bytes. For cross-page records
    /// this is a slice of the walker's stitched buffer; for
    /// single-page it's a slice of `walker_buf` at the record's
    /// `byte_ranges[0]`. Caller must pass `walker.buffer()`.
    pub fn logical_bytes<'a>(&'a self, walker_buf: &'a [u8]) -> &'a [u8] {
        if let Some(v) = &self.stitched_bytes {
            return v.as_slice();
        }
        let (off, len) = self.byte_ranges[0];
        &walker_buf[off..off + len]
    }

    /// Total record length (== `xl_tot_len`).
    pub fn total_len(&self) -> usize {
        self.byte_ranges.iter().map(|(_, l)| *l).sum()
    }
}

#[derive(Debug)]
struct Pending {
    start_offset: usize,
    total_len: Option<u32>,
    /// `(offset, len)` slices in [`StreamingWalker::buf`] the record's
    /// bytes occupy so far. Sole source of truth — the byte values
    /// themselves live in the walker buffer, never duplicated. Field
    /// `accumulated_len` mirrors `byte_ranges.iter().map(|(_, l)|
    /// l).sum()` so hot-path completion checks dodge an iter walk.
    byte_ranges: ByteRanges,
    accumulated_len: usize,
    page_magic: u16,
}

impl Pending {
    /// Read the record's `xl_tot_len` (first 4 bytes) once it's
    /// landed across `byte_ranges`. Walks ranges into `buf` rather
    /// than a duplicated `accumulated` Vec.
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

    /// Materialise the logical bytes the record's `byte_ranges` cover
    /// in `buf`. Called at completion to hand owned bytes to the
    /// caller; never invoked mid-stitch.
    fn materialise(&self, buf: &[u8]) -> Vec<u8> {
        let mut out = Vec::with_capacity(self.accumulated_len);
        for &(off, len) in &self.byte_ranges {
            out.extend_from_slice(&buf[off..off + len]);
        }
        out
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

    /// Append source bytes onto the buffer. May grow past `seg_size`
    /// while a record straddles the boundary; cross-seg `pending`
    /// (`xl_tot_len` exceeds bytes left in the active seg) needs the
    /// next seg's continuation bytes to complete + rewrite uniformly.
    /// [`WalStream`](crate::wal_stream::WalStream) calls
    /// [`truncate_first_segment`](Self::truncate_first_segment) once
    /// the spanning record completes & no pending remains in seg 0.
    pub fn extend(&mut self, bytes: &[u8]) {
        self.buf.extend_from_slice(bytes);
    }

    /// Buf-offset of the in-flight partial record, or `None` if no
    /// record straddles the parse frontier. Used to gate first-segment
    /// flush: pending whose `start_offset < seg_size` must complete
    /// before the seg can ship, so [`rewrite_record`](Self::rewrite_record)
    /// can apply the NOOP rewrite to bytes in both segs uniformly.
    pub fn pending_start_offset(&self) -> Option<usize> {
        self.pending.as_ref().map(|p| p.start_offset)
    }

    /// Drop the first `seg_size` bytes off the buffer, rebasing every
    /// walker offset (page cursor, pending byte ranges) by `-seg_size`.
    /// Pre: `buf.len() >= seg_size`, and any in-flight `pending` lives
    /// in `[seg_size, buf.len())`.
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
            // Walker advanced into seg 1 already (typical spanning
            // case: spanning record completed + more seg-1 records
            // emitted before flush). Rebase page state.
            self.page_start -= n;
            self.page_data_start = self.page_data_start.saturating_sub(n);
            self.page_cursor = self.page_cursor.saturating_sub(n);
            // page_magic describes the page at page_start, stays valid
            // post-shift.
        } else {
            // Walker still parking on seg 0 (zero-pad tail or hadn't
            // seen seg-1 bytes yet). Restart parse from new buf[0] so
            // seg 1's first-page header gets read fresh.
            self.page_start = 0;
            self.page_magic = 0;
            self.page_data_start = 0;
            self.page_cursor = 0;
        }
        self.cursor = self.cursor.saturating_sub(n);
        self.done_in_segment = false;
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

    /// In-place variant of [`rewrite_record`](Self::rewrite_record) for a
    /// single-page record whose `len` bytes are contiguous in the buffer
    /// at `off` (`stitched_bytes: None`). Runs `rewrite` directly over
    /// the segment buffer, skipping the copy-out + scatter-back the
    /// fragmented (cross-page) path needs.
    pub fn rewrite_record_in_place<E>(
        &mut self,
        off: usize,
        len: usize,
        rewrite: impl FnOnce(&mut [u8]) -> Result<(), E>,
    ) -> Result<(), E> {
        rewrite(&mut self.buf[off..off + len])
    }

    /// Reset walker state for the next segment. Reuses the existing
    /// 16 MiB buffer allocation (`Vec::clear` retains capacity) so
    /// segment-cadence flushes don't churn the allocator — under
    /// steady-state replication that's roughly `seg_size` per
    /// `archive_timeout` of saved commit charge on no-overcommit
    /// hosts. Cross-segment `Pending` is dropped (matches
    /// per-segment-walker semantics).
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

    /// Yield the next completed record if available. Returns `None`
    /// when the walker needs more bytes; the caller's pattern is
    /// `while let Some(r) = walker.try_next()? { ... }` after every
    /// `extend`.
    ///
    /// Single-page records are returned with `stitched_bytes: None`
    /// — the caller reads bytes via [`CompletedRecord::logical_bytes`]
    /// straight off [`buffer`](Self::buffer), no per-record alloc.
    /// Cross-page records carry an owned stitched Vec.
    pub fn try_next(&mut self) -> Option<Result<CompletedRecord, WalkError>> {
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
                // Loop back; resolved or still-resolving handled by
                // step 2 / next iteration.
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
    fn try_read_page_header(&mut self) -> Result<bool, WalkError> {
        let (magic, data_start, remaining_data_len) =
            match parse_page_header(&self.buf, self.page_start)? {
                // Header bytes haven't all arrived yet: wait for more.
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
    fn try_read_record(&mut self) -> Result<Option<CompletedRecord>, WalkError> {
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
            // Whole record sits on this page — no per-record alloc.
            // Caller reads bytes via `CompletedRecord::logical_bytes`
            // straight off `walker.buffer()`.
            self.page_cursor += take_this_page;
            return Ok(Some(CompletedRecord {
                stitched_bytes: None,
                byte_ranges: smallvec![range],
                start_offset: range.0,
                page_magic: self.page_magic,
            }));
        }
        // Record will continue onto subsequent pages. byte_ranges
        // remains the sole source of truth; no duplicate Vec.
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

        // First push: header + half of the body, walker has no record yet.
        walker.extend(&page[..40 + 30]);
        assert!(walker.try_next().is_none());

        // Second push: enough to complete the record.
        walker.extend(&page[40 + 30..40 + 50]);
        let r = walker.try_next().unwrap().unwrap();
        assert_eq!(r.total_len(), 50);
        assert_eq!(r.byte_ranges.len(), 1);
        assert_eq!(r.start_offset, 40);
        assert_eq!(r.page_magic, XLP_PAGE_MAGIC_PG15);
        assert!(r.stitched_bytes.is_none(), "single-page borrow path");

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
        // Record not yet complete — half-page header not crossed.
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
        // Stitched bytes match the byte_ranges concatenation off the
        // walker buffer, confirming `Pending.accumulated` was not
        // re-introduced.
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
        assert!(matches!(e, WalkError::BadPageMagic(_, _)));
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

    /// Spanning-record regression. Walker buf grows past `seg_size`
    /// so `pending` can complete across the boundary;
    /// `rewrite_record` scatters NOOP bytes back into both seg
    /// portions; `truncate_first_segment` drops seg-0 and rebases
    /// indices so subsequent records emit at seg-1-relative offsets.
    #[test]
    fn handles_record_spanning_segment_boundary() {
        // seg_size = one page; spanning record occupies seg-0's full
        // data area + 16 bytes of seg-1's data area.
        let overflow: usize = 16;
        let in_seg0 = PAGE_SIZE - 40;
        let record_total = in_seg0 + overflow;
        let body_after_header = record_total - X_LOG_RECORD_HEADER_SIZE;
        let mut record = header_le(record_total as u32);
        record.extend_from_slice(&vec![0xAAu8; body_after_header]);

        let seg0 = build_long_header_page(&record[..in_seg0], XLP_PAGE_MAGIC_PG15, 0, 0);
        // Seg-1's first page: long header with remaining_data_len =
        // overflow; body starts with continuation of the spanning
        // record + a trailing zero pad.
        let seg1_body = record[in_seg0..].to_vec();
        let seg1 = build_long_header_page(
            &seg1_body,
            XLP_PAGE_MAGIC_PG15,
            overflow as u32,
            PAGE_SIZE as u64,
        );

        let mut walker = StreamingWalker::new(PAGE_SIZE);
        walker.extend(&seg0);
        // Walker should be pending — record header read, body
        // overflowed seg-0's page. No completion yet.
        assert!(walker.try_next().is_none());
        assert!(walker.pending_start_offset().is_some());
        let pend_off = walker.pending_start_offset().unwrap();
        assert!(pend_off < PAGE_SIZE, "pending lives in seg-0");

        // Now extend seg-1. Walker should yield the spanning record
        // with byte_ranges across the boundary.
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

        // Rewrite must scatter back into the walker buf at the
        // record's byte_ranges, covering both seg portions. Mimic
        // noop_replace by writing a sentinel.
        let new_logical = vec![0xCCu8; record_total];
        let byte_ranges = r.byte_ranges.clone();
        walker.rewrite_record(&byte_ranges, &new_logical);
        for (off, len) in byte_ranges.iter().copied() {
            assert!(
                walker.buffer()[off..off + len].iter().all(|&b| b == 0xCC),
                "rewrite landed at off={off} len={len}",
            );
        }

        // Truncate seg-0; walker.buf shrinks; subsequent parsing
        // continues in what was seg-1.
        assert!(walker.buffer_len() >= PAGE_SIZE);
        walker.truncate_first_segment();
        assert_eq!(walker.buffer_len(), PAGE_SIZE);
        assert!(walker.pending_start_offset().is_none());
        // Remaining bytes are seg-1 bytes (page header + continuation
        // + zero pad). Sentinel bytes survive at the continuation.
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
        // Capacity reused across the segment boundary.
        assert_eq!(walker.buf.capacity(), cap_before);
        // Cursor / page state reset so a fresh segment starts clean.
        walker.extend(&page);
        let r2 = walker.try_next().unwrap().unwrap();
        assert_eq!(r2.start_offset, 40);
    }
}
