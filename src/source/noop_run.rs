//! Merge each run of dropped records starting on one WAL page into one
//! `XLOG_NOOP` over the same bytes, so the shadow replays one record per
//! run rather than one per dropped record.
//!
//! Page headers stay as the source wrote them: a record joins only while
//! it starts on the run's page, and the run closes once a member ends past
//! that page, so only the last member crosses a header and the merged
//! record ends where it does. A record ending in a later segment never
//! joins, so each segment starts from the source's `xl_prev` and a stream
//! resumed at a segment boundary rebuilds identical bytes. The record after
//! a merged run points `xl_prev` at the run's start.

use smallvec::SmallVec;
use walrus::pg::walparser::X_LOG_RECORD_HEADER_SIZE;

use crate::filter::rewrite::{NOOP_PREFIX, RewriteError, noop_prefix, patch_prev};
use crate::source::wal_page::PAGE_SIZE;

/// One record's `(lsn, len)` pieces, split at page headers
pub(super) type Pieces = SmallVec<[(u64, usize); 2]>;

#[derive(Default)]
pub(super) struct NoopRun {
    open: Option<Run>,
    /// Start of the last record merged away, and the run start the next
    /// record's `xl_prev` points at instead
    remap: Option<(u64, u64)>,
}

struct Run {
    xl_prev: u64,
    first: Pieces,
    last: Pieces,
    /// A member may park on shadow replay, so it dispatches as the run closes
    parks: bool,
}

impl NoopRun {
    /// Start of the open run: bytes from here on are not final
    pub fn start(&self) -> Option<u64> {
        self.open.as_ref().map(|r| r.first[0].0)
    }

    /// Bytes below this are final and safe to send. A run opening a page
    /// holds its header back too: at a page start, PostgreSQL's reader waits
    /// for the header alone before reading the first record's length
    /// (`xlogreader.c`, `XLogDecodeNextRecord`)
    pub fn ship_limit(&self) -> Option<u64> {
        let page_start = |s: u64| s - s % PAGE_SIZE as u64;
        self.start().map(|s| {
            if s - page_start(s) <= LONG_HEADER {
                page_start(s)
            } else {
                s
            }
        })
    }

    /// End of the open run's last member
    pub fn end(&self) -> Option<u64> {
        self.open
            .as_ref()
            .and_then(|r| r.last.last().map(|&(lsn, len)| lsn + len as u64))
    }

    /// Whether a record at `lsn` that may or may not `join` ends the open run
    pub fn ends_before(&self, lsn: u64, joins: bool) -> bool {
        self.start().is_some_and(|s| !joins || page(s) != page(lsn))
    }

    /// `xl_prev` of the next record, given the one the source wrote
    pub fn prev(&mut self, source: u64) -> u64 {
        match self.remap.take() {
            Some((from, to)) if from == source => to,
            _ => source,
        }
    }

    pub fn join(&mut self, pieces: Pieces, xl_prev: u64, parks: bool) {
        match &mut self.open {
            Some(run) => {
                run.last = pieces;
                run.parks |= parks;
            }
            None => {
                self.open = Some(Run {
                    xl_prev,
                    first: pieces.clone(),
                    last: pieces,
                    parks,
                })
            }
        }
    }

    /// Whether a member at `lsn` ending at `next_lsn` closes its run
    pub fn closes_after(lsn: u64, next_lsn: u64) -> bool {
        page(next_lsn) != page(lsn)
    }

    /// Rewrite the open run as one NOOP in `buf`, which starts at `base`.
    /// `Some(parks)` when a run closed
    pub fn close(&mut self, buf: &mut [u8], base: u64) -> Result<Option<bool>, RewriteError> {
        let Some(run) = self.open.take() else {
            return Ok(None);
        };
        let at = |lsn: u64| (lsn - base) as usize;
        let (start, last) = (run.first[0].0, run.last[0].0);
        // Members before the last share one page, so their span is contiguous
        buf[at(start)..at(last)].fill(0);
        for &(lsn, len) in &run.last {
            buf[at(lsn)..at(lsn) + len].fill(0);
        }
        let total = (last - start) as usize + run.last.iter().map(|p| p.1).sum::<usize>();
        let prefix = noop_prefix(total, run.xl_prev)?;
        let mut into = run.first;
        if last != start {
            // Still on this page: the second member starts on it 24 bytes in
            // or more
            into = SmallVec::from_slice(&[(start, NOOP_PREFIX)]);
            self.remap = Some((last, start));
        }
        scatter(buf, base, &into, &prefix[..NOOP_PREFIX.min(total)]);
        Ok(Some(run.parks))
    }

    /// Rewrite a record that joins no run as a NOOP of its own
    pub fn replace(
        buf: &mut [u8],
        base: u64,
        pieces: &Pieces,
        xl_prev: u64,
    ) -> Result<(), RewriteError> {
        let mut run = Self::default();
        run.join(pieces.clone(), xl_prev, false);
        run.close(buf, base).map(drop)
    }

    /// Point a kept record at `xl_prev`
    pub fn repoint(buf: &mut [u8], base: u64, pieces: &Pieces, xl_prev: u64) {
        let mut header = [0u8; X_LOG_RECORD_HEADER_SIZE];
        let mut got = 0;
        for &(lsn, len) in pieces {
            let take = len.min(header.len() - got);
            let off = (lsn - base) as usize;
            header[got..got + take].copy_from_slice(&buf[off..off + take]);
            got += take;
        }
        patch_prev(&mut header, xl_prev);
        scatter(buf, base, pieces, &header);
    }
}

/// Record ends of the segments a resumed stream finds copies of, kept as
/// run boundaries one segment at a time
#[derive(Default)]
pub(super) struct Retained {
    pub dirs: Vec<std::path::PathBuf>,
    /// End of the newest segment a copy was named for as the stream began;
    /// later ones the shadow can only have from this stream
    until: u64,
    seg: Option<u64>,
    ends: std::collections::VecDeque<u64>,
}

impl Retained {
    pub fn new(dirs: &[std::path::PathBuf], until: u64) -> Self {
        Self {
            dirs: dirs.to_vec(),
            until,
            ..Self::default()
        }
    }

    /// Segment to load next
    pub fn wants(&self, seg: u64) -> bool {
        seg < self.until && self.seg.is_none_or(|s| s < seg)
    }

    pub fn load(&mut self, seg: u64, ends: Option<Vec<u64>>) {
        self.seg = Some(seg);
        self.ends = ends.unwrap_or_default().into();
    }

    /// Whether a retained record ends at `lsn`
    pub fn boundary(&mut self, lsn: u64) -> bool {
        while self.ends.front().is_some_and(|e| *e < lsn) {
            self.ends.pop_front();
        }
        self.ends.front() == Some(&lsn)
    }
}

/// Long page header, aligned: the furthest a page's first record starts
const LONG_HEADER: u64 = 40;

fn page(lsn: u64) -> u64 {
    lsn / PAGE_SIZE as u64
}

/// Write `bytes` over the start of a record's pieces
fn scatter(buf: &mut [u8], base: u64, pieces: &[(u64, usize)], bytes: &[u8]) {
    let mut done = 0;
    for &(lsn, len) in pieces {
        let take = len.min(bytes.len() - done);
        let off = (lsn - base) as usize;
        buf[off..off + take].copy_from_slice(&bytes[done..done + take]);
        done += take;
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use walrus::pg::wal::segment::SegmentName;
    use walrus::pg::walparser::RmId;

    use super::*;
    use crate::filter::rewrite::{XLOG_NOOP, compute_crc};
    use crate::pos::Pos;
    use crate::record::{CollectingRecordSink, CollectingSegmentSink};
    use crate::source::streaming_walker::StreamingWalker;
    use crate::source::wal_gen::{TARGET_DB, Wal, mix, wal};
    use crate::source::wal_stream::WalStream;

    const SEG: usize = 4 * PAGE_SIZE;
    const SEGMENTS: usize = 12;

    type Chunks = Vec<(u64, Vec<u8>)>;

    /// Shadow wire chunks
    #[derive(Clone, Default)]
    struct Wire(std::sync::Arc<std::sync::Mutex<Chunks>>);

    impl crate::record::RecordBytesSink for Wire {
        fn on_wire_chunk<'a>(
            &'a mut self,
            start_lsn: u64,
            bytes: &'a [u8],
        ) -> std::pin::Pin<Box<dyn Future<Output = Result<(), crate::record::SinkError>> + Send + 'a>>
        {
            self.0.lock().unwrap().push((start_lsn, bytes.to_vec()));
            Box::pin(std::future::ready(Ok(())))
        }
    }

    /// Sealed output from `from` on, pushing the source in chunks drawn from
    /// `seed`, resuming off retained copies in `dir`, and the LSNs of the
    /// records dispatched. Checks the wire carries the same bytes, in
    /// chunks the shadow's reader can take
    async fn filter(src: &[u8], from: usize, seed: u64, dir: &Path) -> (Vec<u8>, Vec<u64>) {
        let mut stream = WalStream::builder(1, SEG as u64, Pos::new(from as u64)).unwrap();
        stream.filter_mut().set_target_db(TARGET_DB);
        stream
            .preserve_resume_prefix(&[dir.to_path_buf()])
            .await
            .unwrap();
        let wire = Wire::default();
        stream.set_bytes_sink(Box::new(wire.clone()));
        let mut stream = stream.start();
        let mut records = CollectingRecordSink::default();
        let mut segments = CollectingSegmentSink::default();
        let (mut at, mut seed) = (from, seed);
        while at < src.len() {
            let take = (1 + mix(&mut seed) as usize % (3 * PAGE_SIZE)).min(src.len() - at);
            stream
                .push(at as u64, &src[at..at + take], &mut records, &mut segments)
                .await
                .unwrap();
            at += take;
        }
        let out: Vec<u8> = segments.segments.into_iter().flat_map(|s| s.1).collect();
        let mut sent = Vec::new();
        for (lsn, chunk) in wire.0.lock().unwrap().iter() {
            assert_eq!(*lsn, (from + sent.len()) as u64, "contiguous wire");
            sent.extend_from_slice(chunk);
            // A page whose first record starts right after its header is
            // read for that record as soon as the header lands
            let end = lsn + chunk.len() as u64;
            let page = (end - end % PAGE_SIZE as u64) as usize;
            let Some(header) = src.get(page..page + 20) else {
                continue;
            };
            let rem_len = u32::from_le_bytes(header[16..20].try_into().unwrap());
            let data = if page.is_multiple_of(SEG) { 40 } else { 24 };
            assert!(
                end as usize != page + data || rem_len != 0,
                "chunk ends at header {end:#X}"
            );
        }
        assert!(sent.starts_with(&out), "wire matches the archive");
        let lsns = records.records.iter().map(|r| r.source_lsn).collect();
        (out, lsns)
    }

    /// What the shadow reads: every record's CRC and `xl_prev` hold, page
    /// headers and kept records are the source's but for `xl_prev`, and each
    /// NOOP covers exactly a run of dropped records. NOOPs, and the dropped
    /// records they cover
    fn check(wal: &Wal, out: &[u8]) -> (usize, usize) {
        for page in (0..out.len()).step_by(PAGE_SIZE) {
            assert_eq!(
                out[page..page + 20],
                wal.bytes[page..page + 20],
                "page {page:#X}"
            );
        }
        let mut walker = StreamingWalker::new(SEG);
        walker.extend(out);
        let mut source = wal.records.iter().peekable();
        let (mut prev, mut noops, mut covered) = (None, 0, 0);
        while let Some(r) = walker.try_next() {
            let r = r.unwrap();
            let bytes = r.logical_bytes(walker.buffer());
            let lsn = r.start_offset as u64;
            assert_eq!(
                compute_crc(bytes),
                u32::from_le_bytes(bytes[20..24].try_into().unwrap())
            );
            let xl_prev = u64::from_le_bytes(bytes[8..16].try_into().unwrap());
            assert!(prev.is_none_or(|p| p == xl_prev), "xl_prev at {lsn:#X}");
            prev = Some(lsn);
            let &(src_lsn, src_len, dropped) = source.next().unwrap();
            assert_eq!(src_lsn, lsn);
            if !dropped {
                let mut original = vec![0u8; src_len];
                let mut got = 0;
                for &(off, len) in &r.byte_ranges {
                    original[got..got + len].copy_from_slice(&wal.bytes[off..off + len]);
                    got += len;
                }
                assert_eq!(bytes[..8], original[..8]);
                assert_eq!(bytes[16..20], original[16..20]);
                assert_eq!(bytes[24..], original[24..], "kept record at {lsn:#X}");
                continue;
            }
            assert_eq!((bytes[17], bytes[16]), (RmId::Xlog as u8, XLOG_NOOP));
            noops += 1;
            covered += 1;
            let end = lsn + bytes.len() as u64;
            let mut last = (src_lsn, src_len);
            while let Some(&&(l, n, d)) = source.peek()
                && page(l) == page(lsn)
                && l < end
            {
                assert!(d, "kept record at {l:#X} inside a NOOP");
                last = (l, n);
                covered += 1;
                source.next();
            }
            assert_eq!(bytes.len() as u64, last.0 - lsn + last.1 as u64);
        }
        (noops, covered)
    }

    /// The bytes do not depend on how the source arrives, nor on which
    /// segment boundary a restarted stream resumes at
    #[tokio::test]
    async fn merged_noops_replay_and_rebuild_identically_from_any_segment() {
        for seed in 0..4 {
            let wal = wal(seed, SEG, SEGMENTS);
            let dir = tempfile::tempdir().unwrap();
            let (out, lsns) = filter(&wal.bytes, 0, seed, dir.path()).await;
            assert!(out.len() >= SEGMENTS * SEG);
            let (noops, dropped) = check(&wal, &out);
            assert!(
                noops < dropped * 3 / 4,
                "{noops} NOOPs for {dropped} dropped records"
            );
            let sealed: Vec<u64> = wal
                .records
                .iter()
                .map(|r| r.0)
                .filter(|l| *l < out.len() as u64)
                .collect();
            assert_eq!(
                lsns[..sealed.len()],
                sealed,
                "every sealed record dispatches in order"
            );
            retain(dir.path(), &out);
            // A continuation spans up to two more segments, all retained
            for from in 0..out.len() / SEG - 2 {
                let (again, _) = filter(&wal.bytes, from * SEG, seed + 100, dir.path()).await;
                assert!(
                    again == out[from * SEG..],
                    "seed {seed}: resumed at segment {from}"
                );
            }
        }
    }

    /// Segments from LSN 0, as a resumed stream finds them retained
    fn retain(dir: &Path, segments: &[u8]) {
        for (n, seg) in segments.chunks(SEG).enumerate() {
            let name = SegmentName {
                timeline: 1,
                log_id: 0,
                seg_no: n as u32,
            };
            std::fs::write(dir.join(name.format()), seg).unwrap();
        }
    }

    /// A resumed stream rebuilds the bytes a shadow already holds whatever
    /// layout wrote them, and merges past them: an older release's NOOP per
    /// dropped record, or bootstrap's copy filtered from WAL cut at the
    /// backup end, which ends a run there
    #[tokio::test]
    async fn resume_keeps_the_layout_already_retained() {
        let none = tempfile::tempdir().unwrap();
        for seed in 0..4 {
            let wal = wal(seed, SEG, SEGMENTS);
            // Resume where no continuation runs past the segment it opens, at
            // one where the live stream merges across a backup end
            let rem_len = |seg: usize| {
                let at = seg * SEG + 16;
                u32::from_le_bytes(wal.bytes[at..at + 4].try_into().unwrap()) as usize
            };
            let (held, end) = wal
                .records
                .windows(2)
                .find(|w| {
                    let seg = w[0].0 as usize / SEG;
                    (2..SEGMENTS - 2).contains(&seg)
                        && rem_len(seg) < SEG / 2
                        && w[0].2
                        && w[1].2
                        && page(w[0].0) == page(w[1].0)
                })
                .map(|w| (w[0].0 as usize / SEG, w[1].0 as usize))
                .unwrap();
            let (merged, _) = filter(&wal.bytes, 0, seed, none.path()).await;
            let raw = tempfile::tempdir().unwrap();
            retain(raw.path(), &wal.bytes[..merged.len()]);
            let (old, _) = filter(&wal.bytes, 0, seed, raw.path()).await;
            let (noops, dropped) = check(&wal, &old);
            assert_eq!(noops, dropped, "a NOOP per record");

            let copies = tempfile::tempdir().unwrap();
            retain(copies.path(), &old[..(held + 1) * SEG]);
            let (again, _) = filter(&wal.bytes, held * SEG, seed + 1, copies.path()).await;
            assert!(
                again[..SEG] == old[held * SEG..(held + 1) * SEG],
                "held layout"
            );
            assert!(again[SEG..] == merged[(held + 1) * SEG..], "merged past it");

            // Backup end inside a run the live stream merges
            let mut cut = wal.bytes[..(held + 1) * SEG].to_vec();
            cut[end..].fill(0);
            let (landed, _) = filter(&cut, 0, seed, none.path()).await;
            let copies = tempfile::tempdir().unwrap();
            retain(copies.path(), &landed);
            let (live, _) = filter(&wal.bytes, held * SEG, seed + 2, copies.path()).await;
            assert!(live[..end - held * SEG] == landed[held * SEG..end]);
            let mut shadow = landed[..end].to_vec();
            shadow.extend_from_slice(&live[end - held * SEG..]);
            check(&wal, &shadow);
        }
    }
}
