//! Preserve filtered continuation bytes whose record starts before resume

use std::ops::Range;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail, ensure};
use walrus::pg::wal::segment::SegmentName;
use walrus::pg::walparser::XLP_FIRST_IS_CONT_RECORD;

use super::wal_page::{PAGE_SIZE, PageHeaderParse, parse_page_header};

pub(super) struct ResumePrefix {
    start_lsn: u64,
    bytes: Vec<u8>,
    headers: Vec<Range<usize>>,
}

impl ResumePrefix {
    pub async fn load(
        dirs: &[PathBuf],
        timeline: u32,
        seg_size: u64,
        start_lsn: u64,
    ) -> Result<Option<Self>> {
        for dir in dirs {
            let path = segment_path(dir, timeline, seg_size, start_lsn);
            let Some(segment) = read_segment(&path, seg_size).await? else {
                continue;
            };
            let prefix = Self::read(dir, timeline, seg_size, start_lsn, segment).await?;
            if prefix.is_some() {
                return Ok(prefix);
            }
        }
        Ok(None)
    }

    async fn read(
        dir: &Path,
        timeline: u32,
        seg_size: u64,
        start_lsn: u64,
        mut segment: Vec<u8>,
    ) -> Result<Option<Self>> {
        let mut prefix = Self {
            start_lsn,
            bytes: Vec::new(),
            headers: Vec::new(),
        };
        let mut remaining = None;
        let mut page_lsn = start_lsn;
        loop {
            let page = (page_lsn % seg_size) as usize;
            let parsed = parse_page_header(&segment, page)?;
            if remaining.is_none() && matches!(parsed, PageHeaderParse::ZeroPage) {
                return Ok(None);
            }
            let PageHeaderParse::Valid {
                data_start,
                remaining_data_len,
                ..
            } = parsed
            else {
                bail!("incomplete retained WAL page at {page_lsn:#X}");
            };
            let address = page_address(&segment, page);
            // PostgreSQL can leave recycled segments under their next filename
            if remaining.is_none() && address != page_lsn {
                return Ok(None);
            }
            ensure!(
                address == page_lsn,
                "retained WAL page address mismatch at {page_lsn:#X}"
            );
            if remaining.is_none() && remaining_data_len == 0 {
                return Ok(None);
            }
            ensure!(
                remaining.is_none_or(|n| n == remaining_data_len),
                "retained WAL continuation length mismatch at {page_lsn:#X}"
            );
            ensure!(
                page_info(&segment, page) & XLP_FIRST_IS_CONT_RECORD != 0,
                "retained WAL continuation flag missing at {page_lsn:#X}"
            );
            let take = remaining_data_len.min(page + PAGE_SIZE - data_start);
            let offset = prefix.bytes.len();
            prefix.headers.push(offset..offset + data_start - page);
            prefix
                .bytes
                .extend_from_slice(&segment[page..data_start + take]);
            if take == remaining_data_len {
                return Ok(Some(prefix));
            }
            remaining = Some(remaining_data_len - take);
            page_lsn += PAGE_SIZE as u64;
            if page_lsn.is_multiple_of(seg_size) {
                let path = segment_path(dir, timeline, seg_size, page_lsn);
                segment = read_segment(&path, seg_size)
                    .await?
                    .with_context(|| format!("retained continuation {} missing", path.display()))?;
            }
        }
    }

    pub fn end_lsn(&self) -> u64 {
        self.start_lsn + self.bytes.len() as u64
    }

    /// Splice retained bytes over the part of `bytes` they cover, `bytes`
    /// starting at `lsn`. A chunk reaching past either end of the retained
    /// span keeps its own bytes there, so any push offset is safe
    pub fn apply(&self, lsn: u64, bytes: &mut [u8]) -> Result<()> {
        let from = lsn.max(self.start_lsn);
        let to = (lsn + bytes.len() as u64).min(self.end_lsn());
        if to <= from {
            return Ok(());
        }
        let dst = (from - lsn) as usize;
        let src = (from - self.start_lsn) as usize..(to - self.start_lsn) as usize;
        for header in &self.headers {
            let lo = src.start.max(header.start);
            let hi = src.end.min(header.end);
            if lo < hi {
                ensure!(
                    bytes[dst + lo - src.start..dst + hi - src.start] == self.bytes[lo..hi],
                    "source WAL header differs from retained continuation at {:#X}",
                    self.start_lsn + lo as u64
                );
            }
        }
        bytes[dst..dst + src.len()].copy_from_slice(&self.bytes[src]);
        Ok(())
    }
}

/// `None` when the segment is absent; a retained one is always whole
async fn read_segment(path: &Path, seg_size: u64) -> Result<Option<Vec<u8>>> {
    let bytes = match tokio::fs::read(path).await {
        Ok(bytes) => bytes,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e).with_context(|| format!("read {}", path.display())),
    };
    ensure!(
        bytes.len() == seg_size as usize,
        "retained WAL segment {} has {} bytes, expected {seg_size}",
        path.display(),
        bytes.len()
    );
    Ok(Some(bytes))
}

/// `xlp_info`
fn page_info(segment: &[u8], page: usize) -> u16 {
    u16::from_le_bytes(segment[page + 2..page + 4].try_into().unwrap())
}

/// `xlp_pageaddr`
fn page_address(segment: &[u8], page: usize) -> u64 {
    u64::from_le_bytes(segment[page + 8..page + 16].try_into().unwrap())
}

fn segment_path(dir: &Path, timeline: u32, seg_size: u64, lsn: u64) -> PathBuf {
    dir.join(
        SegmentName {
            timeline,
            log_id: (lsn >> 32) as u32,
            seg_no: ((lsn & 0xffff_ffff) / seg_size) as u32,
        }
        .format(),
    )
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::{Arc, Mutex};

    use walrus::pg::walparser::{
        X_LOG_RECORD_ALIGNMENT, X_LOG_RECORD_HEADER_SIZE, XLP_LONG_HEADER, XLP_PAGE_MAGIC_PG15,
        XLR_BLOCK_ID_DATA_LONG,
    };

    use super::*;
    use crate::filter::rewrite::{compute_crc, noop_replace};
    use crate::record::{
        CollectingRecordSink, CollectingSegmentSink, RecordBytesSink, SinkError, WAL_SEG_SIZE,
    };
    use crate::source::wal_page::align_up;
    use crate::source::wal_stream::{WalStream, WalStreamError};

    /// Every fixture page sits at a segment start, so all carry long headers
    const LONG_HDR: usize = 40;
    const XLP_REM_LEN: usize = 16;
    const XL_INFO: usize = 16;
    const XL_CRC: usize = 20;
    const HDR: usize = X_LOG_RECORD_HEADER_SIZE;

    struct Wire(Arc<Mutex<Vec<u8>>>);

    impl RecordBytesSink for Wire {
        fn on_wire_chunk<'a>(
            &'a mut self,
            _: u64,
            bytes: &'a [u8],
        ) -> Pin<Box<dyn Future<Output = Result<(), SinkError>> + Send + 'a>> {
            Box::pin(async move {
                self.0.lock().unwrap().extend_from_slice(bytes);
                Ok(())
            })
        }

        fn on_segment_boundary<'a>(
            &'a mut self,
            lsn: u64,
            bytes: &'a [u8],
        ) -> Pin<Box<dyn Future<Output = Result<(), SinkError>> + Send + 'a>> {
            self.on_wire_chunk(lsn, bytes)
        }
    }

    /// Segment-start page whose first `remaining` bytes continue a record
    /// that began in the preceding segment
    fn page(lsn: u64, remaining: u32, seg_size: u64) -> Vec<u8> {
        let contrecord = if remaining > 0 {
            XLP_FIRST_IS_CONT_RECORD
        } else {
            0
        };
        let mut bytes = vec![0; PAGE_SIZE];
        bytes[..2].copy_from_slice(&XLP_PAGE_MAGIC_PG15.to_le_bytes());
        bytes[2..4].copy_from_slice(&(XLP_LONG_HEADER | contrecord).to_le_bytes());
        bytes[4..8].copy_from_slice(&1u32.to_le_bytes());
        bytes[8..16].copy_from_slice(&lsn.to_le_bytes());
        bytes[XLP_REM_LEN..XLP_REM_LEN + 4].copy_from_slice(&remaining.to_le_bytes());
        bytes[24..32].copy_from_slice(&12345u64.to_le_bytes());
        bytes[32..36].copy_from_slice(&(seg_size as u32).to_le_bytes());
        bytes[36..40].copy_from_slice(&(PAGE_SIZE as u32).to_le_bytes());
        bytes
    }

    fn stored_crc(record: &[u8]) -> u32 {
        u32::from_le_bytes(record[XL_CRC..XL_CRC + 4].try_into().unwrap())
    }

    /// Bootstrap rewrote the record's first `SPLIT` bytes to a NOOP in the
    /// preceding segment; the resume segment arrives from the archive still
    /// carrying the original continuation
    #[tokio::test]
    async fn resume_preserves_bootstrap_noop_across_segment_boundary() {
        const TOTAL: usize = 8162;
        const SPLIT: usize = 2312;
        const CONT: usize = TOTAL - SPLIT;
        const CONT_END: usize = LONG_HDR + CONT;

        let start = 0x5fd55000000;
        let mut original = vec![0xAA; TOTAL];
        original[..HDR].fill(0);
        original[..4].copy_from_slice(&(TOTAL as u32).to_le_bytes());
        original[XL_INFO] = 0x20;
        original[HDR] = XLR_BLOCK_ID_DATA_LONG;
        original[HDR + 1..HDR + 5].copy_from_slice(&((TOTAL - HDR - 5) as u32).to_le_bytes());
        let crc = compute_crc(&original);
        original[XL_CRC..XL_CRC + 4].copy_from_slice(&crc.to_le_bytes());
        let mut rewritten = original.clone();
        noop_replace(&mut rewritten).unwrap();

        let mut raw = page(start, CONT as u32, WAL_SEG_SIZE);
        raw[LONG_HDR..CONT_END].copy_from_slice(&original[SPLIT..]);
        // A following complete record must still reach decoder and wire
        let mut next = vec![0; 32];
        next[..4].copy_from_slice(&32u32.to_le_bytes());
        noop_replace(&mut next).unwrap();
        let next_at = align_up(CONT_END, X_LOG_RECORD_ALIGNMENT);
        raw[next_at..next_at + next.len()].copy_from_slice(&next);
        raw.resize(WAL_SEG_SIZE as usize, 0);

        let mut mixed = rewritten[..SPLIT].to_vec();
        mixed.extend_from_slice(&original[SPLIT..]);
        assert_ne!(compute_crc(&mixed), stored_crc(&mixed));

        let mut retained = raw.clone();
        retained[LONG_HDR..CONT_END].copy_from_slice(&rewritten[SPLIT..]);
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(segment_path(dir.path(), 1, WAL_SEG_SIZE, start), &retained).unwrap();

        for chunk_size in [17, WAL_SEG_SIZE as usize] {
            let mut stream = WalStream::new(1, WAL_SEG_SIZE, start).unwrap();
            stream
                .preserve_resume_prefix(&[dir.path().to_path_buf()])
                .await
                .unwrap();
            let wire = Arc::new(Mutex::new(Vec::new()));
            stream.set_bytes_sink(Box::new(Wire(wire.clone())));
            let mut records = CollectingRecordSink::default();
            let mut segments = CollectingSegmentSink::default();
            let mut lsn = start;
            for part in [&raw[..CONT_END], &raw[CONT_END..]] {
                for chunk in part.chunks(chunk_size) {
                    stream
                        .push(lsn, chunk, &mut records, &mut segments)
                        .await
                        .unwrap();
                    lsn += chunk.len() as u64;
                }
                if lsn == start + CONT_END as u64 {
                    assert_eq!(stream.fork_prefix().crc, crc32c::crc32c(&raw[..CONT_END]));
                }
            }
            assert_eq!(records.records.len(), 1);
            let output = &segments.segments[0].1;
            assert_eq!(output, &retained);
            assert_eq!(*wire.lock().unwrap(), retained);
            let mut record = rewritten[..SPLIT].to_vec();
            record.extend_from_slice(&output[LONG_HDR..CONT_END]);
            assert_eq!(compute_crc(&record), stored_crc(&record));
        }
    }

    #[tokio::test]
    async fn resume_rejects_changed_source_header_before_publication() {
        let start = WAL_SEG_SIZE;
        let dir = tempfile::tempdir().unwrap();
        let mut retained = page(start, 100, WAL_SEG_SIZE);
        retained.resize(WAL_SEG_SIZE as usize, 0);
        std::fs::write(segment_path(dir.path(), 1, WAL_SEG_SIZE, start), &retained).unwrap();
        let mut stream = WalStream::new(1, WAL_SEG_SIZE, start).unwrap();
        stream
            .preserve_resume_prefix(&[dir.path().to_path_buf()])
            .await
            .unwrap();
        // Source disagrees with the retained copy on xlp_rem_len
        retained[XLP_REM_LEN..XLP_REM_LEN + 4].copy_from_slice(&101u32.to_le_bytes());
        let mut records = CollectingRecordSink::default();
        let mut segments = CollectingSegmentSink::default();
        let error = stream
            .push(start, &retained, &mut records, &mut segments)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("header differs"), "{error}");
        assert!(segments.segments.is_empty());
        assert!(records.records.is_empty());
        assert!(matches!(
            stream
                .push(start, &retained, &mut records, &mut segments)
                .await,
            Err(WalStreamError::Poisoned)
        ));
    }

    #[tokio::test]
    async fn resume_preserves_continuation_across_multiple_segments() {
        let seg_size = PAGE_SIZE as u64;
        let start = seg_size;
        let capacity = PAGE_SIZE - LONG_HDR;
        let dir = tempfile::tempdir().unwrap();
        let first = page(start, (capacity + 100) as u32, seg_size);
        let second = page(start + seg_size, 100, seg_size);
        std::fs::write(segment_path(dir.path(), 1, seg_size, start), &first).unwrap();
        let missing = ResumePrefix::load(&[dir.path().to_path_buf()], 1, seg_size, start).await;
        assert!(missing.is_err());
        std::fs::write(
            segment_path(dir.path(), 1, seg_size, start + seg_size),
            &second,
        )
        .unwrap();
        let prefix = ResumePrefix::load(&[dir.path().to_path_buf()], 1, seg_size, start)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(prefix.end_lsn(), start + seg_size + 140);
        let mut raw = second.clone();
        raw[LONG_HDR..].fill(0xAA);
        prefix.apply(start + seg_size, &mut raw).unwrap();
        assert_eq!(&raw[..140], &second[..140]);
        assert!(raw[140..].iter().all(|b| *b == 0xAA));
    }

    #[tokio::test]
    async fn resume_ignores_absent_or_recycled_wal() {
        let dir = tempfile::tempdir().unwrap();
        let dirs = [dir.path().to_path_buf()];
        assert!(
            ResumePrefix::load(&dirs, 1, WAL_SEG_SIZE, WAL_SEG_SIZE)
                .await
                .unwrap()
                .is_none()
        );
        let mut old = page(0, 100, WAL_SEG_SIZE);
        old.resize(WAL_SEG_SIZE as usize, 0);
        std::fs::write(
            segment_path(dir.path(), 1, WAL_SEG_SIZE, WAL_SEG_SIZE),
            &old,
        )
        .unwrap();
        assert!(
            ResumePrefix::load(&dirs, 1, WAL_SEG_SIZE, WAL_SEG_SIZE)
                .await
                .unwrap()
                .is_none()
        );
    }

    /// Chunks landing off either end of the retained span keep their own
    /// bytes rather than indexing outside it
    #[test]
    fn apply_splices_only_the_overlap() {
        let start_lsn = 0x1000;
        let mut retained = vec![0; 64];
        retained[..HDR].fill(0xBB);
        let prefix = ResumePrefix {
            start_lsn,
            bytes: retained,
            headers: std::iter::once(0..HDR).collect(),
        };

        for lsn in [start_lsn - 64, start_lsn + 64] {
            let mut outside = vec![0xAA; 64];
            prefix.apply(lsn, &mut outside).unwrap();
            assert!(outside.iter().all(|b| *b == 0xAA));
        }

        let mut tail = vec![0xAA; 64];
        prefix.apply(start_lsn + 32, &mut tail).unwrap();
        assert!(tail[..32].iter().all(|b| *b == 0));
        assert!(tail[32..].iter().all(|b| *b == 0xAA));

        let mut head = vec![0xAA; 64];
        head[32..32 + HDR].fill(0xBB);
        prefix.apply(start_lsn - 32, &mut head).unwrap();
        assert!(head[..32].iter().all(|b| *b == 0xAA));
        assert!(head[32..32 + HDR].iter().all(|b| *b == 0xBB));
        assert!(head[32 + HDR..].iter().all(|b| *b == 0));

        head[32] = 0xCC;
        let error = prefix.apply(start_lsn - 32, &mut head).unwrap_err();
        assert!(error.to_string().contains("header differs"), "{error}");
    }
}
