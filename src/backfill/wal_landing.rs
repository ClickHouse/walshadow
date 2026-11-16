//! Filter the WAL bootstrap landed in the shadow's `pg_wal/`.
//!
//! `BASE_BACKUP` with `wal = true` bundles source WAL in the tar, and the
//! object-store leg hydrates the same range from the archive. Neither passes
//! through [`Filter`](crate::filter::Filter): the bytes land raw and the
//! shadow's own recovery replays them before walshadow's walsender exists, so
//! user-heap records and their page images redo into `base/` — files the
//! landing deliberately skipped, which redo then re-creates and zero-extends.
//!
//! Rewrite them once, through the same [`WalStream`] the live path uses, after
//! the backup-window leg has read the raw bytes and before the shadow starts.

use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;

use anyhow::{Context, Result, bail};
use walrus::pg::wal::segment::{SegmentName, is_wal_filename};

use crate::filter::catalog_tracker::CatalogTracker;
use crate::pos::Pos;
use crate::record::{Record, RecordSink, SegmentSink, SinkError, WAL_SEG_SIZE};
use crate::source::wal_stream::WalStream;

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct LandedWalStats {
    pub segments: u64,
    /// Segments wholly above the backup end, blanked rather than filtered
    pub segments_blanked: u64,
    pub kept: u64,
    pub dropped: u64,
    pub dropped_bytes: u64,
}

/// Rewrite every WAL segment in `pg_wal` in place, dropping user-relation
/// records to `XLOG_NOOP`, and zero everything above `end_lsn`.
///
/// `tracker` carries the source's catalog filenodes so rotated catalogs
/// (`VACUUM FULL` / `REINDEX`, filenode >= 16384) stay kept — it must be the
/// same seed the landing's `CatalogFilenodes` used, or the two disagree and
/// redo re-creates a heap file the landing skipped.
pub async fn filter_landed_wal(
    pg_wal: &Path,
    timeline: u32,
    end_lsn: u64,
    tracker: CatalogTracker,
    // Start with staged relations, then add relations created during recovery
    // `None` keeps only catalog WAL
    shadow_rels: Option<(foldhash::HashSet<(u32, u32)>, u64)>,
) -> Result<LandedWalStats> {
    let segments = segments_on_disk(pg_wal, timeline).await?;
    let in_window = segments
        .iter()
        .take_while(|s| s.start_lsn(WAL_SEG_SIZE) < end_lsn)
        .count();
    let Some(first) = segments.first().copied().filter(|_| in_window > 0) else {
        return Ok(LandedWalStats::default());
    };

    let mut stream = WalStream::builder(
        timeline,
        WAL_SEG_SIZE,
        Pos::new(first.start_lsn(WAL_SEG_SIZE)),
    )
    .context("wal_landing: WalStream")?;
    *stream.filter_mut().tracker_mut() = tracker;
    if let Some((rels, redo_lsn)) = shadow_rels {
        stream.filter_mut().keep_user_rels(rels, redo_lsn);
        stream
            .filter_mut()
            .persist_shadow_rels(
                pg_wal
                    .parent()
                    .context("pg_wal must belong to shadow data directory")?,
            )
            .await?;
    }
    let mut stream = stream.start();

    let mut records = DropRecords;
    let mut writer = WriteBack {
        dir: pg_wal.to_path_buf(),
        written: 0,
    };
    for seg in &segments[..in_window] {
        let start = seg.start_lsn(WAL_SEG_SIZE);
        let mut bytes = read_segment(pg_wal, *seg).await?;
        // A recycled segment still holds its previous occupant's records
        // above `end_lsn`. Walking them parses garbage, and leaving them lets
        // the shadow replay unfiltered WAL past the consistency point; zeros
        // are what a freshly initialised segment carries there.
        let usable = end_lsn.saturating_sub(start).min(WAL_SEG_SIZE) as usize;
        bytes[usable..].fill(0);
        stream
            .push(start, &bytes, &mut records, &mut writer)
            .await
            .map_err(|e| anyhow::anyhow!("wal_landing: {}: {e}", seg.format()))?;
    }
    if writer.written != in_window as u64 {
        bail!(
            "wal_landing: rewrote {} of {in_window} in-window segments",
            writer.written,
        );
    }

    let blanked = &segments[in_window..];
    for seg in blanked {
        write_segment(pg_wal, *seg, &vec![0u8; WAL_SEG_SIZE as usize])
            .await
            .with_context(|| format!("wal_landing: blank {}", seg.format()))?;
    }

    let stats = stream.filter().stats();
    Ok(LandedWalStats {
        segments: writer.written,
        segments_blanked: blanked.len() as u64,
        kept: stats.kept,
        dropped: stats.dropped,
        dropped_bytes: stats.dropped_bytes,
    })
}

/// Segment files present, ascending. Rejects a gap or a foreign timeline:
/// the shadow's recovery cannot cross either, and a fresh [`WalStream`] per
/// run would drop the catalog state the previous run learned.
async fn segments_on_disk(pg_wal: &Path, timeline: u32) -> Result<Vec<SegmentName>> {
    let mut dir = match tokio::fs::read_dir(pg_wal).await {
        Ok(d) => d,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(e).context(format!("wal_landing: read {}", pg_wal.display())),
    };
    let mut found = Vec::new();
    while let Some(entry) = dir
        .next_entry()
        .await
        .context(format!("wal_landing: scan {}", pg_wal.display()))?
    {
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        if is_rewrite_leftover(name) {
            let path = entry.path();
            tokio::fs::remove_file(&path)
                .await
                .with_context(|| format!("wal_landing: remove {}", path.display()))?;
            continue;
        }
        let Ok(seg) = SegmentName::parse(name) else {
            continue;
        };
        if seg.timeline != timeline {
            bail!(
                "wal_landing: {name} sits on timeline {} but bootstrap ran on {timeline}",
                seg.timeline,
            );
        }
        found.push(seg);
    }
    found.sort_unstable();
    for pair in found.windows(2) {
        let (prev, next) = (pair[0], pair[1]);
        if prev.next(WAL_SEG_SIZE) != next {
            bail!(
                "wal_landing: gap in {} between {} and {}",
                pg_wal.display(),
                prev.format(),
                next.format(),
            );
        }
    }
    Ok(found)
}

/// Temp file an interrupted [`write_segment`] left beside its segment
fn is_rewrite_leftover(name: &str) -> bool {
    [".tmp", ".walshadow-filtering"]
        .iter()
        .any(|suffix| name.strip_suffix(suffix).is_some_and(is_wal_filename))
}

async fn read_segment(dir: &Path, seg: SegmentName) -> Result<Vec<u8>> {
    let path = dir.join(seg.format());
    let bytes = tokio::fs::read(&path)
        .await
        .with_context(|| format!("wal_landing: read {}", path.display()))?;
    if bytes.len() as u64 != WAL_SEG_SIZE {
        bail!(
            "wal_landing: {} is {} bytes, expected a {WAL_SEG_SIZE}-byte segment",
            path.display(),
            bytes.len(),
        );
    }
    Ok(bytes)
}

/// A crash mid-write must not leave shadow a half-rewritten segment to replay
async fn write_segment(dir: &Path, seg: SegmentName, bytes: &[u8]) -> std::io::Result<()> {
    crate::fs::write_atomic(dir, &seg.format(), bytes).await
}

struct DropRecords;

impl RecordSink for DropRecords {
    fn on_record<'a>(
        &'a mut self,
        _record: &'a Record<'a>,
    ) -> Pin<Box<dyn Future<Output = std::result::Result<(), SinkError>> + Send + 'a>> {
        Box::pin(std::future::ready(Ok(())))
    }
}

struct WriteBack {
    dir: PathBuf,
    written: u64,
}

impl SegmentSink for WriteBack {
    fn on_segment<'a>(
        &'a mut self,
        seg: SegmentName,
        bytes: &'a [u8],
    ) -> Pin<Box<dyn Future<Output = std::result::Result<(), SinkError>> + Send + 'a>> {
        Box::pin(async move {
            write_segment(&self.dir, seg, bytes).await?;
            self.written += 1;
            Ok(())
        })
    }
}

/// Copy the in-window segments out of `pg_wal` into `dir`.
///
/// Backup WAL processing needs original bytes, while [`filter_landed_wal`]
/// rewrites `pg_wal` before shadow recovery. Read original segments from copy.
pub async fn copy_window_segments(
    pg_wal: &Path,
    dir: &Path,
    timeline: u32,
    from_lsn: u64,
    end_lsn: u64,
) -> Result<u64> {
    tokio::fs::create_dir_all(dir)
        .await
        .with_context(|| format!("create {}", dir.display()))?;
    let mut copied = 0u64;
    for seg in segments_on_disk(pg_wal, timeline).await? {
        if seg.start_lsn(WAL_SEG_SIZE) >= end_lsn {
            break;
        }
        // Start copy at segment containing replay floor
        if seg.start_lsn(WAL_SEG_SIZE) + WAL_SEG_SIZE <= from_lsn {
            continue;
        }
        let name = seg.format();
        tokio::fs::copy(pg_wal.join(&name), dir.join(&name))
            .await
            .with_context(|| format!("copy WAL segment {name}"))?;
        copied += 1;
    }
    Ok(copied)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn seg(seg_no: u32) -> SegmentName {
        SegmentName {
            timeline: 1,
            log_id: 0,
            seg_no,
        }
    }

    async fn touch(dir: &Path, name: &str) {
        tokio::fs::write(dir.join(name), b"").await.unwrap();
    }

    #[tokio::test]
    async fn scan_orders_segments_and_ignores_aux_files() {
        let tmp = tempfile::tempdir().unwrap();
        for n in [3u32, 1, 2] {
            touch(tmp.path(), &seg(n).format()).await;
        }
        touch(tmp.path(), "00000002.history").await;
        touch(tmp.path(), "000000010000000000000009.partial").await;
        tokio::fs::create_dir(tmp.path().join("archive_status"))
            .await
            .unwrap();

        let got = segments_on_disk(tmp.path(), 1).await.unwrap();
        assert_eq!(got, vec![seg(1), seg(2), seg(3)]);
    }

    #[tokio::test]
    async fn scan_removes_interrupted_rewrites() {
        let tmp = tempfile::tempdir().unwrap();
        touch(tmp.path(), &seg(1).format()).await;
        let leftovers = [
            format!("{}.tmp", seg(1).format()),
            format!("{}.walshadow-filtering", seg(2).format()),
        ];
        for name in &leftovers {
            touch(tmp.path(), name).await;
        }
        touch(tmp.path(), "keep.tmp").await;

        let got = segments_on_disk(tmp.path(), 1).await.unwrap();
        assert_eq!(got, vec![seg(1)]);
        for name in &leftovers {
            assert!(!tmp.path().join(name).exists(), "{name} survived");
        }
        assert!(tmp.path().join("keep.tmp").exists());
    }

    #[tokio::test]
    async fn scan_refuses_a_gap() {
        let tmp = tempfile::tempdir().unwrap();
        touch(tmp.path(), &seg(1).format()).await;
        touch(tmp.path(), &seg(3).format()).await;
        let err = segments_on_disk(tmp.path(), 1).await.unwrap_err();
        assert!(err.to_string().contains("gap"), "{err}");
    }

    #[tokio::test]
    async fn scan_refuses_a_foreign_timeline() {
        let tmp = tempfile::tempdir().unwrap();
        touch(tmp.path(), &seg(1).format()).await;
        touch(
            tmp.path(),
            &SegmentName {
                timeline: 2,
                log_id: 0,
                seg_no: 2,
            }
            .format(),
        )
        .await;
        let err = segments_on_disk(tmp.path(), 1).await.unwrap_err();
        assert!(err.to_string().contains("timeline"), "{err}");
    }

    #[tokio::test]
    async fn scan_reports_a_pg_wal_that_is_no_directory() {
        let tmp = tempfile::tempdir().unwrap();
        let file = tmp.path().join("pg_wal");
        touch(tmp.path(), "pg_wal").await;
        let err = segments_on_disk(&file, 1).await.unwrap_err();
        assert!(err.to_string().contains("wal_landing: read"), "{err}");
    }

    #[tokio::test]
    async fn filter_refuses_a_short_segment() {
        let tmp = tempfile::tempdir().unwrap();
        touch(tmp.path(), &seg(1).format()).await;
        let err = filter_landed_wal(tmp.path(), 1, u64::MAX, CatalogTracker::new(), None)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("is 0 bytes"), "{err}");
    }

    #[tokio::test]
    async fn copy_takes_only_segments_inside_the_window() {
        let tmp = tempfile::tempdir().unwrap();
        for n in 1..=3 {
            touch(tmp.path(), &seg(n).format()).await;
        }
        let out = tmp.path().join("window");
        let from = seg(2).start_lsn(WAL_SEG_SIZE);
        let copied = copy_window_segments(tmp.path(), &out, 1, from, from + 1)
            .await
            .unwrap();
        assert_eq!(copied, 1);
        assert!(out.join(seg(2).format()).exists());
        let err = copy_window_segments(tmp.path(), &out.join(seg(2).format()), 1, from, from + 1)
            .await
            .unwrap_err();
        assert!(err.to_string().starts_with("create "), "{err}");
    }

    /// Long page header of `seg`, then `body` at first record position
    async fn write_wal(dir: &Path, seg: SegmentName, body: &[u8]) {
        use walrus::pg::walparser::{WAL_PAGE_SIZE, XLP_LONG_HEADER, XLP_PAGE_MAGIC_PG15};
        let mut bytes = Vec::with_capacity(WAL_SEG_SIZE as usize);
        bytes.extend_from_slice(&XLP_PAGE_MAGIC_PG15.to_le_bytes());
        bytes.extend_from_slice(&XLP_LONG_HEADER.to_le_bytes());
        bytes.extend_from_slice(&seg.timeline.to_le_bytes());
        bytes.extend_from_slice(&seg.start_lsn(WAL_SEG_SIZE).to_le_bytes());
        bytes.extend_from_slice(&[0; 8]);
        bytes.extend_from_slice(&12345u64.to_le_bytes());
        bytes.extend_from_slice(&(WAL_SEG_SIZE as u32).to_le_bytes());
        bytes.extend_from_slice(&u32::from(WAL_PAGE_SIZE).to_le_bytes());
        bytes.extend_from_slice(body);
        bytes.resize(WAL_SEG_SIZE as usize, 0);
        tokio::fs::write(dir.join(seg.format()), bytes)
            .await
            .unwrap();
    }

    async fn filter_err(dir: &Path, end_lsn: u64) -> String {
        filter_landed_wal(dir, 1, end_lsn, CatalogTracker::new(), None)
            .await
            .unwrap_err()
            .to_string()
    }

    #[tokio::test]
    async fn filter_rewrites_window_and_blanks_above_end() {
        let tmp = tempfile::tempdir().unwrap();
        write_wal(tmp.path(), seg(1), &[]).await;
        tokio::fs::write(tmp.path().join(seg(2).format()), [0xAA; 8])
            .await
            .unwrap();
        let stats = filter_landed_wal(
            tmp.path(),
            1,
            seg(2).start_lsn(WAL_SEG_SIZE),
            CatalogTracker::new(),
            None,
        )
        .await
        .unwrap();
        let blank = tokio::fs::read(tmp.path().join(seg(2).format()))
            .await
            .unwrap();
        assert_eq!(
            (stats.segments, stats.segments_blanked, blank.len() as u64),
            (1, 1, WAL_SEG_SIZE)
        );
        assert!(blank.iter().all(|&b| b == 0));
    }

    #[tokio::test]
    async fn filter_reports_unparsable_segment() {
        let tmp = tempfile::tempdir().unwrap();
        tokio::fs::write(tmp.path().join(seg(1).format()), {
            let mut bytes = vec![0u8; WAL_SEG_SIZE as usize];
            bytes[..3].copy_from_slice(&[0xFF, 0xFF, 1]);
            bytes
        })
        .await
        .unwrap();
        let err = filter_err(tmp.path(), u64::MAX).await;
        assert!(
            err.starts_with(&format!("wal_landing: {}: ", seg(1).format())),
            "{err}"
        );
    }

    #[tokio::test]
    async fn filter_refuses_segment_walk_holds_back() {
        use walrus::pg::walparser::{RmId, WAL_PAGE_SIZE};
        let tmp = tempfile::tempdir().unwrap();
        let mut rec = (2 * WAL_SEG_SIZE as u32).to_le_bytes().to_vec();
        rec.extend_from_slice(&[0; 12]);
        rec.extend_from_slice(&[0, RmId::Xact as u8, 0, 0]);
        rec.resize(usize::from(WAL_PAGE_SIZE) - 40, 0xAA);
        write_wal(tmp.path(), seg(1), &rec).await;
        let end = seg(1).start_lsn(WAL_SEG_SIZE) + u64::from(WAL_PAGE_SIZE);
        let err = filter_err(tmp.path(), end).await;
        assert_eq!(err, "wal_landing: rewrote 0 of 1 in-window segments");
    }

    #[tokio::test]
    async fn filter_reports_segment_it_cannot_read_or_blank() {
        let tmp = tempfile::tempdir().unwrap();
        tokio::fs::create_dir(tmp.path().join(seg(1).format()))
            .await
            .unwrap();
        let err = filter_err(tmp.path(), u64::MAX).await;
        assert!(err.starts_with("wal_landing: read "), "{err}");

        tokio::fs::remove_dir(tmp.path().join(seg(1).format()))
            .await
            .unwrap();
        write_wal(tmp.path(), seg(1), &[]).await;
        tokio::fs::create_dir(tmp.path().join(seg(2).format()))
            .await
            .unwrap();
        let err = filter_err(tmp.path(), seg(2).start_lsn(WAL_SEG_SIZE)).await;
        assert_eq!(err, format!("wal_landing: blank {}", seg(2).format()));
    }

    #[tokio::test]
    async fn scan_reports_leftover_it_cannot_remove() {
        let tmp = tempfile::tempdir().unwrap();
        let leftover = tmp.path().join(format!("{}.tmp", seg(1).format()));
        tokio::fs::create_dir(&leftover).await.unwrap();
        let err = segments_on_disk(tmp.path(), 1).await.unwrap_err();
        assert_eq!(
            err.to_string(),
            format!("wal_landing: remove {}", leftover.display())
        );
    }

    #[tokio::test]
    async fn copy_reports_segment_it_cannot_copy() {
        let tmp = tempfile::tempdir().unwrap();
        tokio::fs::create_dir(tmp.path().join(seg(1).format()))
            .await
            .unwrap();
        let err = copy_window_segments(tmp.path(), &tmp.path().join("out"), 1, 0, u64::MAX)
            .await
            .unwrap_err();
        assert_eq!(
            err.to_string(),
            format!("copy WAL segment {}", seg(1).format())
        );
    }

    #[tokio::test]
    async fn missing_pg_wal_is_not_an_error() {
        let tmp = tempfile::tempdir().unwrap();
        let stats = filter_landed_wal(
            &tmp.path().join("absent"),
            1,
            WAL_SEG_SIZE,
            CatalogTracker::new(),
            None,
        )
        .await
        .unwrap();
        assert_eq!(stats, LandedWalStats::default());
    }
}
