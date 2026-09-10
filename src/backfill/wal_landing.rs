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
use crate::filter::manifest::Manifest;
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
) -> Result<LandedWalStats> {
    let segments = segments_on_disk(pg_wal, timeline).await?;
    let in_window = segments
        .iter()
        .take_while(|s| s.start_lsn(WAL_SEG_SIZE) < end_lsn)
        .count();
    let Some(first) = segments.first().copied().filter(|_| in_window > 0) else {
        return Ok(LandedWalStats::default());
    };

    let mut stream = WalStream::new(timeline, WAL_SEG_SIZE, first.start_lsn(WAL_SEG_SIZE))
        .map_err(|e| anyhow::anyhow!("wal_landing: WalStream: {e}"))?;
    *stream.filter_mut().tracker_mut() = tracker;

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
    sync_dir(pg_wal).await?;

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
        Err(e) => return Err(e).with_context(|| format!("wal_landing: read {}", pg_wal.display())),
    };
    let mut found = Vec::new();
    while let Some(entry) = dir
        .next_entry()
        .await
        .with_context(|| format!("wal_landing: scan {}", pg_wal.display()))?
    {
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        if !is_wal_filename(name) {
            continue;
        }
        let seg = SegmentName::parse(name)
            .map_err(|e| anyhow::anyhow!("wal_landing: segment name {name}: {e}"))?;
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

/// Replaces a segment through a sibling temp file: a crash mid-write must not
/// leave the shadow a half-rewritten segment it would then replay.
async fn write_segment(dir: &Path, seg: SegmentName, bytes: &[u8]) -> std::io::Result<()> {
    let name = seg.format();
    let tmp = dir.join(format!("{name}.walshadow-filtering"));
    let mut f = tokio::fs::File::create(&tmp).await?;
    tokio::io::AsyncWriteExt::write_all(&mut f, bytes).await?;
    f.sync_all().await?;
    drop(f);
    tokio::fs::rename(&tmp, dir.join(&name)).await
}

async fn sync_dir(dir: &Path) -> Result<()> {
    let handle = tokio::fs::File::open(dir)
        .await
        .with_context(|| format!("wal_landing: open {}", dir.display()))?;
    handle
        .sync_all()
        .await
        .with_context(|| format!("wal_landing: fsync {}", dir.display()))
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
        _manifest: &'a Manifest,
    ) -> Pin<Box<dyn Future<Output = std::result::Result<(), SinkError>> + Send + 'a>> {
        Box::pin(async move {
            write_segment(&self.dir, seg, bytes).await?;
            self.written += 1;
            Ok(())
        })
    }
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
    async fn missing_pg_wal_is_not_an_error() {
        let tmp = tempfile::tempdir().unwrap();
        let stats = filter_landed_wal(
            &tmp.path().join("absent"),
            1,
            WAL_SEG_SIZE,
            CatalogTracker::new(),
        )
        .await
        .unwrap();
        assert_eq!(stats, LandedWalStats::default());
    }
}
