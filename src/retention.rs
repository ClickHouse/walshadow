//! Filtered segment retention.
//!
//! Shadow PG's `restore_command` copies (not moves) segments out of the
//! filter's output dir, so originals accumulate forever. Drop segments +
//! manifests once shadow has replayed past them, bounded below by
//! `retention_bytes` of WAL so a restart with `--start-lsn` slightly
//! behind head still finds segments to replay through.
//!
//! Trim in LSN bytes not wall-clock: retention is "how far behind can
//! shadow be" which is exactly LSN, keeping the trimmer clock-free.
//!
//! `.partial` files (crash residue) and `*.manifest.json` sidecars are
//! removed with their segment. Unknown files left alone, so a sibling
//! system sharing the dir doesn't lose unrelated files.

use std::io;
use std::path::Path;
use std::time::Duration;

use pgwalrs::pg::wal::segment::{SEGMENT_NAME_LEN, SegmentName};
use thiserror::Error;

use crate::wal_stream::WAL_SEG_SIZE;

/// 256 MiB = ~16 segments at 16 MiB. Enough to replay through a typical
/// workload gap without holding multi-GB of catalog WAL on disk.
pub const DEFAULT_RETENTION_BYTES: u64 = 256 * 1024 * 1024;

/// Trim cost is a `pg_last_wal_replay_lsn` query + a `read_dir`, both
/// sub-ms; 30s is plenty given segment cadence is the same order.
pub const DEFAULT_TRIM_INTERVAL: Duration = Duration::from_secs(30);

#[derive(Debug, Error)]
pub enum RetentionError {
    #[error("io: {0}")]
    Io(#[from] io::Error),
}

/// Per-category counts from one sweep; surfaced on the daemon status line.
#[derive(Debug, Default, Clone, Copy)]
pub struct TrimReport {
    pub segments_removed: u64,
    pub manifests_removed: u64,
    pub partials_removed: u64,
    pub bytes_freed: u64,
}

/// Trim segments whose *end* LSN (`start_lsn + WAL_SEG_SIZE`) is below
/// `cutoff_lsn`. End-LSN boundary preserves the segment containing the
/// cutoff: shadow may still be reading it.
pub async fn trim_below_lsn(dir: &Path, cutoff_lsn: u64) -> Result<TrimReport, RetentionError> {
    let mut report = TrimReport::default();
    if !dir.exists() {
        return Ok(report);
    }
    let mut rd = tokio::fs::read_dir(dir).await?;
    while let Some(entry) = rd.next_entry().await? {
        let path = entry.path();
        let name = match path.file_name().and_then(|n| n.to_str()) {
            Some(s) => s.to_string(),
            None => continue,
        };
        let (seg_str, kind) = classify(&name);
        let seg = match seg_str.and_then(|s| SegmentName::parse(s).ok()) {
            Some(s) => s,
            None => continue, // unknown file, leave alone
        };
        let end_lsn = seg.start_lsn(WAL_SEG_SIZE).saturating_add(WAL_SEG_SIZE);
        if end_lsn > cutoff_lsn {
            continue;
        }
        let size = entry.metadata().await.map(|m| m.len()).unwrap_or(0);
        tokio::fs::remove_file(&path).await?;
        report.bytes_freed = report.bytes_freed.saturating_add(size);
        match kind {
            FileKind::Segment => report.segments_removed += 1,
            FileKind::Manifest => report.manifests_removed += 1,
            FileKind::Partial => report.partials_removed += 1,
        }
        tracing::debug!(
            target: "walshadow::retention",
            file = %name,
            end_lsn,
            cutoff_lsn,
            "trimmed",
        );
    }
    Ok(report)
}

#[derive(Debug, Clone, Copy)]
enum FileKind {
    Segment,
    Manifest,
    Partial,
}

/// Pluck the 24-hex segment prefix, tag the suffix kind. `(None, _)` for
/// filenames matching no expected shape.
fn classify(name: &str) -> (Option<&str>, FileKind) {
    if name.len() == SEGMENT_NAME_LEN && all_hex(name) {
        return (Some(name), FileKind::Segment);
    }
    if let Some(stem) = name.strip_suffix(".manifest.json")
        && stem.len() == SEGMENT_NAME_LEN
        && all_hex(stem)
    {
        return (Some(stem), FileKind::Manifest);
    }
    if let Some(stem) = name.strip_suffix(".partial.manifest.json")
        && stem.len() == SEGMENT_NAME_LEN
        && all_hex(stem)
    {
        return (Some(stem), FileKind::Manifest);
    }
    if let Some(stem) = name.strip_suffix(".partial")
        && stem.len() == SEGMENT_NAME_LEN
        && all_hex(stem)
    {
        return (Some(stem), FileKind::Partial);
    }
    (None, FileKind::Segment)
}

fn all_hex(s: &str) -> bool {
    s.chars().all(|c| c.is_ascii_hexdigit())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn touch(dir: &Path, name: &str, body: &[u8]) {
        std::fs::write(dir.join(name), body).unwrap();
    }

    fn seg_name(timeline: u32, log_id: u32, seg_no: u32) -> String {
        SegmentName {
            timeline,
            log_id,
            seg_no,
        }
        .format()
    }

    #[tokio::test(flavor = "current_thread")]
    async fn keeps_segments_above_cutoff() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        // Bodies are stand-ins; trimmer keys on filename + size only.
        touch(dir, &seg_name(1, 0, 1), b"seg-1-body");
        touch(dir, &seg_name(1, 0, 2), b"seg-2-body");
        touch(dir, &seg_name(1, 0, 3), b"seg-3-body");
        // Cutoff inside segment 2: 1 goes, 2 stays (cutoff below its end), 3 stays.
        let cutoff = SegmentName {
            timeline: 1,
            log_id: 0,
            seg_no: 2,
        }
        .start_lsn(WAL_SEG_SIZE)
            + 4096;
        let report = trim_below_lsn(dir, cutoff).await.unwrap();
        assert_eq!(report.segments_removed, 1, "{report:?}");
        assert_eq!(report.bytes_freed as usize, b"seg-1-body".len());
        assert!(!dir.join(seg_name(1, 0, 1)).exists());
        assert!(dir.join(seg_name(1, 0, 2)).exists());
        assert!(dir.join(seg_name(1, 0, 3)).exists());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn removes_manifest_and_partial_siblings() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        let seg = seg_name(1, 0, 5);
        touch(dir, &seg, b"seg-body");
        touch(dir, &format!("{seg}.manifest.json"), b"{}");
        touch(dir, &format!("{seg}.partial"), b"partial-body");
        touch(dir, &format!("{seg}.partial.manifest.json"), b"{}");
        let cutoff = SegmentName {
            timeline: 1,
            log_id: 0,
            seg_no: 5,
        }
        .start_lsn(WAL_SEG_SIZE)
            + WAL_SEG_SIZE
            + 1;
        let report = trim_below_lsn(dir, cutoff).await.unwrap();
        assert_eq!(report.segments_removed, 1, "{report:?}");
        assert_eq!(report.manifests_removed, 2, "{report:?}");
        assert_eq!(report.partials_removed, 1, "{report:?}");
        assert!(dir.read_dir().unwrap().next().is_none(), "dir not empty");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn leaves_unknown_files_alone() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        touch(dir, "README", b"hi");
        touch(dir, "00000001-bad.dat", b"x");
        let cutoff = u64::MAX;
        let report = trim_below_lsn(dir, cutoff).await.unwrap();
        assert_eq!(report.segments_removed, 0);
        assert!(dir.join("README").exists());
        assert!(dir.join("00000001-bad.dat").exists());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn missing_dir_returns_empty_report() {
        let missing = std::path::Path::new("/this/path/does/not/exist/walshadow-retention-test");
        let report = trim_below_lsn(missing, u64::MAX).await.unwrap();
        assert_eq!(report.segments_removed, 0);
        assert_eq!(report.bytes_freed, 0);
    }

    #[cfg(unix)]
    #[tokio::test(flavor = "current_thread")]
    async fn skips_files_with_non_utf8_names() {
        use std::os::unix::ffi::OsStrExt;
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        let raw = std::ffi::OsStr::from_bytes(&[0xFF, 0xFE, b'.', b'b', b'i', b'n']);
        std::fs::write(dir.join(raw), b"x").unwrap();
        let report = trim_below_lsn(dir, u64::MAX).await.unwrap();
        assert_eq!(report.segments_removed, 0);
        assert!(dir.join(raw).exists());
    }

    #[test]
    fn classify_picks_segment_manifest_partial() {
        let s = seg_name(1, 0, 9);
        let (stem, kind) = classify(&s);
        assert_eq!(stem, Some(s.as_str()));
        assert!(matches!(kind, FileKind::Segment));

        let m = format!("{s}.manifest.json");
        let (stem, kind) = classify(&m);
        assert_eq!(stem, Some(s.as_str()));
        assert!(matches!(kind, FileKind::Manifest));

        let p = format!("{s}.partial");
        let (stem, kind) = classify(&p);
        assert_eq!(stem, Some(s.as_str()));
        assert!(matches!(kind, FileKind::Partial));
    }
}
