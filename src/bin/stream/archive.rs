//! Archive WAL fetch: pull segments the source no longer has from the
//! backup archive, into shadow's `pg_wal`.

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use anyhow::{Context, Result};
use futures::{StreamExt, stream as futures_stream};
use walrus::pg::backup::format_pg_lsn;
use walshadow::record::{WAL_SEG_SIZE, segments_covering};
use walshadow::wal_stream::WalStream;

/// Fetch archived WAL for source recovery, returning the bytes that begin at
/// exactly `start_lsn`. The archive stores whole 16 MiB segment files, so
/// fetch the single segment containing `start_lsn` (aligned range → one
/// entry) and slice off the already-consumed prefix — the returned bytes line
/// up with `WalStream::next_lsn`, which is byte- not segment-aligned in steady
/// state.
///
/// Reading whole into memory keeps a prefetch slot off the staging disk, which
/// otherwise costs a 32 MiB round trip per 16 MiB of WAL and leaves a tmp file
/// behind on an aborted leg
pub(crate) async fn fetch_archive_segment(
    settings: &walrus::config::Settings,
    storage: &walrus::storage::DynStorage,
    timeline: u32,
    start_lsn: u64,
) -> Result<(String, Vec<u8>)> {
    let seg_start = WalStream::align_down(start_lsn, WAL_SEG_SIZE);
    let name = segments_covering(timeline, seg_start..seg_start + WAL_SEG_SIZE)[0].format();
    let mut bytes = walrus::pg::wal::fetch::read_segment(settings, storage, &name).await?;
    if bytes.len() != WAL_SEG_SIZE as usize {
        anyhow::bail!(
            "archived WAL {name} has {} bytes, expected {WAL_SEG_SIZE}",
            bytes.len(),
        );
    }
    bytes.drain(..(start_lsn - seg_start) as usize);
    Ok((name, bytes))
}

/// Fetched segment, holding the budget slot it occupies until the pump takes it
type ArchiveSegment = (u64, Vec<u8>, tokio::sync::OwnedSemaphorePermit);

pub(crate) struct ArchiveFeed {
    pub(crate) wait_nanos: AtomicU64,
    pub(crate) rx: tokio::sync::mpsc::Receiver<Result<ArchiveSegment>>,
    pub(crate) task: tokio::task::JoinHandle<()>,
    pub(crate) fetch_nanos: Arc<AtomicU64>,
}

impl ArchiveFeed {
    pub(crate) fn spawn(
        settings: walrus::config::Settings,
        storage: walrus::storage::DynStorage,
        timeline: u32,
        start: u64,
        concurrency: usize,
    ) -> Self {
        // `buffered` only advances its fetches while the stream is polled, so
        // a worker parked on a full channel freezes every download in flight.
        // Capacity below `concurrency` caps real depth at that capacity
        let (tx, rx) = tokio::sync::mpsc::channel(concurrency);
        // Ordered consumption lets a completed fetch sit in `buffered` waiting
        // its turn, so slots alone bound nothing. A permit taken before the
        // download and released at handoff holds resident segments to
        // `concurrency`, plus the one the pump is replaying
        let budget = Arc::new(tokio::sync::Semaphore::new(concurrency));
        let fetch_nanos = Arc::new(AtomicU64::new(0));
        let elapsed = fetch_nanos.clone();
        let task = tokio::spawn(async move {
            let starts = std::iter::successors(Some(start), |lsn| {
                (lsn / WAL_SEG_SIZE + 1).checked_mul(WAL_SEG_SIZE)
            });
            let pending = futures_stream::iter(starts)
                .map(|lsn| {
                    let (settings, storage, elapsed) = (&settings, &storage, &elapsed);
                    let budget = budget.clone();
                    async move {
                        let permit = budget.acquire_owned().await.expect("budget stays open");
                        let began = Instant::now();
                        let result = fetch_archive_segment(settings, storage, timeline, lsn)
                            .await
                            .map(|(_, bytes)| (lsn, bytes, permit));
                        elapsed.fetch_add(began.elapsed().as_nanos() as u64, Ordering::Relaxed);
                        result
                    }
                })
                .buffered(concurrency);
            tokio::pin!(pending);
            while let Some(result) = pending.next().await {
                let failed = result.is_err();
                if tx.send(result).await.is_err() || failed {
                    break;
                }
            }
        });
        Self {
            wait_nanos: AtomicU64::new(0),
            rx,
            task,
            fetch_nanos,
        }
    }

    pub(crate) async fn next(&mut self) -> Option<Result<(u64, Vec<u8>)>> {
        let _elapsed = ArchiveWait {
            nanos: &self.wait_nanos,
            started: Instant::now(),
        };
        let fetched = self.rx.recv().await?;
        Some(fetched.map(|(lsn, bytes, _budget)| (lsn, bytes)))
    }
}

struct ArchiveWait<'a> {
    nanos: &'a AtomicU64,
    started: Instant,
}

impl Drop for ArchiveWait<'_> {
    fn drop(&mut self) {
        self.nanos
            .fetch_add(self.started.elapsed().as_nanos() as u64, Ordering::Relaxed);
    }
}

impl Drop for ArchiveFeed {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// Fetch WAL `[start_lsn, end_lsn]` from archive storage into shadow's `pg_wal/`.
pub(crate) async fn fetch_wal_into_pg_wal(
    settings: &walrus::config::Settings,
    storage: walrus::storage::DynStorage,
    shadow_data_dir: &Path,
    start_lsn: u64,
    end_lsn: u64,
    timeline: u32,
) -> Result<()> {
    let pg_wal_dir = shadow_data_dir.join("pg_wal");
    tokio::fs::create_dir_all(&pg_wal_dir)
        .await
        .with_context(|| format!("create {}", pg_wal_dir.display()))?;
    let segments = segments_covering(timeline, start_lsn..end_lsn.saturating_add(1));
    for seg in &segments {
        let name = seg.format();
        let dst = pg_wal_dir.join(&name);
        // Off: the range is enumerated explicitly, so read-ahead would only
        // duplicate the next fetch & risk downloading past end_lsn
        walrus::pg::wal::fetch::handle(
            settings,
            storage.clone(),
            &name,
            &dst,
            walrus::pg::wal::fetch::Prefetch::Off,
        )
        .await
        .with_context(|| format!("fetch WAL {name} -> {}", dst.display()))?;
    }
    // A direct bootstrap's tar carries pg_wal whole, history files included;
    // this leg enumerates segments, so without it the shadow lands on a
    // promoted branch with no `<tli>.history` and has to ask the walsender for
    // one on its first connection. Absent from the archive is survivable —
    // walshadow serves it from `seed_shadow_branches` — so warn, don't fail.
    let history = walshadow::timeline::history_filename(timeline);
    if timeline > 1 {
        let dst = pg_wal_dir.join(&history);
        match walrus::pg::wal::fetch::handle(
            settings,
            storage.clone(),
            &history,
            &dst,
            walrus::pg::wal::fetch::Prefetch::Off,
        )
        .await
        {
            Ok(()) => {}
            Err(e) => tracing::warn!(
                target: "walshadow::bootstrap",
                timeline,
                error = %e,
                "archive holds no {history}; the shadow will ask the walsender for it",
            ),
        }
    }
    tracing::info!(
        target: "walshadow::bootstrap",
        fetched = segments.len(),
        start_lsn = format_pg_lsn(start_lsn).to_string(),
        end_lsn = format_pg_lsn(end_lsn).to_string(),
        timeline,
        "hydrated shadow pg_wal from object store",
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[tokio::test]
    async fn archive_prefetch_preserves_order_and_stops_at_gap() {
        let tmp = tempfile::tempdir().unwrap();
        let settings = walrus::config::Settings {
            storage: walrus::config::StorageSettings::Fs {
                path: tmp.path().join("archive").display().to_string(),
            },
            ..Default::default()
        };
        let storage = settings.build_storage().unwrap();
        for index in [0u64, 1, 3] {
            let name =
                segments_covering(1, index * WAL_SEG_SIZE..(index + 1) * WAL_SEG_SIZE)[0].format();
            let path = tmp.path().join(name);
            fs::write(&path, vec![index as u8; WAL_SEG_SIZE as usize]).unwrap();
            walrus::pg::wal::push::handle(&settings, storage.clone(), &path)
                .await
                .unwrap();
        }
        let mut reader = ArchiveFeed::spawn(settings, storage, 1, 42, 4);
        let (lsn, bytes) = reader.next().await.unwrap().unwrap();
        assert_eq!(lsn, 42);
        assert_eq!(bytes.len(), WAL_SEG_SIZE as usize - 42);
        assert!(bytes.iter().all(|b| *b == 0));
        let (lsn, bytes) = reader.next().await.unwrap().unwrap();
        assert_eq!(lsn, WAL_SEG_SIZE);
        assert!(bytes.iter().all(|b| *b == 1));
        assert!(reader.next().await.unwrap().is_err());
        assert!(
            reader.next().await.is_none(),
            "must not skip missing segment"
        );
    }

    #[tokio::test]
    async fn archive_prefetch_drop_cancels_worker() {
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        let task = tokio::spawn(async move {
            let _tx = tx;
            std::future::pending::<()>().await;
        });
        let abort = task.abort_handle();
        drop(ArchiveFeed {
            wait_nanos: AtomicU64::new(0),
            rx,
            task,
            fetch_nanos: Arc::new(AtomicU64::new(0)),
        });
        tokio::task::yield_now().await;
        assert!(abort.is_finished());
    }

    #[tokio::test]
    async fn archive_fetch_reads_exact_segment() {
        let tmp = tempfile::tempdir().unwrap();
        let archive = tmp.path().join("archive");
        let segment_path = tmp.path().join("000000010000000000000000");
        fs::write(&segment_path, vec![0; WAL_SEG_SIZE as usize]).unwrap();
        let settings = walrus::config::Settings {
            storage: walrus::config::StorageSettings::Fs {
                path: archive.display().to_string(),
            },
            ..Default::default()
        };
        let storage = settings.build_storage().unwrap();
        walrus::pg::wal::push::handle(&settings, storage.clone(), &segment_path)
            .await
            .unwrap();

        let (name, bytes) = fetch_archive_segment(&settings, &storage, 1, 0)
            .await
            .unwrap();
        assert_eq!(name, "000000010000000000000000");
        assert_eq!(bytes.len(), WAL_SEG_SIZE as usize);
    }

    #[tokio::test]
    async fn archive_fetch_falls_back_across_compressions() {
        let tmp = tempfile::tempdir().unwrap();
        let segment_path = tmp.path().join("000000010000000000000000");
        fs::write(&segment_path, vec![7; WAL_SEG_SIZE as usize]).unwrap();
        let storage = walrus::config::StorageSettings::Fs {
            path: tmp.path().join("archive").display().to_string(),
        };
        let pushed = walrus::config::Settings {
            storage: storage.clone(),
            compression: walrus::compression::Method::None,
            ..Default::default()
        };
        let built = pushed.build_storage().unwrap();
        walrus::pg::wal::push::handle(&pushed, built.clone(), &segment_path)
            .await
            .unwrap();
        // A bucket written under another compression must still read back
        let reading = walrus::config::Settings {
            storage,
            ..Default::default()
        };
        let (_, bytes) = fetch_archive_segment(&reading, &built, 1, 0).await.unwrap();
        assert_eq!(bytes.len(), WAL_SEG_SIZE as usize);
        assert!(bytes.iter().all(|b| *b == 7));
    }

    #[tokio::test]
    async fn archive_fetch_slices_from_mid_segment() {
        // A mid-segment resume LSN must return the segment's tail beginning at
        // that LSN, not the whole segment (which would misalign the replay).
        let tmp = tempfile::tempdir().unwrap();
        let archive = tmp.path().join("archive");
        let segment_path = tmp.path().join("000000010000000000000000");
        let pattern: Vec<u8> = (0..WAL_SEG_SIZE as usize)
            .map(|i| (i % 251) as u8)
            .collect();
        fs::write(&segment_path, &pattern).unwrap();
        let settings = walrus::config::Settings {
            storage: walrus::config::StorageSettings::Fs {
                path: archive.display().to_string(),
            },
            ..Default::default()
        };
        let storage = settings.build_storage().unwrap();
        walrus::pg::wal::push::handle(&settings, storage.clone(), &segment_path)
            .await
            .unwrap();

        let offset = WAL_SEG_SIZE / 2;
        let (name, bytes) = fetch_archive_segment(&settings, &storage, 1, offset)
            .await
            .unwrap();
        // Same segment file, sliced to begin at the mid-segment LSN.
        assert_eq!(name, "000000010000000000000000");
        assert_eq!(bytes.len(), (WAL_SEG_SIZE - offset) as usize);
        assert_eq!(bytes, pattern[offset as usize..]);
    }
}
