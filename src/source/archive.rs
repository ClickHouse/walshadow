//! Read archived WAL using source timeline history.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use anyhow::{Context, Result};
use futures::{StreamExt, stream as futures_stream};
use walrus::config::Settings;
use walrus::storage::DynStorage;

use crate::record::{WAL_SEG_SIZE, segments_covering};
use crate::source::timeline::{TimelineHistory, history_filename};

/// Initialize archive storage once so invalid `[backup]` settings fail at startup.
#[derive(Clone)]
pub struct Archive {
    settings: Settings,
    storage: DynStorage,
}

impl Archive {
    pub fn open(settings: Settings) -> Result<Self> {
        let storage = settings.build_storage().context("build archive storage")?;
        Ok(Self { settings, storage })
    }

    /// Read archived WAL from `lsn` to end of its segment.
    ///
    /// At a timeline fork, PostgreSQL copies earlier WAL into a segment named
    /// for its new timeline (`XLogInitNewTimeline`). Read that file because
    /// its parent timeline may have archived only a `.partial` file.
    ///
    /// Read into memory to avoid writing and reading 16 MiB of WAL on disk,
    /// or leaving temporary files behind when recovery is interrupted.
    pub async fn read_segment(
        &self,
        history: &TimelineHistory,
        lsn: u64,
    ) -> Result<(String, Vec<u8>)> {
        let seg_start = lsn / WAL_SEG_SIZE * WAL_SEG_SIZE;
        let timeline = history
            .tli_of_segment(seg_start, WAL_SEG_SIZE)
            .context("archive position outside source history")?;
        let mut refused = None;
        for name in segment_candidates(history, timeline, seg_start) {
            let attempt =
                walrus::pg::wal::fetch::read_segment(&self.settings, &self.storage, &name).await;
            match attempt {
                Ok(mut bytes) if bytes.len() == WAL_SEG_SIZE as usize => {
                    bytes.drain(..(lsn - seg_start) as usize);
                    return Ok((name, bytes));
                }
                Ok(bytes) => {
                    refused.get_or_insert_with(|| {
                        anyhow::anyhow!(
                            "archived WAL {name} has {} bytes, expected {WAL_SEG_SIZE}",
                            bytes.len(),
                        )
                    });
                }
                Err(e) => {
                    refused.get_or_insert_with(|| e.context(format!("fetch {name}")));
                }
            }
        }
        Err(refused.expect("every segment has at least one candidate name"))
    }

    pub async fn discover_history(
        &self,
        known: &TimelineHistory,
        lsn: u64,
    ) -> Result<Option<TimelineHistory>> {
        let mut newest = None;
        for tli in (known.target() + 1)..=u32::MAX {
            let name = history_filename(tli);
            let Ok(raw) =
                walrus::pg::wal::fetch::read_segment(&self.settings, &self.storage, &name).await
            else {
                break;
            };
            let parsed =
                TimelineHistory::parse(tli, &raw).with_context(|| format!("parse {name}"))?;
            let proved = newest.as_ref().unwrap_or(known);
            if !extends(proved, &parsed) || parsed.tli_of_point(lsn).is_none() {
                break;
            }
            newest = Some(parsed);
        }
        Ok(newest)
    }

    /// Prefetch WAL from `start` until `timeline` ends so replay can switch timelines.
    pub fn feed(
        &self,
        history: TimelineHistory,
        timeline: u32,
        start: u64,
        concurrency: usize,
    ) -> ArchiveFeed {
        let end = history.switchpoint_of(timeline).unwrap_or(u64::MAX);
        let archive = self.clone();
        // `buffered` advances downloads only while polled. A full channel
        // pauses all downloads, so allow room for `concurrency` segments.
        let (tx, rx) = tokio::sync::mpsc::channel(concurrency);
        // Completed downloads can wait in `buffered` as well as in this channel.
        // Hold a permit until replay receives each segment to limit memory use
        // to `concurrency` segments plus one being replayed.
        let budget = Arc::new(tokio::sync::Semaphore::new(concurrency));
        let fetch_nanos = Arc::new(AtomicU64::new(0));
        let elapsed = fetch_nanos.clone();
        let task = tokio::spawn(async move {
            let starts = std::iter::successors(Some(start), |lsn| {
                (lsn / WAL_SEG_SIZE + 1).checked_mul(WAL_SEG_SIZE)
            })
            .take_while(|lsn| *lsn < end);
            let pending = futures_stream::iter(starts)
                .map(|lsn| {
                    let (archive, history, elapsed) = (&archive, &history, &elapsed);
                    let budget = budget.clone();
                    async move {
                        let permit = budget.acquire_owned().await.expect("budget stays open");
                        let began = Instant::now();
                        let result =
                            archive
                                .read_segment(history, lsn)
                                .await
                                .map(|(_, mut bytes)| {
                                    bytes.truncate(bytes.len().min((end - lsn) as usize));
                                    (lsn, bytes, permit)
                                });
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
        ArchiveFeed {
            wait_nanos: AtomicU64::new(0),
            rx,
            task,
            fetch_nanos,
        }
    }
}

fn segment_candidates(history: &TimelineHistory, owner: u32, seg_start: u64) -> Vec<String> {
    let name_of = |tli| {
        segments_covering(tli, seg_start..seg_start + WAL_SEG_SIZE)[0]
            .format()
            .to_string()
    };
    let mut names: Vec<String> = history
        .entries()
        .iter()
        .map(|e| e.tli)
        .filter(|tli| *tli >= owner)
        .rev()
        .map(name_of)
        .collect();
    names.push(format!("{}.partial", name_of(owner)));
    names
}

fn extends(known: &TimelineHistory, next: &TimelineHistory) -> bool {
    let mut entries = next.entries().iter();
    known.entries().iter().all(|entry| {
        entries.find(|e| e.tli == entry.tli).is_some_and(|found| {
            found.begin == entry.begin && entry.end.is_none_or(|end| found.end == Some(end))
        })
    })
}

/// Check that archive and source have matching timeline histories, since replay
/// uses source history to choose archived segment names.
pub async fn verify_history(
    settings: &Settings,
    storage: &DynStorage,
    source: &TimelineHistory,
) -> Result<()> {
    let name = history_filename(source.target());
    let raw = walrus::pg::wal::fetch::read_segment(settings, storage, &name)
        .await
        .with_context(|| format!("fetch {name}"))?;
    let archived =
        TimelineHistory::parse(source.target(), &raw).with_context(|| format!("parse {name}"))?;
    anyhow::ensure!(
        archived.entries() == source.entries(),
        "archived {name} disagrees with source timeline history",
    );
    Ok(())
}

/// Keep a downloaded segment's memory permit until replay receives it.
type ArchiveSegment = (u64, Vec<u8>, tokio::sync::OwnedSemaphorePermit);

pub struct ArchiveFeed {
    pub wait_nanos: AtomicU64,
    rx: tokio::sync::mpsc::Receiver<Result<ArchiveSegment>>,
    task: tokio::task::JoinHandle<()>,
    pub fetch_nanos: Arc<AtomicU64>,
}

impl ArchiveFeed {
    pub async fn next(&mut self) -> Option<Result<(u64, Vec<u8>)>> {
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

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::*;
    use std::fs;

    /// Match `archive_command`: store history uncompressed under `wal_005/`.
    async fn archive_history_file(settings: &Settings, storage: &DynStorage, tli: u32, body: &str) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(history_filename(tli));
        tokio::fs::write(&path, body).await.unwrap();
        walrus::pg::wal::push::handle(settings, storage.clone(), &path)
            .await
            .unwrap();
    }

    fn fs_archive(root: &Path) -> (Settings, DynStorage) {
        let settings = Settings {
            storage: walrus::config::StorageSettings::Fs {
                path: root.display().to_string(),
            },
            ..Default::default()
        };
        let storage = settings.build_storage().unwrap();
        (settings, storage)
    }

    #[tokio::test]
    async fn reject_an_archive_missing_the_sources_own_history() {
        let tmp = tempfile::tempdir().unwrap();
        let (settings, storage) = fs_archive(&tmp.path().join("archive"));
        archive_history_file(&settings, &storage, 2, "1\t0/3000000\tpromotion\n").await;
        let source = TimelineHistory::parse(3, b"1\t0/3000000\tpromotion\n").unwrap();
        let err = verify_history(&settings, &storage, &source)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("00000003.history"), "{err:#}");
    }

    #[tokio::test]
    async fn archived_ids_need_not_be_consecutive() {
        let tmp = tempfile::tempdir().unwrap();
        let (settings, storage) = fs_archive(&tmp.path().join("archive"));
        archive_history_file(&settings, &storage, 4, "1\t0/3000000\tpromotion\n").await;
        let source = TimelineHistory::parse(4, b"1\t0/3000000\tpromotion\n").unwrap();
        verify_history(&settings, &storage, &source).await.unwrap();
    }

    #[tokio::test]
    async fn reject_archived_history_with_different_ancestry_or_switchpoint() {
        for body in ["1\t0/4000000\tpromotion\n", "2\t0/3000000\tpromotion\n"] {
            let tmp = tempfile::tempdir().unwrap();
            let (settings, storage) = fs_archive(&tmp.path().join("archive"));
            archive_history_file(&settings, &storage, 3, body).await;
            let source = TimelineHistory::parse(3, b"1\t0/3000000\tpromotion\n").unwrap();
            let err = verify_history(&settings, &storage, &source)
                .await
                .unwrap_err();
            assert!(err.to_string().contains("disagrees"), "{err:#}");
        }
    }

    async fn archive_segment(settings: &Settings, storage: &DynStorage, name: &str, fill: u8) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(name);
        fs::write(&path, vec![fill; WAL_SEG_SIZE as usize]).unwrap();
        walrus::pg::wal::push::handle(settings, storage.clone(), &path)
            .await
            .unwrap();
    }

    /// TL4 promoted inside segment 39 and TL5 promoted before it completed, so
    /// only TL5 copied the whole segment forward
    #[tokio::test]
    async fn read_segment_falls_back_to_a_descendants_copy() {
        let tmp = tempfile::tempdir().unwrap();
        let (settings, storage) = fs_archive(&tmp.path().join("archive"));
        let history = TimelineHistory::parse(
            5,
            b"1\t3/10000000\tpromotion\n\
              2\t3/20000000\tpromotion\n\
              3\t3/39000100\tpromotion\n\
              4\t3/39000900\tpromotion\n",
        )
        .unwrap();
        let seg_start = 0x3_3900_0000u64;
        archive_segment(&settings, &storage, "000000050000000300000039", 0x5a).await;
        let archive = Archive {
            settings: settings.clone(),
            storage: storage.clone(),
        };
        let (name, bytes) = archive.read_segment(&history, seg_start).await.unwrap();
        assert_eq!(name, "000000050000000300000039");
        assert_eq!(bytes.len(), WAL_SEG_SIZE as usize);
        assert!(bytes.iter().all(|b| *b == 0x5a));
    }

    #[tokio::test]
    async fn read_segment_falls_back_to_a_partial() {
        let tmp = tempfile::tempdir().unwrap();
        let (settings, storage) = fs_archive(&tmp.path().join("archive"));
        archive_segment(
            &settings,
            &storage,
            "000000010000000000000062.partial",
            0x17,
        )
        .await;
        let archive = Archive {
            settings: settings.clone(),
            storage: storage.clone(),
        };
        let (name, bytes) = archive
            .read_segment(&TimelineHistory::root(1), 0x6200_0000)
            .await
            .unwrap();
        assert_eq!(name, "000000010000000000000062.partial");
        assert!(bytes.iter().all(|b| *b == 0x17));
    }

    #[tokio::test]
    async fn discover_history_extends_the_proved_chain_and_refuses_a_fork() {
        let tmp = tempfile::tempdir().unwrap();
        let (settings, storage) = fs_archive(&tmp.path().join("archive"));
        let known = TimelineHistory::parse(
            4,
            b"1\t3/10000000\tpromotion\n2\t3/20000000\tpromotion\n3\t3/39000100\tpromotion\n",
        )
        .unwrap();
        let archive = Archive {
            settings: settings.clone(),
            storage: storage.clone(),
        };
        let lsn = 0x3_3900_0000u64;
        assert!(
            archive
                .discover_history(&known, lsn)
                .await
                .unwrap()
                .is_none(),
            "nothing archived past the proved chain",
        );

        archive_history_file(
            &settings,
            &storage,
            5,
            "1\t3/10000000\tpromotion\n2\t3/20000000\tpromotion\n3\t3/39000100\tpromotion\n4\t3/39000900\tpromotion\n",
        )
        .await;
        let found = archive
            .discover_history(&known, lsn)
            .await
            .unwrap()
            .expect("timeline 5 extends the chain");
        assert_eq!(found.target(), 5);
        assert_eq!(found.switchpoint_of(4), Some(0x3_3900_0900));

        archive_history_file(
            &settings,
            &storage,
            6,
            "1\t3/10000000\tpromotion\n2\t3/20000000\tpromotion\n3\t3/39000100\tpromotion\n4\t3/50000000\tpromotion\n5\t3/60000000\tpromotion\n",
        )
        .await;
        assert_eq!(
            archive
                .discover_history(&known, lsn)
                .await
                .unwrap()
                .map(|h| h.target()),
            Some(5),
            "a chain that moved timeline 4's switchpoint is a different lineage",
        );
    }

    #[test]
    fn segment_candidates_try_the_newest_branch_first() {
        let history = TimelineHistory::parse(
            5,
            b"1\t3/10000000\tpromotion\n2\t3/20000000\tpromotion\n3\t3/39000100\tpromotion\n4\t3/39000900\tpromotion\n",
        )
        .unwrap();
        assert_eq!(
            segment_candidates(&history, 4, 0x3_3900_0000),
            [
                "000000050000000300000039",
                "000000040000000300000039",
                "000000040000000300000039.partial",
            ],
        );
    }

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
        let archive = Archive { settings, storage };
        let mut reader = archive.feed(TimelineHistory::root(1), 1, 42, 4);
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
    async fn archive_prefetch_reads_fork_segment_under_descendant_timeline() {
        let tmp = tempfile::tempdir().unwrap();
        let wal = tmp.path().join("wal_005");
        fs::create_dir(&wal).unwrap();
        fs::write(
            wal.join("000000010000000000000000"),
            vec![1; WAL_SEG_SIZE as usize],
        )
        .unwrap();
        fs::write(
            wal.join("000000010000000000000001.partial"),
            vec![9; WAL_SEG_SIZE as usize],
        )
        .unwrap();
        fs::write(
            wal.join("000000020000000000000001"),
            vec![2; WAL_SEG_SIZE as usize],
        )
        .unwrap();
        let settings = walrus::config::Settings {
            storage: walrus::config::StorageSettings::Fs {
                path: tmp.path().display().to_string(),
            },
            ..Default::default()
        };
        let history = TimelineHistory::parse(2, b"1\t0/10000A0\n").unwrap();
        let mut feed = Archive::open(settings)
            .unwrap()
            .feed(history, 1, WAL_SEG_SIZE - 42, 2);
        let (lsn, bytes) = feed.next().await.unwrap().unwrap();
        assert_eq!(lsn, WAL_SEG_SIZE - 42);
        assert_eq!(bytes, vec![1; 42]);
        let (lsn, bytes) = feed.next().await.unwrap().unwrap();
        assert_eq!(lsn, WAL_SEG_SIZE);
        assert_eq!(bytes, vec![2; 0xA0], "cut at ancestor's switchpoint");
        assert!(feed.next().await.is_none(), "feed ends at switchpoint");
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

        let (name, bytes) = Archive { settings, storage }
            .read_segment(&TimelineHistory::root(1), 0)
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
        // Read archives written with different compression settings.
        let reading = walrus::config::Settings {
            storage,
            ..Default::default()
        };
        let (_, bytes) = Archive {
            settings: reading,
            storage: built,
        }
        .read_segment(&TimelineHistory::root(1), 0)
        .await
        .unwrap();
        assert_eq!(bytes.len(), WAL_SEG_SIZE as usize);
        assert!(bytes.iter().all(|b| *b == 7));
    }

    #[tokio::test]
    async fn archive_fetch_slices_from_mid_segment() {
        // Resume at requested LSN to keep replay aligned.
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
        let (name, bytes) = Archive { settings, storage }
            .read_segment(&TimelineHistory::root(1), offset)
            .await
            .unwrap();
        assert_eq!(name, "000000010000000000000000");
        assert_eq!(bytes.len(), (WAL_SEG_SIZE - offset) as usize);
        assert_eq!(bytes, pattern[offset as usize..]);
    }
}
