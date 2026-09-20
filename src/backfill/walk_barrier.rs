//! Checkpoint coordination for a resumable page walk
//!
//! A walked file counts as done only once every tuple it produced is durable:
//! in ClickHouse, or in one of the spools the gate and drain fsync. Ordering
//! comes from the walk channel itself: the sink registers a finished file,
//! then sends an empty slab, so the gate reading that slab has already
//! consumed every tuple registered before the slab was sent.
//!
//! Stages publish on their own timer rather than answering a request: the
//! checkpointer combines whatever reports it finds, and a file whose tuples
//! the drain has not reached yet simply waits for the next tick.
//!
//! Files the checkpointer never records are re-walked, which duplicates rows
//! `ReplacingMergeTree` collapses at unchanged `_lsn`. Files it records
//! wrongly would be lost, so every rule here errs towards re-walking.

use std::collections::VecDeque;

use ahash::HashSet;
use std::time::Duration;

use tokio::sync::Mutex;

use crate::backfill::spool::SpoolMark;

/// Seconds between stage publishes and checkpoint writes
pub const WALK_CHECKPOINT_PERIOD: Duration = Duration::from_secs(30);

#[derive(Clone, Copy, Default)]
struct DrainReport {
    /// Tuples taken off the gate's output channel
    consumed: u64,
    /// Every seq below this is placed, so the tail can prove it
    next_seq: u64,
    spool: SpoolMark,
}

/// What one drained archive part held. A part is only skippable once every
/// heap file it tapped is recorded, and it carried no SLRU segment: the gate
/// rebuilds its transaction view from scratch on every attempt
struct PartTally {
    key: String,
    files: Vec<String>,
    slru: bool,
}

/// Marks and file names a checkpoint may record once the tail proves
/// `next_seq`
pub struct WalkProof {
    pub files: Vec<String>,
    pub gate_deferred: SpoolMark,
    pub toast_deferred: SpoolMark,
    pub next_seq: u64,
}

#[derive(Default)]
pub struct WalkBarrier(Mutex<WalkProgress>);

#[derive(Default)]
struct WalkProgress {
    /// Sink to gate: files whose tuples are all on the walk channel
    finished: VecDeque<String>,
    /// Gate to checkpointer: files it has consumed, each with the forwarded
    /// tuple count the drain must reach before they are durable
    consumed: Vec<(String, u64)>,
    gate: SpoolMark,
    drain: DrainReport,
    /// Drained parts still waiting on one of their files to be recorded
    parts: Vec<PartTally>,
}

impl WalkBarrier {
    /// Sink: called after the file's last slab is on the channel, before its
    /// end-of-file slab
    pub async fn finished_file(&self, path: String) {
        self.0.lock().await.finished.push_back(path);
    }

    /// Gate: an empty slab stands for one finished file. Which name it pops
    /// does not matter, only that every registered file's tuples precede the
    /// slab that pops it
    pub async fn pop_finished(&self) -> Option<String> {
        self.0.lock().await.finished.pop_front()
    }

    pub async fn publish_gate(&self, files: Vec<(String, u64)>, deferred: SpoolMark) {
        let mut progress = self.0.lock().await;
        progress.gate = deferred;
        progress.consumed.extend(files);
    }

    /// Sink: one archive part fully dispatched
    pub async fn part_drained(&self, key: String, files: Vec<String>, slru: bool) {
        self.0
            .lock()
            .await
            .parts
            .push(PartTally { key, files, slru });
    }

    pub async fn publish_drain(&self, consumed: u64, next_seq: u64, spool: SpoolMark) {
        self.0.lock().await.drain = DrainReport {
            consumed,
            next_seq,
            spool,
        };
    }

    pub async fn collect(&self) -> Option<WalkProof> {
        let mut progress = self.0.lock().await;
        let drain = progress.drain;
        let mut files = Vec::new();
        progress.consumed.retain(|(path, forwarded)| {
            let durable = *forwarded <= drain.consumed;
            if durable {
                files.push(path.clone());
            }
            !durable
        });
        if files.is_empty() {
            return None;
        }
        Some(WalkProof {
            files,
            gate_deferred: progress.gate,
            toast_deferred: drain.spool,
            next_seq: drain.next_seq,
        })
    }

    /// Parts whose every tapped file is now recorded. Called with the whole
    /// recorded set, so a part waits however many ticks its files take
    pub async fn settled_parts(&self, recorded: &HashSet<String>) -> Vec<String> {
        let mut out = Vec::new();
        self.0.lock().await.parts.retain(|p| {
            let settled = !p.slru && p.files.iter().all(|f| recorded.contains(f));
            if settled {
                out.push(p.key.clone());
            }
            !settled
        });
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn collect_holds_files_the_drain_has_not_reached() {
        let barrier = WalkBarrier::default();
        barrier.finished_file("base/5/16400".into()).await;
        barrier.finished_file("base/5/16401".into()).await;
        let first = barrier.pop_finished().await.unwrap();
        let second = barrier.pop_finished().await.unwrap();
        barrier
            .publish_gate(
                vec![(first, 10), (second, 40)],
                SpoolMark {
                    records: 2,
                    bytes: 64,
                },
            )
            .await;
        barrier
            .publish_drain(
                20,
                7,
                SpoolMark {
                    records: 1,
                    bytes: 32,
                },
            )
            .await;

        let proof = barrier.collect().await.unwrap();
        assert_eq!(proof.files, vec!["base/5/16400".to_string()]);
        assert_eq!(proof.gate_deferred.records, 2);
        assert_eq!(proof.toast_deferred.bytes, 32);
        assert_eq!(proof.next_seq, 7);
        assert!(
            barrier.collect().await.is_none(),
            "a recorded file is not offered twice"
        );

        barrier.publish_drain(40, 9, SpoolMark::default()).await;
        assert_eq!(
            barrier.collect().await.unwrap().files,
            vec!["base/5/16401".to_string()],
        );
    }

    #[tokio::test]
    async fn pop_without_a_registered_file_records_nothing() {
        let barrier = WalkBarrier::default();
        assert!(barrier.pop_finished().await.is_none());
        barrier.publish_drain(100, 1, SpoolMark::default()).await;
        assert!(barrier.collect().await.is_none());
    }

    #[tokio::test]
    async fn a_part_settles_once_every_file_it_held_is_recorded() {
        let barrier = WalkBarrier::default();
        barrier
            .part_drained(
                "part_001.tar.zst".into(),
                vec!["base/5/16400".into(), "base/5/16401".into()],
                false,
            )
            .await;
        barrier
            .part_drained("part_002.tar.zst".into(), vec!["base/5/16402".into()], true)
            .await;

        let mut recorded: HashSet<String> = HashSet::default();
        recorded.insert("base/5/16400".into());
        assert!(barrier.settled_parts(&recorded).await.is_empty());
        recorded.insert("base/5/16401".into());
        recorded.insert("base/5/16402".into());
        assert_eq!(
            barrier.settled_parts(&recorded).await,
            vec!["part_001.tar.zst".to_string()],
            "an SLRU-bearing part is always re-read",
        );
        assert!(
            barrier.settled_parts(&recorded).await.is_empty(),
            "a settled part is not offered twice"
        );
    }
}
