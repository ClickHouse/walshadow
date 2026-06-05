//! Cumulative-ack durability watermark for the parallel pipeline.
//!
//! Replaces the serial emitter's synchronous `on_xact_end -> durable_lsn`
//! with a refcount-driven contiguous watermark. Everything downstream
//! completes out of order; `emitter_ack_lsn` (which the daemon advertises
//! as the standby `apply_lsn`, bounding source slot recycling) must not.
//!
//! Reorder assigns each committed/aborted xact a dense `seq` and a
//! `commit_lsn` (monotonic in `seq`). For each `seq` the collector tracks
//! how many rows were *placed* (routed by a decoder) and how many have
//! *acked* (an inserter drained the batch to `EndOfStream`). A `seq` is
//! **done** once `placed == Some(R)` and `acked == R` (rows=0 xacts —
//! aborts / empty / filtered — are done as soon as placed). The watermark
//! is the highest contiguous done `seq`; its `commit_lsn` is published into
//! the `emitter_ack` atomic. Contiguity is the safety property: commit_lsn
//! is monotonic in seq, so advertising the contiguous-done commit_lsn never
//! claims durability for a later seq whose rows are still in flight.
//!
//! Invariant: an inserter sends [`AckEvent::Acked`] only *after*
//! drain-to-`EndOfStream`. A row counted in `placed` but never acked pins
//! the watermark forever — the daemon's stall watchdog escalates that.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;

/// One event into the collector actor.
pub enum AckEvent {
    /// Reorder registered `seq` at `commit_lsn` (in seq order, no gaps).
    Register { seq: u64, commit_lsn: u64 },
    /// A decoder finished routing `seq`'s rows; `rows` will be acked.
    Placed { seq: u64, rows: u64 },
    /// An inserter drained a batch carrying these `(seq, rows)` counts.
    Acked { counts: Vec<(u64, u64)> },
    /// Trailing non-commit WAL: advance to `lsn` iff every registered seq
    /// is already done (nothing buffered anywhere). Reorder only sends this
    /// when the xact buffer is empty.
    Trailing { lsn: u64 },
}

struct SeqState {
    commit_lsn: u64,
    placed: Option<u64>,
    acked: u64,
}

impl SeqState {
    fn done(&self) -> bool {
        self.placed.is_some_and(|r| r == self.acked)
    }
}

/// Sync core of the collector — actor is a thin loop over [`Self::apply`].
/// Split out so it can be unit-tested without spawning a task.
pub struct AckState {
    map: BTreeMap<u64, SeqState>,
    /// Lowest seq not yet done == count of contiguous done seqs (dense from 0).
    next_expected: u64,
    /// Lowest seq not yet placed (its decoder hasn't finished routing rows).
    /// The DDL barrier waits on this so `FlushAll` can't run ahead of rows
    /// still in flight from the decode pool.
    placed_frontier: u64,
    /// Number of seqs registered so far (dense from 0).
    registered: u64,
    emitter_ack: Arc<AtomicU64>,
    frontier_tx: watch::Sender<u64>,
    placed_tx: watch::Sender<u64>,
}

impl AckState {
    fn new(
        emitter_ack: Arc<AtomicU64>,
        frontier_tx: watch::Sender<u64>,
        placed_tx: watch::Sender<u64>,
    ) -> Self {
        Self {
            map: BTreeMap::new(),
            next_expected: 0,
            placed_frontier: 0,
            registered: 0,
            emitter_ack,
            frontier_tx,
            placed_tx,
        }
    }

    pub fn apply(&mut self, ev: AckEvent) {
        match ev {
            AckEvent::Register { seq, commit_lsn } => self.register(seq, commit_lsn),
            AckEvent::Placed { seq, rows } => self.placed(seq, rows),
            AckEvent::Acked { counts } => {
                for (seq, n) in counts {
                    self.acked(seq, n);
                }
            }
            AckEvent::Trailing { lsn } => self.trailing(lsn),
        }
    }

    fn register(&mut self, seq: u64, commit_lsn: u64) {
        self.map.insert(
            seq,
            SeqState {
                commit_lsn,
                placed: None,
                acked: 0,
            },
        );
        // Seqs are dense from 0, so the count is highest+1.
        self.registered = self.registered.max(seq + 1);
        self.advance();
    }

    fn placed(&mut self, seq: u64, rows: u64) {
        if let Some(s) = self.map.get_mut(&seq) {
            s.placed = Some(rows);
        }
        self.advance();
    }

    fn acked(&mut self, seq: u64, n: u64) {
        if let Some(s) = self.map.get_mut(&seq) {
            s.acked += n;
        }
        self.advance();
    }

    fn trailing(&mut self, lsn: u64) {
        if self.all_done() {
            self.emitter_ack.fetch_max(lsn, Ordering::Release);
        }
    }

    /// Drain the contiguous done prefix, advancing `emitter_ack` to the
    /// last done seq's `commit_lsn` and publishing the new frontier.
    fn advance(&mut self) {
        let mut moved = false;
        while let Some(s) = self.map.get(&self.next_expected) {
            if !s.done() {
                break;
            }
            self.emitter_ack.fetch_max(s.commit_lsn, Ordering::Release);
            self.map.remove(&self.next_expected);
            self.next_expected += 1;
            moved = true;
        }
        if moved {
            self.frontier_tx.send_replace(self.next_expected);
        }
        // Advance the placed frontier from where it last stopped, not from
        // `next_expected`: `[next_expected, placed_frontier)` are already
        // known placed (done implies placed, so a `next_expected` bump never
        // strands a placed gap). Restarting at `next_expected` re-walked that
        // whole run via `BTreeMap::get` on every event — O(N) per event,
        // O(N^2) overall — which pegged the collector at 100% CPU and stalled
        // `emitter_ack` whenever the decode pool placed far ahead of durable.
        // Resuming at `placed_frontier` visits each seq once: O(1) amortized.
        let mut pf = self.placed_frontier.max(self.next_expected);
        while self.map.get(&pf).is_some_and(|s| s.placed.is_some()) {
            pf += 1;
        }
        if pf != self.placed_frontier {
            self.placed_frontier = pf;
            self.placed_tx.send_replace(pf);
        }
    }

    /// True once every registered seq is done (nothing in flight).
    pub fn all_done(&self) -> bool {
        self.next_expected == self.registered
    }

    /// Oldest seq still incomplete (the watermark is pinned behind it),
    /// for the daemon's stall watchdog. `None` when fully caught up.
    pub fn oldest_incomplete(&self) -> Option<(u64, u64)> {
        if self.all_done() {
            None
        } else {
            self.map
                .get(&self.next_expected)
                .map(|s| (self.next_expected, s.commit_lsn))
        }
    }
}

/// Producer-side handle. Cloneable; the actor exits when the last handle
/// drops. Reorder, decoders, and inserters all hold clones.
#[derive(Clone)]
pub struct AckHandle {
    tx: mpsc::UnboundedSender<AckEvent>,
    emitter_ack: Arc<AtomicU64>,
    frontier: watch::Receiver<u64>,
    placed: watch::Receiver<u64>,
}

impl AckHandle {
    pub fn register(&self, seq: u64, commit_lsn: u64) {
        let _ = self.tx.send(AckEvent::Register { seq, commit_lsn });
    }

    pub fn placed(&self, seq: u64, rows: u64) {
        let _ = self.tx.send(AckEvent::Placed { seq, rows });
    }

    pub fn acked(&self, counts: Vec<(u64, u64)>) {
        if !counts.is_empty() {
            let _ = self.tx.send(AckEvent::Acked { counts });
        }
    }

    pub fn trailing(&self, lsn: u64) {
        let _ = self.tx.send(AckEvent::Trailing { lsn });
    }

    pub fn emitter_ack(&self) -> u64 {
        self.emitter_ack.load(Ordering::Acquire)
    }

    /// Block until the contiguous-done frontier covers all seqs `< seq`
    /// (i.e. every earlier xact is durable on ClickHouse). Used by the DDL
    /// barrier. Returns early if the collector actor has shut down.
    pub async fn wait_through(&self, seq: u64) {
        let mut r = self.frontier.clone();
        while *r.borrow_and_update() < seq {
            if r.changed().await.is_err() {
                break;
            }
        }
    }

    /// Block until every seq `< seq` has been *placed* (its decoder finished
    /// routing all its rows to the batcher). The barrier waits on this
    /// before issuing `FlushAll` so the flush can't seal ahead of rows still
    /// in flight from the decode pool.
    pub async fn wait_placed_through(&self, seq: u64) {
        let mut r = self.placed.clone();
        while *r.borrow_and_update() < seq {
            if r.changed().await.is_err() {
                break;
            }
        }
    }
}

/// Spawn the collector actor. The returned [`AckHandle`] clones feed it;
/// when all clones drop the actor drains and exits, completing the
/// [`JoinHandle`].
pub fn spawn(emitter_ack: Arc<AtomicU64>) -> (AckHandle, JoinHandle<()>) {
    let (tx, mut rx) = mpsc::unbounded_channel::<AckEvent>();
    let (frontier_tx, frontier) = watch::channel(0u64);
    let (placed_tx, placed) = watch::channel(0u64);
    let mut state = AckState::new(emitter_ack.clone(), frontier_tx, placed_tx);
    let handle = tokio::spawn(async move {
        while let Some(ev) = rx.recv().await {
            state.apply(ev);
        }
    });
    (
        AckHandle {
            tx,
            emitter_ack,
            frontier,
            placed,
        },
        handle,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn state() -> (AckState, Arc<AtomicU64>) {
        let ack = Arc::new(AtomicU64::new(0));
        let (ftx, _frx) = watch::channel(0u64);
        let (ptx, _prx) = watch::channel(0u64);
        (AckState::new(ack.clone(), ftx, ptx), ack)
    }

    #[test]
    fn in_order_advances_to_each_commit() {
        let (mut s, ack) = state();
        for seq in 0..3u64 {
            s.register(seq, (seq + 1) * 100);
            s.placed(seq, 2);
            s.acked(seq, 2);
        }
        assert_eq!(ack.load(Ordering::Acquire), 300);
        assert!(s.all_done());
    }

    #[test]
    fn out_of_order_done_holds_watermark_until_gap_fills() {
        let (mut s, ack) = state();
        s.register(0, 100);
        s.register(1, 200);
        s.register(2, 300);
        // seq 2 finishes first — watermark must stay at 0 (nothing done yet).
        s.placed(2, 1);
        s.acked(2, 1);
        assert_eq!(ack.load(Ordering::Acquire), 0);
        // seq 0 done — advance to 100, but 1 still blocks 2.
        s.placed(0, 1);
        s.acked(0, 1);
        assert_eq!(ack.load(Ordering::Acquire), 100);
        assert_eq!(s.oldest_incomplete(), Some((1, 200)));
        // seq 1 done — frontier jumps over the already-done 2 to 300.
        s.placed(1, 1);
        s.acked(1, 1);
        assert_eq!(ack.load(Ordering::Acquire), 300);
        assert!(s.all_done());
    }

    #[test]
    fn empty_or_abort_seq_is_done_when_placed_zero() {
        let (mut s, ack) = state();
        s.register(0, 100);
        s.placed(0, 0); // abort / empty: no rows
        assert_eq!(ack.load(Ordering::Acquire), 100);
        assert!(s.all_done());
    }

    #[test]
    fn rows_split_across_batches_complete_at_total() {
        let (mut s, ack) = state();
        s.register(0, 100);
        s.placed(0, 5);
        s.acked(0, 2);
        assert_eq!(ack.load(Ordering::Acquire), 0); // 2/5 acked
        s.acked(0, 3);
        assert_eq!(ack.load(Ordering::Acquire), 100); // 5/5
    }

    #[test]
    fn acked_before_placed_still_completes() {
        let (mut s, ack) = state();
        s.register(0, 100);
        s.acked(0, 3);
        assert_eq!(ack.load(Ordering::Acquire), 0); // placed unknown
        s.placed(0, 3);
        assert_eq!(ack.load(Ordering::Acquire), 100);
    }

    #[test]
    fn placed_frontier_tracks_contiguous_placed_seqs() {
        let ack = Arc::new(AtomicU64::new(0));
        let (ftx, _frx) = watch::channel(0u64);
        let (ptx, prx) = watch::channel(0u64);
        let mut s = AckState::new(ack, ftx, ptx);
        s.register(0, 100);
        s.register(1, 200);
        s.register(2, 300);
        // Place 0 and 2 (out of order); frontier stops at the gap (1).
        s.placed(2, 1);
        assert_eq!(*prx.borrow(), 0);
        s.placed(0, 1);
        assert_eq!(*prx.borrow(), 1, "placed through seq 0");
        // Filling the gap advances the frontier past the already-placed 2.
        s.placed(1, 1);
        assert_eq!(*prx.borrow(), 3, "placed through all three");
    }

    /// Decode pool places far ahead of the durable watermark (rows in flight
    /// to CH). `advance` must resume the placed-frontier scan at
    /// `placed_frontier`, not re-walk `[next_expected, placed_frontier)` every
    /// event — else this is O(N^2) and pegs the collector. With many seqs the
    /// O(N^2) form is visibly slow; the resume form stays linear.
    #[test]
    fn placed_far_ahead_of_acked_stays_linear() {
        let (mut s, ack) = state();
        let n = 50_000u64;
        for seq in 0..n {
            s.register(seq, (seq + 1) * 10);
            s.placed(seq, 1); // placed immediately, but nothing acked yet
        }
        // Everything placed, nothing durable: watermark pinned at 0.
        assert_eq!(ack.load(Ordering::Acquire), 0);
        assert!(!s.all_done());
        // Drain durability in order; watermark walks to the last commit_lsn.
        for seq in 0..n {
            s.acked(seq, 1);
        }
        assert!(s.all_done());
        assert_eq!(ack.load(Ordering::Acquire), n * 10);
    }

    #[test]
    fn trailing_advances_only_when_all_done() {
        let (mut s, ack) = state();
        s.register(0, 100);
        s.placed(0, 1);
        // Not acked yet → trailing must not advance past the buffered row.
        s.trailing(9_999);
        assert_eq!(ack.load(Ordering::Acquire), 0);
        s.acked(0, 1);
        assert_eq!(ack.load(Ordering::Acquire), 100);
        // Now fully drained → trailing advances to the dispatched marker.
        s.trailing(9_999);
        assert_eq!(ack.load(Ordering::Acquire), 9_999);
    }
}
