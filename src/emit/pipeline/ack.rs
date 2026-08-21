//! Cumulative-ack durability watermark for the parallel pipeline
//!
//! Downstream work completes out of order, but `emitter_ack_lsn` cannot
//! advance past gaps because it bounds source slot recycling. Each
//! transaction gets a dense `seq` plus a monotonic `commit_lsn`; the
//! watermark advances through contiguous done seqs only
//!
//! One commit may span several seqs. Only the final seq publishes its
//! `commit_lsn`, earlier slices gate contiguity via
//! [`AckHandle::register_partial`]
//!
//! Inserter sends [`AckEvent::Acked`] only after draining `EndOfStream`

use std::collections::BTreeMap;
use std::sync::Arc;

use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;

use crate::pos::{AckFrontier, EmitterAck, Gate, GateClosed, Monotone, PlacedFrontier, Pos};

pub enum AckEvent {
    /// In seq order, no gaps. `publish: false` marks a non-final slice of a
    /// multi-seq commit: done-ness gates contiguity, `commit_lsn` never
    /// publishes.
    Register {
        seq: u64,
        commit_lsn: u64,
        publish: bool,
    },
    Placed {
        seq: u64,
        rows: u64,
    },
    Acked {
        counts: Vec<(u64, u64)>,
    },
    /// Pump position beyond buffered transactions, held until dispatched seqs finish
    Trailing {
        lsn: u64,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SeqFault {
    DoublePlaced,
    OverAcked,
}

/// Per-seq progress; placement and acks arrive in either order
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct Progress {
    /// `None` while waiting on decode, `Some` while waiting on ClickHouse
    rows: Option<u64>,
    acked: u64,
}

/// A fault leaves the counts untouched, so the wedged seq stays diagnosable
impl Progress {
    fn place(&mut self, rows: u64) -> Result<(), SeqFault> {
        if self.rows.is_some() {
            return Err(SeqFault::DoublePlaced);
        }
        if self.acked > rows {
            return Err(SeqFault::OverAcked);
        }
        self.rows = Some(rows);
        Ok(())
    }

    fn ack(&mut self, n: u64) -> Result<(), SeqFault> {
        let acked = self.acked + n;
        if self.rows.is_some_and(|rows| acked > rows) {
            return Err(SeqFault::OverAcked);
        }
        self.acked = acked;
        Ok(())
    }

    fn is_done(self) -> bool {
        self.rows == Some(self.acked)
    }

    /// Row count reported, safe for the `FlushAll` placement barrier
    fn is_placed(self) -> bool {
        self.rows.is_some()
    }
}

struct Seq {
    commit_lsn: u64,
    publish: bool,
    progress: Progress,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IncompleteSeq {
    pub seq: u64,
    pub commit_lsn: Pos<EmitterAck>,
    /// `None` while waiting on decode, `Some` while waiting on ClickHouse
    pub rows: Option<u64>,
    pub acked: u64,
}

/// Watermark state exposed to stall diagnostics
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct AckSnapshot {
    pub registered: u64,
    pub frontier: u64,
    pub placed_frontier: u64,
    pub oldest_incomplete: Option<IncompleteSeq>,
    pub trailing_held: Pos<EmitterAck>,
    /// Protocol faults, each of which pins a seq forever
    pub wedged: u64,
    /// Events for retired seqs, benign: inserter batches straddle retirement
    pub late: u64,
}

impl AckSnapshot {
    pub fn all_done(&self) -> bool {
        self.oldest_incomplete.is_none()
    }

    pub fn stall_reason(&self) -> Option<&'static str> {
        let inc = self.oldest_incomplete?;
        Some(match inc.rows {
            None => "decode has not reported rows",
            Some(_) => "clickhouse has not acked rows",
        })
    }
}

pub struct AckState {
    map: BTreeMap<u64, Seq>,
    /// Lowest seq not yet done == count of contiguous done seqs (dense from 0)
    frontier: u64,
    /// Lowest seq not yet placed. DDL barrier waits on this so `FlushAll`
    /// can't run ahead of rows still in flight from the decode pool.
    placed_frontier: u64,
    registered: u64,
    /// Highest publishing `commit_lsn` retired from map
    watermark: u64,
    /// Retained across events because quiescent source cannot resend it
    trailing: u64,
    emitter_ack: Arc<Monotone<EmitterAck>>,
    frontier_cell: Monotone<AckFrontier>,
    placed_cell: Monotone<PlacedFrontier>,
    wedged: u64,
    late: u64,
    probe_tx: watch::Sender<AckSnapshot>,
}

impl AckState {
    fn new(
        emitter_ack: Arc<Monotone<EmitterAck>>,
        frontier_cell: Monotone<AckFrontier>,
        placed_cell: Monotone<PlacedFrontier>,
        probe_tx: watch::Sender<AckSnapshot>,
    ) -> Self {
        Self {
            map: BTreeMap::new(),
            frontier: 0,
            placed_frontier: 0,
            registered: 0,
            watermark: 0,
            trailing: 0,
            emitter_ack,
            frontier_cell,
            placed_cell,
            wedged: 0,
            late: 0,
            probe_tx,
        }
    }

    pub fn apply(&mut self, ev: AckEvent) {
        match ev {
            AckEvent::Register {
                seq,
                commit_lsn,
                publish,
            } => self.open_seq(seq, commit_lsn, publish),
            AckEvent::Placed { seq, rows } => self.step(seq, |p| p.place(rows)),
            AckEvent::Acked { counts } => {
                for (seq, n) in counts {
                    self.step(seq, |p| p.ack(n));
                }
            }
            AckEvent::Trailing { lsn } => self.trailing = self.trailing.max(lsn),
        }
        self.publish();
    }

    fn open_seq(&mut self, seq: u64, commit_lsn: u64, publish: bool) {
        // Preserve live counts, never resurrect retired seqs
        if seq < self.frontier || self.map.contains_key(&seq) {
            self.wedge(seq, "seq registered twice");
            return;
        }
        self.map.insert(
            seq,
            Seq {
                commit_lsn,
                publish,
                progress: Progress::default(),
            },
        );
        self.registered = self.registered.max(seq + 1);
    }

    fn step(&mut self, seq: u64, f: impl FnOnce(&mut Progress) -> Result<(), SeqFault>) {
        let Some(s) = self.map.get_mut(&seq) else {
            self.absent(seq);
            return;
        };
        match f(&mut s.progress) {
            Ok(()) => {}
            Err(SeqFault::DoublePlaced) => self.wedge(seq, "seq placed twice"),
            Err(SeqFault::OverAcked) => self.wedge(seq, "seq acked past its row count"),
        }
    }

    /// Distinguish benign retired events from unregistered seqs that pin frontier
    fn absent(&mut self, seq: u64) {
        if seq < self.frontier {
            self.late += 1;
            return;
        }
        self.wedge(seq, "ack event for an unregistered seq");
    }

    /// Log the first fault only; the rest are noise once the watermark pins
    fn wedge(&mut self, seq: u64, what: &'static str) {
        self.wedged += 1;
        if self.wedged == 1 {
            tracing::error!(
                target: "walshadow::pipeline",
                seq,
                frontier = self.frontier,
                registered = self.registered,
                "{what} — watermark will pin here",
            );
        }
    }

    /// Sole writer of every published output, called after every event
    ///
    /// Both scans resume at their own frontier, never at the done frontier:
    /// placement runs far ahead of acks, so re-walking that span per event is
    /// O(N²)
    fn publish(&mut self) {
        while let Some(s) = self.map.get(&self.frontier) {
            if !s.progress.is_done() {
                break;
            }
            let (publish, commit_lsn) = (s.publish, s.commit_lsn);
            self.map.remove(&self.frontier);
            if publish {
                self.watermark = self.watermark.max(commit_lsn);
            }
            self.frontier += 1;
        }
        let mut pf = self.placed_frontier.max(self.frontier);
        while self.map.get(&pf).is_some_and(|s| s.progress.is_placed()) {
            pf += 1;
        }
        self.placed_frontier = pf;

        // Later registrations carry later commit LSNs, so publish trailing only at idle
        let ack = if self.all_done() {
            self.watermark.max(self.trailing)
        } else {
            self.watermark
        };
        self.emitter_ack.join(ack);
        self.frontier_cell.join(self.frontier);
        self.placed_cell.join(self.placed_frontier);
        self.probe_tx.send_replace(self.snapshot());
    }

    pub fn snapshot(&self) -> AckSnapshot {
        AckSnapshot {
            registered: self.registered,
            frontier: self.frontier,
            placed_frontier: self.placed_frontier,
            oldest_incomplete: self.oldest_incomplete(),
            trailing_held: self.trailing.into(),
            wedged: self.wedged,
            late: self.late,
        }
    }

    pub fn all_done(&self) -> bool {
        self.frontier == self.registered
    }

    pub fn oldest_incomplete(&self) -> Option<IncompleteSeq> {
        if self.all_done() {
            return None;
        }
        let s = self.map.get(&self.frontier)?;
        Some(IncompleteSeq {
            seq: self.frontier,
            commit_lsn: s.commit_lsn.into(),
            rows: s.progress.rows,
            acked: s.progress.acked,
        })
    }
}

/// Producer-side handle. Actor exits when the last clone drops; reorder,
/// decoders, inserters all hold clones.
#[derive(Clone)]
pub struct AckHandle {
    tx: mpsc::UnboundedSender<AckEvent>,
    emitter_ack: Arc<Monotone<EmitterAck>>,
    frontier: Gate<AckFrontier>,
    placed: Gate<PlacedFrontier>,
    probe: watch::Receiver<AckSnapshot>,
}

impl AckHandle {
    /// Register a commit's final (or only) seq; its `commit_lsn` publishes
    /// once the contiguous-done frontier passes it.
    pub fn register(&self, seq: u64, commit_lsn: u64) {
        let _ = self.tx.send(AckEvent::Register {
            seq,
            commit_lsn,
            publish: true,
        });
    }

    /// Register a non-final slice of a multi-seq commit: counts toward
    /// contiguity but never advances `emitter_ack` (see module doc).
    pub fn register_partial(&self, seq: u64, commit_lsn: u64) {
        let _ = self.tx.send(AckEvent::Register {
            seq,
            commit_lsn,
            publish: false,
        });
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

    pub fn emitter_ack(&self) -> Pos<EmitterAck> {
        self.emitter_ack.get()
    }

    pub fn probe(&self) -> watch::Receiver<AckSnapshot> {
        self.probe.clone()
    }

    /// Wait until every seq below `seq` is durable on ClickHouse
    pub async fn wait_through(&self, seq: u64) -> Result<(), GateClosed> {
        self.frontier.wait(seq).await.map(|_| ())
    }

    /// Wait until every seq below `seq` reports its row count
    pub async fn wait_placed_through(&self, seq: u64) -> Result<(), GateClosed> {
        self.placed.wait(seq).await.map(|_| ())
    }
}

/// Spawn the collector actor. When all [`AckHandle`] clones drop it drains
/// and exits, completing the [`JoinHandle`].
pub fn spawn(emitter_ack: Arc<Monotone<EmitterAck>>) -> (AckHandle, JoinHandle<()>) {
    let (tx, mut rx) = mpsc::unbounded_channel::<AckEvent>();
    let (frontier_cell, placed_cell) = (Monotone::default(), Monotone::default());
    let (frontier, placed) = (frontier_cell.watch(), placed_cell.watch());
    let (probe_tx, probe) = watch::channel(AckSnapshot::default());
    let mut state = AckState::new(emitter_ack.clone(), frontier_cell, placed_cell, probe_tx);
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
            probe,
        },
        handle,
    )
}

#[cfg(test)]
impl AckState {
    fn register(&mut self, seq: u64, commit_lsn: u64, publish: bool) {
        self.apply(AckEvent::Register {
            seq,
            commit_lsn,
            publish,
        });
    }

    fn placed(&mut self, seq: u64, rows: u64) {
        self.apply(AckEvent::Placed { seq, rows });
    }

    fn acked(&mut self, seq: u64, n: u64) {
        self.apply(AckEvent::Acked {
            counts: vec![(seq, n)],
        });
    }

    fn trailing(&mut self, lsn: u64) {
        self.apply(AckEvent::Trailing { lsn });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    pub(super) fn state() -> (AckState, Arc<Monotone<EmitterAck>>) {
        let (s, ack, _) = state_watching_placed();
        (s, ack)
    }

    fn state_watching_placed() -> (AckState, Arc<Monotone<EmitterAck>>, Gate<PlacedFrontier>) {
        let ack = Arc::new(Monotone::default());
        let (frontier, placed) = (Monotone::default(), Monotone::default());
        let prx = placed.watch();
        let (probe_tx, _probe) = watch::channel(AckSnapshot::default());
        (
            AckState::new(ack.clone(), frontier, placed, probe_tx),
            ack,
            prx,
        )
    }

    #[test]
    fn placed_frontier_tracks_contiguous_placed_seqs() {
        let (mut s, _ack, prx) = state_watching_placed();
        s.register(0, 100, true);
        s.register(1, 200, true);
        s.register(2, 300, true);
        s.placed(2, 1);
        assert_eq!(prx.current(), 0);
        s.placed(0, 1);
        assert_eq!(prx.current(), 1, "placed through seq 0");
        s.placed(1, 1);
        assert_eq!(prx.current(), 3, "placed through all three");
    }

    /// Guard against O(N²) placement scan when placement runs ahead of acks
    #[test]
    fn placed_far_ahead_of_acked_stays_linear() {
        let (mut s, ack) = state();
        let n = 50_000u64;
        for seq in 0..n {
            s.register(seq, (seq + 1) * 10, true);
            s.placed(seq, 1);
        }
        assert_eq!(ack.get(), 0);
        assert!(!s.all_done());
        for seq in 0..n {
            s.acked(seq, 1);
        }
        assert!(s.all_done());
        assert_eq!(ack.get(), n * 10);
    }

    #[test]
    fn probe_names_what_holds_the_watermark() {
        let (mut s, _ack) = state();
        s.register(0, 100, true);
        let snap = s.snapshot();
        assert_eq!(snap.stall_reason(), Some("decode has not reported rows"));
        assert_eq!(
            snap.oldest_incomplete.map(|o| (o.seq, o.rows, o.acked)),
            Some((0, None, 0))
        );
        s.placed(0, 4);
        s.acked(0, 1);
        s.trailing(9_999);
        let snap = s.snapshot();
        assert_eq!(snap.stall_reason(), Some("clickhouse has not acked rows"));
        assert_eq!(
            snap.oldest_incomplete.map(|o| (o.rows, o.acked)),
            Some((Some(4), 1))
        );
        assert_eq!(snap.trailing_held, 9_999, "held, not yet published");
        s.acked(0, 3);
        let snap = s.snapshot();
        assert_eq!(snap.stall_reason(), None);
        assert!(snap.all_done());
        assert_eq!((snap.wedged, snap.late), (0, 0));
    }

    #[test]
    fn events_for_retired_seqs_are_benign() {
        let (mut s, _ack) = state();
        s.register(0, 100, true);
        s.placed(0, 1);
        s.acked(0, 1);
        s.acked(0, 1);
        let snap = s.snapshot();
        assert_eq!((snap.late, snap.wedged), (1, 0));
    }

    #[test]
    fn illegal_transitions_pin_the_seq_instead_of_being_absorbed() {
        let (mut s, ack) = state();
        s.register(0, 100, true);
        s.placed(0, 2);
        s.placed(0, 5);
        s.acked(0, 3);
        s.register(0, 100, true);
        assert_eq!(s.snapshot().wedged, 3);
        assert!(!s.all_done(), "a faulted seq stays incomplete");
        assert_eq!(ack.get(), 0);
        s.placed(7, 3);
        assert_eq!(s.snapshot().wedged, 4, "seq past the registration frontier");
    }
}

/// Compare incremental collector with full-history model across legal schedules
#[cfg(test)]
mod interleavings {
    use super::*;

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum Ev {
        Reg { seq: u64, lsn: u64, publish: bool },
        Place { seq: u64, rows: u64 },
        Ack { seq: u64, n: u64 },
        Trail { lsn: u64 },
    }

    impl Ev {
        fn seq(self) -> Option<u64> {
            match self {
                Self::Reg { seq, .. } | Self::Place { seq, .. } | Self::Ack { seq, .. } => {
                    Some(seq)
                }
                Self::Trail { .. } => None,
            }
        }

        fn is_reg(self) -> bool {
            matches!(self, Self::Reg { .. })
        }
    }

    fn reg(seq: u64, lsn: u64, publish: bool) -> Ev {
        Ev::Reg { seq, lsn, publish }
    }

    fn place(seq: u64, rows: u64) -> Ev {
        Ev::Place { seq, rows }
    }

    fn ack(seq: u64, n: u64) -> Ev {
        Ev::Ack { seq, n }
    }

    #[derive(Clone, Copy)]
    struct ModelSeq {
        lsn: u64,
        publish: bool,
        rows: Option<u64>,
        acked: u64,
    }

    #[derive(Default)]
    struct Model {
        seqs: Vec<ModelSeq>,
        trailing: u64,
        high: u64,
    }

    impl Model {
        fn apply(&mut self, ev: Ev) {
            match ev {
                Ev::Reg { seq, lsn, publish } => {
                    assert_eq!(seq as usize, self.seqs.len(), "dense registration");
                    self.seqs.push(ModelSeq {
                        lsn,
                        publish,
                        rows: None,
                        acked: 0,
                    });
                }
                Ev::Place { seq, rows } => self.seqs[seq as usize].rows = Some(rows),
                Ev::Ack { seq, n } => self.seqs[seq as usize].acked += n,
                Ev::Trail { lsn } => self.trailing = self.trailing.max(lsn),
            }
            self.high = self.high.max(self.expected());
        }

        fn expected(&self) -> u64 {
            let mut w = 0;
            let mut done = 0;
            for s in &self.seqs {
                if s.rows != Some(s.acked) {
                    break;
                }
                if s.publish {
                    w = w.max(s.lsn);
                }
                done += 1;
            }
            if done == self.seqs.len() {
                w.max(self.trailing)
            } else {
                w
            }
        }
    }

    fn interleave(events: &[Ev], visit: &mut impl FnMut(&[Ev])) -> u64 {
        assert!(events.len() <= 32, "mask is a u32");
        let mut order = Vec::with_capacity(events.len());
        let mut count = 0;
        walk(events, 0, &mut order, &mut count, visit);
        count
    }

    fn walk(
        events: &[Ev],
        used: u32,
        order: &mut Vec<Ev>,
        count: &mut u64,
        visit: &mut impl FnMut(&[Ev]),
    ) {
        if order.len() == events.len() {
            *count += 1;
            visit(order);
            return;
        }
        let registered = |seq: u64| {
            events
                .iter()
                .enumerate()
                .any(|(i, e)| e.is_reg() && e.seq() == Some(seq) && used & (1 << i) != 0)
        };
        for (i, ev) in events.iter().enumerate() {
            if used & (1 << i) != 0 {
                continue;
            }
            let ready = match ev.seq() {
                None => true,
                Some(seq) if ev.is_reg() => (0..seq).all(registered),
                Some(seq) => registered(seq),
            };
            if !ready {
                continue;
            }
            order.push(*ev);
            walk(events, used | (1 << i), order, count, visit);
            order.pop();
        }
    }

    fn to_event(ev: Ev) -> AckEvent {
        match ev {
            Ev::Reg { seq, lsn, publish } => AckEvent::Register {
                seq,
                commit_lsn: lsn,
                publish,
            },
            Ev::Place { seq, rows } => AckEvent::Placed { seq, rows },
            Ev::Ack { seq, n } => AckEvent::Acked {
                counts: vec![(seq, n)],
            },
            Ev::Trail { lsn } => AckEvent::Trailing { lsn },
        }
    }

    fn check(schedule: &[Ev]) {
        let (mut s, ack) = super::tests::state();
        let mut m = Model::default();
        let mut prev = Pos::ZERO;
        for (i, ev) in schedule.iter().enumerate() {
            s.apply(to_event(*ev));
            m.apply(*ev);
            let got = ack.get();
            assert!(
                got >= prev,
                "watermark went backwards at step {i} of {schedule:?}"
            );
            assert_eq!(
                got, m.high,
                "step {i} of {schedule:?}: collector {got}, model {}",
                m.high
            );
            prev = got;
        }
        assert_eq!(
            s.all_done(),
            m.seqs.iter().all(|q| q.rows == Some(q.acked)),
            "all_done disagrees for {schedule:?}"
        );
        assert_eq!(
            s.snapshot().wedged,
            0,
            "legal schedule produced a fault: {schedule:?}"
        );
    }

    #[test]
    fn two_commits_and_a_trailing_position() {
        let events = [
            reg(0, 100, true),
            place(0, 1),
            ack(0, 1),
            reg(1, 200, true),
            place(1, 1),
            ack(1, 1),
            Ev::Trail { lsn: 9_999 },
        ];
        let n = interleave(&events, &mut check);
        assert_eq!(n, 280, "every legal interleaving covered");
    }

    #[test]
    fn barrier_segments_then_a_rows_zero_marker() {
        let events = [
            reg(0, 500, false),
            place(0, 1),
            ack(0, 1),
            reg(1, 500, false),
            place(1, 1),
            ack(1, 1),
            reg(2, 500, true),
            place(2, 0),
        ];
        let n = interleave(&events, &mut check);
        assert_eq!(n, 504, "every legal interleaving covered");
    }

    #[test]
    fn split_acks_across_two_commits() {
        let events = [
            reg(0, 100, true),
            place(0, 2),
            ack(0, 1),
            ack(0, 1),
            reg(1, 200, true),
            place(1, 2),
            ack(1, 2),
        ];
        let n = interleave(&events, &mut check);
        assert_eq!(n, 240, "every legal interleaving covered");
    }

    #[test]
    fn seeded_sweep_over_wider_schedules() {
        let mut rng = 0x5EED_1234_ABCD_0001u64;
        let mut next = move || {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            rng
        };
        for round in 0..300u64 {
            let seqs = 3 + (round % 4) as usize;
            let mut pending: Vec<Vec<Ev>> = Vec::new();
            let mut events = Vec::new();
            for seq in 0..seqs as u64 {
                let rows = next() % 3;
                let publish = next() % 4 != 0;
                events.push(reg(seq, (seq + 1) * 100, publish));
                let mut per = vec![place(seq, rows)];
                per.extend((0..rows).map(|_| ack(seq, 1)));
                pending.push(per);
            }
            let mut schedule = Vec::new();
            let mut regs = events.into_iter();
            let mut open: Vec<usize> = Vec::new();
            loop {
                let more_regs = open.len() < seqs;
                let pick_reg = more_regs && (open.is_empty() || next() % 3 == 0);
                if pick_reg {
                    schedule.push(regs.next().expect("one reg per seq"));
                    open.push(open.len());
                    continue;
                }
                let live: Vec<usize> = open
                    .iter()
                    .copied()
                    .filter(|i| !pending[*i].is_empty())
                    .collect();
                if live.is_empty() {
                    if more_regs {
                        continue;
                    }
                    break;
                }
                let pick = live[(next() % live.len() as u64) as usize];
                schedule.push(pending[pick].remove(0));
            }
            if next() % 2 == 0 {
                let at = (next() % (schedule.len() as u64 + 1)) as usize;
                // Below and above every commit: trailing must not lower one
                let lsn = if next() % 2 == 0 { 50 } else { 1_000_000 };
                schedule.insert(at, Ev::Trail { lsn });
            }
            check(&schedule);
        }
    }
}
