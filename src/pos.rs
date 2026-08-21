//! Typed stream positions and the monotone cells that publish them
//!
//! Kinds name roles, not an order: shadow and decoder consume one stream
//! independently, so `drain` routinely runs ahead of `shadow_replay`

use std::cmp::Ordering as CmpOrdering;
use std::fmt;
use std::marker::PhantomData;

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use tokio::sync::watch;
use walrus::pg::backup::{format_pg_lsn, parse_pg_lsn};

/// Position role, named for diagnostics
pub trait PosKind: 'static {
    const NAME: &'static str;
}

/// WAL byte position. Seq-space counts stay off this trait, so rendering or
/// persisting a count as an LSN is a compile error
pub trait LsnKind: PosKind {}

macro_rules! kinds {
    ($($(#[$m:meta])* $name:ident => $label:literal;)+) => {
        $(
            $(#[$m])*
            #[derive(Debug)]
            pub enum $name {}
            impl PosKind for $name {
                const NAME: &'static str = $label;
            }
        )+
    };
}

macro_rules! lsn_kinds {
    ($($(#[$m:meta])* $name:ident => $label:literal;)+) => {
        kinds! { $($(#[$m])* $name => $label;)+ }
        $(impl LsnKind for $name {})+
    };
}

lsn_kinds! {
    /// Highest `server_wal_end` seen on replication socket
    SourceReceived => "source_received";
    /// Highest sealed segment boundary fsynced by segment sink
    FilterDurable => "filter_durable";
    /// Shadow PG `pg_last_wal_replay_lsn()`
    ShadowReplay => "shadow_replay";
    /// Minimum `flush_lsn` across active shadow streams
    ShadowFlush => "shadow_flush";
    /// Highest byte position dispatched onto one shadow stream
    ShadowDispatched => "shadow_dispatched";
    /// Highest commit LSN drained from transaction buffer
    Drain => "drain";
    /// Highest commit LSN durably acked by pipeline
    EmitterAck => "emitter_ack";
    /// Emitter ack floored at oldest undurable transaction
    ResumeSafe => "resume_safe";
    XactFirst => "xact_first";
    /// Last segment boundary handed downstream, durable after fsync
    FilterDispatched => "filter_dispatched";
    Commit => "commit";
    Snapshot => "snapshot";
    /// Segment-aligned, archive-clamped resume and GC floor
    Floor => "floor";
    /// Timeline start or ancestor switchpoint
    Switchpoint => "switchpoint";
}

kinds! {
    /// Contiguous done prefix in ack collector seq space
    AckFrontier => "ack_frontier";
    /// Prefix of seqs fully routed by decode pool
    PlacedFrontier => "placed_frontier";
    /// Enqueues onto shadow send queues
    QueuedWake => "queued_wake";
    /// Standby status reports plus connection attaches
    AppliedWake => "applied_wake";
}

pub struct Pos<K>(u64, PhantomData<fn() -> K>);

impl<K> Pos<K> {
    pub const ZERO: Self = Self(0, PhantomData);

    pub const fn new(raw: u64) -> Self {
        Self(raw, PhantomData)
    }

    pub const fn get(self) -> u64 {
        self.0
    }

    pub const fn is_zero(self) -> bool {
        self.0 == 0
    }

    /// Cross role boundary, callers must document why roles align
    pub const fn retag<J>(self) -> Pos<J> {
        Pos::new(self.0)
    }
}

impl<K> Clone for Pos<K> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<K> Copy for Pos<K> {}

impl<K> Default for Pos<K> {
    fn default() -> Self {
        Self::ZERO
    }
}

impl<K> From<u64> for Pos<K> {
    fn from(raw: u64) -> Self {
        Self::new(raw)
    }
}

impl<K> PartialEq<u64> for Pos<K> {
    fn eq(&self, other: &u64) -> bool {
        self.0 == *other
    }
}

impl<K> PartialOrd<u64> for Pos<K> {
    fn partial_cmp(&self, other: &u64) -> Option<CmpOrdering> {
        Some(self.0.cmp(other))
    }
}

impl<K> PartialEq for Pos<K> {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl<K> Eq for Pos<K> {}

impl<K> PartialOrd for Pos<K> {
    fn partial_cmp(&self, other: &Self) -> Option<CmpOrdering> {
        Some(self.cmp(other))
    }
}

impl<K> Ord for Pos<K> {
    fn cmp(&self, other: &Self) -> CmpOrdering {
        self.0.cmp(&other.0)
    }
}

impl<K: LsnKind> fmt::Display for Pos<K> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&format_pg_lsn(self.0), f)
    }
}

impl<K: PosKind> fmt::Debug for Pos<K> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}={}", K::NAME, self.0)
    }
}

impl<K: LsnKind> Serialize for Pos<K> {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.collect_str(&format_pg_lsn(self.0))
    }
}

impl<'de, K: LsnKind> Deserialize<'de> for Pos<K> {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let s = String::deserialize(d)?;
        parse_pg_lsn(&s)
            .map(Pos::new)
            .map_err(serde::de::Error::custom)
    }
}

/// Shared position that only moves forward, with level-triggered waiters
///
/// `join` is the routine mutator: a watermark that can be assigned is one that
/// can regress, and a regressed durability watermark recycles WAL the
/// destination never received. Dropping the cell closes every [`Gate`] on it
pub struct Monotone<K> {
    tx: watch::Sender<u64>,
    _kind: PhantomData<fn() -> K>,
}

impl<K> Monotone<K> {
    pub fn new(init: impl Into<Pos<K>>) -> Self {
        Self {
            tx: watch::Sender::new(init.into().get()),
            _kind: PhantomData,
        }
    }

    pub fn join(&self, v: impl Into<Pos<K>>) -> Pos<K> {
        let v = v.into().get();
        let mut prev = 0;
        self.tx.send_if_modified(|cur| {
            prev = std::mem::replace(cur, (*cur).max(v));
            prev < v
        });
        Pos::new(prev.max(v))
    }

    /// Re-anchor after position space changes, such as timeline fork
    pub fn rebase(&self, v: impl Into<Pos<K>>) -> Pos<K> {
        let v = v.into();
        self.tx.send_replace(v.get());
        v
    }

    /// Advance a cell that counts events rather than bytes
    pub fn bump(&self) {
        self.tx.send_modify(|cur| *cur += 1);
    }

    pub fn get(&self) -> Pos<K> {
        Pos::new(*self.tx.borrow())
    }

    pub fn watch(&self) -> Gate<K> {
        Gate {
            rx: self.tx.subscribe(),
            _kind: PhantomData,
        }
    }
}

impl<K> Default for Monotone<K> {
    fn default() -> Self {
        Self::new(0u64)
    }
}

impl<K: PosKind> fmt::Debug for Monotone<K> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.get().fmt(f)
    }
}

/// Level-triggered view of a [`Monotone`]
pub struct Gate<K> {
    rx: watch::Receiver<u64>,
    _kind: PhantomData<fn() -> K>,
}

impl<K> Clone for Gate<K> {
    fn clone(&self) -> Self {
        Self {
            rx: self.rx.clone(),
            _kind: PhantomData,
        }
    }
}

impl<K: PosKind> fmt::Debug for Gate<K> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.current().fmt(f)
    }
}

/// Cell dropped before the target was covered, so nothing can reach it
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GateClosed;

impl fmt::Display for GateClosed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("gate closed before target was covered")
    }
}

impl std::error::Error for GateClosed {}

impl<K> Gate<K> {
    pub fn current(&self) -> Pos<K> {
        Pos::new(*self.rx.borrow())
    }

    /// Resolve once the cell covers `target`
    ///
    /// Probe before parking, so a move that landed first is not waited out
    pub async fn wait(&self, target: impl Into<Pos<K>>) -> Result<Pos<K>, GateClosed> {
        let target = target.into().get();
        let mut rx = self.rx.clone();
        loop {
            let cur = *rx.borrow_and_update();
            if cur >= target {
                return Ok(Pos::new(cur));
            }
            rx.changed().await.map_err(|_| GateClosed)?;
        }
    }

    /// Resolve on the first move past `seen`, for consumers that redo their
    /// work from current state rather than aiming at a target
    pub async fn advance(&self, seen: Pos<K>) -> Result<Pos<K>, GateClosed> {
        self.wait(seen.get().saturating_add(1)).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_rebase_lowers_a_cell() {
        let m: Monotone<Floor> = Monotone::default();
        assert_eq!(m.join(500u64), 500);
        assert_eq!(m.join(100u64), 500);
        assert_eq!(m.rebase(100u64), 100, "fork re-anchors the floor");
        assert_eq!(m.get(), 100);
    }

    #[test]
    fn counts_never_render_as_pg_lsn() {
        assert_eq!(Pos::<Drain>::new(0x4100588).to_string(), "0/4100588");
        assert_eq!(
            format!(
                "{} {:?}",
                Pos::<EmitterAck>::new(16),
                Pos::<AckFrontier>::new(16)
            ),
            "0/10 ack_frontier=16"
        );
    }

    #[test]
    fn lsn_kinds_round_trip_through_pg_text() {
        let p = Pos::<Floor>::new(0x6A000000);
        let s = serde_json::to_string(&p).expect("serialize");
        assert_eq!(s, "\"0/6A000000\"");
        assert_eq!(serde_json::from_str::<Pos<Floor>>(&s).expect("parse"), p);
    }

    #[tokio::test]
    async fn gate_opens_on_a_target_already_covered() {
        let m = Monotone::<Drain>::default();
        m.join(900u64);
        assert_eq!(m.watch().wait(500u64).await.expect("covered"), 900);
    }

    /// Consumers that redo work from current state wake on any move, and on
    /// a move that landed before they asked
    #[tokio::test]
    async fn advance_resolves_on_a_move_already_made() {
        let m = Monotone::<QueuedWake>::default();
        let gate = m.watch();
        m.bump();
        assert_eq!(gate.advance(Pos::ZERO).await.expect("moved"), 1);
        let waiting = tokio::spawn({
            let gate = gate.clone();
            async move { gate.advance(Pos::new(1)).await }
        });
        tokio::task::yield_now().await;
        m.bump();
        assert_eq!(waiting.await.expect("join").expect("moved"), 2);
    }

    #[tokio::test]
    async fn dropping_the_cell_wakes_an_uncovered_waiter() {
        let m = Monotone::<Drain>::default();
        let gate = m.watch();
        m.join(10u64);
        drop(m);
        assert_eq!(gate.wait(10u64).await.expect("covered"), 10);
        assert_eq!(gate.wait(11u64).await, Err(GateClosed));
    }
}
