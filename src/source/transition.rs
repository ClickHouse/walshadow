//! Source timeline crossing.
//!
//! A promoted source ends the branch walshadow reads at a fork point `F` and
//! continues on a descendant. The walsender says so by closing COPY on a
//! historic timeline; from there one owner walks the stream across, in an order
//! chosen so a restart at any point sees one branch rather than two:
//!
//! ```text
//! ancestor ends at F ─► prove lineage ─► [barrier: pipeline drains to F] ─►
//!   publish history ─► request descendant at fork segment start ─►
//!   verify repeated prefix ─► commit the resume position ─►
//!   transition WalStream ─► advertise to shadow ─► resume
//! ```
//!
//! The commit is the hinge. Everything before it is reversible: the stream is
//! still the ancestor's at `F`, so a failure or a kill re-crosses from where the
//! branch ended. Everything after it is the descendant's, and the barrier is
//! what earns that — with nothing in flight below `F`, the resume floor moves to
//! the fork segment's start on the descendant, which PostgreSQL serves out of
//! its own verbatim copy of the ancestor prefix
//! (plans/failover.md §Crossing order).
//!
//! Advertising after the commit is load-bearing in both directions. Earlier and
//! the shadow is told about a fork whose floor still names the ancestor; later
//! and a client still reading the ancestor is handed a descendant record.
//!
//! Scope is operator-driven switchover: writes stop, every transaction
//! resolves, and no torn record survives, so the fence and the
//! overwrite-contrecord path are typed refusals rather than working paths
//! (plans/future/failover.md).

use std::io;
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context, Result};
use thiserror::Error;
use tokio::sync::Mutex;
use walrus::pg::backup::format_pg_lsn;

use crate::pos::{Drain, FilterDurable, Floor, Pos, ResumeSafe, ShadowReplay, Switchpoint};
use crate::record::{RecordSink, SegmentSink};
use crate::source::shadow_stream::ShadowStreamState;
use crate::source::source_feed::{SlotError, SourceEvent, SourceFeed, StandbyStatus};
use crate::source::timeline::{HistoryError, TimelineHistory, history_filename};
use crate::source::wal_stream::{ForkPrefix, WalStream, WalStreamError};

/// `reason=` label order of [`TimelineStats::failures_by_reason`].
pub const SWITCH_FAILURE_REASONS: [&str; 12] = [
    "timeline_not_descendant",
    "foreign_system_id",
    "sibling_branch",
    "history_missing",
    "history_malformed",
    "resume_past_fork",
    "published_past_fork",
    "open_xact_at_fork",
    "fork_prefix_mismatch",
    "slot_missing",
    "slot_too_new",
    "source",
];

#[derive(Debug, Error)]
pub enum TransitionError {
    #[error("live timeline {live} does not descend from {finished}")]
    NotDescendant { finished: u32, live: u32 },
    #[error("source is system {live}, artifacts belong to {stored}")]
    ForeignSystem { stored: u64, live: u64 },
    #[error(
        "source places timeline {tli} at {live_begin:#X}, walshadow crossed onto it at \
         {stored_begin:#X}: same number, different branch"
    )]
    SiblingBranch {
        tli: u32,
        stored_begin: u64,
        live_begin: u64,
    },
    #[error("source has no history file for timeline {tli}")]
    HistoryMissing { tli: u32 },
    #[error("history for timeline {tli}: {source}")]
    HistoryMalformed {
        tli: u32,
        #[source]
        source: HistoryError,
    },
    #[error("consumed frontier {next_lsn} sits past the fork {switch_lsn:#X}")]
    ResumePastFork {
        next_lsn: Pos<Floor>,
        switch_lsn: u64,
    },
    #[error("drained through {drain_lsn:#X}, past the fork {switch_lsn:#X}")]
    PublishedPastFork { drain_lsn: u64, switch_lsn: u64 },
    #[error(
        "{open_xacts} transaction(s) still open where timeline {finished} ends, \
         so the source did not shut down cleanly"
    )]
    OpenXactAtFork { finished: u32, open_xacts: usize },
    #[error("descendant's copy of [{from:#X}, {to:#X}) is not the WAL the ancestor served")]
    ForkPrefixMismatch { from: u64, to: u64 },
    #[error("descendant stream ended inside the repeated prefix at {lsn:#X}")]
    PrefixTruncated { lsn: u64 },
    #[error("fork prefix [{from:#X}, {to:#X}) is no longer retained")]
    PrefixNotRetained { from: u64, to: u64 },
    #[error(transparent)]
    Slot(#[from] SlotError),
    #[error("persist timeline history: {0}")]
    PersistHistory(#[source] io::Error),
    #[error("commit the fork resume position: {0:#}")]
    Commit(#[source] anyhow::Error),
    #[error(transparent)]
    Stream(#[from] WalStreamError),
    #[error("source: {0:#}")]
    Source(#[source] anyhow::Error),
}

impl TransitionError {
    /// Metric label; every variant maps into [`SWITCH_FAILURE_REASONS`].
    pub fn reason(&self) -> &'static str {
        match self {
            Self::NotDescendant { .. } => "timeline_not_descendant",
            Self::ForeignSystem { .. } => "foreign_system_id",
            Self::SiblingBranch { .. } => "sibling_branch",
            Self::HistoryMissing { .. } => "history_missing",
            Self::HistoryMalformed { .. } => "history_malformed",
            Self::ResumePastFork { .. } | Self::Stream(_) => "resume_past_fork",
            Self::PublishedPastFork { .. } => "published_past_fork",
            Self::OpenXactAtFork { .. } => "open_xact_at_fork",
            Self::ForkPrefixMismatch { .. }
            | Self::PrefixTruncated { .. }
            | Self::PrefixNotRetained { .. } => "fork_prefix_mismatch",
            Self::Slot(e) => e.reason(),
            Self::PersistHistory(_) | Self::Commit(_) | Self::Source(_) => "source",
        }
    }

    /// Lineage, prefix, and publication proofs need an operator; only source
    /// and storage trouble is worth another attempt. Every retryable failure
    /// lands before the commit, so a retry starts from the ancestor at `F`
    /// again rather than from a half-crossed stream.
    pub fn retryable(&self) -> bool {
        matches!(
            self,
            Self::Source(_)
                | Self::PersistHistory(_)
                | Self::Commit(_)
                | Self::Slot(SlotError::Query(_))
        )
    }
}

/// Cumulative crossing counters, read by the metrics snapshot.
#[derive(Debug, Default, Clone, Copy)]
pub struct TimelineStats {
    pub switches: u64,
    pub failures_by_reason: [u64; SWITCH_FAILURE_REASONS.len()],
    /// Most recent fork point, diagnostic
    pub switch_lsn: u64,
    pub prefix_bytes_verified: u64,
    pub seconds_total: f64,
}

impl TimelineStats {
    fn record_failure(&mut self, err: &TransitionError) {
        self.record_reason(err.reason());
    }

    /// Count a refusal raised outside the crossing itself — a repoint proving
    /// the target before the fork exists reads the same failures.
    pub fn record_reason(&mut self, reason: &str) {
        if let Some(at) = SWITCH_FAILURE_REASONS.iter().position(|r| *r == reason) {
            self.failures_by_reason[at] += 1;
        }
    }
}

/// Proof obligations the pump holds and the crossing must check before
/// adopting anything from the descendant.
#[derive(Debug, Clone, Copy)]
pub struct ForkGuards {
    /// Highest commit LSN out of the xact buffer — above every commit whose
    /// rows could have reached ClickHouse. `emitter_ack` cannot carry this
    /// proof: the pipeline completes out of order, so a stalled early sequence
    /// pins it below rows already inserted from a later one.
    pub drain_lsn: Pos<Drain>,
    /// Ordinary transactions still unresolved once the ancestor ends. A clean
    /// switchover leaves none: fast shutdown rolls live sessions back and each
    /// xid-assigned transaction writes its abort before the shutdown
    /// checkpoint.
    pub open_xacts: usize,
}

/// Pipeline frontiers the barrier reads. Every consumer has to be past the fork
/// segment's start before the descendant is requested — that boundary is what
/// the crossing commits, and a restart from it must lose nothing.
///
/// The window `[fork segment start, F)` is deliberately not included: a restart
/// re-reads it from the descendant's own copy of the ancestor prefix, so a
/// transaction in flight there is re-read rather than lost.
#[derive(Debug, Clone, Copy)]
pub struct ForkBarrier {
    /// Highest commit acknowledged by the pipeline, floored at the oldest
    /// unresolved transaction ([`XactBuffer::resume_safe_lsn`]).
    ///
    /// [`XactBuffer::resume_safe_lsn`]:
    ///     crate::xact::xact_buffer::XactBuffer::resume_safe_lsn
    pub resume_safe_lsn: Pos<ResumeSafe>,
    /// Shadow's aggregate apply LSN; `None` with no walreceiver attached.
    pub shadow_apply_lsn: Option<Pos<ShadowReplay>>,
    /// Highest fsynced sealed-segment boundary.
    pub filter_durable: Pos<FilterDurable>,
    /// Floor already persisted, which may already cover the fork segment.
    pub floor: Pos<Floor>,
}

/// What the barrier is still waiting on. Reported rather than bounded: the
/// source has stopped producing, so waiting costs nothing that is moving, and
/// crossing anyway would resurrect the mixed state the barrier exists to delete.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ForkWait {
    /// A transaction below the fork segment's start is not durable in
    /// ClickHouse, so resuming from that boundary would skip it.
    EmitterAck {
        acked: Pos<ResumeSafe>,
        fork_segment: Pos<Floor>,
    },
    /// Shadow has not replayed the ancestor's last record. It gets there
    /// without being told about the fork — replay only needs bytes it has.
    ShadowApply {
        applied: Option<Pos<ShadowReplay>>,
        fork: Pos<Switchpoint>,
    },
    /// Segments below the fork are not fsynced, so the fork segment's start is
    /// not yet a durable position. A clean end seals them on its own; an
    /// unclean one needs the truncation from
    /// plans/future/failover.md §Unplanned promotion first.
    ArchiveSeal {
        durable: Pos<FilterDurable>,
        fork_segment: Pos<Floor>,
    },
}

impl std::fmt::Display for ForkWait {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::EmitterAck {
                acked,
                fork_segment,
            } => write!(
                f,
                "emitter acked {:#X}, need {:#X}",
                acked.get(),
                fork_segment.get()
            ),
            Self::ShadowApply { applied, fork } => match applied {
                Some(a) => write!(f, "shadow applied {a}, fork at {:#X}", fork.get()),
                None => write!(f, "no walreceiver attached, fork at {:#X}", fork.get()),
            },
            Self::ArchiveSeal {
                durable,
                fork_segment,
            } => write!(
                f,
                "archive fsynced {:#X}, need {:#X}",
                durable.get(),
                fork_segment.get()
            ),
        }
    }
}

impl ForkWait {
    /// `waiting_on=` label, stable across the three reasons.
    pub fn label(&self) -> &'static str {
        match self {
            Self::EmitterAck { .. } => "emitter_ack",
            Self::ShadowApply { .. } => "shadow_apply",
            Self::ArchiveSeal { .. } => "archive_seal",
        }
    }
}

impl ForkBarrier {
    /// `None` once every consumer has reached the fork; otherwise what is still
    /// behind.
    pub fn pending(
        &self,
        switch_lsn: impl Into<Pos<Switchpoint>>,
        seg_size: u64,
    ) -> Option<ForkWait> {
        let switch_lsn = switch_lsn.into();
        // One barrier, three roles: each comparison unwraps the far side once
        let fork_segment: Pos<Floor> = Pos::new(WalStream::align_down(switch_lsn.get(), seg_size));
        if self.resume_safe_lsn < fork_segment.get() {
            return Some(ForkWait::EmitterAck {
                acked: self.resume_safe_lsn,
                fork_segment,
            });
        }
        if self.shadow_apply_lsn.is_none_or(|a| a < switch_lsn.get()) {
            return Some(ForkWait::ShadowApply {
                applied: self.shadow_apply_lsn,
                fork: switch_lsn,
            });
        }
        if fork_segment > self.filter_durable.get().max(self.floor.get()) {
            return Some(ForkWait::ArchiveSeal {
                durable: self.filter_durable,
                fork_segment,
            });
        }
        None
    }
}

/// Fork the ancestor's walsender reported, proved against the source's history
/// before anything moves. Held across the barrier, so the crossing acts on a
/// proof taken where the branch ended rather than re-deriving one from a source
/// that may have forked again meanwhile.
#[derive(Debug, Clone)]
pub struct ForkPoint {
    pub finished_tli: u32,
    pub next_tli: u32,
    pub live_tli: u32,
    pub switch_lsn: u64,
    /// Every timeline the source forked through, oldest first.
    histories: Vec<TimelineHistory>,
}

impl ForkPoint {
    fn live_history(&self) -> &TimelineHistory {
        self.histories.last().expect("probe pushes at least one")
    }
}

/// Resume position a crossing commits: the fork segment's start, on the
/// descendant. Nothing published afterwards may leave the floor on the ancestor.
#[derive(Debug, Clone, Copy)]
pub struct ForkResume {
    pub timeline: u32,
    pub floor: Pos<Floor>,
    pub switch_lsn: Pos<Switchpoint>,
}

/// One crossing's outcome. The history comes back with it: the floor timeline
/// is resolved against this chain, and a caller still holding the pre-fork
/// history would pin the floor on the ancestor forever.
#[derive(Debug, Clone)]
pub struct Crossing {
    pub finished_tli: u32,
    pub next_tli: u32,
    pub live_tli: u32,
    pub switch_lsn: u64,
    pub prefix_bytes: u64,
    pub history: TimelineHistory,
}

pub struct Switchover<'a> {
    /// Cluster that owns every artifact. Re-proved on each attempt, not only on
    /// the operator's repoint: a crossing retry dials the endpoint again, and a
    /// fork at a segment boundary repeats no bytes for the prefix digest to
    /// catch a foreign cluster with.
    pub system_id: u64,
    /// Filtered archive the shadow's `restore_command` reads; history files
    /// land beside the segments so timeline discovery and every later restart
    /// probe can find them.
    pub out_dir: &'a Path,
    pub shadow_state: &'a Arc<Mutex<ShadowStreamState>>,
}

impl Switchover<'_> {
    /// Prove the reported transition against the source's own history, without
    /// touching anything. The caller holds the result while the pipeline drains
    /// to `switch_lsn`, so this must stay side-effect free.
    ///
    /// `feed` must be in simple-query mode — the caller answers the backend's
    /// `CopyDone` — so a failed attempt can be retried on a fresh connection.
    pub async fn probe(
        &self,
        feed: &mut SourceFeed,
        stream: &WalStream,
        branch_begin: u64,
        stats: &mut TimelineStats,
    ) -> Result<ForkPoint, TransitionError> {
        let started = std::time::Instant::now();
        let out = self.probe_inner(feed, stream, branch_begin).await;
        stats.seconds_total += started.elapsed().as_secs_f64();
        if let Err(e) = &out {
            stats.record_failure(e);
        }
        out
    }

    async fn probe_inner(
        &self,
        feed: &mut SourceFeed,
        stream: &WalStream,
        branch_begin: u64,
    ) -> Result<ForkPoint, TransitionError> {
        let finished_tli = stream.timeline();
        let ident = feed.identify_system().await.map_err(source)?;
        let live: u64 = ident.sysid.parse().map_err(|e| {
            source(anyhow::anyhow!(
                "IDENTIFY_SYSTEM sysid {:?}: {e}",
                ident.sysid
            ))
        })?;
        if live != self.system_id {
            return Err(TransitionError::ForeignSystem {
                stored: self.system_id,
                live,
            });
        }
        let live_tli = ident.timeline;
        if live_tli <= finished_tli {
            return Err(TransitionError::NotDescendant {
                finished: finished_tli,
                live: live_tli,
            });
        }
        // Every timeline the source forked through, oldest first: the shadow
        // probes `<tli>.history` one ID at a time to find the newest branch, so
        // a gap in the chain stops discovery short
        // (`src/backend/access/transam/timeline.c`, `findNewestTimeLine`).
        let mut histories: Vec<TimelineHistory> = Vec::new();
        for tli in (finished_tli + 1)..=live_tli {
            let raw = feed
                .timeline_history(tli)
                .await
                .map_err(source)?
                .ok_or(TransitionError::HistoryMissing { tli })?;
            let history = TimelineHistory::parse(tli, &raw)
                .map_err(|source| TransitionError::HistoryMalformed { tli, source })?;
            histories.push(history);
        }
        let live_history = histories.last().expect("live_tli > finished_tli");
        let not_descendant = || TransitionError::NotDescendant {
            finished: finished_tli,
            live: live_tli,
        };
        // Same number, different branch: two standbys of one primary, promoted
        // independently, are both timeline 2 under one system identifier. Only
        // where the branch begins separates them, and walshadow already proved
        // that against the chain it came in on
        // (plans/failover.md §Lineage)
        let live_begin = live_history
            .begin_of(finished_tli)
            .ok_or_else(not_descendant)?;
        // `0` above timeline 1 is unrecorded, not "begins at 0/0"
        if branch_begin != 0 && live_begin != branch_begin {
            return Err(TransitionError::SiblingBranch {
                tli: finished_tli,
                stored_begin: branch_begin,
                live_begin,
            });
        }
        let switch_lsn = live_history
            .switchpoint_of(finished_tli)
            .ok_or_else(not_descendant)?;
        let next_tli = live_history
            .successor_of(finished_tli)
            .ok_or_else(not_descendant)?;
        // The walsender stops exactly at the switchpoint, so a frontier above
        // it means bytes were read that the descendant branch never had
        if stream.next_lsn() > switch_lsn {
            return Err(TransitionError::ResumePastFork {
                next_lsn: stream.next_lsn(),
                switch_lsn,
            });
        }
        Ok(ForkPoint {
            finished_tli,
            next_tli,
            live_tli,
            switch_lsn,
            histories,
        })
    }

    /// Walk one probed transition. Repeat while the stream is still behind the
    /// live timeline; each call crosses exactly one fork.
    ///
    /// `commit` persists [`ForkResume`] and must not return until it is durable:
    /// it is the point the stream stops being the ancestor's. The caller runs
    /// the barrier before getting here, so by now nothing below the fork is in
    /// flight anywhere.
    ///
    /// `slot` comes per call, not off the struct: it reloads with `[source]`,
    /// so a crossing after a repoint asks the descendant for the slot the
    /// promotion target owns.
    #[allow(clippy::too_many_arguments)]
    pub async fn cross<C>(
        &self,
        feed: &mut SourceFeed,
        slot: Option<&str>,
        stream: &mut WalStream,
        record_sink: &mut (dyn RecordSink + Send),
        segment_sink: &mut (dyn SegmentSink + Send),
        status: StandbyStatus,
        guards: ForkGuards,
        fork: &ForkPoint,
        commit: C,
        stats: &mut TimelineStats,
    ) -> Result<Crossing, TransitionError>
    where
        C: AsyncFnOnce(ForkResume) -> anyhow::Result<()>,
    {
        let started = std::time::Instant::now();
        let out = self
            .cross_inner(
                feed,
                slot,
                stream,
                record_sink,
                segment_sink,
                status,
                guards,
                fork,
                commit,
            )
            .await;
        stats.seconds_total += started.elapsed().as_secs_f64();
        match &out {
            Ok(c) => {
                stats.switches += 1;
                stats.switch_lsn = c.switch_lsn;
                stats.prefix_bytes_verified += c.prefix_bytes;
            }
            Err(e) => stats.record_failure(e),
        }
        out
    }

    #[allow(clippy::too_many_arguments)]
    async fn cross_inner<C>(
        &self,
        feed: &mut SourceFeed,
        slot: Option<&str>,
        stream: &mut WalStream,
        record_sink: &mut (dyn RecordSink + Send),
        segment_sink: &mut (dyn SegmentSink + Send),
        status: StandbyStatus,
        guards: ForkGuards,
        fork: &ForkPoint,
        commit: C,
    ) -> Result<Crossing, TransitionError>
    where
        C: AsyncFnOnce(ForkResume) -> anyhow::Result<()>,
    {
        let &ForkPoint {
            finished_tli,
            next_tli,
            switch_lsn,
            ..
        } = fork;
        if guards.drain_lsn.get() > switch_lsn {
            return Err(TransitionError::PublishedPastFork {
                drain_lsn: guards.drain_lsn.get(),
                switch_lsn,
            });
        }
        if guards.open_xacts > 0 {
            return Err(TransitionError::OpenXactAtFork {
                finished: finished_tli,
                open_xacts: guards.open_xacts,
            });
        }
        // History before the crossing: a shadow that reads its history file
        // too early assumes a parentless timeline and then cannot find any
        // ancestor-named segment
        for history in &fork.histories {
            crate::fs::write_atomic(
                self.out_dir,
                &history_filename(history.target()),
                history.raw(),
            )
            .await
            .map_err(TransitionError::PersistHistory)?;
        }
        let ancestor = stream.fork_prefix();
        let seg_start = WalStream::align_down(switch_lsn, stream.seg_size());
        if (ancestor.from, ancestor.through) != (seg_start, switch_lsn) {
            return Err(TransitionError::PrefixNotRetained {
                from: seg_start,
                to: switch_lsn,
            });
        }
        // The promotion target's slot has to already reach the position about to
        // be committed. `START_REPLICATION` would answer for the request alone,
        // leaving a slot that pins nothing below it to be found at the next
        // restart (plans/failover.md §Slot)
        if let Some(name) = slot {
            let restart_lsn = feed
                .prove_physical_slot(name, Pos::new(seg_start), Pos::new(seg_start))
                .await?;
            tracing::info!(
                target: "walshadow",
                slot = name,
                restart_lsn = restart_lsn.map(|l| walrus::pg::backup::format_pg_lsn(l).to_string()),
                resume_lsn = %walrus::pg::backup::format_pg_lsn(seg_start),
                "target slot reaches the fork resume position",
            );
        }
        feed.start_physical_replication(slot, seg_start, next_tli)
            .await
            .map_err(source)?;
        let (prefix_bytes, past_fork) = self
            .verify_prefix(feed, switch_lsn, status, ancestor)
            .await?;
        // The hinge. Above this line the stream is still the ancestor's at `F`
        // and every failure re-crosses from there; below it the resume position
        // is the fork segment's start on the descendant
        commit(ForkResume {
            timeline: next_tli,
            floor: Pos::new(seg_start),
            switch_lsn: Pos::new(switch_lsn),
        })
        .await
        .map_err(TransitionError::Commit)?;
        stream
            .transition_timeline(next_tli, switch_lsn, segment_sink)
            .await?;
        // Advertise between the commit and the first descendant byte. Later and
        // a client still reading the ancestor is handed a descendant record,
        // which it PANICs on (`src/backend/access/transam/xlog.c`: a checkpoint
        // or end-of-recovery record whose timeline is not the one being
        // replayed). The descendant's own history goes with it, since that is
        // what its walreceiver fetches once `IDENTIFY_SYSTEM` reports the new
        // branch
        let next_history = fork
            .histories
            .iter()
            .find(|h| h.target() == next_tli)
            .map(|h| h.raw().to_vec())
            .unwrap_or_default();
        self.shadow_state
            .lock()
            .await
            .advertise_timeline(next_tli, switch_lsn, next_history);
        if !past_fork.is_empty() {
            stream
                .push(switch_lsn, &past_fork, record_sink, segment_sink)
                .await?;
        }
        Ok(Crossing {
            finished_tli,
            next_tli,
            live_tli: fork.live_tli,
            switch_lsn,
            prefix_bytes,
            history: fork.live_history().clone(),
        })
    }

    /// Always restart the descendant at the fork segment's start, matching
    /// `pg_receivewal`: the repeated `[from, switch_lsn)` bytes are the verbatim
    /// copy PostgreSQL made of the ancestor prefix (`XLogInitNewTimeline`), so
    /// digesting them and comparing against what the ancestor fed catches a
    /// branch that shares the system identifier but not the WAL. Matching bytes
    /// are suppressed rather than re-fed, so no record is classified or
    /// buffered twice.
    ///
    /// Returns the prefix length verified plus whatever the last chunk carried
    /// past the fork, held for the caller to push once the crossing has been
    /// advertised. Nothing here touches the stream: a mismatch has to leave the
    /// ancestor exactly as it was.
    async fn verify_prefix(
        &self,
        feed: &mut SourceFeed,
        switch_lsn: u64,
        status: StandbyStatus,
        ancestor: ForkPrefix,
    ) -> Result<(u64, Vec<u8>), TransitionError> {
        let mut pos = ancestor.from;
        let mut crc = 0u32;
        let mut buf = Vec::new();
        let mut past_fork = Vec::new();
        while pos < switch_lsn {
            let chunk = match feed.next_event(status, &mut buf).await.map_err(source)? {
                SourceEvent::Wal(c) => c,
                SourceEvent::TimelineEnd | SourceEvent::Shutdown => {
                    return Err(TransitionError::PrefixTruncated { lsn: pos });
                }
            };
            if chunk.start_lsn != pos {
                return Err(TransitionError::PrefixNotRetained {
                    from: pos,
                    to: chunk.start_lsn,
                });
            }
            let overlap = ((switch_lsn - pos) as usize).min(chunk.data.len());
            crc = crc32c::crc32c_append(crc, &chunk.data[..overlap]);
            pos += chunk.data.len() as u64;
            // Held back, not pushed: nothing from the descendant is adopted
            // until its copy of the prefix has answered for itself
            past_fork.extend_from_slice(&chunk.data[overlap..]);
        }
        if crc != ancestor.crc {
            return Err(TransitionError::ForkPrefixMismatch {
                from: ancestor.from,
                to: switch_lsn,
            });
        }
        Ok((switch_lsn - ancestor.from, past_fork))
    }
}

fn source(e: anyhow::Error) -> TransitionError {
    TransitionError::Source(e)
}

/// Non-retryable crossing failure parked for operator intervention
#[derive(Debug, Clone)]
pub struct CrossingWedge {
    pub reason: &'static str,
    pub detail: String,
}

/// Timeline crossing state retained across pump iterations
#[derive(Debug, Default)]
pub struct CrossingState {
    pending: bool,
    reconnect: bool,
    retry_at: Option<Instant>,
    wedge: Option<CrossingWedge>,
    fork: Option<ForkPoint>,
}

impl CrossingState {
    pub fn ancestor_ended(&mut self) {
        self.pending = true;
    }

    pub fn pending(&self) -> bool {
        self.pending
    }

    pub fn wedge(&self) -> Option<&CrossingWedge> {
        self.wedge.as_ref()
    }

    pub fn needs_connection(&mut self) {
        self.reconnect = true;
    }

    pub fn awaiting_connection(&self) -> bool {
        self.reconnect
    }

    pub fn connected(&mut self) {
        self.reconnect = false;
    }

    pub fn due(&self, now: Instant) -> bool {
        self.pending && self.wedge.is_none() && self.retry_at.is_none_or(|at| now >= at)
    }

    pub fn retry_at(&mut self, at: Instant) {
        self.retry_at = Some(at);
    }

    pub fn retry_from_source(&mut self, at: Instant) {
        self.reconnect = true;
        self.retry_at = Some(at);
    }

    pub fn hold_fork(&mut self, fork: ForkPoint) {
        self.fork = Some(fork);
    }

    pub fn has_fork(&self) -> bool {
        self.fork.is_some()
    }

    pub fn take_fork(&mut self) -> Option<ForkPoint> {
        self.fork.take()
    }

    pub fn park(&mut self, err: TransitionError, consumed: u64, switch_lsn: Option<u64>) {
        let wedge = CrossingWedge {
            reason: err.reason(),
            detail: format!("{err:#}"),
        };
        tracing::error!(
            target: "walshadow",
            reason = wedge.reason,
            error = %wedge.detail,
            consumed_lsn = %format_pg_lsn(consumed),
            switch_lsn = switch_lsn.map(|l| format_pg_lsn(l).to_string()),
            "crossing refused — pump parked; fix what the reason names, then \
             pause and resume to prove the fork again",
        );
        self.wedge = Some(wedge);
    }

    pub fn unpark(&mut self) -> Option<CrossingWedge> {
        let wedge = self.wedge.take()?;
        self.fork = None;
        self.reconnect = true;
        Some(wedge)
    }

    pub fn committed(&mut self) {
        self.pending = false;
        self.reconnect = false;
        self.retry_at = None;
        self.fork = None;
    }
}

/// Load live timeline history, requiring ancestry proof when branch changed
pub async fn load_boot_history(
    feed: &mut SourceFeed,
    live_timeline: u32,
    stored_timeline: u32,
) -> Result<TimelineHistory> {
    if live_timeline <= 1 {
        return Ok(TimelineHistory::root(live_timeline));
    }
    let missing = || {
        format!(
            "source reports timeline {live_timeline} but serves no history file for it, \
             so nothing can be placed against its ancestry",
        )
    };
    let Some(raw) = feed.timeline_history(live_timeline).await? else {
        anyhow::ensure!(
            live_timeline == stored_timeline,
            "history_missing: {}, and stored timeline {stored_timeline} needs that proof",
            missing(),
        );
        tracing::warn!(target: "walshadow", live_timeline, "history_missing: {}", missing());
        return Ok(TimelineHistory::root(live_timeline));
    };
    TimelineHistory::parse(live_timeline, &raw)
        .with_context(|| format!("history_malformed: timeline {live_timeline}"))
}

/// Re-advertise crossings floor passed before shadow
pub async fn seed_shadow_branches(
    state: &mut ShadowStreamState,
    feed: &mut SourceFeed,
    history: &TimelineHistory,
    out_dir: &Path,
    through: u32,
) -> Result<()> {
    while state.timeline < through {
        let finished = state.timeline;
        let next = history
            .successor_of(finished)
            .with_context(|| format!("timeline {finished} has no successor in the chain"))?;
        let switch_lsn = history
            .begin_of(next)
            .with_context(|| format!("timeline {next} has no switchpoint in the chain"))?;
        let raw = if next == history.target() {
            history.raw().to_vec()
        } else {
            feed.timeline_history(next)
                .await?
                .with_context(|| format!("history_missing: timeline {next}"))?
        };
        tokio::fs::create_dir_all(out_dir)
            .await
            .with_context(|| format!("create filtered dir {}", out_dir.display()))?;
        crate::fs::write_atomic(out_dir, &history_filename(next), &raw)
            .await
            .with_context(|| format!("persist history for timeline {next}"))?;
        state.advertise_timeline(next, switch_lsn, raw);
        tracing::info!(
            target: "walshadow",
            finished_timeline = finished,
            next_timeline = next,
            switch_lsn = %format_pg_lsn(switch_lsn),
            "re-advertising a crossing the floor already made",
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_reason_has_a_label() {
        let errs = [
            TransitionError::NotDescendant {
                finished: 4,
                live: 3,
            },
            TransitionError::ForeignSystem { stored: 1, live: 2 },
            TransitionError::SiblingBranch {
                tli: 2,
                stored_begin: 0x300_0000,
                live_begin: 0x500_0000,
            },
            TransitionError::Slot(SlotError::Missing {
                slot: "s".into(),
                kind: None,
            }),
            TransitionError::Slot(SlotError::TooNew {
                slot: "s".into(),
                restart_lsn: 9,
                resume_lsn: 8,
            }),
            TransitionError::Slot(SlotError::Query(anyhow::anyhow!("x"))),
            TransitionError::HistoryMissing { tli: 5 },
            TransitionError::HistoryMalformed {
                tli: 5,
                source: HistoryError::TargetNotAbove {
                    target: 5,
                    highest: 5,
                },
            },
            TransitionError::ResumePastFork {
                next_lsn: 2.into(),
                switch_lsn: 1,
            },
            TransitionError::PublishedPastFork {
                drain_lsn: 2,
                switch_lsn: 1,
            },
            TransitionError::OpenXactAtFork {
                finished: 4,
                open_xacts: 2,
            },
            TransitionError::ForkPrefixMismatch { from: 8, to: 9 },
            TransitionError::PrefixTruncated { lsn: 9 },
            TransitionError::PrefixNotRetained { from: 1, to: 2 },
            TransitionError::PersistHistory(io::Error::other("x")),
            TransitionError::Commit(anyhow::anyhow!("x")),
            TransitionError::Source(anyhow::anyhow!("x")),
        ];
        for e in &errs {
            assert!(
                SWITCH_FAILURE_REASONS.contains(&e.reason()),
                "{} → unlabelled reason {}",
                e,
                e.reason(),
            );
        }
    }

    #[test]
    fn failures_count_under_their_own_reason() {
        let mut stats = TimelineStats::default();
        stats.record_failure(&TransitionError::PublishedPastFork {
            drain_lsn: 2,
            switch_lsn: 1,
        });
        stats.record_failure(&TransitionError::PrefixTruncated { lsn: 4 });
        stats.record_failure(&TransitionError::ForkPrefixMismatch { from: 8, to: 9 });
        let at = |reason: &str| {
            SWITCH_FAILURE_REASONS
                .iter()
                .position(|r| *r == reason)
                .unwrap()
        };
        assert_eq!(stats.failures_by_reason[at("published_past_fork")], 1);
        assert_eq!(
            stats.failures_by_reason[at("fork_prefix_mismatch")],
            2,
            "a truncated prefix is a prefix failure",
        );
        assert_eq!(stats.switches, 0);
    }

    const SEG: u64 = 16 * 1024 * 1024;

    /// Everything caught up to a fork one segment in, at offset 0x1000
    fn caught_up() -> ForkBarrier {
        ForkBarrier {
            resume_safe_lsn: (SEG + 0x800).into(),
            shadow_apply_lsn: Some((SEG + 0x1000).into()),
            filter_durable: SEG.into(),
            floor: Pos::ZERO,
        }
    }

    #[test]
    fn barrier_opens_once_every_frontier_reaches_the_fork() {
        assert_eq!(caught_up().pending(SEG + 0x1000, SEG), None);
    }

    /// An unacked commit *inside* the fork segment does not hold the barrier: a
    /// restart from the segment's start re-reads it. One below that boundary
    /// would be skipped, so it does
    #[test]
    fn barrier_holds_only_for_an_ack_below_the_fork_segment() {
        assert_eq!(
            ForkBarrier {
                resume_safe_lsn: SEG.into(),
                ..caught_up()
            }
            .pending(SEG + 0x1000, SEG),
            None,
            "acked exactly at the fork segment's start is enough",
        );
        assert_eq!(
            ForkBarrier {
                resume_safe_lsn: (SEG - 1).into(),
                ..caught_up()
            }
            .pending(SEG + 0x1000, SEG),
            Some(ForkWait::EmitterAck {
                acked: (SEG - 1).into(),
                fork_segment: SEG.into(),
            }),
        );
    }

    #[test]
    fn barrier_holds_until_shadow_replays_the_ancestors_last_record() {
        let fork: Pos<Switchpoint> = (SEG + 0x1000).into();
        for applied in [None, Some((fork.get() - 1).into())] {
            let b = ForkBarrier {
                shadow_apply_lsn: applied,
                ..caught_up()
            };
            assert_eq!(
                b.pending(fork, SEG),
                Some(ForkWait::ShadowApply { applied, fork }),
                "applied {applied:?} must not open the barrier",
            );
        }
    }

    /// The fork segment's start only becomes a resume position once what
    /// precedes it is fsynced — or is already covered by the floor, which is
    /// where a run that started at that boundary sits
    #[test]
    fn barrier_holds_for_an_unsealed_archive_unless_the_floor_covers_it() {
        let b = ForkBarrier {
            filter_durable: Pos::ZERO,
            ..caught_up()
        };
        assert_eq!(
            b.pending(SEG + 0x1000, SEG),
            Some(ForkWait::ArchiveSeal {
                durable: Pos::ZERO,
                fork_segment: SEG.into(),
            }),
        );
        assert_eq!(
            ForkBarrier {
                floor: SEG.into(),
                ..b
            }
            .pending(SEG + 0x1000, SEG),
            None,
            "a floor already at the fork segment needs no seal",
        );
    }

    #[test]
    fn only_source_and_storage_failures_retry() {
        assert!(TransitionError::Source(anyhow::anyhow!("drop")).retryable());
        assert!(TransitionError::PersistHistory(io::Error::other("x")).retryable());
        assert!(
            !TransitionError::PublishedPastFork {
                drain_lsn: 2,
                switch_lsn: 1
            }
            .retryable(),
        );
        assert!(!TransitionError::ForkPrefixMismatch { from: 8, to: 9 }.retryable(),);
        assert!(
            TransitionError::Slot(SlotError::Query(anyhow::anyhow!("x"))).retryable(),
            "reading pg_replication_slots is source trouble",
        );
        assert!(
            !TransitionError::Slot(SlotError::Missing {
                slot: "s".into(),
                kind: Some("logical".into()),
            })
            .retryable(),
            "the target's slot is the operator's to create",
        );
    }

    #[test]
    fn a_parked_crossing_is_not_due_until_a_pause_clears_it() {
        let now = Instant::now();
        let mut c = CrossingState::default();
        assert!(!c.due(now), "no crossing without an ended ancestor");
        c.ancestor_ended();
        assert!(c.due(now));
        c.hold_fork(ForkPoint {
            finished_tli: 1,
            next_tli: 2,
            live_tli: 2,
            switch_lsn: 0x300_0000,
            histories: Vec::new(),
        });
        c.park(
            TransitionError::ForkPrefixMismatch { from: 8, to: 9 },
            0x300_0000,
            None,
        );
        assert!(!c.due(now));
        assert_eq!(c.wedge().map(|w| w.reason), Some("fork_prefix_mismatch"));
        assert_eq!(c.unpark().map(|w| w.reason), Some("fork_prefix_mismatch"));
        assert!(
            !c.has_fork() && c.awaiting_connection(),
            "clearing a park re-proves from a fresh connection",
        );
        assert!(c.due(now), "the ancestor still ended");
        assert!(c.unpark().is_none());
        c.committed();
        assert!(!c.pending() && !c.due(now));
    }

    #[test]
    fn a_retry_is_due_only_once_its_backoff_elapses() {
        let now = Instant::now();
        let mut c = CrossingState::default();
        c.ancestor_ended();
        c.retry_from_source(now + std::time::Duration::from_secs(2));
        assert!(c.awaiting_connection());
        assert!(!c.due(now));
        assert!(c.due(now + std::time::Duration::from_secs(3)));
        c.connected();
        assert!(!c.awaiting_connection());
    }
}
