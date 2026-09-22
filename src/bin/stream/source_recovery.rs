//! Source reconnect, endpoint swap, and timeline crossing — everything the
//! pump does when the source it was reading stops answering or forks.

use std::path::Path;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use tokio_postgres::types::PgLsn;
use walrus::pg::replication::conn::PgConfig;
use walshadow::config::SourceConn;
use walshadow::manifest;
use walshadow::pos::{Floor, Monotone, Pos};
use walshadow::record::WAL_SEG_SIZE;
use walshadow::source_feed::SourceFeed;
use walshadow::timeline::TimelineHistory;
use walshadow::transition::{TransitionError, source_history};
use walshadow::wal_stream::WalStream;

use crate::archive::ArchiveFeed;
use crate::args::{Args, cli_base};

/// How long the fork proofs wait for the pump-side queue to drain. Past it the
/// buffer's own view answers, which reads a still-queued record as a
/// transaction open at the fork and refuses the crossing — the fail-closed
/// direction.
pub(crate) const FORK_FENCE_DRAIN: Duration = Duration::from_secs(30);

/// Branch the stream is reading, as a reconnect has to name it: number plus the
/// switchpoint the proved chain places it at.
pub(crate) fn stream_branch(
    history: &TimelineHistory,
    system_id: u64,
    stream: &WalStream,
) -> SourceBranch {
    SourceBranch {
        system_id,
        timeline: stream.timeline(),
        begin: history.begin_of(stream.timeline()).unwrap_or(0),
    }
}

/// Step 5's gate: what the promotion target owes before it may be promoted,
/// answered off the source connection walshadow already holds rather than a
/// second `psql` (architecture/recovery.md).
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct PromotionGate {
    pub(crate) ready: bool,
    /// Term that fails, empty once ready
    pub(crate) blocked_on: &'static str,
    pub(crate) in_recovery: bool,
    pub(crate) replay_lsn: u64,
    pub(crate) receive_lsn: u64,
}

impl PromotionGate {
    pub(crate) fn blocked(blocked_on: &'static str) -> Self {
        Self {
            blocked_on,
            ..Self::default()
        }
    }

    pub(crate) fn unreachable() -> Self {
        Self::blocked("source_unreachable")
    }
}

/// How often the gate is re-read while paused, and how long one read may take
/// before the endpoint counts as unreachable. The pump publishes every tick, so
/// a target that stops answering must not stall the loop with it.
pub(crate) const PROMOTION_POLL: Duration = Duration::from_secs(1);

/// Read the gate off `feed`'s sidecar SQL connection. Only meaningful while
/// paused: `pause_received` is the frozen head the target has to reach, and an
/// unfrozen one moves under the decision.
pub(crate) async fn promotion_gate(
    feed: &mut SourceFeed,
    pause_frontier: Option<(u64, u64)>,
) -> PromotionGate {
    let Some((_, pause_received)) = pause_frontier else {
        return PromotionGate::blocked("not_paused");
    };
    let client = match feed.sql_client().await {
        Ok(c) => c,
        Err(e) => {
            tracing::debug!(target: "walshadow", error = %format!("{e:#}"), "promotion gate");
            return PromotionGate::unreachable();
        }
    };
    let row = client
        .query_one(
            "SELECT pg_is_in_recovery(), pg_last_wal_replay_lsn(), pg_last_wal_receive_lsn()",
            &[],
        )
        .await;
    let row = match row {
        Ok(row) => row,
        Err(e) => {
            tracing::debug!(target: "walshadow", error = %e, "promotion gate");
            feed.drop_sql_client();
            return PromotionGate::unreachable();
        }
    };
    let in_recovery: bool = row.get(0);
    let replay_lsn = row.get::<_, Option<PgLsn>>(1).map(u64::from).unwrap_or(0);
    let receive_lsn = row.get::<_, Option<PgLsn>>(2).map(u64::from).unwrap_or(0);
    // Order names the first term to fix, not every one that fails
    let blocked_on = if !in_recovery {
        "not_a_standby"
    } else if replay_lsn < pause_received {
        "replay_below_pause_received"
    } else if receive_lsn > replay_lsn {
        "received_not_replayed"
    } else {
        ""
    };
    PromotionGate {
        ready: blocked_on.is_empty(),
        blocked_on,
        in_recovery,
        replay_lsn,
        receive_lsn,
    }
}

/// Cadence of the fork barrier's progress line. The barrier is unbounded by
/// design — the source has stopped, so waiting costs nothing that is moving —
/// which makes the log the only place the wait is legible.
pub(crate) const BARRIER_LOG_INTERVAL: Duration = Duration::from_secs(2);

/// Manifest for one resume point, floor included. The pump loop's cadence
/// write and the shutdown write have to land the same floor, so both derive it
/// here rather than each from the terms it happens to hold
pub(crate) fn resume_manifest(
    history: &TimelineHistory,
    identity: &manifest::SourceIdentity,
    published_floor: Pos<Floor>,
    shadow_floor: manifest::ShadowFloor,
    stream_timeline: u32,
    lsn: manifest::LsnSet,
) -> manifest::Manifest {
    // A rewind (`--start-lsn`, `--ignore-cursor`) lowers the floor by seeding
    // `resume_floor` at the rewind point, never through these terms
    let floor = manifest::FloorInputs {
        resume_safe: lsn.emitter_ack,
        filter_durable: lsn.filter_durable,
        shadow: shadow_floor,
        published: published_floor,
        fork: None,
    }
    .floor();
    let floor_timeline = history.floor_branch(
        floor.get(),
        identity.timeline,
        stream_timeline,
        WAL_SEG_SIZE,
    );
    manifest::Manifest {
        version: manifest::MANIFEST_VERSION,
        floor,
        source: manifest::SourceIdentity {
            system_id: identity.system_id,
            timeline: floor_timeline,
            timeline_begin: Pos::new(history.begin_of(floor_timeline).unwrap_or(0)),
        },
        wal: manifest::WalBranch { stream_timeline },
        lsn,
    }
}

/// Commit a crossing's resume position: the fork segment's start, on the
/// descendant. Sound only behind the barrier, which proved nothing below the
/// fork is still in flight — the floor's contract is that a restart from it
/// loses nothing, not that the natural terms have caught up to it
/// (architecture/recovery.md).
///
/// Publishes to the pruners only after the persist, the same order the status
/// loop uses: a cut must never sit above what a crash-now restart replays from.
pub(crate) async fn commit_fork_resume(
    spill_dir: &Path,
    identity: &manifest::SourceIdentity,
    resume: walshadow::transition::ForkResume,
    lsn: manifest::LsnSet,
    resume_floor: &Monotone<Floor>,
    gc_floor: &Monotone<Floor>,
) -> Result<()> {
    let floor = manifest::FloorInputs {
        resume_safe: lsn.emitter_ack,
        filter_durable: lsn.filter_durable,
        published: resume_floor.get(),
        fork: Some(resume.floor),
        ..manifest::FloorInputs::default()
    }
    .floor();
    let committed = manifest::Manifest {
        version: manifest::MANIFEST_VERSION,
        floor,
        source: manifest::SourceIdentity {
            system_id: identity.system_id,
            timeline: resume.timeline,
            // The fork is where the descendant begins, so the next boot can
            // refuse a sibling that shares its number
            timeline_begin: resume.switch_lsn,
        },
        wal: manifest::WalBranch {
            stream_timeline: resume.timeline,
        },
        lsn,
    };
    manifest::write(spill_dir, &committed)
        .await
        .context("write resume manifest at the fork")?;
    // Descendant floor starts new position space
    resume_floor.rebase(floor);
    gc_floor.rebase(floor);
    tracing::info!(
        target: "walshadow",
        timeline = resume.timeline,
        floor = %floor,
        switch_lsn = %resume.switch_lsn,
        "committed the fork resume position",
    );
    Ok(())
}

/// Dial `[source]` until it answers, re-resolving the endpoint between
/// attempts.
///
/// Exiting instead would crash-loop the window a switchover opens between
/// stopping writes on the old primary and repointing at the target
/// (architecture/recovery.md): every restart there dials a server
/// that is down. `ctl` and `/metrics` are bound before this, so the repoint
/// that ends the wait can be applied to the daemon doing the waiting.
pub(crate) async fn connect_source_waiting(
    args: &Args,
    source_conn: &mut SourceConn,
    cfg: &mut PgConfig,
) -> Result<SourceFeed> {
    loop {
        match SourceFeed::connect(cfg).await {
            Ok(feed) => {
                return Ok(feed.with_status_interval(Duration::from_secs(args.status_interval)));
            }
            Err(e) => tracing::warn!(
                target: "walshadow",
                error = %format!("{e:#}"),
                endpoint = source_conn.endpoint(),
                "source unreachable — waiting for it, or for a repoint",
            ),
        }
        tokio::time::sleep(SOURCE_SWAP_RETRY).await;
        let Some(path) = args.ch_config.as_deref() else {
            continue;
        };
        match walshadow::ch_emitter::load_effective(path, cli_base(args)).await {
            Ok(table) => match SourceConn::from_table(&table).map(|mut next| {
                // Preserve CLI slot override across reloads
                if args.slot.is_some() {
                    next.slot = args.slot.clone();
                }
                next
            }) {
                Ok(next) if next != *source_conn => {
                    tracing::info!(
                        target: "walshadow",
                        from = source_conn.endpoint(),
                        to = next.endpoint(),
                        slot = next.slot.as_deref(),
                        "source moved while waiting",
                    );
                    *source_conn = next;
                    *cfg = source_conn.to_pg_config();
                }
                Ok(_) => {}
                Err(e) => tracing::warn!(target: "walshadow", error = %e, "[source] reload"),
            },
            Err(e) => {
                tracing::warn!(target: "walshadow", error = %format!("{e:#}"), "config reload")
            }
        }
    }
}

/// Backoff between attempts at a moved `[source]` endpoint. The old feed keeps
/// streaming meanwhile, so this only paces retries against an endpoint that is
/// not up yet (repointed before the target accepts connections).
pub(crate) const SOURCE_SWAP_RETRY: Duration = Duration::from_secs(2);

/// Cluster plus the branch the stream is reading, what a resumed connection has
/// to match.
#[derive(Debug, Clone, Copy)]
pub(crate) struct SourceBranch {
    pub(crate) system_id: u64,
    pub(crate) timeline: u32,
    /// Where that branch begins per the chain walshadow proved. A timeline
    /// number is not unique across branches — two standbys of one primary,
    /// promoted independently, are both timeline 2 under one system identifier
    /// — so number equality alone accepts a sibling
    /// (architecture/recovery.md). `0` above timeline 1 means unrecorded.
    pub(crate) begin: u64,
}

/// Dial the source and resume at `resume_lsn`, proving continuity first:
///
/// 1. same cluster, or foreign WAL replays into these artifacts
/// 2. the live chain places the requested branch where walshadow left it,
///    which is what separates a descendant from a sibling sharing its number
/// 3. the requested branch still serves `resume_lsn`
/// 4. the configured slot reaches `floor`, the position a restart asks for
///
/// A live timeline *newer* than the requested one is a promotion that landed
/// under a stable endpoint, so the request stays on the requested branch: the
/// walsender then ends it at the fork and the crossing takes over, needing no
/// operator repoint and no daemon restart. `[source]` is live-reloadable, so
/// the address reached here can differ from the one boot dialed and these
/// proofs are what make that safe.
///
/// Resume is LSN-exact, so `WalStream`, filter, and catalog state stand and no
/// WAL is re-read.
pub(crate) async fn resume_source_feed(
    cfg: &PgConfig,
    slot: Option<&str>,
    resume_lsn: Pos<Floor>,
    branch: SourceBranch,
    floor: Pos<Floor>,
    status_interval: Duration,
) -> Result<SourceFeed> {
    let mut feed = SourceFeed::connect(cfg)
        .await
        .with_context(|| format!("connect source {}:{}", cfg.host, cfg.port))?
        .with_status_interval(status_interval);
    let ident = feed.identify_system().await.context("IDENTIFY_SYSTEM")?;
    let system_id: u64 = ident.sysid.parse().context("IDENTIFY_SYSTEM sysid")?;
    anyhow::ensure!(
        system_id == branch.system_id,
        "source is system {system_id}, artifacts belong to {}",
        branch.system_id,
    );
    anyhow::ensure!(
        ident.timeline >= branch.timeline,
        "source is on timeline {}, below the stream's {}; an older branch cannot \
         serve what has already been read",
        ident.timeline,
        branch.timeline,
    );
    match source_history(&mut feed, ident.timeline).await? {
        Some(history) => prove_branch(&history, branch, resume_lsn.get())?,
        // Timeline 1 has no history file, and a source serving none for a newer
        // branch can place nothing; only a run that never left the branch it is
        // asking for is provable without one
        None if ident.timeline == branch.timeline && branch.begin == 0 => {}
        None => Err(TransitionError::HistoryMissing {
            tli: ident.timeline,
        })?,
    }
    if let Some(name) = slot {
        feed.prove_physical_slot(name, resume_lsn, floor)
            .await
            .map_err(TransitionError::from)?;
    }
    feed.start_physical_replication(slot, resume_lsn.get(), branch.timeline)
        .await
        .with_context(|| format!("START_REPLICATION at {resume_lsn}"))?;
    Ok(feed)
}

/// The live chain has to agree with the branch walshadow is reading, both about
/// where it began and about it still owning `resume_lsn`. Typed with the
/// crossing's own vocabulary, so a refused reconnect names the same proof a
/// refused crossing would.
pub(crate) fn prove_branch(
    history: &TimelineHistory,
    branch: SourceBranch,
    resume_lsn: u64,
) -> Result<(), TransitionError> {
    let live_begin =
        history
            .begin_of(branch.timeline)
            .ok_or_else(|| TransitionError::NotDescendant {
                finished: branch.timeline,
                live: history.target(),
            })?;
    // `0` above timeline 1 is unrecorded, not "begins at 0/0": `--ignore-cursor`
    // adopts a live branch without a chain to read a switchpoint from
    if branch.begin != 0 && live_begin != branch.begin {
        return Err(TransitionError::SiblingBranch {
            tli: branch.timeline,
            stored_begin: branch.begin,
            live_begin,
        });
    }
    if !history.proves_ancestor(branch.timeline, resume_lsn) {
        return Err(TransitionError::ResumePastFork {
            next_lsn: Pos::new(resume_lsn),
            switch_lsn: history.switchpoint_of(branch.timeline).unwrap_or(0),
        });
    }
    Ok(())
}

/// `reason=` label for a refused reconnect. Same vocabulary as a refused
/// crossing: an endpoint move that cannot proceed is a switchover proof
/// failing, and "the swap failed" alone does not say which.
pub(crate) fn swap_reason(err: &anyhow::Error) -> &'static str {
    err.downcast_ref::<TransitionError>()
        .map(TransitionError::reason)
        .unwrap_or("source")
}

/// Where the pump reads WAL from; `feed` serves only [`Live`](Self::Live)
pub(crate) enum SourcePath {
    Live,
    Archive(ArchiveFeed),
    Redial(Redial),
}

impl SourcePath {
    pub(crate) fn archive(&mut self) -> Option<&mut ArchiveFeed> {
        match self {
            Self::Archive(a) => Some(a),
            _ => None,
        }
    }
}

/// Source lost with no archive to read, redialed each due pump iteration so
/// the loop keeps publishing, pausing and applying `[source]` repoints
pub(crate) struct Redial {
    pub(crate) retry_at: Instant,
    pub(crate) backoff: Duration,
    /// Why the archive could not stand in, named if the source cannot serve
    pub(crate) archive_error: String,
}

impl Redial {
    pub(crate) const MIN_BACKOFF: Duration = Duration::from_millis(200);
    pub(crate) const MAX_BACKOFF: Duration = Duration::from_secs(10);

    pub(crate) fn now(archive_error: String) -> Self {
        Self {
            retry_at: Instant::now(),
            backoff: Self::MIN_BACKOFF,
            archive_error,
        }
    }
}

pub(crate) struct SourceRecovery<'a> {
    pub(crate) status_interval: Duration,
    pub(crate) backup: Option<&'a walrus::config::Settings>,
    /// Published resume floor, which is what a slot on the far end has to still
    /// reach — the reconnect's own `resume_lsn` sits above it
    pub(crate) floor: &'a Monotone<Floor>,
    pub(crate) prefetch: usize,
}

impl SourceRecovery<'_> {
    /// Try source, otherwise start bounded archive fetches for normal pump,
    /// otherwise redial. `cfg`, `slot`, and `branch` are the live endpoint,
    /// slot name, and proved branch, passed per call rather than held, so a
    /// recovery that starts after a `[source]` reload or a crossing dials the
    /// new address under the new name and asks for the descendant, with the
    /// archive read under its segment names.
    pub(crate) async fn recover(
        &self,
        source_error: anyhow::Error,
        cfg: &PgConfig,
        slot: Option<&str>,
        branch: SourceBranch,
        resume_lsn: Pos<Floor>,
        feed: &mut SourceFeed,
    ) -> SourcePath {
        // Source first (primary_conninfo analog): a plain drop is usually
        // transient, so try the source again at the exact resume point before
        // reaching for the archive. A removed-WAL (58P01) error means the
        // source genuinely can't serve it — skip straight to the archive.
        let source_missing = walshadow::source_feed::is_wal_segment_removed(&source_error);
        let reason = if source_missing {
            source_error
        } else {
            match resume_source_feed(
                cfg,
                slot,
                resume_lsn,
                branch,
                self.floor.get(),
                self.status_interval,
            )
            .await
            {
                Ok(fresh) => {
                    *feed = fresh;
                    tracing::info!(
                        target: "walshadow",
                        resume_lsn = %resume_lsn,
                        "source reconnected — resuming replication",
                    );
                    return SourcePath::Live;
                }
                Err(retry_error) => retry_error,
            }
        };
        tracing::warn!(
            target: "walshadow",
            error = %reason,
            resume_lsn = %resume_lsn,
            source_missing,
            "source cannot serve the resume point — trying archive",
        );
        // Archive fallback (restore_command analog). Redial covers every "no
        // archive": a transient error retries the source with backoff, a
        // removed-WAL error surfaces the operator-action message.
        let archive_error = match self.backup.map(|s| (s, s.build_storage())) {
            None => "no [backup] archive configured".to_string(),
            Some((_, Err(e))) => format!("build archive storage: {e:#}"),
            Some((settings, Ok(storage))) => {
                tracing::info!(target: "walshadow", resume_lsn = %resume_lsn,
                    prefetch = self.prefetch, "starting archive recovery");
                return SourcePath::Archive(ArchiveFeed::spawn(
                    settings.clone(),
                    storage,
                    branch.timeline,
                    resume_lsn.get(),
                    self.prefetch,
                ));
            }
        };
        SourcePath::Redial(Redial::now(archive_error))
    }

    /// One attempt once `redial` is due. Removed WAL needs an operator; any
    /// other failure backs off for the next iteration. Every attempt goes
    /// through [`resume_source_feed`]'s proofs
    pub(crate) async fn redial(
        &self,
        redial: &mut Redial,
        cfg: &PgConfig,
        slot: Option<&str>,
        branch: SourceBranch,
        resume_lsn: Pos<Floor>,
    ) -> Result<Option<SourceFeed>> {
        if Instant::now() < redial.retry_at {
            return Ok(None);
        }
        match resume_source_feed(
            cfg,
            slot,
            resume_lsn,
            branch,
            self.floor.get(),
            self.status_interval,
        )
        .await
        {
            Ok(feed) => Ok(Some(feed)),
            Err(e) if walshadow::source_feed::is_wal_segment_removed(&e) => {
                Err(e.context(format!(
                    "source cannot serve WAL at {resume_lsn}; {}; \
                     base-backup refresh requires operator action",
                    redial.archive_error,
                )))
            }
            Err(e) => {
                tracing::warn!(
                    target: "walshadow",
                    error = %e,
                    endpoint = %format!("{}:{}", cfg.host, cfg.port),
                    retry_in_ms = redial.backoff.as_millis() as u64,
                    "source reconnect failed — retrying",
                );
                redial.retry_at = Instant::now() + redial.backoff;
                redial.backoff = (redial.backoff * 2).min(Redial::MAX_BACKOFF);
                Ok(None)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Two standbys of one primary, promoted independently, are both timeline 2
    /// under one system identifier. The chain places either one, so only where
    /// the branch begins refuses the wrong one
    #[test]
    fn prove_branch_refuses_a_sibling_sharing_the_branch_number() {
        let ours = TimelineHistory::parse(2, b"1\t0/3000000\tno recovery target\n").unwrap();
        let sibling = TimelineHistory::parse(2, b"1\t0/5000000\tno recovery target\n").unwrap();
        let branch = SourceBranch {
            system_id: 7,
            timeline: 2,
            begin: ours.begin_of(2).unwrap(),
        };
        prove_branch(&ours, branch, 0x400_0000).expect("our own branch");
        let err = prove_branch(&sibling, branch, 0x600_0000).unwrap_err();
        assert_eq!(err.reason(), "sibling_branch", "{err}");
    }

    #[test]
    fn prove_branch_refuses_a_position_past_the_branchs_own_fork() {
        let history = TimelineHistory::parse(3, b"1\t0/3000000\n2\t0/5000000\n").unwrap();
        let branch = SourceBranch {
            system_id: 7,
            timeline: 2,
            begin: 0x300_0000,
        };
        prove_branch(&history, branch, 0x400_0000).expect("still inside timeline 2");
        let err = prove_branch(&history, branch, 0x500_0000).unwrap_err();
        assert_eq!(err.reason(), "resume_past_fork", "{err}");
        let absent = SourceBranch {
            timeline: 9,
            ..branch
        };
        assert_eq!(
            prove_branch(&history, absent, 0x100).unwrap_err().reason(),
            "timeline_not_descendant",
        );
    }

    #[test]
    fn stream_branch_names_the_branch_by_its_switchpoint() {
        let history = TimelineHistory::parse(2, b"1\t0/3000000\tno recovery target\n").unwrap();
        let stream = WalStream::new(2, WAL_SEG_SIZE, Pos::new(0x300_0000)).unwrap();
        assert_eq!(stream_branch(&history, 7, &stream).begin, 0x300_0000);
    }

    #[test]
    fn promotion_gate_defaults_are_not_ready() {
        assert!(!PromotionGate::default().ready);
        assert_eq!(
            PromotionGate::blocked("not_paused").blocked_on,
            "not_paused"
        );
        assert_eq!(
            PromotionGate::unreachable().blocked_on,
            "source_unreachable",
        );
    }

    #[test]
    fn swap_reason_reads_the_refusal_out_of_the_error() {
        let sibling = anyhow::Error::from(TransitionError::SiblingBranch {
            tli: 2,
            stored_begin: 1,
            live_begin: 2,
        });
        assert_eq!(swap_reason(&sibling), "sibling_branch");
        assert_eq!(
            swap_reason(&anyhow::anyhow!("connection refused")),
            "source"
        );
    }
}
