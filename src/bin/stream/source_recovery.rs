//! Source reconnect, endpoint swap, and timeline crossing — everything the
//! pump does when the source it was reading stops answering or forks.

use std::path::Path;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use tokio_postgres::types::PgLsn;
use walrus::pg::replication::conn::PgConfig;
use walshadow::archive::{Archive, ArchiveFeed};
use walshadow::config::SourceConn;
use walshadow::manifest;
use walshadow::pos::{Floor, Monotone, Pos};
use walshadow::record::WAL_SEG_SIZE;
use walshadow::source_feed::SourceFeed;
use walshadow::timeline::TimelineHistory;
use walshadow::transition::{TransitionError, source_history};
use walshadow::wal_stream::WalStream;

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
    /// Source lost with no archive to read, redialed each due pump iteration so
    /// the loop keeps publishing, pausing and applying `[source]` repoints
    Redial,
}

impl SourcePath {
    pub(crate) fn archive(&mut self) -> Option<&mut ArchiveFeed> {
        match self {
            Self::Archive(a) => Some(a),
            _ => None,
        }
    }
}

/// Redial pacing, held across archive legs so an archive gap waits out the
/// delay its failed dial set instead of redialing hot
pub(crate) struct ReconnectBackoff {
    retry_at: Instant,
    delay: Duration,
}

impl Default for ReconnectBackoff {
    fn default() -> Self {
        Self {
            retry_at: Instant::now(),
            delay: Self::MIN,
        }
    }
}

impl ReconnectBackoff {
    const MIN: Duration = Duration::from_millis(200);
    const MAX: Duration = Duration::from_secs(10);

    pub(crate) fn due(&self) -> bool {
        Instant::now() >= self.retry_at
    }

    pub(crate) fn reset(&mut self) {
        *self = Self::default();
    }

    fn failed(&mut self) {
        self.retry_at = Instant::now() + self.delay;
        self.delay = (self.delay * 2).min(Self::MAX);
    }
}

pub(crate) struct SourceRecovery<'a> {
    pub(crate) system_id: u64,
    pub(crate) status_interval: Duration,
    pub(crate) backup: Option<&'a Archive>,
    /// Published resume floor, which is what a slot on the far end has to still
    /// reach — the reconnect's own `resume_lsn` sits above it
    pub(crate) floor: &'a Monotone<Floor>,
    pub(crate) prefetch: usize,
    pub(crate) backoff: ReconnectBackoff,
}

impl SourceRecovery<'_> {
    /// Dial source, otherwise start bounded archive fetches for normal pump,
    /// otherwise redial. `source` and `history` are the live endpoint and
    /// proved chain, passed per call rather than held, so a recovery that
    /// starts after a `[source]` reload or a crossing dials the new address
    /// under the new slot and asks for the descendant, with the archive read
    /// under its segment names.
    ///
    /// `lost` is what ended a live feed, starting a fresh outage. A removed-WAL
    /// (58P01) loss means source genuinely can't serve resume point, so skip
    /// straight to archive
    pub(crate) async fn attempt(
        &mut self,
        lost: Option<anyhow::Error>,
        source: &SourceConn,
        history: &TimelineHistory,
        stream: &WalStream,
        feed: &mut SourceFeed,
    ) -> Result<SourcePath> {
        if lost.is_some() {
            self.backoff.reset();
        }
        let resume_lsn = stream.next_lsn();
        // Source first (primary_conninfo analog), plain drop is usually transient
        let error = match lost {
            Some(e) if walshadow::source_feed::is_wal_segment_removed(&e) => e,
            _ => match resume_source_feed(
                &source.to_pg_config(),
                source.slot.as_deref(),
                resume_lsn,
                stream_branch(history, self.system_id, stream),
                self.floor.get(),
                self.status_interval,
            )
            .await
            {
                Ok(fresh) => {
                    *feed = fresh;
                    self.backoff.reset();
                    tracing::info!(
                        target: "walshadow",
                        endpoint = source.endpoint(),
                        resume_lsn = %resume_lsn,
                        "source reconnected — resuming replication",
                    );
                    return Ok(SourcePath::Live);
                }
                Err(e) => e,
            },
        };
        tracing::warn!(
            target: "walshadow",
            error = %format!("{error:#}"),
            endpoint = source.endpoint(),
            resume_lsn = %resume_lsn,
            retry_in_ms = self.backoff.delay.as_millis() as u64,
            "source cannot serve the resume point — trying archive",
        );
        self.fall_back(error, history, stream.timeline(), resume_lsn)
            .await
    }

    /// Archive fallback (restore_command analog). Without an archive, removed
    /// WAL needs an operator, any other failure redials after backoff
    async fn fall_back(
        &mut self,
        error: anyhow::Error,
        history: &TimelineHistory,
        timeline: u32,
        resume_lsn: Pos<Floor>,
    ) -> Result<SourcePath> {
        self.backoff.failed();
        match self.backup {
            Some(archive) => {
                let history = match archive.discover_history(history, resume_lsn.get()).await {
                    Ok(Some(extended)) => {
                        tracing::info!(
                            target: "walshadow",
                            known_timeline = history.target(),
                            archive_timeline = extended.target(),
                            "archive history names branches past the proved chain",
                        );
                        extended
                    }
                    Ok(None) => history.clone(),
                    Err(e) => {
                        tracing::warn!(
                            target: "walshadow",
                            error = %format!("{e:#}"),
                            "archive history unreadable; resolving segments on the proved chain",
                        );
                        history.clone()
                    }
                };
                tracing::info!(target: "walshadow", resume_lsn = %resume_lsn,
                    prefetch = self.prefetch, "starting archive recovery");
                Ok(SourcePath::Archive(archive.feed(
                    history,
                    timeline,
                    resume_lsn.get(),
                    self.prefetch,
                )))
            }
            None if walshadow::source_feed::is_wal_segment_removed(&error) => {
                Err(error.context(format!(
                    "source cannot serve WAL at {resume_lsn}; no [backup] archive configured; \
                     base-backup refresh requires operator action",
                )))
            }
            None => Ok(SourcePath::Redial),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn removed_wal() -> anyhow::Error {
        walshadow::source_feed::WalSegmentRemoved {
            error_code: walshadow::source_feed::SQLSTATE_UNDEFINED_FILE.to_string(),
            message: "requested WAL segment has already been removed".to_string(),
        }
        .into()
    }

    fn slot_too_new() -> anyhow::Error {
        TransitionError::Slot(walshadow::source_feed::SlotError::TooNew {
            slot: "walshadow".to_string(),
            restart_lsn: 0x1_0E00_0490,
            resume_lsn: 0x6200_0000,
        })
        .into()
    }

    fn recovery<'a>(backup: Option<&'a Archive>, floor: &'a Monotone<Floor>) -> SourceRecovery<'a> {
        SourceRecovery {
            system_id: 1,
            status_interval: Duration::from_secs(10),
            backup,
            floor,
            prefetch: 1,
            backoff: ReconnectBackoff::default(),
        }
    }

    #[tokio::test]
    async fn archive_retries_after_slot_refusal_and_delayed_upload() {
        let tmp = tempfile::tempdir().unwrap();
        let settings = walrus::config::Settings {
            storage: walrus::config::StorageSettings::Fs {
                path: tmp.path().join("archive").display().to_string(),
            },
            ..Default::default()
        };
        let floor = Monotone::new(Pos::new(0x6200_0000));
        let archive = Archive::open(settings.clone()).unwrap();
        let mut recovery = recovery(Some(&archive), &floor);
        let history = TimelineHistory::root(1);
        let resume = Pos::new(0x6200_002A);
        for attempt in 0..8 {
            let delay = recovery.backoff.delay;
            let before = Instant::now();
            let error = if attempt % 2 == 0 {
                slot_too_new()
            } else {
                removed_wal()
            };
            let mut path = recovery
                .fall_back(error, &history, 1, resume)
                .await
                .unwrap();
            let error = path.archive().unwrap().next().await.unwrap().unwrap_err();
            assert!(format!("{error:#}").contains("000000010000000000000062"));
            assert!(recovery.backoff.retry_at >= before + delay);
            assert_eq!(
                recovery.backoff.delay,
                (delay * 2).min(ReconnectBackoff::MAX)
            );
        }
        assert_eq!(recovery.backoff.delay, ReconnectBackoff::MAX);

        let segment = tmp.path().join("000000010000000000000062");
        std::fs::write(&segment, vec![0x5a; WAL_SEG_SIZE as usize]).unwrap();
        walrus::pg::wal::push::handle(&settings, settings.build_storage().unwrap(), &segment)
            .await
            .unwrap();
        let mut path = recovery
            .fall_back(slot_too_new(), &history, 1, resume)
            .await
            .unwrap();
        let (lsn, bytes) = path.archive().unwrap().next().await.unwrap().unwrap();
        assert_eq!(lsn, resume.get());
        assert_eq!(bytes.len(), WAL_SEG_SIZE as usize - 42);
        assert!(bytes.iter().all(|b| *b == 0x5a));
        let error = path.archive().unwrap().next().await.unwrap().unwrap_err();
        assert!(format!("{error:#}").contains("000000010000000000000063"));
    }

    #[tokio::test]
    async fn removed_wal_without_archive_requires_operator() {
        let floor = Monotone::new(Pos::new(0x6200_0000));
        let mut recovery = recovery(None, &floor);
        let history = TimelineHistory::root(1);
        let Err(error) = recovery
            .fall_back(removed_wal(), &history, 1, floor.get())
            .await
        else {
            panic!("removed WAL without archive must fail");
        };
        assert!(
            error
                .to_string()
                .contains("base-backup refresh requires operator action")
        );
        assert!(error.to_string().contains("no [backup] archive configured"));
        assert!(walshadow::source_feed::is_wal_segment_removed(&error));
        let path = recovery
            .fall_back(slot_too_new(), &history, 1, floor.get())
            .await
            .unwrap();
        assert!(matches!(path, SourcePath::Redial));
        assert!(!recovery.backoff.due());
    }

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
