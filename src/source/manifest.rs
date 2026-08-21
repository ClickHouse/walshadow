//! Durable resume manifest. Lives at `{spill_dir}/manifest.toml` so a
//! `mv` of the working dir keeps resume state coherent with spill files.
//!
//! One durable floor, computed in one place: every artifact family
//! (retire ledger, backfill ledger, future descriptor log) prunes
//! against `floor`, and restart resumes at it, so a pruner can never cut
//! above what a crash would replay. Persist is crash-safe via
//! [`crate::fs::write_atomic`]; parse failure = corrupt (no CRC field,
//! rename discipline leaves old-complete or new-complete, never torn).
//!
//! ## Schema
//!
//! ```toml
//! version = 1
//! # resume LSN = decode floor = GC cut; segment-aligned, archive-clamped
//! floor = "0/6A000000"
//!
//! [source]           # identity gate for every spill-dir artifact
//! system_id = 7334001234567890123
//! timeline = 1
//!
//! [lsn]
//! source_received = "0/6A2B3C4D"
//! filter_durable = "0/6A000000"
//! shadow_replay = "0/69FF0120"
//! drain = "0/69FE0000"
//! emitter_ack = "0/69FD8000"
//! shadow_flush = "0/69FC0000"
//! ```
//!
//! ## LSN semantics
//!
//! Six roles, roughly newest→oldest in WAL position:
//!
//! * `source_received`: highest server_wal_end seen on the replication
//!   socket. Bookkeeping only, never gates anything.
//! * `filter_durable`: highest segment-boundary LSN
//!   [`DirSegmentSink`](crate::source::segment_sink::DirSegmentSink) fsynced.
//!   Doubles as standby-status `flush_lsn` advertised to source.
//! * `shadow_replay`: shadow PG's `pg_last_wal_replay_lsn()`
//! * `drain`: highest commit-record LSN drained out of the xact buffer.
//!   Strictly higher than `emitter_ack`.
//! * `emitter_ack`: [`ResumeSafe`], not the live ack behind
//!   `walshadow_emitter_ack_lsn`. Slot-advance ceiling.
//! * `shadow_flush`: min `flush_lsn` from inbound `'r'` standby status
//!   across active shadow streaming connections. On restart, resume
//!   position walsender hands shadow via `START_REPLICATION PHYSICAL
//!   <lsn>`. Bookkeeping-only with no active connections; on-disk
//!   `restore_command` fallback takes over.
//!
//! standby-status `apply_lsn` shipped to source equals
//! `min(shadow_replay, emitter_ack)`: neither side may advance past
//! either replica.

use std::io;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::pos::{
    Drain, FilterDurable, Floor, Pos, ResumeSafe, ShadowFlush, ShadowReplay, SourceReceived,
    Switchpoint,
};
use crate::record::WAL_SEG_SIZE;
use crate::source::wal_stream::WalStream;

pub const MANIFEST_FILENAME: &str = "manifest.toml";

/// Bump on any schema change; boot path rejects mismatched versions.
// v2: descriptor-log-aware builds. Any v1 spill dir predates the log and
// cannot be resumed against (decode would read uncovered intervals); the
// version gate turns that into a deterministic upgrade failure.
pub const MANIFEST_VERSION: u32 = 2;

/// Artifact ownership plus the branch the floor sits on. The system identifier
/// gates every nonvolatile spill-dir artifact: reusing a spill dir against a
/// different cluster must not load foreign resume LSNs, retire oids, or
/// backfill state. Timeline is the selected branch through those artifacts, so
/// it moves with the floor rather than with the source's live head.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceIdentity {
    pub system_id: u64,
    /// Branch owning [`Manifest::floor`]. Restart requests this timeline at
    /// that LSN even when the source reports a newer one; it advances only once
    /// the floor crosses the corresponding fork.
    pub timeline: u32,
    /// Where that branch begins, which is its ancestor's switchpoint (`0` on
    /// the oldest one). A timeline number is not unique across branches — two
    /// standbys of one primary, promoted independently, are both timeline 2 —
    /// so the number alone lets a sibling pass the lineage gate. The
    /// switchpoint is what separates them, and only a stored one carries the
    /// chain a run proved forward to the next boot.
    ///
    /// `0` above timeline 1 reads as unrecorded rather than as a switchpoint:
    /// a live identity off `IDENTIFY_SYSTEM` has parsed no history to place
    /// itself with, and an on-disk manifest may carry no switchpoint at all.
    #[serde(default)]
    pub timeline_begin: Pos<Switchpoint>,
}

/// Branch the pump is reading, which runs ahead of
/// [`SourceIdentity::timeline`] between a fork and the floor crossing it.
/// Diagnostic: resume reads the floor's timeline, never this.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct WalBranch {
    pub stream_timeline: u32,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct LsnSet {
    pub source_received: Pos<SourceReceived>,
    pub filter_durable: Pos<FilterDurable>,
    pub shadow_replay: Pos<ShadowReplay>,
    pub drain: Pos<Drain>,
    /// Resume-safe ack, TOML key retained for compatibility
    pub emitter_ack: Pos<ResumeSafe>,
    pub shadow_flush: Pos<ShadowFlush>,
}

/// Scalars precede tables (TOML emit constraint): `version`/`floor`
/// first, `source`/`lsn` after.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Manifest {
    pub version: u32,
    /// Resume LSN = decode floor = GC cut. Segment-aligned,
    /// archive-clamped at write time via [`resolved_floor`].
    pub floor: Pos<Floor>,
    pub source: SourceIdentity,
    /// Absent in manifests written before timeline crossing landed; those runs
    /// never left the floor's branch, so the floor timeline is also the stream's
    #[serde(default)]
    pub wal: WalBranch,
    pub lsn: LsnSet,
}

#[derive(Debug, Error)]
pub enum ManifestError {
    #[error("io: {0}")]
    Io(#[from] io::Error),
    #[error("manifest parse: {0}")]
    Parse(#[from] toml::de::Error),
    #[error("manifest serialize: {0}")]
    Ser(#[from] toml::ser::Error),
    #[error("unsupported manifest schema version {0} (this build expects {MANIFEST_VERSION})")]
    Version(u32),
    #[error(
        "spill dir belongs to another source: stored system_id={}, \
         live system_id={}; wipe the spill dir for a new source, \
         or point --spill-dir at the old one",
        stored.system_id, live.system_id
    )]
    ForeignSource {
        stored: SourceIdentity,
        live: SourceIdentity,
    },
}

pub fn manifest_path(spill_dir: &Path) -> PathBuf {
    spill_dir.join(MANIFEST_FILENAME)
}

/// One floor, one function: resume LSN = decode floor = GC cut.
///
/// `filter_durable` is the highest fsynced sealed-segment boundary — a
/// crash-durable lower bound on the sealed archive end — so the archive
/// clamp folds in at write time. Restart resumes at this floor and every
/// pruner cuts against it: cut ≤ resume by construction, never by test.
pub fn resolved_floor(
    emitter_ack: impl Into<Pos<ResumeSafe>>,
    filter_durable: impl Into<Pos<FilterDurable>>,
) -> Pos<Floor> {
    Pos::new(
        WalStream::align_down(emitter_ack.into().get(), WAL_SEG_SIZE)
            .min(filter_durable.into().get()),
    )
}

/// Stream-start selection.
///
/// `pinned` (`--start-lsn` / fresh bootstrap) aligns only: operator
/// rewind and bootstrap positions outrank archive continuity. Persisted
/// `floor` wins next (already aligned + archive-clamped; zero = not yet
/// established). Greenfield aligns then clamps to the sealed archive end
/// so shadow's `restore_command` never sees a gap.
///
/// Result may sit below a source slot's `restart_lsn` (floor lags the
/// live ack by up to one status interval); slot errors surface at
/// START_REPLICATION, same exposure as the boot-scan clamp had.
pub fn resolve_start(
    raw_start: impl Into<Pos<Floor>>,
    floor: Option<Pos<Floor>>,
    pinned: bool,
    archive_end: Option<Pos<FilterDurable>>,
) -> Pos<Floor> {
    let aligned = Pos::new(WalStream::align_down(raw_start.into().get(), WAL_SEG_SIZE));
    if pinned {
        return aligned;
    }
    if let Some(f) = floor.filter(|f| !f.is_zero()) {
        return f;
    }
    // Archive clamp becomes restart floor
    archive_end
        .map(Pos::retag)
        .filter(|end| *end < aligned)
        .unwrap_or(aligned)
}

/// Resolve WAL resume LSN, precedence order:
///
///   1. operator `--start-lsn` override (recovery drills rewind here)
///   2. fresh-bootstrap `end_lsn`: shadow catalog at `end_lsn`, WAL
///      before it double-counts
///   3. manifest's last `emitter_ack`: durable CH resume point
///   4. greenfield: source's current write head
///
/// Seed the live pipeline ack from this value, not zero, so the first status
/// write cannot discard persisted resume state before WAL re-read catches up
pub fn resolve_resume_lsn(
    start_lsn: Option<Pos<Floor>>,
    bootstrap_end_lsn: Option<Pos<Floor>>,
    manifest_ack_lsn: Option<Pos<ResumeSafe>>,
    greenfield_head: impl Into<Pos<SourceReceived>>,
) -> Pos<Floor> {
    match (start_lsn, bootstrap_end_lsn, manifest_ack_lsn) {
        (Some(s), _, _) => s,
        (None, Some(l), _) => l,
        (None, None, Some(c)) if !c.is_zero() => c.retag(),
        (None, None, _) => greenfield_head.into().retag(),
    }
}

/// Out-dir trim cut — shadow-recovery domain, distinct from the manifest
/// floor. Keep `retention_bytes` behind replay, never past the last
/// restartpoint REDO (shadow resumes recovery there).
pub fn retention_cutoff(
    shadow_replay: impl Into<Pos<ShadowReplay>>,
    retention_bytes: u64,
    redo: Option<Pos<ShadowReplay>>,
) -> Pos<ShadowReplay> {
    Pos::new(
        shadow_replay
            .into()
            .get()
            .saturating_sub(retention_bytes)
            .min(redo.map_or(u64::MAX, Pos::get)),
    )
}

/// `Ok(None)` for greenfield (no manifest). `Err(ForeignSource)` when the
/// stored system identifier differs from `live`, which is always fatal.
///
/// Timeline is deliberately not gated here: a newer live timeline is a
/// promotion, and whether the stored branch is an ancestor of it is a question
/// for the source's timeline history, not for string equality.
pub async fn load(
    spill_dir: &Path,
    live: &SourceIdentity,
) -> Result<Option<Manifest>, ManifestError> {
    match tokio::fs::read_to_string(manifest_path(spill_dir)).await {
        Ok(text) => {
            let m: Manifest = toml::from_str(&text)?;
            if m.version != MANIFEST_VERSION {
                return Err(ManifestError::Version(m.version));
            }
            if m.source.system_id != live.system_id {
                return Err(ManifestError::ForeignSource {
                    stored: m.source,
                    live: live.clone(),
                });
            }
            Ok(Some(m))
        }
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e.into()),
    }
}

/// Crash-safe persist; `spill_dir` must already exist
/// ([`XactBuffer::new`](crate::xact::xact_buffer::XactBuffer) creates it).
pub async fn write(spill_dir: &Path, m: &Manifest) -> Result<(), ManifestError> {
    let text = toml::to_string(m)?;
    crate::fs::write_atomic(spill_dir, MANIFEST_FILENAME, text.as_bytes()).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    const SEG: u64 = WAL_SEG_SIZE;

    fn ident() -> SourceIdentity {
        SourceIdentity {
            system_id: 7_334_001_234_567_890_123,
            timeline: 1,
            timeline_begin: Pos::ZERO,
        }
    }

    fn sample() -> Manifest {
        Manifest {
            version: MANIFEST_VERSION,
            floor: (0x0123_4564_0000_0000 & !(SEG - 1)).into(),
            source: ident(),
            wal: WalBranch { stream_timeline: 2 },
            lsn: LsnSet {
                source_received: 0x0123_4567_89AB_CDEF.into(),
                filter_durable: 0x0123_4567_0000_0000.into(),
                shadow_replay: 0x0123_4566_0000_0000.into(),
                drain: 0x0123_4565_0000_0000.into(),
                emitter_ack: 0x0123_4564_0000_0000.into(),
                shadow_flush: 0x0123_4563_0000_0000.into(),
            },
        }
    }

    #[test]
    fn toml_round_trips_with_pg_lsn_strings() {
        let m = sample();
        let text = toml::to_string(&m).unwrap();
        assert!(text.contains("floor = \"123"), "pg_lsn text form: {text}");
        assert!(
            text.contains("system_id = 7334001234567890123"),
            "numeric system_id: {text}",
        );
        let got: Manifest = toml::from_str(&text).unwrap();
        assert_eq!(got, m);
    }

    /// Preserve manifest compatibility across typed-position migration
    #[test]
    fn on_disk_toml_is_exact() {
        const EXPECTED: &str = "\
version = 2
floor = \"1234564/0\"

[source]
system_id = 7334001234567890123
timeline = 1
timeline_begin = \"0/0\"

[wal]
stream_timeline = 2

[lsn]
source_received = \"1234567/89ABCDEF\"
filter_durable = \"1234567/0\"
shadow_replay = \"1234566/0\"
drain = \"1234565/0\"
emitter_ack = \"1234564/0\"
shadow_flush = \"1234563/0\"
";
        let text = toml::to_string(&sample()).unwrap();
        assert_eq!(text, EXPECTED);
        assert_eq!(toml::from_str::<Manifest>(EXPECTED).unwrap(), sample());
    }

    #[test]
    fn parse_rejects_garbage_and_bad_lsn() {
        assert!(toml::from_str::<Manifest>("not toml at all [").is_err());
        let text = toml::to_string(&sample()).unwrap();
        let bad = text.replace("shadow_flush = \"123", "shadow_flush = \"xyz");
        assert!(toml::from_str::<Manifest>(&bad).is_err());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn load_rejects_wrong_version() {
        let tmp = tempdir().unwrap();
        let mut m = sample();
        m.version = 999;
        let text = toml::to_string(&m).unwrap();
        std::fs::write(manifest_path(tmp.path()), text).unwrap();
        let err = load(tmp.path(), &ident()).await.unwrap_err();
        assert!(matches!(err, ManifestError::Version(999)));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn load_rejects_foreign_source() {
        let tmp = tempdir().unwrap();
        write(tmp.path(), &sample()).await.unwrap();
        let live = SourceIdentity {
            system_id: 42,
            ..ident()
        };
        let err = load(tmp.path(), &live).await.unwrap_err();
        assert!(matches!(err, ManifestError::ForeignSource { .. }));
    }

    /// A promoted source reports a newer timeline; lineage is proved against
    /// its history, so the load itself must not refuse.
    #[tokio::test(flavor = "current_thread")]
    async fn load_accepts_a_newer_live_timeline() {
        let tmp = tempdir().unwrap();
        write(tmp.path(), &sample()).await.unwrap();
        let live = SourceIdentity {
            timeline: 5,
            ..ident()
        };
        let got = load(tmp.path(), &live).await.unwrap().expect("manifest");
        assert_eq!(got.source.timeline, 1, "floor stays on its own branch");
    }

    /// A manifest written before the sibling check carries no switchpoint for
    /// its branch. Zero reads as unrecorded, which is also the truth on the
    /// oldest branch.
    #[test]
    fn timeline_begin_defaults_when_absent() {
        let text = toml::to_string(&sample()).unwrap();
        let without = text
            .lines()
            .filter(|l| !l.starts_with("timeline_begin"))
            .collect::<Vec<_>>()
            .join("\n");
        let got: Manifest = toml::from_str(&without).expect("parse without timeline_begin");
        assert_eq!(got.source.timeline_begin, Pos::ZERO);
    }

    /// Manifests predating the crossing carry no `[wal]`; the floor timeline
    /// stands alone.
    #[test]
    fn stream_timeline_defaults_when_absent() {
        let text = toml::to_string(&sample()).unwrap();
        let without = text
            .lines()
            .filter(|l| !l.starts_with("[wal]") && !l.starts_with("stream_timeline"))
            .collect::<Vec<_>>()
            .join("\n");
        let got: Manifest = toml::from_str(&without).expect("parse without [wal]");
        assert_eq!(got.wal.stream_timeline, 0);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn load_returns_none_when_absent() {
        let tmp = tempdir().unwrap();
        let got = load(tmp.path(), &ident()).await.unwrap();
        assert!(got.is_none(), "greenfield boot must surface as None");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn write_then_load_round_trips() {
        let tmp = tempdir().unwrap();
        let m = sample();
        write(tmp.path(), &m).await.unwrap();
        assert!(
            !tmp.path().join(format!("{MANIFEST_FILENAME}.tmp")).exists(),
            "rename must clean up the .tmp sidecar",
        );
        let got = load(tmp.path(), &ident())
            .await
            .unwrap()
            .expect("manifest present");
        assert_eq!(got, m);
    }

    #[test]
    fn floor_aligns_ack_down() {
        assert_eq!(resolved_floor(2 * SEG + 123, u64::MAX), 2 * SEG);
    }

    #[test]
    fn floor_clamps_to_durable_archive_end() {
        // PLAN_XACT2 finding 5 core: ack in segment N+2, sealed archive
        // end at N — cut must be N, else restart replays pruned range
        let n = 7 * SEG;
        assert_eq!(resolved_floor(n + 2 * SEG + 55, n), n);
    }

    #[test]
    fn floor_zero_before_first_seal() {
        assert!(resolved_floor(2 * SEG + 1, Pos::ZERO).is_zero());
    }

    #[test]
    fn start_pinned_aligns_only() {
        assert_eq!(
            resolve_start(3 * SEG + 9, Some(SEG.into()), true, Some(SEG.into())),
            3 * SEG,
        );
    }

    #[test]
    fn start_floor_wins_when_nonzero() {
        assert_eq!(
            resolve_start(3 * SEG + 9, Some((2 * SEG).into()), false, None),
            2 * SEG
        );
    }

    #[test]
    fn start_zero_floor_falls_through_to_archive_clamp() {
        assert_eq!(
            resolve_start(3 * SEG + 9, Some(Pos::ZERO), false, Some((2 * SEG).into())),
            2 * SEG,
        );
    }

    #[test]
    fn start_greenfield_aligns_and_clamps() {
        assert_eq!(resolve_start(3 * SEG + 9, None, false, None), 3 * SEG);
        assert_eq!(
            resolve_start(3 * SEG + 9, None, false, Some((4 * SEG).into())),
            3 * SEG
        );
        assert_eq!(
            resolve_start(3 * SEG + 9, None, false, Some(SEG.into())),
            SEG
        );
    }

    #[test]
    fn retention_cutoff_keeps_window_and_redo() {
        assert_eq!(retention_cutoff(10 * SEG, 2 * SEG, None), 8 * SEG);
        assert_eq!(
            retention_cutoff(10 * SEG, 2 * SEG, Some((5 * SEG).into())),
            5 * SEG
        );
        assert!(retention_cutoff(SEG, 2 * SEG, None).is_zero());
    }

    #[test]
    fn resume_lsn_start_override_wins() {
        assert_eq!(
            resolve_resume_lsn(
                Some(0x10.into()),
                Some(0x99.into()),
                Some(0x88.into()),
                0xFF
            ),
            0x10,
        );
    }

    #[test]
    fn resume_lsn_bootstrap_end_outranks_manifest() {
        assert_eq!(
            resolve_resume_lsn(None, Some(0x99.into()), Some(0x88.into()), 0xFF),
            0x99
        );
    }

    #[test]
    fn resume_lsn_resumes_from_manifest_ack_not_greenfield() {
        // Regression: durable-manifest restart must resume from
        // emitter_ack, never fall through to source head (would
        // silently skip [ack, head] WAL)
        let ack = 0xAABB_0000u64;
        let head = 0xFFFF_0000u64;
        let resume = resolve_resume_lsn(None, None, Some(ack.into()), head);
        assert_eq!(resume, ack, "must resume from durable ack");
        assert!(!resume.is_zero(), "ack seed must not regress to 0");
        assert_ne!(resume.get(), head, "must not skip ahead to source head");
    }

    #[test]
    fn resume_lsn_zero_ack_falls_through_to_greenfield() {
        // ack == 0 is greenfield-equivalent: nothing below head to ship
        assert_eq!(resolve_resume_lsn(None, None, Some(Pos::ZERO), 0xFF), 0xFF);
    }

    #[test]
    fn resume_lsn_greenfield_uses_head() {
        assert_eq!(resolve_resume_lsn(None, None, None, 0x4242), 0x4242);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn second_write_overwrites_first() {
        let tmp = tempdir().unwrap();
        let mut m = sample();
        write(tmp.path(), &m).await.unwrap();
        m.lsn.emitter_ack = 0x0DEA_DBEE_F00D_0000.into();
        write(tmp.path(), &m).await.unwrap();
        let got = load(tmp.path(), &ident()).await.unwrap().unwrap();
        assert_eq!(got, m);
    }
}
