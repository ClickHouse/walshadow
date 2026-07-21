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
//! * `emitter_ack`: highest commit-record LSN durably acked by the
//!   pipeline's contiguous-done watermark. Slot-advance ceiling.
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

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use thiserror::Error;
use walrus::pg::backup::{format_pg_lsn, parse_pg_lsn};

use crate::record::WAL_SEG_SIZE;
use crate::source::wal_stream::WalStream;

pub const MANIFEST_FILENAME: &str = "manifest.toml";

/// Bump on any schema change; boot path rejects mismatched versions.
pub const MANIFEST_VERSION: u32 = 1;

/// LSN persisted in postgres `pg_lsn` text form (`1A/2B3C4D5E`).
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Lsn(pub u64);

impl Serialize for Lsn {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.collect_str(&format_pg_lsn(self.0))
    }
}

impl<'de> Deserialize<'de> for Lsn {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let s = String::deserialize(d)?;
        parse_pg_lsn(&s).map(Lsn).map_err(serde::de::Error::custom)
    }
}

/// IDENTIFY_SYSTEM identity. Gates every nonvolatile spill-dir artifact:
/// reusing a spill dir against a different cluster must not load foreign
/// resume LSNs, retire oids, or backfill state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceIdentity {
    pub system_id: u64,
    pub timeline: u32,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct LsnSet {
    pub source_received: Lsn,
    pub filter_durable: Lsn,
    pub shadow_replay: Lsn,
    pub drain: Lsn,
    pub emitter_ack: Lsn,
    pub shadow_flush: Lsn,
}

/// Scalars precede tables (TOML emit constraint): `version`/`floor`
/// first, `source`/`lsn` after.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Manifest {
    pub version: u32,
    /// Resume LSN = decode floor = GC cut. Segment-aligned,
    /// archive-clamped at write time via [`resolved_floor`].
    pub floor: Lsn,
    pub source: SourceIdentity,
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
        "spill dir belongs to another source: stored system_id={} timeline={}, \
         live system_id={} timeline={}; wipe the spill dir for a new source, \
         or point --spill-dir at the old one",
        stored.system_id, stored.timeline, live.system_id, live.timeline
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
pub fn resolved_floor(emitter_ack: u64, filter_durable: u64) -> u64 {
    WalStream::align_down(emitter_ack, WAL_SEG_SIZE).min(filter_durable)
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
    raw_start: u64,
    floor: Option<u64>,
    pinned: bool,
    archive_end: Option<u64>,
) -> u64 {
    let aligned = WalStream::align_down(raw_start, WAL_SEG_SIZE);
    if pinned {
        return aligned;
    }
    if let Some(f) = floor.filter(|f| *f != 0) {
        return f;
    }
    match archive_end {
        Some(end) if end < aligned => end,
        _ => aligned,
    }
}

/// Resolve WAL resume LSN, precedence order:
///
///   1. operator `--start-lsn` override (recovery drills rewind here)
///   2. fresh-bootstrap `end_lsn`: shadow catalog at `end_lsn`, WAL
///      before it double-counts
///   3. manifest's last `emitter_ack`: durable CH resume point
///   4. greenfield: source's current write head
///
/// Pipeline ack atomic MUST seed from this SAME value, not 0: status
/// loop persists atomic into the manifest's `emitter_ack` every interval
/// with no monotonic guard, first write fires at boot before any
/// re-read acks. Seeding 0 clobbers a resumed manifest's ack to 0; a
/// crash before re-read of `[aligned, resume]` then falls through to
/// case 4 next boot (zero ack skipped), silently dropping `[resume,
/// head]` WAL that never reached CH.
pub fn resolve_resume_lsn(
    start_lsn: Option<u64>,
    bootstrap_end_lsn: Option<u64>,
    manifest_ack_lsn: Option<u64>,
    greenfield_head: u64,
) -> u64 {
    match (start_lsn, bootstrap_end_lsn, manifest_ack_lsn) {
        (Some(s), _, _) => s,
        (None, Some(l), _) => l,
        (None, None, Some(c)) if c != 0 => c,
        (None, None, _) => greenfield_head,
    }
}

/// Out-dir trim cut — shadow-recovery domain, distinct from the manifest
/// floor. Keep `retention_bytes` behind replay, never past the last
/// restartpoint REDO (shadow resumes recovery there).
pub fn retention_cutoff(shadow_replay: u64, retention_bytes: u64, redo: Option<u64>) -> u64 {
    shadow_replay
        .saturating_sub(retention_bytes)
        .min(redo.unwrap_or(u64::MAX))
}

/// `Ok(None)` for greenfield (no manifest). `Err(ForeignSource)` when
/// the stored identity differs from `live` — caller decides fatality
/// (`--ignore-cursor` may adopt a timeline-only change).
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
            if m.source != *live {
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
        }
    }

    fn sample() -> Manifest {
        Manifest {
            version: MANIFEST_VERSION,
            floor: Lsn(0x0123_4564_0000_0000 & !(SEG - 1)),
            source: ident(),
            lsn: LsnSet {
                source_received: Lsn(0x0123_4567_89AB_CDEF),
                filter_durable: Lsn(0x0123_4567_0000_0000),
                shadow_replay: Lsn(0x0123_4566_0000_0000),
                drain: Lsn(0x0123_4565_0000_0000),
                emitter_ack: Lsn(0x0123_4564_0000_0000),
                shadow_flush: Lsn(0x0123_4563_0000_0000),
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
            timeline: 1,
        };
        let err = load(tmp.path(), &live).await.unwrap_err();
        assert!(matches!(err, ManifestError::ForeignSource { .. }));
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
        assert_eq!(resolved_floor(2 * SEG + 1, 0), 0);
    }

    #[test]
    fn start_pinned_aligns_only() {
        assert_eq!(
            resolve_start(3 * SEG + 9, Some(SEG), true, Some(SEG)),
            3 * SEG,
        );
    }

    #[test]
    fn start_floor_wins_when_nonzero() {
        assert_eq!(
            resolve_start(3 * SEG + 9, Some(2 * SEG), false, None),
            2 * SEG
        );
    }

    #[test]
    fn start_zero_floor_falls_through_to_archive_clamp() {
        assert_eq!(
            resolve_start(3 * SEG + 9, Some(0), false, Some(2 * SEG)),
            2 * SEG,
        );
    }

    #[test]
    fn start_greenfield_aligns_and_clamps() {
        assert_eq!(resolve_start(3 * SEG + 9, None, false, None), 3 * SEG);
        assert_eq!(
            resolve_start(3 * SEG + 9, None, false, Some(4 * SEG)),
            3 * SEG
        );
        assert_eq!(resolve_start(3 * SEG + 9, None, false, Some(SEG)), SEG);
    }

    #[test]
    fn retention_cutoff_keeps_window_and_redo() {
        assert_eq!(retention_cutoff(10 * SEG, 2 * SEG, None), 8 * SEG);
        assert_eq!(retention_cutoff(10 * SEG, 2 * SEG, Some(5 * SEG)), 5 * SEG);
        assert_eq!(retention_cutoff(SEG, 2 * SEG, None), 0);
    }

    #[test]
    fn resume_lsn_start_override_wins() {
        assert_eq!(
            resolve_resume_lsn(Some(0x10), Some(0x99), Some(0x88), 0xFF),
            0x10,
        );
    }

    #[test]
    fn resume_lsn_bootstrap_end_outranks_manifest() {
        assert_eq!(resolve_resume_lsn(None, Some(0x99), Some(0x88), 0xFF), 0x99);
    }

    #[test]
    fn resume_lsn_resumes_from_manifest_ack_not_greenfield() {
        // Regression: durable-manifest restart must resume from
        // emitter_ack, never fall through to source head (would
        // silently skip [ack, head] WAL)
        let ack = 0xAABB_0000u64;
        let head = 0xFFFF_0000u64;
        let resume = resolve_resume_lsn(None, None, Some(ack), head);
        assert_eq!(resume, ack, "must resume from durable ack");
        assert_ne!(resume, 0, "ack seed must not regress to 0");
        assert_ne!(resume, head, "must not skip ahead to source head");
    }

    #[test]
    fn resume_lsn_zero_ack_falls_through_to_greenfield() {
        // ack == 0 is greenfield-equivalent: nothing below head to ship
        assert_eq!(resolve_resume_lsn(None, None, Some(0), 0xFF), 0xFF);
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
        m.lsn.emitter_ack = Lsn(0x0DEA_DBEE_F00D_0000);
        write(tmp.path(), &m).await.unwrap();
        let got = load(tmp.path(), &ident()).await.unwrap().unwrap();
        assert_eq!(got, m);
    }
}
