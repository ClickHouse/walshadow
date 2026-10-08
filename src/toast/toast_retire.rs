//! Durable queue of deferred toast-mirror retirements. Lives at
//! `{spill_dir}/toast_retires.toml` beside `manifest.toml` (survives
//! `clear_spill_dir`, which wipes only `scratch/`).
//!
//! A toast rel's `Dropped` only queues its retire; the wipe defers until
//! the persisted resolved floor passes the dropping commit. The floor
//! advances independently of the flush, so a stop after the floor passes
//! the drop but before a later commit flushes leaves this ledger as the
//! only route to the wipe — resume never replays the drop.
//!
//! Entries persist at enqueue, inside the dropping xact's barrier apply —
//! strictly before its commit publishes to the ack collector, so any
//! manifest whose floor passed the drop was written after the entry was
//! durable. Removal persists after the wipe; a crash between the two
//! re-runs an idempotent `TRUNCATE` on the already-empty mirror. A
//! replayed drop re-pushes an identical entry; dedup keeps one.
//!
//! ## Schema
//!
//! ```toml
//! version = 2
//! system_id = 7334001234567890123
//!
//! [[retire]]
//! db_oid = 16384
//! toast_relid = 16500
//! commit_lsn = "0/1A2B3C4D"
//! ```
//!
//! Write atomically with [`crate::fs::write_atomic`]. Reject corrupt files
//! and files from another source system. Treating them as empty would lose
//! pending cleanup and leave unused mirrors behind. Rewrite older files
//! without `system_id` to include it. Version 1 entries also lack database
//! OIDs; assign them to sole followed database and rewrite file, matching
//! [`crate::toast::adopt_legacy_mirrors`]

use std::io;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::pos::{Commit, Floor, Pos};

pub const RETIRE_LEDGER_FILENAME: &str = "toast_retires.toml";

/// Bump on any schema change; load rejects mismatched versions.
pub const RETIRE_LEDGER_VERSION: u32 = 2;

/// Identify older entries that lack database OIDs
// TODO(0.2.0): remove with legacy loading after 0.1.x ledgers migrate
const LEGACY_LEDGER_VERSION: u32 = 1;

#[derive(Debug, Error)]
pub enum RetireLedgerError {
    #[error("io: {0}")]
    Io(#[from] io::Error),
    #[error("ledger parse: {0}")]
    Parse(#[from] toml::de::Error),
    #[error("ledger serialize: {0}")]
    Ser(#[from] toml::ser::Error),
    #[error("unsupported ledger schema version {0} (this build expects {RETIRE_LEDGER_VERSION})")]
    Version(u32),
    #[error(
        "version {LEGACY_LEDGER_VERSION} ledger lacks source database OIDs; start once \
         following only its original source database"
    )]
    LegacyOwner,
}

pub fn ledger_path(spill_dir: &Path) -> PathBuf {
    spill_dir.join(RETIRE_LEDGER_FILENAME)
}

#[derive(Deserialize)]
struct LegacyEntry {
    toast_relid: u32,
    commit_lsn: Pos<Commit>,
}

#[derive(Serialize, Deserialize)]
struct RetireFile<E = RetireEntry> {
    version: u32,
    #[serde(default)]
    system_id: Option<u64>,
    #[serde(default = "Vec::new")]
    retire: Vec<E>,
}

/// Record source TOAST relation dropped at `commit_lsn` for later mirror cleanup
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetireEntry {
    pub db_oid: u32,
    pub toast_relid: u32,
    pub commit_lsn: Pos<Commit>,
}

/// Track pending mirror cleanup and save every change
#[derive(Debug)]
pub struct RetireLedger {
    dir: PathBuf,
    system_id: u64,
    entries: Vec<RetireEntry>,
}

impl RetireLedger {
    /// Load pending cleanup; return empty ledger if file is absent, error if corrupt.
    /// Use `legacy_owner` as database OID for older entries when following one database
    pub async fn load(
        spill_dir: &Path,
        system_id: u64,
        legacy_owner: Option<u32>,
    ) -> Result<Self, RetireLedgerError> {
        let mut ledger = Self {
            dir: spill_dir.to_path_buf(),
            system_id,
            entries: Vec::new(),
        };
        let path = ledger_path(spill_dir);
        match tokio::fs::read_to_string(&path).await {
            Ok(text) => {
                let file: RetireFile<toml::Value> = toml::from_str(&text)?;
                let unstamped = crate::fs::check_source(&path, file.system_id, system_id)?;
                ledger.entries = match file.version {
                    RETIRE_LEDGER_VERSION => file
                        .retire
                        .into_iter()
                        .map(toml::Value::try_into)
                        .collect::<Result<_, _>>()?,
                    LEGACY_LEDGER_VERSION => file
                        .retire
                        .into_iter()
                        .map(|v| {
                            let e: LegacyEntry = v.try_into()?;
                            Ok(RetireEntry {
                                db_oid: legacy_owner.ok_or(RetireLedgerError::LegacyOwner)?,
                                toast_relid: e.toast_relid,
                                commit_lsn: e.commit_lsn,
                            })
                        })
                        .collect::<Result<_, RetireLedgerError>>()?,
                    v => return Err(RetireLedgerError::Version(v)),
                };
                if unstamped || file.version == LEGACY_LEDGER_VERSION {
                    ledger.persist().await?;
                }
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(e.into()),
        }
        Ok(ledger)
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn entries(&self) -> &[RetireEntry] {
        &self.entries
    }

    /// Entries whose dropping commit precedes `cut` (persisted resolved
    /// floor); snapshot so the caller can await between removals.
    pub fn due(&self, cut: Pos<Floor>) -> Vec<RetireEntry> {
        self.entries
            .iter()
            .copied()
            // Restart resumes at the floor, so a commit below it never replays
            .filter(|e| e.commit_lsn.retag() < cut)
            .collect()
    }

    /// Append + persist; a replayed drop re-pushes its identical entry,
    /// dedup keeps one.
    pub async fn push(&mut self, entry: RetireEntry) -> Result<(), RetireLedgerError> {
        if self.entries.contains(&entry) {
            return Ok(());
        }
        self.entries.push(entry);
        self.persist().await
    }

    /// Drop entry + persist after its mirror wipe.
    pub async fn remove(&mut self, entry: RetireEntry) -> Result<(), RetireLedgerError> {
        let before = self.entries.len();
        self.entries.retain(|&e| e != entry);
        if self.entries.len() == before {
            return Ok(());
        }
        self.persist().await
    }

    async fn persist(&self) -> Result<(), RetireLedgerError> {
        let file = RetireFile {
            version: RETIRE_LEDGER_VERSION,
            system_id: Some(self.system_id),
            retire: self.entries.clone(),
        };
        let text = toml::to_string(&file)?;
        crate::fs::write_atomic(&self.dir, RETIRE_LEDGER_FILENAME, text.as_bytes()).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    const SYSID: u64 = 7_300_000_000_000_000_001;

    fn entry(toast_relid: u32, commit_lsn: u64) -> RetireEntry {
        RetireEntry {
            db_oid: 5,
            toast_relid,
            commit_lsn: Pos::new(commit_lsn),
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn foreign_source_is_error_and_unstamped_file_upgrades() {
        let tmp = tempdir().unwrap();
        std::fs::write(
            ledger_path(tmp.path()),
            "version = 2\n\n[[retire]]\ndb_oid = 5\ntoast_relid = 1\ncommit_lsn = \"0/10\"\n",
        )
        .unwrap();
        let ledger = RetireLedger::load(tmp.path(), SYSID, None).await.unwrap();
        assert_eq!(ledger.entries(), &[entry(1, 0x10)]);
        let text = std::fs::read_to_string(ledger_path(tmp.path())).unwrap();
        assert!(text.contains(&format!("system_id = {SYSID}")), "{text}");
        let err = RetireLedger::load(tmp.path(), SYSID + 1, None)
            .await
            .unwrap_err();
        assert!(matches!(err, RetireLedgerError::Io(_)), "{err:?}");
        assert!(
            err.to_string().contains("belongs to source system"),
            "{err}"
        );
    }

    /// Require one followed database to migrate nonempty version 1 ledgers
    #[tokio::test(flavor = "current_thread")]
    async fn legacy_entries_take_the_sole_database() {
        let tmp = tempdir().unwrap();
        let legacy = format!(
            "version = 1\nsystem_id = {SYSID}\n\n[[retire]]\ntoast_relid = 1\n\
             commit_lsn = \"0/10\"\n"
        );
        std::fs::write(ledger_path(tmp.path()), &legacy).unwrap();
        let err = RetireLedger::load(tmp.path(), SYSID, None)
            .await
            .unwrap_err();
        assert!(matches!(err, RetireLedgerError::LegacyOwner), "{err:?}");
        let ledger = RetireLedger::load(tmp.path(), SYSID, Some(5))
            .await
            .unwrap();
        assert_eq!(ledger.entries(), &[entry(1, 0x10)]);
        let reloaded = RetireLedger::load(tmp.path(), SYSID, None).await.unwrap();
        assert_eq!(reloaded.entries(), &[entry(1, 0x10)], "rewritten as v2");
        std::fs::write(
            ledger_path(tmp.path()),
            format!("version = 1\nsystem_id = {SYSID}\n"),
        )
        .unwrap();
        assert!(
            RetireLedger::load(tmp.path(), SYSID, None)
                .await
                .unwrap()
                .is_empty(),
            "empty legacy ledger needs no owner",
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn load_absent_is_empty() {
        let tmp = tempdir().unwrap();
        let ledger = RetireLedger::load(tmp.path(), SYSID, None).await.unwrap();
        assert!(ledger.is_empty());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn push_persists_and_reloads() {
        let tmp = tempdir().unwrap();
        let mut ledger = RetireLedger::load(tmp.path(), SYSID, None).await.unwrap();
        ledger.push(entry(16500, 0x1000)).await.unwrap();
        ledger.push(entry(16600, 0x2000)).await.unwrap();
        assert!(
            !tmp.path()
                .join(format!("{RETIRE_LEDGER_FILENAME}.tmp"))
                .exists(),
            "rename must clean up the .tmp sidecar",
        );
        let reloaded = RetireLedger::load(tmp.path(), SYSID, None).await.unwrap();
        assert_eq!(
            reloaded.entries(),
            &[entry(16500, 0x1000), entry(16600, 0x2000)]
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn push_dedups_replayed_drop() {
        let tmp = tempdir().unwrap();
        let mut ledger = RetireLedger::load(tmp.path(), SYSID, None).await.unwrap();
        ledger.push(entry(16500, 0x1000)).await.unwrap();
        ledger.push(entry(16500, 0x1000)).await.unwrap();
        assert_eq!(ledger.entries(), &[entry(16500, 0x1000)]);
        let reloaded = RetireLedger::load(tmp.path(), SYSID, None).await.unwrap();
        assert_eq!(reloaded.entries(), &[entry(16500, 0x1000)]);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn remove_persists() {
        let tmp = tempdir().unwrap();
        let mut ledger = RetireLedger::load(tmp.path(), SYSID, None).await.unwrap();
        ledger.push(entry(16500, 0x1000)).await.unwrap();
        ledger.push(entry(16600, 0x2000)).await.unwrap();
        ledger.remove(entry(16500, 0x1000)).await.unwrap();
        let reloaded = RetireLedger::load(tmp.path(), SYSID, None).await.unwrap();
        assert_eq!(reloaded.entries(), &[entry(16600, 0x2000)]);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn due_filters_below_cut() {
        let tmp = tempdir().unwrap();
        let mut ledger = RetireLedger::load(tmp.path(), SYSID, None).await.unwrap();
        ledger.push(entry(1, 0x1000)).await.unwrap();
        ledger.push(entry(2, 0x2000)).await.unwrap();
        ledger.push(entry(3, 0x3000)).await.unwrap();
        assert_eq!(ledger.due(Pos::new(0x2000)), [entry(1, 0x1000)]);
        assert_eq!(ledger.due(Pos::new(u64::MAX)).len(), 3);
        assert!(ledger.due(Pos::ZERO).is_empty());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn corrupt_file_is_error_not_empty() {
        let tmp = tempdir().unwrap();
        let mut ledger = RetireLedger::load(tmp.path(), SYSID, None).await.unwrap();
        ledger.push(entry(16500, 0x1000)).await.unwrap();
        std::fs::write(ledger_path(tmp.path()), "version = 2\n[[retire").unwrap();
        let err = RetireLedger::load(tmp.path(), SYSID, None)
            .await
            .unwrap_err();
        assert!(matches!(err, RetireLedgerError::Parse(_)), "{err:?}");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn bad_lsn_is_error() {
        let tmp = tempdir().unwrap();
        std::fs::write(
            ledger_path(tmp.path()),
            "version = 2\n\n[[retire]]\ndb_oid = 5\ntoast_relid = 1\ncommit_lsn = \"nope\"\n",
        )
        .unwrap();
        let err = RetireLedger::load(tmp.path(), SYSID, None)
            .await
            .unwrap_err();
        assert!(matches!(err, RetireLedgerError::Parse(_)), "{err:?}");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn wrong_version_is_error() {
        let tmp = tempdir().unwrap();
        std::fs::write(ledger_path(tmp.path()), "version = 999\n").unwrap();
        let err = RetireLedger::load(tmp.path(), SYSID, None)
            .await
            .unwrap_err();
        assert!(matches!(err, RetireLedgerError::Version(999)), "{err:?}");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn archive_lag_defers_due_retire() {
        // PLAN_XACT2 finding 5 composition: ack in segment N+2, sealed
        // archive end N, drop commit in N+1 — resolved floor clamps to N,
        // entry stays; archive catching up past N+1 releases it
        use crate::record::WAL_SEG_SIZE as SEG;
        use crate::source::manifest::resolved_floor;
        let n = 7 * SEG;
        let tmp = tempdir().unwrap();
        let mut ledger = RetireLedger::load(tmp.path(), SYSID, None).await.unwrap();
        ledger.push(entry(16500, n + SEG + 42)).await.unwrap();
        assert!(
            ledger
                .due(resolved_floor(Pos::new(n + 2 * SEG + 5), Pos::new(n)))
                .is_empty(),
            "archive lag must defer the retire",
        );
        assert_eq!(
            ledger.due(resolved_floor(
                Pos::new(n + 2 * SEG + 5),
                Pos::new(n + 2 * SEG)
            )),
            vec![entry(16500, n + SEG + 42)],
        );
    }
}
