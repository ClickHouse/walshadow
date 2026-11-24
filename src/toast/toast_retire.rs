//! Durable queue of deferred toast-mirror retirements, stored at
//! `{spill_dir}/toast_retires.toml` beside `manifest.toml`.
//! Scratch cleanup leaves this queue intact.
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
//! pending cleanup and leave unused mirrors behind

use std::io;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::pos::{Commit, Floor, Pos};

pub const RETIRE_LEDGER_FILENAME: &str = "toast_retires.toml";

/// Bump on any schema change; load rejects mismatched versions.
pub const RETIRE_LEDGER_VERSION: u32 = 2;

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
}

pub fn ledger_path(spill_dir: &Path) -> PathBuf {
    spill_dir.join(RETIRE_LEDGER_FILENAME)
}

#[derive(Serialize, Deserialize)]
struct RetireFile {
    version: u32,
    system_id: u64,
    #[serde(default = "Vec::new")]
    retire: Vec<RetireEntry>,
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
    /// Load pending cleanup; return empty ledger if file is absent, error if corrupt
    pub async fn load(spill_dir: &Path, system_id: u64) -> Result<Self, RetireLedgerError> {
        let path = ledger_path(spill_dir);
        let entries = match tokio::fs::read_to_string(&path).await {
            Ok(text) => {
                let file: RetireFile = toml::from_str(&text)?;
                crate::fs::check_source(&path, Some(file.system_id), system_id)?;
                if file.version != RETIRE_LEDGER_VERSION {
                    return Err(RetireLedgerError::Version(file.version));
                }
                file.retire
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => Vec::new(),
            Err(e) => return Err(e.into()),
        };
        Ok(Self {
            dir: spill_dir.to_path_buf(),
            system_id,
            entries,
        })
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
            system_id: self.system_id,
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
    async fn foreign_or_unstamped_source_is_error() {
        let tmp = tempdir().unwrap();
        let body = "\n[[retire]]\ndb_oid = 5\ntoast_relid = 1\ncommit_lsn = \"0/10\"\n";
        std::fs::write(ledger_path(tmp.path()), format!("version = 2\n{body}")).unwrap();
        let err = RetireLedger::load(tmp.path(), SYSID).await.unwrap_err();
        assert!(matches!(err, RetireLedgerError::Parse(_)), "{err:?}");
        std::fs::write(
            ledger_path(tmp.path()),
            format!("version = 2\nsystem_id = {SYSID}\n{body}"),
        )
        .unwrap();
        let ledger = RetireLedger::load(tmp.path(), SYSID).await.unwrap();
        assert_eq!(ledger.entries(), &[entry(1, 0x10)]);
        let err = RetireLedger::load(tmp.path(), SYSID + 1).await.unwrap_err();
        assert!(matches!(err, RetireLedgerError::Io(_)), "{err:?}");
        assert!(
            err.to_string().contains("belongs to source system"),
            "{err}"
        );
    }

    /// Unreadable ledger surfaces; removing an absent entry is a no-op
    #[tokio::test(flavor = "current_thread")]
    async fn unreadable_ledger_errors_and_absent_remove_noops() {
        let tmp = tempdir().unwrap();
        std::fs::create_dir(ledger_path(tmp.path())).unwrap();
        let err = RetireLedger::load(tmp.path(), SYSID).await.unwrap_err();
        assert!(matches!(err, RetireLedgerError::Io(_)), "{err:?}");
        std::fs::remove_dir(ledger_path(tmp.path())).unwrap();
        let mut ledger = RetireLedger::load(tmp.path(), SYSID).await.unwrap();
        ledger.remove(entry(1, 0x10)).await.unwrap();
        assert!(
            !ledger_path(tmp.path()).exists(),
            "no-op remove writes nothing"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn load_absent_is_empty() {
        let tmp = tempdir().unwrap();
        let ledger = RetireLedger::load(tmp.path(), SYSID).await.unwrap();
        assert!(ledger.is_empty());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn push_persists_and_reloads() {
        let tmp = tempdir().unwrap();
        let mut ledger = RetireLedger::load(tmp.path(), SYSID).await.unwrap();
        ledger.push(entry(16500, 0x1000)).await.unwrap();
        ledger.push(entry(16600, 0x2000)).await.unwrap();
        assert!(
            !tmp.path()
                .join(format!("{RETIRE_LEDGER_FILENAME}.tmp"))
                .exists(),
            "rename must clean up the .tmp sidecar",
        );
        let reloaded = RetireLedger::load(tmp.path(), SYSID).await.unwrap();
        assert_eq!(
            reloaded.entries(),
            &[entry(16500, 0x1000), entry(16600, 0x2000)]
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn push_dedups_replayed_drop() {
        let tmp = tempdir().unwrap();
        let mut ledger = RetireLedger::load(tmp.path(), SYSID).await.unwrap();
        ledger.push(entry(16500, 0x1000)).await.unwrap();
        ledger.push(entry(16500, 0x1000)).await.unwrap();
        assert_eq!(ledger.entries(), &[entry(16500, 0x1000)]);
        let reloaded = RetireLedger::load(tmp.path(), SYSID).await.unwrap();
        assert_eq!(reloaded.entries(), &[entry(16500, 0x1000)]);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn remove_persists() {
        let tmp = tempdir().unwrap();
        let mut ledger = RetireLedger::load(tmp.path(), SYSID).await.unwrap();
        ledger.push(entry(16500, 0x1000)).await.unwrap();
        ledger.push(entry(16600, 0x2000)).await.unwrap();
        ledger.remove(entry(16500, 0x1000)).await.unwrap();
        let reloaded = RetireLedger::load(tmp.path(), SYSID).await.unwrap();
        assert_eq!(reloaded.entries(), &[entry(16600, 0x2000)]);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn due_filters_below_cut() {
        let tmp = tempdir().unwrap();
        let mut ledger = RetireLedger::load(tmp.path(), SYSID).await.unwrap();
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
        let mut ledger = RetireLedger::load(tmp.path(), SYSID).await.unwrap();
        ledger.push(entry(16500, 0x1000)).await.unwrap();
        std::fs::write(ledger_path(tmp.path()), "version = 2\n[[retire").unwrap();
        let err = RetireLedger::load(tmp.path(), SYSID).await.unwrap_err();
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
        let err = RetireLedger::load(tmp.path(), SYSID).await.unwrap_err();
        assert!(matches!(err, RetireLedgerError::Parse(_)), "{err:?}");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn wrong_version_is_error() {
        let tmp = tempdir().unwrap();
        std::fs::write(
            ledger_path(tmp.path()),
            format!("version = 999\nsystem_id = {SYSID}\n"),
        )
        .unwrap();
        let err = RetireLedger::load(tmp.path(), SYSID).await.unwrap_err();
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
        let mut ledger = RetireLedger::load(tmp.path(), SYSID).await.unwrap();
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
