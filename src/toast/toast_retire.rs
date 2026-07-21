//! Durable queue of deferred toast-mirror retirements. Lives at
//! `{spill_dir}/toast_retires.toml` beside `manifest.toml` (survives
//! `clear_spill_dir`, which removes only `xid-*.bin`).
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
//! version = 1
//!
//! [[retire]]
//! toast_relid = 16500
//! commit_lsn = "0/1A2B3C4D"
//! ```
//!
//! Persist is crash-safe via [`crate::fs::write_atomic`]. A corrupt file
//! is an error, never an empty fallback — silently dropping entries
//! reintroduces the mirror leak.

use std::io;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::source::manifest::Lsn;

pub const RETIRE_LEDGER_FILENAME: &str = "toast_retires.toml";

/// Bump on any schema change; load rejects mismatched versions.
pub const RETIRE_LEDGER_VERSION: u32 = 1;

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
    #[serde(default)]
    retire: Vec<RetireEntry>,
}

#[derive(Serialize, Deserialize)]
struct RetireEntry {
    toast_relid: u32,
    commit_lsn: Lsn,
}

/// Pending `(toast_relid, dropping commit_lsn)` retires, persisted on
/// every mutation.
#[derive(Debug)]
pub struct RetireLedger {
    dir: PathBuf,
    entries: Vec<(u32, u64)>,
}

impl RetireLedger {
    /// Absent file is an empty ledger; corrupt is an error (see module
    /// doc).
    pub async fn load(spill_dir: &Path) -> Result<Self, RetireLedgerError> {
        let mut ledger = Self {
            dir: spill_dir.to_path_buf(),
            entries: Vec::new(),
        };
        match tokio::fs::read_to_string(ledger_path(spill_dir)).await {
            Ok(text) => {
                let file: RetireFile = toml::from_str(&text)?;
                if file.version != RETIRE_LEDGER_VERSION {
                    return Err(RetireLedgerError::Version(file.version));
                }
                ledger.entries = file
                    .retire
                    .into_iter()
                    .map(|e| (e.toast_relid, e.commit_lsn.0))
                    .collect();
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(e.into()),
        }
        Ok(ledger)
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn entries(&self) -> &[(u32, u64)] {
        &self.entries
    }

    /// Entries whose dropping commit precedes `cut` (persisted resolved
    /// floor); snapshot so the caller can await between removals.
    pub fn due(&self, cut: u64) -> Vec<(u32, u64)> {
        self.entries
            .iter()
            .copied()
            .filter(|&(_, commit_lsn)| commit_lsn < cut)
            .collect()
    }

    /// Append + persist; a replayed drop re-pushes its identical entry,
    /// dedup keeps one.
    pub async fn push(
        &mut self,
        toast_relid: u32,
        commit_lsn: u64,
    ) -> Result<(), RetireLedgerError> {
        if self.entries.contains(&(toast_relid, commit_lsn)) {
            return Ok(());
        }
        self.entries.push((toast_relid, commit_lsn));
        self.persist().await
    }

    /// Drop entry + persist after its mirror wipe.
    pub async fn remove(
        &mut self,
        toast_relid: u32,
        commit_lsn: u64,
    ) -> Result<(), RetireLedgerError> {
        let before = self.entries.len();
        self.entries.retain(|&e| e != (toast_relid, commit_lsn));
        if self.entries.len() == before {
            return Ok(());
        }
        self.persist().await
    }

    async fn persist(&self) -> Result<(), RetireLedgerError> {
        let file = RetireFile {
            version: RETIRE_LEDGER_VERSION,
            retire: self
                .entries
                .iter()
                .map(|&(toast_relid, commit_lsn)| RetireEntry {
                    toast_relid,
                    commit_lsn: Lsn(commit_lsn),
                })
                .collect(),
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

    #[tokio::test(flavor = "current_thread")]
    async fn load_absent_is_empty() {
        let tmp = tempdir().unwrap();
        let ledger = RetireLedger::load(tmp.path()).await.unwrap();
        assert!(ledger.is_empty());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn push_persists_and_reloads() {
        let tmp = tempdir().unwrap();
        let mut ledger = RetireLedger::load(tmp.path()).await.unwrap();
        ledger.push(16500, 0x1000).await.unwrap();
        ledger.push(16600, 0x2000).await.unwrap();
        assert!(
            !tmp.path()
                .join(format!("{RETIRE_LEDGER_FILENAME}.tmp"))
                .exists(),
            "rename must clean up the .tmp sidecar",
        );
        let reloaded = RetireLedger::load(tmp.path()).await.unwrap();
        assert_eq!(reloaded.entries(), &[(16500, 0x1000), (16600, 0x2000)]);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn push_dedups_replayed_drop() {
        let tmp = tempdir().unwrap();
        let mut ledger = RetireLedger::load(tmp.path()).await.unwrap();
        ledger.push(16500, 0x1000).await.unwrap();
        ledger.push(16500, 0x1000).await.unwrap();
        assert_eq!(ledger.entries(), &[(16500, 0x1000)]);
        let reloaded = RetireLedger::load(tmp.path()).await.unwrap();
        assert_eq!(reloaded.entries(), &[(16500, 0x1000)]);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn remove_persists() {
        let tmp = tempdir().unwrap();
        let mut ledger = RetireLedger::load(tmp.path()).await.unwrap();
        ledger.push(16500, 0x1000).await.unwrap();
        ledger.push(16600, 0x2000).await.unwrap();
        ledger.remove(16500, 0x1000).await.unwrap();
        let reloaded = RetireLedger::load(tmp.path()).await.unwrap();
        assert_eq!(reloaded.entries(), &[(16600, 0x2000)]);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn due_filters_below_cut() {
        let tmp = tempdir().unwrap();
        let mut ledger = RetireLedger::load(tmp.path()).await.unwrap();
        ledger.push(1, 0x1000).await.unwrap();
        ledger.push(2, 0x2000).await.unwrap();
        ledger.push(3, 0x3000).await.unwrap();
        assert_eq!(ledger.due(0x2000), vec![(1, 0x1000)]);
        assert_eq!(ledger.due(u64::MAX).len(), 3);
        assert!(ledger.due(0).is_empty());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn corrupt_file_is_error_not_empty() {
        let tmp = tempdir().unwrap();
        let mut ledger = RetireLedger::load(tmp.path()).await.unwrap();
        ledger.push(16500, 0x1000).await.unwrap();
        std::fs::write(ledger_path(tmp.path()), "version = 1\n[[retire").unwrap();
        let err = RetireLedger::load(tmp.path()).await.unwrap_err();
        assert!(matches!(err, RetireLedgerError::Parse(_)), "{err:?}");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn bad_lsn_is_error() {
        let tmp = tempdir().unwrap();
        std::fs::write(
            ledger_path(tmp.path()),
            "version = 1\n\n[[retire]]\ntoast_relid = 1\ncommit_lsn = \"nope\"\n",
        )
        .unwrap();
        let err = RetireLedger::load(tmp.path()).await.unwrap_err();
        assert!(matches!(err, RetireLedgerError::Parse(_)), "{err:?}");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn wrong_version_is_error() {
        let tmp = tempdir().unwrap();
        std::fs::write(ledger_path(tmp.path()), "version = 999\n").unwrap();
        let err = RetireLedger::load(tmp.path()).await.unwrap_err();
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
        let mut ledger = RetireLedger::load(tmp.path()).await.unwrap();
        ledger.push(16500, n + SEG + 42).await.unwrap();
        assert!(
            ledger.due(resolved_floor(n + 2 * SEG + 5, n)).is_empty(),
            "archive lag must defer the retire",
        );
        assert_eq!(
            ledger.due(resolved_floor(n + 2 * SEG + 5, n + 2 * SEG)),
            vec![(16500, n + SEG + 42)],
        );
    }
}
