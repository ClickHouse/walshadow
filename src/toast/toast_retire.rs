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
//! version = 1
//! system_id = 7334001234567890123
//!
//! [[retire]]
//! toast_relid = 16500
//! commit_lsn = "0/1A2B3C4D"
//! ```
//!
//! Persist is crash-safe via [`crate::fs::write_atomic`]. A corrupt file
//! is an error, never an empty fallback — silently dropping entries
//! reintroduces the mirror leak. So is a file another source system wrote;
//! one predating `system_id` loads once and is rewritten stamped

use std::io;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::pos::{Commit, Floor, Pos};

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
    system_id: Option<u64>,
    #[serde(default)]
    retire: Vec<RetireEntry>,
}

#[derive(Serialize, Deserialize)]
struct RetireEntry {
    toast_relid: u32,
    commit_lsn: Pos<Commit>,
}

/// Pending `(toast_relid, dropping commit_lsn)` retires, persisted on
/// every mutation.
#[derive(Debug)]
pub struct RetireLedger {
    dir: PathBuf,
    system_id: u64,
    entries: Vec<(u32, Pos<Commit>)>,
}

impl RetireLedger {
    /// Absent file is an empty ledger; corrupt is an error (see module
    /// doc).
    pub async fn load(spill_dir: &Path, system_id: u64) -> Result<Self, RetireLedgerError> {
        let mut ledger = Self {
            dir: spill_dir.to_path_buf(),
            system_id,
            entries: Vec::new(),
        };
        let path = ledger_path(spill_dir);
        match tokio::fs::read_to_string(&path).await {
            Ok(text) => {
                let file: RetireFile = toml::from_str(&text)?;
                if file.version != RETIRE_LEDGER_VERSION {
                    return Err(RetireLedgerError::Version(file.version));
                }
                let unstamped = crate::fs::check_source(&path, file.system_id, system_id)?;
                ledger.entries = file
                    .retire
                    .into_iter()
                    .map(|e| (e.toast_relid, e.commit_lsn))
                    .collect();
                if unstamped {
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

    pub fn entries(&self) -> &[(u32, Pos<Commit>)] {
        &self.entries
    }

    /// Entries whose dropping commit precedes `cut` (persisted resolved
    /// floor); snapshot so the caller can await between removals.
    pub fn due(&self, cut: Pos<Floor>) -> Vec<(u32, Pos<Commit>)> {
        self.entries
            .iter()
            .copied()
            .filter(|&(_, commit_lsn)| commit_lsn.retag() < cut)
            .collect()
    }

    /// Append + persist; a replayed drop re-pushes its identical entry,
    /// dedup keeps one.
    pub async fn push(
        &mut self,
        toast_relid: u32,
        commit_lsn: Pos<Commit>,
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
        commit_lsn: Pos<Commit>,
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
            system_id: Some(self.system_id),
            retire: self
                .entries
                .iter()
                .map(|&(toast_relid, commit_lsn)| RetireEntry {
                    toast_relid,
                    commit_lsn,
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

    const SYSID: u64 = 7_300_000_000_000_000_001;

    #[tokio::test(flavor = "current_thread")]
    async fn foreign_source_is_error_and_unstamped_file_upgrades() {
        let tmp = tempdir().unwrap();
        std::fs::write(
            ledger_path(tmp.path()),
            "version = 1\n\n[[retire]]\ntoast_relid = 1\ncommit_lsn = \"0/10\"\n",
        )
        .unwrap();
        let ledger = RetireLedger::load(tmp.path(), SYSID).await.unwrap();
        assert_eq!(ledger.entries(), &[(1, 0x10.into())]);
        let text = std::fs::read_to_string(ledger_path(tmp.path())).unwrap();
        assert!(text.contains(&format!("system_id = {SYSID}")), "{text}");
        let err = RetireLedger::load(tmp.path(), SYSID + 1).await.unwrap_err();
        assert!(matches!(err, RetireLedgerError::Io(_)), "{err:?}");
        assert!(
            err.to_string().contains("belongs to source system"),
            "{err}"
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
        ledger.push(16500, Pos::new(0x1000)).await.unwrap();
        ledger.push(16600, Pos::new(0x2000)).await.unwrap();
        assert!(
            !tmp.path()
                .join(format!("{RETIRE_LEDGER_FILENAME}.tmp"))
                .exists(),
            "rename must clean up the .tmp sidecar",
        );
        let reloaded = RetireLedger::load(tmp.path(), SYSID).await.unwrap();
        assert_eq!(
            reloaded.entries(),
            &[(16500, 0x1000.into()), (16600, 0x2000.into())]
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn push_dedups_replayed_drop() {
        let tmp = tempdir().unwrap();
        let mut ledger = RetireLedger::load(tmp.path(), SYSID).await.unwrap();
        ledger.push(16500, Pos::new(0x1000)).await.unwrap();
        ledger.push(16500, Pos::new(0x1000)).await.unwrap();
        assert_eq!(ledger.entries(), &[(16500, 0x1000.into())]);
        let reloaded = RetireLedger::load(tmp.path(), SYSID).await.unwrap();
        assert_eq!(reloaded.entries(), &[(16500, 0x1000.into())]);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn remove_persists() {
        let tmp = tempdir().unwrap();
        let mut ledger = RetireLedger::load(tmp.path(), SYSID).await.unwrap();
        ledger.push(16500, Pos::new(0x1000)).await.unwrap();
        ledger.push(16600, Pos::new(0x2000)).await.unwrap();
        ledger.remove(16500, Pos::new(0x1000)).await.unwrap();
        let reloaded = RetireLedger::load(tmp.path(), SYSID).await.unwrap();
        assert_eq!(reloaded.entries(), &[(16600, 0x2000.into())]);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn due_filters_below_cut() {
        let tmp = tempdir().unwrap();
        let mut ledger = RetireLedger::load(tmp.path(), SYSID).await.unwrap();
        ledger.push(1, Pos::new(0x1000)).await.unwrap();
        ledger.push(2, Pos::new(0x2000)).await.unwrap();
        ledger.push(3, Pos::new(0x3000)).await.unwrap();
        assert_eq!(ledger.due(Pos::new(0x2000)), [(1, 0x1000.into())]);
        assert_eq!(ledger.due(Pos::new(u64::MAX)).len(), 3);
        assert!(ledger.due(Pos::ZERO).is_empty());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn corrupt_file_is_error_not_empty() {
        let tmp = tempdir().unwrap();
        let mut ledger = RetireLedger::load(tmp.path(), SYSID).await.unwrap();
        ledger.push(16500, Pos::new(0x1000)).await.unwrap();
        std::fs::write(ledger_path(tmp.path()), "version = 1\n[[retire").unwrap();
        let err = RetireLedger::load(tmp.path(), SYSID).await.unwrap_err();
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
        let err = RetireLedger::load(tmp.path(), SYSID).await.unwrap_err();
        assert!(matches!(err, RetireLedgerError::Parse(_)), "{err:?}");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn wrong_version_is_error() {
        let tmp = tempdir().unwrap();
        std::fs::write(ledger_path(tmp.path()), "version = 999\n").unwrap();
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
        ledger.push(16500, Pos::new(n + SEG + 42)).await.unwrap();
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
            vec![(16500, (n + SEG + 42).into())],
        );
    }
}
