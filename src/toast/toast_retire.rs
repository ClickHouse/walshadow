//! Durable queue of deferred toast-mirror retirements. Lives at
//! `{spill_dir}/toast_retires.bin` beside `cursor.bin` (survives
//! `clear_spill_dir`, which removes only `xid-*.bin`).
//!
//! A toast rel's `Dropped` only queues its retire; the wipe defers until
//! the persisted resume floor's segment passes the dropping commit.
//! The floor advances independently of the flush, so a stop after the
//! cursor passes the drop but before a later commit flushes leaves this
//! ledger as the only route to the wipe — resume never replays the drop.
//!
//! Entries persist at enqueue, inside the dropping xact's barrier apply —
//! strictly before its commit publishes to the ack collector, so any
//! cursor whose floor passed the drop was written after the entry was
//! durable. Removal persists after the wipe; a crash between the two
//! re-runs an idempotent `TRUNCATE` on the already-empty mirror. A
//! replayed drop re-pushes an identical entry; dedup keeps one.
//!
//! ## Schema
//!
//! `MAGIC (8B) | version u32 LE | count u32 LE |
//!  count × (toast_relid u32 LE | commit_lsn u64 LE) | crc32c u32 LE`
//!
//! CRC32C covers every preceding byte. Persist is crash-safe: write+fsync
//! `.tmp`, rename, fsync dir. A corrupt file is an error, never an empty
//! fallback — silently dropping entries reintroduces the mirror leak.

use std::io;
use std::path::{Path, PathBuf};

use thiserror::Error;
use tokio::fs::OpenOptions;
use tokio::io::AsyncWriteExt;

use crate::fs::fsync_dir;

pub const RETIRE_LEDGER_FILENAME: &str = "toast_retires.bin";

/// Bump on any layout change; load rejects mismatched versions.
pub const RETIRE_LEDGER_VERSION: u32 = 1;

/// `WSRTRS` + version-byte + reserved-byte.
const MAGIC: &[u8; 8] = b"WSRTRS\x01\x00";

const HEADER_LEN: usize = MAGIC.len() + 4 /*version*/ + 4 /*count*/;
const ENTRY_LEN: usize = 4 + 8;
const CRC_LEN: usize = 4;

#[derive(Debug, Error)]
pub enum RetireLedgerError {
    #[error("io: {0}")]
    Io(#[from] io::Error),
    #[error("ledger size {got} inconsistent with entry count")]
    Size { got: usize },
    #[error("bad magic header")]
    BadMagic,
    #[error("unsupported ledger schema version {0} (this build expects {RETIRE_LEDGER_VERSION})")]
    Version(u32),
    #[error("crc mismatch: stored={stored:#X}, computed={computed:#X}")]
    Crc { stored: u32, computed: u32 },
}

pub fn ledger_path(spill_dir: &Path) -> PathBuf {
    spill_dir.join(RETIRE_LEDGER_FILENAME)
}

fn encode(entries: &[(u32, u64)]) -> Vec<u8> {
    let mut out = Vec::with_capacity(HEADER_LEN + entries.len() * ENTRY_LEN + CRC_LEN);
    out.extend_from_slice(MAGIC);
    out.extend_from_slice(&RETIRE_LEDGER_VERSION.to_le_bytes());
    out.extend_from_slice(&(entries.len() as u32).to_le_bytes());
    for (toast_relid, commit_lsn) in entries {
        out.extend_from_slice(&toast_relid.to_le_bytes());
        out.extend_from_slice(&commit_lsn.to_le_bytes());
    }
    let crc = crc32c::crc32c(&out);
    out.extend_from_slice(&crc.to_le_bytes());
    out
}

fn decode(bytes: &[u8]) -> Result<Vec<(u32, u64)>, RetireLedgerError> {
    if bytes.len() < HEADER_LEN + CRC_LEN {
        return Err(RetireLedgerError::Size { got: bytes.len() });
    }
    if &bytes[0..8] != MAGIC {
        return Err(RetireLedgerError::BadMagic);
    }
    let version = u32::from_le_bytes(bytes[8..12].try_into().unwrap());
    if version != RETIRE_LEDGER_VERSION {
        return Err(RetireLedgerError::Version(version));
    }
    let count = u32::from_le_bytes(bytes[12..16].try_into().unwrap()) as usize;
    let payload_len = HEADER_LEN + count * ENTRY_LEN;
    if bytes.len() != payload_len + CRC_LEN {
        return Err(RetireLedgerError::Size { got: bytes.len() });
    }
    let stored = u32::from_le_bytes(bytes[payload_len..payload_len + 4].try_into().unwrap());
    let computed = crc32c::crc32c(&bytes[..payload_len]);
    if stored != computed {
        return Err(RetireLedgerError::Crc { stored, computed });
    }
    let mut entries = Vec::with_capacity(count);
    for i in 0..count {
        let off = HEADER_LEN + i * ENTRY_LEN;
        let toast_relid = u32::from_le_bytes(bytes[off..off + 4].try_into().unwrap());
        let commit_lsn = u64::from_le_bytes(bytes[off + 4..off + 12].try_into().unwrap());
        entries.push((toast_relid, commit_lsn));
    }
    Ok(entries)
}

/// Pending `(toast_relid, dropping commit_lsn)` retires, persisted on
/// every mutation.
#[derive(Debug)]
pub struct RetireLedger {
    dir: PathBuf,
    entries: Vec<(u32, u64)>,
}

impl RetireLedger {
    /// Absent file is an empty ledger; corrupt is an error (see module doc).
    pub async fn load(spill_dir: &Path) -> Result<Self, RetireLedgerError> {
        let entries = match tokio::fs::read(ledger_path(spill_dir)).await {
            Ok(bytes) => decode(&bytes)?,
            Err(e) if e.kind() == io::ErrorKind::NotFound => Vec::new(),
            Err(e) => return Err(e.into()),
        };
        Ok(Self {
            dir: spill_dir.to_path_buf(),
            entries,
        })
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn entries(&self) -> &[(u32, u64)] {
        &self.entries
    }

    /// Entries whose dropping commit precedes `cut` (aligned persisted
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
        let final_path = ledger_path(&self.dir);
        let tmp_path = self.dir.join(format!("{RETIRE_LEDGER_FILENAME}.tmp"));
        let bytes = encode(&self.entries);
        let mut f = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&tmp_path)
            .await?;
        f.write_all(&bytes).await?;
        f.sync_all().await?;
        drop(f);
        tokio::fs::rename(&tmp_path, &final_path).await?;
        fsync_dir(&self.dir).await?;
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
        let path = ledger_path(tmp.path());
        let mut bytes = std::fs::read(&path).unwrap();
        *bytes.last_mut().unwrap() ^= 0xFF;
        std::fs::write(&path, &bytes).unwrap();
        let err = RetireLedger::load(tmp.path()).await.unwrap_err();
        assert!(matches!(err, RetireLedgerError::Crc { .. }), "{err:?}");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn truncated_file_is_error() {
        let tmp = tempdir().unwrap();
        let mut ledger = RetireLedger::load(tmp.path()).await.unwrap();
        ledger.push(16500, 0x1000).await.unwrap();
        let path = ledger_path(tmp.path());
        let bytes = std::fs::read(&path).unwrap();
        std::fs::write(&path, &bytes[..bytes.len() - 6]).unwrap();
        let err = RetireLedger::load(tmp.path()).await.unwrap_err();
        assert!(matches!(err, RetireLedgerError::Size { .. }), "{err:?}");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn wrong_version_is_error() {
        let tmp = tempdir().unwrap();
        let mut ledger = RetireLedger::load(tmp.path()).await.unwrap();
        ledger.push(16500, 0x1000).await.unwrap();
        let path = ledger_path(tmp.path());
        let mut bytes = std::fs::read(&path).unwrap();
        bytes[8..12].copy_from_slice(&999u32.to_le_bytes());
        let crc = crc32c::crc32c(&bytes[..bytes.len() - CRC_LEN]);
        let at = bytes.len() - CRC_LEN;
        bytes[at..].copy_from_slice(&crc.to_le_bytes());
        std::fs::write(&path, &bytes).unwrap();
        let err = RetireLedger::load(tmp.path()).await.unwrap_err();
        assert!(matches!(err, RetireLedgerError::Version(999)), "{err:?}");
    }
}
