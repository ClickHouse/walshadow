use std::io;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};

use crate::ch_emitter::BootstrapMode;

pub const MARKER_FILENAME: &str = "walshadow_bootstrap.incomplete";

pub const MAX_ATTEMPTS: u32 = 3;

pub const EXTRACTED_FILENAME: &str = "walshadow_bootstrap.extracted";

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct BootstrapMarker {
    pub attempts: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub backup_name: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ExtractedCheckpoint {
    pub backup_name: String,
    pub start_lsn: u64,
    pub end_lsn: u64,
    pub timeline: u32,
    #[serde(default)]
    pub deferred_spools: Vec<SpooledRecords>,
    #[serde(default)]
    pub handback_spools: Vec<SpooledRecords>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SpooledRecords {
    pub path: PathBuf,
    pub records: u64,
}

impl SpooledRecords {
    pub fn expected(spools: &[Self], path: &Path) -> Option<u64> {
        spools.iter().find(|s| s.path == path).map(|s| s.records)
    }
}

impl ExtractedCheckpoint {
    pub fn read(dir: &Path) -> Result<Option<Self>> {
        let path = dir.join(EXTRACTED_FILENAME);
        let raw = match std::fs::read_to_string(&path) {
            Ok(raw) => raw,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e).with_context(|| format!("read {}", path.display())),
        };
        toml::from_str(&raw)
            .map(Some)
            .with_context(|| format!("parse {}", path.display()))
    }

    /// Sync extracted files first, since this claims they all landed
    pub async fn write(&self, dir: &Path) -> Result<()> {
        crate::fs::sync_filesystem(dir)
            .await
            .context("sync extracted backup")?;
        let body = toml::to_string(self).context("render extracted checkpoint")?;
        crate::fs::write_atomic(dir, EXTRACTED_FILENAME, body.as_bytes())
            .await
            .context("persist extracted checkpoint")
    }

    pub async fn clear(dir: &Path) -> Result<()> {
        match tokio::fs::remove_file(dir.join(EXTRACTED_FILENAME)).await {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(()),
            Err(e) => return Err(e).context("clear extracted checkpoint"),
        }
        crate::fs::fsync_dir(dir)
            .await
            .context("persist extracted checkpoint removal")
    }

    pub fn matches(&self, backup_name: &str) -> bool {
        self.backup_name == backup_name
    }
}

impl BootstrapMarker {
    /// Only absence is a clean answer: an unreadable or unparseable marker
    /// still says a bootstrap was interrupted, so it stops startup rather
    /// than reading as a fresh data dir
    pub fn read(dir: &Path) -> Result<Option<Self>> {
        let path = dir.join(MARKER_FILENAME);
        let raw = match std::fs::read_to_string(&path) {
            Ok(raw) => raw,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e).with_context(|| format!("read {}", path.display())),
        };
        toml::from_str(&raw).map(Some).with_context(|| {
            format!(
                "parse {}; bootstrap incomplete, use operator recovery",
                path.display()
            )
        })
    }

    pub async fn write(&self, dir: &Path) -> Result<()> {
        let body = toml::to_string(self).context("render bootstrap marker")?;
        crate::fs::write_atomic(dir, MARKER_FILENAME, body.as_bytes())
            .await
            .context("persist bootstrap marker")
    }

    pub async fn clear(dir: &Path) -> Result<()> {
        tokio::fs::remove_file(dir.join(MARKER_FILENAME))
            .await
            .context("clear completed bootstrap marker")?;
        crate::fs::fsync_dir(dir)
            .await
            .context("persist bootstrap completion")
    }

    fn next_attempt(&self) -> Self {
        Self {
            attempts: self.attempts + 1,
            backup_name: self.backup_name.clone(),
        }
    }

    /// Retrying against a different backup rebases every row on a different
    /// snapshot, so only a resolved name licenses one
    fn pinned_backup(&self) -> Result<&str> {
        self.backup_name
            .as_deref()
            .filter(|name| name.starts_with(walrus::pg::backup::BACKUP_NAME_PREFIX))
            .context("bootstrap incomplete without a resolved backup pin; use operator recovery")
    }

    pub fn pinned_backup_name(&self) -> Result<&str> {
        self.pinned_backup()
    }

    fn check_retry(&self) -> Result<()> {
        self.pinned_backup()?;
        anyhow::ensure!(
            (1..MAX_ATTEMPTS).contains(&self.attempts),
            "bootstrap incomplete after {} attempt(s); use operator recovery",
            self.attempts,
        );
        Ok(())
    }
}

pub fn pending_attempt(dir: &Path, mode: BootstrapMode) -> Result<Option<BootstrapMarker>> {
    let Some(marker) = BootstrapMarker::read(dir)? else {
        return Ok(None);
    };
    anyhow::ensure!(
        mode == BootstrapMode::ObjectStore,
        "shadow data dir {} contains {MARKER_FILENAME}; bootstrap incomplete in mode {mode:?}, \
         automatic rebootstrap unsupported, choose a new empty data dir or use operator recovery",
        dir.display(),
    );
    marker
        .check_retry()
        .with_context(|| format!("shadow data dir {}", dir.display()))?;
    Ok(Some(marker))
}

pub async fn resolve_backup(
    storage: &walrus::storage::DynStorage,
    configured: &str,
    previous: Option<&BootstrapMarker>,
) -> Result<String> {
    let name = if let Some(marker) = previous {
        marker.pinned_backup()?
    } else {
        configured
    };
    anyhow::ensure!(
        name == "LATEST" || name.starts_with(walrus::pg::backup::BACKUP_NAME_PREFIX),
        "bootstrap: backup name {name:?} must be `LATEST` or begin with `{}` \
         (--bootstrap-backup-name / [bootstrap] backup_name)",
        walrus::pg::backup::BACKUP_NAME_PREFIX,
    );
    walrus::pg::backup::fetch::resolve_name(storage, name)
        .await
        .with_context(|| format!("bootstrap: resolve {name}"))
}

pub async fn begin_attempt(
    data_dir: &Path,
    previous: Option<BootstrapMarker>,
    pin: Option<String>,
) -> Result<BootstrapMarker> {
    let Some(previous) = previous else {
        return first_attempt(data_dir, pin).await;
    };
    previous.check_retry()?;
    anyhow::ensure!(
        pin == previous.backup_name,
        "bootstrap retry changed backup pin",
    );
    let marker = previous.next_attempt();
    // Land the next attempt before deleting anything: an interrupted or
    // failed cleanup must still leave a marker naming the pinned backup
    marker.write(data_dir).await?;
    if let Some(done) = resumable_extraction(data_dir, marker.backup_name.as_deref())? {
        tracing::warn!(
            target: "walshadow::bootstrap",
            data_dir = %data_dir.display(),
            attempt = marker.attempts,
            max_attempts = MAX_ATTEMPTS,
            backup_name = done.backup_name,
            end_lsn = done.end_lsn,
            deferred_spools = done.deferred_spools.len(),
            "resuming an incomplete bootstrap past extraction",
        );
        return Ok(marker);
    }
    tracing::warn!(
        target: "walshadow::bootstrap",
        data_dir = %data_dir.display(),
        attempt = marker.attempts,
        max_attempts = MAX_ATTEMPTS,
        backup_name = marker.backup_name.as_deref(),
        "discarding an incomplete bootstrap and extracting again",
    );
    discard_partial(data_dir).await?;
    Ok(marker)
}

pub fn resumable_extraction(
    data_dir: &Path,
    pinned: Option<&str>,
) -> Result<Option<ExtractedCheckpoint>> {
    let Some(done) = ExtractedCheckpoint::read(data_dir)? else {
        return Ok(None);
    };
    let Some(pinned) = pinned else {
        return Ok(None);
    };
    if !done.matches(pinned) {
        return Ok(None);
    }
    if done
        .deferred_spools
        .iter()
        .chain(&done.handback_spools)
        .any(|s| !s.path.exists())
    {
        return Ok(None);
    }
    Ok(Some(done))
}

/// Never clear partial or initialized standby state automatically: an empty
/// dir is the only state a fresh load may claim
async fn first_attempt(data_dir: &Path, pin: Option<String>) -> Result<BootstrapMarker> {
    tokio::fs::create_dir_all(data_dir)
        .await
        .with_context(|| format!("create {}", data_dir.display()))?;
    let mut rd = tokio::fs::read_dir(data_dir).await?;
    anyhow::ensure!(
        rd.next_entry().await?.is_none(),
        "shadow data dir {} is non-empty; automatic rebootstrap unsupported, choose a new empty \
         data dir or use operator recovery",
        data_dir.display(),
    );
    let marker = BootstrapMarker {
        attempts: 1,
        backup_name: pin,
    };
    marker.write(data_dir).await?;
    Ok(marker)
}

/// Empty the data dir apart from the marker: the pinned next attempt has to
/// outlive the partial data it replaces
async fn discard_partial(data_dir: &Path) -> Result<()> {
    let mut rd = match tokio::fs::read_dir(data_dir).await {
        Ok(rd) => rd,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(e).with_context(|| format!("read {}", data_dir.display())),
    };
    while let Some(entry) = rd.next_entry().await? {
        if entry.file_name() == MARKER_FILENAME {
            continue;
        }
        let path = entry.path();
        if entry.file_type().await?.is_dir() {
            tokio::fs::remove_dir_all(&path).await
        } else {
            tokio::fs::remove_file(&path).await
        }
        .with_context(|| format!("discard partial bootstrap {}", path.display()))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::os::unix::fs::PermissionsExt;

    use super::*;

    fn pinned(attempts: u32) -> BootstrapMarker {
        BootstrapMarker {
            attempts,
            backup_name: Some("base_original".into()),
        }
    }

    #[tokio::test]
    async fn round_trips_attempts_and_pinned_backup() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();
        assert_eq!(BootstrapMarker::read(dir).unwrap(), None);
        pinned(1).write(dir).await.unwrap();
        assert_eq!(BootstrapMarker::read(dir).unwrap(), Some(pinned(1)));
        pinned(2).write(dir).await.unwrap();
        assert_eq!(BootstrapMarker::read(dir).unwrap(), Some(pinned(2)));
        BootstrapMarker::clear(dir).await.unwrap();
        assert_eq!(BootstrapMarker::read(dir).unwrap(), None);
    }

    #[test]
    fn no_marker_is_not_a_pending_attempt() {
        let tmp = tempfile::tempdir().unwrap();
        for mode in [
            BootstrapMode::Off,
            BootstrapMode::Direct,
            BootstrapMode::ObjectStore,
        ] {
            assert_eq!(pending_attempt(tmp.path(), mode).unwrap(), None);
        }
    }

    /// Legacy empty markers and torn writes name no backup, so no mode may
    /// read them as a resumable attempt
    #[test]
    fn corrupt_and_legacy_markers_block_resume() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::write(tmp.path().join("PG_VERSION"), b"17\n").unwrap();
        for raw in [b"".as_slice(), b"attempts =", &[0xff]] {
            std::fs::write(tmp.path().join(MARKER_FILENAME), raw).unwrap();
            for mode in [
                BootstrapMode::Off,
                BootstrapMode::Direct,
                BootstrapMode::ObjectStore,
            ] {
                assert!(pending_attempt(tmp.path(), mode).is_err());
            }
        }
    }

    #[test]
    fn marker_io_error_blocks_resume() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::create_dir(tmp.path().join(MARKER_FILENAME)).unwrap();
        assert!(pending_attempt(tmp.path(), BootstrapMode::ObjectStore).is_err());
    }

    #[tokio::test]
    async fn retry_requires_concrete_pin_valid_count_and_object_store() {
        let tmp = tempfile::tempdir().unwrap();
        for attempts in [0, MAX_ATTEMPTS, u32::MAX] {
            pinned(attempts).write(tmp.path()).await.unwrap();
            let err = format!(
                "{:#}",
                pending_attempt(tmp.path(), BootstrapMode::ObjectStore).unwrap_err(),
            );
            assert!(err.contains("operator recovery"), "{err}");
            assert!(err.contains(&format!("{attempts} attempt")), "{err}");
        }
        for backup_name in [None, Some("LATEST".into()), Some("invalid".into())] {
            BootstrapMarker {
                attempts: 1,
                backup_name,
            }
            .write(tmp.path())
            .await
            .unwrap();
            assert!(pending_attempt(tmp.path(), BootstrapMode::ObjectStore).is_err());
        }
        pinned(1).write(tmp.path()).await.unwrap();
        assert_eq!(
            pending_attempt(tmp.path(), BootstrapMode::ObjectStore).unwrap(),
            Some(pinned(1)),
        );
        for mode in [BootstrapMode::Off, BootstrapMode::Direct] {
            let err = pending_attempt(tmp.path(), mode).unwrap_err();
            assert!(err.to_string().contains("operator recovery"), "{err}");
        }
    }

    #[tokio::test]
    async fn first_attempt_requires_an_empty_dir() {
        let tmp = tempfile::tempdir().unwrap();
        let data_dir = tmp.path().join("shadow");
        let marker = begin_attempt(&data_dir, None, None).await.unwrap();
        assert_eq!(marker.attempts, 1);
        assert_eq!(BootstrapMarker::read(&data_dir).unwrap(), Some(marker));

        let err = begin_attempt(&data_dir, None, None).await.unwrap_err();
        assert!(err.to_string().contains("non-empty"), "{err}");
        assert!(
            data_dir.join(MARKER_FILENAME).exists(),
            "a refusal must not delete what is there",
        );
    }

    #[tokio::test]
    async fn retry_keeps_the_pin_and_removes_only_partial_data() {
        let tmp = tempfile::tempdir().unwrap();
        let data_dir = tmp.path().join("shadow");
        let first = begin_attempt(&data_dir, None, pinned(1).backup_name)
            .await
            .unwrap();
        assert_eq!(first, pinned(1));
        tokio::fs::create_dir_all(data_dir.join("base"))
            .await
            .unwrap();
        tokio::fs::write(data_dir.join("PG_VERSION"), b"17\n")
            .await
            .unwrap();
        let spill_dir = tmp.path().join("spill");
        tokio::fs::create_dir_all(&spill_dir).await.unwrap();
        let keep = spill_dir.join("xact_spill.0.bin");
        tokio::fs::write(&keep, b"keep").await.unwrap();

        let second = begin_attempt(&data_dir, Some(first), pinned(1).backup_name)
            .await
            .unwrap();

        assert_eq!(second, pinned(2));
        assert_eq!(
            pending_attempt(&data_dir, BootstrapMode::ObjectStore).unwrap(),
            Some(second),
        );
        assert!(!data_dir.join("PG_VERSION").exists());
        assert!(!data_dir.join("base").exists());
        assert!(
            keep.exists(),
            "only the data dir is this function's to empty"
        );
    }

    /// Cleanup dies partway: the next attempt is already on disk, so the
    /// restart still knows its backup and burns an attempt rather than
    /// looping on the same failure
    #[tokio::test]
    async fn cleanup_failure_keeps_pin_and_consumes_attempt() {
        let tmp = tempfile::tempdir().unwrap();
        let data_dir = tmp.path().join("shadow");
        let first = begin_attempt(&data_dir, None, pinned(1).backup_name)
            .await
            .unwrap();
        let blocked = data_dir.join("base");
        tokio::fs::create_dir_all(&blocked).await.unwrap();
        tokio::fs::write(blocked.join("1"), b"page").await.unwrap();
        let sealed = std::fs::Permissions::from_mode(0o500);
        tokio::fs::set_permissions(&blocked, sealed).await.unwrap();

        assert!(
            begin_attempt(&data_dir, Some(first), pinned(1).backup_name)
                .await
                .is_err()
        );
        let previous = pending_attempt(&data_dir, BootstrapMode::ObjectStore).unwrap();
        assert_eq!(previous, Some(pinned(2)));

        let open = std::fs::Permissions::from_mode(0o700);
        tokio::fs::set_permissions(&blocked, open).await.unwrap();
        assert_eq!(
            begin_attempt(&data_dir, previous, pinned(1).backup_name)
                .await
                .unwrap(),
            pinned(3),
        );
        assert!(!blocked.exists());
        assert!(pending_attempt(&data_dir, BootstrapMode::ObjectStore).is_err());
    }

    #[tokio::test]
    async fn changed_pin_leaves_partial_data_intact() {
        let tmp = tempfile::tempdir().unwrap();
        let data_dir = tmp.path().join("shadow");
        let first = begin_attempt(&data_dir, None, pinned(1).backup_name)
            .await
            .unwrap();
        let version = data_dir.join("PG_VERSION");
        tokio::fs::write(&version, b"17\n").await.unwrap();

        assert!(
            begin_attempt(&data_dir, Some(first), Some("base_new".into()))
                .await
                .is_err()
        );
        assert!(version.exists());
        assert_eq!(BootstrapMarker::read(&data_dir).unwrap(), Some(pinned(1)));
    }

    #[tokio::test]
    async fn resolved_pin_overrides_configured_backup() {
        let tmp = tempfile::tempdir().unwrap();
        let storage: walrus::storage::DynStorage =
            std::sync::Arc::new(walrus::storage::fs::FsStorage::new(tmp.path()).unwrap());
        for configured in ["LATEST", "base_new"] {
            assert_eq!(
                resolve_backup(&storage, configured, Some(&pinned(1)))
                    .await
                    .unwrap(),
                "base_original",
            );
            let unpinned = BootstrapMarker {
                attempts: 1,
                backup_name: None,
            };
            assert!(
                resolve_backup(&storage, configured, Some(&unpinned))
                    .await
                    .is_err()
            );
        }
    }

    #[tokio::test]
    async fn discard_is_idempotent_when_nothing_landed() {
        let tmp = tempfile::tempdir().unwrap();
        discard_partial(&tmp.path().join("absent")).await.unwrap();
    }
}
