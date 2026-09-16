use std::path::Path;

use anyhow::{Context, Result};

use crate::ch_emitter::BootstrapMode;

pub const MARKER_FILENAME: &str = "walshadow_bootstrap.incomplete";

pub const MAX_ATTEMPTS: u32 = 3;

const GATE_SPOOL_PREFIX: &str = "bootstrap_gate_deferred.";

#[derive(Debug, Default, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct BootstrapMarker {
    pub attempts: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub backup_name: Option<String>,
}

impl BootstrapMarker {
    pub fn read(dir: &Path) -> Option<Self> {
        let raw = std::fs::read_to_string(dir.join(MARKER_FILENAME)).ok()?;
        Some(toml::from_str(&raw).unwrap_or(Self {
            attempts: 1,
            backup_name: None,
        }))
    }

    pub async fn write(&self, dir: &Path) -> Result<()> {
        let body = toml::to_string(self).context("render bootstrap marker")?;
        tokio::fs::write(dir.join(MARKER_FILENAME), body)
            .await
            .context("write bootstrap marker")
    }

    pub async fn clear(dir: &Path) -> Result<()> {
        tokio::fs::remove_file(dir.join(MARKER_FILENAME))
            .await
            .context("clear completed bootstrap marker")
    }

    pub fn attempts_exhausted(&self) -> bool {
        self.attempts >= MAX_ATTEMPTS
    }

    pub fn next_attempt(&self) -> Self {
        Self {
            attempts: self.attempts + 1,
            backup_name: self.backup_name.clone(),
        }
    }

    pub fn pin(&mut self, backup_name: &str) {
        self.backup_name = Some(backup_name.to_owned());
    }
}

pub fn pending_attempt(dir: &Path, mode: BootstrapMode) -> Result<Option<BootstrapMarker>> {
    let Some(marker) = BootstrapMarker::read(dir) else {
        return Ok(None);
    };
    anyhow::ensure!(
        mode == BootstrapMode::ObjectStore && !marker.attempts_exhausted(),
        "shadow data dir {} contains {MARKER_FILENAME}; bootstrap incomplete after {} attempt(s) \
         in mode {mode:?}, automatic rebootstrap unsupported, choose a new empty data dir or use \
         operator recovery",
        dir.display(),
        marker.attempts,
    );
    Ok(Some(marker))
}

pub async fn resolve_backup(
    storage: &walrus::storage::DynStorage,
    configured: &str,
    previous: Option<&BootstrapMarker>,
) -> Result<String> {
    let name = if let Some(pinned) = previous.and_then(|m| m.backup_name.as_deref()) {
        pinned
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
    spill_dir: &Path,
    previous: Option<BootstrapMarker>,
    pin: Option<String>,
) -> Result<BootstrapMarker> {
    let mut marker = match previous {
        Some(previous) => {
            let next = previous.next_attempt();
            tracing::warn!(
                target: "walshadow::bootstrap",
                data_dir = %data_dir.display(),
                attempt = next.attempts,
                max_attempts = MAX_ATTEMPTS,
                backup_name = next.backup_name.as_deref().unwrap_or("LATEST"),
                "discarding an incomplete bootstrap and extracting again",
            );
            discard_partial(data_dir, spill_dir).await?;
            next
        }
        None => BootstrapMarker::default().next_attempt(),
    };
    if let Some(name) = pin {
        marker.pin(&name);
    }
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
    marker.write(data_dir).await?;
    Ok(marker)
}

pub async fn discard_partial(data_dir: &Path, spill_dir: &Path) -> Result<()> {
    match tokio::fs::remove_dir_all(data_dir).await {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e),
    }
    .with_context(|| format!("discard partial bootstrap {}", data_dir.display()))?;
    clear_gate_spools(spill_dir).await
}

async fn clear_gate_spools(spill_dir: &Path) -> Result<()> {
    let mut rd = match tokio::fs::read_dir(spill_dir).await {
        Ok(rd) => rd,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => {
            return Err(e).with_context(|| format!("read spill dir {}", spill_dir.display()));
        }
    };
    while let Some(entry) = rd.next_entry().await? {
        if entry
            .file_name()
            .to_string_lossy()
            .starts_with(GATE_SPOOL_PREFIX)
        {
            tokio::fs::remove_file(entry.path())
                .await
                .with_context(|| format!("remove {}", entry.path().display()))?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn round_trips_attempts_and_pinned_backup() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();
        assert_eq!(BootstrapMarker::read(dir), None);

        let mut marker = BootstrapMarker {
            attempts: 1,
            backup_name: None,
        };
        marker.pin("base_00000006000003DE00000064");
        marker.write(dir).await.unwrap();

        let back = BootstrapMarker::read(dir).expect("marker present");
        assert_eq!(back, marker);
        assert!(!back.attempts_exhausted());

        BootstrapMarker::clear(dir).await.unwrap();
        assert_eq!(BootstrapMarker::read(dir), None);
    }

    #[tokio::test]
    async fn unparseable_marker_counts_as_one_attempt() {
        let tmp = tempfile::tempdir().unwrap();
        tokio::fs::write(tmp.path().join(MARKER_FILENAME), b"")
            .await
            .unwrap();
        let marker = BootstrapMarker::read(tmp.path()).expect("marker present");
        assert_eq!(marker.attempts, 1);
        assert_eq!(marker.backup_name, None);
        assert!(!marker.attempts_exhausted());
    }

    #[test]
    fn next_attempt_keeps_the_pin_and_stops_at_the_cap() {
        let mut marker = BootstrapMarker {
            attempts: 1,
            backup_name: None,
        };
        marker.pin("base_x");
        for expected in 2..=MAX_ATTEMPTS {
            marker = marker.next_attempt();
            assert_eq!(marker.attempts, expected);
            assert_eq!(marker.backup_name.as_deref(), Some("base_x"));
        }
        assert!(marker.attempts_exhausted());
    }

    #[tokio::test]
    async fn discard_takes_the_data_dir_and_the_gate_spools_only() {
        let tmp = tempfile::tempdir().unwrap();
        let data_dir = tmp.path().join("shadow");
        let spill_dir = tmp.path().join("spill");
        tokio::fs::create_dir_all(data_dir.join("base"))
            .await
            .unwrap();
        tokio::fs::write(data_dir.join("PG_VERSION"), b"17\n")
            .await
            .unwrap();
        tokio::fs::create_dir_all(&spill_dir).await.unwrap();
        let spool = spill_dir.join(format!("{GATE_SPOOL_PREFIX}3.bin"));
        tokio::fs::write(&spool, b"stale").await.unwrap();
        let keep = spill_dir.join("xact_spill.0.bin");
        tokio::fs::write(&keep, b"keep").await.unwrap();

        discard_partial(&data_dir, &spill_dir).await.unwrap();

        assert!(!data_dir.exists());
        assert!(!spool.exists());
        assert!(keep.exists(), "unrelated spill files must survive");
    }

    #[tokio::test]
    async fn no_marker_is_not_a_pending_attempt() {
        let tmp = tempfile::tempdir().unwrap();
        for mode in [
            BootstrapMode::Off,
            BootstrapMode::Direct,
            BootstrapMode::ObjectStore,
        ] {
            assert_eq!(pending_attempt(tmp.path(), mode).unwrap(), None);
        }
    }

    #[tokio::test]
    async fn object_store_retries_under_the_cap_and_refuses_at_it() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();
        let mut marker = BootstrapMarker {
            attempts: 1,
            backup_name: None,
        };
        marker.pin("base_x");
        marker.write(dir).await.unwrap();
        assert_eq!(
            pending_attempt(dir, BootstrapMode::ObjectStore).unwrap(),
            Some(marker.clone()),
        );

        BootstrapMarker {
            attempts: MAX_ATTEMPTS,
            backup_name: Some("base_x".into()),
        }
        .write(dir)
        .await
        .unwrap();
        let err = pending_attempt(dir, BootstrapMode::ObjectStore).unwrap_err();
        assert!(err.to_string().contains("operator recovery"), "{err}");
        assert!(
            err.to_string().contains(&format!("{MAX_ATTEMPTS} attempt")),
            "{err}",
        );
    }

    #[tokio::test]
    async fn other_modes_still_demand_an_operator() {
        let tmp = tempfile::tempdir().unwrap();
        BootstrapMarker {
            attempts: 1,
            backup_name: None,
        }
        .write(tmp.path())
        .await
        .unwrap();
        for mode in [BootstrapMode::Off, BootstrapMode::Direct] {
            let err = pending_attempt(tmp.path(), mode).unwrap_err();
            assert!(err.to_string().contains("operator recovery"), "{err}");
        }
    }

    #[tokio::test]
    async fn first_attempt_requires_an_empty_dir() {
        let tmp = tempfile::tempdir().unwrap();
        let data_dir = tmp.path().join("shadow");
        let spill_dir = tmp.path().join("spill");

        let marker = begin_attempt(&data_dir, &spill_dir, None, None)
            .await
            .unwrap();
        assert_eq!(marker.attempts, 1);
        assert_eq!(BootstrapMarker::read(&data_dir), Some(marker));

        let err = begin_attempt(&data_dir, &spill_dir, None, None)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("non-empty"), "{err}");
        assert!(
            data_dir.join(MARKER_FILENAME).exists(),
            "a refusal must not delete what is there",
        );
    }

    #[tokio::test]
    async fn retry_discards_the_partial_and_keeps_the_pin() {
        let tmp = tempfile::tempdir().unwrap();
        let data_dir = tmp.path().join("shadow");
        let spill_dir = tmp.path().join("spill");
        let first = begin_attempt(&data_dir, &spill_dir, None, None)
            .await
            .unwrap();
        tokio::fs::write(data_dir.join("PG_VERSION"), b"17\n")
            .await
            .unwrap();
        tokio::fs::create_dir_all(&spill_dir).await.unwrap();
        let spool = spill_dir.join(format!("{GATE_SPOOL_PREFIX}0.bin"));
        tokio::fs::write(&spool, b"stale").await.unwrap();

        let mut pinned = first;
        pinned.pin("base_pinned");
        let second = begin_attempt(&data_dir, &spill_dir, Some(pinned), None)
            .await
            .unwrap();

        assert_eq!(second.attempts, 2);
        assert_eq!(second.backup_name.as_deref(), Some("base_pinned"));
        assert!(!data_dir.join("PG_VERSION").exists());
        assert!(!spool.exists());
        assert_eq!(BootstrapMarker::read(&data_dir), Some(second));
    }

    #[tokio::test]
    async fn discard_is_idempotent_when_nothing_landed() {
        let tmp = tempfile::tempdir().unwrap();
        discard_partial(&tmp.path().join("absent"), &tmp.path().join("no-spill"))
            .await
            .unwrap();
    }
}
