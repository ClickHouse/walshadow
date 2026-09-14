//! Cross-check archived timeline history against the branch the source proved

use std::path::Path;

use anyhow::{Context, Result};
use walrus::config::Settings;
use walrus::pg::wal::fetch::Prefetch;
use walrus::storage::DynStorage;

use crate::source::timeline::{TimelineHistory, history_filename};

/// Gap replay names segments off `source`'s chain, so the archive serving them
/// has to record that same chain
pub async fn verify(
    settings: &Settings,
    storage: &DynStorage,
    dir: &Path,
    source: &TimelineHistory,
) -> Result<()> {
    tokio::fs::create_dir_all(dir)
        .await
        .with_context(|| format!("create {}", dir.display()))?;
    let name = history_filename(source.target());
    let dst = dir.join(&name);
    if !dst.exists() {
        walrus::pg::wal::fetch::handle(settings, storage.clone(), &name, &dst, Prefetch::Off)
            .await
            .with_context(|| format!("fetch {name}"))?;
    }
    let raw = tokio::fs::read(&dst)
        .await
        .with_context(|| format!("read {}", dst.display()))?;
    let archived =
        TimelineHistory::parse(source.target(), &raw).with_context(|| format!("parse {name}"))?;
    anyhow::ensure!(
        archived.entries() == source.entries(),
        "archived {name} disagrees with source timeline history",
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Match `archive_command`: store history uncompressed under `wal_005/`
    async fn archive_history_file(settings: &Settings, storage: &DynStorage, tli: u32, body: &str) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(history_filename(tli));
        tokio::fs::write(&path, body).await.unwrap();
        walrus::pg::wal::push::handle(settings, storage.clone(), &path)
            .await
            .unwrap();
    }

    fn fs_archive(root: &Path) -> (Settings, DynStorage) {
        let settings = Settings {
            storage: walrus::config::StorageSettings::Fs {
                path: root.display().to_string(),
            },
            ..Default::default()
        };
        let storage = settings.build_storage().unwrap();
        (settings, storage)
    }

    #[tokio::test]
    async fn reject_an_archive_missing_the_sources_own_history() {
        let tmp = tempfile::tempdir().unwrap();
        let (settings, storage) = fs_archive(&tmp.path().join("archive"));
        archive_history_file(&settings, &storage, 2, "1\t0/3000000\tpromotion\n").await;
        let source = TimelineHistory::parse(3, b"1\t0/3000000\tpromotion\n").unwrap();
        let err = verify(&settings, &storage, &tmp.path().join("probe"), &source)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("00000003.history"), "{err:#}");
    }

    #[tokio::test]
    async fn archived_ids_need_not_be_consecutive() {
        let tmp = tempfile::tempdir().unwrap();
        let (settings, storage) = fs_archive(&tmp.path().join("archive"));
        archive_history_file(&settings, &storage, 4, "1\t0/3000000\tpromotion\n").await;
        let source = TimelineHistory::parse(4, b"1\t0/3000000\tpromotion\n").unwrap();
        verify(&settings, &storage, &tmp.path().join("probe"), &source)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn reject_archived_history_with_different_ancestry_or_switchpoint() {
        for body in ["1\t0/4000000\tpromotion\n", "2\t0/3000000\tpromotion\n"] {
            let tmp = tempfile::tempdir().unwrap();
            let (settings, storage) = fs_archive(&tmp.path().join("archive"));
            archive_history_file(&settings, &storage, 3, body).await;
            let source = TimelineHistory::parse(3, b"1\t0/3000000\tpromotion\n").unwrap();
            let err = verify(&settings, &storage, &tmp.path().join("probe"), &source)
                .await
                .unwrap_err();
            assert!(err.to_string().contains("disagrees"), "{err:#}");
        }
    }
}
