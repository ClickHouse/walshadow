//! Keep source tablespaces in place inside shadow's data dir
//!
//! Source tablespace locations do not exist on this host, so each one lives
//! as a real `pg_tblspc/<oid>` directory, PostgreSQL's in-place tablespace.
//! Backup recovery would replace every entry `tablespace_map` names with a
//! symlink to its source location, so the map retires the way recovery
//! itself retires it. Streamed `CREATE TABLESPACE` lands in place too, see
//! [`crate::filter::rewrite::blank_tablespace_path`]. Shadow runs with
//! `allow_in_place_tablespaces`, without which PostgreSQL 15+ refuses a
//! directory under `pg_tblspc` once recovery is consistent

use std::collections::BTreeSet;
use std::io;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};

use crate::backfill::pg_path::TBLSPC_DIR;

const MAP: &str = "tablespace_map";
/// Name recovery itself gives an applied map; never read back
const MAP_APPLIED: &str = "tablespace_map.old";

/// Cluster-relative root a tablespace archive's entries land under
pub fn landing_root(oid: u32) -> PathBuf {
    Path::new(TBLSPC_DIR).join(oid.to_string())
}

/// Give every tablespace the map names a directory, then retire the map.
/// Idempotent. Returns the tablespace OIDs the map named
pub async fn retire_map(data_dir: &Path) -> Result<Vec<u32>> {
    let oids = map_oids(data_dir).await?;
    if oids.is_empty() {
        return Ok(Vec::new());
    }
    // Mapped but nothing landed: redo creates what it needs underneath
    for &oid in &oids {
        let dir = data_dir.join(landing_root(oid));
        tokio::fs::create_dir_all(&dir)
            .await
            .with_context(|| format!("create {}", dir.display()))?;
    }
    crate::fs::fsync_dir(&data_dir.join(TBLSPC_DIR))
        .await
        .context("persist pg_tblspc entries")?;
    tokio::fs::rename(data_dir.join(MAP), data_dir.join(MAP_APPLIED))
        .await
        .context("retire tablespace_map")?;
    crate::fs::fsync_dir(data_dir)
        .await
        .context("persist tablespace_map retirement")?;
    tracing::info!(
        target: "walshadow::bootstrap",
        tablespaces = ?oids,
        "source tablespaces kept in place",
    );
    Ok(oids.into_iter().collect())
}

/// `<oid> <location>` per line; location escaping never matters here
async fn map_oids(data_dir: &Path) -> Result<BTreeSet<u32>> {
    let path = data_dir.join(MAP);
    let raw = match tokio::fs::read_to_string(&path).await {
        Ok(raw) => raw,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(BTreeSet::new()),
        Err(e) => return Err(e).with_context(|| format!("read {}", path.display())),
    };
    raw.lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| {
            l.split_once(' ')
                .and_then(|(oid, _)| oid.parse().ok())
                .with_context(|| format!("{}: malformed line {l:?}", path.display()))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    const MAP_LINE: &str = "61160 /var/lib/postgresql/tablespaces/temp/data\n";

    async fn write(path: &Path, body: &[u8]) {
        tokio::fs::create_dir_all(path.parent().unwrap())
            .await
            .unwrap();
        tokio::fs::write(path, body).await.unwrap();
    }

    #[tokio::test]
    async fn landed_tablespace_stays_and_map_retires() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();
        write(&dir.join(MAP), MAP_LINE.as_bytes()).await;
        let page = dir.join("pg_tblspc/61160/PG_18_202506291/61161/16400");
        write(&page, b"page").await;

        assert_eq!(retire_map(dir).await.unwrap(), vec![61160]);
        assert_eq!(tokio::fs::read(&page).await.unwrap(), b"page");
        assert!(!dir.join(MAP).exists());
        assert_eq!(
            tokio::fs::read_to_string(dir.join(MAP_APPLIED))
                .await
                .unwrap(),
            MAP_LINE,
        );
        // Second pass finds no map
        assert!(retire_map(dir).await.unwrap().is_empty());
    }

    /// Recovery replays into a mapped tablespace that shipped no files
    #[tokio::test]
    async fn mapped_tablespace_without_files_gets_a_directory() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();
        write(&dir.join(MAP), b"16385 /srv/a\n16386 /srv/with space\n\n").await;
        write(&dir.join("pg_tblspc/16385/PG_18_202506291/5/16400"), b"x").await;

        assert_eq!(retire_map(dir).await.unwrap(), vec![16385, 16386]);
        let meta = tokio::fs::symlink_metadata(dir.join("pg_tblspc/16386"))
            .await
            .unwrap();
        assert!(meta.is_dir());
    }

    #[tokio::test]
    async fn malformed_map_is_refused() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();
        write(&dir.join(MAP), b"not-an-oid /srv/a\n").await;
        assert!(retire_map(dir).await.is_err());
        assert!(dir.join(MAP).exists());
    }

    #[tokio::test]
    async fn unreadable_map_is_refused() {
        let tmp = tempfile::tempdir().unwrap();
        tokio::fs::create_dir(tmp.path().join(MAP)).await.unwrap();
        let err = retire_map(tmp.path()).await.unwrap_err();
        assert!(format!("{err:#}").contains("read"), "{err:#}");
    }

    #[tokio::test]
    async fn tablespace_slot_taken_by_a_file_is_refused() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();
        write(&dir.join(MAP), b"16385 /srv/a\n").await;
        write(&dir.join("pg_tblspc/16385"), b"not a dir").await;
        let err = retire_map(dir).await.unwrap_err();
        assert!(format!("{err:#}").contains("create"), "{err:#}");
        assert!(dir.join(MAP).exists());
    }
}
