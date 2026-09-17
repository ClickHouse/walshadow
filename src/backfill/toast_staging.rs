//! Stage the source's TOAST heap files so PostgreSQL can serve values out of
//! them instead of a ClickHouse chunk mirror.
//!
//! One staged tree serves two consumers:
//!
//! * Bootstrap oracle uses copied files while processing backup WAL.
//!   `pg_dump --binary-upgrade` preserves source relation OIDs and relfilenodes,
//!   so `va_toastrelid` resolves without remapping.
//! * Shadow uses same files and replays WAL through `end_lsn`. Replay adds
//!   values written during backup and repairs torn pages and checksums.
//!
//! Tree mirrors PostgreSQL data-directory layout.

use std::io;
use std::path::{Path, PathBuf};

use ahash::HashSet;

/// Staged TOAST heaps and indexes, keyed by `(db_node, rel_node)`
pub type StagedRels = std::sync::Arc<ahash::HashSet<(u32, u32)>>;

/// Staging root and relation set
pub type StagingTarget = (PathBuf, StagedRels);

/// Root of a tree mirroring the data dir
#[derive(Debug, Clone)]
pub struct ToastStaging {
    root: PathBuf,
}

impl ToastStaging {
    pub fn new(root: PathBuf) -> Self {
        Self { root }
    }

    pub fn root(&self) -> &Path {
        &self.root
    }
}

/// Read `(db, relnode)` pairs from staged filenames
pub async fn staged_filenodes(staging: &ToastStaging) -> io::Result<HashSet<(u32, u32)>> {
    let mut out: HashSet<(u32, u32)> = HashSet::default();
    let base = staging.root().join("base");
    let mut dbs = match tokio::fs::read_dir(&base).await {
        Ok(rd) => rd,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(out),
        Err(e) => return Err(e),
    };
    while let Some(db) = dbs.next_entry().await? {
        let Some(db_oid) = db.file_name().to_str().and_then(|s| s.parse::<u32>().ok()) else {
            continue;
        };
        let mut files = tokio::fs::read_dir(db.path()).await?;
        while let Some(f) = files.next_entry().await? {
            let name = f.file_name();
            let Some(name) = name.to_str() else { continue };
            // Map `<relnode>` and `<relnode>.<segno>` to same relation
            let stem = name.split('.').next().unwrap_or(name);
            if let Ok(rel) = stem.parse::<u32>() {
                out.insert((db_oid, rel));
            }
        }
    }
    Ok(out)
}

/// Copy the tree into `data_dir`, renaming the database directory to `db_oid`.
///
/// Copy because oracle and shadow both need source tree.
/// `pg_dump --binary-upgrade` preserves relation OIDs and relfilenodes but not
/// database OID. Rename database directory for oracle; shadow keeps source OID.
pub async fn install_into(
    staging: &ToastStaging,
    data_dir: &Path,
    db_oid: u32,
) -> io::Result<(u64, u64)> {
    let mut files = 0u64;
    let mut bytes = 0u64;
    let base = staging.root().join("base");
    let mut dbs = match tokio::fs::read_dir(&base).await {
        Ok(rd) => rd,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok((0, 0)),
        Err(e) => return Err(e),
    };
    let dst = data_dir.join("base").join(db_oid.to_string());
    while let Some(db) = dbs.next_entry().await? {
        if !db.file_type().await?.is_dir() {
            continue;
        }
        tokio::fs::create_dir_all(&dst).await?;
        let mut src_files = tokio::fs::read_dir(db.path()).await?;
        while let Some(f) = src_files.next_entry().await? {
            let to = dst.join(f.file_name());
            bytes += tokio::fs::copy(f.path(), &to).await?;
            files += 1;
        }
    }
    Ok((files, bytes))
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn stage(root: &Path, db: u32, names: &[&str]) {
        let dir = root.join("base").join(db.to_string());
        tokio::fs::create_dir_all(&dir).await.unwrap();
        for n in names {
            tokio::fs::write(dir.join(n), b"page").await.unwrap();
        }
    }

    #[tokio::test]
    async fn staged_filenodes_collapses_segments_to_one_relation() {
        let tmp = tempfile::tempdir().unwrap();
        let staging = ToastStaging::new(tmp.path().to_path_buf());
        stage(staging.root(), 5, &["16400", "16400.1", "16400.2", "16500"]).await;

        let got = staged_filenodes(&staging).await.unwrap();
        assert_eq!(got.len(), 2, "segments of one relation are one entry");
        assert!(got.contains(&(5, 16400)) && got.contains(&(5, 16500)));
    }

    #[tokio::test]
    async fn staged_filenodes_tolerates_an_absent_tree() {
        let tmp = tempfile::tempdir().unwrap();
        let staging = ToastStaging::new(tmp.path().join("never-written"));
        assert!(staged_filenodes(&staging).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn install_renames_the_database_directory_and_keeps_the_source() {
        let tmp = tempfile::tempdir().unwrap();
        let staging = ToastStaging::new(tmp.path().join("staged"));
        stage(staging.root(), 5, &["16400", "16400.1"]).await;
        let data_dir = tmp.path().join("oracle");

        let (files, bytes) = install_into(&staging, &data_dir, 99).await.unwrap();
        assert_eq!(files, 2);
        assert_eq!(bytes, 8);
        assert!(data_dir.join("base/99/16400").is_file());
        assert!(data_dir.join("base/99/16400.1").is_file());
        assert!(
            staging.root().join("base/5/16400").is_file(),
            "the second consumer still needs the tree",
        );

        // Install same tree for shadow using source database OID
        let shadow = tmp.path().join("shadow");
        install_into(&staging, &shadow, 5).await.unwrap();
        assert!(shadow.join("base/5/16400").is_file());
    }

    #[tokio::test]
    async fn install_tolerates_an_absent_tree() {
        let tmp = tempfile::tempdir().unwrap();
        let staging = ToastStaging::new(tmp.path().join("never-written"));
        let got = install_into(&staging, &tmp.path().join("d"), 5)
            .await
            .unwrap();
        assert_eq!(got, (0, 0));
    }
}
