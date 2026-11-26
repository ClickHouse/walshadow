//! Re-home source tablespaces inside shadow's data dir.
//!
//! Backup sources land each user tablespace as a real `pg_tblspc/<oid>`
//! directory beside a `tablespace_map` naming its source location. Backup
//! recovery would replace every mapped entry with a symlink to that
//! location, absent on this host, and PostgreSQL 15+ refuses a real
//! directory under `pg_tblspc` once recovery reaches consistency. Move each
//! one under [`TABLESPACE_DIR`], link it back by relative path so the data
//! dir stays relocatable, then retire the map the way recovery itself does.

use std::collections::BTreeSet;
use std::io;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};

/// Shadow data dir subdirectory owning relocated tablespaces
pub const TABLESPACE_DIR: &str = "walshadow_tblspc";

const TBLSPC: &str = "pg_tblspc";
const MAP: &str = "tablespace_map";
/// Name recovery itself gives an applied map; never read back
const MAP_APPLIED: &str = "tablespace_map.old";

/// Cluster-relative root a tablespace archive's entries land under
pub fn landing_root(oid: u32) -> PathBuf {
    Path::new(TBLSPC).join(oid.to_string())
}

/// Link every tablespace the backup carried to a directory inside
/// `data_dir`. Idempotent: an entry already linked to its home is left
/// alone. Returns the tablespace OIDs now linked
pub async fn relocate(data_dir: &Path) -> Result<Vec<u32>> {
    let mut oids = map_oids(data_dir).await?;
    oids.extend(landed_oids(data_dir).await?);
    if oids.is_empty() {
        return Ok(Vec::new());
    }
    let homes = data_dir.join(TABLESPACE_DIR);
    let links = data_dir.join(TBLSPC);
    create_private_dir(&homes).await?;
    create_private_dir(&links).await?;
    for &oid in &oids {
        link_home(data_dir, oid).await?;
    }
    for dir in [&homes, &links] {
        crate::fs::fsync_dir(dir)
            .await
            .with_context(|| format!("fsync {}", dir.display()))?;
    }
    // Links land before the map retires, so a crash between them leaves a
    // map that still names every tablespace
    match tokio::fs::rename(data_dir.join(MAP), data_dir.join(MAP_APPLIED)).await {
        Ok(()) => {}
        Err(e) if e.kind() == io::ErrorKind::NotFound => {}
        Err(e) => return Err(e).context("retire tablespace_map"),
    }
    crate::fs::fsync_dir(data_dir)
        .await
        .with_context(|| format!("fsync {}", data_dir.display()))?;
    Ok(oids.into_iter().collect())
}

async fn link_home(data_dir: &Path, oid: u32) -> Result<()> {
    let link = data_dir.join(landing_root(oid));
    let home = data_dir.join(TABLESPACE_DIR).join(oid.to_string());
    let target = Path::new("..").join(TABLESPACE_DIR).join(oid.to_string());
    match tokio::fs::symlink_metadata(&link).await {
        Ok(m) if m.file_type().is_symlink() => {
            let current = tokio::fs::read_link(&link)
                .await
                .with_context(|| format!("read link {}", link.display()))?;
            if current == target {
                return Ok(());
            }
            bail!(
                "{} links to {}, outside shadow data dir; files under it did not land in \
                 shadow, re-bootstrap into an empty data dir",
                link.display(),
                current.display(),
            );
        }
        Ok(m) if m.is_dir() => {
            if tokio::fs::symlink_metadata(&home).await.is_ok() {
                bail!(
                    "tablespace {oid} landed at {} but {} already exists",
                    link.display(),
                    home.display(),
                );
            }
            tokio::fs::rename(&link, &home)
                .await
                .with_context(|| format!("move {} to {}", link.display(), home.display()))?;
        }
        Ok(_) => bail!("{} is neither a directory nor a symlink", link.display()),
        // Mapped but nothing landed: redo creates what it needs underneath
        Err(e) if e.kind() == io::ErrorKind::NotFound => create_private_dir(&home).await?,
        Err(e) => return Err(e).with_context(|| format!("stat {}", link.display())),
    }
    symlink(&target, &link)
        .await
        .with_context(|| format!("link {} to {}", link.display(), target.display()))?;
    tracing::info!(
        target: "walshadow::bootstrap",
        tablespace = oid,
        home = %home.display(),
        "tablespace relocated into shadow data dir",
    );
    Ok(())
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

async fn landed_oids(data_dir: &Path) -> Result<BTreeSet<u32>> {
    let dir = data_dir.join(TBLSPC);
    let mut rd = match tokio::fs::read_dir(&dir).await {
        Ok(rd) => rd,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(BTreeSet::new()),
        Err(e) => return Err(e).with_context(|| format!("read {}", dir.display())),
    };
    let mut out = BTreeSet::new();
    while let Some(entry) = rd.next_entry().await? {
        match entry.file_name().to_str().and_then(|n| n.parse().ok()) {
            Some(oid) => {
                out.insert(oid);
            }
            None => bail!(
                "unexpected entry {} in {}",
                entry.file_name().to_string_lossy(),
                dir.display(),
            ),
        }
    }
    Ok(out)
}

async fn create_private_dir(dir: &Path) -> Result<()> {
    tokio::fs::create_dir_all(dir)
        .await
        .with_context(|| format!("create {}", dir.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        tokio::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700))
            .await
            .with_context(|| format!("chmod 0700 {}", dir.display()))?;
    }
    Ok(())
}

#[cfg(unix)]
async fn symlink(target: &Path, link: &Path) -> io::Result<()> {
    tokio::fs::symlink(target, link).await
}

#[cfg(not(unix))]
async fn symlink(_target: &Path, _link: &Path) -> io::Result<()> {
    Err(io::Error::other("tablespace relocation requires unix"))
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
    async fn landed_tablespace_moves_home_and_map_retires() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();
        write(&dir.join(MAP), MAP_LINE.as_bytes()).await;
        write(
            &dir.join("pg_tblspc/61160/PG_18_202506291/61161/16400"),
            b"page",
        )
        .await;
        tokio::fs::create_dir_all(dir.join("pg_tblspc/61160/PG_18_202506291/pgsql_tmp"))
            .await
            .unwrap();

        assert_eq!(relocate(dir).await.unwrap(), vec![61160]);

        let link = dir.join("pg_tblspc/61160");
        let meta = tokio::fs::symlink_metadata(&link).await.unwrap();
        assert!(meta.file_type().is_symlink());
        assert_eq!(
            tokio::fs::read_link(&link).await.unwrap(),
            Path::new("../walshadow_tblspc/61160"),
        );
        assert_eq!(
            tokio::fs::read(link.join("PG_18_202506291/61161/16400"))
                .await
                .unwrap(),
            b"page",
        );
        assert!(
            dir.join("walshadow_tblspc/61160/PG_18_202506291/pgsql_tmp")
                .is_dir()
        );
        assert!(!dir.join(MAP).exists());
        assert_eq!(
            tokio::fs::read_to_string(dir.join(MAP_APPLIED))
                .await
                .unwrap(),
            MAP_LINE,
        );

        // Second pass finds every link in place
        assert_eq!(relocate(dir).await.unwrap(), vec![61160]);
        assert!(
            tokio::fs::symlink_metadata(&link)
                .await
                .unwrap()
                .file_type()
                .is_symlink()
        );
    }

    /// Recovery replays into a mapped tablespace that shipped no files
    #[tokio::test]
    async fn mapped_tablespace_without_files_gets_an_empty_home() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();
        write(&dir.join(MAP), b"16385 /srv/a\n16386 /srv/with space\n\n").await;
        write(&dir.join("pg_tblspc/16385/PG_18_202506291/5/16400"), b"x").await;

        assert_eq!(relocate(dir).await.unwrap(), vec![16385, 16386]);
        assert!(dir.join("pg_tblspc/16386").is_dir());
        assert!(dir.join("walshadow_tblspc/16386").is_dir());
        assert!(
            dir.join("pg_tblspc/16385/PG_18_202506291/5/16400")
                .is_file()
        );
    }

    #[tokio::test]
    async fn no_tablespaces_leaves_data_dir_untouched() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();
        tokio::fs::create_dir(dir.join(TBLSPC)).await.unwrap();
        assert!(relocate(dir).await.unwrap().is_empty());
        assert!(!dir.join(TABLESPACE_DIR).exists());
    }

    /// Entries under a link to the source's own location never reached shadow
    #[tokio::test]
    async fn foreign_link_is_refused() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();
        tokio::fs::create_dir(dir.join(TBLSPC)).await.unwrap();
        tokio::fs::symlink("/srv/elsewhere", dir.join("pg_tblspc/16385"))
            .await
            .unwrap();
        let err = relocate(dir).await.unwrap_err();
        assert!(format!("{err:#}").contains("/srv/elsewhere"), "{err:#}");
    }

    #[tokio::test]
    async fn malformed_map_is_refused() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();
        write(&dir.join(MAP), b"not-an-oid /srv/a\n").await;
        assert!(relocate(dir).await.is_err());
        assert!(dir.join(MAP).exists());
    }
}
