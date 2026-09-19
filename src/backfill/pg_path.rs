use std::io;
use std::path::Path;

use ahash::HashSet;

use crate::schema::FIRST_NORMAL_OBJECT_ID;

pub const SYSTEM_DIRS_DENYLIST: &[&str] = &[
    "pg_replslot",
    "pg_stat_tmp",
    "pg_logical",
    "pg_dynshmem",
    "pg_subtrans",
    "pg_notify",
    "pg_serial",
    "pg_snapshots",
    "pgsql_tmp",
];

pub fn is_system_dir(path: &Path) -> bool {
    let head = path
        .components()
        .next()
        .and_then(|c| c.as_os_str().to_str())
        .unwrap_or("");
    SYSTEM_DIRS_DENYLIST.contains(&head) || head.starts_with("temp_")
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RelFork {
    Main,
    Fsm,
    Vm,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BaseRelFile {
    pub db: u32,
    pub filenode: u32,
    pub fork: RelFork,
    pub segno: u32,
}

pub fn parse_base_path(path: &Path) -> Option<BaseRelFile> {
    let rest = path.to_str()?.strip_prefix("base/")?;
    let (db, leaf) = rest.split_once('/')?;
    let (stem, segno) = match leaf.split_once('.') {
        Some((stem, seg)) => (stem, seg.parse().ok()?),
        None => (leaf, 0),
    };
    let (stem, fork) = if let Some(stem) = stem.strip_suffix("_fsm") {
        (stem, RelFork::Fsm)
    } else if let Some(stem) = stem.strip_suffix("_vm") {
        (stem, RelFork::Vm)
    } else {
        (stem, RelFork::Main)
    };
    Some(BaseRelFile {
        db: db.parse().ok()?,
        filenode: stem.parse().ok()?,
        fork,
        segno,
    })
}

/// Find user relations with main forks under shadow's `base/<db_oid>/`
///
/// Skip initdb filenodes. Catalog routing handles rotated catalog files first
pub async fn user_relation_filenodes(
    data_dir: &Path,
    db_oid: u32,
) -> io::Result<HashSet<(u32, u32)>> {
    let rel_dir = Path::new("base").join(db_oid.to_string());
    let mut out = HashSet::default();
    let mut files = tokio::fs::read_dir(data_dir.join(&rel_dir)).await?;
    while let Some(f) = files.next_entry().await? {
        if let Some(rel) = parse_base_path(&rel_dir.join(f.file_name()))
            .filter(|r| r.fork == RelFork::Main && r.filenode >= FIRST_NORMAL_OBJECT_ID)
        {
            out.insert((db_oid, rel.filenode));
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn user_relation_filenodes_reads_one_database_main_forks() {
        let tmp = tempfile::tempdir().unwrap();
        for (db, names) in [
            (
                5,
                &[
                    "1259",
                    "16400",
                    "16400.1",
                    "16400_fsm",
                    "16400_vm",
                    "16401_init",
                    "24000",
                ][..],
            ),
            (6, &["16500"][..]),
        ] {
            let dir = tmp.path().join("base").join(db.to_string());
            tokio::fs::create_dir_all(&dir).await.unwrap();
            for n in names {
                tokio::fs::write(dir.join(n), b"page").await.unwrap();
            }
        }

        let mut got: Vec<_> = user_relation_filenodes(tmp.path(), 5)
            .await
            .unwrap()
            .into_iter()
            .collect();
        got.sort_unstable();
        assert_eq!(got, vec![(5, 16400), (5, 24000)]);

        assert!(
            user_relation_filenodes(tmp.path(), 7).await.is_err(),
            "missing database directory must return an error",
        );
    }
}
