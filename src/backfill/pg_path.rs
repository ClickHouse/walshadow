use std::io;
use std::path::Path;

use foldhash::HashSet;
use walrus::pg::walparser::RelFileNode;

use crate::schema::{DEFAULT_TABLESPACE_OID, FIRST_NORMAL_OBJECT_ID};

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

/// Directory holding each tablespace, one `<oid>` entry apiece
pub const TBLSPC_DIR: &str = "pg_tblspc";

pub fn is_system_dir(path: &Path) -> bool {
    let mut parts = path.components().filter_map(|c| c.as_os_str().to_str());
    let head = parts.next().unwrap_or("");
    if head == TBLSPC_DIR {
        // pg_tblspc/<oid>/<version dir>/pgsql_tmp
        return parts.nth(2) == Some("pgsql_tmp");
    }
    SYSTEM_DIRS_DENYLIST.contains(&head) || head.starts_with("temp_")
}

/// Tablespace OID when `path` is a `pg_tblspc/<oid>` entry itself
pub fn tablespace_entry(path: &Path) -> Option<u32> {
    let rest = path.to_str()?.strip_prefix(TBLSPC_DIR)?.strip_prefix('/')?;
    rest.parse().ok()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RelFork {
    Main,
    Fsm,
    Vm,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RelFile {
    pub rfn: RelFileNode,
    pub fork: RelFork,
    pub segno: u32,
}

/// `base/<db>/<file>` or `pg_tblspc/<spc>/PG_<major>_<catversion>/<db>/<file>`
pub fn parse_rel_path(path: &Path) -> Option<RelFile> {
    let path = path.to_str()?;
    let (spc_node, rest) = match path.strip_prefix("base/") {
        Some(rest) => (DEFAULT_TABLESPACE_OID, rest),
        None => {
            let (spc, rest) = path
                .strip_prefix(TBLSPC_DIR)?
                .strip_prefix('/')?
                .split_once('/')?;
            let (version, rest) = rest.split_once('/')?;
            if !version.starts_with("PG_") {
                return None;
            }
            (spc.parse().ok()?, rest)
        }
    };
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
    Some(RelFile {
        rfn: RelFileNode {
            spc_node,
            db_node: db.parse().ok()?,
            rel_node: stem.parse().ok()?,
        },
        fork,
        segno,
    })
}

/// Find user relations with main forks under shadow's `base/<db_oid>/`
///
/// Skip initdb filenodes. Catalog routing handles rotated catalog files first
///
/// Not a daemon path: recovery can recreate a file without its pages, so
/// replay eligibility is durable state
/// ([`crate::filter::shadow_relations`]), never inferred from the directory.
/// Here for callers that have no eligibility file to read, such as tests
/// standing in for bootstrap
pub async fn user_relation_filenodes(
    data_dir: &Path,
    db_oid: u32,
) -> io::Result<HashSet<RelFileNode>> {
    let rel_dir = Path::new("base").join(db_oid.to_string());
    let mut out = HashSet::default();
    let mut files = tokio::fs::read_dir(data_dir.join(&rel_dir)).await?;
    while let Some(f) = files.next_entry().await? {
        if let Some(rel) = parse_rel_path(&rel_dir.join(f.file_name()))
            .filter(|r| r.fork == RelFork::Main && r.rfn.rel_node >= FIRST_NORMAL_OBJECT_ID)
        {
            out.insert(rel.rfn);
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
        let rfn = |rel_node| RelFileNode {
            spc_node: DEFAULT_TABLESPACE_OID,
            db_node: 5,
            rel_node,
        };
        assert_eq!(got, vec![rfn(16400), rfn(24000)]);

        assert!(
            user_relation_filenodes(tmp.path(), 7).await.is_err(),
            "missing database directory must return an error",
        );
    }

    #[test]
    fn tablespace_paths_carry_their_tablespace() {
        let file =
            parse_rel_path(Path::new("pg_tblspc/16385/PG_18_202506291/5/16400_vm.2")).unwrap();
        assert_eq!(
            file,
            RelFile {
                rfn: RelFileNode {
                    spc_node: 16385,
                    db_node: 5,
                    rel_node: 16400,
                },
                fork: RelFork::Vm,
                segno: 2,
            }
        );
        for path in [
            "pg_tblspc/16385",
            "pg_tblspc/16385/PG_18_202506291",
            "pg_tblspc/16385/other/5/16400",
            "pg_tblspc/x/PG_18_202506291/5/16400",
            "pg_tblspcs/16385/PG_18_202506291/5/16400",
        ] {
            assert_eq!(parse_rel_path(Path::new(path)), None, "{path}");
        }
    }

    #[test]
    fn tablespace_temp_files_are_system_dirs() {
        assert!(is_system_dir(Path::new(
            "pg_tblspc/16385/PG_18_202506291/pgsql_tmp/pgsql_tmp1.0"
        )));
        assert!(!is_system_dir(Path::new(
            "pg_tblspc/16385/PG_18_202506291/5/16400"
        )));
        assert!(!is_system_dir(Path::new("pg_tblspc/16385")));
        assert!(is_system_dir(Path::new("pgsql_tmp/x")));
    }

    #[test]
    fn tablespace_entry_names_only_the_link() {
        assert_eq!(tablespace_entry(Path::new("pg_tblspc/16385")), Some(16385));
        assert_eq!(tablespace_entry(Path::new("pg_tblspc/16385/PG_18_1")), None);
        assert_eq!(tablespace_entry(Path::new("pg_tblspc")), None);
        assert_eq!(tablespace_entry(Path::new("base/16385")), None);
    }
}
