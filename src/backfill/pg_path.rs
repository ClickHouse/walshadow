use std::path::Path;

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
