//! DiskLanderSink + MultiplexSink.
//!
//! The two production-shape sinks composed by the bootstrap
//! orchestrator. Page-walking sink lives in
//! [`crate::backup_page_walk`] so it can pull in the heap decoder
//! without making this module recompile-heavy.
//!
//! ## DiskLanderSink
//!
//! Routes catalog + system files through `FileAction::Keep`. The
//! source impl writes the body to `data_dir/path`. User-heap files
//! return `Skip` when used standalone; the multiplex sink overrides
//! that for user heap to `Tap` when a page-walk sink is composed in.
//!
//! Catalog detection follows walshadow's
//! [`crate::classify::FIRST_NORMAL_OBJECT_ID`] convention: filenodes
//! `< 16384` are bootstrap-rule catalog. Filenodes `>= 16384` come
//! from `pg_class.relfilenode` and may be catalog (rotated via
//! `VACUUM FULL` / `REINDEX`) or user heap. The optional whitelist
//! covers the rotated-catalog case; bootstrap seeds it from
//! `CatalogTracker::seed_from_source`.
//!
//! ## MultiplexSink
//!
//! Composes a DiskLanderSink + a Tap-style sink (typically
//! `PageWalkSink`). Per-file dispatch: catalogs / system files →
//! lander (Keep), user heap → tap (Tap), denylist contents → Skip,
//! denylist dirs themselves → Keep as empty dir.

use std::collections::HashSet;
use std::io;
use std::path::Path;

use crate::backup_source::{
    BackupSink, EndInfo, EntryId, FileAction, FileKind, FileMeta, StartInfo,
};
use crate::classify::FIRST_NORMAL_OBJECT_ID;

/// System-dir denylist mirroring `wal_rs::pg::backup::SYSTEM_DIRS_DENYLIST`.
/// Listed locally so this module has no wal-rs build dependency at
/// the lookup-table level (the wal-rs constant remains the source of
/// truth for the *protocol*-driven filter).
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

/// True iff `path`'s leading component is a denylisted system dir, or
/// matches the `temp_*` pattern PG uses for transient WAL receiver
/// directories.
pub fn is_system_dir(path: &Path) -> bool {
    let head = path
        .components()
        .next()
        .and_then(|c| c.as_os_str().to_str())
        .unwrap_or("");
    SYSTEM_DIRS_DENYLIST.contains(&head) || head.starts_with("temp_")
}

/// Parse `base/<dbid>/<filenode>` and `base/<dbid>/<filenode>.<seg>`
/// (segments past 1 GiB). Returns `None` for paths outside `base/`.
/// `_fsm` / `_vm` suffixes map back to the same filenode for routing
/// purposes — both ride the same heap.
pub fn parse_base_path(path: &Path) -> Option<(u32, u32)> {
    let s = path.to_str()?;
    let rest = s.strip_prefix("base/")?;
    let mut it = rest.splitn(2, '/');
    let db: u32 = it.next()?.parse().ok()?;
    let leaf = it.next()?;
    let stem = leaf.split('.').next()?;
    let stem = stem.strip_suffix("_fsm").unwrap_or(stem);
    let stem = stem.strip_suffix("_vm").unwrap_or(stem);
    let filenode: u32 = stem.parse().ok()?;
    Some((db, filenode))
}

/// Catalog filenode classifier. `relfilenode < 16384` is always a
/// bootstrap catalog. Rotated-catalog filenodes (`VACUUM FULL` /
/// `REINDEX` against a catalog table) land in `whitelist`.
#[derive(Debug, Clone, Default)]
pub struct CatalogFilenodes {
    /// `(db_node, rel_node)` pairs known to be rotated catalogs. A
    /// `db_node == 0` entry matches any database (covers shared
    /// catalogs like `pg_database`).
    whitelist: HashSet<(u32, u32)>,
}

impl CatalogFilenodes {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&mut self, db_node: u32, rel_node: u32) {
        self.whitelist.insert((db_node, rel_node));
    }

    /// True iff this filenode is a catalog. Combines bootstrap rule
    /// (`rel_node < 16384`) with the whitelist seed.
    pub fn is_catalog(&self, db_node: u32, rel_node: u32) -> bool {
        if rel_node != 0 && rel_node < FIRST_NORMAL_OBJECT_ID {
            return true;
        }
        self.whitelist
            .iter()
            .any(|&(d, r)| r == rel_node && (d == 0 || d == db_node))
    }

    pub fn len(&self) -> usize {
        self.whitelist.len()
    }

    pub fn is_empty(&self) -> bool {
        self.whitelist.is_empty()
    }
}

impl FromIterator<(u32, u32)> for CatalogFilenodes {
    fn from_iter<I: IntoIterator<Item = (u32, u32)>>(iter: I) -> Self {
        Self {
            whitelist: iter.into_iter().collect(),
        }
    }
}

/// Catalog + system-file sink. Routes Keep for files that belong on
/// shadow's data_dir; Skip for denylist file contents and user heap;
/// Keep for denylist directory entries themselves (PG recovery
/// refuses to start without them).
pub struct DiskLanderSink {
    pub catalog_filenodes: CatalogFilenodes,
    /// Stats — operator-visible counters. No load-bearing role.
    pub stats: DiskLanderStats,
}

#[derive(Debug, Default, Clone)]
pub struct DiskLanderStats {
    pub kept_files: u64,
    pub kept_dirs: u64,
    pub kept_symlinks: u64,
    pub skipped_denylist: u64,
    pub skipped_user_heap: u64,
}

impl DiskLanderSink {
    pub fn new(catalog_filenodes: CatalogFilenodes) -> Self {
        Self {
            catalog_filenodes,
            stats: DiskLanderStats::default(),
        }
    }

    /// Classification, exposed so the multiplex sink can re-use the
    /// same predicate for its first-pass dispatch.
    pub fn classify(&self, meta: &FileMeta) -> DiskAction {
        if is_system_dir(&meta.path) {
            // Keep the dir entry itself; drop everything under it.
            // The path leading component is the denylist root; if the
            // entry IS that dir, keep it. Files / sub-paths inside
            // get Skip.
            return if matches!(meta.kind, FileKind::Dir) && is_top_level(&meta.path) {
                DiskAction::Keep
            } else {
                DiskAction::SkipDenylist
            };
        }
        if let Some((db, filenode)) = parse_base_path(&meta.path) {
            return if self.catalog_filenodes.is_catalog(db, filenode) {
                DiskAction::Keep
            } else {
                DiskAction::SkipUserHeap
            };
        }
        // global/, pg_xact/, pg_multixact/, pg_filenode.map,
        // tablespace_map, backup_label, pg_control, pg_tblspc symlinks,
        // top-level dirs that aren't denylisted — every catalog
        // prerequisite recovery needs.
        DiskAction::Keep
    }
}

/// DiskLanderSink-only routing result. Distinguishes "skip because
/// denylist" from "skip because user heap" so the multiplex sink can
/// flip the second case to `Tap` when a page-walk sink is composed in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiskAction {
    Keep,
    SkipDenylist,
    SkipUserHeap,
}

impl BackupSink for DiskLanderSink {
    fn begin(&mut self, _entry: EntryId, meta: &FileMeta) -> io::Result<FileAction> {
        let action = match self.classify(meta) {
            DiskAction::Keep => FileAction::Keep,
            DiskAction::SkipDenylist | DiskAction::SkipUserHeap => FileAction::Skip,
        };
        if action == FileAction::Keep {
            match meta.kind {
                FileKind::File => self.stats.kept_files += 1,
                FileKind::Dir => self.stats.kept_dirs += 1,
                FileKind::Symlink { .. } => self.stats.kept_symlinks += 1,
            }
        } else {
            match self.classify(meta) {
                DiskAction::SkipDenylist => self.stats.skipped_denylist += 1,
                DiskAction::SkipUserHeap => self.stats.skipped_user_heap += 1,
                DiskAction::Keep => unreachable!(),
            }
        }
        Ok(action)
    }
    fn chunk(&mut self, _entry: EntryId, _bytes: &[u8]) -> io::Result<()> {
        // DiskLanderSink never returns Tap, so chunk() should never
        // fire. Surface a programming-error path here loudly.
        Err(io::Error::other(
            "DiskLanderSink::chunk called — sink only ever Keeps or Skips",
        ))
    }
    fn end(&mut self, _entry: EntryId) -> io::Result<()> {
        Ok(())
    }
}

/// Multiplex two sinks: a DiskLanderSink (always Keep / Skip) and a
/// Tap-target sink (typically PageWalkSink) for user heap. Per-file
/// dispatch decides the route before the body lands; chunk/end calls
/// route to whichever inner sink begin() chose.
///
/// One pass over the source; both sinks process simultaneously.
pub struct MultiplexSink<T> {
    lander: DiskLanderSink,
    tap: T,
    /// Entries currently routed to the tap. A set, not one flag, so
    /// concurrent entries (object_store fan-out) dispatch `chunk`/`end`
    /// to the right inner sink instead of racing one shared bool.
    tap_entries: HashSet<EntryId>,
}

impl<T: BackupSink> MultiplexSink<T> {
    pub fn new(lander: DiskLanderSink, tap: T) -> Self {
        Self {
            lander,
            tap,
            tap_entries: HashSet::new(),
        }
    }

    pub fn lander_stats(&self) -> &DiskLanderStats {
        &self.lander.stats
    }

    pub fn into_inner(self) -> (DiskLanderSink, T) {
        (self.lander, self.tap)
    }
}

impl<T: BackupSink> BackupSink for MultiplexSink<T> {
    fn start(&mut self, info: &StartInfo) -> io::Result<()> {
        self.lander.start(info)?;
        self.tap.start(info)?;
        Ok(())
    }
    fn begin(&mut self, entry: EntryId, meta: &FileMeta) -> io::Result<FileAction> {
        let action = match self.lander.classify(meta) {
            DiskAction::Keep => {
                self.lander.begin(entry, meta)?;
                FileAction::Keep
            }
            DiskAction::SkipDenylist => {
                self.lander.begin(entry, meta)?;
                FileAction::Skip
            }
            DiskAction::SkipUserHeap => {
                // User heap — flip to Tap if the inner sink accepts.
                // Inner sink's begin() can decline by returning Skip /
                // Keep, in which case we honour that.
                let inner_action = self.tap.begin(entry, meta)?;
                if inner_action == FileAction::Tap {
                    self.tap_entries.insert(entry);
                }
                inner_action
            }
        };
        Ok(action)
    }
    fn chunk(&mut self, entry: EntryId, bytes: &[u8]) -> io::Result<()> {
        if self.tap_entries.contains(&entry) {
            self.tap.chunk(entry, bytes)
        } else {
            // Lander never asks for chunk; defensive
            Err(io::Error::other("MultiplexSink: chunk without active tap"))
        }
    }
    fn end(&mut self, entry: EntryId) -> io::Result<()> {
        if self.tap_entries.remove(&entry) {
            self.tap.end(entry)?;
        } else {
            self.lander.end(entry)?;
        }
        Ok(())
    }
    fn finish(&mut self, info: &EndInfo) -> io::Result<()> {
        self.lander.finish(info)?;
        self.tap.finish(info)?;
        Ok(())
    }
}

fn is_top_level(path: &Path) -> bool {
    path.components().count() == 1
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn is_system_dir_matches_denylist() {
        assert!(is_system_dir(Path::new("pg_replslot")));
        assert!(is_system_dir(Path::new("pg_replslot/0/state")));
        assert!(is_system_dir(Path::new("pg_stat_tmp/foo")));
        assert!(is_system_dir(Path::new("temp_42")));
        assert!(!is_system_dir(Path::new("base/5/16400")));
        assert!(!is_system_dir(Path::new("global/1213")));
        assert!(!is_system_dir(Path::new("pg_control")));
    }

    #[test]
    fn parse_base_path_handles_segments_fsm_vm() {
        assert_eq!(parse_base_path(Path::new("base/5/16400")), Some((5, 16400)));
        assert_eq!(
            parse_base_path(Path::new("base/5/16400.1")),
            Some((5, 16400))
        );
        assert_eq!(
            parse_base_path(Path::new("base/5/16400_fsm")),
            Some((5, 16400))
        );
        assert_eq!(
            parse_base_path(Path::new("base/5/16400_vm")),
            Some((5, 16400))
        );
        assert_eq!(parse_base_path(Path::new("global/1213")), None);
        assert_eq!(parse_base_path(Path::new("pg_control")), None);
    }

    #[test]
    fn catalog_filenodes_bootstrap_rule_and_whitelist() {
        let mut c = CatalogFilenodes::new();
        c.insert(5, 50000);
        assert!(c.is_catalog(5, 1259)); // bootstrap rule
        assert!(c.is_catalog(5, 50000)); // whitelist
        assert!(!c.is_catalog(5, 60000));
        c.insert(0, 99999); // shared catalog
        assert!(c.is_catalog(5, 99999));
        assert!(c.is_catalog(7, 99999));
    }

    #[test]
    fn disk_lander_routes_keep_skip_denylist_skip_user_heap() {
        let lander = DiskLanderSink::new(CatalogFilenodes::from_iter([(5, 50000)]));
        let cases = [
            (
                FileMeta {
                    path: PathBuf::from("base/5/1259"),
                    size: 0,
                    mode: 0,
                    kind: FileKind::File,
                },
                DiskAction::Keep,
            ),
            (
                FileMeta {
                    path: PathBuf::from("base/5/50000"),
                    size: 0,
                    mode: 0,
                    kind: FileKind::File,
                },
                DiskAction::Keep,
            ),
            (
                FileMeta {
                    path: PathBuf::from("base/5/16400"),
                    size: 0,
                    mode: 0,
                    kind: FileKind::File,
                },
                DiskAction::SkipUserHeap,
            ),
            (
                FileMeta {
                    path: PathBuf::from("pg_replslot/0/state"),
                    size: 0,
                    mode: 0,
                    kind: FileKind::File,
                },
                DiskAction::SkipDenylist,
            ),
            (
                FileMeta {
                    path: PathBuf::from("pg_replslot"),
                    size: 0,
                    mode: 0,
                    kind: FileKind::Dir,
                },
                DiskAction::Keep,
            ),
            (
                FileMeta {
                    path: PathBuf::from("pg_control"),
                    size: 0,
                    mode: 0,
                    kind: FileKind::File,
                },
                DiskAction::Keep,
            ),
            (
                FileMeta {
                    path: PathBuf::from("pg_tblspc/16384"),
                    size: 0,
                    mode: 0,
                    kind: FileKind::Symlink {
                        target: PathBuf::from("/srv/ts/a"),
                    },
                },
                DiskAction::Keep,
            ),
        ];
        for (meta, expected) in cases {
            assert_eq!(
                lander.classify(&meta),
                expected,
                "classify({}) wrong",
                meta.path.display()
            );
        }
    }

    /// Minimal tap that counts every begin / chunk / end and always
    /// returns Tap for user-heap entries it's offered. Used to exercise
    /// MultiplexSink without pulling in the full page walker.
    #[derive(Debug, Default)]
    struct CountingTap {
        begins: u64,
        chunks: u64,
        ends: u64,
        bytes: u64,
    }
    impl BackupSink for CountingTap {
        fn begin(&mut self, _entry: EntryId, _meta: &FileMeta) -> io::Result<FileAction> {
            self.begins += 1;
            Ok(FileAction::Tap)
        }
        fn chunk(&mut self, _entry: EntryId, bytes: &[u8]) -> io::Result<()> {
            self.chunks += 1;
            self.bytes += bytes.len() as u64;
            Ok(())
        }
        fn end(&mut self, _entry: EntryId) -> io::Result<()> {
            self.ends += 1;
            Ok(())
        }
    }

    #[test]
    fn multiplex_sink_routes_user_heap_to_tap() {
        let lander = DiskLanderSink::new(CatalogFilenodes::new());
        let tap = CountingTap::default();
        let mut mux = MultiplexSink::new(lander, tap);

        // catalog → Keep, no tap
        let m = FileMeta {
            path: PathBuf::from("base/5/1259"),
            size: 0,
            mode: 0,
            kind: FileKind::File,
        };
        assert_eq!(mux.begin(EntryId(0), &m).unwrap(), FileAction::Keep);
        mux.end(EntryId(0)).unwrap();

        // user heap → Tap
        let m = FileMeta {
            path: PathBuf::from("base/5/16400"),
            size: 0,
            mode: 0,
            kind: FileKind::File,
        };
        assert_eq!(mux.begin(EntryId(1), &m).unwrap(), FileAction::Tap);
        mux.chunk(EntryId(1), &[0u8; 1024]).unwrap();
        mux.chunk(EntryId(1), &[1u8; 512]).unwrap();
        mux.end(EntryId(1)).unwrap();

        // denylist file → Skip, no tap
        let m = FileMeta {
            path: PathBuf::from("pg_replslot/0/state"),
            size: 0,
            mode: 0,
            kind: FileKind::File,
        };
        assert_eq!(mux.begin(EntryId(2), &m).unwrap(), FileAction::Skip);
        mux.end(EntryId(2)).unwrap();

        let (lander, tap) = mux.into_inner();
        assert_eq!(tap.begins, 1);
        assert_eq!(tap.chunks, 2);
        assert_eq!(tap.ends, 1);
        assert_eq!(tap.bytes, 1536);
        assert_eq!(lander.stats.kept_files, 1);
        // User heap was delegated to the tap; lander never `begin`'d it,
        // so its skipped_user_heap counter stays at zero. The tap's
        // begins == 1 is the operator-visible signal for "user heap
        // routed away from disk".
        assert_eq!(lander.stats.skipped_user_heap, 0);
        assert_eq!(lander.stats.skipped_denylist, 1);
    }

    #[test]
    fn catalog_filenodes_len_and_is_empty() {
        let mut c = CatalogFilenodes::new();
        assert!(c.is_empty());
        assert_eq!(c.len(), 0);
        c.insert(5, 50000);
        c.insert(0, 99999);
        assert!(!c.is_empty());
        assert_eq!(c.len(), 2);
        // HashSet semantics: re-inserting an existing pair is a no-op.
        c.insert(5, 50000);
        assert_eq!(c.len(), 2);
    }

    #[test]
    fn multiplex_lander_stats_exposes_disk_counters() {
        let lander = DiskLanderSink::new(CatalogFilenodes::new());
        let mut mux = MultiplexSink::new(lander, CountingTap::default());
        // catalog file → Keep → kept_files
        let m = FileMeta {
            path: PathBuf::from("base/5/1259"),
            size: 0,
            mode: 0,
            kind: FileKind::File,
        };
        assert_eq!(mux.begin(EntryId(0), &m).unwrap(), FileAction::Keep);
        mux.end(EntryId(0)).unwrap();
        // denylist file → Skip → skipped_denylist
        let m = FileMeta {
            path: PathBuf::from("pg_replslot/0/state"),
            size: 0,
            mode: 0,
            kind: FileKind::File,
        };
        assert_eq!(mux.begin(EntryId(1), &m).unwrap(), FileAction::Skip);
        mux.end(EntryId(1)).unwrap();

        let stats = mux.lander_stats();
        assert_eq!(stats.kept_files, 1);
        assert_eq!(stats.skipped_denylist, 1);
    }
}
