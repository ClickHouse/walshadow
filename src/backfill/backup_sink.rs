//! DiskLanderSink + MultiplexSink, the two production-shape sinks the
//! bootstrap orchestrator composes. Page-walking sink lives in
//! [`crate::backfill::backup_page_walk`] to keep the heap-decoder dependency out
//! of this module.
//!
//! Catalog detection follows
//! [`crate::filter::classify::FIRST_NORMAL_OBJECT_ID`]: filenodes `< 16384` are
//! bootstrap-rule catalog. `>= 16384` come from `pg_class.relfilenode`
//! and may be catalog (rotated via `VACUUM FULL` / `REINDEX`) or user
//! heap; the whitelist covers the rotated-catalog case, seeded from
//! `CatalogTracker::seed_from_source`.

use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;

use crate::backfill::backup_source::{
    BackupSink, EndInfo, FileAction, FileKind, FileMeta, StartInfo,
};
use crate::backfill::pg_path::{is_system_dir, parse_base_path};
use crate::schema::FIRST_NORMAL_OBJECT_ID;
use ahash::HashSet;

/// `relfilenode < 16384` is always bootstrap catalog. Rotated-catalog
/// filenodes (`VACUUM FULL` / `REINDEX` on a catalog) land in `whitelist`.
#[derive(Debug, Clone, Default)]
pub struct CatalogFilenodes {
    /// `(db_node, rel_node)` rotated catalogs; `db_node == 0` matches
    /// any database (shared catalogs like `pg_database`)
    whitelist: HashSet<(u32, u32)>,
}

impl CatalogFilenodes {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&mut self, db_node: u32, rel_node: u32) {
        self.whitelist.insert((db_node, rel_node));
    }

    /// Bootstrap rule (`rel_node < 16384`) or whitelist seed
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

/// Keeps catalog + system files for shadow's data_dir; Skips denylist
/// file contents and user heap; Keeps denylist dir entries themselves
/// (PG recovery refuses to start without them).
pub struct DiskLanderSink {
    pub catalog_filenodes: CatalogFilenodes,
    pub stats: Arc<DiskLanderStats>,
}

crate::atomic_stats! {
    pub struct DiskLanderStats {
        pub kept_files,
        pub kept_dirs,
        pub kept_symlinks,
        pub skipped_denylist,
        pub skipped_user_heap,
    }
}

impl DiskLanderSink {
    pub fn new(catalog_filenodes: CatalogFilenodes) -> Self {
        Self {
            catalog_filenodes,
            stats: Arc::new(DiskLanderStats::default()),
        }
    }

    /// Exposed so MultiplexSink reuses the predicate for first-pass dispatch
    pub fn classify(&self, meta: &FileMeta) -> DiskAction {
        if is_system_dir(&meta.path) {
            // Keep the dir tree (PG requires nested ones like
            // pg_logical/snapshots to exist), skip only file contents under it.
            return if matches!(meta.kind, FileKind::Dir) {
                DiskAction::Keep
            } else {
                DiskAction::SkipDenylist
            };
        }
        if let Some(f) = parse_base_path(&meta.path) {
            return if self.catalog_filenodes.is_catalog(f.db, f.filenode) {
                DiskAction::Keep
            } else {
                DiskAction::SkipUserHeap
            };
        }
        // global/, pg_xact/, pg_multixact/, pg_filenode.map,
        // tablespace_map, backup_label, pg_control, pg_tblspc symlinks,
        // non-denylisted top-level dirs: every recovery prerequisite
        DiskAction::Keep
    }
}

/// Distinguishes skip-denylist from skip-user-heap so MultiplexSink can
/// flip the latter to a tap when a page-walk sink is composed in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiskAction {
    Keep,
    SkipDenylist,
    SkipUserHeap,
}

impl DiskLanderSink {
    /// Count one routing decision. Split out so `MultiplexSink` records the
    /// lander's view of an entry it hands to the tap instead
    fn record(&self, action: DiskAction, meta: &FileMeta) {
        let bump = |c: &AtomicU64| {
            c.fetch_add(1, Ordering::Relaxed);
        };
        match action {
            DiskAction::Keep => match meta.kind {
                FileKind::File => bump(&self.stats.kept_files),
                FileKind::Dir => bump(&self.stats.kept_dirs),
                FileKind::Symlink { .. } => bump(&self.stats.kept_symlinks),
            },
            DiskAction::SkipDenylist => bump(&self.stats.skipped_denylist),
            DiskAction::SkipUserHeap => bump(&self.stats.skipped_user_heap),
        }
    }
}

#[async_trait]
impl BackupSink for DiskLanderSink {
    async fn begin(&self, meta: &FileMeta) -> io::Result<FileAction> {
        let action = self.classify(meta);
        self.record(action, meta);
        Ok(match action {
            DiskAction::Keep => FileAction::Keep,
            DiskAction::SkipDenylist | DiskAction::SkipUserHeap => FileAction::Skip,
        })
    }
}

/// Routes a DiskLanderSink (Keep / Skip) and a tap-target sink
/// (typically PageWalkSink) over one source pass. Pure router: the tap's
/// own `EntrySink` carries the body, so nothing here is on the hot path.
pub struct MultiplexSink<T> {
    lander: DiskLanderSink,
    tap: T,
}

impl<T: BackupSink> MultiplexSink<T> {
    pub fn new(lander: DiskLanderSink, tap: T) -> Self {
        Self { lander, tap }
    }

    #[cfg(test)]
    pub fn lander_stats(&self) -> &Arc<DiskLanderStats> {
        &self.lander.stats
    }

    #[cfg(test)]
    pub fn into_inner(self) -> (DiskLanderSink, T) {
        (self.lander, self.tap)
    }
}

#[async_trait]
impl<T: BackupSink> BackupSink for MultiplexSink<T> {
    async fn start(&self, info: &StartInfo) -> io::Result<()> {
        self.lander.start(info).await?;
        self.tap.start(info).await?;
        Ok(())
    }
    async fn begin(&self, meta: &FileMeta) -> io::Result<FileAction> {
        let disk = self.lander.classify(meta);
        if disk != DiskAction::SkipUserHeap {
            return self.lander.begin(meta).await;
        }
        // Hand it to the tap; honour its decline (Skip / Keep) otherwise,
        // counting the lander's own verdict only when it keeps the entry
        let inner = self.tap.begin(meta).await?;
        if !inner.is_tap() {
            self.lander.record(disk, meta);
        }
        Ok(inner)
    }
    async fn finish(&self, info: &EndInfo) -> io::Result<()> {
        self.lander.finish(info).await?;
        self.tap.finish(info).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backfill::pg_path::{BaseRelFile, RelFork, is_system_dir, parse_base_path};
    use std::path::{Path, PathBuf};

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
        let f = |filenode, fork, segno| {
            Some(BaseRelFile {
                db: 5,
                filenode,
                fork,
                segno,
            })
        };
        assert_eq!(
            parse_base_path(Path::new("base/5/16400")),
            f(16400, RelFork::Main, 0)
        );
        assert_eq!(
            parse_base_path(Path::new("base/5/16400.1")),
            f(16400, RelFork::Main, 1)
        );
        assert_eq!(
            parse_base_path(Path::new("base/5/16400_fsm")),
            f(16400, RelFork::Fsm, 0)
        );
        assert_eq!(
            parse_base_path(Path::new("base/5/16400_vm.2")),
            f(16400, RelFork::Vm, 2)
        );
        assert_eq!(parse_base_path(Path::new("base/5/16400_init")), None);
        assert_eq!(parse_base_path(Path::new("base/5/16400.x")), None);
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
        c.insert(0, 99999); // db_node 0 = shared catalog
        assert!(c.is_catalog(5, 99999));
        assert!(c.is_catalog(7, 99999));
    }

    #[test]
    fn disk_lander_keeps_required_nested_denylist_dirs() {
        // PG opens pg_logical/snapshots (and mappings) at the first restartpoint
        // and ERRORs if absent, so the landing must keep these nested dir entries
        // — not just the top-level pg_logical/. Their *file* contents stay skipped.
        let lander = DiskLanderSink::new(CatalogFilenodes::new());
        let dir = |p: &str| FileMeta {
            path: PathBuf::from(p),
            size: 0,
            mode: 0,
            kind: FileKind::Dir,
        };
        assert_eq!(lander.classify(&dir("pg_logical")), DiskAction::Keep);
        assert_eq!(
            lander.classify(&dir("pg_logical/snapshots")),
            DiskAction::Keep,
            "pg_logical/snapshots dir dropped -> shadow PG fails at restartpoint",
        );
        assert_eq!(
            lander.classify(&dir("pg_logical/mappings")),
            DiskAction::Keep,
        );
        assert_eq!(
            lander.classify(&FileMeta {
                path: PathBuf::from("pg_logical/snapshots/0-1A2B3C.snap"),
                size: 0,
                mode: 0,
                kind: FileKind::File,
            }),
            DiskAction::SkipDenylist,
            "file contents under a denylisted dir stay skipped",
        );
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

    fn ld(a: &AtomicU64) -> u64 {
        a.load(Ordering::Relaxed)
    }

    /// Counts begin / chunk / end, always taps. Exercises MultiplexSink
    /// without the full page walker.
    #[derive(Debug, Default)]
    struct TapCounts {
        begins: AtomicU64,
        chunks: AtomicU64,
        ends: AtomicU64,
        bytes: AtomicU64,
    }

    struct CountingTap(Arc<TapCounts>);

    #[async_trait]
    impl BackupSink for CountingTap {
        async fn begin(&self, _meta: &FileMeta) -> io::Result<FileAction> {
            self.0.begins.fetch_add(1, Ordering::Relaxed);
            Ok(FileAction::Tap(Box::new(CountingEntry(self.0.clone()))))
        }
    }

    struct CountingEntry(Arc<TapCounts>);

    #[async_trait]
    impl crate::backfill::backup_source::EntrySink for CountingEntry {
        async fn chunk(&mut self, bytes: &[u8]) -> io::Result<()> {
            self.0.chunks.fetch_add(1, Ordering::Relaxed);
            self.0
                .bytes
                .fetch_add(bytes.len() as u64, Ordering::Relaxed);
            Ok(())
        }
        async fn end(self: Box<Self>) -> io::Result<()> {
            self.0.ends.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }
    }

    #[tokio::test]
    async fn multiplex_sink_routes_user_heap_to_tap() {
        let lander = DiskLanderSink::new(CatalogFilenodes::new());
        let counts = Arc::new(TapCounts::default());
        let mux = MultiplexSink::new(lander, CountingTap(counts.clone()));

        let file = |p: &str| FileMeta {
            path: PathBuf::from(p),
            size: 0,
            mode: 0,
            kind: FileKind::File,
        };
        assert!(matches!(
            mux.begin(&file("base/5/1259")).await.unwrap(),
            FileAction::Keep
        ));

        let FileAction::Tap(mut entry) = mux.begin(&file("base/5/16400")).await.unwrap() else {
            panic!("user heap must tap");
        };
        entry.chunk(&[0u8; 1024]).await.unwrap();
        entry.chunk(&[1u8; 512]).await.unwrap();
        entry.end().await.unwrap();

        assert!(matches!(
            mux.begin(&file("pg_replslot/0/state")).await.unwrap(),
            FileAction::Skip
        ));

        let (lander, _) = mux.into_inner();
        assert_eq!(ld(&counts.begins), 1);
        assert_eq!(ld(&counts.chunks), 2);
        assert_eq!(ld(&counts.ends), 1);
        assert_eq!(ld(&counts.bytes), 1536);
        assert_eq!(ld(&lander.stats.kept_files), 1);
        // User heap delegated to tap, so the lander's own skip counter
        // stays 0. `counts.begins == 1` is the operator-visible
        // "routed away from disk" signal.
        assert_eq!(ld(&lander.stats.skipped_user_heap), 0);
        assert_eq!(ld(&lander.stats.skipped_denylist), 1);
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
        // Re-inserting an existing pair is a no-op
        c.insert(5, 50000);
        assert_eq!(c.len(), 2);
    }

    #[tokio::test]
    async fn multiplex_lander_stats_exposes_disk_counters() {
        let lander = DiskLanderSink::new(CatalogFilenodes::new());
        let mux = MultiplexSink::new(lander, CountingTap(Arc::default()));
        let file = |p: &str| FileMeta {
            path: PathBuf::from(p),
            size: 0,
            mode: 0,
            kind: FileKind::File,
        };
        mux.begin(&file("base/5/1259")).await.unwrap();
        mux.begin(&file("pg_replslot/0/state")).await.unwrap();

        let stats = mux.lander_stats();
        assert_eq!(ld(&stats.kept_files), 1);
        assert_eq!(ld(&stats.skipped_denylist), 1);
    }
}
