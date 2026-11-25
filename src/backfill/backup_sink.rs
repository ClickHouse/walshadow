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
//! `CatalogTracker::seed_from_source`. Tablespace, database and filenode
//! together name a relation file, so heaps in user tablespaces route the
//! same way as `base/`.

use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;

use crate::backfill::backup_source::{
    BackupSink, EndInfo, FileAction, FileKind, FileMeta, StartInfo,
};
use crate::backfill::pg_path::{is_system_dir, parse_rel_path};
use crate::schema::FIRST_NORMAL_OBJECT_ID;
use foldhash::HashSet;
use walrus::pg::walparser::RelFileNode;

/// `relfilenode < 16384` is always bootstrap catalog. Rotated-catalog
/// filenodes (`VACUUM FULL` / `REINDEX` on a catalog) land in `whitelist`.
#[derive(Debug, Clone, Default)]
pub struct CatalogFilenodes {
    whitelist: HashSet<RelFileNode>,
}

impl CatalogFilenodes {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&mut self, rfn: RelFileNode) {
        self.whitelist.insert(rfn);
    }

    /// Bootstrap rule (`rel_node < 16384`) or whitelist seed
    pub fn is_catalog(&self, rfn: RelFileNode) -> bool {
        if rfn.rel_node != 0 && rfn.rel_node < FIRST_NORMAL_OBJECT_ID {
            return true;
        }
        self.whitelist.contains(&rfn)
    }

    pub fn len(&self) -> usize {
        self.whitelist.len()
    }

    pub fn is_empty(&self) -> bool {
        self.whitelist.is_empty()
    }
}

impl FromIterator<RelFileNode> for CatalogFilenodes {
    fn from_iter<I: IntoIterator<Item = RelFileNode>>(iter: I) -> Self {
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
        if let Some(f) = parse_rel_path(&meta.path) {
            return if self.catalog_filenodes.is_catalog(f.rfn) {
                DiskAction::Keep
            } else {
                DiskAction::SkipUserHeap
            };
        }
        // global/, pg_xact/, pg_multixact/, pg_filenode.map,
        // tablespace_map, backup_label, pg_control, tablespace dirs,
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
    use crate::backfill::backup_source::testing::expect_tap;
    use crate::backfill::pg_path::{RelFile, RelFork, is_system_dir, parse_rel_path};
    use crate::schema::DEFAULT_TABLESPACE_OID;
    use std::path::{Path, PathBuf};

    fn rfn(spc_node: u32, db_node: u32, rel_node: u32) -> RelFileNode {
        RelFileNode {
            spc_node,
            db_node,
            rel_node,
        }
    }

    #[tokio::test]
    async fn lander_keeps_and_counts_tablespace_symlinks() {
        let lander = DiskLanderSink::new(CatalogFilenodes::new());
        let meta = FileMeta {
            path: PathBuf::from("pg_tblspc/16384"),
            kind: FileKind::Symlink {
                target: PathBuf::from("/srv/ts"),
            },
            ..Default::default()
        };
        assert!(matches!(
            lander.begin(&meta).await.unwrap(),
            FileAction::Keep
        ));
        assert_eq!(lander.stats.kept_symlinks.load(Ordering::Relaxed), 1);
    }

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
    fn parse_rel_path_handles_segments_fsm_vm() {
        let f = |filenode, fork, segno| {
            Some(RelFile {
                rfn: rfn(DEFAULT_TABLESPACE_OID, 5, filenode),
                fork,
                segno,
            })
        };
        assert_eq!(
            parse_rel_path(Path::new("base/5/16400")),
            f(16400, RelFork::Main, 0)
        );
        assert_eq!(
            parse_rel_path(Path::new("base/5/16400.1")),
            f(16400, RelFork::Main, 1)
        );
        assert_eq!(
            parse_rel_path(Path::new("base/5/16400_fsm")),
            f(16400, RelFork::Fsm, 0)
        );
        assert_eq!(
            parse_rel_path(Path::new("base/5/16400_vm.2")),
            f(16400, RelFork::Vm, 2)
        );
        assert_eq!(parse_rel_path(Path::new("base/5/16400_init")), None);
        assert_eq!(parse_rel_path(Path::new("base/5/16400.x")), None);
        assert_eq!(parse_rel_path(Path::new("global/1213")), None);
        assert_eq!(parse_rel_path(Path::new("pg_control")), None);
    }

    #[test]
    fn catalog_filenodes_bootstrap_rule_and_whitelist() {
        let mut c = CatalogFilenodes::new();
        c.insert(rfn(1663, 5, 50000));
        assert!(c.is_catalog(rfn(16385, 5, 1259))); // bootstrap rule
        assert!(c.is_catalog(rfn(1663, 5, 50000))); // whitelist
        assert!(!c.is_catalog(rfn(1663, 5, 60000)));
        // Same database and filenode in another tablespace is another file
        assert!(!c.is_catalog(rfn(16385, 5, 50000)));
        c.insert(rfn(1664, 0, 99999));
        assert!(c.is_catalog(rfn(1664, 0, 99999)));
        assert!(!c.is_catalog(rfn(1663, 7, 99999)));
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
            ..Default::default()
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
                ..Default::default()
            }),
            DiskAction::SkipDenylist,
            "file contents under a denylisted dir stay skipped",
        );
    }

    #[test]
    fn disk_lander_routes_keep_skip_denylist_skip_user_heap() {
        let lander = DiskLanderSink::new(CatalogFilenodes::from_iter([rfn(1663, 5, 50000)]));
        let cases = [
            (
                FileMeta {
                    path: PathBuf::from("base/5/1259"),
                    size: 0,
                    mode: 0,
                    ..Default::default()
                },
                DiskAction::Keep,
            ),
            (
                FileMeta {
                    path: PathBuf::from("base/5/50000"),
                    size: 0,
                    mode: 0,
                    ..Default::default()
                },
                DiskAction::Keep,
            ),
            (
                FileMeta {
                    path: PathBuf::from("base/5/16400"),
                    size: 0,
                    mode: 0,
                    ..Default::default()
                },
                DiskAction::SkipUserHeap,
            ),
            (
                FileMeta {
                    path: PathBuf::from("pg_replslot/0/state"),
                    size: 0,
                    mode: 0,
                    ..Default::default()
                },
                DiskAction::SkipDenylist,
            ),
            (
                FileMeta {
                    path: PathBuf::from("pg_replslot"),
                    size: 0,
                    mode: 0,
                    kind: FileKind::Dir,
                    ..Default::default()
                },
                DiskAction::Keep,
            ),
            (
                FileMeta {
                    path: PathBuf::from("pg_control"),
                    size: 0,
                    mode: 0,
                    ..Default::default()
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
                    ..Default::default()
                },
                DiskAction::Keep,
            ),
            (
                FileMeta {
                    path: PathBuf::from("pg_tblspc/16384/PG_18_202506291/5/16400"),
                    ..Default::default()
                },
                DiskAction::SkipUserHeap,
            ),
            (
                FileMeta {
                    path: PathBuf::from("pg_tblspc/16384/PG_18_202506291/5/1259"),
                    ..Default::default()
                },
                DiskAction::Keep,
            ),
            (
                FileMeta {
                    path: PathBuf::from("pg_tblspc/16384/PG_18_202506291/pgsql_tmp/t1.0"),
                    ..Default::default()
                },
                DiskAction::SkipDenylist,
            ),
        ];
        for (meta, expected) in cases {
            let path = meta.path.display();
            assert_eq!(lander.classify(&meta), expected, "classify({path}) wrong");
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
        async fn chunk(&mut self, bytes: Vec<u8>) -> io::Result<Vec<u8>> {
            self.0.chunks.fetch_add(1, Ordering::Relaxed);
            self.0
                .bytes
                .fetch_add(bytes.len() as u64, Ordering::Relaxed);
            Ok(bytes)
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
            ..Default::default()
        };
        assert!(matches!(
            mux.begin(&file("base/5/1259")).await.unwrap(),
            FileAction::Keep
        ));

        let mut entry = expect_tap(mux.begin(&file("base/5/16400")).await.unwrap());
        entry.chunk(vec![0u8; 1024]).await.unwrap();
        entry.chunk(vec![1u8; 512]).await.unwrap();
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
        c.insert(rfn(1663, 5, 50000));
        c.insert(rfn(1664, 0, 99999));
        assert!(!c.is_empty());
        assert_eq!(c.len(), 2);
        // Re-inserting an existing file is a no-op
        c.insert(rfn(1663, 5, 50000));
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
            ..Default::default()
        };
        mux.begin(&file("base/5/1259")).await.unwrap();
        mux.begin(&file("pg_replslot/0/state")).await.unwrap();

        let stats = mux.lander_stats();
        assert_eq!(ld(&stats.kept_files), 1);
        assert_eq!(ld(&stats.skipped_denylist), 1);
    }
}
