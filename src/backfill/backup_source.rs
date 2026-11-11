//! File-streaming backup-source trait. A base backup is a stream of
//! files with cluster-relative paths, regardless of wire encoding.
//! See [architecture/bootstrap.md](../../architecture/bootstrap.md).
//!
//! Production impls: [`crate::backfill::backup_source_direct::DirectSource`]
//! (replication protocol) and
//! [`crate::backfill::backup_source_object_store::ObjectStoreSource`] (wal-g
//! layout via `DynStorage`).
//!
//! All I/O is async on wal-rus's `tarball`; no `spawn_blocking`.
//!
//! ## Contracts every source guarantees
//!
//! 1. `start()` fires before any `begin()`, carrying start_lsn,
//!    timeline, user tablespace list.
//! 2. Tablespace contents emit under `pg_tblspc/<oid>/`; a tablespace
//!    symlink, where one is sent, emits as `FileKind::Symlink` before any
//!    file under its subtree.
//! 3. `pg_control` emits last; PG's BASE_BACKUP protocol and wal-rus's
//!    `list_tar_parts` both honour this, future impls must too.
//! 4. `finish()` fires after the last `end()`, carrying end_lsn.
//! 5. Paths are cluster-relative, sanitised against `..` / absolute-root
//!    traversal at the source impl boundary.

use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Instant;

use async_trait::async_trait;
use tokio::io::AsyncRead;

use walrus::pg::backup::tarball::{self, Kind};
use walrus::pg::replication::base_backup::Tablespace;

crate::atomic_stats! {
    /// Bootstrap pump stage attribution, shared from the orchestrator down
    /// through the source. Measures what the pump hands over, not what a
    /// sink made of it: entry counts live on the sinks' own stats
    pub struct PumpStats {
        /// Body bytes handed to a `Tap` entry
        pub bytes_tapped,
        /// Inside `EntrySink::chunk`: page framing, decode, channel send.
        /// Against `page_walk.decode_nanos` this is the tap's overhead
        pub sink_chunk_nanos,
        pub parts_total,
        pub parts_done,
        /// Recorded complete by an earlier attempt, never fetched
        pub parts_skipped,
    }
}

/// Filesystem-object kind. Tar-driven sources translate tar entry types
/// here; the trait does not expose tar.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum FileKind {
    #[default]
    File,
    Dir,
    /// `target` is the resolved path; PG tablespace symlinks carry
    /// absolute filesystem paths
    Symlink {
        target: PathBuf,
    },
}

/// Cluster-relative path examples:
///
/// - `base/5/16400` user heap file
/// - `base/5/1259` `pg_class` heap (catalog, OID 1259)
/// - `global/1213` shared catalog
/// - `pg_control` controlfile
/// - `pg_tblspc/16384` tablespace symlink
/// - `pg_xact/0000` transaction-status file
#[derive(Debug, Clone, Default)]
pub struct FileMeta {
    pub path: PathBuf,
    pub size: u64,
    pub mode: u32,
    pub kind: FileKind,
    /// Archive object this entry came out of, for sources that fetch in
    /// parts. A resumed walk skips whole parts by this key
    pub part: Option<Arc<str>>,
}

/// Sink routing decision per file at `begin()`.
pub enum FileAction {
    /// Source writes body / materializes dir or symlink under
    /// `data_dir`; no body callbacks
    Keep,
    /// Source drains body unread; no land, no body callbacks
    Skip,
    /// Source streams the body into this owned sink, then drops it.
    /// Owned rather than a shared-state decision so concurrent entries
    /// need no lock on the body path. Dir / Symlink chunk zero times
    Tap(Box<dyn EntrySink>),
}

impl FileAction {
    pub fn is_tap(&self) -> bool {
        matches!(self, FileAction::Tap(_))
    }
}

impl std::fmt::Debug for FileAction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            FileAction::Keep => "Keep",
            FileAction::Skip => "Skip",
            FileAction::Tap(_) => "Tap",
        })
    }
}

/// One `Tap` entry's body consumer. Owned by the source task driving that
/// entry, so nothing here contends with another entry: whatever state a
/// page walk or SLRU accumulator needs lives in the impl, and shared
/// state it does touch (catalog map, counters, channel sender) is
/// already `Sync`.
#[async_trait]
pub trait EntrySink: Send {
    /// Bytes per `chunk` the source reads straight into the sink's buffer
    fn chunk_len(&self) -> usize {
        256 * 1024
    }

    /// Body bytes in file order, `chunk_len` long apart from the last.
    /// Returns a buffer for the source to refill, its length kept so the
    /// refill skips zeroing. Awaiting here backpressures the source
    async fn chunk(&mut self, bytes: Vec<u8>) -> io::Result<Vec<u8>>;

    /// Fires once, after the last `chunk`
    async fn end(self: Box<Self>) -> io::Result<()>;
}

/// Mirrors wal-rus `pg::replication::base_backup::StartInfo` so callers
/// wired to wal-rus types don't translate.
#[derive(Debug, Clone)]
pub struct StartInfo {
    pub start_lsn: u64,
    pub timeline: u32,
    pub tablespaces: Vec<Tablespace>,
}

/// Mirrors wal-rus's `EndInfo`.
#[derive(Debug, Clone)]
pub struct EndInfo {
    pub end_lsn: u64,
    pub timeline: u32,
}

/// Per-file router, driven by [`BackupSource::run`]: `start` once, then
/// `begin` per file, then `finish` once. A `Tap` decision hands back its
/// own [`EntrySink`], so the body path never re-enters here.
///
/// `&self` throughout: routing reads immutable state and bumps atomics, so
/// parallel source impls share one `Arc` with no lock. A source that holds
/// a lock across a body read serializes every other entry behind it,
/// which is what the owned entry sink exists to avoid.
#[async_trait]
pub trait BackupSink: Send + Sync {
    async fn start(&self, _info: &StartInfo) -> io::Result<()> {
        Ok(())
    }

    /// Must be cheap; per-file dispatch is the hot path
    async fn begin(&self, meta: &FileMeta) -> io::Result<FileAction>;

    /// Parts a resumed walk need not fetch. Default takes every part
    async fn want_part(&self, _key: &str) -> bool {
        true
    }

    /// Every entry of `key` has been dispatched and its bodies streamed
    async fn part_done(&self, _key: &str) {}

    async fn finish(&self, _info: &EndInfo) -> io::Result<()> {
        Ok(())
    }
}

/// Everything a tar-part driver needs behind one `Arc`, so a parallel
/// source hands its workers a clone instead of four arguments. `data_dir`
/// receives `Keep`d bodies.
pub struct PumpTarget {
    pub data_dir: PathBuf,
    pub sink: Arc<dyn BackupSink>,
    pub stats: Arc<PumpStats>,
    pub part: Option<Arc<str>>,
    /// Cluster-relative directory an archive's entries sit under. A
    /// replication-protocol tablespace archive is rooted at the tablespace
    pub root: Option<PathBuf>,
}

impl PumpTarget {
    pub fn new(data_dir: PathBuf, sink: Arc<dyn BackupSink>, stats: Arc<PumpStats>) -> Self {
        Self {
            data_dir,
            sink,
            stats,
            part: None,
            root: None,
        }
    }

    /// Tag every entry of one archive object with the key it came from
    pub fn for_part(&self, key: &str) -> Self {
        Self {
            data_dir: self.data_dir.clone(),
            sink: self.sink.clone(),
            stats: self.stats.clone(),
            part: Some(Arc::from(key)),
            root: self.root.clone(),
        }
    }

    /// Land one tablespace's archive where an object-store backup carries
    /// it, `pg_tblspc/<oid>/`, rather than over the data dir root
    pub fn for_tablespace(&self, oid: u32) -> Self {
        Self {
            data_dir: self.data_dir.clone(),
            sink: self.sink.clone(),
            stats: self.stats.clone(),
            part: self.part.clone(),
            root: Some(crate::backfill::shadow_tablespaces::landing_root(oid)),
        }
    }
}

#[async_trait]
pub trait BackupSource: Send {
    async fn run(
        self: Box<Self>,
        data_dir: PathBuf,
        sink: Arc<dyn BackupSink>,
        stats: Arc<PumpStats>,
    ) -> anyhow::Result<(StartInfo, EndInfo)>;
}

// Shared helpers: tar->file translation, async disk-land writer

/// Translate one tar entry into a `FileMeta`. `None` for entries
/// the source ignores (absolute paths, `..` traversal, hard links; PG
/// basebackup emits none). Path rebuilt component-by-component skipping
/// `..` / `/` so callers can't escape `data_dir` on write.
pub(crate) fn tar_entry_meta(entry: &tarball::Entry) -> io::Result<Option<FileMeta>> {
    use std::path::Component;

    let mut rel = PathBuf::new();
    for c in Path::new(&entry.path).components() {
        match c {
            Component::Prefix(..) | Component::RootDir | Component::CurDir => continue,
            Component::ParentDir => return Ok(None),
            Component::Normal(p) => rel.push(p),
        }
    }
    if rel.as_os_str().is_empty() {
        return Ok(None);
    }
    let kind = match entry.kind {
        Kind::Directory => FileKind::Dir,
        Kind::SymbolicLink if entry.link.is_empty() => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "symlink without target",
            ));
        }
        Kind::SymbolicLink => FileKind::Symlink {
            target: PathBuf::from(&entry.link),
        },
        Kind::Regular | Kind::HardLink => FileKind::File,
        _ => return Ok(None),
    };
    Ok(Some(FileMeta {
        path: rel,
        size: entry.size,
        mode: entry.mode,
        kind,
        part: None,
    }))
}

/// Drain one tar archive against a sink, emitting per-entry
/// callbacks. Called by `DirectSource` (per `BackupEvent::Archive.body`)
/// and `ObjectStoreSource` (per fetched tar part).
pub async fn pump_tar_to_sink<R>(
    archive: &mut tarball::Reader<R>,
    target: &PumpTarget,
) -> io::Result<()>
where
    R: AsyncRead + Unpin + Send,
{
    while let Some((entry, mut body)) = archive.next().await? {
        let Some(mut meta) = tar_entry_meta(&entry)? else {
            continue;
        };
        meta.part = target.part.clone();
        if let Some(root) = &target.root {
            meta.path = root.join(&meta.path);
        }
        pump_entry(&mut body, &meta, target).await?;
    }
    Ok(())
}

/// One tar entry through the sink. Skipped bodies go unread, the reader
/// drains them on its next entry
async fn pump_entry<R>(
    body: &mut tarball::Body<'_, R>,
    meta: &FileMeta,
    target: &PumpTarget,
) -> io::Result<()>
where
    R: AsyncRead + Unpin + Send,
{
    match target.sink.begin(meta).await? {
        FileAction::Keep => write_kept(body, meta, &target.data_dir).await,
        FileAction::Skip => Ok(()),
        FileAction::Tap(entry) => stream_to_entry(body, entry, &target.stats).await,
    }
}

async fn stream_to_entry<R>(
    body: &mut tarball::Body<'_, R>,
    mut entry: Box<dyn EntrySink>,
    stats: &PumpStats,
) -> io::Result<()>
where
    R: AsyncRead + Unpin + Send,
{
    let mut buf = Vec::new();
    while body.next_chunk(&mut buf, entry.chunk_len()).await? {
        stats
            .bytes_tapped
            .fetch_add(buf.len() as u64, Ordering::Relaxed);
        // Awaiting here parks only this entry's read, and through it this
        // part's fetch. Other parts keep draining
        let entered = Instant::now();
        buf = entry.chunk(buf).await?;
        stats
            .sink_chunk_nanos
            .fetch_add(entered.elapsed().as_nanos() as u64, Ordering::Relaxed);
    }
    entry.end().await
}

async fn write_kept<R>(
    body: &mut tarball::Body<'_, R>,
    meta: &FileMeta,
    data_dir: &Path,
) -> io::Result<()>
where
    R: AsyncRead + Unpin + Send,
{
    let target = data_dir.join(&meta.path);
    if let Some(parent) = target.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    match &meta.kind {
        FileKind::Dir => {
            match tokio::fs::create_dir(&target).await {
                Ok(()) => Ok(()),
                Err(e) if e.kind() == io::ErrorKind::AlreadyExists => Ok(()),
                Err(e) => Err(e),
            }?;
        }
        FileKind::Symlink {
            target: link_target,
        } => {
            #[cfg(unix)]
            {
                let _ = tokio::fs::remove_file(&target).await;
                tokio::fs::symlink(link_target, &target).await?;
            }
            #[cfg(not(unix))]
            {
                let _ = link_target;
                return Err(io::Error::other("symlink restore requires unix"));
            }
        }
        FileKind::File => {
            let mut f = tokio::fs::OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&target)
                .await?;
            tokio::io::copy_buf(body, &mut f).await?;
            f.sync_data().await?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                let _ =
                    tokio::fs::set_permissions(&target, std::fs::Permissions::from_mode(meta.mode))
                        .await;
            }
        }
    }
    Ok(())
}

#[cfg(test)]
pub(crate) mod testing {
    //! Helpers reused across crate tests.

    use walrus::pg::backup::tarball::{Entry, Kind, Writer};

    use super::{EntrySink, FileAction};

    #[track_caller]
    pub fn expect_tap(action: FileAction) -> Box<dyn EntrySink> {
        assert_eq!(format!("{action:?}"), "Tap");
        let FileAction::Tap(e) = action else { panic!() };
        e
    }

    /// In-memory tar roughly mirroring PG's BASE_BACKUP layout: empty
    /// pg_replslot dir + denylist file inside, global/ catalog,
    /// base/<db>/<catalog>, base/<db>/<heap>, pg_control last.
    pub async fn build_synthetic_tar() -> Vec<u8> {
        let mut w = Writer::new(Vec::new());
        let dir = Entry::new("pg_replslot", Kind::Directory, 0, 0o700, 0);
        w.append(&dir, tokio::io::empty()).await.unwrap();
        for (name, body) in [
            ("pg_replslot/0/state", &b"REPLSLOT"[..]),
            ("global/1213", b"global-catalog-bytes"),
            ("base/5/1259", b"pg_class-bytes"),
            ("base/5/16400", &[0xAB; 8192]),
            ("pg_control", b"pg-control-bytes"),
        ] {
            let e = Entry::new(name, Kind::Regular, body.len() as u64, 0o600, 0);
            w.append(&e, body).await.unwrap();
        }
        w.finish().await.unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Collects every callback into `events` so tests assert on the
    /// exact sequence; `tapped` holds captured Tap bodies per file.
    #[derive(Debug, Default)]
    pub(crate) struct RecordingSink {
        pub events: std::sync::Mutex<Vec<Event>>,
        pub tapped: std::sync::Mutex<Vec<(PathBuf, Vec<u8>)>>,
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    pub(crate) enum Event {
        Begin { path: PathBuf, action: &'static str },
        Chunk { len: usize },
        End { path: PathBuf },
    }

    /// Owned per-entry recorder, so the sink itself stays lock-free on the
    /// body path exactly as production sinks do
    struct RecordingEntry {
        sink: Arc<RecordingSink>,
        path: PathBuf,
        body: Vec<u8>,
    }

    #[async_trait]
    impl EntrySink for RecordingEntry {
        async fn chunk(&mut self, bytes: Vec<u8>) -> io::Result<Vec<u8>> {
            self.sink
                .events
                .lock()
                .unwrap()
                .push(Event::Chunk { len: bytes.len() });
            self.body.extend_from_slice(&bytes);
            Ok(bytes)
        }
        async fn end(self: Box<Self>) -> io::Result<()> {
            let RecordingEntry { sink, path, body } = *self;
            sink.tapped.lock().unwrap().push((path.clone(), body));
            sink.events.lock().unwrap().push(Event::End { path });
            Ok(())
        }
    }

    #[async_trait]
    impl BackupSink for Arc<RecordingSink> {
        async fn begin(&self, meta: &FileMeta) -> io::Result<FileAction> {
            let s = meta.path.to_string_lossy();
            let action = if s.starts_with("pg_replslot/") {
                if matches!(meta.kind, FileKind::Dir) {
                    FileAction::Keep
                } else {
                    FileAction::Skip
                }
            } else if s == "base/5/16400" {
                FileAction::Tap(Box::new(RecordingEntry {
                    sink: self.clone(),
                    path: meta.path.clone(),
                    body: Vec::new(),
                }))
            } else {
                FileAction::Keep
            };
            self.events.lock().unwrap().push(Event::Begin {
                path: meta.path.clone(),
                action: match action {
                    FileAction::Keep => "Keep",
                    FileAction::Skip => "Skip",
                    FileAction::Tap(_) => "Tap",
                },
            });
            if !action.is_tap() {
                self.events.lock().unwrap().push(Event::End {
                    path: meta.path.clone(),
                });
            }
            Ok(action)
        }
    }

    async fn tar_metas(entries: &[(tarball::Entry, &[u8])]) -> Vec<Option<FileMeta>> {
        let mut w = tarball::Writer::new(Vec::new());
        for (e, body) in entries {
            w.append(e, *body).await.unwrap();
        }
        let bytes = w.finish().await.unwrap();
        let mut r = tarball::Reader::new(bytes.as_slice());
        let mut metas = Vec::new();
        while let Some((e, _)) = r.next().await.unwrap() {
            metas.push(tar_entry_meta(&e).unwrap());
        }
        metas
    }

    #[tokio::test]
    async fn tar_entry_meta_translates_file_and_dir_and_symlink() {
        let mut link = tarball::Entry::new("pg_tblspc/16384", Kind::SymbolicLink, 0, 0o755, 0);
        link.link = "/srv/ts/a".into();
        let metas = tar_metas(&[
            (
                tarball::Entry::new("base/5/16400", Kind::Regular, 8, 0o600, 0),
                b"AAAAAAAA",
            ),
            (
                tarball::Entry::new("pg_replslot", Kind::Directory, 0, 0o700, 0),
                b"",
            ),
            (link, b""),
        ])
        .await;
        let kinds: Vec<_> = metas.into_iter().map(|m| m.unwrap()).collect();
        assert_eq!(kinds.len(), 3);
        assert_eq!(kinds[0].path, PathBuf::from("base/5/16400"));
        assert_eq!((kinds[0].size, kinds[0].mode), (8, 0o600));
        assert!(matches!(kinds[0].kind, FileKind::File));
        assert_eq!(kinds[1].path, PathBuf::from("pg_replslot"));
        assert!(matches!(kinds[1].kind, FileKind::Dir));
        assert_eq!(kinds[2].path, PathBuf::from("pg_tblspc/16384"));
        let FileKind::Symlink { target } = &kinds[2].kind else {
            panic!("expected Symlink, got {:?}", kinds[2].kind);
        };
        assert_eq!(target, &PathBuf::from("/srv/ts/a"));
    }

    /// PG never emits `..` but a hostile / corrupt tar must not escape data_dir
    #[tokio::test]
    async fn tar_entry_meta_rejects_parent_dir_traversal() {
        let e = tarball::Entry::new("../../etc/passwd", Kind::Regular, 0, 0o600, 0);
        let metas = tar_metas(&[(e, b"")]).await;
        assert!(metas[0].is_none());
    }

    #[tokio::test]
    async fn pump_tar_routes_keep_skip_tap_and_lands_catalogs() {
        let tmp = tempfile::tempdir().unwrap();
        let data_dir = tmp.path();
        let tar_bytes = testing::build_synthetic_tar().await;
        let recording = Arc::new(RecordingSink::default());
        let sink: Arc<dyn BackupSink> = Arc::new(recording.clone());
        let mut archive = tarball::Reader::new(tar_bytes.as_slice());
        let target = PumpTarget::new(data_dir.to_path_buf(), sink, Arc::new(PumpStats::default()));
        pump_tar_to_sink(&mut archive, &target).await.unwrap();

        assert!(data_dir.join("base/5/1259").exists(), "catalog must land");
        assert!(data_dir.join("global/1213").exists(), "global must land");
        assert!(data_dir.join("pg_control").exists(), "pg_control must land");
        // Denylist dir lands empty, file inside does not
        assert!(data_dir.join("pg_replslot").is_dir());
        assert!(!data_dir.join("pg_replslot/0/state").exists());
        // User heap tapped, not landed
        assert!(!data_dir.join("base/5/16400").exists());

        let events = recording.events.lock().unwrap();
        // Last event must be pg_control end (contract 3)
        assert_eq!(
            events.last(),
            Some(&Event::End {
                path: PathBuf::from("pg_control")
            })
        );
        // Tapped chunks sum to the file body length
        let tapped_bytes: usize = events
            .iter()
            .filter_map(|e| match e {
                Event::Chunk { len } => Some(*len),
                _ => None,
            })
            .sum();
        assert_eq!(tapped_bytes, 8192);
    }

    /// A replication-protocol tablespace archive is rooted at the
    /// tablespace; its entries must not land over the data dir root
    #[tokio::test]
    async fn tablespace_archive_lands_under_pg_tblspc() {
        let tmp = tempfile::tempdir().unwrap();
        let data_dir = tmp.path();
        let mut w = tarball::Writer::new(Vec::new());
        let dir = tarball::Entry::new("PG_18_202506291", Kind::Directory, 0, 0o700, 0);
        w.append(&dir, tokio::io::empty()).await.unwrap();
        let file = tarball::Entry::new("PG_18_202506291/5/16400", Kind::Regular, 4, 0o600, 0);
        w.append(&file, &b"page"[..]).await.unwrap();
        let tar_bytes = w.finish().await.unwrap();

        let recording = Arc::new(RecordingSink::default());
        let sink: Arc<dyn BackupSink> = Arc::new(recording.clone());
        let target = PumpTarget::new(data_dir.to_path_buf(), sink, Arc::new(PumpStats::default()))
            .for_tablespace(16385);
        let mut archive = tarball::Reader::new(tar_bytes.as_slice());
        pump_tar_to_sink(&mut archive, &target).await.unwrap();

        assert_eq!(
            std::fs::read(data_dir.join("pg_tblspc/16385/PG_18_202506291/5/16400")).unwrap(),
            b"page",
        );
        assert!(!data_dir.join("PG_18_202506291").exists());
        let begun: Vec<_> = recording
            .events
            .lock()
            .unwrap()
            .iter()
            .filter_map(|e| match e {
                Event::Begin { path, .. } => Some(path.clone()),
                _ => None,
            })
            .collect();
        assert_eq!(
            begun,
            [
                PathBuf::from("pg_tblspc/16385/PG_18_202506291"),
                PathBuf::from("pg_tblspc/16385/PG_18_202506291/5/16400"),
            ],
        );
    }
}
