//! File-streaming backup-source trait. A base backup is a stream of
//! files with cluster-relative paths, regardless of wire encoding.
//! See [plans/bootstrap.md](../plans/bootstrap.md).
//!
//! Production impls: [`crate::backfill::backup_source_direct::DirectSource`]
//! (replication protocol) and
//! [`crate::backfill::backup_source_object_store::ObjectStoreSource`] (wal-g
//! layout via `DynStorage`).
//!
//! All I/O is async on `tokio_tar` (astral-sh fork of sync `tar`); no
//! `spawn_blocking`.
//!
//! ## Contracts every source guarantees
//!
//! 1. `start()` fires before any `begin()`, carrying start_lsn,
//!    timeline, user tablespace list.
//! 2. Tablespace symlinks (`pg_tblspc/<oid>`) emit as `FileKind::Symlink`
//!    before any file under their subtree.
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
use tokio::io::{AsyncRead, AsyncReadExt};

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
    }
}

/// Filesystem-object kind. Tar-driven sources translate tar entry types
/// here; the trait does not expose tar.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FileKind {
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
#[derive(Debug, Clone)]
pub struct FileMeta {
    pub path: PathBuf,
    pub size: u64,
    pub mode: u32,
    pub kind: FileKind,
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
    /// Body bytes in file order, arbitrary chunk sizes. Awaiting here
    /// backpressures the source
    async fn chunk(&mut self, bytes: &[u8]) -> io::Result<()>;

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
}

impl PumpTarget {
    pub fn new(data_dir: PathBuf, sink: Arc<dyn BackupSink>, stats: Arc<PumpStats>) -> Self {
        Self {
            data_dir,
            sink,
            stats,
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

/// Translate one tokio_tar entry into a `FileMeta`. `None` for entries
/// the source ignores (absolute paths, `..` traversal, hard links; PG
/// basebackup emits none). Path rebuilt component-by-component skipping
/// `..` / `/` so callers can't escape `data_dir` on write.
pub(crate) fn tar_entry_meta<R: AsyncRead + Unpin>(
    entry: &tokio_tar::Entry<R>,
) -> io::Result<Option<FileMeta>> {
    use std::path::Component;

    let path = entry.path()?.into_owned();
    let mut rel = PathBuf::new();
    for c in path.components() {
        match c {
            Component::Prefix(..) | Component::RootDir | Component::CurDir => continue,
            Component::ParentDir => return Ok(None),
            Component::Normal(p) => rel.push(p),
        }
    }
    if rel.as_os_str().is_empty() {
        return Ok(None);
    }
    let header = entry.header();
    let size = entry.effective_size();
    let mode = header.mode().unwrap_or(0o644);
    let etype = header.entry_type();
    let kind = if etype.is_dir() {
        FileKind::Dir
    } else if etype.is_symlink() {
        let target = header
            .link_name()?
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "symlink without target"))?
            .into_owned();
        FileKind::Symlink { target }
    } else if etype.is_file() || etype.is_hard_link() {
        FileKind::File
    } else {
        return Ok(None);
    };
    Ok(Some(FileMeta {
        path: rel,
        size,
        mode,
        kind,
    }))
}

/// Drain one tokio_tar archive against a sink, emitting per-entry
/// callbacks. Called by `DirectSource` (per `BackupEvent::Archive.body`)
/// and `ObjectStoreSource` (per fetched tar part).
pub async fn pump_tar_to_sink<R>(
    archive: &mut tokio_tar::Archive<R>,
    target: &PumpTarget,
) -> io::Result<()>
where
    R: AsyncRead + Unpin + Send,
{
    use futures::StreamExt;

    let mut entries = archive.entries()?;
    while let Some(entry_res) = entries.next().await {
        let mut entry = entry_res?;
        let Some(meta) = tar_entry_meta(&entry)? else {
            continue;
        };
        pump_entry(&mut entry, &meta, target).await?;
    }
    Ok(())
}

/// One tar entry through the sink. Factored so callers can drive
/// non-tar-shaped FileMeta sequences (e.g. inline symlink emission).
pub async fn pump_entry<R>(body: &mut R, meta: &FileMeta, target: &PumpTarget) -> io::Result<()>
where
    R: AsyncRead + Unpin + ?Sized,
{
    match target.sink.begin(meta).await? {
        FileAction::Keep => write_kept(body, meta, &target.data_dir).await,
        FileAction::Skip => drain_to_void(body).await,
        FileAction::Tap(entry) => stream_to_entry(body, meta, entry, &target.stats).await,
    }
}

async fn drain_to_void<R>(body: &mut R) -> io::Result<()>
where
    R: AsyncRead + Unpin + ?Sized,
{
    let mut buf = [0u8; 16 * 1024];
    loop {
        let n = body.read(&mut buf).await?;
        if n == 0 {
            break;
        }
    }
    Ok(())
}

async fn stream_to_entry<R>(
    body: &mut R,
    meta: &FileMeta,
    mut entry: Box<dyn EntrySink>,
    stats: &PumpStats,
) -> io::Result<()>
where
    R: AsyncRead + Unpin + ?Sized,
{
    if !matches!(meta.kind, FileKind::File) {
        drain_to_void(body).await?;
        return entry.end().await;
    }
    let mut buf = [0u8; 64 * 1024];
    loop {
        let n = body.read(&mut buf).await?;
        if n == 0 {
            break;
        }
        stats.bytes_tapped.fetch_add(n as u64, Ordering::Relaxed);
        // Awaiting here parks only this entry's read, and through it this
        // part's fetch. Other parts keep draining
        let entered = Instant::now();
        entry.chunk(&buf[..n]).await?;
        stats
            .sink_chunk_nanos
            .fetch_add(entered.elapsed().as_nanos() as u64, Ordering::Relaxed);
    }
    entry.end().await
}

async fn write_kept<R>(body: &mut R, meta: &FileMeta, data_dir: &Path) -> io::Result<()>
where
    R: AsyncRead + Unpin + ?Sized,
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
            drain_to_void(body).await?;
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
            drain_to_void(body).await?;
        }
        FileKind::File => {
            let mut f = tokio::fs::OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&target)
                .await?;
            tokio::io::copy(body, &mut f).await?;
            f.sync_data().await.ok();
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

/// Materialize a non-default tablespace symlink and pump it through the
/// sink. Both production impls get symlinks from inside the data-dir
/// archive, so this is unused today, exposed for future LocalDir shapes.
#[allow(dead_code)]
pub(crate) async fn emit_tablespace_symlink(
    tablespace: &Tablespace,
    target: &PumpTarget,
) -> io::Result<()> {
    if tablespace.is_default() {
        return Ok(());
    }
    let meta = FileMeta {
        path: PathBuf::from(format!("pg_tblspc/{}", tablespace.oid)),
        size: 0,
        mode: 0o755,
        kind: FileKind::Symlink {
            target: PathBuf::from(&tablespace.location),
        },
    };
    let mut body = tokio::io::empty();
    pump_entry(&mut body, &meta, target).await
}

#[cfg(test)]
pub(crate) mod testing {
    //! Helpers reused across crate tests.

    use tokio::io::AsyncWriteExt;

    /// In-memory tar roughly mirroring PG's BASE_BACKUP layout: empty
    /// pg_replslot dir + denylist file inside, global/ catalog,
    /// base/<db>/<catalog>, base/<db>/<heap>, pg_control last.
    pub async fn build_synthetic_tar() -> Vec<u8> {
        let buf: Vec<u8> = Vec::new();
        let mut b = tokio_tar::Builder::new(buf);
        append_dir(&mut b, "pg_replslot", 0o700).await;
        append_file(&mut b, "pg_replslot/0/state", b"REPLSLOT", 0o600).await;
        append_file(&mut b, "global/1213", b"global-catalog-bytes", 0o600).await;
        append_file(&mut b, "base/5/1259", b"pg_class-bytes", 0o600).await;
        append_file(&mut b, "base/5/16400", &[0xAB; 8192], 0o600).await;
        append_file(&mut b, "pg_control", b"pg-control-bytes", 0o600).await;
        b.finish().await.unwrap();
        let mut out = b.into_inner().await.unwrap();
        out.flush().await.unwrap();
        out
    }

    async fn append_dir<W: tokio::io::AsyncWrite + Unpin + Send + 'static>(
        b: &mut tokio_tar::Builder<W>,
        name: &str,
        mode: u32,
    ) {
        let mut h = tokio_tar::Header::new_gnu();
        h.set_path(name).unwrap();
        h.set_size(0);
        h.set_mode(mode);
        h.set_entry_type(tokio_tar::EntryType::Directory);
        h.set_cksum();
        b.append(&h, tokio::io::empty()).await.unwrap();
    }

    async fn append_file<W: tokio::io::AsyncWrite + Unpin + Send + 'static>(
        b: &mut tokio_tar::Builder<W>,
        name: &str,
        body: &[u8],
        mode: u32,
    ) {
        let mut h = tokio_tar::Header::new_gnu();
        h.set_path(name).unwrap();
        h.set_size(body.len() as u64);
        h.set_mode(mode);
        h.set_entry_type(tokio_tar::EntryType::Regular);
        h.set_cksum();
        b.append(&h, body).await.unwrap();
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
        Start { start_lsn: u64, timeline: u32 },
        Begin { path: PathBuf, action: &'static str },
        Chunk { len: usize },
        End { path: PathBuf },
        Finish { end_lsn: u64 },
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
        async fn chunk(&mut self, bytes: &[u8]) -> io::Result<()> {
            self.sink
                .events
                .lock()
                .unwrap()
                .push(Event::Chunk { len: bytes.len() });
            self.body.extend_from_slice(bytes);
            Ok(())
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
        async fn start(&self, info: &StartInfo) -> io::Result<()> {
            self.events.lock().unwrap().push(Event::Start {
                start_lsn: info.start_lsn,
                timeline: info.timeline,
            });
            Ok(())
        }
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
        async fn finish(&self, info: &EndInfo) -> io::Result<()> {
            self.events.lock().unwrap().push(Event::Finish {
                end_lsn: info.end_lsn,
            });
            Ok(())
        }
    }

    #[tokio::test]
    async fn tar_entry_meta_translates_file_and_dir_and_symlink() {
        let buf: Vec<u8> = Vec::new();
        let mut b = tokio_tar::Builder::new(buf);

        let mut h = tokio_tar::Header::new_gnu();
        h.set_path("base/5/16400").unwrap();
        h.set_size(8);
        h.set_mode(0o600);
        h.set_entry_type(tokio_tar::EntryType::Regular);
        h.set_cksum();
        b.append(&h, &b"AAAAAAAA"[..]).await.unwrap();

        let mut h = tokio_tar::Header::new_gnu();
        h.set_path("pg_replslot").unwrap();
        h.set_size(0);
        h.set_mode(0o700);
        h.set_entry_type(tokio_tar::EntryType::Directory);
        h.set_cksum();
        b.append(&h, tokio::io::empty()).await.unwrap();

        let mut h = tokio_tar::Header::new_gnu();
        h.set_path("pg_tblspc/16384").unwrap();
        h.set_link_name("/srv/ts/a").unwrap();
        h.set_size(0);
        h.set_mode(0o755);
        h.set_entry_type(tokio_tar::EntryType::Symlink);
        h.set_cksum();
        b.append(&h, tokio::io::empty()).await.unwrap();

        b.finish().await.unwrap();
        let bytes = b.into_inner().await.unwrap();

        use futures::StreamExt;
        let mut archive = tokio_tar::Archive::new(std::io::Cursor::new(bytes));
        let mut entries = archive.entries().unwrap();
        let mut kinds = Vec::new();
        while let Some(entry_res) = entries.next().await {
            let entry = entry_res.unwrap();
            let meta = tar_entry_meta(&entry).unwrap().unwrap();
            kinds.push((meta.path, meta.kind));
        }
        assert_eq!(kinds.len(), 3);
        assert_eq!(kinds[0].0, PathBuf::from("base/5/16400"));
        assert!(matches!(kinds[0].1, FileKind::File));
        assert_eq!(kinds[1].0, PathBuf::from("pg_replslot"));
        assert!(matches!(kinds[1].1, FileKind::Dir));
        assert_eq!(kinds[2].0, PathBuf::from("pg_tblspc/16384"));
        match &kinds[2].1 {
            FileKind::Symlink { target } => {
                assert_eq!(target, &PathBuf::from("/srv/ts/a"))
            }
            other => panic!("expected Symlink, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn tar_entry_meta_rejects_parent_dir_traversal() {
        let mut bytes = Vec::new();
        {
            let mut h = tokio_tar::Header::new_gnu();
            // set_path refuses `..`, so write header bytes directly. PG
            // never emits `..` but a hostile / corrupt tar must not
            // escape data_dir.
            let name = b"../../etc/passwd";
            h.as_old_mut().name[..name.len()].copy_from_slice(name);
            h.set_size(0);
            h.set_mode(0o600);
            h.set_entry_type(tokio_tar::EntryType::Regular);
            h.set_cksum();
            bytes.extend_from_slice(h.as_bytes());
            bytes.extend_from_slice(&[0u8; 1024]);
        }
        use futures::StreamExt;
        let mut archive = tokio_tar::Archive::new(std::io::Cursor::new(bytes));
        let mut entries = archive.entries().unwrap();
        let entry = entries.next().await.unwrap().unwrap();
        assert!(tar_entry_meta(&entry).unwrap().is_none());
    }

    #[tokio::test]
    async fn pump_tar_routes_keep_skip_tap_and_lands_catalogs() {
        let tmp = tempfile::tempdir().unwrap();
        let data_dir = tmp.path();
        let tar_bytes = testing::build_synthetic_tar().await;
        let recording = Arc::new(RecordingSink::default());
        let sink: Arc<dyn BackupSink> = Arc::new(recording.clone());
        let mut archive = tokio_tar::Archive::new(std::io::Cursor::new(tar_bytes));
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
        // Last file event must be pg_control end (contract 3)
        let last_end = events
            .iter()
            .rev()
            .find_map(|e| match e {
                Event::End { path } => Some(path.clone()),
                _ => None,
            })
            .unwrap();
        assert_eq!(last_end, PathBuf::from("pg_control"));
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
}
