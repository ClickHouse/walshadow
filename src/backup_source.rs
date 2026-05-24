//! File-streaming backup-source trait.
//!
//! See [plans/bootstrap.md](../plans/bootstrap.md) for the BackupSource
//! trait surface and orchestrator wiring.
//!
//! The trait lifts above tar: a base backup is a stream of files with
//! cluster-relative paths, regardless of wire encoding. Two production
//! impls today, [`crate::backup_source_direct::DirectSource`]
//! (replication protocol) and
//! [`crate::backup_source_object_store::ObjectStoreSource`] (wal-g
//! layout via `DynStorage`). LocalDir is a future third impl.
//!
//! Sinks consume per-file events and route through `Keep` (source
//! writes body to `data_dir`), `Skip` (drop body), or `Tap` (sink
//! receives `chunk()` callbacks). The shape is structurally close to
//! wal-rs's `EntryAction` but at a higher layer that knows about
//! FileKind / Symlink / cluster-relative paths instead of tar entries.
//!
//! All I/O is async on top of `tokio_tar` (the astral-sh fork of the
//! sync `tar` crate). Source impls never need `spawn_blocking`; sinks
//! see entries as the bytes land.
//!
//! ## Contracts every source guarantees
//!
//! 1. `start()` fires before any `begin()`, carrying `start_lsn`,
//!    timeline, and the user tablespace list.
//! 2. Tablespace symlinks (`pg_tblspc/<oid>`) emit as `FileKind::Symlink`
//!    before any file under their subtree.
//! 3. `pg_control` emits last — both PG's BASE_BACKUP protocol and
//!    wal-rs's `list_tar_parts` honour this; future impls must too.
//! 4. `finish()` fires after the last `end()`, carrying `end_lsn`.
//! 5. Paths are cluster-relative, sanitised against `..` /
//!    absolute-root traversal at the source impl boundary.

use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use tokio::io::{AsyncRead, AsyncReadExt};

use wal_rs::pg::replication::base_backup::Tablespace;

/// Filesystem-object kind of a file event. Tar-driven sources translate
/// tar entry types here; the trait does not expose tar at all.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FileKind {
    /// Regular file. Body bytes flow through `chunk()` (Tap) or get
    /// written to `data_dir/path` (Keep).
    File,
    /// Directory. No body. `Keep` creates it, `Skip` drops it, `Tap`
    /// fires `chunk()` with zero bytes.
    Dir,
    /// Symbolic link. No body. `target` is the cluster-relative or
    /// absolute path the link resolves to (PG tablespace symlinks
    /// carry absolute filesystem paths).
    Symlink { target: PathBuf },
}

/// Cluster-relative path + size + mode + kind. Path examples:
///
/// - `base/5/16400` — user heap file
/// - `base/5/1259` — `pg_class` heap (catalog, OID 1259)
/// - `global/1213` — shared catalog
/// - `pg_control` — controlfile
/// - `pg_tblspc/16384` — tablespace symlink (FileKind::Symlink)
/// - `pg_xact/0000` — clog file
#[derive(Debug, Clone)]
pub struct FileMeta {
    pub path: PathBuf,
    pub size: u64,
    pub mode: u32,
    pub kind: FileKind,
}

/// Sink decision per file at `begin()` time. Source acts on the
/// returned action; sink owns whatever bookkeeping the action implies.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileAction {
    /// Source writes the body (regular files) or materializes the dir
    /// / symlink under caller-supplied `data_dir`. `chunk()` is not
    /// called for `Keep`.
    Keep,
    /// Source drains the body unread; no on-disk land, no `chunk()`
    /// callbacks.
    Skip,
    /// Source streams body bytes through `chunk()` to the sink; no
    /// on-disk land. Dir / Symlink entries fire `chunk()` zero times
    /// regardless.
    Tap,
}

/// Source-side invariants known before the first file event. Mirrors
/// wal-rs `pg::replication::base_backup::StartInfo` so callers wired to
/// wal-rs types don't translate. Cloned by value.
#[derive(Debug, Clone)]
pub struct StartInfo {
    pub start_lsn: u64,
    pub timeline: u32,
    pub tablespaces: Vec<Tablespace>,
}

/// Recovery-target LSN + timeline known after the last file event.
/// Mirrors wal-rs's `EndInfo`.
#[derive(Debug, Clone)]
pub struct EndInfo {
    pub end_lsn: u64,
    pub timeline: u32,
}

/// Per-file consumer.
///
/// Lifecycle, fired by [`BackupSource::run`]:
///
/// 1. `start(&StartInfo)` once.
/// 2. For each file in source order:
///    - `begin(&FileMeta) -> FileAction`,
///    - if `Tap`: zero-or-more `chunk(bytes)`,
///    - `end()` always.
/// 3. `finish(&EndInfo)` once.
///
/// `Send` is required because parallel source impls (object_store
/// fan-out) drive the sink from worker tasks. The trait methods are
/// sync — sinks that need async I/O (e.g. database catalog seeding)
/// must complete that work in `start()`/`finish()` via a runtime
/// handle, or buffer events for an async drain task.
pub trait BackupSink: Send {
    /// Fired once before any file event.
    fn start(&mut self, _info: &StartInfo) -> io::Result<()> {
        Ok(())
    }

    /// Routing decision. Implementations are expected to be cheap;
    /// per-file dispatch is the hot path.
    fn begin(&mut self, meta: &FileMeta) -> io::Result<FileAction>;

    /// Body bytes for a `Tap` entry. Called zero or more times in
    /// arbitrary chunk sizes between `begin()` and `end()`. Sources
    /// guarantee bytes are presented in file order; sink owns any
    /// per-page / per-chunk framing.
    fn chunk(&mut self, bytes: &[u8]) -> io::Result<()>;

    /// Closes the current file. Fires once per `begin()`, regardless of
    /// the action returned.
    fn end(&mut self) -> io::Result<()>;

    /// Fired once after the last file event.
    fn finish(&mut self, _info: &EndInfo) -> io::Result<()> {
        Ok(())
    }
}

/// One base backup as a stream of file events.
///
/// `data_dir` receives `Keep`d bodies. The sink lives behind `Arc<Mutex<_>>`
/// so parallel source impls share it without per-worker copies.
#[async_trait]
pub trait BackupSource: Send {
    async fn run(
        self: Box<Self>,
        data_dir: PathBuf,
        sink: Arc<Mutex<dyn BackupSink>>,
    ) -> anyhow::Result<(StartInfo, EndInfo)>;
}

// ---------------------------------------------------------------------
// Shared helpers — tar→file translation, async disk-land writer

/// Translate one tokio_tar entry into a `FileMeta`. Returns `None` for
/// entries the source should ignore (absolute paths, parent-dir
/// traversal, hard links — PG basebackup emits none of these).
///
/// Cluster-relative path is rebuilt component-by-component skipping
/// any `..` / `/` so callers can't escape `data_dir` on write.
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
    let size = header.size().unwrap_or(0);
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

/// Drive one tokio_tar archive against a sink. Drains `archive` to
/// end, emitting per-entry `begin()`/`chunk()`/`end()` callbacks.
/// `Keep` entries land under `data_dir`; `Tap` entries stream through
/// `chunk()`; `Skip` entries drop the body unread.
///
/// Called by both `DirectSource` (per `BackupEvent::Archive.body`) and
/// `ObjectStoreSource` (per fetched tar part).
pub(crate) async fn pump_tar_to_sink<R>(
    archive: &mut tokio_tar::Archive<R>,
    data_dir: &Path,
    sink: &Arc<Mutex<dyn BackupSink>>,
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
        pump_entry(&mut entry, &meta, data_dir, sink).await?;
    }
    Ok(())
}

/// One tar entry through the sink. Factored so callers can drive
/// non-tar-shaped FileMeta sequences (e.g. inline symlink emission
/// before the data archive opens, or a future LocalDir source).
pub(crate) async fn pump_entry<R>(
    body: &mut R,
    meta: &FileMeta,
    data_dir: &Path,
    sink: &Arc<Mutex<dyn BackupSink>>,
) -> io::Result<()>
where
    R: AsyncRead + Unpin + ?Sized,
{
    let action = {
        let mut s = sink
            .lock()
            .map_err(|_| io::Error::other("sink mutex poisoned"))?;
        s.begin(meta)?
    };
    match action {
        FileAction::Keep => write_kept(body, meta, data_dir).await?,
        FileAction::Skip => drain_to_void(body).await?,
        FileAction::Tap => stream_to_sink(body, meta, sink).await?,
    }
    let mut s = sink
        .lock()
        .map_err(|_| io::Error::other("sink mutex poisoned"))?;
    s.end()
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

async fn stream_to_sink<R>(
    body: &mut R,
    meta: &FileMeta,
    sink: &Arc<Mutex<dyn BackupSink>>,
) -> io::Result<()>
where
    R: AsyncRead + Unpin + ?Sized,
{
    if !matches!(meta.kind, FileKind::File) {
        drain_to_void(body).await?;
        return Ok(());
    }
    let mut buf = [0u8; 64 * 1024];
    loop {
        let n = body.read(&mut buf).await?;
        if n == 0 {
            break;
        }
        let mut s = sink
            .lock()
            .map_err(|_| io::Error::other("sink mutex poisoned"))?;
        s.chunk(&buf[..n])?;
    }
    Ok(())
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

/// Materialize a tablespace symlink as a `FileMeta` and pump it
/// through the sink. Source impls call this for any
/// `Tablespace::is_default() == false` entry that didn't already ride
/// inside the tar stream as a `pg_tblspc/<oid>` Symlink. Both
/// production impls today get the symlinks from inside the data-dir
/// archive, so this helper is currently used only by future LocalDir
/// shapes — exposed for API symmetry.
#[allow(dead_code)]
pub(crate) async fn emit_tablespace_symlink(
    tablespace: &Tablespace,
    data_dir: &Path,
    sink: &Arc<Mutex<dyn BackupSink>>,
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
    pump_entry(&mut body, &meta, data_dir, sink).await
}

#[cfg(test)]
pub(crate) mod testing {
    //! Helpers reused across crate tests.

    use tokio::io::AsyncWriteExt;

    /// Build a one-archive tar in memory containing the curated set of
    /// entries every routing test asserts on. Mirrors PG's BASE_BACKUP
    /// layout roughly: an empty pg_replslot dir + denylist file inside,
    /// a global/ catalog, a base/<db>/<catalog>, a base/<db>/<heap>,
    /// pg_control last.
    pub async fn build_synthetic_tar() -> Vec<u8> {
        let buf: Vec<u8> = Vec::new();
        let mut b = tokio_tar::Builder::new(buf);
        append_dir(&mut b, "pg_replslot", 0o700).await;
        append_file(&mut b, "pg_replslot/0/state", b"REPLSLOT", 0o600).await;
        append_file(&mut b, "global/1213", b"global-catalog-bytes", 0o600).await;
        append_file(&mut b, "base/5/1259", b"pg_class-bytes", 0o600).await;
        append_file(&mut b, "base/5/16400", &vec![0xAB; 8192], 0o600).await;
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

    /// Recording sink — collects every callback into a `Vec<Event>` so
    /// tests assert on the exact sequence the source produced. Owns
    /// captured `Tap` chunks per file.
    #[derive(Debug, Default)]
    pub(crate) struct RecordingSink {
        pub events: Vec<Event>,
        pub tapped: Vec<(PathBuf, Vec<u8>)>,
        cur_path: Option<PathBuf>,
        cur_tap: Option<Vec<u8>>,
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    pub(crate) enum Event {
        Start { start_lsn: u64, timeline: u32 },
        Begin { path: PathBuf, action: FileAction },
        Chunk { len: usize },
        End { path: PathBuf },
        Finish { end_lsn: u64 },
    }

    impl BackupSink for RecordingSink {
        fn start(&mut self, info: &StartInfo) -> io::Result<()> {
            self.events.push(Event::Start {
                start_lsn: info.start_lsn,
                timeline: info.timeline,
            });
            Ok(())
        }
        fn begin(&mut self, meta: &FileMeta) -> io::Result<FileAction> {
            // Route catalogs Keep, denylist Skip, user heap Tap, rest Keep
            let s = meta.path.to_string_lossy();
            let action = if s.starts_with("pg_replslot/") {
                if matches!(meta.kind, FileKind::Dir) {
                    FileAction::Keep
                } else {
                    FileAction::Skip
                }
            } else if s == "base/5/16400" {
                FileAction::Tap
            } else {
                FileAction::Keep
            };
            self.events.push(Event::Begin {
                path: meta.path.clone(),
                action,
            });
            self.cur_path = Some(meta.path.clone());
            self.cur_tap = (action == FileAction::Tap).then(Vec::new);
            Ok(action)
        }
        fn chunk(&mut self, bytes: &[u8]) -> io::Result<()> {
            self.events.push(Event::Chunk { len: bytes.len() });
            if let Some(buf) = self.cur_tap.as_mut() {
                buf.extend_from_slice(bytes);
            }
            Ok(())
        }
        fn end(&mut self) -> io::Result<()> {
            let path = self.cur_path.take().unwrap_or_default();
            if let Some(buf) = self.cur_tap.take() {
                self.tapped.push((path.clone(), buf));
            }
            self.events.push(Event::End { path });
            Ok(())
        }
        fn finish(&mut self, info: &EndInfo) -> io::Result<()> {
            self.events.push(Event::Finish {
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
            // tokio_tar::Builder::set_path refuses `..`, so synthesise
            // a header with the path bytes directly. PG's BASE_BACKUP
            // never emits `..` paths but a hostile / corrupt tar must
            // not escape data_dir.
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
        let recording = Arc::new(Mutex::new(RecordingSink::default()));
        let sink: Arc<Mutex<dyn BackupSink>> = recording.clone();
        let mut archive = tokio_tar::Archive::new(std::io::Cursor::new(tar_bytes));
        pump_tar_to_sink(&mut archive, data_dir, &sink)
            .await
            .unwrap();

        // Catalogs landed
        assert!(data_dir.join("base/5/1259").exists(), "catalog must land");
        assert!(data_dir.join("global/1213").exists(), "global must land");
        assert!(data_dir.join("pg_control").exists(), "pg_control must land");
        // Denylist dir landed (empty), file inside did not
        assert!(data_dir.join("pg_replslot").is_dir());
        assert!(!data_dir.join("pg_replslot/0/state").exists());
        // User heap tapped, did not land on disk
        assert!(!data_dir.join("base/5/16400").exists());

        let r = recording.lock().unwrap();
        // Last file event must be pg_control end
        let last_end = r
            .events
            .iter()
            .rev()
            .find_map(|e| match e {
                Event::End { path } => Some(path.clone()),
                _ => None,
            })
            .unwrap();
        assert_eq!(last_end, PathBuf::from("pg_control"));
        // Tap fired for user heap exactly once worth of chunks summing to
        // the file body length
        let tapped_bytes: usize = r
            .events
            .iter()
            .filter_map(|e| match e {
                Event::Chunk { len } => Some(*len),
                _ => None,
            })
            .sum();
        assert_eq!(tapped_bytes, 8192);
    }
}
