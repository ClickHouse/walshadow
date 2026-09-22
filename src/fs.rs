use std::io;
use std::path::{Path, PathBuf};

use tokio::fs::OpenOptions;
use tokio::io::AsyncWriteExt;

/// Process-local scratch under durable `root`: contents rebuild from WAL at
/// restart, so boot wipes it wholesale and nothing fsyncs into it
pub fn scratch_dir(root: &Path) -> PathBuf {
    root.join("scratch")
}

/// Wipe and recreate `dir` wholesale
pub fn reset_dir(dir: &Path) -> io::Result<()> {
    match std::fs::remove_dir_all(dir) {
        Ok(()) => {}
        Err(e) if e.kind() == io::ErrorKind::NotFound => {}
        Err(e) => return Err(e),
    }
    std::fs::create_dir_all(dir)
}

/// Persist directory entry updates
pub async fn fsync_dir(dir: &Path) -> io::Result<()> {
    OpenOptions::new()
        .read(true)
        .open(dir)
        .await?
        .sync_all()
        .await
}

/// Check source system identifier a state file recorded against live one.
/// `Ok(true)` asks caller to rewrite a file predating the stamp
pub fn check_source(path: &Path, stored: Option<u64>, live: u64) -> io::Result<bool> {
    let Some(stored) = stored else {
        return Ok(true);
    };
    if stored != live {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "{} belongs to source system {stored}, live source is {live}; \
                 wipe the spill dir for a new source",
                path.display()
            ),
        ));
    }
    Ok(false)
}

/// Flush every dirty file and directory entry on `dir`'s filesystem, for a
/// tree too wide to fsync entry by entry
pub async fn sync_filesystem(dir: &Path) -> io::Result<()> {
    let f = tokio::fs::File::open(dir).await?.into_std().await;
    tokio::task::spawn_blocking(move || syncfs(&f))
        .await
        .map_err(io::Error::other)?
}

#[cfg(target_os = "linux")]
fn syncfs(f: &std::fs::File) -> io::Result<()> {
    use std::os::fd::AsRawFd;
    // SAFETY: `f` owns the fd for the whole call
    if unsafe { libc::syncfs(f.as_raw_fd()) } == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

#[cfg(not(target_os = "linux"))]
fn syncfs(_f: &std::fs::File) -> io::Result<()> {
    unreachable!("walshadow is Linux-only")
}

/// Crash-safe replace: write+fsync `{name}.tmp`, rename over `{name}`,
/// fsync dir so rename survives power loss. Reader sees old-complete or
/// new-complete file, never a torn write; a crash between write and
/// rename leaves a stale `.tmp` no boot path reads.
pub async fn write_atomic(dir: &Path, name: &str, bytes: &[u8]) -> io::Result<()> {
    let tmp_path = dir.join(format!("{name}.tmp"));
    let mut f = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&tmp_path)
        .await?;
    f.write_all(bytes).await?;
    f.sync_all().await?;
    drop(f);
    tokio::fs::rename(&tmp_path, dir.join(name)).await?;
    fsync_dir(dir).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(flavor = "current_thread")]
    async fn write_atomic_replaces_and_cleans_tmp() {
        let tmp = tempfile::tempdir().unwrap();
        write_atomic(tmp.path(), "f.toml", b"a = 1\n")
            .await
            .unwrap();
        write_atomic(tmp.path(), "f.toml", b"a = 2\n")
            .await
            .unwrap();
        assert_eq!(
            std::fs::read(tmp.path().join("f.toml")).unwrap(),
            b"a = 2\n"
        );
        assert!(!tmp.path().join("f.toml.tmp").exists());
    }

    #[test]
    fn reset_dir_wipes_nested_contents() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = scratch_dir(tmp.path());
        std::fs::create_dir_all(dir.join("nested")).unwrap();
        std::fs::write(dir.join("nested/a.bin"), b"x").unwrap();
        std::fs::write(tmp.path().join("durable.toml"), b"x").unwrap();
        reset_dir(&dir).unwrap();
        assert_eq!(std::fs::read_dir(&dir).unwrap().count(), 0);
        assert!(tmp.path().join("durable.toml").exists());
        reset_dir(&tmp.path().join("absent")).unwrap();
    }
}
