use std::io;
use std::path::Path;

use tokio::fs::OpenOptions;
use tokio::io::AsyncWriteExt;

/// Persist directory entry updates
pub async fn fsync_dir(dir: &Path) -> io::Result<()> {
    OpenOptions::new()
        .read(true)
        .open(dir)
        .await?
        .sync_all()
        .await
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
}
