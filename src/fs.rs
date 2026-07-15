use std::io;
use std::path::Path;

use tokio::fs::OpenOptions;

/// Persist directory entry updates
pub async fn fsync_dir(dir: &Path) -> io::Result<()> {
    OpenOptions::new()
        .read(true)
        .open(dir)
        .await?
        .sync_all()
        .await
}
