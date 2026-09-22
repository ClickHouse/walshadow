//! Filtered WAL segment durability sink

use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;

use tokio::sync::mpsc;
use walrus::pg::wal::segment::SegmentName;

use crate::record::{SegmentSink, SinkError};

pub struct SegFsync {
    pub end_lsn: u64,
    pub seg_path: PathBuf,
}

enum Durability {
    Inline,
    Background {
        seg_size: u64,
        tx: mpsc::Sender<SegFsync>,
    },
}

pub struct DirSegmentSink {
    out_dir: PathBuf,
    durability: Durability,
}

impl DirSegmentSink {
    pub fn new(out_dir: PathBuf) -> Result<Self, SinkError> {
        std::fs::create_dir_all(&out_dir)?;
        Ok(Self {
            out_dir,
            durability: Durability::Inline,
        })
    }

    pub fn with_durability(
        out_dir: PathBuf,
        seg_size: u64,
        tx: mpsc::Sender<SegFsync>,
    ) -> Result<Self, SinkError> {
        std::fs::create_dir_all(&out_dir)?;
        Ok(Self {
            out_dir,
            durability: Durability::Background { seg_size, tx },
        })
    }

    async fn write(&self, seg: &SegmentName, bytes: &[u8]) -> Result<(), SinkError> {
        let inline = matches!(self.durability, Durability::Inline);
        let name = seg.format();
        let seg_path = self.out_dir.join(&name);
        // Retention sweeps a temp a crash leaves behind
        let seg_tmp = self.out_dir.join(format!("{name}.tmp"));
        write_sync_rename(&seg_tmp, &seg_path, bytes, inline).await?;
        match &self.durability {
            Durability::Inline => crate::fs::fsync_dir(&self.out_dir).await?,
            Durability::Background { seg_size, tx } => tx
                .send(SegFsync {
                    end_lsn: seg.start_lsn(*seg_size) + bytes.len() as u64,
                    seg_path,
                })
                .await
                .map_err(|_| SinkError::Other("segment fsync queue closed".into()))?,
        }
        Ok(())
    }
}

impl SegmentSink for DirSegmentSink {
    fn on_segment<'a>(
        &'a mut self,
        seg: SegmentName,
        bytes: &'a [u8],
    ) -> Pin<Box<dyn Future<Output = Result<(), SinkError>> + Send + 'a>> {
        Box::pin(async move { self.write(&seg, bytes).await })
    }
}

async fn write_sync_rename(
    tmp: &Path,
    final_path: &Path,
    bytes: &[u8],
    fsync: bool,
) -> Result<(), SinkError> {
    use tokio::io::AsyncWriteExt as _;
    let mut file = tokio::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(tmp)
        .await?;
    file.write_all(bytes).await?;
    if fsync {
        file.sync_all().await?;
    }
    drop(file);
    tokio::fs::rename(tmp, final_path).await?;
    Ok(())
}
