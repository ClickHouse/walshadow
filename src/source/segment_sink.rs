//! Filtered WAL segment durability sink

use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;

use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use walrus::pg::wal::segment::SegmentName;

use crate::pos::{Durable, FilterDurable, Pos};
use crate::record::{SegmentSink, SinkError};

/// Renamed segment awaiting filesystem flush
pub struct SegFsync {
    end_lsn: Pos<FilterDurable>,
    seg_path: PathBuf,
}

impl SegFsync {
    pub fn end_lsn(&self) -> Pos<FilterDurable> {
        self.end_lsn
    }

    pub fn seg_path(&self) -> &Path {
        &self.seg_path
    }

    /// `syncfs` the filesystem under `dir`, proving every segment renamed
    /// there up to `self` durable
    pub fn syncfs(&self, dir: &std::fs::File) -> std::io::Result<Durable<FilterDurable>> {
        crate::fs::syncfs(dir)?;
        Ok(Durable::new(self.end_lsn))
    }
}

enum Durability {
    Inline,
    Background {
        seg_size: u64,
        tx: mpsc::Sender<SegFsync>,
        /// Previous segment's write, yielding its buffer back for reuse
        in_flight: Option<JoinHandle<Result<Vec<u8>, SinkError>>>,
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
            durability: Durability::Background {
                seg_size,
                tx,
                in_flight: None,
            },
        })
    }

    /// Wait out the background write, surfacing its error
    pub async fn close(mut self) -> Result<(), SinkError> {
        if let Durability::Background { in_flight, .. } = &mut self.durability
            && let Some(write) = in_flight.take()
        {
            join_write(write).await?;
        }
        Ok(())
    }

    async fn write(&mut self, seg: &SegmentName, bytes: &[u8]) -> Result<(), SinkError> {
        let name = seg.format();
        let seg_path = self.out_dir.join(&name);
        // Retention sweeps a temp a crash leaves behind
        let seg_tmp = self.out_dir.join(format!("{name}.tmp"));
        let Durability::Background {
            seg_size,
            tx,
            in_flight,
        } = &mut self.durability
        else {
            write_sync_rename(&seg_tmp, &seg_path, bytes).await?;
            return Ok(crate::fs::fsync_dir(&self.out_dir).await?);
        };
        // Copy out so pump keeps streaming while a blocking thread writes.
        // One write in flight: returning here proves every earlier segment
        // renamed into place
        let mut buf = match in_flight.take() {
            Some(write) => join_write(write).await?,
            None => Vec::with_capacity(bytes.len()),
        };
        buf.clear();
        buf.extend_from_slice(bytes);
        let fsync = SegFsync {
            end_lsn: Pos::new(seg.start_lsn(*seg_size) + bytes.len() as u64),
            seg_path,
        };
        let tx = tx.clone();
        *in_flight = Some(tokio::task::spawn_blocking(move || {
            std::fs::write(&seg_tmp, &buf)?;
            std::fs::rename(&seg_tmp, &fsync.seg_path)?;
            tx.blocking_send(fsync)
                .map_err(|_| SinkError::Other("segment fsync queue closed".into()))?;
            Ok(buf)
        }));
        Ok(())
    }
}

async fn join_write(write: JoinHandle<Result<Vec<u8>, SinkError>>) -> Result<Vec<u8>, SinkError> {
    write
        .await
        .map_err(|e| SinkError::Other(format!("segment write task: {e}")))?
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

async fn write_sync_rename(tmp: &Path, final_path: &Path, bytes: &[u8]) -> Result<(), SinkError> {
    use tokio::io::AsyncWriteExt as _;
    let mut file = tokio::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(tmp)
        .await?;
    file.write_all(bytes).await?;
    file.flush().await?;
    file.sync_all().await?;
    drop(file);
    tokio::fs::rename(tmp, final_path).await?;
    Ok(())
}
