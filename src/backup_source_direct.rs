//! Direct base-backup source. Wraps wal-rs's
//! `pg::replication::base_backup::run_base_backup` to drive walshadow's
//! [`BackupSource`] trait. Tablespace symlinks ride inside the data-dir
//! archive in PG protocol order, surfacing as `FileKind::Symlink`.
//!
//! Single-pass: seed the page-walk
//! [`CatalogMap`](crate::backup_page_walk::CatalogMap) from source PG
//! before this runs so all routing decisions are known when bytes land.
//! See [plans/bootstrap.md](../plans/bootstrap.md).

use std::path::PathBuf;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use tokio::sync::mpsc;
use wal_rs::pg::replication::base_backup::{
    BackupEvent, BaseBackupOpts, ChannelReader, run_base_backup,
};
use wal_rs::pg::replication::conn::{PgConfig, ReplicationConn};

use crate::backup_source::{BackupSink, BackupSource, EndInfo, StartInfo, pump_tar_to_sink};

/// Replication-protocol BASE_BACKUP issuer
pub struct DirectSource {
    pub source: PgConfig,
    pub opts: BaseBackupOpts,
}

impl DirectSource {
    pub fn new(source: PgConfig, opts: BaseBackupOpts) -> Self {
        Self { source, opts }
    }
}

#[async_trait]
impl BackupSource for DirectSource {
    async fn run(
        self: Box<Self>,
        data_dir: PathBuf,
        sink: Arc<Mutex<dyn BackupSink>>,
    ) -> Result<(StartInfo, EndInfo)> {
        let DirectSource { source, opts } = *self;

        let conn = ReplicationConn::connect(&source)
            .await
            .context("DirectSource: connect to source PG for BASE_BACKUP")?;

        // depth=8 matches wal-rs internal sizing; producer back-pressures
        // on slow archive drain
        let (tx, mut rx) = mpsc::channel::<Result<BackupEvent>>(8);
        let pump = tokio::spawn(async move {
            run_base_backup(conn, opts, tx).await;
        });

        let mut start: Option<StartInfo> = None;
        let mut end: Option<EndInfo> = None;
        // Archives drain sequentially here; shared counter kept for
        // symmetry with object_store's concurrent parts
        let next_entry = AtomicU64::new(0);

        while let Some(ev) = rx.recv().await {
            let ev = ev.context("DirectSource: BASE_BACKUP event channel")?;
            match ev {
                BackupEvent::Start(s) => {
                    let s = StartInfo {
                        start_lsn: s.start_lsn,
                        timeline: s.timeline,
                        tablespaces: s.tablespaces,
                    };
                    {
                        let mut g = sink
                            .lock()
                            .map_err(|_| anyhow::anyhow!("sink mutex poisoned"))?;
                        g.start(&s)?;
                    }
                    start = Some(s);
                }
                BackupEvent::Archive { meta, body } => {
                    tracing::debug!(
                        target = "walshadow::backup_source_direct",
                        name = %meta.name,
                        oid = meta.oid,
                        "archive open",
                    );
                    drive_archive(body, &data_dir, sink.clone(), &next_entry).await?;
                }
                BackupEvent::Finish(e) => {
                    let e = EndInfo {
                        end_lsn: e.end_lsn,
                        timeline: e.timeline,
                    };
                    {
                        let mut g = sink
                            .lock()
                            .map_err(|_| anyhow::anyhow!("sink mutex poisoned"))?;
                        g.finish(&e)?;
                    }
                    end = Some(e);
                }
            }
        }

        if let Err(e) = pump.await {
            bail!("DirectSource: BASE_BACKUP pump task panicked: {e:#}");
        }

        let start = start.ok_or_else(|| anyhow::anyhow!("DirectSource: no StartInfo emitted"))?;
        let end = end.ok_or_else(|| anyhow::anyhow!("DirectSource: no EndInfo emitted"))?;
        Ok((start, end))
    }
}

/// Drain one archive body through `pump_tar_to_sink`. wal-rs's
/// `ChannelReader` is `AsyncRead`, so tokio_tar takes it directly, no
/// SyncIoBridge / spawn_blocking.
async fn drive_archive(
    body: mpsc::Receiver<std::io::Result<bytes::Bytes>>,
    data_dir: &std::path::Path,
    sink: Arc<Mutex<dyn BackupSink>>,
    next_entry: &AtomicU64,
) -> Result<()> {
    let reader = ChannelReader::new(body);
    let mut archive = tokio_tar::Archive::new(reader);
    pump_tar_to_sink(&mut archive, data_dir, &sink, next_entry)
        .await
        .context("DirectSource: tar unpack")?;
    Ok(())
}
