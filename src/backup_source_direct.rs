//! Phase 12 — Direct base-backup source.
//!
//! Wraps wal-rs's `pg::replication::base_backup::run_base_backup` to
//! drive walshadow's file-streaming [`BackupSource`] trait. Issues
//! `BASE_BACKUP` against source PG; for each `BackupEvent::Archive`
//! body received over the events channel, async-tar-parses the stream
//! and pumps entries through the caller-supplied [`BackupSink`].
//! Tablespace symlinks ride inside the data-dir archive in the PG
//! protocol order; they appear naturally as `FileKind::Symlink`
//! entries.
//!
//! Single-pass design: the [PHASE12plan.md](../plans/PHASE12plan.md)
//! design seeds the page-walk
//! [`CatalogMap`](crate::backup_page_walk::CatalogMap) from source PG
//! via a sidecar SQL query *before* this runs, so all routing
//! decisions are known by the time bytes land.

use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use tokio::sync::mpsc;
use wal_rs::pg::replication::base_backup::{
    BackupEvent, BaseBackupOpts, ChannelReader, run_base_backup,
};
use wal_rs::pg::replication::conn::{PgConfig, ReplicationConn};

use crate::backup_source::{BackupSink, BackupSource, EndInfo, StartInfo, pump_tar_to_sink};

/// Replication-protocol BASE_BACKUP issuer. Holds the connection
/// config + options; one-shot construction per bootstrap.
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

        // wal-rs's events channel is bounded; depth=8 matches its own
        // internal sizing. Producer back-pressures naturally on slow
        // archive drain
        let (tx, mut rx) = mpsc::channel::<Result<BackupEvent>>(8);
        let pump = tokio::spawn(async move {
            run_base_backup(conn, opts, tx).await;
        });

        let mut start: Option<StartInfo> = None;
        let mut end: Option<EndInfo> = None;

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
                    drive_archive(body, &data_dir, sink.clone()).await?;
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

        // Reap the pump task — error here surfaces a join panic or a
        // late-arriving send failure
        if let Err(e) = pump.await {
            bail!("DirectSource: BASE_BACKUP pump task panicked: {e:#}");
        }

        let start = start.ok_or_else(|| anyhow::anyhow!("DirectSource: no StartInfo emitted"))?;
        let end = end.ok_or_else(|| anyhow::anyhow!("DirectSource: no EndInfo emitted"))?;
        Ok((start, end))
    }
}

/// Drain one archive body through `pump_tar_to_sink`. The wal-rs
/// `ChannelReader` is `AsyncRead`, so tokio_tar's async pipeline takes
/// it directly — no SyncIoBridge / spawn_blocking dance.
async fn drive_archive(
    body: mpsc::Receiver<std::io::Result<bytes::Bytes>>,
    data_dir: &std::path::Path,
    sink: Arc<Mutex<dyn BackupSink>>,
) -> Result<()> {
    let reader = ChannelReader::new(body);
    let mut archive = tokio_tar::Archive::new(reader);
    pump_tar_to_sink(&mut archive, data_dir, &sink)
        .await
        .context("DirectSource: tar unpack")?;
    Ok(())
}
