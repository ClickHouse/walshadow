//! Off-pump background tasks: segment fsync, descriptor-log compaction, and
//! WAL retention trimming.

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Result;
use tokio_postgres::types::PgLsn;
use walshadow::manifest;
use walshadow::pos::{FilterDurable, Floor, Gate, Monotone, Pos, ShadowReplay};
use walshadow::retention::{DEFAULT_TRIM_INTERVAL, trim_below_lsn};
use walshadow::segment_sink::SegFsync;

/// Max unsynced segments queued before the pump blocks on `on_segment`;
pub(crate) const SEGMENT_FSYNC_QUEUE: usize = 64;

#[cfg(target_os = "linux")]
pub(crate) fn sync_filesystem(fd: std::os::fd::RawFd) -> std::io::Result<()> {
    if unsafe { libc::syncfs(fd) } == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error())
    }
}

#[cfg(not(target_os = "linux"))]
pub(crate) fn sync_filesystem(_fd: std::os::fd::RawFd) -> std::io::Result<()> {
    unreachable!("walshadow-stream is Linux-only")
}

/// Background segment durability: drain the fsync queue, then `syncfs` the
/// filesystem holding `out_dir` once per batch — this flushes every written
/// segment + manifest + the directory entries in one syscall, avoiding the
/// per-file `open`+`sync_data` walk (à la PG `recovery_init_sync_method=syncfs`).
/// Then advance `durable_lsn` to the highest covered LSN. A sync error sets
/// `fatal` and stops (the main loop then exits rather than advertising
/// durability past the failure).
///
/// `syncfs` error reporting requires Linux >= 5.8; the fd is held for the task's
/// lifetime so writeback errors on this filesystem are seen. Because it flushes
/// the *whole* filesystem, `out_dir` should live on a volume walshadow owns —
/// on a shared disk it may block on unrelated writeback.
pub(crate) fn spawn_segment_fsync(
    out_dir: PathBuf,
    mut rx: tokio::sync::mpsc::Receiver<SegFsync>,
    durable_lsn: Arc<Monotone<FilterDurable>>,
    fatal: walshadow::pipeline::Fatal,
) -> tokio::task::JoinHandle<()> {
    use std::os::unix::io::AsRawFd;
    tokio::spawn(async move {
        let dir = match std::fs::File::open(&out_dir) {
            Ok(f) => f,
            Err(e) => {
                fatal.set(format!("open {} for syncfs: {e}", out_dir.display()));
                return;
            }
        };
        let dirfd = dir.as_raw_fd();
        while let Some(item) = rx.recv().await {
            let mut max_lsn = item.end_lsn;
            while let Ok(next) = rx.try_recv() {
                max_lsn = max_lsn.max(next.end_lsn);
            }
            let synced = tokio::task::spawn_blocking(move || sync_filesystem(dirfd)).await;
            match synced {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    fatal.set(format!("syncfs {}: {e}", out_dir.display()));
                    return;
                }
                Err(e) => {
                    fatal.set(format!("syncfs join {}: {e}", out_dir.display()));
                    return;
                }
            }
            durable_lsn.join(Pos::new(max_lsn));
        }
    })
}

/// Compact the descriptor log against each published resume floor, off the
/// pump task. A compaction rewrites the whole ckpt inline; on the pump that
/// stalls WAL consumption while `wal_sender_timeout` runs with no keepalive
/// answered. Boundary capture still shares the log's writer mutex, so a
/// boundary landing mid-compaction blocks its hold — this removes the stall
/// for boundary-free stretches, which is the common case.
pub(crate) fn spawn_desc_log_gc(
    desc_log: Arc<walshadow::desc_log::DescriptorLog>,
    floor: Gate<Floor>,
    fatal: walshadow::pipeline::Fatal,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut cut = floor.current();
        loop {
            if let Err(e) = desc_log.maybe_gc(cut).await {
                fatal.set(format!("descriptor log gc at {cut}: {e}"));
                return;
            }
            let Ok(next) = floor.advance(cut).await else {
                return;
            };
            cut = next;
        }
    })
}

/// Every [`DEFAULT_TRIM_INTERVAL`], read last restartpoint REDO LSN and trim
/// below `min(replay_lsn - retention_bytes, redo)`
/// Keep WAL from restartpoint because shadow resumes recovery there
/// Reconnect after failed query because daemon may restart shadow
pub(crate) async fn trim_retention(
    out_dir: PathBuf,
    retention_bytes: u64,
    shadow_conninfo: String,
    shadow_replay_lsn: Arc<Monotone<ShadowReplay>>,
) {
    let mut client: Option<tokio_postgres::Client> = None;
    loop {
        tokio::time::sleep(DEFAULT_TRIM_INTERVAL).await;
        // Wait until shadow replays first record
        let lsn = shadow_replay_lsn.get();
        if lsn.is_zero() {
            continue;
        }
        if client.is_none() {
            match open_retention_client(&shadow_conninfo).await {
                Ok(c) => client = Some(c),
                Err(e) => {
                    tracing::warn!(
                        target: "walshadow::retention",
                        error = %e,
                        "shadow connect failed; retrying next cycle",
                    );
                    continue;
                }
            }
        }
        let redo = match query_redo_lsn(client.as_ref().expect("just set")).await {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!(target: "walshadow::retention", error = %e, "redo lsn query");
                client = None;
                continue;
            }
        };
        let cutoff = manifest::retention_cutoff(lsn, retention_bytes, redo.map(Pos::new));
        match trim_below_lsn(&out_dir, cutoff).await {
            Ok(r) if r.segments_removed > 0 => {
                tracing::info!(
                    target: "walshadow::retention",
                    segments = r.segments_removed,
                    manifests = r.manifests_removed,
                    partials = r.partials_removed,
                    bytes_freed = r.bytes_freed,
                    cutoff_lsn = %cutoff,
                    "trim cycle",
                );
            }
            Ok(_) => {}
            Err(e) => tracing::warn!(target: "walshadow::retention", error = %e, "trim"),
        }
    }
}

pub(crate) async fn open_retention_client(conninfo: &str) -> Result<tokio_postgres::Client> {
    let (client, conn) = tokio_postgres::connect(conninfo, tokio_postgres::NoTls).await?;
    tokio::spawn(async move {
        let _ = conn.await;
    });
    Ok(client)
}

pub(crate) async fn query_redo_lsn(client: &tokio_postgres::Client) -> Result<Option<u64>> {
    let row = client
        .query_one("SELECT redo_lsn FROM pg_control_checkpoint()", &[])
        .await?;
    let redo: Option<PgLsn> = row.get(0);
    Ok(redo.map(u64::from))
}
