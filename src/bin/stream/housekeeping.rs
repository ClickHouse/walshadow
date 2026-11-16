//! Off-pump background tasks: segment fsync, descriptor-log compaction, and
//! WAL retention trimming.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use tokio_postgres::types::PgLsn;
use walshadow::manifest;
use walshadow::pos::{FilterDurable, Floor, Gate, Monotone, Pos, ShadowReplay};
use walshadow::retention::trim_below_lsn;
use walshadow::segment_sink::SegFsync;

/// Max unsynced segments queued before the pump blocks on `on_segment`;
pub(crate) const SEGMENT_FSYNC_QUEUE: usize = 64;

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
    out_dir: std::fs::File,
    mut rx: tokio::sync::mpsc::Receiver<SegFsync>,
    durable_lsn: Arc<Monotone<FilterDurable>>,
    fatal: walshadow::pipeline::Fatal,
) -> tokio::task::JoinHandle<()> {
    let dir = Arc::new(out_dir);
    tokio::spawn(async move {
        while let Some(mut last) = rx.recv().await {
            while let Ok(next) = rx.try_recv() {
                last = std::cmp::max_by_key(last, next, SegFsync::end_lsn);
            }
            let dir = Arc::clone(&dir);
            let synced = tokio::task::spawn_blocking(move || last.syncfs(&dir)).await;
            match synced.map_err(std::io::Error::from).flatten() {
                Ok(durable) => durable_lsn.publish(durable),
                Err(e) => {
                    fatal.set(format!("syncfs out-dir: {e}"));
                    return;
                }
            };
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
    io: &tokio_util::task::TaskTracker,
) -> tokio::task::JoinHandle<()> {
    io.spawn(async move {
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

/// Read last restartpoint REDO LSN every `interval` and trim
/// below `min(replay_lsn - retention_bytes, redo)`
/// Keep WAL from restartpoint because shadow resumes recovery there
/// Reconnect after failed query because daemon may restart shadow
pub(crate) async fn trim_retention(
    out_dir: PathBuf,
    retention_bytes: u64,
    shadow_conninfo: String,
    shadow_replay_lsn: Arc<Monotone<ShadowReplay>>,
    interval: Duration,
) {
    let mut client: Option<tokio_postgres::Client> = None;
    loop {
        tokio::time::sleep(interval).await;
        // Wait until shadow replays first record
        let lsn = shadow_replay_lsn.get();
        if lsn.is_zero() {
            continue;
        }
        let conn = if let Some(c) = &mut client {
            c
        } else {
            let Ok(c) = open_retention_client(&shadow_conninfo)
                .await
                .inspect_err(|e| {
                    tracing::warn!(
                        target: "walshadow::retention",
                        error = %e,
                        "shadow connect failed; retrying next cycle",
                    );
                })
            else {
                continue;
            };
            client.insert(c)
        };
        let redo = match query_redo_lsn(conn).await {
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

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use walshadow::record::{SegmentSink, WAL_SEG_SIZE, segments_covering};
    use walshadow::segment_sink::DirSegmentSink;

    /// Drain every queued segment, publishing highest end as durable
    #[tokio::test]
    async fn queued_segments_publish_highest_end() {
        let tmp = tempfile::tempdir().unwrap();
        let (tx, rx) = tokio::sync::mpsc::channel(SEGMENT_FSYNC_QUEUE);
        let io = tokio_util::task::TaskTracker::new();
        let mut sink =
            DirSegmentSink::with_durability(tmp.path().to_path_buf(), WAL_SEG_SIZE, tx, io)
                .unwrap();
        let segs = segments_covering(1, 0..3 * WAL_SEG_SIZE);
        for seg in &segs {
            sink.on_segment(*seg, &[0u8; 64]).await.unwrap();
        }
        sink.close().await.unwrap();
        let durable = Arc::new(Monotone::new(Pos::new(0)));
        let fatal = walshadow::pipeline::Fatal::new();
        let dir = std::fs::File::open(tmp.path()).unwrap();
        spawn_segment_fsync(dir, rx, durable.clone(), fatal.clone())
            .await
            .unwrap();
        assert_eq!(fatal.message(), None);
        assert_eq!(durable.get().get(), segs[2].start_lsn(WAL_SEG_SIZE) + 64);
    }

    /// Path-only fd makes syncfs fail EBADF, standing in for a writeback error
    #[tokio::test]
    async fn fsync_failure_sets_fatal_without_publishing() {
        use std::os::unix::fs::OpenOptionsExt;
        let tmp = tempfile::tempdir().unwrap();
        let dir = std::fs::OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_PATH)
            .open(tmp.path())
            .unwrap();
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        let io = tokio_util::task::TaskTracker::new();
        let mut sink =
            DirSegmentSink::with_durability(tmp.path().to_path_buf(), WAL_SEG_SIZE, tx, io)
                .unwrap();
        let seg = segments_covering(1, 0..WAL_SEG_SIZE)[0];
        sink.on_segment(seg, &[0u8; 64]).await.unwrap();
        sink.close().await.unwrap();
        let durable = Arc::new(Monotone::new(Pos::new(0)));
        let fatal = walshadow::pipeline::Fatal::new();
        spawn_segment_fsync(dir, rx, durable.clone(), fatal.clone())
            .await
            .unwrap();
        assert_eq!(
            fatal.message().as_deref(),
            Some("syncfs out-dir: Bad file descriptor (os error 9)")
        );
        assert_eq!(durable.get().get(), 0);
    }

    /// Session wrapper's wait covers gc until its floor cell drops
    #[tokio::test]
    async fn desc_log_gc_tracked_until_floor_drops() {
        let tmp = tempfile::tempdir().unwrap();
        let identity = walshadow::desc_log::DescLogIdentity {
            pg_major: 17,
            system_id: "7300000000000000001".into(),
            timeline: 1,
            db_oid: 5,
            wal_seg_size: WAL_SEG_SIZE as u32,
        };
        let log = walshadow::desc_log::DescriptorLog::open(tmp.path(), identity)
            .await
            .unwrap();
        let floor = Monotone::<Floor>::default();
        let fatal = walshadow::pipeline::Fatal::new();
        let io = tokio_util::task::TaskTracker::new();
        let gc = spawn_desc_log_gc(Arc::new(log), floor.watch(), fatal.clone(), &io);
        io.close();
        assert_eq!(io.len(), 1);
        drop(floor);
        io.wait().await;
        assert!(gc.is_finished());
        assert_eq!(fatal.message(), None);
    }

    /// Read-only log dir fails the ckpt rewrite once dead tombstones trip compaction
    #[tokio::test]
    async fn desc_log_gc_failure_sets_fatal() {
        use std::os::unix::fs::PermissionsExt;
        use walshadow::desc_log::{BatchRecord, LogEntry, LogValue};
        let tmp = tempfile::tempdir().unwrap();
        let identity = walshadow::desc_log::DescLogIdentity {
            pg_major: 17,
            system_id: "7300000000000000001".into(),
            timeline: 1,
            db_oid: 5,
            wal_seg_size: WAL_SEG_SIZE as u32,
        };
        let log = walshadow::desc_log::DescriptorLog::open(tmp.path(), identity)
            .await
            .unwrap();
        let entries = (0..1024)
            .map(|oid| {
                Arc::new(LogEntry {
                    valid_from: 10,
                    oid,
                    rfn: walrus::pg::walparser::RelFileNode {
                        spc_node: 1663,
                        db_node: 5,
                        rel_node: oid,
                    },
                    value: LogValue::Dropped,
                })
            })
            .collect();
        log.append_batch(BatchRecord {
            captured_at: 20,
            commit_lsn: 0,
            observations: Vec::new(),
            ambiguities: Vec::new(),
            entries,
        })
        .await
        .unwrap();
        std::fs::set_permissions(tmp.path(), std::fs::Permissions::from_mode(0o555)).unwrap();
        let floor = Monotone::<Floor>::new(Pos::new(100));
        let fatal = walshadow::pipeline::Fatal::new();
        let io = tokio_util::task::TaskTracker::new();
        spawn_desc_log_gc(Arc::new(log), floor.watch(), fatal.clone(), &io)
            .await
            .unwrap();
        std::fs::set_permissions(tmp.path(), std::fs::Permissions::from_mode(0o755)).unwrap();
        let msg = fatal.message().unwrap();
        assert!(msg.starts_with("descriptor log gc at 0/64: "), "{msg}");
    }

    /// Every event's message, for asserting what a task logged
    #[derive(Clone, Default)]
    pub(crate) struct Messages(Arc<std::sync::Mutex<Vec<String>>>);

    impl Messages {
        pub(crate) fn count(&self, message: &str) -> usize {
            let seen = self.0.lock().unwrap();
            seen.iter().filter(|m| *m == message).count()
        }

        pub(crate) async fn wait(&self, message: &str, n: usize) {
            let deadline = std::time::Instant::now() + Duration::from_secs(10);
            while self.count(message) < n {
                assert!(
                    std::time::Instant::now() < deadline,
                    "never logged {message:?} {n} times"
                );
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }
    }

    impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for Messages {
        fn on_event(
            &self,
            event: &tracing::Event<'_>,
            _: tracing_subscriber::layer::Context<'_, S>,
        ) {
            struct Visit<'a>(&'a mut String);
            impl tracing::field::Visit for Visit<'_> {
                fn record_debug(
                    &mut self,
                    field: &tracing::field::Field,
                    value: &dyn std::fmt::Debug,
                ) {
                    if field.name() == "message" {
                        *self.0 = format!("{value:?}");
                    }
                }
            }
            let mut msg = String::new();
            event.record(&mut Visit(&mut msg));
            self.0.lock().unwrap().push(msg);
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn trim_keeps_redo_segment_across_shadow_restart() {
        if crate::tools::on_path("initdb", "--version") {
            trim_drill().await;
        }
    }

    async fn trim_drill() {
        use tracing::instrument::WithSubscriber;
        use tracing_subscriber::layer::SubscriberExt;
        use walshadow::shadow::{Shadow, ShadowConfig};

        let tmp = tempfile::tempdir().unwrap();
        let out_dir = tmp.path().join("filtered");
        std::fs::create_dir_all(&out_dir).unwrap();
        let mut cfg = ShadowConfig::new(tmp.path().join("data"), out_dir.clone());
        cfg.socket_dir = tmp.path().join("sock");
        let conninfo = walshadow::pg::socket_conninfo(
            cfg.socket_dir.to_str().unwrap(),
            cfg.port,
            "postgres",
            "postgres",
        );
        let shadow = Shadow::new(cfg);
        struct StopOnDrop<'a>(&'a Shadow);
        impl Drop for StopOnDrop<'_> {
            fn drop(&mut self) {
                let _ = self.0.stop();
            }
        }
        let _stop = StopOnDrop(&shadow);
        shadow.initdb().unwrap();
        shadow.write_base_conf().unwrap();

        let seg = |n: u64| out_dir.join(format!("{:024X}", (1u128 << 64) | u128::from(n)));
        for n in 0..3 {
            std::fs::write(seg(n), b"seg").unwrap();
        }
        std::fs::write(out_dir.join("unrelated"), b"keep").unwrap();

        let messages = Messages::default();
        let replay = Arc::new(Monotone::<ShadowReplay>::default());
        let task = tokio::spawn(
            trim_retention(
                out_dir.clone(),
                1,
                conninfo,
                replay.clone(),
                Duration::from_millis(10),
            )
            .with_subscriber(tracing_subscriber::registry().with(messages.clone())),
        );

        let refused = "shadow connect failed; retrying next cycle";
        // No replay yet, so no cycle dials shadow
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(messages.count(refused), 0);
        replay.join(Pos::new(64 * WAL_SEG_SIZE));
        messages.wait(refused, 1).await;
        assert!(seg(0).exists(), "trimmed without a REDO bound");
        shadow.start().unwrap();
        messages.wait("trim cycle", 1).await;
        assert!(!seg(0).exists());
        let redo: u64 = u64::from(
            shadow
                .psql_one("SELECT redo_lsn FROM pg_control_checkpoint()")
                .unwrap()
                .parse::<PgLsn>()
                .unwrap(),
        );
        // initdb places REDO in segment 1, below replay's retention bound
        assert_eq!(redo / WAL_SEG_SIZE, 1);
        assert!(seg(1).exists() && seg(2).exists(), "trimmed past REDO");
        assert!(out_dir.join("unrelated").exists());

        shadow.stop().unwrap();
        messages.wait("redo lsn query", 1).await;
        messages.wait(refused, messages.count(refused) + 1).await;
        std::fs::write(seg(0), b"seg").unwrap();
        shadow.start().unwrap();
        messages.wait("trim cycle", 2).await;
        assert!(!seg(0).exists() && seg(1).exists());
        std::fs::remove_dir_all(&out_dir).unwrap();
        std::fs::write(&out_dir, b"not a dir").unwrap();
        messages.wait("trim", 1).await;
        task.abort();
    }
}
