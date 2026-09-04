//! Replay greenfield backup-window WAL into ClickHouse
//!
//! [`stream_window`] reads live WAL beside `BASE_BACKUP`; [`replay_segments`]
//! reads hydrated segments. Both emit through [`WalReplaySink`] at commit LSN
//! so replayed rows outrank page-walk rows tagged at backup start

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use tokio::sync::{Mutex, watch};
use walrus::pg::backup::format_pg_lsn;
use walrus::pg::wal::segment::SegmentName;
use walrus::pg::walparser::Oid;

use crate::backfill::backup_page_walk::CatalogMap;
use crate::backfill::wal_replay::{
    DropSegments, ReplayStats, ReplayTargets, WalReplayInputs, WalReplaySink, pump_segments_through,
};
use crate::catalog::desc_log::{BatchRecord, DescLogIdentity, DescriptorLog, LogEntry, LogValue};
use crate::config::ResolvedConfig;
use crate::decode::visibility::PgXactPatch;
use crate::emit::ch_emitter::{EmitterConfig, EmitterStats};
use crate::emit::pipeline::Fatal;
use crate::emit::pipeline::tail::OwnedTail;
use crate::mapping::MappingHandle;
use crate::record::{WAL_SEG_SIZE, segments_covering};
use crate::source::source_feed::{SourceEvent, SourceFeed, StandbyStatus};
use crate::source::wal_stream::WalStream;
use crate::toast::ToastResolver;
use crate::xact::xact_buffer::{XactBuffer, XactBufferConfig};
use ahash::HashSet;

/// Wind-down poll while source sends no WAL
const STOP_POLL: Duration = Duration::from_millis(100);

/// Maximum wait for transactions open below handoff
const WIND_DOWN_MAX: Duration = Duration::from_secs(5);

/// Inputs shared with concurrent bootstrap drain
#[derive(Clone)]
pub struct WindowLegConfig {
    pub emitter: EmitterConfig,
    pub mapping: MappingHandle,
    pub config: Arc<ResolvedConfig>,
    /// Shared emitter counters
    pub stats: Arc<EmitterStats>,
    /// Shared TOAST resolver
    pub resolver: ToastResolver,
    /// Bootstrap conversion oracle
    pub oracle: Option<Arc<crate::ops::oracle::Oracle>>,
    /// Shared pipeline failure state
    pub fatal: Fatal,
    /// Transaction spill and descriptor-log root
    pub scratch_dir: PathBuf,
    /// Commit overlay for walked-tuple visibility
    pub patch: Arc<std::sync::Mutex<PgXactPatch>>,
    pub catalog: CatalogMap,
    pub pg_major: u32,
    pub system_id: String,
    pub timeline: u32,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct WindowLegStats {
    pub replay: ReplayStats,
    /// Walk coverage floor, read starts at previous segment boundary
    pub from_lsn: u64,
    /// One past the last byte the leg covered
    pub through_lsn: u64,
    /// Lowest first-record LSN among transactions open at seal
    pub open_floor: Option<u64>,
}

/// Stream window on caller's slotless replication connection
///
/// `stop` publishes `end_lsn`; zero stops at current position
pub async fn stream_window(
    cfg: WindowLegConfig,
    feed: &mut SourceFeed,
    from_lsn: u64,
    stop: watch::Receiver<Option<u64>>,
) -> Result<WindowLegStats> {
    let leg = Leg::open(&cfg, from_lsn)
        .await
        .context("bootstrap window leg: open")?;
    run_live(leg, cfg.timeline, feed, from_lsn, stop).await
}

/// Replay window WAL already on disk
pub async fn replay_segments(
    cfg: WindowLegConfig,
    segments: &[(SegmentName, PathBuf)],
    from_lsn: u64,
    end_lsn: u64,
) -> Result<WindowLegStats> {
    let mut leg = Leg::open(&cfg, from_lsn)
        .await
        .context("bootstrap window leg: open")?;
    let db_oid = leg.db_oid;
    let drive = pump_segments_through(segments, cfg.timeline, db_oid, &mut leg.sink).await;
    // Drain inserter after replay failure, report replay error first
    let sealed = leg.close(from_lsn, end_lsn, end_lsn).await;
    drive.context("bootstrap window leg: replay segments")?;
    sealed
}

/// Enumerate complete segment range covering `[from_lsn, end_lsn)`. A boundary
/// `end_lsn` needs no further segment, and `pg_basebackup` ships none
/// (XLByteToPrevSeg)
pub async fn segments_in_dir(
    dir: &Path,
    timeline: u32,
    from_lsn: u64,
    end_lsn: u64,
) -> Result<Vec<(SegmentName, PathBuf)>> {
    let segments = segments_covering(timeline, read_start(from_lsn)..end_lsn);
    let mut out = Vec::with_capacity(segments.len());
    for seg in segments {
        let path = dir.join(seg.format());
        anyhow::ensure!(
            tokio::fs::try_exists(&path).await.unwrap_or(false),
            "bootstrap window leg: WAL segment {} missing from {}",
            seg.format(),
            dir.display(),
        );
        out.push((seg, path));
    }
    Ok(out)
}

/// Align read start below commit floor
fn read_start(from_lsn: u64) -> u64 {
    WalStream::align_down(from_lsn, WAL_SEG_SIZE)
}

/// Every catalog filenode decodes, so TOAST records reach their parent rows;
/// only non-TOAST rels route. No ceiling: a leg owns every commit it decodes,
/// the pump resumes at seal
fn replay_scope(catalog: &CatalogMap) -> (HashSet<(Oid, Oid)>, ReplayTargets) {
    let filter_rfns = catalog
        .descriptors()
        .map(|d| (d.rfn.db_node, d.rfn.rel_node))
        .collect();
    let targets = catalog
        .descriptors()
        .filter(|d| !catalog.is_toast(d.rfn.db_node, d.rfn.rel_node))
        .map(|d| ((d.rfn.db_node, d.rfn.rel_node), (d.clone(), u64::MAX)))
        .collect();
    (filter_rfns, targets)
}

/// Replay sink and owned insert tail
struct Leg {
    sink: WalReplaySink,
    tail: OwnedTail,
    db_oid: Oid,
}

impl Leg {
    async fn open(cfg: &WindowLegConfig, from_lsn: u64) -> Result<Self> {
        let db_oid = cfg
            .catalog
            .descriptors()
            .map(|d| d.rfn.db_node)
            .find(|db| *db != 0)
            .context("bootstrap window leg: catalog map names no database")?;
        let log = seed_scratch_log(
            &cfg.scratch_dir.join("desc_log"),
            DescLogIdentity {
                pg_major: cfg.pg_major,
                system_id: cfg.system_id.clone(),
                timeline: cfg.timeline,
                db_oid,
                wal_seg_size: WAL_SEG_SIZE as u32,
            },
            &cfg.catalog,
            read_start(from_lsn),
        )
        .await?;

        let spill = cfg.scratch_dir.join("xact_spill");
        tokio::fs::create_dir_all(&spill)
            .await
            .with_context(|| format!("create {}", spill.display()))?;
        let buffer = Arc::new(Mutex::new(
            XactBuffer::new(XactBufferConfig::new(spill))
                .map_err(|e| anyhow::anyhow!("bootstrap window leg: xact buffer: {e}"))?,
        ));
        buffer.lock().await.clear_spill_dir().await.ok();

        let (filter_rfns, targets) = replay_scope(&cfg.catalog);
        let tail = OwnedTail::spawn(
            &cfg.emitter,
            1,
            cfg.stats.clone(),
            cfg.fatal.clone(),
            None,
            cfg.oracle.clone(),
            "bootstrap window leg",
        )
        .await
        .map_err(anyhow::Error::msg)?;

        let sink = WalReplaySink::new(WalReplayInputs {
            log,
            buffer,
            resolver: cfg.resolver.clone(),
            filter_rfns,
            targets,
            from_lsn,
            // Unknown user filenodes imply window DDL
            whole_db_filter: true,
            mapping: cfg.mapping.snapshot().await,
            stats: cfg.stats.clone(),
            budget: cfg.resolver.budget().cloned(),
            row_policy: cfg.emitter.row_policy(),
            config: Some(cfg.config.clone()),
            batch_rows: cfg.emitter.drain_batch_rows,
            batch_bytes: cfg.emitter.drain_batch_bytes,
            msg_tx: tail.msg_tx.clone(),
            ack: tail.ack.clone(),
            next_seq: 0,
            patch: Some(cfg.patch.clone()),
        });
        Ok(Self { sink, tail, db_oid })
    }

    /// Flush and prove every sequence durable
    ///
    /// `handoff` bounds the open-xact report: the pump rebuilds anything
    /// opened above it on its own
    async fn close(self, from_lsn: u64, through_lsn: u64, handoff: u64) -> Result<WindowLegStats> {
        let replay = self.sink.stats();
        let open_floor = self
            .sink
            .xacts_opened_below(handoff)
            .await
            .into_iter()
            .map(|(_, first_lsn)| first_lsn)
            .min();
        drop(self.sink);
        self.tail
            .finish(replay.next_seq)
            .await
            .map_err(anyhow::Error::msg)?;
        Ok(WindowLegStats {
            replay,
            from_lsn,
            through_lsn,
            open_floor,
        })
    }
}

/// Seed window-wide descriptors from snapshot catalog
async fn seed_scratch_log(
    dir: &Path,
    identity: DescLogIdentity,
    catalog: &CatalogMap,
    read_start: u64,
) -> Result<Arc<DescriptorLog>> {
    tokio::fs::create_dir_all(dir)
        .await
        .with_context(|| format!("create {}", dir.display()))?;
    for f in [
        crate::catalog::desc_log::CKPT_FILE,
        crate::catalog::desc_log::TAIL_FILE,
    ] {
        let _ = tokio::fs::remove_file(dir.join(f)).await;
    }
    let log = DescriptorLog::open(dir, identity)
        .await
        .context("bootstrap window leg: open scratch descriptor log")?;
    let entries = catalog
        .descriptors()
        .map(|d| {
            Arc::new(LogEntry {
                valid_from: read_start,
                oid: d.oid,
                rfn: d.rfn,
                value: LogValue::Present(d.clone()),
            })
        })
        .collect();
    log.seed(
        BatchRecord {
            captured_at: read_start,
            commit_lsn: 0,
            observations: Vec::new(),
            ambiguities: Vec::new(),
            entries,
        },
        read_start,
    )
    .await
    .context("bootstrap window leg: seed scratch descriptor log")?;
    Ok(Arc::new(log))
}

/// Consume live WAL through published `end_lsn`
async fn run_live(
    mut leg: Leg,
    timeline: u32,
    feed: &mut SourceFeed,
    from_lsn: u64,
    mut stop: watch::Receiver<Option<u64>>,
) -> Result<WindowLegStats> {
    let begin = read_start(from_lsn);
    feed.start_physical_replication(None, begin, timeline)
        .await
        .context("bootstrap window leg: START_REPLICATION")?;
    let mut stream = WalStream::new(timeline, WAL_SEG_SIZE, begin)
        .map_err(|e| anyhow::anyhow!("bootstrap window leg: WalStream: {e}"))?;
    stream.filter_mut().set_target_db(leg.db_oid);
    let mut seg_sink = DropSegments;
    let mut buf = Vec::new();

    let mut wind_down: Option<std::time::Instant> = None;
    let res = loop {
        // Read through end_lsn so final-segment commits reach PgXactPatch
        let target = *stop.borrow_and_update();
        if let Some(target) = target.filter(|t| stream.next_lsn().get() >= *t) {
            let spanning = leg.sink.xacts_opened_below(target).await;
            if spanning.is_empty() {
                break Ok(());
            }
            let since = *wind_down.get_or_insert_with(std::time::Instant::now);
            if since.elapsed() >= WIND_DOWN_MAX {
                // Pump resumes from open_floor to rebuild these transactions
                tracing::warn!(
                    target: "walshadow::bootstrap",
                    xids = ?spanning.iter().map(|(xid, _)| *xid).collect::<Vec<_>>(),
                    target = %format_pg_lsn(target),
                    "backup-window leg stopped waiting on xacts open across the \
                     handoff; the pump resumes below their first records instead",
                );
                break Ok(());
            }
        }
        // Slotless status update only prevents sender timeout
        let status = StandbyStatus::collapsed(stream.next_lsn().get());
        let event = tokio::select! {
            biased;
            // Closed watch means caller is unwinding
            res = stop.changed() => match res {
                Ok(()) => continue,
                Err(_) => break Ok(()),
            },
            // Poll only while waiting for open transactions
            _ = tokio::time::sleep(STOP_POLL), if wind_down.is_some() => continue,
            res = feed.next_event(status, &mut buf) => res,
        };
        match event {
            Ok(SourceEvent::Wal(chunk)) => {
                let (lsn, data) = (chunk.start_lsn, chunk.data);
                if let Err(e) = stream.push(lsn, data, &mut leg.sink, &mut seg_sink).await {
                    break Err(anyhow::anyhow!("bootstrap window leg: push: {e}"));
                }
            }
            Ok(SourceEvent::TimelineEnd) => {
                break Err(anyhow::anyhow!(
                    "bootstrap window leg: source ended timeline {timeline} inside the \
                     backup window; the source was promoted mid-bootstrap"
                ));
            }
            Ok(SourceEvent::Shutdown) => {
                break Err(anyhow::anyhow!(
                    "bootstrap window leg: source closed the stream inside the backup window"
                ));
            }
            Err(e) => break Err(e.context("bootstrap window leg: source read")),
        }
    };
    let through = stream.next_lsn().get();
    // Read runs past the handoff to land final-segment commits; report open
    // xacts at the handoff the wind-down waited on
    let handoff = stop.borrow().unwrap_or(through).min(through);
    // Drain tail after partial-window failure
    let sealed = leg.close(from_lsn, through, handoff).await;
    res?;
    sealed
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backfill::backup_page_walk::make_rel_named;
    use crate::catalog::desc_log::LookupResult;
    use crate::schema::{RelDescriptor, RelName};

    /// `make_rel`'s database, the one `replay_scope` keys off
    const DB: Oid = 5;

    fn desc(oid: Oid, rel_node: u32, namespace: &str, name: &str) -> Arc<RelDescriptor> {
        make_rel_named(oid, rel_node, 0, RelName::new(namespace, name))
    }

    fn ident() -> DescLogIdentity {
        DescLogIdentity {
            pg_major: 17,
            system_id: "7300000000000000001".into(),
            timeline: 1,
            db_oid: DB,
            wal_seg_size: WAL_SEG_SIZE as u32,
        }
    }

    /// Read starts at segment boundary below coverage floor
    #[test]
    fn read_start_aligns_below_the_floor() {
        assert_eq!(read_start(WAL_SEG_SIZE), WAL_SEG_SIZE);
        assert_eq!(read_start(WAL_SEG_SIZE + 1), WAL_SEG_SIZE);
        assert_eq!(read_start(3 * WAL_SEG_SIZE - 1), 2 * WAL_SEG_SIZE);
    }

    /// Snapshot descriptors cover full window
    #[tokio::test]
    async fn scratch_log_answers_window_lookups() {
        let tmp = tempfile::tempdir().unwrap();
        let mut catalog = CatalogMap::new();
        let d = desc(16400, 16400, "public", "t");
        catalog.insert(d.clone());
        let from = 3 * WAL_SEG_SIZE + 4096;
        let log = seed_scratch_log(tmp.path(), ident(), &catalog, read_start(from))
            .await
            .unwrap();

        assert_eq!(log.covered_through(), read_start(from));
        assert!(matches!(
            log.descriptor_at(d.rfn, from),
            LookupResult::Present(got) if got.rel_name == d.rel_name
        ));
        assert!(
            matches!(
                log.descriptor_at(d.rfn, read_start(from)),
                LookupResult::Present(_)
            ),
            "records in the alignment prefix decode too; their commits drop on `from_lsn`",
        );
        assert!(matches!(
            log.descriptor_at(d.rfn, read_start(from) - 1),
            LookupResult::NotCovered
        ));
    }

    /// TOAST records decode but do not route directly
    #[test]
    fn toast_rels_filter_but_do_not_target() {
        let mut catalog = CatalogMap::new();
        catalog.insert(desc(16400, 16400, "public", "t"));
        catalog.insert(desc(16402, 16402, "pg_toast", "pg_toast_16400"));

        let (filter, targets) = replay_scope(&catalog);

        assert_eq!(filter.len(), 2);
        assert_eq!(
            targets.keys().map(|(_, rel)| *rel).collect::<Vec<_>>(),
            [16400]
        );
        assert!(targets.values().all(|(_, ceiling)| *ceiling == u64::MAX));
    }

    /// End on a boundary stops at the segment holding the last byte
    #[tokio::test]
    async fn segments_in_dir_stops_below_a_boundary_end() {
        let tmp = tempfile::tempdir().unwrap();
        let names: Vec<String> = (1..=2)
            .map(|n| {
                SegmentName {
                    timeline: 1,
                    log_id: 0,
                    seg_no: n,
                }
                .format()
            })
            .collect();
        for n in &names {
            tokio::fs::write(tmp.path().join(n), b"").await.unwrap();
        }
        let segs = segments_in_dir(tmp.path(), 1, WAL_SEG_SIZE + 1024, 3 * WAL_SEG_SIZE)
            .await
            .unwrap();
        assert_eq!(
            segs.iter().map(|(s, _)| s.format()).collect::<Vec<_>>(),
            names,
        );
    }

    /// Reject incomplete segment ranges
    #[tokio::test]
    async fn segments_in_dir_spans_the_window_and_refuses_gaps() {
        let tmp = tempfile::tempdir().unwrap();
        let from = WAL_SEG_SIZE + 1024;
        let end = 3 * WAL_SEG_SIZE + 512;
        let names: Vec<String> = (1..=3)
            .map(|n| {
                SegmentName {
                    timeline: 1,
                    log_id: 0,
                    seg_no: n,
                }
                .format()
            })
            .collect();
        for n in &names[..2] {
            tokio::fs::write(tmp.path().join(n), b"").await.unwrap();
        }
        let err = segments_in_dir(tmp.path(), 1, from, end)
            .await
            .expect_err("third segment missing");
        assert!(err.to_string().contains(&names[2]), "{err}");

        tokio::fs::write(tmp.path().join(&names[2]), b"")
            .await
            .unwrap();
        let segs = segments_in_dir(tmp.path(), 1, from, end).await.unwrap();
        assert_eq!(
            segs.iter().map(|(s, _)| s.format()).collect::<Vec<_>>(),
            names,
        );
    }
}
