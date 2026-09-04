//! Decode bounded WAL ranges through shared transaction pipeline
//!
//! Used by greenfield window replay and object-store gap replay. Emit commits
//! above page-walk coverage and through each target's upper bound

use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;

use anyhow::{Context, Result};
use tokio::sync::{Mutex, mpsc};
use walrus::pg::wal::segment::SegmentName;
use walrus::pg::walparser::{Oid, RmId};

use crate::budget::MemoryBudget;
use crate::catalog::desc_log::DescriptorLog;
use crate::config::ResolvedConfig;
use crate::decode::heap_decoder::CommittedTuple;
use crate::decode::visibility::PgXactPatch;
use crate::decode::wal_xact::{
    XLOG_XACT_ABORT, XLOG_XACT_ABORT_PREPARED, XLOG_XACT_ASSIGNMENT, XLOG_XACT_COMMIT,
    XLOG_XACT_COMMIT_PREPARED, XLOG_XACT_OPMASK, parse_xact_assignment, parse_xact_payload,
};
use crate::emit::ch_emitter::EmitterStats;
use crate::emit::pipeline::ack::AckHandle;
use crate::emit::pipeline::batcher::{BatcherMsg, RoutedRow};
use crate::emit::route::{RouteSnapshot, freeze_routes};
use crate::filter::manifest::Manifest;
use crate::mapping::MappingSnapshot;
use crate::record::{Record, RecordSink, SegmentSink, SinkError, WAL_SEG_SIZE};
use crate::schema::{FIRST_NORMAL_OBJECT_ID, RelDescriptor, RelName};
use crate::source::wal_stream::WalStream;
use crate::toast::{ChunkRefMap, ToastResolver};
use crate::xact::xact_buffer::{
    BufferingDecoderSink, DrainEntry, DrainedBatch, SubxactTracker, WalkStep, XactBuffer,
    detoast_heap, resolve_stash,
};
use ahash::{HashMap, HashSet};

/// Per-filenode descriptor and exclusive replay ceiling
pub type ReplayTargets = HashMap<(Oid, Oid), (Arc<RelDescriptor>, u64)>;

/// Replay inputs shared across records
pub struct WalReplayInputs {
    pub log: Arc<DescriptorLog>,
    pub buffer: Arc<Mutex<XactBuffer>>,
    pub resolver: ToastResolver,
    /// Rfns whose heap records reach the decoder: targets plus their toast rels
    pub filter_rfns: HashSet<(Oid, Oid)>,
    pub targets: ReplayTargets,
    /// Walk-coverage floor; commits at or below it drop
    pub from_lsn: u64,
    /// Treat unfiltered user filenodes as DDL when filter covers database
    pub whole_db_filter: bool,
    pub mapping: MappingSnapshot,
    pub stats: Arc<EmitterStats>,
    pub budget: Option<MemoryBudget>,
    pub row_policy: crate::emit::route::RowPolicy,
    pub config: Option<Arc<ResolvedConfig>>,
    pub batch_rows: usize,
    pub batch_bytes: usize,
    pub msg_tx: mpsc::Sender<BatcherMsg>,
    pub ack: AckHandle,
    pub next_seq: u64,
    /// Commit/abort overlay for walked-tuple visibility
    pub patch: Option<Arc<std::sync::Mutex<PgXactPatch>>>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ReplayStats {
    /// One past the last seq the leg registered
    pub next_seq: u64,
    pub rows_replayed: u64,
    pub commits_past_through: u64,
    /// Commits covered by walked pages
    pub commits_below_from: u64,
    /// Unknown user filenodes when filter covers database
    pub unknown_rfns: u64,
}

/// Serial replay drain over prefiltered records
pub struct WalReplaySink {
    decoder: BufferingDecoderSink,
    buffer: Arc<Mutex<XactBuffer>>,
    log: Arc<DescriptorLog>,
    /// Empty because replay applies committed history
    pending: crate::catalog::pending::PendingCatalog,
    subxact_tracker: SubxactTracker,
    resolver: ToastResolver,
    filter_rfns: HashSet<(Oid, Oid)>,
    targets: ReplayTargets,
    from_lsn: u64,
    /// See [`WalReplayInputs::whole_db_filter`]
    whole_db_filter: bool,
    /// Routes frozen at replay start from the mapping + config snapshots
    routes: HashMap<RelName, Arc<RouteSnapshot>>,
    stats: Arc<EmitterStats>,
    budget: Option<MemoryBudget>,
    /// Drain-slice budget, same knobs as the pipeline reorder
    batch_rows: usize,
    batch_bytes: usize,
    msg_tx: mpsc::Sender<BatcherMsg>,
    ack: AckHandle,
    patch: Option<Arc<std::sync::Mutex<PgXactPatch>>>,
    /// Current `(sequence, routed rows)`, registered on first row
    open: Option<(u64, u64)>,
    replay: ReplayStats,
}

impl WalReplaySink {
    pub fn new(inputs: WalReplayInputs) -> Self {
        Self {
            decoder: BufferingDecoderSink::new(inputs.log.clone(), inputs.buffer.clone()),
            buffer: inputs.buffer,
            log: inputs.log,
            pending: Default::default(),
            subxact_tracker: SubxactTracker::new(),
            resolver: inputs.resolver,
            filter_rfns: inputs.filter_rfns,
            targets: inputs.targets,
            from_lsn: inputs.from_lsn,
            whole_db_filter: inputs.whole_db_filter,
            routes: freeze_routes(
                &inputs.mapping,
                inputs.config.as_deref(),
                &inputs.row_policy,
            ),
            stats: inputs.stats,
            budget: inputs.budget,
            batch_rows: inputs.batch_rows,
            batch_bytes: inputs.batch_bytes,
            msg_tx: inputs.msg_tx,
            ack: inputs.ack,
            patch: inputs.patch,
            open: None,
            replay: ReplayStats {
                next_seq: inputs.next_seq,
                ..Default::default()
            },
        }
    }

    /// Transactions needing replay below `lsn` to rebuild buffered prefix
    pub async fn xacts_opened_below(&self, lsn: u64) -> Vec<(u32, u64)> {
        self.buffer
            .lock()
            .await
            .inflight_snapshot()
            .into_iter()
            .filter(|e| e.first_lsn < lsn)
            .map(|e| (e.xid, e.first_lsn))
            .collect()
    }

    pub fn stats(&self) -> ReplayStats {
        self.replay
    }

    async fn on_commit(
        &mut self,
        xid: u32,
        info: u8,
        record: &Record<'_>,
    ) -> std::result::Result<(), SinkError> {
        // Require subxact list to preserve buffered rows
        let payload = parse_xact_payload(info, &record.parsed.main_data, record.page_magic)
            .map_err(|e| SinkError::Other(format!("wal_replay: commit payload: {e}")))?;
        // Prepared xid owns buffered work and visibility verdict
        let xid = payload.twophase_xid.unwrap_or(xid);
        if let Some(patch) = &self.patch {
            patch
                .lock()
                .expect("wal_replay patch lock")
                .commit(xid, &payload.subxacts);
        }
        // Resolve filenodes invisible at record time before drain
        resolve_stash(
            &self.buffer,
            &self.log,
            &self.pending,
            xid,
            &payload.subxacts,
            record.next_lsn,
            self.resolver.stats_handle(),
        )
        .await
        .map_err(SinkError::from)?;
        let mut drain = self
            .buffer
            .lock()
            .await
            .drain_committed(
                xid,
                payload.xact_time,
                record.source_lsn,
                &payload.subxacts,
                self.resolver.stores_chunks(),
            )
            .await
            .map_err(SinkError::from)?;
        while let Some(batch) = drain
            .next_batch(self.batch_rows, self.batch_bytes, self.budget.as_ref())
            .await
            .map_err(SinkError::from)?
        {
            self.apply_batch(batch, drain.commit_ts, drain.commit_lsn)
                .await?;
        }
        drain.finish().await.map_err(SinkError::from)?;
        if let Some((seq, rows)) = self.open.take() {
            self.ack.placed(seq, rows);
        }
        self.subxact_tracker.forget_tree(xid);
        Ok(())
    }

    async fn apply_batch(
        &mut self,
        batch: DrainedBatch,
        commit_ts: i64,
        commit_lsn: u64,
    ) -> std::result::Result<(), SinkError> {
        let walk = batch.into_walk();
        let ref_maps: Vec<&ChunkRefMap> = walk.chunks.iter().map(|g| g.map()).collect();
        // One spool per transaction
        let spool = walk.chunks.iter().find_map(|g| g.spool());
        let mut rows_cursor = 0usize;
        for step in walk.steps {
            match step {
                WalkStep::Rows { upto } => {
                    if upto > rows_cursor {
                        self.resolver
                            .put_row_refs(walk.new_rows.spool(), &walk.new_rows[rows_cursor..upto])
                            .await
                            .map_err(|e| SinkError::Other(format!("toast store put: {e}")))?;
                        rows_cursor = upto;
                    }
                }
                // Live stream owns DDL/config apply
                WalkStep::Event(DrainEntry::Catalog(_))
                | WalkStep::Event(DrainEntry::Config(_)) => {}
                WalkStep::Event(DrainEntry::ToastBarrier {
                    toast_relid,
                    marker_lsn,
                }) => {
                    self.resolver
                        .rewrite_barrier(toast_relid, marker_lsn, commit_lsn)
                        .await
                        .map_err(|e| SinkError::Other(format!("toast rewrite barrier: {e}")))?;
                }
                WalkStep::Truncate(_) => {
                    // xl_heap_truncate carries no block ref, never passes the
                    // rfn filter
                    debug_assert!(false, "TRUNCATE heap in gap replay");
                }
                WalkStep::Heap(mut heap) => {
                    let rfn = heap.decoded.rfn;
                    // Decode TOAST chunks, route through parent row
                    let Some((rel, through)) = self.targets.get(&(rfn.db_node, rfn.rel_node))
                    else {
                        continue;
                    };
                    if commit_lsn <= self.from_lsn {
                        // Walked pages cover commits through from_lsn
                        self.replay.commits_below_from += 1;
                        continue;
                    }
                    if commit_lsn > *through {
                        self.replay.commits_past_through += 1;
                        continue;
                    }
                    let Some(route) = self.routes.get(&rel.rel_name).cloned() else {
                        self.stats
                            .unsupported_relations
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        continue;
                    };
                    // Skip deletes for append-only destinations
                    if route.drops_deletes()
                        && matches!(heap.decoded.op, crate::decode::heap_decoder::HeapOp::Delete)
                    {
                        self.stats
                            .deletes_discarded
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        continue;
                    }
                    let rel = rel.clone();
                    let value_permit = detoast_heap(&mut heap, spool, &ref_maps, &self.resolver)
                        .await
                        .map_err(SinkError::from)?;
                    let seq = if let Some((seq, rows)) = &mut self.open {
                        *rows += 1;
                        *seq
                    } else {
                        let seq = self.replay.next_seq;
                        self.replay.next_seq += 1;
                        self.ack.register(seq, commit_lsn);
                        self.open = Some((seq, 1));
                        seq
                    };
                    self.msg_tx
                        .send(BatcherMsg::Row(RoutedRow {
                            seq,
                            rel,
                            route,
                            committed: CommittedTuple {
                                decoded: heap.decoded,
                                commit_ts,
                                commit_lsn,
                            },
                            value_permit: value_permit.map(Arc::new),
                        }))
                        .await
                        .map_err(|_| SinkError::Other("wal_replay: tail closed".into()))?;
                    self.replay.rows_replayed += 1;
                }
            }
        }
        Ok(())
    }
}

impl RecordSink for WalReplaySink {
    fn on_record<'a>(
        &'a mut self,
        record: &'a Record<'a>,
    ) -> Pin<Box<dyn Future<Output = std::result::Result<(), SinkError>> + Send + 'a>> {
        Box::pin(async move {
            let rm = record.parsed.header.resource_manager_id;
            if rm == RmId::Heap as u8 || rm == RmId::Heap2 as u8 {
                if let Some(rel) = record.parsed.blocks.first().map(|b| b.header.location.rel) {
                    if self.filter_rfns.contains(&(rel.db_node, rel.rel_node)) {
                        self.decoder.on_record(record).await?;
                    } else if self.whole_db_filter
                        && rel.db_node == self.log.db_oid()
                        && rel.rel_node >= FIRST_NORMAL_OBJECT_ID
                    {
                        self.replay.unknown_rfns += 1;
                    }
                }
            } else if rm == RmId::Xact as u8 {
                let info = record.parsed.header.info;
                let xid = record.parsed.header.xact_id;
                match info & XLOG_XACT_OPMASK {
                    XLOG_XACT_COMMIT | XLOG_XACT_COMMIT_PREPARED => {
                        self.on_commit(xid, info, record).await?;
                    }
                    XLOG_XACT_ABORT | XLOG_XACT_ABORT_PREPARED => {
                        let payload =
                            parse_xact_payload(info, &record.parsed.main_data, record.page_magic)
                                .map_err(|e| {
                                SinkError::Other(format!("wal_replay: abort payload: {e}"))
                            })?;
                        // ABORT PREPARED keys off the prepared xid too
                        let xid = payload.twophase_xid.unwrap_or(xid);
                        if let Some(patch) = &self.patch {
                            patch
                                .lock()
                                .expect("wal_replay patch lock")
                                .abort(xid, &payload.subxacts);
                        }
                        self.buffer
                            .lock()
                            .await
                            .abort(xid, record.source_lsn, &payload.subxacts)
                            .await
                            .map_err(SinkError::from)?;
                        self.subxact_tracker.forget_tree(xid);
                    }
                    XLOG_XACT_ASSIGNMENT => {
                        // Assignment only guides eviction policy
                        if let Some((xtop, subs)) = parse_xact_assignment(&record.parsed.main_data)
                        {
                            self.subxact_tracker.assign(xtop, &subs);
                        }
                    }
                    _ => {
                        // PREPARE / INVALIDATIONS unhandled; xact stays
                        // buffered until COMMIT_PREPARED
                    }
                }
            }
            Ok(())
        })
    }
}

/// Discard segment output while retaining record dispatch
pub struct DropSegments;

impl SegmentSink for DropSegments {
    fn on_segment<'a>(
        &'a mut self,
        _seg: SegmentName,
        _bytes: &'a [u8],
        _manifest: &'a Manifest,
    ) -> Pin<Box<dyn Future<Output = std::result::Result<(), SinkError>> + Send + 'a>> {
        Box::pin(std::future::ready(Ok(())))
    }
}

/// Drive fetched segments through `RecordSink` in LSN order
pub async fn pump_segments_through(
    segments: &[(SegmentName, PathBuf)],
    timeline: u32,
    target_db_oid: Oid,
    sink: &mut (dyn RecordSink + Send),
) -> Result<()> {
    let Some((first, _)) = segments.first() else {
        return Ok(());
    };
    let mut stream = WalStream::new(timeline, WAL_SEG_SIZE, first.start_lsn(WAL_SEG_SIZE))
        .map_err(|e| anyhow::anyhow!("wal_replay: WalStream: {e}"))?;
    stream.filter_mut().set_target_db(target_db_oid);
    let mut seg_sink = DropSegments;
    for (seg, path) in segments {
        let bytes = tokio::fs::read(path)
            .await
            .with_context(|| format!("read {}", path.display()))?;
        stream
            .push(seg.start_lsn(WAL_SEG_SIZE), &bytes, sink, &mut seg_sink)
            .await
            .map_err(|e| anyhow::anyhow!("wal_replay: {}: {e}", seg.format()))?;
    }
    stream
        .close(None, sink)
        .await
        .map_err(|e| anyhow::anyhow!("wal_replay: close: {e}"))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::desc_log::DescLogIdentity;
    use crate::decode::visibility::{PgXactAccum, PgXactView, XidStatus};
    use crate::decode::wal_xact::{
        XACT_XINFO_HAS_SUBXACTS, XACT_XINFO_HAS_TWOPHASE, XLOG_XACT_HAS_INFO,
    };
    use crate::emit::pipeline::ack;
    use crate::pos::{EmitterAck, Monotone};
    use crate::record::Record;
    use std::path::Path;
    use walrus::pg::walparser::{XLogRecord, XLogRecordHeader};

    const DB: Oid = 5;
    /// Backend that runs COMMIT PREPARED; not the xact that wrote the rows
    const FINISHER_XID: u32 = 777;
    const PREPARED_XID: u32 = 4242;
    const PREPARED_SUBXID: u32 = 4243;

    fn xact_record(op: u8, xid: u32, subxacts: &[u32], twophase: Option<u32>) -> Record<'static> {
        let mut md: Vec<u8> = 0i64.to_le_bytes().to_vec();
        let mut xinfo = 0u32;
        if !subxacts.is_empty() {
            xinfo |= XACT_XINFO_HAS_SUBXACTS;
        }
        if twophase.is_some() {
            xinfo |= XACT_XINFO_HAS_TWOPHASE;
        }
        md.extend_from_slice(&xinfo.to_le_bytes());
        if !subxacts.is_empty() {
            md.extend_from_slice(&(subxacts.len() as i32).to_le_bytes());
            for sub in subxacts {
                md.extend_from_slice(&sub.to_le_bytes());
            }
        }
        if let Some(prepared) = twophase {
            md.extend_from_slice(&prepared.to_le_bytes());
        }
        Record {
            parsed: XLogRecord {
                header: XLogRecordHeader {
                    resource_manager_id: RmId::Xact as u8,
                    info: op | XLOG_XACT_HAS_INFO,
                    xact_id: xid,
                    ..Default::default()
                },
                main_data: std::borrow::Cow::Owned(md),
                ..Default::default()
            },
            source_lsn: 0x5000,
            next_lsn: 0x5100,
            page_magic: 0xD116,
            ..Default::default()
        }
    }

    /// Sink with no targets: the xact records under test carry no rows, so
    /// only the patch and the buffer see them
    async fn patch_sink(dir: &Path, patch: &Arc<std::sync::Mutex<PgXactPatch>>) -> WalReplaySink {
        let desc_dir = dir.join("desc_log");
        tokio::fs::create_dir_all(&desc_dir).await.unwrap();
        let log = DescriptorLog::open(
            &desc_dir,
            DescLogIdentity {
                pg_major: 17,
                system_id: "7300000000000000001".into(),
                timeline: 1,
                db_oid: DB,
                wal_seg_size: WAL_SEG_SIZE as u32,
            },
        )
        .await
        .unwrap();
        let spill = dir.join("xact_spill");
        tokio::fs::create_dir_all(&spill).await.unwrap();
        let buffer = Arc::new(Mutex::new(
            XactBuffer::new(crate::xact::xact_buffer::XactBufferConfig::new(spill)).unwrap(),
        ));
        let (msg_tx, _msg_rx) = mpsc::channel(8);
        let (ack, _collector) = ack::spawn(Arc::new(Monotone::<EmitterAck>::new(0)));
        WalReplaySink::new(WalReplayInputs {
            log: Arc::new(log),
            buffer,
            resolver: ToastResolver::disabled(),
            filter_rfns: HashSet::default(),
            targets: ReplayTargets::default(),
            from_lsn: 0,
            whole_db_filter: false,
            mapping: Arc::default(),
            stats: Arc::new(EmitterStats::default()),
            budget: None,
            row_policy: Default::default(),
            config: None,
            batch_rows: 64,
            batch_bytes: 1 << 20,
            msg_tx,
            ack,
            next_seq: 0,
            patch: Some(patch.clone()),
        })
    }

    fn status(patch: &PgXactPatch, xid: u32) -> XidStatus {
        let accum = PgXactAccum::new();
        PgXactView::new(&accum, patch).xid_status(xid)
    }

    /// Patch prepared xid, not finishing backend xid
    #[tokio::test]
    async fn commit_prepared_patches_the_prepared_xid() {
        let tmp = tempfile::tempdir().unwrap();
        let patch = Arc::new(std::sync::Mutex::new(PgXactPatch::new()));
        let mut sink = patch_sink(tmp.path(), &patch).await;
        sink.on_record(&xact_record(
            XLOG_XACT_COMMIT_PREPARED,
            FINISHER_XID,
            &[PREPARED_SUBXID],
            Some(PREPARED_XID),
        ))
        .await
        .unwrap();
        let patch = patch.lock().unwrap();
        assert_eq!(status(&patch, PREPARED_XID), XidStatus::Committed);
        assert_eq!(status(&patch, PREPARED_SUBXID), XidStatus::Committed);
        assert_ne!(status(&patch, FINISHER_XID), XidStatus::Committed);
    }

    #[tokio::test]
    async fn abort_prepared_patches_the_prepared_xid() {
        let tmp = tempfile::tempdir().unwrap();
        let patch = Arc::new(std::sync::Mutex::new(PgXactPatch::new()));
        let mut sink = patch_sink(tmp.path(), &patch).await;
        sink.on_record(&xact_record(
            XLOG_XACT_ABORT_PREPARED,
            FINISHER_XID,
            &[],
            Some(PREPARED_XID),
        ))
        .await
        .unwrap();
        let patch = patch.lock().unwrap();
        assert_eq!(status(&patch, PREPARED_XID), XidStatus::Aborted);
        assert_ne!(status(&patch, FINISHER_XID), XidStatus::Aborted);
    }

    /// Reject payloads missing subxact list
    #[tokio::test]
    async fn malformed_xact_payload_stops_the_leg() {
        let tmp = tempfile::tempdir().unwrap();
        let patch = Arc::new(std::sync::Mutex::new(PgXactPatch::new()));
        let mut sink = patch_sink(tmp.path(), &patch).await;
        let mut rec = xact_record(XLOG_XACT_COMMIT, 900, &[901], None);
        rec.parsed.main_data = std::borrow::Cow::Owned(vec![0u8; 10]);
        assert!(sink.on_record(&rec).await.is_err());
        let mut rec = xact_record(XLOG_XACT_ABORT, 900, &[901], None);
        rec.parsed.main_data = std::borrow::Cow::Owned(vec![0u8; 10]);
        assert!(sink.on_record(&rec).await.is_err());
    }
}
