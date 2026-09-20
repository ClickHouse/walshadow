//! Decode bounded WAL ranges through shared transaction pipeline
//!
//! Used by greenfield window replay and object-store gap replay. Emit commits
//! above page-walk coverage and through each target's upper bound

use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;

use anyhow::{Context, Result};
use tokio::sync::{Mutex, mpsc};
use walrus::pg::wal::segment::SegmentName;
use walrus::pg::walparser::{Oid, RmId};

use crate::budget::{MemoryBudget, MemoryPermit, acquire_opt};
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
use crate::toast::{ChunkRefMap, ToastResolver, ToastRow, ToastRowRef};
use crate::xact::spill::BodySpoolFile;
use crate::xact::xact_buffer::{
    BufferingDecoderSink, DrainEntry, DrainedBatch, SubxactTracker, WalkStep, XactBuffer,
    detoast_heap, heap_reads_toast, resolve_stash,
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
    /// Rows waiting for next mirror write
    pending_rows: Vec<ToastRow>,
    pending_bytes: usize,
    /// Leaf permits covering the materialized bodies in `pending_rows`
    pending_permits: Vec<MemoryPermit>,
    /// Resident ceiling for `pending_rows`, inside the leaf share so a
    /// held batch never withholds bytes a drain admit waits on
    pending_cap: usize,
    replay: ReplayStats,
}

impl WalReplaySink {
    pub fn new(inputs: WalReplayInputs) -> Self {
        let pending_cap = inputs
            .resolver
            .budget()
            .map_or(usize::MAX, MemoryBudget::leaf_max);
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
            pending_rows: Vec::new(),
            pending_bytes: 0,
            pending_permits: Vec::new(),
            pending_cap,
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

    /// Mirror writes flushed, seq boundary reported. Commits close their own
    /// seq, so a segment boundary leaves none open
    pub async fn segment_boundary(&mut self) -> std::result::Result<u64, SinkError> {
        self.flush_rows().await?;
        Ok(self.replay.next_seq)
    }

    /// Lowest first-record LSN still buffered. A resume above it would drop
    /// the prefix of a transaction that commits later
    pub async fn oldest_inflight(&self) -> Option<u64> {
        self.buffer
            .lock()
            .await
            .inflight_snapshot()
            .into_iter()
            .map(|e| e.first_lsn)
            .min()
    }

    /// Flush pending rows and finish replay
    pub async fn finish(mut self) -> std::result::Result<ReplayStats, SinkError> {
        self.flush_rows().await?;
        Ok(self.stats())
    }

    /// Mirror all chunks from restored TOAST page image. Bypass transaction
    /// stash because neighboring tuples may belong to other transactions.
    ///
    /// Repair backup pages copied during concurrent writes.
    ///
    /// Process Heap images and XLOG_FPI_FOR_HINT images. Deletes of pre-backup
    /// chunks may have only latter image.
    async fn harvest_toast_images(
        &mut self,
        record: &Record<'_>,
    ) -> std::result::Result<(), SinkError> {
        if !self.resolver.stores_chunks() {
            return Ok(());
        }
        let mut rows = Vec::new();
        for block in record.parsed.blocks.iter().filter(|b| b.header.has_image()) {
            if i32::from(block.header.fork_num()) != crate::filter::main_data::MAIN_FORKNUM {
                continue;
            }
            let rfn = block.header.location.rel;
            let Ok((rel, _)) = self.log.descriptor_at_spanned(rfn, record.next_lsn) else {
                continue;
            };
            if rel.kind != 't' {
                continue;
            }
            let page = crate::decode::fpi::restore_block_image(block, record.page_magic)
                .map_err(|e| SinkError::Other(format!("wal_replay: toast image restore: {e}")))?;
            rows.extend(crate::backfill::backup_page_walk::toast_rows_from_page(
                &page,
                &rel,
                block.header.location.block_no,
                crate::backfill::backup_page_walk::page_pd_lsn(&page),
            ));
        }
        if rows.is_empty() {
            return Ok(());
        }
        self.stats
            .toast_image_rows_mirrored
            .fetch_add(rows.len() as u64, std::sync::atomic::Ordering::Relaxed);
        for row in rows {
            self.queue_row(row).await?;
        }
        Ok(())
    }

    /// Seal a batch that reached a put limit, or that the next body would
    /// push past the resident cap, then cover the body the caller is
    /// about to hold. Sealing first keeps held permits inside
    /// `pending_cap`, so the acquire never waits on units this sink holds
    async fn reserve_pending(
        &mut self,
        bytes: usize,
    ) -> std::result::Result<Option<MemoryPermit>, SinkError> {
        if self
            .resolver
            .put_limit_reached(self.pending_rows.len(), self.pending_bytes)
            || self.pending_bytes + bytes > self.pending_cap
        {
            self.flush_rows().await?;
        }
        Ok(acquire_opt(self.resolver.budget(), bytes).await)
    }

    fn push_pending(&mut self, row: ToastRow, permit: Option<MemoryPermit>) {
        self.pending_bytes += row.chunk_data.len();
        self.pending_permits.extend(permit);
        self.pending_rows.push(row);
    }

    /// Buffer rows to reduce store round trips. Flush before reads, at batch
    /// limit, and when replay closes
    async fn queue_row(&mut self, row: ToastRow) -> std::result::Result<(), SinkError> {
        let permit = self.reserve_pending(row.chunk_data.len()).await?;
        self.push_pending(row, permit);
        Ok(())
    }

    /// Load file-backed data before transaction spool is removed
    async fn queue_rows(
        &mut self,
        spool: Option<&BodySpoolFile>,
        rows: &[ToastRowRef],
    ) -> std::result::Result<(), SinkError> {
        if !self.resolver.stores_chunks() {
            return Ok(());
        }
        for r in rows {
            // Permit before the read: the body is resident from here
            let permit = self.reserve_pending(r.chunk_data.len()).await?;
            let row = r
                .materialize(spool)
                .map_err(|e| SinkError::Other(format!("wal_replay: toast body load: {e}")))?;
            self.push_pending(row, permit);
        }
        Ok(())
    }

    /// Write pending rows to chunk store
    async fn flush_rows(&mut self) -> std::result::Result<(), SinkError> {
        if self.pending_rows.is_empty() {
            return Ok(());
        }
        let rows = std::mem::take(&mut self.pending_rows);
        self.pending_bytes = 0;
        // Bodies stay covered until the write lands
        let _permits = std::mem::take(&mut self.pending_permits);
        self.resolver
            .put_batched(&rows)
            .await
            .map_err(|e| SinkError::Other(format!("wal_replay: write toast rows: {e}")))
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
                        self.queue_rows(walk.new_rows.spool(), &walk.new_rows[rows_cursor..upto])
                            .await?;
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
                    // Barrier reads rows from current rewrite
                    self.flush_rows().await?;
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
                    // Make earlier commits available before resolving values.
                    // Inline-only heaps read nothing: flushing for them
                    // shreds the batch on a row-heavy segment. Gate stays
                    // as wide as detoast's leaf acquire: narrowing it to
                    // store-bound pointers would hold pending leaf permits
                    // across that acquire, which can wait on them
                    if heap_reads_toast(&heap) {
                        self.flush_rows().await?;
                    }
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
            // Capture TOAST page images from Heap and XLOG resource managers
            if crate::filter::classify::is_page_image(&record.parsed) {
                self.harvest_toast_images(record).await?;
            }
            if rm == RmId::Heap as u8 || rm == RmId::Heap2 as u8 {
                if let Some(rel) = record.parsed.blocks.first().map(|b| b.header.location.rel) {
                    if self.filter_rfns.contains(&(rel.db_node, rel.rel_node)) {
                        self.harvest_toast_images(record).await?;
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

/// Segment-at-a-time driver behind [`pump_segments_through`]. Split out so a
/// resumable caller can checkpoint between segments without closing the
/// stream, which would strand a record spanning the boundary
pub struct SegmentPump {
    stream: WalStream,
    seg_sink: DropSegments,
}

impl SegmentPump {
    pub fn start(first: &SegmentName, target_db_oid: Oid) -> Result<Self> {
        let mut stream =
            WalStream::new(first.timeline, WAL_SEG_SIZE, first.start_lsn(WAL_SEG_SIZE))
                .map_err(|e| anyhow::anyhow!("wal_replay: WalStream: {e}"))?;
        stream.filter_mut().set_target_db(target_db_oid);
        Ok(Self {
            stream,
            seg_sink: DropSegments,
        })
    }

    pub async fn push(
        &mut self,
        seg: &SegmentName,
        path: &Path,
        sink: &mut (dyn RecordSink + Send),
    ) -> Result<()> {
        if seg.timeline != self.stream.timeline() {
            self.stream
                .adopt_timeline(seg.timeline)
                .map_err(|e| anyhow::anyhow!("wal_replay: {}: {e}", seg.format()))?;
        }
        let bytes = tokio::fs::read(path)
            .await
            .with_context(|| format!("read {}", path.display()))?;
        self.stream
            .push(
                seg.start_lsn(WAL_SEG_SIZE),
                &bytes,
                sink,
                &mut self.seg_sink,
            )
            .await
            .map_err(|e| anyhow::anyhow!("wal_replay: {}: {e}", seg.format()))?;
        Ok(())
    }

    pub async fn close(self, sink: &mut (dyn RecordSink + Send)) -> Result<()> {
        self.stream
            .close(None, sink)
            .await
            .map_err(|e| anyhow::anyhow!("wal_replay: close: {e}"))?;
        Ok(())
    }
}

/// Replay fetched segments in LSN order, following each segment's timeline
pub async fn pump_segments_through(
    segments: &[(SegmentName, PathBuf)],
    target_db_oid: Oid,
    sink: &mut (dyn RecordSink + Send),
) -> Result<()> {
    let Some((first, _)) = segments.first() else {
        return Ok(());
    };
    let mut pump = SegmentPump::start(first, target_db_oid)?;
    for (seg, path) in segments {
        pump.push(seg, path, sink).await?;
    }
    pump.close(sink).await
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

    /// Heap record carrying one TOAST page image and nothing else, the
    /// shape an FPI takes when a backup forces page writes
    fn image_record(page: &[u8], rel_node: Oid, block_no: u32) -> Record<'static> {
        use walrus::pg::walparser::{
            BKP_BLOCK_HAS_IMAGE, BlockLocation, XLogRecordBlock, XLogRecordBlockHeader,
            XLogRecordBlockImageHeader,
        };
        let mut header = XLogRecordBlockHeader::new(0);
        header.fork_flags = BKP_BLOCK_HAS_IMAGE;
        header.image_header = XLogRecordBlockImageHeader {
            image_length: page.len() as u16,
            hole_offset: 0,
            hole_length: 0,
            info: 0,
        };
        header.location = BlockLocation::new(1663, DB, rel_node, block_no);
        Record {
            parsed: XLogRecord {
                header: XLogRecordHeader {
                    resource_manager_id: RmId::Heap as u8,
                    info: crate::decode::heap_decoder::XLOG_HEAP_INSERT,
                    xact_id: 55,
                    ..Default::default()
                },
                blocks: vec![XLogRecordBlock {
                    header,
                    image: std::borrow::Cow::Owned(page.to_vec()),
                    data: std::borrow::Cow::Borrowed(&[]),
                }],
                ..Default::default()
            },
            source_lsn: 0x7000,
            next_lsn: 0x7100,
            page_magic: walrus::pg::walparser::XLP_PAGE_MAGIC_PG15,
            ..Default::default()
        }
    }

    /// Neighbours on a restored image reach the mirror, so a value whose
    /// chunks the backup copy lost still assembles. They date from the
    /// page's own version, below the walked referrer's bound
    #[tokio::test]
    async fn image_harvest_mirrors_every_chunk_on_the_page() {
        use crate::backfill::backup_page_walk::{synth_toast_chunk_page, toast_chunk_rel};
        use crate::toast::{ChunkStore, FetchedValue, MemChunkStore};

        let dir = tempfile::tempdir().unwrap();
        let rel = Arc::new(toast_chunk_rel());
        let mut catalog = crate::backfill::backup_page_walk::CatalogMap::new();
        catalog.insert(rel.clone());
        let log = seed_test_log(dir.path(), &catalog).await;
        let store = Arc::new(MemChunkStore::new());
        let resolver = ToastResolver::with_store(store.clone(), Arc::new(EmitterStats::default()));
        let mut sink = image_sink(dir.path(), log, resolver).await;

        let page = synth_toast_chunk_page(
            &[
                (7, 0, b"first".as_slice()),
                (8, 0, b"only".as_slice()),
                (7, 1, b"third".as_slice()),
            ],
            0x6000,
        );
        let record = image_record(&page, rel.rfn.rel_node, 3);
        sink.harvest_toast_images(&record).await.unwrap();
        sink.finish().await.unwrap();

        // Referrer bound is the walk's `start_lsn`, above the page version
        assert_eq!(
            store.fetch(rel.oid, 7, 0x6800, 10).await.unwrap(),
            FetchedValue::Assembled(b"firstthird".to_vec()),
        );
        assert_eq!(
            store.fetch(rel.oid, 8, 0x6800, 4).await.unwrap(),
            FetchedValue::Assembled(b"only".to_vec()),
        );
    }

    /// Batch image rows from multiple records into one store write
    #[tokio::test]
    async fn image_harvest_batches_rows_across_records() {
        use crate::backfill::backup_page_walk::{synth_toast_chunk_page, toast_chunk_rel};
        use crate::toast::{ChunkStore, FetchedValue, MemChunkStore};
        use std::sync::atomic::Ordering;

        let dir = tempfile::tempdir().unwrap();
        let rel = Arc::new(toast_chunk_rel());
        let mut catalog = crate::backfill::backup_page_walk::CatalogMap::new();
        catalog.insert(rel.clone());
        let log = seed_test_log(dir.path(), &catalog).await;
        let store = Arc::new(MemChunkStore::new());
        let stats = Arc::new(EmitterStats::default());
        let resolver = ToastResolver::with_store(store.clone(), stats.clone());
        let mut sink = image_sink(dir.path(), log, resolver).await;

        for (block_no, value_id) in [(3u32, 7u32), (4, 8)] {
            let page = synth_toast_chunk_page(&[(value_id, 0, b"body".as_slice())], 0x6000);
            let record = image_record(&page, rel.rfn.rel_node, block_no);
            sink.harvest_toast_images(&record).await.unwrap();
        }
        assert_eq!(stats.toast_chunk_puts.load(Ordering::Relaxed), 0);

        sink.finish().await.unwrap();
        assert_eq!(stats.toast_chunk_puts.load(Ordering::Relaxed), 1);
        for value_id in [7, 8] {
            assert_eq!(
                store.fetch(rel.oid, value_id, 0x6800, 4).await.unwrap(),
                FetchedValue::Assembled(b"body".to_vec()),
            );
        }
    }

    /// Queued bodies live off the store until flush, so they must ride
    /// permits the way the per-slice put they replaced did
    #[tokio::test]
    async fn queued_rows_hold_budget_until_flush() {
        use crate::backfill::backup_page_walk::{synth_toast_chunk_page, toast_chunk_rel};
        use crate::toast::MemChunkStore;

        let dir = tempfile::tempdir().unwrap();
        let rel = Arc::new(toast_chunk_rel());
        let mut catalog = crate::backfill::backup_page_walk::CatalogMap::new();
        catalog.insert(rel.clone());
        let log = seed_test_log(dir.path(), &catalog).await;
        let budget = MemoryBudget::new(1 << 20);
        let resolver = ToastResolver::with_store(
            Arc::new(MemChunkStore::new()),
            Arc::new(EmitterStats::default()),
        )
        .with_budget(budget.clone());
        let mut sink = image_sink(dir.path(), log, resolver).await;

        let page = synth_toast_chunk_page(&[(7, 0, b"body".as_slice())], 0x6000);
        let record = image_record(&page, rel.rfn.rel_node, 3);
        sink.harvest_toast_images(&record).await.unwrap();
        assert_eq!(budget.resident_bytes(), 4);

        sink.finish().await.unwrap();
        assert_eq!(budget.resident_bytes(), 0);
    }

    async fn seed_test_log(
        dir: &Path,
        catalog: &crate::backfill::backup_page_walk::CatalogMap,
    ) -> Arc<DescriptorLog> {
        use crate::catalog::desc_log::{BatchRecord, LogEntry, LogValue};
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
        log.seed(
            BatchRecord {
                captured_at: 0x1000,
                commit_lsn: 0,
                observations: Vec::new(),
                ambiguities: Vec::new(),
                entries: catalog
                    .descriptors()
                    .map(|d| {
                        Arc::new(LogEntry {
                            valid_from: 0x1000,
                            oid: d.oid,
                            rfn: d.rfn,
                            value: LogValue::Present(d.clone()),
                        })
                    })
                    .collect(),
            },
            0x1000,
        )
        .await
        .unwrap();
        Arc::new(log)
    }

    async fn image_sink(
        dir: &Path,
        log: Arc<DescriptorLog>,
        resolver: ToastResolver,
    ) -> WalReplaySink {
        let spill = dir.join("image_spill");
        tokio::fs::create_dir_all(&spill).await.unwrap();
        let buffer = Arc::new(Mutex::new(
            XactBuffer::new(crate::xact::xact_buffer::XactBufferConfig::new(spill)).unwrap(),
        ));
        let (msg_tx, _msg_rx) = mpsc::channel(8);
        let (ack, _collector) = ack::spawn(Arc::new(Monotone::<EmitterAck>::new(0)));
        WalReplaySink::new(WalReplayInputs {
            log,
            buffer,
            resolver,
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
            patch: None,
        })
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

    #[derive(Default)]
    struct CountingSink(usize);

    impl RecordSink for CountingSink {
        fn on_record<'a>(
            &'a mut self,
            _record: &'a Record<'a>,
        ) -> Pin<Box<dyn Future<Output = std::result::Result<(), SinkError>> + Send + 'a>> {
            self.0 += 1;
            Box::pin(std::future::ready(Ok(())))
        }
    }

    /// Write two records followed by zeros, which mark end of valid WAL
    async fn write_segment(dir: &Path, seg: SegmentName) -> PathBuf {
        use walrus::pg::walparser::{
            RmId, WAL_PAGE_SIZE, X_LOG_RECORD_HEADER_SIZE, XLP_LONG_HEADER, XLP_PAGE_MAGIC_PG15,
            XLR_BLOCK_ID_DATA_SHORT,
        };
        let mut rec = Vec::new();
        rec.extend_from_slice(&0u32.to_le_bytes()); // total_len backpatched
        rec.extend_from_slice(&0u32.to_le_bytes()); // xid
        rec.extend_from_slice(&0u64.to_le_bytes()); // prev
        rec.push(0); // info
        rec.push(RmId::Clog as u8);
        rec.extend_from_slice(&[0u8; 2]); // pad
        rec.extend_from_slice(&0u32.to_le_bytes()); // crc backpatched
        rec.push(XLR_BLOCK_ID_DATA_SHORT);
        rec.push(4);
        rec.extend_from_slice(&[0xDEu8; 4]);
        let total = (X_LOG_RECORD_HEADER_SIZE + 6) as u32;
        assert_eq!(total as usize, rec.len());
        rec[0..4].copy_from_slice(&total.to_le_bytes());
        let crc = crate::filter::rewrite::compute_crc(&rec);
        rec[20..24].copy_from_slice(&crc.to_le_bytes());

        let mut page = Vec::with_capacity(WAL_PAGE_SIZE as usize);
        page.extend_from_slice(&XLP_PAGE_MAGIC_PG15.to_le_bytes());
        page.extend_from_slice(&XLP_LONG_HEADER.to_le_bytes());
        page.extend_from_slice(&seg.timeline.to_le_bytes());
        page.extend_from_slice(&seg.start_lsn(WAL_SEG_SIZE).to_le_bytes());
        page.extend_from_slice(&0u32.to_le_bytes()); // remaining_data_len
        page.extend_from_slice(&12345u64.to_le_bytes()); // sysid
        page.extend_from_slice(&(WAL_SEG_SIZE as u32).to_le_bytes());
        page.extend_from_slice(&(WAL_PAGE_SIZE as u32).to_le_bytes());
        page.extend_from_slice(&[0u8; 4]); // pad to 40
        for _ in 0..2 {
            page.extend_from_slice(&rec);
            let pad = (8 - (page.len() % 8)) % 8;
            page.extend(std::iter::repeat_n(0u8, pad));
        }

        let path = dir.join(seg.format());
        let mut file = tokio::fs::File::create(&path).await.unwrap();
        tokio::io::AsyncWriteExt::write_all(&mut file, &page)
            .await
            .unwrap();
        // Keep zero tail sparse
        file.set_len(WAL_SEG_SIZE).await.unwrap();
        path
    }

    #[tokio::test]
    async fn pump_crosses_a_timeline_switch_between_segments() {
        let tmp = tempfile::tempdir().unwrap();
        let mut segments = Vec::new();
        for (timeline, seg_no) in [(1u32, 1u32), (2, 2)] {
            let seg = SegmentName {
                timeline,
                log_id: 0,
                seg_no,
            };
            segments.push((seg, write_segment(tmp.path(), seg).await));
        }
        let mut sink = CountingSink::default();
        pump_segments_through(&segments, DB, &mut sink)
            .await
            .unwrap();
        assert_eq!(sink.0, 4, "two records out of each branch's segment");
    }

    #[tokio::test]
    async fn pump_refuses_a_segment_below_the_branch_it_reached() {
        let tmp = tempfile::tempdir().unwrap();
        let mut segments = Vec::new();
        for (timeline, seg_no) in [(2u32, 1u32), (1, 2)] {
            let seg = SegmentName {
                timeline,
                log_id: 0,
                seg_no,
            };
            segments.push((seg, write_segment(tmp.path(), seg).await));
        }
        let mut sink = CountingSink::default();
        let err = pump_segments_through(&segments, DB, &mut sink)
            .await
            .expect_err("a descending branch is not a lineage");
        assert!(err.to_string().contains("timeline"), "{err}");
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
