//! Reorder worker — single-threaded commit-order coordinator.
//!
//! Runs as inner sink of the `QueueingRecordSink` worker, off the WAL pump
//! task (preserves the wire-shadow deadlock fix). Pairs with
//! [`BufferingDecoderSink`]; on each
//! COMMIT/ABORT assigns a dense `seq`, registers it with the collector in
//! order, then either dispatches to the decode pool or — for a DDL/TRUNCATE
//! barrier — quiesces, drains earlier seqs to durable, and applies the schema
//! change via [`DdlApplicator`] before resuming.
//!
//! Barrier coarseness is deliberate (DDL/TRUNCATE rare). Within a barrier
//! xact, data segments between catalog/truncate ops each get their own seq
//! and are fenced so a `TRUNCATE` (no `_lsn`, so can't ride
//! `ReplacingMergeTree` reconciliation) orders correctly against surrounding
//! inserts.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use tokio::sync::{Mutex, mpsc, oneshot};
use tokio::task::JoinHandle;
use walrus::pg::walparser::RmId;

use crate::ch_ddl::DdlApplicator;
use crate::ch_emitter::EmitterStats;
use crate::decoder_sink::DecoderSinkError;
use crate::heap_decoder::{DecodedHeap, HeapOp, XLOG_HEAP_OPMASK, XLOG_HEAP_TRUNCATE};
use crate::shadow_catalog::{CatalogError, SchemaEvent, ShadowCatalog};
use crate::wal_stream::{Record, RecordSink, SinkError};
use tracing::Instrument;

use crate::xact_buffer::{
    BufferingDecoderSink, DrainedXact, SchemaEventRx, SubxactTracker, TxnSpanRegistry,
    XLOG_XACT_ABORT, XLOG_XACT_ABORT_PREPARED, XLOG_XACT_ASSIGNMENT, XLOG_XACT_COMMIT,
    XLOG_XACT_COMMIT_PREPARED, XLOG_XACT_OPMASK, XactBuffer, drain_pending_schema_events,
    parse_xact_assignment, parse_xact_payload,
};

use crate::pipeline::ack::AckHandle;
use crate::pipeline::batcher::BatcherMsg;
use crate::pipeline::decode::{DecodeJob, ToastChunks};
use crate::pipeline::{Fatal, mpmc};
use crate::toast::ToastResolver;

pub struct ReorderSink {
    buffer: Arc<Mutex<XactBuffer>>,
    catalog: Arc<Mutex<ShadowCatalog>>,
    subxact_tracker: Arc<Mutex<SubxactTracker>>,
    schema_events: Option<SchemaEventRx>,
    pg_class_delete_epoch: Option<Arc<AtomicU64>>,
    last_seen_delete_epoch: u64,
    applicator: DdlApplicator,
    ack: AckHandle,
    jobs_tx: mpmc::Sender<DecodeJob>,
    /// Shared FIFO channel to the batcher; `FlushAll` here orders after
    /// enqueued rows.
    msg_tx: mpsc::Sender<BatcherMsg>,
    fatal: Fatal,
    /// Reorder owns the commit-order boundary, so bumps `xacts_committed`
    /// (per commit) and `truncates_emitted`.
    stats: Arc<EmitterStats>,
    /// TOAST chunk store: each commit's chunks persist here (disk / CH) so a
    /// later re-emit of a pre-window referrer can rebuild its value. No-op
    /// when disabled.
    resolver: ToastResolver,
    /// Dense commit-order counter; one seq per dispatched data unit.
    next_seq: u64,
    /// Per-txn span map (shared with the pump + buffer). `Some` only when
    /// OTLP tracing is on; reorder parents `commit.drain`/`dispatch` under
    /// the `txn` and prunes the entry at commit (the buffer prunes at abort).
    span_registry: Option<TxnSpanRegistry>,
}

impl ReorderSink {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        buffer: Arc<Mutex<XactBuffer>>,
        catalog: Arc<Mutex<ShadowCatalog>>,
        subxact_tracker: Arc<Mutex<SubxactTracker>>,
        schema_events: Option<SchemaEventRx>,
        pg_class_delete_epoch: Option<Arc<AtomicU64>>,
        applicator: DdlApplicator,
        ack: AckHandle,
        jobs_tx: mpmc::Sender<DecodeJob>,
        msg_tx: mpsc::Sender<BatcherMsg>,
        stats: Arc<EmitterStats>,
        resolver: ToastResolver,
        fatal: Fatal,
        span_registry: Option<TxnSpanRegistry>,
    ) -> Self {
        let last_seen_delete_epoch = pg_class_delete_epoch
            .as_ref()
            .map(|e| e.load(Ordering::Acquire))
            .unwrap_or(0);
        Self {
            buffer,
            catalog,
            subxact_tracker,
            schema_events,
            pg_class_delete_epoch,
            last_seen_delete_epoch,
            applicator,
            ack,
            jobs_tx,
            msg_tx,
            stats,
            resolver,
            fatal,
            next_seq: 0,
            span_registry,
        }
    }

    fn alloc_seq(&mut self) -> u64 {
        let s = self.next_seq;
        self.next_seq += 1;
        s
    }

    /// Allocate the next dense `seq` and register it with the ack collector at
    /// `commit_lsn`. The sharded coordinator calls this synchronously in WAL
    /// (commit) order so the contiguous watermark stays correct while shards
    /// dispatch in parallel.
    pub(crate) fn alloc_and_register(&mut self, commit_lsn: u64) -> u64 {
        let seq = self.alloc_seq();
        self.ack.register(seq, commit_lsn);
        seq
    }

    fn fatal_err(&self) -> SinkError {
        SinkError::Other(
            self.fatal
                .message()
                .unwrap_or_else(|| "pipeline fatal".into()),
        )
    }

    /// Drain pending DROP events (post-`sweep_dropped`) into the buffer keyed
    /// on `(xid, source_lsn)`. Mirrors
    /// `XactRecordSink::route_pending_schema_events`.
    async fn route_pending_schema_events(
        &mut self,
        xid: u32,
        source_lsn: u64,
    ) -> Result<(), SinkError> {
        let Some(rx) = self.schema_events.as_ref() else {
            return Ok(());
        };
        let pending = drain_pending_schema_events(rx);
        if pending.is_empty() {
            return Ok(());
        }
        let mut buf = self.buffer.lock().await;
        for ev in pending {
            buf.on_schema_event(xid, source_lsn, ev);
        }
        Ok(())
    }

    /// Poll-based DROP discovery at commit, gated on the pg_class delete epoch
    /// so ADD COLUMN / VACUUM noise doesn't sweep. Same as `XactRecordSink`'s
    /// commit branch.
    async fn maybe_sweep_dropped(&mut self, xid: u32, source_lsn: u64) -> Result<(), SinkError> {
        if self.schema_events.is_none() {
            return Ok(());
        }
        let current = self
            .pg_class_delete_epoch
            .as_ref()
            .map(|e| e.load(Ordering::Acquire))
            .unwrap_or(self.last_seen_delete_epoch);
        if current == self.last_seen_delete_epoch {
            return Ok(());
        }
        let dropped = {
            let mut cat = self.catalog.lock().await;
            if source_lsn > 0 {
                cat.wait_for_replay(source_lsn)
                    .await
                    .map_err(|e| SinkError::from(DecoderSinkError::from(e)))?;
            }
            cat.sweep_dropped()
                .await
                .map_err(|e| SinkError::from(DecoderSinkError::from(e)))?
        };
        self.last_seen_delete_epoch = current;
        if dropped > 0 {
            self.route_pending_schema_events(xid, source_lsn).await?;
        }
        Ok(())
    }

    // Helpers take `&mut self` so the borrow across awaits is `&mut Self`
    // (Send): owned `DdlApplicator`/`AsyncClient` is Send but not Sync, so a
    // shared `&Self` across an await wouldn't be Send.
    async fn dispatch_job(&mut self, job: DecodeJob) -> Result<(), SinkError> {
        self.stats.queue_jobs_out.fetch_add(1, Ordering::Relaxed);
        tokio::select! {
            r = self.jobs_tx.send(job) => r.map_err(|_| SinkError::Other("decode job queue closed".into())),
            _ = self.fatal.wait() => Err(self.fatal_err()),
        }
    }

    /// Seal every batcher table and wait for the reply. Sent on the shared row
    /// channel so it orders after every row enqueued before it.
    async fn flush_all_batcher(&mut self) -> Result<(), SinkError> {
        let (tx, rx) = oneshot::channel();
        if self.msg_tx.send(BatcherMsg::FlushAll(tx)).await.is_err() {
            return Err(SinkError::Other("batcher channel closed".into()));
        }
        tokio::select! {
            r = rx => r.map_err(|_| SinkError::Other("batcher dropped flush ack".into())),
            _ = self.fatal.wait() => Err(self.fatal_err()),
        }
    }

    /// Wait until every dispatched seq is *placed* (decode pool routed all
    /// their rows onto the shared channel), so a `FlushAll` orders after them.
    async fn wait_all_placed(&mut self) -> Result<(), SinkError> {
        let through = self.next_seq;
        tokio::select! {
            _ = self.ack.wait_placed_through(through) => Ok(()),
            _ = self.fatal.wait() => Err(self.fatal_err()),
        }
    }

    /// Block until every seq `< self.next_seq` is durable on CH, or a fatal
    /// trips (e.g. CH down past the inserter retry budget).
    async fn wait_all_durable(&mut self) -> Result<(), SinkError> {
        let through = self.next_seq;
        tokio::select! {
            _ = self.ack.wait_through(through) => Ok(()),
            _ = self.fatal.wait() => Err(self.fatal_err()),
        }
    }

    /// Fence before applying a DDL event / TRUNCATE so it orders strictly
    /// after all earlier data: wait placed, seal batcher, wait durable. The
    /// placed-wait stops `FlushAll` sealing a partial set while the decode
    /// pool is still routing earlier rows.
    async fn barrier_fence(&mut self) -> Result<(), SinkError> {
        self.wait_all_placed().await?;
        self.flush_all_batcher().await?;
        self.wait_all_durable().await
    }

    /// Dispatch accumulated barrier data rows as their own seq. No-op when
    /// empty.
    async fn dispatch_segment(
        &mut self,
        pending: &mut Vec<DecodedHeap>,
        commit_ts: i64,
        commit_lsn: u64,
        chunks: &Arc<ToastChunks>,
    ) -> Result<(), SinkError> {
        if pending.is_empty() {
            return Ok(());
        }
        let seq = self.alloc_seq();
        self.ack.register(seq, commit_lsn);
        let job = DecodeJob {
            seq,
            commit_ts,
            commit_lsn,
            heaps: std::mem::take(pending),
            chunks: chunks.clone(),
        };
        self.dispatch_job(job).await
    }

    async fn apply_event(&mut self, event: &SchemaEvent) -> Result<(), SinkError> {
        self.applicator
            .apply(event)
            .await
            .map_err(|e| SinkError::Other(format!("ddl apply: {e}")))
    }

    async fn apply_truncate(&mut self, heap: &DecodedHeap) -> Result<(), SinkError> {
        let rel = match crate::shadow_catalog::resolve_at(&self.catalog, heap.rfn, heap.source_lsn)
            .await
        {
            Ok(r) => r,
            Err(CatalogError::ForeignDatabase(_)) => return Ok(()),
            Err(e) => return Err(SinkError::from(DecoderSinkError::from(e))),
        };
        self.applicator
            .truncate(rel.qualified_name.as_ref())
            .await
            .map_err(|e| SinkError::Other(format!("ch truncate: {e}")))?;
        self.stats.truncates_emitted.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Process a barrier xact in source_lsn order: data rows accumulate into
    /// segments; each DDL event / TRUNCATE is preceded by dispatching the
    /// pending segment + a fence (so earlier data is durable first).
    pub(crate) async fn run_barrier(&mut self, drained: DrainedXact) -> Result<(), SinkError> {
        let DrainedXact {
            commit_ts,
            commit_lsn,
            heaps,
            chunks,
            ordered_events,
            ..
        } = drained;
        let chunks = Arc::new(chunks);
        let mut pending: Vec<DecodedHeap> = Vec::new();
        let mut ev_cursor = 0usize;
        for (heap_idx, heap) in heaps.into_iter().enumerate() {
            while ev_cursor < ordered_events.len() && ordered_events[ev_cursor].0 <= heap_idx {
                self.dispatch_segment(&mut pending, commit_ts, commit_lsn, &chunks)
                    .await?;
                self.barrier_fence().await?;
                self.apply_event(&ordered_events[ev_cursor].1).await?;
                ev_cursor += 1;
            }
            if matches!(heap.op, HeapOp::Truncate) {
                self.dispatch_segment(&mut pending, commit_ts, commit_lsn, &chunks)
                    .await?;
                self.barrier_fence().await?;
                self.apply_truncate(&heap).await?;
            } else {
                pending.push(heap);
            }
        }
        // Trailing events: no heap follows them in the merge
        while ev_cursor < ordered_events.len() {
            self.dispatch_segment(&mut pending, commit_ts, commit_lsn, &chunks)
                .await?;
            self.barrier_fence().await?;
            self.apply_event(&ordered_events[ev_cursor].1).await?;
            ev_cursor += 1;
        }
        // Trailing data flows async like a normal commit, already encoding
        // against the post-DDL shape.
        self.dispatch_segment(&mut pending, commit_ts, commit_lsn, &chunks)
            .await
    }

    async fn on_commit(
        &mut self,
        xid: u32,
        info: u8,
        record: &Record<'_>,
    ) -> Result<(), SinkError> {
        let payload = parse_xact_payload(info, &record.parsed.main_data);
        // Parent for this commit's spans; held until on_commit returns so it
        // outlives the prune below. No-op span when tracing off/unsampled.
        let txn = self
            .span_registry
            .as_ref()
            .and_then(|r| r.txn_span(xid))
            .unwrap_or_else(tracing::Span::none);
        self.maybe_sweep_dropped(xid, record.source_lsn).await?;
        let drain_span = trace_span!(
            !txn.is_none(),
            parent: &txn,
            "commit.drain",
            xid = xid,
            commit_lsn = record.source_lsn,
        );
        // Same drain, parented contextually so it shows under `record` in the
        // batch view (`commit.drain` shows only in the per-txn trace).
        let reorder_span = trace_span!(
            !txn.is_none(),
            "reorder",
            xid = xid,
            commit_lsn = record.source_lsn,
        );
        let drained = {
            let mut buf = self.buffer.lock().await;
            buf.drain_committed(xid, payload.xact_time, record.source_lsn, &payload.subxacts)
                .instrument(drain_span)
                .instrument(reorder_span)
                .await
                .map_err(SinkError::from)?
        };
        self.subxact_tracker.lock().await.forget_tree(xid);
        // One per drained commit, incl. empty / unmapped-only
        self.stats.xacts_committed.fetch_add(1, Ordering::Relaxed);
        txn.record("rows", drained.heaps.len() as u64);
        txn.record("outcome", "committed");
        // Prune the committed tree's span handles (else the map grows
        // unbounded); the local `txn` clone keeps the span alive for dispatch.
        if let Some(r) = &self.span_registry {
            let mut xids: Vec<u32> = Vec::with_capacity(1 + payload.subxacts.len());
            xids.push(xid);
            xids.extend_from_slice(&payload.subxacts);
            r.prune(&xids);
        }

        self.dispatch_drained(drained, &txn).await
    }

    /// Seq-allocate + register + dispatch (or barrier) an already-drained xact.
    /// Split out of [`Self::on_commit`] so the xid-sharded path can drain on the
    /// owning shard and feed the `DrainedXact` here — the serial coordinator
    /// keeps sole ownership of `seq` order, `ack.register`, and the DDL barrier
    /// (the watermark-critical state), so sharding adds no ordering risk.
    /// `txn` parents the dispatch/barrier spans.
    pub(crate) async fn dispatch_drained(
        &mut self,
        drained: DrainedXact,
        txn: &tracing::Span,
    ) -> Result<(), SinkError> {
        // Persist this xact's chunks (disk / CH) before they're consumed by
        // the decode pool, so a later re-emit of a pre-window referrer finds
        // them. No-op when disabled or chunk-free. `commit_lsn` is the
        // convergence `_lsn`; per-chunk LSN was dropped at merge.
        if !drained.chunks.is_empty() {
            self.resolver
                .put_map(&drained.chunks, drained.commit_lsn)
                .await
                .map_err(|e| SinkError::Other(format!("toast store put: {e}")))?;
        }

        let is_barrier = !drained.ordered_events.is_empty()
            || drained
                .heaps
                .iter()
                .any(|h| matches!(h.op, HeapOp::Truncate));
        if is_barrier {
            self.run_barrier(drained)
                .instrument(trace_span!(
                    !txn.is_none(),
                    parent: txn,
                    "commit.barrier",
                ))
                .await
        } else if drained.heaps.is_empty() {
            // Empty commit: rows=0 seq keeps the contiguous watermark unbroken
            let seq = self.alloc_seq();
            self.ack.register(seq, drained.commit_lsn);
            self.ack.placed(seq, 0);
            Ok(())
        } else {
            let seq = self.alloc_seq();
            self.ack.register(seq, drained.commit_lsn);
            let job = DecodeJob {
                seq,
                commit_ts: drained.commit_ts,
                commit_lsn: drained.commit_lsn,
                heaps: drained.heaps,
                chunks: Arc::new(drained.chunks),
            };
            self.dispatch_job(job)
                .instrument(trace_span!(
                    !txn.is_none(),
                    parent: txn,
                    "dispatch",
                    seq = seq,
                ))
                .await
        }
    }

    /// ABORT: drop the buffer, emit a rows=0 seq through the gate (never a
    /// direct ack bump).
    async fn on_abort(&mut self, xid: u32, info: u8, record: &Record<'_>) -> Result<(), SinkError> {
        let payload = parse_xact_payload(info, &record.parsed.main_data);
        let seq = self.alloc_seq();
        self.ack.register(seq, record.source_lsn);
        {
            let mut buf = self.buffer.lock().await;
            buf.abort(xid, record.source_lsn, &payload.subxacts)
                .await
                .map_err(SinkError::from)?;
        }
        self.ack.placed(seq, 0);
        self.subxact_tracker.lock().await.forget_tree(xid);
        Ok(())
    }
}

impl RecordSink for ReorderSink {
    fn on_record<'a>(
        &'a mut self,
        record: &'a Record<'a>,
    ) -> Pin<Box<dyn Future<Output = Result<(), SinkError>> + Send + 'a>> {
        Box::pin(async move {
            if record.parsed.header.resource_manager_id != RmId::Xact as u8 {
                return Ok(());
            }
            let info = record.parsed.header.info;
            let op = info & XLOG_XACT_OPMASK;
            let xid = record.parsed.header.xact_id;
            match op {
                XLOG_XACT_COMMIT | XLOG_XACT_COMMIT_PREPARED => {
                    self.on_commit(xid, info, record).await
                }
                XLOG_XACT_ABORT | XLOG_XACT_ABORT_PREPARED => {
                    self.on_abort(xid, info, record).await
                }
                XLOG_XACT_ASSIGNMENT => {
                    if let Some((xtop, subs)) = parse_xact_assignment(&record.parsed.main_data) {
                        self.subxact_tracker.lock().await.assign(xtop, &subs);
                    }
                    Ok(())
                }
                // PREPARE allocates no seq; COMMIT_PREPARED drains it later
                _ => Ok(()),
            }
        })
    }

    fn on_idle_advance<'a>(
        &'a mut self,
        lsn: u64,
    ) -> Pin<Box<dyn Future<Output = Result<(), SinkError>> + Send + 'a>> {
        Box::pin(async move {
            // Trailing non-commit WAL only when no xact buffered; collector
            // also requires every registered seq done before advancing.
            let active = self.buffer.lock().await.stats().xacts_active;
            if active == 0 {
                self.ack.trailing(lsn);
                self.buffer.lock().await.advance_idle(lsn, lsn);
            }
            Ok(())
        })
    }
}

// ---------------------------------------------------------------------------
// xid-sharded parallel decode+absorb (Design A, Stage 0/1)
//
// Fans the single-threaded decode+absorb across N shards keyed on xid. The
// coordinator (`ShardedDecodeReorder`) keeps sole ownership of `seq` order +
// `ack.register`: no-subxact commits route Commit{seq} to one shard; a TRUNCATE
// barrier or subxact tree (whose subxids live in other shards) is drained across
// shards and merged (`drain_tree`) before dispatch. Catalog DROP still FATAL
// under shards>1 (Stage 2) — use `--queueing-shards 1`.
// ---------------------------------------------------------------------------

/// Per-shard bounded inbound queue. Backpressures the coordinator (hence the
/// pump) when a shard falls behind.
const SHARD_CHANNEL_CAP: usize = 2048;

enum ShardMsg {
    /// A heap record to decode + absorb into this shard's buffer.
    Heap(Box<Record<'static>>),
    /// Commit `xid`: drain this shard's buffer + dispatch with the
    /// coordinator-assigned `seq` (already `register`ed by the coordinator).
    Commit {
        xid: u32,
        seq: u64,
        xact_time: i64,
        commit_lsn: u64,
        subxacts: Vec<u32>,
    },
    /// Abort `xid`: drop its buffered rows + `placed(seq, 0)`.
    Abort {
        xid: u32,
        seq: u64,
        commit_lsn: u64,
        subxacts: Vec<u32>,
    },
    /// Drop whichever of `{xid} ∪ subxacts` live in this shard (no ack) and
    /// reply — the coordinator does the single `placed(seq, 0)` once every
    /// shard of a multi-shard abort tree has dropped its slice.
    AbortTree {
        xid: u32,
        commit_lsn: u64,
        subxacts: Vec<u32>,
        reply: oneshot::Sender<std::result::Result<(), SinkError>>,
    },
    /// Drain `xid` and hand the `DrainedXact` back to the coordinator instead of
    /// dispatching — the barrier (TRUNCATE) path, where the coordinator applies
    /// it serially via `run_barrier`.
    DrainBarrier {
        xid: u32,
        xact_time: i64,
        commit_lsn: u64,
        subxacts: Vec<u32>,
        reply: oneshot::Sender<std::result::Result<DrainedXact, SinkError>>,
    },
    /// Process everything queued so far, then reply. Quiesces a shard so its
    /// earlier commits are all dispatched before a barrier fence.
    Flush(oneshot::Sender<()>),
}

/// One parallel decode+absorb worker: owns its `BufferingDecoderSink` + buffer
/// (no shared lock) and a clone of the shared dispatch resources.
struct Shard {
    decoder: BufferingDecoderSink,
    buffer: Arc<Mutex<XactBuffer>>,
    ack: AckHandle,
    jobs_tx: mpmc::Sender<DecodeJob>,
    resolver: ToastResolver,
    stats: Arc<EmitterStats>,
    fatal: Fatal,
}

impl Shard {
    async fn run(mut self, mut rx: mpsc::Receiver<ShardMsg>) {
        while let Some(msg) = rx.recv().await {
            let r = match msg {
                ShardMsg::Heap(rec) => self.decoder.on_record(&rec).await,
                ShardMsg::Commit {
                    xid,
                    seq,
                    xact_time,
                    commit_lsn,
                    subxacts,
                } => {
                    self.commit(xid, seq, xact_time, commit_lsn, &subxacts)
                        .await
                }
                ShardMsg::Abort {
                    xid,
                    seq,
                    commit_lsn,
                    subxacts,
                } => self.abort(xid, seq, commit_lsn, &subxacts).await,
                ShardMsg::AbortTree {
                    xid,
                    commit_lsn,
                    subxacts,
                    reply,
                } => {
                    let r = self
                        .buffer
                        .lock()
                        .await
                        .abort(xid, commit_lsn, &subxacts)
                        .await
                        .map_err(SinkError::from);
                    let _ = reply.send(r);
                    Ok(())
                }
                ShardMsg::DrainBarrier {
                    xid,
                    xact_time,
                    commit_lsn,
                    subxacts,
                    reply,
                } => {
                    let drained = self
                        .buffer
                        .lock()
                        .await
                        .drain_committed(xid, xact_time, commit_lsn, &subxacts)
                        .await
                        .map_err(SinkError::from);
                    let _ = reply.send(drained);
                    Ok(())
                }
                ShardMsg::Flush(reply) => {
                    let _ = reply.send(());
                    Ok(())
                }
            };
            if let Err(e) = r {
                self.fatal.set(format!("decode shard: {e}"));
                return;
            }
        }
    }

    async fn commit(
        &mut self,
        xid: u32,
        seq: u64,
        xact_time: i64,
        commit_lsn: u64,
        subxacts: &[u32],
    ) -> Result<(), SinkError> {
        let drained = self
            .buffer
            .lock()
            .await
            .drain_committed(xid, xact_time, commit_lsn, subxacts)
            .await
            .map_err(SinkError::from)?;
        // Stage 1 is happy-path only: a barrier (DDL event / TRUNCATE) needs a
        // cross-shard quiesce (Stage 2). Fail loud rather than mis-order.
        if !drained.ordered_events.is_empty()
            || drained
                .heaps
                .iter()
                .any(|h| matches!(h.op, HeapOp::Truncate))
        {
            return Err(SinkError::Other(
                "DDL/TRUNCATE under --queueing-shards>1 unsupported (Stage 2); use 1".into(),
            ));
        }
        self.stats.xacts_committed.fetch_add(1, Ordering::Relaxed);
        if drained.heaps.is_empty() {
            // Empty commit: rows=0 seq keeps the contiguous watermark unbroken.
            self.ack.placed(seq, 0);
            return Ok(());
        }
        if !drained.chunks.is_empty() {
            self.resolver
                .put_map(&drained.chunks, drained.commit_lsn)
                .await
                .map_err(|e| SinkError::Other(format!("toast store put: {e}")))?;
        }
        // Mirror ReorderSink::dispatch_job's counter (the shard dispatches
        // directly, bypassing it): keeps queue_jobs_out accurate.
        self.stats.queue_jobs_out.fetch_add(1, Ordering::Relaxed);
        let job = DecodeJob {
            seq,
            commit_ts: drained.commit_ts,
            commit_lsn: drained.commit_lsn,
            heaps: drained.heaps,
            chunks: Arc::new(drained.chunks),
        };
        // Decode pool calls `ack.placed(seq, nrows)` after routing the rows.
        tokio::select! {
            r = self.jobs_tx.send(job) => r.map_err(|_| SinkError::Other("decode job queue closed".into())),
            _ = self.fatal.wait() => Err(SinkError::Other(
                self.fatal.message().unwrap_or_else(|| "pipeline fatal".into()),
            )),
        }
    }

    async fn abort(
        &mut self,
        xid: u32,
        seq: u64,
        commit_lsn: u64,
        subxacts: &[u32],
    ) -> Result<(), SinkError> {
        self.buffer
            .lock()
            .await
            .abort(xid, commit_lsn, subxacts)
            .await
            .map_err(SinkError::from)?;
        self.ack.placed(seq, 0);
        Ok(())
    }
}

/// Merge per-shard drain pieces of one xact tree into a single `DrainedXact`.
/// A subtransaction's heaps land in `shard_for(subxid)`, which differs from the
/// top's shard, so its tree drains across shards; the COMMIT record carries the
/// full subxid list, so we drain each holder and merge here (k-way by
/// `source_lsn`, union the TOAST chunk maps). `ordered_events` (catalog DROPs)
/// must be empty — DROP is rejected upstream (Stage 2).
fn merge_drained(
    pieces: Vec<DrainedXact>,
    commit_ts: i64,
    commit_lsn: u64,
) -> Result<DrainedXact, SinkError> {
    let mut heaps = Vec::new();
    let mut chunks: HashMap<(u32, u32), BTreeMap<u32, Vec<u8>>> = HashMap::new();
    let mut had_states = false;
    for p in pieces {
        if !p.ordered_events.is_empty() {
            return Err(SinkError::Other(
                "catalog DROP under --queueing-shards>1 unsupported (Stage 2); use 1".into(),
            ));
        }
        had_states |= p.had_states;
        heaps.extend(p.heaps);
        for (k, v) in p.chunks {
            chunks.entry(k).or_default().extend(v);
        }
    }
    heaps.sort_by_key(|h| h.source_lsn);
    Ok(DrainedXact {
        commit_ts,
        commit_lsn,
        heaps,
        chunks,
        ordered_events: Vec::new(),
        had_states,
    })
}

/// Serial coordinator: routes records to shards by xid; keeps the full
/// `ReorderSink` (`coord`) for the global `seq` + `ack.register` order and the
/// DDL/TRUNCATE barrier (which it runs serially after quiescing all shards).
/// Slots in as the `QueueingRecordSink` inner sink in place of
/// `DecoderXactPair` when `--queueing-shards > 1`.
pub struct ShardedDecodeReorder {
    coord: ReorderSink,
    shard_txs: Vec<mpsc::Sender<ShardMsg>>,
    shard_buffers: Vec<Arc<Mutex<XactBuffer>>>,
    joins: Vec<JoinHandle<()>>,
    fatal: Fatal,
    /// xids whose transaction contains a TRUNCATE (seen on the WAL record);
    /// handled as a barrier at their commit. Cleared on commit/abort.
    barrier_xids: HashSet<u32>,
    /// pg_class delete-epoch baseline; a change means a DROP was replayed.
    base_delete_epoch: u64,
}

impl ShardedDecodeReorder {
    fn shard_for(&self, xid: u32) -> usize {
        (xid as usize) % self.shard_txs.len()
    }

    fn fatal_err(&self) -> SinkError {
        SinkError::Other(
            self.fatal
                .message()
                .unwrap_or_else(|| "pipeline fatal".into()),
        )
    }

    // `&mut self` (not `&self`) so the borrow held across the await is
    // `&mut Self` (Send): `coord`'s owned DdlApplicator is Send but not Sync,
    // so a shared `&Self` across an await wouldn't be Send.
    async fn route(&mut self, idx: usize, msg: ShardMsg) -> Result<(), SinkError> {
        tokio::select! {
            r = self.shard_txs[idx].send(msg) => r.map_err(|_| SinkError::Other("decode shard channel closed".into())),
            _ = self.fatal.wait() => Err(self.fatal_err()),
        }
    }

    /// A DROP was replayed (pg_class delete-epoch moved). DROP needs the
    /// commit-time `sweep_dropped` the sharded path doesn't run — Stage 2.
    fn drop_seen(&self) -> bool {
        match &self.coord.pg_class_delete_epoch {
            Some(e) => e.load(Ordering::Acquire) != self.base_delete_epoch,
            None => false,
        }
    }

    /// Distinct shards holding any of `{xid} ∪ subxacts`.
    fn tree_shards(&self, xid: u32, subxacts: &[u32]) -> Vec<usize> {
        let mut s: Vec<usize> = std::iter::once(xid)
            .chain(subxacts.iter().copied())
            .map(|x| self.shard_for(x))
            .collect();
        s.sort_unstable();
        s.dedup();
        s
    }

    /// Drain a committed xact tree across every shard that holds part of it
    /// (`DrainBarrier` is FIFO-after the tree's already-routed heaps, so each
    /// shard's slice is fully absorbed first) and merge into one `DrainedXact`.
    async fn drain_tree(
        &mut self,
        xid: u32,
        xact_time: i64,
        commit_lsn: u64,
        subxacts: &[u32],
    ) -> Result<DrainedXact, SinkError> {
        let mut pieces = Vec::new();
        for idx in self.tree_shards(xid, subxacts) {
            let (tx, rx) = oneshot::channel();
            self.route(
                idx,
                ShardMsg::DrainBarrier {
                    xid,
                    xact_time,
                    commit_lsn,
                    subxacts: subxacts.to_vec(),
                    reply: tx,
                },
            )
            .await?;
            let piece = tokio::select! {
                r = rx => r.map_err(|_| SinkError::Other("shard drain dropped".into()))?,
                _ = self.fatal.wait() => return Err(self.fatal_err()),
            }?;
            pieces.push(piece);
        }
        merge_drained(pieces, xact_time, commit_lsn)
    }

    /// Barrier (TRUNCATE) at `xid`'s commit: quiesce every shard so all earlier
    /// commits are dispatched, drain the barrier xid on its shard, then apply
    /// it serially via the coordinator's `dispatch_drained` (→ `run_barrier`,
    /// which fences earlier seqs durable before the TRUNCATE).
    async fn run_barrier_xact(
        &mut self,
        xid: u32,
        xact_time: i64,
        commit_lsn: u64,
        subxacts: Vec<u32>,
    ) -> Result<(), SinkError> {
        // Flush every shard FIFO so all already-routed Commits are dispatched
        // (their seqs become placeable) before the barrier fence waits on them.
        for idx in 0..self.shard_txs.len() {
            let (tx, rx) = oneshot::channel();
            self.route(idx, ShardMsg::Flush(tx)).await?;
            tokio::select! {
                r = rx => r.map_err(|_| SinkError::Other("shard flush dropped".into()))?,
                _ = self.fatal.wait() => return Err(self.fatal_err()),
            }
        }
        let drained = self
            .drain_tree(xid, xact_time, commit_lsn, &subxacts)
            .await?;
        self.coord
            .stats
            .xacts_committed
            .fetch_add(1, Ordering::Relaxed);
        // dispatch_drained handles toast-put + (barrier => run_barrier with
        // fences | plain => normal dispatch). Tracing off in the sharded path.
        self.coord
            .dispatch_drained(drained, &tracing::Span::none())
            .await?;
        if let Some(e) = &self.coord.pg_class_delete_epoch {
            self.base_delete_epoch = e.load(Ordering::Acquire);
        }
        Ok(())
    }

    /// Handle an xact (COMMIT/ABORT/ASSIGNMENT) record by borrow — shared by
    /// both `on_record` and `on_record_owned`. Returns `Ok(true)` if it was an
    /// xact record (handled here), `Ok(false)` if it's a heap record the caller
    /// should route to a shard.
    async fn dispatch_xact(&mut self, record: &Record<'_>) -> Result<bool, SinkError> {
        if record.parsed.header.resource_manager_id != RmId::Xact as u8 {
            return Ok(false);
        }
        let info = record.parsed.header.info;
        let op = info & XLOG_XACT_OPMASK;
        let xid = record.parsed.header.xact_id;
        match op {
            XLOG_XACT_COMMIT | XLOG_XACT_COMMIT_PREPARED => {
                let payload = parse_xact_payload(info, &record.parsed.main_data);
                // DROP needs the commit-time sweep the sharded path skips.
                if self.drop_seen() {
                    return Err(SinkError::Other(
                        "DROP under --queueing-shards>1 unsupported (Stage 2); use 1".into(),
                    ));
                }
                // A TRUNCATE anywhere in the tree (top or any subxid) makes this
                // a barrier; remove every tree member's mark.
                let is_barrier = self.tree_is_barrier(xid, &payload.subxacts);
                if is_barrier {
                    // TRUNCATE barrier: quiesce all shards + apply serially.
                    self.run_barrier_xact(
                        xid,
                        payload.xact_time,
                        record.source_lsn,
                        payload.subxacts,
                    )
                    .await?;
                    return Ok(true);
                }
                if !payload.subxacts.is_empty() {
                    // Subxact tree spans shards: drain its slices + merge, then
                    // dispatch as one seq (per-shard FIFO already absorbed the
                    // heaps, so no cross-shard flush needed).
                    let drained = self
                        .drain_tree(xid, payload.xact_time, record.source_lsn, &payload.subxacts)
                        .await?;
                    self.coord
                        .stats
                        .xacts_committed
                        .fetch_add(1, Ordering::Relaxed);
                    self.coord
                        .dispatch_drained(drained, &tracing::Span::none())
                        .await?;
                    return Ok(true);
                }
                let seq = self.coord.alloc_and_register(record.source_lsn);
                let idx = self.shard_for(xid);
                self.route(
                    idx,
                    ShardMsg::Commit {
                        xid,
                        seq,
                        xact_time: payload.xact_time,
                        commit_lsn: record.source_lsn,
                        subxacts: payload.subxacts,
                    },
                )
                .await?;
            }
            XLOG_XACT_ABORT | XLOG_XACT_ABORT_PREPARED => {
                let payload = parse_xact_payload(info, &record.parsed.main_data);
                // Rolled-back TRUNCATE: clear the tree's marks, never apply.
                self.tree_is_barrier(xid, &payload.subxacts);
                if !payload.subxacts.is_empty() {
                    // Drop the tree's slices across shards, then one placed(seq,0).
                    for idx in self.tree_shards(xid, &payload.subxacts) {
                        let (tx, rx) = oneshot::channel();
                        self.route(
                            idx,
                            ShardMsg::AbortTree {
                                xid,
                                commit_lsn: record.source_lsn,
                                subxacts: payload.subxacts.clone(),
                                reply: tx,
                            },
                        )
                        .await?;
                        tokio::select! {
                            r = rx => r.map_err(|_| SinkError::Other("shard abort dropped".into()))??,
                            _ = self.fatal.wait() => return Err(self.fatal_err()),
                        }
                    }
                    let seq = self.coord.alloc_and_register(record.source_lsn);
                    self.coord.ack.placed(seq, 0);
                    return Ok(true);
                }
                let seq = self.coord.alloc_and_register(record.source_lsn);
                let idx = self.shard_for(xid);
                self.route(
                    idx,
                    ShardMsg::Abort {
                        xid,
                        seq,
                        commit_lsn: record.source_lsn,
                        subxacts: payload.subxacts,
                    },
                )
                .await?;
            }
            // Subxid→top hint. Routing keys on each record's own xid and the
            // COMMIT record carries the full subxid list, so the merge needs no
            // pre-commit mapping — nothing to do.
            XLOG_XACT_ASSIGNMENT => {}
            // PREPARE etc.: no seq, nothing to route.
            _ => {}
        }
        Ok(true)
    }

    /// Remove every tree member (`{xid} ∪ subxacts`) from `barrier_xids`,
    /// returning whether any was marked (a TRUNCATE landed in the tree —
    /// possibly under a subxid). Clears the marks either way.
    fn tree_is_barrier(&mut self, xid: u32, subxacts: &[u32]) -> bool {
        let mut hit = self.barrier_xids.remove(&xid);
        for s in subxacts {
            hit |= self.barrier_xids.remove(s);
        }
        hit
    }

    /// Mark a TRUNCATE's xid as a barrier (its commit takes the serial path).
    fn note_truncate(&mut self, record: &Record<'_>) {
        if record.parsed.header.resource_manager_id == RmId::Heap as u8
            && (record.parsed.header.info & XLOG_HEAP_OPMASK) == XLOG_HEAP_TRUNCATE
        {
            self.barrier_xids.insert(record.parsed.header.xact_id);
        }
    }
}

impl RecordSink for ShardedDecodeReorder {
    // Fallback (by-ref) path: clones the heap record. The queueing worker uses
    // `on_record_owned` (move, no clone) — this exists only to satisfy the
    // trait / non-worker callers.
    fn on_record<'a>(
        &'a mut self,
        record: &'a Record<'a>,
    ) -> Pin<Box<dyn Future<Output = Result<(), SinkError>> + Send + 'a>> {
        Box::pin(async move {
            if self.dispatch_xact(record).await? {
                return Ok(());
            }
            self.note_truncate(record);
            let owned = Record {
                parsed: record.parsed.clone().into_owned(),
                source_lsn: record.source_lsn,
                page_magic: record.page_magic,
                route: record.route,
            };
            let idx = self.shard_for(record.parsed.header.xact_id);
            self.route(idx, ShardMsg::Heap(Box::new(owned))).await
        })
    }

    // Hot path from the queueing worker: the record is already `'static`, so a
    // heap record is **moved** straight onto its shard channel — no clone (this
    // is the per-record `into_owned` that was the serial-router ceiling).
    fn on_record_owned<'a>(
        &'a mut self,
        record: Record<'static>,
    ) -> Pin<Box<dyn Future<Output = Result<(), SinkError>> + Send + 'a>> {
        Box::pin(async move {
            if self.dispatch_xact(&record).await? {
                return Ok(());
            }
            self.note_truncate(&record);
            let idx = self.shard_for(record.parsed.header.xact_id);
            self.route(idx, ShardMsg::Heap(Box::new(record))).await
        })
    }

    fn on_close<'a>(
        &'a mut self,
    ) -> Pin<Box<dyn Future<Output = Result<(), SinkError>> + Send + 'a>> {
        Box::pin(async move {
            // Drop the senders so each shard drains its queue and exits, which
            // drops its jobs_tx clone; once all are gone the decode pool drains
            // (PipelineHandle::join awaits it).
            self.shard_txs.clear();
            for h in self.joins.drain(..) {
                let _ = h.await;
            }
            Ok(())
        })
    }

    fn on_idle_advance<'a>(
        &'a mut self,
        lsn: u64,
    ) -> Pin<Box<dyn Future<Output = Result<(), SinkError>> + Send + 'a>> {
        Box::pin(async move {
            // try_lock only: the worker drives the queue drain that keeps the
            // pump→shadow loop alive, so it must never *block* on a shard buffer
            // (a held lock would stall the pump, freeze shadow replay, and
            // deadlock). A locked shard means not-idle anyway → skip this round.
            let mut guards = Vec::with_capacity(self.shard_buffers.len());
            for b in &self.shard_buffers {
                match b.try_lock() {
                    Ok(g) => guards.push(g),
                    Err(_) => return Ok(()),
                }
            }
            if guards.iter().all(|g| g.stats().xacts_active == 0) {
                self.coord.ack.trailing(lsn);
                for g in guards.iter_mut() {
                    g.advance_idle(lsn, lsn);
                }
            }
            Ok(())
        })
    }
}

impl ReorderSink {
    /// Consume this (serial) reorder sink and fan its decode+absorb across `n`
    /// shards, one `XactBuffer` each. Reuses its dispatch resources (ack,
    /// jobs_tx, resolver, stats, fatal) — the shards dispatch; the returned
    /// coordinator keeps the seq/register. The shared `ShadowCatalog` is reused
    /// (each shard's `BufferingDecoderSink` has its own epoch-gated memo).
    pub fn into_sharded(
        self,
        n: usize,
        buffers: Vec<Arc<Mutex<XactBuffer>>>,
        decoder_stats: Arc<crate::decoder_sink::DecoderStats>,
    ) -> ShardedDecodeReorder {
        assert_eq!(buffers.len(), n, "one buffer per shard");
        // Keep `self` as the coordinator (seq + ack + applicator + barrier);
        // clone its dispatch resources into the shards. Its own `jobs_tx` stays
        // alive (for the barrier path) — `jobs_rx` closes once the coordinator
        // AND all shards drop their senders at shutdown.
        let base_delete_epoch = self.last_seen_delete_epoch;
        let fatal = self.fatal.clone();
        let mut shard_txs = Vec::with_capacity(n);
        let mut shard_buffers = Vec::with_capacity(n);
        let mut joins = Vec::with_capacity(n);
        for buffer in buffers.into_iter() {
            shard_buffers.push(buffer.clone());
            let decoder = BufferingDecoderSink::new(self.catalog.clone(), buffer.clone())
                .with_stats(decoder_stats.clone());
            let shard = Shard {
                decoder,
                buffer,
                ack: self.ack.clone(),
                jobs_tx: self.jobs_tx.clone(),
                resolver: self.resolver.clone(),
                stats: self.stats.clone(),
                fatal: self.fatal.clone(),
            };
            let (tx, rx) = mpsc::channel(SHARD_CHANNEL_CAP);
            joins.push(tokio::spawn(shard.run(rx)));
            shard_txs.push(tx);
        }
        ShardedDecodeReorder {
            coord: self,
            shard_txs,
            shard_buffers,
            joins,
            fatal,
            barrier_xids: HashSet::new(),
            base_delete_epoch,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::heap_decoder::HeapOp;
    use walrus::pg::walparser::RelFileNode;

    fn heap(source_lsn: u64) -> DecodedHeap {
        DecodedHeap {
            rfn: RelFileNode {
                spc_node: 1663,
                db_node: 5,
                rel_node: 16385,
            },
            xid: 0,
            source_lsn,
            op: HeapOp::Insert,
            new: None,
            old: None,
        }
    }

    fn piece(lsns: &[u64]) -> DrainedXact {
        DrainedXact {
            commit_ts: 0,
            commit_lsn: 100,
            heaps: lsns.iter().copied().map(heap).collect(),
            chunks: HashMap::new(),
            ordered_events: Vec::new(),
            had_states: !lsns.is_empty(),
        }
    }

    #[test]
    fn merge_drained_orders_heaps_across_pieces_by_lsn() {
        // Two shards' slices of one tree; interleaved LSNs must come out sorted.
        let merged = merge_drained(vec![piece(&[10, 40]), piece(&[20, 30])], 7, 100).unwrap();
        let lsns: Vec<u64> = merged.heaps.iter().map(|h| h.source_lsn).collect();
        assert_eq!(lsns, [10, 20, 30, 40]);
        assert_eq!(merged.commit_ts, 7);
        assert!(merged.had_states);
    }

    #[test]
    fn merge_drained_unions_chunks_and_rejects_catalog_events() {
        let mut a = piece(&[1]);
        a.chunks
            .insert((9, 1), [(0u32, vec![0xAB])].into_iter().collect());
        let mut b = piece(&[2]);
        b.chunks
            .insert((9, 2), [(0u32, vec![0xCD])].into_iter().collect());
        let merged = merge_drained(vec![a, b], 0, 100).unwrap();
        assert_eq!(merged.chunks.len(), 2);

        // A catalog DROP event (ordered_events) is Stage 2 → error.
        let mut c = piece(&[3]);
        c.ordered_events.push((
            0,
            SchemaEvent::Dropped {
                oid: 1,
                qualified_name: "public.t".into(),
            },
        ));
        assert!(merge_drained(vec![c], 0, 100).is_err());
    }
}
