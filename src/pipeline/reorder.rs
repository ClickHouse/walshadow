//! Reorder worker — the single-threaded commit-order coordinator.
//!
//! Runs as the inner sink of the `QueueingRecordSink` worker (so it stays
//! off the WAL pump task, preserving the wire-shadow deadlock fix). It
//! pairs with [`BufferingDecoderSink`](crate::xact_buffer::BufferingDecoderSink)
//! (which accumulates heaps per xid); on each COMMIT / ABORT it assigns a
//! dense `seq`, [`registers`](crate::pipeline::ack::AckHandle::register) it
//! with the collector in order, and either dispatches the xact to the
//! decode pool or — for a DDL / TRUNCATE barrier — quiesces, drains every
//! earlier seq to durable, and applies the schema change to ClickHouse via
//! [`DdlApplicator`] before resuming.
//!
//! Barrier coarseness is deliberate (DDL/TRUNCATE are rare). Within a
//! barrier xact, data segments between catalog/truncate ops each get their
//! own seq and are fenced so a `TRUNCATE` (which carries no `_lsn` and so
//! can't ride `ReplacingMergeTree` reconciliation) orders correctly against
//! surrounding inserts.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use tokio::sync::{Mutex, mpsc, oneshot};
use wal_rs::pg::walparser::RmId;

use crate::ch_ddl::DdlApplicator;
use crate::ch_emitter::EmitterStats;
use crate::decoder_sink::DecoderSinkError;
use crate::heap_decoder::{DecodedHeap, HeapOp};
use crate::relation_resolver::RelationResolver;
use crate::shadow_catalog::{CatalogError, SchemaEvent, ShadowCatalog};
use crate::wal_stream::{Record, RecordSink, SinkError};
use crate::xact_buffer::{
    DrainedXact, SchemaEventRx, SubxactTracker, XLOG_XACT_ABORT, XLOG_XACT_ABORT_PREPARED,
    XLOG_XACT_ASSIGNMENT, XLOG_XACT_COMMIT, XLOG_XACT_COMMIT_PREPARED, XLOG_XACT_OPMASK,
    XactBuffer, drain_pending_schema_events, parse_xact_assignment, parse_xact_payload,
};

use crate::pipeline::ack::AckHandle;
use crate::pipeline::batcher::BatcherMsg;
use crate::pipeline::decode::{DecodeJob, ToastChunks};
use crate::pipeline::{Fatal, mpmc};

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
    /// Shared FIFO channel to the batcher (also carries decode-pool rows);
    /// used to send `FlushAll` so it orders after enqueued rows.
    msg_tx: mpsc::Sender<BatcherMsg>,
    fatal: Fatal,
    /// Shared emitter counters. Reorder owns the commit-order boundary, so
    /// it bumps `xacts_committed` (once per commit) and `truncates_emitted`,
    /// matching the serial emitter's `on_xact_end_with_lsn` / `route`.
    stats: Arc<EmitterStats>,
    /// Dense commit-order counter; one seq per dispatched data unit.
    next_seq: u64,
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
        fatal: Fatal,
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
            fatal,
            next_seq: 0,
        }
    }

    fn alloc_seq(&mut self) -> u64 {
        let s = self.next_seq;
        self.next_seq += 1;
        s
    }

    fn fatal_err(&self) -> SinkError {
        SinkError::Other(
            self.fatal
                .message()
                .unwrap_or_else(|| "pipeline fatal".into()),
        )
    }

    /// Drain pending DROP events (post-`sweep_dropped`) into the buffer
    /// keyed on the commit's `(xid, source_lsn)`. Mirrors
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

    /// Poll-based DROP discovery at the commit boundary, gated on the
    /// pg_class delete epoch so ADD COLUMN / VACUUM noise doesn't sweep.
    /// Same logic as `XactRecordSink`'s commit branch.
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

    // Helpers below take `&mut self` (not `&self`) so the borrow held
    // across their awaits is `&mut ReorderSink` (Send) rather than
    // `&ReorderSink` — the owned `DdlApplicator`/`AsyncClient` is `Send`
    // but not `Sync`, so a shared ref across an await would not be `Send`.
    async fn dispatch_job(&mut self, job: DecodeJob) -> Result<(), SinkError> {
        tokio::select! {
            r = self.jobs_tx.send(job) => r.map_err(|_| SinkError::Other("decode job queue closed".into())),
            _ = self.fatal.wait() => Err(self.fatal_err()),
        }
    }

    /// Seal every batcher table to the inserters and wait until it replies.
    /// Sent on the shared row channel so it's ordered after every row
    /// enqueued before it.
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

    /// Wait until every dispatched seq has been *placed* — i.e. the decode
    /// pool has finished routing all their rows onto the shared channel —
    /// so a subsequent `FlushAll` is ordered after them.
    async fn wait_all_placed(&mut self) -> Result<(), SinkError> {
        let through = self.next_seq;
        tokio::select! {
            _ = self.ack.wait_placed_through(through) => Ok(()),
            _ = self.fatal.wait() => Err(self.fatal_err()),
        }
    }

    /// Block until every seq `< self.next_seq` is durable on CH, or a
    /// fatal error trips (e.g. CH down past the inserter retry budget).
    async fn wait_all_durable(&mut self) -> Result<(), SinkError> {
        let through = self.next_seq;
        tokio::select! {
            _ = self.ack.wait_through(through) => Ok(()),
            _ = self.fatal.wait() => Err(self.fatal_err()),
        }
    }

    /// Fence: ensure every dispatched seq's rows have been routed (placed),
    /// then seal the batcher and wait for them all to be durable. Run before
    /// applying a DDL event / TRUNCATE so it orders strictly after all
    /// earlier data. The placed-wait is what stops `FlushAll` from sealing a
    /// partial set while the decode pool is still routing earlier rows.
    async fn barrier_fence(&mut self) -> Result<(), SinkError> {
        self.wait_all_placed().await?;
        self.flush_all_batcher().await?;
        self.wait_all_durable().await
    }

    /// Dispatch accumulated barrier data rows as their own seq (decoder
    /// will `Placed` them). No-op when empty.
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
        let rel = match self.catalog.relation_at(heap.rfn, heap.source_lsn).await {
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

    /// Process a barrier xact in source_lsn order: data rows accumulate
    /// into segments; each DDL event / TRUNCATE is preceded by dispatching
    /// the pending segment + a fence (so earlier data is durable first).
    async fn run_barrier(&mut self, drained: DrainedXact) -> Result<(), SinkError> {
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
        // Trailing events (no heap follows them in the merge).
        while ev_cursor < ordered_events.len() {
            self.dispatch_segment(&mut pending, commit_ts, commit_lsn, &chunks)
                .await?;
            self.barrier_fence().await?;
            self.apply_event(&ordered_events[ev_cursor].1).await?;
            ev_cursor += 1;
        }
        // Trailing data (after the last DDL/TRUNCATE): flows async like a
        // normal commit — already encodes against the post-DDL shape.
        self.dispatch_segment(&mut pending, commit_ts, commit_lsn, &chunks)
            .await
    }

    /// Handle a COMMIT / COMMIT_PREPARED record.
    async fn on_commit(
        &mut self,
        xid: u32,
        info: u8,
        record: &Record<'_>,
    ) -> Result<(), SinkError> {
        let payload = parse_xact_payload(info, &record.parsed.main_data);
        self.maybe_sweep_dropped(xid, record.source_lsn).await?;
        let drained = {
            let mut buf = self.buffer.lock().await;
            buf.drain_committed(xid, payload.xact_time, record.source_lsn, &payload.subxacts)
                .await
                .map_err(SinkError::from)?
        };
        self.subxact_tracker.lock().await.forget_tree(xid);
        // One per drained commit (incl. empty / unmapped-only), matching the
        // serial emitter's bump in `on_xact_end_with_lsn`.
        self.stats.xacts_committed.fetch_add(1, Ordering::Relaxed);

        let is_barrier = !drained.ordered_events.is_empty()
            || drained
                .heaps
                .iter()
                .any(|h| matches!(h.op, HeapOp::Truncate));
        if is_barrier {
            self.run_barrier(drained).await
        } else if drained.heaps.is_empty() {
            // Read-only / empty / unmapped-only commit: a rows=0 seq keeps
            // the contiguous watermark unbroken.
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
            self.dispatch_job(job).await
        }
    }

    /// Handle an ABORT / ABORT_PREPARED record: drop the buffer and emit a
    /// rows=0 seq through the gate (never a direct ack bump).
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
                // PREPARE / INVALIDATIONS: not this stage's territory.
                // PREPARE allocates no seq; COMMIT_PREPARED drains it later.
                _ => Ok(()),
            }
        })
    }

    fn on_idle_advance<'a>(
        &'a mut self,
        lsn: u64,
    ) -> Pin<Box<dyn Future<Output = Result<(), SinkError>> + Send + 'a>> {
        Box::pin(async move {
            // Trailing non-commit WAL: only when no xact is buffered. The
            // collector additionally requires every registered seq done
            // before it lets `emitter_ack` advance to `lsn`.
            let active = self.buffer.lock().await.stats().xacts_active;
            if active == 0 {
                self.ack.trailing(lsn);
                self.buffer.lock().await.advance_idle(lsn, lsn);
            }
            Ok(())
        })
    }
}
