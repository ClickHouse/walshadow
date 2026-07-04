//! Reorder worker — single-threaded commit-order coordinator.
//!
//! Runs as inner sink of the `QueueingRecordSink` worker, off the WAL pump
//! task (replay gates never pace wire delivery). Pairs with
//! [`BufferingDecoderSink`](crate::xact_buffer::BufferingDecoderSink); on each
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

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use tokio::sync::{Mutex, mpsc, oneshot};
use walrus::pg::walparser::RmId;

use crate::ch_ddl::DdlApplicator;
use crate::ch_emitter::EmitterStats;
use crate::decoder_sink::DecoderSinkError;
use crate::heap_decoder::{DecodedHeap, HeapOp};
use crate::shadow_catalog::{CatalogError, SchemaEvent, ShadowCatalog};
use crate::wal_stream::{Record, RecordSink, SinkError};
use tracing::Instrument;

use crate::xact_buffer::{
    DrainEntry, DrainedXact, SchemaEventRx, SubxactTracker, TxnSpanRegistry, XLOG_XACT_ABORT,
    XLOG_XACT_ABORT_PREPARED, XLOG_XACT_ASSIGNMENT, XLOG_XACT_COMMIT, XLOG_XACT_COMMIT_PREPARED,
    XLOG_XACT_OPMASK, XactBuffer, drain_pending_schema_events, parse_xact_assignment,
    parse_xact_payload,
};

use crate::config::ConfigResolver;
use crate::copy_backfill::CopyBackfiller;
use crate::pipeline::Fatal;
use crate::pipeline::ack::AckHandle;
use crate::pipeline::batcher::BatcherMsg;
use crate::pipeline::decode::{DecodeJob, ToastChunks};
use crate::runtime_config::ConfigEvent;
use crate::toast::ToastResolver;

pub struct ReorderSink {
    buffer: Arc<Mutex<XactBuffer>>,
    catalog: Arc<Mutex<ShadowCatalog>>,
    subxact_tracker: Arc<Mutex<SubxactTracker>>,
    schema_events: Option<SchemaEventRx>,
    /// Armed by the decoder at pg_class heap_delete records, consumed
    /// only at the arming xact's own commit (see
    /// [`PendingSweeps`](crate::catalog_tracker::PendingSweeps))
    pending_sweeps: Option<crate::catalog_tracker::PendingSweeps>,
    applicator: DdlApplicator,
    ack: AckHandle,
    jobs_tx: async_channel::Sender<DecodeJob>,
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
    /// Runtime-config overlay resolver. `Some` with the config overlay active;
    /// a `DrainEntry::Config` applies to it inside the barrier fence so
    /// trailing rows route against post-config state (plan §6).
    config_resolver: Option<Arc<ConfigResolver>>,
    /// COPY backfiller for `initial_load='copy'` opt-ins; spawns off the barrier
    /// (detached task, own CH tail), the apply only records + launches.
    backfiller: Option<Arc<CopyBackfiller>>,
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
        pending_sweeps: Option<crate::catalog_tracker::PendingSweeps>,
        applicator: DdlApplicator,
        ack: AckHandle,
        jobs_tx: async_channel::Sender<DecodeJob>,
        msg_tx: mpsc::Sender<BatcherMsg>,
        stats: Arc<EmitterStats>,
        resolver: ToastResolver,
        config_resolver: Option<Arc<ConfigResolver>>,
        backfiller: Option<Arc<CopyBackfiller>>,
        fatal: Fatal,
        span_registry: Option<TxnSpanRegistry>,
    ) -> Self {
        Self {
            buffer,
            catalog,
            subxact_tracker,
            schema_events,
            pending_sweeps,
            applicator,
            ack,
            jobs_tx,
            msg_tx,
            stats,
            resolver,
            config_resolver,
            backfiller,
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

    fn fatal_err(&self) -> SinkError {
        SinkError::Other(
            self.fatal
                .message()
                .unwrap_or_else(|| "pipeline fatal".into()),
        )
    }

    /// Apply a config-table change inside the barrier fence, so the fenced
    /// routing-map write lands before the trailing segment dispatches. Merge
    /// itself is infallible (Regime A: a malformed value is rejected + logged,
    /// never fatal); the per-table opt-in dispatch can create a CH table, so it
    /// surfaces CH errors like a DDL apply.
    ///
    /// `&mut self` (like [`Self::apply_event`]): the opt-in dispatch needs
    /// `&mut applicator` (the `!Sync` CH client) + `&catalog`, both fields of
    /// self. `&mut self`-across-await stays `Send`; only a shared `&self` would
    /// poison the sink future's `Send` bound.
    async fn apply_config(
        &mut self,
        event: &ConfigEvent,
        commit_lsn: u64,
    ) -> Result<(), SinkError> {
        let Some(resolver) = self.config_resolver.clone() else {
            return Ok(());
        };
        // Overlay merge first (target overrides, global/namespace knobs).
        resolver.apply_config_event(event.clone()).await;
        // Then inclusion dispatch for table rows: create the CH table +
        // register / drop the descriptor-derived mapping. `commit_lsn` is the
        // backfill boundary `S` for an `initial_load` opt-in.
        match event {
            ConfigEvent::TableUpserted { rel, row } => {
                crate::opt_in::apply_table_opt_in(
                    &resolver,
                    &mut self.applicator,
                    &self.catalog,
                    self.backfiller.as_ref(),
                    rel,
                    row,
                    commit_lsn,
                )
                .await
                .map_err(|e| SinkError::Other(format!("opt-in: {e}")))?;
            }
            ConfigEvent::TableRemoved { rel } => {
                resolver.exclude_table(rel).await;
                if let Some(b) = &self.backfiller {
                    b.note_opt_out(rel).await;
                }
            }
            _ => {}
        }
        Ok(())
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

    /// Poll-based DROP discovery, run only at the commit of an xact that
    /// wrote pg_class heap_delete (ADD COLUMN / VACUUM noise never arms)
    /// so the replay gate makes the drop visible in shadow. Same as
    /// `XactRecordSink`'s commit branch.
    async fn maybe_sweep_dropped(
        &mut self,
        xid: u32,
        payload: &crate::xact_buffer::XactCommitPayload,
        source_lsn: u64,
    ) -> Result<(), SinkError> {
        if self.schema_events.is_none() {
            return Ok(());
        }
        let Some(pending) = &self.pending_sweeps else {
            return Ok(());
        };
        if !pending.disarm(xid, payload.twophase_xid, &payload.subxacts) {
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
            .map_err(|e| SinkError::Other(format!("ddl apply: {e}")))?;
        // A `CREATE TABLE` for a forward-declared opt-in materialises here, in
        // the same barrier before this xact's trailing rows dispatch.
        if let SchemaEvent::Added { desc } = event
            && let Some(resolver) = self.config_resolver.clone()
        {
            crate::opt_in::materialize_pending_on_added(&resolver, &mut self.applicator, desc)
                .await
                .map_err(|e| SinkError::Other(format!("opt-in materialize: {e}")))?;
        }
        Ok(())
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
            .truncate(&rel.rel_name)
            .await
            .map_err(|e| SinkError::Other(format!("ch truncate: {e}")))?;
        self.stats.truncates_emitted.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Process a barrier xact in source_lsn order: data rows accumulate into
    /// segments; each DDL event / TRUNCATE is preceded by dispatching the
    /// pending segment + a fence (so earlier data is durable first).
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
                match &ordered_events[ev_cursor].1 {
                    DrainEntry::Catalog(ev) => self.apply_event(ev).await?,
                    DrainEntry::Config(ev) => self.apply_config(ev, commit_lsn).await?,
                }
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
            match &ordered_events[ev_cursor].1 {
                DrainEntry::Catalog(ev) => self.apply_event(ev).await?,
                DrainEntry::Config(ev) => self.apply_config(ev, commit_lsn).await?,
            }
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
        self.maybe_sweep_dropped(xid, &payload, record.source_lsn)
            .await?;
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
                    parent: &txn,
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
                    parent: &txn,
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
        // Rolled-back pg_class heap_delete resurrects the row; drop the arm
        if let Some(pending) = &self.pending_sweeps {
            pending.disarm(xid, payload.twophase_xid, &payload.subxacts);
        }
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
