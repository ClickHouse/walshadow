//! Reorder worker — single-threaded commit-order coordinator.
//!
//! Runs as inner sink of the `QueueingRecordSink` worker, off the WAL pump
//! task (replay gates never pace wire delivery). Pairs with
//! [`BufferingDecoderSink`](crate::xact::xact_buffer::BufferingDecoderSink); on each
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
use std::sync::atomic::{AtomicU64, Ordering};

use std::collections::{HashMap, HashSet};
use tokio::sync::{Mutex, mpsc, oneshot, watch};
use walrus::pg::walparser::RmId;

use crate::catalog::desc_log::DescriptorLog;
use crate::catalog::shadow_catalog::ShadowCatalog;
use crate::decode::heap_decoder::{DescribedHeap, HeapOp};
use crate::emit::ch_ddl::DdlApplicator;
use crate::emit::ch_emitter::EmitterStats;
use crate::record::{Record, RecordSink, SinkError};
use crate::schema::{RelDescriptor, RelName, SchemaEvent};
use tracing::Instrument;

use crate::decode::wal_xact::{
    XLOG_XACT_ABORT, XLOG_XACT_ABORT_PREPARED, XLOG_XACT_ASSIGNMENT, XLOG_XACT_COMMIT,
    XLOG_XACT_COMMIT_PREPARED, XLOG_XACT_OPMASK, parse_xact_assignment, parse_xact_payload,
};
use crate::ops::trace::TxnSpanRegistry;
use crate::xact::xact_buffer::{DrainEntry, SubxactTracker, XactBuffer};

use crate::config::{ConfigResolver, ResolvedConfig};
use crate::emit::pipeline::Fatal;
use crate::emit::pipeline::ack::AckHandle;
use crate::emit::pipeline::batcher::BatcherMsg;
use crate::emit::pipeline::decode::DecodeJob;
use crate::emit::pipeline::plan_spool::{PlanItem, SealedPlan};
use crate::emit::pipeline::planner::{PlanRouteView, Planner, drain_reason};
use crate::emit::route::{RouteSnapshot, RoutedHeap};
use crate::mapping::{MappingHandle, MappingSnapshot, TableMapping};
use crate::runtime_config::{ConfigEvent, TableRow};
use crate::toast::ToastResolver;
use crate::toast::toast_retire::RetireLedger;

pub struct ReorderSink {
    buffer: Arc<Mutex<XactBuffer>>,
    /// Interval-scoped descriptor oracle: stash resolution + truncate
    log: Arc<DescriptorLog>,
    /// Opt-in dispatch still resolves by name against live shadow
    catalog: Arc<Mutex<ShadowCatalog>>,
    subxact_tracker: Arc<Mutex<SubxactTracker>>,
    /// `None` on the metrics-only (null-tail) configuration: schema events
    /// and truncates are observed, never applied to CH
    applicator: Option<DdlApplicator>,
    ack: AckHandle,
    jobs_tx: async_channel::Sender<DecodeJob>,
    /// Shared FIFO channel to the batcher; `FlushAll` here orders after
    /// enqueued rows.
    msg_tx: mpsc::Sender<BatcherMsg>,
    fatal: Fatal,
    /// Reorder owns the commit-order boundary, so bumps `xacts_committed`
    /// (per commit) and `truncates_emitted`.
    stats: Arc<EmitterStats>,
    /// TOAST chunk resolver shared with decode workers
    resolver: ToastResolver,
    /// Runtime-config overlay resolver. `Some` with the config overlay active;
    /// a `DrainEntry::Config` applies to it inside the barrier fence so
    /// trailing rows route against post-config state (plan §6).
    config_resolver: Option<Arc<ConfigResolver>>,
    /// COPY backfiller for `initial_load='copy'` opt-ins; spawns off the barrier
    /// (detached task, own CH tail), the apply only records + launches.
    backfiller: Option<Arc<dyn crate::backfill::opt_in::Backfiller>>,
    /// Retires wait until persisted replay floor passes dropping commit;
    /// ledger persists queue so a stop inside the wait window can't leak
    /// the mirror (resume never replays the drop)
    retires: RetireLedger,
    /// Persisted resolved floor (aligned, archive-clamped) — the position
    /// a crash-now restart resumes from. Seeded at the resolved start,
    /// advanced only after each manifest persist.
    resume_floor: Arc<AtomicU64>,
    /// Dense commit-order counter; one seq per dispatched data unit.
    next_seq: u64,
    /// Drain-slice budget: rows / bytes per [`DrainedBatch`] pulled from the
    /// buffer. Bounds resident decoded rows while a spilled xact streams
    /// back; the decode pool works one slice while the next loads.
    batch_rows: usize,
    batch_bytes: usize,
    /// Global resident-payload pool; slice admission acquired here before
    /// dispatch, riding rows to insert ack. `None` = unmetered (tests)
    budget: Option<crate::budget::MemoryBudget>,
    /// Per-txn span map (shared with the pump + buffer). `Some` only when
    /// OTLP tracing is on; reorder parents `commit.drain`/`dispatch` under
    /// the `txn` and prunes the entry at commit (the buffer prunes at abort).
    span_registry: Option<TxnSpanRegistry>,
    /// Live-reload receiver + the config-driven opt-in set applied so far. On a
    /// republish (`ctl reload` / SIGHUP), the coordinator diffs `table_opt_ins`
    /// at the next commit barrier — add → `apply_table_opt_in`, drop →
    /// `exclude_table` (CH table retained).
    reload_rx: Option<watch::Receiver<Arc<ResolvedConfig>>>,
    applied_opt_ins: HashSet<RelName>,
    /// Opt-ins whose descriptor the shadow catalog can't resolve yet — a table
    /// created just before `ctl tables select` races the CREATE's replay into
    /// the shadow. Retried each commit until it resolves, then created +
    /// backfilled (moves to `applied_opt_ins`).
    pending_opt_ins: HashMap<RelName, TableRow>,
    /// Shared routing map, snapshotted into `route_mapping` at route-state
    /// resets (this coordinator's own event applies are the fenced writers).
    mapping: MappingHandle,
    /// Boot-only delete-retention policy, frozen into route snapshots.
    soft_delete: bool,
    /// Byte cap per transaction plan spool file
    plan_disk_max: u64,
    /// Plan spool directory (the xact spill dir), cached at spawn so the
    /// per-commit path needs no buffer lock
    plan_dir: std::path::PathBuf,
    /// Mapping version frozen with the memo reset: one transaction plans
    /// against one mapping version, a concurrent republish (SIGHUP /
    /// control-socket, no WAL position) can't split its rows. In-walk event
    /// applies re-snapshot so trailing heaps see the event's own effect.
    route_mapping: Option<MappingSnapshot>,
    /// Resolved-config snapshot taken with the memo reset; every override in
    /// one interval comes from one snapshot, never a mid-interval republish.
    route_config: Option<Arc<ResolvedConfig>>,
}

impl ReorderSink {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        buffer: Arc<Mutex<XactBuffer>>,
        log: Arc<DescriptorLog>,
        catalog: Arc<Mutex<ShadowCatalog>>,
        subxact_tracker: Arc<Mutex<SubxactTracker>>,
        applicator: Option<DdlApplicator>,
        ack: AckHandle,
        jobs_tx: async_channel::Sender<DecodeJob>,
        msg_tx: mpsc::Sender<BatcherMsg>,
        stats: Arc<EmitterStats>,
        resolver: ToastResolver,
        config_resolver: Option<Arc<ConfigResolver>>,
        backfiller: Option<Arc<dyn crate::backfill::opt_in::Backfiller>>,
        fatal: Fatal,
        span_registry: Option<TxnSpanRegistry>,
        batch_rows: usize,
        batch_bytes: usize,
        plan_disk_max: u64,
        plan_dir: std::path::PathBuf,
        budget: Option<crate::budget::MemoryBudget>,
        retires: RetireLedger,
        resume_floor: Arc<AtomicU64>,
        mapping: MappingHandle,
        soft_delete: bool,
    ) -> Self {
        // subscribe() marks the current value seen, so a `ctl reload`
        // racing pipeline spawn would stay invisible to has_changed —
        // and seeding the applied set from that raced value would record
        // its opt-ins as done without ever applying them. Start empty and
        // force the first commit to diff from scratch: re-applying
        // boot-seeded opt-ins is the designed-idempotent restart path
        // (CH table persists, backfill ledger dedups)
        let reload_rx = config_resolver.as_ref().map(|r| {
            let mut rx = r.subscribe();
            rx.mark_changed();
            rx
        });
        Self {
            buffer,
            log,
            catalog,
            subxact_tracker,
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
            batch_rows,
            batch_bytes,
            plan_disk_max,
            plan_dir,
            budget,
            span_registry,
            retires,
            resume_floor,
            reload_rx,
            applied_opt_ins: HashSet::new(),
            pending_opt_ins: HashMap::new(),
            mapping,
            soft_delete,

            route_mapping: None,
            route_config: None,
        }
    }

    /// Route-point steps 1–2 close out here: preceding schema/config state is
    /// applied, so drop memoized routes and freeze mapping + resolved-config
    /// versions for the next interval. Per commit this is the
    /// whole-transaction snapshot; a non-WAL-positioned republish landing
    /// mid-plan can't reroute rows already planned or split the transaction.
    async fn reset_route_state(&mut self) {
        self.route_mapping = Some(self.mapping.read().await.clone());
        self.route_config = self.reload_rx.as_ref().map(|rx| rx.borrow().clone());
    }

    /// Apply a live config reload's table opt-in/opt-out diff at a commit
    /// barrier (`opt_in_lsn = commit_lsn`). Base config (mappings/budgets/CH
    /// connection) already republished onto the watch; here we do the part that
    /// needs the applicator/catalog — create/drop the CH scope.
    async fn maybe_apply_reload(&mut self, commit_lsn: u64) -> Result<(), SinkError> {
        let Some(resolver) = self.config_resolver.clone() else {
            return Ok(());
        };
        // On a republish, re-diff `table_opt_ins`: opt-outs drain now, new
        // opt-ins queue as pending, dropped intents leave the queue.
        let changed = self
            .reload_rx
            .as_mut()
            .is_some_and(|rx| rx.has_changed().unwrap_or(false));
        if changed {
            let desired: Vec<(RelName, TableRow)> = {
                let rx = self.reload_rx.as_mut().unwrap();
                let snap = rx.borrow_and_update();
                snap.table_opt_ins
                    .iter()
                    .map(|(rel, row)| (rel.clone(), row.clone()))
                    .collect()
            };
            let desired_in: HashSet<RelName> = desired
                .iter()
                .filter(|(_, row)| row.replicate == Some(true))
                .map(|(rel, _)| rel.clone())
                .collect();
            let stale: Vec<RelName> = self
                .applied_opt_ins
                .iter()
                .filter(|rel| !desired_in.contains(*rel))
                .cloned()
                .collect();
            for rel in stale {
                resolver.exclude_table(&rel).await;
                if let Some(b) = &self.backfiller {
                    b.note_opt_out(&rel).await;
                }
                self.applied_opt_ins.remove(&rel);
            }
            self.pending_opt_ins
                .retain(|rel, _| desired_in.contains(rel));
            for (rel, row) in desired {
                if row.replicate == Some(true) && !self.applied_opt_ins.contains(&rel) {
                    self.pending_opt_ins.insert(rel, row);
                }
            }
            tracing::info!(
                target: "walshadow::config",
                pending = self.pending_opt_ins.len(),
                applied = self.applied_opt_ins.len(),
                "reload diff applied",
            );
        }
        // Each commit, apply any pending opt-in the shadow catalog can now
        // resolve — a table created just before `select` races the CREATE's
        // replay, so retry until the descriptor lands, then create + backfill.
        if self.pending_opt_ins.is_empty() {
            return Ok(());
        }
        // No applicator (bootstrap drain / tests without DDL) → can't create
        // CH tables, so opt-ins stay pending.
        let Some(applicator) = self.applicator.as_mut() else {
            return Ok(());
        };
        let candidates: Vec<(RelName, TableRow)> = self
            .pending_opt_ins
            .iter()
            .map(|(rel, row)| (rel.clone(), row.clone()))
            .collect();
        for (rel, row) in candidates {
            let known = self
                .catalog
                .lock()
                .await
                .descriptor_by_name(&rel)
                .await
                .map_err(|e| SinkError::Other(format!("opt-in descriptor lookup: {e}")))?
                .is_some();
            if !known {
                tracing::debug!(
                    target: "walshadow::config",
                    qname = %rel,
                    "opt-in retry: descriptor unknown",
                );
                continue;
            }
            crate::backfill::opt_in::apply_table_opt_in(
                &resolver,
                applicator,
                &self.catalog,
                self.backfiller.as_ref(),
                &rel,
                &row,
                commit_lsn,
            )
            .await
            .map_err(|e| SinkError::Other(format!("reload opt-in: {e}")))?;
            self.pending_opt_ins.remove(&rel);
            self.applied_opt_ins.insert(rel);
        }
        Ok(())
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
                if let Some(applicator) = self.applicator.as_mut() {
                    crate::backfill::opt_in::apply_table_opt_in(
                        &resolver,
                        applicator,
                        &self.catalog,
                        self.backfiller.as_ref(),
                        rel,
                        row,
                        commit_lsn,
                    )
                    .await
                    .map_err(|e| SinkError::Other(format!("opt-in: {e}")))?;
                }
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

    async fn apply_event(&mut self, event: &SchemaEvent, commit_lsn: u64) -> Result<(), SinkError> {
        let Some(applicator) = self.applicator.as_mut() else {
            return Ok(());
        };
        applicator
            .apply(event)
            .await
            .map_err(|e| SinkError::Other(format!("ddl apply: {e}")))?;
        // A `CREATE TABLE` for a forward-declared opt-in materialises here, in
        // the same barrier before this xact's trailing rows dispatch.
        if let SchemaEvent::Added { desc } = event
            && let Some(resolver) = self.config_resolver.clone()
        {
            crate::backfill::opt_in::materialize_pending_on_added(&resolver, applicator, desc)
                .await
                .map_err(|e| SinkError::Other(format!("opt-in materialize: {e}")))?;
        }
        // Immediate DROP wipe corrupts same-LSN replay fills. Ledger fsync
        // here precedes this commit's ack publication, so any persisted
        // cursor whose floor passed the drop already holds the entry.
        if let SchemaEvent::Dropped { oid, rel_name } = event
            && &*rel_name.namespace == "pg_toast"
            && self.resolver.stores_chunks()
        {
            self.retires
                .push(*oid, commit_lsn)
                .await
                .map_err(|e| SinkError::Other(format!("toast retire ledger: {e}")))?;
        }
        Ok(())
    }

    /// Retire below persisted resolved floor
    ///
    /// Restart resumes at the floor, DROP lock excludes later referrers.
    /// Pub for boot: entries due at resume must retire during standup —
    /// their drop never replays, so no commit re-triggers this flush.
    /// Ledger removal persists after each wipe; a crash between re-runs
    /// an idempotent `TRUNCATE` on the emptied mirror
    /// Boot Added pass: every relation `Present` in the descriptor log at
    /// resume gets an `Added` apply (idempotent `CREATE TABLE IF NOT
    /// EXISTS` + forward-declaration materialise). Runs pre-pump every
    /// boot, like [`Self::flush_due_retires`] — brownfield auto-create
    /// tables exist at attach instead of first write, and newly enabled
    /// auto-create/mapping picks up existing rels without log mutation.
    pub async fn apply_boot_events(
        &mut self,
        descs: Vec<Arc<RelDescriptor>>,
        resume_lsn: u64,
    ) -> Result<(), SinkError> {
        for desc in descs {
            if desc.kind == 't' {
                continue;
            }
            self.apply_event(&SchemaEvent::Added { desc }, resume_lsn)
                .await?;
        }
        Ok(())
    }

    pub async fn flush_due_retires(&mut self) -> Result<(), SinkError> {
        // Disabled resolver no-ops retire_mirror: flushing would drop ledger
        // entries without wiping mirrors, leaking them for a later CH run
        // over the same spill dir
        if !self.resolver.stores_chunks() || self.retires.is_empty() {
            return Ok(());
        }
        let cut = self.resume_floor.load(Ordering::Acquire);
        for (oid, commit_lsn) in self.retires.due(cut) {
            self.resolver
                .retire_mirror(oid)
                .await
                .map_err(|e| SinkError::Other(format!("toast mirror retire: {e}")))?;
            self.retires
                .remove(oid, commit_lsn)
                .await
                .map_err(|e| SinkError::Other(format!("toast retire ledger: {e}")))?;
        }
        Ok(())
    }

    /// Residual `O - B` deaths for one rewrite generation; barrier loop
    /// already flushed its births
    async fn apply_toast_barrier(
        &mut self,
        toast_relid: u32,
        marker_lsn: u64,
        commit_lsn: u64,
    ) -> Result<(), SinkError> {
        self.resolver
            .rewrite_barrier(toast_relid, marker_lsn, commit_lsn)
            .await
            .map_err(|e| SinkError::Other(format!("toast rewrite barrier: {e}")))
    }

    async fn apply_drain_entry(
        &mut self,
        entry: &DrainEntry,
        commit_lsn: u64,
    ) -> Result<(), SinkError> {
        match entry {
            DrainEntry::Catalog(ev) => self.apply_event(ev, commit_lsn).await,
            DrainEntry::Config(ev) => self.apply_config(ev, commit_lsn).await,
            DrainEntry::ToastBarrier {
                toast_relid,
                marker_lsn,
            } => {
                self.apply_toast_barrier(*toast_relid, *marker_lsn, commit_lsn)
                    .await
            }
        }
    }

    async fn apply_truncate(&mut self, heap: &DescribedHeap) -> Result<(), SinkError> {
        let Some(applicator) = self.applicator.as_mut() else {
            return Ok(());
        };
        // Attached at truncate fan-out (record time = pre-capture Present),
        // so the rotation's drain-time Retired answer no longer needs a
        // predecessor walk here
        let rel = &heap.descriptor;
        applicator
            .truncate(&rel.rel_name)
            .await
            .map_err(|e| SinkError::Other(format!("ch truncate: {e}")))?;
        self.stats.truncates_emitted.fetch_add(1, Ordering::Relaxed);
        // PG swaps TOAST relfilenode without listing it in `xl_heap_truncate`;
        // the descriptor carries the owner's toast oid
        if self.resolver.stores_chunks() && rel.toast_oid != 0 {
            self.resolver
                .truncate_mirror(rel.toast_oid)
                .await
                .map_err(|e| SinkError::Other(format!("toast mirror truncate: {e}")))?;
        }
        Ok(())
    }

    /// Materialize plan mirror rows `[cursor..end)` just in time; global
    /// indices span the plan's carried row batches in order
    async fn put_plan_rows(
        &mut self,
        plan: &SealedPlan,
        cursor: &mut usize,
        end: usize,
    ) -> Result<(), SinkError> {
        let mut base = 0usize;
        for rb in &plan.row_batches {
            let (lo, hi) = ((*cursor).max(base), end.min(base + rb.len()));
            if lo < hi {
                self.resolver
                    .put_row_refs(rb.spool(), &rb[lo - base..hi - base])
                    .await
                    .map_err(|e| SinkError::Other(format!("toast store put: {e}")))?;
            }
            base += rb.len();
        }
        *cursor = (*cursor).max(end);
        Ok(())
    }

    /// Dispatch accumulated planned heaps as one seq under a fresh
    /// admission permit. Chunks ride empty: values detoasted at planning.
    /// `publish` marks the commit's final data segment so its seq carries
    /// the LSN publication (no trailing marker needed)
    async fn dispatch_planned(
        &mut self,
        pending: &mut Vec<RoutedHeap>,
        pending_bytes: &mut usize,
        commit_ts: i64,
        commit_lsn: u64,
        publish: bool,
    ) -> Result<(), SinkError> {
        if pending.is_empty() {
            return Ok(());
        }
        let heaps = std::mem::take(pending);
        let bytes = std::mem::take(pending_bytes);
        let permit = match &self.budget {
            Some(b) => Some(Arc::new(b.admit(bytes).await)),
            None => None,
        };
        let seq = self.alloc_seq();
        if publish {
            self.ack.register(seq, commit_lsn);
        } else {
            self.ack.register_partial(seq, commit_lsn);
        }
        let job = DecodeJob {
            seq,
            commit_ts,
            commit_lsn,
            heaps,
            chunks: Vec::new(),
            permit,
        };
        self.dispatch_job(job).await
    }

    /// Replay one sealed plan through the existing barrier ordering. Routes
    /// ride the plan — nothing re-resolves. Heap segments slice at the
    /// batch budget; controls fence then apply their real side effects at
    /// their pinned positions; mirror rows put just in time; truncates
    /// fence per their carried cursor. The final data segment publishes the
    /// commit LSN when nothing follows it; otherwise the caller's trailing
    /// rows=0 marker does. Returns dispatched rows + whether it published
    pub async fn execute_plan(&mut self, plan: &SealedPlan) -> Result<(u64, bool), SinkError> {
        let (commit_ts, commit_lsn) = (plan.commit_ts, plan.commit_lsn);
        // Mem-resident plans hold the bytes validated at write; file-backed
        // plans re-read from disk, checksum-verify fully before the first
        // side effect so corruption fails the whole transaction
        if plan.path().is_some() {
            plan.verify()
                .map_err(|e| SinkError::Other(format!("plan verify: {e}")))?;
        }
        let mut rd = plan
            .replay()
            .map_err(|e| SinkError::Other(format!("plan replay: {e}")))?;
        let mut pending: Vec<RoutedHeap> = Vec::new();
        let mut pending_bytes = 0usize;
        let mut rows_cursor = 0usize;
        let mut trunc = plan.truncate_rows.iter().copied();
        let total_rows: usize = plan.row_batches.iter().map(|rb| rb.len()).sum();
        let mut rows_total = 0u64;
        while let Some(item) = rd
            .next_item()
            .map_err(|e| SinkError::Other(format!("plan replay: {e}")))?
        {
            match item {
                PlanItem::Control(c) => {
                    self.put_plan_rows(plan, &mut rows_cursor, c.row_idx)
                        .await?;
                    self.dispatch_planned(
                        &mut pending,
                        &mut pending_bytes,
                        commit_ts,
                        commit_lsn,
                        false,
                    )
                    .await?;
                    self.barrier_fence().await?;
                    self.apply_drain_entry(&c.event, commit_lsn).await?;
                }
                PlanItem::Heap(h) if matches!(h.described.decoded.op, HeapOp::Truncate) => {
                    let upto = trunc.next().unwrap_or(rows_cursor);
                    self.put_plan_rows(plan, &mut rows_cursor, upto).await?;
                    self.dispatch_planned(
                        &mut pending,
                        &mut pending_bytes,
                        commit_ts,
                        commit_lsn,
                        false,
                    )
                    .await?;
                    self.barrier_fence().await?;
                    self.apply_truncate(&h.described).await?;
                }
                PlanItem::Heap(h) => {
                    rows_total += 1;
                    pending_bytes += h.described.approx_bytes();
                    pending.push(h);
                    if pending.len() >= self.batch_rows || pending_bytes >= self.batch_bytes {
                        self.dispatch_planned(
                            &mut pending,
                            &mut pending_bytes,
                            commit_ts,
                            commit_lsn,
                            false,
                        )
                        .await?;
                    }
                }
            }
        }
        self.put_plan_rows(plan, &mut rows_cursor, total_rows)
            .await?;
        let publish = !pending.is_empty();
        self.dispatch_planned(
            &mut pending,
            &mut pending_bytes,
            commit_ts,
            commit_lsn,
            publish,
        )
        .await?;
        Ok((rows_total, publish))
    }

    async fn on_commit(
        &mut self,
        xid: u32,
        info: u8,
        record: &Record<'_>,
    ) -> Result<(), SinkError> {
        self.flush_due_retires().await?;
        let payload = parse_xact_payload(info, &record.parsed.main_data, record.page_magic)
            .unwrap_or_default();
        // COMMIT PREPARED: header xid is the finishing backend's (0-ish),
        // the buffered work lives under the prepared xid — drain there, or
        // capture-keyed events would never leave the buffer
        let xid = payload.twophase_xid.unwrap_or(xid);
        // Parent for this commit's spans; held until on_commit returns so it
        // outlives the prune below. No-op span when tracing off/unsampled.
        let txn = self
            .span_registry
            .as_ref()
            .and_then(|r| r.txn_span(xid))
            .unwrap_or_else(tracing::Span::none);
        crate::xact::xact_buffer::resolve_stash(
            &self.buffer,
            &self.log,
            xid,
            &payload.subxacts,
            record.next_lsn,
            self.stats.clone(),
        )
        .await
        .map_err(SinkError::from)?;
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
        let mut drain = {
            let mut buf = self.buffer.lock().await;
            buf.drain_committed(
                xid,
                payload.xact_time,
                record.source_lsn,
                &payload.subxacts,
                self.resolver.stores_chunks(),
            )
            .instrument(reorder_span)
            .await
            .map_err(SinkError::from)?
        };
        self.subxact_tracker.lock().await.forget_tree(xid);
        // One per drained commit, incl. empty / unmapped-only
        self.stats.xacts_committed.fetch_add(1, Ordering::Relaxed);
        // Prune the committed tree's span handles (else the map grows
        // unbounded); the local `txn` clone keeps the span alive for dispatch.
        if let Some(r) = &self.span_registry {
            let mut xids: Vec<u32> = Vec::with_capacity(1 + payload.subxacts.len());
            xids.push(xid);
            xids.extend_from_slice(&payload.subxacts);
            r.prune(&xids);
        }

        // Plan the whole transaction side-effect-free, then execute the
        // sealed plan: every input-derived failure surfaces before the first
        // side effect. A planning error abandons the plan file (writer drop
        // unlinks) and the transaction emits nothing.
        let commit_ts = drain.commit_ts;
        let commit_lsn = drain.commit_lsn;
        // Apply any pending live-reload opt-in/opt-out diff before this commit's
        // rows so newly-selected tables are in scope + created for it.
        self.maybe_apply_reload(commit_lsn).await?;
        // One route state per transaction: a mid-commit config republish
        // can't split this xact's rows across two route versions. In-walk
        // catalog events fold into the plan-time view, not shared state.
        self.reset_route_state().await;
        let mut rows_total: u64 = 0;
        let mut published = false;
        if drain.had_states {
            let plan_path = self.plan_dir.join(format!("xact-{xid}-{commit_lsn}.plan"));
            let plan = {
                let mut view = ReorderRouteView::new(
                    self.route_mapping.clone(),
                    self.route_config.clone(),
                    self.soft_delete,
                    self.applicator.as_mut(),
                    self.stats.clone(),
                );
                let resolver = self.resolver.clone();
                let (batch_rows, batch_bytes) = (self.batch_rows, self.batch_bytes);
                let budget = self.budget.clone();
                let stats = self.stats.clone();
                let mut planner =
                    Planner::create(plan_path, self.plan_disk_max, &mut view, &resolver).map_err(
                        |e| {
                            bump_plan_failure(&stats, e.reason());
                            SinkError::Other(format!("plan open: {e}"))
                        },
                    )?;
                loop {
                    let Some(batch) = drain
                        .next_batch(batch_rows, batch_bytes, budget.as_ref())
                        .instrument(drain_span.clone())
                        .await
                        .map_err(|e| {
                            bump_plan_failure(&stats, drain_reason(&e));
                            SinkError::from(e)
                        })?
                    else {
                        break;
                    };
                    let is_final = batch.is_final;
                    planner.plan_batch(batch).await.map_err(|e| {
                        bump_plan_failure(&stats, e.reason());
                        SinkError::Other(format!("plan: {e}"))
                    })?;
                    if is_final {
                        break;
                    }
                }
                planner.seal(commit_lsn, commit_ts).map_err(|e| {
                    bump_plan_failure(&stats, e.reason());
                    SinkError::Other(format!("plan seal: {e}"))
                })?
            };
            self.stats
                .plan_rows
                .fetch_add(plan.routed_count, Ordering::Relaxed);
            let plan_bytes = if plan.path().is_some() {
                &self.stats.plan_bytes_file
            } else {
                &self.stats.plan_bytes_mem
            };
            plan_bytes.fetch_add(plan.size_bytes, Ordering::Relaxed);
            (rows_total, published) = self
                .execute_plan(&plan)
                .instrument(trace_span!(
                    !txn.is_none(),
                    parent: &txn,
                    "commit.execute",
                ))
                .await?;
        }
        // Unlink spill files now that every segment dispatched; an error above
        // drops the drain instead, leaving files for inspection.
        drain.finish().await.map_err(SinkError::from)?;
        txn.record("rows", rows_total);
        txn.record("outcome", "committed");
        if !published {
            // rows=0 marker: publishes commit_lsn once every earlier partial
            // segment is durable. Covers empty / read-only commits and plans
            // whose tail is a control or truncate.
            let seq = self.alloc_seq();
            self.ack.register(seq, commit_lsn);
            self.ack.placed(seq, 0);
        }
        Ok(())
    }

    /// ABORT: drop the buffer, emit a rows=0 seq through the gate (never a
    /// direct ack bump).
    async fn on_abort(&mut self, xid: u32, info: u8, record: &Record<'_>) -> Result<(), SinkError> {
        let payload = parse_xact_payload(info, &record.parsed.main_data, record.page_magic)
            .unwrap_or_default();
        // ABORT PREPARED: buffered state keys off the prepared xid
        let xid = payload.twophase_xid.unwrap_or(xid);
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

/// Route a plan-failure reason label onto its counter
fn bump_plan_failure(stats: &EmitterStats, reason: &'static str) {
    let counter = match reason {
        "spool" => &stats.plan_failures_spool,
        "fail_closed" => &stats.plan_failures_fail_closed,
        "detoast" => &stats.plan_failures_detoast,
        "partial_update" => &stats.plan_failures_partial_update,
        "view" => &stats.plan_failures_view,
        _ => &stats.plan_failures_drain,
    };
    counter.fetch_add(1, Ordering::Relaxed);
}

/// Plan-time route state for one transaction: frozen mapping + config
/// versions plus a local fold of in-walk catalog entries. The applicator
/// predicts what executing each event will leave in the routing map
/// ([`DdlApplicator::predict_route_mapping`]) so post-event rows plan under
/// post-event routes while the real side effects wait for the executor.
/// Config-table events are not folded: a same-xact config write followed
/// by rows plans under the frozen version (whole-transaction granularity;
/// the in-xact interval refinement lands with the config fold)
pub struct ReorderRouteView<'a> {
    mapping: Option<MappingSnapshot>,
    config: Option<Arc<ResolvedConfig>>,
    /// Catalog fold above `mapping`; `None` value = locally dropped
    overlay: HashMap<RelName, Option<TableMapping>>,
    memo: HashMap<RelName, Option<Arc<RouteSnapshot>>>,
    soft_delete: bool,
    applicator: Option<&'a mut DdlApplicator>,
    stats: Arc<EmitterStats>,
}

impl<'a> ReorderRouteView<'a> {
    pub fn new(
        mapping: Option<MappingSnapshot>,
        config: Option<Arc<ResolvedConfig>>,
        soft_delete: bool,
        applicator: Option<&'a mut DdlApplicator>,
        stats: Arc<EmitterStats>,
    ) -> Self {
        Self {
            mapping,
            config,
            overlay: HashMap::new(),
            memo: HashMap::new(),
            soft_delete,
            applicator,
            stats,
        }
    }
}

impl PlanRouteView for ReorderRouteView<'_> {
    fn route_for(&mut self, heap: &DescribedHeap) -> Option<Arc<RouteSnapshot>> {
        let rel_name = &heap.descriptor.rel_name;
        if let Some(r) = self.memo.get(rel_name) {
            return r.clone();
        }
        let mapped = match self.overlay.get(rel_name) {
            Some(o) => o.clone(),
            None => self.mapping.as_ref().and_then(|m| m.get(rel_name)).cloned(),
        };
        let route = mapped.map(|m| {
            let overrides = self
                .config
                .as_ref()
                .and_then(|rc| rc.columns.get(rel_name))
                .cloned()
                .map(Arc::new)
                .unwrap_or_default();
            RouteSnapshot::freeze(Arc::new(m), overrides, self.soft_delete)
        });
        let result = if route.is_none() {
            self.stats
                .unsupported_relations
                .fetch_add(1, Ordering::Relaxed);
            &self.stats.route_snapshots_unmapped
        } else {
            &self.stats.route_snapshots_mapped
        };
        result.fetch_add(1, Ordering::Relaxed);
        self.memo.insert(rel_name.clone(), route.clone());
        route
    }

    async fn apply(&mut self, entry: &DrainEntry) -> Result<(), String> {
        let DrainEntry::Catalog(ev) = entry else {
            // Config: frozen-version planning (doc above); ToastBarrier:
            // no route effect
            return Ok(());
        };
        let Some(app) = self.applicator.as_deref_mut() else {
            return Ok(());
        };
        if let Some((rel, m)) = app
            .predict_route_mapping(ev)
            .await
            .map_err(|e| e.to_string())?
        {
            self.memo.remove(&rel);
            self.overlay.insert(rel, m);
        }
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
                self.buffer.lock().await.advance_idle(lsn);
                // Quiescent source never re-enters on_commit; retire due
                // drops here so the flush doesn't wait for a later commit
                self.flush_due_retires().await?;
            }
            Ok(())
        })
    }
}
