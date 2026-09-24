//! Reorder worker — single-threaded commit-order coordinator.
//!
//! Runs as inner sink of the `QueueingRecordSink` worker, off the WAL pump
//! task (replay gates never pace wire delivery). Pairs with
//! [`BufferingDecoderSink`](crate::xact::xact_buffer::BufferingDecoderSink); on each
//! COMMIT/ABORT assigns a dense `seq`, registers it with the collector in
//! order, then either places its rows onto the batcher or — for a
//! DDL/TRUNCATE barrier — quiesces, drains earlier seqs to durable, and
//! applies the schema change via [`DdlApplicator`] before resuming.
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

use tokio::sync::{Mutex, mpsc, oneshot, watch};
use walrus::pg::walparser::RmId;

use crate::backfill::backfill_staging::StagingSession;
use crate::backfill::visibility_pending::{self, SharedPendingLedger};
use crate::catalog::pending::PendingCatalog;
use crate::decode::heap_decoder::{DescribedHeap, HeapOp};
use crate::decode::visibility::{PgXactPatch, PgXactView, read_pg_xact};
use crate::emit::ch_ddl::DdlApplicator;
use crate::emit::ch_emitter::EmitterStats;
use crate::record::{Record, RecordSink, SinkError};
use crate::schema::{RelDescriptor, RelName, SchemaEvent};
use ahash::{HashMap, HashMapExt, HashSet, HashSetExt};
use tracing::Instrument;

use crate::decode::wal_xact::{
    XLOG_XACT_ABORT, XLOG_XACT_ABORT_PREPARED, XLOG_XACT_ASSIGNMENT, XLOG_XACT_COMMIT,
    XLOG_XACT_COMMIT_PREPARED, XLOG_XACT_OPMASK, parse_xact_assignment, parse_xact_payload,
};
use crate::ops::trace::TxnSpanRegistry;
use crate::xact::xact_buffer::{DrainEntry, SubxactTracker, XactBuffer};

use crate::config::ResolvedConfig;
use crate::emit::pipeline::Fatal;
use crate::emit::pipeline::ack::AckHandle;
use crate::emit::pipeline::batcher::BatcherMsg;
use crate::emit::pipeline::decode;
use crate::emit::pipeline::plan_spool::{PlanItem, SealedPlan};
use crate::emit::pipeline::planner::{PlanRouteView, Planner, drain_reason};
use crate::emit::route::{RouteSnapshot, RoutedHeap, RowPolicy};
use crate::mapping::{MappingSnapshot, TableMapping};
use crate::pos::{Floor, Monotone, Pos};
use crate::runtime_config::{ConfigEvent, TableRow};
use crate::source_db::{SourceDb, SourceDbs};
use crate::toast::ToastResolver;
use crate::toast::toast_retire::RetireLedger;
use tokio_postgres::types::Oid;

/// One followed database's ClickHouse-side state. Every transaction writes
/// one database, so a commit selects one of these and keeps it for the whole
/// drain
struct DbScope {
    db: Arc<SourceDb>,
    /// `None` observes schema events without applying CH DDL
    applicator: Option<DdlApplicator>,
    /// Live-reload receiver; see [`ReorderSink::maybe_apply_reload`]
    reload_rx: Option<watch::Receiver<Arc<ResolvedConfig>>>,
    /// COPY backfiller for this database's `initial_load` opt-ins
    backfiller: Option<Arc<dyn crate::backfill::opt_in::Backfiller>>,
    applied_opt_ins: HashSet<RelName>,
    /// Opt-ins whose descriptor the shadow catalog can't resolve yet — a table
    /// created just before `ctl tables select` races the CREATE's replay into
    /// the shadow. Retried each commit until it resolves, then created +
    /// backfilled (moves to `applied_opt_ins`).
    pending_opt_ins: HashMap<RelName, TableRow>,
}

pub struct ReorderSink {
    buffer: Arc<Mutex<XactBuffer>>,
    /// Followed databases: descriptor log, shadow catalog, routing map and
    /// rules per database
    dbs: Arc<SourceDbs>,
    /// Keyed by database oid, parallel to [`SourceDbs::all`]
    scopes: HashMap<Oid, DbScope>,
    /// Speculative catalog state per in-flight xact, written by capture at
    /// command boundaries. Read at stash resolution, dropped once the
    /// commit's drain has consumed it
    pending: Arc<PendingCatalog>,
    subxact_tracker: Arc<Mutex<SubxactTracker>>,
    ack: AckHandle,
    /// Shared FIFO channel to the batcher; `FlushAll` here orders after
    /// enqueued rows.
    msg_tx: mpsc::Sender<BatcherMsg>,
    fatal: Fatal,
    /// Reorder owns the commit-order boundary, so bumps `xacts_committed`
    /// (per commit) and `truncates_emitted`.
    stats: Arc<EmitterStats>,
    /// TOAST chunk resolver: planning detoast, mirror puts, retires
    resolver: ToastResolver,
    /// Retires wait until persisted replay floor passes dropping commit;
    /// ledger persists queue so a stop inside the wait window can't leak
    /// the mirror (resume never replays the drop)
    retires: RetireLedger,
    /// Retain undecided backup rows until transaction outcomes arrive
    pending_rows: SharedPendingLedger,
    /// Control connection for the settle statements, opened on first hit
    pending_session: Option<StagingSession>,
    /// Live destination for that lazy connect
    dest: Arc<crate::config::DestEmitter>,
    /// Persisted resolved floor (aligned, archive-clamped) — the position
    /// a crash-now restart resumes from. Seeded at the resolved start,
    /// advanced only after each manifest persist.
    resume_floor: Arc<Monotone<Floor>>,
    /// Dense commit-order counter; one seq per dispatched data unit.
    next_seq: u64,
    /// Drain-slice budget: rows / bytes per [`DrainedBatch`] pulled from the
    /// buffer. Bounds resident decoded rows while a spilled xact streams
    /// back.
    batch_rows: usize,
    batch_bytes: usize,
    /// Global resident-payload pool; slice admission acquired here before
    /// dispatch, riding rows to insert ack. `None` = unmetered (tests)
    budget: Option<crate::budget::MemoryBudget>,
    /// Per-txn span map (shared with the pump + buffer). `Some` only when
    /// OTLP tracing is on; reorder parents `commit.drain`/`dispatch` under
    /// the `txn` and prunes the entry at commit (the buffer prunes at abort).
    span_registry: Option<TxnSpanRegistry>,
    /// Byte cap per transaction plan spool file
    plan_disk_max: u64,
    /// Plan spool directory (xact scratch dir), cached at spawn so the
    /// per-commit path needs no buffer lock
    plan_dir: std::path::PathBuf,
    /// Frozen per transaction, with catalog events folded into local overlay
    route_mapping: Option<MappingSnapshot>,
    /// Resolved-config snapshot taken with the memo reset; every override in
    /// one interval comes from one snapshot, never a mid-interval republish.
    route_config: Option<Arc<ResolvedConfig>>,
}

impl ReorderSink {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        buffer: Arc<Mutex<XactBuffer>>,
        dbs: Arc<SourceDbs>,
        pending: Arc<PendingCatalog>,
        subxact_tracker: Arc<Mutex<SubxactTracker>>,
        // One per database that applies CH DDL; absent leaves that
        // database's events observed only
        mut applicators: HashMap<Oid, DdlApplicator>,
        ack: AckHandle,
        msg_tx: mpsc::Sender<BatcherMsg>,
        stats: Arc<EmitterStats>,
        resolver: ToastResolver,
        mut backfillers: HashMap<Oid, Arc<dyn crate::backfill::opt_in::Backfiller>>,
        fatal: Fatal,
        span_registry: Option<TxnSpanRegistry>,
        batch_rows: usize,
        batch_bytes: usize,
        plan_disk_max: u64,
        plan_dir: std::path::PathBuf,
        budget: Option<crate::budget::MemoryBudget>,
        retires: RetireLedger,
        pending_rows: SharedPendingLedger,
        dest: Arc<crate::config::DestEmitter>,
        resume_floor: Arc<Monotone<Floor>>,
    ) -> Self {
        // subscribe() marks the current value seen, so a `ctl reload`
        // racing pipeline spawn would stay invisible to has_changed —
        // and seeding the applied set from that raced value would record
        // its opt-ins as done without ever applying them. Start empty and
        // force the first commit to diff from scratch: re-applying
        // boot-seeded opt-ins is the designed-idempotent restart path
        // (CH table persists, backfill ledger dedups)
        let scopes = dbs
            .all()
            .iter()
            .map(|db| {
                let reload_rx = db.resolver.as_ref().map(|r| {
                    let mut rx = r.subscribe();
                    rx.mark_changed();
                    rx
                });
                let scope = DbScope {
                    db: db.clone(),
                    applicator: applicators.remove(&db.oid),
                    backfiller: backfillers.remove(&db.oid),
                    reload_rx,
                    applied_opt_ins: HashSet::new(),
                    pending_opt_ins: HashMap::new(),
                };
                (db.oid, scope)
            })
            .collect();
        Self {
            buffer,
            dbs,
            scopes,
            pending,
            subxact_tracker,
            ack,
            msg_tx,
            stats,
            resolver,
            fatal,
            next_seq: 0,
            batch_rows,
            batch_bytes,
            plan_disk_max,
            plan_dir,
            budget,
            span_registry,
            retires,
            pending_rows,
            pending_session: None,
            dest,
            resume_floor,
            route_mapping: None,
            route_config: None,
        }
    }

    /// Route-point steps 1–2 close out here: preceding schema/config state is
    /// applied, so drop memoized routes and freeze mapping + resolved-config
    /// versions for the next interval. Per commit this is the
    /// whole-transaction snapshot; a non-WAL-positioned republish landing
    /// mid-plan can't reroute rows already planned or split the transaction.
    async fn reset_route_state(&mut self, db: Oid) {
        // Read the handles out before the await: a borrow of the scope held
        // across one would put the applicator's `!Sync` client in the sink
        // future
        let scoped = self.scopes.get(&db).map(|scope| {
            let config = scope.reload_rx.as_ref().map(|rx| rx.borrow().clone());
            (scope.db.mapping.clone(), config)
        });
        let Some((mapping, config)) = scoped else {
            self.route_mapping = None;
            self.route_config = None;
            return;
        };
        self.route_mapping = Some(mapping.snapshot().await);
        self.route_config = config;
    }

    /// Apply a live config reload's table opt-in/opt-out diff at a commit
    /// barrier (`opt_in_lsn = commit_lsn`). Base config (mappings/budgets/CH
    /// connection) already republished onto the watch; here we do the part that
    /// needs the applicator/catalog — create/drop the CH scope.
    async fn maybe_apply_reload(&mut self, commit_lsn: u64) -> Result<(), SinkError> {
        // A reload republishes every database's config, so the diff is not
        // scoped to whichever database is committing
        let dbs: Vec<Oid> = self.dbs.oids().collect();
        for db in dbs {
            self.apply_reload_for(db, commit_lsn).await?;
        }
        Ok(())
    }

    async fn apply_reload_for(&mut self, db: Oid, commit_lsn: u64) -> Result<(), SinkError> {
        let Some(scope) = self.scopes.get_mut(&db) else {
            return Ok(());
        };
        let Some(resolver) = scope.db.resolver.clone() else {
            return Ok(());
        };
        // On a republish, re-diff `table_opt_ins`: opt-outs drain now, new
        // opt-ins queue as pending, dropped intents leave the queue.
        let changed = scope
            .reload_rx
            .as_mut()
            .is_some_and(|rx| rx.has_changed().unwrap_or(false));
        if changed {
            let desired: Vec<(RelName, TableRow)> = {
                let log = scope.db.desc_log.clone();
                let applied = &scope.applied_opt_ins;
                let config_schema = scope
                    .applicator
                    .as_ref()
                    .and_then(|a| a.config().runtime_config_schema.clone());
                let rx = scope.reload_rx.as_mut().unwrap();
                let snap = rx.borrow_and_update();
                // Keep prior pattern opt-ins in desired set
                let scoped = snap.rules.pattern_scoped(
                    || log.user_rel_names_at(commit_lsn, config_schema.as_deref()),
                    |rel| snap.tables.contains_key(rel) && !applied.contains(rel),
                );
                snap.table_opt_ins
                    .iter()
                    .map(|(rel, row)| (rel.clone(), row.clone()))
                    .chain(scoped)
                    .collect()
            };
            let desired_in: HashSet<RelName> = desired
                .iter()
                .filter(|(_, row)| row.replicate == Some(true))
                .map(|(rel, _)| rel.clone())
                .collect();
            let stale: Vec<RelName> = scope
                .applied_opt_ins
                .iter()
                .filter(|rel| !desired_in.contains(*rel))
                .cloned()
                .collect();
            for rel in stale {
                resolver.exclude_table(&rel).await;
                if let Some(b) = &scope.backfiller {
                    b.note_opt_out(&rel).await;
                }
                scope.applied_opt_ins.remove(&rel);
            }
            scope
                .pending_opt_ins
                .retain(|rel, _| desired_in.contains(rel));
            for (rel, row) in desired {
                if row.replicate == Some(true) && !scope.applied_opt_ins.contains(&rel) {
                    scope.pending_opt_ins.insert(rel, row);
                }
            }
            tracing::info!(
                target: "walshadow::config",
                pending = scope.pending_opt_ins.len(),
                applied = scope.applied_opt_ins.len(),
                "reload diff applied",
            );
        }
        // Each commit, apply any pending opt-in the shadow catalog can now
        // resolve — a table created just before `select` races the CREATE's
        // replay, so retry until the descriptor lands, then create + backfill.
        if scope.pending_opt_ins.is_empty() {
            return Ok(());
        }
        // No applicator (bootstrap drain / tests without DDL) → can't create
        // CH tables, so opt-ins stay pending.
        let Some(applicator) = scope.applicator.as_mut() else {
            return Ok(());
        };
        let candidates: Vec<(RelName, TableRow)> = scope
            .pending_opt_ins
            .iter()
            .map(|(rel, row)| (rel.clone(), row.clone()))
            .collect();
        for (rel, row) in candidates {
            let known = scope
                .db
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
                &scope.db.catalog,
                scope.backfiller.as_ref(),
                &rel,
                &row,
                commit_lsn,
            )
            .await
            .map_err(|e| SinkError::Other(format!("reload opt-in: {e}")))?;
            scope.pending_opt_ins.remove(&rel);
            scope.applied_opt_ins.insert(rel);
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
        db: Oid,
        event: &ConfigEvent,
        commit_lsn: u64,
    ) -> Result<(), SinkError> {
        let Some(scope) = self.scopes.get_mut(&db) else {
            return Ok(());
        };
        let Some(resolver) = scope.db.resolver.clone() else {
            return Ok(());
        };
        // Overlay merge first (target overrides, global/namespace knobs).
        resolver.apply_config_event(event.clone()).await;
        // Then inclusion dispatch for table rows: create the CH table +
        // register / drop the descriptor-derived mapping. `commit_lsn` is the
        // backfill boundary `S` for an `initial_load` opt-in.
        match event {
            ConfigEvent::TableUpserted { rel, row } if !row.is_pattern() => {
                if let Some(applicator) = scope.applicator.as_mut() {
                    crate::backfill::opt_in::apply_table_opt_in(
                        &resolver,
                        applicator,
                        &scope.db.catalog,
                        scope.backfiller.as_ref(),
                        rel,
                        row,
                        commit_lsn,
                    )
                    .await
                    .map_err(|e| SinkError::Other(format!("opt-in: {e}")))?;
                }
            }
            ConfigEvent::TableRemoved {
                rel,
                pattern: false,
            } => {
                resolver.exclude_table(rel).await;
                if let Some(b) = &scope.backfiller {
                    b.note_opt_out(rel).await;
                }
            }
            _ => {}
        }
        Ok(())
    }

    /// Seal every batcher table and wait for the reply. Sent on the shared row
    /// channel so it orders after every row enqueued before it.
    async fn flush_all_batcher(&mut self) -> Result<(), SinkError> {
        let (tx, rx) = oneshot::channel();
        if self.msg_tx.send(BatcherMsg::FlushAll(tx)).await.is_err() {
            return Err(SinkError::Other("batcher channel closed".into()));
        }
        tokio::select! {
            biased;
            _ = self.fatal.wait() => Err(self.fatal_err()),
            r = rx => r.map_err(|_| SinkError::Other("batcher dropped flush ack".into())),
        }
    }

    // Barrier waits prefer concurrent fatal over successful completion
    /// Block until every seq `< self.next_seq` is durable on CH, or a fatal
    /// trips (e.g. CH down past the inserter retry budget).
    async fn wait_all_durable(&mut self) -> Result<(), SinkError> {
        let through = self.next_seq;
        tokio::select! {
            biased;
            _ = self.fatal.wait() => Err(self.fatal_err()),
            r = self.ack.wait_through(through) => r.map_err(|e| {
                SinkError::Other(format!("durability barrier through {through}: {e}"))
            }),
        }
    }

    /// Fence before applying a DDL event / TRUNCATE so it orders strictly
    /// after all earlier data: seal batcher, wait durable. Rows place inline
    /// before dispatch returns, so `FlushAll` orders after all of them
    async fn barrier_fence(&mut self) -> Result<(), SinkError> {
        self.flush_all_batcher().await?;
        self.wait_all_durable().await
    }

    async fn apply_event(
        &mut self,
        db: Oid,
        event: &SchemaEvent,
        commit_lsn: u64,
    ) -> Result<(), SinkError> {
        let Some(scope) = self.scopes.get_mut(&db) else {
            return Ok(());
        };
        let resolver_cfg = scope.db.resolver.clone();
        let mapping = scope.db.mapping.clone();
        let Some(applicator) = scope.applicator.as_mut() else {
            return Ok(());
        };
        // Same frozen config planning predicted with, so planned routes match
        let frozen = self.route_config.as_deref();
        // Pinned mappings outlive DROP, so only Added/Changed must match
        let predicted = if cfg!(debug_assertions) && !matches!(event, SchemaEvent::Dropped { .. }) {
            let before = mapping.snapshot().await;
            applicator
                .predict_route_mapping(event, &before, frozen)
                .await
                .map_err(|e| SinkError::Other(format!("ddl predict: {e}")))?
        } else {
            None
        };
        applicator
            .apply_under(event, frozen)
            .await
            .map_err(|e| SinkError::Other(format!("ddl apply: {e}")))?;
        if let Some((rel, m)) = predicted {
            let after = mapping.snapshot().await;
            debug_assert_eq!(
                after.get(&rel),
                m.as_ref(),
                "{rel}: apply diverged from plan"
            );
        }
        // A `CREATE TABLE` for a forward-declared opt-in materialises here, in
        // the same barrier before this xact's trailing rows dispatch.
        if let SchemaEvent::Added { desc } = event
            && let Some(resolver) = resolver_cfg
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
                .push(*oid, Pos::new(commit_lsn))
                .await
                .map_err(|e| SinkError::Other(format!("toast retire ledger: {e}")))?;
        }
        Ok(())
    }

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
            let db = desc.rfn.db_node;
            self.apply_event(db, &SchemaEvent::Added { desc }, resume_lsn)
                .await?;
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
    pub async fn flush_due_retires(&mut self) -> Result<(), SinkError> {
        // Disabled resolver no-ops retire_mirror: flushing would drop ledger
        // entries without wiping mirrors, leaking them for a later CH run
        // over the same spill dir
        if !self.resolver.stores_chunks() || self.retires.is_empty() {
            return Ok(());
        }
        let cut = self.resume_floor.get();
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

    /// Settle pending rows on commit or abort, including subtransactions
    async fn note_pending(
        &mut self,
        xid: u32,
        subxacts: &[u32],
        committed: bool,
    ) -> Result<(), SinkError> {
        let settled = self
            .pending_rows
            .lock()
            .await
            .note(xid, subxacts, committed);
        if settled == 0 {
            return Ok(());
        }
        self.stats
            .pending_xacts_settled
            .fetch_add(settled, Ordering::Relaxed);
        self.settle_pending().await
    }

    async fn settle_pending(&mut self) -> Result<(), SinkError> {
        let ledger = self.pending_rows.clone();
        let mut ledger = ledger.lock().await;
        if ledger.is_empty() {
            return Ok(());
        }
        if self.pending_session.is_none() {
            self.pending_session = Some(
                StagingSession::connect(self.dest.clone())
                    .await
                    .map_err(|e| SinkError::Other(format!("pending visibility: connect: {e}")))?,
            );
        }
        let sess = self.pending_session.as_mut().expect("just connected");
        visibility_pending::settle(&mut ledger, sess, &self.stats)
            .await
            .map_err(SinkError::Other)
    }

    /// Recover outcomes from shadow transaction logs before resumed WAL
    pub async fn settle_pending_boot(
        &mut self,
        shadow_data_dir: Option<&std::path::Path>,
    ) -> Result<(), SinkError> {
        if self.pending_rows.lock().await.is_empty() {
            return Ok(());
        }
        if let Some(dir) = shadow_data_dir.filter(|d| d.join("pg_xact").is_dir()) {
            let accum = read_pg_xact(dir).await.map_err(|e| {
                SinkError::Other(format!("pending visibility: shadow pg_xact: {e}"))
            })?;
            let patch = PgXactPatch::new();
            let fold = self
                .pending_rows
                .lock()
                .await
                .note_view(&PgXactView::new(&accum, &patch));
            self.stats
                .pending_xacts_settled
                .fetch_add(fold.settled, Ordering::Relaxed);
            self.stats
                .pending_undecidable_xids
                .store(fold.undecidable, Ordering::Relaxed);
            if fold.undecidable > 0 {
                tracing::warn!(
                    target: "walshadow::visibility_pending",
                    xids = fold.undecidable,
                    "shadow pg_xact lacks deciding xids; retain pending rows and tables",
                );
            }
        }
        self.settle_pending().await
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
        db: Oid,
        entry: &DrainEntry,
        commit_lsn: u64,
    ) -> Result<(), SinkError> {
        match entry {
            DrainEntry::Catalog(ev) => self.apply_event(db, ev, commit_lsn).await,
            DrainEntry::Config(ev) => self.apply_config(db, ev, commit_lsn).await,
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
        let Some(scope) = self.scopes.get_mut(&heap.descriptor.rfn.db_node) else {
            return Ok(());
        };
        let Some(applicator) = scope.applicator.as_mut() else {
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

    /// Place accumulated planned heaps as one seq under a fresh admission
    /// permit; values detoasted at planning. `publish` marks the commit's
    /// final data segment so its seq carries the LSN publication (no
    /// trailing marker needed)
    ///
    /// Takes `&mut self` so the borrow across awaits is `&mut Self` (Send):
    /// owned `DdlApplicator` is Send but not Sync
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
        let permit = tokio::select! {
            biased;
            _ = self.fatal.wait() => return Err(self.fatal_err()),
            p = crate::budget::admit_opt(self.budget.as_ref(), bytes) => p.map(Arc::new),
        };
        let seq = self.alloc_seq();
        if publish {
            self.ack.register(seq, commit_lsn);
        } else {
            self.ack.register_partial(seq, commit_lsn);
        }
        self.stats.queue_jobs_out.fetch_add(1, Ordering::Relaxed);
        let chunk_rows = self.dest.current().decode_chunk_rows;
        let rows = tokio::select! {
            biased;
            _ = self.fatal.wait() => return Err(self.fatal_err()),
            r = decode::place_rows(
                &self.msg_tx,
                &self.stats,
                chunk_rows,
                seq,
                commit_ts,
                commit_lsn,
                heaps,
                permit,
            ) => r.map_err(SinkError::Other)?,
        };
        self.ack.placed(seq, rows);
        Ok(())
    }

    /// Replay one sealed plan through the existing barrier ordering. Routes
    /// ride the plan — nothing re-resolves. Heap segments slice at the
    /// batch budget; controls fence then apply their real side effects at
    /// their pinned positions; mirror rows put just in time; truncates
    /// fence per their carried cursor. The final data segment publishes the
    /// commit LSN when nothing follows it; otherwise the caller's trailing
    /// rows=0 marker does. Returns dispatched rows + whether it published
    pub async fn execute_plan(
        &mut self,
        db: Oid,
        plan: &SealedPlan,
    ) -> Result<(u64, bool), SinkError> {
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
                    self.apply_drain_entry(db, &c.event, commit_lsn).await?;
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
        // One backend writes one database, so the whole drain routes through
        // that database's log, rules and applicator. `xl_xact_dbinfo` is
        // always logged at wal_level=logical; the primary is the fallback
        let db = payload.db_id.unwrap_or(self.dbs.primary().oid);
        self.note_pending(xid, &payload.subxacts, true).await?;
        // Parent for this commit's spans; held until on_commit returns so it
        // outlives the prune below. No-op span when tracing off/unsampled.
        let txn = self
            .span_registry
            .as_ref()
            .and_then(|r| r.txn_span(xid))
            .unwrap_or_else(tracing::Span::none);
        let stash_log = self
            .scopes
            .get(&db)
            .map(|scope| scope.db.desc_log.clone())
            .unwrap_or_else(|| self.dbs.primary().desc_log.clone());
        crate::xact::xact_buffer::resolve_stash(
            &self.buffer,
            &stash_log,
            &self.pending,
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
        // Timeline outlived its use: resolution above already folded it
        // into the outcomes this drain reads
        self.pending.forget_tree(xid);
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
        self.reset_route_state(db).await;
        let mut rows_total: u64 = 0;
        let mut published = false;
        if drain.had_states {
            let plan_path = self.plan_dir.join(format!("xact-{xid}-{commit_lsn}.plan"));
            let plan = {
                let scope = self.scopes.get_mut(&db);
                let row_policy = scope
                    .as_ref()
                    .map(|scope| scope.db.row_policy())
                    .unwrap_or_default();
                let mut view = ReorderRouteView::new(
                    self.route_mapping.clone(),
                    self.route_config.clone(),
                    row_policy,
                    scope.and_then(|scope| scope.applicator.as_mut()),
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
                .execute_plan(db, &plan)
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
        self.note_pending(xid, &payload.subxacts, false).await?;
        let seq = self.alloc_seq();
        self.ack.register(seq, record.source_lsn);
        {
            let mut buf = self.buffer.lock().await;
            buf.abort(xid, Pos::new(record.source_lsn), &payload.subxacts)
                .await
                .map_err(SinkError::from)?;
        }
        self.ack.placed(seq, 0);
        self.subxact_tracker.lock().await.forget_tree(xid);
        self.pending.forget_tree(xid);
        Ok(())
    }
}

/// Route a plan-failure reason label onto its counter
fn bump_plan_failure(stats: &EmitterStats, reason: &'static str) {
    let counter = match reason {
        "spool" => &stats.plan_failures_spool,
        "fail_closed_image_only" => &stats.plan_failures_fail_closed_image_only,
        "fail_closed_malformed" => &stats.plan_failures_fail_closed_malformed,
        "fail_closed_unsupported_op" => &stats.plan_failures_fail_closed_unsupported_op,
        "stash_ambiguous" => &stats.plan_failures_stash_ambiguous,
        "incomplete_toast" => &stats.plan_failures_incomplete_toast,
        "missing_stash_resolution" => &stats.plan_failures_missing_stash_resolution,
        "detoast" => &stats.plan_failures_detoast,
        "partial_update" => &stats.plan_failures_partial_update,
        "view" => &stats.plan_failures_view,
        _ => &stats.plan_failures_drain,
    };
    counter.fetch_add(1, Ordering::Relaxed);
}

/// Frozen route state plus transaction-local catalog changes
///
/// Predict route effects before executor applies matching DDL
/// Config-table events are not folded: a same-xact config write followed
/// by rows plans under the frozen version (whole-transaction granularity;
/// the in-xact interval refinement lands with the config fold)
pub struct ReorderRouteView<'a> {
    mapping: Option<MappingSnapshot>,
    config: Option<Arc<ResolvedConfig>>,
    /// Catalog fold above `mapping`; `None` value = locally dropped
    overlay: HashMap<RelName, Option<TableMapping>>,
    memo: HashMap<RelName, Option<Arc<RouteSnapshot>>>,
    row_policy: RowPolicy,
    applicator: Option<&'a mut DdlApplicator>,
    stats: Arc<EmitterStats>,
}

impl<'a> ReorderRouteView<'a> {
    pub fn new(
        mapping: Option<MappingSnapshot>,
        config: Option<Arc<ResolvedConfig>>,
        row_policy: RowPolicy,
        applicator: Option<&'a mut DdlApplicator>,
        stats: Arc<EmitterStats>,
    ) -> Self {
        Self {
            mapping,
            config,
            overlay: HashMap::new(),
            memo: HashMap::new(),
            row_policy,
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
            let rules = self
                .config
                .as_ref()
                .map_or_else(Arc::default, |rc| rc.column_rules.clone());
            let policy = self.row_policy.for_rel(self.config.as_deref(), rel_name);
            RouteSnapshot::freeze(Arc::new(m), rules, policy)
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
        let mapping = self.mapping.clone().unwrap_or_default();
        let config = self.config.clone();
        let Some(app) = self.applicator.as_deref_mut() else {
            return Ok(());
        };
        if let Some((rel, m)) = app
            .predict_route_mapping(ev, &mapping, config.as_deref())
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
            {
                let mut buf = self.buffer.lock().await;
                if buf.stats().xacts_active != 0 {
                    return Ok(());
                }
                self.ack.trailing(lsn);
                buf.advance_idle(Pos::new(lsn));
            }
            // Quiescent source never re-enters on_commit; retire due drops
            // here so the flush doesn't wait for a later commit
            self.flush_due_retires().await
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::decode::heap_decoder::DecodedHeap;
    use crate::mapping::TableTarget;
    use crate::xact::xact_buffer::raw_fixtures::int4_descriptor;

    fn mapping_for(rel: &RelName) -> TableMapping {
        TableMapping {
            target: TableTarget::new("db", &rel.name),
            columns: Vec::new(),
        }
    }

    fn heap_of(rel: &RelName) -> DescribedHeap {
        let mut desc = (*int4_descriptor(16400)).clone();
        desc.rel_name = rel.clone();
        DescribedHeap {
            decoded: DecodedHeap {
                rfn: desc.rfn,
                xid: 1,
                source_lsn: 0x100,
                op: HeapOp::Insert,
                new: None,
                old: None,
            },
            descriptor: Arc::new(desc),
            descriptor_valid_from: 0x40,
        }
    }

    #[tokio::test]
    async fn plan_view_holds_one_mapping_version_across_a_republish() {
        let planned = RelName::new("public", "planned");
        let added = RelName::new("public", "added");
        let handle = crate::mapping::mapping_handle(HashMap::from_iter([(
            planned.clone(),
            mapping_for(&planned),
        )]));

        let stats = Arc::new(EmitterStats::default());
        let mut view = ReorderRouteView::new(
            Some(handle.snapshot().await),
            None,
            RowPolicy::default(),
            None,
            stats,
        );
        assert!(view.route_for(&heap_of(&planned)).is_some());
        assert!(view.route_for(&heap_of(&added)).is_none());

        handle
            .publish(Arc::new(HashMap::from_iter([(
                added.clone(),
                mapping_for(&added),
            )])))
            .await;

        assert!(
            view.route_for(&heap_of(&planned)).is_some(),
            "republish can't unroute rows already planned"
        );
        assert!(
            view.route_for(&heap_of(&added)).is_none(),
            "republish can't route rows this transaction planned unmapped"
        );
    }
}
