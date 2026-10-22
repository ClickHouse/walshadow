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
#[cfg(feature = "test-support")]
use crate::crash_test;
use crate::decode::heap_decoder::{DecodedHeap, DescribedHeap, Predecoded};
use crate::decode::visibility::{PgXactView, SealedPatch, read_pg_xact};
use crate::emit::ch_ddl::{DdlApplicator, Stamp};
use crate::emit::ch_emitter::EmitterStats;
use crate::record::{Record, RecordSink, SinkError};
use crate::schema::{RelDescriptor, RelName, SchemaEvent, TableKey};
use ahash::{HashMap, HashMapExt, HashSet, HashSetExt};
use tracing::Instrument;

use crate::decode::wal_xact::{
    XLOG_XACT_ABORT, XLOG_XACT_ABORT_PREPARED, XLOG_XACT_ASSIGNMENT, XLOG_XACT_COMMIT,
    XLOG_XACT_COMMIT_PREPARED, XLOG_XACT_OPMASK, parse_xact_assignment, parse_xact_payload,
};
use crate::ops::trace::TxnSpanRegistry;
use crate::xact::xact_buffer::{DrainEntry, SubxactTracker, XactBuffer, XactBufferError};

use crate::config::ResolvedConfig;
use crate::emit::pipeline::Fatal;
use crate::emit::pipeline::ack::{AckHandle, Publish, SeqAlloc};
use crate::emit::pipeline::batcher::{BatcherMsg, RowChunk};
use crate::emit::pipeline::decode;
use crate::emit::pipeline::plan_spool::{
    PlanItem, PlanPath, PlanTable, PlannedHeap, SealedPlan, VerifiedPlan,
};
use crate::emit::pipeline::planner::{PlanRouteView, Planner, drain_reason};
use crate::emit::route::{RouteSnapshot, RowPolicy, route_via};
use crate::mapping::{MappingSnapshot, TableMapping};
use crate::pos::{Floor, Monotone, Pos};
use crate::runtime_config::{ConfigEvent, TableRow};
use crate::source_db::{SourceDb, SourceDbs};
use crate::toast::ToastResolver;
use crate::toast::toast_retire::{RetireEntry, RetireLedger};
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

impl DbScope {
    /// Republished config to diff or opt-ins to retry; mirrors the early
    /// returns of [`ReorderSink::apply_reload_for`]
    fn reload_due(&self) -> bool {
        self.db.resolver.is_some()
            && (!self.pending_opt_ins.is_empty()
                || self
                    .reload_rx
                    .as_ref()
                    .is_some_and(|rx| rx.has_changed().unwrap_or(false)))
    }
}

/// Rare commit-path steps (reloads, controls, truncates, settles, spill
/// cleanup) run under `Box::pin`: kilobytes deep, inline they would bloat
/// every commit future
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
    /// Batcher channel with rows, seq events and admission held for it
    out: Outbox,
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
    /// Drain-slice budget: rows / bytes per [`DrainedBatch`] pulled from the
    /// buffer. Bounds resident decoded rows while a spilled xact streams
    /// back.
    batch_rows: usize,
    batch_bytes: usize,
    /// Per-txn span map (shared with the pump + buffer). `Some` only when
    /// OTLP tracing is on; reorder parents `commit.drain`/`dispatch` under
    /// the `txn` and prunes the entry at commit (the buffer prunes at abort).
    span_registry: Option<TxnSpanRegistry>,
    /// Byte cap per transaction plan spool file
    plan_disk_max: u64,
    /// Plan spool directory (xact scratch dir), cached at spawn so the
    /// per-commit path needs no buffer lock
    plan_dir: Arc<std::path::Path>,
    /// Frozen per transaction, with catalog events folded into local overlay
    route_mapping: Option<MappingSnapshot>,
    route_cache: Option<RouteCache>,
    /// Resolved-config snapshot taken with the memo reset; every override in
    /// one interval comes from one snapshot, never a mid-interval republish.
    route_config: Option<Arc<ResolvedConfig>>,
}

/// Split from [`ReorderSink`] so planning can ship and admit while the
/// planner borrows the scopes
struct Outbox {
    /// Shared FIFO channel to the batcher; `FlushAll` here orders after
    /// enqueued rows.
    msg_tx: mpsc::Sender<BatcherMsg>,
    fatal: Fatal,
    /// One seq per dispatched data unit, in commit order. Held: events ship
    /// with [`Self::staged`]
    seqs: SeqAlloc,
    /// Placed rows not yet on the batcher channel, shipped per worker batch
    /// so small commits share one channel hop and wakeup
    staged: Vec<RowChunk>,
    staged_rows: usize,
    staged_bytes: usize,
    /// Global resident-payload pool; slice and chunk generation admission
    /// acquired here, riding rows to insert ack. `None` = unmetered (tests)
    budget: Option<crate::budget::MemoryBudget>,
}

impl Outbox {
    fn fatal_err(&self) -> SinkError {
        SinkError::Other(
            self.fatal
                .message()
                .unwrap_or_else(|| "pipeline fatal".into()),
        )
    }

    /// Admission permit for `bytes`. Staged rows hold permits only shipping
    /// releases, so ship before waiting on the pool
    async fn admit(
        &mut self,
        bytes: usize,
    ) -> Result<Option<crate::budget::MemoryPermit>, SinkError> {
        let Some(budget) = self.budget.clone() else {
            return Ok(None);
        };
        if let Some(p) = budget.try_admit(bytes) {
            return Ok(Some(p));
        }
        self.ship().await?;
        tokio::select! {
            biased;
            _ = self.fatal.wait() => Err(self.fatal_err()),
            p = budget.admit(bytes) => Ok(Some(p)),
        }
    }

    /// Send staged rows with their held seq events: registrations first so
    /// no insert ack beats them, placements once rows are on the channel
    async fn ship(&mut self) -> Result<(), SinkError> {
        self.seqs.send_registered();
        if !self.staged.is_empty() {
            let chunks = std::mem::take(&mut self.staged);
            self.staged_rows = 0;
            self.staged_bytes = 0;
            let sent = tokio::select! {
                biased;
                _ = self.fatal.wait() => return Err(self.fatal_err()),
                r = self.msg_tx.send(BatcherMsg::Chunks(chunks)) => r,
            };
            if sent.is_err() {
                return Err(SinkError::Other("batcher channel closed".into()));
            }
        }
        self.seqs.send_placed();
        Ok(())
    }
}

/// Every earlier row is durable on CH, so a DDL, TRUNCATE or config apply
/// orders strictly after the data before it
struct Fenced(());

impl Fenced {
    /// Boot runs before the pump dispatches any row
    const fn pre_pump() -> Self {
        Self(())
    }
}

impl ReorderSink {
    /// Sink only usable after [`BootingReorder::boot`]
    #[allow(clippy::too_many_arguments)]
    pub(super) fn booting(
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
        plan_dir: Arc<std::path::Path>,
        budget: Option<crate::budget::MemoryBudget>,
        retires: RetireLedger,
        pending_rows: SharedPendingLedger,
        dest: Arc<crate::config::DestEmitter>,
        resume_floor: Arc<Monotone<Floor>>,
    ) -> BootingReorder {
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
        let seqs = ack.seqs(0).held();
        BootingReorder(Self {
            buffer,
            dbs,
            scopes,
            pending,
            subxact_tracker,
            ack,
            out: Outbox {
                msg_tx,
                fatal,
                seqs,
                staged: Vec::new(),
                staged_rows: 0,
                staged_bytes: 0,
                budget,
            },
            stats,
            resolver,
            batch_rows,
            batch_bytes,
            plan_disk_max,
            plan_dir,
            span_registry,
            retires,
            pending_rows,
            pending_session: None,
            dest,
            resume_floor,
            route_mapping: None,
            route_cache: None,
            route_config: None,
        })
    }

    /// Resolver shared with predecode for TOAST read-ahead
    pub fn planning_resolver(&self) -> ToastResolver {
        self.resolver.clone()
    }

    /// Route-point steps 1–2 close out here: preceding schema/config state is
    /// applied, so drop memoized routes and freeze mapping + resolved-config
    /// versions for the next interval. Per commit this is the
    /// whole-transaction snapshot; a non-WAL-positioned republish landing
    /// mid-plan can't reroute rows already planned or split the transaction.
    fn reset_route_state(&mut self, db: Oid) {
        let Some(scope) = self.scopes.get(&db) else {
            self.route_mapping = None;
            self.route_config = None;
            return;
        };
        self.route_mapping = Some(scope.db.mapping.snapshot());
        self.route_config = scope.reload_rx.as_ref().map(|rx| rx.borrow().clone());
    }

    /// Apply a live config reload's table opt-in/opt-out diff at a commit
    /// barrier (`opt_in_lsn = commit_lsn`). Base config (mappings/budgets/CH
    /// connection) already republished onto the watch; here we do the part that
    /// needs the applicator/catalog — create/drop the CH scope.
    async fn maybe_apply_reload(&mut self, commit_lsn: u64) -> Result<(), SinkError> {
        // A reload republishes every database's config, so the diff is not
        // scoped to whichever database is committing
        let dbs: Vec<Oid> = self
            .dbs
            .oids()
            .filter(|db| self.scopes.get(db).is_some_and(DbScope::reload_due))
            .collect();
        for db in dbs {
            Box::pin(self.apply_reload_for(db, commit_lsn)).await?;
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
        _: Fenced,
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
        self.out.ship().await?;
        let (tx, rx) = oneshot::channel();
        if self
            .out
            .msg_tx
            .send(BatcherMsg::FlushAll(tx))
            .await
            .is_err()
        {
            return Err(SinkError::Other("batcher channel closed".into()));
        }
        tokio::select! {
            biased;
            _ = self.out.fatal.wait() => Err(self.out.fatal_err()),
            r = rx => r.map_err(|_| SinkError::Other("batcher dropped flush ack".into())),
        }
    }

    // Barrier waits prefer concurrent fatal over successful completion
    /// Block until every opened seq is durable on CH, or a fatal trips (e.g.
    /// CH down past the inserter retry budget).
    async fn wait_all_durable(&mut self) -> Result<(), SinkError> {
        self.out.ship().await?;
        let through = self.out.seqs.next();
        tokio::select! {
            biased;
            _ = self.out.fatal.wait() => Err(self.out.fatal_err()),
            r = self.ack.wait_through(through) => r.map_err(|e| {
                SinkError::Other(format!("durability barrier through {through}: {e}"))
            }),
        }
    }

    /// Fence before applying a DDL event / TRUNCATE so it orders strictly
    /// after all earlier data: seal batcher, wait durable. Rows place inline
    /// before dispatch returns, so `FlushAll` orders after all of them
    async fn barrier_fence(&mut self) -> Result<Fenced, SinkError> {
        self.flush_all_batcher().await?;
        self.wait_all_durable().await?;
        Ok(Fenced(()))
    }

    async fn apply_event(
        &mut self,
        _: Fenced,
        db: Oid,
        event: &SchemaEvent,
        at: Stamp,
    ) -> Result<(), SinkError> {
        let commit_lsn = at.lsn;
        let Some(scope) = self.scopes.get_mut(&db) else {
            return Ok(());
        };
        let resolver_cfg = scope.db.resolver.clone();
        let mapping = scope.db.mapping.clone();
        let backfiller = scope.backfiller.clone();
        let Some(applicator) = scope.applicator.as_mut() else {
            return Ok(());
        };
        // Same frozen config planning predicted with, so planned routes match
        let frozen = self.route_config.as_deref();
        // Pinned mappings outlive DROP, so only Added/Changed must match
        let predicted = if cfg!(debug_assertions) && !matches!(event, SchemaEvent::Dropped { .. }) {
            let before = mapping.snapshot();
            applicator
                .predict_route_mapping(event, &before, frozen)
                .await
                .map_err(|e| SinkError::Other(format!("ddl predict: {e}")))?
        } else {
            None
        };
        #[cfg(feature = "test-support")]
        crash_test::hit(crash_test::Point::BeforeDdl, commit_lsn);
        applicator
            .apply_at(event, frozen, at)
            .await
            .map_err(|e| SinkError::Other(format!("ddl apply: {e}")))?;
        #[cfg(feature = "test-support")]
        crash_test::hit(crash_test::Point::AfterDdl, commit_lsn);
        if let Some((rel, m)) = predicted {
            let after = mapping.snapshot();
            debug_assert_eq!(
                after.get(&rel).map(|a| a.as_ref()),
                m.as_ref(),
                "{rel}: apply diverged from plan"
            );
        }
        // `ATTACH` brings rows that never streamed into the root's destination
        if let SchemaEvent::Changed { old, new, .. } = event
            && new.is_partition()
            && old.parent_oid() != new.parent_oid()
            && let Some(backfiller) = backfiller
        {
            let mode = applicator.partition_load(new);
            backfiller.note_opt_in(new.clone(), mode, commit_lsn).await;
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
        if let SchemaEvent::Dropped { oid, rel_name, .. } = event
            && &*rel_name.namespace == "pg_toast"
            && self.resolver.stores_chunks()
        {
            self.retires
                .push(RetireEntry {
                    db_oid: db,
                    toast_relid: *oid,
                    commit_lsn: Pos::new(commit_lsn),
                })
                .await
                .map_err(|e| SinkError::Other(format!("toast retire ledger: {e}")))?;
        }
        Ok(())
    }

    /// Boot Added pass: every relation `Present` in the descriptor log at
    /// resume gets an `Added` apply (idempotent `CREATE TABLE IF NOT
    /// EXISTS` + forward-declaration materialise). Runs pre-pump every
    /// boot, like [`Self::flush_due_retires`], brownfield auto-create
    /// tables exist at attach instead of first write, and newly enabled
    /// auto-create/mapping picks up existing rels without log mutation.
    async fn apply_boot_events(
        &mut self,
        mut descs: Vec<Arc<RelDescriptor>>,
        resume_lsn: u64,
    ) -> Result<(), SinkError> {
        // Validate partitions against root destinations after creating roots
        descs.sort_by_key(|d| d.ancestors.len());
        for desc in descs {
            if desc.kind == 't' {
                continue;
            }
            let db = desc.rfn.db_node;
            self.apply_event(
                Fenced::pre_pump(),
                db,
                &SchemaEvent::Added { desc },
                Stamp {
                    lsn: resume_lsn,
                    ..Stamp::default()
                },
            )
            .await?;
        }
        Ok(())
    }

    /// Retire below persisted resolved floor
    ///
    /// Restart resumes at the floor, DROP lock excludes later referrers.
    /// Boot runs it too: entries due at resume must retire during standup,
    /// their drop never replays, so no commit re-triggers this flush.
    /// Ledger removal persists after each wipe; a crash between re-runs
    /// an idempotent `TRUNCATE` on the emptied mirror
    async fn flush_due_retires(&mut self) -> Result<(), SinkError> {
        // Disabled resolver no-ops retire_mirror: flushing would drop ledger
        // entries without wiping mirrors, leaking them for a later CH run
        // over the same spill dir
        if !self.resolver.stores_chunks() || self.retires.is_empty() {
            return Ok(());
        }
        let cut = self.resume_floor.get();
        for entry in self.retires.due(cut) {
            self.resolver
                .retire_mirror(entry.db_oid, entry.toast_relid)
                .await
                .map_err(|e| SinkError::Other(format!("toast mirror retire: {e}")))?;
            self.retires
                .remove(entry)
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
        Box::pin(self.settle_pending()).await
    }

    async fn settle_pending(&mut self) -> Result<(), SinkError> {
        let ledger = self.pending_rows.clone();
        let mut ledger = ledger.lock().await;
        if ledger.is_empty() {
            return Ok(());
        }
        let sess = match &mut self.pending_session {
            Some(sess) => sess,
            None => self.pending_session.insert(
                StagingSession::connect(self.dest.clone())
                    .await
                    .map_err(|e| SinkError::Other(format!("pending visibility: connect: {e}")))?,
            ),
        };
        visibility_pending::settle(&mut ledger, sess, &self.stats)
            .await
            .map_err(SinkError::Other)
    }

    /// Recover outcomes from shadow transaction logs before resumed WAL
    async fn settle_pending_boot(
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
            let patch = SealedPatch::default();
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
        db: Oid,
        toast_relid: u32,
        marker_lsn: u64,
        commit_lsn: u64,
    ) -> Result<(), SinkError> {
        self.resolver
            .rewrite_barrier(db, toast_relid, marker_lsn, commit_lsn)
            .await
            .map_err(|e| SinkError::Other(format!("toast rewrite barrier: {e}")))
    }

    async fn apply_drain_entry(
        &mut self,
        fenced: Fenced,
        db: Oid,
        entry: &DrainEntry,
        at: Stamp,
    ) -> Result<(), SinkError> {
        let commit_lsn = at.lsn;
        match entry {
            DrainEntry::Catalog(ev) => self.apply_event(fenced, db, ev, at).await,
            DrainEntry::Config(ev) => self.apply_config(fenced, db, ev, commit_lsn).await,
            DrainEntry::ToastBarrier {
                toast_relid,
                marker_lsn,
            } => {
                self.apply_toast_barrier(db, *toast_relid, *marker_lsn, commit_lsn)
                    .await
            }
        }
    }

    async fn apply_truncate(
        &mut self,
        _: Fenced,
        rel: &RelDescriptor,
        heap: &DecodedHeap,
        commit_ts: i64,
    ) -> Result<(), SinkError> {
        let Some(scope) = self.scopes.get_mut(&rel.rfn.db_node) else {
            return Ok(());
        };
        let Some(applicator) = scope.applicator.as_mut() else {
            return Ok(());
        };
        // Attached at truncate fan-out (record time = pre-capture Present),
        // so the rotation's drain-time Retired answer no longer needs a
        // predecessor walk here
        let at = Stamp {
            lsn: heap.source_lsn,
            xid: heap.xid,
            commit_ts,
        };
        applicator
            .truncate(rel, at)
            .await
            .map_err(|e| SinkError::Other(format!("ch truncate: {e}")))?;
        self.stats.truncates_emitted.fetch_add(1, Ordering::Relaxed);
        // PG swaps TOAST relfilenode without listing it in `xl_heap_truncate`;
        // the descriptor carries the owner's toast oid
        if self.resolver.stores_chunks() && rel.toast_oid != 0 {
            self.resolver
                .truncate_mirror(rel.rfn.db_node, rel.toast_oid)
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
        tables: &[PlanTable],
        pending: &mut Vec<PlannedHeap>,
        pending_bytes: &mut usize,
        commit_ts: i64,
        commit_lsn: u64,
        publish: Publish,
    ) -> Result<(), SinkError> {
        if pending.is_empty() {
            return Ok(());
        }
        let heaps = std::mem::take(pending);
        let bytes = std::mem::take(pending_bytes);
        let permit = self.out.admit(bytes).await?.map(Arc::new);
        let seq = self.out.seqs.open(commit_lsn, publish);
        self.stats.queue_jobs_out.fetch_add(1, Ordering::Relaxed);
        let chunk_rows = self.dest.current().decode_chunk_rows;
        let rows = decode::place_rows(
            &mut self.out.staged,
            &self.stats,
            chunk_rows,
            seq.seq(),
            commit_ts,
            commit_lsn,
            tables,
            heaps,
            permit,
        );
        let out = &mut self.out;
        out.seqs.place(seq, rows);
        out.staged_rows += rows as usize;
        out.staged_bytes += bytes;
        if out.staged_rows >= chunk_rows || out.staged_bytes >= decode::DECODE_CHUNK_BYTES {
            out.ship().await?;
        }
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
        xid: u32,
        plan: &VerifiedPlan,
    ) -> Result<(u64, bool), SinkError> {
        let (commit_ts, commit_lsn) = (plan.commit_ts, plan.commit_lsn);
        let at = Stamp {
            lsn: commit_lsn,
            xid,
            commit_ts,
        };
        let mut rd = plan
            .replay()
            .map_err(|e| SinkError::Other(format!("plan replay: {e}")))?;
        let mut pending: Vec<PlannedHeap> = Vec::new();
        let mut pending_bytes = 0usize;
        let mut rows_cursor = 0usize;
        let total_rows: usize = plan.row_batches.iter().map(|rb| rb.len()).sum();
        let mut rows_total = 0u64;
        while let Some(item) = rd
            .next_item()
            .map_err(|e| SinkError::Other(format!("plan replay: {e}")))?
        {
            match item {
                PlanItem::Control(c) => {
                    Box::pin(async {
                        self.put_plan_rows(plan, &mut rows_cursor, c.row_idx)
                            .await?;
                        self.dispatch_planned(
                            &plan.tables,
                            &mut pending,
                            &mut pending_bytes,
                            commit_ts,
                            commit_lsn,
                            Publish::Partial,
                        )
                        .await?;
                        let fenced = self.barrier_fence().await?;
                        self.apply_drain_entry(fenced, db, &c.event, at).await
                    })
                    .await?;
                }
                PlanItem::Truncate { heap: h, upto } => {
                    Box::pin(async {
                        self.put_plan_rows(plan, &mut rows_cursor, upto).await?;
                        self.dispatch_planned(
                            &plan.tables,
                            &mut pending,
                            &mut pending_bytes,
                            commit_ts,
                            commit_lsn,
                            Publish::Partial,
                        )
                        .await?;
                        let fenced = self.barrier_fence().await?;
                        let rel = &plan.tables[h.table as usize].descriptor;
                        self.apply_truncate(fenced, rel, &h.decoded, commit_ts)
                            .await
                    })
                    .await?;
                }
                PlanItem::Heap(h) => {
                    rows_total += 1;
                    pending_bytes += h.bytes;
                    pending.push(h);
                    if pending.len() >= self.batch_rows || pending_bytes >= self.batch_bytes {
                        self.dispatch_planned(
                            &plan.tables,
                            &mut pending,
                            &mut pending_bytes,
                            commit_ts,
                            commit_lsn,
                            Publish::Partial,
                        )
                        .await?;
                    }
                }
            }
        }
        if rows_cursor < total_rows {
            Box::pin(self.put_plan_rows(plan, &mut rows_cursor, total_rows)).await?;
        }
        let published = !pending.is_empty();
        self.dispatch_planned(
            &plan.tables,
            &mut pending,
            &mut pending_bytes,
            commit_ts,
            commit_lsn,
            Publish::Commit,
        )
        .await?;
        Ok((rows_total, published))
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
        let stash = crate::xact::xact_buffer::resolve_stash(
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
                stash,
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
        self.reset_route_state(db);
        let mut rows_total: u64 = 0;
        let mut published = false;
        if drain.had_states() {
            let plan_path = PlanPath::Xact {
                dir: self.plan_dir.clone(),
                xid,
                commit_lsn,
            };
            let plan = {
                let scope = self.scopes.get_mut(&db);
                let row_policy = scope
                    .as_ref()
                    .map(|scope| scope.db.row_policy())
                    .unwrap_or_default();
                let maps = match self.route_cache.take() {
                    Some(c)
                        if c.matches(db, &self.route_mapping, &self.route_config, &row_policy) =>
                    {
                        c.maps
                    }
                    Some(mut c) => {
                        c.maps.clear();
                        c.maps
                    }
                    None => RouteMaps::default(),
                };
                let mut view = ReorderRouteView::new(
                    self.route_mapping.clone(),
                    self.route_config.clone(),
                    row_policy.clone(),
                    scope.and_then(|scope| scope.applicator.as_mut()),
                    self.stats.clone(),
                    maps,
                );
                let resolver = self.resolver.clone();
                let (batch_rows, batch_bytes) = (self.batch_rows, self.batch_bytes);
                let out = &mut self.out;
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
                        .next_batch_admitting(batch_rows, batch_bytes, async |bytes| {
                            out.admit(bytes).await.map_err(PullError::Admit)
                        })
                        .instrument(drain_span.clone())
                        .await
                        .map_err(|e| match e {
                            PullError::Drain(e) => {
                                bump_plan_failure(&stats, drain_reason(&e));
                                SinkError::from(e)
                            }
                            PullError::Admit(e) => e,
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
                let plan = planner.seal(commit_lsn, commit_ts).map_err(|e| {
                    bump_plan_failure(&stats, e.reason());
                    SinkError::Other(format!("plan seal: {e}"))
                })?;
                // Folded catalog events make routes this transaction's own
                let ReorderRouteView {
                    mapping,
                    config,
                    maps,
                    ..
                } = view;
                if maps.overlay.is_empty() {
                    self.route_cache = Some(RouteCache {
                        db,
                        mapping,
                        config,
                        policy: row_policy,
                        maps,
                    });
                }
                plan
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
            // Corruption fails the whole transaction before any side effect
            let plan = plan
                .into_verified()
                .map_err(|e| SinkError::Other(format!("plan verify: {e}")))?;
            (rows_total, published) = self
                .execute_plan(db, xid, &plan)
                .instrument(trace_span!(
                    !txn.is_none(),
                    parent: &txn,
                    "commit.execute",
                ))
                .await?;
        }
        // Unlink spill files now that every segment dispatched; an error above
        // drops the drain instead, leaving files for inspection.
        if drain.spilled() {
            Box::pin(drain.finish()).await.map_err(SinkError::from)?;
        }
        txn.record("rows", rows_total);
        txn.record("outcome", "committed");
        if !published {
            // rows=0 marker: publishes commit_lsn once every earlier partial
            // segment is durable. Covers empty / read-only commits and plans
            // whose tail is a control or truncate.
            self.out.seqs.mark(commit_lsn);
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
        let seq = self.out.seqs.open(record.source_lsn, Publish::Commit);
        {
            let mut buf = self.buffer.lock().await;
            buf.abort(xid, Pos::new(record.source_lsn), &payload.subxacts)
                .await
                .map_err(SinkError::from)?;
        }
        self.out.seqs.place(seq, 0);
        self.subxact_tracker.lock().await.forget_tree(xid);
        self.pending.forget_tree(xid);
        Ok(())
    }
}

/// Reorder sink awaiting its boot pass; only [`Self::boot`] yields the
/// [`RecordSink`]
pub struct BootingReorder(ReorderSink);

impl BootingReorder {
    /// Retire due mirrors, recover pending row outcomes, then apply the
    /// Added pass over relations `present` at `resume_lsn`
    pub async fn boot(
        self,
        shadow_data_dir: Option<&std::path::Path>,
        present: Vec<Arc<RelDescriptor>>,
        resume_lsn: u64,
    ) -> Result<ReorderSink, SinkError> {
        let mut sink = self.0;
        sink.flush_due_retires()
            .await
            .map_err(|e| boot_step("flush of due toast-mirror retires", e))?;
        sink.settle_pending_boot(shadow_data_dir)
            .await
            .map_err(|e| boot_step("settle of pending backup rows", e))?;
        sink.apply_boot_events(present, resume_lsn)
            .await
            .map_err(|e| boot_step("Added pass over descriptor log", e))?;
        Ok(sink)
    }
}

fn boot_step(step: &str, e: SinkError) -> SinkError {
    SinkError::Other(format!("boot {step}: {e}"))
}

/// Drain errors count as plan failures, admission errors are pipeline fatal
enum PullError {
    Drain(XactBufferError),
    Admit(SinkError),
}

impl From<XactBufferError> for PullError {
    fn from(e: XactBufferError) -> Self {
        Self::Drain(e)
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

/// Routes one transaction resolved under frozen (database, mapping, config,
/// row policy) versions, reused by later commits that freeze the same ones
struct RouteCache {
    db: Oid,
    mapping: Option<MappingSnapshot>,
    config: Option<Arc<ResolvedConfig>>,
    policy: RowPolicy,
    maps: RouteMaps,
}

impl RouteCache {
    /// Versions compare by identity: a republish or fold swaps the Arc
    fn matches(
        &self,
        db: Oid,
        mapping: &Option<MappingSnapshot>,
        config: &Option<Arc<ResolvedConfig>>,
        policy: &RowPolicy,
    ) -> bool {
        fn same<T>(a: &Option<Arc<T>>, b: &Option<Arc<T>>) -> bool {
            match (a, b) {
                (Some(a), Some(b)) => Arc::ptr_eq(a, b),
                (None, None) => true,
                _ => false,
            }
        }
        self.db == db
            && same(&self.mapping, mapping)
            && same(&self.config, config)
            && self.policy.soft_delete == policy.soft_delete
            && Arc::ptr_eq(&self.policy.system, &policy.system)
    }
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
    maps: RouteMaps,
    row_policy: RowPolicy,
    applicator: Option<&'a mut DdlApplicator>,
    stats: Arc<EmitterStats>,
}

/// Lookups a route view fills. Outlive the view so later commits skip
/// rebuilding them, or at least their allocation and hasher seeding
#[derive(Default)]
pub struct RouteMaps {
    /// Catalog fold above `mapping`; `None` value = locally dropped
    overlay: HashMap<RelName, Option<Arc<TableMapping>>>,
    /// Routes by row relation, partitions resolved through their root. The
    /// route derives from the descriptor (a partition's ancestors and
    /// attnums), so an entry answers only for the version it was built from
    memo: HashMap<RelName, (Arc<RelDescriptor>, Option<Arc<RouteSnapshot>>)>,
    /// Routes by owning relation
    owned: HashMap<RelName, Option<Arc<RouteSnapshot>>>,
}

impl RouteMaps {
    fn clear(&mut self) {
        self.overlay.clear();
        self.memo.clear();
        self.owned.clear();
    }
}

impl<'a> ReorderRouteView<'a> {
    pub fn new(
        mapping: Option<MappingSnapshot>,
        config: Option<Arc<ResolvedConfig>>,
        row_policy: RowPolicy,
        applicator: Option<&'a mut DdlApplicator>,
        stats: Arc<EmitterStats>,
        maps: RouteMaps,
    ) -> Self {
        Self {
            mapping,
            config,
            maps,
            row_policy,
            applicator,
            stats,
        }
    }
}

impl ReorderRouteView<'_> {
    fn owned_route(&mut self, rel_name: &RelName) -> Option<Arc<RouteSnapshot>> {
        if let Some(r) = self.maps.owned.get(rel_name) {
            return r.clone();
        }
        let mapped = if let Some(o) = self.maps.overlay.get(rel_name) {
            o.clone()
        } else {
            self.mapping.as_ref().and_then(|m| m.get(rel_name)).cloned()
        };
        let route = mapped.map(|m| {
            let rules = self
                .config
                .as_ref()
                .map_or_else(Arc::default, |rc| rc.column_rules.clone());
            let policy = self.row_policy.for_rel(self.config.as_deref(), rel_name);
            RouteSnapshot::freeze(m, rules, policy)
        });
        self.maps.owned.insert(rel_name.clone(), route.clone());
        route
    }
}

impl PlanRouteView for ReorderRouteView<'_> {
    fn route_for(&mut self, heap: &DescribedHeap) -> Option<Arc<RouteSnapshot>> {
        let rel_name = &heap.descriptor.rel_name;
        if let Some((desc, r)) = self.maps.memo.get(rel_name)
            && Arc::ptr_eq(desc, &heap.descriptor)
        {
            r.clone()
        } else {
            let rules = self.config.as_ref().map(|rc| rc.rules.clone());
            let route = route_via(&heap.descriptor, rules.as_deref(), |owner| {
                self.owned_route(owner)
            });
            self.maps
                .memo
                .insert(rel_name.clone(), (heap.descriptor.clone(), route.clone()));
            route
        }
    }

    fn unmapped(&mut self, rel: &RelDescriptor, heaps: u64) {
        self.stats
            .unsupported_relations
            .fetch_add(heaps, Ordering::Relaxed);
        let key = TableKey::new(rel.rfn.db_node, rel.rel_name.clone());
        self.stats
            .unmapped_rows_by_table
            .counter(&key)
            .fetch_add(heaps, Ordering::Relaxed);
    }

    async fn apply(&mut self, entry: &DrainEntry) -> Result<(), String> {
        let DrainEntry::Catalog(ev) = entry else {
            // Config: frozen-version planning (doc above); ToastBarrier:
            // no route effect
            return Ok(());
        };
        // Schema events can change partition descriptors or root mappings
        self.maps.memo.clear();
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
            self.maps.owned.remove(&rel);
            self.maps.overlay.insert(rel, m.map(Arc::new));
        }
        Ok(())
    }
}

impl ReorderSink {
    async fn handle(&mut self, record: &Record<'_>) -> Result<(), SinkError> {
        if !Self::wants(record) {
            return Ok(());
        }
        let info = record.parsed.header.info;
        let op = info & XLOG_XACT_OPMASK;
        let xid = record.parsed.header.xact_id;
        match op {
            XLOG_XACT_COMMIT | XLOG_XACT_COMMIT_PREPARED => self.on_commit(xid, info, record).await,
            XLOG_XACT_ABORT | XLOG_XACT_ABORT_PREPARED => self.on_abort(xid, info, record).await,
            XLOG_XACT_ASSIGNMENT => {
                if let Some((xtop, subs)) = parse_xact_assignment(&record.parsed.main_data) {
                    self.subxact_tracker.lock().await.assign(xtop, &subs);
                }
                Ok(())
            }
            // PREPARE allocates no seq; COMMIT_PREPARED drains it later
            _ => Ok(()),
        }
    }
}

impl RecordSink for ReorderSink {
    fn wants(record: &Record<'_>) -> bool {
        record.parsed.header.resource_manager_id == RmId::Xact as u8
    }

    fn on_close<'a>(
        &'a mut self,
    ) -> Pin<Box<dyn Future<Output = Result<(), SinkError>> + Send + 'a>> {
        Box::pin(self.out.ship())
    }

    fn on_record<'a>(
        &'a mut self,
        record: &'a Record<'a>,
    ) -> Pin<Box<dyn Future<Output = Result<(), SinkError>> + Send + 'a>> {
        Box::pin(async move {
            self.handle(record).await?;
            self.out.ship().await
        })
    }

    fn on_records<'a>(
        &'a mut self,
        batch: &'a [Record<'a>],
        _pre: &'a mut [Option<Predecoded>],
    ) -> Pin<Box<dyn Future<Output = Result<(), SinkError>> + Send + 'a>> {
        Box::pin(async move {
            for record in batch {
                self.handle(record).await?;
            }
            self.out.ship().await
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
    use crate::decode::heap_decoder::{DecodedHeap, HeapOp};
    use crate::emit::pipeline::tail::{self, TailParts};
    use crate::mapping::TableTarget;
    use crate::xact::xact_buffer::raw_fixtures::int4_descriptor;
    use std::time::Duration;

    fn mapping_for(rel: &RelName) -> TableMapping {
        TableMapping {
            target: TableTarget::new("db", &rel.name),
            columns: Vec::new(),
            tees: Vec::new(),
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
            Some(handle.snapshot()),
            None,
            RowPolicy::default(),
            None,
            stats,
            RouteMaps::default(),
        );
        assert!(view.route_for(&heap_of(&planned)).is_some());
        assert!(view.route_for(&heap_of(&added)).is_none());

        handle.publish(Arc::new(HashMap::from_iter([(
            added.clone(),
            Arc::new(mapping_for(&added)),
        )])));

        assert!(
            view.route_for(&heap_of(&planned)).is_some(),
            "republish can't unroute rows already planned"
        );
        assert!(
            view.route_for(&heap_of(&added)).is_none(),
            "republish can't route rows this transaction planned unmapped"
        );
    }

    /// Commits reuse routes only under the exact versions they were built
    /// from: a mapping republish, config reload or policy change rebuilds
    #[tokio::test]
    async fn route_cache_matches_only_identical_versions() {
        let rel = RelName::new("public", "t");
        let handle =
            crate::mapping::mapping_handle(HashMap::from_iter([(rel.clone(), mapping_for(&rel))]));
        let mapping = Some(handle.snapshot());
        let config = Some(Arc::new(ResolvedConfig::default()));
        let policy = RowPolicy::default();
        let cache = RouteCache {
            db: 5,
            mapping: mapping.clone(),
            config: config.clone(),
            policy: policy.clone(),
            maps: RouteMaps::default(),
        };
        assert!(cache.matches(5, &mapping, &config, &policy));
        assert!(
            cache.matches(5, &Some(handle.snapshot()), &config, &policy),
            "unchanged handle snapshots the same version"
        );
        assert!(!cache.matches(6, &mapping, &config, &policy));
        assert!(!cache.matches(5, &None, &config, &policy));
        assert!(!cache.matches(5, &mapping, &None, &policy));
        assert!(
            !cache.matches(
                5,
                &mapping,
                &Some(Arc::new(ResolvedConfig::default())),
                &policy
            ),
            "equal content under a new Arc is a reload"
        );
        let soft = RowPolicy {
            soft_delete: !policy.soft_delete,
            ..policy.clone()
        };
        assert!(!cache.matches(5, &mapping, &config, &soft));
        let renamed = RowPolicy {
            system: Arc::new((*policy.system).clone()),
            ..policy.clone()
        };
        assert!(!cache.matches(5, &mapping, &config, &renamed));

        handle.publish(Arc::new(HashMap::from_iter([(
            rel.clone(),
            Arc::new(mapping_for(&rel)),
        )])));
        assert!(!cache.matches(5, &Some(handle.snapshot()), &config, &policy));
    }

    /// Memoized routes carried into the next commit answer only for the
    /// descriptor version they were built from
    #[tokio::test]
    async fn carried_route_maps_rebuild_after_schema_change() {
        let rel = RelName::new("public", "t");
        let handle =
            crate::mapping::mapping_handle(HashMap::from_iter([(rel.clone(), mapping_for(&rel))]));
        let snapshot = handle.snapshot();
        let stats = Arc::new(EmitterStats::default());
        let heap = heap_of(&rel);
        let memoized = |view: &ReorderRouteView, desc: &Arc<RelDescriptor>| {
            view.maps
                .memo
                .get(&rel)
                .is_some_and(|(d, _)| Arc::ptr_eq(d, desc))
        };

        let mut view = ReorderRouteView::new(
            Some(snapshot.clone()),
            None,
            RowPolicy::default(),
            None,
            stats.clone(),
            RouteMaps::default(),
        );
        let first = view.route_for(&heap).expect("mapped");
        assert!(Arc::ptr_eq(&first, &view.route_for(&heap).unwrap()));
        let ReorderRouteView { maps, .. } = view;

        let mut view = ReorderRouteView::new(
            Some(snapshot.clone()),
            None,
            RowPolicy::default(),
            None,
            stats.clone(),
            maps,
        );
        assert!(
            Arc::ptr_eq(&first, &view.route_for(&heap).unwrap()),
            "next commit reuses the carried route"
        );
        assert!(memoized(&view, &heap.descriptor));

        let mut altered = heap.clone();
        altered.descriptor = Arc::new((*heap.descriptor).clone());
        view.route_for(&altered).expect("mapped");
        assert!(
            memoized(&view, &altered.descriptor),
            "new descriptor version rebuilds"
        );

        // Catalog event folded mid-plan drops the memo even without an
        // applicator to predict its mapping
        view.apply(&DrainEntry::Catalog(SchemaEvent::Dropped {
            oid: altered.descriptor.oid,
            rel_name: rel.clone(),
            partition: None,
        }))
        .await
        .unwrap();
        assert!(view.maps.memo.is_empty());
        view.route_for(&altered).expect("mapped");
        view.apply(&DrainEntry::ToastBarrier {
            toast_relid: 1,
            marker_lsn: 0,
        })
        .await
        .unwrap();
        assert!(
            memoized(&view, &altered.descriptor),
            "non-catalog entries keep the memo"
        );
    }

    fn outbox(budget: &crate::budget::MemoryBudget, fatal: &Fatal) -> (Outbox, TailParts) {
        let (msg_tx, ack, tail) = tail::spawn_null(Arc::new(Monotone::default()), fatal.clone());
        let out = Outbox {
            msg_tx,
            fatal: fatal.clone(),
            seqs: ack.seqs(0).held(),
            staged: Vec::new(),
            staged_rows: 0,
            staged_bytes: 0,
            budget: Some(budget.clone()),
        };
        (out, tail)
    }

    /// Pool short only by permits riding staged rows: admission ships them
    /// rather than waiting on itself
    #[tokio::test]
    async fn admit_ships_staged_permits_before_waiting() {
        let budget = crate::budget::MemoryBudget::new(1 << 20);
        let fatal = Fatal::new();
        let (mut out, _tail) = outbox(&budget, &fatal);
        let staged = budget.try_admit(768 << 10).unwrap();
        out.staged.push(RowChunk {
            rows: Vec::new(),
            permit: Some(Arc::new(staged)),
        });
        let permit = tokio::time::timeout(Duration::from_secs(5), out.admit(512 << 10))
            .await
            .expect("admit waited on its own staged permits")
            .unwrap()
            .unwrap();
        assert_eq!(permit.bytes(), 512 << 10);
        assert!(out.staged.is_empty());
    }

    #[tokio::test]
    async fn admit_returns_on_fatal_while_pool_held() {
        let budget = crate::budget::MemoryBudget::new(1 << 20);
        let fatal = Fatal::new();
        let (mut out, _tail) = outbox(&budget, &fatal);
        let _held = budget.try_admit(1 << 20).unwrap();
        let trip = async {
            tokio::task::yield_now().await;
            fatal.set("inserter down".into());
        };
        let (admitted, ()) = tokio::join!(
            tokio::time::timeout(Duration::from_secs(5), out.admit(512 << 10)),
            trip,
        );
        let Err(err) = admitted.expect("admit ignored fatal") else {
            panic!("admit passed a held pool");
        };
        assert!(err.to_string().contains("inserter down"), "{err}");
    }
}
