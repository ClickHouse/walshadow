//! Descriptor capture at catalog-commit boundaries.
//!
//! Runs inside the pump's publication hold, after shadow applies through the
//! boundary's `next_lsn` and before the commit record forwards to the
//! worker: capture observes exactly the commit's catalog state, its batch is
//! durable before any successor byte publishes, and drain finds events
//! already attached to the xact.
//!
//! Replay-from-log first: a boundary whose batch is already stored derives
//! its events from the stored entries against each oid's historical
//! predecessor — no SQL, deterministic across restarts. Boundaries at or
//! below the seed's `covered_through` are baked into the seed snapshot and
//! skip entirely. A miss queries shadow pinned at `next_lsn` (nothing past
//! the commit has published during the hold); replay anywhere else means the
//! log lost coverage, fatal.
//!
//! Statistics-only commits leave relation definitions unchanged, so skip
//! replay waits and save an empty batch. Restart loses evidence that only
//! statistics changed. Replay saved batch to avoid querying shadow after
//! it has replayed past this commit
//!
//! [`timeline::build`] combines the previous descriptor, reads and misses
//! during the transaction, and the descriptor at commit. It records gaps
//! where decoding may be unsafe. Record each gap by both filenode and OID
//! so either lookup stops decoding there. Start a new filenode at its
//! `XLOG_SMGR_CREATE` marker and retire its predecessor at the same LSN.
//! Record dropped relations at `next_lsn`.
//!
//! At each [`crate::record::BoundaryKind::Command`], read the transaction's
//! uncommitted catalog rows for the named relations into [`PendingCatalog`].
//! Use these reads to build the log entries at commit. Skip a read only when
//! WAL shows no row of those relations is written between the command's
//! first catalog write and the next read that covers them
//! ([`crate::source::wal_stream`] finds that read in buffered WAL).

use std::sync::Arc;
use std::time::Duration;

use tokio_postgres::types::Oid;
use walrus::pg::walparser::RelFileNode;

use crate::catalog::desc_log::{
    Ambiguity, AmbiguityReason, AmbiguityScope, BatchRecord, DescriptorLog, LogEntry, LogValue,
    ObservationKind, RelationObservation,
};
use crate::catalog::pending::{DegradeReason, PendingCatalog, PendingSlot};
use crate::catalog::shadow_catalog::{CatalogError, ParkedAt, ShadowCatalog};
use crate::catalog::timeline::{self, Shapes};
#[cfg(feature = "test-support")]
use crate::crash_test;
use crate::filter::SmgrMarkers;
use crate::ops::bridge::BridgeError;
use crate::record::{BoundaryInfo, BoundaryKind, SinkError};
use crate::schema::{DroppedPartition, RelDescriptor, SchemaEvent, compute_schema_diff};
use crate::xact::xact_buffer::XactBuffer;
use foldhash::{HashMap, HashMapExt, HashSet};

crate::atomic_stats! {
    pub struct CaptureStats {
        /// Boundaries captured via shadow SQL
        pub sql_captures,
        /// Boundaries replayed from stored batches
        pub log_replays,
        /// Boundaries at or below covered_through
        pub skipped_covered,
        /// Descriptors fetched across SQL captures
        pub rels_captured,
        /// Commits that scanned the whole catalog after a cache flush or
        /// a catalog write with no relation list.
        pub capture_all_runs,
        pub events_added,
        pub events_changed,
        pub events_dropped,
        /// Intervals where no known descriptor could safely decode rows.
        pub ambiguities_published,
        /// Command boundaries captured into the pending timeline
        pub pending_captures,
        /// Command boundaries skipped because WAL showed no row needs them
        pub pending_skipped,
        /// Descriptors read across those captures
        pub pending_rels,
        /// Publication holds taken for a command boundary
        pub pending_holds,
        /// Cumulative nanos parked in command-boundary holds
        pub pending_hold_nanos,
        /// Pending slots folded into a commit batch
        pub pending_entries_promoted,
        /// Pending slots dropped with an aborted tree
        pub pending_entries_dropped_abort,
        /// Transactions with missed catalog reads, by reason for the first miss.
        pub pending_degraded: [std::sync::atomic::AtomicU64; DegradeReason::ALL.len()],
        /// Capture duration, nanos (inside the boundary hold)
        pub capture_nanos,
    }
}

impl CaptureStats {
    fn count_degrade(&self, reason: DegradeReason) {
        let at = DegradeReason::ALL
            .iter()
            .position(|r| *r == reason)
            .expect("reason is one of ALL");
        self.pending_degraded[at].fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
}

pub struct CatalogCapture {
    log: Arc<DescriptorLog>,
    catalog: Arc<tokio::sync::Mutex<ShadowCatalog>>,
    buffer: Arc<tokio::sync::Mutex<XactBuffer>>,
    markers: Arc<std::sync::Mutex<SmgrMarkers>>,
    /// Speculative per-transaction catalog state, written here at command
    /// boundaries and read by the commit drain
    pending: Arc<PendingCatalog>,
    stats: Arc<CaptureStats>,
    /// Command reads left to succeed before every later one fails,
    /// `u32::MAX` disarmed
    #[cfg(feature = "test-support")]
    reads_before_failure: std::sync::atomic::AtomicU32,
}

/// One derived schema event keyed at its drain LSN
struct PendingEvent {
    lsn: u64,
    event: SchemaEvent,
}

impl CatalogCapture {
    pub fn new(
        log: Arc<DescriptorLog>,
        catalog: Arc<tokio::sync::Mutex<ShadowCatalog>>,
        buffer: Arc<tokio::sync::Mutex<XactBuffer>>,
        markers: Arc<std::sync::Mutex<SmgrMarkers>>,
        pending: Arc<PendingCatalog>,
    ) -> Self {
        Self {
            log,
            catalog,
            buffer,
            markers,
            pending,
            stats: Arc::new(CaptureStats::default()),
            #[cfg(feature = "test-support")]
            reads_before_failure: std::sync::atomic::AtomicU32::new(u32::MAX),
        }
    }

    /// Fail every command read after `reads` more succeed
    #[cfg(feature = "test-support")]
    pub fn fail_command_read_after(&self, reads: u32) {
        self.reads_before_failure
            .store(reads, std::sync::atomic::Ordering::Relaxed);
    }

    #[cfg(feature = "test-support")]
    fn forced_failure(&self) -> bool {
        use std::sync::atomic::Ordering::Relaxed;
        let left = self.reads_before_failure.load(Relaxed);
        if left == u32::MAX || left == 0 {
            return left == 0;
        }
        self.reads_before_failure.store(left - 1, Relaxed);
        false
    }

    #[cfg(not(feature = "test-support"))]
    fn forced_failure(&self) -> bool {
        false
    }

    pub fn stats_handle(&self) -> Arc<CaptureStats> {
        self.stats.clone()
    }

    /// Decide whether to wait for shadow replay and read the catalog.
    pub fn admits(&self, info: &BoundaryInfo, next_lsn: u64) -> bool {
        let BoundaryKind::Command { writer_xid } = info.kind else {
            return !info.stats_only;
        };
        if no_row_needs(info, self.log.db_oid(), |rfn| self.owner(rfn)) {
            self.stats
                .pending_skipped
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            return false;
        }
        // At commit, record misses for reads from unlinked subtransactions
        // that this name refresh could not reach.
        if info.capture_all {
            self.pending
                .name_refresh(info.drain_xid, writer_xid, next_lsn);
        }
        let oids = self.boundary_oids(info);
        // A namespace change before any catalog reads has no names to refresh.
        if oids.is_empty() {
            return false;
        }
        // Shadow has already replayed past boundaries covered by the log.
        if next_lsn > self.log.covered_through() {
            return true;
        }
        let reason = DegradeReason::ReplayMismatch;
        if self.miss(info.drain_xid, writer_xid, &oids, next_lsn, reason) {
            tracing::warn!(
                target: "walshadow::desc_log",
                xid = info.drain_xid,
                boundary = format_args!("{next_lsn:#X}"),
                reason = reason.label(),
                "skipped catalog read at command boundary",
            );
        }
        false
    }

    /// Relation whose rows a filenode holds now
    fn owner(&self, rfn: RelFileNode) -> Option<Oid> {
        self.log
            .peek_spanned(rfn, u64::MAX)
            .ok()
            .map(|(desc, _)| desc.oid)
            .or_else(|| self.pending.rfn_owner(rfn))
    }

    /// Read named relations. A cache flush or namespace change can also
    /// affect names, so re-read relations already captured for this
    /// transaction. Layout changes always name their relations
    /// ([`crate::catalog::pending`]).
    fn boundary_oids(&self, info: &BoundaryInfo) -> Vec<Oid> {
        let mut oids: Vec<Oid> = info.oids.iter().map(|a| a.oid).collect();
        if info.capture_all {
            oids.extend(self.pending.slot_oids(info.drain_xid));
            oids.sort_unstable();
            oids.dedup();
        }
        oids
    }

    /// Record a missed read. Count it and return `true` on the first miss
    /// for this transaction.
    fn miss(
        &self,
        top_xid: u32,
        writer_xid: u32,
        oids: &[Oid],
        at: u64,
        reason: DegradeReason,
    ) -> bool {
        let first = self.pending.miss(top_xid, writer_xid, oids, at, reason);
        if first {
            self.stats.count_degrade(reason);
        }
        first
    }

    /// Count a released command-boundary hold
    pub fn count_hold(&self, info: &BoundaryInfo, held: Duration) {
        use std::sync::atomic::Ordering::Relaxed;
        let BoundaryKind::Command { .. } = info.kind else {
            return;
        };
        self.stats.pending_holds.fetch_add(1, Relaxed);
        self.stats
            .pending_hold_nanos
            .fetch_add(held.as_nanos() as u64, Relaxed);
    }

    /// Aborted tree: its speculative slots die here, on the pump, before any
    /// later boundary could promote them
    pub fn forget_aborted(&self, members: &[u32]) {
        use std::sync::atomic::Ordering::Relaxed;
        let dropped = self.pending.forget_members(members);
        self.stats
            .pending_entries_dropped_abort
            .fetch_add(dropped as u64, Relaxed);
    }

    pub async fn capture_boundary(
        &self,
        info: &BoundaryInfo,
        commit_lsn: u64,
        parked: &ParkedAt,
    ) -> Result<(), SinkError> {
        use std::sync::atomic::Ordering::Relaxed;
        if let BoundaryKind::Command { writer_xid } = info.kind {
            let start = std::time::Instant::now();
            let out = self.capture_command(info, writer_xid, parked).await;
            self.stats
                .capture_nanos
                .fetch_add(start.elapsed().as_nanos() as u64, Relaxed);
            return out;
        }
        self.capture_commit(info, commit_lsn, parked.lsn(), Some(parked))
            .await
    }

    /// Statistics-only commit capture declined to park for: answered from the
    /// log or an empty batch, never a shadow read
    pub async fn cover_unparked(
        &self,
        info: &BoundaryInfo,
        commit_lsn: u64,
        next_lsn: u64,
    ) -> Result<(), SinkError> {
        self.capture_commit(info, commit_lsn, next_lsn, None).await
    }

    async fn capture_commit(
        &self,
        info: &BoundaryInfo,
        commit_lsn: u64,
        next_lsn: u64,
        parked: Option<&ParkedAt>,
    ) -> Result<(), SinkError> {
        use std::sync::atomic::Ordering::Relaxed;
        let start = std::time::Instant::now();
        // Late `XLOG_XACT_ASSIGNMENT` leaves a subxact's slots under its own
        // key; the commit's member list is the first place the whole tree is
        // named, so fold before anything reads the timeline — including the
        // paths below that capture nothing
        self.pending.consolidate(info.drain_xid, &info.members);
        if next_lsn <= self.log.covered_through() {
            self.stats.skipped_covered.fetch_add(1, Relaxed);
            return Ok(());
        }
        let events = if let Some(batch) = self.log.batch_at(next_lsn) {
            self.stats.log_replays.fetch_add(1, Relaxed);
            self.replay_events(&batch)
        } else if let Some(parked) = parked
            && !info.stats_only
        {
            self.sql_capture(info, commit_lsn, parked).await?
        } else {
            self.cover_stub(commit_lsn, next_lsn).await?;
            Vec::new()
        };
        if !events.is_empty() {
            let mut buf = self.buffer.lock().await;
            for pe in events {
                match &pe.event {
                    SchemaEvent::Added { .. } => self.stats.events_added.fetch_add(1, Relaxed),
                    SchemaEvent::Changed { .. } => self.stats.events_changed.fetch_add(1, Relaxed),
                    SchemaEvent::Dropped { .. } => self.stats.events_dropped.fetch_add(1, Relaxed),
                };
                buf.on_schema_event(info.drain_xid, pe.lsn, pe.event);
            }
        }
        self.stats
            .capture_nanos
            .fetch_add(start.elapsed().as_nanos() as u64, Relaxed);
        Ok(())
    }

    /// Derive a batch's events against each oid's historical predecessor
    /// (never the loaded head — boot loads the whole log before the WAL
    /// re-read). Live capture runs this over the batch it just appended, so
    /// a boundary replayed from the log yields the same events by
    /// construction.
    ///
    /// Send one event per table based on its final schema. Apply adds and
    /// changes at its earliest `Present` entry so ClickHouse can accept
    /// values for new columns before replay sends those rows.
    fn replay_events(&self, batch: &BatchRecord) -> Vec<PendingEvent> {
        let mut last_for_oid: HashMap<Oid, usize> = HashMap::with_capacity(batch.entries.len());
        let mut first_present: HashMap<Oid, u64> = HashMap::with_capacity(batch.entries.len());
        for (at, entry) in batch.entries.iter().enumerate() {
            if !matches!(entry.value, LogValue::Retired) {
                last_for_oid.insert(entry.oid, at);
            }
            if matches!(entry.value, LogValue::Present(_)) {
                first_present
                    .entry(entry.oid)
                    .and_modify(|lsn| *lsn = (*lsn).min(entry.valid_from))
                    .or_insert(entry.valid_from);
            }
        }
        let dropped_here: HashSet<Oid> = batch
            .entries
            .iter()
            .filter(|e| matches!(e.value, LogValue::Dropped))
            .map(|e| e.oid)
            .collect();
        let mut out = Vec::with_capacity(last_for_oid.len());
        for (at, entry) in batch.entries.iter().enumerate() {
            if last_for_oid.get(&entry.oid) != Some(&at) {
                continue;
            }
            let pred_desc = self.present_before(entry.oid, batch.captured_at);
            // Not Present means Dropped, last_for_oid skips Retired
            let (lsn, event) = if let LogValue::Present(desc) = &entry.value {
                (
                    first_present[&entry.oid],
                    diff_event(pred_desc.as_deref(), desc),
                )
            } else {
                let dropped = pred_desc.map(|old| SchemaEvent::Dropped {
                    oid: entry.oid,
                    rel_name: old.rel_name.clone(),
                    partition: old.is_partition().then(|| DroppedPartition {
                        desc: old.clone(),
                        dropped: old
                            .ancestors
                            .iter()
                            .map(|a| a.oid)
                            .filter(|oid| dropped_here.contains(oid))
                            .collect(),
                    }),
                });
                (entry.valid_from, dropped)
            };
            out.extend(event.map(|event| PendingEvent { lsn, event }));
        }
        out
    }

    /// Read the transaction's catalog changes while shadow replay is paused
    /// at this command boundary. Record a miss if the read fails so commit
    /// can check compatibility.
    async fn capture_command(
        &self,
        info: &BoundaryInfo,
        writer_xid: u32,
        parked: &ParkedAt,
    ) -> Result<(), SinkError> {
        use std::sync::atomic::Ordering::Relaxed;
        let next_lsn = parked.lsn();
        let top_xid = info.drain_xid;
        let oids = self.boundary_oids(info);
        let read = if self.forced_failure() {
            Err(CatalogError::Parse("forced command read failure".into()))
        } else {
            let mut cat = self.catalog.lock().await;
            cat.fetch_overlay_descriptors(&oids, top_xid, parked).await
        };
        let descs = match read {
            Ok(descs) => descs,
            Err(e) => {
                let reason = degrade_reason(&e);
                self.miss(top_xid, writer_xid, &oids, next_lsn, reason);
                tracing::warn!(
                    target: "walshadow::desc_log",
                    xid = top_xid,
                    boundary = format_args!("{next_lsn:#X}"),
                    reason = reason.label(),
                    err = %e,
                    "catalog read failed at command boundary",
                );
                return Ok(());
            }
        };
        let slots: Vec<PendingSlot> = descs
            .into_iter()
            .filter(|d| matches!(d.kind, b'r' | b'p' | b'm' | b't'))
            .map(|desc| PendingSlot {
                boundary: next_lsn,
                writer_xid,
                desc: Arc::new(desc),
            })
            .collect();
        self.stats.pending_captures.fetch_add(1, Relaxed);
        self.stats
            .pending_rels
            .fetch_add(slots.len() as u64, Relaxed);
        self.pending.record(top_xid, slots);
        Ok(())
    }

    /// Save an empty batch so restart can replay this boundary without
    /// querying shadow after it has replayed past this position
    async fn cover_stub(&self, commit_lsn: u64, next_lsn: u64) -> Result<(), SinkError> {
        self.log
            .append_batch(BatchRecord {
                captured_at: next_lsn,
                commit_lsn,
                observations: Vec::new(),
                ambiguities: Vec::new(),
                entries: Vec::new(),
            })
            .await
            .map_err(SinkError::ctx("descriptor log append"))
    }

    async fn sql_capture(
        &self,
        info: &BoundaryInfo,
        commit_lsn: u64,
        parked: &ParkedAt,
    ) -> Result<Vec<PendingEvent>, SinkError> {
        use std::sync::atomic::Ordering::Relaxed;
        let next_lsn = parked.lsn();
        self.stats.sql_captures.fetch_add(1, Relaxed);
        // Pinned read fails `ReplayMismatch` once shadow replayed past this
        // boundary without a stored batch: log lost coverage (wiped or foreign
        // spill dir), decode would misread
        let descs = {
            let mut cat = self.catalog.lock().await;
            if info.capture_all {
                self.stats.capture_all_runs.fetch_add(1, Relaxed);
                cat.fetch_all_descriptors_at(parked).await
            } else {
                let oids: Vec<Oid> = info.oids.iter().map(|a| a.oid).collect();
                cat.fetch_descriptors_batch_at(&oids, parked).await
            }
        }
        .map_err(SinkError::ctx(format_args!(
            "descriptor capture at {commit_lsn:#X}"
        )))?;
        self.stats
            .rels_captured
            .fetch_add(descs.len() as u64, Relaxed);

        let fetched: HashMap<Oid, RelDescriptor> = descs
            .into_iter()
            .filter(|d| matches!(d.kind, b'r' | b'p' | b'm' | b't'))
            .map(|d| (d.oid, d))
            .collect();
        // Tombstone scope: targeted capture checks its own oid list;
        // capture-all diffs the log's whole Present set
        let mut expected: Vec<Oid> = if info.capture_all {
            let mut all = self.log.present_oids();
            all.extend(fetched.keys().copied());
            all.sort_unstable();
            all.dedup();
            all
        } else {
            let mut oids: Vec<Oid> = info.oids.iter().map(|a| a.oid).collect();
            oids.extend(fetched.keys().copied());
            oids.sort_unstable();
            oids.dedup();
            oids
        };
        // Deterministic entry order within the batch
        expected.sort_unstable();

        let pg_class_touch: HashMap<Oid, u64> = info
            .oids
            .iter()
            .filter_map(|a| a.pg_class_touch.map(|l| (a.oid, l)))
            .collect();

        // Evidence: what the boundary knew, so replay reproduces the
        // verdict without reinferring from current catalog. Sorted for
        // deterministic encoding (info.oids order is map-derived)
        let mut observations: Vec<RelationObservation> = info
            .oids
            .iter()
            .map(|a| RelationObservation {
                oid: Some(a.oid),
                rfn: None,
                first_touch_lsn: a.pg_class_touch.unwrap_or(info.tree_first_touch),
                smgr_create_lsn: None,
                kind: ObservationKind::AffectedOid,
            })
            .collect();
        if info.capture_all {
            observations.push(RelationObservation {
                oid: None,
                rfn: None,
                first_touch_lsn: info.tree_first_touch,
                smgr_create_lsn: None,
                kind: ObservationKind::FullScan,
            });
        }

        let promoted = self.pending.promoted(info.drain_xid);

        let mut entries: Vec<Arc<LogEntry>> = Vec::new();
        let mut ambiguities: Vec<Arc<Ambiguity>> = Vec::new();
        for oid in expected {
            let pred_desc = self.present_before(oid, next_lsn);
            match fetched.get(&oid) {
                Some(desc) => {
                    let fin = Arc::new(desc.clone());
                    // Full physical identity: SET TABLESPACE changes spc
                    // alongside rel_node, and rel_node reuse across
                    // tablespaces must not read as "same filenode"
                    let in_place = pred_desc.as_ref().is_some_and(|old| old.rfn == fin.rfn);
                    let first_touch = pg_class_touch
                        .get(&oid)
                        .copied()
                        .unwrap_or(info.tree_first_touch);
                    if !in_place && let Some(m) = self.marker_for(fin.rfn) {
                        observations.push(RelationObservation {
                            oid: Some(oid),
                            rfn: Some(fin.rfn),
                            first_touch_lsn: m,
                            smgr_create_lsn: Some(m),
                            kind: ObservationKind::SmgrCreate,
                        });
                    }
                    let shapes = Shapes {
                        pred: pred_desc.as_ref(),
                        first_touch,
                        slots: promoted.as_ref().map_or(&[][..], |p| p.slots_for(oid)),
                        misses: promoted.as_ref().map_or(&[][..], |p| p.misses_for(oid)),
                        fin: &fin,
                        next_lsn,
                    };
                    let timeline = timeline::build(&shapes, |rfn| self.marker_for(rfn));
                    for why in &timeline.benign {
                        tracing::debug!(
                            target: "walshadow::desc_log",
                            oid,
                            rel = %fin.rel_name,
                            why,
                            "descriptor change leaves row decoding unchanged",
                        );
                    }
                    for run in &timeline.fenced {
                        tracing::warn!(
                            target: "walshadow::desc_log",
                            oid,
                            rel = %fin.rel_name,
                            from = format_args!("{:#X}", run.from),
                            through = format_args!("{:#X}", run.through),
                            why = run.why,
                            "recorded an ambiguity interval because decoding may be unsafe",
                        );
                        // Count once even though both lookup keys get an entry.
                        self.stats.ambiguities_published.fetch_add(1, Relaxed);
                        let interval = |scope| {
                            Arc::new(Ambiguity {
                                scope,
                                from_lsn: run.from,
                                through_lsn: run.through,
                                reason: AmbiguityReason::UnknownMutationPosition,
                            })
                        };
                        // Keep entry order stable for batch equality and hashing.
                        ambiguities.push(interval(AmbiguityScope::Rfn(fin.rfn)));
                        ambiguities.push(interval(AmbiguityScope::Oid(oid)));
                    }
                    self.stats
                        .pending_entries_promoted
                        .fetch_add(timeline.promoted as u64, Relaxed);
                    entries.extend(timeline.entries.into_iter().map(Arc::new));
                }
                None => {
                    let Some(old) = pred_desc else { continue };
                    entries.push(Arc::new(LogEntry {
                        valid_from: next_lsn,
                        oid,
                        rfn: old.rfn,
                        value: LogValue::Dropped,
                    }));
                }
            }
        }
        observations.sort_unstable_by_key(|o| {
            (
                o.kind as u8,
                o.oid,
                o.rfn.map(|r| (r.spc_node, r.db_node, r.rel_node)),
                o.first_touch_lsn,
            )
        });
        // Zero-entry stub still appends: boot replay must distinguish
        // "captured, no shape change" from "never captured"
        let batch = BatchRecord {
            captured_at: next_lsn,
            commit_lsn,
            observations,
            ambiguities,
            entries,
        };
        #[cfg(feature = "test-support")]
        {
            crash_test::capture(commit_lsn);
            crash_test::hit(crash_test::Point::BeforeDescriptor, commit_lsn);
        }
        self.log
            .append_batch(batch.clone())
            .await
            .map_err(SinkError::ctx("descriptor log append"))?;
        #[cfg(feature = "test-support")]
        crash_test::hit(crash_test::Point::AfterDescriptor, commit_lsn);
        // Events off the appended batch, the same derivation a restart runs:
        // one boundary cannot mean two schema histories.
        // `predecessor_before` reads strictly older batches, so the append
        // above is invisible to it
        Ok(self.replay_events(&batch))
    }

    /// Markers key physical WAL locators; descriptor rfns are resolved to
    /// physical at capture — match on full identity
    /// Descriptor `oid` held just before `lsn`, `None` once dropped or retired
    fn present_before(&self, oid: Oid, lsn: u64) -> Option<Arc<RelDescriptor>> {
        self.log
            .predecessor_before(oid, lsn)?
            .value
            .present()
            .cloned()
    }

    fn marker_for(&self, rfn: RelFileNode) -> Option<u64> {
        self.markers.lock().expect("smgr markers poisoned").get(rfn)
    }
}

/// Whether skipping a command read leaves no row of database `db` without
/// its descriptor. Rows of a filenode `owner` cannot place count, since they
/// may belong to a relation this transaction created or rewrote
fn no_row_needs(info: &BoundaryInfo, db: Oid, owner: impl Fn(RelFileNode) -> Option<Oid>) -> bool {
    let (Some(before), Some(after)) = (&info.preceding_rows, &info.following_rows) else {
        return false;
    };
    let mut rows = before.iter().chain(after).filter(|r| r.db_node == db);
    if info.capture_all {
        return rows.next().is_none();
    }
    // TOAST rows ride with a row on the owning heap
    let named: Vec<Oid> = info.oids.iter().map(|a| a.oid).collect();
    rows.all(|rfn| owner(*rfn).is_some_and(|oid| !named.contains(&oid)))
}

/// Shadow cannot rewind to retry a read at an earlier replay position.
fn degrade_reason(err: &CatalogError) -> DegradeReason {
    match err {
        CatalogError::Bridge(BridgeError::ReplayMismatch { .. }) => DegradeReason::ReplayMismatch,
        _ => DegradeReason::QueryError,
    }
}

/// Added / Changed for heap kinds; toast shape changes are internal (chunk
/// layout is fixed), only its Dropped feeds the retire ledger. Partition
/// trees also report `ATTACH` / `DETACH` and identity or persistence
/// changes, which decide whether a partition may route to its root
fn diff_event(pred: Option<&RelDescriptor>, desc: &Arc<RelDescriptor>) -> Option<SchemaEvent> {
    if desc.kind == b't' {
        return None;
    }
    match pred {
        None => Some(SchemaEvent::Added { desc: desc.clone() }),
        Some(old) => {
            let diff = compute_schema_diff(old, desc);
            let tree = old.is_partition() || desc.is_partition() || desc.kind == b'p';
            let relinked = tree
                && (old.parent_oid() != desc.parent_oid()
                    || old.replident != desc.replident
                    || old.persistence != desc.persistence);
            (!diff.is_empty() || relinked).then(|| SchemaEvent::Changed {
                old: Arc::new(old.clone()),
                new: desc.clone(),
                diff,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{RelAttr, RelName, ReplIdent};

    fn rel(type_oid: u32, type_len: i16, name: &str) -> RelDescriptor {
        RelDescriptor {
            rfn: RelFileNode {
                spc_node: 1663,
                db_node: 5,
                rel_node: 7000,
            },
            oid: 42,
            toast_oid: 0,
            namespace_oid: 2200,
            rel_name: RelName::new("public", name),
            kind: b'r',
            persistence: b'p',
            ancestors: Vec::new(),
            replident: ReplIdent::Default { pk_attnums: None },
            attributes: vec![RelAttr {
                attnum: 1,
                name: "c1".into(),
                type_oid,
                typmod: -1,
                not_null: false,
                dropped: false,
                type_name: "t".into(),
                type_byval: true,
                type_len,
                type_align: b'i',
                type_storage: b'p',
                missing_default: None,
            }],
        }
    }

    /// `ATTACH` and identity changes alter no column, yet decide routing
    #[test]
    fn partition_relinks_surface_as_changed() {
        let root = Arc::new(RelDescriptor {
            kind: b'p',
            ..rel(23, 4, "m")
        });
        let standalone = rel(23, 4, "t");
        let attached = Arc::new(RelDescriptor {
            ancestors: vec![root.as_ancestor()],
            ..standalone.clone()
        });
        assert!(matches!(
            diff_event(Some(&standalone), &attached),
            Some(SchemaEvent::Changed { diff, .. }) if diff.is_empty()
        ));
        assert!(matches!(
            diff_event(Some(&attached), &Arc::new(standalone.clone())),
            Some(SchemaEvent::Changed { .. })
        ));
        assert!(diff_event(Some(&attached), &attached).is_none());
        let full = Arc::new(RelDescriptor {
            replident: crate::schema::ReplIdent::Full { pk_attnums: None },
            ..(*attached).clone()
        });
        assert!(diff_event(Some(&attached), &full).is_some());
        let unlogged = Arc::new(RelDescriptor {
            persistence: b'u',
            ..(*root).clone()
        });
        assert!(diff_event(Some(&root), &unlogged).is_some());
        let plain_full = Arc::new(RelDescriptor {
            replident: crate::schema::ReplIdent::Full { pk_attnums: None },
            ..standalone.clone()
        });
        assert!(
            diff_event(Some(&standalone), &plain_full).is_none(),
            "outside partition trees identity stays silent"
        );
    }

    #[test]
    fn skipped_read_needs_rows_of_named_or_unknown_relations() {
        use crate::record::AffectedOid;
        let rfn = |db, rel_node| RelFileNode {
            spc_node: 1663,
            db_node: db,
            rel_node,
        };
        let owner = |r: RelFileNode| match r.rel_node {
            7000 => Some(42),
            7001 => Some(43),
            _ => None,
        };
        let info =
            |before: Option<Vec<RelFileNode>>, after: Option<Vec<RelFileNode>>| BoundaryInfo {
                oids: vec![AffectedOid {
                    oid: 42,
                    pg_class_touch: None,
                }],
                kind: BoundaryKind::Command { writer_xid: 7 },
                preceding_rows: before,
                following_rows: after,
                ..Default::default()
            };
        let skip = |b: &BoundaryInfo| no_row_needs(b, 5, owner);
        assert!(skip(&info(Some(vec![]), Some(vec![]))));
        assert!(!skip(&info(None, Some(vec![]))), "command WAL unknown");
        assert!(!skip(&info(Some(vec![]), None)), "no covering read found");
        assert!(skip(&info(
            Some(vec![rfn(5, 7001)]),
            Some(vec![rfn(6, 7000)])
        )));
        assert!(
            !skip(&info(Some(vec![]), Some(vec![rfn(5, 7000)]))),
            "named"
        );
        assert!(
            !skip(&info(Some(vec![rfn(5, 7002)]), Some(vec![]))),
            "unknown"
        );
        let all = BoundaryInfo {
            capture_all: true,
            ..info(Some(vec![rfn(6, 7001)]), Some(vec![]))
        };
        assert!(skip(&all), "another database's rows");
        let all = BoundaryInfo {
            capture_all: true,
            ..info(Some(vec![]), Some(vec![rfn(5, 7001)]))
        };
        assert!(!skip(&all), "names may change for any relation");
    }

    #[test]
    fn degrade_reason_separates_replay_mismatch_from_the_rest() {
        assert_eq!(
            degrade_reason(&CatalogError::Bridge(BridgeError::ReplayMismatch {
                expected: 1,
                start: 2,
                end: 3,
            })),
            DegradeReason::ReplayMismatch,
        );
        assert_eq!(
            degrade_reason(&CatalogError::Parse("nope".into())),
            DegradeReason::QueryError,
        );
    }
}

/// Read catalog changes through each database's shadow connection
/// Database OID zero requests capture for all databases
/// An empty set skips capture without pausing WAL processing
#[derive(Default)]
pub struct CaptureSet {
    by_db: Vec<(Oid, CatalogCapture)>,
}

impl CaptureSet {
    pub fn insert(&mut self, db: Oid, capture: CatalogCapture) {
        self.by_db.push((db, capture));
    }

    pub fn is_empty(&self) -> bool {
        self.by_db.is_empty()
    }

    /// Each database counts its own captures, which `database=` labels on
    /// the metric surface
    pub fn stats_handles(&self) -> impl Iterator<Item = (Oid, Arc<CaptureStats>)> + '_ {
        self.by_db
            .iter()
            .map(|(db, capture)| (*db, capture.stats_handle()))
    }

    /// Select capture instances for affected databases
    fn scoped(&self, db_oid: Oid) -> impl Iterator<Item = &CatalogCapture> {
        self.by_db
            .iter()
            .filter(move |(db, _)| db_oid == 0 || *db == db_oid)
            .map(|(_, capture)| capture)
    }

    /// Pause WAL processing if any affected database needs catalog reads
    pub fn admits(&self, info: &BoundaryInfo, next_lsn: u64) -> bool {
        self.scoped(info.db_oid)
            .any(|capture| capture.admits(info, next_lsn))
    }

    pub fn count_hold(&self, info: &BoundaryInfo, held: Duration) {
        for capture in self.scoped(info.db_oid) {
            capture.count_hold(info, held);
        }
    }

    #[cfg(feature = "test-support")]
    pub fn fail_command_read_after(&self, reads: u32) {
        for (_, capture) in &self.by_db {
            capture.fail_command_read_after(reads);
        }
    }

    /// Transaction IDs are unique across databases, clear shared pending state once
    pub fn forget_aborted(&self, members: &[u32]) {
        for (_, capture) in &self.by_db {
            capture.forget_aborted(members);
        }
    }

    pub async fn capture_boundary(
        &self,
        info: &BoundaryInfo,
        commit_lsn: u64,
        parked: &ParkedAt,
    ) -> Result<(), SinkError> {
        for capture in self.scoped(info.db_oid) {
            capture.capture_boundary(info, commit_lsn, parked).await?;
        }
        Ok(())
    }

    pub async fn cover_unparked(
        &self,
        info: &BoundaryInfo,
        commit_lsn: u64,
        next_lsn: u64,
    ) -> Result<(), SinkError> {
        for capture in self.scoped(info.db_oid) {
            capture.cover_unparked(info, commit_lsn, next_lsn).await?;
        }
        Ok(())
    }
}
