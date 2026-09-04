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
//! skip entirely. A miss queries shadow; the returned replay position must
//! equal `next_lsn` (nothing past the commit has published during the hold),
//! anything else means the log lost coverage — fatal.
//!
//! valid_from bias-early: a descriptor is a backward-compatible reader of
//! older tuples, never the reverse. Rotated filenode → the rfn's
//! `XLOG_SMGR_CREATE` marker (before any page write); in-place change → the
//! oid's first pg_class touch in the xact; fallback the xact tree's first
//! catalog touch. Dropped tombstones at `next_lsn`.
//!
//! Bias-early holds when the final descriptor provably reads the whole
//! dirty interval, and when the only drift is one `catalog::compat` calls
//! benign — declared shape changed, every byte reads the same. A physically
//! unproven in-place transition publishes an `Ambiguity` over
//! `[first_touch, next_lsn)` instead and lands its `Present` at `next_lsn`:
//! post-commit rows decode, interval rows fail closed per record at the
//! drain ([`crate::xact::xact_buffer::resolve_stash`]). One interval per
//! identity key (rfn, oid) so both lookup paths see the same fence.
//! Rotations skip the check: the rewrite emits final-layout tuples and
//! superseded-generation rows retire with the old rfn. Fresh generations
//! have no covered predecessor to compare.
//!
//! Command boundaries ([`crate::record::BoundaryKind::Command`]) sample the
//! same relations mid-transaction, off the writing transaction's own
//! uncommitted rows, into
//! [`PendingCatalog`]. Nothing there
//! is durable until this xact's commit folds those slots into the batch at
//! their own positions, which is also what shrinks the fence: promoted slots
//! are exact shapes at exact positions, so the ambiguity survives only over
//! the run before the first of them, and rows past it decode.

use std::sync::Arc;
use std::time::Duration;

use tokio_postgres::types::Oid;
use walrus::pg::walparser::RelFileNode;

use crate::catalog::compat::Incompat;
use crate::catalog::desc_log::{
    Ambiguity, AmbiguityReason, AmbiguityScope, BatchRecord, DescriptorLog, LogEntry, LogValue,
    ObservationKind, RelationObservation,
};
use crate::catalog::pending::{DegradeReason, PendingCatalog, PendingSlot};
use crate::catalog::shadow_catalog::{CatalogError, ShadowCatalog};
use crate::filter::SmgrMarkers;
use crate::ops::bridge::BridgeError;
use crate::record::{BoundaryInfo, BoundaryKind, SinkError};
use crate::schema::{RelDescriptor, SchemaEvent, compute_schema_diff};
use crate::xact::xact_buffer::XactBuffer;
use ahash::{HashMap, HashMapExt};

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
        /// Capture-all boundaries (whole-relcache inval or unenumerated
        /// catalog write)
        pub capture_all_runs,
        pub events_added,
        pub events_changed,
        pub events_dropped,
        /// Ambiguity intervals published for unproven in-place changes
        pub ambiguities_published,
        /// Unproven in-place changes whose whole interval the pending
        /// timeline answered for, so the commit published no fence
        pub ambiguities_suppressed,
        /// Command boundaries captured into the pending timeline
        pub pending_captures,
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
        /// Transactions degraded to commit-time capture, by reason
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

/// Cost controls on command-boundary capture. Every refusal degrades the
/// transaction to commit-time capture, which is sound
#[derive(Debug, Clone, Copy)]
pub struct PendingCaptureConfig {
    /// Boundaries one transaction may hold for
    pub max_boundaries_per_xact: u32,
    /// Cumulative parked time one transaction may cost
    pub max_hold_per_xact: Duration,
}

impl Default for PendingCaptureConfig {
    fn default() -> Self {
        Self {
            max_boundaries_per_xact: 64,
            max_hold_per_xact: Duration::from_secs(10),
        }
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
    pending_cfg: PendingCaptureConfig,
    stats: Arc<CaptureStats>,
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
        pending_cfg: PendingCaptureConfig,
    ) -> Self {
        Self {
            log,
            catalog,
            buffer,
            markers,
            pending,
            pending_cfg,
            stats: Arc::new(CaptureStats::default()),
        }
    }

    pub fn stats_handle(&self) -> Arc<CaptureStats> {
        self.stats.clone()
    }

    /// Whether the pump should park for this boundary. A commit always
    /// bounds; a command boundary the caps or the inval set rule out costs
    /// nothing and is skipped before the hold
    pub fn admits(&self, info: &BoundaryInfo, next_lsn: u64) -> bool {
        let BoundaryKind::Command { .. } = info.kind else {
            return true;
        };
        // Aligned-prefix re-read: shadow replayed past this point long ago,
        // so the scan could only answer for where replay is now. Degrades
        // rather than skips silently — an xact straddling the horizon would
        // otherwise claim coverage over boundaries nothing read
        if next_lsn <= self.log.covered_through() {
            if self
                .pending
                .degrade(info.drain_xid, DegradeReason::ReplayMismatch)
            {
                self.stats.count_degrade(DegradeReason::ReplayMismatch);
            }
            return false;
        }
        // Whole-relcache flush or a namespace catcache hit names no
        // relations, and a full catalog scan per command is the one shape
        // that makes holds expensive
        if info.capture_all {
            if self
                .pending
                .degrade(info.drain_xid, DegradeReason::CaptureAll)
            {
                self.stats.count_degrade(DegradeReason::CaptureAll);
            }
            return false;
        }
        if info.oids.is_empty() {
            return false;
        }
        match self.pending.admit(
            info.drain_xid,
            self.pending_cfg.max_boundaries_per_xact,
            self.pending_cfg.max_hold_per_xact.as_nanos() as u64,
        ) {
            Ok(()) => true,
            Err(reason) => {
                if let Some(reason) = reason {
                    self.stats.count_degrade(reason);
                }
                false
            }
        }
    }

    /// Charge a released command-boundary hold against the transaction's
    /// cumulative budget
    pub fn charge_hold(&self, info: &BoundaryInfo, held: Duration) {
        use std::sync::atomic::Ordering::Relaxed;
        let BoundaryKind::Command { .. } = info.kind else {
            return;
        };
        let nanos = held.as_nanos() as u64;
        self.stats.pending_holds.fetch_add(1, Relaxed);
        self.stats.pending_hold_nanos.fetch_add(nanos, Relaxed);
        self.pending.charge_hold(info.drain_xid, nanos);
    }

    /// Aborted tree: its speculative slots die here, on the pump, before any
    /// later boundary could promote them
    pub fn forget_aborted(&self, members: &[u32]) {
        use std::sync::atomic::Ordering::Relaxed;
        let dropped = self.pending.forget_members(members);
        if dropped > 0 {
            self.stats
                .pending_entries_dropped_abort
                .fetch_add(dropped as u64, Relaxed);
        }
    }

    pub async fn capture_boundary(
        &self,
        info: &BoundaryInfo,
        commit_lsn: u64,
        next_lsn: u64,
    ) -> Result<(), SinkError> {
        use std::sync::atomic::Ordering::Relaxed;
        let start = std::time::Instant::now();
        if let BoundaryKind::Command { writer_xid } = info.kind {
            let out = self.capture_command(info, writer_xid, next_lsn).await;
            self.stats
                .capture_nanos
                .fetch_add(start.elapsed().as_nanos() as u64, Relaxed);
            return out;
        }
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
        } else {
            self.sql_capture(info, commit_lsn, next_lsn).await?
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
    /// One event per oid: promoted command-boundary entries are intermediate
    /// versions of the same commit, and capture always appends the commit
    /// shape after them, so the oid's last non-`Retired` entry is the shape
    /// CH is told about.
    fn replay_events(&self, batch: &BatchRecord) -> Vec<PendingEvent> {
        let mut last_for_oid: HashMap<Oid, usize> = HashMap::with_capacity(batch.entries.len());
        for (at, entry) in batch.entries.iter().enumerate() {
            if !matches!(entry.value, LogValue::Retired) {
                last_for_oid.insert(entry.oid, at);
            }
        }
        let mut out = Vec::with_capacity(last_for_oid.len());
        for (at, entry) in batch.entries.iter().enumerate() {
            if last_for_oid.get(&entry.oid) != Some(&at) {
                continue;
            }
            let pred = self.log.predecessor_before(entry.oid, batch.captured_at);
            let pred_desc = pred.as_ref().and_then(|p| match &p.value {
                LogValue::Present(d) => Some(d.clone()),
                _ => None,
            });
            match &entry.value {
                LogValue::Present(desc) => {
                    if let Some(event) = diff_event(pred_desc.as_deref(), desc) {
                        out.push(PendingEvent {
                            lsn: entry.valid_from,
                            event,
                        });
                    }
                }
                LogValue::Dropped => {
                    if let Some(old) = pred_desc {
                        out.push(PendingEvent {
                            lsn: entry.valid_from,
                            event: SchemaEvent::Dropped {
                                oid: entry.oid,
                                rel_name: old.rel_name.clone(),
                            },
                        });
                    }
                }
                LogValue::Retired => {}
            }
        }
        out
    }

    /// One command boundary: the shapes the writing transaction sees at
    /// `next_lsn`, where shadow's replay is parked. Best effort — every
    /// failure degrades the transaction to commit-time capture instead of
    /// poisoning the stream
    async fn capture_command(
        &self,
        info: &BoundaryInfo,
        writer_xid: u32,
        next_lsn: u64,
    ) -> Result<(), SinkError> {
        use std::sync::atomic::Ordering::Relaxed;
        let top_xid = info.drain_xid;
        let oids: Vec<Oid> = info.oids.iter().map(|a| a.oid).collect();
        let read = {
            let mut cat = self.catalog.lock().await;
            cat.fetch_overlay_descriptors(&oids, top_xid, next_lsn)
                .await
        };
        let descs = match read {
            Ok(descs) => descs,
            Err(e) => {
                let reason = degrade_reason(&e);
                if self.pending.degrade(top_xid, reason) {
                    self.stats.count_degrade(reason);
                }
                tracing::warn!(
                    target: "walshadow::desc_log",
                    xid = top_xid,
                    boundary = format_args!("{next_lsn:#X}"),
                    reason = reason.label(),
                    err = %e,
                    "pending capture degraded, commit-time capture stands",
                );
                return Ok(());
            }
        };
        let slots: Vec<PendingSlot> = descs
            .into_iter()
            .filter(|d| matches!(d.kind, 'r' | 'p' | 'm' | 't'))
            .map(|desc| {
                // A relation born in this xact cannot have changed layout
                // before its first command boundary, and its rows start at
                // the smgr create — the exact lower bound `CREATE TABLE AS`
                // needs, where one command writes storage, fills it, and
                // only then hits its CCI. Only the first shape seen on that
                // generation may claim the marker; a later boundary
                // answering from it would bury the earlier one
                let pred = self.log.predecessor_before(desc.oid, next_lsn);
                let pred_rfn = pred.as_ref().and_then(|p| match &p.value {
                    LogValue::Present(d) => Some(d.rfn),
                    _ => None,
                });
                let first_seen =
                    pred_rfn != Some(desc.rfn) && !self.pending.has_slots(top_xid, desc.rfn);
                let valid_from = first_seen
                    .then(|| self.marker_for(desc.rfn))
                    .flatten()
                    .unwrap_or(next_lsn);
                PendingSlot {
                    valid_from,
                    writer_xid,
                    desc: Arc::new(desc),
                }
            })
            .collect();
        self.stats.pending_captures.fetch_add(1, Relaxed);
        self.stats
            .pending_rels
            .fetch_add(slots.len() as u64, Relaxed);
        self.pending.record(top_xid, slots);
        Ok(())
    }

    async fn sql_capture(
        &self,
        info: &BoundaryInfo,
        commit_lsn: u64,
        next_lsn: u64,
    ) -> Result<Vec<PendingEvent>, SinkError> {
        use std::sync::atomic::Ordering::Relaxed;
        self.stats.sql_captures.fetch_add(1, Relaxed);
        let (replay_lsn, descs) = {
            let mut cat = self.catalog.lock().await;
            if info.capture_all {
                self.stats.capture_all_runs.fetch_add(1, Relaxed);
                cat.fetch_all_descriptors().await
            } else {
                let oids: Vec<Oid> = info.oids.iter().map(|a| a.oid).collect();
                cat.fetch_descriptors_batch(&oids).await
            }
        }
        .map_err(|e| SinkError::Other(format!("descriptor capture at {commit_lsn:#X}: {e}")))?;
        // Hold guarantees apply >= next_lsn; nothing past the commit has
        // published, so equality is the only sane reading. Ahead = this
        // boundary replayed into shadow without a stored batch: the log
        // lost coverage (wiped/foreign spill dir), decode would misread
        if replay_lsn != next_lsn {
            return Err(SinkError::Other(format!(
                "shadow replay {replay_lsn:#X} != boundary next_lsn {next_lsn:#X}: \
                 descriptor log lost coverage; re-bootstrap or --ignore-cursor",
            )));
        }
        self.stats
            .rels_captured
            .fetch_add(descs.len() as u64, Relaxed);

        let fetched: HashMap<Oid, RelDescriptor> = descs
            .into_iter()
            .filter(|d| matches!(d.kind, 'r' | 'p' | 'm' | 't'))
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

        // Command boundaries this xact held for. Slots are exact shapes at
        // exact positions, so they promote even from a degraded xact; what
        // degradation costs is the coverage claim, not the evidence
        let promoted = self.pending.promoted(info.drain_xid);

        let mut entries: Vec<Arc<LogEntry>> = Vec::new();
        let mut ambiguities: Vec<Arc<Ambiguity>> = Vec::new();
        for oid in expected {
            let pred = self.log.predecessor_before(oid, next_lsn);
            let pred_desc = pred.as_ref().and_then(|p| match &p.value {
                LogValue::Present(d) => Some(d.clone()),
                _ => None,
            });
            match fetched.get(&oid) {
                Some(desc) => {
                    // Full physical identity: SET TABLESPACE changes spc
                    // alongside rel_node, and rel_node reuse across
                    // tablespaces must not read as "same filenode"
                    let rotated = pred_desc.as_ref().is_some_and(|old| old.rfn != desc.rfn);
                    let fresh = pred_desc.is_none();
                    // New generation: rows cannot precede the smgr create,
                    // the marker is an exact lower bound; in-place keeps
                    // the pg_class-touch bias-early bound
                    let marker = if rotated || fresh {
                        self.marker_for(desc.rfn)
                    } else {
                        None
                    };
                    let first_touch = marker
                        .or_else(|| pg_class_touch.get(&oid).copied())
                        .unwrap_or(info.tree_first_touch);
                    if let Some(m) = marker {
                        observations.push(RelationObservation {
                            oid: Some(oid),
                            rfn: Some(desc.rfn),
                            first_touch_lsn: first_touch,
                            smgr_create_lsn: Some(m),
                            kind: ObservationKind::SmgrCreate,
                        });
                    }
                    if rotated && let Some(old) = &pred_desc {
                        entries.push(Arc::new(LogEntry {
                            valid_from: first_touch,
                            oid,
                            rfn: old.rfn,
                            value: LogValue::Retired,
                        }));
                    }
                    let slots = promoted.as_ref().map_or(&[][..], |p| p.slots_for(oid));
                    let changed = pred_desc.as_deref() != Some(desc);
                    // A slot-carrying oid always lands its commit shape too,
                    // even unchanged: the batch's last entry for an oid is
                    // what event derivation reads, and an intermediate
                    // version is not what CH is told about
                    if changed || !slots.is_empty() {
                        // In-place transition must prove the final
                        // descriptor reads the whole dirty interval; a
                        // rotation's rewrite emits final-layout tuples and
                        // a fresh generation has no covered predecessor
                        let mut valid_from = first_touch;
                        if changed
                            && !rotated
                            && let Some(pred) = pred_desc.as_deref()
                        {
                            // The timeline answers from the first boundary
                            // that named the relation on, so only the run
                            // up to it stays fenced. Rows there predate the
                            // transaction's first `CommandCounterIncrement`
                            // and so were written under the predecessor,
                            // which is what the fence protects: the exact
                            // mutation position inside that run is still
                            // unknown
                            let covered_from = promoted.as_ref().and_then(|p| p.coverage_from(oid));
                            let (from, published, incompat) = in_place_verdict(
                                pred,
                                desc,
                                oid,
                                first_touch,
                                next_lsn,
                                covered_from,
                            );
                            valid_from = from;
                            if let Some(why) = incompat.filter(|i| !i.is_physical()) {
                                tracing::debug!(
                                    target: "walshadow::desc_log",
                                    oid,
                                    rel = %desc.rel_name,
                                    why = why.why(),
                                    "in-place change reads identically, bias-early kept",
                                );
                            }
                            if published.is_empty()
                                && covered_from.is_some()
                                && incompat.is_some_and(|i| i.is_physical())
                            {
                                self.stats.ambiguities_suppressed.fetch_add(1, Relaxed);
                            } else if !published.is_empty() {
                                tracing::warn!(
                                    target: "walshadow::desc_log",
                                    oid,
                                    rel = %desc.rel_name,
                                    from = format_args!("{first_touch:#X}"),
                                    through = format_args!("{next_lsn:#X}"),
                                    why = incompat.map_or("", |i| i.why()),
                                    "in-place change not provably decodable, ambiguity published",
                                );
                                // One verdict, one count: siblings name the
                                // same event under different keys
                                self.stats.ambiguities_published.fetch_add(1, Relaxed);
                                ambiguities.extend(published.into_iter().map(Arc::new));
                            }
                        }
                        // Promoted first, commit shape last: the batch reads
                        // as the transaction's own sequence of shapes
                        let mut prev = pred_desc.as_deref();
                        for slot in slots {
                            if prev == Some(slot.desc.as_ref()) {
                                continue;
                            }
                            prev = Some(slot.desc.as_ref());
                            entries.push(Arc::new(LogEntry {
                                valid_from: slot.valid_from,
                                oid,
                                rfn: slot.desc.rfn,
                                value: LogValue::Present(slot.desc.clone()),
                            }));
                            self.stats.pending_entries_promoted.fetch_add(1, Relaxed);
                        }
                        let desc = Arc::new(desc.clone());
                        entries.push(Arc::new(LogEntry {
                            valid_from,
                            oid,
                            rfn: desc.rfn,
                            value: LogValue::Present(desc),
                        }));
                    }
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
        self.log
            .append_batch(batch.clone())
            .await
            .map_err(|e| SinkError::Other(format!("descriptor log append: {e}")))?;
        // Events off the appended batch, the same derivation a restart runs:
        // one boundary cannot mean two schema histories.
        // `predecessor_before` reads strictly older batches, so the append
        // above is invisible to it
        Ok(self.replay_events(&batch))
    }

    /// Markers key physical WAL locators; descriptor rfns are resolved to
    /// physical at capture — match on full identity
    fn marker_for(&self, rfn: RelFileNode) -> Option<u64> {
        self.markers.lock().expect("smgr markers poisoned").get(rfn)
    }
}

/// Why an overlay read failed, as the transaction's degrade reason. A
/// replay position other than the boundary is its own case: shadow cannot
/// rewind, so nothing later re-reads that point
fn degrade_reason(err: &CatalogError) -> DegradeReason {
    match err {
        CatalogError::Bridge(BridgeError::ReplayMismatch { .. }) => DegradeReason::ReplayMismatch,
        _ => DegradeReason::QueryError,
    }
}

/// Entry LSN for an in-place final version + the ambiguities covering the
/// dirty interval when the final descriptor is not a proven reader of it.
/// First pg_class touch bounds the interval; exact change positions inside
/// stay unknown (only the first touch is tracked). Half-open end: the final
/// version answers from `next_lsn`, keeping the post-commit descriptor
/// usable over the ambiguous interval.
///
/// `covered_from` is where this transaction's own timeline starts answering
/// for the relation, which ends the fence there: the promoted slots are
/// exact shapes at exact positions, so only the run before the first of them
/// has an unknown mutation position left in it. An empty run publishes
/// nothing.
///
/// A benign reject (declared shape drifted, every byte reads the same)
/// keeps bias-early and publishes nothing. A physical one publishes one
/// interval per identity key so the rfn-keyed decode path and the oid-keyed
/// truncate path agree; order is fixed, batch equality and digest are
/// order-sensitive
fn in_place_verdict(
    pred: &RelDescriptor,
    fin: &RelDescriptor,
    oid: Oid,
    first_touch: u64,
    next_lsn: u64,
    covered_from: Option<u64>,
) -> (u64, Vec<Ambiguity>, Option<Incompat>) {
    let Err(incompat) = crate::catalog::compat::compatible_reader(pred, fin) else {
        return (first_touch, Vec::new(), None);
    };
    if !incompat.is_physical() {
        return (first_touch, Vec::new(), Some(incompat));
    }
    let through_lsn = covered_from.unwrap_or(next_lsn).min(next_lsn);
    if through_lsn <= first_touch {
        return (next_lsn, Vec::new(), Some(incompat));
    }
    let interval = |scope| Ambiguity {
        scope,
        from_lsn: first_touch,
        through_lsn,
        reason: AmbiguityReason::UnknownMutationPosition,
    };
    (
        next_lsn,
        vec![
            interval(AmbiguityScope::Rfn(fin.rfn)),
            interval(AmbiguityScope::Oid(oid)),
        ],
        Some(incompat),
    )
}

/// Added / Changed for heap kinds; toast shape changes are internal (chunk
/// layout is fixed), only its Dropped feeds the retire ledger.
fn diff_event(pred: Option<&RelDescriptor>, desc: &Arc<RelDescriptor>) -> Option<SchemaEvent> {
    if desc.kind == 't' {
        return None;
    }
    match pred {
        None => Some(SchemaEvent::Added { desc: desc.clone() }),
        Some(old) => {
            let diff = compute_schema_diff(old, desc);
            (!diff.is_empty()).then(|| SchemaEvent::Changed {
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
            kind: 'r',
            persistence: 'p',
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
                type_align: 'i',
                type_storage: 'p',
                missing_text: None,
            }],
        }
    }

    #[test]
    fn compatible_in_place_keeps_bias_early() {
        let pred = rel(23, 4, "t");
        let fin = rel(23, 4, "renamed");
        let (from, published, incompat) = in_place_verdict(&pred, &fin, 42, 100, 500, None);
        assert_eq!(from, 100);
        assert!(published.is_empty());
        assert_eq!(incompat, None);
    }

    #[test]
    fn benign_in_place_keeps_bias_early() {
        let pred = rel(1043, -1, "t");
        let mut fin = rel(1043, -1, "t");
        fin.attributes[0].typmod = 24;
        let (from, published, incompat) = in_place_verdict(&pred, &fin, 42, 100, 500, None);
        assert_eq!(from, 100, "read-identical drift keeps bias-early");
        assert!(published.is_empty(), "no fence for a benign reject");
        assert_eq!(incompat, Some(Incompat::Benign("typmod change")));
    }

    #[test]
    fn physical_in_place_publishes_rfn_and_oid_siblings() {
        let pred = rel(23, 4, "t");
        let fin = rel(20, 8, "t");
        let (from, published, incompat) = in_place_verdict(&pred, &fin, 42, 100, 500, None);
        assert_eq!(from, 500, "final version serves post-commit rows only");
        assert!(incompat.is_some_and(|i| i.is_physical()));
        // Fixed order: batch equality and digest gate append idempotency
        let scopes: Vec<AmbiguityScope> = published.iter().map(|a| a.scope).collect();
        assert_eq!(
            scopes,
            vec![AmbiguityScope::Rfn(fin.rfn), AmbiguityScope::Oid(42)]
        );
        for amb in &published {
            assert_eq!(amb.from_lsn, 100);
            assert_eq!(amb.through_lsn, 500);
            assert_eq!(amb.reason, AmbiguityReason::UnknownMutationPosition);
        }
    }

    #[test]
    fn pending_coverage_shrinks_the_fence_to_the_pre_boundary_run() {
        let pred = rel(23, 4, "t");
        let fin = rel(20, 8, "t");
        let (from, published, _) = in_place_verdict(&pred, &fin, 42, 100, 500, Some(200));
        assert_eq!(from, 500);
        assert_eq!(published.len(), 2, "both identity keys still filed");
        for amb in &published {
            assert_eq!(amb.from_lsn, 100);
            assert_eq!(
                amb.through_lsn, 200,
                "timeline answers from the first boundary on",
            );
        }
    }

    #[test]
    fn coverage_from_the_first_touch_publishes_nothing() {
        let pred = rel(23, 4, "t");
        let fin = rel(20, 8, "t");
        let (from, published, incompat) = in_place_verdict(&pred, &fin, 42, 100, 500, Some(100));
        assert_eq!(from, 500);
        assert!(published.is_empty(), "empty run needs no fence");
        assert!(incompat.is_some_and(|i| i.is_physical()));
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
