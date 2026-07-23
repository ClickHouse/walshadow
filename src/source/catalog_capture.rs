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
//! Bias-early holds only when the final descriptor provably reads the whole
//! dirty interval (`catalog::compat`). An unproven in-place transition
//! publishes an `Ambiguity` over `[first_touch, next_lsn)` instead and
//! lands its `Present` at `next_lsn` — post-commit rows decode, interval
//! rows fail closed. Rotations skip the check: the rewrite emits
//! final-layout tuples and superseded-generation rows retire with the old
//! rfn. Fresh generations have no covered predecessor to compare.

use std::collections::HashMap;
use std::sync::Arc;

use tokio_postgres::types::Oid;
use walrus::pg::walparser::RelFileNode;

use crate::catalog::desc_log::{
    Ambiguity, AmbiguityReason, AmbiguityScope, BatchRecord, DescriptorLog, LogEntry, LogValue,
    ObservationKind, RelationObservation,
};
use crate::catalog::shadow_catalog::ShadowCatalog;
use crate::filter::SmgrMarkers;
use crate::record::{BoundaryInfo, SinkError};
use crate::schema::{RelDescriptor, SchemaEvent, compute_schema_diff};
use crate::xact::xact_buffer::XactBuffer;

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
        /// Capture duration, nanos (inside the boundary hold)
        pub capture_nanos,
    }
}

pub struct CatalogCapture {
    log: Arc<DescriptorLog>,
    catalog: Arc<tokio::sync::Mutex<ShadowCatalog>>,
    buffer: Arc<tokio::sync::Mutex<XactBuffer>>,
    markers: Arc<std::sync::Mutex<SmgrMarkers>>,
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
    ) -> Self {
        Self {
            log,
            catalog,
            buffer,
            markers,
            stats: Arc::new(CaptureStats::default()),
        }
    }

    pub fn stats_handle(&self) -> Arc<CaptureStats> {
        self.stats.clone()
    }

    pub async fn capture_boundary(
        &self,
        info: &BoundaryInfo,
        commit_lsn: u64,
        next_lsn: u64,
    ) -> Result<(), SinkError> {
        use std::sync::atomic::Ordering::Relaxed;
        let start = std::time::Instant::now();
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

    /// Derive a stored batch's events against each oid's historical
    /// predecessor (never the loaded head — boot loads the whole log
    /// before the WAL re-read).
    fn replay_events(&self, batch: &BatchRecord) -> Vec<PendingEvent> {
        let mut out = Vec::new();
        for entry in &batch.entries {
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

        let mut entries: Vec<Arc<LogEntry>> = Vec::new();
        let mut ambiguities: Vec<Arc<Ambiguity>> = Vec::new();
        let mut events: Vec<PendingEvent> = Vec::new();
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
                    let changed = pred_desc.as_deref() != Some(desc);
                    if changed {
                        // In-place transition must prove the final
                        // descriptor reads the whole dirty interval; a
                        // rotation's rewrite emits final-layout tuples and
                        // a fresh generation has no covered predecessor
                        let mut valid_from = first_touch;
                        if !rotated && let Some(pred) = pred_desc.as_deref() {
                            let (from, ambiguity) =
                                in_place_verdict(pred, desc, first_touch, next_lsn);
                            valid_from = from;
                            if let Some((amb, why)) = ambiguity {
                                tracing::warn!(
                                    target: "walshadow::desc_log",
                                    oid,
                                    rel = %desc.rel_name,
                                    from = format_args!("{:#X}", amb.from_lsn),
                                    through = format_args!("{:#X}", amb.through_lsn),
                                    why,
                                    "in-place change not provably decodable, ambiguity published",
                                );
                                ambiguities.push(Arc::new(amb));
                                self.stats.ambiguities_published.fetch_add(1, Relaxed);
                            }
                        }
                        let desc = Arc::new(desc.clone());
                        entries.push(Arc::new(LogEntry {
                            valid_from,
                            oid,
                            rfn: desc.rfn,
                            value: LogValue::Present(desc.clone()),
                        }));
                        if let Some(event) = diff_event(pred_desc.as_deref(), &desc) {
                            events.push(PendingEvent {
                                lsn: valid_from,
                                event,
                            });
                        }
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
                    events.push(PendingEvent {
                        lsn: next_lsn,
                        event: SchemaEvent::Dropped {
                            oid,
                            rel_name: old.rel_name.clone(),
                        },
                    });
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
        self.log
            .append_batch(BatchRecord {
                captured_at: next_lsn,
                commit_lsn,
                observations,
                ambiguities,
                entries,
            })
            .await
            .map_err(|e| SinkError::Other(format!("descriptor log append: {e}")))?;
        Ok(events)
    }

    /// Markers key physical WAL locators; descriptor rfns are resolved to
    /// physical at capture — match on full identity
    fn marker_for(&self, rfn: RelFileNode) -> Option<u64> {
        self.markers.lock().expect("smgr markers poisoned").get(rfn)
    }
}

/// Entry LSN for an in-place final version + the ambiguity covering the
/// dirty interval when the final descriptor is not a proven reader of it.
/// First pg_class touch bounds the interval; exact change positions inside
/// stay unknown (only the first touch is tracked). Half-open end: the final
/// version answers from `next_lsn`, keeping the post-commit descriptor
/// usable over the ambiguous interval
fn in_place_verdict(
    pred: &RelDescriptor,
    fin: &RelDescriptor,
    first_touch: u64,
    next_lsn: u64,
) -> (u64, Option<(Ambiguity, &'static str)>) {
    match crate::catalog::compat::compatible_reader(pred, fin) {
        Ok(()) => (first_touch, None),
        Err(why) => (
            next_lsn,
            Some((
                Ambiguity {
                    scope: AmbiguityScope::Rfn(fin.rfn),
                    from_lsn: first_touch,
                    through_lsn: next_lsn,
                    reason: AmbiguityReason::UnknownMutationPosition,
                },
                why,
            )),
        ),
    }
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
        let (from, amb) = in_place_verdict(&pred, &fin, 100, 500);
        assert_eq!(from, 100);
        assert!(amb.is_none());
    }

    #[test]
    fn incompatible_in_place_publishes_interval() {
        let pred = rel(23, 4, "t");
        let fin = rel(20, 8, "t");
        let (from, amb) = in_place_verdict(&pred, &fin, 100, 500);
        assert_eq!(from, 500, "final version serves post-commit rows only");
        let (amb, why) = amb.expect("ambiguity for type change");
        assert_eq!(amb.scope, AmbiguityScope::Rfn(fin.rfn));
        assert_eq!(amb.from_lsn, 100);
        assert_eq!(amb.through_lsn, 500);
        assert_eq!(amb.reason, AmbiguityReason::UnknownMutationPosition);
        assert!(!why.is_empty());
    }
}
