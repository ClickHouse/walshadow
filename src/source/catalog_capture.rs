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

use std::collections::HashMap;
use std::sync::Arc;

use tokio_postgres::types::Oid;
use walrus::pg::walparser::RelFileNode;

use crate::catalog::desc_log::{BatchRecord, DescriptorLog, LogEntry, LogValue};
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

        let mut entries: Vec<Arc<LogEntry>> = Vec::new();
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
                    let valid_from = if rotated || fresh {
                        self.marker_for(desc.rfn)
                            .or_else(|| pg_class_touch.get(&oid).copied())
                            .unwrap_or(info.tree_first_touch)
                    } else {
                        pg_class_touch
                            .get(&oid)
                            .copied()
                            .unwrap_or(info.tree_first_touch)
                    };
                    if rotated && let Some(old) = &pred_desc {
                        entries.push(Arc::new(LogEntry {
                            valid_from,
                            oid,
                            rfn: old.rfn,
                            value: LogValue::Retired,
                        }));
                    }
                    let changed = pred_desc.as_deref() != Some(desc);
                    if changed {
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
        // Zero-entry stub still appends: boot replay must distinguish
        // "captured, no shape change" from "never captured"
        self.log
            .append_batch(BatchRecord {
                captured_at: next_lsn,
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
