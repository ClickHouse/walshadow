//! Publish step for the metrics endpoint: gathers pump, emitter, shadow, and
//! bootstrap readings into one snapshot per status tick.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use walshadow::backfill_bootstrap::BootstrapProgress;
use walshadow::boundary_hold::BoundaryHoldStats;
use walshadow::ch_emitter::EmitterStats;
use walshadow::config::ConfigResolver;
use walshadow::metrics::{DbSeries, MetricsRegistry, MetricsSnapshot};
use walshadow::pos::{
    Drain, EmitterAck, FilterDispatched, Floor, Pos, ShadowReplay, SourceReceived,
};
use walshadow::record::MetricsRecordSink;
use walshadow::transition::{CrossingWedge, TimelineStats};

use crate::source_recovery::{PromotionGate, SOURCE_SWAP_RETRY};

/// Shadow-side numbers for the metrics publish step, from
/// [`ShadowStreamState::aggregate`](walshadow::shadow_stream::ShadowStreamState::aggregate)
/// + the daemon's [`RateEstimator`](walshadow::metrics::RateEstimator).
pub(crate) struct ShadowMetricsView {
    pub(crate) apply_lag_bytes: u64,
    pub(crate) apply_lag_seconds: f64,
    pub(crate) active_connections: u64,
    pub(crate) dropped_total: u64,
}

/// Live `[source]` endpoint moves the pump has yet to reach
#[derive(Default)]
pub(crate) struct SourceSwap {
    /// Config names an endpoint the pump has not reached yet
    pub(crate) pending: bool,
    pub(crate) retry_at: Option<Instant>,
    pub(crate) swaps: u64,
    pub(crate) failures: u64,
    /// Proof the last attempt failed on, empty once one lands
    pub(crate) blocked_on: &'static str,
}

impl SourceSwap {
    pub(crate) fn requested(&mut self) {
        self.pending = true;
        self.retry_at = None;
    }

    pub(crate) fn due(&self, now: Instant) -> bool {
        self.pending && self.retry_at.is_none_or(|at| now >= at)
    }

    pub(crate) fn failed(&mut self, reason: &'static str) {
        self.failures += 1;
        self.retry_at = Some(Instant::now() + SOURCE_SWAP_RETRY);
        self.blocked_on = reason;
    }

    /// A fresh feed dialed the live endpoint, nothing left to swap
    pub(crate) fn settled(&mut self) {
        self.pending = false;
        self.retry_at = None;
        self.blocked_on = "";
    }
}

/// Branch selection plus the frozen pause frontier — everything a switchover
/// decision reads (architecture/recovery.md).
pub(crate) struct TimelineView {
    pub(crate) source_system_id: u64,
    /// Branch the pump is reading
    pub(crate) source_timeline: u32,
    /// Branch owning the durable floor, which restart resumes on
    pub(crate) floor_timeline: u32,
    /// Branch the shadow-facing walsender advertises
    pub(crate) shadow_served_timeline: u32,
    /// Branch the shadow is replaying
    pub(crate) shadow_replay_timeline: u32,
    pub(crate) floor_lsn: Pos<Floor>,
    pub(crate) stats: TimelineStats,
    /// `(consumed, received)` frozen when the pump observed a pause
    pub(crate) pause_frontier: Option<(u64, u64)>,
    /// That freeze re-derived a pause this process found already in effect
    pub(crate) pause_refrozen: bool,
    /// Crossing the pump parked on, waiting for an operator
    pub(crate) wedge: Option<CrossingWedge>,
    pub(crate) promotion: PromotionGate,
}

#[allow(clippy::too_many_arguments)]
/// CPU seconds, RSS bytes and threads from `/proc/self`. Zero if
/// unreadable. Assumes `CLK_TCK` 100 (USER_HZ) and `VmRSS` in kB.
pub(crate) fn read_process_stats() -> (f64, u64, u64) {
    const CLK_TCK: f64 = 100.0;
    let cpu = std::fs::read_to_string("/proc/self/stat")
        .ok()
        .and_then(|s| {
            // Split after the last ')' (comm may hold spaces/parens): utime
            // (field 14) and stime (15) are then indices 11 and 12.
            let rest = s.rsplit_once(')')?.1;
            let f: Vec<&str> = rest.split_whitespace().collect();
            let utime: u64 = f.get(11)?.parse().ok()?;
            let stime: u64 = f.get(12)?.parse().ok()?;
            Some((utime + stime) as f64 / CLK_TCK)
        })
        .unwrap_or(0.0);
    let status = std::fs::read_to_string("/proc/self/status").unwrap_or_default();
    let field = |name: &str| walshadow::budget::proc_field(&status, name).unwrap_or(0);
    (cpu, field("VmRSS:") * 1024, field("Threads:"))
}

/// Stats handles one source database owns. A scrape renders each as its own
/// `database=` labelled series, so a quiet database reads as quiet rather
/// than hiding inside a cluster total
pub(crate) struct DbMetricSources {
    pub(crate) database: String,
    /// Live bridge first, bootstrap's throwaway oracle second. Both count
    /// into the database bootstrap restored, so a handoff is not a reset
    pub(crate) bridge: [Option<Arc<walshadow::bridge::BridgeStats>>; 2],
    pub(crate) desc_log: Option<Arc<walshadow::desc_log::DescriptorLog>>,
    pub(crate) capture: Option<Arc<walshadow::catalog_capture::CaptureStats>>,
    pub(crate) resolver: Option<Arc<ConfigResolver>>,
    pub(crate) backfiller: Option<Arc<walshadow::copy_backfill::CopyBackfiller>>,
}

impl DbMetricSources {
    /// Zero rather than absent for a handle this phase has not built yet,
    /// which reads the same as not yet counted
    pub(crate) fn series(&self) -> DbSeries {
        use walshadow::bridge::{BridgeStats, OP_COUNT};
        use walshadow::catalog_capture::CaptureStats;
        use walshadow::desc_log::DescLogStats;
        let ld = |a: &AtomicU64| a.load(Ordering::Relaxed);
        let bridge = |pick: fn(&BridgeStats) -> &AtomicU64| -> u64 {
            self.bridge.iter().flatten().map(|s| ld(pick(s))).sum()
        };
        let bridge_ops = |pick: fn(&BridgeStats) -> &[AtomicU64; OP_COUNT]| -> [u64; OP_COUNT] {
            std::array::from_fn(|i| self.bridge.iter().flatten().map(|s| ld(&pick(s)[i])).sum())
        };
        let bridge_op_seconds = |pick: fn(&BridgeStats) -> &[AtomicU64; OP_COUNT]| {
            bridge_ops(pick).map(|nanos| nanos as f64 / 1e9)
        };
        let log_stats = self.desc_log.as_ref().map(|log| log.stats_handle());
        let log = |pick: fn(&DescLogStats) -> &AtomicU64| -> u64 {
            log_stats.as_ref().map_or(0, |s| ld(pick(s)))
        };
        let (log_entries, log_tail_bytes, log_batches) =
            self.desc_log.as_ref().map_or((0, 0, 0), |log| log.gauges());
        let cap = |pick: fn(&CaptureStats) -> &AtomicU64| -> u64 {
            self.capture.as_ref().map_or(0, |s| ld(pick(s)))
        };
        let cap_seconds = |pick: fn(&CaptureStats) -> &AtomicU64| -> f64 { cap(pick) as f64 / 1e9 };
        let resolver = self.resolver.as_ref();
        let backfiller = self.backfiller.as_ref();
        DbSeries {
            database: self.database.clone(),
            // Gauge, so it answers off whichever bridge is serving:
            // bootstrap's oracle socket is gone by the time the live bridge
            // dials
            bridge_up: self.bridge.iter().flatten().next().map_or(0, |s| ld(&s.up)),
            bridge_requests_by_op: bridge_ops(|s| &s.requests),
            bridge_errors_by_op: bridge_ops(|s| &s.errors),
            bridge_request_seconds_by_op: bridge_op_seconds(|s| &s.request_nanos),
            bridge_lock_wait_seconds_by_op: bridge_op_seconds(|s| &s.lock_wait_nanos),
            bridge_service_seconds_by_op: bridge_op_seconds(|s| &s.service_nanos),
            bridge_request_bytes_by_op: bridge_ops(|s| &s.request_bytes),
            bridge_response_bytes_by_op: bridge_ops(|s| &s.response_bytes),
            bridge_reconnects_total: bridge(|s| &s.reconnects),
            bridge_scan_rows_total: bridge(|s| &s.scan_rows),
            bridge_scan_subtrans_mismatch_total: bridge(|s| &s.scan_subtrans_mismatch),
            bridge_scan_replay_moved_total: bridge(|s| &s.scan_replay_moved),
            bridge_native_bytes_total: bridge(|s| &s.native_bytes),
            desc_capture_sql_total: cap(|s| &s.sql_captures),
            desc_capture_log_replay_total: cap(|s| &s.log_replays),
            desc_capture_skipped_covered_total: cap(|s| &s.skipped_covered),
            desc_capture_all_total: cap(|s| &s.capture_all_runs),
            desc_capture_rels_total: cap(|s| &s.rels_captured),
            desc_capture_seconds_total: cap_seconds(|s| &s.capture_nanos),
            desc_events_added_total: cap(|s| &s.events_added),
            desc_events_changed_total: cap(|s| &s.events_changed),
            desc_events_dropped_total: cap(|s| &s.events_dropped),
            descriptor_ambiguous_total: cap(|s| &s.ambiguities_published),
            pending_captures_total: cap(|s| &s.pending_captures),
            pending_rels_total: cap(|s| &s.pending_rels),
            pending_holds_total: cap(|s| &s.pending_holds),
            pending_hold_seconds_total: cap_seconds(|s| &s.pending_hold_nanos),
            pending_entries_promoted_total: cap(|s| &s.pending_entries_promoted),
            pending_entries_dropped_abort_total: cap(|s| &s.pending_entries_dropped_abort),
            pending_ambiguities_suppressed_total: cap(|s| &s.ambiguities_suppressed),
            pending_degraded_by_reason: std::array::from_fn(|i| {
                self.capture
                    .as_ref()
                    .map_or(0, |s| ld(&s.pending_degraded[i]))
            }),
            desc_log_entries: log_entries,
            desc_log_tail_bytes: log_tail_bytes,
            desc_log_batches: log_batches,
            desc_log_gc_total: log(|s| &s.gc_runs),
            desc_log_gc_dropped_entries_total: log(|s| &s.gc_dropped_entries),
            desc_lookups_present_total: log(|s| &s.lookups_present),
            desc_lookups_dropped_total: log(|s| &s.lookups_dropped),
            desc_lookups_retired_total: log(|s| &s.lookups_retired),
            desc_lookups_ambiguous_total: log(|s| &s.lookups_ambiguous),
            desc_lookups_not_covered_total: log(|s| &s.lookups_not_covered),
            desc_lookups_foreign_db_total: log(|s| &s.lookups_foreign_db),
            config_pending_decl_rels: resolver.map_or(0, |r| r.pending_decl_count()),
            config_replicate_opt_in_total: resolver.map_or(0, |r| r.opt_in_total()),
            config_replicate_opt_out_total: resolver.map_or(0, |r| r.opt_out_total()),
            config_backfills_pending: backfiller.map_or(0, |b| b.pending_count()),
            config_backfills_pending_by_mode: backfiller.map_or([0; 3], |b| b.pending_by_mode()),
        }
    }
}

/// Drain-resident + spool gauge readings taken under one buffer lock
pub(crate) struct DrainResident {
    pub(crate) total: u64,
    pub(crate) chunks: u64,
    pub(crate) rows: u64,
    pub(crate) spool: u64,
    pub(crate) raw_pending_rows: u64,
    pub(crate) raw_pending_bytes: u64,
}

impl DrainResident {
    pub(crate) fn from_buffer(b: &walshadow::xact_buffer::XactBuffer) -> Self {
        Self {
            total: b.drain_resident_bytes(),
            chunks: b.drain_chunk_resident_bytes(),
            rows: b.drain_row_resident_bytes(),
            spool: b.toast_spool_bytes(),
            raw_pending_rows: b.raw_pending_rows(),
            raw_pending_bytes: b.raw_pending_bytes(),
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn populate_metrics(
    registry: &MetricsRegistry,
    source_received_lsn: Pos<SourceReceived>,
    filter_lsn: Pos<FilterDispatched>,
    shadow_replay_lsn: Pos<ShadowReplay>,
    decoder_commit_lsn: Pos<Drain>,
    emitter_ack_lsn: Pos<EmitterAck>,
    rec_metrics: &MetricsRecordSink,
    pump_queue_depth: u64,
    queue_records_out_total: u64,
    xact_stats: &walshadow::xact_buffer::XactBufferStats,
    drain_resident: DrainResident,
    budget: Option<&walshadow::budget::MemoryBudget>,
    decoder_stats: &walshadow::decoder_sink::DecoderStats,
    source_swap: &SourceSwap,
    timeline_view: TimelineView,
    shadow_view: ShadowMetricsView,
    boundary_hold: &BoundaryHoldStats,
    by_database: Vec<DbSeries>,
    counters: StageCounters<'_>,
) {
    let base = MetricsSnapshot {
        source_received_lsn,
        filter_lsn,
        shadow_replay_lsn,
        decoder_commit_lsn,
        emitter_ack_lsn,
        source_endpoint_swaps_total: source_swap.swaps,
        source_endpoint_swap_failures_total: source_swap.failures,
        source_endpoint_swap_pending: u64::from(source_swap.pending),
        source_endpoint_swap_blocked_on: source_swap.blocked_on,
        crossing_blocked_on: timeline_view.wedge.as_ref().map_or("", |w| w.reason),
        crossing_detail: timeline_view.wedge.map(|w| w.detail).unwrap_or_default(),
        source_system_id: timeline_view.source_system_id,
        source_timeline: timeline_view.source_timeline,
        floor_timeline: timeline_view.floor_timeline,
        shadow_served_timeline: timeline_view.shadow_served_timeline,
        shadow_replay_timeline: timeline_view.shadow_replay_timeline,
        floor_lsn: timeline_view.floor_lsn,
        timeline_switches_total: timeline_view.stats.switches,
        timeline_switch_failures_by_reason: timeline_view.stats.failures_by_reason,
        timeline_switch_lsn: timeline_view.stats.switch_lsn,
        timeline_prefix_bytes_verified_total: timeline_view.stats.prefix_bytes_verified,
        timeline_transition_seconds_total: timeline_view.stats.seconds_total,
        pause_consumed_lsn: timeline_view.pause_frontier.map_or(0, |(c, _)| c),
        pause_received_lsn: timeline_view.pause_frontier.map_or(0, |(_, r)| r),
        pause_refrozen: timeline_view.pause_refrozen,
        promotion_ready: timeline_view.promotion.ready,
        promotion_blocked_on: timeline_view.promotion.blocked_on,
        promotion_target_in_recovery: timeline_view.promotion.in_recovery,
        promotion_target_replay_lsn: timeline_view.promotion.replay_lsn,
        promotion_target_receive_lsn: timeline_view.promotion.receive_lsn,
        shadow_apply_lag_bytes: shadow_view.apply_lag_bytes,
        shadow_apply_lag_seconds: shadow_view.apply_lag_seconds,
        shadow_stream_active_connections: shadow_view.active_connections,
        shadow_stream_dropped_connections_total: shadow_view.dropped_total,
        ..registry.snapshot().await
    };
    populate_pipeline_metrics(
        registry,
        base,
        PipelineMetrics {
            rec_metrics,
            pump_queue_depth,
            queue_records_out_total,
            xact_stats,
            drain_resident,
            budget,
            decoder_stats,
            boundary_hold,
            by_database,
            counters,
        },
    )
    .await;
}

pub(crate) struct PipelineMetrics<'a> {
    pub(crate) rec_metrics: &'a MetricsRecordSink,
    pub(crate) pump_queue_depth: u64,
    pub(crate) queue_records_out_total: u64,
    pub(crate) xact_stats: &'a walshadow::xact_buffer::XactBufferStats,
    pub(crate) drain_resident: DrainResident,
    pub(crate) budget: Option<&'a walshadow::budget::MemoryBudget>,
    pub(crate) decoder_stats: &'a walshadow::decoder_sink::DecoderStats,
    pub(crate) boundary_hold: &'a BoundaryHoldStats,
    pub(crate) by_database: Vec<DbSeries>,
    pub(crate) counters: StageCounters<'a>,
}

pub(crate) async fn populate_pipeline_metrics(
    registry: &MetricsRegistry,
    base: MetricsSnapshot,
    pipeline: PipelineMetrics<'_>,
) {
    let PipelineMetrics {
        rec_metrics,
        pump_queue_depth,
        queue_records_out_total,
        xact_stats,
        drain_resident,
        budget,
        decoder_stats,
        boundary_hold,
        by_database,
        counters,
    } = pipeline;
    use std::collections::BTreeMap;
    use walshadow::record::rmgr_label;
    let mut by_rm = BTreeMap::new();
    for ((rm, route), n) in &rec_metrics.by_rm_route {
        let key = (
            rmgr_label(*rm).to_string(),
            match route {
                walshadow::record::Route::ToShadow => "to_shadow",
                walshadow::record::Route::ToDecoder => "to_decoder",
                walshadow::record::Route::ToBoth => "to_both",
            },
        );
        by_rm.insert(key, *n);
    }
    let snap = MetricsSnapshot {
        records_by_rm_route: by_rm,
        xact_active: xact_stats.xacts_active,
        xact_bytes_in_memory: xact_stats.bytes_in_memory,
        spill_xacts_active: xact_stats.spill_xacts_active,
        spill_bytes_active: xact_stats.spill_bytes_active,
        drain_resident_bytes: drain_resident.total,
        drain_chunk_resident_bytes: drain_resident.chunks,
        drain_row_resident_bytes: drain_resident.rows,
        toast_xact_spool_bytes: drain_resident.spool,
        resident_payload_bytes: budget.map(|b| b.resident_bytes()).unwrap_or(0),
        resident_payload_peak_bytes: budget.map(|b| b.peak_bytes()).unwrap_or(0),
        memory_budget_waits_total: budget.map(|b| b.waits_total()).unwrap_or(0),
        memory_budget_overshoots_total: budget.map(|b| b.overshoots_total()).unwrap_or(0),
        memory_budget_big_leaf_waits_total: budget.map(|b| b.big_leaf_waits_total()).unwrap_or(0),
        spill_evictions_total: xact_stats.spill_evictions_total,
        xacts_committed_total: xact_stats.committed_xacts_total,
        xacts_aborted_total: xact_stats.aborted_xacts_total,
        decoder_decoded_total: decoder_stats.decoded.load(Ordering::Relaxed),
        decoder_partial_total: decoder_stats.partial.load(Ordering::Relaxed),
        decoder_toast_chunks_total: decoder_stats.toast_chunks_buffered.load(Ordering::Relaxed),
        decoder_toast_malformed_total: decoder_stats.toast_chunks_malformed.load(Ordering::Relaxed),
        decoder_toast_deletes_total: decoder_stats.toast_chunk_deletes.load(Ordering::Relaxed),
        toast_stash_buffered_total: decoder_stats.toast_stash_buffered.load(Ordering::Relaxed),
        raw_stash_deferred_total: decoder_stats.raw_stash_deferred.load(Ordering::Relaxed),
        raw_stash_records_by_kind_op: [
            decoder_stats.raw_stash_dirty_ops.load(),
            decoder_stats.raw_stash_marker_ops.load(),
        ],
        raw_stash_bytes_by_storage: [
            xact_stats.raw_stash_bytes_mem,
            xact_stats.raw_stash_bytes_spill,
        ],
        raw_pending_rows: drain_resident.raw_pending_rows,
        raw_pending_bytes: drain_resident.raw_pending_bytes,
        pump_queue_depth,
        queue_records_out_total,
        catalog_boundary_holds_total: boundary_hold.holds.load(Ordering::Relaxed),
        catalog_boundary_hold_failures_total: boundary_hold.failures.load(Ordering::Relaxed),
        catalog_boundary_hold_seconds_total: boundary_hold.hold_seconds_total(),
        // `ctl status` reports one number for the daemon; the scrape
        // splits it by database
        config_backfills_pending: by_database
            .iter()
            .map(|db| db.config_backfills_pending)
            .sum(),
        by_database,
        ..stage_gauges_on(&counters, base)
    };
    registry.set(snap).await;
}

/// Counters both phases write. Bootstrap runs its own insert tail and oracle
/// PG before a status loop exists, so its ticker publishes this group beside
/// pump progress and the series carry across handoff rather than reading as a
/// reset: the emitter handle is shared, and the two oracle bridges' cumulative
/// counters sum into one series
pub(crate) struct StageCounters<'a> {
    pub(crate) emitter: Option<&'a walshadow::ch_emitter::EmitterStats>,
    /// Live first, bootstrap's second. Only one of the pair is ever serving
    pub(crate) oracle: [Option<&'a walshadow::oracle::OracleStats>; 2],
    pub(crate) bootstrap: Option<&'a BootstrapProgress>,
    pub(crate) bootstrap_attempt: u32,
    pub(crate) uptime_secs: u64,
}

/// Zero rather than absent when no emitter is serving: the phase has not
/// started, which reads the same as not yet counted
pub(crate) fn emitter_counts<const N: usize>(
    stats: Option<&EmitterStats>,
    picks: [fn(&EmitterStats) -> &AtomicU64; N],
) -> [u64; N] {
    picks.map(|pick| stats.map_or(0, |s| pick(s).load(Ordering::Relaxed)))
}

pub(crate) fn stage_gauges(v: &StageCounters<'_>) -> MetricsSnapshot {
    stage_gauges_on(v, MetricsSnapshot::default())
}

pub(crate) fn stage_gauges_on(v: &StageCounters<'_>, base: MetricsSnapshot) -> MetricsSnapshot {
    let (proc_cpu, proc_rss, proc_threads) = read_process_stats();
    let emitter = |pick: fn(&EmitterStats) -> &AtomicU64| -> u64 {
        v.emitter.map_or(0, |s| pick(s).load(Ordering::Relaxed))
    };
    let emitter_seconds =
        |pick: fn(&EmitterStats) -> &AtomicU64| -> f64 { emitter(pick) as f64 / 1e9 };
    let emitter_ops =
        |pick: fn(&EmitterStats) -> &walshadow::decode::heap_decoder::OpCounters| -> [u64; 7] {
            v.emitter.map_or([0; 7], |s| pick(s).load())
        };
    let oracle = |pick: fn(&walshadow::oracle::OracleStats) -> &AtomicU64| -> u64 {
        v.oracle
            .iter()
            .flatten()
            .map(|s| pick(s).load(Ordering::Relaxed))
            .sum()
    };
    MetricsSnapshot {
        bootstrap_deferred_bytes: emitter(|s| &s.bootstrap_deferred_bytes),
        bootstrap_deferred_spool_bytes: emitter(|s| &s.bootstrap_deferred_spool_bytes),
        bootstrap_deferred_replay_bytes: emitter(|s| &s.bootstrap_deferred_replay_bytes),
        bootstrap_deferred_replayed_bytes: emitter(|s| &s.bootstrap_deferred_replayed_bytes),
        pending_rows_total: emitter(|s| &s.pending_rows),
        pending_tables_total: emitter(|s| &s.pending_tables),
        pending_tables_dropped_total: emitter(|s| &s.pending_tables_dropped),
        pending_xacts_settled_total: emitter(|s| &s.pending_xacts_settled),
        pending_outstanding_xids: emitter(|s| &s.pending_outstanding_xids),
        pending_undecidable_xids: emitter(|s| &s.pending_undecidable_xids),
        toast_chunk_puts_total: emitter(|s| &s.toast_chunk_puts),
        toast_chunk_put_seconds: emitter_seconds(|s| &s.toast_chunk_put_nanos),
        toast_chunks_stored_total: emitter(|s| &s.toast_chunks_stored),
        toast_tombstones_stored_total: emitter(|s| &s.toast_tombstones_stored),
        toast_values_fetched_total: emitter(|s| &s.toast_values_fetched),
        toast_value_fetch_batches_total: emitter(|s| &s.toast_value_fetch_batches),
        toast_value_fetch_seconds: emitter_seconds(|s| &s.toast_value_fetch_nanos),
        toast_image_rows_mirrored_total: emitter(|s| &s.toast_image_rows_mirrored),
        toast_values_filled_superseded_total: emitter(|s| &s.toast_values_filled_superseded),
        toast_values_filled_mismatch_total: emitter(|s| &s.toast_values_filled_mismatch),
        toast_values_filled_generation_total: emitter(|s| &s.toast_values_filled_generation),
        toast_values_filled_oversize_total: emitter(|s| &s.toast_values_filled_oversize),
        toast_mirror_truncates_total: emitter(|s| &s.toast_mirror_truncates),
        toast_mirror_retires_total: emitter(|s| &s.toast_mirror_retires),
        toast_rewrite_barriers_total: emitter(|s| &s.toast_rewrite_barriers),
        toast_stash_decoded_total: emitter(|s| &s.toast_stash_decoded),
        toast_stash_discarded_total: emitter(|s| &s.toast_stash_discarded),
        toast_stash_in_place_total: emitter(|s| &s.toast_stash_in_place),
        stash_foreign_db_skipped_total: emitter(|s| &s.stash_foreign_db_skipped),
        xact_plan_rows: emitter(|s| &s.plan_rows),
        xact_plan_bytes_by_storage: emitter_counts(
            v.emitter,
            [|s| &s.plan_bytes_mem, |s| &s.plan_bytes_file],
        ),
        xact_plan_failures_by_reason: emitter_counts(
            v.emitter,
            [
                |s| &s.plan_failures_spool,
                |s| &s.plan_failures_fail_closed_image_only,
                |s| &s.plan_failures_fail_closed_malformed,
                |s| &s.plan_failures_fail_closed_unsupported_op,
                |s| &s.plan_failures_stash_ambiguous,
                |s| &s.plan_failures_incomplete_toast,
                |s| &s.plan_failures_missing_stash_resolution,
                |s| &s.plan_failures_detoast,
                |s| &s.plan_failures_partial_update,
                |s| &s.plan_failures_view,
                |s| &s.plan_failures_drain,
            ],
        ),
        route_snapshots_by_result: emitter_counts(
            v.emitter,
            [
                |s| &s.route_snapshots_mapped,
                |s| &s.route_snapshots_unmapped,
            ],
        ),
        raw_decode_records_by_kind_op: [
            emitter_ops(|s| &s.raw_decode_toast_ops),
            emitter_ops(|s| &s.raw_decode_ordinary_ops),
        ],
        raw_decode_rows_by_op: emitter_ops(|s| &s.raw_decode_rows_ops),
        emitter_rows_total: emitter(|s| &s.rows_emitted),
        backfill_backup_rows_total: emitter(|s| &s.backfill_backup_walk.tuples_emitted),
        backfill_backup_bytes_total: emitter(|s| &s.backfill_backup_pump.bytes_tapped),
        backfill_copy_rows_total: emitter(|s| &s.backfill_copy_rows),
        backfill_copy_bytes_total: emitter(|s| &s.backfill_copy_bytes),
        emitter_blocks_total: emitter(|s| &s.blocks_sent),
        queue_jobs_out_total: emitter(|s| &s.queue_jobs_out),
        decode_jobs_in_total: emitter(|s| &s.decode_jobs_in),
        decode_rows_out_total: emitter(|s| &s.decode_rows_out),
        insertbatch_rows_in_total: emitter(|s| &s.insertbatch_rows_in),
        insertbatch_batches_out_total: emitter(|s| &s.insertbatch_batches_out),
        inserter_batches_in_total: emitter(|s| &s.inserter_batches_in),
        inserter_ch_seconds_total: emitter_seconds(|s| &s.inserter_ch_nanos),
        inserter_encode_seconds_total: emitter_seconds(|s| &s.inserter_encode_nanos),
        oracle_resolve_seconds_total: emitter_seconds(|s| &s.oracle_resolve_nanos),
        process_cpu_seconds_total: proc_cpu,
        process_resident_memory_bytes: proc_rss,
        process_threads: proc_threads,
        emitter_xacts_total: emitter(|s| &s.xacts_committed),
        emitter_unsupported_relations: emitter(|s| &s.unsupported_relations),
        emitter_deletes_discarded: emitter(|s| &s.deletes_discarded),
        oracle_local_columns_total: emitter(|s| &s.oracle_local_columns),
        oracle_blocks_total: oracle(|s| &s.blocks),
        oracle_rows_total: oracle(|s| &s.rows),
        oracle_cells_total: oracle(|s| &s.cells),
        oracle_conversion_errors_total: oracle(|s| &s.conversion_errors),
        oracle_errors_total: oracle(|s| &s.errors),
        uptime_seconds: v.uptime_secs,
        bootstrap_attempt: v.bootstrap_attempt,
        ..bootstrap_gauges(v.bootstrap, base)
    }
}

/// Bootstrap stage attribution, frozen at its final values once the pump
/// returns. Rendered for the whole session so a slow initial load stays
/// attributable after the fact
pub(crate) fn bootstrap_gauges(
    progress: Option<&BootstrapProgress>,
    base: MetricsSnapshot,
) -> MetricsSnapshot {
    let Some(p) = progress else {
        return base;
    };
    let ld = |a: &AtomicU64| a.load(Ordering::Relaxed);
    MetricsSnapshot {
        bootstrap_parts_total: ld(&p.pump.parts_total),
        bootstrap_parts_done: ld(&p.pump.parts_done),
        bootstrap_bytes_tapped: ld(&p.pump.bytes_tapped),
        bootstrap_pages_walked: ld(&p.page_walk.pages_walked),
        bootstrap_tuples_emitted: ld(&p.page_walk.tuples_emitted),
        bootstrap_files_walked: ld(&p.page_walk.files_walked),
        bootstrap_files_skipped_unmapped: ld(&p.page_walk.files_skipped_unmapped),
        bootstrap_decode_seconds: ld(&p.page_walk.decode_nanos) as f64 / 1e9,
        bootstrap_tap_seconds: ld(&p.pump.sink_chunk_nanos) as f64 / 1e9,
        bootstrap_channel_block_seconds: ld(&p.page_walk.channel_block_nanos) as f64 / 1e9,
        ..base
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A partial publish from inside the leg must not blank the fields the
    /// status loop owns, or the leg would look like a dead pipeline
    #[tokio::test]
    async fn metrics_update_keeps_fields_it_does_not_touch() {
        let registry = MetricsRegistry::new();
        registry
            .set(MetricsSnapshot {
                emitter_rows_total: 17,
                ..MetricsSnapshot::default()
            })
            .await;
        registry
            .update(|snap| snap.archive_restore_active = 1)
            .await;
        let snap = registry.snapshot().await;
        assert_eq!(snap.emitter_rows_total, 17);
        assert_eq!(snap.archive_restore_active, 1);
    }

    #[tokio::test]
    async fn pipeline_metrics_refresh_during_archive_recovery() {
        use walshadow::record::WAL_SEG_SIZE;
        let registry = MetricsRegistry::new();
        registry
            .set(MetricsSnapshot {
                archive_restore_active: 1,
                archive_wal_segments_total: 42,
                filter_lsn: Pos::new(2 * WAL_SEG_SIZE),
                ..MetricsSnapshot::default()
            })
            .await;
        let records = MetricsRecordSink::default();
        let decoder = walshadow::decoder_sink::DecoderStats::default();
        let emitter = EmitterStats::default();
        let boundary = BoundaryHoldStats::default();
        for n in [7, 13] {
            decoder.decoded.store(n, Ordering::Relaxed);
            emitter.rows_emitted.store(n * 2, Ordering::Relaxed);
            let xacts = walshadow::xact_buffer::XactBufferStats {
                xacts_active: n,
                ..Default::default()
            };
            populate_pipeline_metrics(
                &registry,
                registry.snapshot().await,
                PipelineMetrics {
                    rec_metrics: &records,
                    pump_queue_depth: n,
                    queue_records_out_total: n * 3,
                    xact_stats: &xacts,
                    drain_resident: DrainResident {
                        total: n * 10,
                        chunks: 0,
                        rows: n * 10,
                        spool: 0,
                        raw_pending_rows: n,
                        raw_pending_bytes: n * 10,
                    },
                    budget: None,
                    decoder_stats: &decoder,
                    boundary_hold: &boundary,
                    by_database: Vec::new(),
                    counters: StageCounters {
                        emitter: Some(&emitter),
                        oracle: [None, None],
                        bootstrap: None,
                        bootstrap_attempt: 0,
                        uptime_secs: n,
                    },
                },
            )
            .await;
            let snap = registry.snapshot().await;
            assert_eq!(snap.decoder_decoded_total, n);
            assert_eq!(snap.emitter_rows_total, n * 2);
            assert_eq!(snap.xact_active, n);
            assert_eq!(snap.pump_queue_depth, n);
            assert_eq!(snap.queue_records_out_total, n * 3);
            assert_eq!(snap.drain_resident_bytes, n * 10);
            assert_eq!(snap.raw_pending_rows, n);
            assert_eq!(snap.uptime_seconds, n);
            assert_eq!(snap.archive_restore_active, 1);
            assert_eq!(snap.archive_wal_segments_total, 42);
            assert_eq!(snap.filter_lsn.get(), 2 * WAL_SEG_SIZE);
        }
    }

    #[test]
    fn archive_stage_metrics_refresh_without_resetting_recovery_state() {
        use walshadow::record::WAL_SEG_SIZE;
        let emitter = EmitterStats::default();
        let counters = StageCounters {
            emitter: Some(&emitter),
            oracle: [None, None],
            bootstrap: None,
            bootstrap_attempt: 0,
            uptime_secs: 10,
        };
        let base = MetricsSnapshot {
            archive_restore_active: 1,
            archive_wal_segments_total: 42,
            source_received_lsn: Pos::new(3 * WAL_SEG_SIZE),
            filter_lsn: Pos::new(2 * WAL_SEG_SIZE),
            config_backfills_pending: 21,
            ..MetricsSnapshot::default()
        };
        emitter
            .backfill_backup_walk
            .tuples_emitted
            .store(3, Ordering::Relaxed);
        emitter
            .backfill_backup_pump
            .bytes_tapped
            .store(8192, Ordering::Relaxed);
        emitter.rows_emitted.store(17, Ordering::Relaxed);
        let first = stage_gauges_on(&counters, base);
        assert_eq!(first.emitter_rows_total, 17);
        assert_eq!(first.backfill_backup_rows_total, 3);
        assert_eq!(first.backfill_backup_bytes_total, 8192);
        emitter.rows_emitted.store(29, Ordering::Relaxed);
        emitter.decode_rows_out.store(31, Ordering::Relaxed);
        let next = stage_gauges_on(
            &StageCounters {
                uptime_secs: 20,
                ..counters
            },
            first,
        );
        assert_eq!(next.emitter_rows_total, 29);
        assert_eq!(next.decode_rows_out_total, 31);
        assert_eq!(next.uptime_seconds, 20);
        assert_eq!(next.archive_restore_active, 1);
        assert_eq!(next.archive_wal_segments_total, 42);
        assert_eq!(next.source_received_lsn.get(), 3 * WAL_SEG_SIZE);
        assert_eq!(next.filter_lsn.get(), 2 * WAL_SEG_SIZE);
        assert_eq!(next.config_backfills_pending, 21);
    }

    #[test]
    fn stage_gauges_folds_bootstrap_oracle_into_the_live_series() {
        use walshadow::oracle::OracleStats;
        let (live_oracle, boot_oracle) = (OracleStats::default(), OracleStats::default());
        live_oracle.rows.fetch_add(3, Ordering::Relaxed);
        boot_oracle.rows.fetch_add(7, Ordering::Relaxed);

        let snap = stage_gauges(&StageCounters {
            emitter: None,
            oracle: [Some(&live_oracle), Some(&boot_oracle)],
            bootstrap: None,
            bootstrap_attempt: 2,
            uptime_secs: 11,
        });
        assert_eq!(snap.oracle_rows_total, 10);
        assert_eq!(snap.uptime_seconds, 11);
        assert_eq!(snap.bootstrap_attempt, 2);

        let boot_only = stage_gauges(&StageCounters {
            emitter: None,
            oracle: [None, Some(&boot_oracle)],
            bootstrap: None,
            bootstrap_attempt: 2,
            uptime_secs: 11,
        });
        assert_eq!(boot_only.oracle_rows_total, 7);
    }

    /// Bootstrap's bridge and the live one that replaces it are the same
    /// database's work, so they share its series rather than resetting it
    #[test]
    fn db_series_folds_bootstrap_bridge_into_the_database_it_restored() {
        use walshadow::bridge::{BridgeStats, OP_LABELS};
        let encode = OP_LABELS
            .iter()
            .position(|l| *l == "encode_native")
            .expect("op label");
        let bump = |s: &BridgeStats, n: u64| {
            s.up.store(1, Ordering::Relaxed);
            s.requests[encode].fetch_add(n, Ordering::Relaxed);
            s.native_bytes.fetch_add(n, Ordering::Relaxed);
        };
        let (live, boot) = (
            Arc::new(BridgeStats::default()),
            Arc::new(BridgeStats::default()),
        );
        bump(&live, 2);
        bump(&boot, 5);
        live.up.store(0, Ordering::Relaxed);
        let sources = |bridge| DbMetricSources {
            database: "app".into(),
            bridge,
            desc_log: None,
            capture: None,
            resolver: None,
            backfiller: None,
        };

        let handed_over = sources([Some(live.clone()), Some(boot.clone())]).series();
        assert_eq!(handed_over.database, "app");
        assert_eq!(handed_over.bridge_native_bytes_total, 7);
        assert_eq!(
            handed_over.bridge_requests_by_op[encode], 7,
            "both bridges' requests land on the database's series"
        );
        // Live bridge owns the gauge once it exists, whatever bootstrap left
        assert_eq!(handed_over.bridge_up, 0);

        let boot_only = sources([None, Some(boot)]).series();
        assert_eq!(boot_only.bridge_up, 1, "bootstrap's bridge answers alone");
        assert_eq!(boot_only.bridge_native_bytes_total, 5);
    }
}
