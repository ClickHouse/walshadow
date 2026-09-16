//! HTTP/OpenMetrics metrics surface.
//!
//! `/metrics` over plain TCP, encoded by `prometheus-client` as
//! [OpenMetrics text](https://github.com/prometheus/OpenMetrics/blob/v1.0.0/specification/OpenMetrics.md#text-format):
//! counter samples carry `_total`, the body closes with `# EOF`.
//!
//! Declarations live here rather than in a mutable metric registry: values are
//! already atomics elsewhere in the pipeline, so a scrape encodes one owned
//! [`MetricsSnapshot`] through a [`Collector`].
//!
//! Registry is `Arc`-cloneable: daemon's main loop writes at status-tick
//! cadence, HTTP server reads a snapshot per request. Endpoint is read-only by
//! design (no `/quit`, no admin verbs); operator actions stay on the CLI.

use std::collections::{BTreeMap, VecDeque};
use std::fmt;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use prometheus_client::collector::Collector;
use prometheus_client::encoding::{
    DescriptorEncoder, EncodeCounterValue, EncodeGaugeValue, GaugeValueEncoder, MetricEncoder,
    NoLabelSet, text,
};
use prometheus_client::metrics::MetricType;
use prometheus_client::registry::Registry;
use thiserror::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;

use crate::catalog::pending::DegradeReason;
use crate::decode::heap_decoder::HEAP_OP_LABELS;
use crate::ops::bridge::OP_LABELS;
use crate::pos::{Drain, EmitterAck, FilterDispatched, Floor, Pos, ShadowReplay, SourceReceived};
use crate::source::transition::SWITCH_FAILURE_REASONS;

#[derive(Debug, Error)]
pub enum MetricsError {
    #[error("io: {0}")]
    Io(#[from] io::Error),
    #[error("bind {addr}: {source}")]
    Bind { addr: String, source: io::Error },
}

macro_rules! encode_metric {
    ($enc:expr, counter, $name:expr, $help:expr, $value:expr) => {
        counter($enc, $name, $help, $value)?
    };
    ($enc:expr, gauge, $name:expr, $help:expr, $value:expr) => {
        gauge($enc, $name, $help, $value)?
    };
    ($enc:expr, counter, $name:expr, $help:expr, $value:expr, $key:literal, $labels:expr) => {
        counter_series($enc, $name, $help, $key, $labels.into_iter().zip($value))?
    };
    ($enc:expr, gauge, $name:expr, $help:expr, $value:expr, $key:literal, $labels:expr) => {
        gauge_series($enc, $name, $help, $key, $labels.into_iter().zip($value))?
    };
}

/// Declares [`MetricsSnapshot`] and the families a scrape renders off it, so a
/// metric is one entry rather than a field, a doc comment and an encode call.
/// Help text doubles as the field's documentation; `walshadow_` plus the field
/// name is the family name, which [`declare`] trims of the `_by_<label>` tail a
/// `["label" = LABELS]` array carries, and of the `_total` OpenMetrics re-adds
/// to counter samples.
///
/// A `custom` field the table declares but does not encode: either its family
/// carries several labels, a bare sample beside a labelled one, or a value no
/// field holds, so [`encode_snapshot`] writes it by hand, or `ctl status` is
/// its only reader.
macro_rules! snapshot {
    (
        custom { $($(#[doc = $custom_doc:literal])* $custom:ident: $custom_ty:ty,)* }
        $(
            $(#[doc = $doc:literal])*
            $kind:ident $field:ident: $ty:ty $([$key:literal = $labels:expr])? = $help:literal,
        )*
    ) => {
        /// Snapshot of every value `/metrics` renders. Daemon writes per
        /// status-line iteration; HTTP readers take a read lock and serialise.
        #[derive(Debug, Default, Clone)]
        pub struct MetricsSnapshot {
            $($(#[doc = $custom_doc])* pub $custom: $custom_ty,)*
            $(
                #[doc = $help]
                $(#[doc = $doc])*
                pub $field: $ty,
            )*
        }

        fn encode_fields(snap: &MetricsSnapshot, enc: &mut DescriptorEncoder<'_>) -> fmt::Result {
            $(encode_metric!(
                enc,
                $kind,
                concat!("walshadow_", stringify!($field)),
                $help,
                snap.$field
                $(, $key, $labels)?
            );)*
            Ok(())
        }
    };
}

snapshot! {
    custom {
        /// `route` is `"to_shadow"` / `"to_decoder"`
        records_by_rm_route: BTreeMap<(String, &'static str), u64>,
        /// Raw-stash records `[dirty, marker]` x op, rendered `kind=`/`op=`
        /// labelled
        raw_stash_records_by_kind_op: [[u64; 7]; 2],
        /// Commit-resolve raw decode records `[toast, ordinary]` x op
        raw_decode_records_by_kind_op: [[u64; 7]; 2],
        /// `initial_load` backfills recorded in the ledger but not yet
        /// complete (in flight, or awaiting re-run on next boot), rendered
        /// bare and split `[copy, base_backup, object_store]` under one family
        config_backfills_pending: u64,
        config_backfills_pending_by_mode: [u64; 3],
        /// Refusal a crossing parked on, rendered as `crossing_wedged`. The
        /// pump keeps publishing rather than exiting into a restart that
        /// re-crosses and re-fails, so this is how the daemon says it is
        /// waiting for an operator
        crossing_blocked_on: &'static str,
        /// PostgreSQL source system identifier, rendered as a `source_info`
        /// label to hold 64 bits a float64 sample would round
        source_system_id: u64,
        /// Proof the last endpoint swap failed on, from the crossing's own
        /// vocabulary; empty once one lands
        source_endpoint_swap_blocked_on: &'static str,
        /// `crossing_blocked_on`'s own words, which name the pair behind it
        crossing_detail: String,
        /// Both frozen pause numbers were re-derived by this process from a
        /// pause it found already in effect, so a pair read before a restart
        /// is stale
        pause_refrozen: bool,
        /// Every term of the promotion gate holds, so the target may be
        /// promoted
        promotion_ready: bool,
        /// First term that does not, empty once ready
        promotion_blocked_on: &'static str,
        promotion_target_in_recovery: bool,
        /// Target's `pg_last_wal_replay_lsn()`, read while paused
        promotion_target_replay_lsn: u64,
        /// Target's `pg_last_wal_receive_lsn()`, read while paused
        promotion_target_receive_lsn: u64,
    }

    gauge source_received_lsn: Pos<SourceReceived> =
        "Source PG's most recent server_wal_end seen on the replication socket.",
    /// Last segment-boundary LSN dispatched downstream; becomes durable once
    /// segment fsync lands
    gauge filter_lsn: Pos<FilterDispatched> =
        "LSN of the last filtered WAL byte the daemon has dispatched.",
    gauge shadow_replay_lsn: Pos<ShadowReplay> =
        "Shadow PG's pg_last_wal_replay_lsn(), polled at status cadence.",
    gauge decoder_commit_lsn: Pos<Drain> = "Highest commit LSN drained out of the xact buffer.",
    /// Live insert watermark, unlike manifest's resume-safe `emitter_ack`
    gauge emitter_ack_lsn: Pos<EmitterAck> = "Contiguous-done watermark from the insert pipeline.",
    gauge floor_lsn: Pos<Floor> = "Durable resume floor: restart resumes here and pruners cut to it.",
    gauge xact_active: u64 = "Active transactions buffered in memory or on spill.",
    gauge xact_bytes_in_memory: u64 = "Bytes held in memory across all buffered xacts.",
    gauge spill_xacts_active: u64 = "Xacts with at least one entry currently in their spill file.",
    gauge spill_bytes_active: u64 = "Bytes currently held across all active xact spill files.",
    gauge drain_resident_bytes: u64 =
        "Bytes resident inside an active commit drain (heads + chunk generations + mirror rows).",
    gauge drain_chunk_resident_bytes: u64 =
        "Chunk-generation share of drain_resident_bytes, held until consumers drop.",
    gauge drain_row_resident_bytes: u64 =
        "Mirror-row share of drain_resident_bytes, held until store put completes.",
    gauge toast_xact_spool_bytes: u64 =
        "Bytes in transaction TOAST body spool files (disk, not resident).",
    gauge resident_payload_bytes: u64 =
        "Bytes held by live memory-budget permits across pipeline stages.",
    gauge resident_payload_peak_bytes: u64 = "High-water mark of resident payload permit bytes.",
    counter memory_budget_waits_total: u64 = "Budget acquisitions that waited for a release.",
    counter memory_budget_overshoots_total: u64 =
        "Requests above a budget compartment, admitted with only the satisfiable share metered.",
    gauge bootstrap_deferred_bytes: u64 =
        "Resident bytes in the in-memory prefixes of every bootstrap TOAST-deferred spool.",
    gauge bootstrap_deferred_spool_bytes: u64 =
        "Encoded bytes in every bootstrap TOAST-deferred spool file.",
    counter pending_rows_total: u64 =
        "Undecided backup rows written to ClickHouse pending tables.",
    counter pending_tables_total: u64 = "Pending tables created for undecided backup rows.",
    counter pending_tables_dropped_total: u64 =
        "Pending tables dropped after all transaction outcomes were resolved.",
    counter pending_xacts_settled_total: u64 =
        "Transaction outcomes resolved for pending rows.",
    gauge pending_outstanding_xids: u64 =
        "Transaction ids pending rows still wait on.",
    gauge pending_undecidable_xids: u64 =
        "Outstanding transaction ids absent from shadow pg_xact, leaving rows pending.",
    /// Bootstrap pump stage attribution. Live while a greenfield bootstrap
    /// runs, then frozen at its final values for the rest of the session
    counter bootstrap_bytes_tapped: u64 =
        "Backup body bytes the bootstrap pump handed to the page-walk sink.",
    counter bootstrap_pages_walked: u64 = "8 KiB heap pages the bootstrap walk framed.",
    counter bootstrap_tuples_emitted: u64 =
        "Live tuples the bootstrap walk decoded off backup pages.",
    counter bootstrap_files_walked: u64 = "User-heap segments the bootstrap walk decoded.",
    counter bootstrap_files_skipped_unmapped: u64 =
        "User-heap segments declined at begin because no mapped relation owns them; their bytes drain unread.",
    counter bootstrap_decode_seconds: f64 =
        "Cumulative CPU inside the bootstrap page walk, tuple decode included.",
    counter bootstrap_tap_seconds: f64 =
        "Cumulative time bootstrap tap readers spent inside the sink: page framing, decode, channel send. Against bootstrap_decode_seconds this is the tap's own overhead.",
    counter bootstrap_channel_block_seconds: f64 =
        "Cumulative time the bootstrap walk spent waiting for a free tuple-channel slot, i.e. emitter drain time seen by the walk.",
    counter spill_evictions_total: u64 = "Total evictions in→spill since daemon start.",
    counter xacts_committed_total: u64 = "Total xacts drained as commits since daemon start.",
    counter xacts_aborted_total: u64 = "Total xacts dropped as aborts since daemon start.",
    counter decoder_decoded_total: u64 = "Heap records decoded since daemon start.",
    counter decoder_partial_total: u64 = "Decoded tuples with prefix/suffix-from-old elided columns.",
    counter decoder_toast_chunks_total: u64 =
        "TOAST chunks routed into the xact buffer's chunk slot.",
    counter decoder_toast_malformed_total: u64 =
        "TOAST inserts the decoder couldn't reinterpret as a chunk.",
    counter decoder_toast_deletes_total: u64 =
        "DELETE records on toast relations, buffered as tombstone rows.",
    counter toast_chunks_stored_total: u64 =
        "TOAST chunk rows persisted to the CH store; the bootstrap's TOAST-phase progress signal.",
    counter toast_chunk_puts_total: u64 =
        "Chunk-store INSERTs issued. Against the seconds counter this gives per-part commit latency, which caps a TOAST-heavy restore.",
    counter toast_chunk_put_seconds: f64 =
        "Cumulative wall-clock inside chunk-store INSERTs. Divided by toast_chunk_puts_total this is per-part commit latency.",
    counter toast_tombstones_stored_total: u64 =
        "TOAST delete tombstone rows persisted to the CH store.",
    counter toast_values_filled_superseded_total: u64 =
        "Store-mode values filled after their history merge-collapsed.",
    counter toast_values_filled_mismatch_total: u64 =
        "Store-mode values filled off a dense-but-short store run (partial collapse or generation mixing).",
    counter toast_mirror_truncates_total: u64 =
        "Mirror wipes from owner TRUNCATE, applied at the reorder barrier.",
    counter toast_mirror_retires_total: u64 =
        "Mirrors emptied because their toast rel dropped (owner DROP / rewrite); table retained.",
    counter toast_rewrite_barriers_total: u64 =
        "Rewrite generations closed with residual O-B tombstones.",
    counter toast_stash_buffered_total: u64 =
        "Records on marker-proven invisible filenodes stashed raw for commit-time resolution.",
    counter raw_stash_deferred_total: u64 =
        "Records held raw because their xact tree wrote catalog state earlier in the stream; resolved at commit.",
    counter toast_stash_decoded_total: u64 =
        "Stashed records decoded at commit against a resolved toast heap.",
    counter toast_stash_discarded_total: u64 =
        "Stashed records discarded: filenode unresolvable post-commit (dropped or rotated away).",
    counter toast_stash_in_place_total: u64 =
        "Stashed toast filenodes that superseded no predecessor, so queued no residual barrier.",
    counter stash_foreign_db_skipped_total: u64 =
        "Stashed filenodes resolved to a foreign database at commit, counted once per filenode.",
    counter xact_plan_rows: u64 = "Routed heaps sealed into transaction plans.",
    /// Sealed plan bytes `[mem, file]`, rendered `storage=` labelled
    counter xact_plan_bytes_by_storage: [u64; 2] ["storage" = ["mem", "file"]] =
        "Sealed transaction-plan bytes by final backing.",
    /// Planning failures `[spool, fail_closed, detoast, partial_update,
    /// view, drain]`, rendered `reason=` labelled
    counter xact_plan_failures_by_reason: [u64; 11] ["reason" = PLAN_FAILURE_REASONS] =
        "Planning-stage failures; the whole transaction emits nothing.",
    counter route_snapshots_by_result: [u64; 2] ["result" = ["mapped", "unmapped"]] =
        "Plan-time route resolutions, one per relation per transaction.",
    counter raw_stash_bytes_by_storage: [u64; 2] ["storage" = ["mem", "spill"]] =
        "Cumulative raw-stash payload bytes by first landing.",
    counter raw_decode_rows_by_op: [u64; 7] ["op" = HEAP_OP_LABELS] =
        "Rows fanned out of decoded raw records.",
    gauge raw_pending_rows: u64 = "Raw-decoded heaps queued for pending-first yield.",
    gauge raw_pending_bytes: u64 = "Bytes held by the pending raw fanout.",
    counter emitter_rows_total: u64 = "Rows the CH emitter has handed to send_data.",
    counter emitter_blocks_total: u64 = "Native blocks the CH emitter has written.",
    counter emitter_xacts_total: u64 = "Xacts the CH emitter has drained.",
    counter emitter_unsupported_relations: u64 =
        "Tuples skipped because the source relation has no mapping in --ch-config.",
    counter emitter_deletes_discarded: u64 =
        "DELETE rows dropped because is_deleted = false leaves no marker column.",
    gauge config_pending_decl_rels: u64 =
        "Forward-declared per-table opt-ins awaiting their CREATE TABLE.",
    /// Cumulative `replicate=true` materialisations / `replicate=false`
    /// exclusions applied via the config overlay.
    counter config_replicate_opt_in_total: u64 =
        "Total config_table.replicate=true materialisations applied.",
    counter config_replicate_opt_out_total: u64 =
        "Total config_table.replicate=false / removals applied.",
    gauge pump_queue_depth: u64 = "Records buffered between the WAL pump and the queueing worker.",
    counter queue_records_out_total: u64 =
        "Records the queueing/reorder worker has dequeued and dispatched. rate() is the worker's throughput; with pump_queue_depth it tells deep-and-draining from deep-and-stalled.",
    counter queue_jobs_out_total: u64 =
        "DecodeJobs the queueing worker shipped to the decode pool. queue_jobs_out - decode_jobs_in is the worker->pool channel depth.",
    counter decode_jobs_in_total: u64 =
        "DecodeJobs the decode pool has pulled. Pinned at queue_jobs_out ⇒ pool idle; the gap at the channel cap ⇒ the pool is the limiter.",
    counter decode_rows_out_total: u64 =
        "Rows the decode pool routed to the insertbatch builder. decode_rows_out - insertbatch_rows_in is the pool->builder channel depth.",
    counter insertbatch_rows_in_total: u64 =
        "Rows the insertbatch builder accepted before sealing into InsertBatches.",
    counter insertbatch_batches_out_total: u64 =
        "InsertBatches the builder sealed and pushed to the inserter pool.",
    counter inserter_batches_in_total: u64 =
        "InsertBatches an inserter finished draining to ClickHouse. insertbatch_batches_out - inserter_batches_in is the live backlog; their rates show inserter-pool saturation.",
    counter inserter_ch_seconds_total: f64 =
        "Cumulative inserter time inside the ClickHouse INSERT round trip. Against inserter_pool_size x uptime this is CH-side utilization.",
    counter inserter_encode_seconds_total: f64 =
        "Cumulative inserter time rebuilding the Native block over a batch's owned slabs.",
    counter oracle_resolve_seconds_total: f64 =
        "Cumulative resolver time inside the oracle round trip. Overlaps inserter_ch_seconds_total, so the larger of the two is the tail's limiter.",
    counter process_cpu_seconds_total: f64 =
        "Total user+system CPU seconds consumed by the walshadow process.",
    gauge process_resident_memory_bytes: u64 = "Resident set size of the walshadow process (VmRSS).",
    counter oracle_local_columns_total: u64 =
        "Oracle-routed columns the daemon built itself: already-rendered cells against a String target, which PG would hand straight back.",
    counter oracle_blocks_total: u64 =
        "Partial Native blocks the walshadow extension returned and the daemon validated.",
    counter oracle_rows_total: u64 = "Rows carried by those blocks.",
    counter oracle_cells_total: u64 = "Source cells sent for conversion, rows times oracle columns.",
    counter oracle_conversion_errors_total: u64 =
        "Requests the extension failed on one PG Datum it could not convert.",
    counter oracle_errors_total: u64 = "Oracle request, transport, or response-validation failures.",
    gauge bridge_up: u64 =
        "1 while the pgext bridge worker answered the last request over its socket.",
    /// Per-op, rendered `op=` labelled; order matches
    /// [`OP_LABELS`].
    counter bridge_requests_by_op: [u64; 4] ["op" = OP_LABELS] =
        "Requests sent to the pgext bridge worker.",
    counter bridge_errors_by_op: [u64; 4] ["op" = OP_LABELS] =
        "Bridge requests that failed, transport or worker-side.",
    counter bridge_request_seconds_by_op: [f64; 4] ["op" = OP_LABELS] =
        "Wall time spent in bridge round trips.",
    /// Queued behind another caller on the single bridge socket
    counter bridge_lock_wait_seconds_by_op: [f64; 4] ["op" = OP_LABELS] =
        "Wall time bridge callers spent queued for the socket. Against bridge_service_seconds this says whether the worker or the funnel in front of it is the limiter.",
    /// Wire time with the socket held
    counter bridge_service_seconds_by_op: [f64; 4] ["op" = OP_LABELS] =
        "Wall time on the wire with the bridge socket held: worker conversion plus transfer.",
    counter bridge_request_bytes_by_op: [u64; 4] ["op" = OP_LABELS] =
        "Request frame bytes written to the bridge socket.",
    counter bridge_response_bytes_by_op: [u64; 4] ["op" = OP_LABELS] =
        "Response frame bytes read back off the bridge socket.",
    counter bridge_reconnects_total: u64 =
        "Bridge sockets redialled after a worker exit or transport error.",
    counter bridge_scan_rows_total: u64 = "Catalog rows the bridge's overlay scans returned.",
    counter bridge_scan_subtrans_mismatch_total: u64 =
        "Overlay tuples whose writer did not resolve to the requested top xid; trusted as ours only on rel-scoped catalogs.",
    counter bridge_scan_replay_moved_total: u64 =
        "Bridge scans that found shadow replay off the position their read pinned; committed reads answer these off SQL instead.",
    counter bridge_native_bytes_total: u64 =
        "Native block bytes the bridge returned for ENCODE_NATIVE requests.",
    counter uptime_seconds: u64 = "Seconds since the daemon began its status loop.",
    counter source_endpoint_swaps_total: u64 =
        "Source feeds swapped onto a reloaded `[source]` endpoint or slot.",
    counter source_endpoint_swap_failures_total: u64 =
        "Swap attempts refused or unreachable (identity proof, connect, START_REPLICATION, missing slot).",
    gauge source_endpoint_swap_pending: u64 =
        "1 while config names a source endpoint or slot the pump has not reached yet.",
    gauge source_timeline: u32 = "Source WAL timeline the pump is reading.",
    gauge floor_timeline: u32 = "Timeline owning the durable floor, which restart resumes on.",
    counter timeline_switches_total: u64 = "Source timeline crossings completed.",
    /// Crossings refused, rendered `reason=` labelled; order matches
    /// [`SWITCH_FAILURE_REASONS`]
    counter timeline_switch_failures_by_reason: [u64; SWITCH_FAILURE_REASONS.len()]
        ["reason" = SWITCH_FAILURE_REASONS] =
        "Crossings refused; the stream stays on the ancestor.",
    /// Most recent fork point, diagnostic
    gauge timeline_switch_lsn: u64 = "Fork LSN of the most recent timeline crossing.",
    counter timeline_prefix_bytes_verified_total: u64 =
        "Descendant bytes compared against the retained ancestor fork prefix.",
    counter timeline_transition_seconds_total: f64 = "Seconds spent inside timeline crossings.",
    /// `WalStream::next_lsn` frozen when the pump observed `[stream] paused`,
    /// which resume asks the source to serve. `0` while not paused: a value
    /// left from an earlier pause cannot be compared against a promotion
    /// decision either
    gauge pause_consumed_lsn: u64 =
        "Consumed frontier frozen when the pump observed the pause; 0 while running.",
    /// Source head last heard about, frozen at the same instant. The promotion
    /// target must reach it before it is promoted
    gauge pause_received_lsn: u64 =
        "Source head frozen when the pump observed the pause; 0 while running.",
    gauge shadow_served_timeline: u32 = "Timeline the shadow-facing walsender advertises.",
    /// Branch the shadow is replaying, which is how a shadow that followed the
    /// chain reads differently from one that merely survived
    gauge shadow_replay_timeline: u32 = "Timeline the shadow is replaying.",
    /// `source_received_lsn - min_apply_lsn` across active shadow walreceivers.
    /// Caller saturates to 0 when shadow is ahead; passes `source_received_lsn`
    /// when none connected (disconnect = max lag)
    gauge shadow_apply_lag_bytes: u64 =
        "Bytes between source_received_lsn and the min apply LSN reported by active shadow walreceivers.",
    /// `shadow_apply_lag_bytes` / rolling 30s WAL byte-rate estimate.
    /// `f64::INFINITY` (renders `+Inf`) when rate is 0
    gauge shadow_apply_lag_seconds: f64 =
        "Estimated seconds shadow trails source, by source byte rate over last 30s.",
    gauge shadow_stream_active_connections: u64 =
        "Currently-attached walreceiver connections to walshadow's walsender.",
    /// Cumulative connections dropped by `slow_threshold` overflow
    counter shadow_stream_dropped_connections_total: u64 =
        "Connections dropped by slow-client cutoff since daemon start.",
    counter catalog_boundary_holds_total: u64 =
        "Publication holds released at catalog-mutating commit boundaries.",
    counter catalog_boundary_hold_failures_total: u64 =
        "Catalog-boundary holds woken with an error (worker death, walreceiver loss, timeout).",
    counter catalog_boundary_hold_seconds_total: f64 =
        "Cumulative seconds the pump parked in released catalog-boundary holds.",
    counter desc_capture_sql_total: u64 = "Catalog boundaries captured via shadow SQL fan-out.",
    counter desc_capture_log_replay_total: u64 =
        "Catalog boundaries replayed from stored descriptor-log batches.",
    counter desc_capture_skipped_covered_total: u64 =
        "Boundaries at or below the seed's covered_through, skipped.",
    counter desc_capture_all_total: u64 =
        "Capture-all boundaries (whole-relcache inval / pg_namespace write).",
    counter desc_capture_rels_total: u64 = "Descriptors fetched across SQL captures.",
    counter desc_capture_seconds_total: f64 =
        "Cumulative seconds inside descriptor capture (within the boundary hold).",
    counter desc_events_added_total: u64 = "Added schema events produced by descriptor capture.",
    counter desc_events_changed_total: u64 = "Changed schema events produced by descriptor capture.",
    counter desc_events_dropped_total: u64 = "Dropped schema events produced by descriptor capture.",
    counter pending_captures_total: u64 =
        "Command boundaries read into the pending catalog timeline.",
    counter pending_rels_total: u64 = "Descriptors read across command boundaries.",
    counter pending_holds_total: u64 = "Publication holds taken for a command boundary.",
    counter pending_hold_seconds_total: f64 =
        "Cumulative seconds the pump parked in command-boundary holds.",
    counter pending_entries_promoted_total: u64 =
        "Pending slots folded into a commit's descriptor-log batch.",
    counter pending_entries_dropped_abort_total: u64 =
        "Pending slots dropped with an aborted transaction tree.",
    counter pending_ambiguities_suppressed_total: u64 =
        "Ambiguity intervals the pending timeline covered end to end.",
    counter pending_degraded_by_reason: [u64; 5]
        ["reason" = DegradeReason::ALL.map(|reason| reason.label())] =
        "Transactions degraded to commit-time capture, by reason.",
    gauge desc_log_entries: u64 = "Descriptor-log index entries resident.",
    gauge desc_log_tail_bytes: u64 = "Descriptor-log tail bytes since last checkpoint.",
    gauge desc_log_batches: u64 = "Descriptor-log batches resident.",
    counter desc_log_gc_total: u64 = "Descriptor-log checkpoint compactions.",
    counter desc_log_gc_dropped_entries_total: u64 = "Entries dropped by descriptor-log GC.",
    counter desc_lookups_present_total: u64 = "Descriptor lookups answered Present.",
    counter desc_lookups_dropped_total: u64 = "Descriptor lookups answered Dropped.",
    counter desc_lookups_retired_total: u64 =
        "Descriptor lookups answered Retired (rotated-away filenode).",
    counter desc_lookups_ambiguous_total: u64 =
        "Descriptor lookups landing in an ambiguity interval.",
    counter descriptor_ambiguous_total: u64 =
        "Ambiguity intervals published at capture for unproven in-place changes.",
    counter desc_lookups_not_covered_total: u64 = "Descriptor lookups answered NotCovered.",
    counter desc_lookups_foreign_db_total: u64 =
        "Descriptor lookups on a foreign database's filenode.",
}

#[derive(Debug, Clone, Default)]
pub struct MetricsRegistry {
    inner: Arc<RwLock<MetricsSnapshot>>,
}

impl MetricsRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Single writer (status-line loop), so the write lock is uncontended.
    pub async fn set(&self, snap: MetricsSnapshot) {
        *self.inner.write().await = snap;
    }

    pub async fn snapshot(&self) -> MetricsSnapshot {
        self.inner.read().await.clone()
    }
}

/// Rolling 30s ring of `(timestamp, source_received_lsn)` samples, deriving a
/// coarse WAL byte-rate for `shadow_apply_lag_seconds`.
#[derive(Debug)]
pub struct RateEstimator {
    window: Duration,
    samples: VecDeque<(Instant, u64)>,
}

impl RateEstimator {
    pub fn new(window: Duration) -> Self {
        Self {
            window,
            samples: VecDeque::new(),
        }
    }

    /// Push a sample, prune entries older than `window`.
    pub fn observe(&mut self, now: Instant, received_lsn: u64) {
        self.samples.push_back((now, received_lsn));
        let cutoff = now.checked_sub(self.window);
        if let Some(cutoff) = cutoff {
            while let Some(&(t, _)) = self.samples.front()
                && t < cutoff
                && self.samples.len() > 1
            {
                self.samples.pop_front();
            }
        }
    }

    /// Bytes-per-second across the ring. `None` if < 2 samples or zero elapsed.
    pub fn rate(&self) -> Option<f64> {
        let (front_t, front_lsn) = *self.samples.front()?;
        let (back_t, back_lsn) = *self.samples.back()?;
        let elapsed = back_t.saturating_duration_since(front_t).as_secs_f64();
        if elapsed <= 0.0 {
            return None;
        }
        let delta = back_lsn.saturating_sub(front_lsn);
        if delta == 0 {
            return None;
        }
        Some(delta as f64 / elapsed)
    }

    /// Lag bytes → seconds against current rate. `0.0` for zero lag,
    /// `INFINITY` for unknown rate.
    pub fn seconds_for(&self, lag_bytes: u64) -> f64 {
        if lag_bytes == 0 {
            return 0.0;
        }
        match self.rate() {
            Some(r) if r > 0.0 => lag_bytes as f64 / r,
            _ => f64::INFINITY,
        }
    }
}

impl Default for RateEstimator {
    fn default() -> Self {
        Self::new(Duration::from_secs(30))
    }
}

/// `reason=` label order of `MetricsSnapshot::xact_plan_failures_by_reason`,
/// matching [`crate::emit::pipeline::planner::drain_reason`]
const PLAN_FAILURE_REASONS: [&str; 11] = [
    "spool",
    "fail_closed_image_only",
    "fail_closed_malformed",
    "fail_closed_unsupported_op",
    "stash_ambiguous",
    "incomplete_toast",
    "missing_stash_resolution",
    "detoast",
    "partial_update",
    "view",
    "drain",
];

/// Family declaration off a snapshot field name: a `_by_<label>` tail names
/// the label rather than the family, and OpenMetrics appends `_total` to
/// counter samples itself, so a counter family declares the name without it
fn declare<'s>(
    enc: &'s mut DescriptorEncoder<'_>,
    name: &'s str,
    help: &str,
    kind: MetricType,
) -> Result<MetricEncoder<'s>, fmt::Error> {
    let name = name.rsplit_once("_by_").map_or(name, |(head, _)| head);
    let name = if matches!(kind, MetricType::Counter) {
        name.strip_suffix("_total").unwrap_or(name)
    } else {
        name
    };
    enc.encode_descriptor(name, help, None, kind)
}

pub(crate) fn counter<V: EncodeCounterValue>(
    enc: &mut DescriptorEncoder<'_>,
    name: &str,
    help: &str,
    value: V,
) -> fmt::Result {
    declare(enc, name, help, MetricType::Counter)?
        .encode_counter::<NoLabelSet, _, u64>(&value, None)
}

pub(crate) fn gauge<V: EncodeGaugeValue>(
    enc: &mut DescriptorEncoder<'_>,
    name: &str,
    help: &str,
    value: V,
) -> fmt::Result {
    declare(enc, name, help, MetricType::Gauge)?.encode_gauge(&value)
}

/// One `key=` labelled series per `(label, value)`, under one declaration
pub(crate) fn counter_series<'a, V: EncodeCounterValue>(
    enc: &mut DescriptorEncoder<'_>,
    name: &str,
    help: &str,
    key: &str,
    series: impl IntoIterator<Item = (&'a str, V)>,
) -> fmt::Result {
    let mut family = declare(enc, name, help, MetricType::Counter)?;
    for (label, value) in series {
        family
            .encode_family(&[(key, label)])?
            .encode_counter::<NoLabelSet, _, u64>(&value, None)?;
    }
    Ok(())
}

pub(crate) fn gauge_series<'a, V: EncodeGaugeValue>(
    enc: &mut DescriptorEncoder<'_>,
    name: &str,
    help: &str,
    key: &str,
    series: impl IntoIterator<Item = (&'a str, V)>,
) -> fmt::Result {
    let mut family = declare(enc, name, help, MetricType::Gauge)?;
    for (label, value) in series {
        family
            .encode_family(&[(key, label)])?
            .encode_gauge(&value)?;
    }
    Ok(())
}

/// `kind=`/`op=` grid over [`HEAP_OP_LABELS`]
fn counter_kind_op(
    enc: &mut DescriptorEncoder<'_>,
    name: &str,
    help: &str,
    kinds: [&str; 2],
    grid: &[[u64; 7]; 2],
) -> fmt::Result {
    let mut family = declare(enc, name, help, MetricType::Counter)?;
    for (kind, ops) in kinds.into_iter().zip(grid) {
        for (op, value) in HEAP_OP_LABELS.into_iter().zip(ops) {
            family
                .encode_family(&[("kind", kind), ("op", op)])?
                .encode_counter::<NoLabelSet, _, u64>(value, None)?;
        }
    }
    Ok(())
}

/// LSN gauges carry their raw `u64`
impl<K> EncodeGaugeValue for Pos<K> {
    fn encode(&self, encoder: &mut GaugeValueEncoder) -> fmt::Result {
        EncodeGaugeValue::encode(&self.get(), encoder)
    }
}

/// Snapshot owned for the length of a scrape, so encoding runs without the
/// registry lock
#[derive(Debug)]
struct SnapshotCollector(MetricsSnapshot);

impl Collector for SnapshotCollector {
    fn encode(&self, mut enc: DescriptorEncoder) -> fmt::Result {
        encode_snapshot(&self.0, &mut enc)
    }
}

/// `snapshot!`-declared families, then the `custom` ones
fn encode_snapshot(snap: &MetricsSnapshot, enc: &mut DescriptorEncoder<'_>) -> fmt::Result {
    encode_fields(snap, enc)?;

    gauge(
        enc,
        "walshadow_crossing_wedged",
        "1 while a crossing is parked on a refusal only an operator can clear.",
        u64::from(!snap.crossing_blocked_on.is_empty()),
    )?;

    // rmgr names come out of the fixed resource-manager table, so label values
    // need no escaping the encoder does not do
    let mut records = declare(
        enc,
        "walshadow_filter_records_total",
        "Records observed by the filter, labeled by rmgr + route.",
        MetricType::Counter,
    )?;
    for ((rm, route), n) in &snap.records_by_rm_route {
        records
            .encode_family(&[("rmgr", rm.as_str()), ("route", *route)])?
            .encode_counter::<NoLabelSet, _, u64>(n, None)?;
    }

    counter_kind_op(
        enc,
        "walshadow_raw_stash_records_total",
        "Records stashed raw for commit-time resolution.",
        ["dirty", "marker"],
        &snap.raw_stash_records_by_kind_op,
    )?;
    counter_kind_op(
        enc,
        "walshadow_raw_decode_records_total",
        "Stashed records decoded at commit resolution.",
        ["toast", "ordinary"],
        &snap.raw_decode_records_by_kind_op,
    )?;

    // Label preserves 64-bit identifier beyond float64 precision
    declare(
        enc,
        "walshadow_source_info",
        "Source cluster the pump is reading.",
        MetricType::Gauge,
    )?
    .encode_family(&[("system_id", snap.source_system_id)])?
    .encode_gauge(&1u64)?;

    // Umbrella count bare + per-mode labelled series in one family
    let mut backfills = declare(
        enc,
        "walshadow_config_backfills_pending",
        "initial_load backfills recorded but not yet complete.",
        MetricType::Gauge,
    )?;
    backfills.encode_gauge(&snap.config_backfills_pending)?;
    for (mode, value) in ["copy", "base_backup", "object_store"]
        .into_iter()
        .zip(snap.config_backfills_pending_by_mode)
    {
        backfills
            .encode_family(&[("mode", mode)])?
            .encode_gauge(&value)?;
    }
    Ok(())
}

/// Snapshot families, stage timings, then the OpenMetrics `# EOF` marker
pub fn render(snap: MetricsSnapshot) -> String {
    let mut registry = Registry::default();
    registry.register_collector(Box::new(SnapshotCollector(snap)));
    registry.register_collector(Box::new(crate::ops::stages::StageCollector));
    let mut out = String::with_capacity(16 << 10);
    text::encode(&mut out, &registry).expect("String write cannot fail");
    out
}

/// Returns the bound address (resolves `:0` ephemeral ports) and join handle.
/// Task runs until `listener.accept` errors or the runtime tears down.
pub async fn serve(
    addr: SocketAddr,
    registry: MetricsRegistry,
) -> Result<(SocketAddr, JoinHandle<()>), MetricsError> {
    let listener = TcpListener::bind(addr)
        .await
        .map_err(|e| MetricsError::Bind {
            addr: addr.to_string(),
            source: e,
        })?;
    let local = listener.local_addr()?;
    let handle = tokio::spawn(async move {
        loop {
            let (mut socket, _peer) = match listener.accept().await {
                Ok(p) => p,
                Err(e) => {
                    tracing::warn!(
                        target: "walshadow::metrics",
                        error = %e,
                        "accept failed; metrics server exiting",
                    );
                    return;
                }
            };
            let reg = registry.clone();
            tokio::spawn(async move {
                if let Err(e) = handle_client(&mut socket, &reg).await {
                    tracing::debug!(
                        target: "walshadow::metrics",
                        error = %e,
                        "metrics client errored",
                    );
                }
                let _ = socket.shutdown().await;
            });
        }
    });
    Ok((local, handle))
}

async fn handle_client(
    socket: &mut tokio::net::TcpStream,
    registry: &MetricsRegistry,
) -> io::Result<()> {
    // Don't parse the request; serve the same body for any path, so even
    // `curl http://host:port/` works
    let mut buf = [0u8; 1024];
    let n = socket.read(&mut buf).await?;
    let _ = n;
    let body = render(registry.snapshot().await);
    let resp = format!(
        "HTTP/1.0 200 OK\r\n\
         Content-Type: application/openmetrics-text; version=1.0.0; charset=utf-8\r\n\
         Content-Length: {}\r\n\
         Connection: close\r\n\
         \r\n\
         {body}",
        body.len()
    );
    socket.write_all(resp.as_bytes()).await?;
    socket.flush().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn render_includes_help_type_lines() {
        let mut snap = MetricsSnapshot {
            source_received_lsn: 0xCAFE_BABE.into(),
            filter_lsn: 0xC0FFEE.into(),
            xact_active: 3,
            uptime_seconds: 42,
            ..MetricsSnapshot::default()
        };
        snap.records_by_rm_route
            .insert(("Heap".into(), "to_decoder"), 17);

        let body = render(snap);
        assert!(body.contains("# HELP walshadow_source_received_lsn"));
        assert!(body.contains("# TYPE walshadow_source_received_lsn gauge"));
        assert!(body.contains("walshadow_source_received_lsn 3405691582"));
        assert!(body.contains("walshadow_filter_lsn 12648430"));
        assert!(
            body.contains("walshadow_filter_records_total{rmgr=\"Heap\",route=\"to_decoder\"} 17")
        );
        assert!(body.contains("walshadow_xact_active 3"));
        assert!(body.contains("walshadow_uptime_seconds_total 42"));
    }

    #[test]
    fn render_emits_plan_families_labelled() {
        let snap = MetricsSnapshot {
            xact_plan_rows: 9,
            xact_plan_bytes_by_storage: [100, 200],
            xact_plan_failures_by_reason: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11],
            route_snapshots_by_result: [7, 8],
            ..MetricsSnapshot::default()
        };
        let body = render(snap);
        assert!(body.contains("# TYPE walshadow_xact_plan_bytes counter"));
        assert!(body.contains("walshadow_xact_plan_bytes_total{storage=\"mem\"} 100"));
        assert!(body.contains("walshadow_xact_plan_bytes_total{storage=\"file\"} 200"));
        assert!(body.contains("walshadow_xact_plan_rows_total 9"));
        for (i, reason) in PLAN_FAILURE_REASONS.iter().enumerate() {
            let want = format!(
                "walshadow_xact_plan_failures_total{{reason=\"{reason}\"}} {}",
                i + 1
            );
            assert!(body.contains(&want), "{want}");
        }
        assert!(body.contains("walshadow_route_snapshots_total{result=\"mapped\"} 7"));
        assert!(body.contains("walshadow_route_snapshots_total{result=\"unmapped\"} 8"));
    }

    /// Bootstrap stage attribution reaches `/metrics`. `BootstrapOutcome`
    /// counters used to stop at a log line, which is what made a slow
    /// initial load unattributable
    #[test]
    fn render_exposes_bootstrap_stage_attribution() {
        let snap = MetricsSnapshot {
            bootstrap_bytes_tapped: 1 << 30,
            bootstrap_pages_walked: 131_072,
            bootstrap_files_skipped_unmapped: 297,
            bootstrap_decode_seconds: 12.5,
            bootstrap_channel_block_seconds: 3.25,
            bootstrap_tap_seconds: 0.0,
            inserter_ch_seconds_total: 41.5,
            oracle_resolve_seconds_total: 8.25,
            ..MetricsSnapshot::default()
        };
        let body = render(snap);
        for want in [
            "walshadow_bootstrap_bytes_tapped_total 1073741824",
            "walshadow_bootstrap_pages_walked_total 131072",
            "walshadow_bootstrap_files_skipped_unmapped_total 297",
            "walshadow_bootstrap_decode_seconds_total 12.5",
            "walshadow_bootstrap_channel_block_seconds_total 3.25",
            "walshadow_bootstrap_tap_seconds_total 0.0",
            "walshadow_inserter_ch_seconds_total 41.5",
            "walshadow_oracle_resolve_seconds_total 8.25",
        ] {
            assert!(body.contains(want), "missing {want}");
        }
    }

    /// Prometheus rejects a family declared twice; `descriptor_ambiguous_total`
    /// was emitted bare and labelled with contradicting HELP text
    #[test]
    fn render_declares_each_family_once() {
        let body = render(MetricsSnapshot::default());
        let mut seen: ahash::HashMap<&str, usize> = ahash::HashMap::default();
        for line in body.lines() {
            if let Some(rest) = line.strip_prefix("# TYPE ")
                && let Some(name) = rest.split_whitespace().next()
            {
                *seen.entry(name).or_default() += 1;
            }
        }
        let dupes: Vec<&&str> = seen
            .iter()
            .filter(|(_, n)| **n > 1)
            .map(|(name, _)| name)
            .collect();
        assert!(dupes.is_empty(), "families declared twice: {dupes:?}");
        let helps = body.lines().filter(|l| l.starts_with("# HELP ")).count();
        assert_eq!(helps, seen.len(), "one HELP per TYPE");
    }

    #[test]
    fn render_emits_raw_families_labelled() {
        let mut snap = MetricsSnapshot {
            raw_stash_bytes_by_storage: [11, 12],
            raw_decode_rows_by_op: [1, 2, 3, 4, 5, 6, 7],
            raw_pending_rows: 13,
            raw_pending_bytes: 14,
            descriptor_ambiguous_total: 21,
            desc_lookups_ambiguous_total: 22,
            ..MetricsSnapshot::default()
        };
        snap.raw_stash_records_by_kind_op[0][0] = 31; // dirty insert
        snap.raw_stash_records_by_kind_op[1][5] = 32; // marker multi_insert
        snap.raw_decode_records_by_kind_op[0][0] = 33; // toast insert
        snap.raw_decode_records_by_kind_op[1][2] = 34; // ordinary update
        let body = render(snap);
        assert!(
            body.contains("walshadow_raw_stash_records_total{kind=\"dirty\",op=\"insert\"} 31")
        );
        assert!(
            body.contains(
                "walshadow_raw_stash_records_total{kind=\"marker\",op=\"multi_insert\"} 32"
            )
        );
        assert!(body.contains("walshadow_raw_stash_bytes_total{storage=\"mem\"} 11"));
        assert!(body.contains("walshadow_raw_stash_bytes_total{storage=\"spill\"} 12"));
        assert!(
            body.contains("walshadow_raw_decode_records_total{kind=\"toast\",op=\"insert\"} 33")
        );
        assert!(
            body.contains("walshadow_raw_decode_records_total{kind=\"ordinary\",op=\"update\"} 34")
        );
        assert!(body.contains("walshadow_raw_decode_rows_total{op=\"multi_insert\"} 6"));
        assert!(body.contains("walshadow_raw_pending_rows 13"));
        assert!(body.contains("walshadow_raw_pending_bytes 14"));
        // Published intervals and lookups landing in one stay distinct
        // families, each bare
        assert!(body.contains("walshadow_descriptor_ambiguous_total 21"));
        assert!(body.contains("walshadow_desc_lookups_ambiguous_total 22"));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn registry_set_and_snapshot_round_trip() {
        let reg = MetricsRegistry::new();
        let snap = MetricsSnapshot {
            filter_lsn: 7.into(),
            xacts_committed_total: 11,
            ..MetricsSnapshot::default()
        };
        reg.set(snap.clone()).await;
        let got = reg.snapshot().await;
        assert_eq!(got.filter_lsn, 7);
        assert_eq!(got.xacts_committed_total, 11);
    }

    #[test]
    fn render_emits_shadow_apply_lag_lines() {
        let snap = MetricsSnapshot {
            shadow_apply_lag_bytes: 12345,
            shadow_apply_lag_seconds: 1.23456,
            shadow_stream_active_connections: 2,
            shadow_stream_dropped_connections_total: 5,
            ..MetricsSnapshot::default()
        };
        let body = render(snap);
        assert!(body.contains("# HELP walshadow_shadow_apply_lag_bytes"));
        assert!(body.contains("# TYPE walshadow_shadow_apply_lag_bytes gauge"));
        assert!(body.contains("walshadow_shadow_apply_lag_bytes 12345"));
        assert!(body.contains("# HELP walshadow_shadow_apply_lag_seconds"));
        assert!(body.contains("# TYPE walshadow_shadow_apply_lag_seconds gauge"));
        assert!(body.contains("walshadow_shadow_apply_lag_seconds 1.23456"));
        assert!(body.contains("# HELP walshadow_shadow_stream_active_connections"));
        assert!(body.contains("# TYPE walshadow_shadow_stream_active_connections gauge"));
        assert!(body.contains("walshadow_shadow_stream_active_connections 2"));
        assert!(body.contains("# HELP walshadow_shadow_stream_dropped_connections"));
        assert!(body.contains("# TYPE walshadow_shadow_stream_dropped_connections counter"));
        assert!(body.contains("walshadow_shadow_stream_dropped_connections_total 5"));
    }

    /// Unknown rate stays infinite on the wire. prometheus-client spells it
    /// `inf` where the hand-rolled encoder spelled it `+Inf`; Prometheus
    /// parses either through Go's ParseFloat
    #[test]
    fn render_emits_infinity_for_unknown_rate() {
        let snap = MetricsSnapshot {
            shadow_apply_lag_seconds: f64::INFINITY,
            ..MetricsSnapshot::default()
        };
        let body = render(snap);
        assert!(
            body.contains("walshadow_shadow_apply_lag_seconds inf"),
            "{body}"
        );
    }

    #[test]
    fn rate_estimator_rate_across_window() {
        let mut e = RateEstimator::new(Duration::from_secs(30));
        let t0 = Instant::now();
        e.observe(t0, 0);
        e.observe(t0 + Duration::from_secs(10), 10_000);
        let r = e.rate().expect("rate");
        assert!((r - 1000.0).abs() < 1e-6, "expected ~1000 B/s got {r}");
    }

    #[test]
    fn rate_estimator_prunes_outside_window() {
        let mut e = RateEstimator::new(Duration::from_secs(5));
        let t0 = Instant::now();
        e.observe(t0, 0);
        e.observe(t0 + Duration::from_secs(1), 1_000);
        e.observe(t0 + Duration::from_secs(10), 10_000);
        let (front_t, _) = *e.samples.front().unwrap();
        assert!(front_t >= t0 + Duration::from_secs(1));
    }

    #[test]
    fn rate_estimator_seconds_for_zero_lag() {
        let mut e = RateEstimator::new(Duration::from_secs(30));
        e.observe(Instant::now(), 0);
        assert_eq!(e.seconds_for(0), 0.0);
    }

    #[test]
    fn rate_estimator_seconds_for_unknown_rate_is_infinity() {
        let e = RateEstimator::new(Duration::from_secs(30));
        assert!(e.seconds_for(1024).is_infinite());
    }

    #[test]
    fn rate_estimator_seconds_for_known_rate() {
        let mut e = RateEstimator::new(Duration::from_secs(30));
        let t0 = Instant::now();
        e.observe(t0, 0);
        e.observe(t0 + Duration::from_secs(10), 10_000);
        // rate = 1000 B/s, lag 5000 B → 5 s
        let s = e.seconds_for(5_000);
        assert!((s - 5.0).abs() < 1e-6, "expected 5.0 got {s}");
    }

    /// Wire shape rather than substrings: every sample belongs to the family
    /// declared above it, counter samples carry `_total`, labels are quoted,
    /// values parse, and one EOF marker closes the body after stage families
    #[test]
    fn exposition_parses_as_openmetrics() {
        let mut snap = MetricsSnapshot {
            shadow_apply_lag_seconds: f64::INFINITY,
            uptime_seconds: 7,
            source_system_id: u64::MAX,
            ..MetricsSnapshot::default()
        };
        snap.records_by_rm_route
            .insert(("Heap".into(), "to_shadow"), 3);
        let body = render(snap);

        let (families, tail) = body.split_once("# EOF\n").expect("EOF marker");
        assert!(tail.is_empty(), "bytes after EOF: {tail:?}");
        assert!(families.contains("walshadow_stage_completed_total{stage=\"copy\"} 0"));

        let mut declared: Option<(&str, &str)> = None;
        let mut helped = "";
        for line in families.lines() {
            if let Some(rest) = line.strip_prefix("# HELP ") {
                let (name, help) = rest.split_once(' ').expect("HELP text");
                assert!(!help.is_empty(), "{name} declared with empty help");
                helped = name;
                continue;
            }
            if let Some(rest) = line.strip_prefix("# TYPE ") {
                let (name, kind) = rest.split_once(' ').expect("TYPE keyword");
                assert_eq!(name, helped, "TYPE without its own HELP: {line}");
                assert!(matches!(kind, "counter" | "gauge"), "{line}");
                declared = Some((name, kind));
                continue;
            }
            let (head, value) = line.rsplit_once(' ').expect("sample value");
            let (name, labels) = match head.split_once('{') {
                Some((name, labels)) => (name, labels.strip_suffix('}').expect("closing brace")),
                None => (head, ""),
            };
            let (family, kind) = declared.expect("sample ahead of its declaration");
            let want = match kind {
                "counter" => format!("{family}_total"),
                _ => family.to_owned(),
            };
            assert_eq!(name, want, "sample outside its family: {line}");
            for label in labels.split_terminator(',') {
                let (key, val) = label.split_once('=').expect("key=value label");
                assert!(!key.is_empty(), "{line}");
                assert!(
                    val.len() >= 2 && val.starts_with('"') && val.ends_with('"'),
                    "{line}"
                );
            }
            assert!(value.parse::<f64>().is_ok(), "unparseable value: {line}");
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn http_serve_returns_text_format_body() {
        let reg = MetricsRegistry::new();
        let snap = MetricsSnapshot {
            filter_lsn: 0xDEAD.into(),
            ..MetricsSnapshot::default()
        };
        reg.set(snap).await;
        let (addr, _handle) = serve("127.0.0.1:0".parse().unwrap(), reg).await.unwrap();

        let mut sock = tokio::net::TcpStream::connect(addr).await.unwrap();
        sock.write_all(b"GET /metrics HTTP/1.0\r\n\r\n")
            .await
            .unwrap();
        let mut buf = Vec::new();
        sock.read_to_end(&mut buf).await.unwrap();
        let resp = String::from_utf8(buf).unwrap();
        assert!(resp.starts_with("HTTP/1.0 200 OK\r\n"), "{resp}");
        assert!(resp.contains("Content-Type: application/openmetrics-text"));
        assert!(resp.contains("walshadow_filter_lsn 57005"), "{resp}");
    }
}
