//! HTTP/Prometheus metrics surface.
//!
//! `/metrics` over plain TCP, Prometheus
//! [text-format](https://prometheus.io/docs/instrumenting/exposition_formats/),
//! hand-rolled to avoid a `prometheus` crate dependency for a tiny gauge set.
//!
//! Registry is `Arc`-cloneable: daemon's main loop writes at status-tick
//! cadence, HTTP server reads a snapshot per request. Endpoint is read-only by
//! design (no `/quit`, no admin verbs); operator actions stay on the CLI.

use std::collections::{BTreeMap, VecDeque};
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use thiserror::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;

#[derive(Debug, Error)]
pub enum MetricsError {
    #[error("io: {0}")]
    Io(#[from] io::Error),
    #[error("bind {addr}: {source}")]
    Bind { addr: String, source: io::Error },
}

/// Snapshot of every value `/metrics` renders. Daemon writes per status-line
/// iteration; HTTP readers take a read lock and serialise.
#[derive(Debug, Default, Clone)]
pub struct MetricsSnapshot {
    /// Source PG's most recent `server_wal_end` (write LSN as PG sees it)
    pub source_received_lsn: u64,
    /// Last segment-boundary LSN dispatched downstream; becomes durable once
    /// segment fsync lands
    pub filter_lsn: u64,
    /// Shadow PG's `pg_last_wal_replay_lsn()`, `0` until first poll
    pub shadow_replay_lsn: u64,
    /// Stays `0`: surface fixed before durability work
    pub decoder_commit_lsn: u64,
    /// Stays `0`, same as `decoder_commit_lsn`
    pub emitter_ack_lsn: u64,
    /// `route` is `"to_shadow"` / `"to_decoder"`
    pub records_by_rm_route: BTreeMap<(String, &'static str), u64>,
    pub xact_active: u64,
    pub xact_bytes_in_memory: u64,
    pub spill_xacts_active: u64,
    pub spill_bytes_active: u64,
    /// Bytes resident inside an active commit drain (merge heads + in-mem
    /// tail + chunk generations + mirror rows held by consumers)
    pub drain_resident_bytes: u64,
    /// Chunk-generation share of `drain_resident_bytes`, held until the
    /// last drain batch / decode job drops its generation
    pub drain_chunk_resident_bytes: u64,
    /// Mirror-row share of `drain_resident_bytes`, held until store put
    pub drain_row_resident_bytes: u64,
    /// Bytes in transaction TOAST body spool files (disk, not resident)
    pub toast_xact_spool_bytes: u64,
    /// Bytes held by live [`crate::budget::MemoryBudget`] permits
    pub resident_payload_bytes: u64,
    pub resident_payload_peak_bytes: u64,
    /// Budget acquisitions that had to wait for a release
    pub memory_budget_waits_total: u64,
    /// Requests above a budget compartment's satisfiable share, admitted
    /// with only that share metered
    pub memory_budget_overshoots_total: u64,
    /// Bytes resident in the bootstrap TOAST-deferred spool's in-memory
    /// prefix
    pub bootstrap_deferred_bytes: u64,
    /// Encoded bytes in the bootstrap TOAST-deferred spool file
    pub bootstrap_deferred_spool_bytes: u64,
    pub spill_evictions_total: u64,
    pub xacts_committed_total: u64,
    pub xacts_aborted_total: u64,
    pub decoder_decoded_total: u64,
    pub decoder_partial_total: u64,
    pub decoder_toast_chunks_total: u64,
    pub decoder_toast_malformed_total: u64,
    pub decoder_toast_deletes_total: u64,
    pub toast_tombstones_stored_total: u64,
    pub toast_values_filled_superseded_total: u64,
    pub toast_values_filled_mismatch_total: u64,
    pub toast_mirror_truncates_total: u64,
    pub toast_mirror_retires_total: u64,
    pub toast_rewrite_barriers_total: u64,
    pub toast_stash_buffered_total: u64,
    pub toast_stash_decoded_total: u64,
    pub toast_stash_discarded_total: u64,
    pub toast_stash_skipped_total: u64,
    pub emitter_rows_total: u64,
    pub emitter_blocks_total: u64,
    pub emitter_xacts_total: u64,
    pub emitter_unsupported_relations: u64,
    /// Forward-declared per-table opt-ins (`config_table.replicate=true`)
    /// awaiting their `CREATE TABLE`.
    pub config_pending_decl_rels: u64,
    /// Cumulative `replicate=true` materialisations / `replicate=false`
    /// exclusions applied via the config overlay.
    pub config_replicate_opt_in_total: u64,
    pub config_replicate_opt_out_total: u64,
    /// `initial_load` backfills recorded in the ledger but not yet complete
    /// (in flight, or awaiting re-run on next boot).
    pub config_backfills_pending: u64,
    /// `config_backfills_pending` split `[copy, base_backup, object_store]`;
    /// rendered as `mode=` labelled series under the umbrella gauge.
    pub config_backfills_pending_by_mode: [u64; 3],
    // Pipeline flow + process gauges; see `render` for descriptions.
    pub pump_queue_depth: u64,
    pub queue_records_out_total: u64,
    pub queue_jobs_out_total: u64,
    pub decode_jobs_in_total: u64,
    pub decode_rows_out_total: u64,
    pub insertbatch_rows_in_total: u64,
    pub insertbatch_batches_out_total: u64,
    pub inserter_batches_in_total: u64,
    pub process_cpu_seconds_total: f64,
    pub process_resident_memory_bytes: u64,
    pub oracle_resolved_total: u64,
    pub oracle_fallback_raw_total: u64,
    pub oracle_validate_sampled_total: u64,
    pub oracle_validate_mismatches_total: u64,
    pub oracle_errors_total: u64,
    pub uptime_secs: u64,
    /// `source_received_lsn - min_apply_lsn` across active shadow walreceivers.
    /// Caller saturates to 0 when shadow is ahead; passes `source_received_lsn`
    /// when none connected (disconnect = max lag)
    pub shadow_apply_lag_bytes: u64,
    /// `shadow_apply_lag_bytes` / rolling 30s WAL byte-rate estimate.
    /// `f64::INFINITY` (renders `+Inf`) when rate is 0
    pub shadow_apply_lag_seconds: f64,
    pub shadow_stream_active_connections: u64,
    /// Cumulative connections dropped by `slow_threshold` overflow
    pub shadow_stream_dropped_connections_total: u64,
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

/// Prometheus text-format. Each metric gets `# HELP` + `# TYPE`; counters use
/// the `_total` suffix per Prom convention.
pub fn render(snap: &MetricsSnapshot) -> String {
    use std::fmt::Write as _;
    let mut s = String::with_capacity(1024);

    for (name, help, value) in [
        (
            "walshadow_source_received_lsn",
            "Source PG's most recent server_wal_end seen on the replication socket.",
            snap.source_received_lsn,
        ),
        (
            "walshadow_filter_lsn",
            "LSN of the last filtered WAL byte the daemon has dispatched.",
            snap.filter_lsn,
        ),
        (
            "walshadow_shadow_replay_lsn",
            "Shadow PG's pg_last_wal_replay_lsn(), polled at status cadence.",
            snap.shadow_replay_lsn,
        ),
        (
            "walshadow_decoder_commit_lsn",
            "Highest LSN the decoder has committed downstream (currently 0).",
            snap.decoder_commit_lsn,
        ),
        (
            "walshadow_emitter_ack_lsn",
            "Highest LSN the CH emitter has acked (currently 0).",
            snap.emitter_ack_lsn,
        ),
    ] {
        writeln!(s, "# HELP {name} {help}").unwrap();
        writeln!(s, "# TYPE {name} gauge").unwrap();
        writeln!(s, "{name} {value}").unwrap();
    }

    writeln!(
        s,
        "# HELP walshadow_filter_records_total Records observed by the filter, labeled by rmgr + route."
    )
    .unwrap();
    writeln!(s, "# TYPE walshadow_filter_records_total counter").unwrap();
    for ((rm, route), n) in &snap.records_by_rm_route {
        writeln!(
            s,
            "walshadow_filter_records_total{{rmgr={rm:?},route={route:?}}} {n}"
        )
        .unwrap();
    }

    let pairs: &[(&str, &str, &str, u64)] = &[
        (
            "walshadow_xact_active",
            "Active transactions buffered in memory or on spill.",
            "gauge",
            snap.xact_active,
        ),
        (
            "walshadow_xact_bytes_in_memory",
            "Bytes held in memory across all buffered xacts.",
            "gauge",
            snap.xact_bytes_in_memory,
        ),
        (
            "walshadow_spill_xacts_active",
            "Xacts with at least one entry currently in their spill file.",
            "gauge",
            snap.spill_xacts_active,
        ),
        (
            "walshadow_spill_bytes_active",
            "Bytes currently held across all active xact spill files.",
            "gauge",
            snap.spill_bytes_active,
        ),
        (
            "walshadow_drain_resident_bytes",
            "Bytes resident inside an active commit drain (heads + chunk generations + mirror rows).",
            "gauge",
            snap.drain_resident_bytes,
        ),
        (
            "walshadow_drain_chunk_resident_bytes",
            "Chunk-generation share of drain_resident_bytes, held until consumers drop.",
            "gauge",
            snap.drain_chunk_resident_bytes,
        ),
        (
            "walshadow_drain_row_resident_bytes",
            "Mirror-row share of drain_resident_bytes, held until store put completes.",
            "gauge",
            snap.drain_row_resident_bytes,
        ),
        (
            "walshadow_toast_xact_spool_bytes",
            "Bytes in transaction TOAST body spool files (disk, not resident).",
            "gauge",
            snap.toast_xact_spool_bytes,
        ),
        (
            "walshadow_resident_payload_bytes",
            "Bytes held by live memory-budget permits across pipeline stages.",
            "gauge",
            snap.resident_payload_bytes,
        ),
        (
            "walshadow_resident_payload_peak_bytes",
            "High-water mark of resident payload permit bytes.",
            "gauge",
            snap.resident_payload_peak_bytes,
        ),
        (
            "walshadow_memory_budget_waits_total",
            "Budget acquisitions that waited for a release.",
            "counter",
            snap.memory_budget_waits_total,
        ),
        (
            "walshadow_memory_budget_overshoots_total",
            "Requests above a budget compartment, admitted with only the satisfiable share metered.",
            "counter",
            snap.memory_budget_overshoots_total,
        ),
        (
            "walshadow_bootstrap_deferred_bytes",
            "Resident bytes in the bootstrap TOAST-deferred spool's in-memory prefix.",
            "gauge",
            snap.bootstrap_deferred_bytes,
        ),
        (
            "walshadow_bootstrap_deferred_spool_bytes",
            "Encoded bytes in the bootstrap TOAST-deferred spool file.",
            "gauge",
            snap.bootstrap_deferred_spool_bytes,
        ),
        (
            "walshadow_spill_evictions_total",
            "Total evictions in→spill since daemon start.",
            "counter",
            snap.spill_evictions_total,
        ),
        (
            "walshadow_xacts_committed_total",
            "Total xacts drained as commits since daemon start.",
            "counter",
            snap.xacts_committed_total,
        ),
        (
            "walshadow_xacts_aborted_total",
            "Total xacts dropped as aborts since daemon start.",
            "counter",
            snap.xacts_aborted_total,
        ),
        (
            "walshadow_decoder_decoded_total",
            "Heap records decoded since daemon start.",
            "counter",
            snap.decoder_decoded_total,
        ),
        (
            "walshadow_decoder_partial_total",
            "Decoded tuples with prefix/suffix-from-old elided columns.",
            "counter",
            snap.decoder_partial_total,
        ),
        (
            "walshadow_decoder_toast_chunks_total",
            "TOAST chunks routed into the xact buffer's chunk slot.",
            "counter",
            snap.decoder_toast_chunks_total,
        ),
        (
            "walshadow_decoder_toast_malformed_total",
            "TOAST inserts the decoder couldn't reinterpret as a chunk.",
            "counter",
            snap.decoder_toast_malformed_total,
        ),
        (
            "walshadow_decoder_toast_deletes_total",
            "DELETE records on toast relations, buffered as tombstone rows.",
            "counter",
            snap.decoder_toast_deletes_total,
        ),
        (
            "walshadow_toast_tombstones_stored_total",
            "TOAST delete tombstone rows persisted to the CH store.",
            "counter",
            snap.toast_tombstones_stored_total,
        ),
        (
            "walshadow_toast_values_filled_superseded_total",
            "Store-mode values filled after their history merge-collapsed.",
            "counter",
            snap.toast_values_filled_superseded_total,
        ),
        (
            "walshadow_toast_values_filled_mismatch_total",
            "Store-mode values filled off a dense-but-short store run \
             (partial collapse or generation mixing).",
            "counter",
            snap.toast_values_filled_mismatch_total,
        ),
        (
            "walshadow_toast_mirror_truncates_total",
            "Mirror wipes from owner TRUNCATE, applied at the reorder barrier.",
            "counter",
            snap.toast_mirror_truncates_total,
        ),
        (
            "walshadow_toast_mirror_retires_total",
            "Mirrors emptied because their toast rel dropped (owner DROP / rewrite); \
             table retained.",
            "counter",
            snap.toast_mirror_retires_total,
        ),
        (
            "walshadow_toast_rewrite_barriers_total",
            "Rewrite generations closed with residual O-B tombstones.",
            "counter",
            snap.toast_rewrite_barriers_total,
        ),
        (
            "walshadow_toast_stash_buffered_total",
            "Records on marker-proven invisible filenodes stashed raw for \
             commit-time resolution.",
            "counter",
            snap.toast_stash_buffered_total,
        ),
        (
            "walshadow_toast_stash_decoded_total",
            "Stashed records decoded at commit against a resolved toast heap.",
            "counter",
            snap.toast_stash_decoded_total,
        ),
        (
            "walshadow_toast_stash_discarded_total",
            "Stashed records discarded: filenode unresolvable post-commit \
             (dropped or rotated away).",
            "counter",
            snap.toast_stash_discarded_total,
        ),
        (
            "walshadow_toast_stash_skipped_total",
            "Stashed records resolved to a non-toast heap; decode fenced off.",
            "counter",
            snap.toast_stash_skipped_total,
        ),
        (
            "walshadow_emitter_rows_total",
            "Rows the CH emitter has handed to send_data.",
            "counter",
            snap.emitter_rows_total,
        ),
        (
            "walshadow_emitter_blocks_total",
            "Native blocks the CH emitter has written.",
            "counter",
            snap.emitter_blocks_total,
        ),
        (
            "walshadow_emitter_xacts_total",
            "Xacts the CH emitter has drained.",
            "counter",
            snap.emitter_xacts_total,
        ),
        (
            "walshadow_emitter_unsupported_relations_total",
            "Tuples skipped because the source relation has no mapping in --ch-config.",
            "counter",
            snap.emitter_unsupported_relations,
        ),
        (
            "walshadow_pump_queue_depth",
            "Records buffered between the WAL pump and the queueing worker.",
            "gauge",
            snap.pump_queue_depth,
        ),
        (
            "walshadow_queue_records_out_total",
            "Records the queueing/reorder worker has dequeued and dispatched. rate() is the worker's throughput; with pump_queue_depth it tells deep-and-draining from deep-and-stalled.",
            "counter",
            snap.queue_records_out_total,
        ),
        (
            "walshadow_queue_jobs_out_total",
            "DecodeJobs the queueing worker shipped to the decode pool. queue_jobs_out - decode_jobs_in is the worker->pool channel depth.",
            "counter",
            snap.queue_jobs_out_total,
        ),
        (
            "walshadow_decode_jobs_in_total",
            "DecodeJobs the decode pool has pulled. Pinned at queue_jobs_out ⇒ pool idle; the gap at the channel cap ⇒ the pool is the limiter.",
            "counter",
            snap.decode_jobs_in_total,
        ),
        (
            "walshadow_decode_rows_out_total",
            "Rows the decode pool routed to the insertbatch builder. decode_rows_out - insertbatch_rows_in is the pool->builder channel depth.",
            "counter",
            snap.decode_rows_out_total,
        ),
        (
            "walshadow_insertbatch_rows_in_total",
            "Rows the insertbatch builder accepted before sealing into InsertBatches.",
            "counter",
            snap.insertbatch_rows_in_total,
        ),
        (
            "walshadow_insertbatch_batches_out_total",
            "InsertBatches the builder sealed and pushed to the inserter pool.",
            "counter",
            snap.insertbatch_batches_out_total,
        ),
        (
            "walshadow_inserter_batches_in_total",
            "InsertBatches an inserter finished draining to ClickHouse. insertbatch_batches_out - inserter_batches_in is the live backlog; their rates show inserter-pool saturation.",
            "counter",
            snap.inserter_batches_in_total,
        ),
        (
            "walshadow_process_resident_memory_bytes",
            "Resident set size of the walshadow process (VmRSS).",
            "gauge",
            snap.process_resident_memory_bytes,
        ),
        (
            "walshadow_decode_resolved_total",
            "PgPending columns resolved via the walshadow extension.",
            "counter",
            snap.oracle_resolved_total,
        ),
        (
            "walshadow_decode_fallback_raw_total",
            "PgPending columns shipped as raw bytes (extension absent).",
            "counter",
            snap.oracle_fallback_raw_total,
        ),
        (
            "walshadow_decode_validate_sampled_total",
            "Rows the differential-decode sampler probed.",
            "counter",
            snap.oracle_validate_sampled_total,
        ),
        (
            "walshadow_decode_validate_mismatches_total",
            "Sampled rows where local codec output ≠ shadow PG's render.",
            "counter",
            snap.oracle_validate_mismatches_total,
        ),
        (
            "walshadow_decode_errors_total",
            "Decode-bridge SQL errors swallowed by the fallback path.",
            "counter",
            snap.oracle_errors_total,
        ),
        (
            "walshadow_uptime_seconds",
            "Seconds since the daemon began its status loop.",
            "counter",
            snap.uptime_secs,
        ),
        (
            "walshadow_shadow_apply_lag_bytes",
            "Bytes between source_received_lsn and the min apply LSN reported by active shadow walreceivers.",
            "gauge",
            snap.shadow_apply_lag_bytes,
        ),
        (
            "walshadow_shadow_stream_active_connections",
            "Currently-attached walreceiver connections to walshadow's walsender.",
            "gauge",
            snap.shadow_stream_active_connections,
        ),
        (
            "walshadow_shadow_stream_dropped_connections_total",
            "Connections dropped by slow-client cutoff since daemon start.",
            "counter",
            snap.shadow_stream_dropped_connections_total,
        ),
        (
            "walshadow_config_pending_decl_rels",
            "Forward-declared per-table opt-ins awaiting their CREATE TABLE.",
            "gauge",
            snap.config_pending_decl_rels,
        ),
        (
            "walshadow_config_replicate_opt_in_total",
            "Total config_table.replicate=true materialisations applied.",
            "counter",
            snap.config_replicate_opt_in_total,
        ),
        (
            "walshadow_config_replicate_opt_out_total",
            "Total config_table.replicate=false / removals applied.",
            "counter",
            snap.config_replicate_opt_out_total,
        ),
    ];
    for (name, help, kind, value) in pairs {
        writeln!(s, "# HELP {name} {help}").unwrap();
        writeln!(s, "# TYPE {name} {kind}").unwrap();
        writeln!(s, "{name} {value}").unwrap();
    }

    // Umbrella count bare + per-mode labelled series in one family
    {
        let name = "walshadow_config_backfills_pending";
        writeln!(
            s,
            "# HELP {name} initial_load backfills recorded but not yet complete."
        )
        .unwrap();
        writeln!(s, "# TYPE {name} gauge").unwrap();
        writeln!(s, "{name} {}", snap.config_backfills_pending).unwrap();
        for (mode, v) in ["copy", "base_backup", "object_store"]
            .iter()
            .zip(snap.config_backfills_pending_by_mode)
        {
            writeln!(s, "{name}{{mode=\"{mode}\"}} {v}").unwrap();
        }
    }

    // Prom format accepts `+Inf` for unknown rate
    let name = "walshadow_shadow_apply_lag_seconds";
    writeln!(
        s,
        "# HELP {name} Estimated seconds shadow trails source, by source byte rate over last 30s.",
    )
    .unwrap();
    writeln!(s, "# TYPE {name} gauge").unwrap();
    if snap.shadow_apply_lag_seconds.is_infinite() {
        writeln!(s, "{name} +Inf").unwrap();
    } else {
        writeln!(s, "{name} {:.3}", snap.shadow_apply_lag_seconds).unwrap();
    }

    // Process CPU as a float counter (seconds); rate() ≈ cores in use.
    let name = "walshadow_process_cpu_seconds_total";
    writeln!(
        s,
        "# HELP {name} Total user+system CPU seconds consumed by the walshadow process.",
    )
    .unwrap();
    writeln!(s, "# TYPE {name} counter").unwrap();
    writeln!(s, "{name} {:.3}", snap.process_cpu_seconds_total).unwrap();
    s
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
    let snap = registry.snapshot().await;
    let body = render(&snap);
    let resp = format!(
        "HTTP/1.0 200 OK\r\n\
         Content-Type: text/plain; version=0.0.4; charset=utf-8\r\n\
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
            source_received_lsn: 0xCAFE_BABE,
            filter_lsn: 0xC0FFEE,
            xact_active: 3,
            uptime_secs: 42,
            ..MetricsSnapshot::default()
        };
        snap.records_by_rm_route
            .insert(("Heap".into(), "to_decoder"), 17);

        let body = render(&snap);
        assert!(body.contains("# HELP walshadow_source_received_lsn"));
        assert!(body.contains("# TYPE walshadow_source_received_lsn gauge"));
        assert!(body.contains("walshadow_source_received_lsn 3405691582"));
        assert!(body.contains("walshadow_filter_lsn 12648430"));
        assert!(
            body.contains("walshadow_filter_records_total{rmgr=\"Heap\",route=\"to_decoder\"} 17")
        );
        assert!(body.contains("walshadow_xact_active 3"));
        assert!(body.contains("walshadow_uptime_seconds 42"));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn registry_set_and_snapshot_round_trip() {
        let reg = MetricsRegistry::new();
        let snap = MetricsSnapshot {
            filter_lsn: 7,
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
        let body = render(&snap);
        assert!(body.contains("# HELP walshadow_shadow_apply_lag_bytes"));
        assert!(body.contains("# TYPE walshadow_shadow_apply_lag_bytes gauge"));
        assert!(body.contains("walshadow_shadow_apply_lag_bytes 12345"));
        assert!(body.contains("# HELP walshadow_shadow_apply_lag_seconds"));
        assert!(body.contains("# TYPE walshadow_shadow_apply_lag_seconds gauge"));
        assert!(body.contains("walshadow_shadow_apply_lag_seconds 1.235"));
        assert!(body.contains("# HELP walshadow_shadow_stream_active_connections"));
        assert!(body.contains("# TYPE walshadow_shadow_stream_active_connections gauge"));
        assert!(body.contains("walshadow_shadow_stream_active_connections 2"));
        assert!(body.contains("# HELP walshadow_shadow_stream_dropped_connections_total"));
        assert!(body.contains("# TYPE walshadow_shadow_stream_dropped_connections_total counter"));
        assert!(body.contains("walshadow_shadow_stream_dropped_connections_total 5"));
    }

    #[test]
    fn render_emits_infinity_as_plus_inf() {
        let snap = MetricsSnapshot {
            shadow_apply_lag_seconds: f64::INFINITY,
            ..MetricsSnapshot::default()
        };
        let body = render(&snap);
        assert!(body.contains("walshadow_shadow_apply_lag_seconds +Inf"));
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn http_serve_returns_text_format_body() {
        let reg = MetricsRegistry::new();
        let snap = MetricsSnapshot {
            filter_lsn: 0xDEAD,
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
        assert!(resp.contains("Content-Type: text/plain"));
        assert!(resp.contains("walshadow_filter_lsn 57005"), "{resp}");
    }
}
