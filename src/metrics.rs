//! Phase 10 — HTTP/Prometheus metrics surface.
//!
//! Exposes a `/metrics` endpoint over plain TCP that scrapers like
//! Prometheus already speak. Output is the Prometheus
//! [text-format](https://prometheus.io/docs/instrumenting/exposition_formats/),
//! hand-rolled to avoid pulling in a `prometheus` crate dependency for
//! what is otherwise a tiny set of gauges/counters.
//!
//! The registry is `Arc`-cloneable: the daemon's main loop populates the
//! values at status-tick cadence, the HTTP server reads a snapshot per
//! request. Lock contention is invisible at scrape rates (≤ once/sec
//! per Prom replica).
//!
//! Scope is the LSN triple ([Phase 11 fills the resume-safe values]),
//! per-rmgr filter counters, xact buffer occupancy, spill stats, oracle
//! sampler stats, and a daemon-uptime counter. The endpoint is
//! intentionally read-only (no `/quit`, no admin verbs); operator
//! actions stay on the CLI side.

use std::collections::BTreeMap;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;

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

/// Shared snapshot of every value the `/metrics` handler will render.
/// Daemon updates the gauges on every status-line iteration; HTTP
/// readers grab a read lock and serialise.
#[derive(Debug, Default, Clone)]
pub struct MetricsSnapshot {
    /// Source PG's most recent `server_wal_end` (write LSN as PG sees
    /// it). Walshadow's "source_received_lsn" in [Phase 11] parlance.
    pub source_received_lsn: u64,
    /// Last segment-boundary LSN the filter has dispatched downstream.
    /// Becomes filter_durable_lsn in Phase 11 once segment fsync lands.
    pub filter_lsn: u64,
    /// Shadow PG's `pg_last_wal_replay_lsn()`, polled at status-line
    /// cadence. `0` until first poll.
    pub shadow_replay_lsn: u64,
    /// Latest LSN the decoder has committed downstream (Phase 11).
    /// Stays `0` in Phase 10 — surface lands here so the endpoint shape
    /// is fixed before durability work.
    pub decoder_commit_lsn: u64,
    /// CH emitter ack-LSN (Phase 11). Same placeholder treatment as
    /// `decoder_commit_lsn`.
    pub emitter_ack_lsn: u64,
    /// Per-(rmgr name, decision) record counters. `decision` is one of
    /// `"keep"` / `"drop"`.
    pub records_by_rm_decision: BTreeMap<(String, &'static str), u64>,
    pub xact_active: u64,
    pub xact_bytes_in_memory: u64,
    pub spill_xacts_active: u64,
    pub spill_bytes_active: u64,
    pub spill_evictions_total: u64,
    pub xacts_committed_total: u64,
    pub xacts_aborted_total: u64,
    pub decoder_decoded_total: u64,
    pub decoder_partial_total: u64,
    pub emitter_rows_total: u64,
    pub emitter_blocks_total: u64,
    pub emitter_xacts_total: u64,
    pub emitter_unsupported_relations: u64,
    pub oracle_resolved_total: u64,
    pub oracle_fallback_raw_total: u64,
    pub oracle_validate_sampled_total: u64,
    pub oracle_validate_mismatches_total: u64,
    pub oracle_errors_total: u64,
    pub uptime_secs: u64,
}

/// Shared handle the daemon clones to write + the HTTP server clones
/// to read.
#[derive(Debug, Clone, Default)]
pub struct MetricsRegistry {
    inner: Arc<RwLock<MetricsSnapshot>>,
}

impl MetricsRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Replace the live snapshot in-place. Single-writer pattern — the
    /// daemon's status-line loop is the only writer, so the write lock
    /// is uncontended.
    pub async fn set(&self, snap: MetricsSnapshot) {
        *self.inner.write().await = snap;
    }

    /// Snapshot-copy for the HTTP renderer. Holds the read lock for
    /// the duration of the clone (microseconds).
    pub async fn snapshot(&self) -> MetricsSnapshot {
        self.inner.read().await.clone()
    }
}

/// Render the snapshot in Prometheus text-format. Every gauge has a
/// `# HELP` + `# TYPE` line; counters use `_total` suffix per Prom
/// naming conventions.
pub fn render(snap: &MetricsSnapshot) -> String {
    use std::fmt::Write as _;
    let mut s = String::with_capacity(1024);

    // LSN gauges --------------------------------------------------------
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
            "Highest LSN the decoder has committed downstream (Phase 11; 0 in Phase 10).",
            snap.decoder_commit_lsn,
        ),
        (
            "walshadow_emitter_ack_lsn",
            "Highest LSN the CH emitter has acked (Phase 11; 0 in Phase 10).",
            snap.emitter_ack_lsn,
        ),
    ] {
        writeln!(s, "# HELP {name} {help}").unwrap();
        writeln!(s, "# TYPE {name} gauge").unwrap();
        writeln!(s, "{name} {value}").unwrap();
    }

    // Per-rmgr counters -------------------------------------------------
    writeln!(
        s,
        "# HELP walshadow_filter_records_total Records observed by the filter, labeled by rmgr + decision."
    )
    .unwrap();
    writeln!(s, "# TYPE walshadow_filter_records_total counter").unwrap();
    for ((rm, decision), n) in &snap.records_by_rm_decision {
        writeln!(
            s,
            "walshadow_filter_records_total{{rmgr={rm:?},decision={decision:?}}} {n}"
        )
        .unwrap();
    }

    // Xact buffer + spill ----------------------------------------------
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
    ];
    for (name, help, kind, value) in pairs {
        writeln!(s, "# HELP {name} {help}").unwrap();
        writeln!(s, "# TYPE {name} {kind}").unwrap();
        writeln!(s, "{name} {value}").unwrap();
    }
    s
}

/// Spawn the HTTP server task. Returns the bound address (handy when
/// the caller passed `:0` to pick an ephemeral port) and the join
/// handle. The task runs until the registry's last clone drops and
/// `listener.accept` errors, or until the runtime tears down.
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
    // Read up to a 1 KiB request line + headers — enough for any
    // sensible scraper. Don't parse the request; we serve the same
    // body for any path so even `curl http://host:port/` works.
    let mut buf = [0u8; 1024];
    let n = socket.read(&mut buf).await?;
    let _ = n; // request body intentionally unused
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
        snap.records_by_rm_decision
            .insert(("Heap".into(), "drop"), 17);

        let body = render(&snap);
        assert!(body.contains("# HELP walshadow_source_received_lsn"));
        assert!(body.contains("# TYPE walshadow_source_received_lsn gauge"));
        assert!(body.contains("walshadow_source_received_lsn 3405691582"));
        assert!(body.contains("walshadow_filter_lsn 12648430"));
        assert!(
            body.contains("walshadow_filter_records_total{rmgr=\"Heap\",decision=\"drop\"} 17")
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
