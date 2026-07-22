//! Catalog-boundary publication hold.
//!
//! At the commit of a catalog-mutating xact ([`Record::catalog_boundary`],
//! stamped by the pump classifier) the pump must not publish successor
//! bytes until shadow replays through the commit's `next_lsn`
//! (PG `EndRecPtr`). [`BoundaryHoldSink`] enforces this by blocking inside
//! `on_record`: [`WalStream`](crate::source::wal_stream::WalStream)
//! dispatches wire bytes for record N, then awaits the record sink before
//! framing N+1, so an await here holds every successor byte from both the
//! shadow wire and the archive segment sink. DML-only commits never park.
//!
//! Shadow keeps applying during the hold — the walsender listener task
//! flushes already-queued bytes independently — and reports apply progress
//! via `'r'` standby-status frames. Non-forced walreceiver replies fire
//! only when the flush position advances, so the gate prods with
//! reply-requested keepalives ([`ShadowStreamState::request_status`]) to
//! observe apply advancing at poll cadence rather than
//! `wal_receiver_status_interval`.
//!
//! Waiter is result-bearing: worker death (channel closed / panic),
//! walreceiver loss past the hold timeout, and replay timeout all wake it
//! with `Err`, which poisons the stream and terminates the pump with the
//! root cause. The hold never waits on ClickHouse, committed drains, or
//! queued barrier work — only shadow replay of already-shipped bytes.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use tokio::sync::Mutex;

use crate::record::{Record, RecordSink, SinkError};
use crate::source::queueing_record_sink::QueueingRecordSink;
use crate::source::shadow_stream::ShadowStreamState;

/// Bounds one publication hold. Must stay well under source PG's
/// `wal_sender_timeout` (default 60s): the pump answers no source
/// keepalives while parked.
pub const DEFAULT_HOLD_TIMEOUT: Duration = Duration::from_secs(30);

/// Apply-LSN poll cadence. Listener flushes queued frames every ~50ms, so
/// finer polling only spins the lock.
pub const DEFAULT_HOLD_POLL: Duration = Duration::from_millis(20);

#[derive(Debug, Default)]
pub struct BoundaryHoldStats {
    /// Holds released by shadow replay reaching the commit's `next_lsn`.
    pub holds: AtomicU64,
    /// Holds woken with `Err` (worker death, walreceiver loss, timeout).
    pub failures: AtomicU64,
    /// Cumulative released-hold duration.
    pub hold_nanos: AtomicU64,
}

impl BoundaryHoldStats {
    pub fn hold_seconds_total(&self) -> f64 {
        self.hold_nanos.load(Ordering::Relaxed) as f64 / 1e9
    }
}

#[derive(Debug, Clone, Copy)]
pub struct BoundaryGateConfig {
    pub hold_timeout: Duration,
    pub poll_interval: Duration,
}

impl Default for BoundaryGateConfig {
    fn default() -> Self {
        Self {
            hold_timeout: DEFAULT_HOLD_TIMEOUT,
            poll_interval: DEFAULT_HOLD_POLL,
        }
    }
}

/// Waits for shadow replay to pass a catalog commit's `next_lsn`.
pub struct CatalogBoundaryGate {
    state: Arc<Mutex<ShadowStreamState>>,
    config: BoundaryGateConfig,
    pub stats: Arc<BoundaryHoldStats>,
}

impl CatalogBoundaryGate {
    pub fn new(state: Arc<Mutex<ShadowStreamState>>, config: BoundaryGateConfig) -> Self {
        Self {
            state,
            config,
            stats: Arc::new(BoundaryHoldStats::default()),
        }
    }

    /// Park until shadow's aggregate apply LSN reaches `next_lsn`.
    /// `worker_alive` is polled each tick; a dead decoder worker wakes the
    /// waiter with `Err` instead of letting the daemon hang out the
    /// timeout. Walreceiver loss mid-hold is tolerated until the deadline —
    /// a reconnect backfills the in-progress segment and replay resumes —
    /// then fails the boundary.
    pub async fn hold(
        &self,
        commit_lsn: u64,
        next_lsn: u64,
        worker_alive: impl Fn() -> bool,
    ) -> Result<(), SinkError> {
        let start = Instant::now();
        loop {
            let agg = self.state.lock().await.aggregate();
            if agg.min_apply_lsn.is_some_and(|apply| apply >= next_lsn) {
                self.stats.holds.fetch_add(1, Ordering::Relaxed);
                self.stats
                    .hold_nanos
                    .fetch_add(start.elapsed().as_nanos() as u64, Ordering::Relaxed);
                // Info: fires at DDL rate, operators read it as the
                // hold-latency signal
                tracing::info!(
                    target: "walshadow::boundary_hold",
                    commit_lsn = format_args!("{commit_lsn:#X}"),
                    next_lsn = format_args!("{next_lsn:#X}"),
                    held = ?start.elapsed(),
                    "catalog boundary released",
                );
                return Ok(());
            }
            if !worker_alive() {
                return self.fail(format!(
                    "decoder worker terminated during catalog boundary hold at {commit_lsn:#X}"
                ));
            }
            if start.elapsed() >= self.config.hold_timeout {
                return self.fail(format!(
                    "catalog boundary hold at {commit_lsn:#X} timed out after {:?}: \
                     shadow apply {:?} < {next_lsn:#X} ({} walreceiver connection(s))",
                    self.config.hold_timeout, agg.min_apply_lsn, agg.active_connections,
                ));
            }
            self.state.lock().await.request_status();
            tokio::time::sleep(self.config.poll_interval).await;
        }
    }

    fn fail(&self, msg: String) -> Result<(), SinkError> {
        self.stats.failures.fetch_add(1, Ordering::Relaxed);
        Err(SinkError::Other(msg))
    }
}

/// [`QueueingRecordSink`] wrapper enacting the hold. Forwards every record,
/// then at a catalog boundary force-flushes the pump-side batch (the commit
/// must not strand in the accumulator while the pump parks — and the flush
/// surfaces any parked worker error first) and parks in
/// [`CatalogBoundaryGate::hold`].
pub struct BoundaryHoldSink {
    pub inner: QueueingRecordSink,
    pub gate: CatalogBoundaryGate,
}

impl BoundaryHoldSink {
    pub fn new(inner: QueueingRecordSink, gate: CatalogBoundaryGate) -> Self {
        Self { inner, gate }
    }

    pub fn in_flight(&self) -> u64 {
        self.inner.in_flight()
    }

    pub fn processed(&self) -> u64 {
        self.inner.processed()
    }

    pub async fn flush(&mut self) -> Result<(), SinkError> {
        self.inner.flush().await
    }

    pub async fn close(self) -> Result<(), SinkError> {
        self.inner.close().await
    }
}

impl RecordSink for BoundaryHoldSink {
    fn on_record<'a>(
        &'a mut self,
        record: &'a Record<'a>,
    ) -> Pin<Box<dyn Future<Output = Result<(), SinkError>> + Send + 'a>> {
        Box::pin(async move {
            self.inner.on_record(record).await?;
            if !record.catalog_boundary {
                return Ok(());
            }
            self.inner.flush().await?;
            let inner = &self.inner;
            if let Err(hold_err) = self
                .gate
                .hold(record.source_lsn, record.next_lsn, || inner.worker_alive())
                .await
            {
                // Prefer the worker's parked root cause over the generic
                // hold error (empty-buffer flush only drains the err slot)
                self.inner.flush().await?;
                return Err(hold_err);
            }
            Ok(())
        })
    }

    fn on_idle<'a>(
        &'a mut self,
    ) -> Pin<Box<dyn Future<Output = Result<(), SinkError>> + Send + 'a>> {
        self.inner.on_idle()
    }

    fn on_close<'a>(
        &'a mut self,
    ) -> Pin<Box<dyn Future<Output = Result<(), SinkError>> + Send + 'a>> {
        self.inner.on_close()
    }

    fn on_idle_advance<'a>(
        &'a mut self,
        lsn: u64,
    ) -> Pin<Box<dyn Future<Output = Result<(), SinkError>> + Send + 'a>> {
        self.inner.on_idle_advance(lsn)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::record::CountingRecordSink;

    fn gate_with(
        state: Arc<Mutex<ShadowStreamState>>,
        hold_timeout: Duration,
    ) -> CatalogBoundaryGate {
        CatalogBoundaryGate::new(
            state,
            BoundaryGateConfig {
                hold_timeout,
                poll_interval: Duration::from_millis(1),
            },
        )
    }

    fn state() -> Arc<Mutex<ShadowStreamState>> {
        Arc::new(Mutex::new(ShadowStreamState::new(
            1,
            "sys".into(),
            0x1000,
            1024 * 1024,
        )))
    }

    #[tokio::test]
    async fn hold_releases_when_apply_reaches_exact_next_lsn() {
        let s = state();
        let id = s.lock().await.register_connection(0x1000);
        let gate = gate_with(s.clone(), Duration::from_secs(5));
        let waiter = tokio::spawn({
            let s = s.clone();
            async move {
                tokio::time::sleep(Duration::from_millis(20)).await;
                // apply == next_lsn exactly must release (replay reports
                // EndRecPtr, not last wire byte)
                s.lock().await.observe_status(id, 0x2000, 0x2000, 0x2000);
            }
        });
        gate.hold(0x1F00, 0x2000, || true).await.expect("released");
        waiter.await.unwrap();
        assert_eq!(gate.stats.holds.load(Ordering::Relaxed), 1);
        assert_eq!(gate.stats.failures.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn hold_prods_walreceiver_with_reply_requested_keepalive() {
        let s = state();
        let id = s.lock().await.register_connection(0x1000);
        let gate = gate_with(s.clone(), Duration::from_secs(5));
        let prodded = tokio::spawn({
            let s = s.clone();
            async move {
                loop {
                    let drained = s.lock().await.drain_send_queue(id);
                    if let Some(bytes) = drained {
                        // 'd' + u32 len + 'k' + wal_end(8) + time(8) + reply(1)
                        assert_eq!(bytes[5], b'k');
                        assert_eq!(*bytes.last().unwrap(), 1, "reply requested");
                        s.lock().await.observe_status(id, 0x3000, 0x3000, 0x3000);
                        return;
                    }
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            }
        });
        gate.hold(0x2F00, 0x3000, || true).await.expect("released");
        prodded.await.unwrap();
    }

    #[tokio::test]
    async fn hold_times_out_without_apply_progress() {
        let s = state();
        s.lock().await.register_connection(0x1000);
        let gate = gate_with(s.clone(), Duration::from_millis(20));
        let err = gate
            .hold(0x1F00, 0x2000, || true)
            .await
            .expect_err("must time out");
        assert!(err.to_string().contains("timed out"), "{err}");
        assert_eq!(gate.stats.failures.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn hold_times_out_with_no_walreceiver() {
        // No connection at all: min_apply_lsn stays None; reconnect never
        // comes, deadline fails the boundary
        let gate = gate_with(state(), Duration::from_millis(20));
        let err = gate
            .hold(0x1F00, 0x2000, || true)
            .await
            .expect_err("must time out");
        assert!(err.to_string().contains("0 walreceiver"), "{err}");
    }

    #[tokio::test]
    async fn hold_releases_after_mid_hold_reconnect() {
        // Walreceiver absent when the hold starts; a late attach + status
        // must still release within the deadline
        let s = state();
        let gate = gate_with(s.clone(), Duration::from_secs(5));
        let attach = tokio::spawn({
            let s = s.clone();
            async move {
                tokio::time::sleep(Duration::from_millis(20)).await;
                let id = s.lock().await.register_connection(0x1000);
                s.lock().await.observe_status(id, 0x2000, 0x2000, 0x2000);
            }
        });
        gate.hold(0x1F00, 0x2000, || true).await.expect("released");
        attach.await.unwrap();
    }

    #[tokio::test]
    async fn dead_worker_wakes_waiter_with_err() {
        let s = state();
        s.lock().await.register_connection(0x1000);
        let gate = gate_with(s, Duration::from_secs(30));
        let err = gate
            .hold(0x1F00, 0x2000, || false)
            .await
            .expect_err("dead worker must fail the hold");
        assert!(err.to_string().contains("worker terminated"), "{err}");
        assert_eq!(gate.stats.failures.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn sink_parks_only_at_catalog_boundary() {
        // DML-only records (catalog_boundary = false) pass straight
        // through with no shadow connection and no timeout
        let s = state();
        let q = QueueingRecordSink::spawn(CountingRecordSink::default(), 4, 16, None);
        let gate = gate_with(s, Duration::from_millis(10));
        let mut sink = BoundaryHoldSink::new(q, gate);
        let rec = Record {
            source_lsn: 0x1100,
            next_lsn: 0x1140,
            ..Default::default()
        };
        for _ in 0..8 {
            sink.on_record(&rec).await.expect("no park");
        }
        sink.close().await.expect("close");
    }

    #[tokio::test]
    async fn sink_boundary_flushes_partial_batch_and_releases() {
        // batch_size 64 with one record: without the forced flush the
        // commit strands in the pump-side buffer while the pump parks
        let s = state();
        let id = s.lock().await.register_connection(0x1000);
        let counter = Arc::new(std::sync::Mutex::new(0u64));
        struct Count(Arc<std::sync::Mutex<u64>>);
        impl RecordSink for Count {
            fn on_record<'a>(
                &'a mut self,
                _r: &'a Record<'a>,
            ) -> Pin<Box<dyn Future<Output = Result<(), SinkError>> + Send + 'a>> {
                let c = self.0.clone();
                Box::pin(async move {
                    *c.lock().unwrap() += 1;
                    Ok(())
                })
            }
        }
        let q = QueueingRecordSink::spawn(Count(counter.clone()), 64, 1024, None);
        let gate = gate_with(s.clone(), Duration::from_secs(5));
        let mut sink = BoundaryHoldSink::new(q, gate);
        let releaser = tokio::spawn({
            let s = s.clone();
            async move {
                tokio::time::sleep(Duration::from_millis(20)).await;
                s.lock().await.observe_status(id, 0x2000, 0x2000, 0x2000);
            }
        });
        let rec = Record {
            source_lsn: 0x1F00,
            next_lsn: 0x2000,
            catalog_boundary: true,
            ..Default::default()
        };
        sink.on_record(&rec).await.expect("boundary releases");
        releaser.await.unwrap();
        // Forced flush shipped the sub-batch-size buffer to the worker
        for _ in 0..100 {
            if *counter.lock().unwrap() == 1 {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(*counter.lock().unwrap(), 1, "commit reached the worker");
        sink.close().await.expect("close");
    }

    #[tokio::test]
    async fn sink_boundary_surfaces_parked_worker_error_over_hold_error() {
        struct Fail;
        impl RecordSink for Fail {
            fn on_record<'a>(
                &'a mut self,
                _r: &'a Record<'a>,
            ) -> Pin<Box<dyn Future<Output = Result<(), SinkError>> + Send + 'a>> {
                Box::pin(async { Err(SinkError::Other("worker boom".into())) })
            }
        }
        let s = state();
        s.lock().await.register_connection(0x1000);
        let q = QueueingRecordSink::spawn(Fail, 1, 4, None);
        let gate = gate_with(s, Duration::from_secs(30));
        let mut sink = BoundaryHoldSink::new(q, gate);
        let rec = Record {
            source_lsn: 0x1F00,
            next_lsn: 0x2000,
            catalog_boundary: true,
            ..Default::default()
        };
        // Worker fails on the shipped commit; hold's worker_alive check
        // wakes with Err and the parked root cause wins
        let err = sink.on_record(&rec).await.expect_err("must fail");
        assert!(err.to_string().contains("boom"), "{err}");
    }
}
