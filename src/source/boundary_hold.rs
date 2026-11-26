//! Catalog-boundary publication hold.
//!
//! At the commit of a catalog-mutating xact ([`Record::boundary_info`],
//! stamped by the pump classifier) the pump must not publish successor
//! bytes until shadow replays through the commit's `next_lsn`
//! (PG `EndRecPtr`). [`BoundaryHoldSink`] enforces this by blocking inside
//! `on_record`: [`WalStream`](crate::source::wal_stream::WalStream)
//! dispatches wire bytes for record N, then awaits the record sink before
//! framing N+1, so an await here holds every successor byte from both the
//! shadow wire and the archive segment sink. DML-only and
//! statistics-only commits never wait for shadow replay
//!
//! Shadow keeps applying during the hold — the walsender listener task
//! flushes already-queued bytes independently — and reports apply progress
//! via `'r'` standby-status frames. Non-forced walreceiver replies fire
//! only when the flush position advances, so the gate prods with
//! reply-requested keepalives ([`ShadowStreamState::request_status`]) to
//! observe apply advancing at poll cadence rather than
//! `wal_receiver_status_interval`.
//!
//! During archive recovery, read replay progress directly from the shadow
//! through the bridge. Report stalls and keep waiting for replay.

use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use tokio::sync::{Mutex, watch};
use tokio::time::Instant;

use crate::catalog::shadow_catalog::ParkedAt;
use crate::ops::bridge::Bridge;
use crate::ops::metrics::MetricsRegistry;
use crate::pos::{Pos, ShadowReplay};
use crate::record::{BoundaryKind, Record, RecordSink, Route, SinkError};
use crate::source::catalog_capture::CaptureSet;
use crate::source::queueing_record_sink::QueueingRecordSink;
use crate::source::shadow_stream::ShadowStreamState;

const DEFAULT_STALL_WARNING: Duration = Duration::from_secs(30);
const REPLAY_PROBE_INTERVAL: Duration = Duration::from_millis(250);
// Dropping a probe mid round trip closes its bridge socket
const REPLAY_PROBE_TIMEOUT: Duration = Duration::from_secs(5);

/// Apply-LSN poll cadence. Listener flushes queued frames every ~50ms, so
/// finer polling only spins the lock.
pub const DEFAULT_HOLD_POLL: Duration = Duration::from_millis(20);

#[derive(Debug, Default)]
pub struct BoundaryHoldStats {
    /// Holds released by shadow replay reaching the commit's `next_lsn`.
    pub holds: AtomicU64,
    /// Holds interrupted by decoder worker failure.
    pub failures: AtomicU64,
    /// Cumulative released-hold duration.
    pub hold_nanos: AtomicU64,
    /// Target replay LSN of a stalled hold, zero otherwise
    pub stalled_lsn: AtomicU64,
    /// True while a hold parks the pump
    pub parked: watch::Sender<bool>,
}

impl BoundaryHoldStats {
    pub fn hold_seconds_total(&self) -> f64 {
        self.hold_nanos.load(Ordering::Relaxed) as f64 / 1e9
    }
}

#[derive(Debug, Clone, Copy)]
pub struct BoundaryGateConfig {
    pub warn_after: Duration,
    pub poll_interval: Duration,
}

impl Default for BoundaryGateConfig {
    fn default() -> Self {
        Self {
            warn_after: DEFAULT_STALL_WARNING,
            poll_interval: DEFAULT_HOLD_POLL,
        }
    }
}

/// Waits for shadow replay to pass a catalog commit's `next_lsn`.
pub struct CatalogBoundaryGate {
    state: Arc<Mutex<ShadowStreamState>>,
    config: BoundaryGateConfig,
    probe: Option<(Arc<Bridge>, MetricsRegistry)>,
    pub stats: Arc<BoundaryHoldStats>,
}

impl CatalogBoundaryGate {
    pub fn new(state: Arc<Mutex<ShadowStreamState>>, config: BoundaryGateConfig) -> Self {
        Self {
            state,
            config,
            probe: None,
            stats: Arc::new(BoundaryHoldStats::default()),
        }
    }

    pub fn with_local_shadow(mut self, bridge: Arc<Bridge>, metrics: MetricsRegistry) -> Self {
        self.probe = Some((bridge, metrics));
        self
    }

    /// Publish directly since the pump, which publishes metrics, is parked
    async fn set_stalled(&self, lsn: u64) {
        if self.stats.stalled_lsn.swap(lsn, Ordering::Relaxed) != lsn
            && let Some((_, metrics)) = &self.probe
        {
            metrics
                .update(|snap| snap.catalog_boundary_stalled_lsn = lsn)
                .await;
        }
    }

    /// Hold successor WAL until shadow applies through `next_lsn`.
    pub async fn hold(
        &self,
        commit_lsn: u64,
        next_lsn: Pos<ShadowReplay>,
        worker_alive: impl Fn() -> bool,
    ) -> Result<ParkedAt, SinkError> {
        self.stats.parked.send_replace(true);
        let _parked = ParkedGuard(&self.stats);
        let applied = self.state.lock().await.applied();
        let start = Instant::now();
        let mut next_probe = start;
        let mut quiet_since = start;
        let mut last_apply = Pos::ZERO;
        let held = loop {
            let seen = applied.current();
            let agg = self.state.lock().await.aggregate();
            if !worker_alive() {
                self.set_stalled(0).await;
                return self.fail(format!(
                    "decoder worker terminated during catalog boundary hold at {commit_lsn:#X}"
                ));
            }
            let mut apply = agg.min_apply_lsn.unwrap_or(Pos::ZERO);
            if agg.active_connections == 0
                && Instant::now() >= next_probe
                && let Some((bridge, _)) = &self.probe
            {
                // Archive replay has no receiver. Probe shared memory since SQL can
                // wait on a catalog lock that replay releases after this record.
                if let Ok(Ok(lsn)) =
                    tokio::time::timeout(REPLAY_PROBE_TIMEOUT, bridge.replay_lsn()).await
                {
                    apply = Pos::new(lsn);
                }
                next_probe = Instant::now() + REPLAY_PROBE_INTERVAL;
            }
            if apply >= next_lsn {
                self.set_stalled(0).await;
                break start.elapsed();
            }
            let now = Instant::now();
            if apply > last_apply {
                last_apply = apply;
                quiet_since = now;
                self.set_stalled(0).await;
            } else if now.duration_since(quiet_since) >= self.config.warn_after {
                tracing::warn!(
                    target: "walshadow::boundary_hold",
                    commit_lsn = format_args!("{commit_lsn:#X}"),
                    next_lsn = %next_lsn,
                    shadow_apply = %last_apply,
                    walreceivers = agg.active_connections,
                    held = ?start.elapsed(),
                    "catalog boundary stalled waiting for shadow replay",
                );
                self.set_stalled(next_lsn.get()).await;
                quiet_since = now;
            }
            if agg.active_connections != 0 {
                self.state.lock().await.request_status();
            }
            let _ = tokio::time::timeout(self.config.poll_interval, applied.advance(seen)).await;
        };
        self.stats.holds.fetch_add(1, Ordering::Relaxed);
        self.stats
            .hold_nanos
            .fetch_add(held.as_nanos() as u64, Ordering::Relaxed);
        tracing::info!(
            target: "walshadow::boundary_hold",
            commit_lsn = format_args!("{commit_lsn:#X}"),
            next_lsn = %next_lsn,
            held = ?held,
            "catalog boundary released",
        );
        Ok(ParkedAt::new(next_lsn.get()))
    }

    fn fail<T>(&self, msg: String) -> Result<T, SinkError> {
        self.stats.failures.fetch_add(1, Ordering::Relaxed);
        Err(SinkError::Other(msg))
    }
}

/// Clear hold state when a hold is cancelled. Next metrics publish clears gauge
struct ParkedGuard<'a>(&'a BoundaryHoldStats);

impl Drop for ParkedGuard<'_> {
    fn drop(&mut self) {
        self.0.stalled_lsn.store(0, Ordering::Relaxed);
        self.0.parked.send_replace(false);
    }
}

#[derive(Debug, Clone)]
pub struct PendingDatabase {
    pub oid: u32,
    pub name: String,
    pub visible_at: u64,
    pub bridge_socket: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatabaseAttach {
    pub oid: u32,
    pub name: String,
    pub lsn: u64,
}

pub type AttachSlot = Arc<std::sync::Mutex<Option<DatabaseAttach>>>;

#[derive(Debug, Clone, Default)]
pub struct PendingDatabases {
    waiting: Vec<PendingDatabase>,
    attach_timeout: Duration,
    attached: AttachSlot,
}

impl PendingDatabases {
    pub fn new(
        waiting: Vec<PendingDatabase>,
        attach_timeout: Duration,
        attached: AttachSlot,
    ) -> Self {
        Self {
            waiting,
            attach_timeout,
            attached,
        }
    }

    fn hit(&self, record: &Record<'_>) -> Option<&PendingDatabase> {
        self.waiting.iter().find(|p| {
            record.source_lsn >= p.visible_at
                || (!matches!(record.route, Route::ToShadow)
                    && record
                        .parsed
                        .blocks
                        .iter()
                        .any(|b| b.header.location.rel.db_node == p.oid))
        })
    }
}

async fn bridge_listening(path: &Path, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    loop {
        if tokio::net::UnixStream::connect(path).await.is_ok() {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// [`QueueingRecordSink`] wrapper enacting the hold. At a catalog boundary:
/// force-flush the pump-side batch (predecessors must not strand in the
/// accumulator while the pump parks — and the flush surfaces any parked
/// worker error first), park in [`CatalogBoundaryGate::hold`], capture
/// descriptors against the caught-up shadow, then forward the commit record
/// — so by the time the worker drains the xact, its schema events are
/// already attached and the log batch is durable.
///
/// A command boundary
/// ([`BoundaryKind::Command`]) reuses
/// that sequence unchanged, and capture reads the transaction's own
/// uncommitted rows there. Capture declines, before the park, a boundary
/// whose read WAL shows no row needs, since parking there stalls for
/// nothing.
///
/// Aborts drop their speculative slots here rather than at the drain: the
/// pump runs ahead of the decoder, so a later commit could otherwise promote
/// a rolled-back subtree's shapes into the durable log.
pub struct BoundaryHoldSink {
    pub inner: QueueingRecordSink,
    pub gate: CatalogBoundaryGate,
    /// Empty = hold-only (tests / capture-less harnesses)
    pub capture: CaptureSet,
    pub pending: PendingDatabases,
}

impl BoundaryHoldSink {
    pub fn new(inner: QueueingRecordSink, gate: CatalogBoundaryGate) -> Self {
        Self {
            inner,
            gate,
            capture: CaptureSet::default(),
            pending: PendingDatabases::default(),
        }
    }

    pub fn with_capture(mut self, capture: CaptureSet) -> Self {
        self.capture = capture;
        self
    }

    pub fn with_pending(mut self, pending: PendingDatabases) -> Self {
        self.pending = pending;
        self
    }

    /// Hold fails only once the worker dies, so flush to surface its root cause
    async fn hold(&mut self, record: &Record<'_>) -> Result<ParkedAt, SinkError> {
        let inner = &self.inner;
        let held = self
            .gate
            .hold(record.source_lsn, Pos::new(record.next_lsn), || {
                inner.worker_alive()
            })
            .await;
        if let Err(hold_err) = held {
            return self.inner.flush().await.and(Err(hold_err));
        }
        held
    }

    async fn attach_pending(
        &mut self,
        db: PendingDatabase,
        record: &Record<'_>,
    ) -> Result<(), SinkError> {
        self.inner.flush().await?;
        self.hold(record).await?;
        if !bridge_listening(&db.bridge_socket, self.pending.attach_timeout).await {
            return Err(SinkError::Other(format!(
                "database {} (oid {}) is on the source, but the shadow replayed through \
                 {:#X} and its bridge at {} never came up: the shadow lacks the database \
                 (dropped and recreated after the backup?) or its bridge worker fails, \
                 see the shadow's startup.log",
                db.name,
                db.oid,
                record.next_lsn,
                db.bridge_socket.display(),
            )));
        }
        tracing::info!(
            target: "walshadow",
            database = %db.name,
            oid = db.oid,
            lsn = format_args!("{:#X}", record.source_lsn),
            "shadow replayed a pending database's creation; restarting the session to follow it",
        );
        let msg = format!("attaching database {} at {:#X}", db.name, record.source_lsn);
        *self.pending.attached.lock().expect("attach slot poisoned") = Some(DatabaseAttach {
            oid: db.oid,
            name: db.name,
            lsn: record.source_lsn,
        });
        Err(SinkError::Other(msg))
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
    /// A pending database attaches on a record's blocks
    fn header_only(&self, record: &Record<'_>) -> bool {
        self.pending.waiting.is_empty() && self.inner.header_only(record)
    }

    fn may_park(&self, record: &Record<'_>) -> bool {
        record.boundary_info.is_some()
            || record.aborted_tree.is_some()
            || self.pending.hit(record).is_some()
    }

    fn on_record<'a>(
        &'a mut self,
        record: &'a Record<'a>,
    ) -> Pin<Box<dyn Future<Output = Result<(), SinkError>> + Send + 'a>> {
        // Common record: nothing to attach, forget or hold, so pass the inner
        // future through instead of boxing a hold-sized one around it
        if !self.may_park(record) {
            return self.inner.on_record(record);
        }
        Box::pin(async move {
            if let Some(db) = self.pending.hit(record).cloned() {
                return self.attach_pending(db, record).await;
            }
            if let Some(members) = &record.aborted_tree {
                self.capture.forget_aborted(members);
            }
            let Some(info) = &record.boundary_info else {
                return self.inner.on_record(record).await;
            };
            let park = if self.capture.is_empty() {
                // Without capture, wait only for commits that change more than statistics
                matches!(info.kind, BoundaryKind::Commit) && !info.stats_only
            } else {
                self.capture.admits(info, record.next_lsn)
            };
            if !park {
                // Save an empty batch so restart can replay this commit
                // without reading catalog state from shadow
                if matches!(info.kind, BoundaryKind::Commit) {
                    self.capture
                        .cover_unparked(info, record.source_lsn, record.next_lsn)
                        .await?;
                }
                return self.inner.on_record(record).await;
            }
            // Ship + flush the xact's predecessors before parking; the
            // commit itself forwards only after capture so the worker's
            // drain finds events already attached
            self.inner.flush().await?;
            let parked = Instant::now();
            let held = self.hold(record).await?;
            self.capture.count_hold(info, parked.elapsed());
            self.capture
                .capture_boundary(info, record.source_lsn, &held)
                .await?;
            self.inner.on_record(record).await?;
            // Ship the commit immediately: the drain (and its CH effects)
            // must not wait out the accumulator
            self.inner.flush().await
        })
    }

    fn on_record_owned(
        &mut self,
        record: Record<'static>,
    ) -> Pin<Box<dyn Future<Output = Result<(), SinkError>> + Send + '_>> {
        if !self.may_park(&record) {
            return self.inner.on_record_owned(record);
        }
        // Rare, so parking keeps its one by-reference path
        Box::pin(async move { self.on_record(&record).await })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::record::CountingRecordSink;

    fn gate_with(
        state: Arc<Mutex<ShadowStreamState>>,
        warn_after: Duration,
    ) -> CatalogBoundaryGate {
        CatalogBoundaryGate::new(
            state,
            BoundaryGateConfig {
                warn_after,
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
        let id = s
            .lock()
            .await
            .register_connection(0x1000, 1, None)
            .expect("current timeline");
        let gate = gate_with(s.clone(), Duration::from_secs(5));
        let waiter = tokio::spawn({
            let s = s.clone();
            async move {
                tokio::time::sleep(Duration::from_millis(20)).await;
                // apply == next_lsn exactly must release (replay reports
                // EndRecPtr, not last wire byte)
                s.lock()
                    .await
                    .observe_status(id, 0x2000, Pos::new(0x2000), Pos::new(0x2000));
            }
        });
        gate.hold(0x1F00, Pos::new(0x2000), || true)
            .await
            .expect("released");
        waiter.await.unwrap();
        assert_eq!(gate.stats.holds.load(Ordering::Relaxed), 1);
        assert_eq!(gate.stats.failures.load(Ordering::Relaxed), 0);
    }

    /// Release rides the status wake, not the poll: with the backstop set
    /// far past the status, only the wake can release inside the timeout
    #[tokio::test]
    async fn hold_releases_on_the_status_wake_not_the_poll() {
        let s = state();
        let id = s
            .lock()
            .await
            .register_connection(0x1000, 1, None)
            .expect("current timeline");
        let gate = CatalogBoundaryGate::new(
            s.clone(),
            BoundaryGateConfig {
                warn_after: Duration::from_secs(5),
                poll_interval: Duration::from_secs(4),
            },
        );
        let waiter = tokio::spawn({
            let s = s.clone();
            async move {
                tokio::time::sleep(Duration::from_millis(20)).await;
                s.lock()
                    .await
                    .observe_status(id, 0x2000, Pos::new(0x2000), Pos::new(0x2000));
            }
        });
        let started = Instant::now();
        gate.hold(0x1F00, Pos::new(0x2000), || true)
            .await
            .expect("released");
        waiter.await.unwrap();
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "waited {:?}, so the poll released it, not the wake",
            started.elapsed(),
        );
    }

    #[tokio::test]
    async fn hold_prods_walreceiver_with_reply_requested_keepalive() {
        let s = state();
        let id = s
            .lock()
            .await
            .register_connection(0x1000, 1, None)
            .expect("current timeline");
        let gate = gate_with(s.clone(), Duration::from_secs(5));
        let queued = s.lock().await.queued();
        let prodded = tokio::spawn({
            let s = s.clone();
            async move {
                // Wait on listener wake, not runtime task order
                queued.advance(Pos::ZERO).await.expect("state alive");
                let bytes = s.lock().await.drain_send_queue(id);
                let bytes = bytes.expect("keepalive queued before hold parks");
                // 'd' + u32 len + 'k' + wal_end(8) + time(8) + reply(1)
                assert_eq!(bytes[5], b'k');
                assert_eq!(*bytes.last().unwrap(), 1, "reply requested");
                s.lock()
                    .await
                    .observe_status(id, 0x3000, Pos::new(0x3000), Pos::new(0x3000));
            }
        });
        gate.hold(0x2F00, Pos::new(0x3000), || true)
            .await
            .expect("released");
        prodded.await.unwrap();
    }

    #[tokio::test]
    async fn hold_releases_from_archive_replay_without_a_receiver() {
        use crate::ops::bridge::{
            PROJECTION_VERSION, PROTO_VERSION,
            tests::{hello_body, lsn_body, remote_body, spawn_worker},
        };

        let (_dir, path) = spawn_worker(vec![
            Some(hello_body(PROTO_VERSION, PROJECTION_VERSION)),
            Some(remote_body("replay position unavailable")),
            Some(lsn_body(0x1000)),
            Some(lsn_body(0x2000)),
        ]);
        let bridge = Arc::new(Bridge::connect(path).await.unwrap());
        let metrics = MetricsRegistry::new();
        let gate = gate_with(state(), Duration::from_millis(10))
            .with_local_shadow(bridge, metrics.clone());
        let hold = gate.hold(0x1F00, Pos::new(0x2000), || true);
        tokio::pin!(hold);

        assert!(
            tokio::time::timeout(Duration::from_millis(100), &mut hold)
                .await
                .is_err()
        );
        assert_eq!(
            metrics.snapshot().await.catalog_boundary_stalled_lsn,
            0x2000
        );
        assert!(*gate.stats.parked.borrow());
        let parked = tokio::time::timeout(Duration::from_secs(2), hold)
            .await
            .expect("probe should observe archive replay")
            .expect("released");
        assert_eq!(parked.lsn(), 0x2000);
        assert_eq!(metrics.snapshot().await.catalog_boundary_stalled_lsn, 0);
        assert!(!*gate.stats.parked.borrow());
        assert_eq!(gate.stats.failures.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn hold_releases_after_mid_hold_reconnect() {
        // Walreceiver absent when the hold starts; a late attach + status
        // releases even after a stall warning
        let s = state();
        let gate = gate_with(s.clone(), Duration::from_millis(10));
        let attach = tokio::spawn({
            let s = s.clone();
            async move {
                tokio::time::sleep(Duration::from_millis(20)).await;
                let id = s
                    .lock()
                    .await
                    .register_connection(0x1000, 1, None)
                    .expect("current timeline");
                s.lock()
                    .await
                    .observe_status(id, 0x2000, Pos::new(0x2000), Pos::new(0x2000));
            }
        });
        gate.hold(0x1F00, Pos::new(0x2000), || true)
            .await
            .expect("released");
        attach.await.unwrap();
    }

    #[tokio::test]
    async fn dead_worker_wakes_waiter_with_err() {
        let s = state();
        let _ = s.lock().await.register_connection(0x1000, 1, None);
        let gate = gate_with(s, Duration::from_secs(30));
        let err = gate
            .hold(0x1F00, Pos::new(0x2000), || false)
            .await
            .expect_err("dead worker must fail the hold");
        assert!(err.to_string().contains("worker terminated"), "{err}");
        assert_eq!(gate.stats.failures.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn sink_parks_only_at_catalog_boundary() {
        // DML-only records (no boundary_info) pass straight
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
    async fn sink_never_parks_at_a_command_boundary_nobody_captures() {
        // Hold-only harness: the pending timeline is what a command
        // boundary exists for, so with no capture there is nothing to wait
        // on. No walreceiver here, so a park would time out
        let q = QueueingRecordSink::spawn(CountingRecordSink::default(), 4, 16, None);
        let gate = gate_with(state(), Duration::from_millis(10));
        let mut sink = BoundaryHoldSink::new(q, gate);
        let rec = Record {
            source_lsn: 0x1F00,
            next_lsn: 0x2000,
            boundary_info: Some(Arc::new(crate::record::BoundaryInfo {
                kind: BoundaryKind::Command { writer_xid: 7 },
                ..Default::default()
            })),
            ..Default::default()
        };
        sink.on_record(&rec).await.expect("no park");
        assert_eq!(sink.gate.stats.failures.load(Ordering::Relaxed), 0);
        sink.close().await.expect("close");
    }

    #[tokio::test]
    async fn sink_never_parks_at_a_statistics_only_commit() {
        // No walreceiver runs here, so waiting for replay would time out
        let q = QueueingRecordSink::spawn(CountingRecordSink::default(), 4, 16, None);
        let gate = gate_with(state(), Duration::from_millis(10));
        let mut sink = BoundaryHoldSink::new(q, gate);
        let stub = crate::record::BoundaryInfo {
            drain_xid: 7,
            stats_only: true,
            ..Default::default()
        };
        let rec = Record {
            source_lsn: 0x1F00,
            next_lsn: 0x2000,
            boundary_info: Some(Arc::new(stub)),
            ..Default::default()
        };
        sink.on_record(&rec).await.expect("no park");
        assert_eq!(sink.gate.stats.holds.load(Ordering::Relaxed), 0);
        assert_eq!(sink.gate.stats.failures.load(Ordering::Relaxed), 0);
        sink.close().await.expect("close");
    }

    #[tokio::test]
    async fn sink_boundary_flushes_partial_batch_and_releases() {
        // batch_size 64 with one record: without the forced flush the
        // commit strands in the pump-side buffer while the pump parks
        let s = state();
        let id = s
            .lock()
            .await
            .register_connection(0x1000, 1, None)
            .expect("current timeline");
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
                s.lock()
                    .await
                    .observe_status(id, 0x2000, Pos::new(0x2000), Pos::new(0x2000));
            }
        });
        let rec = Record {
            source_lsn: 0x1F00,
            next_lsn: 0x2000,
            boundary_info: Some(Arc::new(crate::record::BoundaryInfo::default())),
            ..Default::default()
        };
        sink.on_record(&rec).await.expect("boundary releases");
        releaser.await.unwrap();
        // Forced flush shipped the sub-batch-size buffer to the worker
        for _ in 0..1000 {
            if *counter.lock().unwrap() == 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
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
                Box::pin(async {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    Err(SinkError::Other("worker boom".into()))
                })
            }
        }
        let s = state();
        let _ = s.lock().await.register_connection(0x1000, 1, None);
        let q = QueueingRecordSink::spawn(Fail, 1, 4, None);
        let gate = gate_with(s, Duration::from_secs(30));
        let mut sink = BoundaryHoldSink::new(q, gate);
        // Predecessor record ships (batch_size 1) and kills the worker
        // mid-hold; the hold's worker_alive wake must surface that root
        // cause, never the generic hold error
        let dml = Record {
            source_lsn: 0x1E00,
            next_lsn: 0x1F00,
            ..Default::default()
        };
        let _ = sink.on_record(&dml).await;
        let rec = Record {
            source_lsn: 0x1F00,
            next_lsn: 0x2000,
            boundary_info: Some(Arc::new(crate::record::BoundaryInfo::default())),
            ..Default::default()
        };
        let err = sink.on_record(&rec).await.expect_err("must fail");
        assert!(err.to_string().contains("boom"), "{err}");
        assert_eq!(sink.gate.stats.failures.load(Ordering::Relaxed), 1);
    }

    fn boundary() -> Record<'static> {
        Record {
            source_lsn: 0x1F00,
            next_lsn: 0x2000,
            boundary_info: Some(Arc::new(crate::record::BoundaryInfo::default())),
            ..Default::default()
        }
    }

    /// Database visible from LSN 0 whose bridge never listens
    fn pending_sink(gate: CatalogBoundaryGate) -> BoundaryHoldSink {
        let q = QueueingRecordSink::spawn(CountingRecordSink::default(), 4, 16, None);
        let db = PendingDatabase {
            oid: 5,
            name: "late".into(),
            visible_at: 0,
            bridge_socket: PathBuf::new(),
        };
        BoundaryHoldSink::new(q, gate).with_pending(PendingDatabases::new(
            vec![db],
            Duration::ZERO,
            AttachSlot::default(),
        ))
    }

    #[tokio::test]
    async fn pending_attach_fails_when_bridge_never_listens() {
        let s = state();
        let id = s
            .lock()
            .await
            .register_connection(0x1000, 1, None)
            .expect("current timeline");
        s.lock()
            .await
            .observe_status(id, 0x2000, Pos::new(0x2000), Pos::new(0x2000));
        let mut sink = pending_sink(gate_with(s, Duration::from_secs(5)));
        let err = sink.on_record(&boundary()).await.expect_err("no bridge");
        assert!(err.to_string().contains("never came up"), "{err}");
        assert!(sink.pending.attached.lock().unwrap().is_none());
        sink.close().await.expect("close");
    }

    #[tokio::test]
    async fn aborted_tree_without_boundary_forwards() {
        let q = QueueingRecordSink::spawn(CountingRecordSink::default(), 1, 4, None);
        let mut sink = BoundaryHoldSink::new(q, gate_with(state(), Duration::from_millis(10)));
        let rec = Record {
            aborted_tree: Some(Arc::new(vec![7])),
            ..Default::default()
        };
        assert!(sink.may_park(&rec));
        sink.on_record(&rec).await.expect("forwarded");
        assert_eq!(sink.gate.stats.failures.load(Ordering::Relaxed), 0);
        sink.close().await.expect("close");
    }
}
