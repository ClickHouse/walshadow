//! `RecordSink` wrapper decoupling per-record dispatch from the WAL
//! pump task.
//!
//! ## Why
//!
//! Pump awaits `on_wire_chunk` (shadow-wire bytes) then `on_record`
//! (decoder/xact buffer/emitter) per record. Decoder's
//! `ShadowCatalog::wait_for_replay` targets bytes the wire already
//! pushed but clears only once shadow *applies* them: send queue →
//! walreceiver flush → startup replay → poll. Under DDL-mixed workload
//! the gate exceeds one record latency; awaited inline it parks the
//! pump for that whole round-trip and couples wire pacing to decode,
//! turning any delivery path that needs fresh pump bytes into a
//! deadlock.
//!
//! Break the lockstep: `on_record` owns the record `'static`, pushes
//! onto an mpsc channel, returns. Worker drains through the inner sink
//! at its own pace while pump keeps streaming bytes so shadow applies.
//!
//! Worker errors surface back to the pump on the next `on_record` via
//! a shared error slot, so the daemon exits with the root cause rather
//! than hanging.

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, Instant};

use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use tracing::Instrument;

use crate::decode::heap_decoder::Predecoded;
use crate::ops::trace::TxnSpanRegistry;
use crate::record::{Done, Record, RecordSink, SinkError};

/// Records batched before a channel send. Amortises per-send overhead
/// (atomic + alloc + wakeup).
pub const DEFAULT_QUEUEING_BATCH_SIZE: usize = 512;

/// Hard in-flight cap (channel batches + pump buffer). The channel is bounded
/// at `max_records / batch_size`, so past it the pump's `send` blocks — real
/// backpressure to the source instead of unbounded RAM growth. Deadlock-safe:
/// shadow is fed by an independent walsender task (+keepalive), so a parked
/// pump can't starve `wait_for_replay`.
pub const DEFAULT_QUEUEING_RECORD_SINK_CAPACITY: usize = 131_072;

/// Worker `on_idle` cadence. Lets CH emitter's hold-INSERT-open
/// deadline fire shortly after `flush_timeout` without piling on
/// wakeups; deployments should match ~`flush_timeout / 2`.
pub const DEFAULT_QUEUEING_IDLE_INTERVAL: Duration = Duration::from_millis(50);

/// Longest stretch the unconstrained worker polls before yielding. Under
/// backlog every await is ready, so without a yield it holds its runtime
/// thread, starving the IO driver and the task it last woke into tokio's
/// unstealable LIFO slot
const YIELD_INTERVAL: Duration = Duration::from_millis(1);

/// Pure per-record decode run ahead of the worker; see
/// [`QueueingRecordSink::with_predecode`]
pub type Predecode = Arc<dyn Fn(&Record<'_>) -> Option<Predecoded> + Send + Sync>;

/// Batch records, or the blocking-pool job decoding them ahead
enum Records {
    Ready(Vec<Record<'static>>),
    Decoding(JoinHandle<(Vec<Record<'static>>, Vec<Option<Predecoded>>)>),
}

/// One channel message: wanted records plus the position of those the inner
/// sink's [`RecordSink::wants`] skipped pump-side
struct Batch {
    /// Ship instant, for the worker's `queued_ms`
    shipped: Instant,
    records: Records,
    len: u64,
    skipped: u64,
    /// Highest `source_lsn` among skipped records, 0 when none
    skipped_lsn: u64,
}

/// Construct via [`QueueingRecordSink::spawn`].
pub struct QueueingRecordSink {
    tx: Option<mpsc::Sender<Batch>>,
    /// Pump-side accumulator; shipped as one message at `batch_size` (or `close`).
    buf: Vec<Record<'static>>,
    /// Records `wants` dropped since the last ship, and their high-water LSN
    skipped: u64,
    skipped_lsn: u64,
    /// Inner sink's [`RecordSink::wants`], evaluated before the owned clone
    wants: fn(&Record<'_>) -> bool,
    predecode: Option<Predecode>,
    batch_size: usize,
    err: Arc<StdMutex<Option<SinkError>>>,
    in_flight: Arc<AtomicU64>,
    /// Records the worker has dispatched; with `in_flight`, tells a draining
    /// queue from a stalled one.
    processed: Arc<AtomicU64>,
    send_wait_nanos: u64,
    worker: Option<JoinHandle<()>>,
    /// Per-txn span map; `Some` only with OTLP on. `flush_buf` stamps each
    /// shipped record's ship instant (`note_shipped`).
    span_registry: Option<TxnSpanRegistry>,
}

impl QueueingRecordSink {
    /// Default idle cadence; see [`Self::spawn_with_idle`].
    pub fn spawn<S>(
        inner: S,
        batch_size: usize,
        max_records: usize,
        span_registry: Option<TxnSpanRegistry>,
    ) -> Self
    where
        S: RecordSink + Send + 'static,
    {
        Self::spawn_with_idle(
            inner,
            batch_size,
            max_records,
            DEFAULT_QUEUEING_IDLE_INTERVAL,
            span_registry,
        )
    }

    /// Worker owns `inner`, drains batches, dispatches each record.
    /// `max_records` bounds records in flight (channel + pump buffer):
    /// the channel holds `max_records / batch_size` batches, so a slow
    /// worker blocks the pump's send instead of growing without limit.
    /// `idle_interval` paces `inner.on_idle()` on a quiescent channel so
    /// time-based observer work (CH emitter's hold-INSERT-open deadline)
    /// fires without fresh records.
    pub fn spawn_with_idle<S>(
        mut inner: S,
        batch_size: usize,
        max_records: usize,
        idle_interval: Duration,
        span_registry: Option<TxnSpanRegistry>,
    ) -> Self
    where
        S: RecordSink + Send + 'static,
    {
        let batch_size = batch_size.max(1);
        let channel_cap = (max_records / batch_size).max(1);
        let (tx, mut rx) = mpsc::channel::<Batch>(channel_cap);
        let err = Arc::new(StdMutex::new(None));
        let in_flight = Arc::new(AtomicU64::new(0));
        let processed = Arc::new(AtomicU64::new(0));
        let err_w = err.clone();
        let in_flight_w = in_flight.clone();
        let processed_w = processed.clone();
        let reg_w = span_registry.clone();
        let idle_interval = idle_interval.max(Duration::from_millis(1));
        // Unconstrained: the worker's per-record mutex awaits exhaust the coop
        // budget, and each forced yield requeues the serial stage behind every
        // other task
        let worker = tokio::spawn(tokio::task::coop::unconstrained(async move {
            // Park error, drop in-flight (`n`, or 0 on idle path),
            // close+drain so `in_flight` settles. Caller breaks after.
            let park_err_and_drain =
                async |e: SinkError, n: u64, rx: &mut mpsc::Receiver<Batch>| {
                    tracing::error!(target: "walshadow::pipeline", error = %e, "queueing sink fatal");
                    *err_w.lock().expect("queueing sink err slot poisoned") = Some(e);
                    in_flight_w.fetch_sub(n, Ordering::Relaxed);
                    rx.close();
                    while let Some(rest) = rx.recv().await {
                        in_flight_w.fetch_sub(rest.len, Ordering::Relaxed);
                    }
                };
            let mut last_yield = Instant::now();
            'outer: loop {
                match tokio::time::timeout(idle_interval, rx.recv()).await {
                    Ok(Some(Batch {
                        shipped: enqueued,
                        records,
                        len: n,
                        skipped,
                        skipped_lsn,
                    })) => {
                        let (batch, mut pre) = match records {
                            Records::Ready(batch) => (batch, Vec::new()),
                            Records::Decoding(job) => match job.await {
                                Ok(decoded) => decoded,
                                Err(e) => {
                                    let e = SinkError::Other(format!("predecode job: {e}"));
                                    park_err_and_drain(e, n, &mut rx).await;
                                    break 'outer;
                                }
                            },
                        };
                        // Batch spans only when some record is sampled.
                        let batch_sampled = reg_w.as_ref().is_some_and(|reg| {
                            batch
                                .iter()
                                .any(|r| reg.is_sampled(r.parsed.header.xact_id))
                        });
                        // `queued_ms` = time this batch waited for pickup.
                        let queuingthread = trace_span!(
                            batch_sampled,
                            "queuingthread",
                            queued_ms = enqueued.elapsed().as_secs_f64() * 1e3,
                        );
                        let outcome: Result<(), SinkError> = async {
                            // Nests under `queuingthread`; one per dequeued batch.
                            let batch_span = trace_span!(batch_sampled, "batch", batch_size = n);
                            async {
                                let mut max_lsn = skipped_lsn;
                                for record in &batch {
                                    max_lsn = max_lsn.max(record.source_lsn);
                                    if let Some(reg) = reg_w.as_ref() {
                                        reg.note_popped(record.parsed.header.xact_id);
                                    }
                                }
                                inner.on_records(&batch, &mut pre).await?;
                                // Advance idle ack past trailing non-commit WAL
                                // (`max_lsn` = dispatched high-water).
                                inner.on_idle_advance(max_lsn).await
                            }
                            .instrument(batch_span)
                            .await
                        }
                        .instrument(queuingthread)
                        .await;
                        if let Err(e) = outcome {
                            park_err_and_drain(e, n, &mut rx).await;
                            break 'outer;
                        }
                        processed_w.fetch_add(n + skipped, Ordering::Relaxed);
                        in_flight_w.fetch_sub(n, Ordering::Relaxed);
                        if last_yield.elapsed() >= YIELD_INTERVAL {
                            tokio::task::yield_now().await;
                            last_yield = Instant::now();
                        }
                    }
                    Ok(None) => {
                        // Channel closed by `close`. Final shutdown
                        // tick: CH emitter force-flushes hold-open
                        // INSERTs so the last window's rows reach CH
                        // durably before the worker exits.
                        if let Err(e) = inner.on_close().await {
                            tracing::error!(target: "walshadow::pipeline", error = %e, "queueing sink fatal on close");
                            *err_w.lock().expect("queueing sink err slot poisoned") = Some(e);
                        }
                        break 'outer;
                    }
                    Err(_) => {
                        // Idle wakeup: drive time-based inner work
                        // (CH emitter flush deadline).
                        if let Err(e) = inner.on_idle().await {
                            park_err_and_drain(e, 0, &mut rx).await;
                            break 'outer;
                        }
                    }
                }
            }
        }));
        Self {
            tx: Some(tx),
            buf: Vec::with_capacity(batch_size),
            skipped: 0,
            skipped_lsn: 0,
            wants: S::wants,
            predecode: None,
            batch_size,
            err,
            in_flight,
            processed,
            send_wait_nanos: 0,
            worker: Some(worker),
            span_registry,
        }
    }

    /// Decode each shipped batch on the blocking pool before the worker takes
    /// it, overlapping the worker's serial pass over earlier batches. The
    /// worker hands the results to [`RecordSink::on_records`]
    pub fn with_predecode(mut self, predecode: Predecode) -> Self {
        self.predecode = Some(predecode);
        self
    }

    /// Pump-queue depth: in-channel + pump-side buffer, for metrics.
    pub fn in_flight(&self) -> u64 {
        self.in_flight.load(Ordering::Relaxed) + self.buf.len() as u64
    }

    /// Records the worker has dispatched (see the `processed` field).
    pub fn processed(&self) -> u64 {
        self.processed.load(Ordering::Relaxed)
    }

    pub fn send_wait_seconds(&self) -> f64 {
        self.send_wait_nanos as f64 / 1e9
    }

    /// False once the worker task ended: fatal-error drain closes the
    /// channel, a panic drops the receiver. Lets a publication hold detect
    /// worker death without shipping a record.
    pub fn worker_alive(&self) -> bool {
        self.tx.as_ref().is_some_and(|tx| !tx.is_closed())
    }

    /// Ship the accumulated buffer without waiting for `batch_size`.
    /// Pump calls this after each chunk so a quiescent source can't
    /// strand commits in the pump-side buffer, and an idle pump learns
    /// the worker died
    pub async fn flush(&mut self) -> Result<(), SinkError> {
        if !self.worker_alive() {
            return Err(self.worker_stopped());
        }
        self.flush_buf().await
    }

    async fn flush_buf(&mut self) -> Result<(), SinkError> {
        if let Some(e) = self.take_pending_error() {
            return Err(e);
        }
        if self.buf.is_empty() && self.skipped == 0 {
            return Ok(());
        }
        let batch = std::mem::replace(&mut self.buf, Vec::with_capacity(self.batch_size));
        let skipped = std::mem::take(&mut self.skipped);
        let skipped_lsn = std::mem::take(&mut self.skipped_lsn);
        // Stamp the ship instant for each xid in the shipped batch (feeds the
        // `txn` span's fill_ms/queue_ms tags at note_popped).
        if let Some(reg) = &self.span_registry {
            for r in &batch {
                reg.note_shipped(r.parsed.header.xact_id);
            }
        }
        let n = batch.len() as u64;
        self.in_flight.fetch_add(n, Ordering::Relaxed);
        let tx = self
            .tx
            .as_ref()
            .ok_or_else(|| SinkError::Other("queueing record sink already closed".into()))?;
        let records = match &self.predecode {
            Some(predecode) if !batch.is_empty() => {
                let predecode = predecode.clone();
                Records::Decoding(tokio::task::spawn_blocking(move || {
                    let pre = batch.iter().map(|r| predecode(r)).collect();
                    (batch, pre)
                }))
            }
            _ => Records::Ready(batch),
        };
        // Blocking the pump here is deadlock-safe: shadow is fed by an independent
        // walsender task (+keepalive), so a parked pump can't starve wait_for_replay.
        let started = Instant::now();
        let sent = tx
            .send(Batch {
                shipped: started,
                records,
                len: n,
                skipped,
                skipped_lsn,
            })
            .await;
        self.send_wait_nanos = self
            .send_wait_nanos
            .saturating_add(started.elapsed().as_nanos() as u64);
        if sent.is_err() {
            self.in_flight.fetch_sub(n, Ordering::Relaxed);
            return Err(self.worker_stopped());
        }
        Ok(())
    }

    /// Drop the sender + join the worker, surfacing any parked error.
    /// Call after the pump stops feeding records.
    pub async fn close(mut self) -> Result<(), SinkError> {
        // Flush tail before dropping sender so worker sees final batch.
        self.flush_buf().await?;
        self.tx.take();
        // Treat a worker panic as a sink error so daemon shutdown surfaces it
        let worker = self.worker.take().expect("only close and drop take worker");
        worker
            .await
            .map_err(|e| SinkError::Other(format!("queueing sink worker: {e}")))?;
        self.take_pending_error().map_or(Ok(()), Err)
    }

    fn take_pending_error(&self) -> Option<SinkError> {
        self.err
            .lock()
            .expect("queueing sink err slot poisoned")
            .take()
    }

    /// Parked root cause, else generic; a panic parks nothing
    fn worker_stopped(&self) -> SinkError {
        self.take_pending_error()
            .unwrap_or_else(|| SinkError::Other("queueing record sink worker stopped".into()))
    }
}

impl Drop for QueueingRecordSink {
    fn drop(&mut self) {
        if let Some(handle) = self.worker.take() {
            // No `close()`: best-effort abort so the task doesn't leak.
            // Graceful shutdown should `close().await` first.
            handle.abort();
        }
    }
}

enum Admit {
    Dead,
    Skip { ship: bool },
    Take,
}

impl QueueingRecordSink {
    /// Whether to buffer `record`; a skipped one only advances position
    fn admit(&mut self, record: &Record<'_>) -> Admit {
        // Closed channel means the worker parked an error or died
        if !self.worker_alive() {
            return Admit::Dead;
        }
        if !(self.wants)(record) {
            self.skipped += 1;
            self.skipped_lsn = self.skipped_lsn.max(record.source_lsn);
            // Ship position-only runs too, so the idle ack keeps moving
            return Admit::Skip {
                ship: self.skipped >= self.batch_size as u64,
            };
        }
        Admit::Take
    }

    /// Act on `admit`, owning the record only when buffering it
    fn enqueue(
        &mut self,
        admit: Admit,
        own: impl FnOnce() -> Record<'static>,
    ) -> Pin<Box<dyn Future<Output = Result<(), SinkError>> + Send + '_>> {
        match admit {
            Admit::Dead => Box::pin(std::future::ready(Err(self.worker_stopped()))),
            Admit::Skip { ship: true } => Box::pin(self.flush_buf()),
            Admit::Skip { ship: false } => Box::pin(Done),
            Admit::Take => {
                self.buf.push(own());
                if self.buf.len() >= self.batch_size {
                    return Box::pin(self.flush_buf());
                }
                Box::pin(Done)
            }
        }
    }
}

impl RecordSink for QueueingRecordSink {
    /// Backpressure parks on the worker, never on shadow replay
    fn may_park(&self, _record: &Record<'_>) -> bool {
        false
    }

    fn on_record<'a>(
        &'a mut self,
        record: &'a Record<'a>,
    ) -> Pin<Box<dyn Future<Output = Result<(), SinkError>> + Send + 'a>> {
        let admit = self.admit(record);
        self.enqueue(admit, || record.to_static())
    }

    fn on_record_owned(
        &mut self,
        record: Record<'static>,
    ) -> Pin<Box<dyn Future<Output = Result<(), SinkError>> + Send + '_>> {
        let admit = self.admit(&record);
        self.enqueue(admit, || record)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::record::Record;
    use walrus::pg::walparser::XLogRecord;

    fn synth(source_lsn: u64) -> Record<'static> {
        Record {
            parsed: XLogRecord::default(),
            source_lsn,
            route: crate::record::Route::ToShadow,
            ..Default::default()
        }
    }

    struct CaptureLsn(Arc<StdMutex<Vec<(u64, bool)>>>);
    impl RecordSink for CaptureLsn {
        fn on_record<'a>(
            &'a mut self,
            r: &'a Record<'a>,
        ) -> Pin<Box<dyn Future<Output = Result<(), SinkError>> + Send + 'a>> {
            let sink = self.0.clone();
            let item = (r.source_lsn, r.boundary_info.is_some());
            Box::pin(async move {
                sink.lock().unwrap().push(item);
                Ok(())
            })
        }
    }

    #[tokio::test]
    async fn forwards_records_in_order() {
        let collected = Arc::new(StdMutex::new(Vec::new()));
        let mut q = QueueingRecordSink::spawn(CaptureLsn(collected.clone()), 2, 8, None);
        for lsn in [10, 20, 30, 40] {
            q.on_record(&synth(lsn)).await.expect("send");
        }
        // `boundary_info` must survive the clone-to-owned hop: the worker's
        // drain reads it
        let mut ddl = synth(50);
        ddl.boundary_info = Some(std::sync::Arc::new(crate::record::BoundaryInfo::default()));
        q.on_record(&ddl).await.expect("send");
        q.close().await.expect("close");
        assert_eq!(
            collected.lock().unwrap().as_slice(),
            &[
                (10, false),
                (20, false),
                (30, false),
                (40, false),
                (50, true),
            ],
        );
    }

    #[tokio::test]
    async fn forwards_records_in_order_across_yields() {
        struct Slow(CaptureLsn);
        impl RecordSink for Slow {
            fn on_record<'a>(
                &'a mut self,
                r: &'a Record<'a>,
            ) -> Pin<Box<dyn Future<Output = Result<(), SinkError>> + Send + 'a>> {
                std::thread::sleep(YIELD_INTERVAL);
                self.0.on_record(r)
            }
        }
        let collected = Arc::new(StdMutex::new(Vec::new()));
        let mut q = QueueingRecordSink::spawn(Slow(CaptureLsn(collected.clone())), 2, 8, None);
        for lsn in 1..=8 {
            q.on_record(&synth(lsn)).await.expect("send");
        }
        q.close().await.expect("close");
        let lsns: Vec<u64> = collected.lock().unwrap().iter().map(|&(l, _)| l).collect();
        assert_eq!(lsns, (1..=8).collect::<Vec<_>>());
    }

    #[tokio::test]
    async fn skipped_records_advance_position() {
        struct Selective(Arc<StdMutex<Vec<(&'static str, u64)>>>);

        impl RecordSink for Selective {
            fn wants(record: &Record<'_>) -> bool {
                record.source_lsn > 20
            }

            fn on_record<'a>(
                &'a mut self,
                record: &'a Record<'a>,
            ) -> Pin<Box<dyn Future<Output = Result<(), SinkError>> + Send + 'a>> {
                self.0.lock().unwrap().push(("record", record.source_lsn));
                Box::pin(Done)
            }

            fn on_idle_advance<'a>(
                &'a mut self,
                lsn: u64,
            ) -> Pin<Box<dyn Future<Output = Result<(), SinkError>> + Send + 'a>> {
                Box::pin(async move {
                    self.0.lock().unwrap().push(("advance", lsn));
                    Ok(())
                })
            }
        }

        let positions = Arc::new(StdMutex::new(Vec::new()));
        let mut q = QueueingRecordSink::spawn(Selective(positions.clone()), 2, 8, None);
        let processed = q.processed.clone();
        let in_flight = q.in_flight.clone();
        for lsn in [10, 20, 30, 40] {
            q.on_record(&synth(lsn)).await.expect("send");
        }
        q.close().await.expect("close");

        assert_eq!(
            *positions.lock().unwrap(),
            [
                ("advance", 20),
                ("record", 30),
                ("record", 40),
                ("advance", 40)
            ]
        );
        assert_eq!(processed.load(Ordering::Relaxed), 4);
        assert_eq!(in_flight.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn surfaces_worker_error() {
        struct Fail;
        impl RecordSink for Fail {
            fn on_record<'a>(
                &'a mut self,
                _r: &'a Record<'a>,
            ) -> Pin<Box<dyn Future<Output = Result<(), SinkError>> + Send + 'a>> {
                Box::pin(async move { Err(SinkError::Other("boom".into())) })
            }
        }
        let mut q = QueueingRecordSink::spawn(Fail, 1, 4, None);
        // First send returns before the worker consumes; wait until the
        // error parks so the next send hits the slot, not a race.
        let _ = q.on_record(&synth(1)).await;
        for _ in 0..1000 {
            if q.err.lock().unwrap().is_some() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        let err = q
            .on_record(&synth(2))
            .await
            .expect_err("error must surface");
        assert!(matches!(err, SinkError::Other(s) if s.contains("boom")));
    }

    #[tokio::test]
    async fn worker_alive_false_after_worker_error() {
        struct Fail;
        impl RecordSink for Fail {
            fn on_record<'a>(
                &'a mut self,
                _r: &'a Record<'a>,
            ) -> Pin<Box<dyn Future<Output = Result<(), SinkError>> + Send + 'a>> {
                Box::pin(async move { Err(SinkError::Other("boom".into())) })
            }
        }
        let mut q = QueueingRecordSink::spawn(Fail, 1, 4, None);
        assert!(q.worker_alive());
        let _ = q.on_record(&synth(1)).await;
        // Fatal path closes the channel; a boundary hold polls this
        // without shipping a record
        for _ in 0..1000 {
            if !q.worker_alive() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        panic!("worker_alive stayed true after fatal worker error");
    }

    #[tokio::test]
    async fn close_drains_pending() {
        let count = Arc::new(StdMutex::new(0u64));
        struct Counter(Arc<StdMutex<u64>>);
        impl RecordSink for Counter {
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
        let mut q = QueueingRecordSink::spawn(Counter(count.clone()), 4, 4, None);
        for lsn in 0..32 {
            q.on_record(&synth(lsn)).await.expect("send");
        }
        q.close().await.expect("close");
        assert_eq!(*count.lock().unwrap(), 32);
    }

    /// Accepts every record; `on_idle` and `on_close` behave per `F`
    struct Hooks<F>(F);
    impl<F: FnMut(&'static str) -> Result<(), SinkError> + Send> RecordSink for Hooks<F> {
        fn on_record<'a>(
            &'a mut self,
            _r: &'a Record<'a>,
        ) -> Pin<Box<dyn Future<Output = Result<(), SinkError>> + Send + 'a>> {
            Box::pin(Done)
        }

        fn on_idle<'a>(
            &'a mut self,
        ) -> Pin<Box<dyn Future<Output = Result<(), SinkError>> + Send + 'a>> {
            let r = (self.0)("idle");
            Box::pin(async move { r })
        }

        fn on_close<'a>(
            &'a mut self,
        ) -> Pin<Box<dyn Future<Output = Result<(), SinkError>> + Send + 'a>> {
            let r = (self.0)("close");
            Box::pin(async move { r })
        }
    }

    async fn wait_worker_dead(q: &QueueingRecordSink) {
        tokio::time::timeout(Duration::from_secs(1), async {
            while q.worker_alive() {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("worker stayed alive");
    }

    #[tokio::test]
    async fn never_parks_on_replay() {
        let q = QueueingRecordSink::spawn(Hooks(|_| Ok(())), 1, 4, None);
        assert!(!q.may_park(&synth(1)));
    }

    #[tokio::test]
    async fn error_drains_queued_batches() {
        struct GatedFail(Arc<tokio::sync::Notify>);
        impl RecordSink for GatedFail {
            fn on_record<'a>(
                &'a mut self,
                _r: &'a Record<'a>,
            ) -> Pin<Box<dyn Future<Output = Result<(), SinkError>> + Send + 'a>> {
                let gate = self.0.clone();
                Box::pin(async move {
                    gate.notified().await;
                    Err(SinkError::Other("boom".into()))
                })
            }
        }
        let gate = Arc::new(tokio::sync::Notify::new());
        let mut q = QueueingRecordSink::spawn(GatedFail(gate.clone()), 1, 4, None);
        let in_flight = q.in_flight.clone();
        // Worker blocks on the first record while the rest queue behind it
        for lsn in [1, 2, 3] {
            q.on_record(&synth(lsn)).await.expect("send");
        }
        assert_eq!(in_flight.load(Ordering::Relaxed), 3);
        gate.notify_one();
        wait_worker_dead(&q).await;
        assert_eq!(in_flight.load(Ordering::Relaxed), 0);
        let err = q.close().await.expect_err("parked error");
        assert!(matches!(err, SinkError::Other(s) if s.contains("boom")));
    }

    #[tokio::test]
    async fn predecode_panic_surfaces() {
        let mut q = QueueingRecordSink::spawn(Hooks(|_| Ok(())), 1, 4, None)
            .with_predecode(Arc::new(|_| panic!("predecode bug")));
        let in_flight = q.in_flight.clone();
        q.on_record(&synth(1)).await.expect("send");
        wait_worker_dead(&q).await;
        assert_eq!(in_flight.load(Ordering::Relaxed), 0);
        let err = q.close().await.expect_err("predecode error");
        assert!(matches!(err, SinkError::Other(s) if s.contains("predecode job")));
    }

    #[tokio::test]
    async fn predecode_results_reach_inner_by_index() {
        struct PreLens(Arc<StdMutex<Vec<(usize, usize)>>>);
        impl RecordSink for PreLens {
            fn on_record<'a>(
                &'a mut self,
                _r: &'a Record<'a>,
            ) -> Pin<Box<dyn Future<Output = Result<(), SinkError>> + Send + 'a>> {
                Box::pin(Done)
            }

            fn on_records<'a>(
                &'a mut self,
                batch: &'a [Record<'a>],
                pre: &'a mut [Option<Predecoded>],
            ) -> Pin<Box<dyn Future<Output = Result<(), SinkError>> + Send + 'a>> {
                self.0.lock().unwrap().push((batch.len(), pre.len()));
                Box::pin(async move {
                    for record in batch {
                        self.on_record(record).await?;
                    }
                    Ok(())
                })
            }
        }
        let seen = Arc::new(StdMutex::new(Vec::new()));
        let calls = Arc::new(AtomicU64::new(0));
        let calls_p = calls.clone();
        let mut q = QueueingRecordSink::spawn(PreLens(seen.clone()), 2, 8, None).with_predecode(
            Arc::new(move |_| {
                calls_p.fetch_add(1, Ordering::Relaxed);
                None
            }),
        );
        for lsn in [1, 2, 3] {
            q.on_record(&synth(lsn)).await.expect("send");
        }
        q.close().await.expect("close");
        assert_eq!(*seen.lock().unwrap(), [(2, 2), (1, 1)]);
        assert_eq!(calls.load(Ordering::Relaxed), 3);
    }

    #[tokio::test]
    async fn idle_error_surfaces() {
        let q = QueueingRecordSink::spawn_with_idle(
            // Fatal idle ends the worker, so `on_close` never runs
            Hooks(|_| Err(SinkError::Other("idle boom".into()))),
            1,
            4,
            Duration::from_millis(1),
            None,
        );
        wait_worker_dead(&q).await;
        let err = q.close().await.expect_err("idle error");
        assert!(matches!(err, SinkError::Other(s) if s.contains("idle boom")));
    }

    #[tokio::test]
    async fn close_error_surfaces_after_join() {
        let mut q = QueueingRecordSink::spawn_with_idle(
            Hooks(|hook| Err(SinkError::Other(format!("{hook} boom")))),
            1,
            4,
            Duration::MAX,
            None,
        );
        q.on_record(&synth(1)).await.expect("send");
        let err = q.close().await.expect_err("close error");
        assert!(matches!(err, SinkError::Other(s) if s.contains("close boom")));
    }

    fn panicking_worker() -> QueueingRecordSink {
        QueueingRecordSink::spawn_with_idle(
            Hooks(|_| -> Result<(), SinkError> { panic!("idle bug") }),
            4,
            8,
            Duration::from_millis(1),
            None,
        )
    }

    #[tokio::test]
    async fn worker_panic_fails_record_flush_and_close() {
        let mut q = panicking_worker();
        wait_worker_dead(&q).await;
        // Panic parks no error and the buffer is empty, still must fail
        let err = q
            .on_record(&synth(1))
            .await
            .expect_err("record to dead worker");
        assert!(matches!(err, SinkError::Other(s) if s.contains("worker stopped")));
        q.flush().await.expect_err("idle flush to dead worker");
        let err = q.close().await.expect_err("join error");
        assert!(matches!(err, SinkError::Other(s) if s.contains("panicked")));
    }

    #[tokio::test]
    async fn close_ships_tail_to_panicked_worker() {
        let mut q = panicking_worker();
        let in_flight = q.in_flight.clone();
        q.on_record(&synth(1)).await.expect("buffered");
        wait_worker_dead(&q).await;
        let err = q.close().await.expect_err("send to dead worker");
        assert!(matches!(err, SinkError::Other(s) if s.contains("worker stopped")));
        assert_eq!(in_flight.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn sampled_xact_opens_txn_span() {
        let _g = crate::ops::trace::SAMPLE_TEST_GUARD.lock().await;
        crate::ops::trace::set_sample_ratio(1.0);
        let reg = TxnSpanRegistry::new();
        let mut r = synth(1);
        r.parsed.header.xact_id = 7;
        reg.open(7, r.source_lsn);
        assert!(reg.is_sampled(7));
        let mut q = QueueingRecordSink::spawn(Hooks(|_| Ok(())), 1, 4, Some(reg.clone()));
        q.on_record(&r).await.expect("send");
        q.close().await.expect("close");
        // Pump stamps ship, worker pop then builds the span from it
        assert!(reg.txn_span(7).is_some());
    }
}
