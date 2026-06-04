//! `RecordSink` wrapper that decouples per-record dispatch from the
//! WAL pump task.
//!
//! ## Why
//!
//! The streaming-shadow design fires `RecordBytesSink::on_wire_chunk`
//! (shadow-wire bytes) before `RecordSink::on_record` (decoder, xact
//! buffer, emitter) per record, with both calls awaited in the pump
//! task. The decoder's `ShadowCatalog::wait_for_replay` gate clears
//! against bytes the wire already pushed, so under steady workload it
//! resolves in milliseconds.
//!
//! Under sustained workload mixed with DDL (the
//! `pgbench_acceptance` and `kill_restart` drills),
//! `wait_for_replay` can take longer than one record latency. Because
//! the pump task is parked inside that await, the bytes_sink stops
//! firing on subsequent records → walsender's per-connection queues
//! drain → shadow's walreceiver stops getting WAL → shadow's apply
//! LSN stalls below `record.source_lsn` → the wait trips its 30 s
//! catalog timeout. Both sides deadlocked on each other.
//!
//! `QueueingRecordSink` breaks the lockstep: pump-side `on_record`
//! converts the record to a `'static` owned form, pushes it onto a
//! bounded `mpsc` channel, and returns immediately. A worker task
//! drains the channel through the real inner sink at its own pace.
//! The pump task keeps streaming bytes to `RecordBytesSink` (so
//! shadow keeps applying), and the decoder waits on shadow inside the
//! worker without blocking the wire.
//!
//! Errors from the worker (catalog timeout, decoder semantic error,
//! emitter I/O) surface back to the pump on the next `on_record` call
//! by way of a shared error slot — the daemon exits cleanly with the
//! actual root cause rather than hanging.

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::wal_stream::{Record, RecordSink, SinkError};

/// Pump-side buffer size: records collected before a batch is sent
/// onto the worker channel. Picked to amortise the per-send overhead
/// (atomic + alloc + wakeup) which dominates the queueing path at
/// small per-record sizes. 64 lands the channel cost near 8 ns per
/// record on top of the clone-into-owned baseline.
pub const DEFAULT_QUEUEING_BATCH_SIZE: usize = 64;

/// Soft cap on the in-flight queue (records, summed across batches in
/// the channel + the pump-side buffer). Past this the pump yields to
/// the runtime so the worker drains. The hard cap is open-ended — if
/// the worker permanently stalls, the catalog `wait_for_replay`
/// timeout surfaces an error on the shared err slot and the pump
/// bails on the next `on_record`.
pub const DEFAULT_QUEUEING_RECORD_SINK_CAPACITY: usize = 16_384;

/// Default idle-wakeup cadence for the worker's `on_idle` tick. Picked
/// to give the CH emitter's hold-INSERT-open deadline a chance to
/// fire shortly after `flush_timeout` elapses without piling on
/// wakeups during steady-state traffic. Concrete deployments should
/// match this to roughly `flush_timeout / 2`.
pub const DEFAULT_QUEUEING_IDLE_INTERVAL: Duration = Duration::from_millis(50);

/// Wraps any `RecordSink` so per-record dispatch runs on a separate
/// task. Construct via [`QueueingRecordSink::spawn`].
pub struct QueueingRecordSink {
    tx: Option<mpsc::UnboundedSender<Vec<Record<'static>>>>,
    /// Pump-side accumulator. `on_record` clones the record here;
    /// when `buf.len() >= batch_size` the buffer is shipped onto the
    /// channel as one message. Final-flush happens in `close()`.
    buf: Vec<Record<'static>>,
    batch_size: usize,
    err: Arc<StdMutex<Option<SinkError>>>,
    in_flight: Arc<AtomicU64>,
    soft_cap: u64,
    worker: Option<JoinHandle<()>>,
}

impl QueueingRecordSink {
    /// Spawn a worker task with the default idle cadence — see
    /// [`Self::spawn_with_idle`] for the full-control entry.
    pub fn spawn<S>(inner: S, batch_size: usize, soft_cap: usize) -> Self
    where
        S: RecordSink + Send + 'static,
    {
        Self::spawn_with_idle(inner, batch_size, soft_cap, DEFAULT_QUEUEING_IDLE_INTERVAL)
    }

    /// Spawn a worker task that owns `inner`. Pump-side `on_record`
    /// clones records into a local buffer; once `batch_size` records
    /// accumulate, the whole batch is shipped onto an unbounded
    /// channel. The worker drains batches and dispatches each record
    /// through `inner`. `soft_cap` triggers a `yield_now` once the
    /// in-flight count (channel + pump buffer) exceeds it.
    ///
    /// `idle_interval` controls how often the worker calls
    /// `inner.on_idle()` when the channel is quiescent. Lets time-based
    /// observer work (CH emitter's hold-INSERT-open deadline) fire
    /// without requiring fresh records to arrive.
    pub fn spawn_with_idle<S>(
        mut inner: S,
        batch_size: usize,
        soft_cap: usize,
        idle_interval: Duration,
    ) -> Self
    where
        S: RecordSink + Send + 'static,
    {
        let (tx, mut rx) = mpsc::unbounded_channel::<Vec<Record<'static>>>();
        let err = Arc::new(StdMutex::new(None));
        let in_flight = Arc::new(AtomicU64::new(0));
        let err_w = err.clone();
        let in_flight_w = in_flight.clone();
        let batch_size = batch_size.max(1);
        let idle_interval = idle_interval.max(Duration::from_millis(1));
        let worker = tokio::spawn(async move {
            // Park the first error, drop the in-flight batch (`n`, or 0 on
            // the idle path), then close+drain the channel so `in_flight`
            // settles. Caller `break 'outer`s after.
            let park_err_and_drain =
                async |e, n: u64, rx: &mut mpsc::UnboundedReceiver<Vec<Record<'static>>>| {
                    *err_w.lock().expect("queueing sink err slot poisoned") = Some(e);
                    in_flight_w.fetch_sub(n, Ordering::Relaxed);
                    rx.close();
                    while let Some(rest) = rx.recv().await {
                        in_flight_w.fetch_sub(rest.len() as u64, Ordering::Relaxed);
                    }
                };
            'outer: loop {
                match tokio::time::timeout(idle_interval, rx.recv()).await {
                    Ok(Some(batch)) => {
                        let n = batch.len() as u64;
                        let mut max_lsn: u64 = 0;
                        for record in &batch {
                            max_lsn = max_lsn.max(record.source_lsn);
                            if let Err(e) = inner.on_record(record).await {
                                park_err_and_drain(e, n, &mut rx).await;
                                break 'outer;
                            }
                        }
                        // Post-batch nudge: lets the inner sink advance
                        // its idle ack past trailing non-commit WAL.
                        // Pump emits records in LSN order, so `max_lsn`
                        // is a high-water mark for "everything ≤ this
                        // is dispatched."
                        if let Err(e) = inner.on_idle_advance(max_lsn).await {
                            park_err_and_drain(e, n, &mut rx).await;
                            break 'outer;
                        }
                        in_flight_w.fetch_sub(n, Ordering::Relaxed);
                    }
                    Ok(None) => {
                        // Channel closed by `QueueingRecordSink::close`.
                        // Give the inner sink one last shutdown tick
                        // (CH emitter force-flushes hold-open
                        // INSERTs here so the final flush window's
                        // rows reach CH durably before the worker
                        // exits).
                        if let Err(e) = inner.on_close().await {
                            *err_w.lock().expect("queueing sink err slot poisoned") = Some(e);
                        }
                        break 'outer;
                    }
                    Err(_) => {
                        // Idle wakeup: drive any time-based work the
                        // inner sink wants (CH emitter flush deadline).
                        if let Err(e) = inner.on_idle().await {
                            park_err_and_drain(e, 0, &mut rx).await;
                            break 'outer;
                        }
                    }
                }
            }
        });
        Self {
            tx: Some(tx),
            buf: Vec::with_capacity(batch_size),
            batch_size,
            err,
            in_flight,
            soft_cap: soft_cap.max(1) as u64,
            worker: Some(worker),
        }
    }

    /// Public flush. Ships whatever the pump has accumulated to the
    /// worker without waiting for `batch_size`. Pump loop calls this
    /// after each chunk so a quiescent source can't strand commits in
    /// the pump-side buffer.
    pub async fn flush(&mut self) -> Result<(), SinkError> {
        if let Some(e) = self.take_pending_error() {
            return Err(e);
        }
        self.flush_buf()
    }

    /// Ship the pump-side buffer to the worker. Called from
    /// `on_record` when the batch fills, and from `close()` to flush
    /// any tail.
    fn flush_buf(&mut self) -> Result<(), SinkError> {
        if self.buf.is_empty() {
            return Ok(());
        }
        let batch = std::mem::replace(&mut self.buf, Vec::with_capacity(self.batch_size));
        let n = batch.len() as u64;
        self.in_flight.fetch_add(n, Ordering::Relaxed);
        let tx = self
            .tx
            .as_ref()
            .ok_or_else(|| SinkError::Other("queueing record sink already closed".into()))?;
        if tx.send(batch).is_err() {
            self.in_flight.fetch_sub(n, Ordering::Relaxed);
            if let Some(e) = self.take_pending_error() {
                return Err(e);
            }
            return Err(SinkError::Other(
                "queueing record sink worker stopped".into(),
            ));
        }
        Ok(())
    }

    /// Drop the channel sender + join the worker. Any error the worker
    /// has parked surfaces here. Call after the pump has stopped
    /// feeding records.
    pub async fn close(mut self) -> Result<(), SinkError> {
        // Flush any pump-side tail before dropping the sender so the
        // worker sees the final batch.
        self.flush_buf()?;
        self.tx.take(); // drop sender so worker observes channel close
        if let Some(handle) = self.worker.take() {
            // Worker exits on `rx.recv() == None` (channel closed).
            // Join propagates panics; treat as a sink error so the
            // daemon's shutdown path surfaces them.
            if let Err(e) = handle.await {
                let msg = if e.is_panic() {
                    format!("queueing sink worker panicked: {e}")
                } else {
                    format!("queueing sink worker join error: {e}")
                };
                return Err(SinkError::Other(msg));
            }
        }
        if let Some(err) = self
            .err
            .lock()
            .expect("queueing sink err slot poisoned")
            .take()
        {
            return Err(err);
        }
        Ok(())
    }

    fn take_pending_error(&self) -> Option<SinkError> {
        self.err
            .lock()
            .expect("queueing sink err slot poisoned")
            .take()
    }
}

impl Drop for QueueingRecordSink {
    fn drop(&mut self) {
        if let Some(handle) = self.worker.take() {
            // Caller didn't `close()` — best-effort abort so the
            // runtime doesn't leak the task. Errors get dropped here;
            // a graceful shutdown should call `close().await` first.
            handle.abort();
        }
    }
}

impl RecordSink for QueueingRecordSink {
    fn on_record<'a>(
        &'a mut self,
        record: &'a Record<'a>,
    ) -> Pin<Box<dyn Future<Output = Result<(), SinkError>> + Send + 'a>> {
        Box::pin(async move {
            if let Some(e) = self.take_pending_error() {
                return Err(e);
            }
            self.buf.push(Record {
                parsed: record.parsed.clone().into_owned(),
                source_lsn: record.source_lsn,
                page_magic: record.page_magic,
                route: record.route,
            });
            if self.buf.len() >= self.batch_size {
                self.flush_buf()?;
                // Soft backpressure: yield once the worker is N
                // records behind so the runtime schedules it. Atomic
                // load on the hot path; only yields when actually
                // behind. Check only at flush time so the per-record
                // cost stays minimal.
                if self.in_flight.load(Ordering::Relaxed) > self.soft_cap {
                    tokio::task::yield_now().await;
                }
            }
            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wal_stream::Record;
    use wal_rs::pg::walparser::XLogRecord;

    fn synth(source_lsn: u64) -> Record<'static> {
        Record {
            parsed: XLogRecord::default(),
            source_lsn,
            page_magic: 0,
            route: crate::filter::Route::ToShadow,
        }
    }

    struct CaptureLsn(Arc<StdMutex<Vec<u64>>>);
    impl RecordSink for CaptureLsn {
        fn on_record<'a>(
            &'a mut self,
            r: &'a Record<'a>,
        ) -> Pin<Box<dyn Future<Output = Result<(), SinkError>> + Send + 'a>> {
            let sink = self.0.clone();
            let lsn = r.source_lsn;
            Box::pin(async move {
                sink.lock().unwrap().push(lsn);
                Ok(())
            })
        }
    }

    #[tokio::test]
    async fn forwards_records_in_order() {
        let collected = Arc::new(StdMutex::new(Vec::<u64>::new()));
        let mut q = QueueingRecordSink::spawn(CaptureLsn(collected.clone()), 2, 8);
        for lsn in [10, 20, 30, 40, 50] {
            q.on_record(&synth(lsn)).await.expect("send");
        }
        q.close().await.expect("close");
        assert_eq!(collected.lock().unwrap().as_slice(), &[10, 20, 30, 40, 50]);
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
        let mut q = QueueingRecordSink::spawn(Fail, 1, 4);
        // First send completes before the worker has consumed; the
        // error parks in the slot. Spin until it lands so the next
        // send hits the slot rather than racing the worker.
        let _ = q.on_record(&synth(1)).await;
        for _ in 0..50 {
            if q.err.lock().unwrap().is_some() {
                break;
            }
            tokio::task::yield_now().await;
        }
        let err = q
            .on_record(&synth(2))
            .await
            .expect_err("error must surface");
        assert!(matches!(err, SinkError::Other(s) if s.contains("boom")));
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
        let mut q = QueueingRecordSink::spawn(Counter(count.clone()), 4, 4);
        for lsn in 0..32 {
            q.on_record(&synth(lsn)).await.expect("send");
        }
        q.close().await.expect("close");
        assert_eq!(*count.lock().unwrap(), 32);
    }
}
