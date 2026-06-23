//! `RecordSink` wrapper decoupling per-record dispatch from the WAL
//! pump task.
//!
//! ## Why
//!
//! Pump awaits `on_wire_chunk` (shadow-wire bytes) then `on_record`
//! (decoder/xact buffer/emitter) per record. Decoder's
//! `ShadowCatalog::wait_for_replay` clears against bytes the wire
//! already pushed. Under DDL-mixed workload (`pgbench_acceptance`,
//! `kill_restart` drills) it can exceed one record latency, parking
//! the pump inside the await: bytes_sink stops firing, walsender
//! queues drain, shadow's walreceiver starves, apply LSN stalls below
//! `record.source_lsn`, wait trips its 30s catalog timeout. Deadlock.
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

use crate::classify::rmgr_label;
use crate::wal_stream::{Record, RecordSink, SinkError};
use crate::xact_buffer::TxnSpanRegistry;

/// Records batched before a channel send. Amortises per-send overhead
/// (atomic + alloc + wakeup); 64 lands channel cost near 8ns/record
/// over the clone-into-owned baseline.
pub const DEFAULT_QUEUEING_BATCH_SIZE: usize = 64;

/// Soft in-flight cap (channel batches + pump buffer). Past it the
/// pump yields so the worker drains. No hard cap: a permanently
/// stalled worker surfaces via the `wait_for_replay` timeout on the
/// shared err slot.
pub const DEFAULT_QUEUEING_RECORD_SINK_CAPACITY: usize = 16_384;

/// Worker `on_idle` cadence. Lets CH emitter's hold-INSERT-open
/// deadline fire shortly after `flush_timeout` without piling on
/// wakeups; deployments should match ~`flush_timeout / 2`.
pub const DEFAULT_QUEUEING_IDLE_INTERVAL: Duration = Duration::from_millis(50);

/// Construct via [`QueueingRecordSink::spawn`].
pub struct QueueingRecordSink {
    /// Each batch carries its ship `Instant` for the worker's `queued_ms`.
    tx: Option<mpsc::UnboundedSender<(Instant, Vec<Record<'static>>)>>,
    /// Pump-side accumulator; shipped as one message at `batch_size` (or `close`).
    buf: Vec<Record<'static>>,
    batch_size: usize,
    err: Arc<StdMutex<Option<SinkError>>>,
    in_flight: Arc<AtomicU64>,
    /// Records the worker has dispatched; with `in_flight`, tells a draining
    /// queue from a stalled one.
    processed: Arc<AtomicU64>,
    soft_cap: u64,
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
        soft_cap: usize,
        span_registry: Option<TxnSpanRegistry>,
    ) -> Self
    where
        S: RecordSink + Send + 'static,
    {
        Self::spawn_with_idle(
            inner,
            batch_size,
            soft_cap,
            DEFAULT_QUEUEING_IDLE_INTERVAL,
            span_registry,
        )
    }

    /// Worker owns `inner`, drains batches, dispatches each record.
    /// `soft_cap` triggers `yield_now` once in-flight (channel + pump
    /// buffer) exceeds it. `idle_interval` paces `inner.on_idle()` on a
    /// quiescent channel so time-based observer work (CH emitter's
    /// hold-INSERT-open deadline) fires without fresh records.
    pub fn spawn_with_idle<S>(
        mut inner: S,
        batch_size: usize,
        soft_cap: usize,
        idle_interval: Duration,
        span_registry: Option<TxnSpanRegistry>,
    ) -> Self
    where
        S: RecordSink + Send + 'static,
    {
        let (tx, mut rx) = mpsc::unbounded_channel::<(Instant, Vec<Record<'static>>)>();
        let err = Arc::new(StdMutex::new(None));
        let in_flight = Arc::new(AtomicU64::new(0));
        let processed = Arc::new(AtomicU64::new(0));
        let err_w = err.clone();
        let in_flight_w = in_flight.clone();
        let processed_w = processed.clone();
        let reg_w = span_registry.clone();
        let batch_size = batch_size.max(1);
        let idle_interval = idle_interval.max(Duration::from_millis(1));
        let worker = tokio::spawn(async move {
            // Park error, drop in-flight (`n`, or 0 on idle path),
            // close+drain so `in_flight` settles. Caller breaks after.
            let park_err_and_drain = async |e: SinkError,
                                            n: u64,
                                            rx: &mut mpsc::UnboundedReceiver<(
                Instant,
                Vec<Record<'static>>,
            )>| {
                tracing::error!(target: "walshadow::pipeline", error = %e, "queueing sink fatal");
                *err_w.lock().expect("queueing sink err slot poisoned") = Some(e);
                in_flight_w.fetch_sub(n, Ordering::Relaxed);
                rx.close();
                while let Some((_, rest)) = rx.recv().await {
                    in_flight_w.fetch_sub(rest.len() as u64, Ordering::Relaxed);
                }
            };
            'outer: loop {
                match tokio::time::timeout(idle_interval, rx.recv()).await {
                    Ok(Some((enqueued, batch))) => {
                        let n = batch.len() as u64;
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
                                let mut max_lsn: u64 = 0;
                                for record in &batch {
                                    // Open the `txn` span; verdict gates the
                                    // per-record spans.
                                    let sampled = reg_w.as_ref().is_some_and(|reg| {
                                        reg.note_popped(record.parsed.header.xact_id)
                                    });
                                    max_lsn = max_lsn.max(record.source_lsn);
                                    // One `record` span per record; `rm` tells
                                    // heap from commit records apart.
                                    let rec_span = trace_span!(
                                        sampled,
                                        "record",
                                        lsn = record.source_lsn,
                                        xid = record.parsed.header.xact_id,
                                        rm = rmgr_label(record.parsed.header.resource_manager_id),
                                    );
                                    inner.on_record(record).instrument(rec_span).await?;
                                }
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
                        processed_w.fetch_add(n, Ordering::Relaxed);
                        in_flight_w.fetch_sub(n, Ordering::Relaxed);
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
        });
        Self {
            tx: Some(tx),
            buf: Vec::with_capacity(batch_size),
            batch_size,
            err,
            in_flight,
            processed,
            soft_cap: soft_cap.max(1) as u64,
            worker: Some(worker),
            span_registry,
        }
    }

    /// Pump-queue depth: in-channel + pump-side buffer, for metrics.
    pub fn in_flight(&self) -> u64 {
        self.in_flight.load(Ordering::Relaxed) + self.buf.len() as u64
    }

    /// Records the worker has dispatched (see the `processed` field).
    pub fn processed(&self) -> u64 {
        self.processed.load(Ordering::Relaxed)
    }

    /// Ship the accumulated buffer without waiting for `batch_size`.
    /// Pump calls this after each chunk so a quiescent source can't
    /// strand commits in the pump-side buffer.
    pub async fn flush(&mut self) -> Result<(), SinkError> {
        if let Some(e) = self.take_pending_error() {
            return Err(e);
        }
        self.flush_buf()
    }

    fn flush_buf(&mut self) -> Result<(), SinkError> {
        if self.buf.is_empty() {
            return Ok(());
        }
        let batch = std::mem::replace(&mut self.buf, Vec::with_capacity(self.batch_size));
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
        if tx.send((Instant::now(), batch)).is_err() {
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

    /// Drop the sender + join the worker, surfacing any parked error.
    /// Call after the pump stops feeding records.
    pub async fn close(mut self) -> Result<(), SinkError> {
        // Flush tail before dropping sender so worker sees final batch.
        self.flush_buf()?;
        self.tx.take();
        if let Some(handle) = self.worker.take() {
            // Treat a worker panic as a sink error so daemon shutdown
            // surfaces it.
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
            // No `close()`: best-effort abort so the task doesn't leak.
            // Graceful shutdown should `close().await` first.
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
                // Soft backpressure: yield only when actually behind,
                // checked at flush time to keep per-record cost low.
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
    use walrus::pg::walparser::XLogRecord;

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
        let mut q = QueueingRecordSink::spawn(CaptureLsn(collected.clone()), 2, 8, None);
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
        let mut q = QueueingRecordSink::spawn(Fail, 1, 4, None);
        // First send returns before the worker consumes; spin until
        // the error parks so the next send hits the slot, not a race.
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
        let mut q = QueueingRecordSink::spawn(Counter(count.clone()), 4, 4, None);
        for lsn in 0..32 {
            q.on_record(&synth(lsn)).await.expect("send");
        }
        q.close().await.expect("close");
        assert_eq!(*count.lock().unwrap(), 32);
    }
}
