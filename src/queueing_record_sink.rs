//! `RecordSink` wrapper that decouples per-record dispatch from the
//! WAL pump task.
//!
//! ## Why
//!
//! The PHASE13 §3 design fires `RecordBytesSink::on_wire_chunk`
//! (shadow-wire bytes) before `RecordSink::on_record` (decoder, xact
//! buffer, emitter) per record, with both calls awaited in the pump
//! task. The decoder's `ShadowCatalog::wait_for_replay` gate clears
//! against bytes the wire already pushed, so under steady workload it
//! resolves in milliseconds.
//!
//! Under sustained workload mixed with DDL (the
//! `phase14_pgbench_acceptance` and `phase14_kill_restart` drills),
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
use std::sync::{Arc, Mutex as StdMutex};

use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::wal_stream::{Record, RecordSink, SinkError};

/// Default channel capacity. Sized to absorb the worker's worst-case
/// `wait_for_replay` (30 s catalog timeout) at typical record cadence
/// — at ~120 byte records and 10 MiB/s WAL rate that's 2.5 M records.
/// Picked a budget of ~256 k records (~30 MiB at the small-record
/// extreme, more for fat records). The pump blocks on send past this
/// point, which is the right escalation: the worker has stalled long
/// enough that surfacing the error is preferable to unbounded memory.
pub const DEFAULT_QUEUEING_RECORD_SINK_CAPACITY: usize = 262_144;

/// Wraps any `RecordSink` so per-record dispatch runs on a separate
/// task. Construct via [`QueueingRecordSink::spawn`].
pub struct QueueingRecordSink {
    tx: Option<mpsc::Sender<Record<'static>>>,
    err: Arc<StdMutex<Option<SinkError>>>,
    worker: Option<JoinHandle<()>>,
}

impl QueueingRecordSink {
    /// Spawn a worker task that owns `inner`. Records arriving via
    /// [`RecordSink::on_record`] are cloned into `'static` form and
    /// shipped through a bounded channel of `capacity` records.
    pub fn spawn<S>(mut inner: S, capacity: usize) -> Self
    where
        S: RecordSink + Send + 'static,
    {
        let (tx, mut rx) = mpsc::channel::<Record<'static>>(capacity.max(1));
        let err = Arc::new(StdMutex::new(None));
        let err_w = err.clone();
        let worker = tokio::spawn(async move {
            while let Some(record) = rx.recv().await {
                if let Err(e) = inner.on_record(&record).await {
                    *err_w.lock().expect("queueing sink err slot poisoned") = Some(e);
                    // Drain remaining items so the pump's `send` calls
                    // wake up promptly instead of stalling against a
                    // dead receiver. The first slot-stored error
                    // surfaces on the next pump-side `on_record`.
                    rx.close();
                    while rx.recv().await.is_some() {}
                    return;
                }
            }
        });
        Self {
            tx: Some(tx),
            err,
            worker: Some(worker),
        }
    }

    /// Drop the channel sender + join the worker. Any error the worker
    /// has parked surfaces here. Call after the pump has stopped
    /// feeding records.
    pub async fn close(mut self) -> Result<(), SinkError> {
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
            let owned = Record {
                parsed: record.parsed.clone().into_owned(),
                source_lsn: record.source_lsn,
                page_magic: record.page_magic,
                decision: record.decision,
            };
            let tx = self.tx.as_ref().ok_or_else(|| {
                SinkError::Other("queueing record sink already closed".into())
            })?;
            if tx.send(owned).await.is_err() {
                // Worker exited. Prefer surfacing the stored error
                // over a generic "worker dead" message.
                if let Some(e) = self.take_pending_error() {
                    return Err(e);
                }
                return Err(SinkError::Other(
                    "queueing record sink worker stopped".into(),
                ));
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
            decision: crate::filter::Decision::Keep,
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
        let mut q = QueueingRecordSink::spawn(CaptureLsn(collected.clone()), 8);
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
        let mut q = QueueingRecordSink::spawn(Fail, 4);
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
        let mut q = QueueingRecordSink::spawn(Counter(count.clone()), 4);
        for lsn in 0..32 {
            q.on_record(&synth(lsn)).await.expect("send");
        }
        q.close().await.expect("close");
        assert_eq!(*count.lock().unwrap(), 32);
    }
}
