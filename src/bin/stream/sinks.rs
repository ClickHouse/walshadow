//! `RecordSink` composites the WAL pump feeds.

use std::sync::Arc;

use anyhow::Result;
use std::future::Future;
use std::pin::Pin;
use walshadow::boundary_hold::BoundaryHoldSink;
use walshadow::heap_decoder::Predecoded;
use walshadow::record::{MetricsRecordSink, Record, RecordSink, SinkError};
use walshadow::xact_buffer::BufferingDecoderSink;

/// `decoder + xact_drain` pair as one `RecordSink` for the queueing worker.
///
/// Order matters: decoder absorbs the heap record into the xact buffer
/// before xact_drain flushes the matching commit/abort. A multi-statement
/// xact whose COMMIT lands in the same dispatch batch as its heap records
/// would otherwise miss the latest writes.
pub(crate) struct DecoderXactPair<D: RecordSink + Send> {
    pub(crate) decoder: BufferingDecoderSink,
    pub(crate) xact_drain: D,
}

impl<D: RecordSink + Send> RecordSink for DecoderXactPair<D> {
    fn wants(record: &Record<'_>) -> bool {
        BufferingDecoderSink::wants(record) || D::wants(record)
    }

    fn on_record<'a>(
        &'a mut self,
        record: &'a Record<'a>,
    ) -> Pin<Box<dyn Future<Output = Result<(), SinkError>> + Send + 'a>> {
        Box::pin(async move {
            self.decoder.on_record(record).await?;
            self.xact_drain.on_record(record).await
        })
    }

    /// Absorbs every heap record before draining any commit in the batch:
    /// stronger than the per-record order, and a commit only drains its own
    fn on_records<'a>(
        &'a mut self,
        batch: &'a [Record<'a>],
        pre: &'a mut [Option<Predecoded>],
    ) -> Pin<Box<dyn Future<Output = Result<(), SinkError>> + Send + 'a>> {
        Box::pin(async move {
            self.decoder.on_records(batch, pre).await?;
            self.xact_drain.on_records(batch, &mut []).await?;
            Ok(())
        })
    }

    fn on_idle<'a>(
        &'a mut self,
    ) -> Pin<Box<dyn Future<Output = Result<(), SinkError>> + Send + 'a>> {
        // Decoder has no time-based work; xact_drain forwards to the
        // CH emitter's deadline check.
        self.xact_drain.on_idle()
    }

    fn on_close<'a>(
        &'a mut self,
    ) -> Pin<Box<dyn Future<Output = Result<(), SinkError>> + Send + 'a>> {
        // Decoder has no close work; xact_drain forwards the final flush.
        self.xact_drain.on_close()
    }

    fn on_idle_advance<'a>(
        &'a mut self,
        lsn: u64,
    ) -> Pin<Box<dyn Future<Output = Result<(), SinkError>> + Send + 'a>> {
        self.xact_drain.on_idle_advance(lsn)
    }
}

/// Daemon-side `RecordSink` composite.
///
/// `metrics` stays synchronous on the pump task (counter bumps, never
/// await). The decoder/xact-drain pair runs behind a [`QueueingRecordSink`](walshadow::queueing_record_sink::QueueingRecordSink)
/// so its `wait_for_replay` waits don't park the pump task: each gate
/// would freeze wire delivery for a full shadow apply round-trip and
/// couple wire pacing to decode.
pub(crate) struct DaemonSinks {
    pub(crate) metrics: MetricsRecordSink,
    /// Queueing sink wrapped with the catalog-boundary publication hold:
    /// at a catalog-mutating commit the pump parks here until shadow
    /// replays through the commit's `next_lsn`, so successor bytes reach
    /// neither the shadow wire nor the archive while held.
    pub(crate) decoder_xact: BoundaryHoldSink,
    /// Shared with the `BufferingDecoderSink` on the queueing worker;
    /// status loop polls without contending on the worker.
    pub(crate) decoder_stats: Arc<walshadow::decoder_sink::DecoderStats>,
    /// Shared with parallel pipeline's inserter pool (bumps counters
    /// post-`EndOfStream`). `None` when no CH pipeline is wired.
    pub(crate) emitter_stats: Option<Arc<walshadow::ch_emitter::EmitterStats>>,
    /// Per-txn span map; `Some` only with OTLP on. Registering at WAL read
    /// (here) makes the `txn` span cover the pump→worker channel wait.
    pub(crate) span_registry: Option<walshadow::trace::TxnSpanRegistry>,
}

impl RecordSink for DaemonSinks {
    fn may_park(&self, record: &Record<'_>) -> bool {
        self.decoder_xact.may_park(record)
    }

    fn on_record<'a>(
        &'a mut self,
        record: &'a Record<'a>,
    ) -> Pin<Box<dyn Future<Output = Result<(), SinkError>> + Send + 'a>> {
        // Register at WAL read (pre-channel) so the span covers the queue wait.
        if let Some(reg) = &self.span_registry {
            reg.open(record.parsed.header.xact_id, record.source_lsn);
        }
        self.metrics.observe(record);
        // Inner future as is: one boxed future per record, not one per layer
        self.decoder_xact.on_record(record)
    }

    fn on_record_owned(
        &mut self,
        record: Record<'static>,
    ) -> Pin<Box<dyn Future<Output = Result<(), SinkError>> + Send + '_>> {
        if let Some(reg) = &self.span_registry {
            reg.open(record.parsed.header.xact_id, record.source_lsn);
        }
        self.metrics.observe(&record);
        self.decoder_xact.on_record_owned(record)
    }
}
