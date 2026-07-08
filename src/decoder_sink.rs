//! Shared types for the heap-tuple decoder fan-out: [`TupleObserver`],
//! [`DecoderStats`], [`DecoderSinkError`].
//!
//! CH path drains through the parallel [`pipeline`](crate::pipeline);
//! metrics-only path (no `--ch-config`) drains to [`MetricsTupleObserver`].
//! Catalog gate timeouts poison the stream, so no `replay_timeout` counter:
//! silent loss is impossible by construction.

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};

use thiserror::Error;

use crate::heap_decoder::{CommittedTuple, DecodeError};
use crate::runtime_config::ConfigEvent;
use crate::shadow_catalog::{CatalogError, SchemaEvent};
use crate::wal_stream::SinkError;

#[derive(Debug, Error)]
pub enum DecoderSinkError {
    #[error("decode: {0}")]
    Decode(#[from] DecodeError),
    #[error("catalog: {0}")]
    Catalog(#[from] CatalogError),
    #[error("observer: {0}")]
    Observer(String),
}

impl From<DecoderSinkError> for SinkError {
    fn from(e: DecoderSinkError) -> Self {
        SinkError::Other(e.to_string())
    }
}

/// Destination of decoded + committed heap events. Wire type is
/// [`CommittedTuple`] not [`DecodedHeap`](crate::heap_decoder::DecodedHeap):
/// CH emitter needs `commit_ts` for its `_commit_ts` synthetic column.
///
/// `on_xact_end` fires after every tuple in a committed xact is delivered.
/// Returns highest commit_lsn now durable on the observer (CH server acked,
/// MergeTree part finalized). Callers advance their ack ceiling from the
/// return value, NOT `commit_lsn`, so an observer holding INSERTs open across
/// xacts reports ack lag without breaking the slot-advance gate. Default
/// returns `commit_lsn` (instant ack).
pub trait TupleObserver: Send {
    fn on_tuple<'a>(
        &'a mut self,
        committed: &'a CommittedTuple,
    ) -> Pin<Box<dyn Future<Output = Result<(), DecoderSinkError>> + Send + 'a>>;

    fn on_xact_end<'a>(
        &'a mut self,
        commit_lsn: u64,
    ) -> Pin<Box<dyn Future<Output = Result<u64, DecoderSinkError>> + Send + 'a>> {
        Box::pin(async move { Ok(commit_lsn) })
    }

    /// Dispatched from [`crate::xact_buffer::XactBuffer::commit`]'s k-way merge
    /// in `source_lsn` order alongside `on_tuple`, so DDL (ALTER/CREATE/DROP)
    /// lands on dest before the next `on_tuple` encodes against post-DDL shape.
    /// Default no-op: non-CH observers ignore schema events.
    fn on_schema_event<'a>(
        &'a mut self,
        _event: &'a SchemaEvent,
    ) -> Pin<Box<dyn Future<Output = Result<(), DecoderSinkError>> + Send + 'a>> {
        Box::pin(async { Ok(()) })
    }

    /// Dispatched alongside `on_schema_event` for a source-PG config-table
    /// write, at its `source_lsn` in the drain. The parallel pipeline applies
    /// config in the reorder coordinator's barrier instead, so this fires only
    /// on the serial drain path; default no-op.
    fn on_config_event<'a>(
        &'a mut self,
        _event: &'a ConfigEvent,
    ) -> Pin<Box<dyn Future<Output = Result<(), DecoderSinkError>> + Send + 'a>> {
        Box::pin(async { Ok(()) })
    }

    /// Ceiling for an idle-advance ack at `lsn`. Returns `lsn` when observer
    /// holds nothing client-side; otherwise its durable horizon, so a
    /// quiescent-tick nudge can't promote the emitter ack past rows still
    /// buffered in open INSERTs. Default `lsn`: non-buffering observers.
    fn idle_ack_ceiling(&self, lsn: u64) -> u64 {
        lsn
    }

    /// Observer-layer mirror of [`crate::wal_stream::RecordSink::on_idle`]:
    /// CH emitter closes hold-open INSERTs once `flush_timeout` elapses with no
    /// fresh xact_end to piggyback the deadline check on. Returns commit LSN
    /// made durable by any deadline-triggered close (`0` = nothing promoted),
    /// folded into `emitter_ack_lsn`. Default no-op `Ok(0)`.
    fn on_idle<'a>(
        &'a mut self,
    ) -> Pin<Box<dyn Future<Output = Result<u64, DecoderSinkError>> + Send + 'a>> {
        Box::pin(async { Ok(0) })
    }

    /// Observer-layer mirror of [`crate::wal_stream::RecordSink::on_close`]:
    /// CH emitter force-flushes any held-open INSERT on daemon shutdown.
    fn on_close<'a>(
        &'a mut self,
    ) -> Pin<Box<dyn Future<Output = Result<(), DecoderSinkError>> + Send + 'a>> {
        Box::pin(async { Ok(()) })
    }
}

/// Forwarding impl so the daemon picks an observer at runtime without making
/// [`crate::xact_buffer::XactRecordSink`] generic over a closed enum.
impl<T: TupleObserver + ?Sized> TupleObserver for Box<T> {
    fn on_tuple<'a>(
        &'a mut self,
        committed: &'a CommittedTuple,
    ) -> Pin<Box<dyn Future<Output = Result<(), DecoderSinkError>> + Send + 'a>> {
        (**self).on_tuple(committed)
    }

    fn on_xact_end<'a>(
        &'a mut self,
        commit_lsn: u64,
    ) -> Pin<Box<dyn Future<Output = Result<u64, DecoderSinkError>> + Send + 'a>> {
        (**self).on_xact_end(commit_lsn)
    }

    fn on_schema_event<'a>(
        &'a mut self,
        event: &'a SchemaEvent,
    ) -> Pin<Box<dyn Future<Output = Result<(), DecoderSinkError>> + Send + 'a>> {
        (**self).on_schema_event(event)
    }

    fn idle_ack_ceiling(&self, lsn: u64) -> u64 {
        (**self).idle_ack_ceiling(lsn)
    }

    fn on_idle<'a>(
        &'a mut self,
    ) -> Pin<Box<dyn Future<Output = Result<u64, DecoderSinkError>> + Send + 'a>> {
        (**self).on_idle()
    }

    fn on_close<'a>(
        &'a mut self,
    ) -> Pin<Box<dyn Future<Output = Result<(), DecoderSinkError>> + Send + 'a>> {
        (**self).on_close()
    }
}

crate::atomic_stats! {
    pub struct DecoderStats {
        pub decoded,
        pub inserts,
        pub updates,
        pub hot_updates,
        pub deletes,
        /// WAL elided columns via `XLH_UPDATE_PREFIX_FROM_OLD` /
        /// `..._SUFFIX_FROM_OLD`; xact buffer reassembles from prev tuple image
        pub partial,
        /// `record.parsed.blocks` empty: no relation. Heap LOCK / INPLACE /
        /// TRUNCATE land here under the silent-skip policy
        pub skipped_no_block,
        /// [`ShadowCatalog`](crate::shadow_catalog::ShadowCatalog) returned
        /// `NotFoundByFilenode`. Causes: replay-LSN gate ahead of catalog
        /// mutation, mapping rotation, race with `seed_from_source`. Not
        /// retried, xact buffer can reorder
        pub catalog_not_found,
        /// `User` relation but rmgr/info combo outside the type matrix
        /// (MULTI_INSERT, HEAP2 PRUNE, etc)
        pub skipped_op,
        /// `XLOG_HEAP_TRUNCATE` fanned out to per-relid `HeapOp::Truncate`
        pub truncates,
        /// TOAST chunks routed into the xact buffer's chunk slot, distinct
        /// from `inserts` (user-table writes only)
        pub toast_chunks_buffered,
        /// TOAST inserts not reinterpretable as a chunk (missing
        /// chunk_id/seq/data, type mismatch). Surfaces a corrupt catalog or
        /// future TOAST layout as a counter, not silent loss
        pub toast_chunks_malformed,
        /// DELETE/TRUNCATE on a toast rel: TID-keyed (replica identity
        /// `nothing`), no chunk_id to apply against the store, dropped by
        /// design — dead chunks reclaimed by the GC sweep
        /// (`crate::toast_gc`), not per-record
        pub toast_chunk_deletes,
    }
}

#[derive(Debug, Default)]
pub struct MetricsTupleObserver {
    pub stats: DecoderStats,
}

impl TupleObserver for MetricsTupleObserver {
    fn on_tuple<'a>(
        &'a mut self,
        committed: &'a CommittedTuple,
    ) -> Pin<Box<dyn Future<Output = Result<(), DecoderSinkError>> + Send + 'a>> {
        Box::pin(async move {
            self.stats.record(&committed.decoded);
            Ok(())
        })
    }
}

/// In-memory test observer; retains full committed tuples for assertions.
#[derive(Debug, Default)]
pub struct CollectingTupleObserver {
    pub tuples: Vec<CommittedTuple>,
}

impl TupleObserver for CollectingTupleObserver {
    fn on_tuple<'a>(
        &'a mut self,
        committed: &'a CommittedTuple,
    ) -> Pin<Box<dyn Future<Output = Result<(), DecoderSinkError>> + Send + 'a>> {
        Box::pin(async move {
            self.tuples.push(committed.clone());
            Ok(())
        })
    }
}

impl DecoderStats {
    /// Single source of truth for the `HeapOp → counter` mapping; all decoder
    /// dispatch sites route through here.
    pub fn record(&self, decoded: &crate::heap_decoder::DecodedHeap) {
        use crate::heap_decoder::HeapOp;
        self.decoded.fetch_add(1, Ordering::Relaxed);
        match decoded.op {
            HeapOp::Insert => self.inserts.fetch_add(1, Ordering::Relaxed),
            HeapOp::Update => self.updates.fetch_add(1, Ordering::Relaxed),
            HeapOp::HotUpdate => self.hot_updates.fetch_add(1, Ordering::Relaxed),
            HeapOp::Delete => self.deletes.fetch_add(1, Ordering::Relaxed),
            HeapOp::Truncate => self.truncates.fetch_add(1, Ordering::Relaxed),
        };
        let partial = decoded.new.as_ref().is_some_and(|t| t.partial)
            || decoded.old.as_ref().is_some_and(|t| t.partial);
        if partial {
            self.partial.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Single-line status summary, zero buckets elided.
    pub fn summary(&self) -> String {
        use std::fmt::Write as _;
        let ld = |a: &AtomicU64| a.load(Ordering::Relaxed);
        let mut s = format!("decoded={}", ld(&self.decoded));
        let pairs: [(&str, u64); 11] = [
            ("ins", ld(&self.inserts)),
            ("upd", ld(&self.updates)),
            ("hot", ld(&self.hot_updates)),
            ("del", ld(&self.deletes)),
            ("trunc", ld(&self.truncates)),
            ("partial", ld(&self.partial)),
            ("no_blk", ld(&self.skipped_no_block)),
            ("not_found", ld(&self.catalog_not_found)),
            ("toast", ld(&self.toast_chunks_buffered)),
            ("toast_bad", ld(&self.toast_chunks_malformed)),
            ("toast_del", ld(&self.toast_chunk_deletes)),
        ];
        for (label, n) in pairs {
            if n > 0 {
                write!(&mut s, " {label}={n}").unwrap();
            }
        }
        let skip_op = ld(&self.skipped_op);
        if skip_op > 0 {
            write!(&mut s, " skip_op={skip_op}").unwrap();
        }
        s
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::heap_decoder::{ColumnValue, DecodedHeap, HeapOp};
    use walrus::pg::walparser::RelFileNode;

    fn wrap(decoded: DecodedHeap) -> CommittedTuple {
        CommittedTuple {
            decoded,
            commit_ts: 0,
            commit_lsn: 0,
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn metrics_observer_buckets_by_op() {
        let mut obs = MetricsTupleObserver::default();
        let mk = |op: HeapOp, partial: bool| DecodedHeap {
            rfn: RelFileNode::default(),
            xid: 0,
            source_lsn: 0,
            op,
            new: Some(crate::heap_decoder::DecodedTuple {
                columns: vec![Some(ColumnValue::Null)],
                partial,
            }),
            old: None,
        };
        for op in [
            HeapOp::Insert,
            HeapOp::Insert,
            HeapOp::Update,
            HeapOp::HotUpdate,
            HeapOp::Delete,
        ] {
            obs.on_tuple(&wrap(mk(op, false))).await.unwrap();
        }
        obs.on_tuple(&wrap(mk(HeapOp::Update, true))).await.unwrap();
        let s = &obs.stats;
        let ld = |a: &AtomicU64| a.load(Ordering::Relaxed);
        assert_eq!(ld(&s.decoded), 6);
        assert_eq!(ld(&s.inserts), 2);
        assert_eq!(ld(&s.updates), 2);
        assert_eq!(ld(&s.hot_updates), 1);
        assert_eq!(ld(&s.deletes), 1);
        assert_eq!(ld(&s.partial), 1);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn collecting_observer_keeps_full_clone() {
        let mut obs = CollectingTupleObserver::default();
        let d = DecodedHeap {
            rfn: RelFileNode {
                spc_node: 1663,
                db_node: 5,
                rel_node: 16385,
            },
            xid: 42,
            source_lsn: 0x1234,
            op: HeapOp::Insert,
            new: Some(crate::heap_decoder::DecodedTuple {
                columns: vec![Some(ColumnValue::Int4(7))],
                partial: false,
            }),
            old: None,
        };
        let c = CommittedTuple {
            decoded: d,
            commit_ts: 9_876,
            commit_lsn: 0,
        };
        obs.on_tuple(&c).await.unwrap();
        assert_eq!(obs.tuples.len(), 1);
        assert_eq!(obs.tuples[0].decoded.xid, 42);
        assert_eq!(obs.tuples[0].decoded.source_lsn, 0x1234);
        assert_eq!(obs.tuples[0].commit_ts, 9_876);
    }

    #[test]
    fn stats_summary_skips_zero_buckets() {
        let s = DecoderStats::default();
        s.decoded.store(5, Ordering::Relaxed);
        s.inserts.store(3, Ordering::Relaxed);
        s.deletes.store(2, Ordering::Relaxed);
        s.partial.store(1, Ordering::Relaxed);
        let out = s.summary();
        assert!(out.starts_with("decoded=5"), "{out}");
        assert!(out.contains("ins=3"), "{out}");
        assert!(out.contains("del=2"), "{out}");
        assert!(out.contains("partial=1"), "{out}");
        assert!(!out.contains("upd="), "{out}");
        assert!(!out.contains("hot="), "{out}");
        assert!(!out.contains("no_blk="), "{out}");
    }

    #[test]
    fn observer_error_wraps_to_sink_other() {
        let e: SinkError = DecoderSinkError::Observer("boom".into()).into();
        match e {
            SinkError::Other(msg) => assert!(msg.contains("boom"), "{msg}"),
            other => panic!("expected Other, got {other:?}"),
        }
    }
}
