//! Phase 5 — shared types for the heap-tuple decoder fan-out:
//! [`TupleObserver`] (downstream of [`BufferingDecoderSink`](crate::xact_buffer::BufferingDecoderSink)
//! 's commit drain), [`DecoderStats`] counters, [`DecoderSinkError`].
//!
//! Per-record dispatch lives in
//! [`BufferingDecoderSink`](crate::xact_buffer::BufferingDecoderSink);
//! the production observer is [`crate::ch_emitter::EmitterObserver`].
//! Catalog gate timeouts poison the stream — no `replay_timeout`
//! counter, since silent loss is impossible by construction.

use std::future::Future;
use std::pin::Pin;

use thiserror::Error;

use crate::heap_decoder::{CommittedTuple, DecodeError};
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

/// Trait wrapper for the destination of decoded + committed heap
/// events. Production fans into `MetricsTupleObserver` (counters) or
/// the CH-emitter observer; tests use the in-memory collector. The
/// CH emitter wants `commit_ts` for its `_commit_ts` synthetic
/// column, so [`CommittedTuple`] (not [`DecodedHeap`]) is the wire
/// type.
///
/// `on_xact_end` fires after every tuple in a committed xact has been
/// delivered. Phase 7's CH emitter uses it as the per-xact landmark
/// for closing or extending its open INSERT blocks. Returns the
/// highest commit_lsn now known durable on the observer (CH server
/// acked, MergeTree part finalized). Callers advance their ack
/// ceiling from the returned value, not from `commit_lsn`, so an
/// observer that holds INSERTs open across xacts can report ack lag
/// without breaking the slot-advance gate. Default impl returns
/// `commit_lsn` — no async work, instant ack.
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

    /// PHASE15 §2 — schema-event dispatch. Called from
    /// [`crate::xact_buffer::XactBuffer::commit`]'s k-way merge in
    /// `source_lsn` order alongside `on_tuple`, so the CH DDL applicator
    /// runs ALTER / CREATE / DROP against the dest before the next
    /// `on_tuple` encodes a row against the post-DDL shape. Default:
    /// no-op — observers that don't own CH (metrics, collecting test
    /// observer) ignore schema events.
    fn on_schema_event<'a>(
        &'a mut self,
        _event: &'a SchemaEvent,
    ) -> Pin<Box<dyn Future<Output = Result<(), DecoderSinkError>> + Send + 'a>> {
        Box::pin(async { Ok(()) })
    }

    /// Driver-initiated tick. Mirror of [`crate::wal_stream::RecordSink::on_idle`]
    /// at the observer layer — lets the CH emitter close its
    /// hold-open INSERTs once `flush_timeout` has elapsed without any
    /// fresh xact_end calls to piggyback the deadline check on.
    /// Default: no-op.
    fn on_idle<'a>(
        &'a mut self,
    ) -> Pin<Box<dyn Future<Output = Result<(), DecoderSinkError>> + Send + 'a>> {
        Box::pin(async { Ok(()) })
    }

    /// Final hook before drop. Mirror of [`crate::wal_stream::RecordSink::on_close`]
    /// at the observer layer — used by the CH emitter to force-flush
    /// any held-open INSERT on daemon shutdown. Default: no-op.
    fn on_close<'a>(
        &'a mut self,
    ) -> Pin<Box<dyn Future<Output = Result<(), DecoderSinkError>> + Send + 'a>> {
        Box::pin(async { Ok(()) })
    }
}

/// Forwarding impl so `Box<dyn TupleObserver>` itself implements
/// [`TupleObserver`]. Lets the daemon pick an observer at runtime
/// (e.g. metrics-only vs CH-emitter) without making
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

    fn on_idle<'a>(
        &'a mut self,
    ) -> Pin<Box<dyn Future<Output = Result<(), DecoderSinkError>> + Send + 'a>> {
        (**self).on_idle()
    }

    fn on_close<'a>(
        &'a mut self,
    ) -> Pin<Box<dyn Future<Output = Result<(), DecoderSinkError>> + Send + 'a>> {
        (**self).on_close()
    }
}

/// Counter observer suitable for the production daemon. Discards the
/// tuple payload; tracks counts by op kind so the daemon's status line
/// surfaces decoded-tuple cadence.
#[derive(Debug, Default, Clone)]
pub struct DecoderStats {
    pub decoded: u64,
    pub inserts: u64,
    pub updates: u64,
    pub hot_updates: u64,
    pub deletes: u64,
    /// Decoded but the WAL elided some columns via
    /// `XLH_UPDATE_PREFIX_FROM_OLD` / `..._SUFFIX_FROM_OLD`. Phase 6
    /// reassembles from previous tuple image; Phase 5 emits as-is.
    pub partial: u64,
    /// `record.parsed.blocks` was empty — record references no
    /// relation. Heap LOCK / INPLACE / TRUNCATE land here under the
    /// decoder's silent-skip policy.
    pub skipped_no_block: u64,
    /// Heap record on a relation [`ShadowCatalog`] returned
    /// `NotFoundByFilenode` for. Possible causes: replay-LSN gate
    /// ahead of catalog mutation, mapping rotation, race with
    /// `seed_from_source`. Counted, not retried — Phase 6's xact
    /// buffer can reorder.
    pub catalog_not_found: u64,
    /// Record was on a `User` relation but the rmgr/info combo isn't
    /// in the Phase 5 matrix (MULTI_INSERT, HEAP2 PRUNE, etc).
    pub skipped_op: u64,
    /// `XLOG_HEAP_TRUNCATE` records fanned out to per-relid
    /// `HeapOp::Truncate` events. Counted by
    /// [`BufferingDecoderSink::on_record`]'s pre-decode intercept.
    pub truncates: u64,
    /// TOAST chunks routed into the xact buffer's chunk slot. Distinct
    /// from `inserts`, which only counts user-table writes.
    pub toast_chunks_buffered: u64,
    /// TOAST inserts the decoder couldn't reinterpret as a chunk
    /// (missing chunk_id/seq/data columns, type mismatch). Surfaces
    /// so a corrupt catalog or a future TOAST layout shows up as a
    /// counter, not silent loss.
    pub toast_chunks_malformed: u64,
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

/// In-memory observer for tests. Owns the full committed-tuple
/// stream so tests can assert on `commit_ts` alongside the decoded
/// payload.
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
    /// Bump per-op counters off one decoded heap event. Single source
    /// of truth for the `HeapOp → counter` mapping; every decoder
    /// dispatch site routes through here so new ops only add code in
    /// one place.
    pub fn record(&mut self, decoded: &crate::heap_decoder::DecodedHeap) {
        use crate::heap_decoder::HeapOp;
        self.decoded += 1;
        match decoded.op {
            HeapOp::Insert => self.inserts += 1,
            HeapOp::Update => self.updates += 1,
            HeapOp::HotUpdate => self.hot_updates += 1,
            HeapOp::Delete => self.deletes += 1,
            HeapOp::Truncate => self.truncates += 1,
        }
        let partial = decoded.new.as_ref().is_some_and(|t| t.partial)
            || decoded.old.as_ref().is_some_and(|t| t.partial);
        if partial {
            self.partial += 1;
        }
    }

    /// Single-line summary suitable for the daemon's status line. Skips
    /// zero buckets so a quiet workload shows a tight format.
    pub fn summary(&self) -> String {
        use std::fmt::Write as _;
        let mut s = format!("decoded={}", self.decoded);
        let pairs: [(&str, u64); 10] = [
            ("ins", self.inserts),
            ("upd", self.updates),
            ("hot", self.hot_updates),
            ("del", self.deletes),
            ("trunc", self.truncates),
            ("partial", self.partial),
            ("no_blk", self.skipped_no_block),
            ("not_found", self.catalog_not_found),
            ("toast", self.toast_chunks_buffered),
            ("toast_bad", self.toast_chunks_malformed),
        ];
        for (label, n) in pairs {
            if n > 0 {
                write!(&mut s, " {label}={n}").unwrap();
            }
        }
        if self.skipped_op > 0 {
            write!(&mut s, " skip_op={}", self.skipped_op).unwrap();
        }
        s
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::heap_decoder::{ColumnValue, DecodedHeap, HeapOp};
    use wal_rs::pg::walparser::RelFileNode;

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
        assert_eq!(obs.stats.decoded, 6);
        assert_eq!(obs.stats.inserts, 2);
        assert_eq!(obs.stats.updates, 2);
        assert_eq!(obs.stats.hot_updates, 1);
        assert_eq!(obs.stats.deletes, 1);
        assert_eq!(obs.stats.partial, 1);
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
        let s = DecoderStats {
            decoded: 5,
            inserts: 3,
            updates: 0,
            hot_updates: 0,
            deletes: 2,
            partial: 1,
            ..Default::default()
        };
        let out = s.summary();
        assert!(out.starts_with("decoded=5"), "{out}");
        assert!(out.contains("ins=3"), "{out}");
        assert!(out.contains("del=2"), "{out}");
        assert!(out.contains("partial=1"), "{out}");
        // updates / hot_updates / no_blk are zero and must be elided
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
