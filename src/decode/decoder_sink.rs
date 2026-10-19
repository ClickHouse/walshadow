//! Shared types for the heap-tuple decoder fan-out: [`TupleObserver`],
//! [`DecoderStats`], [`DecoderSinkError`].
//!
//! WAL commits drain through the parallel [`pipeline`](crate::emit::pipeline);
//! [`TupleObserver`] is the greenfield-bootstrap page-walk drain destination
//! (`drain_backfill`). Catalog gate timeouts poison the stream, so no
//! `replay_timeout` counter: silent loss is impossible by construction.

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};

use thiserror::Error;

use crate::catalog::shadow_catalog::CatalogError;
use crate::ch::EmitterError;
use crate::decode::heap_decoder::{CommittedTuple, DecodeError};
use crate::record::SinkError;
use walrus::pg::walparser::RmId;

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

impl From<EmitterError> for DecoderSinkError {
    fn from(error: EmitterError) -> Self {
        Self::Observer(error.to_string())
    }
}

/// Destination of decoded + committed heap events. Wire type is
/// [`CommittedTuple`] not [`DecodedHeap`](crate::decode::heap_decoder::DecodedHeap):
/// CH emitter needs `commit_ts` for its `_commit_ts` synthetic column.
///
/// `on_xact_end` fires after every tuple in a committed xact is delivered.
/// Returns highest commit_lsn now durable on the observer. Callers advance
/// their ack ceiling from the return value, NOT `commit_lsn`. Default
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
        /// [`ShadowCatalog`](crate::catalog::shadow_catalog::ShadowCatalog) returned
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
        /// DELETE on a toast rel: TID-keyed (replica identity `nothing`),
        /// buffered as a store tombstone row applied at commit drain
        pub toast_chunk_deletes,
        /// Records on marker-proven invisible filenodes stashed raw for
        /// commit-time resolution (same-xact CREATE / TRUNCATE / rewrite)
        pub toast_stash_buffered,
        /// Records deferred raw because their xact tree wrote catalog
        /// state earlier in the stream; resolve at commit under
        /// post-boundary descriptors
        pub raw_stash_deferred,
        /// `raw_stash_records_total{kind="dirty",op}`: per-op split of
        /// `raw_stash_deferred`
        pub raw_stash_dirty_ops: crate::decode::heap_decoder::OpCounters,
        /// `raw_stash_records_total{kind="marker",op}`: per-op split of
        /// `toast_stash_buffered`
        pub raw_stash_marker_ops: crate::decode::heap_decoder::OpCounters,
        pub attribute_skips: std::sync::atomic::AtomicBool,
        pub skipped_by_rel: SkipCounts,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum SkipReason {
    UnfollowedDb,
    ForeignDb,
    NotCovered,
    Retired,
    Unresolvable,
    TruncateUnparsed,
    TruncateRelMissing,
    NoBlock,
    NoTuple,
    IndexDropped,
}

impl SkipReason {
    pub fn label(self) -> &'static str {
        match self {
            Self::UnfollowedDb => "unfollowed_db",
            Self::ForeignDb => "foreign_db",
            Self::NotCovered => "not_covered",
            Self::Retired => "retired",
            Self::Unresolvable => "unresolvable",
            Self::TruncateUnparsed => "truncate_unparsed",
            Self::TruncateRelMissing => "truncate_rel_missing",
            Self::NoBlock => "no_block",
            Self::NoTuple => "no_tuple",
            Self::IndexDropped => "index_dropped",
        }
    }

    pub fn expected(self) -> bool {
        matches!(
            self,
            Self::UnfollowedDb
                | Self::ForeignDb
                | Self::Retired
                | Self::NoTuple
                | Self::IndexDropped
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SkipKey {
    pub db_oid: u32,
    pub rel_node: u32,
    pub op: &'static str,
    pub reason: SkipReason,
}

pub type SkipCounts = crate::emit::pipeline::row_ledger::KeyedCounts<SkipKey>;

#[derive(Debug, Clone, Copy)]
pub struct SkippedRecord {
    pub reason: SkipReason,
    pub db_oid: u32,
    pub rel_node: u32,
    pub rm: u8,
    pub info: u8,
    pub xid: u32,
    pub lsn: u64,
}

impl SkippedRecord {
    pub fn of(
        record: &crate::record::Record<'_>,
        reason: SkipReason,
        db_oid: u32,
        rel_node: u32,
    ) -> Self {
        let header = &record.parsed.header;
        Self {
            reason,
            db_oid,
            rel_node,
            rm: header.resource_manager_id,
            info: header.info,
            xid: header.xact_id,
            lsn: record.source_lsn,
        }
    }

    pub fn on(
        record: &crate::record::Record<'_>,
        reason: SkipReason,
        rfn: walrus::pg::walparser::RelFileNode,
    ) -> Self {
        Self::of(record, reason, rfn.db_node, rfn.rel_node)
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
    pub fn record(&self, decoded: &crate::decode::heap_decoder::DecodedHeap) {
        use crate::decode::heap_decoder::HeapOp;
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

    pub fn skip(&self, s: SkippedRecord) {
        use crate::decode::heap_decoder::{HEAP_OP_LABELS, heap_op_index};
        let aggregate = match s.reason {
            SkipReason::NoBlock => Some(&self.skipped_no_block),
            SkipReason::TruncateUnparsed | SkipReason::NoTuple => Some(&self.skipped_op),
            SkipReason::IndexDropped => None,
            _ => Some(&self.catalog_not_found),
        };
        if let Some(aggregate) = aggregate {
            aggregate.fetch_add(1, Ordering::Relaxed);
        }
        if !self.attribute_skips.load(Ordering::Relaxed) {
            return;
        }
        let op = if s.rm == RmId::Heap as u8 || s.rm == RmId::Heap2 as u8 {
            HEAP_OP_LABELS[heap_op_index(s.rm, s.info)]
        } else {
            crate::record::rmgr_name(s.rm)
        };
        let key = SkipKey {
            db_oid: s.db_oid,
            rel_node: s.rel_node,
            op,
            reason: s.reason,
        };
        let prior = self
            .skipped_by_rel
            .counter(&key)
            .fetch_add(1, Ordering::Relaxed);
        if prior == 0 && !s.reason.expected() {
            tracing::warn!(
                target: "walshadow::decoder",
                db_oid = s.db_oid,
                rel_node = s.rel_node,
                rm = s.rm,
                info = s.info,
                op,
                reason = s.reason.label(),
                xid = s.xid,
                lsn = %walrus::pg::backup::format_pg_lsn(s.lsn),
                "first record skipped for relation",
            );
        } else {
            tracing::debug!(
                target: "walshadow::decoder",
                db_oid = s.db_oid,
                rel_node = s.rel_node,
                rm = s.rm,
                info = s.info,
                op,
                reason = s.reason.label(),
                xid = s.xid,
                lsn = %walrus::pg::backup::format_pg_lsn(s.lsn),
                skipped = prior + 1,
                "record skipped",
            );
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
    use crate::decode::heap_decoder::{ColumnValue, DecodedHeap, HeapOp};
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
            new: Some(crate::decode::heap_decoder::DecodedTuple {
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
            new: Some(crate::decode::heap_decoder::DecodedTuple {
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
    fn skip_splits_by_relation_and_reason() {
        use walrus::pg::walparser::RmId;
        let s = DecoderStats::default();
        s.skip(SkippedRecord {
            reason: SkipReason::NotCovered,
            db_oid: 5,
            rel_node: 16399,
            rm: RmId::Heap as u8,
            info: 0,
            xid: 7,
            lsn: 0x1000,
        });
        assert!(s.skipped_by_rel.snapshot().is_empty(), "off by default");
        assert_eq!(s.catalog_not_found.load(Ordering::Relaxed), 1);
        s.catalog_not_found.store(0, Ordering::Relaxed);
        s.attribute_skips.store(true, Ordering::Relaxed);
        let skip = |reason, rel_node| SkippedRecord {
            reason,
            db_oid: 5,
            rel_node,
            rm: RmId::Heap as u8,
            info: crate::decode::heap_decoder::XLOG_HEAP_INSERT,
            xid: 7,
            lsn: 0x1000,
        };
        s.skip(skip(SkipReason::NotCovered, 16400));
        s.skip(skip(SkipReason::NotCovered, 16400));
        s.skip(skip(SkipReason::Retired, 16401));
        s.skip(skip(SkipReason::NoTuple, 16400));
        s.skip(skip(SkipReason::NoBlock, 0));
        let ld = |a: &AtomicU64| a.load(Ordering::Relaxed);
        assert_eq!(ld(&s.catalog_not_found), 3);
        assert_eq!(ld(&s.skipped_op), 1);
        assert_eq!(ld(&s.skipped_no_block), 1);
        let key = |rel_node, reason| SkipKey {
            db_oid: 5,
            rel_node,
            op: "insert",
            reason,
        };
        assert_eq!(
            s.skipped_by_rel.snapshot(),
            vec![
                (key(16400, SkipReason::NotCovered), 2),
                (key(0, SkipReason::NoBlock), 1),
                (key(16400, SkipReason::NoTuple), 1),
                (key(16401, SkipReason::Retired), 1),
            ],
        );
    }

    #[test]
    fn observer_error_wraps_to_sink_other() {
        let e: SinkError = DecoderSinkError::Observer("boom".into()).into();
        assert_eq!(format!("{e:?}"), r#"Other("observer: boom")"#);
    }
}
