//! Phase 5 — `RecordSink` adapter wiring the heap-tuple decoder to
//! [`WalStream`](crate::wal_stream::WalStream).
//!
//! Routes records whose filter decision is [`Decision::Drop`] (user
//! relations) through [`decode_heap_record`], fans the output to a
//! pluggable observer. Decoder hot path needs the relation descriptor
//! ([`ShadowCatalog::relation_at`] gated on shadow's replay LSN), so
//! the sink owns an `Arc<Mutex<ShadowCatalog>>` clone — see
//! `bin/stream.rs` for the daemon-side wiring.
//!
//! Stats coverage matches the PHASE 5 plan's "Rollback status,
//! explicit" caveat: every successful decode increments `decoded`;
//! `partial` counts prefix/suffix-compressed UPDATEs that Phase 6 must
//! reassemble. `unsupported_rmgr_info`, `skipped_no_block`, and
//! close the per-class accounting so operators can tell silent loss
//! from real volume. Catalog gate timeouts poison the stream — no
//! `replay_timeout` counter, since silent loss is impossible by
//! construction.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use thiserror::Error;
use tokio::sync::Mutex;
use wal_rs::pg::walparser::RmId;

use crate::filter::Decision;
use crate::heap_decoder::{CommittedTuple, DecodeError, decode_heap_record};
use crate::shadow_catalog::{CatalogError, ShadowCatalog};
use crate::wal_stream::{Record, RecordSink, SinkError};

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
/// events. Phase 5 fans into `MetricsTupleObserver` (production
/// counters) plus an in-memory collector for integration tests. Phase
/// 7 plugs the CH-emitter observer in here, which is why
/// [`CommittedTuple`] (not [`DecodedHeap`]) is the wire type — the
/// emitter wants `commit_ts` for its `_commit_ts` synthetic column.
/// Phase 5's pre-buffer [`DecoderSink`] path passes `commit_ts = 0`
/// since the commit record hasn't landed at that point.
///
/// `on_xact_end` fires after every tuple in a committed xact has been
/// delivered. Phase 7's CH emitter uses it to close the open INSERT
/// (`send_data(None)`) so each xact lands as a single CH block group.
/// Default no-op; metrics & collector observers ignore the hook.
pub trait TupleObserver: Send {
    fn on_tuple<'a>(
        &'a mut self,
        committed: &'a CommittedTuple,
    ) -> Pin<Box<dyn Future<Output = Result<(), DecoderSinkError>> + Send + 'a>>;

    fn on_xact_end<'a>(
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
    ) -> Pin<Box<dyn Future<Output = Result<(), DecoderSinkError>> + Send + 'a>> {
        (**self).on_xact_end()
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
            use crate::heap_decoder::HeapOp;
            let decoded = &committed.decoded;
            self.stats.decoded += 1;
            match decoded.op {
                HeapOp::Insert => self.stats.inserts += 1,
                HeapOp::Update => self.stats.updates += 1,
                HeapOp::HotUpdate => self.stats.hot_updates += 1,
                HeapOp::Delete => self.stats.deletes += 1,
                HeapOp::Truncate => self.stats.truncates += 1,
            }
            if decoded.new.as_ref().map(|t| t.partial).unwrap_or(false)
                || decoded.old.as_ref().map(|t| t.partial).unwrap_or(false)
            {
                self.stats.partial += 1;
            }
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

/// `RecordSink` wiring. Each [`Record`] crosses two gates:
///
/// 1. Decision gate: only `Decision::Drop` records reach the decoder.
///    `Keep` records ride the existing catalog-replay path (shadow PG
///    consumes them); decoding them would double-count or misclassify.
/// 2. rmgr gate: only `RmId::Heap` / `RmId::Heap2` are decoded today.
///    Future expansion (`RmId::Btree` for unique-index rows?) lives
///    behind the same gate.
///
/// On `Decode` / `Catalog` errors the sink **does not poison** the
/// stream: errors are absorbed into `DecoderStats` so a single bad
/// tuple won't take down the rest of the segment. The poison contract
/// of [`WalStream`](crate::wal_stream::WalStream) is reserved for
/// errors that compromise byte-level integrity (filter parse failure,
/// IO loss); decoder semantic errors don't qualify.
pub struct DecoderSink<O: TupleObserver> {
    catalog: Arc<Mutex<ShadowCatalog>>,
    observer: O,
    pub stats: DecoderStats,
}

impl<O: TupleObserver> DecoderSink<O> {
    pub fn new(catalog: Arc<Mutex<ShadowCatalog>>, observer: O) -> Self {
        Self {
            catalog,
            observer,
            stats: DecoderStats::default(),
        }
    }

    /// Borrow the observer mutably — test convenience to inspect
    /// downstream state without re-extracting the sink.
    pub fn observer_mut(&mut self) -> &mut O {
        &mut self.observer
    }

    /// Stats snapshot; the production daemon logs this in the status line.
    pub fn stats(&self) -> &DecoderStats {
        &self.stats
    }
}

impl<O: TupleObserver> RecordSink for DecoderSink<O> {
    fn on_record<'a>(
        &'a mut self,
        record: &'a Record<'a>,
    ) -> Pin<Box<dyn Future<Output = Result<(), SinkError>> + Send + 'a>> {
        Box::pin(async move {
            if record.decision != Decision::Drop {
                return Ok(());
            }
            let rm = record.parsed.header.resource_manager_id;
            if rm != RmId::Heap as u8 && rm != RmId::Heap2 as u8 {
                return Ok(());
            }
            let rfn = match record.parsed.blocks.first() {
                Some(b) => b.header.location.rel,
                None => {
                    self.stats.skipped_no_block += 1;
                    return Ok(());
                }
            };
            let mut cat = self.catalog.lock().await;
            let rel = match cat.relation_at(rfn, record.source_lsn).await {
                Ok(r) => r,
                Err(CatalogError::NotFoundByFilenode(_)) => {
                    self.stats.catalog_not_found += 1;
                    return Ok(());
                }
                // PHASE13 §6: ReplayTimeout poisons the stream so the
                // daemon exits cleanly. Phase 11 cursor resumes from
                // `dispatched_lsn` on the next boot.
                Err(e) => return Err(DecoderSinkError::from(e).into()),
            };
            drop(cat);
            let decoded_set = match decode_heap_record(&record.parsed, record.source_lsn, &rel) {
                Ok(set) => set,
                Err(e) => return Err(DecoderSinkError::from(e).into()),
            };
            if decoded_set.is_empty() {
                self.stats.skipped_op += 1;
                return Ok(());
            }
            use crate::heap_decoder::HeapOp;
            for decoded in decoded_set {
                self.stats.decoded += 1;
                match decoded.op {
                    HeapOp::Insert => self.stats.inserts += 1,
                    HeapOp::Update => self.stats.updates += 1,
                    HeapOp::HotUpdate => self.stats.hot_updates += 1,
                    HeapOp::Delete => self.stats.deletes += 1,
                    HeapOp::Truncate => self.stats.truncates += 1,
                }
                if decoded.new.as_ref().map(|t| t.partial).unwrap_or(false)
                    || decoded.old.as_ref().map(|t| t.partial).unwrap_or(false)
                {
                    self.stats.partial += 1;
                }
                // Phase 5 unbuffered path emits the moment the heap
                // record lands — no commit record yet, so commit_ts=0
                // and commit_lsn=0. Phase 6's BufferingDecoderSink
                // takes over in the production dispatch chain.
                let committed = CommittedTuple {
                    decoded,
                    commit_ts: 0,
                    commit_lsn: 0,
                };
                self.observer
                    .on_tuple(&committed)
                    .await
                    .map_err(SinkError::from)?;
            }
            Ok(())
        })
    }
}

impl DecoderStats {
    /// Single-line summary suitable for the daemon's status line. Skips
    /// zero buckets so a quiet workload shows a tight format.
    pub fn summary(&self) -> String {
        use std::fmt::Write as _;
        let mut s = format!("decoded={}", self.decoded);
        let pairs: [(&str, u64); 8] = [
            ("ins", self.inserts),
            ("upd", self.updates),
            ("hot", self.hot_updates),
            ("del", self.deletes),
            ("trunc", self.truncates),
            ("partial", self.partial),
            ("no_blk", self.skipped_no_block),
            ("not_found", self.catalog_not_found),
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
