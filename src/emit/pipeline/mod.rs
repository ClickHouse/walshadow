//! Plan + parallel insert pipeline.
//!
//! ```text
//! pump -> QueueingRecordSink -> reorder (plan, place) -> InsertBatcher
//!            -> [inserter x N] -> ClickHouse
//!                              \-> ack collector -> emitter_ack_lsn
//! ```
//!
//! See `architecture/README.md`. Inserter pool size N comes from the CLI;
//! size-1 is the degenerate serial case. The [`ack`] watermark is
//! contiguous-done so source slot recycling never outruns CH durability.

pub mod ack;
pub mod batcher;
pub mod bootstrap;
pub mod decode;
pub mod inserter;
pub mod plan_spool;
pub mod planner;
pub mod reorder;
pub mod resolver;
pub mod tail;

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{Mutex, watch};

use crate::ch::EmitterError;
use crate::emit::ch_ddl::DdlApplicator;
use crate::emit::ch_emitter::{EmitterConfig, EmitterStats};
use crate::ops::oracle::Oracle;
use crate::ops::trace::TxnSpanRegistry;
use crate::pos::{EmitterAck, Floor, Monotone};
use crate::source_db::SourceDbs;
use crate::xact::xact_buffer::{SubxactTracker, XactBuffer};
use ahash::HashMap;
use tokio_postgres::types::Oid;

/// One-shot fatal-error signal shared across pipeline stages. Pump polls
/// [`Fatal::message`] to exit with the root cause; the DDL barrier `select`s
/// on [`Fatal::wait`] so a CH outage mid-barrier surfaces instead of hanging.
#[derive(Clone)]
pub struct Fatal(Arc<watch::Sender<Option<String>>>);

impl Default for Fatal {
    fn default() -> Self {
        Self(Arc::new(watch::channel(None).0))
    }
}

impl Fatal {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record the first message (root cause) and wake every waiter; later
    /// calls keep the first.
    pub fn set(&self, msg: String) {
        self.0.send_if_modified(|slot| {
            if slot.is_some() {
                return false;
            }
            tracing::error!(target: "walshadow::pipeline", error = %msg, "pipeline fatal");
            *slot = Some(msg);
            true
        });
    }

    pub fn is_set(&self) -> bool {
        self.0.borrow().is_some()
    }

    pub fn message(&self) -> Option<String> {
        self.0.borrow().clone()
    }

    /// Resolve once the flag is set (now or later). Holding the sender keeps
    /// the channel open, so this never resolves unset.
    pub async fn wait(&self) {
        let _ = self.0.subscribe().wait_for(Option::is_some).await;
    }
}

/// Partial-batch flush deadline when operator left `flush_timeout` at 0;
/// without it a cold table's rows pin the watermark indefinitely.
const DEFAULT_PIPELINE_FLUSH: Duration = Duration::from_millis(100);

/// Tail selection: `ClickHouse` ships sealed blocks over N connections;
/// `Null` acks at swallow — metrics-only runs, zero CH connections.
pub enum TailKind {
    ClickHouse,
    Null,
}

/// Inputs the daemon supplies to stand up the pipeline.
pub struct PipelineConfig {
    /// Cluster-wide knobs: CH connection, insert budgets, pool sizes. The
    /// per-database table rules live on each [`SourceDb`](crate::source_db::SourceDb)
    pub emitter: EmitterConfig,
    /// Concurrent value resolutions the leaf reserve covers when `budget`
    /// is `None`
    pub decoder_pool_size: usize,
    pub inserter_pool_size: usize,
    /// Followed databases: descriptor log, shadow catalog, routing map
    pub dbs: Arc<SourceDbs>,
    pub oracle: Option<Arc<Oracle>>,
    /// One per database that applies CH DDL. A database with no entry has
    /// its schema events and truncates observed only (pairs with
    /// [`TailKind::Null`])
    pub applicators: HashMap<Oid, DdlApplicator>,
    pub tail: TailKind,
    pub buffer: Arc<Mutex<XactBuffer>>,
    pub subxact_tracker: Arc<Mutex<SubxactTracker>>,
    /// Speculative per-transaction catalog state shared with capture; empty
    /// unless pending capture is on
    pub pending: Arc<crate::catalog::pending::PendingCatalog>,
    pub stats: Arc<EmitterStats>,
    /// Per-txn span map shared with the pump + buffer; `Some` only when OTLP
    /// tracing is on. Reorder parents `commit.drain`/`dispatch` under `txn`.
    pub span_registry: Option<TxnSpanRegistry>,
    /// COPY backfillers for `initial_load='copy'` opt-ins, one per database.
    /// A database with no entry streams from the opt-in LSN only
    pub backfillers: HashMap<Oid, Arc<dyn crate::backfill::opt_in::Backfiller>>,
    /// Durable queue of deferred toast-mirror retires, loaded from the
    /// spill dir; entries due at resume retire via the post-spawn
    /// [`reorder::ReorderSink::flush_due_retires`] call
    pub retires: crate::toast::toast_retire::RetireLedger,
    /// Pending backup rows; recover outcomes with [`reorder::ReorderSink::settle_pending_boot`]
    pub pending_rows: crate::backfill::visibility_pending::SharedPendingLedger,
    /// Persisted resolved floor (aligned, archive-clamped), seeded at the
    /// resolved start; pruners cut against it verbatim
    pub resume_floor: Arc<Monotone<Floor>>,
    /// Shared resident-payload pool ([`build_budget`]); backup passes run
    /// concurrently with the pipeline and must draw from the same pool.
    /// `None` builds one at spawn
    pub budget: Option<crate::budget::MemoryBudget>,
}

/// Spawned-stage join handles + shared signals. The daemon drives the
/// [`reorder::ReorderSink`] as inner sink of its `QueueingRecordSink`; once
/// that sink drops the batcher channel closes and [`PipelineHandle::join`]
/// drains the rest in order.
pub struct PipelineHandle {
    /// Live contiguous-done watermark, floored before manifest persistence
    pub emitter_ack: Arc<Monotone<EmitterAck>>,
    /// Expose stalls after rows leave transaction buffer
    pub ack_probe: tokio::sync::watch::Receiver<ack::AckSnapshot>,
    pub fatal: Fatal,
    pub toast: crate::toast::ToastResolver,
    /// Global resident-payload pool, exposed for the metrics loop
    pub budget: crate::budget::MemoryBudget,
    tail: tail::TailParts,
}

impl PipelineHandle {
    /// Await the drain cascade: batcher flushes-all + exits → inserters drain
    /// to `EndOfStream` + exit → ack collector exits. Surfaces any fatal
    /// error.
    pub async fn join(self) -> Result<(), String> {
        self.tail.join().await;
        match self.fatal.message() {
            Some(msg) => Err(msg),
            None => Ok(()),
        }
    }
}

impl PipelineConfig {
    /// Stand up the pipeline. Returns the reorder sink (drive via the daemon's
    /// `QueueingRecordSink`) and a handle for shutdown / watermark reads. Fails
    /// only if an inserter connection can't open.
    pub async fn spawn(
        self,
        emitter_ack: Arc<Monotone<EmitterAck>>,
    ) -> Result<(reorder::ReorderSink, PipelineHandle), EmitterError> {
        let PipelineConfig {
            emitter,
            decoder_pool_size,
            inserter_pool_size,
            dbs,
            oracle,
            applicators,
            tail,
            buffer,
            subxact_tracker,
            pending,
            stats,
            span_registry,
            backfillers,
            retires,
            pending_rows,
            resume_floor,
            budget,
        } = self;
        let emitter = Arc::new(emitter);
        let dest = dbs.primary().dest.clone();
        let fatal = Fatal::new();

        let budget = match budget {
            Some(b) => b,
            None => build_budget(&emitter, decoder_pool_size).map_err(EmitterError::Config)?,
        };

        // Reorder coordinator detoasts at planning and puts per commit
        // Metrics-only (Null tail) has no CH connection, so no chunk store.
        let mut resolver = if matches!(tail, TailKind::Null) {
            crate::toast::ToastResolver::disabled().with_stats(stats.clone())
        } else {
            // Shadow mode requires oracle bridge
            crate::toast::ToastResolver::for_mode(
                dest.clone(),
                stats.clone(),
                oracle
                    .as_deref()
                    .map(crate::toast::shadow_store::ShadowRead::from),
            )
            .map_err(EmitterError::Config)?
        }
        .with_budget(budget.clone());

        if emitter.toast.mode.is_shadow() && !matches!(tail, TailKind::Null) {
            let ceiling = oracle
                .as_ref()
                .expect("shadow oracle checked")
                .xid_ceiling();
            resolver = resolver.with_database_stores(dbs.all().iter().map(|db| {
                let read = crate::toast::shadow_store::ShadowRead {
                    bridge: crate::toast::shadow_store::bound(db.bridge.clone()),
                    ceiling: ceiling.clone(),
                };
                (
                    db.oid,
                    Arc::new(crate::toast::shadow_store::ShadowToastStore::late(read))
                        as Arc<dyn crate::toast::ChunkStore>,
                )
            }));
        }

        // Live emitter knobs (budgets/flush/compression/retry) reach the batcher
        // + inserter pool via this receiver; `None` keeps them at boot values.
        // Insert-side knobs are cluster-wide, so the primary's resolver is
        // the one the tail tracks
        let tail_config_rx = dbs.primary().resolver.as_ref().map(|r| r.subscribe());

        // Shared tail (ack collector + inserter pool + batcher), the same unit
        // bootstrap feeds via the page walk. Null swaps in the swallow task.
        let (msg_tx, ack, tail) = match tail {
            TailKind::ClickHouse => {
                tail::spawn_with_config(
                    dest.clone(),
                    inserter_pool_size,
                    stats.clone(),
                    emitter_ack.clone(),
                    fatal.clone(),
                    tail_config_rx,
                    oracle,
                )
                .await?
            }
            TailKind::Null => tail::spawn_null(emitter_ack.clone(), fatal.clone()),
        };
        let ack_probe = ack.probe();

        let plan_dir = buffer.lock().await.scratch_dir().to_path_buf();
        let reorder = reorder::ReorderSink::new(
            buffer,
            dbs,
            pending,
            subxact_tracker,
            applicators,
            ack,
            msg_tx,
            stats,
            resolver.clone(),
            backfillers,
            fatal.clone(),
            span_registry,
            emitter.drain_batch_rows,
            emitter.drain_batch_bytes,
            emitter.plan_disk_max,
            plan_dir,
            Some(budget.clone()),
            retires,
            pending_rows,
            dest,
            resume_floor,
        );

        Ok((
            reorder,
            PipelineHandle {
                emitter_ack,
                ack_probe,
                fatal,
                toast: resolver,
                budget,
                tail,
            },
        ))
    }
}

/// Validated shared pool from `[memory]` knobs; built once per process
/// and handed to every metered component (pipeline, backup passes)
pub fn build_budget(
    emitter: &EmitterConfig,
    decoders: usize,
) -> Result<crate::budget::MemoryBudget, String> {
    let reserve = leaf_reserve_for(
        emitter.resident_payload_max,
        emitter.value_reserve,
        decoders,
    )?;
    // Progress invariant: a drain admits slices while holding its sealed
    // generations (bounded by the body-spool + index caps), so the
    // compartment must fit both plus slice headroom or a mid-drain admit
    // could wait on units the drain itself holds
    let admission_max = emitter.resident_payload_max - reserve;
    let drain_floor = crate::xact::xact_buffer::TOAST_BODY_SPOOL_MEM_MAX
        + crate::xact::xact_buffer::TOAST_INDEX_MEM_MAX
        + 2 * emitter.drain_batch_bytes;
    if admission_max < drain_floor {
        return Err(format!(
            "admission compartment {admission_max} below drain floor {drain_floor} \
             (body spool cap + index cap + 2x drain_batch_bytes); raise \
             resident_payload_max or lower drain_batch_bytes",
        ));
    }
    Ok(crate::budget::MemoryBudget::with_leaf_reserve(
        emitter.resident_payload_max,
        reserve,
    ))
}

/// Reserve `value_reserve` bytes per decoder
///
/// Keep reserve below half of pool so pipeline can accept nonempty batches.
/// Values larger than reserve run one at a time
pub fn leaf_reserve_for(
    resident_payload_max: usize,
    value_reserve: usize,
    decoders: usize,
) -> Result<usize, String> {
    let reserve = decoders.max(1).saturating_mul(value_reserve);
    if reserve > resident_payload_max / 2 {
        return Err(format!(
            "value_reserve needs {reserve} bytes for {} decoders, more than half of \
             resident_payload_max {resident_payload_max}; raise resident_payload_max \
             or lower value_reserve or decoder_pool_size",
            decoders.max(1),
        ));
    }
    Ok(reserve)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Default byte values with a wide decode pool must fail startup, not
    /// zero out the admission compartment
    #[test]
    fn leaf_reserve_rejects_zero_admission_headroom() {
        let defaults = crate::emit::ch_emitter::EmitterConfig::default();
        let (pool, value) = (defaults.resident_payload_max, defaults.value_reserve);
        assert_eq!(leaf_reserve_for(pool, value, 1), Ok(value));
        assert_eq!(leaf_reserve_for(pool, value, 4), Ok(4 * value));
        // Equality with the pool previously passed the old check and left
        // admission_max == 0
        assert!(leaf_reserve_for(pool, pool / 8, 8).is_err());
        assert!(leaf_reserve_for(pool, pool, 1).is_err());
    }

    /// Do not size pool from maximum value size
    #[test]
    fn hard_value_cap_does_not_size_the_pool() {
        let cfg = crate::emit::ch_emitter::EmitterConfig {
            resident_payload_max: 512 << 20,
            inline_value_max: 1 << 30,
            ..Default::default()
        };
        assert!(build_budget(&cfg, 4).is_ok());
    }

    /// Admission must fit one drain's retained state plus slice headroom,
    /// else a mid-drain admit could wait on units the drain itself holds
    #[test]
    fn build_budget_rejects_admission_below_drain_floor() {
        let mut cfg = crate::emit::ch_emitter::EmitterConfig::default();
        assert!(build_budget(&cfg, 4).is_ok());
        cfg.resident_payload_max = 64 << 20;
        cfg.value_reserve = 1 << 20;
        assert!(build_budget(&cfg, 1).is_err());
    }
}
