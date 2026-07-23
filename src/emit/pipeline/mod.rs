//! Parallel decode + insert pipeline.
//!
//! ```text
//! pump -> QueueingRecordSink -> reorder -> [decode x M] -> InsertBatcher
//!            -> [inserter x N] -> ClickHouse
//!                              \-> ack collector -> emitter_ack_lsn
//! ```
//!
//! See `plans/future/pipeline_backpressure_and_scaling.md`. Pool sizes M/N come from
//! the CLI; size-1 is the degenerate serial case. The [`ack`] watermark is
//! contiguous-done so source slot recycling never outruns CH durability.

pub mod ack;
pub mod batcher;
pub mod bootstrap;
pub mod decode;
pub mod inserter;
pub mod plan_spool;
pub mod planner;
pub mod reorder;
pub mod tail;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use tokio::sync::{Mutex, Notify};
use tokio::task::JoinHandle;

use crate::catalog::shadow_catalog::ShadowCatalog;
use crate::ch::EmitterError;
use crate::emit::ch_ddl::DdlApplicator;
use crate::emit::ch_emitter::{EmitterConfig, EmitterStats};
use crate::mapping::MappingHandle;
use crate::ops::oracle::Oracle;
use crate::ops::trace::TxnSpanRegistry;
use crate::xact::xact_buffer::{SubxactTracker, XactBuffer};

/// One-shot fatal-error signal shared across pipeline stages. Pump polls
/// [`Fatal::message`] to exit with the root cause; the DDL barrier `select`s
/// on [`Fatal::wait`] so a CH outage mid-barrier surfaces instead of hanging.
#[derive(Clone, Default)]
pub struct Fatal {
    flag: Arc<AtomicBool>,
    msg: Arc<std::sync::Mutex<Option<String>>>,
    notify: Arc<Notify>,
}

impl Fatal {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record the first message (root cause) and wake every waiter; later
    /// calls keep the first.
    pub fn set(&self, msg: String) {
        {
            let mut slot = self.msg.lock().expect("fatal slot poisoned");
            if slot.is_none() {
                tracing::error!(target: "walshadow::pipeline", error = %msg, "pipeline fatal");
                *slot = Some(msg);
            }
        }
        self.flag.store(true, Ordering::Release);
        self.notify.notify_waiters();
    }

    pub fn is_set(&self) -> bool {
        self.flag.load(Ordering::Acquire)
    }

    pub fn message(&self) -> Option<String> {
        self.msg.lock().expect("fatal slot poisoned").clone()
    }

    /// Resolve once the flag is set (now or later).
    pub async fn wait(&self) {
        loop {
            let notified = self.notify.notified();
            if self.is_set() {
                return;
            }
            notified.await;
            if self.is_set() {
                return;
            }
        }
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
    pub emitter: EmitterConfig,
    pub decoder_pool_size: usize,
    pub inserter_pool_size: usize,
    pub catalog: Arc<Mutex<ShadowCatalog>>,
    pub mapping: MappingHandle,
    pub oracle: Option<Arc<Oracle>>,
    /// `None` observes schema events / truncates without CH DDL (pairs
    /// with [`TailKind::Null`])
    pub applicator: Option<DdlApplicator>,
    pub tail: TailKind,
    pub buffer: Arc<Mutex<XactBuffer>>,
    pub subxact_tracker: Arc<Mutex<SubxactTracker>>,
    /// Durable descriptor log: decode pool + reorder read interval-scoped
    /// descriptors from it
    pub log: Arc<crate::catalog::desc_log::DescriptorLog>,
    pub stats: Arc<EmitterStats>,
    /// Per-txn span map shared with the pump + buffer; `Some` only when OTLP
    /// tracing is on. Reorder parents `commit.drain`/`dispatch` under `txn`.
    pub span_registry: Option<TxnSpanRegistry>,
    /// Runtime-config overlay resolver, applied to inside the reorder barrier
    /// when a `DrainEntry::Config` drains. `None` disables live config apply.
    pub config_resolver: Option<Arc<crate::config::ConfigResolver>>,
    /// COPY backfiller for `initial_load='copy'` opt-ins; `None` streams from
    /// the opt-in LSN only.
    pub backfiller: Option<Arc<dyn crate::backfill::opt_in::Backfiller>>,
    /// Durable queue of deferred toast-mirror retires, loaded from the
    /// spill dir; entries due at resume retire via the post-spawn
    /// [`reorder::ReorderSink::flush_due_retires`] call
    pub retires: crate::toast::toast_retire::RetireLedger,
    /// Persisted resolved floor (aligned, archive-clamped), seeded at the
    /// resolved start; pruners cut against it verbatim
    pub resume_floor: Arc<AtomicU64>,
    /// Shared resident-payload pool ([`build_budget`]); backup passes run
    /// concurrently with the pipeline and must draw from the same pool.
    /// `None` builds one at spawn
    pub budget: Option<crate::budget::MemoryBudget>,
}

/// Spawned-stage join handles + shared signals. The daemon drives the
/// [`reorder::ReorderSink`] as inner sink of its `QueueingRecordSink`; once
/// that sink drops the job queue closes and [`PipelineHandle::join`] drains
/// the rest in order.
pub struct PipelineHandle {
    /// Durable watermark (contiguous-done commit_lsn). Pump advertises it as
    /// the standby `apply_lsn` and writes it to the resume cursor.
    pub emitter_ack: Arc<AtomicU64>,
    pub fatal: Fatal,
    pub toast: crate::toast::ToastResolver,
    /// Global resident-payload pool, exposed for the metrics loop
    pub budget: crate::budget::MemoryBudget,
    decoders: Vec<JoinHandle<()>>,
    tail: tail::TailParts,
}

impl PipelineHandle {
    /// Await the drain cascade: decoders finish and drop row senders → batcher
    /// flushes-all + exits → inserters drain to `EndOfStream` + exit → ack
    /// collector exits. Surfaces any fatal error.
    pub async fn join(self) -> Result<(), String> {
        for h in self.decoders {
            let _ = h.await;
        }
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
        emitter_ack: Arc<AtomicU64>,
    ) -> Result<(reorder::ReorderSink, PipelineHandle), EmitterError> {
        let PipelineConfig {
            emitter,
            decoder_pool_size,
            inserter_pool_size,
            catalog,
            mapping,
            oracle,
            applicator,
            tail,
            buffer,
            subxact_tracker,
            log,
            stats,
            span_registry,
            config_resolver,
            backfiller,
            retires,
            resume_floor,
            budget,
        } = self;
        let m = decoder_pool_size.max(1);
        let fatal = Fatal::new();

        let budget = match budget {
            Some(b) => b,
            None => build_budget(&emitter, m).map_err(EmitterError::Config)?,
        };

        // One resolver shared by the decode pool (fetch on miss) and the
        // reorder coordinator (put per commit).
        let resolver = crate::toast::ToastResolver::from_config(&emitter, stats.clone())
            .with_budget(budget.clone());

        // Live emitter knobs (budgets/flush/compression/retry) reach the batcher
        // + inserter pool via this receiver; `None` keeps them at boot values.
        let tail_config_rx = config_resolver.as_ref().map(|r| r.subscribe());

        // Shared tail (ack collector + inserter pool + batcher), the same unit
        // bootstrap feeds via the page walk. Null swaps in the swallow task.
        let (msg_tx, ack, tail) = match tail {
            TailKind::ClickHouse => {
                tail::spawn_with_config(
                    &emitter,
                    inserter_pool_size,
                    stats.clone(),
                    emitter_ack.clone(),
                    fatal.clone(),
                    tail_config_rx,
                )
                .await?
            }
            TailKind::Null => tail::spawn_null(emitter_ack.clone()),
        };

        // Job-queue bound scales with the decode pool for bounded overlap
        let (jobs_tx, jobs_rx) = async_channel::bounded::<decode::DecodeJob>((m * 4).max(8));

        let ctx = decode::DecodeCtx {
            oracle,
            msg_tx: msg_tx.clone(),
            stats: stats.clone(),
            resolver: resolver.clone(),
            chunk_rows: emitter.decode_chunk_rows,
        };
        let decoders = decode::spawn_pool(m, ctx, jobs_rx, ack.clone(), fatal.clone());

        let plan_dir = buffer.lock().await.spill_dir().to_path_buf();
        let reorder = reorder::ReorderSink::new(
            buffer,
            log,
            catalog,
            subxact_tracker,
            applicator,
            ack,
            jobs_tx,
            msg_tx,
            stats,
            resolver.clone(),
            config_resolver,
            backfiller,
            fatal.clone(),
            span_registry,
            emitter.drain_batch_rows,
            emitter.drain_batch_bytes,
            emitter.plan_disk_max,
            plan_dir,
            Some(budget.clone()),
            retires,
            resume_floor,
            mapping,
            emitter.soft_delete,
        );

        Ok((
            reorder,
            PipelineHandle {
                emitter_ack,
                fatal,
                toast: resolver,
                budget,
                decoders,
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
        emitter.inline_value_max,
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

/// Leaf reserve: one in-flight per-value transient per decode worker.
/// Reserve capped at half the pool so admission keeps meaningful
/// headroom — at equality every nonempty slice would fail `admit`
pub fn leaf_reserve_for(
    resident_payload_max: usize,
    inline_value_max: usize,
    decoders: usize,
) -> Result<usize, String> {
    let reserve = decoders.max(1).saturating_mul(inline_value_max);
    if reserve > resident_payload_max / 2 {
        return Err(format!(
            "leaf reserve {reserve} ({} decoders x inline_value_max {inline_value_max}) \
             exceeds half of resident_payload_max {resident_payload_max}; raise \
             resident_payload_max or lower inline_value_max / decoder pool",
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
        let (pool, value) = (defaults.resident_payload_max, defaults.inline_value_max);
        assert_eq!(leaf_reserve_for(pool, value, 1), Ok(value));
        assert_eq!(leaf_reserve_for(pool, value, 4), Ok(4 * value));
        assert!(leaf_reserve_for(pool, value, 8).is_err());
        // Equality with the pool previously passed the old check and left
        // admission_max == 0
        assert!(leaf_reserve_for(pool, pool / 8, 8).is_err());
    }

    /// Admission must fit one drain's retained state plus slice headroom,
    /// else a mid-drain admit could wait on units the drain itself holds
    #[test]
    fn build_budget_rejects_admission_below_drain_floor() {
        let mut cfg = crate::emit::ch_emitter::EmitterConfig::default();
        assert!(build_budget(&cfg, 4).is_ok());
        cfg.resident_payload_max = 64 << 20;
        cfg.inline_value_max = 1 << 20;
        assert!(build_budget(&cfg, 1).is_err());
    }
}
