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
pub mod reorder;
pub mod tail;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use tokio::sync::{Mutex, Notify};
use tokio::task::JoinHandle;

use crate::ch_ddl::DdlApplicator;
use crate::ch_emitter::{EmitterConfig, EmitterError, EmitterStats, MappingHandle, TableMapping};
use crate::oracle::Oracle;
use crate::shadow_catalog::{RelName, ShadowCatalog};
use crate::xact_buffer::{SchemaEventRx, SubxactTracker, TxnSpanRegistry, XactBuffer};

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

/// Resolve a relation's destination mapping. Bumps `unsupported_relations`
/// and returns None when the relation maps to no destination
pub(crate) async fn lookup_mapping(
    mapping: &MappingHandle,
    rel: &RelName,
    stats: &EmitterStats,
) -> Option<Arc<TableMapping>> {
    match mapping.read().await.get(rel) {
        Some(v) => Some(Arc::new(v.clone())),
        None => {
            stats
                .unsupported_relations
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            None
        }
    }
}

/// Inputs the daemon supplies to stand up the pipeline.
pub struct PipelineConfig {
    pub emitter: EmitterConfig,
    pub decoder_pool_size: usize,
    pub inserter_pool_size: usize,
    pub catalog: Arc<Mutex<ShadowCatalog>>,
    pub mapping: MappingHandle,
    pub oracle: Option<Arc<Oracle>>,
    pub applicator: DdlApplicator,
    pub buffer: Arc<Mutex<XactBuffer>>,
    pub subxact_tracker: Arc<Mutex<SubxactTracker>>,
    pub schema_events: Option<SchemaEventRx>,
    /// Same handle as the decoder's `with_catalog_signals` armer; reorder
    /// consumes at the arming xact's commit
    pub pending_sweeps: Option<crate::catalog_tracker::PendingSweeps>,
    pub stats: Arc<EmitterStats>,
    /// Per-txn span map shared with the pump + buffer; `Some` only when OTLP
    /// tracing is on. Reorder parents `commit.drain`/`dispatch` under `txn`.
    pub span_registry: Option<TxnSpanRegistry>,
    /// Runtime-config overlay resolver, applied to inside the reorder barrier
    /// when a `DrainEntry::Config` drains. `None` disables live config apply.
    pub config_resolver: Option<Arc<crate::config::ConfigResolver>>,
    /// COPY backfiller for `initial_load='copy'` opt-ins; `None` streams from
    /// the opt-in LSN only.
    pub backfiller: Option<Arc<crate::copy_backfill::CopyBackfiller>>,
    /// Durable queue of deferred toast-mirror retires, loaded from the
    /// spill dir; entries due at resume retire via the post-spawn
    /// [`reorder::ReorderSink::flush_due_retires`] call
    pub retires: crate::toast_retire::RetireLedger,
    /// Last durable resume cursor, seeded at the resume point
    pub resume_floor: Arc<AtomicU64>,
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
            buffer,
            subxact_tracker,
            schema_events,
            pending_sweeps,
            stats,
            span_registry,
            config_resolver,
            backfiller,
            retires,
            resume_floor,
        } = self;
        let m = decoder_pool_size.max(1);
        let fatal = Fatal::new();

        // One resolver shared by the decode pool (fetch on miss) and the
        // reorder coordinator (put per commit).
        let resolver = crate::toast::ToastResolver::from_config(&emitter, stats.clone());

        // Live emitter knobs (budgets/flush/compression/retry) reach the batcher
        // + inserter pool via this receiver; `None` keeps them at boot values.
        let tail_config_rx = config_resolver.as_ref().map(|r| r.subscribe());

        // Shared tail (ack collector + inserter pool + batcher), the same unit
        // bootstrap feeds via the page walk.
        let (msg_tx, ack, tail) = tail::spawn_with_config(
            &emitter,
            inserter_pool_size,
            stats.clone(),
            emitter_ack.clone(),
            fatal.clone(),
            tail_config_rx,
        )
        .await?;

        // Job-queue bound scales with the decode pool for bounded overlap
        let (jobs_tx, jobs_rx) = async_channel::bounded::<decode::DecodeJob>((m * 4).max(8));

        let ctx = decode::DecodeCtx {
            catalog: catalog.clone(),
            mapping,
            oracle,
            msg_tx: msg_tx.clone(),
            stats: stats.clone(),
            resolver: resolver.clone(),
            chunk_rows: emitter.decode_chunk_rows,
        };
        let decoders = decode::spawn_pool(m, ctx, jobs_rx, ack.clone(), fatal.clone());

        let reorder = reorder::ReorderSink::new(
            buffer,
            catalog,
            subxact_tracker,
            schema_events,
            pending_sweeps,
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
            retires,
            resume_floor,
        );

        Ok((
            reorder,
            PipelineHandle {
                emitter_ack,
                fatal,
                toast: resolver,
                decoders,
                tail,
            },
        ))
    }
}
