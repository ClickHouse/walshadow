//! Parallel decode + insert pipeline.
//!
//! Replaces the serial CH emitter tail with a fan-out:
//!
//! ```text
//! pump -> QueueingRecordSink -> reorder -> [decode x M] -> InsertBatcher
//!            -> [inserter x N] -> ClickHouse
//!                              \-> ack collector -> emitter_ack_lsn
//! ```
//!
//! See `plans/future/parallel_decode_and_insert.md`. Pool sizes M (decode)
//! and N (insert) come from the CLI; size-1 pools are the degenerate
//! serial case. The watermark fed back to the daemon ([`ack`]) is
//! contiguous-done so source slot recycling never outruns CH durability.

pub mod ack;
pub mod batcher;
pub mod decode;
pub mod inserter;
pub mod mpmc;
pub mod reorder;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use tokio::sync::{Mutex, Notify, mpsc};
use tokio::task::JoinHandle;

use clickhouse_c::Allocator;

use crate::ch_ddl::DdlApplicator;
use crate::ch_emitter::{EmitterConfig, EmitterError, EmitterStats, MappingHandle};
use crate::oracle::Oracle;
use crate::shadow_catalog::ShadowCatalog;
use crate::xact_buffer::{SchemaEventRx, SubxactTracker, XactBuffer};

/// One-shot fatal-error signal shared across pipeline stages. A stage that
/// hits an unrecoverable error (encode rejection, retry-exhausted inserter,
/// decode/catalog failure) calls [`Fatal::set`]; the daemon's pump loop
/// polls [`Fatal::message`] to exit cleanly with the root cause, and the
/// DDL barrier `select`s on [`Fatal::wait`] so a CH outage mid-barrier
/// surfaces instead of hanging.
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

    /// Record the first fatal message and wake every waiter. Later calls
    /// keep the first message (the root cause).
    pub fn set(&self, msg: String) {
        {
            let mut g = self.msg.lock().expect("fatal slot poisoned");
            if g.is_none() {
                *g = Some(msg);
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

    /// Resolve once the fatal flag is set (now or later).
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

/// Partial-batch flush deadline used when the operator left
/// `flush_timeout` at 0. In the pipeline a cold table's rows would
/// otherwise pin the watermark indefinitely, so 0 means "use this".
const DEFAULT_PIPELINE_FLUSH: Duration = Duration::from_millis(100);

/// Inputs the daemon supplies to stand up the pipeline. Mirrors what the
/// serial emitter path consumed, plus the two pool sizes.
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
    pub pg_class_delete_epoch: Option<Arc<AtomicU64>>,
    pub stats: Arc<EmitterStats>,
}

/// Spawned-stage join handles + the shared signals the daemon reads. The
/// daemon drives the returned [`reorder::ReorderSink`] as the inner sink of
/// its `QueueingRecordSink`; once that sink drops (worker close) the job
/// queue closes and [`PipelineHandle::join`] drains the rest in order.
pub struct PipelineHandle {
    /// Durable watermark (contiguous-done commit_lsn). Pump advertises it
    /// as the standby `apply_lsn` and writes it to the resume cursor.
    pub emitter_ack: Arc<AtomicU64>,
    pub fatal: Fatal,
    collector: JoinHandle<()>,
    batcher: JoinHandle<()>,
    decoders: Vec<JoinHandle<()>>,
    inserters: Vec<JoinHandle<()>>,
}

impl PipelineHandle {
    /// Await the drain cascade: decoders (job queue closed) finish and drop
    /// their row senders → batcher flushes-all + exits → inserters drain to
    /// `EndOfStream` + exit → ack collector exits. Surfaces any fatal error.
    pub async fn join(self) -> Result<(), String> {
        for h in self.decoders {
            let _ = h.await;
        }
        let _ = self.batcher.await;
        for h in self.inserters {
            let _ = h.await;
        }
        let _ = self.collector.await;
        match self.fatal.message() {
            Some(msg) => Err(msg),
            None => Ok(()),
        }
    }
}

impl PipelineConfig {
    /// Stand up the pipeline: ack collector, inserter pool (N connections),
    /// batcher, decode pool (M), and the reorder coordinator. Returns the
    /// reorder sink (drive it via the daemon's `QueueingRecordSink`) and a
    /// handle for shutdown / watermark reads. Fails only if an inserter
    /// connection can't be opened.
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
            pg_class_delete_epoch,
            stats,
        } = self;
        let m = decoder_pool_size.max(1);
        let n = inserter_pool_size.max(1);
        let fatal = Fatal::new();

        let (ack, collector) = ack::spawn(emitter_ack.clone());

        // Channel bounds scale with pool sizes for bounded overlap. Rows
        // (decode pool) and FlushAll (barrier) share one FIFO channel so the
        // barrier flush can't overtake enqueued rows.
        let (jobs_tx, jobs_rx) = mpmc::channel::<decode::DecodeJob>((m * 4).max(8));
        let (msg_tx, msg_rx) = mpsc::channel::<batcher::BatcherMsg>(8192);
        let (batches_tx, batches_rx) = mpmc::channel::<batcher::InsertBatch>((n * 2).max(4));

        // Inserters first (they only consume): a connect failure aborts
        // before any other stage spins up.
        let inserters = inserter::spawn_pool(
            n,
            &emitter,
            batches_rx,
            ack.clone(),
            stats.clone(),
            fatal.clone(),
        )
        .await?;

        let flush_timeout = if emitter.flush_timeout.is_zero() {
            DEFAULT_PIPELINE_FLUSH
        } else {
            emitter.flush_timeout
        };
        let batcher = batcher::spawn(
            msg_rx,
            batches_tx,
            batcher::BatcherConfig {
                row_budget: emitter.row_budget,
                byte_budget: emitter.byte_budget,
                flush_timeout,
            },
            Allocator::stdlib(),
            fatal.clone(),
        );

        let ctx = decode::DecodeCtx {
            catalog: catalog.clone(),
            mapping,
            oracle,
            msg_tx: msg_tx.clone(),
            stats: stats.clone(),
        };
        let decoders = decode::spawn_pool(m, ctx, jobs_rx, ack.clone(), fatal.clone());

        let reorder = reorder::ReorderSink::new(
            buffer,
            catalog,
            subxact_tracker,
            schema_events,
            pg_class_delete_epoch,
            applicator,
            ack,
            jobs_tx,
            msg_tx,
            stats,
            fatal.clone(),
        );

        Ok((
            reorder,
            PipelineHandle {
                emitter_ack,
                fatal,
                collector,
                batcher,
                decoders,
                inserters,
            },
        ))
    }
}
