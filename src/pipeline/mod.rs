//! Parallel decode + insert pipeline.
//!
//! ```text
//! pump -> QueueingRecordSink -> reorder -> [decode x M] -> InsertBatcher
//!            -> [inserter x N] -> ClickHouse
//!                              \-> ack collector -> emitter_ack_lsn
//! ```
//!
//! See `plans/future/parallel_decode_and_insert.md`. Pool sizes M/N come from
//! the CLI; size-1 is the degenerate serial case. The [`ack`] watermark is
//! contiguous-done so source slot recycling never outruns CH durability.

pub mod ack;
pub mod batcher;
pub mod bootstrap;
pub mod decode;
pub mod inserter;
pub mod mpmc;
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
use crate::shadow_catalog::ShadowCatalog;
use crate::xact_buffer::{SchemaEventRx, SubxactTracker, XactBuffer};

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
    qualified_name: &str,
    stats: &EmitterStats,
) -> Option<Arc<TableMapping>> {
    match mapping.read().await.get(qualified_name) {
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
    pub pg_class_delete_epoch: Option<Arc<AtomicU64>>,
    pub stats: Arc<EmitterStats>,
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
            pg_class_delete_epoch,
            stats,
        } = self;
        let m = decoder_pool_size.max(1);
        let fatal = Fatal::new();

        // Shared tail (ack collector + inserter pool + batcher), the same unit
        // bootstrap feeds via the page walk.
        let (msg_tx, ack, tail) = tail::spawn(
            &emitter,
            inserter_pool_size,
            stats.clone(),
            emitter_ack.clone(),
            fatal.clone(),
        )
        .await?;

        // Job-queue bound scales with the decode pool for bounded overlap
        let (jobs_tx, jobs_rx) = mpmc::channel::<decode::DecodeJob>((m * 4).max(8));

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
                decoders,
                tail,
            },
        ))
    }
}
