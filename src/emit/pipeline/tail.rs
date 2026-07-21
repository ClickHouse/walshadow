//! Reusable insert tail: batcher + inserter pool + ack collector.
//!
//! Both the WAL pipeline
//! ([`PipelineConfig::spawn`](crate::emit::pipeline::PipelineConfig::spawn)) and
//! greenfield bootstrap ([`bootstrap::drain`](crate::emit::pipeline::bootstrap))
//! feed this identical tail — one shipping path so bootstrap inherits the
//! N-connection inserter pool, reconnect + retry, the durable watermark, and
//! backpressure for free.
//!
//! Drains in cascade once every `BatcherMsg` sender drops: batcher
//! final-flushes and exits → inserters drain to `EndOfStream` and exit → ack
//! collector exits.

use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use clickhouse_c::Allocator;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinHandle;

use crate::ch::EmitterError;
use crate::config::ResolvedConfig;
use crate::emit::ch_emitter::{EmitterConfig, EmitterStats};
use crate::emit::pipeline::ack::{self, AckHandle};
use crate::emit::pipeline::batcher::{self, BatcherConfig, BatcherMsg, InsertBatch};
use crate::emit::pipeline::inserter;
use crate::emit::pipeline::{DEFAULT_PIPELINE_FLUSH, Fatal};

/// Spawned tail stages; holding this keeps the tasks owned by the caller.
pub struct TailParts {
    collector: JoinHandle<()>,
    batcher: JoinHandle<()>,
    inserters: Vec<JoinHandle<()>>,
}

impl TailParts {
    /// Await the drain cascade. Call only after every producer-held `msg_tx`
    /// and `AckHandle` clone has dropped, else the batcher never sees its
    /// channel close and this hangs.
    pub async fn join(self) {
        let _ = self.batcher.await;
        for h in self.inserters {
            let _ = h.await;
        }
        let _ = self.collector.await;
    }

    /// Bootstrap completion + teardown: seal partial batches, wait every seq
    /// < `through` durable on CH, then drain the tail. Consumes producer
    /// handles so the drop-before-join ordering can't be gotten wrong; `fatal`
    /// short-circuits a CH outage instead of hanging.
    pub async fn finish(
        self,
        msg_tx: mpsc::Sender<BatcherMsg>,
        ack: AckHandle,
        through: u64,
        fatal: &Fatal,
    ) -> Result<(), String> {
        let (reply_tx, reply_rx) = oneshot::channel();
        if msg_tx.send(BatcherMsg::FlushAll(reply_tx)).await.is_err() {
            return Err(fatal
                .message()
                .unwrap_or_else(|| "tail closed before flush".into()));
        }
        tokio::select! {
            r = reply_rx => r.map_err(|_| "batcher dropped flush ack".to_string())?,
            _ = fatal.wait() => {
                return Err(fatal.message().unwrap_or_else(|| "tail fatal during flush".into()));
            }
        }
        tokio::select! {
            _ = ack.wait_through(through) => {}
            _ = fatal.wait() => {
                return Err(fatal.message().unwrap_or_else(|| "tail fatal during drain".into()));
            }
        }
        drop(msg_tx);
        drop(ack);
        self.join().await;
        if let Some(msg) = fatal.message() {
            return Err(msg);
        }
        Ok(())
    }
}

/// Stand up the tail: ack collector, inserter pool (`n` connections),
/// batcher. Returns the `BatcherMsg` sender + [`AckHandle`] (clone into
/// producers) and join handles. Fails only if an inserter connection can't
/// open — inserters spin up first (consume-only) so a connect failure aborts
/// before any other stage starts.
pub async fn spawn(
    emitter: &EmitterConfig,
    inserter_pool_size: usize,
    stats: Arc<EmitterStats>,
    emitter_ack: Arc<AtomicU64>,
    fatal: Fatal,
) -> Result<(mpsc::Sender<BatcherMsg>, AckHandle, TailParts), EmitterError> {
    spawn_with_config(emitter, inserter_pool_size, stats, emitter_ack, fatal, None).await
}

/// Metrics-only tail: ack collector + one swallow task, zero CH
/// connections. Every routed row acks at swallow (permits release on
/// drop) so nothing can pin the watermark; `FlushAll` replies
/// immediately — nothing buffers. Keeps the placed/acked protocol
/// identical to the CH tail, so reorder/decode stages run unchanged.
pub fn spawn_null(emitter_ack: Arc<AtomicU64>) -> (mpsc::Sender<BatcherMsg>, AckHandle, TailParts) {
    let (ack, collector) = ack::spawn(emitter_ack);
    let (msg_tx, mut msg_rx) = mpsc::channel::<BatcherMsg>(256);
    let swallow_ack = ack.clone();
    let batcher = tokio::spawn(async move {
        while let Some(msg) = msg_rx.recv().await {
            match msg {
                BatcherMsg::Row(r) => swallow_ack.acked(vec![(r.seq, 1)]),
                BatcherMsg::Rows(rows) => {
                    let mut counts: std::collections::HashMap<u64, u64> =
                        std::collections::HashMap::new();
                    for r in &rows {
                        *counts.entry(r.seq).or_insert(0) += 1;
                    }
                    swallow_ack.acked(counts.into_iter().collect());
                }
                BatcherMsg::FlushAll(reply) => {
                    let _ = reply.send(());
                }
            }
        }
    });
    (
        msg_tx,
        ack,
        TailParts {
            collector,
            batcher,
            inserters: Vec::new(),
        },
    )
}

/// [`spawn`] plus a live config receiver: the batcher re-reads
/// budgets/flush and the inserter pool re-reads compression/retry from it on
/// each republish. `None` == boot values only (bootstrap + tests use [`spawn`]).
pub async fn spawn_with_config(
    emitter: &EmitterConfig,
    inserter_pool_size: usize,
    stats: Arc<EmitterStats>,
    emitter_ack: Arc<AtomicU64>,
    fatal: Fatal,
    config_rx: Option<watch::Receiver<Arc<ResolvedConfig>>>,
) -> Result<(mpsc::Sender<BatcherMsg>, AckHandle, TailParts), EmitterError> {
    let n = inserter_pool_size.max(1);

    let (ack, collector) = ack::spawn(emitter_ack);

    // Rows and FlushAll share one FIFO channel so a flush can't overtake rows
    // enqueued before it
    let (msg_tx, msg_rx) = mpsc::channel::<BatcherMsg>(256);
    let (batches_tx, batches_rx) = async_channel::bounded::<InsertBatch>((n * 2).max(4));

    let inserters = inserter::spawn_pool(
        n,
        emitter,
        batches_rx,
        ack.clone(),
        stats.clone(),
        fatal.clone(),
        config_rx.clone(),
    )
    .await?;

    // Boot fallback for the batcher; the live path re-reads from `config_rx`.
    let flush_timeout = if emitter.flush_timeout.is_zero() {
        DEFAULT_PIPELINE_FLUSH
    } else {
        emitter.flush_timeout
    };
    let batcher = batcher::spawn(
        msg_rx,
        batches_tx,
        BatcherConfig {
            row_budget: emitter.row_budget,
            byte_budget: emitter.byte_budget,
            flush_timeout,
        },
        Allocator::global(&mimalloc::MiMalloc),
        fatal,
        stats.clone(),
        config_rx,
    );

    Ok((
        msg_tx,
        ack,
        TailParts {
            collector,
            batcher,
            inserters,
        },
    ))
}
