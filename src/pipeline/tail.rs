//! Reusable insert tail: batcher + inserter pool + ack collector.
//!
//! Both producers feed this identical tail: the WAL pipeline
//! ([`PipelineConfig::spawn`](crate::pipeline::PipelineConfig::spawn),
//! via reorder + decode pool) and greenfield bootstrap
//! ([`bootstrap::drain`](crate::pipeline::bootstrap), via the page walk).
//! The producer differs; the tail is the same — one shipping path so
//! bootstrap inherits the N-connection inserter pool, reconnect + retry,
//! the durable watermark, and backpressure for free.
//!
//! Fed by a `mpsc::Sender<BatcherMsg>` (rows + barrier `FlushAll`) and an
//! [`AckHandle`]. Drains in cascade once every `BatcherMsg` sender drops:
//! the batcher final-flushes and exits → inserters drain to `EndOfStream`
//! and exit → the ack collector exits.

use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use clickhouse_c::Allocator;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

use crate::ch_emitter::{EmitterConfig, EmitterError, EmitterStats};
use crate::pipeline::ack::{self, AckHandle};
use crate::pipeline::batcher::{self, BatcherConfig, BatcherMsg, InsertBatch};
use crate::pipeline::inserter;
use crate::pipeline::{DEFAULT_PIPELINE_FLUSH, Fatal, mpmc};

/// Spawned tail stages. [`Self::join`] awaits the drain cascade: the
/// batcher exits once every `BatcherMsg` sender drops (final flush),
/// inserters drain to `EndOfStream` and exit, then the ack collector
/// exits. Holding this keeps the tail tasks owned by the caller.
pub struct TailParts {
    collector: JoinHandle<()>,
    batcher: JoinHandle<()>,
    inserters: Vec<JoinHandle<()>>,
}

impl TailParts {
    /// Await the drain cascade. Call after every producer-held `msg_tx`
    /// and `AckHandle` clone has dropped (else the batcher never sees its
    /// channel close and this hangs).
    pub async fn join(self) {
        let _ = self.batcher.await;
        for h in self.inserters {
            let _ = h.await;
        }
        let _ = self.collector.await;
    }

    /// Bootstrap completion + teardown: seal partial batches, wait every seq
    /// < `through` durable on CH, then drain the tail. Consumes producer
    /// handles so drop-before-join ordering can't be gotten wrong. `fatal`
    /// short-circuits a CH outage instead of hanging
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

/// Stand up the tail: ack collector, inserter pool (`n` connections), and
/// the batcher. Returns the `BatcherMsg` sender (clone into producers),
/// the [`AckHandle`] (clone into producers), and the join handles. The
/// `fatal` is shared with the producer stages so an encode/insert failure
/// anywhere trips one signal. Fails only if an inserter connection can't
/// open — inserters spin up first (consume-only) so a connect failure
/// aborts before any other stage starts.
pub async fn spawn(
    emitter: &EmitterConfig,
    inserter_pool_size: usize,
    stats: Arc<EmitterStats>,
    emitter_ack: Arc<AtomicU64>,
    fatal: Fatal,
) -> Result<(mpsc::Sender<BatcherMsg>, AckHandle, TailParts), EmitterError> {
    let n = inserter_pool_size.max(1);

    let (ack, collector) = ack::spawn(emitter_ack);

    // Rows (decode pool / bootstrap drain) and FlushAll (barrier /
    // bootstrap completion) share one FIFO channel so a flush can't
    // overtake rows enqueued before it.
    let (msg_tx, msg_rx) = mpsc::channel::<BatcherMsg>(256);
    let (batches_tx, batches_rx) = mpmc::channel::<InsertBatch>((n * 2).max(4));

    let inserters = inserter::spawn_pool(
        n,
        emitter,
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
        BatcherConfig {
            row_budget: emitter.row_budget,
            byte_budget: emitter.byte_budget,
            flush_timeout,
        },
        Allocator::stdlib(),
        fatal,
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
