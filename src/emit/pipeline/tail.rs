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

use tokio::sync::{mpsc, oneshot, watch};
use tokio_util::task::AbortOnDropHandle;

use crate::ch::EmitterError;
use crate::config::DestEmitter;
use crate::config::ResolvedConfig;
use crate::emit::ch_emitter::EmitterStats;
use crate::emit::pipeline::Fatal;
use crate::emit::pipeline::ack::{self, AckHandle};
use crate::emit::pipeline::batcher::{self, BatcherConfig, BatcherMsg, InsertBatch};
use crate::emit::pipeline::encode::EncodedRun;
use crate::emit::pipeline::inserter;
use crate::emit::pipeline::resolver::{self, ResolvedBatch};
use crate::pos::{EmitterAck, Monotone};
use foldhash::{HashMap, HashMapExt};

/// Spawned tail stages; holding this keeps the tasks owned by the caller.
pub struct TailParts {
    collector: AbortOnDropHandle<()>,
    batcher: AbortOnDropHandle<()>,
    resolvers: Vec<AbortOnDropHandle<()>>,
    inserters: Vec<AbortOnDropHandle<()>>,
}

impl TailParts {
    /// Drop the spawn-returned producer handles, then await the drain
    /// cascade. Hangs while any clone handed to a producer stays alive
    pub async fn close(self, msg_tx: mpsc::Sender<BatcherMsg>, ack: AckHandle) {
        drop(msg_tx);
        drop(ack);
        self.join().await;
    }

    /// Await the drain cascade once every producer handle has dropped
    pub(super) async fn join(self) {
        let _ = self.batcher.await;
        for h in self.resolvers {
            let _ = h.await;
        }
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
        flush_and_prove(&msg_tx, &ack, through, fatal).await?;
        self.close(msg_tx, ack).await;
        fatal.message().map_or(Ok(()), Err)
    }
}

async fn flush_and_prove(
    msg_tx: &mpsc::Sender<BatcherMsg>,
    ack: &AckHandle,
    through: u64,
    fatal: &Fatal,
) -> Result<(), String> {
    let (reply_tx, reply_rx) = oneshot::channel();
    if msg_tx.send(BatcherMsg::FlushAll(reply_tx)).await.is_err() {
        return Err(fatal
            .message()
            .unwrap_or_else(|| "tail closed before flush".into()));
    }
    // Prefer concurrent fatal over successful completion
    tokio::select! {
        biased;
        msg = fatal.wait() => return Err(msg),
        r = reply_rx => r.map_err(|_| "batcher dropped flush ack".to_string())?,
    }
    tokio::select! {
        biased;
        msg = fatal.wait() => return Err(msg),
        r = ack.wait_through(through) => r.map_err(|e| format!("tail drain: {e}"))?,
    }
    Ok(())
}

/// Tail plus the producer handles a single bootstrap leg holds
///
/// The leg owns its seq space, so the durable watermark is throwaway:
/// completion is `wait_through(next_seq)`, not a resume position
pub struct OwnedTail {
    pub msg_tx: mpsc::Sender<BatcherMsg>,
    pub ack: AckHandle,
    parts: TailParts,
    fatal: Fatal,
    /// Prefix for the errors this leg reports
    context: &'static str,
}

impl OwnedTail {
    #[cfg(test)]
    pub(crate) fn null() -> Self {
        let fatal = Fatal::new();
        let (msg_tx, ack, parts) = spawn_null(Arc::new(Monotone::default()), fatal.clone());
        Self {
            msg_tx,
            ack,
            parts,
            fatal,
            context: "test",
        }
    }

    pub async fn spawn(
        dest: Arc<DestEmitter>,
        inserter_pool_size: usize,
        stats: Arc<EmitterStats>,
        fatal: Fatal,
        config_rx: Option<watch::Receiver<Arc<ResolvedConfig>>>,
        oracle: Option<Arc<crate::ops::oracle::Oracle>>,
        context: &'static str,
    ) -> Result<Self, String> {
        let (msg_tx, ack, parts) = spawn_with_config(
            dest,
            inserter_pool_size,
            stats,
            Arc::new(Monotone::<EmitterAck>::default()),
            fatal.clone(),
            config_rx,
            oracle,
        )
        .await
        .map_err(|e| format!("{context}: spawn insert tail: {e}"))?;
        Ok(Self {
            msg_tx,
            ack,
            parts,
            fatal,
            context,
        })
    }

    /// [`Self::finish`] without closing the tail: what licenses recording
    /// resume progress mid-pass
    pub async fn checkpoint(&self, through: u64) -> Result<(), String> {
        flush_and_prove(&self.msg_tx, &self.ack, through, &self.fatal)
            .await
            .map_err(|m| format!("{}: {m}", self.context))
    }

    /// Seal batches and prove every seq below `through` durable
    pub async fn finish(self, through: u64) -> Result<(), String> {
        self.parts
            .finish(self.msg_tx, self.ack, through, &self.fatal)
            .await
            .map_err(|m| format!("{}: {m}", self.context))
    }

    /// Failed-leg teardown: dropping the last producer handles closes the
    /// batcher, which final-flushes and cascades the inserters + collector
    /// down. Bounded by the inserters' retry policy (a CH outage trips their
    /// fatal, not a hang)
    pub async fn quiesce(self) {
        self.parts.close(self.msg_tx, self.ack).await;
    }
}

/// Stand up the tail: ack collector, inserter pool (`n` connections),
/// batcher. Returns the `BatcherMsg` sender + [`AckHandle`] (clone into
/// producers) and join handles. Fails only if an inserter connection can't
/// open — inserters spin up first (consume-only) so a connect failure aborts
/// before any other stage starts.
pub async fn spawn(
    dest: Arc<DestEmitter>,
    inserter_pool_size: usize,
    stats: Arc<EmitterStats>,
    emitter_ack: Arc<Monotone<EmitterAck>>,
    fatal: Fatal,
) -> Result<(mpsc::Sender<BatcherMsg>, AckHandle, TailParts), EmitterError> {
    spawn_with_config(
        dest,
        inserter_pool_size,
        stats,
        emitter_ack,
        fatal,
        None,
        None,
    )
    .await
}

/// Ack every row of `segments` as done, one count per seq
fn swallow_segments(ack: &AckHandle, segments: Vec<batcher::PlacedSegment>) {
    let mut counts: HashMap<u64, u64> = HashMap::new();
    for s in segments {
        let rows = s.chunks.iter().map(|c| c.rows() as u64).sum::<u64>();
        if rows > 0 {
            *counts.entry(s.seq).or_insert(0) += rows;
        }
    }
    ack.swallowed(counts.into_iter().collect());
}

/// Ack every row an encode job produced
fn swallow_run(ack: &AckHandle, run: EncodedRun) {
    let mut counts: HashMap<u64, u64> = HashMap::new();
    for (seq, rows) in run.chunks.into_iter().flat_map(|(_, seqs)| seqs) {
        *counts.entry(seq).or_insert(0) += rows;
    }
    ack.swallowed(counts.into_iter().collect());
}

/// Metrics-only tail: ack collector + one swallow task, zero CH
/// connections. Every routed row acks at swallow (permits release on
/// drop) so nothing can pin the watermark; `FlushAll` replies
/// immediately — nothing buffers. Keeps the placed/acked protocol
/// identical to the CH tail, so reorder/decode stages run unchanged.
pub fn spawn_null(
    emitter_ack: Arc<Monotone<EmitterAck>>,
    fatal: Fatal,
) -> (mpsc::Sender<BatcherMsg>, AckHandle, TailParts) {
    let (ack, collector) = ack::spawn(emitter_ack, fatal.clone());
    let collector = AbortOnDropHandle::new(collector);
    let (msg_tx, mut msg_rx) = mpsc::channel::<BatcherMsg>(256);
    let swallow_ack = ack.clone();
    let batcher = tokio::spawn(async move {
        while let Some(msg) = msg_rx.recv().await {
            match msg {
                BatcherMsg::Segments(segments) => swallow_segments(&swallow_ack, segments),
                BatcherMsg::Encoded(job) => match job.await {
                    Ok(Ok(run)) => swallow_run(&swallow_ack, run),
                    Ok(Err(e)) => return fatal.set(format!("encode: {e}")),
                    Err(e) => return fatal.set(format!("encode job: {e}")),
                },
                BatcherMsg::Load(slab) => swallow_ack.swallowed(slab.seq_rows()),
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
            batcher: AbortOnDropHandle::new(batcher),
            resolvers: Vec::new(),
            inserters: Vec::new(),
        },
    )
}

/// Batcher whose sealed batches drop on arrival, acked as inserted.
/// Measures producers and batching without ClickHouse
#[cfg(any(test, feature = "test-support"))]
pub fn spawn_discard(
    cfg: BatcherConfig,
    stats: Arc<EmitterStats>,
    fatal: Fatal,
) -> (mpsc::Sender<BatcherMsg>, AckHandle, TailParts) {
    let (ack, collector) = ack::spawn(Arc::new(Monotone::default()), fatal.clone());
    let (msg_tx, msg_rx) = mpsc::channel::<BatcherMsg>(256);
    let (batches_tx, batches_rx) = async_channel::bounded::<InsertBatch>(4);
    let budget = resolver::output_pool(
        cfg.byte_budget,
        1,
        false,
        stats.output_resident_bytes.clone(),
    );
    let batcher = batcher::spawn(msg_rx, batches_tx, cfg, budget, fatal, stats, None);
    let discard_ack = ack.clone();
    let discard = tokio::spawn(async move {
        while let Ok(batch) = batches_rx.recv().await {
            discard_ack.swallowed(batch.per_seq.clone());
        }
    });
    (
        msg_tx,
        ack,
        TailParts {
            collector: AbortOnDropHandle::new(collector),
            batcher: AbortOnDropHandle::new(batcher),
            resolvers: Vec::new(),
            inserters: vec![AbortOnDropHandle::new(discard)],
        },
    )
}

/// [`spawn`] plus a live config receiver: the batcher re-reads
/// budgets/flush and the inserter pool re-reads compression/retry from it on
/// each republish. `None` == boot values only (bootstrap + tests use [`spawn`]).
pub async fn spawn_with_config(
    dest: Arc<DestEmitter>,
    inserter_pool_size: usize,
    stats: Arc<EmitterStats>,
    emitter_ack: Arc<Monotone<EmitterAck>>,
    fatal: Fatal,
    config_rx: Option<watch::Receiver<Arc<ResolvedConfig>>>,
    oracle: Option<Arc<crate::ops::oracle::Oracle>>,
) -> Result<(mpsc::Sender<BatcherMsg>, AckHandle, TailParts), EmitterError> {
    let n = inserter_pool_size.max(1);

    let (ack, collector) = ack::spawn(emitter_ack, fatal.clone());
    let collector = AbortOnDropHandle::new(collector);

    // Rows and FlushAll share one FIFO channel so a flush can't overtake rows
    // enqueued before it
    let (msg_tx, msg_rx) = mpsc::channel::<BatcherMsg>(256);
    let (batches_tx, batches_rx) = async_channel::bounded::<InsertBatch>((n * 2).max(4));
    // One resolved batch per inserter is what keeps every one of them fed
    let (resolved_tx, resolved_rx) = async_channel::bounded::<ResolvedBatch>(n);

    let emitter = dest.current();
    let inserters = inserter::spawn_pool(
        n,
        dest,
        resolved_rx,
        ack.clone(),
        stats.clone(),
        fatal.clone(),
    )
    .await?;

    // As wide as the bridge: a narrower pool leaves shadow workers idle, a
    // wider one only queues on their sockets
    let width = oracle.as_ref().map_or(1, |o| o.concurrency());
    let budget = resolver::output_pool(
        emitter.byte_budget,
        width,
        oracle.is_some(),
        stats.output_resident_bytes.clone(),
    );
    let resolvers = resolver::spawn_pool(
        width,
        batches_rx,
        resolved_tx,
        resolver::ResolverOptions {
            oracle,
            retry: emitter.retry.clone(),
            codec: emitter.compression,
            stats: stats.clone(),
            fatal: fatal.clone(),
            config_rx: config_rx.clone(),
            budget: budget.clone(),
        },
    );

    let batcher = batcher::spawn(
        msg_rx,
        batches_tx,
        BatcherConfig {
            row_budget: emitter.row_budget,
            byte_budget: emitter.byte_budget,
            inserters: n,
            flush_timeout: emitter.flush_timeout,
        },
        budget,
        fatal,
        stats.clone(),
        config_rx,
    );

    Ok((
        msg_tx,
        ack,
        TailParts {
            collector,
            batcher: AbortOnDropHandle::new(batcher),
            resolvers: resolvers.into_iter().map(AbortOnDropHandle::new).collect(),
            inserters: inserters.into_iter().map(AbortOnDropHandle::new).collect(),
        },
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::emit::pipeline::bootstrap::Slab;

    /// One routed row under `seq`, opened and placed on `ack`
    fn placed_row(ack: &AckHandle, seq: u64) -> batcher::PlacedSegment {
        let batcher::RowTable { rel, route } = resolver::tests::wkt_table();
        let (_, row) = resolver::tests::wkt_row(seq, Some((1.0, 2.0)));
        let mut seqs = ack.seqs(seq);
        let open = seqs.open(row.commit_lsn, Some(row.commit_ts), ack::Publish::Commit);
        seqs.place(open, 1);
        batcher::PlacedSegment::of_rows(seq, &rel, &route, &[row]).unwrap()
    }

    #[tokio::test]
    async fn dropped_tail_closes_workers_with_live_producers() {
        let (tx, ack, parts) = spawn_null(Arc::new(Monotone::default()), Fatal::new());
        drop(parts);
        tokio::time::timeout(std::time::Duration::from_secs(1), tx.closed())
            .await
            .unwrap();
        drop(ack);
    }

    #[tokio::test]
    async fn closed_batcher_fails_flush_with_fallback_reason() {
        let tail = OwnedTail::null();
        let (msg_tx, msg_rx) = mpsc::channel(1);
        drop(msg_rx);
        let e = flush_and_prove(&msg_tx, &tail.ack, 0, &tail.fatal).await;
        assert_eq!(e, Err("tail closed before flush".into()));
        tail.quiesce().await;
    }

    #[tokio::test]
    async fn fatal_fails_checkpoint_and_finish_under_leg_context() {
        let tail = OwnedTail::null();
        tail.fatal.set("ch down".into());
        assert_eq!(tail.checkpoint(0).await, Err("test: ch down".into()));
        assert_eq!(tail.finish(0).await, Err("test: ch down".into()));
    }

    #[tokio::test]
    async fn unreachable_destination_fails_spawn_under_leg_context() {
        let cfg = crate::emit::ch_emitter::EmitterConfig {
            conn: crate::emit::ch_emitter::InstanceConfig {
                host: "127.0.0.1".into(),
                port: 1,
                ..Default::default()
            },
            retry: crate::emit::ch_emitter::RetryConfig {
                max_attempts: 0,
                ..Default::default()
            },
            ..Default::default()
        };
        let dest = DestEmitter::new(Arc::new(cfg), None);
        let spawned =
            OwnedTail::spawn(dest, 1, Arc::default(), Fatal::new(), None, None, "leg").await;
        let e = spawned.err().expect("dead port fails spawn");
        assert!(e.starts_with("leg: spawn insert tail: "), "{e}");
    }

    #[tokio::test]
    async fn null_tail_swallows_encoded_runs() {
        let fatal = Fatal::new();
        let (tx, ack, parts) = spawn_null(Arc::new(Monotone::default()), fatal.clone());
        let chunks = placed_row(&ack, 0).chunks.into_iter();
        let run = EncodedRun {
            chunks: chunks.map(|c| (c, vec![(0, 1)])).collect(),
            permit: None,
        };
        tx.send(BatcherMsg::Encoded(tokio::spawn(async { Ok(run) })))
            .await
            .unwrap();
        parts.finish(tx, ack, 1, &fatal).await.unwrap();
    }

    #[tokio::test]
    async fn null_tail_swallows_segments() {
        let fatal = Fatal::new();
        let (tx, ack, parts) = spawn_null(Arc::new(Monotone::default()), fatal.clone());
        let segment = placed_row(&ack, 0);
        tx.send(BatcherMsg::Segments(vec![segment])).await.unwrap();
        parts.finish(tx, ack, 1, &fatal).await.unwrap();
    }

    #[tokio::test]
    async fn null_tail_swallows_load_rows() {
        let fatal = Fatal::new();
        let (tx, ack, parts) = spawn_null(Arc::new(Monotone::default()), fatal.clone());
        let open = ack.seqs(0).open(1, None, ack::Publish::Commit);
        ack.place(open, 2);
        let slab = Slab::of_cells(0, &resolver::tests::wkt_table(), &[vec![None], vec![None]]);
        tx.send(BatcherMsg::Load(slab)).await.unwrap();
        parts.finish(tx, ack, 1, &fatal).await.unwrap();
    }

    /// A failed encode job stops the null tail, so a later flush reports the
    /// job's error rather than a closed channel
    #[tokio::test]
    async fn null_tail_fails_on_encode_errors() {
        let failed = || tokio::spawn(async { Err("boom".to_string()) });
        let panicked = || tokio::spawn(async { panic!("encoder panicked") });
        for (job, want) in [(failed(), "encode: boom"), (panicked(), "encode job: ")] {
            let fatal = Fatal::new();
            let (tx, ack, parts) = spawn_null(Arc::new(Monotone::default()), fatal.clone());
            tx.send(BatcherMsg::Encoded(job)).await.unwrap();
            tx.closed().await;
            let err = parts.finish(tx, ack, 0, &fatal).await.unwrap_err();
            assert!(err.starts_with(want), "{err}");
        }
    }

    #[tokio::test]
    async fn discard_tail_finishes_without_clickhouse() {
        let fatal = Fatal::new();
        let cfg = BatcherConfig {
            row_budget: 1024,
            byte_budget: 1 << 20,
            inserters: 1,
            flush_timeout: std::time::Duration::from_secs(1),
        };
        let (tx, ack, parts) = spawn_discard(cfg, Arc::default(), fatal.clone());
        let segment = placed_row(&ack, 0);
        tx.send(BatcherMsg::Segments(vec![segment])).await.unwrap();
        parts.finish(tx, ack, 1, &fatal).await.unwrap();
        assert!(!fatal.is_set());
    }
}
