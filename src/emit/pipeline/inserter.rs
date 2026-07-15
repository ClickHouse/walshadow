//! Inserter pool — N `AsyncClient` connections sending sealed batches.
//!
//! ClickHouse Cloud INSERT cost is mostly RTT + object-store part commit, so
//! throughput comes from keeping many INSERTs in flight. Each inserter pulls
//! `InsertBatch`es off the shared spmc queue (any idle inserter takes any
//! batch, so a hot table can use more than one connection), rebuilds the
//! Native block over the batch's owned slabs, and runs one `send_query` +
//! `send_data` + `send_data_end` + drain-to-`EndOfStream` INSERT.
//!
//! Durability invariant: [`AckHandle::acked`] fires **only after** the drain
//! returns. Until then a connection drop replays the still-owned batch (CH
//! dedups by `_lsn`). Retry-exhaustion is fatal: the watermark can't advance
//! without this batch.

use std::collections::HashMap;

use clickhouse_c::{Allocator, AsyncClient, BlockBuilder, TypeAst};
use tokio::task::JoinHandle;

use crate::ch::{
    EmitterError, backoff_step, connect_client, drain_to_end_of_stream, is_retryable,
    reconnect_if_idle, with_timeout,
};
use crate::config::ResolvedConfig;
use crate::emit::ch_emitter::{EmitterConfig, EmitterStats, append_buf};
use crate::emit::pipeline::Fatal;
use crate::emit::pipeline::ack::AckHandle;
use crate::emit::pipeline::batcher::{BatchMeta, InsertBatch};
use crate::schema::RelName;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use tokio::sync::watch;

struct Inserter {
    client: AsyncClient,
    last_used: std::time::Instant,
    alloc: Allocator,
    config: EmitterConfig,
    /// Parsed column types per table, refreshed when a batch's `schema_epoch`
    /// changes. `TypeAst` is `Send` but not `Sync`, so each inserter parses
    /// its own.
    asts: HashMap<RelName, (u64, Vec<TypeAst>)>,
    ack: AckHandle,
    stats: Arc<EmitterStats>,
    /// Live emitter knobs. `Some` with the overlay active: retry budget +
    /// compression are re-read at each batch boundary (a compression change
    /// reconnects, since the codec is fixed at connect).
    config_rx: Option<watch::Receiver<Arc<ResolvedConfig>>>,
}

impl Inserter {
    fn ensure_asts(&mut self, meta: &BatchMeta) -> Result<(), EmitterError> {
        let fresh = self
            .asts
            .get(&meta.table_key)
            .is_none_or(|(epoch, _)| *epoch != meta.schema_epoch);
        if fresh {
            let mut parsed = Vec::with_capacity(meta.columns.len());
            for col in &meta.columns {
                parsed.push(TypeAst::parse(&col.type_repr, self.alloc)?);
            }
            self.asts
                .insert(meta.table_key.clone(), (meta.schema_epoch, parsed));
        }
        Ok(())
    }

    async fn reconnect(&mut self) -> Result<(), EmitterError> {
        self.client = connect_client(&self.config).await?;
        self.stats.reconnects.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Bounded reconnect+retry around one prepared INSERT. Only `bb`
    /// (`Send + Sync`) and `self` cross the awaits, never a bare `&TypeAst`
    /// (`!Sync`), so the task future stays `Send`. The block is unchanged
    /// across retries, so a reconnect just resends.
    async fn send_with_retry(
        &mut self,
        sql: &str,
        bb: &BlockBuilder<'_>,
    ) -> Result<(), EmitterError> {
        let retry = self.config.retry.clone();
        let mut attempt = 0u32;
        let mut backoff = retry.initial_backoff;
        reconnect_if_idle(&mut self.client, &self.config, self.last_used).await?;
        loop {
            let attempt_result = with_timeout(self.config.insert_timeout, async {
                self.client.send_query(sql, None).await?;
                self.client.send_data(Some(bb)).await?;
                self.client.send_data_end().await?;
                // Only after EndOfStream returns are rows durable and ackable
                drain_to_end_of_stream(&mut self.client).await
            })
            .await;
            match attempt_result {
                Ok(()) => {
                    self.last_used = std::time::Instant::now();
                    return Ok(());
                }
                Err(e) if is_retryable(&e) && attempt < retry.max_attempts => {
                    self.stats.retries_attempted.fetch_add(1, Ordering::Relaxed);
                    attempt += 1;
                    backoff_step(&mut backoff, retry.max_backoff).await;
                    self.reconnect().await?;
                }
                Err(e) => return Err(e),
            }
        }
    }

    async fn run(mut self, rx: async_channel::Receiver<InsertBatch>, fatal: Fatal) {
        while let Ok(batch) = rx.recv().await {
            // Live emitter knobs (overlay active): pick up the retry budget and
            // compression. A compression change needs a fresh client — the codec
            // is fixed at connect — reconnected here at a batch boundary, never
            // mid-INSERT. Snapshot into owned values first so no watch borrow is
            // held across the reconnect's `&mut self`.
            let live = self.config_rx.as_ref().map(|rx| {
                let r = rx.borrow();
                (r.retry_max_attempts, r.compression)
            });
            if let Some((retry_max, compression)) = live {
                self.config.retry.max_attempts = retry_max;
                if compression != self.config.compression {
                    self.config.compression = compression;
                    if let Err(e) = self.reconnect().await {
                        fatal.set(format!("inserter compression reconnect: {e}"));
                        break;
                    }
                }
            }
            if let Err(e) = self.ensure_asts(&batch.meta) {
                fatal.set(format!("inserter type parse: {e}"));
                break;
            }
            // Own the asts (`Vec<TypeAst>` is `Send`); index inline so no
            // `&[TypeAst]` binding lives across the send await
            let (epoch, asts) = self
                .asts
                .remove(&batch.meta.table_key)
                .expect("ensure_asts inserted");
            let result = match BlockBuilder::new(self.alloc) {
                Ok(mut bb) => {
                    let mut build_err = None;
                    for (i, col) in batch.meta.columns.iter().enumerate() {
                        if let Err(e) = append_buf(
                            &mut bb,
                            &col.name,
                            &asts[i],
                            &batch.buffers[i],
                            batch.n_rows,
                        ) {
                            build_err = Some(e);
                            break;
                        }
                    }
                    if let Some(e) = build_err {
                        Err(e)
                    } else {
                        self.send_with_retry(&batch.meta.insert_sql, &bb).await
                    }
                }
                Err(e) => Err(e.into()),
            };
            self.asts
                .insert(batch.meta.table_key.clone(), (epoch, asts));
            match result {
                Ok(()) => {
                    self.stats
                        .rows_emitted
                        .fetch_add(batch.n_rows as u64, Ordering::Relaxed);
                    self.stats.blocks_sent.fetch_add(1, Ordering::Relaxed);
                    self.stats
                        .inserter_batches_in
                        .fetch_add(1, Ordering::Relaxed);
                    self.ack.acked(batch.per_seq);
                }
                Err(e) => {
                    fatal.set(format!("inserter: {e}"));
                    break;
                }
            }
        }
    }
}

/// Connect `n` inserters and spawn their drain loops; a connect failure
/// aborts pool startup.
pub(crate) async fn spawn_pool(
    n: usize,
    config: &EmitterConfig,
    rx: async_channel::Receiver<InsertBatch>,
    ack: AckHandle,
    stats: Arc<EmitterStats>,
    fatal: Fatal,
    config_rx: Option<watch::Receiver<Arc<ResolvedConfig>>>,
) -> Result<Vec<JoinHandle<()>>, EmitterError> {
    let mut handles = Vec::with_capacity(n.max(1));
    for _ in 0..n.max(1) {
        let client = connect_client(config).await?;
        let inserter = Inserter {
            client,
            last_used: std::time::Instant::now(),
            alloc: Allocator::global(&mimalloc::MiMalloc),
            config: config.clone(),
            asts: HashMap::new(),
            ack: ack.clone(),
            stats: stats.clone(),
            config_rx: config_rx.clone(),
        };
        let rx = rx.clone();
        let fatal = fatal.clone();
        handles.push(tokio::spawn(inserter.run(rx, fatal)));
    }
    Ok(handles)
}
