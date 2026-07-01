//! Inserter pool — N `AsyncClient` connections sending sealed batches.
//!
//! ClickHouse Cloud INSERT cost is mostly RTT + object-store part commit, so
//! throughput comes from keeping many INSERTs in flight. Each inserter pulls
//! [`InsertBatch`]es off the shared spmc queue (any idle inserter takes any
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

use crate::ch_emitter::{
    EmitterConfig, EmitterError, EmitterStats, append_buf, connect_client, drain_to_end_of_stream,
    is_retryable,
};
use crate::pipeline::ack::AckHandle;
use crate::pipeline::batcher::{BatchMeta, InsertBatch};
use crate::pipeline::{Fatal, mpmc};
use std::sync::Arc;
use std::sync::atomic::Ordering;

struct Inserter {
    client: AsyncClient,
    alloc: Allocator,
    config: EmitterConfig,
    /// Parsed column types per table, refreshed when a batch's `schema_epoch`
    /// changes. `TypeAst` is `Send` but not `Sync`, so each inserter parses
    /// its own.
    asts: HashMap<String, (u64, Vec<TypeAst>)>,
    ack: AckHandle,
    stats: Arc<EmitterStats>,
}

impl Inserter {
    fn ensure_asts(&mut self, meta: &BatchMeta) -> Result<(), EmitterError> {
        let fresh = match self.asts.get(&meta.table_key) {
            Some((epoch, _)) => *epoch != meta.schema_epoch,
            None => true,
        };
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
        loop {
            let attempt_result = match tokio::time::timeout(self.config.insert_timeout, async {
                self.client.send_query(sql, None).await?;
                self.client.send_data(Some(bb)).await?;
                self.client.send_data_end().await?;
                // Only after EndOfStream returns are rows durable and ackable
                drain_to_end_of_stream(&mut self.client).await
            })
            .await
            {
                Ok(r) => r,
                // A connection wedged mid-INSERT must not pin the watermark:
                // surface a retryable timeout so the retry arm reconnects and
                // resends on a fresh socket. CH dedups the resend by `_lsn`.
                Err(_elapsed) => Err(EmitterError::Timeout {
                    secs: self.config.insert_timeout.as_secs(),
                }),
            };
            match attempt_result {
                Ok(()) => return Ok(()),
                Err(e) if is_retryable(&e) && attempt < retry.max_attempts => {
                    self.stats.retries_attempted.fetch_add(1, Ordering::Relaxed);
                    attempt += 1;
                    tokio::time::sleep(backoff).await;
                    backoff = backoff.saturating_mul(2).min(retry.max_backoff);
                    self.reconnect().await?;
                }
                Err(e) => return Err(e),
            }
        }
    }

    async fn run(mut self, rx: mpmc::Receiver<InsertBatch>, fatal: Fatal) {
        while let Some(batch) = rx.recv().await {
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
                    match build_err {
                        Some(e) => Err(e),
                        None => self.send_with_retry(&batch.meta.insert_sql, &bb).await,
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
pub async fn spawn_pool(
    n: usize,
    config: &EmitterConfig,
    rx: mpmc::Receiver<InsertBatch>,
    ack: AckHandle,
    stats: Arc<EmitterStats>,
    fatal: Fatal,
) -> Result<Vec<JoinHandle<()>>, EmitterError> {
    let mut handles = Vec::with_capacity(n.max(1));
    for _ in 0..n.max(1) {
        let client = connect_client(config).await?;
        let inserter = Inserter {
            client,
            alloc: Allocator::global(&mimalloc::MiMalloc),
            config: config.clone(),
            asts: HashMap::new(),
            ack: ack.clone(),
            stats: stats.clone(),
        };
        let rx = rx.clone();
        let fatal = fatal.clone();
        handles.push(tokio::spawn(inserter.run(rx, fatal)));
    }
    Ok(handles)
}
