//! Inserter pool — N `BoxedAsyncClient` connections sending sealed batches.
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

use clickhouse_c::{Allocator, BlockBuilder, ColumnBuilder, TypeAst};
use tokio::task::JoinHandle;

use crate::ch::{ChConn, EmitterError, drain_to_end_of_stream, with_timeout};
use crate::config::DestEmitter;
use crate::emit::ch_emitter::{ColumnBuf, EmitterStats, build_leaf, build_root};
use crate::emit::pipeline::Fatal;
use crate::emit::pipeline::ack::AckHandle;
use crate::emit::pipeline::batcher::BatchMeta;
use crate::emit::pipeline::resolver::ResolvedBatch;
use crate::schema::TableKey;
use ahash::{HashMap, HashMapExt};
use std::sync::Arc;
use std::sync::atomic::Ordering;

struct Inserter {
    client: ChConn,
    alloc: Allocator,
    /// Parsed column types per table, refreshed when a batch's `schema_epoch`
    /// changes. `TypeAst` is `Send` but not `Sync`, so each inserter parses
    /// its own.
    asts: HashMap<TableKey, (u64, Vec<TypeAst>)>,
    ack: AckHandle,
    stats: Arc<EmitterStats>,
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

    /// Bounded reconnect+retry around one prepared INSERT. Only `bb`
    /// (`Send + Sync`) and `self` cross the awaits, never a bare `&TypeAst`
    /// (`!Sync`), so the task future stays `Send`. The block is unchanged
    /// across retries, so a reconnect just resends.
    async fn send_with_retry(
        &mut self,
        sql: &str,
        bb: &BlockBuilder<'_>,
    ) -> Result<(), EmitterError> {
        let insert_timeout = self.client.config().insert_timeout;
        let started = std::time::Instant::now();
        let result = self
            .client
            .retry(
                |mut client| async move {
                    let result = with_timeout(insert_timeout, async {
                        client.send_query(sql, None).await?;
                        client.send_data(Some(bb)).await?;
                        client.send_data_end().await?;
                        // Only after EndOfStream returns are rows durable and ackable
                        drain_to_end_of_stream(&mut client).await
                    })
                    .await;
                    (client, result)
                },
                |_, _| {
                    self.stats.retries_attempted.fetch_add(1, Ordering::Relaxed);
                },
            )
            .await;
        self.stats
            .reconnects
            .fetch_add(self.client.take_dials(), Ordering::Relaxed);
        if result.is_ok() {
            self.stats
                .inserter_ch_nanos
                .fetch_add(started.elapsed().as_nanos() as u64, Ordering::Relaxed);
        }
        result
    }

    async fn run(mut self, rx: async_channel::Receiver<ResolvedBatch>, fatal: Fatal) {
        while let Ok(ResolvedBatch { batch, resolved }) = rx.recv().await {
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
            let result = 'send: {
                let encode_started = std::time::Instant::now();
                let leaves: Vec<Option<ColumnBuilder<'_>>> = match batch
                    .buffers
                    .iter()
                    .map(|buf| build_leaf(buf, batch.n_rows))
                    .collect::<Result<_, _>>()
                {
                    Ok(v) => v,
                    Err(e) => break 'send Err(e),
                };
                let roots: Vec<Option<ColumnBuilder<'_>>> = match batch
                    .buffers
                    .iter()
                    .zip(&leaves)
                    .map(|(buf, leaf)| match buf {
                        ColumnBuf::Oracle(_) => Ok(None),
                        _ => build_root(buf, leaf.as_ref(), batch.n_rows).map(Some),
                    })
                    .collect::<Result<_, _>>()
                {
                    Ok(v) => v,
                    Err(e) => break 'send Err(e),
                };
                let mut bb = BlockBuilder::new();
                let appended: Result<(), EmitterError> = batch
                    .meta
                    .columns
                    .iter()
                    .enumerate()
                    .try_for_each(|(i, col)| match &roots[i] {
                        Some(node) => bb
                            .append(&col.name, asts[i].view(), node)
                            .map_err(Into::into),
                        None => {
                            let decoded = resolved
                                .as_ref()
                                .and_then(|r| r.column(i as u32))
                                .ok_or_else(|| {
                                    EmitterError::Type(format!(
                                        "oracle answered no column for {}",
                                        col.name
                                    ))
                                })?;
                            bb.append_column(&col.name, asts[i].view(), decoded)
                                .map_err(Into::into)
                        }
                    });
                self.stats.inserter_encode_nanos.fetch_add(
                    encode_started.elapsed().as_nanos() as u64,
                    Ordering::Relaxed,
                );
                match appended {
                    Ok(()) => self.send_with_retry(&batch.meta.insert_sql, &bb).await,
                    Err(e) => Err(e),
                }
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

/// Connect `n` inserters and spawn drain loops
pub(crate) async fn spawn_pool(
    n: usize,
    dest: Arc<DestEmitter>,
    rx: async_channel::Receiver<ResolvedBatch>,
    ack: AckHandle,
    stats: Arc<EmitterStats>,
    fatal: Fatal,
) -> Result<Vec<JoinHandle<()>>, EmitterError> {
    let mut handles = Vec::with_capacity(n.max(1));
    for _ in 0..n.max(1) {
        let inserter = Inserter {
            client: ChConn::connect(dest.clone()).await?,
            alloc: Allocator::global(&mimalloc::MiMalloc),
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::emit::pipeline::ack;

    #[tokio::test]
    async fn insert_retry_preserves_payload_through_failed_reconnect() {
        let sql = "INSERT INTO retry_test (x) VALUES";
        for (retries, idle) in [(0, false), (1, false), (2, false), (1, true)] {
            let (config, server) =
                crate::ch::test_support::retry_server(retries, sql, true, idle).await;
            let dest = DestEmitter::new(Arc::new(config), None);
            let client = ChConn::connect(dest).await.unwrap();
            let (ack, collector) = ack::spawn(Arc::default(), Fatal::new());
            let stats = Arc::new(EmitterStats::default());
            let mut inserter = Inserter {
                client,
                alloc: Allocator::global(&mimalloc::MiMalloc),
                asts: HashMap::new(),
                ack,
                stats: stats.clone(),
            };
            let ast = TypeAst::parse("UInt8", inserter.alloc).unwrap();
            let column = ColumnBuilder::fixed(&[42], 1, 1).unwrap();
            let mut block = BlockBuilder::new();
            block.append("x", ast.view(), &column).unwrap();
            assert_eq!(
                inserter.send_with_retry(sql, &block).await.is_ok(),
                retries == 2 || idle
            );
            assert_eq!(
                stats.retries_attempted.load(Ordering::Relaxed),
                u64::from(retries)
            );
            assert_eq!(
                stats.reconnects.load(Ordering::Relaxed),
                u64::from(retries == 2 || idle)
            );
            server.await.unwrap();
            drop(inserter);
            collector.await.unwrap();
        }
    }
}
