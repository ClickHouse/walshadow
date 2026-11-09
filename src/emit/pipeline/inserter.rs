//! Inserter pool — N `BoxedAsyncClient` connections sending sealed batches.
//!
//! ClickHouse Cloud INSERT cost is mostly RTT + object-store part commit, so
//! throughput comes from keeping many INSERTs in flight. Each inserter pulls
//! `InsertBatch`es off the shared spmc queue (any idle inserter takes any
//! batch, so a hot table can use more than one connection), and runs one
//! `send_query`, a prepared Data packet per block, `send_data_end` and
//! drain-to-`EndOfStream` INSERT.
//!
//! Durability invariant: [`AckHandle::acked`] takes the drain's
//! [`EndOfStream`] proof. Until then a connection drop replays the still-owned batch (CH
//! dedups by `_lsn`). Retry-exhaustion is fatal: the watermark can't advance
//! without this batch.

use clickhouse_c::QueryOpts;
use tokio::task::JoinHandle;

use crate::ch::{ChConn, EmitterError, EndOfStream, drain_to_end_of_stream, with_timeout};
use crate::config::DestEmitter;
use crate::emit::ch_emitter::EmitterStats;
use crate::emit::pipeline::Fatal;
use crate::emit::pipeline::ack::AckHandle;
use crate::emit::pipeline::batcher::BatchMeta;
use crate::emit::pipeline::resolver::ResolvedBatch;
use crate::emit::pipeline::sealed::SealedBlock;
use std::sync::Arc;
use std::sync::atomic::Ordering;

struct Inserter {
    client: ChConn,
    ack: AckHandle,
    stats: Arc<EmitterStats>,
}

impl Inserter {
    /// Bounded reconnect+retry around one INSERT. Every attempt resends
    /// every block in order, so a replay carries unchanged rows
    async fn send_with_retry(
        &mut self,
        sql: &str,
        blocks: &[SealedBlock],
    ) -> Result<EndOfStream, EmitterError> {
        let insert_timeout = self.client.config().insert_timeout;
        let started = std::time::Instant::now();
        let stats = &self.stats;
        let result = self
            .client
            .retry(
                |mut client| async move {
                    let result = with_timeout(insert_timeout, async {
                        client.send_query(sql, &QueryOpts::new()).await?;
                        for sealed in blocks {
                            client.send_prepared(sealed.prepared()).await?;
                        }
                        client.send_data_end().await?;
                        drain_to_end_of_stream(&mut client).await
                    })
                    .await;
                    (client, result)
                },
                |_, _| {
                    stats.retries_attempted.fetch_add(1, Ordering::Relaxed);
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

    /// Acknowledge after every destination receives batch. Failure can cause
    /// replay of entire batch, including rows already delivered
    async fn send_all(
        &mut self,
        meta: &BatchMeta,
        blocks: &[SealedBlock],
    ) -> Result<EndOfStream, EmitterError> {
        let mut durable = None;
        for query in &meta.inserts {
            self.client.select(query.instance.as_deref())?;
            durable = Some(self.send_with_retry(&query.sql, blocks).await?);
            #[cfg(feature = "test-support")]
            crate::crash_test::hit(crate::crash_test::Point::AfterInsert, u64::MAX);
        }
        durable.ok_or_else(|| EmitterError::Config("batch has no INSERT target".into()))
    }

    async fn run(mut self, rx: async_channel::Receiver<ResolvedBatch>, fatal: Fatal) {
        while let Ok(batch) = rx.recv().await {
            self.client.force_compression(batch.compression);
            match self.send_all(&batch.meta, &batch.blocks).await {
                Ok(durable) => {
                    self.stats
                        .rows_emitted
                        .fetch_add(batch.n_rows as u64, Ordering::Relaxed);
                    batch
                        .meta
                        .rows_inserted
                        .fetch_add(batch.n_rows as u64, Ordering::Relaxed);
                    for col in &batch.meta.columns {
                        col.cells_inserted
                            .fetch_add(batch.n_rows as u64, Ordering::Relaxed);
                    }
                    self.stats
                        .blocks_sent
                        .fetch_add(batch.blocks.len() as u64, Ordering::Relaxed);
                    self.stats
                        .inserter_batches_in
                        .fetch_add(1, Ordering::Relaxed);
                    self.ack.acked(durable, batch.per_seq);
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
    use crate::ch::CompressionChoice;
    use crate::emit::pipeline::ack;
    use clickhouse_c::{Allocator, BlockBuilder, ColumnBuilder, TypeAst};

    /// One uncompressed UInt8 column x holding one row 42
    fn one_row_block() -> SealedBlock {
        let alloc = Allocator::global(&mimalloc::MiMalloc);
        let ast = TypeAst::parse("UInt8", alloc).unwrap();
        let column = ColumnBuilder::fixed(&[42], 1, 1).unwrap();
        let mut bb = BlockBuilder::new();
        bb.append("x", ast.view(), &column).unwrap();
        SealedBlock::seal(&bb, CompressionChoice::None, None, alloc).unwrap()
    }

    /// Every attempt resends every block of the INSERT through failed and
    /// idle reconnects, and after a drop between Data blocks, dialing
    /// uncompressed blocks' compression over configured lz4
    #[tokio::test]
    async fn insert_retry_preserves_payload_through_failed_reconnect() {
        let sql = "INSERT INTO retry_test (x) VALUES";
        for (retries, idle, blocks, cut) in [
            (0, false, 1, None),
            (1, false, 1, None),
            (2, false, 1, None),
            (2, false, 3, None),
            (1, true, 2, None),
            (2, false, 3, Some(1)),
            (2, false, 3, Some(2)),
        ] {
            let (mut config, server) =
                crate::ch::test_support::retry_server(retries, sql, blocks, idle, cut).await;
            config.compression = CompressionChoice::Lz4;
            let dest = DestEmitter::new(Arc::new(config), None);
            let mut client = ChConn::deferred(dest);
            client.force_compression(CompressionChoice::None);
            client.dial().await.unwrap();
            client.take_dials();
            let (ack, collector) = ack::spawn(Arc::default(), Fatal::new());
            let stats = Arc::new(EmitterStats::default());
            let mut inserter = Inserter {
                client,
                ack,
                stats: stats.clone(),
            };
            let sealed: Vec<_> = (0..blocks).map(|_| one_row_block()).collect();
            assert_eq!(
                inserter.send_with_retry(sql, &sealed).await.is_ok(),
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
