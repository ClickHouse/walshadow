//! Oracle resolve, off the inserters' critical path.
//!
//! An inserter that resolves its own batch leaves its ClickHouse connection
//! idle for the whole round trip, and the oracle idle for the whole INSERT:
//! throughput is `1 / (T_oracle + T_ch / P)` where it could be
//! `1 / max(T_oracle / W, T_ch / P)`. This stage sits between batcher and
//! inserter pool so the two overlap, `W` wide to match the bridge — that is
//! how many requests the shadow answers at once.
//!
//! Costs one extra resident batch per inserter, which is what a queue deep
//! enough to keep every inserter fed takes.
//!
//! A batch with no oracle column crosses unchanged, without touching the
//! bridge.

use std::sync::Arc;
use std::sync::atomic::Ordering;

use clickhouse_c::Allocator;
use tokio::sync::watch;
use tokio::task::JoinHandle;

use crate::ch::{EmitterError, backoff_step};
use crate::config::ResolvedConfig;
use crate::emit::ch_emitter::{ColumnBuf, EmitterStats, RetryConfig, literal_column};
use crate::emit::pipeline::Fatal;
use crate::emit::pipeline::batcher::InsertBatch;
use crate::ops::oracle::{Oracle, OracleBlock, OracleError, OracleRequestColumn};

/// A sealed batch past the oracle. `local` is by ordinal, `Some` where the
/// daemon built an oracle-routed column itself and left it out of the
/// request; the inserter splices from there instead of from `resolved`
pub(crate) struct ResolvedBatch {
    pub batch: InsertBatch,
    pub local: Vec<Option<ColumnBuf>>,
    pub resolved: Option<OracleBlock>,
}

/// `None` oracle resolves nothing, which a pipeline with no oracle column
/// and a pipeline that has one but no shadow both look like; the second
/// fails at its first oracle batch
#[derive(Clone)]
pub(crate) struct ResolverOptions {
    pub oracle: Option<Arc<Oracle>>,
    /// Boot budget for oracle transport retries, re-read from `config_rx`
    pub retry: RetryConfig,
    pub stats: Arc<EmitterStats>,
    pub fatal: Fatal,
    pub config_rx: Option<watch::Receiver<Arc<ResolvedConfig>>>,
}

/// Spawn `n` resolvers over the shared batch queue. Drops the sender it was
/// handed, so the inserters' queue closes once every resolver exits
pub(crate) fn spawn_pool(
    n: usize,
    rx: async_channel::Receiver<InsertBatch>,
    tx: async_channel::Sender<ResolvedBatch>,
    opts: ResolverOptions,
) -> Vec<JoinHandle<()>> {
    (0..n.max(1))
        .map(|_| tokio::spawn(run(rx.clone(), tx.clone(), opts.clone())))
        .collect()
}

async fn run(
    rx: async_channel::Receiver<InsertBatch>,
    tx: async_channel::Sender<ResolvedBatch>,
    opts: ResolverOptions,
) {
    let ResolverOptions {
        oracle,
        mut retry,
        stats,
        fatal,
        config_rx,
    } = opts;
    let alloc = Allocator::global(&mimalloc::MiMalloc);
    while let Ok(batch) = rx.recv().await {
        if let Some(rx) = config_rx.as_ref() {
            retry.max_attempts = rx.borrow().retry_max_attempts;
        }
        let local = local_columns(&batch, &stats);
        let started = std::time::Instant::now();
        let resolved =
            match resolve_oracle_with_retry(&oracle, alloc, &batch, &local, &retry, &stats).await {
                Ok(v) => v,
                Err(e) => {
                    // Seq stays unacknowledged, so a restart replays it
                    fatal.set(format!("oracle resolve: {e}"));
                    return;
                }
            };
        stats
            .oracle_resolve_nanos
            .fetch_add(started.elapsed().as_nanos() as u64, Ordering::Relaxed);
        if tx
            .send(ResolvedBatch {
                batch,
                local,
                resolved,
            })
            .await
            .is_err()
        {
            return;
        }
    }
}

/// Oracle columns the daemon answers itself, by ordinal. Empty when the
/// batch has no oracle column at all, which is the common shape
fn local_columns(batch: &InsertBatch, stats: &EmitterStats) -> Vec<Option<ColumnBuf>> {
    if !batch
        .buffers
        .iter()
        .any(|b| matches!(b, ColumnBuf::Oracle(_)))
    {
        return Vec::new();
    }
    let taken: Vec<Option<ColumnBuf>> = batch
        .buffers
        .iter()
        .enumerate()
        .map(|(i, buf)| match buf {
            ColumnBuf::Oracle(o) => {
                literal_column(o, &batch.meta.columns[i].type_repr, batch.n_rows)
            }
            _ => None,
        })
        .collect();
    let n = taken.iter().filter(|c| c.is_some()).count() as u64;
    if n > 0 {
        stats.oracle_local_columns.fetch_add(n, Ordering::Relaxed);
    }
    taken
}

/// Retry transport failures only
async fn resolve_oracle_with_retry(
    oracle: &Option<Arc<Oracle>>,
    alloc: Allocator,
    batch: &InsertBatch,
    local: &[Option<ColumnBuf>],
    retry: &RetryConfig,
    stats: &EmitterStats,
) -> Result<Option<OracleBlock>, EmitterError> {
    let mut attempt = 0u32;
    let mut backoff = retry.initial_backoff;
    loop {
        match resolve_oracle(oracle, alloc, batch, local).await {
            Ok(v) => return Ok(v),
            Err(e) if e.retryable() && attempt < retry.max_attempts => {
                stats.retries_attempted.fetch_add(1, Ordering::Relaxed);
                attempt += 1;
                backoff_step(&mut backoff, retry.max_backoff).await;
            }
            Err(e) => return Err(EmitterError::Type(e.to_string())),
        }
    }
}

async fn resolve_oracle(
    oracle: &Option<Arc<Oracle>>,
    alloc: Allocator,
    batch: &InsertBatch,
    local: &[Option<ColumnBuf>],
) -> Result<Option<OracleBlock>, OracleError> {
    let columns: Vec<OracleRequestColumn<'_>> = batch
        .buffers
        .iter()
        .enumerate()
        .filter_map(|(i, buf)| match buf {
            ColumnBuf::Oracle(o) if local.get(i).is_none_or(Option::is_none) => {
                Some(OracleRequestColumn {
                    ordinal: i as u32,
                    name: &batch.meta.columns[i].name,
                    target_type: &batch.meta.columns[i].type_repr,
                    buf: o,
                })
            }
            _ => None,
        })
        .collect();
    if columns.is_empty() {
        return Ok(None);
    }
    let oracle = oracle.as_ref().ok_or_else(|| {
        OracleError::Absent(format!(
            "{} needs the shadow oracle, which this pipeline has none of",
            batch.meta.table_key
        ))
    })?;
    oracle
        .encode_batch(&columns, batch.n_rows, alloc)
        .await
        .map(Some)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::decode::heap_decoder::{
        ColumnValue, CommittedTuple, DecodedHeap, DecodedTuple, HeapOp,
    };
    use crate::emit::pipeline::batcher::{BatcherConfig, BatcherMsg, RoutedRow};
    use crate::emit::route::RouteSnapshot;
    use crate::mapping::{ColumnMapping, TableMapping, TableTarget};
    use crate::schema::{RelAttr, RelDescriptor, RelName, ReplIdent};
    use std::time::Duration;
    use tokio::sync::{mpsc, oneshot};
    use walrus::pg::walparser::RelFileNode;

    /// An oracle-routed column the local matrix does not cover, rendered by
    /// the daemon: the shape a PostGIS 2-D point takes
    const GEOM_OID: u32 = 999_999;

    fn wkt_row(seq: u64, text: Option<&str>) -> RoutedRow {
        let rel = Arc::new(RelDescriptor {
            rfn: RelFileNode {
                spc_node: 1663,
                db_node: 5,
                rel_node: 16385,
            },
            oid: 16385,
            toast_oid: 0,
            namespace_oid: 2200,
            rel_name: RelName::new("public", "g"),
            kind: 'r',
            persistence: 'p',
            replident: ReplIdent::Default { pk_attnums: None },
            attributes: vec![RelAttr {
                attnum: 1,
                name: "geom".into(),
                type_oid: GEOM_OID,
                typmod: -1,
                not_null: false,
                dropped: false,
                type_name: "geometry".into(),
                type_byval: false,
                type_len: -1,
                type_align: 'i',
                type_storage: 'x',
                missing_text: None,
            }],
        });
        let route = RouteSnapshot::freeze(
            Arc::new(TableMapping {
                target: TableTarget::new("default", "g"),
                columns: vec![ColumnMapping {
                    src_attnum: 1,
                    target_name: "geom".into(),
                    target_type: "Nullable(String)".into(),
                }],
            }),
            Arc::default(),
            Default::default(),
        );
        RoutedRow {
            seq,
            rel,
            route,
            committed: CommittedTuple {
                decoded: DecodedHeap {
                    rfn: RelFileNode {
                        spc_node: 1663,
                        db_node: 5,
                        rel_node: 16385,
                    },
                    xid: 7,
                    source_lsn: 0x1000 + seq,
                    op: HeapOp::Insert,
                    new: Some(DecodedTuple {
                        columns: vec![text.map(|t| ColumnValue::Text(t.into()))],
                        partial: false,
                    }),
                    old: None,
                },
                commit_ts: 0,
                commit_lsn: (seq + 1) * 100,
            },
            value_permit: None,
        }
    }

    /// One batch of the rows, sealed through the real batcher: `InsertBatch`
    /// is only reachable that way, and its cell tags are what the encoder
    /// chose rather than what a test asserted
    async fn seal(rows: Vec<RoutedRow>) -> InsertBatch {
        let (msg_tx, msg_rx) = mpsc::channel(64);
        let (batches_tx, batches_rx) = async_channel::bounded(64);
        let fatal = Fatal::new();
        let handle = crate::emit::pipeline::batcher::spawn(
            msg_rx,
            batches_tx,
            BatcherConfig {
                row_budget: 1_000,
                byte_budget: 1 << 30,
                flush_timeout: Duration::from_secs(3600),
            },
            Allocator::stdlib(),
            fatal.clone(),
            Arc::new(EmitterStats::default()),
            None,
        );
        for r in rows {
            msg_tx.send(BatcherMsg::Row(r)).await.expect("send row");
        }
        let (reply_tx, reply_rx) = oneshot::channel();
        msg_tx
            .send(BatcherMsg::FlushAll(reply_tx))
            .await
            .expect("send flush");
        reply_rx.await.expect("flush ack");
        let batch = batches_rx.recv().await.expect("one batch");
        drop(msg_tx);
        handle.await.expect("batcher task");
        assert!(fatal.message().is_none());
        batch
    }

    /// Cells the daemon rendered against a `String` target never reach the
    /// bridge: the same batch resolves with no oracle at all, and fails
    /// without the local path
    #[tokio::test]
    async fn rendered_cells_against_a_string_target_stay_local() {
        let batch = seal(vec![
            wkt_row(0, Some("POINT(1 2)")),
            wkt_row(0, None),
            wkt_row(0, Some("POINT(3 4)")),
        ])
        .await;
        assert!(
            matches!(batch.buffers[0], ColumnBuf::Oracle(_)),
            "premise: an uncovered source type routes to the oracle",
        );

        let stats = EmitterStats::default();
        let local = local_columns(&batch, &stats);
        assert_eq!(stats.oracle_local_columns.load(Ordering::Relaxed), 1);
        let Some(ColumnBuf::NullableString {
            offsets,
            data,
            null_map,
        }) = local[0].as_ref()
        else {
            panic!("column not built locally: {:?}", local[0]);
        };
        assert_eq!(data, b"POINT(1 2)POINT(3 4)");
        assert_eq!(offsets, &[10, 10, 20], "absent cell holds its offset");
        assert_eq!(null_map, &[0, 1, 0]);

        let alloc = Allocator::stdlib();
        assert!(
            resolve_oracle(&None, alloc, &batch, &local)
                .await
                .expect("no request to make")
                .is_none(),
        );
        // Same batch without the local build has to ask someone
        assert!(matches!(
            resolve_oracle(&None, alloc, &batch, &[]).await,
            Err(OracleError::Absent(_)),
        ));
    }

    /// A cell PG must convert keeps the whole column in the request, even
    /// when its neighbours are rendered
    #[tokio::test]
    async fn one_unrendered_cell_keeps_the_column_remote() {
        let mut rows = vec![wkt_row(0, Some("POINT(1 2)"))];
        let mut raw = wkt_row(0, None);
        raw.committed.decoded.new.as_mut().unwrap().columns[0] = Some(ColumnValue::Unsupported {
            type_oid: GEOM_OID,
            raw: vec![1, 2, 3],
        });
        rows.push(raw);
        let batch = seal(rows).await;

        let stats = EmitterStats::default();
        let local = local_columns(&batch, &stats);
        assert!(local[0].is_none());
        assert_eq!(stats.oracle_local_columns.load(Ordering::Relaxed), 0);
    }
}
