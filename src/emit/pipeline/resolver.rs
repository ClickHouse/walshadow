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

use std::collections::hash_map::Entry;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use backon::Retryable;
use clickhouse_c::{Allocator, BlockBuilder, ColumnBuilder, TypeAst};
use tokio::sync::watch;
use tokio::task::JoinHandle;

use crate::ch::{CompressionChoice, EmitterError};
use crate::config::ResolvedConfig;
use crate::emit::ch_emitter::{
    ColumnBuf, EmitterStats, RetryConfig, build_leaf, build_root, literal_column,
};
use crate::emit::pipeline::Fatal;
use crate::emit::pipeline::batcher::{BatchMeta, BlockSlabs, InsertBatch};
use crate::emit::pipeline::sealed::SealedBlock;
use crate::ops::oracle::{Oracle, OracleBlock, OracleError, OracleRequestColumn};
use crate::schema::TableKey;
use foldhash::HashMap;

/// Resolved INSERT: every block serialized and compressed, slabs and
/// oracle output released
pub(crate) struct ResolvedBatch {
    pub meta: Arc<BatchMeta>,
    pub blocks: Vec<SealedBlock>,
    pub n_rows: usize,
    pub per_seq: Vec<(u64, u64)>,
    /// Admission permit shares covering these rows, dropped post-insert-ack
    _permits: Vec<Arc<crate::budget::MemoryPermit>>,
    _encoded: crate::budget::MemoryPermit,
}

/// `None` oracle resolves nothing, which a pipeline with no oracle column
/// and a pipeline that has one but no shadow both look like; the second
/// fails at its first oracle batch
#[derive(Clone)]
pub(crate) struct ResolverOptions {
    pub oracle: Option<Arc<Oracle>>,
    /// Boot budget for oracle transport retries, re-read from `config_rx`
    pub retry: RetryConfig,
    /// Compression of prepared blocks, matching connections that negotiate
    /// it; others rebuild
    pub codec: CompressionChoice,
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

/// Per-task resolver state: parsed column types and compression codec
pub(crate) struct Sealer {
    alloc: Allocator,
    compression: CompressionChoice,
    codec: Option<std::pin::Pin<Box<clickhouse_c::Codec>>>,
    /// Parsed column types per table, refreshed when a batch's
    /// `schema_epoch` changes. `TypeAst` is `Send` but not `Sync`, so each
    /// task parses its own
    asts: HashMap<TableKey, (u64, Vec<TypeAst>)>,
}

impl Sealer {
    pub(crate) fn new(compression: CompressionChoice) -> Result<Self, EmitterError> {
        Ok(Self {
            alloc: Allocator::global(&mimalloc::MiMalloc),
            compression,
            codec: compression.build_codec()?,
            asts: HashMap::default(),
        })
    }

    /// Serialize and compress one block over its slabs and oracle answer
    pub(crate) fn seal(
        &mut self,
        meta: &BatchMeta,
        block: &BlockSlabs,
        resolved: Option<&OracleBlock>,
    ) -> Result<SealedBlock, EmitterError> {
        let asts = match self.asts.entry(meta.table_key.clone()) {
            Entry::Occupied(e) if e.get().0 == meta.schema_epoch => e.into_mut(),
            e => {
                let parsed = meta
                    .columns
                    .iter()
                    .map(|c| TypeAst::parse(&c.type_repr, self.alloc))
                    .collect::<Result<Vec<_>, _>>()?;
                let fresh = (meta.schema_epoch, parsed);
                match e {
                    Entry::Occupied(mut e) => {
                        e.insert(fresh);
                        e.into_mut()
                    }
                    Entry::Vacant(e) => e.insert(fresh),
                }
            }
        };
        let n_rows = block.n_rows;
        let leaves: Vec<Option<ColumnBuilder<'_>>> = block
            .buffers
            .iter()
            .map(|buf| build_leaf(buf, n_rows))
            .collect::<Result<_, _>>()?;
        let roots: Vec<Option<ColumnBuilder<'_>>> = block
            .buffers
            .iter()
            .zip(&leaves)
            .map(|(buf, leaf)| build_root(buf, leaf.as_ref(), n_rows))
            .collect::<Result<_, _>>()?;
        let mut bb = BlockBuilder::new();
        for (i, col) in meta.columns.iter().enumerate() {
            match &roots[i] {
                Some(node) => bb.append(&col.name, asts.1[i].view(), node)?,
                None => {
                    let decoded = resolved.and_then(|r| r.column(i as u32)).ok_or_else(|| {
                        EmitterError::Type(format!("oracle answered no column for {}", col.name))
                    })?;
                    bb.append_column(&col.name, asts.1[i].view(), decoded)?;
                }
            }
        }
        SealedBlock::seal(
            &bb,
            self.compression,
            self.codec.as_ref().map(|c| c.as_ref()),
            self.alloc,
        )
    }
}

async fn run(
    rx: async_channel::Receiver<InsertBatch>,
    tx: async_channel::Sender<ResolvedBatch>,
    opts: ResolverOptions,
) {
    let ResolverOptions {
        oracle,
        mut retry,
        codec,
        stats,
        fatal,
        config_rx,
    } = opts;
    let alloc = Allocator::global(&mimalloc::MiMalloc);
    let mut sealer = match Sealer::new(codec) {
        Ok(sealer) => sealer,
        Err(e) => {
            fatal.set(format!("seal codec: {e}"));
            return;
        }
    };
    while let Ok(batch) = rx.recv().await {
        if let Some(rx) = config_rx.as_ref() {
            retry.max_attempts = rx.borrow().retry_max_attempts;
        }
        let InsertBatch {
            meta,
            blocks,
            n_rows,
            per_seq,
            permits,
            mut encoded,
        } = batch;
        let mut sealed = Vec::with_capacity(blocks.len());
        for mut block in blocks {
            resolve_local_columns(&mut block, &stats);
            let started = std::time::Instant::now();
            let resolved = match resolve_oracle_with_retry(
                &oracle, alloc, &meta, &block, &retry, &stats,
            )
            .await
            {
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
            let started = std::time::Instant::now();
            match sealer.seal(&meta, &block, resolved.as_ref()) {
                Ok(b) => {
                    stats
                        .resident_raw_bytes
                        .fetch_add(b.raw_len() as u64, Ordering::Relaxed);
                    stats
                        .resident_stored_bytes
                        .fetch_add(b.allocated_bytes() as u64, Ordering::Relaxed);
                    sealed.push(b);
                }
                Err(e) => {
                    fatal.set(format!("seal block: {e}"));
                    return;
                }
            }
            stats
                .resident_seal_nanos
                .fetch_add(started.elapsed().as_nanos() as u64, Ordering::Relaxed);
        }
        // Slabs and oracle output already dropped
        encoded.shrink(sealed.iter().map(|b| b.allocated_bytes() as u64).sum());
        let ready = ResolvedBatch {
            meta,
            blocks: sealed,
            n_rows,
            per_seq,
            _permits: permits,
            _encoded: encoded,
        };
        if tx.send(ready).await.is_err() {
            return;
        }
    }
}

fn resolve_local_columns(block: &mut BlockSlabs, stats: &EmitterStats) {
    let mut n = 0;
    for buf in &mut block.buffers {
        if let ColumnBuf::Oracle(o) = buf
            && let Some(local) = literal_column(o, block.n_rows)
        {
            *buf = local;
            n += 1;
        }
    }
    if n > 0 {
        stats.oracle_local_columns.fetch_add(n, Ordering::Relaxed);
    }
}

/// Retry transport failures only
async fn resolve_oracle_with_retry(
    oracle: &Option<Arc<Oracle>>,
    alloc: Allocator,
    meta: &BatchMeta,
    block: &BlockSlabs,
    retry: &RetryConfig,
    stats: &EmitterStats,
) -> Result<Option<OracleBlock>, EmitterError> {
    (|| resolve_oracle(oracle, alloc, meta, block))
        .retry(retry.backoff())
        .when(OracleError::retryable)
        .notify(|_, _| {
            stats.retries_attempted.fetch_add(1, Ordering::Relaxed);
        })
        .await
        .map_err(|e| EmitterError::Type(e.to_string()))
}

async fn resolve_oracle(
    oracle: &Option<Arc<Oracle>>,
    alloc: Allocator,
    meta: &BatchMeta,
    block: &BlockSlabs,
) -> Result<Option<OracleBlock>, OracleError> {
    let columns: Vec<OracleRequestColumn<'_>> = block
        .buffers
        .iter()
        .enumerate()
        .filter_map(|(i, buf)| match buf {
            ColumnBuf::Oracle(o) => Some(OracleRequestColumn {
                ordinal: i as u32,
                name: &meta.columns[i].name,
                target_type: &meta.columns[i].type_repr,
                buf: o,
            }),
            _ => None,
        })
        .collect();
    if columns.is_empty() {
        return Ok(None);
    }
    let oracle = oracle.as_ref().ok_or_else(|| {
        OracleError::Absent(format!(
            "{} needs the shadow oracle, which this pipeline has none of",
            meta.table_key
        ))
    })?;
    oracle
        .encode_batch(meta.table_key.db_oid, &columns, block.n_rows, alloc)
        .await
        .map(Some)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::decode::heap_decoder::{
        ColumnValue, CommittedTuple, DecodedHeap, DecodedTuple, HeapOp,
    };
    use crate::emit::ch_emitter::Wire;
    use crate::emit::pipeline::batcher::{
        BatcherConfig, BatcherMsg, RoutedRow, RowChunk, RowTable,
    };
    use crate::emit::route::RouteSnapshot;
    use crate::mapping::{ColumnMapping, TableMapping, TableTarget};
    use crate::schema::{RelAttr, RelDescriptor, RelName, ReplIdent};
    use std::time::Duration;
    use tokio::sync::{mpsc, oneshot};
    use walrus::pg::walparser::RelFileNode;

    /// An oracle-routed column the local matrix does not cover, rendered by
    /// the daemon: the shape a PostGIS 2-D point takes
    const GEOM_OID: u32 = 999_999;

    fn wkt_table() -> RowTable {
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
            ancestors: Vec::new(),
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
                missing_default: None,
            }],
        });
        let route = RouteSnapshot::freeze(
            Arc::new(TableMapping {
                target: TableTarget::new("default", "g"),
                columns: vec![ColumnMapping {
                    src_attnum: 1,
                    target_name: "geom".into(),
                    target_type: "Nullable(String)".into(),
                    type_pinned: false,
                }],
                tees: Vec::new(),
            }),
            Arc::default(),
            Default::default(),
        );
        RowTable { rel, route }
    }

    fn wkt_row(seq: u64, text: Option<&str>) -> RoutedRow {
        RoutedRow {
            seq,
            table: 0,
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
                inserters: 1,
                flush_timeout: Duration::from_secs(3600),
            },
            Allocator::global(&mimalloc::MiMalloc),
            fatal.clone(),
            Arc::new(EmitterStats::default()),
            None,
        );
        let chunk = RowChunk {
            tables: vec![wkt_table()],
            rows,
            permit: None,
        };
        msg_tx
            .send(BatcherMsg::Rows(chunk))
            .await
            .expect("send rows");
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
        let mut batch = seal(vec![
            wkt_row(0, Some("POINT(1 2)")),
            wkt_row(0, None),
            wkt_row(0, Some("")),
            wkt_row(0, Some("POINT(3 4)")),
        ])
        .await;
        assert!(
            matches!(batch.blocks[0].buffers[0], ColumnBuf::Oracle(_)),
            "premise: an uncovered source type routes to the oracle",
        );

        let alloc = Allocator::global(&mimalloc::MiMalloc);
        assert!(matches!(
            resolve_oracle(&None, alloc, &batch.meta, &batch.blocks[0]).await,
            Err(OracleError::Absent(_)),
        ));
        let stats = EmitterStats::default();
        resolve_local_columns(&mut batch.blocks[0], &stats);
        assert_eq!(stats.oracle_local_columns.load(Ordering::Relaxed), 1);
        assert_eq!(
            batch.blocks[0].buffers[0].wire(),
            Wire::NullableString {
                offsets: &[10, 10, 10, 20],
                data: b"POINT(1 2)POINT(3 4)",
                null_map: &[0, 1, 0, 0],
                absent: b"",
            }
        );

        assert!(
            resolve_oracle(&None, alloc, &batch.meta, &batch.blocks[0])
                .await
                .expect("no request to make")
                .is_none(),
        );
        resolve_local_columns(&mut batch.blocks[0], &stats);
        assert_eq!(stats.oracle_local_columns.load(Ordering::Relaxed), 1);
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
        let mut batch = seal(rows).await;

        let stats = EmitterStats::default();
        resolve_local_columns(&mut batch.blocks[0], &stats);
        assert!(matches!(batch.blocks[0].buffers[0], ColumnBuf::Oracle(_)));
        assert_eq!(stats.oracle_local_columns.load(Ordering::Relaxed), 0);
        assert!(matches!(
            resolve_oracle(
                &None,
                Allocator::global(&mimalloc::MiMalloc),
                &batch.meta,
                &batch.blocks[0],
            )
            .await,
            Err(OracleError::Absent(_)),
        ));
    }
}
