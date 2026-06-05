//! Insert batcher — the per-table accumulation stage.
//!
//! Decoders produce decoded, detoasted, type-resolved rows
//! ([`RoutedRow`]); the batcher coalesces them per destination table into
//! budget-sized ClickHouse Native blocks ([`InsertBatch`]) that any idle
//! inserter can send. Encoding (the per-column byte packing) happens here,
//! not in the decoders, so rows from all M decoders and all xacts merge
//! into one part per flush window per table instead of one part per decoder
//! per xact.
//!
//! A single hub task owns one [`TableEncoder`] per table. This is strictly
//! more parallel than the old single-connection emitter (which encoded
//! every table on one task) while keeping batch coalescing intact; per-table
//! task sharding / hash(pk) splitting is the plan's later optimization.
//!
//! Flush triggers: `row_budget`, `byte_budget`, a per-table deadline armed
//! on the first buffered row (so a cold table's rows still reach an inserter
//! within `flush_timeout` — the watermark would otherwise pin behind them),
//! and an explicit flush-all from the DDL/TRUNCATE barrier or shutdown.
//! Each [`InsertBatch`] carries the `(seq, rows)` counts the ack collector
//! needs.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use clickhouse_c::Allocator;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

use crate::ch_emitter::{
    ColumnBuf, OP_DELETE, OP_INSERT, OP_UPDATE, TableEncoder, TableMapping, TablePlan,
};
use crate::heap_decoder::{CommittedTuple, HeapOp};
use crate::pipeline::{Fatal, mpmc};
use crate::shadow_catalog::RelDescriptor;

/// One decoded row routed to its destination table. `mapping` and `rel`
/// are cheap `Arc` clones a decoder resolves once per xact/table.
pub struct RoutedRow {
    pub seq: u64,
    pub rel: Arc<RelDescriptor>,
    pub mapping: Arc<TableMapping>,
    pub committed: CommittedTuple,
}

/// Per-column name + CH type string. The inserter parses `type_repr` into
/// its own `TypeAst` (which is `Send` but not `Sync`, so can't be shared).
#[derive(Clone)]
pub struct ColMeta {
    pub name: String,
    pub type_repr: String,
}

/// Immutable per-table block shape, shared by every batch of that table
/// until a barrier rebuilds it (bumping `schema_epoch`).
pub struct BatchMeta {
    pub table_key: String,
    pub insert_sql: String,
    /// Column order matches [`InsertBatch::buffers`]: mapped columns then
    /// the four synthetic ones (`_lsn`, `_xid`, `_op`, `_commit_ts`).
    pub columns: Vec<ColMeta>,
    pub schema_epoch: u64,
}

impl BatchMeta {
    fn from_plan(plan: &TablePlan, table_key: String, schema_epoch: u64) -> Self {
        let mut columns = Vec::with_capacity(plan.columns.len() + 4);
        for c in &plan.columns {
            columns.push(ColMeta {
                name: c.name.clone(),
                type_repr: c.type_repr.clone(),
            });
        }
        for synth in [
            &plan.synth_lsn,
            &plan.synth_xid,
            &plan.synth_op,
            &plan.synth_commit_ts,
        ] {
            columns.push(ColMeta {
                name: synth.name.clone(),
                type_repr: synth.type_repr.clone(),
            });
        }
        Self {
            table_key,
            insert_sql: plan.insert_sql.clone(),
            columns,
            schema_epoch,
        }
    }
}

/// A complete, independently-durable INSERT's worth of rows. `buffers` are
/// the owned column slabs; an inserter rebuilds the `BlockBuilder` over
/// them. `per_seq` tags which xacts' rows it carries for ack accounting.
pub struct InsertBatch {
    pub meta: Arc<BatchMeta>,
    pub buffers: Vec<ColumnBuf>,
    pub n_rows: usize,
    pub per_seq: Vec<(u64, u64)>,
}

/// One message into the batcher. Rows (from the decode pool) and `FlushAll`
/// (from the reorder/barrier coordinator) share a single FIFO channel so a
/// barrier's flush can never be processed ahead of rows already enqueued
/// before it — otherwise the flush would seal a partial set and falsely
/// signal "earlier data sealed", pinning the barrier's durability wait.
pub enum BatcherMsg {
    Row(RoutedRow),
    /// Seal every open table and push to inserters, then reply. Used by the
    /// barrier (drain before DDL/TRUNCATE) and reachable on shutdown.
    FlushAll(oneshot::Sender<()>),
}

#[derive(Clone, Copy)]
pub struct BatcherConfig {
    pub row_budget: usize,
    pub byte_budget: usize,
    /// Partial-batch deadline. The caller passes a positive value (0 is
    /// defaulted upstream) so cold tables can't pin the watermark.
    pub flush_timeout: Duration,
}

struct Table {
    enc: TableEncoder,
    meta: Arc<BatchMeta>,
    seq_counts: Vec<(u64, u64)>,
    deadline: Option<Instant>,
}

/// Spawn the batcher hub.
///
/// * `msg_rx` — single FIFO channel of [`BatcherMsg`] (rows from the M
///   decoders + `FlushAll` from the barrier coordinator). Sharing one
///   channel is what guarantees a flush seals every row enqueued before it.
/// * `out` — sealed batches to the inserter pool (spmc).
pub fn spawn(
    mut msg_rx: mpsc::Receiver<BatcherMsg>,
    out: mpmc::Sender<InsertBatch>,
    cfg: BatcherConfig,
    alloc: Allocator,
    fatal: Fatal,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut tables: HashMap<String, Table> = HashMap::new();
        let mut epoch: u64 = 0;
        let mut ticker = tokio::time::interval(cfg.flush_timeout);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                msg = msg_rx.recv() => match msg {
                    Some(BatcherMsg::Row(r)) => {
                        if let Err(e) = handle_row(&mut tables, &cfg, &out, alloc, epoch, r).await {
                            fatal.set(format!("batcher: {e}"));
                            break;
                        }
                    }
                    Some(BatcherMsg::FlushAll(reply)) => {
                        if let Err(e) = flush_all(&mut tables, &out, &mut epoch).await {
                            fatal.set(format!("batcher barrier flush: {e}"));
                            break;
                        }
                        let _ = reply.send(());
                    }
                    // All senders (decoders + reorder) dropped: final flush.
                    None => {
                        if let Err(e) = flush_all(&mut tables, &out, &mut epoch).await {
                            fatal.set(format!("batcher final flush: {e}"));
                        }
                        break;
                    }
                },
                _ = ticker.tick() => {
                    if let Err(e) = flush_due(&mut tables, &out, Instant::now()).await {
                        fatal.set(format!("batcher deadline flush: {e}"));
                        break;
                    }
                }
            }
        }
    })
}

async fn handle_row(
    tables: &mut HashMap<String, Table>,
    cfg: &BatcherConfig,
    out: &mpmc::Sender<InsertBatch>,
    alloc: Allocator,
    epoch: u64,
    row: RoutedRow,
) -> Result<(), String> {
    let key = row.rel.qualified_name.as_ref();
    if !tables.contains_key(key) {
        let plan = TablePlan::build(alloc, &row.rel, &row.mapping).map_err(|e| e.to_string())?;
        let meta = Arc::new(BatchMeta::from_plan(&plan, key.to_owned(), epoch));
        let enc = TableEncoder::new(plan).map_err(|e| e.to_string())?;
        tables.insert(
            key.to_owned(),
            Table {
                enc,
                meta,
                seq_counts: Vec::new(),
                deadline: None,
            },
        );
    }
    let t = tables.get_mut(key).expect("just inserted");
    let op = match row.committed.decoded.op {
        HeapOp::Insert => OP_INSERT,
        HeapOp::Update | HeapOp::HotUpdate => OP_UPDATE,
        HeapOp::Delete => OP_DELETE,
        // TRUNCATE is a barrier handled by reorder; it must never route here.
        HeapOp::Truncate => return Err("TRUNCATE routed to batcher".into()),
    };
    t.enc
        .append_row(&row.committed, &row.mapping, op)
        .map_err(|e| e.to_string())?;
    match t.seq_counts.last_mut() {
        Some((s, c)) if *s == row.seq => *c += 1,
        _ => t.seq_counts.push((row.seq, 1)),
    }
    if t.deadline.is_none() {
        t.deadline = Some(Instant::now() + cfg.flush_timeout);
    }
    if t.enc.rows >= cfg.row_budget || t.enc.approx_bytes >= cfg.byte_budget {
        emit_batch(t, out).await?;
    }
    Ok(())
}

/// Seal one table's buffered rows into an [`InsertBatch`] and hand it to an
/// inserter. No-op when empty. Resets the table's deadline + seq counts.
async fn emit_batch(t: &mut Table, out: &mpmc::Sender<InsertBatch>) -> Result<(), String> {
    let (buffers, n_rows) = t.enc.take_block().map_err(|e| e.to_string())?;
    t.deadline = None;
    if n_rows == 0 {
        t.seq_counts.clear();
        return Ok(());
    }
    let per_seq = std::mem::take(&mut t.seq_counts);
    let batch = InsertBatch {
        meta: t.meta.clone(),
        buffers,
        n_rows,
        per_seq,
    };
    out.send(batch)
        .await
        .map_err(|_| "inserter queue closed".to_string())
}

async fn flush_due(
    tables: &mut HashMap<String, Table>,
    out: &mpmc::Sender<InsertBatch>,
    now: Instant,
) -> Result<(), String> {
    for t in tables.values_mut() {
        if t.enc.rows > 0 && t.deadline.is_some_and(|d| now >= d) {
            emit_batch(t, out).await?;
        }
    }
    Ok(())
}

/// Seal every table, then drop all encoders and bump `epoch` so the next
/// rows rebuild against fresh descriptors (post-DDL) and inserters
/// re-parse their cached types.
async fn flush_all(
    tables: &mut HashMap<String, Table>,
    out: &mpmc::Sender<InsertBatch>,
    epoch: &mut u64,
) -> Result<(), String> {
    for t in tables.values_mut() {
        if t.enc.rows > 0 {
            emit_batch(t, out).await?;
        }
    }
    tables.clear();
    *epoch += 1;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ch_emitter::ColumnMapping;
    use crate::heap_decoder::{ColumnValue, DecodedHeap, DecodedTuple, HeapOp};
    use crate::shadow_catalog::{RelAttr, RelDescriptor, ReplIdent};
    use tokio::sync::oneshot;
    use wal_rs::pg::walparser::RelFileNode;

    fn rel() -> Arc<RelDescriptor> {
        Arc::new(RelDescriptor {
            rfn: RelFileNode {
                spc_node: 1663,
                db_node: 5,
                rel_node: 16385,
            },
            oid: 16385,
            namespace_oid: 2200,
            namespace_name: "public".into(),
            name: "t".into(),
            qualified_name: RelDescriptor::build_qualified_name("public", "t"),
            kind: 'r',
            persistence: 'p',
            replident: ReplIdent::Default { pk_attnums: None },
            attributes: vec![RelAttr {
                attnum: 1,
                name: "id".into(),
                type_oid: 23,
                typmod: -1,
                not_null: true,
                dropped: false,
                type_name: "int4".into(),
                type_byval: true,
                type_len: 4,
                type_align: 'i',
                type_storage: 'p',
                missing_text: None,
            }],
        })
    }

    fn mapping() -> Arc<TableMapping> {
        Arc::new(TableMapping {
            target: "default.t".into(),
            columns: vec![ColumnMapping {
                src_attnum: 1,
                target_name: "id".into(),
                target_type: "Int32".into(),
            }],
        })
    }

    fn row(seq: u64, id: i32) -> RoutedRow {
        RoutedRow {
            seq,
            rel: rel(),
            mapping: mapping(),
            committed: CommittedTuple {
                decoded: DecodedHeap {
                    rfn: RelFileNode {
                        spc_node: 1663,
                        db_node: 5,
                        rel_node: 16385,
                    },
                    xid: 7,
                    source_lsn: 0x1000 + id as u64,
                    op: HeapOp::Insert,
                    new: Some(DecodedTuple {
                        columns: vec![Some(ColumnValue::Int4(id))],
                        partial: false,
                    }),
                    old: None,
                },
                commit_ts: 0,
                commit_lsn: (seq + 1) * 100,
            },
        }
    }

    /// Rows from two xacts coalesce into budget-sized batches, and every
    /// batch carries accurate per-seq row counts (what the ack collector
    /// compares against). Budget trips + the final flush split the 5 rows
    /// across batches; the totals must still reconcile per seq.
    #[tokio::test]
    async fn coalesces_and_tracks_per_seq_counts() {
        let (msg_tx, msg_rx) = mpsc::channel(64);
        let (batches_tx, batches_rx) = mpmc::channel(64);
        let fatal = Fatal::new();
        let handle = spawn(
            msg_rx,
            batches_tx,
            BatcherConfig {
                row_budget: 2,
                byte_budget: 1 << 30,
                flush_timeout: Duration::from_secs(3600),
            },
            Allocator::stdlib(),
            fatal.clone(),
        );
        for id in 0..3 {
            msg_tx
                .send(BatcherMsg::Row(row(0, id)))
                .await
                .expect("send seq0");
        }
        for id in 0..2 {
            msg_tx
                .send(BatcherMsg::Row(row(1, id)))
                .await
                .expect("send seq1");
        }
        // Drop the sender → final flush + graceful exit, closing the batch
        // channel once drained.
        drop(msg_tx);

        let (mut total, mut s0, mut s1) = (0u64, 0u64, 0u64);
        while let Some(b) = batches_rx.recv().await {
            total += b.n_rows as u64;
            for (seq, n) in b.per_seq {
                match seq {
                    0 => s0 += n,
                    1 => s1 += n,
                    other => panic!("unexpected seq {other}"),
                }
            }
        }
        handle.await.expect("batcher task");
        assert_eq!(total, 5, "all rows sealed exactly once");
        assert_eq!(s0, 3, "seq 0 rows");
        assert_eq!(s1, 2, "seq 1 rows");
        assert!(fatal.message().is_none(), "no fatal: {:?}", fatal.message());
    }

    /// FlushAll seals everything sent before it and replies, even below
    /// budget and with a huge deadline — the barrier's drain-before-DDL step
    /// depends on this. Because rows and FlushAll share one FIFO channel,
    /// the row enqueued first can't be missed (the bug `codex.md` caught).
    #[tokio::test]
    async fn flush_all_seals_rows_enqueued_before_it() {
        let (msg_tx, msg_rx) = mpsc::channel(64);
        let (batches_tx, batches_rx) = mpmc::channel(64);
        let fatal = Fatal::new();
        let handle = spawn(
            msg_rx,
            batches_tx,
            BatcherConfig {
                row_budget: 1_000,
                byte_budget: 1 << 30,
                // Huge deadline: only FlushAll (not the timer) can seal.
                flush_timeout: Duration::from_secs(3600),
            },
            Allocator::stdlib(),
            fatal.clone(),
        );
        msg_tx
            .send(BatcherMsg::Row(row(0, 1)))
            .await
            .expect("send row");
        let (reply_tx, reply_rx) = oneshot::channel();
        msg_tx
            .send(BatcherMsg::FlushAll(reply_tx))
            .await
            .expect("send flush");
        reply_rx.await.expect("flush-all ack");
        let batch = batches_rx.recv().await.expect("one batch");
        assert_eq!(batch.n_rows, 1);
        assert_eq!(batch.per_seq, vec![(0, 1)]);
        drop(msg_tx);
        handle.await.expect("batcher task");
        assert!(fatal.message().is_none());
    }
}
