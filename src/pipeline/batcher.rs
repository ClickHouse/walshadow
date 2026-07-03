//! Insert batcher — per-table accumulation stage.
//!
//! Coalesces decoded rows ([`RoutedRow`]) per destination table into
//! budget-sized ClickHouse Native blocks ([`InsertBatch`]). Encoding happens
//! here, not in decoders, so rows from all M decoders and all xacts merge
//! into one part per flush window per table instead of one part per decoder
//! per xact.
//!
//! Single hub task owns one [`TableEncoder`] per table; per-table task
//! sharding / hash(pk) splitting is the plan's later optimization.
//!
//! Flush triggers: `row_budget`, `byte_budget`, a per-table deadline armed
//! on first buffered row (so a cold table's rows reach an inserter within
//! `flush_timeout`, else the watermark pins behind them), and explicit
//! flush-all from the DDL/TRUNCATE barrier or shutdown. Each [`InsertBatch`]
//! carries the `(seq, rows)` counts the ack collector needs.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use clickhouse_c::Allocator;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinHandle;

use crate::ch_emitter::{
    ColumnBuf, EmitterStats, OP_DELETE, OP_INSERT, OP_UPDATE, TableEncoder, TableMapping, TablePlan,
};
use crate::config::ResolvedConfig;
use crate::heap_decoder::{CommittedTuple, HeapOp};
use crate::pipeline::{DEFAULT_PIPELINE_FLUSH, Fatal};
use crate::shadow_catalog::RelDescriptor;

/// One decoded row routed to its destination. `mapping`/`rel` are `Arc`
/// clones a decoder resolves once per xact/table.
pub struct RoutedRow {
    pub seq: u64,
    pub rel: Arc<RelDescriptor>,
    pub mapping: Arc<TableMapping>,
    pub committed: CommittedTuple,
}

/// Per-column name + CH type string. Inserter parses `type_repr` into its
/// own `TypeAst` (`Send` but not `Sync`, so unshareable).
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
    /// Order matches [`InsertBatch::buffers`]: mapped columns then the four
    /// synthetic (`_lsn`, `_xid`, `_commit_ts`, `_is_deleted`).
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
            &plan.synth_commit_ts,
            &plan.synth_is_deleted,
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

/// One independently-durable INSERT's worth of rows. `buffers` are owned
/// column slabs an inserter rebuilds a `BlockBuilder` over; `per_seq` tags
/// which xacts' rows it carries for ack accounting.
pub struct InsertBatch {
    pub meta: Arc<BatchMeta>,
    pub buffers: Vec<ColumnBuf>,
    pub n_rows: usize,
    pub per_seq: Vec<(u64, u64)>,
}

/// Rows and `FlushAll` share one FIFO channel so a barrier's flush can never
/// process ahead of rows enqueued before it — else flush seals a partial set
/// and falsely signals "earlier data sealed", pinning the durability wait.
pub enum BatcherMsg {
    /// Single row (bootstrap drain). One channel hop + wakeup per row.
    Row(RoutedRow),
    /// Chunk of rows from one decode worker (see `decode::DECODE_CHUNK_ROWS`),
    /// amortizing the per-row channel-send + cross-thread wakeup — the
    /// dominant coordination cost under sustained load. Rows may carry
    /// different `seq`s; batcher routes each independently.
    Rows(Vec<RoutedRow>),
    /// Seal every open table, push to inserters, reply. Barrier (drain before
    /// DDL/TRUNCATE) and shutdown.
    FlushAll(oneshot::Sender<()>),
}

#[derive(Clone, Copy)]
pub struct BatcherConfig {
    pub row_budget: usize,
    pub byte_budget: usize,
    /// Partial-batch deadline; caller passes positive (0 defaulted upstream)
    /// so cold tables can't pin the watermark.
    pub flush_timeout: Duration,
}

struct Table {
    enc: TableEncoder,
    meta: Arc<BatchMeta>,
    seq_counts: Vec<(u64, u64)>,
    deadline: Option<Instant>,
}

/// Per-message routing state shared by every row of one `BatcherMsg`.
struct RowCtx<'a> {
    cfg: BatcherConfig,
    out: &'a async_channel::Sender<InsertBatch>,
    alloc: Allocator,
    epoch: u64,
    /// Live snapshot for `config_column` overrides at plan build; `None`
    /// when the overlay is off.
    resolved: Option<&'a ResolvedConfig>,
    stats: &'a EmitterStats,
}

/// Spawn the batcher hub. `msg_rx` is one FIFO channel so a flush seals
/// every row enqueued before it; `out` carries sealed batches to inserters.
pub fn spawn(
    mut msg_rx: mpsc::Receiver<BatcherMsg>,
    out: async_channel::Sender<InsertBatch>,
    cfg: BatcherConfig,
    alloc: Allocator,
    fatal: Fatal,
    stats: Arc<EmitterStats>,
    mut config_rx: Option<watch::Receiver<Arc<ResolvedConfig>>>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut tables: HashMap<String, Table> = HashMap::new();
        let mut epoch: u64 = 0;
        let mut snap = snapshot(config_rx.as_ref());
        let mut live = effective_cfg(&cfg, snap.as_deref());
        let mut ticker = tokio::time::interval(live.flush_timeout);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let stats = stats.as_ref();
        loop {
            tokio::select! {
                msg = msg_rx.recv() => match msg {
                    Some(BatcherMsg::Row(r)) => {
                        snap = snapshot(config_rx.as_ref());
                        live = effective_cfg(&cfg, snap.as_deref());
                        let ctx = RowCtx { cfg: live, out: &out, alloc, epoch, resolved: snap.as_deref(), stats };
                        if let Err(e) = handle_row(&mut tables, &ctx, r).await {
                            fatal.set(format!("batcher: {e}"));
                            break;
                        }
                    }
                    Some(BatcherMsg::Rows(rows)) => {
                        snap = snapshot(config_rx.as_ref());
                        live = effective_cfg(&cfg, snap.as_deref());
                        let ctx = RowCtx { cfg: live, out: &out, alloc, epoch, resolved: snap.as_deref(), stats };
                        if let Err(e) = handle_rows(&mut tables, &ctx, rows).await {
                            fatal.set(format!("batcher: {e}"));
                            break;
                        }
                    }
                    Some(BatcherMsg::FlushAll(reply)) => {
                        if let Err(e) = flush_all(&mut tables, &out, &mut epoch, stats).await {
                            fatal.set(format!("batcher barrier flush: {e}"));
                            break;
                        }
                        let _ = reply.send(());
                    }
                    // All senders dropped: final flush
                    None => {
                        if let Err(e) = flush_all(&mut tables, &out, &mut epoch, stats).await {
                            fatal.set(format!("batcher final flush: {e}"));
                        }
                        break;
                    }
                },
                _ = ticker.tick() => {
                    if let Err(e) = flush_due(&mut tables, &out, Instant::now(), stats).await {
                        fatal.set(format!("batcher deadline flush: {e}"));
                        break;
                    }
                }
                // Live emitter-knob change: re-arm the deadline ticker to the
                // new flush_timeout. Budgets are re-read per message below.
                _ = config_changed(&mut config_rx) => {
                    snap = snapshot(config_rx.as_ref());
                    live = effective_cfg(&cfg, snap.as_deref());
                    ticker = tokio::time::interval(live.flush_timeout);
                    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                }
            }
        }
    })
}

/// Latest resolved snapshot off the watch, `None` when the overlay is off.
/// One `Arc` clone per message; also feeds the per-table column overrides at
/// plan build.
fn snapshot(rx: Option<&watch::Receiver<Arc<ResolvedConfig>>>) -> Option<Arc<ResolvedConfig>> {
    rx.map(|rx| rx.borrow().clone())
}

/// Effective batch knobs: the live resolved snapshot when the overlay is wired,
/// else the boot config. A zero live `flush_timeout` falls back to the pipeline
/// default so a cold table can't pin the watermark.
fn effective_cfg(boot: &BatcherConfig, resolved: Option<&ResolvedConfig>) -> BatcherConfig {
    let Some(r) = resolved else {
        return *boot;
    };
    let flush_timeout = if r.flush_timeout.is_zero() {
        DEFAULT_PIPELINE_FLUSH
    } else {
        r.flush_timeout
    };
    BatcherConfig {
        row_budget: r.row_budget,
        byte_budget: r.byte_budget,
        flush_timeout,
    }
}

/// Resolve once the config watch republishes; parks forever when the overlay
/// is off, so the select branch stays inert.
async fn config_changed(rx: &mut Option<watch::Receiver<Arc<ResolvedConfig>>>) {
    if let Some(rx) = rx {
        let _ = rx.changed().await;
    } else {
        std::future::pending::<()>().await
    }
}

/// Process a decoder's row chunk in order. Chunk only amortizes the channel
/// hop, not the coalescing; budget/deadline trips behave per-row.
async fn handle_rows(
    tables: &mut HashMap<String, Table>,
    ctx: &RowCtx<'_>,
    rows: Vec<RoutedRow>,
) -> Result<(), String> {
    for row in rows {
        handle_row(tables, ctx, row).await?;
    }
    Ok(())
}

async fn handle_row(
    tables: &mut HashMap<String, Table>,
    ctx: &RowCtx<'_>,
    row: RoutedRow,
) -> Result<(), String> {
    ctx.stats
        .insertbatch_rows_in
        .fetch_add(1, Ordering::Relaxed);
    let key = row.rel.qualified_name.as_ref();
    if !tables.contains_key(key) {
        // Column-type overrides re-read per plan build; a `Column*` config
        // event applies under the barrier fence, whose FlushAll cleared this
        // plan cache, so post-apply rows rebuild against the new snapshot
        let overrides = ctx.resolved.and_then(|rc| rc.columns.get(key));
        let plan = TablePlan::build(ctx.alloc, &row.rel, &row.mapping, overrides)
            .map_err(|e| e.to_string())?;
        let meta = Arc::new(BatchMeta::from_plan(&plan, key.to_owned(), ctx.epoch));
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
        // TRUNCATE is a reorder barrier; must never route here
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
        t.deadline = Some(Instant::now() + ctx.cfg.flush_timeout);
    }
    if t.enc.rows >= ctx.cfg.row_budget || t.enc.approx_bytes >= ctx.cfg.byte_budget {
        emit_batch(t, ctx.out, ctx.stats).await?;
    }
    Ok(())
}

/// Seal one table's buffered rows into an [`InsertBatch`] and hand to an
/// inserter. No-op when empty. Bumps `insertbatch_batches_out` once the batch
/// is on the inserter channel (the inserter bumps `inserter_batches_in` once it
/// drains).
async fn emit_batch(
    t: &mut Table,
    out: &async_channel::Sender<InsertBatch>,
    stats: &EmitterStats,
) -> Result<(), String> {
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
        .map_err(|_| "inserter queue closed".to_string())?;
    stats
        .insertbatch_batches_out
        .fetch_add(1, Ordering::Relaxed);
    Ok(())
}

async fn flush_due(
    tables: &mut HashMap<String, Table>,
    out: &async_channel::Sender<InsertBatch>,
    now: Instant,
    stats: &EmitterStats,
) -> Result<(), String> {
    for t in tables.values_mut() {
        if t.enc.rows > 0 && t.deadline.is_some_and(|d| now >= d) {
            emit_batch(t, out, stats).await?;
        }
    }
    Ok(())
}

/// Seal every table, drop all encoders, bump `epoch` so next rows rebuild
/// against post-DDL descriptors and inserters re-parse cached types.
async fn flush_all(
    tables: &mut HashMap<String, Table>,
    out: &async_channel::Sender<InsertBatch>,
    epoch: &mut u64,
    stats: &EmitterStats,
) -> Result<(), String> {
    for t in tables.values_mut() {
        if t.enc.rows > 0 {
            emit_batch(t, out, stats).await?;
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
    use walrus::pg::walparser::RelFileNode;

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

    /// Rows from two xacts coalesce into budget-sized batches; per-seq counts
    /// (what the ack collector compares) reconcile across the split.
    #[tokio::test]
    async fn coalesces_and_tracks_per_seq_counts() {
        let (msg_tx, msg_rx) = mpsc::channel(64);
        let (batches_tx, batches_rx) = async_channel::bounded(64);
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
            Arc::new(EmitterStats::default()),
            None,
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
        // Drop sender → final flush + graceful exit
        drop(msg_tx);

        let (mut total, mut s0, mut s1) = (0u64, 0u64, 0u64);
        while let Ok(b) = batches_rx.recv().await {
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

    /// A mixed-seq `Rows` chunk trips the budget mid-chunk yet reconciles
    /// per-seq same as the per-row path — chunk boundary is purely a
    /// channel-hop amortization (point of `DECODE_CHUNK_ROWS`).
    #[tokio::test]
    async fn rows_chunk_trips_budget_and_tracks_per_seq() {
        let (msg_tx, msg_rx) = mpsc::channel(64);
        let (batches_tx, batches_rx) = async_channel::bounded(64);
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
            Arc::new(EmitterStats::default()),
            None,
        );
        let chunk = vec![row(0, 0), row(0, 1), row(0, 2), row(1, 0), row(1, 1)];
        msg_tx
            .send(BatcherMsg::Rows(chunk))
            .await
            .expect("send chunk");
        drop(msg_tx);

        let (mut total, mut s0, mut s1) = (0u64, 0u64, 0u64);
        while let Ok(b) = batches_rx.recv().await {
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

    /// FlushAll seals everything sent before it and replies, even below budget
    /// with a huge deadline (the barrier's drain-before-DDL step). Shared FIFO
    /// channel means the first-enqueued row can't be missed (bug `codex.md`).
    #[tokio::test]
    async fn flush_all_seals_rows_enqueued_before_it() {
        let (msg_tx, msg_rx) = mpsc::channel(64);
        let (batches_tx, batches_rx) = async_channel::bounded(64);
        let fatal = Fatal::new();
        let handle = spawn(
            msg_rx,
            batches_tx,
            BatcherConfig {
                row_budget: 1_000,
                byte_budget: 1 << 30,
                // Huge deadline: only FlushAll, not the timer, can seal
                flush_timeout: Duration::from_secs(3600),
            },
            Allocator::stdlib(),
            fatal.clone(),
            Arc::new(EmitterStats::default()),
            None,
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
