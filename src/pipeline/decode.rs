//! Decode pool — the CPU/IO-parallel stage.
//!
//! Each worker pulls a [`DecodeJob`] (one committed xact's ordered,
//! still-toasted heaps + chunk map) off the shared job queue and, per heap:
//! detoasts (catalog), resolves the relation → mapping → destination table,
//! runs oracle `PgPending` resolution + sampled validation, and routes the
//! row to the [`InsertBatcher`](crate::pipeline::batcher). After the xact's
//! last row it reports `Placed{seq, rows}` to the ack collector.
//!
//! Out-of-order completion across workers is fine: rows carry `source_lsn`
//! as `_lsn`, so `ReplacingMergeTree(_lsn)` converges per PK
//! ([[project_walshadow_eventual_consistency]]). At M=1 dispatch order (so
//! per-table WAL order) is preserved.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use tokio::sync::{Mutex, mpsc};
use tokio::task::JoinHandle;

use crate::ch_emitter::MappingHandle;
use crate::heap_decoder::{CommittedTuple, DecodedHeap};
use crate::oracle::{Oracle, maybe_validate_tuple, resolve_pending_tuple};
use crate::pipeline::ack::AckHandle;
use crate::pipeline::batcher::{BatcherMsg, RoutedRow};
use crate::pipeline::{Fatal, mpmc};
use crate::relation_resolver::RelationResolver;
use crate::shadow_catalog::{CatalogError, ShadowCatalog};
use crate::xact_buffer::detoast_heap;

/// Reassembled-TOAST chunk map: `(toast_relid, value_id) -> seq -> bytes`.
pub type ToastChunks = HashMap<(u32, u32), BTreeMap<u32, Vec<u8>>>;

/// One committed xact handed to the decode pool. `chunks` is `Arc` so the
/// barrier coordinator can dispatch several data segments of one xact
/// sharing the same TOAST chunk map without cloning it.
pub struct DecodeJob {
    pub seq: u64,
    pub commit_ts: i64,
    pub commit_lsn: u64,
    pub heaps: Vec<DecodedHeap>,
    pub chunks: Arc<ToastChunks>,
}

/// Shared dependencies a decode worker (or the barrier's inline data path)
/// needs to turn heaps into routed rows.
#[derive(Clone)]
pub struct DecodeCtx {
    pub catalog: Arc<Mutex<ShadowCatalog>>,
    pub mapping: MappingHandle,
    pub oracle: Option<Arc<Oracle>>,
    /// Rows go to the batcher on the shared FIFO `BatcherMsg` channel (so a
    /// barrier `FlushAll` can't overtake them).
    pub msg_tx: mpsc::Sender<BatcherMsg>,
}

/// Detoast, resolve, and route every heap of one xact to the batcher.
/// Returns the number of rows routed (= rows the batcher will encode and
/// inserters will ack — the `R` the collector compares against). Used by
/// both the decode workers and the barrier's data segments.
///
/// Foreign-database and unmapped relations are skipped (not counted), as
/// in the serial emitter; any other catalog error poisons the stream.
pub async fn decode_and_route(
    ctx: &DecodeCtx,
    seq: u64,
    commit_ts: i64,
    commit_lsn: u64,
    heaps: Vec<DecodedHeap>,
    chunks: Arc<ToastChunks>,
) -> Result<u64, String> {
    let mut routed = 0u64;
    for mut heap in heaps {
        detoast_heap(&mut heap, &chunks, &ctx.catalog)
            .await
            .map_err(|e| e.to_string())?;
        let rel = match ctx.catalog.relation_at(heap.rfn, heap.source_lsn).await {
            Ok(r) => r,
            // Foreign-DB WAL (physical WAL carries the whole cluster): skip.
            Err(CatalogError::ForeignDatabase(_)) => continue,
            Err(e) => return Err(e.to_string()),
        };
        let mapping = {
            let m = ctx.mapping.read().await;
            match m.get(rel.qualified_name.as_ref()) {
                Some(v) => Arc::new(v.clone()),
                // Unmapped relation: skip (not part of any destination).
                None => continue,
            }
        };
        let mut committed = CommittedTuple {
            decoded: heap,
            commit_ts,
            commit_lsn,
        };
        if let Some(oracle) = &ctx.oracle {
            // Mirror OracleObserver::on_tuple — resolve PgPending columns via
            // shadow PG's extension, then fire the 1-in-N validator probe.
            if let Some(t) = committed.decoded.new.as_mut() {
                resolve_pending_tuple(oracle, &mut t.columns).await;
            }
            if let Some(t) = committed.decoded.old.as_mut() {
                resolve_pending_tuple(oracle, &mut t.columns).await;
            }
            if let Some(t) = committed.decoded.new.as_ref() {
                maybe_validate_tuple(oracle, &t.columns).await;
            }
        }
        ctx.msg_tx
            .send(BatcherMsg::Row(RoutedRow {
                seq,
                rel,
                mapping,
                committed,
            }))
            .await
            .map_err(|_| "batcher channel closed".to_string())?;
        routed += 1;
    }
    Ok(routed)
}

/// Spawn `m` decode workers draining `jobs`. Each reports `Placed{seq, R}`
/// on success; a decode error is fatal (a never-placed seq would pin the
/// watermark forever).
pub fn spawn_pool(
    m: usize,
    ctx: DecodeCtx,
    jobs: mpmc::Receiver<DecodeJob>,
    ack: AckHandle,
    fatal: Fatal,
) -> Vec<JoinHandle<()>> {
    let mut handles = Vec::with_capacity(m.max(1));
    for _ in 0..m.max(1) {
        let ctx = ctx.clone();
        let jobs = jobs.clone();
        let ack = ack.clone();
        let fatal = fatal.clone();
        handles.push(tokio::spawn(async move {
            while let Some(job) = jobs.recv().await {
                let seq = job.seq;
                match decode_and_route(
                    &ctx,
                    seq,
                    job.commit_ts,
                    job.commit_lsn,
                    job.heaps,
                    job.chunks,
                )
                .await
                {
                    Ok(rows) => ack.placed(seq, rows),
                    Err(e) => {
                        fatal.set(format!("decode: {e}"));
                        break;
                    }
                }
            }
        }));
    }
    handles
}
