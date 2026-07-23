//! Decode pool — the CPU/IO-parallel stage.
//!
//! Each worker pulls a [`DecodeJob`] and per heap detoasts, runs oracle
//! `PgPending` resolution, and routes under the attached
//! [`RouteSnapshot`](crate::emit::route::RouteSnapshot) to the
//! [`InsertBatcher`](crate::emit::pipeline::batcher). After the xact's last row it
//! reports `Placed{seq, rows}`.
//!
//! Out-of-order completion across workers is fine: rows carry `source_lsn`
//! as `_lsn`, so `ReplacingMergeTree(_lsn)` converges per PK
//! (`project_walshadow_eventual_consistency`). At M=1 dispatch order (hence
//! per-table WAL order) is preserved.

use std::sync::Arc;
use std::sync::atomic::Ordering;

use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::decode::heap_decoder::CommittedTuple;
use crate::emit::ch_emitter::EmitterStats;
use crate::emit::pipeline::Fatal;
use crate::emit::pipeline::ack::AckHandle;
use crate::emit::pipeline::batcher::{BatcherMsg, RoutedRow};
use crate::emit::route::RoutedHeap;
use crate::ops::oracle::{Oracle, resolve_pending_tuple};
use crate::toast::{ChunkRefMap, ToastResolver};
use crate::xact::xact_buffer::{ChunkGeneration, detoast_heap};

/// `chunks` holds the xact's chunk-map generations (oldest first), each
/// immutable once sealed by the drain: batches / barrier segments of one
/// xact share payloads via `Arc` while later slices are still loading. A
/// heap's referenced value lives in exactly one generation (chunk WAL
/// precedes referrer). Each generation carries its resident-gauge share,
/// released when the last holder drops.
pub struct DecodeJob {
    pub seq: u64,
    pub commit_ts: i64,
    pub commit_lsn: u64,
    pub heaps: Vec<RoutedHeap>,
    pub chunks: Vec<Arc<ChunkGeneration>>,
    /// Slice admission permit; shares ride every routed row through the
    /// batcher to the in-flight insert, releasing post-insert-ack
    pub permit: Option<Arc<crate::budget::MemoryPermit>>,
}

/// Shared dependencies a decode worker needs to turn routed heaps into
/// batcher rows. Descriptor and route ride each heap's envelope; nothing
/// here resolves catalog or mapping state.
#[derive(Clone)]
pub struct DecodeCtx {
    pub oracle: Option<Arc<Oracle>>,
    /// Shared FIFO `BatcherMsg` channel: a chunk enqueues as one ordered item
    /// so a barrier `FlushAll` can't overtake it.
    pub msg_tx: mpsc::Sender<BatcherMsg>,
    /// Throughput counters (`decode_jobs_in` / `decode_rows_out`).
    pub stats: Arc<EmitterStats>,
    /// TOAST chunk store + miss policy for values absent from this xact's
    /// in-memory chunk map (pre-window re-emits).
    pub resolver: ToastResolver,
    /// Row cap before a mid-loop chunk route; defaults to emitter configuration.
    pub chunk_rows: usize,
}

/// Byte half of dual trigger with configured row cap. Bounds channel item for
/// fat detoasted rows that would pin many MiB before row cap fires; above
/// ~100 KiB an ordinary row-cap chunk reaches, so steady-state coalescing is
/// unchanged.
pub const DECODE_CHUNK_BYTES: usize = 4 << 20;

/// `Err` means the batcher channel closed (tail tripped fatal).
async fn route_chunk(
    msg_tx: &mpsc::Sender<BatcherMsg>,
    rows: Vec<RoutedRow>,
) -> Result<(), String> {
    msg_tx
        .send(BatcherMsg::Rows(rows))
        .await
        .map_err(|_| "batcher channel closed".to_string())
}

/// Detoast and route every heap of one xact under its attached route.
/// Returns rows routed (the `R` the collector compares against). Used by
/// decode workers and the barrier's data segments.
///
/// The buffer is flushed before returning so every routed row is on the
/// channel by the time the caller reports `Placed` (watermark invariant).
/// Routes attach at the reorder coordinator's planning step; `route = None`
/// (deterministically unmapped, counted there) discards here.
#[allow(clippy::too_many_arguments)]
pub async fn decode_and_route(
    ctx: &DecodeCtx,
    seq: u64,
    commit_ts: i64,
    commit_lsn: u64,
    heaps: Vec<RoutedHeap>,
    chunks: Vec<Arc<ChunkGeneration>>,
    permit: Option<Arc<crate::budget::MemoryPermit>>,
) -> Result<u64, String> {
    let ref_maps: Vec<&ChunkRefMap> = chunks.iter().map(|g| g.map()).collect();
    // One spool per xact; generations sealed before spooling carry None
    let spool = chunks.iter().find_map(|g| g.spool());
    let mut routed = 0u64;
    let mut buf: Vec<RoutedRow> = Vec::new();
    let mut buf_bytes = 0usize;
    for envelope in heaps {
        // Discard precedes detoast: unrouted values never hit the resolver
        let Some(route) = envelope.route else {
            continue;
        };
        let mut heap = envelope.described;
        let value_permit = detoast_heap(&mut heap, spool, &ref_maps, &ctx.resolver)
            .await
            .map_err(|e| e.to_string())?
            .map(Arc::new);
        let rel = heap.descriptor.clone();
        let mut committed = CommittedTuple {
            decoded: heap.decoded,
            commit_ts,
            commit_lsn,
        };
        if let Some(oracle) = &ctx.oracle {
            // Resolve PgPending via shadow PG extension
            if let Some(t) = committed.decoded.new.as_mut() {
                resolve_pending_tuple(oracle, &mut t.columns).await;
            }
            if let Some(t) = committed.decoded.old.as_mut() {
                resolve_pending_tuple(oracle, &mut t.columns).await;
            }
        }
        buf_bytes += committed.decoded.approx_bytes();
        buf.push(RoutedRow {
            seq,
            rel,
            route,
            committed,
            permit: permit.clone(),
            value_permit,
        });
        routed += 1;
        if buf.len() >= ctx.chunk_rows || buf_bytes >= DECODE_CHUNK_BYTES {
            route_chunk(&ctx.msg_tx, std::mem::take(&mut buf)).await?;
            buf_bytes = 0;
        }
    }
    if !buf.is_empty() {
        route_chunk(&ctx.msg_tx, buf).await?;
    }
    ctx.stats
        .decode_rows_out
        .fetch_add(routed, Ordering::Relaxed);
    Ok(routed)
}

/// Spawn `m` decode workers draining `jobs`. A decode error is fatal: a
/// never-placed seq would pin the watermark forever.
pub fn spawn_pool(
    m: usize,
    ctx: DecodeCtx,
    jobs: async_channel::Receiver<DecodeJob>,
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
            while let Ok(job) = jobs.recv().await {
                ctx.stats.decode_jobs_in.fetch_add(1, Ordering::Relaxed);
                let seq = job.seq;
                match decode_and_route(
                    &ctx,
                    seq,
                    job.commit_ts,
                    job.commit_lsn,
                    job.heaps,
                    job.chunks,
                    job.permit,
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
