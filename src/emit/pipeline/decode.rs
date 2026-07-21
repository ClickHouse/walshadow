//! Decode pool — the CPU/IO-parallel stage.
//!
//! Each worker pulls a [`DecodeJob`] and per heap detoasts, resolves
//! relation → mapping → table, runs oracle `PgPending` resolution + sampled
//! validation, and routes to the
//! [`InsertBatcher`](crate::emit::pipeline::batcher). After the xact's last row it
//! reports `Placed{seq, rows}`.
//!
//! Out-of-order completion across workers is fine: rows carry `source_lsn`
//! as `_lsn`, so `ReplacingMergeTree(_lsn)` converges per PK
//! (`project_walshadow_eventual_consistency`). At M=1 dispatch order (hence
//! per-table WAL order) is preserved.

use std::collections::hash_map::Entry;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use tokio::sync::{Mutex, mpsc};
use tokio::task::JoinHandle;

use crate::catalog::shadow_catalog::{CatalogError, ShadowCatalog};
use crate::decode::heap_decoder::{CommittedTuple, DecodedHeap};
use crate::emit::ch_emitter::EmitterStats;
use crate::emit::pipeline::Fatal;
use crate::emit::pipeline::ack::AckHandle;
use crate::emit::pipeline::batcher::{BatcherMsg, RoutedRow};
use crate::mapping::{MappingHandle, TableMapping};
use crate::ops::oracle::{Oracle, maybe_validate_tuple, resolve_pending_tuple};
use crate::schema::RelDescriptor;
use crate::toast::{ChunkRefMap, ToastResolver};
use crate::xact::xact_buffer::{ChunkGeneration, RelCache, detoast_heap};

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
    pub heaps: Vec<DecodedHeap>,
    pub chunks: Vec<Arc<ChunkGeneration>>,
    /// Slice admission permit; shares ride every routed row through the
    /// batcher to the in-flight insert, releasing post-insert-ack
    pub permit: Option<Arc<crate::budget::MemoryPermit>>,
}

/// Shared dependencies a decode worker (or the barrier's inline data path)
/// needs to turn heaps into routed rows.
#[derive(Clone)]
pub struct DecodeCtx {
    pub catalog: Arc<Mutex<ShadowCatalog>>,
    pub mapping: MappingHandle,
    pub oracle: Option<Arc<Oracle>>,
    /// Shared FIFO `BatcherMsg` channel: a chunk enqueues as one ordered item
    /// so a barrier `FlushAll` can't overtake it.
    pub msg_tx: mpsc::Sender<BatcherMsg>,
    /// Decode bumps `foreign_db_rows_skipped` / `unsupported_relations` on the
    /// skip arms so the parallel path keeps those metrics live.
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

/// Detoast, resolve, and route every heap of one xact. Returns rows routed
/// (the `R` the collector compares against). Used by decode workers and the
/// barrier's data segments.
///
/// The buffer is flushed before returning so every routed row is on the
/// channel by the time the caller reports `Placed` (watermark invariant).
/// Foreign-database and unmapped relations are skipped (bumping
/// `foreign_db_rows_skipped` / `unsupported_relations`); any other catalog
/// error poisons the stream.
#[allow(clippy::too_many_arguments)]
pub async fn decode_and_route(
    ctx: &DecodeCtx,
    seq: u64,
    commit_ts: i64,
    commit_lsn: u64,
    heaps: Vec<DecodedHeap>,
    chunks: Vec<Arc<ChunkGeneration>>,
    permit: Option<Arc<crate::budget::MemoryPermit>>,
    cache: &mut RelCache<(Arc<RelDescriptor>, Arc<TableMapping>)>,
) -> Result<u64, String> {
    cache.refresh();
    let ref_maps: Vec<&ChunkRefMap> = chunks.iter().map(|g| g.map()).collect();
    // One spool per xact; generations sealed before spooling carry None
    let spool = chunks.iter().find_map(|g| g.spool());
    let mut routed = 0u64;
    let mut buf: Vec<RoutedRow> = Vec::new();
    let mut buf_bytes = 0usize;
    for mut heap in heaps {
        let value_permit = detoast_heap(
            &mut heap,
            spool,
            &ref_maps,
            &ctx.catalog,
            true,
            &ctx.resolver,
        )
        .await
        .map_err(|e| e.to_string())?
        .map(Arc::new);
        // Cache hit: no shared catalog lock, no mapping read. Skip/error arms
        // are never cached, so `foreign_db_rows_skipped`/`unsupported_relations`
        // still count per row.
        let (rel, mapping) = match cache.entry(heap.rfn) {
            Entry::Occupied(e) => e.get().clone(),
            Entry::Vacant(slot) => {
                let rel = match crate::catalog::shadow_catalog::resolve_at_pooled(
                    &ctx.catalog,
                    heap.rfn,
                    heap.source_lsn,
                )
                .await
                {
                    Ok(r) => r,
                    // Physical WAL carries the whole cluster; skip foreign-DB rows
                    Err(CatalogError::ForeignDatabase(_)) => {
                        ctx.stats
                            .foreign_db_rows_skipped
                            .fetch_add(1, Ordering::Relaxed);
                        continue;
                    }
                    Err(e) => return Err(e.to_string()),
                };
                let Some(mapping) =
                    crate::emit::pipeline::lookup_mapping(&ctx.mapping, &rel.rel_name, &ctx.stats)
                        .await
                else {
                    continue;
                };
                slot.insert((rel, mapping)).clone()
            }
        };
        let mut committed = CommittedTuple {
            decoded: heap,
            commit_ts,
            commit_lsn,
        };
        if let Some(oracle) = &ctx.oracle {
            // Resolve PgPending via shadow PG extension, then fire the 1-in-N
            // validator probe
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
        buf_bytes += committed.decoded.approx_bytes();
        buf.push(RoutedRow {
            seq,
            rel,
            mapping,
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
            // Per-worker descriptor cache; epoch handle read once at startup.
            let epoch = ctx.catalog.lock().await.invalidation_epoch_handle();
            let mut cache = RelCache::new(epoch);
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
                    &mut cache,
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
