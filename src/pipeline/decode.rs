//! Decode pool — the CPU/IO-parallel stage.
//!
//! Each worker pulls a [`DecodeJob`] and per heap detoasts, resolves
//! relation → mapping → table, runs oracle `PgPending` resolution + sampled
//! validation, and routes to the
//! [`InsertBatcher`](crate::pipeline::batcher). After the xact's last row it
//! reports `Placed{seq, rows}`.
//!
//! Out-of-order completion across workers is fine: rows carry `source_lsn`
//! as `_lsn`, so `ReplacingMergeTree(_lsn)` converges per PK
//! (`project_walshadow_eventual_consistency`). At M=1 dispatch order (hence
//! per-table WAL order) is preserved.

use std::collections::hash_map::Entry;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use tokio::sync::{Mutex, mpsc};
use tokio::task::JoinHandle;
use walrus::pg::walparser::RelFileNode;

use crate::ch_emitter::{EmitterStats, MappingHandle, TableMapping};
use crate::heap_decoder::{CommittedTuple, DecodedHeap};
use crate::oracle::{Oracle, maybe_validate_tuple, resolve_pending_tuple};
use crate::pipeline::Fatal;
use crate::pipeline::ack::AckHandle;
use crate::pipeline::batcher::{BatcherMsg, RoutedRow};
use crate::shadow_catalog::{CatalogError, RelDescriptor, ShadowCatalog};
use crate::toast::ToastResolver;
use crate::xact_buffer::detoast_heap;

/// Caches `RelFileNode → (Postgres descriptor, ClickHouse mapping)` so the pool
/// skips the shared catalog lock after the first lookup. Flushed when the
/// catalog's invalidation epoch bumps (DDL).
pub struct RelCache<V> {
    epoch: Option<Arc<AtomicU64>>,
    seen_epoch: u64,
    map: HashMap<RelFileNode, V>,
}

impl<V> RelCache<V> {
    pub fn new(epoch: Option<Arc<AtomicU64>>) -> Self {
        let seen_epoch = epoch
            .as_ref()
            .map(|e| e.load(Ordering::Acquire))
            .unwrap_or(0);
        Self {
            epoch,
            seen_epoch,
            map: HashMap::new(),
        }
    }

    /// Flush if the catalog's invalidation epoch advanced (DDL). Called once
    /// per job — a cheap lock-free atomic load in steady state.
    pub fn refresh(&mut self) {
        if let Some(e) = &self.epoch {
            let cur = e.load(Ordering::Acquire);
            if cur != self.seen_epoch {
                self.seen_epoch = cur;
                self.map.clear();
            }
        }
    }

    /// Without an epoch handle there's no DDL signal, so entries could never be
    /// invalidated — callers must not memoize.
    pub fn enabled(&self) -> bool {
        self.epoch.is_some()
    }

    pub fn get(&self, rfn: RelFileNode) -> Option<&V> {
        self.map.get(&rfn)
    }

    pub fn insert(&mut self, rfn: RelFileNode, value: V) {
        self.map.insert(rfn, value);
    }
}

/// Reassembled-TOAST chunk map: `(toast_relid, value_id) -> seq -> bytes`.
pub type ToastChunks = HashMap<(u32, u32), BTreeMap<u32, Vec<u8>>>;

/// `chunks` is `Arc` so the barrier coordinator can dispatch several data
/// segments of one xact sharing the same TOAST chunk map without cloning.
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
    /// Shared FIFO `BatcherMsg` channel: a chunk enqueues as one ordered item
    /// so a barrier `FlushAll` can't overtake it.
    pub msg_tx: mpsc::Sender<BatcherMsg>,
    /// Decode bumps `foreign_db_rows_skipped` / `unsupported_relations` on the
    /// skip arms so the parallel path keeps those metrics live.
    pub stats: Arc<EmitterStats>,
    /// TOAST chunk store + miss policy for values absent from this xact's
    /// in-memory chunk map (pre-window re-emits).
    pub resolver: ToastResolver,
    /// Row cap before a mid-loop chunk route; defaults to [`DECODE_CHUNK_ROWS`].
    pub chunk_rows: usize,
}

/// Rows coalesced before one [`BatcherMsg::Rows`] send.
pub const DECODE_CHUNK_ROWS: usize = 1024;

/// Byte half of the dual trigger with [`DECODE_CHUNK_ROWS`]. Bounds the
/// channel item for fat detoasted rows that would pin many MiB before the row
/// cap fires; above the ~100 KiB an ordinary row-cap chunk reaches, so
/// steady-state coalescing is unchanged.
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
pub async fn decode_and_route(
    ctx: &DecodeCtx,
    seq: u64,
    commit_ts: i64,
    commit_lsn: u64,
    heaps: Vec<DecodedHeap>,
    chunks: Arc<ToastChunks>,
    cache: &mut RelCache<(Arc<RelDescriptor>, Arc<TableMapping>)>,
) -> Result<u64, String> {
    cache.refresh();
    let mut routed = 0u64;
    let mut buf: Vec<RoutedRow> = Vec::new();
    let mut buf_bytes = 0usize;
    for mut heap in heaps {
        detoast_heap(&mut heap, &chunks, &ctx.catalog, true, &ctx.resolver)
            .await
            .map_err(|e| e.to_string())?;
        // Cache hit: no shared catalog lock, no mapping read. Skip/error arms
        // are never cached, so `foreign_db_rows_skipped`/`unsupported_relations`
        // still count per row.
        let (rel, mapping) = match cache.map.entry(heap.rfn) {
            Entry::Occupied(e) => e.get().clone(),
            Entry::Vacant(slot) => {
                let rel = match crate::shadow_catalog::resolve_at_pooled(
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
                    crate::pipeline::lookup_mapping(&ctx.mapping, &rel.rel_name, &ctx.stats).await
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
            // Mirror OracleObserver::on_tuple: resolve PgPending via shadow PG
            // extension, then fire the 1-in-N validator probe
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
