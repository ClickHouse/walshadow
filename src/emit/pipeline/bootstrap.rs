//! Bootstrap page-walk producer
//!
//! Keep `start_lsn` as row version and sequence position. Persist TOAST mirrors
//! before resolving deferred values, which may read past `start_lsn`. Wait for
//! synthetic sequences to become durable before advancing resume LSN to backup end

use std::collections::hash_map::Entry;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use tokio::sync::mpsc;
use walrus::pg::walparser::RelFileNode;

use crate::backfill::backup_page_walk::{BackfillTuple, CatalogMap, TupleData, toast_row};
use crate::backfill::copy_backfill::copy_slot;
use crate::backfill::spool::{DeferredReader, DeferredSpool, Replayable, SpoolMark};
use crate::backfill::visibility_pending::metadata_patch;
use crate::backfill::wal_replay::HeapTid;
use crate::backfill::walk_barrier::{WALK_CHECKPOINT_PERIOD, WalkBarrier};
use crate::budget::{MemoryBudget, MemoryPermit};
use crate::ch::EmitterError;
use crate::config::ResolvedConfig;
use crate::decode::datum::{Datum, DatumRef};
use crate::decode::heap_decoder::{ToastPointer, TupleView, view_block_data_into};
use crate::emit::ch_emitter::{Append, EmitterStats, OP_INSERT, RowStamp, TableEncoder};
use crate::emit::pipeline::ack::{AckHandle, OpenSeq, Publish, SeqAlloc};
use crate::emit::pipeline::batcher::{BatcherClosed, BatcherMsg, DECODE_CHUNK_BYTES, RowTable};
use crate::emit::route::{FrozenRoutes, RouteSnapshot, RowPolicy, freeze_routes};
use crate::mapping::{MappingSnapshot, TableMapping};
use crate::schema::{RelDescriptor, RelName};
use crate::ticker::Ticker;
use crate::toast::{
    ChunkStoreError, FetchedValue, ToastResolver, check_value_caps, detoasted_sized, finish_value,
    pointer_extsize,
};
use crate::xact::xact_buffer::{MissPolicy, StoreMiss, fill_store_miss};
use foldhash::{HashMap, HashMapExt, HashSet};

/// Window replay coverage for deferred TOAST reads. Row versions and sequence
/// positions remain at `start_lsn`
#[derive(Debug, Default)]
pub struct WindowCoverage {
    /// Read mirrors at heap page LSN after replay through backup end.
    /// Disabled for shadow, whose bound is a replay floor
    pub page_bound: bool,
    /// Slots with a tombstone or newer row version emitted by window replay
    pub replaced: HashSet<HeapTid>,
}

fn lookup_bound(tuple: &BackfillTuple, coverage: Option<&WindowCoverage>) -> u64 {
    if coverage.is_some_and(|c| c.page_bound) {
        tuple.source_lsn.max(tuple.page_lsn)
    } else {
        tuple.source_lsn
    }
}

/// Completion frontier for `FlushAll` and resume advance
#[derive(Debug, Default)]
pub struct BootstrapDrainOutcome {
    /// Dense over `[0, next_seq)`
    pub next_seq: u64,
    pub rows_routed: u64,
    /// Referrers a [`Deferral::Handback`] left for the caller's own pass
    pub deferred: Option<DeferredSpool>,
}

/// One holder's share of the deferred-spool gauges
///
/// Greenfield runs a spool per lane against one gauge pair, so each holder
/// reports deltas and releases what it still owns on drop: a failed lane
/// cannot leave its bytes standing, and a finishing lane cannot zero a peer's
struct DeferredFootprint<'a> {
    stats: &'a EmitterStats,
    resident: u64,
    spooled: u64,
}

impl<'a> DeferredFootprint<'a> {
    fn new(stats: &'a EmitterStats) -> Self {
        Self {
            stats,
            resident: 0,
            spooled: 0,
        }
    }

    /// Take over bytes a lane published, so replay releases them
    fn adopt(stats: &'a EmitterStats, spool: &Replayable) -> Self {
        Self {
            stats,
            resident: spool.resident_bytes() as u64,
            spooled: spool.spooled_bytes(),
        }
    }

    fn publish(&mut self, spool: &DeferredSpool) {
        shift(
            &self.stats.bootstrap_deferred_bytes,
            &mut self.resident,
            spool.resident_bytes() as u64,
        );
        shift(
            &self.stats.bootstrap_deferred_spool_bytes,
            &mut self.spooled,
            spool.spooled_bytes(),
        );
    }

    /// Spool outlives this holder: whoever takes it owns the bytes
    fn hand_off(mut self) {
        self.resident = 0;
        self.spooled = 0;
    }
}

impl Drop for DeferredFootprint<'_> {
    fn drop(&mut self) {
        self.stats
            .bootstrap_deferred_bytes
            .fetch_sub(self.resident, Ordering::Relaxed);
        self.stats
            .bootstrap_deferred_spool_bytes
            .fetch_sub(self.spooled, Ordering::Relaxed);
    }
}

struct DeferredReplayProgress<'a> {
    stats: &'a EmitterStats,
    total: u64,
    processed: u64,
}

impl<'a> DeferredReplayProgress<'a> {
    fn new(stats: &'a EmitterStats, total: u64) -> Self {
        stats
            .bootstrap_deferred_replay_bytes
            .fetch_add(total, Ordering::Relaxed);
        stats
            .bootstrap_fill_bytes
            .fetch_add(total, Ordering::Relaxed);
        Self {
            stats,
            total,
            processed: 0,
        }
    }

    fn advance(&mut self, remaining: u64) {
        let done = self.total - remaining;
        self.stats
            .bootstrap_filled_bytes
            .fetch_add(done.saturating_sub(self.processed), Ordering::Relaxed);
        shift(
            &self.stats.bootstrap_deferred_replayed_bytes,
            &mut self.processed,
            self.total - remaining,
        );
    }
}

impl Drop for DeferredReplayProgress<'_> {
    fn drop(&mut self) {
        self.stats
            .bootstrap_deferred_replayed_bytes
            .fetch_sub(self.processed, Ordering::Relaxed);
        self.stats
            .bootstrap_deferred_replay_bytes
            .fetch_sub(self.total, Ordering::Relaxed);
    }
}

/// Move a shared gauge from what this holder reported to what it holds now
fn shift(gauge: &AtomicU64, held: &mut u64, now: u64) {
    if now >= *held {
        gauge.fetch_add(now - *held, Ordering::Relaxed);
    } else {
        gauge.fetch_sub(*held - now, Ordering::Relaxed);
    }
    *held = now;
}

/// Referrers whose TOAST files the walk had not reached when their row passed
pub enum Deferral {
    /// Detoasted input: an external pointer is a bug
    Rejected,
    /// Resolve at drain end, past every chunk put this task made
    Local(DeferredSpool),
    /// Hand back through the outcome: a sibling lane may still be putting
    /// chunks, so only the caller knows when every file landed
    Handback(DeferredSpool),
}

/// Drain baseline tuples into shared insert tail
#[allow(clippy::too_many_arguments)]
pub async fn drain(
    mut rx: mpsc::Receiver<Vec<BackfillTuple>>,
    catalog: CatalogMap,
    routes: FrozenRoutes,
    msg_tx: mpsc::Sender<BatcherMsg>,
    ack: AckHandle,
    stats: Arc<EmitterStats>,
    resolver: ToastResolver,
    deferral: Deferral,
    skip_initial: HashSet<RelName>,
    barrier: Option<Arc<WalkBarrier>>,
) -> Result<BootstrapDrainOutcome, String> {
    let (mut deferred, handback) = match deferral {
        Deferral::Rejected => (None, false),
        Deferral::Local(spool) => (Some(spool), false),
        Deferral::Handback(spool) => (Some(spool), true),
    };
    let mut footprint = DeferredFootprint::new(&stats);
    let mut rows_routed = 0;
    let mut open: Option<Run> = None;
    let mut puts = resolver.chunk_puts();
    let mut out = LoadOut::new(&msg_tx, &ack, 0);
    let mut consumed = 0u64;
    let mut ticker = Ticker::new(
        barrier
            .as_ref()
            .map_or(WALK_CHECKPOINT_PERIOD, |b| b.period()),
    );
    loop {
        if rx.is_empty() {
            out.send().await?;
        }
        let Some(batch) = rx.recv().await else {
            break;
        };
        consumed += batch.len() as u64;
        for tuple in batch {
            let rfn = tuple.rfn;
            let run = match &mut open {
                Some(run) if run.rfn == rfn => run,
                slot => {
                    if let Some(prev) = slot.take() {
                        out.place(prev.seq, prev.rows);
                    }
                    slot.insert(Run {
                        rfn,
                        seq: out.seqs.open(tuple.source_lsn, None, Publish::Commit),
                        rows: 0,
                        target: Target::resolve(&catalog, &routes, &skip_initial, rfn),
                    })
                }
            };
            let (rel, route, varlena) = match &run.target {
                Target::Routed {
                    rel,
                    route,
                    varlena,
                } => (rel, route, *varlena),
                Target::Unsupported => {
                    stats.unsupported_relations.fetch_add(1, Ordering::Relaxed);
                    continue;
                }
                Target::Skipped => continue,
                Target::Toast(toast) => {
                    // Weakest evidence for a TID: a backup copy can be
                    // torn, so any WAL-sourced row for the same TID outranks it
                    if let TupleData::Heap(data) = &tuple.data
                        && let Some(row) = toast_row(data, toast, tuple.blkno, tuple.offnum, 0)
                    {
                        debug_assert_ne!(tuple.offnum, 0, "walked toast tuple without TID");
                        out.send().await?;
                        puts.push(row).await.map_err(put_err)?;
                    }
                    continue;
                }
            };

            let sites = if varlena && tuple.has_external() {
                let view = tuple.view(rel).map_err(|e| decode_err(rel, e))?;
                view.as_ref()
                    .map_or_else(Vec::new, |v| mapped_pointers(v, &route.mapping))
            } else {
                Vec::new()
            };
            let mut patch = Vec::new();
            if !sites.is_empty() {
                // Defer until page walk has populated or started backing store
                if !resolver.fill_on_miss() {
                    let spool = deferred.as_mut().ok_or_else(|| {
                        format!("bootstrap: undeferrable external value in {}", rel.rel_name)
                    })?;
                    spool
                        .push(tuple)
                        .await
                        .map_err(|e| format!("bootstrap: deferred spool: {e}"))?;
                    footprint.publish(spool);
                    continue;
                }
                patch = fill_toast(sites, &resolver)?;
            }
            out.push(run.seq.seq(), rel, route, tuple, patch).await?;
            run.rows += 1;
            rows_routed += 1;
        }
        // Resumable walk: close the seq space and fsync the spool so the
        // checkpointer can name every tuple consumed so far durable
        if let Some(b) = &barrier
            && ticker.fire()
        {
            // Durable files must include their chunks
            puts.flush().await.map_err(put_err)?;
            if let Some(run) = open.take() {
                out.place(run.seq, run.rows);
            }
            out.send().await?;
            let mut mark = SpoolMark::default();
            if let Some(spool) = deferred.as_mut() {
                mark = spool
                    .checkpoint()
                    .await
                    .map_err(|e| format!("bootstrap: deferred spool checkpoint: {e}"))?;
                footprint.publish(spool);
            }
            b.publish_drain(consumed, out.seqs.next(), mark).await;
        }
    }
    if let Some(run) = open.take() {
        out.place(run.seq, run.rows);
    }
    let next_seq = out.seqs.next();
    out.send().await?;

    puts.flush().await.map_err(put_err)?;

    match deferred.take().filter(|s| s.records() > 0) {
        Some(spool) if handback => {
            footprint.hand_off();
            Ok(BootstrapDrainOutcome {
                next_seq,
                rows_routed,
                deferred: Some(spool),
            })
        }
        Some(spool) => {
            footprint.hand_off();
            let resolved = resolve_spooled(
                spool.into(),
                &routes,
                &catalog,
                &msg_tx,
                &ack,
                &stats,
                &resolver,
                next_seq,
                None,
                None,
            )
            .await?;
            Ok(BootstrapDrainOutcome {
                next_seq: resolved.next_seq,
                rows_routed: rows_routed + resolved.rows_routed,
                deferred: None,
            })
        }
        None => Ok(BootstrapDrainOutcome {
            next_seq,
            rows_routed,
            deferred: None,
        }),
    }
}

/// Resolve referrers a walk handed back, against the chunk store
///
/// Split from [`drain`] so a multi-lane caller resolves once every lane
/// flushed its puts: one lane reaching its end proves nothing about a
/// sibling's chunk files
#[allow(clippy::too_many_arguments)]
pub async fn drain_deferred(
    spool: Replayable,
    catalog: &CatalogMap,
    mapping: &MappingSnapshot,
    msg_tx: &mpsc::Sender<BatcherMsg>,
    ack: &AckHandle,
    stats: &EmitterStats,
    resolver: &ToastResolver,
    row_policy: &RowPolicy,
    config: Option<&ResolvedConfig>,
    first_seq: u64,
    checkpoint: Option<ReplayCheckpoint<'_>>,
    coverage: Option<&WindowCoverage>,
) -> Result<BootstrapDrainOutcome, String> {
    let routes = freeze_routes(mapping, config, row_policy);
    resolve_spooled(
        spool, &routes, catalog, msg_tx, ack, stats, resolver, first_seq, checkpoint, coverage,
    )
    .await
}

pub struct ReplayCheckpoint<'a> {
    pub state: &'a mut crate::backfill::backup_checkpoint::BackupCheckpoint,
    pub dir: &'a std::path::Path,
    pub tail: &'a crate::emit::pipeline::tail::OwnedTail,
}

/// Route spooled referrers under one trailing seq, registered on the first
/// row that routes so an all-unmapped spool leaves no seq to prove
#[allow(clippy::too_many_arguments)]
async fn resolve_spooled(
    spool: Replayable,
    routes: &FrozenRoutes,
    catalog: &CatalogMap,
    msg_tx: &mpsc::Sender<BatcherMsg>,
    ack: &AckHandle,
    stats: &EmitterStats,
    resolver: &ToastResolver,
    first_seq: u64,
    mut checkpoint: Option<ReplayCheckpoint<'_>>,
    coverage: Option<&WindowCoverage>,
) -> Result<BootstrapDrainOutcome, String> {
    tracing::info!(
        target: "walshadow::bootstrap",
        deferred = spool.records(),
        spooled_bytes = spool.spooled_bytes(),
        page_bound = coverage.is_some_and(|c| c.page_bound),
        replaced_slots = coverage.map(|c| c.replaced.len()),
        "resolving deferred TOAST tuples from chunk store",
    );
    let covered_before = stats.bootstrap_replay_covered_rows.load(Ordering::Relaxed);
    // Released on drop, so a failed replay gives its bytes back too
    let _footprint = DeferredFootprint::adopt(stats, &spool);
    let mut seq = None;
    let mut placed = 0u64;
    let total_bytes = spool.spooled_bytes();
    let mut total_placed = 0;
    let mut ticker = Ticker::new(WALK_CHECKPOINT_PERIOD);
    let mut replay = spool
        .into_reader()
        .await
        .map_err(|e| format!("bootstrap: deferred spool seal: {e}"))?;
    let mut progress = DeferredReplayProgress::new(stats, total_bytes);
    progress.advance(replay.remaining_file_bytes());
    let mut out = LoadOut::new(msg_tx, ack, first_seq);
    // Read and fetch the next batch while this one routes: spool reads and
    // inserts otherwise leave the store pool idle
    let mut ready = prepare_batch(&mut replay, routes, catalog, stats, resolver, coverage).await?;
    while let Some(batch) = ready.take() {
        let remaining = replay.remaining_file_bytes();
        let (next, routed) = tokio::join!(
            prepare_batch(&mut replay, routes, catalog, stats, resolver, coverage),
            route_batch(batch, &mut out, &mut seq, &mut placed),
        );
        routed?;
        progress.advance(remaining);
        if let Some(c) = checkpoint.as_mut() {
            if let Some(s) = seq.take() {
                out.place(s, placed);
            }
            total_placed += placed;
            placed = 0;
            if ticker.fire() {
                out.send().await?;
                c.tail.checkpoint(out.seqs.next()).await?;
                c.state.offset = total_bytes - remaining;
                c.state.rows += total_placed;
                total_placed = 0;
                c.state.save(c.dir).await.map_err(|e| e.to_string())?;
            }
        }
        ready = next?;
    }
    let rows_routed = match checkpoint.as_mut() {
        Some(c) => {
            out.send().await?;
            c.tail.checkpoint(out.seqs.next()).await?;
            c.state.offset = total_bytes;
            c.state.rows += total_placed;
            c.state.save(c.dir).await.map_err(|e| e.to_string())?;
            c.state.rows
        }
        // Resumable passes keep the spool until the pass itself publishes
        None => {
            replay
                .finish()
                .await
                .map_err(|e| format!("bootstrap: deferred spool cleanup: {e}"))?;
            placed
        }
    };
    if let Some(s) = seq {
        out.place(s, placed);
    }
    let next_seq = out.seqs.next();
    out.send().await?;
    let covered = stats.bootstrap_replay_covered_rows.load(Ordering::Relaxed) - covered_before;
    if covered > 0 {
        tracing::info!(
            target: "walshadow::bootstrap",
            rows = covered,
            "deferred referrers dropped: their TOAST values were unreadable, and window \
             replay emitted a tombstone or newer version for each walked tuple",
        );
    }
    Ok(BootstrapDrainOutcome {
        next_seq,
        rows_routed,
        deferred: None,
    })
}

fn closed(e: BatcherClosed) -> String {
    format!("bootstrap: {e}")
}

fn decode_err(rel: &RelDescriptor, e: impl std::fmt::Display) -> String {
    format!("bootstrap: {} tuple decode: {e}", rel.rel_name)
}

/// Rows per load slab; the byte limit bounds wide rows
const LOAD_SEGMENT_ROWS: usize = 65536;

/// Slab bytes a lane queues ahead of its batcher: one encoding, one waiting
const LOAD_QUEUE_BYTES: usize = 2 * DECODE_CHUNK_BYTES;

/// Routed load rows, encoded by the batcher straight into its open blocks
#[derive(Default)]
pub struct Slab {
    pub(crate) tables: Vec<RowTable>,
    pub(crate) rows: Vec<LoadRow>,
    bytes: usize,
    /// Leaf permit riding these rows to insert ack
    pub(crate) permit: Option<Arc<MemoryPermit>>,
    /// Lane queue share, released once the batcher encoded the slab
    queued: Option<MemoryPermit>,
}

pub(crate) struct LoadRow {
    pub(crate) seq: u64,
    /// Index into containing slab's `tables`
    pub(crate) table: u32,
    tuple: BackfillTuple,
    /// Resolved external values
    patch: Vec<(usize, Datum)>,
}

impl Slab {
    fn push(
        &mut self,
        seq: u64,
        rel: &Arc<RelDescriptor>,
        route: &Arc<RouteSnapshot>,
        tuple: BackfillTuple,
        patch: Vec<(usize, Datum)>,
    ) {
        let table = match self
            .tables
            .iter()
            .rposition(|t| Arc::ptr_eq(&t.rel, rel) && Arc::ptr_eq(&t.route, route))
        {
            Some(i) => i,
            None => {
                self.tables.push(RowTable {
                    rel: rel.clone(),
                    route: route.clone(),
                });
                self.tables.len() - 1
            }
        };
        self.bytes += crate::backfill::spool::approx_bytes(&tuple)
            + patch.iter().map(|(_, v)| v.approx_bytes()).sum::<usize>();
        self.rows.push(LoadRow {
            seq,
            table: table as u32,
            tuple,
            patch,
        });
    }

    fn due(&self) -> bool {
        self.rows.len() >= LOAD_SEGMENT_ROWS || self.bytes >= DECODE_CHUNK_BYTES
    }

    /// `(seq, rows)` per seq run
    pub(crate) fn seq_rows(&self) -> Vec<(u64, u64)> {
        let mut out: Vec<(u64, u64)> = Vec::new();
        for row in &self.rows {
            match out.last_mut() {
                Some((s, n)) if *s == row.seq => *n += 1,
                _ => out.push((row.seq, 1)),
            }
        }
        out
    }

    /// Heap rows of `cells` under one seq
    #[cfg(test)]
    pub(crate) fn of_cells(seq: u64, t: &RowTable, rows: &[Vec<Option<Datum>>]) -> Self {
        let mut slab = Self::default();
        for cells in rows {
            let tuple = BackfillTuple {
                rfn: t.rel.rfn,
                xid: 0,
                xmax: 0,
                infomask: 0,
                source_lsn: 0,
                page_lsn: 0,
                blkno: 0,
                offnum: 1,
                data: TupleData::from_cells(cells),
                pending: None,
            };
            slab.push(seq, &t.rel, &t.route, tuple, Vec::new());
        }
        slab
    }

    /// Each row encoded into its own chunk, under its seq
    #[cfg(test)]
    pub(crate) fn encoded(mut self) -> Vec<(u64, crate::emit::ch_emitter::EncodedChunk)> {
        use crate::emit::pipeline::plan_spool::ChunkBuilder;
        let mut out = Vec::new();
        let mut view = TupleView::default();
        for row in &mut self.rows {
            let (t, seq) = (&self.tables[row.table as usize], row.seq);
            let mut chunks = ChunkBuilder::new(usize::MAX, usize::MAX);
            row.encode(&mut view, t, |append| {
                chunks.append((), &t.rel, &t.route, append).unwrap();
                Ok(())
            })
            .unwrap();
            out.extend(chunks.take().into_iter().map(|(_, c)| (seq, c)));
        }
        out
    }

    /// `(seq, mapped cells)` per row
    #[cfg(test)]
    pub(crate) fn cells(self) -> Vec<(u64, Vec<Datum>)> {
        self.encoded()
            .into_iter()
            .flat_map(|(seq, c)| c.cells().into_iter().map(move |cells| (seq, cells)))
            .collect()
    }
}

impl LoadRow {
    /// Encode as an insert at its load version against `t`'s plan, through
    /// `append`, which seals and retries on [`Append::Full`]. `view` is
    /// scratch, refilled per heap row
    pub(crate) fn encode<'r>(
        &'r mut self,
        view: &mut TupleView<'r>,
        t: &RowTable,
        append: impl FnOnce(
            &mut dyn FnMut(&mut TableEncoder) -> Result<Append, EmitterError>,
        ) -> Result<(), String>,
    ) -> Result<(), String> {
        let LoadRow { tuple, patch, .. } = self;
        let tuple: &'r BackfillTuple = tuple;
        let RowTable { rel, route } = t;
        let stamp = RowStamp {
            source_lsn: tuple.source_lsn,
            xid: tuple.xid,
            commit_ts: 0,
            op_code: OP_INSERT,
            leaf: route.partition.as_ref().map(|p| p.leaf),
        };
        let appended = match &tuple.data {
            TupleData::Heap(data) => {
                view_block_data_into(data, rel, view).map_err(|e| decode_err(rel, e))?;
                metadata_patch(tuple, rel, patch);
                append(&mut |enc| enc.append_view(rel, &view.datums, patch, &stamp))
            }
            TupleData::Copy(row) => append(&mut |enc| {
                enc.append_slots(|src| Ok(copy_slot(row, rel, src.index)), &stamp)
            }),
        };
        appended.map_err(|e| format!("bootstrap: encode {}: {e}", rel.rel_name))
    }
}

/// Routed load rows go straight to the batcher, each seq placed after its
/// rows. Router sends its slab before waiting on anything but the batcher
struct LoadOut {
    msg_tx: mpsc::Sender<BatcherMsg>,
    ack: AckHandle,
    seqs: SeqAlloc,
    slab: Slab,
    places: Vec<(OpenSeq, u64)>,
    /// Leaf permit riding rows sent while held
    permit: Option<Arc<MemoryPermit>>,
    /// Bounds slab bytes queued ahead of the batcher
    queue: MemoryBudget,
}

impl LoadOut {
    fn new(msg_tx: &mpsc::Sender<BatcherMsg>, ack: &AckHandle, first_seq: u64) -> Self {
        Self {
            msg_tx: msg_tx.clone(),
            ack: ack.clone(),
            seqs: ack.seqs(first_seq),
            slab: Slab::default(),
            places: Vec::new(),
            permit: None,
            queue: MemoryBudget::new(LOAD_QUEUE_BYTES),
        }
    }

    fn hold(&mut self, permit: Option<Arc<MemoryPermit>>) {
        self.permit = permit;
    }

    /// Queue `tuple` as an insert under `seq`
    async fn push(
        &mut self,
        seq: u64,
        rel: &Arc<RelDescriptor>,
        route: &Arc<RouteSnapshot>,
        tuple: BackfillTuple,
        patch: Vec<(usize, Datum)>,
    ) -> Result<(), String> {
        self.slab.push(seq, rel, route, tuple, patch);
        if self.slab.due() {
            self.send().await?;
        }
        Ok(())
    }

    /// Hand queued rows to the batcher, then place seqs whose rows all went
    async fn send(&mut self) -> Result<(), String> {
        if !self.slab.rows.is_empty() {
            let mut slab = std::mem::take(&mut self.slab);
            slab.permit = self.permit.clone();
            slab.queued = Some(self.queue.acquire(slab.bytes).await);
            self.msg_tx
                .send(BatcherMsg::Load(slab))
                .await
                .map_err(|_| closed(BatcherClosed))?;
        }
        for (seq, rows) in self.places.drain(..) {
            self.ack.place(seq, rows);
        }
        Ok(())
    }

    /// Report `rows` for `seq`, every one already pushed
    fn place(&mut self, seq: OpenSeq, rows: u64) {
        self.places.push((seq, rows));
    }
}

struct Run {
    rfn: RelFileNode,
    seq: OpenSeq,
    rows: u64,
    target: Target,
}

enum Target {
    /// Count each tuple with a missing descriptor or route
    Unsupported,
    /// Already loaded by caller
    Skipped,
    /// Store TOAST chunks of this relation
    Toast(Arc<RelDescriptor>),
    Routed {
        rel: Arc<RelDescriptor>,
        route: Arc<RouteSnapshot>,
        /// Some mapped column is varlena, so a tuple may hold external values
        varlena: bool,
    },
}

impl Target {
    fn resolve(
        catalog: &CatalogMap,
        routes: &FrozenRoutes,
        skip_initial: &HashSet<RelName>,
        rfn: RelFileNode,
    ) -> Self {
        let Some(rel) = catalog.get(rfn.db_node, rfn.rel_node) else {
            return Self::Unsupported;
        };
        if skip_initial.contains(&rel.rel_name) {
            return Self::Skipped;
        }
        if catalog.is_toast(rfn.db_node, rfn.rel_node) {
            return Self::Toast(rel);
        }
        let Some(route) = routes.route(&rel) else {
            return Self::Unsupported;
        };
        let varlena = route.mapping.columns.iter().any(|c| {
            usize::try_from(c.src_attnum as i32 - 1)
                .ok()
                .and_then(|i| rel.attributes.get(i))
                .is_some_and(|a| a.type_len == -1)
        });
        Self::Routed {
            rel,
            route,
            varlena,
        }
    }
}

const REPLAY_BATCH_ROWS: usize = 65536;

const REPLAY_FETCH_BYTES: usize = 64 << 20;

/// `(db_oid, toast_relid, value_id, as-of bound)`. The bound is part of the key:
/// a resumed load tags relations with their own `_lsn`
type ValueKey = (u32, u32, u32, u64);

/// Routed referrers awaiting their values, with the leaf bytes resolving
/// them peaks at
struct ReplayBatch {
    /// Spool preserves relation scan order, so consecutive rows share metadata
    tables: Vec<RowTable>,
    rows: Vec<ReplayRow>,
    /// Leaf permit bytes: what the round's values retain plus its largest
    /// decompression transient
    need: usize,
    /// Inline bytes the held tuples already carry
    resident: usize,
}

struct ReplayRow {
    tuple: BackfillTuple,
    /// Index into containing batch's `tables`
    table: u32,
    pointers: Vec<PointerSite>,
    /// Values settled for external attributes
    patch: Vec<(usize, Datum)>,
    replay_covered: bool,
}

/// Mapped external pointer: its tuple column, its mapping column (the miss
/// message's target name) and the pointer itself
struct PointerSite {
    idx: usize,
    col: usize,
    p: ToastPointer,
}

/// Check only columns routed to ClickHouse
fn mapped_pointers(view: &TupleView<'_>, mapping: &TableMapping) -> Vec<PointerSite> {
    mapping
        .columns
        .iter()
        .enumerate()
        .filter_map(|(col, c)| {
            let idx = usize::try_from(c.src_attnum as i32 - 1).ok()?;
            let DatumRef::External(p) = *view.datums.get(idx)? else {
                return None;
            };
            Some(PointerSite { idx, col, p })
        })
        .collect()
}

/// Settle oversized values as NULL and remove them from fetch list
fn fill_oversize(
    sites: &mut Vec<PointerSite>,
    patch: &mut Vec<(usize, Datum)>,
    resolver: &ToastResolver,
) {
    sites.retain(|site| {
        let filled = resolver.fills_oversize(&site.p);
        if filled {
            patch.push((site.idx, Datum::Null));
        }
        !filled
    });
}

/// Batch whose values are resolved, holding the leaf permit its rows ride
struct ResolvedReplayBatch {
    tables: Vec<RowTable>,
    rows: Vec<ReplayRow>,
    permit: Option<Arc<crate::budget::MemoryPermit>>,
}

/// Read one batch and resolve its values; `None` at spool end
async fn prepare_batch(
    replay: &mut DeferredReader,
    routes: &FrozenRoutes,
    catalog: &CatalogMap,
    stats: &EmitterStats,
    resolver: &ToastResolver,
    coverage: Option<&WindowCoverage>,
) -> Result<Option<ResolvedReplayBatch>, String> {
    let mut batch = next_batch(replay, routes, catalog, stats, resolver).await?;
    if batch.rows.is_empty() {
        return Ok(None);
    }
    // One permit per batch: every value a round fetched stays resident
    // until its row routes, so the rows share what survives
    let mut leaf = crate::budget::acquire_opt(resolver.budget(), batch.need).await;
    let retained = resolve_batch(&mut batch, resolver, stats, coverage).await?;
    if let Some(p) = leaf.as_mut() {
        p.shrink(retained as u64);
    }
    Ok(Some(ResolvedReplayBatch {
        tables: batch.tables,
        rows: batch.rows,
        permit: leaf.map(Arc::new),
    }))
}

/// Route a resolved batch's rows, each under the replay's trailing seq.
/// Every row reaches the batcher before return: a buffered row pins the
/// batch's leaf permit, which the next batch's acquire may wait on
async fn route_batch(
    batch: ResolvedReplayBatch,
    out: &mut LoadOut,
    seq: &mut Option<OpenSeq>,
    placed: &mut u64,
) -> Result<(), String> {
    let ResolvedReplayBatch {
        tables,
        rows,
        permit,
    } = batch;
    out.hold(permit);
    for row in rows {
        if row.replay_covered {
            continue;
        }
        let s = seq
            .get_or_insert_with(|| out.seqs.open(row.tuple.source_lsn, None, Publish::Commit))
            .seq();
        let RowTable { rel, route } = &tables[row.table as usize];
        out.push(s, rel, route, row.tuple, row.patch).await?;
        *placed += 1;
    }
    out.send().await?;
    out.hold(None);
    Ok(())
}

/// Read referrers until the batch seals or the spool ends, dropping rows no
/// mapped relation owns. Value caps are checked here, before any fetch
async fn next_batch(
    replay: &mut DeferredReader,
    routes: &FrozenRoutes,
    catalog: &CatalogMap,
    stats: &EmitterStats,
    resolver: &ToastResolver,
) -> Result<ReplayBatch, String> {
    let mut batch = ReplayBatch {
        tables: Vec::new(),
        rows: Vec::new(),
        need: 0,
        resident: 0,
    };
    let mut run: Option<(RelFileNode, Option<u32>)> = None;
    while batch.rows.len() < REPLAY_BATCH_ROWS && batch.need + batch.resident < REPLAY_FETCH_BYTES {
        let Some(tuple) = replay
            .next()
            .await
            .map_err(|e| format!("bootstrap: deferred spool replay: {e}"))?
        else {
            break;
        };
        let table = match run {
            Some((rfn, table)) if rfn == tuple.rfn => table,
            _ => {
                let rfn = tuple.rfn;
                let table = catalog
                    .get(rfn.db_node, rfn.rel_node)
                    .and_then(|rel| Some((routes.route(&rel)?, rel)))
                    .map(|(route, rel)| {
                        batch.tables.push(RowTable { rel, route });
                        (batch.tables.len() - 1) as u32
                    });
                run = Some((rfn, table));
                table
            }
        };
        let Some(table) = table else {
            stats.unsupported_relations.fetch_add(1, Ordering::Relaxed);
            continue;
        };
        let RowTable { rel, route } = &batch.tables[table as usize];
        let mut pointers = tuple
            .view(rel)
            .map_err(|e| decode_err(rel, e))?
            .map_or_else(Vec::new, |v| mapped_pointers(&v, &route.mapping));
        batch.need += check_value_caps(
            pointers.iter().map(|site| site.p),
            resolver.inline_value_max(),
            resolver.overflow(),
        )
        .map_err(|e| format!("bootstrap: {e}"))?;
        let mut patch = Vec::new();
        fill_oversize(&mut pointers, &mut patch, resolver);
        batch.resident += crate::backfill::spool::approx_bytes(&tuple);
        batch.rows.push(ReplayRow {
            tuple,
            table,
            pointers,
            patch,
            replay_covered: false,
        });
    }
    Ok(batch)
}

/// Resolve all values before applying miss policy so column order cannot
/// affect row survival. Retry unreadable, unreplaced rows at `start_lsn`:
/// window deletes may have removed values that existed at load version
async fn resolve_batch(
    batch: &mut ReplayBatch,
    resolver: &ToastResolver,
    stats: &EmitterStats,
    coverage: Option<&WindowCoverage>,
) -> Result<usize, String> {
    let rows = &batch.rows;
    let mut values = fetch_values_at(
        rows.iter()
            .map(|row| (row, lookup_bound(&row.tuple, coverage))),
        resolver,
    )
    .await?;
    let mut fetched: Vec<_> = rows
        .iter()
        .map(|row| values.take_row(row, lookup_bound(&row.tuple, coverage)))
        .collect();
    let replaced = |tuple: &BackfillTuple| {
        coverage.is_some_and(|c| c.replaced.contains(&(tuple.rfn, tuple.blkno, tuple.offnum)))
    };
    let retry: Vec<usize> = (0..rows.len())
        .filter(|&i| {
            let tuple = &rows[i].tuple;
            unreadable(&fetched[i])
                && !replaced(tuple)
                && lookup_bound(tuple, coverage) != tuple.source_lsn
        })
        .collect();
    if !retry.is_empty() {
        let mut again = fetch_values_at(
            retry.iter().map(|&i| (&rows[i], rows[i].tuple.source_lsn)),
            resolver,
        )
        .await?;
        for &i in &retry {
            fetched[i] = again.take_row(&rows[i], rows[i].tuple.source_lsn);
        }
    }
    let mut retained = 0usize;
    for (
        ReplayRow {
            tuple,
            table,
            pointers,
            patch,
            replay_covered,
        },
        fetched,
    ) in batch.rows.iter_mut().zip(fetched)
    {
        if unreadable(&fetched) && replaced(tuple) {
            stats
                .bootstrap_replay_covered_rows
                .fetch_add(1, Ordering::Relaxed);
            *replay_covered = true;
            continue;
        }
        let RowTable { rel, route } = &batch.tables[*table as usize];
        for (site, fetched) in pointers.iter().zip(fetched) {
            let target = &route.mapping.columns[site.col].target_name;
            let (column, bytes) = apply_fetched(fetched, &site.p, rel, target, resolver)?;
            retained += bytes;
            patch.push((site.idx, column));
        }
    }
    Ok(retained)
}

fn unreadable(fetched: &[Option<FetchedValue>]) -> bool {
    fetched.iter().any(|f| {
        f.as_ref()
            .is_some_and(|v| !matches!(v, FetchedValue::Assembled(_)))
    })
}

/// A batch's values, with how many referrers each one still owes
struct FetchedValues {
    values: HashMap<ValueKey, FetchedEntry>,
}

#[derive(Default)]
struct FetchedEntry {
    remaining: u32,
    value: Option<FetchedValue>,
}

impl FetchedValues {
    /// Last referrer of a value takes its bytes, earlier ones copy
    fn take(&mut self, key: ValueKey) -> Option<FetchedValue> {
        let Entry::Occupied(mut entry) = self.values.entry(key) else {
            return None;
        };
        entry.get_mut().remaining -= 1;
        if entry.get().remaining == 0 {
            entry.remove().value
        } else {
            entry.get().value.clone()
        }
    }

    fn take_row(&mut self, row: &ReplayRow, bound: u64) -> Vec<Option<FetchedValue>> {
        row.pointers
            .iter()
            .map(|site| value_key(row, site, bound))
            .map(|key| self.take(key))
            .collect()
    }
}

fn value_key(row: &ReplayRow, site: &PointerSite, bound: u64) -> ValueKey {
    (
        row.tuple.rfn.db_node,
        site.p.va_toastrelid,
        site.p.va_valueid,
        bound,
    )
}

async fn fetch_values_at<'a>(
    rows: impl Iterator<Item = (&'a ReplayRow, u64)>,
    resolver: &ToastResolver,
) -> Result<FetchedValues, String> {
    let mut values: HashMap<ValueKey, FetchedEntry> = HashMap::new();
    let mut wanted: HashMap<(u32, u32, u64), Vec<(u32, usize)>> = HashMap::new();
    for (row, bound) in rows {
        for site in &row.pointers {
            let key = value_key(row, site, bound);
            let entry = values.entry(key).or_default();
            entry.remaining += 1;
            if entry.remaining == 1 {
                wanted
                    .entry((key.0, key.1, bound))
                    .or_default()
                    .push((key.2, pointer_extsize(&site.p)));
            }
        }
    }
    for ((db_oid, toast_relid, bound), batch) in wanted {
        let Some(got) = resolver
            .fetch_values(db_oid, toast_relid, &batch, bound)
            .await
            .map_err(|e| format!("bootstrap: toast store fetch: {e}"))?
        else {
            return Ok(FetchedValues {
                values: HashMap::new(),
            });
        };
        for ((value_id, _), value) in batch.iter().zip(got) {
            values
                .get_mut(&(db_oid, toast_relid, *value_id, bound))
                .expect("counted in fetch_values_at")
                .value = Some(value);
        }
    }
    Ok(FetchedValues { values })
}

/// Turn one fetch outcome into the column its row routes, counting fills.
/// Returns the bytes the column retains
fn apply_fetched(
    fetched: Option<FetchedValue>,
    p: &ToastPointer,
    rel: &RelDescriptor,
    target: &str,
    resolver: &ToastResolver,
) -> Result<(Datum, usize), String> {
    let miss = match StoreMiss::split(fetched) {
        Ok(stored) => {
            let raw = finish_value(p, stored).map_err(|e| e.to_string())?;
            return Ok(detoasted_sized(raw));
        }
        Err(miss) => miss,
    };
    let column =
        fill_store_miss(miss, p, resolver, MissPolicy::bootstrap(resolver)).map_err(|detail| {
            format!(
                "bootstrap: relation {} column {target} value_id={} on toast relid={}: \
                 {detail}; remedy: fresher backup, or initial_load='copy'",
                rel.rel_name, p.va_valueid, p.va_toastrelid
            )
        })?;
    Ok((column, 0))
}

/// No store to consult: NULL-fill mapped TOAST pointers after the value
/// cap check, counting each fill
fn fill_toast(
    mut sites: Vec<PointerSite>,
    resolver: &ToastResolver,
) -> Result<Vec<(usize, Datum)>, String> {
    check_value_caps(
        sites.iter().map(|site| site.p),
        resolver.inline_value_max(),
        resolver.overflow(),
    )
    .map_err(|e| format!("bootstrap: {e}"))?;
    let mut patch = Vec::with_capacity(sites.len());
    fill_oversize(&mut sites, &mut patch, resolver);
    for site in &sites {
        resolver.note_filled_default();
        patch.push((site.idx, Datum::Null));
    }
    Ok(patch)
}

fn put_err(e: ChunkStoreError) -> String {
    format!("bootstrap: toast store put: {e}")
}

#[cfg(test)]
mod tests {
    #[test]
    fn deferred_replay_progress_preserves_other_lanes_on_drop() {
        use super::*;
        let stats = EmitterStats::default();
        let mut first = DeferredReplayProgress::new(&stats, 100);
        let mut second = DeferredReplayProgress::new(&stats, 200);
        first.advance(75);
        second.advance(100);
        assert_eq!(
            stats
                .bootstrap_deferred_replay_bytes
                .load(Ordering::Relaxed),
            300
        );
        assert_eq!(
            stats
                .bootstrap_deferred_replayed_bytes
                .load(Ordering::Relaxed),
            125
        );
        drop(first);
        assert_eq!(
            stats
                .bootstrap_deferred_replay_bytes
                .load(Ordering::Relaxed),
            200
        );
        assert_eq!(
            stats
                .bootstrap_deferred_replayed_bytes
                .load(Ordering::Relaxed),
            100
        );
        second.advance(0);
        assert_eq!(
            stats
                .bootstrap_deferred_replayed_bytes
                .load(Ordering::Relaxed),
            200
        );
        drop(second);
        assert_eq!(
            stats
                .bootstrap_deferred_replay_bytes
                .load(Ordering::Relaxed),
            0
        );
        assert_eq!(
            stats
                .bootstrap_deferred_replayed_bytes
                .load(Ordering::Relaxed),
            0
        );
    }

    /// Flatten the drain's slabs back to `(seq, mapped cells)` rows
    async fn collect_rows(rx: &mut mpsc::Receiver<BatcherMsg>) -> Vec<(u64, Vec<Datum>)> {
        let mut rows = Vec::new();
        // Any other message ends collection short of the expected rows
        while let Some(BatcherMsg::Load(slab)) = rx.recv().await {
            rows.extend(slab.cells());
        }
        rows
    }

    use super::*;
    use crate::decode::datum::{Datum, build};
    use crate::toast::ToastRow;

    /// End-of-backup store, refuses puts
    struct ReadOnly;

    #[async_trait::async_trait]
    impl crate::toast::ChunkStore for ReadOnly {
        async fn fetch_many(
            &self,
            _: u32,
            _: u32,
            values: &[(u32, usize)],
            _: u64,
        ) -> Result<Vec<FetchedValue>, crate::toast::ChunkStoreError> {
            // Incomplete value is absent from end-of-backup state
            Ok(vec![FetchedValue::Mismatch { got: 7984 }; values.len()])
        }
    }

    /// Distinguish missing data in walk-seeded store from superseded value in
    /// read-only end-of-backup store
    #[tokio::test(flavor = "current_thread")]
    async fn a_miss_is_fatal_for_a_seeded_mirror_and_superseded_for_a_read_only_store() {
        use crate::toast::MemChunkStore;

        let ptr = crate::decode::heap_decoder::ToastPointer {
            va_rawsize: 9104,
            va_extinfo: 9100,
            va_valueid: 16402,
            va_toastrelid: 16390,
        };
        let rel = crate::backfill::backup_page_walk::toast_chunk_rel();
        let short = Some(FetchedValue::Mismatch { got: 7984 });

        // Walk-seeded store must contain chunks just written
        let seeded = ToastResolver::with_store(
            Arc::new(MemChunkStore::new()),
            Arc::new(EmitterStats::default()),
        );
        assert!(seeded.stores_chunks());
        let err = apply_fetched(short.clone(), &ptr, &rel, "body", &seeded)
            .expect_err("a seeded mirror losing its own chunk must stop the load");
        assert!(
            err.contains("chunks sum to 7984 bytes, pointer says 9100"),
            "{err}"
        );
        assert_eq!(
            seeded
                .stats_handle()
                .toast_fetch_miss
                .load(Ordering::Relaxed),
            1
        );

        // Missing read-only value is superseded at end-of-backup state
        let stats = Arc::new(EmitterStats::default());
        let read_only = ToastResolver::with_store(Arc::new(ReadOnly), stats.clone());
        assert!(!read_only.stores_chunks() && !read_only.fill_on_miss());
        let fetched = read_only
            .fetch_value(0, 16390, 16402, u64::MAX, 9100)
            .await
            .unwrap();
        assert_eq!(fetched, short);
        let (column, retained) = apply_fetched(fetched, &ptr, &rel, "body", &read_only)
            .expect("a read-only backend fills instead of failing the load");
        assert_eq!(column, Datum::Null);
        assert_eq!(retained, 0);
        assert_eq!(
            stats.toast_values_filled_superseded.load(Ordering::Relaxed),
            1
        );
        assert_eq!(
            stats.toast_fetch_miss.load(Ordering::Relaxed),
            0,
            "not counted as a miss: it is the expected answer, not a fault",
        );
    }
    use crate::backfill::spool::DEFERRED_SPOOL_MEM_MAX;
    use crate::mapping::{ColumnMapping, TableMapping, TableTarget};

    /// Mem-only under the default threshold; path never created
    fn mem_spool() -> DeferredSpool {
        DeferredSpool::new(
            std::env::temp_dir().join("ws-bootstrap-test-unused.bin"),
            DEFERRED_SPOOL_MEM_MAX,
        )
    }
    use crate::decode::heap_decoder::ToastPointer;
    use crate::emit::pipeline::batcher::BatcherMsg;
    use crate::emit::pipeline::{Fatal, ack};
    use crate::schema::{RelAttr, RelDescriptor, RelName, ReplIdent};
    use crate::toast::{GatedStore, MemChunkStore};
    use foldhash::{HashMap, HashMapExt, HashSetExt};
    use walrus::pg::walparser::RelFileNode;

    fn rel(rel_node: u32) -> Arc<RelDescriptor> {
        let name = format!("t{rel_node}");
        Arc::new(RelDescriptor {
            rfn: RelFileNode {
                spc_node: 1663,
                db_node: 5,
                rel_node,
            },
            oid: rel_node,
            toast_oid: 0,
            namespace_oid: 2200,
            rel_name: RelName::new("public", &name),
            kind: b'r',
            persistence: b'p',
            ancestors: Vec::new(),
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
                type_align: b'i',
                type_storage: b'p',
                missing_default: None,
            }],
        })
    }

    fn mapping_for(rel_node: u32) -> Arc<TableMapping> {
        Arc::new(TableMapping {
            target: TableTarget::new("default", &format!("t{rel_node}")),
            columns: vec![ColumnMapping {
                src_attnum: 1,
                target_name: "id".into(),
                target_type: "Int32".into(),
                type_pinned: false,
            }],
            tees: Vec::new(),
        })
    }

    fn tuple(rel_node: u32, id: i32) -> BackfillTuple {
        BackfillTuple {
            rfn: RelFileNode {
                spc_node: 1663,
                db_node: 5,
                rel_node,
            },
            xid: 99,
            xmax: 0,
            infomask: 0,
            source_lsn: 0x1000,
            page_lsn: 0,
            blkno: 0,
            offnum: 0,
            data: TupleData::from_cells(&[Some(build::int4(id))]),
            pending: None,
        }
    }

    /// Main rel with one mapped `bytea` column (attnum 1), the detoast target.
    fn bytea_rel(rel_node: u32) -> Arc<RelDescriptor> {
        let name = format!("t{rel_node}");
        Arc::new(RelDescriptor {
            rfn: RelFileNode {
                spc_node: 1663,
                db_node: 5,
                rel_node,
            },
            oid: rel_node,
            toast_oid: 0,
            namespace_oid: 2200,
            rel_name: RelName::new("public", &name),
            kind: b'r',
            persistence: b'p',
            ancestors: Vec::new(),
            replident: ReplIdent::Default { pk_attnums: None },
            attributes: vec![RelAttr {
                attnum: 1,
                name: "b".into(),
                type_oid: 17,
                typmod: -1,
                not_null: false,
                dropped: false,
                type_name: "bytea".into(),
                type_byval: false,
                type_len: -1,
                type_align: b'i',
                type_storage: b'x',
                missing_default: None,
            }],
        })
    }

    fn bytea_mapping_for(rel_node: u32) -> Arc<TableMapping> {
        Arc::new(TableMapping {
            target: TableTarget::new("default", &format!("t{rel_node}")),
            columns: vec![ColumnMapping {
                src_attnum: 1,
                target_name: "b".into(),
                target_type: "Nullable(String)".into(),
                type_pinned: false,
            }],
            tees: Vec::new(),
        })
    }

    /// `pg_toast` rel so [`CatalogMap::is_toast`] fires; `oid` matches the
    /// referring pointer's `va_toastrelid`
    fn toast_rel(rel_node: u32) -> Arc<RelDescriptor> {
        let base = crate::backfill::backup_page_walk::toast_chunk_rel();
        Arc::new(RelDescriptor {
            rfn: RelFileNode {
                spc_node: 1663,
                db_node: 5,
                rel_node,
            },
            oid: rel_node,
            rel_name: RelName::new("pg_toast", &format!("pg_toast_{rel_node}")),
            ..base
        })
    }

    /// Main-rel tuple whose mapped bytea column is an on-disk TOAST pointer
    /// into `toast_relid`/`value_id`, uncompressed (`va_extinfo` high bits
    /// clear so reassembly returns the concatenated chunks verbatim).
    fn bytea_toast_tuple(rel_node: u32, toast_relid: u32, value_id: u32) -> BackfillTuple {
        referrer_tuple(rel_node, &[pointer(toast_relid, value_id)])
    }

    fn pointer(toast_relid: u32, value_id: u32) -> ToastPointer {
        ToastPointer {
            va_rawsize: 9,
            va_extinfo: 5,
            va_valueid: value_id,
            va_toastrelid: toast_relid,
        }
    }

    /// Main-rel tuple whose columns are on-disk TOAST pointers
    fn referrer_tuple(rel_node: u32, pointers: &[ToastPointer]) -> BackfillTuple {
        let cells: Vec<_> = pointers.iter().map(|p| Some(Datum::External(*p))).collect();
        BackfillTuple {
            rfn: RelFileNode {
                spc_node: 1663,
                db_node: 5,
                rel_node,
            },
            xid: 99,
            xmax: 0,
            infomask: 0,
            source_lsn: 0x1000,
            page_lsn: 0,
            blkno: 0,
            offnum: 0,
            data: TupleData::from_cells(&cells),
            pending: None,
        }
    }

    /// `pg_toast_*` page tuple: 3 columns (`chunk_id oid`, `chunk_seq int4`,
    /// `chunk_data bytea`) the drain reinterprets into a stored chunk.
    fn toast_chunk_tuple(rel_node: u32, value_id: u32, seq: i32, body: &[u8]) -> BackfillTuple {
        BackfillTuple {
            rfn: RelFileNode {
                spc_node: 1663,
                db_node: 5,
                rel_node,
            },
            xid: 99,
            xmax: 0,
            infomask: 0,
            source_lsn: 0x1000,
            page_lsn: 0,
            blkno: 1,
            offnum: 1,
            data: TupleData::from_cells(&chunk_cells(value_id, seq, body)),
            pending: None,
        }
    }

    fn chunk_cells(value_id: u32, seq: i32, body: &[u8]) -> [Option<Datum>; 3] {
        [
            Some(build::oid(value_id)),
            Some(build::int4(seq)),
            Some(build::bytes(body)),
        ]
    }

    /// Stored chunk `toast_chunk_tuple` walks into under `toast`
    fn chunk_row(toast: &RelDescriptor, value_id: u32, body: &[u8]) -> ToastRow {
        let data =
            crate::decode::heap_decoder::form_block_data(&[], &chunk_cells(value_id, 0, body));
        toast_row(&data, toast, 1, 1, 0).unwrap()
    }

    #[tokio::test]
    async fn detoasted_input_rejects_external_pointers() {
        let mut catalog = CatalogMap::new();
        catalog.insert(bytea_rel(16400));
        let mapping = Arc::new(
            [(RelName::new("public", "t16400"), bytea_mapping_for(16400))]
                .into_iter()
                .collect(),
        );
        let (ack, collector) = ack::spawn(Arc::new(crate::pos::Monotone::default()), Fatal::new());
        let (msg_tx, _msg_rx) = mpsc::channel(1);
        let (tx, rx) = mpsc::channel(1);
        tx.send(vec![bytea_toast_tuple(16400, 16401, 7)])
            .await
            .unwrap();
        drop(tx);
        let stats = Arc::new(EmitterStats::default());
        let err = drain(
            rx,
            catalog,
            freeze_routes(&mapping, None, &RowPolicy::default()),
            msg_tx,
            ack,
            stats.clone(),
            ToastResolver::with_store(Arc::new(MemChunkStore::new()), stats),
            Deferral::Rejected,
            HashSet::new(),
            None,
        )
        .await
        .unwrap_err();
        assert!(err.contains("undeferrable external value"), "{err}");
        collector.await.unwrap();
    }

    /// Two rels, contiguous rows each: one seq per rfn, every row routed,
    /// each seq placed with its exact count.
    #[tokio::test]
    async fn seq_per_rfn_places_exact_counts() {
        let mut catalog = CatalogMap::new();
        catalog.insert(rel(16400));
        catalog.insert(rel(16401));
        let mut tables = HashMap::new();
        tables.insert(RelName::new("public", "t16400"), mapping_for(16400));
        tables.insert(RelName::new("public", "t16401"), mapping_for(16401));
        let mapping = Arc::new(tables);

        let emitter_ack = Arc::new(crate::pos::Monotone::default());
        let (ack, collector) = ack::spawn(emitter_ack, Fatal::new());
        let (msg_tx, mut msg_rx) = mpsc::channel::<BatcherMsg>(64);
        let (tup_tx, tup_rx) = mpsc::channel::<Vec<BackfillTuple>>(64);

        for id in 0..3 {
            tup_tx.send(vec![tuple(16400, id)]).await.unwrap();
        }
        for id in 0..2 {
            tup_tx.send(vec![tuple(16401, id)]).await.unwrap();
        }
        drop(tup_tx);

        let stats = Arc::new(EmitterStats::default());
        let drain_task = tokio::spawn(drain(
            tup_rx,
            catalog,
            freeze_routes(&mapping, None, &RowPolicy::default()),
            msg_tx,
            ack.clone(),
            stats.clone(),
            ToastResolver::disabled(),
            Deferral::Local(mem_spool()),
            HashSet::new(),
            None,
        ));

        let mut by_seq: HashMap<u64, u64> = HashMap::new();
        for r in collect_rows(&mut msg_rx).await {
            *by_seq.entry(r.0).or_default() += 1;
        }
        let outcome = drain_task.await.unwrap().unwrap();
        assert_eq!(outcome.next_seq, 2, "one seq per rfn");
        assert_eq!(outcome.rows_routed, 5);
        assert_eq!(by_seq.get(&0), Some(&3), "rel 16400 → seq 0, 3 rows");
        assert_eq!(by_seq.get(&1), Some(&2), "rel 16401 → seq 1, 2 rows");
        assert_eq!(stats.unsupported_relations.load(Ordering::Relaxed), 0);

        // No inserter, so only placed (not acked); drop ack to let collector exit.
        drop(ack);
        collector.await.unwrap();
    }

    /// rfn reappearing non-contiguously (object_store interleave) gets a fresh
    /// seq each run; unmapped rel skipped but still consumes a zero-row seq.
    #[tokio::test]
    async fn reappearing_and_unmapped_rfns() {
        let mut catalog = CatalogMap::new();
        catalog.insert(rel(16400));
        catalog.insert(rel(16401)); // resolvable but unmapped
        let mut tables = HashMap::new();
        tables.insert(RelName::new("public", "t16400"), mapping_for(16400));
        let mapping = Arc::new(tables);

        let emitter_ack = Arc::new(crate::pos::Monotone::default());
        let (ack, collector) = ack::spawn(emitter_ack, Fatal::new());
        let (msg_tx, mut msg_rx) = mpsc::channel::<BatcherMsg>(64);
        let (tup_tx, tup_rx) = mpsc::channel::<Vec<BackfillTuple>>(64);

        // 16400, 16401(unmapped), 16400 → seqs 0,1,2; only 0 and 2 route
        tup_tx.send(vec![tuple(16400, 1)]).await.unwrap();
        tup_tx.send(vec![tuple(16401, 9)]).await.unwrap();
        tup_tx.send(vec![tuple(16400, 2)]).await.unwrap();
        drop(tup_tx);

        let stats = Arc::new(EmitterStats::default());
        let drain_task = tokio::spawn(drain(
            tup_rx,
            catalog,
            freeze_routes(&mapping, None, &RowPolicy::default()),
            msg_tx,
            ack.clone(),
            stats.clone(),
            ToastResolver::disabled(),
            Deferral::Local(mem_spool()),
            HashSet::new(),
            None,
        ));

        let seqs: Vec<u64> = collect_rows(&mut msg_rx)
            .await
            .iter()
            .map(|r| r.0)
            .collect();
        let outcome = drain_task.await.unwrap().unwrap();
        assert_eq!(outcome.next_seq, 3, "three distinct rfn runs");
        assert_eq!(outcome.rows_routed, 2, "unmapped rel routed nothing");
        assert_eq!(seqs, [0, 2], "seq 1 (unmapped) routed no rows");
        assert_eq!(stats.unsupported_relations.load(Ordering::Relaxed), 1);
        drop(ack);
        collector.await.unwrap();
    }

    async fn drain_plain(
        tuples: Vec<BackfillTuple>,
        skip_initial: HashSet<RelName>,
        batcher_open: bool,
    ) -> (
        Result<BootstrapDrainOutcome, String>,
        Vec<(u64, Vec<Datum>)>,
    ) {
        let mut catalog = CatalogMap::new();
        catalog.insert(rel(16400));
        catalog.insert(toast_rel(16500));
        let tables = [(RelName::new("public", "t16400"), mapping_for(16400))];
        let mapping = Arc::new(tables.into_iter().collect());
        let (ack, collector) = ack::spawn(Arc::default(), Fatal::new());
        let (msg_tx, mut msg_rx) = mpsc::channel::<BatcherMsg>(64);
        let (tup_tx, tup_rx) = mpsc::channel::<Vec<BackfillTuple>>(64);
        tup_tx.send(tuples).await.unwrap();
        drop(tup_tx);
        if !batcher_open {
            msg_rx.close();
        }
        let outcome = drain(
            tup_rx,
            catalog,
            freeze_routes(&mapping, None, &RowPolicy::default()),
            msg_tx,
            ack.clone(),
            Arc::default(),
            ToastResolver::disabled(),
            Deferral::Local(mem_spool()),
            skip_initial,
            None,
        )
        .await;
        let rows = collect_rows(&mut msg_rx).await;
        drop(ack);
        collector.await.unwrap();
        (outcome, rows)
    }

    /// Caller-loaded relations and malformed chunk rows route nothing
    #[tokio::test]
    async fn skipped_relations_and_malformed_chunks_route_nothing() {
        let skip = [RelName::new("public", "t16400")].into_iter().collect();
        let mut malformed = tuple(16500, 2);
        malformed.offnum = 1;
        let tuples = vec![tuple(16400, 1), malformed];
        let (outcome, rows) = drain_plain(tuples, skip, true).await;
        assert_eq!(outcome.unwrap().rows_routed, 0);
        assert!(rows.is_empty());
    }

    /// Store-less fill still enforces the value cap
    #[tokio::test]
    async fn oversize_value_fails_storeless_drain() {
        let mut catalog = CatalogMap::new();
        catalog.insert(bytea_rel(16400));
        let tables = [(RelName::new("public", "t16400"), bytea_mapping_for(16400))];
        let mapping = Arc::new(tables.into_iter().collect());
        let (ack, collector) = ack::spawn(Arc::default(), Fatal::new());
        let (msg_tx, _msg_rx) = mpsc::channel::<BatcherMsg>(64);
        let (tup_tx, tup_rx) = mpsc::channel::<Vec<BackfillTuple>>(64);
        tup_tx
            .send(vec![bytea_toast_tuple(16400, 16500, 1)])
            .await
            .unwrap();
        drop(tup_tx);
        let outcome = drain(
            tup_rx,
            catalog,
            freeze_routes(&mapping, None, &RowPolicy::default()),
            msg_tx,
            ack.clone(),
            Arc::default(),
            ToastResolver::disabled()
                .with_inline_value_max(1)
                .with_overflow(crate::emit::ch_emitter::InlineValueOverflow::Error),
            Deferral::Rejected,
            HashSet::new(),
            None,
        )
        .await;
        let e = outcome.expect_err("oversize value fails drain");
        assert!(e.starts_with("bootstrap: "), "{e}");
        drop(ack);
        collector.await.unwrap();
    }

    /// A chunk store refusing writes fails the walk at the put
    #[tokio::test]
    async fn refused_chunk_put_fails_the_drain() {
        let mut catalog = CatalogMap::new();
        catalog.insert(toast_rel(16500));
        let (ack, collector) = ack::spawn(Arc::default(), Fatal::new());
        let (msg_tx, _msg_rx) = mpsc::channel::<BatcherMsg>(64);
        let (tup_tx, tup_rx) = mpsc::channel::<Vec<BackfillTuple>>(64);
        tup_tx
            .send(vec![toast_chunk_tuple(16500, 1, 0, b"a")])
            .await
            .unwrap();
        drop(tup_tx);
        let stats = Arc::new(EmitterStats::default());
        let resolver =
            ToastResolver::with_store(Arc::new(ReadOnly), stats.clone()).with_put_batch_rows(1);
        let outcome = drain(
            tup_rx,
            catalog,
            freeze_routes(&Arc::new(HashMap::new()), None, &RowPolicy::default()),
            msg_tx,
            ack.clone(),
            stats,
            resolver,
            Deferral::Local(mem_spool()),
            HashSet::new(),
            None,
        )
        .await;
        let e = outcome.expect_err("refused put fails drain");
        assert!(e.starts_with("bootstrap: toast store put: "), "{e}");
        drop(ack);
        collector.await.unwrap();
    }

    #[tokio::test]
    async fn closed_batcher_fails_the_drain() {
        let (outcome, _) = drain_plain(vec![tuple(16400, 1)], HashSet::new(), false).await;
        let e = outcome.expect_err("closed batcher fails drain");
        assert!(e.starts_with("bootstrap: "), "{e}");
    }

    /// Disabled resolver: a mapped externally-TOASTed column has no store to
    /// consult, so it NULL-fills inline, routes the row, and counts the fill.
    #[tokio::test]
    async fn disabled_resolver_fills_toast_with_null() {
        let mut catalog = CatalogMap::new();
        catalog.insert(bytea_rel(16400));
        let mut tables = HashMap::new();
        tables.insert(RelName::new("public", "t16400"), bytea_mapping_for(16400));
        let mapping = Arc::new(tables);

        let emitter_ack = Arc::new(crate::pos::Monotone::default());
        let (ack, collector) = ack::spawn(emitter_ack, Fatal::new());
        let (msg_tx, mut msg_rx) = mpsc::channel::<BatcherMsg>(64);
        let (tup_tx, tup_rx) = mpsc::channel::<Vec<BackfillTuple>>(64);

        tup_tx
            .send(vec![bytea_toast_tuple(16400, 16500, 1)])
            .await
            .unwrap();
        drop(tup_tx);

        let stats = Arc::new(EmitterStats::default());
        let resolver = ToastResolver::disabled().with_stats(stats.clone());
        let drain_task = tokio::spawn(drain(
            tup_rx,
            catalog,
            freeze_routes(&mapping, None, &RowPolicy::default()),
            msg_tx,
            ack.clone(),
            stats.clone(),
            resolver,
            Deferral::Local(mem_spool()),
            HashSet::new(),
            None,
        ));

        let rows = collect_rows(&mut msg_rx).await;
        let outcome = drain_task.await.unwrap().unwrap();
        assert_eq!(outcome.next_seq, 1);
        assert_eq!(outcome.rows_routed, 1);
        assert_eq!(rows.len(), 1);
        let cols = &rows[0].1;
        assert_eq!(cols[0], Datum::Null, "unresolved toast NULL-filled");
        assert_eq!(stats.toast_values_filled_default.load(Ordering::Relaxed), 1);
        drop(ack);
        collector.await.unwrap();
    }

    /// Replace oversized deferred value without fetching it
    #[tokio::test]
    async fn null_overflow_fills_oversize_toast_without_fetch() {
        let mut catalog = CatalogMap::new();
        catalog.insert(bytea_rel(16400));
        let mut tables = HashMap::new();
        tables.insert(RelName::new("public", "t16400"), bytea_mapping_for(16400));
        let mapping = Arc::new(tables);

        let emitter_ack = Arc::new(crate::pos::Monotone::default());
        let (ack, collector) = ack::spawn(emitter_ack, Fatal::new());
        let (msg_tx, mut msg_rx) = mpsc::channel::<BatcherMsg>(64);
        let (tup_tx, tup_rx) = mpsc::channel::<Vec<BackfillTuple>>(64);

        tup_tx
            .send(vec![bytea_toast_tuple(16400, 16500, 1)])
            .await
            .unwrap();
        drop(tup_tx);

        let stats = Arc::new(EmitterStats::default());
        // Pointer footprint is 5 bytes
        let resolver = ToastResolver::with_store(Arc::new(MemChunkStore::new()), stats.clone())
            .with_inline_value_max(4)
            .with_overflow(crate::emit::ch_emitter::InlineValueOverflow::Null);
        let drain_task = tokio::spawn(drain(
            tup_rx,
            catalog,
            freeze_routes(&mapping, None, &RowPolicy::default()),
            msg_tx,
            ack.clone(),
            stats.clone(),
            resolver,
            Deferral::Local(mem_spool()),
            HashSet::new(),
            None,
        ));

        let rows = collect_rows(&mut msg_rx).await;
        let outcome = drain_task.await.unwrap().unwrap();
        assert_eq!(outcome.rows_routed, 1);
        assert_eq!(rows.len(), 1);
        let cols = &rows[0].1;
        assert_eq!(cols[0], Datum::Null);
        assert_eq!(
            stats.toast_values_filled_oversize.load(Ordering::Relaxed),
            1
        );
        assert_eq!(stats.toast_values_filled_default.load(Ordering::Relaxed), 0);
        assert_eq!(stats.toast_values_fetched.load(Ordering::Relaxed), 0);
        drop(ack);
        collector.await.unwrap();
    }

    /// Store-backed resolver: a `pg_toast_*` page tuple is persisted as a
    /// row, then a deferred main tuple fetches it back, reassembles the
    /// value, and routes it as a `Bytea` under the trailing
    /// deferred-resolution seq.
    #[tokio::test]
    async fn store_resolver_reassembles_toast_from_chunk() {
        let mut catalog = CatalogMap::new();
        catalog.insert(bytea_rel(16400));
        catalog.insert(toast_rel(16500));
        let mut tables = HashMap::new();
        tables.insert(RelName::new("public", "t16400"), bytea_mapping_for(16400));
        let mapping = Arc::new(tables);

        let emitter_ack = Arc::new(crate::pos::Monotone::default());
        let (ack, collector) = ack::spawn(emitter_ack, Fatal::new());
        let (msg_tx, mut msg_rx) = mpsc::channel::<BatcherMsg>(64);
        let (tup_tx, tup_rx) = mpsc::channel::<Vec<BackfillTuple>>(64);
        let spool_tmp = tempfile::tempdir().unwrap();

        // toast chunk first (its own zero-row seq), then the referring main row
        tup_tx
            .send(vec![toast_chunk_tuple(16500, 1, 0, b"hello")])
            .await
            .unwrap();
        tup_tx
            .send(vec![bytea_toast_tuple(16400, 16500, 1)])
            .await
            .unwrap();
        drop(tup_tx);

        let stats = Arc::new(EmitterStats::default());
        let store = Arc::new(MemChunkStore::new());
        let drain_task = tokio::spawn(drain(
            tup_rx,
            catalog,
            freeze_routes(&mapping, None, &RowPolicy::default()),
            msg_tx,
            ack.clone(),
            stats.clone(),
            ToastResolver::with_store(store, stats.clone()),
            // Threshold 0: deferred referrer rides a real spool file
            Deferral::Local(DeferredSpool::new(
                spool_tmp.path().join("bootstrap_deferred.bin"),
                0,
            )),
            HashSet::new(),
            None,
        ));

        let rows = collect_rows(&mut msg_rx).await;
        let outcome = drain_task.await.unwrap().unwrap();
        assert_eq!(outcome.next_seq, 3, "toast seq, main seq, deferred seq");
        assert_eq!(outcome.rows_routed, 1);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].0, 2, "deferred row routed under the trailing seq");
        let cols = &rows[0].1;
        assert_eq!(cols[0], build::bytes(b"hello"));
        assert_eq!(stats.toast_chunks_stored.load(Ordering::Relaxed), 1);
        assert_eq!(stats.toast_values_fetched.load(Ordering::Relaxed), 1);
        drop(ack);
        collector.await.unwrap();
    }

    fn gated_drain(
        tup_rx: mpsc::Receiver<Vec<BackfillTuple>>,
        msg_tx: mpsc::Sender<BatcherMsg>,
        ack: AckHandle,
        store: Arc<GatedStore>,
        put_batch_rows: usize,
        barrier: Option<Arc<WalkBarrier>>,
    ) -> tokio::task::JoinHandle<Result<BootstrapDrainOutcome, String>> {
        let mut catalog = CatalogMap::new();
        catalog.insert(rel(16400));
        catalog.insert(toast_rel(16500));
        let mapping = Arc::new(
            [(RelName::new("public", "t16400"), mapping_for(16400))]
                .into_iter()
                .collect(),
        );
        let stats = Arc::new(EmitterStats::default());
        let resolver =
            ToastResolver::with_store(store, stats.clone()).with_put_batch_rows(put_batch_rows);
        tokio::spawn(drain(
            tup_rx,
            catalog,
            freeze_routes(&mapping, None, &RowPolicy::default()),
            msg_tx,
            ack,
            stats,
            resolver,
            Deferral::Local(mem_spool()),
            HashSet::new(),
            barrier,
        ))
    }

    /// Heap rows behind a gated put still reach the batcher
    #[tokio::test]
    async fn drain_reads_past_inflight_put() {
        let (ack, collector) = ack::spawn(Arc::new(crate::pos::Monotone::default()), Fatal::new());
        let (msg_tx, mut msg_rx) = mpsc::channel(64);
        let (tup_tx, tup_rx) = mpsc::channel(64);
        tup_tx
            .send(vec![
                toast_chunk_tuple(16500, 1, 0, b"a"),
                tuple(16400, 1),
                tuple(16400, 2),
                toast_chunk_tuple(16500, 2, 0, b"b"),
            ])
            .await
            .unwrap();
        drop(tup_tx);
        let store = Arc::new(GatedStore::new());
        let drain_task = gated_drain(tup_rx, msg_tx, ack.clone(), store.clone(), 1, None);

        let msg = tokio::time::timeout(std::time::Duration::from_secs(5), msg_rx.recv())
            .await
            .expect("heap rows stuck behind gated put");
        assert!(matches!(&msg, Some(BatcherMsg::Load(s)) if s.rows.len() == 2));
        assert_eq!(store.permits.available_permits(), 0, "no put landed yet");

        store.permits.add_permits(2);
        drain_task.await.unwrap().unwrap();
        assert_eq!(store.puts_started.load(Ordering::Relaxed), 2);
        drop(ack);
        collector.await.unwrap();
    }

    /// Checkpoint publishes drain progress only after chunks land
    #[tokio::test(start_paused = true)]
    async fn drain_checkpoint_waits_for_puts() {
        let (ack, collector) = ack::spawn(Arc::new(crate::pos::Monotone::default()), Fatal::new());
        let (msg_tx, _msg_rx) = mpsc::channel(64);
        let (tup_tx, tup_rx) = mpsc::channel(64);
        let barrier = Arc::new(WalkBarrier::default());
        barrier
            .publish_gate(vec![("f".into(), 2)], SpoolMark::default())
            .await;
        let store = Arc::new(GatedStore::new());
        let drain_task = gated_drain(
            tup_rx,
            msg_tx,
            ack.clone(),
            store.clone(),
            usize::MAX,
            Some(barrier.clone()),
        );

        // Paused clock: polls sleep instead of spin so a stall trips timeout
        let poll = std::time::Duration::from_millis(1);
        let deadline = std::time::Duration::from_secs(5);
        tup_tx
            .send(vec![toast_chunk_tuple(16500, 1, 0, b"a")])
            .await
            .unwrap();
        // Drain's ticker starts before the clock moves
        tokio::time::timeout(deadline, async {
            while tup_tx.capacity() < tup_tx.max_capacity() {
                tokio::time::sleep(poll).await;
            }
        })
        .await
        .expect("drain took first batch");
        tokio::time::advance(WALK_CHECKPOINT_PERIOD).await;
        tup_tx.send(vec![tuple(16400, 1)]).await.unwrap();
        tokio::time::timeout(deadline, async {
            while store.puts_started.load(Ordering::Relaxed) == 0 {
                tokio::time::sleep(poll).await;
            }
        })
        .await
        .expect("checkpoint submitted chunks");
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        assert!(
            barrier.collect().await.is_none(),
            "drain published before its chunks landed"
        );

        store.permits.add_permits(1);
        let proof = tokio::time::timeout(deadline, async {
            loop {
                if let Some(proof) = barrier.collect().await {
                    break proof;
                }
                tokio::time::sleep(poll).await;
            }
        })
        .await
        .expect("drain published after chunks landed");
        assert_eq!(proof.files(), ["f"]);
        drop(tup_tx);
        drain_task.await.unwrap().unwrap();
        drop(ack);
        collector.await.unwrap();
    }

    /// Handback leaves a referrer unrouted: a lane cannot know when its
    /// siblings' chunk puts landed, so resolution is the caller's call
    #[tokio::test]
    async fn handback_defers_resolution_to_the_caller() {
        let mut catalog = CatalogMap::new();
        catalog.insert(bytea_rel(16400));
        catalog.insert(toast_rel(16500));
        let mut tables = HashMap::new();
        tables.insert(RelName::new("public", "t16400"), bytea_mapping_for(16400));
        let mapping = Arc::new(tables);

        let (ack, collector) = ack::spawn(Arc::new(crate::pos::Monotone::default()), Fatal::new());
        let (msg_tx, mut msg_rx) = mpsc::channel::<BatcherMsg>(64);
        let (tup_tx, tup_rx) = mpsc::channel::<Vec<BackfillTuple>>(64);
        tup_tx
            .send(vec![toast_chunk_tuple(16500, 1, 0, b"hello")])
            .await
            .unwrap();
        tup_tx
            .send(vec![bytea_toast_tuple(16400, 16500, 1)])
            .await
            .unwrap();
        drop(tup_tx);

        let stats = Arc::new(EmitterStats::default());
        let resolver = ToastResolver::with_store(Arc::new(MemChunkStore::new()), stats.clone());
        let mut outcome = drain(
            tup_rx,
            catalog.clone(),
            freeze_routes(&mapping, None, &RowPolicy::default()),
            msg_tx.clone(),
            ack.clone(),
            stats.clone(),
            resolver.clone(),
            Deferral::Handback(mem_spool()),
            HashSet::new(),
            None,
        )
        .await
        .unwrap();
        assert_eq!(outcome.rows_routed, 0, "handback routes no referrer");
        let spool = outcome.deferred.take().expect("referrer handed back");
        assert_eq!(spool.records(), 1);
        assert_eq!(
            stats.bootstrap_deferred_bytes.load(Ordering::Relaxed),
            spool.resident_bytes() as u64,
            "handed-back bytes stay charged until the caller replays them",
        );

        let resolved = drain_deferred(
            spool.into(),
            &catalog,
            &mapping,
            &msg_tx,
            &ack,
            &stats,
            &resolver,
            &Default::default(),
            None,
            outcome.next_seq,
            None,
            None,
        )
        .await
        .unwrap();
        assert_eq!(resolved.rows_routed, 1);
        assert_eq!(
            stats.bootstrap_deferred_bytes.load(Ordering::Relaxed),
            0,
            "replay releases the bytes it adopted",
        );
        drop(msg_tx);

        let rows = collect_rows(&mut msg_rx).await;
        assert_eq!(rows.len(), 1);
        let cols = &rows[0].1;
        assert_eq!(cols[0], build::bytes(b"hello"));
        drop(ack);
        collector.await.unwrap();
    }

    /// Page LSN separates reused TOAST IDs without changing row version.
    /// Retry unreadable, unreplaced rows at backup start. Drop replaced rows
    /// with unreadable values regardless of column order
    #[tokio::test]
    async fn window_coverage_reads_at_page_lsn_and_drops_only_replaced_rows() {
        const WINDOW: u64 = 0x2000;
        const LATE: u64 = WINDOW + 0x100;
        let mut rel = (*bytea_rel(16400)).clone();
        let mut attr = rel.attributes[0].clone();
        attr.attnum = 2;
        attr.name = "c".into();
        rel.attributes.push(attr);
        let mut catalog = CatalogMap::new();
        catalog.insert(Arc::new(rel));
        catalog.insert(toast_rel(16500));
        let mut table = (*bytea_mapping_for(16400)).clone();
        let mut col = table.columns[0].clone();
        col.src_attnum = 2;
        col.target_name = "c".into();
        table.columns.push(col);
        let mapping: MappingSnapshot = Arc::new(
            [(RelName::new("public", "t16400"), Arc::new(table))]
                .into_iter()
                .collect(),
        );
        let chunk = |value_id: u32, offnum: u16, body: &[u8], lsn: u64| ToastRow {
            db_oid: 5,
            toast_relid: 16500,
            blkno: 1,
            offnum,
            chunk_id: value_id,
            chunk_seq: 0,
            chunk_data: bytes::Bytes::copy_from_slice(body),
            lsn,
        };
        // IDs: 1 unchanged, 2 created in window, 3 deleted in window,
        // 4 deleted and reused in window with equal size
        let rows = [
            chunk(1, 1, b"hello", 0),
            chunk(2, 2, b"world", WINDOW),
            chunk(3, 3, b"gone!", 0),
            chunk(0, 3, b"", WINDOW),
            chunk(4, 4, b"olden", 0),
            chunk(0, 4, b"", WINDOW),
            chunk(4, 5, b"newer", WINDOW + 0x80),
        ];
        let slot = (
            RelFileNode {
                spc_node: 1663,
                db_node: 5,
                rel_node: 16400,
            },
            0,
            7,
        );
        let replaced = || WindowCoverage {
            page_bound: true,
            replaced: [slot].into_iter().collect(),
        };
        let unreplaced = || WindowCoverage {
            page_bound: true,
            replaced: HashSet::default(),
        };
        enum Want {
            Routed(&'static [u8], &'static [u8]),
            Dropped,
            Fatal,
        }
        let cases = [
            (
                (1, 2),
                LATE,
                Some(unreplaced()),
                Want::Routed(b"hello", b"world"),
            ),
            ((1, 2), LATE, None, Want::Fatal),
            ((1, 2), 0, Some(unreplaced()), Want::Fatal),
            ((3, 1), LATE, Some(replaced()), Want::Dropped),
            ((1, 3), LATE, Some(replaced()), Want::Dropped),
            (
                (3, 1),
                LATE,
                Some(unreplaced()),
                Want::Routed(b"gone!", b"hello"),
            ),
            (
                (1, 3),
                LATE,
                Some(unreplaced()),
                Want::Routed(b"hello", b"gone!"),
            ),
            (
                (4, 3),
                LATE,
                Some(unreplaced()),
                Want::Routed(b"olden", b"gone!"),
            ),
            (
                (1, 1),
                LATE,
                Some(replaced()),
                Want::Routed(b"hello", b"hello"),
            ),
            (
                (4, 1),
                0,
                Some(replaced()),
                Want::Routed(b"olden", b"hello"),
            ),
            (
                (4, 1),
                LATE,
                Some(unreplaced()),
                Want::Routed(b"newer", b"hello"),
            ),
        ];
        for ((b, c), page_lsn, coverage, want) in cases {
            let label = format!("values=({b}, {c}) page_lsn={page_lsn:#x} coverage={coverage:?}");
            let stats = Arc::new(EmitterStats::default());
            let resolver = ToastResolver::with_store(Arc::new(MemChunkStore::new()), stats.clone());
            resolver.put(&rows).await.unwrap();
            let (ack, collector) =
                ack::spawn(Arc::new(crate::pos::Monotone::default()), Fatal::new());
            let (msg_tx, mut msg_rx) = mpsc::channel::<BatcherMsg>(64);
            let mut referrer = referrer_tuple(16400, &[pointer(16500, b), pointer(16500, c)]);
            (referrer.blkno, referrer.offnum) = (slot.1, slot.2);
            referrer.page_lsn = page_lsn;
            let mut spool = mem_spool();
            spool.push(referrer).await.unwrap();
            let result = drain_deferred(
                spool.into(),
                &catalog,
                &mapping,
                &msg_tx,
                &ack,
                &stats,
                &resolver,
                &Default::default(),
                None,
                0,
                None,
                coverage.as_ref(),
            )
            .await;
            drop(msg_tx);
            let mut routed = Vec::new();
            while let Some(msg) = msg_rx.recv().await {
                routed.push(msg);
            }
            let covered = stats.bootstrap_replay_covered_rows.load(Ordering::Relaxed);
            match want {
                Want::Routed(b, c) => {
                    let resolved = result.unwrap_or_else(|e| panic!("{label}: {e}"));
                    assert_eq!(resolved.rows_routed, 1, "{label}");
                    let Some(BatcherMsg::Load(slab)) = routed.pop() else {
                        panic!("{label}: expected load rows");
                    };
                    let (_, chunk) = &slab.encoded()[0];
                    assert_eq!(
                        chunk.buffers[chunk.route.mapping.columns.len()].wire(),
                        crate::emit::ch_emitter::Wire::Fixed {
                            width: 8,
                            bytes: &0x1000u64.to_le_bytes(),
                        },
                        "{label}: load version moved with lookup bound"
                    );
                    assert_eq!(
                        chunk.cells(),
                        [[build::bytes(b), build::bytes(c)]],
                        "{label}"
                    );
                    assert_eq!(covered, 0, "{label}");
                }
                Want::Dropped => {
                    let resolved = result.unwrap_or_else(|e| panic!("{label}: {e}"));
                    assert_eq!(resolved.rows_routed, 0, "{label}");
                    assert!(routed.is_empty(), "{label}");
                    assert_eq!(covered, 1, "{label}");
                    assert_eq!(stats.toast_fetch_miss.load(Ordering::Relaxed), 0, "{label}");
                }
                Want::Fatal => {
                    result.expect_err(&label);
                    assert!(routed.is_empty(), "{label}");
                    assert_eq!(covered, 0, "{label}");
                }
            }
            drop(ack);
            collector.await.unwrap();
        }
    }

    /// Lanes share one gauge pair, so each charges its own spool and a
    /// finishing lane leaves its peers' bytes standing
    #[tokio::test]
    async fn lane_footprints_add_up_and_release_one_at_a_time() {
        let mut catalog = CatalogMap::new();
        catalog.insert(bytea_rel(16400));
        catalog.insert(toast_rel(16500));
        let mut tables = HashMap::new();
        tables.insert(RelName::new("public", "t16400"), bytea_mapping_for(16400));
        let mapping = Arc::new(tables);
        let stats = Arc::new(EmitterStats::default());
        let resolver = ToastResolver::with_store(Arc::new(MemChunkStore::new()), stats.clone());
        let (msg_tx, _msg_rx) = mpsc::channel::<BatcherMsg>(64);

        let mut lanes = Vec::new();
        for value_id in [1, 2] {
            let (ack, collector) =
                ack::spawn(Arc::new(crate::pos::Monotone::default()), Fatal::new());
            let (tup_tx, tup_rx) = mpsc::channel::<Vec<BackfillTuple>>(64);
            let mut chunk = toast_chunk_tuple(16500, value_id, 0, b"hello");
            // Store keys generations by TID, so each lane needs its own
            chunk.offnum = value_id as u16;
            tup_tx.send(vec![chunk]).await.unwrap();
            tup_tx
                .send(vec![bytea_toast_tuple(16400, 16500, value_id)])
                .await
                .unwrap();
            drop(tup_tx);
            let mut outcome = drain(
                tup_rx,
                catalog.clone(),
                freeze_routes(&mapping, None, &RowPolicy::default()),
                msg_tx.clone(),
                ack.clone(),
                stats.clone(),
                resolver.clone(),
                Deferral::Handback(mem_spool()),
                HashSet::new(),
                None,
            )
            .await
            .unwrap();
            let spool = outcome.deferred.take().expect("referrer handed back");
            lanes.push((spool, ack, collector, outcome.next_seq));
        }

        let mut remaining: u64 = lanes.iter().map(|(s, ..)| s.resident_bytes() as u64).sum();
        assert!(remaining > 0, "both lanes hold resident referrers");
        assert_eq!(
            stats.bootstrap_deferred_bytes.load(Ordering::Relaxed),
            remaining,
            "gauge sums the lanes",
        );

        for (spool, ack, collector, first_seq) in lanes {
            remaining -= spool.resident_bytes() as u64;
            drain_deferred(
                spool.into(),
                &catalog,
                &mapping,
                &msg_tx,
                &ack,
                &stats,
                &resolver,
                &Default::default(),
                None,
                first_seq,
                None,
                None,
            )
            .await
            .unwrap();
            assert_eq!(
                stats.bootstrap_deferred_bytes.load(Ordering::Relaxed),
                remaining,
                "replay releases only its own bytes",
            );
            drop(ack);
            collector.await.unwrap();
        }
    }

    /// Referrer deferred past the walk still routes under the pass's frozen
    /// mapping — one relation's initial load keeps one target shape
    #[tokio::test]
    async fn deferred_replay_routes_referrer() {
        let mut catalog = CatalogMap::new();
        catalog.insert(bytea_rel(16400));
        catalog.insert(toast_rel(16500));
        let mut tables = HashMap::new();
        tables.insert(RelName::new("public", "t16400"), bytea_mapping_for(16400));
        let mapping = Arc::new(tables);

        let emitter_ack = Arc::new(crate::pos::Monotone::default());
        let (ack, collector) = ack::spawn(emitter_ack, Fatal::new());
        let (msg_tx, mut msg_rx) = mpsc::channel::<BatcherMsg>(64);
        let (tup_tx, tup_rx) = mpsc::channel::<Vec<BackfillTuple>>(64);

        tup_tx
            .send(vec![toast_chunk_tuple(16500, 1, 0, b"hello")])
            .await
            .unwrap();
        tup_tx
            .send(vec![bytea_toast_tuple(16400, 16500, 1)])
            .await
            .unwrap();

        let stats = Arc::new(EmitterStats::default());
        let store = Arc::new(MemChunkStore::new());
        let drain_task = tokio::spawn(drain(
            tup_rx,
            catalog,
            freeze_routes(&mapping, None, &RowPolicy::default()),
            msg_tx,
            ack.clone(),
            stats.clone(),
            ToastResolver::with_store(store, stats.clone()),
            Deferral::Local(mem_spool()),
            HashSet::new(),
            None,
        ));
        drop(tup_tx);

        let rows = collect_rows(&mut msg_rx).await;
        let outcome = drain_task.await.unwrap().unwrap();
        assert_eq!(outcome.rows_routed, 1);
        assert_eq!(rows.len(), 1);
        let cols = &rows[0].1;
        assert_eq!(cols[0], build::bytes(b"hello"));
        drop(ack);
        collector.await.unwrap();
    }

    async fn replay_one(
        mapping: HashMap<RelName, Arc<TableMapping>>,
        resolver: ToastResolver,
        tuple: BackfillTuple,
    ) -> (Result<BootstrapDrainOutcome, String>, u64) {
        let mut catalog = CatalogMap::new();
        catalog.insert(bytea_rel(16400));
        let (ack, collector) = ack::spawn(Arc::default(), Fatal::new());
        let (msg_tx, mut msg_rx) = mpsc::channel::<BatcherMsg>(64);
        let mut spool = mem_spool();
        spool.push(tuple).await.unwrap();
        let stats = EmitterStats::default();
        let routes = freeze_routes(&Arc::new(mapping), None, &RowPolicy::default());
        let outcome = resolve_spooled(
            spool.into(),
            &routes,
            &catalog,
            &msg_tx,
            &ack,
            &stats,
            &resolver,
            0,
            None,
            None,
        )
        .await;
        drop(msg_tx);
        assert!(collect_rows(&mut msg_rx).await.is_empty());
        drop(ack);
        collector.await.unwrap();
        (outcome, stats.unsupported_relations.load(Ordering::Relaxed))
    }

    /// Replay drops referrers whose relation lost its route, checks value
    /// caps before any fetch, and fails on a referrer it cannot decode
    #[tokio::test]
    async fn deferred_replay_skips_unrouted_and_rejects_oversize() {
        let referrer = || bytea_toast_tuple(16400, 16500, 1);
        let (outcome, unsupported) =
            replay_one(HashMap::new(), ToastResolver::disabled(), referrer()).await;
        assert_eq!(outcome.unwrap().rows_routed, 0);
        assert_eq!(unsupported, 1);
        let tables = || [(RelName::new("public", "t16400"), bytea_mapping_for(16400))];
        let store = Arc::new(MemChunkStore::new());
        let resolver = ToastResolver::with_store(store, Arc::default())
            .with_inline_value_max(1)
            .with_overflow(crate::emit::ch_emitter::InlineValueOverflow::Error);
        let (outcome, _) = replay_one(tables().into_iter().collect(), resolver, referrer()).await;
        let e = outcome.expect_err("oversize value fails replay");
        assert!(e.starts_with("bootstrap: "), "{e}");
        let mut torn = referrer();
        torn.data = TupleData::Heap(Box::new([0xff]));
        let resolver = ToastResolver::disabled();
        let (outcome, _) = replay_one(tables().into_iter().collect(), resolver, torn).await;
        let e = outcome.expect_err("torn referrer fails replay");
        assert!(
            e.starts_with("bootstrap: public.t16400 tuple decode: "),
            "{e}"
        );
    }

    /// Replay order survives the batch boundary: batch N+1 resolves while
    /// batch N routes, so a late batch must not overtake an early one
    #[tokio::test]
    async fn deferred_replay_keeps_order_across_batches() {
        let rows = REPLAY_BATCH_ROWS + 1;
        let mut catalog = CatalogMap::new();
        catalog.insert(bytea_rel(16400));
        catalog.insert(toast_rel(16500));
        let mut tables = HashMap::new();
        tables.insert(RelName::new("public", "t16400"), bytea_mapping_for(16400));
        let mapping = Arc::new(tables);

        let (ack, collector) = ack::spawn(Arc::new(crate::pos::Monotone::default()), Fatal::new());
        let (msg_tx, mut msg_rx) = mpsc::channel::<BatcherMsg>(64);
        let (tup_tx, tup_rx) = mpsc::channel::<Vec<BackfillTuple>>(4);
        let spool_tmp = tempfile::tempdir().unwrap();

        // `bytea_toast_tuple` pins extsize at 5, so every body is 5 bytes
        let bodies: Vec<Vec<u8>> = (0..rows).map(|i| format!("{i:05}").into_bytes()).collect();
        let chunks = bodies
            .iter()
            .enumerate()
            .map(|(i, body)| {
                let mut t = toast_chunk_tuple(16500, i as u32 + 1, 0, body);
                t.blkno = i as u32;
                t.offnum = 1;
                t
            })
            .collect();
        tup_tx.send(chunks).await.unwrap();
        let referrers = (0..rows)
            .map(|i| bytea_toast_tuple(16400, 16500, i as u32 + 1))
            .collect();
        tup_tx.send(referrers).await.unwrap();
        drop(tup_tx);

        let stats = Arc::new(EmitterStats::default());
        let drain_task = tokio::spawn(drain(
            tup_rx,
            catalog,
            freeze_routes(&mapping, None, &RowPolicy::default()),
            msg_tx,
            ack.clone(),
            stats.clone(),
            ToastResolver::with_store(Arc::new(MemChunkStore::new()), stats.clone()),
            Deferral::Local(DeferredSpool::new(
                spool_tmp.path().join("bootstrap_deferred.bin"),
                0,
            )),
            HashSet::new(),
            None,
        ));

        let routed = collect_rows(&mut msg_rx).await;
        let outcome = drain_task.await.unwrap().unwrap();
        assert_eq!(outcome.rows_routed as usize, rows);
        let got: Vec<_> = routed.iter().map(|r| r.1[0].clone()).collect();
        let want: Vec<_> = bodies.into_iter().map(build::bytes).collect();
        assert_eq!(got, want, "replay order kept across the batch boundary");
        assert_eq!(
            stats.toast_value_fetch_batches.load(Ordering::Relaxed),
            2,
            "row cap sealed a second batch, one round trip each",
        );
        drop(ack);
        collector.await.unwrap();
    }

    /// Next batch's leaf acquire may wait on the routed batch's permit, so
    /// no routed row may sit in the coalescing buffer across it
    #[tokio::test]
    async fn deferred_replay_prefetch_never_waits_on_unsent_rows() {
        // 3.5 MiB values: buffer flushes every 2 rows, fetch cap seals 19
        const VALUE: usize = 7 << 19;
        let rows = 20;
        let mut catalog = CatalogMap::new();
        catalog.insert(bytea_rel(16400));
        catalog.insert(toast_rel(16500));
        let mut tables = HashMap::new();
        tables.insert(RelName::new("public", "t16400"), bytea_mapping_for(16400));
        let mapping = Arc::new(tables);

        let (ack, collector) = ack::spawn(Arc::new(crate::pos::Monotone::default()), Fatal::new());
        let (msg_tx, mut msg_rx) = mpsc::channel::<BatcherMsg>(64);
        let (tup_tx, tup_rx) = mpsc::channel::<Vec<BackfillTuple>>(4);
        let spool_tmp = tempfile::tempdir().unwrap();
        let chunks = (0..rows)
            .map(|i| {
                let mut t = toast_chunk_tuple(16500, i + 1, 0, &vec![b'x'; VALUE]);
                t.blkno = i;
                t
            })
            .collect();
        tup_tx.send(chunks).await.unwrap();
        let referrers = (0..rows)
            .map(|i| {
                let p = ToastPointer {
                    va_rawsize: VALUE as i32 + 4,
                    va_extinfo: VALUE as u32,
                    ..pointer(16500, i + 1)
                };
                referrer_tuple(16400, &[p])
            })
            .collect();
        tup_tx.send(referrers).await.unwrap();
        drop(tup_tx);

        let stats = Arc::new(EmitterStats::default());
        let resolver = ToastResolver::with_store(Arc::new(MemChunkStore::new()), stats.clone())
            .with_budget(crate::budget::MemoryBudget::new(2 << 20));
        let drain_task = tokio::spawn(drain(
            tup_rx,
            catalog,
            freeze_routes(&mapping, None, &RowPolicy::default()),
            msg_tx,
            ack.clone(),
            stats.clone(),
            resolver,
            Deferral::Local(DeferredSpool::new(
                spool_tmp.path().join("bootstrap_deferred.bin"),
                0,
            )),
            HashSet::new(),
            None,
        ));
        // Drop rows on receipt, as insert ack would
        let mut routed = 0;
        let count = async {
            while let Some(BatcherMsg::Load(slab)) = msg_rx.recv().await {
                routed += slab.rows.len();
            }
        };
        tokio::time::timeout(std::time::Duration::from_secs(5), count)
            .await
            .expect("replay finishes");
        assert_eq!(routed, rows as usize);
        drain_task.await.unwrap().unwrap();
        assert_eq!(
            stats.toast_value_fetch_batches.load(Ordering::Relaxed),
            2,
            "fetch cap sealed a second batch",
        );
        drop(ack);
        collector.await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn checkpoint_excludes_failed_prefetch_and_resumes_acknowledged_prefix() {
        use crate::backfill::backup_checkpoint::BackupCheckpoint;
        use crate::emit::ch_emitter::EmitterConfig;
        use crate::emit::pipeline::tail::OwnedTail;
        use crate::runtime_config::InitialLoadMode;
        use crate::toast::{ChunkStore, ChunkStoreError};

        struct FailPrefetch(AtomicU64);
        #[async_trait::async_trait]
        impl ChunkStore for FailPrefetch {
            async fn fetch_many(
                &self,
                _: u32,
                _: u32,
                values: &[(u32, usize)],
                _: u64,
            ) -> Result<Vec<FetchedValue>, ChunkStoreError> {
                tokio::time::advance(std::time::Duration::from_secs(31)).await;
                if self.0.fetch_add(1, Ordering::Relaxed) == 1 {
                    return Err(ChunkStoreError::Shadow("injected prefetch failure".into()));
                }
                Ok(values
                    .iter()
                    .map(|_| FetchedValue::Assembled(b"hello".to_vec()))
                    .collect())
            }
        }

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("bootstrap_deferred.bin");
        let mut spool = DeferredSpool::new(path.clone(), 0);
        for _ in 0..=REPLAY_BATCH_ROWS {
            spool
                .push(bytea_toast_tuple(16400, 16500, 1))
                .await
                .unwrap();
        }
        let mark = spool.checkpoint().await.unwrap();
        let total = spool.spooled_bytes();
        let mut state = BackupCheckpoint::new(
            InitialLoadMode::ObjectStore,
            &[],
            &Arc::default(),
            &EmitterConfig::default(),
            None,
        );
        state.spool = mark;
        state.save(dir.path()).await.unwrap();
        let mut catalog = CatalogMap::new();
        catalog.insert(bytea_rel(16400));
        let mut tables = HashMap::new();
        tables.insert(RelName::new("public", "t16400"), bytea_mapping_for(16400));
        let mapping = Arc::new(tables);
        let stats = Arc::new(EmitterStats::default());
        let resolver =
            ToastResolver::with_store(Arc::new(FailPrefetch(AtomicU64::new(0))), stats.clone());
        let tail = OwnedTail::null();
        stats
            .bootstrap_deferred_spool_bytes
            .store(total, Ordering::Relaxed);
        let result = drain_deferred(
            spool.into(),
            &catalog,
            &mapping,
            &tail.msg_tx,
            &tail.ack,
            &stats,
            &resolver,
            &Default::default(),
            None,
            0,
            Some(ReplayCheckpoint {
                state: &mut state,
                dir: dir.path(),
                tail: &tail,
            }),
            None,
        )
        .await;
        assert!(result.unwrap_err().contains("injected prefetch failure"));
        let mut saved = BackupCheckpoint::load(dir.path()).await.unwrap().unwrap();
        assert_eq!(saved.rows, REPLAY_BATCH_ROWS as u64);
        assert_eq!(saved.offset, total - total / saved.spool.records);
        assert!(path.exists());
        tail.finish(1).await.unwrap();

        let spool = DeferredSpool::resume(path.clone(), saved.spool, saved.offset)
            .await
            .unwrap();
        stats
            .bootstrap_deferred_spool_bytes
            .store(total, Ordering::Relaxed);
        let tail = OwnedTail::null();
        let result = drain_deferred(
            spool.into(),
            &catalog,
            &mapping,
            &tail.msg_tx,
            &tail.ack,
            &stats,
            &resolver,
            &Default::default(),
            None,
            0,
            Some(ReplayCheckpoint {
                state: &mut saved,
                dir: dir.path(),
                tail: &tail,
            }),
            None,
        )
        .await
        .unwrap();
        assert_eq!(result.rows_routed, REPLAY_BATCH_ROWS as u64 + 1);
        assert_eq!(saved.offset, total);
        assert_eq!(result.next_seq, 1);
        assert!(path.exists(), "retain spool until pass publishes");
        tail.finish(result.next_seq).await.unwrap();
    }

    #[tokio::test]
    async fn deferred_replay_isolates_database_value_ids() {
        use crate::toast::ChunkStore;

        let mut stores = Vec::new();
        let mut batch = ReplayBatch {
            tables: Vec::new(),
            rows: Vec::new(),
            need: 0,
            resident: 0,
        };
        for (db_oid, body) in [(1, b"first"), (2, b"other")] {
            let store = Arc::new(MemChunkStore::new());
            let mut toast = (*toast_rel(16500)).clone();
            toast.rfn.db_node = db_oid;
            store.put(&[chunk_row(&toast, 1, body)]).await.unwrap();
            stores.push((db_oid, store as Arc<dyn ChunkStore>));
            let mut tuple = bytea_toast_tuple(16400, 16500, 1);
            tuple.rfn.db_node = db_oid;
            let route = RouteSnapshot::freeze(
                bytea_mapping_for(16400),
                Arc::default(),
                RowPolicy::default(),
            );
            let rel = bytea_rel(16400);
            let view = tuple.view(&rel).unwrap().unwrap();
            let pointers = mapped_pointers(&view, &route.mapping);
            drop(view);
            batch.tables.push(RowTable { rel, route });
            batch.rows.push(ReplayRow {
                tuple,
                table: batch.tables.len() as u32 - 1,
                pointers,
                patch: Vec::new(),
                replay_covered: false,
            });
        }
        let resolver = ToastResolver::with_store(stores[0].1.clone(), Arc::default())
            .with_database_stores(stores);
        resolve_batch(&mut batch, &resolver, &EmitterStats::default(), None)
            .await
            .unwrap();
        for (row, expected) in batch.rows.iter().zip([b"first", b"other"]) {
            assert_eq!(row.patch, [(0, build::bytes(expected))]);
        }
    }

    /// One round trip per mirror resolves a whole batch, distinct values
    /// fetched once however many referrers share them, rows routed in
    /// replay order
    #[tokio::test]
    async fn deferred_replay_fetches_once_per_mirror() {
        /// `toast_chunk_tuple` pins one TID; a mirror needs its rows apart
        fn chunk_at(value_id: u32, body: &[u8], tid: (u32, u16), rel_node: u32) -> BackfillTuple {
            let mut t = toast_chunk_tuple(rel_node, value_id, 0, body);
            t.blkno = tid.0;
            t.offnum = tid.1;
            t
        }

        let mut catalog = CatalogMap::new();
        catalog.insert(bytea_rel(16400));
        catalog.insert(bytea_rel(16401));
        catalog.insert(toast_rel(16500));
        catalog.insert(toast_rel(16600));
        let mut tables = HashMap::new();
        tables.insert(RelName::new("public", "t16400"), bytea_mapping_for(16400));
        tables.insert(RelName::new("public", "t16401"), bytea_mapping_for(16401));
        let mapping = Arc::new(tables);

        let (ack, collector) = ack::spawn(Arc::new(crate::pos::Monotone::default()), Fatal::new());
        let (msg_tx, mut msg_rx) = mpsc::channel::<BatcherMsg>(64);
        let (tup_tx, tup_rx) = mpsc::channel::<Vec<BackfillTuple>>(64);
        let spool_tmp = tempfile::tempdir().unwrap();

        tup_tx
            .send(vec![
                chunk_at(1, b"hello", (1, 1), 16500),
                chunk_at(2, b"world", (1, 2), 16500),
            ])
            .await
            .unwrap();
        tup_tx
            .send(vec![chunk_at(3, b"there", (1, 1), 16600)])
            .await
            .unwrap();
        // Value 1 referred to twice: an UPDATE leaves the pointer alone
        tup_tx
            .send(vec![
                bytea_toast_tuple(16400, 16500, 1),
                bytea_toast_tuple(16400, 16500, 1),
                bytea_toast_tuple(16400, 16500, 2),
            ])
            .await
            .unwrap();
        tup_tx
            .send(vec![bytea_toast_tuple(16401, 16600, 3)])
            .await
            .unwrap();
        drop(tup_tx);

        let stats = Arc::new(EmitterStats::default());
        let drain_task = tokio::spawn(drain(
            tup_rx,
            catalog,
            freeze_routes(&mapping, None, &RowPolicy::default()),
            msg_tx,
            ack.clone(),
            stats.clone(),
            ToastResolver::with_store(Arc::new(MemChunkStore::new()), stats.clone()),
            Deferral::Local(DeferredSpool::new(
                spool_tmp.path().join("bootstrap_deferred.bin"),
                0,
            )),
            HashSet::new(),
            None,
        ));

        let rows = collect_rows(&mut msg_rx).await;
        let outcome = drain_task.await.unwrap().unwrap();
        assert_eq!(outcome.rows_routed, 4);
        let bodies: Vec<_> = rows.iter().map(|r| r.1[0].clone()).collect();
        assert_eq!(
            bodies,
            [b"hello", b"hello", b"world", b"there"]
                .map(build::bytes)
                .to_vec(),
            "replay order kept, shared value resolved for both referrers",
        );
        assert!(
            rows.iter().all(|r| r.0 == rows[0].0),
            "one trailing seq for the replay",
        );
        assert_eq!(
            stats.toast_value_fetch_batches.load(Ordering::Relaxed),
            2,
            "one round trip per mirror, not per referrer",
        );
        assert_eq!(
            stats.toast_values_fetched.load(Ordering::Relaxed),
            3,
            "distinct values fetched once each",
        );
        drop(ack);
        collector.await.unwrap();
    }

    /// Spool whose file sits below a regular file, so creating it fails
    fn unwritable_spool(dir: &tempfile::TempDir, mem_max: usize) -> DeferredSpool {
        std::fs::write(dir.path().join("file"), b"").unwrap();
        DeferredSpool::new(dir.path().join("file/spool.bin"), mem_max)
    }

    async fn drain_referrer(
        deferral: Deferral,
        barrier: Option<Arc<WalkBarrier>>,
    ) -> Result<BootstrapDrainOutcome, String> {
        let mut catalog = CatalogMap::new();
        catalog.insert(bytea_rel(16400));
        let mapping = Arc::new(HashMap::from_iter([(
            RelName::new("public", "t16400"),
            bytea_mapping_for(16400),
        )]));
        let (ack, collector) = ack::spawn(Arc::default(), Fatal::new());
        let (msg_tx, _msg_rx) = mpsc::channel::<BatcherMsg>(64);
        let (tup_tx, tup_rx) = mpsc::channel::<Vec<BackfillTuple>>(64);
        tup_tx
            .send(vec![bytea_toast_tuple(16400, 16500, 1)])
            .await
            .unwrap();
        drop(tup_tx);
        let stats = Arc::new(EmitterStats::default());
        let outcome = drain(
            tup_rx,
            catalog,
            freeze_routes(&mapping, None, &RowPolicy::default()),
            msg_tx,
            ack.clone(),
            stats.clone(),
            ToastResolver::with_store(Arc::new(MemChunkStore::new()), stats),
            deferral,
            HashSet::new(),
            barrier,
        )
        .await;
        drop(ack);
        collector.await.unwrap();
        outcome
    }

    #[tokio::test]
    async fn unwritable_deferred_spool_fails_the_drain() {
        let dir = tempfile::tempdir().unwrap();
        let err = drain_referrer(Deferral::Local(unwritable_spool(&dir, 0)), None)
            .await
            .unwrap_err();
        assert!(err.starts_with("bootstrap: deferred spool: "), "{err}");

        let barrier = Arc::new(WalkBarrier::new(std::time::Duration::ZERO));
        let spool = unwritable_spool(&dir, DEFERRED_SPOOL_MEM_MAX);
        let err = drain_referrer(Deferral::Local(spool), Some(barrier))
            .await
            .unwrap_err();
        assert!(
            err.starts_with("bootstrap: deferred spool checkpoint: "),
            "{err}"
        );
    }

    /// Checkpoint behind an empty end-of-file slab has no open run to place
    #[tokio::test]
    async fn checkpoint_without_spool_or_open_run_keeps_seqs() {
        let mut catalog = CatalogMap::new();
        catalog.insert(rel(16400));
        let mapping = Arc::new(HashMap::from_iter([(
            RelName::new("public", "t16400"),
            mapping_for(16400),
        )]));
        let (ack, collector) = ack::spawn(Arc::default(), Fatal::new());
        let (msg_tx, mut msg_rx) = mpsc::channel::<BatcherMsg>(64);
        let (tup_tx, tup_rx) = mpsc::channel::<Vec<BackfillTuple>>(64);
        tup_tx.send(vec![tuple(16400, 1)]).await.unwrap();
        tup_tx.send(Vec::new()).await.unwrap();
        drop(tup_tx);
        let outcome = drain(
            tup_rx,
            catalog,
            freeze_routes(&mapping, None, &RowPolicy::default()),
            msg_tx,
            ack.clone(),
            Arc::default(),
            ToastResolver::disabled(),
            Deferral::Rejected,
            HashSet::new(),
            Some(Arc::new(WalkBarrier::new(std::time::Duration::ZERO))),
        )
        .await
        .unwrap();
        assert_eq!((outcome.next_seq, outcome.rows_routed), (1, 1));
        assert_eq!(collect_rows(&mut msg_rx).await.len(), 1);
        drop(ack);
        collector.await.unwrap();
    }

    /// Checkpointed replay of a batch whose every row window replay covers
    /// advances the saved offset without a seq to place
    #[tokio::test]
    async fn checkpointed_replay_of_covered_rows_places_no_seq() {
        use crate::backfill::backup_checkpoint::BackupCheckpoint;
        use crate::emit::ch_emitter::EmitterConfig;
        use crate::emit::pipeline::tail::OwnedTail;
        use crate::runtime_config::InitialLoadMode;

        let dir = tempfile::tempdir().unwrap();
        let referrer = bytea_toast_tuple(16400, 16500, 1);
        let coverage = WindowCoverage {
            page_bound: false,
            replaced: [(referrer.rfn, referrer.blkno, referrer.offnum)]
                .into_iter()
                .collect(),
        };
        let mut spool = DeferredSpool::new(dir.path().join("spool.bin"), 0);
        spool.push(referrer).await.unwrap();
        spool.checkpoint().await.unwrap();
        let total = spool.spooled_bytes();
        let mut catalog = CatalogMap::new();
        catalog.insert(bytea_rel(16400));
        catalog.insert(toast_rel(16500));
        let mapping = Arc::new(HashMap::from_iter([(
            RelName::new("public", "t16400"),
            bytea_mapping_for(16400),
        )]));
        let stats = Arc::new(EmitterStats::default());
        stats
            .bootstrap_deferred_spool_bytes
            .store(total, Ordering::Relaxed);
        // Mirror exists but lost value 1
        let resolver = ToastResolver::with_store(Arc::new(MemChunkStore::new()), stats.clone());
        let other = chunk_row(&toast_rel(16500), 9, b"x");
        resolver.put(&[other]).await.unwrap();
        let mut state = BackupCheckpoint::new(
            InitialLoadMode::ObjectStore,
            &[],
            &Arc::default(),
            &EmitterConfig::default(),
            None,
        );
        let tail = OwnedTail::null();
        let outcome = drain_deferred(
            spool.into(),
            &catalog,
            &mapping,
            &tail.msg_tx,
            &tail.ack,
            &stats,
            &resolver,
            &RowPolicy::default(),
            None,
            0,
            Some(ReplayCheckpoint {
                state: &mut state,
                dir: dir.path(),
                tail: &tail,
            }),
            Some(&coverage),
        )
        .await
        .unwrap();
        assert_eq!((outcome.next_seq, outcome.rows_routed), (0, 0));
        assert_eq!((state.offset, state.rows), (total, 0));
        assert_eq!(
            stats.bootstrap_replay_covered_rows.load(Ordering::Relaxed),
            1
        );
        tail.finish(0).await.unwrap();
    }

    /// File-backed spool of one unmapped referrer
    async fn file_spool(dir: &tempfile::TempDir) -> DeferredSpool {
        let mut spool = DeferredSpool::new(dir.path().join("spool.bin"), 0);
        spool
            .push(bytea_toast_tuple(16400, 16500, 1))
            .await
            .unwrap();
        spool.checkpoint().await.unwrap();
        spool
    }

    async fn replay_spool(
        spool: DeferredSpool,
        catalog: &CatalogMap,
        resolver: &ToastResolver,
    ) -> (
        Result<BootstrapDrainOutcome, String>,
        Vec<(u64, Vec<Datum>)>,
    ) {
        let mapping = Arc::new(HashMap::from_iter([(
            RelName::new("public", "t16400"),
            bytea_mapping_for(16400),
        )]));
        let (ack, collector) = ack::spawn(Arc::default(), Fatal::new());
        let (msg_tx, mut msg_rx) = mpsc::channel::<BatcherMsg>(64);
        let outcome = drain_deferred(
            spool.into(),
            catalog,
            &mapping,
            &msg_tx,
            &ack,
            &EmitterStats::default(),
            resolver,
            &RowPolicy::default(),
            None,
            0,
            None,
            None,
        )
        .await;
        drop(msg_tx);
        let rows = collect_rows(&mut msg_rx).await;
        drop(ack);
        collector.await.unwrap();
        (outcome, rows)
    }

    #[tokio::test]
    async fn deferred_replay_reports_spool_faults() {
        let catalog = CatalogMap::new();
        let resolver = ToastResolver::disabled();

        let dir = tempfile::tempdir().unwrap();
        let spool = file_spool(&dir).await;
        std::fs::remove_file(spool.path()).unwrap();
        let err = replay_spool(spool, &catalog, &resolver)
            .await
            .0
            .unwrap_err();
        assert!(err.starts_with("bootstrap: deferred spool seal: "), "{err}");

        let spool = file_spool(&dir).await;
        std::fs::OpenOptions::new()
            .write(true)
            .open(spool.path())
            .unwrap()
            .set_len(6)
            .unwrap();
        let err = replay_spool(spool, &catalog, &resolver)
            .await
            .0
            .unwrap_err();
        assert!(
            err.starts_with("bootstrap: deferred spool replay: "),
            "{err}"
        );

        // Directory without write permission keeps replayed file in place
        use std::os::unix::fs::PermissionsExt;
        std::fs::remove_file(dir.path().join("spool.bin")).unwrap();
        let spool = file_spool(&dir).await;
        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o500)).unwrap();
        let (outcome, _) = replay_spool(spool, &catalog, &resolver).await;
        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        let err = outcome.unwrap_err();
        assert!(
            err.starts_with("bootstrap: deferred spool cleanup: "),
            "{err}"
        );
        assert!(dir.path().join("spool.bin").exists());
    }

    /// No store to consult: replayed referrer routes with its value filled
    #[tokio::test]
    async fn deferred_replay_without_store_fills_referrers() {
        let mut catalog = CatalogMap::new();
        catalog.insert(bytea_rel(16400));
        let dir = tempfile::tempdir().unwrap();
        let (outcome, rows) =
            replay_spool(file_spool(&dir).await, &catalog, &ToastResolver::disabled()).await;
        assert_eq!(outcome.unwrap().rows_routed, 1);
        assert_eq!(
            rows.iter()
                .map(|(_, cells)| cells.clone())
                .collect::<Vec<_>>(),
            [vec![Datum::Null]]
        );
        assert!(!dir.path().join("spool.bin").exists());
    }
}
