//! Bootstrap page-walk producer
//!
//! Every row uses `start_lsn`. Persist TOAST mirrors before resolving deferred
//! referrers. Caller waits through synthetic sequence frontier before advancing
//! resume LSN to backup end

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use tokio::sync::mpsc;

use crate::backfill::backup_page_walk::{BackfillTuple, CatalogMap};
use crate::backfill::spool::{DeferredReader, DeferredSpool};
use crate::config::ResolvedConfig;
use crate::decode::heap_decoder::{ColumnValue, ToastPointer};
use crate::emit::ch_emitter::EmitterStats;
use crate::emit::pipeline::ack::AckHandle;
use crate::emit::pipeline::batcher::{BatcherMsg, RoutedRow, RowChunk};
use crate::emit::pipeline::decode::DECODE_CHUNK_BYTES;
use crate::emit::route::{RouteSnapshot, RowPolicy, freeze_routes};
use crate::mapping::{MappingSnapshot, TableMapping};
use crate::ops::oracle::render_ext_columns;
use crate::schema::{RelDescriptor, RelName};
use crate::toast::{
    FetchedValue, ToastResolver, ToastRow, check_value_caps, detoasted_value, finish_value,
    pointer_extsize,
};
use ahash::{HashMap, HashMapExt, HashSet};

/// Rows per `BatcherMsg::Rows` from the bootstrap drain. Fixed rather than
/// config-driven: bootstrap rows are uniform inserts, and the byte trigger
/// covers the fat-row case
const DRAIN_CHUNK_ROWS: usize = 1024;

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
    fn adopt(stats: &'a EmitterStats, spool: &DeferredSpool) -> Self {
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
    mapping: MappingSnapshot,
    msg_tx: mpsc::Sender<BatcherMsg>,
    ack: AckHandle,
    stats: Arc<EmitterStats>,
    resolver: ToastResolver,
    deferral: Deferral,
    row_policy: RowPolicy,
    config: Option<Arc<ResolvedConfig>>,
    skip_initial: HashSet<RelName>,
) -> Result<BootstrapDrainOutcome, String> {
    let (mut deferred, handback) = match deferral {
        Deferral::Rejected => (None, false),
        Deferral::Local(spool) => (Some(spool), false),
        Deferral::Handback(spool) => (Some(spool), true),
    };
    let routes = freeze_routes(&mapping, config.as_deref(), &row_policy);
    let mut footprint = DeferredFootprint::new(&stats);
    let mut next_seq = 0;
    let mut rows_routed = 0;
    let mut open = None;
    let mut chunk_batch = Vec::new();
    let mut chunk_batch_bytes = 0;
    let mut out = RowBuf::default();
    while let Some(batch) = rx.recv().await {
        for tuple in batch {
            let rfn = tuple.rfn;
            let source_lsn = tuple.source_lsn;

            let same = matches!(&open, Some((r, _, _)) if *r == rfn);
            let seq = if same {
                open.as_ref().expect("same implies open").1
            } else {
                if let Some((_, prev_seq, prev_rows)) = open.take() {
                    // Every row of the closing seq on the channel before its
                    // expected count is published
                    out.flush(&msg_tx).await?;
                    ack.placed(prev_seq, prev_rows);
                }
                let s = next_seq;
                next_seq += 1;
                ack.register(s, source_lsn);
                open = Some((rfn, s, 0));
                s
            };

            let Some(rel) = catalog.get(rfn.db_node, rfn.rel_node) else {
                stats.unsupported_relations.fetch_add(1, Ordering::Relaxed);
                continue;
            };

            if skip_initial.contains(&rel.rel_name) {
                continue;
            }

            if catalog.is_toast(rfn.db_node, rfn.rel_node) {
                if let Some(row) = row_from_columns(tuple, rel.oid) {
                    chunk_batch_bytes += row.chunk_data.len();
                    chunk_batch.push(row);
                    if resolver.put_limit_reached(chunk_batch.len(), chunk_batch_bytes) {
                        flush_chunks(&resolver, &mut chunk_batch).await?;
                        chunk_batch_bytes = 0;
                    }
                }
                continue;
            }

            let Some(route) = routes.get(&rel.rel_name).cloned() else {
                stats.unsupported_relations.fetch_add(1, Ordering::Relaxed);
                continue;
            };

            let mut tuple = tuple;
            let mut permit = None;
            if tuple.has_mapped_external(&route.mapping) {
                // A store owes this value, so it cannot be rendered here: the
                // mirror has not been written yet on this pass, and a
                // shadow-backed read has nothing serving it until the walk is
                // done. Only a resolver with no store at all fills inline
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
                permit = resolve_or_fill_toast(&mut tuple, &rel, &route.mapping, &resolver)
                    .await?
                    .map(Arc::new);
            }
            render_ext_columns(&rel.attributes, &mut tuple.columns);
            out.push(&msg_tx, seq, rel, route, tuple, permit).await?;
            bump(&mut open, &mut rows_routed);
        }
    }
    out.flush(&msg_tx).await?;
    if let Some((_, seq, rows)) = open.take() {
        ack.placed(seq, rows);
    }

    if !chunk_batch.is_empty() {
        flush_chunks(&resolver, &mut chunk_batch).await?;
    }

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
                spool, &routes, &catalog, &msg_tx, &ack, &stats, &resolver, next_seq,
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
    spool: DeferredSpool,
    catalog: &CatalogMap,
    mapping: &MappingSnapshot,
    msg_tx: &mpsc::Sender<BatcherMsg>,
    ack: &AckHandle,
    stats: &EmitterStats,
    resolver: &ToastResolver,
    row_policy: &RowPolicy,
    config: Option<&ResolvedConfig>,
    first_seq: u64,
) -> Result<BootstrapDrainOutcome, String> {
    let routes = freeze_routes(mapping, config, row_policy);
    resolve_spooled(
        spool, &routes, catalog, msg_tx, ack, stats, resolver, first_seq,
    )
    .await
}

/// Route spooled referrers under one trailing seq, registered on the first
/// row that routes so an all-unmapped spool leaves no seq to prove
#[allow(clippy::too_many_arguments)]
async fn resolve_spooled(
    spool: DeferredSpool,
    routes: &ahash::HashMap<RelName, Arc<RouteSnapshot>>,
    catalog: &CatalogMap,
    msg_tx: &mpsc::Sender<BatcherMsg>,
    ack: &AckHandle,
    stats: &EmitterStats,
    resolver: &ToastResolver,
    first_seq: u64,
) -> Result<BootstrapDrainOutcome, String> {
    tracing::info!(
        target: "walshadow::bootstrap",
        deferred = spool.records(),
        spooled_bytes = spool.spooled_bytes(),
        "resolving deferred TOAST tuples from chunk store",
    );
    // Released on drop, so a failed replay gives its bytes back too
    let _footprint = DeferredFootprint::adopt(stats, &spool);
    let mut out = RowBuf::default();
    let mut seq = None;
    let mut placed = 0u64;
    let mut replay = spool
        .into_reader()
        .await
        .map_err(|e| format!("bootstrap: deferred spool seal: {e}"))?;
    // Read and fetch the next batch while this one routes: spool reads and
    // inserts otherwise leave the store pool idle
    let mut ready = prepare_batch(&mut replay, routes, catalog, stats, resolver).await?;
    while let Some(batch) = ready.take() {
        let (next, routed) = tokio::join!(
            prepare_batch(&mut replay, routes, catalog, stats, resolver),
            route_batch(
                batch,
                &mut out,
                msg_tx,
                ack,
                first_seq,
                &mut seq,
                &mut placed
            ),
        );
        routed?;
        ready = next?;
    }
    out.flush(msg_tx).await?;
    replay
        .finish()
        .await
        .map_err(|e| format!("bootstrap: deferred spool cleanup: {e}"))?;
    let next_seq = match seq {
        Some(s) => {
            ack.placed(s, placed);
            s + 1
        }
        None => first_seq,
    };
    Ok(BootstrapDrainOutcome {
        next_seq,
        rows_routed: placed,
        deferred: None,
    })
}

fn bump(open: &mut Option<(walrus::pg::walparser::RelFileNode, u64, u64)>, rows_routed: &mut u64) {
    if let Some(slot) = open.as_mut() {
        slot.2 += 1;
    }
    *rows_routed += 1;
}

/// Coalesces routed rows into one `BatcherMsg::Rows` per
/// [`DECODE_CHUNK_BYTES`]-shaped trigger, the same amortization the streaming
/// decode pool gets. Rows of different seqs may share a chunk; the batcher
/// routes each independently
#[derive(Default)]
struct RowBuf {
    rows: Vec<RoutedRow>,
    bytes: usize,
}

impl RowBuf {
    async fn push(
        &mut self,
        msg_tx: &mpsc::Sender<BatcherMsg>,
        seq: u64,
        rel: Arc<RelDescriptor>,
        route: Arc<RouteSnapshot>,
        tuple: BackfillTuple,
        value_permit: Option<Arc<crate::budget::MemoryPermit>>,
    ) -> Result<(), String> {
        let committed = tuple.into_committed_insert();
        self.bytes += committed.decoded.approx_bytes();
        self.rows.push(RoutedRow {
            seq,
            rel,
            route,
            committed,
            value_permit,
        });
        if self.rows.len() >= DRAIN_CHUNK_ROWS || self.bytes >= DECODE_CHUNK_BYTES {
            self.flush(msg_tx).await?;
        }
        Ok(())
    }

    async fn flush(&mut self, msg_tx: &mpsc::Sender<BatcherMsg>) -> Result<(), String> {
        if self.rows.is_empty() {
            return Ok(());
        }
        self.bytes = 0;
        msg_tx
            .send(BatcherMsg::Rows(RowChunk {
                rows: std::mem::take(&mut self.rows),
                permit: None,
            }))
            .await
            .map_err(|_| "bootstrap: batcher channel closed".to_string())
    }
}

const REPLAY_BATCH_ROWS: usize = 65536;

const REPLAY_FETCH_BYTES: usize = 64 << 20;

/// `(toast_relid, value_id, as-of bound)`. The bound is part of the key:
/// a resumed load tags relations with their own `_lsn`
type ValueKey = (u32, u32, u64);

/// Routed referrers awaiting their values, with the leaf bytes resolving
/// them peaks at
struct ReplayBatch {
    rows: Vec<ReplayRow>,
    /// Leaf permit bytes: what the round's values retain plus its largest
    /// decompression transient
    need: usize,
    /// Inline bytes the held tuples already carry
    resident: usize,
}

struct ReplayRow {
    tuple: BackfillTuple,
    rel: Arc<RelDescriptor>,
    route: Arc<RouteSnapshot>,
    pointers: Vec<PointerSite>,
}

/// Mapped external pointer: its tuple column, its mapping column (the miss
/// message's target name) and the pointer itself
struct PointerSite {
    idx: usize,
    col: usize,
    p: ToastPointer,
}

/// Check only columns routed to ClickHouse
fn mapped_pointers(tuple: &BackfillTuple, mapping: &TableMapping) -> Vec<PointerSite> {
    mapping
        .columns
        .iter()
        .enumerate()
        .filter_map(|(col, c)| {
            let idx = usize::try_from(c.src_attnum as i32 - 1).ok()?;
            let Some(ColumnValue::ExternalToast(p)) = tuple.columns.get(idx)? else {
                return None;
            };
            Some(PointerSite { idx, col, p: *p })
        })
        .collect()
}

/// Batch whose values are resolved, holding the leaf permit its rows ride
struct ResolvedReplayBatch {
    rows: Vec<ReplayRow>,
    permit: Option<Arc<crate::budget::MemoryPermit>>,
}

/// Read one batch and resolve its values; `None` at spool end
async fn prepare_batch(
    replay: &mut DeferredReader,
    routes: &ahash::HashMap<RelName, Arc<RouteSnapshot>>,
    catalog: &CatalogMap,
    stats: &EmitterStats,
    resolver: &ToastResolver,
) -> Result<Option<ResolvedReplayBatch>, String> {
    let mut batch = next_batch(replay, routes, catalog, stats, resolver).await?;
    if batch.rows.is_empty() {
        return Ok(None);
    }
    // One permit per batch: every value a round fetched stays resident
    // until its row routes, so the rows share what survives
    let mut leaf = crate::budget::acquire_opt(resolver.budget(), batch.need).await;
    let retained = resolve_batch(&mut batch, resolver).await?;
    if let Some(p) = leaf.as_mut() {
        p.shrink(retained as u64);
    }
    Ok(Some(ResolvedReplayBatch {
        rows: batch.rows,
        permit: leaf.map(Arc::new),
    }))
}

/// Route a resolved batch's rows, each under the replay's trailing seq
async fn route_batch(
    batch: ResolvedReplayBatch,
    out: &mut RowBuf,
    msg_tx: &mpsc::Sender<BatcherMsg>,
    ack: &AckHandle,
    first_seq: u64,
    seq: &mut Option<u64>,
    placed: &mut u64,
) -> Result<(), String> {
    let ResolvedReplayBatch { rows, permit } = batch;
    for row in rows {
        let mut tuple = row.tuple;
        let at = *seq.get_or_insert_with(|| {
            ack.register(first_seq, tuple.source_lsn);
            first_seq
        });
        render_ext_columns(&row.rel.attributes, &mut tuple.columns);
        out.push(msg_tx, at, row.rel, row.route, tuple, permit.clone())
            .await?;
        *placed += 1;
    }
    Ok(())
}

/// Read referrers until the batch seals or the spool ends, dropping rows no
/// mapped relation owns. Value caps are checked here, before any fetch
async fn next_batch(
    replay: &mut DeferredReader,
    routes: &ahash::HashMap<RelName, Arc<RouteSnapshot>>,
    catalog: &CatalogMap,
    stats: &EmitterStats,
    resolver: &ToastResolver,
) -> Result<ReplayBatch, String> {
    let mut batch = ReplayBatch {
        rows: Vec::new(),
        need: 0,
        resident: 0,
    };
    while batch.rows.len() < REPLAY_BATCH_ROWS && batch.need + batch.resident < REPLAY_FETCH_BYTES {
        let Some(tuple) = replay
            .next()
            .await
            .map_err(|e| format!("bootstrap: deferred spool replay: {e}"))?
        else {
            break;
        };
        let Some(rel) = catalog.get(tuple.rfn.db_node, tuple.rfn.rel_node) else {
            stats.unsupported_relations.fetch_add(1, Ordering::Relaxed);
            continue;
        };
        let Some(route) = routes.get(&rel.rel_name).cloned() else {
            stats.unsupported_relations.fetch_add(1, Ordering::Relaxed);
            continue;
        };
        let pointers = mapped_pointers(&tuple, &route.mapping);
        batch.need += check_value_caps(
            pointers.iter().map(|site| site.p),
            resolver.inline_value_max(),
        )
        .map_err(|e| format!("bootstrap: {e}"))?;
        batch.resident += crate::backfill::spool::approx_bytes(&tuple);
        batch.rows.push(ReplayRow {
            tuple,
            rel,
            route,
            pointers,
        });
    }
    Ok(batch)
}

/// Fetch the batch's values, one round trip per mirror and bound, then
/// detoast every referrer's columns. Returns the bytes the rows retain
async fn resolve_batch(batch: &mut ReplayBatch, resolver: &ToastResolver) -> Result<usize, String> {
    let mut values = fetch_batch_values(batch, resolver).await?;
    let mut retained = 0usize;
    for ReplayRow {
        tuple,
        rel,
        route,
        pointers,
    } in &mut batch.rows
    {
        for site in pointers.iter() {
            let key = (site.p.va_toastrelid, site.p.va_valueid, tuple.source_lsn);
            let fetched = values.take(key);
            let type_oid = rel.attributes.get(site.idx).map_or(0, |a| a.type_oid);
            let target = &route.mapping.columns[site.col].target_name;
            let (column, bytes) = apply_fetched(fetched, &site.p, type_oid, rel, target, resolver)?;
            retained += bytes;
            tuple.columns[site.idx] = Some(column);
        }
    }
    Ok(retained)
}

/// A batch's values, with how many referrers each one still owes
struct FetchedValues {
    /// `None` without store: nothing to consult, every referrer fills
    values: Option<HashMap<ValueKey, FetchedValue>>,
    uses: HashMap<ValueKey, u32>,
}

impl FetchedValues {
    /// Last referrer of a value takes its bytes, earlier ones copy
    fn take(&mut self, key: ValueKey) -> Option<FetchedValue> {
        let left = self.uses.get_mut(&key)?;
        *left -= 1;
        let values = self.values.as_mut()?;
        if *left == 0 {
            values.remove(&key)
        } else {
            values.get(&key).cloned()
        }
    }
}

/// Distinct values a batch needs, fetched per mirror and bound
async fn fetch_batch_values(
    batch: &ReplayBatch,
    resolver: &ToastResolver,
) -> Result<FetchedValues, String> {
    let mut uses: HashMap<ValueKey, u32> = HashMap::new();
    let mut wanted: HashMap<(u32, u64), Vec<(u32, usize)>> = HashMap::new();
    for row in &batch.rows {
        let bound = row.tuple.source_lsn;
        for site in &row.pointers {
            let key = (site.p.va_toastrelid, site.p.va_valueid, bound);
            let seen = uses.entry(key).or_default();
            *seen += 1;
            if *seen == 1 {
                wanted
                    .entry((key.0, bound))
                    .or_default()
                    .push((key.1, pointer_extsize(&site.p)));
            }
        }
    }
    let mut values = HashMap::with_capacity(uses.len());
    for ((toast_relid, bound), batch) in wanted {
        let Some(got) = resolver
            .fetch_values(toast_relid, &batch, bound)
            .await
            .map_err(|e| format!("bootstrap: toast store fetch: {e}"))?
        else {
            return Ok(FetchedValues { values: None, uses });
        };
        for ((value_id, _), value) in batch.iter().zip(got) {
            values.insert((toast_relid, *value_id, bound), value);
        }
    }
    Ok(FetchedValues {
        values: Some(values),
        uses,
    })
}

/// Turn one fetch outcome into the column its row routes, counting fills.
/// Returns the bytes the column retains
fn apply_fetched(
    fetched: Option<FetchedValue>,
    p: &ToastPointer,
    type_oid: u32,
    rel: &RelDescriptor,
    target: &str,
    resolver: &ToastResolver,
) -> Result<(ColumnValue, usize), String> {
    match fetched {
        Some(FetchedValue::Assembled(stored)) => {
            let raw = finish_value(p, stored).map_err(|e| e.to_string())?;
            let retained = raw.len();
            Ok((detoasted_value(raw, type_oid), retained))
        }
        None => {
            resolver.note_filled_default();
            Ok((ColumnValue::Null, 0))
        }
        // A miss means different things depending on who filled the store.
        //
        // When the walk seeded it, the walk put these chunks moments earlier
        // and no superseding version can precede deferred resolution, so a
        // miss is a bug and reporting it is the only safe move.
        //
        // A read-only backend was seeded by nothing: it holds the state at
        // the backup's end position, and a value the copy still carried can
        // legitimately be gone there because a later record removed it. That
        // is provably supersession rather than loss — PostgreSQL cannot
        // reclaim a live row's chunks, so the value being absent at the end
        // position means the row is not live there, and every walk row
        // carries `start_lsn`, which any later version outranks
        Some(_) if !resolver.stores_chunks() => {
            resolver.note_filled_superseded();
            Ok((ColumnValue::Null, 0))
        }
        Some(outcome) => {
            resolver.note_fetch_miss();
            let extsize = pointer_extsize(p);
            let detail = match outcome {
                FetchedValue::Mismatch { got } => {
                    format!("chunks sum to {got} bytes, pointer says {extsize}")
                }
                _ => "has no chunks in the store".into(),
            };
            Err(format!(
                "bootstrap: relation {} column {target} value_id={} on toast relid={}: \
                 {detail}; remedy: fresher backup, or initial_load='copy'",
                rel.rel_name, p.va_valueid, p.va_toastrelid
            ))
        }
    }
}

/// Resolve mapped TOAST pointers or fill in disabled mode. Value cap
/// checked before any fetch; the returned leaf permit is shrunk to the
/// retained decoded bytes and rides the routed row to insert ack
async fn resolve_or_fill_toast(
    tuple: &mut BackfillTuple,
    rel: &RelDescriptor,
    mapping: &TableMapping,
    resolver: &ToastResolver,
) -> Result<Option<crate::budget::MemoryPermit>, String> {
    let sites = mapped_pointers(tuple, mapping);
    if sites.is_empty() {
        return Ok(None);
    }
    let need = check_value_caps(sites.iter().map(|site| site.p), resolver.inline_value_max())
        .map_err(|e| format!("bootstrap: {e}"))?;
    let mut leaf = crate::budget::acquire_opt(resolver.budget(), need).await;
    let mut retained = 0usize;
    for site in &sites {
        let type_oid = rel.attributes.get(site.idx).map_or(0, |a| a.type_oid);
        let fetched = resolver
            .fetch_value(
                site.p.va_toastrelid,
                site.p.va_valueid,
                tuple.source_lsn,
                pointer_extsize(&site.p),
            )
            .await
            .map_err(|e| format!("bootstrap: toast store fetch: {e}"))?;
        let target = &mapping.columns[site.col].target_name;
        let (column, bytes) = apply_fetched(fetched, &site.p, type_oid, rel, target, resolver)?;
        retained += bytes;
        tuple.columns[site.idx] = Some(column);
    }
    if let Some(p) = leaf.as_mut() {
        p.shrink(retained as u64);
    }
    Ok(leaf)
}

/// Replay tombstones supersede walk rows for dead referrers
async fn flush_chunks(resolver: &ToastResolver, batch: &mut Vec<ToastRow>) -> Result<(), String> {
    resolver
        .put(batch)
        .await
        .map_err(|e| format!("bootstrap: toast store put: {e}"))?;
    batch.clear();
    Ok(())
}

/// Convert PostgreSQL TOAST tuple shape into mirror row
fn row_from_columns(mut tuple: BackfillTuple, toast_relid: u32) -> Option<ToastRow> {
    let (chunk_id, chunk_seq, chunk_data) =
        crate::decode::heap_decoder::take_toast_chunk_columns(&mut tuple.columns)?;
    debug_assert_ne!(tuple.offnum, 0, "walked toast tuple without TID");
    Some(ToastRow {
        toast_relid,
        blkno: tuple.blkno,
        offnum: tuple.offnum,
        chunk_id,
        chunk_seq,
        chunk_data: bytes::Bytes::from(chunk_data),
        // Weakest evidence for a TID: a backup copy can be torn, so any
        // WAL-sourced row for the same TID outranks it
        lsn: 0,
    })
}

#[cfg(test)]
mod tests {
    /// Flatten the drain's coalesced `Rows` chunks back to a row list
    async fn collect_rows(rx: &mut mpsc::Receiver<BatcherMsg>) -> Vec<RoutedRow> {
        let mut rows = Vec::new();
        while let Some(msg) = rx.recv().await {
            match msg {
                BatcherMsg::Rows(chunk) => rows.extend(chunk.rows),
                BatcherMsg::Row(r) => rows.push(r),
                BatcherMsg::FlushAll(reply) => {
                    let _ = reply.send(());
                }
            }
        }
        rows
    }

    use super::*;

    /// A miss means opposite things depending on who seeded the store, and
    /// the two must not converge: a walk-seeded mirror losing a chunk it just
    /// wrote is a bug worth stopping the load for, while a read-only backend
    /// not holding a value at the backup's end position is supersession — the
    /// row cannot be live there, because PostgreSQL does not reclaim a live
    /// row's chunks, and the walk's copy carries `start_lsn` for any later
    /// version to outrank
    #[tokio::test(flavor = "current_thread")]
    async fn a_miss_is_fatal_for_a_seeded_mirror_and_superseded_for_a_read_only_store() {
        use crate::toast::{ChunkStore, ChunkStoreError, MemChunkStore, ToastRow};

        struct ReadOnly;

        #[async_trait::async_trait]
        impl ChunkStore for ReadOnly {
            fn accepts_writes(&self) -> bool {
                false
            }
            async fn put(&self, _: &[ToastRow]) -> Result<(), ChunkStoreError> {
                Err(ChunkStoreError::ReadOnly("put"))
            }
            async fn fetch(
                &self,
                _: u32,
                _: u32,
                _: u64,
                _: usize,
            ) -> Result<FetchedValue, ChunkStoreError> {
                // Short run: the value is not in the end-position state
                Ok(FetchedValue::Mismatch { got: 7984 })
            }
            async fn truncate_mirror(&self, _: u32) -> Result<(), ChunkStoreError> {
                Err(ChunkStoreError::ReadOnly("truncate_mirror"))
            }
            async fn rewrite_barrier(&self, _: u32, _: u64, _: u64) -> Result<(), ChunkStoreError> {
                Err(ChunkStoreError::ReadOnly("rewrite_barrier"))
            }
        }

        let ptr = crate::decode::heap_decoder::ToastPointer {
            va_rawsize: 9104,
            va_extinfo: 9100,
            va_valueid: 16402,
            va_toastrelid: 16390,
        };
        let rel = crate::backfill::backup_page_walk::toast_chunk_rel();
        let short = Some(FetchedValue::Mismatch { got: 7984 });

        // Walk-seeded: the chunks were put moments ago, so this is a defect
        let seeded = ToastResolver::with_store(
            Arc::new(MemChunkStore::new()),
            Arc::new(EmitterStats::default()),
        );
        assert!(seeded.stores_chunks());
        let err = apply_fetched(short.clone(), &ptr, 25, &rel, "body", &seeded)
            .expect_err("a seeded mirror losing its own chunk must stop the load");
        assert!(err.contains("chunks sum to 7984 bytes, pointer says 9100"), "{err}");
        assert_eq!(seeded.stats_handle().toast_fetch_miss.load(Ordering::Relaxed), 1);

        // Read-only: nothing seeded it, so the row is not live at the end
        // position and a later version supersedes the walk's copy
        let stats = Arc::new(EmitterStats::default());
        let read_only = ToastResolver::with_store(Arc::new(ReadOnly), stats.clone());
        assert!(!read_only.stores_chunks() && !read_only.fill_on_miss());
        let (column, retained) = apply_fetched(short, &ptr, 25, &rel, "body", &read_only)
            .expect("a read-only backend fills instead of failing the load");
        assert_eq!(column, ColumnValue::Null);
        assert_eq!(retained, 0);
        assert_eq!(stats.toast_values_filled_superseded.load(Ordering::Relaxed), 1);
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
    use crate::decode::heap_decoder::{ColumnValue, ToastPointer};
    use crate::emit::pipeline::ack;
    use crate::emit::pipeline::batcher::BatcherMsg;
    use crate::schema::{RelAttr, RelDescriptor, RelName, ReplIdent};
    use crate::toast::MemChunkStore;
    use ahash::{HashMap, HashMapExt, HashSetExt};
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
                missing_default: None,
            }],
        })
    }

    fn mapping_for(rel_node: u32) -> TableMapping {
        TableMapping {
            target: TableTarget::new("default", &format!("t{rel_node}")),
            columns: vec![ColumnMapping {
                src_attnum: 1,
                target_name: "id".into(),
                target_type: "Int32".into(),
            }],
        }
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
            blkno: 0,
            offnum: 0,
            columns: vec![Some(ColumnValue::Int4(id))],
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
            kind: 'r',
            persistence: 'p',
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
                type_align: 'i',
                type_storage: 'x',
                missing_default: None,
            }],
        })
    }

    fn bytea_mapping_for(rel_node: u32) -> TableMapping {
        TableMapping {
            target: TableTarget::new("default", &format!("t{rel_node}")),
            columns: vec![ColumnMapping {
                src_attnum: 1,
                target_name: "b".into(),
                target_type: "String".into(),
            }],
        }
    }

    /// `pg_toast` rel so [`CatalogMap::is_toast`] fires; `oid` matches the
    /// referring pointer's `va_toastrelid`. Attributes unread — the drain
    /// reinterprets a toast tuple's columns positionally (`chunk_from_columns`).
    fn toast_rel(rel_node: u32) -> Arc<RelDescriptor> {
        let name = format!("pg_toast_{rel_node}");
        Arc::new(RelDescriptor {
            rfn: RelFileNode {
                spc_node: 1663,
                db_node: 5,
                rel_node,
            },
            oid: rel_node,
            toast_oid: 0,
            namespace_oid: 99,
            rel_name: RelName::new("pg_toast", &name),
            kind: 't',
            persistence: 'p',
            replident: ReplIdent::Default { pk_attnums: None },
            attributes: vec![],
        })
    }

    /// Main-rel tuple whose mapped bytea column is an on-disk TOAST pointer
    /// into `toast_relid`/`value_id`, uncompressed (`va_extinfo` high bits
    /// clear so reassembly returns the concatenated chunks verbatim).
    fn bytea_toast_tuple(rel_node: u32, toast_relid: u32, value_id: u32) -> BackfillTuple {
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
            blkno: 0,
            offnum: 0,
            columns: vec![Some(ColumnValue::ExternalToast(ToastPointer {
                va_rawsize: 9,
                va_extinfo: 5,
                va_valueid: value_id,
                va_toastrelid: toast_relid,
            }))],
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
            blkno: 1,
            offnum: 1,
            columns: vec![
                Some(ColumnValue::Oid(value_id)),
                Some(ColumnValue::Int4(seq)),
                Some(ColumnValue::Bytea(body.to_vec())),
            ],
        }
    }

    #[tokio::test]
    async fn detoasted_input_rejects_external_pointers() {
        let mut catalog = CatalogMap::new();
        catalog.insert(rel(16400));
        let mapping = Arc::new(
            [(RelName::new("public", "t16400"), mapping_for(16400))]
                .into_iter()
                .collect(),
        );
        let (ack, collector) = ack::spawn(Arc::new(crate::pos::Monotone::new(0)));
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
            mapping,
            msg_tx,
            ack,
            stats.clone(),
            ToastResolver::with_store(Arc::new(MemChunkStore::new()), stats),
            Deferral::Rejected,
            Default::default(),
            None,
            HashSet::new(),
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

        let emitter_ack = Arc::new(crate::pos::Monotone::new(0));
        let (ack, collector) = ack::spawn(emitter_ack);
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
            mapping,
            msg_tx,
            ack.clone(),
            stats.clone(),
            ToastResolver::disabled(),
            Deferral::Local(mem_spool()),
            Default::default(),
            None,
            HashSet::new(),
        ));

        let mut by_seq: HashMap<u64, u64> = HashMap::new();
        for r in collect_rows(&mut msg_rx).await {
            *by_seq.entry(r.seq).or_default() += 1;
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

        let emitter_ack = Arc::new(crate::pos::Monotone::new(0));
        let (ack, collector) = ack::spawn(emitter_ack);
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
            mapping,
            msg_tx,
            ack.clone(),
            stats.clone(),
            ToastResolver::disabled(),
            Deferral::Local(mem_spool()),
            Default::default(),
            None,
            HashSet::new(),
        ));

        let seqs: Vec<u64> = collect_rows(&mut msg_rx)
            .await
            .iter()
            .map(|r| r.seq)
            .collect();
        let outcome = drain_task.await.unwrap().unwrap();
        assert_eq!(outcome.next_seq, 3, "three distinct rfn runs");
        assert_eq!(outcome.rows_routed, 2, "unmapped rel routed nothing");
        assert_eq!(seqs, [0, 2], "seq 1 (unmapped) routed no rows");
        assert_eq!(stats.unsupported_relations.load(Ordering::Relaxed), 1);
        drop(ack);
        collector.await.unwrap();
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

        let emitter_ack = Arc::new(crate::pos::Monotone::new(0));
        let (ack, collector) = ack::spawn(emitter_ack);
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
            mapping,
            msg_tx,
            ack.clone(),
            stats.clone(),
            resolver,
            Deferral::Local(mem_spool()),
            Default::default(),
            None,
            HashSet::new(),
        ));

        let rows = collect_rows(&mut msg_rx).await;
        let outcome = drain_task.await.unwrap().unwrap();
        assert_eq!(outcome.next_seq, 1);
        assert_eq!(outcome.rows_routed, 1);
        assert_eq!(rows.len(), 1);
        let cols = &rows[0].committed.decoded.new.as_ref().unwrap().columns;
        assert_eq!(
            cols[0],
            Some(ColumnValue::Null),
            "unresolved toast NULL-filled"
        );
        assert_eq!(stats.toast_values_filled_default.load(Ordering::Relaxed), 1);
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

        let emitter_ack = Arc::new(crate::pos::Monotone::new(0));
        let (ack, collector) = ack::spawn(emitter_ack);
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
            mapping,
            msg_tx,
            ack.clone(),
            stats.clone(),
            ToastResolver::with_store(store, stats.clone()),
            // Threshold 0: deferred referrer rides a real spool file
            Deferral::Local(DeferredSpool::new(
                spool_tmp.path().join("bootstrap_deferred.bin"),
                0,
            )),
            Default::default(),
            None,
            HashSet::new(),
        ));

        let rows = collect_rows(&mut msg_rx).await;
        let outcome = drain_task.await.unwrap().unwrap();
        assert_eq!(outcome.next_seq, 3, "toast seq, main seq, deferred seq");
        assert_eq!(outcome.rows_routed, 1);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].seq, 2, "deferred row routed under the trailing seq");
        let cols = &rows[0].committed.decoded.new.as_ref().unwrap().columns;
        assert_eq!(cols[0], Some(ColumnValue::Bytea(b"hello".to_vec())));
        assert_eq!(stats.toast_chunks_stored.load(Ordering::Relaxed), 1);
        assert_eq!(stats.toast_values_fetched.load(Ordering::Relaxed), 1);
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

        let (ack, collector) = ack::spawn(Arc::new(crate::pos::Monotone::new(0)));
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
            mapping.clone(),
            msg_tx.clone(),
            ack.clone(),
            stats.clone(),
            resolver.clone(),
            Deferral::Handback(mem_spool()),
            Default::default(),
            None,
            HashSet::new(),
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
            spool,
            &catalog,
            &mapping,
            &msg_tx,
            &ack,
            &stats,
            &resolver,
            &Default::default(),
            None,
            outcome.next_seq,
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
        let cols = &rows[0].committed.decoded.new.as_ref().unwrap().columns;
        assert_eq!(cols[0], Some(ColumnValue::Bytea(b"hello".to_vec())));
        drop(ack);
        collector.await.unwrap();
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
            let (ack, collector) = ack::spawn(Arc::new(crate::pos::Monotone::new(0)));
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
                mapping.clone(),
                msg_tx.clone(),
                ack.clone(),
                stats.clone(),
                resolver.clone(),
                Deferral::Handback(mem_spool()),
                Default::default(),
                None,
                HashSet::new(),
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
                spool,
                &catalog,
                &mapping,
                &msg_tx,
                &ack,
                &stats,
                &resolver,
                &Default::default(),
                None,
                first_seq,
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

        let emitter_ack = Arc::new(crate::pos::Monotone::new(0));
        let (ack, collector) = ack::spawn(emitter_ack);
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
            mapping,
            msg_tx,
            ack.clone(),
            stats.clone(),
            ToastResolver::with_store(store, stats.clone()),
            Deferral::Local(mem_spool()),
            Default::default(),
            None,
            HashSet::new(),
        ));
        drop(tup_tx);

        let rows = collect_rows(&mut msg_rx).await;
        let outcome = drain_task.await.unwrap().unwrap();
        assert_eq!(outcome.rows_routed, 1);
        assert_eq!(rows.len(), 1);
        let cols = &rows[0].committed.decoded.new.as_ref().unwrap().columns;
        assert_eq!(cols[0], Some(ColumnValue::Bytea(b"hello".to_vec())));
        drop(ack);
        collector.await.unwrap();
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

        let (ack, collector) = ack::spawn(Arc::new(crate::pos::Monotone::new(0)));
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
            mapping,
            msg_tx,
            ack.clone(),
            stats.clone(),
            ToastResolver::with_store(Arc::new(MemChunkStore::new()), stats.clone()),
            Deferral::Local(DeferredSpool::new(
                spool_tmp.path().join("bootstrap_deferred.bin"),
                0,
            )),
            Default::default(),
            None,
            HashSet::new(),
        ));

        let routed = collect_rows(&mut msg_rx).await;
        let outcome = drain_task.await.unwrap().unwrap();
        assert_eq!(outcome.rows_routed as usize, rows);
        let got: Vec<_> = routed
            .iter()
            .map(|r| r.committed.decoded.new.as_ref().unwrap().columns[0].clone())
            .collect();
        let want: Vec<_> = bodies
            .into_iter()
            .map(|b| Some(ColumnValue::Bytea(b)))
            .collect();
        assert_eq!(got, want, "replay order kept across the batch boundary");
        assert_eq!(
            stats.toast_value_fetch_batches.load(Ordering::Relaxed),
            2,
            "row cap sealed a second batch, one round trip each",
        );
        drop(ack);
        collector.await.unwrap();
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

        let (ack, collector) = ack::spawn(Arc::new(crate::pos::Monotone::new(0)));
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
            mapping,
            msg_tx,
            ack.clone(),
            stats.clone(),
            ToastResolver::with_store(Arc::new(MemChunkStore::new()), stats.clone()),
            Deferral::Local(DeferredSpool::new(
                spool_tmp.path().join("bootstrap_deferred.bin"),
                0,
            )),
            Default::default(),
            None,
            HashSet::new(),
        ));

        let rows = collect_rows(&mut msg_rx).await;
        let outcome = drain_task.await.unwrap().unwrap();
        assert_eq!(outcome.rows_routed, 4);
        let bodies: Vec<_> = rows
            .iter()
            .map(|r| r.committed.decoded.new.as_ref().unwrap().columns[0].clone())
            .collect();
        assert_eq!(
            bodies,
            [b"hello", b"hello", b"world", b"there"]
                .map(|b| Some(ColumnValue::Bytea(b.to_vec())))
                .to_vec(),
            "replay order kept, shared value resolved for both referrers",
        );
        assert!(
            rows.iter().all(|r| r.seq == rows[0].seq),
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
}
