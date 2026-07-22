//! Backup-sourced per-table initial load: `initial_load='base_backup'` and
//! `'object_store'` (plans/add_table.md).
//!
//! Reuses greenfield bootstrap plumbing — [`BackupSource`] impls,
//! [`PageWalkSink`], [`bootstrap::drain`] — with a per-rel filter over the
//! tables being added and a backup-era visibility gate
//! ([`crate::decode::visibility`]) the greenfield walk lacks. Nothing lands on disk:
//! the sink Taps filtered heap files (+ `pg_xact/` and `pg_multixact/` into
//! memory for the gate) and Skips everything else.
//!
//! ## `_lsn` tagging (plans/add_table.md §invariant)
//!
//! Walked rows must lose to every WAL-delivered mutation the backup state
//! does not already reflect: tag with the LSN where continuous WAL coverage
//! of the rel begins, never later.
//!
//! - `'base_backup'`: fresh `BASE_BACKUP` starts at `B ≥ S`, the live stream
//!   covers `(S, ∞)` → tag `S`. No replay leg: any xact the tar catches
//!   mid-write commits after `S`, and pre-`S` in-flight xacts' rows were
//!   buffered inclusion-agnostically by the live pump.
//! - `'object_store'`: the backup predates the opt-in; archive replay covers
//!   `(B_redo, S]` → tag `min(B_redo, S)` per rel (a backup *newer* than an
//!   opt-in needs no replay for that rel and its rows tag `S`).
//!
//! ## object_store sequencing
//!
//! sentinel → fetch gap segments → records-only pre-scan (catalog-skew
//! abort + [`PgXactPatch`] harvest) → filtered walk (gate resolves deferred
//! tuples against backup pg_xact + patch at successful walk EOF) → gap replay
//! through the shared decode path ([`BufferingDecoderSink`] +
//! [`XactBuffer::drain_committed`]) with
//! rows emitted at real commit LSNs, commits past a rel's `S` dropped (the
//! live stream owns them; dedup absorbs overlap regardless).
//!
//! The pre-scan aborts on gap writes that would invalidate the walk: a
//! pg_class / pg_attribute new-tuple write whose row oid is (or cannot be
//! proven not to be) a filtered rel — a rewrite means the backup's filenode
//! isn't the current rfn at all — a relmap update (mapped-catalog rewrite,
//! pg_class filenode tracking would go stale), or a TRUNCATE naming a
//! filtered rel. Error names the remedies: fresher backup, or `'copy'`.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use anyhow::{Context, Result, bail};
use tokio::sync::{Mutex, mpsc, oneshot};
use walrus::pg::backup::BACKUP_NAME_PREFIX;
use walrus::pg::replication::base_backup::BaseBackupOpts;
use walrus::pg::wal::segment::SegmentName;
use walrus::pg::walparser::{Oid, RmId};

use crate::backfill::backfill_types::{BackupRequest, PassContext, PassOutcome};
use crate::backfill::backup_page_walk::{
    BOOTSTRAP_TUPLE_CHANNEL_CAP, BackfillTuple, CatalogMap, PageWalkSink,
};
use crate::backfill::backup_sentinel::build_lsn_pair;
use crate::backfill::backup_source::{BackupSink, BackupSource};
use crate::backfill::backup_source_direct::DirectSource;
use crate::backfill::backup_source_object_store::ObjectStoreSource;
use crate::backfill::spool::{DEFERRED_SPOOL_MEM_MAX, DeferredSpool};
use crate::decode::heap_decoder::{CommittedTuple, XLOG_HEAP_OPMASK, XLOG_HEAP_TRUNCATE};
use crate::decode::visibility::{
    PgMultiXactAccum, PgXactAccum, PgXactPatch, PgXactView, Visibility, tuple_visibility,
};
use crate::decode::wal_xact::{
    XLOG_XACT_ABORT, XLOG_XACT_ABORT_PREPARED, XLOG_XACT_ASSIGNMENT, XLOG_XACT_COMMIT,
    XLOG_XACT_COMMIT_PREPARED, XLOG_XACT_OPMASK, parse_xact_assignment, parse_xact_payload,
};
use crate::emit::ch_emitter::EmitterStats;
use crate::emit::pipeline::batcher::{BatcherMsg, RoutedRow};
use crate::emit::pipeline::{Fatal, ack::AckHandle, bootstrap, tail};
use crate::filter::manifest::Manifest;
use crate::filter::pg_class_decoder::{
    DecodeOutcome, decode_pg_class_tuple, info_carries_new_tuple_heap,
};
use crate::mapping::MappingHandle;
use crate::record::{Record, RecordSink, SegmentSink, SinkError, WAL_SEG_SIZE};
use crate::runtime_config::InitialLoadMode;
use crate::schema::RelDescriptor;
use crate::source::wal_stream::WalStream;
use crate::toast::{ChunkRefMap, ToastResolver};
use crate::xact::xact_buffer::{
    BufferingDecoderSink, DrainEntry, DrainedBatch, SubxactTracker, WalkStep, XactBuffer,
    XactBufferConfig, detoast_heap, resolve_stash,
};

/// Run one coalesced backup pass for `reqs` (all sharing `mode`).
pub async fn run_pass(
    ctx: &PassContext,
    mode: InitialLoadMode,
    reqs: &[BackupRequest],
) -> Result<PassOutcome> {
    match mode {
        InitialLoadMode::None | InitialLoadMode::Copy => {
            bail!("backup_backfill: mode {mode:?} routes via the copy backfiller")
        }
        InitialLoadMode::BaseBackup => run_base_backup_pass(ctx, reqs).await,
        InitialLoadMode::ObjectStore => run_object_store_pass(ctx, reqs).await,
    }
}

async fn run_base_backup_pass(ctx: &PassContext, reqs: &[BackupRequest]) -> Result<PassOutcome> {
    let opts = BaseBackupOpts {
        label: "walshadow-backfill".to_string(),
        fast_checkpoint: true,
        no_verify_checksums: false,
        max_rate_kib: None,
        // WAL rides the live stream; the tar only feeds the page walk
        wal: false,
    };
    let source = Box::new(DirectSource::new(ctx.pg.clone(), opts));
    // Tag S per rel: backup B ≥ S, live stream covers (S, ∞)
    let tags: HashMap<(Oid, Oid), u64> = reqs.iter().map(|r| (rfn_key(&r.desc), r.s_lsn)).collect();
    let mut outcome = PassOutcome::default();
    walk_and_ship(
        ctx,
        source,
        reqs,
        &tags,
        PgXactPatch::new(),
        None,
        &mut outcome,
    )
    .await?;
    Ok(outcome)
}

async fn run_object_store_pass(ctx: &PassContext, reqs: &[BackupRequest]) -> Result<PassOutcome> {
    // Archive from the `[backup]` config, never the source-PG overlay:
    // credentials in a source table is the wrong trust direction
    // (plans/add_table.md §Anti-goals)
    let settings = ctx
        .emitter
        .backup
        .as_ref()
        .context("backup_backfill: object_store initial_load requires a [backup] section")?;
    let storage = settings
        .build_storage()
        .context("backup_backfill: build archive storage")?;
    let resolved = walrus::pg::backup::fetch::resolve_name(&storage, "LATEST")
        .await
        .context("backup_backfill: resolve LATEST backup")?;
    if !resolved.starts_with(BACKUP_NAME_PREFIX) {
        bail!("backup_backfill: resolved backup name {resolved:?} not wal-g shaped");
    }
    let sentinel = walrus::pg::backup::fetch::fetch_sentinel(&storage, &resolved).await?;
    if sentinel.sentinel.increment_from.is_some() {
        bail!(
            "backup_backfill: {resolved} is a delta backup (parent: {:?}); the streaming \
             page walk needs a full base — take a full backup, or use initial_load='copy'",
            sentinel.sentinel.increment_from
        );
    }
    let (start, _end) = build_lsn_pair(&resolved, &sentinel)?;
    let b_redo = start.start_lsn;
    let s_max = reqs.iter().map(|r| r.s_lsn).max().unwrap_or(0);

    let mut outcome = PassOutcome {
        b_redo,
        ..Default::default()
    };

    // Gap leg only when the backup predates an opt-in boundary
    let (patch, gap_segments) = if b_redo < s_max {
        let seg_dir = ctx.scratch_dir.join("gap_wal");
        let segments =
            fetch_gap_segments(settings, &storage, &seg_dir, start.timeline, b_redo, s_max)
                .await
                .context(
                    "backup_backfill: fetch archive WAL for the gap (a timeline switch or archive \
             gap aborts; remedy: fresher backup, or initial_load='copy')",
                )?;
        outcome.gap_segments = segments.len() as u32;
        let filter_oids: HashSet<u32> = reqs.iter().map(|r| r.desc.oid).collect();
        let current_rfns: HashMap<u32, u32> = reqs
            .iter()
            .map(|r| (r.desc.oid, r.desc.rfn.rel_node))
            .collect();
        let patch = prescan_gap(
            &segments,
            start.timeline,
            &filter_oids,
            &current_rfns,
            s_max,
        )
        .await
        .context("backup_backfill: gap catalog pre-scan")?;
        (patch, segments)
    } else {
        (PgXactPatch::new(), Vec::new())
    };
    outcome.pg_xact_patch_len = patch.len();

    let source =
        Box::new(ObjectStoreSource::new(settings.clone(), storage, resolved).with_parallelism(4));
    // Tag min(B_redo, S) per rel: gap replay covers (B_redo, S], so walked
    // rows must lose to replayed commits; a backup newer than the opt-in
    // has no replay leg for that rel and tags S
    let tags: HashMap<(Oid, Oid), u64> = reqs
        .iter()
        .map(|r| (rfn_key(&r.desc), r.s_lsn.min(b_redo)))
        .collect();
    let had_gap = !gap_segments.is_empty();
    walk_and_ship(
        ctx,
        source,
        reqs,
        &tags,
        patch,
        had_gap.then_some((gap_segments, start.timeline, b_redo)),
        &mut outcome,
    )
    .await?;
    // Fetched segments only help a *failed* pass resume; reclaim on success
    if had_gap {
        tokio::fs::remove_dir_all(ctx.scratch_dir.join("gap_wal"))
            .await
            .ok();
    }
    Ok(outcome)
}

fn rfn_key(desc: &RelDescriptor) -> (Oid, Oid) {
    (desc.rfn.db_node, desc.rfn.rel_node)
}

/// Gap replay inputs: fetched segments, timeline, `B_redo`.
type ReplayLeg = (Vec<(SegmentName, PathBuf)>, u32, u64);

/// Shared trunk: filter map (+ toast rels), gated walk into a dedicated
/// insert tail, then an optional gap replay continuing the seq space.
#[allow(clippy::too_many_arguments)]
async fn walk_and_ship(
    ctx: &PassContext,
    source: Box<dyn BackupSource>,
    reqs: &[BackupRequest],
    tags: &HashMap<(Oid, Oid), u64>,
    patch: PgXactPatch,
    replay: Option<ReplayLeg>,
    outcome: &mut PassOutcome,
) -> Result<()> {
    // Filter set: the rels being added plus their pg_toast_<oid> rels, so a
    // filtered walk carries external chunks. Toast rows tag their parent's
    // boundary.
    let mut filter = CatalogMap::new();
    let mut lsn_overrides: HashMap<(Oid, Oid), u64> = tags.clone();
    for r in reqs {
        filter.insert(r.desc.clone());
        let toast = ctx
            .catalog
            .lock()
            .await
            .toast_descriptor_for(r.desc.oid)
            .await
            .map_err(|e| anyhow::anyhow!("backup_backfill: toast descriptor: {e}"))?;
        if let Some(td) = toast {
            lsn_overrides.insert(rfn_key(&td), tags[&rfn_key(&r.desc)]);
            filter.insert(td);
        }
    }

    let mut resolver = ToastResolver::from_config(&ctx.emitter, ctx.stats.clone());
    if let Some(b) = &ctx.budget {
        resolver = resolver.with_budget(b.clone());
    }
    let store_toast = resolver.stores_chunks();

    // Dedicated tail: own CH connection, own seq space, own fatal — the
    // live pipeline never blocks on a backfill (Regime A)
    let fatal = Fatal::new();
    let (msg_tx, ack, tail) = tail::spawn_with_config(
        &ctx.emitter,
        1,
        ctx.stats.clone(),
        Arc::new(AtomicU64::new(0)),
        fatal.clone(),
        ctx.config_rx.clone(),
    )
    .await
    .map_err(|e| anyhow::anyhow!("backup_backfill: spawn insert tail: {e}"))?;

    let pg_xact = Arc::new(std::sync::Mutex::new(PgXactAccum::new()));
    let pg_multixact = Arc::new(std::sync::Mutex::new(PgMultiXactAccum::new()));
    let (walk_tx, walk_rx) = mpsc::channel::<BackfillTuple>(BOOTSTRAP_TUPLE_CHANNEL_CAP);
    let (gated_tx, gated_rx) = mpsc::channel::<BackfillTuple>(BOOTSTRAP_TUPLE_CHANNEL_CAP);

    let sink = PageWalkSink::new(filter.clone(), walk_tx, store_toast)
        .with_pg_xact_accum(pg_xact.clone())
        .with_pg_multixact_accum(pg_multixact.clone())
        .with_lsn_overrides(lsn_overrides);
    let erased: Arc<Mutex<dyn BackupSink>> = Arc::new(Mutex::new(sink));

    // data_dir is never written: PageWalkSink only Taps/Skips
    let data_dir = ctx.scratch_dir.join("void");
    tokio::fs::create_dir_all(&data_dir).await.ok();

    let (walk_ok_tx, walk_ok_rx) = oneshot::channel();
    // Deferred spools live under scratch; stale files from a crashed pass
    // block create_new, remove first
    let gate_spool_path = ctx.scratch_dir.join("gate_deferred.bin");
    let toast_spool_path = ctx.scratch_dir.join("bootstrap_deferred.bin");
    tokio::fs::remove_file(&gate_spool_path).await.ok();
    tokio::fs::remove_file(&toast_spool_path).await.ok();
    let gate = tokio::spawn(gate_task(
        walk_rx,
        gated_tx,
        filter.clone(),
        pg_xact,
        pg_multixact,
        patch,
        walk_ok_rx,
        DeferredSpool::new(gate_spool_path, DEFERRED_SPOOL_MEM_MAX),
    ));
    let drain = tokio::spawn(bootstrap::drain(
        gated_rx,
        filter,
        ctx.mapping.clone(),
        msg_tx.clone(),
        ack.clone(),
        ctx.stats.clone(),
        resolver.clone(),
        DeferredSpool::new(toast_spool_path, DEFERRED_SPOOL_MEM_MAX),
    ));

    // Success signal before the joins: gate resolves deferred tuples only
    // against a complete pg_xact accum; a failed source drops the sender
    // and the gate discards them instead
    let run_res = source
        .run(data_dir, erased)
        .await
        .context("backup_backfill: source.run");
    if run_res.is_ok() {
        let _ = walk_ok_tx.send(());
    } else {
        drop(walk_ok_tx);
    }

    // Join gate + drain on every path (both exit on channel close), then
    // quiesce the tail before surfacing an error: a detached inserter could
    // otherwise final-flush into a staging table a retry pass has already
    // rebuilt (plans/add_table.md §Staging swap)
    let gate_join = gate.await.context("backup_backfill: gate join");
    let drain_join = drain.await.context("backup_backfill: drain join");

    if let Err(e) = run_res {
        quiesce_tail(msg_tx, ack, tail).await;
        return Err(e);
    }
    let gate_stats = match gate_join.and_then(|r| r.map_err(anyhow::Error::msg)) {
        Ok(s) => s,
        Err(e) => {
            quiesce_tail(msg_tx, ack, tail).await;
            return Err(e);
        }
    };
    // Every non-toast tuple lands in exactly one of emitted/gated (deferred
    // resolves into one at EOF), so their sum is the walked total
    outcome.rows_walked += gate_stats.emitted + gate_stats.gated;
    outcome.rows_gated += gate_stats.gated;
    outcome.rows_deferred += gate_stats.deferred;
    outcome.multixact_emitted += gate_stats.multixact_emitted;
    outcome.pg_xact_segments = gate_stats.pg_xact_segments;
    let drain_outcome = match drain_join.and_then(|r| r.map_err(anyhow::Error::msg)) {
        Ok(o) => o,
        Err(e) => {
            quiesce_tail(msg_tx, ack, tail).await;
            return Err(e);
        }
    };

    let mut next_seq = drain_outcome.next_seq;
    if let Some((segments, timeline, b_redo)) = replay {
        let s_by_rfn: HashMap<(Oid, Oid), (Arc<RelDescriptor>, u64)> = reqs
            .iter()
            .map(|r| (rfn_key(&r.desc), (r.desc.clone(), r.s_lsn)))
            .collect();
        let replay_res = replay_gap(
            ctx,
            &segments,
            timeline,
            b_redo,
            s_by_rfn,
            resolver.clone(),
            msg_tx.clone(),
            ack.clone(),
            next_seq,
        )
        .await
        .context("backup_backfill: gap replay");
        let replay_stats = match replay_res {
            Ok(s) => s,
            Err(e) => {
                quiesce_tail(msg_tx, ack, tail).await;
                return Err(e);
            }
        };
        next_seq = replay_stats.next_seq;
        outcome.rows_replayed = replay_stats.rows_replayed;
        outcome.replay_commits_past_s = replay_stats.commits_past_s;
    }

    tail.finish(msg_tx, ack, next_seq, &fatal)
        .await
        .map_err(anyhow::Error::msg)?;
    Ok(())
}

/// Failed-pass teardown: with gate + drain already joined, dropping the
/// last producer handles closes the batcher, which final-flushes and
/// cascades the inserters + collector down. Bounded by the inserters'
/// retry policy (a CH outage trips their fatal, not a hang).
async fn quiesce_tail(msg_tx: mpsc::Sender<BatcherMsg>, ack: AckHandle, tail: tail::TailParts) {
    drop(msg_tx);
    drop(ack);
    tail.join().await;
}

#[derive(Debug, Default)]
struct GateStats {
    emitted: u64,
    gated: u64,
    deferred: u64,
    multixact_emitted: u64,
    pg_xact_segments: usize,
}

/// Visibility gate between the page walk and the drain. Hint-decidable
/// tuples route immediately; undecidable ones (including every non-lock-only
/// multixact xmax) defer until walk EOF, when collected pg_xact +
/// pg_multixact (+ gap patch) are complete. Toast-chunk tuples bypass the
/// gate: the store is keyed and only referenced values get pulled.
/// Deferred resolution requires `walk_ok`: channel close alone also happens
/// when a failed source drops the sink mid-walk. An unresolvable multixact
/// errors the pass: emitting risks resurrecting a dead version whose delete
/// predates WAL coverage, skipping risks dropping a live row.
#[allow(clippy::too_many_arguments)]
async fn gate_task(
    mut rx: mpsc::Receiver<BackfillTuple>,
    tx: mpsc::Sender<BackfillTuple>,
    filter: CatalogMap,
    pg_xact: Arc<std::sync::Mutex<PgXactAccum>>,
    pg_multixact: Arc<std::sync::Mutex<PgMultiXactAccum>>,
    patch: PgXactPatch,
    walk_ok: oneshot::Receiver<()>,
    mut deferred: DeferredSpool,
) -> Result<GateStats, String> {
    let mut stats = GateStats::default();
    while let Some(t) = rx.recv().await {
        if filter.is_toast(t.rfn.db_node, t.rfn.rel_node) {
            if tx.send(t).await.is_err() {
                return Ok(stats);
            }
            continue;
        }
        match tuple_visibility(t.xid, t.xmax, t.infomask, None) {
            Visibility::Emit => {
                if t.infomask & crate::decode::visibility::HEAP_XMAX_IS_MULTI != 0 {
                    stats.multixact_emitted += 1;
                }
                stats.emitted += 1;
                if tx.send(t).await.is_err() {
                    return Ok(stats);
                }
            }
            Visibility::Skip => stats.gated += 1,
            Visibility::Defer => deferred
                .push(t)
                .await
                .map_err(|e| format!("backup_backfill: deferred spool: {e}"))?,
            Visibility::Unresolvable => return Err(unresolvable_multixact(&t)),
        }
    }
    // Walk EOF: sink dropped. Deferred tuples sit in the spool past its
    // in-memory prefix, resident bytes bounded regardless of unhinted count.
    stats.deferred = deferred.records();
    // pg_xact is complete only if the walk finished; a partial accum reads
    // committed deleters as in-progress and recent aborts as ancient-committed,
    // emitting dead tuples a rerun can't remove
    if walk_ok.await.is_err() {
        stats.gated += stats.deferred;
        deferred.discard().await;
        return Ok(stats);
    }
    // Take the accums out so no std guard is held across the sends below
    let accum = std::mem::take(&mut *pg_xact.lock().expect("pg_xact accum lock"));
    let multi = std::mem::take(&mut *pg_multixact.lock().expect("pg_multixact accum lock"));
    stats.pg_xact_segments = accum.segment_count();
    let view = PgXactView::new(&accum, &patch).with_multixact(&multi);
    let mut replay = deferred
        .into_reader()
        .await
        .map_err(|e| format!("backup_backfill: deferred spool seal: {e}"))?;
    while let Some(t) = replay
        .next()
        .await
        .map_err(|e| format!("backup_backfill: deferred spool replay: {e}"))?
    {
        match tuple_visibility(t.xid, t.xmax, t.infomask, Some(&view)) {
            Visibility::Emit => {
                if t.infomask & crate::decode::visibility::HEAP_XMAX_IS_MULTI != 0 {
                    stats.multixact_emitted += 1;
                }
                stats.emitted += 1;
                if tx.send(t).await.is_err() {
                    return Ok(stats);
                }
            }
            Visibility::Skip | Visibility::Defer => stats.gated += 1,
            Visibility::Unresolvable => return Err(unresolvable_multixact(&t)),
        }
    }
    replay
        .finish()
        .await
        .map_err(|e| format!("backup_backfill: deferred spool cleanup: {e}"))?;
    Ok(stats)
}

fn unresolvable_multixact(t: &BackfillTuple) -> String {
    format!(
        "backup_backfill: multixact xmax {} (rfn {}/{}) unresolvable from the backup's \
         pg_multixact snapshot; remedy: fresher backup, or initial_load='copy'",
        t.xmax, t.rfn.db_node, t.rfn.rel_node
    )
}

// ---------------------------------------------------------------------------
// Gap segment fetch
// ---------------------------------------------------------------------------

/// Fetch archive WAL covering `[from, to]` on `timeline` into `seg_dir`.
/// A missing segment (archive gap, or the archive switched timelines) errors
/// through wal-rus's `fetch::handle`. Exposed for restart archive fallback
/// (see the `fetch_archive_segment` / `SourceRecovery` path in the stream binary).
pub async fn fetch_gap_segments(
    settings: &walrus::config::Settings,
    storage: &walrus::storage::DynStorage,
    seg_dir: &Path,
    timeline: u32,
    from: u64,
    to: u64,
) -> Result<Vec<(SegmentName, PathBuf)>> {
    tokio::fs::create_dir_all(seg_dir)
        .await
        .with_context(|| format!("create {}", seg_dir.display()))?;
    let seg_size = WAL_SEG_SIZE;
    let mut cur = SegmentName {
        timeline,
        log_id: (from >> 32) as u32,
        seg_no: ((from & 0xFFFF_FFFF) / seg_size) as u32,
    };
    let mut out = Vec::new();
    loop {
        let name = cur.format();
        let dst = seg_dir.join(&name);
        if !dst.exists() {
            walrus::pg::wal::fetch::handle(
                settings,
                storage.clone(),
                &name,
                &dst,
                walrus::pg::wal::fetch::Prefetch::Off,
            )
            .await
            .with_context(|| format!("fetch WAL {name}"))?;
        }
        out.push((cur, dst));
        let seg_end = cur.start_lsn(seg_size).saturating_add(seg_size);
        if to < seg_end {
            break;
        }
        cur = cur.next(seg_size);
    }
    Ok(out)
}

/// Segment output of the replay/pre-scan streams is discarded; only the
/// record dispatch matters.
struct DropSegments;

impl SegmentSink for DropSegments {
    fn on_segment<'a>(
        &'a mut self,
        _seg: SegmentName,
        _bytes: &'a [u8],
        _manifest: &'a Manifest,
    ) -> Pin<Box<dyn Future<Output = std::result::Result<(), SinkError>> + Send + 'a>> {
        Box::pin(std::future::ready(Ok(())))
    }

    fn on_partial_segment<'a>(
        &'a mut self,
        _seg: SegmentName,
        _bytes: &'a [u8],
        _manifest: &'a Manifest,
    ) -> Pin<Box<dyn Future<Output = std::result::Result<(), SinkError>> + Send + 'a>> {
        Box::pin(std::future::ready(Ok(())))
    }
}

/// Drive fetched segments through a `RecordSink` in LSN order.
async fn pump_segments_through(
    segments: &[(SegmentName, PathBuf)],
    timeline: u32,
    sink: &mut (dyn RecordSink + Send),
) -> Result<()> {
    let Some((first, _)) = segments.first() else {
        return Ok(());
    };
    let mut stream = WalStream::new(timeline, WAL_SEG_SIZE, first.start_lsn(WAL_SEG_SIZE))
        .map_err(|e| anyhow::anyhow!("backup_backfill: WalStream: {e}"))?;
    let mut seg_sink = DropSegments;
    for (seg, path) in segments {
        let bytes = tokio::fs::read(path)
            .await
            .with_context(|| format!("read {}", path.display()))?;
        stream
            .push(seg.start_lsn(WAL_SEG_SIZE), &bytes, sink, &mut seg_sink)
            .await
            .map_err(|e| anyhow::anyhow!("backup_backfill: replay {}: {e}", seg.format()))?;
    }
    stream
        .close(None, sink)
        .await
        .map_err(|e| anyhow::anyhow!("backup_backfill: replay close: {e}"))?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Gap pre-scan
// ---------------------------------------------------------------------------

/// Records-only sweep of the gap: harvest commit/abort outcomes into a
/// [`PgXactPatch`] and abort on catalog skew touching filtered rels.
/// Skew checks stop at `s_max`: a catalog change past the opt-in boundary
/// arrives via the live DDL path and the walked (pre-backup) tuples decode
/// with the opt-in-era descriptor regardless; only the trailing partial
/// segment carries such records.
async fn prescan_gap(
    segments: &[(SegmentName, PathBuf)],
    timeline: u32,
    filter_oids: &HashSet<u32>,
    current_rfns: &HashMap<u32, u32>,
    s_max: u64,
) -> Result<PgXactPatch> {
    let mut sink = PrescanSink {
        filter_oids: filter_oids.clone(),
        current_rfns: current_rfns.clone(),
        patch: PgXactPatch::new(),
        skew: None,
        s_max,
    };
    pump_segments_through(segments, timeline, &mut sink).await?;
    if let Some(reason) = sink.skew {
        bail!(
            "backup_backfill: catalog skew in the backup→opt-in gap ({reason}); the walk \
             would decode with the wrong shape or a stale filenode. Remedies: take a \
             fresher backup, or use initial_load='copy'"
        );
    }
    Ok(sink.patch)
}

/// `pg_class` / `pg_attribute` initial (mapped) filenodes; a rewrite of the
/// mapped catalogs themselves surfaces as `RM_RELMAP`, which aborts.
const PG_CLASS_RELNODE: u32 = 1259;
const PG_ATTRIBUTE_RELNODE: u32 = 1249;
/// `SizeOfHeapTruncate = offsetof(xl_heap_truncate, relids)`:
/// dbId(4) + nrelids(4) + flags(1) + align pad(3)
const SIZE_OF_HEAP_TRUNCATE: usize = 12;

struct PrescanSink {
    filter_oids: HashSet<u32>,
    /// oid → current main-fork rel_node; a decoded pg_class row for a
    /// filtered oid carrying a different filenode is a rewrite in the gap
    current_rfns: HashMap<u32, u32>,
    patch: PgXactPatch,
    skew: Option<String>,
    /// Skew checks apply to records at or below this LSN
    s_max: u64,
}

impl PrescanSink {
    fn observe(&mut self, record: &Record<'_>) {
        let rm = record.parsed.header.resource_manager_id;
        if rm == RmId::Xact as u8 {
            let info = record.parsed.header.info;
            let xid = record.parsed.header.xact_id;
            match info & XLOG_XACT_OPMASK {
                XLOG_XACT_COMMIT | XLOG_XACT_COMMIT_PREPARED => {
                    let payload =
                        parse_xact_payload(info, &record.parsed.main_data, record.page_magic)
                            .unwrap_or_default();
                    self.patch.commit(xid, &payload.subxacts);
                }
                XLOG_XACT_ABORT | XLOG_XACT_ABORT_PREPARED => {
                    let payload =
                        parse_xact_payload(info, &record.parsed.main_data, record.page_magic)
                            .unwrap_or_default();
                    self.patch.abort(xid, &payload.subxacts);
                }
                _ => {}
            }
            return;
        }
        if self.skew.is_some() || record.source_lsn > self.s_max {
            return;
        }
        if rm == RmId::RelMap as u8 {
            self.skew = Some("relmap update (mapped-catalog rewrite)".into());
            return;
        }
        if rm != RmId::Heap as u8 {
            return;
        }
        let info = record.parsed.header.info;
        if info & XLOG_HEAP_OPMASK == XLOG_HEAP_TRUNCATE {
            for oid in truncate_relids(&record.parsed.main_data) {
                if self.filter_oids.contains(&oid) {
                    self.skew = Some(format!("TRUNCATE of oid {oid}"));
                    return;
                }
            }
            return;
        }
        let Some(block) = record.parsed.blocks.first() else {
            return;
        };
        let rel_node = block.header.location.rel.rel_node;
        if rel_node != PG_CLASS_RELNODE && rel_node != PG_ATTRIBUTE_RELNODE {
            return;
        }
        if !info_carries_new_tuple_heap(info) {
            return;
        }
        // pg_class rows carry oid at data offset 0, pg_attribute rows
        // attrelid at 0 — one decode covers both membership checks
        match decode_pg_class_tuple(&record.parsed, 0) {
            DecodeOutcome::Decoded(row) => {
                if self.filter_oids.contains(&row.oid) {
                    if rel_node == PG_CLASS_RELNODE
                        && self.current_rfns.get(&row.oid) == Some(&row.relfilenode)
                    {
                        // pg_class write not changing the filenode (e.g.
                        // relhasindex flip): shape + filenode intact
                        return;
                    }
                    self.skew = Some(format!(
                        "{} write for filtered oid {}",
                        if rel_node == PG_CLASS_RELNODE {
                            "pg_class"
                        } else {
                            "pg_attribute"
                        },
                        row.oid
                    ));
                }
            }
            // Prefix-compressed / short rows hide the oid: can't prove the
            // write isn't a filtered rel's
            DecodeOutcome::OidInPrefix | DecodeOutcome::Undecoded => {
                self.skew = Some(format!("undecodable catalog write on relnode {rel_node}"));
            }
        }
    }
}

/// `xl_heap_truncate.relids` (PG `heapam_xlog.h`).
fn truncate_relids(main_data: &[u8]) -> Vec<u32> {
    if main_data.len() < SIZE_OF_HEAP_TRUNCATE {
        return Vec::new();
    }
    let nrelids = u32::from_le_bytes(main_data[4..8].try_into().unwrap()) as usize;
    main_data[SIZE_OF_HEAP_TRUNCATE..]
        .chunks_exact(4)
        .take(nrelids)
        .map(|c| u32::from_le_bytes(c.try_into().unwrap()))
        .collect()
}

impl RecordSink for PrescanSink {
    fn on_record<'a>(
        &'a mut self,
        record: &'a Record<'a>,
    ) -> Pin<Box<dyn Future<Output = std::result::Result<(), SinkError>> + Send + 'a>> {
        self.observe(record);
        Box::pin(std::future::ready(Ok(())))
    }
}

// ---------------------------------------------------------------------------
// Gap replay
// ---------------------------------------------------------------------------

struct ReplayStats {
    next_seq: u64,
    rows_replayed: u64,
    commits_past_s: u64,
}

/// Replay the gap through the shared decode path: heap records whose rfn is
/// in the filter set feed the same [`BufferingDecoderSink`] the hot path
/// uses (subxacts, TOAST reassembly, update/delete decode for free); commit
/// records drain through [`XactBuffer::drain_committed`] +
/// [`DrainedBatch::into_walk`] — the same apply plan the reorder barrier
/// runs — shipping rows at their real commit LSNs on the pass's tail.
#[allow(clippy::too_many_arguments)]
async fn replay_gap(
    ctx: &PassContext,
    segments: &[(SegmentName, PathBuf)],
    timeline: u32,
    b_redo: u64,
    targets: HashMap<(Oid, Oid), (Arc<RelDescriptor>, u64)>,
    resolver: ToastResolver,
    msg_tx: mpsc::Sender<BatcherMsg>,
    ack: AckHandle,
    next_seq: u64,
) -> Result<ReplayStats> {
    let spill = ctx.scratch_dir.join("replay_spill");
    tokio::fs::create_dir_all(&spill).await.ok();
    let buffer = Arc::new(Mutex::new(
        XactBuffer::new(XactBufferConfig::new(spill))
            .map_err(|e| anyhow::anyhow!("backup_backfill: replay xact buffer: {e}"))?,
    ));
    buffer.lock().await.clear_spill_dir().await.ok();

    // Filter rfns: opted-in mains + their toast rels (chunks reassemble
    // inside the buffered xact)
    let mut filter_rfns: HashSet<(Oid, Oid)> = targets.keys().copied().collect();
    for (desc, _) in targets.values() {
        if let Some(td) = ctx
            .catalog
            .lock()
            .await
            .toast_descriptor_for(desc.oid)
            .await
            .map_err(|e| anyhow::anyhow!("backup_backfill: toast descriptor: {e}"))?
        {
            filter_rfns.insert(rfn_key(&td));
        }
    }

    let mut sink = ReplaySink {
        decoder: BufferingDecoderSink::new(ctx.log.clone(), buffer.clone()),
        buffer,
        log: ctx.log.clone(),
        subxact_tracker: SubxactTracker::new(),
        resolver,
        filter_rfns,
        targets,
        b_redo,
        mapping: ctx.mapping.clone(),
        stats: ctx.stats.clone(),
        budget: ctx.budget.clone(),
        batch_rows: ctx.emitter.drain_batch_rows,
        batch_bytes: ctx.emitter.drain_batch_bytes,
        msg_tx,
        ack,
        next_seq,
        open: None,
        rows_replayed: 0,
        commits_past_s: 0,
    };
    pump_segments_through(segments, timeline, &mut sink).await?;

    Ok(ReplayStats {
        next_seq: sink.next_seq,
        rows_replayed: sink.rows_replayed,
        commits_past_s: sink.commits_past_s,
    })
}

/// Serial gap drain over pre-filtered records; mirrors the daemon's
/// `DecoderXactPair` without the queueing worker. Rows gate per rel: one
/// seq per commit that routed at least one row, real `commit_lsn`, commits
/// at or under `b_redo` live in the walked backup pages, commits past the
/// rel's `S` belong to the live stream (dedup absorbs overlap regardless).
/// Catalog/config drain entries are ignored — the live stream owns DDL and
/// the prescan aborts on filtered-rel catalog skew below `S`.
struct ReplaySink {
    decoder: BufferingDecoderSink,
    buffer: Arc<Mutex<XactBuffer>>,
    log: Arc<crate::catalog::desc_log::DescriptorLog>,
    subxact_tracker: SubxactTracker,
    resolver: ToastResolver,
    filter_rfns: HashSet<(Oid, Oid)>,
    targets: HashMap<(Oid, Oid), (Arc<RelDescriptor>, u64)>,
    b_redo: u64,
    mapping: MappingHandle,
    stats: Arc<EmitterStats>,
    budget: Option<crate::budget::MemoryBudget>,
    /// Drain-slice budget, same knobs as the pipeline reorder
    batch_rows: usize,
    batch_bytes: usize,
    msg_tx: mpsc::Sender<BatcherMsg>,
    ack: AckHandle,
    next_seq: u64,
    /// Current commit's `(seq, rows routed)`; registered lazily on its
    /// first routed row so row-less commits consume no seq
    open: Option<(u64, u64)>,
    rows_replayed: u64,
    commits_past_s: u64,
}

impl ReplaySink {
    async fn on_commit(
        &mut self,
        xid: u32,
        info: u8,
        record: &Record<'_>,
    ) -> std::result::Result<(), SinkError> {
        let payload = parse_xact_payload(info, &record.parsed.main_data, record.page_magic)
            .unwrap_or_default();
        // Deferred resolution for filenodes invisible at record time;
        // installs decode verdicts + `O - B` barriers ahead of the drain
        resolve_stash(
            &self.buffer,
            &self.log,
            xid,
            &payload.subxacts,
            record.next_lsn,
            self.resolver.stats_handle(),
        )
        .await
        .map_err(SinkError::from)?;
        let mut drain = self
            .buffer
            .lock()
            .await
            .drain_committed(
                xid,
                payload.xact_time,
                record.source_lsn,
                &payload.subxacts,
                self.resolver.stores_chunks(),
            )
            .await
            .map_err(SinkError::from)?;
        while let Some(batch) = drain
            .next_batch(self.batch_rows, self.batch_bytes, self.budget.as_ref())
            .await
            .map_err(SinkError::from)?
        {
            self.apply_batch(batch, drain.commit_ts, drain.commit_lsn)
                .await?;
        }
        drain.finish().await.map_err(SinkError::from)?;
        if let Some((seq, rows)) = self.open.take() {
            self.ack.placed(seq, rows);
        }
        self.subxact_tracker.forget_tree(xid);
        Ok(())
    }

    async fn apply_batch(
        &mut self,
        batch: DrainedBatch,
        commit_ts: i64,
        commit_lsn: u64,
    ) -> std::result::Result<(), SinkError> {
        let walk = batch.into_walk();
        let ref_maps: Vec<&ChunkRefMap> = walk.chunks.iter().map(|g| g.map()).collect();
        // One spool per xact; generations sealed before spooling carry None
        let spool = walk.chunks.iter().find_map(|g| g.spool());
        let mut rows_cursor = 0usize;
        for step in walk.steps {
            match step {
                WalkStep::Rows { upto } => {
                    if upto > rows_cursor {
                        self.resolver
                            .put_row_refs(walk.new_rows.spool(), &walk.new_rows[rows_cursor..upto])
                            .await
                            .map_err(|e| SinkError::Other(format!("toast store put: {e}")))?;
                        rows_cursor = upto;
                    }
                }
                // Live stream owns DDL/config apply
                WalkStep::Event(DrainEntry::Catalog(_))
                | WalkStep::Event(DrainEntry::Config(_)) => {}
                WalkStep::Event(DrainEntry::ToastBarrier {
                    toast_relid,
                    marker_lsn,
                }) => {
                    self.resolver
                        .rewrite_barrier(toast_relid, marker_lsn, commit_lsn)
                        .await
                        .map_err(|e| SinkError::Other(format!("toast rewrite barrier: {e}")))?;
                }
                WalkStep::Truncate(_) => {
                    // xl_heap_truncate carries no block ref, never passes the
                    // rfn filter
                    debug_assert!(false, "TRUNCATE heap in gap replay");
                }
                WalkStep::Heap(mut heap) => {
                    let rfn = heap.rfn;
                    let Some((rel, s_cap)) = self.targets.get(&(rfn.db_node, rfn.rel_node)) else {
                        continue;
                    };
                    if commit_lsn <= self.b_redo {
                        // Backup pages already reflect this commit; the walked
                        // copy (tagged min(B_redo, S)) carries it
                        continue;
                    }
                    if commit_lsn > *s_cap {
                        self.commits_past_s += 1;
                        continue;
                    }
                    let rel = rel.clone();
                    let value_permit =
                        detoast_heap(&mut heap, spool, &ref_maps, &self.log, &self.resolver)
                            .await
                            .map_err(SinkError::from)?;
                    let Some(mapping) = crate::emit::pipeline::lookup_mapping(
                        &self.mapping,
                        &rel.rel_name,
                        &self.stats,
                    )
                    .await
                    else {
                        continue;
                    };
                    let seq = if let Some((seq, rows)) = &mut self.open {
                        *rows += 1;
                        *seq
                    } else {
                        let seq = self.next_seq;
                        self.next_seq += 1;
                        self.ack.register(seq, commit_lsn);
                        self.open = Some((seq, 1));
                        seq
                    };
                    self.msg_tx
                        .send(BatcherMsg::Row(RoutedRow {
                            seq,
                            rel,
                            mapping,
                            committed: CommittedTuple {
                                decoded: heap,
                                commit_ts,
                                commit_lsn,
                            },
                            permit: None,
                            value_permit: value_permit.map(Arc::new),
                        }))
                        .await
                        .map_err(|_| {
                            SinkError::Other("backup_backfill: replay tail closed".into())
                        })?;
                    self.rows_replayed += 1;
                }
            }
        }
        Ok(())
    }
}

impl RecordSink for ReplaySink {
    fn on_record<'a>(
        &'a mut self,
        record: &'a Record<'a>,
    ) -> Pin<Box<dyn Future<Output = std::result::Result<(), SinkError>> + Send + 'a>> {
        Box::pin(async move {
            let rm = record.parsed.header.resource_manager_id;
            if rm == RmId::Heap as u8 || rm == RmId::Heap2 as u8 {
                let in_filter = record.parsed.blocks.first().is_some_and(|b| {
                    let rel = b.header.location.rel;
                    self.filter_rfns.contains(&(rel.db_node, rel.rel_node))
                });
                if in_filter {
                    self.decoder.on_record(record).await?;
                }
            } else if rm == RmId::Xact as u8 {
                let info = record.parsed.header.info;
                let xid = record.parsed.header.xact_id;
                match info & XLOG_XACT_OPMASK {
                    XLOG_XACT_COMMIT | XLOG_XACT_COMMIT_PREPARED => {
                        self.on_commit(xid, info, record).await?;
                    }
                    XLOG_XACT_ABORT | XLOG_XACT_ABORT_PREPARED => {
                        let payload =
                            parse_xact_payload(info, &record.parsed.main_data, record.page_magic)
                                .unwrap_or_default();
                        self.buffer
                            .lock()
                            .await
                            .abort(xid, record.source_lsn, &payload.subxacts)
                            .await
                            .map_err(SinkError::from)?;
                        self.subxact_tracker.forget_tree(xid);
                    }
                    XLOG_XACT_ASSIGNMENT => {
                        // Hint for eviction policy; correctness rides on the
                        // commit / abort record's authoritative subxact list
                        if let Some((xtop, subs)) = parse_xact_assignment(&record.parsed.main_data)
                        {
                            self.subxact_tracker.assign(xtop, &subs);
                        }
                    }
                    _ => {
                        // PREPARE / INVALIDATIONS unhandled; xact stays
                        // buffered until COMMIT_PREPARED
                    }
                }
            }
            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::decode::visibility::{
        HEAP_XMAX_INVALID, HEAP_XMAX_IS_MULTI, HEAP_XMIN_COMMITTED, HEAP_XMIN_INVALID,
    };
    use crate::record::Route;
    use walrus::pg::walparser::{
        BlockLocation, RelFileNode, XLogRecord, XLogRecordBlock, XLogRecordBlockHeader,
        XLogRecordHeader,
    };

    fn record(
        rm: RmId,
        info: u8,
        xid: u32,
        main_data: Vec<u8>,
        block: Option<(RelFileNode, Vec<u8>)>,
    ) -> Record<'static> {
        let blocks = block
            .map(|(rel, data)| {
                vec![XLogRecordBlock {
                    header: XLogRecordBlockHeader {
                        location: BlockLocation { rel, block_no: 0 },
                        ..Default::default()
                    },
                    data: std::borrow::Cow::Owned(data),
                    ..Default::default()
                }]
            })
            .unwrap_or_default();
        Record {
            parsed: XLogRecord {
                header: XLogRecordHeader {
                    resource_manager_id: rm as u8,
                    info,
                    xact_id: xid,
                    ..Default::default()
                },
                blocks,
                main_data: std::borrow::Cow::Owned(main_data),
                ..Default::default()
            },
            source_lsn: 0x5000,
            route: Route::ToShadow,
            ..Default::default()
        }
    }

    fn catalog_rfn(rel_node: u32) -> RelFileNode {
        RelFileNode {
            spc_node: 1663,
            db_node: 5,
            rel_node,
        }
    }

    /// pg_class-shaped insert block: xl_heap_header + pad + oid at data
    /// offset 0, relfilenode at 88.
    fn pg_class_insert_block(oid: u32, relfilenode: u32) -> Vec<u8> {
        let mut v = Vec::new();
        v.extend_from_slice(&33u16.to_le_bytes()); // t_infomask2
        v.extend_from_slice(&0u16.to_le_bytes()); // t_infomask
        v.push(24); // t_hoff
        v.push(0); // pad byte 23..24
        v.extend_from_slice(&oid.to_le_bytes());
        v.extend_from_slice(&[0u8; 84]); // relname + cols 3..7
        v.extend_from_slice(&relfilenode.to_le_bytes());
        v
    }

    fn prescan(filter_oid: u32, current_rfn: u32) -> PrescanSink {
        PrescanSink {
            filter_oids: HashSet::from([filter_oid]),
            current_rfns: HashMap::from([(filter_oid, current_rfn)]),
            patch: PgXactPatch::new(),
            skew: None,
            s_max: u64::MAX,
        }
    }

    #[test]
    fn prescan_harvests_commit_and_abort_into_patch() {
        let mut s = prescan(16400, 16400);
        // xact_time only, no XLOG_XACT_HAS_INFO
        s.observe(&record(
            RmId::Xact,
            XLOG_XACT_COMMIT,
            700,
            7i64.to_le_bytes().to_vec(),
            None,
        ));
        s.observe(&record(
            RmId::Xact,
            XLOG_XACT_ABORT,
            701,
            7i64.to_le_bytes().to_vec(),
            None,
        ));
        assert!(s.skew.is_none());
        assert_eq!(s.patch.len(), 2);
        let accum = PgXactAccum::new();
        let view = PgXactView::new(&accum, &s.patch);
        assert_eq!(
            view.xid_status(700),
            crate::decode::visibility::XidStatus::Committed
        );
        assert_eq!(
            view.xid_status(701),
            crate::decode::visibility::XidStatus::Aborted
        );
    }

    #[test]
    fn prescan_aborts_on_filtered_pg_class_rewrite() {
        let mut s = prescan(16400, 16400);
        // Same filenode: benign pg_class touch
        s.observe(&record(
            RmId::Heap,
            0x00, // INSERT
            10,
            Vec::new(),
            Some((
                catalog_rfn(PG_CLASS_RELNODE),
                pg_class_insert_block(16400, 16400),
            )),
        ));
        assert!(s.skew.is_none(), "filenode unchanged is not skew");
        // Rewrite: filenode changed
        s.observe(&record(
            RmId::Heap,
            0x00,
            11,
            Vec::new(),
            Some((
                catalog_rfn(PG_CLASS_RELNODE),
                pg_class_insert_block(16400, 99999),
            )),
        ));
        assert!(s.skew.is_some(), "filenode rotation is skew: {:?}", s.skew);
    }

    #[test]
    fn prescan_aborts_on_filtered_pg_attribute_write_and_ignores_others() {
        let mut s = prescan(16400, 16400);
        s.observe(&record(
            RmId::Heap,
            0x00,
            10,
            Vec::new(),
            Some((
                catalog_rfn(PG_ATTRIBUTE_RELNODE),
                pg_class_insert_block(16777, 0),
            )),
        ));
        assert!(s.skew.is_none(), "other rel's pg_attribute write ignored");
        s.observe(&record(
            RmId::Heap,
            0x00,
            11,
            Vec::new(),
            Some((
                catalog_rfn(PG_ATTRIBUTE_RELNODE),
                pg_class_insert_block(16400, 0),
            )),
        ));
        assert!(s.skew.is_some(), "ADD COLUMN on filtered rel is skew");
    }

    #[test]
    fn prescan_aborts_on_relmap_and_filtered_truncate() {
        let mut s = prescan(16400, 16400);
        s.observe(&record(RmId::RelMap, 0x00, 0, Vec::new(), None));
        assert!(s.skew.is_some());

        let mut s = prescan(16400, 16400);
        // xl_heap_truncate: dbId, nrelids=2, flags, pad, relids
        let mut md = Vec::new();
        md.extend_from_slice(&5u32.to_le_bytes());
        md.extend_from_slice(&2u32.to_le_bytes());
        md.extend_from_slice(&[0u8; 4]); // flags + align pad
        md.extend_from_slice(&777u32.to_le_bytes());
        md.extend_from_slice(&16400u32.to_le_bytes());
        s.observe(&record(
            RmId::Heap,
            XLOG_HEAP_TRUNCATE,
            12,
            md.clone(),
            None,
        ));
        assert!(s.skew.is_some(), "TRUNCATE naming filtered oid is skew");

        let mut s = prescan(16401, 16401);
        s.observe(&record(RmId::Heap, XLOG_HEAP_TRUNCATE, 12, md, None));
        assert!(s.skew.is_none(), "TRUNCATE of other rels ignored");
    }

    #[test]
    fn prescan_aborts_on_undecodable_catalog_write() {
        let mut s = prescan(16400, 16400);
        s.observe(&record(
            RmId::Heap,
            0x00,
            10,
            Vec::new(),
            Some((catalog_rfn(PG_CLASS_RELNODE), vec![0u8; 3])),
        ));
        assert!(s.skew.is_some(), "can't prove the write isn't ours");
    }

    /// Catalog writes past `s_max` (post-opt-in DDL in the trailing partial
    /// segment) arrive via the live path; not skew for the walk.
    #[test]
    fn prescan_ignores_catalog_writes_past_s_max() {
        let mut s = prescan(16400, 16400);
        s.s_max = 0x100; // records are built at source_lsn 0x5000
        s.observe(&record(
            RmId::Heap,
            0x00,
            10,
            Vec::new(),
            Some((
                catalog_rfn(PG_CLASS_RELNODE),
                pg_class_insert_block(16400, 99999),
            )),
        ));
        assert!(s.skew.is_none());
        // Commit harvest is not lsn-gated
        s.observe(&record(
            RmId::Xact,
            XLOG_XACT_COMMIT,
            700,
            7i64.to_le_bytes().to_vec(),
            None,
        ));
        assert_eq!(s.patch.len(), 1);
    }

    #[test]
    fn truncate_relids_parses_flexible_array() {
        let mut md = Vec::new();
        md.extend_from_slice(&5u32.to_le_bytes());
        md.extend_from_slice(&3u32.to_le_bytes());
        md.extend_from_slice(&[0u8; 4]);
        for oid in [1u32, 2, 3] {
            md.extend_from_slice(&oid.to_le_bytes());
        }
        assert_eq!(truncate_relids(&md), vec![1, 2, 3]);
        assert!(truncate_relids(&[0u8; 4]).is_empty());
    }

    fn tuple(rel_node: u32, xmin: u32, xmax: u32, infomask: u16) -> BackfillTuple {
        BackfillTuple {
            rfn: catalog_rfn(rel_node),
            xid: xmin,
            xmax,
            infomask,
            source_lsn: 0x1000,
            blkno: 0,
            offnum: 0,
            columns: Vec::new(),
        }
    }

    /// Mem-only under the default threshold; path never created
    fn mem_spool() -> DeferredSpool {
        DeferredSpool::new(
            std::env::temp_dir().join("ws-gate-test-unused.bin"),
            DEFERRED_SPOOL_MEM_MAX,
        )
    }

    #[tokio::test]
    async fn gate_task_routes_hinted_defers_unhinted_and_resolves_at_eof() {
        let filter = CatalogMap::new();
        let pg_xact = Arc::new(std::sync::Mutex::new(PgXactAccum::new()));
        let pg_multixact = Arc::new(std::sync::Mutex::new(PgMultiXactAccum::new()));
        let mut patch = PgXactPatch::new();
        patch.commit(500, &[]);
        patch.abort(600, &[]);

        let (walk_tx, walk_rx) = mpsc::channel(16);
        let (gated_tx, mut gated_rx) = mpsc::channel(16);
        let (walk_ok_tx, walk_ok_rx) = oneshot::channel();
        // Threshold 0: deferred tuples traverse a real spool file
        let tmp = tempfile::tempdir().unwrap();
        let spool_path = tmp.path().join("gate_deferred.bin");
        let gate = tokio::spawn(gate_task(
            walk_rx,
            gated_tx,
            filter,
            pg_xact,
            pg_multixact,
            patch,
            walk_ok_rx,
            DeferredSpool::new(spool_path.clone(), 0),
        ));

        // Hinted-committed: passes through immediately
        walk_tx
            .send(tuple(
                16400,
                100,
                0,
                HEAP_XMIN_COMMITTED | HEAP_XMAX_INVALID,
            ))
            .await
            .unwrap();
        // Hinted-aborted: gated
        walk_tx
            .send(tuple(16400, 101, 0, HEAP_XMIN_INVALID))
            .await
            .unwrap();
        // Unhinted, gap-committed writer: deferred, then emitted via patch
        walk_tx.send(tuple(16400, 500, 0, 0)).await.unwrap();
        // Unhinted, gap-aborted writer: deferred, then gated via patch
        walk_tx.send(tuple(16400, 600, 0, 0)).await.unwrap();
        drop(walk_tx);
        walk_ok_tx.send(()).unwrap();

        let stats = gate.await.unwrap().unwrap();
        let mut got = Vec::new();
        while let Some(t) = gated_rx.recv().await {
            got.push(t.xid);
        }
        assert_eq!(got, vec![100, 500]);
        assert_eq!(stats.emitted, 2);
        assert_eq!(stats.gated, 2);
        assert_eq!(stats.deferred, 2);
        assert!(!spool_path.exists(), "replay unlinks the spool");
    }

    /// Failed source drops the sink mid-walk: channel close without the
    /// success signal must not resolve deferred tuples against partial
    /// pg_xact (a missing segment reads a committed deleter as in-progress,
    /// emitting a dead tuple a rerun can't remove).
    #[tokio::test]
    async fn gate_task_discards_deferred_without_walk_success() {
        let filter = CatalogMap::new();
        let pg_xact = Arc::new(std::sync::Mutex::new(PgXactAccum::new()));
        let pg_multixact = Arc::new(std::sync::Mutex::new(PgMultiXactAccum::new()));
        let mut patch = PgXactPatch::new();
        // Patch alone would emit xid 500; failure path must not consult it
        patch.commit(500, &[]);

        let (walk_tx, walk_rx) = mpsc::channel(16);
        let (gated_tx, mut gated_rx) = mpsc::channel(16);
        let (walk_ok_tx, walk_ok_rx) = oneshot::channel::<()>();
        // Threshold 0: discard must unlink the spool file
        let tmp = tempfile::tempdir().unwrap();
        let spool_path = tmp.path().join("gate_deferred.bin");
        let gate = tokio::spawn(gate_task(
            walk_rx,
            gated_tx,
            filter,
            pg_xact,
            pg_multixact,
            patch,
            walk_ok_rx,
            DeferredSpool::new(spool_path.clone(), 0),
        ));

        // Hinted-committed: routed before the failure, stays flushed
        walk_tx
            .send(tuple(
                16400,
                100,
                0,
                HEAP_XMIN_COMMITTED | HEAP_XMAX_INVALID,
            ))
            .await
            .unwrap();
        // Unhinted: deferred, must be discarded
        walk_tx.send(tuple(16400, 500, 0, 0)).await.unwrap();
        drop(walk_tx);
        drop(walk_ok_tx);

        let stats = gate.await.unwrap().unwrap();
        let mut got = Vec::new();
        while let Some(t) = gated_rx.recv().await {
            got.push(t.xid);
        }
        assert_eq!(got, vec![100], "deferred tuple not emitted");
        assert_eq!(stats.emitted, 1);
        assert_eq!(stats.deferred, 1);
        assert_eq!(stats.gated, 1, "discarded deferred counts as gated");
    }

    /// Multixact accum with mxid 10 → member offsets [100, 101): one Update
    /// member, xid 901 (member offset 100 → group 25 slot 0: flag byte at
    /// 500, xid at 504).
    fn multi_with_updater_901() -> PgMultiXactAccum {
        let mut off = vec![0u8; 8192];
        off[10 * 4..10 * 4 + 4].copy_from_slice(&100u32.to_le_bytes());
        off[11 * 4..11 * 4 + 4].copy_from_slice(&101u32.to_le_bytes());
        let mut mem = vec![0u8; 8192];
        mem[500] = 5;
        mem[504..508].copy_from_slice(&901u32.to_le_bytes());
        let mut multi = PgMultiXactAccum::new();
        multi.insert_offsets_segment(0, off);
        multi.insert_members_segment(0, mem);
        multi
    }

    /// Multixact with a committed delete member must gate: its commit may
    /// predate WAL coverage, so nothing re-delivers a higher-`_lsn` winner.
    #[tokio::test]
    async fn gate_task_gates_multixact_with_committed_updater() {
        let filter = CatalogMap::new();
        let pg_xact = Arc::new(std::sync::Mutex::new(PgXactAccum::new()));
        let pg_multixact = Arc::new(std::sync::Mutex::new(multi_with_updater_901()));
        let mut patch = PgXactPatch::new();
        patch.commit(901, &[]);

        let (walk_tx, walk_rx) = mpsc::channel(16);
        let (gated_tx, mut gated_rx) = mpsc::channel(16);
        let (walk_ok_tx, walk_ok_rx) = oneshot::channel();
        let gate = tokio::spawn(gate_task(
            walk_rx,
            gated_tx,
            filter,
            pg_xact,
            pg_multixact,
            patch,
            walk_ok_rx,
            mem_spool(),
        ));

        walk_tx
            .send(tuple(
                16400,
                100,
                10,
                HEAP_XMIN_COMMITTED | HEAP_XMAX_IS_MULTI,
            ))
            .await
            .unwrap();
        drop(walk_tx);
        walk_ok_tx.send(()).unwrap();

        let stats = gate.await.unwrap().unwrap();
        assert!(gated_rx.recv().await.is_none(), "dead tuple must not emit");
        assert_eq!(stats.deferred, 1, "multixact defers to EOF");
        assert_eq!(stats.gated, 1);
        assert_eq!(stats.multixact_emitted, 0);
    }

    #[tokio::test]
    async fn gate_task_errors_on_unresolvable_multixact() {
        let filter = CatalogMap::new();
        let pg_xact = Arc::new(std::sync::Mutex::new(PgXactAccum::new()));
        // Empty accum: mxid below any collected segment ⇒ unresolvable
        let pg_multixact = Arc::new(std::sync::Mutex::new(PgMultiXactAccum::new()));

        let (walk_tx, walk_rx) = mpsc::channel(16);
        let (gated_tx, _gated_rx) = mpsc::channel(16);
        let (walk_ok_tx, walk_ok_rx) = oneshot::channel();
        let gate = tokio::spawn(gate_task(
            walk_rx,
            gated_tx,
            filter,
            pg_xact,
            pg_multixact,
            PgXactPatch::new(),
            walk_ok_rx,
            mem_spool(),
        ));

        walk_tx
            .send(tuple(
                16400,
                100,
                10,
                HEAP_XMIN_COMMITTED | HEAP_XMAX_IS_MULTI,
            ))
            .await
            .unwrap();
        drop(walk_tx);
        walk_ok_tx.send(()).unwrap();

        let err = gate.await.unwrap().unwrap_err();
        assert!(err.contains("pg_multixact"), "{err}");
        assert!(err.contains("initial_load='copy'"), "{err}");
    }
}
