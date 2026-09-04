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
//! through [`WalReplaySink`], shared with greenfield window replay. Rows use
//! commit LSNs and stop at each relation's coverage bound.
//!
//! The pre-scan aborts on gap writes that would invalidate the walk: a
//! pg_class / pg_attribute new-tuple write whose row oid is (or cannot be
//! proven not to be) a filtered rel — a rewrite means the backup's filenode
//! isn't the current rfn at all — a relmap update (mapped-catalog rewrite,
//! pg_class filenode tracking would go stale), or a TRUNCATE naming a
//! filtered rel. Error names the remedies: fresher backup, or `'copy'`.

use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;

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
use crate::backfill::visibility_gate::{GateStats, Undecidable, resolve_phase, stream_phase};
use crate::backfill::visibility_repair::PendingSet;
use crate::backfill::wal_replay::{
    ReplayStats, ReplayTargets, WalReplayInputs, WalReplaySink, pump_segments_through,
};
use crate::decode::heap_decoder::{XLOG_HEAP_OPMASK, XLOG_HEAP_TRUNCATE};
use crate::decode::visibility::{PgMultiXactAccum, PgXactAccum, PgXactPatch, PgXactView};
use crate::decode::wal_xact::{
    XLOG_XACT_ABORT, XLOG_XACT_ABORT_PREPARED, XLOG_XACT_COMMIT, XLOG_XACT_COMMIT_PREPARED,
    XLOG_XACT_OPMASK, parse_xact_payload,
};
use crate::emit::pipeline::batcher::BatcherMsg;
use crate::emit::pipeline::tail::OwnedTail;
use crate::emit::pipeline::{Fatal, ack::AckHandle, bootstrap};
use crate::filter::main_data::{parse_xl_heap_truncate, parse_xl_relmap_update};
use crate::filter::pg_class_decoder::{
    DecodeOutcome, decode_pg_class_tuple, info_carries_new_tuple_heap,
};
use crate::record::{Record, RecordSink, SinkError, segments_covering};
use crate::runtime_config::InitialLoadMode;
use crate::schema::RelDescriptor;
use crate::toast::ToastResolver;
use crate::xact::xact_buffer::{XactBuffer, XactBufferConfig};
use ahash::{HashMap, HashSet, HashSetExt};

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

/// One pass covers relations of one database: the followed one. Catalog
/// skew is a per-database question, so the prescan needs that oid before it
/// compares any WAL OID with a request's
fn target_db_oid(reqs: &[BackupRequest]) -> Result<Oid> {
    let db_oid = reqs
        .first()
        .context("backup_backfill: empty request set")?
        .desc
        .rfn
        .db_node;
    if db_oid == 0 {
        bail!("backup_backfill: request carries no database oid");
    }
    if let Some(other) = reqs.iter().find(|r| r.desc.rfn.db_node != db_oid) {
        bail!(
            "backup_backfill: requests span databases {db_oid} and {}",
            other.desc.rfn.db_node
        );
    }
    Ok(db_oid)
}

async fn run_object_store_pass(ctx: &PassContext, reqs: &[BackupRequest]) -> Result<PassOutcome> {
    let target_db = target_db_oid(reqs)?;
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
            target_db,
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

    let source = Box::new(
        ObjectStoreSource::new(settings.clone(), storage, resolved, ctx.scratch_dir.clone())
            .with_parallelism(4),
    );
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
    let tail = OwnedTail::spawn(
        &ctx.emitter,
        1,
        ctx.stats.clone(),
        Fatal::new(),
        ctx.config_rx.clone(),
        ctx.oracle.clone(),
        "backup_backfill",
    )
    .await
    .map_err(anyhow::Error::msg)?;

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
        tail.msg_tx.clone(),
        tail.ack.clone(),
        ctx.stats.clone(),
        resolver.clone(),
        DeferredSpool::new(toast_spool_path, DEFERRED_SPOOL_MEM_MAX),
        ctx.emitter.row_policy(),
        ctx.config_rx.as_ref().map(|rx| rx.borrow().clone()),
        HashSet::new(),
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
        tail.quiesce().await;
        return Err(e);
    }
    let (gate_stats, pg_xact_segments) = match gate_join.and_then(|r| r.map_err(anyhow::Error::msg))
    {
        Ok(s) => s,
        Err(e) => {
            tail.quiesce().await;
            return Err(e);
        }
    };
    // Every non-toast tuple lands in exactly one of emitted/gated (deferred
    // resolves into one at EOF), so their sum is the walked total
    outcome.rows_walked += gate_stats.emitted + gate_stats.gated;
    outcome.rows_gated += gate_stats.gated;
    outcome.rows_deferred += gate_stats.deferred;
    outcome.multixact_emitted += gate_stats.multixact_emitted;
    outcome.pg_xact_segments = pg_xact_segments;
    let drain_outcome = match drain_join.and_then(|r| r.map_err(anyhow::Error::msg)) {
        Ok(o) => o,
        Err(e) => {
            tail.quiesce().await;
            return Err(e);
        }
    };

    let mut next_seq = drain_outcome.next_seq;
    if let Some((segments, timeline, b_redo)) = replay {
        let s_by_rfn: ReplayTargets = reqs
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
            tail.msg_tx.clone(),
            tail.ack.clone(),
            next_seq,
        )
        .await
        .context("backup_backfill: gap replay");
        let replay_stats = match replay_res {
            Ok(s) => s,
            Err(e) => {
                tail.quiesce().await;
                return Err(e);
            }
        };
        next_seq = replay_stats.next_seq;
        outcome.rows_replayed = replay_stats.rows_replayed;
        outcome.replay_commits_past_s = replay_stats.commits_past_through;
    }

    tail.finish(next_seq).await.map_err(anyhow::Error::msg)?;
    Ok(())
}

/// Run visibility gate and track `pg_xact` segments
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
) -> Result<(GateStats, usize), String> {
    let mut stats = GateStats::default();
    // Per-table loads abort on unprovable tuples
    let no_pending = PendingSet::empty();
    stream_phase(
        &mut rx,
        &tx,
        &filter,
        &no_pending,
        &mut deferred,
        &mut stats,
    )
    .await?;
    if walk_ok.await.is_err() {
        stats.gated += stats.deferred;
        deferred.discard().await;
        return Ok((stats, 0));
    }
    // Take the accums out so no std guard is held across the sends below
    let accum = std::mem::take(&mut *pg_xact.lock().expect("pg_xact accum lock"));
    let multi = std::mem::take(&mut *pg_multixact.lock().expect("pg_multixact accum lock"));
    let segments = accum.segment_count();
    let view = PgXactView::new(&accum, &patch).with_multixact(&multi);
    resolve_phase(deferred, &view, &tx, Undecidable::Abort, &mut stats).await?;
    Ok((stats, segments))
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
    let segments = segments_covering(timeline, from..to.saturating_add(1));
    let mut out = Vec::with_capacity(segments.len());
    for seg in segments {
        let name = seg.format();
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
        out.push((seg, dst));
    }
    Ok(out)
}

// ---------------------------------------------------------------------------
// Gap replay
// ---------------------------------------------------------------------------

/// Replay fetched gap segments between walk and live-stream coverage
#[allow(clippy::too_many_arguments)]
async fn replay_gap(
    ctx: &PassContext,
    segments: &[(SegmentName, PathBuf)],
    timeline: u32,
    b_redo: u64,
    targets: ReplayTargets,
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

    // Include opted-in main and TOAST filenodes
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

    let mut sink = WalReplaySink::new(WalReplayInputs {
        log: ctx.log.clone(),
        buffer,
        resolver,
        filter_rfns,
        targets,
        from_lsn: b_redo,
        // Filter is the opted-in set, so unfiltered rels are deliberate
        whole_db_filter: false,
        mapping: ctx.mapping.snapshot().await,
        stats: ctx.stats.clone(),
        budget: ctx.budget.clone(),
        row_policy: ctx.emitter.row_policy(),
        config: ctx.config_rx.as_ref().map(|rx| rx.borrow().clone()),
        batch_rows: ctx.emitter.drain_batch_rows,
        batch_bytes: ctx.emitter.drain_batch_bytes,
        msg_tx,
        ack,
        next_seq,
        // Pre-scan already harvested transaction patch
        patch: None,
    });
    pump_segments_through(segments, timeline, ctx.log.db_oid(), &mut sink).await?;
    Ok(sink.stats())
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
    target_db_oid: Oid,
    filter_oids: &HashSet<u32>,
    current_rfns: &HashMap<u32, u32>,
    s_max: u64,
) -> Result<PgXactPatch> {
    let mut sink = PrescanSink {
        target_db_oid,
        filter_oids: filter_oids.clone(),
        current_rfns: current_rfns.clone(),
        patch: PgXactPatch::new(),
        skew: None,
        s_max,
    };
    pump_segments_through(segments, timeline, target_db_oid, &mut sink).await?;
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

struct PrescanSink {
    /// Database the requests live in. Catalog OIDs and mapped-catalog
    /// filenodes repeat in every database, so skew checks compare only
    /// after the record's own database matches
    target_db_oid: Oid,
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
    /// Keep the first reason: later records observe a patch already known bad
    fn fail_closed(&mut self, reason: String) {
        self.skew.get_or_insert(reason);
    }

    fn observe(&mut self, record: &Record<'_>) {
        let rm = record.parsed.header.resource_manager_id;
        if rm == RmId::Xact as u8 {
            let info = record.parsed.header.info;
            let xid = record.parsed.header.xact_id;
            match info & XLOG_XACT_OPMASK {
                XLOG_XACT_COMMIT | XLOG_XACT_COMMIT_PREPARED => {
                    match parse_xact_payload(info, &record.parsed.main_data, record.page_magic) {
                        // COMMIT PREPARED: header xid is the finishing
                        // backend's, the verdict belongs to the prepared xid
                        Ok(p) => self
                            .patch
                            .commit(p.twophase_xid.unwrap_or(xid), &p.subxacts),
                        Err(e) => self.fail_closed(format!("malformed commit payload: {e}")),
                    }
                }
                XLOG_XACT_ABORT | XLOG_XACT_ABORT_PREPARED => {
                    match parse_xact_payload(info, &record.parsed.main_data, record.page_magic) {
                        Ok(p) => self.patch.abort(p.twophase_xid.unwrap_or(xid), &p.subxacts),
                        Err(e) => self.fail_closed(format!("malformed abort payload: {e}")),
                    }
                }
                _ => {}
            }
            return;
        }
        if self.skew.is_some() || record.source_lsn > self.s_max {
            return;
        }
        if rm == RmId::RelMap as u8 {
            // Header names the map's database; dbid 0 is the shared map,
            // which holds no pg_class / pg_attribute of any database
            match parse_xl_relmap_update(&record.parsed.main_data) {
                Some(map) if map.dbid == self.target_db_oid => {
                    self.skew = Some("relmap update (mapped-catalog rewrite)".into());
                }
                Some(_) => {}
                None => self.skew = Some("malformed relmap update".into()),
            }
            return;
        }
        if rm != RmId::Heap as u8 {
            return;
        }
        let info = record.parsed.header.info;
        if info & XLOG_HEAP_OPMASK == XLOG_HEAP_TRUNCATE {
            // OIDs are unique per database, so the record's own dbId
            // decides whether they can name a filtered rel at all
            let Some(truncate) = parse_xl_heap_truncate(&record.parsed.main_data) else {
                self.skew = Some("malformed TRUNCATE".into());
                return;
            };
            if truncate.db_oid != self.target_db_oid {
                return;
            }
            if let Some(oid) = truncate
                .relids
                .iter()
                .find(|oid| self.filter_oids.contains(oid))
            {
                self.skew = Some(format!("TRUNCATE of oid {oid}"));
            }
            return;
        }
        let Some(block) = record.parsed.blocks.first() else {
            return;
        };
        let locator = block.header.location.rel;
        if locator.db_node != self.target_db_oid {
            return;
        }
        let rel_node = locator.rel_node;
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

impl RecordSink for PrescanSink {
    fn on_record<'a>(
        &'a mut self,
        record: &'a Record<'a>,
    ) -> Pin<Box<dyn Future<Output = std::result::Result<(), SinkError>> + Send + 'a>> {
        self.observe(record);
        Box::pin(std::future::ready(Ok(())))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::decode::visibility::{
        HEAP_XMAX_INVALID, HEAP_XMAX_IS_MULTI, HEAP_XMIN_COMMITTED, HEAP_XMIN_INVALID,
    };
    use crate::decode::wal_xact::{XACT_XINFO_HAS_TWOPHASE, XLOG_XACT_HAS_INFO};
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

    /// Requests live in database 5; 6 is another database on the cluster
    const TARGET_DB: Oid = 5;
    const FOREIGN_DB: Oid = 6;

    fn catalog_rfn(rel_node: u32) -> RelFileNode {
        db_rfn(TARGET_DB, rel_node)
    }

    fn db_rfn(db_node: Oid, rel_node: u32) -> RelFileNode {
        RelFileNode {
            spc_node: 1663,
            db_node,
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
            target_db_oid: TARGET_DB,
            filter_oids: HashSet::from_iter([filter_oid]),
            current_rfns: HashMap::from_iter([(filter_oid, current_rfn)]),
            patch: PgXactPatch::new(),
            skew: None,
            s_max: u64::MAX,
        }
    }

    /// `xl_heap_truncate`: `dbId, nrelids, flags + align pad, relids[]`
    fn truncate_main_data(db_oid: Oid, relids: &[u32]) -> Vec<u8> {
        let mut md = Vec::new();
        md.extend_from_slice(&db_oid.to_le_bytes());
        md.extend_from_slice(&(relids.len() as u32).to_le_bytes());
        md.extend_from_slice(&[0u8; 4]); // flags + align pad
        for oid in relids {
            md.extend_from_slice(&oid.to_le_bytes());
        }
        md
    }

    /// `xl_relmap_update` header; body is irrelevant to the skew verdict
    fn relmap_main_data(dbid: Oid) -> Vec<u8> {
        let mut md = dbid.to_le_bytes().to_vec();
        md.extend_from_slice(&1663u32.to_le_bytes()); // tsid
        md.extend_from_slice(&512i32.to_le_bytes()); // nbytes
        md
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

    /// `xl_xact_twophase` payload: xact_time, xinfo, then the prepared xid
    fn two_phase_main_data(prepared: u32) -> Vec<u8> {
        let mut md: Vec<u8> = 7i64.to_le_bytes().to_vec();
        md.extend_from_slice(&XACT_XINFO_HAS_TWOPHASE.to_le_bytes());
        md.extend_from_slice(&prepared.to_le_bytes());
        md
    }

    /// Patch prepared xid rather than finishing backend xid
    #[test]
    fn prescan_keys_two_phase_verdicts_to_the_prepared_xid() {
        let mut s = prescan(16400, 16400);
        s.observe(&record(
            RmId::Xact,
            XLOG_XACT_COMMIT_PREPARED | XLOG_XACT_HAS_INFO,
            777,
            two_phase_main_data(800),
            None,
        ));
        s.observe(&record(
            RmId::Xact,
            XLOG_XACT_ABORT_PREPARED | XLOG_XACT_HAS_INFO,
            778,
            two_phase_main_data(801),
            None,
        ));
        assert!(s.skew.is_none());
        let accum = PgXactAccum::new();
        let view = PgXactView::new(&accum, &s.patch);
        assert_eq!(
            view.xid_status(800),
            crate::decode::visibility::XidStatus::Committed
        );
        assert_eq!(
            view.xid_status(801),
            crate::decode::visibility::XidStatus::Aborted
        );
        assert_eq!(s.patch.len(), 2, "finishing backend's xid stays unpatched");
    }

    /// Reject truncated subxact payload
    #[test]
    fn prescan_fails_closed_on_malformed_xact_payload() {
        let mut s = prescan(16400, 16400);
        s.observe(&record(
            RmId::Xact,
            XLOG_XACT_COMMIT | XLOG_XACT_HAS_INFO,
            700,
            vec![0u8; 10],
            None,
        ));
        assert!(s.skew.is_some(), "malformed commit payload fails closed");
        assert!(s.patch.is_empty());
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
        s.observe(&record(
            RmId::RelMap,
            0x00,
            0,
            relmap_main_data(TARGET_DB),
            None,
        ));
        assert!(s.skew.is_some());

        let md = truncate_main_data(TARGET_DB, &[777, 16400]);
        let mut s = prescan(16400, 16400);
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

    /// Relation OIDs, mapped-catalog filenodes and pg_class rows all repeat
    /// per database: only the record's own database decides skew
    #[test]
    fn prescan_ignores_foreign_database_catalog_activity() {
        // Same oid, other database's TRUNCATE
        let mut s = prescan(16400, 16400);
        s.observe(&record(
            RmId::Heap,
            XLOG_HEAP_TRUNCATE,
            12,
            truncate_main_data(FOREIGN_DB, &[16400]),
            None,
        ));
        assert!(s.skew.is_none(), "foreign TRUNCATE cannot name our rel");

        // Malformed TRUNCATE proves no database: fail closed
        let mut s = prescan(16400, 16400);
        s.observe(&record(
            RmId::Heap,
            XLOG_HEAP_TRUNCATE,
            12,
            vec![0u8; 4],
            None,
        ));
        assert!(s.skew.is_some(), "malformed TRUNCATE fails closed");

        // Relmap of another database, and of the shared map
        for dbid in [FOREIGN_DB, 0] {
            let mut s = prescan(16400, 16400);
            s.observe(&record(RmId::RelMap, 0x00, 0, relmap_main_data(dbid), None));
            assert!(s.skew.is_none(), "relmap dbid {dbid} is not our skew");
        }
        let mut s = prescan(16400, 16400);
        s.observe(&record(RmId::RelMap, 0x00, 0, vec![0u8; 4], None));
        assert!(s.skew.is_some(), "malformed relmap fails closed");

        // pg_class / pg_attribute writes for a colliding oid elsewhere,
        // plus one this walk could not decode at all
        for rel_node in [PG_CLASS_RELNODE, PG_ATTRIBUTE_RELNODE] {
            let mut s = prescan(16400, 16400);
            s.observe(&record(
                RmId::Heap,
                0x00,
                10,
                Vec::new(),
                Some((
                    db_rfn(FOREIGN_DB, rel_node),
                    pg_class_insert_block(16400, 99999),
                )),
            ));
            assert!(s.skew.is_none(), "foreign relnode {rel_node} write ignored");
        }
        let mut s = prescan(16400, 16400);
        s.observe(&record(
            RmId::Heap,
            0x00,
            10,
            Vec::new(),
            Some((db_rfn(FOREIGN_DB, PG_CLASS_RELNODE), vec![0u8; 3])),
        ));
        assert!(
            s.skew.is_none(),
            "undecodable foreign catalog write is not our skew"
        );

        // Commit harvest stays cluster-wide: backup pages hold XIDs from
        // every database
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
        assert_eq!(s.patch.len(), 2);
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
    fn target_db_oid_rejects_empty_and_mixed_request_sets() {
        let req = |db_node: Oid| {
            let mut desc = crate::backfill::backup_page_walk::make_rel();
            desc.rfn.db_node = db_node;
            BackupRequest {
                desc: Arc::new(desc),
                s_lsn: 0x1000,
            }
        };
        assert!(target_db_oid(&[]).is_err(), "nothing to scope the prescan");
        assert_eq!(target_db_oid(&[req(TARGET_DB), req(TARGET_DB)]).unwrap(), 5);
        assert!(target_db_oid(&[req(TARGET_DB), req(FOREIGN_DB)]).is_err());
        assert!(target_db_oid(&[req(0)]).is_err(), "no database oid");
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

        let (stats, _segments) = gate.await.unwrap().unwrap();
        let mut got = Vec::new();
        while let Some(t) = gated_rx.recv().await {
            got.push(t.xid);
        }
        assert_eq!(got, [100, 500]);
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

        let (stats, _segments) = gate.await.unwrap().unwrap();
        let mut got = Vec::new();
        while let Some(t) = gated_rx.recv().await {
            got.push(t.xid);
        }
        assert_eq!(got, [100], "deferred tuple not emitted");
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

        let (stats, _segments) = gate.await.unwrap().unwrap();
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
