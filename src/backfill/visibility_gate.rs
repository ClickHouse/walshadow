//! Filter backup-page tuples by PostgreSQL visibility
//!
//! [`stream_phase`] resolves hint bits and spools unknowns. [`resolve_phase`]
//! uses complete backup transaction logs plus WAL commit overlay. Greenfield
//! repairs relations whose tuples remain undecidable

use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result};
use tokio::sync::mpsc;
use walrus::pg::replication::conn::PgConfig;

use crate::backfill::backup_page_walk::{BOOTSTRAP_TUPLE_CHANNEL_CAP, BackfillTuple, CatalogMap};
use crate::backfill::spool::{DEFERRED_SPOOL_MEM_MAX, DeferredSpool};
use crate::backfill::visibility_repair::{PendingReason, PendingSet, RepairStats, repair};
use crate::config::ResolvedConfig;
use crate::decode::visibility::{
    HEAP_XMAX_IS_MULTI, PgMultiXactAccum, PgXactAccum, PgXactPatch, PgXactView, Visibility,
    read_pg_multixact, read_pg_xact, tuple_visibility,
};
use crate::emit::ch_emitter::{EmitterConfig, EmitterStats};
use crate::emit::pipeline::tail::OwnedTail;
use crate::emit::pipeline::{Fatal, bootstrap};
use crate::mapping::MappingHandle;
use crate::schema::RelName;
use crate::toast::ToastResolver;
use ahash::HashSet;

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct GateStats {
    pub emitted: u64,
    pub gated: u64,
    pub deferred: u64,
    pub multixact_emitted: u64,
    /// Chunk tuples the hint bits proved dead, dropped before the store
    pub chunks_gated: u64,
    /// Walked tuples replaced by relation repair
    pub pending_discarded: u64,
    /// Relations handed to visibility repair
    pub pending_relations: u64,
    /// Relations pending for undecidable multixacts
    pub pending_multixact: u64,
    /// Rows emitted by visibility repair
    pub repaired_rows: u64,
    /// Source write head after final repair read
    pub p_hi: u64,
}

/// Policy for tuples remaining undecidable
#[derive(Debug)]
pub enum Undecidable<'a> {
    /// Abort per-table loads
    Abort,
    /// Hand greenfield relation to repair
    Pending(&'a mut PendingSet),
}

/// Resolve hint bits, spool unknown main tuples, pass chunks unless proven dead
pub async fn stream_phase(
    rx: &mut mpsc::Receiver<BackfillTuple>,
    tx: &mpsc::Sender<BackfillTuple>,
    catalog: &CatalogMap,
    pending: &PendingSet,
    deferred: &mut DeferredSpool,
    stats: &mut GateStats,
) -> Result<(), String> {
    while let Some(t) = rx.recv().await {
        if catalog.is_toast(t.rfn.db_node, t.rfn.rel_node) {
            if tuple_visibility(t.xid, t.xmax, t.infomask, None) == Visibility::Skip {
                stats.chunks_gated += 1;
                continue;
            }
            if tx.send(t).await.is_err() {
                break;
            }
            continue;
        }
        // Repair replaces main-page tuples
        if pending.holds(t.rfn.db_node, t.rfn.rel_node) {
            stats.pending_discarded += 1;
            continue;
        }
        match tuple_visibility(t.xid, t.xmax, t.infomask, None) {
            Visibility::Emit => {
                if !emit(t, tx, stats).await {
                    break;
                }
            }
            Visibility::Skip => stats.gated += 1,
            // Multixact verdict requires complete view
            Visibility::Defer | Visibility::Unresolvable => deferred
                .push(t)
                .await
                .map_err(|e| format!("visibility gate: deferred spool: {e}"))?,
        }
    }
    stats.deferred = deferred.records();
    Ok(())
}

/// Replay spool against complete transaction view
pub async fn resolve_phase(
    deferred: DeferredSpool,
    view: &PgXactView<'_>,
    tx: &mpsc::Sender<BackfillTuple>,
    mut undecidable: Undecidable<'_>,
    stats: &mut GateStats,
) -> Result<(), String> {
    let mut replay = deferred
        .into_reader()
        .await
        .map_err(|e| format!("visibility gate: deferred spool seal: {e}"))?;
    let mut fail = None;
    while let Some(t) = replay
        .next()
        .await
        .map_err(|e| format!("visibility gate: deferred spool replay: {e}"))?
    {
        match tuple_visibility(t.xid, t.xmax, t.infomask, Some(view)) {
            Visibility::Emit => {
                if !emit(t, tx, stats).await {
                    break;
                }
            }
            Visibility::Skip | Visibility::Defer => stats.gated += 1,
            Visibility::Unresolvable => match &mut undecidable {
                Undecidable::Abort => {
                    fail = Some(undecidable_multixact(&t));
                    break;
                }
                Undecidable::Pending(set) => {
                    set.mark(t.rfn, PendingReason::UnresolvedMultiXact);
                    stats.pending_discarded += 1;
                }
            },
        }
    }
    replay
        .finish()
        .await
        .map_err(|e| format!("visibility gate: deferred spool cleanup: {e}"))?;
    match fail {
        Some(e) => Err(e),
        None => Ok(()),
    }
}

async fn emit(t: BackfillTuple, tx: &mpsc::Sender<BackfillTuple>, stats: &mut GateStats) -> bool {
    if t.infomask & HEAP_XMAX_IS_MULTI != 0 {
        stats.multixact_emitted += 1;
    }
    stats.emitted += 1;
    tx.send(t).await.is_ok()
}

fn undecidable_multixact(t: &BackfillTuple) -> String {
    format!(
        "visibility gate: multixact xmax {} (rfn {}/{}) unresolvable from the backup's \
         pg_multixact snapshot; remedy: fresher backup, or initial_load='copy'",
        t.xmax, t.rfn.db_node, t.rfn.rel_node
    )
}

/// Greenfield resolution inputs
pub struct PendingGate {
    pub deferred: DeferredSpool,
    pub catalog: CatalogMap,
    pub mapping: MappingHandle,
    pub config: Arc<ResolvedConfig>,
    pub emitter: EmitterConfig,
    pub stats: Arc<EmitterStats>,
    pub resolver: ToastResolver,
    pub oracle: Option<Arc<crate::ops::oracle::Oracle>>,
    /// Relations excluded from initial load
    pub skip_initial: HashSet<RelName>,
    /// Source endpoint for repair reads
    pub source: PgConfig,
    /// Relations handed to repair
    pub pending: PendingSet,
    /// Coverage tag for walked and repaired rows
    pub start_lsn: u64,
    /// Repair spill root
    pub spill_dir: PathBuf,
    /// Streaming-phase counters
    pub stream_stats: GateStats,
}

/// Resolve deferred tuples and repair pending relations
pub async fn resolve_greenfield(
    gate: PendingGate,
    data_dir: &Path,
    patch: &PgXactPatch,
) -> Result<GateStats> {
    let PendingGate {
        deferred,
        catalog,
        mapping,
        config,
        emitter,
        stats,
        resolver,
        oracle,
        skip_initial,
        source,
        mut pending,
        start_lsn,
        spill_dir,
        stream_stats: mut gate_stats,
    } = gate;
    if deferred.records() == 0 && pending.is_empty() {
        return Ok(gate_stats);
    }
    // Repair rides the same drain, so it needs its own view of what the
    // drain takes ownership of
    let repair_catalog = catalog.clone();
    let repair_skip = skip_initial.clone();
    // SLRUs only answer the spool; a pending-only pass reads none of them
    let (accum, multi) = if deferred.records() == 0 {
        (PgXactAccum::new(), PgMultiXactAccum::new())
    } else {
        (
            read_pg_xact(data_dir).await?,
            read_pg_multixact(data_dir).await?,
        )
    };
    let view = PgXactView::new(&accum, patch).with_multixact(&multi);

    let tail = OwnedTail::spawn(
        &emitter,
        1,
        stats.clone(),
        Fatal::new(),
        None,
        oracle,
        "visibility gate",
    )
    .await
    .map_err(anyhow::Error::msg)?;

    let toast_spool = spill_dir.join("bootstrap_gate_toast.bin");
    tokio::fs::remove_file(&toast_spool).await.ok();
    let (tx, rx) = mpsc::channel::<BackfillTuple>(BOOTSTRAP_TUPLE_CHANNEL_CAP);
    let drain = tokio::spawn(bootstrap::drain(
        rx,
        catalog,
        mapping,
        tail.msg_tx.clone(),
        tail.ack.clone(),
        stats,
        resolver,
        DeferredSpool::new(toast_spool, DEFERRED_SPOOL_MEM_MAX),
        emitter.row_policy(),
        Some(config),
        skip_initial,
    ));

    let mut resolved = resolve_phase(
        deferred,
        &view,
        &tx,
        Undecidable::Pending(&mut pending),
        &mut gate_stats,
    )
    .await;
    gate_stats.pending_relations = pending.len() as u64;
    gate_stats.pending_multixact = pending.count_for(PendingReason::UnresolvedMultiXact);
    if resolved.is_ok() {
        match repair(
            &pending,
            &repair_catalog,
            &repair_skip,
            &source,
            start_lsn,
            &tx,
        )
        .await
        {
            Ok(r) => {
                gate_stats.repaired_rows = r.rows;
                gate_stats.p_hi = r.p_hi;
                log_repair(&r, gate_stats.pending_multixact);
            }
            Err(e) => resolved = Err(format!("{e:#}")),
        }
    }
    drop(tx);
    let drained = drain.await.context("visibility gate: drain join")?;
    // Drain tail before surfacing errors
    let next_seq = match (resolved, drained) {
        (Ok(()), Ok(o)) => o.next_seq,
        (Err(e), _) | (_, Err(e)) => {
            tail.quiesce().await;
            anyhow::bail!(e);
        }
    };
    tail.finish(next_seq).await.map_err(anyhow::Error::msg)?;
    Ok(gate_stats)
}

/// Log repair convergence frontier
fn log_repair(r: &RepairStats, multixact_relations: u64) {
    if r.relations == 0 && r.skipped == 0 {
        return;
    }
    tracing::info!(
        target: "walshadow::bootstrap",
        relations = r.relations,
        rows = r.rows,
        skipped = r.skipped,
        multixact_relations,
        p_hi = r.p_hi,
        "visibility repair read pending relations through PostgreSQL",
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backfill::backup_page_walk::make_rel_named;
    use crate::decode::visibility::{
        HEAP_XMAX_COMMITTED, HEAP_XMAX_INVALID, HEAP_XMAX_IS_MULTI, HEAP_XMIN_COMMITTED,
        HEAP_XMIN_INVALID,
    };
    use ahash::HashSetExt;
    use walrus::pg::walparser::RelFileNode;

    fn rfn(rel_node: u32) -> RelFileNode {
        RelFileNode {
            spc_node: 1663,
            db_node: 5,
            rel_node,
        }
    }

    fn tuple(xid: u32, xmax: u32, infomask: u16) -> BackfillTuple {
        BackfillTuple {
            rfn: rfn(16400),
            xid,
            xmax,
            infomask,
            source_lsn: 0x1000,
            blkno: 0,
            offnum: 0,
            columns: Vec::new(),
        }
    }

    async fn spool_of(tuples: Vec<BackfillTuple>) -> DeferredSpool {
        let mut spool = DeferredSpool::new(
            std::env::temp_dir().join("ws-visibility-gate-unused.bin"),
            DEFERRED_SPOOL_MEM_MAX,
        );
        for t in tuples {
            spool.push(t).await.unwrap();
        }
        spool
    }

    fn toast_catalog(rel_node: u32) -> CatalogMap {
        let mut catalog = CatalogMap::new();
        catalog.insert(make_rel_named(
            rel_node,
            rel_node,
            0,
            RelName::new("pg_toast", &format!("pg_toast_{rel_node}")),
        ));
        catalog
    }

    /// Drive the walk phase over `tuples`, return its tally and whatever it
    /// let through
    async fn run_stream_phase(
        catalog: &CatalogMap,
        pending: &PendingSet,
        tuples: Vec<BackfillTuple>,
    ) -> (GateStats, Vec<BackfillTuple>) {
        let (tx, mut rx) = mpsc::channel(8);
        let (walk_tx, mut walk_rx) = mpsc::channel(8);
        for t in tuples {
            walk_tx.send(t).await.unwrap();
        }
        drop(walk_tx);

        let mut stats = GateStats::default();
        let mut spool = spool_of(Vec::new()).await;
        stream_phase(&mut walk_rx, &tx, catalog, pending, &mut spool, &mut stats)
            .await
            .unwrap();
        drop(tx);

        let mut passed = Vec::new();
        while let Some(t) = rx.recv().await {
            passed.push(t);
        }
        (stats, passed)
    }

    /// Drop proven-dead chunk generations
    #[tokio::test]
    async fn proven_dead_chunks_never_reach_the_store() {
        let chunk = |xmax, infomask| BackfillTuple {
            rfn: rfn(16401),
            xmax,
            infomask,
            ..tuple(100, 0, 0)
        };
        // Deleted value, aborted insert, then one that is still live
        let (stats, passed) = run_stream_phase(
            &toast_catalog(16401),
            &PendingSet::empty(),
            vec![
                chunk(200, HEAP_XMIN_COMMITTED | HEAP_XMAX_COMMITTED),
                chunk(0, HEAP_XMIN_INVALID),
                chunk(0, HEAP_XMIN_COMMITTED | HEAP_XMAX_INVALID),
            ],
        )
        .await;

        assert_eq!(stats.chunks_gated, 2);
        assert_eq!(passed.len(), 1, "only the live chunk passes");
        // Chunks never take the main-tuple counters or the spool
        assert_eq!(stats.emitted, 0);
        assert_eq!(stats.deferred, 0);
    }

    /// Repair replaces main rows but preserves chunk cache
    #[tokio::test]
    async fn pending_relation_drops_its_pages_and_keeps_its_chunks() {
        let mut catalog = CatalogMap::new();
        catalog.insert(make_rel_named(
            16400,
            16400,
            16402,
            RelName::new("public", "t"),
        ));
        catalog.insert(make_rel_named(
            16402,
            16403,
            0,
            RelName::new("pg_toast", "pg_toast_16400"),
        ));
        let pending = PendingSet::toast_capable(&catalog);

        let live = HEAP_XMIN_COMMITTED | HEAP_XMAX_INVALID;
        let (stats, passed) = run_stream_phase(
            &catalog,
            &pending,
            vec![
                tuple(100, 0, live),
                BackfillTuple {
                    rfn: rfn(16403),
                    ..tuple(100, 0, live)
                },
            ],
        )
        .await;

        assert_eq!(stats.pending_discarded, 1, "its main page is dropped");
        assert_eq!(stats.emitted, 0);
        assert_eq!(
            passed.iter().map(|t| t.rfn.rel_node).collect::<Vec<_>>(),
            [16403],
            "only its chunk reaches the store",
        );
    }

    /// Preserve chunks without decisive hint bits
    #[tokio::test]
    async fn undecidable_chunks_still_pass_through() {
        let (stats, passed) = run_stream_phase(
            &toast_catalog(16401),
            &PendingSet::empty(),
            vec![BackfillTuple {
                rfn: rfn(16401),
                ..tuple(100, 0, 0)
            }],
        )
        .await;

        assert_eq!(stats.chunks_gated, 0);
        assert_eq!(passed.len(), 1);
        assert_eq!(stats.deferred, 0, "chunks never spool");
    }

    #[tokio::test]
    async fn greenfield_hands_undecidable_multixact_relation_over() {
        let accum = PgXactAccum::new();
        let patch = PgXactPatch::new();
        // No collected offsets segment: the mxid is below any known range
        let multi = PgMultiXactAccum::new();
        let view = PgXactView::new(&accum, &patch).with_multixact(&multi);
        let (tx, mut rx) = mpsc::channel(4);
        let mut stats = GateStats::default();
        let spool = spool_of(vec![tuple(
            100,
            10,
            HEAP_XMIN_COMMITTED | HEAP_XMAX_IS_MULTI,
        )])
        .await;

        let mut pending = PendingSet::empty();
        resolve_phase(
            spool,
            &view,
            &tx,
            Undecidable::Pending(&mut pending),
            &mut stats,
        )
        .await
        .unwrap();
        drop(tx);

        assert!(
            rx.recv().await.is_none(),
            "an undecidable tuple is never guessed"
        );
        assert_eq!(stats.emitted, 0);
        assert_eq!(pending.len(), 1, "its relation goes to the repair path");
        assert_eq!(pending.count_for(PendingReason::UnresolvedMultiXact), 1);
        assert!(pending.holds(5, 16400));
    }

    #[tokio::test]
    async fn per_table_aborts_on_undecidable_multixact() {
        let accum = PgXactAccum::new();
        let patch = PgXactPatch::new();
        let multi = PgMultiXactAccum::new();
        let view = PgXactView::new(&accum, &patch).with_multixact(&multi);
        let (tx, _rx) = mpsc::channel(4);
        let mut stats = GateStats::default();
        let spool = spool_of(vec![tuple(
            100,
            10,
            HEAP_XMIN_COMMITTED | HEAP_XMAX_IS_MULTI,
        )])
        .await;

        let err = resolve_phase(spool, &view, &tx, Undecidable::Abort, &mut stats)
            .await
            .unwrap_err();
        assert!(err.contains("pg_multixact"), "{err}");
        assert_eq!(stats.emitted, 0);
    }

    /// Nothing deferred: no tail, no CH connection, no work
    #[tokio::test]
    async fn resolve_greenfield_is_a_noop_without_deferrals() {
        let pending = PendingGate {
            deferred: spool_of(Vec::new()).await,
            catalog: CatalogMap::new(),
            mapping: crate::mapping::mapping_handle(Default::default()),
            config: Arc::new(ResolvedConfig::default()),
            emitter: EmitterConfig::default(),
            stats: Arc::new(EmitterStats::default()),
            resolver: ToastResolver::disabled(),
            oracle: None,
            skip_initial: HashSet::new(),
            source: crate::config::SourceConn::default().to_pg_config(),
            pending: PendingSet::empty(),
            start_lsn: 0x1000,
            spill_dir: std::env::temp_dir(),
            stream_stats: GateStats {
                emitted: 7,
                ..Default::default()
            },
        };
        let stats = resolve_greenfield(pending, Path::new("/nonexistent"), &PgXactPatch::new())
            .await
            .unwrap();
        assert_eq!(stats.emitted, 7);
    }
}
