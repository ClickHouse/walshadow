//! Filter backup-page tuples by PostgreSQL visibility
//!
//! [`stream_phase`] resolves hint bits and spools unknowns. [`resolve_phase`]
//! uses complete backup transaction logs plus WAL commit overlay, routing
//! tuples whose writer or deleter is still running to
//! [pending tables](crate::backfill::visibility_pending). A multixact the
//! backup snapshot cannot bound ends the pass

use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::Result;
use tokio::sync::mpsc;
use tokio_util::task::AbortOnDropHandle;

use crate::backfill::backup_page_walk::{BOOTSTRAP_TUPLE_CHANNEL_CAP, BackfillTuple, CatalogMap};
use crate::backfill::spool::{DEFERRED_SPOOL_MEM_MAX, DeferredSpool};
use crate::backfill::visibility_pending::{PendingManifest, PendingSpool};
use crate::config::ResolvedConfig;
use crate::decode::visibility::{
    HEAP_XMAX_IS_MULTI, PgXactPatch, PgXactView, Visibility, deferred_xids, read_pg_multixact,
    read_pg_xact, tuple_visibility,
};
use crate::emit::ch_emitter::{EmitterConfig, EmitterStats};
use crate::emit::pipeline::ack::AckHandle;
use crate::emit::pipeline::batcher::BatcherMsg;
use crate::emit::pipeline::bootstrap::BootstrapDrainOutcome;
use crate::emit::pipeline::tail::OwnedTail;
use crate::emit::pipeline::{Fatal, bootstrap};
use crate::mapping::MappingSnapshot;
use crate::schema::RelName;
use crate::toast::ToastResolver;
use ahash::HashSet;

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct GateStats {
    pub emitted: u64,
    pub gated: u64,
    pub deferred: u64,
    /// Undecided tuples handed to a pending table
    pub pending: u64,
    pub multixact_emitted: u64,
    /// Chunk tuples the hint bits proved dead, dropped before the store
    pub chunks_gated: u64,
}

/// Resolve hint bits, spool unknown main tuples, pass chunks unless proven dead
///
/// Slab in, slab out: the walk's batching survives the gate, which only drops
/// what the verdict removes
pub async fn stream_phase(
    rx: &mut mpsc::Receiver<Vec<BackfillTuple>>,
    tx: &mpsc::Sender<Vec<BackfillTuple>>,
    catalog: &CatalogMap,
    deferred: &mut DeferredSpool,
    stats: &mut GateStats,
) -> Result<(), String> {
    while let Some(slab) = rx.recv().await {
        let mut pass = Vec::with_capacity(slab.len());
        for t in slab {
            if catalog.is_toast(t.rfn.db_node, t.rfn.rel_node) {
                if tuple_visibility(t.xid, t.xmax, t.infomask, None) == Visibility::Skip {
                    stats.chunks_gated += 1;
                    continue;
                }
                pass.push(t);
                continue;
            }
            let visibility = tuple_visibility(t.xid, t.xmax, t.infomask, None);
            match visibility {
                Visibility::Emit => {
                    if t.infomask & HEAP_XMAX_IS_MULTI != 0 {
                        stats.multixact_emitted += 1;
                    }
                    stats.emitted += 1;
                    pass.push(t);
                }
                Visibility::Skip => stats.gated += 1,
                // Multixact verdict requires complete view
                Visibility::Defer | Visibility::Unresolvable => deferred
                    .push(t)
                    .await
                    .map_err(|e| format!("visibility gate: deferred spool: {e}"))?,
            }
        }
        if !pass.is_empty() && tx.send(pass).await.is_err() {
            break;
        }
    }
    stats.deferred = deferred.records();
    Ok(())
}

/// Replay spool against complete transaction view
///
/// Retain in-progress tuples in `pending_spool` when supplied; otherwise discard
pub async fn resolve_phase(
    deferred: DeferredSpool,
    view: &PgXactView<'_>,
    tx: &mpsc::Sender<Vec<BackfillTuple>>,
    mut pending_spool: Option<&mut PendingSpool>,
    stats: &mut GateStats,
) -> Result<(), String> {
    let mut out = SlabTx::new(tx);
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
                if t.infomask & HEAP_XMAX_IS_MULTI != 0 {
                    stats.multixact_emitted += 1;
                }
                stats.emitted += 1;
                if !out.push(t).await {
                    break;
                }
            }
            Visibility::Skip => stats.gated += 1,
            Visibility::Defer => {
                let pending = deferred_xids(t.xid, t.xmax, t.infomask, view);
                let retained = match pending_spool.as_mut() {
                    Some(spool) if !pending.is_empty() => spool.push(t, pending).await?,
                    _ => false,
                };
                if retained {
                    stats.pending += 1;
                } else {
                    stats.gated += 1;
                }
            }
            Visibility::Unresolvable => {
                fail = Some(undecidable_multixact(&t));
                break;
            }
        }
    }
    out.flush().await;
    replay
        .finish()
        .await
        .map_err(|e| format!("visibility gate: deferred spool cleanup: {e}"))?;
    match fail {
        Some(e) => Err(e),
        None => Ok(()),
    }
}

/// Regroup a per-tuple source into walk-sized slabs, so the spool replay
/// costs the drain one channel hop per batch, not per row
struct SlabTx<'a> {
    tx: &'a mpsc::Sender<Vec<BackfillTuple>>,
    buf: Vec<BackfillTuple>,
    closed: bool,
}

impl<'a> SlabTx<'a> {
    fn new(tx: &'a mpsc::Sender<Vec<BackfillTuple>>) -> Self {
        Self {
            tx,
            buf: Vec::with_capacity(GATE_SLAB_ROWS),
            closed: false,
        }
    }

    /// `false` once the drain hangs up
    async fn push(&mut self, t: BackfillTuple) -> bool {
        self.buf.push(t);
        if self.buf.len() < GATE_SLAB_ROWS {
            return !self.closed;
        }
        self.flush().await
    }

    async fn flush(&mut self) -> bool {
        if self.closed || self.buf.is_empty() {
            return !self.closed;
        }
        let slab = std::mem::replace(&mut self.buf, Vec::with_capacity(GATE_SLAB_ROWS));
        self.closed = self.tx.send(slab).await.is_err();
        !self.closed
    }
}

/// Rows per slab out of the spool replay. Row width is unbounded here, so
/// the drain's own budget is the resident ceiling
const GATE_SLAB_ROWS: usize = 256;

fn undecidable_multixact(t: &BackfillTuple) -> String {
    format!(
        "visibility gate: multixact xmax {} (rfn {}/{}) unresolvable from the backup's \
         pg_multixact snapshot; remedy: fresher backup, or initial_load='copy'",
        t.xmax, t.rfn.db_node, t.rfn.rel_node
    )
}

/// Destination both greenfield legs write: page walk, then deferred
/// resolution. One frozen mapping serves every lane, so no route wants a
/// value another lane rendered from different rules
pub struct GreenfieldSink {
    pub catalog: CatalogMap,
    pub mapping: MappingSnapshot,
    pub config: Arc<ResolvedConfig>,
    pub emitter: EmitterConfig,
    pub stats: Arc<EmitterStats>,
    pub resolver: ToastResolver,
    /// Relations excluded from initial load
    pub skip_initial: HashSet<RelName>,
    /// Holds each lane's deferred-referrer spool
    pub scratch_dir: PathBuf,
}

pub struct DrainLanes {
    lanes: Vec<AbortOnDropHandle<Result<BootstrapDrainOutcome, String>>>,
}

impl GreenfieldSink {
    /// Spawn one drain per insert tail. Gate stage owns the returned senders;
    /// dropping each winds its lane down
    pub async fn spawn(
        &self,
        tails: Vec<(mpsc::Sender<BatcherMsg>, AckHandle)>,
        tag: &str,
    ) -> (Vec<mpsc::Sender<Vec<BackfillTuple>>>, DrainLanes) {
        let mut senders = Vec::with_capacity(tails.len());
        let mut lanes = Vec::with_capacity(tails.len());
        for (i, (msg_tx, ack)) in tails.into_iter().enumerate() {
            let (tx, rx) = mpsc::channel(BOOTSTRAP_TUPLE_CHANNEL_CAP);
            // Stale files from a crashed pass block create_new
            let spool = self.scratch_dir.join(format!("{tag}_deferred.{i}.bin"));
            tokio::fs::remove_file(&spool).await.ok();
            lanes.push(AbortOnDropHandle::new(tokio::spawn(bootstrap::drain(
                rx,
                self.catalog.clone(),
                self.mapping.clone(),
                msg_tx,
                ack,
                self.stats.clone(),
                self.resolver.clone(),
                // A referrer can want chunks a sibling lane is still putting
                bootstrap::Deferral::Handback(DeferredSpool::new(spool, DEFERRED_SPOOL_MEM_MAX)),
                self.emitter.row_policy(),
                Some(self.config.clone()),
                self.skip_initial.clone(),
            ))));
            senders.push(tx);
        }
        (senders, DrainLanes { lanes })
    }

    /// Render referrers the lanes handed back, on one lane's tail, which
    /// therefore closes its seq space last
    pub async fn resolve_deferred(
        &self,
        spools: Vec<DeferredSpool>,
        msg_tx: &mpsc::Sender<BatcherMsg>,
        ack: &AckHandle,
        first_seq: u64,
    ) -> Result<BootstrapDrainOutcome, String> {
        let row_policy = self.emitter.row_policy();
        let mut out = BootstrapDrainOutcome {
            next_seq: first_seq,
            ..Default::default()
        };
        for spool in spools {
            let resolved = bootstrap::drain_deferred(
                spool,
                &self.catalog,
                &self.mapping,
                msg_tx,
                ack,
                &self.stats,
                &self.resolver,
                &row_policy,
                Some(&self.config),
                out.next_seq,
            )
            .await?;
            out.next_seq = resolved.next_seq;
            out.rows_routed += resolved.rows_routed;
        }
        Ok(out)
    }
}

impl DrainLanes {
    /// Joins every lane before surfacing an error, so a failed one still lets
    /// the others finish what they hold
    pub async fn join(self) -> Result<Vec<BootstrapDrainOutcome>, String> {
        let mut drained = Vec::with_capacity(self.lanes.len());
        for lane in self.lanes {
            drained.push(join_stage(lane, "drain").await);
        }
        drained.into_iter().collect()
    }
}

async fn join_stage<T, E: std::fmt::Display>(
    handle: AbortOnDropHandle<Result<T, E>>,
    stage: &str,
) -> Result<T, String> {
    match handle.await {
        Ok(Ok(v)) => Ok(v),
        Ok(Err(e)) => Err(format!("bootstrap {stage}: {e:#}")),
        Err(e) => Err(format!("bootstrap {stage} join: {e}")),
    }
}

/// Greenfield resolution inputs
pub struct PendingGate {
    /// One per gate task
    pub deferred: Vec<DeferredSpool>,
    pub sink: GreenfieldSink,
    pub oracle: Option<Arc<crate::ops::oracle::Oracle>>,
    /// Streaming-phase counters
    pub stream_stats: GateStats,
}

/// Resolve deferred tuples and retain the undecided as pending rows
pub async fn resolve_greenfield(
    gate: PendingGate,
    data_dir: &Path,
    patch: &PgXactPatch,
) -> Result<(GateStats, Vec<PendingManifest>)> {
    let PendingGate {
        deferred,
        sink,
        oracle,
        stream_stats: mut gate_stats,
    } = gate;
    let scratch_dir = sink.scratch_dir.clone();
    if deferred.iter().map(DeferredSpool::records).sum::<u64>() == 0 {
        return Ok((gate_stats, Vec::new()));
    }
    let accum = read_pg_xact(data_dir).await?;
    let multi = read_pg_multixact(data_dir).await?;
    let view = PgXactView::new(&accum, patch).with_multixact(&multi);

    let oracle_for_pending = oracle.clone();
    let fatal = Fatal::new();
    let tail = OwnedTail::spawn(
        &sink.emitter,
        sink.emitter.inserter_pool_size,
        sink.stats.clone(),
        fatal.clone(),
        None,
        oracle,
        "visibility gate",
    )
    .await
    .map_err(anyhow::Error::msg)?;

    let pending_path = scratch_dir.join("bootstrap_gate_pending.bin");
    tokio::fs::remove_file(&pending_path).await.ok();
    let mut pending_spool = PendingSpool::new(pending_path, sink.catalog.clone());

    // Replays what the walk deferred, not the bulk load: one lane is enough
    let (txs, stages) = sink
        .spawn(vec![(tail.msg_tx.clone(), tail.ack.clone())], "gate_drain")
        .await;
    let mut resolved = Ok(());
    for spool in deferred {
        if spool.records() == 0 {
            continue;
        }
        resolved = resolve_phase(
            spool,
            &view,
            &txs[0],
            Some(&mut pending_spool),
            &mut gate_stats,
        )
        .await;
        if resolved.is_err() {
            break;
        }
    }
    drop(txs);
    // Drain tail before surfacing errors
    let frontier = match (resolved, stages.join().await) {
        (Ok(()), Ok(mut drained)) => {
            let spools = drained
                .iter_mut()
                .filter_map(|d| d.deferred.take())
                .collect();
            let first_seq = drained.iter().map(|d| d.next_seq).max().unwrap_or(0);
            sink.resolve_deferred(spools, &tail.msg_tx, &tail.ack, first_seq)
                .await
                .map(|resolved| resolved.next_seq)
        }
        (Err(e), _) | (_, Err(e)) => Err(e),
    };
    let next_seq = match frontier {
        Ok(next_seq) => next_seq,
        Err(e) => {
            tail.quiesce().await;
            pending_spool.discard().await;
            anyhow::bail!(fatal.message().unwrap_or(e));
        }
    };
    tail.finish(next_seq).await.map_err(anyhow::Error::msg)?;

    // Pending rows need a separate tail after this one closes its seq space
    let pending_tables = crate::backfill::visibility_pending::ship(
        pending_spool,
        &sink.mapping,
        Arc::new(sink.emitter.clone()),
        sink.stats.clone(),
        sink.resolver.clone(),
        Some(sink.config.clone()),
        oracle_for_pending,
        &scratch_dir,
    )
    .await
    .map_err(anyhow::Error::msg)?;
    Ok((gate_stats, pending_tables))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backfill::backup_page_walk::make_rel_named;
    use crate::backfill::spool::DEFERRED_SPOOL_MEM_MAX;
    use crate::decode::visibility::{
        HEAP_XMAX_COMMITTED, HEAP_XMAX_INVALID, HEAP_XMAX_IS_MULTI, HEAP_XMIN_COMMITTED,
        HEAP_XMIN_INVALID,
    };
    use crate::decode::visibility::{PgMultiXactAccum, PgXactAccum};
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

    /// A failed lane surfaces its own message, not a channel-closure artifact
    #[tokio::test]
    async fn drain_failure_surfaces_through_join() {
        let lanes = DrainLanes {
            lanes: vec![
                AbortOnDropHandle::new(tokio::spawn(async {
                    Err("toast store unavailable".to_string())
                })),
                AbortOnDropHandle::new(tokio::spawn(async {
                    Ok(BootstrapDrainOutcome::default())
                })),
            ],
        };
        let err = lanes.join().await.unwrap_err();
        assert_eq!(err, "bootstrap drain: toast store unavailable");
    }

    #[tokio::test]
    async fn drain_lanes_close_independently() {
        let tmp = tempfile::tempdir().unwrap();
        let sink = GreenfieldSink {
            catalog: CatalogMap::new(),
            mapping: Default::default(),
            config: Arc::new(ResolvedConfig::default()),
            emitter: EmitterConfig::default(),
            stats: Arc::new(EmitterStats::default()),
            resolver: ToastResolver::disabled(),
            skip_initial: HashSet::new(),
            scratch_dir: tmp.path().to_path_buf(),
        };
        let (ack, ack_task) = crate::emit::pipeline::ack::spawn(Arc::new(Default::default()));
        let (msg_tx, _msg_rx) = mpsc::channel(1);
        let (mut txs, mut lanes) = sink
            .spawn(
                vec![(msg_tx.clone(), ack.clone()), (msg_tx, ack.clone())],
                "lane-test",
            )
            .await;
        let first = txs.remove(0);
        first
            .send(vec![tuple(100, 0, HEAP_XMIN_COMMITTED)])
            .await
            .unwrap();
        drop(first);
        let lane = lanes.lanes.remove(0);
        let drained =
            tokio::time::timeout(std::time::Duration::from_secs(5), join_stage(lane, "drain"))
                .await
                .expect("first lane closes while second remains open")
                .unwrap();
        assert_eq!(drained.next_seq, 1);
        assert!(!lanes.lanes[0].is_finished());
        drop(txs);
        let drained = lanes.join().await.unwrap();
        assert_eq!(drained[0].next_seq, 0);
        drop(ack);
        ack_task.await.unwrap();
    }

    async fn spool_of(tuples: Vec<BackfillTuple>) -> DeferredSpool {
        spool_at("unused", tuples).await
    }

    async fn spool_at(tag: &str, tuples: Vec<BackfillTuple>) -> DeferredSpool {
        let mut spool = DeferredSpool::new(
            std::env::temp_dir().join(format!("ws-visibility-gate-{tag}.bin")),
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
        tuples: Vec<BackfillTuple>,
    ) -> (GateStats, Vec<BackfillTuple>) {
        let (tx, mut rx) = mpsc::channel(8);
        let (walk_tx, mut walk_rx) = mpsc::channel(8);
        // One tuple per slab: exercises the per-tuple verdicts, not batching
        for t in tuples {
            walk_tx.send(vec![t]).await.unwrap();
        }
        drop(walk_tx);

        let mut stats = GateStats::default();
        let mut spool = spool_of(Vec::new()).await;
        stream_phase(&mut walk_rx, &tx, catalog, &mut spool, &mut stats)
            .await
            .unwrap();
        drop(tx);

        let mut passed = Vec::new();
        while let Some(slab) = rx.recv().await {
            passed.extend(slab);
        }
        (stats, passed)
    }

    /// External pointers ride the stream phase untouched; the drain spools
    /// them for chunk-store resolution
    #[tokio::test]
    async fn mapped_external_values_stream_untouched() {
        use crate::decode::heap_decoder::{ColumnValue, ToastPointer};
        use crate::mapping::{ColumnMapping, TableMapping, TableTarget};
        use crate::schema::RelName;

        let mut desc = make_rel_named(16400, 16400, 16402, RelName::new("public", "t"));
        let d = Arc::make_mut(&mut desc);
        let mut body = d.attributes[0].clone();
        body.attnum = 2;
        body.name = "body".into();
        body.type_oid = crate::schema::TEXTOID;
        body.type_len = -1;
        // SET STORAGE PLAIN leaves existing external values untouched
        body.type_storage = 'p';
        d.attributes.push(body);
        let mut catalog = CatalogMap::new();
        catalog.insert(desc.clone());
        let external = ColumnValue::ExternalToast(ToastPointer {
            va_rawsize: 8004,
            va_extinfo: 8000,
            va_valueid: 100,
            va_toastrelid: 16402,
        });
        for body_mapped in [false, true] {
            let mut columns = vec![ColumnMapping {
                src_attnum: 1,
                target_name: "id".into(),
                target_type: "Int32".into(),
            }];
            if body_mapped {
                columns.push(ColumnMapping {
                    src_attnum: 2,
                    target_name: "body".into(),
                    target_type: "String".into(),
                });
            }
            let mapping = TableMapping {
                target: TableTarget::new("default", "t"),
                columns,
            };
            let row = |value, infomask| BackfillTuple {
                columns: vec![Some(ColumnValue::Int4(1)), Some(value)],
                ..tuple(100, 0, infomask)
            };
            let (stats, passed) = run_stream_phase(
                &catalog,
                vec![
                    row(ColumnValue::Text("inline".into()), HEAP_XMIN_COMMITTED),
                    row(external.clone(), HEAP_XMIN_INVALID),
                    row(external.clone(), HEAP_XMIN_COMMITTED),
                ],
            )
            .await;
            assert_eq!(stats.gated, 1);
            assert_eq!(passed.len(), 2);
            assert!(!passed[0].has_mapped_external(&mapping));
            assert_eq!(passed[1].has_mapped_external(&mapping), body_mapped);
        }
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

    /// Preserve chunks without decisive hint bits
    #[tokio::test]
    async fn undecidable_chunks_still_pass_through() {
        let (stats, passed) = run_stream_phase(
            &toast_catalog(16401),
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

    /// A spool skipped here is rows silently missing from the destination
    #[tokio::test]
    async fn several_spools_all_replay_into_one_output() {
        let accum = PgXactAccum::new();
        let patch = PgXactPatch::new();
        let multi = PgMultiXactAccum::new();
        let view = PgXactView::new(&accum, &patch).with_multixact(&multi);
        let (tx, mut rx) = mpsc::channel(16);
        let mut stats = GateStats::default();

        let mut spools = Vec::new();
        for i in 0..3u32 {
            let mut t = tuple(100, 0, HEAP_XMIN_COMMITTED);
            t.blkno = i;
            spools.push(spool_at(&format!("several-{i}"), vec![t]).await);
        }
        spools.push(spool_at("several-empty", Vec::new()).await);

        for spool in spools {
            if spool.records() == 0 {
                continue;
            }
            resolve_phase(spool, &view, &tx, None, &mut stats)
                .await
                .unwrap();
        }
        drop(tx);

        let mut blknos = Vec::new();
        while let Some(batch) = rx.recv().await {
            blknos.extend(batch.iter().map(|r| r.blkno));
        }
        blknos.sort_unstable();
        assert_eq!(blknos, vec![0, 1, 2], "every spool's tuple must resolve");
        assert_eq!(stats.emitted, 3);
    }

    /// Zeroed pg_xact segment 0: every xid it covers reads in-progress. An
    /// empty accum would read `Unknown` instead, which the gate resolves
    /// against the page it holds
    fn in_progress_accum() -> PgXactAccum {
        let mut accum = PgXactAccum::new();
        accum.insert_segment(0, vec![0; 1024]);
        accum
    }

    /// Retain rows from in-flight writers and deleters until their outcomes arrive:
    /// their rows predate WAL coverage, so a gated verdict is lost data
    #[tokio::test]
    async fn in_flight_tuples_reach_pending() {
        let accum = in_progress_accum();
        let patch = PgXactPatch::new();
        let multi = PgMultiXactAccum::new();
        let view = PgXactView::new(&accum, &patch).with_multixact(&multi);
        let (tx, mut rx) = mpsc::channel(8);
        let mut stats = GateStats::default();
        let mut catalog = CatalogMap::new();
        catalog.insert(make_rel_named(16400, 16400, 0, RelName::new("public", "t")));
        let mut pending_spool = PendingSpool::new(
            std::env::temp_dir().join("ws-gate-pending-test.bin"),
            catalog,
        );

        let spool = spool_of(vec![
            // Writer still running
            tuple(100, 0, 0),
            // Deleter still running over a committed row
            tuple(100, 200, HEAP_XMIN_COMMITTED),
            // Committed writer, no deleter: publishes as before
            tuple(100, 0, HEAP_XMIN_COMMITTED | HEAP_XMAX_INVALID),
        ])
        .await;
        resolve_phase(spool, &view, &tx, Some(&mut pending_spool), &mut stats)
            .await
            .unwrap();
        drop(tx);

        let visible = rx.recv().await.expect("visible rows");
        assert_eq!(visible.len(), 1);
        assert!(rx.recv().await.is_none());
        assert_eq!(stats.emitted, 1);
        assert_eq!(stats.pending, 2);
        assert_eq!(stats.gated, 0, "an in-flight verdict must not discard");
        assert_eq!(pending_spool.rows(), 2);
        pending_spool.discard().await;
    }

    /// Without a pending row sink the undecided fall back to the discard path
    #[tokio::test]
    async fn in_flight_tuples_without_pending_stay_gated() {
        let accum = in_progress_accum();
        let patch = PgXactPatch::new();
        let multi = PgMultiXactAccum::new();
        let view = PgXactView::new(&accum, &patch).with_multixact(&multi);
        let (tx, _rx) = mpsc::channel(8);
        let mut stats = GateStats::default();
        let spool = spool_at("no-pending", vec![tuple(100, 0, 0)]).await;
        resolve_phase(spool, &view, &tx, None, &mut stats)
            .await
            .unwrap();
        assert_eq!(stats.pending, 0);
        assert_eq!(stats.gated, 1);
    }

    #[tokio::test]
    async fn undecidable_multixact_aborts_the_pass() {
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

        let err = resolve_phase(spool, &view, &tx, None, &mut stats)
            .await
            .unwrap_err();
        assert!(err.contains("pg_multixact"), "{err}");
        assert_eq!(stats.emitted, 0);
    }

    /// Nothing deferred: no tail, no CH connection, no work
    #[tokio::test]
    async fn resolve_greenfield_is_a_noop_without_deferrals() {
        let pending = PendingGate {
            deferred: vec![spool_of(Vec::new()).await],
            sink: GreenfieldSink {
                catalog: CatalogMap::new(),
                mapping: Default::default(),
                config: Arc::new(ResolvedConfig::default()),
                emitter: EmitterConfig::default(),
                stats: Arc::new(EmitterStats::default()),
                resolver: ToastResolver::disabled(),
                skip_initial: HashSet::new(),
                scratch_dir: std::env::temp_dir(),
            },
            oracle: None,
            stream_stats: GateStats {
                emitted: 7,
                ..Default::default()
            },
        };
        let (stats, pending_tables) =
            resolve_greenfield(pending, Path::new("/nonexistent"), &PgXactPatch::new())
                .await
                .unwrap();
        assert_eq!(stats.emitted, 7);
        assert!(pending_tables.is_empty());
    }
}
