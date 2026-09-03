//! Filter backup-page tuples by PostgreSQL visibility
//!
//! [`stream_phase`] resolves hint bits and spools unknowns. [`resolve_phase`]
//! uses complete backup transaction logs plus WAL commit overlay. Greenfield
//! repairs tuples whose visibility remains undecidable

use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use tokio::sync::mpsc;
use tokio_util::task::AbortOnDropHandle;
use walrus::pg::replication::conn::PgConfig;

use crate::backfill::backup_page_walk::{BOOTSTRAP_TUPLE_CHANNEL_CAP, BackfillTuple, CatalogMap};
use crate::backfill::copy_backfill::CopyRate;
use crate::backfill::spool::DeferredSpool;
use crate::backfill::visibility_repair::{RepairBatch, RepairScope, RepairStats, RowRepair};
use crate::config::ResolvedConfig;
use crate::decode::visibility::{
    HEAP_XMAX_IS_MULTI, PgXactPatch, PgXactView, Visibility, read_pg_multixact, read_pg_xact,
    tuple_visibility,
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
    pub multixact_emitted: u64,
    /// Chunk tuples the hint bits proved dead, dropped before the store
    pub chunks_gated: u64,
    /// Tuples handed to source visibility repair
    pub unresolved: u64,
    /// Rows emitted by visibility repair
    pub repaired_rows: u64,
    /// Source write head after final repair read
    pub p_hi: u64,
}

pub enum GateOutput<'a> {
    Rows(&'a mpsc::Sender<Vec<BackfillTuple>>),
    Repair(&'a mpsc::Sender<RepairBatch>),
}

impl GateOutput<'_> {
    async fn send(&self, rows: Vec<BackfillTuple>, unresolved: bool) -> bool {
        match self {
            Self::Rows(tx) => tx.send(rows).await.is_ok(),
            Self::Repair(tx) => tx.send(RepairBatch { rows, unresolved }).await.is_ok(),
        }
    }
}

/// Resolve hint bits, spool unknown main tuples, pass chunks unless proven dead
///
/// Slab in, slab out: the walk's batching survives the gate, which only drops
/// what the verdict removes
pub async fn stream_phase(
    rx: &mut mpsc::Receiver<Vec<BackfillTuple>>,
    tx: &GateOutput<'_>,
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
        if !pass.is_empty() && !tx.send(pass, false).await {
            break;
        }
    }
    stats.deferred = deferred.records();
    Ok(())
}

/// Replay spool against complete transaction view
pub async fn resolve_phase(
    deferred: DeferredSpool,
    view: &PgXactView<'_>,
    tx: &GateOutput<'_>,
    stats: &mut GateStats,
) -> Result<(), String> {
    let mut out = SlabTx::new(tx, false);
    let mut repair = SlabTx::new(tx, true);
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
            Visibility::Skip | Visibility::Defer => stats.gated += 1,
            Visibility::Unresolvable => match tx {
                GateOutput::Rows(_) => {
                    fail = Some(undecidable_multixact(&t));
                    break;
                }
                GateOutput::Repair(_) => {
                    stats.unresolved += 1;
                    if !repair.push(t).await {
                        break;
                    }
                }
            },
        }
    }
    out.flush().await;
    repair.flush().await;
    replay
        .finish()
        .await
        .map_err(|e| format!("visibility gate: deferred spool cleanup: {e}"))?;
    match fail {
        Some(e) => Err(e),
        None => Ok(()),
    }
}

/// Regroup a per-tuple source into walk-sized slabs, so the spool replay and
/// repair reads cost the drain one channel hop per batch, not per row
struct SlabTx<'a> {
    tx: &'a GateOutput<'a>,
    unresolved: bool,
    buf: Vec<BackfillTuple>,
    closed: bool,
}

impl<'a> SlabTx<'a> {
    fn new(tx: &'a GateOutput<'a>, unresolved: bool) -> Self {
        Self {
            tx,
            unresolved,
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
        self.closed = !self.tx.send(slab, self.unresolved).await;
        !self.closed
    }
}

/// Rows per slab out of the spool replay and repair reads. Row width is
/// unbounded here, so the drain's own budget is the resident ceiling
const GATE_SLAB_ROWS: usize = 256;

fn undecidable_multixact(t: &BackfillTuple) -> String {
    format!(
        "visibility gate: multixact xmax {} (rfn {}/{}) unresolvable from the backup's \
         pg_multixact snapshot; remedy: fresher backup, or initial_load='copy'",
        t.xmax, t.rfn.db_node, t.rfn.rel_node
    )
}

/// Destination both greenfield legs write: page walk, then deferred
/// resolution. One frozen mapping serves the repair scope and the drain's
/// routes, so a route can never want a value the repair never read
pub struct GreenfieldSink {
    /// Source endpoint for repair reads
    pub source: PgConfig,
    pub catalog: CatalogMap,
    pub mapping: MappingSnapshot,
    pub repair_scope: RepairScope,
    pub copy_rate: CopyRate,
    pub config: Arc<ResolvedConfig>,
    pub emitter: EmitterConfig,
    pub stats: Arc<EmitterStats>,
    pub resolver: ToastResolver,
    /// Relations excluded from initial load
    pub skip_initial: HashSet<RelName>,
}

/// Spawned repair and drain stages of one leg
pub struct RepairDrain {
    repair: AbortOnDropHandle<Result<RepairStats>>,
    drain: AbortOnDropHandle<Result<BootstrapDrainOutcome, String>>,
}

impl GreenfieldSink {
    /// Spawn source repair feeding the bootstrap drain. Gate stage owns the
    /// returned sender; dropping it winds both stages down
    pub fn spawn(
        &self,
        msg_tx: mpsc::Sender<BatcherMsg>,
        ack: AckHandle,
    ) -> (mpsc::Sender<RepairBatch>, RepairDrain) {
        let (tx, rx) = mpsc::channel(BOOTSTRAP_TUPLE_CHANNEL_CAP);
        let (repaired_tx, repaired_rx) = mpsc::channel(BOOTSTRAP_TUPLE_CHANNEL_CAP);
        let repair = AbortOnDropHandle::new(tokio::spawn(
            RowRepair {
                source: self.source.clone(),
                catalog: self.catalog.clone(),
                scope: self.repair_scope.clone(),
                rate: self.copy_rate.clone(),
            }
            .run(rx, repaired_tx),
        ));
        let drain = AbortOnDropHandle::new(tokio::spawn(bootstrap::drain(
            repaired_rx,
            self.catalog.clone(),
            self.mapping.clone(),
            msg_tx,
            ack,
            self.stats.clone(),
            self.resolver.clone(),
            // Repair strips mapped external pointers, so nothing defers
            None,
            self.emitter.row_policy(),
            Some(self.config.clone()),
            self.skip_initial.clone(),
        )));
        (tx, RepairDrain { repair, drain })
    }
}

impl RepairDrain {
    /// Join both stages before surfacing either error, so a failed repair
    /// still lets the drain finish what it holds
    pub async fn join(self) -> Result<(RepairStats, BootstrapDrainOutcome), String> {
        let repaired = join_stage(self.repair, "row repair").await;
        let drained = join_stage(self.drain, "drain").await;
        Ok((repaired?, drained?))
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
    pub deferred: DeferredSpool,
    pub sink: GreenfieldSink,
    pub oracle: Option<Arc<crate::ops::oracle::Oracle>>,
    /// Streaming-phase counters
    pub stream_stats: GateStats,
}

/// Resolve deferred tuples and repair unresolved visibility
pub async fn resolve_greenfield(
    gate: PendingGate,
    data_dir: &Path,
    patch: &PgXactPatch,
) -> Result<GateStats> {
    let PendingGate {
        deferred,
        sink,
        oracle,
        stream_stats: mut gate_stats,
    } = gate;
    if deferred.records() == 0 {
        return Ok(gate_stats);
    }
    let accum = read_pg_xact(data_dir).await?;
    let multi = read_pg_multixact(data_dir).await?;
    let view = PgXactView::new(&accum, patch).with_multixact(&multi);

    let tail = OwnedTail::spawn(
        &sink.emitter,
        sink.emitter.inserter_pool_size,
        sink.stats.clone(),
        Fatal::new(),
        None,
        oracle,
        "visibility gate",
    )
    .await
    .map_err(anyhow::Error::msg)?;

    let (tx, stages) = sink.spawn(tail.msg_tx.clone(), tail.ack.clone());
    let resolved = resolve_phase(deferred, &view, &GateOutput::Repair(&tx), &mut gate_stats).await;
    drop(tx);
    // Drain tail before surfacing errors
    let next_seq = match (resolved, stages.join().await) {
        (Ok(()), Ok((repaired, drained))) => {
            gate_stats.repaired_rows += repaired.rows;
            gate_stats.p_hi = gate_stats.p_hi.max(repaired.p_hi);
            drained.next_seq
        }
        (Err(e), _) | (_, Err(e)) => {
            tail.quiesce().await;
            anyhow::bail!(e);
        }
    };
    tail.finish(next_seq).await.map_err(anyhow::Error::msg)?;
    Ok(gate_stats)
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
        stream_phase(
            &mut walk_rx,
            &GateOutput::Rows(&tx),
            catalog,
            &mut spool,
            &mut stats,
        )
        .await
        .unwrap();
        drop(tx);

        let mut passed = Vec::new();
        while let Some(slab) = rx.recv().await {
            passed.extend(slab);
        }
        (stats, passed)
    }

    #[tokio::test]
    async fn mapped_external_values_stream_without_marking_relations() {
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
            let mapping = [(
                desc.rel_name.clone(),
                TableMapping {
                    target: TableTarget::new("default", "t"),
                    columns,
                },
            )]
            .into_iter()
            .collect::<ahash::HashMap<_, _>>()
            .into();
            let scope = RepairScope::mapped(&catalog, &mapping, |_| true);
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
            assert!(!scope.has_external(&passed[0]));
            assert_eq!(scope.has_external(&passed[1]), body_mapped);
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

    #[tokio::test]
    async fn greenfield_repairs_only_undecidable_tuples() {
        let accum = PgXactAccum::new();
        let patch = PgXactPatch::new();
        let multi = PgMultiXactAccum::new();
        let view = PgXactView::new(&accum, &patch).with_multixact(&multi);
        let (tx, mut rx) = mpsc::channel(4);
        let mut stats = GateStats::default();
        let spool = spool_of(vec![
            tuple(100, 0, HEAP_XMIN_COMMITTED),
            tuple(100, 10, HEAP_XMIN_COMMITTED | HEAP_XMAX_IS_MULTI),
            tuple(100, 0, HEAP_XMIN_COMMITTED),
        ])
        .await;
        resolve_phase(spool, &view, &GateOutput::Repair(&tx), &mut stats)
            .await
            .unwrap();
        drop(tx);
        let visible = rx.recv().await.expect("visible rows");
        assert!(!visible.unresolved);
        assert_eq!(visible.rows.len(), 2);
        let unresolved = rx.recv().await.expect("unresolved row");
        assert!(unresolved.unresolved);
        assert_eq!(unresolved.rows.len(), 1);
        assert_eq!(unresolved.rows[0].xmax, 10);
        assert!(rx.recv().await.is_none());
        assert_eq!(stats.emitted, 2);
        assert_eq!(stats.unresolved, 1);
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

        let err = resolve_phase(spool, &view, &GateOutput::Rows(&tx), &mut stats)
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
            sink: GreenfieldSink {
                source: crate::config::SourceConn::default().to_pg_config(),
                catalog: CatalogMap::new(),
                mapping: Default::default(),
                repair_scope: RepairScope::default(),
                copy_rate: CopyRate::new(None),
                config: Arc::new(ResolvedConfig::default()),
                emitter: EmitterConfig::default(),
                stats: Arc::new(EmitterStats::default()),
                resolver: ToastResolver::disabled(),
                skip_initial: HashSet::new(),
            },
            oracle: None,
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
