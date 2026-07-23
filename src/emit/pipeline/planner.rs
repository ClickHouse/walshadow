//! Transaction planner — the side-effect-free planning stage.
//!
//! Consumes committed-drain batches in walk order and streams them into a
//! [`plan_spool`](crate::emit::pipeline::plan_spool) plan: heaps detoast
//! and route at planning so the executor never re-resolves, control
//! entries pin their walk positions, mirror-row refs and truncate fences
//! carry through re-based to plan-global indices. Every input-derived
//! failure (descriptor, decode, toast, route) surfaces before the first
//! transaction side effect.
//!
//! Forbidden side effects are unrepresentable, not merely avoided: the
//! planner holds no ack handle, no batcher channel, no ClickHouse client,
//! and no config applicator. Route state folds into a
//! [`PlanRouteView`] the caller owns; the toast resolver is only read.
//! Any error drops the writer and the plan file unlinks — the transaction
//! emits nothing.
//!
//! Validation coverage, one enforcement point per plan-success guarantee:
//! - descriptor Present and unambiguous → commit-time stash verdicts at
//!   the drain (ambiguous / never-visible generations fail closed)
//! - operation supported with logical tuple data → raw operation policy
//!   ([`FailClosedReason`](crate::xact::xact_buffer::FailClosedReason))
//! - decoded xid matches owning xact/subxact → merge ownership check
//!   (`XactBufferError::ForeignXid`)
//! - partial update reconstructs → [`PlanError::PartialUpdate`] here; no
//!   reconstruction path exists and PG only elides below
//!   `wal_level=logical`, so a routed partial fails the plan
//! - needed toast values resolve deterministically → detoast at planning
//! - mapped output columns have deterministic codecs → plan-time codec
//!   policy layers onto this stage (deterministic-codec phase)
//! - route snapshot complete → `RouteSnapshot::freeze` populates every
//!   field by construction; `None` is the counted unmapped discard, which
//!   skips detoast and codec validation entirely
//! - planned schema transition reproducible → control entries carry
//!   `SchemaEvent` values (old + new descriptors) into replay verbatim

use std::path::PathBuf;
use std::sync::Arc;

use crate::decode::heap_decoder::DescribedHeap;
use crate::emit::pipeline::plan_spool::{
    DEFAULT_PLAN_MEM_MAX, PlanSpoolError, PlanWriter, SealedPlan,
};
use crate::emit::route::RouteSnapshot;
use crate::toast::{ChunkRefMap, ToastResolver};
use crate::xact::xact_buffer::{
    DrainEntry, DrainWalk, DrainedBatch, WalkStep, XactBufferError, detoast_heap,
};

/// Planner's window onto route state. Implementations resolve from frozen
/// versions and fold in-walk control entries into their LOCAL view only —
/// global mapping/config/ClickHouse changes belong to the executor at
/// replay. `route_for` returning `None` is the deterministic unmapped
/// discard, counted by the implementation
pub trait PlanRouteView {
    fn route_for(&mut self, heap: &DescribedHeap) -> Option<Arc<RouteSnapshot>>;
    /// Fold one in-walk control entry into the local view. Async so
    /// implementations can read shared config, never mutate it
    fn apply(
        &mut self,
        entry: &DrainEntry,
    ) -> impl std::future::Future<Output = Result<(), String>> + Send;
}

#[derive(Debug, thiserror::Error)]
pub enum PlanError {
    #[error("plan spool: {0}")]
    Spool(#[from] PlanSpoolError),
    #[error("planning detoast: {0}")]
    Detoast(#[from] XactBufferError),
    /// New-tuple prefix/suffix elision references a predecessor image no
    /// reconstruction path provides; PG only elides below wal_level=logical
    #[error("partial update at {lsn:#X} lacks reconstructable predecessor")]
    PartialUpdate { lsn: u64 },
    #[error("route view: {0}")]
    View(String),
}

impl PlanError {
    /// `reason=` label for `xact_plan_failures_total`
    pub fn reason(&self) -> &'static str {
        match self {
            Self::Spool(_) => "spool",
            Self::Detoast(e) => drain_reason(e),
            Self::PartialUpdate { .. } => "partial_update",
            Self::View(_) => "view",
        }
    }
}

/// `reason=` label for drain-side errors aborting a plan
pub fn drain_reason(e: &XactBufferError) -> &'static str {
    match e {
        XactBufferError::OrdinaryFailClosed { .. } => "fail_closed",
        XactBufferError::Detoast(_) | XactBufferError::ValueTooLarge { .. } => "detoast",
        _ => "drain",
    }
}

/// Streams one committed transaction's walk into a sealed plan
pub struct Planner<'a, V> {
    writer: PlanWriter,
    view: &'a mut V,
    resolver: &'a ToastResolver,
    /// Mirror rows carried by earlier batches; batch-relative row indices
    /// re-base against this
    rows_base: usize,
}

impl<'a, V: PlanRouteView> Planner<'a, V> {
    pub fn create(
        path: PathBuf,
        disk_cap: u64,
        view: &'a mut V,
        resolver: &'a ToastResolver,
    ) -> Result<Self, PlanError> {
        Ok(Self {
            writer: PlanWriter::create(path, disk_cap, DEFAULT_PLAN_MEM_MAX)?,
            view,
            resolver,
            rows_base: 0,
        })
    }

    /// Fold one drained batch. Chunk generations release with the batch —
    /// detoast happens here, the executor never re-resolves values
    pub async fn plan_batch(&mut self, batch: DrainedBatch) -> Result<(), PlanError> {
        let DrainWalk {
            steps,
            chunks,
            new_rows,
            is_final: _,
        } = batch.into_walk();
        let ref_maps: Vec<&ChunkRefMap> = chunks.iter().map(|g| g.map()).collect();
        // One spool per xact; generations sealed before spooling carry None
        let spool = chunks.iter().find_map(|g| g.spool());
        // Batch-relative mirror rows sealed so far
        let mut cursor = 0usize;
        for step in steps {
            match step {
                WalkStep::Rows { upto } => cursor = cursor.max(upto),
                WalkStep::Event(e) => {
                    self.view.apply(&e).await.map_err(PlanError::View)?;
                    self.writer.push_control(e, self.rows_base + cursor);
                }
                WalkStep::Truncate(heap) => {
                    self.writer.note_truncate_cursor(self.rows_base + cursor);
                    let route = self.view.route_for(&heap);
                    self.writer.push_heap(&heap, route.as_ref())?;
                }
                WalkStep::Heap(mut heap) => {
                    // Route before validation and detoast, matching the
                    // decode pool: unmapped rows discard without touching
                    // the resolver or codec checks. The value permit drops
                    // once bytes land in the plan file
                    let route = self.view.route_for(&heap);
                    if route.is_some() {
                        if heap.decoded.new.as_ref().is_some_and(|t| t.partial) {
                            return Err(PlanError::PartialUpdate {
                                lsn: heap.decoded.source_lsn,
                            });
                        }
                        detoast_heap(&mut heap, spool, &ref_maps, self.resolver).await?;
                    }
                    self.writer.push_heap(&heap, route.as_ref())?;
                }
            }
        }
        self.rows_base += new_rows.len();
        self.writer.push_rows(new_rows);
        Ok(())
    }

    /// Freeze the validated plan for the executor
    pub fn seal(self, commit_lsn: u64, commit_ts: i64) -> Result<SealedPlan, PlanError> {
        Ok(self.writer.seal(commit_lsn, commit_ts)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::decode::heap_decoder::{
        ColumnValue, DecodedHeap, DecodedTuple, HeapOp, ToastPointer,
    };
    use crate::emit::pipeline::plan_spool::PlanItem;
    use crate::mapping::{TableMapping, TableTarget};
    use crate::schema::{INT4OID, RelAttr, RelDescriptor, RelName, SchemaEvent, TEXTOID};
    use crate::xact::xact_buffer::raw_fixtures::{
        inject_ordinary, int4_descriptor, multi_insert_raw,
    };
    use crate::xact::xact_buffer::{FailClosedReason, XactBuffer, XactBufferConfig};
    use std::collections::{HashMap, HashSet};
    use walrus::pg::walparser::RelFileNode;

    /// Pins the `reason=` vocabulary `bump_plan_failure` and the metrics
    /// render both key on
    #[test]
    fn plan_error_reason_labels() {
        assert_eq!(
            PlanError::PartialUpdate { lsn: 1 }.reason(),
            "partial_update"
        );
        assert_eq!(PlanError::View("x".into()).reason(), "view");
        let fail_closed = XactBufferError::OrdinaryFailClosed {
            relid: 1,
            lsn: 2,
            reason: FailClosedReason::ImageOnly,
        };
        assert_eq!(drain_reason(&fail_closed), "fail_closed");
        assert_eq!(
            PlanError::Detoast(XactBufferError::Detoast("x".into())).reason(),
            "detoast"
        );
        assert_eq!(
            drain_reason(&XactBufferError::ForeignXid { xid: 1, top: 2 }),
            "drain"
        );
    }

    fn descriptor(rel_node: u32, type_oid: u32) -> Arc<RelDescriptor> {
        Arc::new(RelDescriptor {
            rfn: RelFileNode {
                spc_node: 1663,
                db_node: 5,
                rel_node,
            },
            oid: rel_node,
            toast_oid: 0,
            namespace_oid: 2200,
            rel_name: RelName::new("public", &format!("t{rel_node}")),
            kind: 'r',
            persistence: 'p',
            replident: crate::schema::ReplIdent::Full { pk_attnums: None },
            attributes: vec![RelAttr {
                attnum: 1,
                name: "v".into(),
                type_oid,
                typmod: -1,
                not_null: false,
                dropped: false,
                type_name: if type_oid == INT4OID { "int4" } else { "text" }.into(),
                type_byval: type_oid == INT4OID,
                type_len: if type_oid == INT4OID { 4 } else { -1 },
                type_align: 'i',
                type_storage: if type_oid == INT4OID { 'p' } else { 'x' },
                missing_text: None,
            }],
        })
    }

    fn heap(rel: &Arc<RelDescriptor>, lsn: u64, col: ColumnValue) -> DescribedHeap {
        DescribedHeap {
            decoded: DecodedHeap {
                rfn: rel.rfn,
                xid: 1,
                source_lsn: lsn,
                op: HeapOp::Insert,
                new: Some(DecodedTuple {
                    columns: vec![Some(col)],
                    partial: false,
                }),
                old: None,
            },
            descriptor: rel.clone(),
            descriptor_valid_from: 0x40,
        }
    }

    fn route() -> Arc<RouteSnapshot> {
        RouteSnapshot::freeze(
            Arc::new(TableMapping {
                target: TableTarget::new("db", "t"),
                columns: Vec::new(),
            }),
            Arc::new(HashMap::new()),
            false,
        )
    }

    /// Fixed-map view: routes by rel name until a Dropped event for the oid
    /// folds in, then unmapped. Records applied entries
    struct MapView {
        routes: HashMap<RelName, Arc<RouteSnapshot>>,
        dropped: HashSet<u32>,
        applied: Vec<u32>,
    }

    impl PlanRouteView for MapView {
        fn route_for(&mut self, heap: &DescribedHeap) -> Option<Arc<RouteSnapshot>> {
            if self.dropped.contains(&heap.descriptor.oid) {
                return None;
            }
            self.routes.get(&heap.descriptor.rel_name).cloned()
        }

        async fn apply(&mut self, entry: &DrainEntry) -> Result<(), String> {
            if let DrainEntry::Catalog(SchemaEvent::Dropped { oid, .. }) = entry {
                self.dropped.insert(*oid);
                self.applied.push(*oid);
            }
            Ok(())
        }
    }

    fn cfg(dir: std::path::PathBuf) -> XactBufferConfig {
        XactBufferConfig {
            xact_buffer_max: 1024,
            ..XactBufferConfig::new(dir)
        }
    }

    /// Multi-batch drain plans in walk order: an in-walk Dropped event flips
    /// the local view so later heaps plan unmapped; replay reproduces the
    /// interleave with routes frozen at planning
    #[tokio::test(flavor = "current_thread")]
    async fn plans_drain_in_walk_order_with_view_updates() {
        let tmp = tempfile::tempdir().unwrap();
        let mut b = XactBuffer::new(cfg(tmp.path().to_path_buf())).unwrap();
        let rel = descriptor(16600, INT4OID);
        b.on_heap(heap(&rel, 100, ColumnValue::Int4(1)))
            .await
            .unwrap();
        b.on_heap(heap(&rel, 120, ColumnValue::Int4(2)))
            .await
            .unwrap();
        b.on_schema_event(
            1,
            150,
            SchemaEvent::Dropped {
                oid: 16600,
                rel_name: rel.rel_name.clone(),
            },
        );
        b.on_heap(heap(&rel, 200, ColumnValue::Int4(3)))
            .await
            .unwrap();

        let mut view = MapView {
            routes: HashMap::from([(rel.rel_name.clone(), route())]),
            dropped: HashSet::new(),
            applied: Vec::new(),
        };
        let resolver = ToastResolver::disabled();
        let mut planner =
            Planner::create(tmp.path().join("1.plan"), 1 << 20, &mut view, &resolver).unwrap();
        let mut drain = b.drain_committed(1, 42, 0x2000, &[], false).await.unwrap();
        // 1-row slices force the multi-batch path
        while let Some(batch) = drain.next_batch(1, usize::MAX, None).await.unwrap() {
            let is_final = batch.is_final;
            planner.plan_batch(batch).await.unwrap();
            if is_final {
                break;
            }
        }
        drain.finish().await.unwrap();
        let plan = planner.seal(0x2000, 42).unwrap();
        assert_eq!(view.applied, vec![16600], "event folded into local view");
        assert_eq!((plan.commit_lsn, plan.commit_ts), (0x2000, 42));

        let mut rd = plan.replay().unwrap();
        let mut order = Vec::new();
        while let Some(item) = rd.next_item().unwrap() {
            order.push(match item {
                PlanItem::Control(_) => "e".to_string(),
                PlanItem::Heap(h) => format!(
                    "h{}{}",
                    h.described.decoded.source_lsn,
                    if h.route.is_some() { "r" } else { "-" }
                ),
            });
        }
        assert_eq!(
            order,
            ["h100r", "h120r", "e", "h200-"],
            "post-event heap plans unmapped under the folded view"
        );
    }

    /// Unresolvable toast in the LAST row fails planning after earlier
    /// valid rows planned: nothing seals, so the executor never runs and
    /// the plan file unlinks with the abandoned writer. Store-backed
    /// resolver: disabled mode fills placeholders on miss instead
    #[tokio::test(flavor = "current_thread")]
    async fn detoast_failure_in_last_row_abandons_plan() {
        let tmp = tempfile::tempdir().unwrap();
        let mut b = XactBuffer::new(cfg(tmp.path().to_path_buf())).unwrap();
        let rel = descriptor(16601, TEXTOID);
        b.on_heap(heap(&rel, 90, ColumnValue::Text("ok".into())))
            .await
            .unwrap();
        b.on_heap(heap(
            &rel,
            100,
            ColumnValue::ExternalToast(ToastPointer {
                va_rawsize: 64,
                va_extinfo: 60,
                va_valueid: 9999,
                va_toastrelid: 16602,
            }),
        ))
        .await
        .unwrap();

        let mut view = MapView {
            routes: HashMap::from([(rel.rel_name.clone(), route())]),
            dropped: HashSet::new(),
            applied: Vec::new(),
        };
        let resolver = ToastResolver::with_store(
            Arc::new(crate::toast::MemChunkStore::new()),
            Arc::new(crate::emit::ch_emitter::EmitterStats::default()),
        );
        let path = tmp.path().join("1.plan");
        let mut planner = Planner::create(path.clone(), 1 << 20, &mut view, &resolver).unwrap();
        let mut drain = b.drain_committed(1, 42, 0x2000, &[], false).await.unwrap();
        // 1-row slices: the valid row plans before the toast row fails
        let first = drain
            .next_batch(1, usize::MAX, None)
            .await
            .unwrap()
            .expect("valid row slice");
        planner.plan_batch(first).await.unwrap();
        let second = drain
            .next_batch(1, usize::MAX, None)
            .await
            .unwrap()
            .expect("toast row slice");
        let Err(err) = planner.plan_batch(second).await else {
            panic!("expected detoast failure");
        };
        assert!(matches!(err, PlanError::Detoast(_)), "{err}");
        drain.finish().await.unwrap();
        drop(planner);
        assert!(!path.exists(), "failed plan unlinks");
    }

    /// Failing raw record LAST in the walk: earlier rows already planned,
    /// the drain's fold error surfaces before seal, so no `SealedPlan`
    /// exists for the executor — zero emitter calls for the whole xact
    #[tokio::test(flavor = "current_thread")]
    async fn failing_last_raw_after_planned_rows_abandons_plan() {
        let tmp = tempfile::tempdir().unwrap();
        let mut b = XactBuffer::new(cfg(tmp.path().to_path_buf())).unwrap();
        let rel = int4_descriptor(16610);
        let rfn = rel.rfn;
        b.stash_raw(1, multi_insert_raw(1, 100, 16610, &[1, 2]))
            .await
            .unwrap();
        let mut bad = multi_insert_raw(1, 120, 16610, &[3]);
        bad.main_data[0] = 0; // strip CONTAINS_NEW_TUPLE: ImageOnly on fold
        b.stash_raw(1, bad).await.unwrap();
        inject_ordinary(&mut b, rfn, rel.clone());

        let mut view = MapView {
            routes: HashMap::from([(rel.rel_name.clone(), route())]),
            dropped: HashSet::new(),
            applied: Vec::new(),
        };
        let resolver = ToastResolver::disabled();
        let path = tmp.path().join("1.plan");
        let mut planner = Planner::create(path.clone(), 1 << 20, &mut view, &resolver).unwrap();
        let mut drain = b.drain_committed(1, 42, 0x2000, &[], false).await.unwrap();
        let mut planned = 0usize;
        // 1-row slices: valid fanout rows plan before the bad record folds
        let err = loop {
            match drain.next_batch(1, usize::MAX, None).await {
                Ok(Some(batch)) => {
                    planned += batch.heaps.len();
                    planner.plan_batch(batch).await.unwrap();
                }
                Ok(None) => panic!("expected fold failure before EOF"),
                Err(e) => break e,
            }
        };
        assert!(planned >= 1, "earlier valid rows planned before failure");
        assert!(
            matches!(
                &err,
                XactBufferError::OrdinaryFailClosed {
                    reason: FailClosedReason::ImageOnly,
                    ..
                }
            ),
            "{err}"
        );
        drop(planner);
        assert!(!path.exists(), "abandoned plan unlinks, nothing to execute");
    }

    /// Transaction larger than the buffer memory budget validates through
    /// disk spool end to end: xact rows spill at the 1 KiB budget, plan
    /// bytes cross the mem threshold into file mode, replay yields every
    /// row routed
    #[tokio::test(flavor = "current_thread")]
    async fn oversize_xact_validates_through_disk_spools() {
        let tmp = tempfile::tempdir().unwrap();
        let mut b = XactBuffer::new(cfg(tmp.path().to_path_buf())).unwrap();
        let rel = descriptor(16612, INT4OID);
        const ROWS: usize = 32 * 1024;
        for i in 0..ROWS {
            b.on_heap(heap(&rel, 100 + i as u64, ColumnValue::Int4(i as i32)))
                .await
                .unwrap();
        }

        let mut view = MapView {
            routes: HashMap::from([(rel.rel_name.clone(), route())]),
            dropped: HashSet::new(),
            applied: Vec::new(),
        };
        let resolver = ToastResolver::disabled();
        let mut planner =
            Planner::create(tmp.path().join("1.plan"), 1 << 30, &mut view, &resolver).unwrap();
        let mut drain = b.drain_committed(1, 42, 0x2000, &[], false).await.unwrap();
        while let Some(batch) = drain.next_batch(1024, usize::MAX, None).await.unwrap() {
            let is_final = batch.is_final;
            planner.plan_batch(batch).await.unwrap();
            if is_final {
                break;
            }
        }
        drain.finish().await.unwrap();
        let plan = planner.seal(0x2000, 42).unwrap();
        assert!(
            plan.path().is_some(),
            "plan crossed mem threshold into file spool"
        );
        plan.verify().unwrap();
        let mut rd = plan.replay().unwrap();
        let mut rows = 0usize;
        while let Some(item) = rd.next_item().unwrap() {
            let PlanItem::Heap(h) = item else {
                panic!("no controls planned");
            };
            assert!(h.route.is_some());
            rows += 1;
        }
        drop(rd);
        assert_eq!(rows, ROWS);
    }

    /// Mirror-row refs and fences survive multi-batch planning re-based to
    /// plan-global indices: control `row_idx` and truncate cursors index
    /// the concatenation of carried batches
    #[tokio::test(flavor = "current_thread")]
    async fn mirror_rows_and_fences_rebase_globally() {
        use crate::xact::spill::ToastChunk;
        let tmp = tempfile::tempdir().unwrap();
        let mut b = XactBuffer::new(cfg(tmp.path().to_path_buf())).unwrap();
        let rel = descriptor(16605, INT4OID);
        for (seq, lsn) in [(0u32, 100u64), (1, 102)] {
            b.on_toast_chunk(
                ToastChunk {
                    toast_relid: 16606,
                    value_id: 50,
                    chunk_seq: seq,
                    source_lsn: lsn,
                    blkno: 0,
                    offnum: 1 + seq as u16,
                    chunk_data: bytes::Bytes::from_static(b"abcd"),
                },
                1,
            )
            .await
            .unwrap();
            b.on_heap(heap(&rel, lsn + 1, ColumnValue::Int4(seq as i32)))
                .await
                .unwrap();
        }
        b.on_schema_event(
            1,
            104,
            SchemaEvent::Dropped {
                oid: 16699,
                rel_name: RelName::new("public", "other"),
            },
        );
        let mut truncate = heap(&rel, 105, ColumnValue::Int4(0));
        truncate.decoded.op = HeapOp::Truncate;
        truncate.decoded.new = None;
        b.on_heap(truncate).await.unwrap();

        let mut view = MapView {
            routes: HashMap::from([(rel.rel_name.clone(), route())]),
            dropped: HashSet::new(),
            applied: Vec::new(),
        };
        let resolver = ToastResolver::disabled();
        let mut planner =
            Planner::create(tmp.path().join("1.plan"), 1 << 20, &mut view, &resolver).unwrap();
        let mut drain = b.drain_committed(1, 42, 0x2000, &[], true).await.unwrap();
        while let Some(batch) = drain.next_batch(1, usize::MAX, None).await.unwrap() {
            let is_final = batch.is_final;
            planner.plan_batch(batch).await.unwrap();
            if is_final {
                break;
            }
        }
        drain.finish().await.unwrap();
        let plan = planner.seal(0x2000, 42).unwrap();
        let total_rows: usize = plan.row_batches.iter().map(|rb| rb.len()).sum();
        assert_eq!(total_rows, 2, "both chunk births carried");
        assert_eq!(plan.heap_count, 3, "2 inserts + truncate");
        assert_eq!(plan.controls.len(), 1);
        assert_eq!(
            plan.controls[0].row_idx, 2,
            "event fence re-based to plan-global rows"
        );
        assert_eq!(
            plan.truncate_rows,
            vec![2],
            "truncate fence re-based to plan-global rows"
        );
    }

    /// Routed partial update fails the plan (no reconstruction path); the
    /// same row plans cleanly as an unmapped discard, matching the spec's
    /// "unmapped rows do not require validation for unreferenced columns"
    #[tokio::test(flavor = "current_thread")]
    async fn partial_update_fails_plan_only_when_routed() {
        for (routed, expect_err) in [(true, true), (false, false)] {
            let tmp = tempfile::tempdir().unwrap();
            let mut b = XactBuffer::new(cfg(tmp.path().to_path_buf())).unwrap();
            let rel = descriptor(16607, INT4OID);
            let mut partial = heap(&rel, 100, ColumnValue::Int4(1));
            partial.decoded.op = HeapOp::Update;
            partial.decoded.new.as_mut().unwrap().partial = true;
            b.on_heap(partial).await.unwrap();

            let mut view = MapView {
                routes: if routed {
                    HashMap::from([(rel.rel_name.clone(), route())])
                } else {
                    HashMap::new()
                },
                dropped: HashSet::new(),
                applied: Vec::new(),
            };
            let resolver = ToastResolver::disabled();
            let path = tmp.path().join("1.plan");
            let mut planner = Planner::create(path.clone(), 1 << 20, &mut view, &resolver).unwrap();
            let mut drain = b.drain_committed(1, 42, 0x2000, &[], false).await.unwrap();
            let batch = drain
                .next_batch(8, usize::MAX, None)
                .await
                .unwrap()
                .expect("one slice");
            let res = planner.plan_batch(batch).await;
            drain.finish().await.unwrap();
            if expect_err {
                assert!(
                    matches!(res, Err(PlanError::PartialUpdate { lsn: 100 })),
                    "{res:?}"
                );
                drop(planner);
                assert!(!path.exists(), "failed plan unlinks");
            } else {
                res.unwrap();
                let plan = planner.seal(0x2000, 42).unwrap();
                assert_eq!(plan.heap_count, 1, "unmapped partial plans as discard");
            }
        }
    }

    /// Unmapped heaps skip the resolver entirely: the same unresolvable
    /// pointer plans cleanly when the view discards the relation
    #[tokio::test(flavor = "current_thread")]
    async fn unmapped_heap_skips_detoast() {
        let tmp = tempfile::tempdir().unwrap();
        let mut b = XactBuffer::new(cfg(tmp.path().to_path_buf())).unwrap();
        let rel = descriptor(16603, TEXTOID);
        b.on_heap(heap(
            &rel,
            100,
            ColumnValue::ExternalToast(ToastPointer {
                va_rawsize: 64,
                va_extinfo: 60,
                va_valueid: 9999,
                va_toastrelid: 16604,
            }),
        ))
        .await
        .unwrap();

        let mut view = MapView {
            routes: HashMap::new(),
            dropped: HashSet::new(),
            applied: Vec::new(),
        };
        let resolver = ToastResolver::disabled();
        let mut planner =
            Planner::create(tmp.path().join("1.plan"), 1 << 20, &mut view, &resolver).unwrap();
        let mut drain = b.drain_committed(1, 42, 0x2000, &[], false).await.unwrap();
        while let Some(batch) = drain.next_batch(8, usize::MAX, None).await.unwrap() {
            let is_final = batch.is_final;
            planner.plan_batch(batch).await.unwrap();
            if is_final {
                break;
            }
        }
        drain.finish().await.unwrap();
        let plan = planner.seal(0x2000, 42).unwrap();
        assert_eq!(plan.heap_count, 1);
        let mut rd = plan.replay().unwrap();
        let Some(PlanItem::Heap(h)) = rd.next_item().unwrap() else {
            panic!("expected heap");
        };
        assert!(h.route.is_none(), "unmapped discard planned as such");
    }
}
