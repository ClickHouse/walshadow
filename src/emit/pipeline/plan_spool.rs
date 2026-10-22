//! Plan one committed transaction before executing it.
//!
//! Keep heap entries in memory until their size exceeds memory limit, then
//! write them to a spool file. Keep table metadata and control entries in
//! memory, assuming control state stays small as in live transaction buffer.
//! Count and discard heaps without mappings, but keep their truncates.
//! Replay heaps and controls in original order, with controls before heaps
//! at matching positions. On planning failure, delete spool file and emit
//! nothing. Store files in transaction scratch directory, cleared at startup;
//! recover plans from source WAL. Read errors during replay require recovery
//! from source WAL, as with crashes during execution, so frames omit checksums.
//!
//! Frame layout after 4-byte `magic + version` header:
//! `[len:u32][table_id:u32][heap payload]`. Encode heap payload with spill
//! codec, omitting relation identity. Read `heap_count` frames from file.

use std::collections::hash_map::Entry;
use std::fs::{self, File};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use thiserror::Error;

use crate::decode::heap_decoder::{ColumnValue, DecodedHeap, DescribedHeap, HeapOp};
use crate::emit::route::RouteSnapshot;
use crate::schema::RelDescriptor;
use crate::xact::spill::{self, Cursor, IO_CHUNK, SpillError};
use crate::xact::xact_buffer::{DrainEntry, OrderedEvent, ToastRowBatch};
use ahash::HashMap;

/// ASCII for `xxd`-friendly debug
pub const PLAN_MAGIC: [u8; 2] = *b"WP";
pub const PLAN_VERSION: u16 = 2;

/// Plans at or below this stay memory-resident: the common
/// single-statement commit never touches the filesystem or the codec.
/// Larger plans spill to the file at `path` (bounded by the disk cap)
pub const DEFAULT_PLAN_MEM_MAX: u64 = 1 << 20;

/// Resident cost charged per typed heap on top of its values, standing in
/// for the frame a spilled heap would take
const MEM_FRAME_OVERHEAD: u64 = 64;

/// Descriptor version and the route its heaps planned under. Heaps index
/// these, so a run of one relation's heaps shares one set of refcounts
pub struct PlanTable {
    pub descriptor: Arc<RelDescriptor>,
    pub valid_from: u64,
    /// Count and discard heaps when `None`; still replay truncates
    pub route: Option<Arc<RouteSnapshot>>,
    /// Heap count for this table, including discarded heaps
    pub heaps: u64,
}

impl PlanTable {
    fn is(&self, heap: &DescribedHeap) -> bool {
        self.key() == Self::key_of(heap)
    }

    fn key(&self) -> (usize, u64) {
        (Arc::as_ptr(&self.descriptor) as usize, self.valid_from)
    }

    fn key_of(heap: &DescribedHeap) -> (usize, u64) {
        (
            Arc::as_ptr(&heap.descriptor) as usize,
            heap.descriptor_valid_from,
        )
    }
}

/// Validated heap under its plan's table
#[derive(Debug)]
pub struct PlannedHeap {
    pub decoded: DecodedHeap,
    /// Index into plan's `tables`
    pub table: u32,
    /// [`DecodedHeap::approx_bytes`] measured once, as every stage
    /// downstream budgets by it
    pub bytes: usize,
}

impl PlannedHeap {
    fn new(decoded: DecodedHeap, table: u32) -> Self {
        let bytes = decoded.approx_bytes();
        Self {
            decoded,
            table,
            bytes,
        }
    }
}

/// Resident cost of a typed heap: values count payload only, so add each
/// column slot, else narrow wide rows overrun `mem_max` several times
fn mem_cost(heap: &PlannedHeap) -> u64 {
    let d = &heap.decoded;
    let slots = [&d.new, &d.old]
        .into_iter()
        .flatten()
        .map(|t| t.columns.len())
        .sum::<usize>();
    MEM_FRAME_OVERHEAD + (heap.bytes + slots * std::mem::size_of::<Option<ColumnValue>>()) as u64
}

#[derive(Debug, Error)]
pub enum PlanSpoolError {
    #[error("plan spool io: {0}")]
    Io(#[from] std::io::Error),
    #[error("plan spool format: {detail}")]
    Format { detail: String },
    #[error("plan spool {needed} bytes exceeds disk budget {cap}")]
    DiskBudget { needed: u64, cap: u64 },
}

impl From<SpillError> for PlanSpoolError {
    fn from(e: SpillError) -> Self {
        match e {
            SpillError::Io(e) => Self::Io(e),
            e => Self::Format {
                detail: e.to_string(),
            },
        }
    }
}

pub type Result<T> = std::result::Result<T, PlanSpoolError>;

/// Spool file location, formatted only once a plan spills
pub enum PlanPath {
    At(PathBuf),
    Xact {
        dir: Arc<Path>,
        xid: u32,
        commit_lsn: u64,
    },
}

impl PlanPath {
    fn resolve(&self) -> PathBuf {
        match self {
            Self::At(p) => p.clone(),
            Self::Xact {
                dir,
                xid,
                commit_lsn,
            } => dir.join(format!("xact-{xid}-{commit_lsn}.plan")),
        }
    }
}

impl From<PathBuf> for PlanPath {
    fn from(p: PathBuf) -> Self {
        Self::At(p)
    }
}

/// Delete spool file on drop; plans replay once
struct PlanFile(PathBuf);

impl Drop for PlanFile {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.0);
    }
}

/// Streams one transaction's validated heaps, typed in memory or to disk,
/// while accumulating the bounded plan header. Dropped unsealed (any
/// planning error), the file unlinks and nothing replays
enum PlanSink {
    Mem(Vec<PlannedHeap>),
    File(BufWriter<File>, PlanFile),
}

pub struct PlanWriter {
    sink: PlanSink,
    path: PlanPath,
    mem_max: u64,
    cap: u64,
    bytes: u64,
    /// Number of heaps to replay, including truncates
    heap_count: u64,
    tables: Vec<PlanTable>,
    /// Tables since the last control entry, which may change routes
    live_from: usize,
    /// Table of the previous heap, checked before `table_ids`
    last_table: u32,
    /// Live table ids by `(descriptor Arc identity, valid_from)`; the held
    /// Arc in `tables` keeps each pointer live and unique. Built once a
    /// second live table appears, so single-relation plans never hash
    table_ids: Option<HashMap<(usize, u64), u32>>,
    controls: Vec<OrderedEvent>,
    row_batches: Vec<ToastRowBatch>,
    truncate_rows: Vec<usize>,
    frame: Vec<u8>,
}

impl PlanWriter {
    /// Starts memory-resident; the file at `path` is only created if the
    /// plan outgrows `mem_max`
    pub fn create(path: impl Into<PlanPath>, cap: u64, mem_max: u64) -> Self {
        Self {
            sink: PlanSink::Mem(Vec::new()),
            path: path.into(),
            mem_max,
            cap,
            bytes: 4,
            heap_count: 0,
            tables: Vec::new(),
            live_from: 0,
            last_table: 0,
            table_ids: None,
            controls: Vec::new(),
            row_batches: Vec::new(),
            truncate_rows: Vec::new(),
            frame: Vec::new(),
        }
    }

    /// Disk bytes written so far, header included
    pub fn bytes(&self) -> u64 {
        self.bytes
    }

    /// Table `heap` plans under. `route` resolves only for a descriptor
    /// version without a table since the last control entry
    pub fn table(
        &mut self,
        heap: &DescribedHeap,
        route: impl FnOnce() -> Option<Arc<RouteSnapshot>>,
    ) -> u32 {
        let live = self.live_from..self.tables.len();
        if live.contains(&(self.last_table as usize))
            && self.tables[self.last_table as usize].is(heap)
        {
            return self.last_table;
        }
        let next_id = self.tables.len() as u32;
        if !live.is_empty() {
            let ids = self
                .table_ids
                .get_or_insert_with(|| live.map(|id| (self.tables[id].key(), id as u32)).collect());
            match ids.entry(PlanTable::key_of(heap)) {
                Entry::Occupied(e) => {
                    self.last_table = *e.get();
                    return self.last_table;
                }
                Entry::Vacant(e) => {
                    e.insert(next_id);
                }
            }
        }
        self.tables.push(PlanTable {
            descriptor: heap.descriptor.clone(),
            valid_from: heap.descriptor_valid_from,
            route: route(),
            heaps: 0,
        });
        self.last_table = next_id;
        next_id
    }

    pub fn routed(&self, table: u32) -> bool {
        self.tables[table as usize].route.is_some()
    }

    /// Append validated heap with a [`Self::table`] id; count and discard
    /// it if table has no mapping
    pub fn push_heap(&mut self, decoded: DecodedHeap, table: u32) -> Result<()> {
        if self.routed(table) {
            self.store(decoded, table)?;
        }
        self.tables[table as usize].heaps += 1;
        Ok(())
    }

    /// Append `HeapOp::Truncate`; executor stores mirror rows with indices
    /// below `upto`, then applies truncate
    pub fn push_truncate(&mut self, decoded: DecodedHeap, table: u32, upto: usize) -> Result<()> {
        self.store(decoded, table)?;
        self.tables[table as usize].heaps += 1;
        self.truncate_rows.push(upto);
        Ok(())
    }

    fn store(&mut self, decoded: DecodedHeap, table: u32) -> Result<()> {
        let heap = PlannedHeap::new(decoded, table);
        self.heap_count += 1;
        // Resident estimate overstates encoded size, so enforce disk cap
        // only against encoded frames
        if let PlanSink::Mem(heaps) = &mut self.sink {
            let cost = mem_cost(&heap);
            if self.bytes + cost <= self.mem_max.min(self.cap) {
                self.bytes += cost;
                heaps.push(heap);
                return Ok(());
            }
            self.spill()?;
        }
        self.encode_heap(&heap)
    }

    /// Move resident heaps into a fresh spool file; later heaps append there
    fn spill(&mut self) -> Result<()> {
        let path = self.path.resolve();
        let mut out = BufWriter::with_capacity(IO_CHUNK, File::create(&path)?);
        out.write_all(&PLAN_MAGIC)?;
        out.write_all(&PLAN_VERSION.to_le_bytes())?;
        let sink = PlanSink::File(out, PlanFile(path));
        let PlanSink::Mem(heaps) = std::mem::replace(&mut self.sink, sink) else {
            unreachable!("spill runs once, from memory");
        };
        self.bytes = 4;
        for h in &heaps {
            self.encode_heap(h)?;
        }
        Ok(())
    }

    fn encode_heap(&mut self, heap: &PlannedHeap) -> Result<()> {
        let frame = &mut self.frame;
        frame.clear();
        frame.extend_from_slice(&[0; 4]);
        spill::push_u32(frame, heap.table);
        spill::encode_heap_into(frame, &heap.decoded);
        let needed = self.bytes + frame.len() as u64;
        if needed > self.cap {
            return Err(PlanSpoolError::DiskBudget {
                needed,
                cap: self.cap,
            });
        }
        let len = (frame.len() - 4) as u32;
        frame[..4].copy_from_slice(&len.to_le_bytes());
        let PlanSink::File(out, _) = &mut self.sink else {
            unreachable!("frames require a spool file");
        };
        out.write_all(frame)?;
        self.bytes = needed;
        Ok(())
    }

    /// Pin a control entry before the next heap: replay yields it ahead of
    /// heap `heap_count`, preserving the drain's event-before-heap tie break.
    /// `row_idx` is the plan-global mirror-row position it fences
    pub fn push_control(&mut self, event: DrainEntry, row_idx: usize) {
        self.live_from = self.tables.len();
        self.table_ids = None;
        self.controls.push(OrderedEvent {
            heap_idx: self.heap_count as usize,
            row_idx,
            event,
        });
    }

    /// Carry one batch's mirror-row refs; already gauged, released when the
    /// plan drops. Control `row_idx` / truncate fences index the
    /// concatenation of pushed batches
    pub fn push_rows(&mut self, rows: ToastRowBatch) {
        if !rows.is_empty() {
            self.row_batches.push(rows);
        }
    }

    /// Flush spool file and finalize plan metadata
    pub fn seal(self, commit_lsn: u64, commit_ts: i64) -> Result<SealedPlan> {
        let store = match self.sink {
            PlanSink::Mem(heaps) => PlanStore::Mem(heaps),
            PlanSink::File(mut out, file) => {
                out.flush()?;
                PlanStore::File(file)
            }
        };
        Ok(SealedPlan {
            store,
            tables: self.tables,
            controls: self.controls,
            row_batches: self.row_batches,
            truncate_rows: self.truncate_rows,
            heap_count: self.heap_count,
            size_bytes: self.bytes,
            commit_lsn,
            commit_ts,
        })
    }
}

/// Store sealed plan heaps in memory or a spool file
enum PlanStore {
    Mem(Vec<PlannedHeap>),
    File(PlanFile),
}

/// Validated plan, consumed by [`Self::replay`]
pub struct SealedPlan {
    store: PlanStore,
    /// Tables in first-use order, indexed by [`PlannedHeap::table`]
    pub tables: Vec<PlanTable>,
    /// Plan-global positions: entry at `heap_idx` replays before that heap
    pub controls: Vec<OrderedEvent>,
    /// Mirror-row refs in plan order, still gauged; `controls[..].row_idx`
    /// and `truncate_rows` index the concatenation
    pub row_batches: Vec<ToastRowBatch>,
    /// Plan-global mirror-row fence per `HeapOp::Truncate` heap, heap order
    pub truncate_rows: Vec<usize>,
    /// Number of heaps to replay, including truncates
    pub heap_count: u64,
    /// File size including header, or estimated memory use for plans in
    /// memory; reported as `xact_plan_bytes`
    pub size_bytes: u64,
    pub commit_lsn: u64,
    pub commit_ts: i64,
}

impl SealedPlan {
    /// Spool file path; `None` while memory-resident
    pub fn path(&self) -> Option<&Path> {
        match &self.store {
            PlanStore::Mem(_) => None,
            PlanStore::File(f) => Some(&f.0),
        }
    }

    /// Count heaps with routes for `xact_plan_rows`
    pub fn routed_count(&self) -> u64 {
        self.tables
            .iter()
            .filter(|t| t.route.is_some())
            .map(|t| t.heaps)
            .sum()
    }

    pub fn replay(self) -> Result<PlanReader> {
        let input = match self.store {
            PlanStore::Mem(heaps) => PlanInput::Mem(heaps.into_iter()),
            PlanStore::File(file) => {
                let mut input = BufReader::with_capacity(IO_CHUNK, File::open(&file.0)?);
                let mut header = [0u8; 4];
                input.read_exact(&mut header)?;
                let [m0, m1, v0, v1] = header;
                if [m0, m1] != PLAN_MAGIC {
                    return Err(PlanSpoolError::Format {
                        detail: "bad plan magic".into(),
                    });
                }
                let version = u16::from_le_bytes([v0, v1]);
                if version != PLAN_VERSION {
                    return Err(PlanSpoolError::Format {
                        detail: format!(
                            "unsupported plan version {version}, expected {PLAN_VERSION}"
                        ),
                    });
                }
                PlanInput::File {
                    input,
                    frame: Vec::new(),
                    heaps: self.heap_count,
                    bytes: self.size_bytes - 4,
                    _file: file,
                }
            }
        };
        Ok(PlanReader {
            tables: self.tables,
            row_batches: self.row_batches,
            controls: self.controls.into_iter().peekable(),
            truncate_rows: self.truncate_rows.into_iter(),
            input,
            next_heap: 0,
        })
    }
}

/// One replayed plan step, original drain order
pub enum PlanItem {
    Control(OrderedEvent),
    Heap(PlannedHeap),
    /// Mirror rows below `upto` put before the truncate applies
    Truncate {
        heap: PlannedHeap,
        upto: usize,
    },
}

enum PlanInput {
    Mem(std::vec::IntoIter<PlannedHeap>),
    File {
        input: BufReader<File>,
        frame: Vec<u8>,
        /// Frames and bytes left before the sealed end
        heaps: u64,
        bytes: u64,
        _file: PlanFile,
    },
}

/// Replay [`SealedPlan`] in order, resolving table ids from plan metadata
/// and inserting controls at recorded positions
pub struct PlanReader {
    /// [`SealedPlan::tables`]
    pub tables: Vec<PlanTable>,
    /// [`SealedPlan::row_batches`]
    pub row_batches: Vec<ToastRowBatch>,
    controls: std::iter::Peekable<std::vec::IntoIter<OrderedEvent>>,
    truncate_rows: std::vec::IntoIter<usize>,
    input: PlanInput,
    next_heap: u64,
}

impl PlanReader {
    pub fn next_item(&mut self) -> Result<Option<PlanItem>> {
        let next_heap = self.next_heap;
        if let Some(c) = self.controls.next_if(|c| c.heap_idx as u64 <= next_heap) {
            return Ok(Some(PlanItem::Control(c)));
        }
        let heap = match &mut self.input {
            PlanInput::Mem(heaps) => heaps.next(),
            PlanInput::File { heaps: 0, .. } => None,
            PlanInput::File {
                input,
                frame,
                heaps,
                bytes,
                ..
            } => {
                *heaps -= 1;
                Some(read_frame(input, frame, bytes, &self.tables)?)
            }
        };
        let Some(heap) = heap else {
            return Ok(None);
        };
        self.next_heap += 1;
        if heap.decoded.op != HeapOp::Truncate {
            return Ok(Some(PlanItem::Heap(heap)));
        }
        let Some(upto) = self.truncate_rows.next() else {
            return Err(PlanSpoolError::Format {
                detail: format!(
                    "truncate heap {} has no mirror-row fence",
                    self.next_heap - 1
                ),
            });
        };
        Ok(Some(PlanItem::Truncate { heap, upto }))
    }
}

/// Read next `[len][table_id][heap payload]` frame
fn read_frame(
    input: &mut impl Read,
    frame: &mut Vec<u8>,
    bytes: &mut u64,
    tables: &[PlanTable],
) -> Result<PlannedHeap> {
    let mut len = [0u8; 4];
    input.read_exact(&mut len)?;
    let len = u32::from_le_bytes(len);
    // Bound against sealed size before allocating, so a garbage length
    // reads as format error instead of a multi-gigabyte buffer
    if 4 + u64::from(len) > *bytes {
        return Err(PlanSpoolError::Format {
            detail: format!("frame claims {len} bytes, {} left in plan", *bytes - 4),
        });
    }
    *bytes -= 4 + u64::from(len);
    frame.resize(len as usize, 0);
    input.read_exact(frame)?;
    let mut c = Cursor::new(frame);
    let table = c.u32()?;
    let Some(t) = tables.get(table as usize) else {
        return Err(PlanSpoolError::Format {
            detail: format!(
                "heap references table id {table}, header has {}",
                tables.len()
            ),
        });
    };
    let decoded = spill::decode_heap(&mut c, t.descriptor.rfn)?;
    Ok(PlannedHeap::new(decoded, table))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::decode::heap_decoder::{DecodedHeap, DecodedTuple, HeapOp};
    use crate::mapping::{TableMapping, TableTarget};
    use crate::schema::{INT4OID, RelAttr, RelDescriptor, RelName, SchemaEvent};
    use walrus::pg::walparser::RelFileNode;

    fn descriptor(rel_node: u32) -> Arc<RelDescriptor> {
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
            ancestors: Vec::new(),
            replident: crate::schema::ReplIdent::Full { pk_attnums: None },
            attributes: vec![RelAttr {
                attnum: 1,
                name: "id".into(),
                type_oid: INT4OID,
                typmod: -1,
                not_null: false,
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

    fn heap(rel: &Arc<RelDescriptor>, lsn: u64, v: i32) -> DescribedHeap {
        DescribedHeap {
            decoded: DecodedHeap {
                rfn: rel.rfn,
                xid: 7,
                source_lsn: lsn,
                op: HeapOp::Insert,
                new: Some(DecodedTuple {
                    columns: vec![Some(ColumnValue::Int4(v))],
                    partial: false,
                }),
                old: None,
            },
            descriptor: rel.clone(),
            descriptor_valid_from: 0x40,
        }
    }

    /// Push `heap` under the table it plans to, `route` deciding a new one
    fn push(
        w: &mut PlanWriter,
        heap: DescribedHeap,
        route: Option<Arc<RouteSnapshot>>,
    ) -> Result<()> {
        let table = w.table(&heap, || route);
        w.push_heap(heap.decoded, table)
    }

    fn route() -> Arc<RouteSnapshot> {
        RouteSnapshot::freeze(
            Arc::new(TableMapping {
                target: TableTarget::new("db", "t"),
                columns: Vec::new(),
                tees: Vec::new(),
            }),
            Arc::default(),
            Default::default(),
        )
    }

    fn dropped(oid: u32) -> DrainEntry {
        DrainEntry::Catalog(SchemaEvent::Dropped {
            oid,
            rel_name: RelName::new("public", &format!("t{oid}")),
            partition: None,
        })
    }

    /// Preserve write order, placing controls before their heap or after
    /// all heaps. Reuse one table per descriptor between controls. Count
    /// and discard unmapped heaps, but replay unmapped truncates
    #[test]
    fn round_trip_orders_heaps_and_controls() {
        for mem_max in [DEFAULT_PLAN_MEM_MAX, 0] {
            let tmp = tempfile::tempdir().unwrap();
            let path = tmp.path().join("1.plan");
            let mut w = PlanWriter::create(path.clone(), 1 << 20, mem_max);
            let (d1, d2) = (descriptor(16500), descriptor(16501));
            let r = route();
            w.push_control(dropped(1), 0);
            push(&mut w, heap(&d1, 100, 10), Some(r.clone())).unwrap();
            push(&mut w, heap(&d2, 110, 20), None).unwrap();
            let mut truncate = heap(&d2, 115, 25);
            truncate.decoded.op = HeapOp::Truncate;
            let table = w.table(&truncate, || unreachable!("table live"));
            w.push_truncate(truncate.decoded, table, 0).unwrap();
            w.push_control(dropped(2), 0);
            push(&mut w, heap(&d1, 120, 30), Some(r.clone())).unwrap();
            w.push_control(dropped(3), 0);
            let plan = w.seal(0x2000, 42).unwrap();
            let tables: Vec<_> = plan
                .tables
                .iter()
                .map(|t| (t.descriptor.oid, t.route.is_some(), t.heaps))
                .collect();
            assert_eq!(
                tables,
                [(16500, true, 1), (16501, false, 2), (16500, true, 1)],
                "control re-resolves routes"
            );
            assert_eq!(plan.heap_count, 3, "unmapped insert discarded");
            assert_eq!(plan.routed_count(), 2);
            assert!(plan.size_bytes > 0);
            assert_eq!((plan.commit_lsn, plan.commit_ts), (0x2000, 42));
            assert_eq!(plan.path().is_some(), mem_max == 0);

            let mut rd = plan.replay().unwrap();
            let mut order = Vec::new();
            while let Some(item) = rd.next_item().unwrap() {
                order.push(match item {
                    PlanItem::Control(c) => format!("{:?}", c.event),
                    PlanItem::Heap(h) | PlanItem::Truncate { heap: h, .. } => {
                        let t = &rd.tables[h.table as usize];
                        assert!(
                            Arc::ptr_eq(&t.descriptor, &d1) || Arc::ptr_eq(&t.descriptor, &d2),
                            "descriptor identity survives the plan"
                        );
                        if let Some(route) = &t.route {
                            assert!(Arc::ptr_eq(route, &r));
                        }
                        assert_eq!(h.decoded.rfn, t.descriptor.rfn);
                        let new = h.decoded.new.as_ref().unwrap();
                        let routed = if t.route.is_some() { "r" } else { "-" };
                        format!("{:?}{routed}", new.columns[0])
                    }
                });
            }
            let ev = |oid| format!("{:?}", dropped(oid));
            let heap = |v, routed| format!("{:?}{routed}", Some(ColumnValue::Int4(v)));
            assert_eq!(
                order,
                [
                    ev(1),
                    heap(10, "r"),
                    heap(25, "-"),
                    ev(2),
                    heap(30, "r"),
                    ev(3)
                ]
            );
            drop(rd);
            assert!(!path.exists(), "spool file removed after replay");
        }
    }

    #[test]
    fn compact_heaps_preserve_subxids_and_images() {
        for mem_max in [DEFAULT_PLAN_MEM_MAX, 0] {
            let tmp = tempfile::tempdir().unwrap();
            let mut w = PlanWriter::create(tmp.path().join("1.plan"), 1 << 20, mem_max);
            let descriptors = [descriptor(16500), descriptor(16501)];
            let mut expected = Vec::new();
            let mut encoded_bytes = 4;
            for (i, op) in [
                HeapOp::Insert,
                HeapOp::Update,
                HeapOp::HotUpdate,
                HeapOp::Delete,
                HeapOp::Truncate,
            ]
            .into_iter()
            .enumerate()
            {
                let mut h = heap(&descriptors[i % 2], 100 + i as u64, i as i32);
                h.decoded.xid += i as u32;
                h.decoded.op = op;
                if op != HeapOp::Insert {
                    h.decoded.old = h.decoded.new.clone();
                }
                if matches!(op, HeapOp::Delete | HeapOp::Truncate) {
                    h.decoded.new = None;
                }
                if op == HeapOp::Truncate {
                    h.decoded.old = None;
                }
                let mut full = Vec::new();
                spill::encode_heap_into(&mut full, &h.decoded);
                encoded_bytes += (4 + 4 + full.len()) as u64;
                expected.push(h.decoded.clone());
                let table = w.table(&h, || Some(route()));
                if op == HeapOp::Truncate {
                    w.push_truncate(h.decoded, table, 0).unwrap();
                } else {
                    w.push_heap(h.decoded, table).unwrap();
                }
            }
            let plan = w.seal(0x2000, 42).unwrap();
            if mem_max == 0 {
                assert_eq!(plan.size_bytes, encoded_bytes);
            }
            let mut rd = plan.replay().unwrap();
            for decoded in expected {
                let h = match rd.next_item().unwrap().unwrap() {
                    PlanItem::Heap(h) | PlanItem::Truncate { heap: h, .. } => h,
                    PlanItem::Control(_) => panic!(),
                };
                assert_eq!(h.decoded, decoded);
            }
            assert!(rd.next_item().unwrap().is_none());
        }
    }

    /// Plan outgrowing its memory cap moves resident heaps into the file in
    /// order, ahead of later heaps
    #[test]
    fn spill_preserves_resident_heaps() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("1.plan");
        let d = descriptor(16500);
        let resident_max = 2 * mem_cost(&PlannedHeap::new(heap(&d, 0, 0).decoded, 0));
        let mut w = PlanWriter::create(path.clone(), 1 << 20, resident_max);
        for v in 0..4 {
            push(&mut w, heap(&d, 100 + v as u64, v), Some(route())).unwrap();
        }
        let plan = w.seal(0x2000, 42).unwrap();
        assert!(path.exists(), "spilled past the memory cap");
        assert_eq!(plan.size_bytes, fs::read(&path).unwrap().len() as u64);
        let mut rd = plan.replay().unwrap();
        let mut seen = Vec::new();
        while let Some(PlanItem::Heap(h)) = rd.next_item().unwrap() {
            seen.push(h.decoded.new.unwrap().columns[0].clone());
        }
        let want: Vec<_> = (0..4).map(|v| Some(ColumnValue::Int4(v))).collect();
        assert_eq!(seen, want);
    }

    /// Wide narrow-typed rows charge per column slot, so a plan whose
    /// payload alone fits the memory cap still spills
    #[test]
    fn wide_rows_charge_column_slots() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("1.plan");
        let mut w = PlanWriter::create(path.clone(), 1 << 30, DEFAULT_PLAN_MEM_MAX);
        let d = descriptor(16500);
        let mut payload = 0;
        for v in 0..64 {
            let mut h = heap(&d, 100 + v as u64, v);
            h.decoded.new.as_mut().unwrap().columns = vec![Some(ColumnValue::Int4(v)); 1000];
            payload += MEM_FRAME_OVERHEAD + h.decoded.approx_bytes() as u64;
            push(&mut w, h, Some(route())).unwrap();
        }
        assert!(payload < DEFAULT_PLAN_MEM_MAX);
        let plan = w.seal(0x2000, 42).unwrap();
        assert!(
            plan.path().is_some(),
            "column slots pushed plan past mem cap"
        );
        assert!(path.exists());
    }

    /// Disk cap applies to encoded bytes: a wide row whose resident
    /// estimate exceeds the cap still plans when its frame fits
    #[test]
    fn disk_cap_measures_encoded_bytes() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("1.plan");
        let cap = 16 << 10;
        let mut w = PlanWriter::create(path.clone(), cap, DEFAULT_PLAN_MEM_MAX);
        let d = descriptor(16500);
        let mut h = heap(&d, 100, 0);
        h.decoded.new.as_mut().unwrap().columns = vec![Some(ColumnValue::Int4(7)); 1000];
        assert!(mem_cost(&PlannedHeap::new(h.decoded.clone(), 0)) > cap);
        push(&mut w, h, Some(route())).unwrap();
        let plan = w.seal(0x2000, 42).unwrap();
        assert!(plan.size_bytes <= cap);
        assert!(path.exists());
    }

    /// Oracle-pending values (`PgPending` / `Unsupported`) round-trip
    /// byte-identical so post-plan decode resolves them exactly as
    /// live-decoded rows. Best-effort policy: no plan-time codec rejection,
    /// shadow-PG oracle resolves after replay, may lag row's catalog state
    #[test]
    fn pending_values_round_trip() {
        use PlanItem::Heap;
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("1.plan");
        let mut w = PlanWriter::create(path, 1 << 20, DEFAULT_PLAN_MEM_MAX);
        let d = descriptor(16500);
        let cols = vec![
            Some(ColumnValue::PgPending {
                type_oid: 3802,
                raw: br#"{"k":1}"#.to_vec(),
            }),
            Some(ColumnValue::Unsupported {
                type_oid: 600,
                raw: vec![1, 2, 3, 4],
            }),
        ];
        let mut h = heap(&d, 100, 0);
        h.decoded.new.as_mut().unwrap().columns = cols.clone();
        push(&mut w, h, Some(route())).unwrap();
        let plan = w.seal(0x2000, 42).unwrap();
        let mut rd = plan.replay().unwrap();
        let item = rd.next_item().unwrap();
        let Some(Heap(out)) = item else { panic!() };
        assert_eq!(out.decoded.new.unwrap().columns, cols);
        assert!(rd.next_item().unwrap().is_none());
    }

    /// Delete sealed plan file on drop even without replay
    #[test]
    fn sealed_plan_drop_unlinks_file() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("1.plan");
        let mut w = PlanWriter::create(path.clone(), 1 << 20, 0);
        let d = descriptor(16500);
        push(&mut w, heap(&d, 100, 10), Some(route())).unwrap();
        let plan = w.seal(0x2000, 42).unwrap();
        assert!(path.exists(), "mem_max 0 forced file mode");
        drop(plan);
        assert!(!path.exists(), "spool file removed on drop");
    }

    /// Replay a spilled plan after `tamper`, returning the first error
    fn replay_err(
        heaps: Vec<DescribedHeap>,
        tamper: impl FnOnce(&mut SealedPlan, &mut Vec<u8>),
    ) -> PlanSpoolError {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("1.plan");
        let mut w = PlanWriter::create(path.clone(), 1 << 20, 0);
        for h in heaps {
            let table = w.table(&h, || Some(route()));
            if h.decoded.op == HeapOp::Truncate {
                w.push_truncate(h.decoded, table, 0).unwrap();
            } else {
                w.push_heap(h.decoded, table).unwrap();
            }
        }
        let mut plan = w.seal(0x2000, 42).unwrap();
        let mut buf = fs::read(&path).unwrap();
        tamper(&mut plan, &mut buf);
        fs::write(&path, &buf).unwrap();
        let mut rd = match plan.replay() {
            Ok(rd) => rd,
            Err(e) => return e,
        };
        loop {
            match rd.next_item() {
                Ok(Some(_)) => {}
                Ok(None) => panic!("tampered plan replayed clean"),
                Err(e) => return e,
            }
        }
    }

    /// Reject invalid headers, frames, and truncated files during replay
    #[test]
    fn inconsistent_plan_fails_replay() {
        let d = descriptor(16500);
        let one = || vec![heap(&d, 100, 10)];
        let mut truncate = heap(&d, 100, 10);
        truncate.decoded.op = HeapOp::Truncate;
        // header 4, len 4, table 4, xid 4, lsn 8
        let op_byte = 24;
        let cases: Vec<(&str, PlanSpoolError)> = vec![
            ("bad plan magic", replay_err(one(), |_, b| b[0] ^= 0xFF)),
            (
                "unsupported plan version",
                replay_err(one(), |_, b| b[2] ^= 0xFF),
            ),
            ("table id", replay_err(one(), |p, _| p.tables.clear())),
            (
                "unknown HeapOp",
                replay_err(one(), |_, b| b[op_byte] = 0xFF),
            ),
            (
                "left in plan",
                replay_err(one(), |_, b| {
                    b[4..8].copy_from_slice(&u32::MAX.to_le_bytes())
                }),
            ),
            (
                "no mirror-row fence",
                replay_err(vec![truncate], |p, _| p.truncate_rows.clear()),
            ),
        ];
        for (want, err) in cases {
            assert!(
                matches!(err, PlanSpoolError::Format { .. }),
                "{want}: {err}"
            );
            assert!(err.to_string().contains(want), "{want}: {err}");
        }
        let tail = replay_err(vec![heap(&d, 100, 10), heap(&d, 110, 20)], |_, b| {
            b.pop();
        });
        assert!(
            matches!(&tail, PlanSpoolError::Io(e) if e.kind() == std::io::ErrorKind::UnexpectedEof),
            "{tail}"
        );
    }

    /// Byte cap bounds writes; an unsealed writer unlinks its file on drop
    #[test]
    fn budget_bounds_writes_and_drop_unlinks() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("1.plan");
        let mut w = PlanWriter::create(path.clone(), 16, 0);
        let d = descriptor(16500);
        let err = push(&mut w, heap(&d, 100, 10), Some(route())).unwrap_err();
        assert!(matches!(err, PlanSpoolError::DiskBudget { .. }), "{err}");
        drop(w);
        assert!(!path.exists(), "abandoned plan unlinks");
    }
}
