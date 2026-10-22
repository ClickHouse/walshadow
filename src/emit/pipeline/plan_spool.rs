//! Transient validated plan spool — side-effect-free transaction planning.
//!
//! Planning writes one plan per committed transaction: high-volume heap
//! entries stay typed in memory until the plan outgrows its memory cap,
//! then stream through a checksummed spool file while bounded metadata
//! stays resident in the plan header — table list, control entries —
//! matching the live buffer's position that control state per xact stays
//! small. Replay after a successful seal walks heaps and controls in
//! original order with the event-before-heap tie break; any planning error
//! drops the writer and the file unlinks, so the transaction emits nothing.
//! Files are transient and source-WAL reconstructible: never durable, live
//! in xact scratch dir wiped at startup.
//!
//! Frame layout after the 4-byte `magic + version` header:
//! `[len:u32][crc32c:u32][body]`, body tag 0 = heap
//! (`table_id:u32 + heap bytes`, spill codec), tag 1 = seal
//! (`heap_count:u64`). A missing seal means planning never finished;
//! trailing bytes after it mean corruption.

use std::collections::hash_map::Entry;
use std::fs::{self, File};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex as StdMutex};

use thiserror::Error;

use crate::decode::heap_decoder::{ColumnValue, DecodedHeap, DescribedHeap, HeapOp};
use crate::emit::route::RouteSnapshot;
use crate::schema::RelDescriptor;
use crate::xact::spill::{self, Cursor, SpillError};
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
    /// `None` is the deterministically unmapped relation: its heaps are
    /// planned counted discards
    pub route: Option<Arc<RouteSnapshot>>,
    /// Heaps planned under this table
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

const TAG_HEAP: u8 = 0;
const TAG_SEAL: u8 = 1;

#[derive(Debug, Error)]
pub enum PlanSpoolError {
    #[error("plan spool io: {0}")]
    Io(#[from] std::io::Error),
    #[error("plan spool format: {detail}")]
    Format { detail: String },
    #[error("plan frame checksum mismatch at offset {offset}")]
    Corrupt { offset: u64 },
    #[error("plan spool {needed} bytes exceeds disk budget {cap}")]
    DiskBudget { needed: u64, cap: u64 },
    #[error("plan spool ends without seal; planning did not finish")]
    Unsealed,
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

/// Streams one transaction's validated heaps, typed in memory or to disk,
/// while accumulating the bounded plan header. Dropped unsealed (any
/// planning error), the file unlinks and nothing replays
enum PlanSink {
    Mem(Vec<PlannedHeap>),
    File(BufWriter<File>, PathBuf),
}

pub struct PlanWriter {
    sink: PlanSink,
    path: PlanPath,
    mem_max: u64,
    cap: u64,
    bytes: u64,
    heap_count: u64,
    /// Heaps pushed with a route (unmapped discards excluded)
    routed_count: u64,
    sealed: bool,
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
    scratch: Vec<u8>,
}

impl PlanWriter {
    /// Starts memory-resident; the file at `path` is only created if the
    /// plan outgrows `mem_max`
    pub fn create(path: impl Into<PlanPath>, cap: u64, mem_max: u64) -> Result<Self> {
        Ok(Self {
            sink: PlanSink::Mem(Vec::new()),
            path: path.into(),
            mem_max,
            cap,
            bytes: 4,
            heap_count: 0,
            routed_count: 0,
            sealed: false,
            tables: Vec::new(),
            live_from: 0,
            last_table: 0,
            table_ids: None,
            controls: Vec::new(),
            row_batches: Vec::new(),
            truncate_rows: Vec::new(),
            scratch: Vec::new(),
        })
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

    /// Append one validated heap under a [`Self::table`] id
    pub fn push_heap(&mut self, decoded: DecodedHeap, table: u32) -> Result<()> {
        let t = &mut self.tables[table as usize];
        t.heaps += 1;
        if t.route.is_some() {
            self.routed_count += 1;
        }
        let heap = PlannedHeap::new(decoded, table);
        // Resident estimate overstates encoded size, so enforce disk cap
        // only against encoded frames
        if let PlanSink::Mem(_) = &self.sink
            && self.bytes + mem_cost(&heap) > self.mem_max.min(self.cap)
        {
            self.spill()?;
        }
        if let PlanSink::Mem(heaps) = &mut self.sink {
            self.bytes += mem_cost(&heap);
            heaps.push(heap);
        } else {
            self.encode_heap(&heap)?;
        }
        self.heap_count += 1;
        Ok(())
    }

    /// Move resident heaps into a fresh spool file; later heaps append there
    fn spill(&mut self) -> Result<()> {
        let path = self.path.resolve();
        let mut f = BufWriter::new(File::create(&path)?);
        f.write_all(&PLAN_MAGIC)?;
        f.write_all(&PLAN_VERSION.to_le_bytes())?;
        let PlanSink::Mem(heaps) = std::mem::replace(&mut self.sink, PlanSink::File(f, path))
        else {
            unreachable!("spill runs once, from memory");
        };
        self.bytes = 4;
        for h in &heaps {
            self.encode_heap(h)?;
        }
        Ok(())
    }

    fn encode_heap(&mut self, heap: &PlannedHeap) -> Result<()> {
        let mut body = std::mem::take(&mut self.scratch);
        body.clear();
        body.push(TAG_HEAP);
        body.extend_from_slice(&heap.table.to_le_bytes());
        spill::encode_heap_into(&mut body, &heap.decoded);
        let res = self.write_frame(&body, false);
        self.scratch = body;
        res
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

    /// Append a `HeapOp::Truncate` heap with its mirror-row fence: executor
    /// puts rows up to `upto` before applying the truncate
    pub fn push_truncate(&mut self, decoded: DecodedHeap, table: u32, upto: usize) -> Result<()> {
        self.push_heap(decoded, table)?;
        self.truncate_rows.push(upto);
        Ok(())
    }

    /// Write the seal frame and freeze the header. Seal is exempt from the
    /// disk budget so a boundary-sized transaction still closes cleanly
    pub fn seal(mut self, commit_lsn: u64, commit_ts: i64) -> Result<SealedPlan> {
        let store = if let PlanSink::Mem(heaps) = &mut self.sink {
            PlanStore::Mem(StdMutex::new(std::mem::take(heaps)))
        } else {
            let mut body = std::mem::take(&mut self.scratch);
            body.clear();
            body.push(TAG_SEAL);
            body.extend_from_slice(&self.heap_count.to_le_bytes());
            self.write_frame(&body, true)?;
            let PlanSink::File(f, path) = &mut self.sink else {
                unreachable!("memory plans sealed above");
            };
            f.flush()?;
            PlanStore::File(std::mem::take(path))
        };
        self.sealed = true;
        Ok(SealedPlan {
            store,
            tables: std::mem::take(&mut self.tables),
            controls: std::mem::take(&mut self.controls),
            row_batches: std::mem::take(&mut self.row_batches),
            truncate_rows: std::mem::take(&mut self.truncate_rows),
            heap_count: self.heap_count,
            routed_count: self.routed_count,
            size_bytes: self.bytes,
            commit_lsn,
            commit_ts,
        })
    }

    fn write_frame(&mut self, body: &[u8], budget_exempt: bool) -> Result<()> {
        let needed = self.bytes + 8 + body.len() as u64;
        if !budget_exempt && needed > self.cap {
            return Err(PlanSpoolError::DiskBudget {
                needed,
                cap: self.cap,
            });
        }
        if let PlanSink::Mem(_) = &self.sink {
            self.spill()?;
        }
        let PlanSink::File(f, _) = &mut self.sink else {
            unreachable!("spilled above");
        };
        f.write_all(&(body.len() as u32).to_le_bytes())?;
        f.write_all(&crc32c::crc32c(body).to_le_bytes())?;
        f.write_all(body)?;
        self.bytes = needed;
        Ok(())
    }
}

impl Drop for PlanWriter {
    fn drop(&mut self) {
        if !self.sealed
            && let PlanSink::File(_, path) = &self.sink
        {
            let _ = fs::remove_file(path);
        }
    }
}

/// Backing heaps of a sealed plan: typed in memory for small plans, taken
/// by the one replay; the spool file once spilled
enum PlanStore {
    Mem(StdMutex<Vec<PlannedHeap>>),
    File(PathBuf),
}

/// Complete validated plan: bounded header plus the sealed frame bytes.
/// A spilled plan unlinks its file on drop — plans are transient,
/// replay-once state
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
    pub heap_count: u64,
    /// `heap_count` minus unmapped discards; feeds `xact_plan_rows`
    pub routed_count: u64,
    /// Sealed frame bytes, header + seal included, or the resident estimate
    /// of a memory plan; feeds `xact_plan_bytes`
    pub size_bytes: u64,
    pub commit_lsn: u64,
    pub commit_ts: i64,
}

impl SealedPlan {
    /// Spool file path; `None` while memory-resident
    pub fn path(&self) -> Option<&Path> {
        match &self.store {
            PlanStore::Mem(_) => None,
            PlanStore::File(p) => Some(p),
        }
    }

    /// Memory plans hand their heaps to the first replay only
    pub fn replay(&self) -> Result<PlanReader<'_>> {
        let input = match &self.store {
            PlanStore::Mem(heaps) => {
                let heaps = std::mem::take(&mut *heaps.lock().expect("plan heaps poisoned"));
                if heaps.len() as u64 != self.heap_count {
                    return Err(PlanSpoolError::Format {
                        detail: "memory plan already replayed".into(),
                    });
                }
                PlanInput::Mem(heaps.into_iter())
            }
            PlanStore::File(p) => PlanInput::File(Self::open_file(p)?),
        };
        Ok(PlanReader {
            plan: self,
            input,
            offset: 4,
            next_heap: 0,
            next_control: 0,
            next_truncate: 0,
            done: false,
        })
    }

    fn open_file(path: &Path) -> Result<BufReader<File>> {
        let mut input = BufReader::new(File::open(path)?);
        let mut header = [0u8; 4];
        read_or_unsealed(&mut input, &mut header)?;
        if header[..2] != PLAN_MAGIC {
            return Err(PlanSpoolError::Format {
                detail: "bad plan magic".into(),
            });
        }
        let version = u16::from_le_bytes([header[2], header[3]]);
        if version != PLAN_VERSION {
            return Err(PlanSpoolError::Format {
                detail: format!("unsupported plan version {version}, expected {PLAN_VERSION}"),
            });
        }
        Ok(input)
    }

    /// Walk every frame without acting on it: checksums, id resolution,
    /// seal cross-check. Executors verify file-backed plans before the
    /// first side effect so disk corruption fails the whole transaction
    /// instead of a prefix emitting. Memory plans hold heaps validated at
    /// write, nothing to cross-check, and replay would consume them
    pub fn verify(&self) -> Result<()> {
        if self.path().is_none() {
            return Ok(());
        }
        let mut rd = self.replay()?;
        while rd.next_item()?.is_some() {}
        Ok(())
    }

    pub fn into_verified(self) -> Result<VerifiedPlan> {
        self.verify()?;
        Ok(VerifiedPlan(self))
    }
}

/// Plan whose bytes passed checksum, safe to start side effects from
pub struct VerifiedPlan(SealedPlan);

impl std::ops::Deref for VerifiedPlan {
    type Target = SealedPlan;

    fn deref(&self) -> &SealedPlan {
        &self.0
    }
}

impl Drop for SealedPlan {
    fn drop(&mut self) {
        if let PlanStore::File(p) = &self.store {
            let _ = fs::remove_file(p);
        }
    }
}

/// One replayed plan step, original drain order
pub enum PlanItem<'p> {
    Control(&'p OrderedEvent),
    Heap(PlannedHeap),
    /// Mirror rows below `upto` put before the truncate applies
    Truncate {
        heap: PlannedHeap,
        upto: usize,
    },
}

enum PlanInput {
    Mem(std::vec::IntoIter<PlannedHeap>),
    File(BufReader<File>),
}

/// Linear replay over a [`SealedPlan`]: verifies every spooled frame
/// checksum, resolves table ids against the header, interleaves
/// controls at their pinned positions
pub struct PlanReader<'p> {
    plan: &'p SealedPlan,
    input: PlanInput,
    offset: u64,
    next_heap: u64,
    next_control: usize,
    next_truncate: usize,
    done: bool,
}

impl<'p> PlanReader<'p> {
    pub fn next_item(&mut self) -> Result<Option<PlanItem<'p>>> {
        if let Some(c) = self.plan.controls.get(self.next_control)
            && c.heap_idx as u64 <= self.next_heap
        {
            self.next_control += 1;
            return Ok(Some(PlanItem::Control(c)));
        }
        if self.done {
            return Ok(None);
        }
        let heap = match &mut self.input {
            PlanInput::Mem(heaps) => heaps.next(),
            PlanInput::File(_) => self.next_frame()?,
        };
        let Some(heap) = heap else {
            self.done = true;
            return self.next_item();
        };
        self.next_heap += 1;
        if heap.decoded.op != HeapOp::Truncate {
            return Ok(Some(PlanItem::Heap(heap)));
        }
        let Some(&upto) = self.plan.truncate_rows.get(self.next_truncate) else {
            return Err(PlanSpoolError::Format {
                detail: format!(
                    "truncate heap {} has no mirror-row fence, header has {}",
                    self.next_truncate,
                    self.plan.truncate_rows.len(),
                ),
            });
        };
        self.next_truncate += 1;
        Ok(Some(PlanItem::Truncate { heap, upto }))
    }

    /// Next spooled heap, `None` past a seal that cross-checks clean
    fn next_frame(&mut self) -> Result<Option<PlannedHeap>> {
        let PlanInput::File(input) = &mut self.input else {
            unreachable!("frames only spool to file");
        };
        let frame_offset = self.offset;
        let mut fixed = [0u8; 8];
        read_or_unsealed(input, &mut fixed)?;
        let len = u32::from_le_bytes(fixed[..4].try_into().unwrap()) as usize;
        let crc = u32::from_le_bytes(fixed[4..].try_into().unwrap());
        // Length sits outside the body checksum, so bound it against the
        // sealed size before allocating: garbage reads as format error
        // instead of a multi-gigabyte reservation
        let remaining = self.plan.size_bytes.saturating_sub(frame_offset + 8);
        if len as u64 > remaining {
            return Err(PlanSpoolError::Format {
                detail: format!(
                    "frame at {frame_offset} claims {len} bytes, {remaining} left in plan"
                ),
            });
        }
        let mut body = vec![0u8; len];
        read_or_unsealed(input, &mut body)?;
        self.offset += 8 + len as u64;
        if crc32c::crc32c(&body) != crc {
            return Err(PlanSpoolError::Corrupt {
                offset: frame_offset,
            });
        }
        let format = |detail: String| PlanSpoolError::Format { detail };
        match body.first() {
            Some(&TAG_HEAP) => {
                let mut c = Cursor::new(&body[1..]);
                let table = c.u32()?;
                let decoded = spill::decode_heap(&mut c)?;
                if c.remaining() != 0 {
                    return Err(format(format!(
                        "heap frame at {frame_offset} has {} trailing bytes",
                        c.remaining(),
                    )));
                }
                if table as usize >= self.plan.tables.len() {
                    return Err(format(format!(
                        "heap references table id {table}, header has {}",
                        self.plan.tables.len(),
                    )));
                }
                Ok(Some(PlannedHeap::new(decoded, table)))
            }
            Some(&TAG_SEAL) => {
                let count = u64::from_le_bytes(
                    body.get(1..9)
                        .ok_or_else(|| format("short seal frame".into()))?
                        .try_into()
                        .unwrap(),
                );
                if count != self.next_heap || count != self.plan.heap_count {
                    return Err(format(format!(
                        "seal count {count} vs replayed {} / header {}",
                        self.next_heap, self.plan.heap_count,
                    )));
                }
                if self.next_truncate != self.plan.truncate_rows.len() {
                    return Err(format(format!(
                        "replayed {} truncates, header fences {}",
                        self.next_truncate,
                        self.plan.truncate_rows.len(),
                    )));
                }
                let mut probe = [0u8; 1];
                if input.read(&mut probe)? != 0 {
                    return Err(format("trailing bytes after seal".into()));
                }
                Ok(None)
            }
            Some(&tag) => Err(format(format!("unknown plan frame tag {tag}"))),
            None => Err(format("empty plan frame".into())),
        }
    }
}

/// EOF anywhere before the seal frame means planning never finished
fn read_or_unsealed(r: &mut impl Read, buf: &mut [u8]) -> Result<()> {
    r.read_exact(buf).map_err(|e| {
        if e.kind() == std::io::ErrorKind::UnexpectedEof {
            PlanSpoolError::Unsealed
        } else {
            e.into()
        }
    })
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

    /// Replay reproduces write order: controls at their pinned positions
    /// (before their heap, trailing after the last), one table per
    /// descriptor between controls
    #[test]
    fn round_trip_orders_heaps_and_controls() {
        for mem_max in [DEFAULT_PLAN_MEM_MAX, 0] {
            let tmp = tempfile::tempdir().unwrap();
            let path = tmp.path().join("1.plan");
            let mut w = PlanWriter::create(path.clone(), 1 << 20, mem_max).unwrap();
            let (d1, d2) = (descriptor(16500), descriptor(16501));
            let r = route();
            w.push_control(dropped(1), 0);
            push(&mut w, heap(&d1, 100, 10), Some(r.clone())).unwrap();
            push(&mut w, heap(&d2, 110, 20), None).unwrap();
            w.push_control(dropped(2), 0);
            push(&mut w, heap(&d1, 120, 30), Some(r.clone())).unwrap();
            w.push_control(dropped(3), 0);
            let plan = w.seal(0x2000, 42).unwrap();
            let spilled = mem_max == 0;
            let tables: Vec<_> = plan
                .tables
                .iter()
                .map(|t| (t.descriptor.oid, t.route.is_some(), t.heaps))
                .collect();
            assert_eq!(
                tables,
                [(16500, true, 1), (16501, false, 1), (16500, true, 1)],
                "control re-resolves routes"
            );
            assert_eq!(plan.heap_count, 3);
            assert_eq!(plan.routed_count, 2, "unmapped discard not counted");
            assert!(plan.size_bytes > 0);
            assert_eq!((plan.commit_lsn, plan.commit_ts), (0x2000, 42));

            let mut rd = plan.replay().unwrap();
            let mut order = Vec::new();
            while let Some(item) = rd.next_item().unwrap() {
                order.push(match item {
                    PlanItem::Control(c) => format!("{:?}", c.event),
                    PlanItem::Heap(h) | PlanItem::Truncate { heap: h, .. } => {
                        let t = &plan.tables[h.table as usize];
                        assert!(
                            Arc::ptr_eq(&t.descriptor, &d1) || Arc::ptr_eq(&t.descriptor, &d2),
                            "descriptor identity survives the plan"
                        );
                        if let Some(route) = &t.route {
                            assert!(Arc::ptr_eq(route, &r));
                        }
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
                    heap(20, "-"),
                    ev(2),
                    heap(30, "r"),
                    ev(3)
                ]
            );
            assert_eq!(plan.path().is_some(), spilled);
            drop(rd);
            drop(plan);
            assert!(!path.exists(), "no file outlives the plan");
        }
    }

    /// Memory plans hand their heaps to replay once, not verify; a second
    /// replay must not read as an empty transaction
    #[test]
    fn memory_plan_replays_once() {
        let tmp = tempfile::tempdir().unwrap();
        let mut w =
            PlanWriter::create(tmp.path().join("1.plan"), 1 << 20, DEFAULT_PLAN_MEM_MAX).unwrap();
        push(&mut w, heap(&descriptor(16500), 100, 10), None).unwrap();
        let plan = w.seal(0x2000, 42).unwrap();
        plan.verify().unwrap();
        let mut rd = plan.replay().unwrap();
        assert!(rd.next_item().unwrap().is_some(), "verify left heaps");
        while rd.next_item().unwrap().is_some() {}
        let err = plan.replay().err().expect("second replay");
        assert!(err.to_string().contains("already replayed"), "{err}");
    }

    /// Plan outgrowing its memory cap moves resident heaps into the file in
    /// order, ahead of later heaps
    #[test]
    fn spill_preserves_resident_heaps() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("1.plan");
        let d = descriptor(16500);
        let resident_max = 2 * mem_cost(&PlannedHeap::new(heap(&d, 0, 0).decoded, 0));
        let mut w = PlanWriter::create(path.clone(), 1 << 20, resident_max).unwrap();
        for v in 0..4 {
            push(&mut w, heap(&d, 100 + v as u64, v), None).unwrap();
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
        let mut w = PlanWriter::create(path.clone(), 1 << 30, DEFAULT_PLAN_MEM_MAX).unwrap();
        let d = descriptor(16500);
        let mut payload = 0;
        for v in 0..64 {
            let mut h = heap(&d, 100 + v as u64, v);
            h.decoded.new.as_mut().unwrap().columns = vec![Some(ColumnValue::Int4(v)); 1000];
            payload += MEM_FRAME_OVERHEAD + h.decoded.approx_bytes() as u64;
            push(&mut w, h, None).unwrap();
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
        let mut w = PlanWriter::create(path.clone(), cap, DEFAULT_PLAN_MEM_MAX).unwrap();
        let d = descriptor(16500);
        let mut h = heap(&d, 100, 0);
        h.decoded.new.as_mut().unwrap().columns = vec![Some(ColumnValue::Int4(7)); 1000];
        assert!(mem_cost(&PlannedHeap::new(h.decoded.clone(), 0)) > cap);
        push(&mut w, h, None).unwrap();
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
        let mut w = PlanWriter::create(path, 1 << 20, DEFAULT_PLAN_MEM_MAX).unwrap();
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

    /// Flipped payload byte surfaces as a checksum error, not a bad decode
    #[test]
    fn corruption_fails_replay() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("1.plan");
        let mut w = PlanWriter::create(path.clone(), 1 << 20, 0).unwrap();
        let d = descriptor(16500);
        push(&mut w, heap(&d, 100, 10), None).unwrap();
        let plan = w.seal(0x2000, 42).unwrap();
        let mut bytes = fs::read(&path).unwrap();
        let mid = 4 + 8 + 5; // into the first frame body
        bytes[mid] ^= 0xFF;
        fs::write(&path, &bytes).unwrap();
        let mut rd = plan.replay().unwrap();
        let err = rd.next_item().err().expect("corruption error");
        assert!(matches!(err, PlanSpoolError::Corrupt { .. }), "{err}");
    }

    /// Frame length lives outside the body checksum: a garbage length is
    /// rejected against the sealed size rather than allocated
    #[test]
    fn oversize_frame_len_fails_before_allocating() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("1.plan");
        let mut w = PlanWriter::create(path.clone(), 1 << 20, 0).unwrap();
        let d = descriptor(16500);
        push(&mut w, heap(&d, 100, 10), None).unwrap();
        let plan = w.seal(0x2000, 42).unwrap();
        let mut bytes = fs::read(&path).unwrap();
        bytes[4..8].copy_from_slice(&u32::MAX.to_le_bytes());
        fs::write(&path, &bytes).unwrap();
        let err = plan.verify().unwrap_err();
        assert!(matches!(err, PlanSpoolError::Format { .. }), "{err}");
    }

    /// Sealed spilled plan verifies clean, fails after post-seal byte flip
    #[test]
    fn verify_walks_all_frames() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("1.plan");
        let mut w = PlanWriter::create(path.clone(), 1 << 20, 0).unwrap();
        let d = descriptor(16500);
        push(&mut w, heap(&d, 100, 10), None).unwrap();
        push(&mut w, heap(&d, 110, 20), None).unwrap();
        let plan = w.seal(0x2000, 42).unwrap();
        plan.verify().unwrap();
        let mut bytes = fs::read(&path).unwrap();
        assert_eq!(plan.size_bytes, bytes.len() as u64);
        let last_body = bytes.len() - 18; // last heap frame body, before 17-byte seal
        bytes[last_body] ^= 0xFF;
        fs::write(&path, &bytes).unwrap();
        let err = plan.verify().unwrap_err();
        assert!(matches!(err, PlanSpoolError::Corrupt { .. }), "{err}");
    }

    /// Executed (sealed + spilled) plan removes its file on drop
    #[test]
    fn sealed_plan_drop_unlinks_file() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("1.plan");
        let mut w = PlanWriter::create(path.clone(), 1 << 20, 0).unwrap();
        let d = descriptor(16500);
        push(&mut w, heap(&d, 100, 10), None).unwrap();
        let plan = w.seal(0x2000, 42).unwrap();
        assert!(path.exists(), "mem_max 0 forced file mode");
        drop(plan);
        assert!(!path.exists(), "replayed-once plan unlinks");
    }

    /// Truncated tail (crash mid-planning shape) reads as Unsealed
    #[test]
    fn missing_seal_fails_replay() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("1.plan");
        let mut w = PlanWriter::create(path.clone(), 1 << 20, 0).unwrap();
        let d = descriptor(16500);
        push(&mut w, heap(&d, 100, 10), None).unwrap();
        let heaps_only = w.bytes();
        let plan = w.seal(0x2000, 42).unwrap();
        let bytes = fs::read(&path).unwrap();
        fs::write(&path, &bytes[..heaps_only as usize]).unwrap();
        let mut rd = plan.replay().unwrap();
        assert!(
            matches!(rd.next_item(), Ok(Some(PlanItem::Heap(_)))),
            "pre-seal frames intact"
        );
        let err = rd.next_item().err().expect("unsealed error");
        assert!(matches!(err, PlanSpoolError::Unsealed), "{err}");
    }

    /// Replay a spilled plan after `tamper`, returning the first error
    fn replay_err(
        push: impl FnOnce(&mut PlanWriter),
        tamper: impl FnOnce(&mut SealedPlan, &mut Vec<u8>),
    ) -> PlanSpoolError {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("1.plan");
        let mut w = PlanWriter::create(path.clone(), 1 << 20, 0).unwrap();
        push(&mut w);
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

    /// Replay cross-checks every frame against the header it was sealed with
    #[test]
    fn inconsistent_plan_fails_replay() {
        let d = descriptor(16500);
        let mut truncate = heap(&d, 100, 10);
        truncate.decoded.op = HeapOp::Truncate;
        let one = |w: &mut PlanWriter| push(w, heap(&d, 100, 10), None).unwrap();
        let mut trailing_heap = vec![TAG_HEAP];
        trailing_heap.extend_from_slice(&0u32.to_le_bytes());
        spill::encode_heap_into(&mut trailing_heap, &heap(&d, 100, 10).decoded);
        trailing_heap.push(0);
        let cases: Vec<(&str, PlanSpoolError)> = vec![
            ("bad plan magic", replay_err(one, |_, b| b[0] ^= 0xFF)),
            (
                "unsupported plan version",
                replay_err(one, |_, b| b[2] ^= 0xFF),
            ),
            ("table id", replay_err(one, |p, _| p.tables.clear())),
            (
                "no mirror-row fence",
                replay_err(|w| push(w, truncate.clone(), None).unwrap(), |_, _| {}),
            ),
            ("seal count", replay_err(one, |p, _| p.heap_count += 1)),
            (
                "header fences",
                replay_err(one, |p, _| p.truncate_rows.push(0)),
            ),
            (
                "trailing bytes after seal",
                replay_err(one, |_, b| b.push(0)),
            ),
            (
                "unknown plan frame tag",
                replay_err(|w| w.write_frame(&[0x7F], false).unwrap(), |_, _| {}),
            ),
            (
                "empty plan frame",
                replay_err(|w| w.write_frame(&[], false).unwrap(), |_, _| {}),
            ),
            (
                "trailing bytes",
                replay_err(
                    |w| {
                        one(w);
                        w.write_frame(&trailing_heap, false).unwrap();
                    },
                    |_, _| {},
                ),
            ),
            (
                "",
                replay_err(
                    |w| {
                        one(w);
                        w.write_frame(&trailing_heap[..10], false).unwrap();
                    },
                    |_, _| {},
                ),
            ),
        ];
        for (want, err) in cases {
            assert!(
                matches!(err, PlanSpoolError::Format { .. }),
                "{want}: {err}"
            );
            assert!(err.to_string().contains(want), "{want}: {err}");
        }
    }

    /// Byte cap bounds writes; an unsealed writer unlinks its file on drop
    #[test]
    fn budget_bounds_writes_and_drop_unlinks() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("1.plan");
        let mut w = PlanWriter::create(path.clone(), 16, 0).unwrap();
        let d = descriptor(16500);
        let err = push(&mut w, heap(&d, 100, 10), None).unwrap_err();
        assert!(matches!(err, PlanSpoolError::DiskBudget { .. }), "{err}");
        drop(w);
        assert!(!path.exists(), "abandoned plan unlinks");
    }
}
