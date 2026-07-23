//! Transient validated plan spool — side-effect-free transaction planning.
//!
//! Planning writes one plan per committed transaction: high-volume heap
//! entries stream through a checksummed spool file while bounded metadata
//! stays resident in the plan header — descriptor dictionary, route table,
//! control entries — matching the live buffer's position that control
//! state per xact stays small. Replay after a successful seal walks heaps
//! and controls in original order with the event-before-heap tie break;
//! any planning error drops the writer and the file unlinks, so the
//! transaction emits nothing. Files are transient and source-WAL
//! reconstructible: never durable, removed at startup by
//! [`clean_plan_files`] via the spill-dir clear.
//!
//! Frame layout after the 4-byte `magic + version` header:
//! `[len:u32][crc32c:u32][body]`, body tag 0 = heap
//! (`dict_id:u32 + route_id:u32 + heap bytes`, spill codec), tag 1 = seal
//! (`heap_count:u64`). A missing seal means planning never finished;
//! trailing bytes after it mean corruption. `route_id == u32::MAX` is the
//! deterministically unmapped row (planned counted discard).

use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use thiserror::Error;

use crate::decode::heap_decoder::DescribedHeap;
use crate::emit::route::{RouteSnapshot, RoutedHeap};
use crate::schema::RelDescriptor;
use crate::xact::spill::{self, Cursor, SpillError};
use crate::xact::xact_buffer::{DrainEntry, OrderedEvent, ToastRowBatch};

/// ASCII for `xxd`-friendly debug
pub const PLAN_MAGIC: [u8; 2] = *b"WP";
pub const PLAN_VERSION: u16 = 1;
/// Spool file suffix, startup-cleanup key
pub const PLAN_SUFFIX: &str = ".plan";

/// Plans at or below this stay memory-resident: the common
/// single-statement commit never touches the filesystem. Larger plans
/// spill to the file at `path` (bounded by the disk cap)
pub const DEFAULT_PLAN_MEM_MAX: u64 = 1 << 20;

const TAG_HEAP: u8 = 0;
const TAG_SEAL: u8 = 1;
/// `route_id` sentinel for unmapped rows
const ROUTE_NONE: u32 = u32::MAX;

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

/// Streams one transaction's validated heaps to disk while accumulating the
/// bounded plan header. Dropped unsealed (any planning error), the file
/// unlinks and nothing replays
enum PlanSink {
    Mem(Vec<u8>),
    File(BufWriter<File>),
}

impl PlanSink {
    fn write_all(&mut self, buf: &[u8]) -> std::io::Result<()> {
        match self {
            Self::Mem(v) => {
                v.extend_from_slice(buf);
                Ok(())
            }
            Self::File(f) => f.write_all(buf),
        }
    }
}

pub struct PlanWriter {
    sink: PlanSink,
    path: PathBuf,
    /// File exists on disk (spilled); unsealed drop unlinks only then
    spilled: bool,
    mem_max: u64,
    cap: u64,
    bytes: u64,
    heap_count: u64,
    /// Heaps pushed with a route (unmapped discards excluded)
    routed_count: u64,
    sealed: bool,
    /// Dictionary ids by `(descriptor Arc identity, valid_from)`; the held
    /// Arc in `descriptors` keeps each pointer live and unique
    desc_ids: HashMap<(usize, u64), u32>,
    descriptors: Vec<(Arc<RelDescriptor>, u64)>,
    route_ids: HashMap<usize, u32>,
    routes: Vec<Arc<RouteSnapshot>>,
    controls: Vec<OrderedEvent>,
    row_batches: Vec<ToastRowBatch>,
    truncate_rows: Vec<usize>,
    scratch: Vec<u8>,
}

impl PlanWriter {
    /// Starts memory-resident; the file at `path` is only created if the
    /// plan outgrows `mem_max`
    pub fn create(path: PathBuf, cap: u64, mem_max: u64) -> Result<Self> {
        let mut sink = PlanSink::Mem(Vec::new());
        sink.write_all(&PLAN_MAGIC)?;
        sink.write_all(&PLAN_VERSION.to_le_bytes())?;
        Ok(Self {
            sink,
            path,
            spilled: false,
            mem_max,
            cap,
            bytes: 4,
            heap_count: 0,
            routed_count: 0,
            sealed: false,
            desc_ids: HashMap::new(),
            descriptors: Vec::new(),
            route_ids: HashMap::new(),
            routes: Vec::new(),
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

    /// Append one validated heap with the route it planned under. Descriptor
    /// and route dedup by Arc identity into the plan header
    pub fn push_heap(
        &mut self,
        heap: &DescribedHeap,
        route: Option<&Arc<RouteSnapshot>>,
    ) -> Result<()> {
        let desc_key = (
            Arc::as_ptr(&heap.descriptor) as usize,
            heap.descriptor_valid_from,
        );
        let next_id = self.descriptors.len() as u32;
        let dict_id = *self.desc_ids.entry(desc_key).or_insert_with(|| {
            self.descriptors
                .push((heap.descriptor.clone(), heap.descriptor_valid_from));
            next_id
        });
        let route_id = if let Some(r) = route {
            let next_id = self.routes.len() as u32;
            *self
                .route_ids
                .entry(Arc::as_ptr(r) as usize)
                .or_insert_with(|| {
                    self.routes.push(r.clone());
                    next_id
                })
        } else {
            ROUTE_NONE
        };
        let mut body = std::mem::take(&mut self.scratch);
        body.clear();
        body.push(TAG_HEAP);
        body.extend_from_slice(&dict_id.to_le_bytes());
        body.extend_from_slice(&route_id.to_le_bytes());
        spill::encode_heap_into(&mut body, &heap.decoded);
        let res = self.write_frame(&body, false);
        self.scratch = body;
        res?;
        self.heap_count += 1;
        if route.is_some() {
            self.routed_count += 1;
        }
        Ok(())
    }

    /// Pin a control entry before the next heap: replay yields it ahead of
    /// heap `heap_count`, preserving the drain's event-before-heap tie break.
    /// `row_idx` is the plan-global mirror-row position it fences
    pub fn push_control(&mut self, event: DrainEntry, row_idx: usize) {
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

    /// Mirror-row fence for the next `HeapOp::Truncate` heap: executor puts
    /// rows up to `upto` before applying the truncate
    pub fn note_truncate_cursor(&mut self, upto: usize) {
        self.truncate_rows.push(upto);
    }

    /// Write the seal frame and freeze the header. Seal is exempt from the
    /// disk budget so a boundary-sized transaction still closes cleanly
    pub fn seal(mut self, commit_lsn: u64, commit_ts: i64) -> Result<SealedPlan> {
        let mut body = std::mem::take(&mut self.scratch);
        body.clear();
        body.push(TAG_SEAL);
        body.extend_from_slice(&self.heap_count.to_le_bytes());
        self.write_frame(&body, true)?;
        let store = match &mut self.sink {
            PlanSink::Mem(buf) => PlanStore::Mem(std::mem::take(buf)),
            PlanSink::File(f) => {
                f.flush()?;
                PlanStore::File(std::mem::take(&mut self.path))
            }
        };
        self.sealed = true;
        Ok(SealedPlan {
            store,
            descriptors: std::mem::take(&mut self.descriptors),
            routes: std::mem::take(&mut self.routes),
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
        if needed > self.mem_max
            && let PlanSink::Mem(buf) = &self.sink
        {
            let mut f = BufWriter::new(File::create(&self.path)?);
            f.write_all(buf)?;
            self.sink = PlanSink::File(f);
            self.spilled = true;
        }
        self.sink.write_all(&(body.len() as u32).to_le_bytes())?;
        self.sink.write_all(&crc32c::crc32c(body).to_le_bytes())?;
        self.sink.write_all(body)?;
        self.bytes = needed;
        Ok(())
    }
}

impl Drop for PlanWriter {
    fn drop(&mut self) {
        if !self.sealed && self.spilled {
            let _ = fs::remove_file(&self.path);
        }
    }
}

/// Backing bytes of a sealed plan: memory for small plans, the spool
/// file once spilled
enum PlanStore {
    Mem(Vec<u8>),
    File(PathBuf),
}

/// Complete validated plan: bounded header plus the sealed frame bytes.
/// A spilled plan unlinks its file on drop — plans are transient,
/// replay-once state
pub struct SealedPlan {
    store: PlanStore,
    /// Dictionary in first-use order, `(descriptor, valid_from)`
    pub descriptors: Vec<(Arc<RelDescriptor>, u64)>,
    /// Route table in first-use order
    pub routes: Vec<Arc<RouteSnapshot>>,
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
    /// Sealed frame bytes, header + seal included; feeds `xact_plan_bytes`
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

    pub fn replay(&self) -> Result<PlanReader<'_>> {
        let mut input: Box<dyn Read + Send + '_> = match &self.store {
            PlanStore::Mem(buf) => Box::new(std::io::Cursor::new(buf.as_slice())),
            PlanStore::File(p) => Box::new(BufReader::new(File::open(p)?)),
        };
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
        Ok(PlanReader {
            plan: self,
            input,
            offset: 4,
            next_heap: 0,
            next_control: 0,
            done: false,
        })
    }

    /// Walk every frame without acting on it: checksums, id resolution,
    /// seal cross-check. Executors verify file-backed plans before the
    /// first side effect so disk corruption fails the whole transaction
    /// instead of a prefix emitting
    pub fn verify(&self) -> Result<()> {
        let mut rd = self.replay()?;
        while rd.next_item()?.is_some() {}
        Ok(())
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
    Heap(RoutedHeap),
}

/// Linear replay over a [`SealedPlan`]: verifies every frame checksum,
/// resolves dictionary/route ids against the header, interleaves controls
/// at their pinned positions
pub struct PlanReader<'p> {
    plan: &'p SealedPlan,
    input: Box<dyn Read + Send + 'p>,
    offset: u64,
    next_heap: u64,
    next_control: usize,
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
        let frame_offset = self.offset;
        let mut fixed = [0u8; 8];
        read_or_unsealed(&mut self.input, &mut fixed)?;
        let len = u32::from_le_bytes(fixed[..4].try_into().unwrap()) as usize;
        let crc = u32::from_le_bytes(fixed[4..].try_into().unwrap());
        let mut body = vec![0u8; len];
        read_or_unsealed(&mut self.input, &mut body)?;
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
                let dict_id = c.u32()? as usize;
                let route_id = c.u32()?;
                let decoded = spill::decode_heap(&mut c)?;
                if c.remaining() != 0 {
                    return Err(format(format!(
                        "heap frame at {frame_offset} has {} trailing bytes",
                        c.remaining(),
                    )));
                }
                let Some((descriptor, valid_from)) = self.plan.descriptors.get(dict_id).cloned()
                else {
                    return Err(format(format!(
                        "heap references dict id {dict_id}, header has {}",
                        self.plan.descriptors.len(),
                    )));
                };
                let route = if route_id == ROUTE_NONE {
                    None
                } else {
                    let Some(r) = self.plan.routes.get(route_id as usize) else {
                        return Err(format(format!(
                            "heap references route id {route_id}, header has {}",
                            self.plan.routes.len(),
                        )));
                    };
                    Some(r.clone())
                };
                self.next_heap += 1;
                Ok(Some(PlanItem::Heap(RoutedHeap {
                    described: DescribedHeap {
                        decoded,
                        descriptor,
                        descriptor_valid_from: valid_from,
                    },
                    route,
                })))
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
                let mut probe = [0u8; 1];
                if self.input.read(&mut probe)? != 0 {
                    return Err(format("trailing bytes after seal".into()));
                }
                self.done = true;
                self.next_item()
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

/// Startup cleanup: plans are transient, any survivor is a crash leftover.
/// Returns removed count
pub fn clean_plan_files(dir: &Path) -> std::io::Result<u64> {
    let entries = match fs::read_dir(dir) {
        Ok(e) => e,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(e) => return Err(e),
    };
    let mut removed = 0;
    for entry in entries {
        let p = entry?.path();
        if p.file_name()
            .and_then(|n| n.to_str())
            .is_some_and(|s| s.ends_with(PLAN_SUFFIX))
        {
            fs::remove_file(&p)?;
            removed += 1;
        }
    }
    Ok(removed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::decode::heap_decoder::{ColumnValue, DecodedHeap, DecodedTuple, HeapOp};
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
                missing_text: None,
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

    fn dropped(oid: u32) -> DrainEntry {
        DrainEntry::Catalog(SchemaEvent::Dropped {
            oid,
            rel_name: RelName::new("public", &format!("t{oid}")),
        })
    }

    /// Replay reproduces write order: controls at their pinned positions
    /// (before their heap, trailing after the last), dictionary and route
    /// table deduped to one entry each per identity
    #[test]
    fn round_trip_orders_heaps_and_controls() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("1.plan");
        let mut w = PlanWriter::create(path.clone(), 1 << 20, DEFAULT_PLAN_MEM_MAX).unwrap();
        let (d1, d2) = (descriptor(16500), descriptor(16501));
        let r = route();
        w.push_control(dropped(1), 0);
        w.push_heap(&heap(&d1, 100, 10), Some(&r)).unwrap();
        w.push_heap(&heap(&d2, 110, 20), None).unwrap();
        w.push_control(dropped(2), 0);
        w.push_heap(&heap(&d1, 120, 30), Some(&r)).unwrap();
        w.push_control(dropped(3), 0);
        let plan = w.seal(0x2000, 42).unwrap();
        assert_eq!(plan.descriptors.len(), 2, "dict deduped");
        assert_eq!(plan.routes.len(), 1, "route table deduped");
        assert_eq!(plan.heap_count, 3);
        assert_eq!(plan.routed_count, 2, "unmapped discard not counted");
        assert!(plan.size_bytes > 0);
        assert_eq!((plan.commit_lsn, plan.commit_ts), (0x2000, 42));

        let mut rd = plan.replay().unwrap();
        let mut order = Vec::new();
        while let Some(item) = rd.next_item().unwrap() {
            order.push(match item {
                PlanItem::Control(c) => {
                    let DrainEntry::Catalog(SchemaEvent::Dropped { oid, .. }) = &c.event else {
                        panic!("unexpected control");
                    };
                    format!("e{oid}")
                }
                PlanItem::Heap(h) => {
                    assert!(
                        Arc::ptr_eq(&h.described.descriptor, &plan.descriptors[0].0)
                            || Arc::ptr_eq(&h.described.descriptor, &plan.descriptors[1].0),
                        "descriptor resolves through the header table"
                    );
                    if let Some(route) = &h.route {
                        assert!(Arc::ptr_eq(route, &plan.routes[0]));
                    }
                    let new = h.described.decoded.new.as_ref().unwrap();
                    let Some(ColumnValue::Int4(v)) = new.columns[0] else {
                        panic!("unexpected column");
                    };
                    format!("h{v}{}", if h.route.is_some() { "r" } else { "-" })
                }
            });
        }
        assert_eq!(order, ["e1", "h10r", "h20-", "e2", "h30r", "e3"]);
        assert!(plan.path().is_none(), "small plan stays memory-resident");
        drop(rd);
        drop(plan);
        assert!(!path.exists(), "no file was ever created");
    }

    /// Oracle-pending values (`PgPending` / `Unsupported`) round-trip
    /// byte-identical so post-plan decode resolves them exactly as
    /// live-decoded rows. Best-effort policy: no plan-time codec rejection,
    /// shadow-PG oracle resolves after replay, may lag row's catalog state
    #[test]
    fn pending_values_round_trip() {
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
        w.push_heap(&h, Some(&route())).unwrap();
        let plan = w.seal(0x2000, 42).unwrap();
        let mut rd = plan.replay().unwrap();
        let Some(PlanItem::Heap(out)) = rd.next_item().unwrap() else {
            panic!("expected heap");
        };
        assert_eq!(out.described.decoded.new.unwrap().columns, cols);
        assert!(rd.next_item().unwrap().is_none());
    }

    /// Flipped payload byte surfaces as a checksum error, not a bad decode
    #[test]
    fn corruption_fails_replay() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("1.plan");
        let mut w = PlanWriter::create(path.clone(), 1 << 20, 0).unwrap();
        let d = descriptor(16500);
        w.push_heap(&heap(&d, 100, 10), None).unwrap();
        let plan = w.seal(0x2000, 42).unwrap();
        let mut bytes = fs::read(&path).unwrap();
        let mid = 4 + 8 + 5; // into the first frame body
        bytes[mid] ^= 0xFF;
        fs::write(&path, &bytes).unwrap();
        let mut rd = plan.replay().unwrap();
        let Err(err) = rd.next_item() else {
            panic!("expected corruption error");
        };
        assert!(matches!(err, PlanSpoolError::Corrupt { .. }), "{err}");
    }

    /// Sealed spilled plan verifies clean, fails after post-seal byte flip
    #[test]
    fn verify_walks_all_frames() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("1.plan");
        let mut w = PlanWriter::create(path.clone(), 1 << 20, 0).unwrap();
        let d = descriptor(16500);
        w.push_heap(&heap(&d, 100, 10), None).unwrap();
        w.push_heap(&heap(&d, 110, 20), None).unwrap();
        let plan = w.seal(0x2000, 42).unwrap();
        plan.verify().unwrap();
        let mut bytes = fs::read(&path).unwrap();
        assert_eq!(plan.size_bytes, bytes.len() as u64);
        let last_body = bytes.len() - 18; // last heap frame body, before 17-byte seal
        bytes[last_body] ^= 0xFF;
        fs::write(&path, &bytes).unwrap();
        let Err(err) = plan.verify() else {
            panic!("expected corruption error");
        };
        assert!(matches!(err, PlanSpoolError::Corrupt { .. }), "{err}");
    }

    /// Executed (sealed + spilled) plan removes its file on drop
    #[test]
    fn sealed_plan_drop_unlinks_file() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("1.plan");
        let mut w = PlanWriter::create(path.clone(), 1 << 20, 0).unwrap();
        let d = descriptor(16500);
        w.push_heap(&heap(&d, 100, 10), None).unwrap();
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
        w.push_heap(&heap(&d, 100, 10), None).unwrap();
        let heaps_only = w.bytes();
        let plan = w.seal(0x2000, 42).unwrap();
        let bytes = fs::read(&path).unwrap();
        fs::write(&path, &bytes[..heaps_only as usize]).unwrap();
        let mut rd = plan.replay().unwrap();
        assert!(
            matches!(rd.next_item(), Ok(Some(PlanItem::Heap(_)))),
            "pre-seal frames intact"
        );
        let Err(err) = rd.next_item() else {
            panic!("expected unsealed error");
        };
        assert!(matches!(err, PlanSpoolError::Unsealed), "{err}");
    }

    /// Byte cap bounds writes; an unsealed writer unlinks its file on drop
    #[test]
    fn budget_bounds_writes_and_drop_unlinks() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("1.plan");
        let mut w = PlanWriter::create(path.clone(), 16, 0).unwrap();
        let d = descriptor(16500);
        let Err(err) = w.push_heap(&heap(&d, 100, 10), None) else {
            panic!("expected budget error");
        };
        assert!(matches!(err, PlanSpoolError::DiskBudget { .. }), "{err}");
        drop(w);
        assert!(!path.exists(), "abandoned plan unlinks");
    }

    #[test]
    fn cleanup_removes_only_plan_files() {
        let tmp = tempfile::tempdir().unwrap();
        for name in ["1.plan", "2.plan", "xid-3.bin"] {
            fs::write(tmp.path().join(name), b"x").unwrap();
        }
        assert_eq!(clean_plan_files(tmp.path()).unwrap(), 2);
        assert!(tmp.path().join("xid-3.bin").exists());
        assert_eq!(clean_plan_files(&tmp.path().join("absent")).unwrap(), 0);
    }
}
