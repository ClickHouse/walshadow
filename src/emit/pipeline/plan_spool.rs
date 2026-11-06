//! Plan one committed transaction before executing it.
//!
//! Encode routed rows into per-table column chunks while planning. Seal open
//! chunks into a segment at row and byte limits and before every control or
//! truncate; each segment dispatches as one seq. Keep chunks in memory until
//! their size exceeds memory limit, then write them to a spool file. Keep
//! table metadata, controls, and truncates in memory, assuming control state
//! stays small as in live transaction buffer. Count and discard rows without
//! mappings, but keep their truncates. On planning failure, delete spool file
//! and emit nothing. Store files in transaction scratch directory, cleared at
//! startup; recover plans from source WAL. Read errors during replay require
//! recovery from source WAL, as with crashes during execution, so frames omit
//! checksums.
//!
//! Frame layout after 4-byte `magic + version` header, one frame per chunk:
//! `[len:u32][table_id:u32][rows:u32][columns:u32][column buffers]`.
//! Relation and route identity come from plan tables.

use std::collections::hash_map::Entry;
use std::fs::{self, File};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use clickhouse_c::Allocator;
use thiserror::Error;

use crate::ch::EmitterError;
use crate::decode::datum::DatumRef;
use crate::decode::heap_decoder::{
    DecodedHeap, HeapOp, TupleView, datum_value, take_key_change_tombstone,
};
use crate::emit::ch_emitter::{
    Append, EncodedChunk, OP_DELETE, OP_INSERT, OP_UPDATE, RowStamp, TableEncoder, TablePlan,
};
use crate::emit::route::RouteSnapshot;
use crate::schema::RelDescriptor;
use crate::xact::spill::{self, Cursor, IO_CHUNK, SpillError};
use crate::xact::xact_buffer::{DrainEntry, ToastRowBatch};
use ahash::HashMap;

/// ASCII for `xxd`-friendly debug
pub const PLAN_MAGIC: [u8; 2] = *b"WP";
/// v3 replaced typed heap frames with encoded column chunks
pub const PLAN_VERSION: u16 = 3;

/// Plans at or below this stay memory-resident: the common
/// single-statement commit never touches the filesystem or the codec.
/// Larger plans spill to the file at `path` (bounded by the disk cap)
pub const DEFAULT_PLAN_MEM_MAX: u64 = 1 << 20;

/// Descriptor version and the route its rows planned under. Chunks index
/// these, so a run of one relation's rows shares one set of refcounts
pub struct PlanTable {
    pub descriptor: Arc<RelDescriptor>,
    pub valid_from: u64,
    /// Count and discard rows when `None`; still replay truncates
    pub route: Option<Arc<RouteSnapshot>>,
    /// Row changes for this table, including discarded ones
    pub heaps: u64,
}

impl PlanTable {
    fn key(&self) -> (usize, u64) {
        (Arc::as_ptr(&self.descriptor) as usize, self.valid_from)
    }
}

/// Truncate under its plan table
#[derive(Debug)]
pub struct PlannedHeap {
    pub decoded: DecodedHeap,
    /// Index into plan's `tables`
    pub table: u32,
}

/// Rows sealed together; executor dispatches one seq per segment
pub struct Segment {
    pub chunks: Vec<EncodedChunk>,
    /// Destination rows, delete markers for key changes included
    pub rows: u64,
    /// Allocated column bytes, the admission a dispatched segment holds
    pub bytes: usize,
}

#[derive(Debug, Error)]
pub enum PlanSpoolError {
    #[error("plan spool io: {0}")]
    Io(#[from] std::io::Error),
    #[error("plan spool format: {detail}")]
    Format { detail: String },
    #[error("plan spool {needed} bytes exceeds disk budget {cap}")]
    DiskBudget { needed: u64, cap: u64 },
    #[error("encode: {0}")]
    Encode(#[from] EmitterError),
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

/// One plan step in drain order
enum Step {
    /// `chunks` stay `None` once written to the spool file
    Segment {
        count: usize,
        rows: u64,
        bytes: usize,
        chunks: Option<Vec<(u32, EncodedChunk)>>,
    },
    Control {
        event: DrainEntry,
        row_idx: usize,
    },
    Truncate {
        heap: PlannedHeap,
        upto: usize,
    },
}

/// Per-table encoder of the open segment, keyed by plan table
struct OpenTable {
    table: u32,
    enc: TableEncoder,
}

/// Empty encoders kept across plans, so a route compiles its table plan
/// once rather than per transaction. One per relation name: a new
/// descriptor or route version replaces the old
#[derive(Default)]
pub struct EncoderCache {
    entries: Vec<CachedEncoder>,
}

struct CachedEncoder {
    rel: Arc<RelDescriptor>,
    route: Arc<RouteSnapshot>,
    enc: TableEncoder,
}

impl EncoderCache {
    fn take(
        &mut self,
        rel: &Arc<RelDescriptor>,
        route: &Arc<RouteSnapshot>,
    ) -> Option<TableEncoder> {
        let i = self
            .entries
            .iter()
            .position(|e| Arc::ptr_eq(&e.rel, rel) && Arc::ptr_eq(&e.route, route))?;
        Some(self.entries.swap_remove(i).enc)
    }

    fn put(&mut self, rel: Arc<RelDescriptor>, route: Arc<RouteSnapshot>, enc: TableEncoder) {
        self.entries.retain(|e| e.rel.rel_name != rel.rel_name);
        self.entries.push(CachedEncoder { rel, route, enc });
    }

    pub fn clear(&mut self) {
        self.entries.clear();
    }
}

/// Streams one transaction's validated rows into column chunks, in memory
/// or on disk, while accumulating the bounded plan header. Dropped unsealed
/// (any planning error), the file unlinks and nothing replays
pub struct PlanWriter {
    path: PlanPath,
    file: Option<(BufWriter<File>, PlanFile)>,
    mem_max: u64,
    cap: u64,
    /// Resident chunk bytes while in memory, file bytes once spilled
    bytes: u64,
    alloc: Allocator,
    tables: Vec<PlanTable>,
    /// Tables since the last control entry, which may change routes
    live_from: usize,
    /// Table of the previous row, checked before `table_ids`
    last_table: u32,
    /// Live table ids by `(descriptor Arc identity, valid_from)`; the held
    /// Arc in `tables` keeps each pointer live and unique. Built once a
    /// second live table appears, so single-relation plans never hash
    table_ids: Option<HashMap<(usize, u64), u32>>,
    open: Vec<OpenTable>,
    encoders: EncoderCache,
    /// Chunks of the open segment sealed early at an oracle frame limit
    sealed: Vec<(u32, EncodedChunk)>,
    commit_ts: i64,
    open_rows: usize,
    segment_rows: usize,
    segment_bytes: usize,
    steps: Vec<Step>,
    row_batches: Vec<ToastRowBatch>,
    routed_rows: u64,
    deletes_discarded: u64,
    frame: Vec<u8>,
}

impl PlanWriter {
    /// Starts memory-resident; the file at `path` is only created if the
    /// plan outgrows `mem_max`
    pub fn create(path: impl Into<PlanPath>, cap: u64, mem_max: u64) -> Self {
        Self {
            path: path.into(),
            file: None,
            mem_max,
            cap,
            bytes: 4,
            alloc: Allocator::global(&mimalloc::MiMalloc),
            tables: Vec::new(),
            live_from: 0,
            last_table: 0,
            table_ids: None,
            open: Vec::new(),
            encoders: EncoderCache::default(),
            sealed: Vec::new(),
            commit_ts: 0,
            open_rows: 0,
            segment_rows: usize::MAX,
            segment_bytes: usize::MAX,
            steps: Vec::new(),
            row_batches: Vec::new(),
            routed_rows: 0,
            deletes_discarded: 0,
            frame: Vec::new(),
        }
    }

    /// Seal a segment once its open rows or bytes reach these
    pub fn with_segment_limits(mut self, rows: usize, bytes: usize) -> Self {
        self.segment_rows = rows.max(1);
        self.segment_bytes = bytes.max(1);
        self
    }

    /// Commit timestamp stamped on every planned row
    pub fn for_commit(mut self, commit_ts: i64) -> Self {
        self.commit_ts = commit_ts;
        self
    }

    /// Reuse encoders an earlier plan returned through [`Self::seal_keeping`]
    pub fn with_encoders(mut self, encoders: EncoderCache) -> Self {
        self.encoders = encoders;
        self
    }

    /// Disk bytes written so far, header included
    pub fn bytes(&self) -> u64 {
        self.bytes
    }

    /// Table rows of `descriptor` plan under. `route` resolves only for a
    /// descriptor version without a table since the last control entry
    pub fn table(
        &mut self,
        descriptor: &Arc<RelDescriptor>,
        valid_from: u64,
        route: impl FnOnce() -> Option<Arc<RouteSnapshot>>,
    ) -> u32 {
        let key = (Arc::as_ptr(descriptor) as usize, valid_from);
        let live = self.live_from..self.tables.len();
        if live.contains(&(self.last_table as usize))
            && self.tables[self.last_table as usize].key() == key
        {
            return self.last_table;
        }
        let next_id = self.tables.len() as u32;
        if !live.is_empty() {
            let ids = self
                .table_ids
                .get_or_insert_with(|| live.map(|id| (self.tables[id].key(), id as u32)).collect());
            match ids.entry(key) {
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
            descriptor: descriptor.clone(),
            valid_from,
            route: route(),
            heaps: 0,
        });
        self.last_table = next_id;
        next_id
    }

    pub fn routed(&self, table: u32) -> bool {
        self.tables[table as usize].route.is_some()
    }

    /// Count `rows` changes of an unmapped table without planning them
    pub fn discard(&mut self, table: u32, rows: u64) {
        debug_assert!(!self.routed(table));
        self.tables[table as usize].heaps += rows;
    }

    /// Encode one owned row change: drop deletes an append-only route has
    /// nowhere to land, render PostGIS cells, split key changes into a
    /// delete marker plus the new row
    pub fn push_cells(&mut self, mut decoded: DecodedHeap, table: u32) -> Result<()> {
        let t = &mut self.tables[table as usize];
        t.heaps += 1;
        let (rel, route) = (t.descriptor.clone(), t.route.clone().expect("routed table"));
        if route.drops_deletes() && decoded.op == HeapOp::Delete {
            self.deletes_discarded += 1;
            return Ok(());
        }
        for t in [decoded.new.as_mut(), decoded.old.as_mut()]
            .into_iter()
            .flatten()
        {
            crate::ops::oracle::render_ext_columns(&rel.attributes, &mut t.columns);
        }
        let tomb = (!route.drops_deletes())
            .then(|| take_key_change_tombstone(&mut decoded, &rel))
            .flatten();
        let leaf = route.partition.as_ref().map(|p| p.leaf);
        for decoded in tomb.into_iter().chain([decoded]) {
            let op = op_code(decoded.op)?;
            let committed = crate::decode::heap_decoder::CommittedTuple {
                decoded,
                commit_ts: self.commit_ts,
                commit_lsn: 0,
            };
            self.append(table, |enc| {
                enc.append_routed(&committed, &route.mapping, op, leaf)
            })?;
        }
        Ok(())
    }

    /// [`Self::push_cells`] reading tuple images in place
    pub fn push_views(&mut self, change: ViewChange<'_, '_>, table: u32) -> Result<()> {
        let t = &mut self.tables[table as usize];
        t.heaps += 1;
        let (rel, route) = (t.descriptor.clone(), t.route.clone().expect("routed table"));
        if route.drops_deletes() && change.op == HeapOp::Delete {
            self.deletes_discarded += 1;
            return Ok(());
        }
        let leaf = route.partition.as_ref().map(|p| p.leaf);
        let commit_ts = self.commit_ts;
        let stamp = |source_lsn, op_code| RowStamp {
            source_lsn,
            xid: change.xid,
            commit_ts,
            op_code,
            leaf,
        };
        if !route.drops_deletes()
            && let (Some(old), Some(new)) = (change.old, change.new)
            && key_moved(&rel, change.op, old, new)
        {
            let stamp = stamp(change.source_lsn - 1, OP_DELETE);
            self.append(table, |enc| enc.append_view(&rel, &old.datums, &stamp))?;
        }
        let side = match change.op {
            HeapOp::Delete => change.old,
            _ => change.new,
        };
        let stamp = stamp(change.source_lsn, op_code(change.op)?);
        let datums = side.map_or(&[][..], |t| &t.datums);
        self.append(table, |enc| enc.append_view(&rel, datums, &stamp))
    }

    /// Encode into `table`'s open chunk, sealing it first when an oracle
    /// frame fills
    fn append(
        &mut self,
        table: u32,
        mut row: impl FnMut(&mut TableEncoder) -> std::result::Result<Append, EmitterError>,
    ) -> Result<()> {
        let i = match self.open.iter().position(|o| o.table == table) {
            Some(i) => i,
            None => {
                let t = &self.tables[table as usize];
                let route = t.route.as_ref().expect("routed table");
                let enc = match self.encoders.take(&t.descriptor, route) {
                    Some(enc) => enc,
                    None => {
                        TableEncoder::new(TablePlan::for_route(self.alloc, &t.descriptor, route)?)?
                    }
                };
                self.open.push(OpenTable { table, enc });
                self.open.len() - 1
            }
        };
        if row(&mut self.open[i].enc)? == Append::Full {
            let chunk = self.take_chunk(i)?;
            self.sealed.push(chunk);
            row(&mut self.open[i].enc)?;
        }
        self.open_rows += 1;
        self.routed_rows += 1;
        let open_bytes = self.open.iter().map(|o| o.enc.approx_bytes).sum::<usize>()
            + self
                .sealed
                .iter()
                .map(|(_, c)| c.allocated_bytes())
                .sum::<usize>();
        if self.open_rows >= self.segment_rows || open_bytes >= self.segment_bytes {
            self.seal_segment()?;
        }
        Ok(())
    }

    fn take_chunk(&mut self, i: usize) -> Result<(u32, EncodedChunk)> {
        let o = &mut self.open[i];
        let t = &self.tables[o.table as usize];
        let (buffers, rows) = o.enc.take_block()?;
        let chunk = EncodedChunk {
            rel: t.descriptor.clone(),
            route: t.route.clone().expect("routed table"),
            buffers,
            rows,
        };
        Ok((o.table, chunk))
    }

    /// Close the open segment into a step, spilling once memory runs out
    fn seal_segment(&mut self) -> Result<()> {
        let mut chunks = std::mem::take(&mut self.sealed);
        for i in 0..self.open.len() {
            if self.open[i].enc.rows > 0 {
                chunks.push(self.take_chunk(i)?);
            }
        }
        self.open_rows = 0;
        if chunks.is_empty() {
            return Ok(());
        }
        let rows = chunks.iter().map(|(_, c)| c.rows as u64).sum();
        let bytes = chunks
            .iter()
            .map(|(_, c)| c.allocated_bytes())
            .sum::<usize>();
        let count = chunks.len();
        let chunks =
            if self.file.is_none() && self.bytes + bytes as u64 <= self.mem_max.min(self.cap) {
                self.bytes += bytes as u64;
                Some(chunks)
            } else {
                self.spill()?;
                for (table, c) in &chunks {
                    self.write_chunk(*table, c)?;
                }
                None
            };
        self.steps.push(Step::Segment {
            count,
            rows,
            bytes,
            chunks,
        });
        Ok(())
    }

    /// Move resident chunks into a fresh spool file, in step order
    fn spill(&mut self) -> Result<()> {
        if self.file.is_some() {
            return Ok(());
        }
        let path = self.path.resolve();
        let mut out = BufWriter::with_capacity(IO_CHUNK, File::create(&path)?);
        out.write_all(&PLAN_MAGIC)?;
        out.write_all(&PLAN_VERSION.to_le_bytes())?;
        self.file = Some((out, PlanFile(path)));
        self.bytes = 4;
        let mut steps = std::mem::take(&mut self.steps);
        let written = steps.iter_mut().try_for_each(|step| {
            if let Step::Segment { chunks, .. } = step
                && let Some(chunks) = chunks.take()
            {
                chunks
                    .iter()
                    .try_for_each(|(table, c)| self.write_chunk(*table, c))?;
            }
            Ok::<_, PlanSpoolError>(())
        });
        self.steps = steps;
        written
    }

    fn write_chunk(&mut self, table: u32, chunk: &EncodedChunk) -> Result<()> {
        let frame = &mut self.frame;
        frame.clear();
        frame.extend_from_slice(&[0; 4]);
        spill::push_u32(frame, table);
        chunk.encode_into(frame);
        let needed = self.bytes + frame.len() as u64;
        if needed > self.cap {
            return Err(PlanSpoolError::DiskBudget {
                needed,
                cap: self.cap,
            });
        }
        let len = (frame.len() - 4) as u32;
        frame[..4].copy_from_slice(&len.to_le_bytes());
        let (out, _) = self.file.as_mut().expect("frames require a spool file");
        out.write_all(frame)?;
        self.bytes = needed;
        Ok(())
    }

    /// Append `HeapOp::Truncate`; executor stores mirror rows with indices
    /// below `upto`, then applies truncate
    pub fn push_truncate(&mut self, decoded: DecodedHeap, table: u32, upto: usize) -> Result<()> {
        self.seal_segment()?;
        self.tables[table as usize].heaps += 1;
        self.steps.push(Step::Truncate {
            heap: PlannedHeap { decoded, table },
            upto,
        });
        Ok(())
    }

    /// Pin a control entry after every row planned so far, preserving the
    /// drain's event-before-heap tie break. `row_idx` is the plan-global
    /// mirror-row position it fences
    pub fn push_control(&mut self, event: DrainEntry, row_idx: usize) -> Result<()> {
        self.seal_segment()?;
        self.open.clear();
        self.live_from = self.tables.len();
        self.table_ids = None;
        self.steps.push(Step::Control { event, row_idx });
        Ok(())
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
        self.seal_keeping(commit_lsn, commit_ts)
            .map(|(plan, _)| plan)
    }

    /// [`Self::seal`], returning emptied encoders for the next plan
    pub fn seal_keeping(
        mut self,
        commit_lsn: u64,
        commit_ts: i64,
    ) -> Result<(SealedPlan, EncoderCache)> {
        self.seal_segment()?;
        let mut encoders = std::mem::take(&mut self.encoders);
        for o in self.open.drain(..) {
            let t = &self.tables[o.table as usize];
            let route = t.route.clone().expect("routed table");
            encoders.put(t.descriptor.clone(), route, o.enc);
        }
        let file = match self.file.take() {
            None => None,
            Some((mut out, file)) => {
                out.flush()?;
                Some(file)
            }
        };
        let plan = SealedPlan {
            file,
            steps: self.steps,
            tables: self.tables,
            row_batches: self.row_batches,
            routed_rows: self.routed_rows,
            deletes_discarded: self.deletes_discarded,
            size_bytes: self.bytes,
            commit_lsn,
            commit_ts,
        };
        Ok((plan, encoders))
    }
}

/// One row change read from tuple images in place
#[derive(Clone, Copy)]
pub struct ViewChange<'v, 'a> {
    pub op: HeapOp,
    pub xid: u32,
    pub source_lsn: u64,
    pub new: Option<&'v TupleView<'a>>,
    pub old: Option<&'v TupleView<'a>>,
}

/// [`take_key_change_tombstone`]'s test over tuple images in place
fn key_moved(rel: &RelDescriptor, op: HeapOp, old: &TupleView<'_>, new: &TupleView<'_>) -> bool {
    let hot = match op {
        HeapOp::Update => false,
        HeapOp::HotUpdate => true,
        _ => return false,
    };
    let differs = |a: usize| match (old.datums.get(a), new.datums.get(a), rel.attributes.get(a)) {
        (Some(&o), Some(&n), Some(att)) => datums_differ(att, o, n),
        _ => false,
    };
    let keys = crate::schema::replident_key_attnums(rel);
    if keys.is_empty() {
        matches!(
            rel.replident,
            crate::schema::ReplIdent::Full { pk_attnums: None }
        ) && (0..new.datums.len()).any(differs)
    } else {
        // PostgreSQL HOT updates leave indexed columns unchanged
        !hot && keys.iter().any(|&a| differs(a as usize - 1))
    }
}

/// Cell inequality as owned decode compares it. Equal stored bytes decode
/// equal except float NaN, which never equals itself
fn datums_differ(att: &crate::schema::RelAttr, o: DatumRef<'_>, n: DatumRef<'_>) -> bool {
    use crate::schema::{FLOAT4OID, FLOAT8OID};
    match (o, n) {
        (DatumRef::Absent, _) | (_, DatumRef::Absent) => false,
        (DatumRef::Fixed(a), DatumRef::Fixed(b))
            if a == b && !matches!(att.type_oid, FLOAT4OID | FLOAT8OID) =>
        {
            false
        }
        (DatumRef::Varlena(a), DatumRef::Varlena(b)) if a == b => false,
        (DatumRef::Null | DatumRef::Dropped, DatumRef::Null | DatumRef::Dropped) => false,
        _ => {
            let mut values = [o, n].map(|d| datum_value(att, d));
            if !att.dropped && matches!(att.type_name.as_str(), "geography" | "geometry") {
                for v in values.iter_mut().flatten() {
                    crate::ops::oracle::render_ext_value(v);
                }
            }
            values[0] != values[1]
        }
    }
}

fn op_code(op: HeapOp) -> std::result::Result<i8, EmitterError> {
    Ok(match op {
        HeapOp::Insert => OP_INSERT,
        HeapOp::Update | HeapOp::HotUpdate => OP_UPDATE,
        HeapOp::Delete => OP_DELETE,
        HeapOp::Truncate => {
            return Err(EmitterError::Type("TRUNCATE planned as a row".into()));
        }
    })
}

/// Validated plan, consumed by [`Self::replay`]
pub struct SealedPlan {
    file: Option<PlanFile>,
    steps: Vec<Step>,
    /// Tables in first-use order, indexed by chunk frames and truncates
    pub tables: Vec<PlanTable>,
    /// Mirror-row refs in plan order, still gauged; control `row_idx` and
    /// truncate fences index the concatenation
    pub row_batches: Vec<ToastRowBatch>,
    /// Destination rows planned, delete markers included
    pub routed_rows: u64,
    /// Deletes dropped for append-only routes
    pub deletes_discarded: u64,
    /// File size including header, or column bytes for plans in memory;
    /// reported as `xact_plan_bytes`
    pub size_bytes: u64,
    pub commit_lsn: u64,
    pub commit_ts: i64,
}

impl SealedPlan {
    /// Spool file path; `None` while memory-resident
    pub fn path(&self) -> Option<&Path> {
        self.file.as_ref().map(|f| f.0.as_path())
    }

    /// Count row changes with routes for `xact_plan_rows`
    pub fn routed_count(&self) -> u64 {
        self.tables
            .iter()
            .filter(|t| t.route.is_some())
            .map(|t| t.heaps)
            .sum()
    }

    pub fn replay(self) -> Result<PlanReader> {
        let input = match self.file {
            None => None,
            Some(file) => {
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
                Some(FileInput {
                    input,
                    frame: Vec::new(),
                    bytes: self.size_bytes - 4,
                    _file: file,
                })
            }
        };
        Ok(PlanReader {
            tables: self.tables,
            row_batches: self.row_batches,
            deletes_discarded: self.deletes_discarded,
            steps: self.steps.into_iter(),
            input,
        })
    }
}

/// One replayed plan step, original drain order
pub enum PlanItem {
    Control {
        event: DrainEntry,
        row_idx: usize,
    },
    Segment(Segment),
    /// Mirror rows below `upto` put before the truncate applies
    Truncate {
        heap: PlannedHeap,
        upto: usize,
    },
}

struct FileInput {
    input: BufReader<File>,
    frame: Vec<u8>,
    /// Bytes left before the sealed end
    bytes: u64,
    _file: PlanFile,
}

/// Replay [`SealedPlan`] in order, resolving table ids from plan metadata
pub struct PlanReader {
    /// [`SealedPlan::tables`]
    pub tables: Vec<PlanTable>,
    /// [`SealedPlan::row_batches`]
    pub row_batches: Vec<ToastRowBatch>,
    /// [`SealedPlan::deletes_discarded`]
    pub deletes_discarded: u64,
    steps: std::vec::IntoIter<Step>,
    input: Option<FileInput>,
}

impl PlanReader {
    pub fn next_item(&mut self) -> Result<Option<PlanItem>> {
        let Some(step) = self.steps.next() else {
            return Ok(None);
        };
        Ok(Some(match step {
            Step::Control { event, row_idx } => PlanItem::Control { event, row_idx },
            Step::Truncate { heap, upto } => PlanItem::Truncate { heap, upto },
            Step::Segment {
                count,
                rows,
                bytes,
                chunks,
            } => {
                let chunks = match chunks {
                    Some(chunks) => chunks.into_iter().map(|(_, c)| c).collect(),
                    None => {
                        let Some(f) = self.input.as_mut() else {
                            return Err(PlanSpoolError::Format {
                                detail: "spilled segment without spool file".into(),
                            });
                        };
                        (0..count)
                            .map(|_| read_frame(f, &self.tables))
                            .collect::<Result<_>>()?
                    }
                };
                PlanItem::Segment(Segment {
                    chunks,
                    rows,
                    bytes,
                })
            }
        }))
    }
}

/// Read next `[len][table_id][chunk]` frame
fn read_frame(f: &mut FileInput, tables: &[PlanTable]) -> Result<EncodedChunk> {
    let mut len = [0u8; 4];
    f.input.read_exact(&mut len)?;
    let len = u32::from_le_bytes(len);
    // Bound against sealed size before allocating, so a garbage length
    // reads as format error instead of a multi-gigabyte buffer
    if 4 + u64::from(len) > f.bytes {
        return Err(PlanSpoolError::Format {
            detail: format!("frame claims {len} bytes, {} left in plan", f.bytes - 4),
        });
    }
    f.bytes -= 4 + u64::from(len);
    f.frame.resize(len as usize, 0);
    f.input.read_exact(&mut f.frame)?;
    let mut c = Cursor::new(&f.frame);
    let table = c.u32()?;
    let Some(PlanTable {
        descriptor,
        route: Some(route),
        ..
    }) = tables.get(table as usize)
    else {
        return Err(PlanSpoolError::Format {
            detail: format!(
                "chunk references table id {table}, header has {} routed",
                tables.len()
            ),
        });
    };
    Ok(EncodedChunk::decode(
        descriptor.clone(),
        route.clone(),
        &mut c,
    )?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::decode::heap_decoder::{ColumnValue, DecodedTuple};
    use crate::emit::ch_emitter::Wire;
    use crate::mapping::{ColumnMapping, TableMapping, TableTarget};
    use crate::schema::{INT4OID, RelAttr, RelName, SchemaEvent};
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

    fn heap(rel: &Arc<RelDescriptor>, lsn: u64, v: i32) -> DecodedHeap {
        DecodedHeap {
            rfn: rel.rfn,
            xid: 7,
            source_lsn: lsn,
            op: HeapOp::Insert,
            new: Some(DecodedTuple {
                columns: vec![Some(ColumnValue::Int4(v))],
                partial: false,
            }),
            old: None,
        }
    }

    /// Route mapping `id` to `target`
    fn route_to(target: &str) -> Arc<RouteSnapshot> {
        RouteSnapshot::freeze(
            Arc::new(TableMapping {
                target: TableTarget::new("db", "t"),
                columns: vec![ColumnMapping {
                    src_attnum: 1,
                    target_name: "id".into(),
                    target_type: target.into(),
                    type_pinned: false,
                }],
                tees: Vec::new(),
            }),
            Arc::default(),
            Default::default(),
        )
    }

    fn route() -> Arc<RouteSnapshot> {
        route_to("Nullable(Int32)")
    }

    /// Push `heap` under the table it plans to, `route` deciding a new one
    fn push(
        w: &mut PlanWriter,
        rel: &Arc<RelDescriptor>,
        heap: DecodedHeap,
        route: Option<Arc<RouteSnapshot>>,
    ) -> Result<()> {
        let table = w.table(rel, 0x40, || route);
        if w.routed(table) {
            w.push_cells(heap, table)
        } else {
            w.discard(table, 1);
            Ok(())
        }
    }

    fn dropped(oid: u32) -> DrainEntry {
        DrainEntry::Catalog(SchemaEvent::Dropped {
            oid,
            rel_name: RelName::new("public", &format!("t{oid}")),
            partition: None,
        })
    }

    /// `(id, _lsn, _xid, _is_deleted)` per row of every chunk
    fn rows(chunks: &[EncodedChunk]) -> Vec<(Option<i32>, u64, u32, u8)> {
        let mut out = Vec::new();
        for c in chunks {
            let Wire::NullableFixed {
                null_map, inner, ..
            } = c.buffers[0].wire()
            else {
                panic!("nullable int4");
            };
            let fixed = |i: usize, w: usize| match c.buffers[i].wire() {
                Wire::Fixed { bytes, .. } => bytes
                    .chunks_exact(w)
                    .map(|b| {
                        let mut v = [0u8; 8];
                        v[..w].copy_from_slice(b);
                        u64::from_le_bytes(v)
                    })
                    .collect::<Vec<_>>(),
                w => panic!("{w:?}"),
            };
            let (lsn, xid) = (fixed(1, 8), fixed(2, 4));
            // Append-only routes carry no delete marker
            let deleted = if c.buffers.len() > 4 {
                fixed(4, 1)
            } else {
                vec![0; c.rows]
            };
            for r in 0..c.rows {
                let id = (null_map[r] == 0)
                    .then(|| i32::from_le_bytes(inner[r * 4..r * 4 + 4].try_into().unwrap()));
                out.push((id, lsn[r], xid[r] as u32, deleted[r] as u8));
            }
        }
        out
    }

    /// Preserve write order, sealing segments before controls and
    /// truncates. Reuse one table per descriptor between controls. Count
    /// and discard unmapped rows, but replay unmapped truncates
    #[test]
    fn round_trip_orders_segments_and_controls() {
        for mem_max in [DEFAULT_PLAN_MEM_MAX, 0] {
            let tmp = tempfile::tempdir().unwrap();
            let path = tmp.path().join("1.plan");
            let mut w = PlanWriter::create(path.clone(), 1 << 20, mem_max).for_commit(42);
            let (d1, d2) = (descriptor(16500), descriptor(16501));
            let r = route();
            w.push_control(dropped(1), 0).unwrap();
            push(&mut w, &d1, heap(&d1, 100, 10), Some(r.clone())).unwrap();
            push(&mut w, &d2, heap(&d2, 110, 20), None).unwrap();
            let mut truncate = heap(&d2, 115, 25);
            truncate.op = HeapOp::Truncate;
            let table = w.table(&d2, 0x40, || unreachable!("table live"));
            w.push_truncate(truncate, table, 0).unwrap();
            w.push_control(dropped(2), 0).unwrap();
            push(&mut w, &d1, heap(&d1, 120, 30), Some(r.clone())).unwrap();
            w.push_control(dropped(3), 0).unwrap();
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
            assert_eq!(plan.routed_rows, 2, "unmapped insert discarded");
            assert_eq!(plan.routed_count(), 2);
            assert!(plan.size_bytes > 0);
            assert_eq!((plan.commit_lsn, plan.commit_ts), (0x2000, 42));
            assert_eq!(plan.path().is_some(), mem_max == 0);

            let mut rd = plan.replay().unwrap();
            let mut order = Vec::new();
            while let Some(item) = rd.next_item().unwrap() {
                order.push(match item {
                    PlanItem::Control { event, .. } => format!("{event:?}"),
                    PlanItem::Segment(s) => {
                        for c in &s.chunks {
                            assert!(Arc::ptr_eq(&c.rel, &d1), "descriptor survives plan");
                            assert!(Arc::ptr_eq(&c.route, &r));
                        }
                        format!("{:?}", rows(&s.chunks))
                    }
                    PlanItem::Truncate { heap: h, .. } => {
                        assert!(rd.tables[h.table as usize].route.is_none());
                        format!("t{}", h.decoded.source_lsn)
                    }
                });
            }
            let ev = |oid| format!("{:?}", dropped(oid));
            let seg = |v, lsn| format!("{:?}", [(Some(v), lsn, 7u32, 0u8)]);
            assert_eq!(
                order,
                [
                    ev(1),
                    seg(10, 100),
                    "t115".into(),
                    ev(2),
                    seg(30, 120),
                    ev(3)
                ]
            );
            drop(rd);
            assert!(!path.exists(), "spool file removed after replay");
        }
    }

    /// Updates, deletes and key changes encode as destination rows;
    /// append-only routes drop deletes and count them
    #[test]
    fn row_changes_encode_markers_and_drops() {
        let d = descriptor(16500);
        let image = |v| {
            Some(DecodedTuple {
                columns: vec![Some(ColumnValue::Int4(v))],
                partial: false,
            })
        };
        let change = |op, lsn, new, old| DecodedHeap {
            op,
            new,
            old,
            xid: 8,
            ..heap(&d, lsn, 0)
        };
        let append_only = RouteSnapshot::freeze(
            route().mapping.clone(),
            Arc::default(),
            crate::emit::route::RowPolicy {
                system: Arc::new(crate::mapping::SystemColumns {
                    is_deleted: None,
                    ..Default::default()
                }),
                ..Default::default()
            },
        );
        for (route, want, discarded) in [
            (
                route(),
                vec![
                    (Some(1), 100, 8, 0),
                    (Some(1), 199, 8, 1),
                    (Some(2), 200, 8, 0),
                    (Some(2), 300, 8, 0),
                    (Some(2), 400, 8, 1),
                ],
                0,
            ),
            (
                append_only,
                vec![
                    (Some(1), 100, 8, 0),
                    (Some(2), 200, 8, 0),
                    (Some(2), 300, 8, 0),
                ],
                1,
            ),
        ] {
            let tmp = tempfile::tempdir().unwrap();
            let mut w = PlanWriter::create(tmp.path().join("1.plan"), 1 << 20, 0);
            let table = w.table(&d, 0x40, || Some(route));
            for h in [
                change(HeapOp::Insert, 100, image(1), None),
                // FULL identity without key: any change moves the row
                change(HeapOp::Update, 200, image(2), image(1)),
                change(HeapOp::HotUpdate, 300, image(2), image(2)),
                change(HeapOp::Delete, 400, None, image(2)),
            ] {
                w.push_cells(h, table).unwrap();
            }
            let plan = w.seal(0x2000, 42).unwrap();
            assert_eq!(plan.deletes_discarded, discarded);
            let mut rd = plan.replay().unwrap();
            let Some(PlanItem::Segment(s)) = rd.next_item().unwrap() else {
                panic!("one segment");
            };
            assert_eq!(rows(&s.chunks), want);
            assert_eq!(s.rows, want.len() as u64);
            assert_eq!(rd.deletes_discarded, discarded);
        }
    }

    /// Segment limits split rows into seqs; a plan outgrowing its memory
    /// cap moves resident chunks into the file in order, ahead of later
    #[test]
    fn segments_split_and_spill_in_order() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("1.plan");
        let d = descriptor(16500);
        let mut w =
            PlanWriter::create(path.clone(), 1 << 20, 1 << 10).with_segment_limits(2, usize::MAX);
        for v in 0..64 {
            push(&mut w, &d, heap(&d, 100 + v as u64, v), Some(route())).unwrap();
        }
        let plan = w.seal(0x2000, 42).unwrap();
        assert!(path.exists(), "spilled past the memory cap");
        assert_eq!(plan.size_bytes, fs::read(&path).unwrap().len() as u64);
        let mut rd = plan.replay().unwrap();
        let mut seen = Vec::new();
        while let Some(PlanItem::Segment(s)) = rd.next_item().unwrap() {
            assert_eq!(s.rows, 2);
            seen.extend(rows(&s.chunks).into_iter().map(|r| r.0.unwrap()));
        }
        assert_eq!(seen, (0..64).collect::<Vec<_>>());
    }

    /// Sealing returns emptied encoders that serve the next plan under the
    /// same route; another route version compiles its own
    #[test]
    fn encoders_carry_across_plans() {
        let tmp = tempfile::tempdir().unwrap();
        let (d, r) = (descriptor(16500), route());
        let mut encoders = EncoderCache::default();
        let routes = [r.clone(), r, route()];
        for (i, route) in routes.into_iter().enumerate() {
            let mut w = PlanWriter::create(tmp.path().join(format!("{i}.plan")), 1 << 20, 1 << 20)
                .with_encoders(encoders);
            let cached = w.encoders.entries.len();
            push(
                &mut w,
                &d,
                heap(&d, 100 + i as u64, i as i32),
                Some(route.clone()),
            )
            .unwrap();
            let reused = cached - w.encoders.entries.len();
            assert_eq!((cached, reused), [(0, 0), (1, 1), (1, 0)][i]);
            let plan;
            (plan, encoders) = w.seal_keeping(0x2000, 42).unwrap();
            assert_eq!(encoders.entries.len(), 1);
            assert!(Arc::ptr_eq(&encoders.entries[0].route, &route));
            let mut rd = plan.replay().unwrap();
            let Some(PlanItem::Segment(s)) = rd.next_item().unwrap() else {
                panic!("one segment");
            };
            assert_eq!(rows(&s.chunks), [(Some(i as i32), 100 + i as u64, 7, 0)]);
        }
    }

    /// Oracle cells and strings survive the spool byte-identical
    #[test]
    fn oracle_and_string_chunks_round_trip() {
        let mut d = (*descriptor(16500)).clone();
        d.attributes[0].type_oid = 3614;
        d.attributes[0].type_len = -1;
        let d = Arc::new(d);
        let cells = |path: PathBuf, mem_max| {
            let mut w = PlanWriter::create(path, 1 << 20, mem_max);
            let table = w.table(&d, 0x40, || Some(route_to("String")));
            for (lsn, v) in [
                (
                    100,
                    Some(ColumnValue::PgPending {
                        type_oid: 3614,
                        raw: vec![1, 2, 3],
                    }),
                ),
                (110, Some(ColumnValue::Null)),
                (120, None),
            ] {
                let mut h = heap(&d, lsn, 0);
                h.new.as_mut().unwrap().columns = vec![v];
                w.push_cells(h, table).unwrap();
            }
            let mut rd = w.seal(0x2000, 42).unwrap().replay().unwrap();
            let Some(PlanItem::Segment(s)) = rd.next_item().unwrap() else {
                panic!("one segment");
            };
            s.chunks
                .iter()
                .flat_map(|c| c.buffers.iter().map(|b| format!("{:?}", b.wire())))
                .collect::<Vec<_>>()
        };
        let tmp = tempfile::tempdir().unwrap();
        let mem = cells(tmp.path().join("a.plan"), DEFAULT_PLAN_MEM_MAX);
        let file = cells(tmp.path().join("b.plan"), 0);
        assert!(
            mem[0].starts_with("Oracle([DiskRaw([1, 2, 3]), Default, Default])"),
            "{mem:?}"
        );
        assert_eq!(mem, file);
    }

    /// Delete sealed plan file on drop even without replay
    #[test]
    fn sealed_plan_drop_unlinks_file() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("1.plan");
        let mut w = PlanWriter::create(path.clone(), 1 << 20, 0);
        let d = descriptor(16500);
        push(&mut w, &d, heap(&d, 100, 10), Some(route())).unwrap();
        let plan = w.seal(0x2000, 42).unwrap();
        assert!(path.exists(), "mem_max 0 forced file mode");
        drop(plan);
        assert!(!path.exists(), "spool file removed on drop");
    }

    /// Replay a spilled plan after `tamper`, returning the first error
    fn replay_err(tamper: impl FnOnce(&mut SealedPlan, &mut Vec<u8>)) -> PlanSpoolError {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("1.plan");
        let d = descriptor(16500);
        let mut w = PlanWriter::create(path.clone(), 1 << 20, 0).with_segment_limits(1, usize::MAX);
        for (lsn, v) in [(100, 10), (110, 20)] {
            push(&mut w, &d, heap(&d, lsn, v), Some(route())).unwrap();
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
        // header 4, len 4, table 4, rows 4, columns 4, then a buffer tag
        let tag_byte = 20;
        let cases: Vec<(&str, PlanSpoolError)> = vec![
            ("bad plan magic", replay_err(|_, b| b[0] ^= 0xFF)),
            ("unsupported plan version", replay_err(|_, b| b[2] ^= 0xFF)),
            ("table id", replay_err(|p, _| p.tables.clear())),
            ("column buffer tag", replay_err(|_, b| b[tag_byte] = 0xFF)),
            (
                "left in plan",
                replay_err(|_, b| b[4..8].copy_from_slice(&u32::MAX.to_le_bytes())),
            ),
        ];
        for (want, err) in cases {
            assert!(
                matches!(err, PlanSpoolError::Format { .. }),
                "{want}: {err}"
            );
            assert!(err.to_string().contains(want), "{want}: {err}");
        }
        let tail = replay_err(|_, b| {
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
        let mut w = PlanWriter::create(path.clone(), 16, 0).with_segment_limits(1, usize::MAX);
        let d = descriptor(16500);
        let err = push(&mut w, &d, heap(&d, 100, 10), Some(route())).unwrap_err();
        assert!(matches!(err, PlanSpoolError::DiskBudget { .. }), "{err}");
        drop(w);
        assert!(!path.exists(), "abandoned plan unlinks");
    }
}
