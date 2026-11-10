//! Plan one committed transaction before executing it.
//!
//! Seal routed rows into a segment at row and byte limits and before every
//! control or truncate; each segment dispatches as one seq. A deferring
//! writer keeps validated records as WAL bytes so encoding runs once
//! dispatched, off the commit path; others encode per-table column chunks
//! while planning. Keep segments in memory until their size exceeds memory
//! limit, then encode and write them to a spool file. Keep
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
use crate::decode::datum::{Datum, DatumRef};
use crate::decode::heap_decoder::{HeapOp, TupleView, inflate, missing_datum, view_heap_record};
use crate::emit::ch_emitter::{
    Append, EncodedChunk, OP_DELETE, OP_INSERT, OP_UPDATE, RowStamp, TableEncoder, TablePlan,
};
use crate::emit::route::RouteSnapshot;
use crate::schema::RelDescriptor;
use crate::xact::spill::{self, Cursor, DescribedInput, IO_CHUNK, SpillError, Truncate};
use crate::xact::xact_buffer::{DrainEntry, ToastRowBatch};
use foldhash::HashMap;

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
pub struct PlannedTruncate {
    pub xid: u32,
    pub source_lsn: u64,
    /// Index into plan's `tables`
    pub table: u32,
}

/// Rows sealed together; executor dispatches one seq per segment
pub struct Segment {
    pub body: SegmentBody,
    /// Destination rows, delete markers for key changes included
    pub rows: u64,
    /// Allocated column bytes or held record bytes, the admission a
    /// dispatched segment holds
    pub bytes: usize,
}

pub enum SegmentBody {
    Encoded(Vec<EncodedChunk>),
    /// Validated records still WAL bytes, encoded once dispatched
    Pending(PendingRows),
}

impl Segment {
    #[cfg(any(test, feature = "test-support"))]
    /// [`Self::into_chunks`], panicking on a codec failure
    pub fn chunks(self) -> Vec<EncodedChunk> {
        self.into_chunks().expect("segment encodes")
    }

    /// Column chunks, encoding pending records here
    pub fn into_chunks(self) -> std::result::Result<Vec<EncodedChunk>, EmitterError> {
        match self.body {
            SegmentBody::Encoded(chunks) => Ok(chunks),
            SegmentBody::Pending(rows) => {
                let mut b = ChunkBuilder::new(usize::MAX, usize::MAX);
                rows.encode_into(&mut b)?;
                Ok(b.take().into_iter().map(|(_, c)| c).collect())
            }
        }
    }
}

/// Routed heap record planned without encoding: routed, validated and
/// detoasted, so encoding can only fail on the value codec
pub struct PendingHeap {
    table: u32,
    route: Arc<RouteSnapshot>,
    input: Box<DescribedInput>,
    /// Resolved external values per image as `[new, old]`, empty when no
    /// image has any
    patches: Vec<[Vec<(usize, Datum)>; 2]>,
}

impl PendingHeap {
    /// Encode every image into `b` under `key`, returning rows appended
    fn encode_into<K: Copy + PartialEq>(
        &self,
        b: &mut ChunkBuilder<K>,
        key: K,
        commit_ts: i64,
    ) -> std::result::Result<usize, EmitterError> {
        let rel = &self.input.descriptor;
        let lsn = self.input.input.source_lsn;
        let images = view_heap_record(self.input.input.record_bytes(), rel)
            .map_err(|e| EmitterError::Type(format!("record at {lsn:#X}: {e}")))?;
        let mut rows = 0;
        for (i, h) in images.iter().enumerate() {
            let (new_patch, old_patch) = self
                .patches
                .get(i)
                .map_or((&[][..], &[][..]), |[n, o]| (&n[..], &o[..]));
            let change = ViewChange {
                op: h.op,
                xid: self.input.input.xid,
                source_lsn: lsn,
                new: h.new.as_ref(),
                old: h.old.as_ref(),
                new_patch,
                old_patch,
            };
            rows += b.push_views(key, rel, &self.route, change, commit_ts)?;
        }
        Ok(rows)
    }
}

/// One segment's pending records
#[derive(Default)]
pub struct PendingRows {
    commit_ts: i64,
    heaps: Vec<PendingHeap>,
}

impl PendingRows {
    /// Encode into `b`, keying tables by relation and route identity so
    /// several segments' rows of one table share a chunk. Returns rows
    /// appended
    pub fn encode_into(
        &self,
        b: &mut ChunkBuilder<(usize, usize)>,
    ) -> std::result::Result<usize, EmitterError> {
        self.heaps.iter().try_fold(0, |rows, h| {
            let key = (
                Arc::as_ptr(&h.input.descriptor) as usize,
                Arc::as_ptr(&h.route) as usize,
            );
            Ok(rows + h.encode_into(b, key, self.commit_ts)?)
        })
    }
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
    Segment {
        rows: u64,
        bytes: usize,
        body: StepBody,
    },
    Control {
        event: DrainEntry,
        row_idx: usize,
    },
    Truncate {
        truncate: PlannedTruncate,
        upto: usize,
    },
}

enum StepBody {
    Pending(PendingRows),
    Encoded(Vec<(u32, EncodedChunk)>),
    /// `frames` chunks written to the spool file
    Spooled {
        frames: usize,
    },
}

/// Per-table encoder of the open segment
struct OpenTable<K> {
    key: K,
    rel: Arc<RelDescriptor>,
    route: Arc<RouteSnapshot>,
    enc: TableEncoder,
    /// `(seq, rows)` of the open chunk, in append order, when tagging
    seqs: SeqRows,
}

/// `(seq, rows)` runs one chunk carries, in append order
pub type SeqRows = Vec<(u64, u64)>;

/// Chunk sealed out of a [`ChunkBuilder`], with the seqs its rows belong to
type Taken<K> = (K, EncodedChunk, SeqRows);

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

/// Encodes row changes into per-table column chunks of one open segment,
/// sealing a table's chunk early when an oracle frame fills. `K` names the
/// caller's table
pub struct ChunkBuilder<K> {
    alloc: Allocator,
    open: Vec<OpenTable<K>>,
    encoders: EncoderCache,
    /// Chunks of the open segment sealed early at an oracle frame limit
    sealed: Vec<Taken<K>>,
    /// Seq rows append under, see [`Self::tag`]
    seq: Option<u64>,
    rows: usize,
    segment_rows: usize,
    segment_bytes: usize,
}

impl<K: Copy + PartialEq> ChunkBuilder<K> {
    /// Segment is due once its rows or allocated bytes reach these
    pub fn new(segment_rows: usize, segment_bytes: usize) -> Self {
        Self {
            alloc: Allocator::global(&mimalloc::MiMalloc),
            open: Vec::new(),
            encoders: EncoderCache::default(),
            sealed: Vec::new(),
            seq: None,
            rows: 0,
            segment_rows: segment_rows.max(1),
            segment_bytes: segment_bytes.max(1),
        }
    }

    /// Reuse encoders an earlier builder returned
    pub fn with_encoders(mut self, encoders: EncoderCache) -> Self {
        self.encoders = encoders;
        self
    }

    /// Tag rows appended from here on with `seq`, so one chunk can carry
    /// rows of several seqs; see [`Self::take_runs`]
    pub fn tag(&mut self, seq: u64) {
        self.seq = Some(seq);
    }

    /// Rows in the open segment
    pub fn rows(&self) -> usize {
        self.rows
    }

    /// Whether the open segment reached its row or byte limit
    pub fn due(&self) -> bool {
        self.rows >= self.segment_rows || self.open_bytes() >= self.segment_bytes
    }

    fn open_bytes(&self) -> usize {
        self.open.iter().map(|o| o.enc.approx_bytes).sum::<usize>()
            + self
                .sealed
                .iter()
                .map(|(_, c, _)| c.allocated_bytes())
                .sum::<usize>()
    }

    /// Encode one row into `key`'s open chunk, sealing it first when an
    /// oracle frame fills
    pub(crate) fn append(
        &mut self,
        key: K,
        rel: &Arc<RelDescriptor>,
        route: &Arc<RouteSnapshot>,
        mut row: impl FnMut(&mut TableEncoder) -> std::result::Result<Append, EmitterError>,
    ) -> std::result::Result<(), EmitterError> {
        let i = match self.open.iter().position(|o| o.key == key) {
            Some(i) => i,
            None => {
                let enc = match self.encoders.take(rel, route) {
                    Some(enc) => enc,
                    None => TableEncoder::new(TablePlan::for_route(self.alloc, rel, route)?)?,
                };
                self.open.push(OpenTable {
                    key,
                    rel: rel.clone(),
                    route: route.clone(),
                    enc,
                    seqs: SeqRows::new(),
                });
                self.open.len() - 1
            }
        };
        if row(&mut self.open[i].enc)? == Append::Full {
            let chunk = self.take_chunk(i);
            self.sealed.push(chunk);
            row(&mut self.open[i].enc)?;
        }
        if let Some(seq) = self.seq {
            let seqs = &mut self.open[i].seqs;
            match seqs.last_mut() {
                Some((s, n)) if *s == seq => *n += 1,
                _ => seqs.push((seq, 1)),
            }
        }
        self.rows += 1;
        Ok(())
    }

    /// Encode one row change from tuple images in place: drop deletes an
    /// append-only route has nowhere to land, split key changes into a delete
    /// marker plus the new row. Returns rows appended
    pub fn push_views(
        &mut self,
        key: K,
        rel: &Arc<RelDescriptor>,
        route: &Arc<RouteSnapshot>,
        change: ViewChange<'_, '_>,
        commit_ts: i64,
    ) -> std::result::Result<usize, EmitterError> {
        if route.drops_deletes() && change.op == HeapOp::Delete {
            return Ok(0);
        }
        let leaf = route.partition.as_ref().map(|p| p.leaf);
        let stamp = |source_lsn, op_code| RowStamp {
            source_lsn,
            xid: change.xid,
            commit_ts,
            op_code,
            leaf,
        };
        let mut rows = 0;
        if let Some(old) = moved_key(rel, route, &change) {
            let stamp = stamp(change.source_lsn - 1, OP_DELETE);
            self.append(key, rel, route, |enc| {
                enc.append_view(rel, &old.datums, change.old_patch, &stamp)
            })?;
            rows += 1;
        }
        let (side, patch) = match change.op {
            HeapOp::Delete => (change.old, change.old_patch),
            _ => (change.new, change.new_patch),
        };
        let stamp = stamp(change.source_lsn, op_code(change.op)?);
        let datums = side.map_or(&[][..], |t| &t.datums);
        self.append(key, rel, route, |enc| {
            enc.append_view(rel, datums, patch, &stamp)
        })?;
        Ok(rows + 1)
    }

    fn take_chunk(&mut self, i: usize) -> Taken<K> {
        let o = &mut self.open[i];
        let (buffers, rows) = o.enc.take_block();
        let chunk = EncodedChunk {
            rel: o.rel.clone(),
            route: o.route.clone(),
            buffers,
            rows,
        };
        (o.key, chunk, std::mem::take(&mut o.seqs))
    }

    /// Seal the open segment's chunks, early-sealed ones first
    fn take_tagged(&mut self) -> Vec<Taken<K>> {
        let mut chunks = std::mem::take(&mut self.sealed);
        for i in 0..self.open.len() {
            if self.open[i].enc.rows > 0 {
                chunks.push(self.take_chunk(i));
            }
        }
        self.rows = 0;
        chunks
    }

    /// Seal the open segment's chunks, early-sealed ones first
    pub fn take(&mut self) -> Vec<(K, EncodedChunk)> {
        self.take_tagged()
            .into_iter()
            .map(|(k, c, _)| (k, c))
            .collect()
    }

    /// [`Self::take`] with each chunk's rows per [`Self::tag`]ged seq
    pub fn take_runs(&mut self) -> Vec<(EncodedChunk, SeqRows)> {
        self.take_tagged()
            .into_iter()
            .map(|(_, c, seqs)| (c, seqs))
            .collect()
    }

    /// Park open encoders, emptied by [`Self::take`], so later keys start
    /// fresh. A control entry may change routes, and the cache matches
    /// route identity
    pub fn clear(&mut self) {
        debug_assert_eq!(self.rows, 0, "clear after take");
        for o in self.open.drain(..) {
            self.encoders.put(o.rel, o.route, o.enc);
        }
    }

    /// Emptied encoders for the next builder
    pub fn into_encoders(mut self) -> EncoderCache {
        self.clear();
        self.encoders
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
    tables: Vec<PlanTable>,
    /// Tables since the last control entry, which may change routes
    live_from: usize,
    /// Table of the previous row, checked before `table_ids`
    last_table: u32,
    /// Live table ids by `(descriptor Arc identity, valid_from)`; the held
    /// Arc in `tables` keeps each pointer live and unique. Built once a
    /// second live table appears, so single-relation plans never hash
    table_ids: Option<HashMap<(usize, u64), u32>>,
    chunks: ChunkBuilder<u32>,
    /// Keep routed records as WAL bytes until dispatch, see [`Self::defers`]
    defer: bool,
    /// Open segment's records when deferring
    pending: Vec<PendingHeap>,
    pending_rows: usize,
    pending_bytes: usize,
    commit_ts: i64,
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
            tables: Vec::new(),
            live_from: 0,
            last_table: 0,
            table_ids: None,
            chunks: ChunkBuilder::new(usize::MAX, usize::MAX),
            defer: false,
            pending: Vec::new(),
            pending_rows: 0,
            pending_bytes: 0,
            commit_ts: 0,
            steps: Vec::new(),
            row_batches: Vec::new(),
            routed_rows: 0,
            deletes_discarded: 0,
            frame: Vec::new(),
        }
    }

    /// Seal a segment once its open rows or bytes reach these
    pub fn with_segment_limits(mut self, rows: usize, bytes: usize) -> Self {
        self.chunks.segment_rows = rows.max(1);
        self.chunks.segment_bytes = bytes.max(1);
        self
    }

    /// Commit timestamp stamped on every planned row
    pub fn for_commit(mut self, commit_ts: i64) -> Self {
        self.commit_ts = commit_ts;
        self
    }

    /// Plan routed records without encoding them, so encoding runs once
    /// dispatched instead of on the commit path
    pub fn deferring(mut self) -> Self {
        self.defer = true;
        self
    }

    /// Whether the next record should go through [`Self::push_pending`]:
    /// only memory-resident plans defer, a spilled one writes chunks
    pub fn defers(&self) -> bool {
        self.defer && self.file.is_none()
    }

    /// Reuse encoders an earlier plan returned through [`Self::seal_keeping`]
    pub fn with_encoders(mut self, encoders: EncoderCache) -> Self {
        self.chunks.encoders = encoders;
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

    #[cfg(any(test, feature = "test-support"))]
    /// [`Self::push_views`] over an owned change's record bytes
    pub fn push_cells(
        &mut self,
        decoded: crate::decode::heap_decoder::DecodedHeap,
        table: u32,
    ) -> Result<()> {
        let input = spill::DescribedInput::from(crate::decode::heap_decoder::DescribedHeap {
            decoded,
            descriptor: self.tables[table as usize].descriptor.clone(),
            descriptor_valid_from: 0,
        });
        let images = crate::decode::heap_decoder::view_heap_record(
            input.input.record_bytes(),
            &input.descriptor,
        )
        .expect("formed record views");
        let defer = self.defers();
        let mut rows = 0;
        for h in &images {
            let change = ViewChange {
                op: h.op,
                xid: input.input.xid,
                source_lsn: input.input.source_lsn,
                new: h.new.as_ref(),
                old: h.old.as_ref(),
                new_patch: &[],
                old_patch: &[],
            };
            if defer {
                rows += self.count_change(&change, table)?;
            } else {
                self.push_views(change, table)?;
            }
        }
        drop(images);
        if defer {
            self.push_pending(table, Box::new(input), Vec::new(), rows)?;
        }
        Ok(())
    }

    /// Encode one change from tuple images in place, see
    /// [`ChunkBuilder::push_views`]
    pub fn push_views(&mut self, change: ViewChange<'_, '_>, table: u32) -> Result<()> {
        let t = &mut self.tables[table as usize];
        t.heaps += 1;
        let route = t.route.as_ref().expect("routed table");
        let rows = self
            .chunks
            .push_views(table, &t.descriptor, route, change, self.commit_ts)?;
        self.pushed(rows)
    }

    /// Count one change [`Self::push_pending`] will carry, validating its
    /// operation. Returns the rows it encodes to
    pub fn count_change(&mut self, change: &ViewChange<'_, '_>, table: u32) -> Result<usize> {
        let t = &mut self.tables[table as usize];
        t.heaps += 1;
        let route = t.route.as_ref().expect("routed table");
        let rows = if route.drops_deletes() && change.op == HeapOp::Delete {
            0
        } else {
            op_code(change.op)?;
            1 + usize::from(moved_key(&t.descriptor, route, change).is_some())
        };
        self.count(rows);
        Ok(rows)
    }

    /// Carry a record whose images [`Self::count_change`] counted as `rows`
    pub fn push_pending(
        &mut self,
        table: u32,
        input: Box<DescribedInput>,
        patches: Vec<[Vec<(usize, Datum)>; 2]>,
        rows: usize,
    ) -> Result<()> {
        if rows == 0 {
            return Ok(());
        }
        let route = self.tables[table as usize].route.clone();
        self.pending_bytes += size_of::<PendingHeap>()
            + input.approx_bytes()
            + patches
                .iter()
                .flatten()
                .flatten()
                .map(|(_, d)| size_of::<(usize, Datum)>() + d.approx_bytes())
                .sum::<usize>();
        self.pending_rows += rows;
        self.pending.push(PendingHeap {
            table,
            route: route.expect("routed table"),
            input,
            patches,
        });
        if self.pending_rows >= self.chunks.segment_rows
            || self.pending_bytes >= self.chunks.segment_bytes
        {
            self.seal_segment()?;
        }
        Ok(())
    }

    /// Count `rows` of one change, none for a dropped delete
    fn count(&mut self, rows: usize) {
        if rows == 0 {
            self.deletes_discarded += 1;
        }
        self.routed_rows += rows as u64;
    }

    /// Count `rows` just encoded and seal a due segment
    fn pushed(&mut self, rows: usize) -> Result<()> {
        self.count(rows);
        if self.chunks.due() {
            self.seal_segment()?;
        }
        Ok(())
    }

    /// Close the open segment into a step, spilling once memory runs out
    fn seal_segment(&mut self) -> Result<()> {
        let (rows, bytes, body) = if self.pending.is_empty() {
            let chunks = self.chunks.take();
            if chunks.is_empty() {
                return Ok(());
            }
            let rows = chunks.iter().map(|(_, c)| c.rows as u64).sum();
            let bytes = chunks.iter().map(|(_, c)| c.allocated_bytes()).sum();
            (rows, bytes, StepBody::Encoded(chunks))
        } else {
            let rows = std::mem::take(&mut self.pending_rows) as u64;
            let bytes = std::mem::take(&mut self.pending_bytes);
            let records = PendingRows {
                commit_ts: self.commit_ts,
                heaps: std::mem::take(&mut self.pending),
            };
            (rows, bytes, StepBody::Pending(records))
        };
        let mut step = Step::Segment { rows, bytes, body };
        if self.file.is_none() && self.bytes + bytes as u64 <= self.mem_max.min(self.cap) {
            self.bytes += bytes as u64;
        } else {
            self.spill()?;
            self.spool_step(&mut step)?;
        }
        self.steps.push(step);
        Ok(())
    }

    /// Write a resident segment's chunks to the spool file, encoding
    /// pending records first
    fn spool_step(&mut self, step: &mut Step) -> Result<()> {
        let Step::Segment { bytes, body, .. } = step else {
            return Ok(());
        };
        let chunks = match std::mem::replace(body, StepBody::Spooled { frames: 0 }) {
            StepBody::Encoded(chunks) => chunks,
            StepBody::Pending(records) => {
                for h in &records.heaps {
                    h.encode_into(&mut self.chunks, h.table, records.commit_ts)?;
                }
                let chunks = self.chunks.take();
                *bytes = chunks.iter().map(|(_, c)| c.allocated_bytes()).sum();
                chunks
            }
            spooled @ StepBody::Spooled { .. } => {
                *body = spooled;
                return Ok(());
            }
        };
        for (table, c) in &chunks {
            self.write_chunk(*table, c)?;
        }
        *body = StepBody::Spooled {
            frames: chunks.len(),
        };
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
        let written = steps.iter_mut().try_for_each(|step| self.spool_step(step));
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
    pub fn push_truncate(&mut self, t: Truncate, table: u32, upto: usize) -> Result<()> {
        self.seal_segment()?;
        self.tables[table as usize].heaps += 1;
        let truncate = PlannedTruncate {
            xid: t.xid,
            source_lsn: t.source_lsn,
            table,
        };
        self.steps.push(Step::Truncate { truncate, upto });
        Ok(())
    }

    /// Pin a control entry after every row planned so far, preserving the
    /// drain's event-before-heap tie break. `row_idx` is the plan-global
    /// mirror-row position it fences
    pub fn push_control(&mut self, event: DrainEntry, row_idx: usize) -> Result<()> {
        self.seal_segment()?;
        self.chunks.clear();
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
        let encoders = self.chunks.into_encoders();
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

/// One row change read from tuple images in place. Patches replace image
/// attributes by index: resolved TOAST values, and values past the
/// descriptor's attributes
#[derive(Clone, Copy)]
pub struct ViewChange<'v, 'a> {
    pub op: HeapOp,
    pub xid: u32,
    pub source_lsn: u64,
    pub new: Option<&'v TupleView<'a>>,
    pub old: Option<&'v TupleView<'a>>,
    pub new_patch: &'v [(usize, Datum)],
    pub old_patch: &'v [(usize, Datum)],
}

/// Whether an UPDATE changes primary/index key, or any column under
/// `REPLICA IDENTITY FULL` without a primary key. PostgreSQL default/index
/// identity logs old values only when key changes or contains TOASTed values
fn key_moved<'v>(
    rel: &RelDescriptor,
    change: &ViewChange<'v, '_>,
    old: &TupleView<'_>,
    new: &TupleView<'_>,
) -> bool {
    let hot = match change.op {
        HeapOp::Update => false,
        HeapOp::HotUpdate => true,
        _ => return false,
    };
    let differs = |a: usize| {
        let (Some(&o), Some(&n), Some(att)) =
            (old.datums.get(a), new.datums.get(a), rel.attributes.get(a))
        else {
            return false;
        };
        let patched = |patch: &'v [(usize, Datum)], d| {
            patch
                .iter()
                .find(|(i, _)| *i == a)
                .map_or(d, |(_, v)| v.view())
        };
        datums_differ(
            att,
            patched(change.old_patch, o),
            patched(change.new_patch, n),
        )
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

/// Old image when `change` moves its key and the route keeps deletes: the
/// change encodes as a delete marker plus the new row
fn moved_key<'v, 'a>(
    rel: &RelDescriptor,
    route: &RouteSnapshot,
    change: &ViewChange<'v, 'a>,
) -> Option<&'v TupleView<'a>> {
    let (Some(old), Some(new)) = (change.old, change.new) else {
        return None;
    };
    (!route.drops_deletes() && key_moved(rel, change, old, new)).then_some(old)
}

/// Stored-byte inequality, as PostgreSQL's `datumIsEqual` judges index
/// keys; absent never differs. Floats compare by value, so NaN differs from
/// itself and signed zeros match
fn datums_differ(att: &crate::schema::RelAttr, o: DatumRef<'_>, n: DatumRef<'_>) -> bool {
    use crate::schema::{FLOAT4OID, FLOAT8OID};
    match (o, n) {
        (DatumRef::Absent, _) | (_, DatumRef::Absent) => false,
        (DatumRef::Missing, d) | (d, DatumRef::Missing) => {
            datums_differ(att, missing_datum(att), d)
        }
        (DatumRef::Compressed(c), d) | (d, DatumRef::Compressed(c)) => {
            inflate(c).is_none_or(|raw| datums_differ(att, DatumRef::Varlena(&raw), d))
        }
        (DatumRef::Null | DatumRef::Dropped, DatumRef::Null | DatumRef::Dropped) => false,
        (DatumRef::Fixed(a), DatumRef::Fixed(b)) => match att.type_oid {
            FLOAT4OID => float_bits_differ::<4>(a, b, |w| f64::from(f32::from_le_bytes(w))),
            FLOAT8OID => float_bits_differ::<8>(a, b, f64::from_le_bytes),
            _ => a != b,
        },
        (
            DatumRef::Varlena(a) | DatumRef::Cstring(a) | DatumRef::Text(a),
            DatumRef::Varlena(b) | DatumRef::Cstring(b) | DatumRef::Text(b),
        ) => a != b,
        (DatumRef::External(a), DatumRef::External(b)) => a != b,
        _ => true,
    }
}

fn float_bits_differ<const N: usize>(a: &[u8], b: &[u8], value: fn([u8; N]) -> f64) -> bool {
    match (a.try_into(), b.try_into()) {
        (Ok(a), Ok(b)) => value(a) != value(b),
        _ => a != b,
    }
}

pub(crate) fn op_code(op: HeapOp) -> std::result::Result<i8, EmitterError> {
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
        truncate: PlannedTruncate,
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
            Step::Truncate { truncate, upto } => PlanItem::Truncate { truncate, upto },
            Step::Segment { rows, bytes, body } => {
                let body = match body {
                    StepBody::Pending(records) => SegmentBody::Pending(records),
                    StepBody::Encoded(chunks) => {
                        SegmentBody::Encoded(chunks.into_iter().map(|(_, c)| c).collect())
                    }
                    StepBody::Spooled { frames } => {
                        let Some(f) = self.input.as_mut() else {
                            return Err(PlanSpoolError::Format {
                                detail: "spilled segment without spool file".into(),
                            });
                        };
                        SegmentBody::Encoded(
                            (0..frames)
                                .map(|_| read_frame(f, &self.tables))
                                .collect::<Result<_>>()?,
                        )
                    }
                };
                PlanItem::Segment(Segment { body, rows, bytes })
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
    use crate::decode::datum::{Datum, build};
    use crate::decode::heap_decoder::{CommittedTuple, DecodedHeap, DecodedTuple};
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

    /// Index-key equality as `datumIsEqual` judges stored bytes
    #[test]
    fn datums_differ_matrix() {
        use crate::decode::heap_decoder::ToastPointer;
        use crate::schema::FLOAT4OID;
        let mut att = descriptor(1).attributes[0].clone();
        let one = 1i32.to_le_bytes();
        let two = 2i32.to_le_bytes();
        let toast = |va_valueid| ToastPointer {
            va_rawsize: 8,
            va_extinfo: 4,
            va_valueid,
            va_toastrelid: 9,
        };
        let cases = [
            (DatumRef::Absent, DatumRef::Fixed(&one), false),
            (DatumRef::Missing, DatumRef::Null, false),
            (DatumRef::Fixed(&one), DatumRef::Missing, true),
            (DatumRef::Null, DatumRef::Dropped, false),
            (DatumRef::Fixed(&one), DatumRef::Fixed(&two), true),
            (DatumRef::Text(b"a"), DatumRef::Varlena(b"a"), false),
            (DatumRef::Cstring(b"a"), DatumRef::Varlena(b"b"), true),
            (
                DatumRef::External(toast(1)),
                DatumRef::External(toast(1)),
                false,
            ),
            (
                DatumRef::External(toast(1)),
                DatumRef::External(toast(2)),
                true,
            ),
            (DatumRef::Null, DatumRef::Fixed(&one), true),
        ];
        for (o, n, want) in cases {
            assert_eq!(datums_differ(&att, o, n), want, "{o:?} vs {n:?}");
        }
        att.type_oid = FLOAT4OID;
        let f = |v: f32| v.to_le_bytes();
        let (zero, neg_zero, nan) = (f(0.0), f(-0.0), f(f32::NAN));
        assert!(!datums_differ(
            &att,
            DatumRef::Fixed(&zero),
            DatumRef::Fixed(&neg_zero)
        ));
        assert!(datums_differ(
            &att,
            DatumRef::Fixed(&nan),
            DatumRef::Fixed(&nan)
        ));
        assert!(datums_differ(
            &att,
            DatumRef::Fixed(&zero),
            DatumRef::Fixed(&[0])
        ));
    }

    /// Delete marker splits off when UPDATE moves a key, or any column under
    /// `REPLICA IDENTITY FULL` without a primary key
    #[test]
    fn key_moved_matrix() {
        use crate::decode::heap_decoder::{form_block_data, view_block_data};
        use crate::schema::ReplIdent;
        let mut rel = (*descriptor(1)).clone();
        let mut v = rel.attributes[0].clone();
        (v.attnum, v.name, v.type_oid) = (2, "v".into(), crate::schema::TEXTOID);
        (v.type_name, v.type_byval, v.type_len) = ("text".into(), false, -1);
        rel.attributes.push(v);
        let row =
            |id: i32, v: &str| form_block_data(&[], &[Some(build::int4(id)), Some(build::text(v))]);
        let pk = || ReplIdent::Default {
            pk_attnums: Some(vec![1]),
        };
        let full_pk = || ReplIdent::Full {
            pk_attnums: Some(vec![1]),
        };
        let full = || ReplIdent::Full { pk_attnums: None };
        let index = ReplIdent::UsingIndex {
            index_oid: 9,
            key_attnums: vec![2],
        };
        let cases = [
            (pk(), HeapOp::Update, (1, ""), true),
            // PostgreSQL logs TOASTed keys even when unchanged
            (pk(), HeapOp::Update, (2, ""), false),
            (full_pk(), HeapOp::Update, (2, "a"), false),
            (full_pk(), HeapOp::HotUpdate, (1, "a"), false),
            (full(), HeapOp::HotUpdate, (2, "a"), true),
            (full(), HeapOp::Update, (2, "b"), false),
            (full(), HeapOp::Delete, (2, "a"), false),
            (index, HeapOp::Update, (2, "a"), true),
        ];
        let new = row(2, "b");
        for (replident, op, (id, v), split) in cases {
            rel.replident = replident;
            let old = row(id, v);
            let (old, new) = (
                view_block_data(&old, &rel).unwrap(),
                view_block_data(&new, &rel).unwrap(),
            );
            let change = ViewChange {
                op,
                xid: 7,
                source_lsn: 0x100,
                new: Some(&new),
                old: Some(&old),
                new_patch: &[],
                old_patch: &[],
            };
            let moved = key_moved(&rel, &change, &old, &new);
            assert_eq!(moved, split, "{:?} {op:?} ({id}, {v:?})", rel.replident);
        }
    }

    fn heap(rel: &Arc<RelDescriptor>, lsn: u64, v: i32) -> DecodedHeap {
        DecodedHeap {
            rfn: rel.rfn,
            xid: 7,
            source_lsn: lsn,
            op: HeapOp::Insert,
            new: Some(DecodedTuple {
                columns: vec![Some(build::int4(v))],
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
            let truncate = Truncate {
                xid: 7,
                source_lsn: 115,
                descriptor: d2.clone(),
                descriptor_valid_from: 0x40,
            };
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
                        let chunks = s.chunks();
                        for c in &chunks {
                            assert!(Arc::ptr_eq(&c.rel, &d1), "descriptor survives plan");
                            assert!(Arc::ptr_eq(&c.route, &r));
                        }
                        format!("{:?}", rows(&chunks))
                    }
                    PlanItem::Truncate { truncate: t, .. } => {
                        assert!(rd.tables[t.table as usize].route.is_none());
                        format!("t{}", t.source_lsn)
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
                columns: vec![Some(build::int4(v))],
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
            assert_eq!(s.rows, want.len() as u64);
            assert_eq!(rows(&s.chunks()), want);
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
            seen.extend(rows(&s.chunks()).into_iter().map(|r| r.0.unwrap()));
        }
        assert_eq!(seen, (0..64).collect::<Vec<_>>());
    }

    /// Deferred records replay pending and encode to the rows inline
    /// planning writes; a spill encodes them into frames
    #[test]
    fn deferred_records_encode_like_inline() {
        let tmp = tempfile::tempdir().unwrap();
        let d = descriptor(16500);
        let r = route();
        let plan = |name: &str, defer: bool, mem_max: u64| {
            let mut w = PlanWriter::create(tmp.path().join(name), 1 << 20, mem_max)
                .with_segment_limits(2, usize::MAX)
                .for_commit(42);
            if defer {
                w = w.deferring();
            }
            for v in 0..5 {
                push(&mut w, &d, heap(&d, 100 + v as u64, v), Some(r.clone())).unwrap();
            }
            let mut rd = w.seal(0x2000, 42).unwrap().replay().unwrap();
            let (mut out, mut pending) = (Vec::new(), 0);
            while let Some(PlanItem::Segment(s)) = rd.next_item().unwrap() {
                pending += usize::from(matches!(s.body, SegmentBody::Pending(_)));
                let want = s.rows;
                let got = rows(&s.chunks());
                assert_eq!(got.len() as u64, want);
                out.push(got);
            }
            (out, pending)
        };
        let (inline, none) = plan("inline", false, 1 << 20);
        assert_eq!((inline.len(), none), (3, 0));
        assert_eq!(plan("deferred", true, 1 << 20), (inline.clone(), 3));
        assert_eq!(plan("spilled", true, 0), (inline, 0));
    }

    /// Rows tagged with seqs share one chunk per table, each run counted
    #[test]
    fn tagged_rows_share_chunk_per_table() {
        let (d1, d2, r) = (descriptor(1), descriptor(2), route());
        let mut b = ChunkBuilder::new(usize::MAX, usize::MAX);
        for (seq, rel) in [(5, &d1), (5, &d2), (6, &d1), (6, &d1), (7, &d2)] {
            b.tag(seq);
            let h = heap(rel, 100 + seq, seq as i32);
            b.append(Arc::as_ptr(rel) as usize, rel, &r, |enc| {
                enc.append_routed(
                    &CommittedTuple {
                        decoded: h.clone(),
                        commit_ts: 42,
                        commit_lsn: 0,
                    },
                    &r.mapping,
                    OP_INSERT,
                    None,
                )
            })
            .unwrap();
        }
        let runs: Vec<_> = b
            .take_runs()
            .into_iter()
            .map(|(c, seqs)| (c.rel.oid, c.rows, seqs))
            .collect();
        assert_eq!(
            runs,
            [(1, 3, vec![(5, 1), (6, 2)]), (2, 2, vec![(5, 1), (7, 1)])]
        );
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
            let cached = w.chunks.encoders.entries.len();
            push(
                &mut w,
                &d,
                heap(&d, 100 + i as u64, i as i32),
                Some(route.clone()),
            )
            .unwrap();
            let reused = cached - w.chunks.encoders.entries.len();
            assert_eq!((cached, reused), [(0, 0), (1, 1), (1, 0)][i]);
            let plan;
            (plan, encoders) = w.seal_keeping(0x2000, 42).unwrap();
            assert_eq!(encoders.entries.len(), 1);
            assert!(Arc::ptr_eq(&encoders.entries[0].route, &route));
            let mut rd = plan.replay().unwrap();
            let Some(PlanItem::Segment(s)) = rd.next_item().unwrap() else {
                panic!("one segment");
            };
            assert_eq!(rows(&s.chunks()), [(Some(i as i32), 100 + i as u64, 7, 0)]);
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
                (100, Some(build::bytes(vec![1, 2, 3]))),
                (110, Some(Datum::Null)),
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
            s.chunks()
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
