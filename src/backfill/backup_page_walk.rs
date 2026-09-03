//! Page-walk Tap sink. Decodes user-heap tar entries through the same
//! heap decoder the WAL hot path uses (`decode_block_data`).
//! See [plans/bootstrap.md](../plans/bootstrap.md).
//!
//! ## V1 limits
//!
//! - No FPI replay on backup pages. Pages with `pd_lsn < start_lsn`
//!   captured mid-write get walked as-shipped. WAL records in
//!   `[start_lsn, end_lsn]` re-emit at higher `_lsn` and
//!   `ReplacingMergeTree(_lsn)` collapses the duplicate. Accepted
//!   brief-duplicate window, see [plans/bootstrap.md](../plans/bootstrap.md).
//! - With chunk storage enabled, walk `pg_toast_<relid>` pages and let
//!   bootstrap drain resolve deferred referrers

use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use async_trait::async_trait;
use thiserror::Error;
use tokio::sync::mpsc;
use walrus::pg::walparser::{Oid, RelFileNode};

use crate::backfill::backup_source::{
    BackupSink, EntrySink, FileAction, FileKind, FileMeta, StartInfo,
};
use crate::backfill::pg_path::{BaseRelFile, RelFork, parse_base_path};
use crate::decode::heap_decoder::{
    ColumnValue, CommittedTuple, DecodeError, DecodedHeap, DecodedTuple, HeapOp, decode_block_data,
};
use crate::schema::RelDescriptor;
use ahash::{HashMap, HashMapExt, HashSet};

/// Heap page size, PG compile-time, identical to wal-rus `BLOCK_SIZE`
pub const PAGE_BYTES: usize = 8192;
/// Blocks per relation segment: PG `RELSEG_SIZE` (pg_config.h), 1 GiB
/// default at 8 KiB pages. A `.N` file's first page is global block
/// `N * RELSEG_BLOCKS`
pub const RELSEG_BLOCKS: u32 = 131_072;
/// `PageHeaderData` size, 24 bytes since PG 8.x
pub const SIZE_OF_PAGE_HEADER: usize = 24;
pub const SIZE_OF_ITEM_ID: usize = 4;
/// `lp_flags` value for a live tuple slot
pub const LP_NORMAL: u8 = 1;

/// `pg_toast` regnamespace; TOAST tables ship as `pg_toast_<relid>`
pub const PG_TOAST_NS: &str = "pg_toast";

/// Body bytes a segment accumulates before its complete pages walk. Sets
/// the `spawn_blocking` grain and, with the channel depth, the pages in
/// flight; 16 pages decode in well under a millisecond, so the hop cost
/// amortizes without pinning much heap.
pub const SLAB_BYTES: usize = 16 * PAGE_BYTES;
/// Bootstrap tuple channel depth, in slabs. Small + bounded so a saturated
/// CH inserter parks the page walk (and its source fetch) rather than
/// buffering a whole relation in RAM. `SLAB_BYTES * CAP` bounds the heap
/// payload in flight per concurrent segment.
pub const BOOTSTRAP_TUPLE_CHANNEL_CAP: usize = 8;

#[derive(Debug, Error)]
pub enum PageWalkError {
    #[error("io: {0}")]
    Io(#[from] io::Error),
    #[error("bad page header at offset {offset}: lower={lower} upper={upper}")]
    BadPageHeader {
        offset: usize,
        lower: u16,
        upper: u16,
    },
    #[error("heap decode at lp_off={off} lp_len={len}: {err}")]
    HeapDecode {
        off: usize,
        len: usize,
        err: DecodeError,
    },
}

/// One decoded tuple from a backup page; a synthetic INSERT at
/// `_lsn = start_lsn`.
#[derive(Debug, Clone)]
pub struct BackfillTuple {
    pub rfn: RelFileNode,
    /// `t_xmin`; sequencing not load-bearing, every backfill row shares
    /// the same `_lsn = start_lsn`
    pub xid: u32,
    /// `t_xmax`; read with `infomask` by the backfill visibility gate
    /// ([`crate::decode::visibility`]), ignored on the greenfield path
    pub xmax: u32,
    /// `t_infomask` hint bits
    pub infomask: u16,
    pub source_lsn: u64,
    /// On-page TID; toast tuples become store rows keyed on it
    /// ([`crate::toast::ToastRow`])
    pub blkno: u32,
    pub offnum: u16,
    /// Attnum-1 indexed, matching `RelDescriptor.attributes`
    pub columns: Vec<Option<ColumnValue>>,
}

impl BackfillTuple {
    /// start_lsn rides as both source_lsn and commit_lsn so
    /// ReplacingMergeTree(_lsn) collapses duplicates the WAL decoder
    /// re-emits for records in [start_lsn, end_lsn]
    pub fn into_committed_insert(self) -> CommittedTuple {
        CommittedTuple {
            decoded: DecodedHeap {
                rfn: self.rfn,
                xid: self.xid,
                source_lsn: self.source_lsn,
                op: HeapOp::Insert,
                new: Some(DecodedTuple {
                    columns: self.columns,
                    partial: false,
                }),
                old: None,
            },
            commit_ts: 0,
            commit_lsn: self.source_lsn,
        }
    }
}

/// Resolved `(db_node, rel_node) → RelDescriptor` map. Seeded before
/// [`PageWalkSink`] runs from source PG's
/// `pg_class`/`pg_attribute`/`pg_type` for relations `oid >= 16384`.
#[derive(Debug, Default, Clone)]
pub struct CatalogMap {
    by_filenode: HashMap<(Oid, Oid), Arc<RelDescriptor>>,
}

impl CatalogMap {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&mut self, desc: Arc<RelDescriptor>) {
        self.by_filenode
            .insert((desc.rfn.db_node, desc.rfn.rel_node), desc);
    }

    pub fn get(&self, db_node: Oid, rel_node: Oid) -> Option<Arc<RelDescriptor>> {
        self.by_filenode.get(&(db_node, rel_node)).cloned()
    }

    pub fn descriptors(&self) -> impl Iterator<Item = &Arc<RelDescriptor>> {
        self.by_filenode.values()
    }

    pub fn is_toast(&self, db_node: Oid, rel_node: Oid) -> bool {
        self.by_filenode
            .get(&(db_node, rel_node))
            .is_some_and(|d| &*d.rel_name.namespace == PG_TOAST_NS)
    }

    pub fn len(&self) -> usize {
        self.by_filenode.len()
    }

    pub fn is_empty(&self) -> bool {
        self.by_filenode.is_empty()
    }
}

crate::atomic_stats! {
    /// Per-pump counters, operator-visible. Atomic so per-entry walkers
    /// share one handle instead of funnelling through the sink lock
    pub struct PageWalkStats {
        pub files_seen,
        pub files_walked,
        /// Filenode absent from catalog map, typically a race against the seed
        pub files_skipped_unknown_filenode,
        /// Filenode outside the mapped set, declined before any page decode
        pub files_skipped_unmapped,
        pub toast_files_observed,
        pub pages_walked,
        pub slots_seen,
        pub tuples_emitted,
        pub tuples_skipped_lp_flag,
        pub tuples_skipped_truncated,
        /// Trailing partial-page bytes; PG heap files are page-aligned so
        /// nonzero is anomalous
        pub tail_bytes_dropped,
        /// `PageWalker::walk_page` CPU, decode included
        pub decode_nanos,
        /// Blocked on a free tuple-channel slot: the bootstrap
        /// backpressure point, so this is emitter drain time seen by the walk
        pub channel_block_nanos,
    }
}

/// Plain tally one `walk_page` fills, published into [`PageWalkStats`] once
/// per slab. Per-tuple `fetch_add` on a line the reader task also touches
/// (the shared `Arc`'s refcount sits in the same allocation) costs more
/// than the decode it counts
#[derive(Debug, Default, Clone, Copy)]
pub struct PageWalkTally {
    pub pages_walked: u64,
    pub slots_seen: u64,
    pub tuples_emitted: u64,
    pub tuples_skipped_lp_flag: u64,
    pub tuples_skipped_truncated: u64,
}

impl PageWalkTally {
    pub fn publish(&self, stats: &PageWalkStats) {
        let add = |c: &AtomicU64, v: u64| {
            if v > 0 {
                c.fetch_add(v, Ordering::Relaxed);
            }
        };
        add(&stats.pages_walked, self.pages_walked);
        add(&stats.slots_seen, self.slots_seen);
        add(&stats.tuples_emitted, self.tuples_emitted);
        add(&stats.tuples_skipped_lp_flag, self.tuples_skipped_lp_flag);
        add(
            &stats.tuples_skipped_truncated,
            self.tuples_skipped_truncated,
        );
    }
}

/// Walks one 8 KiB page, emitting `BackfillTuple`s for `LP_NORMAL`
/// slots. Errors only on framing corruption; per-tuple decode failures
/// bump skip stats so a torn page can't abort the bootstrap.
pub struct PageWalker<'a> {
    pub rel: &'a RelDescriptor,
    pub source_lsn: u64,
}

impl<'a> PageWalker<'a> {
    pub fn new(rel: &'a RelDescriptor, source_lsn: u64) -> Self {
        Self { rel, source_lsn }
    }

    pub fn walk_page(
        &self,
        page: &[u8],
        block_no: u32,
        out: &mut Vec<BackfillTuple>,
        tally: &mut PageWalkTally,
    ) -> Result<(), PageWalkError> {
        if page.len() < SIZE_OF_PAGE_HEADER {
            return Err(PageWalkError::BadPageHeader {
                offset: 0,
                lower: 0,
                upper: 0,
            });
        }
        // PageHeaderData (PG src/include/storage/bufpage.h):
        //   pd_lsn 0..8  pd_checksum 8..10  pd_flags 10..12
        //   pd_lower 12..14  pd_upper 14..16  pd_special 16..18
        //   pd_pagesize 18..20  pd_prune 20..24
        let pd_lower = u16::from_le_bytes(page[12..14].try_into().unwrap());
        let pd_upper = u16::from_le_bytes(page[14..16].try_into().unwrap());
        if pd_upper == 0 && page.iter().all(|&byte| byte == 0) {
            tally.pages_walked += 1;
            return Ok(());
        }
        if pd_lower as usize == SIZE_OF_PAGE_HEADER && pd_upper as usize == PAGE_BYTES {
            // Fresh / empty page
            tally.pages_walked += 1;
            return Ok(());
        }
        if (pd_lower as usize) < SIZE_OF_PAGE_HEADER
            || (pd_upper as usize) > PAGE_BYTES
            || pd_lower > pd_upper
        {
            return Err(PageWalkError::BadPageHeader {
                offset: 0,
                lower: pd_lower,
                upper: pd_upper,
            });
        }
        let n_slots = (pd_lower as usize - SIZE_OF_PAGE_HEADER) / SIZE_OF_ITEM_ID;
        tally.pages_walked += 1;
        tally.slots_seen += n_slots as u64;
        out.reserve(n_slots);
        for i in 0..n_slots {
            let off = SIZE_OF_PAGE_HEADER + i * SIZE_OF_ITEM_ID;
            let raw = u32::from_le_bytes(page[off..off + 4].try_into().unwrap());
            // bit-packed: lp_off (15) | lp_flags (2) | lp_len (15)
            let lp_off = (raw & 0x7FFF) as usize;
            let lp_flags = ((raw >> 15) & 0x3) as u8;
            let lp_len = ((raw >> 17) & 0x7FFF) as usize;
            if lp_flags != LP_NORMAL {
                tally.tuples_skipped_lp_flag += 1;
                continue;
            }
            if lp_off + lp_len > PAGE_BYTES || lp_len == 0 {
                tally.tuples_skipped_truncated += 1;
                continue;
            }
            let tuple_bytes = &page[lp_off..lp_off + lp_len];
            match decode_on_page_tuple(tuple_bytes, self.rel) {
                Some((xid, xmax, infomask, columns)) => {
                    out.push(BackfillTuple {
                        rfn: self.rel.rfn,
                        xid,
                        xmax,
                        infomask,
                        source_lsn: self.source_lsn,
                        blkno: block_no,
                        // OffsetNumber is 1-based (PG off/itemid.h)
                        offnum: (i + 1) as u16,
                        columns,
                    });
                    tally.tuples_emitted += 1;
                }
                None => tally.tuples_skipped_truncated += 1,
            }
        }
        Ok(())
    }
}

/// Tuple bytes behind `offnum`'s line pointer, `None` unless the slot is
/// `LP_NORMAL` and in-bounds. Serves commit-time decode of a stashed
/// insert whose tuple rides only the FPI (`HEAP_INSERT_NO_LOGICAL` strips
/// `REGBUF_KEEP_DATA`, so a checkpoint mid-rewrite leaves no block data)
pub(crate) fn page_tuple_bytes(page: &[u8], offnum: u16) -> Option<&[u8]> {
    if offnum == 0 || page.len() < SIZE_OF_PAGE_HEADER {
        return None;
    }
    let pd_lower = u16::from_le_bytes(page[12..14].try_into().unwrap()) as usize;
    let off = SIZE_OF_PAGE_HEADER + (offnum as usize - 1) * SIZE_OF_ITEM_ID;
    if off + SIZE_OF_ITEM_ID > pd_lower.min(page.len()) {
        return None;
    }
    let raw = u32::from_le_bytes(page[off..off + 4].try_into().unwrap());
    let lp_off = (raw & 0x7FFF) as usize;
    let lp_flags = ((raw >> 15) & 0x3) as u8;
    let lp_len = ((raw >> 17) & 0x7FFF) as usize;
    if lp_flags != LP_NORMAL || lp_len == 0 || lp_off + lp_len > page.len() {
        return None;
    }
    Some(&page[lp_off..lp_off + lp_len])
}

/// Decode one on-page tuple into `(xmin, xmax, infomask, columns)`.
/// On-disk shape carries a full `HeapTupleHeaderData` (23 bytes); the
/// shared heap decoder consumes the `xl_heap_header` (5 bytes) shape PG
/// strips into WAL via
/// `XLogRegisterBufData(0, tup->t_data + SizeofHeapTupleHeader, ...)`,
/// so reshape before calling it. `None` on truncated / malformed header.
pub(crate) fn decode_on_page_tuple(
    tuple: &[u8],
    rel: &RelDescriptor,
) -> Option<(u32, u32, u16, Vec<Option<ColumnValue>>)> {
    use crate::decode::heap_decoder::{HEAP_HASNULL, HEAP_NATTS_MASK, SIZE_OF_HEAP_TUPLE_HEADER};

    if tuple.len() < SIZE_OF_HEAP_TUPLE_HEADER {
        return None;
    }
    // HeapTupleHeaderData (htup_details.h):
    //   t_xmin 0..4  t_xmax 4..8  t_field3 8..12  t_ctid 12..18
    //   t_infomask2 18..20  t_infomask 20..22
    //   t_hoff 22 (offset to user data, 8-byte aligned)
    //   t_bits[] 23.. (NULL bitmap, only if HEAP_HASNULL)
    let xmin = u32::from_le_bytes(tuple[0..4].try_into().unwrap());
    let xmax = u32::from_le_bytes(tuple[4..8].try_into().unwrap());
    let t_infomask2 = u16::from_le_bytes(tuple[18..20].try_into().unwrap());
    let t_infomask = u16::from_le_bytes(tuple[20..22].try_into().unwrap());
    let t_hoff = tuple[22] as usize;
    if t_hoff < SIZE_OF_HEAP_TUPLE_HEADER || t_hoff > tuple.len() {
        return None;
    }
    let natts = (t_infomask2 & HEAP_NATTS_MASK) as usize;
    let has_null = t_infomask & HEAP_HASNULL != 0;
    let bitmap_bytes = if has_null { natts.div_ceil(8) } else { 0 };
    if tuple.len() < SIZE_OF_HEAP_TUPLE_HEADER + bitmap_bytes {
        return None;
    }

    // Synthetic xl_heap_header-prefixed buffer:
    //   xl_heap_header (5): t_infomask2, t_infomask, t_hoff
    //   bitmap, then t_bits..t_hoff padding gap, then column data
    let mut wal_shaped = Vec::with_capacity(5 + (tuple.len() - SIZE_OF_HEAP_TUPLE_HEADER));
    wal_shaped.extend_from_slice(&t_infomask2.to_le_bytes());
    wal_shaped.extend_from_slice(&t_infomask.to_le_bytes());
    wal_shaped.push(t_hoff as u8);
    if bitmap_bytes > 0 {
        wal_shaped.extend_from_slice(
            &tuple[SIZE_OF_HEAP_TUPLE_HEADER..SIZE_OF_HEAP_TUPLE_HEADER + bitmap_bytes],
        );
    }
    let pad_start = SIZE_OF_HEAP_TUPLE_HEADER + bitmap_bytes;
    if pad_start < t_hoff {
        wal_shaped.extend_from_slice(&tuple[pad_start..t_hoff]);
    }
    wal_shaped.extend_from_slice(&tuple[t_hoff..]);

    match decode_block_data(&wal_shaped, rel) {
        Ok(d) => Some((xmin, xmax, t_infomask, d.columns)),
        Err(_) => None,
    }
}

/// Tap router: resolves a segment's descriptor at `begin` and hands back a
/// [`PageWalkEntry`] that owns the walk. Nothing here is on the body path,
/// so concurrent tar parts never queue behind each other.
pub struct PageWalkSink {
    catalog: CatalogMap,
    /// `StartInfo.start_lsn`, set once before any `begin`
    source_lsn: AtomicU64,
    /// Per-rfn `_lsn` tag overriding `source_lsn` (backup-sourced opt-in
    /// backfills tag each rel with its own boundary; greenfield leaves
    /// this empty). Keyed `(db_node, rel_node)`.
    lsn_overrides: HashMap<(Oid, Oid), u64>,
    pub stats: Arc<PageWalkStats>,
    /// Bounded ([`BOOTSTRAP_TUPLE_CHANNEL_CAP`]) slab channel: a full
    /// channel awaits in the entry sink, parking that entry's body read
    /// instead of buffering. Backpressure, not a buffer.
    out_tx: Option<mpsc::Sender<Vec<BackfillTuple>>>,
    /// Test-only capture, populated when `out_tx` is None
    captured: Arc<std::sync::Mutex<Vec<BackfillTuple>>>,
    /// Decode TOAST pages only when configured store consumes them
    store_toast: bool,
    /// Filenodes worth decoding, mapped relations plus their TOAST heaps.
    /// `None` taps everything the catalog map holds. A superset filter, not a
    /// second source of truth: the drain's `skip_initial` stays the authority
    tap_filenodes: Option<Arc<HashSet<(Oid, Oid)>>>,
    /// `pg_xact/` segments Tap into here for backfill visibility gate
    /// (plans/add_table.md); `None` (greenfield) keeps Skip.
    pg_xact: Option<Arc<std::sync::Mutex<crate::decode::visibility::PgXactAccum>>>,
    /// `pg_multixact/{offsets,members}` segments, for multixact xmax
    /// resolution in the same gate
    pg_multixact: Option<Arc<std::sync::Mutex<crate::decode::visibility::PgMultiXactAccum>>>,
}

/// Which SLRU accum a Tapped non-heap file installs into at `end()`.
enum SlruSegment {
    PgXact(u32),
    MultiOffsets(u32),
    MultiMembers(u32),
}

impl PageWalkSink {
    pub fn new(
        catalog: CatalogMap,
        out_tx: mpsc::Sender<Vec<BackfillTuple>>,
        store_toast: bool,
    ) -> Self {
        Self {
            catalog,
            source_lsn: AtomicU64::new(0),
            lsn_overrides: HashMap::new(),
            stats: Arc::new(PageWalkStats::default()),
            out_tx: Some(out_tx),
            captured: Arc::default(),
            store_toast,
            tap_filenodes: None,
            pg_xact: None,
            pg_multixact: None,
        }
    }

    /// Collect `pg_xact/` segments for the visibility gate.
    pub fn with_pg_xact_accum(
        mut self,
        accum: Arc<std::sync::Mutex<crate::decode::visibility::PgXactAccum>>,
    ) -> Self {
        self.pg_xact = Some(accum);
        self
    }

    /// Collect `pg_multixact/` segments for multixact xmax resolution.
    pub fn with_pg_multixact_accum(
        mut self,
        accum: Arc<std::sync::Mutex<crate::decode::visibility::PgMultiXactAccum>>,
    ) -> Self {
        self.pg_multixact = Some(accum);
        self
    }

    /// Decline filenodes outside `set` at `begin`. Bytes still drain off the
    /// wire; tuples never decode
    pub fn with_tap_filenodes(mut self, set: Arc<HashSet<(Oid, Oid)>>) -> Self {
        self.tap_filenodes = Some(set);
        self
    }

    /// Share the counter handle so a caller watches the walk live instead
    /// of waiting for the pump to hand its sink back.
    pub fn with_stats(mut self, stats: Arc<PageWalkStats>) -> Self {
        self.stats = stats;
        self
    }

    /// Tag listed rfns' rows with their own `_lsn` instead of `source_lsn`.
    pub fn with_lsn_overrides(mut self, overrides: HashMap<(Oid, Oid), u64>) -> Self {
        self.lsn_overrides = overrides;
        self
    }

    /// Test-mode: emitted tuples land in `captured` instead of the mpsc
    #[cfg(test)]
    pub fn new_capturing(catalog: CatalogMap) -> Self {
        Self {
            catalog,
            source_lsn: AtomicU64::new(0),
            lsn_overrides: HashMap::new(),
            stats: Arc::new(PageWalkStats::default()),
            out_tx: None,
            captured: Arc::default(),
            store_toast: false,
            tap_filenodes: None,
            pg_xact: None,
            pg_multixact: None,
        }
    }

    /// Test-mode capturing sink that also walks toast pages.
    #[cfg(test)]
    pub fn new_capturing_with_toast(catalog: CatalogMap) -> Self {
        Self {
            store_toast: true,
            ..Self::new_capturing(catalog)
        }
    }

    /// Tuples a capturing sink collected. Empty once `out_tx` is wired
    #[cfg(test)]
    pub fn captured(&self) -> Vec<BackfillTuple> {
        self.captured.lock().expect("captured lock").clone()
    }

    pub fn source_lsn(&self) -> u64 {
        self.source_lsn.load(Ordering::Relaxed)
    }

    fn classify(&self, meta: &FileMeta) -> Option<BaseRelFile> {
        if !matches!(meta.kind, FileKind::File) {
            return None;
        }
        parse_base_path(&meta.path)
    }

    fn classify_slru(&self, path: &std::path::Path) -> Option<SlruSegment> {
        if self.pg_xact.is_some()
            && let Some(segno) = crate::decode::visibility::pg_xact_segno_from_path(path)
        {
            return Some(SlruSegment::PgXact(segno));
        }
        if self.pg_multixact.is_some() {
            use crate::decode::visibility::MultiXactSegment;
            return match crate::decode::visibility::pg_multixact_segno_from_path(path)? {
                MultiXactSegment::Offsets(s) => Some(SlruSegment::MultiOffsets(s)),
                MultiXactSegment::Members(s) => Some(SlruSegment::MultiMembers(s)),
            };
        }
        None
    }
}

#[async_trait]
impl BackupSink for PageWalkSink {
    async fn start(&self, info: &StartInfo) -> io::Result<()> {
        self.source_lsn.store(info.start_lsn, Ordering::Relaxed);
        Ok(())
    }

    async fn begin(&self, meta: &FileMeta) -> io::Result<FileAction> {
        if matches!(meta.kind, FileKind::File)
            && let Some(slru) = self.classify_slru(&meta.path)
        {
            return Ok(FileAction::Tap(Box::new(SlruEntry {
                seg: slru,
                buf: Vec::with_capacity(meta.size as usize),
                pg_xact: self.pg_xact.clone(),
                pg_multixact: self.pg_multixact.clone(),
            })));
        }
        let Some(f) = self.classify(meta) else {
            // Not base/<db>/<filenode>; multiplex sink falls back to lander
            return Ok(FileAction::Skip);
        };
        if f.fork != RelFork::Main {
            // fsm/vm carry no tuples; keep them out of the TID-producing walk
            return Ok(FileAction::Skip);
        }
        self.stats.files_seen.fetch_add(1, Ordering::Relaxed);
        // Mapping filter first: an unmapped relation would decode every page
        // only for the drain to discard it into `unsupported_relations`
        if self
            .tap_filenodes
            .as_ref()
            .is_some_and(|set| !set.contains(&(f.db, f.filenode)))
        {
            self.stats
                .files_skipped_unmapped
                .fetch_add(1, Ordering::Relaxed);
            return Ok(FileAction::Skip);
        }
        let desc = self.catalog.get(f.db, f.filenode);
        let is_toast = self.catalog.is_toast(f.db, f.filenode);
        if is_toast {
            self.stats
                .toast_files_observed
                .fetch_add(1, Ordering::Relaxed);
        } else if desc.is_some() {
            self.stats.files_walked.fetch_add(1, Ordering::Relaxed);
        } else {
            // Filenode absent from map: seed race (greenfield) or non-opted
            // rel (filtered backfill pass, where this is most files). Skip
            // drains body without page buffering; mux honours the decline
            self.stats
                .files_skipped_unknown_filenode
                .fetch_add(1, Ordering::Relaxed);
            return Ok(FileAction::Skip);
        }
        let Some(desc) = desc else {
            // TOAST heap whose descriptor the map lacks; count pages, no walk
            return Ok(FileAction::Tap(Box::new(PageWalkEntry::counting(
                f.segno.saturating_mul(RELSEG_BLOCKS),
                self.stats.clone(),
            ))));
        };
        let lsn = self
            .lsn_overrides
            .get(&(desc.rfn.db_node, desc.rfn.rel_node))
            .copied()
            .unwrap_or_else(|| self.source_lsn());
        Ok(FileAction::Tap(Box::new(PageWalkEntry {
            block_no: f.segno.saturating_mul(RELSEG_BLOCKS),
            slab: Vec::with_capacity(SLAB_BYTES + PAGE_BYTES),
            spare: None,
            walk: (!is_toast || self.store_toast).then_some(WalkTarget { desc, lsn }),
            out: match &self.out_tx {
                Some(tx) => Out::Channel(tx.clone()),
                None => Out::Captured(self.captured.clone()),
            },
            stats: self.stats.clone(),
            pending: None,
        })))
    }
}

/// Descriptor a slab walks against. `None` on a TOAST heap the configured
/// store does not consume: its pages are counted, never decoded
struct WalkTarget {
    desc: Arc<RelDescriptor>,
    lsn: u64,
}

enum Out {
    Channel(mpsc::Sender<Vec<BackfillTuple>>),
    Captured(Arc<std::sync::Mutex<Vec<BackfillTuple>>>),
}

/// One user-heap segment's walk. Owns its slab, so page framing and decode
/// run without touching the router; the walk itself goes to `spawn_blocking`
/// so tar read and decompress overlap it.
pub struct PageWalkEntry {
    /// Block number within the relation, global across `.N` segments
    /// (seeded `segno * RELSEG_BLOCKS`): TOAST rows key on
    /// `(blkno, offnum)`, and all walk rows share one LSN, so per-file
    /// numbering would collide segment TIDs at equal version
    block_no: u32,
    /// Accumulates body bytes; complete pages walk in place out of it and
    /// only a sub-page remainder carries into the next slab
    slab: Vec<u8>,
    /// The slab last handed to the walk, back for reuse. Two buffers
    /// ping-pong, so a segment allocates twice however long it is
    spare: Option<Vec<u8>>,
    walk: Option<WalkTarget>,
    out: Out,
    stats: Arc<PageWalkStats>,
    /// Previous slab's walk, still running. One slab of overlap: the tar
    /// reader refills while the blocking pool decodes, so read and decode
    /// cost `max`, not `sum`
    pending: Option<tokio::task::JoinHandle<(Vec<u8>, Vec<BackfillTuple>)>>,
}

impl PageWalkEntry {
    /// Counts pages without decoding them
    fn counting(block_no: u32, stats: Arc<PageWalkStats>) -> Self {
        Self {
            block_no,
            slab: Vec::with_capacity(SLAB_BYTES + PAGE_BYTES),
            spare: None,
            walk: None,
            out: Out::Captured(Arc::default()),
            stats,
            pending: None,
        }
    }

    /// Collect the in-flight walk: recover its slab and ship its tuples.
    /// Shipping here is what carries emitter backpressure back to the read
    async fn join_pending(&mut self) -> io::Result<()> {
        let Some(handle) = self.pending.take() else {
            return Ok(());
        };
        let (walked, tuples) = handle.await.map_err(io::Error::other)?;
        self.spare = Some(walked);
        self.ship(tuples).await
    }

    /// Hand every complete page in the slab to the walk, carrying the
    /// sub-page remainder into the next slab.
    async fn drain_slab(&mut self) -> io::Result<()> {
        let full = self.slab.len() / PAGE_BYTES;
        if full == 0 {
            return Ok(());
        }
        let take = full * PAGE_BYTES;
        let first_block = self.block_no;
        self.block_no = self.block_no.saturating_add(full as u32);

        let Some(target) = self.walk.as_ref() else {
            self.stats
                .pages_walked
                .fetch_add(full as u64, Ordering::Relaxed);
            self.slab.drain(..take);
            return Ok(());
        };
        let desc = target.desc.clone();
        let lsn = target.lsn;

        // At most one walk in flight, so a segment holds two slabs, never more
        self.join_pending().await?;
        let mut next = self
            .spare
            .take()
            .unwrap_or_else(|| Vec::with_capacity(SLAB_BYTES + PAGE_BYTES));
        next.clear();
        next.extend_from_slice(&self.slab[take..]);
        let mut walked = std::mem::replace(&mut self.slab, next);
        walked.truncate(take);

        let stats = self.stats.clone();
        // Pure CPU over a byte slice; off the async runtime so the drain
        // stage it feeds keeps a worker to itself
        self.pending = Some(tokio::task::spawn_blocking(move || {
            let walker = PageWalker::new(&desc, lsn);
            let mut out = Vec::new();
            let mut tally = PageWalkTally::default();
            let started = Instant::now();
            for i in 0..full {
                let page = &walked[i * PAGE_BYTES..(i + 1) * PAGE_BYTES];
                let block = first_block.saturating_add(i as u32);
                if let Err(e) = walker.walk_page(page, block, &mut out, &mut tally) {
                    tracing::warn!(
                        target = "walshadow::backup_page_walk",
                        block,
                        error = %e,
                        "page walk skipped due to framing error"
                    );
                }
            }
            tally.publish(&stats);
            stats
                .decode_nanos
                .fetch_add(started.elapsed().as_nanos() as u64, Ordering::Relaxed);
            (walked, out)
        }));
        Ok(())
    }

    async fn ship(&self, tuples: Vec<BackfillTuple>) -> io::Result<()> {
        if tuples.is_empty() {
            return Ok(());
        }
        match &self.out {
            // Awaits a free slot when full: this is the bootstrap
            // backpressure point, parking this segment until CH drains
            Out::Channel(tx) => {
                let started = Instant::now();
                let res = tx.send(tuples).await.map_err(|e| {
                    io::Error::other(format!("PageWalkSink: emitter channel closed: {e}"))
                });
                self.stats
                    .channel_block_nanos
                    .fetch_add(started.elapsed().as_nanos() as u64, Ordering::Relaxed);
                res
            }
            Out::Captured(buf) => {
                buf.lock().expect("captured lock").extend(tuples);
                Ok(())
            }
        }
    }
}

#[async_trait]
impl EntrySink for PageWalkEntry {
    async fn chunk(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.slab.extend_from_slice(bytes);
        if self.slab.len() >= SLAB_BYTES {
            self.drain_slab().await?;
        }
        Ok(())
    }

    async fn end(mut self: Box<Self>) -> io::Result<()> {
        self.drain_slab().await?;
        self.join_pending().await?;
        let trailing = self.slab.len() as u64;
        if trailing > 0 {
            // PG heap files are page-aligned; trailing bytes are
            // zero-padding or anomalous, so count without decoding
            self.stats
                .tail_bytes_dropped
                .fetch_add(trailing, Ordering::Relaxed);
        }
        Ok(())
    }
}

/// `pg_xact` / `pg_multixact` segment: no page framing, bytes accumulate
/// whole and install into the visibility accum at `end`
struct SlruEntry {
    seg: SlruSegment,
    buf: Vec<u8>,
    pg_xact: Option<Arc<std::sync::Mutex<crate::decode::visibility::PgXactAccum>>>,
    pg_multixact: Option<Arc<std::sync::Mutex<crate::decode::visibility::PgMultiXactAccum>>>,
}

#[async_trait]
impl EntrySink for SlruEntry {
    async fn chunk(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.buf.extend_from_slice(bytes);
        Ok(())
    }

    async fn end(self: Box<Self>) -> io::Result<()> {
        let SlruEntry {
            seg,
            buf,
            pg_xact,
            pg_multixact,
        } = *self;
        match seg {
            SlruSegment::PgXact(segno) => {
                if let Some(a) = pg_xact {
                    a.lock()
                        .expect("pg_xact accum lock")
                        .insert_segment(segno, buf);
                }
            }
            SlruSegment::MultiOffsets(segno) => {
                if let Some(a) = pg_multixact {
                    a.lock()
                        .expect("pg_multixact accum lock")
                        .insert_offsets_segment(segno, buf);
                }
            }
            SlruSegment::MultiMembers(segno) => {
                if let Some(a) = pg_multixact {
                    a.lock()
                        .expect("pg_multixact accum lock")
                        .insert_members_segment(segno, buf);
                }
            }
        }
        Ok(())
    }
}

/// Test fixture `public.t(id int4)`, shared with `backfill_bootstrap`
#[cfg(test)]
pub(crate) fn make_rel() -> RelDescriptor {
    use crate::schema::{RelAttr, RelName, ReplIdent};
    RelDescriptor {
        rfn: RelFileNode {
            spc_node: 1663,
            db_node: 5,
            rel_node: 16400,
        },
        oid: 16400,
        toast_oid: 0,
        namespace_oid: 2200,
        rel_name: RelName::new("public", "t"),
        kind: 'r',
        persistence: 'p',
        replident: ReplIdent::Default { pk_attnums: None },
        attributes: vec![RelAttr {
            attnum: 1,
            name: "id".into(),
            type_oid: crate::schema::INT4OID,
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
    }
}

/// Test fixture: synthesise an 8 KiB heap page with one int4 tuple in
/// PG on-disk layout. Shared with `backfill_bootstrap`.
#[cfg(test)]
pub(crate) fn synth_single_tuple_page(value: i32) -> [u8; PAGE_BYTES] {
    let mut page = [0u8; PAGE_BYTES];
    // Tuple body: HeapTupleHeaderData (23) + 1 byte pad + 4-byte int
    let tuple_off = PAGE_BYTES - 32;
    page[tuple_off..tuple_off + 4].copy_from_slice(&99u32.to_le_bytes()); // t_xmin
    page[tuple_off + 18..tuple_off + 20].copy_from_slice(&1u16.to_le_bytes()); // t_infomask2, natts=1
    page[tuple_off + 20..tuple_off + 22].copy_from_slice(&0u16.to_le_bytes()); // t_infomask
    page[tuple_off + 22] = 24; // t_hoff = MAXALIGN(8) past 23-byte header
    page[tuple_off + 24..tuple_off + 28].copy_from_slice(&value.to_le_bytes());
    let tuple_len = 28u16;
    // pd_lower = header + one slot, pd_upper = tuple_off
    page[12..14].copy_from_slice(&((SIZE_OF_PAGE_HEADER + 4) as u16).to_le_bytes());
    page[14..16].copy_from_slice(&(tuple_off as u16).to_le_bytes());
    // ItemIdData slot 0: lp_off (15) | lp_flags (2) | lp_len (15)
    let raw = ((tuple_off as u32) & 0x7FFF)
        | (((LP_NORMAL as u32) & 0x3) << 15)
        | (((tuple_len as u32) & 0x7FFF) << 17);
    page[SIZE_OF_PAGE_HEADER..SIZE_OF_PAGE_HEADER + SIZE_OF_ITEM_ID]
        .copy_from_slice(&raw.to_le_bytes());
    page
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::backfill::backup_source::EndInfo;

    fn ld(a: &AtomicU64) -> u64 {
        a.load(Ordering::Relaxed)
    }

    /// `begin` must tap; hands back the owned entry sink
    async fn tap(sink: &PageWalkSink, meta: &FileMeta) -> Box<dyn EntrySink> {
        match sink.begin(meta).await.unwrap() {
            FileAction::Tap(e) => e,
            other => panic!("expected Tap for {}, got {other:?}", meta.path.display()),
        }
    }

    fn heap_meta(path: &str) -> FileMeta {
        FileMeta {
            path: PathBuf::from(path),
            size: PAGE_BYTES as u64,
            mode: 0o600,
            kind: FileKind::File,
        }
    }
    use crate::schema::RelName;
    use std::path::PathBuf;

    #[tokio::test]
    async fn source_lsn_reflects_start_info() {
        let sink = PageWalkSink::new_capturing(CatalogMap::new());
        assert_eq!(sink.source_lsn(), 0);
        sink.start(&StartInfo {
            start_lsn: 0xABCD_1234,
            timeline: 1,
            tablespaces: Vec::new(),
        })
        .await
        .unwrap();
        assert_eq!(sink.source_lsn(), 0xABCD_1234);
    }

    #[test]
    fn page_walker_emits_single_tuple() {
        let rel = make_rel();
        let walker = PageWalker::new(&rel, 0xABCD);
        let page = synth_single_tuple_page(42);
        let mut out = Vec::new();
        let mut tally = PageWalkTally::default();
        walker.walk_page(&page, 0, &mut out, &mut tally).unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].source_lsn, 0xABCD);
        assert_eq!(out[0].xid, 99);
        assert_eq!(out[0].columns.len(), 1);
        assert!(matches!(out[0].columns[0], Some(ColumnValue::Int4(42))));
        assert_eq!(tally.pages_walked, 1);
        assert_eq!(tally.tuples_emitted, 1);
    }

    #[test]
    fn page_walker_handles_empty_page() {
        let rel = make_rel();
        let walker = PageWalker::new(&rel, 0);
        let mut page = [0u8; PAGE_BYTES];
        // Fresh-init: pd_lower at header end, pd_upper at page end
        page[12..14].copy_from_slice(&(SIZE_OF_PAGE_HEADER as u16).to_le_bytes());
        page[14..16].copy_from_slice(&(PAGE_BYTES as u16).to_le_bytes());
        let mut out = Vec::new();
        let mut tally = PageWalkTally::default();
        walker.walk_page(&page, 0, &mut out, &mut tally).unwrap();
        assert!(out.is_empty());
        assert_eq!(tally.pages_walked, 1);
        assert_eq!(tally.slots_seen, 0);
    }

    #[test]
    fn page_walker_handles_zero_page() {
        let rel = make_rel();
        let walker = PageWalker::new(&rel, 0);
        let mut out = Vec::new();
        let mut tally = PageWalkTally::default();
        walker
            .walk_page(&[0; PAGE_BYTES], 0, &mut out, &mut tally)
            .unwrap();
        assert!(out.is_empty());
        assert_eq!(tally.pages_walked, 1);
        assert_eq!(tally.slots_seen, 0);
    }

    #[test]
    fn page_walker_skips_lp_dead_slots() {
        let rel = make_rel();
        let walker = PageWalker::new(&rel, 0);
        let mut page = synth_single_tuple_page(7);
        // Flip lp_flags LP_NORMAL (1) -> LP_DEAD (3)
        let raw = u32::from_le_bytes(
            page[SIZE_OF_PAGE_HEADER..SIZE_OF_PAGE_HEADER + 4]
                .try_into()
                .unwrap(),
        );
        let lp_off = raw & 0x7FFF;
        let lp_len = (raw >> 17) & 0x7FFF;
        let new_raw = lp_off | (3u32 << 15) | (lp_len << 17);
        page[SIZE_OF_PAGE_HEADER..SIZE_OF_PAGE_HEADER + 4].copy_from_slice(&new_raw.to_le_bytes());
        let mut out = Vec::new();
        let mut tally = PageWalkTally::default();
        walker.walk_page(&page, 0, &mut out, &mut tally).unwrap();
        assert!(out.is_empty());
        assert_eq!(tally.tuples_skipped_lp_flag, 1);
        assert_eq!(tally.tuples_emitted, 0);
    }

    #[test]
    fn page_walker_rejects_bad_header_bounds() {
        let rel = make_rel();
        let walker = PageWalker::new(&rel, 0);
        let mut page = [0u8; PAGE_BYTES];
        // pd_lower > pd_upper
        page[12..14].copy_from_slice(&(PAGE_BYTES as u16).to_le_bytes());
        page[14..16].copy_from_slice(&(SIZE_OF_PAGE_HEADER as u16).to_le_bytes());
        let mut out = Vec::new();
        let mut tally = PageWalkTally::default();
        let err = walker.walk_page(&page, 0, &mut out, &mut tally);
        assert!(matches!(err, Err(PageWalkError::BadPageHeader { .. })));
    }

    #[test]
    fn catalog_map_routes_filenodes_and_marks_toast() {
        let mut m = CatalogMap::new();
        let mut rel = make_rel();
        rel.rel_name = RelName::new("public", &rel.rel_name.name);
        m.insert(Arc::new(rel.clone()));
        let mut toast_rel = rel.clone();
        toast_rel.rfn.rel_node = 99999;
        toast_rel.rel_name = RelName::new("pg_toast", &toast_rel.rel_name.name);
        m.insert(Arc::new(toast_rel));

        assert!(m.get(5, 16400).is_some());
        assert!(!m.is_toast(5, 16400));
        assert!(m.get(5, 99999).is_some());
        assert!(m.is_toast(5, 99999));
        assert_eq!(m.len(), 2);
    }

    #[tokio::test]
    async fn pagewalk_sink_decodes_one_page_via_chunk_stream() {
        let mut catalog = CatalogMap::new();
        catalog.insert(Arc::new(make_rel()));
        let sink = PageWalkSink::new_capturing(catalog);
        sink.start(&StartInfo {
            start_lsn: 0x1234_5678,
            timeline: 1,
            tablespaces: Vec::new(),
        })
        .await
        .unwrap();
        let mut entry = tap(&sink, &heap_meta("base/5/16400")).await;
        let page = synth_single_tuple_page(99);
        // Two chunks exercise the buffer-across-chunk path
        entry.chunk(&page[..4096]).await.unwrap();
        entry.chunk(&page[4096..]).await.unwrap();
        entry.end().await.unwrap();
        sink.finish(&EndInfo {
            end_lsn: 0,
            timeline: 1,
        })
        .await
        .unwrap();

        let captured = sink.captured();
        assert_eq!(captured.len(), 1);
        assert_eq!(captured[0].source_lsn, 0x1234_5678);
        assert_eq!(captured[0].xid, 99);
        assert!(matches!(
            captured[0].columns[0],
            Some(ColumnValue::Int4(99))
        ));
        assert_eq!(ld(&sink.stats.files_seen), 1);
        assert_eq!(ld(&sink.stats.files_walked), 1);
        assert_eq!(ld(&sink.stats.pages_walked), 1);
        assert_eq!(ld(&sink.stats.tuples_emitted), 1);
    }

    /// `.N` segment tuples get global block numbers: same local page +
    /// offnum in the base file and `.1` must yield distinct TIDs, else
    /// toast rows collide at equal walk LSN and merge keeps either.
    #[tokio::test]
    async fn pagewalk_sink_seeds_segment_block_numbers() {
        let mut catalog = CatalogMap::new();
        catalog.insert(Arc::new(make_rel()));
        let sink = PageWalkSink::new_capturing(catalog);
        sink.start(&StartInfo {
            start_lsn: 0x1000,
            timeline: 1,
            tablespaces: Vec::new(),
        })
        .await
        .unwrap();
        for path in ["base/5/16400", "base/5/16400.1"] {
            let mut entry = tap(&sink, &heap_meta(path)).await;
            entry.chunk(&synth_single_tuple_page(7)).await.unwrap();
            entry.end().await.unwrap();
        }
        let captured = sink.captured();
        assert_eq!(captured.len(), 2);
        assert_eq!(
            (captured[0].blkno, captured[0].offnum),
            (0, 1),
            "base file walks from block 0"
        );
        assert_eq!(
            (captured[1].blkno, captured[1].offnum),
            (RELSEG_BLOCKS, 1),
            "segment 1 walks from its global block"
        );
    }

    /// fsm/vm forks carry no tuples; they must not enter the walk.
    #[tokio::test]
    async fn pagewalk_sink_skips_non_main_forks() {
        let mut catalog = CatalogMap::new();
        catalog.insert(Arc::new(make_rel()));
        let sink = PageWalkSink::new_capturing(catalog);
        sink.start(&StartInfo {
            start_lsn: 0x1000,
            timeline: 1,
            tablespaces: Vec::new(),
        })
        .await
        .unwrap();
        for path in ["base/5/16400_fsm", "base/5/16400_vm", "base/5/16400_vm.1"] {
            assert!(
                matches!(
                    sink.begin(&heap_meta(path)).await.unwrap(),
                    FileAction::Skip
                ),
                "{path}"
            );
        }
        assert_eq!(ld(&sink.stats.files_seen), 0);
    }

    #[tokio::test]
    async fn pagewalk_sink_skips_when_filenode_absent_from_catalog() {
        let sink = PageWalkSink::new_capturing(CatalogMap::new());
        let m = sink.classify(&FileMeta {
            path: PathBuf::from("base/5/16400"),
            size: 0,
            mode: 0,
            kind: FileKind::File,
        });
        assert_eq!(
            m,
            Some(BaseRelFile {
                db: 5,
                filenode: 16400,
                fork: RelFork::Main,
                segno: 0,
            })
        );

        sink.start(&StartInfo {
            start_lsn: 0,
            timeline: 1,
            tablespaces: Vec::new(),
        })
        .await
        .unwrap();
        // Skip so source drains body without chunk() delivery; filtered
        // backfill pass relies on this for every non-opted rel
        assert!(matches!(
            sink.begin(&heap_meta("base/5/16400")).await.unwrap(),
            FileAction::Skip
        ));
        assert!(sink.captured().is_empty());
        assert_eq!(ld(&sink.stats.files_seen), 1);
        assert_eq!(ld(&sink.stats.files_skipped_unknown_filenode), 1);
        assert_eq!(ld(&sink.stats.tuples_emitted), 0);
        assert_eq!(ld(&sink.stats.pages_walked), 0);
    }

    /// Item 3.4: an unmapped relation's bytes drain off the wire without a
    /// page decode. Before the filter the walk decoded every seeded rel and
    /// the drain discarded them into `unsupported_relations`.
    #[tokio::test]
    async fn tap_filenode_filter_declines_unmapped_relations() {
        let mut catalog = CatalogMap::new();
        let mapped = make_rel();
        let mut unmapped = make_rel();
        unmapped.rfn.rel_node = 16401;
        unmapped.oid = 16401;
        catalog.insert(Arc::new(mapped));
        catalog.insert(Arc::new(unmapped));
        let sink = PageWalkSink::new_capturing(catalog)
            .with_tap_filenodes(Arc::new([(5, 16400)].into_iter().collect()));
        sink.start(&StartInfo {
            start_lsn: 0x1000,
            timeline: 1,
            tablespaces: Vec::new(),
        })
        .await
        .unwrap();

        let mut entry = tap(&sink, &heap_meta("base/5/16400")).await;
        entry.chunk(&synth_single_tuple_page(1)).await.unwrap();
        entry.end().await.unwrap();
        assert!(matches!(
            sink.begin(&heap_meta("base/5/16401")).await.unwrap(),
            FileAction::Skip
        ));

        assert_eq!(sink.captured().len(), 1, "only the mapped rel decoded");
        assert_eq!(ld(&sink.stats.files_seen), 2);
        assert_eq!(ld(&sink.stats.files_walked), 1);
        assert_eq!(ld(&sink.stats.files_skipped_unmapped), 1);
        assert_eq!(ld(&sink.stats.pages_walked), 1);
    }

    /// A slab boundary must not lose the page it straddles, nor renumber
    /// blocks: 64 KiB chunks against a 16-page slab put the split mid-page.
    #[tokio::test]
    async fn slab_boundary_keeps_every_page_and_block_number() {
        let mut catalog = CatalogMap::new();
        catalog.insert(Arc::new(make_rel()));
        let sink = PageWalkSink::new_capturing(catalog);
        sink.start(&StartInfo {
            start_lsn: 0x1000,
            timeline: 1,
            tablespaces: Vec::new(),
        })
        .await
        .unwrap();

        // 40 pages, one tuple each, delivered in 6000-byte chunks so no
        // chunk edge lands on a page or slab edge
        let pages = 40usize;
        let mut body = Vec::with_capacity(pages * PAGE_BYTES);
        for i in 0..pages {
            body.extend_from_slice(&synth_single_tuple_page(i as i32));
        }
        let mut entry = tap(&sink, &heap_meta("base/5/16400")).await;
        for chunk in body.chunks(6000) {
            entry.chunk(chunk).await.unwrap();
        }
        entry.end().await.unwrap();

        let captured = sink.captured();
        assert_eq!(captured.len(), pages);
        assert_eq!(ld(&sink.stats.pages_walked), pages as u64);
        assert_eq!(ld(&sink.stats.tail_bytes_dropped), 0);
        for (i, t) in captured.iter().enumerate() {
            assert_eq!(t.blkno, i as u32, "page {i} kept its block number");
            assert!(matches!(t.columns[0], Some(ColumnValue::Int4(v)) if v == i as i32));
        }
    }

    #[tokio::test]
    async fn pagewalk_sink_rejects_non_base_paths() {
        let sink = PageWalkSink::new_capturing(CatalogMap::new());
        sink.start(&StartInfo {
            start_lsn: 0,
            timeline: 1,
            tablespaces: Vec::new(),
        })
        .await
        .unwrap();
        assert!(matches!(
            sink.begin(&FileMeta {
                path: PathBuf::from("pg_control"),
                size: 0,
                mode: 0,
                kind: FileKind::File,
            })
            .await
            .unwrap(),
            FileAction::Skip
        ));
    }

    /// object_store fan-out interleaves begin/chunk across concurrent
    /// parts. Each entry owns its walk state, so a later begin cannot
    /// clobber an in-flight entry and misframe pages against the wrong
    /// relation.
    #[tokio::test]
    async fn interleaved_entries_keep_independent_state() {
        let mut catalog = CatalogMap::new();
        let rel_a = make_rel();
        let mut rel_b = make_rel();
        rel_b.rfn.rel_node = 16401;
        rel_b.oid = 16401;
        catalog.insert(Arc::new(rel_a));
        catalog.insert(Arc::new(rel_b));
        let sink = PageWalkSink::new_capturing(catalog);
        sink.start(&StartInfo {
            start_lsn: 0x1000,
            timeline: 1,
            tablespaces: Vec::new(),
        })
        .await
        .unwrap();

        // Both open before either streams, chunks arrive reversed, ends
        // interleave: worst case for a shared slot
        let mut a = tap(&sink, &heap_meta("base/5/16400")).await;
        let mut b = tap(&sink, &heap_meta("base/5/16401")).await;
        b.chunk(&synth_single_tuple_page(200)).await.unwrap();
        a.chunk(&synth_single_tuple_page(100)).await.unwrap();
        a.end().await.unwrap();
        b.end().await.unwrap();

        // Each tuple carries its own file's rfn + value; a shared slot
        // would attribute both to rel B
        let captured = sink.captured();
        let mut by_rel: HashMap<Oid, i32> = HashMap::new();
        for t in &captured {
            if let Some(Some(ColumnValue::Int4(v))) = t.columns.first() {
                by_rel.insert(t.rfn.rel_node, *v);
            }
        }
        assert_eq!(captured.len(), 2);
        assert_eq!(by_rel.get(&16400), Some(&100), "entry A decoded vs rel A");
        assert_eq!(by_rel.get(&16401), Some(&200), "entry B decoded vs rel B");
    }
}
