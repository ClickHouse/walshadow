//! Page-walk Tap sink + supporting walker types.
//!
//! Sinks user-heap tar entries (`base/<dbid>/<filenode>` with
//! filenode >= 16384) via [`FileAction::Tap`]. As bytes arrive in
//! `chunk()` callbacks, the sink accumulates them 8 KiB at a time
//! (PG's heap page size), walks each full page's `ItemIdData` slots,
//! and decodes live tuples through the **same heap decoder the WAL
//! hot path uses** ([`crate::heap_decoder::decode_block_data`]).
//! Output is one [`BackfillTuple`] per `LP_NORMAL` slot; the sink
//! hands them off through an mpsc to an async drain task that pumps
//! the CH emitter. See [plans/bootstrap.md](../plans/bootstrap.md).
//!
//! ## V1 limits
//!
//! - **No FPI replay on backup pages.** Pages with `pd_lsn < start_lsn`
//!   captured mid-write get walked as-shipped. WAL records in
//!   `[start_lsn, end_lsn]` that update the same tuples re-emit at
//!   higher `_lsn` and `ReplacingMergeTree(_lsn)` collapses the
//!   duplicate. Accepted brief-duplicate window, see
//!   [plans/bootstrap.md](../plans/bootstrap.md).
//! - **TOAST-spilled columns surface as `ColumnValue::PgPending`.**
//!   Inline-stored varlena columns decode through the heap decoder's type
//!   matrix; external TOAST chunks aren't reassembled here. The
//!   chunk-and-assemble logic is WAL-shared work, not 2A-specific.
//! - **`pg_toast_<relid>` tar entries are observed but not decoded.**
//!   The page count surfaces via stats; the chunk projection is
//!   deferred to the WAL-side TOAST decoder.

use std::collections::HashMap;
use std::io;
use std::sync::Arc;

use thiserror::Error;
use tokio::sync::mpsc;
use wal_rs::pg::walparser::{Oid, RelFileNode};

use crate::backup_sink::parse_base_path;
use crate::backup_source::{BackupSink, EndInfo, FileAction, FileKind, FileMeta, StartInfo};
use crate::heap_decoder::{ColumnValue, DecodeError, decode_block_data};
use crate::shadow_catalog::RelDescriptor;

/// Heap page size — PG compile-time, identical to wal-rs `BLOCK_SIZE`.
pub const PAGE_BYTES: usize = 8192;
/// `PageHeaderData` size — 24 bytes since PG 8.x.
pub const SIZE_OF_PAGE_HEADER: usize = 24;
/// `ItemIdData` size — 4 bytes, packed.
pub const SIZE_OF_ITEM_ID: usize = 4;
/// `lp_flags` LP_NORMAL — slot carries a live tuple.
pub const LP_NORMAL: u8 = 1;

/// `pg_toast` regnamespace name. TOAST tables ship under this
/// namespace with `pg_toast_<relid>` names; pages from them are
/// recognised but their `(chunk_id, chunk_seq, chunk_data)` shape
/// isn't decoded in V1.
pub const PG_TOAST_NS: &str = "pg_toast";

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

/// One decoded tuple from a backup page. Mirrors the shape the
/// CH emitter consumes (a synthetic INSERT at `_lsn = start_lsn`).
#[derive(Debug, Clone)]
pub struct BackfillTuple {
    pub rfn: RelFileNode,
    /// `t_xmin` of the on-page tuple. Sequencing isn't load-bearing
    /// since `_lsn = start_lsn` is the same for every backfill row.
    pub xid: u32,
    /// `start_lsn` from `StartInfo`. Every backfill row uses this.
    pub source_lsn: u64,
    /// Attnum-1 indexed columns, matching `RelDescriptor.attributes`.
    pub columns: Vec<Option<ColumnValue>>,
}

/// Resolved `(db_node, rel_node) → RelDescriptor` map. Populated
/// before [`PageWalkSink`] runs, typically by querying source PG's
/// `pg_class`/`pg_attribute`/`pg_type` for relations with
/// `oid >= 16384`. The bootstrap orchestrator owns the seed; the
/// sink consumes immutable lookups.
#[derive(Debug, Default, Clone)]
pub struct CatalogMap {
    by_filenode: HashMap<(Oid, Oid), Arc<RelDescriptor>>,
    /// `pg_toast_<relid>` tables — pages observed but not decoded
    /// in V1.
    toast_filenodes: HashMap<(Oid, Oid), Oid>,
}

impl CatalogMap {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&mut self, desc: Arc<RelDescriptor>) {
        let key = (desc.rfn.db_node, desc.rfn.rel_node);
        if desc.namespace_name == PG_TOAST_NS {
            self.toast_filenodes.insert(key, desc.oid);
        }
        self.by_filenode.insert(key, desc);
    }

    pub fn get(&self, db_node: Oid, rel_node: Oid) -> Option<Arc<RelDescriptor>> {
        self.by_filenode.get(&(db_node, rel_node)).cloned()
    }

    pub fn is_toast(&self, db_node: Oid, rel_node: Oid) -> bool {
        self.toast_filenodes.contains_key(&(db_node, rel_node))
    }

    pub fn len(&self) -> usize {
        self.by_filenode.len()
    }

    pub fn is_empty(&self) -> bool {
        self.by_filenode.is_empty()
    }
}

/// Per-pump counters. Operator-visible; not load-bearing.
#[derive(Debug, Default, Clone)]
pub struct PageWalkStats {
    /// User-heap files observed (begin'd) regardless of decode.
    pub files_seen: u64,
    /// User-heap files whose pages were walked (filenode resolved to
    /// a `RelDescriptor`).
    pub files_walked: u64,
    /// User-heap files skipped because the catalog map didn't carry
    /// their filenode — typically a race against the catalog seed.
    pub files_skipped_unknown_filenode: u64,
    /// TOAST relation files observed; pages noted, contents deferred.
    pub toast_files_observed: u64,
    pub pages_walked: u64,
    pub slots_seen: u64,
    pub tuples_emitted: u64,
    pub tuples_skipped_lp_flag: u64,
    pub tuples_skipped_truncated: u64,
    /// Page-buffer bytes dropped because chunk arrived between pages
    /// (PG never emits non-page-aligned data — anomalous).
    pub tail_bytes_dropped: u64,
}

/// Walk one 8 KiB page slice, emit `BackfillTuple`s for `LP_NORMAL`
/// slots. Returns errors only on framing corruption (bad page header
/// bounds); per-tuple decode failures bump skip stats so a single
/// torn page can't abort the whole bootstrap.
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
        out: &mut Vec<BackfillTuple>,
        stats: &mut PageWalkStats,
    ) -> Result<(), PageWalkError> {
        if page.len() < SIZE_OF_PAGE_HEADER {
            return Err(PageWalkError::BadPageHeader {
                offset: 0,
                lower: 0,
                upper: 0,
            });
        }
        // PageHeaderData (PG src/include/storage/bufpage.h):
        //   pd_lsn       0..8  (XLogRecPtr)
        //   pd_checksum  8..10
        //   pd_flags    10..12
        //   pd_lower    12..14
        //   pd_upper    14..16
        //   pd_special  16..18
        //   pd_pagesize 18..20
        //   pd_prune    20..24
        let pd_lower = u16::from_le_bytes(page[12..14].try_into().unwrap());
        let pd_upper = u16::from_le_bytes(page[14..16].try_into().unwrap());
        if pd_lower as usize == SIZE_OF_PAGE_HEADER && pd_upper as usize == PAGE_BYTES {
            // Fresh / empty page — no slots
            stats.pages_walked += 1;
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
        stats.pages_walked += 1;
        for i in 0..n_slots {
            stats.slots_seen += 1;
            let off = SIZE_OF_PAGE_HEADER + i * SIZE_OF_ITEM_ID;
            let raw = u32::from_le_bytes(page[off..off + 4].try_into().unwrap());
            // bit-packed: lp_off (15) | lp_flags (2) | lp_len (15)
            let lp_off = (raw & 0x7FFF) as usize;
            let lp_flags = ((raw >> 15) & 0x3) as u8;
            let lp_len = ((raw >> 17) & 0x7FFF) as usize;
            if lp_flags != LP_NORMAL {
                stats.tuples_skipped_lp_flag += 1;
                continue;
            }
            if lp_off + lp_len > PAGE_BYTES || lp_len == 0 {
                stats.tuples_skipped_truncated += 1;
                continue;
            }
            let tuple_bytes = &page[lp_off..lp_off + lp_len];
            match decode_on_page_tuple(tuple_bytes, self.rel) {
                Some((xid, columns)) => {
                    out.push(BackfillTuple {
                        rfn: self.rel.rfn,
                        xid,
                        source_lsn: self.source_lsn,
                        columns,
                    });
                    stats.tuples_emitted += 1;
                }
                None => stats.tuples_skipped_truncated += 1,
            }
        }
        Ok(())
    }
}

/// Decode one on-page tuple. The on-disk tuple shape carries a full
/// `HeapTupleHeaderData` (23 bytes); the heap decoder consumes the
/// `xl_heap_header` (5 bytes) shape PG strips into WAL via
/// `XLogRegisterBufData(0, tup->t_data + SizeofHeapTupleHeader, ...)`.
/// Reshape `HeapTupleHeaderData` → `xl_heap_header`-prefixed buffer,
/// then call the shared decoder.
///
/// Returns `(xmin, columns)` or `None` on a truncated / malformed
/// header — a single bad tuple shouldn't abort page-walk.
fn decode_on_page_tuple(
    tuple: &[u8],
    rel: &RelDescriptor,
) -> Option<(u32, Vec<Option<ColumnValue>>)> {
    use crate::heap_decoder::{HEAP_HASNULL, HEAP_NATTS_MASK, SIZE_OF_HEAP_TUPLE_HEADER};

    if tuple.len() < SIZE_OF_HEAP_TUPLE_HEADER {
        return None;
    }
    // HeapTupleHeaderData layout (htup_details.h):
    //   t_xmin       0..4   (TransactionId)
    //   t_xmax       4..8
    //   t_field3     8..12  (cid | xvac union)
    //   t_ctid       12..18 (BlockIdData + OffsetNumber)
    //   t_infomask2  18..20
    //   t_infomask   20..22
    //   t_hoff       22     (offset to user data, 8-byte aligned)
    //   t_bits[]     23..   (NULL bitmap, only if HEAP_HASNULL)
    let xmin = u32::from_le_bytes(tuple[0..4].try_into().unwrap());
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

    // Build a synthetic xl_heap_header-prefixed buffer:
    //   xl_heap_header (5): t_infomask2 (2), t_infomask (2), t_hoff (1)
    //   bitmap (bitmap_bytes)
    //   padding (HeapTupleHeader's t_bits..t_hoff gap)
    //   column data (tuple[t_hoff..])
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
        Ok(d) => Some((xmin, d.columns)),
        Err(_) => None,
    }
}

/// Tap sink that buffers tar entry bytes and walks them 8 KiB at a
/// time. Tuples ship over `out_tx` to an async drain task that pumps
/// the CH emitter.
pub struct PageWalkSink {
    catalog: CatalogMap,
    /// `_lsn` value stamped onto every emitted tuple. Set by
    /// `start()` from the source's `StartInfo`.
    source_lsn: u64,
    /// Per-pump stats.
    pub stats: PageWalkStats,
    /// Output channel. Unbounded for V1 — the `BackupSink::chunk`
    /// callback is sync, and `tokio::sync::mpsc::Sender::blocking_send`
    /// panics inside the tokio runtime context the source drives.
    /// Memory exposure: bounded by user-heap row count emitted between
    /// drain ticks; in practice the emitter task ahead of this drains
    /// fast enough that unbounded queue is bounded by IO jitter alone.
    /// Switch to `try_send` + capacity if measurement shows
    /// pathological build-up.
    out_tx: Option<mpsc::UnboundedSender<BackfillTuple>>,
    /// Test-only capture, populated when `out_tx` is None.
    pub captured: Vec<BackfillTuple>,
    /// State carried between begin / chunk / end for the current entry.
    cur: Option<TapEntry>,
}

struct TapEntry {
    /// Block number within the relation, starting at 0 for each new
    /// tar entry. PG segments past 1 GiB carry `.1`, `.2` suffixes;
    /// in-segment block numbering is local. Cross-segment continuation
    /// (e.g. a 2 GiB relation as `<filenode>` + `<filenode>.1`) needs
    /// the caller to track segment offset — V1 walks each segment as
    /// if it were block 0 onward, which is correct for the LSN-stamp
    /// case (every row gets `source_lsn` regardless).
    block_no: u32,
    /// Carry-over bytes when chunk() reads straddled a page boundary.
    page_buf: Vec<u8>,
    /// `is_toast` cached at begin() so we can short-circuit page-walk
    /// inside chunk() without re-locking the catalog map.
    is_toast: bool,
    /// Resolved descriptor, cached at begin().
    desc: Option<Arc<RelDescriptor>>,
}

impl PageWalkSink {
    pub fn new(catalog: CatalogMap, out_tx: mpsc::UnboundedSender<BackfillTuple>) -> Self {
        Self {
            catalog,
            source_lsn: 0,
            stats: PageWalkStats::default(),
            out_tx: Some(out_tx),
            captured: Vec::new(),
            cur: None,
        }
    }

    /// Test-mode constructor — emitted tuples land in `captured`
    /// instead of being shipped through an mpsc.
    #[cfg(test)]
    pub fn new_capturing(catalog: CatalogMap) -> Self {
        Self {
            catalog,
            source_lsn: 0,
            stats: PageWalkStats::default(),
            out_tx: None,
            captured: Vec::new(),
            cur: None,
        }
    }

    /// Currently-active source_lsn. Set by `start()`.
    pub fn source_lsn(&self) -> u64 {
        self.source_lsn
    }

    fn classify(&self, meta: &FileMeta) -> Option<(u32, u32)> {
        if !matches!(meta.kind, FileKind::File) {
            return None;
        }
        parse_base_path(&meta.path)
    }

    fn flush_full_pages(&mut self) -> io::Result<()> {
        loop {
            let entry = match self.cur.as_mut() {
                Some(e) => e,
                None => return Ok(()),
            };
            if entry.page_buf.len() < PAGE_BYTES {
                return Ok(());
            }
            // Steal the page out so we can drop the entry borrow before
            // calling out_tx.send
            let block_no = entry.block_no;
            entry.block_no = entry.block_no.saturating_add(1);
            let is_toast = entry.is_toast;
            let desc_opt = entry.desc.clone();
            let page: Vec<u8> = entry.page_buf.drain(..PAGE_BYTES).collect();

            if is_toast {
                // V1: count the page, skip decode. WAL-side TOAST work
                // will pick this up when it lands.
                self.stats.pages_walked += 1;
                continue;
            }
            let Some(desc) = desc_opt else {
                // Catalog seed didn't carry this filenode — skip the
                // page entirely. Stats bookkeeping happened at begin().
                continue;
            };

            let walker = PageWalker::new(&desc, self.source_lsn);
            let mut local_out = Vec::new();
            if let Err(e) = walker.walk_page(&page, &mut local_out, &mut self.stats) {
                tracing::warn!(
                    target = "walshadow::backup_page_walk",
                    block = block_no,
                    error = %e,
                    "page walk skipped due to framing error"
                );
                continue;
            }
            for t in local_out {
                self.ship_tuple(t)?;
            }
        }
    }

    fn ship_tuple(&mut self, t: BackfillTuple) -> io::Result<()> {
        match &self.out_tx {
            Some(tx) => tx.send(t).map_err(|e| {
                io::Error::other(format!("PageWalkSink: emitter channel closed: {e}"))
            }),
            None => {
                self.captured.push(t);
                Ok(())
            }
        }
    }
}

impl BackupSink for PageWalkSink {
    fn start(&mut self, info: &StartInfo) -> io::Result<()> {
        self.source_lsn = info.start_lsn;
        Ok(())
    }

    fn begin(&mut self, meta: &FileMeta) -> io::Result<FileAction> {
        let Some((db, rel)) = self.classify(meta) else {
            // Path doesn't parse as base/<db>/<filenode> — refuse Tap;
            // multiplex sink will fall back to lander.
            return Ok(FileAction::Skip);
        };
        self.stats.files_seen += 1;
        let desc = self.catalog.get(db, rel);
        let is_toast = self.catalog.is_toast(db, rel);
        if is_toast {
            self.stats.toast_files_observed += 1;
        } else if desc.is_some() {
            self.stats.files_walked += 1;
        } else {
            self.stats.files_skipped_unknown_filenode += 1;
        }
        self.cur = Some(TapEntry {
            block_no: 0,
            page_buf: Vec::with_capacity(PAGE_BYTES * 2),
            is_toast,
            desc,
        });
        Ok(FileAction::Tap)
    }

    fn chunk(&mut self, bytes: &[u8]) -> io::Result<()> {
        {
            let entry = self
                .cur
                .as_mut()
                .ok_or_else(|| io::Error::other("PageWalkSink: chunk before begin"))?;
            entry.page_buf.extend_from_slice(bytes);
        }
        self.flush_full_pages()?;
        Ok(())
    }

    fn end(&mut self) -> io::Result<()> {
        if let Some(entry) = self.cur.take() {
            let trailing = entry.page_buf.len() as u64;
            if trailing > 0 {
                // PG heap files are always page-aligned; trailing
                // partial-page bytes are zero-padding or anomalous.
                // Count them but don't try to decode.
                self.stats.tail_bytes_dropped += trailing;
            }
        }
        Ok(())
    }

    fn finish(&mut self, _info: &EndInfo) -> io::Result<()> {
        // Drop the sender clone so the emitter drain task observes
        // channel close after this BackupSink is dropped (caller
        // controls the Arc lifetime; nothing to do here).
        Ok(())
    }
}

/// Test fixture: a `public.t(id int4)` descriptor (rfn 1663/5/16400,
/// oid 16400). Shared with `backfill_bootstrap`'s tests.
#[cfg(test)]
pub(crate) fn make_rel() -> RelDescriptor {
    use crate::shadow_catalog::{RelAttr, ReplIdent};
    RelDescriptor {
        rfn: RelFileNode {
            spc_node: 1663,
            db_node: 5,
            rel_node: 16400,
        },
        oid: 16400,
        namespace_oid: 2200,
        namespace_name: "public".into(),
        name: "t".into(),
        qualified_name: RelDescriptor::build_qualified_name("public", "t"),
        kind: 'r',
        persistence: 'p',
        replident: ReplIdent::Default { pk_attnums: None },
        attributes: vec![RelAttr {
            attnum: 1,
            name: "id".into(),
            type_oid: crate::heap_decoder::INT4OID,
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

/// Test fixture: synthesise an 8 KiB heap page with one int4 tuple, laid
/// out as PG would (PageHeaderData at 0, one ItemIdData slot at 24,
/// tuple body at the upper end). Shared with `backfill_bootstrap`.
#[cfg(test)]
pub(crate) fn synth_single_tuple_page(value: i32) -> [u8; PAGE_BYTES] {
    let mut page = [0u8; PAGE_BYTES];
    // Tuple body: HeapTupleHeaderData (23) + 1 byte pad + 4-byte int
    let tuple_off = PAGE_BYTES - 32;
    // t_xmin = 99
    page[tuple_off..tuple_off + 4].copy_from_slice(&99u32.to_le_bytes());
    // t_infomask2 = 1 (natts = 1)
    page[tuple_off + 18..tuple_off + 20].copy_from_slice(&1u16.to_le_bytes());
    // t_infomask = 0
    page[tuple_off + 20..tuple_off + 22].copy_from_slice(&0u16.to_le_bytes());
    // t_hoff = 24 (MAXALIGN(8) past 23-byte header)
    page[tuple_off + 22] = 24;
    // column 1 (int4) at offset 24
    page[tuple_off + 24..tuple_off + 28].copy_from_slice(&value.to_le_bytes());
    let tuple_len = 28u16;
    // Page header: pd_lower = 24 + 4 (one slot), pd_upper = tuple_off
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
    use std::path::PathBuf;

    #[test]
    fn page_walker_emits_single_tuple() {
        let rel = make_rel();
        let walker = PageWalker::new(&rel, 0xABCD);
        let page = synth_single_tuple_page(42);
        let mut out = Vec::new();
        let mut stats = PageWalkStats::default();
        walker.walk_page(&page, &mut out, &mut stats).unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].source_lsn, 0xABCD);
        assert_eq!(out[0].xid, 99);
        assert_eq!(out[0].columns.len(), 1);
        assert!(matches!(out[0].columns[0], Some(ColumnValue::Int4(42))));
        assert_eq!(stats.pages_walked, 1);
        assert_eq!(stats.tuples_emitted, 1);
    }

    #[test]
    fn page_walker_handles_empty_page() {
        let rel = make_rel();
        let walker = PageWalker::new(&rel, 0);
        let mut page = [0u8; PAGE_BYTES];
        // Fresh-init page: pd_lower at header end, pd_upper at page end
        page[12..14].copy_from_slice(&(SIZE_OF_PAGE_HEADER as u16).to_le_bytes());
        page[14..16].copy_from_slice(&(PAGE_BYTES as u16).to_le_bytes());
        let mut out = Vec::new();
        let mut stats = PageWalkStats::default();
        walker.walk_page(&page, &mut out, &mut stats).unwrap();
        assert!(out.is_empty());
        assert_eq!(stats.pages_walked, 1);
        assert_eq!(stats.slots_seen, 0);
    }

    #[test]
    fn page_walker_skips_lp_dead_slots() {
        let rel = make_rel();
        let walker = PageWalker::new(&rel, 0);
        let mut page = synth_single_tuple_page(7);
        // Flip the slot's lp_flags from LP_NORMAL (1) to LP_DEAD (3)
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
        let mut stats = PageWalkStats::default();
        walker.walk_page(&page, &mut out, &mut stats).unwrap();
        assert!(out.is_empty());
        assert_eq!(stats.tuples_skipped_lp_flag, 1);
        assert_eq!(stats.tuples_emitted, 0);
    }

    #[test]
    fn page_walker_rejects_bad_header_bounds() {
        let rel = make_rel();
        let walker = PageWalker::new(&rel, 0);
        let mut page = [0u8; PAGE_BYTES];
        // pd_lower past pd_upper
        page[12..14].copy_from_slice(&(PAGE_BYTES as u16).to_le_bytes());
        page[14..16].copy_from_slice(&(SIZE_OF_PAGE_HEADER as u16).to_le_bytes());
        let mut out = Vec::new();
        let mut stats = PageWalkStats::default();
        let err = walker.walk_page(&page, &mut out, &mut stats);
        assert!(matches!(err, Err(PageWalkError::BadPageHeader { .. })));
    }

    #[test]
    fn catalog_map_routes_filenodes_and_marks_toast() {
        let mut m = CatalogMap::new();
        let mut rel = make_rel();
        rel.namespace_name = "public".into();
        m.insert(Arc::new(rel.clone()));
        let mut toast_rel = rel.clone();
        toast_rel.rfn.rel_node = 99999;
        toast_rel.namespace_name = "pg_toast".into();
        m.insert(Arc::new(toast_rel));

        assert!(m.get(5, 16400).is_some());
        assert!(!m.is_toast(5, 16400));
        assert!(m.get(5, 99999).is_some());
        assert!(m.is_toast(5, 99999));
        assert_eq!(m.len(), 2);
    }

    #[test]
    fn pagewalk_sink_decodes_one_page_via_chunk_stream() {
        let mut catalog = CatalogMap::new();
        catalog.insert(Arc::new(make_rel()));
        let mut sink = PageWalkSink::new_capturing(catalog);
        sink.start(&StartInfo {
            start_lsn: 0x1234_5678,
            timeline: 1,
            tablespaces: Vec::new(),
        })
        .unwrap();
        let meta = FileMeta {
            path: PathBuf::from("base/5/16400"),
            size: PAGE_BYTES as u64,
            mode: 0o600,
            kind: FileKind::File,
        };
        let action = sink.begin(&meta).unwrap();
        assert_eq!(action, FileAction::Tap);
        let page = synth_single_tuple_page(99);
        // Feed in two chunks to exercise the buffer-across-chunk path
        sink.chunk(&page[..4096]).unwrap();
        sink.chunk(&page[4096..]).unwrap();
        sink.end().unwrap();
        sink.finish(&EndInfo {
            end_lsn: 0,
            timeline: 1,
        })
        .unwrap();

        assert_eq!(sink.captured.len(), 1);
        assert_eq!(sink.captured[0].source_lsn, 0x1234_5678);
        assert_eq!(sink.captured[0].xid, 99);
        assert!(matches!(
            sink.captured[0].columns[0],
            Some(ColumnValue::Int4(99))
        ));
        assert_eq!(sink.stats.files_seen, 1);
        assert_eq!(sink.stats.files_walked, 1);
        assert_eq!(sink.stats.pages_walked, 1);
        assert_eq!(sink.stats.tuples_emitted, 1);
    }

    #[test]
    fn pagewalk_sink_skips_when_filenode_absent_from_catalog() {
        let sink = PageWalkSink::new_capturing(CatalogMap::new());
        let m = sink.classify(&FileMeta {
            path: PathBuf::from("base/5/16400"),
            size: 0,
            mode: 0,
            kind: FileKind::File,
        });
        assert_eq!(m, Some((5, 16400)));

        let mut sink = sink;
        sink.start(&StartInfo {
            start_lsn: 0,
            timeline: 1,
            tablespaces: Vec::new(),
        })
        .unwrap();
        let meta = FileMeta {
            path: PathBuf::from("base/5/16400"),
            size: PAGE_BYTES as u64,
            mode: 0o600,
            kind: FileKind::File,
        };
        let action = sink.begin(&meta).unwrap();
        assert_eq!(action, FileAction::Tap);
        sink.chunk(&synth_single_tuple_page(7)).unwrap();
        sink.end().unwrap();
        // Tuple decoding skipped because no RelDescriptor; stat
        // reflects unknown filenode
        assert!(sink.captured.is_empty());
        assert_eq!(sink.stats.files_skipped_unknown_filenode, 1);
        assert_eq!(sink.stats.tuples_emitted, 0);
        // Page bookkeeping is silent on the "no descriptor" path —
        // we don't claim to have walked it
        assert_eq!(sink.stats.pages_walked, 0);
    }

    #[test]
    fn pagewalk_sink_rejects_non_base_paths() {
        let mut sink = PageWalkSink::new_capturing(CatalogMap::new());
        sink.start(&StartInfo {
            start_lsn: 0,
            timeline: 1,
            tablespaces: Vec::new(),
        })
        .unwrap();
        let meta = FileMeta {
            path: PathBuf::from("pg_control"),
            size: 0,
            mode: 0,
            kind: FileKind::File,
        };
        assert_eq!(sink.begin(&meta).unwrap(), FileAction::Skip);
    }
}
