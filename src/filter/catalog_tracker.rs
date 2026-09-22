//! Live catalog relfilenode set.
//!
//! Bootstrap rule: rel_node < FirstNormalObjectId (16384) is catalog.
//!
//! Update sources:
//! * `RM_RELMAP_ID / XLOG_RELMAP_UPDATE` — authoritative for mapped
//!   catalogs (pg_class, pg_attribute, pg_type, pg_proc, pg_database, …).
//!   Body is `xl_relmap_update` + `RelMapFile` blob (magic + mappings +
//!   crc, see PG `src/backend/utils/cache/relmapper.c`). Each non-zero
//!   `(mapoid, mapfilenumber)` adds `mapfilenumber` for that database
//!   (shared set if `dbid == 0`).
//! * Heap writes to `pg_class` (`pg_class_decoder`). Carry new
//!   relfilenodes for non-mapped catalogs after VACUUM FULL / REINDEX /
//!   CLUSTER. `oid < FirstNormalObjectId` filter keeps user-table
//!   inserts into pg_class out of the catalog set. Update whose OID sits
//!   in prefix-compressed bytes resolves through the catalog row last
//!   seen at its old slot.
//! * Catalog rebuilds (`make_new_heap`): transient `pg_temp_<catalog>`
//!   heap is catalog storage before swap, as is every relfilenode its
//!   xact creates until the transient row drops (toast, rebuilt indexes).
//! * [`seed_from_source`](CatalogTracker::seed_from_source) — closes the
//!   hole where a long-running source rotated a mapped catalog above
//!   16384 before walshadow attached, so its `XLOG_RELMAP_UPDATE` sits
//!   in pre-attach WAL the bootstrap rule never sees.
//!

use thiserror::Error;
use tokio_postgres::Client;
use tokio_postgres::types::Oid;
use walrus::pg::walparser::{RmId, XLogRecord};

use crate::decode::fpi::restore_block_image;
use crate::decode::wal_xact::{XLOG_XACT_ABORT, XLOG_XACT_COMMIT, XLOG_XACT_OPMASK};
use crate::filter::main_data::{
    XL_RELMAP_UPDATE_HEADER_SIZE, XLOG_SMGR_CREATE, parse_xl_relmap_update, parse_xl_smgr_create,
};
use crate::filter::pg_class_decoder::{
    DecodeOutcome, PgClassRow, decode_pg_class_tuple, info_carries_new_tuple_heap,
    info_carries_new_tuple_heap2, info_is_inplace, page_rows, tuple_slots,
};
use crate::schema::FIRST_NORMAL_OBJECT_ID;
use ahash::{HashMap, HashSet};

/// XLOG_RELMAP_UPDATE info byte (`xl_info & XLR_RMGR_INFO_MASK`).
const XLOG_RELMAP_UPDATE: u8 = 0x00;
/// `RELMAPPER_FILEMAGIC` from `src/backend/utils/cache/relmapper.c`.
const RELMAPPER_FILEMAGIC: i32 = 0x592717;
const MAX_MAPPINGS: usize = 64;
const REL_MAP_FILE_SIZE: usize = 4 + 4 + MAX_MAPPINGS * 8 + 4; // magic + n + mappings + crc

/// `pg_class.oid`, fixed PG catalog OID
pub const PG_CLASS_OID: u32 = 1259;
/// Catalogs that store statistics, including indexes and toast heaps
///
/// Bootstrap OIDs, declared in PG `src/include/catalog/pg_statistic.h` and
/// `pg_statistic_ext_data.h`; identical PG 13 through 19. VACUUM FULL moves the
/// filenodes, tracked per database in `opaque_filenode_by_oid`
///
/// Keep `pg_statistic_ext` outside this list because DDL writes its definition.
/// ANALYZE also rewrites `pg_class.relpages` / `reltuples`, caught instead by
/// the INPLACE rule since the same heap carries shape
const OPAQUE_CATALOG_OIDS: &[u32] = &[
    2619, // pg_statistic
    2696, // pg_statistic_relid_att_inh_index
    2840, // pg_toast_2619
    2841, // pg_toast_2619_index
    3429, // pg_statistic_ext_data
    3430, // pg_toast_3429
    3431, // pg_toast_3429_index
    3433, // pg_statistic_ext_data_stxoid_inh_index
];
/// `pg_namespace.oid`; writes to it force capture-all (relcache invals
/// enumerate rels only for pg_class/pg_attribute/pg_index/pg_constraint
/// changes — PG `src/backend/utils/cache/inval.c` — while namespace rename
/// changes every embedded namespace text with zero per-relation invals)
pub const PG_NAMESPACE_OID: u32 = 2615;

#[derive(Debug, Default)]
pub struct CatalogTracker {
    /// `(db_node, rel_node)`; `db_node == 0` is the shared catalog set,
    /// consulted by queries on any db
    nodes: HashSet<(u32, u32)>,
    /// Current pg_class filenode per db. Empty bootstrap falls through to
    /// `rel == PG_CLASS_OID` (mapped-catalog relfilenode == oid until
    /// first rewrite).
    pg_class_filenode: HashMap<u32, u32>,
    /// Current pg_namespace filenode per db; fallback `rel ==
    /// PG_NAMESPACE_OID`. Unmapped catalog: VACUUM FULL relocates it via
    /// its own pg_class row, harvested below.
    pg_namespace_filenode: HashMap<u32, u32>,
    /// Current statistics catalog filenodes by database
    opaque_filenodes: HashSet<(u32, u32)>,
    /// Current filenode for each statistics catalog oid
    opaque_filenode_by_oid: HashMap<(u32, u32), u32>,
    /// Catalog row `(oid, relfilenode)` per pg_class slot `(db, pg_class
    /// filenode, block, offnum)`. Resolves prefix-compressed updates, which
    /// PG emits only when old and new tuple share an already-imaged page
    catalog_slots: HashMap<(u32, u32, u32, u16), (u32, u32)>,
    /// Xids inside a catalog rebuild, until its transient pg_class row drops
    catalog_rebuilds: HashSet<u32>,
    /// Transient heap rebuilding pg_class, per db; imaged ahead of relmap swap
    pg_class_transient: HashMap<u32, u32>,
    relmap_updates: u64,
    /// pg_class heap writes the decoder couldn't reconstruct (truncated /
    /// malformed `t_hoff`). OID-prefix-compressed records count in
    /// `pg_class_writes_oid_in_prefix` instead.
    pg_class_writes_undecoded: u64,
    pg_class_writes_decoded: u64,
    /// pg_class UPDATE / HOT_UPDATE that prefix-compressed past the OID
    /// (`XLH_UPDATE_PREFIX_FROM_OLD`, `prefixlen > 0`). Record alone can't
    /// reconstruct `(oid, relfilenode)`; resolved via `catalog_slots`.
    /// Typical: VACUUM FULL / REINDEX on a non-mapped catalog (pg_depend,
    /// pg_namespace, …).
    pg_class_writes_oid_in_prefix: u64,
    seeded_from_source: u64,
    /// Coarse invalidations, including writes without decoded OID
    invalidation_signals_sent: u64,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct CatalogTrackerStats {
    pub relmap_updates: u64,
    pub pg_class_writes_undecoded: u64,
    pub pg_class_writes_decoded: u64,
    pub pg_class_writes_oid_in_prefix: u64,
    pub seeded_from_source: u64,
    pub invalidation_signals_sent: u64,
}

#[derive(Debug, Error)]
pub enum SeedError {
    #[error("pg: {0}")]
    Pg(#[from] tokio_postgres::Error),
}

/// [`CatalogTracker::observe`] verdict: whether the record mutated a
/// tracked catalog, which database owns that catalog, plus — when block 0
/// decoded a user relation's pg_class row — that oid, the filter's per-oid
/// first-touch source for boundary capture.
///
/// The tracker itself stays cluster-wide: every database's catalog
/// filenodes must classify for shadow routing. `catalog_db_oid` is what
/// lets the filter admit only the followed database's writes into
/// descriptor capture.
#[derive(Debug, Default, Clone, Copy)]
pub struct Observation {
    pub catalog_write: bool,
    /// Database owning the written catalog: block 0 `db_node` for pg_class
    /// writes, `xl_relmap_update.dbid` for relmap. `0` = shared catalog
    pub catalog_db_oid: Option<u32>,
    pub pg_class_user_oid: Option<u32>,
}

impl Observation {
    fn catalog(db_oid: u32) -> Self {
        Self {
            catalog_write: true,
            catalog_db_oid: Some(db_oid),
            pg_class_user_oid: None,
        }
    }
}

impl CatalogTracker {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn stats(&self) -> CatalogTrackerStats {
        CatalogTrackerStats {
            relmap_updates: self.relmap_updates,
            pg_class_writes_undecoded: self.pg_class_writes_undecoded,
            pg_class_writes_decoded: self.pg_class_writes_decoded,
            pg_class_writes_oid_in_prefix: self.pg_class_writes_oid_in_prefix,
            seeded_from_source: self.seeded_from_source,
            invalidation_signals_sent: self.invalidation_signals_sent,
        }
    }

    pub fn add(&mut self, db_node: u32, rel_node: u32) {
        self.nodes.insert((db_node, rel_node));
    }

    /// Known catalog `(db_node, rel_node)` pairs, for consumers that classify
    /// off the same seed without carrying the tracker
    pub fn nodes(&self) -> impl Iterator<Item = (u32, u32)> + '_ {
        self.nodes.iter().copied()
    }

    /// `rel < FIRST_NORMAL_OBJECT_ID` is the bootstrap rule; relmap
    /// updates add post-rewrite filenumbers. `db_node == 0` (shared:
    /// pg_database, pg_authid, …) consulted for any db.
    pub fn is_catalog(&self, db_node: u32, rel_node: u32) -> bool {
        if rel_node == 0 {
            return false;
        }
        if rel_node < FIRST_NORMAL_OBJECT_ID {
            return true;
        }
        if db_node == 0 {
            return self.nodes.contains(&(0, rel_node));
        }
        self.nodes.contains(&(db_node, rel_node)) || self.nodes.contains(&(0, rel_node))
    }

    /// Observe catalog database and decoded user relation OID when present.
    /// `page_magic` reads full-page images
    pub fn observe(&mut self, record: &XLogRecord, page_magic: u16) -> Observation {
        let rm = record.header.resource_manager_id;
        let info_high = record.header.info & 0xF0;

        if rm == RmId::RelMap as u8 && info_high == XLOG_RELMAP_UPDATE {
            return self
                .handle_relmap_update(record)
                .map(Observation::catalog)
                .unwrap_or_default();
        }
        if rm == RmId::Smgr as u8
            && info_high == XLOG_SMGR_CREATE
            && self.catalog_rebuilds.contains(&record.header.xact_id)
            && let Some((rfn, _fork)) = parse_xl_smgr_create(&record.main_data)
        {
            self.nodes.insert((rfn.db_node, rfn.rel_node));
            return Observation::default();
        }
        if rm == RmId::Xact as u8
            && matches!(
                record.header.info & XLOG_XACT_OPMASK,
                XLOG_XACT_COMMIT | XLOG_XACT_ABORT
            )
        {
            self.catalog_rebuilds.remove(&record.header.xact_id);
            return Observation::default();
        }

        self.learn_imaged_pages(record, page_magic);
        let heap_new_tuple = rm == RmId::Heap as u8 && info_carries_new_tuple_heap(info_high);
        let heap2_new_tuple = rm == RmId::Heap2 as u8 && info_carries_new_tuple_heap2(info_high);
        if heap_new_tuple || heap2_new_tuple {
            return self.harvest_pg_class_blocks(record, page_magic);
        }
        if rm == RmId::Heap as u8 && info_is_inplace(info_high) {
            self.learn_inplace(record, page_magic);
            return Observation::default();
        }
        // DROP TABLE writes pg_class heap_delete, skipped by the
        // insert/update-only harvest path. Signal anyway so cache
        // invalidates + sweep_dropped runs at this xact's commit. Dying
        // tuple OID not decoded: catalogs default relreplident='n', WAL
        // omits it.
        if rm == RmId::Heap as u8 {
            let info_op = info_high & 0x70;
            if info_op == 0x10 {
                // HEAP_DELETE
                let Some((db, rel)) = self.pg_class_block(record) else {
                    return Observation::default();
                };
                if let Some((blk, off)) = tuple_slots(record).old {
                    self.catalog_slots.remove(&(db, rel, blk, off));
                }
                // Rebuild's transient row drops after swap and reindex
                self.catalog_rebuilds.remove(&record.header.xact_id);
                self.invalidation_signals_sent += 1;
                return Observation::catalog(db);
            }
        }
        Observation::default()
    }

    /// First block's `(db_node, rel_node)` iff it targets the current
    /// pg_class filenode; `None` otherwise.
    fn pg_class_block(&self, record: &XLogRecord) -> Option<(u32, u32)> {
        let blk = record.blocks.first()?;
        let (db, rel) = (
            blk.header.location.rel.db_node,
            blk.header.location.rel.rel_node,
        );
        self.is_pg_class_relfilenode(db, rel).then_some((db, rel))
    }

    /// Decode block 0 when `record` targets pg_class. PG registers the
    /// new tuple via `XLogRegisterBufData(0, ...)`; later block refs
    /// (heap_update's block 1 old page) carry no tuple, must not decode.
    fn harvest_pg_class_blocks(&mut self, record: &XLogRecord, page_magic: u16) -> Observation {
        let Some((db, rel)) = self.pg_class_block(record) else {
            return Observation::default();
        };
        let slots = tuple_slots(record);
        let old = slots
            .old
            .and_then(|(blk, off)| self.catalog_slots.remove(&(db, rel, blk, off)));
        let mut user_oid = None;
        let mut catalog_row = None;
        match decode_pg_class_tuple(record, 0, page_magic) {
            DecodeOutcome::Decoded(row) => {
                self.pg_class_writes_decoded += 1;
                if row.oid != 0 && row.oid < FIRST_NORMAL_OBJECT_ID && row.relfilenode != 0 {
                    catalog_row = Some((row.oid, row.relfilenode));
                }
                if row.oid >= FIRST_NORMAL_OBJECT_ID {
                    user_oid = Some(row.oid);
                    self.learn_rebuild(db, &row, record.header.xact_id);
                }
            }
            DecodeOutcome::OidInPrefix(logged) => {
                self.pg_class_writes_oid_in_prefix += 1;
                catalog_row = old.map(|(oid, old_rfn)| {
                    let old_bytes = old_rfn.to_le_bytes();
                    let rfn = std::array::from_fn(|i| logged[i].unwrap_or(old_bytes[i]));
                    (oid, u32::from_le_bytes(rfn))
                });
            }
            DecodeOutcome::Undecoded => {
                // Cache must still drop: PG 17 ALTER ADD COLUMN emits a
                // pg_class HOT_UPDATE whose new tuple omits the relnatts
                // prefix; silent skip shipped c=NULL for post-ALTER rows
                // decoded against the stale 2-column descriptor.
                self.pg_class_writes_undecoded += 1;
            }
        }
        if let Some((oid, relfilenode)) = catalog_row {
            self.learn_catalog_row(db, oid, relfilenode);
        }
        if let Some((blk, off)) = slots.new {
            self.set_slot((db, rel, blk, off), catalog_row);
        }
        // Coarse-fire regardless: over-invalidation is cheap (lazy
        // refetch), under-invalidation silently masks DDL.
        self.invalidation_signals_sent += 1;
        Observation {
            pg_class_user_oid: user_oid,
            ..Observation::catalog(db)
        }
    }

    fn learn_catalog_row(&mut self, db: u32, oid: u32, relfilenode: u32) {
        self.nodes.insert((db, relfilenode));
        if oid == PG_NAMESPACE_OID {
            self.pg_namespace_filenode.insert(db, relfilenode);
        }
        if OPAQUE_CATALOG_OIDS.contains(&oid) {
            self.learn_opaque(db, oid, relfilenode);
        }
    }

    fn set_slot(&mut self, key: (u32, u32, u32, u16), catalog_row: Option<(u32, u32)>) {
        if let Some(row) = catalog_row {
            self.catalog_slots.insert(key, row);
        } else {
            self.catalog_slots.remove(&key);
        }
    }

    /// Transient heap of a catalog rebuild: its storage, and everything its
    /// xact creates before the transient row drops, is catalog
    fn learn_rebuild(&mut self, db: u32, row: &PgClassRow, xid: u32) {
        let Some(parent) = row.rebuilt_catalog() else {
            return;
        };
        self.nodes.insert((db, row.relfilenode));
        if xid != 0 {
            self.catalog_rebuilds.insert(xid);
        }
        if parent == PG_CLASS_OID {
            self.pg_class_transient.insert(db, row.relfilenode);
        }
    }

    /// In-place update confirms which row a slot holds
    fn learn_inplace(&mut self, record: &XLogRecord, page_magic: u16) {
        let Some((db, rel)) = self.pg_class_block(record) else {
            return;
        };
        let Some((blk, off)) = tuple_slots(record).new else {
            return;
        };
        let row = match decode_pg_class_tuple(record, 0, page_magic) {
            DecodeOutcome::Decoded(row) if row.oid != 0 && row.oid < FIRST_NORMAL_OBJECT_ID => {
                Some((row.oid, row.relfilenode))
            }
            _ => None,
        };
        self.set_slot((db, rel, blk, off), row);
    }

    /// Replace slot knowledge for every imaged pg_class page, including the
    /// transient heap a pg_class rebuild fills before relmap swaps it in
    fn learn_imaged_pages(&mut self, record: &XLogRecord, page_magic: u16) {
        for block in &record.blocks {
            let loc = block.header.location;
            let (db, rel, blk) = (loc.rel.db_node, loc.rel.rel_node, loc.block_no);
            if !block.header.has_image()
                || !(self.is_pg_class_relfilenode(db, rel)
                    || self.pg_class_transient.get(&db) == Some(&rel))
            {
                continue;
            }
            self.catalog_slots
                .retain(|&(d, r, b, _), _| (d, r, b) != (db, rel, blk));
            let Ok(page) = restore_block_image(block, page_magic) else {
                continue;
            };
            for (off, row) in page_rows(&page) {
                if row.oid != 0 && row.oid < FIRST_NORMAL_OBJECT_ID {
                    self.catalog_slots
                        .insert((db, rel, blk, off), (row.oid, row.relfilenode));
                }
            }
        }
    }

    /// Return true for a statistics catalog filenode
    pub fn is_opaque_catalog(&self, db: u32, rel: u32) -> bool {
        if self.opaque_filenodes.contains(&(db, rel)) {
            return true;
        }
        OPAQUE_CATALOG_OIDS.contains(&rel) && !self.opaque_filenode_by_oid.contains_key(&(db, rel))
    }

    fn learn_opaque(&mut self, db: u32, oid: u32, filenode: u32) {
        if let Some(prev) = self.opaque_filenode_by_oid.insert((db, oid), filenode)
            && prev != filenode
        {
            self.opaque_filenodes.remove(&(db, prev));
        }
        self.opaque_filenodes.insert((db, filenode));
    }

    /// True when `(db, rel)` is pg_namespace's current heap — the
    /// capture-all trigger set.
    ///
    /// pg_type stays out by choice, though its writes are equally
    /// unenumerated by relcache invals: CREATE TABLE writes pg_type on every
    /// run (composite + array rows), so triggering on it would fire
    /// capture-all constantly and leave the enumerated path dead. The cost
    /// is a stale `RelAttr.type_name` after `ALTER TYPE … RENAME`, which no
    /// decode path reads — accepted, with remediation options in
    /// `plans/catalog.md`
    pub fn is_capture_all_catalog(&self, db: u32, rel: u32) -> bool {
        match self.pg_namespace_filenode.get(&db) {
            Some(&fnum) => fnum == rel,
            None => rel == PG_NAMESPACE_OID,
        }
    }

    /// Falls back to `rel == PG_CLASS_OID` until a filenode is observed
    /// for `db` (mapped-catalog relfilenode == oid until first rewrite).
    fn is_pg_class_relfilenode(&self, db: u32, rel: u32) -> bool {
        match self.pg_class_filenode.get(&db) {
            Some(&fnum) => fnum == rel,
            None => rel == PG_CLASS_OID,
        }
    }

    /// `Some(dbid)` once the mapping body applied; `None` for malformed
    /// bodies, which apply nothing
    fn handle_relmap_update(&mut self, record: &XLogRecord) -> Option<u32> {
        self.relmap_updates += 1;
        let md = &record.main_data;
        let header = parse_xl_relmap_update(md)?;
        if md.len() < XL_RELMAP_UPDATE_HEADER_SIZE + REL_MAP_FILE_SIZE
            || header.nbytes != REL_MAP_FILE_SIZE
        {
            return None;
        }
        let dbid = header.dbid;
        let map =
            &md[XL_RELMAP_UPDATE_HEADER_SIZE..XL_RELMAP_UPDATE_HEADER_SIZE + REL_MAP_FILE_SIZE];
        let magic = i32::from_le_bytes(map[0..4].try_into().unwrap());
        if magic != RELMAPPER_FILEMAGIC {
            return None;
        }
        let num_mappings = i32::from_le_bytes(map[4..8].try_into().unwrap()) as usize;
        if num_mappings > MAX_MAPPINGS {
            return None;
        }
        let mappings = &map[8..8 + MAX_MAPPINGS * 8];
        for i in 0..num_mappings {
            let off = i * 8;
            let mapoid = u32::from_le_bytes(mappings[off..off + 4].try_into().unwrap());
            let filenum = u32::from_le_bytes(mappings[off + 4..off + 8].try_into().unwrap());
            if mapoid != 0 && filenum != 0 {
                self.nodes.insert((dbid, filenum));
                if mapoid == PG_CLASS_OID {
                    self.pg_class_filenode.insert(dbid, filenum);
                    self.catalog_slots
                        .retain(|&(d, r, _, _), _| d != dbid || r == filenum);
                    self.pg_class_transient.remove(&dbid);
                }
            }
        }
        self.invalidation_signals_sent += 1;
        Some(dbid)
    }

    /// Query source `pg_class` for every catalog relation (oid < 16384).
    /// Closes the rotated-mapped-catalog-before-attach hole: post-rewrite
    /// filenodes whose `XLOG_RELMAP_UPDATE` sits in pre-attach WAL.
    /// Shared catalogs seeded under `db_node = 0`, per-db under the
    /// source's current-database oid.
    pub async fn seed_from_source(&mut self, client: &Client) -> Result<usize, SeedError> {
        let rows = client
            .query(
                "SELECT \
                    CASE WHEN c.relisshared THEN 0::oid \
                         ELSE (SELECT d.oid FROM pg_database d \
                               WHERE d.datname = current_database()) \
                    END AS db_node, \
                    c.oid AS catalog_oid, \
                    pg_relation_filenode(c.oid) AS filenode, \
                    c.ctid::text AS slot \
                 FROM pg_class c \
                 WHERE c.oid < 16384 \
                   AND pg_relation_filenode(c.oid) IS NOT NULL",
                &[],
            )
            .await?;
        let mut added = 0usize;
        let mut slots = Vec::with_capacity(rows.len());
        for row in &rows {
            let db_node: Oid = row.get(0);
            let catalog_oid: Oid = row.get(1);
            let filenode: Oid = row.get(2);
            if filenode == 0 {
                continue;
            }
            if db_node != 0
                && let Some(slot) = parse_ctid(row.get(3))
            {
                slots.push((db_node, slot, catalog_oid, filenode));
            }
            if self.nodes.insert((db_node, filenode)) {
                added += 1;
            }
            if catalog_oid == PG_CLASS_OID {
                self.pg_class_filenode.insert(db_node, filenode);
            }
            if catalog_oid == PG_NAMESPACE_OID {
                self.pg_namespace_filenode.insert(db_node, filenode);
            }
            if OPAQUE_CATALOG_OIDS.contains(&catalog_oid) {
                self.learn_opaque(db_node, catalog_oid, filenode);
            }
        }
        // Stream may already hold fresher slot knowledge than this snapshot
        for (db_node, (blk, off), oid, filenode) in slots {
            let pg_class = self
                .pg_class_filenode
                .get(&db_node)
                .copied()
                .unwrap_or(PG_CLASS_OID);
            self.catalog_slots
                .entry((db_node, pg_class, blk, off))
                .or_insert((oid, filenode));
        }
        self.seeded_from_source += added as u64;
        Ok(added)
    }

    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }
}

/// `(block, offnum)` from tid text `(b,o)`
fn parse_ctid(text: &str) -> Option<(u32, u16)> {
    let (blk, off) = text.strip_prefix('(')?.strip_suffix(')')?.split_once(',')?;
    Some((blk.parse().ok()?, off.parse().ok()?))
}

/// Well-formed `XLOG_RELMAP_UPDATE` record, shared with filter-engine tests.
#[cfg(test)]
pub(crate) fn test_relmap_record(dbid: u32, mappings: &[(u32, u32)]) -> XLogRecord<'static> {
    let mut data = Vec::new();
    data.extend_from_slice(&dbid.to_le_bytes());
    data.extend_from_slice(&1664u32.to_le_bytes()); // tsid pg_global
    data.extend_from_slice(&(REL_MAP_FILE_SIZE as i32).to_le_bytes());
    data.extend_from_slice(&RELMAPPER_FILEMAGIC.to_le_bytes());
    data.extend_from_slice(&(mappings.len() as i32).to_le_bytes());
    for &(oid, fnum) in mappings {
        data.extend_from_slice(&oid.to_le_bytes());
        data.extend_from_slice(&fnum.to_le_bytes());
    }
    for _ in mappings.len()..MAX_MAPPINGS {
        data.extend_from_slice(&[0u8; 8]);
    }
    data.extend_from_slice(&0u32.to_le_bytes()); // crc, ignored

    XLogRecord {
        header: walrus::pg::walparser::XLogRecordHeader {
            resource_manager_id: RmId::RelMap as u8,
            info: XLOG_RELMAP_UPDATE,
            total_record_length: 24 + data.len() as u32,
            ..Default::default()
        },
        main_data_len: data.len() as u32,
        main_data: std::borrow::Cow::Owned(data),
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use walrus::pg::walparser::{
        BlockLocation, RelFileNode, XLP_PAGE_MAGIC_PG15, XLogRecordBlock, XLogRecordBlockHeader,
        XLogRecordHeader,
    };

    use super::test_relmap_record as relmap_record;

    const MAGIC: u16 = XLP_PAGE_MAGIC_PG15;

    fn heap_block_record(
        rm: RmId,
        info: u8,
        db: u32,
        rel: u32,
        data: Vec<u8>,
    ) -> XLogRecord<'static> {
        heap_block_record_with_main(rm, info, db, rel, data, Vec::new())
    }

    fn heap_block_record_with_main(
        rm: RmId,
        info: u8,
        db: u32,
        rel: u32,
        data: Vec<u8>,
        main_data: Vec<u8>,
    ) -> XLogRecord<'static> {
        XLogRecord {
            header: XLogRecordHeader {
                resource_manager_id: rm as u8,
                info,
                ..Default::default()
            },
            blocks: vec![XLogRecordBlock {
                header: XLogRecordBlockHeader {
                    location: BlockLocation {
                        rel: RelFileNode {
                            spc_node: 1663,
                            db_node: db,
                            rel_node: rel,
                        },
                        block_no: 0,
                    },
                    ..Default::default()
                },
                data: std::borrow::Cow::Owned(data),
                ..Default::default()
            }],
            main_data: std::borrow::Cow::Owned(main_data),
            ..Default::default()
        }
    }

    /// Decoder reads only byte 7 (flags), so all-zero suffices.
    fn xl_heap_update_no_compression() -> Vec<u8> {
        vec![0u8; 14] // SizeOfHeapUpdate
    }

    /// `XLH_UPDATE_PREFIX_FROM_OLD` shape: VACUUM FULL on a non-mapped
    /// catalog compresses cols 1..7 (88 bytes), so WAL payload begins at
    /// relfilenode.
    fn pg_class_update_block_prefix_88(relfilenode: u32) -> Vec<u8> {
        let mut v = Vec::new();
        v.extend_from_slice(&88u16.to_le_bytes()); // prefixlen
        v.extend_from_slice(&33u16.to_le_bytes()); // t_infomask2
        v.extend_from_slice(&0u16.to_le_bytes()); // t_infomask
        v.push(24); // t_hoff
        v.push(0); // MAXALIGN pad, offset 23 -> 24
        v.extend_from_slice(&relfilenode.to_le_bytes());
        v
    }

    /// xl_heap_header + payload decoding to a pg_class tuple. No nulls,
    /// t_hoff = 24.
    fn pg_class_block_data(oid: u32, relfilenode: u32) -> Vec<u8> {
        let mut v = Vec::new();
        v.extend_from_slice(&33u16.to_le_bytes()); // t_infomask2 (pg_class natts)
        v.extend_from_slice(&0u16.to_le_bytes()); // t_infomask
        v.push(24); // t_hoff = MAXALIGN(SizeOfHeapTupleHeader)
        v.push(0); // MAXALIGN pad, offset 23 -> 24
        v.extend_from_slice(&oid.to_le_bytes()); // col 1: oid
        v.extend_from_slice(&[0u8; 64]); // col 2: relname (NAMEDATALEN)
        v.extend_from_slice(&0u32.to_le_bytes()); // col 3: relnamespace
        v.extend_from_slice(&0u32.to_le_bytes()); // col 4: reltype
        v.extend_from_slice(&0u32.to_le_bytes()); // col 5: reloftype
        v.extend_from_slice(&0u32.to_le_bytes()); // col 6: relowner
        v.extend_from_slice(&0u32.to_le_bytes()); // col 7: relam
        v.extend_from_slice(&relfilenode.to_le_bytes()); // col 8: relfilenode
        v
    }

    #[test]
    fn bootstrap_low_oids_are_catalog() {
        let t = CatalogTracker::new();
        assert!(t.is_catalog(5, 1259));
        assert!(t.is_catalog(5, 16383));
        assert!(!t.is_catalog(5, 16384));
        assert!(!t.is_catalog(5, 0));
    }

    #[test]
    fn statistics_catalogs_are_opaque_at_their_bootstrap_filenode() {
        let t = CatalogTracker::new();
        assert!(t.is_opaque_catalog(5, 2619)); // pg_statistic
        assert!(t.is_opaque_catalog(5, 2696)); // its index
        assert!(t.is_opaque_catalog(5, 2840)); // its toast heap
        assert!(!t.is_opaque_catalog(5, 1259)); // pg_class
        assert!(!t.is_opaque_catalog(5, 16400)); // user rel
    }

    #[test]
    fn vacuum_full_moves_the_opaque_filenode() {
        let mut t = CatalogTracker::new();
        // Simulate VACUUM FULL moving pg_statistic
        let data = pg_class_block_data(2619, 40000);
        let rec = heap_block_record_with_main(
            RmId::Heap,
            0x20,
            5,
            1259,
            data,
            xl_heap_update_no_compression(),
        );
        t.observe(&rec, MAGIC);
        assert!(t.is_opaque_catalog(5, 40000));
        assert!(!t.is_opaque_catalog(5, 2619), "old filenode may be reused",);
        assert!(
            t.is_opaque_catalog(6, 2619),
            "filenode tracking is per database",
        );
    }

    #[test]
    fn relmap_update_adds_post_rewrite_filenodes() {
        let mut t = CatalogTracker::new();
        let r = relmap_record(5, &[(1259, 50000)]);
        t.observe(&r, MAGIC);
        assert!(t.is_catalog(5, 50000));
        assert_eq!(t.relmap_updates, 1);
    }

    #[test]
    fn shared_relmap_visible_across_dbs() {
        let mut t = CatalogTracker::new();
        // pg_database (oid 1262) in shared/global (dbid 0)
        let r = relmap_record(0, &[(1262, 60000)]);
        t.observe(&r, MAGIC);
        assert!(t.is_catalog(0, 60000));
        assert!(t.is_catalog(99, 60000));
    }

    #[test]
    fn relmap_for_pg_class_updates_pg_class_filenode() {
        let mut t = CatalogTracker::new();
        let r = relmap_record(5, &[(1259, 50000), (1247, 60000)]);
        t.observe(&r, MAGIC);
        assert_eq!(t.pg_class_filenode.get(&5), Some(&50000));
    }

    #[test]
    fn pg_class_heap_insert_adds_non_mapped_catalog_filenode() {
        let mut t = CatalogTracker::new();
        // VACUUM FULL pg_namespace (oid 2615) -> fresh relfilenode
        let data = pg_class_block_data(2615, 30000);
        let rec = heap_block_record(RmId::Heap, 0x00, 5, 1259, data); // XLOG_HEAP_INSERT
        t.observe(&rec, MAGIC);
        assert!(t.is_catalog(5, 30000));
        assert_eq!(t.pg_class_writes_decoded, 1);
        assert_eq!(t.pg_class_writes_undecoded, 0);
    }

    #[test]
    fn pg_class_heap_update_adds_post_vacuum_full_filenode() {
        let mut t = CatalogTracker::new();
        // VACUUM FULL pg_depend (oid 2608) without prefix/suffix
        // compression; realistic prefixlen ≈ 88 shape covered by
        // pg_class_heap_update_with_prefix_compression_increments_oid_in_prefix
        let data = pg_class_block_data(2608, 40000);
        let rec = heap_block_record_with_main(
            RmId::Heap,
            0x20,
            5,
            1259,
            data,
            xl_heap_update_no_compression(),
        );
        t.observe(&rec, MAGIC);
        assert!(t.is_catalog(5, 40000));
        assert_eq!(t.pg_class_writes_decoded, 1);
        assert_eq!(t.pg_class_writes_oid_in_prefix, 0);
    }

    #[test]
    fn pg_class_heap_update_with_prefix_compression_increments_oid_in_prefix() {
        // VACUUM FULL non-mapped catalog: cols 1..7 unchanged so PG sets
        // XLH_UPDATE_PREFIX_FROM_OLD, prefixlen ≈ 88, OID in un-logged
        // prefix. Catalog set unchanged: can't tell which catalog owns it.
        let mut t = CatalogTracker::new();
        let data = pg_class_update_block_prefix_88(40000);
        let mut md = xl_heap_update_no_compression();
        md[7] = 0x20; // XLH_UPDATE_PREFIX_FROM_OLD
        let rec = heap_block_record_with_main(RmId::Heap, 0x20, 5, 1259, data, md);
        t.observe(&rec, MAGIC);
        assert_eq!(t.pg_class_writes_oid_in_prefix, 1);
        assert_eq!(t.pg_class_writes_undecoded, 0);
        assert_eq!(t.pg_class_writes_decoded, 0);
        assert!(!t.is_catalog(5, 40000));
    }

    #[test]
    fn pg_class_heap_insert_for_user_table_does_not_add() {
        let mut t = CatalogTracker::new();
        // CREATE TABLE: pg_class INSERT with oid >= 16384, must not add
        let data = pg_class_block_data(50000, 50001);
        let rec = heap_block_record(RmId::Heap, 0x00, 5, 1259, data);
        t.observe(&rec, MAGIC);
        assert!(!t.is_catalog(5, 50001));
        assert_eq!(t.pg_class_writes_decoded, 1); // decoded, filtered by oid range
    }

    #[test]
    fn pg_class_truncated_block_data_increments_undecoded() {
        let mut t = CatalogTracker::new();
        let rec = heap_block_record(RmId::Heap, 0x00, 5, 1259, vec![]);
        t.observe(&rec, MAGIC);
        assert_eq!(t.pg_class_writes_undecoded, 1);
        assert_eq!(t.pg_class_writes_decoded, 0);
    }

    #[test]
    fn pg_class_heap_record_with_non_insert_info_ignored() {
        let mut t = CatalogTracker::new();
        // 0x30 = HEAP_INPLACE: no new tuple, block data not
        // xl_heap_header + tuple, must skip
        let data = pg_class_block_data(2608, 40000);
        let rec = heap_block_record(RmId::Heap, 0x30, 5, 1259, data);
        t.observe(&rec, MAGIC);
        assert!(!t.is_catalog(5, 40000));
        assert_eq!(t.pg_class_writes_decoded, 0);
    }

    #[test]
    fn pg_class_heap_record_after_relmap_uses_new_filenode() {
        let mut t = CatalogTracker::new();
        // Source rotated pg_class to filenode 50000 first
        let rm = relmap_record(5, &[(1259, 50000)]);
        t.observe(&rm, MAGIC);
        // VACUUM FULL pg_depend; pg_class block now at 50000, not 1259.
        // Tests relmap -> pg_class filenode lookup, not the prefix path.
        let data = pg_class_block_data(2608, 70000);
        let rec = heap_block_record_with_main(
            RmId::Heap,
            0x20,
            5,
            50000,
            data,
            xl_heap_update_no_compression(),
        );
        t.observe(&rec, MAGIC);
        assert!(t.is_catalog(5, 70000));
        assert_eq!(t.pg_class_writes_decoded, 1);
    }

    #[test]
    fn relmap_malformed_main_data_is_ignored() {
        let mut t = CatalogTracker::new();
        let mut r = relmap_record(5, &[(1259, 50000)]);
        r.main_data.to_mut().truncate(8); // chop off nbytes
        t.observe(&r, MAGIC);
        assert!(!t.is_catalog(5, 50000));
        assert_eq!(t.relmap_updates, 1); // counted, no update applied
    }

    #[test]
    fn observe_relmap_update_signals() {
        let mut t = CatalogTracker::new();
        let v = t.observe(&relmap_record(5, &[(1259, 50000)]), MAGIC);
        assert!(v.catalog_write, "relmap update must signal");
        assert_eq!(t.invalidation_signals_sent, 1);
    }

    #[test]
    fn observe_pg_class_decoded_signals() {
        let mut t = CatalogTracker::new();
        let data = pg_class_block_data(2615, 30000);
        let v = t.observe(&heap_block_record(RmId::Heap, 0x00, 5, 1259, data), MAGIC);
        assert!(v.catalog_write, "decoded pg_class write must signal");
        assert_eq!(t.invalidation_signals_sent, 1);
    }

    #[test]
    fn observe_pg_class_oid_in_prefix_signals() {
        let mut t = CatalogTracker::new();
        let data = pg_class_update_block_prefix_88(40000);
        let mut md = xl_heap_update_no_compression();
        md[7] = 0x20;
        let v = t.observe(
            &heap_block_record_with_main(RmId::Heap, 0x20, 5, 1259, data, md),
            MAGIC,
        );
        assert!(
            v.catalog_write,
            "oid_in_prefix is still a catalog mutation — must signal",
        );
        assert_eq!(t.invalidation_signals_sent, 1);
    }

    #[test]
    fn observe_pg_class_undecoded_still_signals() {
        let mut t = CatalogTracker::new();
        // Undecoded but still touched pg_class: coarse signal, cache drops
        let v = t.observe(&heap_block_record(RmId::Heap, 0x00, 5, 1259, vec![]), MAGIC);
        assert!(v.catalog_write);
        assert_eq!(t.invalidation_signals_sent, 1);
        assert_eq!(t.pg_class_writes_undecoded, 1);
    }

    #[test]
    fn observe_verdict_matches_signal_kind() {
        // Verdict rides the record; the decoder worker bumps epochs off it
        // at its own stream position
        let mut t = CatalogTracker::new();
        assert!(
            t.observe(&relmap_record(5, &[(1259, 50000)]), MAGIC)
                .catalog_write
        );
        let data = pg_class_block_data(2615, 30000);
        assert!(
            t.observe(&heap_block_record(RmId::Heap, 0x00, 5, 50000, data), MAGIC)
                .catalog_write
        );
        // HEAP_DELETE on pg_class: DROP shape counts as a catalog write
        assert!(
            t.observe(
                &heap_block_record(RmId::Heap, 0x10, 5, 50000, vec![]),
                MAGIC
            )
            .catalog_write
        );
        // User-table write: no catalog effect
        assert!(
            !t.observe(
                &heap_block_record(RmId::Heap, 0x00, 5, 60000, vec![0u8; 16]),
                MAGIC
            )
            .catalog_write
        );
        // Malformed relmap: counted but not applied, no signal
        let mut r = relmap_record(5, &[(1247, 70000)]);
        r.main_data.to_mut().truncate(8);
        assert!(!t.observe(&r, MAGIC).catalog_write);
    }

    /// Filter admits capture input only for the followed database, so every
    /// catalog signal has to name the database it came from
    #[test]
    fn observation_carries_source_database() {
        let mut t = CatalogTracker::new();
        assert_eq!(
            t.observe(&relmap_record(6, &[(1259, 50000)]), MAGIC)
                .catalog_db_oid,
            Some(6)
        );
        assert!(t.is_catalog(6, 50000), "foreign mapping still installed");
        assert_eq!(
            t.observe(&relmap_record(0, &[(1262, 60000)]), MAGIC)
                .catalog_db_oid,
            Some(0),
            "shared map keeps PG's dbid 0"
        );
        // db 6's pg_class now lives at 50000
        let data = pg_class_block_data(2615, 30000);
        let obs = t.observe(&heap_block_record(RmId::Heap, 0x00, 6, 50000, data), MAGIC);
        assert_eq!(obs.catalog_db_oid, Some(6));
        assert_eq!(
            t.observe(
                &heap_block_record(RmId::Heap, 0x10, 6, 50000, vec![]),
                MAGIC
            )
            .catalog_db_oid,
            Some(6),
            "DROP-shaped delete names its database too"
        );
        assert_eq!(
            t.observe(
                &heap_block_record(RmId::Heap, 0x00, 6, 70000, vec![0u8; 16]),
                MAGIC
            )
            .catalog_db_oid,
            None,
            "user write is no catalog signal"
        );
    }

    #[test]
    fn observe_non_catalog_record_does_not_signal() {
        let mut t = CatalogTracker::new();
        // User-table relfilenode (no relmap seen), harvest skipped
        let rec = heap_block_record(RmId::Heap, 0x00, 5, 50000, vec![0u8; 16]);
        assert!(!t.observe(&rec, MAGIC).catalog_write);
        assert_eq!(t.invalidation_signals_sent, 0);
    }

    #[test]
    fn fresh_tracker_is_empty() {
        let t = CatalogTracker::new();
        assert!(t.is_empty(), "no learned nodes yet");
        assert_eq!(t.len(), 0);
    }

    #[test]
    fn add_grows_len_idempotently() {
        let mut t = CatalogTracker::new();
        t.add(5, 50000);
        t.add(5, 50000); // duplicate
        t.add(5, 50001);
        assert!(!t.is_empty());
        assert_eq!(t.len(), 2);
    }

    #[test]
    fn relmap_update_with_wrong_nbytes_is_ignored() {
        let mut t = CatalogTracker::new();
        let mut r = relmap_record(5, &[(1259, 50000)]);
        // nbytes at main_data[8..12]; mismatch must short-circuit
        r.main_data.to_mut()[8..12].copy_from_slice(&12345i32.to_le_bytes());
        t.observe(&r, MAGIC);
        assert!(!t.is_catalog(5, 50000));
        assert_eq!(t.relmap_updates, 1);
    }

    #[test]
    fn relmap_update_with_wrong_magic_is_ignored() {
        let mut t = CatalogTracker::new();
        let mut r = relmap_record(5, &[(1259, 50000)]);
        // magic at main_data[12..16] (12 header + magic offset 0)
        r.main_data.to_mut()[12..16].copy_from_slice(&0xDEADBEEFu32.to_le_bytes());
        t.observe(&r, MAGIC);
        assert!(!t.is_catalog(5, 50000));
    }

    #[test]
    fn relmap_update_rejects_oversized_num_mappings() {
        let mut t = CatalogTracker::new();
        let mut r = relmap_record(5, &[(1259, 50000)]);
        // num_mappings at main_data[16..20] (12 header + 4 magic)
        r.main_data.to_mut()[16..20].copy_from_slice(&((MAX_MAPPINGS + 1) as i32).to_le_bytes());
        t.observe(&r, MAGIC);
        assert!(!t.is_catalog(5, 50000));
    }

    #[test]
    fn relmap_update_skips_zero_mapping_entries() {
        let mut t = CatalogTracker::new();
        // mapoid=0 or filenum=0 entries are absentees, must not pollute
        let r = relmap_record(5, &[(0, 50000), (1259, 0)]);
        t.observe(&r, MAGIC);
        assert!(t.is_empty(), "zero-tagged entries must be skipped");
    }

    #[test]
    fn relmap_update_with_truncated_main_data_is_ignored() {
        let mut t = CatalogTracker::new();
        let mut r = relmap_record(5, &[(1259, 50000)]);
        r.main_data.to_mut().truncate(4); // len < 12 + REL_MAP_FILE_SIZE
        t.observe(&r, MAGIC);
        assert!(!t.is_catalog(5, 50000));
    }

    /// pg_class tuple naming `relname` in `relnamespace`
    fn named_block_data(oid: u32, relname: &str, relnamespace: u32, relfilenode: u32) -> Vec<u8> {
        let mut v = pg_class_block_data(oid, relfilenode);
        v[10..10 + relname.len()].copy_from_slice(relname.as_bytes());
        v[74..78].copy_from_slice(&relnamespace.to_le_bytes());
        v
    }

    fn with_xid(mut rec: XLogRecord<'static>, xid: u32) -> XLogRecord<'static> {
        rec.header.xact_id = xid;
        rec
    }

    fn smgr_create(db: u32, rel: u32, xid: u32) -> XLogRecord<'static> {
        let mut md = Vec::new();
        for v in [1663u32, db, rel, 0] {
            md.extend_from_slice(&v.to_le_bytes());
        }
        XLogRecord {
            header: XLogRecordHeader {
                resource_manager_id: RmId::Smgr as u8,
                info: XLOG_SMGR_CREATE,
                xact_id: xid,
                ..Default::default()
            },
            main_data: std::borrow::Cow::Owned(md),
            ..Default::default()
        }
    }

    /// xl_heap_update main data naming old and new offnum on one page
    fn update_main_data(old_off: u16, new_off: u16, flags: u8) -> Vec<u8> {
        let mut md = vec![0u8; 14];
        md[4..6].copy_from_slice(&old_off.to_le_bytes());
        md[7] = flags;
        md[12..14].copy_from_slice(&new_off.to_le_bytes());
        md
    }

    #[test]
    fn catalog_rebuild_claims_transient_heap_and_its_xact_storage() {
        let mut t = CatalogTracker::new();
        let insert = heap_block_record_with_main(
            RmId::Heap,
            0x00,
            5,
            1259,
            named_block_data(50000, "pg_temp_2608", 11, 50000),
            1u16.to_le_bytes().to_vec(),
        );
        t.observe(&with_xid(insert, 900), MAGIC);
        assert!(t.is_catalog(5, 50000), "transient heap is catalog storage");
        t.observe(&smgr_create(5, 50003, 900), MAGIC);
        assert!(t.is_catalog(5, 50003), "toast or rebuilt index");
        t.observe(&smgr_create(5, 50004, 901), MAGIC);
        assert!(!t.is_catalog(5, 50004), "other xact");
        let delete = heap_block_record_with_main(RmId::Heap, 0x10, 5, 1259, vec![], vec![0; 6]);
        t.observe(&with_xid(delete, 900), MAGIC);
        t.observe(&smgr_create(5, 50005, 900), MAGIC);
        assert!(!t.is_catalog(5, 50005), "transient drop closes the rebuild");
    }

    #[test]
    fn user_rebuild_claims_nothing() {
        let mut t = CatalogTracker::new();
        let insert = heap_block_record_with_main(
            RmId::Heap,
            0x00,
            5,
            1259,
            named_block_data(50000, "pg_temp_16400", 2200, 50000),
            1u16.to_le_bytes().to_vec(),
        );
        t.observe(&with_xid(insert, 900), MAGIC);
        t.observe(&smgr_create(5, 50003, 900), MAGIC);
        assert!(!t.is_catalog(5, 50000));
        assert!(!t.is_catalog(5, 50003));
    }

    #[test]
    fn prefix_compressed_update_resolves_through_old_slot() {
        let mut t = CatalogTracker::new();
        // Decoded update parks pg_depend_reference_index at (0, 7)
        let seed = heap_block_record_with_main(
            RmId::Heap,
            0x40,
            5,
            1259,
            pg_class_block_data(2674, 2674),
            update_main_data(3, 7, 0),
        );
        t.observe(&seed, MAGIC);
        // REINDEX moves it to 40000 at (0, 9), OID compressed away
        let reindex = heap_block_record_with_main(
            RmId::Heap,
            0x40,
            5,
            1259,
            pg_class_update_block_prefix_88(40000),
            update_main_data(7, 9, 0x20),
        );
        t.observe(&reindex, MAGIC);
        assert!(t.is_catalog(5, 40000));
        // Chained rotation resolves from the new slot
        let again = heap_block_record_with_main(
            RmId::Heap,
            0x40,
            5,
            1259,
            pg_class_update_block_prefix_88(40100),
            update_main_data(9, 10, 0x20),
        );
        t.observe(&again, MAGIC);
        assert!(t.is_catalog(5, 40100));
    }

    #[test]
    fn user_row_takes_over_slot() {
        let mut t = CatalogTracker::new();
        t.catalog_slots.insert((5, 1259, 0, 7), (2674, 2674));
        // User row lands at (0, 7) after the catalog row moved on
        let insert = heap_block_record_with_main(
            RmId::Heap,
            0x00,
            5,
            1259,
            pg_class_block_data(50000, 50000),
            7u16.to_le_bytes().to_vec(),
        );
        t.observe(&insert, MAGIC);
        let rotate = heap_block_record_with_main(
            RmId::Heap,
            0x40,
            5,
            1259,
            pg_class_update_block_prefix_88(40000),
            update_main_data(7, 8, 0x20),
        );
        t.observe(&rotate, MAGIC);
        assert!(!t.is_catalog(5, 40000), "user index rotation stays user");
    }

    #[test]
    fn pg_class_rewrite_drops_slots_of_old_filenode() {
        let mut t = CatalogTracker::new();
        t.catalog_slots.insert((5, 1259, 0, 7), (2674, 2674));
        t.observe(&relmap_record(5, &[(1259, 50000)]), MAGIC);
        assert!(t.catalog_slots.is_empty());
    }

    #[test]
    fn parse_ctid_reads_tid_text() {
        assert_eq!(parse_ctid("(12,3)"), Some((12, 3)));
        assert_eq!(parse_ctid("12,3"), None);
        assert_eq!(parse_ctid("(12,70000)"), None);
    }
}
