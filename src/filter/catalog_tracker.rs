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
//!   inserts into pg_class out of the catalog set.
//! * [`seed_from_source`](CatalogTracker::seed_from_source) — closes the
//!   hole where a long-running source rotated a mapped catalog above
//!   16384 before walshadow attached, so its `XLOG_RELMAP_UPDATE` sits
//!   in pre-attach WAL the bootstrap rule never sees.
//!

use std::collections::{HashMap, HashSet};

use thiserror::Error;
use tokio_postgres::Client;
use tokio_postgres::types::Oid;
use walrus::pg::walparser::{RmId, XLogRecord};

use crate::filter::pg_class_decoder::{
    DecodeOutcome, decode_pg_class_tuple, info_carries_new_tuple_heap, info_carries_new_tuple_heap2,
};
use crate::schema::FIRST_NORMAL_OBJECT_ID;

/// XLOG_RELMAP_UPDATE info byte (`xl_info & XLR_RMGR_INFO_MASK`).
const XLOG_RELMAP_UPDATE: u8 = 0x00;
/// `RELMAPPER_FILEMAGIC` from `src/backend/utils/cache/relmapper.c`.
const RELMAPPER_FILEMAGIC: i32 = 0x592717;
const MAX_MAPPINGS: usize = 64;
const REL_MAP_FILE_SIZE: usize = 4 + 4 + MAX_MAPPINGS * 8 + 4; // magic + n + mappings + crc

/// `pg_class.oid`, fixed PG catalog OID
pub const PG_CLASS_OID: u32 = 1259;
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
    relmap_updates: u64,
    /// pg_class heap writes the decoder couldn't reconstruct (truncated /
    /// malformed `t_hoff`). OID-prefix-compressed records count in
    /// `pg_class_writes_oid_in_prefix` instead.
    pg_class_writes_undecoded: u64,
    pg_class_writes_decoded: u64,
    /// pg_class UPDATE / HOT_UPDATE that prefix-compressed past the OID
    /// (`XLH_UPDATE_PREFIX_FROM_OLD`, `prefixlen > 0`). WAL alone can't
    /// reconstruct `(oid, relfilenode)`; rotated filenode learned via seed
    /// snapshot or later `XLOG_RELMAP_UPDATE`. Typical: VACUUM FULL on a
    /// non-mapped catalog (pg_depend, pg_namespace, …).
    pg_class_writes_oid_in_prefix: u64,
    seeded_from_source: u64,
    /// Non-`None` verdicts returned by `observe`; catalog's
    /// `generation_bumps` may lag as it collapses bumps between lookups
    /// into one `invalidate`.
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

impl CatalogTrackerStats {
    pub fn delta_from(self, previous: Self) -> Self {
        Self {
            relmap_updates: self.relmap_updates - previous.relmap_updates,
            pg_class_writes_undecoded: self.pg_class_writes_undecoded
                - previous.pg_class_writes_undecoded,
            pg_class_writes_decoded: self.pg_class_writes_decoded
                - previous.pg_class_writes_decoded,
            pg_class_writes_oid_in_prefix: self.pg_class_writes_oid_in_prefix
                - previous.pg_class_writes_oid_in_prefix,
            seeded_from_source: self.seeded_from_source - previous.seeded_from_source,
            invalidation_signals_sent: self.invalidation_signals_sent
                - previous.invalidation_signals_sent,
        }
    }
}

#[derive(Debug, Error)]
pub enum SeedError {
    #[error("pg: {0}")]
    Pg(#[from] tokio_postgres::Error),
}

/// [`CatalogTracker::observe`] verdict: whether the record mutated a
/// tracked catalog plus, when block 0 decoded a user relation's pg_class
/// row, that oid — the filter's per-oid first-touch source for boundary
/// capture.
#[derive(Debug, Default, Clone, Copy)]
pub struct Observation {
    pub catalog_write: bool,
    pub pg_class_user_oid: Option<u32>,
}

impl Observation {
    fn write(catalog_write: bool) -> Self {
        Self {
            catalog_write,
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

    /// Filters internally on rmgr + info; safe to call unconditionally.
    /// Returned verdict rides the record to the decoder worker, which
    /// bumps invalidation epochs at its own stream position (see module
    /// doc).
    pub fn observe(&mut self, record: &XLogRecord) -> Observation {
        let rm = record.header.resource_manager_id;
        let info_high = record.header.info & 0xF0;

        if rm == RmId::RelMap as u8 && info_high == XLOG_RELMAP_UPDATE {
            return Observation::write(self.handle_relmap_update(record));
        }

        let heap_new_tuple = rm == RmId::Heap as u8 && info_carries_new_tuple_heap(info_high);
        let heap2_new_tuple = rm == RmId::Heap2 as u8 && info_carries_new_tuple_heap2(info_high);
        if heap_new_tuple || heap2_new_tuple {
            return self.harvest_pg_class_blocks(record);
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
                return Observation::write(self.signal_pg_class_touch(record));
            }
        }
        Observation::default()
    }

    /// Coarse-fire (no row decode) when a record hits the current
    /// pg_class filenode, for ops the harvest path skips (DELETE). The
    /// `InvalidateSweep` verdict arms the DROP sweep at the worker
    /// ([`PendingSweeps`], keyed by the record's xid).
    fn signal_pg_class_touch(&mut self, record: &XLogRecord) -> bool {
        if self.pg_class_block(record).is_none() {
            return false;
        }
        self.invalidation_signals_sent += 1;
        true
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
    fn harvest_pg_class_blocks(&mut self, record: &XLogRecord) -> Observation {
        let Some((db, _rel)) = self.pg_class_block(record) else {
            return Observation::default();
        };
        let mut user_oid = None;
        match decode_pg_class_tuple(record, 0) {
            DecodeOutcome::Decoded(row) => {
                self.pg_class_writes_decoded += 1;
                if row.oid != 0 && row.oid < FIRST_NORMAL_OBJECT_ID && row.relfilenode != 0 {
                    self.nodes.insert((db, row.relfilenode));
                    if row.oid == PG_NAMESPACE_OID {
                        self.pg_namespace_filenode.insert(db, row.relfilenode);
                    }
                }
                if row.oid >= FIRST_NORMAL_OBJECT_ID {
                    user_oid = Some(row.oid);
                }
            }
            DecodeOutcome::OidInPrefix => {
                self.pg_class_writes_oid_in_prefix += 1;
            }
            DecodeOutcome::Undecoded => {
                // Cache must still drop: PG 17 ALTER ADD COLUMN emits a
                // pg_class HOT_UPDATE whose new tuple omits the relnatts
                // prefix; silent skip shipped c=NULL for post-ALTER rows
                // decoded against the stale 2-column descriptor.
                self.pg_class_writes_undecoded += 1;
            }
        }
        // Coarse-fire regardless: over-invalidation is cheap (lazy
        // refetch), under-invalidation silently masks DDL.
        self.invalidation_signals_sent += 1;
        Observation {
            catalog_write: true,
            pg_class_user_oid: user_oid,
        }
    }

    /// True when `(db, rel)` is pg_namespace's current heap — the
    /// capture-all trigger set
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

    fn handle_relmap_update(&mut self, record: &XLogRecord) -> bool {
        self.relmap_updates += 1;
        let md = &record.main_data;
        // xl_relmap_update header: dbid(4) + tsid(4) + nbytes(4) = 12
        if md.len() < 12 + REL_MAP_FILE_SIZE {
            return false;
        }
        let dbid = u32::from_le_bytes(md[0..4].try_into().unwrap());
        let _tsid = u32::from_le_bytes(md[4..8].try_into().unwrap());
        let nbytes = i32::from_le_bytes(md[8..12].try_into().unwrap()) as usize;
        if nbytes != REL_MAP_FILE_SIZE {
            return false;
        }
        let map = &md[12..12 + REL_MAP_FILE_SIZE];
        let magic = i32::from_le_bytes(map[0..4].try_into().unwrap());
        if magic != RELMAPPER_FILEMAGIC {
            return false;
        }
        let num_mappings = i32::from_le_bytes(map[4..8].try_into().unwrap()) as usize;
        if num_mappings > MAX_MAPPINGS {
            return false;
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
                }
            }
        }
        self.invalidation_signals_sent += 1;
        true
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
                    pg_relation_filenode(c.oid) AS filenode \
                 FROM pg_class c \
                 WHERE c.oid < 16384 \
                   AND pg_relation_filenode(c.oid) IS NOT NULL",
                &[],
            )
            .await?;
        let mut added = 0usize;
        for row in &rows {
            let db_node: Oid = row.get(0);
            let catalog_oid: Oid = row.get(1);
            let filenode: Oid = row.get(2);
            if filenode == 0 {
                continue;
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
        BlockLocation, RelFileNode, XLogRecordBlock, XLogRecordBlockHeader, XLogRecordHeader,
    };

    use super::test_relmap_record as relmap_record;

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
    fn relmap_update_adds_post_rewrite_filenodes() {
        let mut t = CatalogTracker::new();
        let r = relmap_record(5, &[(1259, 50000)]);
        t.observe(&r);
        assert!(t.is_catalog(5, 50000));
        assert_eq!(t.relmap_updates, 1);
    }

    #[test]
    fn shared_relmap_visible_across_dbs() {
        let mut t = CatalogTracker::new();
        // pg_database (oid 1262) in shared/global (dbid 0)
        let r = relmap_record(0, &[(1262, 60000)]);
        t.observe(&r);
        assert!(t.is_catalog(0, 60000));
        assert!(t.is_catalog(99, 60000));
    }

    #[test]
    fn relmap_for_pg_class_updates_pg_class_filenode() {
        let mut t = CatalogTracker::new();
        let r = relmap_record(5, &[(1259, 50000), (1247, 60000)]);
        t.observe(&r);
        assert_eq!(t.pg_class_filenode.get(&5), Some(&50000));
    }

    #[test]
    fn pg_class_heap_insert_adds_non_mapped_catalog_filenode() {
        let mut t = CatalogTracker::new();
        // VACUUM FULL pg_namespace (oid 2615) -> fresh relfilenode
        let data = pg_class_block_data(2615, 30000);
        let rec = heap_block_record(RmId::Heap, 0x00, 5, 1259, data); // XLOG_HEAP_INSERT
        t.observe(&rec);
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
        t.observe(&rec);
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
        t.observe(&rec);
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
        t.observe(&rec);
        assert!(!t.is_catalog(5, 50001));
        assert_eq!(t.pg_class_writes_decoded, 1); // decoded, filtered by oid range
    }

    #[test]
    fn pg_class_truncated_block_data_increments_undecoded() {
        let mut t = CatalogTracker::new();
        let rec = heap_block_record(RmId::Heap, 0x00, 5, 1259, vec![]);
        t.observe(&rec);
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
        t.observe(&rec);
        assert!(!t.is_catalog(5, 40000));
        assert_eq!(t.pg_class_writes_decoded, 0);
    }

    #[test]
    fn pg_class_heap_record_after_relmap_uses_new_filenode() {
        let mut t = CatalogTracker::new();
        // Source rotated pg_class to filenode 50000 first
        let rm = relmap_record(5, &[(1259, 50000)]);
        t.observe(&rm);
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
        t.observe(&rec);
        assert!(t.is_catalog(5, 70000));
        assert_eq!(t.pg_class_writes_decoded, 1);
    }

    #[test]
    fn relmap_malformed_main_data_is_ignored() {
        let mut t = CatalogTracker::new();
        let mut r = relmap_record(5, &[(1259, 50000)]);
        r.main_data.to_mut().truncate(8); // chop off nbytes
        t.observe(&r);
        assert!(!t.is_catalog(5, 50000));
        assert_eq!(t.relmap_updates, 1); // counted, no update applied
    }

    #[test]
    fn observe_relmap_update_signals() {
        let mut t = CatalogTracker::new();
        let v = t.observe(&relmap_record(5, &[(1259, 50000)]));
        assert!(v.catalog_write, "relmap update must signal");
        assert_eq!(t.invalidation_signals_sent, 1);
    }

    #[test]
    fn observe_pg_class_decoded_signals() {
        let mut t = CatalogTracker::new();
        let data = pg_class_block_data(2615, 30000);
        let v = t.observe(&heap_block_record(RmId::Heap, 0x00, 5, 1259, data));
        assert!(v.catalog_write, "decoded pg_class write must signal");
        assert_eq!(t.invalidation_signals_sent, 1);
    }

    #[test]
    fn observe_pg_class_oid_in_prefix_signals() {
        let mut t = CatalogTracker::new();
        let data = pg_class_update_block_prefix_88(40000);
        let mut md = xl_heap_update_no_compression();
        md[7] = 0x20;
        let v = t.observe(&heap_block_record_with_main(
            RmId::Heap,
            0x20,
            5,
            1259,
            data,
            md,
        ));
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
        let v = t.observe(&heap_block_record(RmId::Heap, 0x00, 5, 1259, vec![]));
        assert!(v.catalog_write);
        assert_eq!(t.invalidation_signals_sent, 1);
        assert_eq!(t.pg_class_writes_undecoded, 1);
    }

    #[test]
    fn observe_verdict_matches_signal_kind() {
        // Verdict rides the record; the decoder worker bumps epochs off it
        // at its own stream position
        let mut t = CatalogTracker::new();
        assert!(t.observe(&relmap_record(5, &[(1259, 50000)])).catalog_write);
        let data = pg_class_block_data(2615, 30000);
        assert!(
            t.observe(&heap_block_record(RmId::Heap, 0x00, 5, 50000, data))
                .catalog_write
        );
        // HEAP_DELETE on pg_class: DROP shape counts as a catalog write
        assert!(
            t.observe(&heap_block_record(RmId::Heap, 0x10, 5, 50000, vec![]))
                .catalog_write
        );
        // User-table write: no catalog effect
        assert!(
            !t.observe(&heap_block_record(
                RmId::Heap,
                0x00,
                5,
                60000,
                vec![0u8; 16]
            ))
            .catalog_write
        );
        // Malformed relmap: counted but not applied, no signal
        let mut r = relmap_record(5, &[(1247, 70000)]);
        r.main_data.to_mut().truncate(8);
        assert!(!t.observe(&r).catalog_write);
    }

    #[test]
    fn observe_non_catalog_record_does_not_signal() {
        let mut t = CatalogTracker::new();
        // User-table relfilenode (no relmap seen), harvest skipped
        let rec = heap_block_record(RmId::Heap, 0x00, 5, 50000, vec![0u8; 16]);
        assert!(!t.observe(&rec).catalog_write);
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
        t.observe(&r);
        assert!(!t.is_catalog(5, 50000));
        assert_eq!(t.relmap_updates, 1);
    }

    #[test]
    fn relmap_update_with_wrong_magic_is_ignored() {
        let mut t = CatalogTracker::new();
        let mut r = relmap_record(5, &[(1259, 50000)]);
        // magic at main_data[12..16] (12 header + magic offset 0)
        r.main_data.to_mut()[12..16].copy_from_slice(&0xDEADBEEFu32.to_le_bytes());
        t.observe(&r);
        assert!(!t.is_catalog(5, 50000));
    }

    #[test]
    fn relmap_update_rejects_oversized_num_mappings() {
        let mut t = CatalogTracker::new();
        let mut r = relmap_record(5, &[(1259, 50000)]);
        // num_mappings at main_data[16..20] (12 header + 4 magic)
        r.main_data.to_mut()[16..20].copy_from_slice(&((MAX_MAPPINGS + 1) as i32).to_le_bytes());
        t.observe(&r);
        assert!(!t.is_catalog(5, 50000));
    }

    #[test]
    fn relmap_update_skips_zero_mapping_entries() {
        let mut t = CatalogTracker::new();
        // mapoid=0 or filenum=0 entries are absentees, must not pollute
        let r = relmap_record(5, &[(0, 50000), (1259, 0)]);
        t.observe(&r);
        assert!(t.is_empty(), "zero-tagged entries must be skipped");
    }

    #[test]
    fn relmap_update_with_truncated_main_data_is_ignored() {
        let mut t = CatalogTracker::new();
        let mut r = relmap_record(5, &[(1259, 50000)]);
        r.main_data.to_mut().truncate(4); // len < 12 + REL_MAP_FILE_SIZE
        t.observe(&r);
        assert!(!t.is_catalog(5, 50000));
    }
}
