//! Live catalog relfilenode set.
//!
//! Bootstrap rule: every rel_node < FirstNormalObjectId (16384) starts
//! in the catalog set. Matches `classify::is_catalog_relnode` for parity
//! with Phase 0.
//!
//! Updates:
//! * `RM_RELMAP_ID / XLOG_RELMAP_UPDATE` — authoritative for mapped
//!   catalogs (pg_class, pg_attribute, pg_type, pg_proc, pg_database,
//!   pg_authid, pg_shdepend, …). Body is `xl_relmap_update` + a
//!   `RelMapFile` blob (magic + mappings + crc, see PG
//!   `src/backend/utils/cache/relmapper.c`). Each non-zero mapping
//!   `(mapoid, mapfilenumber)` adds `mapfilenumber` to the catalog
//!   set for that database (or the shared set if `dbid == 0`).
//! * Heap writes to `pg_class` — decoded via [`pg_class_decoder`]
//!   (PRE5 item 2). Carries new relfilenodes for non-mapped catalogs
//!   after `VACUUM FULL` / `REINDEX` / `CLUSTER`. Filters on
//!   `oid < FirstNormalObjectId` so user-table inserts into pg_class
//!   never pollute the catalog set.
//! * [`seed_from_source`](CatalogTracker::seed_from_source) — bootstrap
//!   from a libpq connection to the source PG before the replication
//!   cursor advances. Closes the "long-running source has already
//!   rotated a mapped catalog above 16384 before walshadow attaches"
//!   hole that the < 16384 bootstrap rule misses on its own.

use std::collections::{HashMap, HashSet};

use thiserror::Error;
use tokio_postgres::Client;
use tokio_postgres::types::Oid;
use wal_rs::pg::walparser::{RmId, XLogRecord};

use crate::classify::FIRST_NORMAL_OBJECT_ID;
use crate::pg_class_decoder::{
    decode_pg_class_tuple, info_carries_new_tuple_heap, info_carries_new_tuple_heap2,
};

/// XLOG_RELMAP_UPDATE info byte (`xl_info & XLR_RMGR_INFO_MASK`).
const XLOG_RELMAP_UPDATE: u8 = 0x00;
/// `RELMAPPER_FILEMAGIC` from `src/backend/utils/cache/relmapper.c`.
const RELMAPPER_FILEMAGIC: i32 = 0x592717;
const MAX_MAPPINGS: usize = 64;
const REL_MAP_FILE_SIZE: usize = 4 + 4 + MAX_MAPPINGS * 8 + 4; // magic + n + mappings + crc

/// `pg_class.oid` — fixed PG catalog OID since forever.
pub const PG_CLASS_OID: u32 = 1259;

#[derive(Debug, Default)]
pub struct CatalogTracker {
    /// `(db_node, rel_node)` for per-database catalogs. `db_node == 0`
    /// is the shared catalog set; queries on any db must consult it.
    nodes: HashSet<(u32, u32)>,
    /// Current pg_class filenode per database. Seeded by relmap update
    /// for oid=1259 and by [`seed_from_source`](Self::seed_from_source).
    /// Empty bootstrap falls through to the `rel == PG_CLASS_OID`
    /// initial-relfilenode check (mapped-catalog convention).
    pg_class_filenode: HashMap<u32, u32>,
    /// Count of relmap updates observed (debug / metrics).
    pub relmap_updates: u64,
    /// pg_class heap writes whose payload failed `pg_class_decoder`
    /// (truncated block data, missing column data). Successful decodes
    /// do not increment this — see `pg_class_writes_decoded`.
    pub pg_class_writes_undecoded: u64,
    /// pg_class heap writes successfully decoded. Catalog filenodes
    /// (oid < FirstNormalObjectId) extracted from these are added to
    /// `nodes`. User-table inserts decode fine but their oid trips the
    /// catalog filter and they are not added.
    pub pg_class_writes_decoded: u64,
    /// `(db_node, filenode)` pairs added at attach time via
    /// [`seed_from_source`](Self::seed_from_source).
    pub seeded_from_source: u64,
}

#[derive(Debug, Error)]
pub enum SeedError {
    #[error("pg: {0}")]
    Pg(#[from] tokio_postgres::Error),
}

impl CatalogTracker {
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a `(db, rel)` pair to the catalog set explicitly.
    pub fn add(&mut self, db_node: u32, rel_node: u32) {
        self.nodes.insert((db_node, rel_node));
    }

    /// True if `(db, rel)` is currently catalog. `rel < FIRST_NORMAL_OBJECT_ID`
    /// is the bootstrap rule; tracked relmap updates add post-rewrite
    /// filenumbers. Shared catalogs (`db_node == 0`) always treated as
    /// catalog (pg_database, pg_authid, pg_tablespace, etc.).
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

    /// Feed every record through here. Filters internally on rmgr
    /// + info so callers can call unconditionally.
    pub fn observe(&mut self, record: &XLogRecord) {
        let rm = record.header.resource_manager_id;
        let info_high = record.header.info & 0xF0;

        if rm == RmId::RelMap as u8 && info_high == XLOG_RELMAP_UPDATE {
            self.handle_relmap_update(record);
            return;
        }

        let heap_new_tuple = rm == RmId::Heap as u8 && info_carries_new_tuple_heap(info_high);
        let heap2_new_tuple = rm == RmId::Heap2 as u8 && info_carries_new_tuple_heap2(info_high);
        if heap_new_tuple || heap2_new_tuple {
            self.harvest_pg_class_blocks(record);
        }
    }

    /// For each block in `record` that targets pg_class, decode its
    /// new-tuple payload and add `(db, relfilenode)` if the tuple's
    /// oid falls in the catalog range.
    fn harvest_pg_class_blocks(&mut self, record: &XLogRecord) {
        for blk in &record.blocks {
            let (db, rel) = (
                blk.header.location.rel.db_node,
                blk.header.location.rel.rel_node,
            );
            if !self.is_pg_class_relfilenode(db, rel) {
                continue;
            }
            match decode_pg_class_tuple(&blk.data) {
                Some(row) => {
                    self.pg_class_writes_decoded += 1;
                    if row.oid != 0 && row.oid < FIRST_NORMAL_OBJECT_ID && row.relfilenode != 0 {
                        self.nodes.insert((db, row.relfilenode));
                    }
                }
                None => self.pg_class_writes_undecoded += 1,
            }
        }
    }

    /// True iff `(db, rel)` is the current pg_class filenode for `db`.
    /// Falls back to `rel == PG_CLASS_OID` when nothing has been observed
    /// for `db` yet (initial mapped-catalog convention: relfilenode
    /// equals oid until the first RELMAP rewrite).
    fn is_pg_class_relfilenode(&self, db: u32, rel: u32) -> bool {
        match self.pg_class_filenode.get(&db) {
            Some(&fnum) => fnum == rel,
            None => rel == PG_CLASS_OID,
        }
    }

    fn handle_relmap_update(&mut self, record: &XLogRecord) {
        self.relmap_updates += 1;
        let md = &record.main_data;
        // xl_relmap_update: dbid(4) + tsid(4) + nbytes(4) = 12 bytes
        if md.len() < 12 + REL_MAP_FILE_SIZE {
            return;
        }
        let dbid = u32::from_le_bytes(md[0..4].try_into().unwrap());
        let _tsid = u32::from_le_bytes(md[4..8].try_into().unwrap());
        let nbytes = i32::from_le_bytes(md[8..12].try_into().unwrap()) as usize;
        if nbytes != REL_MAP_FILE_SIZE {
            return;
        }
        let map = &md[12..12 + REL_MAP_FILE_SIZE];
        let magic = i32::from_le_bytes(map[0..4].try_into().unwrap());
        if magic != RELMAPPER_FILEMAGIC {
            return;
        }
        let num_mappings = i32::from_le_bytes(map[4..8].try_into().unwrap()) as usize;
        if num_mappings > MAX_MAPPINGS {
            return;
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
    }

    /// Populate the catalog set & pg_class-filenode map by querying the
    /// source PG's `pg_class` for every catalog relation (oid < 16384).
    /// Closes the "rotated mapped catalog before walshadow attached"
    /// hole — the bootstrap rule otherwise misses post-rewrite filenodes
    /// because the corresponding `XLOG_RELMAP_UPDATE` sits in pre-attach
    /// WAL walshadow never sees.
    ///
    /// Shared catalogs are seeded under `db_node = 0`; per-db under the
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
        }
        self.seeded_from_source += added as u64;
        Ok(added)
    }

    /// Size of the tracked set, for metrics.
    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use wal_rs::pg::walparser::{
        BlockLocation, RelFileNode, XLogRecordBlock, XLogRecordBlockHeader, XLogRecordHeader,
    };

    fn relmap_record(dbid: u32, mappings: &[(u32, u32)]) -> XLogRecord {
        let mut data = Vec::new();
        data.extend_from_slice(&dbid.to_le_bytes());
        data.extend_from_slice(&1664u32.to_le_bytes()); // tsid pg_global
        data.extend_from_slice(&(REL_MAP_FILE_SIZE as i32).to_le_bytes());
        // RelMapFile
        data.extend_from_slice(&RELMAPPER_FILEMAGIC.to_le_bytes());
        data.extend_from_slice(&(mappings.len() as i32).to_le_bytes());
        for &(oid, fnum) in mappings {
            data.extend_from_slice(&oid.to_le_bytes());
            data.extend_from_slice(&fnum.to_le_bytes());
        }
        // Pad rest of mappings array
        for _ in mappings.len()..MAX_MAPPINGS {
            data.extend_from_slice(&[0u8; 8]);
        }
        data.extend_from_slice(&0u32.to_le_bytes()); // crc, ignored

        XLogRecord {
            header: XLogRecordHeader {
                resource_manager_id: RmId::RelMap as u8,
                info: XLOG_RELMAP_UPDATE,
                total_record_length: 24 + data.len() as u32,
                ..Default::default()
            },
            main_data_len: data.len() as u32,
            main_data: data,
            ..Default::default()
        }
    }

    fn heap_block_record(rm: RmId, info: u8, db: u32, rel: u32, data: Vec<u8>) -> XLogRecord {
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
                data,
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    /// Build an xl_heap_header (t_infomask2, t_infomask, t_hoff) +
    /// payload that decodes to a pg_class tuple with the given oid and
    /// relfilenode. No nulls, t_hoff = 24 (no bitmap).
    fn pg_class_block_data(oid: u32, relfilenode: u32) -> Vec<u8> {
        // xl_heap_header: t_infomask2 (col count + flags), t_infomask
        // (HEAP_HASOID and HASNULL bits — we use 0 for no nulls,
        // no system oid), t_hoff.
        let mut v = Vec::new();
        v.extend_from_slice(&33u16.to_le_bytes()); // t_infomask2 (PG 18 pg_class natts)
        v.extend_from_slice(&0u16.to_le_bytes()); // t_infomask (no nulls)
        v.push(24); // t_hoff = MAXALIGN(SizeOfHeapTupleHeader) when no nulls
        // 1 byte of MAXALIGN padding to bring offset from 23 → 24 in the
        // reconstructed tuple.
        v.push(0);
        // Column data
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
        // pg_class (oid 1259) rewritten to filenode 50000 in db 5
        let r = relmap_record(5, &[(1259, 50000)]);
        t.observe(&r);
        assert!(t.is_catalog(5, 50000));
        assert_eq!(t.relmap_updates, 1);
    }

    #[test]
    fn shared_relmap_visible_across_dbs() {
        let mut t = CatalogTracker::new();
        // pg_database (oid 1262) rewritten in shared/global (dbid 0)
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
        // INSERT into pg_class for pg_namespace (oid 2615) carrying
        // a fresh relfilenode 30000 (e.g., VACUUM FULL pg_namespace).
        let data = pg_class_block_data(2615, 30000);
        // info_high = 0x00 = XLOG_HEAP_INSERT
        let rec = heap_block_record(RmId::Heap, 0x00, 5, 1259, data);
        t.observe(&rec);
        assert!(t.is_catalog(5, 30000));
        assert_eq!(t.pg_class_writes_decoded, 1);
        assert_eq!(t.pg_class_writes_undecoded, 0);
    }

    #[test]
    fn pg_class_heap_update_adds_post_vacuum_full_filenode() {
        let mut t = CatalogTracker::new();
        // UPDATE on pg_class row for pg_depend (oid 2608) to a fresh
        // relfilenode after VACUUM FULL pg_depend.
        let data = pg_class_block_data(2608, 40000);
        // info_high = 0x20 = XLOG_HEAP_UPDATE
        let rec = heap_block_record(RmId::Heap, 0x20, 5, 1259, data);
        t.observe(&rec);
        assert!(t.is_catalog(5, 40000));
        assert_eq!(t.pg_class_writes_decoded, 1);
    }

    #[test]
    fn pg_class_heap_insert_for_user_table_does_not_add() {
        let mut t = CatalogTracker::new();
        // CREATE TABLE user_t generates an INSERT into pg_class with
        // oid >= 16384. Tracker must NOT add the new filenode.
        let data = pg_class_block_data(50000, 50001);
        let rec = heap_block_record(RmId::Heap, 0x00, 5, 1259, data);
        t.observe(&rec);
        assert!(!t.is_catalog(5, 50001));
        // Decoded successfully, just filtered by oid range.
        assert_eq!(t.pg_class_writes_decoded, 1);
    }

    #[test]
    fn pg_class_truncated_block_data_increments_undecoded() {
        let mut t = CatalogTracker::new();
        // Block targeting pg_class with no payload — too short to
        // decode anything.
        let rec = heap_block_record(RmId::Heap, 0x00, 5, 1259, vec![]);
        t.observe(&rec);
        assert_eq!(t.pg_class_writes_undecoded, 1);
        assert_eq!(t.pg_class_writes_decoded, 0);
    }

    #[test]
    fn pg_class_heap_record_with_non_insert_info_ignored() {
        let mut t = CatalogTracker::new();
        // info_high = 0x30 in RM_HEAP is HEAP_INPLACE — no new tuple.
        // We must skip these so we don't try to decode block data
        // that isn't shaped like xl_heap_header + tuple.
        let data = pg_class_block_data(2608, 40000);
        let rec = heap_block_record(RmId::Heap, 0x30, 5, 1259, data);
        t.observe(&rec);
        assert!(!t.is_catalog(5, 40000));
        assert_eq!(t.pg_class_writes_decoded, 0);
    }

    #[test]
    fn pg_class_heap_record_after_relmap_uses_new_filenode() {
        let mut t = CatalogTracker::new();
        // Source rotated pg_class to filenode 50000 first.
        let rm = relmap_record(5, &[(1259, 50000)]);
        t.observe(&rm);
        // Then VACUUM FULL on pg_depend; pg_class block now lives at
        // filenode 50000, not 1259.
        let data = pg_class_block_data(2608, 70000);
        let rec = heap_block_record(RmId::Heap, 0x20, 5, 50000, data);
        t.observe(&rec);
        assert!(t.is_catalog(5, 70000));
        assert_eq!(t.pg_class_writes_decoded, 1);
    }

    #[test]
    fn relmap_malformed_main_data_is_ignored() {
        let mut t = CatalogTracker::new();
        let mut r = relmap_record(5, &[(1259, 50000)]);
        r.main_data.truncate(8); // chop off nbytes
        t.observe(&r);
        assert!(!t.is_catalog(5, 50000));
        assert_eq!(t.relmap_updates, 1); // still counted, just no update applied
    }
}
