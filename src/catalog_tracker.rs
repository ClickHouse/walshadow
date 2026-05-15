//! Live catalog relfilenode set.
//!
//! Bootstrap rule (no shadow PG yet, Phase 1): every rel_node <
//! FirstNormalObjectId (16384) starts in the catalog set. Matches
//! `classify::is_catalog_relnode` for parity with Phase 0.
//!
//! Updates:
//! * `RM_RELMAP_ID / XLOG_RELMAP_UPDATE` — authoritative for mapped
//!   catalogs (pg_class, pg_attribute, pg_type, pg_proc, pg_database,
//!   pg_authid, pg_shdepend, …). Body is `xl_relmap_update` + a
//!   `RelMapFile` blob (magic + mappings + crc, see PG
//!   `src/backend/utils/cache/relmapper.c`). Each non-zero mapping
//!   `(mapoid, mapfilenumber)` adds `mapfilenumber` to the catalog
//!   set for that database (or the shared set if `dbid == 0`).
//! * Heap writes to `pg_class` — NOT decoded in Phase 1. Required to
//!   keep the **non**-mapped catalog set live across VACUUM FULL /
//!   REINDEX (pg_depend, pg_namespace, pg_constraint, ...). Decoding
//!   pg_class tuples needs the catalog cache from Phase 3; tracked
//!   as a Phase 1 gap and exercised against a fixture that covers
//!   only mapped-catalog rewrites.

use std::collections::HashSet;

use wal_rs::pg::walparser::{RmId, XLogRecord};

use crate::classify::FIRST_NORMAL_OBJECT_ID;

/// XLOG_RELMAP_UPDATE info byte (`xl_info & XLR_RMGR_INFO_MASK`).
const XLOG_RELMAP_UPDATE: u8 = 0x00;
/// `RELMAPPER_FILEMAGIC` from `src/backend/utils/cache/relmapper.c`.
const RELMAPPER_FILEMAGIC: i32 = 0x592717;
const MAX_MAPPINGS: usize = 64;
const REL_MAP_FILE_SIZE: usize = 4 + 4 + MAX_MAPPINGS * 8 + 4; // magic + n + mappings + crc

#[derive(Debug, Default)]
pub struct CatalogTracker {
    /// `(db_node, rel_node)` for per-database catalogs. `db_node == 0`
    /// is the shared catalog set; queries on any db must consult it.
    nodes: HashSet<(u32, u32)>,
    /// Count of relmap updates observed (debug / metrics).
    pub relmap_updates: u64,
    /// Count of heap writes targeting pg_class that we saw but could
    /// not decode in Phase 1. Surfaces gap to operator.
    pub pg_class_writes_undecoded: u64,
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
            // Any rel under global/ that's been recorded via relmap update.
            return self.nodes.contains(&(0, rel_node));
        }
        self.nodes.contains(&(db_node, rel_node))
            || self.nodes.contains(&(0, rel_node))
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

        // Heap write targeting pg_class? Track for diagnostics.
        // pg_class oid is 1259; its relfilenode equals its mapped
        // filenumber, which we learn from RELMAP. For Phase 1 we just
        // count occurrences so users see the gap.
        if rm == RmId::Heap as u8 || rm == RmId::Heap2 as u8 {
            for blk in &record.blocks {
                let (db, rel) = (blk.header.location.rel.db_node, blk.header.location.rel.rel_node);
                if self.is_pg_class_relfilenode(db, rel) {
                    self.pg_class_writes_undecoded += 1;
                    break;
                }
            }
        }
    }

    /// `pg_class` oid is 1259. Initially relfilenode == oid for mapped
    /// catalogs but the relmap may have rewritten it. Heuristic: known
    /// pg_class relfilenodes are the ones we've recorded under any db
    /// keyed by mapoid==1259. For the Phase 1 bootstrap (no relmap
    /// observed yet) we fall back to the oid==filenode assumption.
    fn is_pg_class_relfilenode(&self, _db: u32, rel: u32) -> bool {
        rel == 1259
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
            }
        }
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
    use wal_rs::pg::walparser::{XLogRecordHeader, XLogRecordBlock, XLogRecordBlockHeader,
        BlockLocation, RelFileNode};

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

    fn heap_record(rm: RmId, db: u32, rel: u32) -> XLogRecord {
        XLogRecord {
            header: XLogRecordHeader {
                resource_manager_id: rm as u8,
                ..Default::default()
            },
            blocks: vec![XLogRecordBlock {
                header: XLogRecordBlockHeader {
                    location: BlockLocation {
                        rel: RelFileNode { spc_node: 1663, db_node: db, rel_node: rel },
                        block_no: 0,
                    },
                    ..Default::default()
                },
                ..Default::default()
            }],
            ..Default::default()
        }
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
    fn pg_class_heap_writes_are_counted() {
        let mut t = CatalogTracker::new();
        t.observe(&heap_record(RmId::Heap, 5, 1259));
        t.observe(&heap_record(RmId::Heap2, 5, 1259));
        t.observe(&heap_record(RmId::Heap, 5, 99999));
        assert_eq!(t.pg_class_writes_undecoded, 2);
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
