//! Per-record routing decision for the WAL rewriter: where each record
//! goes, not whether it survives. Records fed in source order.
//!
//! Route policy:
//! * `Special` rmgr → ToShadow (recovery plumbing shadow needs verbatim)
//! * `Catalog` → ToShadow
//! * `User` → ToDecoder (XLOG_NOOP placeholder on shadow; original bytes
//!   feed the heap decoder)
//! * `Empty` → reclassify via `main_data::relation_for_empty` against
//!   `CatalogTracker`. Unrecognised → ToShadow: correctness over bytes,
//!   wrongly suppressing a catalog record breaks shadow.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};

use walrus::pg::walparser::{RelFileNode, RmId, XLogRecord, XLogRecordBlock};

use crate::decode::wal_xact::{
    XLOG_XACT_ABORT, XLOG_XACT_ABORT_PREPARED, XLOG_XACT_COMMIT, XLOG_XACT_COMMIT_PREPARED,
    XLOG_XACT_INVALIDATIONS, XLOG_XACT_OPMASK, XactPayloadError, parse_xact_invalidations,
    parse_xact_payload,
};
use crate::filter::catalog_tracker::{CatalogTracker, CatalogTrackerStats};
use crate::filter::classify::{Class, classify};
use crate::filter::main_data;
use crate::filter::manifest::ManifestStats;
use crate::record::{AffectedOid, BoundaryInfo, Route, rmgr_label};
use crate::schema::FIRST_NORMAL_OBJECT_ID;

#[derive(Debug, Default, Clone, Copy)]
pub struct FilterStats {
    pub kept: u64,
    pub dropped: u64,
    pub kept_bytes: u64,
    pub dropped_bytes: u64,
    pub kept_catalog: u64,
    pub kept_user: u64,
    pub kept_special: u64,
    pub kept_empty: u64,
}

impl FilterStats {
    /// Field-wise difference; per-segment manifest carves a window out of
    /// a long-lived [`Filter`]'s cumulative `stats`.
    pub fn delta_from(&self, prev: &Self) -> Self {
        Self {
            kept: self.kept - prev.kept,
            dropped: self.dropped - prev.dropped,
            kept_bytes: self.kept_bytes - prev.kept_bytes,
            dropped_bytes: self.dropped_bytes - prev.dropped_bytes,
            kept_catalog: self.kept_catalog - prev.kept_catalog,
            kept_user: self.kept_user - prev.kept_user,
            kept_special: self.kept_special - prev.kept_special,
            kept_empty: self.kept_empty - prev.kept_empty,
        }
    }

    pub fn record(&mut self, class: Class, route: Route, bytes: u64) {
        match route {
            Route::ToShadow => {
                self.kept += 1;
                self.kept_bytes += bytes;
                match class {
                    Class::Catalog => self.kept_catalog += 1,
                    Class::User => self.kept_user += 1,
                    Class::Special => self.kept_special += 1,
                    Class::Empty => self.kept_empty += 1,
                }
            }
            Route::ToDecoder => {
                self.dropped += 1;
                self.dropped_bytes += bytes;
            }
        }
    }
}

impl ManifestStats {
    pub(crate) fn from_filter(stats: FilterStats, catalog: CatalogTrackerStats) -> Self {
        Self {
            records: stats.kept + stats.dropped,
            kept: stats.kept,
            dropped: stats.dropped,
            kept_bytes: stats.kept_bytes,
            dropped_bytes: stats.dropped_bytes,
            catalog_keeps: stats.kept_catalog,
            user_keeps: stats.kept_user,
            special_keeps: stats.kept_special,
            empty_keeps: stats.kept_empty,
            relmap_updates: catalog.relmap_updates,
            pg_class_writes_undecoded: catalog.pg_class_writes_undecoded,
            pg_class_writes_oid_in_prefix: catalog.pg_class_writes_oid_in_prefix,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct FilterSnapshot {
    stats: FilterStats,
    catalog: CatalogTrackerStats,
}

/// Full routing verdict for one record; see [`Filter::decide_record`].
#[derive(Debug, Clone)]
pub struct Verdict {
    pub route: Route,
    /// Commit record of a catalog-mutating xact (top, subxact, or prepared
    /// xid wrote a catalog-touching record). Pump holds shadow publication
    /// here until replay passes the commit's `next_lsn`
    pub catalog_boundary: bool,
    /// Capture input; `Some` iff `catalog_boundary`
    pub boundary: Option<Arc<BoundaryInfo>>,
}

/// One catalog-dirty xact's accumulated capture inputs, keyed by the
/// writing xid (top or sub); merged across the tree at its commit.
#[derive(Debug)]
struct DirtyXact {
    /// First catalog-touching record LSN under this xid
    first_touch: u64,
    /// User oid → first pg_class touch LSN under this xid
    oids: HashMap<u32, u64>,
    /// Wrote a capture-all catalog (pg_namespace)
    unenumerated: bool,
}

/// Pump-side `XLOG_SMGR_CREATE` main-fork markers: physical rfn → creation
/// LSN, the sharpest bias-early valid_from for a rotated filenode. Keyed by
/// full rfn — relfilenumbers are unique only per (tablespace, database), and
/// capture resolves descriptor tablespace to physical so both sides carry
/// concrete spcOid. FIFO-capped like the worker-side map (which stays
/// separate for stash admission). Shared with descriptor capture, same
/// task — uncontended.
#[derive(Debug, Default)]
pub struct SmgrMarkers {
    map: HashMap<RelFileNode, u64>,
    order: VecDeque<(RelFileNode, u64)>,
}

/// Mirror of the worker-side marker backstop
const SMGR_MARKER_CAP: usize = 65536;

impl SmgrMarkers {
    fn insert(&mut self, rfn: RelFileNode, lsn: u64) {
        if self.map.insert(rfn, lsn) != Some(lsn) {
            self.order.push_back((rfn, lsn));
            while self.order.len() > SMGR_MARKER_CAP {
                if let Some((old, old_lsn)) = self.order.pop_front()
                    && self.map.get(&old) == Some(&old_lsn)
                {
                    self.map.remove(&old);
                }
            }
        }
    }

    pub fn get(&self, rfn: RelFileNode) -> Option<u64> {
        self.map.get(&rfn).copied()
    }
}

/// Routes against the *post-update* catalog set so an XLOG_RELMAP_UPDATE
/// introducing a new mapped-catalog filenumber immediately routes later
/// records on that filenumber to shadow.
pub struct Filter {
    tracker: CatalogTracker,
    stats: FilterStats,
    /// Xids (top or sub) that wrote a catalog-touching record or logged a
    /// descriptor-relevant `XLOG_XACT_INVALIDATIONS` set; drained at their
    /// commit / abort. Crash-orphaned xids linger, bounded by workload (no
    /// commit ever arrives to hold on)
    catalog_dirty: HashMap<u32, DirtyXact>,
    smgr_markers: Arc<Mutex<SmgrMarkers>>,
    /// Relcache-inval scope: accept db in {0, this}; `None` (unwired)
    /// accepts any db
    inval_db_oid: Option<u32>,
}

impl Filter {
    pub fn new() -> Self {
        Self {
            tracker: CatalogTracker::new(),
            stats: FilterStats::default(),
            catalog_dirty: HashMap::new(),
            smgr_markers: Arc::new(Mutex::new(SmgrMarkers::default())),
            inval_db_oid: None,
        }
    }

    /// Scope relcache-inval extraction to the followed database
    pub fn set_inval_db(&mut self, db_oid: u32) {
        self.inval_db_oid = Some(db_oid);
    }

    /// Capture reads rotation markers through this handle
    pub fn smgr_markers(&self) -> Arc<Mutex<SmgrMarkers>> {
        self.smgr_markers.clone()
    }

    pub fn decide(&mut self, record: &XLogRecord) -> Route {
        // Offline callers (segment filter tool) have no LSN and no capture;
        // a malformed commit payload only degrades boundary metadata there,
        // and commit records route ToShadow either way
        self.decide_record(record, 0, 0xD116)
            .map(|v| v.route)
            .unwrap_or(Route::ToShadow)
    }

    pub fn tracker(&self) -> &CatalogTracker {
        &self.tracker
    }

    pub fn tracker_mut(&mut self) -> &mut CatalogTracker {
        &mut self.tracker
    }

    pub fn stats(&self) -> &FilterStats {
        &self.stats
    }

    pub(crate) fn snapshot(&self) -> FilterSnapshot {
        FilterSnapshot {
            stats: self.stats,
            catalog: self.tracker.stats(),
        }
    }

    pub(crate) fn manifest_stats_since(&self, previous: FilterSnapshot) -> ManifestStats {
        ManifestStats::from_filter(
            self.stats.delta_from(&previous.stats),
            self.tracker.stats().delta_from(previous.catalog),
        )
    }

    /// Route plus the tracker's [`CatalogSignal`] verdict, stamped on the
    /// outgoing [`Record`](crate::record::Record) so the decoder worker
    /// bumps invalidation epochs at its own stream position (a
    /// pump-position bump would be consumable before pre-DDL records finish
    /// decoding; see `catalog_tracker` module doc), plus the
    /// catalog-boundary verdict driving the pump's publication hold and
    /// descriptor capture. `Err` = malformed commit payload: capture input
    /// would be silently incomplete, poison the stream.
    pub fn decide_record(
        &mut self,
        record: &XLogRecord,
        source_lsn: u64,
        page_magic: u16,
    ) -> Result<Verdict, XactPayloadError> {
        let obs = self.tracker.observe(record);
        let class = classify(record);
        // `catalog_touch` marks the record's xid dirty: only paths proving a
        // catalog relation was written qualify. `Empty`'s None → ToShadow
        // safe default must not dirty (would hold at unrelated commits)
        let (route, catalog_touch) = match class {
            Class::Catalog => (Route::ToShadow, true),
            // Relmap update (VACUUM FULL mapped catalog) is Special-class
            Class::Special => (Route::ToShadow, obs.catalog_write),
            Class::User => {
                if any_block_is_catalog(&self.tracker, &record.blocks) {
                    // tracker has filenodes the bootstrap classify rule misses
                    (Route::ToShadow, true)
                } else {
                    (Route::ToDecoder, false)
                }
            }
            Class::Empty => match main_data::relation_for_empty(record) {
                Some(rel) => {
                    if self.tracker.is_catalog(rel.db_node, rel.rel_node) {
                        (Route::ToShadow, true)
                    } else {
                        (Route::ToDecoder, false)
                    }
                }
                None => (Route::ToShadow, false), // safe default
            },
        };
        if record.header.resource_manager_id == RmId::Smgr as u8
            && record.header.info & 0xF0 == main_data::XLOG_SMGR_CREATE
            && let Some((rfn, fork)) = main_data::parse_xl_smgr_create(&record.main_data)
            && fork == main_data::MAIN_FORKNUM
        {
            self.smgr_markers
                .lock()
                .expect("smgr markers poisoned")
                .insert(rfn, source_lsn);
        }
        let xid = record.header.xact_id;
        if catalog_touch && xid != 0 {
            let dirty = self.catalog_dirty.entry(xid).or_insert_with(|| DirtyXact {
                first_touch: source_lsn,
                oids: HashMap::new(),
                unenumerated: false,
            });
            if let Some(oid) = obs.pg_class_user_oid {
                dirty.oids.entry(oid).or_insert(source_lsn);
            }
            if record.blocks.iter().any(|b| {
                let r = b.header.location.rel;
                self.tracker.is_capture_all_catalog(r.db_node, r.rel_node)
            }) {
                dirty.unenumerated = true;
            }
        }
        let boundary = self.observe_xact_end(record, source_lsn, page_magic)?;
        self.stats
            .record(class, route, record.header.total_record_length as u64);
        Ok(Verdict {
            route,
            catalog_boundary: boundary.is_some(),
            boundary,
        })
    }

    /// Drain dirty xids at commit / abort. Commit of any dirty xid (top,
    /// listed subxact, or prepared xid) is a catalog boundary; abort clears
    /// without holding — rolled-back catalog changes never become visible
    /// in shadow. Commit records carry the full committed-subxact list
    /// (`xactGetCommittedChildren`), so no ASSIGNMENT tracking is needed.
    /// Defense: a commit carrying local relcache invals is a boundary even
    /// when the dirty tracker missed every write.
    fn observe_xact_end(
        &mut self,
        record: &XLogRecord,
        source_lsn: u64,
        page_magic: u16,
    ) -> Result<Option<Arc<BoundaryInfo>>, XactPayloadError> {
        if record.header.resource_manager_id != RmId::Xact as u8 {
            return Ok(None);
        }
        let info = record.header.info;
        let op = info & XLOG_XACT_OPMASK;
        if op == XLOG_XACT_INVALIDATIONS {
            self.observe_xact_invals(record, source_lsn, page_magic)?;
            return Ok(None);
        }
        let is_commit = op == XLOG_XACT_COMMIT || op == XLOG_XACT_COMMIT_PREPARED;
        let is_abort = op == XLOG_XACT_ABORT || op == XLOG_XACT_ABORT_PREPARED;
        if !is_commit && !is_abort {
            return Ok(None);
        }
        let payload = parse_xact_payload(info, &record.main_data, page_magic)?;
        let header_xid = record.header.xact_id;
        let mut merged: Option<DirtyXact> = None;
        let mut absorb = |dirty: Option<DirtyXact>| {
            let Some(dirty) = dirty else { return };
            match &mut merged {
                None => merged = Some(dirty),
                Some(m) => {
                    m.first_touch = m.first_touch.min(dirty.first_touch);
                    m.unenumerated |= dirty.unenumerated;
                    for (oid, lsn) in dirty.oids {
                        m.oids
                            .entry(oid)
                            .and_modify(|l| *l = (*l).min(lsn))
                            .or_insert(lsn);
                    }
                }
            }
        };
        absorb(self.catalog_dirty.remove(&header_xid));
        if let Some(x) = payload.twophase_xid {
            absorb(self.catalog_dirty.remove(&x));
        }
        for x in &payload.subxacts {
            absorb(self.catalog_dirty.remove(x));
        }
        if !is_commit {
            return Ok(None);
        }
        // Local relcache invals: second oid source + capture-all trigger.
        // db 0 = shared relation; user rels there are impossible, kept for
        // symmetry with is_local_db
        let mut capture_all = false;
        let mut inval_oids: Vec<u32> = Vec::new();
        for inval in &payload.invals.relcache {
            if !self.is_local_db(inval.db_id) {
                continue;
            }
            if inval.rel_id == 0 {
                capture_all = true;
            } else if inval.rel_id >= FIRST_NORMAL_OBJECT_ID {
                inval_oids.push(inval.rel_id);
            }
        }
        // Namespace catcache / whole-catalog inval: restart-safe capture-all
        // trigger. Commit records carry the xact tree's full inval set, so
        // classification holds even when the resume floor passed the
        // pg_namespace writes and the dirty tracker never saw them
        if payload.invals.namespace.hits(|db| self.is_local_db(db)) {
            capture_all = true;
        }
        let dirty_hit = merged.is_some();
        if !dirty_hit && inval_oids.is_empty() && !capture_all {
            return Ok(None);
        }
        let mut merged = merged.unwrap_or_else(|| DirtyXact {
            // Inval-only boundary (dirty tracker missed the writes): the
            // commit record itself is the only LSN at hand. Later than any
            // of the xact's rows, so its events order after them — safe for
            // descriptor bias (newer reader reads older tuples)
            first_touch: source_lsn,
            oids: HashMap::new(),
            unenumerated: false,
        });
        for oid in inval_oids {
            merged.oids.entry(oid).or_default();
        }
        let mut oids: Vec<AffectedOid> = merged
            .oids
            .into_iter()
            .map(|(oid, lsn)| AffectedOid {
                oid,
                pg_class_touch: (lsn != 0).then_some(lsn),
            })
            .collect();
        oids.sort_unstable_by_key(|a| a.oid);
        Ok(Some(Arc::new(BoundaryInfo {
            drain_xid: payload.twophase_xid.unwrap_or(header_xid),
            tree_first_touch: merged.first_touch,
            oids,
            capture_all: capture_all || merged.unenumerated,
        })))
    }

    /// `XLOG_XACT_INVALIDATIONS`: command-boundary inval set logged
    /// mid-xact at `wal_level=logical`. Re-dirties the writing xid so
    /// boundary classification survives a restart whose resume floor sits
    /// past the xact's catalog records. Only descriptor-relevant messages
    /// dirty — an entry with nothing to capture would hold publication at
    /// commit for nothing
    fn observe_xact_invals(
        &mut self,
        record: &XLogRecord,
        source_lsn: u64,
        page_magic: u16,
    ) -> Result<(), XactPayloadError> {
        let xid = record.header.xact_id;
        if xid == 0 {
            return Ok(());
        }
        let invals = parse_xact_invalidations(&record.main_data, page_magic)?;
        let namespace_hit = invals.namespace.hits(|db| self.is_local_db(db));
        let mut flush = false;
        let mut oids: Vec<u32> = Vec::new();
        for inval in &invals.relcache {
            if !self.is_local_db(inval.db_id) {
                continue;
            }
            if inval.rel_id == 0 {
                flush = true;
            } else if inval.rel_id >= FIRST_NORMAL_OBJECT_ID {
                oids.push(inval.rel_id);
            }
        }
        if !namespace_hit && !flush && oids.is_empty() {
            return Ok(());
        }
        let dirty = self.catalog_dirty.entry(xid).or_insert_with(|| DirtyXact {
            first_touch: source_lsn,
            oids: HashMap::new(),
            unenumerated: false,
        });
        dirty.unenumerated |= namespace_hit || flush;
        for oid in oids {
            // Inval record LSN sits at command end: after the command's
            // catalog writes, before commit — a live pg_class decode's
            // earlier touch wins via or_insert
            dirty.oids.entry(oid).or_insert(source_lsn);
        }
        Ok(())
    }

    /// Inval scope: accept db in {0, followed}; `None` (unwired) accepts any
    fn is_local_db(&self, db: u32) -> bool {
        !self
            .inval_db_oid
            .is_some_and(|local| db != 0 && db != local)
    }

    pub fn rmgr_label(record: &XLogRecord) -> String {
        rmgr_label(record.header.resource_manager_id)
    }
}

impl Default for Filter {
    fn default() -> Self {
        Self::new()
    }
}

fn any_block_is_catalog(tracker: &CatalogTracker, blocks: &[XLogRecordBlock]) -> bool {
    blocks.iter().any(|b| {
        let r = b.header.location.rel;
        tracker.is_catalog(r.db_node, r.rel_node)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use walrus::pg::walparser::{
        BlockLocation, RelFileNode, RmId, XLogRecordBlockHeader, XLogRecordHeader,
    };

    fn rec(rm: RmId, rels: &[(u32, u32)]) -> XLogRecord<'static> {
        XLogRecord {
            header: XLogRecordHeader {
                resource_manager_id: rm as u8,
                total_record_length: 64,
                ..Default::default()
            },
            blocks: rels
                .iter()
                .map(|&(db, rel)| XLogRecordBlock {
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
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        }
    }

    #[test]
    fn catalog_record_is_kept() {
        let mut f = Filter::new();
        let r = rec(RmId::Heap, &[(5, 1259)]);
        assert_eq!(f.decide(&r), Route::ToShadow);
    }

    #[test]
    fn user_record_is_dropped() {
        let mut f = Filter::new();
        let r = rec(RmId::Heap, &[(5, 20000)]);
        assert_eq!(f.decide(&r), Route::ToDecoder);
    }

    #[test]
    fn special_rmgr_is_kept() {
        let mut f = Filter::new();
        let r = rec(RmId::Xact, &[]);
        assert_eq!(f.decide(&r), Route::ToShadow);
    }

    #[test]
    fn empty_unknown_is_kept_safe_default() {
        let mut f = Filter::new();
        let r = rec(RmId::Heap, &[]);
        assert_eq!(f.decide(&r), Route::ToShadow);
    }

    #[test]
    fn tracker_promotes_user_to_catalog_post_relmap() {
        let mut f = Filter::new();
        // Learned mapping: catalog on db 5 rewritten to filenode 50000.
        f.tracker.add(5, 50000);
        let r = rec(RmId::Heap, &[(5, 50000)]);
        assert_eq!(f.decide(&r), Route::ToShadow);
    }

    #[test]
    fn empty_class_with_known_relation_is_classified_against_tracker() {
        use crate::filter::main_data::XLOG_HEAP2_NEW_CID;
        // XLOG_HEAP2_NEW_CID carries a locator in main_data (Class::Empty).
        // Catalog filenode (oid < 16384) → Keep; user filenode → Drop.
        fn new_cid_main_data(db: u32, rel: u32) -> Vec<u8> {
            let mut md = Vec::with_capacity(34);
            md.extend_from_slice(&100u32.to_le_bytes()); // top_xid
            md.extend_from_slice(&1u32.to_le_bytes()); // cmin
            md.extend_from_slice(&2u32.to_le_bytes()); // cmax
            md.extend_from_slice(&0u32.to_le_bytes()); // combocid
            md.extend_from_slice(&1663u32.to_le_bytes()); // spc
            md.extend_from_slice(&db.to_le_bytes());
            md.extend_from_slice(&rel.to_le_bytes());
            md.extend_from_slice(&[0u8; 6]); // target_tid
            md
        }
        fn new_cid_record(db: u32, rel: u32) -> XLogRecord<'static> {
            XLogRecord {
                header: walrus::pg::walparser::XLogRecordHeader {
                    resource_manager_id: RmId::Heap2 as u8,
                    info: XLOG_HEAP2_NEW_CID,
                    total_record_length: 64,
                    ..Default::default()
                },
                main_data: std::borrow::Cow::Owned(new_cid_main_data(db, rel)),
                ..Default::default()
            }
        }
        let mut f = Filter::new();
        // catalog filenode (1259 = pg_class) → Keep
        assert_eq!(f.decide(&new_cid_record(5, 1259)), Route::ToShadow);
        // user filenode → Drop
        assert_eq!(f.decide(&new_cid_record(5, 20000)), Route::ToDecoder);
    }

    #[test]
    fn default_matches_new_and_rmgr_label_round_trips() {
        let _: Filter = Filter::default();
        let r = rec(RmId::Heap, &[]);
        let label = Filter::rmgr_label(&r);
        assert!(!label.is_empty());
    }

    #[test]
    fn stats_track_kept_dropped() {
        let mut f = Filter::new();
        f.decide(&rec(RmId::Heap, &[(5, 1259)]));
        f.decide(&rec(RmId::Heap, &[(5, 20000)]));
        f.decide(&rec(RmId::Heap, &[(5, 20001)]));
        assert_eq!(f.stats.kept, 1);
        assert_eq!(f.stats.dropped, 2);
    }

    fn rec_with_xid(rm: RmId, rels: &[(u32, u32)], xid: u32) -> XLogRecord<'static> {
        let mut r = rec(rm, rels);
        r.header.xact_id = xid;
        r
    }

    /// `xl_xact_commit` / `xl_xact_abort` main_data: xact_time, then
    /// optional xinfo + subxact / inval / twophase sections. Invals encode
    /// as 16-byte `SharedInvalidationMessage`s: `(id, dbId, relId)`.
    fn xact_end_full(
        op: u8,
        xid: u32,
        subxacts: &[u32],
        invals: &[(i8, u32, u32)],
        twophase: Option<u32>,
    ) -> XLogRecord<'static> {
        let mut info = op;
        let mut md: Vec<u8> = 0i64.to_le_bytes().to_vec();
        if !subxacts.is_empty() || twophase.is_some() || !invals.is_empty() {
            info |= XLOG_XACT_HAS_INFO;
            let mut xinfo = 0u32;
            if !subxacts.is_empty() {
                xinfo |= 1 << 1; // XACT_XINFO_HAS_SUBXACTS
            }
            if !invals.is_empty() {
                xinfo |= 1 << 3; // XACT_XINFO_HAS_INVALS
            }
            if twophase.is_some() {
                xinfo |= 1 << 4; // XACT_XINFO_HAS_TWOPHASE
            }
            md.extend_from_slice(&xinfo.to_le_bytes());
            if !subxacts.is_empty() {
                md.extend_from_slice(&(subxacts.len() as i32).to_le_bytes());
                for x in subxacts {
                    md.extend_from_slice(&x.to_le_bytes());
                }
            }
            if !invals.is_empty() {
                md.extend_from_slice(&(invals.len() as i32).to_le_bytes());
                for &(id, db, rel) in invals {
                    let mut msg = [0u8; 16];
                    msg[0] = id as u8;
                    msg[4..8].copy_from_slice(&db.to_le_bytes());
                    msg[8..12].copy_from_slice(&rel.to_le_bytes());
                    md.extend_from_slice(&msg);
                }
            }
            if let Some(x) = twophase {
                md.extend_from_slice(&x.to_le_bytes());
            }
        }
        let mut r = rec_with_xid(RmId::Xact, &[], xid);
        r.header.info = info;
        r.main_data = std::borrow::Cow::Owned(md);
        r
    }

    fn xact_end(op: u8, xid: u32, subxacts: &[u32], twophase: Option<u32>) -> XLogRecord<'static> {
        xact_end_full(op, xid, subxacts, &[], twophase)
    }

    use crate::decode::wal_xact::XLOG_XACT_HAS_INFO;

    #[test]
    fn catalog_commit_is_boundary_dml_commit_is_not() {
        let mut f = Filter::new();
        f.decide_record(&rec_with_xid(RmId::Heap, &[(5, 1259)], 7), 0, 0xD116)
            .unwrap();
        f.decide_record(&rec_with_xid(RmId::Heap, &[(5, 20000)], 8), 0, 0xD116)
            .unwrap();
        // DML-only xid 8 commit: never parks
        let v = f
            .decide_record(&xact_end(XLOG_XACT_COMMIT, 8, &[], None), 0, 0xD116)
            .unwrap();
        assert!(!v.catalog_boundary);
        // Catalog-dirty xid 7 commit: boundary, drained after
        let v = f
            .decide_record(&xact_end(XLOG_XACT_COMMIT, 7, &[], None), 0, 0xD116)
            .unwrap();
        assert!(v.catalog_boundary);
        let v = f
            .decide_record(&xact_end(XLOG_XACT_COMMIT, 7, &[], None), 0, 0xD116)
            .unwrap();
        assert!(!v.catalog_boundary, "dirty mark consumed once");
    }

    #[test]
    fn abort_clears_dirty_without_boundary() {
        let mut f = Filter::new();
        f.decide_record(&rec_with_xid(RmId::Heap, &[(5, 1259)], 7), 0, 0xD116)
            .unwrap();
        let v = f
            .decide_record(&xact_end(XLOG_XACT_ABORT, 7, &[], None), 0, 0xD116)
            .unwrap();
        assert!(!v.catalog_boundary, "rolled-back DDL never holds");
        let v = f
            .decide_record(&xact_end(XLOG_XACT_COMMIT, 7, &[], None), 0, 0xD116)
            .unwrap();
        assert!(!v.catalog_boundary, "abort drained the mark");
    }

    #[test]
    fn subxact_catalog_write_marks_top_commit() {
        let mut f = Filter::new();
        // DDL under savepoint: catalog record carries subxid 101
        f.decide_record(&rec_with_xid(RmId::Heap, &[(5, 1259)], 101), 0, 0xD116)
            .unwrap();
        let v = f
            .decide_record(
                &xact_end(XLOG_XACT_COMMIT, 100, &[101, 102], None),
                0,
                0xD116,
            )
            .unwrap();
        assert!(v.catalog_boundary);
    }

    #[test]
    fn commit_prepared_matches_prepared_xid() {
        let mut f = Filter::new();
        f.decide_record(&rec_with_xid(RmId::Heap, &[(5, 1259)], 300), 0, 0xD116)
            .unwrap();
        // COMMIT PREPARED: header xid differs, prepared xid in payload
        let v = f
            .decide_record(
                &xact_end(XLOG_XACT_COMMIT_PREPARED, 0, &[], Some(300)),
                0,
                0xD116,
            )
            .unwrap();
        assert!(v.catalog_boundary);
    }

    #[test]
    fn relmap_update_marks_writing_xid() {
        use crate::filter::catalog_tracker::test_relmap_record as relmap;
        let mut f = Filter::new();
        let mut r = relmap(5, &[(1259, 50000)]);
        r.header.xact_id = 9;
        f.decide_record(&r, 0, 0xD116).unwrap();
        let v = f
            .decide_record(&xact_end(XLOG_XACT_COMMIT, 9, &[], None), 0, 0xD116)
            .unwrap();
        assert!(
            v.catalog_boundary,
            "VACUUM FULL relmap write holds at commit"
        );
    }

    #[test]
    fn empty_safe_default_route_does_not_dirty() {
        let mut f = Filter::new();
        // Class::Empty, unrecognised main_data → ToShadow safe default
        let r = rec_with_xid(RmId::Heap, &[], 7);
        assert_eq!(
            f.decide_record(&r, 0, 0xD116).unwrap().route,
            Route::ToShadow
        );
        let v = f
            .decide_record(&xact_end(XLOG_XACT_COMMIT, 7, &[], None), 0, 0xD116)
            .unwrap();
        assert!(
            !v.catalog_boundary,
            "safe-default keep is not a catalog touch"
        );
    }

    #[test]
    fn tracker_promoted_user_record_dirties() {
        let mut f = Filter::new();
        f.tracker.add(5, 50000); // rotated mapped catalog above 16384
        f.decide_record(&rec_with_xid(RmId::Heap, &[(5, 50000)], 7), 0, 0xD116)
            .unwrap();
        let v = f
            .decide_record(&xact_end(XLOG_XACT_COMMIT, 7, &[], None), 0, 0xD116)
            .unwrap();
        assert!(v.catalog_boundary);
    }

    #[test]
    fn boundary_merges_inval_oids_and_first_touch() {
        let mut f = Filter::new();
        f.decide_record(&rec_with_xid(RmId::Heap, &[(5, 1259)], 7), 100, 0xD116)
            .unwrap();
        // Commit carries relcache invals: local user rel + skippable ids
        let commit = xact_end_full(
            XLOG_XACT_COMMIT,
            7,
            &[],
            &[
                (7, 5, 0),      // catcache: skip
                (-2, 5, 16400), // relcache, local user rel
                (-2, 5, 1259),  // relcache on a catalog oid: filtered
                (-3, 5, 16400), // smgr: skip
                (-6, 5, 16400), // relsync (PG 18): skip
            ],
            None,
        );
        let v = f.decide_record(&commit, 200, 0xD116).unwrap();
        let b = v.boundary.expect("boundary");
        assert_eq!(b.drain_xid, 7);
        assert_eq!(b.tree_first_touch, 100);
        assert!(!b.capture_all);
        assert_eq!(b.oids.len(), 1);
        assert_eq!(b.oids[0].oid, 16400);
        assert_eq!(b.oids[0].pg_class_touch, None, "inval-sourced oid");
    }

    #[test]
    fn inval_only_commit_is_boundary_defense() {
        let mut f = Filter::new();
        // Dirty tracker saw nothing, but the commit proves catalog effects
        let commit = xact_end_full(XLOG_XACT_COMMIT, 9, &[], &[(-2, 5, 16500)], None);
        let v = f.decide_record(&commit, 300, 0xD116).unwrap();
        let b = v.boundary.expect("inval-only boundary");
        assert_eq!(b.tree_first_touch, 300, "commit lsn fallback");
        assert_eq!(b.oids[0].oid, 16500);
    }

    #[test]
    fn whole_relcache_inval_forces_capture_all() {
        let mut f = Filter::new();
        let commit = xact_end_full(XLOG_XACT_COMMIT, 9, &[], &[(-2, 5, 0)], None);
        let v = f.decide_record(&commit, 300, 0xD116).unwrap();
        assert!(v.boundary.expect("boundary").capture_all);
    }

    #[test]
    fn pg_namespace_write_forces_capture_all() {
        let mut f = Filter::new();
        // Namespace rename: pg_namespace heap write, zero relcache oids
        f.decide_record(&rec_with_xid(RmId::Heap, &[(5, 2615)], 7), 100, 0xD116)
            .unwrap();
        let v = f
            .decide_record(&xact_end(XLOG_XACT_COMMIT, 7, &[], None), 200, 0xD116)
            .unwrap();
        let b = v.boundary.expect("boundary");
        assert!(b.capture_all);
        assert!(b.oids.is_empty());
    }

    /// `xl_xact_invals`: `(i32 nmsgs, nmsgs × 16-byte msg)`, same message
    /// encoding as commit invals
    fn xact_invals_rec(xid: u32, invals: &[(i8, u32, u32)]) -> XLogRecord<'static> {
        let mut md = (invals.len() as i32).to_le_bytes().to_vec();
        for &(id, db, arg) in invals {
            let mut msg = [0u8; 16];
            msg[0] = id as u8;
            msg[4..8].copy_from_slice(&db.to_le_bytes());
            msg[8..12].copy_from_slice(&arg.to_le_bytes());
            md.extend_from_slice(&msg);
        }
        let mut r = rec_with_xid(RmId::Xact, &[], xid);
        r.header.info = XLOG_XACT_INVALIDATIONS;
        r.main_data = std::borrow::Cow::Owned(md);
        r
    }

    #[test]
    fn namespace_catcache_commit_is_capture_all_boundary() {
        let mut f = Filter::new();
        // ALTER SCHEMA RENAME whose pg_namespace writes precede the resume
        // floor: commit carries only catcache invals (NAMESPACENAME = 35 on
        // PG 16-17)
        let commit = xact_end_full(XLOG_XACT_COMMIT, 9, &[], &[(35, 5, 0xBEEF)], None);
        let v = f.decide_record(&commit, 300, 0xD116).unwrap();
        let b = v.boundary.expect("namespace catcache boundary");
        assert!(b.capture_all);
        assert!(b.oids.is_empty());
        assert_eq!(b.tree_first_touch, 300, "commit lsn fallback");
    }

    #[test]
    fn namespace_catcache_ids_keyed_per_major() {
        let mut f = Filter::new();
        // 35 is a different syscache on PG 18 (namespace ids shift to 37/38)
        let commit = xact_end_full(XLOG_XACT_COMMIT, 9, &[], &[(35, 5, 0)], None);
        assert!(
            f.decide_record(&commit, 300, 0xD118)
                .unwrap()
                .boundary
                .is_none()
        );
        let commit = xact_end_full(XLOG_XACT_COMMIT, 10, &[], &[(37, 5, 0)], None);
        let v = f.decide_record(&commit, 300, 0xD118).unwrap();
        assert!(v.boundary.expect("PG 18 namespace id").capture_all);
    }

    #[test]
    fn irrelevant_catcache_commit_is_not_boundary() {
        let mut f = Filter::new();
        // STATRELATTINH (63): ANALYZE-rate churn must not bound
        let commit = xact_end_full(XLOG_XACT_COMMIT, 9, &[], &[(63, 5, 0xBEEF)], None);
        assert!(
            f.decide_record(&commit, 300, 0xD116)
                .unwrap()
                .boundary
                .is_none()
        );
    }

    #[test]
    fn namespace_catcache_foreign_db_filtered() {
        let mut f = Filter::new();
        f.set_inval_db(5);
        let commit = xact_end_full(XLOG_XACT_COMMIT, 9, &[], &[(35, 6, 0)], None);
        assert!(
            f.decide_record(&commit, 300, 0xD116)
                .unwrap()
                .boundary
                .is_none()
        );
    }

    #[test]
    fn catalog_inval_on_pg_namespace_forces_capture_all() {
        let mut f = Filter::new();
        // VACUUM FULL pg_namespace: whole-catalog msg names catId directly
        let commit = xact_end_full(XLOG_XACT_COMMIT, 9, &[], &[(-1, 5, 2615)], None);
        let v = f.decide_record(&commit, 300, 0xD116).unwrap();
        assert!(v.boundary.expect("catalog inval boundary").capture_all);
        let commit = xact_end_full(XLOG_XACT_COMMIT, 10, &[], &[(-1, 5, 1259)], None);
        assert!(
            f.decide_record(&commit, 300, 0xD116)
                .unwrap()
                .boundary
                .is_none()
        );
    }

    #[test]
    fn midxact_invals_dirty_xid() {
        let mut f = Filter::new();
        // Restart lost the pg_class write; command-end inval set re-dirties
        f.decide_record(&xact_invals_rec(7, &[(-2, 5, 16400)]), 150, 0xD116)
            .unwrap();
        let v = f
            .decide_record(&xact_end(XLOG_XACT_COMMIT, 7, &[], None), 200, 0xD116)
            .unwrap();
        let b = v.boundary.expect("boundary");
        assert_eq!(b.tree_first_touch, 150);
        assert!(!b.capture_all);
        assert_eq!(b.oids.len(), 1);
        assert_eq!(b.oids[0].oid, 16400);
        assert_eq!(b.oids[0].pg_class_touch, Some(150));
    }

    #[test]
    fn midxact_namespace_inval_forces_capture_all() {
        let mut f = Filter::new();
        f.decide_record(&xact_invals_rec(7, &[(35, 5, 0xAB)]), 150, 0xD116)
            .unwrap();
        let v = f
            .decide_record(&xact_end(XLOG_XACT_COMMIT, 7, &[], None), 200, 0xD116)
            .unwrap();
        let b = v.boundary.expect("boundary");
        assert!(b.capture_all);
        assert_eq!(b.tree_first_touch, 150);
    }

    #[test]
    fn midxact_whole_relcache_flush_forces_capture_all() {
        let mut f = Filter::new();
        f.decide_record(&xact_invals_rec(7, &[(-2, 5, 0)]), 150, 0xD116)
            .unwrap();
        let v = f
            .decide_record(&xact_end(XLOG_XACT_COMMIT, 7, &[], None), 200, 0xD116)
            .unwrap();
        assert!(v.boundary.expect("boundary").capture_all);
    }

    #[test]
    fn midxact_irrelevant_invals_do_not_dirty() {
        let mut f = Filter::new();
        // Non-namespace catcache + relcache on a catalog oid: nothing to
        // capture, an entry would hold publication at commit for nothing
        f.decide_record(
            &xact_invals_rec(7, &[(63, 5, 0xAB), (-2, 5, 1259)]),
            150,
            0xD116,
        )
        .unwrap();
        let v = f
            .decide_record(&xact_end(XLOG_XACT_COMMIT, 7, &[], None), 200, 0xD116)
            .unwrap();
        assert!(v.boundary.is_none());
    }

    #[test]
    fn midxact_inval_foreign_db_filtered() {
        let mut f = Filter::new();
        f.set_inval_db(5);
        f.decide_record(
            &xact_invals_rec(7, &[(-2, 6, 16400), (35, 6, 0)]),
            150,
            0xD116,
        )
        .unwrap();
        let v = f
            .decide_record(&xact_end(XLOG_XACT_COMMIT, 7, &[], None), 200, 0xD116)
            .unwrap();
        assert!(v.boundary.is_none());
    }

    #[test]
    fn midxact_inval_abort_clears() {
        let mut f = Filter::new();
        f.decide_record(&xact_invals_rec(7, &[(-2, 5, 16400)]), 150, 0xD116)
            .unwrap();
        let v = f
            .decide_record(&xact_end(XLOG_XACT_ABORT, 7, &[], None), 200, 0xD116)
            .unwrap();
        assert!(!v.catalog_boundary);
        let v = f
            .decide_record(&xact_end(XLOG_XACT_COMMIT, 7, &[], None), 300, 0xD116)
            .unwrap();
        assert!(!v.catalog_boundary, "abort drained the mark");
    }

    #[test]
    fn midxact_inval_under_subxact_merges_at_top_commit() {
        let mut f = Filter::new();
        f.decide_record(&xact_invals_rec(101, &[(-2, 5, 16400)]), 150, 0xD116)
            .unwrap();
        let v = f
            .decide_record(&xact_end(XLOG_XACT_COMMIT, 100, &[101], None), 200, 0xD116)
            .unwrap();
        assert_eq!(v.boundary.expect("boundary").oids[0].oid, 16400);
    }

    #[test]
    fn malformed_midxact_inval_record_poisons() {
        let mut f = Filter::new();
        let mut r = xact_invals_rec(7, &[(-2, 5, 16400)]);
        // Claim two messages, carry one
        match &mut r.main_data {
            std::borrow::Cow::Owned(md) => md[0..4].copy_from_slice(&2i32.to_le_bytes()),
            _ => unreachable!(),
        }
        assert!(f.decide_record(&r, 150, 0xD116).is_err());
    }

    #[test]
    fn inval_db_scope_filters_foreign_db() {
        let mut f = Filter::new();
        f.set_inval_db(5);
        let commit = xact_end_full(XLOG_XACT_COMMIT, 9, &[], &[(-2, 6, 16500)], None);
        let v = f.decide_record(&commit, 300, 0xD116).unwrap();
        assert!(v.boundary.is_none(), "foreign-db inval must not bound");
    }

    #[test]
    fn prepared_commit_boundary_drains_under_prepared_xid() {
        let mut f = Filter::new();
        f.decide_record(&rec_with_xid(RmId::Heap, &[(5, 1259)], 300), 100, 0xD116)
            .unwrap();
        let v = f
            .decide_record(
                &xact_end(XLOG_XACT_COMMIT_PREPARED, 0, &[], Some(300)),
                400,
                0xD116,
            )
            .unwrap();
        assert_eq!(v.boundary.expect("boundary").drain_xid, 300);
    }

    #[test]
    fn unknown_sinval_id_poisons() {
        let mut f = Filter::new();
        let commit = xact_end_full(XLOG_XACT_COMMIT, 9, &[], &[(-7, 5, 16500)], None);
        assert!(f.decide_record(&commit, 300, 0xD116).is_err());
    }

    #[test]
    fn smgr_create_records_pump_marker() {
        use crate::filter::main_data::XLOG_SMGR_CREATE;
        let mut f = Filter::new();
        let mut md = Vec::new();
        md.extend_from_slice(&1663u32.to_le_bytes());
        md.extend_from_slice(&5u32.to_le_bytes());
        md.extend_from_slice(&24000u32.to_le_bytes());
        md.extend_from_slice(&0i32.to_le_bytes()); // MAIN_FORKNUM
        let mut r = rec(RmId::Smgr, &[]);
        r.header.info = XLOG_SMGR_CREATE;
        r.main_data = std::borrow::Cow::Owned(md);
        f.decide_record(&r, 777, 0xD116).unwrap();
        let rfn = RelFileNode {
            spc_node: 1663,
            db_node: 5,
            rel_node: 24000,
        };
        assert_eq!(f.smgr_markers().lock().unwrap().get(rfn), Some(777));
        // Same (db, rel) under another tablespace is a distinct filenode
        assert_eq!(
            f.smgr_markers().lock().unwrap().get(RelFileNode {
                spc_node: 9999,
                ..rfn
            }),
            None
        );
    }
}
