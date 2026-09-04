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

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use walrus::pg::walparser::{RelFileNode, RmId, XLogRecord, XLogRecordBlock};

use crate::decode::heap_decoder::{XLOG_HEAP_INPLACE, XLOG_HEAP_OPMASK};
use crate::decode::wal_xact::{
    XLOG_XACT_ABORT, XLOG_XACT_ABORT_PREPARED, XLOG_XACT_ASSIGNMENT, XLOG_XACT_COMMIT,
    XLOG_XACT_COMMIT_PREPARED, XLOG_XACT_INVALIDATIONS, XLOG_XACT_OPMASK, XactPayloadError,
    parse_xact_assignment, parse_xact_invalidations, parse_xact_payload,
};
use tokio_postgres::Client;

use crate::filter::catalog_tracker::{CatalogTracker, CatalogTrackerStats, SeedError};
use crate::filter::classify::{Class, classify};
use crate::filter::dirty_tree::{DirtyState, DirtyTree};
use crate::filter::main_data;
use crate::filter::manifest::ManifestStats;
use crate::record::{AffectedOid, BoundaryInfo, BoundaryKind, Route, rmgr_label};
use crate::schema::FIRST_NORMAL_OBJECT_ID;
use ahash::{HashMap, HashSet, HashSetExt};

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
    /// xid wrote a catalog-touching record), or a command boundary inside
    /// one. Pump holds shadow publication here
    /// until replay passes the record's `next_lsn`
    pub catalog_boundary: bool,
    /// Capture input; `Some` iff `catalog_boundary`
    pub boundary: Option<Arc<BoundaryInfo>>,
    /// Members of a catalog-dirty tree this record aborted
    pub aborted_tree: Option<Arc<Vec<u32>>>,
    /// User-route record whose xact tree wrote catalog state earlier in
    /// the stream: decoder must not decode with live descriptors, hold raw
    /// until commit-time capture publishes the final layout
    pub defer_catalog_decode: bool,
}

/// What one `Xact` rmgr record did to the dirty tree
#[derive(Debug, Default)]
struct XactEnd {
    boundary: Option<Arc<BoundaryInfo>>,
    aborted_tree: Option<Arc<Vec<u32>>>,
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
    /// Catalog-dirty transaction trees: xids (top or sub) that wrote a
    /// catalog-touching record or logged a descriptor-relevant
    /// `XLOG_XACT_INVALIDATIONS` set, plus subxid → top links; drained at
    /// commit / abort
    dirty: DirtyTree,
    /// Transactions that wrote only statistics
    stats_writers: HashSet<u32>,
    /// First xid known to be fully visible to this run
    observed_from_xid: Option<u32>,
    smgr_markers: Arc<Mutex<SmgrMarkers>>,
    /// Followed database. Routing and catalog-filenode tracking stay
    /// cluster-wide; this scopes descriptor-capture input only. `None`
    /// (offline segment filter, no capture consumer) proves no record's
    /// database, so no record dirties
    target_db_oid: Option<u32>,
}

impl Filter {
    pub fn new() -> Self {
        Self {
            tracker: CatalogTracker::new(),
            stats: FilterStats::default(),
            dirty: DirtyTree::default(),
            stats_writers: HashSet::new(),
            observed_from_xid: None,
            smgr_markers: Arc::new(Mutex::new(SmgrMarkers::default())),
            target_db_oid: None,
        }
    }

    /// Scope descriptor-capture input to the followed database. Live
    /// streams set this before the first record
    pub fn set_target_db(&mut self, db_oid: u32) {
        self.target_db_oid = Some(db_oid);
    }

    /// Capture reads rotation markers through this handle
    pub fn smgr_markers(&self) -> Arc<Mutex<SmgrMarkers>> {
        self.smgr_markers.clone()
    }

    /// Record an xid after which transactions are fully observed
    pub fn observe_from_xid(&mut self, xid: u32) {
        let earlier = match self.observed_from_xid {
            Some(have) if (have.wrapping_sub(xid) as i32) <= 0 => have,
            _ => xid,
        };
        self.observed_from_xid = Some(earlier);
    }

    /// Seed observation state from source snapshot
    pub async fn seed_observed_from_source(&mut self, client: &Client) -> Result<u32, SeedError> {
        let row = client
            .query_one(
                "SELECT (pg_snapshot_xmax(pg_current_snapshot())::text::numeric \
                 % 4294967296)::bigint",
                &[],
            )
            .await?;
        let next_xid: i64 = row.get(0);
        let next_xid = next_xid as u32;
        self.observe_from_xid(next_xid);
        Ok(next_xid)
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

    /// Classify route and catalog boundary, reject incomplete commit metadata
    pub fn decide_record(
        &mut self,
        record: &XLogRecord,
        source_lsn: u64,
        page_magic: u16,
    ) -> Result<Verdict, XactPayloadError> {
        let obs = self.tracker.observe(record);
        let class = classify(record);
        // `catalog_touch_db` names the database whose catalog this record
        // wrote: only paths proving a catalog relation was written yield
        // `Some`. `Empty`'s None → ToShadow safe default must not dirty
        // (would hold at unrelated commits). Route is database-blind:
        // foreign catalog records still replay on shadow
        let (route, catalog_touch_db) = match class {
            Class::Catalog => (Route::ToShadow, self.descriptor_touch_db(record)),
            // Relmap update (VACUUM FULL mapped catalog) is Special-class,
            // and carries its database in main_data
            Class::Special => (Route::ToShadow, obs.catalog_db_oid),
            Class::User => {
                if any_block_is_catalog(&self.tracker, &record.blocks) {
                    // tracker has filenodes the bootstrap classify rule misses
                    (Route::ToShadow, self.descriptor_touch_db(record))
                } else {
                    (Route::ToDecoder, None)
                }
            }
            Class::Empty => match main_data::relation_for_empty(record) {
                Some(rel) => {
                    if self.tracker.is_catalog(rel.db_node, rel.rel_node) {
                        let opaque = self.tracker.is_opaque_catalog(rel.db_node, rel.rel_node);
                        (Route::ToShadow, (!opaque).then_some(rel.db_node))
                    } else {
                        (Route::ToDecoder, None)
                    }
                }
                None => (Route::ToShadow, None), // safe default
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
        // Running-xacts records provide observation points during streaming
        if record.header.resource_manager_id == RmId::Standby as u8
            && record.header.info & 0xF0 == main_data::XLOG_RUNNING_XACTS
            && let Some(next_xid) = main_data::parse_running_xacts_next_xid(&record.main_data)
        {
            self.observe_from_xid(next_xid);
        }
        let xid = record.header.xact_id;
        // Subxid → top link rides the subxact's first record at
        // wal_level=logical (`XLR_BLOCK_ID_TOPLEVEL_XID`); learn before
        // touch and admission so both resolve the true root
        self.dirty.link(xid, record.toplevel_xid);
        if xid != 0 && self.is_stats_write(record) {
            self.stats_writers.insert(xid);
        }
        // Foreign and shared catalog writes route to shadow and update the
        // cluster-wide tracker above, but feed no descriptor of the
        // followed database, so they must not dirty its capture tree
        if xid != 0 && catalog_touch_db.is_some_and(|db| self.is_target_db(db)) {
            let dirty = self.dirty.touch(xid, source_lsn);
            dirty.direct_write = true;
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
        let defer_catalog_decode = route == Route::ToDecoder && self.dirty.is_dirty(xid);
        let end = self.observe_xact_end(record, source_lsn, page_magic)?;
        self.stats
            .record(class, route, record.header.total_record_length as u64);
        Ok(Verdict {
            route,
            catalog_boundary: end.boundary.is_some(),
            boundary: end.boundary,
            aborted_tree: end.aborted_tree,
            defer_catalog_decode,
        })
    }

    /// Drain dirty xids at commit / abort. Commit of any dirty xid (top,
    /// listed subxact, or prepared xid) is a catalog boundary; abort clears
    /// without holding — rolled-back catalog changes never become visible
    /// in shadow. Commit records carry the full committed-subxact list
    /// (`xactGetCommittedChildren`), the authoritative boundary merge;
    /// ASSIGNMENT / inline-toplevel links only sharpen mid-xact admission.
    /// Defense: a commit carrying local relcache invals is a boundary even
    /// when the dirty tracker missed every write.
    fn observe_xact_end(
        &mut self,
        record: &XLogRecord,
        source_lsn: u64,
        page_magic: u16,
    ) -> Result<XactEnd, XactPayloadError> {
        if record.header.resource_manager_id != RmId::Xact as u8 {
            return Ok(XactEnd::default());
        }
        let info = record.header.info;
        let op = info & XLOG_XACT_OPMASK;
        if op == XLOG_XACT_INVALIDATIONS {
            return Ok(XactEnd {
                boundary: self.observe_xact_invals(record, source_lsn, page_magic)?,
                aborted_tree: None,
            });
        }
        if op == XLOG_XACT_ASSIGNMENT {
            // Batched subxid → top links (every PGPROC_MAX_CACHED_SUBXIDS
            // assignments). Silent loss would leave later child records
            // undeferred, so malformation poisons like commit payloads
            let (top, subs) = parse_xact_assignment(&record.main_data)
                .ok_or_else(|| XactPayloadError::new("xact assignment"))?;
            for sub in subs {
                self.dirty.link(sub, top);
            }
            return Ok(XactEnd::default());
        }
        let is_commit = op == XLOG_XACT_COMMIT || op == XLOG_XACT_COMMIT_PREPARED;
        let is_abort = op == XLOG_XACT_ABORT || op == XLOG_XACT_ABORT_PREPARED;
        if !is_commit && !is_abort {
            return Ok(XactEnd::default());
        }
        let payload = parse_xact_payload(info, &record.main_data, page_magic)?;
        let header_xid = record.header.xact_id;
        let (merged, members) =
            self.dirty
                .drain_tree(header_xid, payload.twophase_xid, &payload.subxacts);
        let root = payload.twophase_xid.unwrap_or(header_xid);
        let stats_only = self.stats_only_tree(root, &members, merged.is_some());
        // Remove statistics markers for all transaction members
        self.forget_stats_writers(&members);
        self.forget_stats_writers(&[header_xid, root]);
        self.forget_stats_writers(&payload.subxacts);
        if !is_commit {
            // Speculative catalog state the tree wrote dies with it. Named
            // on the record so the drop lands on the pump, ahead of any
            // later boundary that would promote it
            return Ok(XactEnd {
                boundary: None,
                aborted_tree: merged.is_some().then(|| Arc::new(members)),
            });
        }
        // Scope contradiction: the tree holds writes admitted as this
        // database's, yet the committing backend names another. One of the
        // two scopes is wrong, so no boundary built here is trustworthy.
        // Checked after the drain — state leaves with the xact either way
        if let Some(target) = self.target_db_oid
            && let Some(db_id) = payload.db_id
            && db_id != target
            && merged.as_ref().is_some_and(|state| state.direct_write)
        {
            return Err(XactPayloadError::ForeignScope { db_id, target });
        }
        // Target relcache invals: second oid source + capture-all trigger.
        // db 0 = shared relation; user rels there are impossible, kept for
        // symmetry with is_target_or_shared
        let mut capture_all = false;
        let mut inval_oids: Vec<u32> = Vec::with_capacity(payload.invals.relcache.len());
        for inval in &payload.invals.relcache {
            if !self.is_target_or_shared(inval.db_id) {
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
        if payload
            .invals
            .namespace
            .hits(|db| self.is_target_or_shared(db))
        {
            capture_all = true;
        }
        let dirty_hit = merged.is_some();
        // Statistics-only transactions do not need invalidation recapture
        if stats_only && !capture_all {
            inval_oids.clear();
        }
        if !dirty_hit && inval_oids.is_empty() && !capture_all {
            return Ok(XactEnd::default());
        }
        // Inval-only boundary (dirty tracker missed the writes): the
        // commit record itself is the only LSN at hand. Later than any
        // of the xact's rows, so its events order after them — safe for
        // descriptor bias (newer reader reads older tuples)
        let mut merged = merged.unwrap_or_else(|| DirtyState::new(source_lsn));
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
        Ok(XactEnd {
            boundary: Some(Arc::new(BoundaryInfo {
                drain_xid: payload.twophase_xid.unwrap_or(header_xid),
                tree_first_touch: merged.first_touch,
                oids,
                capture_all: capture_all || merged.unenumerated,
                kind: BoundaryKind::Commit,
                members,
            })),
            aborted_tree: None,
        })
    }

    /// `XLOG_XACT_INVALIDATIONS`: command-boundary inval set logged
    /// mid-xact at `wal_level=logical`, i.e. at every
    /// `CommandCounterIncrement`. Re-dirties the writing xid so boundary
    /// classification survives a restart whose resume floor sits past the
    /// xact's catalog records. Only descriptor-relevant messages dirty — an
    /// entry with nothing to capture would hold publication at commit for
    /// nothing.
    ///
    /// Same set bounds record: a relation absent from it did not change shape
    /// at this command, so scan stays scoped to named relations. A capture-all
    /// set still bounds nothing, capture degrades xact rather than scan whole
    /// catalog per command
    fn observe_xact_invals(
        &mut self,
        record: &XLogRecord,
        source_lsn: u64,
        page_magic: u16,
    ) -> Result<Option<Arc<BoundaryInfo>>, XactPayloadError> {
        let xid = record.header.xact_id;
        if xid == 0 {
            return Ok(None);
        }
        let invals = parse_xact_invalidations(&record.main_data, page_magic)?;
        let namespace_hit = invals.namespace.hits(|db| self.is_target_or_shared(db));
        let mut flush = false;
        let mut oids: Vec<u32> = Vec::with_capacity(invals.relcache.len());
        for inval in &invals.relcache {
            if !self.is_target_or_shared(inval.db_id) {
                continue;
            }
            if inval.rel_id == 0 {
                flush = true;
            } else if inval.rel_id >= FIRST_NORMAL_OBJECT_ID {
                oids.push(inval.rel_id);
            }
        }
        if !namespace_hit && !flush && oids.is_empty() {
            return Ok(None);
        }
        let root = self.dirty.root(xid);
        // Skip invalidation-only work for statistics-only transactions
        if !namespace_hit && !flush && self.stats_only_tree(root, &[xid], self.dirty.is_dirty(xid))
        {
            return Ok(None);
        }
        let dirty = self.dirty.touch(xid, source_lsn);
        dirty.unenumerated |= namespace_hit || flush;
        for oid in &oids {
            // Inval record LSN sits at command end: after the command's
            // catalog writes, before commit — a live pg_class decode's
            // earlier touch wins via or_insert
            dirty.oids.entry(*oid).or_insert(source_lsn);
        }
        let tree_first_touch = dirty.first_touch;
        let touches: Vec<AffectedOid> = {
            let mut touches: Vec<AffectedOid> = oids
                .iter()
                .map(|oid| AffectedOid {
                    oid: *oid,
                    pg_class_touch: dirty.oids.get(oid).copied(),
                })
                .collect();
            touches.sort_unstable_by_key(|a| a.oid);
            touches.dedup_by_key(|a| a.oid);
            touches
        };
        Ok(Some(Arc::new(BoundaryInfo {
            drain_xid: root,
            tree_first_touch,
            oids: touches,
            capture_all: namespace_hit || flush,
            kind: BoundaryKind::Command { writer_xid: xid },
            members: Vec::new(),
        })))
    }

    /// Return true for a statistics write in followed database
    fn is_stats_write(&self, record: &XLogRecord) -> bool {
        let Some(rel) = record.blocks.first().map(|b| b.header.location.rel) else {
            return false;
        };
        if !self.is_target_db(rel.db_node) {
            return false;
        }
        if self.tracker.is_opaque_catalog(rel.db_node, rel.rel_node) {
            return true;
        }
        record.header.resource_manager_id == RmId::Heap as u8
            && record.header.info & XLOG_HEAP_OPMASK == XLOG_HEAP_INPLACE
            && self.tracker.is_catalog(rel.db_node, rel.rel_node)
    }

    /// Return true when transaction history is fully observed
    fn fully_observed(&self, xid: u32) -> bool {
        // Compare transaction IDs with wraparound
        self.observed_from_xid
            .is_some_and(|from| (xid.wrapping_sub(from) as i32) >= 0)
    }

    /// Return true when tree contains only fully observed statistics writes
    fn stats_only_tree(&self, root: u32, members: &[u32], dirty_hit: bool) -> bool {
        !dirty_hit
            && self.fully_observed(root)
            && (self.stats_writers.contains(&root)
                || members.iter().any(|m| self.stats_writers.contains(m)))
    }

    fn forget_stats_writers(&mut self, members: &[u32]) {
        for m in members {
            self.stats_writers.remove(m);
        }
    }

    /// Return database for catalog writes that affect descriptors
    fn descriptor_touch_db(&self, record: &XLogRecord) -> Option<u32> {
        let db = catalog_write_db(record)?;
        let rel = record.blocks.first()?.header.location.rel;
        (!self.tracker.is_opaque_catalog(rel.db_node, rel.rel_node)).then_some(db)
    }

    /// Per-database relation / catalog scope: the record's database is
    /// provably the followed one. An unwired filter proves nothing
    fn is_target_db(&self, db: u32) -> bool {
        self.target_db_oid == Some(db)
    }

    /// Invalidation-message scope: accept db in {0, followed}. PG uses
    /// `dbId == 0` for shared relations and whole-relcache scope
    /// (`src/include/storage/sinval.h`). An unwired filter proves nothing,
    /// so it admits nothing: shared scope is only shared relative to a
    /// followed database
    fn is_target_or_shared(&self, db: u32) -> bool {
        self.target_db_oid
            .is_some_and(|target| db == 0 || db == target)
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

/// Catalog pages take physical writes from any backend — opportunistic
/// prune during a scan (PG `src/backend/access/heap/pruneheap.c`), tuple
/// locks, vacuum inplace stats — all stamped with the writer's xid and none
/// a logical catalog change. Only row mutations prove DDL; everything else
/// still routes ToShadow but must not dirty the writer's tree (a user xact
/// pruning a pg_class page would fence its own rows). Non-heap rmgrs
/// (catalog index writes) never dirty: their logical counterpart is the
/// heap record beside them
/// Database of a direct catalog write: block 0's locator, which PG stamps
/// with the writing backend's database (`0` for shared catalogs). A record
/// with no block reference proves no database and never dirties
fn catalog_write_db(record: &XLogRecord) -> Option<u32> {
    if !catalog_mutation(record) {
        return None;
    }
    Some(record.blocks.first()?.header.location.rel.db_node)
}

fn catalog_mutation(record: &XLogRecord) -> bool {
    use crate::decode::heap_decoder::{
        XLOG_HEAP_DELETE, XLOG_HEAP_HOT_UPDATE, XLOG_HEAP_INSERT, XLOG_HEAP_OPMASK,
        XLOG_HEAP_UPDATE, XLOG_HEAP2_MULTI_INSERT,
    };
    let op = record.header.info & XLOG_HEAP_OPMASK;
    let rm = record.header.resource_manager_id;
    if rm == RmId::Heap as u8 {
        matches!(
            op,
            XLOG_HEAP_INSERT | XLOG_HEAP_DELETE | XLOG_HEAP_UPDATE | XLOG_HEAP_HOT_UPDATE
        )
    } else {
        rm == RmId::Heap2 as u8 && op == XLOG_HEAP2_MULTI_INSERT
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use walrus::pg::walparser::{
        BlockLocation, RelFileNode, RmId, XLogRecordBlockHeader, XLogRecordHeader,
    };

    /// Followed database in these tests; 6 is the foreign one
    const TARGET_DB: u32 = 5;

    /// Live wiring: the followed database is set before the first record
    fn target_filter() -> Filter {
        let mut f = Filter::new();
        f.set_target_db(TARGET_DB);
        f
    }

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
        let mut f = target_filter();
        let r = rec(RmId::Heap, &[(5, 1259)]);
        assert_eq!(f.decide(&r), Route::ToShadow);
    }

    #[test]
    fn user_record_is_dropped() {
        let mut f = target_filter();
        let r = rec(RmId::Heap, &[(5, 20000)]);
        assert_eq!(f.decide(&r), Route::ToDecoder);
    }

    #[test]
    fn special_rmgr_is_kept() {
        let mut f = target_filter();
        let r = rec(RmId::Xact, &[]);
        assert_eq!(f.decide(&r), Route::ToShadow);
    }

    #[test]
    fn empty_unknown_is_kept_safe_default() {
        let mut f = target_filter();
        let r = rec(RmId::Heap, &[]);
        assert_eq!(f.decide(&r), Route::ToShadow);
    }

    #[test]
    fn tracker_promotes_user_to_catalog_post_relmap() {
        let mut f = target_filter();
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
        let mut f = target_filter();
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
        let mut f = target_filter();
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
        let mut f = target_filter();
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
        let mut f = target_filter();
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
        let mut f = target_filter();
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
    fn defer_flag_tracks_dirty_tree() {
        let mut f = target_filter();
        f.decide_record(&rec_with_xid(RmId::Heap, &[(5, 1259)], 7), 100, 0xD116)
            .unwrap();
        let user = |f: &mut Filter, xid| {
            f.decide_record(&rec_with_xid(RmId::Heap, &[(5, 20000)], xid), 110, 0xD116)
                .unwrap()
        };
        assert!(user(&mut f, 7).defer_catalog_decode, "post-touch user row");
        assert!(!user(&mut f, 8).defer_catalog_decode, "interleaved xid");
        f.decide_record(&xact_end(XLOG_XACT_COMMIT, 7, &[], None), 200, 0xD116)
            .unwrap();
        assert!(!user(&mut f, 7).defer_catalog_decode, "commit clears");
    }

    #[test]
    fn inline_toplevel_defers_whole_tree_until_subxact_abort() {
        let mut f = target_filter();
        // Subxact's catalog write carries its top inline (logical WAL)
        let mut ddl = rec_with_xid(RmId::Heap, &[(5, 1259)], 101);
        ddl.toplevel_xid = 100;
        f.decide_record(&ddl, 100, 0xD116).unwrap();
        let user = |f: &mut Filter, xid| {
            f.decide_record(&rec_with_xid(RmId::Heap, &[(5, 20000)], xid), 110, 0xD116)
                .unwrap()
                .defer_catalog_decode
        };
        assert!(user(&mut f, 100), "top defers via dirty child");
        assert!(user(&mut f, 101));
        // ROLLBACK TO SAVEPOINT drops the child's dirt, top runs clean
        f.decide_record(&xact_end(XLOG_XACT_ABORT, 101, &[], None), 150, 0xD116)
            .unwrap();
        assert!(!user(&mut f, 100), "subxact abort clears its subtree");
        let v = f
            .decide_record(&xact_end(XLOG_XACT_COMMIT, 100, &[], None), 200, 0xD116)
            .unwrap();
        assert!(!v.catalog_boundary, "aborted child never bounds");
    }

    #[test]
    fn catalog_page_maintenance_never_dirties() {
        use crate::decode::heap_decoder::{XLOG_HEAP_INPLACE, XLOG_HEAP_LOCK};
        let mut f = target_filter();
        let user = |f: &mut Filter, xid| {
            f.decide_record(&rec_with_xid(RmId::Heap, &[(5, 20000)], xid), 110, 0xD116)
                .unwrap()
        };
        // Opportunistic prune on a pg_class page carries the scanning
        // xact's xid (PRUNE_ON_ACCESS = 0x10 on PG 17)
        let mut prune = rec_with_xid(RmId::Heap2, &[(5, 1259)], 7);
        prune.header.info = 0x10;
        assert_eq!(
            f.decide_record(&prune, 100, 0xD116).unwrap().route,
            Route::ToShadow,
            "maintenance still routes to shadow"
        );
        assert!(
            !user(&mut f, 7).defer_catalog_decode,
            "prune must not fence the pruner's own rows"
        );
        let v = f
            .decide_record(&xact_end(XLOG_XACT_COMMIT, 7, &[], None), 200, 0xD116)
            .unwrap();
        assert!(!v.catalog_boundary, "prune-only commit never bounds");
        // Tuple lock and vacuum inplace stats: same treatment
        for info in [XLOG_HEAP_LOCK, XLOG_HEAP_INPLACE] {
            let mut r = rec_with_xid(RmId::Heap, &[(5, 1259)], 8);
            r.header.info = info;
            f.decide_record(&r, 300, 0xD116).unwrap();
        }
        assert!(!user(&mut f, 8).defer_catalog_decode);
        // Real mutation still dirties
        f.decide_record(&rec_with_xid(RmId::Heap, &[(5, 1259)], 9), 400, 0xD116)
            .unwrap();
        assert!(user(&mut f, 9).defer_catalog_decode);
    }

    /// `xl_xact_assignment`: `(u32 xtop, i32 nsubxacts, subxids…)`
    fn xact_assignment(top: u32, subs: &[u32]) -> XLogRecord<'static> {
        let mut md = top.to_le_bytes().to_vec();
        md.extend_from_slice(&(subs.len() as i32).to_le_bytes());
        for s in subs {
            md.extend_from_slice(&s.to_le_bytes());
        }
        let mut r = rec_with_xid(RmId::Xact, &[], subs.first().copied().unwrap_or(0));
        r.header.info = XLOG_XACT_ASSIGNMENT;
        r.main_data = std::borrow::Cow::Owned(md);
        r
    }

    #[test]
    fn assignment_merges_retained_child_state() {
        let mut f = target_filter();
        // Child dirties without inline toplevel (replica-level WAL shape)
        f.decide_record(&rec_with_xid(RmId::Heap, &[(5, 1259)], 101), 100, 0xD116)
            .unwrap();
        let user = |f: &mut Filter, xid| {
            f.decide_record(&rec_with_xid(RmId::Heap, &[(5, 20000)], xid), 110, 0xD116)
                .unwrap()
                .defer_catalog_decode
        };
        assert!(user(&mut f, 101), "child defers after own touch");
        assert!(!user(&mut f, 100), "top unknown until assignment");
        f.decide_record(&xact_assignment(100, &[101, 102]), 120, 0xD116)
            .unwrap();
        assert!(user(&mut f, 100), "assignment merges retained state");
        assert!(user(&mut f, 102), "assigned sibling shares the tree");
        let v = f
            .decide_record(
                &xact_end(XLOG_XACT_COMMIT, 100, &[101, 102], None),
                200,
                0xD116,
            )
            .unwrap();
        assert!(v.catalog_boundary);
        assert!(!user(&mut f, 100), "commit clears the tree");
    }

    #[test]
    fn malformed_assignment_poisons() {
        let mut f = target_filter();
        let mut r = xact_assignment(100, &[101]);
        // Claim two subxids, carry one
        match &mut r.main_data {
            std::borrow::Cow::Owned(md) => md[4..8].copy_from_slice(&2i32.to_le_bytes()),
            _ => unreachable!(),
        }
        assert!(f.decide_record(&r, 150, 0xD116).is_err());
    }

    #[test]
    fn commit_prepared_matches_prepared_xid() {
        let mut f = target_filter();
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
        let mut f = target_filter();
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
        let mut f = target_filter();
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
        let mut f = target_filter();
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
        let mut f = target_filter();
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

    /// Build a running-xacts record
    fn running_xacts_rec(next_xid: u32) -> XLogRecord<'static> {
        let mut md = vec![0u8; 24];
        md[12..16].copy_from_slice(&next_xid.to_le_bytes());
        let mut r = rec(RmId::Standby, &[]);
        r.header.info = main_data::XLOG_RUNNING_XACTS;
        r.main_data = std::borrow::Cow::Owned(md);
        r
    }

    /// Build an in-place pg_class statistics write
    fn pg_class_inplace(xid: u32) -> XLogRecord<'static> {
        let mut r = rec_with_xid(RmId::Heap, &[(5, 1259)], xid);
        r.header.info = XLOG_HEAP_INPLACE;
        r
    }

    /// Statistics writes and their invalidations do not need a boundary
    #[test]
    fn analyze_commit_raises_no_boundary() {
        let mut f = target_filter();
        f.decide_record(&running_xacts_rec(700), 10, 0xD116)
            .unwrap();
        f.decide_record(&rec_with_xid(RmId::Heap, &[(5, 2619)], 746), 100, 0xD116)
            .unwrap();
        f.decide_record(&pg_class_inplace(746), 110, 0xD116)
            .unwrap();
        let invals = xact_invals_rec(746, &[(-2, 5, 16384), (-2, 5, 16389)]);
        assert!(
            f.decide_record(&invals, 120, 0xD116)
                .unwrap()
                .boundary
                .is_none(),
            "command boundary for a statistics-only transaction",
        );
        let commit = xact_end_full(
            XLOG_XACT_COMMIT,
            746,
            &[],
            &[(-2, 5, 16384), (-2, 5, 16389)],
            None,
        );
        assert!(
            f.decide_record(&commit, 130, 0xD116)
                .unwrap()
                .boundary
                .is_none(),
            "commit boundary for a statistics-only transaction",
        );
    }

    #[test]
    fn analyze_before_the_running_xacts_watermark_keeps_its_boundary() {
        let mut f = target_filter();
        // Older transaction may have records before stream start
        f.decide_record(&running_xacts_rec(900), 10, 0xD116)
            .unwrap();
        f.decide_record(&rec_with_xid(RmId::Heap, &[(5, 2619)], 746), 100, 0xD116)
            .unwrap();
        f.decide_record(&pg_class_inplace(746), 110, 0xD116)
            .unwrap();
        let commit = xact_end_full(XLOG_XACT_COMMIT, 746, &[], &[(-2, 5, 16384)], None);
        let b = f
            .decide_record(&commit, 130, 0xD116)
            .unwrap()
            .boundary
            .expect("boundary");
        assert_eq!(b.oids[0].oid, 16384);
    }

    #[test]
    fn ddl_alongside_analyze_keeps_its_boundary() {
        let mut f = target_filter();
        f.decide_record(&running_xacts_rec(700), 10, 0xD116)
            .unwrap();
        // Keep boundary when DDL and ANALYZE share a transaction
        f.decide_record(&rec_with_xid(RmId::Heap, &[(5, 1249)], 746), 100, 0xD116)
            .unwrap();
        f.decide_record(&rec_with_xid(RmId::Heap, &[(5, 2619)], 746), 110, 0xD116)
            .unwrap();
        let commit = xact_end_full(XLOG_XACT_COMMIT, 746, &[], &[(-2, 5, 16384)], None);
        let b = f
            .decide_record(&commit, 130, 0xD116)
            .unwrap()
            .boundary
            .expect("boundary");
        assert_eq!(b.tree_first_touch, 100, "pg_attribute write dirtied first");
        assert_eq!(b.oids[0].oid, 16384);
    }

    #[test]
    fn statistics_writes_alone_never_dirty() {
        let mut f = target_filter();
        let stats = rec_with_xid(RmId::Heap, &[(5, 2619)], 7);
        assert_eq!(
            f.decide_record(&stats, 100, 0xD116).unwrap().route,
            Route::ToShadow,
            "statistics still replay on shadow",
        );
        let commit = xact_end(XLOG_XACT_COMMIT, 7, &[], None);
        assert!(
            f.decide_record(&commit, 200, 0xD116)
                .unwrap()
                .boundary
                .is_none(),
        );
    }

    #[test]
    fn inval_only_commit_is_boundary_defense() {
        let mut f = target_filter();
        // Dirty tracker saw nothing, but the commit proves catalog effects
        let commit = xact_end_full(XLOG_XACT_COMMIT, 9, &[], &[(-2, 5, 16500)], None);
        let v = f.decide_record(&commit, 300, 0xD116).unwrap();
        let b = v.boundary.expect("inval-only boundary");
        assert_eq!(b.tree_first_touch, 300, "commit lsn fallback");
        assert_eq!(b.oids[0].oid, 16500);
    }

    #[test]
    fn whole_relcache_inval_forces_capture_all() {
        let mut f = target_filter();
        let commit = xact_end_full(XLOG_XACT_COMMIT, 9, &[], &[(-2, 5, 0)], None);
        let v = f.decide_record(&commit, 300, 0xD116).unwrap();
        assert!(v.boundary.expect("boundary").capture_all);
    }

    #[test]
    fn pg_namespace_write_forces_capture_all() {
        let mut f = target_filter();
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
        let mut f = target_filter();
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
        let mut f = target_filter();
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
        let mut f = target_filter();
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
    fn unwired_filter_captures_nothing() {
        let mut f = Filter::new();
        // Mid-xact set mixing per-database, shared and whole-relcache scope
        let v = f
            .decide_record(
                &xact_invals_rec(7, &[(-2, 5, 16400), (-2, 0, 0), (35, 0, 0)]),
                150,
                0xD116,
            )
            .unwrap();
        assert!(v.boundary.is_none(), "no followed database to be in scope");
        let commit = xact_end_full(XLOG_XACT_COMMIT, 7, &[], &[(-2, 5, 16400)], None);
        let v = f.decide_record(&commit, 200, 0xD116).unwrap();
        assert!(v.boundary.is_none(), "unwired filter stays route-only");
        assert_eq!(v.route, Route::ToShadow, "routing stays database-blind");
    }

    #[test]
    fn namespace_catcache_foreign_db_filtered() {
        let mut f = target_filter();
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
        let mut f = target_filter();
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
        let mut f = target_filter();
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
        let mut f = target_filter();
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
        let mut f = target_filter();
        f.decide_record(&xact_invals_rec(7, &[(-2, 5, 0)]), 150, 0xD116)
            .unwrap();
        let v = f
            .decide_record(&xact_end(XLOG_XACT_COMMIT, 7, &[], None), 200, 0xD116)
            .unwrap();
        assert!(v.boundary.expect("boundary").capture_all);
    }

    #[test]
    fn midxact_irrelevant_invals_do_not_dirty() {
        let mut f = target_filter();
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
        let mut f = target_filter();
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
        let mut f = target_filter();
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
        let mut f = target_filter();
        f.decide_record(&xact_invals_rec(101, &[(-2, 5, 16400)]), 150, 0xD116)
            .unwrap();
        let v = f
            .decide_record(&xact_end(XLOG_XACT_COMMIT, 100, &[101], None), 200, 0xD116)
            .unwrap();
        assert_eq!(v.boundary.expect("boundary").oids[0].oid, 16400);
    }

    #[test]
    fn command_boundary_is_always_on_and_scopes_to_this_command_invals() {
        let mut f = target_filter();
        // Earlier command touched 16400; this one names 16500 only
        f.decide_record(&xact_invals_rec(7, &[(-2, 5, 16400)]), 150, 0xD116)
            .unwrap();
        let v = f
            .decide_record(&xact_invals_rec(7, &[(-2, 5, 16500)]), 250, 0xD116)
            .unwrap();
        let b = v.boundary.expect("command boundary");
        assert_eq!(b.kind, BoundaryKind::Command { writer_xid: 7 });
        assert_eq!(b.drain_xid, 7);
        assert_eq!(b.tree_first_touch, 150);
        let oids: Vec<u32> = b.oids.iter().map(|a| a.oid).collect();
        assert_eq!(oids, [16500], "a relation this command left alone");
        assert!(b.members.is_empty());
        // Commit still bounds, over the whole tree's oids
        let v = f
            .decide_record(&xact_end(XLOG_XACT_COMMIT, 7, &[], None), 300, 0xD116)
            .unwrap();
        let b = v.boundary.expect("commit boundary");
        assert_eq!(b.kind, BoundaryKind::Commit);
        assert_eq!(b.members, [7]);
        let oids: Vec<u32> = b.oids.iter().map(|a| a.oid).collect();
        assert_eq!(oids, [16400, 16500]);
    }

    #[test]
    fn command_boundary_under_subxact_names_the_known_root() {
        let mut f = target_filter();
        let mut sub = xact_invals_rec(101, &[(-2, 5, 16400)]);
        sub.toplevel_xid = 100;
        let v = f.decide_record(&sub, 150, 0xD116).unwrap();
        let b = v.boundary.expect("command boundary");
        assert_eq!(b.drain_xid, 100, "slots key under the tree root");
        assert_eq!(b.kind, BoundaryKind::Command { writer_xid: 101 });
    }

    #[test]
    fn capture_all_command_boundary_carries_the_flag() {
        let mut f = target_filter();
        let v = f
            .decide_record(&xact_invals_rec(7, &[(-2, 5, 0)]), 150, 0xD116)
            .unwrap();
        let b = v.boundary.expect("command boundary");
        assert!(b.capture_all, "whole-relcache flush names no relations");
        assert!(b.oids.is_empty());
    }

    #[test]
    fn abort_names_the_dirty_tree_for_pending_drop() {
        let mut f = target_filter();
        let mut sub = xact_invals_rec(101, &[(-2, 5, 16400)]);
        sub.toplevel_xid = 100;
        f.decide_record(&sub, 150, 0xD116).unwrap();
        // ROLLBACK TO SAVEPOINT: the subxact alone
        let v = f
            .decide_record(&xact_end(XLOG_XACT_ABORT, 101, &[], None), 200, 0xD116)
            .unwrap();
        let members = v.aborted_tree.expect("aborted members");
        assert_eq!(*members, [101]);
        // Clean tree: nothing speculative to drop
        let v = f
            .decide_record(&xact_end(XLOG_XACT_ABORT, 900, &[], None), 300, 0xD116)
            .unwrap();
        assert!(v.aborted_tree.is_none());
    }

    #[test]
    fn malformed_midxact_inval_record_poisons() {
        let mut f = target_filter();
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
        let mut f = target_filter();
        let commit = xact_end_full(XLOG_XACT_COMMIT, 9, &[], &[(-2, 6, 16500)], None);
        let v = f.decide_record(&commit, 300, 0xD116).unwrap();
        assert!(v.boundary.is_none(), "foreign-db inval must not bound");
    }

    #[test]
    fn prepared_commit_boundary_drains_under_prepared_xid() {
        let mut f = target_filter();
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
        let mut f = target_filter();
        let commit = xact_end_full(XLOG_XACT_COMMIT, 9, &[], &[(-7, 5, 16500)], None);
        assert!(f.decide_record(&commit, 300, 0xD116).is_err());
    }

    #[test]
    fn smgr_create_records_pump_marker() {
        use crate::filter::main_data::XLOG_SMGR_CREATE;
        let mut f = target_filter();
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

    const FOREIGN_DB: u32 = 6;

    /// Heap insert into `db`'s pg_class carrying a decodable row for `oid`
    fn pg_class_write(db: u32, xid: u32, oid: u32) -> XLogRecord<'static> {
        let mut data = Vec::new();
        data.extend_from_slice(&33u16.to_le_bytes()); // t_infomask2
        data.extend_from_slice(&0u16.to_le_bytes()); // t_infomask
        data.push(24); // t_hoff
        data.push(0); // MAXALIGN pad, offset 23 -> 24
        data.extend_from_slice(&oid.to_le_bytes());
        data.extend_from_slice(&[0u8; 84]); // relname + cols 3..7
        data.extend_from_slice(&(oid + 1).to_le_bytes()); // relfilenode
        let mut r = rec_with_xid(RmId::Heap, &[(db, 1259)], xid);
        r.blocks[0].data = std::borrow::Cow::Owned(data);
        r
    }

    /// Commit / abort carrying `xl_xact_dbinfo` (`Oid dbId; Oid tsId`), the
    /// committing backend's database
    fn xact_end_dbinfo(
        op: u8,
        xid: u32,
        db_id: u32,
        invals: &[(i8, u32, u32)],
    ) -> XLogRecord<'static> {
        use crate::decode::wal_xact::{XACT_XINFO_HAS_DBINFO, XACT_XINFO_HAS_INVALS};
        let mut md: Vec<u8> = 0i64.to_le_bytes().to_vec();
        let mut xinfo = XACT_XINFO_HAS_DBINFO;
        if !invals.is_empty() {
            xinfo |= XACT_XINFO_HAS_INVALS;
        }
        md.extend_from_slice(&xinfo.to_le_bytes());
        md.extend_from_slice(&db_id.to_le_bytes());
        md.extend_from_slice(&1663u32.to_le_bytes()); // tsId
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
        let mut r = rec_with_xid(RmId::Xact, &[], xid);
        r.header.info = op | XLOG_XACT_HAS_INFO;
        r.main_data = std::borrow::Cow::Owned(md);
        r
    }

    #[test]
    fn foreign_pg_class_write_never_dirties_target_capture() {
        let mut f = target_filter();
        // DDL in another database: still routes to shadow, but its oids
        // belong to a descriptor log this filter does not feed
        let v = f
            .decide_record(&pg_class_write(FOREIGN_DB, 7, 16400), 100, 0xD116)
            .unwrap();
        assert_eq!(v.route, Route::ToShadow);
        let user = f
            .decide_record(
                &rec_with_xid(RmId::Heap, &[(FOREIGN_DB, 20000)], 7),
                110,
                0xD116,
            )
            .unwrap();
        assert!(
            !user.defer_catalog_decode,
            "foreign DDL must not fence later rows"
        );
        let v = f
            .decide_record(
                &xact_end_dbinfo(XLOG_XACT_COMMIT, 7, FOREIGN_DB, &[]),
                200,
                0xD116,
            )
            .unwrap();
        assert!(!v.catalog_boundary, "foreign commit bounds nothing local");

        // Same oid in the followed database: dirt, fence, boundary, and a
        // first touch that owes nothing to the foreign write
        f.decide_record(&pg_class_write(TARGET_DB, 8, 16400), 300, 0xD116)
            .unwrap();
        let user = f
            .decide_record(
                &rec_with_xid(RmId::Heap, &[(TARGET_DB, 20000)], 8),
                310,
                0xD116,
            )
            .unwrap();
        assert!(user.defer_catalog_decode, "target DDL fences its own rows");
        let b = f
            .decide_record(
                &xact_end_dbinfo(XLOG_XACT_COMMIT, 8, TARGET_DB, &[]),
                400,
                0xD116,
            )
            .unwrap()
            .boundary
            .expect("target boundary");
        assert_eq!(b.oids.len(), 1);
        assert_eq!(b.oids[0].oid, 16400);
        assert_eq!(b.oids[0].pg_class_touch, Some(300), "target write's lsn");
    }

    #[test]
    fn foreign_pg_namespace_write_does_not_force_target_capture_all() {
        let mut f = target_filter();
        // Namespace rename in another database enumerates no relations
        // anywhere; capture-all here would rescan the followed catalog
        f.decide_record(
            &rec_with_xid(RmId::Heap, &[(FOREIGN_DB, 2615)], 7),
            100,
            0xD116,
        )
        .unwrap();
        let v = f
            .decide_record(
                &xact_end_dbinfo(XLOG_XACT_COMMIT, 7, FOREIGN_DB, &[]),
                200,
                0xD116,
            )
            .unwrap();
        assert!(v.boundary.is_none());
    }

    #[test]
    fn foreign_and_shared_relmap_track_without_dirtying_target() {
        use crate::filter::catalog_tracker::test_relmap_record as relmap;
        let mut f = target_filter();
        for (db, xid, filenode) in [(FOREIGN_DB, 9, 50000), (0, 10, 60000)] {
            let mut r = relmap(db, &[(1259, filenode)]);
            r.header.xact_id = xid;
            let v = f.decide_record(&r, 100, 0xD116).unwrap();
            assert_eq!(v.route, Route::ToShadow, "shadow replays every relmap");
            assert!(
                f.tracker().is_catalog(db, filenode),
                "filenode tracking stays cluster-wide"
            );
            let v = f
                .decide_record(
                    &xact_end_dbinfo(XLOG_XACT_COMMIT, xid, db, &[]),
                    200,
                    0xD116,
                )
                .unwrap();
            assert!(!v.catalog_boundary, "db {db} relmap is not target dirt");
        }
    }

    #[test]
    fn shared_inval_bounds_and_drains_under_a_foreign_commit() {
        let mut f = target_filter();
        // dbId 0 = shared / whole-relcache scope, which does reach the
        // followed database
        f.decide_record(&xact_invals_rec(7, &[(-2, 0, 0)]), 150, 0xD116)
            .unwrap();
        let v = f
            .decide_record(
                &xact_end_dbinfo(XLOG_XACT_COMMIT, 7, FOREIGN_DB, &[]),
                200,
                0xD116,
            )
            .unwrap();
        assert!(
            v.boundary.expect("shared inval boundary").capture_all,
            "foreign dbId must not blanket-drop shared invalidations"
        );
        let v = f
            .decide_record(
                &xact_end_dbinfo(XLOG_XACT_COMMIT, 7, FOREIGN_DB, &[]),
                300,
                0xD116,
            )
            .unwrap();
        assert!(!v.catalog_boundary, "commit drained the tree");
    }

    #[test]
    fn foreign_dbinfo_contradicting_target_dirt_poisons() {
        let mut f = target_filter();
        f.decide_record(&pg_class_write(TARGET_DB, 7, 16400), 100, 0xD116)
            .unwrap();
        let err = f
            .decide_record(
                &xact_end_dbinfo(XLOG_XACT_COMMIT, 7, FOREIGN_DB, &[]),
                200,
                0xD116,
            )
            .unwrap_err();
        assert!(
            matches!(
                err,
                XactPayloadError::ForeignScope {
                    db_id: FOREIGN_DB,
                    target: TARGET_DB
                }
            ),
            "{err}"
        );
        let v = f
            .decide_record(&xact_end(XLOG_XACT_COMMIT, 7, &[], None), 300, 0xD116)
            .unwrap();
        assert!(!v.catalog_boundary, "drain ran before the scope check");
    }

    #[test]
    fn absent_dbinfo_leaves_record_scope_in_charge() {
        let mut f = target_filter();
        f.decide_record(&pg_class_write(TARGET_DB, 7, 16400), 100, 0xD116)
            .unwrap();
        let b = f
            .decide_record(&xact_end(XLOG_XACT_COMMIT, 7, &[], None), 200, 0xD116)
            .unwrap()
            .boundary
            .expect("boundary");
        assert_eq!(b.oids[0].oid, 16400);
    }
}
