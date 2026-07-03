//! Backup-era tuple visibility gate for backup-sourced initial loads
//! (plans/add_table.md §Visibility gate).
//!
//! A page walk sees raw pages: dead-but-unvacuumed tuples, aborted inserts,
//! in-flight writers. Emit a tuple only when backup-era `pg_xact` says `xmin`
//! committed and `xmax` absent/aborted; infomask hint bits
//! (PG `src/include/access/htup_details.h`) short-circuit most lookups.
//!
//! - Skipped in-flight tuples are re-delivered by the mode's WAL leg
//!   (their commits land past the walk's coverage start).
//! - Skipped aborted tuples were never real.
//! - Dead tuples stop resurrecting, the fidelity gain over greenfield's
//!   emit-every-`LP_NORMAL` stance.
//!
//! PgXact sources layer: a [`PgXactPatch`] harvested from gap-WAL commit/abort
//! records overlays the [`PgXactAccum`] collected from the backup's `pg_xact/`
//! files. Patch covers xacts in flight across the backup's redo point whose
//! commits land inside the archive-replay gap; without it their backup-page
//! tuples read as in-progress and rows written before the redo point would be
//! lost because gap replay only re-delivers records ≥ redo.
//!
//! `HEAP_XMAX_IS_MULTI` resolves through the backup's `pg_multixact/`
//! (offsets + members SLRUs, [`PgMultiXactAccum`]): the update/delete
//! member's xid runs through the same pg_xact view. Bytes the backup never
//! copied prove the multi postdates the copy — copies happen past the redo
//! point, so the update's WAL record is covered by the mode's WAL leg and
//! the old version emits safely. A multi the snapshot can't bound
//! (truncated below, garbage) is [`Visibility::Unresolvable`]: emitting
//! risks resurrecting a pre-coverage dead version, skipping risks dropping
//! a live row, so the pass aborts.

use std::collections::{HashMap, HashSet};

// t_infomask bits, PG src/include/access/htup_details.h
pub const HEAP_XMAX_KEYSHR_LOCK: u16 = 0x0010;
pub const HEAP_XMAX_EXCL_LOCK: u16 = 0x0040;
pub const HEAP_XMAX_LOCK_ONLY: u16 = 0x0080;
pub const HEAP_XMIN_COMMITTED: u16 = 0x0100;
pub const HEAP_XMIN_INVALID: u16 = 0x0200;
pub const HEAP_XMIN_FROZEN: u16 = HEAP_XMIN_COMMITTED | HEAP_XMIN_INVALID;
pub const HEAP_XMAX_COMMITTED: u16 = 0x0400;
pub const HEAP_XMAX_INVALID: u16 = 0x0800;
pub const HEAP_XMAX_IS_MULTI: u16 = 0x1000;
pub const HEAP_XMAX_SHR_LOCK: u16 = HEAP_XMAX_EXCL_LOCK | HEAP_XMAX_KEYSHR_LOCK;
pub const HEAP_LOCK_MASK: u16 = HEAP_XMAX_SHR_LOCK | HEAP_XMAX_EXCL_LOCK | HEAP_XMAX_KEYSHR_LOCK;

/// `FirstNormalTransactionId`; 1 = bootstrap, 2 = frozen, both committed
pub const FIRST_NORMAL_XID: u32 = 3;

// pg_xact SLRU geometry: 2 status bits per xid, 8 KiB pages, 32 pages per
// segment file (PG transaction-status SLRU / slru.h)
const PG_XACT_XIDS_PER_BYTE: u32 = 4;
const PG_XACT_XIDS_PER_PAGE: u32 = 8192 * PG_XACT_XIDS_PER_BYTE;
const SLRU_PAGES_PER_SEGMENT: u32 = 32;
pub const PG_XACT_XIDS_PER_SEGMENT: u32 = PG_XACT_XIDS_PER_PAGE * SLRU_PAGES_PER_SEGMENT;

// 0x00 in-progress, 0x03 sub-committed: both resolve to InProgress
const TRANSACTION_STATUS_COMMITTED: u8 = 0x01;
const TRANSACTION_STATUS_ABORTED: u8 = 0x02;
#[cfg(test)]
const TRANSACTION_STATUS_SUB_COMMITTED: u8 = 0x03;

// pg_multixact SLRU geometry (PG src/backend/access/transam/multixact.c):
// offsets are 4-byte MultiXactOffset entries, 2048 per 8 KiB page; members
// pack groups of 4 flag bytes + 4 xids (20 bytes), 409 groups per page with
// 12 pad bytes at each page end
const MXOFF_PER_SEGMENT: u32 = (8192 / 4) * SLRU_PAGES_PER_SEGMENT;
const MULTIXACT_MEMBERS_PER_GROUP: u32 = 4;
const MULTIXACT_GROUP_SIZE: usize = 4 + 4 * 4;
const MULTIXACT_MEMBERS_PER_PAGE: u32 =
    (8192 / MULTIXACT_GROUP_SIZE as u32) * MULTIXACT_MEMBERS_PER_GROUP;
// MultiXactStatus (PG src/include/access/multixact.h): 0..=3 lock
// strengths, 4 NoKeyUpdate, 5 Update/delete; ISUPDATE is status > ForUpdate
const MULTIXACT_STATUS_FOR_UPDATE: u8 = 3;
const MULTIXACT_STATUS_UPDATE: u8 = 5;
/// Members-range width past any plausible locker count reads as snapshot
/// skew between the two offsets entries, not a real multi
const MULTIXACT_MEMBERS_SANITY_CAP: u32 = 1 << 20;

/// `HeapTupleHeaderXminFrozen`: both bits set means frozen, not
/// committed+invalid
pub fn xmin_frozen(infomask: u16) -> bool {
    infomask & HEAP_XMIN_FROZEN == HEAP_XMIN_FROZEN
}

/// `HEAP_XMAX_IS_LOCKED_ONLY` (htup_details.h): xmax is a locker, not an
/// updater; pg_upgrade legacy shape is EXCL_LOCK without IS_MULTI/LOCK_MASK
pub fn xmax_locked_only(infomask: u16) -> bool {
    infomask & HEAP_XMAX_LOCK_ONLY != 0
        || infomask & (HEAP_XMAX_IS_MULTI | HEAP_LOCK_MASK) == HEAP_XMAX_EXCL_LOCK
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum XidStatus {
    Committed,
    Aborted,
    InProgress,
    /// No pg_xact coverage: xid predates oldest collected segment
    /// (truncated ⇒ ancient ⇒ committed-or-vacuumed)
    Unknown,
}

/// `pg_xact/` segment files collected from the backup stream, keyed by
/// segment number (the hex filename). Whole files stay in memory: 256 KiB
/// per segment, one per ~1M xids.
#[derive(Debug, Default)]
pub struct PgXactAccum {
    segments: HashMap<u32, Vec<u8>>,
}

impl PgXactAccum {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert_segment(&mut self, segno: u32, bytes: Vec<u8>) {
        self.segments.insert(segno, bytes);
    }

    pub fn segment_count(&self) -> usize {
        self.segments.len()
    }

    /// Raw 2-bit status for a normal xid. Beyond the collected tail ⇒ the
    /// xid wasn't assigned when the backup copied pg_xact ⇒ in-progress (its
    /// commit postdates the backup; the WAL leg owns it). Below the oldest
    /// collected segment ⇒ truncated ⇒ `Unknown`.
    pub fn status(&self, xid: u32) -> XidStatus {
        let segno = xid / PG_XACT_XIDS_PER_SEGMENT;
        let Some(seg) = self.segments.get(&segno) else {
            if self.segments.keys().any(|s| *s < segno) {
                return XidStatus::InProgress;
            }
            return XidStatus::Unknown;
        };
        let byte = ((xid % PG_XACT_XIDS_PER_SEGMENT) / PG_XACT_XIDS_PER_BYTE) as usize;
        let Some(b) = seg.get(byte) else {
            return XidStatus::InProgress;
        };
        let shift = (xid % PG_XACT_XIDS_PER_BYTE) * 2;
        match (b >> shift) & 0x3 {
            TRANSACTION_STATUS_COMMITTED => XidStatus::Committed,
            TRANSACTION_STATUS_ABORTED => XidStatus::Aborted,
            // In-progress, or sub-committed (parent unresolved when this
            // byte was copied; the patch resolves a gap-committed parent)
            _ => XidStatus::InProgress,
        }
    }
}

/// Resolution of a non-lock-only multixact `xmax` against the backup's
/// `pg_multixact/` snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MultiXactUpdater {
    /// Created after the snapshot bytes were copied. Copies happen past the
    /// backup's redo point, so the update's WAL record is ≥ redo and the
    /// mode's WAL leg re-delivers it: emitting the old version is safe
    Covered,
    /// Update/delete member's xid; its commit status decides deadness
    Updater(u32),
    /// Every member is a locker
    LockOnly,
    /// Referenced mxid below the snapshot's collected range, or garbage
    /// bytes: deadness unprovable either way
    Unresolvable,
}

enum SlruRead {
    Val(u32),
    /// Segment, page tail, or entry past what the backup copied — or the
    /// reserved zero value: unwritten when the copy happened
    Unwritten,
    /// Below the oldest collected segment
    Truncated,
}

enum MemberRead {
    Member { xid: u32, status: u8 },
    Unwritten,
    Truncated,
}

/// `pg_multixact/{offsets,members}` segments collected from the backup
/// stream, same whole-file-in-memory posture as [`PgXactAccum`].
#[derive(Debug, Default)]
pub struct PgMultiXactAccum {
    offsets: HashMap<u32, Vec<u8>>,
    members: HashMap<u32, Vec<u8>>,
}

impl PgMultiXactAccum {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert_offsets_segment(&mut self, segno: u32, bytes: Vec<u8>) {
        self.offsets.insert(segno, bytes);
    }

    pub fn insert_members_segment(&mut self, segno: u32, bytes: Vec<u8>) {
        self.members.insert(segno, bytes);
    }

    pub fn segment_count(&self) -> usize {
        self.offsets.len() + self.members.len()
    }

    fn read_u32(map: &HashMap<u32, Vec<u8>>, segno: u32, byte: usize) -> SlruRead {
        let Some(seg) = map.get(&segno) else {
            if map.keys().any(|s| *s < segno) {
                return SlruRead::Unwritten;
            }
            return SlruRead::Truncated;
        };
        match seg.get(byte..byte + 4) {
            Some(b) => SlruRead::Val(u32::from_le_bytes(b.try_into().expect("4-byte slice"))),
            None => SlruRead::Unwritten,
        }
    }

    /// Offsets entry for `mxid`. Zero is reserved to mean unset
    /// (`GetNewMultiXactId` skips it), so it reads as unwritten-at-copy.
    fn offset_at(&self, mxid: u32) -> SlruRead {
        let segno = mxid / MXOFF_PER_SEGMENT;
        let byte = ((mxid % MXOFF_PER_SEGMENT) * 4) as usize;
        match Self::read_u32(&self.offsets, segno, byte) {
            SlruRead::Val(0) => SlruRead::Unwritten,
            r => r,
        }
    }

    fn member_at(&self, off: u32) -> MemberRead {
        let page = off / MULTIXACT_MEMBERS_PER_PAGE;
        let segno = page / SLRU_PAGES_PER_SEGMENT;
        let Some(seg) = self.members.get(&segno) else {
            if self.members.keys().any(|s| *s < segno) {
                return MemberRead::Unwritten;
            }
            return MemberRead::Truncated;
        };
        let idx = off % MULTIXACT_MEMBERS_PER_PAGE;
        let member = (idx % MULTIXACT_MEMBERS_PER_GROUP) as usize;
        let base = ((page % SLRU_PAGES_PER_SEGMENT) * 8192) as usize
            + (idx / MULTIXACT_MEMBERS_PER_GROUP) as usize * MULTIXACT_GROUP_SIZE;
        let xid_pos = base + 4 + member * 4;
        match (seg.get(base + member), seg.get(xid_pos..xid_pos + 4)) {
            (Some(&status), Some(b)) => MemberRead::Member {
                xid: u32::from_le_bytes(b.try_into().expect("4-byte slice")),
                status,
            },
            _ => MemberRead::Unwritten,
        }
    }

    /// Resolve `mxid`'s update/delete member. `RecordNewMultiXact` (PG
    /// src/backend/access/transam/multixact.c) fills the offsets entries for
    /// `mxid` and `mxid+1` and every member slot before the mxid can appear
    /// in any tuple, so a read the copy missed proves post-copy creation —
    /// [`MultiXactUpdater::Covered`]. Member xid zero means an unfilled slot
    /// (never a valid xid); slot at member-offset zero is the reserved one
    /// `GetNewMultiXactId` skips.
    pub fn updater(&self, mxid: u32) -> MultiXactUpdater {
        let start = match self.offset_at(mxid) {
            SlruRead::Val(v) => v,
            SlruRead::Unwritten => return MultiXactUpdater::Covered,
            SlruRead::Truncated => return MultiXactUpdater::Unresolvable,
        };
        // mxid+1 wraps past FirstMultiXactId, as GetMultiXactIdMembers
        let next = match mxid.wrapping_add(1) {
            0 => 1,
            n => n,
        };
        let end = match self.offset_at(next) {
            SlruRead::Val(v) => v,
            SlruRead::Unwritten => return MultiXactUpdater::Covered,
            SlruRead::Truncated => return MultiXactUpdater::Unresolvable,
        };
        let nmembers = end.wrapping_sub(start);
        if nmembers == 0 || nmembers > MULTIXACT_MEMBERS_SANITY_CAP {
            return MultiXactUpdater::Unresolvable;
        }
        for i in 0..nmembers {
            let off = start.wrapping_add(i);
            match self.member_at(off) {
                MemberRead::Member { xid: 0, .. } if off == 0 => {}
                MemberRead::Member { xid: 0, .. } | MemberRead::Unwritten => {
                    return MultiXactUpdater::Covered;
                }
                MemberRead::Member { xid, status } => {
                    if status > MULTIXACT_STATUS_UPDATE {
                        return MultiXactUpdater::Unresolvable;
                    }
                    if status > MULTIXACT_STATUS_FOR_UPDATE {
                        return MultiXactUpdater::Updater(xid);
                    }
                }
                MemberRead::Truncated => return MultiXactUpdater::Unresolvable,
            }
        }
        MultiXactUpdater::LockOnly
    }
}

/// Commit/abort outcomes harvested from gap-WAL xact records (top xid +
/// subxids), overlaying backup pg_xact.
#[derive(Debug, Default)]
pub struct PgXactPatch {
    committed: HashSet<u32>,
    aborted: HashSet<u32>,
}

impl PgXactPatch {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn commit(&mut self, xid: u32, subxids: &[u32]) {
        self.committed.insert(xid);
        self.committed.extend(subxids);
    }

    pub fn abort(&mut self, xid: u32, subxids: &[u32]) {
        self.aborted.insert(xid);
        self.aborted.extend(subxids);
    }

    pub fn len(&self) -> usize {
        self.committed.len() + self.aborted.len()
    }

    pub fn is_empty(&self) -> bool {
        self.committed.is_empty() && self.aborted.is_empty()
    }
}

/// Patch-over-accum xid resolution, plus optional pg_multixact for
/// `HEAP_XMAX_IS_MULTI` xmax.
pub struct PgXactView<'a> {
    accum: &'a PgXactAccum,
    patch: &'a PgXactPatch,
    multi: Option<&'a PgMultiXactAccum>,
}

impl<'a> PgXactView<'a> {
    pub fn new(accum: &'a PgXactAccum, patch: &'a PgXactPatch) -> Self {
        Self {
            accum,
            patch,
            multi: None,
        }
    }

    pub fn with_multixact(mut self, multi: &'a PgMultiXactAccum) -> Self {
        self.multi = Some(multi);
        self
    }

    pub fn xid_status(&self, xid: u32) -> XidStatus {
        if xid > 0 && xid < FIRST_NORMAL_XID {
            // Bootstrap / frozen xids are permanently committed
            return XidStatus::Committed;
        }
        if self.patch.committed.contains(&xid) {
            return XidStatus::Committed;
        }
        if self.patch.aborted.contains(&xid) {
            return XidStatus::Aborted;
        }
        self.accum.status(xid)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Visibility {
    Emit,
    Skip,
    /// Hint bits alone can't decide; re-check with complete [`PgXactView`]
    /// once the walk (and any gap pre-scan) finished
    Defer,
    /// Backup snapshot can't decide a multixact xmax: emitting risks
    /// resurrecting a pre-coverage dead version, skipping risks dropping a
    /// live row — caller aborts the pass
    Unresolvable,
}

/// Gate one on-page tuple. `pg_xact: None` is the streaming pass: hint bits
/// only, undecidable tuples (including every non-lock-only multixact xmax)
/// defer; `Some` is the post-walk resolution and never defers, though a
/// multixact the snapshot can't decide surfaces as
/// [`Visibility::Unresolvable`].
///
/// `Unknown` resolves optimistically for `xmin` (truncated pg_xact ⇒ ancient ⇒
/// committed) and pessimistically for `xmax` (an ancient committed deleter ⇒
/// dead): both directions keep dead tuples dead. A multixact updater xid
/// gets the same xmax pessimism: freeze clears aborted updaters, so a
/// referenced multi outliving pg_xact truncation implies its updater
/// committed.
pub fn tuple_visibility(
    xmin: u32,
    xmax: u32,
    infomask: u16,
    pg_xact: Option<&PgXactView>,
) -> Visibility {
    let frozen = xmin_frozen(infomask) || (xmin > 0 && xmin < FIRST_NORMAL_XID);
    if !frozen {
        if infomask & HEAP_XMIN_INVALID != 0 {
            return Visibility::Skip;
        }
        if infomask & HEAP_XMIN_COMMITTED == 0 {
            match pg_xact {
                None => return Visibility::Defer,
                Some(v) => match v.xid_status(xmin) {
                    XidStatus::Committed | XidStatus::Unknown => {}
                    XidStatus::Aborted | XidStatus::InProgress => return Visibility::Skip,
                },
            }
        }
    }
    if xmax == 0 || infomask & HEAP_XMAX_INVALID != 0 || xmax_locked_only(infomask) {
        return Visibility::Emit;
    }
    if infomask & HEAP_XMAX_IS_MULTI != 0 {
        let Some(v) = pg_xact else {
            return Visibility::Defer;
        };
        let Some(multi) = v.multi else {
            return Visibility::Unresolvable;
        };
        return match multi.updater(xmax) {
            MultiXactUpdater::Covered | MultiXactUpdater::LockOnly => Visibility::Emit,
            MultiXactUpdater::Updater(x) => match v.xid_status(x) {
                XidStatus::Committed | XidStatus::Unknown => Visibility::Skip,
                XidStatus::Aborted | XidStatus::InProgress => Visibility::Emit,
            },
            MultiXactUpdater::Unresolvable => Visibility::Unresolvable,
        };
    }
    if infomask & HEAP_XMAX_COMMITTED != 0 {
        return Visibility::Skip;
    }
    match pg_xact {
        None => Visibility::Defer,
        Some(v) => match v.xid_status(xmax) {
            XidStatus::Committed | XidStatus::Unknown => Visibility::Skip,
            XidStatus::Aborted | XidStatus::InProgress => Visibility::Emit,
        },
    }
}

/// Parse a `pg_xact/<hex>` cluster-relative path into its segment number.
pub fn pg_xact_segno_from_path(path: &std::path::Path) -> Option<u32> {
    let mut comps = path.components();
    let dir = comps.next()?;
    if dir.as_os_str() != "pg_xact" {
        return None;
    }
    let file = comps.next()?.as_os_str().to_str()?;
    if comps.next().is_some() {
        return None;
    }
    u32::from_str_radix(file, 16).ok()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MultiXactSegment {
    Offsets(u32),
    Members(u32),
}

/// Parse a `pg_multixact/{offsets,members}/<hex>` cluster-relative path.
pub fn pg_multixact_segno_from_path(path: &std::path::Path) -> Option<MultiXactSegment> {
    let mut comps = path.components();
    if comps.next()?.as_os_str() != "pg_multixact" {
        return None;
    }
    let dir = comps.next()?;
    let file = comps.next()?.as_os_str().to_str()?;
    if comps.next().is_some() {
        return None;
    }
    let segno = u32::from_str_radix(file, 16).ok()?;
    match dir.as_os_str().to_str()? {
        "offsets" => Some(MultiXactSegment::Offsets(segno)),
        "members" => Some(MultiXactSegment::Members(segno)),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    fn accum_with(segno: u32, statuses: &[(u32, u8)]) -> PgXactAccum {
        let mut bytes = vec![0u8; 8192];
        for (xid, status) in statuses {
            let byte = ((xid % PG_XACT_XIDS_PER_SEGMENT) / PG_XACT_XIDS_PER_BYTE) as usize;
            let shift = (xid % PG_XACT_XIDS_PER_BYTE) * 2;
            bytes[byte] |= status << shift;
        }
        let mut a = PgXactAccum::new();
        a.insert_segment(segno, bytes);
        a
    }

    #[test]
    fn pg_xact_status_reads_two_bit_entries() {
        let a = accum_with(
            0,
            &[
                (100, TRANSACTION_STATUS_COMMITTED),
                (101, TRANSACTION_STATUS_ABORTED),
                (102, TRANSACTION_STATUS_SUB_COMMITTED),
            ],
        );
        assert_eq!(a.status(100), XidStatus::Committed);
        assert_eq!(a.status(101), XidStatus::Aborted);
        assert_eq!(
            a.status(102),
            XidStatus::InProgress,
            "sub-committed defers to patch"
        );
        assert_eq!(a.status(103), XidStatus::InProgress);
        // Beyond the written tail of the newest segment
        assert_eq!(a.status(8192 * 4 + 1), XidStatus::InProgress);
        // Beyond the newest collected segment entirely
        assert_eq!(
            a.status(PG_XACT_XIDS_PER_SEGMENT + 5),
            XidStatus::InProgress
        );
    }

    #[test]
    fn pg_xact_missing_older_segment_is_unknown() {
        let a = accum_with(
            3,
            &[(
                3 * PG_XACT_XIDS_PER_SEGMENT + 9,
                TRANSACTION_STATUS_COMMITTED,
            )],
        );
        assert_eq!(a.status(5), XidStatus::Unknown, "truncated ancient segment");
        assert_eq!(
            a.status(4 * PG_XACT_XIDS_PER_SEGMENT),
            XidStatus::InProgress,
            "newer than every collected segment"
        );
    }

    #[test]
    fn patch_overlays_accum() {
        let a = accum_with(0, &[]);
        let mut p = PgXactPatch::new();
        p.commit(100, &[101, 102]);
        p.abort(200, &[]);
        let v = PgXactView::new(&a, &p);
        assert_eq!(v.xid_status(100), XidStatus::Committed);
        assert_eq!(v.xid_status(102), XidStatus::Committed, "subxid patched");
        assert_eq!(v.xid_status(200), XidStatus::Aborted);
        assert_eq!(v.xid_status(300), XidStatus::InProgress);
        assert_eq!(v.xid_status(1), XidStatus::Committed, "bootstrap xid");
        assert_eq!(v.xid_status(2), XidStatus::Committed, "frozen xid");
    }

    #[test]
    fn hint_bits_short_circuit() {
        // Committed xmin, invalid xmax: emit without pg_xact
        assert_eq!(
            tuple_visibility(100, 0, HEAP_XMIN_COMMITTED | HEAP_XMAX_INVALID, None),
            Visibility::Emit
        );
        // Aborted xmin
        assert_eq!(
            tuple_visibility(100, 0, HEAP_XMIN_INVALID, None),
            Visibility::Skip
        );
        // Frozen (both bits) is committed, not invalid
        assert_eq!(
            tuple_visibility(100, 0, HEAP_XMIN_FROZEN | HEAP_XMAX_INVALID, None),
            Visibility::Emit
        );
        // Committed deleter
        assert_eq!(
            tuple_visibility(100, 200, HEAP_XMIN_COMMITTED | HEAP_XMAX_COMMITTED, None),
            Visibility::Skip
        );
        // Locker-only xmax is not a delete
        assert_eq!(
            tuple_visibility(
                100,
                200,
                HEAP_XMIN_COMMITTED | HEAP_XMAX_LOCK_ONLY | HEAP_XMAX_EXCL_LOCK,
                None
            ),
            Visibility::Emit
        );
        // Multixact xmax: hint bits can't name the updater, defer
        assert_eq!(
            tuple_visibility(100, 200, HEAP_XMIN_COMMITTED | HEAP_XMAX_IS_MULTI, None),
            Visibility::Defer
        );
        // Unhinted xmin defers; unhinted xmax defers
        assert_eq!(tuple_visibility(100, 0, 0, None), Visibility::Defer);
        assert_eq!(
            tuple_visibility(100, 200, HEAP_XMIN_COMMITTED, None),
            Visibility::Defer
        );
    }

    #[test]
    fn pg_xact_resolution_never_defers() {
        let a = accum_with(
            0,
            &[
                (100, TRANSACTION_STATUS_COMMITTED),
                (101, TRANSACTION_STATUS_ABORTED),
                (200, TRANSACTION_STATUS_COMMITTED),
            ],
        );
        let p = PgXactPatch::new();
        let v = PgXactView::new(&a, &p);
        // xmin committed via pg_xact, no xmax
        assert_eq!(tuple_visibility(100, 0, 0, Some(&v)), Visibility::Emit);
        // xmin aborted via pg_xact
        assert_eq!(tuple_visibility(101, 0, 0, Some(&v)), Visibility::Skip);
        // xmin in-progress: WAL leg re-delivers
        assert_eq!(tuple_visibility(150, 0, 0, Some(&v)), Visibility::Skip);
        // deleter committed via pg_xact
        assert_eq!(tuple_visibility(100, 200, 0, Some(&v)), Visibility::Skip);
        // deleter in-flight: tuple stays visible
        assert_eq!(tuple_visibility(100, 150, 0, Some(&v)), Visibility::Emit);
    }

    #[test]
    fn gap_patch_rescues_tuples_in_flight_across_redo() {
        // Xact 500 in flight at backup: pg_xact says in-progress, gap replay
        // saw its commit. Tuple must emit (its pre-redo rows aren't replayed).
        let a = accum_with(0, &[]);
        let mut p = PgXactPatch::new();
        p.commit(500, &[]);
        let v = PgXactView::new(&a, &p);
        assert_eq!(tuple_visibility(500, 0, 0, Some(&v)), Visibility::Emit);
        // Same for a gap-committed deleter: tuple is dead
        assert_eq!(
            tuple_visibility(100, 500, HEAP_XMIN_COMMITTED, Some(&v)),
            Visibility::Skip
        );
    }

    /// Offsets entries and members laid out per multixact.c geometry into
    /// segment 0 (mxids < 65536, member offsets < 52352).
    fn mx_accum(offsets: &[(u32, u32)], members: &[(u32, u32, u8)]) -> PgMultiXactAccum {
        let mut off = vec![0u8; 8192];
        for (mxid, v) in offsets {
            let byte = ((mxid % MXOFF_PER_SEGMENT) * 4) as usize;
            off[byte..byte + 4].copy_from_slice(&v.to_le_bytes());
        }
        let mut mem = vec![0u8; 8192];
        for (o, xid, status) in members {
            let idx = o % MULTIXACT_MEMBERS_PER_PAGE;
            let member = (idx % MULTIXACT_MEMBERS_PER_GROUP) as usize;
            let base = (idx / MULTIXACT_MEMBERS_PER_GROUP) as usize * MULTIXACT_GROUP_SIZE;
            mem[base + member] = *status;
            mem[base + 4 + member * 4..base + 8 + member * 4].copy_from_slice(&xid.to_le_bytes());
        }
        let mut m = PgMultiXactAccum::new();
        m.insert_offsets_segment(0, off);
        m.insert_members_segment(0, mem);
        m
    }

    #[test]
    fn multixact_updater_resolves_members() {
        let m = mx_accum(
            &[(10, 100), (11, 103), (20, 103), (21, 105), (30, 200)],
            &[
                // mxid 10: keyshare locker, NoKeyUpdate updater, share locker
                (100, 900, 0),
                (101, 901, 4),
                (102, 902, 1),
                // mxid 20: lockers only
                (103, 910, 0),
                (104, 911, 3),
            ],
        );
        assert_eq!(m.updater(10), MultiXactUpdater::Updater(901));
        assert_eq!(m.updater(20), MultiXactUpdater::LockOnly);
        // mxid 30: offsets[31] unwritten ⇒ created mid-copy ⇒ covered
        assert_eq!(m.updater(30), MultiXactUpdater::Covered);
        // mxid 40: offsets entry zero ⇒ unwritten at copy
        assert_eq!(m.updater(40), MultiXactUpdater::Covered);
        // Next segment never copied ⇒ allocated post-copy
        assert_eq!(m.updater(MXOFF_PER_SEGMENT + 5), MultiXactUpdater::Covered);
    }

    #[test]
    fn multixact_updater_edge_reads() {
        // Member xid zero mid-range: members page copied before the write
        let m = mx_accum(&[(50, 300), (51, 302)], &[(300, 950, 0)]);
        assert_eq!(m.updater(50), MultiXactUpdater::Covered);
        // Garbage status byte
        let m = mx_accum(&[(10, 100), (11, 101)], &[(100, 900, 9)]);
        assert_eq!(m.updater(10), MultiXactUpdater::Unresolvable);
        // Zero-width range
        let m = mx_accum(&[(10, 100), (11, 100)], &[]);
        assert_eq!(m.updater(10), MultiXactUpdater::Unresolvable);
        // Truncated below the collected range
        let mut m = PgMultiXactAccum::new();
        m.insert_offsets_segment(3, vec![0u8; 8192]);
        assert_eq!(m.updater(5), MultiXactUpdater::Unresolvable);
        // Members segment truncated while offsets resolve
        let mut m = mx_accum(&[(10, 100), (11, 101)], &[]);
        m.members = HashMap::from([(2, vec![0u8; 8192])]);
        assert_eq!(m.updater(10), MultiXactUpdater::Unresolvable);
    }

    #[test]
    fn multixact_xmax_gates_through_pg_xact() {
        let a = accum_with(
            0,
            &[
                (901, TRANSACTION_STATUS_COMMITTED),
                (911, TRANSACTION_STATUS_ABORTED),
            ],
        );
        let p = PgXactPatch::new();
        let m = mx_accum(
            &[(10, 100), (11, 102), (20, 102), (21, 104), (30, 200)],
            &[
                (100, 900, 0),
                (101, 901, 5), // committed deleter
                (102, 910, 0),
                (103, 911, 4), // aborted updater
            ],
        );
        let v = PgXactView::new(&a, &p).with_multixact(&m);
        let mask = HEAP_XMIN_COMMITTED | HEAP_XMAX_IS_MULTI;
        // Committed delete member: dead, and its commit may predate WAL
        // coverage — must not resurrect
        assert_eq!(tuple_visibility(100, 10, mask, Some(&v)), Visibility::Skip);
        // Aborted updater: tuple lives
        assert_eq!(tuple_visibility(100, 20, mask, Some(&v)), Visibility::Emit);
        // Covered (post-copy) multi: WAL leg re-delivers the update
        assert_eq!(tuple_visibility(100, 30, mask, Some(&v)), Visibility::Emit);
        // In-progress updater: WAL leg owns the update, tuple emits
        let m2 = mx_accum(&[(10, 100), (11, 101)], &[(100, 950, 4)]);
        let v2 = PgXactView::new(&a, &p).with_multixact(&m2);
        assert_eq!(tuple_visibility(100, 10, mask, Some(&v2)), Visibility::Emit);
        // Gap-patch-committed updater: dead
        let mut p3 = PgXactPatch::new();
        p3.commit(950, &[]);
        let v3 = PgXactView::new(&a, &p3).with_multixact(&m2);
        assert_eq!(tuple_visibility(100, 10, mask, Some(&v3)), Visibility::Skip);
        // View without pg_multixact: unresolvable, caller aborts
        let v4 = PgXactView::new(&a, &p);
        assert_eq!(
            tuple_visibility(100, 10, mask, Some(&v4)),
            Visibility::Unresolvable
        );
    }

    #[test]
    fn pg_multixact_segno_parses_paths() {
        use std::path::Path;
        assert_eq!(
            pg_multixact_segno_from_path(Path::new("pg_multixact/offsets/0000")),
            Some(MultiXactSegment::Offsets(0))
        );
        assert_eq!(
            pg_multixact_segno_from_path(Path::new("pg_multixact/members/00A3")),
            Some(MultiXactSegment::Members(0xA3))
        );
        assert_eq!(
            pg_multixact_segno_from_path(Path::new("pg_multixact/0000")),
            None
        );
        assert_eq!(
            pg_multixact_segno_from_path(Path::new("pg_xact/0000")),
            None
        );
    }

    #[test]
    fn pg_xact_segno_parses_pg_xact_paths() {
        assert_eq!(pg_xact_segno_from_path(Path::new("pg_xact/0000")), Some(0));
        assert_eq!(
            pg_xact_segno_from_path(Path::new("pg_xact/00A3")),
            Some(0xA3)
        );
        assert_eq!(pg_xact_segno_from_path(Path::new("pg_xact")), None);
        assert_eq!(pg_xact_segno_from_path(Path::new("base/5/16384")), None);
        assert_eq!(
            pg_xact_segno_from_path(Path::new("pg_xact/nested/0000")),
            None
        );
    }
}
