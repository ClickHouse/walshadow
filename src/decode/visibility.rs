//! Resolve backup tuple visibility from hints and transaction history
//!
//! Emit committed inserts with absent or aborted deleters. Defer in-progress
//! transactions to [pending tables](crate::backfill::visibility_pending): row
//! WAL can predate backup redo, so replay cannot reconstruct discarded tuples
//!
//! Overlay backup `pg_xact` ([`PgXactAccum`]) with WAL commit/abort outcomes
//! ([`PgXactPatch`]). Resolve multixact updaters through backup `pg_multixact`
//! ([`PgMultiXactAccum`]), then consult same transaction view
//!
//! Multixacts newer than copied SLRU bounds belong to covered WAL; retain old
//! tuple version. Unbounded or corrupt multixacts yield [`Visibility::Unresolvable`],
//! which ends the pass
//! See architecture/bootstrap.md

use std::path::{Path, PathBuf};

use ahash::HashMap;
use anyhow::{Context, Result};
use roaring::RoaringBitmap;

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
// offsets hold MultiXactOffset entries, 4 bytes through PG 18 and 8 from PG
// 19 (PG src/include/c.h); members pack groups of 4 flag bytes + 4 xids
// (20 bytes), 409 groups per page with 12 pad bytes at each page end
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
    /// No pg_xact coverage: xid predates oldest collected segment. Reading
    /// that as ancient needs the tuple's own page, since vacuum freezes
    /// surviving tuples and removes aborted ones before the horizon moves
    /// past them
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
    Val(u64),
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
#[derive(Debug)]
pub struct PgMultiXactAccum {
    offsets: HashMap<u32, Vec<u8>>,
    members: HashMap<u32, Vec<u8>>,
    /// `sizeof(MultiXactOffset)` on the source
    offset_bytes: u32,
}

impl PgMultiXactAccum {
    pub fn new(source_major: u32) -> Self {
        Self {
            offsets: HashMap::default(),
            members: HashMap::default(),
            offset_bytes: if source_major >= 19 { 8 } else { 4 },
        }
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

    /// Move segments out, leaving an empty accum of the same layout
    pub fn take(&mut self) -> Self {
        Self {
            offsets: std::mem::take(&mut self.offsets),
            members: std::mem::take(&mut self.members),
            offset_bytes: self.offset_bytes,
        }
    }

    fn offsets_per_segment(&self) -> u32 {
        (8192 / self.offset_bytes) * SLRU_PAGES_PER_SEGMENT
    }

    /// Member offsets wrap at the offsets entry width
    fn wrap_offset(&self, off: u64) -> u64 {
        if self.offset_bytes == 4 {
            return off & u64::from(u32::MAX);
        }
        off
    }

    fn read_offset(&self, segno: u32, byte: usize) -> SlruRead {
        let Some(seg) = self.offsets.get(&segno) else {
            if self.offsets.keys().any(|s| *s < segno) {
                return SlruRead::Unwritten;
            }
            return SlruRead::Truncated;
        };
        let width = self.offset_bytes as usize;
        let Some(b) = seg.get(byte..byte + width) else {
            return SlruRead::Unwritten;
        };
        let mut entry = [0u8; 8];
        entry[..width].copy_from_slice(b);
        SlruRead::Val(u64::from_le_bytes(entry))
    }

    /// Offsets entry for `mxid`. Zero is reserved to mean unset
    /// (`GetNewMultiXactId` skips it), so it reads as unwritten-at-copy.
    fn offset_at(&self, mxid: u32) -> SlruRead {
        let per_segment = self.offsets_per_segment();
        let byte = ((mxid % per_segment) * self.offset_bytes) as usize;
        match self.read_offset(mxid / per_segment, byte) {
            SlruRead::Val(0) => SlruRead::Unwritten,
            r => r,
        }
    }

    fn member_at(&self, off: u64) -> MemberRead {
        let page = off / u64::from(MULTIXACT_MEMBERS_PER_PAGE);
        // Segment keys parse as u32, so a wider segno resolves through repair
        let Ok(segno) = u32::try_from(page / u64::from(SLRU_PAGES_PER_SEGMENT)) else {
            return MemberRead::Truncated;
        };
        let Some(seg) = self.members.get(&segno) else {
            if self.members.keys().any(|s| *s < segno) {
                return MemberRead::Unwritten;
            }
            return MemberRead::Truncated;
        };
        let idx = (off % u64::from(MULTIXACT_MEMBERS_PER_PAGE)) as u32;
        let member = (idx % MULTIXACT_MEMBERS_PER_GROUP) as usize;
        let base = (page % u64::from(SLRU_PAGES_PER_SEGMENT)) as usize * 8192
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
        let nmembers = self.wrap_offset(end.wrapping_sub(start));
        if nmembers == 0 || nmembers > u64::from(MULTIXACT_MEMBERS_SANITY_CAP) {
            return MultiXactUpdater::Unresolvable;
        }
        for i in 0..nmembers {
            let off = self.wrap_offset(start.wrapping_add(i));
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
///
/// RoaringBitmaps as a long bootstrap holds every xid source assigned,
/// and those are a near-dense ascending range.
#[derive(Debug, Default)]
pub struct PgXactPatch {
    committed: RoaringBitmap,
    aborted: RoaringBitmap,
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

    /// Run-length encode the ascending stretches once harvesting is done
    pub fn seal(&mut self) {
        self.committed.optimize();
        self.aborted.optimize();
    }

    pub fn len(&self) -> usize {
        self.committed.len() as usize + self.aborted.len() as usize
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
        if self.patch.committed.contains(xid) {
            return XidStatus::Committed;
        }
        if self.patch.aborted.contains(xid) {
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

/// One side of a tuple's verdict. `Pending` names the xid whose outcome
/// decides it: the insert side needs a commit, the delete side an abort
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Side {
    /// This side keeps the tuple alive
    Live,
    /// This side kills it
    Dead,
    Pending(u32),
    /// Hint bits alone can't decide and no view was supplied
    Unknown,
    /// Multixact the backup snapshot can't bound
    Unresolvable,
}

/// Insert side: did the tuple's writer commit?
fn insert_side(xmin: u32, infomask: u16, pg_xact: Option<&PgXactView>) -> Side {
    if xmin_frozen(infomask) || (xmin > 0 && xmin < FIRST_NORMAL_XID) {
        return Side::Live;
    }
    if infomask & HEAP_XMIN_INVALID != 0 {
        return Side::Dead;
    }
    if infomask & HEAP_XMIN_COMMITTED != 0 {
        return Side::Live;
    }
    let Some(v) = pg_xact else {
        return Side::Unknown;
    };
    match v.xid_status(xmin) {
        XidStatus::Committed | XidStatus::Unknown => Side::Live,
        XidStatus::Aborted => Side::Dead,
        XidStatus::InProgress => Side::Pending(xmin),
    }
}

/// Delete side: did a deleter or multixact updater commit?
fn delete_side(xmax: u32, infomask: u16, pg_xact: Option<&PgXactView>) -> Side {
    if xmax == 0 || infomask & HEAP_XMAX_INVALID != 0 || xmax_locked_only(infomask) {
        return Side::Live;
    }
    if infomask & HEAP_XMAX_IS_MULTI != 0 {
        let Some(v) = pg_xact else {
            return Side::Unknown;
        };
        let Some(multi) = v.multi else {
            return Side::Unresolvable;
        };
        return match multi.updater(xmax) {
            MultiXactUpdater::Covered | MultiXactUpdater::LockOnly => Side::Live,
            MultiXactUpdater::Updater(x) => xid_delete_side(v, x),
            MultiXactUpdater::Unresolvable => Side::Unresolvable,
        };
    }
    if infomask & HEAP_XMAX_COMMITTED != 0 {
        return Side::Dead;
    }
    let Some(v) = pg_xact else {
        return Side::Unknown;
    };
    xid_delete_side(v, xmax)
}

fn xid_delete_side(v: &PgXactView, xid: u32) -> Side {
    match v.xid_status(xid) {
        XidStatus::Committed | XidStatus::Unknown => Side::Dead,
        XidStatus::Aborted => Side::Live,
        XidStatus::InProgress => Side::Pending(xid),
    }
}

/// Resolve on-page visibility from hints and optional transaction history
///
/// Vacuum freezes survivors and removes aborted tuples before truncating
/// `pg_xact`, allowing ancient xmin to count as committed and xmax as dead
/// Pending copies lack this evidence and must retain unknown outcomes
pub fn tuple_visibility(
    xmin: u32,
    xmax: u32,
    infomask: u16,
    pg_xact: Option<&PgXactView>,
) -> Visibility {
    let insert = insert_side(xmin, infomask, pg_xact);
    if insert == Side::Dead {
        return Visibility::Skip;
    }
    if insert == Side::Unknown {
        return Visibility::Defer;
    }
    match delete_side(xmax, infomask, pg_xact) {
        Side::Dead => Visibility::Skip,
        Side::Unresolvable => Visibility::Unresolvable,
        Side::Unknown | Side::Pending(_) => Visibility::Defer,
        Side::Live if matches!(insert, Side::Pending(_)) => Visibility::Defer,
        Side::Live => Visibility::Emit,
    }
}

/// Require insert commit and delete abort; zero marks an already settled side
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PendingXids {
    pub insert: u32,
    pub delete: u32,
}

impl PendingXids {
    pub fn is_empty(&self) -> bool {
        self.insert == 0 && self.delete == 0
    }
}

/// Resolve deciding xids, reducing multixact xmax to its updater
pub fn deferred_xids(xmin: u32, xmax: u32, infomask: u16, view: &PgXactView) -> PendingXids {
    let pending = |side| match side {
        Side::Pending(x) => x,
        _ => 0,
    };
    PendingXids {
        insert: pending(insert_side(xmin, infomask, Some(view))),
        delete: pending(delete_side(xmax, infomask, Some(view))),
    }
}

/// Parse a `pg_xact/<hex>` cluster-relative path into its segment number.
pub fn pg_xact_segno_from_path(path: &Path) -> Option<u32> {
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
pub fn pg_multixact_segno_from_path(path: &Path) -> Option<MultiXactSegment> {
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

/// Read landed `pg_xact` segments off a cluster data dir
pub async fn read_pg_xact(data_dir: &Path) -> Result<PgXactAccum> {
    let mut accum = PgXactAccum::new();
    for (rel, bytes) in read_slru_dir(data_dir, Path::new("pg_xact")).await? {
        if let Some(segno) = pg_xact_segno_from_path(&rel) {
            accum.insert_segment(segno, bytes);
        }
    }
    Ok(accum)
}

/// Read landed `pg_multixact` offsets and members segments
pub async fn read_pg_multixact(data_dir: &Path, source_major: u32) -> Result<PgMultiXactAccum> {
    let mut accum = PgMultiXactAccum::new(source_major);
    for sub in ["offsets", "members"] {
        let dir = Path::new("pg_multixact").join(sub);
        for (rel, bytes) in read_slru_dir(data_dir, &dir).await? {
            match pg_multixact_segno_from_path(&rel) {
                Some(MultiXactSegment::Offsets(s)) => accum.insert_offsets_segment(s, bytes),
                Some(MultiXactSegment::Members(s)) => accum.insert_members_segment(s, bytes),
                None => {}
            }
        }
    }
    Ok(accum)
}

/// Read files under a cluster-relative SLRU directory, keyed by that path.
/// A directory the cluster never made reads as empty
async fn read_slru_dir(data_dir: &Path, rel_dir: &Path) -> Result<Vec<(PathBuf, Vec<u8>)>> {
    let dir = data_dir.join(rel_dir);
    let mut entries = match tokio::fs::read_dir(&dir).await {
        Ok(e) => e,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(e).with_context(|| format!("slru: read {}", dir.display())),
    };
    let mut out = Vec::new();
    while let Some(entry) = entries
        .next_entry()
        .await
        .with_context(|| format!("slru: scan {}", dir.display()))?
    {
        let path = entry.path();
        if !entry
            .file_type()
            .await
            .with_context(|| format!("slru: stat {}", path.display()))?
            .is_file()
        {
            continue;
        }
        let bytes = tokio::fs::read(&path)
            .await
            .with_context(|| format!("slru: read {}", path.display()))?;
        out.push((rel_dir.join(entry.file_name()), bytes));
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

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

    /// In-flight writers and deleters defer even against a complete view:
    /// their rows predate WAL coverage, so retain them until their outcomes arrive
    #[test]
    fn pg_xact_resolution_defers_only_in_flight_xacts() {
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
        assert_eq!(tuple_visibility(150, 0, 0, Some(&v)), Visibility::Defer);
        assert_eq!(
            deferred_xids(150, 0, 0, &v),
            PendingXids {
                insert: 150,
                delete: 0
            }
        );
        // deleter committed via pg_xact
        assert_eq!(tuple_visibility(100, 200, 0, Some(&v)), Visibility::Skip);
        assert_eq!(tuple_visibility(100, 150, 0, Some(&v)), Visibility::Defer);
        assert_eq!(
            deferred_xids(100, 150, 0, &v),
            PendingXids {
                insert: 0,
                delete: 150
            }
        );
        // Same xact inserted and deleted: neither outcome can free the row
        assert_eq!(
            deferred_xids(150, 150, 0, &v),
            PendingXids {
                insert: 150,
                delete: 150
            }
        );
        // A settled tuple has no deciding xid left
        assert!(deferred_xids(100, 0, 0, &v).is_empty());
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
        mx_accum_major(17, offsets, members)
    }

    /// `major` selects the offsets entry width. Entries stay under 2^32, so
    /// the wide layout writes the same bytes with a zero high half
    fn mx_accum_major(
        major: u32,
        offsets: &[(u32, u32)],
        members: &[(u32, u32, u8)],
    ) -> PgMultiXactAccum {
        let mut m = PgMultiXactAccum::new(major);
        let mut off = vec![0u8; 8192];
        for (mxid, v) in offsets {
            let byte = ((mxid % m.offsets_per_segment()) * m.offset_bytes) as usize;
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
        assert_eq!(
            m.updater(m.offsets_per_segment() + 5),
            MultiXactUpdater::Covered
        );
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
        let mut m = PgMultiXactAccum::new(17);
        m.insert_offsets_segment(3, vec![0u8; 8192]);
        assert_eq!(m.updater(5), MultiXactUpdater::Unresolvable);
        // Members segment truncated while offsets resolve
        let mut m = mx_accum(&[(10, 100), (11, 101)], &[]);
        m.members = HashMap::from_iter([(2, vec![0u8; 8192])]);
        assert_eq!(m.updater(10), MultiXactUpdater::Unresolvable);
    }

    /// PG 19 widened MultiXactOffset, so reading its offsets at the PG 18
    /// stride lands on an unwritten neighbour and resurrects a dead version
    #[test]
    fn pg19_offsets_resolve_at_their_own_stride() {
        let offsets = &[(10, 100), (11, 101)];
        let members = &[(100, 900, 4)];
        let wide = mx_accum_major(19, offsets, members);
        assert_eq!(wide.updater(10), MultiXactUpdater::Updater(900));
        let mut narrow = PgMultiXactAccum::new(18);
        narrow.insert_offsets_segment(0, wide.offsets[&0].clone());
        narrow.insert_members_segment(0, wide.members[&0].clone());
        assert_eq!(narrow.updater(10), MultiXactUpdater::Covered);
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
        // Retain the row until its updater aborts, keyed on the
        // resolved member xid rather than the multi
        let m2 = mx_accum(&[(10, 100), (11, 101)], &[(100, 950, 4)]);
        let v2 = PgXactView::new(&a, &p).with_multixact(&m2);
        assert_eq!(
            tuple_visibility(100, 10, mask, Some(&v2)),
            Visibility::Defer
        );
        assert_eq!(
            deferred_xids(100, 10, mask, &v2),
            PendingXids {
                insert: 0,
                delete: 950
            }
        );
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

    /// pg_xact segment bytes with `xid` marked committed
    fn pg_xact_segment(xid: u32) -> Vec<u8> {
        let mut bytes = vec![0u8; 8192];
        bytes[(xid / 4) as usize] |= 0x01 << ((xid % 4) * 2);
        bytes
    }

    #[tokio::test]
    async fn slru_reads_come_off_the_landed_data_dir() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();
        tokio::fs::create_dir_all(dir.join("pg_xact"))
            .await
            .unwrap();
        tokio::fs::write(dir.join("pg_xact").join("0000"), pg_xact_segment(700))
            .await
            .unwrap();
        // Segment 1 proves the hex filename is the segment number, not an index
        tokio::fs::write(dir.join("pg_xact").join("0001"), pg_xact_segment(3))
            .await
            .unwrap();

        let accum = read_pg_xact(dir).await.unwrap();
        // No pg_multixact/ at all: a cluster that never made one
        let multi = read_pg_multixact(dir, 17).await.unwrap();
        let patch = PgXactPatch::new();
        let view = PgXactView::new(&accum, &patch).with_multixact(&multi);

        assert_eq!(view.xid_status(700), XidStatus::Committed);
        assert_eq!(
            view.xid_status(PG_XACT_XIDS_PER_SEGMENT + 3),
            XidStatus::Committed,
        );
        assert_eq!(view.xid_status(701), XidStatus::InProgress);
    }
}
