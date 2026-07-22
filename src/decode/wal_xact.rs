//! Transaction WAL record parsing

use crate::filter::catalog_tracker::PG_NAMESPACE_OID;

pub(crate) const XLOG_XACT_OPMASK: u8 = 0x70;
pub(crate) const XLOG_XACT_COMMIT: u8 = 0x00;
pub(crate) const XLOG_XACT_ABORT: u8 = 0x20;
pub(crate) const XLOG_XACT_COMMIT_PREPARED: u8 = 0x30;
pub(crate) const XLOG_XACT_ABORT_PREPARED: u8 = 0x40;
pub(crate) const XLOG_XACT_ASSIGNMENT: u8 = 0x50;
pub(crate) const XLOG_XACT_INVALIDATIONS: u8 = 0x60;
pub(crate) const XLOG_XACT_HAS_INFO: u8 = 0x80;

pub(crate) const XACT_XINFO_HAS_DBINFO: u32 = 1 << 0;
pub(crate) const XACT_XINFO_HAS_SUBXACTS: u32 = 1 << 1;
const XACT_XINFO_HAS_RELFILELOCATORS: u32 = 1 << 2;
pub(crate) const XACT_XINFO_HAS_INVALS: u32 = 1 << 3;
pub(crate) const XACT_XINFO_HAS_TWOPHASE: u32 = 1 << 4;
const XACT_XINFO_HAS_ORIGIN: u32 = 1 << 5;
pub(crate) const XACT_XINFO_HAS_GID: u32 = 1 << 7;
const XACT_XINFO_HAS_DROPPED_STATS: u32 = 1 << 8;

/// `SharedInvalRelcacheMsg`: relation whose relcache the committing xact
/// invalidated. `rel_id == 0` = whole-relcache flush.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RelcacheInval {
    pub(crate) db_id: u32,
    pub(crate) rel_id: u32,
}

/// Classified `SharedInvalidationMessage` set. Relcache messages enumerate
/// affected rels; pg_namespace catcache / whole-catalog messages mark
/// namespace-text changes relcache invals never enumerate (capture-all
/// trigger)
#[derive(Debug, Default)]
pub(crate) struct InvalSet {
    pub(crate) relcache: Vec<RelcacheInval>,
    /// db scope of pg_namespace syscache / whole-catalog invals
    pub(crate) namespace: NamespaceInval,
}

/// One backend writes one db, so scope is at most one oid plus db 0
/// (shared-catalog messages target every db)
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) enum NamespaceInval {
    #[default]
    Empty,
    Shared,
    Unshared(u32),
    Both(u32),
}

impl NamespaceInval {
    fn mark(&mut self, db_id: u32) {
        *self = match (*self, db_id) {
            (Self::Empty | Self::Shared, 0) => Self::Shared,
            (Self::Empty, db) => Self::Unshared(db),
            (Self::Shared, db) | (Self::Unshared(db) | Self::Both(db), 0) => Self::Both(db),
            (state @ (Self::Unshared(db) | Self::Both(db)), mark) if mark == db => state,
            // second distinct db can't come from one backend; widen to all-db
            _ => Self::Shared,
        };
    }

    pub(crate) fn hits(self, local: impl Fn(u32) -> bool) -> bool {
        match self {
            Self::Empty => false,
            Self::Shared => local(0),
            Self::Unshared(db) => local(db),
            Self::Both(db) => local(0) || local(db),
        }
    }
}

#[derive(Debug, Default)]
pub(crate) struct XactCommitPayload {
    pub(crate) xact_time: i64,
    pub(crate) subxacts: Vec<u32>,
    pub(crate) twophase_xid: Option<u32>,
    pub(crate) invals: InvalSet,
}

/// Commit payload is descriptor-capture input: a silent partial parse could
/// drop the inval that marks a boundary, so malformation poisons the stream
/// instead
#[derive(Debug, thiserror::Error)]
#[error("xact payload: {0}")]
pub struct XactPayloadError(String);

pub(crate) fn parse_xact_assignment(mut data: &[u8]) -> Option<(u32, Vec<u32>)> {
    let top = take_u32(&mut data)?;
    let count = take_count(&mut data)?;
    let mut subs = Vec::with_capacity(count);
    for _ in 0..count {
        subs.push(take_u32(&mut data)?);
    }
    Some((top, subs))
}

/// `xl_xact_stats_item`: `(int kind, Oid dboid, Oid objoid)` = 12 bytes
/// through PG 17; PG 18 splits objid into two u32 = 16 bytes (PG
/// `src/include/access/xact.h`). Width keyed off the WAL page magic
/// (0xD118 = PG 18).
fn stats_item_width(page_magic: u16) -> usize {
    if page_magic >= 0xD118 { 16 } else { 12 }
}

/// pg_namespace syscache ids (`NAMESPACENAME`, `NAMESPACEOID`).
/// `SysCacheIdentifier` values shift across majors: name-sorted generation
/// (PG `src/backend/catalog/genbki.pl`; stable branches append via
/// Z-prefixed names so ids hold within a major). 35/36 on PG 16-17,
/// 37/38 on PG 18 (EXTENSIONNAME/OID sort ahead)
fn namespace_catcache_ids(page_magic: u16) -> [i8; 2] {
    if page_magic >= 0xD118 {
        [37, 38]
    } else {
        [35, 36]
    }
}

fn take_invals(
    data: &mut &[u8],
    page_magic: u16,
    out: &mut InvalSet,
) -> Result<(), XactPayloadError> {
    let err = |what: &str| XactPayloadError(what.to_string());
    let count = take_count(data).ok_or_else(|| err("inval count"))?;
    let ns_ids = namespace_catcache_ids(page_magic);
    for _ in 0..count {
        // SharedInvalidationMessage: 16-byte union, id i8 at 0, dbId at 4;
        // relcache relId / catalog catId at 8 (PG
        // src/include/storage/sinval.h, layout identical on majors 16-18).
        // Ids: >= 0 catcache (id = syscache id, payload is a hash — only
        // "which catalog" is recoverable), -1 catalog, -2 relcache, -3
        // smgr, -4 relmap, -5 snapshot, -6 relsync (PG 18; skipping costs
        // nothing on older majors where it cannot occur)
        let msg: [u8; 16] = take(data).ok_or_else(|| err("inval msg"))?;
        let db_id = u32::from_le_bytes(msg[4..8].try_into().unwrap());
        let arg = u32::from_le_bytes(msg[8..12].try_into().unwrap());
        match msg[0] as i8 {
            -2 => out.relcache.push(RelcacheInval { db_id, rel_id: arg }),
            -1 if arg == PG_NAMESPACE_OID => out.namespace.mark(db_id),
            id if ns_ids.contains(&id) => out.namespace.mark(db_id),
            -6..=-1 => {}
            id if id >= 0 => {}
            id => return Err(XactPayloadError(format!("unknown sinval id {id}"))),
        }
    }
    Ok(())
}

/// `xl_xact_invals` (`XLOG_XACT_INVALIDATIONS`): command-boundary inval set
/// logged mid-xact at `wal_level=logical` (PG
/// `src/backend/utils/cache/inval.c` `LogLogicalInvalidations`). Lets the
/// filter re-dirty an open xact whose catalog writes precede the restart
/// resume floor
pub(crate) fn parse_xact_invalidations(
    mut data: &[u8],
    page_magic: u16,
) -> Result<InvalSet, XactPayloadError> {
    let mut out = InvalSet::default();
    take_invals(&mut data, page_magic, &mut out)?;
    Ok(out)
}

pub(crate) fn parse_xact_payload(
    info: u8,
    mut data: &[u8],
    page_magic: u16,
) -> Result<XactCommitPayload, XactPayloadError> {
    let err = |what: &str| XactPayloadError(what.to_string());
    let mut out = XactCommitPayload {
        xact_time: take_i64(&mut data).ok_or_else(|| err("xact_time"))?,
        ..Default::default()
    };
    let xinfo = if info & XLOG_XACT_HAS_INFO != 0 {
        take_u32(&mut data).ok_or_else(|| err("xinfo"))?
    } else {
        0
    };
    if xinfo & XACT_XINFO_HAS_DBINFO != 0 && !skip(&mut data, 8) {
        return Err(err("dbinfo"));
    }
    if xinfo & XACT_XINFO_HAS_SUBXACTS != 0 {
        let count = take_count(&mut data).ok_or_else(|| err("subxact count"))?;
        let mut subs = Vec::with_capacity(count);
        for _ in 0..count {
            subs.push(take_u32(&mut data).ok_or_else(|| err("subxact"))?);
        }
        out.subxacts = subs;
    }
    if !skip_counted(&mut data, xinfo, XACT_XINFO_HAS_RELFILELOCATORS, 12) {
        return Err(err("relfilelocators"));
    }
    if !skip_counted(
        &mut data,
        xinfo,
        XACT_XINFO_HAS_DROPPED_STATS,
        stats_item_width(page_magic),
    ) {
        return Err(err("dropped stats"));
    }
    if xinfo & XACT_XINFO_HAS_INVALS != 0 {
        take_invals(&mut data, page_magic, &mut out.invals)?;
    }
    if xinfo & XACT_XINFO_HAS_TWOPHASE != 0 {
        out.twophase_xid = Some(take_u32(&mut data).ok_or_else(|| err("twophase xid"))?);
        if xinfo & XACT_XINFO_HAS_GID != 0 {
            let end = data
                .iter()
                .position(|byte| *byte == 0)
                .ok_or_else(|| err("gid terminator"))?;
            data = &data[end + 1..];
        }
    }
    if xinfo & XACT_XINFO_HAS_ORIGIN != 0 && data.len() < 16 {
        return Err(err("origin"));
    }
    Ok(out)
}

fn take<const N: usize>(data: &mut &[u8]) -> Option<[u8; N]> {
    let (value, rest) = data.split_at_checked(N)?;
    *data = rest;
    value.try_into().ok()
}

fn take_u32(data: &mut &[u8]) -> Option<u32> {
    Some(u32::from_le_bytes(take(data)?))
}

fn take_i64(data: &mut &[u8]) -> Option<i64> {
    Some(i64::from_le_bytes(take(data)?))
}

fn take_count(data: &mut &[u8]) -> Option<usize> {
    usize::try_from(i32::from_le_bytes(take(data)?)).ok()
}

fn skip(data: &mut &[u8], count: usize) -> bool {
    let Some((_, rest)) = data.split_at_checked(count) else {
        return false;
    };
    *data = rest;
    true
}

fn skip_counted(data: &mut &[u8], xinfo: u32, flag: u32, width: usize) -> bool {
    if xinfo & flag == 0 {
        return true;
    }
    take_count(data)
        .and_then(|count| count.checked_mul(width))
        .is_some_and(|bytes| skip(data, bytes))
}
