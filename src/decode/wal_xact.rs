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
    /// `xl_xact_dbinfo.dbId`: database of the committing backend. `None`
    /// when the record carries no dbinfo, which proves nothing about scope
    pub(crate) db_id: Option<u32>,
}

/// Commit payload is descriptor-capture input: a silent partial parse could
/// drop the inval that marks a boundary, so malformation poisons the stream
/// instead
#[derive(Debug, thiserror::Error)]
pub enum XactPayloadError {
    #[error("xact payload: {0}")]
    Malformed(String),
    /// Commit names a database that cannot have written the catalog dirt
    /// held for its tree: scope contradiction, fail closed rather than
    /// publish a boundary built from another database's OIDs
    #[error("commit dbinfo db {db_id} contradicts target db {target} catalog dirt")]
    ForeignScope { db_id: u32, target: u32 },
}

impl XactPayloadError {
    pub(crate) fn new(what: impl Into<String>) -> Self {
        Self::Malformed(what.into())
    }
}

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
    let count = take_count(data).ok_or_else(|| XactPayloadError::new("inval count"))?;
    let ns_ids = namespace_catcache_ids(page_magic);
    for _ in 0..count {
        // SharedInvalidationMessage: 16-byte union, id i8 at 0, dbId at 4;
        // relcache relId / catalog catId at 8 (PG
        // src/include/storage/sinval.h, layout identical on majors 16-18).
        // Ids: >= 0 catcache (id = syscache id, payload is a hash — only
        // "which catalog" is recoverable), -1 catalog, -2 relcache, -3
        // smgr, -4 relmap, -5 snapshot, -6 relsync (PG 18; skipping costs
        // nothing on older majors where it cannot occur)
        let msg: [u8; 16] = take(data).ok_or_else(|| XactPayloadError::new("inval msg"))?;
        let db_id = u32::from_le_bytes(msg[4..8].try_into().unwrap());
        let arg = u32::from_le_bytes(msg[8..12].try_into().unwrap());
        match msg[0] as i8 {
            -2 => out.relcache.push(RelcacheInval { db_id, rel_id: arg }),
            -1 if arg == PG_NAMESPACE_OID => out.namespace.mark(db_id),
            id if ns_ids.contains(&id) => out.namespace.mark(db_id),
            -6..=-1 => {}
            id if id >= 0 => {}
            id => return Err(XactPayloadError::new(format!("unknown sinval id {id}"))),
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
    let mut out = XactCommitPayload {
        xact_time: take_i64(&mut data).ok_or_else(|| XactPayloadError::new("xact_time"))?,
        ..Default::default()
    };
    let xinfo = if info & XLOG_XACT_HAS_INFO != 0 {
        take_u32(&mut data).ok_or_else(|| XactPayloadError::new("xinfo"))?
    } else {
        0
    };
    // `xl_xact_dbinfo { Oid dbId; Oid tsId }`: database of the committing
    // backend, the scope any catalog dirt this tree holds must match
    if xinfo & XACT_XINFO_HAS_DBINFO != 0 {
        let db_id = take_u32(&mut data).ok_or_else(|| XactPayloadError::new("dbinfo"))?;
        if take_u32(&mut data).is_none() {
            return Err(XactPayloadError::new("dbinfo"));
        }
        out.db_id = Some(db_id);
    }
    if xinfo & XACT_XINFO_HAS_SUBXACTS != 0 {
        let count = take_count(&mut data).ok_or_else(|| XactPayloadError::new("subxact count"))?;
        let mut subs = Vec::with_capacity(count);
        for _ in 0..count {
            subs.push(take_u32(&mut data).ok_or_else(|| XactPayloadError::new("subxact"))?);
        }
        out.subxacts = subs;
    }
    if !skip_counted(&mut data, xinfo, XACT_XINFO_HAS_RELFILELOCATORS, 12) {
        return Err(XactPayloadError::new("relfilelocators"));
    }
    if !skip_counted(
        &mut data,
        xinfo,
        XACT_XINFO_HAS_DROPPED_STATS,
        stats_item_width(page_magic),
    ) {
        return Err(XactPayloadError::new("dropped stats"));
    }
    if xinfo & XACT_XINFO_HAS_INVALS != 0 {
        take_invals(&mut data, page_magic, &mut out.invals)?;
    }
    if xinfo & XACT_XINFO_HAS_TWOPHASE != 0 {
        out.twophase_xid =
            Some(take_u32(&mut data).ok_or_else(|| XactPayloadError::new("twophase xid"))?);
        if xinfo & XACT_XINFO_HAS_GID != 0 {
            let end = data
                .iter()
                .position(|byte| *byte == 0)
                .ok_or_else(|| XactPayloadError::new("gid terminator"))?;
            data = &data[end + 1..];
        }
    }
    if xinfo & XACT_XINFO_HAS_ORIGIN != 0 && data.len() < 16 {
        return Err(XactPayloadError::new("origin"));
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

#[cfg(test)]
mod tests {
    use super::*;

    const PG17_MAGIC: u16 = 0xD116;
    const PG18_MAGIC: u16 = 0xD118;

    /// `xl_xact_commit` tail in PG source order: dbinfo, subxacts, dropped
    /// stats, invals. Sections present iff their `xinfo` bit is set, so a
    /// misparsed dbinfo shifts everything after it
    fn commit_payload(
        db_id: Option<u32>,
        subxacts: &[u32],
        dropped_stats: &[u8],
        invals: &[(i8, u32, u32)],
        stats_width: usize,
    ) -> Vec<u8> {
        let mut md: Vec<u8> = 0i64.to_le_bytes().to_vec();
        let mut xinfo = 0u32;
        if db_id.is_some() {
            xinfo |= XACT_XINFO_HAS_DBINFO;
        }
        if !subxacts.is_empty() {
            xinfo |= XACT_XINFO_HAS_SUBXACTS;
        }
        if !dropped_stats.is_empty() {
            xinfo |= XACT_XINFO_HAS_DROPPED_STATS;
        }
        if !invals.is_empty() {
            xinfo |= XACT_XINFO_HAS_INVALS;
        }
        md.extend_from_slice(&xinfo.to_le_bytes());
        if let Some(db) = db_id {
            md.extend_from_slice(&db.to_le_bytes());
            md.extend_from_slice(&1663u32.to_le_bytes()); // tsId
        }
        if !subxacts.is_empty() {
            md.extend_from_slice(&(subxacts.len() as i32).to_le_bytes());
            for x in subxacts {
                md.extend_from_slice(&x.to_le_bytes());
            }
        }
        if !dropped_stats.is_empty() {
            md.extend_from_slice(&((dropped_stats.len() / stats_width) as i32).to_le_bytes());
            md.extend_from_slice(dropped_stats);
        }
        if !invals.is_empty() {
            md.extend_from_slice(&(invals.len() as i32).to_le_bytes());
            for &(id, db, arg) in invals {
                let mut msg = [0u8; 16];
                msg[0] = id as u8;
                msg[4..8].copy_from_slice(&db.to_le_bytes());
                msg[8..12].copy_from_slice(&arg.to_le_bytes());
                md.extend_from_slice(&msg);
            }
        }
        md
    }

    #[test]
    fn dbinfo_yields_db_id_and_keeps_tail_aligned() {
        let md = commit_payload(Some(5), &[101, 102], &[], &[(-2, 5, 16400)], 12);
        let p = parse_xact_payload(XLOG_XACT_HAS_INFO, &md, PG17_MAGIC).expect("parse");
        assert_eq!(p.db_id, Some(5));
        assert_eq!(p.subxacts, vec![101, 102]);
        assert_eq!(
            p.invals.relcache,
            vec![RelcacheInval {
                db_id: 5,
                rel_id: 16400
            }]
        );
    }

    #[test]
    fn absent_dbinfo_is_unknown_scope() {
        let md = commit_payload(None, &[101], &[], &[], 12);
        let p = parse_xact_payload(XLOG_XACT_HAS_INFO, &md, PG17_MAGIC).expect("parse");
        assert_eq!(p.db_id, None);
        assert_eq!(p.subxacts, vec![101]);
        // No xinfo at all: same unknown scope
        let md = 0i64.to_le_bytes().to_vec();
        assert_eq!(parse_xact_payload(0, &md, PG17_MAGIC).unwrap().db_id, None);
    }

    #[test]
    fn truncated_dbinfo_poisons() {
        let full = commit_payload(Some(5), &[], &[], &[], 12);
        for cut in [4, 8, 12, 15] {
            let mut md = full.clone();
            md.truncate(cut);
            assert!(
                parse_xact_payload(XLOG_XACT_HAS_INFO, &md, PG17_MAGIC).is_err(),
                "truncated to {cut} bytes must poison"
            );
        }
    }

    /// `xl_xact_stats_item` widens on PG 18; both tails must still land on
    /// the inval section after dbinfo
    #[test]
    fn dbinfo_then_dropped_stats_parses_on_both_majors() {
        for (magic, width) in [(PG17_MAGIC, 12), (PG18_MAGIC, 16)] {
            let md = commit_payload(
                Some(7),
                &[],
                &vec![0u8; width * 2],
                &[(-2, 7, 16500)],
                width,
            );
            let p = parse_xact_payload(XLOG_XACT_HAS_INFO, &md, magic).expect("parse");
            assert_eq!(p.db_id, Some(7));
            assert_eq!(p.invals.relcache.len(), 1, "magic {magic:#X}");
            assert_eq!(p.invals.relcache[0].rel_id, 16500);
        }
    }
}
