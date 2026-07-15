//! Transaction WAL record parsing

pub(crate) const XLOG_XACT_OPMASK: u8 = 0x70;
pub(crate) const XLOG_XACT_COMMIT: u8 = 0x00;
pub(crate) const XLOG_XACT_ABORT: u8 = 0x20;
pub(crate) const XLOG_XACT_COMMIT_PREPARED: u8 = 0x30;
pub(crate) const XLOG_XACT_ABORT_PREPARED: u8 = 0x40;
pub(crate) const XLOG_XACT_ASSIGNMENT: u8 = 0x50;
pub(crate) const XLOG_XACT_HAS_INFO: u8 = 0x80;

pub(crate) const XACT_XINFO_HAS_DBINFO: u32 = 1 << 0;
pub(crate) const XACT_XINFO_HAS_SUBXACTS: u32 = 1 << 1;
const XACT_XINFO_HAS_RELFILELOCATORS: u32 = 1 << 2;
pub(crate) const XACT_XINFO_HAS_INVALS: u32 = 1 << 3;
pub(crate) const XACT_XINFO_HAS_TWOPHASE: u32 = 1 << 4;
const XACT_XINFO_HAS_ORIGIN: u32 = 1 << 5;
pub(crate) const XACT_XINFO_HAS_GID: u32 = 1 << 7;
const XACT_XINFO_HAS_DROPPED_STATS: u32 = 1 << 8;

#[derive(Debug, Default)]
pub(crate) struct XactCommitPayload {
    pub(crate) xact_time: i64,
    pub(crate) subxacts: Vec<u32>,
    pub(crate) twophase_xid: Option<u32>,
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

pub(crate) fn parse_xact_payload(info: u8, mut data: &[u8]) -> XactCommitPayload {
    let mut out = XactCommitPayload::default();
    let Some(time) = take_i64(&mut data) else {
        return out;
    };
    out.xact_time = time;
    let xinfo = if info & XLOG_XACT_HAS_INFO != 0 {
        let Some(value) = take_u32(&mut data) else {
            return out;
        };
        value
    } else {
        0
    };
    if xinfo & XACT_XINFO_HAS_DBINFO != 0 && !skip(&mut data, 8) {
        return out;
    }
    if xinfo & XACT_XINFO_HAS_SUBXACTS != 0 {
        let Some(count) = take_count(&mut data) else {
            return out;
        };
        let mut subs = Vec::with_capacity(count);
        for _ in 0..count {
            let Some(xid) = take_u32(&mut data) else {
                return out;
            };
            subs.push(xid);
        }
        out.subxacts = subs;
    }
    if !skip_counted(&mut data, xinfo, XACT_XINFO_HAS_RELFILELOCATORS, 12)
        || !skip_counted(&mut data, xinfo, XACT_XINFO_HAS_DROPPED_STATS, 16)
        || !skip_counted(&mut data, xinfo, XACT_XINFO_HAS_INVALS, 16)
    {
        return out;
    }
    if xinfo & XACT_XINFO_HAS_TWOPHASE != 0 {
        let Some(xid) = take_u32(&mut data) else {
            return out;
        };
        out.twophase_xid = Some(xid);
        if xinfo & XACT_XINFO_HAS_GID != 0 {
            let Some(end) = data.iter().position(|byte| *byte == 0) else {
                return out;
            };
            data = &data[end + 1..];
        }
    }
    if xinfo & XACT_XINFO_HAS_ORIGIN != 0 && data.len() < 16 {
        return out;
    }
    out
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
