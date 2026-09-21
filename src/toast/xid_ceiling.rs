//! Sample xid ceilings at WAL positions to detect reused TOAST value IDs
//!
//! PostgreSQL reuses an ID only after all its chunks are gone
//! (`GetNewOidWithIndex` uses `SnapshotAny`, PostgreSQL
//! `src/backend/catalog/catalog.c`). Replacement chunks postdate referring
//! record. Compare their newest `xmin` with xid ceiling from `at`
//!
//! Round lookups up to a later sample. A higher ceiling can miss reuse but
//! does not reject original values. Evicting old samples has same effect

use std::collections::VecDeque;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

/// WAL bytes between samples. A reuse escapes the check only when delete,
/// reclamation and reinsert all land within one interval of the referring
/// record
const SAMPLE_BYTES: u64 = 1 << 20;

/// Retained samples, oldest evicted first. 16 GiB of lag at [`SAMPLE_BYTES`]
const SAMPLE_CAP: usize = 16 * 1024;

/// True when `a` is later than `b` in xid order, PG `TransactionIdFollows`
pub fn follows(a: u32, b: u32) -> bool {
    (a.wrapping_sub(b) as i32) > 0
}

/// `(lsn, highest xid seen at or before it)` samples fed by the pump and
/// read by the shadow TOAST store.
#[derive(Debug, Default)]
pub struct XidCeiling {
    /// Written only by the pump, read only when a sample is taken
    max_xid: AtomicU32,
    next_sample_lsn: AtomicU64,
    samples: Mutex<VecDeque<(u64, u32)>>,
}

impl XidCeiling {
    /// Fold one record in. Single writer, ordinary loads and stores
    pub fn observe(&self, lsn: u64, xid: u32) {
        let have = self.max_xid.load(Ordering::Relaxed);
        if xid != 0 && (have == 0 || follows(xid, have)) {
            self.max_xid.store(xid, Ordering::Relaxed);
        }
        if lsn < self.next_sample_lsn.load(Ordering::Relaxed) {
            return;
        }
        self.next_sample_lsn
            .store(lsn + SAMPLE_BYTES, Ordering::Relaxed);
        let max_xid = self.max_xid.load(Ordering::Relaxed);
        if max_xid == 0 {
            return;
        }
        let mut samples = self.samples.lock().expect("xid ceiling poisoned");
        samples.push_back((lsn, max_xid));
        while samples.len() > SAMPLE_CAP {
            samples.pop_front();
        }
    }

    /// Return earliest sample at or after `lsn`
    /// Return zero (`InvalidTransactionId`) if no sample is available,
    /// skipping generation check
    pub fn at(&self, lsn: u64) -> u32 {
        let samples = self.samples.lock().expect("xid ceiling poisoned");
        let at = samples.partition_point(|&(sample_lsn, _)| sample_lsn < lsn);
        samples.get(at).map_or(0, |&(_, xid)| xid)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ceiling_covers_every_xid_written_at_or_before_the_lookup() {
        let c = XidCeiling::default();
        c.observe(0, 100);
        c.observe(SAMPLE_BYTES / 2, 140);
        c.observe(SAMPLE_BYTES, 150);
        c.observe(SAMPLE_BYTES + 8, 190);
        c.observe(2 * SAMPLE_BYTES, 200);

        assert_eq!(c.at(0), 100, "sample at the lookup wins");
        assert_eq!(
            c.at(1),
            150,
            "round up to include xid 140 from a later record",
        );
        assert_eq!(c.at(2 * SAMPLE_BYTES), 200);
        assert_eq!(c.at(2 * SAMPLE_BYTES + 1), 0, "not sampled that far");
    }

    #[test]
    fn xid_zero_and_regression_leave_the_ceiling_alone() {
        let c = XidCeiling::default();
        c.observe(0, 0);
        assert_eq!(c.at(0), 0, "records without an xid start no sample");
        c.observe(SAMPLE_BYTES, 700);
        c.observe(2 * SAMPLE_BYTES, 500);
        assert_eq!(c.at(0), 700);
        assert_eq!(c.at(2 * SAMPLE_BYTES), 700, "out-of-order xid");
    }

    #[test]
    fn wraparound_keeps_the_later_xid() {
        let c = XidCeiling::default();
        c.observe(0, u32::MAX - 4);
        c.observe(SAMPLE_BYTES, 6);
        assert_eq!(c.at(SAMPLE_BYTES), 6, "6 follows u32::MAX - 4");
    }

    #[test]
    fn oldest_samples_evict() {
        let c = XidCeiling::default();
        for i in 0..SAMPLE_CAP as u64 + 10 {
            c.observe(i * SAMPLE_BYTES, 100 + i as u32);
        }
        assert_eq!(c.samples.lock().unwrap().len(), SAMPLE_CAP);
        assert_eq!(
            c.at(0),
            110,
            "evicted lookups round up to the oldest kept sample",
        );
    }
}
