//! Hold source vacuum horizon below values shadow may still be asked for
//!
//! walshadow reports this xmin to source as hot-standby feedback. Source then
//! keeps every row version deleted by a transaction at or after it, so shadow
//! never replays reclamation of TOAST chunks that pending rows still reference
//!
//! `oldestRunningXid` of a running-xacts record bounds xid of every transaction
//! writing after that record. So does a snapshot xmin taken before reading
//! source insert position. Pin newest sample at or below resolved floor:
//! restart rereads from floor, so values deleted later must stay

use std::collections::BTreeMap;

/// Samples above floor. Sidecar samples arrive about every second, so this
/// spans most of a day of lag. Eviction drops oldest, leaving pin older
const SAMPLE_CAP: usize = 1 << 16;

#[derive(Debug)]
pub struct HorizonPin {
    /// Full xid near newest WAL read, gives 32-bit WAL xids their epoch
    reference: u64,
    /// LSN to full xid bounding every transaction writing at or after it
    samples: BTreeMap<u64, u64>,
    pin: u64,
}

impl HorizonPin {
    /// `next` is source's next full xid, `oldest` its oldest running one.
    /// `saved` is xmin an earlier run held for floor this one resumes from,
    /// source may not have vacuumed past it yet. Slot xmin only moves
    /// forward, so hold it when set
    pub fn seed(next: u64, oldest: u64, slot_xmin: Option<u32>, saved: Option<u64>) -> Self {
        let mut pin = Self {
            reference: next,
            samples: BTreeMap::new(),
            pin: saved.map_or(oldest, |s| s.min(oldest)),
        };
        if let Some(xmin) = slot_xmin {
            pin.pin = pin.widen(xmin);
            if saved.is_some_and(|s| s < pin.pin) {
                tracing::warn!(
                    target: "walshadow::toast",
                    slot_xmin = pin.pin,
                    saved_xmin = saved,
                    "source slot xmin passed saved horizon pin, pending rows may read reclaimed values",
                );
            }
        }
        pin
    }

    /// Walsender ignores feedback whose epoch does not match its own
    fn widen(&self, xid: u32) -> u64 {
        let delta = xid.wrapping_sub(self.reference as u32) as i32;
        self.reference.wrapping_add_signed(i64::from(delta))
    }

    /// Running-xacts record at `lsn`
    pub fn observe(&mut self, lsn: u64, next_xid: u32, oldest_xid: u32) {
        self.reference = self.widen(next_xid);
        self.sample(lsn, self.widen(oldest_xid));
    }

    /// Full `xid` below every transaction writing at or after `lsn`
    pub fn sample(&mut self, lsn: u64, xid: u64) {
        let held = self.samples.entry(lsn).or_insert(xid);
        *held = (*held).max(xid);
        if self.samples.len() > SAMPLE_CAP {
            self.samples.pop_first();
        }
    }

    /// Drop samples above `lsn`. A new source connection may follow another
    /// timeline past WAL consumed so far
    pub fn forget_above(&mut self, lsn: u64) {
        self.samples.retain(|at, _| *at <= lsn);
    }

    /// Full xid to hold once `floor` is resolved. Never moves backward
    pub fn advance(&mut self, floor: u64) -> u64 {
        while let Some(e) = self.samples.first_entry()
            && *e.key() <= floor
        {
            self.pin = self.pin.max(e.remove());
        }
        self.pin
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const EPOCH: u64 = 3 << 32;

    #[test]
    fn pin_holds_seed_until_floor_passes_a_sample() {
        let mut p = HorizonPin::seed(EPOCH + 900, EPOCH + 500, None, None);
        p.observe(0x200, 950, 700);
        p.observe(0x300, 990, 800);
        assert_eq!(p.advance(0x1ff), EPOCH + 500);
        assert_eq!(p.advance(0x2ff), EPOCH + 700);
        assert_eq!(p.advance(0x300), EPOCH + 800);
        assert_eq!(p.advance(u64::MAX), EPOCH + 800);
    }

    #[test]
    fn slot_xmin_outranks_running_horizon() {
        let p = HorizonPin::seed(EPOCH + 900, EPOCH + 850, Some(400), None);
        assert_eq!(p.pin, EPOCH + 400);
        let p = HorizonPin::seed(EPOCH + 5, EPOCH + 3, Some(u32::MAX - 9), None);
        assert_eq!(p.pin, EPOCH - 10, "slot xmin from previous epoch");
    }

    #[test]
    fn saved_xmin_holds_below_running_horizon() {
        let p = HorizonPin::seed(EPOCH + 900, EPOCH + 850, None, Some(EPOCH + 300));
        assert_eq!(p.pin, EPOCH + 300);
        let p = HorizonPin::seed(EPOCH + 900, EPOCH + 850, None, Some(EPOCH + 870));
        assert_eq!(p.pin, EPOCH + 850, "saved xmin from a rewound source");
        let p = HorizonPin::seed(EPOCH + 900, EPOCH + 850, Some(400), Some(EPOCH + 300));
        assert_eq!(p.pin, EPOCH + 400, "slot xmin cannot move back");
        let p = HorizonPin::seed(EPOCH + 900, EPOCH + 850, Some(400), Some(EPOCH + 500));
        assert_eq!(p.pin, EPOCH + 400);
    }

    #[test]
    fn new_connection_forgets_samples_ahead_of_consumed_wal() {
        let mut p = HorizonPin::seed(EPOCH + 900, EPOCH + 500, None, None);
        p.sample(0x200, EPOCH + 600);
        p.sample(0x300, EPOCH + 990);
        p.forget_above(0x200);
        assert_eq!(p.advance(u64::MAX), EPOCH + 600);
        p.sample(0x300, EPOCH + 700);
        p.forget_above(u64::MAX);
        assert_eq!(p.advance(0x300), EPOCH + 700);
    }

    #[test]
    fn samples_follow_epoch_through_wraparound() {
        let mut p = HorizonPin::seed(EPOCH - 100, EPOCH - 200, None, None);
        p.observe(0x100, 50, u32::MAX - 49);
        p.observe(0x200, 150, 20);
        assert_eq!(p.advance(0x100), EPOCH - 50);
        assert_eq!(p.advance(0x200), EPOCH + 20);
    }

    #[test]
    fn regressing_sample_keeps_pin() {
        let mut p = HorizonPin::seed(EPOCH + 900, EPOCH + 600, None, None);
        p.observe(0x100, 900, 500);
        assert_eq!(p.advance(0x100), EPOCH + 600);
    }

    #[test]
    fn full_buffer_evicts_oldest_sample() {
        let mut p = HorizonPin::seed(EPOCH + 10, EPOCH + 1, None, None);
        for i in 0..=SAMPLE_CAP as u64 {
            p.observe(i, 10, 2 + i as u32);
        }
        assert_eq!(p.samples.len(), SAMPLE_CAP);
        assert_eq!(p.samples.first_key_value(), Some((&1, &(EPOCH + 3))));
    }

    #[test]
    fn sidecar_samples_ahead_of_wal_wait_for_floor() {
        let mut p = HorizonPin::seed(EPOCH + 900, EPOCH + 500, None, None);
        p.sample(0x900, EPOCH + 880);
        p.observe(0x200, 950, 700);
        p.sample(0x200, EPOCH + 650);
        assert_eq!(p.advance(0x200), EPOCH + 700, "same LSN keeps higher bound");
        assert_eq!(p.advance(0x8ff), EPOCH + 700);
        assert_eq!(p.advance(0x900), EPOCH + 880);
    }
}
