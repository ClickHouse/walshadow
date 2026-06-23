//! Process-global tracing knobs, set once at startup.

use std::sync::atomic::{AtomicU64, Ordering};

// f64 bits so it can be a plain static (0x3FF... == 1.0).
static SAMPLE_RATIO: AtomicU64 = AtomicU64::new(0x3FF0_0000_0000_0000);
static COUNTER: AtomicU64 = AtomicU64::new(0);

pub fn set_sample_ratio(ratio: f64) {
    SAMPLE_RATIO.store(ratio.clamp(0.0, 1.0).to_bits(), Ordering::Relaxed);
}

/// One head-sampling decision; called once per txn (verdict then cached), so a
/// 1-in-N stride suffices.
pub fn should_sample() -> bool {
    let ratio = f64::from_bits(SAMPLE_RATIO.load(Ordering::Relaxed));
    if ratio >= 1.0 {
        return true;
    }
    if ratio <= 0.0 {
        return false;
    }
    let stride = ((1.0 / ratio).round() as u64).max(1);
    COUNTER
        .fetch_add(1, Ordering::Relaxed)
        .is_multiple_of(stride)
}

#[cfg(test)]
mod tests {
    use super::*;

    // Serialized: the ratio is a process global.
    static GUARD: std::sync::Mutex<()> = std::sync::Mutex::new(());

    #[test]
    fn ratio_endpoints_and_distribution() {
        let _g = GUARD.lock().unwrap();

        set_sample_ratio(1.0);
        assert!((0..1000).all(|_| should_sample()));

        set_sample_ratio(0.0);
        assert!(!(0..1000).any(|_| should_sample()));

        set_sample_ratio(0.1);
        let hits = (0..10_000).filter(|_| should_sample()).count();
        assert_eq!(hits, 1000);

        set_sample_ratio(5.0); // clamps to always
        assert!(should_sample());
        set_sample_ratio(1.0);
    }
}
