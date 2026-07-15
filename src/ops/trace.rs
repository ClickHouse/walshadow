//! Process-global tracing knobs, set once at startup.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

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

struct TxnSpans {
    txn: Option<tracing::Span>,
    first_lsn: u64,
    read_at: Instant,
    shipped_at: Option<Instant>,
    decode_parent: Option<tracing::Span>,
    sampled: bool,
}

#[derive(Clone, Default)]
pub struct TxnSpanRegistry {
    inner: Arc<std::sync::Mutex<HashMap<u32, TxnSpans>>>,
}

impl TxnSpanRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn is_sampled(&self, xid: u32) -> bool {
        self.inner
            .lock()
            .expect("txn span registry poisoned")
            .get(&xid)
            .is_some_and(|entry| entry.sampled)
    }

    pub fn open(&self, xid: u32, first_lsn: u64) {
        if xid == 0 {
            return;
        }
        self.inner
            .lock()
            .expect("txn span registry poisoned")
            .entry(xid)
            .or_insert_with(|| TxnSpans {
                txn: None,
                first_lsn,
                read_at: Instant::now(),
                shipped_at: None,
                decode_parent: None,
                sampled: should_sample(),
            });
    }

    pub fn note_shipped(&self, xid: u32) {
        if xid == 0 {
            return;
        }
        let mut spans = self.inner.lock().expect("txn span registry poisoned");
        if let Some(entry) = spans.get_mut(&xid)
            && entry.shipped_at.is_none()
        {
            entry.shipped_at = Some(Instant::now());
        }
    }

    pub fn note_popped(&self, xid: u32) -> bool {
        if xid == 0 {
            return false;
        }
        let mut spans = self.inner.lock().expect("txn span registry poisoned");
        let Some(entry) = spans.get_mut(&xid) else {
            return false;
        };
        if !entry.sampled || entry.txn.is_some() {
            return entry.sampled;
        }
        let Some(shipped) = entry.shipped_at else {
            return true;
        };
        let txn = new_txn_span(xid, entry.first_lsn);
        txn.record(
            "fill_ms",
            shipped.duration_since(entry.read_at).as_secs_f64() * 1e3,
        );
        txn.record("queue_ms", shipped.elapsed().as_secs_f64() * 1e3);
        entry.decode_parent = Some(txn.clone());
        entry.txn = Some(txn);
        true
    }

    pub fn txn_span(&self, xid: u32) -> Option<tracing::Span> {
        self.inner
            .lock()
            .expect("txn span registry poisoned")
            .get(&xid)
            .and_then(|entry| entry.txn.clone())
    }

    pub fn adopt(&self, xid: u32) -> Option<tracing::Span> {
        let mut spans = self.inner.lock().expect("txn span registry poisoned");
        let entry = spans.get_mut(&xid)?;
        entry.decode_parent = None;
        entry.txn.clone()
    }

    pub fn decode_parent(&self, xid: u32) -> Option<tracing::Span> {
        self.inner
            .lock()
            .expect("txn span registry poisoned")
            .get(&xid)?
            .decode_parent
            .clone()
    }

    pub fn prune(&self, xids: &[u32]) {
        let mut spans = self.inner.lock().expect("txn span registry poisoned");
        for xid in xids {
            spans.remove(xid);
        }
    }
}

pub(crate) fn new_txn_span(xid: u32, first_lsn: u64) -> tracing::Span {
    tracing::info_span!(
        target: "walshadow::trace",
        parent: None,
        "txn",
        xid,
        first_lsn,
        fill_ms = tracing::field::Empty,
        queue_ms = tracing::field::Empty,
        top_xid = tracing::field::Empty,
        rows = tracing::field::Empty,
        spilled = tracing::field::Empty,
        outcome = tracing::field::Empty,
    )
}

#[derive(Debug, Clone)]
pub struct InflightSnapshotEntry {
    pub xid: u32,
    pub first_lsn: u64,
    pub last_lsn: u64,
    pub heap_count: u64,
    pub chunk_count: u64,
    pub in_mem_bytes: u64,
    pub spilled: bool,
    pub catalog_events: u64,
    pub rels: String,
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
