//! Rolling samples of the daemon's cumulative `rows_synced` counter. The shim
//! keeps no history of its own and the control socket exposes only a live
//! aggregate, so PeerDB's sync-history graph is synthesized by sampling that
//! counter on a timer and serving per-bucket deltas.

use std::collections::{BTreeMap, VecDeque};
use std::sync::Mutex;

#[derive(Clone, Copy)]
struct Sample {
    unix_secs: i64,
    rows: i64,
}

pub struct StatsHistory {
    samples: Mutex<VecDeque<Sample>>,
    cap: usize,
}

impl Default for StatsHistory {
    fn default() -> Self {
        // 15s cadence * 5760 ~= 24h of retained history
        Self {
            samples: Mutex::new(VecDeque::new()),
            cap: 5760,
        }
    }
}

impl StatsHistory {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record one cumulative-counter reading. A drop (the daemon restart resets
    /// the counter) clears history so bucket deltas never go negative.
    pub fn record(&self, unix_secs: i64, rows: i64) {
        let mut s = self.samples.lock().unwrap();
        match s.back() {
            Some(last) if rows < last.rows => s.clear(),
            Some(last) if last.unix_secs == unix_secs => {
                s.pop_back();
            }
            _ => {}
        }
        s.push_back(Sample { unix_secs, rows });
        while s.len() > self.cap {
            s.pop_front();
        }
    }

    /// Rows synced per `bucket_secs`-wide interval as `(bucket_start_ms, rows)`
    /// points, oldest first. Each consecutive-sample delta lands in the bucket
    /// its endpoint falls into.
    pub fn graph(&self, bucket_secs: i64) -> Vec<(f64, f64)> {
        if bucket_secs <= 0 {
            return Vec::new();
        }
        let s = self.samples.lock().unwrap();
        let mut buckets: BTreeMap<i64, i64> = BTreeMap::new();
        let mut prev: Option<Sample> = None;
        for &cur in s.iter() {
            if let Some(p) = prev {
                let delta = (cur.rows - p.rows).max(0);
                let bucket = (cur.unix_secs / bucket_secs) * bucket_secs;
                *buckets.entry(bucket).or_default() += delta;
            }
            prev = Some(cur);
        }
        buckets
            .into_iter()
            .map(|(bucket, rows)| ((bucket * 1000) as f64, rows as f64))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn buckets_deltas_and_resets() {
        let h = StatsHistory::new();
        // three 5-min buckets (bucket_secs=300)
        h.record(0, 0);
        h.record(150, 100); // bucket 0: +100
        h.record(300, 250); // bucket 300: +150
        h.record(450, 250); // bucket 300: +0
        let g = h.graph(300);
        assert_eq!(g, vec![(0.0, 100.0), (300_000.0, 150.0)]);

        // counter reset drops history; a lone sample yields no deltas
        h.record(600, 5);
        assert!(h.graph(300).is_empty());
        h.record(660, 55);
        assert_eq!(h.graph(300), vec![(600_000.0, 50.0)]);
    }
}
