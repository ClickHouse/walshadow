use crate::config::Detector as Thresholds;
use serde::Serialize;
use std::collections::HashMap;
use std::time::{Duration, Instant};

/// One feature row as returned by sql/collector.sql.
#[derive(Debug, Clone, Serialize)]
pub struct MarketFeatures {
    pub market_id: i32,
    pub last_price_cents: i32,
    pub buy_1s: i64,
    pub sell_1s: i64,
    pub normal_buy_per_s: f64,
    pub baseline_trades: i64,
    pub recent_trades: i64,
    pub buy_multiple: f64,
    pub imbalance: f64,
}

/// One completed collector query. Both consumers evaluate this exact struct;
/// the delayed one just receives it later.
#[derive(Debug, Clone)]
pub struct Frame {
    pub frame_id: u64,
    /// Window end bound handed to ClickHouse — the frame's feature timestamp.
    pub feature_ts: chrono::DateTime<chrono::Utc>,
    /// The same instant on the monotonic clock, captured before the query was
    /// issued. The delay queue anchors to this, so the simulated delay is a
    /// clean interval of *data staleness* and does not silently absorb
    /// replication or query time on top.
    pub feature_at: Instant,
    pub query_completed_at: Instant,
    pub query_elapsed_ms: f64,
    pub markets_evaluated: usize,
    pub read_rows: Option<u64>,
    pub read_bytes: Option<u64>,
    pub rows: Vec<MarketFeatures>,
}

#[derive(Debug, Clone, Serialize)]
pub struct Alert {
    pub market_id: i32,
    pub frame_id: u64,
    pub feature_ts: chrono::DateTime<chrono::Utc>,
    pub buy_multiple: f64,
    pub imbalance: f64,
    pub last_price_cents: i32,
    pub recent_trades: i64,
    pub baseline_trades: i64,
    #[serde(skip)]
    pub emitted_at: Instant,
    /// Wall time only for display; every elapsed number uses `emitted_at`.
    pub emitted_wall: chrono::DateTime<chrono::Utc>,
}

/// The alert rule. One implementation, instantiated twice — the immediate
/// consumer and the delayed consumer share this code and this dedup logic, so
/// the only difference between the two panels is when a frame reaches them.
pub struct Rule {
    thresholds: Thresholds,
    last_alert: HashMap<i32, Instant>,
}

impl Rule {
    pub fn new(thresholds: Thresholds) -> Self {
        Self { thresholds, last_alert: HashMap::new() }
    }

    pub fn reset(&mut self) {
        self.last_alert.clear();
    }

    fn fires(&self, m: &MarketFeatures) -> bool {
        m.baseline_trades >= self.thresholds.baseline_trades_min
            && m.recent_trades >= self.thresholds.recent_trades_min
            && m.buy_multiple > self.thresholds.buy_multiple_min
            && m.imbalance > self.thresholds.imbalance_min
    }

    /// Evaluate a frame against that frame's own feature values. `evaluated_at`
    /// is when this consumer actually looked at it, which is what separates the
    /// live panel from the delayed one.
    pub fn evaluate(&mut self, frame: &Frame, evaluated_at: Instant) -> Vec<Alert> {
        let cooldown = Duration::from_millis(self.thresholds.cooldown_ms);
        let mut out = Vec::new();
        for m in &frame.rows {
            if !self.fires(m) {
                continue;
            }
            if let Some(prev) = self.last_alert.get(&m.market_id)
                && evaluated_at.saturating_duration_since(*prev) < cooldown
            {
                continue;
            }
            self.last_alert.insert(m.market_id, evaluated_at);
            out.push(Alert {
                market_id: m.market_id,
                frame_id: frame.frame_id,
                feature_ts: frame.feature_ts,
                buy_multiple: m.buy_multiple,
                imbalance: m.imbalance,
                last_price_cents: m.last_price_cents,
                recent_trades: m.recent_trades,
                baseline_trades: m.baseline_trades,
                emitted_at: evaluated_at,
                emitted_wall: chrono::Utc::now(),
            });
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn thresholds() -> Thresholds {
        Thresholds {
            baseline_trades_min: 30,
            recent_trades_min: 10,
            buy_multiple_min: 8.0,
            imbalance_min: 0.8,
            cooldown_ms: 15_000,
        }
    }

    fn features(buy_multiple: f64, imbalance: f64) -> MarketFeatures {
        MarketFeatures {
            market_id: 101,
            last_price_cents: 42,
            buy_1s: 1000,
            sell_1s: 10,
            normal_buy_per_s: 10.0,
            baseline_trades: 300,
            recent_trades: 200,
            buy_multiple,
            imbalance,
        }
    }

    fn frame(rows: Vec<MarketFeatures>) -> Frame {
        Frame {
            frame_id: 1,
            feature_ts: chrono::Utc::now(),
            feature_at: Instant::now(),
            query_completed_at: Instant::now(),
            query_elapsed_ms: 1.0,
            markets_evaluated: rows.len(),
            read_rows: None,
            read_bytes: None,
            rows,
        }
    }

    #[test]
    fn quiet_market_does_not_alert() {
        let mut rule = Rule::new(thresholds());
        let f = frame(vec![features(1.2, 0.05)]);
        assert!(rule.evaluate(&f, Instant::now()).is_empty());
    }

    #[test]
    fn surge_alerts() {
        let mut rule = Rule::new(thresholds());
        let f = frame(vec![features(40.0, 0.95)]);
        assert_eq!(rule.evaluate(&f, Instant::now()).len(), 1);
    }

    #[test]
    fn thin_baseline_cannot_alert() {
        let mut rule = Rule::new(thresholds());
        let mut m = features(40.0, 0.95);
        m.baseline_trades = 3;
        assert!(rule.evaluate(&frame(vec![m]), Instant::now()).is_empty());
    }

    #[test]
    fn cooldown_suppresses_repeat() {
        let mut rule = Rule::new(thresholds());
        let now = Instant::now();
        assert_eq!(rule.evaluate(&frame(vec![features(40.0, 0.95)]), now).len(), 1);
        assert!(
            rule.evaluate(&frame(vec![features(40.0, 0.95)]), now + Duration::from_secs(1))
                .is_empty()
        );
        assert_eq!(
            rule.evaluate(&frame(vec![features(40.0, 0.95)]), now + Duration::from_secs(20))
                .len(),
            1
        );
    }

    /// The whole comparison rests on this: the same frame, evaluated at two
    /// different times by two independent rule instances, yields the same
    /// verdict from the same numbers.
    #[test]
    fn identical_frame_yields_identical_verdict_at_any_time() {
        let f = frame(vec![features(40.0, 0.95)]);
        let mut live = Rule::new(thresholds());
        let mut delayed = Rule::new(thresholds());
        let t0 = Instant::now();
        let a = live.evaluate(&f, t0);
        let b = delayed.evaluate(&f, t0 + Duration::from_secs(5));
        assert_eq!(a.len(), b.len());
        assert_eq!(a[0].frame_id, b[0].frame_id);
        assert_eq!(a[0].buy_multiple, b[0].buy_multiple);
        assert_eq!(a[0].feature_ts, b[0].feature_ts);
    }
}
