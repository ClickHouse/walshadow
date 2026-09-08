use chrono::{DateTime, Utc};
use std::time::{Duration, Instant};

/// Durations come from `Instant`; wall-clock labels come from `Utc::now()`.
///
/// Those must not be mixed. `Instant` is CLOCK_MONOTONIC, which stops advancing
/// while the machine is suspended, so reconstructing a wall time from a
/// process-start origin drifts without bound — and any timestamp that has to
/// line up with rows written by PostgreSQL (the feature-window bound above all)
/// would then address a window in the past and match nothing.
///
/// `wall` anchors to the present instead, so its error is only whatever drift
/// occurs across the short gap between `at` and now.
#[derive(Debug, Clone, Copy, Default)]
pub struct Clock;

impl Clock {
    pub fn new() -> Self {
        Self
    }

    pub fn wall(&self, at: Instant) -> DateTime<Utc> {
        let now = Instant::now();
        let wall_now = Utc::now();
        if at >= now {
            wall_now + chrono::Duration::from_std(at - now).unwrap_or_default()
        } else {
            wall_now - chrono::Duration::from_std(now - at).unwrap_or_default()
        }
    }

    pub fn wall_now(&self) -> DateTime<Utc> {
        Utc::now()
    }
}

pub fn ms(d: Duration) -> f64 {
    d.as_secs_f64() * 1000.0
}

/// Windowed sample ring for percentile reporting. Keeps the sample instant so a
/// stale window is never silently reported as current.
#[derive(Debug)]
pub struct Samples {
    window: Duration,
    values: std::collections::VecDeque<(Instant, f64)>,
    timeouts: std::collections::VecDeque<Instant>,
}

#[derive(Debug, Clone, Copy, serde::Serialize)]
pub struct Percentiles {
    pub p50: f64,
    pub p95: f64,
    pub p99: f64,
    pub max: f64,
    pub count: usize,
    pub timeouts: usize,
    pub window_secs: u64,
    /// Seconds of samples actually held, which is shorter than `window_secs`
    /// right after a load change. Displayed rather than rounded up.
    pub span_secs: f64,
}

impl Samples {
    pub fn new(window: Duration) -> Self {
        Self { window, values: Default::default(), timeouts: Default::default() }
    }

    pub fn push(&mut self, at: Instant, value_ms: f64) {
        self.values.push_back((at, value_ms));
        self.evict(at);
    }

    pub fn push_timeout(&mut self, at: Instant) {
        self.timeouts.push_back(at);
        self.evict(at);
    }

    pub fn clear(&mut self) {
        self.values.clear();
        self.timeouts.clear();
    }

    fn evict(&mut self, now: Instant) {
        let cutoff = now.checked_sub(self.window);
        let Some(cutoff) = cutoff else { return };
        while self.values.front().is_some_and(|(t, _)| *t < cutoff) {
            self.values.pop_front();
        }
        while self.timeouts.front().is_some_and(|t| *t < cutoff) {
            self.timeouts.pop_front();
        }
    }

    pub fn percentiles(&self) -> Percentiles {
        let mut v: Vec<f64> = self.values.iter().map(|(_, x)| *x).collect();
        v.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let span = match (self.values.front(), self.values.back()) {
            (Some((a, _)), Some((b, _))) => b.saturating_duration_since(*a).as_secs_f64(),
            _ => 0.0,
        };
        Percentiles {
            p50: quantile(&v, 0.50),
            p95: quantile(&v, 0.95),
            p99: quantile(&v, 0.99),
            max: v.last().copied().unwrap_or(0.0),
            count: v.len(),
            timeouts: self.timeouts.len(),
            window_secs: self.window.as_secs(),
            span_secs: span,
        }
    }
}

fn quantile(sorted: &[f64], q: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let idx = ((sorted.len() - 1) as f64 * q).round() as usize;
    sorted[idx]
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A wall label for a recent instant must track real time, not a monotonic
    /// delta from process start — the bug that made the feature window address
    /// a moment hours in the past after the host suspended.
    #[test]
    fn wall_tracks_real_time_for_recent_instants() {
        let clock = Clock::new();
        let past = Instant::now() - Duration::from_secs(2);
        let drift = (clock.wall(past) - (Utc::now() - chrono::Duration::seconds(2)))
            .num_milliseconds()
            .abs();
        assert!(drift < 50, "wall(past) drifted {drift}ms from real time");
    }

    #[test]
    fn wall_handles_future_instants() {
        let clock = Clock::new();
        let ahead = Instant::now() + Duration::from_secs(5);
        let delta = (clock.wall(ahead) - Utc::now()).num_milliseconds();
        assert!((4900..=5100).contains(&delta), "future delta was {delta}ms");
    }
}
