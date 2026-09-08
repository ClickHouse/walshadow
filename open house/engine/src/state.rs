use crate::clock::{Clock, Percentiles};
use crate::collector::Collector;
use crate::config::Config;
use crate::detector::Alert;
use crate::generator::{Burst, Generator};
use crate::observer::{Observer, PricePoint, TapeRow};
use crate::probe::Probe;
use chrono::{DateTime, Utc};
use parking_lot::Mutex;
use serde::Serialize;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Instant;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Side {
    Live,
    Delayed,
}

#[derive(Debug, Clone, Serialize)]
pub struct AlertRecord {
    pub frame_id: u64,
    pub emitted_wall: DateTime<Utc>,
    pub feature_ts: DateTime<Utc>,
    pub ms_from_burst_start: f64,
    pub during_burst: bool,
    pub buy_multiple: f64,
    pub imbalance: f64,
    pub last_price_cents: i32,
}

#[derive(Debug, Clone, Serialize)]
pub struct RunRecord {
    pub run_id: String,
    pub market_id: i32,
    pub market_name: String,
    pub duration_ms: u64,
    pub countdown_secs: u64,
    pub accepted_wall: DateTime<Utc>,
    /// Seconds of countdown that ran before any source write. Excluded from
    /// every shock-to-alert number; recorded so the exclusion is auditable.
    pub countdown_ms_measured: Option<f64>,
    pub scheduled_wall: DateTime<Utc>,
    pub burst_start_wall: Option<DateTime<Utc>>,
    pub burst_end_wall: Option<DateTime<Utc>>,
    pub burst_rows: u64,
    pub live: Option<AlertRecord>,
    pub delayed: Option<AlertRecord>,
    pub rate_at_arm: u64,
    pub status: String,
}

/// What a consumer is looking at *right now* for the watched market, updated
/// every frame. This is what makes the delay visible while it is happening:
/// the live gauge climbs during the burst while the delayed gauge sits flat on
/// five-second-old features, then climbs once the queue releases them.
#[derive(Debug, Clone, Serialize)]
pub struct ConsumerView {
    pub market_id: i32,
    pub buy_multiple: f64,
    pub imbalance: f64,
    pub recent_trades: i64,
    pub baseline_trades: i64,
    pub frame_id: u64,
    pub feature_ts: DateTime<Utc>,
    /// How stale the features this consumer is acting on are.
    pub data_age_ms: f64,
    pub gates_ok: bool,
}

/// Per-frame audit trail proving both consumers saw the same frames and how
/// long the delay queue actually held each one.
#[derive(Debug, Clone, Serialize)]
pub struct FrameAudit {
    pub frame_id: u64,
    pub feature_ts: DateTime<Utc>,
    pub markets_evaluated: usize,
    pub query_ms: f64,
    pub read_rows: Option<u64>,
    pub read_bytes: Option<u64>,
    pub live_evaluated_wall: Option<DateTime<Utc>>,
    pub delayed_evaluated_wall: Option<DateTime<Utc>>,
    pub actual_delay_ms: Option<f64>,
}

pub struct Throughput {
    marks: Mutex<VecDeque<(Instant, u64, u64)>>,
}

impl Throughput {
    pub fn new() -> Self {
        Self { marks: Mutex::new(VecDeque::new()) }
    }

    pub fn sample(&self, commits: u64, rows: u64) -> (f64, f64) {
        let now = Instant::now();
        let mut m = self.marks.lock();
        m.push_back((now, commits, rows));
        while m.len() > 2 && now.saturating_duration_since(m[0].0).as_secs_f64() > 5.0 {
            m.pop_front();
        }
        let (t0, c0, r0) = m[0];
        let dt = now.saturating_duration_since(t0).as_secs_f64();
        if dt < 0.25 {
            return (0.0, 0.0);
        }
        (((commits - c0) as f64) / dt, ((rows - r0) as f64) / dt)
    }
}

pub struct AppState {
    pub cfg: Config,
    pub clock: Clock,
    pub generator: Arc<Generator>,
    pub probe: Arc<Probe>,
    pub observer: Arc<Observer>,
    pub collector: Arc<Collector>,
    pub throughput: Throughput,
    pub markets: HashMap<i32, String>,

    pub pg: Arc<tokio_postgres::Client>,
    pub ch: crate::ch::ChClient,
    pub results_dir: String,
    pub live_rule: Mutex<crate::detector::Rule>,
    pub delayed_rule: Mutex<crate::detector::Rule>,

    pub current_run: Mutex<Option<RunRecord>>,
    pub history: Mutex<VecDeque<RunRecord>>,
    pub frames: Mutex<VecDeque<FrameAudit>>,
    pub last_frame: Mutex<Option<crate::detector::Frame>>,
    pub live_view: Mutex<Option<ConsumerView>>,
    pub delayed_view: Mutex<Option<ConsumerView>>,
    pub query_samples: Mutex<crate::clock::Samples>,
    pub collector_error: Mutex<Option<(Instant, String)>>,
    /// created_at -> _arrived_at, measured in ClickHouse. Corroborates the
    /// probe-based headline p95; never replaces it (it spans two clocks).
    pub row_lateness: Mutex<Option<RowLateness>>,
    pub seq: std::sync::atomic::AtomicU64,
    pub tx: tokio::sync::broadcast::Sender<Arc<Snapshot>>,
}

impl AppState {
    pub fn market_name(&self, id: i32) -> String {
        self.markets.get(&id).cloned().unwrap_or_else(|| format!("Market {id}"))
    }

    /// Records that a consumer evaluated a frame. Both sides call this, so the
    /// audit shows the real queue delay rather than the configured one.
    pub fn note_frame(&self, frame: &crate::detector::Frame, side: Side, at: Instant) {
        let mut frames = self.frames.lock();
        if let Some(entry) = frames.iter_mut().find(|f| f.frame_id == frame.frame_id) {
            match side {
                Side::Live => entry.live_evaluated_wall = Some(self.clock.wall(at)),
                Side::Delayed => {
                    entry.delayed_evaluated_wall = Some(self.clock.wall(at));
                    entry.actual_delay_ms =
                        Some(crate::clock::ms(at.saturating_duration_since(frame.feature_at)));
                }
            }
            return;
        }
        frames.push_back(FrameAudit {
            frame_id: frame.frame_id,
            feature_ts: frame.feature_ts,
            markets_evaluated: frame.markets_evaluated,
            query_ms: frame.query_elapsed_ms,
            read_rows: frame.read_rows,
            read_bytes: frame.read_bytes,
            live_evaluated_wall: (side == Side::Live).then(|| self.clock.wall(at)),
            delayed_evaluated_wall: (side == Side::Delayed).then(|| self.clock.wall(at)),
            actual_delay_ms: (side == Side::Delayed).then(|| {
                crate::clock::ms(at.saturating_duration_since(frame.feature_at))
            }),
        });
        while frames.len() > 600 {
            frames.pop_front();
        }
    }

    /// Snapshots what this consumer sees for the watched market on this frame.
    pub fn note_view(&self, side: Side, frame: &crate::detector::Frame, at: Instant) {
        let watched = self.observer.market_id.load(Ordering::Relaxed);
        let d = &self.cfg.detector;
        let view = frame.rows.iter().find(|m| m.market_id == watched).map(|m| ConsumerView {
            market_id: m.market_id,
            buy_multiple: m.buy_multiple,
            imbalance: m.imbalance,
            recent_trades: m.recent_trades,
            baseline_trades: m.baseline_trades,
            frame_id: frame.frame_id,
            feature_ts: frame.feature_ts,
            data_age_ms: crate::clock::ms(at.saturating_duration_since(frame.feature_at)),
            gates_ok: m.baseline_trades >= d.baseline_trades_min
                && m.recent_trades >= d.recent_trades_min
                && m.imbalance > d.imbalance_min,
        });
        match side {
            Side::Live => *self.live_view.lock() = view,
            Side::Delayed => *self.delayed_view.lock() = view,
        }
    }

    /// Attributes an alert to the running comparison, if it belongs to it. The
    /// during/after verdict is assigned here, from observed times only.
    pub fn record_alert(&self, side: Side, alert: &Alert, burst: Option<&Arc<Burst>>) {
        let mut run = self.current_run.lock();
        let Some(rec) = run.as_mut() else { return };
        if alert.market_id != rec.market_id {
            return;
        }
        let Some(burst) = burst else { return };
        let Some(start) = *burst.first_commit_at.lock() else { return };
        if alert.emitted_at < start {
            return;
        }
        let slot = match side {
            Side::Live => &mut rec.live,
            Side::Delayed => &mut rec.delayed,
        };
        if slot.is_some() {
            return;
        }
        let ms = crate::clock::ms(alert.emitted_at.saturating_duration_since(start));
        *slot = Some(AlertRecord {
            frame_id: alert.frame_id,
            emitted_wall: self.clock.wall(alert.emitted_at),
            feature_ts: alert.feature_ts,
            ms_from_burst_start: ms,
            during_burst: alert.emitted_at < burst.ends_at,
            buy_multiple: alert.buy_multiple,
            imbalance: alert.imbalance,
            last_price_cents: alert.last_price_cents,
        });
    }

    pub fn snapshot(&self) -> Snapshot {
        let committed = self.generator.committed.load(Ordering::Relaxed);
        let rows = self.generator.rows_written.load(Ordering::Relaxed);
        let (commits_per_s, rows_per_s) = self.throughput.sample(committed, rows);

        let burst = self.generator.burst.lock().clone();
        let now = Instant::now();
        let countdown_ms = burst.as_ref().and_then(|b| {
            (b.starts_at > now).then(|| crate::clock::ms(b.starts_at - now))
        });
        let burst_active = burst.as_ref().is_some_and(|b| b.active_at(now));

        let collector_err = self
            .collector_error
            .lock()
            .as_ref()
            .filter(|(t, _)| t.elapsed().as_secs_f64() < 5.0)
            .map(|(_, m)| m.clone());

        let last_frame_age_ms = self
            .last_frame
            .lock()
            .as_ref()
            .map(|f| crate::clock::ms(now.saturating_duration_since(f.query_completed_at)));

        Snapshot {
            seq: self.seq.fetch_add(1, Ordering::Relaxed),
            emitted_wall: self.clock.wall_now(),
            market_id: self.observer.market_id.load(Ordering::Relaxed),
            market_name: self.market_name(self.observer.market_id.load(Ordering::Relaxed)),
            points: self.observer.points.lock().iter().cloned().collect(),
            tape: self.observer.tape.lock().iter().cloned().collect(),
            committed_per_s: commits_per_s,
            rows_per_s,
            rows_per_commit: self.generator.rows_per_commit.load(Ordering::Relaxed),
            target_rate: self.generator.base_rate(),
            gen_errors: self.generator.errors.load(Ordering::Relaxed),
            replication: self.probe.samples.lock().percentiles(),
            source_commit: self.probe.insert_samples.lock().percentiles(),
            query: self.query_samples.lock().percentiles(),
            simulated_delay_min_ms: self.cfg.delay.simulated_min_ms,
            simulated_delay_max_ms: self.cfg.delay.simulated_max_ms,
            countdown_ms,
            burst_active,
            burst_start_wall: burst
                .as_ref()
                .and_then(|b| *b.first_commit_at.lock())
                .map(|t| self.clock.wall(t)),
            burst_end_wall: burst.as_ref().map(|b| self.clock.wall(b.ends_at)),
            current_run: self.current_run.lock().clone(),
            history: self.history.lock().iter().cloned().collect(),
            source_stale_secs: self.observer.staleness_secs(),
            features_stale_ms: last_frame_age_ms,
            collector_error: collector_err,
            live_view: self.live_view.lock().clone(),
            delayed_view: self.delayed_view.lock().clone(),
            buy_multiple_min: self.cfg.detector.buy_multiple_min,
            row_lateness: *self.row_lateness.lock(),
            profile: self.cfg.profile.clone(),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize)]
pub struct RowLateness {
    pub rows: u64,
    pub avg_ms: f64,
    pub p50_ms: f64,
    pub p95_ms: f64,
    pub max_ms: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct Snapshot {
    pub seq: u64,
    pub emitted_wall: DateTime<Utc>,
    pub market_id: i32,
    pub market_name: String,
    pub points: Vec<PricePoint>,
    pub tape: Vec<TapeRow>,
    pub committed_per_s: f64,
    pub rows_per_s: f64,
    pub rows_per_commit: u64,
    pub target_rate: u64,
    pub gen_errors: u64,
    pub replication: Percentiles,
    pub source_commit: Percentiles,
    pub query: Percentiles,
    pub simulated_delay_min_ms: u64,
    pub simulated_delay_max_ms: u64,
    pub countdown_ms: Option<f64>,
    pub burst_active: bool,
    pub burst_start_wall: Option<DateTime<Utc>>,
    pub burst_end_wall: Option<DateTime<Utc>>,
    pub current_run: Option<RunRecord>,
    pub history: Vec<RunRecord>,
    pub source_stale_secs: Option<f64>,
    pub features_stale_ms: Option<f64>,
    pub collector_error: Option<String>,
    pub live_view: Option<ConsumerView>,
    pub delayed_view: Option<ConsumerView>,
    pub buy_multiple_min: f64,
    pub row_lateness: Option<RowLateness>,
    pub profile: String,
}
