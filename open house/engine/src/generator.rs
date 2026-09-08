use anyhow::Result;
use parking_lot::Mutex;
use rand::Rng;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicI64, AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// A scheduled or running burst. `first_commit_at` is set by whichever burst
/// worker commits first and is the only start point used for shock-to-alert
/// timing — the countdown never counts toward it.
#[derive(Debug)]
pub struct Burst {
    pub run_id: uuid::Uuid,
    pub market_id: i32,
    pub armed_at: Instant,
    pub starts_at: Instant,
    pub ends_at: Instant,
    pub price_from: i32,
    pub price_to: i32,
    pub first_commit_at: Mutex<Option<Instant>>,
    pub last_commit_at: Mutex<Option<Instant>>,
    pub rows: AtomicU64,
}

impl Burst {
    pub fn active_at(&self, now: Instant) -> bool {
        now >= self.starts_at && now < self.ends_at
    }

    pub fn finished_at(&self, now: Instant) -> bool {
        now >= self.ends_at
    }
}

pub struct Generator {
    pub dsn: String,
    pub committed: AtomicU64,
    pub rows_written: AtomicU64,
    pub errors: AtomicU64,
    pub rate: AtomicU64,
    pub rows_per_commit: AtomicU64,
    pub active_markets: AtomicI64,
    pub market_base: i32,
    pub burst_multiplier: AtomicU64,
    pub burst: Mutex<Option<Arc<Burst>>>,
    pub running: AtomicBool,
    /// Per-market mid price in millicents. Baseline trades jitter around it and
    /// nudge it on a slow random walk; a burst drives it along the declared
    /// price path. Without this the chart is uniform noise instead of a market.
    pub mids: Vec<AtomicI32>,
}

pub const MID_SCALE: i32 = 1000;
const MID_START: i32 = 42 * MID_SCALE;
const MID_FLOOR: i32 = 15 * MID_SCALE;
const MID_CEIL: i32 = 85 * MID_SCALE;
pub const MAX_MARKETS: usize = 1000;

impl Generator {
    pub fn new(
        dsn: String,
        rate: u64,
        rows_per_commit: u64,
        active_markets: i32,
        market_base: i32,
        burst_multiplier: u64,
    ) -> Self {
        Self {
            dsn,
            committed: AtomicU64::new(0),
            rows_written: AtomicU64::new(0),
            errors: AtomicU64::new(0),
            rate: AtomicU64::new(rate),
            rows_per_commit: AtomicU64::new(rows_per_commit.max(1)),
            active_markets: AtomicI64::new(active_markets as i64),
            market_base,
            burst_multiplier: AtomicU64::new(burst_multiplier),
            burst: Mutex::new(None),
            running: AtomicBool::new(true),
            mids: (0..MAX_MARKETS).map(|_| AtomicI32::new(MID_START)).collect(),
        }
    }

    pub fn base_rate(&self) -> u64 {
        self.rate.load(Ordering::Relaxed)
    }

    pub fn markets(&self) -> i32 {
        self.active_markets.load(Ordering::Relaxed).max(1) as i32
    }

    pub fn in_range(&self, market_id: i32) -> bool {
        market_id >= self.market_base && market_id < self.market_base + self.markets()
    }


    /// Per-market baseline trade rate implied by the current settings.
    pub fn per_market_rate(&self) -> f64 {
        self.base_rate() as f64 / self.markets() as f64
    }


    fn mid_slot(&self, market_id: i32) -> Option<&AtomicI32> {
        let idx = market_id.checked_sub(self.market_base)? as usize;
        self.mids.get(idx)
    }

    pub fn mid_cents(&self, market_id: i32) -> i32 {
        self.mid_slot(market_id)
            .map(|m| m.load(Ordering::Relaxed) / MID_SCALE)
            .unwrap_or(MID_START / MID_SCALE)
    }

    fn drift_mid(&self, market_id: i32, step: i32) -> i32 {
        let Some(slot) = self.mid_slot(market_id) else {
            return MID_START / MID_SCALE;
        };
        let next = (slot.load(Ordering::Relaxed) + step).clamp(MID_FLOOR, MID_CEIL);
        slot.store(next, Ordering::Relaxed);
        next / MID_SCALE
    }

    fn set_mid(&self, market_id: i32, cents: f64) {
        if let Some(slot) = self.mid_slot(market_id) {
            slot.store(
                ((cents * MID_SCALE as f64) as i32).clamp(MID_FLOOR, MID_CEIL),
                Ordering::Relaxed,
            );
        }
    }

    pub fn ease_mid(&self, market_id: i32, cents: f64) {
        self.set_mid(market_id, cents);
    }

    pub fn reset_mids(&self) {
        for m in &self.mids {
            m.store(MID_START, Ordering::Relaxed);
        }
    }
}

const INSERT_ONE: &str = "INSERT INTO public.market_trades \
    (market_id, trader_id, taker_side, price_cents, quantity, event_ts, scenario_tag) \
    VALUES ($1, $2, $3, $4, $5, $6, $7)";

const INSERT_BATCH: &str = "INSERT INTO public.market_trades \
    (market_id, trader_id, taker_side, price_cents, quantity, event_ts, scenario_tag) \
    SELECT * FROM unnest($1::int[], $2::int[], $3::text[], $4::int[], $5::int[], \
    $6::timestamptz[], $7::text[])";

struct Row {
    market_id: i32,
    trader_id: i32,
    side: &'static str,
    price: i32,
    quantity: i32,
    event_ts: chrono::DateTime<chrono::Utc>,
    tag: &'static str,
}

async fn commit_rows(
    client: &tokio_postgres::Client,
    one: &tokio_postgres::Statement,
    batch: &tokio_postgres::Statement,
    rows: &[Row],
) -> Result<u64, tokio_postgres::Error> {
    if rows.len() == 1 {
        let r = &rows[0];
        return client
            .execute(
                one,
                &[&r.market_id, &r.trader_id, &r.side, &r.price, &r.quantity, &r.event_ts, &r.tag],
            )
            .await;
    }
    let markets: Vec<i32> = rows.iter().map(|r| r.market_id).collect();
    let traders: Vec<i32> = rows.iter().map(|r| r.trader_id).collect();
    let sides: Vec<&str> = rows.iter().map(|r| r.side).collect();
    let prices: Vec<i32> = rows.iter().map(|r| r.price).collect();
    let qtys: Vec<i32> = rows.iter().map(|r| r.quantity).collect();
    let ts: Vec<chrono::DateTime<chrono::Utc>> = rows.iter().map(|r| r.event_ts).collect();
    let tags: Vec<&str> = rows.iter().map(|r| r.tag).collect();
    client
        .execute(batch, &[&markets, &traders, &sides, &prices, &qtys, &ts, &tags])
        .await
}

async fn connect(
    dsn: &str,
) -> Result<(tokio_postgres::Client, tokio_postgres::Statement, tokio_postgres::Statement)> {
    let client = crate::pgconn::connect(dsn).await?;
    let one = client.prepare(INSERT_ONE).await?;
    let batch = client.prepare(INSERT_BATCH).await?;
    Ok((client, one, batch))
}

/// Baseline workers. They spread trades across the active market range and are
/// never repurposed for a burst, so background activity in other markets keeps
/// running while one market is being shocked.
pub fn spawn_baseline(gn: Arc<Generator>, connections: usize) {
    for worker in 0..connections {
        let gn = gn.clone();
        tokio::spawn(async move {
            loop {
                if let Err(e) = baseline_worker(gn.clone(), connections).await {
                    tracing::warn!(worker, error = %e, "baseline worker restarting");
                    gn.errors.fetch_add(1, Ordering::Relaxed);
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
            }
        });
    }
}

async fn baseline_worker(gn: Arc<Generator>, workers: usize) -> Result<()> {
    let (client, one, batch) = connect(&gn.dsn).await?;
    let mut next = Instant::now();

    loop {
        if !gn.running.load(Ordering::Relaxed) {
            tokio::time::sleep(Duration::from_millis(50)).await;
            next = Instant::now();
            continue;
        }

        let per_commit = gn.rows_per_commit.load(Ordering::Relaxed).max(1) as usize;
        let rate = gn.base_rate().max(1) as f64;
        let commits_per_worker = (rate / per_commit as f64 / workers as f64).max(0.001);
        let interval = Duration::from_secs_f64(1.0 / commits_per_worker);

        let markets = gn.markets();
        let base = gn.market_base;
        let event_ts = chrono::Utc::now();
        let rows: Vec<Row> = {
            let mut rng = rand::rng();
            (0..per_commit)
                .map(|_| {
                    let market_id = base + rng.random_range(0..markets);
                    // slow random walk, so the line has shape without drifting away
                    let mid = if rng.random_ratio(1, 40) {
                        gn.drift_mid(market_id, rng.random_range(-220..=220))
                    } else {
                        gn.mid_cents(market_id)
                    };
                    Row {
                        market_id,
                        trader_id: rng.random_range(1..5000),
                        side: if rng.random_bool(0.5) { "BUY" } else { "SELL" },
                        price: (mid + rng.random_range(-1..=1)).clamp(1, 99),
                        quantity: rng.random_range(1..25),
                        event_ts,
                        tag: "live",
                    }
                })
                .collect()
        };

        match commit_rows(&client, &one, &batch, &rows).await {
            Ok(n) => {
                gn.committed.fetch_add(1, Ordering::Relaxed);
                gn.rows_written.fetch_add(n, Ordering::Relaxed);
            }
            Err(e) => {
                gn.errors.fetch_add(1, Ordering::Relaxed);
                if e.is_closed() {
                    return Err(e.into());
                }
            }
        }

        next += interval;
        let now = Instant::now();
        if next < now {
            next = now;
        } else {
            tokio::time::sleep(next - now).await;
        }
    }
}

/// Dedicated burst workers, spawned when a shock is armed and gone when it
/// ends. They write only to the shocked market and only BUY flow, following a
/// predeclared price path. The detector never sees any of this scheduling — it
/// only ever sees the resulting trades once they reach ClickHouse.
pub fn spawn_burst(gn: Arc<Generator>, burst: Arc<Burst>, workers: usize) {
    let target_rate =
        (gn.per_market_rate() * gn.burst_multiplier.load(Ordering::Relaxed) as f64).max(1.0);
    for _ in 0..workers {
        let gn = gn.clone();
        let burst = burst.clone();
        let per_worker = target_rate / workers as f64;
        tokio::spawn(async move {
            if let Err(e) = burst_worker(gn.clone(), burst, per_worker).await {
                tracing::warn!(error = %e, "burst worker failed");
                gn.errors.fetch_add(1, Ordering::Relaxed);
            }
        });
    }
}

/// Eases the market back to where it was before the burst. A burst is a
/// transient spike, not a repricing: without this the market stays at the peak
/// forever and cannot be shocked again without a full reset.
///
/// This moves price only — it adds no trades, so it cannot trip the detector,
/// which keys on buy volume.
pub fn spawn_recovery(gn: Arc<Generator>, burst: Arc<Burst>, recovery_secs: f64) {
    if recovery_secs <= 0.0 {
        return;
    }
    tokio::spawn(async move {
        let now = Instant::now();
        if burst.ends_at > now {
            tokio::time::sleep(burst.ends_at - now).await;
        }
        let peak = gn.mid_cents(burst.market_id) as f64;
        let base = burst.price_from as f64;
        let started = Instant::now();
        loop {
            let frac = started.elapsed().as_secs_f64() / recovery_secs;
            if frac >= 1.0 {
                break;
            }
            // ease-out: falls away quickly, then settles
            let eased = 1.0 - (1.0 - frac).powi(3);
            gn.ease_mid(burst.market_id, peak + (base - peak) * eased);
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        gn.ease_mid(burst.market_id, base);
    });
}

async fn burst_worker(gn: Arc<Generator>, burst: Arc<Burst>, rate: f64) -> Result<()> {
    let (client, one, batch) = connect(&gn.dsn).await?;

    let now = Instant::now();
    if burst.starts_at > now {
        tokio::time::sleep(burst.starts_at - now).await;
    }

    let interval = Duration::from_secs_f64(1.0 / rate.max(0.001));
    let span = burst.ends_at.saturating_duration_since(burst.starts_at).as_secs_f64();
    let mut next = Instant::now();

    while Instant::now() < burst.ends_at {
        let now = Instant::now();
        let frac = (now.saturating_duration_since(burst.starts_at).as_secs_f64() / span).clamp(0.0, 1.0);
        let path = burst.price_from as f64
            + (burst.price_to - burst.price_from) as f64 * frac;
        gn.set_mid(burst.market_id, path);

        let rows = {
            let mut rng = rand::rng();
            vec![Row {
                market_id: burst.market_id,
                trader_id: rng.random_range(1..5000),
                side: "BUY",
                price: (path.round() as i32 + rng.random_range(0..=1)).clamp(1, 99),
                quantity: rng.random_range(8..40),
                event_ts: chrono::Utc::now(),
                tag: "burst",
            }]
        };

        match commit_rows(&client, &one, &batch, &rows).await {
            Ok(n) => {
                gn.committed.fetch_add(1, Ordering::Relaxed);
                gn.rows_written.fetch_add(n, Ordering::Relaxed);
                burst.rows.fetch_add(n, Ordering::Relaxed);
                let at = Instant::now();
                let mut first = burst.first_commit_at.lock();
                if first.is_none() {
                    *first = Some(at);
                }
                drop(first);
                *burst.last_commit_at.lock() = Some(at);
            }
            Err(e) => {
                gn.errors.fetch_add(1, Ordering::Relaxed);
                if e.is_closed() {
                    return Err(e.into());
                }
            }
        }

        next += interval;
        let now = Instant::now();
        if next < now {
            next = now;
        } else {
            tokio::time::sleep(next - now).await;
        }
    }
    Ok(())
}
