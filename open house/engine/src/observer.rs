use anyhow::Result;
use parking_lot::Mutex;
use serde::Serialize;
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicI32, Ordering};
use std::time::Duration;

/// Ground truth read straight from the source database, for the price chart and
/// trade tape. Labelled as source market activity on the page. Neither detector
/// can see any of this — it exists so the audience can watch the market move
/// independently of what either consumer knows.
#[derive(Debug, Clone, Serialize)]
pub struct PricePoint {
    pub ts_ms: i64,
    pub price_cents: i32,
    pub trades: i64,
}

#[derive(Debug, Clone, Serialize)]
pub struct TapeRow {
    pub ts_ms: i64,
    pub side: String,
    pub price_cents: i32,
    pub quantity: i32,
    pub burst: bool,
}

pub struct Observer {
    pub market_id: AtomicI32,
    pub points: Mutex<VecDeque<PricePoint>>,
    pub tape: Mutex<VecDeque<TapeRow>>,
    pub last_success: Mutex<Option<std::time::Instant>>,
    max_points: usize,
    max_tape: usize,
}

impl Observer {
    pub fn new(market_id: i32, max_points: usize, max_tape: usize) -> Self {
        Self {
            market_id: AtomicI32::new(market_id),
            points: Mutex::new(VecDeque::new()),
            tape: Mutex::new(VecDeque::new()),
            last_success: Mutex::new(None),
            max_points,
            max_tape,
        }
    }

    pub fn select(&self, market_id: i32) {
        if self.market_id.swap(market_id, Ordering::Relaxed) != market_id {
            self.points.lock().clear();
            self.tape.lock().clear();
        }
    }

    pub fn clear(&self) {
        self.points.lock().clear();
        self.tape.lock().clear();
    }

    pub fn staleness_secs(&self) -> Option<f64> {
        self.last_success.lock().map(|t| t.elapsed().as_secs_f64())
    }
}

const BARS: &str = "\
SELECT (extract(epoch FROM date_trunc('milliseconds', event_ts)) * 1000)::bigint / 250 * 250 AS bucket_ms,
       (avg(price_cents))::int AS price,
       count(*)::bigint AS trades
FROM public.market_trades
WHERE market_id = $1
  AND event_ts > now() - make_interval(secs => $2::float8)
GROUP BY bucket_ms
ORDER BY bucket_ms";

const TAPE: &str = "\
SELECT (extract(epoch FROM event_ts) * 1000)::bigint AS ts_ms,
       taker_side, price_cents, quantity, scenario_tag
FROM public.market_trades
WHERE market_id = $1
ORDER BY id DESC
LIMIT $2";

pub fn spawn(observer: Arc<Observer>, dsn: String, interval: Duration, window_secs: f64) {
    tokio::spawn(async move {
        loop {
            if let Err(e) = run(observer.clone(), &dsn, interval, window_secs).await {
                tracing::warn!(error = %e, "observer restarting");
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        }
    });
}

async fn run(
    observer: Arc<Observer>,
    dsn: &str,
    interval: Duration,
    window_secs: f64,
) -> Result<()> {
    let client = crate::pgconn::connect(dsn).await?;
    let bars = client.prepare(BARS).await?;
    let tape = client.prepare(TAPE).await?;

    loop {
        let market_id = observer.market_id.load(Ordering::Relaxed);
        let limit = observer.max_tape as i64;

        let bar_rows = client.query(&bars, &[&market_id, &window_secs]).await?;
        let tape_rows = client.query(&tape, &[&market_id, &limit]).await?;

        {
            let mut pts = observer.points.lock();
            pts.clear();
            for r in &bar_rows {
                pts.push_back(PricePoint {
                    ts_ms: r.get::<_, i64>(0),
                    price_cents: r.get::<_, i32>(1),
                    trades: r.get::<_, i64>(2),
                });
            }
            while pts.len() > observer.max_points {
                pts.pop_front();
            }
        }
        {
            let mut tp = observer.tape.lock();
            tp.clear();
            for r in &tape_rows {
                tp.push_back(TapeRow {
                    ts_ms: r.get::<_, i64>(0),
                    side: r.get::<_, String>(1),
                    price_cents: r.get::<_, i32>(2),
                    quantity: r.get::<_, i32>(3),
                    burst: r.get::<_, String>(4) == "burst",
                });
            }
        }
        *observer.last_success.lock() = Some(std::time::Instant::now());

        tokio::time::sleep(interval).await;
    }
}
