use crate::ch::ChClient;
use crate::clock::Samples;
use anyhow::Result;
use parking_lot::Mutex;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// Replication freshness probe.
///
/// Probes ride the *same table as real trades*, tagged `scenario_tag='probe'`
/// and filed under a market id outside the traded range. A probe therefore
/// takes the same path, the same batches and the same flush as the data the
/// demo is actually about — a dedicated low-volume probe table would measure an
/// easier path and understate the number on stage. The collector filters the
/// tag out, so probes never reach the detector.
///
/// The timer starts immediately before the INSERT is sent, so the measurement
/// contains statement execution, commit, replication, and the poll that first
/// observes the row. That over-reports rather than under-reports:
/// acknowledgement-to-visible is never relabelled as commit-to-visible.
pub struct Probe {
    /// commit-request -> first query that observes the row (the headline).
    pub samples: Mutex<Samples>,
    /// The source-commit portion alone. Kept separate because the timer starts
    /// before the INSERT, so a saturated Postgres inflates the headline number
    /// without replication being slow at all — without this split you cannot
    /// tell those two apart.
    pub insert_samples: Mutex<Samples>,
    pub sent: AtomicU64,
    pub observed: AtomicU64,
    pub timed_out: AtomicU64,
    pub poll_interval_ms: u64,
    pub timeout_ms: u64,
    pub interval_ms: u64,
    pub table: String,
    pub market_id: i32,
}

impl Probe {
    pub fn new(cfg: &crate::config::Probe, table: String, market_id: i32) -> Self {
        Self {
            samples: Mutex::new(Samples::new(Duration::from_secs(cfg.window_secs))),
            insert_samples: Mutex::new(Samples::new(Duration::from_secs(cfg.window_secs))),
            sent: AtomicU64::new(0),
            observed: AtomicU64::new(0),
            timed_out: AtomicU64::new(0),
            poll_interval_ms: cfg.poll_interval_ms,
            timeout_ms: cfg.timeout_ms,
            interval_ms: cfg.interval_ms,
            table,
            market_id,
        }
    }

    /// Drops the sample window. Called on a load change so an old healthy
    /// percentile is never carried forward under a new load.
    pub fn reset_window(&self) {
        self.samples.lock().clear();
        self.insert_samples.lock().clear();
    }
}

pub fn spawn(probe: Arc<Probe>, dsn: String, ch: ChClient, workers: usize) {
    for _ in 0..workers {
        let probe = probe.clone();
        let dsn = dsn.clone();
        let ch = ch.clone();
        tokio::spawn(async move {
            loop {
                if let Err(e) = worker(probe.clone(), &dsn, &ch).await {
                    tracing::warn!(error = %e, "probe worker restarting");
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
            }
        });
    }
}

fn visible(body: &str) -> bool {
    body.lines()
        .find_map(|l| serde_json::from_str::<Vec<i64>>(l.trim()).ok())
        .and_then(|v| v.first().copied())
        .is_some_and(|n| n > 0)
}

async fn worker(probe: Arc<Probe>, dsn: &str, ch: &ChClient) -> Result<()> {
    let client = crate::pgconn::connect(dsn).await?;
    let stmt = client
        .prepare(
            "INSERT INTO public.market_trades \
             (market_id, trader_id, taker_side, price_cents, quantity, event_ts, scenario_tag) \
             VALUES ($1, 0, 'BUY', 1, 1, $2, 'probe') RETURNING id",
        )
        .await?;

    // event_ts leads the destination sort key, so pinning it in the predicate
    // keeps the visibility poll a granule lookup rather than a table scan.
    let sql = format!(
        "SELECT count() FROM {} WHERE event_ts = {{ts:DateTime64(6,'UTC')}} \
         AND market_id = {{mkt:Int32}} AND id = {{id:Int64}} \
         SETTINGS output_format_json_quote_64bit_integers = 0 FORMAT JSONCompactEachRow",
        probe.table
    );

    let poll = Duration::from_millis(probe.poll_interval_ms);
    let timeout = Duration::from_millis(probe.timeout_ms);
    let gap = Duration::from_millis(probe.interval_ms);

    loop {
        let event_ts = chrono::Utc::now();
        let ts_param = event_ts.format("%Y-%m-%d %H:%M:%S%.6f").to_string();

        let t0 = Instant::now();
        let row = client.query_one(&stmt, &[&probe.market_id, &event_ts]).await?;
        let id: i64 = row.get(0);
        let committed_at = Instant::now();
        probe.insert_samples
            .lock()
            .push(committed_at, crate::clock::ms(committed_at - t0));
        probe.sent.fetch_add(1, Ordering::Relaxed);

        let params = [
            ("ts", ts_param),
            ("mkt", probe.market_id.to_string()),
            ("id", id.to_string()),
        ];

        let mut seen = false;
        while t0.elapsed() < timeout {
            if let Ok(r) = ch.query(&sql, &params, Duration::from_millis(2000)).await
                && visible(&r.body)
            {
                probe.samples.lock().push(Instant::now(), crate::clock::ms(t0.elapsed()));
                probe.observed.fetch_add(1, Ordering::Relaxed);
                seen = true;
                break;
            }
            tokio::time::sleep(poll).await;
        }

        if !seen {
            probe.timed_out.fetch_add(1, Ordering::Relaxed);
            probe.samples.lock().push_timeout(Instant::now());
        }

        tokio::time::sleep(gap).await;
    }
}
