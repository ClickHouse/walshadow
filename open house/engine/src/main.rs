mod api;
mod ch;
mod clock;
mod collector;
mod config;
mod detector;
mod generator;
mod observer;
mod pgconn;
mod probe;
mod state;

use anyhow::{Context, Result};
use clock::{Clock, Samples};
use collector::Collector;
use config::Config;
use detector::{Frame, Rule};
use generator::Generator;
use observer::Observer;
use parking_lot::Mutex;
use probe::Probe;
use state::{AppState, Side, Throughput};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::time::{Duration, Instant};

const MARKET_BASE: i32 = 101;

fn env(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,market_engine=info".into()),
        )
        .init();

    let root = env("OPENHOUSE_ROOT", "..");
    let cfg = Config::load(
        &env("CONFIG_PATH", &format!("{root}/config/demo.toml")),
        std::env::var("DEMO_PROFILE").ok(),
    )?;
    let profile = cfg.active_profile();
    let dsn = std::env::var("PG_DSN").context("PG_DSN is required")?;
    let ch_url = std::env::var("CH_URL").context("CH_URL is required")?;
    let bind = env("BIND_ADDR", "0.0.0.0:8080");
    let static_dir = env("STATIC_DIR", "static");
    let results_dir = env("RESULTS_DIR", &format!("{root}/results"));

    let clock = Clock::new();
    let ch_client = ch::ChClient::from_url(&ch_url)?;

    let pg = pgconn::connect(&dsn).await.context("connecting to PG_DSN")?;

    let markets: HashMap<i32, String> = pg
        .query("SELECT market_id, name FROM public.markets ORDER BY market_id", &[])
        .await
        .context("loading markets — run bin/setup.sh first")?
        .iter()
        .map(|r| (r.get::<_, i32>(0), r.get::<_, String>(1)))
        .collect();
    if markets.is_empty() {
        anyhow::bail!("no markets seeded; run bin/setup.sh first");
    }
    tracing::info!(markets = markets.len(), profile = %cfg.profile, "starting");

    let template = std::fs::read_to_string(format!("{root}/sql/collector.sql"))
        .context("reading sql/collector.sql")?;
    let collector = Arc::new(Collector::new(
        ch_client.clone(),
        &template,
        &cfg.destination,
        cfg.collector,
    ));

    let generator = Arc::new(Generator::new(
        dsn.clone(),
        profile.rate,
        profile.rows_per_commit,
        profile.active_markets,
        MARKET_BASE,
        profile.burst_multiplier,
    ));
    let probe = Arc::new(Probe::new(
        &cfg.probe,
        cfg.destination.table.clone(),
        cfg.destination.probe_market_id,
    ));
    let obs = Arc::new(Observer::new(
        MARKET_BASE,
        cfg.snapshot.chart_points,
        cfg.snapshot.tape_rows,
    ));

    let (tx, _) = tokio::sync::broadcast::channel(64);
    let app = Arc::new(AppState {
        clock,
        generator: generator.clone(),
        probe: probe.clone(),
        observer: obs.clone(),
        collector: collector.clone(),
        throughput: Throughput::new(),
        markets,
        pg: Arc::new(pg),
        ch: ch_client.clone(),
        results_dir: results_dir.clone(),
        live_rule: Mutex::new(Rule::new(cfg.detector)),
        delayed_rule: Mutex::new(Rule::new(cfg.detector)),
        current_run: Mutex::new(None),
        history: Mutex::new(Default::default()),
        frames: Mutex::new(Default::default()),
        last_frame: Mutex::new(None),
        live_view: Mutex::new(None),
        delayed_view: Mutex::new(None),
        query_samples: Mutex::new(Samples::new(Duration::from_secs(cfg.probe.window_secs))),
        collector_error: Mutex::new(None),
        row_lateness: Mutex::new(None),
        seq: AtomicU64::new(0),
        tx: tx.clone(),
        cfg,
    });

    let _ = std::fs::create_dir_all(&results_dir);

    generator::spawn_baseline(generator.clone(), profile.connections);
    probe::spawn(probe.clone(), dsn.clone(), ch_client.clone(), app.cfg.probe.workers);
    observer::spawn(
        obs.clone(),
        dsn.clone(),
        Duration::from_millis(app.cfg.snapshot.push_interval_ms),
        60.0,
    );

    spawn_lateness(app.clone(), ch_client.clone());
    spawn_pipeline(app.clone());
    spawn_pusher(app.clone(), tx);

    let listener = tokio::net::TcpListener::bind(&bind).await?;
    tracing::info!(%bind, "market-engine listening");
    axum::serve(listener, api::router(app, static_dir)).await?;
    Ok(())
}

/// Collector → immediate consumer, and the same frame → delay queue → delayed
/// consumer. Both consumers run the same `Rule`; nothing else differs.
fn spawn_pipeline(app: Arc<AppState>) {
    let (frame_tx, frame_rx) = tokio::sync::mpsc::channel::<Arc<Frame>>(256);
    let (delayed_tx, mut delayed_rx) =
        tokio::sync::mpsc::channel::<(Arc<Frame>, Instant)>(256);

    tokio::spawn(collector::delay_queue(
        frame_rx,
        delayed_tx,
        Duration::from_millis(app.cfg.delay.simulated_min_ms),
        Duration::from_millis(app.cfg.delay.simulated_max_ms),
    ));

    {
        let app = app.clone();
        tokio::spawn(async move {
            let mut ticker =
                tokio::time::interval(Duration::from_millis(app.cfg.collector.interval_ms));
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            let mut empty_streak = 0u32;
            loop {
                ticker.tick().await;
                let feature_ts = app.clock.wall_now();
                match app.collector.collect(feature_ts).await {
                    Ok(frame) => {
                        let frame = Arc::new(frame);
                        app.query_samples
                            .lock()
                            .push(Instant::now(), frame.query_elapsed_ms);
                        // A query that succeeds but matches nothing is the
                        // dangerous case: the panels just read "Monitoring"
                        // forever. Surface it rather than letting it pass as
                        // healthy.
                        if frame.markets_evaluated == 0 {
                            empty_streak += 1;
                        } else {
                            empty_streak = 0;
                        }
                        let secs = empty_streak as f64
                            * app.cfg.collector.interval_ms as f64
                            / 1000.0;
                        *app.collector_error.lock() = (empty_streak >= 20).then(|| {
                            (
                                Instant::now(),
                                format!(
                                    "feature query matched 0 markets for {secs:.0}s — window bound \
                                     {} may not line up with source event_ts",
                                    frame.feature_ts
                                ),
                            )
                        });
                        *app.last_frame.lock() = Some((*frame).clone());

                        let at = Instant::now();
                        app.note_frame(&frame, Side::Live, at);
                        app.note_view(Side::Live, &frame, at);
                        let alerts = app.live_rule.lock().evaluate(&frame, at);
                        let burst = app.generator.burst.lock().clone();
                        for a in &alerts {
                            tracing::info!(market = a.market_id, frame = a.frame_id, "live alert");
                            app.record_alert(Side::Live, a, burst.as_ref());
                        }
                        let _ = frame_tx.send(frame).await;
                    }
                    Err(e) => {
                        *app.collector_error.lock() = Some((Instant::now(), e.to_string()));
                        tracing::warn!(error = %e, "collector query failed");
                    }
                }
            }
        });
    }

    tokio::spawn(async move {
        while let Some((frame, released_at)) = delayed_rx.recv().await {
            let at = Instant::now().max(released_at);
            app.note_frame(&frame, Side::Delayed, at);
            app.note_view(Side::Delayed, &frame, at);
            let alerts = app.delayed_rule.lock().evaluate(&frame, at);
            let burst = app.generator.burst.lock().clone();
            for a in &alerts {
                tracing::info!(market = a.market_id, frame = a.frame_id, "delayed alert");
                app.record_alert(Side::Delayed, a, burst.as_ref());
            }
        }
    });
}

/// Polls the destination for per-row trip time. Offstage corroboration only,
/// so it runs on a slow cadence and never blocks the pipeline.
fn spawn_lateness(app: Arc<AppState>, ch: ch::ChClient) {
    let sql = format!(
        "SELECT count(), avg(d), quantile(0.50)(d), quantile(0.95)(d), max(d) FROM \
         (SELECT dateDiff('millisecond', created_at, _arrived_at) AS d FROM {} \
          WHERE created_at >= now64(3) - INTERVAL 5 SECOND AND _arrived_at > toDateTime64(0, 6) \
            AND scenario_tag != 'probe') \
         SETTINGS output_format_json_quote_64bit_integers = 0 FORMAT JSONCompactEachRow",
        app.cfg.destination.table
    );
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(Duration::from_millis(1000));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            ticker.tick().await;
            let Ok(resp) = ch.query(&sql, &[], Duration::from_millis(3000)).await else {
                continue;
            };
            let parsed = resp
                .body
                .lines()
                .find_map(|l| serde_json::from_str::<Vec<serde_json::Value>>(l.trim()).ok());
            if let Some(v) = parsed
                && v.len() >= 5
                && v[0].as_u64().unwrap_or(0) > 0
            {
                let f = |i: usize| v[i].as_f64().unwrap_or(0.0);
                *app.row_lateness.lock() = Some(state::RowLateness {
                    rows: v[0].as_u64().unwrap_or(0),
                    avg_ms: f(1),
                    p50_ms: f(2),
                    p95_ms: f(3),
                    max_ms: f(4),
                });
            }
        }
    });
}

fn spawn_pusher(app: Arc<AppState>, tx: tokio::sync::broadcast::Sender<Arc<state::Snapshot>>) {
    tokio::spawn(async move {
        let mut ticker =
            tokio::time::interval(Duration::from_millis(app.cfg.snapshot.push_interval_ms));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            ticker.tick().await;
            sync_run(&app);
            let _ = tx.send(Arc::new(app.snapshot()));
        }
    });
}

/// Keeps the run record's observed burst boundaries and status in step with
/// what the generator actually did.
fn sync_run(app: &Arc<AppState>) {
    let burst = app.generator.burst.lock().clone();
    let Some(burst) = burst else { return };
    let mut cur = app.current_run.lock();
    let Some(run) = cur.as_mut() else { return };
    if run.run_id != burst.run_id.to_string() {
        return;
    }
    let now = Instant::now();
    if let Some(first) = *burst.first_commit_at.lock() {
        run.burst_start_wall = Some(app.clock.wall(first));
        run.countdown_ms_measured =
            Some(clock::ms(first.saturating_duration_since(burst.armed_at)));
    }
    run.burst_rows = burst.rows.load(std::sync::atomic::Ordering::Relaxed);
    run.status = if now < burst.starts_at {
        "armed".into()
    } else if burst.active_at(now) {
        "bursting".into()
    } else {
        run.burst_end_wall = Some(app.clock.wall(burst.ends_at));
        "complete".into()
    };
}
