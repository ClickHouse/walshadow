use crate::generator::{Burst, spawn_burst, spawn_recovery};
use crate::state::{AppState, RunRecord};
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::response::sse::{Event, Sse};
use axum::routing::{get, post};
use axum::{Json, Router};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::convert::Infallible;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio_stream::StreamExt;

const BURST_WORKERS: usize = 8;

#[derive(Debug, Deserialize)]
pub struct ShockReq {
    pub market_id: i32,
    #[serde(default = "default_duration")]
    pub duration_ms: u64,
    #[serde(default)]
    pub in_secs: u64,
}

fn default_duration() -> u64 {
    30_000
}

#[derive(Debug, Serialize)]
pub struct ShockResp {
    pub accepted: bool,
    pub run_id: Option<String>,
    pub market_id: i32,
    pub market_name: Option<String>,
    pub duration_ms: u64,
    pub starts_in_secs: u64,
    pub scheduled_wall: Option<chrono::DateTime<chrono::Utc>>,
    pub burst_rate_per_s: Option<f64>,
    pub action: String,
    pub warning: Option<String>,
    pub reason: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct LoadReq {
    pub rate: u64,
}

#[derive(Debug, Serialize)]
pub struct LoadResp {
    pub accepted: bool,
    pub rate: u64,
    pub per_market_rate: f64,
    pub action: String,
    pub reason: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct ResultsReq {
    pub name: String,
}

pub fn router(state: Arc<AppState>, static_dir: String) -> Router {
    Router::new()
        .route("/api/state", get(state_handler))
        .route("/api/stream", get(stream))
        .route("/api/shock", post(shock))
        .route("/api/load", post(load))
        .route("/api/evidence", get(evidence))
        .route("/api/results", post(results))
        .route("/api/reset", post(reset))
        .route("/api/sql", get(sql))
        .route("/api/samples", get(samples))
        .fallback_service(tower_http::services::ServeDir::new(static_dir))
        .with_state(state)
}

async fn state_handler(State(s): State<Arc<AppState>>) -> impl IntoResponse {
    Json(s.snapshot())
}

async fn stream(
    State(s): State<Arc<AppState>>,
) -> Sse<impl tokio_stream::Stream<Item = Result<Event, Infallible>>> {
    let rx = s.tx.subscribe();
    let initial = Arc::new(s.snapshot());
    let head = tokio_stream::once(initial);
    let tail = tokio_stream::wrappers::BroadcastStream::new(rx).filter_map(|r| r.ok());
    let stream = head.chain(tail).map(|snap| {
        Ok(Event::default()
            .id(snap.seq.to_string())
            .json_data(&*snap)
            .unwrap_or_else(|_| Event::default().comment("encode failed")))
    });
    Sse::new(stream).keep_alive(axum::response::sse::KeepAlive::new())
}

fn rejected(req: &ShockReq, reason: String) -> (StatusCode, Json<ShockResp>) {
    (
        StatusCode::CONFLICT,
        Json(ShockResp {
            accepted: false,
            run_id: None,
            market_id: req.market_id,
            market_name: None,
            duration_ms: req.duration_ms,
            starts_in_secs: req.in_secs,
            scheduled_wall: None,
            burst_rate_per_s: None,
            action: "rejected".into(),
            warning: None,
            reason: Some(reason),
        }),
    )
}

/// Arms a burst and returns immediately. The countdown runs here, server-side,
/// so the presenter can switch back to the browser before any source write
/// happens. The countdown never counts toward shock-to-alert timing.
async fn shock(State(s): State<Arc<AppState>>, Json(req): Json<ShockReq>) -> impl IntoResponse {
    if !s.markets.contains_key(&req.market_id) {
        return rejected(&req, format!("unknown market {}", req.market_id));
    }
    if !s.generator.in_range(req.market_id) {
        return rejected(
            &req,
            format!(
                "market {} is outside the active range {}..{} for profile '{}', so it carries no baseline traffic",
                req.market_id,
                s.generator.market_base,
                s.generator.market_base + s.generator.markets() - 1,
                s.cfg.profile
            ),
        );
    }
    if req.duration_ms == 0 || req.duration_ms > s.cfg.shock.max_duration_ms {
        return rejected(
            &req,
            format!(
                "duration must be between 0.1s and {}s (got {}s)",
                s.cfg.shock.max_duration_ms as f64 / 1000.0,
                req.duration_ms as f64 / 1000.0
            ),
        );
    }

    let existing = s.generator.burst.lock().clone();
    if let Some(b) = existing
        && !b.finished_at(Instant::now())
    {
        return rejected(&req, "a shock is already armed or running".into());
    }

    // A surge is only meaningful against a real baseline, so check the thing
    // that actually matters: how many trades the detector can currently see in
    // this market's baseline window. That is what `normal_buy_per_s` divides
    // by. Time-since-first-write was only ever a proxy for it, and a bad one —
    // it resets on restart even though the history in ClickHouse is untouched,
    // forcing a wait for a baseline that is already there.
    let need = s.cfg.detector.baseline_trades_min;
    let seen = s
        .last_frame
        .lock()
        .as_ref()
        .map(|f| {
            f.rows
                .iter()
                .find(|m| m.market_id == req.market_id)
                .map(|m| m.baseline_trades)
                .unwrap_or(0)
        });
    match seen {
        None => {
            return rejected(
                &req,
                "no feature frame yet — the collector has not completed a query. \
                 Check the destination table exists and try again in a second."
                    .into(),
            );
        }
        Some(n) if n < need => {
            let rate = s.generator.per_market_rate().max(0.1);
            let wait = ((need - n) as f64 / rate).ceil();
            return rejected(
                &req,
                format!(
                    "market {} has only {} trades in its {}s baseline window; {} required. \
                     At {:.0} trades/s per market that is about {:.0}s away — wait, or use --in {:.0}.",
                    req.market_id, n, 30, need, rate, wait, wait + 2.0
                ),
            );
        }
        Some(_) => {}
    }

    let run_id = uuid::Uuid::new_v4();
    let now = Instant::now();
    let starts_at = now + Duration::from_secs(req.in_secs);
    let ends_at = starts_at + Duration::from_millis(req.duration_ms);

    let burst = Arc::new(Burst {
        run_id,
        market_id: req.market_id,
        armed_at: now,
        starts_at,
        ends_at,
        // start from where the market actually is, not a fixed number, so a
        // re-shocked market does not visibly snap before it climbs
        price_from: s.generator.mid_cents(req.market_id),
        price_to: s.cfg.shock.price_path_to,
        first_commit_at: Mutex::new(None),
        last_commit_at: Mutex::new(None),
        rows: AtomicU64::new(0),
    });

    let market_name = s.market_name(req.market_id);
    let scheduled_wall = s.clock.wall(starts_at);

    {
        let mut cur = s.current_run.lock();
        if let Some(prev) = cur.take() {
            let mut hist = s.history.lock();
            hist.push_front(prev);
            while hist.len() > s.cfg.snapshot.history_runs {
                hist.pop_back();
            }
        }
        *cur = Some(RunRecord {
            run_id: run_id.to_string(),
            market_id: req.market_id,
            market_name: market_name.clone(),
            duration_ms: req.duration_ms,
            countdown_secs: req.in_secs,
            accepted_wall: s.clock.wall_now(),
            countdown_ms_measured: None,
            scheduled_wall,
            burst_start_wall: None,
            burst_end_wall: None,
            burst_rows: 0,
            live: None,
            delayed: None,
            rate_at_arm: s.generator.base_rate(),
            status: "armed".into(),
        });
    }

    *s.generator.burst.lock() = Some(burst.clone());
    s.observer.select(req.market_id);
    spawn_burst(s.generator.clone(), burst.clone(), BURST_WORKERS);
    spawn_recovery(s.generator.clone(), burst.clone(), s.cfg.shock.recovery_secs);

    let burst_rate = s.generator.per_market_rate()
        * s.generator.burst_multiplier.load(Ordering::Relaxed) as f64;

    let pg = s.pg.clone();
    let scheduled = scheduled_wall;
    tokio::spawn(async move {
        let sql = "INSERT INTO public.shock_events \
                   (run_id, market_id, duration_ms, scheduled_for, status) \
                   VALUES ($1, $2, $3, $4, 'armed')";
        if let Err(e) = pg
            .execute(sql, &[&run_id, &req.market_id, &(req.duration_ms as i32), &scheduled])
            .await
        {
            tracing::warn!(error = %e, "recording shock_events row failed");
        }
    });

    (
        StatusCode::OK,
        Json(ShockResp {
            accepted: true,
            run_id: Some(run_id.to_string()),
            market_id: req.market_id,
            market_name: Some(market_name),
            duration_ms: req.duration_ms,
            starts_in_secs: req.in_secs,
            scheduled_wall: Some(scheduled_wall),
            burst_rate_per_s: Some(burst_rate),
            action: format!(
                "armed: {:.0} BUY trades/s into market {} for {}s, starting in {}s",
                burst_rate,
                req.market_id,
                req.duration_ms as f64 / 1000.0,
                req.in_secs
            ),
            // The reveal depends on the delayed side still being blind when the
            // burst ends. A burst that outlasts the delay makes both panels
            // read "during burst" and the comparison shows nothing.
            warning: (req.duration_ms >= s.cfg.delay.simulated_min_ms).then(|| {
                format!(
                    "burst ({}s) is not shorter than the simulated delay ({}s), so BOTH panels \
                     will alert during the burst and the comparison will show no contrast. \
                     Use --duration {} or less.",
                    req.duration_ms as f64 / 1000.0,
                    s.cfg.delay.simulated_min_ms as f64 / 1000.0,
                    (s.cfg.delay.simulated_min_ms as f64 / 1000.0 / 2.0).max(1.0)
                )
            }),
            reason: None,
        }),
    )
}

/// Changes the sustained rate and returns immediately. Percentile windows are
/// dropped, because a percentile measured at the old rate is not a result for
/// the new one.
async fn load(State(s): State<Arc<AppState>>, Json(req): Json<LoadReq>) -> impl IntoResponse {
    if req.rate == 0 || req.rate > 5_000_000 {
        return (
            StatusCode::BAD_REQUEST,
            Json(LoadResp {
                accepted: false,
                rate: req.rate,
                per_market_rate: 0.0,
                action: "rejected".into(),
                reason: Some("rate must be 1..=5000000".into()),
            }),
        );
    }
    s.generator.rate.store(req.rate, Ordering::Relaxed);
    s.probe.reset_window();
    s.query_samples.lock().clear();

    let per_market = s.generator.per_market_rate();
    (
        StatusCode::OK,
        Json(LoadResp {
            accepted: true,
            rate: req.rate,
            per_market_rate: per_market,
            action: format!(
                "target {} committed trades/s across {} markets ({:.1}/s per market); \
                 percentile windows reset",
                req.rate,
                s.generator.markets(),
                per_market
            ),
            reason: None,
        }),
    )
}

async fn evidence(State(s): State<Arc<AppState>>) -> impl IntoResponse {
    Json(serde_json::json!({
        "frames": s.frames.lock().iter().cloned().collect::<Vec<_>>(),
        "history": s.history.lock().iter().cloned().collect::<Vec<_>>(),
        "current": s.current_run.lock().clone(),
        "collector_sql": s.collector.sql(),
        "simulated_delay_ms": {"min": s.cfg.delay.simulated_min_ms, "max": s.cfg.delay.simulated_max_ms},
        "thresholds": {
            "baseline_trades_min": s.cfg.detector.baseline_trades_min,
            "recent_trades_min": s.cfg.detector.recent_trades_min,
            "buy_multiple_min": s.cfg.detector.buy_multiple_min,
            "imbalance_min": s.cfg.detector.imbalance_min,
            "cooldown_ms": s.cfg.detector.cooldown_ms,
        },
    }))
}

/// Freezes the current measured state to results/<name>.json.
async fn results(
    State(s): State<Arc<AppState>>,
    Json(req): Json<ResultsReq>,
) -> impl IntoResponse {
    let safe: String = req
        .name
        .chars()
        .filter(|c| c.is_alphanumeric() || *c == '-' || *c == '_')
        .collect();
    if safe.is_empty() {
        return (StatusCode::BAD_REQUEST, Json(serde_json::json!({"error": "bad name"})));
    }
    let snap = s.snapshot();
    let doc = serde_json::json!({
        "name": safe,
        "captured": snap.emitted_wall,
        "profile": snap.profile,
        "target_rate": snap.target_rate,
        "committed_per_s": snap.committed_per_s,
        "rows_per_s": snap.rows_per_s,
        "rows_per_commit": snap.rows_per_commit,
        "generator_errors": snap.gen_errors,
        "replication_ms": snap.replication,
        "feature_query_ms": snap.query,
        "simulated_delay_ms": {"min": snap.simulated_delay_min_ms, "max": snap.simulated_delay_max_ms},
        "current_run": snap.current_run,
        "history": snap.history,
        "frames": s.frames.lock().iter().cloned().collect::<Vec<_>>(),
    });
    let path = format!("{}/{}.json", s.results_dir, safe);
    match std::fs::write(&path, serde_json::to_vec_pretty(&doc).unwrap_or_default()) {
        Ok(()) => (StatusCode::OK, Json(serde_json::json!({"written": path}))),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": e.to_string()})),
        ),
    }
}

async fn reset(State(s): State<Arc<AppState>>) -> impl IntoResponse {
    s.current_run.lock().take();
    s.history.lock().clear();
    s.frames.lock().clear();
    s.probe.reset_window();
    s.query_samples.lock().clear();
    s.live_rule.lock().reset();
    s.delayed_rule.lock().reset();
    s.observer.clear();
    s.generator.reset_mids();
    *s.generator.burst.lock() = None;
    Json(serde_json::json!({"reset": true}))
}

/// The same handful of trades on both sides, so the drawer can show a row in
/// Postgres and the identical row in ClickHouse with its arrival stamp.
async fn samples(State(s): State<Arc<AppState>>) -> impl IntoResponse {
    let pg_rows = s
        .pg
        .query(
            // a couple of seconds behind the write head, so these rows have
            // certainly replicated — sampling the newest rows just races
            "SELECT id, market_id, taker_side, price_cents, quantity, \
             to_char(created_at AT TIME ZONE 'UTC','HH24:MI:SS.US') \
             FROM public.market_trades \
             WHERE scenario_tag <> 'probe' AND created_at < now() - interval '2 seconds' \
             ORDER BY id DESC LIMIT 5",
            &[],
        )
        .await
        .map(|rows| {
            rows.iter()
                .map(|r| {
                    serde_json::json!({
                        "id": r.get::<_, i64>(0),
                        "market_id": r.get::<_, i32>(1),
                        "side": r.get::<_, String>(2),
                        "price": r.get::<_, i32>(3),
                        "qty": r.get::<_, i32>(4),
                        "created_at": r.get::<_, String>(5),
                    })
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();

    let ids: Vec<String> = pg_rows
        .iter()
        .filter_map(|r| r.get("id").and_then(|v| v.as_i64()))
        .map(|i| i.to_string())
        .collect();

    let mut ch_rows = Vec::new();
    if !ids.is_empty() {
        let sql = format!(
            "SELECT id, substring(toString(created_at), 12), substring(toString(_arrived_at), 12), \
             dateDiff('millisecond', created_at, _arrived_at), toString(_lsn) \
             FROM {} FINAL \
             WHERE event_ts >= now64(6) - INTERVAL 60 SECOND AND id IN ({}) \
             ORDER BY id DESC \
             SETTINGS output_format_json_quote_64bit_integers = 0 FORMAT JSONCompactEachRow",
            s.cfg.destination.table,
            ids.join(",")
        );
        if let Ok(resp) = s.ch.query(&sql, &[], Duration::from_millis(3000)).await {
            for line in resp.body.lines().filter(|l| !l.trim().is_empty()) {
                if let Ok(v) = serde_json::from_str::<Vec<serde_json::Value>>(line) {
                    if v.len() >= 5 {
                        ch_rows.push(serde_json::json!({
                            "id": v[0].as_i64().unwrap_or(0),
                            "created_at": v[1].as_str().unwrap_or(""),
                            "arrived_at": v[2].as_str().unwrap_or(""),
                            "trip_ms": v[3].as_i64().unwrap_or(0),
                            "lsn": v[4].as_str().unwrap_or(""),
                        }));
                    }
                }
            }
        }
    }
    Json(serde_json::json!({ "postgres": pg_rows, "clickhouse": ch_rows }))
}

async fn sql(State(s): State<Arc<AppState>>) -> impl IntoResponse {
    Json(serde_json::json!({ "collector": s.collector.sql() }))
}
