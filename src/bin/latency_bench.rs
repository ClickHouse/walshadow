//! Replication-latency benchmarks for the walshadow demo stack.
//!
//! Writes rows to the `source` Postgres and measures how long until they
//! become visible in ClickHouse — i.e. end-to-end replication latency.
//! Runs against the `docker/docker-compose.yml` stack over host-exposed
//! ports; all timing uses one host-side monotonic clock (`Instant`), so
//! there is no cross-container clock skew (both the "committed" and the
//! "visible" instants are taken here).
//!
//! Two independent benchmarks, selected with `--bench`:
//!   * `single-row` — insert one unique row at a time, time commit→visible,
//!                    repeat, report a percentile distribution.
//!   * `sustained`  — drive a continuous insert rate, sample latency under
//!                    load via probe rows, report achieved rate + drain time.
//!
//! Example:
//!   cargo run --release --bin walshadow-latency-bench -- \
//!       --bench single-row --iterations 100
//!   cargo run --release --bin walshadow-latency-bench -- \
//!       --bench sustained --rate 200 --duration-secs 20
//!
//! The ClickHouse side is queried via a tiny hand-rolled HTTP client
//! (modeled on `tests/common/bootstrap_ch_fixture.rs::http_get`) so no
//! reqwest-class dependency is pulled in.

use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use clap::{Parser, ValueEnum};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_postgres::NoTls;

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
enum Bench {
    SingleRow,
    Sustained,
}

#[derive(Parser, Debug)]
#[command(
    name = "walshadow-latency-bench",
    about = "Measure source-Postgres → ClickHouse replication latency"
)]
struct Args {
    /// Which benchmark to run.
    #[arg(long, value_enum)]
    bench: Bench,

    // ---- source Postgres ------------------------------------------------
    #[arg(long, default_value = "127.0.0.1")]
    pg_host: String,
    #[arg(long, default_value_t = 5432)]
    pg_port: u16,
    #[arg(long, default_value = "postgres")]
    pg_user: String,
    #[arg(long, default_value = "postgres")]
    pg_dbname: String,
    #[arg(long)]
    pg_password: Option<String>,
    /// Fully-qualified source table to insert into.
    #[arg(long, default_value = "demo.users")]
    table: String,

    // ---- ClickHouse -----------------------------------------------------
    #[arg(long, default_value = "127.0.0.1")]
    ch_host: String,
    #[arg(long, default_value_t = 8123)]
    ch_http_port: u16,
    /// Fully-qualified destination table to poll.
    #[arg(long, default_value = "demo.users")]
    ch_table: String,

    // ---- single-row bench -----------------------------------------------
    #[arg(long, default_value_t = 100)]
    iterations: u64,
    #[arg(long, default_value_t = 10)]
    warmup: u64,
    #[arg(long, default_value_t = 1)]
    poll_interval_ms: u64,
    #[arg(long, default_value_t = 30_000)]
    row_timeout_ms: u64,

    // ---- sustained bench ------------------------------------------------
    /// Target insert rate (rows/sec).
    #[arg(long, default_value_t = 200)]
    rate: u64,
    #[arg(long, default_value_t = 20)]
    duration_secs: u64,
    /// Tag every Nth row as a latency probe.
    #[arg(long, default_value_t = 25)]
    probe_every: u64,

    // ---- bookkeeping ----------------------------------------------------
    /// Bench rows live at `id >= id_base`; cleared at startup so each run
    /// starts clean. Keep clear of the demo's seeded ids (1..=3).
    #[arg(long, default_value_t = 1_000_000)]
    id_base: i64,
    /// Daemon Prometheus endpoint sampled for emit throughput (best effort).
    #[arg(long, default_value = "http://127.0.0.1:9484/metrics")]
    metrics_url: String,
    /// Emit a machine-readable JSON summary in addition to the table.
    #[arg(long)]
    json: bool,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let args = Args::parse();

    let pg = Arc::new(PgClient::connect(&args).await.context("connect source Postgres")?);
    let ch = ChHttp {
        host: args.ch_host.clone(),
        port: args.ch_http_port,
        table: args.ch_table.clone(),
    };

    // Preflight: fail fast on a misconfigured endpoint rather than looking
    // like every row "timed out".
    let one = ch.query("SELECT 1").await.context("ClickHouse preflight (SELECT 1)")?;
    if one.trim() != "1" {
        bail!("ClickHouse preflight returned {one:?}, expected \"1\"");
    }

    // Clean slate for our id range so every timed write is a true INSERT
    // and re-runs are idempotent.
    pg.delete_bench_rows(args.id_base).await.context("clear prior bench rows")?;

    println!(
        "walshadow-latency-bench — source {}:{}  →  clickhouse {}:{}",
        args.pg_host, args.pg_port, args.ch_host, args.ch_http_port
    );
    println!(
        "latency measured commit→visible (host monotonic clock); poll resolution ≈ {}ms\n",
        args.poll_interval_ms
    );

    let summary = match args.bench {
        Bench::SingleRow => run_single_row(&args, &pg, &ch).await?,
        Bench::Sustained => run_sustained(&args, pg.clone(), &ch).await?,
    };

    if args.json {
        println!("\n{}", serde_json::to_string_pretty(&summary)?);
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Benchmark: single-row latency distribution
// ---------------------------------------------------------------------------

async fn run_single_row(args: &Args, pg: &PgClient, ch: &ChHttp) -> Result<serde_json::Value> {
    let total = args.warmup + args.iterations;
    println!(
        "── single-row latency: {} iterations ({} warmup) ──",
        args.iterations, args.warmup
    );
    let poll = Duration::from_millis(args.poll_interval_ms);
    let timeout = Duration::from_millis(args.row_timeout_ms);

    let mut samples_ms: Vec<f64> = Vec::with_capacity(args.iterations as usize);
    let mut timeouts = 0u64;

    for i in 0..total {
        let id = args.id_base + i as i64;
        pg.insert_row(id).await.with_context(|| format!("insert id={id}"))?;
        let t_commit = Instant::now();

        match wait_visible(ch, id, poll, timeout).await? {
            Some(_) => {
                let ms = t_commit.elapsed().as_secs_f64() * 1000.0;
                if i >= args.warmup {
                    samples_ms.push(ms);
                }
            }
            None => {
                if i >= args.warmup {
                    timeouts += 1;
                }
                eprintln!("  id={id} did not appear within {}ms", args.row_timeout_ms);
            }
        }
    }

    let summary = Summary::from(&mut samples_ms);
    summary.print("commit→visible latency (ms)");
    if timeouts > 0 {
        println!("  timeouts: {timeouts}");
    }
    println!();
    Ok(summary.to_json_with("timeouts", timeouts))
}

// ---------------------------------------------------------------------------
// Benchmark: sustained-load lag
// ---------------------------------------------------------------------------

async fn run_sustained(args: &Args, pg: Arc<PgClient>, ch: &ChHttp) -> Result<serde_json::Value> {
    let base = args.id_base + 1_000_000;
    println!(
        "── sustained load: target {} rows/s for {}s (probe every {} rows) ──",
        args.rate, args.duration_secs, args.probe_every
    );

    // Probe channel: inserter → poller. Each probe is (id, commit instant).
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<(i64, Instant)>();

    let emit_rows_start = sample_metric(&args.metrics_url, "walshadow_emitter_rows_total").await;
    let wall_start = Instant::now();

    // Inserter task: paced at `rate` for `duration_secs`.
    let inserter = {
        let pg = pg.clone();
        let rate = args.rate.max(1);
        let duration = Duration::from_secs(args.duration_secs);
        let probe_every = args.probe_every.max(1);
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs_f64(1.0 / rate as f64));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Burst);
            let start = Instant::now();
            let mut n: u64 = 0;
            while start.elapsed() < duration {
                tick.tick().await;
                let id = base + n as i64;
                pg.insert_row(id).await.with_context(|| format!("sustained insert id={id}"))?;
                if n % probe_every == 0 {
                    let _ = tx.send((id, Instant::now()));
                }
                n += 1;
            }
            // Always probe the final row so drain time is measurable.
            if n > 0 && (n - 1) % probe_every != 0 {
                let id = base + (n - 1) as i64;
                let _ = tx.send((id, Instant::now()));
            }
            // Capture the insert-window duration before dropping the
            // sender so the achieved insert rate is measured over the
            // window itself — NOT the drain that follows it.
            let insert_elapsed = start.elapsed();
            drop(tx); // close channel → poller drains and exits
            Ok::<(u64, Duration), anyhow::Error>((n, insert_elapsed))
        })
    };

    // Poller: pull oldest probe, wait until visible, record latency.
    let poll = Duration::from_millis(args.poll_interval_ms);
    let timeout = Duration::from_millis(args.row_timeout_ms);
    let mut samples_ms: Vec<f64> = Vec::new();
    let mut timeouts = 0u64;
    let mut last_visible: Option<Instant> = None;
    while let Some((id, t_commit)) = rx.recv().await {
        match wait_visible(ch, id, poll, timeout).await? {
            Some(seen_at) => {
                samples_ms.push(t_commit.elapsed().as_secs_f64() * 1000.0);
                last_visible = Some(seen_at);
            }
            None => timeouts += 1,
        }
    }

    let (rows_inserted, insert_elapsed) = inserter.await.context("inserter join")??;
    // Achieved insert rate: measured over the insert window only.
    let insert_secs = insert_elapsed.as_secs_f64().max(f64::MIN_POSITIVE);
    let achieved_rate = rows_inserted as f64 / insert_secs;
    // `wall` spans insert + drain — the right window for the pipeline's
    // effective end-to-end emit throughput (the metric delta below).
    let wall = wall_start.elapsed().as_secs_f64();
    let drain_ms =
        last_visible.map(|seen| seen.saturating_duration_since(wall_start).as_secs_f64() * 1000.0);

    let emit_rows_end = sample_metric(&args.metrics_url, "walshadow_emitter_rows_total").await;
    let emit_rate = match (emit_rows_start, emit_rows_end) {
        (Some(a), Some(b)) if b >= a && wall > 0.0 => Some((b - a) as f64 / wall),
        _ => None,
    };

    println!("  rows inserted:        {rows_inserted}");
    println!("  insert window:        {insert_secs:.1}s");
    println!("  target insert rate:   {} rows/s", args.rate);
    println!("  achieved insert rate: {achieved_rate:.0} rows/s");
    if let Some(r) = emit_rate {
        println!("  emit rate (CH):       {r:.0} rows/s  (Δwalshadow_emitter_rows_total ÷ insert+drain)");
    } else {
        println!("  emit rate (CH):       n/a  (metrics endpoint unreachable)");
    }
    let summary = Summary::from(&mut samples_ms);
    summary.print("under-load latency (ms)");
    if let Some(d) = drain_ms {
        println!("  last-probe visible at: {d:.0}ms into the run");
    }
    if timeouts > 0 {
        println!("  timeouts: {timeouts}");
    }
    println!();

    let mut json = summary.to_json_with("timeouts", timeouts);
    if let serde_json::Value::Object(ref mut m) = json {
        m.insert("rows_inserted".into(), rows_inserted.into());
        m.insert("insert_secs".into(), insert_secs.into());
        m.insert("target_rate".into(), args.rate.into());
        m.insert("achieved_rate".into(), achieved_rate.into());
        m.insert(
            "emit_rate".into(),
            emit_rate.map(serde_json::Value::from).unwrap_or(serde_json::Value::Null),
        );
    }
    Ok(json)
}

/// Poll ClickHouse for `id` until present or `timeout` elapses. Returns the
/// `Instant` it was first observed, or `None` on timeout. A transient query
/// error is treated as "not yet visible" so a momentary CH hiccup doesn't
/// abort the whole run.
async fn wait_visible(
    ch: &ChHttp,
    id: i64,
    poll: Duration,
    timeout: Duration,
) -> Result<Option<Instant>> {
    let start = Instant::now();
    loop {
        match ch.count_id(id).await {
            Ok(n) if n >= 1 => return Ok(Some(Instant::now())),
            Ok(_) => {}
            Err(_) => {}
        }
        if start.elapsed() >= timeout {
            return Ok(None);
        }
        tokio::time::sleep(poll).await;
    }
}

// ---------------------------------------------------------------------------
// Postgres client
// ---------------------------------------------------------------------------

struct PgClient {
    client: tokio_postgres::Client,
    insert_sql: String,
    table: String,
}

impl PgClient {
    async fn connect(args: &Args) -> Result<Self> {
        let mut conninfo = format!(
            "host={} port={} user={} dbname={}",
            args.pg_host, args.pg_port, args.pg_user, args.pg_dbname
        );
        if let Some(pw) = &args.pg_password {
            conninfo.push_str(&format!(" password={pw}"));
        }
        let (client, connection) = tokio_postgres::connect(&conninfo, NoTls)
            .await
            .with_context(|| format!("tokio_postgres::connect ({conninfo})"))?;
        // The connection object drives the protocol and must be polled.
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("postgres connection error: {e}");
            }
        });
        let insert_sql = format!(
            "INSERT INTO {} (id, name, email) VALUES ($1, $2, $3)",
            args.table
        );
        Ok(Self { client, insert_sql, table: args.table.clone() })
    }

    async fn insert_row(&self, id: i64) -> Result<()> {
        let name = format!("bench-{id}");
        let email = format!("bench-{id}@lat");
        self.client
            .execute(&self.insert_sql, &[&id, &name, &email])
            .await?;
        Ok(())
    }

    async fn delete_bench_rows(&self, id_base: i64) -> Result<()> {
        // Table name is interpolated (can't be a bind param); id_base bound.
        let sql = format!("DELETE FROM {} WHERE id >= $1", self.table);
        self.client.execute(&sql, &[&id_base]).await?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// ClickHouse HTTP client (hand-rolled; no reqwest-class dep)
// ---------------------------------------------------------------------------

struct ChHttp {
    host: String,
    port: u16,
    table: String,
}

impl ChHttp {
    /// POST `sql` to the CH HTTP endpoint and return the trimmed body.
    async fn query(&self, sql: &str) -> Result<String> {
        let mut stream = TcpStream::connect((self.host.as_str(), self.port))
            .await
            .with_context(|| format!("connect ClickHouse {}:{}", self.host, self.port))?;
        let req = format!(
            "POST / HTTP/1.0\r\nHost: {}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            self.host,
            sql.len(),
            sql
        );
        stream.write_all(req.as_bytes()).await?;
        let mut buf = Vec::new();
        stream.read_to_end(&mut buf).await?;
        let txt = String::from_utf8_lossy(&buf);
        let (head, body) = txt
            .split_once("\r\n\r\n")
            .ok_or_else(|| anyhow::anyhow!("malformed HTTP response from ClickHouse"))?;
        let status_ok = head.lines().next().is_some_and(|l| l.contains(" 200 "));
        if !status_ok {
            bail!("ClickHouse HTTP error: {}", body.trim());
        }
        Ok(body.trim().to_string())
    }

    /// `SELECT count() FROM <table> WHERE id = <id>` — no FINAL needed, a
    /// single freshly-inserted row appears exactly once.
    async fn count_id(&self, id: i64) -> Result<u64> {
        let sql = format!("SELECT count() FROM {} WHERE id = {}", self.table, id);
        let body = self.query(&sql).await?;
        body.trim()
            .parse::<u64>()
            .with_context(|| format!("parse count() response {body:?}"))
    }
}

/// Best-effort single Prometheus counter read from `metrics_url`. Returns
/// `None` on any failure (endpoint down, metric absent) so the benchmark
/// degrades gracefully when metrics aren't reachable.
async fn sample_metric(metrics_url: &str, name: &str) -> Option<u64> {
    let (host, port, path) = parse_http_url(metrics_url)?;
    let mut stream = TcpStream::connect((host.as_str(), port)).await.ok()?;
    let req = format!("GET {path} HTTP/1.0\r\nHost: {host}\r\nConnection: close\r\n\r\n");
    stream.write_all(req.as_bytes()).await.ok()?;
    let mut buf = Vec::new();
    stream.read_to_end(&mut buf).await.ok()?;
    let txt = String::from_utf8_lossy(&buf);
    let body = txt.split_once("\r\n\r\n").map(|(_, b)| b).unwrap_or("");
    parse_metric(body, name)
}

/// Parse `http://host:port/path` into parts. Minimal — only what the
/// `--metrics-url` default needs.
fn parse_http_url(url: &str) -> Option<(String, u16, String)> {
    let rest = url.strip_prefix("http://")?;
    let (authority, path) = match rest.split_once('/') {
        Some((a, p)) => (a, format!("/{p}")),
        None => (rest, "/".to_string()),
    };
    let (host, port) = match authority.split_once(':') {
        Some((h, p)) => (h.to_string(), p.parse().ok()?),
        None => (authority.to_string(), 80),
    };
    Some((host, port, path))
}

/// Match `name <v>` or `name{label=...} <v>` in a Prometheus body.
/// (Mirrors `tests/common/bootstrap_ch_fixture.rs::parse_metric`.)
fn parse_metric(body: &str, name: &str) -> Option<u64> {
    for line in body.lines() {
        if line.starts_with('#') {
            continue;
        }
        let head = line.split_once(' ').map(|(h, _)| h)?;
        let stem = head.split_once('{').map(|(s, _)| s).unwrap_or(head);
        if stem != name {
            continue;
        }
        let value_str = line.rsplit_once(' ').map(|(_, v)| v)?;
        if let Ok(v) = value_str.parse::<u64>() {
            return Some(v);
        }
        if let Ok(v) = value_str.parse::<f64>() {
            return Some(v as u64);
        }
    }
    None
}

// ---------------------------------------------------------------------------
// Stats
// ---------------------------------------------------------------------------

struct Summary {
    n: usize,
    min: f64,
    p50: f64,
    p90: f64,
    p99: f64,
    max: f64,
    mean: f64,
}

impl Summary {
    /// Sorts `samples` in place and computes nearest-rank percentiles.
    fn from(samples: &mut Vec<f64>) -> Self {
        if samples.is_empty() {
            return Self { n: 0, min: 0.0, p50: 0.0, p90: 0.0, p99: 0.0, max: 0.0, mean: 0.0 };
        }
        samples.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let n = samples.len();
        let pct = |p: f64| -> f64 {
            // nearest-rank: ceil(p/100 * n), 1-based, clamped.
            let rank = ((p / 100.0) * n as f64).ceil() as usize;
            samples[rank.clamp(1, n) - 1]
        };
        let mean = samples.iter().sum::<f64>() / n as f64;
        Self {
            n,
            min: samples[0],
            p50: pct(50.0),
            p90: pct(90.0),
            p99: pct(99.0),
            max: samples[n - 1],
            mean,
        }
    }

    fn print(&self, title: &str) {
        if self.n == 0 {
            println!("  {title}: no samples");
            return;
        }
        println!("  {title}:");
        println!("    n     {}", self.n);
        println!("    min   {:.2}", self.min);
        println!("    p50   {:.2}", self.p50);
        println!("    p90   {:.2}", self.p90);
        println!("    p99   {:.2}", self.p99);
        println!("    max   {:.2}", self.max);
        println!("    mean  {:.2}", self.mean);
    }

    fn to_json_with(&self, extra_key: &str, extra_val: u64) -> serde_json::Value {
        serde_json::json!({
            "n": self.n,
            "min_ms": self.min,
            "p50_ms": self.p50,
            "p90_ms": self.p90,
            "p99_ms": self.p99,
            "max_ms": self.max,
            "mean_ms": self.mean,
            extra_key: extra_val,
        })
    }
}
