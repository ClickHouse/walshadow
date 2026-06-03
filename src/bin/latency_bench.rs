//! Replication-latency benchmarks for the walshadow demo stack.
//!
//! Writes rows to the `source` Postgres and measures how long until they
//! become visible in ClickHouse — end-to-end replication latency. Runs
//! against the docker/docker-compose.yml stack over host-exposed ports;
//! all timing uses one host-side monotonic clock (`Instant`), so there's
//! no cross-container clock skew (both the "committed" and the "visible"
//! instants are taken here).
//!
//! Two benchmarks, selected with `--bench`:
//!   * `single-row` — insert one row at a time, time commit→visible,
//!     repeat, report a percentile distribution.
//!   * `sustained`  — drive a continuous insert rate across N connections,
//!     sample latency under load, report achieved rate + drain time.
//!
//! Example:
//!   cargo run --release --bin walshadow-latency-bench -- --bench single-row
//!   cargo run --release --bin walshadow-latency-bench -- \
//!       --bench sustained --rate 500 --concurrency 4
//!
//! ClickHouse is queried via a tiny hand-rolled HTTP client (one short
//! connection per query) so no reqwest-class dependency is pulled in.

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
    /// Source table to insert into.
    #[arg(long, default_value = "demo.users")]
    table: String,

    // ---- ClickHouse -----------------------------------------------------
    #[arg(long, default_value = "127.0.0.1")]
    ch_host: String,
    #[arg(long, default_value_t = 8123)]
    ch_http_port: u16,
    /// Destination table to poll.
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
    /// Parallel insert connections. >1 fans the target rate across N
    /// Postgres connections — a single connection serialises on the wire.
    #[arg(long, default_value_t = 1)]
    concurrency: u64,

    /// Bench rows live at `id >= id_base`; cleared at startup so each run
    /// starts clean. Keep clear of the demo's seeded ids (1..=3).
    #[arg(long, default_value_t = 1_000_000)]
    id_base: i64,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let args = Args::parse();

    let pg = Arc::new(
        PgClient::connect(&args)
            .await
            .context("connect source Postgres")?,
    );
    let ch = ChHttp::new(
        args.ch_host.clone(),
        args.ch_http_port,
        args.ch_table.clone(),
    );

    // Preflight so a misconfigured endpoint fails fast instead of looking
    // like every row "timed out".
    let one = ch
        .query("SELECT 1")
        .await
        .context("ClickHouse preflight (SELECT 1)")?;
    if one.trim() != "1" {
        bail!("ClickHouse preflight returned {one:?}, expected \"1\"");
    }

    // Clean slate for our id range so every timed write is a true INSERT
    // and re-runs are idempotent.
    pg.delete_bench_rows(args.id_base)
        .await
        .context("clear prior bench rows")?;

    println!(
        "walshadow-latency-bench — source {}:{}  →  clickhouse {}:{}",
        args.pg_host, args.pg_port, args.ch_host, args.ch_http_port
    );
    println!(
        "latency measured commit→visible (host monotonic clock); poll resolution ≈ {}ms\n",
        args.poll_interval_ms
    );

    match args.bench {
        Bench::SingleRow => run_single_row(&args, &pg, &ch).await?,
        Bench::Sustained => run_sustained(&args, pg.clone(), &ch).await?,
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Benchmark: single-row latency distribution
// ---------------------------------------------------------------------------

async fn run_single_row(args: &Args, pg: &PgClient, ch: &ChHttp) -> Result<()> {
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
        pg.insert_row(id)
            .await
            .with_context(|| format!("insert id={id}"))?;
        let t_commit = Instant::now();

        match wait_visible(ch, id, poll, timeout).await? {
            Some(_) => {
                if i >= args.warmup {
                    samples_ms.push(t_commit.elapsed().as_secs_f64() * 1000.0);
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

    Summary::from(&mut samples_ms).print("commit→visible latency (ms)");
    if timeouts > 0 {
        println!("  timeouts: {timeouts}");
    }
    println!();
    Ok(())
}

// ---------------------------------------------------------------------------
// Benchmark: sustained-load latency
// ---------------------------------------------------------------------------

async fn run_sustained(args: &Args, pg: Arc<PgClient>, ch: &ChHttp) -> Result<()> {
    let base = args.id_base + 1_000_000;
    let concurrency: i64 = args.concurrency.max(1) as i64;
    println!(
        "── sustained load: target {} rows/s for {}s across {} conn(s) (probe every {} rows) ──",
        args.rate, args.duration_secs, concurrency, args.probe_every
    );

    // One Postgres connection per inserter worker so inserts run in
    // parallel — a single connection serialises on the wire. Reuse the
    // caller's client as worker 0; open the rest.
    let mut conns: Vec<Arc<PgClient>> = Vec::with_capacity(concurrency as usize);
    conns.push(pg);
    for w in 1..concurrency {
        conns.push(Arc::new(
            PgClient::connect(args)
                .await
                .with_context(|| format!("connect inserter worker {w}"))?,
        ));
    }

    // Probe channel: inserters → poller. Each probe is (id, commit instant).
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<(i64, Instant)>();
    let wall_start = Instant::now();

    // Spawn `concurrency` inserter workers. Each targets rate/concurrency
    // rows/s; combined they aim for `rate`. Worker w owns the id lattice
    // `base + k*concurrency + w` so ids never collide and stay monotonic
    // within a worker. Each returns its row count and the actual
    // insert-window duration (achieved rate is measured over the window,
    // NOT the drain that follows it).
    let duration = Duration::from_secs(args.duration_secs);
    let probe_every: i64 = args.probe_every.max(1) as i64;
    let per_worker_rate = (args.rate.max(1) as f64 / concurrency as f64).max(f64::MIN_POSITIVE);
    let mut handles = Vec::with_capacity(concurrency as usize);
    for (w, conn) in (0i64..).zip(conns) {
        let tx = tx.clone();
        let handle = tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs_f64(1.0 / per_worker_rate));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Burst);
            let start = Instant::now();
            let mut k: i64 = 0;
            let mut last_id: Option<i64> = None;
            while start.elapsed() < duration {
                tick.tick().await;
                let id = base + k * concurrency + w;
                conn.insert_row(id)
                    .await
                    .with_context(|| format!("sustained insert id={id}"))?;
                if k % probe_every == 0 {
                    let _ = tx.send((id, Instant::now()));
                }
                last_id = Some(id);
                k += 1;
            }
            // Always probe this worker's final row so the drain tail shows.
            if let Some(id) = last_id
                && (k - 1) % probe_every != 0
            {
                let _ = tx.send((id, Instant::now()));
            }
            Ok::<(i64, Duration), anyhow::Error>((k, start.elapsed()))
        });
        handles.push(handle);
    }
    drop(tx); // workers hold their own clones; poller exits when all close

    // Poller: pull each probe, wait until visible, record latency.
    let poll = Duration::from_millis(args.poll_interval_ms);
    let timeout = Duration::from_millis(args.row_timeout_ms);
    let mut samples_ms: Vec<f64> = Vec::new();
    let mut timeouts = 0u64;
    let mut last_visible: Option<Instant> = None;
    let mut processed: u64 = 0;
    while let Some((id, t_commit)) = rx.recv().await {
        match wait_visible(ch, id, poll, timeout).await? {
            Some(seen_at) => {
                samples_ms.push(t_commit.elapsed().as_secs_f64() * 1000.0);
                last_visible = Some(seen_at);
            }
            None => timeouts += 1,
        }
        processed += 1;
        // When load exceeds pipeline capacity the drain runs far past the
        // insert window — print progress so a long (but live) drain isn't
        // mistaken for a hang.
        if processed.is_multiple_of(500) {
            println!(
                "  … drained {processed} probes (last latency {:.0}ms)",
                samples_ms.last().copied().unwrap_or(0.0)
            );
        }
    }

    let mut rows_inserted: i64 = 0;
    let mut insert_elapsed = Duration::ZERO;
    for h in handles {
        let (c, e) = h.await.context("inserter join")??;
        rows_inserted += c;
        insert_elapsed = insert_elapsed.max(e);
    }
    let insert_secs = insert_elapsed.as_secs_f64().max(f64::MIN_POSITIVE);
    let achieved_rate = rows_inserted as f64 / insert_secs;
    let drain_ms =
        last_visible.map(|seen| seen.saturating_duration_since(wall_start).as_secs_f64() * 1000.0);

    println!("  rows inserted:        {rows_inserted}");
    println!("  insert window:        {insert_secs:.1}s");
    println!("  target insert rate:   {} rows/s", args.rate);
    println!("  achieved insert rate: {achieved_rate:.0} rows/s");
    Summary::from(&mut samples_ms).print("under-load latency (ms)");
    if let Some(d) = drain_ms {
        println!("  last-probe visible at: {d:.0}ms into the run");
    }
    if timeouts > 0 {
        println!("  timeouts: {timeouts}");
    }
    println!();
    Ok(())
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
        if let Ok(n) = ch.count_id(id).await
            && n >= 1
        {
            return Ok(Some(Instant::now()));
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
        Ok(Self {
            client,
            insert_sql,
            table: args.table.clone(),
        })
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
// ClickHouse HTTP client — one short connection per query, no extra deps.
// ---------------------------------------------------------------------------

struct ChHttp {
    host: String,
    port: u16,
    table: String,
}

impl ChHttp {
    fn new(host: String, port: u16, table: String) -> Self {
        Self { host, port, table }
    }

    /// POST `sql` to the CH HTTP endpoint and return the trimmed body.
    /// Opens a fresh connection per call (HTTP/1.0, `Connection: close`).
    /// Simple; under very long sustained-overload drains raise
    /// `--poll-interval-ms` so polling doesn't churn through sockets.
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
        if !head.lines().next().is_some_and(|l| l.contains(" 200 ")) {
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
    fn from(samples: &mut [f64]) -> Self {
        if samples.is_empty() {
            return Self {
                n: 0,
                min: 0.0,
                p50: 0.0,
                p90: 0.0,
                p99: 0.0,
                max: 0.0,
                mean: 0.0,
            };
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
}
