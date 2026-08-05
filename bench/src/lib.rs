//! Replication-latency benchmark engine, shared by the `local_bench`
//! (docker-compose stack) and `ec2_bench` (EC2 deployment) binaries.
//!
//! Writes rows to the source Postgres and measures how long until they become
//! visible at a destination — end-to-end replication latency. The destination
//! is abstracted by [`Destination`]: either ClickHouse over HTTP (the
//! walshadow / PeerDB CDC pipelines) or a **standby Postgres** (PG→PG physical
//! streaming replication). All timing uses one host-side monotonic clock
//! (`Instant`): both the "committed" and the "visible" instants are taken on
//! the machine running the bench, so there's no cross-host clock skew. Every
//! sample carries the driver→stack round trip and the insert loop is capped by
//! it, so the driver belongs beside the stack — for the EC2 harness that is the
//! in-VPC runner box (`bench/ec2/stack.sh bench run <name>`).
//!
//! Throughput and latency are separate instruments and must stay that way:
//! [`Destination::count_all`] sampled on a cadence measures throughput, while
//! [`Destination::count_id`] probes measure latency. Deriving one from the other
//! reports the observer's own drain time as the destination's ingest time.
//!
//! Benchmarks: [`run_single_row`], [`run_sustained`], [`run_interleaved`].
//! [`run`] is the high-level entry point; the binaries are thin CLIs that build
//! a [`PgConfig`] + [`DestSpec`] + params and call [`dispatch`].

pub mod suite;

use std::sync::Arc;
use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use clap::{Args, ValueEnum};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_postgres::NoTls;

// ---------------------------------------------------------------------------
// Config + params (decoupled from any CLI parser)
// ---------------------------------------------------------------------------

/// Which benchmark to run.
#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
pub enum Bench {
    SingleRow,
    Sustained,
    Interleaved,
}

/// Which kind of destination the bench polls for visibility.
#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
pub enum DestKind {
    /// ClickHouse over HTTP (walshadow / PeerDB CDC pipelines).
    Clickhouse,
    /// A standby Postgres (PG→PG physical streaming replication).
    Postgres,
}

/// Source Postgres connection + target table.
#[derive(Clone, Debug)]
pub struct PgConfig {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub dbname: String,
    pub password: Option<String>,
    pub table: String,
}

/// ClickHouse HTTP endpoint + table to poll.
#[derive(Clone, Debug)]
pub struct ChConfig {
    pub host: String,
    pub http_port: u16,
    pub table: String,
}

/// How to build the destination the bench polls.
pub enum DestSpec {
    Clickhouse(ChConfig),
    /// Standby Postgres connection (read-only replica of the source).
    Postgres(PgConfig),
}

/// Parameters for the single-row latency distribution.
#[derive(Clone, Copy, Debug)]
pub struct SingleRowParams {
    pub iterations: u64,
    pub warmup: u64,
    pub poll_interval_ms: u64,
    pub row_timeout_ms: u64,
    /// Bench rows live at `id >= id_base`; cleared at startup.
    pub id_base: i64,
}

/// Parameters for the sustained-load benchmark.
#[derive(Clone, Copy, Debug)]
pub struct SustainedParams {
    pub rate: u64,
    pub duration_secs: u64,
    /// Latency-probe cadence. Time-clocked, so probe traffic stays independent
    /// of the throughput under test.
    pub probe_interval_ms: u64,
    /// Destination row-count cadence — the throughput instrument.
    pub count_interval_ms: u64,
    pub concurrency: u64,
    /// Rows per multi-row INSERT/COMMIT; 0 or 1 = single-row commits.
    pub rows_per_xact: u64,
    pub poll_interval_ms: u64,
    pub row_timeout_ms: u64,
    pub id_base: i64,
}

/// Parameters for the interleaved long-transaction benchmark. `threads` long
/// transactions run concurrently (interleaving on the wire); each thread runs
/// `rounds` of them in sequence. A transaction stays open for `xact_secs` (or
/// `rows_per_xact` rows), inserting every `insert_interval_ms`, then commits —
/// at which point the destination should become able to see the whole xact.
#[derive(Clone, Copy, Debug)]
pub struct InterleavedParams {
    pub threads: u64,
    pub rounds: u64,
    /// Count bound: if > 0, each transaction inserts exactly this many rows
    /// then commits (overrides `xact_secs`). If 0, time-bounded by `xact_secs`.
    pub rows_per_xact: u64,
    pub xact_secs: u64,
    pub insert_interval_ms: u64,
    pub poll_interval_ms: u64,
    pub row_timeout_ms: u64,
    pub id_base: i64,
}

// ---------------------------------------------------------------------------
// Destination abstraction
// ---------------------------------------------------------------------------

/// A replication destination the bench polls for row visibility.
#[async_trait::async_trait]
pub trait Destination: Send + Sync {
    /// Cheap connectivity check; fail fast on misconfiguration.
    async fn preflight(&self) -> Result<()>;
    /// Clear the destination for a clean run. For a read-only PG standby this
    /// is a no-op — the primary's TRUNCATE replicates in on its own.
    async fn clear(&self) -> Result<()>;
    /// Count rows carrying this id — the visibility probe.
    async fn count_id(&self, id: i64) -> Result<u64>;
    /// Count every row in the destination table — the throughput instrument.
    /// Sampled on a fixed cadence, so its cost stays independent of load.
    async fn count_all(&self) -> Result<u64>;
    /// Human-readable endpoint for the header line.
    fn endpoint(&self) -> String;
}

// ---------------------------------------------------------------------------
// High-level entry point + CLI
// ---------------------------------------------------------------------------

/// Every benchmark knob, shared by both bench binaries via `#[command(flatten)]`.
/// A binary owns only how it resolves the source/destination hosts (left as
/// `Option`s here and filled in by the binary — localhost for the compose
/// stack, state.env lookup for the EC2 deployment).
#[derive(Args, Debug)]
pub struct CommonArgs {
    /// Which benchmark to run. Required unless the binary offers a whole-suite
    /// mode (see `ec2_bench`'s `--suite`).
    #[arg(long, value_enum)]
    pub bench: Option<Bench>,

    /// Destination kind to poll for visibility.
    #[arg(long, value_enum, default_value_t = DestKind::Clickhouse)]
    pub dest: DestKind,

    // ---- source Postgres ------------------------------------------------
    /// Source host. If unset, the binary supplies its own default.
    #[arg(long)]
    pub pg_host: Option<String>,
    #[arg(long, default_value_t = 5432)]
    pub pg_port: u16,
    #[arg(long, default_value = "postgres")]
    pub pg_user: String,
    #[arg(long, default_value = "postgres")]
    pub pg_dbname: String,
    #[arg(long)]
    pub pg_password: Option<String>,
    /// Source table to insert into (also the destination table for a PG standby).
    #[arg(long, default_value = "demo.users")]
    pub table: String,

    // ---- ClickHouse destination -----------------------------------------
    /// ClickHouse host. If unset, the binary supplies its own default.
    #[arg(long)]
    pub ch_host: Option<String>,
    #[arg(long, default_value_t = 8123)]
    pub ch_http_port: u16,
    /// ClickHouse destination table to poll.
    #[arg(long, default_value = "demo.users")]
    pub ch_table: String,

    // ---- standby Postgres destination -----------------------------------
    /// Standby Postgres port (when --dest postgres). Host is resolved by the
    /// binary (e.g. the ec2-pg-standby node); user/dbname/password reuse the
    /// pg-* flags, table reuses --table.
    #[arg(long, default_value_t = 5432)]
    pub dest_pg_port: u16,

    // ---- single-row bench -----------------------------------------------
    #[arg(long, default_value_t = 100)]
    pub iterations: u64,
    #[arg(long, default_value_t = 10)]
    pub warmup: u64,
    /// Poll resolution; commit→visible is rounded up to this. Over a WAN link
    /// keep it ≥ the round-trip so polling doesn't churn connections.
    #[arg(long, default_value_t = 5)]
    pub poll_interval_ms: u64,
    #[arg(long, default_value_t = 30_000)]
    pub row_timeout_ms: u64,

    // ---- sustained bench ------------------------------------------------
    /// Target insert rate (rows/sec).
    #[arg(long, default_value_t = 200)]
    pub rate: u64,
    #[arg(long, default_value_t = 20)]
    pub duration_secs: u64,
    /// Latency-probe cadence. Probes are clocked on time, not row count, so
    /// probe traffic doesn't scale with the throughput being measured. Only one
    /// probe is outstanding at a time, making this a ceiling on the probe rate
    /// rather than the rate itself.
    #[arg(long, default_value_t = 50)]
    pub probe_interval_ms: u64,
    /// Destination row-count cadence — the throughput instrument, independent
    /// of the latency probes.
    #[arg(long, default_value_t = 250)]
    pub count_interval_ms: u64,
    /// Parallel insert connections (sustained). >1 fans the target rate across
    /// N Postgres connections — a single connection serialises on the wire.
    #[arg(long, default_value_t = 1)]
    pub concurrency: u64,

    // ---- interleaved long-transaction bench -----------------------------
    /// Number of concurrent long transactions (threads) — they interleave on
    /// the wire.
    #[arg(long, default_value_t = 2)]
    pub xact_threads: u64,
    /// Long transactions each thread runs back-to-back.
    #[arg(long, default_value_t = 1)]
    pub rounds: u64,
    /// Rows per transaction. Interleaved: if > 0, each xact inserts exactly this
    /// many rows then commits (overrides --xact-secs). Sustained: rows per
    /// multi-row INSERT/COMMIT (0 or 1 = single-row commits).
    #[arg(long, default_value_t = 0)]
    pub rows_per_xact: u64,
    /// How long each transaction stays open before COMMIT (when
    /// --rows-per-xact is 0).
    #[arg(long, default_value_t = 150)]
    pub xact_secs: u64,
    /// Insert cadence inside an open transaction.
    #[arg(long, default_value_t = 100)]
    pub insert_interval_ms: u64,

    /// Where bench ids begin. The source table is TRUNCATEd at the start of
    /// every run (and the destination cleared / replicated-clear), so this just
    /// sets the id origin.
    #[arg(long, default_value_t = 1_000_000)]
    pub id_base: i64,
}

/// Assemble configs/params from parsed [`CommonArgs`] (source + destination
/// hosts already resolved by the caller) and run the selected benchmark.
pub async fn dispatch(c: &CommonArgs, pg_host: String, dest_host: String) -> Result<()> {
    let which = c.bench.context("--bench is required")?;
    let pg_cfg = PgConfig {
        host: pg_host,
        port: c.pg_port,
        user: c.pg_user.clone(),
        dbname: c.pg_dbname.clone(),
        password: c.pg_password.clone(),
        table: c.table.clone(),
    };
    let dest_spec = match c.dest {
        DestKind::Clickhouse => DestSpec::Clickhouse(ChConfig {
            host: dest_host,
            http_port: c.ch_http_port,
            table: c.ch_table.clone(),
        }),
        DestKind::Postgres => DestSpec::Postgres(PgConfig {
            host: dest_host,
            port: c.dest_pg_port,
            user: c.pg_user.clone(),
            dbname: c.pg_dbname.clone(),
            password: c.pg_password.clone(),
            table: c.table.clone(),
        }),
    };
    let single = SingleRowParams {
        iterations: c.iterations,
        warmup: c.warmup,
        poll_interval_ms: c.poll_interval_ms,
        row_timeout_ms: c.row_timeout_ms,
        id_base: c.id_base,
    };
    let sustained = SustainedParams {
        rate: c.rate,
        duration_secs: c.duration_secs,
        probe_interval_ms: c.probe_interval_ms,
        count_interval_ms: c.count_interval_ms,
        concurrency: c.concurrency,
        rows_per_xact: c.rows_per_xact,
        poll_interval_ms: c.poll_interval_ms,
        row_timeout_ms: c.row_timeout_ms,
        id_base: c.id_base,
    };
    let interleaved = InterleavedParams {
        threads: c.xact_threads,
        rounds: c.rounds,
        rows_per_xact: c.rows_per_xact,
        xact_secs: c.xact_secs,
        insert_interval_ms: c.insert_interval_ms,
        poll_interval_ms: c.poll_interval_ms,
        row_timeout_ms: c.row_timeout_ms,
        id_base: c.id_base,
    };
    run(&pg_cfg, dest_spec, which, &single, &sustained, &interleaved).await
}

/// Connect, preflight the destination, clear both sides, print a header, then
/// run the selected benchmark. Only the params for `which` are used.
pub async fn run(
    pg_cfg: &PgConfig,
    dest_spec: DestSpec,
    which: Bench,
    single: &SingleRowParams,
    sustained: &SustainedParams,
    interleaved: &InterleavedParams,
) -> Result<()> {
    let pg = Arc::new(
        PgClient::connect(pg_cfg)
            .await
            .context("connect source Postgres")?,
    );
    let dest: Arc<dyn Destination> = match dest_spec {
        DestSpec::Clickhouse(c) => Arc::new(ChHttp::new(c.host, c.http_port, c.table)),
        DestSpec::Postgres(c) => Arc::new(
            PgDest::connect(&c)
                .await
                .context("connect standby Postgres")?,
        ),
    };

    // Preflight so a misconfigured endpoint fails fast instead of looking like
    // every row "timed out".
    dest.preflight().await?;

    // Clean slate on BOTH sides before the run. Clearing only the source would
    // leave a prior run's rows at the destination at the same ids, and the
    // visibility poll (count by id) would match them instantly — reporting bogus
    // ~0ms latency even when nothing is replicating. TRUNCATE the source, let any
    // in-flight replication of that settle, then clear the destination (a no-op
    // for a read-only PG standby, which receives the TRUNCATE via replication).
    let poll_ms = match which {
        Bench::SingleRow => single.poll_interval_ms,
        Bench::Sustained => sustained.poll_interval_ms,
        Bench::Interleaved => interleaved.poll_interval_ms,
    };
    pg.truncate().await.context("clear source table")?;
    tokio::time::sleep(Duration::from_millis(500)).await;
    dest.clear().await.context("clear destination")?;
    // Verify rather than assume: a PG standby's `clear` is a no-op, so an empty
    // destination there means the replicated TRUNCATE has landed. Completion
    // measurement counts rows, so a non-empty start would silently pass.
    wait_dest_empty(dest.as_ref(), Duration::from_secs(60))
        .await
        .context("destination did not reach zero rows before the run")?;

    println!(
        "replication-latency bench — source {}:{}  →  {}",
        pg_cfg.host,
        pg_cfg.port,
        dest.endpoint()
    );
    println!(
        "latency measured commit→visible (host monotonic clock); poll resolution ≈ {poll_ms}ms\n"
    );

    match which {
        Bench::SingleRow => run_single_row(&pg, dest.as_ref(), single).await?,
        Bench::Sustained => run_sustained(pg_cfg, pg.clone(), dest.clone(), sustained).await?,
        Bench::Interleaved => run_interleaved(pg_cfg, dest.clone(), interleaved).await?,
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Benchmark: single-row latency distribution
// ---------------------------------------------------------------------------

pub async fn run_single_row(
    pg: &PgClient,
    dest: &dyn Destination,
    p: &SingleRowParams,
) -> Result<()> {
    let total = p.warmup + p.iterations;
    println!(
        "── single-row latency: {} iterations ({} warmup) ──",
        p.iterations, p.warmup
    );
    let poll = Duration::from_millis(p.poll_interval_ms);
    let timeout = Duration::from_millis(p.row_timeout_ms);

    let mut samples_ms: Vec<f64> = Vec::with_capacity(p.iterations as usize);
    let mut timeouts = 0u64;

    for i in 0..total {
        let id = p.id_base + i as i64;
        pg.insert_row(id)
            .await
            .with_context(|| format!("insert id={id}"))?;
        let t_commit = Instant::now();

        match wait_visible(dest, id, poll, timeout).await? {
            Some(_) => {
                if i >= p.warmup {
                    samples_ms.push(t_commit.elapsed().as_secs_f64() * 1000.0);
                }
            }
            None => {
                if i >= p.warmup {
                    timeouts += 1;
                }
                eprintln!("  id={id} did not appear within {}ms", p.row_timeout_ms);
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

/// Probe slot: IDLE → ARMED by the clock, ARMED → BUSY by the committing
/// worker, BUSY → IDLE by the observer once the row is visible or times out.
const SLOT_IDLE: u8 = 0;
const SLOT_ARMED: u8 = 1;
const SLOT_BUSY: u8 = 2;

/// Throughput and latency are measured by two independent instruments.
///
/// Throughput comes from [`Destination::count_all`] sampled on a fixed cadence,
/// never from the probes — deriving it from the last probe observation reports
/// observer drain time as ingest time.
///
/// Latency uses a single probe slot: one commit→visible observation outstanding
/// at a time, armed by a clock at `probe_interval_ms`. Nothing queues, so no
/// sample can carry observer wait time. The slot bounds the probe rate at
/// `1 / latency`, which biases the sample toward the fast intervals — ticks that
/// find the slot busy are counted, and the reported sampled fraction is what
/// says how much of the run the distribution actually covers.
pub async fn run_sustained(
    pg_cfg: &PgConfig,
    pg0: Arc<PgClient>,
    dest: Arc<dyn Destination>,
    p: &SustainedParams,
) -> Result<()> {
    let base = p.id_base + 1_000_000;
    let concurrency: i64 = p.concurrency.max(1) as i64;
    let probe_interval = Duration::from_millis(p.probe_interval_ms.max(1));
    let count_interval = Duration::from_millis(p.count_interval_ms.max(1));
    println!(
        "── sustained load: target {} rows/s for {}s across {} conn(s), {} row(s)/txn ──",
        p.rate,
        p.duration_secs,
        concurrency,
        p.rows_per_xact.max(1),
    );
    println!(
        "  throughput: destination row count every {}ms; latency: 1 probe at a time, armed every {}ms",
        count_interval.as_millis(),
        probe_interval.as_millis(),
    );

    // One Postgres connection per inserter worker so inserts run in parallel —
    // a single connection serialises on the wire. Reuse the caller's client as
    // worker 0; open the rest.
    let mut conns: Vec<Arc<PgClient>> = Vec::with_capacity(concurrency as usize);
    conns.push(pg0);
    for w in 1..concurrency {
        conns.push(Arc::new(
            PgClient::connect(pg_cfg)
                .await
                .with_context(|| format!("connect inserter worker {w}"))?,
        ));
    }

    // Probe channel: inserters → observer. The slot admits one probe at a time,
    // so this never holds more than one message and the observer never queues.
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<(i64, Instant)>();
    let slot = Arc::new(AtomicU8::new(SLOT_IDLE));
    // u64::MAX until the producers report their committed total, so the count
    // sampler cannot decide the run is complete while load is still running.
    let expected_rows = Arc::new(AtomicU64::new(u64::MAX));

    let duration = Duration::from_secs(p.duration_secs);
    let poll = Duration::from_millis(p.poll_interval_ms);
    let row_timeout = Duration::from_millis(p.row_timeout_ms);
    let per_worker_rate = (p.rate.max(1) as f64 / concurrency as f64).max(f64::MIN_POSITIVE);
    let rows_per_txn: i64 = p.rows_per_xact.max(1) as i64;
    let load_started_at = Instant::now();

    // Throughput instrument. Samples until the destination holds every
    // committed row, or until the drain allowance runs out.
    let sampler = {
        let dest = dest.clone();
        let expected_rows = expected_rows.clone();
        let hard_stop = load_started_at + duration + row_timeout;
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(count_interval);
            // Cadence is a floor, not a quota: a count slower than the interval
            // must not make the next ticks fire back-to-back to catch up.
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            let mut curve: Vec<(f64, u64)> = Vec::new();
            let mut errors: u64 = 0;
            let mut announced = Duration::ZERO;
            loop {
                tick.tick().await;
                if Instant::now() >= hard_stop {
                    break;
                }
                let Ok(rows) = dest.count_all().await else {
                    errors += 1;
                    continue;
                };
                let at = load_started_at.elapsed();
                curve.push((at.as_secs_f64(), rows));
                if at.saturating_sub(announced) >= Duration::from_secs(2) {
                    announced = at;
                    println!("  … t={:.1}s  destination {rows} rows", at.as_secs_f64());
                }
                if rows >= expected_rows.load(Ordering::Relaxed) {
                    break;
                }
            }
            (curve, errors)
        })
    };

    // Probe clock. Arms the slot on cadence; a tick landing on an outstanding
    // probe is a recorded skip. A tick landing on an already-armed slot is not —
    // that only means no commit has claimed it yet.
    let clock = {
        let slot = slot.clone();
        let stop_at = load_started_at + duration;
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(probe_interval);
            // A starved clock must not burst a backlog of ticks and record them
            // all as skips.
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            let mut ticks: u64 = 0;
            let mut skipped: u64 = 0;
            loop {
                tick.tick().await;
                if Instant::now() >= stop_at {
                    break;
                }
                ticks += 1;
                if slot
                    .compare_exchange(SLOT_IDLE, SLOT_ARMED, Ordering::AcqRel, Ordering::Acquire)
                    .is_err_and(|state| state == SLOT_BUSY)
                {
                    skipped += 1;
                }
            }
            (ticks, skipped)
        })
    };

    let mut handles = Vec::with_capacity(concurrency as usize);
    for (w, conn) in (0i64..).zip(conns) {
        let tx = tx.clone();
        let slot = slot.clone();
        let handle = tokio::spawn(async move {
            // Tick once per transaction; each inserts `rows_per_txn` rows, so
            // the row rate stays `rate` and the txn rate is rate/rows_per_txn.
            let txn_interval = Duration::from_secs_f64(rows_per_txn as f64 / per_worker_rate);
            let mut tick = tokio::time::interval(txn_interval);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Burst);
            let start = Instant::now();
            let mut k: i64 = 0;
            while start.elapsed() < duration {
                tick.tick().await;
                let mut ids = Vec::with_capacity(rows_per_txn as usize);
                for _ in 0..rows_per_txn {
                    ids.push(base + k * concurrency + w);
                    k += 1;
                }
                conn.insert_txn(&ids).await.with_context(|| {
                    format!("sustained insert txn ending id={}", ids.last().unwrap())
                })?;
                // Claim an armed slot, if there is one. One atomic on the commit
                // path, and never a blocking claim — waiting for the observer
                // would throttle offered load and change the workload measured.
                // All rows in this txn share its commit, so the last id stands
                // in for the txn.
                if slot
                    .compare_exchange(SLOT_ARMED, SLOT_BUSY, Ordering::AcqRel, Ordering::Relaxed)
                    .is_ok()
                {
                    let id = *ids.last().expect("rows_per_xact >= 1");
                    let _ = tx.send((id, Instant::now()));
                }
            }
            Ok::<(i64, Duration), anyhow::Error>((k, start.elapsed()))
        });
        handles.push(handle);
    }
    drop(tx); // workers hold their own clones; observer exits when all close

    // Observer. Serial by construction: the slot stays BUSY until this releases
    // it, so at most one probe is ever outstanding and nothing waits in line.
    let mut samples_ms: Vec<f64> = Vec::new();
    let mut timeouts = 0u64;
    while let Some((id, committed_at)) = rx.recv().await {
        let seen = wait_visible(dest.as_ref(), id, poll, row_timeout).await?;
        slot.store(SLOT_IDLE, Ordering::Release);
        match seen {
            Some(at) => {
                samples_ms.push(at.saturating_duration_since(committed_at).as_secs_f64() * 1000.0)
            }
            None => timeouts += 1,
        }
    }

    let mut rows_inserted: i64 = 0;
    let mut insert_elapsed = Duration::ZERO;
    for h in handles {
        let (c, e) = h.await.context("inserter join")??;
        rows_inserted += c;
        insert_elapsed = insert_elapsed.max(e);
    }
    let expected = rows_inserted.max(0) as u64;
    expected_rows.store(expected, Ordering::Relaxed);
    let (curve, count_errors) = sampler.await.context("count sampler join")?;
    let (probe_ticks, skipped_probes) = clock.await.context("probe clock join")?;

    let insert_secs = insert_elapsed.as_secs_f64().max(f64::MIN_POSITIVE);
    let achieved_rate = rows_inserted as f64 / insert_secs;
    let tput = Throughput::from_curve(&curve, expected, insert_secs, achieved_rate);
    let Throughput {
        dest_rows,
        all_visible_at,
        peak_rate,
        max_backlog,
    } = tput;

    println!();
    println!("source load:");
    println!("  rows committed:        {rows_inserted}");
    println!("  insert window:         {insert_secs:.1}s");
    println!("  target rate:           {} rows/s", p.rate);
    println!("  achieved rate:         {achieved_rate:.0} rows/s");
    println!();
    println!("destination throughput:");
    println!("  rows visible:          {dest_rows} / {expected}");
    match all_visible_at {
        Some(at) => {
            println!("  all rows visible at:   {at:.1}s into the run");
            println!(
                "  drain after source:    {:.1}s",
                (at - insert_secs).max(0.0)
            );
            println!(
                "  completion rate:       {:.0} rows/s",
                expected as f64 / at
            );
        }
        None => println!("  all rows visible at:   NOT REACHED within the drain allowance"),
    }
    println!("  peak sampled rate:     {peak_rate:.0} rows/s");
    println!("  max backlog:           {max_backlog:.0} rows");
    println!(
        "  count samples:         {} ({count_errors} query errors)",
        curve.len()
    );
    println!();
    Summary::from(&mut samples_ms).print("sampled commit→visible latency (ms)");
    if timeouts > 0 {
        println!("  probe timeouts: {timeouts}");
    }
    // One probe at a time means the probe rate falls as latency rises, so the
    // slow intervals are the under-sampled ones. State the coverage rather than
    // let the percentiles imply the whole run.
    if probe_ticks > 0 {
        println!(
            "  probe coverage: {skipped_probes} of {probe_ticks} ticks found the slot busy \
             ({:.0}% sampled) — quantiles under-weight the slow intervals",
            100.0 * (probe_ticks - skipped_probes) as f64 / probe_ticks as f64
        );
    }
    println!();

    if all_visible_at.is_none() {
        bail!(
            "destination reached {dest_rows} of {expected} rows — throughput result is a lower bound, not a measurement"
        );
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Benchmark: interleaved long transactions
// ---------------------------------------------------------------------------

pub async fn run_interleaved(
    pg_cfg: &PgConfig,
    dest: Arc<dyn Destination>,
    p: &InterleavedParams,
) -> Result<()> {
    let threads = p.threads.max(1) as i64;
    let rounds = p.rounds.max(1) as i64;
    let interval_ms = p.insert_interval_ms.max(1);
    // Count-bounded (exactly M rows/xact) or time-bounded (xact_secs).
    let rows_cap: Option<i64> = (p.rows_per_xact > 0).then_some(p.rows_per_xact as i64);
    // Each transaction owns a contiguous id block. Size the stride well above
    // the rows one xact could insert so blocks never overlap, even with timing
    // slack: cap if known, else estimate from the time bound; 4× headroom.
    let est_rows = rows_cap.unwrap_or((p.xact_secs * 1000 / interval_ms).max(1) as i64);
    let stride = (est_rows * 4).max(1_000_000);

    let shape = match rows_cap {
        Some(m) => format!("{m} rows each"),
        None => format!("each open {}s (~{est_rows} rows)", p.xact_secs),
    };
    println!(
        "── interleaved long transactions: {threads} concurrent, {rounds} round(s) each, \
         {shape}, inserting every {interval_ms}ms ──"
    );
    println!(
        "  a transaction's rows aren't visible at the destination until COMMIT; latency below is commit→all-visible.\n"
    );

    let poll = Duration::from_millis(p.poll_interval_ms);
    let timeout = Duration::from_millis(p.row_timeout_ms);
    let xact_dur = Duration::from_secs(p.xact_secs);
    let id_base = p.id_base;
    let row_timeout_ms = p.row_timeout_ms;

    // One worker per concurrent transaction. Workers run at the same time, so
    // their INSERTs interleave on the wire.
    let mut handles = Vec::with_capacity(threads as usize);
    for w in 0..threads {
        let pg_cfg = pg_cfg.clone();
        let dest = dest.clone();
        let handle = tokio::spawn(async move {
            let pg = PgClient::connect(&pg_cfg)
                .await
                .with_context(|| format!("connect xact worker {w}"))?;
            let mut results: Vec<(i64, Option<f64>)> = Vec::with_capacity(rounds as usize);
            for r in 0..rounds {
                // Global xact index → disjoint id block for this transaction.
                let xact_index = r * threads + w;
                let block_base = id_base + xact_index * stride;

                pg.begin()
                    .await
                    .with_context(|| format!("BEGIN (worker {w}, round {r})"))?;
                let start = Instant::now();
                let mut tick = tokio::time::interval(Duration::from_millis(interval_ms));
                tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Burst);
                let mut k: i64 = 0;
                loop {
                    // Stop at M rows (count-bounded) or after xact_secs (time-bounded).
                    let done = match rows_cap {
                        Some(m) => k >= m,
                        None => start.elapsed() >= xact_dur,
                    };
                    if done {
                        break;
                    }
                    tick.tick().await;
                    pg.insert_row(block_base + k)
                        .await
                        .with_context(|| format!("insert id={}", block_base + k))?;
                    k += 1;
                }
                pg.commit()
                    .await
                    .with_context(|| format!("COMMIT (worker {w}, round {r})"))?;
                let t_commit = Instant::now();

                if k == 0 {
                    results.push((0, None));
                    continue;
                }
                // A committed xact becomes visible as a unit, so the last id
                // appearing implies the whole transaction is visible.
                let last_id = block_base + k - 1;
                println!(
                    "  worker {w} round {r}: committed {k} rows (ids {block_base}..={last_id}); waiting for visibility…"
                );
                let lat = match wait_visible(dest.as_ref(), last_id, poll, timeout).await? {
                    Some(_) => {
                        let ms = t_commit.elapsed().as_secs_f64() * 1000.0;
                        println!("  worker {w} round {r}: visible {ms:.0}ms after commit");
                        Some(ms)
                    }
                    None => {
                        eprintln!(
                            "  worker {w} round {r}: id={last_id} not visible within {row_timeout_ms}ms"
                        );
                        None
                    }
                };
                results.push((k, lat));
            }
            Ok::<Vec<(i64, Option<f64>)>, anyhow::Error>(results)
        });
        handles.push(handle);
    }

    let mut samples_ms: Vec<f64> = Vec::new();
    let mut timeouts = 0u64;
    let mut total_rows: i64 = 0;
    let mut xacts: u64 = 0;
    for h in handles {
        for (rows, lat) in h.await.context("xact worker join")?? {
            total_rows += rows;
            xacts += 1;
            match lat {
                Some(ms) => samples_ms.push(ms),
                None => timeouts += 1,
            }
        }
    }

    println!();
    println!("  committed transactions: {xacts}");
    println!("  rows total:             {total_rows}");
    Summary::from(&mut samples_ms).print("commit→all-visible latency (ms)");
    if timeouts > 0 {
        println!("  timeouts: {timeouts}");
    }
    println!();
    Ok(())
}

/// Poll until the destination table is empty. A ClickHouse `clear` truncates
/// directly so this returns on the first check; a PG standby only empties once
/// the primary's TRUNCATE replicates in.
pub async fn wait_dest_empty(dest: &dyn Destination, timeout: Duration) -> Result<()> {
    let start = Instant::now();
    loop {
        let rows = dest.count_all().await?;
        if rows == 0 {
            return Ok(());
        }
        if start.elapsed() >= timeout {
            bail!("destination still holds {rows} rows after {:?}", timeout);
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Poll the destination for `id` until present or `timeout` elapses. Returns the
/// `Instant` it was first observed, or `None` on timeout. A transient query
/// error is treated as "not yet visible" so a momentary hiccup doesn't abort
/// the whole run.
pub async fn wait_visible(
    dest: &dyn Destination,
    id: i64,
    poll: Duration,
    timeout: Duration,
) -> Result<Option<Instant>> {
    let start = Instant::now();
    loop {
        if let Ok(n) = dest.count_id(id).await
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
// Source Postgres client (the primary the bench writes to)
// ---------------------------------------------------------------------------

pub struct PgClient {
    client: tokio_postgres::Client,
    insert_sql: String,
    table: String,
}

impl PgClient {
    pub async fn connect(cfg: &PgConfig) -> Result<Self> {
        let client = pg_connect(cfg).await?;
        let insert_sql = format!(
            "INSERT INTO {} (id, name, email) VALUES ($1, $2, $3)",
            cfg.table
        );
        Ok(Self {
            client,
            insert_sql,
            table: cfg.table.clone(),
        })
    }

    pub async fn insert_row(&self, id: i64) -> Result<()> {
        let name = format!("bench-{id}");
        let email = format!("bench-{id}@lat");
        self.client
            .execute(&self.insert_sql, &[&id, &name, &email])
            .await?;
        Ok(())
    }

    /// Insert every id in one multi-row `INSERT` — a single implicit
    /// transaction, so all rows share one COMMIT (and one commit LSN).
    pub async fn insert_txn(&self, ids: &[i64]) -> Result<()> {
        if ids.len() == 1 {
            return self.insert_row(ids[0]).await;
        }
        let mut sql = format!("INSERT INTO {} (id, name, email) VALUES ", self.table);
        let mut names: Vec<String> = Vec::with_capacity(ids.len());
        let mut emails: Vec<String> = Vec::with_capacity(ids.len());
        for (i, &id) in ids.iter().enumerate() {
            if i > 0 {
                sql.push(',');
            }
            let b = i * 3;
            sql.push_str(&format!("(${}, ${}, ${})", b + 1, b + 2, b + 3));
            names.push(format!("bench-{id}"));
            emails.push(format!("bench-{id}@lat"));
        }
        let mut params: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> =
            Vec::with_capacity(ids.len() * 3);
        for i in 0..ids.len() {
            params.push(&ids[i]);
            params.push(&names[i]);
            params.push(&emails[i]);
        }
        self.client.execute(&sql, &params).await?;
        Ok(())
    }

    /// Open an explicit transaction on this connection. Subsequent `insert_row`
    /// calls run inside it until `commit`; their rows stay invisible downstream
    /// until then.
    pub async fn begin(&self) -> Result<()> {
        self.client.batch_execute("BEGIN").await?;
        Ok(())
    }

    pub async fn commit(&self) -> Result<()> {
        self.client.batch_execute("COMMIT").await?;
        Ok(())
    }

    /// Clear the whole source table — full clean slate before a run.
    pub async fn truncate(&self) -> Result<()> {
        self.client
            .batch_execute(&format!("TRUNCATE TABLE {}", self.table))
            .await?;
        Ok(())
    }
}

/// Open a tokio-postgres connection and spawn its protocol driver.
async fn pg_connect(cfg: &PgConfig) -> Result<tokio_postgres::Client> {
    let mut conninfo = format!(
        "host={} port={} user={} dbname={}",
        cfg.host, cfg.port, cfg.user, cfg.dbname
    );
    if let Some(pw) = &cfg.password {
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
    Ok(client)
}

// ---------------------------------------------------------------------------
// Destination: standby Postgres (PG→PG physical replication)
// ---------------------------------------------------------------------------

pub struct PgDest {
    client: tokio_postgres::Client,
    /// Own connection for the throughput instrument. A backend serves one query
    /// at a time, so sharing would put a full-table count ahead of the probes
    /// and charge its scan to measured latency.
    counter: tokio_postgres::Client,
    count_sql: String,
    count_all_sql: String,
    endpoint: String,
}

impl PgDest {
    pub async fn connect(cfg: &PgConfig) -> Result<Self> {
        let client = pg_connect(cfg).await?;
        let counter = pg_connect(cfg).await?;
        Ok(Self {
            client,
            counter,
            count_sql: format!("SELECT count(*) FROM {} WHERE id = $1", cfg.table),
            count_all_sql: format!("SELECT count(*) FROM {}", cfg.table),
            endpoint: format!("postgres standby {}:{}", cfg.host, cfg.port),
        })
    }
}

#[async_trait::async_trait]
impl Destination for PgDest {
    async fn preflight(&self) -> Result<()> {
        let row = self
            .client
            .query_one("SELECT 1", &[])
            .await
            .context("standby preflight (SELECT 1)")?;
        let one: i32 = row.get(0);
        if one != 1 {
            bail!("standby preflight returned {one}, expected 1");
        }
        Ok(())
    }

    /// No-op: a physical standby is read-only; the primary's TRUNCATE replicates
    /// in on its own.
    async fn clear(&self) -> Result<()> {
        Ok(())
    }

    async fn count_id(&self, id: i64) -> Result<u64> {
        let row = self.client.query_one(&self.count_sql, &[&id]).await?;
        let n: i64 = row.get(0);
        Ok(n.max(0) as u64)
    }

    async fn count_all(&self) -> Result<u64> {
        let row = self.counter.query_one(&self.count_all_sql, &[]).await?;
        let n: i64 = row.get(0);
        Ok(n.max(0) as u64)
    }

    fn endpoint(&self) -> String {
        self.endpoint.clone()
    }
}

// ---------------------------------------------------------------------------
// Destination: ClickHouse over HTTP — one short connection per query, no deps.
// ---------------------------------------------------------------------------

pub struct ChHttp {
    host: String,
    port: u16,
    table: String,
}

impl ChHttp {
    pub fn new(host: String, port: u16, table: String) -> Self {
        Self { host, port, table }
    }

    /// POST `sql` to the CH HTTP endpoint and return the trimmed body. Opens a
    /// fresh connection per call (HTTP/1.0, `Connection: close`). Simple; under
    /// very long sustained-overload drains raise `poll_interval_ms` so polling
    /// doesn't churn through sockets.
    pub async fn query(&self, sql: &str) -> Result<String> {
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
}

#[async_trait::async_trait]
impl Destination for ChHttp {
    async fn preflight(&self) -> Result<()> {
        let one = self
            .query("SELECT 1")
            .await
            .context("ClickHouse preflight (SELECT 1)")?;
        if one.trim() != "1" {
            bail!("ClickHouse preflight returned {one:?}, expected \"1\"");
        }
        Ok(())
    }

    async fn clear(&self) -> Result<()> {
        self.query(&format!("TRUNCATE TABLE {}", self.table))
            .await
            .map(|_| ())
    }

    /// `SELECT count() FROM <table> WHERE id = <id>` — no FINAL needed, a single
    /// freshly-inserted row appears exactly once.
    async fn count_id(&self, id: i64) -> Result<u64> {
        let sql = format!("SELECT count() FROM {} WHERE id = {}", self.table, id);
        let body = self.query(&sql).await?;
        body.trim()
            .parse::<u64>()
            .with_context(|| format!("parse count() response {body:?}"))
    }

    /// No FINAL: this counts row versions, so at-least-once redelivery shows up
    /// as an overshoot past the committed total rather than being hidden.
    async fn count_all(&self) -> Result<u64> {
        let body = self
            .query(&format!("SELECT count() FROM {}", self.table))
            .await?;
        body.trim()
            .parse::<u64>()
            .with_context(|| format!("parse count() response {body:?}"))
    }

    fn endpoint(&self) -> String {
        format!("clickhouse {}:{}", self.host, self.port)
    }
}

// ---------------------------------------------------------------------------
// Stats
// ---------------------------------------------------------------------------

/// Destination throughput, derived from a row-count curve rather than from any
/// probe observation.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Throughput {
    /// Last sampled destination count.
    pub dest_rows: u64,
    /// When the destination first held every committed row, in seconds into the
    /// run. `None` means the run never completed and the rates are lower bounds.
    pub all_visible_at: Option<f64>,
    /// Best rate the destination sustained across one sampling interval.
    pub peak_rate: f64,
    /// Largest gap between committed and visible rows over the run.
    pub max_backlog: f64,
}

impl Throughput {
    /// `curve` is `(seconds into the run, destination row count)` in sample
    /// order. `achieved_rate` describes the source: producers are interval
    /// driven, so source progress is linear across `insert_secs`, which is what
    /// makes a per-sample backlog computable without a second instrument.
    pub fn from_curve(
        curve: &[(f64, u64)],
        expected: u64,
        insert_secs: f64,
        achieved_rate: f64,
    ) -> Self {
        Self {
            dest_rows: curve.last().map(|(_, rows)| *rows).unwrap_or(0),
            all_visible_at: curve
                .iter()
                .find(|(_, rows)| *rows >= expected)
                .map(|(at, _)| *at)
                .filter(|at| *at > 0.0),
            peak_rate: curve
                .windows(2)
                .filter_map(|w| {
                    let dt = w[1].0 - w[0].0;
                    (dt > 0.0).then(|| w[1].1.saturating_sub(w[0].1) as f64 / dt)
                })
                .fold(0.0, f64::max),
            max_backlog: curve
                .iter()
                .map(|(at, rows)| {
                    (achieved_rate * at.min(insert_secs)).min(expected as f64) - *rows as f64
                })
                .fold(0.0, f64::max),
        }
    }
}

pub struct Summary {
    pub n: usize,
    pub min: f64,
    pub p50: f64,
    pub p90: f64,
    pub p99: f64,
    pub max: f64,
    pub mean: f64,
}

impl Summary {
    /// Sorts `samples` in place and computes nearest-rank percentiles.
    pub fn from(samples: &mut [f64]) -> Self {
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

    pub fn print(&self, title: &str) {
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
