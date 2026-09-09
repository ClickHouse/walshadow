use std::collections::BTreeMap;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use clap::{Args, ValueEnum};
use serde_json::json;
use tokio::time::{MissedTickBehavior, timeout};

use crate::{ChHttp, Destination, PgConfig, pg_connect};

#[derive(Clone, Copy, Debug, ValueEnum)]
pub enum InitialLoadMode {
    Copy,
    BaseBackup,
    ObjectStore,
}

impl InitialLoadMode {
    fn config_value(self) -> &'static str {
        match self {
            Self::Copy => "copy",
            Self::BaseBackup => "base_backup",
            Self::ObjectStore => "object_store",
        }
    }
}

#[derive(Args, Debug)]
pub struct InitialLoadArgs {
    #[arg(long, value_enum, default_value = "base-backup")]
    pub initial_load_mode: InitialLoadMode,
    #[arg(long, default_value_t = 25_000_000, value_parser = clap::value_parser!(i64).range(1..))]
    pub seed_rows: i64,
    #[arg(long, default_value_t = 100_000, value_parser = clap::value_parser!(i64).range(1..))]
    pub seed_batch_rows: i64,
    /// Calibration target, report suggested row count without changing measured work
    #[arg(long, default_value_t = 300, value_parser = clap::value_parser!(u64).range(1..))]
    pub target_load_secs: u64,
    /// Uncompressed payload bytes per row, excluding bigint key
    #[arg(long, default_value_t = 128, value_parser = clap::value_parser!(u32).range(1..=1048576))]
    pub row_width: u32,
    #[arg(long, default_value = "ws_bench")]
    pub seed_schema: String,
    #[arg(long, default_value = "demo")]
    pub initial_load_database: String,
    #[arg(long, default_value = "walshadow")]
    pub runtime_config_schema: String,
    #[arg(long, default_value_t = 1800, value_parser = clap::value_parser!(u64).range(1..))]
    pub initial_load_timeout_secs: u64,
    /// Required for load benchmarks, plain HTTP Prometheus endpoint
    #[arg(long)]
    pub metrics_url: Option<String>,
    /// Run after seed, before opt-in, e.g. create object-store backup
    #[arg(long)]
    pub prepare_cmd: Option<String>,
    /// Bootstrap only: stop daemon before opt-in, return once process exits
    #[arg(long)]
    pub stop_cmd: Option<String>,
    /// Bootstrap only: reset shadow data and start daemon, return after launching
    #[arg(long)]
    pub reset_cmd: Option<String>,
}

async fn hook(
    command: &str,
    table: &str,
    destination: &str,
    bootstrap_config: &str,
    duration: Duration,
) -> Result<()> {
    let mut child = tokio::process::Command::new("sh")
        .args(["-c", command])
        .env("BENCH_SOURCE_TABLE", table)
        .env("BENCH_DESTINATION_TABLE", destination)
        .env("BENCH_BOOTSTRAP_CONFIG", bootstrap_config)
        .kill_on_drop(true)
        .spawn()
        .context("spawn benchmark hook")?;
    let status = timeout(duration, child.wait())
        .await
        .context("benchmark hook timeout")??;
    if !status.success() {
        bail!("benchmark hook failed: {status}");
    }
    Ok(())
}

fn ident(value: &str) -> Result<String> {
    if value.is_empty() || value.len() > 63 || value.contains('\0') {
        bail!("invalid identifier {value:?}");
    }
    Ok(format!("\"{}\"", value.replace('"', "\"\"")))
}

fn bootstrap_config(schema: &str, table: &str, database: &str) -> String {
    format!(
        "[namespace.{}]\ntarget_database = {}\nauto_create = true\ninitial_load = \"base_backup\"\n\n[table.{}.{}]\nreplicate = true\ntarget_database = {}\n",
        json!(schema),
        json!(database),
        json!(schema),
        json!(table),
        json!(database),
    )
}

fn metrics_values(body: &str) -> BTreeMap<String, f64> {
    body.lines()
        .filter(|line| line.starts_with("walshadow_"))
        .filter_map(|line| {
            let (key, value) = line.rsplit_once(' ')?;
            let value: f64 = value.parse().ok()?;
            value.is_finite().then(|| (key.to_owned(), value))
        })
        .collect()
}

struct Metrics {
    client: ChHttp,
    path: String,
}

fn parse_lsn(value: &str) -> Result<u64> {
    let (high, low) = value.split_once('/').context("invalid LSN")?;
    Ok((u32::from_str_radix(high, 16)? as u64) << 32 | u32::from_str_radix(low, 16)? as u64)
}

fn metric_deltas(
    before: &BTreeMap<String, f64>,
    after: &BTreeMap<String, f64>,
) -> BTreeMap<String, f64> {
    after
        .iter()
        .filter(|(key, _)| key.starts_with("walshadow_stage_") && key.contains("_total{"))
        .map(|(key, value)| (key.clone(), value - before.get(key).unwrap_or(&0.0)))
        .collect()
}

fn lifecycle_complete(
    bootstrap: bool,
    mode: InitialLoadMode,
    before: &BTreeMap<String, f64>,
    after: &BTreeMap<String, f64>,
) -> bool {
    let finished = |stage| {
        let key = format!("walshadow_stage_completed_total{{stage=\"{stage}\"}}");
        after
            .get(&key)
            .is_some_and(|value| *value > *before.get(&key).unwrap_or(&0.0))
    };
    if bootstrap {
        finished("bootstrap") && finished("shadow_replay")
    } else {
        let pass = match mode {
            InitialLoadMode::Copy => finished("copy") && finished("insert_flush"),
            InitialLoadMode::BaseBackup | InitialLoadMode::ObjectStore => {
                finished("publish") && finished("settle")
            }
        };
        pass && after.get("walshadow_config_backfills_pending") == Some(&0.0)
    }
}

impl Metrics {
    fn new(value: &str) -> Result<Self> {
        let url = url::Url::parse(value)?;
        if url.scheme() != "http" || !url.username().is_empty() || url.password().is_some() {
            bail!("--metrics-url requires unauthenticated http://host:port/path");
        }
        let host = url.host_str().context("metrics URL needs host")?;
        let path = match url.query() {
            Some(query) => format!("{}?{query}", url.path()),
            None => url.path().to_owned(),
        };
        Ok(Self {
            client: ChHttp::new(host.to_owned(), url.port().unwrap_or(80), String::new()),
            path,
        })
    }

    async fn scrape(&self) -> Result<BTreeMap<String, f64>> {
        let body = timeout(Duration::from_secs(2), self.client.get(&self.path)).await??;
        Ok(metrics_values(&body))
    }
}

pub async fn run(
    pg_cfg: &PgConfig,
    ch_host: String,
    ch_port: u16,
    count_interval_ms: u64,
    bootstrap: bool,
    args: &InitialLoadArgs,
) -> Result<()> {
    if bootstrap != args.reset_cmd.is_some() || bootstrap != args.stop_cmd.is_some() {
        bail!(
            "--bench bootstrap requires --stop-cmd and --reset-cmd; initial-load cannot stop or reset daemon"
        );
    }
    let schema = ident(&args.seed_schema)?;
    let config = ident(&args.runtime_config_schema)?;
    let database = ident(&args.initial_load_database)?;
    let metrics = Metrics::new(
        args.metrics_url
            .as_deref()
            .context("load benchmarks require --metrics-url")?,
    )?;
    let preflight = metrics.scrape().await.context("metrics preflight")?;
    let previous_epoch = *preflight.get("walshadow_stage_epoch_seconds").context(
        "daemon lacks stage instrumentation, rebuild measured revision with stage counters",
    )?;
    let mut harness_stages = BTreeMap::new();
    let name = format!(
        "bl_{}_{:x}",
        std::process::id(),
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
    );
    let table = format!("{schema}.{}", ident(&name)?);
    let ch_table = format!("{database}.{}", ident(&name)?);
    let bootstrap_config = bootstrap_config(&args.seed_schema, &name, &args.initial_load_database);
    let dest = ChHttp::new(ch_host, ch_port, ch_table.clone());
    timeout(Duration::from_secs(10), dest.preflight()).await??;
    let mut pg = pg_connect(pg_cfg).await?;
    let seed_started = Instant::now();
    let tx = pg.transaction().await?;
    tx.batch_execute(&format!("CREATE SCHEMA IF NOT EXISTS {schema}"))
        .await?;
    // Namespace defaults must not stream seed rows before opt-in
    tx.execute(
        &format!(
            "INSERT INTO {config}.config_namespace (namespace, target_database, auto_create)
                  VALUES ($1, $2, false) ON CONFLICT (namespace) DO NOTHING"
        ),
        &[&args.seed_schema, &args.initial_load_database],
    )
    .await?;
    tx.execute(
        &format!(
            "INSERT INTO {config}.config_table
                  (namespace, relname, replicate, target_database, initial_load)
                  VALUES ($1, $2, false, $3, 'none')"
        ),
        &[&args.seed_schema, &name, &args.initial_load_database],
    )
    .await?;
    tx.batch_execute(&format!(
        "CREATE TABLE {table} (id bigint PRIMARY KEY, payload text NOT NULL);
         ALTER TABLE {table} ALTER COLUMN payload SET STORAGE EXTERNAL"
    ))
    .await?;
    tx.commit().await?;
    let mut seeded = 0;
    while seeded < args.seed_rows {
        let end = seeded
            .saturating_add(args.seed_batch_rows)
            .min(args.seed_rows);
        pg.execute(
            &format!(
                "INSERT INTO {table} SELECT id, left(repeat(md5(id::text), $3), $4)
                FROM generate_series($1::bigint, $2::bigint) id"
            ),
            &[
                &(seeded + 1),
                &end,
                &((args.row_width as i32 + 31) / 32),
                &(args.row_width as i32),
            ],
        )
        .await
        .context("seed initial-load table")?;
        seeded = end;
        println!(
            "BENCH_JSON {}",
            json!({"event": "seed_progress", "rows": seeded, "at": seed_started.elapsed().as_secs_f64()})
        );
    }
    let seed_secs = seed_started.elapsed().as_secs_f64();
    harness_stages.insert("seed", seed_secs);
    let sizes = pg
        .query_one(
            "SELECT pg_table_size($1::text::regclass), pg_total_relation_size($1::text::regclass),
                pg_database_size(current_database())",
            &[&table],
        )
        .await?;
    let table_bytes: i64 = sizes.get(0);
    let total_relation_bytes: i64 = sizes.get(1);
    let database_bytes: i64 = sizes.get(2);
    println!(
        "initial-load: {table}, {} rows, {} payload bytes/row, mode {}",
        args.seed_rows,
        args.row_width,
        args.initial_load_mode.config_value()
    );
    println!(
        "seed: {seed_secs:.3}s, table {table_bytes} bytes, including indexes {total_relation_bytes} bytes, source database {database_bytes} bytes"
    );
    println!(
        "BENCH_JSON {}",
        json!({
            "event": "seed", "table": table, "destination": ch_table,
            "seed_secs": seed_secs, "table_bytes": table_bytes,
            "total_relation_bytes": total_relation_bytes, "database_bytes": database_bytes,
        })
    );
    if let Some(command) = &args.stop_cmd {
        let stage = Instant::now();
        hook(
            command,
            &table,
            &ch_table,
            &bootstrap_config,
            Duration::from_secs(args.initial_load_timeout_secs),
        )
        .await?;
        if metrics.scrape().await.is_ok() {
            bail!("--stop-cmd left metrics endpoint reachable, daemon must stop before opt-in");
        }
        harness_stages.insert("stop", stage.elapsed().as_secs_f64());
    }
    if let Some(command) = &args.prepare_cmd {
        let stage = Instant::now();
        hook(
            command,
            &table,
            &ch_table,
            &bootstrap_config,
            Duration::from_secs(args.initial_load_timeout_secs),
        )
        .await?;
        harness_stages.insert("prepare", stage.elapsed().as_secs_f64());
    }
    if timeout(
        Duration::from_secs(10),
        dest.query(&format!("EXISTS TABLE {ch_table}")),
    )
    .await??
        != "0"
    {
        bail!("destination {ch_table} exists before opt-in, seed may already be replicating");
    }
    let before = if bootstrap {
        BTreeMap::new()
    } else {
        let stage = Instant::now();
        let lsn: String = pg
            .query_one("SELECT pg_current_wal_insert_lsn()::text", &[])
            .await?
            .get(0);
        pg.simple_query("SELECT pg_switch_wal()").await?;
        let target = parse_lsn(&lsn)?;
        let until = Instant::now() + Duration::from_secs(args.initial_load_timeout_secs);
        loop {
            let sample = metrics.scrape().await.context("seed drain metrics")?;
            if sample.get("walshadow_stage_epoch_seconds") != Some(&previous_epoch) {
                bail!("daemon restarted while seeding");
            }
            if sample
                .get("walshadow_floor_lsn")
                .is_some_and(|value| *value >= target as f64)
                && sample.get("walshadow_config_backfills_pending") == Some(&0.0)
            {
                harness_stages.insert("seed_wal_drain", stage.elapsed().as_secs_f64());
                break sample;
            }
            if Instant::now() >= until {
                bail!("seed WAL drain timeout at {lsn}");
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    };
    let tx = pg.transaction().await?;
    let mode = (!bootstrap).then(|| args.initial_load_mode.config_value());
    tx.execute(
        &format!(
            "UPDATE {config}.config_table SET replicate = true, initial_load = $3
                  WHERE namespace = $1 AND relname = $2"
        ),
        &[&args.seed_schema, &name, &mode],
    )
    .await?;
    let commit_started = Instant::now();
    tx.commit().await?;
    let started = Instant::now();
    let commit_ack_secs = commit_started.elapsed().as_secs_f64();
    harness_stages.insert("opt_in_commit", commit_ack_secs);
    let deadline = started + Duration::from_secs(args.initial_load_timeout_secs);
    let reset_secs = if let Some(command) = &args.reset_cmd {
        hook(
            command,
            &table,
            &ch_table,
            &bootstrap_config,
            deadline.saturating_duration_since(Instant::now()),
        )
        .await?;
        Some(started.elapsed().as_secs_f64())
    } else {
        tokio::time::timeout_at(deadline.into(), pg.simple_query("SELECT pg_switch_wal()"))
            .await
            .context("WAL switch timeout")??;
        harness_stages.insert("wal_switch", started.elapsed().as_secs_f64());
        None
    };
    if let Some(secs) = reset_secs {
        harness_stages.insert("reset_launch", secs);
    }
    let mut tick = tokio::time::interval(Duration::from_millis(count_interval_ms.max(1)));
    tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut curve = vec![(0.0, 0_u64)];
    let mut metric_samples = Vec::new();
    if !bootstrap {
        metric_samples.push((0.0, before.clone()));
    }
    let mut count_errors = 0_u64;
    let mut metrics_errors = 0_u64;
    let mut last_error = None;
    let mut complete_at = None;
    let mut settled_at = None;
    let mut epoch = if bootstrap {
        None
    } else {
        Some(previous_epoch)
    };
    let mut last_metrics_at = None;
    let mut latest = BTreeMap::new();
    loop {
        tick.tick().await;
        if Instant::now() >= deadline {
            break;
        }
        let query = async {
            match dest.count_all().await {
                Ok(rows) => Ok(rows),
                Err(error) if error.to_string().contains("UNKNOWN_TABLE") => Ok(0),
                Err(error) => Err(error),
            }
        };
        let query_deadline = deadline.min(Instant::now() + Duration::from_secs(10));
        let count = tokio::time::timeout_at(query_deadline.into(), query).await;
        let at = started.elapsed().as_secs_f64();
        match count {
            Ok(Ok(rows)) => {
                curve.push((at, rows));
                println!(
                    "BENCH_JSON {}",
                    json!({"event": "count", "at": at, "rows": rows})
                );
                if rows >= args.seed_rows as u64 && complete_at.is_none() {
                    complete_at = Some(at);
                }
            }
            error => {
                count_errors += 1;
                last_error = Some(format!("{error:?}"));
            }
        }
        if last_metrics_at.is_none_or(|last: Instant| last.elapsed() >= Duration::from_secs(1)) {
            last_metrics_at = Some(Instant::now());
            match metrics.scrape().await {
                Ok(values) => {
                    let current_epoch = values.get("walshadow_stage_epoch_seconds").copied();
                    if current_epoch.is_none()
                        || (bootstrap && current_epoch == Some(previous_epoch))
                        || (epoch.is_some() && epoch != current_epoch)
                    {
                        last_error = Some("unexpected daemon identity during measurement".into());
                        break;
                    }
                    epoch = current_epoch;
                    latest = values;
                    let at = started.elapsed().as_secs_f64();
                    println!(
                        "BENCH_JSON {}",
                        json!({"event": "metrics", "at": at, "values": latest})
                    );
                    metric_samples.push((at, latest.clone()));
                }
                Err(_) => metrics_errors += 1,
            }
        }
        if complete_at.is_some()
            && lifecycle_complete(bootstrap, args.initial_load_mode, &before, &latest)
        {
            settled_at = Some(started.elapsed().as_secs_f64());
            break;
        }
    }
    // Count alone can hide missing IDs behind duplicate delivery
    let verify_started = Instant::now();
    let verified = if complete_at.is_some() {
        match timeout(Duration::from_secs(60), dest.query(&format!(
            "SELECT count(), uniqExact(id), min(id), max(id), sum(length(payload)) FROM {ch_table}"
        ))).await {
            Ok(Ok(value)) => value,
            result => format!("verification failed: {result:?}"),
        }
    } else {
        String::new()
    };
    let expected = format!(
        "{}\t{}\t1\t{}\t{}",
        args.seed_rows,
        args.seed_rows,
        args.seed_rows,
        args.seed_rows as u128 * args.row_width as u128
    );
    harness_stages.insert("verification", verify_started.elapsed().as_secs_f64());
    let complete = settled_at.is_some() && verified == expected;
    let elapsed = complete.then_some(complete_at).flatten();
    let rows_per_sec = elapsed.map(|secs| args.seed_rows as f64 / secs);
    let mib_per_sec = elapsed.map(|secs| table_bytes as f64 / 1048576.0 / secs);
    println!(
        "BENCH_JSON {}",
        json!({
            "bench": if bootstrap { "bootstrap" } else { "initial-load" },
            "mode": if bootstrap { "reset-hook" } else { args.initial_load_mode.config_value() },
            "reset_secs": reset_secs,
            "timing_origin": "opt-in commit acknowledgement, includes WAL switch or reset hook",
            "table": table, "destination": ch_table, "expected_rows": args.seed_rows,
            "row_width": args.row_width, "seed_secs": seed_secs, "table_bytes": table_bytes,
            "total_relation_bytes": total_relation_bytes, "database_bytes": database_bytes,
            "count_interval_ms": count_interval_ms.max(1), "commit_ack_secs": commit_ack_secs,
            "elapsed_secs": elapsed, "rows_per_sec": rows_per_sec, "mib_per_sec": mib_per_sec,
            "curve": curve, "complete": complete,
            "settled_secs": settled_at, "harness_stage_secs": harness_stages,
            "daemon_stage_deltas": metric_deltas(&before, &latest),
            "seed_batch_rows": args.seed_batch_rows,
            "target_load_secs": args.target_load_secs,
            "suggested_seed_rows": elapsed.map(|secs| (args.seed_rows as f64 * args.target_load_secs as f64 / secs).ceil() as u64),
            "measurement_version": 2,
            "seed_wal_drained": !bootstrap,
            "count_errors": count_errors, "last_count_error": last_error,
            "metrics_errors": metrics_errors, "metrics": metric_samples,
            "verification": verified,
        })
    );
    if !complete {
        bail!("initial-load incomplete or row verification failed for {table}: {verified:?}");
    }
    println!(
        "initial-load visible: {:.3}s, {:.0} rows/s, {:.2} table MiB/s; settled: {:.3}s",
        elapsed.unwrap(),
        rows_per_sec.unwrap(),
        mib_per_sec.unwrap(),
        settled_at.unwrap()
    );
    println!(
        "retained {table}; table MiB/s excludes indexes, includes TOAST, does not measure full backup bytes"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn completion_requires_new_stage_work_and_settlement() {
        let before = metrics_values(
            "walshadow_stage_completed_total{stage=\"publish\"} 2\nwalshadow_stage_completed_total{stage=\"settle\"} 2\nwalshadow_config_backfills_pending 0\n",
        );
        assert!(!lifecycle_complete(
            false,
            InitialLoadMode::BaseBackup,
            &before,
            &before
        ));
        let mut after = before.clone();
        after.insert(
            "walshadow_stage_completed_total{stage=\"publish\"}".into(),
            3.0,
        );
        assert!(!lifecycle_complete(
            false,
            InitialLoadMode::BaseBackup,
            &before,
            &after
        ));
        after.insert(
            "walshadow_stage_completed_total{stage=\"settle\"}".into(),
            3.0,
        );
        assert!(lifecycle_complete(
            false,
            InitialLoadMode::BaseBackup,
            &before,
            &after
        ));
        after.insert("walshadow_config_backfills_pending".into(), 1.0);
        assert!(!lifecycle_complete(
            false,
            InitialLoadMode::BaseBackup,
            &before,
            &after
        ));
    }

    #[test]
    fn bootstrap_requires_snapshot_and_shadow_replay() {
        let empty = BTreeMap::new();
        let mut after = metrics_values(
            "walshadow_stage_completed_total{stage=\"copy\"} 1\nwalshadow_config_backfills_pending 0\n",
        );
        assert!(!lifecycle_complete(
            true,
            InitialLoadMode::Copy,
            &empty,
            &after
        ));
        after.insert(
            "walshadow_stage_completed_total{stage=\"bootstrap\"}".into(),
            1.0,
        );
        assert!(!lifecycle_complete(
            true,
            InitialLoadMode::Copy,
            &empty,
            &after
        ));
        after.insert(
            "walshadow_stage_completed_total{stage=\"shadow_replay\"}".into(),
            1.0,
        );
        assert!(lifecycle_complete(
            true,
            InitialLoadMode::Copy,
            &empty,
            &after
        ));
    }

    #[test]
    fn lsn_and_stage_deltas_preserve_boundaries() {
        assert_eq!(parse_lsn("1/FFFFFFFF").unwrap(), 0x1ffffffff);
        assert!(parse_lsn("1/100000000").is_err());
        let before = metrics_values("walshadow_stage_seconds_total{stage=\"copy\"} 50\n");
        let after = metrics_values(
            "walshadow_stage_seconds_total{stage=\"copy\"} 52.5\nwalshadow_stage_active{stage=\"copy\"} 0\n",
        );
        let delta = metric_deltas(&before, &after);
        assert_eq!(delta.len(), 1);
        assert_eq!(delta["walshadow_stage_seconds_total{stage=\"copy\"}"], 2.5);
    }

    #[test]
    fn quote_identifiers() {
        assert_eq!(ident("a\"b").unwrap(), "\"a\"\"b\"");
        assert!(ident("").is_err());
        assert!(ident(&"a".repeat(64)).is_err());
    }

    #[test]
    fn retain_metric_labels_and_skip_nonfinite_values() {
        let parsed = metrics_values(
            "# HELP x\nwalshadow_cpu 3.5\nwalshadow_pending{mode=\"copy\"} 1\nwalshadow_bad NaN\nother 2\n",
        );
        assert_eq!(parsed.len(), 2);
        assert_eq!(parsed["walshadow_pending{mode=\"copy\"}"], 1.0);
    }
}
