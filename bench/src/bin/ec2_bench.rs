//! Replication-latency benchmarks against the EC2 walshadow deployment
//! (the three-node stack under `bench/ec2/`: source Postgres → walshadow →
//! ClickHouse). Same engine + CLI as `local_bench` (`../bench.rs`); the
//! only difference is that endpoints are resolved from the terraform-written
//! `state.env` files instead of defaulting to localhost.
//!
//! `bench/ec2/stack.sh bench run <name>` is the entry point: it runs the suite
//! on the in-VPC runner box and copies the results back.
//!
//! `--network` picks which IP to read:
//!   * `private` (default) — VPC-internal IPs, as seen from the runner box.
//!   * `public`  — instances' public IPs, for reaching them from outside the
//!     VPC. Smoke tests only; the WAN round trip lands in every number.
//!
//! Explicit `--pg-host` / `--ch-host` override the lookup.
//!
//! `--suite <name>` runs the four standard shapes instead of one bench, into
//! `<results-dir>/<name>/` (see [`walshadow_bench::suite`]).
//!
//! Examples (on the runner box, where the wrapper supplies the paths):
//!   walshadow-ec2-bench --bench single-row
//!   walshadow-ec2-bench --bench interleaved --xact-secs 150
//!   walshadow-ec2-bench --suite walshadow-run

use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use clap::{ArgGroup, Parser, ValueEnum};

use walshadow_bench::{CommonArgs, DestKind};

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
enum Network {
    /// Instances' public IPs — reaches them from outside the VPC.
    Public,
    /// VPC-internal IPs — run from a host inside the VPC.
    Private,
}

#[derive(Parser, Debug)]
#[command(
    name = "walshadow-ec2-bench",
    about = "Measure source-Postgres → ClickHouse replication latency (EC2 deployment)",
    // Exactly one of a single bench or the whole suite.
    group(ArgGroup::new("what").args(["bench", "suite"]).required(true)),
)]
struct Args {
    #[command(flatten)]
    common: CommonArgs,

    /// Which IP family to read from the state.env files. Private by default:
    /// the bench is meant to run inside the VPC.
    #[arg(long, value_enum, default_value_t = Network::Private)]
    network: Network,

    /// Directory holding the per-node folders (each with a `state.env`).
    /// Default assumes the current dir is the repo root.
    #[arg(long, default_value = "bench/ec2")]
    state_dir: PathBuf,

    /// Run the four standard benchmark shapes into `<results-dir>/<name>/`
    /// instead of the single `--bench`. Refuses an existing folder.
    #[arg(long, value_name = "NAME")]
    suite: Option<String>,

    /// Where `--suite` writes its run folders.
    #[arg(long, default_value = "bench/results")]
    results_dir: PathBuf,

    /// Load duration for the suite's `sustained` + `interleaved` runs. Unset
    /// keeps the quick-pass durations; `interleaved-long` always runs 10 × 30s.
    #[arg(long)]
    run_secs: Option<u64>,
}

impl Args {
    /// `--run-secs` belongs to the suite. A clap `requires = "suite"` would not
    /// catch this: `suite` shares a group with `bench`, so clap counts the
    /// requirement as met whenever either is present.
    fn validate(&self) -> Result<()> {
        if self.run_secs.is_some() && self.suite.is_none() {
            bail!(
                "--run-secs applies to --suite only (a single --bench takes --duration-secs / --xact-secs)"
            );
        }
        Ok(())
    }
}

/// Read `KEY=value` from a shell-style state.env file.
fn read_state_var(path: &Path, key: &str) -> Option<String> {
    let content = std::fs::read_to_string(path).ok()?;
    let prefix = format!("{key}=");
    content
        .lines()
        .map(str::trim)
        .find_map(|l| l.strip_prefix(&prefix))
        .map(|v| v.trim().to_string())
}

/// Resolve a host: explicit flag wins; otherwise read the right key from the
/// node's state.env based on the chosen network.
fn resolve_host(
    explicit: Option<String>,
    state_file: &Path,
    network: Network,
    private_key: &str,
) -> Result<String> {
    if let Some(h) = explicit {
        return Ok(h);
    }
    let key = match network {
        Network::Public => "PUBLIC_IP",
        Network::Private => private_key,
    };
    read_state_var(state_file, key).with_context(|| {
        format!(
            "no {key} in {} — pass the host explicitly or run the provisioner first",
            state_file.display()
        )
    })
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let args = Args::parse();
    args.validate()?;
    let src_state = args.state_dir.join("ec2-source-pg/state.env");
    // source-pg records its private IP under SOURCE_PRIVATE_IP.
    let pg_host = resolve_host(
        args.common.pg_host.clone(),
        &src_state,
        args.network,
        "SOURCE_PRIVATE_IP",
    )?;
    // Destination depends on --dest: ClickHouse node, or the PG standby node.
    // Both record their private IP under PRIVATE_IP; --ch-host overrides either.
    let dest_state = match args.common.dest {
        DestKind::Clickhouse => args.state_dir.join("ec2-clickhouse/state.env"),
        DestKind::Postgres => args.state_dir.join("ec2-pg-standby/state.env"),
    };
    let dest_host = resolve_host(
        args.common.ch_host.clone(),
        &dest_state,
        args.network,
        "PRIVATE_IP",
    )?;

    if let Some(name) = &args.suite {
        // Children get the already-resolved hosts, so the whole suite keeps the
        // endpoints this run started with and each result file records them.
        let common = suite_child_flags(&args.common, &pg_host, &dest_host);
        let failed = walshadow_bench::suite::run(name, &args.results_dir, &common, args.run_secs)?;
        if !failed.is_empty() {
            bail!("failed: {}", failed.join(", "));
        }
        return Ok(());
    }
    walshadow_bench::dispatch(&args.common, pg_host, dest_host).await
}

/// Flags every suite child inherits: the destination kind, the resolved
/// endpoints and the table identity. Bench shapes come from the suite itself.
/// `--pg-password` is not forwarded — the harness's source Postgres uses `trust`
/// auth and the ClickHouse HTTP probe is unauthenticated.
fn suite_child_flags(c: &CommonArgs, pg_host: &str, dest_host: &str) -> Vec<String> {
    [
        (
            "--dest",
            c.dest
                .to_possible_value()
                .expect("dest has no skipped variants")
                .get_name()
                .to_string(),
        ),
        ("--pg-host", pg_host.to_string()),
        ("--pg-port", c.pg_port.to_string()),
        ("--pg-user", c.pg_user.clone()),
        ("--pg-dbname", c.pg_dbname.clone()),
        ("--table", c.table.clone()),
        ("--ch-host", dest_host.to_string()),
        ("--ch-http-port", c.ch_http_port.to_string()),
        ("--ch-table", c.ch_table.clone()),
        ("--dest-pg-port", c.dest_pg_port.to_string()),
    ]
    .into_iter()
    .flat_map(|(flag, value)| [flag.to_string(), value])
    .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::fs;

    /// Write `content` to a uniquely-named temp file and return its path.
    fn temp_state(name: &str, content: &str) -> PathBuf {
        let path = std::env::temp_dir().join(format!("walshadow-ec2-bench-test-{name}.env"));
        fs::write(&path, content).unwrap();
        path
    }

    #[test]
    fn read_state_var_finds_key_and_trims_value() {
        let path = temp_state(
            "read_var",
            "PUBLIC_IP=1.2.3.4\n  SOURCE_PRIVATE_IP=10.0.0.9\nPRIVATE_IP=10.0.0.5  \n",
        );

        assert_eq!(
            read_state_var(&path, "PUBLIC_IP").as_deref(),
            Some("1.2.3.4")
        );
        // leading whitespace on the line is tolerated…
        assert_eq!(
            read_state_var(&path, "SOURCE_PRIVATE_IP").as_deref(),
            Some("10.0.0.9")
        );
        // …and the value itself is trimmed.
        assert_eq!(
            read_state_var(&path, "PRIVATE_IP").as_deref(),
            Some("10.0.0.5")
        );
        assert_eq!(read_state_var(&path, "MISSING"), None);

        fs::remove_file(&path).ok();
    }

    #[test]
    fn read_state_var_missing_file_is_none() {
        let path = std::env::temp_dir().join("walshadow-ec2-bench-test-nonexistent.env");
        assert_eq!(read_state_var(&path, "PUBLIC_IP"), None);
    }

    #[test]
    fn resolve_host_explicit_flag_wins_without_reading_file() {
        // Path does not exist, but the explicit override means it's never read.
        let path = std::env::temp_dir().join("walshadow-ec2-bench-test-unused.env");
        let host = resolve_host(
            Some("explicit-host".to_string()),
            &path,
            Network::Public,
            "PRIVATE_IP",
        )
        .unwrap();
        assert_eq!(host, "explicit-host");
    }

    #[test]
    fn resolve_host_picks_key_by_network() {
        let path = temp_state(
            "by_network",
            "PUBLIC_IP=1.2.3.4\nPRIVATE_IP=10.0.0.5\nSOURCE_PRIVATE_IP=10.0.0.9\n",
        );

        assert_eq!(
            resolve_host(None, &path, Network::Public, "PRIVATE_IP").unwrap(),
            "1.2.3.4"
        );
        assert_eq!(
            resolve_host(None, &path, Network::Private, "PRIVATE_IP").unwrap(),
            "10.0.0.5"
        );
        // private_key is configurable (source-pg uses SOURCE_PRIVATE_IP).
        assert_eq!(
            resolve_host(None, &path, Network::Private, "SOURCE_PRIVATE_IP").unwrap(),
            "10.0.0.9"
        );

        fs::remove_file(&path).ok();
    }

    #[test]
    fn args_demand_exactly_one_of_bench_or_suite() {
        assert!(Args::try_parse_from(["bench"]).is_err());
        assert!(
            Args::try_parse_from(["bench", "--bench", "single-row", "--suite", "x"]).is_err(),
            "--bench and --suite must not combine"
        );
        let suite = Args::try_parse_from(["bench", "--suite", "myrun"]).unwrap();
        assert_eq!(suite.suite.as_deref(), Some("myrun"));
        assert_eq!(suite.common.bench, None);
        assert_eq!(suite.results_dir, PathBuf::from("bench/results"));
    }

    #[test]
    fn network_defaults_to_the_vpc_interior() {
        let args = Args::try_parse_from(["bench", "--suite", "myrun"]).unwrap();

        assert_eq!(args.network, Network::Private);
    }

    #[test]
    fn run_secs_only_applies_to_the_suite() {
        let single =
            Args::try_parse_from(["bench", "--bench", "sustained", "--run-secs", "300"]).unwrap();
        let err = single.validate().unwrap_err().to_string();
        assert!(err.contains("--run-secs applies to --suite"), "{err}");

        let args = Args::try_parse_from(["bench", "--suite", "r", "--run-secs", "300"]).unwrap();
        args.validate().unwrap();
        assert_eq!(args.run_secs, Some(300));
    }

    #[test]
    fn suite_child_flags_carry_dest_and_resolved_endpoints() {
        let args = Args::try_parse_from(["bench", "--suite", "r", "--dest", "postgres"]).unwrap();
        let flags = suite_child_flags(&args.common, "10.0.0.9", "10.0.0.5");
        let joined = flags.join(" ");

        assert!(joined.contains("--dest postgres"), "{joined}");
        assert!(joined.contains("--pg-host 10.0.0.9"), "{joined}");
        assert!(joined.contains("--ch-host 10.0.0.5"), "{joined}");
        assert!(joined.contains("--table demo.users"), "{joined}");
        // No bench shape: the suite supplies that per run.
        assert!(!joined.contains("--bench"), "{joined}");
        // Children must not need the state.env files again.
        assert!(!joined.contains("--state-dir"), "{joined}");
    }

    #[test]
    fn resolve_host_errors_when_key_absent() {
        let path = temp_state("absent_key", "OTHER=x\n");

        let err = resolve_host(None, &path, Network::Public, "PRIVATE_IP")
            .unwrap_err()
            .to_string();
        assert!(err.contains("no PUBLIC_IP in"), "unexpected error: {err}");

        fs::remove_file(&path).ok();
    }
}
