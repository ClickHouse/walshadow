//! Replication-latency benchmarks against the EC2 walshadow deployment
//! (the three-node stack under `bench/ec2/`: source Postgres → walshadow →
//! ClickHouse). Same engine + CLI as `local_bench` (`../bench.rs`); the
//! only difference is that endpoints are resolved from the terraform-written
//! `state.env` files instead of defaulting to localhost.
//!
//! `--network` picks which IP to read:
//!   * `public`  — instances' public IPs (run from your workstation; note the
//!     workstation↔region RTT is included in commit→visible).
//!   * `private` — VPC-internal IPs (for when this binary is shipped onto a
//!     host in the same VPC; measures pipeline-internal latency).
//!
//! Explicit `--pg-host` / `--ch-host` override the lookup.
//!
//! Examples:
//!   cargo run --release --bin walshadow-ec2-bench -- --bench single-row
//!   cargo run --release --bin walshadow-ec2-bench -- --bench interleaved --xact-secs 150

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use clap::{Parser, ValueEnum};

use walshadow_bench::{CommonArgs, DestKind};

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
enum Network {
    /// Instances' public IPs — run from a workstation outside the VPC.
    Public,
    /// VPC-internal IPs — run from a host inside the VPC.
    Private,
}

#[derive(Parser, Debug)]
#[command(
    name = "walshadow-ec2-bench",
    about = "Measure source-Postgres → ClickHouse replication latency (EC2 deployment)"
)]
struct Args {
    #[command(flatten)]
    common: CommonArgs,

    /// Which IP family to read from the state.env files.
    #[arg(long, value_enum, default_value_t = Network::Public)]
    network: Network,

    /// Directory holding the per-node folders (each with a `state.env`).
    /// Default assumes the current dir is the repo root.
    #[arg(long, default_value = "bench/ec2")]
    state_dir: PathBuf,
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
    walshadow_bench::dispatch(&args.common, pg_host, dest_host).await
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
    fn resolve_host_errors_when_key_absent() {
        let path = temp_state("absent_key", "OTHER=x\n");

        let err = resolve_host(None, &path, Network::Public, "PRIVATE_IP")
            .unwrap_err()
            .to_string();
        assert!(err.contains("no PUBLIC_IP in"), "unexpected error: {err}");

        fs::remove_file(&path).ok();
    }
}
