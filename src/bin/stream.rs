//! `walshadow-stream` — full WAL capture pipeline.
//!
//! Connects to a source PG in replication mode, issues
//! `IDENTIFY_SYSTEM` then `START_REPLICATION PHYSICAL` (optionally
//! bound to a permanent slot), runs a frame loop that filters every
//! WAL byte, writes filtered segments into the target directory
//! shadow PG reads via its `restore_command`.
//!
//! Usage:
//! ```text
//! walshadow-stream \
//!     --host /tmp/source_sock --port 5432 --user postgres --dbname postgres \
//!     --out-dir /var/lib/walshadow/filtered \
//!     [--slot walshadow_phys] \
//!     [--start-lsn 0/16B3750]
//! ```
//!
//! Without `--start-lsn`, the daemon starts at the segment boundary
//! that contains the source's current `pg_current_wal_lsn`. With it,
//! resumes from the supplied LSN rounded down to a segment boundary.
//!
//! Shutdown: Ctrl-C / SIGTERM stops the pump cleanly and writes the
//! current partial segment (if any) so subsequent runs can pick up
//! mid-segment via `--start-lsn`.

use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use clap::Parser;
use wal_rs::pg::replication::conn::PgConfig;
use wal_rs::pg::replication::tls::SslMode;
use walshadow::source_feed::SourceFeed;
use walshadow::wal_stream::{CollectingRecordSink, DirSegmentSink, WAL_SEG_SIZE, WalStream};

#[derive(Debug, Parser)]
#[command(
    name = "walshadow-stream",
    about = "Stream + filter physical WAL from source PG."
)]
struct Args {
    /// Source PG host (TCP) or unix socket directory (leading `/`).
    #[arg(long, default_value = "localhost")]
    host: String,
    #[arg(long, default_value_t = 5432)]
    port: u16,
    #[arg(long, default_value = "postgres")]
    user: String,
    #[arg(long, default_value = "postgres")]
    dbname: String,
    /// Optional cleartext password. Replication-mode connection auth
    /// supports trust / cleartext / SCRAM-SHA-256.
    #[arg(long)]
    password: Option<String>,
    /// SSL mode. `disable`, `prefer`, `require`. TLS is skipped on
    /// unix sockets regardless.
    #[arg(long, default_value = "prefer")]
    sslmode: String,
    /// Where filtered segments + manifests land. Shadow PG's
    /// `restore_command` reads from here.
    #[arg(long)]
    out_dir: PathBuf,
    /// Optional permanent physical slot name on source PG.
    #[arg(long)]
    slot: Option<String>,
    /// Start LSN in `X/Y` hex form. Defaults to the source's current
    /// `pg_current_wal_lsn` (per `IDENTIFY_SYSTEM`), aligned down to a
    /// segment boundary.
    #[arg(long)]
    start_lsn: Option<String>,
    /// Status-update cadence in seconds.
    #[arg(long, default_value_t = 10)]
    status_interval: u64,
    /// Stop after this many segments have been shipped (useful for
    /// smoke tests). Zero = run forever.
    #[arg(long, default_value_t = 0)]
    max_segments: u64,
}

fn main() -> ExitCode {
    let args = Args::parse();
    let rt = match tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("walshadow-stream: tokio runtime: {e}");
            return ExitCode::FAILURE;
        }
    };
    match rt.block_on(run(args)) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("walshadow-stream: {e:#}");
            ExitCode::FAILURE
        }
    }
}

async fn run(args: Args) -> Result<()> {
    let sslmode = match args.sslmode.as_str() {
        "disable" => SslMode::Disable,
        "prefer" => SslMode::Prefer,
        "require" => SslMode::Require,
        other => bail!("invalid sslmode: {other}"),
    };
    let cfg = PgConfig {
        host: args.host,
        port: args.port,
        user: args.user,
        password: args.password,
        database: args.dbname,
        application_name: "walshadow".into(),
        sslmode,
    };
    let mut feed = SourceFeed::connect(&cfg)
        .await
        .context("connect to source PG")?
        .with_status_interval(Duration::from_secs(args.status_interval));

    let ident = feed.identify_system().await.context("IDENTIFY_SYSTEM")?;
    eprintln!(
        "source: sysid={} timeline={} xlogpos={:X}/{:X}",
        ident.sysid,
        ident.timeline,
        ident.xlogpos >> 32,
        ident.xlogpos as u32,
    );

    let raw_start = match args.start_lsn {
        Some(s) => parse_lsn(&s).context("--start-lsn")?,
        None => ident.xlogpos,
    };
    let aligned = WalStream::align_down(raw_start, WAL_SEG_SIZE);
    eprintln!(
        "start_lsn={:X}/{:X} (aligned={:X}/{:X})",
        raw_start >> 32,
        raw_start as u32,
        aligned >> 32,
        aligned as u32,
    );

    feed.start_physical_replication(args.slot.as_deref(), aligned, ident.timeline)
        .await
        .context("START_REPLICATION")?;

    let mut stream = WalStream::new(ident.timeline, WAL_SEG_SIZE, aligned)?;
    let mut record_sink = CollectingRecordSink::default();
    let mut segment_sink = DirSegmentSink::new(args.out_dir.clone()).context("open out-dir")?;
    let mut chunk_buf = Vec::with_capacity(64 * 1024);

    let mut segments_shipped = 0u64;
    let mut prev_dispatched = stream.dispatched_lsn();
    loop {
        let apply_lsn = stream.dispatched_lsn();
        let chunk = match feed.next_chunk(apply_lsn, &mut chunk_buf).await? {
            Some(c) => c,
            None => {
                eprintln!("source: CopyDone — stopping");
                break;
            }
        };
        stream.push(
            chunk.start_lsn,
            chunk.data,
            &mut record_sink,
            &mut segment_sink,
        )?;
        let now_dispatched = stream.dispatched_lsn();
        if now_dispatched != prev_dispatched {
            let new_segs = (now_dispatched - prev_dispatched) / WAL_SEG_SIZE;
            segments_shipped += new_segs;
            prev_dispatched = now_dispatched;
            eprintln!(
                "shipped {} segments, last_lsn={:X}/{:X}, records={}",
                segments_shipped,
                now_dispatched >> 32,
                now_dispatched as u32,
                record_sink.events.len(),
            );
            if args.max_segments != 0 && segments_shipped >= args.max_segments {
                eprintln!("reached --max-segments={}, stopping", args.max_segments);
                break;
            }
        }
    }
    Ok(())
}

fn parse_lsn(s: &str) -> Result<u64> {
    let (hi, lo) = s
        .split_once('/')
        .ok_or_else(|| anyhow::anyhow!("bad pg_lsn {s:?}: no '/'"))?;
    let hi = u32::from_str_radix(hi, 16)?;
    let lo = u32::from_str_radix(lo, 16)?;
    Ok(((hi as u64) << 32) | (lo as u64))
}
