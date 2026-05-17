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
//!     --shadow-socket-dir /tmp/shadow_sock --shadow-port 5433 \
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
//!
//! Shadow catalog ownership: daemon holds the [`ShadowCatalog`] inside
//! `Arc<tokio::sync::Mutex<_>>`. Clones go to the tracker→drain wire
//! today and, post-Phase 5, to the `DecoderSink` that turns dropped
//! heap records into tuple events. See [PRE5b7](../plans/PRE5b7.md).

use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use clap::Parser;
use std::future::Future;
use std::pin::Pin;
use tokio::sync::Mutex;
use wal_rs::pg::replication::conn::PgConfig;
use wal_rs::pg::replication::tls::SslMode;
use walshadow::decoder_sink::MetricsTupleObserver;
use walshadow::shadow_catalog::{
    ShadowCatalog, ShadowCatalogConfig, socket_conninfo, spawn_invalidation_drain,
    with_transient_retry,
};
use walshadow::source_feed::SourceFeed;
use walshadow::wal_stream::{
    DirSegmentSink, MetricsRecordSink, Record, RecordSink, SinkError, WAL_SEG_SIZE, WalStream,
};
use walshadow::xact_buffer::{BufferingDecoderSink, XactBuffer, XactBufferConfig, XactRecordSink};

/// Tiny inline `RecordSink` composite. Phase 6 adds the xact buffer
/// to the chain: heap-tuple records park in `xact` until the matching
/// commit / abort lands, then drain to `xact_drain`'s observer (today
/// the same metrics counter Phase 5 used; Phase 7 swaps in the CH
/// emitter). Status-line code keeps direct ownership so per-section
/// stats render without `dyn Any` round-trips.
struct DaemonSinks {
    metrics: MetricsRecordSink,
    decoder: BufferingDecoderSink,
    xact_drain: XactRecordSink<MetricsTupleObserver>,
}

impl RecordSink for DaemonSinks {
    fn on_record<'a>(
        &'a mut self,
        record: &'a Record,
    ) -> Pin<Box<dyn Future<Output = Result<(), SinkError>> + Send + 'a>> {
        Box::pin(async move {
            self.metrics.on_record(record).await?;
            // Order matters: decoder must absorb the heap record into
            // the buffer before the xact_drain sink (which may flush
            // this same xact) runs. A multi-statement xact whose
            // COMMIT record arrives in the same dispatch batch as
            // its heap records would otherwise miss the latest writes.
            self.decoder.on_record(record).await?;
            self.xact_drain.on_record(record).await?;
            Ok(())
        })
    }
}

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
    /// SSL mode: `disable`, `allow`, `prefer`, `require`, `verify-ca`,
    /// `verify-full`. TLS is skipped on unix sockets regardless. The
    /// verify-ca / verify-full modes consult `PGSSLROOTCERT` (or the
    /// webpki bundle if unset) for the trust anchor — same contract as
    /// libpq.
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
    /// Shadow PG unix socket directory. Reused as libpq `host=` since
    /// PG's libpq treats a leading `/` as a socket dir.
    #[arg(long)]
    shadow_socket_dir: PathBuf,
    /// Shadow PG port.
    #[arg(long, default_value_t = 5432)]
    shadow_port: u16,
    /// Shadow PG user.
    #[arg(long, default_value = "postgres")]
    shadow_user: String,
    /// Shadow PG database.
    #[arg(long, default_value = "postgres")]
    shadow_dbname: String,
    /// Wall-clock budget for the initial connect attempt against
    /// shadow PG. Reused by [`with_transient_retry`] so a still-warming
    /// shadow doesn't fail the daemon on first boot.
    #[arg(long, default_value_t = 30)]
    shadow_connect_timeout: u64,
    /// Phase 6 xact / TOAST buffer spill dir. Created on boot if
    /// missing; wiped clean every startup per the
    /// [PHASE6disk.md](../../plans/PHASE6disk.md) crash-recovery note.
    #[arg(long)]
    spill_dir: PathBuf,
    /// In-memory budget for the xact buffer in bytes. Defaults match
    /// PG's `logical_decoding_work_mem` (64 MiB).
    #[arg(long, default_value_t = walshadow::xact_buffer::DEFAULT_XACT_BUFFER_MAX)]
    xact_buffer_max: usize,
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
    let sslmode = SslMode::parse(&args.sslmode).context("--sslmode")?;
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

    let mut stream = WalStream::new(ident.timeline, WAL_SEG_SIZE, aligned)?;
    // Seed the catalog tracker from source's *current* pg_class before
    // START_REPLICATION. Closes the "long-running source rotated a
    // mapped catalog above 16384 pre-attach" hole that the < 16384
    // bootstrap rule misses on its own. Idempotent so `--start-lsn`
    // resumes seed too.
    {
        let sql_client = feed
            .sql_client()
            .await
            .context("open sidecar sql client for seed_from_source")?;
        let added = stream
            .filter_mut()
            .tracker
            .seed_from_source(sql_client)
            .await
            .context("seed_from_source")?;
        eprintln!("seeded {added} catalog filenodes from source pg_class");
    }

    // Connect the shadow catalog before START_REPLICATION so the
    // tracker→drain wire is hot from the first record. Wrapped in
    // with_transient_retry so a still-warming shadow doesn't kill the
    // daemon on boot. PRE5b7: catalog lives in Arc<Mutex<_>>; clones
    // fan out to the drain task today and to Phase 5's DecoderSink
    // once it lands.
    let shadow_conninfo = socket_conninfo(
        args.shadow_socket_dir
            .to_str()
            .context("shadow-socket-dir not UTF-8")?,
        args.shadow_port,
        &args.shadow_user,
        &args.shadow_dbname,
    );
    let connect_budget = Duration::from_secs(args.shadow_connect_timeout);
    let cat_cfg = ShadowCatalogConfig::default();
    let backoff_initial = cat_cfg.reconnect_backoff_initial;
    let backoff_max = cat_cfg.reconnect_backoff_max;
    let catalog = with_transient_retry(connect_budget, backoff_initial, backoff_max, async || {
        ShadowCatalog::connect(&shadow_conninfo, cat_cfg.clone()).await
    })
    .await
    .context("connect to shadow PG")?;
    let catalog = Arc::new(Mutex::new(catalog));
    eprintln!(
        "shadow: connected via {} (port={}, user={}, dbname={})",
        args.shadow_socket_dir.display(),
        args.shadow_port,
        args.shadow_user,
        args.shadow_dbname,
    );

    // Wire the descriptor-cache invalidation channel. Tracker → drain
    // task → ShadowCatalog::invalidate. Drain holds its own Arc clone
    // of the catalog; future consumers (Phase 5 DecoderSink, oracle)
    // clone again from `catalog`.
    let (invalidation_tx, invalidation_rx) = tokio::sync::mpsc::unbounded_channel::<()>();
    stream
        .filter_mut()
        .tracker
        .set_invalidation_signal(invalidation_tx);
    let _invalidation_drain = spawn_invalidation_drain(catalog.clone(), invalidation_rx);

    feed.start_physical_replication(args.slot.as_deref(), aligned, ident.timeline)
        .await
        .context("START_REPLICATION")?;
    // Phase 6 xact buffer + spill dir. Wiped on every startup —
    // cursor file commits drains atomically so leftover spill files
    // from a prior crash are always either redundant or stale.
    let xact_buf_cfg = XactBufferConfig {
        xact_buffer_max: args.xact_buffer_max,
        spill_dir: args.spill_dir.clone(),
    };
    let xact_buffer = XactBuffer::new(xact_buf_cfg).context("init xact buffer / spill dir")?;
    xact_buffer
        .clear_spill_dir()
        .await
        .context("clear stale spill files")?;
    let xact_buffer = Arc::new(Mutex::new(xact_buffer));
    eprintln!(
        "spill dir: {} (xact_buffer_max={} bytes)",
        args.spill_dir.display(),
        args.xact_buffer_max,
    );
    // Fan-out: metrics-by-rmgr first, then the buffering decoder
    // (heap → xact buffer), then the xact-record drain (commit/abort
    // → emit). Ordering keeps per-rmgr counters intact when a decoder
    // semantic error trips inside the dispatch chain; xact_drain
    // running after decoder absorbs any heap records in the same
    // dispatch batch as the commit.
    let mut record_sink = DaemonSinks {
        metrics: MetricsRecordSink::default(),
        decoder: BufferingDecoderSink::new(catalog.clone(), xact_buffer.clone()),
        xact_drain: XactRecordSink::new(
            xact_buffer.clone(),
            catalog.clone(),
            MetricsTupleObserver::default(),
        ),
    };
    let mut segment_sink = DirSegmentSink::new(args.out_dir.clone()).context("open out-dir")?;
    let mut chunk_buf = Vec::with_capacity(64 * 1024);

    let mut segments_shipped = 0u64;
    let mut prev_dispatched = stream.dispatched_lsn();
    let shutdown_reason = loop {
        let apply_lsn = stream.dispatched_lsn();
        let chunk = tokio::select! {
            biased;
            // Drain signals first so an in-flight ctrl_c doesn't lose to
            // a chunk that's already at the head of the queue.
            sig = tokio::signal::ctrl_c() => {
                sig.context("install ctrl_c handler")?;
                break "signal";
            }
            res = feed.next_chunk(apply_lsn, &mut chunk_buf) => match res? {
                Some(c) => c,
                None => break "CopyDone",
            },
        };
        let dispatched_before = stream.dispatched_lsn();
        let server_end = chunk.server_wal_end;
        stream
            .push(
                chunk.start_lsn,
                chunk.data,
                &mut record_sink,
                &mut segment_sink,
            )
            .await?;
        let now_dispatched = stream.dispatched_lsn();
        if now_dispatched != prev_dispatched {
            let new_segs = (now_dispatched - prev_dispatched) / WAL_SEG_SIZE;
            segments_shipped += new_segs;
            prev_dispatched = now_dispatched;
            let filter = stream.filter();
            let ahead = server_end.saturating_sub(dispatched_before);
            let xact_stats = {
                let b = xact_buffer.lock().await;
                b.stats().summary()
            };
            eprintln!(
                "shipped {} segments, last_lsn={:X}/{:X}, source_ahead={}B, {}, kept={}, dropped={}, relmap_updates={}, pg_class_undecoded={}, pg_class_oid_in_prefix={}, {}, {}",
                segments_shipped,
                now_dispatched >> 32,
                now_dispatched as u32,
                ahead,
                record_sink.metrics.summary(),
                filter.stats.kept,
                filter.stats.dropped,
                filter.tracker.relmap_updates,
                filter.tracker.pg_class_writes_undecoded,
                filter.tracker.pg_class_writes_oid_in_prefix,
                record_sink.decoder.stats().summary(),
                xact_stats,
            );
            if args.max_segments != 0 && segments_shipped >= args.max_segments {
                break "max-segments";
            }
        }
    };
    eprintln!(
        "stopping ({shutdown_reason}); flushing partial segment to {}",
        args.out_dir.display(),
    );
    stream
        .close(Some(&mut segment_sink), &mut record_sink)
        .await
        .context("flush partial segment on shutdown")?;
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
