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
//!     [--start-lsn 0/16B3750] \
//!     [--metrics-bind 127.0.0.1:9484] \
//!     [--retention-bytes 268435456]
//! ```
//!
//! Phase 10 adds the operational scaffolding: pre-flight validators
//! reject mis-configured boots, a Prometheus `/metrics` endpoint
//! exposes the LSN triple + per-rmgr / xact-buffer / emitter / oracle
//! counters, `tracing` is wired so `RUST_LOG=walshadow=debug` surfaces
//! frame-level diagnostics, filtered segments under `--out-dir` are
//! trimmed once shadow replays past them, the standby-status update
//! sent back to source is split into the (write, flush, apply) triple
//! the protocol expects, the CH emitter retries through a bounded
//! reconnect loop, and `SIGHUP` re-reads `--ch-config` to swap the
//! per-relation mapping atomically.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use clap::Parser;
use std::future::Future;
use std::pin::Pin;
use tokio::sync::Mutex;
use wal_rs::pg::replication::conn::PgConfig;
use wal_rs::pg::replication::tls::SslMode;
use walshadow::ch_emitter::{Emitter, EmitterConfig, EmitterObserver, MappingHandle};
use walshadow::cursor;
use walshadow::decoder_sink::{MetricsTupleObserver, TupleObserver};
use walshadow::metrics::{MetricsRegistry, MetricsSnapshot};
use walshadow::retention::{DEFAULT_RETENTION_BYTES, DEFAULT_TRIM_INTERVAL, trim_below_lsn};
use walshadow::shadow_catalog::{
    ShadowCatalog, ShadowCatalogConfig, socket_conninfo, spawn_invalidation_drain,
    with_transient_retry,
};
use walshadow::source_feed::{SourceFeed, StandbyStatus};
use walshadow::wal_stream::{
    DirSegmentSink, MetricsRecordSink, Record, RecordSink, SinkError, WAL_SEG_SIZE, WalStream,
};
use walshadow::xact_buffer::{BufferingDecoderSink, XactBuffer, XactBufferConfig, XactRecordSink};

/// Tiny inline `RecordSink` composite. Phase 6 adds the xact buffer
/// to the chain: heap-tuple records park in `xact` until the matching
/// commit / abort lands, then drain to `xact_drain`'s observer. Phase
/// 7 wires the observer end via `Box<dyn TupleObserver>` so the daemon
/// can pick between metrics-only (no `--ch-config`) and the CH-Native
/// emitter (config provided) at runtime without a closed enum.
/// Status-line code keeps direct ownership so per-section stats render
/// without `dyn Any` round-trips.
struct DaemonSinks {
    metrics: MetricsRecordSink,
    decoder: BufferingDecoderSink,
    xact_drain: XactRecordSink<Box<dyn TupleObserver>>,
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
    /// Optional path to the Phase 7 CH-Native emitter config (TOML).
    /// When set, drained xact tuples ship to ClickHouse via
    /// `clickhouse-c-rs`. When unset the daemon stays metrics-only.
    /// Shape: see [`walshadow::ch_emitter::EmitterConfig::from_toml_str`].
    /// Phase 10 reloads this on SIGHUP (atomic mapping swap; connection
    /// params stay boot-only).
    #[arg(long)]
    ch_config: Option<PathBuf>,
    /// Phase 9 differential decode oracle: probe 1-in-`<N>` rows
    /// through shadow PG's `walshadow_decode_disk(oid, bytea)`
    /// extension function and assert the local decoder matches. `0`
    /// (default) disables. Requires the `walshadow` extension
    /// installed on shadow PG; absent extension surfaces as
    /// `oracle fallback=N` in the status line and the daemon
    /// silently ships raw on-disk bytes for `PgPending` types.
    #[arg(long, default_value_t = 0)]
    validate: u32,
    /// Phase 10 HTTP/Prometheus metrics bind address. Disabled when
    /// absent; pass `127.0.0.1:9484` for a localhost-only scrape.
    #[arg(long)]
    metrics_bind: Option<SocketAddr>,
    /// Phase 10 retention horizon in bytes of WAL. Segments older than
    /// `shadow_replay_lsn - retention_bytes` are deleted on every trim
    /// cycle. Set to `0` to disable trim entirely.
    #[arg(long, default_value_t = DEFAULT_RETENTION_BYTES)]
    retention_bytes: u64,
    /// Skip the Phase 10 pre-flight validators (server_version_num,
    /// wal_level, REPLICA IDENTITY FULL, slot existence). Useful for
    /// recovery drills; production should leave this off.
    #[arg(long, default_value_t = false)]
    skip_preflight: bool,
    /// Phase 11. Ignore any `cursor.bin` under `--spill-dir` at boot
    /// (greenfield resume even when a prior daemon left one). Useful
    /// for "wipe + restart from a known LSN" drills. The cursor still
    /// gets rewritten as the new daemon makes progress.
    #[arg(long, default_value_t = false)]
    ignore_cursor: bool,
}

fn main() -> ExitCode {
    let args = Args::parse();
    init_tracing();
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

/// Wire `tracing` once per process. `RUST_LOG` configures the filter;
/// when unset, defaults to warn-level except for walshadow itself which
/// stays at info so the daemon's startup banner still emits.
fn init_tracing() {
    use tracing_subscriber::EnvFilter;
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("warn,walshadow=info"));
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(true)
        .with_writer(std::io::stderr)
        .try_init();
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
    tracing::info!(
        target: "walshadow",
        sysid = %ident.sysid,
        timeline = ident.timeline,
        xlogpos = format!("{:X}/{:X}", ident.xlogpos >> 32, ident.xlogpos as u32),
        "source identified",
    );

    // Phase 11 cursor-resume gate. `--start-lsn` (explicit operator
    // override) wins; otherwise cursor.bin under spill_dir picks up the
    // last emitter-acked LSN; otherwise greenfield (source's current
    // write head). `--ignore-cursor` forces greenfield even when a
    // valid cursor is on disk (recovery drills).
    let cursor_at_boot: Option<cursor::Cursor> = if args.ignore_cursor {
        None
    } else {
        match cursor::read(&args.spill_dir).await {
            Ok(c) => c,
            Err(e) => {
                tracing::warn!(
                    target: "walshadow::cursor",
                    error = %e,
                    spill_dir = %args.spill_dir.display(),
                    "cursor file unreadable; falling back to greenfield",
                );
                None
            }
        }
    };
    let raw_start = match (&args.start_lsn, &cursor_at_boot) {
        (Some(s), _) => parse_lsn(s).context("--start-lsn")?,
        (None, Some(c)) if c.emitter_ack_lsn != 0 => c.emitter_ack_lsn,
        (None, _) => ident.xlogpos,
    };
    let aligned = WalStream::align_down(raw_start, WAL_SEG_SIZE);
    tracing::info!(
        target: "walshadow",
        raw = format!("{:X}/{:X}", raw_start >> 32, raw_start as u32),
        aligned = format!("{:X}/{:X}", aligned >> 32, aligned as u32),
        from_cursor = cursor_at_boot.is_some()
            && args.start_lsn.is_none()
            && cursor_at_boot.as_ref().is_some_and(|c| c.emitter_ack_lsn != 0),
        "start LSN",
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
        tracing::info!(
            target: "walshadow",
            added,
            "seeded catalog filenodes from source pg_class"
        );
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
    tracing::info!(
        target: "walshadow",
        socket = %args.shadow_socket_dir.display(),
        port = args.shadow_port,
        user = %args.shadow_user,
        dbname = %args.shadow_dbname,
        "shadow connected",
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

    // Phase 10 pre-flight validators. Run after both source + shadow
    // SQL clients are up so every check has the connection it needs;
    // abort the daemon on any finding unless `--skip-preflight` is set.
    let initial_ch_config = match args.ch_config.as_deref() {
        Some(path) => {
            let toml = tokio::fs::read_to_string(path)
                .await
                .with_context(|| format!("read --ch-config {}", path.display()))?;
            Some(EmitterConfig::from_toml_str(&toml).context("parse --ch-config")?)
        }
        None => None,
    };
    if !args.skip_preflight {
        let source_version_num = feed.server_version_num();
        let source_sql = feed
            .sql_client()
            .await
            .context("source sidecar sql for preflight")?;
        let shadow_sql = open_shadow_sql_client(
            &args.shadow_socket_dir,
            args.shadow_port,
            &args.shadow_user,
            &args.shadow_dbname,
        )
        .await?;
        let report = walshadow::preflight::run(walshadow::preflight::Inputs {
            source_version_num,
            source_sql,
            shadow_sql: &shadow_sql,
            slot: args.slot.as_deref(),
            ch_config: initial_ch_config.as_ref(),
        })
        .await
        .context("pre-flight probe")?;
        report
            .into_result()
            .context("pre-flight rejected daemon start")?;
        tracing::info!(target: "walshadow::preflight", "pre-flight passed");
    }

    // Phase 9 oracle. Opens its own libpq connection to shadow PG so
    // its queries don't pessimise the catalog's query-one path. Best-
    // effort: a still-warming shadow or a connect timeout just
    // disables the oracle; the daemon keeps running with the raw-
    // bytes fallback.
    let oracle = match walshadow::oracle::connect_with_budget(
        &shadow_conninfo,
        args.validate,
        connect_budget,
    )
    .await
    {
        Ok(o) => {
            let ext = o.has_extension().await;
            tracing::info!(
                target: "walshadow::oracle",
                validate = args.validate > 0,
                sample_rate = args.validate.max(1),
                extension = if ext { "present" } else { "absent" },
                "oracle connected",
            );
            Some(Arc::new(o))
        }
        Err(e) => {
            tracing::warn!(target: "walshadow::oracle", error = %e, "oracle disabled");
            None
        }
    };

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
    tracing::info!(
        target: "walshadow",
        spill_dir = %args.spill_dir.display(),
        xact_buffer_max = args.xact_buffer_max,
        "spill dir ready",
    );

    // Pick the xact-drain observer: Phase 7 CH-Native emitter when
    // `--ch-config` is supplied, else stay metrics-only. Wrapped in
    // `Box<dyn TupleObserver>` so both arms share one drain-sink type.
    //
    // Config read + TCP connect ride tokio's async APIs so a slow DNS
    // / stalled CH boot can't pin the runtime worker. Hand-off to the
    // emitter requires a blocking-mode `std::net::TcpStream` because
    // clickhouse-c-rs wraps a raw fd through `chc_posix_io` (sync
    // read/write vtable), so we `into_std()` + `set_nonblocking(false)`
    // right before construction.
    let mut mapping_handle: Option<MappingHandle> = None;
    let inner_observer: Box<dyn TupleObserver> = match initial_ch_config {
        Some(emitter_cfg) => {
            let addr = format!("{}:{}", emitter_cfg.host, emitter_cfg.port);
            let tcp = tokio::net::TcpStream::connect(&addr)
                .await
                .with_context(|| format!("connect CH at {addr}"))?;
            tcp.set_nodelay(true).ok();
            let std_tcp = tcp.into_std().context("tokio→std TcpStream handoff")?;
            std_tcp
                .set_nonblocking(false)
                .context("set CH socket to blocking for chc_posix_io")?;
            let emitter =
                Emitter::new(emitter_cfg, catalog.clone(), std_tcp).context("init CH emitter")?;
            mapping_handle = Some(emitter.mapping_handle());
            tracing::info!(target: "walshadow::ch_emitter", addr = %addr, "ch emitter connected");
            Box::new(EmitterObserver::new(emitter))
        }
        None => Box::new(MetricsTupleObserver::default()),
    };
    // Phase 9: wrap with OracleObserver when an oracle is up. The
    // wrapper resolves PgPending columns via shadow PG's extension
    // (no-op when extension is absent) and fires 1-in-N validator
    // probes when `--validate > 0`. Skip the wrapper entirely if the
    // oracle is disabled — keeps the dispatch chain tight for the
    // metrics-only / no-shadow-extension case.
    let observer: Box<dyn TupleObserver> = match oracle.clone() {
        Some(o) => Box::new(walshadow::oracle::OracleObserver::new(o, inner_observer)),
        None => inner_observer,
    };
    // Fan-out: metrics-by-rmgr first, then the buffering decoder
    // (heap → xact buffer), then the xact-record drain (commit/abort
    // → emit). Ordering keeps per-rmgr counters intact when a decoder
    // semantic error trips inside the dispatch chain; xact_drain
    // running after decoder absorbs any heap records in the same
    // dispatch batch as the commit.
    let mut record_sink = DaemonSinks {
        metrics: MetricsRecordSink::default(),
        decoder: BufferingDecoderSink::new(catalog.clone(), xact_buffer.clone()),
        xact_drain: XactRecordSink::new(xact_buffer.clone(), catalog.clone(), observer),
    };
    let mut segment_sink = DirSegmentSink::new(args.out_dir.clone()).context("open out-dir")?;
    let mut chunk_buf = Vec::with_capacity(64 * 1024);

    // Phase 10 metrics endpoint. The registry handle threads through
    // the status-line loop; the HTTP server task lives until the
    // runtime tears down.
    let metrics = MetricsRegistry::new();
    let _metrics_server = match args.metrics_bind {
        Some(addr) => {
            let (bound, _handle) = walshadow::metrics::serve(addr, metrics.clone())
                .await
                .context("bind metrics endpoint")?;
            tracing::info!(target: "walshadow::metrics", addr = %bound, "metrics endpoint serving");
            Some(_handle)
        }
        None => None,
    };

    // Phase 10 SIGHUP handler. Re-reads `--ch-config` and swaps the
    // live mapping in the emitter via the shared handle. Connection
    // params stay boot-only; only the per-relation mapping reloads.
    let sighup_path = args.ch_config.clone();
    let sighup_handle = mapping_handle.clone();
    let _sighup_task = spawn_sighup_handler(sighup_path, sighup_handle);

    // Phase 11 — shared shadow_replay_lsn observed by the retention
    // sweeper (the only thing polling shadow's `pg_last_wal_replay_lsn`
    // today). Status loop reads the same atomic to feed the cursor
    // file's `shadow_replay_lsn` slot + the standby-status `apply_lsn`
    // ceiling. Atomic so the two tasks don't need a shared mutex.
    let shadow_replay_lsn = Arc::new(AtomicU64::new(0));

    // Phase 10 retention sweeper. Polls shadow's replay LSN, drops
    // filtered segments more than `retention_bytes` behind. Disabled
    // when `retention_bytes == 0`. Phase 11 doubles up: the sweeper's
    // poll feeds `shadow_replay_lsn` so the main loop doesn't open a
    // second shadow connection.
    let _retention_task = if args.retention_bytes > 0 {
        Some(spawn_retention(
            args.out_dir.clone(),
            args.retention_bytes,
            shadow_conninfo.clone(),
            shadow_replay_lsn.clone(),
        ))
    } else {
        None
    };

    let start_instant = Instant::now();
    let mut segments_shipped = 0u64;
    let mut prev_dispatched = stream.dispatched_lsn();
    // Phase 11. Cursor write cadence matches the source standby-status
    // cadence so the file's `emitter_ack_lsn` is ≥ the value we advertise
    // to source as `apply_lsn` on every send. Without this ordering the
    // slot could advance past a not-yet-durable resume point.
    let cursor_write_interval = Duration::from_secs(args.status_interval);
    let mut last_cursor_write: Option<Instant> = None;
    let shutdown_reason = loop {
        // Snapshot every LSN the cursor + standby status depend on.
        // dispatched_lsn is filter_durable now that DirSegmentSink
        // fsyncs every segment + the parent dir. shadow_replay_lsn comes
        // from the retention sweeper's poll (0 when retention is off).
        // drain_lsn / emitter_ack_lsn come straight from the xact buffer
        // — single source of truth per PHASE11.
        let dispatched = stream.dispatched_lsn();
        let received = feed.last_server_wal_end().max(dispatched);
        let shadow_replay = shadow_replay_lsn.load(Ordering::Acquire);
        let (drain_lsn, emitter_ack_lsn) = {
            let b = xact_buffer.lock().await;
            let s = b.stats();
            (s.drain_lsn, s.emitter_ack_lsn)
        };
        // apply_lsn ceiling per PLAN §"Phase 11". Treat shadow_replay==0
        // (sweeper disabled or hasn't reported yet) as "no constraint
        // from shadow" rather than the literal min — otherwise a fresh
        // boot with retention off would pin apply_lsn at 0 forever and
        // source's slot would never recycle.
        let apply_ceiling = match shadow_replay {
            0 => emitter_ack_lsn,
            s => s.min(emitter_ack_lsn),
        };
        let cur = cursor::Cursor {
            source_received_lsn: received,
            filter_durable_lsn: dispatched,
            shadow_replay_lsn: shadow_replay,
            drain_lsn,
            emitter_ack_lsn,
        };
        if last_cursor_write.is_none_or(|t| t.elapsed() >= cursor_write_interval) {
            cursor::write(&args.spill_dir, &cur)
                .await
                .context("write resume cursor")?;
            last_cursor_write = Some(Instant::now());
        }
        let status = StandbyStatus {
            write_lsn: received,
            flush_lsn: dispatched,
            apply_lsn: apply_ceiling,
        };
        let chunk = tokio::select! {
            biased;
            // Drain signals first so an in-flight ctrl_c doesn't lose to
            // a chunk that's already at the head of the queue.
            sig = tokio::signal::ctrl_c() => {
                sig.context("install ctrl_c handler")?;
                break "signal";
            }
            res = feed.next_chunk(status, &mut chunk_buf) => match res? {
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
            let ahead = server_end.saturating_sub(dispatched_before);
            // One status-tick = one metrics refresh + one stderr line.
            let xact_summary = {
                let b = xact_buffer.lock().await;
                let stats = b.stats().clone();
                let line = stats.summary();
                (stats, line)
            };
            let (xact_stats, xact_line) = xact_summary;
            let oracle_pair = match &oracle {
                Some(o) => {
                    let s = o.stats.lock().await.clone();
                    let line = s.summary();
                    (Some(s), line)
                }
                None => (None, String::new()),
            };
            let (oracle_stats, oracle_line) = oracle_pair;
            let filter = stream.filter();
            let decoder_stats = record_sink.decoder.stats().clone();
            populate_metrics(
                &metrics,
                received,
                now_dispatched,
                shadow_replay,
                drain_lsn,
                emitter_ack_lsn,
                &record_sink.metrics,
                &xact_stats,
                &decoder_stats,
                oracle_stats.as_ref(),
                start_instant.elapsed().as_secs(),
            )
            .await;
            tracing::info!(
                target: "walshadow",
                segments_shipped,
                last_lsn = format!("{:X}/{:X}", now_dispatched >> 32, now_dispatched as u32),
                source_ahead_bytes = ahead,
                metrics = %record_sink.metrics.summary(),
                kept = filter.stats.kept,
                dropped = filter.stats.dropped,
                relmap_updates = filter.tracker.relmap_updates,
                pg_class_undecoded = filter.tracker.pg_class_writes_undecoded,
                pg_class_oid_in_prefix = filter.tracker.pg_class_writes_oid_in_prefix,
                decoder = %record_sink.decoder.stats().summary(),
                xact_buffer = %xact_line,
                oracle = %oracle_line,
                "status",
            );
            if args.max_segments != 0 && segments_shipped >= args.max_segments {
                break "max-segments";
            }
        }
    };
    tracing::info!(
        target: "walshadow",
        reason = shutdown_reason,
        out_dir = %args.out_dir.display(),
        "stopping — flushing partial segment",
    );
    stream
        .close(Some(&mut segment_sink), &mut record_sink)
        .await
        .context("flush partial segment on shutdown")?;
    Ok(())
}

/// Open a tokio_postgres client against shadow over its unix socket.
/// Used by [`walshadow::preflight::run`] which needs SQL access
/// independent of the [`ShadowCatalog`]'s replay-LSN-gated path.
async fn open_shadow_sql_client(
    socket_dir: &std::path::Path,
    port: u16,
    user: &str,
    dbname: &str,
) -> Result<tokio_postgres::Client> {
    let socket = socket_dir
        .to_str()
        .ok_or_else(|| anyhow::anyhow!("shadow-socket-dir not UTF-8"))?;
    let conninfo = socket_conninfo(socket, port, user, dbname);
    let (client, conn) = tokio_postgres::connect(&conninfo, tokio_postgres::NoTls)
        .await
        .with_context(|| format!("preflight: open shadow sql client ({conninfo})"))?;
    tokio::spawn(async move {
        let _ = conn.await;
    });
    Ok(client)
}

/// Spawn the SIGHUP listener task. Each delivery re-parses
/// `--ch-config`, validates the TOML, and atomically swaps the live
/// mapping table. Parse errors keep the existing mapping in place +
/// log; an absent `--ch-config` makes the handler a no-op tap.
fn spawn_sighup_handler(
    path: Option<PathBuf>,
    handle: Option<MappingHandle>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut sig = match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::hangup()) {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!(
                    target: "walshadow::sighup",
                    error = %e,
                    "SIGHUP install failed; reload disabled",
                );
                return;
            }
        };
        loop {
            if sig.recv().await.is_none() {
                return;
            }
            let (Some(p), Some(h)) = (path.as_deref(), handle.as_ref()) else {
                tracing::info!(target: "walshadow::sighup", "SIGHUP ignored (no --ch-config)");
                continue;
            };
            match tokio::fs::read_to_string(p).await {
                Ok(toml) => match EmitterConfig::from_toml_str(&toml) {
                    Ok(cfg) => {
                        *h.write().await = cfg.tables;
                        tracing::info!(
                            target: "walshadow::sighup",
                            path = %p.display(),
                            "ch-config reload applied",
                        );
                    }
                    Err(e) => tracing::warn!(
                        target: "walshadow::sighup",
                        error = %e,
                        path = %p.display(),
                        "ch-config parse failed; existing mapping preserved",
                    ),
                },
                Err(e) => tracing::warn!(
                    target: "walshadow::sighup",
                    error = %e,
                    path = %p.display(),
                    "ch-config read failed; existing mapping preserved",
                ),
            }
        }
    })
}

/// Spawn the retention sweeper task. Wakes every
/// [`DEFAULT_TRIM_INTERVAL`], queries shadow's
/// `pg_last_wal_replay_lsn()`, and trims segments older than
/// `replay_lsn - retention_bytes`.
fn spawn_retention(
    out_dir: PathBuf,
    retention_bytes: u64,
    shadow_conninfo: String,
    shadow_replay_lsn: Arc<AtomicU64>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let client = match open_retention_client(&shadow_conninfo).await {
            Ok(c) => c,
            Err(e) => {
                tracing::warn!(
                    target: "walshadow::retention",
                    error = %e,
                    "shadow connect failed; retention disabled",
                );
                return;
            }
        };
        loop {
            tokio::time::sleep(DEFAULT_TRIM_INTERVAL).await;
            let lsn = match query_replay_lsn(&client).await {
                Ok(Some(l)) => l,
                Ok(None) => continue, // shadow hasn't replayed anything yet
                Err(e) => {
                    tracing::warn!(target: "walshadow::retention", error = %e, "lsn query");
                    continue;
                }
            };
            shadow_replay_lsn.fetch_max(lsn, Ordering::Release);
            let cutoff = lsn.saturating_sub(retention_bytes);
            match trim_below_lsn(&out_dir, cutoff).await {
                Ok(r) if r.segments_removed > 0 => {
                    tracing::info!(
                        target: "walshadow::retention",
                        segments = r.segments_removed,
                        manifests = r.manifests_removed,
                        partials = r.partials_removed,
                        bytes_freed = r.bytes_freed,
                        cutoff_lsn = format!("{:X}/{:X}", cutoff >> 32, cutoff as u32),
                        "trim cycle",
                    );
                }
                Ok(_) => {}
                Err(e) => tracing::warn!(target: "walshadow::retention", error = %e, "trim"),
            }
        }
    })
}

async fn open_retention_client(conninfo: &str) -> Result<tokio_postgres::Client> {
    let (client, conn) = tokio_postgres::connect(conninfo, tokio_postgres::NoTls).await?;
    tokio::spawn(async move {
        let _ = conn.await;
    });
    Ok(client)
}

async fn query_replay_lsn(client: &tokio_postgres::Client) -> Result<Option<u64>> {
    let row = client
        .query_one("SELECT pg_last_wal_replay_lsn()::text", &[])
        .await?;
    let raw: Option<String> = row.get(0);
    match raw {
        Some(s) => Ok(Some(wal_rs::pg::backup::parse_pg_lsn(&s)?)),
        None => Ok(None),
    }
}

#[allow(clippy::too_many_arguments)]
async fn populate_metrics(
    registry: &MetricsRegistry,
    source_received_lsn: u64,
    filter_lsn: u64,
    shadow_replay_lsn: u64,
    decoder_commit_lsn: u64,
    emitter_ack_lsn: u64,
    rec_metrics: &MetricsRecordSink,
    xact_stats: &walshadow::xact_buffer::XactBufferStats,
    decoder_stats: &walshadow::decoder_sink::DecoderStats,
    oracle_stats: Option<&walshadow::oracle::OracleStats>,
    uptime_secs: u64,
) {
    use std::collections::BTreeMap;
    use walshadow::classify::rmgr_label;
    let mut by_rm = BTreeMap::new();
    for ((rm, decision), n) in &rec_metrics.by_rm_decision {
        let key = (
            rmgr_label(*rm).to_string(),
            match decision {
                walshadow::filter::Decision::Keep => "keep",
                walshadow::filter::Decision::Drop => "drop",
            },
        );
        by_rm.insert(key, *n);
    }
    let snap = MetricsSnapshot {
        source_received_lsn,
        filter_lsn,
        shadow_replay_lsn,
        decoder_commit_lsn,
        emitter_ack_lsn,
        records_by_rm_decision: by_rm,
        xact_active: xact_stats.xacts_active,
        xact_bytes_in_memory: xact_stats.bytes_in_memory,
        spill_xacts_active: xact_stats.spill_xacts_active,
        spill_bytes_active: xact_stats.spill_bytes_active,
        spill_evictions_total: xact_stats.spill_evictions_total,
        xacts_committed_total: xact_stats.committed_xacts_total,
        xacts_aborted_total: xact_stats.aborted_xacts_total,
        decoder_decoded_total: decoder_stats.decoded,
        decoder_partial_total: decoder_stats.partial,
        emitter_rows_total: 0,
        emitter_blocks_total: 0,
        emitter_xacts_total: 0,
        emitter_unsupported_relations: 0,
        oracle_resolved_total: oracle_stats.map(|s| s.resolved).unwrap_or(0),
        oracle_fallback_raw_total: oracle_stats.map(|s| s.fallback_raw).unwrap_or(0),
        oracle_validate_sampled_total: oracle_stats.map(|s| s.probes).unwrap_or(0),
        oracle_validate_mismatches_total: oracle_stats.map(|s| s.mismatches).unwrap_or(0),
        oracle_errors_total: oracle_stats.map(|s| s.errors).unwrap_or(0),
        uptime_secs,
    };
    registry.set(snap).await;
}

fn parse_lsn(s: &str) -> Result<u64> {
    let (hi, lo) = s
        .split_once('/')
        .ok_or_else(|| anyhow::anyhow!("bad pg_lsn {s:?}: no '/'"))?;
    let hi = u32::from_str_radix(hi, 16)?;
    let lo = u32::from_str_radix(lo, 16)?;
    Ok(((hi as u64) << 32) | (lo as u64))
}
