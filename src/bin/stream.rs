//! `walshadow-stream` — full WAL capture pipeline.
//!
//! Connects to source PG in replication mode, `IDENTIFY_SYSTEM` then
//! `START_REPLICATION PHYSICAL` (optionally bound to a permanent slot),
//! filters every WAL byte, writes filtered segments shadow PG reads via
//! `restore_command`.
//!
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

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use clap::{Parser, ValueEnum};
use std::fs;
use std::future::Future;
use std::io::Write as _;
use std::pin::Pin;
use tokio::sync::Mutex;
use wal_rs::pg::backup::BACKUP_NAME_PREFIX;
use wal_rs::pg::replication::base_backup::BaseBackupOpts;
use wal_rs::pg::replication::conn::PgConfig;
use wal_rs::pg::replication::tls::SslMode;
use walshadow::backfill_bootstrap::{
    BootstrapConfig, BootstrapOutcome, drain_backfill, seed_in_snapshot, spawn_greenfield_bootstrap,
};
use walshadow::backup_source::BackupSource;
use walshadow::backup_source_direct::DirectSource;
use walshadow::backup_source_object_store::ObjectStoreSource;
use walshadow::ch_emitter::{EmitterConfig, EmitterStats, MappingHandle};
use walshadow::cursor;
use walshadow::decoder_sink::{MetricsTupleObserver, TupleObserver};
use walshadow::metrics::{MetricsRegistry, MetricsSnapshot, RateEstimator};
use walshadow::pipeline::{Fatal, PipelineConfig, PipelineHandle, bootstrap, tail};
use walshadow::queueing_record_sink::{
    DEFAULT_QUEUEING_BATCH_SIZE, DEFAULT_QUEUEING_RECORD_SINK_CAPACITY, QueueingRecordSink,
};
use walshadow::retention::{DEFAULT_RETENTION_BYTES, DEFAULT_TRIM_INTERVAL, trim_below_lsn};
use walshadow::shadow::{Shadow, ShadowConfig};
use walshadow::shadow_catalog::{
    ShadowCatalog, ShadowCatalogConfig, socket_conninfo, with_transient_retry,
};
use walshadow::source_feed::{SourceFeed, StandbyStatus};
use walshadow::wal_stream::{
    DirSegmentSink, MetricsRecordSink, Record, RecordSink, SinkError, WAL_SEG_SIZE, WalStream,
};
use walshadow::xact_buffer::{
    BufferingDecoderSink, SubxactTracker, XactBuffer, XactBufferConfig, XactRecordSink,
};

/// Bootstrap source pick: lands catalog files + standby.signal so a
/// fresh shadow PG can be brought up against this run's `out_dir`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum, Default)]
enum BootstrapMode {
    /// Caller supplied shadow data dir externally (e.g. `pg_basebackup`)
    #[default]
    Off,
    /// Source-PG-driven BASE_BACKUP over the replication protocol,
    /// reuses `--host` / `--port` / `--user`, no extra credentials
    Direct,
    /// wal-g-compatible BASE_BACKUP from a `DynStorage` bucket. Storage
    /// config read from `WALG_*` env vars (wal-rs CLI convention);
    /// `--bootstrap-backup-name` selects the backup (LATEST = newest sentinel)
    ObjectStore,
}

/// `decoder + xact_drain` pair as one `RecordSink` for the queueing worker.
///
/// Order matters: decoder absorbs the heap record into the xact buffer
/// before xact_drain flushes the matching commit/abort. A multi-statement
/// xact whose COMMIT lands in the same dispatch batch as its heap records
/// would otherwise miss the latest writes.
struct DecoderXactPair<D: RecordSink + Send> {
    decoder: BufferingDecoderSink,
    xact_drain: D,
}

impl<D: RecordSink + Send> RecordSink for DecoderXactPair<D> {
    fn on_record<'a>(
        &'a mut self,
        record: &'a Record<'a>,
    ) -> Pin<Box<dyn Future<Output = Result<(), SinkError>> + Send + 'a>> {
        Box::pin(async move {
            self.decoder.on_record(record).await?;
            self.xact_drain.on_record(record).await?;
            Ok(())
        })
    }

    fn on_idle<'a>(
        &'a mut self,
    ) -> Pin<Box<dyn Future<Output = Result<(), SinkError>> + Send + 'a>> {
        // Decoder has no time-based work; xact_drain forwards to the
        // CH emitter's deadline check.
        self.xact_drain.on_idle()
    }

    fn on_close<'a>(
        &'a mut self,
    ) -> Pin<Box<dyn Future<Output = Result<(), SinkError>> + Send + 'a>> {
        // Decoder has no close work; xact_drain forwards the final flush.
        self.xact_drain.on_close()
    }

    fn on_idle_advance<'a>(
        &'a mut self,
        lsn: u64,
    ) -> Pin<Box<dyn Future<Output = Result<(), SinkError>> + Send + 'a>> {
        self.xact_drain.on_idle_advance(lsn)
    }
}

/// Daemon-side `RecordSink` composite.
///
/// `metrics` stays synchronous on the pump task (counter bumps, never
/// await). The decoder/xact-drain pair runs behind a [`QueueingRecordSink`]
/// so its `wait_for_replay` waits don't park the pump task, which would
/// freeze the wire shadow PG depends on for apply-LSN progress (deadlock
/// the streaming tests surface).
struct DaemonSinks {
    metrics: MetricsRecordSink,
    decoder_xact: QueueingRecordSink,
    /// Shared with the `BufferingDecoderSink` on the queueing worker;
    /// status loop polls without contending on the worker.
    decoder_stats: Arc<walshadow::decoder_sink::DecoderStats>,
    /// Shared with parallel pipeline's inserter pool (bumps counters
    /// post-`EndOfStream`). `None` when no CH pipeline is wired.
    emitter_stats: Option<Arc<walshadow::ch_emitter::EmitterStats>>,
}

impl RecordSink for DaemonSinks {
    fn on_record<'a>(
        &'a mut self,
        record: &'a Record<'a>,
    ) -> Pin<Box<dyn Future<Output = Result<(), SinkError>> + Send + 'a>> {
        Box::pin(async move {
            self.metrics.on_record(record).await?;
            self.decoder_xact.on_record(record).await?;
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
    /// Source PG host (TCP) or unix socket directory (leading `/`)
    #[arg(long, default_value = "localhost")]
    host: String,
    #[arg(long, default_value_t = 5432)]
    port: u16,
    #[arg(long, default_value = "postgres")]
    user: String,
    #[arg(long, default_value = "postgres")]
    dbname: String,
    /// Optional cleartext password. Replication-mode auth supports
    /// trust / cleartext / SCRAM-SHA-256.
    #[arg(long)]
    password: Option<String>,
    /// SSL mode: `disable`, `allow`, `prefer`, `require`, `verify-ca`,
    /// `verify-full`. Skipped on unix sockets regardless. verify-ca /
    /// verify-full consult `PGSSLROOTCERT` (else webpki bundle) for the
    /// trust anchor, same contract as libpq.
    #[arg(long, default_value = "prefer")]
    sslmode: String,
    /// Where filtered segments + manifests land; shadow PG's
    /// `restore_command` reads from here
    #[arg(long)]
    out_dir: PathBuf,
    /// Optional permanent physical slot name on source PG
    #[arg(long)]
    slot: Option<String>,
    /// Start LSN in `X/Y` hex form. Defaults to source's current
    /// `pg_current_wal_lsn` (per `IDENTIFY_SYSTEM`), aligned down to a
    /// segment boundary.
    #[arg(long)]
    start_lsn: Option<String>,
    #[arg(long, default_value_t = 10)]
    status_interval: u64,
    /// Stop after this many segments shipped (smoke tests). Zero = forever.
    #[arg(long, default_value_t = 0)]
    max_segments: u64,
    /// Shadow PG unix socket directory. Reused as libpq `host=` since
    /// libpq treats a leading `/` as a socket dir.
    #[arg(long)]
    shadow_socket_dir: PathBuf,
    #[arg(long, default_value_t = 5432)]
    shadow_port: u16,
    #[arg(long, default_value = "postgres")]
    shadow_user: String,
    #[arg(long, default_value = "postgres")]
    shadow_dbname: String,
    /// Wall-clock budget for the initial connect against shadow PG.
    /// Reused by [`with_transient_retry`] so a still-warming shadow
    /// doesn't fail the daemon on first boot.
    #[arg(long, default_value_t = 30)]
    shadow_connect_timeout: u64,
    /// Walsender bind address. `127.0.0.1:0` lets the kernel pick a free
    /// port; set explicitly when shadow's `primary_conninfo` references
    /// a fixed port.
    #[arg(long, default_value = "127.0.0.1:0")]
    walsender_bind: SocketAddr,
    /// File the daemon writes the bound walsender address into (one line
    /// `host:port`). For `--walsender-bind` port 0: operator reads it to
    /// learn the picked port and configures shadow's `primary_conninfo`.
    #[arg(long)]
    walsender_port_file: Option<PathBuf>,
    /// Slow-client backpressure: bytes queued onto a slow shadow
    /// connection before it's dropped + the wire falls back to
    /// `restore_command`.
    #[arg(long, default_value_t = 64 * 1024 * 1024)]
    walsender_slow_threshold: usize,
    /// Seconds the pump waits for shadow's walreceiver to attach before
    /// processing records. `0` disables the barrier (shadow driven purely
    /// via `restore_command`). `ShadowStreamSink` drops bytes pushed
    /// before a connection registers; without the barrier the pump can
    /// race past shadow's `START_REPLICATION` LSN and leave the catalog
    /// gate timed out against an apply LSN that never advances.
    #[arg(long, default_value_t = 60)]
    walsender_connect_timeout: u64,
    /// Soft cap on in-flight records for the `QueueingRecordSink` feeding
    /// the decoder / xact-drain worker. Past this watermark the pump
    /// yields to let the worker drain; a stuck worker still surfaces via
    /// the catalog `wait_for_replay` timeout on the err slot.
    #[arg(long, default_value_t = DEFAULT_QUEUEING_RECORD_SINK_CAPACITY)]
    decoder_queue_capacity: usize,
    /// Pump-side batch size for the `QueueingRecordSink`. Bigger
    /// amortises per-send overhead but adds pump→worker latency (worker's
    /// `wait_for_replay` lags one batch behind).
    #[arg(long, default_value_t = DEFAULT_QUEUEING_BATCH_SIZE)]
    decoder_batch_size: usize,
    /// Decode-pool size (M): parallel decode workers (detoast, type
    /// coercion, oracle resolution). Only with `--ch-config`. `1` keeps
    /// decode serial so per-table WAL order is preserved; M>1 relaxes
    /// per-table order, relying on `_lsn` ReplacingMergeTree dedup.
    #[arg(long, default_value_t = 1)]
    decoder_pool_size: usize,
    /// Insert-pool size (N): concurrent ClickHouse INSERT connections.
    /// Cloud throughput is RTT/part-commit bound, so N>1 is the main
    /// throughput lever. Only with `--ch-config`.
    #[arg(long, default_value_t = 1)]
    inserter_pool_size: usize,
    /// Xact / TOAST buffer spill dir. Wiped every startup per the
    /// crash-recovery contract in [plans/xact.md](../../plans/xact.md).
    #[arg(long)]
    spill_dir: PathBuf,
    /// In-memory xact buffer budget in bytes. Default matches PG's
    /// `logical_decoding_work_mem` (64 MiB).
    #[arg(long, default_value_t = walshadow::xact_buffer::DEFAULT_XACT_BUFFER_MAX)]
    xact_buffer_max: usize,
    /// CH-Native emitter config (TOML). Set → drained tuples ship to
    /// ClickHouse via `clickhouse-c-rs`; unset → metrics-only. Shape: see
    /// [`walshadow::ch_emitter::EmitterConfig::from_toml_str`]. Reloaded on
    /// SIGHUP (atomic mapping swap; connection params stay boot-only).
    #[arg(long)]
    ch_config: Option<PathBuf>,
    /// CLI override for the TOML's `[ch] flush_timeout_ms`. On the live
    /// pipeline `0` (default) selects a 100ms partial-batch deadline so
    /// cold tables can't pin the watermark; positive sets it explicitly.
    /// No per-xact-close path runs on the live drain (survives only in
    /// bootstrap backfill, forced internally). SIGHUP reads `--ch-config`
    /// only, so use this flag for the boot value when not maintaining the
    /// knob in TOML.
    #[arg(long)]
    ch_flush_timeout_ms: Option<u64>,
    /// Differential decode oracle: probe 1-in-`<N>` rows through shadow
    /// PG's `walshadow_decode_disk(oid, bytea)` extension function and
    /// assert the local decoder matches. `0` disables. Requires the
    /// `walshadow` extension on shadow PG; absent extension surfaces as
    /// `oracle fallback=N` and the daemon ships raw on-disk bytes for
    /// `PgPending` types.
    #[arg(long, default_value_t = 0)]
    validate: u32,
    /// HTTP/Prometheus metrics bind address. Disabled when absent.
    #[arg(long)]
    metrics_bind: Option<SocketAddr>,
    /// WAL retention horizon in bytes. Segments older than
    /// `shadow_replay_lsn - retention_bytes` deleted every trim cycle.
    /// `0` disables trim.
    #[arg(long, default_value_t = DEFAULT_RETENTION_BYTES)]
    retention_bytes: u64,
    /// Skip pre-flight validators (server_version_num, wal_level, REPLICA
    /// IDENTITY FULL, slot existence). For recovery drills.
    #[arg(long, default_value_t = false)]
    skip_preflight: bool,
    /// Ignore any `cursor.bin` under `--spill-dir` at boot (greenfield
    /// resume even when a prior daemon left one). For "wipe + restart
    /// from a known LSN" drills. Cursor still rewritten as the new daemon
    /// progresses.
    #[arg(long, default_value_t = false)]
    ignore_cursor: bool,
    /// Bootstrap source pick. `off` keeps shadow externally bootstrapped.
    /// `direct` issues BASE_BACKUP over the same replication connection;
    /// `object_store` pulls a wal-g-format backup from `DynStorage`
    /// (`WALG_*` env vars). Both modes land catalog files + standby.signal +
    /// restore_command to `--bootstrap-shadow-data-dir`, then resume the
    /// WAL pump at the backup's `end_lsn` (overriding cursor / `--start-lsn`).
    #[arg(long, value_enum, default_value_t = BootstrapMode::Off)]
    bootstrap_mode: BootstrapMode,
    /// Shadow PG data dir the bootstrap lands catalogs into; PG recovery's
    /// `restore_command` then consumes `out_dir/<seg>.partial` to replay
    /// WAL beyond `end_lsn`. Required when `--bootstrap-mode != off`. Must
    /// be empty (or non-existent) at boot.
    #[arg(long)]
    bootstrap_shadow_data_dir: Option<PathBuf>,
    /// Object-store backup name. `LATEST` resolves to newest sentinel;
    /// otherwise the literal `base_TTTTTTTTLLLLLLLLSSSSSSSS` form. Required
    /// when `--bootstrap-mode=object_store`.
    #[arg(long, default_value = "LATEST")]
    bootstrap_backup_name: String,
    /// Object-store fan-out parallelism. Raise for high-bandwidth buckets.
    #[arg(long, default_value_t = 4)]
    bootstrap_object_store_parallelism: usize,
    /// BASE_BACKUP fast-checkpoint flag for `direct` mode. `true` avoids
    /// waiting for source's checkpoint_timeout; flip off if checkpoint
    /// cost matters more than bootstrap latency.
    #[arg(long, default_value_t = true)]
    bootstrap_fast_checkpoint: bool,
    /// Auto-spawn shadow PG against `--bootstrap-shadow-data-dir` after
    /// the bootstrap pump returns (drives `pg_ctl start`, waits for
    /// `pg_last_wal_replay_lsn` to clear `end_lsn`). Off so external
    /// supervisors (systemd, k8s) own shadow lifecycle without
    /// double-management.
    #[arg(long, default_value_t = false)]
    bootstrap_autospawn_shadow: bool,
    /// Seconds budget for `--bootstrap-autospawn-shadow`'s wait on shadow's
    /// replay LSN. Exceeded → daemon aborts.
    #[arg(long, default_value_t = 300)]
    bootstrap_shadow_replay_timeout: u64,
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<()> {
    let args = Args::parse();
    init_tracing();
    run(args).await
}

/// `RUST_LOG` configures the filter; unset defaults to warn except
/// walshadow=info so the startup banner still emits.
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
        host: args.host.clone(),
        port: args.port,
        user: args.user.clone(),
        password: args.password.clone(),
        database: args.dbname.clone(),
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

    let ch_config = match args.ch_config.as_deref() {
        Some(path) => {
            let toml = tokio::fs::read_to_string(path)
                .await
                .with_context(|| format!("read --ch-config {}", path.display()))?;
            let mut cfg = EmitterConfig::from_toml_str(&toml).context("parse --ch-config")?;
            if let Some(ms) = args.ch_flush_timeout_ms {
                cfg.flush_timeout = std::time::Duration::from_millis(ms);
            }
            Some(cfg)
        }
        None => None,
    };
    let bootstrap_end_lsn: Option<u64> = if matches!(args.bootstrap_mode, BootstrapMode::Off) {
        None
    } else {
        Some(
            run_bootstrap(&cfg, &mut feed, &args, ch_config.clone())
                .await
                .context("bootstrap")?,
        )
    };
    if let Some(end_lsn) = bootstrap_end_lsn
        && args.bootstrap_autospawn_shadow
    {
        let shadow_data_dir = args
            .bootstrap_shadow_data_dir
            .clone()
            .context("--bootstrap-shadow-data-dir required with --bootstrap-autospawn-shadow")?;
        autospawn_shadow_and_wait(&args, shadow_data_dir, end_lsn).await?;
    }

    // Resume precedence: `--start-lsn` > cursor.bin emitter-ack > greenfield
    // (source write head). `--ignore-cursor` forces greenfield (recovery drills).
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
    // Bootstrap `end_lsn` outranks the cursor: shadow catalog state is at
    // `end_lsn`, so consuming WAL before it double-counts. `--start-lsn`
    // still wins so recovery drills can rewind further.
    let start_lsn_override = match &args.start_lsn {
        Some(s) => Some(parse_lsn(s).context("--start-lsn")?),
        None => None,
    };
    let raw_start = cursor::resolve_resume_lsn(
        start_lsn_override,
        bootstrap_end_lsn,
        cursor_at_boot.as_ref().map(|c| c.emitter_ack_lsn),
        ident.xlogpos,
    );
    let aligned = WalStream::align_down(raw_start, WAL_SEG_SIZE);
    tracing::info!(
        target: "walshadow",
        raw = format!("{:X}/{:X}", raw_start >> 32, raw_start as u32),
        aligned = format!("{:X}/{:X}", aligned >> 32, aligned as u32),
        from_bootstrap = bootstrap_end_lsn.is_some() && args.start_lsn.is_none(),
        from_cursor = bootstrap_end_lsn.is_none()
            && cursor_at_boot.is_some()
            && args.start_lsn.is_none()
            && cursor_at_boot.as_ref().is_some_and(|c| c.emitter_ack_lsn != 0),
        "start LSN",
    );

    let mut stream = WalStream::new(ident.timeline, WAL_SEG_SIZE, aligned)?;
    // Bind walsender listener BEFORE shadow's walreceiver can connect.
    // Without an active sink, the catalog gate inside `BufferingDecoderSink`
    // deadlocks: shadow's replay LSN never advances since segment-sink fires
    // after per-record dispatch in the current ordering.
    let shadow_state = Arc::new(Mutex::new(
        walshadow::shadow_stream::ShadowStreamState::new(
            ident.timeline,
            ident.sysid.clone(),
            aligned,
            args.walsender_slow_threshold,
        ),
    ));
    let walsender_listener = tokio::net::TcpListener::bind(args.walsender_bind)
        .await
        .with_context(|| format!("bind walsender at {}", args.walsender_bind))?;
    let walsender_addr = walsender_listener
        .local_addr()
        .context("walsender local_addr")?;
    drop(walsender_listener); // spawn_listener re-binds at the same addr
    if let Some(path) = &args.walsender_port_file {
        tokio::fs::write(path, format!("{}\n", walsender_addr))
            .await
            .with_context(|| format!("write walsender port file {}", path.display()))?;
    }
    let _walsender_task = walshadow::shadow_stream::spawn_listener(
        walshadow::shadow_stream::WalSenderAddr::Tcp(walsender_addr),
        shadow_state.clone(),
        Duration::from_millis(50),
    )
    .await
    .context("spawn walsender listener")?;
    tracing::info!(
        target: "walshadow",
        addr = %walsender_addr,
        "walsender listening — point shadow's primary_conninfo here",
    );
    stream.set_bytes_sink(Box::new(walshadow::shadow_stream::ShadowStreamSink::new(
        shadow_state.clone(),
    )));

    // Seed catalog tracker from source's current pg_class before
    // START_REPLICATION. Closes the "source rotated a mapped catalog above
    // 16384 pre-attach" hole the < 16384 bootstrap rule misses. Idempotent.
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

    // Connect shadow catalog before START_REPLICATION so the tracker→drain
    // wire is hot from the first record. with_transient_retry tolerates a
    // still-warming shadow on boot.
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

    // Descriptor-cache invalidation epoch: tracker bumps on every
    // catalog-touching record, catalog folds the delta into `invalidate`
    // before each relation lookup's cache check.
    let invalidation_epoch = Arc::new(AtomicU64::new(0));
    stream
        .filter_mut()
        .tracker
        .set_invalidation_epoch(invalidation_epoch.clone());
    // Narrower drop-only epoch so sweep_dropped throttles off pg_class
    // heap_delete, not every catalog touch (ADD COLUMN / CREATE INDEX
    // would flood at pgbench rate otherwise).
    let pg_class_delete_epoch = Arc::new(AtomicU64::new(0));
    stream
        .filter_mut()
        .tracker
        .set_pg_class_delete_epoch(pg_class_delete_epoch.clone());
    {
        let mut cat = catalog.lock().await;
        cat.set_invalidation_epoch(invalidation_epoch);
        cat.set_pg_class_delete_epoch(pg_class_delete_epoch.clone());
    }

    // Pre-flight validators run after both source + shadow SQL clients
    // are up so every check has its connection.
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
            ch_config: ch_config.as_ref(),
        })
        .await
        .context("pre-flight probe")?;
        report
            .into_result()
            .context("pre-flight rejected daemon start")?;
        tracing::info!(target: "walshadow::preflight", "pre-flight passed");
    }

    // Seed schema-diff baseline for operator-pinned relations before
    // START_REPLICATION so a pinned table's first post-start ALTER diffs
    // against boot shape (→ Changed → CH ALTER) rather than cold-prev_known
    // Added (apply_added skips pinned dests). cfg.tables.keys() is the
    // pinned set; auto-create tables baseline on first-touch CREATE.
    if let Some(cfg) = ch_config.as_ref() {
        let names: Vec<String> = cfg.tables.keys().cloned().collect();
        let seeded = catalog
            .lock()
            .await
            .seed_baseline(&names)
            .await
            .context("seed schema-diff baseline for mapped relations")?;
        tracing::info!(
            target: "walshadow",
            seeded,
            "seeded schema-diff baseline for mapped relations",
        );
    }

    // Oracle opens its own libpq connection so its queries don't pessimise
    // the catalog's query-one path. Best-effort: connect failure disables
    // the oracle, daemon keeps running with the raw-bytes fallback.
    let oracle = match walshadow::oracle::connect_with_budget(
        &shadow_conninfo,
        args.validate,
        connect_budget,
    )
    .await
    {
        Ok(o) => {
            let ext = o.has_extension();
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
    // Spill dir wiped every startup: cursor file commits drains
    // atomically, so leftover spill from a prior crash is redundant or stale.
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

    // Shared schema-events queue (descriptor-fetch + commit-boundary
    // `sweep_dropped`); both decoder and drain stage pull from it.
    let schema_events = Arc::new(std::sync::Mutex::new(catalog.lock().await.subscribe()));
    let decoder = BufferingDecoderSink::new(catalog.clone(), xact_buffer.clone())
        .with_schema_events(schema_events.clone());
    let decoder_stats_handle = decoder.stats_handle();

    let mut mapping_handle: Option<MappingHandle> = None;
    let mut emitter_stats_handle: Option<Arc<EmitterStats>> = None;
    let mut pipeline_handle: Option<PipelineHandle> = None;
    // When the pipeline is wired, the durable watermark comes from its ack
    // collector atomic instead of the xact buffer's synchronous field.
    let mut pipeline_ack: Option<Arc<AtomicU64>> = None;

    let decoder_xact = match ch_config {
        Some(emitter_cfg) => {
            let addr = format!("{}:{}", emitter_cfg.host, emitter_cfg.port);
            // SIGHUP-reloadable mapping shared by DDL applicator + decode pool.
            let mapping: MappingHandle =
                Arc::new(tokio::sync::RwLock::new(emitter_cfg.tables.clone()));
            mapping_handle = Some(mapping.clone());
            // DDL applicator owned by the reorder coordinator so ALTER /
            // CREATE / DROP / TRUNCATE apply inside the barrier, after
            // earlier data is durable.
            let ddl_cfg = walshadow::ch_ddl::DdlConfig::from_emitter(&emitter_cfg);
            let applicator =
                walshadow::ch_ddl::DdlApplicator::new(&emitter_cfg, ddl_cfg, mapping.clone())
                    .await
                    .context("init DDL applicator")?;
            let stats = Arc::new(EmitterStats::default());
            emitter_stats_handle = Some(stats.clone());
            // Seed durable watermark at `raw_start`, not 0. Status loop
            // persists this atomic into cursor's emitter_ack_lsn each
            // interval with no monotonic guard, first write at boot before
            // any WAL re-read acks. Seeding 0 would clobber a resumed
            // cursor; a crash before [aligned, raw_start] re-reads acks
            // then resumes from ident.xlogpos next boot (precedence skips a
            // zero cursor ack), silently dropping [raw_start, head] WAL that
            // never reached CH. tail's `fetch_max` keeps it monotonic as WAL
            // re-reads [aligned, raw_start]. See
            // plans/future/pipeline_backpressure_and_scaling.md (Handoff step 3).
            let emitter_ack = Arc::new(AtomicU64::new(raw_start));
            pipeline_ack = Some(emitter_ack.clone());
            let pcfg = PipelineConfig {
                emitter: emitter_cfg,
                decoder_pool_size: args.decoder_pool_size,
                inserter_pool_size: args.inserter_pool_size,
                catalog: catalog.clone(),
                mapping,
                oracle: oracle.clone(),
                applicator,
                buffer: xact_buffer.clone(),
                subxact_tracker: Arc::new(Mutex::new(SubxactTracker::new())),
                schema_events: Some(schema_events.clone()),
                pg_class_delete_epoch: Some(pg_class_delete_epoch.clone()),
                stats,
            };
            let (reorder_sink, handle) = pcfg
                .spawn(emitter_ack)
                .await
                .context("spawn decode+insert pipeline")?;
            pipeline_handle = Some(handle);
            tracing::info!(
                target: "walshadow::pipeline",
                addr = %addr,
                decoders = args.decoder_pool_size.max(1),
                inserters = args.inserter_pool_size.max(1),
                "parallel decode+insert pipeline started",
            );
            QueueingRecordSink::spawn(
                DecoderXactPair {
                    decoder,
                    xact_drain: reorder_sink,
                },
                args.decoder_batch_size,
                args.decoder_queue_capacity,
            )
        }
        None => {
            // Metrics-only (no CH): serial drain to counters. Oracle wrapper
            // resolves PgPending + fires validator probes when up.
            let observer: Box<dyn TupleObserver> = match oracle.clone() {
                Some(o) => Box::new(walshadow::oracle::OracleObserver::new(
                    o,
                    Box::new(MetricsTupleObserver::default()) as Box<dyn TupleObserver>,
                )),
                None => Box::new(MetricsTupleObserver::default()),
            };
            let xact_drain = XactRecordSink::new(xact_buffer.clone(), catalog.clone(), observer)
                .with_schema_events(schema_events)
                .with_pg_class_delete_epoch(pg_class_delete_epoch.clone());
            QueueingRecordSink::spawn(
                DecoderXactPair {
                    decoder,
                    xact_drain,
                },
                args.decoder_batch_size,
                args.decoder_queue_capacity,
            )
        }
    };
    let mut record_sink = DaemonSinks {
        metrics: MetricsRecordSink::default(),
        decoder_xact,
        decoder_stats: decoder_stats_handle,
        emitter_stats: emitter_stats_handle,
    };
    let mut segment_sink = DirSegmentSink::new(args.out_dir.clone()).context("open out-dir")?;
    let mut chunk_buf = Vec::with_capacity(64 * 1024);

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

    // SIGHUP swaps only the per-relation mapping; connection params stay boot-only.
    let sighup_path = args.ch_config.clone();
    let sighup_handle = mapping_handle.clone();
    let _sighup_task = spawn_sighup_handler(sighup_path, sighup_handle);

    // Retention sweeper writes shadow's `pg_last_wal_replay_lsn` here;
    // status loop reads it for the cursor's `shadow_replay_lsn` slot + the
    // standby-status `apply_lsn` ceiling.
    let shadow_replay_lsn = Arc::new(AtomicU64::new(0));
    // Aggregate flush across ShadowStreamSink connections, fed into the
    // cursor for shadow's `START_REPLICATION PHYSICAL` resume on restart.
    let shadow_flush_lsn = Arc::new(AtomicU64::new(0));

    // Retention sweeper drops filtered segments more than `retention_bytes`
    // behind shadow's replay LSN. Its poll doubles as the only feed of
    // `shadow_replay_lsn`, sparing the main loop a second shadow connection.
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

    // Block until shadow's walreceiver attaches. `ShadowStreamSink::
    // on_wire_chunk` drops bytes with no connection registered, so a pump
    // racing past `START_REPLICATION`'s LSN before walreceiver arrives
    // leaves an unrecoverable gap: post-conn frames carry LSNs past
    // walreceiver's expected continuity, shadow's apply stalls, the catalog
    // gate times out (pgbench_acceptance / kill_restart failure mode). Cap
    // the wait so operators without a streaming shadow still boot via the
    // restore_command archive path.
    if args.walsender_connect_timeout > 0 {
        let timeout = Duration::from_secs(args.walsender_connect_timeout);
        let start = Instant::now();
        let mut attached = false;
        loop {
            if shadow_state.lock().await.aggregate().active_connections > 0 {
                attached = true;
                break;
            }
            if start.elapsed() >= timeout {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        if attached {
            tracing::info!(
                target: "walshadow",
                wait = ?start.elapsed(),
                "walsender connected — starting pump",
            );
        } else {
            tracing::warn!(
                target: "walshadow",
                timeout_secs = args.walsender_connect_timeout,
                "no walsender connection within boot barrier — proceeding (shadow on restore_command path)",
            );
        }
    }

    let start_instant = Instant::now();
    let mut segments_shipped = 0u64;
    let mut prev_dispatched = stream.dispatched_lsn();
    let mut rate_estimator = RateEstimator::default();
    // Cursor write cadence matches standby-status cadence so the file's
    // `emitter_ack_lsn` is ≥ the `apply_lsn` advertised to source on every
    // send; else the slot could advance past a not-yet-durable resume point.
    let cursor_write_interval = Duration::from_secs(args.status_interval);
    let mut last_cursor_write: Option<Instant> = None;
    // Inflight-stall watchdog: xacts_active > 0 with stalled
    // `emitter_ack_lsn` dumps the parked xids holding the slot. One-shot
    // per stall, re-arms when ack advances.
    let mut last_emitter_ack_observed: u64 = 0;
    let mut inflight_stall_since: Option<Instant> = None;
    let mut inflight_stall_logged = false;
    let shutdown_reason = loop {
        // dispatched_lsn is filter-durable since DirSegmentSink fsyncs each
        // segment + the parent dir. shadow_replay 0 when retention is off.
        let dispatched = stream.dispatched_lsn();
        let received = feed.last_server_wal_end().max(dispatched);
        let shadow_replay = shadow_replay_lsn.load(Ordering::Acquire);
        let shadow_agg = shadow_state.lock().await.aggregate();
        if let Some(flush) = shadow_agg.min_flush_lsn {
            shadow_flush_lsn.fetch_max(flush, Ordering::Release);
        }
        let (drain_lsn, emitter_ack_lsn) = {
            let b = xact_buffer.lock().await;
            let s = b.stats();
            // Durable watermark: pipeline → ack collector atomic;
            // serial/metrics → xact buffer field.
            let ea = match &pipeline_ack {
                Some(a) => a.load(Ordering::Acquire),
                None => s.emitter_ack_lsn,
            };
            (s.drain_lsn, ea)
        };
        // shadow_replay==0 (sweeper off or not yet reported) means "no
        // constraint from shadow", not the literal min: else a fresh boot
        // with retention off pins apply_lsn at 0 and source's slot never recycles.
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
            shadow_flush_lsn: shadow_flush_lsn.load(Ordering::Acquire),
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
        let dispatched_before = stream.dispatched_lsn();
        let chunk = tokio::select! {
            biased;
            // ctrl_c first so it doesn't lose to a chunk already at the queue head.
            sig = tokio::signal::ctrl_c() => {
                sig.context("install ctrl_c handler")?;
                break "signal";
            }
            // Idle tick: `emitter_ack_lsn` advances in the queueing worker
            // after the pump pushes, so without a periodic wakeup an idle
            // source (kill-restart post-catchup quiescence) freezes metrics
            // + cursor at the last chunk's values.
            _ = tokio::time::sleep(cursor_write_interval) => None,
            res = feed.next_chunk(status, &mut chunk_buf) => Some(match res? {
                Some(c) => c,
                None => break "CopyDone",
            }),
        };
        let server_end = chunk.as_ref().map(|c| c.server_wal_end).unwrap_or(received);
        if let Some(chunk) = chunk {
            stream
                .push(
                    chunk.start_lsn,
                    chunk.data,
                    &mut record_sink,
                    &mut segment_sink,
                )
                .await?;
        }
        // Flush pump-side accumulator so partial batches don't strand
        // commits in `decoder_xact.buf` when source goes idle (kill-restart
        // post-catchup quiescence).
        record_sink
            .decoder_xact
            .flush()
            .await
            .context("flush queueing decoder sink")?;
        // Surface a pipeline-stage failure as a clean daemon exit with the
        // root cause rather than a silently pinned watermark.
        if let Some(h) = &pipeline_handle
            && let Some(msg) = h.fatal.message()
        {
            anyhow::bail!("decode+insert pipeline failed: {msg}");
        }
        let now_dispatched = stream.dispatched_lsn();
        let advanced = now_dispatched != prev_dispatched;
        let (xact_stats, xact_line) = {
            let b = xact_buffer.lock().await;
            let mut stats = b.stats().clone();
            // Pipeline mode: track the ack collector's watermark (real CH
            // durability), not the unused xact-buffer field.
            if let Some(a) = &pipeline_ack {
                stats.emitter_ack_lsn = a.load(Ordering::Acquire);
            }
            let line = stats.summary();
            (stats, line)
        };
        let oracle_line = match &oracle {
            Some(o) => o.stats.summary(),
            None => String::new(),
        };
        let oracle_stats = oracle.as_ref().map(|o| o.stats.as_ref());
        let decoder_stats: &walshadow::decoder_sink::DecoderStats = &record_sink.decoder_stats;
        let emitter_stats: Option<&walshadow::ch_emitter::EmitterStats> =
            record_sink.emitter_stats.as_deref();
        let shadow_apply_lsn = shadow_agg.min_apply_lsn.unwrap_or(0);
        let lag_bytes = received.saturating_sub(shadow_apply_lsn);
        rate_estimator.observe(Instant::now(), received);
        let lag_seconds = rate_estimator.seconds_for(lag_bytes);
        // Post-worker stats so the metric reflects what the worker drained,
        // not the top-of-iteration snapshot.
        let emitter_ack_for_metric = xact_stats.emitter_ack_lsn;
        let drain_for_metric = xact_stats.drain_lsn;
        populate_metrics(
            &metrics,
            received,
            now_dispatched,
            shadow_replay,
            drain_for_metric,
            emitter_ack_for_metric,
            &record_sink.metrics,
            &xact_stats,
            decoder_stats,
            emitter_stats,
            oracle_stats,
            start_instant.elapsed().as_secs(),
            ShadowMetricsView {
                apply_lag_bytes: lag_bytes,
                apply_lag_seconds: lag_seconds,
                active_connections: shadow_agg.active_connections as u64,
                dropped_total: shadow_agg.dropped_total,
            },
        )
        .await;
        if advanced {
            let new_segs = (now_dispatched - prev_dispatched) / WAL_SEG_SIZE;
            segments_shipped += new_segs;
            prev_dispatched = now_dispatched;
            let ahead = server_end.saturating_sub(dispatched_before);
            let filter = stream.filter();
            tracing::info!(
                target: "walshadow",
                segments_shipped,
                last_lsn = format!("{:X}/{:X}", now_dispatched >> 32, now_dispatched as u32),
                shadow_apply = format!("{:X}/{:X}", shadow_apply_lsn >> 32, shadow_apply_lsn as u32),
                source_ahead_bytes = ahead,
                metrics = %record_sink.metrics.summary(),
                kept = filter.stats.kept,
                dropped = filter.stats.dropped,
                relmap_updates = filter.tracker.relmap_updates,
                pg_class_undecoded = filter.tracker.pg_class_writes_undecoded,
                pg_class_oid_in_prefix = filter.tracker.pg_class_writes_oid_in_prefix,
                decoder = %decoder_stats.summary(),
                xact_buffer = %xact_line,
                oracle = %oracle_line,
                "status",
            );
            if args.max_segments != 0 && segments_shipped >= args.max_segments {
                break "max-segments";
            }
        }
        // Re-arm on ack move; else after 5s of stall with parked xacts dump
        // the xids once. Runs independent of `advanced` so a fully-quiescent
        // pump still surfaces who's holding the slot.
        if xact_stats.emitter_ack_lsn != last_emitter_ack_observed {
            last_emitter_ack_observed = xact_stats.emitter_ack_lsn;
            inflight_stall_since = None;
            inflight_stall_logged = false;
        }
        if xact_stats.xacts_active > 0 {
            let since = inflight_stall_since.get_or_insert(Instant::now());
            if !inflight_stall_logged && since.elapsed() >= Duration::from_secs(5) {
                let snap = xact_buffer.lock().await.inflight_snapshot();
                let summary: String = snap
                    .iter()
                    .map(|e| {
                        format!(
                            "xid={} lsn={:X}/{:X}..{:X}/{:X} heap={} chunk={} bytes={} spill={} cat={} rels=[{}]",
                            e.xid,
                            e.first_lsn >> 32,
                            e.first_lsn as u32,
                            e.last_lsn >> 32,
                            e.last_lsn as u32,
                            e.heap_count,
                            e.chunk_count,
                            e.in_mem_bytes,
                            if e.spilled { "y" } else { "n" },
                            e.catalog_events,
                            e.rels,
                        )
                    })
                    .collect::<Vec<_>>()
                    .join(" | ");
                tracing::warn!(
                    target: "walshadow",
                    xacts_active = xact_stats.xacts_active,
                    emitter_ack_lsn = format!(
                        "{:X}/{:X}",
                        xact_stats.emitter_ack_lsn >> 32,
                        xact_stats.emitter_ack_lsn as u32,
                    ),
                    drain_lsn = format!(
                        "{:X}/{:X}",
                        xact_stats.drain_lsn >> 32,
                        xact_stats.drain_lsn as u32,
                    ),
                    source_received = format!(
                        "{:X}/{:X}",
                        received >> 32,
                        received as u32,
                    ),
                    filter_dispatched = format!(
                        "{:X}/{:X}",
                        now_dispatched >> 32,
                        now_dispatched as u32,
                    ),
                    inflight = %summary,
                    "xact inflight parked — emitter ack pinned by these xids",
                );
                inflight_stall_logged = true;
            }
        } else {
            inflight_stall_since = None;
            inflight_stall_logged = false;
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
    // Drain queueing worker so enqueued-but-undispatched records run
    // through decoder + xact_drain before exit; surfaces worker-parked errors.
    let DaemonSinks { decoder_xact, .. } = record_sink;
    decoder_xact
        .close()
        .await
        .context("drain queueing decoder sink on shutdown")?;
    // Worker close dropped the reorder sink, closing the decode job queue.
    // Drain rest in order (decoders → batcher force-flush → inserters to
    // EndOfStream → ack collector) so no rows are lost + final watermark durable.
    if let Some(handle) = pipeline_handle {
        handle
            .join()
            .await
            .map_err(|m| anyhow::anyhow!("decode+insert pipeline drain failed: {m}"))?;
    }
    Ok(())
}

/// tokio_postgres client against shadow over its unix socket, for
/// [`walshadow::preflight::run`] which needs SQL access independent of
/// [`ShadowCatalog`]'s replay-LSN-gated path.
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

/// SIGHUP listener: re-parses `--ch-config` and atomically swaps the live
/// mapping. Parse errors keep the existing mapping; absent `--ch-config`
/// is a no-op tap.
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

/// Retention sweeper: every [`DEFAULT_TRIM_INTERVAL`], query shadow's
/// `pg_last_wal_replay_lsn()` and trim segments older than
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

/// Shadow-side numbers for the metrics publish step, from
/// [`ShadowStreamState::aggregate`](walshadow::shadow_stream::ShadowStreamState::aggregate)
/// + the daemon's [`RateEstimator`].
struct ShadowMetricsView {
    apply_lag_bytes: u64,
    apply_lag_seconds: f64,
    active_connections: u64,
    dropped_total: u64,
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
    emitter_stats: Option<&walshadow::ch_emitter::EmitterStats>,
    oracle_stats: Option<&walshadow::oracle::OracleStats>,
    uptime_secs: u64,
    shadow_view: ShadowMetricsView,
) {
    use std::collections::BTreeMap;
    use walshadow::classify::rmgr_label;
    let mut by_rm = BTreeMap::new();
    for ((rm, route), n) in &rec_metrics.by_rm_route {
        let key = (
            rmgr_label(*rm).to_string(),
            match route {
                walshadow::filter::Route::ToShadow => "to_shadow",
                walshadow::filter::Route::ToDecoder => "to_decoder",
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
        records_by_rm_route: by_rm,
        xact_active: xact_stats.xacts_active,
        xact_bytes_in_memory: xact_stats.bytes_in_memory,
        spill_xacts_active: xact_stats.spill_xacts_active,
        spill_bytes_active: xact_stats.spill_bytes_active,
        spill_evictions_total: xact_stats.spill_evictions_total,
        xacts_committed_total: xact_stats.committed_xacts_total,
        xacts_aborted_total: xact_stats.aborted_xacts_total,
        decoder_decoded_total: decoder_stats.decoded.load(Ordering::Relaxed),
        decoder_partial_total: decoder_stats.partial.load(Ordering::Relaxed),
        decoder_toast_chunks_total: decoder_stats.toast_chunks_buffered.load(Ordering::Relaxed),
        decoder_toast_malformed_total: decoder_stats.toast_chunks_malformed.load(Ordering::Relaxed),
        emitter_rows_total: emitter_stats
            .map(|s| s.rows_emitted.load(Ordering::Relaxed))
            .unwrap_or(0),
        emitter_blocks_total: emitter_stats
            .map(|s| s.blocks_sent.load(Ordering::Relaxed))
            .unwrap_or(0),
        emitter_xacts_total: emitter_stats
            .map(|s| s.xacts_committed.load(Ordering::Relaxed))
            .unwrap_or(0),
        emitter_unsupported_relations: emitter_stats
            .map(|s| s.unsupported_relations.load(Ordering::Relaxed))
            .unwrap_or(0),
        oracle_resolved_total: oracle_stats
            .map(|s| s.resolved.load(Ordering::Relaxed))
            .unwrap_or(0),
        oracle_fallback_raw_total: oracle_stats
            .map(|s| s.fallback_raw.load(Ordering::Relaxed))
            .unwrap_or(0),
        oracle_validate_sampled_total: oracle_stats
            .map(|s| s.probes.load(Ordering::Relaxed))
            .unwrap_or(0),
        oracle_validate_mismatches_total: oracle_stats
            .map(|s| s.mismatches.load(Ordering::Relaxed))
            .unwrap_or(0),
        oracle_errors_total: oracle_stats
            .map(|s| s.errors.load(Ordering::Relaxed))
            .unwrap_or(0),
        uptime_secs,
        shadow_apply_lag_bytes: shadow_view.apply_lag_bytes,
        shadow_apply_lag_seconds: shadow_view.apply_lag_seconds,
        shadow_stream_active_connections: shadow_view.active_connections,
        shadow_stream_dropped_connections_total: shadow_view.dropped_total,
    };
    registry.set(snap).await;
}

/// Orchestrate BASE_BACKUP into a fresh shadow data dir; returns the
/// backup's `end_lsn` so the caller rebinds the WAL pump past it.
///
/// Operator contract after this returns:
///
/// 1. Bring up shadow PG with `standby.signal` + `restore_command`
///    pointing at `--out-dir` (both written here).
/// 2. Shadow consumes filtered WAL segments the daemon produces against
///    `--out-dir` and replays them.
/// 3. No automatic shadow-process management; a service manager (systemd,
///    k8s) owning shadow should start it once `bootstrap_shadow_data_dir`
///    exists and is non-empty (true once this returns).
///
/// `ch_config` `Some`: bootstrap rows route through the shared insert tail
/// (synthetic INSERT `_op = 1`, `_lsn = start_lsn`, `_commit_ts = 0`).
/// `wait_through(K)` proves every bootstrap seq durable on CH before
/// teardown, so the WAL pump resumes against a fully-shipped baseline.
/// `None`: rows drain to a metrics-only observer via `drain_backfill`.
async fn run_bootstrap(
    src_cfg: &PgConfig,
    feed: &mut SourceFeed,
    args: &Args,
    ch_config: Option<EmitterConfig>,
) -> Result<u64> {
    let shadow_data_dir = args
        .bootstrap_shadow_data_dir
        .clone()
        .context("--bootstrap-shadow-data-dir required when --bootstrap-mode != off")?;

    // Seed catalog map inside a REPEATABLE READ snapshot. DDL between the
    // seed COMMIT and BASE_BACKUP's checkpoint window is operator-quiesced
    // per the bootstrap out-of-scope contract.
    let sql_client = feed
        .sql_client()
        .await
        .context("bootstrap: source sidecar sql client")?;
    let catalog_map = seed_in_snapshot(sql_client)
        .await
        .context("bootstrap: seed_in_snapshot")?;
    tracing::info!(
        target: "walshadow::bootstrap",
        relations = catalog_map.len(),
        mode = ?args.bootstrap_mode,
        shadow_data_dir = %shadow_data_dir.display(),
        "catalog map seeded",
    );

    // Object-store mode retains `(settings, storage)` so the post-pump
    // hydrate can pull WAL `[start_lsn, end_lsn]` from `wal_005/` into
    // shadow's `pg_wal/`. Direct mode ships WAL inside `base.tar` via
    // `BaseBackupOpts { wal: true }`, so no follow-up fetch.
    type ObjectStoreHandles = (wal_rs::config::Settings, wal_rs::storage::DynStorage);
    let (source, object_store_handles): (Box<dyn BackupSource>, Option<ObjectStoreHandles>) =
        match args.bootstrap_mode {
            BootstrapMode::Direct => {
                let opts = BaseBackupOpts {
                    label: format!(
                        "walshadow-bootstrap-{}",
                        chrono::Utc::now().format("%Y%m%dT%H%M%SZ")
                    ),
                    fast_checkpoint: args.bootstrap_fast_checkpoint,
                    no_verify_checksums: false,
                    max_rate_kib: None,
                    // Ship pg_wal [start_lsn, end_lsn] inside base.tar so the
                    // auto-spawned shadow hits `minRecoveryPoint` from local WAL
                    // alone. Else `pg_ctl -w start` polls `restore_command`
                    // against an `out/` dir the streamer hasn't filled yet
                    // (queued behind autospawn) and times out.
                    wal: true,
                };
                (Box::new(DirectSource::new(src_cfg.clone(), opts)), None)
            }
            BootstrapMode::ObjectStore => {
                let settings = wal_rs::config::Settings::from_env()
                    .context("bootstrap: Settings::from_env (WALG_* env vars)")?;
                let storage = settings
                    .build_storage()
                    .context("bootstrap: build storage from WALG_* env vars")?;
                // ObjectStoreSource canonicalises via
                // `wal_rs::pg::backup::fetch::resolve_name`.
                let name = args.bootstrap_backup_name.clone();
                if name != "LATEST" && !name.starts_with(BACKUP_NAME_PREFIX) {
                    anyhow::bail!(
                        "bootstrap: --bootstrap-backup-name {name:?} must be `LATEST` \
                         or begin with `{BACKUP_NAME_PREFIX}`"
                    );
                }
                let src = ObjectStoreSource::new(settings.clone(), storage.clone(), name)
                    .with_parallelism(args.bootstrap_object_store_parallelism);
                (Box::new(src), Some((settings, storage)))
            }
            BootstrapMode::Off => unreachable!("dispatch happened in run()"),
        };

    let cfg = BootstrapConfig::new(shadow_data_dir.clone());
    // Tail drain gets a second CatalogMap clone for rfn → descriptor
    // lookups; cheap since `Arc<RelDescriptor>` values stay shared.
    let drain_catalog = catalog_map.clone();
    let (rx, pump) = spawn_greenfield_bootstrap(cfg, source, catalog_map);

    let (shipped, outcome) = match ch_config {
        Some(emitter_cfg) => {
            // Route bootstrap rows through the shared insert tail. Bootstrap
            // is the easy case: every row op=Insert at _lsn = start_lsn, no
            // aborts / TRUNCATE / DDL. Keep operator's flush_timeout; tail
            // defaults 0 to its own partial-flush deadline.
            let addr = format!("{}:{}", emitter_cfg.host, emitter_cfg.port);
            let stats = Arc::new(EmitterStats::default());
            // Throwaway watermark atomic: durability proof is `wait_through(K)`,
            // resume LSN is carried via the WAL pipeline's emitter_ack seed
            // (see `run`), so uniform `commit_lsn = start_lsn` here is fine.
            let emitter_ack = Arc::new(AtomicU64::new(0));
            let fatal = Fatal::new();
            let inserter_pool_size = args.inserter_pool_size;
            let (msg_tx, ack, tail) = tail::spawn(
                &emitter_cfg,
                inserter_pool_size,
                stats.clone(),
                emitter_ack,
                fatal.clone(),
            )
            .await
            .context("bootstrap: spawn insert tail")?;
            // Static [table.*] mapping (no SIGHUP, no shadow PG during bootstrap).
            let mapping: MappingHandle =
                Arc::new(tokio::sync::RwLock::new(emitter_cfg.tables.clone()));
            tracing::info!(
                target: "walshadow::bootstrap",
                addr = %addr,
                inserters = inserter_pool_size.max(1),
                "bootstrap insert tail started",
            );

            let drain = tokio::spawn(bootstrap::drain(
                rx,
                drain_catalog,
                mapping,
                msg_tx.clone(),
                ack.clone(),
                stats.clone(),
            ));
            let (drain_res, pump_res) = tokio::join!(drain, pump);
            let drain_outcome = drain_res
                .context("bootstrap drain join")?
                .map_err(|e| anyhow::anyhow!("bootstrap drain: {e}"))?;
            let outcome: BootstrapOutcome = pump_res
                .context("bootstrap pump join")?
                .context("bootstrap pump")?;
            let k = drain_outcome.next_seq;

            tail.finish(msg_tx, ack, k, &fatal)
                .await
                .map_err(|m| anyhow::anyhow!("bootstrap: {m}"))?;
            tracing::info!(
                target: "walshadow::bootstrap",
                rows_routed = drain_outcome.rows_routed,
                rows_emitted = stats.rows_emitted.load(Ordering::Relaxed),
                blocks_sent = stats.blocks_sent.load(Ordering::Relaxed),
                seqs = k,
                "bootstrap insert tail drained",
            );
            (drain_outcome.rows_routed, outcome)
        }
        None => {
            // Metrics-only: bootstrap rows counted, not shipped.
            let mut observer = MetricsTupleObserver::default();
            let (drain_res, pump_res) = tokio::join!(drain_backfill(rx, &mut observer), pump);
            let shipped = drain_res.context("bootstrap drain")?;
            let outcome: BootstrapOutcome = pump_res
                .context("bootstrap pump join")?
                .context("bootstrap pump")?;
            (shipped, outcome)
        }
    };

    tracing::info!(
        target: "walshadow::bootstrap",
        start_lsn = format!(
            "{:X}/{:X}",
            outcome.start.start_lsn >> 32,
            outcome.start.start_lsn as u32
        ),
        end_lsn = format!(
            "{:X}/{:X}",
            outcome.end.end_lsn >> 32,
            outcome.end.end_lsn as u32
        ),
        timeline = outcome.start.timeline,
        kept_files = outcome.disk.kept_files,
        skipped_denylist = outcome.disk.skipped_denylist,
        files_walked = outcome.page_walk.files_walked,
        tuples_emitted = outcome.page_walk.tuples_emitted,
        drained = shipped,
        "bootstrap landed",
    );

    // Object-store mode: hydrate shadow's pg_wal/ before pg_ctl. wal-g
    // backups keep WAL in wal_005/ separately (direct mode shipped it in
    // base.tar). Skipping deadlocks autospawn_shadow_and_wait: empty out/
    // dir for restore_command, walsender not yet bound.
    if let Some((settings, storage)) = object_store_handles {
        fetch_wal_into_pg_wal(
            &settings,
            storage,
            &shadow_data_dir,
            outcome.start.start_lsn,
            outcome.end.end_lsn,
            outcome.start.timeline,
        )
        .await
        .context("bootstrap: hydrate shadow pg_wal from object store")?;
    }

    // primary_conninfo points at the daemon's walsender; restore_command
    // is the archive fallback. PG's walreceiver tries the wire first, falls
    // back on disconnect or end-of-WAL.
    write_standby_config(&shadow_data_dir, &args.out_dir, args.walsender_bind)
        .context("bootstrap: write standby.signal + primary_conninfo + restore_command")?;

    // PG refuses to start on a data dir whose mode isn't 0700 or 0750.
    // BASE_BACKUP tar carries no entry for the root, so extraction leaves
    // it at the process umask (typically 0755); reassert 0700 before pg_ctl.
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let perms = fs::Permissions::from_mode(0o700);
        tokio::fs::set_permissions(&shadow_data_dir, perms)
            .await
            .with_context(|| format!("bootstrap: chmod 0700 {}", shadow_data_dir.display()))?;
    }

    Ok(outcome.end.end_lsn)
}

/// Boot shadow PG against the bootstrapped data dir and wait until its
/// replay LSN clears `end_lsn`. Sync `pg_ctl` / `psql` shells run via
/// `block_in_place` so the runtime keeps progressing other tasks.
/// Reuses `--shadow-socket-dir` / `--shadow-port` (same socket
/// `ShadowCatalog` connects to later).
async fn autospawn_shadow_and_wait(
    args: &Args,
    shadow_data_dir: PathBuf,
    end_lsn: u64,
) -> Result<()> {
    // BASE_BACKUP ships source's postgresql.conf verbatim; without override
    // shadow inherits source's port / listen_addresses / socket dir and
    // collides with the still-running source. Write last-wins overrides into
    // postgresql.auto.conf so the clone comes up on the `--shadow-*` values.
    write_shadow_listener_overrides(&shadow_data_dir, args.shadow_port, &args.shadow_socket_dir)
        .context("bootstrap: write shadow listener overrides")?;

    let mut cfg = ShadowConfig::new(shadow_data_dir.clone(), args.out_dir.clone());
    cfg.port = args.shadow_port;
    cfg.socket_dir = args.shadow_socket_dir.clone();
    cfg.ctl_timeout = Duration::from_secs(args.shadow_connect_timeout);
    let shadow = Shadow::new(cfg);
    let timeout = Duration::from_secs(args.bootstrap_shadow_replay_timeout);

    tracing::info!(
        target: "walshadow::bootstrap",
        data_dir = %shadow_data_dir.display(),
        end_lsn = format!("{:X}/{:X}", end_lsn >> 32, end_lsn as u32),
        replay_timeout_secs = args.bootstrap_shadow_replay_timeout,
        "auto-spawning shadow PG",
    );
    let replay_lsn = tokio::task::block_in_place(move || -> Result<u64> {
        shadow.start().context("auto-spawn: shadow start")?;
        shadow
            .wait_for_replay(end_lsn, timeout)
            .context("auto-spawn: wait_for_replay")
    })?;
    tracing::info!(
        target: "walshadow::bootstrap",
        replay_lsn = format!("{:X}/{:X}", replay_lsn >> 32, replay_lsn as u32),
        "shadow caught up to bootstrap end_lsn",
    );
    Ok(())
}

/// Write `standby.signal` (zero-byte marker) + append `restore_command`
/// so shadow starts in standby mode and feeds from the daemon's
/// filtered-segment dir. Idempotent (appended once).
fn write_standby_config(
    shadow_data_dir: &Path,
    filter_out_dir: &Path,
    walsender_bind: SocketAddr,
) -> Result<()> {
    fs::create_dir_all(shadow_data_dir)?;
    fs::write(shadow_data_dir.join("standby.signal"), b"")?;
    let conf = shadow_data_dir.join("postgresql.auto.conf");
    let marker = "# walshadow bootstrap";
    if conf.exists() {
        let existing = fs::read_to_string(&conf).unwrap_or_default();
        if existing.contains(marker) {
            return Ok(());
        }
    }
    let mut f = fs::OpenOptions::new()
        .append(true)
        .create(true)
        .open(&conf)?;
    writeln!(f, "\n{marker}")?;
    // Skip primary_conninfo when port=0: kernel-picked addresses are
    // unstable across daemon restarts, fall back to archive-only.
    if walsender_bind.port() != 0 {
        writeln!(
            f,
            "primary_conninfo = 'host={} port={} user=walshadow application_name=shadow sslmode=disable'",
            walsender_bind.ip(),
            walsender_bind.port(),
        )?;
    }
    writeln!(
        f,
        "restore_command = 'cp {}/%f %p'",
        filter_out_dir.display()
    )?;
    Ok(())
}

/// Append shadow-side `port` / `unix_socket_directories` /
/// `listen_addresses` to the cloned data dir's `postgresql.auto.conf`.
/// PG honours last-wins-per-key across the conf chain, so these override
/// what source's conf carried into the BASE_BACKUP. Idempotent via a marker.
///
/// `listen_addresses = ''` disables TCP: shadow is local-only over the
/// socket dir. Operators wanting TCP override via `ALTER SYSTEM SET
/// listen_addresses = ...` after first boot.
fn write_shadow_listener_overrides(
    shadow_data_dir: &Path,
    port: u16,
    socket_dir: &Path,
) -> Result<()> {
    let conf = shadow_data_dir.join("postgresql.auto.conf");
    let marker = "# walshadow shadow-listener overrides";
    if conf.exists() {
        let existing = fs::read_to_string(&conf).unwrap_or_default();
        if existing.contains(marker) {
            return Ok(());
        }
    }
    let mut f = fs::OpenOptions::new()
        .append(true)
        .create(true)
        .open(&conf)?;
    writeln!(f, "\n{marker}")?;
    writeln!(f, "port = {port}")?;
    writeln!(f, "unix_socket_directories = '{}'", socket_dir.display())?;
    writeln!(f, "listen_addresses = ''")?;
    Ok(())
}

/// Pull WAL `[start_lsn, end_lsn]` on `timeline` from wal-rs storage into
/// `<shadow_data_dir>/pg_wal/` so auto-spawned shadow recovery hits
/// `minRecoveryPoint` from local WAL, depending on neither `restore_command`
/// (filtered out-dir empty until the post-autospawn WAL pump) nor
/// `primary_conninfo` (walsender binds later in `run`).
///
/// `wal_rs::pg::backup::push::handle` sets `wal: false`, so object-store
/// tars don't carry WAL; it lives in `wal_005/` (wal-push / archive_command).
/// Direct mode inlines the same segments via `BaseBackupOpts { wal: true }`.
///
/// Missing segments surface as `WAL <name> not found in storage` from
/// wal-rs's `fetch::handle` — the operator's archiving pipeline left a gap.
async fn fetch_wal_into_pg_wal(
    settings: &wal_rs::config::Settings,
    storage: wal_rs::storage::DynStorage,
    shadow_data_dir: &Path,
    start_lsn: u64,
    end_lsn: u64,
    timeline: u32,
) -> Result<()> {
    use wal_rs::pg::wal::segment::SegmentName;

    let seg_size = WAL_SEG_SIZE;
    let pg_wal_dir = shadow_data_dir.join("pg_wal");
    tokio::fs::create_dir_all(&pg_wal_dir)
        .await
        .with_context(|| format!("create {}", pg_wal_dir.display()))?;
    let mut cur = SegmentName {
        timeline,
        log_id: (start_lsn >> 32) as u32,
        seg_no: ((start_lsn & 0xFFFF_FFFF) / seg_size) as u32,
    };
    let mut fetched: u32 = 0;
    loop {
        let name = cur.format();
        let dst = pg_wal_dir.join(&name);
        wal_rs::pg::wal::fetch::handle(settings, storage.clone(), &name, &dst)
            .await
            .with_context(|| format!("fetch WAL {name} -> {}", dst.display()))?;
        fetched += 1;
        let seg_end = cur.start_lsn(seg_size).saturating_add(seg_size);
        if end_lsn < seg_end {
            break;
        }
        cur = cur.next(seg_size);
    }
    tracing::info!(
        target: "walshadow::bootstrap",
        fetched,
        start_lsn = format!("{:X}/{:X}", start_lsn >> 32, start_lsn as u32),
        end_lsn = format!("{:X}/{:X}", end_lsn >> 32, end_lsn as u32),
        timeline,
        "hydrated shadow pg_wal from object store",
    );
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
