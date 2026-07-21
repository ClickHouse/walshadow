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

// The pipeline allocates rows on the decode thread(s) and frees them on the
// batcher thread; mimalloc's per-thread caches handle that produce-here/
// free-there pattern far better than glibc's shared arena (which serializes on
// its arena lock under that cross-thread churn).
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

use std::collections::HashSet;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use clap::{Parser, ValueEnum};
use std::fs;
use std::future::Future;
use std::pin::Pin;
use tokio::sync::{Mutex, watch};
use tokio_postgres::types::PgLsn;
use tokio_util::sync::CancellationToken;
use walrus::pg::backup::{BACKUP_NAME_PREFIX, format_pg_lsn};
use walrus::pg::replication::base_backup::BaseBackupOpts;
use walrus::pg::replication::conn::PgConfig;
use walrus::pg::replication::tls::{SslMode, TlsParams};
use walshadow::backfill_bootstrap::{
    BootstrapConfig, BootstrapOutcome, drain_backfill, seed_in_snapshot, spawn_greenfield_bootstrap,
};
use walshadow::backup_source::BackupSource;
use walshadow::backup_source_direct::DirectSource;
use walshadow::backup_source_object_store::ObjectStoreSource;
use walshadow::ch_emitter::{EmitterConfig, EmitterStats};
use walshadow::config::{CliOverrides, ConfigResolver, ResolvedConfig};
use walshadow::decoder_sink::MetricsTupleObserver;
use walshadow::manifest;
use walshadow::mapping::MappingHandle;
use walshadow::metrics::{MetricsRegistry, MetricsSnapshot, RateEstimator};
use walshadow::pg::{quote_ident, socket_conninfo};
use walshadow::pipeline::{Fatal, PipelineConfig, TailKind, bootstrap, tail};
use walshadow::queueing_record_sink::{
    DEFAULT_QUEUEING_BATCH_SIZE, DEFAULT_QUEUEING_RECORD_SINK_CAPACITY, QueueingRecordSink,
};
use walshadow::record::{MetricsRecordSink, Record, RecordSink, SinkError, WAL_SEG_SIZE};
use walshadow::retention::{
    DEFAULT_RETENTION_BYTES, DEFAULT_TRIM_INTERVAL, max_segment_end, trim_below_lsn,
};
use walshadow::runtime_config::InitialLoadMode;
use walshadow::schema::{RelName, SchemaEvent};
use walshadow::segment_sink::{DirSegmentSink, SegFsync};
use walshadow::shadow::{ResumeOutcome, Shadow, ShadowConfig};
use walshadow::shadow_catalog::{ShadowCatalog, ShadowCatalogConfig, with_transient_retry};
use walshadow::source_feed::{SourceFeed, StandbyStatus};
use walshadow::toast::ToastResolver;
use walshadow::wal_stream::WalStream;
use walshadow::xact_buffer::{BufferingDecoderSink, SubxactTracker, XactBuffer, XactBufferConfig};

/// Choose bootstrap source for empty shadow data dir
/// Initialized data dir resumes regardless of mode
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum, Default)]
enum BootstrapMode {
    /// Never bootstrap. Without `--bootstrap-shadow-data-dir`, manage shadow
    /// externally. With data dir, manage initialized cluster but reject
    /// empty dir
    #[default]
    Off,
    /// Source-PG-driven BASE_BACKUP over the replication protocol,
    /// reuses `--host` / `--port` / `--user`, no extra credentials
    Direct,
    /// wal-g-compatible BASE_BACKUP from a `DynStorage` bucket. Storage
    /// config read from `WALG_*` env vars (wal-rus CLI convention);
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
/// so its `wait_for_replay` waits don't park the pump task: each gate
/// would freeze wire delivery for a full shadow apply round-trip and
/// couple wire pacing to decode.
struct DaemonSinks {
    metrics: MetricsRecordSink,
    decoder_xact: QueueingRecordSink,
    /// Shared with the `BufferingDecoderSink` on the queueing worker;
    /// status loop polls without contending on the worker.
    decoder_stats: Arc<walshadow::decoder_sink::DecoderStats>,
    /// Shared with parallel pipeline's inserter pool (bumps counters
    /// post-`EndOfStream`). `None` when no CH pipeline is wired.
    emitter_stats: Option<Arc<walshadow::ch_emitter::EmitterStats>>,
    /// Per-txn span map; `Some` only with OTLP on. Registering at WAL read
    /// (here) makes the `txn` span cover the pump→worker channel wait.
    span_registry: Option<walshadow::trace::TxnSpanRegistry>,
}

impl RecordSink for DaemonSinks {
    fn on_record<'a>(
        &'a mut self,
        record: &'a Record<'a>,
    ) -> Pin<Box<dyn Future<Output = Result<(), SinkError>> + Send + 'a>> {
        Box::pin(async move {
            // Register at WAL read (pre-channel) so the span covers the queue wait.
            if let Some(reg) = &self.span_registry {
                reg.open(record.parsed.header.xact_id, record.source_lsn);
            }
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
    /// CLI override for the TOML's `[source] slot` (physical replication
    /// slot). Unset defers to config; unset in both = slotless.
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
    /// port, valid only for externally managed shadow (no
    /// `--bootstrap-shadow-data-dir`): operator reads
    /// `--walsender-port-file` and configures `primary_conninfo` by hand.
    /// Daemon-owned shadow bakes this address into shadow's generated
    /// `primary_conninfo` before shadow starts, so it rejects port 0 —
    /// pass an explicit port there.
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
    /// CLI override for the TOML's `[ch] drop_table_strategy` (`retain` /
    /// `drop` / `warn`). Highest-precedence layer: wins over TOML and
    /// survives SIGHUP reload, so an operator can pin the drop policy from
    /// the command line without editing TOML. Absent defers to TOML.
    #[arg(long)]
    drop_table_strategy: Option<String>,
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
    /// OTLP/gRPC endpoint for traces, e.g. `http://localhost:4317`. Absent
    /// disables tracing (zero overhead); falls back to
    /// `OTEL_EXPORTER_OTLP_ENDPOINT`. Spans emit at the `walshadow::trace`
    /// target.
    #[arg(long)]
    otlp_endpoint: Option<String>,
    /// Fraction of transactions to trace, `[0.0, 1.0]`. Head-sampled per txn
    /// (see `trace::should_sample`), so per-record span cost scales with it.
    #[arg(long, default_value_t = 0.01)]
    trace_sample_ratio: f64,
    /// WAL retention horizon in bytes. Segments older than
    /// `shadow_replay_lsn - retention_bytes` deleted every trim cycle.
    /// `0` disables trim.
    #[arg(long, default_value_t = DEFAULT_RETENTION_BYTES)]
    retention_bytes: u64,
    /// Skip pre-flight validators (server_version_num, wal_level, replica
    /// identity / row key, slot existence). For recovery drills.
    #[arg(long, default_value_t = false)]
    skip_preflight: bool,
    /// Ignore `manifest.toml` resume LSNs under `--spill-dir` at boot
    /// (greenfield resume even when a prior daemon left one), adopt a
    /// changed source timeline, and authorize boot past an unreadable or
    /// corrupt manifest (otherwise fatal). Source identity gate still
    /// applies; the manifest rewrites as the new daemon progresses. For
    /// "wipe + restart from a known LSN" drills.
    #[arg(long, default_value_t = false)]
    ignore_cursor: bool,
    /// Bootstrap source for empty shadow data dir. `off` never bootstraps;
    /// `direct` runs BASE_BACKUP over current replication connection;
    /// `object_store` reads wal-g-format backup from `DynStorage`
    /// (`WALG_*` env vars). Initialized data dir resumes without bootstrap
    /// regardless of mode
    #[arg(long, value_enum, default_value_t = BootstrapMode::Off)]
    bootstrap_mode: BootstrapMode,
    /// Shadow PG data dir. When set, daemon bootstraps or resumes shadow,
    /// writes config, starts and supervises postmaster, then stops it on
    /// exit. When unset, manage shadow externally. Required when
    /// `--bootstrap-mode != off`
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
    /// Maximum seconds to wait for shadow replay after bootstrap
    /// Abort daemon when timeout expires
    #[arg(long, default_value_t = 300)]
    bootstrap_shadow_replay_timeout: u64,
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<()> {
    let args = Args::parse();
    walshadow::trace::set_sample_ratio(args.trace_sample_ratio);
    // `--otlp-endpoint` wins; otherwise honor the conventional env var.
    let otlp_endpoint = args
        .otlp_endpoint
        .clone()
        .or_else(|| std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT").ok());
    let tracer_provider = init_tracing(otlp_endpoint.as_deref());
    let result = run(args).await;
    // The batch span processor lives on a background thread, so a bare
    // process exit drops whatever it hasn't flushed. Drain it before we
    // return (best-effort — a failed flush must not mask `run`'s result).
    if let Some(provider) = tracer_provider
        && let Err(e) = provider.shutdown()
    {
        tracing::warn!(target: "walshadow", error = %e, "otlp tracer shutdown");
    }
    result
}

/// OTLP/gRPC batch tracer provider for `endpoint`. Must run inside the tokio
/// runtime (tonic exporter + batch worker need it).
fn build_otlp_provider(
    endpoint: &str,
) -> anyhow::Result<opentelemetry_sdk::trace::SdkTracerProvider> {
    use opentelemetry_otlp::WithExportConfig;
    use opentelemetry_sdk::Resource;
    use opentelemetry_sdk::trace::{Sampler, SdkTracerProvider};
    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_tonic()
        .with_endpoint(endpoint)
        .build()?;
    // Head sampling happens at span creation (per txn, see TxnSpanRegistry),
    // so the SDK exports everything it's handed.
    Ok(SdkTracerProvider::builder()
        .with_sampler(Sampler::AlwaysOn)
        .with_batch_exporter(exporter)
        .with_resource(Resource::builder().with_service_name("walshadow").build())
        .build())
}

/// Wire `tracing` once per process (`RUST_LOG` filter, default
/// `warn,walshadow=info`). With `otlp_endpoint` set, stacks an OTel layer on
/// the stderr `fmt` layer; the returned provider must be `.shutdown()` at exit.
fn init_tracing(
    otlp_endpoint: Option<&str>,
) -> Option<opentelemetry_sdk::trace::SdkTracerProvider> {
    use opentelemetry::trace::TracerProvider as _;
    use tracing_subscriber::EnvFilter;
    use tracing_subscriber::prelude::*;

    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_target(true)
        .with_writer(std::io::stderr);

    // Best-effort: a bad endpoint logs and degrades to no-traces rather
    // than refusing to boot — observability never blocks the pipeline.
    let provider = if let Some(endpoint) = otlp_endpoint {
        match build_otlp_provider(endpoint) {
            Ok(p) => {
                opentelemetry::global::set_tracer_provider(p.clone());
                Some(p)
            }
            Err(e) => {
                eprintln!("walshadow: OTLP exporter init failed for {endpoint}: {e:#}");
                None
            }
        }
    } else {
        None
    };

    // `walshadow::trace` spans only feed the OTLP exporter; with none attached
    // they are pure per-record overhead, so disable that target — unless the
    // user explicitly set it in RUST_LOG.
    let user_set_trace = std::env::var("RUST_LOG")
        .map(|v| v.contains("walshadow::trace"))
        .unwrap_or(false);
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("warn,walshadow=info"));
    let filter = if provider.is_some() || user_set_trace {
        filter
    } else {
        filter.add_directive(
            "walshadow::trace=off"
                .parse()
                .expect("static trace-off directive parses"),
        )
    };
    let otel_layer = provider
        .as_ref()
        .map(|p| tracing_opentelemetry::layer().with_tracer(p.tracer("walshadow")));

    let _ = tracing_subscriber::registry()
        .with(filter)
        .with(fmt_layer)
        .with(otel_layer)
        .try_init();
    provider
}

async fn run(args: Args) -> Result<()> {
    let sighup = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::hangup())
        .inspect_err(|e| {
            tracing::warn!(
                target: "walshadow::sighup",
                error = %e,
                "SIGHUP install failed; reload disabled",
            );
        })?;
    // Match systemd SIGTERM with ctrl_c shutdown path
    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .inspect_err(|e| {
            tracing::warn!(
                target: "walshadow",
                error = %e,
                "SIGTERM install failed",
            );
        })?;
    let sslmode = SslMode::parse(&args.sslmode).context("--sslmode")?;
    let cfg = PgConfig {
        host: args.host.clone(),
        port: args.port,
        user: args.user.clone(),
        password: args.password.clone(),
        database: args.dbname.clone(),
        application_name: "walshadow".into(),
        sslmode,
        // Cert/key material rides PGSSL* env, matching libpq (--sslmode doc)
        tls: TlsParams::resolve(&walrus::config::Vars::default()),
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
        xlogpos = format_pg_lsn(ident.xlogpos).to_string(),
        "source identified",
    );

    let ch_config = if let Some(path) = args.ch_config.as_deref() {
        let toml = tokio::fs::read_to_string(path)
            .await
            .with_context(|| format!("read --ch-config {}", path.display()))?;
        let mut cfg = EmitterConfig::from_toml_str(&toml).context("parse --ch-config")?;
        if let Some(ms) = args.ch_flush_timeout_ms {
            cfg.flush_timeout = std::time::Duration::from_millis(ms);
        }
        // CLI override wins over TOML `[source] slot` (CLI > config).
        if args.slot.is_some() {
            cfg.source_slot = args.slot.clone();
        }
        Some(cfg)
    } else {
        None
    };
    // Effective physical replication slot (`[source] slot` + --slot override);
    // None = slotless.
    let source_slot: Option<String> = ch_config.as_ref().and_then(|c| c.source_slot.clone());
    let shadow_start = resolve_shadow_start(&args)?;
    let bootstrap_end_lsn: Option<u64> = if matches!(shadow_start, ShadowStart::Bootstrap(_)) {
        Some(
            run_bootstrap(&cfg, &mut feed, &args, ch_config.clone())
                .await
                .context("bootstrap")?,
        )
    } else {
        None
    };
    // Regenerate config because walsender address and port may change
    // Read minimum GUC values from shadow pg_control
    // Keep shadow alive until pipeline teardown finishes
    let shadow_lifecycle: Option<ShadowLifecycle> = match &shadow_start {
        ShadowStart::External => None,
        ShadowStart::Bootstrap(dir) | ShadowStart::Resume(dir) => {
            let shadow = Arc::new(build_owned_shadow(&args, dir.clone()));
            let conninfo = walsender_primary_conninfo(args.walsender_bind);
            shadow
                .write_standby_signal()
                .context("write standby.signal")?;
            start_owned_shadow(
                &shadow,
                conninfo.clone(),
                bootstrap_end_lsn,
                Duration::from_secs(args.bootstrap_shadow_replay_timeout),
            )
            .await?;
            Some(ShadowLifecycle::spawn(shadow, conninfo))
        }
    };

    let live_identity = manifest::SourceIdentity {
        system_id: ident.sysid.parse().context("IDENTIFY_SYSTEM sysid")?,
        timeline: ident.timeline,
    };
    let start_lsn_override = args
        .start_lsn
        .as_deref()
        .map(|s| walshadow::pg::parse_pg_lsn(s).context("--start-lsn"))
        .transpose()?;
    // Identity gate runs before `--ignore-cursor`: the flag discards resume
    // LSNs, not artifact ownership. Foreign system_id is fatal regardless
    // (retire/backfill ledgers would act on another cluster's state); a
    // timeline-only change (promoted source) passes under `--ignore-cursor`,
    // live identity persists at the next manifest write.
    let manifest_at_boot: Option<manifest::Manifest> =
        match manifest::load(&args.spill_dir, &live_identity).await {
            Ok(m) => m,
            Err(manifest::ManifestError::ForeignSource { stored, live })
                if args.ignore_cursor && stored.system_id == live.system_id =>
            {
                tracing::warn!(
                    target: "walshadow::manifest",
                    stored_timeline = stored.timeline,
                    live_timeline = live.timeline,
                    "--ignore-cursor adopts new source timeline",
                );
                None
            }
            Err(e @ manifest::ManifestError::ForeignSource { .. }) => {
                anyhow::bail!("{e}");
            }
            Err(e) if args.ignore_cursor || start_lsn_override.is_some() => {
                tracing::warn!(
                    target: "walshadow::manifest",
                    error = %e,
                    spill_dir = %args.spill_dir.display(),
                    "manifest unreadable; operator override discards it",
                );
                None
            }
            Err(e) => {
                anyhow::bail!(
                    "manifest at {} unreadable: {e}; restore it, or authorize \
                     recovery with --ignore-cursor / --start-lsn",
                    manifest::manifest_path(&args.spill_dir).display(),
                );
            }
        };
    // Resume precedence: `--start-lsn` > bootstrap end > manifest emitter-ack
    // > greenfield (source write head). `--ignore-cursor` forces greenfield
    // (recovery drills). Bootstrap `end_lsn` outranks the manifest: shadow
    // catalog state is at `end_lsn`, so consuming WAL before it double-counts.
    let manifest_at_boot = if args.ignore_cursor {
        None
    } else {
        manifest_at_boot
    };
    let raw_start = manifest::resolve_resume_lsn(
        start_lsn_override,
        bootstrap_end_lsn,
        manifest_at_boot.as_ref().map(|m| m.lsn.emitter_ack.0),
        ident.xlogpos,
    );
    let pinned = bootstrap_end_lsn.is_some() || start_lsn_override.is_some();
    let floor_at_boot = manifest_at_boot
        .as_ref()
        .map(|m| m.floor.0)
        .filter(|f| *f != 0);
    // Archive-end scan only feeds the greenfield clamp (keep archive
    // continuous until live streaming begins: starting after last sealed
    // segment leaves shadow missing WAL; re-read from earlier LSN, CH
    // removes duplicates using `_lsn`). A persisted floor folded the clamp
    // at write time.
    let archive_end = if !pinned && floor_at_boot.is_none() {
        max_segment_end(&args.out_dir)
            .await
            .context("scan out-dir for sealed archive end")?
    } else {
        None
    };
    let aligned = manifest::resolve_start(raw_start, floor_at_boot, pinned, archive_end);
    tracing::info!(
        target: "walshadow",
        raw = format_pg_lsn(raw_start).to_string(),
        aligned = format_pg_lsn(aligned).to_string(),
        from_bootstrap = bootstrap_end_lsn.is_some() && args.start_lsn.is_none(),
        from_floor = floor_at_boot.is_some() && !pinned,
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
            .tracker_mut()
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

    // Descriptor-cache invalidation epoch: decoder worker bumps off each
    // record's tracker verdict (`with_catalog_signals` below), catalog
    // folds the delta into `invalidate` before each relation lookup's
    // cache check. Mapping writes + SIGHUP reload bump out-of-band.
    let invalidation_epoch = Arc::new(AtomicU64::new(0));
    // DROP-sweep arming, keyed by xid: decoder worker arms at pg_class
    // heap_delete records (never ADD COLUMN / CREATE INDEX noise), commit
    // sink consumes only at the arming xact's own commit so the replay
    // gate makes the drop visible before sweep_dropped probes shadow.
    let pending_sweeps = walshadow::catalog_tracker::PendingSweeps::new();
    catalog
        .lock()
        .await
        .set_invalidation_epoch(invalidation_epoch.clone());

    // Create the configured slot before preflight, which requires it to exist.
    if let Some(slot) = source_slot.as_deref() {
        feed.ensure_physical_slot(slot)
            .await
            .with_context(|| format!("ensure physical replication slot {slot}"))?;
        tracing::info!(target: "walshadow", slot, "physical replication slot ready");
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
            slot: source_slot.as_deref(),
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
        let names: Vec<RelName> = cfg.tables.keys().cloned().collect();
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

    feed.start_physical_replication(source_slot.as_deref(), aligned, ident.timeline)
        .await
        .context("START_REPLICATION")?;
    // Spill dir wiped every startup: cursor file commits drains
    // atomically, so leftover spill from a prior crash is redundant or stale.
    let xact_buf_cfg = XactBufferConfig {
        xact_buffer_max: args.xact_buffer_max,
        ..XactBufferConfig::new(args.spill_dir.clone())
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
    // Txn-span registry, shared by pump + decoder; `Some` only with OTLP on.
    let span_registry =
        if args.otlp_endpoint.is_some() || std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT").is_ok() {
            Some(xact_buffer.lock().await.span_registry())
        } else {
            None
        };
    let mut decoder = BufferingDecoderSink::new(catalog.clone(), xact_buffer.clone())
        .with_schema_events(schema_events.clone())
        // Bump / arm at worker position off each record's tracker verdict:
        // the queueing sink below decouples the decoder from the pump, so
        // a pump-position bump would be consumable before pre-DDL records
        // finish decoding
        .with_catalog_signals(invalidation_epoch.clone(), Some(pending_sweeps.clone()));
    if let Some(schema) = ch_config
        .as_ref()
        .and_then(|c| c.runtime_config_schema.as_deref())
    {
        decoder = decoder.with_config_schema(Arc::from(schema));
    }
    if let Some(reg) = &span_registry {
        decoder = decoder.with_span_registry(reg.clone());
    }
    let decoder_stats_handle = decoder.stats_handle();

    let mut emitter_stats_handle: Option<Arc<EmitterStats>> = None;
    // Durable watermark (ack collector atomic). Seed at `raw_start`, not 0:
    // status loop persists this atomic into the manifest's `emitter_ack`
    // each interval with no monotonic guard, first write at boot before any
    // WAL re-read acks. Seeding 0 would clobber a resumed manifest; a crash
    // before [aligned, raw_start] re-reads acks then resumes from
    // ident.xlogpos next boot (precedence skips a zero ack), silently
    // dropping [raw_start, head] WAL that never reached CH. tail's
    // `fetch_max` keeps it monotonic as WAL re-reads [aligned, raw_start].
    // See plans/future/pipeline_backpressure_and_scaling.md (Handoff step 3).
    let emitter_ack = Arc::new(AtomicU64::new(raw_start));
    // Persisted resolved floor. Seed with the resolved start: aligned +
    // archive-clamped, the exact position a crash-now restart replays from.
    // Any Dropped queued during the boot re-read of [aligned, raw_start] has
    // commit_lsn ≥ aligned, so its retire holds until a later manifest write
    // moves the floor past it.
    let resume_floor = Arc::new(AtomicU64::new(aligned));
    // Deferred retires queued before a stop; entries below `aligned` never
    // replay their drop, so the post-spawn flush below is their only route
    // to the wipe. Loaded in metrics-only runs too (inert without a chunk
    // store), preserved for a later CH run over the same spill dir.
    let retires = walshadow::toast_retire::RetireLedger::load(&args.spill_dir)
        .await
        .context("load toast retire ledger")?;
    // Layered config resolver (CLI > TOML); `Some` only with `--ch-config`.
    // Moved into the SIGHUP task, which re-reads TOML and republishes.
    let mut config_resolver: Option<Arc<ConfigResolver>> = None;
    // COPY backfiller for `initial_load='copy'`; `Some` with SQL opt-ins or
    // TOML-pinned initial loads.
    let mut copy_backfiller: Option<Arc<walshadow::copy_backfill::CopyBackfiller>> = None;

    let pcfg = if let Some(mut emitter_cfg) = ch_config {
        let addr = format!("{}:{}", emitter_cfg.host, emitter_cfg.port);
        // Live routing map shared by DDL applicator + decode pool. The
        // refresher below rewrites it on every republished snapshot.
        let mapping: MappingHandle = Arc::new(tokio::sync::RwLock::new(emitter_cfg.tables.clone()));
        // Resolver merges CLI over TOML and publishes ResolvedConfig on
        // the watch substrate; SIGHUP re-reads TOML and republishes. The
        // mapping refresher + DDL applicator subscribe.
        let cli_overrides = CliOverrides {
            drop_table_strategy: args.drop_table_strategy.clone(),
            flush_timeout: args
                .ch_flush_timeout_ms
                .map(std::time::Duration::from_millis),
        };
        let (resolver, config_rx) = ConfigResolver::new(
            &emitter_cfg,
            cli_overrides,
            args.ch_config.clone(),
            mapping.clone(),
            invalidation_epoch.clone(),
        );
        spawn_mapping_refresher(config_rx.clone(), mapping.clone());
        // Runtime-config overlay (§7): before the pump consumes WAL, seed the
        // resolver from source PG's config_* tables via the sidecar libpq
        // connection. Post-seed writes arrive live off the WAL stream. Refuse
        // to start if the named schema is not installed — explicit opt-in
        // means the operator expects the overlay present.
        let mut seeded_table_rows: Vec<(RelName, walshadow::runtime_config::TableRow)> = Vec::new();
        if let Some(schema) = emitter_cfg.runtime_config_schema.clone() {
            let client = feed
                .sql_client()
                .await
                .context("sidecar sql for runtime-config seed")?;
            seeded_table_rows = seed_runtime_config(client, &schema, &resolver)
                .await
                .context("seed runtime config overlay")?;
        }
        // Fold the resolved emitter knobs back onto the boot config so the
        // pipeline's initial batcher/inserter match the seeded + CLI values;
        // they track the watch channel live thereafter.
        {
            let rc = config_rx.borrow();
            emitter_cfg.row_budget = rc.row_budget;
            emitter_cfg.byte_budget = rc.byte_budget;
            emitter_cfg.flush_timeout = rc.flush_timeout;
            emitter_cfg.compression = rc.compression;
            emitter_cfg.retry.max_attempts = rc.retry_max_attempts;
        }
        // DDL applicator owned by the reorder coordinator so ALTER /
        // CREATE / DROP / TRUNCATE apply inside the barrier, after
        // earlier data is durable. Seeds DDL config from the resolved
        // snapshot; refreshes per apply as the resolver republishes.
        let ddl_cfg = walshadow::ch_ddl::DdlConfig::from_resolved(
            &config_rx.borrow(),
            emitter_cfg.database.clone(),
            emitter_cfg.soft_delete,
        );
        let mut applicator = walshadow::ch_ddl::DdlApplicator::new(
            &emitter_cfg,
            ddl_cfg,
            mapping.clone(),
            config_rx.clone(),
        )
        .await
        .context("init DDL applicator")?
        .with_invalidation_epoch(invalidation_epoch.clone())
        .with_resolver(resolver.clone());
        let stats = Arc::new(EmitterStats::default());
        emitter_stats_handle = Some(stats.clone());
        // Backfiller for `initial_load` opt-ins (COPY / backup-sourced):
        // own source session + CH tail per backfill or pass, spill-dir
        // ledger dedups restarts.
        let toml_initial_load = emitter_cfg
            .table_initial_loads
            .values()
            .any(|mode| InitialLoadMode::parse(mode).is_some_and(|m| m != InitialLoadMode::None));
        // One validated resident-payload pool for the pipeline and every
        // concurrent backup pass
        let pipeline_budget =
            walshadow::pipeline::build_budget(&emitter_cfg, args.decoder_pool_size)
                .map_err(|e| anyhow::anyhow!("memory budget: {e}"))?;
        if emitter_cfg.runtime_config_schema.is_some() || toml_initial_load {
            copy_backfiller = Some(Arc::new(
                walshadow::copy_backfill::CopyBackfiller::new(
                    cfg.clone(),
                    emitter_cfg.clone(),
                    mapping.clone(),
                    stats.clone(),
                    catalog.clone(),
                    &args.spill_dir,
                    Some(config_rx.clone()),
                    Some(pipeline_budget.clone()),
                )
                .await,
            ));
        }
        let backfiller_effects: Option<Arc<dyn walshadow::opt_in::Backfiller>> =
            copy_backfiller.clone().map(|backfiller| backfiller as _);
        // Re-materialise per-table opt-in scope from the seeded config_table
        // rows. Live edits arrive off WAL via the reorder coordinator, but a
        // restart replays WAL from past these rows' commit LSN, so the seed
        // is the only chance to rebuild their scope (the CH tables persist).
        // `raw_start` is the backfill boundary S for a first-seen
        // `initial_load` row: COPY covers commits before it, WAL the rest;
        // the ledger resumes/no-ops rows seen on an earlier boot.
        for (rel, row) in &seeded_table_rows {
            if row.replicate.is_some() {
                walshadow::opt_in::apply_table_opt_in(
                    &resolver,
                    &mut applicator,
                    &catalog,
                    backfiller_effects.as_ref(),
                    rel,
                    row,
                    raw_start,
                )
                .await
                .with_context(|| format!("seed opt-in for {rel}"))?;
            }
        }
        let sql_scoped_tables: HashSet<RelName> = seeded_table_rows
            .iter()
            .filter(|(_, row)| row.replicate.is_some())
            .map(|(rel, _)| rel.clone())
            .collect();
        let active_tables: HashSet<RelName> = config_rx.borrow().tables.keys().cloned().collect();
        apply_toml_initial_loads(
            &catalog,
            copy_backfiller.as_ref(),
            &emitter_cfg.table_initial_loads,
            &active_tables,
            &sql_scoped_tables,
            raw_start,
        )
        .await?;
        // Baseline seeding suppresses the Added event for pinned mappings, so a
        // plain TOML mapping (no initial_load, no opt-in) would tail into a
        // missing CH table. Ensure those dests here; the others own their copy.
        for rel in &active_tables {
            if sql_scoped_tables.contains(rel) {
                continue;
            }
            let has_initial_load = emitter_cfg
                .table_initial_loads
                .get(rel)
                .and_then(|mode| InitialLoadMode::parse(mode))
                .is_some_and(|m| m != InitialLoadMode::None);
            if has_initial_load {
                continue;
            }
            let Some(desc) = catalog
                .lock()
                .await
                .descriptor_by_name(rel)
                .await
                .with_context(|| format!("resolve descriptor for pinned mapping {rel}"))?
            else {
                continue;
            };
            applicator
                .apply(&SchemaEvent::Added { desc })
                .await
                .with_context(|| format!("ensure CH dest for pinned mapping {rel}"))?;
        }
        config_resolver = Some(resolver);
        tracing::info!(
            target: "walshadow::pipeline",
            addr = %addr,
            decoders = args.decoder_pool_size.max(1),
            inserters = args.inserter_pool_size.max(1),
            "parallel decode+insert pipeline starting",
        );
        PipelineConfig {
            emitter: emitter_cfg,
            decoder_pool_size: args.decoder_pool_size,
            inserter_pool_size: args.inserter_pool_size,
            catalog: catalog.clone(),
            mapping,
            oracle: oracle.clone(),
            applicator: Some(applicator),
            tail: TailKind::ClickHouse,
            buffer: xact_buffer.clone(),
            subxact_tracker: Arc::new(Mutex::new(SubxactTracker::new())),
            schema_events: Some(schema_events.clone()),
            pending_sweeps: Some(pending_sweeps.clone()),
            stats: stats.clone(),
            span_registry: span_registry.clone(),
            config_resolver: config_resolver.clone(),
            backfiller: backfiller_effects,
            retires,
            resume_floor: resume_floor.clone(),
            budget: Some(pipeline_budget),
        }
    } else {
        // Metrics-only (no CH): the identical pipeline with a null tail —
        // zero CH connections, no DDL applicator, no oracle (nothing ships,
        // PgPending stays raw). The empty mapping routes nothing, so seqs
        // complete at placement and the watermark + slot advance move as in
        // a CH run. Emitter stats stay unexported (`emitter_stats_handle`
        // None), matching the old serial surface.
        tracing::info!(
            target: "walshadow::pipeline",
            decoders = args.decoder_pool_size.max(1),
            "metrics-only pipeline (null tail) starting",
        );
        PipelineConfig {
            emitter: EmitterConfig::default(),
            decoder_pool_size: args.decoder_pool_size,
            inserter_pool_size: args.inserter_pool_size,
            catalog: catalog.clone(),
            mapping: Arc::new(tokio::sync::RwLock::new(Default::default())),
            oracle: None,
            applicator: None,
            tail: TailKind::Null,
            buffer: xact_buffer.clone(),
            subxact_tracker: Arc::new(Mutex::new(SubxactTracker::new())),
            schema_events: Some(schema_events.clone()),
            pending_sweeps: Some(pending_sweeps.clone()),
            stats: Arc::new(EmitterStats::default()),
            span_registry: span_registry.clone(),
            config_resolver: None,
            backfiller: None,
            retires,
            resume_floor: resume_floor.clone(),
            budget: None,
        }
    };
    let (mut reorder_sink, pipeline_handle) = pcfg
        .spawn(emitter_ack.clone())
        .await
        .context("spawn decode+insert pipeline")?;
    reorder_sink
        .flush_due_retires()
        .await
        .context("boot flush of due toast-mirror retires")?;
    let decoder_xact = QueueingRecordSink::spawn(
        DecoderXactPair {
            decoder,
            xact_drain: reorder_sink,
        },
        args.decoder_batch_size,
        args.decoder_queue_capacity,
        span_registry.clone(),
    );
    let mut record_sink = DaemonSinks {
        metrics: MetricsRecordSink::default(),
        decoder_xact,
        decoder_stats: decoder_stats_handle,
        emitter_stats: emitter_stats_handle,
        span_registry,
    };
    // Segment fsync off the hot path: sink writes+renames, the task fsyncs and
    // publishes `durable_lsn`. Seed at the resume point.
    let durable_lsn = Arc::new(AtomicU64::new(stream.dispatched_lsn()));
    let fsync_fatal = walshadow::pipeline::Fatal::new();
    let (fsync_tx, fsync_rx) = tokio::sync::mpsc::channel::<SegFsync>(SEGMENT_FSYNC_QUEUE);
    let fsync_task = spawn_segment_fsync(
        args.out_dir.clone(),
        fsync_rx,
        durable_lsn.clone(),
        fsync_fatal.clone(),
    );
    let mut segment_sink =
        DirSegmentSink::with_durability(args.out_dir.clone(), WAL_SEG_SIZE, fsync_tx)
            .context("open out-dir")?;
    let mut chunk_buf = Vec::with_capacity(64 * 1024);

    let metrics = MetricsRegistry::new();
    let _metrics_server = if let Some(addr) = args.metrics_bind {
        let (bound, _handle) = walshadow::metrics::serve(addr, metrics.clone())
            .await
            .context("bind metrics endpoint")?;
        tracing::info!(target: "walshadow::metrics", addr = %bound, "metrics endpoint serving");
        Some(_handle)
    } else {
        None
    };

    // Kept for the status loop's config metrics (opt-in / pending-decl gauges);
    // the sighup handler takes ownership of `config_resolver` below.
    let metrics_resolver = config_resolver.clone();
    let metrics_backfiller = copy_backfiller.clone();

    // SIGHUP re-reads TOML and republishes the resolved snapshot; the
    // mapping refresher + DDL applicator pick it up. Connection params stay
    // boot-only. No resolver (metrics-only) makes SIGHUP a no-op tap.
    let _sighup_task = spawn_sighup_handler(sighup, config_resolver);

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
    // Manifest write cadence. Slot safety doesn't ride on it: advertised
    // flush_lsn is capped at the persisted floor below, so a lagging write
    // only delays slot advance, never overshoots it.
    let cursor_write_interval = Duration::from_secs(args.status_interval);
    let mut last_cursor_write: Option<Instant> = None;
    // Fast metrics-refresh tick (decoupled from cursor/status): an idle source
    // would otherwise freeze the /metrics snapshot while the pipeline drains.
    let metrics_tick = Duration::from_millis(250);
    // Inflight-stall watchdog: xacts_active > 0 with stalled
    // `emitter_ack_lsn` dumps the parked xids holding the slot. One-shot
    // per stall, re-arms when ack advances.
    let mut last_emitter_ack_observed: u64 = 0;
    let mut inflight_stall_since: Option<Instant> = None;
    let mut inflight_stall_logged = false;
    let shutdown_reason = loop {
        // `durable` (fsynced) lags `dispatched`; advertise it as flush/cursor.
        let dispatched = stream.dispatched_lsn();
        let durable = durable_lsn.load(Ordering::Acquire);
        let received = feed.last_server_wal_end().max(dispatched);
        let shadow_replay = shadow_replay_lsn.load(Ordering::Acquire);
        let shadow_agg = shadow_state.lock().await.aggregate();
        if let Some(flush) = shadow_agg.min_flush_lsn {
            shadow_flush_lsn.fetch_max(flush, Ordering::Release);
        }
        let (drain_lsn, emitter_ack_lsn) = {
            let mut b = xact_buffer.lock().await;
            let ea = emitter_ack.load(Ordering::Acquire);
            let drain_lsn = b.stats().drain_lsn;
            // Keep every undurable transaction reachable after restart
            // Read acknowledgment first so no transaction escapes floor
            (drain_lsn, b.resume_safe_lsn(ea))
        };
        // shadow_replay==0 (sweeper off or not yet reported) means "no
        // constraint from shadow", not the literal min: else a fresh boot
        // with retention off pins apply_lsn at 0 and source's slot never recycles.
        let apply_ceiling = match shadow_replay {
            0 => emitter_ack_lsn,
            s => s.min(emitter_ack_lsn),
        };
        let cur = manifest::Manifest {
            version: manifest::MANIFEST_VERSION,
            floor: manifest::Lsn(manifest::resolved_floor(emitter_ack_lsn, durable)),
            source: live_identity.clone(),
            lsn: manifest::LsnSet {
                source_received: manifest::Lsn(received),
                filter_durable: manifest::Lsn(durable),
                shadow_replay: manifest::Lsn(shadow_replay),
                drain: manifest::Lsn(drain_lsn),
                emitter_ack: manifest::Lsn(emitter_ack_lsn),
                shadow_flush: manifest::Lsn(shadow_flush_lsn.load(Ordering::Acquire)),
            },
        };
        if last_cursor_write.is_none_or(|t| t.elapsed() >= cursor_write_interval) {
            manifest::write(&args.spill_dir, &cur)
                .await
                .context("write resume manifest")?;
            last_cursor_write = Some(Instant::now());
            // Publish only after persist: pruners cut against what a
            // crash-now restart actually resumes from.
            resume_floor.store(cur.floor.0, Ordering::Release);
        }
        // flush caps physical slot's restart_lsn.
        // Manifest writes are cadence-gated above while keepalive replies inside
        // next_chunk can send this status at any time.
        let status = StandbyStatus {
            write_lsn: received,
            flush_lsn: apply_ceiling.min(resume_floor.load(Ordering::Acquire)),
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
            _ = sigterm.recv() => break "signal",
            // Idle tick so metrics/cursor keep tracking the draining pipeline
            // when no new WAL arrives.
            _ = tokio::time::sleep(metrics_tick) => None,
            res = feed.next_chunk(status, &mut chunk_buf) => match res {
                Ok(Some(c)) => Some(c),
                Ok(None) => break "CopyDone",
                Err(e) => {
                    let resume = stream.next_lsn();
                    tracing::warn!(
                        target: "walshadow",
                        error = %e,
                        resume_lsn = format_pg_lsn(resume).to_string(),
                        "source stream error — recovering",
                    );
                    feed = reconnect_or_fatal(
                        e,
                        &cfg,
                        source_slot.as_deref(),
                        resume,
                        ident.timeline,
                        Duration::from_secs(args.status_interval),
                    )
                    .await?;
                    tracing::info!(
                        target: "walshadow",
                        resume_lsn = format_pg_lsn(resume).to_string(),
                        "source reconnected — resuming replication",
                    );
                    None
                }
            },
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
        if let Some(msg) = pipeline_handle.fatal.message() {
            anyhow::bail!("decode+insert pipeline failed: {msg}");
        }
        if let Some(msg) = fsync_fatal.message() {
            anyhow::bail!("segment fsync failed: {msg}");
        }
        let now_dispatched = stream.dispatched_lsn();
        let advanced = now_dispatched != prev_dispatched;
        let (xact_stats, drain_resident, xact_line) = {
            let b = xact_buffer.lock().await;
            let stats = b.stats().clone();
            let line = stats.summary();
            let resident = DrainResident {
                total: b.drain_resident_bytes(),
                chunks: b.drain_chunk_resident_bytes(),
                rows: b.drain_row_resident_bytes(),
                spool: b.toast_spool_bytes(),
            };
            (stats, resident, line)
        };
        let oracle_line = oracle
            .as_ref()
            .map(|o| o.stats.summary())
            .unwrap_or_default();
        let oracle_stats = oracle.as_ref().map(|o| o.stats.as_ref());
        let decoder_stats: &walshadow::decoder_sink::DecoderStats = &record_sink.decoder_stats;
        let emitter_stats: Option<&walshadow::ch_emitter::EmitterStats> =
            record_sink.emitter_stats.as_deref();
        let shadow_apply_lsn = shadow_agg.min_apply_lsn.unwrap_or(0);
        let lag_bytes = received.saturating_sub(shadow_apply_lsn);
        rate_estimator.observe(Instant::now(), received);
        let lag_seconds = rate_estimator.seconds_for(lag_bytes);
        // Post-worker snapshots so the metric reflects what the worker
        // drained, not the top-of-iteration values.
        let emitter_ack_for_metric = emitter_ack.load(Ordering::Acquire);
        let drain_for_metric = xact_stats.drain_lsn;
        populate_metrics(
            &metrics,
            received,
            now_dispatched,
            shadow_replay,
            drain_for_metric,
            emitter_ack_for_metric,
            &record_sink.metrics,
            record_sink.decoder_xact.in_flight(),
            record_sink.decoder_xact.processed(),
            &xact_stats,
            drain_resident,
            Some(&pipeline_handle.budget),
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
            metrics_resolver.as_deref(),
            metrics_backfiller.as_deref(),
        )
        .await;
        if advanced {
            let new_segs = (now_dispatched - prev_dispatched) / WAL_SEG_SIZE;
            segments_shipped += new_segs;
            prev_dispatched = now_dispatched;
            let ahead = server_end.saturating_sub(dispatched_before);
            let filter = stream.filter();
            let filter_stats = filter.stats();
            let tracker_stats = filter.tracker().stats();
            tracing::info!(
                target: "walshadow",
                segments_shipped,
                last_lsn = format_pg_lsn(now_dispatched).to_string(),
                shadow_apply = format_pg_lsn(shadow_apply_lsn).to_string(),
                source_ahead_bytes = ahead,
                metrics = %record_sink.metrics.summary(),
                kept = filter_stats.kept,
                dropped = filter_stats.dropped,
                relmap_updates = tracker_stats.relmap_updates,
                pg_class_undecoded = tracker_stats.pg_class_writes_undecoded,
                pg_class_oid_in_prefix = tracker_stats.pg_class_writes_oid_in_prefix,
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
        if emitter_ack_for_metric != last_emitter_ack_observed {
            last_emitter_ack_observed = emitter_ack_for_metric;
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
                            "xid={} lsn={}..{} heap={} chunk={} bytes={} spill={} cat={} rels=[{}]",
                            e.xid,
                            format_pg_lsn(e.first_lsn),
                            format_pg_lsn(e.last_lsn),
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
                    emitter_ack_lsn = format_pg_lsn(emitter_ack_for_metric).to_string(),
                    drain_lsn = format_pg_lsn(xact_stats.drain_lsn).to_string(),
                    source_received = format_pg_lsn(received).to_string(),
                    filter_dispatched = format_pg_lsn(now_dispatched).to_string(),
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
    // Drop the sink (closes the fsync queue) and drain the fsync task so the
    // final partial is durable.
    drop(segment_sink);
    fsync_task.await.ok();
    if let Some(msg) = fsync_fatal.message() {
        anyhow::bail!("segment fsync failed: {msg}");
    }
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
    pipeline_handle
        .join()
        .await
        .map_err(|m| anyhow::anyhow!("decode+insert pipeline drain failed: {m}"))?;
    if let Some(lifecycle) = shadow_lifecycle {
        lifecycle.shutdown().await;
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

/// Seed the resolver overlay from source PG's `<schema>.config_*` tables via
/// the sidecar libpq connection (plan §7). Refuses (Err → daemon exits) when
/// the schema is named but not installed, or the install is newer than this
/// daemon understands — explicit opt-in should not silently no-op.
async fn seed_runtime_config(
    client: &tokio_postgres::Client,
    schema: &str,
    resolver: &ConfigResolver,
) -> anyhow::Result<Vec<(RelName, walshadow::runtime_config::TableRow)>> {
    use walshadow::runtime_config::{ColumnRow, ConfigOverlay, GlobalRow, NamespaceRow, TableRow};
    let s = quote_ident(schema);
    let mut overlay = ConfigOverlay::default();

    // The config_global read doubles as the install probe: a missing table
    // errors here, so a schema named but not installed refuses to start rather
    // than silently no-op (explicit opt-in). config_global is the singleton, so
    // 0 rows (greenfield) is fine — all TOML defaults then apply.
    if let Some(row) = client
        .query_opt(
            &format!(
                "SELECT row_budget, byte_budget, flush_timeout_ms, compression, \
                 retry_max_attempts, drop_table_strategy FROM {s}.config_global WHERE id = 1"
            ),
            &[],
        )
        .await
        .with_context(|| {
            format!(
                "runtime_config schema {schema:?} not installed (config_global unreadable); \
                 set [runtime_config] schema = \"\" to disable the overlay"
            )
        })?
    {
        overlay.global = Some(GlobalRow {
            row_budget: row.get("row_budget"),
            byte_budget: row.get("byte_budget"),
            flush_timeout_ms: row.get("flush_timeout_ms"),
            compression: row.get("compression"),
            retry_max_attempts: row
                .get::<_, Option<i32>>("retry_max_attempts")
                .map(i64::from),
            drop_table_strategy: row.get("drop_table_strategy"),
        });
    }

    for row in client
        .query(
            &format!(
                "SELECT namespace, target_database, auto_create, drop_table_strategy \
                 FROM {s}.config_namespace"
            ),
            &[],
        )
        .await
        .context("read config_namespace")?
    {
        let namespace: String = row.get("namespace");
        overlay.namespaces.insert(
            namespace,
            NamespaceRow {
                target_database: row.get("target_database"),
                auto_create: row.get("auto_create"),
                drop_table_strategy: row.get("drop_table_strategy"),
            },
        );
    }

    // `SELECT *` + `try_get` for the post-v1 columns so a newer daemon reads an
    // older install (missing `replicate`/`initial_load`) without a hard error —
    // the additive-schema promise. Re-running the install adds the columns.
    for row in client
        .query(&format!("SELECT * FROM {s}.config_table"), &[])
        .await
        .context("read config_table")?
    {
        let namespace: String = row.get("namespace");
        let relname: String = row.get("relname");
        overlay.tables.insert(
            RelName::new(&namespace, &relname),
            TableRow {
                target_database: row.try_get("target_database").ok().flatten(),
                target_table: row.try_get("target_table").ok().flatten(),
                replicate: row.try_get("replicate").ok().flatten(),
                initial_load: row.try_get("initial_load").ok().flatten(),
            },
        );
    }

    for row in client
        .query(
            &format!("SELECT namespace, relname, attname, target_type FROM {s}.config_column"),
            &[],
        )
        .await
        .context("read config_column")?
    {
        let namespace: String = row.get("namespace");
        let relname: String = row.get("relname");
        let attname: String = row.get("attname");
        overlay.columns.insert(
            (RelName::new(&namespace, &relname), attname),
            ColumnRow {
                target_type: row.get("target_type"),
            },
        );
    }

    let (has_global, n_ns, n_tbl, n_col) = (
        overlay.global.is_some(),
        overlay.namespaces.len(),
        overlay.tables.len(),
        overlay.columns.len(),
    );
    // Snapshot table rows for the boot opt-in dispatch: on restart the resume
    // cursor is past these rows' commit LSN, so WAL replay won't re-deliver
    // them — the seed is the only chance to re-materialise their scope.
    let table_rows: Vec<(RelName, TableRow)> = overlay
        .tables
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    resolver.seed_overlay(overlay).await;
    tracing::info!(
        target: "walshadow::config",
        schema,
        global = has_global,
        namespaces = n_ns,
        tables = n_tbl,
        columns = n_col,
        "runtime config overlay seeded from source PG",
    );
    Ok(table_rows)
}

async fn apply_toml_initial_loads(
    catalog: &Arc<Mutex<ShadowCatalog>>,
    backfiller: Option<&Arc<walshadow::copy_backfill::CopyBackfiller>>,
    table_initial_loads: &std::collections::HashMap<RelName, String>,
    active_tables: &HashSet<RelName>,
    sql_scoped_tables: &HashSet<RelName>,
    raw_start: u64,
) -> anyhow::Result<()> {
    for (rel, mode) in table_initial_loads {
        if !active_tables.contains(rel) || sql_scoped_tables.contains(rel) {
            continue;
        }
        match InitialLoadMode::parse(mode) {
            Some(InitialLoadMode::None) => {}
            Some(parsed) => {
                let desc = catalog.lock().await.descriptor_by_name(rel).await?;
                let Some(desc) = desc else {
                    tracing::warn!(
                        target: "walshadow::config",
                        qname = %rel,
                        "TOML initial_load ignored: source rel unknown",
                    );
                    continue;
                };
                match backfiller {
                    Some(b) => b.note_opt_in(&desc, parsed, raw_start).await,
                    None => tracing::info!(
                        target: "walshadow::config",
                        qname = %rel,
                        mode,
                        "TOML initial_load requested but no backfiller wired; streaming from start LSN only",
                    ),
                }
            }
            None => tracing::warn!(
                target: "walshadow::config",
                qname = %rel,
                mode,
                "unknown TOML initial_load mode; streaming from start LSN only",
            ),
        }
    }
    Ok(())
}

/// SIGHUP listener: re-reads `--ch-config` and republishes the resolved
/// snapshot through the resolver (CLI overrides stay on top). Read/parse
/// errors keep the last snapshot in effect; absent resolver (metrics-only)
/// is a no-op tap.
fn spawn_sighup_handler(
    mut sig: tokio::signal::unix::Signal,
    resolver: Option<Arc<ConfigResolver>>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            if sig.recv().await.is_none() {
                return;
            }
            let Some(resolver) = resolver.as_ref() else {
                tracing::info!(target: "walshadow::sighup", "SIGHUP ignored (no --ch-config)");
                continue;
            };
            match resolver.reload().await {
                Ok(()) => tracing::info!(
                    target: "walshadow::sighup",
                    "ch-config reload published",
                ),
                Err(e) => tracing::warn!(
                    target: "walshadow::sighup",
                    error = %e,
                    "ch-config reload failed; existing config preserved",
                ),
            }
        }
    })
}

/// Applies each republished [`ResolvedConfig`] snapshot to the live routing
/// map. Full swap of the operator mapping, matching the boot seed; runs
/// until the resolver's sender drops (SIGHUP disabled or daemon teardown).
fn spawn_mapping_refresher(
    mut config_rx: watch::Receiver<Arc<ResolvedConfig>>,
    mapping: MappingHandle,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        // Boot value already seeded into `mapping`; react to republishes.
        while config_rx.changed().await.is_ok() {
            let tables = config_rx.borrow_and_update().tables.clone();
            *mapping.write().await = tables;
            tracing::info!(
                target: "walshadow::config",
                "routing map refreshed from resolved config",
            );
        }
    })
}

/// Max unsynced segments queued before the pump blocks on `on_segment`;
const SEGMENT_FSYNC_QUEUE: usize = 64;

/// Background segment durability: drain the fsync queue, then `syncfs` the
/// filesystem holding `out_dir` once per batch — this flushes every written
/// segment + manifest + the directory entries in one syscall, avoiding the
/// per-file `open`+`sync_data` walk (à la PG `recovery_init_sync_method=syncfs`).
/// Then advance `durable_lsn` to the highest covered LSN. A sync error sets
/// `fatal` and stops (the main loop then exits rather than advertising
/// durability past the failure).
///
/// `syncfs` error reporting requires Linux >= 5.8; the fd is held for the task's
/// lifetime so writeback errors on this filesystem are seen. Because it flushes
/// the *whole* filesystem, `out_dir` should live on a volume walshadow owns —
/// on a shared disk it may block on unrelated writeback.
fn spawn_segment_fsync(
    out_dir: PathBuf,
    mut rx: tokio::sync::mpsc::Receiver<SegFsync>,
    durable_lsn: Arc<AtomicU64>,
    fatal: walshadow::pipeline::Fatal,
) -> tokio::task::JoinHandle<()> {
    use std::os::unix::io::AsRawFd;
    tokio::spawn(async move {
        let dir = match std::fs::File::open(&out_dir) {
            Ok(f) => f,
            Err(e) => {
                fatal.set(format!("open {} for syncfs: {e}", out_dir.display()));
                return;
            }
        };
        let dirfd = dir.as_raw_fd();
        while let Some(item) = rx.recv().await {
            let mut max_lsn = item.end_lsn;
            while let Ok(next) = rx.try_recv() {
                max_lsn = max_lsn.max(next.end_lsn);
            }
            let synced = tokio::task::spawn_blocking(move || {
                if unsafe { libc::syncfs(dirfd) } == 0 {
                    Ok(())
                } else {
                    Err(std::io::Error::last_os_error())
                }
            })
            .await;
            match synced {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    fatal.set(format!("syncfs {}: {e}", out_dir.display()));
                    return;
                }
                Err(e) => {
                    fatal.set(format!("syncfs join {}: {e}", out_dir.display()));
                    return;
                }
            }
            durable_lsn.fetch_max(max_lsn, Ordering::Release);
        }
    })
}

/// Every [`DEFAULT_TRIM_INTERVAL`], read shadow replay LSN and last
/// restartpoint REDO LSN, then trim below
/// `min(replay_lsn - retention_bytes, redo)`
/// Keep WAL from restartpoint because shadow resumes recovery there
/// Reconnect after failed query because daemon may restart shadow
fn spawn_retention(
    out_dir: PathBuf,
    retention_bytes: u64,
    shadow_conninfo: String,
    shadow_replay_lsn: Arc<AtomicU64>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut client: Option<tokio_postgres::Client> = None;
        loop {
            tokio::time::sleep(DEFAULT_TRIM_INTERVAL).await;
            if client.is_none() {
                match open_retention_client(&shadow_conninfo).await {
                    Ok(c) => client = Some(c),
                    Err(e) => {
                        tracing::warn!(
                            target: "walshadow::retention",
                            error = %e,
                            "shadow connect failed; retrying next cycle",
                        );
                        continue;
                    }
                }
            }
            let (replay, redo) = match query_replay_state(client.as_ref().expect("just set")).await
            {
                Ok(v) => v,
                Err(e) => {
                    tracing::warn!(target: "walshadow::retention", error = %e, "lsn query");
                    client = None;
                    continue;
                }
            };
            // Wait until shadow replays first record
            let Some(lsn) = replay else { continue };
            shadow_replay_lsn.fetch_max(lsn, Ordering::Release);
            let cutoff = manifest::retention_cutoff(lsn, retention_bytes, redo);
            match trim_below_lsn(&out_dir, cutoff).await {
                Ok(r) if r.segments_removed > 0 => {
                    tracing::info!(
                        target: "walshadow::retention",
                        segments = r.segments_removed,
                        manifests = r.manifests_removed,
                        partials = r.partials_removed,
                        bytes_freed = r.bytes_freed,
                        cutoff_lsn = format_pg_lsn(cutoff).to_string(),
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

async fn query_replay_state(client: &tokio_postgres::Client) -> Result<(Option<u64>, Option<u64>)> {
    let row = client
        .query_one(
            "SELECT pg_last_wal_replay_lsn(), redo_lsn FROM pg_control_checkpoint()",
            &[],
        )
        .await?;
    let replay: Option<PgLsn> = row.get(0);
    let redo: Option<PgLsn> = row.get(1);
    Ok((replay.map(u64::from), redo.map(u64::from)))
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
/// CPU seconds + RSS bytes from `/proc/self`. Linux-only; `(0.0, 0)` if
/// unreadable. Assumes `CLK_TCK` 100 (USER_HZ) and `VmRSS` in kB.
fn read_process_stats() -> (f64, u64) {
    const CLK_TCK: f64 = 100.0;
    let cpu = std::fs::read_to_string("/proc/self/stat")
        .ok()
        .and_then(|s| {
            // Split after the last ')' (comm may hold spaces/parens): utime
            // (field 14) and stime (15) are then indices 11 and 12.
            let rest = s.rsplit_once(')')?.1;
            let f: Vec<&str> = rest.split_whitespace().collect();
            let utime: u64 = f.get(11)?.parse().ok()?;
            let stime: u64 = f.get(12)?.parse().ok()?;
            Some((utime + stime) as f64 / CLK_TCK)
        })
        .unwrap_or(0.0);
    let rss = std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|s| {
            let kb: u64 = s
                .lines()
                .find(|l| l.starts_with("VmRSS:"))?
                .split_whitespace()
                .nth(1)?
                .parse()
                .ok()?;
            Some(kb * 1024)
        })
        .unwrap_or(0);
    (cpu, rss)
}

/// Drain-resident + spool gauge readings taken under one buffer lock
struct DrainResident {
    total: u64,
    chunks: u64,
    rows: u64,
    spool: u64,
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
    pump_queue_depth: u64,
    queue_records_out_total: u64,
    xact_stats: &walshadow::xact_buffer::XactBufferStats,
    drain_resident: DrainResident,
    budget: Option<&walshadow::budget::MemoryBudget>,
    decoder_stats: &walshadow::decoder_sink::DecoderStats,
    emitter_stats: Option<&walshadow::ch_emitter::EmitterStats>,
    oracle_stats: Option<&walshadow::oracle::OracleStats>,
    uptime_secs: u64,
    shadow_view: ShadowMetricsView,
    config_resolver: Option<&ConfigResolver>,
    backfiller: Option<&walshadow::copy_backfill::CopyBackfiller>,
) {
    use std::collections::BTreeMap;
    use walshadow::record::rmgr_label;
    let (proc_cpu, proc_rss) = read_process_stats();
    let mut by_rm = BTreeMap::new();
    for ((rm, route), n) in &rec_metrics.by_rm_route {
        let key = (
            rmgr_label(*rm).to_string(),
            match route {
                walshadow::record::Route::ToShadow => "to_shadow",
                walshadow::record::Route::ToDecoder => "to_decoder",
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
        drain_resident_bytes: drain_resident.total,
        drain_chunk_resident_bytes: drain_resident.chunks,
        drain_row_resident_bytes: drain_resident.rows,
        toast_xact_spool_bytes: drain_resident.spool,
        resident_payload_bytes: budget.map(|b| b.resident_bytes()).unwrap_or(0),
        resident_payload_peak_bytes: budget.map(|b| b.peak_bytes()).unwrap_or(0),
        memory_budget_waits_total: budget.map(|b| b.waits_total()).unwrap_or(0),
        memory_budget_overshoots_total: budget.map(|b| b.overshoots_total()).unwrap_or(0),
        bootstrap_deferred_bytes: emitter_stats
            .map(|s| s.bootstrap_deferred_bytes.load(Ordering::Relaxed))
            .unwrap_or(0),
        bootstrap_deferred_spool_bytes: emitter_stats
            .map(|s| s.bootstrap_deferred_spool_bytes.load(Ordering::Relaxed))
            .unwrap_or(0),
        spill_evictions_total: xact_stats.spill_evictions_total,
        xacts_committed_total: xact_stats.committed_xacts_total,
        xacts_aborted_total: xact_stats.aborted_xacts_total,
        decoder_decoded_total: decoder_stats.decoded.load(Ordering::Relaxed),
        decoder_partial_total: decoder_stats.partial.load(Ordering::Relaxed),
        decoder_toast_chunks_total: decoder_stats.toast_chunks_buffered.load(Ordering::Relaxed),
        decoder_toast_malformed_total: decoder_stats.toast_chunks_malformed.load(Ordering::Relaxed),
        decoder_toast_deletes_total: decoder_stats.toast_chunk_deletes.load(Ordering::Relaxed),
        toast_tombstones_stored_total: emitter_stats
            .map(|s| s.toast_tombstones_stored.load(Ordering::Relaxed))
            .unwrap_or(0),
        toast_values_filled_superseded_total: emitter_stats
            .map(|s| s.toast_values_filled_superseded.load(Ordering::Relaxed))
            .unwrap_or(0),
        toast_values_filled_mismatch_total: emitter_stats
            .map(|s| s.toast_values_filled_mismatch.load(Ordering::Relaxed))
            .unwrap_or(0),
        toast_mirror_truncates_total: emitter_stats
            .map(|s| s.toast_mirror_truncates.load(Ordering::Relaxed))
            .unwrap_or(0),
        toast_mirror_retires_total: emitter_stats
            .map(|s| s.toast_mirror_retires.load(Ordering::Relaxed))
            .unwrap_or(0),
        toast_rewrite_barriers_total: emitter_stats
            .map(|s| s.toast_rewrite_barriers.load(Ordering::Relaxed))
            .unwrap_or(0),
        toast_stash_buffered_total: decoder_stats.toast_stash_buffered.load(Ordering::Relaxed),
        toast_stash_decoded_total: emitter_stats
            .map(|s| s.toast_stash_decoded.load(Ordering::Relaxed))
            .unwrap_or(0),
        toast_stash_discarded_total: emitter_stats
            .map(|s| s.toast_stash_discarded.load(Ordering::Relaxed))
            .unwrap_or(0),
        toast_stash_skipped_total: emitter_stats
            .map(|s| s.toast_stash_skipped.load(Ordering::Relaxed))
            .unwrap_or(0),
        emitter_rows_total: emitter_stats
            .map(|s| s.rows_emitted.load(Ordering::Relaxed))
            .unwrap_or(0),
        emitter_blocks_total: emitter_stats
            .map(|s| s.blocks_sent.load(Ordering::Relaxed))
            .unwrap_or(0),
        pump_queue_depth,
        queue_records_out_total,
        queue_jobs_out_total: emitter_stats
            .map(|s| s.queue_jobs_out.load(Ordering::Relaxed))
            .unwrap_or(0),
        decode_jobs_in_total: emitter_stats
            .map(|s| s.decode_jobs_in.load(Ordering::Relaxed))
            .unwrap_or(0),
        decode_rows_out_total: emitter_stats
            .map(|s| s.decode_rows_out.load(Ordering::Relaxed))
            .unwrap_or(0),
        insertbatch_rows_in_total: emitter_stats
            .map(|s| s.insertbatch_rows_in.load(Ordering::Relaxed))
            .unwrap_or(0),
        insertbatch_batches_out_total: emitter_stats
            .map(|s| s.insertbatch_batches_out.load(Ordering::Relaxed))
            .unwrap_or(0),
        inserter_batches_in_total: emitter_stats
            .map(|s| s.inserter_batches_in.load(Ordering::Relaxed))
            .unwrap_or(0),
        process_cpu_seconds_total: proc_cpu,
        process_resident_memory_bytes: proc_rss,
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
        config_pending_decl_rels: config_resolver.map(|r| r.pending_decl_count()).unwrap_or(0),
        config_replicate_opt_in_total: config_resolver.map(|r| r.opt_in_total()).unwrap_or(0),
        config_replicate_opt_out_total: config_resolver.map(|r| r.opt_out_total()).unwrap_or(0),
        config_backfills_pending: backfiller.map(|b| b.pending_count()).unwrap_or(0),
        config_backfills_pending_by_mode: backfiller.map(|b| b.pending_by_mode()).unwrap_or([0; 3]),
    };
    registry.set(snap).await;
}

/// Recover from a source stream error. A transient drop retries the reconnect
/// with exponential backoff until the source is back; a recycled segment
/// (58P01) is fatal — the resume point is gone, so the daemon exits and
/// recovery is a re-seed via config `initial_load` on restart, not a reconnect.
async fn reconnect_or_fatal(
    e: anyhow::Error,
    cfg: &PgConfig,
    slot: Option<&str>,
    resume_lsn: u64,
    timeline: u32,
    status_interval: Duration,
) -> Result<SourceFeed> {
    use backon::{ExponentialBuilder, Retryable};

    let recycled = |e: anyhow::Error| {
        e.context(
            "source WAL segment recycled past the resume point; \
             re-seed the affected tables via config initial_load \
             (base_backup/object_store), then restart",
        )
    };
    if walshadow::source_feed::is_wal_segment_removed(&e) {
        return Err(recycled(e));
    }
    // Ride out a transient drop (source restart, wal_sender_timeout, brief
    // network blip); only a recycled segment stops the retry and is fatal.
    (|| SourceFeed::reconnect(cfg, slot, resume_lsn, timeline, status_interval))
        .retry(
            ExponentialBuilder::default()
                .with_min_delay(Duration::from_millis(200))
                .with_max_delay(Duration::from_secs(10))
                .without_max_times(),
        )
        .when(|e: &anyhow::Error| !walshadow::source_feed::is_wal_segment_removed(e))
        .notify(|e: &anyhow::Error, d: Duration| {
            tracing::warn!(target: "walshadow", error = %e, retry_in_ms = d.as_millis() as u64, "source reconnect failed — retrying");
        })
        .await
        .map_err(recycled)
}

/// Run BASE_BACKUP into new shadow data dir and return backup `end_lsn`
/// Caller starts WAL pump from returned LSN, then starts and supervises
/// shadow in [`run`]
/// [`BOOTSTRAP_INCOMPLETE_MARKER`] remains after any failed bootstrap;
/// automatic rebootstrap is intentionally unsupported
///
/// `ch_config` `Some`: bootstrap rows route through the shared insert tail
/// (synthetic INSERT `_lsn = start_lsn`, `_commit_ts = 0`, `_is_deleted = 0`).
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
    prepare_bootstrap_dir(&shadow_data_dir)
        .await
        .context("prepare shadow data dir for bootstrap")?;

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
    type ObjectStoreHandles = (walrus::config::Settings, walrus::storage::DynStorage);
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
                    // Ship pg_wal [start_lsn, end_lsn] inside base.tar so
                    // daemon-owned shadow reaches `minRecoveryPoint` locally.
                    // Otherwise `pg_ctl -w start` polls `restore_command`
                    // against empty `out/` before WAL pump starts and times out.
                    wal: true,
                };
                (Box::new(DirectSource::new(src_cfg.clone(), opts)), None)
            }
            BootstrapMode::ObjectStore => {
                let settings =
                    walrus::config::Settings::resolve(&walrus::config::Vars::default(), None)
                        .context("bootstrap: Settings::resolve (WALG_* env vars)")?;
                let storage = settings
                    .build_storage()
                    .context("bootstrap: build storage from WALG_* env vars")?;
                // ObjectStoreSource canonicalises via
                // `walrus::pg::backup::fetch::resolve_name`.
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
    // Build the toast resolver up front, sharing its counters with the
    // bootstrap tail. The store-toast flag tells the page walk whether to
    // decode pg_toast_* pages.
    let bootstrap_stats = Arc::new(EmitterStats::default());
    // Leaf-only pool for the bootstrap tail: caps each value (V3) and
    // bounds decoded rows in flight to insert ack; no admission stage
    let resolver = if let Some(cfg) = &ch_config {
        ToastResolver::from_config(cfg, bootstrap_stats.clone()).with_budget(
            walshadow::budget::MemoryBudget::new(cfg.resident_payload_max),
        )
    } else {
        ToastResolver::disabled()
    };
    let store_toast = resolver.stores_chunks();
    let (rx, pump) = spawn_greenfield_bootstrap(cfg, source, catalog_map, store_toast);

    let (shipped, outcome) = if let Some(emitter_cfg) = ch_config {
        // Route bootstrap rows through the shared insert tail. Bootstrap
        // is the easy case: every row op=Insert at _lsn = start_lsn, no
        // aborts / TRUNCATE / DDL. Keep operator's flush_timeout; tail
        // defaults 0 to its own partial-flush deadline.
        let addr = format!("{}:{}", emitter_cfg.host, emitter_cfg.port);
        let stats = bootstrap_stats.clone();
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
        let mapping: MappingHandle = Arc::new(tokio::sync::RwLock::new(emitter_cfg.tables.clone()));
        tracing::info!(
            target: "walshadow::bootstrap",
            addr = %addr,
            inserters = inserter_pool_size.max(1),
            "bootstrap insert tail started",
        );

        let deferred_path = args.spill_dir.join("bootstrap_deferred.bin");
        tokio::fs::remove_file(&deferred_path).await.ok();
        let drain = tokio::spawn(bootstrap::drain(
            rx,
            drain_catalog,
            mapping,
            msg_tx.clone(),
            ack.clone(),
            stats.clone(),
            resolver.clone(),
            walshadow::spool::DeferredSpool::new(
                deferred_path,
                walshadow::spool::DEFERRED_SPOOL_MEM_MAX,
            ),
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
    } else {
        // Metrics-only: bootstrap rows counted, not shipped.
        let mut observer = MetricsTupleObserver::default();
        let (drain_res, pump_res) = tokio::join!(drain_backfill(rx, &mut observer), pump);
        let shipped = drain_res.context("bootstrap drain")?;
        let outcome: BootstrapOutcome = pump_res
            .context("bootstrap pump join")?
            .context("bootstrap pump")?;
        (shipped, outcome)
    };

    tracing::info!(
        target: "walshadow::bootstrap",
        start_lsn = format_pg_lsn(outcome.start.start_lsn).to_string(),
        end_lsn = format_pg_lsn(outcome.end.end_lsn).to_string(),
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
    // base.tar). Skipping blocks shadow startup: restore_command sees empty
    // out/ and walsender has not bound yet.
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

    tokio::fs::remove_file(shadow_data_dir.join(BOOTSTRAP_INCOMPLETE_MARKER))
        .await
        .context("clear completed bootstrap marker")?;

    Ok(outcome.end.end_lsn)
}

/// Mark bootstrap before extraction, clear only after backup and required
/// object-store WAL land successfully. Refuse automatic rebootstrap when
/// marker survives a failed run
const BOOTSTRAP_INCOMPLETE_MARKER: &str = "walshadow_bootstrap.incomplete";

/// Choose external management, one-time bootstrap, or resume from
/// `--bootstrap-shadow-data-dir` and data dir state
/// Mode only chooses bootstrap source
enum ShadowStart {
    /// Connect to externally managed shadow when no data dir is given
    External,
    Bootstrap(PathBuf),
    Resume(PathBuf),
}

fn resolve_shadow_start(args: &Args) -> Result<ShadowStart> {
    let Some(dir) = &args.bootstrap_shadow_data_dir else {
        anyhow::ensure!(
            matches!(args.bootstrap_mode, BootstrapMode::Off),
            "--bootstrap-mode {:?} requires --bootstrap-shadow-data-dir",
            args.bootstrap_mode,
        );
        return Ok(ShadowStart::External);
    };
    anyhow::ensure!(
        args.walsender_bind.port() != 0,
        "--walsender-bind {} has port 0; daemon-owned shadow bakes this \
         address into shadow's primary_conninfo before shadow starts, so the \
         port must be known upfront, pass an explicit --walsender-bind port",
        args.walsender_bind,
    );
    for (flag, other) in [
        ("--out-dir", &args.out_dir),
        ("--spill-dir", &args.spill_dir),
        ("--shadow-socket-dir", &args.shadow_socket_dir),
    ] {
        anyhow::ensure!(
            !paths_overlap(dir, other),
            "--bootstrap-shadow-data-dir {} overlaps {flag} {}",
            dir.display(),
            other.display(),
        );
    }
    anyhow::ensure!(
        !dir.join(BOOTSTRAP_INCOMPLETE_MARKER).exists(),
        "shadow data dir {} contains {BOOTSTRAP_INCOMPLETE_MARKER}; bootstrap incomplete, automatic rebootstrap unsupported, choose a new empty data dir or use operator recovery",
        dir.display(),
    );
    if dir.join("PG_VERSION").exists() {
        if !matches!(args.bootstrap_mode, BootstrapMode::Off) {
            tracing::info!(
                target: "walshadow::bootstrap",
                data_dir = %dir.display(),
                "shadow data dir already initialized, resuming without bootstrap",
            );
        }
        return Ok(ShadowStart::Resume(dir.clone()));
    }
    anyhow::ensure!(
        !matches!(args.bootstrap_mode, BootstrapMode::Off),
        "shadow data dir {} does not contain an initialized cluster; --bootstrap-mode off cannot bootstrap it, pass direct or object_store",
        dir.display(),
    );
    Ok(ShadowStart::Bootstrap(dir.clone()))
}

/// True if `a` and `b` are the same path, or one is an ancestor of the other
fn paths_overlap(a: &Path, b: &Path) -> bool {
    match (std::path::absolute(a), std::path::absolute(b)) {
        (Ok(a), Ok(b)) => a == b || a.starts_with(&b) || b.starts_with(&a),
        _ => true,
    }
}

/// Require empty data dir and mark bootstrap in progress
/// Never clear partial or initialized standby state automatically
async fn prepare_bootstrap_dir(dir: &Path) -> Result<()> {
    tokio::fs::create_dir_all(dir)
        .await
        .with_context(|| format!("create {}", dir.display()))?;
    let mut rd = tokio::fs::read_dir(dir).await?;
    anyhow::ensure!(
        rd.next_entry().await?.is_none(),
        "shadow data dir {} is non-empty; automatic rebootstrap unsupported, choose a new empty data dir or use operator recovery",
        dir.display(),
    );
    tokio::fs::write(dir.join(BOOTSTRAP_INCOMPLETE_MARKER), b"").await?;
    Ok(())
}

fn build_owned_shadow(args: &Args, data_dir: PathBuf) -> Shadow {
    let mut cfg = ShadowConfig::new(data_dir, args.out_dir.clone());
    cfg.port = args.shadow_port;
    cfg.socket_dir = args.shadow_socket_dir.clone();
    cfg.ctl_timeout = Duration::from_secs(args.shadow_connect_timeout);
    cfg.user = args.shadow_user.clone();
    cfg.dbname = args.shadow_dbname.clone();
    Shadow::new(cfg)
}

/// Return `None` for kernel-assigned port because it may change after
/// restart. Shadow then reads only archive through `restore_command`
fn walsender_primary_conninfo(bind: SocketAddr) -> Option<String> {
    (bind.port() != 0).then(|| {
        format!(
            "host={} port={} user=walshadow application_name=shadow sslmode=disable",
            bind.ip(),
            bind.port(),
        )
    })
}

/// Start daemon-owned shadow with current walsender address and minimum
/// GUC values from its `pg_control`
/// After fresh bootstrap, wait for backup `end_lsn`; direct mode includes
/// required WAL in `base.tar`
/// Restart a postmaster left alive by an unclean prior exit so it binds
/// this daemon's port, socket, and walsender address
async fn start_owned_shadow(
    shadow: &Arc<Shadow>,
    conninfo: Option<String>,
    replay_target: Option<u64>,
    replay_timeout: Duration,
) -> Result<()> {
    let s = shadow.clone();
    tokio::task::spawn_blocking(move || -> Result<()> {
        if s.is_running().context("shadow status probe")? {
            // Adopt only fires after unclean prior exit left the postmaster
            // alive holding stale port/socket/primary_conninfo. Stop so the
            // restart below binds params this daemon connects and streams with;
            // start_with_floor_retry regenerates conf.
            tracing::warn!(
                target: "walshadow::shadow",
                "shadow alive from unclean exit; restarting under fresh config",
            );
            s.stop().context("stop stale shadow before restart")?;
        }
        s.clear_stale_pid().context("clear stale postmaster.pid")?;
        s.start_with_floor_retry(conninfo.as_deref())
            .context("shadow start")?;
        if let Some(target) = replay_target {
            let lsn = s
                .wait_for_replay(target, replay_timeout)
                .context("wait for shadow replay of bootstrap end_lsn")?;
            tracing::info!(
                target: "walshadow::shadow",
                replay_lsn = format_pg_lsn(lsn).to_string(),
                "shadow caught up to bootstrap end_lsn",
            );
        }
        Ok(())
    })
    .await
    .context("shadow start task")?
}

const SHADOW_PROBE_INTERVAL: Duration = Duration::from_secs(2);
const SHADOW_RESTART_BACKOFF_MAX: Duration = Duration::from_secs(60);

/// Supervise daemon-owned shadow, restarting stopped postmaster with
/// backoff. `ShadowCatalog` reconnects after restart
/// Read minimum GUC values from `pg_control` before each restart because
/// replayed `XLOG_PARAMETER_CHANGE` may raise them
/// Call `shutdown` on clean exit; Drop is just a fallback, its abort
/// can race a restart already in flight on the blocking pool
struct ShadowLifecycle {
    shadow: Arc<Shadow>,
    supervisor: Option<tokio::task::JoinHandle<()>>,
    cancel: CancellationToken,
}

impl ShadowLifecycle {
    fn spawn(shadow: Arc<Shadow>, conninfo: Option<String>) -> Self {
        let cancel = CancellationToken::new();
        let supervisor = tokio::spawn(Self::supervise(shadow.clone(), conninfo, cancel.clone()));
        Self {
            shadow,
            supervisor: Some(supervisor),
            cancel,
        }
    }

    async fn supervise(shadow: Arc<Shadow>, conninfo: Option<String>, cancel: CancellationToken) {
        let mut backoff = Duration::from_secs(1);
        // Edge-trigger the foreign-pause log so a held operator pause does
        // not spam once per tick
        let mut foreign_logged = false;
        loop {
            tokio::select! {
                () = cancel.cancelled() => return,
                () = tokio::time::sleep(SHADOW_PROBE_INTERVAL) => {}
            }
            match probe_blocking(&shadow, |s| s.is_running()).await {
                Some(true) => {
                    backoff = Duration::from_secs(1);
                    // Higher GUC requirement pauses active hot standby
                    // Resume forces shutdown, then restart uses new values
                    // Ignore probe errors while psql waits for consistency
                    let s = shadow.clone();
                    let outcome =
                        tokio::task::spawn_blocking(move || s.try_pg_wal_replay_resume()).await;
                    match outcome {
                        Ok(Ok(ResumeOutcome::ResumedForFloor)) => {
                            foreign_logged = false;
                            tracing::warn!(
                                target: "walshadow::shadow",
                                "shadow replay paused because GUC value is below primary; \
                                 resumed replay to restart with required value",
                            );
                        }
                        Ok(Ok(ResumeOutcome::PausedForeign)) => {
                            if !foreign_logged {
                                foreign_logged = true;
                                tracing::info!(
                                    target: "walshadow::shadow",
                                    "shadow replay paused for a reason other than GUC floor \
                                     (eg operator pg_wal_replay_pause); leaving paused",
                                );
                            }
                        }
                        Ok(Ok(ResumeOutcome::NotPaused)) => foreign_logged = false,
                        _ => {}
                    }
                }
                Some(false) => {
                    tracing::warn!(
                        target: "walshadow::shadow",
                        "shadow postmaster stopped, restarting",
                    );
                    let ci = conninfo.clone();
                    let restarted = probe_blocking(&shadow, move |s| {
                        s.clear_stale_pid()?;
                        s.start_with_floor_retry(ci.as_deref())
                    })
                    .await;
                    if restarted.is_some() {
                        tracing::info!(target: "walshadow::shadow", "shadow restarted");
                        backoff = Duration::from_secs(1);
                    } else {
                        tokio::select! {
                            () = cancel.cancelled() => return,
                            () = tokio::time::sleep(backoff) => {}
                        }
                        backoff = (backoff * 2).min(SHADOW_RESTART_BACKOFF_MAX);
                    }
                }
                None => {}
            }
        }
    }

    /// Signal supervisor and join it — this waits out any probe/restart
    /// already in flight rather than racing past it — then stop shadow
    /// with the now-settled state. Call on every clean exit path; Drop
    /// covers whatever this misses.
    async fn shutdown(mut self) {
        self.cancel.cancel();
        if let Some(h) = self.supervisor.take()
            && let Err(e) = h.await
        {
            tracing::warn!(target: "walshadow::shadow", error = %e, "shadow supervisor join failed");
        }
        if let Some(true) = probe_blocking(&self.shadow, |s| s.is_running()).await
            && probe_blocking(&self.shadow, |s| s.stop()).await.is_none()
        {
            tracing::warn!(target: "walshadow::shadow", "shadow stop on shutdown failed");
        }
    }
}

/// Run blocking `pg_ctl` operation outside async runtime
/// Return `None` after logging failure
async fn probe_blocking<T: Send + 'static>(
    shadow: &Arc<Shadow>,
    op: impl FnOnce(&Shadow) -> walshadow::shadow::Result<T> + Send + 'static,
) -> Option<T> {
    let s = shadow.clone();
    match tokio::task::spawn_blocking(move || op(&s)).await {
        Ok(Ok(v)) => Some(v),
        Ok(Err(e)) => {
            tracing::warn!(target: "walshadow::shadow", error = %e, "shadow op failed");
            None
        }
        Err(e) => {
            tracing::warn!(target: "walshadow::shadow", error = %e, "shadow op join failed");
            None
        }
    }
}

impl Drop for ShadowLifecycle {
    fn drop(&mut self) {
        if let Some(h) = &self.supervisor {
            h.abort();
        }
        // Daemon is exiting, blocking pg_ctl cannot delay other work
        match self.shadow.is_running() {
            Ok(true) => {
                if let Err(e) = self.shadow.stop() {
                    tracing::warn!(
                        target: "walshadow::shadow",
                        error = %e,
                        "shadow stop on daemon exit failed",
                    );
                }
            }
            Ok(false) => {}
            Err(e) => tracing::warn!(
                target: "walshadow::shadow",
                error = %e,
                "shadow status probe on daemon exit failed",
            ),
        }
    }
}

/// Pull WAL `[start_lsn, end_lsn]` on `timeline` from wal-rus storage into
/// `<shadow_data_dir>/pg_wal/` so daemon-owned shadow recovery reaches
/// `minRecoveryPoint` from local WAL, depending on neither `restore_command`
/// (filtered out-dir stays empty until WAL pump starts) nor `primary_conninfo`
/// (walsender binds later in `run`).
///
/// `walrus::pg::backup::push::handle` sets `wal: false`, so object-store
/// tars don't carry WAL; it lives in `wal_005/` (wal-push / archive_command).
/// Direct mode inlines the same segments via `BaseBackupOpts { wal: true }`.
///
/// Missing segments surface as `WAL <name> not found in storage` from
/// wal-rus's `fetch::handle` — the operator's archiving pipeline left a gap.
async fn fetch_wal_into_pg_wal(
    settings: &walrus::config::Settings,
    storage: walrus::storage::DynStorage,
    shadow_data_dir: &Path,
    start_lsn: u64,
    end_lsn: u64,
    timeline: u32,
) -> Result<()> {
    use walrus::pg::wal::segment::SegmentName;

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
        // Off: loop enumerates every segment in [start,end] explicitly, so
        // read-ahead would only duplicate the next iteration's fetch & risk
        // downloading past end_lsn
        walrus::pg::wal::fetch::handle(
            settings,
            storage.clone(),
            &name,
            &dst,
            walrus::pg::wal::fetch::Prefetch::Off,
        )
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
        start_lsn = format_pg_lsn(start_lsn).to_string(),
        end_lsn = format_pg_lsn(end_lsn).to_string(),
        timeline,
        "hydrated shadow pg_wal from object store",
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args_from(argv: &[&str]) -> Args {
        let base = [
            "walshadow-stream",
            "--out-dir",
            "/tmp/out",
            "--spill-dir",
            "/tmp/spill",
            "--shadow-socket-dir",
            "/tmp/sock",
        ];
        Args::parse_from(base.iter().copied().chain(argv.iter().copied()))
    }

    #[test]
    fn shadow_start_external_without_data_dir() {
        assert!(matches!(
            resolve_shadow_start(&args_from(&[])).unwrap(),
            ShadowStart::External
        ));
        assert!(resolve_shadow_start(&args_from(&["--bootstrap-mode", "direct"])).is_err());
    }

    #[test]
    fn shadow_start_bootstrap_vs_resume_keys_on_dir_state() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("data");
        std::fs::create_dir_all(&dir).unwrap();
        let dir_str = dir.to_str().unwrap();
        let direct = |d: &str| {
            args_from(&[
                "--bootstrap-mode",
                "direct",
                "--bootstrap-shadow-data-dir",
                d,
                "--walsender-bind",
                "127.0.0.1:5555",
            ])
        };
        let off = |d: &str| {
            args_from(&[
                "--bootstrap-shadow-data-dir",
                d,
                "--walsender-bind",
                "127.0.0.1:5555",
            ])
        };

        // Direct bootstraps empty dir, off rejects it
        assert!(matches!(
            resolve_shadow_start(&direct(dir_str)).unwrap(),
            ShadowStart::Bootstrap(_)
        ));
        assert!(resolve_shadow_start(&off(dir_str)).is_err());

        // Resume initialized dir regardless of mode
        std::fs::write(dir.join("PG_VERSION"), b"17\n").unwrap();
        assert!(matches!(
            resolve_shadow_start(&direct(dir_str)).unwrap(),
            ShadowStart::Resume(_)
        ));
        assert!(matches!(
            resolve_shadow_start(&off(dir_str)).unwrap(),
            ShadowStart::Resume(_)
        ));

        // Incomplete bootstrap never triggers automatic rebootstrap
        std::fs::write(dir.join(BOOTSTRAP_INCOMPLETE_MARKER), b"").unwrap();
        assert!(resolve_shadow_start(&direct(dir_str)).is_err());
        assert!(resolve_shadow_start(&off(dir_str)).is_err());
        assert!(dir.join("PG_VERSION").exists());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn prepare_bootstrap_dir_marks_empty_dir() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("data");
        prepare_bootstrap_dir(&dir).await.unwrap();
        assert!(dir.join(BOOTSTRAP_INCOMPLETE_MARKER).exists());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn prepare_bootstrap_dir_refuses_nonempty_dir_without_deleting_it() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("data");
        std::fs::create_dir_all(dir.join("sibling-archive")).unwrap();
        assert!(prepare_bootstrap_dir(&dir).await.is_err());
        assert!(dir.join("sibling-archive").exists());
    }

    #[test]
    fn shadow_start_rejects_kernel_picked_port_for_owned_shadow() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("data");
        std::fs::create_dir_all(&dir).unwrap();
        let dir_str = dir.to_str().unwrap();
        // Default --walsender-bind is 127.0.0.1:0 (kernel-picked); daemon
        // can't bake an unknown port into shadow's primary_conninfo.
        assert!(
            resolve_shadow_start(&args_from(&[
                "--bootstrap-mode",
                "direct",
                "--bootstrap-shadow-data-dir",
                dir_str,
            ]))
            .is_err()
        );
    }

    #[test]
    fn walsender_conninfo_skipped_on_kernel_picked_port() {
        assert!(walsender_primary_conninfo("127.0.0.1:0".parse().unwrap()).is_none());
        let ci = walsender_primary_conninfo("127.0.0.1:5441".parse().unwrap()).unwrap();
        assert!(ci.contains("host=127.0.0.1"), "{ci}");
        assert!(ci.contains("port=5441"), "{ci}");
    }
}
