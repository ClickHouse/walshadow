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

#[cfg(not(target_os = "linux"))]
compile_error!(
    "walshadow-stream is supported only on Linux; the PostgreSQL bridge extension may still be built separately with `make -C pgext`"
);

// The pipeline allocates rows on the decode thread(s) and frees them on the
// batcher thread; mimalloc's per-thread caches handle that produce-here/
// free-there pattern far better than glibc's shared arena (which serializes on
// its arena lock under that cross-thread churn).
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use ahash::HashSet;
use anyhow::{Context, Result};
use clap::Parser;
use std::fs;
use std::future::Future;
use std::pin::Pin;
use tokio::sync::{Mutex, watch};
use tokio_postgres::types::PgLsn;
use tokio_util::sync::CancellationToken;
use walrus::pg::backup::{BACKUP_NAME_PREFIX, format_pg_lsn};
use walrus::pg::replication::base_backup::BaseBackupOpts;
use walrus::pg::replication::conn::PgConfig;
use walrus::pg::replication::tls::SslMode;
use walshadow::backfill_bootstrap::{
    BootstrapConfig, BootstrapOutcome, drain_backfill, seed_in_snapshot, spawn_greenfield_bootstrap,
};
use walshadow::backup_source::BackupSource;
use walshadow::backup_source_direct::DirectSource;
use walshadow::backup_source_object_store::ObjectStoreSource;
use walshadow::boundary_hold::{
    BoundaryGateConfig, BoundaryHoldSink, BoundaryHoldStats, CatalogBoundaryGate,
};
use walshadow::ch_emitter::{
    BootstrapMode, DEFAULT_DECODER_POOL, DEFAULT_INSERTER_POOL, EmitterConfig, EmitterStats,
};
use walshadow::config::{CliOverrides, ConfigResolver, ResolvedConfig, SourceConn, cli_over_toml};
use walshadow::decoder_sink::MetricsTupleObserver;
use walshadow::manifest;
use walshadow::mapping::{DropTableStrategy, MappingHandle};
use walshadow::metrics::{MetricsRegistry, MetricsSnapshot, RateEstimator};
use walshadow::pg::{quote_ident, socket_conninfo};
use walshadow::pipeline::{Fatal, PipelineConfig, TailKind, bootstrap, tail};
use walshadow::pos::{
    Drain, EmitterAck, FilterDispatched, FilterDurable, Floor, Gate, Monotone, Pos, ShadowFlush,
    ShadowReplay, SourceReceived,
};
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
use walshadow::source_feed::{SourceEvent, SourceFeed, StandbyStatus};
use walshadow::timeline::TimelineHistory;
use walshadow::toast::ToastResolver;
use walshadow::transition::{
    CrossingState, CrossingWedge, ForkGuards, Switchover, TimelineStats, TransitionError,
    load_boot_history, seed_shadow_branches,
};
use walshadow::wal_stream::WalStream;
use walshadow::xact_buffer::{BufferingDecoderSink, SubxactTracker, XactBuffer, XactBufferConfig};

#[derive(Debug, Clone, PartialEq, Eq)]
struct BootstrapPlan {
    mode: BootstrapMode,
    backup_name: String,
    parallelism: Option<usize>,
}

/// `cli_over_toml` plus a ≥1 clamp for pool/batch sizes.
fn positive_usize(name: &str, cli: Option<usize>, toml: usize) -> usize {
    match cli_over_toml(cli, Some(toml)).unwrap_or(toml) {
        0 => {
            tracing::warn!(target: "walshadow::config", setting = name, "value below 1, using 1");
            1
        }
        n => n,
    }
}

fn resolve_bootstrap(args: &Args, ch: Option<&EmitterConfig>) -> Result<BootstrapPlan> {
    let toml = ch.map(|c| &c.bootstrap);
    // External-shadow (no data dir) can't bootstrap; only default to Direct when
    // a shadow data dir is configured.
    let default_mode = if args.bootstrap_shadow_data_dir.is_some() {
        BootstrapMode::Direct
    } else {
        BootstrapMode::Off
    };
    let mode =
        cli_over_toml(args.bootstrap_mode, toml.and_then(|b| b.mode)).unwrap_or(default_mode);
    let backup_name = cli_over_toml(
        args.bootstrap_backup_name.clone(),
        toml.and_then(|b| b.backup_name.clone()),
    );
    let parallelism = cli_over_toml(
        args.bootstrap_object_store_parallelism,
        toml.and_then(|b| b.object_store_parallelism),
    )
    .map(NonZeroUsize::get);

    if mode != BootstrapMode::ObjectStore {
        for (knob, set) in [
            ("backup_name", backup_name.is_some()),
            ("object_store_parallelism", parallelism.is_some()),
        ] {
            if set {
                tracing::warn!(
                    target: "walshadow::bootstrap",
                    knob,
                    ?mode,
                    "bootstrap {knob} ignored, it applies only to --bootstrap-mode object_store",
                );
            }
        }
    }

    Ok(BootstrapPlan {
        mode,
        backup_name: backup_name.unwrap_or_else(|| "LATEST".into()),
        parallelism,
    })
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
    /// Queueing sink wrapped with the catalog-boundary publication hold:
    /// at a catalog-mutating commit the pump parks here until shadow
    /// replays through the commit's `next_lsn`, so successor bytes reach
    /// neither the shadow wire nor the archive while held.
    decoder_xact: BoundaryHoldSink,
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

/// `walshadow-stream init`: write a config from two connection URLs, so a
/// first run needs no TOML. Detected before daemon-arg parsing, same as `ctl`.
#[derive(Debug, Parser)]
#[command(
    name = "walshadow-stream init",
    about = "Probe source + destination, pick tables, write the config."
)]
struct InitArgs {
    /// Config to write; the daemon then runs with `--ch-config <path>`
    #[arg(
        long,
        env = "WALSHADOW_CH_CONFIG",
        default_value = "/etc/walshadow/ch-config.toml"
    )]
    config: PathBuf,
    #[arg(long, env = walshadow::init::SOURCE_URL_ENV)]
    source_url: Option<String>,
    #[arg(long, env = walshadow::init::CH_URL_ENV)]
    ch_url: Option<String>,
    /// Replicate this table. Two words, schema then table; repeat per table
    #[arg(long, num_args = 2, value_names = ["SCHEMA", "TABLE"])]
    table: Vec<String>,
    /// Replicate every table that has a row key
    #[arg(long)]
    all_tables: bool,
    /// Restrict listing (and `--all-tables`) to one schema
    #[arg(long)]
    schema: Option<String>,
    /// Backfill of rows that pre-date the opt-in
    #[arg(long, default_value = "copy")]
    initial_load: String,
    /// Overwrite an existing config
    #[arg(long)]
    force: bool,
}

impl InitArgs {
    fn into_opts(self) -> walshadow::init::InitOpts {
        walshadow::init::InitOpts {
            config: self.config,
            source_url: self.source_url,
            ch_url: self.ch_url,
            tables: self
                .table
                .as_chunks::<2>()
                .0
                .iter()
                .map(|pair| RelName::new(&pair[0], &pair[1]))
                .collect(),
            all_tables: self.all_tables,
            namespace: self.schema,
            initial_load: self.initial_load,
            force: self.force,
        }
    }
}

#[derive(Debug, Parser)]
#[command(
    name = "walshadow-stream",
    about = "Stream + filter physical WAL from source PG."
)]
struct Args {
    /// Source connection as one URL, eg
    /// `postgres://user:password@host:5432/dbname?sslmode=require`. Wins
    /// over the discrete `--host` / `--port` / … flags, loses to
    /// `[source]` in `--ch-config`, same as they do
    #[arg(long, env = walshadow::init::SOURCE_URL_ENV)]
    source_url: Option<String>,
    /// Destination as one URL, eg
    /// `clickhouse://user:password@host:9000/database`. Supplies `[ch]`
    /// when no config file does, which is what turns the emitter on
    #[arg(long, env = walshadow::init::CH_URL_ENV)]
    ch_url: Option<String>,
    /// `[source]` / `[ch]` decoded from the two URL flags once at startup,
    /// then merged over the discrete flags by [`cli_base`]
    #[arg(skip)]
    url_base: toml::Table,
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
    /// slot). Unset defers to config, which reloads live; set pins the name
    /// for this process. Unset in both = slotless.
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
    /// Unix socket of the pgext bridge worker. On a daemon-owned shadow
    /// this also writes `shared_preload_libraries` and the
    /// `walshadow.*` GUCs into shadow's conf, so the worker starts.
    /// Defaults to `<shadow-socket-dir>/walshadow-bridge.sock`.
    #[arg(long)]
    bridge_socket: Option<PathBuf>,
    /// Directory holding `walshadow.so` when it isn't in PG's `$libdir`,
    /// ie a build tree instead of `make install`. Written as
    /// `dynamic_library_path`.
    #[arg(long)]
    bridge_lib_dir: Option<PathBuf>,
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
    /// processing records. Must be positive; no attachment within it fails
    /// startup. Catalog-boundary holds require a live wire: whole archive
    /// segments can't stop publication at a mid-segment commit, so
    /// archive-only operation (the old `0` escape hatch) is rejected.
    /// `ShadowStreamSink` also drops bytes pushed before a connection
    /// registers; a pump racing past shadow's `START_REPLICATION` LSN
    /// leaves an apply LSN that never advances.
    #[arg(long, default_value_t = 60)]
    walsender_connect_timeout: u64,
    /// Seconds a catalog-boundary publication hold may wait for shadow to
    /// replay through a catalog-mutating commit before failing the daemon.
    /// Keep well under source's `wal_sender_timeout` (default 60s): the
    /// pump answers no source keepalives while parked.
    #[arg(long, default_value_t = 30)]
    catalog_hold_timeout: u64,
    /// Soft cap on in-flight records for the `QueueingRecordSink` feeding
    /// the decoder / xact-drain worker. Past this watermark the pump
    /// yields to let the worker drain; a stuck worker still surfaces via
    /// the catalog `wait_for_replay` timeout on the err slot.
    /// Overrides `[ch] decoder_queue_capacity`.
    #[arg(long)]
    decoder_queue_capacity: Option<usize>,
    /// Pump-side batch size for the `QueueingRecordSink`. Bigger
    /// amortises per-send overhead but adds pump→worker latency (worker's
    /// `wait_for_replay` lags one batch behind).
    /// Overrides `[ch] decoder_batch_size`.
    #[arg(long)]
    decoder_batch_size: Option<usize>,
    /// Decode-pool size (M): parallel decode workers (detoast, type
    /// coercion, oracle resolution). Only with `--ch-config`. `1` keeps
    /// decode serial so per-table WAL order is preserved; M>1 relaxes
    /// per-table order, relying on `_lsn` ReplacingMergeTree dedup.
    #[arg(long)]
    decoder_pool_size: Option<usize>,
    /// Insert-pool size (N): concurrent ClickHouse INSERT connections.
    /// Cloud throughput is RTT/part-commit bound, so N>1 is the main
    /// throughput lever. Only with `--ch-config`.
    #[arg(long)]
    inserter_pool_size: Option<usize>,
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
    drop_table_strategy: Option<DropTableStrategy>,
    /// HTTP/Prometheus metrics bind address. Disabled when absent.
    #[arg(long)]
    metrics_bind: Option<SocketAddr>,
    /// Control socket path, omit to disable control API
    #[arg(long)]
    control_socket: Option<PathBuf>,
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
    /// `object_store` reads wal-g-format backup from `[backup]` in
    /// `--ch-config`. Initialized data dir resumes without bootstrap
    /// regardless of mode. Unset falls through to `[bootstrap] mode` in
    /// `--ch-config`, then to `off`
    #[arg(long)]
    bootstrap_mode: Option<BootstrapMode>,
    /// Shadow PG data dir. When set, daemon bootstraps or resumes shadow,
    /// writes config, starts and supervises postmaster, then stops it on
    /// exit. When unset, manage shadow externally. Required when
    /// `--bootstrap-mode != off`
    #[arg(long)]
    bootstrap_shadow_data_dir: Option<PathBuf>,
    /// Object-store backup name. `LATEST` resolves to newest sentinel;
    /// otherwise the literal `base_TTTTTTTTLLLLLLLLSSSSSSSS` form. Unset
    /// falls through to `[bootstrap] backup_name`, then to `LATEST`.
    #[arg(long)]
    bootstrap_backup_name: Option<String>,
    /// Object-store fan-out parallelism. Raise for high-bandwidth buckets.
    /// Unset falls through to `[bootstrap] object_store_parallelism`, then
    /// to `ObjectStoreSource`'s own `min(4, num_cpus)` default.
    #[arg(long)]
    bootstrap_object_store_parallelism: Option<NonZeroUsize>,
    /// BASE_BACKUP fast-checkpoint flag for `direct` mode. `true` avoids
    /// waiting for source's checkpoint_timeout; flip off if checkpoint
    /// cost matters more than bootstrap latency.
    #[arg(long, default_value_t = true)]
    bootstrap_fast_checkpoint: bool,
    /// Fetch the bootstrap WAL window from the `[backup]` bucket instead of
    /// inside `base.tar` (`direct` mode only). Source then needn't retain or
    /// re-ship `[start_lsn, end_lsn]`, which is what fills its disk at high
    /// write rates. Requires `[backup]` in `--ch-config` and source
    /// archiving to that same bucket.
    #[arg(long, default_value_t = false)]
    bootstrap_wal_from_archive: bool,
    /// Maximum seconds to wait for shadow replay after bootstrap
    /// Abort daemon when timeout expires
    #[arg(long, default_value_t = 300)]
    bootstrap_shadow_replay_timeout: u64,
}

impl Args {
    fn bridge_socket_path(&self) -> PathBuf {
        self.bridge_socket
            .clone()
            .unwrap_or_else(|| self.shadow_socket_dir.join("walshadow-bridge.sock"))
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<()> {
    // `ctl` client mode is detected before daemon-arg parsing so it needn't
    // supply the daemon's required args.
    let argv: Vec<String> = std::env::args().collect();
    if argv.get(1).map(String::as_str) == Some("ctl") {
        let rest = std::iter::once(format!("{} ctl", argv[0])).chain(argv.into_iter().skip(2));
        let (socket, command) = walshadow::ctl::Cli::parse_from(rest).into_parts()?;
        return run_ctl(&socket, command).await;
    }
    if argv.get(1).map(String::as_str) == Some("init") {
        let rest = std::iter::once(format!("{} init", argv[0])).chain(argv.into_iter().skip(2));
        let opts = InitArgs::parse_from(rest).into_opts();
        init_tracing(None);
        return walshadow::init::run(opts).await;
    }
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

async fn run_ctl(socket: &Path, cmd: walshadow::ctl::Command) -> Result<()> {
    use std::io::{IsTerminal, Read};

    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let body = if cmd.reads_stdin && !std::io::stdin().is_terminal() {
        let mut raw = String::new();
        std::io::stdin().read_to_string(&mut raw)?;
        raw.parse().context("parse config body as TOML")?
    } else {
        cmd.body
    };
    let doc = walshadow::control::encode_request(&cmd.verb, body)?;
    let mut stream = tokio::net::UnixStream::connect(socket)
        .await
        .with_context(|| format!("connect control socket {}", socket.display()))?;
    stream.write_all(doc.as_bytes()).await?;
    stream.flush().await?;
    stream.shutdown().await.ok();
    let mut resp = String::new();
    stream.read_to_string(&mut resp).await?;
    let (head, payload) = resp.split_once('\n').unwrap_or((resp.as_str(), ""));
    let Some(trailer) = head.strip_prefix("OK") else {
        eprint!("{resp}");
        std::process::exit(1);
    };
    if !trailer.trim().is_empty() {
        println!("{}", trailer.trim());
    }
    let rendered = walshadow::ctl::render(&cmd.verb, payload);
    if !rendered.trim().is_empty() {
        println!("{}", rendered.trim_end());
    }
    Ok(())
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

/// `[source]` + `[ch]` defaults from the CLI args — the base layer under the
/// config file for connection resolution, shared by the session and the
/// control surface. A `--source-url` / `--ch-url` merges over the discrete
/// flags, so the URL wins wherever both name a field.
fn cli_base(args: &Args) -> toml::Table {
    let mut s = toml::Table::new();
    s.insert("host".into(), args.host.clone().into());
    s.insert("port".into(), (args.port as i64).into());
    s.insert("user".into(), args.user.clone().into());
    s.insert("dbname".into(), args.dbname.clone().into());
    if let Some(p) = &args.password {
        s.insert("password".into(), p.clone().into());
    }
    s.insert("sslmode".into(), args.sslmode.clone().into());
    let mut root = toml::Table::new();
    root.insert("source".into(), toml::Value::Table(s));
    walshadow::ch_emitter::merge_tables(&mut root, args.url_base.clone());
    root
}

/// Decode `--source-url` / `--ch-url` once, so every later `cli_base` is a
/// pure merge and a malformed URL fails at startup rather than mid-reload
fn url_base(args: &Args) -> Result<toml::Table> {
    let mut root = toml::Table::new();
    // An env var exported empty reads as unset, so a compose file may pass
    // the name through unconditionally
    let nonempty = |u: &&String| !u.trim().is_empty();
    if let Some(url) = args.source_url.as_ref().filter(nonempty) {
        root.insert(
            "source".into(),
            toml::Value::Table(walshadow::dsn::source_table(url)?),
        );
    }
    if let Some(url) = args.ch_url.as_ref().filter(nonempty) {
        root.insert(
            "ch".into(),
            toml::Value::Table(walshadow::dsn::ch_table(url)?),
        );
    }
    Ok(root)
}

fn spawn_sighup_reload(
    mut sig: tokio::signal::unix::Signal,
    reloader: Arc<walshadow::control::Reloader>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        while sig.recv().await.is_some() {
            tracing::info!(target: "walshadow", "SIGHUP — live reload");
            if let Err(e) = reloader.reload().await {
                tracing::warn!(target: "walshadow", error = %format!("{e:#}"), "reload failed");
            }
        }
    })
}

/// Enforce capability, not flag value: catalog-boundary holds need an
/// active walreceiver, so archive-only operation is not startable.
fn validate_transport_args(args: &Args) -> Result<()> {
    anyhow::ensure!(
        args.walsender_connect_timeout > 0,
        "--walsender-connect-timeout 0 (archive-only shadow) is unsupported: \
         catalog-boundary publication holds require an attached walreceiver",
    );
    anyhow::ensure!(
        args.catalog_hold_timeout > 0,
        "--catalog-hold-timeout must be positive",
    );
    Ok(())
}

/// Process-lifetime entry: bind metrics + control socket + SIGHUP, then stream
/// one session. Every reconfigure (socket / SIGHUP) is a live reload — no
/// restart. Ctrl-C breaks the pump loop and drains gracefully.
async fn run(mut args: Args) -> Result<()> {
    use walshadow::control::{Reloader, SharedCtx};

    args.url_base = url_base(&args)?;
    validate_transport_args(&args)?;
    let sighup = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::hangup())
        .inspect_err(|e| {
            tracing::warn!(
                target: "walshadow::sighup",
                error = %e,
                "SIGHUP install failed",
            );
        })?;
    // Match systemd SIGTERM with ctrl_c shutdown path
    let sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .inspect_err(|e| {
            tracing::warn!(
                target: "walshadow",
                error = %e,
                "SIGTERM install failed",
            );
        })?;

    let metrics = MetricsRegistry::new();
    let reloader = Arc::new(Reloader::default());

    let _metrics_server = if let Some(addr) = args.metrics_bind {
        let (bound, h) = walshadow::metrics::serve(addr, metrics.clone())
            .await
            .context("bind metrics endpoint")?;
        tracing::info!(target: "walshadow::metrics", addr = %bound, "metrics endpoint serving");
        Some(h)
    } else {
        None
    };

    let _control_server = if let Some(sock) = args.control_socket.clone() {
        let ch_config = args
            .ch_config
            .clone()
            .context("--control-socket requires --ch-config")?;
        let ctx = SharedCtx {
            ch_config,
            cli_base: cli_base(&args),
            metrics: metrics.clone(),
            reloader: reloader.clone(),
            frag_lock: Arc::new(Mutex::new(())),
        };
        Some(
            walshadow::control::serve(sock, ctx)
                .await
                .context("bind control socket")?,
        )
    } else {
        None
    };
    let _sighup = spawn_sighup_reload(sighup, reloader.clone());

    run_session(&args, &metrics, &reloader, sigterm).await
}

async fn run_session(
    args: &Args,
    metrics: &MetricsRegistry,
    reloader: &Arc<walshadow::control::Reloader>,
    mut sigterm: tokio::signal::unix::Signal,
) -> Result<()> {
    // Clone the Arc-backed registry so the body's `&metrics` uses are unchanged.
    let metrics = metrics.clone();

    let merged: toml::Table = match args.ch_config.as_deref() {
        Some(p) => walshadow::ch_emitter::load_effective(p, cli_base(args))
            .await
            .with_context(|| format!("load config {}", p.display()))?,
        None => cli_base(args),
    };
    // Applied source endpoint. Boot resolves it file-over-CLI; a later reload
    // republishes it on the config watch and the pump swaps its feed.
    let mut source_conn =
        SourceConn::from_table(&merged).map_err(|e| anyhow::anyhow!("[source] {e}"))?;
    if args.slot.is_some() {
        source_conn.slot = args.slot.clone();
    }
    let mut cfg = source_conn.to_pg_config();
    let mut feed = connect_source_waiting(args, &mut source_conn, &mut cfg).await;

    let ident = feed.identify_system().await.context("IDENTIFY_SYSTEM")?;
    tracing::info!(
        target: "walshadow",
        sysid = %ident.sysid,
        timeline = ident.timeline,
        xlogpos = format_pg_lsn(ident.xlogpos).to_string(),
        "source identified",
    );

    // `[ch]` presence decides emitter vs metrics-only.
    let ch_config = if merged.contains_key("ch") {
        let mut cfg = EmitterConfig::from_table(&merged).context("parse ch config")?;
        if let Some(ms) = args.ch_flush_timeout_ms {
            cfg.flush_timeout = std::time::Duration::from_millis(ms);
        }
        // CLI override wins over TOML `[source] slot` (CLI > config).
        if args.slot.is_some() {
            cfg.source.slot = args.slot.clone();
        }
        cfg.decoder_pool_size = positive_usize(
            "decoder_pool_size",
            args.decoder_pool_size,
            cfg.decoder_pool_size,
        );
        cfg.inserter_pool_size = positive_usize(
            "inserter_pool_size",
            args.inserter_pool_size,
            cfg.inserter_pool_size,
        );
        Some(cfg)
    } else {
        None
    };
    // Before anything dials CH naming that database in its handshake — the
    // bootstrap insert tail is first, and its failure there reads as a
    // bootstrap fault rather than a missing destination
    if let Some(cfg) = ch_config.as_ref() {
        walshadow::ch_ddl::ensure_boot_database(cfg)
            .await
            .with_context(|| format!("reach ClickHouse {}:{}", cfg.host, cfg.port))?;
    }
    // QueueingRecordSink knobs feed both the CH and metrics-only pipelines,
    // so resolve here while `ch_config` is still in scope (it is consumed
    // into `emitter_cfg` below). CLI over `[ch]` over the built-in default.
    let decoder_batch_size = positive_usize(
        "decoder_batch_size",
        args.decoder_batch_size,
        ch_config
            .as_ref()
            .map_or(DEFAULT_QUEUEING_BATCH_SIZE, |c| c.decoder_batch_size),
    );
    let decoder_queue_capacity = positive_usize(
        "decoder_queue_capacity",
        args.decoder_queue_capacity,
        ch_config
            .as_ref()
            .map_or(DEFAULT_QUEUEING_RECORD_SINK_CAPACITY, |c| {
                c.decoder_queue_capacity
            }),
    );
    let bootstrap_plan = resolve_bootstrap(args, ch_config.as_ref())?;
    let shadow_start = resolve_shadow_start(args, bootstrap_plan.mode)?;
    // Slot before bootstrap
    if let Some(slot) = source_conn.slot.as_deref() {
        feed.ensure_physical_slot(slot)
            .await
            .with_context(|| format!("ensure physical replication slot {slot}"))?;
        tracing::info!(target: "walshadow", slot, "physical replication slot ready");
    }
    let bootstrap_end_lsn: Option<u64> = if matches!(shadow_start, ShadowStart::Bootstrap(_)) {
        if !args.skip_preflight {
            let source_sql = feed
                .sql_client()
                .await
                .context("source sidecar sql for bootstrap pre-flight")?;
            walshadow::preflight::bootstrap(walshadow::preflight::BootstrapInputs {
                source_sql,
                wal_from_archive: args.bootstrap_wal_from_archive,
            })
            .await
            .context("bootstrap pre-flight probe")?
            .into_result()
            .context("pre-flight rejected bootstrap")?;
        }
        Some(
            run_bootstrap(&cfg, &mut feed, args, &bootstrap_plan, ch_config.clone())
                .await
                .context("bootstrap")?,
        )
    } else {
        None
    };
    // Regenerate config because shadow's port, socket, and GUC floor may change
    // Keep shadow alive until pipeline teardown finishes
    let shadow_lifecycle: Option<ShadowLifecycle> = match &shadow_start {
        ShadowStart::External => None,
        ShadowStart::Bootstrap(dir) | ShadowStart::Resume(dir) => {
            let shadow = Arc::new(build_owned_shadow(args, dir.clone()));
            shadow
                .write_standby_signal()
                .context("write standby.signal")?;
            start_owned_shadow(
                &shadow,
                bootstrap_end_lsn,
                Duration::from_secs(args.bootstrap_shadow_replay_timeout),
            )
            .await?;
            Some(ShadowLifecycle::spawn(
                shadow,
                walsender_primary_conninfo(args.walsender_bind),
            ))
        }
    };
    let backup_settings = ch_config.as_ref().and_then(|c| c.backup.clone());
    let start_lsn_override: Option<Pos<Floor>> = args
        .start_lsn
        .as_deref()
        .map(|s| walshadow::pg::parse_pg_lsn(s).context("--start-lsn"))
        .transpose()?
        .map(Pos::new);

    let live_identity = manifest::SourceIdentity {
        system_id: ident.sysid.parse().context("IDENTIFY_SYSTEM sysid")?,
        timeline: ident.timeline,
        timeline_begin: Pos::ZERO,
    };
    // Identity gate runs before `--ignore-cursor`: the flag discards resume
    // LSNs, not artifact ownership. Foreign system_id is fatal regardless
    // (retire/backfill ledgers would act on another cluster's state). A newer
    // live timeline is a promotion, proved against the source's history below.
    let manifest_at_boot: Option<manifest::Manifest> =
        match manifest::load(&args.spill_dir, &live_identity).await {
            Ok(m) => m,
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
        bootstrap_end_lsn.map(Pos::new),
        manifest_at_boot.as_ref().map(|m| m.lsn.emitter_ack),
        ident.xlogpos,
    );
    let pinned = bootstrap_end_lsn.is_some() || start_lsn_override.is_some();
    let floor_at_boot = manifest_at_boot
        .as_ref()
        .map(|m| m.floor)
        .filter(|f| !f.is_zero());
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
        raw = %raw_start,
        aligned = %aligned,
        from_bootstrap = bootstrap_end_lsn.is_some() && args.start_lsn.is_none(),
        from_floor = floor_at_boot.is_some() && !pinned,
        "start LSN",
    );

    // Branch selection is per segment, through the source's history: a floor
    // stored on an ancestor is served by that ancestor, whatever the live head
    // reports, and a floor at a fork segment's start is served by the descendant
    // whose file holds the ancestor prefix (plans/failover.md §Lineage).
    let stored_timeline = manifest_at_boot
        .as_ref()
        .map(|m| m.source.timeline)
        .unwrap_or(ident.timeline);
    let mut history = load_boot_history(&mut feed, ident.timeline, stored_timeline).await?;
    let start_timeline = match history.resume_branch(stored_timeline, aligned.get(), WAL_SEG_SIZE) {
        Some(tli) => tli,
        None if args.ignore_cursor => {
            let found = history.tli_of_segment(aligned.get(), WAL_SEG_SIZE);
            tracing::warn!(
                target: "walshadow",
                stored_timeline,
                live_timeline = ident.timeline,
                serves_start = found,
                "--ignore-cursor adopts the live timeline without a lineage proof",
            );
            history = TimelineHistory::root(ident.timeline);
            ident.timeline
        }
        None => anyhow::bail!(
            "timeline_not_descendant: stored timeline {stored_timeline} does not reach \
             {} on live timeline {}'s history (it serves {:?}); \
             --ignore-cursor re-baselines onto the live branch",
            aligned,
            ident.timeline,
            history.tli_of_segment(aligned.get(), WAL_SEG_SIZE),
        ),
    };
    // Same number, different branch: the chain places a sibling exactly where it
    // places a descendant, and only the switchpoint separates them. A stored
    // begin is the chain a previous run proved, carried forward
    // (plans/failover.md §Lineage)
    let stored_begin = manifest_at_boot
        .as_ref()
        .map(|m| m.source.timeline_begin.get())
        .unwrap_or(0);
    let live_begin = history.begin_of(stored_timeline).unwrap_or(0);
    match stored_begin {
        0 if stored_timeline > 1 => tracing::warn!(
            target: "walshadow",
            stored_timeline,
            live_begin = %format_pg_lsn(live_begin),
            "manifest records no switchpoint for its branch, so a sibling sharing \
             that number cannot be refused until the next manifest write",
        ),
        0 => {}
        begin if begin != live_begin && !args.ignore_cursor => anyhow::bail!(
            "sibling_branch: source places timeline {stored_timeline} at {}, \
             walshadow's artifacts came off it from {}; the branch behind them is \
             absent from this source's history",
            format_pg_lsn(live_begin),
            format_pg_lsn(begin),
        ),
        begin if begin != live_begin => tracing::warn!(
            target: "walshadow",
            stored_timeline,
            stored_begin = %format_pg_lsn(begin),
            live_begin = %format_pg_lsn(live_begin),
            "--ignore-cursor adopts a branch that begins somewhere else",
        ),
        _ => {}
    }
    if start_timeline != ident.timeline {
        tracing::info!(
            target: "walshadow",
            start_timeline,
            live_timeline = ident.timeline,
            switch_lsn = history
                .switchpoint_of(start_timeline)
                .map(|l| format_pg_lsn(l).to_string()),
            "resuming on an ancestor timeline; the crossing follows its fork",
        );
    }
    // Branches a spill-dir artifact may carry: the resume branch plus every
    // ancestor the chain places below it. A crossing moves the resume branch
    // while the artifacts stay where they were written
    let lineage: Vec<u32> = history
        .entries()
        .iter()
        .map(|e| e.tli)
        .filter(|tli| *tli <= start_timeline)
        .collect();

    let mut stream = WalStream::new(start_timeline, WAL_SEG_SIZE, aligned)?;
    // Shadow must attach to this listener before catalog replay can advance
    let mut shadow_boot = walshadow::shadow_stream::ShadowStreamState::new(
        history.shadow_boot_branch(stored_timeline, aligned.get(), start_timeline),
        ident.sysid.clone(),
        aligned.get(),
        args.walsender_slow_threshold,
    );
    seed_shadow_branches(
        &mut shadow_boot,
        &mut feed,
        &history,
        &args.out_dir,
        start_timeline,
    )
    .await?;
    let shadow_state = Arc::new(Mutex::new(shadow_boot));
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
    // Set address after bind so first connection succeeds
    // Supervisor restarts a shadow that is down, with the address in its conf
    if let (Some(lifecycle), Some(conninfo)) = (
        &shadow_lifecycle,
        walsender_primary_conninfo(args.walsender_bind),
    ) {
        probe_blocking(&lifecycle.shadow, move |s| s.point_at_walsender(&conninfo)).await;
    }

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
        let observed_from = stream
            .filter_mut()
            .seed_observed_from_source(sql_client)
            .await
            .context("seed observed-from xid")?;
        tracing::info!(
            target: "walshadow",
            observed_from,
            "transactions from this xid on are observed whole",
        );
        tracing::info!(
            target: "walshadow",
            added,
            "seeded catalog filenodes from source pg_class"
        );
    }

    // Connect bridge and shadow catalog before START_REPLICATION so the
    // tracker→drain wire is hot from the first record.
    let shadow_conninfo = socket_conninfo(
        args.shadow_socket_dir
            .to_str()
            .context("shadow-socket-dir not UTF-8")?,
        args.shadow_port,
        &args.shadow_user,
        &args.shadow_dbname,
    );
    let connect_budget = Duration::from_secs(args.shadow_connect_timeout);
    let bridge_path = args.bridge_socket_path();
    let bridge = Arc::new(
        walshadow::bridge::connect_with_budget(&bridge_path, connect_budget)
            .await
            .with_context(|| format!("connect bridge at {}", bridge_path.display()))?,
    );
    let info = bridge.info();
    tracing::info!(
        target: "walshadow::bridge",
        socket = %bridge_path.display(),
        pg_version = info.map(|i| i.pg_version_num).unwrap_or(0),
        in_recovery = info.map(|i| i.in_recovery).unwrap_or(false),
        "bridge connected",
    );
    let cat_cfg = ShadowCatalogConfig::default();
    let backoff_initial = cat_cfg.reconnect_backoff_initial;
    let backoff_max = cat_cfg.reconnect_backoff_max;
    let catalog = with_transient_retry(connect_budget, backoff_initial, backoff_max, async || {
        ShadowCatalog::connect(&shadow_conninfo, cat_cfg.clone(), bridge.clone()).await
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
            slot: source_conn.slot.as_deref(),
            ch_config: ch_config.as_ref(),
        })
        .await
        .context("pre-flight probe")?;
        report
            .into_result()
            .context("pre-flight rejected daemon start")?;
        tracing::info!(target: "walshadow::preflight", "pre-flight passed");
    }

    let oracle = Some(Arc::new(walshadow::oracle::Oracle::new(bridge.clone())));

    // START_REPLICATION runs after sinks are built so archive fallback can
    // advance identical filter and decode paths.
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

    // Persist the bootstrap end LSN as the initial resume manifest so a restart
    // in the window before the first manifest write resumes from `end_lsn` (like
    // a standby reading its backup's redo point) instead of falling to greenfield
    // head and skipping `(end_lsn, head]`. Only after a fresh bootstrap.
    if let Some(end_lsn) = bootstrap_end_lsn {
        let initial = manifest::Manifest {
            version: manifest::MANIFEST_VERSION,
            floor: manifest::resolved_floor(end_lsn, end_lsn),
            source: live_identity.clone(),
            wal: manifest::WalBranch {
                stream_timeline: start_timeline,
            },
            lsn: manifest::LsnSet {
                source_received: end_lsn.into(),
                filter_durable: end_lsn.into(),
                shadow_replay: end_lsn.into(),
                drain: end_lsn.into(),
                emitter_ack: end_lsn.into(),
                shadow_flush: end_lsn.into(),
            },
        };
        manifest::write(&args.spill_dir, &initial)
            .await
            .context("write initial resume manifest after bootstrap")?;
    }

    // Descriptor log: durable shape history captured at catalog boundaries,
    // bound to this source + shadow pairing. Sole schema-event source.
    let source_major = (feed.server_version_num() / 10000) as u32;
    anyhow::ensure!(
        (16..=18).contains(&source_major),
        "source PG major {source_major} unsupported (commit-record sinval layout audited for 16-18)",
    );
    let shadow_db_oid = catalog
        .lock()
        .await
        .current_database_oid()
        .await
        .context("shadow database oid")?;
    stream.filter_mut().set_target_db(shadow_db_oid);
    let pending_cfg = ch_config
        .as_ref()
        .map(|c| c.pending_capture)
        .unwrap_or_default();
    let pending_catalog = Arc::new(walshadow::pending::PendingCatalog::default());
    let smgr_markers = stream.filter_mut().smgr_markers();
    // A resumed manifest implies prior progress whose records the log must
    // cover; an empty/missing log there means it was lost — decode would
    // read uncovered intervals. `--ignore-cursor` discards both.
    let log_files_present = args.spill_dir.join(walshadow::desc_log::TAIL_FILE).exists()
        || args.spill_dir.join(walshadow::desc_log::CKPT_FILE).exists();
    anyhow::ensure!(
        manifest_at_boot.is_none() || log_files_present || args.ignore_cursor,
        "manifest present but descriptor log missing in {}; \
         re-bootstrap or pass --ignore-cursor",
        args.spill_dir.display(),
    );
    if args.ignore_cursor {
        for f in [
            walshadow::desc_log::CKPT_FILE,
            walshadow::desc_log::TAIL_FILE,
        ] {
            let _ = tokio::fs::remove_file(args.spill_dir.join(f)).await;
        }
    }
    let desc_log = Arc::new(
        walshadow::desc_log::DescriptorLog::open_on_branch(
            &args.spill_dir,
            walshadow::desc_log::DescLogIdentity {
                pg_major: source_major,
                system_id: ident.sysid.clone(),
                // Resume branch, which a crossing moves without moving the log:
                // the stored header names wherever the log last rewrote itself,
                // so `lineage` is what places it
                timeline: start_timeline,
                db_oid: shadow_db_oid,
                wal_seg_size: WAL_SEG_SIZE as u32,
            },
            &lineage,
        )
        .await
        .context("open descriptor log")?,
    );
    if let Some(lsn) = start_lsn_override {
        anyhow::ensure!(
            lsn >= desc_log.floor_at_write(),
            "--start-lsn {} below descriptor log floor {}; no shape history \
             survives there — --ignore-cursor or re-bootstrap",
            lsn,
            desc_log.floor_at_write(),
        );
        let head = desc_log.head();
        anyhow::ensure!(
            head == 0 || lsn.get() <= head,
            "--start-lsn {} beyond descriptor log head {}; boundaries in \
             between were never captured — --ignore-cursor re-baselines",
            lsn,
            format_pg_lsn(head),
        );
    }
    if desc_log.is_empty() {
        // Baseline snapshot: every eligible rel as of shadow's position,
        // valid from the aligned start so the prefix re-read decodes
        // (newest-shape reader of older tuples — the safe bias direction).
        // Boundaries at or below covered_through are baked in and skip.
        let (replay_lsn, descs) = catalog
            .lock()
            .await
            .fetch_all_descriptors()
            .await
            .context("descriptor log boot seed")?;
        let covered_through = raw_start.get().max(replay_lsn);
        let entries = descs
            .into_iter()
            .map(|d| {
                Arc::new(walshadow::desc_log::LogEntry {
                    valid_from: aligned.get(),
                    oid: d.oid,
                    rfn: d.rfn,
                    value: walshadow::desc_log::LogValue::Present(Arc::new(d)),
                })
            })
            .collect();
        desc_log
            .seed(
                walshadow::desc_log::BatchRecord {
                    captured_at: covered_through,
                    commit_lsn: 0,
                    observations: Vec::new(),
                    ambiguities: Vec::new(),
                    entries,
                },
                covered_through,
            )
            .await
            .context("seed descriptor log")?;
        tracing::info!(
            target: "walshadow::desc_log",
            covered_through = format_pg_lsn(covered_through).to_string(),
            "descriptor log seeded",
        );
    }

    // Txn-span registry, shared by pump + decoder; `Some` only with OTLP on.
    let span_registry =
        if args.otlp_endpoint.is_some() || std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT").is_ok() {
            Some(xact_buffer.lock().await.span_registry())
        } else {
            None
        };
    let mut decoder = BufferingDecoderSink::new(desc_log.clone(), xact_buffer.clone());
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
    // Seed at resume point so first status write cannot replace persisted ack
    // with zero before WAL re-read catches up
    let emitter_ack = Arc::new(Monotone::<EmitterAck>::new(raw_start.retag()));
    // Persisted resolved floor. Seed with the resolved start: aligned +
    // archive-clamped, the exact position a crash-now restart replays from.
    // Any Dropped queued during the boot re-read of [aligned, raw_start] has
    // commit_lsn ≥ aligned, so its retire holds until a later manifest write
    // moves the floor past it.
    let resume_floor = Arc::new(Monotone::<Floor>::new(aligned));
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
        // Live routing map shared by DDL applicator + route planning. The
        // refresher below rewrites it on every republished snapshot.
        let mapping = walshadow::mapping::mapping_handle(emitter_cfg.tables.clone());
        // Resolver merges CLI over TOML and publishes ResolvedConfig on
        // the watch substrate; SIGHUP re-reads TOML and republishes. The
        // mapping refresher + DDL applicator subscribe.
        let cli_overrides = CliOverrides {
            drop_table_strategy: args.drop_table_strategy,
            flush_timeout: args
                .ch_flush_timeout_ms
                .map(std::time::Duration::from_millis),
            source_slot: args.slot.clone(),
        };
        let (resolver, config_rx) = ConfigResolver::new(
            &emitter_cfg,
            cli_overrides,
            args.ch_config.clone(),
            cli_base(args),
            mapping.clone(),
        );
        reloader.set_resolver(Some(resolver.clone())).await;
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
            emitter_cfg.system_columns.clone(),
            emitter_cfg.replicate_all,
            emitter_cfg.runtime_config_schema.clone(),
        );
        let mut applicator = walshadow::ch_ddl::DdlApplicator::new(
            &emitter_cfg,
            ddl_cfg,
            mapping.clone(),
            config_rx.clone(),
        )
        .await
        .context("init DDL applicator")?
        .with_resolver(resolver.clone());
        let stats = Arc::new(EmitterStats::default());
        emitter_stats_handle = Some(stats.clone());
        // Backfiller for `initial_load` opt-ins (COPY / backup-sourced):
        // own source session + CH tail per backfill or pass, spill-dir
        // ledger dedups restarts. Wired whenever the emitter runs, since an
        // opt-in arriving later over the control socket or the overlay would
        // otherwise silently skip its backfill; idle it costs one ledger read.
        // One validated resident-payload pool for the pipeline and every
        // concurrent backup pass
        let pipeline_budget =
            walshadow::pipeline::build_budget(&emitter_cfg, emitter_cfg.decoder_pool_size)
                .map_err(|e| anyhow::anyhow!("memory budget: {e}"))?;
        copy_backfiller = Some(Arc::new(
            walshadow::copy_backfill::CopyBackfiller::new(
                cfg.clone(),
                emitter_cfg.clone(),
                mapping.clone(),
                stats.clone(),
                catalog.clone(),
                desc_log.clone(),
                &args.spill_dir,
                Some(config_rx.clone()),
                Some(pipeline_budget.clone()),
                oracle.clone(),
            )
            .await,
        ));
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
            if row.replicate.is_some() && !row.is_pattern() {
                walshadow::opt_in::apply_table_opt_in(
                    &resolver,
                    &mut applicator,
                    &catalog,
                    backfiller_effects.as_ref(),
                    rel,
                    row,
                    raw_start.get(),
                )
                .await
                .with_context(|| format!("seed opt-in for {rel}"))?;
            }
        }
        for (rel, row) in &emitter_cfg.table_opt_ins {
            if row.replicate.is_some() {
                walshadow::opt_in::apply_table_opt_in(
                    &resolver,
                    &mut applicator,
                    &catalog,
                    backfiller_effects.as_ref(),
                    rel,
                    row,
                    raw_start.get(),
                )
                .await
                .with_context(|| format!("config opt-in for {rel}"))?;
            }
        }
        let pattern_scoped: Vec<(RelName, walshadow::runtime_config::TableRow)> = {
            let snap = config_rx.borrow();
            let config_schema = emitter_cfg.runtime_config_schema.as_deref();
            snap.rules.pattern_scoped(
                || desc_log.user_rel_names_at(raw_start.get(), config_schema),
                |rel| snap.tables.contains_key(rel),
            )
        };
        for (rel, row) in &pattern_scoped {
            walshadow::opt_in::apply_table_opt_in(
                &resolver,
                &mut applicator,
                &catalog,
                backfiller_effects.as_ref(),
                rel,
                row,
                raw_start.get(),
            )
            .await
            .with_context(|| format!("pattern opt-in for {rel}"))?;
        }
        let sql_scoped_tables: HashSet<RelName> = seeded_table_rows
            .iter()
            .filter(|(_, row)| row.replicate.is_some() && !row.is_pattern())
            .chain(pattern_scoped.iter())
            .map(|(rel, _)| rel.clone())
            .collect();
        let active_tables: HashSet<RelName> = config_rx.borrow().tables.keys().cloned().collect();
        apply_toml_initial_loads(
            &catalog,
            copy_backfiller.as_ref(),
            &emitter_cfg.table_initial_loads,
            &active_tables,
            &sql_scoped_tables,
            raw_start.get(),
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
                .and_then(|mode| mode.parse::<InitialLoadMode>().ok())
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
        let (decoders, inserters) = (
            emitter_cfg.decoder_pool_size,
            emitter_cfg.inserter_pool_size,
        );
        tracing::info!(
            target: "walshadow::pipeline",
            addr = %addr,
            decoders,
            inserters,
            "parallel decode+insert pipeline starting",
        );
        PipelineConfig {
            emitter: emitter_cfg,
            decoder_pool_size: decoders,
            inserter_pool_size: inserters,
            catalog: catalog.clone(),
            mapping,
            oracle: oracle.clone(),
            applicator: Some(applicator),
            tail: TailKind::ClickHouse,
            buffer: xact_buffer.clone(),
            subxact_tracker: Arc::new(Mutex::new(SubxactTracker::new())),
            log: desc_log.clone(),
            pending: pending_catalog.clone(),
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
        // No `[ch]` here, so the CLI layers straight onto the constants.
        let decoders = positive_usize(
            "decoder_pool_size",
            args.decoder_pool_size,
            DEFAULT_DECODER_POOL,
        );
        let inserters = positive_usize(
            "inserter_pool_size",
            args.inserter_pool_size,
            DEFAULT_INSERTER_POOL,
        );
        tracing::info!(
            target: "walshadow::pipeline",
            decoders,
            "metrics-only pipeline (null tail) starting",
        );
        PipelineConfig {
            emitter: EmitterConfig::default(),
            decoder_pool_size: decoders,
            inserter_pool_size: inserters,
            catalog: catalog.clone(),
            mapping: walshadow::mapping::mapping_handle(Default::default()),
            oracle: None,
            applicator: None,
            tail: TailKind::Null,
            buffer: xact_buffer.clone(),
            subxact_tracker: Arc::new(Mutex::new(SubxactTracker::new())),
            log: desc_log.clone(),
            pending: pending_catalog.clone(),
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
    let ack_probe = pipeline_handle.ack_probe.clone();
    reorder_sink
        .flush_due_retires()
        .await
        .context("boot flush of due toast-mirror retires")?;
    reorder_sink
        .apply_boot_events(desc_log.active_present_at(raw_start.get()), raw_start.get())
        .await
        .context("boot Added pass over descriptor log")?;
    let decoder_xact = QueueingRecordSink::spawn(
        DecoderXactPair {
            decoder,
            xact_drain: reorder_sink,
        },
        decoder_batch_size,
        decoder_queue_capacity,
        span_registry.clone(),
    );
    let boundary_gate = CatalogBoundaryGate::new(
        shadow_state.clone(),
        BoundaryGateConfig {
            hold_timeout: Duration::from_secs(args.catalog_hold_timeout),
            ..BoundaryGateConfig::default()
        },
    );
    let boundary_hold_stats = boundary_gate.stats.clone();
    let capture = walshadow::catalog_capture::CatalogCapture::new(
        desc_log.clone(),
        catalog.clone(),
        xact_buffer.clone(),
        smgr_markers,
        pending_catalog.clone(),
        pending_cfg,
    );
    let capture_stats = capture.stats_handle();
    let decoder_xact = BoundaryHoldSink::new(decoder_xact, boundary_gate).with_capture(capture);
    let mut record_sink = DaemonSinks {
        metrics: MetricsRecordSink::default(),
        decoder_xact,
        decoder_stats: decoder_stats_handle,
        emitter_stats: emitter_stats_handle,
        span_registry,
    };
    // Segment fsync off the hot path: sink writes+renames, the task fsyncs and
    // publishes `durable_lsn`. Seed at the resume point.
    let durable_lsn = Arc::new(Monotone::<FilterDurable>::new(stream.dispatched_lsn()));
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
    // Descriptor-log GC off the pump task: the pump publishes each persisted
    // floor, the task compacts. Coalesces by construction — a watch holds
    // only the latest floor.
    let gc_fatal = walshadow::pipeline::Fatal::new();
    // Pruner's own cell, not `resume_floor`: dropping it is what tells the gc
    // task the session is done
    let gc_floor = Monotone::<Floor>::default();
    let gc_task = spawn_desc_log_gc(desc_log.clone(), gc_floor.watch(), gc_fatal.clone());
    let mut chunk_buf = Vec::with_capacity(64 * 1024);

    // Metrics endpoint + control socket + SIGHUP are process-lifetime (bound in
    // `run`); the session only writes into the shared registry.
    let metrics_resolver = config_resolver.clone();
    let metrics_backfiller = copy_backfiller.clone();
    // config_resolver stays owned here (dropped at session end → mapping
    // refresher exits); mapping/budget live-reload arrives via the WAL overlay.
    let _ = &config_resolver;

    // Retention sweeper writes shadow's `pg_last_wal_replay_lsn` here;
    // status loop reads it for the cursor's `shadow_replay_lsn` slot + the
    // standby-status `apply_lsn` ceiling.
    let shadow_replay_lsn = Arc::new(Monotone::<ShadowReplay>::default());
    // Aggregate flush across ShadowStreamSink connections, fed into the
    // cursor for shadow's `START_REPLICATION PHYSICAL` resume on restart.
    let shadow_flush_lsn = Arc::new(Monotone::<ShadowFlush>::default());

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
    // gate times out (pgbench_acceptance / kill_restart failure mode). No
    // attachment fails startup: catalog-boundary holds require a live wire,
    // and archive-only operation can't stop publication at a mid-segment
    // commit (restore_command must never observe unreleased bytes).
    {
        let timeout = Duration::from_secs(args.walsender_connect_timeout);
        let start = Instant::now();
        loop {
            let agg = shadow_state.lock().await.aggregate();
            if agg.active_connections > 0 {
                break;
            }
            // `accepted` separates "shadow never dialed" from "shadow dialed
            // and stalled in the handshake" — the latter reads as the former
            // without it, since only START_REPLICATION registers a connection
            anyhow::ensure!(
                start.elapsed() < timeout,
                "no walreceiver streaming from walsender {walsender_addr} within \
                 {}s (accepted {}, none sent START_REPLICATION); catalog-boundary \
                 holds require a live wire — point shadow's primary_conninfo here \
                 or raise --walsender-connect-timeout",
                args.walsender_connect_timeout,
                agg.accepted_total,
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        tracing::info!(
            target: "walshadow",
            wait = ?start.elapsed(),
            "walsender connected — starting pump",
        );
    }

    let source_recovery = SourceRecovery {
        status_interval: Duration::from_secs(args.status_interval),
        backup: backup_settings.as_ref(),
        spill_dir: &args.spill_dir,
        floor: &resume_floor,
    };
    if let Err(e) = feed
        .start_physical_replication(
            source_conn.slot.as_deref(),
            stream.next_lsn().get(),
            start_timeline,
        )
        .await
    {
        feed = source_recovery
            .recover(
                e,
                &cfg,
                source_conn.slot.as_deref(),
                stream_branch(&history, live_identity.system_id, &stream),
                &mut stream,
                &mut record_sink,
                &mut segment_sink,
            )
            .await
            .context("resume WAL source")?;
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
    let mut last_emitter_ack_observed = Pos::<EmitterAck>::ZERO;
    let mut inflight_stall_since: Option<Instant> = None;
    let mut inflight_stall_logged = false;
    // Pump reads `paused` and the source endpoint live off the resolver watch;
    // when paused it idles (stops consuming source WAL) without tearing
    // anything down, and a moved `[source]` swaps the feed in place.
    let pump_config_rx = config_resolver.as_ref().map(|r| r.subscribe());
    let mut source_swap_pending = false;
    let mut source_swap_retry_at: Option<Instant> = None;
    let mut source_swaps_total = 0u64;
    let mut source_swap_failures_total = 0u64;
    // Proof the last swap attempt failed, cleared once one lands
    let mut source_swap_blocked_on: &'static str = "";
    // Frozen when the pump observes a pause, so a promotion decision reads a
    // frontier that cannot move under it. Cleared on resume: a value left over
    // from an earlier pause is as misleading as a live one
    let mut pause_frontier: Option<(u64, u64)> = None;
    // A restart mid-pause re-freezes both numbers, conservatively but not
    // identically, so the pair an operator already read has to be read again
    let mut pause_refrozen = false;
    let mut ever_unpaused = false;
    // Step 5's answer, refreshed while paused off the endpoint the pump holds
    let mut promotion = PromotionGate::default();
    let mut promotion_polled_at: Option<Instant> = None;
    let switchover = Switchover {
        system_id: live_identity.system_id,
        out_dir: &args.out_dir,
        shadow_state: &shadow_state,
    };
    let mut timeline_stats = TimelineStats {
        // Off the chain, so a restart after a crossing keeps reporting the fork
        // it resumed across instead of zero
        switch_lsn: history.begin_of(start_timeline).unwrap_or(0),
        ..TimelineStats::default()
    };
    // The ancestor ended and the descendant has not been adopted yet. Survives
    // iterations so a source error mid-crossing retries the crossing: at the
    // ancestor's switchpoint an ordinary reconnect has nothing to ask for
    let mut crossing = CrossingState::default();
    let mut barrier_logged: Option<Instant> = None;
    let shutdown_reason = loop {
        let paused = pump_config_rx
            .as_ref()
            .map(|rx| rx.borrow().paused)
            .unwrap_or(false);
        // Slot changes require reconnect because START_REPLICATION binds slot
        if let Some(rx) = pump_config_rx.as_ref() {
            let desired = rx.borrow().source.clone();
            if desired != source_conn {
                tracing::info!(
                    target: "walshadow",
                    from = source_conn.endpoint(),
                    to = desired.endpoint(),
                    from_slot = source_conn.slot.as_deref(),
                    to_slot = desired.slot.as_deref(),
                    "source changed — swapping feed",
                );
                source_conn = desired;
                cfg = source_conn.to_pg_config();
                source_swap_pending = true;
                source_swap_retry_at = None;
            }
        }
        // Swap between chunks, so the resume point is the byte-contiguous
        // `next_lsn` and no WalStream state is rebuilt. Old feed stays up
        // until the new endpoint proves same cluster and branch, and until the
        // named slot answers: a wrong address or a slot the target never got
        // costs a warning, not the stream.
        //
        // Not while a crossing is pending: the stream sits at a switchpoint no
        // branch resumes from, and the crossing dials the live endpoint and slot
        // itself, so a repoint made mid-crossing lands there instead.
        if source_swap_pending
            && !crossing.pending()
            && source_swap_retry_at.is_none_or(|at| Instant::now() >= at)
        {
            match resume_source_feed(
                &cfg,
                source_conn.slot.as_deref(),
                stream.next_lsn(),
                stream_branch(&history, live_identity.system_id, &stream),
                resume_floor.get(),
                Duration::from_secs(args.status_interval),
            )
            .await
            {
                Ok(swapped) => {
                    feed = swapped;
                    source_swap_pending = false;
                    source_swap_retry_at = None;
                    source_swaps_total += 1;
                    source_swap_blocked_on = "";
                    tracing::info!(
                        target: "walshadow",
                        endpoint = source_conn.endpoint(),
                        resume_lsn = %stream.next_lsn(),
                        slot = source_conn.slot.as_deref(),
                        "source feed swapped",
                    );
                }
                Err(e) => {
                    source_swap_failures_total += 1;
                    source_swap_retry_at = Some(Instant::now() + SOURCE_SWAP_RETRY);
                    source_swap_blocked_on = swap_reason(&e);
                    timeline_stats.record_reason(source_swap_blocked_on);
                    tracing::warn!(
                        target: "walshadow",
                        error = %format!("{e:#}"),
                        reason = source_swap_blocked_on,
                        endpoint = source_conn.endpoint(),
                        "source endpoint swap failed — staying on current feed",
                    );
                }
            }
        }
        // `durable` (fsynced) lags `dispatched`; advertise it as flush/cursor.
        let dispatched = stream.dispatched_lsn();
        let durable = durable_lsn.get();
        let received: Pos<SourceReceived> = Pos::new(feed.last_server_wal_end().max(dispatched));
        // Two frontiers, two questions. `consumed` is where resume asks the
        // promoted target to start; `received` is the source head last heard
        // about, which the target must reach before promotion. Bytes cannot
        // have been consumed without being received, so a source that has not
        // reported a head yet reads as level with the consumed frontier
        match (paused, pause_frontier) {
            (true, None) => {
                pause_frontier = Some((
                    stream.next_lsn().get(),
                    received.get().max(stream.next_lsn().get()),
                ));
                // A pause this process never saw lifted was taken before it
                // booted, so these two numbers replace ones an operator may
                // already hold. Both re-freeze conservatively — consumed drops
                // back to the floor, received re-derives from the live head —
                // but a promotion decision has to be taken from the pair on
                // offer now (plans/failover.md §What pause freezes)
                pause_refrozen = !ever_unpaused;
                let (consumed, head) = pause_frontier.expect("just frozen");
                tracing::info!(
                    target: "walshadow",
                    pause_consumed_lsn = %format_pg_lsn(consumed),
                    pause_received_lsn = %format_pg_lsn(head),
                    refrozen = pause_refrozen,
                    "pause observed — frontier frozen",
                );
            }
            (false, Some(_)) => {
                pause_frontier = None;
                pause_refrozen = false;
            }
            _ => {}
        }
        ever_unpaused |= !paused;
        // Step 5 of the protocol, answered off the connection step 4's repoint
        // already moved onto the target: replay, receive, and recovery state
        // beside the frozen frontier they have to reach
        // (plans/failover.md §Operator protocol)
        if !paused {
            promotion = PromotionGate::blocked("not_paused");
            promotion_polled_at = None;
        } else if promotion_polled_at.is_none_or(|t| t.elapsed() >= PROMOTION_POLL) {
            promotion_polled_at = Some(Instant::now());
            promotion = match tokio::time::timeout(
                PROMOTION_POLL,
                promotion_gate(&mut feed, pause_frontier),
            )
            .await
            {
                Ok(gate) => gate,
                Err(_) => {
                    feed.drop_sql_client();
                    PromotionGate::unreachable()
                }
            };
        }
        let shadow_replay = shadow_replay_lsn.get();
        let (shadow_agg, shadow_served_tli) = {
            let state = shadow_state.lock().await;
            (state.aggregate(), state.timeline)
        };
        if let Some(flush) = shadow_agg.min_flush_lsn {
            shadow_flush_lsn.join(flush);
        }
        let (drain_lsn, resume_safe_lsn) = {
            let mut b = xact_buffer.lock().await;
            let ea = emitter_ack.get();
            let drain_lsn = b.stats().drain_lsn;
            // Keep every undurable transaction reachable after restart
            // Read acknowledgment first so no transaction escapes floor
            (drain_lsn, b.resume_safe_lsn(ea))
        };
        // shadow_replay==0 (sweeper off or not yet reported) means "no
        // constraint from shadow", not the literal min: else a fresh boot
        // with retention off pins apply_lsn at 0 and source's slot never recycles.
        let apply_ceiling = match shadow_replay.get() {
            0 => resume_safe_lsn,
            s => s.min(resume_safe_lsn.get()).into(),
        };
        // Never walks back. A crossing commits the fork segment's start, which
        // `align_down(emitter_ack)` reaches only once descendant WAL fills that
        // segment; the natural terms must not undo the position a restart
        // resumes from. A rewind (`--start-lsn`, `--ignore-cursor`) lowers it by
        // seeding `resume_floor` at the rewind point instead
        let floor = manifest::resolved_floor(resume_safe_lsn, durable).max(resume_floor.get());
        let floor_timeline = history.floor_branch(
            floor.get(),
            live_identity.timeline,
            stream.timeline(),
            WAL_SEG_SIZE,
        );
        let cur = manifest::Manifest {
            version: manifest::MANIFEST_VERSION,
            floor,
            source: manifest::SourceIdentity {
                system_id: live_identity.system_id,
                timeline: floor_timeline,
                timeline_begin: history.begin_of(floor_timeline).unwrap_or(0).into(),
            },
            wal: manifest::WalBranch {
                stream_timeline: stream.timeline(),
            },
            lsn: manifest::LsnSet {
                source_received: received,
                filter_durable: durable,
                shadow_replay,
                drain: drain_lsn,
                emitter_ack: resume_safe_lsn,
                shadow_flush: shadow_flush_lsn.get(),
            },
        };
        if last_cursor_write.is_none_or(|t| t.elapsed() >= cursor_write_interval) {
            manifest::write(&args.spill_dir, &cur)
                .await
                .context("write resume manifest")?;
            last_cursor_write = Some(Instant::now());
            // Publish only after persist: pruners cut against what a
            // crash-now restart actually resumes from.
            resume_floor.join(cur.floor);
            // Descriptor log prunes against the same floor, off this task: a
            // compaction rewrites the whole ckpt inline and would stall WAL
            // consumption past the source's wal_sender_timeout
            gc_floor.join(cur.floor);
        }
        // flush caps physical slot's restart_lsn.
        // Manifest writes are cadence-gated above while keepalive replies inside
        // next_event can send this status at any time.
        let status = StandbyStatus {
            write_lsn: received,
            // Keep source slot behind crash-safe resume floor
            flush_lsn: resume_floor.get().min(apply_ceiling.retag()),
            apply_lsn: apply_ceiling,
        };
        let dispatched_before = stream.dispatched_lsn();
        // Set inside the select arm, acted on once the chunk borrow is released
        let mut ancestor_ended = false;
        let chunk = tokio::select! {
            biased;
            sig = tokio::signal::ctrl_c() => {
                sig.context("install ctrl_c handler")?;
                break "signal";
            }
            _ = sigterm.recv() => break "signal",
            // Idle tick so metrics/cursor keep tracking, and so a `paused` flip
            // is picked up promptly.
            _ = tokio::time::sleep(metrics_tick) => None,
            // Paused: stop consuming source WAL (idle); resume re-enables this
            // arm and the pump continues from the same LSN. A pending crossing
            // also parks it — that connection is out of COPY until the
            // descendant is requested.
            res = feed.next_event(status, &mut chunk_buf), if !paused && !crossing.pending() => match res {
                Ok(SourceEvent::Wal(c)) => Some(c),
                Ok(SourceEvent::TimelineEnd) => {
                    ancestor_ended = true;
                    None
                }
                // Dropped where the chain says the branch ends: nothing is
                // resumable there, so this is the crossing arriving as a socket
                // close rather than as a next-timeline result
                Ok(SourceEvent::Shutdown) | Err(_)
                if history.branch_exhausted(stream.timeline(), stream.next_lsn().get()) =>
            {
                    tracing::info!(
                        target: "walshadow",
                        switch_lsn = %stream.next_lsn(),
                        finished_timeline = stream.timeline(),
                        "source stream ended where the branch does — crossing",
                    );
                    crossing.ancestor_ended();
                    crossing.needs_connection();
                    None
                }
                // The source stopped, this consumer did not: reconnect, which
                // is also how a switchover's demoted primary hands over
                Ok(SourceEvent::Shutdown) => {
                    tracing::info!(
                        target: "walshadow",
                        resume_lsn = %stream.next_lsn(),
                        "source shut down its walsender — reconnecting",
                    );
                    feed = source_recovery
                        .recover(
                            anyhow::anyhow!("source walsender exited"),
                            &cfg,
                            source_conn.slot.as_deref(),
                            stream_branch(&history, live_identity.system_id, &stream),
                            &mut stream,
                            &mut record_sink,
                            &mut segment_sink,
                        )
                        .await?;
                    source_swap_pending = false;
                    source_swap_retry_at = None;
                    None
                }
                Err(e) => {
                    let resume = stream.next_lsn().get();
                    tracing::warn!(
                        target: "walshadow",
                        error = %e,
                        resume_lsn = format_pg_lsn(resume).to_string(),
                        "source stream error — recovering",
                    );
                    feed = source_recovery
                        .recover(
                            e,
                            &cfg,
                            source_conn.slot.as_deref(),
                            stream_branch(&history, live_identity.system_id, &stream),
                            &mut stream,
                            &mut record_sink,
                            &mut segment_sink,
                        )
                        .await?;
                    // Recovery dialed the live endpoint, so a queued swap is done
                    source_swap_pending = false;
                    source_swap_retry_at = None;
                    let resumed = stream.next_lsn().get();
                    tracing::info!(
                        target: "walshadow",
                        resume_lsn = format_pg_lsn(resumed).to_string(),
                        "source reconnected — resuming replication",
                    );
                    None
                }
            },
        };
        let server_end = chunk
            .as_ref()
            .map(|c| c.server_wal_end)
            .unwrap_or(received.get());
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
        if ancestor_ended {
            // Answer the backend's CopyDone now, leaving the connection in
            // simple-query mode: that is the state the crossing reads history
            // from, and the state a retry can rebuild by reconnecting
            if let Err(e) = feed.end_historic_stream().await {
                tracing::warn!(
                    target: "walshadow",
                    error = %format!("{e:#}"),
                    "ending the historic stream failed — reconnecting to cross",
                );
                crossing.needs_connection();
            }
            crossing.ancestor_ended();
        }
        // Nothing left to stream on the ancestor at its own switchpoint, so
        // only the crossing moves the stream forward. Attempts pace themselves
        // and leave the rest of the loop publishing meanwhile
        // A pause takes the crossing decision back from the pump, so it also
        // clears a wedge: the operator fixes what the refusal named, then
        // resumes and the proof runs again from the untouched ancestor
        if paused && let Some(wedge) = crossing.unpark() {
            tracing::info!(
                target: "walshadow",
                reason = wedge.reason,
                "pause clears the parked crossing — resume re-proves the fork",
            );
        }
        let crossing_due = !paused && crossing.due(Instant::now());
        if crossing_due && crossing.awaiting_connection() {
            match SourceFeed::connect(&cfg).await {
                Ok(fresh) => {
                    feed = fresh.with_status_interval(Duration::from_secs(args.status_interval));
                    crossing.connected();
                }
                Err(e) => {
                    tracing::warn!(
                        target: "walshadow",
                        error = %format!("{e:#}"),
                        endpoint = source_conn.endpoint(),
                        "cannot reach the source to cross the fork — retrying",
                    );
                    crossing.retry_at(Instant::now() + SOURCE_SWAP_RETRY);
                }
            }
        }
        if crossing_due && !crossing.awaiting_connection() && !crossing.has_fork() {
            match switchover
                .probe(
                    &mut feed,
                    &stream,
                    history.begin_of(stream.timeline()).unwrap_or(0),
                    &mut timeline_stats,
                )
                .await
            {
                Ok(probed) => {
                    tracing::info!(
                        target: "walshadow",
                        finished_timeline = probed.finished_tli,
                        next_timeline = probed.next_tli,
                        live_timeline = probed.live_tli,
                        switch_lsn = %format_pg_lsn(probed.switch_lsn),
                        "source fork proved — draining the pipeline to it",
                    );
                    crossing.hold_fork(probed);
                }
                Err(e) if e.retryable() => {
                    tracing::warn!(
                        target: "walshadow",
                        error = %format!("{e:#}"),
                        reason = e.reason(),
                        "proving the source fork failed — retrying",
                    );
                    crossing.retry_from_source(Instant::now() + SOURCE_SWAP_RETRY);
                }
                Err(e) => crossing.park(e, stream.next_lsn().get(), None),
            }
        }
        if crossing_due
            && !crossing.awaiting_connection()
            && let Some(probed) = crossing.take_fork()
        {
            // Both fork proofs read the decoder's view, so the pump-side queue
            // drains first: a record still in flight answers for a frontier the
            // decoder has not reached, which would read as a transaction left
            // open at the fork
            record_sink
                .decoder_xact
                .flush()
                .await
                .context("flush queueing decoder sink at the fork")?;
            let fence = Instant::now();
            let in_flight = loop {
                let n = record_sink.decoder_xact.in_flight();
                if n == 0 || fence.elapsed() >= FORK_FENCE_DRAIN {
                    break n;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            };
            // Timeout stops queue drain only, fork guards remain authoritative
            if in_flight != 0 {
                tracing::warn!(
                    target: "walshadow",
                    in_flight,
                    waited = ?fence.elapsed(),
                    "fork fence gave up draining the pump queue — guards decide",
                );
            }
            let (guards, resume_safe) = {
                let mut b = xact_buffer.lock().await;
                let ea = emitter_ack.get();
                let resume_safe = b.resume_safe_lsn(ea);
                let stats = b.stats();
                (
                    ForkGuards {
                        drain_lsn: stats.drain_lsn,
                        open_xacts: stats.xacts_active as usize,
                    },
                    resume_safe,
                )
            };
            // Barrier: every consumer past the position about to be committed,
            // so a restart from it loses nothing. The loop keeps publishing
            // meanwhile, so a wait reads as a wait rather than a stall, and the
            // source has stopped producing so nothing queues up behind it
            let waiting_on = walshadow::transition::ForkBarrier {
                resume_safe_lsn: resume_safe,
                shadow_apply_lsn: shadow_agg.min_apply_lsn,
                filter_durable: durable,
                floor: resume_floor.get(),
            }
            .pending(probed.switch_lsn, WAL_SEG_SIZE);
            if let Some(wait) = waiting_on {
                // Prod the walreceiver: non-forced replies fire only on flush
                // progress, and the ancestor's tail may be the last thing left
                shadow_state.lock().await.request_status();
                if barrier_logged.is_none_or(|t| t.elapsed() >= BARRIER_LOG_INTERVAL) {
                    tracing::info!(
                        target: "walshadow",
                        switch_lsn = %format_pg_lsn(probed.switch_lsn),
                        waiting_on = wait.label(),
                        "fork barrier: {wait}",
                    );
                    barrier_logged = Some(Instant::now());
                }
                crossing.hold_fork(probed);
            } else {
                barrier_logged = None;
                let commit = async |resume: walshadow::transition::ForkResume| {
                    commit_fork_resume(
                        &args.spill_dir,
                        &live_identity,
                        resume,
                        manifest::LsnSet {
                            // Fork cannot precede last observed source head
                            source_received: received.get().max(resume.switch_lsn.get()).into(),
                            filter_durable: durable,
                            shadow_replay,
                            drain: guards.drain_lsn,
                            emitter_ack: resume_safe,
                            shadow_flush: shadow_flush_lsn.get(),
                        },
                        &resume_floor,
                        &gc_floor,
                    )
                    .await
                };
                match switchover
                    .cross(
                        &mut feed,
                        source_conn.slot.as_deref(),
                        &mut stream,
                        &mut record_sink,
                        &mut segment_sink,
                        status,
                        guards,
                        &probed,
                        commit,
                        &mut timeline_stats,
                    )
                    .await
                {
                    Ok(crossed) => {
                        tracing::info!(
                            target: "walshadow",
                            system_id = live_identity.system_id,
                            finished_timeline = crossed.finished_tli,
                            next_timeline = crossed.next_tli,
                            live_timeline = crossed.live_tli,
                            switch_lsn = %format_pg_lsn(crossed.switch_lsn),
                            resume_lsn = %stream.next_lsn(),
                            floor_lsn = %resume_floor.get(),
                            drain_lsn = %guards.drain_lsn,
                            prefix_bytes_verified = crossed.prefix_bytes,
                            slot = source_conn.slot.as_deref(),
                            "crossed source timeline",
                        );
                        history = crossed.history;
                        crossing.committed();
                        source_swap_pending = false;
                        source_swap_retry_at = None;
                        source_swap_blocked_on = "";
                    }
                    // Lineage, prefix, and publication proofs need an operator; a
                    // source or storage error is worth another attempt. Every
                    // retryable failure lands before the commit, so the retry
                    // starts from the same proof against an untouched ancestor
                    Err(e) if e.retryable() => {
                        tracing::warn!(
                            target: "walshadow",
                            error = %format!("{e:#}"),
                            reason = e.reason(),
                            stream_timeline = stream.timeline(),
                            "timeline crossing failed — retrying",
                        );
                        crossing.retry_from_source(Instant::now() + SOURCE_SWAP_RETRY);
                        crossing.hold_fork(probed);
                    }
                    Err(e) => crossing.park(e, stream.next_lsn().get(), Some(probed.switch_lsn)),
                }
            }
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
        if let Some(msg) = gc_fatal.message() {
            anyhow::bail!("{msg}");
        }
        // Re-read rather than reuse the top-of-iteration pair: a crossing commits
        // a new floor and branch mid-iteration, and this is what an operator
        // watches to know the crossing is durable
        let published_floor = floor.max(resume_floor.get());
        let published_branch = history.floor_branch(
            published_floor.get(),
            live_identity.timeline,
            stream.timeline(),
            WAL_SEG_SIZE,
        );
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
                raw_pending_rows: b.raw_pending_rows(),
                raw_pending_bytes: b.raw_pending_bytes(),
            };
            (stats, resident, line)
        };
        let oracle_line = oracle
            .as_ref()
            .map(|o| o.stats.summary())
            .unwrap_or_default();
        let oracle_stats = oracle.as_ref().map(|o| o.stats.as_ref());
        let bridge_line = bridge.stats.summary();
        let bridge_stats = Some(bridge.stats.as_ref());
        let decoder_stats: &walshadow::decoder_sink::DecoderStats = &record_sink.decoder_stats;
        let emitter_stats: Option<&walshadow::ch_emitter::EmitterStats> =
            record_sink.emitter_stats.as_deref();
        let shadow_apply_lsn = shadow_agg.min_apply_lsn.map_or(0, Pos::get);
        let lag_bytes = received.get().saturating_sub(shadow_apply_lsn);
        rate_estimator.observe(Instant::now(), received.get());
        let lag_seconds = rate_estimator.seconds_for(lag_bytes);
        // Post-worker snapshots so the metric reflects what the worker
        // drained, not the top-of-iteration values.
        let emitter_ack_for_metric = emitter_ack.get();
        let drain_for_metric = xact_stats.drain_lsn;
        populate_metrics(
            &metrics,
            received,
            now_dispatched.into(),
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
            bridge_stats,
            start_instant.elapsed().as_secs(),
            SourceSwapView {
                swaps: source_swaps_total,
                failures: source_swap_failures_total,
                pending: source_swap_pending,
                blocked_on: source_swap_blocked_on,
            },
            TimelineView {
                source_system_id: live_identity.system_id,
                source_timeline: stream.timeline(),
                floor_timeline: published_branch,
                shadow_served_timeline: shadow_served_tli,
                shadow_replay_timeline: shadow_agg.replay_timeline.unwrap_or(0),
                floor_lsn: published_floor,
                stats: timeline_stats,
                pause_frontier,
                pause_refrozen,
                wedge: crossing.wedge().cloned(),
                promotion,
            },
            ShadowMetricsView {
                apply_lag_bytes: lag_bytes,
                apply_lag_seconds: lag_seconds,
                active_connections: shadow_agg.active_connections as u64,
                dropped_total: shadow_agg.dropped_total,
            },
            &boundary_hold_stats,
            &capture_stats,
            &desc_log,
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
                bridge = %bridge_line,
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
        // Ack can pin after transaction leaves buffer
        let ack_snap = *ack_probe.borrow();
        if xact_stats.xacts_active > 0 || !ack_snap.all_done() || ack_snap.wedged != 0 {
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
                    emitter_ack_lsn = %emitter_ack_for_metric,
                    drain_lsn = %xact_stats.drain_lsn,
                    source_received = %received,
                    filter_dispatched = format_pg_lsn(now_dispatched).to_string(),
                    inflight = %summary,
                    ack = ?ack_snap,
                    waiting_on = ack_snap.stall_reason().unwrap_or("buffered xacts"),
                    "emitter ack pinned",
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
    // Close the floor channel and join: nothing else may own desc_log.ckpt
    // after the session returns
    drop(gc_floor);
    gc_task.await.ok();
    if let Some(msg) = gc_fatal.message() {
        anyhow::bail!("{msg}");
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
                order_by: row.try_get("order_by").ok().flatten(),
                primary_key: row.try_get("primary_key").ok().flatten(),
                system: walshadow::mapping::SystemColumnNames {
                    lsn: row.try_get("lsn").ok().flatten(),
                    xid: row.try_get("xid").ok().flatten(),
                    commit_ts: row.try_get("commit_ts").ok().flatten(),
                    is_deleted: row.try_get("is_deleted").ok().flatten(),
                },
                match_kind: row.try_get("match").ok().flatten(),
            },
        );
    }

    for row in client
        .query(
            &format!(
                "SELECT namespace, relname, attname, match, target_type FROM {s}.config_column"
            ),
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
                target_type: row.try_get("target_type").ok().flatten(),
                match_kind: row.try_get("match").ok().flatten(),
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
    table_initial_loads: &ahash::HashMap<RelName, String>,
    active_tables: &HashSet<RelName>,
    sql_scoped_tables: &HashSet<RelName>,
    raw_start: u64,
) -> anyhow::Result<()> {
    for (rel, mode) in table_initial_loads {
        if !active_tables.contains(rel) || sql_scoped_tables.contains(rel) {
            continue;
        }
        match mode.parse() {
            Ok(InitialLoadMode::None) => {}
            Ok(parsed) => {
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
            Err(_) => tracing::warn!(
                target: "walshadow::config",
                qname = %rel,
                mode,
                "unknown TOML initial_load mode; streaming from start LSN only",
            ),
        }
    }
    Ok(())
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
            mapping.publish(Arc::new(tables)).await;
            tracing::info!(
                target: "walshadow::config",
                "routing map refreshed from resolved config",
            );
        }
    })
}

/// Max unsynced segments queued before the pump blocks on `on_segment`;
const SEGMENT_FSYNC_QUEUE: usize = 64;

#[cfg(target_os = "linux")]
fn sync_filesystem(fd: std::os::fd::RawFd) -> std::io::Result<()> {
    if unsafe { libc::syncfs(fd) } == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error())
    }
}

#[cfg(not(target_os = "linux"))]
fn sync_filesystem(_fd: std::os::fd::RawFd) -> std::io::Result<()> {
    unreachable!("walshadow-stream is Linux-only")
}

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
    durable_lsn: Arc<Monotone<FilterDurable>>,
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
            let synced = tokio::task::spawn_blocking(move || sync_filesystem(dirfd)).await;
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
            durable_lsn.join(max_lsn);
        }
    })
}

/// Compact the descriptor log against each published resume floor, off the
/// pump task. A compaction rewrites the whole ckpt inline; on the pump that
/// stalls WAL consumption while `wal_sender_timeout` runs with no keepalive
/// answered. Boundary capture still shares the log's writer mutex, so a
/// boundary landing mid-compaction blocks its hold — this removes the stall
/// for boundary-free stretches, which is the common case.
fn spawn_desc_log_gc(
    desc_log: Arc<walshadow::desc_log::DescriptorLog>,
    floor: Gate<Floor>,
    fatal: walshadow::pipeline::Fatal,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut cut = floor.current();
        loop {
            if let Err(e) = desc_log.maybe_gc(cut).await {
                fatal.set(format!("descriptor log gc at {cut}: {e}"));
                return;
            }
            let Ok(next) = floor.advance(cut).await else {
                return;
            };
            cut = next;
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
    shadow_replay_lsn: Arc<Monotone<ShadowReplay>>,
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
            shadow_replay_lsn.join(lsn);
            let cutoff = manifest::retention_cutoff(lsn, retention_bytes, redo.map(Pos::new));
            match trim_below_lsn(&out_dir, cutoff).await {
                Ok(r) if r.segments_removed > 0 => {
                    tracing::info!(
                        target: "walshadow::retention",
                        segments = r.segments_removed,
                        manifests = r.manifests_removed,
                        partials = r.partials_removed,
                        bytes_freed = r.bytes_freed,
                        cutoff_lsn = %cutoff,
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

/// Live `[source]` endpoint moves, from the pump's swap state.
struct SourceSwapView {
    swaps: u64,
    failures: u64,
    /// Config names an endpoint the pump has not reached yet
    pending: bool,
    /// Proof the last attempt failed on, empty once one lands
    blocked_on: &'static str,
}

/// Branch selection plus the frozen pause frontier — everything a switchover
/// decision reads (plans/failover.md §Surfaces).
struct TimelineView {
    source_system_id: u64,
    /// Branch the pump is reading
    source_timeline: u32,
    /// Branch owning the durable floor, which restart resumes on
    floor_timeline: u32,
    /// Branch the shadow-facing walsender advertises
    shadow_served_timeline: u32,
    /// Branch the shadow is replaying
    shadow_replay_timeline: u32,
    floor_lsn: Pos<Floor>,
    stats: TimelineStats,
    /// `(consumed, received)` frozen when the pump observed a pause
    pause_frontier: Option<(u64, u64)>,
    /// That freeze re-derived a pause this process found already in effect
    pause_refrozen: bool,
    /// Crossing the pump parked on, waiting for an operator
    wedge: Option<CrossingWedge>,
    promotion: PromotionGate,
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
    raw_pending_rows: u64,
    raw_pending_bytes: u64,
}

#[allow(clippy::too_many_arguments)]
async fn populate_metrics(
    registry: &MetricsRegistry,
    source_received_lsn: Pos<SourceReceived>,
    filter_lsn: Pos<FilterDispatched>,
    shadow_replay_lsn: Pos<ShadowReplay>,
    decoder_commit_lsn: Pos<Drain>,
    emitter_ack_lsn: Pos<EmitterAck>,
    rec_metrics: &MetricsRecordSink,
    pump_queue_depth: u64,
    queue_records_out_total: u64,
    xact_stats: &walshadow::xact_buffer::XactBufferStats,
    drain_resident: DrainResident,
    budget: Option<&walshadow::budget::MemoryBudget>,
    decoder_stats: &walshadow::decoder_sink::DecoderStats,
    emitter_stats: Option<&walshadow::ch_emitter::EmitterStats>,
    oracle_stats: Option<&walshadow::oracle::OracleStats>,
    bridge_stats: Option<&walshadow::bridge::BridgeStats>,
    uptime_secs: u64,
    source_swap: SourceSwapView,
    timeline_view: TimelineView,
    shadow_view: ShadowMetricsView,
    boundary_hold: &BoundaryHoldStats,
    capture: &walshadow::catalog_capture::CaptureStats,
    desc_log: &walshadow::desc_log::DescriptorLog,
    config_resolver: Option<&ConfigResolver>,
    backfiller: Option<&walshadow::copy_backfill::CopyBackfiller>,
) {
    use std::collections::BTreeMap;
    use walshadow::record::rmgr_label;
    let (proc_cpu, proc_rss) = read_process_stats();
    let desc_log_gauges = desc_log.gauges();
    let log_stats = desc_log.stats_handle();
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
        raw_stash_deferred_total: decoder_stats.raw_stash_deferred.load(Ordering::Relaxed),
        toast_stash_decoded_total: emitter_stats
            .map(|s| s.toast_stash_decoded.load(Ordering::Relaxed))
            .unwrap_or(0),
        toast_stash_discarded_total: emitter_stats
            .map(|s| s.toast_stash_discarded.load(Ordering::Relaxed))
            .unwrap_or(0),
        toast_stash_in_place_total: emitter_stats
            .map(|s| s.toast_stash_in_place.load(Ordering::Relaxed))
            .unwrap_or(0),
        stash_foreign_db_skipped_total: emitter_stats
            .map(|s| s.stash_foreign_db_skipped.load(Ordering::Relaxed))
            .unwrap_or(0),
        xact_plan_rows: emitter_stats
            .map(|s| s.plan_rows.load(Ordering::Relaxed))
            .unwrap_or(0),
        xact_plan_bytes_by_storage: emitter_stats
            .map(|s| {
                [
                    s.plan_bytes_mem.load(Ordering::Relaxed),
                    s.plan_bytes_file.load(Ordering::Relaxed),
                ]
            })
            .unwrap_or_default(),
        xact_plan_failures_by_reason: emitter_stats
            .map(|s| {
                [
                    s.plan_failures_spool.load(Ordering::Relaxed),
                    s.plan_failures_fail_closed_image_only
                        .load(Ordering::Relaxed),
                    s.plan_failures_fail_closed_malformed
                        .load(Ordering::Relaxed),
                    s.plan_failures_fail_closed_unsupported_op
                        .load(Ordering::Relaxed),
                    s.plan_failures_stash_ambiguous.load(Ordering::Relaxed),
                    s.plan_failures_incomplete_toast.load(Ordering::Relaxed),
                    s.plan_failures_missing_stash_resolution
                        .load(Ordering::Relaxed),
                    s.plan_failures_detoast.load(Ordering::Relaxed),
                    s.plan_failures_partial_update.load(Ordering::Relaxed),
                    s.plan_failures_view.load(Ordering::Relaxed),
                    s.plan_failures_drain.load(Ordering::Relaxed),
                ]
            })
            .unwrap_or_default(),
        route_snapshots_by_result: emitter_stats
            .map(|s| {
                [
                    s.route_snapshots_mapped.load(Ordering::Relaxed),
                    s.route_snapshots_unmapped.load(Ordering::Relaxed),
                ]
            })
            .unwrap_or_default(),
        raw_stash_records_by_kind_op: [
            decoder_stats.raw_stash_dirty_ops.load(),
            decoder_stats.raw_stash_marker_ops.load(),
        ],
        raw_stash_bytes_by_storage: [
            xact_stats.raw_stash_bytes_mem,
            xact_stats.raw_stash_bytes_spill,
        ],
        raw_decode_records_by_kind_op: emitter_stats
            .map(|s| {
                [
                    s.raw_decode_toast_ops.load(),
                    s.raw_decode_ordinary_ops.load(),
                ]
            })
            .unwrap_or_default(),
        raw_decode_rows_by_op: emitter_stats
            .map(|s| s.raw_decode_rows_ops.load())
            .unwrap_or_default(),
        raw_pending_rows: drain_resident.raw_pending_rows,
        raw_pending_bytes: drain_resident.raw_pending_bytes,
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
        emitter_deletes_discarded: emitter_stats
            .map(|s| s.deletes_discarded.load(Ordering::Relaxed))
            .unwrap_or(0),
        oracle_blocks_total: oracle_stats
            .map(|s| s.blocks.load(Ordering::Relaxed))
            .unwrap_or(0),
        oracle_rows_total: oracle_stats
            .map(|s| s.rows.load(Ordering::Relaxed))
            .unwrap_or(0),
        oracle_cells_total: oracle_stats
            .map(|s| s.cells.load(Ordering::Relaxed))
            .unwrap_or(0),
        oracle_conversion_errors_total: oracle_stats
            .map(|s| s.conversion_errors.load(Ordering::Relaxed))
            .unwrap_or(0),
        oracle_errors_total: oracle_stats
            .map(|s| s.errors.load(Ordering::Relaxed))
            .unwrap_or(0),
        uptime_secs,
        source_endpoint_swaps_total: source_swap.swaps,
        source_endpoint_swap_failures_total: source_swap.failures,
        source_endpoint_swap_pending: u64::from(source_swap.pending),
        source_endpoint_swap_blocked_on: source_swap.blocked_on,
        crossing_blocked_on: timeline_view.wedge.as_ref().map_or("", |w| w.reason),
        crossing_detail: timeline_view.wedge.map(|w| w.detail).unwrap_or_default(),
        source_system_id: timeline_view.source_system_id,
        source_timeline: timeline_view.source_timeline,
        floor_timeline: timeline_view.floor_timeline,
        shadow_served_timeline: timeline_view.shadow_served_timeline,
        shadow_replay_timeline: timeline_view.shadow_replay_timeline,
        floor_lsn: timeline_view.floor_lsn,
        timeline_switches_total: timeline_view.stats.switches,
        timeline_switch_failures_by_reason: timeline_view.stats.failures_by_reason,
        timeline_switch_lsn: timeline_view.stats.switch_lsn,
        timeline_prefix_bytes_verified_total: timeline_view.stats.prefix_bytes_verified,
        timeline_transition_seconds_total: timeline_view.stats.seconds_total,
        pause_consumed_lsn: timeline_view.pause_frontier.map_or(0, |(c, _)| c),
        pause_received_lsn: timeline_view.pause_frontier.map_or(0, |(_, r)| r),
        pause_refrozen: timeline_view.pause_refrozen,
        promotion_ready: timeline_view.promotion.ready,
        promotion_blocked_on: timeline_view.promotion.blocked_on,
        promotion_target_in_recovery: timeline_view.promotion.in_recovery,
        promotion_target_replay_lsn: timeline_view.promotion.replay_lsn,
        promotion_target_receive_lsn: timeline_view.promotion.receive_lsn,
        shadow_apply_lag_bytes: shadow_view.apply_lag_bytes,
        shadow_apply_lag_seconds: shadow_view.apply_lag_seconds,
        shadow_stream_active_connections: shadow_view.active_connections,
        shadow_stream_dropped_connections_total: shadow_view.dropped_total,
        catalog_boundary_holds_total: boundary_hold.holds.load(Ordering::Relaxed),
        catalog_boundary_hold_failures_total: boundary_hold.failures.load(Ordering::Relaxed),
        catalog_boundary_hold_seconds_total: boundary_hold.hold_seconds_total(),
        desc_capture_sql_total: capture.sql_captures.load(Ordering::Relaxed),
        desc_capture_log_replay_total: capture.log_replays.load(Ordering::Relaxed),
        desc_capture_skipped_covered_total: capture.skipped_covered.load(Ordering::Relaxed),
        desc_capture_all_total: capture.capture_all_runs.load(Ordering::Relaxed),
        desc_capture_rels_total: capture.rels_captured.load(Ordering::Relaxed),
        desc_capture_seconds_total: capture.capture_nanos.load(Ordering::Relaxed) as f64 / 1e9,
        desc_events_added_total: capture.events_added.load(Ordering::Relaxed),
        desc_events_changed_total: capture.events_changed.load(Ordering::Relaxed),
        desc_events_dropped_total: capture.events_dropped.load(Ordering::Relaxed),
        descriptor_ambiguous_total: capture.ambiguities_published.load(Ordering::Relaxed),
        pending_captures_total: capture.pending_captures.load(Ordering::Relaxed),
        pending_rels_total: capture.pending_rels.load(Ordering::Relaxed),
        pending_holds_total: capture.pending_holds.load(Ordering::Relaxed),
        pending_hold_seconds_total: capture.pending_hold_nanos.load(Ordering::Relaxed) as f64 / 1e9,
        pending_entries_promoted_total: capture.pending_entries_promoted.load(Ordering::Relaxed),
        pending_entries_dropped_abort_total: capture
            .pending_entries_dropped_abort
            .load(Ordering::Relaxed),
        pending_ambiguities_suppressed_total: capture
            .ambiguities_suppressed
            .load(Ordering::Relaxed),
        pending_degraded_by_reason: std::array::from_fn(|i| {
            capture.pending_degraded[i].load(Ordering::Relaxed)
        }),
        desc_log_entries: desc_log_gauges.0,
        desc_log_tail_bytes: desc_log_gauges.1,
        desc_log_batches: desc_log_gauges.2,
        desc_log_gc_total: log_stats.gc_runs.load(Ordering::Relaxed),
        desc_log_gc_dropped_entries_total: log_stats.gc_dropped_entries.load(Ordering::Relaxed),
        desc_lookups_present_total: log_stats.lookups_present.load(Ordering::Relaxed),
        desc_lookups_dropped_total: log_stats.lookups_dropped.load(Ordering::Relaxed),
        desc_lookups_retired_total: log_stats.lookups_retired.load(Ordering::Relaxed),
        desc_lookups_ambiguous_total: log_stats.lookups_ambiguous.load(Ordering::Relaxed),
        desc_lookups_not_covered_total: log_stats.lookups_not_covered.load(Ordering::Relaxed),
        desc_lookups_foreign_db_total: log_stats.lookups_foreign_db.load(Ordering::Relaxed),
        config_pending_decl_rels: config_resolver.map(|r| r.pending_decl_count()).unwrap_or(0),
        config_replicate_opt_in_total: config_resolver.map(|r| r.opt_in_total()).unwrap_or(0),
        config_replicate_opt_out_total: config_resolver.map(|r| r.opt_out_total()).unwrap_or(0),
        config_backfills_pending: backfiller.map(|b| b.pending_count()).unwrap_or(0),
        config_backfills_pending_by_mode: backfiller.map(|b| b.pending_by_mode()).unwrap_or([0; 3]),
        bridge_up: bridge_gauge(bridge_stats, |b| &b.up),
        bridge_requests_by_op: bridge_ops(bridge_stats.map(|b| &b.requests)),
        bridge_errors_by_op: bridge_ops(bridge_stats.map(|b| &b.errors)),
        bridge_request_nanos_by_op: bridge_ops(bridge_stats.map(|b| &b.request_nanos)),
        bridge_reconnects_total: bridge_gauge(bridge_stats, |b| &b.reconnects),
        bridge_scan_rows_total: bridge_gauge(bridge_stats, |b| &b.scan_rows),
        bridge_scan_replay_moved_total: bridge_gauge(bridge_stats, |b| &b.scan_replay_moved),
        bridge_scan_subtrans_mismatch_total: bridge_gauge(bridge_stats, |b| {
            &b.scan_subtrans_mismatch
        }),
        bridge_native_bytes_total: bridge_gauge(bridge_stats, |b| &b.native_bytes),
    };
    registry.set(snap).await;
}

/// Zero when bridge stats are unavailable, so series stays present
fn bridge_gauge(
    stats: Option<&walshadow::bridge::BridgeStats>,
    pick: fn(&walshadow::bridge::BridgeStats) -> &AtomicU64,
) -> u64 {
    stats.map(|b| pick(b).load(Ordering::Relaxed)).unwrap_or(0)
}

fn bridge_ops(
    counters: Option<&[AtomicU64; walshadow::bridge::OP_COUNT]>,
) -> [u64; walshadow::bridge::OP_COUNT] {
    counters
        .map(|a| std::array::from_fn(|i| a[i].load(Ordering::Relaxed)))
        .unwrap_or_default()
}

/// Retry transient source failures, stop when source reports missing WAL.
async fn reconnect_source(
    cfg: &PgConfig,
    slot: Option<&str>,
    resume_lsn: Pos<Floor>,
    branch: SourceBranch,
    floor: Pos<Floor>,
    status_interval: Duration,
) -> Result<SourceFeed> {
    use backon::{ExponentialBuilder, Retryable};

    (|| resume_source_feed(cfg, slot, resume_lsn, branch, floor, status_interval))
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
}

/// How long the fork proofs wait for the pump-side queue to drain. Past it the
/// buffer's own view answers, which reads a still-queued record as a
/// transaction open at the fork and refuses the crossing — the fail-closed
/// direction.
const FORK_FENCE_DRAIN: Duration = Duration::from_secs(30);

/// Branch the stream is reading, as a reconnect has to name it: number plus the
/// switchpoint the proved chain places it at.
fn stream_branch(history: &TimelineHistory, system_id: u64, stream: &WalStream) -> SourceBranch {
    SourceBranch {
        system_id,
        timeline: stream.timeline(),
        begin: history.begin_of(stream.timeline()).unwrap_or(0),
    }
}

/// Step 5's gate: what the promotion target owes before it may be promoted,
/// answered off the source connection walshadow already holds rather than a
/// second `psql` (plans/failover.md §Operator protocol).
#[derive(Debug, Clone, Copy, Default)]
struct PromotionGate {
    ready: bool,
    /// Term that fails, empty once ready
    blocked_on: &'static str,
    in_recovery: bool,
    replay_lsn: u64,
    receive_lsn: u64,
}

impl PromotionGate {
    fn blocked(blocked_on: &'static str) -> Self {
        Self {
            blocked_on,
            ..Self::default()
        }
    }

    fn unreachable() -> Self {
        Self::blocked("source_unreachable")
    }
}

/// How often the gate is re-read while paused, and how long one read may take
/// before the endpoint counts as unreachable. The pump publishes every tick, so
/// a target that stops answering must not stall the loop with it.
const PROMOTION_POLL: Duration = Duration::from_secs(1);

/// Read the gate off `feed`'s sidecar SQL connection. Only meaningful while
/// paused: `pause_received` is the frozen head the target has to reach, and an
/// unfrozen one moves under the decision.
async fn promotion_gate(
    feed: &mut SourceFeed,
    pause_frontier: Option<(u64, u64)>,
) -> PromotionGate {
    let Some((_, pause_received)) = pause_frontier else {
        return PromotionGate::blocked("not_paused");
    };
    let client = match feed.sql_client().await {
        Ok(c) => c,
        Err(e) => {
            tracing::debug!(target: "walshadow", error = %format!("{e:#}"), "promotion gate");
            return PromotionGate::unreachable();
        }
    };
    let row = client
        .query_one(
            "SELECT pg_is_in_recovery(), pg_last_wal_replay_lsn(), pg_last_wal_receive_lsn()",
            &[],
        )
        .await;
    let row = match row {
        Ok(row) => row,
        Err(e) => {
            tracing::debug!(target: "walshadow", error = %e, "promotion gate");
            feed.drop_sql_client();
            return PromotionGate::unreachable();
        }
    };
    let in_recovery: bool = row.get(0);
    let replay_lsn = row.get::<_, Option<PgLsn>>(1).map(u64::from).unwrap_or(0);
    let receive_lsn = row.get::<_, Option<PgLsn>>(2).map(u64::from).unwrap_or(0);
    // Order names the first term to fix, not every one that fails
    let blocked_on = if !in_recovery {
        "not_a_standby"
    } else if replay_lsn < pause_received {
        "replay_below_pause_received"
    } else if receive_lsn > replay_lsn {
        "received_not_replayed"
    } else {
        ""
    };
    PromotionGate {
        ready: blocked_on.is_empty(),
        blocked_on,
        in_recovery,
        replay_lsn,
        receive_lsn,
    }
}

/// Cadence of the fork barrier's progress line. The barrier is unbounded by
/// design — the source has stopped, so waiting costs nothing that is moving —
/// which makes the log the only place the wait is legible.
const BARRIER_LOG_INTERVAL: Duration = Duration::from_secs(2);

/// Commit a crossing's resume position: the fork segment's start, on the
/// descendant. Sound only behind the barrier, which proved nothing below the
/// fork is still in flight — the floor's contract is that a restart from it
/// loses nothing, not that the natural terms have caught up to it
/// (plans/failover.md §Crossing order).
///
/// Publishes to the pruners only after the persist, the same order the status
/// loop uses: a cut must never sit above what a crash-now restart replays from.
async fn commit_fork_resume(
    spill_dir: &Path,
    identity: &manifest::SourceIdentity,
    resume: walshadow::transition::ForkResume,
    lsn: manifest::LsnSet,
    resume_floor: &Monotone<Floor>,
    gc_floor: &Monotone<Floor>,
) -> Result<()> {
    let committed = manifest::Manifest {
        version: manifest::MANIFEST_VERSION,
        floor: resume.floor,
        source: manifest::SourceIdentity {
            system_id: identity.system_id,
            timeline: resume.timeline,
            // The fork is where the descendant begins, so the next boot can
            // refuse a sibling that shares its number
            timeline_begin: resume.switch_lsn,
        },
        wal: manifest::WalBranch {
            stream_timeline: resume.timeline,
        },
        lsn,
    };
    manifest::write(spill_dir, &committed)
        .await
        .context("write resume manifest at the fork")?;
    // Descendant floor starts new position space
    resume_floor.rebase(resume.floor);
    gc_floor.rebase(resume.floor);
    tracing::info!(
        target: "walshadow",
        timeline = resume.timeline,
        floor = %resume.floor,
        switch_lsn = %resume.switch_lsn,
        "committed the fork resume position",
    );
    Ok(())
}

/// Dial `[source]` until it answers, re-resolving the endpoint between
/// attempts.
///
/// Exiting instead would crash-loop the window a switchover opens between
/// stopping writes on the old primary and repointing at the target
/// (plans/failover.md §Operator protocol): every restart there dials a server
/// that is down. `ctl` and `/metrics` are bound before this, so the repoint
/// that ends the wait can be applied to the daemon doing the waiting.
async fn connect_source_waiting(
    args: &Args,
    source_conn: &mut SourceConn,
    cfg: &mut PgConfig,
) -> SourceFeed {
    loop {
        match SourceFeed::connect(cfg).await {
            Ok(feed) => {
                return feed.with_status_interval(Duration::from_secs(args.status_interval));
            }
            Err(e) => tracing::warn!(
                target: "walshadow",
                error = %format!("{e:#}"),
                endpoint = source_conn.endpoint(),
                "source unreachable — waiting for it, or for a repoint",
            ),
        }
        tokio::time::sleep(SOURCE_SWAP_RETRY).await;
        let Some(path) = args.ch_config.as_deref() else {
            continue;
        };
        match walshadow::ch_emitter::load_effective(path, cli_base(args)).await {
            Ok(table) => match SourceConn::from_table(&table).map(|mut next| {
                // Preserve CLI slot override across reloads
                if args.slot.is_some() {
                    next.slot = args.slot.clone();
                }
                next
            }) {
                Ok(next) if next != *source_conn => {
                    tracing::info!(
                        target: "walshadow",
                        from = source_conn.endpoint(),
                        to = next.endpoint(),
                        slot = next.slot.as_deref(),
                        "source moved while waiting",
                    );
                    *source_conn = next;
                    *cfg = source_conn.to_pg_config();
                }
                Ok(_) => {}
                Err(e) => tracing::warn!(target: "walshadow", error = %e, "[source] reload"),
            },
            Err(e) => {
                tracing::warn!(target: "walshadow", error = %format!("{e:#}"), "config reload")
            }
        }
    }
}

/// Backoff between attempts at a moved `[source]` endpoint. The old feed keeps
/// streaming meanwhile, so this only paces retries against an endpoint that is
/// not up yet (repointed before the target accepts connections).
const SOURCE_SWAP_RETRY: Duration = Duration::from_secs(2);

/// Cluster plus the branch the stream is reading, what a resumed connection has
/// to match.
#[derive(Debug, Clone, Copy)]
struct SourceBranch {
    system_id: u64,
    timeline: u32,
    /// Where that branch begins per the chain walshadow proved. A timeline
    /// number is not unique across branches — two standbys of one primary,
    /// promoted independently, are both timeline 2 under one system identifier
    /// — so number equality alone accepts a sibling
    /// (plans/failover.md §Lineage). `0` above timeline 1 means unrecorded.
    begin: u64,
}

/// Dial the source and resume at `resume_lsn`, proving continuity first:
///
/// 1. same cluster, or foreign WAL replays into these artifacts
/// 2. the live chain places the requested branch where walshadow left it,
///    which is what separates a descendant from a sibling sharing its number
/// 3. the requested branch still serves `resume_lsn`
/// 4. the configured slot reaches `floor`, the position a restart asks for
///
/// A live timeline *newer* than the requested one is a promotion that landed
/// under a stable endpoint, so the request stays on the requested branch: the
/// walsender then ends it at the fork and the crossing takes over, needing no
/// operator repoint and no daemon restart. `[source]` is live-reloadable, so
/// the address reached here can differ from the one boot dialed and these
/// proofs are what make that safe.
///
/// Resume is LSN-exact, so `WalStream`, filter, and catalog state stand and no
/// WAL is re-read.
async fn resume_source_feed(
    cfg: &PgConfig,
    slot: Option<&str>,
    resume_lsn: Pos<Floor>,
    branch: SourceBranch,
    floor: Pos<Floor>,
    status_interval: Duration,
) -> Result<SourceFeed> {
    let mut feed = SourceFeed::connect(cfg)
        .await
        .with_context(|| format!("connect source {}:{}", cfg.host, cfg.port))?
        .with_status_interval(status_interval);
    let ident = feed.identify_system().await.context("IDENTIFY_SYSTEM")?;
    let system_id: u64 = ident.sysid.parse().context("IDENTIFY_SYSTEM sysid")?;
    anyhow::ensure!(
        system_id == branch.system_id,
        "source is system {system_id}, artifacts belong to {}",
        branch.system_id,
    );
    anyhow::ensure!(
        ident.timeline >= branch.timeline,
        "source is on timeline {}, below the stream's {}; an older branch cannot \
         serve what has already been read",
        ident.timeline,
        branch.timeline,
    );
    let raw_history = if ident.timeline > 1 {
        feed.timeline_history(ident.timeline)
            .await
            .context("TIMELINE_HISTORY")?
    } else {
        None
    };
    match raw_history {
        Some(raw) => {
            let history = TimelineHistory::parse(ident.timeline, &raw).map_err(|source| {
                TransitionError::HistoryMalformed {
                    tli: ident.timeline,
                    source,
                }
            })?;
            prove_branch(&history, branch, resume_lsn.get())?;
        }
        // Timeline 1 has no history file, and a source serving none for a newer
        // branch can place nothing; only a run that never left the branch it is
        // asking for is provable without one
        None if ident.timeline == branch.timeline && branch.begin == 0 => {}
        None => Err(TransitionError::HistoryMissing {
            tli: ident.timeline,
        })?,
    }
    if let Some(name) = slot {
        feed.prove_physical_slot(name, resume_lsn, floor)
            .await
            .map_err(TransitionError::from)?;
    }
    feed.start_physical_replication(slot, resume_lsn.get(), branch.timeline)
        .await
        .with_context(|| format!("START_REPLICATION at {resume_lsn}"))?;
    Ok(feed)
}

/// The live chain has to agree with the branch walshadow is reading, both about
/// where it began and about it still owning `resume_lsn`. Typed with the
/// crossing's own vocabulary, so a refused reconnect names the same proof a
/// refused crossing would.
fn prove_branch(
    history: &TimelineHistory,
    branch: SourceBranch,
    resume_lsn: u64,
) -> Result<(), TransitionError> {
    let live_begin =
        history
            .begin_of(branch.timeline)
            .ok_or_else(|| TransitionError::NotDescendant {
                finished: branch.timeline,
                live: history.target(),
            })?;
    // `0` above timeline 1 is unrecorded, not "begins at 0/0": `--ignore-cursor`
    // adopts a live branch without a chain to read a switchpoint from
    if branch.begin != 0 && live_begin != branch.begin {
        return Err(TransitionError::SiblingBranch {
            tli: branch.timeline,
            stored_begin: branch.begin,
            live_begin,
        });
    }
    if !history.proves_ancestor(branch.timeline, resume_lsn) {
        return Err(TransitionError::ResumePastFork {
            next_lsn: resume_lsn.into(),
            switch_lsn: history.switchpoint_of(branch.timeline).unwrap_or(0),
        });
    }
    Ok(())
}

/// `reason=` label for a refused reconnect. Same vocabulary as a refused
/// crossing: an endpoint move that cannot proceed is a switchover proof
/// failing, and "the swap failed" alone does not say which.
fn swap_reason(err: &anyhow::Error) -> &'static str {
    err.downcast_ref::<TransitionError>()
        .map(TransitionError::reason)
        .unwrap_or("source")
}

struct SourceRecovery<'a> {
    status_interval: Duration,
    backup: Option<&'a walrus::config::Settings>,
    spill_dir: &'a Path,
    /// Published resume floor, which is what a slot on the far end has to still
    /// reach — the reconnect's own `resume_lsn` sits above it
    floor: &'a Monotone<Floor>,
}

impl SourceRecovery<'_> {
    /// Try source, replay archive gap, then return to source. `cfg`, `slot`,
    /// and `branch` are the live endpoint, slot name, and proved branch, passed
    /// per call rather than held, so a recovery that starts after a `[source]`
    /// reload or a crossing dials the new address under the new name and asks
    /// for the descendant, with the archive read under its segment names.
    #[allow(clippy::too_many_arguments)]
    async fn recover(
        &self,
        source_error: anyhow::Error,
        cfg: &PgConfig,
        slot: Option<&str>,
        branch: SourceBranch,
        stream: &mut WalStream,
        record_sink: &mut (dyn RecordSink + Send),
        segment_sink: &mut (dyn walshadow::record::SegmentSink + Send),
    ) -> Result<SourceFeed> {
        let mut resume_lsn = stream.next_lsn();
        let source_missing = walshadow::source_feed::is_wal_segment_removed(&source_error);

        // Source first (primary_conninfo analog): a plain drop is usually
        // transient, so try the source again at the exact resume point before
        // reaching for the archive. A removed-WAL (58P01) error means the
        // source genuinely can't serve it — skip straight to the archive.
        if !source_missing {
            let floor = self.floor.get();
            match resume_source_feed(cfg, slot, resume_lsn, branch, floor, self.status_interval)
                .await
            {
                Ok(feed) => return Ok(feed),
                Err(retry_error) => tracing::warn!(
                    target: "walshadow",
                    error = %retry_error,
                    resume_lsn = %resume_lsn,
                    "source still unavailable — trying archive",
                ),
            }
        } else {
            tracing::warn!(
                target: "walshadow",
                error = %source_error,
                resume_lsn = %resume_lsn,
                "source recycled resume point — trying archive",
            );
        }

        // Archive fallback (restore_command analog). `reconnect_or_operator`
        // covers both "no archive": a transient error retries the source with
        // backoff, a removed-WAL error surfaces the operator-action message.
        let Some(settings) = self.backup else {
            return self
                .reconnect_or_operator(
                    cfg,
                    slot,
                    branch,
                    resume_lsn,
                    "no [backup] archive configured",
                )
                .await;
        };
        let storage = match settings.build_storage() {
            Ok(storage) => storage,
            Err(archive_error) => {
                return self
                    .reconnect_or_operator(
                        cfg,
                        slot,
                        branch,
                        resume_lsn,
                        &format!("build archive storage: {archive_error:#}"),
                    )
                    .await;
            }
        };
        let seg_dir = self.spill_dir.join("resume_wal");

        loop {
            let archive_segment = fetch_archive_segment(
                settings,
                &storage,
                &seg_dir,
                branch.timeline,
                resume_lsn.get(),
            )
            .await;
            let (name, bytes) = match archive_segment {
                Ok(segment) => segment,
                Err(archive_error) => {
                    let _ = tokio::fs::remove_dir_all(&seg_dir).await;
                    tracing::info!(
                        target: "walshadow",
                        error = %archive_error,
                        resume_lsn = %resume_lsn,
                        "archive lacks next WAL — switching back to source",
                    );
                    return self
                        .reconnect_or_operator(
                            cfg,
                            slot,
                            branch,
                            resume_lsn,
                            &format!("archive fallback failed: {archive_error:#}"),
                        )
                        .await;
                }
            };

            stream
                .push(resume_lsn.get(), &bytes, record_sink, segment_sink)
                .await
                .with_context(|| format!("replay archived WAL {name}"))?;
            tracing::info!(
                target: "walshadow",
                segment = name,
                "restored resume WAL from archive",
            );
            resume_lsn = stream.next_lsn();
        }
    }

    async fn reconnect_or_operator(
        &self,
        cfg: &PgConfig,
        slot: Option<&str>,
        branch: SourceBranch,
        resume_lsn: Pos<Floor>,
        archive_error: &str,
    ) -> Result<SourceFeed> {
        reconnect_source(
            cfg,
            slot,
            resume_lsn,
            branch,
            self.floor.get(),
            self.status_interval,
        )
        .await
        .map_err(|source_error| {
            source_error.context(format!(
                "source cannot serve WAL at {resume_lsn}; {archive_error}; \
                 base-backup refresh requires operator action",
            ))
        })
    }
}

/// A data dir holding `PG_VERSION` was initialized by a prior bootstrap (or
/// external `initdb`), so the shadow can resume rather than reseed.
fn shadow_data_dir_initialized(dir: &std::path::Path) -> bool {
    dir.join("PG_VERSION").exists()
}

/// Run BASE_BACKUP into new shadow data dir and return backup `end_lsn`
/// Caller starts WAL pump from returned LSN, then starts and supervises
/// shadow in [`run`]
/// Config, credential, and CH-endpoint failures are resolved before the data
/// dir is created, so they leave nothing behind. Once extraction starts,
/// [`BOOTSTRAP_INCOMPLETE_MARKER`] remains after a failure; automatic
/// rebootstrap is intentionally unsupported
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
    plan: &BootstrapPlan,
    ch_config: Option<EmitterConfig>,
) -> Result<u64> {
    let shadow_data_dir = args
        .bootstrap_shadow_data_dir
        .clone()
        .context("--bootstrap-shadow-data-dir required when --bootstrap-mode != off")?;

    // Never land a base backup onto a dir that already holds a cluster: a
    // `PG_VERSION` with no completion marker is a crashed bootstrap or a
    // foreign/externally-seeded dir. Overwriting it would be destructive and
    // non-recoverable — make the operator clear it (or use `--bootstrap-mode=off`
    // to resume an externally-managed shadow).
    if shadow_data_dir_initialized(&shadow_data_dir) {
        anyhow::bail!(
            "bootstrap: {} already holds a cluster (PG_VERSION present) but no completed-bootstrap \
             marker — provide an empty data dir to bootstrap, or --bootstrap-mode=off to resume it",
            shadow_data_dir.display(),
        );
    }

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
        mode = ?plan.mode,
        shadow_data_dir = %shadow_data_dir.display(),
        "catalog map seeded",
    );

    type WalHydrate = (walrus::config::Settings, walrus::storage::DynStorage);
    let (source, wal_hydrate): (Box<dyn BackupSource>, Option<WalHydrate>) = match plan.mode {
        BootstrapMode::Direct => {
            let hydrate = if args.bootstrap_wal_from_archive {
                let settings = ch_config.as_ref().and_then(|c| c.backup.clone()).context(
                    "bootstrap: --bootstrap-wal-from-archive requires a [backup] \
                             section in --ch-config",
                )?;
                let storage = settings
                    .build_storage()
                    .context("bootstrap: build archive storage")?;
                Some((settings, storage))
            } else {
                None
            };
            let opts = BaseBackupOpts {
                label: format!(
                    "walshadow-bootstrap-{}",
                    chrono::Utc::now().format("%Y%m%dT%H%M%SZ")
                ),
                fast_checkpoint: args.bootstrap_fast_checkpoint,
                no_verify_checksums: false,
                max_rate_kib: None,
                wal: hydrate.is_none(),
            };
            (Box::new(DirectSource::new(src_cfg.clone(), opts)), hydrate)
        }
        BootstrapMode::ObjectStore => {
            let settings = ch_config
                    .as_ref()
                    .and_then(|c| c.backup.clone())
                    .context("bootstrap: --bootstrap-mode object_store requires a [backup] section in --ch-config")?;
            let storage = settings
                .build_storage()
                .context("bootstrap: build archive storage")?;
            let name = plan.backup_name.clone();
            if name != "LATEST" && !name.starts_with(BACKUP_NAME_PREFIX) {
                anyhow::bail!(
                    "bootstrap: backup name {name:?} must be `LATEST` or begin with \
                         `{BACKUP_NAME_PREFIX}` (--bootstrap-backup-name / [bootstrap] backup_name)"
                );
            }
            let mut src = ObjectStoreSource::new(
                settings.clone(),
                storage.clone(),
                name,
                args.spill_dir.clone(),
            );
            if let Some(n) = plan.parallelism {
                src = src.with_parallelism(n);
            }
            (Box::new(src), Some((settings, storage)))
        }
        BootstrapMode::Off => unreachable!("dispatch happened in run()"),
    };

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

    let ch_target = match ch_config {
        Some(emitter_cfg) => {
            let (mapping, resolved) = bootstrap_build_mapping(&emitter_cfg, &drain_catalog, args)
                .await
                .context("bootstrap: build mapping")?;
            Some((emitter_cfg, mapping, resolved))
        }
        None => None,
    };

    prepare_bootstrap_dir(&shadow_data_dir)
        .await
        .context("prepare shadow data dir for bootstrap")?;

    let cfg = BootstrapConfig::new(shadow_data_dir.clone());
    let (rx, pump) = spawn_greenfield_bootstrap(cfg, source, catalog_map, store_toast);

    let (shipped, outcome) = if let Some((emitter_cfg, mapping, resolved)) = ch_target {
        // Route bootstrap rows through the shared insert tail. Bootstrap
        // is the easy case: every row op=Insert at _lsn = start_lsn, no
        // aborts / TRUNCATE / DDL. Keep operator's flush_timeout; tail
        // defaults 0 to its own partial-flush deadline.
        let addr = format!("{}:{}", emitter_cfg.host, emitter_cfg.port);
        let stats = bootstrap_stats.clone();
        // Throwaway watermark: durability proof is `wait_through(K)`,
        // resume LSN is carried via the WAL pipeline's emitter_ack seed
        // (see `run`), so uniform `commit_lsn = start_lsn` here is fine.
        let emitter_ack = Arc::new(Monotone::<EmitterAck>::new(0));
        let fatal = Fatal::new();
        let inserter_pool_size = emitter_cfg.inserter_pool_size;

        let source_conninfo = format!(
            "host={} port={} user={} dbname={} sslmode={}",
            src_cfg.host,
            src_cfg.port,
            src_cfg.user,
            src_cfg.database,
            if src_cfg.sslmode == SslMode::Disable {
                "disable"
            } else {
                "prefer"
            },
        );
        let bootstrap_oracle = if walshadow::backfill::bootstrap_oracle::needs_oracle(
            &drain_catalog,
            &mapping.snapshot().await,
            &resolved.column_rules,
        ) {
            Some(
                walshadow::backfill::bootstrap_oracle::BootstrapOracle::provision(
                    args.spill_dir.join("bootstrap_oracle"),
                    source_conninfo,
                    src_cfg.password.clone(),
                    args.bridge_lib_dir.clone(),
                    Duration::from_secs(args.shadow_connect_timeout),
                )
                .await
                .context(
                    "bootstrap oracle: greenfield needs it to convert oracle columns; \
                     refusing to load empty columns",
                )?,
            )
        } else {
            None
        };

        let (msg_tx, ack, tail) = tail::spawn_with_config(
            &emitter_cfg,
            inserter_pool_size,
            stats.clone(),
            emitter_ack,
            fatal.clone(),
            None,
            bootstrap_oracle.as_ref().map(|o| o.oracle()),
        )
        .await
        .context("bootstrap: spawn insert tail")?;
        tracing::info!(
            target: "walshadow::bootstrap",
            addr = %addr,
            inserters = inserter_pool_size,
            "bootstrap insert tail started",
        );

        // `initial_load = "none"` (table override, else namespace) opts a
        // relation out of the greenfield snapshot: create it + stream CDC, but
        // don't page-walk its existing rows.
        let skip_initial: std::collections::HashSet<_> = drain_catalog
            .descriptors()
            .filter_map(|d| {
                let rn = &d.rel_name;
                let none = match emitter_cfg.table_initial_loads.get(rn) {
                    Some(s) => s.parse::<InitialLoadMode>() == Ok(InitialLoadMode::None),
                    None => {
                        resolved
                            .namespaces
                            .get(rn.namespace.as_ref())
                            .and_then(|n| n.initial_load)
                            == Some(InitialLoadMode::None)
                    }
                };
                none.then(|| rn.clone())
            })
            .collect();

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
            emitter_cfg.row_policy(),
            // No source-PG overlay during greenfield bootstrap, but the same
            // snapshot the CREATEs above rendered from: per-relation system
            // column names have to match what CH now holds
            Some(resolved),
            skip_initial,
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

    if let Some((settings, storage)) = wal_hydrate {
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

/// Routing map for the bootstrap drain: explicit `[table.*]` seeded up front,
/// then every seeded relation run through the DDL applicator's `Added` path so
/// `auto_create` namespaces get their CH table created and mapping registered.
/// Returns the snapshot those CREATEs rendered from, so the drain freezes
/// routes against the same per-relation rules.
async fn bootstrap_build_mapping(
    emitter_cfg: &EmitterConfig,
    catalog: &walshadow::backup_page_walk::CatalogMap,
    args: &Args,
) -> Result<(MappingHandle, Arc<walshadow::config::ResolvedConfig>)> {
    let mapping = walshadow::mapping::mapping_handle(emitter_cfg.tables.clone());
    let cli_overrides = CliOverrides {
        drop_table_strategy: args.drop_table_strategy,
        flush_timeout: args
            .ch_flush_timeout_ms
            .map(std::time::Duration::from_millis),
        source_slot: args.slot.clone(),
    };
    let (_resolver, config_rx) = ConfigResolver::new(
        emitter_cfg,
        cli_overrides,
        args.ch_config.clone(),
        cli_base(args),
        mapping.clone(),
    );
    let (ddl_cfg, merged_tables, resolved) = {
        let snap = config_rx.borrow();
        (
            walshadow::ch_ddl::DdlConfig::from_resolved(
                &snap,
                emitter_cfg.database.clone(),
                emitter_cfg.soft_delete,
                emitter_cfg.system_columns.clone(),
                emitter_cfg.replicate_all,
                emitter_cfg.runtime_config_schema.clone(),
            ),
            Arc::new(snap.tables.clone()),
            snap.clone(),
        )
    };
    // Publish rule-adjusted targets before creating tables
    mapping.publish(merged_tables).await;
    let mut applicator =
        walshadow::ch_ddl::DdlApplicator::new(emitter_cfg, ddl_cfg, mapping.clone(), config_rx)
            .await
            .context("bootstrap: init DDL applicator")?;
    for desc in catalog.descriptors() {
        applicator
            .apply(&SchemaEvent::Added { desc: desc.clone() })
            .await
            .with_context(|| format!("bootstrap: ensure CH table {}", desc.rel_name))?;
    }
    Ok((mapping, resolved))
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

fn resolve_shadow_start(args: &Args, mode: BootstrapMode) -> Result<ShadowStart> {
    let Some(dir) = &args.bootstrap_shadow_data_dir else {
        anyhow::ensure!(
            matches!(mode, BootstrapMode::Off),
            "bootstrap mode {mode:?} requires --bootstrap-shadow-data-dir",
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
        if !matches!(mode, BootstrapMode::Off) {
            tracing::info!(
                target: "walshadow::bootstrap",
                data_dir = %dir.display(),
                "shadow data dir already initialized, resuming without bootstrap",
            );
        }
        return Ok(ShadowStart::Resume(dir.clone()));
    }
    anyhow::ensure!(
        !matches!(mode, BootstrapMode::Off),
        "shadow data dir {} does not contain an initialized cluster; bootstrap mode off cannot \
         bootstrap it, pass direct or object_store via --bootstrap-mode or [bootstrap] mode",
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
    // Only a shadow walshadow started can be given a preload line; External
    // clusters are the operator's to configure
    let mut bridge = walshadow::shadow::BridgeConf::in_dir(&cfg.socket_dir);
    bridge.socket_path = args.bridge_socket_path();
    bridge.library_dir = args.bridge_lib_dir.clone();
    cfg.bridge = Some(bridge);
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

/// Start daemon-owned shadow using archived WAL
/// After fresh bootstrap, wait for backup `end_lsn`; direct mode includes
/// required WAL in `base.tar`
/// Restart a postmaster left alive by an unclean prior exit so it binds
/// this daemon's port and socket
async fn start_owned_shadow(
    shadow: &Arc<Shadow>,
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
        s.start_with_floor_retry(None).context("shadow start")?;
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

/// Fetch archived WAL for source recovery, returning the bytes that begin at
/// exactly `start_lsn`. The archive stores whole 16 MiB segment files, so
/// fetch the single segment containing `start_lsn` (aligned range → one
/// entry) and slice off the already-consumed prefix — the returned bytes line
/// up with `WalStream::next_lsn`, which is byte- not segment-aligned in steady
/// state.
async fn fetch_archive_segment(
    settings: &walrus::config::Settings,
    storage: &walrus::storage::DynStorage,
    seg_dir: &Path,
    timeline: u32,
    start_lsn: u64,
) -> Result<(String, Vec<u8>)> {
    let seg_start = WalStream::align_down(start_lsn, WAL_SEG_SIZE);
    let seg_end = seg_start + WAL_SEG_SIZE - 1;
    let segments = walshadow::backup_backfill::fetch_gap_segments(
        settings, storage, seg_dir, timeline, seg_start, seg_end,
    )
    .await
    .context("fetch archive WAL")?;
    let [(segment, path)] = segments.as_slice() else {
        anyhow::bail!(
            "archive fetch returned {} segments for one-segment range",
            segments.len()
        );
    };
    let bytes = tokio::fs::read(path)
        .await
        .with_context(|| format!("read archived WAL {}", path.display()))?;
    if bytes.len() != WAL_SEG_SIZE as usize {
        anyhow::bail!(
            "archived WAL {} has {} bytes, expected {}",
            segment.format(),
            bytes.len(),
            WAL_SEG_SIZE,
        );
    }
    let _ = tokio::fs::remove_file(path).await;
    let offset = (start_lsn - seg_start) as usize;
    Ok((segment.format(), bytes[offset..].to_vec()))
}

/// Fetch WAL `[start_lsn, end_lsn]` from archive storage into shadow's `pg_wal/`.
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

    #[tokio::test]
    async fn archive_fetch_reads_exact_segment() {
        let tmp = tempfile::tempdir().unwrap();
        let archive = tmp.path().join("archive");
        let segment_path = tmp.path().join("000000010000000000000000");
        fs::write(&segment_path, vec![0; WAL_SEG_SIZE as usize]).unwrap();
        let settings = walrus::config::Settings {
            storage: walrus::config::StorageSettings::Fs {
                path: archive.display().to_string(),
            },
            ..Default::default()
        };
        let storage = settings.build_storage().unwrap();
        walrus::pg::wal::push::handle(&settings, storage.clone(), &segment_path)
            .await
            .unwrap();

        let (name, bytes) =
            fetch_archive_segment(&settings, &storage, &tmp.path().join("restore"), 1, 0)
                .await
                .unwrap();
        assert_eq!(name, "000000010000000000000000");
        assert_eq!(bytes.len(), WAL_SEG_SIZE as usize);
    }

    #[tokio::test]
    async fn archive_fetch_slices_from_mid_segment() {
        // A mid-segment resume LSN must return the segment's tail beginning at
        // that LSN, not the whole segment (which would misalign the replay).
        let tmp = tempfile::tempdir().unwrap();
        let archive = tmp.path().join("archive");
        let segment_path = tmp.path().join("000000010000000000000000");
        let pattern: Vec<u8> = (0..WAL_SEG_SIZE as usize)
            .map(|i| (i % 251) as u8)
            .collect();
        fs::write(&segment_path, &pattern).unwrap();
        let settings = walrus::config::Settings {
            storage: walrus::config::StorageSettings::Fs {
                path: archive.display().to_string(),
            },
            ..Default::default()
        };
        let storage = settings.build_storage().unwrap();
        walrus::pg::wal::push::handle(&settings, storage.clone(), &segment_path)
            .await
            .unwrap();

        let offset = WAL_SEG_SIZE / 2;
        let (name, bytes) =
            fetch_archive_segment(&settings, &storage, &tmp.path().join("restore"), 1, offset)
                .await
                .unwrap();
        // Same segment file, sliced to begin at the mid-segment LSN.
        assert_eq!(name, "000000010000000000000000");
        assert_eq!(bytes.len(), (WAL_SEG_SIZE - offset) as usize);
        assert_eq!(bytes, pattern[offset as usize..]);
    }

    fn shadow_start(args: &Args) -> Result<ShadowStart> {
        resolve_shadow_start(args, resolve_bootstrap(args, None)?.mode)
    }

    #[test]
    fn shadow_start_external_without_data_dir() {
        assert!(matches!(
            shadow_start(&args_from(&[])).unwrap(),
            ShadowStart::External
        ));
        assert!(shadow_start(&args_from(&["--bootstrap-mode", "direct"])).is_err());
    }

    #[test]
    fn bootstrap_plan_layers_cli_over_toml() {
        let toml = |s: &str| EmitterConfig::from_toml_str(s).unwrap();

        let cfg = toml(
            "[ch]\n[bootstrap]\nmode = \"object_store\"\nbackup_name = \"base_0000000100000000000000AA\"\nobject_store_parallelism = 8\n",
        );
        let plan = resolve_bootstrap(&args_from(&[]), Some(&cfg)).unwrap();
        assert_eq!(plan.mode, BootstrapMode::ObjectStore);
        assert_eq!(plan.backup_name, "base_0000000100000000000000AA");
        assert_eq!(plan.parallelism, Some(8));

        let plan = resolve_bootstrap(
            &args_from(&[
                "--bootstrap-mode",
                "direct",
                "--bootstrap-backup-name",
                "LATEST",
            ]),
            Some(&cfg),
        )
        .unwrap();
        assert_eq!(plan.mode, BootstrapMode::Direct);
        assert_eq!(plan.backup_name, "LATEST");
        assert_eq!(plan.parallelism, Some(8), "TOML fills what the CLI omits");

        let plan = resolve_bootstrap(&args_from(&[]), Some(&toml("[ch]\n"))).unwrap();
        assert_eq!(plan.mode, BootstrapMode::Off);
        assert_eq!(plan.backup_name, "LATEST");
        assert_eq!(plan.parallelism, None);

        assert!(
            EmitterConfig::from_toml_str("[ch]\n[bootstrap]\nmode = \"objectstore\"\n").is_err()
        );
        assert!(
            EmitterConfig::from_toml_str("[ch]\n[bootstrap]\nobject_store_parallelism = 0\n")
                .is_err()
        );
    }

    #[test]
    fn bootstrap_mode_accepts_both_object_store_spellings() {
        for spelling in ["object_store", "object-store"] {
            let plan =
                resolve_bootstrap(&args_from(&["--bootstrap-mode", spelling]), None).unwrap();
            assert_eq!(plan.mode, BootstrapMode::ObjectStore, "{spelling}");
        }
    }

    #[test]
    fn bridge_socket_defaults_beside_shadow_socket() {
        assert_eq!(
            args_from(&[]).bridge_socket_path(),
            PathBuf::from("/tmp/sock/walshadow-bridge.sock")
        );
        assert_eq!(
            args_from(&["--bridge-socket", "/tmp/custom.sock"]).bridge_socket_path(),
            PathBuf::from("/tmp/custom.sock")
        );
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
                "--bootstrap-mode",
                "off",
                "--bootstrap-shadow-data-dir",
                d,
                "--walsender-bind",
                "127.0.0.1:5555",
            ])
        };

        // Direct bootstraps empty dir, off rejects it
        assert!(matches!(
            shadow_start(&direct(dir_str)).unwrap(),
            ShadowStart::Bootstrap(_)
        ));
        assert!(shadow_start(&off(dir_str)).is_err());

        // Resume initialized dir regardless of mode
        std::fs::write(dir.join("PG_VERSION"), b"17\n").unwrap();
        assert!(matches!(
            shadow_start(&direct(dir_str)).unwrap(),
            ShadowStart::Resume(_)
        ));
        assert!(matches!(
            shadow_start(&off(dir_str)).unwrap(),
            ShadowStart::Resume(_)
        ));

        // Incomplete bootstrap never triggers automatic rebootstrap
        std::fs::write(dir.join(BOOTSTRAP_INCOMPLETE_MARKER), b"").unwrap();
        assert!(shadow_start(&direct(dir_str)).is_err());
        assert!(shadow_start(&off(dir_str)).is_err());
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
            shadow_start(&args_from(&[
                "--bootstrap-mode",
                "direct",
                "--bootstrap-shadow-data-dir",
                dir_str,
            ]))
            .is_err()
        );
    }

    #[test]
    fn transport_args_reject_archive_only_and_zero_hold_timeout() {
        assert!(validate_transport_args(&args_from(&[])).is_ok());
        assert!(
            validate_transport_args(&args_from(&["--walsender-connect-timeout", "0"])).is_err(),
            "archive-only escape hatch must fail startup",
        );
        assert!(validate_transport_args(&args_from(&["--catalog-hold-timeout", "0"])).is_err());
    }

    /// Two standbys of one primary, promoted independently, are both timeline 2
    /// under one system identifier. The chain places either one, so only where
    /// the branch begins refuses the wrong one
    #[test]
    fn prove_branch_refuses_a_sibling_sharing_the_branch_number() {
        let ours = TimelineHistory::parse(2, b"1\t0/3000000\tno recovery target\n").unwrap();
        let sibling = TimelineHistory::parse(2, b"1\t0/5000000\tno recovery target\n").unwrap();
        let branch = SourceBranch {
            system_id: 7,
            timeline: 2,
            begin: ours.begin_of(2).unwrap(),
        };
        prove_branch(&ours, branch, 0x400_0000).expect("our own branch");
        let err = prove_branch(&sibling, branch, 0x600_0000).unwrap_err();
        assert_eq!(err.reason(), "sibling_branch", "{err}");
    }

    #[test]
    fn prove_branch_refuses_a_position_past_the_branchs_own_fork() {
        let history = TimelineHistory::parse(3, b"1\t0/3000000\n2\t0/5000000\n").unwrap();
        let branch = SourceBranch {
            system_id: 7,
            timeline: 2,
            begin: 0x300_0000,
        };
        prove_branch(&history, branch, 0x400_0000).expect("still inside timeline 2");
        let err = prove_branch(&history, branch, 0x500_0000).unwrap_err();
        assert_eq!(err.reason(), "resume_past_fork", "{err}");
        let absent = SourceBranch {
            timeline: 9,
            ..branch
        };
        assert_eq!(
            prove_branch(&history, absent, 0x100).unwrap_err().reason(),
            "timeline_not_descendant",
        );
    }

    #[test]
    fn stream_branch_names_the_branch_by_its_switchpoint() {
        let history = TimelineHistory::parse(2, b"1\t0/3000000\tno recovery target\n").unwrap();
        let stream = WalStream::new(2, WAL_SEG_SIZE, 0x300_0000).unwrap();
        assert_eq!(stream_branch(&history, 7, &stream).begin, 0x300_0000);
    }

    #[test]
    fn promotion_gate_defaults_are_not_ready() {
        assert!(!PromotionGate::default().ready);
        assert_eq!(
            PromotionGate::blocked("not_paused").blocked_on,
            "not_paused"
        );
        assert_eq!(
            PromotionGate::unreachable().blocked_on,
            "source_unreachable",
        );
    }

    #[test]
    fn swap_reason_reads_the_refusal_out_of_the_error() {
        let sibling = anyhow::Error::from(TransitionError::SiblingBranch {
            tli: 2,
            stored_begin: 1,
            live_begin: 2,
        });
        assert_eq!(swap_reason(&sibling), "sibling_branch");
        assert_eq!(
            swap_reason(&anyhow::anyhow!("connection refused")),
            "source"
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
