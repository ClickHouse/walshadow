//! Daemon CLI surface and the CLI-over-TOML merge that feeds config
//! resolution.

use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::path::PathBuf;

use anyhow::Result;
use clap::Parser;
use walshadow::ch_emitter::{BootstrapMode, EmitterConfig};
use walshadow::config::cli_over_toml;
use walshadow::mapping::DropTableStrategy;
use walshadow::retention::DEFAULT_RETENTION_BYTES;
use walshadow::schema::RelName;

/// `cli_over_toml` plus a ≥1 clamp for pool/batch sizes.
pub(crate) fn positive_usize(name: &str, cli: Option<usize>, toml: usize) -> usize {
    match cli_over_toml(cli, Some(toml)).unwrap_or(toml) {
        0 => {
            tracing::warn!(target: "walshadow::config", setting = name, "value below 1, using 1");
            1
        }
        n => n,
    }
}

/// `walshadow-stream init`: write a config from two connection URLs, so a
/// first run needs no TOML. Detected before daemon-arg parsing, same as `ctl`.
#[derive(Debug, Parser)]
#[command(
    name = "walshadow-stream init",
    version = walshadow::VERSION,
    about = "Probe source + destination, pick tables, write the config."
)]
pub(crate) struct InitArgs {
    /// Config to write; the daemon then runs with `--ch-config <path>`
    #[arg(
        long,
        env = "WALSHADOW_CH_CONFIG",
        default_value = "/etc/walshadow/ch-config.toml"
    )]
    pub(crate) config: PathBuf,
    #[arg(long, env = walshadow::init::PG_URL_ENV)]
    pub(crate) source_url: Option<String>,
    #[arg(long, env = walshadow::init::CH_URL_ENV)]
    pub(crate) ch_url: Option<String>,
    /// Replicate this table. Two words, schema then table; repeat per table
    #[arg(long, num_args = 2, value_names = ["SCHEMA", "TABLE"])]
    pub(crate) table: Vec<String>,
    /// Replicate every table that has a row key
    #[arg(long)]
    pub(crate) all_tables: bool,
    /// Restrict listing (and `--all-tables`) to one schema
    #[arg(long)]
    pub(crate) schema: Option<String>,
    /// Backfill of rows that pre-date the opt-in
    #[arg(long, default_value = "copy")]
    pub(crate) initial_load: String,
    /// Overwrite an existing config
    #[arg(long)]
    pub(crate) force: bool,
}

impl InitArgs {
    pub(crate) fn into_opts(self) -> walshadow::init::InitOpts {
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
    version = walshadow::VERSION,
    about = "Stream + filter physical WAL from source PG."
)]
pub(crate) struct Args {
    /// Source connection as one URL, eg
    /// `postgres://user:password@host:5432/dbname?sslmode=require`. Wins
    /// over the discrete `--host` / `--port` / … flags, loses to
    /// `[source]` in `--ch-config`, same as they do
    #[arg(long, env = walshadow::init::PG_URL_ENV)]
    pub(crate) source_url: Option<String>,
    /// Destination as one URL, eg
    /// `clickhouse://user:password@host:9000/database`. Supplies `[ch]`
    /// when no config file does, which is what turns the emitter on
    #[arg(long, env = walshadow::init::CH_URL_ENV)]
    pub(crate) ch_url: Option<String>,
    /// `[source]` / `[ch]` decoded from the two URL flags once at startup,
    /// then merged over the discrete flags by [`cli_base`]
    #[arg(skip)]
    pub(crate) url_base: toml::Table,
    /// Source PG host (TCP) or unix socket directory (leading `/`)
    #[arg(long, default_value = "localhost")]
    pub(crate) host: String,
    #[arg(long, default_value_t = 5432)]
    pub(crate) port: u16,
    #[arg(long, default_value = "postgres")]
    pub(crate) user: String,
    #[arg(long, default_value = "postgres")]
    pub(crate) dbname: String,
    /// Optional cleartext password. Replication-mode auth supports
    /// trust / cleartext / SCRAM-SHA-256.
    #[arg(long)]
    pub(crate) password: Option<String>,
    /// SSL mode: `disable`, `allow`, `prefer`, `require`, `verify-ca`,
    /// `verify-full`. Skipped on unix sockets regardless. verify-ca /
    /// verify-full consult `PGSSLROOTCERT` (else webpki bundle) for the
    /// trust anchor, same contract as libpq.
    #[arg(long, default_value = "prefer")]
    pub(crate) sslmode: String,
    /// Where filtered segments + manifests land; shadow PG's
    /// `restore_command` reads from here
    #[arg(long)]
    pub(crate) out_dir: PathBuf,
    /// CLI override for the TOML's `[source] slot` (physical replication
    /// slot). Unset defers to config, which reloads live; set pins the name
    /// for this process. Unset in both = slotless.
    #[arg(long)]
    pub(crate) slot: Option<String>,
    /// Start LSN in `X/Y` hex form. Defaults to source's current
    /// `pg_current_wal_lsn` (per `IDENTIFY_SYSTEM`), aligned down to a
    /// segment boundary.
    #[arg(long)]
    pub(crate) start_lsn: Option<String>,
    #[arg(long, default_value_t = 10)]
    pub(crate) status_interval: u64,

    /// Concurrent archive fetches, each holding at most one WAL segment.
    /// Held bytes are not charged to `[memory] resident_payload_max`, which
    /// leaves only half the host for shadow and every unmetered allocation
    #[arg(long, default_value = "4", value_parser = clap::value_parser!(u16).range(1..=64))]
    pub(crate) archive_prefetch: u16,

    /// Reconnect to compatible shadow PostgreSQL and leave it running on exit
    #[arg(long, env = "WALSHADOW_KEEP_SHADOW_RUNNING")]
    pub(crate) keep_shadow_running: bool,

    /// Stop after this many segments shipped (smoke tests). Zero = forever.
    #[arg(long, default_value_t = 0)]
    pub(crate) max_segments: u64,
    /// Shadow PG unix socket directory. Reused as libpq `host=` since
    /// libpq treats a leading `/` as a socket dir.
    #[arg(long)]
    pub(crate) shadow_socket_dir: PathBuf,
    #[arg(long, default_value_t = 5432)]
    pub(crate) shadow_port: u16,
    #[arg(long, default_value = "postgres")]
    pub(crate) shadow_user: String,
    /// Deprecated and ignored: the shadow is a physical clone of the source
    /// cluster, so its catalog is resolved from the applied `[source] dbname`
    /// (which is `--dbname` when no config overrides it). Setting it warns.
    #[arg(long, hide = true)]
    pub(crate) shadow_dbname: Option<String>,
    /// Wall-clock budget for the initial connect against shadow PG.
    /// Reused by [`walshadow::shadow_catalog::with_transient_retry`] so a still-warming shadow
    /// doesn't fail the daemon on first boot.
    #[arg(long, default_value_t = 30)]
    pub(crate) shadow_connect_timeout: u64,
    /// Unix socket of the pgext bridge worker. On a daemon-owned shadow
    /// this also writes `shared_preload_libraries` and the
    /// `walshadow.*` GUCs into shadow's conf, so the worker starts.
    /// Defaults to `<shadow-socket-dir>/walshadow-bridge.sock`.
    #[arg(long)]
    pub(crate) bridge_socket: Option<PathBuf>,
    /// Directory holding `walshadow.so` when it isn't in PG's `$libdir`,
    /// ie a build tree instead of `make install`. Written as
    /// `dynamic_library_path`.
    #[arg(long)]
    pub(crate) bridge_lib_dir: Option<PathBuf>,
    /// Walsender bind address. Shadow's generated `primary_conninfo` names
    /// it before shadow starts, so port 0 is rejected
    #[arg(long)]
    pub(crate) walsender_bind: SocketAddr,
    /// Slow-client backpressure: bytes queued onto a slow shadow
    /// connection before it's dropped + the wire falls back to
    /// `restore_command`.
    #[arg(long, default_value_t = 64 * 1024 * 1024)]
    pub(crate) walsender_slow_threshold: usize,
    /// Seconds the pump waits for shadow's walreceiver to attach before
    /// processing records. Must be positive; no attachment within it fails
    /// startup. Catalog-boundary holds require a live wire: whole archive
    /// segments can't stop publication at a mid-segment commit, so
    /// archive-only operation (the old `0` escape hatch) is rejected.
    /// `ShadowStreamSink` also drops bytes pushed before a connection
    /// registers; a pump racing past shadow's `START_REPLICATION` LSN
    /// leaves an apply LSN that never advances.
    #[arg(long, default_value_t = 60)]
    pub(crate) walsender_connect_timeout: u64,
    /// Seconds a catalog-boundary publication hold may wait for shadow to
    /// replay through a catalog-mutating commit before failing the daemon.
    /// Keep well under source's `wal_sender_timeout` (default 60s): the
    /// pump answers no source keepalives while parked.
    #[arg(long, default_value_t = 30)]
    pub(crate) catalog_hold_timeout: u64,
    /// Soft cap on in-flight records for the `QueueingRecordSink` feeding
    /// the decoder / xact-drain worker. Past this watermark the pump
    /// yields to let the worker drain; a stuck worker still surfaces via
    /// the catalog `wait_for_replay` timeout on the err slot.
    /// Overrides `[ch] decoder_queue_capacity`.
    #[arg(long)]
    pub(crate) decoder_queue_capacity: Option<usize>,
    /// Pump-side batch size for the `QueueingRecordSink`. Bigger
    /// amortises per-send overhead but adds pump→worker latency (worker's
    /// `wait_for_replay` lags one batch behind).
    /// Overrides `[ch] decoder_batch_size`.
    #[arg(long)]
    pub(crate) decoder_batch_size: Option<usize>,
    /// Decode-pool size (M): parallel decode workers (detoast, type
    /// coercion, oracle resolution). Only with `--ch-config`. `1` keeps
    /// decode serial so per-table WAL order is preserved; M>1 relaxes
    /// per-table order, relying on `_lsn` ReplacingMergeTree dedup.
    #[arg(long)]
    pub(crate) decoder_pool_size: Option<usize>,
    /// Insert-pool size (N): concurrent ClickHouse INSERT connections.
    /// Cloud throughput is RTT/part-commit bound, so N>1 is the main
    /// throughput lever. Only with `--ch-config`.
    #[arg(long)]
    pub(crate) inserter_pool_size: Option<usize>,
    /// Xact / TOAST spill and durable recovery state directory
    /// Startup removes transient transaction spill only
    #[arg(long)]
    pub(crate) spill_dir: PathBuf,
    /// In-memory xact buffer budget in bytes. Default matches PG's
    /// `logical_decoding_work_mem` (64 MiB).
    #[arg(long, default_value_t = walshadow::xact_buffer::DEFAULT_XACT_BUFFER_MAX)]
    pub(crate) xact_buffer_max: usize,
    /// CH-Native emitter config (TOML). Set → drained tuples ship to
    /// ClickHouse via `clickhouse-c-rs`; unset → metrics-only. Shape: see
    /// [`walshadow::ch_emitter::EmitterConfig::from_toml_str`]. Reloaded on
    /// SIGHUP (atomic mapping swap; connection params stay boot-only).
    #[arg(long)]
    pub(crate) ch_config: Option<PathBuf>,
    /// CLI override for the TOML's `[ch] flush_timeout_ms`, which defaults to
    /// 200ms. On the live pipeline an explicit `0` selects a 100ms
    /// partial-batch deadline so cold tables can't pin the watermark.
    /// No per-xact-close path runs on the live drain (survives only in
    /// bootstrap backfill, forced internally). SIGHUP reads `--ch-config`
    /// only, so use this flag for the boot value when not maintaining the
    /// knob in TOML.
    #[arg(long)]
    pub(crate) ch_flush_timeout_ms: Option<u64>,
    /// CLI override for the TOML's `[ch] drop_table_strategy` (`retain` /
    /// `drop` / `warn`). Highest-precedence layer: wins over TOML and
    /// survives SIGHUP reload, so an operator can pin the drop policy from
    /// the command line without editing TOML. Absent defers to TOML.
    #[arg(long)]
    pub(crate) drop_table_strategy: Option<DropTableStrategy>,
    /// HTTP/Prometheus metrics bind address. Disabled when absent.
    #[arg(long)]
    pub(crate) metrics_bind: Option<SocketAddr>,
    /// Control socket path, omit to disable control API
    #[arg(long)]
    pub(crate) control_socket: Option<PathBuf>,
    /// File name inside `--ch-config`'s `.d` directory that control mutations
    /// write. Rename it when a supervisor rewrites `50-api.toml` itself, so
    /// neither clobbers the other; lexical order decides which wins
    #[arg(long, default_value = walshadow::control::DEFAULT_FRAGMENT)]
    pub(crate) control_fragment: String,
    /// OTLP/gRPC endpoint for traces, e.g. `http://localhost:4317`. Absent
    /// disables tracing (zero overhead); falls back to
    /// `OTEL_EXPORTER_OTLP_ENDPOINT`. Spans emit at the `walshadow::trace`
    /// target.
    #[arg(long)]
    pub(crate) otlp_endpoint: Option<String>,
    /// Fraction of transactions to trace, `[0.0, 1.0]`. Head-sampled per txn
    /// (see `trace::should_sample`), so per-record span cost scales with it.
    #[arg(long, default_value_t = 0.01)]
    pub(crate) trace_sample_ratio: f64,
    /// WAL retention horizon in bytes. Segments older than
    /// `shadow_replay_lsn - retention_bytes` deleted every trim cycle.
    /// `0` disables trim.
    #[arg(long, default_value_t = DEFAULT_RETENTION_BYTES)]
    pub(crate) retention_bytes: u64,
    /// Skip pre-flight validators (server_version_num, wal_level, replica
    /// identity / row key, slot existence). For recovery drills.
    #[arg(long, default_value_t = false)]
    pub(crate) skip_preflight: bool,
    /// Ignore `manifest.toml` resume LSNs under `--spill-dir` at boot
    /// (greenfield resume even when a prior daemon left one), adopt a
    /// changed source timeline, and authorize boot past an unreadable or
    /// corrupt manifest (otherwise fatal). Source identity gate still
    /// applies; the manifest rewrites as the new daemon progresses. For
    /// "wipe + restart from a known LSN" drills.
    #[arg(long, default_value_t = false)]
    pub(crate) ignore_cursor: bool,
    /// Bootstrap source for empty shadow data dir. `off` never bootstraps;
    /// `direct` runs BASE_BACKUP over current replication connection;
    /// `object_store` reads wal-g-format backup from `[backup]` in
    /// `--ch-config`. Initialized data dir resumes without bootstrap
    /// regardless of mode. Unset falls through to `[bootstrap] mode` in
    /// `--ch-config`, then to `direct`
    #[arg(long)]
    pub(crate) bootstrap_mode: Option<BootstrapMode>,
    /// Shadow PG data dir. Daemon bootstraps or resumes shadow, writes
    /// config, starts and supervises postmaster, then stops it on exit
    #[arg(long)]
    pub(crate) bootstrap_shadow_data_dir: PathBuf,
    /// Object-store backup name. `LATEST` resolves to newest sentinel;
    /// otherwise the literal `base_TTTTTTTTLLLLLLLLSSSSSSSS` form. Unset
    /// falls through to `[bootstrap] backup_name`, then to `LATEST`.
    #[arg(long)]
    pub(crate) bootstrap_backup_name: Option<String>,
    /// Object-store fan-out parallelism. Raise for high-bandwidth buckets.
    /// Unset falls through to `[bootstrap] object_store_parallelism`, then
    /// to `ObjectStoreSource`'s own `min(4, num_cpus)` default.
    #[arg(long)]
    pub(crate) bootstrap_object_store_parallelism: Option<NonZeroUsize>,
    /// Parallel drain/batcher lanes for greenfield load. Each gets
    /// its own batcher and `inserter_pool_size / lanes` inserters. Unset falls
    /// through to `[bootstrap] lanes`, then `min(inserter_pool_size, num_cpus)`
    #[arg(long)]
    pub(crate) bootstrap_lanes: Option<NonZeroUsize>,
    /// BASE_BACKUP fast-checkpoint flag for `direct` mode. `true` avoids
    /// waiting for source's checkpoint_timeout; flip off if checkpoint
    /// cost matters more than bootstrap latency.
    #[arg(long, default_value_t = true)]
    pub(crate) bootstrap_fast_checkpoint: bool,
    /// Cap direct BASE_BACKUP transfer in KiB/s
    /// PostgreSQL accepts 32..1048576; window WAL remains unthrottled
    #[arg(long, value_parser = clap::value_parser!(i32).range(32..=1_048_576))]
    pub(crate) bootstrap_max_rate_kib: Option<i32>,
    /// Wait this many seconds for transactions crossing live bootstrap handoff
    /// Zero resumes immediately from oldest buffered record
    #[arg(long, default_value_t = 5)]
    pub(crate) bootstrap_wind_down_secs: u64,
    /// Fetch the bootstrap WAL window from the `[backup]` bucket instead of
    /// inside `base.tar` (`direct` mode only). Source then needn't retain or
    /// re-ship `[start_lsn, end_lsn]`, which is what fills its disk at high
    /// write rates. Requires `[backup]` in `--ch-config` and source
    /// archiving to that same bucket.
    #[arg(long, default_value_t = false)]
    pub(crate) bootstrap_wal_from_archive: bool,
    /// Maximum seconds to wait for shadow replay after bootstrap
    /// Abort daemon when timeout expires
    #[arg(long, default_value_t = 300)]
    pub(crate) bootstrap_shadow_replay_timeout: u64,
}

impl Args {
    pub(crate) fn bridge_socket_path(&self) -> PathBuf {
        self.bridge_socket
            .clone()
            .unwrap_or_else(|| self.shadow_socket_dir.join("walshadow-bridge.sock"))
    }
}

/// `[source]` + `[ch]` defaults from the CLI args — the base layer under the
/// config file for connection resolution, shared by the session and the
/// control surface. A `--source-url` / `--ch-url` merges over the discrete
/// flags, so the URL wins wherever both name a field.
pub(crate) fn cli_base(args: &Args) -> toml::Table {
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
pub(crate) fn url_base(args: &Args) -> Result<toml::Table> {
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

/// Enforce capability, not flag value: catalog-boundary holds need an
/// active walreceiver, so archive-only operation is not startable.
pub(crate) fn validate_transport_args(args: &Args) -> Result<()> {
    anyhow::ensure!(
        args.walsender_connect_timeout > 0,
        "--walsender-connect-timeout 0 (archive-only shadow) is unsupported: \
         catalog-boundary publication holds require an attached walreceiver",
    );
    anyhow::ensure!(
        args.catalog_hold_timeout > 0,
        "--catalog-hold-timeout must be positive",
    );
    anyhow::ensure!(
        args.walsender_bind.port() != 0,
        "--walsender-bind {} has port 0; shadow's primary_conninfo names this \
         address before shadow starts, so pass an explicit port",
        args.walsender_bind,
    );
    if let Some(db) = &args.shadow_dbname {
        tracing::warn!(
            target: "walshadow",
            shadow_dbname = %db,
            "--shadow-dbname is deprecated and ignored; the shadow catalog follows the applied [source] dbname",
        );
    }
    Ok(())
}

/// CLI layer over a parsed `[ch]` config, applied to every database's copy
pub(crate) fn finish_ch_config(mut cfg: EmitterConfig, args: &Args) -> EmitterConfig {
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
    cfg
}

#[cfg(test)]
pub(crate) fn args_from(argv: &[&str]) -> Args {
    let mut all = vec![
        "walshadow-stream",
        "--out-dir",
        "/tmp/out",
        "--spill-dir",
        "/tmp/spill",
        "--shadow-socket-dir",
        "/tmp/sock",
    ];
    for (flag, default) in [
        ("--bootstrap-shadow-data-dir", "/tmp/shadow-data"),
        ("--walsender-bind", "127.0.0.1:5555"),
    ] {
        if !argv.contains(&flag) {
            all.extend([flag, default]);
        }
    }
    all.extend(argv);
    Args::parse_from(all)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::args::args_from;

    #[test]
    fn bootstrap_wind_down_accepts_override_and_zero() {
        assert_eq!(args_from(&[]).bootstrap_wind_down_secs, 5);
        assert_eq!(
            args_from(&["--bootstrap-wind-down-secs", "60"]).bootstrap_wind_down_secs,
            60
        );
        assert_eq!(
            args_from(&["--bootstrap-wind-down-secs", "0"]).bootstrap_wind_down_secs,
            0
        );
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
    fn transport_args_reject_archive_only_and_zero_hold_timeout() {
        assert!(validate_transport_args(&args_from(&[])).is_ok());
        assert!(
            validate_transport_args(&args_from(&["--walsender-bind", "127.0.0.1:0"])).is_err(),
            "shadow's primary_conninfo cannot name a kernel-picked port",
        );
        assert!(
            validate_transport_args(&args_from(&["--walsender-connect-timeout", "0"])).is_err(),
            "archive-only escape hatch must fail startup",
        );
        assert!(validate_transport_args(&args_from(&["--catalog-hold-timeout", "0"])).is_err());
    }
}
