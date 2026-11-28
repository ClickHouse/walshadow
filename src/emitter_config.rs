//! Destination and source settings parsed from `--ch-config` TOML

use std::collections::BTreeMap;
use std::num::{NonZeroU32, NonZeroUsize};
use std::sync::Arc;
use std::time::Duration;

use crate::ch::{CompressionChoice, ConnectionConfig, EmitterError};
use crate::column_rules::{ColumnEntry, ColumnRule, Substitute};
use crate::mapping::{
    ColumnMapping, DropTableStrategy, InitialLoadMode, NamespaceMapping, SystemColumnNames,
    SystemColumns, TableMapping, TableTarget, named_instance,
};
use crate::runtime_config::{MatchKind, TableRow};
use crate::schema::RelName;
use crate::source::queueing_record_sink::{
    DEFAULT_QUEUEING_BATCH_SIZE, DEFAULT_QUEUEING_RECORD_SINK_CAPACITY,
};
use crate::table_rules::{PartitionRoute, TableRule};
use foldhash::{HashMap, HashMapExt};
use walrus::pg::replication::conn::PgConfig;
use walrus::pg::replication::tls::{SslMode, TlsParams};

#[derive(Clone, PartialEq, Eq, serde::Deserialize)]
#[serde(default)]
pub struct SourceConn {
    pub host: String,
    #[serde(deserialize_with = "crate::toml_de::de_port")]
    pub port: u16,
    pub user: String,
    pub password: Option<String>,
    pub dbname: String,
    #[serde(deserialize_with = "de_sslmode")]
    pub sslmode: SslMode,
    #[serde(deserialize_with = "crate::toml_de::de_nonempty")]
    pub slot: Option<String>,
}

impl Default for SourceConn {
    fn default() -> Self {
        Self {
            host: String::new(),
            port: 5432,
            user: String::new(),
            password: None,
            dbname: String::new(),
            // libpq default, matching `PgConfig::resolve`
            sslmode: SslMode::Prefer,
            slot: None,
        }
    }
}

/// Masks the password: `ResolvedConfig` is logged whole in places
impl std::fmt::Debug for SourceConn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SourceConn")
            .field("host", &self.host)
            .field("port", &self.port)
            .field("user", &self.user)
            .field("password", &self.password.as_ref().map(|_| "***"))
            .field("dbname", &self.dbname)
            .field("sslmode", &self.sslmode)
            .field("slot", &self.slot)
            .finish()
    }
}

fn de_sslmode<'de, D: serde::Deserializer<'de>>(d: D) -> Result<SslMode, D::Error> {
    use serde::Deserialize;
    SslMode::parse(&String::deserialize(d)?).map_err(serde::de::Error::custom)
}

#[derive(serde::Deserialize)]
struct SourceDocument {
    #[serde(default)]
    source: SourceConn,
}

impl SourceConn {
    /// Parse `[source]` out of a merged config table. Absent section keeps the
    /// defaults; a malformed port or sslmode is an error so `ctl apply`
    /// rejects it before the daemon reloads onto an unreachable endpoint.
    pub fn from_table(root: &toml::Table) -> Result<Self, String> {
        let doc: SourceDocument = toml::Value::Table(root.clone())
            .try_into()
            .map_err(crate::toml_de::message)?;
        Ok(doc.source)
    }

    /// Connection for the replication socket, its sidecar SQL client, and
    /// backfill COPY. TLS material comes from the libpq env vars
    /// (`PGSSLROOTCERT` / `PGSSLCERT` / `PGSSLKEY`), never from TOML, so it is
    /// resolved per connect rather than carried here.
    pub fn to_pg_config(&self) -> PgConfig {
        PgConfig {
            host: self.host.clone(),
            port: self.port,
            user: self.user.clone(),
            password: self.password.clone(),
            database: self.dbname.clone(),
            application_name: "walshadow".into(),
            sslmode: self.sslmode,
            tls: TlsParams::resolve(&walrus::config::Vars::default()),
        }
    }

    /// Credential-free endpoint for logs, metrics labels, and `ctl status`
    pub fn endpoint(&self) -> String {
        format!("{}:{}/{}", self.host, self.port, self.dbname)
    }
}

/// Default block accumulator budgets.
pub(crate) const DEFAULT_ROW_BUDGET: usize = 4_194_304;
pub(crate) const DEFAULT_BYTE_BUDGET: usize = 256 << 20; // 256 MiB

/// Default commit-drain slice budgets
/// ([`crate::xact::xact_buffer::CommittedDrain::next_batch`])
pub(crate) const DEFAULT_DRAIN_BATCH_ROWS: usize = 65_536;
pub(crate) const DEFAULT_DRAIN_BATCH_BYTES: usize = 32 << 20; // 32 MiB
pub(crate) const DEFAULT_PLAN_DISK_MAX: u64 = 8 << 30; // 8 GiB

/// Default flush timeout (ms). Holds INSERTs open across xacts, sealing on a
/// deadline armed at the first row of a fresh INSERT. An explicit `0` keeps the
/// serial emitter's close-on-every-xact behaviour (bootstrap backfill).
pub(crate) const DEFAULT_FLUSH_TIMEOUT_MS: u64 = 1000;

/// Rows coalesced per batcher chunk
pub(crate) const DEFAULT_DECODE_CHUNK_ROWS: usize = 1024;

/// Database of `instance`: `[ch] database` for default, None when unknown
pub fn instance_database<'a>(
    instances: &'a BTreeMap<String, InstanceConfig>,
    default_db: &'a str,
    instance: Option<&str>,
) -> Option<&'a str> {
    match instance {
        None | Some("default") => Some(default_db),
        Some(name) => instances.get(name).map(|c| c.database.as_str()),
    }
}

/// Database for a table without `target_database`: namespace
/// `target_database`, else instance's database, else `[ch] database`
pub fn default_database<'a>(
    namespaces: &'a HashMap<String, NamespaceMapping>,
    instances: &'a BTreeMap<String, InstanceConfig>,
    default_db: &'a str,
    namespace: &str,
    instance: Option<&str>,
) -> &'a str {
    namespaces
        .get(namespace)
        .and_then(|n| n.target_database.as_deref())
        .or_else(|| instance_database(instances, default_db, instance))
        .unwrap_or(default_db)
}

/// Per-replica connection + mapping config. TOML `[ch]` table holds
/// connection params, `[table.<namespace>.<relname>]` blocks declare
/// per-relation mapping; parse via [`EmitterConfig::from_toml_str`].
///
/// `[database.<dbname>.table.<namespace>.<relname>]` names a table in another
/// database of the cluster; entries outside `[database.*]` use `[source] dbname`
#[derive(Debug, Clone)]
pub struct EmitterConfig {
    pub instances: BTreeMap<String, InstanceConfig>,
    pub conn: InstanceConfig,
    /// Wire compression for every destination, so blocks seal once
    pub compression: CompressionChoice,
    /// Custom rustls roots/config for `secure` path: private CA, pinned
    /// self-signed cert, or mTLS. `None` uses public webpki roots via
    /// [`clickhouse_c::tls::default_config`]. Not parsed from TOML;
    /// carried through reconnect + DDL applicator so every CH socket
    /// pins the same roots.
    pub tls_config: Option<Arc<clickhouse_c::tls::rustls::ClientConfig>>,
    pub row_budget: usize,
    pub byte_budget: usize,
    /// Hold INSERTs open across xacts. Deadline arms at the first row of a
    /// fresh INSERT and trips at `now + flush_timeout`; on trip the batcher
    /// seals that block, which advances the durable-LSN horizon once the
    /// insert acks. `Duration::ZERO` (default) takes the pipeline's
    /// `DEFAULT_PIPELINE_FLUSH` (100 ms), not per-xact INSERTs. Latency
    /// cap: a buffered row is at most `flush_timeout` from
    /// sealed, since the batcher sleeps to the nearest deadline rather than
    /// scanning on a `flush_timeout` period. Throughput: small commits
    /// coalesce into one MergeTree part per flush window.
    pub flush_timeout: Duration,
    pub tables: HashMap<RelName, TableMapping>,
    /// Per-table initial-load mode from TOML `[table.*]` blocks. Applies at
    /// boot for pinned mappings; SQL opt-ins carry their own mode.
    pub table_initial_loads: HashMap<RelName, InitialLoadMode>,
    /// Table rules in declaration order
    pub table_entries: Vec<(RelName, MatchKind, TableRule)>,
    /// Column rules in declaration order
    pub column_entries: Vec<ColumnEntry>,
    pub table_opt_ins: HashMap<RelName, TableRow>,
    /// `[stream] paused`: pump idles (stops consuming source WAL) when true.
    /// Live via reload.
    pub paused: bool,
    pub skip_attribution: bool,
    /// `[stream] replicate_all` (default true): replicate every user table in
    /// non-system namespaces. Per-table `replicate = false` still opts out.
    pub replicate_all: bool,
    /// `[stream] auto_create_name_all`: derived-name template for every
    /// namespace, which a `[namespace.*]` entry overrides. `None` is
    /// `$schema$_$table$`. The knob `replicate_all` needs: a namespace with no
    /// entry of its own still auto-creates under it
    pub auto_create_name_all: Option<crate::mapping::NameTemplate>,
    /// Per-namespace defaults keyed on PG schema name; per-table
    /// entries in `tables` win for the relation they name
    pub namespaces: HashMap<String, NamespaceMapping>,
    /// Sorted database names from `[source] dbname` and table or namespace prefixes
    /// Keep same order for every database, bridge socket numbering depends on it
    pub databases: Vec<String>,
    /// Global `--drop-table-strategy` default; per-namespace override
    /// via `[namespace.<ns>] drop_table_strategy = ...`
    pub drop_table_strategy: DropTableStrategy,
    /// `[ch] detach_strategy`: rows a detached partition leaves in its root's
    /// destination, overridable per namespace
    pub detach_strategy: DropTableStrategy,
    /// `[ch] truncate_strategy` defaults to `drop`, which clears ClickHouse
    /// rows after a source `TRUNCATE`
    pub truncate_strategy: DropTableStrategy,
    pub retry: RetryConfig,
    /// A CH connection idle longer than this may be half-open (NAT/LB/CH
    /// idle-reap); reconnect before the next op instead of waiting for TCP
    /// keepalive to fail it. Guards the start of a run, when connections sat
    /// idle since the previous one.
    pub idle_reconnect: Duration,
    /// Keep the delete marker out of `ReplacingMergeTree`'s args so delete
    /// tombstones stay queryable instead of collapsing on FINAL. Column
    /// still emitted; off by default
    pub soft_delete: bool,
    /// `[system_columns]`: cluster-wide names of the columns walshadow appends
    /// per row, and whether the delete marker exists. Boot-only; per-relation
    /// renames layer over it via `table_entries` / `config_table`
    pub system_columns: Arc<SystemColumns>,
    /// Rows coalesced before routing one chunk to the batcher
    /// (`DEFAULT_DECODE_CHUNK_ROWS`). Tunable so
    /// tests can trip the mid-loop flush without a huge xact.
    pub decode_chunk_rows: usize,
    /// Row / byte budget per commit-drain slice
    /// ([`crate::xact::xact_buffer::CommittedDrain::next_batch`]). Bounds decoded
    /// heap rows resident while a spilled xact streams back; TOAST chunk
    /// generations stay per-xact.
    pub drain_batch_rows: usize,
    pub drain_batch_bytes: usize,
    /// `[clickhouse] plan_disk_max`: byte cap per transaction plan spool
    /// file; a larger transaction fails planning instead of filling disk
    pub plan_disk_max: u64,
    /// `[runtime_config] schema`: source-PG schema housing the `config_*`
    /// overlay tables. `None` (field empty or omitted) disables the whole
    /// overlay subsystem — no boot seed, no config_decoder, pure TOML+CLI.
    pub runtime_config_schema: Option<String>,
    /// Source PostgreSQL connection and slot
    pub source: SourceConn,
    /// `[memory] resident_payload_max`: global payload memory limit
    pub resident_payload_max: usize,
    /// `[memory] inline_value_max`: maximum decoded value size. Oversized
    /// values become NULL or fail their transaction, based on
    /// `inline_value_overflow`
    pub inline_value_max: usize,
    /// `[memory] value_reserve`: memory reserved per decoder for one value
    pub value_reserve: usize,
    /// `[memory] inline_value_overflow`: action for oversized values
    pub inline_value_overflow: InlineValueOverflow,
    /// `[ch] decoder_pool_size`: concurrent value resolutions, each holding
    /// `value_reserve` of the memory budget. `--decoder-pool-size`
    /// overrides. Boot-only, the budget is sized at spawn
    pub decoder_pool_size: usize,
    /// `[ch] inserter_pool_size`: concurrent CH INSERT connections (N).
    /// Native is request/response with no pipelining, so N is the
    /// throughput lever against insert RTT — the default of 1 caps a
    /// cross-region destination at roughly one batch per round trip.
    /// `--inserter-pool-size` overrides. Boot-only
    pub inserter_pool_size: usize,
    /// `[ch] decoder_batch_size`: pump-side batch size for the
    /// `QueueingRecordSink`. Bigger amortises per-send overhead but adds
    /// pump→worker latency. `--decoder-batch-size` overrides. Boot-only
    pub decoder_batch_size: usize,
    /// `[ch] decoder_queue_capacity`: soft cap on in-flight records for the
    /// `QueueingRecordSink` feeding the decode/xact-drain worker.
    /// `--decoder-queue-capacity` overrides. Boot-only
    pub decoder_queue_capacity: usize,
    /// `[backup]`: archive storage for object-store bootstrap + WAL refill.
    /// `None` (section omitted) disables refill.
    pub backup: Option<walrus::config::Settings>,
    /// `[bootstrap]`: shadow seeding source + its object-store knobs.
    /// Boot-only, CLI overrides TOML
    pub bootstrap: BootstrapSettings,
    pub toast: ToastSettings,
}

/// Choose bootstrap source for empty shadow data dir
/// Initialized data dir resumes regardless of mode
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum BootstrapMode {
    /// Resume initialized managed shadow; reject empty data dir
    #[default]
    Off,
    /// Source-PG-driven BASE_BACKUP over the replication protocol,
    /// reuses `--host` / `--port` / `--user`, no extra credentials
    Direct,
    /// wal-g-compatible BASE_BACKUP from a `DynStorage` bucket. Storage
    /// config read from `[backup]` in `--ch-config`;
    /// `--bootstrap-backup-name` selects the backup (LATEST = newest sentinel)
    ObjectStore,
}

impl std::str::FromStr for BootstrapMode {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, String> {
        match s.trim().to_ascii_lowercase().as_str() {
            "off" => Ok(Self::Off),
            "direct" => Ok(Self::Direct),
            "object_store" | "object-store" => Ok(Self::ObjectStore),
            other => Err(format!(
                "unknown bootstrap mode `{other}` (expected off / direct / object_store)"
            )),
        }
    }
}

/// `[bootstrap]` table. Every field is `Option` so an omitted key falls
/// through to the CLI flag, then to the built-in default — a set key that
/// the CLI also passes loses to the CLI.
///
/// Keep `shadow_data_dir` CLI-only alongside `--start-lsn` / `--ignore-cursor`
/// as a per-invocation recovery decision
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Deserialize)]
pub struct BootstrapSettings {
    /// `mode`: `off` / `direct` / `object_store`. Validated at parse so a typo
    /// fails at startup rather than silently reading as `off`
    #[serde(default, deserialize_with = "crate::toml_de::de_from_str")]
    pub mode: Option<BootstrapMode>,
    /// `backup_name`: `LATEST` or a literal `base_…` name. The
    /// `base_`-prefix check lives in `bin/stream/bootstrap.rs`, next to the
    /// object-store dispatch that consumes it
    pub backup_name: Option<String>,
    /// `object_store_parallelism`: in-flight data parts. `None` leaves
    /// [`crate::backup_source_object_store::ObjectStoreSource`]'s
    /// `min(4, num_cpus)` clamp in place
    pub object_store_parallelism: Option<NonZeroUsize>,
    /// `lanes`: parallel drain/batcher lanes for the greenfield load
    pub lanes: Option<NonZeroUsize>,
    /// Retry failed table backup loads through source COPY, default true
    pub copy_fallback: Option<bool>,
    /// `copy_chunk_blocks`: heap pages per resumable COPY chunk. Each chunk
    /// proves its rows durable before progress persists, so a restart replays
    /// at most one chunk. `None` uses
    /// [`COPY_CHUNK_BLOCKS`](crate::backfill::copy_backfill::COPY_CHUNK_BLOCKS)
    pub copy_chunk_blocks: Option<NonZeroU32>,
}

/// Where external TOAST values live.
///
/// `Clickhouse` mirrors every chunk into a per-relation `ReplacingMergeTree`
/// keyed by physical tuple location. `Shadow` reads PostgreSQL TOAST heaps and
/// writes no chunks. `Disabled` keeps no store. Values can still be restored
/// from chunks in the same transaction's WAL. Other values become NULL, or
/// target type's default when not Nullable, and increment
/// `toast_values_filled_default`. Switching mode requires fresh bootstrap
/// because each mode stores different history. See `plans/shadow_toast.md`
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToastMode {
    #[default]
    Clickhouse,
    Shadow,
    Disabled,
}

impl ToastMode {
    pub fn is_shadow(self) -> bool {
        matches!(self, ToastMode::Shadow)
    }
}

/// Action for TOAST values larger than `inline_value_max`
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InlineValueOverflow {
    #[default]
    Null,
    Error,
}

/// `[toast]` chunk-store controls, applied at startup
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Deserialize)]
pub struct ToastSettings {
    pub put_batch_rows: Option<NonZeroUsize>,
    pub put_batch_bytes: Option<NonZeroUsize>,
    pub connections: Option<NonZeroUsize>,
    #[serde(default)]
    pub mode: ToastMode,
    /// Start speculative TOAST reads before planning
    pub read_ahead: Option<bool>,
    /// Hold source vacuum horizon for shadow reads, default true
    pub source_horizon_pin: Option<bool>,
}

impl ToastSettings {
    pub fn pins_source_horizon(&self) -> bool {
        self.mode.is_shadow() && self.source_horizon_pin.unwrap_or(true)
    }
}

pub(crate) const DEFAULT_INLINE_VALUE_MAX: usize = 1 << 30;
pub(crate) const DEFAULT_VALUE_RESERVE: usize = 64 << 20;

/// Leave half for shadow PostgreSQL, transaction buffer, and unmetered allocations
const RESIDENT_PAYLOAD_FRACTION: usize = 2;
/// Keep default reserve within half of pool
pub(crate) const MIN_RESIDENT_PAYLOAD_MAX: usize = 512 << 20;

/// Return half of available memory, with enough room for default reserve
pub fn default_resident_payload_max() -> usize {
    crate::budget::host_memory_limit()
        .map(|m| m / RESIDENT_PAYLOAD_FRACTION)
        .unwrap_or(MIN_RESIDENT_PAYLOAD_MAX)
        .max(MIN_RESIDENT_PAYLOAD_MAX)
}

pub const DEFAULT_POOL_FLOOR: usize = 3;

/// A constant, not vCPU-scaled: [`crate::emit::pipeline::leaf_reserve_for`]
/// caps `decoders * value_reserve` at half the resolved `[memory]` budget,
/// so a derived default can refuse boot where this one passes
pub const DEFAULT_DECODER_POOL: usize = DEFAULT_POOL_FLOOR;

/// Round-trip bound, so it tracks vCPUs; also sets
/// `walshadow.bridge_workers`, so it stops where that pool does
pub fn default_inserter_pool() -> usize {
    vcpus().clamp(DEFAULT_POOL_FLOOR, crate::ops::bridge::MAX_BRIDGE_WORKERS)
}

fn vcpus() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(DEFAULT_POOL_FLOOR)
}

pub(crate) const DEFAULT_IDLE_RECONNECT_SECS: u64 = 30;

/// Bounded-retry knobs. Retryable error (IO, clickhouse-c protocol,
/// ServerException) triggers reconnect + retry up to `max_attempts`
/// with exponential backoff capped at `max_backoff`.
#[derive(Debug, Clone, serde::Deserialize)]
#[serde(default)]
pub struct RetryConfig {
    /// Retry budget after initial call, including failed reconnects
    #[serde(rename = "retry_max_attempts")]
    pub max_attempts: u32,
    #[serde(
        rename = "retry_initial_backoff_ms",
        deserialize_with = "crate::toml_de::de_millis"
    )]
    pub initial_backoff: std::time::Duration,
    #[serde(
        rename = "retry_max_backoff_ms",
        deserialize_with = "crate::toml_de::de_millis"
    )]
    pub max_backoff: std::time::Duration,
}

impl RetryConfig {
    pub(crate) fn backoff(&self) -> backon::ExponentialBuilder {
        backon::ExponentialBuilder::default()
            .with_min_delay(self.initial_backoff)
            .with_max_delay(self.max_backoff)
            .with_max_times(self.max_attempts as usize)
    }
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_attempts: 5,
            initial_backoff: std::time::Duration::from_millis(250),
            max_backoff: std::time::Duration::from_secs(10),
        }
    }
}

impl Default for EmitterConfig {
    fn default() -> Self {
        Self::from_sections(
            ChSection::default(),
            MemorySection::default(),
            StreamSection::default(),
        )
    }
}

fn backup_err(msg: impl std::fmt::Display) -> EmitterError {
    EmitterError::Config(format!("[backup] {msg}"))
}

/// `[backup]` fields shared across storage backends
#[derive(Debug, serde::Deserialize)]
struct BackupSection {
    archive: Option<String>,
    region: Option<String>,
    endpoint: Option<String>,
    #[serde(default)]
    force_path_style: bool,
    access_key: Option<String>,
    secret_key: Option<String>,
    session_token: Option<String>,
    credentials_path: Option<String>,
}

fn split_bucket_prefix(rest: &str) -> (String, String) {
    match rest.split_once('/') {
        Some((b, p)) => (b.to_string(), p.trim_end_matches('/').to_string()),
        None => (rest.to_string(), String::new()),
    }
}

/// One `[backup]` storage backend: an `archive` URI prefix and how to build
/// its `walrus` storage config from the `[backup]` table. Add a backend by
/// implementing this and listing it in [`parse_backup`].
trait BackupBackend {
    fn prefix(&self) -> &'static str;
    fn build(
        &self,
        rest: &str,
        bk: &BackupSection,
    ) -> Result<walrus::config::StorageSettings, EmitterError>;
}

struct S3Backend;
impl BackupBackend for S3Backend {
    fn prefix(&self) -> &'static str {
        "s3://"
    }
    fn build(
        &self,
        rest: &str,
        bk: &BackupSection,
    ) -> Result<walrus::config::StorageSettings, EmitterError> {
        use walrus::storage::s3::{CredentialSource, Credentials, ImdsProvider, S3Config};
        let (bucket, prefix) = split_bucket_prefix(rest);
        let creds = match (bk.access_key.clone(), bk.secret_key.clone()) {
            (Some(access_key), Some(secret_key)) => CredentialSource::Static(Credentials {
                access_key,
                secret_key,
                session_token: bk.session_token.clone(),
                expires_at: None,
            }),
            (None, None) => CredentialSource::Imds(Arc::new(
                ImdsProvider::new(None).map_err(|e| backup_err(format!("imds: {e}")))?,
            )),
            _ => {
                return Err(backup_err(
                    "set both access_key and secret_key, or neither (IMDS)",
                ));
            }
        };
        Ok(walrus::config::StorageSettings::S3(S3Config {
            bucket,
            prefix,
            region: bk.region.clone().unwrap_or_else(|| "us-east-1".into()),
            creds,
            endpoint: bk.endpoint.clone(),
            force_path_style: bk.force_path_style,
        }))
    }
}

struct GcsBackend;
impl BackupBackend for GcsBackend {
    fn prefix(&self) -> &'static str {
        "gs://"
    }
    fn build(
        &self,
        rest: &str,
        bk: &BackupSection,
    ) -> Result<walrus::config::StorageSettings, EmitterError> {
        let (bucket, prefix) = split_bucket_prefix(rest);
        Ok(walrus::config::StorageSettings::Gcs(
            walrus::storage::gcs::GcsConfig {
                bucket,
                prefix,
                credentials_path: bk.credentials_path.clone(),
                endpoint: bk.endpoint.clone(),
            },
        ))
    }
}

struct FsBackend;
impl BackupBackend for FsBackend {
    fn prefix(&self) -> &'static str {
        "file://"
    }
    fn build(
        &self,
        rest: &str,
        _bk: &BackupSection,
    ) -> Result<walrus::config::StorageSettings, EmitterError> {
        Ok(walrus::config::StorageSettings::Fs {
            path: rest.to_string(),
        })
    }
}

/// `[backup]` table → `walrus::config::Settings`. The `archive` URI prefix
/// dispatches to the matching [`BackupBackend`].
fn parse_backup(bk: &BackupSection) -> Result<walrus::config::Settings, EmitterError> {
    let backends: [&dyn BackupBackend; 3] = [&S3Backend, &GcsBackend, &FsBackend];
    let archive = bk
        .archive
        .clone()
        .ok_or_else(|| backup_err("archive required (s3://, gs://, or file://)"))?;
    for backend in backends {
        if let Some(rest) = archive.strip_prefix(backend.prefix()) {
            return Ok(walrus::config::Settings {
                storage: backend.build(rest, bk)?,
                ..walrus::config::Settings::default()
            });
        }
    }
    Err(backup_err(format!(
        "archive {archive:?} must start with s3://, gs://, or file://"
    )))
}

impl ConnectionConfig for EmitterConfig {
    fn host(&self) -> &str {
        &self.conn.host
    }

    fn port(&self) -> u16 {
        self.conn.port
    }

    fn database(&self) -> &str {
        &self.conn.database
    }

    fn user(&self) -> &str {
        &self.conn.user
    }

    fn password(&self) -> &str {
        &self.conn.password
    }

    fn secure(&self) -> bool {
        self.conn.secure
    }

    fn tls_server_name(&self) -> &str {
        &self.conn.tls_server_name
    }

    fn tls_config(&self) -> Option<Arc<clickhouse_c::tls::rustls::ClientConfig>> {
        self.tls_config.clone()
    }

    fn compression(&self) -> CompressionChoice {
        self.compression
    }

    fn idle_reconnect(&self) -> Duration {
        self.idle_reconnect
    }
}

#[derive(serde::Deserialize)]
struct ConfigDocument {
    #[serde(default)]
    ch: ChSection,
    #[serde(default)]
    memory: MemorySection,
    #[serde(default)]
    runtime_config: RuntimeConfigPatch,
    #[serde(default)]
    stream: StreamSection,
    #[serde(default)]
    system_columns: SystemColumns,
    #[serde(default)]
    source: SourceConn,
    backup: Option<BackupSection>,
    #[serde(default)]
    bootstrap: BootstrapSettings,
    #[serde(default)]
    toast: ToastSettings,
    /// `[namespace.<schema>]`
    #[serde(default)]
    namespace: BTreeMap<String, NamespacePatch>,
    /// `[table.<schema>.<relname>]`
    #[serde(default)]
    table: BTreeMap<String, BTreeMap<String, TablePatch>>,
    /// Other source databases, `[database.<dbname>.table.<schema>.<relname>]`
    #[serde(default)]
    database: BTreeMap<String, DbEntries>,
}

/// Schema and table settings for one source database
#[derive(Default, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct DbEntries {
    #[serde(default)]
    namespace: BTreeMap<String, NamespacePatch>,
    #[serde(default)]
    table: BTreeMap<String, BTreeMap<String, TablePatch>>,
}

/// Return `(namespace, relname, settings)` entries for source database `dbname`
/// Unprefixed entries belong to `[source] dbname`, as in
/// [`EmitterConfig::for_database`]; leave validation to
/// [`EmitterConfig::from_table`]
pub fn table_entries_for<'t>(
    root: &'t toml::Table,
    dbname: &str,
) -> Vec<(&'t str, &'t str, &'t toml::Table)> {
    let unprefixed = root
        .get("source")
        .and_then(toml::Value::as_table)
        .and_then(|t| t.get("dbname"))
        .and_then(toml::Value::as_str)
        .unwrap_or_default()
        == dbname;
    let prefixed = root
        .get("database")
        .and_then(toml::Value::as_table)
        .and_then(|dbs| dbs.get(dbname))
        .and_then(toml::Value::as_table)
        .and_then(|db| db.get("table"));
    let mut out = Vec::new();
    for schemas in unprefixed
        .then(|| root.get("table"))
        .flatten()
        .into_iter()
        .chain(prefixed)
        .filter_map(toml::Value::as_table)
    {
        for (ns, value) in schemas {
            let Some(rels) = value.as_table() else {
                continue;
            };
            out.extend(
                rels.iter()
                    .filter_map(|(rel, v)| v.as_table().map(|t| (ns.as_str(), rel.as_str(), t))),
            );
        }
    }
    out
}

fn duplicate_entry(ctx: &str) -> EmitterError {
    EmitterError::Config(format!(
        "`{ctx}` is configured twice, with and without a `database` prefix"
    ))
}

/// Fold entries that name no database into the ones `[source] dbname` owns
fn fold_unprefixed(
    into: &mut DbEntries,
    namespace: BTreeMap<String, NamespacePatch>,
    table: BTreeMap<String, BTreeMap<String, TablePatch>>,
) -> Result<(), EmitterError> {
    for (ns, patch) in namespace {
        if into.namespace.insert(ns.clone(), patch).is_some() {
            return Err(duplicate_entry(&format!("namespace.{ns}")));
        }
    }
    for (ns, rels) in table {
        let schema = into.table.entry(ns.clone()).or_default();
        for (rel, patch) in rels {
            if schema.insert(rel.clone(), patch).is_some() {
                return Err(duplicate_entry(&format!("table.{ns}.{rel}")));
            }
        }
    }
    Ok(())
}

/// A cluster backup walks one database's pages, so a load sourced from one
/// covers `[source] dbname` alone. Reject the other databases' requests where
/// the operator writes them, not after a pass starts
fn reject_backup_loads(db: &str, entries: &DbEntries) -> Result<(), EmitterError> {
    let backup = |mode: Option<InitialLoadMode>| {
        mode.filter(|m| {
            matches!(
                m,
                InitialLoadMode::BaseBackup | InitialLoadMode::ObjectStore
            )
        })
    };
    let refuse = |ctx: String, mode: InitialLoadMode| {
        Err(EmitterError::Config(format!(
            "`{ctx}`: initial_load = \"{}\" loads from a cluster backup, which \
             covers only the database `[source] dbname` names; use \"copy\" for \
             `{db}`",
            mode.as_str()
        )))
    };
    for (ns, patch) in &entries.namespace {
        if let Some(mode) = backup(patch.initial_load) {
            return refuse(format!("database.{db}.namespace.{ns}"), mode);
        }
    }
    for (ns, rels) in &entries.table {
        for (rel, patch) in rels {
            if let Some(mode) = backup(patch.initial_load) {
                return refuse(format!("database.{db}.table.{ns}.{rel}"), mode);
            }
        }
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct InstanceConfig {
    pub host: String,
    pub port: u16,
    pub database: String,
    pub user: String,
    pub password: String,
    /// Wrap native protocol in TLS (rustls, public webpki roots). Set
    /// for ClickHouse Cloud, whose secure native port (9440) speaks
    /// native-over-TLS. SNI + cert verification key off `tls_server_name`,
    /// else `host`.
    pub secure: bool,
    /// Name sent as TLS SNI and matched against the server certificate
    /// when `secure`. Lets `host` be an IP or another DNS name while the
    /// handshake still names what the certificate was issued for. Empty
    /// uses `host`.
    pub tls_server_name: String,
}

impl Default for InstanceConfig {
    fn default() -> Self {
        let ch = ChSection::default();
        Self {
            host: ch.host,
            port: ch.port,
            database: ch.database,
            user: ch.user,
            password: ch.password,
            secure: ch.secure,
            tls_server_name: ch.tls_server_name,
        }
    }
}

impl InstanceConfig {
    pub fn apply(&self, base: &EmitterConfig) -> EmitterConfig {
        EmitterConfig {
            conn: self.clone(),
            ..base.clone()
        }
    }
}

#[derive(serde::Deserialize)]
#[serde(default)]
struct ChSection {
    instances: BTreeMap<String, InstanceConfig>,
    host: String,
    port: u16,
    database: String,
    user: String,
    password: String,
    secure: bool,
    tls_server_name: String,
    #[serde(deserialize_with = "crate::toml_de::de_parse")]
    compression: CompressionChoice,
    row_budget: usize,
    byte_budget: usize,
    drain_batch_rows: usize,
    drain_batch_bytes: usize,
    plan_disk_max: u64,
    /// Leave zero for bin/stream/args.rs to clamp
    decoder_pool_size: usize,
    inserter_pool_size: usize,
    decoder_batch_size: usize,
    decoder_queue_capacity: usize,
    #[serde(
        rename = "flush_timeout_ms",
        deserialize_with = "crate::toml_de::de_millis"
    )]
    flush_timeout: Duration,
    #[serde(flatten)]
    retry: RetryConfig,
    #[serde(deserialize_with = "crate::toml_de::de_parse")]
    drop_table_strategy: DropTableStrategy,
    #[serde(deserialize_with = "crate::toml_de::de_parse")]
    detach_strategy: DropTableStrategy,
    #[serde(deserialize_with = "crate::toml_de::de_parse")]
    truncate_strategy: DropTableStrategy,
    soft_delete: bool,
}

impl Default for ChSection {
    fn default() -> Self {
        Self {
            instances: BTreeMap::new(),
            host: "localhost".into(),
            port: 9000,
            database: "default".into(),
            user: "default".into(),
            password: String::new(),
            secure: false,
            tls_server_name: String::new(),
            compression: CompressionChoice::default(),
            row_budget: DEFAULT_ROW_BUDGET,
            byte_budget: DEFAULT_BYTE_BUDGET,
            drain_batch_rows: DEFAULT_DRAIN_BATCH_ROWS,
            drain_batch_bytes: DEFAULT_DRAIN_BATCH_BYTES,
            plan_disk_max: DEFAULT_PLAN_DISK_MAX,
            decoder_pool_size: DEFAULT_DECODER_POOL,
            inserter_pool_size: default_inserter_pool(),
            decoder_batch_size: DEFAULT_QUEUEING_BATCH_SIZE,
            decoder_queue_capacity: DEFAULT_QUEUEING_RECORD_SINK_CAPACITY,
            flush_timeout: Duration::from_millis(DEFAULT_FLUSH_TIMEOUT_MS),
            retry: RetryConfig::default(),
            drop_table_strategy: DropTableStrategy::default(),
            detach_strategy: DropTableStrategy::default(),
            truncate_strategy: DropTableStrategy::Drop,
            soft_delete: false,
        }
    }
}

#[derive(serde::Deserialize)]
#[serde(default)]
struct MemorySection {
    resident_payload_max: usize,
    inline_value_max: usize,
    value_reserve: usize,
    inline_value_overflow: InlineValueOverflow,
}

impl Default for MemorySection {
    fn default() -> Self {
        Self {
            resident_payload_max: default_resident_payload_max(),
            inline_value_max: DEFAULT_INLINE_VALUE_MAX,
            value_reserve: DEFAULT_VALUE_RESERVE,
            inline_value_overflow: InlineValueOverflow::default(),
        }
    }
}

#[derive(Default, serde::Deserialize)]
struct RuntimeConfigPatch {
    #[serde(default, deserialize_with = "crate::toml_de::de_nonempty")]
    schema: Option<String>,
}

#[derive(serde::Deserialize)]
#[serde(default)]
struct StreamSection {
    paused: bool,
    skip_attribution: bool,
    replicate_all: bool,
    auto_create_name_all: Option<String>,
}

impl Default for StreamSection {
    fn default() -> Self {
        Self {
            paused: false,
            skip_attribution: false,
            replicate_all: true,
            auto_create_name_all: None,
        }
    }
}

#[derive(Default, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct NamespacePatch {
    target_database: Option<String>,
    auto_create: Option<bool>,
    auto_create_name: Option<String>,
    #[serde(default, deserialize_with = "crate::toml_de::de_from_str")]
    drop_table_strategy: Option<DropTableStrategy>,
    #[serde(default, deserialize_with = "crate::toml_de::de_from_str")]
    detach_strategy: Option<DropTableStrategy>,
    #[serde(default, deserialize_with = "crate::toml_de::de_from_str")]
    initial_load: Option<InitialLoadMode>,
}

#[derive(Default, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct TablePatch {
    target_instance: Option<String>,
    replicate: Option<bool>,
    target_database: Option<String>,
    target_table: Option<String>,
    #[serde(default, deserialize_with = "crate::toml_de::de_from_str")]
    initial_load: Option<InitialLoadMode>,
    order_by: Option<Vec<String>>,
    primary_key: Option<Vec<String>>,
    engine: Option<String>,
    partition_by: Option<String>,
    #[serde(default, deserialize_with = "crate::toml_de::de_from_str")]
    route: Option<PartitionRoute>,
    tee: Option<Vec<crate::mapping::Tee>>,
    /// Same keys as `[system_columns]`, for this entry's relations alone
    lsn: Option<String>,
    xid: Option<String>,
    commit_ts: Option<String>,
    #[serde(default, deserialize_with = "crate::mapping::de_marker_override")]
    is_deleted: Option<String>,
    #[serde(default, deserialize_with = "crate::mapping::de_leaf_override")]
    leaf: Option<String>,
    #[serde(
        rename = "match",
        default,
        deserialize_with = "crate::toml_de::de_from_str"
    )]
    match_kind: Option<MatchKind>,
    #[serde(default)]
    columns: Vec<ColumnPatch>,
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct ColumnPatch {
    attnum: Option<i16>,
    name: Option<String>,
    target: Option<String>,
    #[serde(rename = "type")]
    target_type: Option<String>,
    #[serde(
        rename = "match",
        default,
        deserialize_with = "crate::toml_de::de_from_str"
    )]
    match_kind: Option<MatchKind>,
    #[serde(default, deserialize_with = "crate::toml_de::de_from_str")]
    nan: Option<Substitute>,
    #[serde(default, deserialize_with = "crate::toml_de::de_from_str")]
    pos_inf: Option<Substitute>,
    #[serde(default, deserialize_with = "crate::toml_de::de_from_str")]
    neg_inf: Option<Substitute>,
}

impl EmitterConfig {
    fn from_sections(ch: ChSection, memory: MemorySection, stream: StreamSection) -> Self {
        Self {
            instances: ch.instances,
            conn: InstanceConfig {
                host: ch.host,
                port: ch.port,
                database: ch.database,
                user: ch.user,
                password: ch.password,
                secure: ch.secure,
                tls_server_name: ch.tls_server_name,
            },
            compression: ch.compression,
            tls_config: None,
            row_budget: ch.row_budget,
            byte_budget: ch.byte_budget,
            flush_timeout: ch.flush_timeout,
            tables: HashMap::new(),
            table_initial_loads: HashMap::new(),
            table_entries: Vec::new(),
            column_entries: Vec::new(),
            table_opt_ins: HashMap::new(),
            paused: stream.paused,
            skip_attribution: stream.skip_attribution,
            replicate_all: stream.replicate_all,
            auto_create_name_all: None,
            namespaces: HashMap::new(),
            databases: Vec::new(),
            drop_table_strategy: ch.drop_table_strategy,
            detach_strategy: ch.detach_strategy,
            truncate_strategy: ch.truncate_strategy,
            retry: ch.retry,
            idle_reconnect: Duration::from_secs(DEFAULT_IDLE_RECONNECT_SECS),
            soft_delete: ch.soft_delete,
            system_columns: Arc::default(),
            decode_chunk_rows: DEFAULT_DECODE_CHUNK_ROWS,
            drain_batch_rows: ch.drain_batch_rows,
            drain_batch_bytes: ch.drain_batch_bytes,
            plan_disk_max: ch.plan_disk_max,
            runtime_config_schema: None,
            source: SourceConn::default(),
            resident_payload_max: memory.resident_payload_max,
            inline_value_max: memory.inline_value_max,
            value_reserve: memory.value_reserve,
            inline_value_overflow: memory.inline_value_overflow,
            decoder_pool_size: ch.decoder_pool_size,
            inserter_pool_size: ch.inserter_pool_size,
            decoder_batch_size: ch.decoder_batch_size,
            decoder_queue_capacity: ch.decoder_queue_capacity,
            backup: None,
            bootstrap: BootstrapSettings::default(),
            toast: ToastSettings::default(),
        }
    }

    pub fn instance_database(&self, instance: Option<&str>) -> Result<&str, EmitterError> {
        instance_database(&self.instances, &self.conn.database, instance).ok_or_else(|| {
            EmitterError::Config(format!(
                "unknown ClickHouse instance `{}`",
                instance.unwrap_or_default()
            ))
        })
    }

    /// Boot-only row-shape knobs frozen into every route
    pub fn row_policy(&self) -> crate::mapping::RowPolicy {
        crate::mapping::RowPolicy {
            soft_delete: self.soft_delete,
            system: self.system_columns.clone(),
        }
    }

    /// Parse a TOML config of the shape:
    ///
    /// ```toml
    /// [ch]
    /// host = "ch.example.com"
    /// port = 9000
    /// database = "default"
    /// user = "default"
    /// password = ""
    /// compression = "lz4"   # one of none / lz4 / zstd
    ///
    /// [system_columns]      # optional: rename what walshadow appends per row
    /// lsn = "_lsn"
    /// xid = "_xid"
    /// commit_ts = "_commit_ts"
    /// is_deleted = "_is_deleted"   # false drops the marker (and DELETE rows)
    ///
    /// [table.public.foo]     # [table.<namespace>.<relname>], quote weird names
    /// replicate = true
    /// initial_load = "none"  # one of none / copy / base_backup / object_store
    /// target_database = "default"  # optional: namespace override, else [ch] database
    /// target_table = "foo"         # optional: source relname
    /// order_by = ["id"]            # optional: CH ORDER BY, else replica identity
    /// primary_key = ["id"]         # optional: index prefix of order_by
    /// engine = "MergeTree"         # optional: CH engine, else ReplacingMergeTree
    /// tee = [{ table = "foo_audit", engine = "MergeTree" }]  # optional: extra destinations
    /// lsn = "_peerdb_version"      # optional: per-relation system column
    /// is_deleted = false           # renames, same keys as [system_columns]
    /// columns = [
    ///   { attnum = 1, target = "id",   type = "UInt64" },
    ///   { attnum = 2, target = "name", type = "Nullable(String)" },
    /// ]
    /// ```
    pub fn from_toml_str(s: &str) -> Result<Self, EmitterError> {
        let root: toml::Table = toml::from_str(s).map_err(crate::toml_de::config_error)?;
        Self::from_table(&root)
    }

    /// Build mappings for `[source] dbname` from parsed and merged TOML
    pub fn from_table(root: &toml::Table) -> Result<Self, EmitterError> {
        Self::from_table_for(root, None)
    }

    /// Build mappings for one source database
    /// Share connection, insert, memory, bootstrap, and TOAST settings across databases
    pub fn for_database(root: &toml::Table, dbname: &str) -> Result<Self, EmitterError> {
        Self::from_table_for(root, Some(dbname))
    }

    fn from_table_for(root: &toml::Table, scope: Option<&str>) -> Result<Self, EmitterError> {
        let doc: ConfigDocument = toml::Value::Table(root.clone())
            .try_into()
            .map_err(crate::toml_de::config_error)?;
        let prefix_all = doc.stream.auto_create_name_all.clone();
        let mut out = Self::from_sections(doc.ch, doc.memory, doc.stream);
        for (name, instance) in &out.instances {
            if name.is_empty() || name == "default" {
                return Err(EmitterError::Config(format!(
                    "ch.instances.{name}: reserved instance name"
                )));
            }
            if instance.host.is_empty() || instance.port == 0 {
                return Err(EmitterError::Config(format!(
                    "ch.instances.{name}: host and port must be nonempty"
                )));
            }
        }
        out.auto_create_name_all = prefix_all
            .as_deref()
            .map(crate::mapping::NameTemplate::parse)
            .transpose()
            .map_err(|e| EmitterError::Config(format!("`stream.auto_create_name_all`: {e}")))?;
        doc.system_columns
            .validate()
            .map_err(EmitterError::Config)?;
        out.system_columns = Arc::new(doc.system_columns);
        out.runtime_config_schema = doc.runtime_config.schema;
        out.source = doc.source;
        // Unprefixed entries belong to `[source] dbname` whatever database
        // this parse is scoped to, so a prefixed entry for another database
        // never collides with them
        let default_db = out.source.dbname.clone();
        let scope = scope.unwrap_or(&default_db).to_owned();
        out.source.dbname = scope.clone();
        if let Some(bk) = &doc.backup {
            out.backup = Some(parse_backup(bk)?);
        }
        out.bootstrap = doc.bootstrap;
        out.toast = doc.toast;
        let mut buckets = doc.database;
        if let Some(db) = buckets.keys().next()
            && default_db.is_empty()
        {
            return Err(EmitterError::Config(format!(
                "`database.{db}` names another source database, set `dbname` in \
                 `[source]` to select this process's own"
            )));
        }
        fold_unprefixed(
            buckets.entry(default_db.clone()).or_default(),
            doc.namespace,
            doc.table,
        )?;
        for (db, entries) in buckets.iter().filter(|(db, _)| **db != default_db) {
            reject_backup_loads(db, entries)?;
        }
        let local = buckets.remove(&scope).unwrap_or_default();
        for (ns, n) in local.namespace {
            let auto_create_name = n
                .auto_create_name
                .as_deref()
                .map(crate::mapping::NameTemplate::parse)
                .transpose()
                .map_err(|e| {
                    EmitterError::Config(format!("`namespace.{ns}.auto_create_name`: {e}"))
                })?;
            out.namespaces.insert(
                ns,
                NamespaceMapping {
                    target_database: n.target_database,
                    auto_create: n.auto_create.unwrap_or(false),
                    auto_create_name,
                    drop_table_strategy: n.drop_table_strategy,
                    detach_strategy: n.detach_strategy,
                    initial_load: n.initial_load,
                },
            );
        }
        for (ns, rels) in local.table {
            for (name, t) in rels {
                out.fold_table_entry(&ns, name, t)?;
            }
        }
        for db in buckets.into_keys() {
            out.databases.push(db);
        }
        out.databases
            .extend([default_db, scope].into_iter().filter(|db| !db.is_empty()));
        out.databases.sort();
        out.databases.dedup();
        Ok(out)
    }

    /// Apply table and column settings for this process's source database
    fn fold_table_entry(
        &mut self,
        ns: &str,
        name: String,
        mut t: TablePatch,
    ) -> Result<(), EmitterError> {
        let rel = RelName::new(ns, &name);
        let ctx = format!("table.{ns}.{name}");
        let kind = t.match_kind.unwrap_or(MatchKind::Exact);
        self.instance_database(t.target_instance.as_deref())?;
        for tee in t.tee.iter_mut().flatten() {
            let database = self.instance_database(tee.instance.as_deref())?;
            if tee.instance.is_some() && tee.database.is_none() {
                tee.database = Some(database.to_owned());
            }
        }
        let replicate = t.replicate;
        let system = SystemColumnNames {
            lsn: t.lsn,
            xid: t.xid,
            commit_ts: t.commit_ts,
            is_deleted: t.is_deleted,
            leaf: t.leaf,
        };
        system.validate(&ctx).map_err(EmitterError::Config)?;
        if t.tee.is_some() && kind != MatchKind::Exact {
            return Err(EmitterError::Config(format!(
                "{ctx}.tee: a `match = \"{}\"` entry would merge every matched table into one tee",
                kind.as_str()
            )));
        }
        let initial_load = t.initial_load;
        let rule = TableRule {
            target_instance: t.target_instance,
            system,
            target_database: t.target_database,
            target_table: t.target_table,
            replicate,
            initial_load: t.initial_load.map(|m| m.as_str().to_string()),
            order_by: t.order_by,
            primary_key: t.primary_key,
            engine: t.engine,
            partition_by: t.partition_by,
            route: t.route,
            tee: t.tee,
        };
        self.table_entries.push((rel.clone(), kind, rule.clone()));
        let mut pinned = Vec::new();
        let mut named = Vec::new();
        for (i, c) in t.columns.into_iter().enumerate() {
            let ctx = format!("{ctx}.columns[{i}]");
            let att_kind = c.match_kind.unwrap_or(MatchKind::Exact);
            match (c.attnum, c.name) {
                (Some(_), Some(_)) => {
                    return Err(EmitterError::Config(format!(
                        "{ctx}: attnum and name are alternatives, not both"
                    )));
                }
                (None, None) => {
                    return Err(EmitterError::Config(format!(
                        "{ctx}: missing attnum or name"
                    )));
                }
                (Some(src_attnum), None) => {
                    if c.match_kind.is_some() {
                        return Err(EmitterError::Config(format!(
                            "{ctx}.match: an attnum entry names one column already"
                        )));
                    }
                    pinned.push(ColumnMapping {
                        src_attnum,
                        target_name: c.target.ok_or_else(|| {
                            EmitterError::Config(format!("{ctx}: missing target"))
                        })?,
                        target_type: c
                            .target_type
                            .ok_or_else(|| EmitterError::Config(format!("{ctx}: missing type")))?,
                        type_pinned: true,
                    });
                }
                (None, Some(attname)) => {
                    if c.target.is_some() && att_kind != MatchKind::Exact {
                        return Err(EmitterError::Config(format!(
                            "{ctx}.target: a `match = \"{}\"` entry can name \
                                 several columns, which cannot share one target",
                            att_kind.as_str()
                        )));
                    }
                    let rule = ColumnRule {
                        target_name: c.target,
                        target_type: c.target_type,
                        nan: c.nan,
                        pos_inf: c.pos_inf,
                        neg_inf: c.neg_inf,
                    };
                    if rule.is_empty() {
                        return Err(EmitterError::Config(format!("{ctx}: sets nothing")));
                    }
                    named.push(ColumnEntry {
                        rel: rel.clone(),
                        rel_kind: kind,
                        attname,
                        att_kind,
                        rule,
                    });
                }
            }
        }
        if !pinned.is_empty() && !named.is_empty() {
            return Err(EmitterError::Config(format!(
                "{ctx}.columns: attnum entries pin the whole projection, so a \
                     name entry beside them would never apply"
            )));
        }
        self.column_entries.append(&mut named);
        if kind != MatchKind::Exact {
            if !pinned.is_empty() {
                return Err(EmitterError::Config(format!(
                    "{ctx}.columns: a `match = \"{}\"` entry cannot pin attnums; \
                         key the entries on `name` instead",
                    kind.as_str()
                )));
            }
            return Ok(());
        }
        if pinned.is_empty() {
            self.table_opt_ins.insert(
                rel,
                TableRow {
                    target_database: rule.target_database,
                    target_table: rule.target_table,
                    replicate,
                    initial_load: rule.initial_load,
                    ..TableRow::default()
                },
            );
            return Ok(());
        }
        if replicate == Some(false) {
            return Ok(());
        }
        let database = rule.target_database.unwrap_or_else(|| {
            default_database(
                &self.namespaces,
                &self.instances,
                &self.conn.database,
                ns,
                rule.target_instance.as_deref(),
            )
            .to_owned()
        });
        let table = rule.target_table.unwrap_or_else(|| {
            self.namespaces
                .get(ns)
                .and_then(|n| n.auto_create_name.clone())
                .or_else(|| self.auto_create_name_all.clone())
                .unwrap_or_default()
                .render(&self.source.dbname, ns, &name)
        });
        self.tables.insert(
            rel.clone(),
            TableMapping {
                target: TableTarget {
                    instance: rule.target_instance.and_then(named_instance),
                    database,
                    table,
                },
                columns: pinned,
                tees: rule.tee.unwrap_or_default(),
            },
        );
        if let Some(mode) = initial_load {
            self.table_initial_loads.insert(rel, mode);
        }
        Ok(())
    }
}

/// Load `--ch-config` and deep-merge every `*.toml` in the sibling conf.d
/// directory (`<ch-config>.d/`, e.g. `ch-config.toml` → `ch-config.d/`), in
/// lexical filename order (later wins) — like Postgres `include_dir`. The base
/// file may be absent (empty table); a malformed fragment is a hard error.
pub async fn load_merged(ch_config: &std::path::Path) -> Result<toml::Table, EmitterError> {
    load_merged_with(ch_config, None).await
}

/// [`load_merged`] taking `over`'s table in place of that fragment file,
/// whether or not the file exists yet
pub async fn load_merged_with(
    ch_config: &std::path::Path,
    over: Option<(&std::path::Path, &toml::Table)>,
) -> Result<toml::Table, EmitterError> {
    let mut root: toml::Table = match tokio::fs::read_to_string(ch_config).await {
        Ok(s) => toml::from_str(&s).map_err(|e| {
            EmitterError::Config(format!(
                "parse {}: {}",
                ch_config.display(),
                crate::toml_de::message(e)
            ))
        })?,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => toml::Table::new(),
        Err(e) => {
            return Err(EmitterError::Config(format!(
                "read {}: {e}",
                ch_config.display()
            )));
        }
    };
    let dir = ch_config.with_extension("d");
    let mut frags: Vec<std::path::PathBuf> = Vec::new();
    if let Ok(mut rd) = tokio::fs::read_dir(&dir).await {
        while let Ok(Some(ent)) = rd.next_entry().await {
            let p = ent.path();
            if p.extension().and_then(|e| e.to_str()) == Some("toml") {
                frags.push(p);
            }
        }
    }
    if let Some((p, _)) = over
        && !frags.iter().any(|f| f == p)
    {
        frags.push(p.to_path_buf());
    }
    frags.sort();
    for p in frags {
        if let Some((_, table)) = over.filter(|(o, _)| *o == p) {
            merge_tables(&mut root, table.clone());
            continue;
        }
        let s = tokio::fs::read_to_string(&p)
            .await
            .map_err(|e| EmitterError::Config(format!("read {}: {e}", p.display())))?;
        let frag: toml::Table = toml::from_str(&s).map_err(|e| {
            EmitterError::Config(format!(
                "parse {}: {}",
                p.display(),
                crate::toml_de::message(e)
            ))
        })?;
        merge_tables(&mut root, frag);
    }
    Ok(root)
}

/// Recursive deep-merge: table-vs-table recurses; any other value from `over`
/// overwrites `base`.
pub fn merge_tables(base: &mut toml::Table, over: toml::Table) {
    for (k, v) in over {
        match (base.get_mut(&k), v) {
            (Some(toml::Value::Table(bt)), toml::Value::Table(ot)) => merge_tables(bt, ot),
            (_, v) => {
                base.insert(k, v);
            }
        }
    }
}

/// Effective config: `base` (e.g. the daemon's CLI-arg source defaults) with
/// the on-disk `--ch-config` + conf.d merged over it. Single resolution point
/// shared by the daemon session and the control surface.
pub async fn load_effective(
    ch_config: &std::path::Path,
    base: toml::Table,
) -> Result<toml::Table, EmitterError> {
    let mut root = base;
    merge_tables(&mut root, load_merged(ch_config).await?);
    Ok(root)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backfill::copy_backfill::COPY_CHUNK_BLOCKS;
    use backon::BackoffBuilder;

    #[test]
    fn retry_backoff_preserves_delay_policy() {
        for (initial, cap, expected) in [
            (1, 3, [1, 2, 3, 3]),
            (4, 3, [4, 3, 3, 3]),
            (1, 0, [1, 0, 0, 0]),
            (0, 3, [0, 0, 0, 0]),
        ] {
            let retry = RetryConfig {
                max_attempts: 4,
                initial_backoff: Duration::from_secs(initial),
                max_backoff: Duration::from_secs(cap),
            };
            assert_eq!(
                retry.backoff().build().collect::<Vec<_>>(),
                expected.map(Duration::from_secs)
            );
        }
    }

    #[test]
    fn namespace_auto_create_name_parses() {
        let parse = |toml: &str| {
            EmitterConfig::from_toml_str(toml)
                .expect("parses")
                .namespaces
                .get("public")
                .expect("namespace parsed")
                .auto_create_name
                .clone()
        };
        let rendered = |toml: &str| {
            parse(toml)
                .unwrap_or_default()
                .render("app", "public", "orders")
        };
        assert_eq!(
            parse("[ch]\n[namespace.public]\nauto_create = true\n"),
            None,
            "an absent template derives from the namespace name",
        );
        for (template, want) in [
            ("", "orders"),
            ("wh_", "wh_orders"),
            ("$schema$_", "public_orders"),
            ("$table$_v2", "orders_v2"),
            ("$database$_$schema$_$table$", "app_public_orders"),
            ("$$lit$$_$table$", "$lit$_orders"),
        ] {
            let toml = format!(
                "[ch]\n[namespace.public]\nauto_create = true\nauto_create_name = \"{template}\"\n"
            );
            assert_eq!(rendered(&toml), want, "template {template:?}");
        }
        for bad in ["$nope$_$table$", "$table$_$unclosed"] {
            let toml = format!(
                "[ch]\n[namespace.public]\nauto_create = true\nauto_create_name = \"{bad}\"\n"
            );
            let err = EmitterConfig::from_toml_str(&toml).expect_err("rejects");
            let msg = err.to_string();
            assert!(
                msg.contains("namespace.public.auto_create_name"),
                "{bad:?} names the key: {msg}"
            );
        }
    }

    #[test]
    fn defaults_match_high_throughput_profile() {
        let c = EmitterConfig::from_toml_str("[ch]\nhost = \"h\"\n").expect("parses");
        assert_eq!(c.row_budget, 4_194_304);
        assert_eq!(c.byte_budget, 256 << 20);
        assert_eq!(c.flush_timeout, Duration::from_millis(1000));
        assert_eq!(c.decoder_pool_size, DEFAULT_DECODER_POOL);
        assert_eq!(c.inserter_pool_size, default_inserter_pool());
        assert!(
            (DEFAULT_POOL_FLOOR..=crate::ops::bridge::MAX_BRIDGE_WORKERS)
                .contains(&c.inserter_pool_size),
            "inserter pool must stay within the bridge pool it sizes",
        );
        assert_eq!(c.decoder_batch_size, 512);
        assert_eq!(c.decoder_queue_capacity, 131_072);
        assert!(c.replicate_all);
    }

    #[test]
    fn stream_auto_create_name_all_parses_and_rejects() {
        let c = EmitterConfig::from_toml_str(
            "[ch]\nhost = \"h\"\n[stream]\nauto_create_name_all = \"$database$_$schema$_\"\n",
        )
        .expect("parses");
        assert_eq!(
            c.auto_create_name_all
                .expect("template parsed")
                .render("app", "audit", "orders"),
            "app_audit_orders",
        );
        assert!(
            EmitterConfig::from_toml_str("[ch]\nhost = \"h\"\n")
                .expect("parses")
                .auto_create_name_all
                .is_none(),
            "absent leaves the built-in default",
        );
        let err = EmitterConfig::from_toml_str(
            "[ch]\nhost = \"h\"\n[stream]\nauto_create_name_all = \"$nope$\"\n",
        )
        .expect_err("rejects")
        .to_string();
        assert!(err.contains("stream.auto_create_name_all"), "{err}");
    }

    #[test]
    fn replicate_all_disabled_via_stream_flag() {
        let c =
            EmitterConfig::from_toml_str("[ch]\nhost = \"h\"\n[stream]\nreplicate_all = false\n")
                .expect("parses");
        assert!(!c.replicate_all);
    }

    #[test]
    fn table_entries_skip_non_table_namespace_values() {
        let root: toml::Table = toml::from_str(
            "[source]\ndbname = \"app\"\n[table]\npublic = 1\n[table.s.t]\nreplicate = true\n",
        )
        .unwrap();
        let entries = table_entries_for(&root, "app");
        assert_eq!(entries.len(), 1);
        assert_eq!((entries[0].0, entries[0].1), ("s", "t"));
    }

    #[test]
    fn pinned_columns_without_namespace_template_use_default_name() {
        let c = EmitterConfig::from_toml_str(
            "[ch]\n[table.app.events]\ncolumns = [{ attnum = 1, target = \"id\", type = \"UInt64\" }]\n",
        )
        .unwrap();
        let m = &c.tables[&RelName::new("app", "events")];
        assert_eq!(m.target.table, "app_events");
    }

    #[test]
    fn pinned_columns_follow_namespace_template_and_opt_out() {
        let c = EmitterConfig::from_toml_str(
            "[ch]\n[stream]\nauto_create_name_all = \"all_\"\n\
             [namespace.app]\nauto_create_name = \"wh_\"\n\
             [table.app.events]\ncolumns = [{ attnum = 1, target = \"id\", type = \"UInt64\" }]\n\
             [table.audit.log]\ncolumns = [{ attnum = 1, target = \"id\", type = \"UInt64\" }]\n\
             [table.app.skipped]\nreplicate = false\n\
             columns = [{ attnum = 1, target = \"id\", type = \"UInt64\" }]\n",
        )
        .unwrap();
        let m = &c.tables[&RelName::new("app", "events")];
        assert_eq!(m.target.table, "wh_events");
        let m = &c.tables[&RelName::new("audit", "log")];
        assert_eq!(m.target.table, "all_log");
        assert!(!c.tables.contains_key(&RelName::new("app", "skipped")));
    }

    #[tokio::test]
    async fn load_merged_names_unreadable_and_malformed_files() {
        let dir = tempfile::tempdir().unwrap();
        let base = dir.path().join("ch-config.toml");
        tokio::fs::create_dir(&base).await.unwrap();
        let e = load_merged(&base).await.unwrap_err().to_string();
        assert!(e.contains("read "), "{e}");
        tokio::fs::remove_dir(&base).await.unwrap();
        tokio::fs::write(&base, "[ch\n").await.unwrap();
        let e = load_merged(&base).await.unwrap_err().to_string();
        assert!(e.contains("parse "), "{e}");
        tokio::fs::write(&base, "[ch]\n").await.unwrap();
        let confd = dir.path().join("ch-config.d");
        tokio::fs::create_dir(&confd).await.unwrap();
        tokio::fs::write(confd.join("README"), "not toml")
            .await
            .unwrap();
        tokio::fs::write(confd.join("10-bad.toml"), "[ch\n")
            .await
            .unwrap();
        let e = load_merged(&base).await.unwrap_err().to_string();
        assert!(e.contains("10-bad.toml"), "{e}");
        tokio::fs::remove_file(confd.join("10-bad.toml"))
            .await
            .unwrap();
        tokio::fs::create_dir(confd.join("20-dir.toml"))
            .await
            .unwrap();
        let e = load_merged(&base).await.unwrap_err().to_string();
        assert!(e.contains("read ") && e.contains("20-dir.toml"), "{e}");
    }

    #[test]
    fn config_parses_full_toml_round_trip() {
        let src = r#"
            [ch]
            host = "ch.example.com"
            port = 9000
            database = "default"
            user = "ingest"
            password = "secret"
            secure = true
            tls_server_name = "svc.clickhouse.cloud"
            compression = "lz4"
            row_budget = 1024
            byte_budget = 4096
            drain_batch_rows = 9
            drain_batch_bytes = 10
            plan_disk_max = 11
            decoder_pool_size = 12
            inserter_pool_size = 13
            decoder_batch_size = 14
            decoder_queue_capacity = 15
            flush_timeout_ms = 16
            retry_max_attempts = 17
            retry_initial_backoff_ms = 18
            retry_max_backoff_ms = 19

            [toast]
            mode = "clickhouse"

            [runtime_config]
            schema = "ws"

            [stream]
            paused = true
            skip_attribution = true
            replicate_all = false

            [table.public.foo]
            initial_load = "copy"
            columns = [
              { attnum = 1, target = "id",   type = "UInt64" },
              { attnum = 2, target = "name", type = "Nullable(String)" },
            ]
        "#;
        let c = EmitterConfig::from_toml_str(src).expect("parses");
        assert_eq!(c.conn.host, "ch.example.com");
        assert_eq!(c.conn.port, 9000);
        assert_eq!(c.conn.database, "default");
        assert_eq!(c.conn.user, "ingest");
        assert_eq!(c.conn.password, "secret");
        assert!(c.conn.secure);
        assert_eq!(c.conn.tls_server_name, "svc.clickhouse.cloud");
        assert_eq!(c.compression, CompressionChoice::Lz4);
        // Omitting `secure` defaults to plaintext, and SNI falls back to `host`
        let plain = EmitterConfig::from_toml_str("[ch]\nhost = \"h\"\n").unwrap();
        assert!(!plain.conn.secure);
        assert_eq!(plain.conn.tls_server_name, "");
        // Named instances take the same key
        let named = EmitterConfig::from_toml_str(
            "[ch]\nhost = \"h\"\n[ch.instances.cloud]\nhost = \"10.0.0.5\"\nsecure = true\n\
             tls_server_name = \"svc.clickhouse.cloud\"\n",
        )
        .unwrap();
        assert_eq!(
            named.instances["cloud"].tls_server_name,
            "svc.clickhouse.cloud"
        );
        assert_eq!(c.row_budget, 1024);
        assert_eq!(c.byte_budget, 4096);
        assert_eq!((c.drain_batch_rows, c.drain_batch_bytes), (9, 10));
        assert_eq!(c.plan_disk_max, 11);
        assert_eq!((c.decoder_pool_size, c.inserter_pool_size), (12, 13));
        assert_eq!((c.decoder_batch_size, c.decoder_queue_capacity), (14, 15));
        assert_eq!(c.flush_timeout, Duration::from_millis(16));
        assert_eq!(c.retry.max_attempts, 17);
        assert_eq!(c.retry.initial_backoff, Duration::from_millis(18));
        assert_eq!(c.retry.max_backoff, Duration::from_millis(19));
        assert_eq!(c.runtime_config_schema.as_deref(), Some("ws"));
        assert!(c.paused);
        assert!(c.skip_attribution);
        assert!(!plain.skip_attribution);
        assert!(!c.replicate_all);
        let rel = RelName::new("public", "foo");
        let t = c.tables.get(&rel).expect("mapping present");
        // target_database/target_table omitted: [ch] database + derived name
        assert_eq!(t.target, TableTarget::new("default", "public_foo"));
        assert_eq!(t.columns.len(), 2);
        assert_eq!(t.columns[0].src_attnum, 1);
        assert_eq!(t.columns[1].target_type, "Nullable(String)");
        assert_eq!(
            c.table_initial_loads.get(&rel).copied(),
            Some(InitialLoadMode::Copy)
        );
        // soft_delete defaults off when the key is absent
        assert!(!c.soft_delete);
        let empty = EmitterConfig::from_toml_str("[runtime_config]\nschema = \"\"\n").unwrap();
        assert!(empty.runtime_config_schema.is_none());
    }

    #[test]
    fn config_memory_section_round_trip() {
        let c = EmitterConfig::from_toml_str(
            "[ch]\n\
             [memory]\n\
             resident_payload_max = 1048576\n\
             inline_value_max = 65536\n\
             value_reserve = 4096\n\
             inline_value_overflow = \"error\"\n",
        )
        .unwrap();
        assert_eq!(c.resident_payload_max, 1 << 20);
        assert_eq!(c.inline_value_max, 64 << 10);
        assert_eq!(c.value_reserve, 4 << 10);
        assert_eq!(c.inline_value_overflow, InlineValueOverflow::Error);
        // Omitted section keeps defaults
        let d = EmitterConfig::from_toml_str("[ch]\n").unwrap();
        assert_eq!(d.resident_payload_max, default_resident_payload_max());
        assert_eq!(d.inline_value_max, DEFAULT_INLINE_VALUE_MAX);
        assert_eq!(d.value_reserve, DEFAULT_VALUE_RESERVE);
        assert_eq!(d.inline_value_overflow, InlineValueOverflow::Null);
    }

    /// Keep default pool large enough for reserve
    #[test]
    fn derived_pool_default_stays_bootable() {
        let pool = default_resident_payload_max();
        let expected = crate::budget::host_memory_limit()
            .map(|m| m / RESIDENT_PAYLOAD_FRACTION)
            .unwrap_or(MIN_RESIDENT_PAYLOAD_MAX)
            .max(MIN_RESIDENT_PAYLOAD_MAX);
        assert_eq!(pool, expected);
        assert!(DEFAULT_DECODER_POOL * DEFAULT_VALUE_RESERVE <= pool / 2);
    }

    #[test]
    fn config_table_replicate_false_skips_mapping() {
        let c = EmitterConfig::from_toml_str(
            "[ch]\n\
             [table.public.skip]\n\
             replicate = false\n\
             initial_load = \"copy\"\n",
        )
        .unwrap();
        let rel = RelName::new("public", "skip");
        assert!(!c.tables.contains_key(&rel));
        assert!(!c.table_initial_loads.contains_key(&rel));
    }

    #[test]
    fn config_table_pattern_entry_keys_on_pattern() {
        let c = EmitterConfig::from_toml_str(
            "[ch]\n\
             [table.public.\"events_*\"]\n\
             match = \"glob\"\n\
             replicate = true\n\
             initial_load = \"copy\"\n",
        )
        .unwrap();
        let (rel, kind, rule) = &c.table_entries[0];
        assert_eq!(*rel, RelName::new("public", "events_*"));
        assert_eq!(*kind, MatchKind::Glob);
        assert_eq!(rule.replicate, Some(true));
        assert_eq!(rule.initial_load.as_deref(), Some("copy"));
        assert!(c.table_opt_ins.is_empty());
    }

    #[test]
    fn config_table_literal_entry_lands_in_entries_and_opt_ins() {
        let c = EmitterConfig::from_toml_str(
            "[ch]\n\
             [table.public.events]\n\
             replicate = true\n\
             target_table = \"ev\"\n",
        )
        .unwrap();
        let rel = RelName::new("public", "events");
        let (_, kind, rule) = &c.table_entries[0];
        assert_eq!(*kind, MatchKind::Exact);
        assert_eq!(rule.target_table.as_deref(), Some("ev"));
        assert!(c.table_opt_ins.contains_key(&rel));
    }

    #[test]
    fn config_table_rejects_bad_match_and_pattern_columns() {
        assert!(
            EmitterConfig::from_toml_str("[ch]\n[table.public.t]\nmatch = \"like\"\n").is_err(),
            "a typo must not read as a literal name"
        );
        assert!(
            EmitterConfig::from_toml_str(
                "[ch]\n[table.public.\"t.*\"]\nmatch = \"regex\"\n\
                 columns = [{ attnum = 1, target = \"id\", type = \"UInt64\" }]\n"
            )
            .is_err()
        );
    }

    #[test]
    fn config_column_name_entries_state_rules_not_a_projection() {
        let c = EmitterConfig::from_toml_str(
            "[ch]\n\
             [table.app.events]\n\
             replicate = true\n\
             columns = [\n  \
               { name = \"legacy_id\", target = \"id\", type = \"UInt64\" },\n  \
               { name = \"*_at\", match = \"glob\", type = \"DateTime64(6, 'UTC')\" },\n\
             ]\n",
        )
        .unwrap();
        let rel = RelName::new("app", "events");
        assert!(!c.tables.contains_key(&rel));
        assert!(c.table_opt_ins.contains_key(&rel));
        assert_eq!(c.column_entries.len(), 2);
        let first = &c.column_entries[0];
        assert_eq!(first.rel, rel);
        assert_eq!(first.rel_kind, MatchKind::Exact);
        assert_eq!(first.attname, "legacy_id");
        assert_eq!(first.att_kind, MatchKind::Exact);
        assert_eq!(first.rule.target_name.as_deref(), Some("id"));
        assert_eq!(first.rule.target_type.as_deref(), Some("UInt64"));
        let second = &c.column_entries[1];
        assert_eq!(second.att_kind, MatchKind::Glob);
        assert!(second.rule.target_name.is_none());
    }

    #[test]
    fn config_column_name_entries_ride_a_pattern_block() {
        let c = EmitterConfig::from_toml_str(
            "[ch]\n\
             [table.app.\"*\"]\n\
             match = \"glob\"\n\
             replicate = true\n\
             columns = [{ name = \"*_at\", match = \"glob\", type = \"DateTime64(6, 'UTC')\" }]\n",
        )
        .unwrap();
        let e = &c.column_entries[0];
        assert_eq!(e.rel, RelName::new("app", "*"));
        assert_eq!(e.rel_kind, MatchKind::Glob);
        assert!(c.table_opt_ins.is_empty(), "a pattern queues no opt-in");
    }

    #[test]
    fn config_column_entry_shapes_are_exclusive() {
        let bad = [
            "columns = [{ attnum = 1, name = \"id\", type = \"UInt64\" }]",
            "columns = [{ target = \"id\", type = \"UInt64\" }]",
            "columns = [{ attnum = 1, match = \"glob\", target = \"id\", type = \"UInt64\" }]",
            "columns = [{ name = \"*_at\", match = \"glob\", target = \"ts\" }]",
            "columns = [{ name = \"id\" }]",
            "columns = [{ attnum = 1, type = \"UInt64\" }]",
            "columns = [{ attnum = 1, target = \"id\" }]",
            "columns = [{ attnum = 1, target = \"id\", type = \"UInt64\" }, \
             { name = \"ts\", type = \"DateTime\" }]",
        ];
        for body in bad {
            assert!(
                EmitterConfig::from_toml_str(&format!("[ch]\n[table.app.events]\n{body}\n"))
                    .is_err(),
                "{body}"
            );
        }
    }

    #[test]
    fn config_system_columns_reach_config() {
        let c = EmitterConfig::from_toml_str(
            "[ch]\n\
             [system_columns]\n\
             lsn = \"_peerdb_version\"\n\
             is_deleted = false\n",
        )
        .unwrap();
        assert_eq!(c.system_columns.lsn, "_peerdb_version");
        assert!(c.system_columns.is_deleted.is_none());
        assert!(!c.row_policy().soft_delete);
        // Absent section keeps the defaults
        let d = EmitterConfig::from_toml_str("[ch]\n").unwrap();
        assert_eq!(*d.system_columns, SystemColumns::default());
    }

    #[test]
    fn table_block_renames_system_columns_for_its_relations() {
        let c = EmitterConfig::from_toml_str(
            "[ch]\n\
             [system_columns]\n\
             lsn = \"_v\"\n\
             [table.public.events]\n\
             lsn = \"_peerdb_version\"\n\
             is_deleted = false\n\
             [table.app.\"events_*\"]\n\
             match = \"glob\"\n\
             commit_ts = \"_peerdb_synced_at\"\n",
        )
        .unwrap();
        assert_eq!(c.system_columns.lsn, "_v", "cluster-wide default stands");
        let entry = |rel: RelName| {
            c.table_entries
                .iter()
                .find(|(r, _, _)| *r == rel)
                .expect("entry")
        };
        let (_, kind, rule) = entry(RelName::new("public", "events"));
        assert_eq!(*kind, MatchKind::Exact);
        assert_eq!(rule.system.lsn.as_deref(), Some("_peerdb_version"));
        assert_eq!(rule.system.is_deleted.as_deref(), Some(""));
        let (_, kind, rule) = entry(RelName::new("app", "events_*"));
        assert_eq!(*kind, MatchKind::Glob);
        assert_eq!(rule.system.commit_ts.as_deref(), Some("_peerdb_synced_at"));
    }

    #[test]
    fn table_block_rejects_rename_onto_another_system_column() {
        assert!(EmitterConfig::from_toml_str("[ch]\n[table.public.t]\nlsn = \"_xid\"\n").is_err());
        assert!(EmitterConfig::from_toml_str("[ch]\n[table.public.t]\nis_deleted = 7\n").is_err());
    }

    #[test]
    fn config_table_key_lists_parse_from_arrays() {
        let c = EmitterConfig::from_toml_str(
            "[ch]\n\
             [table.public.events]\n\
             order_by = [\"tenant\", \"id\"]\n\
             primary_key = [\"tenant\"]\n\
             engine = \"Null\"\n",
        )
        .unwrap();
        let rel = RelName::new("public", "events");
        let (_, _, rule) = &c.table_entries[0];
        assert_eq!(rule.engine.as_deref(), Some("Null"));
        assert_eq!(
            rule.order_by.as_deref(),
            Some(["tenant".to_string(), "id".to_string()].as_slice())
        );
        assert_eq!(
            rule.primary_key.as_deref(),
            Some(["tenant".to_string()].as_slice())
        );
        // Key lists also apply to a columns-less opt-in block
        assert!(!c.tables.contains_key(&rel));
        // Arrays only: a bare string is a config error, never a split list
        for bad in [
            "order_by = 7",
            "order_by = \"tenant, id\"",
            "primary_key = \"tenant\"",
        ] {
            assert!(
                EmitterConfig::from_toml_str(&format!("[ch]\n[table.public.t]\n{bad}\n")).is_err(),
                "{bad}"
            );
        }
    }

    #[test]
    fn named_instances_resolve_and_validate() {
        let cfg = EmitterConfig::from_toml_str(
            r#"
            [ch]
            database = "main"
            [ch.instances.archive]
            host = "archive.internal"
            database = "history"
            [table.public.events]
            target_instance = "archive"
            tee = [
                { table = "local_copy" },
                { instance = "default", table = "default_copy" },
                { instance = "archive", database = "audit", table = "log" },
            ]
            columns = [{ attnum = 1, target = "id", type = "Int32" }]
        "#,
        )
        .unwrap();
        let m = &cfg.tables[&RelName::new("public", "events")];
        let targets: Vec<_> = m.targets().collect();
        assert_eq!(
            targets
                .iter()
                .map(|t| t.instance.as_deref())
                .collect::<Vec<_>>(),
            [Some("archive"), Some("archive"), None, Some("archive")]
        );
        assert_eq!(
            targets
                .iter()
                .map(|t| t.database.as_str())
                .collect::<Vec<_>>(),
            ["history", "history", "main", "audit"]
        );
        for config in [
            "[table.public.t]\ntarget_instance = 'missing'",
            "[table.public.t]\ntee = [{ instance = 'missing', table = 't' }]",
            "[ch.instances.default]\nhost = 'localhost'",
            "[ch.instances.a]\nport = 0",
            "[ch.instances.a]\nhots = 'typo'",
        ] {
            assert!(EmitterConfig::from_toml_str(config).is_err(), "{config}");
        }
    }

    #[test]
    fn tee_parses_on_exact_entries_only() {
        let c = EmitterConfig::from_toml_str(
            "[ch]\n\
             [table.public.events]\n\
             columns = [{ attnum = 1, target = \"id\", type = \"Int32\" }]\n\
             [[table.public.events.tee]]\n\
             table = \"events_audit\"\n\
             engine = \"MergeTree\"\n\
             order_by = [\"id\", \"_lsn\"]\n",
        )
        .unwrap();
        let m = &c.tables[&RelName::new("public", "events")];
        assert_eq!(m.tees.len(), 1);
        assert_eq!(m.tees[0].engine.as_deref(), Some("MergeTree"));
        assert_eq!(
            m.tee_targets().collect::<Vec<_>>(),
            [TableTarget::new("default", "events_audit")]
        );
        let err = EmitterConfig::from_toml_str(
            "[ch]\n[table.public.\"ev_*\"]\nmatch = \"glob\"\ntee = [{ table = \"audit\" }]\n",
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("merge every matched table"),
            "{err}"
        );
        assert!(
            EmitterConfig::from_toml_str("[ch]\n[table.public.t]\ntee = [{ engine = \"Log\" }]\n")
                .is_err(),
            "tee needs a table"
        );
    }

    #[test]
    fn config_soft_delete_defaults_off_and_parses_on() {
        assert!(!EmitterConfig::default().soft_delete);
        let c = EmitterConfig::from_toml_str("[ch]\nsoft_delete = true\n").unwrap();
        assert!(c.soft_delete);
    }

    #[test]
    fn namespace_toml_parses_auto_create() {
        let c = EmitterConfig::from_toml_str(
            "[ch]\n\
             [namespace.s1]\n\
             auto_create = true\n\
             [namespace.s2]\n\
             auto_create = false\n\
             [namespace.s3]\n\
             target_database = \"warehouse\"\n",
        )
        .unwrap();
        assert!(c.namespaces["s1"].auto_create, "explicit true");
        assert!(!c.namespaces["s2"].auto_create, "explicit false");
        // Key absent defaults off (unwrap_or(false)).
        assert!(!c.namespaces["s3"].auto_create, "absent defaults off");
        assert_eq!(
            c.namespaces["s3"].target_database.as_deref(),
            Some("warehouse")
        );
    }

    /// Dotted names stay inside their TOML key level: schema `a.b` table `c`
    /// and schema `a` table `b.c` are distinct rels, distinct targets.
    #[test]
    fn config_table_dotted_names_do_not_collide() {
        let c = EmitterConfig::from_toml_str(
            "[ch]\n\
             database = \"default\"\n\
             [table.\"a.b\".c]\n\
             columns = [{ attnum = 1, target = \"id\", type = \"UInt64\" }]\n\
             [table.a.\"b.c\"]\n\
             columns = [{ attnum = 1, target = \"id\", type = \"UInt64\" }]\n",
        )
        .unwrap();
        let dotted_ns = c.tables.get(&RelName::new("a.b", "c")).expect("a.b / c");
        let dotted_rel = c.tables.get(&RelName::new("a", "b.c")).expect("a / b.c");
        assert_eq!(dotted_ns.target, TableTarget::new("default", "a.b_c"));
        assert_eq!(dotted_rel.target, TableTarget::new("default", "a_b.c"));
        // Interpolation quotes the dot inside the identifier
        assert_eq!(dotted_rel.target.sql(), "`default`.`a_b.c`");
    }

    /// Database prefixes preserve table identity within source database
    #[test]
    fn database_qualified_key_reads_as_the_followed_database() {
        let c = EmitterConfig::from_toml_str(
            "[ch]\n\
             database = \"cdc\"\n\
             [source]\n\
             dbname = \"app\"\n\
             [database.app.table.public.orders]\n\
             target_table = \"orders_v2\"\n\
             [database.app.namespace.public]\n\
             auto_create = true\n",
        )
        .unwrap();
        assert_eq!(
            c.table_entries.iter().map(|(r, ..)| r).collect::<Vec<_>>(),
            vec![&RelName::new("public", "orders")]
        );
        assert_eq!(
            c.table_opt_ins[&RelName::new("public", "orders")]
                .target_table
                .as_deref(),
            Some("orders_v2")
        );
        assert!(c.namespaces["public"].auto_create, "namespace level too");
        assert_eq!(c.databases, vec!["app".to_string()], "only app is named");
    }

    /// Database-prefixed entries name a database without routing it here
    #[test]
    fn foreign_database_entries_route_nothing() {
        const DOC: &str = "[ch]\n\
             database = \"cdc\"\n\
             [source]\n\
             dbname = \"app\"\n\
             [table.public.orders]\n\
             replicate = true\n\
             [database.billing.table.public.orders]\n\
             target_table = \"billing_orders\"\n\
             [database.billing.table.public.invoices]\n\
             target_table = \"billing_invoices\"\n\
             [database.billing.namespace.public]\n\
             auto_create = true\n";
        let c = EmitterConfig::from_toml_str(DOC).unwrap();
        assert_eq!(
            c.table_entries.iter().map(|(r, ..)| r).collect::<Vec<_>>(),
            vec![&RelName::new("public", "orders")]
        );
        assert!(c.namespaces.is_empty(), "billing's namespace stays out");
        assert_eq!(c.databases, vec!["app".to_string(), "billing".to_string()]);
        // Same document scoped to the other database routes its entries
        let doc: toml::Table = toml::from_str(DOC).unwrap();
        let billing = EmitterConfig::for_database(&doc, "billing").unwrap();
        let mut rels: Vec<&str> = billing
            .table_entries
            .iter()
            .map(|(r, ..)| &*r.name)
            .collect();
        rels.sort_unstable();
        assert_eq!(rels, vec!["invoices", "orders"]);
        assert!(
            billing.namespaces["public"].auto_create,
            "billing's namespace"
        );
        assert_eq!(
            billing.databases, c.databases,
            "one list, whatever the scope"
        );
    }

    /// Backup-sourced loads cover the database `[source] dbname` names
    #[test]
    fn backup_initial_load_outside_the_primary_database_is_rejected() {
        let doc = |entry: &str| {
            format!(
                "[ch]\n\
                 database = \"cdc\"\n\
                 [source]\n\
                 dbname = \"app\"\n\
                 {entry}"
            )
        };
        for mode in ["base_backup", "object_store"] {
            let msg = EmitterConfig::from_toml_str(&doc(&format!(
                "[database.billing.table.public.ledger]\ninitial_load = \"{mode}\"\n"
            )))
            .expect_err(mode)
            .to_string();
            assert!(
                msg.contains("database.billing.table.public.ledger"),
                "{msg}"
            );
            assert!(msg.contains(mode) && msg.contains("copy"), "{msg}");
        }
        let msg = EmitterConfig::from_toml_str(&doc(
            "[database.billing.namespace.public]\ninitial_load = \"base_backup\"\n",
        ))
        .expect_err("namespace level too")
        .to_string();
        assert!(msg.contains("database.billing.namespace.public"), "{msg}");
        // The primary database is exactly what a cluster backup covers
        for entry in [
            "[table.public.orders]\ninitial_load = \"base_backup\"\n",
            "[database.app.table.public.orders]\ninitial_load = \"object_store\"\n",
            "[database.billing.table.public.ledger]\ninitial_load = \"copy\"\n",
        ] {
            EmitterConfig::from_toml_str(&doc(entry)).unwrap_or_else(|e| panic!("{entry}: {e}"));
        }
    }

    /// A schema and a database can share a name, the key level says which
    #[test]
    fn schema_named_like_a_database_stays_a_schema() {
        let c = EmitterConfig::from_toml_str(
            "[ch]\n\
             database = \"cdc\"\n\
             [source]\n\
             dbname = \"app\"\n\
             [table.billing.orders]\n\
             target_table = \"app_billing_orders\"\n\
             [database.billing.table.public.orders]\n\
             target_table = \"billing_orders\"\n",
        )
        .unwrap();
        assert_eq!(
            c.table_entries.iter().map(|(r, ..)| r).collect::<Vec<_>>(),
            vec![&RelName::new("billing", "orders")],
            "schema `billing` of database `app`"
        );
        assert_eq!(c.databases, vec!["app".to_string(), "billing".to_string()]);
    }

    /// Two source databases may share one ClickHouse table: rows merge there
    #[test]
    fn cross_database_destination_collision_is_allowed() {
        let shared = "[ch]\n\
             database = \"cdc\"\n\
             [source]\n\
             dbname = \"app\"\n\
             [table.public.orders]\n\
             replicate = true\n\
             [database.billing.table.public.orders]\n\
             replicate = true\n";
        let c = EmitterConfig::from_toml_str(shared).expect("merging destinations is allowed");
        assert_eq!(c.databases, vec!["app".to_string(), "billing".to_string()]);
        assert_eq!(
            c.table_entries.iter().map(|(r, ..)| r).collect::<Vec<_>>(),
            vec![&RelName::new("public", "orders")],
            "this process still scopes to its own database",
        );
        for distinct in [
            "target_database = \"billing_cdc\"",
            "target_table = \"billing_orders\"",
            "replicate = false",
        ] {
            EmitterConfig::from_toml_str(&format!(
                "[ch]\n\
                 database = \"cdc\"\n\
                 [source]\n\
                 dbname = \"app\"\n\
                 [table.public.orders]\n\
                 replicate = true\n\
                 [database.billing.table.public.orders]\n\
                 {distinct}\n"
            ))
            .expect(distinct);
        }
    }

    /// Merge `[database.<dbname>]` entries with unprefixed ones, reject duplicates
    #[test]
    fn qualified_and_unqualified_entries_merge() {
        let c = EmitterConfig::from_toml_str(
            "[ch]\n\
             database = \"cdc\"\n\
             [source]\n\
             dbname = \"zz\"\n\
             [table.public.orders]\n\
             replicate = true\n\
             [database.zz.table.public.users]\n\
             replicate = true\n",
        )
        .unwrap();
        let mut rels: Vec<&str> = c.table_entries.iter().map(|(r, ..)| &*r.name).collect();
        rels.sort_unstable();
        assert_eq!(rels, vec!["orders", "users"], "database `zz` sorts last");
        assert_eq!(c.databases, vec!["zz".to_string()]);

        let msg = EmitterConfig::from_toml_str(
            "[ch]\n\
             [source]\n\
             dbname = \"app\"\n\
             [table.public.orders]\n\
             replicate = true\n\
             [database.app.table.public.orders]\n\
             replicate = false\n",
        )
        .expect_err("one relation, two spellings")
        .to_string();
        assert!(msg.contains("`table.public.orders`"), "{msg}");
        let msg = EmitterConfig::from_toml_str(
            "[ch]\n\
             [source]\n\
             dbname = \"app\"\n\
             [namespace.public]\n\
             auto_create = true\n\
             [database.app.namespace.public]\n\
             auto_create = false\n",
        )
        .expect_err("one schema, two spellings")
        .to_string();
        assert!(msg.contains("`namespace.public`"), "{msg}");
    }

    /// Require source `dbname`, reject stale prefixes and unknown settings
    #[test]
    fn database_key_level_rejects_ambiguous_entries() {
        let rejects = |src: &str| {
            EmitterConfig::from_toml_str(src).expect_err(src);
        };
        rejects("[ch]\n[database.billing.table.public.orders]\nreplicate = true\n");
        rejects("[ch]\n[database.billing.namespace.public]\nauto_create = true\n");
        // A database prefix under `table` reads as a relation setting
        rejects(
            "[ch]\n\
             [source]\n\
             dbname = \"app\"\n\
             [table.billing.public.orders]\n\
             replicate = true\n",
        );
        rejects(
            "[ch]\n\
             [source]\n\
             dbname = \"app\"\n\
             [namespace.billing.public]\n\
             auto_create = true\n",
        );
        rejects(
            "[ch]\n\
             [source]\n\
             dbname = \"app\"\n\
             [table.public.orders]\n\
             replicat = true\n",
        );
    }

    /// Include source database name in errors from shared config
    #[test]
    fn foreign_database_type_error_names_its_database() {
        let msg = EmitterConfig::from_toml_str(
            "[ch]\n\
             [source]\n\
             dbname = \"app\"\n\
             [database.billing.table.public.orders]\n\
             replicate = \"no\"\n",
        )
        .expect_err("replicate is a boolean")
        .to_string();
        assert!(
            msg.contains("database.billing.table.public.orders.replicate"),
            "{msg}"
        );
    }

    #[test]
    fn merge_tables_deep_and_overwrite() {
        let mut base: toml::Table = toml::from_str(
            "[ch]\nhost = \"base\"\nport = 9000\n[table.\"public.users\"]\ntarget = \"demo.users\"\n",
        )
        .unwrap();
        let over: toml::Table = toml::from_str("[ch]\nhost = \"frag\"\n").unwrap();
        merge_tables(&mut base, over);
        // fragment overrides [ch].host, keeps [ch].port and the base [table.*].
        assert_eq!(
            base["ch"].as_table().unwrap()["host"].as_str(),
            Some("frag")
        );
        assert_eq!(
            base["ch"].as_table().unwrap()["port"].as_integer(),
            Some(9000)
        );
        assert!(base.get("table").is_some(), "base [table.*] survived");
    }

    #[tokio::test]
    async fn load_merged_base_plus_confd_lexical() {
        let dir = tempfile::tempdir().unwrap();
        let base = dir.path().join("ch-config.toml");
        let confd = dir.path().join("ch-config.d");
        tokio::fs::write(&base, "[ch]\nhost = \"base\"\nport = 9000\n")
            .await
            .unwrap();
        tokio::fs::create_dir(&confd).await.unwrap();
        tokio::fs::write(confd.join("10-x.toml"), "[ch]\nhost = \"ten\"\n")
            .await
            .unwrap();
        tokio::fs::write(
            confd.join("50-api.toml"),
            "[ch]\nhost = \"fifty\"\ndatabase = \"demo\"\n",
        )
        .await
        .unwrap();
        let merged = load_merged(&base).await.unwrap();
        let ch = merged["ch"].as_table().unwrap();
        // Higher-numbered fragment wins; base port and fragment database persist.
        assert_eq!(ch["host"].as_str(), Some("fifty"));
        assert_eq!(ch["port"].as_integer(), Some(9000));
        assert_eq!(ch["database"].as_str(), Some("demo"));
        let cfg = EmitterConfig::from_table(&merged).unwrap();
        assert_eq!(cfg.conn.host, "fifty");
        assert_eq!(cfg.conn.database, "demo");
    }

    #[tokio::test]
    async fn load_merged_absent_base_ok() {
        let dir = tempfile::tempdir().unwrap();
        let merged = load_merged(&dir.path().join("nope.toml")).await.unwrap();
        assert!(merged.is_empty());
    }

    #[test]
    fn config_backup_absent_is_none() {
        assert!(EmitterConfig::default().backup.is_none());
        let c = EmitterConfig::from_toml_str("[ch]\nhost = \"h\"\n").unwrap();
        assert!(c.backup.is_none());
    }

    #[test]
    fn config_backup_s3_static_creds() {
        use walrus::config::StorageSettings::S3;
        use walrus::storage::s3::CredentialSource::Static;
        let c = EmitterConfig::from_toml_str(
            "[backup]\n\
             archive = \"s3://my-bucket/walshadow/prefix\"\n\
             region = \"eu-west-1\"\n\
             endpoint = \"https://minio.internal\"\n\
             force_path_style = true\n\
             access_key = \"AK\"\n\
             secret_key = \"SK\"\n",
        )
        .unwrap();
        let storage = c.backup.expect("backup set").storage;
        let S3(s3) = storage else { panic!() };
        assert_eq!(s3.bucket, "my-bucket");
        assert_eq!(s3.prefix, "walshadow/prefix");
        assert_eq!(s3.region, "eu-west-1");
        assert_eq!(s3.endpoint.as_deref(), Some("https://minio.internal"));
        assert!(s3.force_path_style);
        let Static(cr) = s3.creds else { panic!() };
        assert_eq!(cr.access_key, "AK");
        assert_eq!(cr.secret_key, "SK");
    }

    #[test]
    fn config_backup_s3_defaults_region_and_imds() {
        use walrus::config::StorageSettings::S3;
        use walrus::storage::s3::CredentialSource;
        let c = EmitterConfig::from_toml_str("[backup]\narchive = \"s3://b\"\n").unwrap();
        let storage = c.backup.unwrap().storage;
        let S3(s3) = storage else { panic!() };
        assert_eq!(s3.bucket, "b");
        assert_eq!(s3.prefix, "");
        assert_eq!(s3.region, "us-east-1");
        assert!(matches!(s3.creds, CredentialSource::Imds(_)));
    }

    #[test]
    fn config_backup_gcs_and_file() {
        use walrus::config::StorageSettings::{self, Gcs};
        let gcs = EmitterConfig::from_toml_str(
            "[backup]\narchive = \"gs://gb/pre\"\ncredentials_path = \"/sa.json\"\n",
        )
        .unwrap()
        .backup
        .unwrap();
        let Gcs(g) = gcs.storage else { panic!() };
        assert_eq!(g.bucket, "gb");
        assert_eq!(g.prefix, "pre");
        assert_eq!(g.credentials_path.as_deref(), Some("/sa.json"));
        let fs = EmitterConfig::from_toml_str("[backup]\narchive = \"file:///var/wal\"\n")
            .unwrap()
            .backup
            .unwrap();
        assert!(matches!(fs.storage, StorageSettings::Fs { path } if path == "/var/wal"));
    }

    #[test]
    fn config_backup_rejects_bad_input() {
        assert!(EmitterConfig::from_toml_str("[backup]\nregion = \"x\"\n").is_err());
        assert!(EmitterConfig::from_toml_str("[backup]\narchive = \"http://b\"\n").is_err());
        assert!(
            EmitterConfig::from_toml_str("[backup]\narchive = \"s3://b\"\naccess_key = \"AK\"\n")
                .is_err()
        );
    }

    #[test]
    fn config_type_error_names_the_path() {
        let rejects = |src: &str, path: &str| {
            let msg = EmitterConfig::from_toml_str(src)
                .expect_err(src)
                .to_string();
            assert!(msg.contains(path), "{src:?}: {msg}");
        };
        rejects("[ch]\nport = 70000\n", "`ch.port`");
        rejects("[ch]\nrow_budget = \"4096\"\n", "`ch.row_budget`");
        rejects("[ch]\nsoft_delete = \"yes\"\n", "`ch.soft_delete`");
        rejects(
            "[memory]\ninline_value_max = -1\n",
            "`memory.inline_value_max`",
        );
        rejects(
            "[memory]\ninline_value_overflow = \"drop\"\n",
            "`memory.inline_value_overflow`",
        );
        rejects("[stream]\nreplicate_all = 0\n", "`stream.replicate_all`");
        rejects(
            "[table.public.orders]\nreplicate = \"false\"\n",
            "`table.public.orders.replicate`",
        );
        rejects(
            "[namespace.sales]\nauto_create = \"true\"\n",
            "`namespace.sales.auto_create`",
        );
        rejects("[table.public.orders]\ntarget_table = 3\n", "target_table");
        rejects(
            "[namespace.sales]\ntarget_database = true\n",
            "target_database",
        );
        rejects("source = 3\n", "`source`");
        rejects("[table]\npublic = 3\n", "`table.public`");
        rejects("[namespace]\nsales = 3\n", "`namespace.sales`");
        rejects("[table.public]\norders = 3\n", "`table.public.orders`");
        rejects("[table.public.orders]\ncolumns = 3\n", "columns");
    }

    #[test]
    fn config_copy_fallback_defaults_on_and_can_be_disabled() {
        for (src, expected) in [
            ("[ch]", true),
            ("[bootstrap]\ncopy_fallback = true", true),
            ("[bootstrap]\ncopy_fallback = false", false),
        ] {
            let config = EmitterConfig::from_toml_str(src).unwrap();
            assert_eq!(config.bootstrap.copy_fallback.unwrap_or(true), expected);
        }
        assert!(EmitterConfig::from_toml_str("[bootstrap]\ncopy_fallback = \"false\"").is_err());
    }

    #[test]
    fn config_copy_chunk_blocks_defaults_to_a_segment() {
        for (src, expected) in [
            ("[ch]", COPY_CHUNK_BLOCKS),
            ("[bootstrap]\ncopy_chunk_blocks = 8192", 8192),
        ] {
            let config = EmitterConfig::from_toml_str(src).unwrap();
            assert_eq!(
                config
                    .bootstrap
                    .copy_chunk_blocks
                    .map_or(COPY_CHUNK_BLOCKS, NonZeroU32::get),
                expected
            );
        }
        assert!(
            EmitterConfig::from_toml_str("[bootstrap]\ncopy_chunk_blocks = 0").is_err(),
            "a zero chunk would never advance the cursor",
        );
    }

    #[test]
    fn config_rejects_unknown_semantic_values() {
        for src in [
            "[ch]\ncompression = \"snappy\"\n",
            "[ch]\ndrop_table_strategy = \"dorp\"\n",
            "[bootstrap]\nmode = \"objectstore\"\n",
            "[bootstrap]\nobject_store_parallelism = 0\n",
            "[source]\nsslmode = \"maybe\"\n",
            "[namespace.sales]\ndrop_table_strategy = \"dorp\"\n",
            "[table.public.orders]\ninitial_load = \"cpoy\"\n",
            "[table.public.orders]\nmatch = \"like\"\n",
        ] {
            EmitterConfig::from_toml_str(src).expect_err(src);
        }
        let c = EmitterConfig::from_toml_str(
            "[ch]\ndrop_table_strategy = \"DROP\"\n             [namespace.sales]\ndrop_table_strategy = \"warn\"\n",
        )
        .expect("parses");
        assert_eq!(
            (
                c.drop_table_strategy,
                c.namespaces["sales"].drop_table_strategy,
            ),
            (DropTableStrategy::Drop, Some(DropTableStrategy::Warn))
        );
    }

    #[test]
    fn config_parses_partition_settings() {
        EmitterConfig::from_toml_str("[table.public.m]\nleaf = 7\n").expect_err("leaf = 7");
        let c = EmitterConfig::from_toml_str(
            "[ch]\ndetach_strategy = \"drop\"\ntruncate_strategy = \"retain\"\n\
             [namespace.sales]\ndetach_strategy = \"warn\"\n\
             [system_columns]\nleaf = \"_part\"\n\
             [table.public.m]\npartition_by = \"toYYYYMM(ts)\"\nleaf = false\n\
             [table.public.\"m_*\"]\nmatch = \"glob\"\nroute = \"self\"\n",
        )
        .expect("parses");
        assert_eq!(c.detach_strategy, DropTableStrategy::Drop);
        assert_eq!(c.truncate_strategy, DropTableStrategy::Retain);
        assert_eq!(
            EmitterConfig::default().truncate_strategy,
            DropTableStrategy::Drop
        );
        assert_eq!(
            c.namespaces["sales"].detach_strategy,
            Some(DropTableStrategy::Warn)
        );
        assert_eq!(c.system_columns.leaf.as_deref(), Some("_part"));
        let rule = |name: &str| {
            c.table_entries
                .iter()
                .find(|(rel, ..)| &*rel.name == name)
                .map(|(_, _, rule)| rule.clone())
                .unwrap()
        };
        assert_eq!(rule("m").partition_by.as_deref(), Some("toYYYYMM(ts)"));
        assert_eq!(rule("m").system.leaf.as_deref(), Some(""));
        assert_eq!(rule("m_*").route, Some(PartitionRoute::Self_));
    }
}
