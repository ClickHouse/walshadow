//! CH-native emitter primitives via `clickhouse-c-rs`. Batching, seal
//! triggers, and xact close live in [`crate::emit::pipeline`], not here.
//!
//! Synthetic columns `_lsn UInt64`, `_xid UInt32`, `_commit_ts
//! DateTime64(6, 'UTC')`, `_is_deleted Bool` append after every mapped
//! column. `_is_deleted` (1 on delete) wires `ReplacingMergeTree`'s
//! deletion arg unless `EmitterConfig::soft_delete` keeps it queryable.
//! PG `TimestampTz` epoch is 2000-01-01; shift to Unix epoch
//! (`DATETIME64_PG_EPOCH_US`) to match CH `DateTime64(6)`.
//!
//! ## Compression
//!
//! Feature-gated via walshadow's `lz4` / `zstd` Cargo features, which
//! forward to clickhouse-c-rs (see top-level `Cargo.toml`). Default
//! builds advertise LZ4 to match the CH server default.
//!
//! ## Cross-table ordering inside an xact
//!
//! `AsyncClient` is single-query-at-a-time, so an xact touching T1 and
//! T2 lands all T1 rows (one INSERT) then all T2 (next INSERT); WAL
//! interleaving across tables is not preserved. `_lsn` carries the
//! source LSN so `ReplacingMergeTree` dedup keys on the right value;
//! WAL ordering within a single dest table is preserved

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use clickhouse_c::{Allocator, ColumnBuilder, Kind, TypeAst};

#[cfg(test)]
use crate::ch::is_retryable;
use crate::ch::{CompressionChoice, ConnectionConfig, EmitterError, quote_ident};
#[cfg(test)]
use crate::decode::decoder_sink::DecoderSinkError;
use crate::decode::heap_decoder::{ColumnValue, CommittedTuple, HeapOp};
use crate::mapping::{
    ColumnMapping, NamespaceMapping, TableMapping, TableTarget, ToastConfig, ToastMode,
};
use crate::runtime_config::TableRow;
use crate::schema::{RelDescriptor, RelName};

/// Microseconds between PG `TimestampTz` epoch (2000-01-01 UTC) and Unix
/// epoch. CH `DateTime64(6)` is Unix microseconds; PG commit-record
/// `xact_time` and tuple `TimestampTz` are PG-epoch microseconds.
pub(crate) const DATETIME64_PG_EPOCH_US: i64 = walrus::pg::replication::PG_EPOCH_USEC;

/// Days between PG `date` epoch (2000-01-01) and the Unix epoch.
pub(crate) const DATE32_PG_EPOCH_DAYS: i32 = (DATETIME64_PG_EPOCH_US / 1_000_000 / 86_400) as i32;

/// Heap op codes for [`TableEncoder::append_row`]; `OP_DELETE` sets `_is_deleted`
pub(crate) const OP_INSERT: i8 = 1;
pub(crate) const OP_UPDATE: i8 = 2;
pub(crate) const OP_DELETE: i8 = 3;

/// Default block accumulator budgets. Mirror common CH server defaults
pub(crate) const DEFAULT_ROW_BUDGET: usize = 65_536;
pub(crate) const DEFAULT_BYTE_BUDGET: usize = 1 << 20; // 1 MiB

/// Default commit-drain slice budgets
/// ([`crate::xact::xact_buffer::CommittedDrain::next_batch`]). Bytes sized at
/// half the default `xact_buffer_max`: a slice plus the one loading behind
/// it stay within the ingest budget.
pub(crate) const DEFAULT_DRAIN_BATCH_ROWS: usize = 65_536;
pub(crate) const DEFAULT_DRAIN_BATCH_BYTES: usize = 32 << 20; // 32 MiB

/// Default flush timeout (ms). `0` keeps serial emitter's
/// close-INSERT-on-every-xact-end behaviour (bootstrap backfill only);
/// live pipeline substitutes a 100ms partial-batch deadline for `0` so
/// cold tables can't pin the watermark. Positive value holds INSERTs
/// open across xacts, seals on a deadline armed at first row of a fresh
/// INSERT.
pub(crate) const DEFAULT_FLUSH_TIMEOUT_MS: u64 = 0;

/// Rows one decode worker coalesces before routing
pub(crate) const DEFAULT_DECODE_CHUNK_ROWS: usize = 1024;

/// Per-replica connection + mapping config. TOML `[ch]` table holds
/// connection params, `[table.<namespace>.<relname>]` blocks declare
/// per-relation mapping; parse via [`EmitterConfig::from_toml_str`].
#[derive(Debug, Clone)]
pub struct EmitterConfig {
    pub host: String,
    pub port: u16,
    pub database: String,
    pub user: String,
    pub password: String,
    /// Wrap native protocol in TLS (rustls, public webpki roots). Set
    /// for ClickHouse Cloud, whose secure native port (9440) speaks
    /// native-over-TLS. SNI + cert verification key off `host`.
    pub secure: bool,
    /// Custom rustls roots/config for `secure` path: private CA, pinned
    /// self-signed cert, or mTLS. `None` uses public webpki roots via
    /// [`clickhouse_c::tls::default_config`]. Not parsed from TOML;
    /// carried through reconnect + DDL applicator so every CH socket
    /// pins the same roots.
    pub tls_config: Option<Arc<clickhouse_c::tls::rustls::ClientConfig>>,
    pub compression: CompressionChoice,
    pub row_budget: usize,
    pub byte_budget: usize,
    /// Hold INSERTs open across xacts. Timer starts at first row of a
    /// fresh INSERT, trips at `now + flush_timeout`; on trip emitter
    /// closes every still-open INSERT and advances its durable-LSN
    /// horizon. `Duration::ZERO` (default): every xact closes its own
    /// INSERTs, ack tracks `drain_lsn` exactly. Latency cap: a buffered
    /// row is at most `flush_timeout` from durable. Throughput: small
    /// commits coalesce into one MergeTree part per flush window.
    pub flush_timeout: Duration,
    pub tables: HashMap<RelName, TableMapping>,
    /// Per-table initial-load mode from TOML `[table.*]` blocks. Applies at
    /// boot for pinned mappings; SQL opt-ins carry their own mode.
    pub table_initial_loads: HashMap<RelName, String>,
    pub table_opt_ins: HashMap<RelName, TableRow>,
    /// `[stream] paused`: pump idles (stops consuming source WAL) when true.
    /// Live via reload.
    pub paused: bool,
    /// Per-namespace defaults keyed on PG schema name; per-table
    /// entries in `tables` win for the relation they name
    pub namespaces: HashMap<String, NamespaceMapping>,
    /// Global `--drop-table-strategy` default; per-namespace override
    /// via `[namespace.<ns>] drop_table_strategy = ...`
    pub drop_table_strategy: String,
    pub retry: RetryConfig,
    /// Wall-clock limit for one INSERT attempt. If connection stalls
    /// mid-INSERT, return retryable [`EmitterError::Timeout`] so inserter
    /// reconnects and resends without blocking durable watermark
    /// Set well above healthy round-trip time
    pub insert_timeout: Duration,
    /// A CH connection idle longer than this may be half-open (NAT/LB/CH
    /// idle-reap); reconnect before the next op instead of blocking the
    /// full `insert_timeout` on a dead socket. Guards the start of a run,
    /// when connections sat idle since the previous one.
    pub idle_reconnect: Duration,
    /// Keep `_is_deleted` out of `ReplacingMergeTree`'s args so delete
    /// tombstones stay queryable instead of collapsing on FINAL. Column
    /// always emitted; off by default
    pub soft_delete: bool,
    /// Where externally-TOASTed chunks live + miss policy. `[toast]` block;
    /// default disabled (NULL/default-fill unrecoverable values)
    pub toast: ToastConfig,
    /// Rows a decode worker coalesces before routing one chunk to the
    /// batcher (`DEFAULT_DECODE_CHUNK_ROWS`). Tunable so
    /// tests can trip the mid-loop flush without a huge xact.
    pub decode_chunk_rows: usize,
    /// Row / byte budget per commit-drain slice
    /// ([`crate::xact::xact_buffer::CommittedDrain::next_batch`]). Bounds decoded
    /// heap rows resident while a spilled xact streams back; TOAST chunk
    /// generations stay per-xact.
    pub drain_batch_rows: usize,
    pub drain_batch_bytes: usize,
    /// `[runtime_config] schema`: source-PG schema housing the `config_*`
    /// overlay tables. `None` (field empty or omitted) disables the whole
    /// overlay subsystem — no boot seed, no config_decoder, pure TOML+CLI.
    pub runtime_config_schema: Option<String>,
    /// `[source] slot`: physical replication slot to create + stream from on
    /// the source. `Some` reserves WAL so a stalled/disconnected consumer
    /// resumes without recycling; `None` runs slotless. Boot-only.
    pub source_slot: Option<String>,
    /// `[memory] resident_payload_max`: global resident payload permit
    /// pool ([`crate::budget::MemoryBudget`])
    pub resident_payload_max: usize,
    /// `[memory] inline_value_max`: hard per-value decode-target cap;
    /// also sizes the budget's leaf reserve per decode worker. Reserve
    /// (`decoder_pool * inline_value_max`) must fit half
    /// `resident_payload_max` (validated at pipeline spawn)
    pub inline_value_max: usize,
    /// `[backup]`: archive storage for object-store bootstrap + WAL refill.
    /// `None` (section omitted) disables refill.
    pub backup: Option<walrus::config::Settings>,
}

pub(crate) const DEFAULT_RESIDENT_PAYLOAD_MAX: usize = 512 << 20;
pub(crate) const DEFAULT_INLINE_VALUE_MAX: usize = 64 << 20;

pub(crate) const DEFAULT_INSERT_TIMEOUT_SECS: u64 = 30;
pub(crate) const DEFAULT_IDLE_RECONNECT_SECS: u64 = 30;

/// Bounded-retry knobs. Retryable error (IO, clickhouse-c protocol,
/// ServerException) triggers reconnect + retry up to `max_attempts`
/// with exponential backoff capped at `max_backoff`.
#[derive(Debug, Clone)]
pub struct RetryConfig {
    pub max_attempts: u32,
    pub initial_backoff: std::time::Duration,
    pub max_backoff: std::time::Duration,
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
        Self {
            host: "localhost".into(),
            port: 9000,
            database: "default".into(),
            user: "default".into(),
            password: String::new(),
            secure: false,
            tls_config: None,
            compression: CompressionChoice::default(),
            row_budget: DEFAULT_ROW_BUDGET,
            byte_budget: DEFAULT_BYTE_BUDGET,
            flush_timeout: Duration::from_millis(DEFAULT_FLUSH_TIMEOUT_MS),
            tables: HashMap::new(),
            table_initial_loads: HashMap::new(),
            table_opt_ins: HashMap::new(),
            paused: false,
            namespaces: HashMap::new(),
            drop_table_strategy: "retain".into(),
            retry: RetryConfig::default(),
            insert_timeout: Duration::from_secs(DEFAULT_INSERT_TIMEOUT_SECS),
            idle_reconnect: Duration::from_secs(DEFAULT_IDLE_RECONNECT_SECS),
            soft_delete: false,
            toast: ToastConfig::default(),
            decode_chunk_rows: DEFAULT_DECODE_CHUNK_ROWS,
            drain_batch_rows: DEFAULT_DRAIN_BATCH_ROWS,
            drain_batch_bytes: DEFAULT_DRAIN_BATCH_BYTES,
            runtime_config_schema: None,
            source_slot: None,
            resident_payload_max: DEFAULT_RESIDENT_PAYLOAD_MAX,
            inline_value_max: DEFAULT_INLINE_VALUE_MAX,
            backup: None,
        }
    }
}

fn backup_err(msg: impl std::fmt::Display) -> EmitterError {
    EmitterError::Config(format!("[backup] {msg}"))
}

fn backup_str(tbl: &toml::value::Table, key: &str) -> Option<String> {
    tbl.get(key)
        .and_then(toml::Value::as_str)
        .map(str::to_string)
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
        tbl: &toml::value::Table,
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
        tbl: &toml::value::Table,
    ) -> Result<walrus::config::StorageSettings, EmitterError> {
        use walrus::storage::s3::{CredentialSource, Credentials, ImdsProvider, S3Config};
        let (bucket, prefix) = split_bucket_prefix(rest);
        let creds = match (backup_str(tbl, "access_key"), backup_str(tbl, "secret_key")) {
            (Some(access_key), Some(secret_key)) => CredentialSource::Static(Credentials {
                access_key,
                secret_key,
                session_token: backup_str(tbl, "session_token"),
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
            region: backup_str(tbl, "region").unwrap_or_else(|| "us-east-1".into()),
            creds,
            endpoint: backup_str(tbl, "endpoint"),
            force_path_style: tbl
                .get("force_path_style")
                .and_then(toml::Value::as_bool)
                .unwrap_or(false),
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
        tbl: &toml::value::Table,
    ) -> Result<walrus::config::StorageSettings, EmitterError> {
        let (bucket, prefix) = split_bucket_prefix(rest);
        Ok(walrus::config::StorageSettings::Gcs(
            walrus::storage::gcs::GcsConfig {
                bucket,
                prefix,
                credentials_path: backup_str(tbl, "credentials_path"),
                endpoint: backup_str(tbl, "endpoint"),
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
        _tbl: &toml::value::Table,
    ) -> Result<walrus::config::StorageSettings, EmitterError> {
        Ok(walrus::config::StorageSettings::Fs {
            path: rest.to_string(),
        })
    }
}

/// `[backup]` table → `walrus::config::Settings`. The `archive` URI prefix
/// dispatches to the matching [`BackupBackend`].
fn parse_backup(tbl: &toml::value::Table) -> Result<walrus::config::Settings, EmitterError> {
    let backends: [&dyn BackupBackend; 3] = [&S3Backend, &GcsBackend, &FsBackend];
    let archive = backup_str(tbl, "archive")
        .ok_or_else(|| backup_err("archive required (s3://, gs://, or file://)"))?;
    for backend in backends {
        if let Some(rest) = archive.strip_prefix(backend.prefix()) {
            return Ok(walrus::config::Settings {
                storage: backend.build(rest, tbl)?,
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
        &self.host
    }

    fn port(&self) -> u16 {
        self.port
    }

    fn database(&self) -> &str {
        &self.database
    }

    fn user(&self) -> &str {
        &self.user
    }

    fn password(&self) -> &str {
        &self.password
    }

    fn secure(&self) -> bool {
        self.secure
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

impl EmitterConfig {
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
    /// [table.public.foo]     # [table.<namespace>.<relname>], quote weird names
    /// replicate = true
    /// initial_load = "none"  # one of none / copy / base_backup / object_store
    /// target_database = "default"  # optional: namespace override, else [ch] database
    /// target_table = "foo"         # optional: source relname
    /// columns = [
    ///   { attnum = 1, target = "id",   type = "UInt64" },
    ///   { attnum = 2, target = "name", type = "Nullable(String)" },
    /// ]
    /// ```
    pub fn from_toml_str(s: &str) -> Result<Self, EmitterError> {
        let root: toml::Table = toml::from_str(s)
            .map_err(|e: toml::de::Error| EmitterError::Config(format!("toml: {e}")))?;
        Self::from_table(&root)
    }

    /// Build from an already-parsed (and possibly conf.d-merged) TOML table.
    pub fn from_table(root: &toml::Table) -> Result<Self, EmitterError> {
        use toml::Value;
        let mut out = Self::default();
        if let Some(ch) = root.get("ch").and_then(Value::as_table) {
            if let Some(v) = ch.get("host").and_then(Value::as_str) {
                out.host = v.into();
            }
            if let Some(v) = ch.get("port").and_then(Value::as_integer) {
                out.port = u16::try_from(v)
                    .map_err(|_| EmitterError::Config(format!("port {v} out of u16 range")))?;
            }
            if let Some(v) = ch.get("database").and_then(Value::as_str) {
                out.database = v.into();
            }
            if let Some(v) = ch.get("user").and_then(Value::as_str) {
                out.user = v.into();
            }
            if let Some(v) = ch.get("password").and_then(Value::as_str) {
                out.password = v.into();
            }
            if let Some(v) = ch.get("secure").and_then(Value::as_bool) {
                out.secure = v;
            }
            if let Some(v) = ch.get("compression").and_then(Value::as_str) {
                out.compression = CompressionChoice::parse(v)?;
            }
            if let Some(v) = ch.get("row_budget").and_then(Value::as_integer) {
                out.row_budget = usize::try_from(v).unwrap_or(DEFAULT_ROW_BUDGET);
            }
            if let Some(v) = ch.get("byte_budget").and_then(Value::as_integer) {
                out.byte_budget = usize::try_from(v).unwrap_or(DEFAULT_BYTE_BUDGET);
            }
            if let Some(v) = ch.get("drain_batch_rows").and_then(Value::as_integer) {
                out.drain_batch_rows = usize::try_from(v).unwrap_or(DEFAULT_DRAIN_BATCH_ROWS);
            }
            if let Some(v) = ch.get("drain_batch_bytes").and_then(Value::as_integer) {
                out.drain_batch_bytes = usize::try_from(v).unwrap_or(DEFAULT_DRAIN_BATCH_BYTES);
            }
            if let Some(v) = ch.get("flush_timeout_ms").and_then(Value::as_integer)
                && let Ok(ms) = u64::try_from(v)
            {
                out.flush_timeout = Duration::from_millis(ms);
            }
            if let Some(v) = ch.get("retry_max_attempts").and_then(Value::as_integer) {
                out.retry.max_attempts = u32::try_from(v).unwrap_or(out.retry.max_attempts);
            }
            if let Some(v) = ch
                .get("retry_initial_backoff_ms")
                .and_then(Value::as_integer)
                && let Ok(ms) = u64::try_from(v)
            {
                out.retry.initial_backoff = std::time::Duration::from_millis(ms);
            }
            if let Some(v) = ch.get("retry_max_backoff_ms").and_then(Value::as_integer)
                && let Ok(ms) = u64::try_from(v)
            {
                out.retry.max_backoff = std::time::Duration::from_millis(ms);
            }
            if let Some(v) = ch.get("drop_table_strategy").and_then(Value::as_str) {
                out.drop_table_strategy = v.into();
            }
            if let Some(v) = ch.get("soft_delete").and_then(Value::as_bool) {
                out.soft_delete = v;
            }
        }
        if let Some(toast) = root.get("toast").and_then(Value::as_table)
            && let Some(v) = toast.get("mode").and_then(Value::as_str)
        {
            out.toast.mode = ToastMode::parse(v).map_err(EmitterError::Config)?;
        }
        if let Some(mem) = root.get("memory").and_then(Value::as_table) {
            if let Some(v) = mem.get("resident_payload_max").and_then(Value::as_integer) {
                out.resident_payload_max =
                    usize::try_from(v).unwrap_or(DEFAULT_RESIDENT_PAYLOAD_MAX);
            }
            if let Some(v) = mem.get("inline_value_max").and_then(Value::as_integer) {
                out.inline_value_max = usize::try_from(v).unwrap_or(DEFAULT_INLINE_VALUE_MAX);
            }
        }
        if let Some(rc) = root.get("runtime_config").and_then(Value::as_table)
            && let Some(schema) = rc.get("schema").and_then(Value::as_str)
            && !schema.is_empty()
        {
            // Empty string == omitted == overlay disabled.
            out.runtime_config_schema = Some(schema.into());
        }
        if let Some(v) = root
            .get("stream")
            .and_then(Value::as_table)
            .and_then(|t| t.get("paused"))
            .and_then(Value::as_bool)
        {
            out.paused = v;
        }
        if let Some(src) = root.get("source").and_then(Value::as_table)
            && let Some(slot) = src.get("slot").and_then(Value::as_str)
            && !slot.is_empty()
        {
            // Empty string == omitted == slotless.
            out.source_slot = Some(slot.into());
        }
        if let Some(bk) = root.get("backup").and_then(Value::as_table) {
            out.backup = Some(parse_backup(bk)?);
        }
        if let Some(nss) = root.get("namespace").and_then(Value::as_table) {
            for (k, v) in nss {
                let t = v.as_table().ok_or_else(|| {
                    EmitterError::Config(format!("namespace.{k}: expected a table"))
                })?;
                let target_database = t
                    .get("target_database")
                    .and_then(Value::as_str)
                    .map(String::from);
                let auto_create = t
                    .get("auto_create")
                    .and_then(Value::as_bool)
                    .unwrap_or(false);
                let drop_table_strategy = t
                    .get("drop_table_strategy")
                    .and_then(Value::as_str)
                    .map(String::from);
                out.namespaces.insert(
                    k.clone(),
                    NamespaceMapping {
                        target_database,
                        auto_create,
                        drop_table_strategy,
                    },
                );
            }
        }
        if let Some(tbls) = root.get("table").and_then(Value::as_table) {
            // Two key levels: [table.<namespace>.<relname>]. Names with
            // weird characters (dots included) quote per TOML key rules
            for (ns, rels) in tbls {
                let rels = rels.as_table().ok_or_else(|| {
                    EmitterError::Config(format!(
                        "table.{ns}: expected [table.<namespace>.<relname>] blocks"
                    ))
                })?;
                for (name, v) in rels {
                    let t = v.as_table().ok_or_else(|| {
                        EmitterError::Config(format!("table.{ns}.{name}: expected a table"))
                    })?;
                    let replicate = t.get("replicate").and_then(Value::as_bool);
                    let rel = RelName::new(ns, name);
                    let Some(cols_v) = t.get("columns").and_then(Value::as_array) else {
                        out.table_opt_ins.insert(
                            rel,
                            TableRow {
                                target_database: t
                                    .get("target_database")
                                    .and_then(Value::as_str)
                                    .map(String::from),
                                target_table: t
                                    .get("target_table")
                                    .and_then(Value::as_str)
                                    .map(String::from),
                                replicate,
                                initial_load: t
                                    .get("initial_load")
                                    .and_then(Value::as_str)
                                    .map(String::from),
                            },
                        );
                        continue;
                    };
                    if replicate == Some(false) {
                        continue;
                    }
                    let database = t
                        .get("target_database")
                        .and_then(Value::as_str)
                        .or_else(|| {
                            out.namespaces
                                .get(ns.as_str())
                                .and_then(|n| n.target_database.as_deref())
                        })
                        .unwrap_or(&out.database)
                        .to_string();
                    let table = t
                        .get("target_table")
                        .and_then(Value::as_str)
                        .unwrap_or(name)
                        .to_string();
                    let mut columns = Vec::with_capacity(cols_v.len());
                    for (i, c) in cols_v.iter().enumerate() {
                        let ct = c.as_table().ok_or_else(|| {
                            EmitterError::Config(format!(
                                "table.{ns}.{name}.columns[{i}]: expected a table"
                            ))
                        })?;
                        let src_attnum =
                            ct.get("attnum")
                                .and_then(Value::as_integer)
                                .ok_or_else(|| {
                                    EmitterError::Config(format!(
                                        "table.{ns}.{name}.columns[{i}]: missing attnum"
                                    ))
                                })?;
                        let target_name = ct
                            .get("target")
                            .and_then(Value::as_str)
                            .ok_or_else(|| {
                                EmitterError::Config(format!(
                                    "table.{ns}.{name}.columns[{i}]: missing target"
                                ))
                            })?
                            .to_string();
                        let target_type = ct
                            .get("type")
                            .and_then(Value::as_str)
                            .ok_or_else(|| {
                                EmitterError::Config(format!(
                                    "table.{ns}.{name}.columns[{i}]: missing type"
                                ))
                            })?
                            .to_string();
                        columns.push(ColumnMapping {
                            src_attnum: i16::try_from(src_attnum).map_err(|_| {
                                EmitterError::Config(format!(
                                    "table.{ns}.{name}.columns[{i}].attnum {src_attnum} out of i16 range"
                                ))
                            })?,
                            target_name,
                            target_type,
                        });
                    }
                    out.tables.insert(
                        rel.clone(),
                        TableMapping {
                            target: TableTarget { database, table },
                            columns,
                        },
                    );
                    if let Some(mode) = t.get("initial_load").and_then(Value::as_str) {
                        out.table_initial_loads.insert(rel, mode.to_string());
                    }
                }
            }
        }
        Ok(out)
    }
}

/// Cached plan for one destination table, built lazily on first row.
pub(crate) struct TablePlan {
    pub(crate) columns: Vec<ColumnPlan>,
    pub(crate) synth_lsn: ColumnPlan,
    pub(crate) synth_xid: ColumnPlan,
    pub(crate) synth_commit_ts: ColumnPlan,
    /// `_is_deleted Bool` (1 on delete, else 0), always appended last
    pub(crate) synth_is_deleted: ColumnPlan,
    /// Pre-formatted so on-tuple paths don't reassemble per row
    pub(crate) insert_sql: String,
}

pub(crate) struct ColumnPlan {
    pub(crate) name: String,
    pub(crate) type_repr: String,
    pub(crate) ast: TypeAst,
    pub(crate) decimal: Option<DecimalWire>,
}

/// Physical wire width of a CH `Decimal`: one of four signed-integer
/// backings. Discriminants are the byte widths, so `as usize` recovers
/// the size.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum DecimalWidth {
    D32 = 4,
    D64 = 8,
    D128 = 16,
    D256 = 32,
}

impl DecimalWidth {
    fn from_elem_size(size: usize) -> Option<Self> {
        Some(match size {
            4 => Self::D32,
            8 => Self::D64,
            16 => Self::D128,
            32 => Self::D256,
            _ => return None,
        })
    }

    fn bytes(self) -> usize {
        self as usize
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct DecimalWire {
    pub(crate) scale: u8,
    pub(crate) width: DecimalWidth,
}

impl TablePlan {
    /// Synthetic columns always non-nullable (emitter always populates).
    ///
    /// `column_overrides` is the `config_column` overlay slice for this rel
    /// (source attname → CH type). Resolved here because this is where the
    /// descriptor meets the mapping: attname→attnum comes from `rel`, and an
    /// inadmissible override (see [`override_wire`]) falls back to the
    /// mapping's type with a WARN — a config row must degrade, never poison
    /// the batcher (Regime A).
    pub(crate) fn build(
        alloc: Allocator,
        rel: &RelDescriptor,
        mapping: &TableMapping,
        column_overrides: Option<&HashMap<String, String>>,
    ) -> Result<Self, EmitterError> {
        let mut columns = Vec::with_capacity(mapping.columns.len());
        let mut col_sql = Vec::with_capacity(mapping.columns.len() + 4);
        // Mapping attnums absent from the catalog descriptor are not a
        // hard error: schema-evolution pre-declares post-ALTER columns,
        // pre-ALTER xacts legitimately see fewer attnums. append_row
        // emits NULL for any missing attnum, so a static-config typo
        // surfaces as an always-NULL column (or CH reject if non-nullable)
        for c in &mapping.columns {
            let ast = TypeAst::parse(&c.target_type, alloc)
                .map_err(|e| EmitterError::Type(format!("{}: {e}", c.target_type)))?;
            let decimal = decimal_wire_of(&ast);
            let mut plan = ColumnPlan {
                name: c.target_name.clone(),
                type_repr: c.target_type.clone(),
                ast,
                decimal,
            };
            if let Some(ov) = column_overrides
                && let Some(ty) = rel
                    .attributes
                    .iter()
                    .find(|a| a.attnum == c.src_attnum && !a.dropped)
                    .and_then(|a| ov.get(&a.name))
            {
                match TypeAst::parse(ty, alloc) {
                    Ok(oast) => {
                        if let Some(decimal) = override_wire(&plan.ast, &oast) {
                            plan = ColumnPlan {
                                name: plan.name,
                                type_repr: ty.clone(),
                                ast: oast,
                                decimal,
                            };
                        } else {
                            tracing::warn!(
                                target: "walshadow::emitter",
                                qname = %rel.rel_name,
                                column = %c.target_name,
                                default = %c.target_type,
                                value = %ty,
                                "config_column.target_type not wire-compatible; keeping default",
                            );
                        }
                    }
                    Err(e) => tracing::warn!(
                        target: "walshadow::emitter",
                        qname = %rel.rel_name,
                        column = %c.target_name,
                        value = %ty,
                        error = %e,
                        "config_column.target_type unparseable; keeping default",
                    ),
                }
            }
            columns.push(plan);
            col_sql.push(quote_ident(&c.target_name));
        }
        let mk = |name: &str, ty: &str| -> Result<ColumnPlan, EmitterError> {
            Ok(ColumnPlan {
                name: name.into(),
                type_repr: ty.into(),
                ast: TypeAst::parse(ty, alloc)
                    .map_err(|e| EmitterError::Type(format!("{ty}: {e}")))?,
                decimal: None,
            })
        };
        let synth_lsn = mk("_lsn", "UInt64")?;
        let synth_xid = mk("_xid", "UInt32")?;
        let synth_commit_ts = mk("_commit_ts", "DateTime64(6, 'UTC')")?;
        let synth_is_deleted = mk("_is_deleted", "Bool")?;
        col_sql.push(quote_ident(&synth_lsn.name));
        col_sql.push(quote_ident(&synth_xid.name));
        col_sql.push(quote_ident(&synth_commit_ts.name));
        col_sql.push(quote_ident(&synth_is_deleted.name));
        let insert_sql = format!(
            "INSERT INTO {} ({}) FORMAT Native",
            mapping.target.sql(),
            col_sql.join(", "),
        );
        Ok(Self {
            columns,
            synth_lsn,
            synth_xid,
            synth_commit_ts,
            synth_is_deleted,
            insert_sql,
        })
    }
}

/// Per-table per-xact accumulator, one block buffer per CH column.
pub(crate) struct TableEncoder {
    pub(crate) plan: TablePlan,
    pub(crate) rows: usize,
    pub(crate) approx_bytes: usize,
    /// Mirrors `plan.columns + 4 synth`
    pub(crate) buffers: Vec<ColumnBuf>,
}

/// On-the-wire-shape column buffer. [`BlockBuilder`] borrows these
/// slices at flush time; cleared after `send_data`.
pub(crate) enum ColumnBuf {
    /// `width` bytes per row, packed little-endian
    Fixed { width: usize, bytes: Vec<u8> },
    /// `offsets[i]` is the cumulative exclusive end of row `i` in `data`
    String { offsets: Vec<u64>, data: Vec<u8> },
    /// `null_map[i] = 1` means NULL; zero bytes go into `inner` for null
    /// rows so the slab stays dense
    NullableFixed {
        width: usize,
        null_map: Vec<u8>,
        inner: Vec<u8>,
    },
    NullableString {
        offsets: Vec<u64>,
        data: Vec<u8>,
        null_map: Vec<u8>,
    },
}

impl ColumnBuf {
    /// Width comes from clickhouse-c `chc_type_elem_size` so
    /// FixedString(N), DateTime64, Decimal*, Enum resolve without
    /// walshadow mirroring the type-string surface. `elem_size == 0`
    /// means varlen on-wire shape; only `String` is handled, other
    /// varlens fall through to the String arms and die on first `append`.
    fn new_for_ast(ast: &TypeAst) -> Result<Self, EmitterError> {
        let view = ast.view();
        let (nullable, inner) = if view.kind() == Some(Kind::Nullable) {
            (
                true,
                view.child(0)
                    .ok_or_else(|| EmitterError::Type("Nullable type with no child".into()))?,
            )
        } else {
            (false, view)
        };
        let elem = inner.elem_size();
        Ok(match (nullable, elem) {
            (false, 0) => Self::String {
                offsets: Vec::new(),
                data: Vec::new(),
            },
            (true, 0) => Self::NullableString {
                offsets: Vec::new(),
                data: Vec::new(),
                null_map: Vec::new(),
            },
            (false, w) => Self::Fixed {
                width: w,
                bytes: Vec::new(),
            },
            (true, w) => Self::NullableFixed {
                width: w,
                null_map: Vec::new(),
                inner: Vec::new(),
            },
        })
    }

    fn approx_size(&self) -> usize {
        match self {
            Self::Fixed { bytes, .. } => bytes.len(),
            Self::String { offsets, data } => offsets.len() * 8 + data.len(),
            Self::NullableFixed {
                null_map, inner, ..
            } => null_map.len() + inner.len(),
            Self::NullableString {
                offsets,
                data,
                null_map,
            } => offsets.len() * 8 + data.len() + null_map.len(),
        }
    }

    fn append_null(&mut self) -> Result<(), EmitterError> {
        match self {
            Self::NullableFixed {
                width,
                null_map,
                inner,
            } => {
                null_map.push(1);
                inner.extend(std::iter::repeat_n(0u8, *width));
                Ok(())
            }
            Self::NullableString {
                offsets,
                data,
                null_map,
            } => {
                null_map.push(1);
                offsets.push(data.len() as u64);
                Ok(())
            }
            _ => Err(EmitterError::UnsupportedValue {
                target_column: String::new(),
                kind: "NULL for non-Nullable column",
            }),
        }
    }

    /// Type default for absent values where NULL is unrepresentable: zero
    /// bytes for fixed shapes (0 / epoch / empty FixedString, matching CH
    /// column DEFAULT), empty string for varlen. Nullable shapes keep NULL,
    /// the more faithful absence marker
    fn append_default(&mut self) {
        match self {
            Self::Fixed { width, bytes } => bytes.extend(std::iter::repeat_n(0u8, *width)),
            Self::String { offsets, data } => offsets.push(data.len() as u64),
            nullable => nullable.append_null().expect("nullable shape takes NULL"),
        }
    }

    fn append_fixed_bytes(&mut self, le: &[u8]) -> Result<(), EmitterError> {
        match self {
            Self::Fixed { width, bytes } => {
                if le.len() != *width {
                    return Err(EmitterError::Type(format!(
                        "fixed-width mismatch: expected {} bytes, got {}",
                        *width,
                        le.len()
                    )));
                }
                bytes.extend_from_slice(le);
                Ok(())
            }
            Self::NullableFixed {
                width,
                null_map,
                inner,
            } => {
                if le.len() != *width {
                    return Err(EmitterError::Type(format!(
                        "nullable-fixed-width mismatch: expected {} bytes, got {}",
                        *width,
                        le.len()
                    )));
                }
                null_map.push(0);
                inner.extend_from_slice(le);
                Ok(())
            }
            _ => Err(EmitterError::UnsupportedValue {
                target_column: String::new(),
                kind: "fixed-width value into string-shaped buffer",
            }),
        }
    }

    fn append_string_bytes(&mut self, raw: &[u8]) -> Result<(), EmitterError> {
        match self {
            Self::String { offsets, data } => {
                data.extend_from_slice(raw);
                offsets.push(data.len() as u64);
                Ok(())
            }
            Self::NullableString {
                offsets,
                data,
                null_map,
            } => {
                null_map.push(0);
                data.extend_from_slice(raw);
                offsets.push(data.len() as u64);
                Ok(())
            }
            _ => Err(EmitterError::UnsupportedValue {
                target_column: String::new(),
                kind: "string value into fixed-shaped buffer",
            }),
        }
    }
}

/// Fresh per-column buffers matching `plan` (mapped + four synthetic).
/// Shared by [`TableEncoder::new`] and [`TableEncoder::take_block`] so
/// synthetic-column widths live in one place.
pub(crate) fn fresh_buffers(plan: &TablePlan) -> Result<Vec<ColumnBuf>, EmitterError> {
    let mut buffers = Vec::with_capacity(plan.columns.len() + 4);
    for c in &plan.columns {
        buffers.push(ColumnBuf::new_for_ast(&c.ast)?);
    }
    buffers.push(ColumnBuf::Fixed {
        width: 8,
        bytes: Vec::new(),
    }); // _lsn UInt64
    buffers.push(ColumnBuf::Fixed {
        width: 4,
        bytes: Vec::new(),
    }); // _xid UInt32
    buffers.push(ColumnBuf::Fixed {
        width: 8,
        bytes: Vec::new(),
    }); // _commit_ts DateTime64(6)
    buffers.push(ColumnBuf::Fixed {
        width: 1,
        bytes: Vec::new(),
    }); // _is_deleted Bool (1 wire byte, same as UInt8)
    Ok(buffers)
}

impl TableEncoder {
    pub(crate) fn new(plan: TablePlan) -> Result<Self, EmitterError> {
        let buffers = fresh_buffers(&plan)?;
        Ok(Self {
            plan,
            rows: 0,
            approx_bytes: 0,
            buffers,
        })
    }

    /// Swap accumulated slabs out for fresh empties, returning old slabs
    /// + row count. Transfers ownership rather than reusing allocations.
    pub(crate) fn take_block(&mut self) -> Result<(Vec<ColumnBuf>, usize), EmitterError> {
        let fresh = fresh_buffers(&self.plan)?;
        let old = std::mem::replace(&mut self.buffers, fresh);
        let rows = self.rows;
        self.rows = 0;
        self.approx_bytes = 0;
        Ok((old, rows))
    }

    /// Caller picks the source side (DELETE uses `old`) and the op code.
    pub fn append_row(
        &mut self,
        committed: &CommittedTuple,
        mapping: &TableMapping,
        op_code: i8,
    ) -> Result<(), EmitterError> {
        let decoded = &committed.decoded;
        let side = match decoded.op {
            HeapOp::Delete => decoded.old.as_ref(),
            _ => decoded.new.as_ref(),
        };
        for (i, col) in mapping.columns.iter().enumerate() {
            let decimal = self.plan.columns[i].decimal;
            let buf = &mut self.buffers[i];
            let raw_value = side
                .and_then(|t| t.columns.get((col.src_attnum - 1) as usize))
                .and_then(|opt| opt.as_ref());
            match raw_value {
                // Absent / NULL coerces: Nullable target takes NULL,
                // non-Nullable the type default. Covers key-only delete
                // tombstones under non-FULL replica identity and NULL
                // source values mapped onto non-Nullable columns
                None | Some(ColumnValue::Null) => buf.append_default(),
                Some(v) => encode_value(buf, v, decimal).map_err(|mut e| {
                    if let EmitterError::UnsupportedValue {
                        ref mut target_column,
                        ..
                    } = e
                    {
                        *target_column = col.target_name.clone();
                    }
                    e
                })?,
            }
        }
        // Synthetic columns: _lsn, _xid, _commit_ts (unix micros), _is_deleted
        let off = mapping.columns.len();
        push_fixed(&mut self.buffers[off], &decoded.source_lsn.to_le_bytes())?;
        push_fixed(&mut self.buffers[off + 1], &decoded.xid.to_le_bytes())?;
        let unix_us = committed.commit_ts.saturating_add(DATETIME64_PG_EPOCH_US);
        push_fixed(&mut self.buffers[off + 2], &unix_us.to_le_bytes())?;
        let is_deleted: u8 = (op_code == OP_DELETE).into();
        push_fixed(&mut self.buffers[off + 3], &is_deleted.to_le_bytes())?;
        self.rows += 1;
        self.approx_bytes = self.buffers.iter().map(ColumnBuf::approx_size).sum();
        Ok(())
    }
}

/// Innermost leaf node per column: the `Nullable` value slab, or the whole
/// column when not nullable. A `ColumnBuilder` node borrows its slabs and
/// cannot move once a wrapper or the block aliases it, so the caller owns
/// these leaves for the block's lifetime. Buffers stay immutable until
/// `send_data` returns.
pub(crate) fn build_leaves(
    bufs: &[ColumnBuf],
    n_rows: usize,
) -> Result<Vec<ColumnBuilder<'_>>, EmitterError> {
    bufs.iter()
        .map(|buf| match buf {
            ColumnBuf::Fixed { width, bytes: data }
            | ColumnBuf::NullableFixed {
                width, inner: data, ..
            } => ColumnBuilder::fixed(data, *width, n_rows).map_err(Into::into),
            ColumnBuf::String { offsets, data }
            | ColumnBuf::NullableString { offsets, data, .. } => {
                ColumnBuilder::string(offsets, data, n_rows).map_err(Into::into)
            }
        })
        .collect()
}

/// `Nullable` wrapper per column, `None` when the column is not nullable.
/// Each wrapper aliases its leaf in `leaves`, so `leaves` must be fully built
/// (no further pushes) and outlive the wrappers. Pair with [`build_leaves`]:
/// the block appends `roots[i]` when `Some`, else `leaves[i]`.
pub(crate) fn build_roots<'l, 'b: 'l>(
    leaves: &'l [ColumnBuilder<'b>],
    bufs: &'b [ColumnBuf],
) -> Result<Vec<Option<ColumnBuilder<'l>>>, EmitterError> {
    leaves
        .iter()
        .zip(bufs)
        .map(|(leaf, buf)| match buf {
            ColumnBuf::NullableFixed { null_map, .. }
            | ColumnBuf::NullableString { null_map, .. } => Ok(Some(leaf.nullable(null_map)?)),
            ColumnBuf::Fixed { .. } | ColumnBuf::String { .. } => Ok(None),
        })
        .collect()
}

fn push_fixed(buf: &mut ColumnBuf, le: &[u8]) -> Result<(), EmitterError> {
    buf.append_fixed_bytes(le)
}

/// Wire metadata for a (possibly `Nullable`) CH `Decimal`, else `None`.
/// Peels one `Nullable` layer like [`ColumnBuf::new_for_ast`].
fn decimal_wire_of(ast: &TypeAst) -> Option<DecimalWire> {
    let view = ast.view();
    let inner = if view.kind() == Some(Kind::Nullable) {
        view.child(0)?
    } else {
        view
    };
    if !matches!(
        inner.kind(),
        Some(Kind::Decimal32 | Kind::Decimal64 | Kind::Decimal128 | Kind::Decimal256)
    ) {
        return None;
    }
    Some(DecimalWire {
        scale: u8::try_from(inner.decimal_scale()).ok()?,
        width: DecimalWidth::from_elem_size(inner.elem_size())?,
    })
}

/// Buffer shape a type encodes into, Nullable-transparent (the null map is
/// orthogonal to the value wire).
enum WireShape {
    Fixed(usize),
    Str,
}

fn wire_shape_of(ast: &TypeAst) -> Option<(WireShape, Kind)> {
    let view = ast.view();
    let inner = if view.kind() == Some(Kind::Nullable) {
        view.child(0)?
    } else {
        view
    };
    let shape = match inner.elem_size() {
        0 => WireShape::Str,
        w => WireShape::Fixed(w),
    };
    Some((shape, inner.kind()?))
}

/// Whether a `config_column.target_type` override may replace `default` in
/// the encode plan, and the `DecimalWire` the plan should carry if so.
/// `encode_value` performs no arithmetic conversion — it writes the source
/// value's natural wire bytes — so an override is admissible only when
/// those bytes are valid wire data for the override type:
///
/// - Decimal-encoded source (`numeric`): any Decimal (the text→scaled path
///   converts), String (lossless text), or a signed Int32/64/128/256 as a
///   scale-0 decimal (the plan's acceptance drill: `numeric(38,0)` →
///   `Int128`). Unsigned ints rejected — a negative value would encode as
///   wrapped garbage
/// - String-shaped source: string-shaped override only
/// - Fixed-width source: same-width non-Decimal override (reinterpretation,
///   e.g. `Int32` → `UInt32`); Decimal rejected because a nonzero scale
///   would silently rescale the value
fn override_wire(default: &TypeAst, over: &TypeAst) -> Option<Option<DecimalWire>> {
    if decimal_wire_of(default).is_some() {
        if let Some(w) = decimal_wire_of(over) {
            return Some(Some(w));
        }
        return match wire_shape_of(over)? {
            (WireShape::Str, _) => Some(None),
            (WireShape::Fixed(w), Kind::Int32 | Kind::Int64 | Kind::Int128 | Kind::Int256) => {
                Some(Some(DecimalWire {
                    scale: 0,
                    width: DecimalWidth::from_elem_size(w)?,
                }))
            }
            _ => None,
        };
    }
    match (wire_shape_of(default)?.0, wire_shape_of(over)?.0) {
        (WireShape::Str, WireShape::Str) => Some(None),
        (WireShape::Fixed(a), WireShape::Fixed(b)) if a == b => {
            if decimal_wire_of(over).is_some() {
                None
            } else {
                Some(None)
            }
        }
        _ => None,
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct U256([u64; 4]);

impl U256 {
    fn is_zero(self) -> bool {
        self.0 == [0; 4]
    }

    fn one_shl(bit: usize) -> Self {
        let mut limbs = [0; 4];
        limbs[bit / 64] = 1u64 << (bit % 64);
        Self(limbs)
    }

    fn checked_mul_small(&mut self, rhs: u32) -> bool {
        let mut carry = 0u128;
        for limb in &mut self.0 {
            let v = (*limb as u128) * (rhs as u128) + carry;
            *limb = v as u64;
            carry = v >> 64;
        }
        carry == 0
    }

    fn checked_add_small(&mut self, rhs: u32) -> bool {
        let mut carry = rhs as u128;
        for limb in &mut self.0 {
            let v = (*limb as u128) + carry;
            *limb = v as u64;
            carry = v >> 64;
            if carry == 0 {
                return true;
            }
        }
        false
    }

    fn div_small(&mut self, rhs: u32) -> u32 {
        let mut rem = 0u128;
        let rhs = rhs as u128;
        for limb in self.0.iter_mut().rev() {
            let v = (rem << 64) | (*limb as u128);
            *limb = (v / rhs) as u64;
            rem = v % rhs;
        }
        rem as u32
    }

    fn to_le_bytes(self) -> [u8; 32] {
        let mut out = [0u8; 32];
        for (i, limb) in self.0.iter().enumerate() {
            out[i * 8..][..8].copy_from_slice(&limb.to_le_bytes());
        }
        out
    }
}

impl Ord for U256 {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        for (a, b) in self.0.iter().rev().zip(other.0.iter().rev()) {
            match a.cmp(b) {
                std::cmp::Ordering::Equal => {}
                ord => return ord,
            }
        }
        std::cmp::Ordering::Equal
    }
}

impl PartialOrd for U256 {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

fn decimal_oob() -> EmitterError {
    EmitterError::UnsupportedValue {
        target_column: String::new(),
        kind: "numeric out of range for Decimal column",
    }
}

fn decimal_type_error(msg: &str) -> EmitterError {
    EmitterError::Type(msg.into())
}

/// PG `numeric_out` text (eg `-12.340`) to the scaled integer a CH
/// `Decimal(_, scale)` stores: `value * 10^scale`, two's-complement
/// little-endian at the Decimal wire width. PG values conform to column
/// typmod so text dscale normally equals `scale`; rescale handles
/// integer-valued numerics and defensive target-scale overrides.
fn decimal_text_to_scaled_le(
    text: &str,
    scale: i32,
    width: DecimalWidth,
) -> Result<[u8; 32], EmitterError> {
    let width = width.bytes();
    if scale < 0 {
        return Err(decimal_type_error("Decimal column has negative scale"));
    }

    let neg = text.starts_with('-');
    // numeric_out emits a single optional '-'; a residual sign in `body`
    // is malformed and the parse loop rejects it as a non-digit
    let body = text.strip_prefix(['-', '+']).unwrap_or(text);
    let mut mag = U256::default();
    let mut frac_digits = 0i32;
    let mut seen_dot = false;
    let mut saw_digit = false;

    for b in body.bytes() {
        match b {
            b'.' if !seen_dot => seen_dot = true,
            b'0'..=b'9' => {
                saw_digit = true;
                if !mag.checked_mul_small(10) || !mag.checked_add_small((b - b'0') as u32) {
                    return Err(decimal_oob());
                }
                if seen_dot {
                    frac_digits += 1;
                }
            }
            _ => return Err(decimal_oob()),
        }
    }
    if !saw_digit {
        return Err(decimal_oob());
    }

    let diff = scale - frac_digits;
    if diff > 0 {
        for _ in 0..diff {
            if !mag.checked_mul_small(10) {
                return Err(decimal_oob());
            }
        }
    } else if diff < 0 {
        // More fractional digits than column scale: PG would have
        // rounded on store, so defensive trunc, shouldn't occur for
        // conforming values
        for _ in 0..-diff {
            mag.div_small(10);
        }
    }

    // Bound by physical wire width (signed Int{32,64,128,256} range),
    // not logical Decimal(p,s) precision 10^p. Backstop turning a
    // too-wide value (eg operator override onto a narrower Decimal) into
    // a clean error instead of a silently truncated store.
    let limit = U256::one_shl(width * 8 - 1);
    if (!neg && mag >= limit) || (neg && mag > limit) {
        return Err(decimal_oob());
    }

    let mut out = mag.to_le_bytes();
    if neg && !mag.is_zero() {
        for b in &mut out[..width] {
            *b = !*b;
        }
        let mut carry = 1u16;
        for b in &mut out[..width] {
            let v = (*b as u16) + carry;
            *b = v as u8;
            carry = v >> 8;
            if carry == 0 {
                break;
            }
        }
    }
    Ok(out)
}

fn encode_value(
    buf: &mut ColumnBuf,
    v: &ColumnValue,
    decimal: Option<DecimalWire>,
) -> Result<(), EmitterError> {
    match v {
        ColumnValue::Null => buf.append_null(),
        ColumnValue::Bool(b) => buf.append_fixed_bytes(&[*b as u8]),
        ColumnValue::Char(c) => buf.append_fixed_bytes(&c.to_le_bytes()),
        ColumnValue::Int2(n) => buf.append_fixed_bytes(&n.to_le_bytes()),
        ColumnValue::Int4(n) => buf.append_fixed_bytes(&n.to_le_bytes()),
        ColumnValue::Int8(n) => buf.append_fixed_bytes(&n.to_le_bytes()),
        ColumnValue::Float4(f) => buf.append_fixed_bytes(&f.to_le_bytes()),
        ColumnValue::Float8(f) => buf.append_fixed_bytes(&f.to_le_bytes()),
        ColumnValue::Oid(n) => buf.append_fixed_bytes(&n.to_le_bytes()),
        // saturating so PG ±infinity dates don't overflow
        ColumnValue::Date(n) => {
            buf.append_fixed_bytes(&n.saturating_add(DATE32_PG_EPOCH_DAYS).to_le_bytes())
        }
        // `time` → `Time64(6)`: microseconds since midnight, no epoch offset
        ColumnValue::Time(n) => buf.append_fixed_bytes(&n.to_le_bytes()),
        ColumnValue::Timestamp(n) | ColumnValue::TimestampTz(n) => {
            let unix_us = n.saturating_add(DATETIME64_PG_EPOCH_US);
            buf.append_fixed_bytes(&unix_us.to_le_bytes())
        }
        // `timetz` → text: CH has no zone-aware time type, text keeps
        // the offset the old fixed encoding dropped
        ColumnValue::TimeTz { micros, tz_seconds } => buf.append_string_bytes(
            crate::decode::codecs::timetz_to_text(*micros, *tz_seconds).as_bytes(),
        ),
        ColumnValue::Uuid(b) => buf.append_fixed_bytes(&crate::decode::codecs::uuid_to_ch_wire(b)),
        ColumnValue::Name(s) | ColumnValue::Text(s) | ColumnValue::Json(s) => {
            buf.append_string_bytes(s.as_bytes())
        }
        ColumnValue::Numeric(n) => {
            use crate::decode::codecs::NumericKind;
            match decimal {
                // Decimal column: non-finite (NaN/±Inf) is unrepresentable,
                // error rather than silently corrupt (operator maps the
                // column to String to recover)
                Some(decimal) => match n {
                    NumericKind::Finite(s) => {
                        let scaled =
                            decimal_text_to_scaled_le(s, i32::from(decimal.scale), decimal.width)?;
                        buf.append_fixed_bytes(&scaled[..decimal.width.bytes()])
                    }
                    NumericKind::NaN | NumericKind::PInf | NumericKind::NInf => {
                        Err(EmitterError::UnsupportedValue {
                            target_column: String::new(),
                            kind: "non-finite numeric (NaN/Inf) into Decimal column",
                        })
                    }
                },
                // String column: lossless text, including NaN/±Inf
                None => {
                    let txt: &str = match n {
                        NumericKind::Finite(s) => s.as_str(),
                        NumericKind::NaN => "NaN",
                        NumericKind::PInf => "Infinity",
                        NumericKind::NInf => "-Infinity",
                    };
                    buf.append_string_bytes(txt.as_bytes())
                }
            }
        }
        ColumnValue::Inet(v) => buf.append_string_bytes(v.to_text().as_bytes()),
        ColumnValue::Interval(v) => buf.append_string_bytes(v.to_text().as_bytes()),
        ColumnValue::Bytea(b) => buf.append_string_bytes(b),
        ColumnValue::ExternalToast(_) => Err(EmitterError::UnsupportedValue {
            target_column: String::new(),
            kind: "unresolved TOAST pointer (xact buffer should have reassembled)",
        }),
        // PgPending normally resolves to text earlier (BufferingDecoderSink
        // via the oracle extension). Still set here means extension absent;
        // fall back to raw on-disk bytes so CH still gets the value
        ColumnValue::PgPending { raw, .. } => buf.append_string_bytes(raw),
        ColumnValue::Unsupported { .. } => Err(EmitterError::UnsupportedValue {
            target_column: String::new(),
            kind: "unsupported PG type oid",
        }),
    }
}

crate::atomic_stats! {
    /// CH emitter counters. `fetch_add(_, Relaxed)`; status loop reads
    /// via `.load(Relaxed)`.
    pub struct EmitterStats {
        pub rows_emitted,
        pub blocks_sent,
        pub xacts_committed,
        pub unsupported_relations,
        /// Rows whose filenode resolved to a foreign database (physical
        /// WAL carries the whole cluster). Skipped, not an error.
        pub foreign_db_rows_skipped,
        pub unsupported_values,
        /// `retries_attempted` counts one per failing operation, not per
        /// attempt (one op needing 3 retries adds 3)
        pub reconnects,
        pub retries_attempted,
        pub truncates_emitted,
        /// Legacy serial-emitter counter; pooled pipeline seals via the
        /// batcher's own `flush_timeout` deadline and never bumps this
        pub flush_deadline_trips,
        pub toast_chunks_stored,
        pub toast_tombstones_stored,
        /// Toasted values reassembled from the store (not the in-xact buffer)
        pub toast_values_fetched,
        /// Toasted values NULL/default-filled because no store could rebuild
        /// them (disabled mode). Surfaced, never silent
        pub toast_values_filled_default,
        pub toast_values_filled_superseded,
        pub toast_values_filled_mismatch,
        pub toast_fetch_miss,
        /// Gauge: bytes resident in the bootstrap TOAST-deferred spool's
        /// in-memory prefix, zeroed after replay
        pub bootstrap_deferred_bytes,
        /// Gauge: encoded bytes in the bootstrap TOAST-deferred spool file
        pub bootstrap_deferred_spool_bytes,
        pub toast_mirror_truncates,
        pub toast_mirror_retires,
        /// Rewrite generations closed with residual `O - B` tombstones
        pub toast_rewrite_barriers,
        /// Stashed records decoded at commit against a resolved toast heap
        pub toast_stash_decoded,
        /// Stashed records discarded: filenode unresolvable post-commit
        /// (dropped or rotated away), end-state-neutral by AEL supersession
        pub toast_stash_discarded,
        /// Stashed records resolved to a non-toast heap; ordinary-heap decode
        /// stays fenced off until a shadow replay fence exists
        pub toast_stash_skipped,
        // Pipeline-flow counters; `_out`/`_in` pairs give channel depth. See
        // `metrics::render`.
        pub queue_jobs_out,
        pub decode_jobs_in,
        pub decode_rows_out,
        pub insertbatch_rows_in,
        pub insertbatch_batches_out,
        pub inserter_batches_in,
    }
}

// Manual Debug: deriving would dump the whole slab; report just lengths
impl std::fmt::Debug for ColumnBuf {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Fixed { width, bytes } => f
                .debug_struct("Fixed")
                .field("width", width)
                .field("bytes_len", &bytes.len())
                .finish(),
            Self::String { offsets, data } => f
                .debug_struct("String")
                .field("rows", &offsets.len())
                .field("data_len", &data.len())
                .finish(),
            Self::NullableFixed {
                width,
                null_map,
                inner,
            } => f
                .debug_struct("NullableFixed")
                .field("width", width)
                .field("rows", &null_map.len())
                .field("inner_len", &inner.len())
                .finish(),
            Self::NullableString {
                offsets,
                data,
                null_map,
            } => f
                .debug_struct("NullableString")
                .field("rows", &null_map.len())
                .field("offsets_len", &offsets.len())
                .field("data_len", &data.len())
                .finish(),
        }
    }
}

/// Load `--ch-config` and deep-merge every `*.toml` in the sibling conf.d
/// directory (`<ch-config>.d/`, e.g. `ch-config.toml` → `ch-config.d/`), in
/// lexical filename order (later wins) — like Postgres `include_dir`. The base
/// file may be absent (empty table); a malformed fragment is a hard error.
pub async fn load_merged(ch_config: &std::path::Path) -> Result<toml::Table, EmitterError> {
    let mut root: toml::Table = match tokio::fs::read_to_string(ch_config).await {
        Ok(s) => toml::from_str(&s).map_err(|e: toml::de::Error| {
            EmitterError::Config(format!("parse {}: {e}", ch_config.display()))
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
    if let Ok(mut rd) = tokio::fs::read_dir(&dir).await {
        let mut frags: Vec<std::path::PathBuf> = Vec::new();
        while let Ok(Some(ent)) = rd.next_entry().await {
            let p = ent.path();
            if p.extension().and_then(|e| e.to_str()) == Some("toml") {
                frags.push(p);
            }
        }
        frags.sort();
        for p in frags {
            let s = tokio::fs::read_to_string(&p)
                .await
                .map_err(|e| EmitterError::Config(format!("read {}: {e}", p.display())))?;
            let frag: toml::Table = toml::from_str(&s).map_err(|e: toml::de::Error| {
                EmitterError::Config(format!("parse {}: {e}", p.display()))
            })?;
            merge_tables(&mut root, frag);
        }
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
    use crate::decode::heap_decoder::{DecodedHeap, DecodedTuple};
    use walrus::pg::walparser::RelFileNode;

    #[test]
    fn decimal_type_error_wraps_message_in_type_variant() {
        match decimal_type_error("scale out of range") {
            EmitterError::Type(msg) => assert_eq!(msg, "scale out of range"),
            other => panic!("expected Type, got {other:?}"),
        }
    }

    #[test]
    fn is_retryable_only_for_transport_and_server_faults() {
        assert!(is_retryable(&EmitterError::Io(std::io::Error::other(
            "reset"
        ))));
        assert!(is_retryable(&EmitterError::ServerException {
            code: 241,
            message: "MEMORY_LIMIT_EXCEEDED".into(),
        }));
        // Semantic / config faults are terminal
        assert!(!is_retryable(&EmitterError::Type("bad decimal".into())));
        assert!(!is_retryable(&EmitterError::Config("missing host".into())));
        assert!(!is_retryable(&EmitterError::NoTableMapping(
            "public.t".into()
        )));
        assert!(!is_retryable(&EmitterError::CompressionUnsupported("zstd")));
    }

    #[test]
    fn emitter_error_converts_into_decoder_observer_error() {
        let d: DecoderSinkError = EmitterError::Type("nope".into()).into();
        match d {
            DecoderSinkError::Observer(msg) => assert!(msg.contains("nope"), "{msg}"),
            other => panic!("expected Observer, got {other:?}"),
        }
    }

    fn mk_mapping() -> TableMapping {
        TableMapping {
            target: TableTarget::new("default", "foo"),
            columns: vec![
                ColumnMapping {
                    src_attnum: 1,
                    target_name: "id".into(),
                    target_type: "Int32".into(),
                },
                ColumnMapping {
                    src_attnum: 2,
                    target_name: "name".into(),
                    target_type: "Nullable(String)".into(),
                },
            ],
        }
    }

    fn mk_rel() -> RelDescriptor {
        use crate::schema::{RelAttr, ReplIdent};
        use walrus::pg::walparser::RelFileNode;
        RelDescriptor {
            rfn: RelFileNode {
                spc_node: 1663,
                db_node: 5,
                rel_node: 16385,
            },
            oid: 16385,
            namespace_oid: 2200,
            rel_name: RelName::new("public", "foo"),
            kind: 'r',
            persistence: 'p',
            replident: ReplIdent::Default { pk_attnums: None },
            attributes: vec![
                RelAttr {
                    attnum: 1,
                    name: "id".into(),
                    type_oid: 23,
                    typmod: -1,
                    not_null: true,
                    dropped: false,
                    type_name: "int4".into(),
                    type_byval: true,
                    type_len: 4,
                    type_align: 'i',
                    type_storage: 'p',
                    missing_text: None,
                },
                RelAttr {
                    attnum: 2,
                    name: "name".into(),
                    type_oid: 25,
                    typmod: -1,
                    not_null: false,
                    dropped: false,
                    type_name: "text".into(),
                    type_byval: false,
                    type_len: -1,
                    type_align: 'i',
                    type_storage: 'x',
                    missing_text: None,
                },
            ],
        }
    }

    fn committed(id: i32, name: Option<&str>) -> CommittedTuple {
        let name_col = Some(
            name.map(|s| ColumnValue::Text(s.to_string()))
                .unwrap_or(ColumnValue::Null),
        );
        CommittedTuple {
            decoded: DecodedHeap {
                rfn: RelFileNode {
                    spc_node: 1663,
                    db_node: 5,
                    rel_node: 16385,
                },
                xid: 42,
                source_lsn: 0xCAFE,
                op: HeapOp::Insert,
                new: Some(DecodedTuple {
                    columns: vec![Some(ColumnValue::Int4(id)), name_col],
                    partial: false,
                }),
                old: None,
            },
            commit_ts: 1_000_000,
            commit_lsn: 0xD00D,
        }
    }

    #[test]
    fn compression_choice_parses_case_insensitively() {
        assert_eq!(
            CompressionChoice::parse("LZ4").unwrap(),
            CompressionChoice::Lz4
        );
        assert_eq!(
            CompressionChoice::parse("Zstd").unwrap(),
            CompressionChoice::Zstd
        );
        assert_eq!(
            CompressionChoice::parse("none").unwrap(),
            CompressionChoice::None
        );
        assert_eq!(
            CompressionChoice::parse("").unwrap(),
            CompressionChoice::None
        );
        CompressionChoice::parse("snappy").expect_err("unknown codec");
    }

    #[test]
    fn compression_choice_build_codec_respects_features() {
        let none = CompressionChoice::None.build_codec().unwrap();
        assert!(none.is_none());
        let lz4 = CompressionChoice::Lz4.build_codec();
        #[cfg(feature = "lz4")]
        {
            assert!(lz4.unwrap().is_some());
        }
        #[cfg(not(feature = "lz4"))]
        {
            assert!(matches!(
                lz4,
                Err(EmitterError::CompressionUnsupported("lz4"))
            ));
        }
        let zstd = CompressionChoice::Zstd.build_codec();
        #[cfg(feature = "zstd")]
        {
            assert!(zstd.unwrap().is_some());
        }
        #[cfg(not(feature = "zstd"))]
        {
            assert!(matches!(
                zstd,
                Err(EmitterError::CompressionUnsupported("zstd"))
            ));
        }
    }

    /// Confirm upstream `chc_type_elem_size` returns match what the
    /// encoder needs for fixed-shape ColumnBufs.
    #[test]
    fn elem_size_covers_tier1() {
        let alloc = Allocator::stdlib();
        let cases = [
            ("UInt8", 1usize),
            ("Int32", 4),
            ("UInt64", 8),
            ("Float64", 8),
            ("DateTime64(6, 'UTC')", 8),
            ("Decimal32(4)", 4),
            ("FixedString(16)", 16),
        ];
        for (name, expected) in cases {
            let ast = TypeAst::parse(name, alloc).expect("parses");
            assert_eq!(ast.view().elem_size(), expected, "{name}");
        }
        // Varlen + composite types report 0 (varlen on-wire shape)
        for name in ["String", "Array(UInt32)"] {
            let ast = TypeAst::parse(name, alloc).expect("parses");
            assert_eq!(ast.view().elem_size(), 0, "{name}");
        }
    }

    #[test]
    fn new_for_ast_picks_shape_from_chc_type_kind() {
        let alloc = Allocator::stdlib();
        let cases = [
            ("Int32", "Fixed"),
            ("String", "String"),
            ("Nullable(Int64)", "NullableFixed"),
            ("Nullable(String)", "NullableString"),
            ("FixedString(7)", "Fixed"),
            ("Nullable(FixedString(7))", "NullableFixed"),
        ];
        for (name, tag) in cases {
            let ast = TypeAst::parse(name, alloc).expect("parses");
            let buf = ColumnBuf::new_for_ast(&ast).expect("shape");
            let actual = match buf {
                ColumnBuf::Fixed { .. } => "Fixed",
                ColumnBuf::String { .. } => "String",
                ColumnBuf::NullableFixed { .. } => "NullableFixed",
                ColumnBuf::NullableString { .. } => "NullableString",
            };
            assert_eq!(actual, tag, "{name}");
        }
    }

    #[test]
    fn decimal_text_scales_to_integer() {
        fn le_i64(text: &str, scale: i32) -> [u8; 8] {
            let le = decimal_text_to_scaled_le(text, scale, DecimalWidth::D64).unwrap();
            le[..8].try_into().unwrap()
        }

        assert_eq!(le_i64("0", 2), 0i64.to_le_bytes());
        assert_eq!(le_i64("12", 2), 1200i64.to_le_bytes());
        assert_eq!(le_i64("1.50", 2), 150i64.to_le_bytes());
        assert_eq!(le_i64("-12.34", 2), (-1234i64).to_le_bytes());
        assert_eq!(le_i64("0.001", 3), 1i64.to_le_bytes());
        assert_eq!(le_i64("123.456", 2), 12345i64.to_le_bytes());
        assert!(decimal_text_to_scaled_le("--5", 0, DecimalWidth::D64).is_err());
        assert!(decimal_text_to_scaled_le("", 0, DecimalWidth::D64).is_err());
        assert!(decimal_text_to_scaled_le("1.2.3", 0, DecimalWidth::D64).is_err());
    }

    #[test]
    fn decimal_text_rejects_signed_width_overflow() {
        let max = i128::MAX.to_string();
        let le = decimal_text_to_scaled_le(&max, 0, DecimalWidth::D128).unwrap();
        assert_eq!(&le[..16], &i128::MAX.to_le_bytes());

        let min_mag = "170141183460469231731687303715884105728";
        assert!(decimal_text_to_scaled_le(min_mag, 0, DecimalWidth::D128).is_err());

        let le = decimal_text_to_scaled_le(&format!("-{min_mag}"), 0, DecimalWidth::D128).unwrap();
        assert_eq!(&le[..16], &i128::MIN.to_le_bytes());
    }

    #[test]
    fn decimal_text_encodes_decimal256_width() {
        let le = decimal_text_to_scaled_le("-1", 0, DecimalWidth::D256).unwrap();
        assert_eq!(&le[..32], &[0xff; 32]);

        let wide39 = "9".repeat(39);
        assert!(decimal_text_to_scaled_le(&wide39, 0, DecimalWidth::D128).is_err());
        let le = decimal_text_to_scaled_le(&wide39, 0, DecimalWidth::D256).unwrap();
        assert!(le[16..32].iter().any(|b| *b != 0));

        let wide76 = "9".repeat(76);
        assert!(decimal_text_to_scaled_le(&wide76, 0, DecimalWidth::D256).is_ok());
    }

    #[test]
    fn encode_numeric_into_decimal_and_string() {
        use crate::decode::codecs::NumericKind;
        let alloc = Allocator::stdlib();
        let ast = TypeAst::parse("Decimal(10, 2)", alloc).unwrap();
        let decimal = decimal_wire_of(&ast);
        assert_eq!(
            decimal,
            Some(DecimalWire {
                scale: 2,
                width: DecimalWidth::D64
            })
        );
        let mut buf = ColumnBuf::new_for_ast(&ast).unwrap();
        encode_value(
            &mut buf,
            &ColumnValue::Numeric(NumericKind::Finite("1.50".into())),
            decimal,
        )
        .unwrap();
        match &buf {
            ColumnBuf::Fixed { width, bytes } => {
                assert_eq!(*width, 8);
                assert_eq!(bytes.as_slice(), &150i64.to_le_bytes());
            }
            _ => panic!("expected fixed-shape buffer"),
        }
        let mut buf_nan = ColumnBuf::new_for_ast(&ast).unwrap();
        assert!(
            encode_value(
                &mut buf_nan,
                &ColumnValue::Numeric(NumericKind::NaN),
                decimal,
            )
            .is_err()
        );

        let wide_ast = TypeAst::parse("Decimal(50, 2)", alloc).unwrap();
        let wide_decimal = decimal_wire_of(&wide_ast);
        assert_eq!(
            wide_decimal,
            Some(DecimalWire {
                scale: 2,
                width: DecimalWidth::D256
            })
        );
        let mut wide_buf = ColumnBuf::new_for_ast(&wide_ast).unwrap();
        encode_value(
            &mut wide_buf,
            &ColumnValue::Numeric(NumericKind::Finite(
                "123456789012345678901234567890123456789012345678.12".into(),
            )),
            wide_decimal,
        )
        .unwrap();
        match &wide_buf {
            ColumnBuf::Fixed { width, bytes } => {
                assert_eq!(*width, 32);
                assert_eq!(bytes.len(), 32);
                assert!(bytes[16..32].iter().any(|b| *b != 0));
            }
            _ => panic!("expected fixed-shape buffer"),
        }

        let sast = TypeAst::parse("String", alloc).unwrap();
        let mut sbuf = ColumnBuf::new_for_ast(&sast).unwrap();
        encode_value(&mut sbuf, &ColumnValue::Numeric(NumericKind::NaN), None).unwrap();
        match &sbuf {
            ColumnBuf::String { data, .. } => assert_eq!(data.as_slice(), b"NaN"),
            _ => panic!("expected string-shape buffer"),
        }
    }

    #[test]
    fn encode_time_native_and_timetz_text() {
        let alloc = Allocator::stdlib();
        let micros = 45_296_000_000i64; // 12:34:56
        let ast = TypeAst::parse("Time64(6)", alloc).unwrap();
        let mut buf = ColumnBuf::new_for_ast(&ast).unwrap();
        encode_value(&mut buf, &ColumnValue::Time(micros), None).unwrap();
        match &buf {
            ColumnBuf::Fixed { width, bytes } => {
                assert_eq!(*width, 8);
                assert_eq!(bytes.as_slice(), &micros.to_le_bytes());
            }
            _ => panic!("expected fixed-shape buffer"),
        }
        let sast = TypeAst::parse("String", alloc).unwrap();
        let mut sbuf = ColumnBuf::new_for_ast(&sast).unwrap();
        encode_value(
            &mut sbuf,
            &ColumnValue::TimeTz {
                micros,
                tz_seconds: -7200,
            },
            None,
        )
        .unwrap();
        match &sbuf {
            ColumnBuf::String { data, .. } => assert_eq!(data.as_slice(), b"12:34:56+02"),
            _ => panic!("expected string-shape buffer"),
        }
    }

    #[test]
    fn quote_ident_escapes_backticks() {
        assert_eq!(quote_ident("foo"), "`foo`");
        assert_eq!(quote_ident("a`b"), "`a``b`");
    }

    #[test]
    fn table_plan_builds_insert_with_synthetic_columns() {
        let alloc = Allocator::stdlib();
        let rel = mk_rel();
        let m = mk_mapping();
        let plan = TablePlan::build(alloc, &rel, &m, None).expect("plan builds");
        assert!(plan.insert_sql.contains("INSERT INTO `default`.`foo`"));
        assert!(plan.insert_sql.contains("`id`"));
        assert!(plan.insert_sql.contains("`name`"));
        assert!(plan.insert_sql.contains("`_lsn`"));
        assert!(plan.insert_sql.contains("`_xid`"));
        assert!(plan.insert_sql.contains("`_commit_ts`"));
        assert!(plan.insert_sql.contains("`_is_deleted`"));
        assert!(plan.insert_sql.ends_with(") FORMAT Native"));
    }

    #[test]
    fn table_plan_applies_admissible_column_override() {
        let alloc = Allocator::stdlib();
        let rel = mk_rel();
        let mut m = mk_mapping();
        // numeric-shaped default: the plan drill `numeric(38,0)` → `Int128`
        m.columns[0].target_type = "Decimal(38, 0)".into();
        let overrides: HashMap<String, String> = [("id".to_string(), "Int128".to_string())].into();
        let plan = TablePlan::build(alloc, &rel, &m, Some(&overrides)).unwrap();
        assert_eq!(plan.columns[0].type_repr, "Int128");
        // scale-0 decimal wire keeps the numeric text→scaled encode path
        assert_eq!(
            plan.columns[0].decimal,
            Some(DecimalWire {
                scale: 0,
                width: DecimalWidth::D128
            })
        );
        assert_eq!(plan.columns[1].type_repr, "Nullable(String)");
    }

    #[test]
    fn table_plan_override_keys_on_source_attname_not_target_name() {
        let alloc = Allocator::stdlib();
        let rel = mk_rel();
        let mut m = mk_mapping();
        // Operator-renamed CH column: override still keys on source attname
        m.columns[1].target_name = "label".into();
        let overrides: HashMap<String, String> =
            [("name".to_string(), "String".to_string())].into();
        let plan = TablePlan::build(alloc, &rel, &m, Some(&overrides)).unwrap();
        assert_eq!(plan.columns[1].name, "label");
        assert_eq!(plan.columns[1].type_repr, "String");
    }

    #[test]
    fn table_plan_keeps_default_on_wire_incompatible_override() {
        let alloc = Allocator::stdlib();
        let rel = mk_rel();
        let m = mk_mapping();
        // encode_value writes int4 as 4 LE bytes; no textualization exists,
        // so Int32 → String must fall back rather than poison the batcher
        let overrides: HashMap<String, String> = [("id".to_string(), "String".to_string())].into();
        let plan = TablePlan::build(alloc, &rel, &m, Some(&overrides)).unwrap();
        assert_eq!(plan.columns[0].type_repr, "Int32");
    }

    #[test]
    fn override_wire_admissibility() {
        let alloc = Allocator::stdlib();
        let p = |s: &str| TypeAst::parse(s, alloc).unwrap();
        // Decimal-encoded source: Decimal / String / signed ints convert
        let dec = p("Decimal(38, 0)");
        assert!(override_wire(&dec, &p("Decimal(38, 2)")).is_some());
        assert!(override_wire(&dec, &p("String")).is_some());
        assert_eq!(
            override_wire(&dec, &p("Int128")),
            Some(Some(DecimalWire {
                scale: 0,
                width: DecimalWidth::D128
            }))
        );
        // Unsigned wraps negatives, floats reinterpret bits: rejected
        assert!(override_wire(&dec, &p("UInt128")).is_none());
        assert!(override_wire(&dec, &p("Float32")).is_none());
        // Fixed-width source: same-width reinterpretation only, no Decimal
        // (a nonzero scale would silently rescale)
        let i = p("Int32");
        assert!(override_wire(&i, &p("UInt32")).is_some());
        assert!(override_wire(&i, &p("Int64")).is_none());
        assert!(override_wire(&i, &p("Decimal32(2)")).is_none());
        // String-shaped source: string-shaped override only
        let s = p("Nullable(String)");
        assert!(override_wire(&s, &p("String")).is_some());
        assert!(override_wire(&s, &p("Int64")).is_none());
    }

    #[test]
    fn is_deleted_codes_delete_in_trailing_buffer() {
        let alloc = Allocator::stdlib();
        let rel = mk_rel();
        let m = mk_mapping();
        let plan = TablePlan::build(alloc, &rel, &m, None).expect("plan builds");
        assert!(plan.insert_sql.contains("`_is_deleted`"));
        let mut enc = TableEncoder::new(plan).unwrap();
        enc.append_row(&committed(1, Some("a")), &m, OP_INSERT)
            .unwrap();
        enc.append_row(&committed(1, Some("a")), &m, OP_DELETE)
            .unwrap();
        // _is_deleted is the trailing buffer: 0 for insert, 1 for delete
        let last = enc.buffers.len() - 1;
        match &enc.buffers[last] {
            ColumnBuf::Fixed { bytes, width } => {
                assert_eq!(*width, 1);
                assert_eq!(bytes, &vec![0u8, 1]);
            }
            other => panic!("_is_deleted expected Fixed(1), got {other:?}"),
        }
    }

    /// Key-only old image, the shape a delete logs under non-FULL
    /// replica identity
    fn committed_delete(id: i32) -> CommittedTuple {
        CommittedTuple {
            decoded: DecodedHeap {
                rfn: RelFileNode {
                    spc_node: 1663,
                    db_node: 5,
                    rel_node: 16385,
                },
                xid: 42,
                source_lsn: 0xCAFE,
                op: HeapOp::Delete,
                new: None,
                old: Some(DecodedTuple {
                    columns: vec![Some(ColumnValue::Int4(id)), None],
                    partial: false,
                }),
            },
            commit_ts: 1_000_000,
            commit_lsn: 0xD00D,
        }
    }

    #[test]
    fn absent_or_null_coerces_to_default_on_non_nullable_target() {
        let alloc = Allocator::stdlib();
        let rel = mk_rel();
        let mut m = mk_mapping();
        m.columns[1].target_type = "String".into();
        let plan = TablePlan::build(alloc, &rel, &m, None).expect("plan builds");
        let mut enc = TableEncoder::new(plan).unwrap();
        // Delete: non-key column absent from the key-only old image
        enc.append_row(&committed_delete(3), &m, OP_DELETE).unwrap();
        // Insert: genuine NULL mapped onto the non-Nullable column
        enc.append_row(&committed(4, None), &m, OP_INSERT).unwrap();
        match &enc.buffers[1] {
            ColumnBuf::String { offsets, data } => {
                assert_eq!(offsets, &vec![0u64, 0]);
                assert!(data.is_empty());
            }
            other => panic!("name expected String, got {other:?}"),
        }
    }

    #[test]
    fn absent_stays_null_on_nullable_target() {
        let alloc = Allocator::stdlib();
        let rel = mk_rel();
        let m = mk_mapping();
        let plan = TablePlan::build(alloc, &rel, &m, None).expect("plan builds");
        let mut enc = TableEncoder::new(plan).unwrap();
        enc.append_row(&committed_delete(3), &m, OP_DELETE).unwrap();
        match &enc.buffers[1] {
            ColumnBuf::NullableString { null_map, .. } => assert_eq!(null_map, &vec![1u8]),
            other => panic!("name expected NullableString, got {other:?}"),
        }
    }

    #[test]
    fn encoder_accumulates_into_typed_buffers() {
        let alloc = Allocator::stdlib();
        let rel = mk_rel();
        let m = mk_mapping();
        let plan = TablePlan::build(alloc, &rel, &m, None).unwrap();
        let mut enc = TableEncoder::new(plan).unwrap();
        enc.append_row(&committed(7, Some("seven")), &m, OP_INSERT)
            .unwrap();
        enc.append_row(&committed(8, None), &m, OP_INSERT).unwrap();
        enc.append_row(&committed(9, Some("nine")), &m, OP_INSERT)
            .unwrap();
        assert_eq!(enc.rows, 3);
        match &enc.buffers[0] {
            ColumnBuf::Fixed { bytes, width } => {
                assert_eq!(*width, 4);
                assert_eq!(bytes.len(), 12);
                assert_eq!(&bytes[0..4], &7i32.to_le_bytes());
                assert_eq!(&bytes[4..8], &8i32.to_le_bytes());
                assert_eq!(&bytes[8..12], &9i32.to_le_bytes());
            }
            other => panic!("col 0 expected Fixed, got {other:?} variant tag"),
        }
        match &enc.buffers[1] {
            ColumnBuf::NullableString {
                offsets,
                data,
                null_map,
            } => {
                assert_eq!(null_map, &vec![0u8, 1, 0]);
                assert_eq!(offsets, &vec![5u64, 5, 9]);
                assert_eq!(&data[..], b"sevennine");
            }
            other => panic!("col 1 expected NullableString, got {other:?} variant tag"),
        }
        let off = m.columns.len();
        match &enc.buffers[off] {
            ColumnBuf::Fixed { bytes, .. } => {
                assert_eq!(bytes.len(), 24);
                assert_eq!(&bytes[0..8], &0xCAFEu64.to_le_bytes());
            }
            other => panic!("_lsn expected Fixed, got {other:?} variant tag"),
        }
        match &enc.buffers[off + 3] {
            ColumnBuf::Fixed { bytes, width } => {
                assert_eq!(*width, 1);
                assert_eq!(bytes, &vec![0u8, 0, 0]);
            }
            _ => panic!("_is_deleted expected Fixed"),
        }
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
            compression = "lz4"
            row_budget = 1024
            byte_budget = 4096

            [toast]
            mode = "clickhouse"

            [table.public.foo]
            initial_load = "copy"
            columns = [
              { attnum = 1, target = "id",   type = "UInt64" },
              { attnum = 2, target = "name", type = "Nullable(String)" },
            ]
        "#;
        let c = EmitterConfig::from_toml_str(src).expect("parses");
        assert_eq!(c.host, "ch.example.com");
        assert_eq!(c.port, 9000);
        assert_eq!(c.user, "ingest");
        assert!(c.secure);
        assert_eq!(c.compression, CompressionChoice::Lz4);
        // Omitting `secure` defaults to plaintext
        assert!(
            !EmitterConfig::from_toml_str("[ch]\nhost = \"h\"\n")
                .unwrap()
                .secure
        );
        assert_eq!(c.row_budget, 1024);
        assert_eq!(c.byte_budget, 4096);
        let rel = RelName::new("public", "foo");
        let t = c.tables.get(&rel).expect("mapping present");
        // target_database/target_table omitted: [ch] database + source relname
        assert_eq!(t.target, TableTarget::new("default", "foo"));
        assert_eq!(t.columns.len(), 2);
        assert_eq!(t.columns[0].src_attnum, 1);
        assert_eq!(t.columns[1].target_type, "Nullable(String)");
        assert_eq!(
            c.table_initial_loads.get(&rel).map(String::as_str),
            Some("copy")
        );
        // soft_delete defaults off when the key is absent
        assert!(!c.soft_delete);
        assert_eq!(c.toast.mode, crate::mapping::ToastMode::ClickHouse);
        // Toast block omitted => disabled
        assert_eq!(
            EmitterConfig::from_toml_str("[ch]\n").unwrap().toast.mode,
            crate::mapping::ToastMode::Disabled
        );
        // Removed disk mode surfaces as a config error, not a silent default
        assert!(EmitterConfig::from_toml_str("[ch]\n[toast]\nmode = \"disk\"\n").is_err());
    }

    #[test]
    fn config_memory_section_round_trip() {
        let c = EmitterConfig::from_toml_str(
            "[ch]\n\
             [memory]\n\
             resident_payload_max = 1048576\n\
             inline_value_max = 65536\n",
        )
        .unwrap();
        assert_eq!(c.resident_payload_max, 1 << 20);
        assert_eq!(c.inline_value_max, 64 << 10);
        // Omitted section keeps defaults
        let d = EmitterConfig::from_toml_str("[ch]\n").unwrap();
        assert_eq!(d.resident_payload_max, DEFAULT_RESIDENT_PAYLOAD_MAX);
        assert_eq!(d.inline_value_max, DEFAULT_INLINE_VALUE_MAX);
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
        assert_eq!(dotted_ns.target, TableTarget::new("default", "c"));
        assert_eq!(dotted_rel.target, TableTarget::new("default", "b.c"));
        // Interpolation quotes the dot inside the identifier
        assert_eq!(dotted_rel.target.sql(), "`default`.`b.c`");
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
        assert_eq!(cfg.host, "fifty");
        assert_eq!(cfg.database, "demo");
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
        use walrus::config::StorageSettings;
        use walrus::storage::s3::CredentialSource;
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
        let s3 = match c.backup.expect("backup set").storage {
            StorageSettings::S3(s3) => s3,
            other => panic!("expected S3, got {other:?}"),
        };
        assert_eq!(s3.bucket, "my-bucket");
        assert_eq!(s3.prefix, "walshadow/prefix");
        assert_eq!(s3.region, "eu-west-1");
        assert_eq!(s3.endpoint.as_deref(), Some("https://minio.internal"));
        assert!(s3.force_path_style);
        match s3.creds {
            CredentialSource::Static(cr) => {
                assert_eq!(cr.access_key, "AK");
                assert_eq!(cr.secret_key, "SK");
            }
            other => panic!("expected static creds, got {other:?}"),
        }
    }

    #[test]
    fn config_backup_s3_defaults_region_and_imds() {
        use walrus::config::StorageSettings;
        use walrus::storage::s3::CredentialSource;
        let c = EmitterConfig::from_toml_str("[backup]\narchive = \"s3://b\"\n").unwrap();
        let s3 = match c.backup.unwrap().storage {
            StorageSettings::S3(s3) => s3,
            other => panic!("expected S3, got {other:?}"),
        };
        assert_eq!(s3.bucket, "b");
        assert_eq!(s3.prefix, "");
        assert_eq!(s3.region, "us-east-1");
        assert!(matches!(s3.creds, CredentialSource::Imds(_)));
    }

    #[test]
    fn config_backup_gcs_and_file() {
        use walrus::config::StorageSettings;
        let gcs = EmitterConfig::from_toml_str(
            "[backup]\narchive = \"gs://gb/pre\"\ncredentials_path = \"/sa.json\"\n",
        )
        .unwrap()
        .backup
        .unwrap();
        match gcs.storage {
            StorageSettings::Gcs(g) => {
                assert_eq!(g.bucket, "gb");
                assert_eq!(g.prefix, "pre");
                assert_eq!(g.credentials_path.as_deref(), Some("/sa.json"));
            }
            other => panic!("expected GCS, got {other:?}"),
        }
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
}
