//! CH-native emitter primitives via `clickhouse-c-rs`.
//!
//! Defines building blocks shared by bootstrap backfill and live
//! streaming, with no emitter object or xact lifecycle of its own:
//! [`EmitterConfig`] (TOML parse + retry classification via
//! [`RetryConfig`]), [`CompressionChoice`], the config-side
//! [`TableMapping`]/[`NamespaceMapping`]/[`ColumnMapping`],
//! [`TablePlan`]/[`ColumnPlan`] (relation schema -> native wire plan),
//! [`TableEncoder`]/[`ColumnBuf`] (row -> native block + per-value
//! encoding), and [`EmitterStats`]. Consumed by
//! [`crate::pipeline`]'s `batcher` and `inserter`, and by
//! [`crate::ch_ddl`]; batching, seal triggers, and xact close all live
//! in the pipeline, not here.
//!
//! Synthetic columns `_lsn UInt64`, `_xid UInt32`, `_op Enum8(...)`,
//! `_commit_ts DateTime64(6, 'UTC')` are appended by [`TablePlan`] after
//! every mapped column and filled by [`TableEncoder`]. PG's
//! `TimestampTz` epoch is 2000-01-01; we shift to the Unix epoch
//! (`DATETIME64_PG_EPOCH_US`) so `DateTime64(6)` semantics line up with
//! ClickHouse.
//!
//! ## Compression
//!
//! Codec choice is feature-gated via walshadow's own `lz4` / `zstd`
//! Cargo features, which forward to `clickhouse-c-rs`'s matching
//! features (see top-level `Cargo.toml`). When a feature is off, the
//! corresponding [`CompressionChoice`] variant fails to construct at
//! [`CompressionChoice::build_codec`] with
//! [`EmitterError::CompressionUnsupported`]. Default builds advertise
//! LZ4 to match the CH server default.
//!
//! ## Cross-table ordering inside an xact
//!
//! `AsyncClient` is single-query-at-a-time, so an xact touching tables
//! T1 and T2 lands as every T1 row first (one INSERT), then every T2
//! (next INSERT); original WAL interleaving across tables is not
//! preserved. `_lsn` carries the source LSN so `ReplacingMergeTree`-style
//! dedup still keys on the right value, and WAL ordering within a single
//! destination table is preserved

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use clickhouse_c::{
    Allocator, AsyncClient, BlockBuilder, ClientOpts, Codec, Compression, Event, Kind, TypeAst,
};
use thiserror::Error;

use crate::decoder_sink::DecoderSinkError;
use crate::heap_decoder::{ColumnValue, CommittedTuple, HeapOp};
use crate::shadow_catalog::{CatalogError, RelDescriptor};

/// Microsecond offset between PG `TimestampTz` epoch (2000-01-01 UTC)
/// and the Unix epoch. `DateTime64(6)` in ClickHouse is Unix
/// microseconds; PG's commit-record `xact_time` and tuple
/// `TimestampTz` columns are PG-epoch microseconds.
pub const DATETIME64_PG_EPOCH_US: i64 = 946_684_800_000_000;

/// `_op` Enum8 codes — keep in sync with the `Enum8('insert'=1, ...)`
/// type advertised by [`TablePlan::synth_op`].
pub const OP_INSERT: i8 = 1;
pub const OP_UPDATE: i8 = 2;
pub const OP_DELETE: i8 = 3;

/// Default block accumulator budgets. Mirror common ClickHouse server
/// defaults; tunable via [`EmitterConfig`].
pub const DEFAULT_ROW_BUDGET: usize = 65_536;
pub const DEFAULT_BYTE_BUDGET: usize = 1 << 20; // 1 MiB

/// Default flush timeout (ms). `0` keeps this serial emitter's
/// close-INSERT-on-every-xact-end behaviour (bootstrap backfill only);
/// the live pipeline substitutes a 100ms partial-batch deadline for
/// `0` so cold tables can't pin the watermark. A positive value, via
/// TOML (`flush_timeout_ms`) or `--ch-flush-timeout-ms`, holds INSERTs
/// open across xacts and seals on a deadline armed at the first row of
/// a fresh INSERT.
pub const DEFAULT_FLUSH_TIMEOUT_MS: u64 = 0;

#[derive(Debug, Error)]
pub enum EmitterError {
    #[error("clickhouse-c: {0}")]
    Client(#[from] clickhouse_c::Error),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("config: {0}")]
    Config(String),
    #[error("type: {0}")]
    Type(String),
    #[error("catalog: {0}")]
    Catalog(#[from] CatalogError),
    #[error("compression `{0}` requested but feature disabled at compile time")]
    CompressionUnsupported(&'static str),
    #[error("no table mapping for source relation `{0}`")]
    NoTableMapping(String),
    #[error("unsupported column value for {target_column}: {kind}")]
    UnsupportedValue {
        target_column: String,
        kind: &'static str,
    },
    #[error("CH server exception {code}: {message}")]
    ServerException { code: i32, message: String },
    #[error("CH insert timed out after {secs}s")]
    Timeout { secs: u64 },
}

impl From<EmitterError> for DecoderSinkError {
    fn from(e: EmitterError) -> Self {
        DecoderSinkError::Observer(e.to_string())
    }
}

/// Per-replica connection + mapping config. Parse from TOML via
/// [`EmitterConfig::from_toml_str`]; the `[ch]` table holds connection
/// params, `[table."<src>"]` blocks declare per-relation mapping.
#[derive(Debug, Clone)]
pub struct EmitterConfig {
    pub host: String,
    pub port: u16,
    pub database: String,
    pub user: String,
    pub password: String,
    /// Wrap the native protocol in TLS (rustls, public webpki roots).
    /// Set for ClickHouse Cloud, whose secure native port (9440) speaks
    /// native-over-TLS. SNI + cert verification key off `host`.
    pub secure: bool,
    /// Custom rustls roots/config for the `secure` path: private CA,
    /// pinned self-signed cert, or mTLS. `None` (default) uses public
    /// webpki roots via [`clickhouse_c::tls::default_config`]. Build from
    /// a `RootCertStore` via [`clickhouse_c::tls::config_with_roots`].
    /// Not parsed from TOML; carried through reconnect + the DDL
    /// applicator so every CH socket pins the same roots.
    pub tls_config: Option<Arc<clickhouse_c::tls::rustls::ClientConfig>>,
    pub compression: CompressionChoice,
    pub row_budget: usize,
    pub byte_budget: usize,
    /// Hold INSERTs open across xacts. Timer starts when the first row
    /// of a fresh INSERT lands and trips at `now + flush_timeout`; on
    /// trip the emitter closes every still-open INSERT (one
    /// `send_data_end()` + drain to `EndOfStream` per table) and
    /// advances its durable-LSN horizon. `Duration::ZERO` (default)
    /// keeps the pre-fix behaviour: every xact closes its own
    /// INSERTs, ack tracks `drain_lsn` exactly.
    ///
    /// Latency cap: a row buffered the moment the deadline starts is
    /// at most `flush_timeout` away from durable on CH. Throughput
    /// win: small commits (the pgbench TPC-B shape) coalesce into one
    /// MergeTree part per flush window instead of one per xact.
    pub flush_timeout: Duration,
    /// Keyed on `"<namespace>.<relname>"` source identifier.
    pub tables: HashMap<String, TableMapping>,
    /// Per-source-namespace defaults. Auto-DDL flow.
    /// Keyed on PG schema name (`"public"`, etc.); per-table entries
    /// in `tables` still win for the relation they name.
    pub namespaces: HashMap<String, NamespaceMapping>,
    /// Global `--drop-table-strategy` default. Per-namespace
    /// override via `[namespace.<ns>] drop_table_strategy = ...`.
    pub drop_table_strategy: String,
    /// Bounded retry against a single CH replica. See
    /// [`RetryConfig`] for semantics.
    pub retry: RetryConfig,
    /// Wall-clock cap on a single INSERT attempt (send + drain to
    /// `EndOfStream`). A connection that wedges mid-INSERT surfaces as a
    /// retryable [`EmitterError::Timeout`] so the inserter reconnects and
    /// resends rather than pinning the durable watermark forever. Sized far
    /// above a healthy round-trip (single-digit ms local, RTT-bound cloud).
    pub insert_timeout: Duration,
}

/// Default per-INSERT wall-clock cap; see [`EmitterConfig::insert_timeout`].
pub const DEFAULT_INSERT_TIMEOUT_SECS: u64 = 30;

/// Bounded-retry knobs for the CH emitter. A retryable error (IO,
/// clickhouse-c protocol, ServerException) triggers reconnect + retry
/// of the failing operation up to `max_attempts` times with
/// exponential backoff capped at `max_backoff`.
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
            namespaces: HashMap::new(),
            drop_table_strategy: "retain".into(),
            retry: RetryConfig::default(),
            insert_timeout: Duration::from_secs(DEFAULT_INSERT_TIMEOUT_SECS),
        }
    }
}

/// Wire-protocol compression choice. Variants are gated on Cargo
/// features in the top crate; the `resolve` step refuses unsupported
/// variants with [`EmitterError::CompressionUnsupported`] before the
/// codec object is built.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CompressionChoice {
    None,
    #[default]
    Lz4,
    Zstd,
}

impl CompressionChoice {
    pub fn parse(s: &str) -> Result<Self, EmitterError> {
        Ok(match s.to_ascii_lowercase().as_str() {
            "none" | "off" | "" => Self::None,
            "lz4" => Self::Lz4,
            "zstd" => Self::Zstd,
            other => {
                return Err(EmitterError::Config(format!(
                    "unknown compression `{other}` (expected none / lz4 / zstd)"
                )));
            }
        })
    }

    pub(crate) fn to_wire(self) -> Compression {
        match self {
            Self::None => Compression::None,
            Self::Lz4 => Compression::Lz4,
            Self::Zstd => Compression::Zstd,
        }
    }

    /// Build the [`Codec`] handle. Feature-gates flow up from
    /// clickhouse-c-rs so the C TU only links a codec lib when its
    /// matching feature is on in the top crate.
    pub fn build_codec(self) -> Result<Option<Pin<Box<Codec>>>, EmitterError> {
        match self {
            Self::None => Ok(None),
            Self::Lz4 => {
                #[cfg(feature = "lz4")]
                {
                    Ok(Some(Codec::lz4()))
                }
                #[cfg(not(feature = "lz4"))]
                {
                    Err(EmitterError::CompressionUnsupported("lz4"))
                }
            }
            Self::Zstd => {
                #[cfg(feature = "zstd")]
                {
                    Ok(Some(Codec::zstd()))
                }
                #[cfg(not(feature = "zstd"))]
                {
                    Err(EmitterError::CompressionUnsupported("zstd"))
                }
            }
        }
    }
}

/// Per-source-relation destination metadata. Carries the destination
/// table name & one entry per non-synthetic column. The mapping
/// declares which source attnums to ship and what CH type to advertise.
#[derive(Debug, Clone)]
pub struct TableMapping {
    pub target: String,
    pub columns: Vec<ColumnMapping>,
}

/// Per-namespace defaults. Operator-pinned blocks shaped
/// like:
///
/// ```toml
/// [namespace."public"]
/// target_database = "default"
/// auto_create = true
/// drop_table_strategy = "retain"   # one of retain / drop / warn
/// ```
///
/// `auto_create = true` lets [`crate::ch_ddl::DdlApplicator`] run
/// `CREATE TABLE IF NOT EXISTS` for relations in this namespace the
/// first time their descriptor is observed. `target_database`
/// overrides the global `[ch] database` for tables in this namespace.
#[derive(Debug, Clone, Default)]
pub struct NamespaceMapping {
    pub target_database: Option<String>,
    pub auto_create: bool,
    pub drop_table_strategy: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ColumnMapping {
    /// Source `pg_attribute.attnum` (1-based, matches PG convention).
    pub src_attnum: i16,
    pub target_name: String,
    /// ClickHouse type expression — parsed via [`TypeAst::parse`]. The
    /// emitter does not validate that the type matches the source
    /// column's PG type; CH will reject on `INSERT` if they mismatch.
    pub target_type: String,
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
    /// [table."public.foo"]
    /// target = "default.foo"
    /// columns = [
    ///   { attnum = 1, target = "id",   type = "UInt64" },
    ///   { attnum = 2, target = "name", type = "Nullable(String)" },
    /// ]
    /// ```
    pub fn from_toml_str(s: &str) -> Result<Self, EmitterError> {
        use toml::Value;
        let root: Value = toml::de::from_str(s)
            .map_err(|e: toml::de::Error| EmitterError::Config(format!("toml: {e}")))?;
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
            for (k, v) in tbls {
                let t = v
                    .as_table()
                    .ok_or_else(|| EmitterError::Config(format!("table.{k}: expected a table")))?;
                let target = t
                    .get("target")
                    .and_then(Value::as_str)
                    .ok_or_else(|| EmitterError::Config(format!("table.{k}: missing target")))?
                    .to_string();
                let cols_v = t.get("columns").and_then(Value::as_array).ok_or_else(|| {
                    EmitterError::Config(format!("table.{k}: missing columns array"))
                })?;
                let mut columns = Vec::with_capacity(cols_v.len());
                for (i, c) in cols_v.iter().enumerate() {
                    let ct = c.as_table().ok_or_else(|| {
                        EmitterError::Config(format!("table.{k}.columns[{i}]: expected a table"))
                    })?;
                    let src_attnum =
                        ct.get("attnum")
                            .and_then(Value::as_integer)
                            .ok_or_else(|| {
                                EmitterError::Config(format!(
                                    "table.{k}.columns[{i}]: missing attnum"
                                ))
                            })?;
                    let target_name = ct
                        .get("target")
                        .and_then(Value::as_str)
                        .ok_or_else(|| {
                            EmitterError::Config(format!("table.{k}.columns[{i}]: missing target"))
                        })?
                        .to_string();
                    let target_type = ct
                        .get("type")
                        .and_then(Value::as_str)
                        .ok_or_else(|| {
                            EmitterError::Config(format!("table.{k}.columns[{i}]: missing type"))
                        })?
                        .to_string();
                    columns.push(ColumnMapping {
                        src_attnum: i16::try_from(src_attnum).map_err(|_| {
                            EmitterError::Config(format!(
                                "table.{k}.columns[{i}].attnum {src_attnum} out of i16 range"
                            ))
                        })?,
                        target_name,
                        target_type,
                    });
                }
                out.tables
                    .insert(k.clone(), TableMapping { target, columns });
            }
        }
        Ok(out)
    }
}

/// Cached plan for one destination table. Built lazily the first time a
/// row lands for the relation; held by the batcher's [`TableEncoder`]
/// keyed by source `(namespace.relname)`.
pub struct TablePlan {
    pub target: String,
    pub columns: Vec<ColumnPlan>,
    pub synth_lsn: ColumnPlan,
    pub synth_xid: ColumnPlan,
    pub synth_op: ColumnPlan,
    pub synth_commit_ts: ColumnPlan,
    /// `INSERT INTO ... (...) VALUES`. Pre-formatted so on-tuple paths
    /// don't reassemble the string per row.
    pub insert_sql: String,
}

pub struct ColumnPlan {
    pub name: String,
    /// CH type expression, eg. "Nullable(String)" / "UInt64".
    pub type_repr: String,
    pub ast: TypeAst,
    /// Wire metadata for a (possibly Nullable) `Decimal(p,s)` column.
    pub decimal: Option<DecimalWire>,
}

/// Physical wire width of a CH `Decimal`: exactly one of four
/// signed-integer backings. Discriminants are the byte widths, so
/// `as usize` recovers the size.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DecimalWidth {
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
pub struct DecimalWire {
    pub scale: u8,
    pub width: DecimalWidth,
}

impl TablePlan {
    /// Build from a relation descriptor + mapping. Synthetic columns
    /// are always non-nullable (the emitter always populates them).
    pub(crate) fn build(
        alloc: Allocator,
        rel: &RelDescriptor,
        mapping: &TableMapping,
    ) -> Result<Self, EmitterError> {
        let mut columns = Vec::with_capacity(mapping.columns.len());
        let mut col_sql = Vec::with_capacity(mapping.columns.len() + 4);
        // Mapping columns whose attnum isn't in the catalog descriptor
        // are not a hard error: schema-evolution workloads pre-declare
        // post-ALTER columns in the mapping, and pre-ALTER xacts will
        // legitimately see fewer attnums than the mapping does.
        // `TableEncoder::append_row` already emits NULL for any source
        // attnum that `decoded.{new,old}.columns.get` returns None for,
        // so the missing-column case lands as NULL on every row of the
        // affected mapping column. Operators chasing a static-config
        // typo see it as "this column is always NULL" — the CH dest
        // table catches it if the column is non-nullable; otherwise
        // surfaces in row-count / aggregate mismatches.
        let _ = rel;
        for c in &mapping.columns {
            let ast = TypeAst::parse(&c.target_type, alloc)
                .map_err(|e| EmitterError::Type(format!("{}: {e}", c.target_type)))?;
            let decimal = decimal_wire_of(&ast);
            columns.push(ColumnPlan {
                name: c.target_name.clone(),
                type_repr: c.target_type.clone(),
                ast,
                decimal,
            });
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
        let synth_op = mk("_op", "Enum8('insert' = 1, 'update' = 2, 'delete' = 3)")?;
        let synth_commit_ts = mk("_commit_ts", "DateTime64(6, 'UTC')")?;
        col_sql.push(quote_ident(&synth_lsn.name));
        col_sql.push(quote_ident(&synth_xid.name));
        col_sql.push(quote_ident(&synth_op.name));
        col_sql.push(quote_ident(&synth_commit_ts.name));
        let insert_sql = format!(
            "INSERT INTO {} ({}) FORMAT Native",
            mapping.target,
            col_sql.join(", "),
        );
        Ok(Self {
            target: mapping.target.clone(),
            columns,
            synth_lsn,
            synth_xid,
            synth_op,
            synth_commit_ts,
            insert_sql,
        })
    }
}

pub(crate) fn quote_ident(name: &str) -> String {
    // CH backtick quoting (mirrors `quoteIdentIfNeed` in the upstream
    // client). Backticks inside identifiers escape via doubling.
    let mut s = String::with_capacity(name.len() + 2);
    s.push('`');
    for c in name.chars() {
        if c == '`' {
            s.push('`');
        }
        s.push(c);
    }
    s.push('`');
    s
}

/// Open a CH [`AsyncClient`] from `config`, connecting a fresh `tokio`
/// TCP socket to `(host, port)` and running the Hello handshake. Shared
/// by the inserter pool ([`crate::pipeline::inserter`]) and
/// [`crate::ch_ddl::DdlApplicator::new`] so connection options stay in one
/// place.
pub(crate) async fn connect_client(config: &EmitterConfig) -> Result<AsyncClient, EmitterError> {
    let codec = config.compression.build_codec()?;
    let mut opts = ClientOpts::new()
        .database(&config.database)
        .user(&config.user)
        .password(&config.password);
    opts.compression = config.compression.to_wire();
    let addr = (config.host.as_str(), config.port);
    let client = if config.secure {
        // SNI + cert verification key off the configured host. Caller's
        // pinned config wins (private CA / self-signed / mTLS); else
        // public webpki roots cover ClickHouse Cloud's CA.
        let tls = config
            .tls_config
            .clone()
            .unwrap_or_else(clickhouse_c::tls::default_config);
        AsyncClient::connect_tls(addr, &config.host, opts, codec, tls).await?
    } else {
        AsyncClient::connect(addr, opts, codec).await?
    };
    Ok(client)
}

/// Drain a CH response stream to `EndOfStream`, surfacing any
/// `Exception` packet as [`EmitterError::ServerException`]. Used after
/// every `send_query`/`send_data_end()` that expects no result rows
/// (INSERT seal, TRUNCATE, DDL).
pub(crate) async fn drain_to_end_of_stream(client: &mut AsyncClient) -> Result<(), EmitterError> {
    loop {
        match client.recv_event().await? {
            Event::EndOfStream => break,
            Event::Exception(exc) => {
                return Err(EmitterError::ServerException {
                    code: exc.code(),
                    message: String::from_utf8_lossy(exc.display_text()).into_owned(),
                });
            }
            _ => {}
        }
    }
    Ok(())
}

/// Per-table per-xact accumulator. One block buffer per CH column;
/// flushed at xact end (or budget trip in a future pass). Buffers reset
/// after `flush` so the encoder reuses allocations.
pub struct TableEncoder {
    pub plan: TablePlan,
    pub rows: usize,
    pub approx_bytes: usize,
    /// Mirrors `plan.columns + 4 synth`.
    pub buffers: Vec<ColumnBuf>,
}

/// On-the-wire-shape column buffer. Owned data lives here; the
/// [`BlockBuilder`] borrows slices at flush time and the buffers are
/// cleared after `send_data`.
pub enum ColumnBuf {
    /// `Type` like UInt32 — `width` bytes per row, packed little-endian.
    Fixed { width: usize, bytes: Vec<u8> },
    /// `String` / `Bytea`. `offsets[i]` is the cumulative exclusive end
    /// of row `i` in `data`.
    String { offsets: Vec<u64>, data: Vec<u8> },
    /// `Nullable(Fixed)`. `null_map[i] = 1` means NULL; default value
    /// (zero bytes) goes into `inner` for null rows so the slab stays
    /// dense.
    NullableFixed {
        width: usize,
        null_map: Vec<u8>,
        inner: Vec<u8>,
    },
    /// `Nullable(String)`.
    NullableString {
        offsets: Vec<u64>,
        data: Vec<u8>,
        null_map: Vec<u8>,
    },
}

impl ColumnBuf {
    /// Build the right shape from a parsed [`TypeAst`]. Width comes
    /// from clickhouse-c's `chc_type_elem_size` so FixedString(N),
    /// DateTime64, Decimal*, Enum, etc. resolve correctly without
    /// walshadow having to mirror the type-string surface. `elem_size
    /// == 0` means a varlen on-wire shape (String / Bytea / Array / …);
    /// the only varlen the emitter handles today is `String`, anything
    /// else falls through to the [`String`] / [`NullableString`] arm
    /// and dies cleanly on the first `append` mismatch.
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

    /// Append one row. Caller decides whether the value is NULL.
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

/// Fresh per-column buffers matching `plan` (mapped columns + the four
/// synthetic ones). Shared by [`TableEncoder::new`] and
/// [`TableEncoder::take_block`] so the synthetic-column widths live in one
/// place.
pub(crate) fn fresh_buffers(plan: &TablePlan) -> Result<Vec<ColumnBuf>, EmitterError> {
    let mut buffers = Vec::with_capacity(plan.columns.len() + 4);
    for c in &plan.columns {
        buffers.push(ColumnBuf::new_for_ast(&c.ast)?);
    }
    // Synthetic columns are non-nullable by construction.
    buffers.push(ColumnBuf::Fixed {
        width: 8,
        bytes: Vec::new(),
    }); // _lsn UInt64
    buffers.push(ColumnBuf::Fixed {
        width: 4,
        bytes: Vec::new(),
    }); // _xid UInt32
    buffers.push(ColumnBuf::Fixed {
        width: 1,
        bytes: Vec::new(),
    }); // _op Enum8
    buffers.push(ColumnBuf::Fixed {
        width: 8,
        bytes: Vec::new(),
    }); // _commit_ts DateTime64(6)
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

    /// Swap the accumulated column slabs out for fresh empties, returning
    /// the old slabs + their row count. The pipeline's batcher hands the
    /// returned slabs to an inserter task (which rebuilds the
    /// [`BlockBuilder`] over them with its own parsed types) and keeps
    /// appending into the fresh ones. Unlike [`Self::clear`] this transfers
    /// ownership rather than reusing the allocations.
    pub(crate) fn take_block(&mut self) -> Result<(Vec<ColumnBuf>, usize), EmitterError> {
        let fresh = fresh_buffers(&self.plan)?;
        let old = std::mem::replace(&mut self.buffers, fresh);
        let rows = self.rows;
        self.rows = 0;
        self.approx_bytes = 0;
        Ok((old, rows))
    }

    /// Append one committed tuple's `new` image (or `old` image for
    /// DELETE / UPDATE old-key). Caller picks the source side and the
    /// `_op` code.
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
                None | Some(ColumnValue::Null) => buf.append_null().map_err(|mut e| {
                    if let EmitterError::UnsupportedValue {
                        ref mut target_column,
                        ..
                    } = e
                    {
                        *target_column = col.target_name.clone();
                    }
                    e
                })?,
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
        // Synthetic columns: lsn (UInt64), xid (UInt32), op (Enum8),
        // commit_ts (DateTime64(6, UTC) = unix microseconds).
        let off = mapping.columns.len();
        push_fixed(&mut self.buffers[off], &decoded.source_lsn.to_le_bytes())?;
        push_fixed(&mut self.buffers[off + 1], &decoded.xid.to_le_bytes())?;
        push_fixed(&mut self.buffers[off + 2], &op_code.to_le_bytes())?;
        let unix_us = committed.commit_ts.saturating_add(DATETIME64_PG_EPOCH_US);
        push_fixed(&mut self.buffers[off + 3], &unix_us.to_le_bytes())?;
        self.rows += 1;
        self.approx_bytes = self.buffers.iter().map(ColumnBuf::approx_size).sum();
        Ok(())
    }
}

pub(crate) fn append_buf<'a>(
    bb: &mut BlockBuilder<'a>,
    name: &str,
    ast: &'a TypeAst,
    buf: &'a ColumnBuf,
    n_rows: usize,
) -> Result<(), EmitterError> {
    // SAFETY: the BlockBuilder retains pointers into the slabs we pass
    // here until `send_data` returns. The buffers live in
    // `TableEncoder.buffers` and are not mutated until `clear()` runs
    // after this function returns.
    match buf {
        ColumnBuf::Fixed { bytes, .. } => {
            bb.append_fixed(name, ast.view(), bytes, n_rows)?;
        }
        ColumnBuf::String { offsets, data } => {
            bb.append_string(name, offsets, data, n_rows)?;
        }
        ColumnBuf::NullableFixed {
            null_map, inner, ..
        } => {
            bb.append_nullable_fixed(name, ast.view(), null_map, inner, n_rows)?;
        }
        ColumnBuf::NullableString {
            offsets,
            data,
            null_map,
        } => {
            bb.append_nullable_string(name, ast.view(), null_map, offsets, data, n_rows)?;
        }
    }
    Ok(())
}

fn push_fixed(buf: &mut ColumnBuf, le: &[u8]) -> Result<(), EmitterError> {
    buf.append_fixed_bytes(le)
}

/// Wire metadata for a (possibly `Nullable`) CH `Decimal` type, or
/// `None` when the type isn't Decimal. Peels one `Nullable` layer the
/// same way [`ColumnBuf::new_for_ast`] does.
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

/// Convert a PG `numeric` text rendering (`numeric_out` form, eg
/// `-12.340`) to the scaled integer a CH `Decimal(_, scale)` stores:
/// `value * 10^scale`, encoded as two's-complement little-endian bytes
/// at the Decimal wire width. PG stores values conforming to column
/// typmod, so text dscale normally equals `scale`; rescale handles
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
    // Strip at most one leading sign; numeric_out emits a single optional
    // '-', so a residual sign char in `body` is malformed and the parse
    // loop below rejects it as a non-digit.
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
        // Text carries more fractional digits than the column scale.
        // PG would have rounded on store, so this is a defensive
        // truncation that should not occur for conforming values.
        for _ in 0..-diff {
            mag.div_small(10);
        }
    }

    // Bound the magnitude by the physical wire width (signed Int{32,64,
    // 128,256} range), not the logical Decimal(p,s) precision of 10^p.
    // The bridge maps numeric(p,s) → Decimal(p,s) with matching p, so
    // conforming values satisfy both; this check is the backstop that
    // turns a too-wide value (eg an operator override onto a narrower
    // Decimal) into a clean error instead of a silently truncated store.
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
        ColumnValue::Date(n) => buf.append_fixed_bytes(&n.to_le_bytes()),
        // `time` → `Time64(6)`: microseconds since midnight, no epoch
        // offset (matches the bridge's `Time64(6)` mapping).
        ColumnValue::Time(n) => buf.append_fixed_bytes(&n.to_le_bytes()),
        ColumnValue::Timestamp(n) | ColumnValue::TimestampTz(n) => {
            // PG epoch → Unix epoch, microseconds. CH `DateTime64(6)` is
            // unix-epoch microseconds.
            let unix_us = n.saturating_add(DATETIME64_PG_EPOCH_US);
            buf.append_fixed_bytes(&unix_us.to_le_bytes())
        }
        // `timetz` → text (CH has no zone-aware time type); preserves
        // the offset the fixed encoding used to drop.
        ColumnValue::TimeTz { micros, tz_seconds } => {
            buf.append_string_bytes(crate::codecs::timetz_to_text(*micros, *tz_seconds).as_bytes())
        }
        ColumnValue::Uuid(b) => buf.append_fixed_bytes(b),
        ColumnValue::Name(s) | ColumnValue::Text(s) | ColumnValue::Json(s) => {
            buf.append_string_bytes(s.as_bytes())
        }
        ColumnValue::Numeric(n) => {
            use crate::codecs::NumericKind;
            match decimal {
                // Decimal column: encode the finite value as a scaled
                // little-endian integer. Non-finite (NaN/±Inf) can't be
                // represented — surface as an error so nothing is
                // silently corrupted (operator maps that column to
                // String to recover).
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
                // String column (unconstrained numeric or operator
                // mapping): lossless text, including NaN/±Inf.
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
        // PgPending: resolution to text happens earlier in the pipeline
        // (BufferingDecoderSink drain via the oracle extension). Reaching
        // the emitter with PgPending still set means the extension is
        // absent — fall back to the raw on-disk bytes so CH still
        // receives the value (operators can post-process via PG-side
        // tooling). No error; no stat bump.
        ColumnValue::PgPending { raw, .. } => buf.append_string_bytes(raw),
        ColumnValue::Unsupported { .. } => Err(EmitterError::UnsupportedValue {
            target_column: String::new(),
            kind: "unsupported PG type oid",
        }),
    }
}

/// Shared per-relation mapping handle. Cloneable via the inner `Arc`; the
/// decode pool and the DDL applicator read it, and the daemon swaps the
/// whole `HashMap` atomically on SIGHUP reload.
///
/// Readers see the swap *between* rows; a table's cached [`TablePlan`] in
/// the batcher rebuilds on the next epoch (after a barrier), so subsequent
/// batches encode against the new map.
pub type MappingHandle = Arc<tokio::sync::RwLock<HashMap<String, TableMapping>>>;

crate::atomic_stats! {
    /// CH emitter counters. Mutations via `fetch_add(_, Relaxed)`; the
    /// daemon's status loop reads via `.load(Relaxed)` at the use site.
    pub struct EmitterStats {
        pub rows_emitted,
        pub blocks_sent,
        pub xacts_committed,
        pub unsupported_relations,
        /// Rows whose filenode resolved to a foreign database (physical WAL
        /// carries the whole cluster). Skipped, not an error.
        pub foreign_db_rows_skipped,
        pub unsupported_values,
        /// Retry bookkeeping. `reconnects` ticks on every fresh
        /// CH socket the daemon hot-replaces; `retries_attempted` ticks on
        /// every reconnect-triggered retry (one per failing operation,
        /// not per attempt — so a single op that needed 3 retries adds 3).
        pub reconnects,
        pub retries_attempted,
        /// `HeapOp::Truncate` events shipped to CH as `TRUNCATE TABLE`.
        pub truncates_emitted,
        /// Legacy serial-emitter counter for hold-INSERT-open deadline
        /// trips. The pooled pipeline seals partial batches via the
        /// batcher's own `flush_timeout` deadline and does not bump this,
        /// so it now stays 0.
        pub flush_deadline_trips,
    }
}

pub(crate) fn is_retryable(e: &EmitterError) -> bool {
    matches!(
        e,
        EmitterError::Io(_)
            | EmitterError::Client(_)
            | EmitterError::ServerException { .. }
            | EmitterError::Timeout { .. }
    )
}

// `ColumnBuf` Debug — used by error paths that want to surface what
// shape the buffer is. Manual impl because deriving would render the
// whole slab through `Vec<u8>` rather than reporting just lengths.
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::heap_decoder::{DecodedHeap, DecodedTuple};
    use wal_rs::pg::walparser::RelFileNode;

    #[test]
    fn decimal_type_error_wraps_message_in_type_variant() {
        match decimal_type_error("scale out of range") {
            EmitterError::Type(msg) => assert_eq!(msg, "scale out of range"),
            other => panic!("expected Type, got {other:?}"),
        }
    }

    #[test]
    fn is_retryable_only_for_transport_and_server_faults() {
        // Transport / server faults: the stream loop should reconnect+retry.
        assert!(is_retryable(&EmitterError::Io(std::io::Error::other(
            "reset"
        ))));
        assert!(is_retryable(&EmitterError::ServerException {
            code: 241,
            message: "MEMORY_LIMIT_EXCEEDED".into(),
        }));
        // Semantic / config faults are terminal — retrying repeats them.
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
            target: "default.foo".into(),
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
        use crate::shadow_catalog::{RelAttr, ReplIdent};
        use wal_rs::pg::walparser::RelFileNode;
        RelDescriptor {
            rfn: RelFileNode {
                spc_node: 1663,
                db_node: 5,
                rel_node: 16385,
            },
            oid: 16385,
            namespace_oid: 2200,
            namespace_name: "public".into(),
            name: "foo".into(),
            qualified_name: RelDescriptor::build_qualified_name("public", "foo"),
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
        let name_col = match name {
            None => Some(ColumnValue::Null),
            Some(s) => Some(ColumnValue::Text(s.to_string())),
        };
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

    /// Each `CompressionChoice::build_codec` arm goes through the
    /// matching feature gate. Build the variants on the current feature
    /// matrix & confirm a successful codec emerges (or a clean error).
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

    /// Reach into chc_type_elem_size for the emitter width matrix
    /// instead of mirroring it in walshadow. Confirms the upstream
    /// elem_size return values match what the encoder needs for
    /// fixed-shape ColumnBufs.
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
        // Varlen + composite types report 0; the encoder reads that
        // as "varlen on-wire shape".
        for name in ["String", "Array(UInt32)"] {
            let ast = TypeAst::parse(name, alloc).expect("parses");
            assert_eq!(ast.view().elem_size(), 0, "{name}");
        }
    }

    /// Nullable wraps in CH wire types live at the type AST layer;
    /// `new_for_ast` peels them via `Kind::Nullable` + `child(0)` and
    /// picks the right buffer shape from the inner's `elem_size`.
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
        // More fractional digits than the column scale: defensive trunc.
        assert_eq!(le_i64("123.456", 2), 12345i64.to_le_bytes());
        // Malformed input rejected: doubled sign, empty, stray chars.
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
        use crate::codecs::NumericKind;
        let alloc = Allocator::stdlib();
        // Decimal(10,2) → Decimal64 (8 bytes); "1.50" scaled by 100 = 150.
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
        // NaN into a Decimal column is unrepresentable → error.
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

        // String-mapped numeric (unconstrained) renders text.
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
        // time → Time64(6): raw microseconds LE, 8 bytes.
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
        // timetz → String text carrying the zone (which the old fixed
        // encoding silently dropped).
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
        let plan = TablePlan::build(alloc, &rel, &m).expect("plan builds");
        assert!(plan.insert_sql.contains("INSERT INTO default.foo"));
        assert!(plan.insert_sql.contains("`id`"));
        assert!(plan.insert_sql.contains("`name`"));
        assert!(plan.insert_sql.contains("`_lsn`"));
        assert!(plan.insert_sql.contains("`_xid`"));
        assert!(plan.insert_sql.contains("`_op`"));
        assert!(plan.insert_sql.contains("`_commit_ts`"));
        assert!(plan.insert_sql.ends_with(") FORMAT Native"));
    }

    #[test]
    fn encoder_accumulates_into_typed_buffers() {
        let alloc = Allocator::stdlib();
        let rel = mk_rel();
        let m = mk_mapping();
        let plan = TablePlan::build(alloc, &rel, &m).unwrap();
        let mut enc = TableEncoder::new(plan).unwrap();
        enc.append_row(&committed(7, Some("seven")), &m, OP_INSERT)
            .unwrap();
        enc.append_row(&committed(8, None), &m, OP_INSERT).unwrap();
        enc.append_row(&committed(9, Some("nine")), &m, OP_INSERT)
            .unwrap();
        assert_eq!(enc.rows, 3);
        // Column 0 (Int32, non-null): 4 bytes * 3 rows = 12 bytes.
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
        // Column 1 (Nullable(String)): null_map [0,1,0], offsets [5, 5, 9].
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
        // Synthetic _lsn at index 2 (after the 2 mapped columns).
        let off = m.columns.len();
        match &enc.buffers[off] {
            ColumnBuf::Fixed { bytes, .. } => {
                assert_eq!(bytes.len(), 24);
                assert_eq!(&bytes[0..8], &0xCAFEu64.to_le_bytes());
            }
            other => panic!("_lsn expected Fixed, got {other:?} variant tag"),
        }
        // _op = INSERT = 1.
        match &enc.buffers[off + 2] {
            ColumnBuf::Fixed { bytes, .. } => assert_eq!(bytes, &vec![1u8, 1, 1]),
            _ => panic!("_op expected Fixed"),
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

            [table."public.foo"]
            target = "default.foo"
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
        // Omitting `secure` defaults to plaintext.
        assert!(
            !EmitterConfig::from_toml_str("[ch]\nhost = \"h\"\n")
                .unwrap()
                .secure
        );
        assert_eq!(c.row_budget, 1024);
        assert_eq!(c.byte_budget, 4096);
        let t = c.tables.get("public.foo").expect("mapping present");
        assert_eq!(t.target, "default.foo");
        assert_eq!(t.columns.len(), 2);
        assert_eq!(t.columns[0].src_attnum, 1);
        assert_eq!(t.columns[1].target_type, "Nullable(String)");
    }
}
