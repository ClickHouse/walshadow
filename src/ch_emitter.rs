//! CH-native emitter primitives via `clickhouse-c-rs`. Batching, seal
//! triggers, and xact close live in [`crate::pipeline`], not here.
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

/// Microseconds between PG `TimestampTz` epoch (2000-01-01 UTC) and Unix
/// epoch. CH `DateTime64(6)` is Unix microseconds; PG commit-record
/// `xact_time` and tuple `TimestampTz` are PG-epoch microseconds.
pub const DATETIME64_PG_EPOCH_US: i64 = 946_684_800_000_000;

/// Heap op codes for [`TableEncoder::append_row`]; `OP_DELETE` sets `_is_deleted`
pub const OP_INSERT: i8 = 1;
pub const OP_UPDATE: i8 = 2;
pub const OP_DELETE: i8 = 3;

/// Default block accumulator budgets. Mirror common CH server defaults
pub const DEFAULT_ROW_BUDGET: usize = 65_536;
pub const DEFAULT_BYTE_BUDGET: usize = 1 << 20; // 1 MiB

/// Default flush timeout (ms). `0` keeps serial emitter's
/// close-INSERT-on-every-xact-end behaviour (bootstrap backfill only);
/// live pipeline substitutes a 100ms partial-batch deadline for `0` so
/// cold tables can't pin the watermark. Positive value holds INSERTs
/// open across xacts, seals on a deadline armed at first row of a fresh
/// INSERT.
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

/// Per-replica connection + mapping config. TOML `[ch]` table holds
/// connection params, `[table."<src>"]` blocks declare per-relation
/// mapping; parse via [`EmitterConfig::from_toml_str`].
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
    /// Keyed on `"<namespace>.<relname>"`
    pub tables: HashMap<String, TableMapping>,
    /// Per-namespace defaults keyed on PG schema name; per-table
    /// entries in `tables` win for the relation they name
    pub namespaces: HashMap<String, NamespaceMapping>,
    /// Global `--drop-table-strategy` default; per-namespace override
    /// via `[namespace.<ns>] drop_table_strategy = ...`
    pub drop_table_strategy: String,
    pub retry: RetryConfig,
    /// Wall-clock cap on a single INSERT attempt. A connection that
    /// wedges mid-INSERT surfaces as retryable [`EmitterError::Timeout`]
    /// so the inserter reconnects + resends rather than pinning the
    /// durable watermark forever. Sized far above a healthy round-trip.
    pub insert_timeout: Duration,
    /// Keep `_is_deleted` out of `ReplacingMergeTree`'s args so delete
    /// tombstones stay queryable instead of collapsing on FINAL. Column
    /// always emitted; off by default
    pub soft_delete: bool,
    /// Where externally-TOASTed chunks live + miss policy. `[toast]` block;
    /// default disabled (NULL/default-fill unrecoverable values)
    pub toast: crate::toast::ToastConfig,
}

pub const DEFAULT_INSERT_TIMEOUT_SECS: u64 = 30;

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
            namespaces: HashMap::new(),
            drop_table_strategy: "retain".into(),
            retry: RetryConfig::default(),
            insert_timeout: Duration::from_secs(DEFAULT_INSERT_TIMEOUT_SECS),
            soft_delete: false,
            toast: crate::toast::ToastConfig::default(),
        }
    }
}

/// Wire-protocol compression choice. Variants gated on Cargo features
/// in the top crate; unsupported variants fail at
/// [`CompressionChoice::build_codec`] with
/// [`EmitterError::CompressionUnsupported`].
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

    /// Feature-gates flow up from clickhouse-c-rs so the C TU only links
    /// a codec lib when its matching feature is on in the top crate.
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

/// Per-source-relation destination metadata: which source attnums to
/// ship and what CH type to advertise, one entry per non-synthetic
/// column.
#[derive(Debug, Clone)]
pub struct TableMapping {
    pub target: String,
    pub columns: Vec<ColumnMapping>,
}

/// Per-namespace defaults. Operator-pinned blocks shaped like:
///
/// ```toml
/// [namespace."public"]
/// target_database = "default"
/// auto_create = true
/// drop_table_strategy = "retain"   # one of retain / drop / warn
/// ```
///
/// `auto_create = true` lets [`crate::ch_ddl::DdlApplicator`] run
/// `CREATE TABLE IF NOT EXISTS` on first descriptor sighting.
/// `target_database` overrides global `[ch] database` for this namespace.
#[derive(Debug, Clone, Default)]
pub struct NamespaceMapping {
    pub target_database: Option<String>,
    pub auto_create: bool,
    pub drop_table_strategy: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ColumnMapping {
    /// Source `pg_attribute.attnum` (1-based)
    pub src_attnum: i16,
    pub target_name: String,
    /// CH type expression, parsed via [`TypeAst::parse`]. Emitter does
    /// not validate against the source PG type; CH rejects on `INSERT`
    /// if they mismatch.
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
            if let Some(v) = ch.get("soft_delete").and_then(Value::as_bool) {
                out.soft_delete = v;
            }
        }
        if let Some(toast) = root.get("toast").and_then(Value::as_table) {
            if let Some(v) = toast.get("mode").and_then(Value::as_str) {
                out.toast.mode = crate::toast::ToastMode::parse(v).map_err(EmitterError::Config)?;
            }
            if let Some(v) = toast.get("disk_dir").and_then(Value::as_str) {
                out.toast.disk_dir = Some(std::path::PathBuf::from(v));
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

/// Cached plan for one destination table, built lazily on first row.
pub struct TablePlan {
    pub target: String,
    pub columns: Vec<ColumnPlan>,
    pub synth_lsn: ColumnPlan,
    pub synth_xid: ColumnPlan,
    pub synth_commit_ts: ColumnPlan,
    /// `_is_deleted Bool` (1 on delete, else 0), always appended last
    pub synth_is_deleted: ColumnPlan,
    /// Pre-formatted so on-tuple paths don't reassemble per row
    pub insert_sql: String,
}

pub struct ColumnPlan {
    pub name: String,
    pub type_repr: String,
    pub ast: TypeAst,
    pub decimal: Option<DecimalWire>,
}

/// Physical wire width of a CH `Decimal`: one of four signed-integer
/// backings. Discriminants are the byte widths, so `as usize` recovers
/// the size.
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
    /// Synthetic columns always non-nullable (emitter always populates).
    pub(crate) fn build(
        alloc: Allocator,
        rel: &RelDescriptor,
        mapping: &TableMapping,
    ) -> Result<Self, EmitterError> {
        let mut columns = Vec::with_capacity(mapping.columns.len());
        let mut col_sql = Vec::with_capacity(mapping.columns.len() + 4);
        // Mapping attnums absent from the catalog descriptor are not a
        // hard error: schema-evolution pre-declares post-ALTER columns,
        // pre-ALTER xacts legitimately see fewer attnums. append_row
        // emits NULL for any missing attnum, so a static-config typo
        // surfaces as an always-NULL column (or CH reject if non-nullable)
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
        let synth_commit_ts = mk("_commit_ts", "DateTime64(6, 'UTC')")?;
        let synth_is_deleted = mk("_is_deleted", "Bool")?;
        col_sql.push(quote_ident(&synth_lsn.name));
        col_sql.push(quote_ident(&synth_xid.name));
        col_sql.push(quote_ident(&synth_commit_ts.name));
        col_sql.push(quote_ident(&synth_is_deleted.name));
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
            synth_commit_ts,
            synth_is_deleted,
            insert_sql,
        })
    }
}

pub(crate) fn quote_ident(name: &str) -> String {
    // CH backtick quoting (mirrors upstream `quoteIdentIfNeed`):
    // backticks inside identifiers escape via doubling
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

/// Open a CH [`AsyncClient`]. Shared by inserter pool and
/// [`crate::ch_ddl::DdlApplicator::new`] so connection options stay in
/// one place.
pub(crate) async fn connect_client(config: &EmitterConfig) -> Result<AsyncClient, EmitterError> {
    let codec = config.compression.build_codec()?;
    let mut opts = ClientOpts::new()
        .database(&config.database)
        .user(&config.user)
        .password(&config.password);
    opts.compression = config.compression.to_wire();
    let addr = (config.host.as_str(), config.port);
    let client = if config.secure {
        // Caller's pinned config wins (private CA / self-signed / mTLS);
        // else public webpki roots cover ClickHouse Cloud's CA
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
/// `Exception` packet as [`EmitterError::ServerException`]. Used after a
/// `send_query`/`send_data_end()` expecting no result rows (INSERT seal,
/// TRUNCATE, DDL).
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

/// Per-table per-xact accumulator, one block buffer per CH column.
pub struct TableEncoder {
    pub plan: TablePlan,
    pub rows: usize,
    pub approx_bytes: usize,
    /// Mirrors `plan.columns + 4 synth`
    pub buffers: Vec<ColumnBuf>,
}

/// On-the-wire-shape column buffer. [`BlockBuilder`] borrows these
/// slices at flush time; cleared after `send_data`.
pub enum ColumnBuf {
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

pub(crate) fn append_buf<'a>(
    bb: &mut BlockBuilder<'a>,
    name: &str,
    ast: &'a TypeAst,
    buf: &'a ColumnBuf,
    n_rows: usize,
) -> Result<(), EmitterError> {
    // SAFETY: BlockBuilder retains pointers into these slabs until
    // `send_data` returns; buffers in `TableEncoder.buffers` are not
    // mutated until `clear()` runs after this returns
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
        ColumnValue::Date(n) => buf.append_fixed_bytes(&n.to_le_bytes()),
        // `time` → `Time64(6)`: microseconds since midnight, no epoch offset
        ColumnValue::Time(n) => buf.append_fixed_bytes(&n.to_le_bytes()),
        ColumnValue::Timestamp(n) | ColumnValue::TimestampTz(n) => {
            let unix_us = n.saturating_add(DATETIME64_PG_EPOCH_US);
            buf.append_fixed_bytes(&unix_us.to_le_bytes())
        }
        // `timetz` → text: CH has no zone-aware time type, text keeps
        // the offset the old fixed encoding dropped
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

/// Shared per-relation mapping handle. Daemon swaps the whole `HashMap`
/// atomically on SIGHUP reload; readers see the swap between rows, and a
/// table's cached [`TablePlan`] rebuilds on the next epoch (after a
/// barrier) so subsequent batches encode against the new map.
pub type MappingHandle = Arc<tokio::sync::RwLock<HashMap<String, TableMapping>>>;

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
        /// TOAST chunk rows persisted to the configured store (disk / CH)
        pub toast_chunks_stored,
        /// Toasted values reassembled from the store (not the in-xact buffer)
        pub toast_values_fetched,
        /// Toasted values NULL/default-filled because no store could rebuild
        /// them (disabled mode). Surfaced, never silent
        pub toast_values_filled_default,
        /// Toasted values whose chunks were absent from an active store
        /// (disk / CH gap) — a real data gap, not a fill
        pub toast_fetch_miss,
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

pub(crate) fn is_retryable(e: &EmitterError) -> bool {
    matches!(
        e,
        EmitterError::Io(_)
            | EmitterError::Client(_)
            | EmitterError::ServerException { .. }
            | EmitterError::Timeout { .. }
    )
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::heap_decoder::{DecodedHeap, DecodedTuple};
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
        use walrus::pg::walparser::RelFileNode;
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
        use crate::codecs::NumericKind;
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
        let plan = TablePlan::build(alloc, &rel, &m).expect("plan builds");
        assert!(plan.insert_sql.contains("INSERT INTO default.foo"));
        assert!(plan.insert_sql.contains("`id`"));
        assert!(plan.insert_sql.contains("`name`"));
        assert!(plan.insert_sql.contains("`_lsn`"));
        assert!(plan.insert_sql.contains("`_xid`"));
        assert!(!plan.insert_sql.contains("`_op`"));
        assert!(plan.insert_sql.contains("`_commit_ts`"));
        assert!(plan.insert_sql.contains("`_is_deleted`"));
        assert!(plan.insert_sql.ends_with(") FORMAT Native"));
    }

    #[test]
    fn is_deleted_codes_delete_in_trailing_buffer() {
        let alloc = Allocator::stdlib();
        let rel = mk_rel();
        let m = mk_mapping();
        let plan = TablePlan::build(alloc, &rel, &m).expect("plan builds");
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
        let plan = TablePlan::build(alloc, &rel, &m).expect("plan builds");
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
        let plan = TablePlan::build(alloc, &rel, &m).expect("plan builds");
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
        let plan = TablePlan::build(alloc, &rel, &m).unwrap();
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
        // Omitting `secure` defaults to plaintext
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
        // soft_delete defaults off when the key is absent
        assert!(!c.soft_delete);
    }

    #[test]
    fn config_soft_delete_defaults_off_and_parses_on() {
        assert!(!EmitterConfig::default().soft_delete);
        let c = EmitterConfig::from_toml_str("[ch]\nsoft_delete = true\n").unwrap();
        assert!(c.soft_delete);
    }
}
