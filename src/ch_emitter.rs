//! Phase 7 — CH Native emitter via [`clickhouse-c-rs`].
//!
//! Translates committed-xact tuple streams into per-table INSERT
//! statements over a single TCP `Client` per CH replica. Lifecycle per
//! `(destination table, xact)`:
//!
//! 1. First row → buffer in [`TableEncoder`]; mark INSERT pending.
//! 2. Either the `row_budget` or `byte_budget` trips → seal one complete
//!    INSERT: `send_query` + `send_data(Some(&bb))` + `send_data(None)`,
//!    drain to `EndOfStream`, then clear the buffer. The buffer is never
//!    cleared before `EndOfStream`, so a mid-flush disconnect replays
//!    rather than loses rows (CH dedups any replay by `_lsn`).
//! 3. `on_xact_end` (called by [`XactBuffer::commit`](crate::xact_buffer::XactBuffer))
//!    seals every still-buffered table the same way (legacy mode, or
//!    when the hold-open `flush_timeout` deadline has tripped). In
//!    hold-open mode rows accumulate across xacts until a budget trip or
//!    the deadline seals them, batching small xacts into fewer parts.
//!
//! Synthetic columns `_lsn UInt64`, `_xid UInt32`, `_op Enum8(...)`,
//! `_commit_ts DateTime64(6, 'UTC')` are appended after every mapped
//! column. PG's `TimestampTz` epoch is 2000-01-01; we shift to the Unix
//! epoch (`DATETIME64_PG_EPOCH_US`) so `DateTime64(6)` semantics line up
//! with ClickHouse.
//!
//! ## Compression
//!
//! Codec choice is feature-gated via walshadow's own `lz4` / `zstd`
//! Cargo features, which forward to [`clickhouse-c-rs`]'s matching
//! features (see top-level `Cargo.toml`). When a feature is off, the
//! corresponding [`Compression`] variant fails to construct at
//! `EmitterConfig::resolve_codec`. Default builds advertise LZ4 to
//! match the CH server default.
//!
//! ## Cross-table ordering inside an xact
//!
//! `Client` is single-query-at-a-time, so an xact touching tables T1
//! and T2 lands as: every T1 row first (one INSERT), then every T2
//! (next INSERT). Original WAL interleaving across tables is not
//! preserved. `_lsn` carries the source LSN so
//! `ReplacingMergeTree`-style dedup still keys on the right value.
//! WAL ordering within a single destination table is preserved.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};

use clickhouse_c::{
    Allocator, BlockBuilder, BlockOpts, Client, ClientOpts, Codec, Compression, Kind, PacketKind,
    PosixIo, TypeAst,
};
use thiserror::Error;

use crate::ch_ddl::DdlApplicator;
use crate::decoder_sink::{DecoderSinkError, TupleObserver};
use crate::heap_decoder::{ColumnValue, CommittedTuple, HeapOp};
use crate::relation_resolver::RelationResolver;
use crate::shadow_catalog::{CatalogError, RelDescriptor, SchemaEvent};

/// Microsecond offset between PG `TimestampTz` epoch (2000-01-01 UTC)
/// and the Unix epoch. `DateTime64(6)` in ClickHouse is Unix
/// microseconds; PG's commit-record `xact_time` and tuple
/// `TimestampTz` columns are PG-epoch microseconds.
pub const DATETIME64_PG_EPOCH_US: i64 = 946_684_800_000_000;

/// `_op` Enum8 codes — keep in sync with the `Enum8('insert'=1, ...)`
/// type advertised by [`TablePlan::synthetic_op_type`].
pub const OP_INSERT: i8 = 1;
pub const OP_UPDATE: i8 = 2;
pub const OP_DELETE: i8 = 3;

/// Default block accumulator budgets. Mirror common ClickHouse server
/// defaults; tunable via [`EmitterConfig`].
pub const DEFAULT_ROW_BUDGET: usize = 65_536;
pub const DEFAULT_BYTE_BUDGET: usize = 1 << 20; // 1 MiB

/// Default flush timeout (ms) — `0` keeps the legacy
/// close-INSERT-on-every-xact-end behaviour. Set this to a positive
/// value via TOML (`flush_timeout_ms`) or `--ch-flush-timeout-ms` to
/// let the emitter hold INSERTs open across xacts and close them on a
/// deadline that starts when the first row of a fresh INSERT lands.
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
    pub compression: CompressionChoice,
    pub row_budget: usize,
    pub byte_budget: usize,
    /// Hold INSERTs open across xacts. Timer starts when the first row
    /// of a fresh INSERT lands and trips at `now + flush_timeout`; on
    /// trip the emitter closes every still-open INSERT (one
    /// `send_data(None)` + drain to `EndOfStream` per table) and
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
    /// PHASE15 §5 — per-source-namespace defaults. Auto-DDL flow.
    /// Keyed on PG schema name (`"public"`, etc.); per-table entries
    /// in `tables` still win for the relation they name.
    pub namespaces: HashMap<String, NamespaceMapping>,
    /// PHASE15 §6 — global `--drop-table-strategy` default. Per-namespace
    /// override via `[namespace.<ns>] drop_table_strategy = ...`.
    pub drop_table_strategy: String,
    /// Phase 10 bounded retry against a single CH replica. See
    /// [`RetryConfig`] for semantics.
    pub retry: RetryConfig,
}

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
            compression: CompressionChoice::default(),
            row_budget: DEFAULT_ROW_BUDGET,
            byte_budget: DEFAULT_BYTE_BUDGET,
            flush_timeout: Duration::from_millis(DEFAULT_FLUSH_TIMEOUT_MS),
            tables: HashMap::new(),
            namespaces: HashMap::new(),
            drop_table_strategy: "retain".into(),
            retry: RetryConfig::default(),
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

/// PHASE15 §5 — per-namespace defaults. Operator-pinned blocks shaped
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

    /// Top-level construction: connect, build codec, return ready
    /// [`Emitter`]. Requires a connected `TcpStream` from the caller
    /// (Phase 7 plumbing); the caller hands the fd over to the emitter,
    /// which owns it for the lifetime of the connection.
    pub fn connect(
        self,
        resolver: Arc<dyn RelationResolver>,
        tcp: std::net::TcpStream,
    ) -> Result<Emitter, EmitterError> {
        Emitter::new(self, resolver, tcp)
    }
}

/// Cached plan for one destination table. Built lazily the first time
/// a tuple lands for the relation; stored in [`Emitter::tables`]
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
    fn build(
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

fn quote_ident(name: &str) -> String {
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

    fn clear(&mut self) {
        match self {
            Self::Fixed { bytes, .. } => bytes.clear(),
            Self::String { offsets, data } => {
                offsets.clear();
                data.clear();
            }
            Self::NullableFixed {
                null_map, inner, ..
            } => {
                null_map.clear();
                inner.clear();
            }
            Self::NullableString {
                offsets,
                data,
                null_map,
            } => {
                offsets.clear();
                data.clear();
                null_map.clear();
            }
        }
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

impl TableEncoder {
    fn new(plan: TablePlan) -> Result<Self, EmitterError> {
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
        Ok(Self {
            plan,
            rows: 0,
            approx_bytes: 0,
            buffers,
        })
    }

    fn clear(&mut self) {
        for b in &mut self.buffers {
            b.clear();
        }
        self.rows = 0;
        self.approx_bytes = 0;
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

    /// Build a [`BlockBuilder`] over the accumulated buffers and send
    /// it through `client.send_data`. Returns the row count just sent.
    fn flush_block(
        &mut self,
        client: &mut Client,
        alloc: Allocator,
        opts: BlockOpts,
    ) -> Result<usize, EmitterError> {
        if self.rows == 0 {
            return Ok(0);
        }
        let mut bb = BlockBuilder::new(alloc)?;
        let n_rows = self.rows;
        // Mapped columns
        for (i, plan) in self.plan.columns.iter().enumerate() {
            append_buf(&mut bb, &plan.name, &plan.ast, &self.buffers[i], n_rows)?;
        }
        let off = self.plan.columns.len();
        append_buf(
            &mut bb,
            &self.plan.synth_lsn.name,
            &self.plan.synth_lsn.ast,
            &self.buffers[off],
            n_rows,
        )?;
        append_buf(
            &mut bb,
            &self.plan.synth_xid.name,
            &self.plan.synth_xid.ast,
            &self.buffers[off + 1],
            n_rows,
        )?;
        append_buf(
            &mut bb,
            &self.plan.synth_op.name,
            &self.plan.synth_op.ast,
            &self.buffers[off + 2],
            n_rows,
        )?;
        append_buf(
            &mut bb,
            &self.plan.synth_commit_ts.name,
            &self.plan.synth_commit_ts.ast,
            &self.buffers[off + 3],
            n_rows,
        )?;
        client.send_data(Some(&bb))?;
        drop(bb);
        // Caller is responsible for `opts`; `BlockBuilder::write` is
        // not called because we go through the TCP packet loop.
        let _ = opts;
        // Buffer is NOT cleared here. Durability lands only at the
        // wire's `EndOfStream`; if the connection drops between this
        // send and that ack, CH rolls back the whole in-progress
        // INSERT, so the rows must stay buffered for the retry to
        // replay. `close_current_wire` clears post-`EndOfStream`.
        Ok(n_rows)
    }
}

fn append_buf<'a>(
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

/// Shared per-relation mapping handle. Cloneable via the inner `Arc`;
/// the daemon hands one clone to [`Emitter`] and keeps another to
/// support SIGHUP reload (atomic swap of the entire `HashMap`).
///
/// Phase 10 sees the swap *between* xacts: tables already cached in
/// [`Emitter::tables`] keep their old [`TablePlan`] until the xact
/// drains and clears the cache. The next xact consults the new map.
pub type MappingHandle = Arc<tokio::sync::RwLock<HashMap<String, TableMapping>>>;

/// Phase 7 CH-Native emitter. Holds one [`Client`] over a connected
/// TCP socket per CH replica plus the per-table accumulator state.
pub struct Emitter {
    client: Client<'static>,
    alloc: Allocator,
    config: EmitterConfig,
    /// Phase 10 SIGHUP-reloadable mapping. Initial value is cloned from
    /// `config.tables` at construction; later mutations come via
    /// [`Emitter::mapping_handle`] reaching out from the daemon's
    /// SIGHUP task.
    mapping: MappingHandle,
    resolver: Arc<dyn RelationResolver>,
    tables: HashMap<String, TableEncoder>,
    /// xid currently held in `tables`. Reset on `on_xact_end`.
    current_xid: Option<u32>,
    /// Set when any INSERT opens (first row after the previous close)
    /// and `flush_timeout > 0`. Cleared on close. Drives the
    /// hold-INSERT-open path's deadline check inside
    /// [`Self::on_xact_end_with_lsn`].
    flush_deadline: Option<Instant>,
    /// Highest `commit_lsn` of any row buffered in a [`TableEncoder`]
    /// but not yet sealed by a completed INSERT (`EndOfStream`). Bumped
    /// per tuple in [`Self::route`], reset to `0` once every buffer has
    /// drained, then `last_durable_commit_lsn` adopts it.
    pending_max_commit_lsn: u64,
    /// Highest `commit_lsn` known durable on CH (i.e. covered by a
    /// closed INSERT's `EndOfStream`). xact_buffer pulls this through
    /// [`EmitterObserver::on_xact_end`] to advance `emitter_ack_lsn`.
    /// Monotonic; only bumped inside [`Self::close_all_open_inserts`]
    /// or when an empty/untracked xact arrives with no rows pending.
    last_durable_commit_lsn: u64,
    /// PHASE15 §2 — CH-side DDL writer. Owns its own clickhouse-c
    /// `Client` (separate from `self.client` so DDL doesn't interleave
    /// with INSERT data). `None` when the emitter is wired without a
    /// DDL applicator (tests, transitional bootstrap emitter).
    applicator: Option<DdlApplicator>,
    /// Key of the table whose `INSERT INTO ... FORMAT Native` is
    /// currently open on `client`. `None` between `send_data(None)` +
    /// EndOfStream drain and the next `send_query`. CH's Native protocol
    /// rejects a fresh `Query` while another INSERT data stream is open
    /// (code 101 "Unexpected packet Query"), so any wire activity for
    /// table B must call `close_current_wire` first if A is open.
    wire_open_key: Option<String>,
    pub stats: EmitterStats,
}

#[derive(Debug, Default, Clone)]
pub struct EmitterStats {
    pub rows_emitted: u64,
    pub blocks_sent: u64,
    pub xacts_committed: u64,
    pub unsupported_relations: u64,
    /// Rows whose filenode resolved to a foreign database (physical WAL
    /// carries the whole cluster). Skipped, not an error.
    pub foreign_db_rows_skipped: u64,
    pub unsupported_values: u64,
    /// Phase 10 retry bookkeeping. `reconnects` ticks on every fresh
    /// CH socket the daemon hot-replaces; `retries_attempted` ticks on
    /// every reconnect-triggered retry (one per failing operation,
    /// not per attempt — so a single op that needed 3 retries adds 3).
    pub reconnects: u64,
    pub retries_attempted: u64,
    /// `HeapOp::Truncate` events shipped to CH as `TRUNCATE TABLE`.
    pub truncates_emitted: u64,
    /// Number of times the hold-INSERT-open deadline tripped (i.e.
    /// `flush_timeout` elapsed since the first row of a fresh INSERT
    /// landed) and forced a multi-xact close. Stays at `0` when
    /// `flush_timeout == 0` because the legacy close-per-xact path
    /// fires through [`Self::close_all_open_inserts`] without setting
    /// a deadline.
    pub flush_deadline_trips: u64,
}

impl Emitter {
    pub fn new(
        config: EmitterConfig,
        resolver: Arc<dyn RelationResolver>,
        tcp: std::net::TcpStream,
    ) -> Result<Self, EmitterError> {
        let alloc = Allocator::stdlib();
        let codec = config.compression.build_codec()?;
        let io = PosixIo::new_owned(tcp);
        let mut opts = ClientOpts::new()
            .database(&config.database)
            .user(&config.user)
            .password(&config.password);
        opts.compression = config.compression.to_wire();
        let client = Client::init(&opts, alloc, io, codec)?;
        let mapping = Arc::new(tokio::sync::RwLock::new(config.tables.clone()));
        Ok(Self {
            client,
            alloc,
            config,
            mapping,
            resolver,
            tables: HashMap::new(),
            current_xid: None,
            flush_deadline: None,
            pending_max_commit_lsn: 0,
            last_durable_commit_lsn: 0,
            applicator: None,
            wire_open_key: None,
            stats: EmitterStats::default(),
        })
    }

    /// PHASE15 §2 — attach a DDL applicator. Caller opens the
    /// applicator's own TCP connection and passes it through
    /// [`DdlApplicator::new`] before handing it off here. Once
    /// attached, schema events drained from the xact buffer route
    /// through the applicator before the next `on_tuple` encodes
    /// rows against the post-DDL shape.
    pub fn with_applicator(mut self, applicator: DdlApplicator) -> Self {
        self.applicator = Some(applicator);
        self
    }

    pub fn applicator_mut(&mut self) -> Option<&mut DdlApplicator> {
        self.applicator.as_mut()
    }

    /// SIGHUP target. Daemon clones this handle at boot and feeds the
    /// post-reload TOML mapping back through `*handle.write().await = new`.
    pub fn mapping_handle(&self) -> MappingHandle {
        self.mapping.clone()
    }

    /// Route one committed tuple. INSERT/UPDATE land via the `new`
    /// image; DELETE pulls from `old`. HOT_UPDATE is treated as UPDATE
    /// downstream (op-code 2) — CH's `ReplacingMergeTree` cares about
    /// `_lsn` ordering, not the PG-internal HOT distinction.
    async fn route(&mut self, committed: &CommittedTuple) -> Result<(), EmitterError> {
        if self.current_xid.is_none() {
            self.current_xid = Some(committed.decoded.xid);
        }
        let rel = match self
            .resolver
            .relation_at(committed.decoded.rfn, committed.decoded.source_lsn)
            .await
        {
            Ok(rel) => rel,
            // Foreign-DB WAL: skip like an unmapped relation (no append,
            // no poison), let the ack advance past it.
            Err(CatalogError::ForeignDatabase(_)) => {
                self.stats.foreign_db_rows_skipped += 1;
                return Ok(());
            }
            Err(e) => return Err(e.into()),
        };
        let mapping = {
            let m = self.mapping.read().await;
            match m.get(rel.qualified_name.as_ref()).cloned() {
                Some(v) => v,
                None => {
                    self.stats.unsupported_relations += 1;
                    return Ok(());
                }
            }
        };
        // Truncate executes in WAL order against the destination.
        // Drain any pending per-table buffer for this relation first
        // (so prior INSERTs in the same xact land before the truncate),
        // then issue `TRUNCATE TABLE <dest>` synchronously. Closing
        // the current wire (if any) lets `send_query(TRUNCATE)` ride
        // the same connection without tripping CH's "one Query at a
        // time" invariant.
        if let HeapOp::Truncate = committed.decoded.op {
            let key = rel.qualified_name.as_ref().to_owned();
            if self.tables.get(&key).is_some_and(|e| e.rows > 0) {
                self.flush_table(&key)?;
            }
            self.close_current_wire()?;
            self.tables.remove(&key);
            let sql = format!("TRUNCATE TABLE {}", mapping.target);
            self.client.send_query(&sql, None)?;
            loop {
                let mut pkt = self.client.recv_packet()?;
                match pkt.kind() {
                    Some(PacketKind::EndOfStream) => break,
                    Some(PacketKind::Exception) => {
                        if let Some(exc) = pkt.take_exception() {
                            return Err(EmitterError::ServerException {
                                code: exc.code(),
                                message: String::from_utf8_lossy(exc.display_text()).into_owned(),
                            });
                        }
                        break;
                    }
                    _ => {}
                }
            }
            self.stats.truncates_emitted += 1;
            return Ok(());
        }
        // Lazily build the per-table encoder on first row. NO wire
        // activity here: pgbench's TPC-B xact touches 4 tables per
        // commit, so eagerly closing prior wires on each table-switch
        // would cost one CH round-trip per row. Wire opens happen at
        // flush time (budget trip in this fn, or close_all at deadline /
        // legacy per-xact close).
        if !self.tables.contains_key(rel.qualified_name.as_ref()) {
            let plan = TablePlan::build(self.alloc, &rel, &mapping)?;
            let enc = TableEncoder::new(plan)?;
            let owned_key = rel.qualified_name.as_ref().to_owned();
            self.tables.insert(owned_key, enc);
        }
        let enc = self
            .tables
            .get_mut(rel.qualified_name.as_ref())
            .expect("just inserted");
        let op = match committed.decoded.op {
            HeapOp::Insert => OP_INSERT,
            HeapOp::Update | HeapOp::HotUpdate => OP_UPDATE,
            HeapOp::Delete => OP_DELETE,
            // Routed above; unreachable here.
            HeapOp::Truncate => return Ok(()),
        };
        if let Err(e) = enc.append_row(committed, &mapping, op) {
            self.stats.unsupported_values += 1;
            return Err(e);
        }
        self.pending_max_commit_lsn = self.pending_max_commit_lsn.max(committed.commit_lsn);
        let tripped =
            enc.rows >= self.config.row_budget || enc.approx_bytes >= self.config.byte_budget;
        // Budget trip seals this table's buffered rows as one complete
        // INSERT (open → block → EndOfStream → clear). Bounds client
        // memory; an over-budget xact lands as several sealed parts
        // rather than one streamed-but-unconfirmed part.
        if tripped {
            let key_owned = rel.qualified_name.as_ref().to_owned();
            self.flush_table(&key_owned)?;
        }
        Ok(())
    }

    /// Ensure `key`'s INSERT is open on `client`, switching off any
    /// other table's open wire first (CH allows only one INSERT data
    /// stream per connection — see [`Self::wire_open_key`]). Issues
    /// `send_query(INSERT … FORMAT Native)` against `client` on first
    /// open for the table; subsequent calls are no-ops while the wire
    /// stays on `key`. Sets [`Self::flush_deadline`] when opening a
    /// fresh window under `flush_timeout > 0`.
    fn open_wire(&mut self, key: &str) -> Result<(), EmitterError> {
        if self.wire_open_key.as_deref() == Some(key) {
            return Ok(());
        }
        self.close_current_wire()?;
        let enc = self
            .tables
            .get(key)
            .expect("open_wire called on unknown table");
        self.client.send_query(&enc.plan.insert_sql, None)?;
        self.wire_open_key = Some(key.to_owned());
        if !self.config.flush_timeout.is_zero() && self.flush_deadline.is_none() {
            self.flush_deadline = Some(Instant::now() + self.config.flush_timeout);
        }
        Ok(())
    }

    /// Close the currently-open INSERT (if any): ship any remaining
    /// rows in that table's encoder, then `send_data(None)` and drain
    /// to `EndOfStream`. Idempotent — no-op when no wire is open.
    /// Called between table-switches, on xact_end (legacy / deadline),
    /// before TRUNCATE / DDL, and on shutdown.
    fn close_current_wire(&mut self) -> Result<(), EmitterError> {
        let Some(key) = self.wire_open_key.take() else {
            return Ok(());
        };
        let alloc = self.alloc;
        if let Some(enc) = self.tables.get_mut(&key) {
            let n = enc.flush_block(&mut self.client, alloc, BlockOpts::default())?;
            if n > 0 {
                self.stats.rows_emitted += n as u64;
                self.stats.blocks_sent += 1;
            }
        }
        self.client.send_data(None)?;
        loop {
            let mut pkt = self.client.recv_packet()?;
            match pkt.kind() {
                Some(PacketKind::EndOfStream) => break,
                Some(PacketKind::Exception) => {
                    if let Some(exc) = pkt.take_exception() {
                        return Err(EmitterError::ServerException {
                            code: exc.code(),
                            message: String::from_utf8_lossy(exc.display_text()).into_owned(),
                        });
                    }
                    break;
                }
                _ => {}
            }
        }
        // EndOfStream confirmed: the INSERT is durable on CH, so the
        // rows just sealed are now safe to drop. Until this point the
        // buffer is the only replay source if the connection bounced.
        if let Some(enc) = self.tables.get_mut(&key) {
            enc.clear();
        }
        Ok(())
    }

    /// Seal `key`'s buffered rows as one complete, independently-durable
    /// INSERT: open the wire, ship the block, terminate, drain to
    /// `EndOfStream`, then clear the buffer. A budget trip lands here,
    /// so an over-budget xact produces several sealed parts rather than
    /// one streamed-but-unconfirmed part — the buffer never holds
    /// shipped-but-unacked rows that a disconnect would lose.
    fn flush_table(&mut self, key: &str) -> Result<(), EmitterError> {
        self.open_wire(key)?;
        self.close_current_wire()
    }

    /// Per-xact landmark. In hold-open mode rows from this xact stay
    /// buffered in their [`TableEncoder`]s, sharing the next flush
    /// window with later xacts — nothing ships until a budget trip or
    /// the deadline seals a complete INSERT. In legacy mode (or when
    /// the deadline has tripped) seals every buffered table now and
    /// drains each to `EndOfStream`. Either way, returns the highest
    /// `commit_lsn` now known durable on CH.
    ///
    /// `xact_buffer`'s `emitter_ack_lsn` tracks the returned value,
    /// not the input `commit_lsn`, so hold-open shows up as
    /// `emitter_ack_lsn` lagging `drain_lsn` until the next seal.
    fn on_xact_end_with_lsn(&mut self, commit_lsn: u64) -> Result<u64, EmitterError> {
        let now = Instant::now();
        let deadline_tripped = self.flush_deadline.is_some_and(|d| now >= d);
        if deadline_tripped {
            self.stats.flush_deadline_trips += 1;
        }
        let must_close = self.config.flush_timeout.is_zero() || deadline_tripped;
        if must_close {
            self.close_all_open_inserts()?;
        } else if self.flush_deadline.is_none() && self.tables.values().any(|e| e.rows > 0) {
            // Hold-open: rows queued from this xact share the next
            // flush window with subsequent xacts. Pure-NOOP commit
            // (route never reached append_row because every relation
            // was unmapped) leaves the deadline unset so an idle slot
            // doesn't tick on phantom traffic.
            self.flush_deadline = Some(Instant::now() + self.config.flush_timeout);
        }
        // Durable horizon can move only when nothing is buffered client-
        // side. In hold-open mode this falls through until the deadline
        // trips and `close_all_open_inserts` promotes; legacy mode
        // closes inside `must_close` above so this catches the empty-xact
        // case (heap touched only unmapped relations or pure DDL).
        if self.wire_open_key.is_none() && self.tables.values().all(|e| e.rows == 0) {
            self.last_durable_commit_lsn = self
                .last_durable_commit_lsn
                .max(self.pending_max_commit_lsn)
                .max(commit_lsn);
            self.pending_max_commit_lsn = 0;
        }
        self.current_xid = None;
        self.stats.xacts_committed += 1;
        Ok(self.last_durable_commit_lsn)
    }

    /// Seal every table with buffered rows as its own complete INSERT
    /// (each `flush_table` opens, ships, and drains to `EndOfStream`),
    /// then promote `pending_max_commit_lsn` to
    /// `last_durable_commit_lsn` and reset the flush window. The
    /// promotion is safe only because every buffer is now drained — the
    /// durable horizon never moves past a still-buffered row. Called
    /// from [`Self::on_xact_end_with_lsn`] on deadline trip or legacy
    /// per-xact close, and from [`Self::flush_open_inserts`] for
    /// explicit shutdown flush.
    fn close_all_open_inserts(&mut self) -> Result<(), EmitterError> {
        let keys: Vec<String> = self
            .tables
            .iter()
            .filter(|(_, enc)| enc.rows > 0)
            .map(|(k, _)| k.clone())
            .collect();
        for key in keys {
            self.flush_table(&key)?;
        }
        // Each flush_table already closed its wire; this is a defensive
        // no-op for a wire left open by a non-flush path (e.g. TRUNCATE
        // ordering). Empty encoders stay around so the next flush window
        // reuses the ColumnBuf allocations + cached TablePlan.
        self.close_current_wire()?;
        self.last_durable_commit_lsn = self
            .last_durable_commit_lsn
            .max(self.pending_max_commit_lsn);
        self.pending_max_commit_lsn = 0;
        self.flush_deadline = None;
        Ok(())
    }

    /// Explicit flush of every open INSERT — for shutdown paths.
    /// Equivalent to a deadline trip but doesn't require routing a
    /// synthetic xact. Returns the post-flush durable horizon.
    pub fn flush_open_inserts(&mut self) -> Result<u64, EmitterError> {
        self.close_all_open_inserts()?;
        Ok(self.last_durable_commit_lsn)
    }

    /// Idle-tick variant: closes only when [`Self::flush_deadline`]
    /// has elapsed. Cheap to call on every wakeup since the common
    /// case is "deadline not yet set" (no rows held) or "deadline
    /// still in the future" (recently opened). Counted under
    /// `flush_deadline_trips` on close.
    pub fn flush_if_deadline_tripped(&mut self) -> Result<u64, EmitterError> {
        if self.flush_deadline.is_some_and(|d| Instant::now() >= d) {
            self.stats.flush_deadline_trips += 1;
            self.close_all_open_inserts()?;
        }
        Ok(self.last_durable_commit_lsn)
    }

    /// Bounded-retry wrapper around [`Self::flush_if_deadline_tripped`].
    /// Mirrors [`Self::on_xact_end_with_retry`].
    pub async fn flush_if_deadline_tripped_with_retry(&mut self) -> Result<u64, EmitterError> {
        let retry = self.config.retry.clone();
        let mut attempt = 0u32;
        let mut backoff = retry.initial_backoff;
        loop {
            match self.flush_if_deadline_tripped() {
                Ok(ack) => return Ok(ack),
                Err(e) if is_retryable(&e) && attempt < retry.max_attempts => {
                    self.stats.retries_attempted += 1;
                    attempt += 1;
                    tokio::time::sleep(backoff).await;
                    backoff = backoff.saturating_mul(2).min(retry.max_backoff);
                    self.reconnect().await?;
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// Bounded-retry wrapper around [`Self::flush_open_inserts`].
    /// Called from the daemon shutdown path.
    pub async fn flush_open_inserts_with_retry(&mut self) -> Result<u64, EmitterError> {
        let retry = self.config.retry.clone();
        let mut attempt = 0u32;
        let mut backoff = retry.initial_backoff;
        loop {
            match self.flush_open_inserts() {
                Ok(ack) => return Ok(ack),
                Err(e) if is_retryable(&e) && attempt < retry.max_attempts => {
                    self.stats.retries_attempted += 1;
                    attempt += 1;
                    tokio::time::sleep(backoff).await;
                    backoff = backoff.saturating_mul(2).min(retry.max_backoff);
                    self.reconnect().await?;
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// Read-only snapshot of the durable horizon. Cheap; the
    /// xact_buffer doesn't poll this (it threads ack through
    /// [`TupleObserver::on_xact_end`]), but external probes (status
    /// line, metrics) can use it.
    pub fn last_durable_commit_lsn(&self) -> u64 {
        self.last_durable_commit_lsn
    }

    /// Ceiling for an idle-advance ack at `lsn`. When nothing is
    /// buffered client-side (no open wire, every encoder empty), `lsn`
    /// is fully durable-safe — trailing WAL past the last commit carries
    /// no rows to ship. Otherwise cap at `last_durable_commit_lsn` so a
    /// quiescent-tick nudge can't promote the ack past rows still held
    /// in open INSERTs. Mirrors the guard in `on_xact_end_with_lsn`.
    pub fn idle_ack_ceiling(&self, lsn: u64) -> u64 {
        let fully_drained =
            self.wire_open_key.is_none() && self.tables.values().all(|e| e.rows == 0);
        idle_ceiling(fully_drained, lsn, self.last_durable_commit_lsn)
    }

    /// Phase 10 reconnect: open a fresh TCP socket against the same
    /// `(host, port)`, build a new [`Client`] (which owns its own
    /// `PosixIo` + `Codec`), and hot-swap `self.client` while preserving
    /// the per-table accumulator state in `self.tables`. The fresh
    /// connection has nothing open, so [`Self::wire_open_key`] clears
    /// and the next [`Self::flush_table`] re-`send_query`s on demand.
    pub async fn reconnect(&mut self) -> Result<(), EmitterError> {
        let addr = format!("{}:{}", self.config.host, self.config.port);
        let tcp = tokio::net::TcpStream::connect(&addr).await.map_err(|e| {
            EmitterError::Io(std::io::Error::other(format!("reconnect to {addr}: {e}")))
        })?;
        tcp.set_nodelay(true).ok();
        let std_tcp = tcp.into_std()?;
        std_tcp.set_nonblocking(false)?;
        let codec = self.config.compression.build_codec()?;
        let io = PosixIo::new_owned(std_tcp);
        let mut opts = ClientOpts::new()
            .database(&self.config.database)
            .user(&self.config.user)
            .password(&self.config.password);
        opts.compression = self.config.compression.to_wire();
        let client = Client::init(&opts, self.alloc, io, codec)?;
        self.client = client;
        self.wire_open_key = None;
        self.stats.reconnects += 1;
        Ok(())
    }

    /// Wrap [`Self::route`] in bounded reconnect+retry per
    /// [`EmitterConfig::retry`]. Used by [`EmitterObserver::on_tuple`].
    /// Retry preserves the per-xact buffer state in `self.tables`, so
    /// a CH bounce mid-xact still lets the surviving buffered rows
    /// flush through the new connection.
    pub async fn route_with_retry(
        &mut self,
        committed: &CommittedTuple,
    ) -> Result<(), EmitterError> {
        let retry = self.config.retry.clone();
        let mut attempt = 0u32;
        let mut backoff = retry.initial_backoff;
        loop {
            match self.route(committed).await {
                Ok(()) => return Ok(()),
                Err(e) if is_retryable(&e) && attempt < retry.max_attempts => {
                    self.stats.retries_attempted += 1;
                    attempt += 1;
                    tokio::time::sleep(backoff).await;
                    backoff = backoff.saturating_mul(2).min(retry.max_backoff);
                    self.reconnect().await?;
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// PHASE15 §2 — dispatch a [`SchemaEvent`] to the in-process
    /// applicator. Closes the open INSERT for the AFFECTED relation
    /// first (`send_data(None)` + drain → durable on CH) so the CH
    /// ALTER doesn't race against a still-buffered INSERT against the
    /// pre-DDL shape. Re-encoding for the post-DDL shape happens
    /// lazily on the next `route` call — the per-relation
    /// [`TablePlan`] is dropped from `tables` so the next row rebuilds
    /// it off the fresh descriptor.
    ///
    /// Surgical close (this table only) keeps other tables' open
    /// INSERTs intact — important for the multi-table-per-xact path
    /// (pgbench's TPC-B writes 4 tables per xact) where closing-all
    /// would break the cross-INSERT pipeline.
    ///
    /// Schema events on unmapped relations still route through the
    /// applicator (CREATE TABLE auto-discovery for namespace-pattern
    /// matched relations); the applicator handles the no-mapping case
    /// internally.
    pub async fn dispatch_schema_event(&mut self, event: &SchemaEvent) -> Result<(), EmitterError> {
        let affected: Option<String> = match event {
            SchemaEvent::Added { desc } => Some(desc.qualified_name.as_ref().to_owned()),
            SchemaEvent::Changed { new, .. } => Some(new.qualified_name.as_ref().to_owned()),
            SchemaEvent::Dropped { qualified_name, .. } => Some(qualified_name.as_ref().to_owned()),
        };
        if let Some(key) = affected.as_deref() {
            if self.tables.get(key).is_some_and(|e| e.rows > 0) {
                self.flush_table(key)?;
            }
            if self.wire_open_key.as_deref() == Some(key) {
                self.close_current_wire()?;
            }
            self.tables.remove(key);
        }
        if let Some(app) = self.applicator.as_mut() {
            app.apply(event).await?;
        }
        Ok(())
    }

    /// Bounded-retry wrapper around [`Self::dispatch_schema_event`].
    /// DDL failures retry through `reconnect_applicator` (today: just
    /// surface the error — the applicator's CH connection lives on a
    /// separate TCP and reconnect is operator-quiesced).
    pub async fn dispatch_schema_event_with_retry(
        &mut self,
        event: &SchemaEvent,
    ) -> Result<(), EmitterError> {
        // Currently no retry — DDL errors poison the stream so the
        // operator sees them. PHASE16 may add bounded reconnect for the
        // DDL connection.
        self.dispatch_schema_event(event).await
    }

    /// Wrap [`Self::on_xact_end_with_lsn`] in bounded reconnect+retry.
    /// Same shape as [`Self::route_with_retry`] — see notes there.
    pub async fn on_xact_end_with_retry(&mut self, commit_lsn: u64) -> Result<u64, EmitterError> {
        let retry = self.config.retry.clone();
        let mut attempt = 0u32;
        let mut backoff = retry.initial_backoff;
        loop {
            match self.on_xact_end_with_lsn(commit_lsn) {
                Ok(ack) => return Ok(ack),
                Err(e) if is_retryable(&e) && attempt < retry.max_attempts => {
                    self.stats.retries_attempted += 1;
                    attempt += 1;
                    tokio::time::sleep(backoff).await;
                    backoff = backoff.saturating_mul(2).min(retry.max_backoff);
                    self.reconnect().await?;
                }
                Err(e) => return Err(e),
            }
        }
    }
}

/// Classify a failure as transient (worth a reconnect+retry) or fatal.
/// IO, clickhouse-c protocol, and ServerException are all transient at
/// the network/server level — operators tune `retry.max_attempts` to
/// bound the total wall-time. Config / Type / Catalog /
/// UnsupportedValue stay fatal because they encode bugs in the daemon
/// or mapping; retrying would loop forever.
/// Pure core of [`Emitter::idle_ack_ceiling`]: when the emitter is
/// fully drained (no open wire, every encoder empty) trailing WAL is
/// shippable-free so `lsn` is safe; otherwise cap at the durable
/// horizon so an idle nudge can't ack rows still buffered client-side.
fn idle_ceiling(fully_drained: bool, lsn: u64, durable: u64) -> u64 {
    if fully_drained { lsn } else { durable }
}

fn is_retryable(e: &EmitterError) -> bool {
    matches!(
        e,
        EmitterError::Io(_) | EmitterError::Client(_) | EmitterError::ServerException { .. }
    )
}

/// Plug Emitter into the [`TupleObserver`] dispatch path. Owned by
/// `XactRecordSink<EmitterObserver>` in `bin/stream.rs`.
pub struct EmitterObserver {
    pub emitter: Emitter,
}

impl EmitterObserver {
    pub fn new(emitter: Emitter) -> Self {
        Self { emitter }
    }
}

impl TupleObserver for EmitterObserver {
    fn on_tuple<'a>(
        &'a mut self,
        committed: &'a CommittedTuple,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<(), DecoderSinkError>> + Send + 'a>,
    > {
        Box::pin(async move {
            self.emitter
                .route_with_retry(committed)
                .await
                .map_err(DecoderSinkError::from)
        })
    }

    fn on_xact_end<'a>(
        &'a mut self,
        commit_lsn: u64,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<u64, DecoderSinkError>> + Send + 'a>,
    > {
        Box::pin(async move {
            self.emitter
                .on_xact_end_with_retry(commit_lsn)
                .await
                .map_err(DecoderSinkError::from)
        })
    }

    fn on_schema_event<'a>(
        &'a mut self,
        event: &'a SchemaEvent,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<(), DecoderSinkError>> + Send + 'a>,
    > {
        Box::pin(async move {
            self.emitter
                .dispatch_schema_event_with_retry(event)
                .await
                .map_err(DecoderSinkError::from)
        })
    }

    fn idle_ack_ceiling(&self, lsn: u64) -> u64 {
        self.emitter.idle_ack_ceiling(lsn)
    }

    /// Idle wakeup: close any held-open INSERT whose deadline has
    /// elapsed. Hot path when `flush_timeout > 0` and traffic stops —
    /// without this the last burst of rows would stay buffered
    /// client-side past the deadline, since `on_xact_end` only fires
    /// per committed xact. Returns the post-close durable horizon so
    /// the xact buffer advances `emitter_ack_lsn` to it.
    fn on_idle<'a>(
        &'a mut self,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<u64, DecoderSinkError>> + Send + 'a>,
    > {
        Box::pin(async move {
            self.emitter
                .flush_if_deadline_tripped_with_retry()
                .await
                .map_err(DecoderSinkError::from)
        })
    }

    /// Daemon shutdown hook: force-close every held-open INSERT
    /// regardless of deadline. Any rows still in flight when the
    /// daemon stops would otherwise stay buffered client-side and
    /// disappear when the process exits.
    fn on_close<'a>(
        &'a mut self,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<(), DecoderSinkError>> + Send + 'a>,
    > {
        Box::pin(async move {
            self.emitter
                .flush_open_inserts_with_retry()
                .await
                .map_err(DecoderSinkError::from)?;
            Ok(())
        })
    }
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
    fn idle_ceiling_caps_when_buffered() {
        // Fully drained: trailing WAL past the durable horizon is safe.
        assert_eq!(idle_ceiling(true, 500, 300), 500);
        // Rows held (open wire or non-empty encoder): cap at durable.
        assert_eq!(idle_ceiling(false, 500, 300), 300);
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

    /// Reach into chc_type_elem_size for the Phase 7 width matrix
    /// instead of mirroring it in walshadow. Confirms the upstream
    /// elem_size return values match what the encoder needs for
    /// fixed-shape ColumnBufs.
    #[test]
    fn elem_size_covers_phase7_tier1() {
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
        assert_eq!(c.compression, CompressionChoice::Lz4);
        assert_eq!(c.row_budget, 1024);
        assert_eq!(c.byte_budget, 4096);
        let t = c.tables.get("public.foo").expect("mapping present");
        assert_eq!(t.target, "default.foo");
        assert_eq!(t.columns.len(), 2);
        assert_eq!(t.columns[0].src_attnum, 1);
        assert_eq!(t.columns[1].target_type, "Nullable(String)");
    }
}
