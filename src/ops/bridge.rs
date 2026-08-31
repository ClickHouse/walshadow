//! Client for the pgext bridge worker.
//!
//! `walshadow.so` exposes no SQL surface. It registers a background worker
//! through `shared_preload_libraries`, which walshadow writes
//! ([`Shadow::write_base_conf`](crate::catalog::shadow::Shadow::write_base_conf)).
//! Worker serves a unix socket, so it needs no `pg_proc` row on a shadow
//! standby whose catalog is a read-only physical copy of source's.
//!
//! Wire contract lives in `pgext/walshadow.h`

use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use backon::{ExponentialBuilder, Retryable};
use thiserror::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixStream;
use tokio::sync::Mutex;

/// Frame and op layouts. Must equal `WS_PROTO_VERSION` in `pgext/walshadow.h`
pub const PROTO_VERSION: u32 = 2;
/// Catalog column plans. Must equal `WS_PROJECTION_VERSION`
pub const PROJECTION_VERSION: u32 = 1;

/// Match `WS_MAX_REQUEST_BYTES`
pub const MAX_REQUEST_BYTES: usize = 256 * 1024 * 1024;
/// Whole-catalog `pg_type` text output is the largest response in practice
const MAX_RESPONSE_BYTES: usize = 256 * 1024 * 1024;
/// Matches `WS_MAX_SCAN_OIDS`. A longer list is the caller's to chunk, since
/// only the caller knows whether the chunks share a replay position
pub const MAX_SCAN_OIDS: usize = 65536;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum Op {
    Hello = 0x01,
    EncodeNative = 0x02,
    Scan = 0x03,
    ReplayLsn = 0x04,
}

pub const OP_LABELS: [&str; 4] = ["hello", "encode_native", "scan", "replay_lsn"];
pub const OP_COUNT: usize = OP_LABELS.len();

impl Op {
    /// Index into the per-op stat arrays, parallel to [`OP_LABELS`]
    fn slot(self) -> usize {
        self as usize - 1
    }
}

/// Catalogs the overlay scan covers. Ids are wire values; never renumber
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum Catalog {
    Class = 1,
    Attribute = 2,
    Index = 3,
    Namespace = 4,
    Type = 5,
}

impl Catalog {
    /// Wire id back to the catalog, for a row source that carries the id
    /// beside the row rather than in a request it framed itself
    pub fn from_id(id: u8) -> Option<Self> {
        match id {
            1 => Some(Catalog::Class),
            2 => Some(Catalog::Attribute),
            3 => Some(Catalog::Index),
            4 => Some(Catalog::Namespace),
            5 => Some(Catalog::Type),
            _ => None,
        }
    }

    /// Columns the worker projects. Bump [`PROJECTION_VERSION`] on any change
    pub fn ncols(self) -> usize {
        match self {
            Catalog::Class => 9,
            Catalog::Attribute => 12,
            Catalog::Index => 5,
            Catalog::Namespace | Catalog::Type => 2,
        }
    }
}

#[derive(Debug, Error)]
pub enum BridgeError {
    #[error("bridge io: {0}")]
    Io(#[from] io::Error),
    /// Malformed frame, truncated payload, or a worker whose identity changed
    #[error("bridge protocol: {0}")]
    Protocol(String),
    /// Worker answered with status 1
    #[error("bridge worker: {0}")]
    Remote(String),
    /// Refused before reaching the socket, so the connection is still good
    #[error("bridge request of {len} bytes over the {cap} cap")]
    RequestTooLarge { len: usize, cap: usize },
    #[error(
        "bridge speaks proto {proto}/projection {projection}, client wants {want_proto}/{want_projection}"
    )]
    Version {
        proto: u32,
        projection: u32,
        want_proto: u32,
        want_projection: u32,
    },
    /// Replay moved off the boundary the caller parked it at, so the overlay
    /// rows describe a different point in WAL than the caller asked about
    #[error("bridge replayed to {start:X}..{end:X}, expected boundary {expected:X}")]
    ReplayMismatch { expected: u64, start: u64, end: u64 },
}

impl BridgeError {
    /// Return whether socket can no longer carry requests
    pub fn is_transport(&self) -> bool {
        matches!(self, Self::Io(_) | Self::Protocol(_))
    }
}

/// Worker identity, captured by `HELLO` and re-verified on every reconnect
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Hello {
    pub proto: u32,
    pub projection: u32,
    pub pg_version_num: u32,
    pub in_recovery: bool,
}

/// Own response frame backing borrowed Native block
#[derive(Clone, Debug)]
pub struct NativeResponse {
    frame: Vec<u8>,
}

impl NativeResponse {
    pub fn bytes(&self) -> &[u8] {
        &self.frame[1..]
    }
}

#[derive(Clone, Debug)]
pub struct ScanResult {
    /// `GetXLogReplayRecPtr` before the scan
    pub replay_lsn_start: u64,
    /// ... and after. Both equal the parked boundary on a correct read
    pub replay_lsn_end: u64,
    /// Tuples `SnapshotAny` returned, before the visibility predicate
    pub scanned: u32,
    /// Writers whose parentage did not resolve to the requested top xid
    pub subtrans_mismatch: u32,
    pub ncols: usize,
    pub rows: Vec<Vec<Option<String>>>,
}

crate::atomic_stats! {
    pub struct BridgeStats {
        /// 1 while the last transport attempt succeeded
        pub up,
        /// Sockets redialled after a worker exit or transport error
        pub reconnects,
        pub scan_rows,
        pub scan_subtrans_mismatch,
        /// Scans that found replay off the position the read pinned. Committed
        /// reads answer these off SQL instead; overlay reads fail
        pub scan_replay_moved,
        pub native_bytes,
        /// Per-op, indexed by [`OP_LABELS`]
        pub requests: [AtomicU64; OP_COUNT],
        pub errors: [AtomicU64; OP_COUNT],
        pub request_nanos: [AtomicU64; OP_COUNT],
    }
}

impl BridgeStats {
    pub fn summary(&self) -> String {
        use std::fmt::Write as _;
        let ld = |a: &AtomicU64| a.load(Ordering::Relaxed);
        let mut s = String::from(if ld(&self.up) == 1 { "up" } else { "down" });
        for (label, n) in OP_LABELS.iter().zip(&self.requests) {
            let n = ld(n);
            if n > 0 {
                write!(&mut s, " {label}={n}").unwrap();
            }
        }
        let pairs: [(&str, u64); 4] = [
            ("err", self.errors.iter().map(ld).sum()),
            ("reconn", ld(&self.reconnects)),
            ("mismatch", ld(&self.scan_subtrans_mismatch)),
            ("replay_moved", ld(&self.scan_replay_moved)),
        ];
        for (label, n) in pairs {
            if n > 0 {
                write!(&mut s, " {label}={n}").unwrap();
            }
        }
        s
    }
}

#[derive(Debug)]
pub struct Bridge {
    path: PathBuf,
    conn: Mutex<Option<UnixStream>>,
    /// Set by the first successful `HELLO`; later dials must match it
    info: OnceLock<Hello>,
    pub stats: Arc<BridgeStats>,
}

impl Bridge {
    /// Connect and gate on the worker's proto and projection versions. A
    /// mismatch is refused rather than negotiated: the daemon would misparse
    /// the projections
    pub async fn connect(path: impl AsRef<Path>) -> Result<Self, BridgeError> {
        let bridge = Self {
            path: path.as_ref().to_owned(),
            conn: Mutex::new(None),
            info: OnceLock::new(),
            stats: Arc::new(BridgeStats::default()),
        };
        let stream = bridge.dial().await?;
        *bridge.conn.lock().await = Some(stream);
        Ok(bridge)
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// `None` before the first successful `HELLO`, which [`connect`](Self::connect)
    /// guarantees
    pub fn info(&self) -> Option<Hello> {
        self.info.get().copied()
    }

    pub fn is_up(&self) -> bool {
        self.stats.up.load(Ordering::Relaxed) == 1
    }

    /// `pg_last_wal_replay_lsn` by shared-memory read, one round trip
    pub async fn replay_lsn(&self) -> Result<u64, BridgeError> {
        let body = self.call(Op::ReplayLsn, &[]).await?;
        Cursor::at(&body, 1).u64()
    }

    /// Return response remainder as one locally framed Native block
    pub async fn encode_native(&self, payload: &[u8]) -> Result<NativeResponse, BridgeError> {
        let frame = self.call(Op::EncodeNative, payload).await?;
        self.stats
            .native_bytes
            .fetch_add((frame.len() - 1) as u64, Ordering::Relaxed);
        Ok(NativeResponse { frame })
    }

    /// Read `cat` as transaction `top_xid` sees it, or the committed view when
    /// `top_xid` is 0. `oids` scopes `pg_class`, `pg_attribute` and `pg_index`
    /// to relations that transaction holds AccessExclusiveLock on; empty reads
    /// the whole catalog, which is the only mode `pg_namespace` and `pg_type`
    /// have. Losing the oid list loses the lock argument with it, so an
    /// uncommitted whole-catalog read fails rather than guess at a writer whose
    /// parentage standby `pg_subtrans` cannot resolve
    pub async fn scan(
        &self,
        cat: Catalog,
        top_xid: u32,
        oids: &[u32],
    ) -> Result<ScanResult, BridgeError> {
        let mut payload = Vec::with_capacity(9 + oids.len() * 4);
        payload.push(cat as u8);
        payload.extend_from_slice(&top_xid.to_be_bytes());
        payload.extend_from_slice(&(oids.len() as u32).to_be_bytes());
        for oid in oids {
            payload.extend_from_slice(&oid.to_be_bytes());
        }

        let body = self.call(Op::Scan, &payload).await?;
        let mut c = Cursor::at(&body, 1);
        let replay_lsn_start = c.u64()?;
        let replay_lsn_end = c.u64()?;
        let scanned = c.u32()?;
        let subtrans_mismatch = c.u32()?;
        let nrows = c.u32()? as usize;
        let ncols = c.u16()? as usize;
        if ncols != cat.ncols() {
            return Err(BridgeError::Protocol(format!(
                "{cat:?} projected {ncols} columns, client expects {}",
                cat.ncols()
            )));
        }
        // Every value costs at least its 4-byte length prefix, so a row count
        // the rest of the frame cannot hold is a desync, not an allocation
        if nrows.saturating_mul(ncols).saturating_mul(4) > body.len() {
            return Err(BridgeError::Protocol(format!(
                "{nrows} rows do not fit a {}-byte frame",
                body.len()
            )));
        }
        let mut rows = Vec::with_capacity(nrows);
        for _ in 0..nrows {
            let mut row = Vec::with_capacity(ncols);
            for _ in 0..ncols {
                row.push(c.opt_str()?);
            }
            rows.push(row);
        }

        self.stats
            .scan_rows
            .fetch_add(nrows as u64, Ordering::Relaxed);
        self.stats
            .scan_subtrans_mismatch
            .fetch_add(u64::from(subtrans_mismatch), Ordering::Relaxed);
        Ok(ScanResult {
            replay_lsn_start,
            replay_lsn_end,
            scanned,
            subtrans_mismatch,
            ncols,
            rows,
        })
    }

    /// [`scan`](Self::scan) plus the assertion that replay never left
    /// `boundary`. Equal but wrong is unreachable: replay cannot rewind and the
    /// daemon holds the successor bytes
    pub async fn scan_at(
        &self,
        cat: Catalog,
        top_xid: u32,
        oids: &[u32],
        boundary: u64,
    ) -> Result<ScanResult, BridgeError> {
        let res = self.scan(cat, top_xid, oids).await?;
        self.pinned(res, boundary)
    }

    /// First scan of a read with no boundary of its own: whatever position it
    /// reports becomes the pin for the rest, so only a move inside this one
    /// scan fails here
    pub async fn scan_pinning(
        &self,
        cat: Catalog,
        top_xid: u32,
        oids: &[u32],
    ) -> Result<ScanResult, BridgeError> {
        let res = self.scan(cat, top_xid, oids).await?;
        let boundary = res.replay_lsn_start;
        self.pinned(res, boundary)
    }

    fn pinned(&self, res: ScanResult, boundary: u64) -> Result<ScanResult, BridgeError> {
        if res.replay_lsn_start != boundary || res.replay_lsn_end != boundary {
            self.stats.scan_replay_moved.fetch_add(1, Ordering::Relaxed);
            return Err(BridgeError::ReplayMismatch {
                expected: boundary,
                start: res.replay_lsn_start,
                end: res.replay_lsn_end,
            });
        }
        Ok(res)
    }

    /// Fresh socket plus `HELLO`. Takes no connection lock, so
    /// [`call`](Self::call) may hold one across it
    async fn dial(&self) -> Result<UnixStream, BridgeError> {
        let mut stream = UnixStream::connect(&self.path).await?;
        let started = Instant::now();
        let res = round_trip(&mut stream, Op::Hello, &[]).await;
        self.record(Op::Hello, started, &res);
        let body = res?;

        let mut c = Cursor::at(&body, 1);
        let info = Hello {
            proto: c.u32()?,
            projection: c.u32()?,
            pg_version_num: c.u32()?,
            in_recovery: c.u8()? != 0,
        };
        if info.proto != PROTO_VERSION || info.projection != PROJECTION_VERSION {
            return Err(BridgeError::Version {
                proto: info.proto,
                projection: info.projection,
                want_proto: PROTO_VERSION,
                want_projection: PROJECTION_VERSION,
            });
        }
        // A worker that came back a different build must not be trusted to
        // answer requests the daemon framed against the old one
        let first = *self.info.get_or_init(|| info);
        if first != info {
            return Err(BridgeError::Protocol(format!(
                "worker identity changed across reconnect: {first:?} then {info:?}"
            )));
        }
        Ok(stream)
    }

    async fn call(&self, op: Op, payload: &[u8]) -> Result<Vec<u8>, BridgeError> {
        let started = Instant::now();
        // Refuse before the socket sees it: the worker answers a frame this
        // size by closing, and a healthy connection must not pay for that
        let len = payload.len() + 1;
        if len > MAX_REQUEST_BYTES {
            let res = Err(BridgeError::RequestTooLarge {
                len,
                cap: MAX_REQUEST_BYTES,
            });
            self.record(op, started, &res);
            return res;
        }
        let mut guard = self.conn.lock().await;
        let mut res = match guard.as_mut() {
            Some(stream) => round_trip(stream, op, payload).await,
            None => Err(BridgeError::Io(io::Error::new(
                io::ErrorKind::NotConnected,
                "bridge disconnected",
            ))),
        };
        // A worker exit drops the socket and `bgw_restart_time` brings it back.
        // Every op is read-only, so replaying one costs nothing
        if is_transport_error(&res) {
            *guard = None;
            self.stats.reconnects.fetch_add(1, Ordering::Relaxed);
            match self.dial().await {
                Ok(mut stream) => {
                    res = round_trip(&mut stream, op, payload).await;
                    if !is_transport_error(&res) {
                        *guard = Some(stream);
                    }
                }
                Err(e) => res = Err(e),
            }
        }
        drop(guard);
        self.record(op, started, &res);
        res
    }

    fn record(&self, op: Op, started: Instant, res: &Result<Vec<u8>, BridgeError>) {
        let slot = op.slot();
        self.stats.requests[slot].fetch_add(1, Ordering::Relaxed);
        self.stats.request_nanos[slot]
            .fetch_add(started.elapsed().as_nanos() as u64, Ordering::Relaxed);
        if res.is_err() {
            self.stats.errors[slot].fetch_add(1, Ordering::Relaxed);
        }
        // A worker that answered with an error status is still up. A frame
        // this side refused never reached it, so it says nothing either way
        if !matches!(res, Err(BridgeError::RequestTooLarge { .. })) {
            self.stats
                .up
                .store(u64::from(!is_transport_error(res)), Ordering::Relaxed);
        }
    }
}

fn is_transport_error(res: &Result<Vec<u8>, BridgeError>) -> bool {
    matches!(res, Err(e) if e.is_transport())
}

async fn round_trip(
    stream: &mut UnixStream,
    op: Op,
    payload: &[u8],
) -> Result<Vec<u8>, BridgeError> {
    let len = payload.len() + 1;
    // One write: a peer that dribbles a partial frame is what the worker's
    // io_timeout_ms exists to bound, and the daemon must not be that peer
    let mut frame = Vec::with_capacity(4 + len);
    frame.extend_from_slice(&(len as u32).to_be_bytes());
    frame.push(op as u8);
    frame.extend_from_slice(payload);
    stream.write_all(&frame).await?;
    stream.flush().await?;

    let mut hdr = [0u8; 4];
    stream.read_exact(&mut hdr).await?;
    let len = u32::from_be_bytes(hdr) as usize;
    if len == 0 || len > MAX_RESPONSE_BYTES {
        return Err(BridgeError::Protocol(format!(
            "response frame of {len} bytes"
        )));
    }
    let mut body = vec![0u8; len];
    stream.read_exact(&mut body).await?;

    match body[0] {
        0 => Ok(body),
        1 => Err(BridgeError::Remote(Cursor::at(&body, 1).lenstr()?)),
        s => Err(BridgeError::Protocol(format!("response status {s}"))),
    }
}

/// Connect with a wall-clock budget while shadow reaches consistency.
/// Matches catalog's
/// [`with_transient_retry`](crate::catalog::shadow_catalog::with_transient_retry) shape
pub async fn connect_with_budget(path: &Path, budget: Duration) -> Result<Bridge, BridgeError> {
    let deadline = tokio::time::Instant::now() + budget;
    (|| Bridge::connect(path))
        .retry(
            ExponentialBuilder::default()
                .with_min_delay(Duration::from_millis(100))
                .with_max_delay(Duration::from_secs(1))
                .without_max_times(),
        )
        // Version skew will not resolve by waiting
        .when(move |e: &BridgeError| {
            !matches!(e, BridgeError::Version { .. }) && tokio::time::Instant::now() < deadline
        })
        .await
}

// ----- projections ---------------------------------------------------------

/// One row of a catalog projection. Column order must match `pgext/overlay.c`
pub trait ScanRow: Sized {
    const CATALOG: Catalog;
    fn parse(row: &[Option<String>]) -> Result<Self, BridgeError>;
}

impl ScanResult {
    pub fn parse<T: ScanRow>(&self) -> Result<Vec<T>, BridgeError> {
        if self.ncols != T::CATALOG.ncols() {
            return Err(BridgeError::Protocol(format!(
                "{:?} rows have {} columns",
                T::CATALOG,
                self.ncols
            )));
        }
        self.rows.iter().map(|r| T::parse(r)).collect()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClassRow {
    pub oid: u32,
    pub relnamespace: u32,
    pub relname: String,
    pub relkind: char,
    pub relpersistence: char,
    pub relreplident: char,
    pub reltoastrelid: u32,
    /// `0` means database default, which the daemon resolves against
    /// `pg_database` as its SQL already does
    pub reltablespace: u32,
    /// The column, not `pg_relation_filenode()`: that goes through relcache and
    /// would not see the overlay. User relations are never mapped
    pub relfilenode: u32,
}

impl ScanRow for ClassRow {
    const CATALOG: Catalog = Catalog::Class;

    fn parse(row: &[Option<String>]) -> Result<Self, BridgeError> {
        Ok(Self {
            oid: field(row, 0)?.parse().map_err(|_| bad(row, 0))?,
            relnamespace: field(row, 1)?.parse().map_err(|_| bad(row, 1))?,
            relname: field(row, 2)?.to_owned(),
            relkind: only_char(row, 3)?,
            relpersistence: only_char(row, 4)?,
            relreplident: only_char(row, 5)?,
            reltoastrelid: field(row, 6)?.parse().map_err(|_| bad(row, 6))?,
            reltablespace: field(row, 7)?.parse().map_err(|_| bad(row, 7))?,
            relfilenode: field(row, 8)?.parse().map_err(|_| bad(row, 8))?,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AttributeRow {
    pub attrelid: u32,
    pub attnum: i16,
    pub attname: String,
    pub atttypid: u32,
    pub atttypmod: i32,
    pub attnotnull: bool,
    pub attisdropped: bool,
    pub attbyval: bool,
    pub attlen: i16,
    pub attalign: char,
    pub attstorage: char,
    /// `anyarray_out` form, `None` unless `atthasmissing`
    pub attmissingval: Option<String>,
}

impl ScanRow for AttributeRow {
    const CATALOG: Catalog = Catalog::Attribute;

    fn parse(row: &[Option<String>]) -> Result<Self, BridgeError> {
        Ok(Self {
            attrelid: field(row, 0)?.parse().map_err(|_| bad(row, 0))?,
            attnum: field(row, 1)?.parse().map_err(|_| bad(row, 1))?,
            attname: field(row, 2)?.to_owned(),
            atttypid: field(row, 3)?.parse().map_err(|_| bad(row, 3))?,
            atttypmod: field(row, 4)?.parse().map_err(|_| bad(row, 4))?,
            attnotnull: pg_bool(row, 5)?,
            attisdropped: pg_bool(row, 6)?,
            attbyval: pg_bool(row, 7)?,
            attlen: field(row, 8)?.parse().map_err(|_| bad(row, 8))?,
            attalign: only_char(row, 9)?,
            attstorage: only_char(row, 10)?,
            attmissingval: row.get(11).and_then(|v| v.clone()),
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IndexRow {
    pub indexrelid: u32,
    pub indrelid: u32,
    pub indisprimary: bool,
    pub indisreplident: bool,
    /// `int2vectorout` form parsed out: attnums in index order, `0` for an
    /// expression column
    pub indkey: Vec<i16>,
}

impl ScanRow for IndexRow {
    const CATALOG: Catalog = Catalog::Index;

    fn parse(row: &[Option<String>]) -> Result<Self, BridgeError> {
        let indkey = field(row, 4)?
            .split_whitespace()
            .map(|t| t.parse::<i16>().map_err(|_| bad(row, 4)))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            indexrelid: field(row, 0)?.parse().map_err(|_| bad(row, 0))?,
            indrelid: field(row, 1)?.parse().map_err(|_| bad(row, 1))?,
            indisprimary: pg_bool(row, 2)?,
            indisreplident: pg_bool(row, 3)?,
            indkey,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NamespaceRow {
    pub oid: u32,
    pub nspname: String,
}

impl ScanRow for NamespaceRow {
    const CATALOG: Catalog = Catalog::Namespace;

    fn parse(row: &[Option<String>]) -> Result<Self, BridgeError> {
        Ok(Self {
            oid: field(row, 0)?.parse().map_err(|_| bad(row, 0))?,
            nspname: field(row, 1)?.to_owned(),
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TypeRow {
    pub oid: u32,
    pub typname: String,
}

impl ScanRow for TypeRow {
    const CATALOG: Catalog = Catalog::Type;

    fn parse(row: &[Option<String>]) -> Result<Self, BridgeError> {
        Ok(Self {
            oid: field(row, 0)?.parse().map_err(|_| bad(row, 0))?,
            typname: field(row, 1)?.to_owned(),
        })
    }
}

fn field(row: &[Option<String>], i: usize) -> Result<&str, BridgeError> {
    match row.get(i) {
        Some(Some(v)) => Ok(v),
        Some(None) => Err(BridgeError::Protocol(format!("column {i} is null"))),
        None => Err(BridgeError::Protocol(format!("column {i} missing"))),
    }
}

fn bad(row: &[Option<String>], i: usize) -> BridgeError {
    let got = row.get(i).and_then(|v| v.as_deref()).unwrap_or("");
    BridgeError::Protocol(format!("column {i} unparsable: {got:?}"))
}

/// `boolout` renders `t` / `f`
fn pg_bool(row: &[Option<String>], i: usize) -> Result<bool, BridgeError> {
    match field(row, i)? {
        "t" => Ok(true),
        "f" => Ok(false),
        _ => Err(bad(row, i)),
    }
}

/// `charout` on a PG `"char"` column
fn only_char(row: &[Option<String>], i: usize) -> Result<char, BridgeError> {
    let mut cs = field(row, i)?.chars();
    match (cs.next(), cs.next()) {
        (Some(c), None) => Ok(c),
        _ => Err(bad(row, i)),
    }
}

// ----- framing -------------------------------------------------------------

struct Cursor<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn at(buf: &'a [u8], pos: usize) -> Self {
        Self { buf, pos }
    }

    fn take(&mut self, n: usize) -> Result<&'a [u8], BridgeError> {
        let end = self.pos.checked_add(n).ok_or_else(|| self.short(n))?;
        let out = self.buf.get(self.pos..end).ok_or_else(|| self.short(n))?;
        self.pos = end;
        Ok(out)
    }

    fn short(&self, n: usize) -> BridgeError {
        BridgeError::Protocol(format!(
            "want {n} bytes at offset {}, frame is {}",
            self.pos,
            self.buf.len()
        ))
    }

    fn u8(&mut self) -> Result<u8, BridgeError> {
        Ok(self.take(1)?[0])
    }

    fn u16(&mut self) -> Result<u16, BridgeError> {
        let b: [u8; 2] = self.take(2)?.try_into().expect("take yields 2");
        Ok(u16::from_be_bytes(b))
    }

    fn u32(&mut self) -> Result<u32, BridgeError> {
        let b: [u8; 4] = self.take(4)?.try_into().expect("take yields 4");
        Ok(u32::from_be_bytes(b))
    }

    fn u64(&mut self) -> Result<u64, BridgeError> {
        let b: [u8; 8] = self.take(8)?.try_into().expect("take yields 8");
        Ok(u64::from_be_bytes(b))
    }

    fn lenstr(&mut self) -> Result<String, BridgeError> {
        let n = self.u32()? as usize;
        text(self.take(n)?)
    }

    /// Column value: `i32` length, `-1` null
    fn opt_str(&mut self) -> Result<Option<String>, BridgeError> {
        let n = self.u32()? as i32;
        if n < 0 {
            return Ok(None);
        }
        Ok(Some(text(self.take(n as usize)?)?))
    }
}

/// Shadow is always initdb'd UTF8, and the SQL path constrains the same way
fn text(b: &[u8]) -> Result<String, BridgeError> {
    String::from_utf8(b.to_vec()).map_err(|_| BridgeError::Protocol("non-UTF8 payload".to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::UnixListener;

    #[test]
    fn cursor_rejects_truncation() {
        let buf = [0u8, 0, 0, 4, 1, 2];
        let mut c = Cursor::at(&buf, 0);
        assert_eq!(c.u32().unwrap(), 4);
        assert!(matches!(c.take(4), Err(BridgeError::Protocol(_))));
    }

    #[test]
    fn cursor_reads_null_column() {
        let mut buf = (-1i32).to_be_bytes().to_vec();
        buf.extend_from_slice(&2u32.to_be_bytes());
        buf.extend_from_slice(b"hi");
        let mut c = Cursor::at(&buf, 0);
        assert_eq!(c.opt_str().unwrap(), None);
        assert_eq!(c.opt_str().unwrap().as_deref(), Some("hi"));
    }

    #[test]
    fn class_row_parses_projection_order() {
        let row: Vec<Option<String>> = ["16384", "2200", "t", "r", "p", "d", "16387", "0", "16384"]
            .iter()
            .map(|s| Some((*s).to_string()))
            .collect();
        let parsed = ClassRow::parse(&row).unwrap();
        assert_eq!(parsed.oid, 16384);
        assert_eq!(parsed.relname, "t");
        assert_eq!(parsed.relkind, 'r');
        assert_eq!(parsed.relreplident, 'd');
        assert_eq!(parsed.reltablespace, 0);
    }

    #[test]
    fn attribute_row_keeps_missingval_null() {
        let mut row: Vec<Option<String>> =
            ["16384", "1", "id", "23", "-1", "t", "f", "t", "4", "i", "p"]
                .iter()
                .map(|s| Some((*s).to_string()))
                .collect();
        row.push(None);
        let parsed = AttributeRow::parse(&row).unwrap();
        assert_eq!(parsed.attnum, 1);
        assert!(parsed.attnotnull && parsed.attbyval && !parsed.attisdropped);
        assert_eq!(parsed.attalign, 'i');
        assert_eq!(parsed.attmissingval, None);
    }

    #[test]
    fn index_row_parses_int2vector_form() {
        let row: Vec<Option<String>> = ["16390", "16384", "t", "f", "1 3"]
            .iter()
            .map(|s| Some((*s).to_string()))
            .collect();
        let parsed = IndexRow::parse(&row).unwrap();
        assert_eq!(parsed.indkey, [1, 3]);
        assert!(parsed.indisprimary && !parsed.indisreplident);
    }

    #[test]
    fn catalog_ids_round_trip() {
        for cat in [
            Catalog::Class,
            Catalog::Attribute,
            Catalog::Index,
            Catalog::Namespace,
            Catalog::Type,
        ] {
            assert_eq!(Catalog::from_id(cat as u8), Some(cat));
        }
        assert_eq!(Catalog::from_id(0), None);
        assert_eq!(Catalog::from_id(6), None);
    }

    #[test]
    fn scan_result_refuses_wrong_projection_width() {
        let res = ScanResult {
            replay_lsn_start: 0,
            replay_lsn_end: 0,
            scanned: 0,
            subtrans_mismatch: 0,
            ncols: 2,
            rows: vec![],
        };
        assert!(res.parse::<ClassRow>().is_err());
    }

    #[test]
    fn stats_summary_skips_zero_buckets() {
        let s = BridgeStats::default();
        s.up.store(1, Ordering::Relaxed);
        s.requests[Op::Scan.slot()].store(3, Ordering::Relaxed);
        s.reconnects.store(1, Ordering::Relaxed);
        let out = s.summary();
        assert!(out.starts_with("up"));
        assert!(out.contains("scan=3"));
        assert!(out.contains("reconn=1"));
        assert!(!out.contains("decode="));
    }

    /// Frames a canned response body, matching the worker's `u32 len | u8
    /// status | payload`
    fn frame(body: Vec<u8>) -> Vec<u8> {
        let mut out = (body.len() as u32).to_be_bytes().to_vec();
        out.extend_from_slice(&body);
        out
    }

    fn hello_body(proto: u32, projection: u32) -> Vec<u8> {
        let mut b = vec![0u8];
        b.extend_from_slice(&proto.to_be_bytes());
        b.extend_from_slice(&projection.to_be_bytes());
        b.extend_from_slice(&170004u32.to_be_bytes());
        b.push(1);
        b
    }

    /// Reads one request frame, writes the next canned response. `None` closes
    /// the connection instead, standing in for a worker exit
    async fn fake_worker(listener: UnixListener, script: Vec<Option<Vec<u8>>>) {
        let mut script = script.into_iter();
        loop {
            let Ok((mut sock, _)) = listener.accept().await else {
                return;
            };
            loop {
                let mut hdr = [0u8; 4];
                if sock.read_exact(&mut hdr).await.is_err() {
                    break;
                }
                let mut body = vec![0u8; u32::from_be_bytes(hdr) as usize];
                if sock.read_exact(&mut body).await.is_err() {
                    break;
                }
                match script.next() {
                    Some(Some(resp)) => {
                        if sock.write_all(&frame(resp)).await.is_err() {
                            break;
                        }
                    }
                    Some(None) | None => break,
                }
            }
        }
    }

    fn spawn_worker(script: Vec<Option<Vec<u8>>>) -> (tempfile::TempDir, PathBuf) {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("bridge.sock");
        let listener = UnixListener::bind(&path).unwrap();
        tokio::spawn(fake_worker(listener, script));
        (tmp, path)
    }

    #[tokio::test(flavor = "current_thread")]
    async fn connect_refuses_projection_skew() {
        let (_tmp, path) = spawn_worker(vec![Some(hello_body(PROTO_VERSION, 99))]);
        let err = Bridge::connect(&path).await.unwrap_err();
        assert!(
            matches!(err, BridgeError::Version { projection: 99, .. }),
            "got {err:?}"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn encode_native_returns_the_frame_past_the_status() {
        let native = b"native-block-bytes".to_vec();
        let mut body = vec![0u8];
        body.extend_from_slice(&native);
        let (_tmp, path) = spawn_worker(vec![
            Some(hello_body(PROTO_VERSION, PROJECTION_VERSION)),
            Some(body),
        ]);

        let bridge = Bridge::connect(&path).await.unwrap();
        let out = bridge.encode_native(&[0u8; 8]).await.unwrap();
        assert_eq!(out.bytes(), &native[..]);
        assert_eq!(
            bridge.stats.native_bytes.load(Ordering::Relaxed),
            native.len() as u64
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn scan_at_rejects_moved_replay() {
        let mut body = vec![0u8];
        body.extend_from_slice(&0x1000u64.to_be_bytes());
        body.extend_from_slice(&0x2000u64.to_be_bytes());
        body.extend_from_slice(&0u32.to_be_bytes());
        body.extend_from_slice(&0u32.to_be_bytes());
        body.extend_from_slice(&0u32.to_be_bytes());
        body.extend_from_slice(&2u16.to_be_bytes());
        let (_tmp, path) = spawn_worker(vec![
            Some(hello_body(PROTO_VERSION, PROJECTION_VERSION)),
            Some(body),
        ]);

        let bridge = Bridge::connect(&path).await.unwrap();
        let err = bridge
            .scan_at(Catalog::Namespace, 700, &[], 0x1000)
            .await
            .unwrap_err();
        assert!(
            matches!(err, BridgeError::ReplayMismatch { end: 0x2000, .. }),
            "got {err:?}"
        );
    }

    /// Header shape only; `scan_pinning` never reaches the row bytes here
    fn scan_body(start: u64, end: u64) -> Vec<u8> {
        let mut b = vec![0u8];
        b.extend_from_slice(&start.to_be_bytes());
        b.extend_from_slice(&end.to_be_bytes());
        b.extend_from_slice(&0u32.to_be_bytes());
        b.extend_from_slice(&0u32.to_be_bytes());
        b.extend_from_slice(&0u32.to_be_bytes());
        b.extend_from_slice(&2u16.to_be_bytes());
        b
    }

    #[tokio::test(flavor = "current_thread")]
    async fn scan_pinning_takes_the_position_it_finds() {
        let (_tmp, path) = spawn_worker(vec![
            Some(hello_body(PROTO_VERSION, PROJECTION_VERSION)),
            Some(scan_body(0x4000, 0x4000)),
        ]);

        let bridge = Bridge::connect(&path).await.unwrap();
        let res = bridge
            .scan_pinning(Catalog::Namespace, 0, &[])
            .await
            .expect("start == end pins");
        assert_eq!(res.replay_lsn_end, 0x4000);
        assert_eq!(bridge.stats.scan_replay_moved.load(Ordering::Relaxed), 0);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn scan_pinning_rejects_a_move_inside_one_scan() {
        let (_tmp, path) = spawn_worker(vec![
            Some(hello_body(PROTO_VERSION, PROJECTION_VERSION)),
            Some(scan_body(0x4000, 0x5000)),
        ]);

        let bridge = Bridge::connect(&path).await.unwrap();
        let err = bridge
            .scan_pinning(Catalog::Namespace, 0, &[])
            .await
            .unwrap_err();
        assert!(
            matches!(
                err,
                BridgeError::ReplayMismatch {
                    expected: 0x4000,
                    end: 0x5000,
                    ..
                }
            ),
            "got {err:?}"
        );
        assert_eq!(bridge.stats.scan_replay_moved.load(Ordering::Relaxed), 1);
    }

    /// A frame this side refuses never reaches the worker, so it must not read
    /// as a dead socket and cost a redial
    #[tokio::test(flavor = "current_thread")]
    async fn oversize_request_keeps_the_connection() {
        let (_tmp, path) = spawn_worker(vec![Some(hello_body(PROTO_VERSION, PROJECTION_VERSION))]);

        let bridge = Bridge::connect(&path).await.unwrap();
        let huge = vec![0u8; MAX_REQUEST_BYTES];
        let err = bridge.encode_native(&huge).await.unwrap_err();
        assert!(
            matches!(err, BridgeError::RequestTooLarge { .. }),
            "got {err:?}"
        );
        assert!(bridge.is_up());
        assert_eq!(bridge.stats.reconnects.load(Ordering::Relaxed), 0);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn remote_error_status_keeps_bridge_up() {
        let mut body = vec![1u8];
        body.extend_from_slice(&5u32.to_be_bytes());
        body.extend_from_slice(b"nope!");
        let (_tmp, path) = spawn_worker(vec![
            Some(hello_body(PROTO_VERSION, PROJECTION_VERSION)),
            Some(body),
        ]);

        let bridge = Bridge::connect(&path).await.unwrap();
        let err = bridge.replay_lsn().await.unwrap_err();
        assert!(matches!(err, BridgeError::Remote(m) if m == "nope!"));
        assert!(bridge.is_up());
        assert_eq!(bridge.stats.reconnects.load(Ordering::Relaxed), 0);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn dropped_connection_reconnects_and_retries() {
        let mut lsn = vec![0u8];
        lsn.extend_from_slice(&0xdeadu64.to_be_bytes());
        let (_tmp, path) = spawn_worker(vec![
            Some(hello_body(PROTO_VERSION, PROJECTION_VERSION)),
            // worker exits mid-request
            None,
            Some(hello_body(PROTO_VERSION, PROJECTION_VERSION)),
            Some(lsn),
        ]);

        let bridge = Bridge::connect(&path).await.unwrap();
        assert_eq!(bridge.replay_lsn().await.unwrap(), 0xdead);
        assert_eq!(bridge.stats.reconnects.load(Ordering::Relaxed), 1);
        assert!(bridge.is_up());
    }
}
