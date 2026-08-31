//! Encode sealed-batch columns through shadow PG

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use clickhouse_c::{Allocator, Block, BlockOpts, BlockReader, Column, SliceIo};

use crate::decode::heap_decoder::ColumnValue;
use crate::ops::bridge::{Bridge, BridgeError, MAX_REQUEST_BYTES};
use crate::schema::RelAttr;

/// Cell tags, matching `WS_CELL_*` in `pgext/walshadow.h`
const CELL_DEFAULT: u8 = 0x00;
const CELL_DISK_RAW: u8 = 0x01;
const CELL_TEXT: u8 = 0x02;
const CELL_LITERAL: u8 = 0x03;

crate::atomic_stats! {
    pub struct OracleStats {
        pub blocks,
        pub rows,
        pub cells,
        pub conversion_errors,
        pub errors,
    }
}

#[derive(Debug, thiserror::Error)]
pub enum OracleError {
    #[error("oracle bridge: {0}")]
    Bridge(#[from] BridgeError),
    #[error("oracle absent: {0}")]
    Absent(String),
    #[error("oracle response: {0}")]
    Response(String),
}

impl OracleError {
    /// Retry only unusable sockets
    pub fn retryable(&self) -> bool {
        matches!(self, Self::Bridge(e) if e.is_transport())
    }
}

/// Cell tags must match `WS_CELL_*`
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OracleCell {
    /// SQL NULL, absent field, or unlogged tombstone field
    Default,
    /// On-disk Datum body, varlena header stripped
    DiskRaw(Vec<u8>),
    /// Attribute-default input text
    TextInput(Vec<u8>),
    /// Pre-rendered text bypassing source type
    Literal(Vec<u8>),
}

impl OracleCell {
    /// Tag plus optional length-prefixed body
    pub fn wire_bytes(&self) -> usize {
        match self {
            Self::Default => 1,
            Self::DiskRaw(b) | Self::TextInput(b) | Self::Literal(b) => 5 + b.len(),
        }
    }
}

#[derive(Debug)]
pub struct OracleColumnBuf {
    pub source_type_oid: u32,
    pub source_typmod: i32,
    cells: Vec<OracleCell>,
    wire_bytes: usize,
}

impl OracleColumnBuf {
    pub fn new(source_type_oid: u32, source_typmod: i32) -> Self {
        Self {
            source_type_oid,
            source_typmod,
            cells: Vec::new(),
            wire_bytes: 0,
        }
    }

    pub fn push(&mut self, cell: OracleCell) {
        self.wire_bytes += cell.wire_bytes();
        self.cells.push(cell);
    }

    pub fn cells(&self) -> &[OracleCell] {
        &self.cells
    }

    pub fn approx_size(&self) -> usize {
        self.wire_bytes
    }
}

pub struct OracleRequestColumn<'a> {
    /// Position in destination batch
    pub ordinal: u32,
    pub name: &'a str,
    /// Canonical CH type expected in response
    pub target_type: &'a str,
    pub buf: &'a OracleColumnBuf,
}

/// Own block backing columns borrowed across ClickHouse retries
pub struct OracleBlock {
    block: Block,
    ordinals: Vec<u32>,
}

impl std::fmt::Debug for OracleBlock {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OracleBlock")
            .field("rows", &self.block.n_rows())
            .field("ordinals", &self.ordinals)
            .finish()
    }
}

impl OracleBlock {
    pub fn column(&self, ordinal: u32) -> Option<Column<'_>> {
        let i = self.ordinals.iter().position(|&o| o == ordinal)?;
        self.block.column(i)
    }
}

pub struct Oracle {
    bridge: Arc<Bridge>,
    pub stats: Arc<OracleStats>,
}

impl Oracle {
    pub fn new(bridge: Arc<Bridge>) -> Self {
        Self {
            bridge,
            stats: Arc::new(OracleStats::default()),
        }
    }

    pub async fn encode_batch(
        &self,
        columns: &[OracleRequestColumn<'_>],
        n_rows: usize,
        alloc: Allocator,
    ) -> Result<OracleBlock, OracleError> {
        // Cells are positional, require one per row
        for c in columns {
            if c.buf.cells().len() != n_rows {
                self.stats.errors.fetch_add(1, Ordering::Relaxed);
                return Err(OracleError::Response(format!(
                    "column {:?} holds {} cells for a {n_rows}-row batch",
                    c.name,
                    c.buf.cells().len()
                )));
            }
        }
        let payload = encode_request(columns, n_rows);
        let response = match self.bridge.encode_native(&payload).await {
            Ok(r) => r,
            Err(e) => {
                if matches!(e, BridgeError::Remote(_)) {
                    self.stats.conversion_errors.fetch_add(1, Ordering::Relaxed);
                }
                self.stats.errors.fetch_add(1, Ordering::Relaxed);
                return Err(e.into());
            }
        };
        let native = response.bytes();
        let block = match decode_block(native, columns, n_rows, alloc) {
            Ok(b) => b,
            Err(e) => {
                self.stats.errors.fetch_add(1, Ordering::Relaxed);
                return Err(e);
            }
        };
        let cells = (columns.len() * n_rows) as u64;
        self.stats.blocks.fetch_add(1, Ordering::Relaxed);
        self.stats.rows.fetch_add(n_rows as u64, Ordering::Relaxed);
        self.stats.cells.fetch_add(cells, Ordering::Relaxed);
        Ok(OracleBlock {
            block,
            ordinals: columns.iter().map(|c| c.ordinal).collect(),
        })
    }
}

/// Fixed wire cost before column cells
pub fn request_column_bytes(name: &str, target_type: &str) -> usize {
    4 + 4 + (4 + name.len()) + (4 + target_type.len())
}

/// Opcode, row count, and column count
pub const REQUEST_FRAME_BYTES: usize = 1 + 4 + 4;

/// Soft batch threshold, `MAX_REQUEST_BYTES` remains hard frame cap
pub const ORACLE_BATCH_SEAL_BYTES: usize = 32 << 20;

const _: () = assert!(ORACLE_BATCH_SEAL_BYTES <= MAX_REQUEST_BYTES);

/// Encode all column metadata before row-major cells
fn encode_request(columns: &[OracleRequestColumn<'_>], n_rows: usize) -> Vec<u8> {
    let size: usize = columns
        .iter()
        .map(|c| request_column_bytes(c.name, c.target_type) + c.buf.approx_size())
        .sum();
    let mut out = Vec::with_capacity(8 + size);
    out.extend_from_slice(&(n_rows as u32).to_be_bytes());
    out.extend_from_slice(&(columns.len() as u32).to_be_bytes());
    for c in columns {
        out.extend_from_slice(&c.buf.source_type_oid.to_be_bytes());
        out.extend_from_slice(&c.buf.source_typmod.to_be_bytes());
        put_lenstr(&mut out, c.name.as_bytes());
        put_lenstr(&mut out, c.target_type.as_bytes());
    }
    for row in 0..n_rows {
        for c in columns {
            let cell = &c.buf.cells()[row];
            match cell {
                OracleCell::Default => out.push(CELL_DEFAULT),
                OracleCell::DiskRaw(b) => {
                    out.push(CELL_DISK_RAW);
                    put_lenstr(&mut out, b);
                }
                OracleCell::TextInput(b) => {
                    out.push(CELL_TEXT);
                    put_lenstr(&mut out, b);
                }
                OracleCell::Literal(b) => {
                    out.push(CELL_LITERAL);
                    put_lenstr(&mut out, b);
                }
            }
        }
    }
    out
}

fn put_lenstr(out: &mut Vec<u8>, bytes: &[u8]) {
    out.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
    out.extend_from_slice(bytes);
}

/// Validate exactly one block against requested shape
fn decode_block(
    native: &[u8],
    columns: &[OracleRequestColumn<'_>],
    n_rows: usize,
    alloc: Allocator,
) -> Result<Block, OracleError> {
    let bad = |m: String| OracleError::Response(m);
    let mut io = SliceIo::new(native);
    let mut reader = BlockReader::new(io.as_mut(), alloc, BlockOpts::default())
        .map_err(|e| bad(format!("reader: {e}")))?;
    let block = reader
        .read()
        .map_err(|e| bad(format!("block read: {e}")))?
        .ok_or_else(|| bad("empty Native response".into()))?;
    if reader
        .read()
        .map_err(|e| bad(format!("trailing read: {e}")))?
        .is_some()
    {
        return Err(bad("more than one Native block".into()));
    }
    drop(reader);
    block
        .validate()
        .map_err(|e| bad(format!("block validate: {e}")))?;
    if block.n_rows() != n_rows || block.n_columns() != columns.len() {
        return Err(bad(format!(
            "block is {}x{}, requested {}x{}",
            block.n_columns(),
            block.n_rows(),
            columns.len(),
            n_rows
        )));
    }
    for (i, want) in columns.iter().enumerate() {
        if block.column_name(i) != Some(want.name.as_bytes()) {
            return Err(bad(format!(
                "column {i} is {:?}, requested {:?}",
                block.column_name(i).map(String::from_utf8_lossy),
                want.name
            )));
        }
        // Canonical type bytes must match across pinned libraries
        let got = block.column_type(i).and_then(|t| t.name());
        if got != Some(want.target_type.as_bytes()) {
            return Err(bad(format!(
                "column {:?} is {:?}, requested {:?}",
                want.name,
                got.map(String::from_utf8_lossy),
                want.target_type
            )));
        }
    }
    Ok(block)
}

/// Render PostGIS 2-D points as WKT, typoutput yields HEXEWKB
pub fn render_ext_columns(attrs: &[RelAttr], columns: &mut [Option<ColumnValue>]) {
    for att in attrs {
        if att.dropped || !matches!(att.type_name.as_str(), "geography" | "geometry") {
            continue;
        }
        let Ok(idx) = usize::try_from(att.attnum - 1) else {
            continue;
        };
        let Some(cell) = columns.get_mut(idx) else {
            continue;
        };
        let Some(ColumnValue::PgPending { raw, .. }) = cell.as_ref() else {
            continue;
        };
        if let Some(text) = gserialized_point_to_wkt(raw) {
            *cell = Some(ColumnValue::Text(text));
        }
    }
}

/// PostGIS on-disk GSERIALIZED → `POINT(x y)` for 2-D points. Layout:
/// `[srid(3) + gflags(1)][geomtype u32-le][…]`; POINT has geomtype 1 and its
/// two `f64`-LE coordinates are the trailing 16 bytes. `None` for non-points.
fn gserialized_point_to_wkt(raw: &[u8]) -> Option<String> {
    if raw.len() < 16 {
        return None;
    }
    let geomtype = u32::from_le_bytes(raw[4..8].try_into().ok()?);
    if geomtype != 1 {
        return None;
    }
    let n = raw.len();
    let x = f64::from_le_bytes(raw[n - 16..n - 8].try_into().ok()?);
    let y = f64::from_le_bytes(raw[n - 8..n].try_into().ok()?);
    Some(format!("POINT({x} {y})"))
}

impl OracleStats {
    pub fn summary(&self) -> String {
        use std::fmt::Write as _;
        let ld = |a: &AtomicU64| a.load(Ordering::Relaxed);
        let mut s = format!(
            "oracle blocks={} cells={}",
            ld(&self.blocks),
            ld(&self.cells)
        );
        let pairs: [(&str, u64); 2] = [
            ("conv_err", ld(&self.conversion_errors)),
            ("err", ld(&self.errors)),
        ];
        for (label, n) in pairs {
            if n > 0 {
                write!(&mut s, " {label}={n}").unwrap();
            }
        }
        s
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn col<'a>(
        ordinal: u32,
        name: &'a str,
        ty: &'a str,
        buf: &'a OracleColumnBuf,
    ) -> OracleRequestColumn<'a> {
        OracleRequestColumn {
            ordinal,
            name,
            target_type: ty,
            buf,
        }
    }

    #[test]
    fn request_frames_metadata_then_row_major_cells() {
        let mut a = OracleColumnBuf::new(1007, -1);
        a.push(OracleCell::DiskRaw(vec![9, 9]));
        a.push(OracleCell::Default);
        let mut b = OracleColumnBuf::new(3802, -1);
        b.push(OracleCell::Literal(b"x".to_vec()));
        b.push(OracleCell::TextInput(b"{}".to_vec()));
        let cols = [col(0, "t", "Array(Int32)", &a), col(3, "j", "JSON", &b)];

        let out = encode_request(&cols, 2);
        let mut c = 0;
        let u32_at = |c: &mut usize| {
            let v = u32::from_be_bytes(out[*c..*c + 4].try_into().unwrap());
            *c += 4;
            v
        };
        assert_eq!(u32_at(&mut c), 2, "n_rows");
        assert_eq!(u32_at(&mut c), 2, "n_columns");
        // First column's metadata, then the second's, before any cell
        assert_eq!(u32_at(&mut c), 1007, "source oid");
        assert_eq!(u32_at(&mut c) as i32, -1, "typmod");
        c += 4 + 1 + 4 + "Array(Int32)".len();
        assert_eq!(u32_at(&mut c), 3802, "second source oid");
        c += 4 + 4 + 1 + 4 + "JSON".len();
        assert_eq!(
            &out[c..],
            &[
                CELL_DISK_RAW,
                0,
                0,
                0,
                2,
                9,
                9,
                CELL_LITERAL,
                0,
                0,
                0,
                1,
                b'x',
                CELL_DEFAULT,
                CELL_TEXT,
                0,
                0,
                0,
                2,
                b'{',
                b'}',
            ]
        );
    }

    #[test]
    fn request_column_bytes_matches_framed_size() {
        let mut buf = OracleColumnBuf::new(114, -1);
        buf.push(OracleCell::Default);
        buf.push(OracleCell::DiskRaw(vec![1, 2, 3]));
        let cols = [col(7, "payload", "Nullable(JSON)", &buf)];
        assert_eq!(
            encode_request(&cols, 2).len() + 1, // opcode the bridge prepends
            REQUEST_FRAME_BYTES
                + request_column_bytes("payload", "Nullable(JSON)")
                + buf.approx_size()
        );
    }

    #[test]
    fn gserialized_2d_point_renders_wkt() {
        // [srid+gflags:4][geomtype=1:4][X f64le][Y f64le]
        let mut raw = vec![0u8, 0, 0, 0, 1, 0, 0, 0];
        raw.extend_from_slice(&30.5_f64.to_le_bytes());
        raw.extend_from_slice(&81.25_f64.to_le_bytes());
        assert_eq!(
            gserialized_point_to_wkt(&raw).as_deref(),
            Some("POINT(30.5 81.25)")
        );
        // non-point geomtype → None
        raw[4] = 2;
        assert_eq!(gserialized_point_to_wkt(&raw), None);
    }

    #[test]
    fn only_broken_sockets_retry() {
        let cases = [
            (
                OracleError::Bridge(BridgeError::Io(std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe,
                    "gone",
                ))),
                true,
            ),
            (
                OracleError::Bridge(BridgeError::Protocol("short frame".into())),
                true,
            ),
            (
                OracleError::Bridge(BridgeError::RequestTooLarge { len: 2, cap: 1 }),
                false,
            ),
            (
                OracleError::Bridge(BridgeError::Version {
                    proto: 1,
                    projection: 1,
                    want_proto: 2,
                    want_projection: 1,
                }),
                false,
            ),
            (
                OracleError::Bridge(BridgeError::Remote("row 1: bad datum".into())),
                false,
            ),
            (OracleError::Response("wrong column".into()), false),
            (OracleError::Absent("t".into()), false),
        ];
        for (err, want) in cases {
            assert_eq!(err.retryable(), want, "{err}");
        }
    }

    #[test]
    fn stats_summary_skips_zero_buckets() {
        let s = OracleStats::default();
        s.blocks.store(4, Ordering::Relaxed);
        s.cells.store(40, Ordering::Relaxed);
        s.errors.store(2, Ordering::Relaxed);
        let out = s.summary();
        assert!(out.contains("blocks=4"));
        assert!(out.contains("cells=40"));
        assert!(out.contains("err=2"));
        assert!(!out.contains("conv_err"));
    }
}
