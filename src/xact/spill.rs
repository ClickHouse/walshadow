//! Per-xact append-only spill backend.
//!
//! Mirrors PG `pg_replslot/<slot>/xid-*.snap`: one file per buffered xid
//! under `{data_dir}/spill/`. Buffer flushes its in-memory queue here once
//! `xact_buffer_max` breached; commit drain reads back in WAL order then
//! unlinks, abort just unlinks.
//!
//! Custom binary encoder over JSON: JSON inflates the bytea / chunk_data path
//! 3–4×, worst on the bulk INSERT/UPDATE workload. Manual length-prefixed
//! binary surfaces every decode failure as [`SpillError::Format`] with a
//! precise offset.
//!
//! ## On-disk layout
//!
//! ```text
//! [2 bytes "WS" magic] [u16 LE version] then repeating:
//! [u8 tag] [u32 len LE] [body of `len` bytes]
//!   tag = 0 → SpillEntry::Heap        (body = encoded DecodedHeap)
//!   tag = 1 → SpillEntry::Chunk       (body = encoded ToastChunk)
//!   tag = 2 → SpillEntry::ToastDelete (body = encoded ToastDelete)
//!   tag = 3 → SpillEntry::Raw         (body = encoded RawRecord)
//! ```
//!
//! Version bumps on any body-encoding change. Files don't survive a restart
//! (resume contract wipes the dir), so magic + version is self-check honesty:
//! a mid-restart format mismatch surfaces as [`SpillError::Format`] not a
//! silent misparse. Outer length lets [`SpillReader::next`] skip a malformed
//! body; reader propagates any malformation as Format since xact is
//! unrecoverable anyway.
//!
//! ## Eviction policy
//!
//! Largest-xact-first, mirroring PG `ReorderBufferLargestTXN`
//! (`src/backend/replication/logical/reorderbuffer.c`).
//! Buffer owns the policy; [`SpillStore`] just lays out files.
//!
//! ## Crash recovery
//!
//! Startup wipes the dir via [`SpillStore::clear`]. Cursor file commits
//! atomically post-drain, so on-disk state is always "drained-and-in-CH" or
//! "replayable-from-WAL-cursor"; re-streaming from cursor LSN is cheaper than
//! verifying spill integrity, so partial files aren't replayed.

use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use thiserror::Error;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use walrus::pg::walparser::RelFileNode;

use crate::decode::heap_decoder::{ColumnValue, DecodedHeap, DecodedTuple, HeapOp, ToastPointer};

/// Heap tuples + TOAST chunks share the file: both flush at commit drain,
/// WAL-aligned ordering keeps the drain a single linear read
#[derive(Debug, Clone, PartialEq)]
pub enum SpillEntry {
    Heap(Box<DecodedHeap>),
    Chunk(ToastChunk),
    ToastDelete(ToastDelete),
    /// Undecoded record on a filenode invisible at record time (same-xact
    /// CREATE / TRUNCATE / rewrite generation); decoded at commit once
    /// `relation_at(rfn, commit_lsn)` resolves the survivor
    Raw(Box<RawRecord>),
}

/// Raw decode inputs for one WAL record, enough to re-run
/// [`crate::decode::heap_decoder::decode_heap_record`] after commit-time
/// resolution. Block images survive so an insert whose tuple lives only
/// in an FPI (`HEAP_INSERT_NO_LOGICAL` strips `REGBUF_KEEP_DATA`, PG
/// `src/backend/access/heap/heapam.c`) still decodes.
#[derive(Debug, Clone, PartialEq)]
pub struct RawRecord {
    pub rm: u8,
    pub info: u8,
    pub source_lsn: u64,
    /// Selects PG-14 vs PG-15 FPI bit semantics for image restore
    pub page_magic: u16,
    pub main_data: Vec<u8>,
    pub blocks: Vec<RawBlock>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RawBlock {
    pub block_id: u8,
    pub fork_flags: u8,
    pub data_length: u16,
    pub image_length: u16,
    pub hole_offset: u16,
    pub hole_length: u16,
    pub bimg_info: u8,
    pub spc_node: u32,
    pub db_node: u32,
    pub rel_node: u32,
    pub block_no: u32,
    pub image: Vec<u8>,
    pub data: Vec<u8>,
}

impl RawRecord {
    pub fn from_parsed(
        parsed: &walrus::pg::walparser::XLogRecord<'_>,
        source_lsn: u64,
        page_magic: u16,
    ) -> Self {
        Self {
            rm: parsed.header.resource_manager_id,
            info: parsed.header.info,
            source_lsn,
            page_magic,
            main_data: parsed.main_data.to_vec(),
            blocks: parsed
                .blocks
                .iter()
                .map(|b| RawBlock {
                    block_id: b.header.block_id,
                    fork_flags: b.header.fork_flags,
                    data_length: b.header.data_length,
                    image_length: b.header.image_header.image_length,
                    hole_offset: b.header.image_header.hole_offset,
                    hole_length: b.header.image_header.hole_length,
                    bimg_info: b.header.image_header.info,
                    spc_node: b.header.location.rel.spc_node,
                    db_node: b.header.location.rel.db_node,
                    rel_node: b.header.location.rel.rel_node,
                    block_no: b.header.location.block_no,
                    image: b.image.to_vec(),
                    data: b.data.to_vec(),
                })
                .collect(),
        }
    }

    /// Rebuild a borrowed record for the shared heap decoder. `xact_id` /
    /// CRC are irrelevant post-commit and stay zero.
    pub fn to_xlog_record(&self) -> walrus::pg::walparser::XLogRecord<'_> {
        use walrus::pg::walparser::{
            BlockLocation, XLogRecord, XLogRecordBlock, XLogRecordBlockHeader,
            XLogRecordBlockImageHeader, XLogRecordHeader,
        };
        XLogRecord {
            header: XLogRecordHeader {
                info: self.info,
                resource_manager_id: self.rm,
                ..Default::default()
            },
            main_data_len: self.main_data.len() as u32,
            origin: 0,
            toplevel_xid: 0,
            blocks: self
                .blocks
                .iter()
                .map(|b| XLogRecordBlock {
                    header: XLogRecordBlockHeader {
                        block_id: b.block_id,
                        fork_flags: b.fork_flags,
                        data_length: b.data_length,
                        image_header: XLogRecordBlockImageHeader {
                            image_length: b.image_length,
                            hole_offset: b.hole_offset,
                            hole_length: b.hole_length,
                            info: b.bimg_info,
                        },
                        location: BlockLocation {
                            rel: RelFileNode {
                                spc_node: b.spc_node,
                                db_node: b.db_node,
                                rel_node: b.rel_node,
                            },
                            block_no: b.block_no,
                        },
                    },
                    image: std::borrow::Cow::Borrowed(&b.image[..]),
                    data: std::borrow::Cow::Borrowed(&b.data[..]),
                })
                .collect(),
            main_data: std::borrow::Cow::Borrowed(&self.main_data[..]),
        }
    }

    /// Target filenode: block 0's relation, matching decoder convention
    pub fn rfn(&self) -> Option<RelFileNode> {
        self.blocks.first().map(|b| RelFileNode {
            spc_node: b.spc_node,
            db_node: b.db_node,
            rel_node: b.rel_node,
        })
    }

    pub fn approx_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
            + self.main_data.len()
            + self
                .blocks
                .iter()
                .map(|b| std::mem::size_of::<RawBlock>() + b.image.len() + b.data.len())
                .sum::<usize>()
    }
}

/// One chunk from a `pg_toast.pg_toast_<oid>` INSERT. PG `toast_save_datum`
/// writes `(chunk_id oid, chunk_seq int4, chunk_data bytea)`; buffer keys by
/// `(toast_relid, value_id)`, concatenates by `chunk_seq` at drain.
#[derive(Debug, Clone, PartialEq)]
pub struct ToastChunk {
    pub toast_relid: u32,
    pub value_id: u32,
    /// 0-based, PG writes sequentially
    pub chunk_seq: u32,
    pub source_lsn: u64,
    /// Invalid TID cannot enter mirror, but remains usable in transaction
    pub blkno: u32,
    pub offnum: u16,
    /// `TOAST_MAX_CHUNK_SIZE` ≈ 1996 bytes typical, last chunk shorter.
    /// Shared body: resolution map and mirror row hold one allocation
    pub chunk_data: bytes::Bytes,
}

/// Deleted TOAST tuple TID, one tombstone per chunk
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ToastDelete {
    pub toast_relid: u32,
    pub blkno: u32,
    pub offnum: u16,
    pub source_lsn: u64,
}

#[derive(Debug, Error)]
pub enum SpillError {
    #[error("io: {0}")]
    Io(#[from] io::Error),
    #[error("spill format at offset {offset}: {detail}")]
    Format { offset: usize, detail: String },
}

pub type Result<T> = std::result::Result<T, SpillError>;

/// ASCII for `xxd`-friendly debug
pub const SPILL_MAGIC: [u8; 2] = *b"WS";
/// v2 added `HeapOp::Truncate` tag-4 body encoding. v3 added chunk TIDs and
/// `ToastDelete` entries. v4 added tag-3 `Raw` stashed records. `DrainEntry`
/// events (Catalog, ToastBarrier, and Config/Signal per the runtime-config
/// plan) are drain-time, never spilled, so they don't touch this format
pub const SPILL_VERSION: u16 = 4;

pub struct SpillStore {
    dir: PathBuf,
}

impl SpillStore {
    /// Synchronous: called once at daemon startup before the runtime is busy
    pub fn new(dir: PathBuf) -> Result<Self> {
        std::fs::create_dir_all(&dir)?;
        Ok(Self { dir })
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// Filename includes `first_lsn` so two streams reusing an xid value
    /// after a slot rotation don't collide on disk
    pub async fn writer(&self, xid: u32, first_lsn: u64) -> Result<SpillWriter> {
        let path = self.dir.join(format!("xid-{xid:010}-{first_lsn:016X}.bin"));
        let mut header = [0u8; 4];
        header[..2].copy_from_slice(&SPILL_MAGIC);
        header[2..].copy_from_slice(&SPILL_VERSION.to_le_bytes());
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .read(true)
            .open(&path)
            .await?;
        file.write_all(&header).await?;
        Ok(SpillWriter {
            file,
            path,
            byte_count: header.len() as u64,
        })
    }

    /// Wipe every spill file. Crash-recovery contract: on-disk state is
    /// always drained-into-CH or replayable from the cursor's `decoder_lsn`,
    /// so prior-crash leftovers are safe to discard
    pub async fn clear(&self) -> Result<()> {
        let mut entries = match tokio::fs::read_dir(&self.dir).await {
            Ok(e) => e,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(()),
            Err(e) => return Err(e.into()),
        };
        while let Some(entry) = entries.next_entry().await? {
            let p = entry.path();
            if p.file_name().and_then(|n| n.to_str()).is_some_and(|s| {
                (s.starts_with("xid-") || s.starts_with("toastbody-")) && s.ends_with(".bin")
            }) {
                let _ = tokio::fs::remove_file(&p).await;
            }
        }
        Ok(())
    }
}

/// Positional range in one transaction body spool
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BodyRef {
    pub offset: u64,
    pub len: u32,
}

/// Read side of one commit drain's TOAST body spool. Consumers hold
/// `Arc<BodySpoolFile>`; unlink-while-open is safe, fd pins data until
/// last reader drops
#[derive(Debug)]
pub struct BodySpoolFile {
    file: std::fs::File,
    path: PathBuf,
    /// Disk accounting rides the shared handle, not the writer: unlink
    /// removes the pathname while open fds keep blocks allocated, so the
    /// spool-bytes gauge releases only when the last owner (writer or
    /// reader view) drops
    spool_bytes: Option<Arc<std::sync::atomic::AtomicU64>>,
    held_bytes: std::sync::atomic::AtomicU64,
}

impl BodySpoolFile {
    pub fn path(&self) -> &Path {
        &self.path
    }

    fn charge(&self, n: u64) {
        self.held_bytes
            .fetch_add(n, std::sync::atomic::Ordering::Relaxed);
        if let Some(gauge) = &self.spool_bytes {
            gauge.fetch_add(n, std::sync::atomic::Ordering::Relaxed);
        }
    }

    /// Positional read, no shared cursor: decode workers read concurrently.
    /// Sync from async context; production wants spawn_blocking or a
    /// buffered pread layer for cold reads
    pub fn read_at(&self, offset: u64, out: &mut [u8]) -> io::Result<()> {
        use std::os::unix::fs::FileExt;
        self.file.read_exact_at(out, offset)
    }

    pub fn read(&self, r: BodyRef) -> io::Result<Vec<u8>> {
        let mut out = vec![0u8; r.len as usize];
        self.read_at(r.offset, &mut out)?;
        Ok(out)
    }
}

impl Drop for BodySpoolFile {
    fn drop(&mut self) {
        if let Some(gauge) = &self.spool_bytes {
            gauge.fetch_sub(
                self.held_bytes.load(std::sync::atomic::Ordering::Relaxed),
                std::sync::atomic::Ordering::Relaxed,
            );
        }
    }
}

/// Write coalescing threshold; refs into the buffered tail become readable
/// at the next [`BodySpoolWriter::flush`]
const BODY_SPOOL_BUF: usize = 256 << 10;

/// Drain-owned append side of the xact body spool. Raw concatenated
/// bodies, no framing: [`BodyRef`]s are process-local and files never
/// survive restart ([`SpillStore::clear`])
pub struct BodySpoolWriter {
    shared: Arc<BodySpoolFile>,
    len: u64,
    buf: Vec<u8>,
}

impl BodySpoolWriter {
    /// Distinct prefix from xact spill so startup wipes both families;
    /// `commit_lsn` disambiguates xid reuse across slot rotations
    pub fn create(
        dir: &Path,
        xid: u32,
        commit_lsn: u64,
        spool_bytes: Option<Arc<std::sync::atomic::AtomicU64>>,
    ) -> Result<Self> {
        let path = dir.join(format!("toastbody-{xid:010}-{commit_lsn:016X}.bin"));
        let file = std::fs::OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(&path)?;
        Ok(Self {
            shared: Arc::new(BodySpoolFile {
                file,
                path,
                spool_bytes,
                held_bytes: std::sync::atomic::AtomicU64::new(0),
            }),
            len: 0,
            buf: Vec::new(),
        })
    }

    pub fn append(&mut self, body: &[u8]) -> Result<BodyRef> {
        let len = u32::try_from(body.len()).map_err(|_| SpillError::Format {
            offset: self.len as usize,
            detail: "toast body over u32".into(),
        })?;
        self.shared.charge(u64::from(len));
        let r = BodyRef {
            offset: self.len,
            len,
        };
        self.buf.extend_from_slice(body);
        self.len += u64::from(len);
        if self.buf.len() >= BODY_SPOOL_BUF {
            self.flush()?;
        }
        Ok(r)
    }

    pub fn flush(&mut self) -> Result<()> {
        if !self.buf.is_empty() {
            use std::io::Write;
            (&self.shared.file).write_all(&self.buf)?;
            self.buf.clear();
        }
        Ok(())
    }

    /// Total appended bytes
    pub fn len(&self) -> u64 {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Reader handle for batch views; outlives writer and unlink
    pub fn shared(&self) -> &Arc<BodySpoolFile> {
        &self.shared
    }

    /// Post-dispatch cleanup; open reader views stay valid via fd
    pub fn unlink(self) -> Result<()> {
        match std::fs::remove_file(&self.shared.path) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e.into()),
        }
    }
}

pub struct SpillWriter {
    file: File,
    path: PathBuf,
    byte_count: u64,
}

impl SpillWriter {
    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn byte_count(&self) -> u64 {
        self.byte_count
    }

    pub async fn write(&mut self, entry: &SpillEntry) -> Result<()> {
        let mut body = Vec::with_capacity(128);
        let tag: u8 = match entry {
            SpillEntry::Heap(_) => 0,
            SpillEntry::Chunk(_) => 1,
            SpillEntry::ToastDelete(_) => 2,
            SpillEntry::Raw(_) => 3,
        };
        body.push(tag);
        // u32 LE length placeholder, back-patched after body appends in place
        let len_off = body.len();
        body.extend_from_slice(&[0u8; 4]);
        let inner_start = body.len();
        match entry {
            SpillEntry::Heap(h) => encode_heap_into(&mut body, h),
            SpillEntry::Chunk(c) => encode_chunk_into(&mut body, c),
            SpillEntry::ToastDelete(d) => encode_toast_delete_into(&mut body, d),
            SpillEntry::Raw(r) => encode_raw_into(&mut body, r),
        }
        let inner_len = (body.len() - inner_start) as u32;
        body[len_off..len_off + 4].copy_from_slice(&inner_len.to_le_bytes());
        self.file.write_all(&body).await?;
        self.byte_count += body.len() as u64;
        Ok(())
    }

    /// Flush + close, return a reader at the start. Caller drives `next()` to
    /// `Ok(None)` then `unlink()`
    pub async fn finish(mut self) -> Result<SpillReader> {
        self.file.flush().await?;
        self.file.sync_all().await?;
        drop(self.file);
        let file = OpenOptions::new().read(true).open(&self.path).await?;
        Ok(SpillReader {
            file,
            path: self.path,
            header_checked: false,
        })
    }

    /// Abort path: drop the file unread
    pub async fn unlink(self) -> Result<()> {
        unlink_file(self.file, &self.path).await
    }
}

/// Drop `file`, remove `path`, tolerating already-gone (abort races,
/// crash-cleanup re-runs)
async fn unlink_file(file: File, path: &Path) -> Result<()> {
    drop(file);
    match tokio::fs::remove_file(path).await {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e.into()),
    }
}

pub struct SpillReader {
    file: File,
    path: PathBuf,
    /// Lazy header check: first `next()` verifies [`SPILL_MAGIC`] + version,
    /// so a stale on-disk spill fails cleanly with [`SpillError::Format`]
    header_checked: bool,
}

impl SpillReader {
    pub fn path(&self) -> &Path {
        &self.path
    }

    async fn check_header(&mut self) -> Result<()> {
        let mut buf = [0u8; 4];
        if let Err(e) = self.file.read_exact(&mut buf).await {
            return Err(match e.kind() {
                io::ErrorKind::UnexpectedEof => SpillError::Format {
                    offset: 0,
                    detail: "spill file shorter than 4-byte header".into(),
                },
                _ => e.into(),
            });
        }
        if buf[..2] != SPILL_MAGIC {
            return Err(SpillError::Format {
                offset: 0,
                detail: format!("bad magic {:02x?}, expected WS", &buf[..2]),
            });
        }
        let version = u16::from_le_bytes([buf[2], buf[3]]);
        if version != SPILL_VERSION {
            return Err(SpillError::Format {
                offset: 2,
                detail: format!("unsupported spill version {version}, expected {SPILL_VERSION}"),
            });
        }
        self.header_checked = true;
        Ok(())
    }

    /// One entry per call; `Ok(None)` at clean EOF
    pub async fn next(&mut self) -> Result<Option<SpillEntry>> {
        if !self.header_checked {
            self.check_header().await?;
        }
        let mut tag_buf = [0u8; 1];
        match self.file.read_exact(&mut tag_buf).await {
            Ok(_) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(e.into()),
        }
        let mut len_buf = [0u8; 4];
        self.file.read_exact(&mut len_buf).await?;
        let len = u32::from_le_bytes(len_buf) as usize;
        let mut body = vec![0u8; len];
        self.file.read_exact(&mut body).await?;
        let entry = match tag_buf[0] {
            0 => {
                let mut cur = Cursor::new(&body);
                let h = decode_heap(&mut cur)?;
                SpillEntry::Heap(Box::new(h))
            }
            1 => {
                let mut cur = Cursor::new(&body);
                let c = decode_chunk(&mut cur)?;
                SpillEntry::Chunk(c)
            }
            2 => {
                let mut cur = Cursor::new(&body);
                let d = decode_toast_delete(&mut cur)?;
                SpillEntry::ToastDelete(d)
            }
            3 => {
                let mut cur = Cursor::new(&body);
                let r = decode_raw(&mut cur)?;
                SpillEntry::Raw(Box::new(r))
            }
            other => {
                return Err(SpillError::Format {
                    offset: 0,
                    detail: format!("unknown entry tag {other}"),
                });
            }
        };
        Ok(Some(entry))
    }

    pub async fn unlink(self) -> Result<()> {
        unlink_file(self.file, &self.path).await
    }
}

// ── encoding ────────────────────────────────────────────────────────

pub(crate) fn push_u8(out: &mut Vec<u8>, v: u8) {
    out.push(v);
}
pub(crate) fn push_u16(out: &mut Vec<u8>, v: u16) {
    out.extend_from_slice(&v.to_le_bytes());
}
pub(crate) fn push_u32(out: &mut Vec<u8>, v: u32) {
    out.extend_from_slice(&v.to_le_bytes());
}
fn push_i32(out: &mut Vec<u8>, v: i32) {
    out.extend_from_slice(&v.to_le_bytes());
}
pub(crate) fn push_u64(out: &mut Vec<u8>, v: u64) {
    out.extend_from_slice(&v.to_le_bytes());
}
fn push_i64(out: &mut Vec<u8>, v: i64) {
    out.extend_from_slice(&v.to_le_bytes());
}
fn push_bytes(out: &mut Vec<u8>, b: &[u8]) {
    push_u32(out, b.len() as u32);
    out.extend_from_slice(b);
}
fn push_str(out: &mut Vec<u8>, s: &str) {
    push_bytes(out, s.as_bytes());
}

fn encode_heap_into(out: &mut Vec<u8>, h: &DecodedHeap) {
    push_u32(out, h.rfn.spc_node);
    push_u32(out, h.rfn.db_node);
    push_u32(out, h.rfn.rel_node);
    push_u32(out, h.xid);
    push_u64(out, h.source_lsn);
    let op_byte: u8 = match h.op {
        HeapOp::Insert => 0,
        HeapOp::Update => 1,
        HeapOp::HotUpdate => 2,
        HeapOp::Delete => 3,
        HeapOp::Truncate => 4,
    };
    push_u8(out, op_byte);
    encode_opt_tuple(out, h.new.as_ref());
    encode_opt_tuple(out, h.old.as_ref());
}

fn encode_opt_tuple(out: &mut Vec<u8>, t: Option<&DecodedTuple>) {
    match t {
        None => push_u8(out, 0),
        Some(t) => {
            push_u8(out, 1);
            push_u8(out, t.partial as u8);
            push_u32(out, t.columns.len() as u32);
            for col in &t.columns {
                match col {
                    None => push_u8(out, 0),
                    Some(v) => {
                        push_u8(out, 1);
                        encode_value(out, v);
                    }
                }
            }
        }
    }
}

pub(crate) fn encode_value(out: &mut Vec<u8>, v: &ColumnValue) {
    match v {
        ColumnValue::Null => push_u8(out, 0),
        ColumnValue::Bool(b) => {
            push_u8(out, 1);
            push_u8(out, *b as u8);
        }
        ColumnValue::Char(i) => {
            push_u8(out, 2);
            push_u8(out, *i as u8);
        }
        ColumnValue::Int2(i) => {
            push_u8(out, 3);
            push_u16(out, *i as u16);
        }
        ColumnValue::Int4(i) => {
            push_u8(out, 4);
            push_i32(out, *i);
        }
        ColumnValue::Int8(i) => {
            push_u8(out, 5);
            push_i64(out, *i);
        }
        ColumnValue::Float4(f) => {
            push_u8(out, 6);
            out.extend_from_slice(&f.to_le_bytes());
        }
        ColumnValue::Float8(f) => {
            push_u8(out, 7);
            out.extend_from_slice(&f.to_le_bytes());
        }
        ColumnValue::Oid(i) => {
            push_u8(out, 8);
            push_u32(out, *i);
        }
        ColumnValue::Date(i) => {
            push_u8(out, 9);
            push_i32(out, *i);
        }
        ColumnValue::Time(i) => {
            push_u8(out, 10);
            push_i64(out, *i);
        }
        ColumnValue::Timestamp(i) => {
            push_u8(out, 11);
            push_i64(out, *i);
        }
        ColumnValue::TimestampTz(i) => {
            push_u8(out, 12);
            push_i64(out, *i);
        }
        ColumnValue::TimeTz { micros, tz_seconds } => {
            push_u8(out, 13);
            push_i64(out, *micros);
            push_i32(out, *tz_seconds);
        }
        ColumnValue::Uuid(b) => {
            push_u8(out, 14);
            out.extend_from_slice(b);
        }
        ColumnValue::Name(s) => {
            push_u8(out, 15);
            push_str(out, s);
        }
        ColumnValue::Bytea(b) => {
            push_u8(out, 16);
            push_bytes(out, b);
        }
        ColumnValue::Text(s) => {
            push_u8(out, 17);
            push_str(out, s);
        }
        ColumnValue::ExternalToast(p) => {
            push_u8(out, 18);
            push_i32(out, p.va_rawsize);
            push_u32(out, p.va_extinfo);
            push_u32(out, p.va_valueid);
            push_u32(out, p.va_toastrelid);
        }
        ColumnValue::Unsupported { type_oid, raw } => {
            push_u8(out, 19);
            push_u32(out, *type_oid);
            push_bytes(out, raw);
        }
        ColumnValue::Numeric(n) => {
            use crate::decode::codecs::NumericKind;
            push_u8(out, 20);
            match n {
                NumericKind::Finite(s) => {
                    push_u8(out, 0);
                    push_str(out, s);
                }
                NumericKind::NaN => push_u8(out, 1),
                NumericKind::PInf => push_u8(out, 2),
                NumericKind::NInf => push_u8(out, 3),
            }
        }
        ColumnValue::Inet(v) => {
            push_u8(out, 21);
            push_u8(out, v.family);
            push_u8(out, v.bits);
            push_u8(out, v.is_cidr as u8);
            push_bytes(out, &v.addr);
        }
        ColumnValue::Interval(v) => {
            push_u8(out, 22);
            push_i32(out, v.months);
            push_i32(out, v.days);
            push_i64(out, v.micros);
        }
        ColumnValue::Json(s) => {
            push_u8(out, 23);
            push_str(out, s);
        }
        ColumnValue::PgPending { type_oid, raw } => {
            push_u8(out, 24);
            push_u32(out, *type_oid);
            push_bytes(out, raw);
        }
    }
}

fn encode_chunk_into(out: &mut Vec<u8>, c: &ToastChunk) {
    out.reserve(40 + c.chunk_data.len());
    push_u32(out, c.toast_relid);
    push_u32(out, c.value_id);
    push_u32(out, c.chunk_seq);
    push_u64(out, c.source_lsn);
    push_u32(out, c.blkno);
    push_u16(out, c.offnum);
    push_bytes(out, &c.chunk_data);
}

fn encode_toast_delete_into(out: &mut Vec<u8>, d: &ToastDelete) {
    push_u32(out, d.toast_relid);
    push_u32(out, d.blkno);
    push_u16(out, d.offnum);
    push_u64(out, d.source_lsn);
}

fn encode_raw_into(out: &mut Vec<u8>, r: &RawRecord) {
    out.reserve(32 + r.approx_bytes());
    push_u8(out, r.rm);
    push_u8(out, r.info);
    push_u64(out, r.source_lsn);
    push_u16(out, r.page_magic);
    push_bytes(out, &r.main_data);
    push_u32(out, r.blocks.len() as u32);
    for b in &r.blocks {
        push_u8(out, b.block_id);
        push_u8(out, b.fork_flags);
        push_u16(out, b.data_length);
        push_u16(out, b.image_length);
        push_u16(out, b.hole_offset);
        push_u16(out, b.hole_length);
        push_u8(out, b.bimg_info);
        push_u32(out, b.spc_node);
        push_u32(out, b.db_node);
        push_u32(out, b.rel_node);
        push_u32(out, b.block_no);
        push_bytes(out, &b.image);
        push_bytes(out, &b.data);
    }
}

// ── decoding ────────────────────────────────────────────────────────

pub(crate) struct Cursor<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    pub(crate) fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }

    fn need(&mut self, n: usize) -> Result<&'a [u8]> {
        if self.pos + n > self.buf.len() {
            return Err(SpillError::Format {
                offset: self.pos,
                detail: format!("short read: need {n}, have {}", self.buf.len() - self.pos),
            });
        }
        let s = &self.buf[self.pos..self.pos + n];
        self.pos += n;
        Ok(s)
    }

    pub(crate) fn u8(&mut self) -> Result<u8> {
        Ok(self.need(1)?[0])
    }
    pub(crate) fn u16(&mut self) -> Result<u16> {
        Ok(u16::from_le_bytes(self.need(2)?.try_into().unwrap()))
    }
    pub(crate) fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_le_bytes(self.need(4)?.try_into().unwrap()))
    }
    fn i32(&mut self) -> Result<i32> {
        Ok(i32::from_le_bytes(self.need(4)?.try_into().unwrap()))
    }
    pub(crate) fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_le_bytes(self.need(8)?.try_into().unwrap()))
    }
    fn i64(&mut self) -> Result<i64> {
        Ok(i64::from_le_bytes(self.need(8)?.try_into().unwrap()))
    }
    fn f32(&mut self) -> Result<f32> {
        Ok(f32::from_le_bytes(self.need(4)?.try_into().unwrap()))
    }
    fn f64(&mut self) -> Result<f64> {
        Ok(f64::from_le_bytes(self.need(8)?.try_into().unwrap()))
    }
    pub(crate) fn bytes(&mut self) -> Result<Vec<u8>> {
        let n = self.u32()? as usize;
        Ok(self.need(n)?.to_vec())
    }
    fn string(&mut self) -> Result<String> {
        let bs = self.bytes()?;
        String::from_utf8(bs).map_err(|e| SpillError::Format {
            offset: self.pos,
            detail: format!("utf8: {e}"),
        })
    }
}

fn decode_heap(c: &mut Cursor) -> Result<DecodedHeap> {
    let spc_node = c.u32()?;
    let db_node = c.u32()?;
    let rel_node = c.u32()?;
    let xid = c.u32()?;
    let source_lsn = c.u64()?;
    let op = match c.u8()? {
        0 => HeapOp::Insert,
        1 => HeapOp::Update,
        2 => HeapOp::HotUpdate,
        3 => HeapOp::Delete,
        4 => HeapOp::Truncate,
        other => {
            return Err(SpillError::Format {
                offset: c.pos,
                detail: format!("unknown HeapOp tag {other}"),
            });
        }
    };
    let new = decode_opt_tuple(c)?;
    let old = decode_opt_tuple(c)?;
    Ok(DecodedHeap {
        rfn: RelFileNode {
            spc_node,
            db_node,
            rel_node,
        },
        xid,
        source_lsn,
        op,
        new,
        old,
    })
}

fn decode_opt_tuple(c: &mut Cursor) -> Result<Option<DecodedTuple>> {
    if c.u8()? == 0 {
        return Ok(None);
    }
    let partial = c.u8()? != 0;
    let n = c.u32()? as usize;
    let mut columns = Vec::with_capacity(n);
    for _ in 0..n {
        if c.u8()? == 0 {
            columns.push(None);
        } else {
            columns.push(Some(decode_value(c)?));
        }
    }
    Ok(Some(DecodedTuple { columns, partial }))
}

pub(crate) fn decode_value(c: &mut Cursor) -> Result<ColumnValue> {
    let tag = c.u8()?;
    let v = match tag {
        0 => ColumnValue::Null,
        1 => ColumnValue::Bool(c.u8()? != 0),
        2 => ColumnValue::Char(c.u8()? as i8),
        3 => ColumnValue::Int2(c.u16()? as i16),
        4 => ColumnValue::Int4(c.i32()?),
        5 => ColumnValue::Int8(c.i64()?),
        6 => ColumnValue::Float4(c.f32()?),
        7 => ColumnValue::Float8(c.f64()?),
        8 => ColumnValue::Oid(c.u32()?),
        9 => ColumnValue::Date(c.i32()?),
        10 => ColumnValue::Time(c.i64()?),
        11 => ColumnValue::Timestamp(c.i64()?),
        12 => ColumnValue::TimestampTz(c.i64()?),
        13 => {
            let micros = c.i64()?;
            let tz_seconds = c.i32()?;
            ColumnValue::TimeTz { micros, tz_seconds }
        }
        14 => ColumnValue::Uuid(c.need(16)?.try_into().unwrap()),
        15 => ColumnValue::Name(c.string()?),
        16 => ColumnValue::Bytea(c.bytes()?),
        17 => ColumnValue::Text(c.string()?),
        18 => {
            let va_rawsize = c.i32()?;
            let va_extinfo = c.u32()?;
            let va_valueid = c.u32()?;
            let va_toastrelid = c.u32()?;
            ColumnValue::ExternalToast(ToastPointer {
                va_rawsize,
                va_extinfo,
                va_valueid,
                va_toastrelid,
            })
        }
        19 => {
            let type_oid = c.u32()?;
            let raw = c.bytes()?;
            ColumnValue::Unsupported { type_oid, raw }
        }
        20 => {
            use crate::decode::codecs::NumericKind;
            let kind = c.u8()?;
            ColumnValue::Numeric(match kind {
                0 => NumericKind::Finite(c.string()?),
                1 => NumericKind::NaN,
                2 => NumericKind::PInf,
                3 => NumericKind::NInf,
                other => {
                    return Err(SpillError::Format {
                        offset: c.pos,
                        detail: format!("unknown NumericKind tag {other}"),
                    });
                }
            })
        }
        21 => {
            let family = c.u8()?;
            let bits = c.u8()?;
            let is_cidr = c.u8()? != 0;
            let addr = c.bytes()?;
            ColumnValue::Inet(crate::decode::codecs::InetValue {
                family,
                bits,
                is_cidr,
                addr,
            })
        }
        22 => {
            let months = c.i32()?;
            let days = c.i32()?;
            let micros = c.i64()?;
            ColumnValue::Interval(crate::decode::codecs::IntervalValue {
                months,
                days,
                micros,
            })
        }
        23 => ColumnValue::Json(c.string()?),
        24 => {
            let type_oid = c.u32()?;
            let raw = c.bytes()?;
            ColumnValue::PgPending { type_oid, raw }
        }
        other => {
            return Err(SpillError::Format {
                offset: c.pos,
                detail: format!("unknown ColumnValue tag {other}"),
            });
        }
    };
    Ok(v)
}

fn decode_chunk(c: &mut Cursor) -> Result<ToastChunk> {
    let toast_relid = c.u32()?;
    let value_id = c.u32()?;
    let chunk_seq = c.u32()?;
    let source_lsn = c.u64()?;
    let blkno = c.u32()?;
    let offnum = c.u16()?;
    let chunk_data = bytes::Bytes::from(c.bytes()?);
    Ok(ToastChunk {
        toast_relid,
        value_id,
        chunk_seq,
        source_lsn,
        blkno,
        offnum,
        chunk_data,
    })
}

fn decode_toast_delete(c: &mut Cursor) -> Result<ToastDelete> {
    Ok(ToastDelete {
        toast_relid: c.u32()?,
        blkno: c.u32()?,
        offnum: c.u16()?,
        source_lsn: c.u64()?,
    })
}

fn decode_raw(c: &mut Cursor) -> Result<RawRecord> {
    let rm = c.u8()?;
    let info = c.u8()?;
    let source_lsn = c.u64()?;
    let page_magic = c.u16()?;
    let main_data = c.bytes()?;
    let nblocks = c.u32()? as usize;
    let mut blocks = Vec::with_capacity(nblocks);
    for _ in 0..nblocks {
        blocks.push(RawBlock {
            block_id: c.u8()?,
            fork_flags: c.u8()?,
            data_length: c.u16()?,
            image_length: c.u16()?,
            hole_offset: c.u16()?,
            hole_length: c.u16()?,
            bimg_info: c.u8()?,
            spc_node: c.u32()?,
            db_node: c.u32()?,
            rel_node: c.u32()?,
            block_no: c.u32()?,
            image: c.bytes()?,
            data: c.bytes()?,
        });
    }
    Ok(RawRecord {
        rm,
        info,
        source_lsn,
        page_magic,
        main_data,
        blocks,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn sample_heap(xid: u32, lsn: u64) -> DecodedHeap {
        DecodedHeap {
            rfn: RelFileNode {
                spc_node: 1663,
                db_node: 5,
                rel_node: 16385,
            },
            xid,
            source_lsn: lsn,
            op: HeapOp::Insert,
            new: Some(DecodedTuple {
                columns: vec![
                    Some(ColumnValue::Int4(7)),
                    Some(ColumnValue::Text("hello".into())),
                    None,
                    Some(ColumnValue::Null),
                    Some(ColumnValue::ExternalToast(ToastPointer {
                        va_rawsize: 1024,
                        va_extinfo: 0x80000200,
                        va_valueid: 99,
                        va_toastrelid: 16400,
                    })),
                ],
                partial: false,
            }),
            old: None,
        }
    }

    fn sample_chunk(value_id: u32, seq: u32, lsn: u64, body: &[u8]) -> ToastChunk {
        ToastChunk {
            toast_relid: 16400,
            value_id,
            chunk_seq: seq,
            source_lsn: lsn,
            blkno: 12,
            offnum: 4,
            chunk_data: bytes::Bytes::copy_from_slice(body),
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn round_trip_heap_and_chunk() {
        let tmp = tempdir().unwrap();
        let store = SpillStore::new(tmp.path().to_path_buf()).unwrap();
        let mut w = store.writer(42, 0x1000).await.unwrap();
        let h = sample_heap(42, 0x2000);
        let c = sample_chunk(99, 0, 0x2100, &[0xDE, 0xAD, 0xBE, 0xEF]);
        w.write(&SpillEntry::Heap(Box::new(h.clone())))
            .await
            .unwrap();
        w.write(&SpillEntry::Chunk(c.clone())).await.unwrap();
        let bc = w.byte_count();
        assert!(bc > 0);
        let mut r = w.finish().await.unwrap();
        match r.next().await.unwrap().unwrap() {
            SpillEntry::Heap(b) => {
                assert_eq!(b.xid, 42);
                assert_eq!(b.source_lsn, 0x2000);
                assert_eq!(b.new.as_ref().unwrap().columns.len(), 5);
                let cols = &b.new.as_ref().unwrap().columns;
                assert!(matches!(cols[0], Some(ColumnValue::Int4(7))));
                assert!(matches!(cols[1], Some(ColumnValue::Text(ref t)) if t == "hello"));
                assert!(cols[2].is_none());
                assert!(matches!(cols[3], Some(ColumnValue::Null)));
                match &cols[4] {
                    Some(ColumnValue::ExternalToast(p)) => {
                        assert_eq!(p.va_valueid, 99);
                        assert_eq!(p.va_rawsize, 1024);
                    }
                    other => panic!("expected ExternalToast, got {other:?}"),
                }
            }
            other => panic!("expected Heap, got {other:?}"),
        }
        match r.next().await.unwrap().unwrap() {
            SpillEntry::Chunk(c2) => {
                assert_eq!(c2, c);
            }
            other => panic!("expected Chunk, got {other:?}"),
        }
        assert!(r.next().await.unwrap().is_none(), "EOF expected");
        r.unlink().await.unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn round_trip_toast_delete() {
        let tmp = tempdir().unwrap();
        let store = SpillStore::new(tmp.path().to_path_buf()).unwrap();
        let mut w = store.writer(7, 0x100).await.unwrap();
        let d = ToastDelete {
            toast_relid: 16400,
            blkno: 3,
            offnum: 9,
            source_lsn: 0x2200,
        };
        w.write(&SpillEntry::ToastDelete(d)).await.unwrap();
        let mut r = w.finish().await.unwrap();
        match r.next().await.unwrap().unwrap() {
            SpillEntry::ToastDelete(d2) => assert_eq!(d2, d),
            other => panic!("expected ToastDelete, got {other:?}"),
        }
        assert!(r.next().await.unwrap().is_none());
        r.unlink().await.unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn round_trip_raw_record() {
        let tmp = tempdir().unwrap();
        let store = SpillStore::new(tmp.path().to_path_buf()).unwrap();
        let mut w = store.writer(9, 0x300).await.unwrap();
        let raw = RawRecord {
            rm: 10,
            info: 0x80,
            source_lsn: 0x4242,
            page_magic: 0xD114,
            main_data: vec![1, 0, 8],
            blocks: vec![RawBlock {
                block_id: 0,
                fork_flags: 0x20,
                data_length: 4,
                image_length: 100,
                hole_offset: 24,
                hole_length: 7000,
                bimg_info: 0x05,
                spc_node: 1663,
                db_node: 5,
                rel_node: 24680,
                block_no: 3,
                image: vec![0xAB; 100],
                data: vec![1, 2, 3, 4],
            }],
        };
        w.write(&SpillEntry::Raw(Box::new(raw.clone())))
            .await
            .unwrap();
        let mut r = w.finish().await.unwrap();
        match r.next().await.unwrap().unwrap() {
            SpillEntry::Raw(got) => {
                assert_eq!(*got, raw);
                let rec = got.to_xlog_record();
                assert_eq!(rec.header.resource_manager_id, 10);
                assert_eq!(rec.header.info, 0x80);
                assert_eq!(rec.blocks.len(), 1);
                assert_eq!(rec.blocks[0].header.location.block_no, 3);
                assert_eq!(rec.blocks[0].header.image_header.hole_length, 7000);
                assert_eq!(&*rec.main_data, &[1, 0, 8]);
                assert_eq!(
                    got.rfn(),
                    Some(RelFileNode {
                        spc_node: 1663,
                        db_node: 5,
                        rel_node: 24680,
                    })
                );
            }
            other => panic!("expected Raw, got {other:?}"),
        }
        assert!(r.next().await.unwrap().is_none());
        r.unlink().await.unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn clear_removes_only_spill_files() {
        let tmp = tempdir().unwrap();
        let store = SpillStore::new(tmp.path().to_path_buf()).unwrap();
        let mut w1 = store.writer(1, 0).await.unwrap();
        w1.write(&SpillEntry::Heap(Box::new(sample_heap(1, 0))))
            .await
            .unwrap();
        let mut w2 = store.writer(2, 0).await.unwrap();
        w2.write(&SpillEntry::Heap(Box::new(sample_heap(2, 0))))
            .await
            .unwrap();
        // Drop without finish() so files stay on disk
        drop(w1);
        drop(w2);
        let bystander = tmp.path().join("README");
        tokio::fs::write(&bystander, b"keep me").await.unwrap();
        store.clear().await.unwrap();
        assert!(bystander.exists(), "non-spill file must survive clear()");
        let mut left = tokio::fs::read_dir(tmp.path()).await.unwrap();
        let mut count = 0;
        while let Some(e) = left.next_entry().await.unwrap() {
            let n = e.file_name();
            let s = n.to_str().unwrap();
            assert!(
                !(s.starts_with("xid-") && s.ends_with(".bin")),
                "spill file leaked: {s}"
            );
            count += 1;
        }
        assert!(count >= 1, "README should still be there");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn unlink_on_writer_removes_file() {
        let tmp = tempdir().unwrap();
        let store = SpillStore::new(tmp.path().to_path_buf()).unwrap();
        let mut w = store.writer(7, 0).await.unwrap();
        w.write(&SpillEntry::Heap(Box::new(sample_heap(7, 0))))
            .await
            .unwrap();
        let path = w.path().to_path_buf();
        assert!(path.exists());
        w.unlink().await.unwrap();
        assert!(!path.exists(), "writer.unlink() must remove file");
    }

    #[test]
    fn body_spool_appends_reads_and_survives_unlink() {
        let tmp = tempdir().unwrap();
        let mut w = BodySpoolWriter::create(tmp.path(), 7, 0x1000, None).unwrap();
        let a = w.append(b"hello").unwrap();
        let b = w.append(b"world!").unwrap();
        assert_eq!((a.offset, a.len), (0, 5));
        assert_eq!((b.offset, b.len), (5, 6));
        assert_eq!(w.len(), 11);
        w.flush().unwrap();
        let shared = w.shared().clone();
        assert_eq!(shared.read(a).unwrap(), b"hello");
        assert_eq!(shared.read(b).unwrap(), b"world!");
        let name = shared
            .path()
            .file_name()
            .unwrap()
            .to_string_lossy()
            .into_owned();
        assert!(name.starts_with("toastbody-") && name.ends_with(".bin"));
        let path = shared.path().to_path_buf();
        w.unlink().unwrap();
        assert!(!path.exists(), "unlink removes the name");
        // Open fd pins data: readers holding the Arc stay valid
        assert_eq!(shared.read(b).unwrap(), b"world!");
    }

    /// Unlink drops the pathname only: the spool-bytes gauge stays
    /// charged while a reader view holds the fd (blocks stay allocated),
    /// releasing with the last owner
    #[tokio::test(flavor = "current_thread")]
    async fn spool_gauge_releases_with_last_reader() {
        let tmp = tempdir().unwrap();
        let gauge = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let mut w = BodySpoolWriter::create(tmp.path(), 5, 0x30, Some(gauge.clone())).unwrap();
        w.append(&[7u8; 48]).unwrap();
        w.flush().unwrap();
        let reader = w.shared().clone();
        w.unlink().unwrap();
        assert_eq!(
            gauge.load(std::sync::atomic::Ordering::Relaxed),
            48,
            "open reader pins disk bytes"
        );
        drop(reader);
        assert_eq!(gauge.load(std::sync::atomic::Ordering::Relaxed), 0);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn clear_wipes_body_spools_too() {
        let tmp = tempdir().unwrap();
        let store = SpillStore::new(tmp.path().to_path_buf()).unwrap();
        let mut w = BodySpoolWriter::create(tmp.path(), 3, 0x20, None).unwrap();
        w.append(b"x").unwrap();
        w.flush().unwrap();
        let path = w.shared().path().to_path_buf();
        // Simulate crash: writer dropped without unlink
        drop(w);
        assert!(path.exists());
        store.clear().await.unwrap();
        assert!(!path.exists(), "startup wipe reclaims body spools");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn malformed_tag_surfaces_format_error() {
        let tmp = tempdir().unwrap();
        let path = tmp.path().join("xid-0000000000-0000000000000000.bin");
        // Valid header + tag=255 + len=0 + no body; tag=255 trips entry dispatch
        let mut bytes = Vec::with_capacity(6 + 5);
        bytes.extend_from_slice(&SPILL_MAGIC);
        bytes.extend_from_slice(&SPILL_VERSION.to_le_bytes());
        bytes.extend_from_slice(&[255u8, 0u8, 0u8, 0u8, 0u8]);
        tokio::fs::write(&path, &bytes).await.unwrap();
        let mut r = SpillReader {
            file: OpenOptions::new().read(true).open(&path).await.unwrap(),
            path,
            header_checked: false,
        };
        let err = r.next().await.expect_err("must error on bad tag");
        match err {
            SpillError::Format { detail, .. } => {
                assert!(detail.contains("unknown entry tag"), "{detail}");
            }
            other => panic!("expected Format, got {other:?}"),
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn missing_magic_surfaces_format_error() {
        let tmp = tempdir().unwrap();
        let path = tmp.path().join("xid-0000000000-0000000000000000.bin");
        tokio::fs::write(&path, &[0u8, 0u8, 0u8, 0u8])
            .await
            .unwrap();
        let mut r = SpillReader {
            file: OpenOptions::new().read(true).open(&path).await.unwrap(),
            path,
            header_checked: false,
        };
        let err = r.next().await.expect_err("must error on missing magic");
        match err {
            SpillError::Format { detail, .. } => assert!(detail.contains("bad magic"), "{detail}"),
            other => panic!("expected Format, got {other:?}"),
        }
    }

    #[test]
    fn encode_decode_value_round_trip_for_every_variant() {
        let cases = [
            ColumnValue::Null,
            ColumnValue::Bool(true),
            ColumnValue::Bool(false),
            ColumnValue::Char(-5),
            ColumnValue::Int2(-32000),
            ColumnValue::Int4(-1_000_000),
            ColumnValue::Int8(1 << 40),
            ColumnValue::Float4(std::f32::consts::PI),
            ColumnValue::Float8(std::f64::consts::E),
            ColumnValue::Oid(0xDEADBEEF),
            ColumnValue::Date(-1),
            ColumnValue::Time(86_400_000_000),
            ColumnValue::Timestamp(-1),
            ColumnValue::TimestampTz(0x1234_5678),
            ColumnValue::TimeTz {
                micros: 3600 * 1_000_000,
                tz_seconds: -28800,
            },
            ColumnValue::Uuid([7u8; 16]),
            ColumnValue::Name("nspname".into()),
            ColumnValue::Bytea(vec![0, 1, 2, 3, 4, 5]),
            ColumnValue::Text("héllo, мир ✓".into()),
            ColumnValue::ExternalToast(ToastPointer {
                va_rawsize: 2 * 1024 * 1024,
                va_extinfo: 0x40000300,
                va_valueid: 12345,
                va_toastrelid: 56789,
            }),
            ColumnValue::Unsupported {
                type_oid: 1700,
                raw: vec![0xAB; 32],
            },
        ];
        for v in cases {
            let mut out = Vec::new();
            encode_value(&mut out, &v);
            let mut cur = Cursor::new(&out);
            let decoded = decode_value(&mut cur).unwrap();
            assert_eq!(decoded, v, "round-trip mismatch for {v:?}");
            assert_eq!(cur.pos, out.len(), "trailing bytes for {v:?}");
        }
    }

    #[test]
    fn encode_decode_remaining_value_variants_round_trip() {
        use crate::decode::codecs::{InetValue, IntervalValue, NumericKind, PGSQL_AF_INET6};
        let cases = [
            ColumnValue::Numeric(NumericKind::Finite("3.14".into())),
            ColumnValue::Numeric(NumericKind::NaN),
            ColumnValue::Numeric(NumericKind::PInf),
            ColumnValue::Numeric(NumericKind::NInf),
            ColumnValue::Inet(InetValue {
                family: PGSQL_AF_INET6,
                bits: 128,
                is_cidr: false,
                addr: vec![0xfe, 0x80, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x01],
            }),
            ColumnValue::Interval(IntervalValue {
                months: -3,
                days: 14,
                micros: 123_456,
            }),
            ColumnValue::Json("{\"a\":1}".into()),
            ColumnValue::PgPending {
                type_oid: 3802,
                raw: vec![0xAA, 0xBB, 0xCC],
            },
        ];
        for v in cases {
            let mut out = Vec::new();
            encode_value(&mut out, &v);
            let mut cur = Cursor::new(&out);
            let decoded = decode_value(&mut cur).unwrap();
            assert_eq!(decoded, v, "round-trip mismatch for {v:?}");
            assert_eq!(cur.pos, out.len(), "trailing bytes for {v:?}");
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn round_trip_all_heap_ops() {
        let tmp = tempdir().unwrap();
        let store = SpillStore::new(tmp.path().to_path_buf()).unwrap();
        assert_eq!(store.dir(), tmp.path());
        let mut w = store.writer(11, 0x500).await.unwrap();
        assert!(w.path().exists());
        for op in [
            HeapOp::Insert,
            HeapOp::Update,
            HeapOp::HotUpdate,
            HeapOp::Delete,
            HeapOp::Truncate,
        ] {
            let mut h = sample_heap(11, 0x600);
            h.op = op;
            if matches!(op, HeapOp::Truncate) {
                // Truncate carries no tuple payload
                h.new = None;
                h.old = None;
            } else {
                h.old = Some(DecodedTuple {
                    columns: vec![Some(ColumnValue::Int4(42))],
                    partial: false,
                });
            }
            w.write(&SpillEntry::Heap(Box::new(h))).await.unwrap();
        }
        let mut r = w.finish().await.unwrap();
        assert!(r.path().to_str().unwrap().contains("xid-"));
        let mut ops = Vec::new();
        while let Some(e) = r.next().await.unwrap() {
            if let SpillEntry::Heap(b) = e {
                ops.push(b.op);
            }
        }
        assert_eq!(
            ops,
            vec![
                HeapOp::Insert,
                HeapOp::Update,
                HeapOp::HotUpdate,
                HeapOp::Delete,
                HeapOp::Truncate,
            ],
        );
        r.unlink().await.unwrap();
    }

    #[test]
    fn cursor_short_read_surfaces_format_error() {
        let mut cur = Cursor::new(&[1u8, 2]);
        let err = cur.u32().unwrap_err();
        assert!(matches!(err, SpillError::Format { offset: 0, .. }));
    }

    #[test]
    fn cursor_string_rejects_invalid_utf8() {
        // len 2, body 0xFF 0xFE invalid UTF-8
        let buf = [2u8, 0, 0, 0, 0xFF, 0xFE];
        let mut cur = Cursor::new(&buf);
        let err = cur.string().unwrap_err();
        assert!(matches!(err, SpillError::Format { .. }));
    }

    #[test]
    fn decode_value_rejects_unknown_tag() {
        let mut cur = Cursor::new(&[99u8]);
        let err = decode_value(&mut cur).unwrap_err();
        match err {
            SpillError::Format { detail, .. } => {
                assert!(detail.contains("unknown ColumnValue tag"), "{detail}");
            }
            other => panic!("expected Format, got {other:?}"),
        }
    }

    #[test]
    fn decode_value_rejects_unknown_numeric_kind() {
        // tag=20 Numeric, bad NumericKind sub-tag
        let buf = [20u8, 99u8];
        let mut cur = Cursor::new(&buf);
        let err = decode_value(&mut cur).unwrap_err();
        match err {
            SpillError::Format { detail, .. } => {
                assert!(detail.contains("unknown NumericKind tag"), "{detail}");
            }
            other => panic!("expected Format, got {other:?}"),
        }
    }

    #[test]
    fn decode_heap_rejects_unknown_op_tag() {
        // 4 u32 (spc/db/rel/xid) + u64 source_lsn = 24 bytes, then bad op 99
        let mut buf = vec![0u8; 24];
        buf.push(99u8);
        let mut cur = Cursor::new(&buf);
        let err = decode_heap(&mut cur).unwrap_err();
        match err {
            SpillError::Format { detail, .. } => {
                assert!(detail.contains("unknown HeapOp tag"), "{detail}");
            }
            other => panic!("expected Format, got {other:?}"),
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn writer_unlink_tolerates_already_gone() {
        let tmp = tempdir().unwrap();
        let store = SpillStore::new(tmp.path().to_path_buf()).unwrap();
        let w = store.writer(13, 0).await.unwrap();
        let path = w.path().to_path_buf();
        // Remove file out from under the writer; unlink() must swallow NotFound
        tokio::fs::remove_file(&path).await.unwrap();
        w.unlink().await.unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn reader_unlink_tolerates_already_gone() {
        let tmp = tempdir().unwrap();
        let store = SpillStore::new(tmp.path().to_path_buf()).unwrap();
        let mut w = store.writer(14, 0).await.unwrap();
        w.write(&SpillEntry::Heap(Box::new(sample_heap(14, 0))))
            .await
            .unwrap();
        let r = w.finish().await.unwrap();
        tokio::fs::remove_file(r.path()).await.unwrap();
        r.unlink().await.unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn clear_returns_ok_when_dir_vanished() {
        let tmp = tempdir().unwrap();
        let store = SpillStore::new(tmp.path().to_path_buf()).unwrap();
        // read_dir returns NotFound, clear must absorb
        tokio::fs::remove_dir_all(tmp.path()).await.unwrap();
        store.clear().await.unwrap();
    }
}
