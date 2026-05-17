//! Phase 6 — per-xact append-only spill backend.
//!
//! Mirrors PG's `pg_replslot/<slot>/xid-*.snap` shape: one file per
//! buffered xid under `{data_dir}/spill/`. The xact buffer flushes its
//! in-memory queue here once `xact_buffer_max` is breached; commit drain
//! reads the file back in WAL order then unlinks. Abort just unlinks.
//!
//! ## Why a custom binary encoder
//!
//! Spill entries are [`DecodedHeap`] tuples and reassembled
//! [`ToastChunk`]s. JSON inflates the bytea / chunk_data path 3–4× and
//! the workload (bulk INSERT/UPDATE) is exactly when that hurts. Adding
//! `bincode` / `postcard` for a contained format with no
//! version-skew surface is dead weight. Manual length-prefixed binary
//! keeps the cost local: ~150 LOC of explicit byte layout, every
//! decode failure surfaces as [`SpillError::Format`] with a precise
//! offset.
//!
//! ## On-disk layout
//!
//! ```text
//! [u8 tag] [u32 len LE] [body of `len` bytes]
//!   tag = 0 → SpillEntry::Heap   (body = encoded DecodedHeap)
//!   tag = 1 → SpillEntry::Chunk  (body = encoded ToastChunk)
//! ```
//!
//! The outer length lets [`SpillReader::next`] skip a malformed body
//! without forcing the whole file. v1 propagates any malformation as
//! [`SpillError::Format`] — the xact is unrecoverable anyway, the
//! caller's only sane response is to drop the buffer.
//!
//! ## Eviction policy
//!
//! Largest-xact-first, mirroring PG `ReorderBufferLargestTXN`
//! (`~/s/postgresql/src/backend/replication/logical/reorderbuffer.c`
//! L3789). The xact buffer owns the policy decision; [`SpillStore`]
//! just lays out the files.
//!
//! ## Crash recovery
//!
//! Startup clears the spill dir wholesale via [`SpillStore::clear`].
//! Cursor file commits atomically post-drain, so on-disk state is
//! always "drained-and-in-CH" or "replayable-from-WAL-cursor". No
//! attempt to replay partial spill files — re-streaming from the
//! cursor LSN is cheaper than verifying the spill's integrity.

use std::io;
use std::path::{Path, PathBuf};

use thiserror::Error;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use wal_rs::pg::walparser::RelFileNode;

use crate::heap_decoder::{
    ColumnValue, DecodedHeap, DecodedTuple, HeapOp, ToastPointer,
};

/// One unit the buffer wants to durably stash. Heap tuples and TOAST
/// chunks share the file because both flush together at commit drain;
/// keeping the file ordering aligned with WAL order means the drain
/// pass is a single linear read.
#[derive(Debug, Clone, PartialEq)]
pub enum SpillEntry {
    Heap(Box<DecodedHeap>),
    Chunk(ToastChunk),
}

/// One TOAST chunk extracted from a `pg_toast.pg_toast_<oid>` INSERT.
/// PG `toast_save_datum` writes chunks as `(chunk_id oid, chunk_seq
/// int4, chunk_data bytea)`; the buffer holds them keyed by
/// `(toast_relid, value_id)` and concatenates by `chunk_seq` at drain.
#[derive(Debug, Clone, PartialEq)]
pub struct ToastChunk {
    /// `RelFileNode.rel_node` of the toast relation the chunk lives on.
    /// `va_toastrelid` on the referring tuple's [`ToastPointer`] matches
    /// this field.
    pub toast_relid: u32,
    /// `chunk_id` — matches `va_valueid`.
    pub value_id: u32,
    /// Per-value chunk index, 0-based. PG writes sequentially.
    pub chunk_seq: u32,
    /// Source-WAL LSN where the chunk record sat. Keeps WAL-order
    /// drain stable across spilled + in-memory chunks for the same
    /// value.
    pub source_lsn: u64,
    /// Raw bytea body of the chunk — `TOAST_MAX_CHUNK_SIZE` ≈ 1996
    /// bytes typical, last chunk shorter.
    pub chunk_data: Vec<u8>,
}

#[derive(Debug, Error)]
pub enum SpillError {
    #[error("io: {0}")]
    Io(#[from] io::Error),
    #[error("spill format at offset {offset}: {detail}")]
    Format { offset: usize, detail: String },
}

pub type Result<T> = std::result::Result<T, SpillError>;

/// Filesystem layout owner. Creates the dir if missing; offers a
/// `writer(xid, first_lsn)` factory and a startup `clear`.
pub struct SpillStore {
    dir: PathBuf,
}

impl SpillStore {
    /// Open or create `dir`. Synchronous on purpose — called once at
    /// daemon startup before the tokio runtime gets busy.
    pub fn new(dir: PathBuf) -> Result<Self> {
        std::fs::create_dir_all(&dir)?;
        Ok(Self { dir })
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// Open a fresh per-xid spill file. Filename includes `first_lsn`
    /// so two streams that picked up the same xid value after a slot
    /// rotation don't collide on disk.
    pub async fn writer(&self, xid: u32, first_lsn: u64) -> Result<SpillWriter> {
        let path = self
            .dir
            .join(format!("xid-{xid:010}-{first_lsn:016X}.bin"));
        let file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .read(true)
            .open(&path)
            .await?;
        Ok(SpillWriter {
            file,
            path,
            byte_count: 0,
        })
    }

    /// Wipe every file under the spill dir. Called at daemon startup
    /// per [PHASE6disk.md]("Crash recovery"): on-disk state is always
    /// either drained-into-CH or replayable from the cursor's
    /// `decoder_lsn`, so leftover spill files from a prior crash are
    /// safe to discard.
    pub async fn clear(&self) -> Result<()> {
        let mut entries = match tokio::fs::read_dir(&self.dir).await {
            Ok(e) => e,
            // Dir was just created or vanished; nothing to clear.
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(()),
            Err(e) => return Err(e.into()),
        };
        while let Some(entry) = entries.next_entry().await? {
            let p = entry.path();
            if p.file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|s| s.starts_with("xid-") && s.ends_with(".bin"))
            {
                let _ = tokio::fs::remove_file(&p).await;
            }
        }
        Ok(())
    }
}

/// Append-only writer for one xact's spill file.
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
        match entry {
            SpillEntry::Heap(h) => {
                body.push(0u8);
                let inner = encode_heap(h);
                push_u32(&mut body, inner.len() as u32);
                body.extend_from_slice(&inner);
            }
            SpillEntry::Chunk(c) => {
                body.push(1u8);
                let inner = encode_chunk(c);
                push_u32(&mut body, inner.len() as u32);
                body.extend_from_slice(&inner);
            }
        }
        self.file.write_all(&body).await?;
        self.byte_count += body.len() as u64;
        Ok(())
    }

    /// Flush + close the writer, return a reader positioned at the
    /// beginning. Caller's responsibility to drive `next()` until
    /// `Ok(None)` and then `unlink()`.
    pub async fn finish(mut self) -> Result<SpillReader> {
        self.file.flush().await?;
        self.file.sync_all().await?;
        drop(self.file);
        let file = OpenOptions::new().read(true).open(&self.path).await?;
        Ok(SpillReader {
            file,
            path: self.path,
        })
    }

    /// Abort path: drop the file unread. Renamed from `discard` so the
    /// intent is obvious at call sites.
    pub async fn unlink(self) -> Result<()> {
        drop(self.file);
        match tokio::fs::remove_file(&self.path).await {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e.into()),
        }
    }
}

/// Streaming reader for one xact's spill file.
pub struct SpillReader {
    file: File,
    path: PathBuf,
}

impl SpillReader {
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// One entry per call. Returns `Ok(None)` at clean EOF.
    pub async fn next(&mut self) -> Result<Option<SpillEntry>> {
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
        drop(self.file);
        match tokio::fs::remove_file(&self.path).await {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e.into()),
        }
    }
}

// ── encoding ────────────────────────────────────────────────────────

fn push_u8(out: &mut Vec<u8>, v: u8) {
    out.push(v);
}
fn push_u16(out: &mut Vec<u8>, v: u16) {
    out.extend_from_slice(&v.to_le_bytes());
}
fn push_u32(out: &mut Vec<u8>, v: u32) {
    out.extend_from_slice(&v.to_le_bytes());
}
fn push_i32(out: &mut Vec<u8>, v: i32) {
    out.extend_from_slice(&v.to_le_bytes());
}
fn push_u64(out: &mut Vec<u8>, v: u64) {
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

fn encode_heap(h: &DecodedHeap) -> Vec<u8> {
    let mut out = Vec::with_capacity(64);
    push_u32(&mut out, h.rfn.spc_node);
    push_u32(&mut out, h.rfn.db_node);
    push_u32(&mut out, h.rfn.rel_node);
    push_u32(&mut out, h.xid);
    push_u64(&mut out, h.source_lsn);
    let op_byte: u8 = match h.op {
        HeapOp::Insert => 0,
        HeapOp::Update => 1,
        HeapOp::HotUpdate => 2,
        HeapOp::Delete => 3,
    };
    push_u8(&mut out, op_byte);
    encode_opt_tuple(&mut out, h.new.as_ref());
    encode_opt_tuple(&mut out, h.old.as_ref());
    out
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

fn encode_value(out: &mut Vec<u8>, v: &ColumnValue) {
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
    }
}

fn encode_chunk(c: &ToastChunk) -> Vec<u8> {
    let mut out = Vec::with_capacity(32 + c.chunk_data.len());
    push_u32(&mut out, c.toast_relid);
    push_u32(&mut out, c.value_id);
    push_u32(&mut out, c.chunk_seq);
    push_u64(&mut out, c.source_lsn);
    push_bytes(&mut out, &c.chunk_data);
    out
}

// ── decoding ────────────────────────────────────────────────────────

struct Cursor<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn new(buf: &'a [u8]) -> Self {
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

    fn u8(&mut self) -> Result<u8> {
        Ok(self.need(1)?[0])
    }
    fn u16(&mut self) -> Result<u16> {
        Ok(u16::from_le_bytes(self.need(2)?.try_into().unwrap()))
    }
    fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_le_bytes(self.need(4)?.try_into().unwrap()))
    }
    fn i32(&mut self) -> Result<i32> {
        Ok(i32::from_le_bytes(self.need(4)?.try_into().unwrap()))
    }
    fn u64(&mut self) -> Result<u64> {
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
    fn bytes(&mut self) -> Result<Vec<u8>> {
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

fn decode_value(c: &mut Cursor) -> Result<ColumnValue> {
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
        14 => {
            let bs = c.need(16)?;
            let mut a = [0u8; 16];
            a.copy_from_slice(bs);
            ColumnValue::Uuid(a)
        }
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
    let chunk_data = c.bytes()?;
    Ok(ToastChunk {
        toast_relid,
        value_id,
        chunk_seq,
        source_lsn,
        chunk_data,
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
            chunk_data: body.to_vec(),
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn round_trip_heap_and_chunk() {
        let tmp = tempdir().unwrap();
        let store = SpillStore::new(tmp.path().to_path_buf()).unwrap();
        let mut w = store.writer(42, 0x1000).await.unwrap();
        let h = sample_heap(42, 0x2000);
        let c = sample_chunk(99, 0, 0x2100, &[0xDE, 0xAD, 0xBE, 0xEF]);
        w.write(&SpillEntry::Heap(Box::new(h.clone()))).await.unwrap();
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
    async fn clear_removes_only_spill_files() {
        let tmp = tempdir().unwrap();
        let store = SpillStore::new(tmp.path().to_path_buf()).unwrap();
        let mut w1 = store.writer(1, 0).await.unwrap();
        w1.write(&SpillEntry::Heap(Box::new(sample_heap(1, 0)))).await.unwrap();
        let mut w2 = store.writer(2, 0).await.unwrap();
        w2.write(&SpillEntry::Heap(Box::new(sample_heap(2, 0)))).await.unwrap();
        // Drop the writers without finish() so files stay on disk.
        drop(w1);
        drop(w2);
        // Non-spill file in the same dir survives the clear.
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
        w.write(&SpillEntry::Heap(Box::new(sample_heap(7, 0)))).await.unwrap();
        let path = w.path().to_path_buf();
        assert!(path.exists());
        w.unlink().await.unwrap();
        assert!(!path.exists(), "writer.unlink() must remove file");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn malformed_tag_surfaces_format_error() {
        let tmp = tempdir().unwrap();
        let path = tmp.path().join("xid-0000000000-0000000000000000.bin");
        // tag=255 + len=0 + no body
        tokio::fs::write(&path, &[255u8, 0u8, 0u8, 0u8, 0u8])
            .await
            .unwrap();
        let mut r = SpillReader {
            file: OpenOptions::new().read(true).open(&path).await.unwrap(),
            path,
        };
        let err = r.next().await.expect_err("must error on bad tag");
        match err {
            SpillError::Format { detail, .. } => {
                assert!(detail.contains("unknown entry tag"), "{detail}");
            }
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
}
