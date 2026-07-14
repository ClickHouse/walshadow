//! Deferred-record spool: bounded-memory replacement for scan-sized `Vec`s.
//!
//! Bootstrap and backup gates defer tuples until walk EOF; count is not a
//! memory bound, so past a byte threshold records append to a versioned
//! file and replay sequentially. Files are disposable derived state
//! ([`crate::spill`] crash-recovery contract): no fsync, startup wipe safe,
//! failure leaves replay covered by the WAL cursor.
//!
//! ```text
//! [2 bytes "WD" magic] [u16 LE version] then repeating:
//! [u32 len LE] [body of `len` bytes]
//! ```
//!
//! Separate magic/version from the xact spill: formats evolve
//! independently, a cross-read surfaces as [`SpillError::Format`].

use std::path::PathBuf;

use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};

use crate::backup_page_walk::BackfillTuple;
use crate::spill::{
    Cursor, SpillError, decode_value, encode_value, push_u8, push_u16, push_u32, push_u64,
};
use walrus::pg::walparser::RelFileNode;

const SPOOL_MAGIC: [u8; 2] = *b"WD";
const SPOOL_VERSION: u16 = 1;
/// Writer coalescing buffer flush threshold
const WRITE_BUF: usize = 256 << 10;
/// Default in-memory prefix budget before records spill to file
pub const DEFERRED_SPOOL_MEM_MAX: usize = 8 << 20;

type Result<T> = std::result::Result<T, SpillError>;

/// Append-only deferred store: small in-memory prefix, file past `mem_max`.
/// Insertion order preserved; once the file exists every record (prefix
/// included) lives there.
pub struct DeferredSpool {
    mem: Vec<BackfillTuple>,
    mem_bytes: usize,
    mem_max: usize,
    path: PathBuf,
    file: Option<File>,
    buf: Vec<u8>,
    records: u64,
    spooled_bytes: u64,
}

impl DeferredSpool {
    /// `path` is created lazily at first overflow; parent dir must exist or
    /// be creatable
    pub fn new(path: PathBuf, mem_max: usize) -> Self {
        Self {
            mem: Vec::new(),
            mem_bytes: 0,
            mem_max,
            path,
            file: None,
            buf: Vec::new(),
            records: 0,
            spooled_bytes: 0,
        }
    }

    pub fn records(&self) -> u64 {
        self.records
    }

    /// Bytes retained in the in-memory prefix
    pub fn resident_bytes(&self) -> usize {
        self.mem_bytes
    }

    /// Encoded bytes written to the spool file
    pub fn spooled_bytes(&self) -> u64 {
        self.spooled_bytes
    }

    pub async fn push(&mut self, value: BackfillTuple) -> Result<()> {
        self.records += 1;
        if self.file.is_none() {
            let value_bytes = approx_bytes(&value);
            if self.mem_bytes + value_bytes <= self.mem_max {
                self.mem_bytes += value_bytes;
                self.mem.push(value);
                return Ok(());
            }
            self.create_and_flush_prefix().await?;
        }
        self.append(&value)?;
        if self.buf.len() >= WRITE_BUF {
            self.flush_buf().await?;
        }
        Ok(())
    }

    async fn create_and_flush_prefix(&mut self) -> Result<()> {
        if let Some(parent) = self.path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&self.path)
            .await?;
        self.buf.extend_from_slice(&SPOOL_MAGIC);
        push_u16(&mut self.buf, SPOOL_VERSION);
        self.file = Some(file);
        for v in std::mem::take(&mut self.mem) {
            self.append(&v)?;
            if self.buf.len() >= WRITE_BUF {
                self.flush_buf().await?;
            }
        }
        self.mem_bytes = 0;
        Ok(())
    }

    fn append(&mut self, value: &BackfillTuple) -> Result<()> {
        let len_at = self.buf.len();
        push_u32(&mut self.buf, 0);
        let body_at = self.buf.len();
        encode_record(value, &mut self.buf);
        let len = (self.buf.len() - body_at) as u32;
        self.buf[len_at..body_at].copy_from_slice(&len.to_le_bytes());
        self.spooled_bytes += 4 + u64::from(len);
        Ok(())
    }

    async fn flush_buf(&mut self) -> Result<()> {
        let file = self.file.as_mut().expect("flush without file");
        file.write_all(&self.buf).await?;
        self.buf.clear();
        Ok(())
    }

    /// Seal writes, hand back a sequential reader
    pub async fn into_reader(mut self) -> Result<DeferredReader> {
        let src = match self.file.take() {
            Some(mut file) => {
                if !self.buf.is_empty() {
                    file.write_all(&self.buf).await?;
                }
                file.flush().await?;
                drop(file);
                let bytes = tokio::fs::metadata(&self.path).await?.len();
                ReadSrc::File {
                    reader: open_validated(&self.path).await?,
                    path: self.path,
                    remaining_bytes: bytes.saturating_sub(4),
                }
            }
            None => ReadSrc::Mem(self.mem.into_iter()),
        };
        Ok(DeferredReader { src })
    }

    /// Drop without replay (walk failure); unlink any file
    pub async fn discard(mut self) {
        if self.file.take().is_some() {
            let _ = tokio::fs::remove_file(&self.path).await;
        }
    }
}

/// Open a spool file and validate its header
async fn open_validated(path: &std::path::Path) -> Result<BufReader<File>> {
    let mut reader = BufReader::new(File::open(path).await?);
    let mut header = [0u8; 4];
    reader.read_exact(&mut header).await?;
    if header[..2] != SPOOL_MAGIC {
        return Err(SpillError::Format {
            offset: 0,
            detail: format!("bad spool magic {:02x}{:02x}", header[0], header[1]),
        });
    }
    let version = u16::from_le_bytes(header[2..4].try_into().unwrap());
    if version != SPOOL_VERSION {
        return Err(SpillError::Format {
            offset: 2,
            detail: format!("spool version {version}, expected {SPOOL_VERSION}"),
        });
    }
    Ok(reader)
}

enum ReadSrc {
    Mem(std::vec::IntoIter<BackfillTuple>),
    File {
        reader: BufReader<File>,
        path: PathBuf,
        /// File bytes past the header not yet consumed; bounds each
        /// record length before its buffer allocates
        remaining_bytes: u64,
    },
}

/// Sequential replay in insertion order; truncation or corruption is a
/// deterministic [`SpillError::Format`]
pub struct DeferredReader {
    src: ReadSrc,
}

impl DeferredReader {
    pub async fn next(&mut self) -> Result<Option<BackfillTuple>> {
        match &mut self.src {
            ReadSrc::Mem(it) => Ok(it.next()),
            ReadSrc::File {
                reader,
                remaining_bytes,
                ..
            } => {
                if *remaining_bytes == 0 {
                    return Ok(None);
                }
                let mut len = [0u8; 4];
                reader.read_exact(&mut len).await.map_err(truncated)?;
                let len = u64::from(u32::from_le_bytes(len));
                // A corrupt length must surface as a format error, not a
                // multi-GiB allocation attempt
                if 4 + len > *remaining_bytes {
                    return Err(SpillError::Format {
                        offset: 0,
                        detail: format!(
                            "record len {len} exceeds remaining spool bytes {}",
                            remaining_bytes.saturating_sub(4),
                        ),
                    });
                }
                *remaining_bytes -= 4 + len;
                let mut body = vec![0u8; len as usize];
                reader.read_exact(&mut body).await.map_err(truncated)?;
                Ok(Some(decode_record(&body)?))
            }
        }
    }

    /// Unlink the spool file after successful replay
    pub async fn finish(self) -> Result<()> {
        if let ReadSrc::File { reader, path, .. } = self.src {
            drop(reader);
            tokio::fs::remove_file(&path).await?;
        }
        Ok(())
    }
}

fn truncated(e: std::io::Error) -> SpillError {
    if e.kind() == std::io::ErrorKind::UnexpectedEof {
        SpillError::Format {
            offset: 0,
            detail: "spool truncated mid-record".into(),
        }
    } else {
        SpillError::Io(e)
    }
}

fn approx_bytes(value: &BackfillTuple) -> usize {
    std::mem::size_of::<BackfillTuple>()
        + value
            .columns
            .iter()
            .flatten()
            .map(crate::heap_decoder::ColumnValue::approx_bytes)
            .sum::<usize>()
}

fn encode_record(value: &BackfillTuple, out: &mut Vec<u8>) {
    push_u32(out, value.rfn.spc_node);
    push_u32(out, value.rfn.db_node);
    push_u32(out, value.rfn.rel_node);
    push_u32(out, value.xid);
    push_u32(out, value.xmax);
    push_u16(out, value.infomask);
    push_u64(out, value.source_lsn);
    push_u32(out, value.blkno);
    push_u16(out, value.offnum);
    push_u32(out, value.columns.len() as u32);
    for col in &value.columns {
        match col {
            None => push_u8(out, 0),
            Some(v) => {
                push_u8(out, 1);
                encode_value(out, v);
            }
        }
    }
}

fn decode_record(buf: &[u8]) -> Result<BackfillTuple> {
    let c = &mut Cursor::new(buf);
    let rfn = RelFileNode {
        spc_node: c.u32()?,
        db_node: c.u32()?,
        rel_node: c.u32()?,
    };
    let xid = c.u32()?;
    let xmax = c.u32()?;
    let infomask = c.u16()?;
    let source_lsn = c.u64()?;
    let blkno = c.u32()?;
    let offnum = c.u16()?;
    let ncols = c.u32()? as usize;
    let mut columns = Vec::with_capacity(ncols);
    for _ in 0..ncols {
        columns.push(match c.u8()? {
            0 => None,
            _ => Some(decode_value(c)?),
        });
    }
    Ok(BackfillTuple {
        rfn,
        xid,
        xmax,
        infomask,
        source_lsn,
        blkno,
        offnum,
        columns,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::heap_decoder::ColumnValue;
    use tempfile::tempdir;

    fn tuple(lsn: u64, payload: &[u8]) -> BackfillTuple {
        BackfillTuple {
            rfn: RelFileNode {
                spc_node: 1663,
                db_node: 5,
                rel_node: 16400,
            },
            xid: 100,
            xmax: 0,
            infomask: 0x0900,
            source_lsn: lsn,
            blkno: 3,
            offnum: 7,
            columns: vec![
                Some(ColumnValue::Int4(1)),
                None,
                Some(ColumnValue::Bytea(payload.to_vec())),
            ],
        }
    }

    async fn drain_all(spool: DeferredSpool) -> Vec<BackfillTuple> {
        let mut reader = spool.into_reader().await.unwrap();
        let mut out = Vec::new();
        while let Some(t) = reader.next().await.unwrap() {
            out.push(t);
        }
        reader.finish().await.unwrap();
        out
    }

    #[tokio::test(flavor = "current_thread")]
    async fn round_trip_stays_in_memory_under_threshold() {
        let tmp = tempdir().unwrap();
        let path = tmp.path().join("gate.bin");
        let mut spool = DeferredSpool::new(path.clone(), 1 << 20);
        for i in 0..10u64 {
            spool.push(tuple(0x1000 + i, b"abc")).await.unwrap();
        }
        assert_eq!(spool.records(), 10);
        assert!(spool.resident_bytes() > 0);
        assert_eq!(spool.spooled_bytes(), 0);
        assert!(!path.exists(), "no file under threshold");
        let out = drain_all(spool).await;
        assert_eq!(out.len(), 10);
        assert_eq!(out[3].source_lsn, 0x1003);
        assert_eq!(out[3].columns[2], Some(ColumnValue::Bytea(b"abc".to_vec())));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn overflow_moves_prefix_to_file_in_order() {
        let tmp = tempdir().unwrap();
        let path = tmp.path().join("gate.bin");
        // Threshold admits ~2 small tuples, third pushes all to file
        let mut spool = DeferredSpool::new(path.clone(), 2 * approx_bytes(&tuple(0, b"abc")));
        for i in 0..50u64 {
            spool.push(tuple(0x2000 + i, b"abc")).await.unwrap();
        }
        assert!(path.exists(), "threshold crossed, file created");
        assert_eq!(spool.resident_bytes(), 0, "prefix flushed");
        assert!(spool.spooled_bytes() > 0);
        let out = drain_all(spool).await;
        assert_eq!(out.len(), 50);
        assert!(
            out.iter()
                .enumerate()
                .all(|(i, t)| t.source_lsn == 0x2000 + i as u64),
            "insertion order preserved across prefix flush"
        );
        assert!(!path.exists(), "finish unlinks");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn truncated_file_is_deterministic_format_error() {
        let tmp = tempdir().unwrap();
        let path = tmp.path().join("gate.bin");
        let mut spool = DeferredSpool::new(path.clone(), 0);
        for i in 0..5u64 {
            spool.push(tuple(0x3000 + i, b"abcdefgh")).await.unwrap();
        }
        // Seal, then truncate mid-record behind the reader's back
        drop(spool.into_reader().await.unwrap());
        let len = std::fs::metadata(&path).unwrap().len();
        let f = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
        f.set_len(len - 6).unwrap();
        let mut reader = DeferredReader {
            src: ReadSrc::File {
                reader: open_validated(&path).await.unwrap(),
                path,
                remaining_bytes: len - 6 - 4,
            },
        };
        let mut seen = 0;
        let err = loop {
            match reader.next().await {
                Ok(Some(_)) => seen += 1,
                Ok(None) => panic!("truncation must error, not end"),
                Err(e) => break e,
            }
        };
        assert!(seen < 5);
        assert!(matches!(err, SpillError::Format { .. }));
    }

    /// A corrupt record length larger than the file is a typed format
    /// error at the length check, never a giant allocation
    #[tokio::test(flavor = "current_thread")]
    async fn corrupt_record_length_bounds_before_allocation() {
        let tmp = tempdir().unwrap();
        let path = tmp.path().join("gate.bin");
        let mut spool = DeferredSpool::new(path.clone(), 0);
        spool.push(tuple(0x5000, b"abcdefgh")).await.unwrap();
        drop(spool.into_reader().await.unwrap());
        // Overwrite the first record's length with u32::MAX
        {
            use std::io::{Seek, SeekFrom, Write};
            let mut f = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
            f.seek(SeekFrom::Start(4)).unwrap();
            f.write_all(&u32::MAX.to_le_bytes()).unwrap();
        }
        let bytes = std::fs::metadata(&path).unwrap().len();
        let mut reader = DeferredReader {
            src: ReadSrc::File {
                reader: open_validated(&path).await.unwrap(),
                path,
                remaining_bytes: bytes - 4,
            },
        };
        let err = reader.next().await.expect_err("corrupt length surfaces");
        match err {
            SpillError::Format { detail, .. } => {
                assert!(detail.contains("exceeds remaining"), "{detail}");
            }
            other => panic!("expected Format, got {other:?}"),
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn discard_unlinks_without_replay() {
        let tmp = tempdir().unwrap();
        let path = tmp.path().join("gate.bin");
        let mut spool = DeferredSpool::new(path.clone(), 0);
        spool.push(tuple(0x4000, b"abc")).await.unwrap();
        assert!(path.exists());
        spool.discard().await;
        assert!(!path.exists());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn foreign_header_rejected() {
        let tmp = tempdir().unwrap();
        let magic = tmp.path().join("magic.bin");
        std::fs::write(&magic, b"WSxx").unwrap();
        assert!(matches!(
            open_validated(&magic).await,
            Err(SpillError::Format { offset: 0, .. })
        ));
        let version = tmp.path().join("version.bin");
        std::fs::write(&version, [b'W', b'D', 0xFF, 0x00]).unwrap();
        assert!(matches!(
            open_validated(&version).await,
            Err(SpillError::Format { offset: 2, .. })
        ));
    }
}
