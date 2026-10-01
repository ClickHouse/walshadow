//! Deferred-record spool: bounded-memory replacement for scan-sized `Vec`s.
//!
//! Bootstrap and backup gates defer tuples until walk EOF; count is not a
//! memory bound, so past a byte threshold records append to a versioned
//! file and replay sequentially. Records stay unsynced until
//! [`DeferredSpool::checkpoint`], which fsyncs them for a resumed load; a
//! spool no checkpoint names is disposable derived state
//! ([`crate::xact::spill`] crash-recovery contract)
//!
//! ```text
//! [2 bytes "WD" magic] [u16 LE version] then repeating:
//! [u32 len LE] [body of `len` bytes]
//! ```
//!
//! Separate magic/version from the xact spill: formats evolve
//! independently, a cross-read surfaces as [`SpillError::Format`].

use std::io::SeekFrom;
use std::path::PathBuf;

use tokio::fs::OpenOptions;
use tokio::io::AsyncSeekExt;

use crate::backfill::backup_page_walk::BackfillTuple;
use crate::xact::spill::{
    ChunkReader, ChunkWriter, Cursor, IO_CHUNK, SpillError, decode_value, encode_value, push_u8,
    push_u16, push_u32, push_u64,
};
use walrus::pg::walparser::RelFileNode;

const SPOOL_MAGIC: [u8; 2] = *b"WD";
const SPOOL_VERSION: u16 = 1;
/// Default in-memory prefix budget before records spill to file
pub const DEFERRED_SPOOL_MEM_MAX: usize = 8 << 20;

type Result<T> = std::result::Result<T, SpillError>;

/// Append-only deferred store: small in-memory prefix, file past `mem_max`.
/// Insertion order preserved; once the file exists every record (prefix
/// included) lives there.
pub struct DeferredSpool {
    store: Store,
    mem_max: usize,
    path: PathBuf,
    records: u64,
    spooled_bytes: u64,
}

enum Store {
    Mem {
        tuples: Vec<BackfillTuple>,
        bytes: usize,
    },
    File(ChunkWriter),
}

/// Checkpointed spool reopened read-only at an acknowledged record boundary
#[derive(Debug)]
pub struct ResumedSpool {
    path: PathBuf,
    records: u64,
    spooled_bytes: u64,
    read_offset: u64,
}

/// Deferred records ready to replay
#[derive(Debug)]
pub enum Replayable {
    Open(DeferredSpool),
    Resumed(ResumedSpool),
}

/// Durable length of an append-only spool. Records pair with bytes so
/// [`DeferredSpool::reopen_at`] can refuse a file a crash left shorter than
/// whoever recorded the mark counted
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SpoolMark {
    pub records: u64,
    pub bytes: u64,
}

/// Records and bytes, never the buffered tuples
impl std::fmt::Debug for DeferredSpool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DeferredSpool")
            .field("records", &self.records)
            .field("spooled_bytes", &self.spooled_bytes)
            .field("path", &self.path)
            .finish_non_exhaustive()
    }
}

impl DeferredSpool {
    /// `path` is created lazily at first overflow; parent dir must exist or
    /// be creatable
    pub fn new(path: PathBuf, mem_max: usize) -> Self {
        Self {
            store: Store::Mem {
                tuples: Vec::new(),
                bytes: 0,
            },
            mem_max,
            path,
            records: 0,
            spooled_bytes: 0,
        }
    }

    fn at_file(path: PathBuf, mem_max: usize, out: ChunkWriter, mark: SpoolMark) -> Self {
        Self {
            store: Store::File(out),
            mem_max,
            path,
            records: mark.records,
            spooled_bytes: mark.bytes,
        }
    }

    pub fn path(&self) -> &std::path::Path {
        &self.path
    }

    pub fn records(&self) -> u64 {
        self.records
    }

    /// Bytes retained in the in-memory prefix
    pub fn resident_bytes(&self) -> usize {
        match &self.store {
            Store::Mem { bytes, .. } => *bytes,
            Store::File(_) => 0,
        }
    }

    /// Encoded bytes written to the spool file
    pub fn spooled_bytes(&self) -> u64 {
        self.spooled_bytes
    }

    pub async fn push(&mut self, value: BackfillTuple) -> Result<()> {
        self.records += 1;
        if let Store::Mem { tuples, bytes } = &mut self.store {
            let value_bytes = approx_bytes(&value);
            if *bytes + value_bytes <= self.mem_max {
                *bytes += value_bytes;
                tuples.push(value);
                return Ok(());
            }
        }
        let out = self.file().await?;
        self.spooled_bytes += append(out, &value).await?;
        Ok(())
    }

    /// Writer, creating the file and moving the prefix into it on first use
    async fn file(&mut self) -> Result<&mut ChunkWriter> {
        if let Store::Mem { tuples, .. } = &mut self.store {
            let tuples = std::mem::take(tuples);
            let parent = self.path.parent().filter(|p| !p.as_os_str().is_empty());
            if let Some(parent) = parent {
                tokio::fs::create_dir_all(parent).await?;
            }
            let file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&self.path)
                .await?;
            // Checkpoint names this file, so its directory entry must survive
            crate::fs::fsync_dir(parent.unwrap_or(std::path::Path::new("."))).await?;
            let mut out = ChunkWriter::new(file.into_std().await, 0);
            out.buf.extend_from_slice(&SPOOL_MAGIC);
            push_u16(&mut out.buf, SPOOL_VERSION);
            for v in &tuples {
                self.spooled_bytes += append(&mut out, v).await?;
            }
            self.store = Store::File(out);
        }
        let Store::File(out) = &mut self.store else {
            unreachable!("file store installed above");
        };
        Ok(out)
    }

    /// Unlike the xact spill's disposable contract, a bootstrap gate spool
    /// outlives a crash: the tuples in it came from backup pages nothing can
    /// re-read, so a resumed load replays them instead of the whole backup
    pub async fn checkpoint(&mut self) -> Result<SpoolMark> {
        if matches!(&self.store, Store::Mem { tuples, .. } if tuples.is_empty()) {
            return Ok(SpoolMark::default());
        }
        self.file().await?.sync_data().await?;
        Ok(SpoolMark {
            records: self.records,
            bytes: self.spooled_bytes,
        })
    }

    /// Reopen a checkpointed spool for further appends, discarding a record
    /// a crash left half-written. `expect_records` is what the checkpoint
    /// counted: a short file means records a resumed pass would silently skip,
    /// so it refuses rather than replaying an incomplete set
    pub async fn reopen(path: PathBuf, mem_max: usize, expect_records: u64) -> Result<Self> {
        let total = tokio::fs::metadata(&path).await?.len();
        let (valid_len, records) = scan_records(&path, total).await?;
        if records != expect_records {
            return Err(SpillError::Format {
                offset: valid_len as usize,
                detail: format!(
                    "spool {} holds {records} complete records, checkpoint counted \
                     {expect_records}",
                    path.display(),
                ),
            });
        }
        let mut file = OpenOptions::new().write(true).open(&path).await?;
        file.set_len(valid_len).await?;
        file.seek(SeekFrom::Start(valid_len)).await?;
        let out = ChunkWriter::new(file.into_std().await, valid_len);
        let mark = SpoolMark {
            records,
            bytes: valid_len.saturating_sub(4),
        };
        Ok(Self::at_file(path, mem_max, out, mark))
    }

    /// Reopen for appends at a checkpointed length, discarding whatever the
    /// walk appended past the last published mark. Those records belong to
    /// files the checkpoint did not record, so a resumed walk writes them again
    pub async fn reopen_at(path: PathBuf, mem_max: usize, mark: SpoolMark) -> Result<Self> {
        let SpoolMark { records, bytes } = mark;
        if bytes == 0 && records == 0 {
            tokio::fs::remove_file(&path).await.ok();
            return Ok(Self::new(path, mem_max));
        }
        let keep = 4 + bytes;
        let (valid_len, complete) = scan_records(&path, keep).await?;
        if (valid_len, complete) != (keep, records) {
            return Err(SpillError::Format {
                offset: valid_len as usize,
                detail: format!(
                    "spool {} holds {complete} records in {valid_len} bytes, checkpoint counted \
                     {records} in {bytes}",
                    path.display(),
                ),
            });
        }
        let mut file = OpenOptions::new().write(true).open(&path).await?;
        file.set_len(keep).await?;
        file.seek(SeekFrom::Start(keep)).await?;
        let out = ChunkWriter::new(file.into_std().await, keep);
        Ok(Self::at_file(path, mem_max, out, mark))
    }

    /// Reopen an immutable spool at a previously acknowledged record boundary
    pub async fn resume(path: PathBuf, mark: SpoolMark, offset: u64) -> Result<ResumedSpool> {
        let SpoolMark { records, bytes } = mark;
        let length = tokio::fs::metadata(&path).await?.len();
        if offset > bytes || length.checked_sub(4) != Some(bytes) {
            return Err(SpillError::Format {
                offset: 0,
                detail: "checkpoint spool size changed".into(),
            });
        }
        open_validated(&path, 0).await?;
        Ok(ResumedSpool {
            path,
            records,
            spooled_bytes: bytes,
            read_offset: offset,
        })
    }

    /// Seal writes, hand back a sequential reader
    pub async fn into_reader(self) -> Result<DeferredReader> {
        let mut out = match self.store {
            Store::Mem { tuples, .. } => {
                return Ok(DeferredReader {
                    src: ReadSrc::Mem(tuples.into_iter()),
                });
            }
            Store::File(out) => out,
        };
        out.flush().await?;
        Ok(DeferredReader {
            src: ReadSrc::File {
                chunks: open_validated(&self.path, 0).await?,
                remaining_bytes: self.spooled_bytes,
                path: self.path,
            },
        })
    }

    /// Drop without replay (walk failure); unlink any file
    pub async fn discard(self) {
        if let Store::File(_) = self.store {
            let _ = tokio::fs::remove_file(&self.path).await;
        }
    }
}

impl ResumedSpool {
    pub fn records(&self) -> u64 {
        self.records
    }

    pub fn spooled_bytes(&self) -> u64 {
        self.spooled_bytes
    }

    /// Reader past the acknowledged prefix
    pub async fn into_reader(self) -> Result<DeferredReader> {
        Ok(DeferredReader {
            src: ReadSrc::File {
                chunks: open_validated(&self.path, self.read_offset).await?,
                remaining_bytes: self.spooled_bytes - self.read_offset,
                path: self.path,
            },
        })
    }
}

impl Replayable {
    pub fn records(&self) -> u64 {
        match self {
            Self::Open(s) => s.records(),
            Self::Resumed(s) => s.records(),
        }
    }

    pub fn resident_bytes(&self) -> usize {
        match self {
            Self::Open(s) => s.resident_bytes(),
            Self::Resumed(_) => 0,
        }
    }

    pub fn spooled_bytes(&self) -> u64 {
        match self {
            Self::Open(s) => s.spooled_bytes(),
            Self::Resumed(s) => s.spooled_bytes(),
        }
    }

    pub async fn into_reader(self) -> Result<DeferredReader> {
        match self {
            Self::Open(s) => s.into_reader().await,
            Self::Resumed(s) => s.into_reader().await,
        }
    }
}

impl From<DeferredSpool> for Replayable {
    fn from(spool: DeferredSpool) -> Self {
        Self::Open(spool)
    }
}

impl From<ResumedSpool> for Replayable {
    fn from(spool: ResumedSpool) -> Self {
        Self::Resumed(spool)
    }
}

/// Frame `value` onto `out`; returns bytes written
async fn append(out: &mut ChunkWriter, value: &BackfillTuple) -> Result<u64> {
    let buf = &mut out.buf;
    let len_at = buf.len();
    push_u32(buf, 0);
    let body_at = buf.len();
    encode_record(value, buf);
    let len = (buf.len() - body_at) as u32;
    buf[len_at..body_at].copy_from_slice(&len.to_le_bytes());
    out.maybe_flush().await?;
    Ok(4 + u64::from(len))
}

/// Where the last whole record below `limit` ends, and how many there were.
/// A returned offset short of `limit` means the tail is torn or `limit` lands
/// inside a record
async fn scan_records(path: &std::path::Path, limit: u64) -> Result<(u64, u64)> {
    let mut chunks = open_validated(path, 0).await?;
    let mut offset = 4u64;
    let mut records = 0u64;
    while let Some(body) = read_frame(&mut chunks, limit.saturating_sub(offset)).await? {
        offset += 4 + body.len() as u64;
        records += 1;
    }
    Ok((offset, records))
}

async fn open_validated(path: &std::path::Path, offset: u64) -> Result<ChunkReader> {
    ChunkReader::open(path, SPOOL_MAGIC, SPOOL_VERSION, offset, IO_CHUNK).await
}

async fn read_frame(chunks: &mut ChunkReader, remaining: u64) -> Result<Option<&[u8]>> {
    if remaining < 4 || !chunks.fill(4).await? {
        return Ok(None);
    }
    let len = u32::from_le_bytes(chunks.unread()[..4].try_into().unwrap()) as usize;
    if 4 + len as u64 > remaining || !chunks.fill(4 + len).await? {
        return Ok(None);
    }
    Ok(Some(&chunks.consume(4 + len)[4..]))
}

enum ReadSrc {
    Mem(std::vec::IntoIter<BackfillTuple>),
    File {
        chunks: ChunkReader,
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
    pub fn remaining_file_bytes(&self) -> u64 {
        match &self.src {
            ReadSrc::File {
                remaining_bytes, ..
            } => *remaining_bytes,
            ReadSrc::Mem(_) => 0,
        }
    }

    pub async fn next(&mut self) -> Result<Option<BackfillTuple>> {
        match &mut self.src {
            ReadSrc::Mem(it) => Ok(it.next()),
            ReadSrc::File {
                chunks,
                remaining_bytes,
                ..
            } => {
                if *remaining_bytes == 0 {
                    return Ok(None);
                }
                let body = read_frame(chunks, *remaining_bytes)
                    .await?
                    .ok_or_else(truncated)?;
                *remaining_bytes -= 4 + body.len() as u64;
                Ok(Some(decode_record(body)?))
            }
        }
    }

    /// Unlink the spool file after successful replay
    pub async fn finish(self) -> Result<()> {
        if let ReadSrc::File { chunks, path, .. } = self.src {
            drop(chunks);
            tokio::fs::remove_file(&path).await?;
        }
        Ok(())
    }
}

fn truncated() -> SpillError {
    SpillError::Format {
        offset: 0,
        detail: "spool truncated mid-record".into(),
    }
}

pub(crate) fn approx_bytes(value: &BackfillTuple) -> usize {
    std::mem::size_of::<BackfillTuple>()
        + value
            .columns
            .iter()
            .flatten()
            .map(crate::decode::heap_decoder::ColumnValue::approx_bytes)
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
    use crate::decode::heap_decoder::ColumnValue;
    use tempfile::tempdir;
    use tokio::io::AsyncWriteExt;

    #[tokio::test]
    async fn reopen_at_drops_records_past_the_mark_and_refuses_a_mid_record_one() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("reopen_at.bin");
        let mut spool = DeferredSpool::new(path.clone(), 0);
        spool.push(tuple(10, b"first")).await.unwrap();
        let mark = spool.checkpoint().await.unwrap();
        spool.push(tuple(20, b"second")).await.unwrap();
        spool.checkpoint().await.unwrap();
        drop(spool);

        let torn = SpoolMark {
            bytes: mark.bytes - 1,
            ..mark
        };
        assert!(
            DeferredSpool::reopen_at(path.clone(), 0, torn)
                .await
                .is_err(),
            "a mark inside a record would truncate it",
        );
        let mut spool = DeferredSpool::reopen_at(path.clone(), 0, mark)
            .await
            .unwrap();
        assert_eq!(spool.records(), 1);
        spool.push(tuple(30, b"third")).await.unwrap();
        spool.checkpoint().await.unwrap();
        let mut reader = spool.into_reader().await.unwrap();
        let mut seen = Vec::new();
        while let Some(t) = reader.next().await.unwrap() {
            seen.push(t.source_lsn);
        }
        assert_eq!(seen, vec![10, 30], "the record past the mark is gone");
    }

    #[tokio::test]
    async fn reopen_at_an_empty_mark_starts_the_spool_over() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("empty_mark.bin");
        let mut spool = DeferredSpool::new(path.clone(), 0);
        spool.push(tuple(10, b"first")).await.unwrap();
        spool.checkpoint().await.unwrap();
        drop(spool);
        let spool = DeferredSpool::reopen_at(path.clone(), 0, SpoolMark::default())
            .await
            .unwrap();
        assert_eq!(spool.records(), 0);
        assert!(!path.exists(), "nothing recorded means nothing to keep");
    }

    #[tokio::test]
    async fn resume_keeps_suffix_and_rejects_changed_spool() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("resume.bin");
        let mut spool = DeferredSpool::new(path.clone(), 0);
        spool.push(tuple(10, b"first")).await.unwrap();
        let offset = spool.spooled_bytes();
        spool.push(tuple(20, b"second")).await.unwrap();
        let mark = spool.checkpoint().await.unwrap();
        drop(spool);
        let mut reader = DeferredSpool::resume(path.clone(), mark, offset)
            .await
            .unwrap()
            .into_reader()
            .await
            .unwrap();
        assert_eq!(reader.next().await.unwrap().unwrap().source_lsn, 20);
        assert!(reader.next().await.unwrap().is_none());
        assert!(
            DeferredSpool::resume(path.clone(), mark, mark.bytes + 1)
                .await
                .is_err()
        );
        let f = OpenOptions::new().write(true).open(&path).await.unwrap();
        f.set_len(mark.bytes + 3).await.unwrap();
        assert!(DeferredSpool::resume(path, mark, offset).await.is_err());
    }

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

    #[tokio::test]
    async fn checkpoint_then_reopen_keeps_every_record() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("gate.bin");
        let mut spool = DeferredSpool::new(path.clone(), usize::MAX);
        assert_eq!(spool.checkpoint().await.unwrap(), SpoolMark::default());
        for i in 0..4 {
            spool.push(tuple(i, b"before")).await.unwrap();
        }
        assert!(!path.exists());
        let mark = spool.checkpoint().await.unwrap();
        assert_eq!(mark.records, 4);
        assert_eq!(
            tokio::fs::metadata(&path).await.unwrap().len(),
            mark.bytes + 4
        );
        drop(spool);

        let mut spool = DeferredSpool::reopen_at(path.clone(), 0, mark)
            .await
            .unwrap();
        assert_eq!(spool.records(), 4);
        spool.push(tuple(4, b"after")).await.unwrap();
        spool.checkpoint().await.unwrap();

        let all = drain_all(spool).await;
        assert_eq!(all.len(), 5);
        assert_eq!(all[0].source_lsn, 0);
        assert_eq!(all[4].source_lsn, 4);
    }

    /// A crash mid-append leaves a record whose body never landed; replay
    /// has to drop it rather than fail the resumed load
    #[tokio::test]
    async fn reopen_truncates_a_half_written_record() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("gate.bin");
        let mut spool = DeferredSpool::new(path.clone(), 0);
        spool.push(tuple(1, b"whole")).await.unwrap();
        spool.checkpoint().await.unwrap();
        let sealed = tokio::fs::metadata(&path).await.unwrap().len();
        drop(spool);

        let mut torn = tokio::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .await
            .unwrap();
        torn.write_all(&99u32.to_le_bytes()).await.unwrap();
        torn.write_all(b"only-a-few").await.unwrap();
        torn.flush().await.unwrap();
        drop(torn);

        let mut reader = DeferredReader {
            src: ReadSrc::File {
                chunks: open_validated(&path, sealed - 4).await.unwrap(),
                path: path.clone(),
                remaining_bytes: 103,
            },
        };
        assert!(matches!(
            reader.next().await,
            Err(SpillError::Format { .. })
        ));
        assert!(
            DeferredSpool::reopen(path.clone(), 0, 2).await.is_err(),
            "a torn tail must refuse a count the checkpoint promised",
        );
        let spool = DeferredSpool::reopen(path.clone(), 0, 1).await.unwrap();
        assert_eq!(spool.records(), 1);
        assert_eq!(tokio::fs::metadata(&path).await.unwrap().len(), sealed);
        let all = drain_all(spool).await;
        assert_eq!(all.len(), 1);
        assert_eq!(all[0].source_lsn, 1);
    }

    #[tokio::test]
    async fn checkpoint_forces_an_in_memory_prefix_to_disk() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("gate.bin");
        let mut spool = DeferredSpool::new(path.clone(), DEFERRED_SPOOL_MEM_MAX);
        spool.push(tuple(7, b"resident")).await.unwrap();
        assert!(!path.exists(), "small spool stays in memory until forced");
        spool.checkpoint().await.unwrap();
        drop(spool);

        let spool = DeferredSpool::reopen(path.clone(), 0, 1).await.unwrap();
        assert_eq!(spool.records(), 1);
        assert_eq!(drain_all(spool).await[0].source_lsn, 7);
    }

    #[tokio::test]
    async fn checkpoint_on_an_empty_spool_writes_nothing() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("gate.bin");
        let mut spool = DeferredSpool::new(path.clone(), DEFERRED_SPOOL_MEM_MAX);
        spool.checkpoint().await.unwrap();
        assert!(!path.exists());
    }

    async fn drain_all(spool: DeferredSpool) -> Vec<BackfillTuple> {
        let mut remaining = spool.spooled_bytes();
        let mut reader = spool.into_reader().await.unwrap();
        assert_eq!(reader.remaining_file_bytes(), remaining);
        let mut out = Vec::new();
        while let Some(t) = reader.next().await.unwrap() {
            let next = reader.remaining_file_bytes();
            if remaining > 0 {
                assert!(next < remaining);
            }
            remaining = next;
            out.push(t);
        }
        assert_eq!(remaining, 0);
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
                chunks: open_validated(&path, 0).await.unwrap(),
                path,
                remaining_bytes: bytes - 4,
            },
        };
        let err = reader.next().await.expect_err("corrupt length surfaces");
        match err {
            SpillError::Format { detail, .. } => {
                assert!(detail.contains("truncated"), "{detail}");
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
            open_validated(&magic, 0).await,
            Err(SpillError::Format { offset: 0, .. })
        ));
        let version = tmp.path().join("version.bin");
        std::fs::write(&version, [b'W', b'D', 0xFF, 0x00]).unwrap();
        assert!(matches!(
            open_validated(&version, 0).await,
            Err(SpillError::Format { offset: 2, .. })
        ));
    }
}
