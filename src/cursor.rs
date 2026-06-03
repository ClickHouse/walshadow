//! Durable resume cursor.
//!
//! Persists the daemon's resume position so a `kill -9` plus restart
//! lands a consistent CH end-state. Lives next to the spill dir
//! (`{spill_dir}/cursor.bin`) so a `mv` of the working dir keeps the
//! resume state coherent with the spill files it would otherwise
//! reference.
//!
//! ## Schema
//!
//! `MAGIC (8B) | version u32 LE | source_received_lsn u64 LE |
//!  filter_durable_lsn u64 LE | shadow_replay_lsn u64 LE |
//!  drain_lsn u64 LE | emitter_ack_lsn u64 LE |
//!  shadow_flush_lsn u64 LE | crc32c u32 LE`
//!
//! Total 64 bytes. CRC32C covers every preceding byte; a corrupt or
//! truncated file surfaces as [`CursorError`] and the boot path falls
//! back to greenfield resume (treat as missing). No partial-write
//! window because every persist runs `create+write+sync+rename+
//! dir_sync` against `cursor.bin.tmp`, then renames over `cursor.bin`,
//! then `fsync`s the spill dir so the rename is durable.
//!
//! ## Semantics
//!
//! Six LSNs, ordered roughly newest→oldest in WAL position:
//!
//! * `source_received_lsn`: highest server_wal_end seen on the
//!   replication socket. Bookkeeping only — never gates anything.
//! * `filter_durable_lsn`: highest segment-boundary LSN
//!   [`DirSegmentSink`](crate::wal_stream::DirSegmentSink) has fsynced
//!   on disk. Doubles as the standby-status `flush_lsn` we advertise
//!   to source.
//! * `shadow_replay_lsn`: shadow PG's `pg_last_wal_replay_lsn()`. Lags
//!   `filter_durable_lsn` by recovery cost.
//! * `drain_lsn`: highest commit-record LSN drained out of the xact
//!   buffer (handed to the observer). Strictly higher than
//!   `emitter_ack_lsn`.
//! * `emitter_ack_lsn`: highest commit-record LSN where the CH
//!   emitter's `on_xact_end` returned Ok. Slot-advance ceiling.
//! * `shadow_flush_lsn`: minimum `flush_lsn` reported via
//!   inbound `'r'` standby status across active shadow streaming
//!   connections. On daemon restart this is the resume position the
//!   walsender hands shadow back through `START_REPLICATION PHYSICAL
//!   <lsn>`. Bookkeeping-only when there are no active streaming
//!   connections; the on-disk `restore_command` fallback takes over.
//!
//! The standby-status `apply_lsn` shipped to source equals
//! `min(shadow_replay_lsn, emitter_ack_lsn)` — neither side may
//! advance past either replica.

use std::io;
use std::path::{Path, PathBuf};

use thiserror::Error;
use tokio::fs::OpenOptions;
use tokio::io::AsyncWriteExt;

/// Filename under `spill_dir`. Picked alongside `xid-*.bin` per
/// PLAN's "cursor file under `{spill_dir}/cursor.bin`".
pub const CURSOR_FILENAME: &str = "cursor.bin";

/// Schema version. Bump on any layout change; boot path rejects
/// mismatched versions explicitly.
pub const CURSOR_VERSION: u32 = 2;

/// Magic prefix. `WSCRSR` + version-byte + reserved-byte.
const MAGIC: &[u8; 8] = b"WSCRSR\x01\x00";

const HEADER_LEN: usize = MAGIC.len() + 4 /*version*/;
const LSN_COUNT: usize = 6;
const PAYLOAD_LEN: usize = HEADER_LEN + LSN_COUNT * 8;
const CRC_LEN: usize = 4;
/// On-disk byte count of a cursor file. `8 + 4 + 6*8 + 4 = 64`.
pub const CURSOR_FILE_LEN: usize = PAYLOAD_LEN + CRC_LEN;

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Cursor {
    pub source_received_lsn: u64,
    pub filter_durable_lsn: u64,
    pub shadow_replay_lsn: u64,
    pub drain_lsn: u64,
    pub emitter_ack_lsn: u64,
    /// Minimum flush_lsn across active shadow streaming connections.
    /// Resume position the walsender hands shadow back through
    /// `START_REPLICATION PHYSICAL <lsn>` after a daemon restart.
    pub shadow_flush_lsn: u64,
}

#[derive(Debug, Error)]
pub enum CursorError {
    #[error("io: {0}")]
    Io(#[from] io::Error),
    #[error("cursor file size {got} not a recognised cursor length")]
    Size { got: usize },
    #[error("bad magic header")]
    BadMagic,
    #[error("unsupported cursor schema version {0} (this build expects {CURSOR_VERSION})")]
    Version(u32),
    #[error("crc mismatch: stored={stored:#X}, computed={computed:#X}")]
    Crc { stored: u32, computed: u32 },
}

/// Absolute path to the cursor file inside `spill_dir`.
pub fn cursor_path(spill_dir: &Path) -> PathBuf {
    spill_dir.join(CURSOR_FILENAME)
}

/// Encode `cur` into the on-disk byte form (v2). Pure function —
/// exposed so tests can pin the format.
pub fn encode(cur: &Cursor) -> Vec<u8> {
    let mut out = Vec::with_capacity(CURSOR_FILE_LEN);
    out.extend_from_slice(MAGIC);
    out.extend_from_slice(&CURSOR_VERSION.to_le_bytes());
    out.extend_from_slice(&cur.source_received_lsn.to_le_bytes());
    out.extend_from_slice(&cur.filter_durable_lsn.to_le_bytes());
    out.extend_from_slice(&cur.shadow_replay_lsn.to_le_bytes());
    out.extend_from_slice(&cur.drain_lsn.to_le_bytes());
    out.extend_from_slice(&cur.emitter_ack_lsn.to_le_bytes());
    out.extend_from_slice(&cur.shadow_flush_lsn.to_le_bytes());
    let crc = crc32c::crc32c(&out);
    out.extend_from_slice(&crc.to_le_bytes());
    debug_assert_eq!(out.len(), CURSOR_FILE_LEN);
    out
}

/// Decode an on-disk cursor file. Returns precise errors for the boot
/// path's fall-back logic.
pub fn decode(bytes: &[u8]) -> Result<Cursor, CursorError> {
    if bytes.len() != CURSOR_FILE_LEN {
        return Err(CursorError::Size { got: bytes.len() });
    }
    if &bytes[0..8] != MAGIC {
        return Err(CursorError::BadMagic);
    }
    let version = u32::from_le_bytes(bytes[8..12].try_into().unwrap());
    if version != CURSOR_VERSION {
        return Err(CursorError::Version(version));
    }
    let stored = u32::from_le_bytes(bytes[PAYLOAD_LEN..PAYLOAD_LEN + 4].try_into().unwrap());
    let computed = crc32c::crc32c(&bytes[..PAYLOAD_LEN]);
    if stored != computed {
        return Err(CursorError::Crc { stored, computed });
    }
    let mut off = HEADER_LEN;
    let mut read = || {
        let v = u64::from_le_bytes(bytes[off..off + 8].try_into().unwrap());
        off += 8;
        v
    };
    let source_received_lsn = read();
    let filter_durable_lsn = read();
    let shadow_replay_lsn = read();
    let drain_lsn = read();
    let emitter_ack_lsn = read();
    let shadow_flush_lsn = read();
    Ok(Cursor {
        source_received_lsn,
        filter_durable_lsn,
        shadow_replay_lsn,
        drain_lsn,
        emitter_ack_lsn,
        shadow_flush_lsn,
    })
}

/// Atomic-rename writer. Writes to `cursor.bin.tmp`, fsyncs, renames
/// over `cursor.bin`, then fsyncs the dir entry so the rename itself
/// survives a power loss. `spill_dir` must already exist (the daemon
/// creates it during [`XactBuffer::new`](crate::xact_buffer::XactBuffer)).
pub async fn write(spill_dir: &Path, cur: &Cursor) -> Result<(), CursorError> {
    let final_path = cursor_path(spill_dir);
    let tmp_path = spill_dir.join(format!("{CURSOR_FILENAME}.tmp"));
    let bytes = encode(cur);
    let mut f = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&tmp_path)
        .await?;
    f.write_all(&bytes).await?;
    f.sync_all().await?;
    drop(f);
    tokio::fs::rename(&tmp_path, &final_path).await?;
    fsync_dir(spill_dir).await?;
    Ok(())
}

/// Read the cursor at boot. `Ok(None)` for greenfield (file absent);
/// `Ok(Some)` for a valid file; `Err` for a corrupt one (caller logs
/// and falls back to greenfield).
pub async fn read(spill_dir: &Path) -> Result<Option<Cursor>, CursorError> {
    let final_path = cursor_path(spill_dir);
    let bytes = match tokio::fs::read(&final_path).await {
        Ok(b) => b,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e.into()),
    };
    Ok(Some(decode(&bytes)?))
}

/// fsync a directory entry. Linux + most BSDs accept `open(dir,
/// O_RDONLY) + fsync(fd)`. tokio's `File::sync_all` maps to fsync(2),
/// so the open-for-read + sync_all combo is exactly what's needed.
pub async fn fsync_dir(dir: &Path) -> io::Result<()> {
    let f = OpenOptions::new().read(true).open(dir).await?;
    f.sync_all().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn sample() -> Cursor {
        Cursor {
            source_received_lsn: 0x0123_4567_89AB_CDEF,
            filter_durable_lsn: 0x0123_4567_0000_0000,
            shadow_replay_lsn: 0x0123_4566_0000_0000,
            drain_lsn: 0x0123_4565_0000_0000,
            emitter_ack_lsn: 0x0123_4564_0000_0000,
            shadow_flush_lsn: 0x0123_4563_0000_0000,
        }
    }

    #[test]
    fn encode_decode_round_trips() {
        let c = sample();
        let bytes = encode(&c);
        assert_eq!(bytes.len(), CURSOR_FILE_LEN);
        let got = decode(&bytes).unwrap();
        assert_eq!(got, c);
    }

    #[test]
    fn decode_rejects_short_input() {
        let err = decode(&[0u8; 4]).unwrap_err();
        assert!(matches!(err, CursorError::Size { got: 4 }));
    }

    #[test]
    fn decode_rejects_bad_magic() {
        let mut bytes = encode(&sample());
        bytes[0] = b'X';
        // CRC will also fail but magic check fires first.
        let err = decode(&bytes).unwrap_err();
        assert!(matches!(err, CursorError::BadMagic));
    }

    #[test]
    fn decode_rejects_wrong_version() {
        let mut bytes = encode(&sample());
        bytes[8..12].copy_from_slice(&999u32.to_le_bytes());
        // Patch CRC so the version check fires before the CRC check.
        let crc = crc32c::crc32c(&bytes[..PAYLOAD_LEN]);
        bytes[PAYLOAD_LEN..PAYLOAD_LEN + 4].copy_from_slice(&crc.to_le_bytes());
        let err = decode(&bytes).unwrap_err();
        assert!(matches!(err, CursorError::Version(999)));
    }

    #[test]
    fn decode_rejects_bad_crc() {
        let mut bytes = encode(&sample());
        // Flip one LSN payload bit, leave the CRC alone.
        bytes[HEADER_LEN] ^= 0xFF;
        let err = decode(&bytes).unwrap_err();
        assert!(matches!(err, CursorError::Crc { .. }));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn read_returns_none_when_file_absent() {
        let tmp = tempdir().unwrap();
        let got = read(tmp.path()).await.unwrap();
        assert!(got.is_none(), "greenfield boot must surface as None");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn write_then_read_round_trips() {
        let tmp = tempdir().unwrap();
        let c = sample();
        write(tmp.path(), &c).await.unwrap();
        // No leftover .tmp from the atomic rename.
        assert!(
            !tmp.path().join(format!("{CURSOR_FILENAME}.tmp")).exists(),
            "rename must clean up the .tmp sidecar",
        );
        let got = read(tmp.path()).await.unwrap().expect("cursor present");
        assert_eq!(got, c);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn read_surfaces_non_not_found_io_error() {
        // `cursor.bin` exists as a directory — tokio::fs::read returns
        // IsADirectory (not NotFound), so the error must propagate.
        let tmp = tempdir().unwrap();
        std::fs::create_dir(tmp.path().join(CURSOR_FILENAME)).unwrap();
        let err = read(tmp.path()).await.unwrap_err();
        assert!(
            matches!(err, CursorError::Io(_)),
            "expected Io error, got {err:?}",
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn second_write_overwrites_first() {
        let tmp = tempdir().unwrap();
        let mut c = sample();
        write(tmp.path(), &c).await.unwrap();
        c.emitter_ack_lsn = 0xDEAD_BEEF_F00D_BABE;
        write(tmp.path(), &c).await.unwrap();
        let got = read(tmp.path()).await.unwrap().expect("cursor present");
        assert_eq!(got, c);
    }
}
