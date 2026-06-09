//! Durable resume cursor. Lives at `{spill_dir}/cursor.bin` so a `mv`
//! of the working dir keeps resume state coherent with spill files.
//!
//! ## Schema
//!
//! `MAGIC (8B) | version u32 LE | source_received_lsn u64 LE |
//!  filter_durable_lsn u64 LE | shadow_replay_lsn u64 LE |
//!  drain_lsn u64 LE | emitter_ack_lsn u64 LE |
//!  shadow_flush_lsn u64 LE | crc32c u32 LE`
//!
//! 64 bytes. CRC32C covers every preceding byte; corrupt/truncated
//! file falls back to greenfield resume. Persist is crash-safe:
//! write+fsync `cursor.bin.tmp`, rename over `cursor.bin`, fsync dir
//! so rename survives power loss.
//!
//! ## Semantics
//!
//! Six LSNs, roughly newest→oldest in WAL position:
//!
//! * `source_received_lsn`: highest server_wal_end seen on the
//!   replication socket. Bookkeeping only, never gates anything.
//! * `filter_durable_lsn`: highest segment-boundary LSN
//!   [`DirSegmentSink`](crate::wal_stream::DirSegmentSink) fsynced.
//!   Doubles as standby-status `flush_lsn` advertised to source.
//! * `shadow_replay_lsn`: shadow PG's `pg_last_wal_replay_lsn()`
//! * `drain_lsn`: highest commit-record LSN drained out of the xact
//!   buffer. Strictly higher than `emitter_ack_lsn`.
//! * `emitter_ack_lsn`: highest commit-record LSN where CH emitter's
//!   `on_xact_end` returned Ok. Slot-advance ceiling.
//! * `shadow_flush_lsn`: min `flush_lsn` from inbound `'r'` standby
//!   status across active shadow streaming connections. On restart,
//!   resume position walsender hands shadow via `START_REPLICATION
//!   PHYSICAL <lsn>`. Bookkeeping-only with no active connections;
//!   on-disk `restore_command` fallback takes over.
//!
//! standby-status `apply_lsn` shipped to source equals
//! `min(shadow_replay_lsn, emitter_ack_lsn)`: neither side may advance
//! past either replica.

use std::io;
use std::path::{Path, PathBuf};

use thiserror::Error;
use tokio::fs::OpenOptions;
use tokio::io::AsyncWriteExt;

pub const CURSOR_FILENAME: &str = "cursor.bin";

/// Bump on any layout change; boot path rejects mismatched versions.
pub const CURSOR_VERSION: u32 = 2;

/// `WSCRSR` + version-byte + reserved-byte.
const MAGIC: &[u8; 8] = b"WSCRSR\x01\x00";

const HEADER_LEN: usize = MAGIC.len() + 4 /*version*/;
const LSN_COUNT: usize = 6;
const PAYLOAD_LEN: usize = HEADER_LEN + LSN_COUNT * 8;
const CRC_LEN: usize = 4;
/// `8 + 4 + 6*8 + 4 = 64`
pub const CURSOR_FILE_LEN: usize = PAYLOAD_LEN + CRC_LEN;

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Cursor {
    pub source_received_lsn: u64,
    pub filter_durable_lsn: u64,
    pub shadow_replay_lsn: u64,
    pub drain_lsn: u64,
    pub emitter_ack_lsn: u64,
    /// Min flush_lsn across active shadow streaming connections.
    /// Resume position walsender hands shadow via `START_REPLICATION
    /// PHYSICAL <lsn>` after restart.
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

pub fn cursor_path(spill_dir: &Path) -> PathBuf {
    spill_dir.join(CURSOR_FILENAME)
}

/// Exposed so tests can pin the format.
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

/// Precise errors feed boot-path fallback logic.
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

/// Write `cursor.bin.tmp`, fsync, rename over `cursor.bin`, fsync dir
/// entry so rename survives power loss. `spill_dir` must already exist
/// ([`XactBuffer::new`](crate::xact_buffer::XactBuffer) creates it).
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

/// Resolve WAL resume LSN, precedence order:
///
///   1. operator `--start-lsn` override (recovery drills rewind here)
///   2. fresh-bootstrap `end_lsn`: shadow catalog at `end_lsn`, WAL
///      before it double-counts
///   3. cursor's last `emitter_ack_lsn`: durable CH resume point
///   4. greenfield: source's current write head
///
/// Pipeline ack atomic MUST seed from this SAME value, not 0: status
/// loop persists atomic into cursor's `emitter_ack_lsn` every interval
/// with no monotonic guard, first write fires at boot before any
/// re-read acks. Seeding 0 clobbers a resumed cursor's ack to 0; a
/// crash before re-read of `[aligned, resume]` then falls through to
/// case 4 next boot (zero ack skipped), silently dropping `[resume,
/// head]` WAL that never reached CH.
pub fn resolve_resume_lsn(
    start_lsn: Option<u64>,
    bootstrap_end_lsn: Option<u64>,
    cursor_ack_lsn: Option<u64>,
    greenfield_head: u64,
) -> u64 {
    match (start_lsn, bootstrap_end_lsn, cursor_ack_lsn) {
        (Some(s), _, _) => s,
        (None, Some(l), _) => l,
        (None, None, Some(c)) if c != 0 => c,
        (None, None, _) => greenfield_head,
    }
}

/// `Ok(None)` for greenfield (file absent); `Err` for corrupt (caller
/// logs, falls back to greenfield).
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
/// O_RDONLY) + fsync(fd)`; tokio's `File::sync_all` maps to fsync(2).
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
        // magic check fires before CRC
        let err = decode(&bytes).unwrap_err();
        assert!(matches!(err, CursorError::BadMagic));
    }

    #[test]
    fn decode_rejects_wrong_version() {
        let mut bytes = encode(&sample());
        bytes[8..12].copy_from_slice(&999u32.to_le_bytes());
        // patch CRC so version check is what fires
        let crc = crc32c::crc32c(&bytes[..PAYLOAD_LEN]);
        bytes[PAYLOAD_LEN..PAYLOAD_LEN + 4].copy_from_slice(&crc.to_le_bytes());
        let err = decode(&bytes).unwrap_err();
        assert!(matches!(err, CursorError::Version(999)));
    }

    #[test]
    fn decode_rejects_bad_crc() {
        let mut bytes = encode(&sample());
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
        assert!(
            !tmp.path().join(format!("{CURSOR_FILENAME}.tmp")).exists(),
            "rename must clean up the .tmp sidecar",
        );
        let got = read(tmp.path()).await.unwrap().expect("cursor present");
        assert_eq!(got, c);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn read_surfaces_non_not_found_io_error() {
        // dir at `cursor.bin`: read returns IsADirectory not NotFound,
        // must propagate rather than greenfield
        let tmp = tempdir().unwrap();
        std::fs::create_dir(tmp.path().join(CURSOR_FILENAME)).unwrap();
        let err = read(tmp.path()).await.unwrap_err();
        assert!(
            matches!(err, CursorError::Io(_)),
            "expected Io error, got {err:?}",
        );
    }

    #[test]
    fn resume_lsn_start_override_wins() {
        assert_eq!(
            resolve_resume_lsn(Some(0x10), Some(0x99), Some(0x88), 0xFF),
            0x10,
        );
    }

    #[test]
    fn resume_lsn_bootstrap_end_outranks_cursor() {
        assert_eq!(resolve_resume_lsn(None, Some(0x99), Some(0x88), 0xFF), 0x99,);
    }

    #[test]
    fn resume_lsn_resumes_from_cursor_ack_not_greenfield() {
        // Regression: durable-cursor restart must resume from
        // emitter_ack_lsn, never fall through to source head (would
        // silently skip [ack, head] WAL)
        let cursor_ack = 0xAABB_0000u64;
        let head = 0xFFFF_0000u64;
        let resume = resolve_resume_lsn(None, None, Some(cursor_ack), head);
        assert_eq!(resume, cursor_ack, "must resume from durable cursor ack");
        assert_ne!(resume, 0, "ack seed must not regress to 0");
        assert_ne!(resume, head, "must not skip ahead to source head");
        assert!(resume >= cursor_ack, "ack seed never below durable point");
    }

    #[test]
    fn resume_lsn_zero_cursor_ack_falls_through_to_greenfield() {
        // ack == 0 is greenfield-equivalent: nothing below head to ship
        assert_eq!(resolve_resume_lsn(None, None, Some(0), 0xFF), 0xFF);
    }

    #[test]
    fn resume_lsn_greenfield_uses_head() {
        assert_eq!(resolve_resume_lsn(None, None, None, 0x4242), 0x4242);
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
