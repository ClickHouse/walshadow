//! Source PG replication pump.
//!
//! Wraps `wal-rs`'s [`ReplicationConn`] in walshadow's call style:
//! issue `IDENTIFY_SYSTEM`, then `START_REPLICATION PHYSICAL`, then run
//! a frame loop that surfaces `CopyData('w')` WAL bytes to a caller
//! callback while transparently handling `CopyData('k')` keepalives and
//! periodic standby-status replies. Used by the daemon binary (PRE5
//! item 1) and by the live-source integration tests.
//!
//! Frame decoding + status-update wire layout duplicate the equivalent
//! pieces in `wal-rs/src/pg/wal/receive.rs`; those helpers are private
//! over there. Follow-up: expose them from wal-rs and drop this
//! duplicate (`PRE5.md`'s "wal-rs reusable library" task).

use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use bytes::Bytes;
use postgres_protocol::message::backend::Message;
use wal_rs::pg::replication::conn::{
    PgConfig, ReplicationConn, error_message, message_kind,
};

/// PG epoch (microseconds between 1970-01-01 and 2000-01-01). Standby
/// status updates carry timestamps in PG's microsecond-since-2000.
const PG_EPOCH_USEC: i64 = 946_684_800_000_000;

/// Default standby-status update cadence. Matches wal-rs / wal-g
/// defaults; servers normally tolerate up to `wal_sender_timeout` of
/// silence (default 60s).
pub const DEFAULT_STATUS_INTERVAL: Duration = Duration::from_secs(10);

#[derive(Debug, Clone, Copy)]
pub struct WalChunk<'a> {
    pub start_lsn: u64,
    pub server_wal_end: u64,
    pub data: &'a [u8],
}

#[derive(Debug, Clone, Copy)]
struct KeepaliveFrame {
    server_wal_end: u64,
    reply_requested: bool,
}

#[derive(Debug, Clone, Copy)]
enum Frame<'a> {
    Wal {
        start_lsn: u64,
        server_wal_end: u64,
        data: &'a [u8],
    },
    Keepalive(KeepaliveFrame),
}

fn decode_frame(payload: &[u8]) -> Result<Frame<'_>> {
    if payload.is_empty() {
        bail!("empty CopyData payload");
    }
    match payload[0] {
        b'w' => {
            if payload.len() < 1 + 24 {
                bail!("WAL data frame too short: {} bytes", payload.len());
            }
            let p = &payload[1..];
            let start_lsn = u64::from_be_bytes(p[0..8].try_into().unwrap());
            let server_wal_end = u64::from_be_bytes(p[8..16].try_into().unwrap());
            let _send_time = i64::from_be_bytes(p[16..24].try_into().unwrap());
            Ok(Frame::Wal {
                start_lsn,
                server_wal_end,
                data: &p[24..],
            })
        }
        b'k' => {
            if payload.len() < 1 + 17 {
                bail!("keepalive frame too short: {} bytes", payload.len());
            }
            let p = &payload[1..];
            let server_wal_end = u64::from_be_bytes(p[0..8].try_into().unwrap());
            let _send_time = i64::from_be_bytes(p[8..16].try_into().unwrap());
            let reply_requested = p[16] != 0;
            Ok(Frame::Keepalive(KeepaliveFrame {
                server_wal_end,
                reply_requested,
            }))
        }
        tag => bail!("unknown CopyData tag: {:?}", tag as char),
    }
}

fn build_status_update(write_lsn: u64, flush_lsn: u64, apply_lsn: u64) -> Vec<u8> {
    let mut out = Vec::with_capacity(34);
    out.push(b'r');
    out.extend_from_slice(&write_lsn.to_be_bytes());
    out.extend_from_slice(&flush_lsn.to_be_bytes());
    out.extend_from_slice(&apply_lsn.to_be_bytes());
    out.extend_from_slice(&now_pg_microseconds().to_be_bytes());
    out.push(0);
    out
}

fn now_pg_microseconds() -> i64 {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_micros() as i64)
        .unwrap_or(0);
    now - PG_EPOCH_USEC
}

/// Result of `IDENTIFY_SYSTEM`: server identity plus current WAL
/// position.
#[derive(Debug, Clone)]
pub struct SystemIdentity {
    pub sysid: String,
    pub timeline: u32,
    /// Current write LSN.
    pub xlogpos: u64,
}

/// One source connection in replication mode. Composes `ReplicationConn`
/// (auth + framing) with walshadow's frame loop.
pub struct SourceFeed {
    conn: ReplicationConn,
    /// Wall-clock budget between standby status updates.
    status_interval: Duration,
    last_status: Instant,
    last_acked_lsn: u64,
}

impl SourceFeed {
    /// Open a replication-mode connection to the source PG.
    pub async fn connect(cfg: &PgConfig) -> Result<Self> {
        let conn = ReplicationConn::connect(cfg).await?;
        Ok(Self {
            conn,
            status_interval: DEFAULT_STATUS_INTERVAL,
            last_status: Instant::now(),
            last_acked_lsn: 0,
        })
    }

    pub fn with_status_interval(mut self, interval: Duration) -> Self {
        self.status_interval = interval;
        self
    }

    /// `IDENTIFY_SYSTEM` round-trip: read server's sysid, timeline,
    /// current LSN.
    pub async fn identify_system(&mut self) -> Result<SystemIdentity> {
        self.conn.send_query("IDENTIFY_SYSTEM").await?;
        let mut sysid = String::new();
        let mut timeline: u32 = 0;
        let mut xlogpos: u64 = 0;
        loop {
            match self.conn.recv_message().await? {
                Message::RowDescription(_) => {}
                Message::DataRow(row) => {
                    use fallible_iterator::FallibleIterator as _;
                    let buf = row.buffer_bytes().clone();
                    let mut ranges = row.ranges();
                    let mut idx = 0;
                    while let Some(r) = ranges.next()? {
                        if let Some(range) = r {
                            let v = std::str::from_utf8(&buf[range])?;
                            match idx {
                                0 => sysid = v.to_string(),
                                1 => timeline = v.parse().context("timeline parse")?,
                                2 => xlogpos = parse_pg_lsn(v)?,
                                _ => {}
                            }
                        }
                        idx += 1;
                    }
                }
                Message::CommandComplete(_) => {}
                Message::ReadyForQuery(_) => break,
                Message::ErrorResponse(e) => bail!("IDENTIFY_SYSTEM: {}", error_message(&e)),
                _ => continue,
            }
        }
        if sysid.is_empty() || timeline == 0 {
            bail!("IDENTIFY_SYSTEM returned an empty result");
        }
        Ok(SystemIdentity {
            sysid,
            timeline,
            xlogpos,
        })
    }

    /// Issue `START_REPLICATION PHYSICAL <slot>? <start_lsn> TIMELINE
    /// <tli>`. When `slot` is `Some`, a permanent physical slot is
    /// referenced (PG ≥ 9.4). Connection enters CopyBoth mode.
    pub async fn start_physical_replication(
        &mut self,
        slot: Option<&str>,
        start_lsn: u64,
        timeline: u32,
    ) -> Result<()> {
        let cmd = match slot {
            Some(slot) => format!(
                "START_REPLICATION SLOT {slot} PHYSICAL {}/{:X} TIMELINE {timeline}",
                start_lsn >> 32,
                start_lsn as u32
            ),
            None => format!(
                "START_REPLICATION {}/{:X} TIMELINE {timeline}",
                start_lsn >> 32,
                start_lsn as u32
            ),
        };
        self.conn.send_query(&cmd).await?;
        self.conn.expect_copy_both_open().await?;
        self.last_acked_lsn = start_lsn;
        self.last_status = Instant::now();
        Ok(())
    }

    /// Block on one frame from the server. Handles keepalives + status
    /// updates internally; returns `Ok(Some(chunk))` for each WAL data
    /// frame, `Ok(None)` on `CopyDone` (server shutdown), and `Err` on
    /// unexpected frames.
    ///
    /// Caller drives this in a loop. `apply_lsn` is the highest LSN
    /// the consumer has durably committed downstream; pass `last_acked`
    /// + segment progress on each iteration.
    pub async fn next_chunk<'b>(
        &mut self,
        apply_lsn: u64,
        buf: &'b mut Vec<u8>,
    ) -> Result<Option<WalChunk<'b>>> {
        buf.clear();
        loop {
            if self.last_status.elapsed() >= self.status_interval {
                self.send_status(apply_lsn).await?;
            }
            let timeout = self
                .status_interval
                .saturating_sub(self.last_status.elapsed())
                .max(Duration::from_millis(50));
            let msg = match tokio::time::timeout(timeout, self.conn.recv_message()).await {
                Ok(r) => r?,
                Err(_) => continue,
            };
            match msg {
                Message::CopyData(d) => {
                    let payload: Bytes = d.into_bytes();
                    match decode_frame(&payload)? {
                        Frame::Wal {
                            start_lsn,
                            server_wal_end,
                            data,
                        } => {
                            buf.extend_from_slice(data);
                            return Ok(Some(WalChunk {
                                start_lsn,
                                server_wal_end,
                                data: buf.as_slice(),
                            }));
                        }
                        Frame::Keepalive(k) => {
                            if k.reply_requested {
                                self.send_status(apply_lsn).await?;
                            }
                            // Update server wal end without surfacing to
                            // caller — useful for diagnostics, ignored
                            // by current `WalStream` consumer.
                            let _ = k.server_wal_end;
                        }
                    }
                }
                Message::CopyDone => return Ok(None),
                Message::ErrorResponse(e) => bail!("source: {}", error_message(&e)),
                m => tracing_debug(message_kind(&m)),
            }
        }
    }

    async fn send_status(&mut self, apply_lsn: u64) -> Result<()> {
        let pos = apply_lsn.max(self.last_acked_lsn);
        let payload = build_status_update(pos, pos, pos);
        self.conn.send_copy_data(&payload).await?;
        self.last_acked_lsn = pos;
        self.last_status = Instant::now();
        Ok(())
    }

    pub fn server_version_num(&self) -> i32 {
        self.conn.server_version_num
    }
}

#[inline]
fn tracing_debug(_kind: &'static str) {
    // Stub for places where wal-rs uses `tracing::debug!`. walshadow
    // doesn't yet plug the tracing pipeline; future work attaches a
    // subscriber so these surfaces are observable. Drop the macro for
    // now to keep build deps thin.
}

/// Parse PG's `pg_lsn` text form (e.g. `"0/16B3750"`, hex pair separated
/// by `/`). Duplicate of [`shadow::parse_pg_lsn`] kept here so source
/// connection setup does not pull in shadow's `ShadowError` type.
fn parse_pg_lsn(s: &str) -> Result<u64> {
    let s = s.trim();
    let (hi, lo) = s
        .split_once('/')
        .ok_or_else(|| anyhow::anyhow!("bad pg_lsn {s:?}: no '/'"))?;
    let hi = u32::from_str_radix(hi, 16)?;
    let lo = u32::from_str_radix(lo, 16)?;
    Ok(((hi as u64) << 32) | (lo as u64))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decode_wal_frame_extracts_start_lsn_and_data() {
        let mut p = Vec::new();
        p.push(b'w');
        p.extend_from_slice(&0x100u64.to_be_bytes());
        p.extend_from_slice(&0x200u64.to_be_bytes());
        p.extend_from_slice(&0i64.to_be_bytes());
        p.extend_from_slice(b"hello");
        let f = decode_frame(&p).unwrap();
        match f {
            Frame::Wal {
                start_lsn,
                server_wal_end,
                data,
            } => {
                assert_eq!(start_lsn, 0x100);
                assert_eq!(server_wal_end, 0x200);
                assert_eq!(data, b"hello");
            }
            _ => panic!("expected WAL frame"),
        }
    }

    #[test]
    fn decode_keepalive_frame_reply_bit() {
        let mut p = Vec::new();
        p.push(b'k');
        p.extend_from_slice(&0x300u64.to_be_bytes());
        p.extend_from_slice(&12345i64.to_be_bytes());
        p.push(1);
        let f = decode_frame(&p).unwrap();
        match f {
            Frame::Keepalive(k) => {
                assert!(k.reply_requested);
                assert_eq!(k.server_wal_end, 0x300);
            }
            _ => panic!("expected keepalive"),
        }
    }

    #[test]
    fn rejects_short_frames() {
        assert!(decode_frame(b"w").is_err());
        assert!(decode_frame(b"k\x00").is_err());
        assert!(decode_frame(b"").is_err());
        assert!(decode_frame(b"x\x00\x00").is_err());
    }

    #[test]
    fn status_update_payload_shape() {
        let bytes = build_status_update(0x1, 0x2, 0x3);
        assert_eq!(bytes[0], b'r');
        assert_eq!(bytes.len(), 1 + 8 * 4 + 1);
        let write = u64::from_be_bytes(bytes[1..9].try_into().unwrap());
        let flush = u64::from_be_bytes(bytes[9..17].try_into().unwrap());
        let apply = u64::from_be_bytes(bytes[17..25].try_into().unwrap());
        assert_eq!(write, 1);
        assert_eq!(flush, 2);
        assert_eq!(apply, 3);
    }

    #[test]
    fn parse_pg_lsn_text_form() {
        assert_eq!(parse_pg_lsn("0/0").unwrap(), 0);
        assert_eq!(parse_pg_lsn("0/16B3750").unwrap(), 0x016B3750);
        assert_eq!(parse_pg_lsn("1/0").unwrap(), 1u64 << 32);
    }
}
