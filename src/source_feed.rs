//! Source PG replication pump.
//!
//! Wraps `wal-rs`'s [`ReplicationConn`]: `IDENTIFY_SYSTEM`, then
//! `START_REPLICATION PHYSICAL`, then a frame loop surfacing
//! `CopyData('w')` WAL bytes while handling `CopyData('k')` keepalives
//! and periodic standby-status replies.

use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use bytes::Bytes;
use postgres_protocol::message::backend::Message;
use tokio::net::{TcpStream, UnixStream};
use tokio_postgres::config::SslMode as TpSslMode;
use tokio_postgres::{Client, NoTls};
use walross::pg::backup::parse_pg_lsn;
use walross::pg::replication::conn::{PgConfig, ReplicationConn, error_message, message_kind};
use walross::pg::replication::stream::{Frame, build_status_update, decode_frame};
use walross::pg::replication::tls::{SocketStream, SslMode, maybe_upgrade};

/// Matches wal-rs / wal-g defaults; servers tolerate up to
/// `wal_sender_timeout` of silence (default 60s).
pub const DEFAULT_STATUS_INTERVAL: Duration = Duration::from_secs(10);

#[derive(Debug, Clone, Copy)]
pub struct WalChunk<'a> {
    pub start_lsn: u64,
    pub server_wal_end: u64,
    pub data: &'a [u8],
}

/// Three LSNs wal-rs's `build_status_update` ships to source PG.
/// `apply`/`flush` gate source's slot: must not advertise durability
/// the filter and CH emitter have not reached, else source recycles
/// un-consumed WAL.
#[derive(Debug, Clone, Copy, Default)]
pub struct StandbyStatus {
    /// `write_lsn` PG sees: WAL pump read + committed to store locally.
    pub write_lsn: u64,
    /// `flush_lsn` PG sees: filter durably wrote out to segments.
    pub flush_lsn: u64,
    /// `apply_lsn` PG sees: bounded by shadow replay LSN and CH emitter
    /// ack LSN, so source's slot won't recycle un-consumed WAL.
    pub apply_lsn: u64,
}

impl StandbyStatus {
    /// All three slots at the same value.
    pub fn collapsed(lsn: u64) -> Self {
        Self {
            write_lsn: lsn,
            flush_lsn: lsn,
            apply_lsn: lsn,
        }
    }
}

/// Per-field monotonic high-water. Each field has its own floor so a
/// leading `write` never lifts `flush`/`apply` (would claim durability
/// the filter and CH emitter have not reached, recycling un-filtered
/// WAL).
#[derive(Debug, Clone, Copy, Default)]
struct StatusFloors {
    write: u64,
    flush: u64,
    apply: u64,
}

/// PG rejects a regressing flush/apply, so hold each field at its own
/// high-water, independent of the others.
fn clamp_status(status: StandbyStatus, floors: &mut StatusFloors) -> (u64, u64, u64) {
    floors.write = status.write_lsn.max(floors.write);
    floors.flush = status.flush_lsn.max(floors.flush);
    floors.apply = status.apply_lsn.max(floors.apply);
    (floors.write, floors.flush, floors.apply)
}

/// `IDENTIFY_SYSTEM` result.
#[derive(Debug, Clone)]
pub struct SystemIdentity {
    pub sysid: String,
    pub timeline: u32,
    pub xlogpos: u64,
}

/// Replication-mode source connection. Lazily opens a sidecar
/// non-replication `tokio_postgres::Client` for SQL the
/// replication-mode connection cannot serve (eg
/// `CatalogTracker::seed_from_source`).
pub struct SourceFeed {
    conn: ReplicationConn,
    cfg: PgConfig,
    /// Opened on first `sql_client` call.
    sql_client: Option<Client>,
    status_interval: Duration,
    last_status: Instant,
    floors: StatusFloors,
    /// Most recent `server_wal_end` from a WAL frame or keepalive;
    /// surfaces "source ahead by N" without re-issuing IDENTIFY_SYSTEM.
    last_server_wal_end: u64,
}

impl SourceFeed {
    pub async fn connect(cfg: &PgConfig) -> Result<Self> {
        let conn = ReplicationConn::connect(cfg).await?;
        Ok(Self {
            conn,
            cfg: cfg.clone(),
            sql_client: None,
            status_interval: DEFAULT_STATUS_INTERVAL,
            last_status: Instant::now(),
            floors: StatusFloors::default(),
            last_server_wal_end: 0,
        })
    }

    pub fn with_status_interval(mut self, interval: Duration) -> Self {
        self.status_interval = interval;
        self
    }

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

    /// `START_REPLICATION PHYSICAL <slot>? <start_lsn> TIMELINE <tli>`.
    /// `Some(slot)` references a permanent physical slot (PG ≥ 9.4).
    /// Connection enters CopyBoth mode.
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
        self.floors = StatusFloors {
            write: start_lsn,
            flush: start_lsn,
            apply: start_lsn,
        };
        self.last_status = Instant::now();
        Ok(())
    }

    /// `Ok(Some)` per WAL data frame, `Ok(None)` on `CopyDone` (server
    /// shutdown), `Err` on unexpected frames; keepalives + status
    /// updates handled internally.
    ///
    /// Pass the consumer's current `status` (write/flush/apply) each
    /// iteration so a keepalive's `reply_requested` echoes fresh values.
    pub async fn next_chunk<'b>(
        &mut self,
        status: StandbyStatus,
        buf: &'b mut Vec<u8>,
    ) -> Result<Option<WalChunk<'b>>> {
        buf.clear();
        loop {
            if self.last_status.elapsed() >= self.status_interval {
                self.send_status(status).await?;
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
                        Frame::Wal(w) => {
                            buf.extend_from_slice(w.data);
                            self.last_server_wal_end = w.server_wal_end;
                            return Ok(Some(WalChunk {
                                start_lsn: w.start_lsn,
                                server_wal_end: w.server_wal_end,
                                data: buf.as_slice(),
                            }));
                        }
                        Frame::Keepalive(k) => {
                            if k.reply_requested {
                                self.send_status(status).await?;
                            }
                            self.last_server_wal_end = k.server_wal_end;
                        }
                    }
                }
                Message::CopyDone => return Ok(None),
                Message::ErrorResponse(e) => bail!("source: {}", error_message(&e)),
                m => {
                    tracing::debug!(
                        target: "walshadow::source_feed",
                        kind = message_kind(&m),
                        "unexpected backend message",
                    );
                }
            }
        }
    }

    /// Zero before `start_physical_replication` produces any traffic.
    pub fn last_server_wal_end(&self) -> u64 {
        self.last_server_wal_end
    }

    async fn send_status(&mut self, status: StandbyStatus) -> Result<()> {
        // PG treats a regressing flush/apply as a protocol violation
        // and may force-disconnect; clamp_status holds per-field floors.
        let (write, flush, apply) = clamp_status(status, &mut self.floors);
        let payload = build_status_update(write, flush, apply);
        self.conn.send_copy_data(&payload).await?;
        self.last_status = Instant::now();
        Ok(())
    }

    pub fn server_version_num(&self) -> i32 {
        self.conn.server_version_num
    }

    /// Lazily-opened sidecar client. Replication-mode connections only
    /// honour the replication command set, not arbitrary `SELECT`s.
    ///
    /// TLS reuses wal-rs's `maybe_upgrade` so sslmode + `PGSSLROOTCERT`
    /// match the replication socket; the wrapped stream goes to
    /// `connect_raw` with `NoTls` and tokio-postgres `ssl_mode` pinned
    /// `Disable` so it does not double-negotiate.
    pub async fn sql_client(&mut self) -> Result<&Client> {
        if self.sql_client.is_none() {
            let client = open_sql_client(&self.cfg).await.with_context(|| {
                format!("sidecar sql connect to {}:{}", self.cfg.host, self.cfg.port)
            })?;
            self.sql_client = Some(client);
        }
        Ok(self.sql_client.as_ref().unwrap())
    }
}

/// Mirrors wal-rs's transport choice: unix socket when `host` starts
/// with `/`, TLS-or-plain TCP otherwise.
async fn open_sql_client(cfg: &PgConfig) -> Result<Client> {
    let mut tp_cfg = tokio_postgres::Config::new();
    tp_cfg
        .user(cfg.user.as_str())
        .dbname(cfg.database.as_str())
        .application_name(format!("{}-sql", cfg.application_name))
        // Stream already TLS-wrapped (or plain); tokio-postgres must
        // not retry SSLRequest over it.
        .ssl_mode(TpSslMode::Disable);
    if let Some(pw) = &cfg.password {
        tp_cfg.password(pw.as_str());
    }

    let stream: Box<dyn SocketStream> = if cfg.host.starts_with('/') {
        // PG refuses TLS on unix sockets server-side; skip negotiation
        let path = format!("{}/.s.PGSQL.{}", cfg.host.trim_end_matches('/'), cfg.port);
        let sock = UnixStream::connect(&path)
            .await
            .with_context(|| format!("connect to unix:{path}"))?;
        Box::new(sock)
    } else {
        let addr = format!("{}:{}", cfg.host, cfg.port);
        let raw = TcpStream::connect(&addr)
            .await
            .with_context(|| format!("connect to {addr}"))?;
        if cfg.sslmode == SslMode::Disable {
            Box::new(raw)
        } else {
            let (sock, _used_tls) = maybe_upgrade(raw, &cfg.host, cfg.sslmode)
                .await
                .with_context(|| format!("tls negotiation against {addr}"))?;
            sock
        }
    };

    let (client, conn) = tp_cfg.connect_raw(stream, NoTls).await?;
    tokio::spawn(async move {
        let _ = conn.await;
    });
    Ok(client)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clamp_keeps_each_field_independent() {
        let mut floors = StatusFloors::default();
        // leading write must not lift flush/apply
        let (w, f, a) = clamp_status(
            StandbyStatus {
                write_lsn: 3000,
                flush_lsn: 1000,
                apply_lsn: 800,
            },
            &mut floors,
        );
        assert_eq!((w, f, a), (3000, 1000, 800));
        // higher flush/apply, same write: each rises from its own floor
        let (w, f, a) = clamp_status(
            StandbyStatus {
                write_lsn: 3000,
                flush_lsn: 1500,
                apply_lsn: 1200,
            },
            &mut floors,
        );
        assert_eq!((w, f, a), (3000, 1500, 1200));
    }

    #[test]
    fn clamp_never_regresses_a_field() {
        let mut floors = StatusFloors::default();
        clamp_status(
            StandbyStatus {
                write_lsn: 5000,
                flush_lsn: 4000,
                apply_lsn: 4000,
            },
            &mut floors,
        );
        // stale lower values hold at prior floor
        let (w, f, a) = clamp_status(
            StandbyStatus {
                write_lsn: 4500,
                flush_lsn: 3000,
                apply_lsn: 3500,
            },
            &mut floors,
        );
        assert_eq!((w, f, a), (5000, 4000, 4000));
    }
}
