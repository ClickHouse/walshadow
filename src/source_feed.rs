//! Source PG replication pump.
//!
//! Wraps `wal-rs`'s [`ReplicationConn`] in walshadow's call style:
//! issue `IDENTIFY_SYSTEM`, then `START_REPLICATION PHYSICAL`, then run
//! a frame loop that surfaces `CopyData('w')` WAL bytes to a caller
//! callback while transparently handling `CopyData('k')` keepalives and
//! periodic standby-status replies. Used by the daemon binary (PRE5
//! item 1) and by the live-source integration tests.

use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use bytes::Bytes;
use postgres_protocol::message::backend::Message;
use tokio::net::{TcpStream, UnixStream};
use tokio_postgres::config::SslMode as TpSslMode;
use tokio_postgres::{Client, NoTls};
use wal_rs::pg::backup::parse_pg_lsn;
use wal_rs::pg::replication::conn::{PgConfig, ReplicationConn, error_message, message_kind};
use wal_rs::pg::replication::stream::{Frame, build_status_update, decode_frame};
use wal_rs::pg::replication::tls::{SocketStream, SslMode, maybe_upgrade};

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
///
/// Lazily opens a sidecar non-replication `tokio_postgres::Client` for
/// SQL queries that the replication-mode connection cannot serve (eg
/// `CatalogTracker::seed_from_source`). Same `PgConfig`, NoTLS — TLS
/// over the sidecar would need a separate connector pipeline.
pub struct SourceFeed {
    conn: ReplicationConn,
    cfg: PgConfig,
    /// Sidecar libpq client for non-replication queries. Opened on
    /// first `sql_client` call.
    sql_client: Option<Client>,
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
            cfg: cfg.clone(),
            sql_client: None,
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
                        Frame::Wal(w) => {
                            buf.extend_from_slice(w.data);
                            return Ok(Some(WalChunk {
                                start_lsn: w.start_lsn,
                                server_wal_end: w.server_wal_end,
                                data: buf.as_slice(),
                            }));
                        }
                        Frame::Keepalive(k) => {
                            if k.reply_requested {
                                self.send_status(apply_lsn).await?;
                            }
                            // server_wal_end surfaces for diagnostics only;
                            // current WalStream consumer ignores it
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

    /// Borrow a sidecar `tokio_postgres::Client` for the same source,
    /// opened lazily on first call. Replication-mode connections cannot
    /// run arbitrary `SELECT`s cleanly (server only honours the small
    /// replication-protocol command set); the seed/sidecar path uses
    /// this fall-back connection.
    ///
    /// TLS reuses wal-rs's `maybe_upgrade` so sslmode (disable / allow /
    /// prefer / require / verify-ca / verify-full) plus `PGSSLROOTCERT`
    /// behave identically to the replication socket. The TLS-wrapped
    /// stream is handed to `tokio_postgres::Config::connect_raw` with
    /// `NoTls`; tokio-postgres's own `ssl_mode` is pinned to `Disable`
    /// so it does not double-negotiate.
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

/// Open a non-replication libpq connection to the same source PG.
/// Mirrors wal-rs's transport choice: unix socket when `host` starts
/// with `/`, TLS-or-plain TCP otherwise. The startup handshake itself
/// runs through tokio-postgres's `connect_raw`, so its `Client`
/// surface is what callers receive.
async fn open_sql_client(cfg: &PgConfig) -> Result<Client> {
    let mut tp_cfg = tokio_postgres::Config::new();
    tp_cfg
        .user(cfg.user.as_str())
        .dbname(cfg.database.as_str())
        .application_name(format!("{}-sql", cfg.application_name))
        // Stream is already TLS-wrapped (or intentionally plain) by the
        // time it reaches connect_raw; tokio-postgres must not retry
        // SSLRequest over the wrapped stream.
        .ssl_mode(TpSslMode::Disable);
    if let Some(pw) = &cfg.password {
        tp_cfg.password(pw.as_str());
    }

    let stream: Box<dyn SocketStream> = if cfg.host.starts_with('/') {
        // PG refuses TLS on unix sockets server-side; skip negotiation.
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

#[inline]
fn tracing_debug(_kind: &'static str) {
    // Stub for places where wal-rs uses `tracing::debug!`. walshadow
    // doesn't yet plug the tracing pipeline; future work attaches a
    // subscriber so these surfaces are observable. Drop the macro for
    // now to keep build deps thin.
}
