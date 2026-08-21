//! Source PG replication pump.
//!
//! Wraps `wal-rus`'s [`ReplicationConn`]: `IDENTIFY_SYSTEM`, then
//! `START_REPLICATION PHYSICAL`, then a frame loop surfacing
//! `CopyData('w')` WAL bytes while handling `CopyData('k')` keepalives
//! and periodic standby-status replies.

use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use bytes::Bytes;
use postgres_protocol::message::backend::Message;
use tokio::net::{TcpStream, UnixStream};
use tokio_postgres::config::SslMode as TpSslMode;
use tokio_postgres::types::PgLsn;
use tokio_postgres::{Client, NoTls};
use walrus::pg::backup::{format_pg_lsn, parse_pg_lsn};
use walrus::pg::replication::conn::{
    PgConfig, ReplicationConn, error_code, error_message, message_kind,
};
use walrus::pg::replication::stream::{Frame, build_status_update, decode_frame};
use walrus::pg::replication::tls::{SocketStream, SslMode, maybe_upgrade};

use crate::pos::{Floor, Pos, ResumeSafe, SourceReceived};

/// Matches wal-rus / wal-g defaults; servers tolerate up to
/// `wal_sender_timeout` of silence (default 60s).
pub const DEFAULT_STATUS_INTERVAL: Duration = Duration::from_secs(10);

/// SQLSTATE for a WAL segment the source has already recycled
/// (`undefined_file`). Locale-independent, unlike the message text.
pub const SQLSTATE_UNDEFINED_FILE: &str = "58P01";

/// The source recycled the requested LSN's WAL segment. Fatal — the resume
/// point is gone; recovery is a re-bootstrap / backfill via config, not a
/// reconnect. Typed so the pump distinguishes it from a transient drop.
#[derive(Debug, thiserror::Error)]
#[error("source WAL segment already removed (SQLSTATE {error_code}): {message}")]
pub struct WalSegmentRemoved {
    pub error_code: String,
    pub message: String,
}

impl WalSegmentRemoved {
    /// walrus collapses the `START_REPLICATION` reply `ErrorResponse` to a flat
    /// string, discarding the SQLSTATE — the one spot forced to read the message
    /// to recover the recycled-segment case, so detection stays typed.
    fn from_start_replication(err: anyhow::Error) -> anyhow::Error {
        let text = format!("{err:#}");
        if text.contains(SQLSTATE_UNDEFINED_FILE) {
            anyhow::Error::new(WalSegmentRemoved {
                error_code: SQLSTATE_UNDEFINED_FILE.to_string(),
                message: text,
            })
        } else {
            err
        }
    }
}

pub fn is_wal_segment_removed(err: &anyhow::Error) -> bool {
    err.downcast_ref::<WalSegmentRemoved>()
        .is_some_and(|e| e.error_code == SQLSTATE_UNDEFINED_FILE)
}

/// Why a named slot cannot carry the resume position. Walshadow creates no slot
/// it was not booted with, so both are the operator's to fix: the promotion
/// target owns its slot and `[source] slot` names it (plans/failover.md
/// §Operator protocol).
#[derive(Debug, thiserror::Error)]
pub enum SlotError {
    #[error("no physical replication slot {slot:?} on the source{}", match kind {
        Some(k) => format!(" (a {k} slot has that name)"),
        None => String::new(),
    })]
    Missing { slot: String, kind: Option<String> },
    #[error(
        "slot {slot:?} reserves write-ahead log from {}, above the {} this consumer resumes at",
        format_pg_lsn(*restart_lsn),
        format_pg_lsn(*resume_lsn),
    )]
    TooNew {
        slot: String,
        restart_lsn: u64,
        resume_lsn: u64,
    },
    #[error("read pg_replication_slots: {0:#}")]
    Query(#[source] anyhow::Error),
}

impl SlotError {
    /// Metric and refusal label.
    pub fn reason(&self) -> &'static str {
        match self {
            Self::Missing { .. } => "slot_missing",
            Self::TooNew { .. } => "slot_too_new",
            Self::Query(_) => "source",
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct WalChunk<'a> {
    pub start_lsn: u64,
    pub server_wal_end: u64,
    pub data: &'a [u8],
}

/// The three ways a physical replication stream ends, told apart by what the
/// backend sends (`src/backend/replication/walsender.c`).
#[derive(Debug)]
pub enum SourceEvent<'a> {
    Wal(WalChunk<'a>),
    /// Backend `CopyDone`: the requested timeline is historic and reached its
    /// switchpoint. Only this ending sends one. The connection stays in COPY
    /// until [`end_historic_stream`](SourceFeed::end_historic_stream) answers.
    TimelineEnd,
    /// `WalSndDone` sends a bare `CommandComplete` from inside COPY, then
    /// exits — no `CopyDone` before it and no `ReadyForQuery` after. A parser
    /// that only leaves COPY on `CopyDone` reads this as a stray streaming
    /// message and then blocks until the socket closes. It is a reconnect, not
    /// a daemon exit: the source stopped, the consumer has not.
    Shutdown,
}

/// Three LSNs wal-rus's `build_status_update` ships to source PG.
/// `apply`/`flush` gate source's slot: must not advertise durability
/// the filter and CH emitter have not reached, else source recycles
/// un-consumed WAL.
#[derive(Debug, Clone, Copy, Default)]
pub struct StandbyStatus {
    /// `write_lsn` PG sees: WAL pump read + committed to store locally.
    pub write_lsn: Pos<SourceReceived>,
    /// `flush_lsn` PG sees: drives a physical slot's restart_lsn, so the
    /// caller caps it at its persisted resume floor — source must retain
    /// WAL a crash-now restart re-requests.
    pub flush_lsn: Pos<Floor>,
    /// `apply_lsn` PG sees: bounded by shadow replay LSN and CH emitter
    /// ack LSN, so source's slot won't recycle un-consumed WAL.
    pub apply_lsn: Pos<ResumeSafe>,
}

impl StandbyStatus {
    /// All three slots at the same value.
    pub fn collapsed(lsn: u64) -> Self {
        Self {
            write_lsn: lsn.into(),
            flush_lsn: lsn.into(),
            apply_lsn: lsn.into(),
        }
    }
}

/// Per-field monotonic high-water. Each field has its own floor so a
/// leading `write` never lifts `flush`/`apply` (would claim durability
/// the filter and CH emitter have not reached, recycling un-filtered
/// WAL).
#[derive(Debug, Clone, Copy, Default)]
struct StatusFloors {
    write: Pos<SourceReceived>,
    flush: Pos<Floor>,
    apply: Pos<ResumeSafe>,
}

/// PG assigns a physical slot's restart_lsn from flush unconditionally
/// (`PhysicalConfirmReceivedLocation`), so a stale lower value walks the
/// slot backwards, below recycled WAL it can read as invalidatable under
/// max_slot_wal_keep_size. Hold each field at its own high-water.
fn clamp_status(status: StandbyStatus, floors: &mut StatusFloors) -> StandbyStatus {
    floors.write = status.write_lsn.max(floors.write);
    floors.flush = status.flush_lsn.max(floors.flush);
    floors.apply = status.apply_lsn.max(floors.apply);
    StandbyStatus {
        write_lsn: floors.write,
        flush_lsn: floors.flush,
        apply_lsn: floors.apply,
    }
}

/// Build the `START_REPLICATION` command string for a physical replication
/// start at `start_lsn`. The LSN is rendered via [`format_pg_lsn`] so both
/// halves use PG's hexadecimal `<hi>/<lo>` `pg_lsn` text form.
fn build_start_replication_cmd(slot: Option<&str>, start_lsn: u64, timeline: u32) -> String {
    let lsn = format_pg_lsn(start_lsn);
    match slot {
        Some(slot) => {
            format!("START_REPLICATION SLOT {slot} PHYSICAL {lsn} TIMELINE {timeline}")
        }
        None => format!("START_REPLICATION {lsn} TIMELINE {timeline}"),
    }
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

    /// Ensure a physical replication slot exists, reserving WAL immediately so
    /// the source retains segments from the slot's `restart_lsn` onward — a
    /// stalled/disconnected consumer resumes without the segment being
    /// recycled. Idempotent: a pre-existing slot is left untouched. Runs on the
    /// sidecar SQL connection.
    pub async fn ensure_physical_slot(&mut self, name: &str) -> Result<()> {
        let client = self.sql_client().await?;
        client
            .execute(
                "SELECT pg_create_physical_replication_slot($1, true) \
                 WHERE NOT EXISTS \
                 (SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)",
                &[&name],
            )
            .await
            .context("create physical replication slot")?;
        Ok(())
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
        let cmd = build_start_replication_cmd(slot, start_lsn, timeline);
        self.conn.send_query(&cmd).await?;
        self.conn
            .expect_copy_both_open()
            .await
            .map_err(WalSegmentRemoved::from_start_replication)?;
        // Floors stay zero: they high-water values actually sent. Seeding at
        // start_lsn would lift flush to the live resume position on reconnect,
        // advancing source's slot past the caller's persisted floor.
        self.last_status = Instant::now();
        Ok(())
    }

    /// One WAL data frame, or the reason the stream ended; keepalives + status
    /// updates handled internally.
    ///
    /// Pass the consumer's current `status` (write/flush/apply) each
    /// iteration so a keepalive's `reply_requested` echoes fresh values.
    pub async fn next_event<'b>(
        &mut self,
        status: StandbyStatus,
        buf: &'b mut Vec<u8>,
    ) -> Result<SourceEvent<'b>> {
        buf.clear();
        loop {
            if self.last_status.elapsed() >= self.status_interval {
                self.send_status(status).await?;
            }
            let timeout = self
                .status_interval
                .saturating_sub(self.last_status.elapsed())
                .max(Duration::from_millis(50));
            let Ok(msg) = tokio::time::timeout(timeout, self.conn.recv_message()).await else {
                continue;
            };
            let msg = msg?;
            match msg {
                Message::CopyData(d) => {
                    let payload: Bytes = d.into_bytes();
                    match decode_frame(&payload)? {
                        Frame::Wal(w) => {
                            buf.extend_from_slice(w.data);
                            self.last_server_wal_end = w.server_wal_end;
                            return Ok(SourceEvent::Wal(WalChunk {
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
                Message::CopyDone => return Ok(SourceEvent::TimelineEnd),
                Message::CommandComplete(_) => return Ok(SourceEvent::Shutdown),
                Message::ErrorResponse(e) => {
                    let (code, message) = (error_code(&e), error_message(&e));
                    if code == SQLSTATE_UNDEFINED_FILE {
                        return Err(anyhow::Error::new(WalSegmentRemoved {
                            error_code: code,
                            message,
                        }));
                    }
                    bail!("source: {message}");
                }
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

    /// Answer a backend `CopyDone` with the client's own, leaving the
    /// connection back in simple-query mode ready for the next command.
    ///
    /// PostgreSQL follows this with a one-row result set carrying `next_tli`
    /// and `next_tli_startpos`; wal-rus drains it. The switchpoint is read from
    /// `TIMELINE_HISTORY` instead, which the crossing has to parse anyway to
    /// prove lineage and to hand the shadow a history file — a row that only
    /// restates it saves nothing.
    pub async fn end_historic_stream(&mut self) -> Result<()> {
        self.conn.end_copy().await.context("end historic COPY")
    }

    /// `TIMELINE_HISTORY <tli>` → the file's exact bytes. `Ok(None)` when the
    /// source has no history file for it (timeline 1 never does).
    pub async fn timeline_history(&mut self, timeline: u32) -> Result<Option<Vec<u8>>> {
        let row = self
            .conn
            .timeline_history(timeline)
            .await
            .with_context(|| format!("TIMELINE_HISTORY {timeline}"))?;
        Ok(row.map(|(_name, content)| content))
    }

    async fn send_status(&mut self, status: StandbyStatus) -> Result<()> {
        let held = clamp_status(status, &mut self.floors);
        let payload = build_status_update(
            held.write_lsn.get(),
            held.flush_lsn.get(),
            held.apply_lsn.get(),
        );
        self.conn.send_copy_data(&payload).await?;
        self.last_status = Instant::now();
        Ok(())
    }

    pub fn server_version_num(&self) -> i32 {
        self.conn.server_version_num
    }

    /// Prove a pre-created physical slot can carry this consumer: it exists, it
    /// is physical, and it reserves write-ahead log from at or below
    /// `resume_lsn`.
    ///
    /// `pg_create_physical_replication_slot(name, true)` reserves at the last
    /// checkpoint or restartpoint redo rather than at the current position
    /// (`ReplicationSlotReserveWal`, `src/backend/replication/slot.c`), so a
    /// slot made ahead of a switchover normally sits *behind* the target's
    /// replay position — a lagging consumer is what inverts it, and no slot can
    /// be created below the redo it would need. Creating the missing one here
    /// would reserve at the target's head and call that continuation
    /// successful, which is the failure this refuses.
    ///
    /// `floor` is the position a *restart* asks for, which the reserve may sit
    /// above without breaking this connection; that is a warning, since the
    /// first standby status pulls `restart_lsn` back down to it
    /// (`PhysicalConfirmReceivedLocation`).
    pub async fn prove_physical_slot(
        &mut self,
        slot: &str,
        resume_lsn: Pos<Floor>,
        floor: Pos<Floor>,
    ) -> Result<Option<u64>, SlotError> {
        let client = self.sql_client().await.map_err(SlotError::Query)?;
        let row = client
            .query_opt(
                "SELECT slot_type, restart_lsn FROM pg_replication_slots WHERE slot_name = $1",
                &[&slot],
            )
            .await
            .map_err(|e| SlotError::Query(e.into()))?
            .ok_or_else(|| SlotError::Missing {
                slot: slot.to_string(),
                kind: None,
            })?;
        let kind: String = row.get(0);
        if kind != "physical" {
            return Err(SlotError::Missing {
                slot: slot.to_string(),
                kind: Some(kind),
            });
        }
        let restart_lsn: Option<u64> = row.get::<_, Option<PgLsn>>(1).map(u64::from);
        let Some(restart_lsn) = restart_lsn else {
            // Created without `immediately_reserve`: it pins nothing until this
            // connection binds it, so the window before that is unprotected
            tracing::warn!(
                target: "walshadow",
                slot,
                "slot reserves no write-ahead log yet; it starts pinning at \
                 START_REPLICATION and nothing retains what precedes that",
            );
            return Ok(None);
        };
        if restart_lsn > resume_lsn.get() {
            return Err(SlotError::TooNew {
                slot: slot.to_string(),
                restart_lsn,
                resume_lsn: resume_lsn.get(),
            });
        }
        if restart_lsn > floor.get() {
            tracing::warn!(
                target: "walshadow",
                slot,
                restart_lsn = %format_pg_lsn(restart_lsn),
                floor = %floor,
                "slot pins no write-ahead log back to the resume floor; a restart \
                 before it advances would ask for segments nothing retains",
            );
        }
        Ok(Some(restart_lsn))
    }

    /// Force the next [`sql_client`](Self::sql_client) to redial. A cached
    /// client whose connection died fails every query until it is dropped.
    pub fn drop_sql_client(&mut self) {
        self.sql_client = None;
    }

    /// Lazily-opened sidecar client. Replication-mode connections only
    /// honour the replication command set, not arbitrary `SELECT`s.
    ///
    /// TLS reuses wal-rus's `maybe_upgrade` so sslmode + `PGSSLROOTCERT`
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

/// Mirrors wal-rus's transport choice: unix socket when `host` starts
/// with `/`, TLS-or-plain TCP otherwise. Shared with the COPY backfiller
/// ([`crate::backfill::copy_backfill`]), which opens its own session per backfill.
pub async fn open_sql_client(cfg: &PgConfig) -> Result<Client> {
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
            let (sock, _used_tls) = maybe_upgrade(raw, &cfg.host, cfg.sslmode, &cfg.tls)
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

    fn triple(s: StandbyStatus) -> (u64, u64, u64) {
        (s.write_lsn.get(), s.flush_lsn.get(), s.apply_lsn.get())
    }

    #[test]
    fn clamp_keeps_each_field_independent() {
        let mut floors = StatusFloors::default();
        // leading write must not lift flush/apply
        let held = clamp_status(
            StandbyStatus {
                write_lsn: 3000.into(),
                flush_lsn: 1000.into(),
                apply_lsn: 800.into(),
            },
            &mut floors,
        );
        assert_eq!(triple(held), (3000, 1000, 800));
        // higher flush/apply, same write: each rises from its own floor
        let held = clamp_status(
            StandbyStatus {
                write_lsn: 3000.into(),
                flush_lsn: 1500.into(),
                apply_lsn: 1200.into(),
            },
            &mut floors,
        );
        assert_eq!(triple(held), (3000, 1500, 1200));
    }

    #[test]
    fn clamp_never_regresses_a_field() {
        let mut floors = StatusFloors::default();
        clamp_status(
            StandbyStatus {
                write_lsn: 5000.into(),
                flush_lsn: 4000.into(),
                apply_lsn: 4000.into(),
            },
            &mut floors,
        );
        // stale lower values hold at prior floor
        let held = clamp_status(
            StandbyStatus {
                write_lsn: 4500.into(),
                flush_lsn: 3000.into(),
                apply_lsn: 3500.into(),
            },
            &mut floors,
        );
        assert_eq!(triple(held), (5000, 4000, 4000));
    }

    /// Both halves of the start LSN must be rendered in hexadecimal, matching
    /// PG's `pg_lsn` text form (`<hi>/<lo>`). A high word of 0xB must serialize
    /// as "B", not "11" — otherwise the source parses it as hex 0x11 and the
    /// requested position lands ahead of its WAL flush position.
    #[test]
    fn start_replication_renders_high_word_in_hex() {
        // 0xB/4C000000 — high word past 0x9, where hex and decimal diverge.
        let lsn = (0xB_u64 << 32) | 0x4C00_0000;

        let with_slot = build_start_replication_cmd(Some("phys"), lsn, 1);
        assert_eq!(
            with_slot, "START_REPLICATION SLOT phys PHYSICAL B/4C000000 TIMELINE 1",
            "high word must be hex (B), not decimal (11)"
        );

        let no_slot = build_start_replication_cmd(None, lsn, 1);
        assert_eq!(
            no_slot, "START_REPLICATION B/4C000000 TIMELINE 1",
            "high word must be hex (B), not decimal (11)"
        );
    }
}
