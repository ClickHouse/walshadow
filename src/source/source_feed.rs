//! Source PG replication pump.
//!
//! Wraps `wal-rus`'s [`ReplicationConn`]: `IDENTIFY_SYSTEM`, then
//! `START_REPLICATION PHYSICAL`, then a frame loop surfacing
//! `CopyData('w')` WAL bytes while handling `CopyData('k')` keepalives
//! and periodic standby-status replies.

use std::time::Duration;

use anyhow::{Context, Result, bail};
use bytes::Bytes;
use postgres_protocol::message::backend::Message;
use tokio::net::{TcpStream, UnixStream};
use tokio::time::Instant;
use tokio_postgres::config::SslMode as TpSslMode;
use tokio_postgres::types::PgLsn;
use tokio_postgres::{Client, NoTls};
use walrus::pg::backup::format_pg_lsn;
use walrus::pg::replication::conn::{
    PgConfig, ReplicationConn, SystemIdentity, error_code, error_message, message_kind,
};
use walrus::pg::replication::stream::{
    Frame, build_status_update, decode_frame, now_pg_microseconds,
};
use walrus::pg::replication::tls::{SocketStream, SslMode, maybe_upgrade};

use crate::pg::{Xid, Xid8};
use crate::pos::{Floor, Pos, ResumeSafe, ShadowReplay, SourceReceived};
use crate::source::manifest::ShadowFloor;
use crate::toast::horizon_pin::HorizonPin;

/// Matches wal-rus / wal-g defaults; servers tolerate up to
/// `wal_sender_timeout` of silence (default 60s).
pub const DEFAULT_STATUS_INTERVAL: Duration = Duration::from_secs(10);

const RECEIVE_TIMEOUT: Duration = Duration::from_secs(60);
const PARKED_STATUS_TIMEOUT: Duration = Duration::from_secs(5);

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
/// target owns its slot and `[source] slot` names it, see docs/failover.md
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
    /// until [`end_stream`](SourceFeed::end_stream) answers.
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
    /// Full xid sent as hot-standby feedback xmin, zero releases a held one.
    /// None sends no feedback
    pub horizon_xmin: Option<u64>,
}

impl StandbyStatus {
    /// `apply` stays under shadow replay and the resume-safe ack, `flush`
    /// also under the persisted floor a crash-now restart asks for. `shadow`
    /// stands in for a replay LSN shadow has not reported yet
    pub fn bounded(
        write_lsn: Pos<SourceReceived>,
        floor: Pos<Floor>,
        resume_safe: Pos<ResumeSafe>,
        shadow_replay: Pos<ShadowReplay>,
        shadow: ShadowFloor,
    ) -> Self {
        // Replay and resume-safe ack share WAL position space, so either caps apply
        let apply_lsn = if shadow_replay.is_zero() {
            shadow.bound(resume_safe)
        } else {
            resume_safe.min(shadow_replay.retag())
        };
        Self {
            write_lsn,
            flush_lsn: floor.min(apply_lsn.retag()),
            apply_lsn,
            horizon_xmin: None,
        }
    }

    /// All three slots at the same value.
    pub fn collapsed(lsn: u64) -> Self {
        Self {
            write_lsn: Pos::new(lsn),
            flush_lsn: Pos::new(lsn),
            apply_lsn: Pos::new(lsn),
            horizon_xmin: None,
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

/// PostgreSQL sets a physical slot's restart_lsn to any valid flush position
/// (`PhysicalConfirmReceivedLocation`). Sending a lower value can move slot
/// behind recycled WAL and make it eligible for invalidation under
/// max_slot_wal_keep_size. Keep each field at its highest reported value.
/// Report flush position only when it advances; otherwise send `InvalidXLogRecPtr`.
/// PostgreSQL leaves slot's `restart_lsn` unchanged for an invalid flush position
/// (`ProcessStandbyReplyMessage`).
///
/// During shutdown, `WalSndDone` waits for acknowledgement of all sent WAL.
/// An invalid flush position makes it use `write`, as with pg_receivewal.
/// Repeating a flush position at a segment boundary cannot acknowledge a shutdown
/// checkpoint ending inside a segment, so source shutdown waits for sender timeout.
fn prepare_status(status: StandbyStatus, floors: &mut StatusFloors) -> StandbyStatus {
    let previous_flush = floors.flush;
    floors.write = status.write_lsn.max(floors.write);
    floors.flush = status.flush_lsn.max(floors.flush);
    floors.apply = status.apply_lsn.max(floors.apply);
    StandbyStatus {
        write_lsn: floors.write,
        flush_lsn: if floors.flush > previous_flush {
            floors.flush
        } else {
            Pos::ZERO
        },
        apply_lsn: floors.apply,
        horizon_xmin: status.horizon_xmin,
    }
}

/// Hot-standby feedback `'h'` with no catalog xmin, see PG
/// `ProcessStandbyHSFeedbackMessage`. Physical slot persists xmin, later
/// feedback only raises it
fn build_hs_feedback(xmin: u64) -> [u8; 25] {
    let mut out = [0; 25];
    out[0] = b'h';
    out[1..9].copy_from_slice(&now_pg_microseconds().to_be_bytes());
    out[9..13].copy_from_slice(&(xmin as u32).to_be_bytes());
    out[13..17].copy_from_slice(&((xmin >> 32) as u32).to_be_bytes());
    out
}

/// Standby source forwards xmin to its primary, where vacuum runs, only with
/// `hot_standby_feedback`, see PG `XLogWalRcvSendHSFeedback`
fn warn_unforwarded_feedback(unforwarded: bool) {
    if unforwarded {
        tracing::warn!(
            target: "walshadow::source_feed",
            "source is a standby with hot_standby_feedback off, horizon pin holds nothing on its primary",
        );
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
    /// Feedback xmin source last heard and keeps until told otherwise
    horizon_sent: Option<u64>,
    receive_deadline: Option<Instant>,
    parked_status_error: Option<anyhow::Error>,
    floors: StatusFloors,
    /// Most recent `server_wal_end` from a WAL frame or keepalive;
    /// surfaces "source ahead by N" without re-issuing IDENTIFY_SYSTEM.
    last_server_wal_end: u64,
    /// Started on first `horizon_samples` call, stops with feed
    horizon: Option<HorizonSampler>,
}

/// Running-xacts records arrive every 15s (`LOG_SNAPSHOT_INTERVAL_MS`)
const HORIZON_SAMPLE_INTERVAL: Duration = Duration::from_secs(1);

/// Statement snapshot precedes reading insert position, so every transaction
/// writing at or after that position has an xid at or above snapshot xmin.
/// Standby sources return no row, their running-xacts records still pin
const HORIZON_SAMPLE_SQL: &str = "SELECT pg_current_wal_insert_lsn(), \
     pg_snapshot_xmin(pg_current_snapshot()) WHERE NOT pg_is_in_recovery()";

struct HorizonSampler {
    rx: tokio::sync::mpsc::Receiver<(u64, u64)>,
    _task: tokio_util::task::AbortOnDropHandle<()>,
}

impl HorizonSampler {
    fn spawn(cfg: PgConfig, every: Duration) -> Self {
        let (tx, rx) = tokio::sync::mpsc::channel(64);
        let task = tokio::spawn(async move {
            let mut tick = tokio::time::interval(every);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            let mut client = None;
            let mut failing = false;
            loop {
                tick.tick().await;
                let sampled = async {
                    let c = if let Some(c) = &client {
                        c
                    } else {
                        client.insert(open_sql_client(&cfg).await?)
                    };
                    c.query(HORIZON_SAMPLE_SQL, &[])
                        .await
                        .context("sample source snapshot")
                };
                let rows = match sampled.await {
                    Ok(rows) => rows,
                    Err(e) => {
                        client = None;
                        if !failing {
                            tracing::warn!(
                                target: "walshadow::source_feed",
                                error = %format!("{e:#}"),
                                "horizon sampler failed, pin follows running-xacts records until it recovers",
                            );
                        }
                        failing = true;
                        continue;
                    }
                };
                if failing {
                    tracing::info!(target: "walshadow::source_feed", "horizon sampler recovered");
                    failing = false;
                }
                for row in rows {
                    let (lsn, Xid8(xmin)): (PgLsn, Xid8) = (row.get(0), row.get(1));
                    // Pump drains every iteration, a full channel only skips samples
                    let _ = tx.try_send((lsn.into(), xmin));
                }
            }
        });
        Self {
            rx,
            _task: tokio_util::task::AbortOnDropHandle::new(task),
        }
    }
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
            horizon_sent: None,
            receive_deadline: None,
            parked_status_error: None,
            floors: StatusFloors::default(),
            last_server_wal_end: 0,
            horizon: None,
        })
    }

    /// `(insert LSN, full snapshot xmin)` pairs sampled since last call, on a
    /// connection of their own so a slow source never stalls pump. None on
    /// first call, which starts sampling this connection
    pub fn horizon_samples(&mut self) -> Option<Vec<(u64, u64)>> {
        let Some(sampler) = &mut self.horizon else {
            self.horizon = Some(HorizonSampler::spawn(
                self.cfg.clone(),
                HORIZON_SAMPLE_INTERVAL,
            ));
            return None;
        };
        Some(std::iter::from_fn(|| sampler.rx.try_recv().ok()).collect())
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
        self.conn.identify_system().await
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
        self.receive_deadline = None;
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
            // tokio::select! drops losing futures.
            let deadline = *self
                .receive_deadline
                .get_or_insert_with(|| Instant::now() + RECEIVE_TIMEOUT);
            anyhow::ensure!(
                Instant::now() < deadline,
                "source replication receive timeout"
            );
            self.report(status, false).await;
            let wake = self
                .status_due()
                .max(Instant::now() + Duration::from_millis(50));
            let Ok(msg) =
                tokio::time::timeout_at(deadline.min(wake), self.conn.recv_message()).await
            else {
                if let Some(err) = self.parked_status_error.take() {
                    return Err(err);
                }
                continue;
            };
            let msg = msg?;
            self.receive_deadline = None;
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
                                self.report(status, true).await;
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
    pub async fn end_stream(&mut self) -> Result<()> {
        self.conn.end_copy().await.context("end replication COPY")
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

    fn status_due(&self) -> Instant {
        self.last_status
            + self
                .status_interval
                .clamp(Duration::from_millis(50), RECEIVE_TIMEOUT / 2)
    }

    pub fn parked_status_due(&self) -> Option<Instant> {
        self.parked_status_error
            .is_none()
            .then(|| self.status_due())
    }

    /// Keep COPY open while the pump holds WAL.
    pub async fn send_parked_status(&mut self, status: StandbyStatus) {
        let result = tokio::time::timeout(PARKED_STATUS_TIMEOUT, self.send_status(status, false))
            .await
            .context("source status write timed out while dispatching WAL")
            .and_then(|result| result);
        if let Err(err) = result {
            tracing::warn!(target: "walshadow::source_feed", error = %err, "source feedback failed during WAL dispatch");
            // Reconnect after dispatch. A failed write may have sent a partial frame.
            self.parked_status_error = Some(err);
        }
    }

    /// Send changed feedback, and status when due or `reply` is owed. Source
    /// holds nothing until first feedback, and vacuums past a stale pin.
    /// A failed write parks: source may have closed behind frames still
    /// unread, such as WAL up to a switchpoint
    async fn report(&mut self, status: StandbyStatus, reply: bool) {
        if self.parked_status_error.is_some() {
            return;
        }
        let sent = async {
            self.send_feedback(status.horizon_xmin).await?;
            if reply || Instant::now() >= self.status_due() {
                self.send_status(status, true).await?;
            }
            anyhow::Ok(())
        };
        if let Err(err) = sent.await {
            self.parked_status_error = Some(err);
        }
    }

    async fn send_status(&mut self, status: StandbyStatus, request_reply: bool) -> Result<()> {
        let held = prepare_status(status, &mut self.floors);
        let mut payload = build_status_update(
            held.write_lsn.get(),
            held.flush_lsn.get(),
            held.apply_lsn.get(),
        );
        // Avoid soliciting replies while the pump isn't reading from COPY.
        *payload.last_mut().expect("standby status reply flag") = u8::from(request_reply);
        self.conn.send_copy_data(&payload).await?;
        self.send_feedback(held.horizon_xmin).await?;
        self.last_status = Instant::now();
        Ok(())
    }

    /// Feedback frame alone, so a moving pin neither asks source for a
    /// keepalive nor defers timed status. Source keeps xmin it last heard, so
    /// an unchanged one is not sent again
    async fn send_feedback(&mut self, xmin: Option<u64>) -> Result<()> {
        if let Some(xmin) = xmin.filter(|x| self.horizon_sent != Some(*x)) {
            self.conn.send_copy_data(&build_hs_feedback(xmin)).await?;
            self.horizon_sent = Some(xmin);
        }
        Ok(())
    }

    /// Seed horizon pin from source's current snapshot, `slot`'s held xmin and
    /// `saved`, xmin an earlier run held for floor this one resumes from
    pub async fn horizon_pin_seed(
        &mut self,
        slot: Option<&str>,
        saved: Option<u64>,
    ) -> Result<HorizonPin> {
        let client = self.sql_client().await?;
        let row = client
            .query_one(
                "SELECT pg_snapshot_xmax(s), pg_snapshot_xmin(s), \
                 (SELECT xmin FROM pg_replication_slots WHERE slot_name = $1), \
                 (SELECT max(age(datfrozenxid)) FROM pg_database), \
                 pg_is_in_recovery() AND NOT current_setting('hot_standby_feedback')::bool \
                 FROM pg_current_snapshot() s",
                &[&slot],
            )
            .await
            .context("read source xid horizon")?;
        let (Xid8(next), Xid8(oldest)) = (row.get(0), row.get(1));
        let frozen_age: i32 = row.get(3);
        let slot_xmin: Option<Xid> = row.get(2);
        warn_unforwarded_feedback(row.get(4));
        // Rows deleted below oldest datfrozenxid are gone, and an xmin that
        // far back would stall freezing
        let frozen = next - frozen_age as u64;
        Ok(HorizonPin::seed(
            next,
            oldest,
            slot_xmin.map(|Xid(x)| x),
            saved.map(|x| x.max(frozen)),
        ))
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

    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    async fn backend(socket: &mut TcpStream, tag: u8, body: &[u8]) {
        let mut frame = vec![tag];
        frame.extend_from_slice(&(body.len() as u32 + 4).to_be_bytes());
        frame.extend_from_slice(body);
        socket.write_all(&frame).await.unwrap();
    }

    fn mock_cfg(port: u16) -> PgConfig {
        PgConfig {
            host: "127.0.0.1".into(),
            port,
            user: "test".into(),
            password: None,
            database: "test".into(),
            application_name: "source-timeout-test".into(),
            sslmode: SslMode::Disable,
            tls: Default::default(),
        }
    }

    /// Feed past startup, and the backend socket it talks to
    async fn mock_session() -> (SourceFeed, TcpStream) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let cfg = mock_cfg(listener.local_addr().unwrap().port());
        let server = async {
            let (mut socket, _) = listener.accept().await.unwrap();
            socket.set_nodelay(true).unwrap();
            let len = socket.read_u32().await.unwrap() as usize;
            socket.read_exact(&mut vec![0; len - 4]).await.unwrap();
            backend(&mut socket, b'R', &0u32.to_be_bytes()).await;
            backend(&mut socket, b'S', b"server_version\x0016.3\0").await;
            backend(&mut socket, b'Z', b"I").await;
            socket
        };
        let (feed, socket) = tokio::join!(SourceFeed::connect(&cfg), server);
        (feed.unwrap(), socket)
    }

    /// Read one simple query off the backend socket
    async fn query(socket: &mut TcpStream) -> String {
        assert_eq!(socket.read_u8().await.unwrap(), b'Q');
        let len = socket.read_u32().await.unwrap() as usize;
        let mut body = vec![0; len - 4];
        socket.read_exact(&mut body).await.unwrap();
        String::from_utf8(body).unwrap()
    }

    fn error_response(code: &str) -> Vec<u8> {
        format!("SERROR\0C{code}\0Mrefused\0\0").into_bytes()
    }

    async fn mock_feed() -> (SourceFeed, TcpStream) {
        let (mut feed, mut socket) = mock_session().await;
        let server = async {
            query(&mut socket).await;
            backend(&mut socket, b'W', &[0, 0, 0]).await;
        };
        let (started, ()) = tokio::join!(feed.start_physical_replication(None, 0, 1), server);
        started.unwrap();
        (feed, socket)
    }

    #[tokio::test]
    async fn parked_status_keeps_copy_alive_without_advancing_flush() {
        let (mut feed, mut server) = mock_feed().await;
        let status = StandbyStatus {
            write_lsn: Pos::new(0x4000),
            flush_lsn: Pos::new(0x2000),
            apply_lsn: Pos::new(0x3000),
            horizon_xmin: Some((5 << 32) + 777),
        };

        for flush in [0x2000, 0] {
            feed.send_parked_status(status).await;
            assert!(feed.parked_status_due().is_some());
            assert_eq!(server.read_u8().await.unwrap(), b'd');
            assert_eq!(server.read_u32().await.unwrap(), 38);
            assert_eq!(server.read_u8().await.unwrap(), b'r');
            assert_eq!(server.read_u64().await.unwrap(), 0x4000);
            assert_eq!(server.read_u64().await.unwrap(), flush);
            assert_eq!(server.read_u64().await.unwrap(), 0x3000);
            let _timestamp = server.read_u64().await.unwrap();
            assert_eq!(server.read_u8().await.unwrap(), 0, "no reply requested");
            // Source keeps xmin it heard, second status repeats none
            if flush == 0 {
                continue;
            }
            assert_eq!(server.read_u8().await.unwrap(), b'd');
            assert_eq!(server.read_u32().await.unwrap(), 29);
            assert_eq!(server.read_u8().await.unwrap(), b'h');
            let _timestamp = server.read_u64().await.unwrap();
            assert_eq!(server.read_u32().await.unwrap(), 777, "xmin");
            assert_eq!(server.read_u32().await.unwrap(), 5, "epoch");
            assert_eq!(server.read_u64().await.unwrap(), 0, "no catalog xmin");
        }
    }

    #[tokio::test]
    async fn failed_parked_status_surfaces_on_next_event() {
        let (mut feed, server) = mock_feed().await;
        drop(server);
        // Peer close can take a write or two to surface as EPIPE
        for _ in 0..100 {
            if feed.parked_status_due().is_none() {
                break;
            }
            feed.send_parked_status(StandbyStatus::default()).await;
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(feed.parked_status_due().is_none(), "write never failed");
        let mut buf = Vec::new();
        assert!(
            feed.next_event(StandbyStatus::default(), &mut buf)
                .await
                .is_err()
        );
    }

    /// Parked write error waits for a silent source, not one still sending
    #[tokio::test]
    async fn parked_error_surfaces_once_source_goes_quiet() {
        let (mut feed, _server) = mock_feed().await;
        feed.parked_status_error = Some(anyhow::anyhow!("parked"));
        tokio::time::pause();
        let mut buf = Vec::new();
        let e = feed.next_event(StandbyStatus::default(), &mut buf).await;
        assert_eq!(e.unwrap_err().to_string(), "parked");
    }

    /// Source that closed behind its last frames fails a write first. Frames
    /// still read, then the close surfaces
    #[tokio::test]
    async fn failed_write_still_reads_frames_source_sent() {
        let (mut feed, mut socket) = mock_feed().await;
        for lsn in [0x100u64, 0x104] {
            let mut frame = vec![b'w'];
            frame.extend_from_slice(&lsn.to_be_bytes());
            frame.extend_from_slice(&0x108u64.to_be_bytes());
            frame.extend_from_slice(&0u64.to_be_bytes());
            frame.extend_from_slice(b"WAL!");
            backend(&mut socket, b'd', &frame).await;
        }
        drop(socket);
        let mut buf = Vec::new();
        // Each changed pin writes ahead of the read
        for (xmin, lsn) in [(700, 0x100), (800, 0x104)] {
            let status = StandbyStatus {
                horizon_xmin: Some(xmin),
                ..StandbyStatus::default()
            };
            let event = feed.next_event(status, &mut buf).await;
            assert!(matches!(event, Ok(SourceEvent::Wal(c)) if c.start_lsn == lsn));
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let status = StandbyStatus {
            horizon_xmin: Some(900),
            ..StandbyStatus::default()
        };
        assert!(feed.next_event(status, &mut buf).await.is_err());
    }

    /// Pin change sends feedback alone ahead of timer, held pin sends nothing
    #[tokio::test]
    async fn pin_change_sends_feedback_ahead_of_timer() {
        let (mut feed, mut server) = mock_feed().await;
        let poll = Duration::from_millis(20);
        let mut buf = Vec::new();
        for (xmin, sent) in [(700, true), (700, false), (900, true)] {
            let status = StandbyStatus {
                horizon_xmin: Some(xmin),
                ..StandbyStatus::default()
            };
            let event = tokio::time::timeout(poll, feed.next_event(status, &mut buf)).await;
            assert!(event.is_err(), "source sent nothing");
            if !sent {
                let early = tokio::time::timeout(poll, server.read_u8()).await;
                assert!(early.is_err(), "held pin sent ahead of timer");
                continue;
            }
            let mut frame = [0; 30];
            server.read_exact(&mut frame).await.unwrap();
            assert_eq!(frame[5], b'h', "no status frame ahead of feedback");
            assert_eq!(frame[14..18], (xmin as u32).to_be_bytes());
        }
        let late = tokio::time::timeout(poll, server.read_u8()).await;
        assert!(late.is_err(), "pin change deferred or sent timed status");
    }

    #[test]
    fn standby_without_feedback_forwarding_warns() {
        warn_unforwarded_feedback(false);
        warn_unforwarded_feedback(true);
    }

    #[tokio::test]
    async fn silent_source_times_out_despite_cancelled_polls() {
        let (mut feed, _server) = mock_feed().await;
        tokio::time::pause();
        let started = Instant::now();
        let mut buf = Vec::new();
        loop {
            match tokio::time::timeout(
                Duration::from_millis(250),
                feed.next_event(StandbyStatus::default(), &mut buf),
            )
            .await
            {
                Err(_) => assert!(started.elapsed() < RECEIVE_TIMEOUT),
                Ok(Err(_)) => break,
                Ok(Ok(event)) => panic!("unexpected event: {event:?}"),
            }
        }
    }

    fn triple(s: StandbyStatus) -> (u64, u64, u64) {
        (s.write_lsn.get(), s.flush_lsn.get(), s.apply_lsn.get())
    }

    #[test]
    fn status_keeps_each_field_independent() {
        let mut floors = StatusFloors::default();
        // leading write must not lift flush/apply
        let held = prepare_status(
            StandbyStatus {
                write_lsn: 3000.into(),
                flush_lsn: 1000.into(),
                apply_lsn: 800.into(),
                ..StandbyStatus::default()
            },
            &mut floors,
        );
        assert_eq!(triple(held), (3000, 1000, 800));
        // higher flush/apply, same write: each rises from its own floor
        let held = prepare_status(
            StandbyStatus {
                write_lsn: 3000.into(),
                flush_lsn: 1500.into(),
                apply_lsn: 1200.into(),
                ..StandbyStatus::default()
            },
            &mut floors,
        );
        assert_eq!(triple(held), (3000, 1500, 1200));
    }

    #[test]
    fn status_retains_floors_when_inputs_regress() {
        let mut floors = StatusFloors::default();
        prepare_status(
            StandbyStatus {
                write_lsn: 5000.into(),
                flush_lsn: 4000.into(),
                apply_lsn: 4000.into(),
                ..StandbyStatus::default()
            },
            &mut floors,
        );
        // stale lower values hold at prior floor
        let held = prepare_status(
            StandbyStatus {
                write_lsn: 4500.into(),
                flush_lsn: 3000.into(),
                apply_lsn: 3500.into(),
                ..StandbyStatus::default()
            },
            &mut floors,
        );
        assert_eq!(triple(held), (5000, 0, 4000));
        assert_eq!(floors.flush.get(), 4000);
    }

    /// Send an invalid flush position when unchanged, allowing shutdown to use
    /// `write` for acknowledgement without moving slot's `restart_lsn`.
    #[test]
    fn status_reports_flush_only_when_it_advances() {
        let mut floors = StatusFloors::default();
        for (flush, expected) in [(0, 0), (2000, 2000), (2000, 0), (1000, 0), (3000, 3000)] {
            let status = prepare_status(
                StandbyStatus {
                    write_lsn: 4000.into(),
                    flush_lsn: flush.into(),
                    apply_lsn: 0.into(),
                    ..StandbyStatus::default()
                },
                &mut floors,
            );
            assert_eq!(status.flush_lsn.get(), expected);
        }
        assert_eq!(floors.flush.get(), 3000);
    }

    #[test]
    fn bounded_status_stays_under_every_consumer() {
        let open = ShadowFloor::unbounded();
        let s = StandbyStatus::bounded(9000.into(), 4000.into(), 7000.into(), 5000.into(), open);
        assert_eq!(triple(s), (9000, 4000, 5000));
        let s = StandbyStatus::bounded(9000.into(), 8000.into(), 7000.into(), 0.into(), open);
        assert_eq!(triple(s), (9000, 7000, 7000), "no replay reported yet");
        let seeded = ShadowFloor::new(true, 0, crate::record::WAL_SEG_SIZE + 5);
        let seg = crate::record::WAL_SEG_SIZE;
        let s = StandbyStatus::bounded(
            (9 * seg).into(),
            (8 * seg).into(),
            (7 * seg).into(),
            0.into(),
            seeded,
        );
        assert_eq!(triple(s), (9 * seg, seg, seg), "persisted replay stands in");
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

    /// A refusal before COPY opens is typed as a removed segment only on PG's
    /// `undefined_file` SQLSTATE
    #[tokio::test]
    async fn start_refusal_reads_its_sqlstate() {
        for (code, removed) in [(SQLSTATE_UNDEFINED_FILE, true), ("XX000", false)] {
            let (mut feed, mut socket) = mock_session().await;
            let server = async {
                query(&mut socket).await;
                backend(&mut socket, b'E', &error_response(code)).await;
            };
            let (started, ()) = tokio::join!(feed.start_physical_replication(None, 0, 1), server);
            let e = started.unwrap_err();
            assert_eq!(is_wal_segment_removed(&e), removed, "{e:#}");
            assert!(format!("{e:#}").contains(code), "{e:#}");
        }
    }

    #[tokio::test]
    async fn stream_skips_unexpected_backend_messages() {
        let (mut feed, mut socket) = mock_feed().await;
        backend(&mut socket, b'A', b"\0\0\0\x01chan\0note\0").await;
        let mut frame = vec![b'w'];
        frame.extend_from_slice(&0x100u64.to_be_bytes());
        frame.extend_from_slice(&0x104u64.to_be_bytes());
        frame.extend_from_slice(&0u64.to_be_bytes());
        frame.extend_from_slice(b"WAL!");
        backend(&mut socket, b'd', &frame).await;
        let mut buf = Vec::new();
        let event = feed.next_event(StandbyStatus::default(), &mut buf).await;
        assert!(
            matches!(event, Ok(SourceEvent::Wal(c)) if c.start_lsn == 0x100 && c.data == b"WAL!")
        );
        assert_eq!(feed.last_server_wal_end(), 0x104);
    }

    #[tokio::test]
    async fn timeline_history_failure_names_the_timeline() {
        let (mut feed, mut socket) = mock_session().await;
        let server = async {
            assert!(query(&mut socket).await.starts_with("TIMELINE_HISTORY 2"));
            backend(&mut socket, b'E', &error_response("XX000")).await;
        };
        let (history, ()) = tokio::join!(feed.timeline_history(2), server);
        let e = history.unwrap_err();
        assert_eq!(e.to_string(), "TIMELINE_HISTORY 2");
    }

    /// Read frontend messages through one tagged `until`
    async fn frontend(socket: &mut TcpStream, until: u8) {
        loop {
            let tag = socket.read_u8().await.unwrap();
            let len = socket.read_u32().await.unwrap() as usize;
            socket.read_exact(&mut vec![0; len - 4]).await.unwrap();
            if tag == until {
                break;
            }
        }
    }

    /// Unreachable source yields no samples, failed query redials until a
    /// session answers
    #[tokio::test]
    async fn horizon_sampler_redials_after_failed_query() {
        let closed = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut unreachable = HorizonSampler::spawn(
            mock_cfg(closed.local_addr().unwrap().port()),
            Duration::from_millis(5),
        );
        drop(closed);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let cfg = mock_cfg(listener.local_addr().unwrap().port());
        let mut sampler = HorizonSampler::spawn(cfg, Duration::from_millis(5));
        // Each session completes startup, then hangs up on sample query
        for _ in 0..2 {
            let (mut socket, _) = listener.accept().await.unwrap();
            let len = socket.read_u32().await.unwrap() as usize;
            socket.read_exact(&mut vec![0; len - 4]).await.unwrap();
            backend(&mut socket, b'R', &0u32.to_be_bytes()).await;
            backend(&mut socket, b'Z', b"I").await;
            socket.read_u8().await.unwrap();
        }
        // Standby source answers with no row. Wait for next sample's parse,
        // sent once sampler handled this answer
        let (mut socket, _) = listener.accept().await.unwrap();
        let len = socket.read_u32().await.unwrap() as usize;
        socket.read_exact(&mut vec![0; len - 4]).await.unwrap();
        backend(&mut socket, b'R', &0u32.to_be_bytes()).await;
        backend(&mut socket, b'Z', b"I").await;
        let prepared: [(u8, &[u8]); 3] = [(b'1', b""), (b't', &[0, 0]), (b'n', b"")];
        let executed: [(u8, &[u8]); 2] = [(b'2', b""), (b'C', b"SELECT 0\0")];
        for answer in [&prepared[..], &executed[..]] {
            frontend(&mut socket, b'S').await;
            for &(tag, body) in answer {
                backend(&mut socket, tag, body).await;
            }
            backend(&mut socket, b'Z', b"I").await;
        }
        frontend(&mut socket, b'P').await;
        assert!(sampler.rx.try_recv().is_err());
        assert!(unreachable.rx.try_recv().is_err());
    }

    /// Sidecar SQL rides the replication connection's transport settings
    #[tokio::test]
    async fn sql_client_dial_failures_name_the_endpoint() {
        let closed = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut cfg = mock_cfg(closed.local_addr().unwrap().port());
        cfg.password = Some("pw".into());
        drop(closed);
        let e = open_sql_client(&cfg).await.err().unwrap();
        assert!(e.to_string().starts_with("connect to 127.0.0.1:"), "{e:#}");

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        cfg.port = listener.local_addr().unwrap().port();
        let refuse_tls = async {
            let (mut socket, _) = listener.accept().await.unwrap();
            socket.read_exact(&mut [0; 8]).await.unwrap();
            socket.write_all(b"N").await.unwrap();
            let (mut plain, _) = listener.accept().await.unwrap();
            plain.read_u32().await.unwrap();
        };
        let dial = async {
            cfg.sslmode = SslMode::Require;
            let tls = open_sql_client(&cfg).await.err().unwrap();
            cfg.sslmode = SslMode::Disable;
            let plain = open_sql_client(&cfg).await.err().unwrap();
            (tls, plain)
        };
        let ((tls, plain), ()) = tokio::join!(dial, refuse_tls);
        assert!(
            tls.to_string().starts_with("tls negotiation against"),
            "{tls:#}"
        );
        assert!(
            !plain.to_string().starts_with("tls negotiation"),
            "{plain:#}"
        );
    }
}
