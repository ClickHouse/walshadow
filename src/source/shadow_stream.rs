//! Streaming-fed shadow.
//!
//! `ShadowStreamSink` frames each filtered record as `'w'` `XLogData`
//! (physical replication protocol) onto every active shadow
//! connection's send buffer. Inbound `'r'` standby-status frames carry
//! shadow's flush/apply LSNs back; the min across connections gates the
//! catalog.
//!
//! Backpressure: a send buffer past `slow_connection_threshold` is dropped and
//! the walreceiver reconnects. Since the pump streams live (it doesn't replay
//! history), a reconnect lands *behind* the head; [`ShadowStreamState`] retains
//! the current segment's wire bytes and backfills `[reconnect_lsn, head]` on
//! connect so the stream stays contiguous. Older complete segments come from
//! the archive (`restore_command`), so a start below the retained bytes is
//! refused rather than answered from the head; only the in-progress segment,
//! which the archive lacks, must come over the wire.

use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use smallvec::SmallVec;
use thiserror::Error;
use tokio::net::{TcpListener, UnixListener};
use tokio::sync::Mutex;
use walrus::pg::backup::format_pg_lsn;
use walrus::pg::replication::server::{
    self, ServerError, TimelineSwitch, WalSenderConn, decode_standby_status,
};
use walrus::pg::replication::stream::{encode_keepalive_frame_into, encode_wal_data_frame_into};

use crate::pos::{
    AppliedWake, Gate, Monotone, Pos, QueuedWake, ShadowDispatched, ShadowFlush, ShadowReplay,
    SourceReceived,
};
use crate::record::{RecordBytesSink, SinkError};
use foldhash::{HashMap, HashMapExt};

/// libpq environment for shadow's walreceiver. This walsender speaks protocol
/// 3.0 and sends no `NegotiateProtocolVersion`; PG 19 beta libpq greases every
/// handshake with protocol 3.9999 plus `_pq_.test_protocol_negotiation` and
/// drops a server that accepts either without negotiating. PG 16-17 libpq has
/// no such option and never reads the variable
pub const WALRECEIVER_PROTOCOL_PIN: (&str, &str) = ("PGMAXPROTOCOLVERSION", "3.0");

#[derive(Debug, Clone, Copy)]
enum Phase {
    /// Switchpoint the branch stops at, PG's `sendTimeLineValidUpto`. `None`
    /// on the current timeline
    Streaming(Option<TimelineSwitch>),
    /// Client must receive next-timeline result
    Ended(TimelineSwitch),
    /// Teardown with optional pending timeline result
    Closing(Option<TimelineSwitch>),
}

impl Phase {
    fn is_streaming(self) -> bool {
        matches!(self, Self::Streaming(_))
    }

    fn is_closing(self) -> bool {
        matches!(self, Self::Closing(_))
    }

    /// Switchpoint this connection is pinned to, whether or not it is served
    fn cut(self) -> Option<TimelineSwitch> {
        match self {
            Self::Streaming(c) | Self::Closing(c) => c,
            Self::Ended(s) => Some(s),
        }
    }

    fn ended(self) -> Option<TimelineSwitch> {
        if let Self::Ended(s) | Self::Closing(Some(s)) = self {
            Some(s)
        } else {
            None
        }
    }

    /// Only a streaming connection reaches its switchpoint, though an overflow
    /// on that same dispatch may already have marked it closing
    fn end(&mut self, switch: TimelineSwitch) {
        *self = if self.is_closing() {
            Self::Closing(Some(switch))
        } else {
            Self::Ended(switch)
        };
    }

    fn close(&mut self) -> bool {
        if self.is_closing() {
            return false;
        }
        *self = Self::Closing(self.ended());
        true
    }
}

/// Per-connection state for one WAL-consuming client (typically
/// shadow PG).
#[derive(Debug)]
struct ConnState {
    /// High water of bytes pushed onto the send buffer; source's
    /// `write_lsn` equivalent.
    dispatched_lsn: Pos<ShadowDispatched>,
    /// Last `flush_lsn` from the client's `'r'` standby status.
    flush_lsn: Pos<ShadowFlush>,
    /// Last `apply_lsn` from the client's `'r'` standby status.
    apply_lsn: Pos<ShadowReplay>,
    /// Bytes queued to catch up a reconnected client. Exclude from
    /// `slow_threshold` until listener drains queue: reconnecting alone
    /// does not mean client is slow.
    backfill_queued: usize,
    phase: Phase,
}

impl ConnState {
    fn fresh(start_lsn: u64, ends_at: Option<TimelineSwitch>) -> Self {
        Self {
            dispatched_lsn: Pos::new(start_lsn),
            flush_lsn: Pos::new(start_lsn),
            apply_lsn: Pos::new(start_lsn),
            backfill_queued: 0,
            phase: Phase::Streaming(ends_at),
        }
    }
}

/// Aggregate flush/apply LSN across shadow-streaming connections.
/// `None` with no active connections (catalog gate falls back to
/// disk-driven polling).
#[derive(Debug, Default, Clone, Copy)]
pub struct AggregateLsn {
    pub min_flush_lsn: Option<Pos<ShadowFlush>>,
    pub min_apply_lsn: Option<Pos<ShadowReplay>>,
    /// Connections past `START_REPLICATION`, the only ones carrying LSNs.
    pub active_connections: usize,
    /// Monotonic count of sockets that entered the handshake. Above
    /// `active_connections` means a client is connected but stalled before
    /// `START_REPLICATION`, which reads as "nothing attached" otherwise.
    pub accepted_total: u64,
    /// Monotonic count of connections dropped by `slow_threshold`
    /// overflow since process start.
    pub dropped_total: u64,
    /// Oldest branch an attached client is still reading: a historic
    /// connection is pinned to the branch its `ends_at` names, the rest to the
    /// served one. `None` with nothing attached. Beside the apply LSN it is
    /// what separates a shadow that followed a crossing from one that only
    /// survived it.
    pub replay_timeline: Option<u32>,
}

/// Why a `START_REPLICATION` gets an `ErrorResponse` instead of a stream
#[derive(Debug, Error, PartialEq, Eq)]
pub enum Refused {
    /// Ahead of what this walsender has, or a branch it never knew. Another
    /// branch's bytes would be WAL the client's own history rejects
    #[error("requested timeline {0} is not in this server's history")]
    Timeline(u32),
    /// Below the retained bytes, so in the archive. Bytes from the head would
    /// leave a hole the client's startup process opens as streamed and PANICs
    /// on (PG `xlogrecovery.c` `WaitForWALToBecomeAvailable`)
    #[error(
        "requested WAL segment at {} has already been removed, this walsender retains WAL from {}",
        format_pg_lsn(*.start),
        format_pg_lsn(*.retained)
    )]
    Archived { start: u64, retained: u64 },
}

impl Refused {
    /// SQLSTATE a PG walsender sends for the same refusal
    fn sqlstate(&self) -> &'static str {
        match self {
            Self::Timeline(_) => "XX000",
            Self::Archived { .. } => crate::source_feed::SQLSTATE_UNDEFINED_FILE,
        }
    }
}

#[derive(Debug, Error)]
pub enum ShadowStreamError {
    #[error("io: {0}")]
    Io(#[from] io::Error),
    #[error("server: {0}")]
    Server(#[from] ServerError),
}

/// Shared between ShadowStreamSink + listener task. `Mutex` not
/// `RwLock`: write traffic (sink dispatch + accept) is symmetric to
/// read traffic (status sweep).
#[derive(Debug)]
pub struct ShadowStreamState {
    /// First-byte LSN every newly-accepted connection receives.
    pub current_lsn: u64,
    pub timeline: u32,
    pub system_identifier: String,
    /// `IDENTIFY_SYSTEM` `dbname` (always empty for physical replication).
    pub dbname: Option<String>,
    /// Branches this walsender has finished, plus the history bytes placing
    /// them. Append-only, one entry per source promotion; `IDENTIFY_SYSTEM`,
    /// `TIMELINE_HISTORY` and end-of-timeline are answered from both
    switches: Vec<TimelineSwitch>,
    histories: Vec<(u32, Vec<u8>)>,
    connections: HashMap<u64, ConnState>,
    next_conn_id: u64,
    /// Bytes queued behind a slow shadow client, bounded by
    /// `slow_threshold`.
    send_queues: HashMap<u64, Vec<u8>>,
    /// Slow-connection byte cutoff; past it the listener kills the socket.
    pub slow_threshold: usize,
    /// Populates `'w'`/`'k'` frame headers.
    pub server_wal_end: Pos<SourceReceived>,
    /// Surfaced via [`AggregateLsn::dropped_total`] for the
    /// `walshadow_shadow_stream_dropped_connections_total` gauge.
    dropped_total: u64,
    accepted_total: u64,
    /// Wire bytes `[wire_buf_start, server_wal_end]` not yet on disk, used to
    /// backfill a reconnect behind the live head (else it gets an unappliable
    /// gap and strands at segment boundaries). Spans up to two segments while
    /// the last sealed one writes
    wire_buf: Vec<u8>,
    wire_buf_start: u64,
    /// Wakes the listener to write now rather than on its batching tick;
    /// bumped by `request_status`, the one caller whose bytes a hold is
    /// waiting behind
    queued: Monotone<QueuedWake>,
    /// Bumped when a client's apply LSN advances: wakes a publication hold
    /// on the standby status that releases it
    applied: Monotone<AppliedWake>,
}

impl ShadowStreamState {
    pub fn new(
        timeline: u32,
        system_identifier: String,
        current_lsn: u64,
        slow_threshold: usize,
    ) -> Self {
        Self {
            current_lsn,
            timeline,
            system_identifier,
            dbname: None,
            switches: Vec::new(),
            histories: Vec::new(),
            connections: HashMap::new(),
            next_conn_id: 1,
            send_queues: HashMap::new(),
            slow_threshold,
            server_wal_end: Pos::new(current_lsn),
            dropped_total: 0,
            accepted_total: 0,
            wire_buf: Vec::new(),
            wire_buf_start: current_lsn,
            queued: Monotone::default(),
            applied: Monotone::default(),
        }
    }

    /// Wakes the listener out of its batching tick, for the paths that
    /// cannot wait one out ([`request_status`](Self::request_status))
    pub fn queued(&self) -> Gate<QueuedWake> {
        self.queued.watch()
    }

    /// Wakes when any client's apply LSN advances; the waiter re-reads
    /// [`aggregate`](Self::aggregate), which stays the release authority
    pub fn applied(&self) -> Gate<AppliedWake> {
        self.applied.watch()
    }

    fn wake_listener(&self) {
        self.queued.bump();
    }

    pub fn aggregate(&self) -> AggregateLsn {
        let served = self.timeline;
        let (active, min_flush, min_apply, branch) = self
            .connections
            .values()
            .filter(|c| !c.phase.is_closing())
            .fold(
                (0usize, Pos::new(u64::MAX), Pos::new(u64::MAX), u32::MAX),
                |(n, flush, apply, tli), c| {
                    (
                        n + 1,
                        flush.min(c.flush_lsn),
                        apply.min(c.apply_lsn),
                        tli.min(c.phase.cut().map_or(served, |s| s.timeline)),
                    )
                },
            );
        AggregateLsn {
            min_flush_lsn: (active > 0).then_some(min_flush),
            min_apply_lsn: (active > 0).then_some(min_apply),
            active_connections: active,
            accepted_total: self.accepted_total,
            dropped_total: self.dropped_total,
            replay_timeline: (active > 0).then_some(branch),
        }
    }

    /// Count a socket entering the handshake. Deliberately not a
    /// `register_connection`: per-connection state needs the `START_REPLICATION`
    /// LSN, and a placeholder would feed boundary holds a fabricated apply
    /// point.
    pub fn note_accepted(&mut self) {
        self.accepted_total += 1;
    }

    /// Refuse a `START_REPLICATION` this walsender cannot serve gap-free
    ///
    /// `ends_at` comes from the handshake, which resolved the request against
    /// [`identity`](Self::identity): a historic connection streams to that
    /// switchpoint and then gets the next-timeline result.
    pub fn register_connection(
        &mut self,
        start_lsn: u64,
        timeline: u32,
        ends_at: Option<TimelineSwitch>,
    ) -> Result<u64, Refused> {
        if ends_at.is_none() && timeline != 0 && timeline != self.timeline {
            return Err(Refused::Timeline(timeline));
        }
        if start_lsn < self.wire_buf_start {
            return Err(Refused::Archived {
                start: start_lsn,
                retained: self.wire_buf_start,
            });
        }
        let id = self.next_conn_id;
        self.next_conn_id += 1;
        self.connections
            .insert(id, ConnState::fresh(start_lsn, ends_at));
        self.backfill_connection(id, start_lsn);
        // Reconnecting at or past the switchpoint: nothing left on this branch,
        // so end it now rather than idling until the next dispatch
        if let Some(switch) = ends_at
            && self.connections[&id].dispatched_lsn.get() >= switch.ends_at
        {
            self.end_timeline_for(id, switch);
        }
        // New connection can lower aggregate without status progress
        self.applied.bump();
        Ok(id)
    }

    /// What the handshake answers `IDENTIFY_SYSTEM`, `TIMELINE_HISTORY`, and a
    /// historic `START_REPLICATION` from. Rebuilt per connection so a crossing
    /// mid-run is visible to the next client that dials in.
    pub fn identity(&self) -> server::Identity {
        server::Identity {
            system_id: self.system_identifier.clone(),
            timeline: self.timeline,
            // 0/0 (InvalidXLogRecPtr) skips PG's cascading-standby catch-up
            // wait: walshadow has no flush position of its own, and a real LSN
            // parks a walreceiver that resumes past it re-polling
            // `IDENTIFY_SYSTEM` until `wal_receiver_timeout`
            xlogpos: 0,
            dbname: self.dbname.clone(),
            switches: self.switches.clone(),
            histories: self.histories.clone(),
        }
    }

    /// Publish history bytes for a branch without moving off it.
    ///
    /// [`advertise_timeline`](Self::advertise_timeline) only carries history
    /// for branches this walsender crossed *onto*, so the one it boots on would
    /// have none while `IDENTIFY_SYSTEM` names it. A walreceiver fetches
    /// `TIMELINE_HISTORY` for every timeline in `[its own, the primary's]` it
    /// lacks locally (`WalRcvFetchTimeLineHistoryFiles`), so an unanswerable
    /// boot branch is a FATAL on its side and a reconnect loop on ours.
    pub fn seed_history(&mut self, tli: u32, history: Vec<u8>) {
        if self.histories.iter().any(|(t, _)| *t == tli) {
            return;
        }
        self.histories.push((tli, history));
    }

    /// Move the served branch to `next_tli`, forked at `switch_lsn`, with the
    /// descendant's history bytes.
    ///
    /// Connections on the branch that just ended keep streaming to the
    /// switchpoint; the listener then ends the timeline on the wire, which is
    /// how a walreceiver learns where the branch went. History bytes go with it
    /// because the client fetches `TIMELINE_HISTORY` for every timeline between
    /// its own and the one `IDENTIFY_SYSTEM` reports
    /// (`src/backend/replication/walreceiver.c`,
    /// `WalRcvFetchTimeLineHistoryFiles`) and writes the answer into its `pg_wal`.
    pub fn advertise_timeline(&mut self, next_tli: u32, switch_lsn: u64, history: Vec<u8>) {
        let switch = TimelineSwitch {
            timeline: self.timeline,
            ends_at: switch_lsn,
            next_timeline: next_tli,
        };
        self.switches.push(switch);
        self.histories.push((next_tli, history));
        self.timeline = next_tli;
        let mut caught_up = SmallVec::<[u64; 4]>::new();
        for (id, c) in self.connections.iter_mut() {
            let Phase::Streaming(cut @ None) = &mut c.phase else {
                continue;
            };
            *cut = Some(switch);
            // Already served everything below the fork: nothing more will be
            // dispatched to notice, and an idle source would leave it waiting
            if c.dispatched_lsn.get() >= switch.ends_at {
                caught_up.push(*id);
            }
        }
        for id in caught_up {
            self.end_timeline_for(id, switch);
        }
        self.wake_listener();
    }

    /// Append wire bytes, replacing any retained from `start_lsn` on, as when a
    /// rebuilt `WalStream` replays its segment. A gap would break the
    /// `Refused::Archived` premise that everything below `wire_buf_start` is in
    /// the archive
    fn retain_wire(&mut self, start_lsn: u64, bytes: &[u8]) {
        let end = self.wire_buf_start + self.wire_buf.len() as u64;
        debug_assert!(
            (self.wire_buf_start..=end).contains(&start_lsn),
            "wire chunk at {start_lsn:#X} outside retained {:#X}..={end:#X}",
            self.wire_buf_start
        );
        self.wire_buf
            .truncate((start_lsn - self.wire_buf_start) as usize);
        self.wire_buf.extend_from_slice(bytes);
    }

    /// Drop retained wire bytes below `lsn` (completed segments; `restore_command`
    /// serves those), keeping `[lsn, head]` for in-progress-segment backfill.
    fn trim_wire_buf_before(&mut self, lsn: u64) {
        if lsn <= self.wire_buf_start {
            return;
        }
        let drop = ((lsn - self.wire_buf_start) as usize).min(self.wire_buf.len());
        self.wire_buf.drain(..drop);
        self.wire_buf_start = lsn;
    }

    /// Frame `bytes` (at `start_lsn`) to every connection past its dispatched
    /// point, bump `server_wal_end`, and retain for backfill.
    fn dispatch_wire(&mut self, start_lsn: u64, bytes: &[u8]) {
        if bytes.is_empty() {
            return;
        }
        let end_lsn = start_lsn + bytes.len() as u64;
        self.server_wal_end = self.server_wal_end.max(Pos::new(end_lsn));
        self.retain_wire(start_lsn, bytes);
        let server_wal_end = self.server_wal_end.get();
        // Snapshot: enqueue below takes `&mut self`, so the map can't stay borrowed
        let targets: SmallVec<[(u64, u64, Option<TimelineSwitch>); 4]> = self
            .connections
            .iter()
            // A connection already at its switchpoint is done: descendant bytes
            // must never go down a stream the client reads as ancestor
            .filter(|(_, c)| c.phase.is_streaming() && c.dispatched_lsn.get() < end_lsn)
            .map(|(id, c)| (*id, c.dispatched_lsn.get(), c.phase.cut()))
            .collect();
        for (id, conn_offset, ends_at) in targets {
            let cut = ends_at.filter(|s| s.ends_at < end_lsn);
            let stop = cut.as_ref().map_or(end_lsn, |s| s.ends_at);
            let take = stop.saturating_sub(start_lsn) as usize;
            let skip = conn_offset.saturating_sub(start_lsn) as usize;
            let to_send = &bytes[skip.min(take)..take.min(bytes.len())];
            let frame_lsn = start_lsn + skip as u64;
            if !to_send.is_empty()
                && self.enqueue_copy_data_with(id, |out| {
                    encode_wal_data_frame_into(out, frame_lsn, server_wal_end, to_send);
                })
            {
                self.advance_dispatched(id, Pos::new(stop));
            }
            if let Some(switch) = cut {
                self.end_timeline_for(id, switch);
            }
        }
    }

    /// Mark a historic connection served through its switchpoint. The listener
    /// picks this up and ends the timeline on the wire.
    fn end_timeline_for(&mut self, id: u64, switch: TimelineSwitch) {
        self.connections
            .get_mut(&id)
            .expect("switchpoint reached by a registered connection")
            .phase
            .end(switch);
        tracing::info!(
            target: "walshadow::shadow_stream",
            conn_id = id,
            timeline = switch.timeline,
            next_timeline = switch.next_timeline,
            switch_lsn = format!("{:#X}", switch.ends_at),
            "historic timeline served to its switchpoint",
        );
        self.wake_listener();
    }

    /// `Some` once a connection has been served through its switchpoint: the
    /// stream owes the client the next-timeline result.
    fn timeline_ended_for(&self, id: u64) -> Option<TimelineSwitch> {
        self.connections.get(&id)?.phase.ended()
    }

    /// Replay `[start_lsn, server_wal_end]` so a reconnect behind the live head
    /// gets a contiguous stream instead of a gap. Older ranges → restore_command.
    /// A historic connection stops at its switchpoint: past it the retained
    /// bytes belong to the descendant.
    fn backfill_connection(&mut self, id: u64, start_lsn: u64) {
        let ends_at = self
            .connections
            .get(&id)
            .and_then(|c| c.phase.cut())
            .map(|s| s.ends_at);
        let head = self.server_wal_end.get();
        let server_wal_end = ends_at.map_or(head, |v| v.min(head));
        if start_lsn >= server_wal_end {
            return;
        }
        let off = (start_lsn - self.wire_buf_start) as usize;
        let end = ((server_wal_end - self.wire_buf_start) as usize).min(self.wire_buf.len());
        if off >= end {
            return;
        }
        let backfill = self.wire_buf[off..end].to_vec();
        const FRAME: usize = 256 * 1024;
        let mut pos = 0;
        while pos < backfill.len() {
            let end = (pos + FRAME).min(backfill.len());
            let frame_lsn = start_lsn + pos as u64;
            let chunk = &backfill[pos..end];
            // Uncapped: a reconnect backfill is bounded (≤ two segments) and
            // required for recovery, so it must not trip the slow-client cap.
            self.frame_copy_data(id, None, |out| {
                encode_wal_data_frame_into(out, frame_lsn, server_wal_end, chunk);
            });
            pos = end;
        }
        self.connections
            .get_mut(&id)
            .expect("backfill follows registration")
            .backfill_queued = self.send_queues.get(&id).map_or(0, Vec::len);
        self.advance_dispatched(id, Pos::new(server_wal_end));
    }

    pub fn drop_connection(&mut self, id: u64) {
        self.connections.remove(&id);
        self.send_queues.remove(&id);
        // Removing laggard can raise aggregate and release hold
        self.applied.bump();
    }

    /// Clear backfill count after listener drains queue.
    fn taken(&mut self, id: u64) {
        if let Some(c) = self.connections.get_mut(&id) {
            c.backfill_queued = 0;
        }
    }

    /// Record an inbound `'r'` standby status.
    pub fn observe_status(
        &mut self,
        id: u64,
        _write_lsn: u64,
        flush_lsn: Pos<ShadowFlush>,
        apply_lsn: Pos<ShadowReplay>,
    ) {
        if let Some(c) = self.connections.get_mut(&id) {
            c.flush_lsn = c.flush_lsn.max(flush_lsn);
            let advanced = apply_lsn > c.apply_lsn;
            c.apply_lsn = c.apply_lsn.max(apply_lsn);
            if advanced {
                self.applied.bump();
            }
        }
    }

    /// Listener pulls framed bytes out of here.
    pub fn drain_send_queue(&mut self, id: u64) -> Option<Vec<u8>> {
        self.taken(id);
        self.send_queues.remove(&id)
    }

    /// Swap queued bytes into `buf`, leaving its cleared capacity for the
    /// pump to fill instead of regrowing a fresh queue. Capacity past a few
    /// source chunks is backfill or burst residue, released here
    pub fn swap_send_queue(&mut self, id: u64, buf: &mut Vec<u8>) {
        const KEEP: usize = 1 << 20;
        buf.clear();
        buf.shrink_to(KEEP);
        if let Some(q) = self.send_queues.get_mut(&id) {
            std::mem::swap(q, buf);
            self.taken(id);
        }
    }

    #[cfg(test)]
    pub(crate) fn wire_buf_len(&self) -> usize {
        self.wire_buf.len()
    }

    /// Overflowing `slow_threshold` marks the connection `closing` and
    /// discards the bytes; listener tears down on its next pass.
    pub fn enqueue(&mut self, id: u64, framed: Vec<u8>) -> bool {
        let q = self.send_queues.entry(id).or_default();
        if q.len() + framed.len() > self.slow_threshold {
            self.overflow(id);
            return false;
        }
        q.extend_from_slice(&framed);
        true
    }

    fn overflow(&mut self, id: u64) {
        if let Some(c) = self.connections.get_mut(&id)
            && c.phase.close()
        {
            self.dropped_total += 1;
        }
        self.send_queues.remove(&id);
    }

    /// Append a CopyData envelope wrapping a `'w'`/`'k'` frame, built in-place.
    /// `build_body` writes everything after the 5-byte CopyData header. `cap`
    /// caps the queue (live traffic); `None` skips the cap for a bounded
    /// reconnect backfill. `false` on cap breach (marks closing, clears queue).
    fn frame_copy_data(
        &mut self,
        id: u64,
        cap: Option<usize>,
        build_body: impl FnOnce(&mut Vec<u8>),
    ) -> bool {
        let backfill = self.connections.get(&id).map_or(0, |c| c.backfill_queued);
        let q = self.send_queues.entry(id).or_default();
        let envelope_start = q.len();
        q.push(b'd');
        // u32 BE length placeholder, back-patched after body appended
        q.extend_from_slice(&[0u8; 4]);
        let body_start = q.len();
        build_body(q);
        let payload_len = 4 + (q.len() - body_start);
        if let Some(cap) = cap
            && envelope_start.saturating_sub(backfill) + 1 + payload_len > cap
        {
            self.overflow(id);
            return false;
        }
        q[envelope_start + 1..envelope_start + 5]
            .copy_from_slice(&(payload_len as u32).to_be_bytes());
        true
    }

    /// Cap-enforcing enqueue for live traffic.
    fn enqueue_copy_data_with(&mut self, id: u64, build_body: impl FnOnce(&mut Vec<u8>)) -> bool {
        self.frame_copy_data(id, Some(self.slow_threshold), build_body)
    }

    pub fn advance_dispatched(&mut self, id: u64, new_lsn: Pos<ShadowDispatched>) {
        if let Some(c) = self.connections.get_mut(&id) {
            c.dispatched_lsn = c.dispatched_lsn.max(new_lsn);
        }
    }

    /// Enqueue a reply-requested `'k'` keepalive on every active
    /// connection and wake the listener to write it now. Shadow's
    /// walreceiver answers immediately with fresh flush/apply LSNs —
    /// non-forced replies otherwise fire only when the flush position
    /// advances or `wal_receiver_status_interval` elapses, so a publication
    /// hold waiting on apply progress prods through this.
    ///
    /// The wake carries the queue's WAL bytes out with the keepalive: a
    /// hold waits on replay of records shadow cannot apply before it
    /// receives them, and the listener's tick is a batching timer sized for
    /// bulk streaming, not for this. Bulk traffic keeps that batching —
    /// only the prod jumps the queue
    pub fn request_status(&mut self) {
        let server_wal_end = self.server_wal_end.get();
        let ids: Vec<u64> = self
            .connections
            .iter()
            .filter(|(_, c)| !c.phase.is_closing())
            .map(|(id, _)| *id)
            .collect();
        for id in ids {
            let _ = self.enqueue_copy_data_with(id, |out| {
                encode_keepalive_frame_into(out, server_wal_end, true);
            });
        }
        self.wake_listener();
    }
}

pub struct ShadowStreamSink {
    state: Arc<Mutex<ShadowStreamState>>,
}

impl ShadowStreamSink {
    pub fn new(state: Arc<Mutex<ShadowStreamState>>) -> Self {
        Self { state }
    }
}

impl RecordBytesSink for ShadowStreamSink {
    fn on_wire_chunk<'a>(
        &'a mut self,
        start_lsn: u64,
        bytes: &'a [u8],
    ) -> Pin<Box<dyn Future<Output = Result<(), SinkError>> + Send + 'a>> {
        Box::pin(async move {
            self.state.lock().await.dispatch_wire(start_lsn, bytes);
            Ok(())
        })
    }

    fn on_segment_retired<'a>(
        &'a mut self,
        new_start_lsn: u64,
    ) -> Pin<Box<dyn Future<Output = Result<(), SinkError>> + Send + 'a>> {
        Box::pin(async move {
            self.state.lock().await.trim_wire_buf_before(new_start_lsn);
            Ok(())
        })
    }
}

#[derive(Debug, Clone)]
pub enum WalSenderAddr {
    Unix(PathBuf),
    Tcp(SocketAddr),
}

/// Accept walreceiver clients, run startup + IDENTIFY_SYSTEM +
/// START_REPLICATION handshake, pump queued bytes onto the socket
/// while decoding inbound `'r'` standby status.
pub async fn spawn_listener(
    addr: WalSenderAddr,
    state: Arc<Mutex<ShadowStreamState>>,
    flush_interval: Duration,
) -> Result<tokio::task::JoinHandle<()>, ShadowStreamError> {
    match addr {
        WalSenderAddr::Unix(path) => {
            let _ = tokio::fs::remove_file(&path).await;
            if let Some(parent) = path.parent() {
                tokio::fs::create_dir_all(parent).await?;
            }
            let listener = UnixListener::bind(&path)?;
            Ok(tokio::spawn(run_unix_listener(
                listener,
                state,
                flush_interval,
            )))
        }
        WalSenderAddr::Tcp(addr) => {
            // SO_REUSEADDR (tokio sets it on unix): a prior bind in TIME_WAIT
            // must not block restart with the same `--walsender-bind`
            tracing::info!(target: "walshadow::shadow_stream", %addr, "binding walsender");
            let listener = TcpListener::bind(addr)
                .await
                .map_err(|e| io::Error::new(e.kind(), format!("bind {addr}: {e}")))?;
            Ok(tokio::spawn(run_tcp_listener(
                listener,
                state,
                flush_interval,
            )))
        }
    }
}

async fn run_unix_listener(
    listener: UnixListener,
    state: Arc<Mutex<ShadowStreamState>>,
    flush_interval: Duration,
) {
    loop {
        match listener.accept().await {
            Ok((sock, _)) => {
                let state = state.clone();
                tokio::spawn(handle_connection(sock, state, flush_interval));
            }
            Err(e) => {
                tracing::warn!(target: "walshadow", error = %e, "walsender listener accept failed");
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    }
}

async fn run_tcp_listener(
    listener: TcpListener,
    state: Arc<Mutex<ShadowStreamState>>,
    flush_interval: Duration,
) {
    loop {
        match listener.accept().await {
            Ok((sock, _)) => {
                let _ = sock.set_nodelay(true);
                let state = state.clone();
                tokio::spawn(handle_connection(sock, state, flush_interval));
            }
            Err(e) => {
                tracing::warn!(target: "walshadow", error = %e, "walsender listener accept failed");
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    }
}

async fn handle_connection<S>(
    sock: S,
    state: Arc<Mutex<ShadowStreamState>>,
    flush_interval: Duration,
) where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send,
{
    if let Err(e) = drive_connection(sock, state, flush_interval).await {
        tracing::warn!(target: "walshadow", error = %e, "walsender connection ended");
    }
}

/// Generic over the socket transport so unix + TCP share the logic.
async fn drive_connection<S>(
    mut sock: S,
    state: Arc<Mutex<ShadowStreamState>>,
    flush_interval: Duration,
) -> Result<(), ShadowStreamError>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send,
{
    let identity = {
        let mut s = state.lock().await;
        s.note_accepted();
        s.identity()
    };

    let mut started = server::handshake_and_await_start(&mut sock, &identity).await?;
    let mut conn = WalSenderConn::new(sock);
    // One socket can carry several streams: a historic one ends at its
    // switchpoint, and the client immediately asks for the branch that took over
    loop {
        let registered = {
            let mut s = state.lock().await;
            s.register_connection(started.start_lsn, started.timeline, started.ends_at)
        };
        let id = match registered {
            Ok(id) => id,
            Err(refused) => {
                tracing::warn!(
                    target: "walshadow",
                    start_lsn = format!("{:#X}", started.start_lsn),
                    requested_timeline = started.timeline,
                    served_timeline = identity.timeline,
                    error = %refused,
                    "walsender refused START_REPLICATION",
                );
                // Inside CopyBoth, where PG's walsender reports a removed segment
                let mut err = bytes::BytesMut::new();
                server::encode_error_response(&mut err, refused.sqlstate(), &refused.to_string());
                conn.write_framed(&err).await?;
                return Ok(());
            }
        };
        tracing::info!(
            target: "walshadow",
            conn_id = id,
            start_lsn = format!("{:#X}", started.start_lsn),
            timeline = started.timeline,
            ends_at = started.ends_at.map(|s| format!("{:#X}", s.ends_at)),
            "walsender START_REPLICATION accepted",
        );
        let outcome = run_connection_loop(&mut conn, state.clone(), id, flush_interval).await;
        state.lock().await.drop_connection(id);
        match outcome? {
            StreamOutcome::Closed => return Ok(()),
            StreamOutcome::TimelineEnded(switch) => {
                conn.end_timeline(switch).await?;
                let identity = state.lock().await.identity();
                started = conn.await_start(&identity).await?;
            }
        }
    }
}

/// How one stream on a walsender connection finished.
enum StreamOutcome {
    /// Client gone, or the connection dropped for slowness
    Closed,
    /// Served through a historic branch's switchpoint; the client is owed the
    /// next-timeline result
    TimelineEnded(TimelineSwitch),
}

async fn run_connection_loop<S>(
    conn: &mut WalSenderConn<S>,
    state: Arc<Mutex<ShadowStreamState>>,
    id: u64,
    flush_interval: Duration,
) -> Result<StreamOutcome, ShadowStreamError>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send,
{
    // Shadow's `wal_receiver_timeout` (default 60s) tears down the
    // connection on silence. `'w'` frames cover the timer while WAL
    // flows; on idle inject a `'k'` after KEEPALIVE_IDLE (10s, PG's
    // wal_receiver_status_interval convention).
    const KEEPALIVE_IDLE: Duration = Duration::from_secs(10);
    let mut last_write = tokio::time::Instant::now();
    let mut ticker = tokio::time::interval(flush_interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    // Writes ride the enqueue wake; the ticker is the idle-keepalive timer
    // and the backstop for a wake the queue raced. Batching survives: a
    // wake drains everything queued, and bytes enqueued during a write
    // land in the next drain
    // Zero start: registration backfilled this connection before the
    // subscribe, and those bytes must not be swallowed
    let queued = state.lock().await.queued();
    let mut woke_at = Pos::ZERO;
    let mut pending = Vec::new();
    loop {
        tokio::select! {
            Ok(seen) = queued.advance(woke_at) => {
                woke_at = seen;
                state.lock().await.swap_send_queue(id, &mut pending);
                if !pending.is_empty() {
                    conn.write_framed(&pending).await?;
                    last_write = tokio::time::Instant::now();
                }
            }
            _ = ticker.tick() => {
                {
                    let mut s = state.lock().await;
                    if last_write.elapsed() >= KEEPALIVE_IDLE {
                        let server_wal_end = s.server_wal_end.get();
                        let _ = s.enqueue_copy_data_with(id, |out| {
                            encode_keepalive_frame_into(out, server_wal_end, false);
                        });
                    }
                    s.swap_send_queue(id, &mut pending);
                }
                if !pending.is_empty() {
                    // Queue holds fully-framed CopyData envelopes
                    conn.write_framed(&pending).await?;
                    last_write = tokio::time::Instant::now();
                }
            }
            frame = conn.try_recv_frame() => {
                match frame? {
                    Some(payload) => {
                        if let Some(status) = decode_standby_status(&payload) {
                            let mut s = state.lock().await;
                            s.observe_status(
                                id,
                                status.write_lsn,
                                Pos::new(status.flush_lsn),
                                Pos::new(status.apply_lsn),
                            );
                        }
                    }
                    None => return Ok(StreamOutcome::Closed),
                }
            }
        }
        let (ended, closing, tail) = {
            let mut s = state.lock().await;
            let ended = s.timeline_ended_for(id);
            // Take the queue under the same lock that reported the end: bytes
            // enqueued after a drain this loop already did would otherwise land
            // behind the result set, or never
            let tail = ended.and_then(|_| s.drain_send_queue(id));
            let closing = s.connections.get(&id).is_none_or(|c| c.phase.is_closing());
            (ended, closing, tail)
        };
        if let Some(switch) = ended {
            if let Some(bytes) = tail.filter(|b| !b.is_empty()) {
                conn.write_framed(&bytes).await?;
            }
            return Ok(StreamOutcome::TimelineEnded(switch));
        }
        if closing {
            return Ok(StreamOutcome::Closed);
        }
    }
}

/// This walsender on a unix socket, standing in for a source PG
#[cfg(test)]
pub(crate) mod fake_source {
    use super::*;
    use walrus::pg::replication::conn::PgConfig;
    use walrus::pg::replication::tls::SslMode;

    pub(crate) struct FakeSource {
        pub(crate) state: Arc<Mutex<ShadowStreamState>>,
        pub(crate) cfg: PgConfig,
        _dir: tempfile::TempDir,
    }

    pub(crate) async fn serve(state: ShadowStreamState) -> FakeSource {
        let dir = tempfile::tempdir().unwrap();
        let state = Arc::new(Mutex::new(state));
        let socket = WalSenderAddr::Unix(dir.path().join("sock/.s.PGSQL.5432"));
        spawn_listener(socket, state.clone(), Duration::from_millis(5))
            .await
            .unwrap();
        let cfg = PgConfig {
            host: dir.path().join("sock").display().to_string(),
            port: 5432,
            user: "test".into(),
            password: None,
            database: "test".into(),
            application_name: "fake-source".into(),
            sslmode: SslMode::Disable,
            tls: Default::default(),
        };
        FakeSource {
            state,
            cfg,
            _dir: dir,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fresh_state() -> ShadowStreamState {
        ShadowStreamState::new(1, "12345".into(), 0x1000, 1024 * 1024)
    }

    #[test]
    fn status_request_wakes_the_listener_with_the_queued_wal() {
        let mut s = fresh_state();
        let id = s
            .register_connection(0x1000, 1, None)
            .expect("current timeline");
        let queued = s.queued();
        let quiet = queued.current();
        s.enqueue(id, vec![b'd', 0, 0, 0, 4]);
        assert_eq!(
            queued.current(),
            quiet,
            "bulk WAL keeps the listener's batching tick",
        );
        s.request_status();
        assert!(queued.current() > quiet);
        let bytes = s.drain_send_queue(id).expect("queue");
        assert!(
            bytes.len() > 5,
            "the wake carries the queued WAL out with the keepalive",
        );
    }

    #[test]
    fn only_apply_progress_wakes_a_hold() {
        let mut s = fresh_state();
        let id = s
            .register_connection(0x1000, 1, None)
            .expect("current timeline");
        let applied = s.applied();
        let quiet = applied.current();
        s.observe_status(id, 0x2000, Pos::new(0x2000), Pos::new(0x1000));
        assert_eq!(
            applied.current(),
            quiet,
            "flush progress alone releases nothing",
        );
        s.observe_status(id, 0x2000, Pos::new(0x2000), Pos::new(0x1800));
        let woke = applied.current();
        assert!(woke > quiet);
        s.observe_status(id, 0x2000, Pos::new(0x2000), Pos::new(0x1800));
        assert_eq!(applied.current(), woke, "a repeated status is not progress");
    }

    #[test]
    fn aggregate_lsn_with_no_connections_is_default() {
        let s = fresh_state();
        let agg = s.aggregate();
        assert_eq!(agg.active_connections, 0);
        assert_eq!(agg.min_flush_lsn, None);
        assert_eq!(agg.min_apply_lsn, None);
    }

    const HISTORY: &[u8] = b"1\t0/1004\tno recovery target specified\n";

    /// The switch is what a client is told about; until it has been served
    /// through the switchpoint its stream stays open, and descendant bytes never
    /// go down it.
    #[tokio::test(flavor = "current_thread")]
    async fn advertising_a_fork_ends_the_ancestor_stream() {
        let state = Arc::new(Mutex::new(fresh_state()));
        let mut sink = ShadowStreamSink::new(state.clone());
        let id = state
            .lock()
            .await
            .register_connection(0x1000, 1, None)
            .expect("current timeline");
        state
            .lock()
            .await
            .advertise_timeline(2, 0x1004, HISTORY.to_vec());
        assert_eq!(state.lock().await.timeline, 2);

        // Bytes spanning the fork: the ancestor half is served, the descendant
        // half is not, and the stream is now owed a next-timeline result
        sink.on_wire_chunk(0x1000, b"AAAABBBB").await.unwrap();
        let mut s = state.lock().await;
        let q = s.drain_send_queue(id).expect("ancestor bytes served");
        assert_eq!(
            &q[WIRE_HDR..],
            b"AAAA",
            "served to the switchpoint and no further",
        );
        assert_eq!(
            s.timeline_ended_for(id),
            Some(TimelineSwitch {
                timeline: 1,
                ends_at: 0x1004,
                next_timeline: 2
            }),
        );
    }

    /// A reconnect behind the fork gets the ancestor tail out of the retained
    /// wire buffer, without the fork segment ever having sealed.
    #[tokio::test(flavor = "current_thread")]
    async fn historic_reconnect_is_served_from_the_fork_segment() {
        let state = Arc::new(Mutex::new(fresh_state()));
        let mut sink = ShadowStreamSink::new(state.clone());
        sink.on_wire_chunk(0x1000, b"AAAA").await.unwrap();
        let switch = TimelineSwitch {
            timeline: 1,
            ends_at: 0x1004,
            next_timeline: 2,
        };
        state
            .lock()
            .await
            .advertise_timeline(2, 0x1004, HISTORY.to_vec());
        sink.on_wire_chunk(0x1004, b"DDDD").await.unwrap();

        let mut s = state.lock().await;
        let id = s
            .register_connection(0x1000, 1, Some(switch))
            .expect("timeline 1 is known");
        let q = s.drain_send_queue(id).expect("ancestor bytes backfilled");
        assert_eq!(&q[WIRE_HDR..], b"AAAA", "descendant bytes withheld");
        assert_eq!(s.timeline_ended_for(id), Some(switch));
    }

    #[test]
    fn reconnect_at_the_switchpoint_ends_without_serving() {
        let mut s = fresh_state();
        let switch = TimelineSwitch {
            timeline: 1,
            ends_at: 0x1004,
            next_timeline: 2,
        };
        s.advertise_timeline(2, 0x1004, HISTORY.to_vec());
        let id = s
            .register_connection(0x1004, 1, Some(switch))
            .expect("timeline 1 is known");
        assert!(
            s.drain_send_queue(id).is_none(),
            "nothing left on that branch"
        );
        assert_eq!(s.timeline_ended_for(id), Some(switch));
    }

    #[test]
    fn refuses_a_timeline_this_walsender_never_served() {
        let mut s = fresh_state();
        assert_eq!(
            s.register_connection(0x1000, 7, None),
            Err(Refused::Timeline(7)),
            "a branch the handshake could not place cannot be answered",
        );
        assert!(
            s.register_connection(0x1004, 1, None).is_ok(),
            "the current branch connects",
        );
        assert!(
            s.register_connection(0x1004, 0, None).is_ok(),
            "an unspecified timeline takes the current branch",
        );
    }

    /// The handshake answers from this, so a crossing has to show up in it.
    #[test]
    fn identity_carries_the_chain_after_a_crossing() {
        let mut s = fresh_state();
        assert!(s.identity().switches.is_empty());
        s.advertise_timeline(2, 0x1004, HISTORY.to_vec());
        let identity = s.identity();
        assert_eq!(identity.timeline, 2);
        assert_eq!(
            identity.switches,
            [TimelineSwitch {
                timeline: 1,
                ends_at: 0x1004,
                next_timeline: 2
            }],
        );
        assert_eq!(identity.histories, [(2, HISTORY.to_vec())]);
    }

    #[test]
    fn identify_system_advertises_invalid_xlogpos() {
        assert_eq!(
            fresh_state().identity().xlogpos,
            0,
            "a real LSN parks PG's walreceiver in its catch-up wait",
        );
    }

    #[test]
    fn accepted_counts_sockets_short_of_start_replication() {
        let mut s = fresh_state();
        s.note_accepted();
        let agg = s.aggregate();
        assert_eq!(agg.accepted_total, 1);
        assert_eq!(agg.active_connections, 0, "handshake carries no LSN");
        assert_eq!(agg.min_apply_lsn, None, "holds must not see a fake apply");
        s.register_connection(0x1000, 1, None)
            .expect("current timeline");
        let agg = s.aggregate();
        assert_eq!(agg.accepted_total, 1, "monotonic, not decremented");
        assert_eq!(agg.active_connections, 1);
    }

    #[test]
    fn aggregate_lsn_returns_min_across_connections() {
        let mut s = fresh_state();
        let a = s
            .register_connection(0x1000, 1, None)
            .expect("current timeline");
        let b = s
            .register_connection(0x1000, 1, None)
            .expect("current timeline");
        s.observe_status(a, 0x2000, Pos::new(0x2000), Pos::new(0x1800));
        s.observe_status(b, 0x2200, Pos::new(0x2100), Pos::new(0x1900));
        let agg = s.aggregate();
        assert_eq!(agg.active_connections, 2);
        assert_eq!(agg.min_flush_lsn, Some(0x2000.into()));
        assert_eq!(agg.min_apply_lsn, Some(0x1800.into()));
    }

    /// A client held on a historic branch reads as that branch, not as the one
    /// the walsender advertises: that gap is a shadow mid-crossing.
    #[test]
    fn aggregate_reports_the_branch_attached_clients_read() {
        let mut s = fresh_state();
        assert_eq!(s.aggregate().replay_timeline, None, "nothing attached");
        s.advertise_timeline(2, 0x2000, HISTORY.to_vec());
        s.register_connection(
            0x1000,
            1,
            Some(TimelineSwitch {
                timeline: 1,
                ends_at: 0x2000,
                next_timeline: 2,
            }),
        )
        .expect("historic branch the chain places");
        assert_eq!(s.aggregate().replay_timeline, Some(1));
        s.register_connection(0x2000, 2, None)
            .expect("served branch");
        assert_eq!(
            s.aggregate().replay_timeline,
            Some(1),
            "the branch furthest behind is the one still crossing",
        );
    }

    #[test]
    fn enqueue_past_slow_threshold_marks_closing() {
        let mut s = ShadowStreamState::new(1, "x".into(), 0, 64);
        let id = s.register_connection(0, 1, None).expect("current timeline");
        assert!(s.enqueue(id, vec![0u8; 32]));
        assert!(s.enqueue(id, vec![0u8; 16]));
        assert!(!s.enqueue(id, vec![0u8; 64]));
        assert!(s.connections[&id].phase.is_closing());
        assert!(!s.send_queues.contains_key(&id));
        assert_eq!(s.aggregate().dropped_total, 1);
    }

    #[test]
    fn an_overflow_after_the_switchpoint_still_owes_the_timeline_end() {
        let mut s = ShadowStreamState::new(1, "x".into(), 0, 64);
        let switch = TimelineSwitch {
            timeline: 1,
            ends_at: 0,
            next_timeline: 2,
        };
        let id = s
            .register_connection(0, 1, Some(switch))
            .expect("timeline 1 is known");
        assert_eq!(s.timeline_ended_for(id), Some(switch));
        assert!(!s.enqueue(id, vec![0u8; 128]));
        assert!(s.connections[&id].phase.is_closing());
        assert_eq!(s.timeline_ended_for(id), Some(switch));
    }

    #[test]
    fn dropped_total_increments_once_per_connection() {
        let mut s = ShadowStreamState::new(1, "x".into(), 0, 64);
        let id = s.register_connection(0, 1, None).expect("current timeline");
        assert!(!s.enqueue(id, vec![0u8; 128]));
        // second overflow on the closing slot must not double-count
        assert!(!s.enqueue(id, vec![0u8; 128]));
        assert_eq!(s.aggregate().dropped_total, 1);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn sink_dispatches_one_wire_chunk_per_active_connection() {
        let state = Arc::new(Mutex::new(fresh_state()));
        let a = state
            .lock()
            .await
            .register_connection(0x1000, 1, None)
            .expect("current timeline");
        let b = state
            .lock()
            .await
            .register_connection(0x1000, 1, None)
            .expect("current timeline");
        let mut sink = ShadowStreamSink::new(state.clone());
        let bytes = b"abc";
        sink.on_wire_chunk(0x1000, bytes).await.unwrap();
        let mut s = state.lock().await;
        let qa = s.drain_send_queue(a).unwrap();
        let qb = s.drain_send_queue(b).unwrap();
        assert!(!qa.is_empty());
        // CopyData envelope over 'w' XLogData: 'd'(1) + len(4) + 'w'(1)
        // + start_lsn(8) + server_wal_end(8) + send_time(8) = 30 bytes
        assert_eq!(&qa[30..], bytes);
        assert_eq!(&qb[30..], bytes);
    }

    // 'w' frame prefix: 'd'(1) + len(4) + 'w'(1) + start_lsn(8) + wal_end(8) +
    // send_time(8) = 30 bytes; payload follows.
    const WIRE_HDR: usize = 30;

    /// `(lsn, payload length)` of each `'w'` frame in a send queue
    fn frames(mut q: &[u8]) -> Vec<(u64, usize)> {
        let mut out = Vec::new();
        while !q.is_empty() {
            let len = 1 + u32::from_be_bytes(q[1..5].try_into().unwrap()) as usize;
            assert_eq!(q[5], b'w');
            let lsn = u64::from_be_bytes(q[6..14].try_into().unwrap());
            out.push((lsn, len - WIRE_HDR));
            q = &q[len..];
        }
        out
    }

    /// Backfill-sized queue capacity does not ride the swap buffers for the
    /// connection's lifetime
    #[tokio::test(flavor = "current_thread")]
    async fn swap_releases_backfill_capacity() {
        let state = Arc::new(Mutex::new(ShadowStreamState::new(
            1,
            "12345".into(),
            0x1000,
            16 << 20,
        )));
        let mut sink = ShadowStreamSink::new(state.clone());
        let wal = vec![7u8; 4 << 20];
        sink.on_wire_chunk(0x1000, &wal).await.unwrap();
        let mut s = state.lock().await;
        let id = s
            .register_connection(0x1000, 1, None)
            .expect("current timeline");
        let mut pending = Vec::new();
        s.swap_send_queue(id, &mut pending);
        assert!(pending.len() > wal.len(), "backfill queued");
        s.swap_send_queue(id, &mut pending);
        assert!(pending.is_empty());
        s.swap_send_queue(id, &mut pending);
        assert!(pending.capacity() <= 1 << 20, "{}", pending.capacity());
        assert!(s.send_queues[&id].capacity() <= 1 << 20);
    }

    /// Every stream the walsender accepts is gap-free from its start, through
    /// backfill and live bytes alike; only a start below the retained bytes is
    /// refused, never answered from further on
    #[tokio::test(flavor = "current_thread")]
    async fn accepted_streams_are_contiguous_from_their_start() {
        for start in (0x1000..=0x100c).step_by(2) {
            let state = Arc::new(Mutex::new(fresh_state()));
            let mut sink = ShadowStreamSink::new(state.clone());
            sink.on_wire_chunk(0x1000, b"AAAABBBB").await.unwrap();
            sink.on_segment_retired(0x1004).await.unwrap();
            let registered = state.lock().await.register_connection(start, 1, None);
            sink.on_wire_chunk(0x1008, b"CCCC").await.unwrap();
            let Ok(id) = registered else {
                assert!(start < 0x1004, "{start:#X} refused: {registered:?}");
                continue;
            };
            assert!(
                start >= 0x1004,
                "{start:#X} accepted below the retained bytes"
            );
            let queued = state.lock().await.drain_send_queue(id).unwrap_or_default();
            let end = frames(&queued).into_iter().fold(start, |next, (lsn, len)| {
                assert_eq!(lsn, next, "gap in the stream from {start:#X}");
                next + len as u64
            });
            assert_eq!(end, start.max(0x100c), "stream from {start:#X} stops short");
        }
    }

    /// A rebuilt `WalStream` replays its segment over retained bytes, which
    /// keeps `wire_buf_start` and the backfill gap-free
    #[tokio::test(flavor = "current_thread")]
    async fn replay_over_retained_bytes_keeps_backfill() {
        let state = Arc::new(Mutex::new(fresh_state()));
        let mut sink = ShadowStreamSink::new(state.clone());
        sink.on_wire_chunk(0x1000, b"AAAABBBB").await.unwrap();
        sink.on_wire_chunk(0x1000, b"AAAABBBBCCCC").await.unwrap();
        let mut s = state.lock().await;
        let id = s.register_connection(0x1004, 1, None).unwrap();
        let q = s.drain_send_queue(id).expect("backfill");
        assert_eq!(&q[WIRE_HDR..], b"BBBBCCCC");
    }

    /// Allow reconnect backfill above threshold, but still limit new queued bytes.
    #[test]
    fn backfill_is_exempt_from_slow_threshold() {
        let mut s = ShadowStreamState::new(1, "x".into(), 0, WIRE_HDR + 8);
        s.dispatch_wire(0, b"AAAABBBBCCCCDDDD");
        let id = s.register_connection(0, 1, None).unwrap();
        s.dispatch_wire(16, b"EEEEEEEE");
        s.swap_send_queue(id, &mut Vec::new());
        s.dispatch_wire(24, b"FFFFFFFF");
        assert_eq!(s.aggregate().dropped_total, 0);
        s.dispatch_wire(32, b"GGGGGGGG");
        assert_eq!(s.aggregate().dropped_total, 1);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn on_segment_retired_trims_completed_keeps_straddling_bytes() {
        let state = Arc::new(Mutex::new(fresh_state())); // current_lsn = 0x1000
        let mut sink = ShadowStreamSink::new(state.clone());
        // Wire dispatched past the 0x1004 boundary into the next segment, as
        // when a record straddles it
        sink.on_wire_chunk(0x1000, b"AAAABBBB").await.unwrap(); // head = 0x1008
        sink.on_segment_retired(0x1004).await.unwrap();

        let mut s = state.lock().await;
        assert_eq!(
            s.wire_buf_len(),
            4,
            "completed segment dropped, in-progress [0x1004,0x1008) kept"
        );
        let cur = s
            .register_connection(0x1004, 1, None)
            .expect("current timeline");
        let q = s.drain_send_queue(cur).expect("in-progress backfill kept");
        assert_eq!(&q[WIRE_HDR..], b"BBBB", "no gap after trim");
    }

    #[test]
    fn overflow_on_the_chunk_reaching_the_switchpoint_still_owes_the_end() {
        let mut s = ShadowStreamState::new(1, "x".into(), 0, WIRE_HDR + 2);
        let switch = TimelineSwitch {
            timeline: 1,
            ends_at: 4,
            next_timeline: 2,
        };
        let id = s.register_connection(0, 1, Some(switch)).unwrap();
        s.dispatch_wire(0, b"AAAABBBB");
        assert!(s.connections[&id].phase.is_closing());
        assert_eq!(s.aggregate().dropped_total, 1);
        assert_eq!(s.timeline_ended_for(id), Some(switch));
    }

    #[test]
    fn advertising_at_the_head_ends_an_idle_stream_once() {
        let mut s = fresh_state();
        let id = s.register_connection(0x1000, 1, None).unwrap();
        s.advertise_timeline(2, 0x1000, HISTORY.to_vec());
        let first = TimelineSwitch {
            timeline: 1,
            ends_at: 0x1000,
            next_timeline: 2,
        };
        assert_eq!(s.timeline_ended_for(id), Some(first));
        s.advertise_timeline(3, 0x1000, HISTORY.to_vec());
        assert_eq!(
            s.timeline_ended_for(id),
            Some(first),
            "an ended stream keeps the branch it was reading",
        );
    }

    #[test]
    fn seeding_a_known_history_keeps_the_first_bytes() {
        let mut s = fresh_state();
        s.seed_history(1, b"first".to_vec());
        s.seed_history(1, b"second".to_vec());
        assert_eq!(s.identity().histories, [(1, b"first".to_vec())]);
    }

    #[test]
    fn an_empty_chunk_keeps_retained_bytes() {
        let mut s = fresh_state();
        s.dispatch_wire(0x1000, b"AAAA");
        s.dispatch_wire(0x1000, b"");
        let id = s.register_connection(0x1000, 1, None).unwrap();
        assert_eq!(&s.drain_send_queue(id).unwrap()[WIRE_HDR..], b"AAAA");
    }

    /// A rebuilt `WalStream` replaying less than the head retains less; a
    /// reconnect past the replay waits for it rather than reading a hole
    #[test]
    fn reconnect_past_a_short_replay_streams_once_replay_reaches_it() {
        let mut s = fresh_state();
        s.dispatch_wire(0x1000, b"AAAABBBB");
        s.dispatch_wire(0x1000, b"AA");
        let id = s.register_connection(0x1004, 1, None).unwrap();
        assert!(s.drain_send_queue(id).is_none());
        s.dispatch_wire(0x1002, b"AABBBBCC");
        assert_eq!(
            frames(&s.drain_send_queue(id).unwrap()),
            [(0x1004, 6)],
            "served from its own start",
        );
    }

    #[test]
    fn a_dropped_connection_ignores_late_traffic() {
        let mut s = fresh_state();
        let id = s.register_connection(0x1000, 1, None).unwrap();
        s.drop_connection(id);
        let applied = s.applied();
        let quiet = applied.current();
        s.observe_status(id, 0x2000, Pos::new(0x2000), Pos::new(0x2000));
        s.advance_dispatched(id, Pos::new(0x2000));
        assert_eq!(applied.current(), quiet);
        assert!(s.drain_send_queue(id).is_none());
        assert_eq!(s.aggregate().active_connections, 0);
    }

    /// Wait past `seen` registrations and drops, each of which bumps `applied`
    async fn applied_past(state: &Mutex<ShadowStreamState>, seen: Pos<AppliedWake>) {
        let applied = state.lock().await.applied();
        applied.advance(seen).await.unwrap();
    }

    #[tokio::test]
    async fn unix_walsender_streams_and_notices_a_client_leave() {
        use crate::source_feed::{SourceEvent, SourceFeed, StandbyStatus};
        let mut state = fresh_state();
        state.dispatch_wire(0x1000, b"AAAA");
        let source = fake_source::serve(state).await;
        let mut feed = SourceFeed::connect(&source.cfg).await.unwrap();
        let ident = feed.identify_system().await.unwrap();
        assert_eq!((ident.system_id, ident.timeline), (12345, 1));
        assert_eq!(feed.timeline_history(1).await.unwrap(), None);
        feed.start_physical_replication(None, 0x1000, 1)
            .await
            .unwrap();
        let mut buf = Vec::new();
        let event = feed.next_event(StandbyStatus::default(), &mut buf).await;
        assert!(
            matches!(event, Ok(SourceEvent::Wal(c)) if c.start_lsn == 0x1000 && c.data == b"AAAA")
        );
        let seen = source.state.lock().await.applied().current();
        drop(feed);
        applied_past(&source.state, seen).await;
        assert_eq!(source.state.lock().await.aggregate().active_connections, 0);
    }

    #[tokio::test]
    async fn unix_walsender_drops_a_slow_client() {
        use crate::source_feed::{SourceFeed, StandbyStatus};
        let source = fake_source::serve(ShadowStreamState::new(1, "1".into(), 0, 64)).await;
        let mut feed = SourceFeed::connect(&source.cfg).await.unwrap();
        feed.start_physical_replication(None, 0, 1).await.unwrap();
        applied_past(&source.state, Pos::ZERO).await;
        source.state.lock().await.dispatch_wire(0, &[0; 128]);
        applied_past(&source.state, Pos::new(1)).await;
        assert_eq!(source.state.lock().await.aggregate().dropped_total, 1);
        let mut buf = Vec::new();
        assert!(
            feed.next_event(StandbyStatus::default(), &mut buf)
                .await
                .is_err()
        );
    }

    /// Refusals arrive inside CopyBoth with the SQLSTATE a PG walsender uses,
    /// and only the archived one reads as a removed segment
    #[tokio::test]
    async fn unix_walsender_refusals_carry_pg_sqlstates() {
        use crate::source_feed::{SourceFeed, StandbyStatus, is_wal_segment_removed};
        let source = fake_source::serve(fresh_state()).await;
        for (start, tli, removed) in [(0x1000, 7, false), (0, 1, true)] {
            let mut feed = SourceFeed::connect(&source.cfg).await.unwrap();
            feed.start_physical_replication(None, start, tli)
                .await
                .unwrap();
            let mut buf = Vec::new();
            let err = feed
                .next_event(StandbyStatus::default(), &mut buf)
                .await
                .unwrap_err();
            assert_eq!(is_wal_segment_removed(&err), removed, "{err:#}");
        }
    }

    #[tokio::test]
    async fn tcp_walsender_names_an_address_it_cannot_bind() {
        let taken = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = taken.local_addr().unwrap();
        let state = Arc::new(Mutex::new(fresh_state()));
        let e = spawn_listener(WalSenderAddr::Tcp(addr), state, Duration::from_millis(5))
            .await
            .err();
        assert!(e.is_some_and(|e| e.to_string().contains(&format!("bind {addr}"))));
    }
}
