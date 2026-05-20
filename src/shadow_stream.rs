//! PHASE13 §3 — streaming-fed shadow.
//!
//! `ShadowStreamSink` composes alongside
//! [`DirSegmentSink`](crate::wal_stream::DirSegmentSink) and
//! [`BufferingDecoderSink`](crate::xact_buffer::BufferingDecoderSink)
//! on a [`WalStream`](crate::wal_stream::WalStream). On each filtered
//! record it appends the rewritten bytes onto every active shadow
//! connection's send buffer, framed as `'w'` `XLogData` per the
//! physical replication protocol. On each segment boundary it
//! advances the per-connection `server_wal_end` for keepalive
//! framing.
//!
//! Tracking state per connection:
//! - `dispatched_to_shadow_lsn` — high water of bytes pushed onto the
//!   wire (mirrors source's `write_lsn`)
//! - `shadow_flush_lsn`, `shadow_apply_lsn` — from `'r'` standby status
//!   frames the connection sends back. The min across active
//!   connections is the catalog-gate input.
//!
//! Backpressure: a socket whose send buffer fills past
//! `slow_connection_threshold` is dropped, letting shadow reconnect
//! via the archive (`restore_command`) path. The catalog gate then
//! polls shadow's apply LSN until streaming resumes.

use std::collections::HashMap;
use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use thiserror::Error;
use tokio::net::{TcpListener, TcpStream, UnixListener, UnixStream};
use tokio::sync::Mutex;
use wal_rs::pg::replication::server::{self, ServerError, WalSenderConn, decode_standby_status};
use wal_rs::pg::replication::stream::{encode_keepalive_frame, encode_wal_data_frame};

use crate::wal_stream::{RecordBytesSink, SinkError};

/// One client (typically shadow PG) currently consuming WAL bytes.
/// State the sink needs per connection.
#[derive(Debug)]
struct ConnState {
    /// High water of bytes the sink has pushed onto this connection's
    /// send buffer. Equivalent to source's `write_lsn`.
    dispatched_lsn: u64,
    /// Last `flush_lsn` reported by the client's `'r'` standby status.
    flush_lsn: u64,
    /// Last `apply_lsn` reported by the client's `'r'` standby status.
    apply_lsn: u64,
    /// Closing? Marked once a write error fires; the listener loop
    /// drops the slot on the next status sweep.
    closing: bool,
}

impl ConnState {
    fn fresh(start_lsn: u64) -> Self {
        Self {
            dispatched_lsn: start_lsn,
            flush_lsn: start_lsn,
            apply_lsn: start_lsn,
            closing: false,
        }
    }
}

/// Aggregate flush/apply LSN view across every shadow-streaming
/// connection. `None` if there are no active connections (the catalog
/// gate falls back to disk-driven polling).
#[derive(Debug, Default, Clone, Copy)]
pub struct AggregateLsn {
    pub min_flush_lsn: Option<u64>,
    pub min_apply_lsn: Option<u64>,
    pub active_connections: usize,
}

#[derive(Debug, Error)]
pub enum ShadowStreamError {
    #[error("io: {0}")]
    Io(#[from] io::Error),
    #[error("server: {0}")]
    Server(#[from] ServerError),
}

/// Shared state ShadowStreamSink + the listener task contend over.
/// `RwLock` would be tempting on the hot path, but write traffic
/// (sink dispatch + listener accept) is symmetric to read traffic
/// (status sweep), so a `Mutex` keeps the protocol simple.
#[derive(Debug)]
pub struct ShadowStreamState {
    /// LSN of the first byte every newly-accepted connection should
    /// receive. Mirrors source's view of "current WAL position" at
    /// the time the connection landed.
    pub current_lsn: u64,
    pub timeline: u32,
    pub system_identifier: String,
    /// `IDENTIFY_SYSTEM`'s `dbname` column (always empty for physical
    /// replication).
    pub dbname: Option<String>,
    /// `IDENTIFY_SYSTEM`'s `xlogpos` column — the source's
    /// `pg_current_wal_lsn()`. Walshadow advertises its
    /// `current_lsn` here so shadow's walreceiver knows where to
    /// resume.
    pub xlogpos: u64,
    /// Per-connection state (keyed by connection id).
    connections: HashMap<u64, ConnState>,
    /// Next connection id to hand out on `register_connection`.
    next_conn_id: u64,
    /// Pending record bytes per connection: bytes queued behind a
    /// slow shadow client. Kept bounded by `slow_threshold`; past it
    /// the connection is dropped.
    send_queues: HashMap<u64, Vec<u8>>,
    /// Slow-connection cutoff in bytes; past this the listener kills
    /// the socket.
    pub slow_threshold: usize,
    /// Most-recent server_wal_end value the sink has observed. Used
    /// to populate `'w'`/`'k'` frame headers.
    pub server_wal_end: u64,
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
            xlogpos: current_lsn,
            connections: HashMap::new(),
            next_conn_id: 1,
            send_queues: HashMap::new(),
            slow_threshold,
            server_wal_end: current_lsn,
        }
    }

    /// Aggregate LSN view across active connections.
    pub fn aggregate(&self) -> AggregateLsn {
        let active: Vec<&ConnState> = self.connections.values().filter(|c| !c.closing).collect();
        if active.is_empty() {
            return AggregateLsn::default();
        }
        AggregateLsn {
            min_flush_lsn: active.iter().map(|c| c.flush_lsn).min(),
            min_apply_lsn: active.iter().map(|c| c.apply_lsn).min(),
            active_connections: active.len(),
        }
    }

    /// Track a new connection. Returns its id.
    pub fn register_connection(&mut self, start_lsn: u64) -> u64 {
        let id = self.next_conn_id;
        self.next_conn_id += 1;
        self.connections.insert(id, ConnState::fresh(start_lsn));
        id
    }

    /// Drop a connection (closed gracefully or killed by slow-client
    /// cutoff). Removes both connection state and any queued bytes.
    pub fn drop_connection(&mut self, id: u64) {
        self.connections.remove(&id);
        self.send_queues.remove(&id);
    }

    /// Record an inbound `'r'` standby status from the wire.
    pub fn observe_status(&mut self, id: u64, write_lsn: u64, flush_lsn: u64, apply_lsn: u64) {
        let _ = write_lsn;
        if let Some(c) = self.connections.get_mut(&id) {
            c.flush_lsn = c.flush_lsn.max(flush_lsn);
            c.apply_lsn = c.apply_lsn.max(apply_lsn);
        }
    }

    /// Drain the pending send queue for connection `id` — listener
    /// task pulls bytes out of here, framed and ready to go on the
    /// socket. Internally also tracks `dispatched_lsn` advance for
    /// keepalive carriage.
    pub fn drain_send_queue(&mut self, id: u64) -> Option<Vec<u8>> {
        self.send_queues.remove(&id)
    }

    /// Append framed bytes to a connection's send queue. If the queue
    /// would overflow `slow_threshold`, the connection is marked
    /// `closing` and the framed bytes are discarded (the listener
    /// task tears down the socket on its next pass).
    pub fn enqueue(&mut self, id: u64, framed: Vec<u8>) -> bool {
        let q = self.send_queues.entry(id).or_default();
        if q.len() + framed.len() > self.slow_threshold {
            if let Some(c) = self.connections.get_mut(&id) {
                c.closing = true;
            }
            self.send_queues.remove(&id);
            return false;
        }
        q.extend_from_slice(&framed);
        true
    }

    /// Advance the per-connection `dispatched_lsn` after a frame
    /// covering `[prev_lsn, new_lsn)` is enqueued.
    pub fn advance_dispatched(&mut self, id: u64, new_lsn: u64) {
        if let Some(c) = self.connections.get_mut(&id) {
            c.dispatched_lsn = c.dispatched_lsn.max(new_lsn);
        }
    }
}

/// `RecordBytesSink` impl: frames each record + segment boundary onto
/// every active shadow connection's send queue.
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
            if bytes.is_empty() {
                return Ok(());
            }
            let mut state = self.state.lock().await;
            let end_lsn = start_lsn + bytes.len() as u64;
            state.server_wal_end = state.server_wal_end.max(end_lsn);
            let ids: Vec<u64> = state.connections.keys().copied().collect();
            for id in ids {
                let conn_offset = state
                    .connections
                    .get(&id)
                    .map(|c| c.dispatched_lsn)
                    .unwrap_or(start_lsn);
                if end_lsn <= conn_offset {
                    continue;
                }
                let skip = conn_offset.saturating_sub(start_lsn) as usize;
                let to_send = &bytes[skip.min(bytes.len())..];
                if to_send.is_empty() {
                    continue;
                }
                let frame_lsn = start_lsn + skip as u64;
                let frame = wrap_copy_data(&encode_wal_data_frame(
                    frame_lsn,
                    state.server_wal_end,
                    to_send,
                ));
                if state.enqueue(id, frame) {
                    state.advance_dispatched(id, end_lsn);
                }
            }
            Ok(())
        })
    }

    fn on_segment_boundary<'a>(
        &'a mut self,
        start_lsn: u64,
        trailing_bytes: &'a [u8],
    ) -> Pin<Box<dyn Future<Output = Result<(), SinkError>> + Send + 'a>> {
        Box::pin(async move {
            let mut state = self.state.lock().await;
            let segment_end_lsn = start_lsn + trailing_bytes.len() as u64;
            state.server_wal_end = state.server_wal_end.max(segment_end_lsn);
            let ids: Vec<u64> = state.connections.keys().copied().collect();
            for id in ids {
                let conn_offset = state
                    .connections
                    .get(&id)
                    .map(|c| c.dispatched_lsn)
                    .unwrap_or(start_lsn);
                if segment_end_lsn > conn_offset && !trailing_bytes.is_empty() {
                    let skip = conn_offset.saturating_sub(start_lsn) as usize;
                    let to_send = &trailing_bytes[skip.min(trailing_bytes.len())..];
                    if !to_send.is_empty() {
                        let frame_lsn = start_lsn + skip as u64;
                        let frame = wrap_copy_data(&encode_wal_data_frame(
                            frame_lsn,
                            state.server_wal_end,
                            to_send,
                        ));
                        if state.enqueue(id, frame) {
                            state.advance_dispatched(id, segment_end_lsn);
                        }
                    }
                }
                let frame = wrap_copy_data(&encode_keepalive_frame(state.server_wal_end, false));
                let _ = state.enqueue(id, frame);
            }
            Ok(())
        })
    }
}

/// Wrap a server-direction frame body (one `'w'` XLogData or `'k'`
/// keepalive) into a CopyData envelope so the queue holds exactly
/// what should appear on the wire. Listener task forwards the bytes
/// verbatim — no further framing.
fn wrap_copy_data(body: &[u8]) -> Vec<u8> {
    let payload_len = 4 + body.len();
    let mut out = Vec::with_capacity(1 + payload_len);
    out.push(b'd');
    out.extend_from_slice(&(payload_len as u32).to_be_bytes());
    out.extend_from_slice(body);
    out
}

/// Walsender listener address: prefers a unix socket path next to
/// shadow PG's socket dir; falls back to TCP.
#[derive(Debug, Clone)]
pub enum WalSenderAddr {
    Unix(PathBuf),
    Tcp(SocketAddr),
}

/// Spawn a listener that accepts walreceiver clients, runs the
/// startup + IDENTIFY_SYSTEM + START_REPLICATION handshake, then
/// pumps queued bytes from `ShadowStreamState::send_queues` onto the
/// socket while decoding inbound `'r'` standby status frames.
///
/// Returns a `JoinHandle` so callers can keep the listener task
/// alive across the daemon lifecycle (bootstrap → main pump →
/// shutdown).
pub async fn spawn_listener(
    addr: WalSenderAddr,
    state: Arc<Mutex<ShadowStreamState>>,
    flush_interval: Duration,
) -> Result<tokio::task::JoinHandle<()>, ShadowStreamError> {
    match addr {
        WalSenderAddr::Unix(path) => {
            // Best-effort cleanup of a stale socket file.
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
            // SO_REUSEADDR so a recent prior bind in TIME_WAIT
            // doesn't block startup. Important for test cycles + for
            // the "daemon restart with same `--walsender-bind`" case.
            let sock = match addr {
                std::net::SocketAddr::V4(_) => tokio::net::TcpSocket::new_v4().map_err(|e| {
                    io::Error::new(e.kind(), format!("TcpSocket::new_v4 {addr}: {e}"))
                })?,
                std::net::SocketAddr::V6(_) => tokio::net::TcpSocket::new_v6().map_err(|e| {
                    io::Error::new(e.kind(), format!("TcpSocket::new_v6 {addr}: {e}"))
                })?,
            };
            sock.set_reuseaddr(true)
                .map_err(|e| io::Error::new(e.kind(), format!("set_reuseaddr {addr}: {e}")))?;
            tracing::info!(target: "walshadow::shadow_stream", %addr, "binding walsender");
            sock.bind(addr)
                .map_err(|e| io::Error::new(e.kind(), format!("bind {addr}: {e}")))?;
            let listener = sock
                .listen(1024)
                .map_err(|e| io::Error::new(e.kind(), format!("listen {addr}: {e}")))?;
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
                tokio::spawn(handle_unix_connection(sock, state, flush_interval));
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
                tokio::spawn(handle_tcp_connection(sock, state, flush_interval));
            }
            Err(e) => {
                tracing::warn!(target: "walshadow", error = %e, "walsender listener accept failed");
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    }
}

async fn handle_unix_connection(
    sock: UnixStream,
    state: Arc<Mutex<ShadowStreamState>>,
    flush_interval: Duration,
) {
    if let Err(e) = drive_connection(sock, state, flush_interval).await {
        tracing::warn!(target: "walshadow", error = %e, "walsender connection ended");
    }
}

async fn handle_tcp_connection(
    sock: TcpStream,
    state: Arc<Mutex<ShadowStreamState>>,
    flush_interval: Duration,
) {
    if let Err(e) = drive_connection(sock, state, flush_interval).await {
        tracing::warn!(target: "walshadow", error = %e, "walsender connection ended");
    }
}

/// Walsender per-connection driver. Generic over the socket transport
/// so unix + TCP share the same protocol logic.
async fn drive_connection<S>(
    mut sock: S,
    state: Arc<Mutex<ShadowStreamState>>,
    flush_interval: Duration,
) -> Result<(), ShadowStreamError>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send,
{
    let (system_id, timeline, xlogpos) = {
        let s = state.lock().await;
        (s.system_identifier.clone(), s.timeline, s.xlogpos)
    };
    let identity = server::Identity {
        system_id,
        timeline,
        xlogpos,
        dbname: None,
    };

    let started = server::handshake_and_await_start(&mut sock, &identity).await?;
    let id = {
        let mut s = state.lock().await;
        s.register_connection(started.start_lsn)
    };
    tracing::info!(
        target: "walshadow",
        conn_id = id,
        start_lsn = format!("{:#X}", started.start_lsn),
        timeline = started.timeline,
        "walsender START_REPLICATION accepted",
    );

    let conn = WalSenderConn::new(sock);
    let result = run_connection_loop(conn, state.clone(), id, flush_interval).await;
    state.lock().await.drop_connection(id);
    result
}

async fn run_connection_loop<S>(
    mut conn: WalSenderConn<S>,
    state: Arc<Mutex<ShadowStreamState>>,
    id: u64,
    flush_interval: Duration,
) -> Result<(), ShadowStreamError>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send,
{
    let mut ticker = tokio::time::interval(flush_interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            _ = ticker.tick() => {
                let pending = {
                    let mut s = state.lock().await;
                    s.drain_send_queue(id)
                };
                if let Some(bytes) = pending
                    && !bytes.is_empty()
                {
                    // Queue holds fully-framed CopyData envelopes; ship verbatim.
                    conn.write_framed(&bytes).await?;
                }
            }
            frame = conn.try_recv_frame() => {
                match frame? {
                    Some(payload) => {
                        if let Some(status) = decode_standby_status(&payload) {
                            let mut s = state.lock().await;
                            s.observe_status(id, status.write_lsn, status.flush_lsn, status.apply_lsn);
                        }
                    }
                    None => break,
                }
            }
        }
        let closing = state
            .lock()
            .await
            .connections
            .get(&id)
            .map(|c| c.closing)
            .unwrap_or(true);
        if closing {
            break;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fresh_state() -> ShadowStreamState {
        ShadowStreamState::new(1, "12345".into(), 0x1000, 1024 * 1024)
    }

    #[test]
    fn aggregate_lsn_with_no_connections_is_default() {
        let s = fresh_state();
        let agg = s.aggregate();
        assert_eq!(agg.active_connections, 0);
        assert_eq!(agg.min_flush_lsn, None);
        assert_eq!(agg.min_apply_lsn, None);
    }

    #[test]
    fn aggregate_lsn_returns_min_across_connections() {
        let mut s = fresh_state();
        let a = s.register_connection(0x1000);
        let b = s.register_connection(0x1000);
        s.observe_status(a, 0x2000, 0x2000, 0x1800);
        s.observe_status(b, 0x2200, 0x2100, 0x1900);
        let agg = s.aggregate();
        assert_eq!(agg.active_connections, 2);
        assert_eq!(agg.min_flush_lsn, Some(0x2000));
        assert_eq!(agg.min_apply_lsn, Some(0x1800));
    }

    #[test]
    fn enqueue_past_slow_threshold_marks_closing() {
        let mut s = ShadowStreamState::new(1, "x".into(), 0, 64);
        let id = s.register_connection(0);
        assert!(s.enqueue(id, vec![0u8; 32]));
        assert!(s.enqueue(id, vec![0u8; 16]));
        // Past threshold:
        assert!(!s.enqueue(id, vec![0u8; 64]));
        assert!(s.connections.get(&id).unwrap().closing);
        assert!(!s.send_queues.contains_key(&id));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn sink_dispatches_one_wire_chunk_per_active_connection() {
        let state = Arc::new(Mutex::new(fresh_state()));
        let a = state.lock().await.register_connection(0x1000);
        let b = state.lock().await.register_connection(0x1000);
        let mut sink = ShadowStreamSink::new(state.clone());
        let bytes = b"abc";
        sink.on_wire_chunk(0x1000, bytes).await.unwrap();
        let mut s = state.lock().await;
        let qa = s.drain_send_queue(a).unwrap();
        let qb = s.drain_send_queue(b).unwrap();
        assert!(!qa.is_empty());
        // Queue holds CopyData envelopes wrapping 'w' XLogData.
        // 'd' (1) + length (4) + 'w' (1) + start_lsn (8) +
        // server_wal_end (8) + send_time (8) = 30 bytes before payload.
        assert_eq!(&qa[30..], bytes);
        assert_eq!(&qb[30..], bytes);
    }
}
