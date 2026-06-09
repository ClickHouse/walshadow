//! Streaming-fed shadow.
//!
//! `ShadowStreamSink` frames each filtered record as `'w'` `XLogData`
//! (physical replication protocol) onto every active shadow
//! connection's send buffer. Inbound `'r'` standby-status frames carry
//! shadow's flush/apply LSNs back; the min across connections gates the
//! catalog.
//!
//! Backpressure: a send buffer past `slow_connection_threshold` is
//! dropped, letting shadow reconnect via the archive (`restore_command`)
//! path; the catalog gate then polls shadow's apply LSN until streaming
//! resumes.

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
use wal_rs::pg::replication::stream::{encode_keepalive_frame_into, encode_wal_data_frame_into};

use crate::wal_stream::{RecordBytesSink, SinkError};

/// Per-connection state for one WAL-consuming client (typically
/// shadow PG).
#[derive(Debug)]
struct ConnState {
    /// High water of bytes pushed onto the send buffer; source's
    /// `write_lsn` equivalent.
    dispatched_lsn: u64,
    /// Last `flush_lsn` from the client's `'r'` standby status.
    flush_lsn: u64,
    /// Last `apply_lsn` from the client's `'r'` standby status.
    apply_lsn: u64,
    /// Marked on a write error/overflow; listener drops the slot on
    /// the next status sweep.
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

/// Aggregate flush/apply LSN across shadow-streaming connections.
/// `None` with no active connections (catalog gate falls back to
/// disk-driven polling).
#[derive(Debug, Default, Clone, Copy)]
pub struct AggregateLsn {
    pub min_flush_lsn: Option<u64>,
    pub min_apply_lsn: Option<u64>,
    pub active_connections: usize,
    /// Monotonic count of connections dropped by `slow_threshold`
    /// overflow since process start.
    pub dropped_total: u64,
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
    /// `IDENTIFY_SYSTEM` `xlogpos`, source's `pg_current_wal_lsn()`.
    /// Advertise `current_lsn` here so shadow's walreceiver knows where
    /// to resume.
    pub xlogpos: u64,
    connections: HashMap<u64, ConnState>,
    next_conn_id: u64,
    /// Bytes queued behind a slow shadow client, bounded by
    /// `slow_threshold`.
    send_queues: HashMap<u64, Vec<u8>>,
    /// Slow-connection byte cutoff; past it the listener kills the socket.
    pub slow_threshold: usize,
    /// Populates `'w'`/`'k'` frame headers.
    pub server_wal_end: u64,
    /// Surfaced via [`AggregateLsn::dropped_total`] for the
    /// `walshadow_shadow_stream_dropped_connections_total` gauge.
    dropped_total: u64,
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
            dropped_total: 0,
        }
    }

    pub fn aggregate(&self) -> AggregateLsn {
        let active: Vec<&ConnState> = self.connections.values().filter(|c| !c.closing).collect();
        if active.is_empty() {
            return AggregateLsn {
                dropped_total: self.dropped_total,
                ..AggregateLsn::default()
            };
        }
        AggregateLsn {
            min_flush_lsn: active.iter().map(|c| c.flush_lsn).min(),
            min_apply_lsn: active.iter().map(|c| c.apply_lsn).min(),
            active_connections: active.len(),
            dropped_total: self.dropped_total,
        }
    }

    pub fn register_connection(&mut self, start_lsn: u64) -> u64 {
        let id = self.next_conn_id;
        self.next_conn_id += 1;
        self.connections.insert(id, ConnState::fresh(start_lsn));
        id
    }

    pub fn drop_connection(&mut self, id: u64) {
        self.connections.remove(&id);
        self.send_queues.remove(&id);
    }

    /// Record an inbound `'r'` standby status.
    pub fn observe_status(&mut self, id: u64, write_lsn: u64, flush_lsn: u64, apply_lsn: u64) {
        let _ = write_lsn;
        if let Some(c) = self.connections.get_mut(&id) {
            c.flush_lsn = c.flush_lsn.max(flush_lsn);
            c.apply_lsn = c.apply_lsn.max(apply_lsn);
        }
    }

    /// Listener pulls framed bytes out of here.
    pub fn drain_send_queue(&mut self, id: u64) -> Option<Vec<u8>> {
        self.send_queues.remove(&id)
    }

    /// Overflowing `slow_threshold` marks the connection `closing` and
    /// discards the bytes; listener tears down on its next pass.
    pub fn enqueue(&mut self, id: u64, framed: Vec<u8>) -> bool {
        let q = self.send_queues.entry(id).or_default();
        if q.len() + framed.len() > self.slow_threshold {
            if let Some(c) = self.connections.get_mut(&id)
                && !c.closing
            {
                c.closing = true;
                self.dropped_total += 1;
            }
            self.send_queues.remove(&id);
            return false;
        }
        q.extend_from_slice(&framed);
        true
    }

    /// Append a CopyData envelope wrapping a `'w'`/`'k'` frame, built
    /// in-place to skip the intermediate `Vec`s per record × connection.
    /// `build_body` writes everything after the 5-byte CopyData header.
    /// `false` on `slow_threshold` breach (marks closing, clears queue,
    /// matching [`enqueue`]).
    fn enqueue_copy_data_with(&mut self, id: u64, build_body: impl FnOnce(&mut Vec<u8>)) -> bool {
        let slow_threshold = self.slow_threshold;
        let q = self.send_queues.entry(id).or_default();
        let envelope_start = q.len();
        q.push(b'd');
        // u32 BE length placeholder, back-patched after body appended
        q.extend_from_slice(&[0u8; 4]);
        let body_start = q.len();
        build_body(q);
        let payload_len = 4 + (q.len() - body_start);
        if envelope_start + 1 + payload_len > slow_threshold {
            q.truncate(envelope_start);
            if let Some(c) = self.connections.get_mut(&id)
                && !c.closing
            {
                c.closing = true;
                self.dropped_total += 1;
            }
            self.send_queues.remove(&id);
            return false;
        }
        q[envelope_start + 1..envelope_start + 5]
            .copy_from_slice(&(payload_len as u32).to_be_bytes());
        true
    }

    pub fn advance_dispatched(&mut self, id: u64, new_lsn: u64) {
        if let Some(c) = self.connections.get_mut(&id) {
            c.dispatched_lsn = c.dispatched_lsn.max(new_lsn);
        }
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
            if bytes.is_empty() {
                return Ok(());
            }
            let mut state = self.state.lock().await;
            let end_lsn = start_lsn + bytes.len() as u64;
            state.server_wal_end = state.server_wal_end.max(end_lsn);
            let server_wal_end = state.server_wal_end;
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
                let enqueued = state.enqueue_copy_data_with(id, |out| {
                    encode_wal_data_frame_into(out, frame_lsn, server_wal_end, to_send);
                });
                if enqueued {
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
            let server_wal_end = state.server_wal_end;
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
                        let enqueued = state.enqueue_copy_data_with(id, |out| {
                            encode_wal_data_frame_into(out, frame_lsn, server_wal_end, to_send);
                        });
                        if enqueued {
                            state.advance_dispatched(id, segment_end_lsn);
                        }
                    }
                }
            }
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
            // SO_REUSEADDR: a prior bind in TIME_WAIT must not block
            // restart with the same `--walsender-bind`.
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

/// Generic over the socket transport so unix + TCP share the logic.
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
    // Shadow's `wal_receiver_timeout` (default 60s) tears down the
    // connection on silence. `'w'` frames cover the timer while WAL
    // flows; on idle inject a `'k'` after KEEPALIVE_IDLE (10s, PG's
    // wal_receiver_status_interval convention).
    const KEEPALIVE_IDLE: Duration = Duration::from_secs(10);
    let mut last_write = tokio::time::Instant::now();
    let mut ticker = tokio::time::interval(flush_interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            _ = ticker.tick() => {
                let pending = {
                    let mut s = state.lock().await;
                    if last_write.elapsed() >= KEEPALIVE_IDLE {
                        let server_wal_end = s.server_wal_end;
                        let _ = s.enqueue_copy_data_with(id, |out| {
                            encode_keepalive_frame_into(out, server_wal_end, false);
                        });
                    }
                    s.drain_send_queue(id)
                };
                if let Some(bytes) = pending
                    && !bytes.is_empty()
                {
                    // Queue holds fully-framed CopyData envelopes
                    conn.write_framed(&bytes).await?;
                    last_write = tokio::time::Instant::now();
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
        assert!(!s.enqueue(id, vec![0u8; 64]));
        assert!(s.connections.get(&id).unwrap().closing);
        assert!(!s.send_queues.contains_key(&id));
        assert_eq!(s.aggregate().dropped_total, 1);
    }

    #[test]
    fn dropped_total_increments_once_per_connection() {
        let mut s = ShadowStreamState::new(1, "x".into(), 0, 64);
        let id = s.register_connection(0);
        assert!(!s.enqueue(id, vec![0u8; 128]));
        // second overflow on the closing slot must not double-count
        assert!(!s.enqueue(id, vec![0u8; 128]));
        assert_eq!(s.aggregate().dropped_total, 1);
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
        // CopyData envelope over 'w' XLogData: 'd'(1) + len(4) + 'w'(1)
        // + start_lsn(8) + server_wal_end(8) + send_time(8) = 30 bytes
        assert_eq!(&qa[30..], bytes);
        assert_eq!(&qb[30..], bytes);
    }
}
