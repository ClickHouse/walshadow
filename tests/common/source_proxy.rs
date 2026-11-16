//! Unix-socket relay in front of a PostgreSQL socket that breaks replication
//! connections at one protocol step on cue. Ordinary connections pass through
//! untouched, so the daemon's sidecar SQL keeps answering while its walsender
//! connection misbehaves

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::unix::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{UnixListener, UnixStream};

/// Armed faults. Counters fire once per replication connection reaching that
/// step, each needle in `drop_query` once
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Faults {
    /// Close right after the startup packet, so the dial fails
    pub refuse: u32,
    /// Close in place of relaying the backend's `CopyDone`
    pub drop_server_copy_done: u32,
    /// Close in place of relaying the client's `CopyDone`
    pub drop_client_copy_done: u32,
    /// Close in place of relaying a query containing this text
    pub drop_query: Vec<&'static str>,
    /// Swallow the walsender's shutdown `CommandComplete` and close, leaving
    /// the client connected to a source that is gone
    pub hold_shutdown: u32,
}

pub struct SourceProxy {
    /// Socket directory to name as `[source] host`
    pub dir: PathBuf,
    faults: Arc<Mutex<Faults>>,
    upstream: Arc<Mutex<PathBuf>>,
    accept: tokio::task::JoinHandle<()>,
}

impl Drop for SourceProxy {
    fn drop(&mut self) {
        self.accept.abort();
    }
}

impl SourceProxy {
    /// Listen as `<dir>/.s.PGSQL.<port>`, relaying to the same name under
    /// `upstream_dir`
    pub fn spawn(dir: &Path, port: u16, upstream_dir: &Path) -> Self {
        let socket = format!(".s.PGSQL.{port}");
        std::fs::create_dir_all(dir).expect("create proxy socket dir");
        let listener = UnixListener::bind(dir.join(&socket)).expect("bind proxy socket");
        let upstream = Arc::new(Mutex::new(upstream_dir.join(&socket)));
        let faults = Arc::new(Mutex::new(Faults::default()));
        let (shared, route) = (faults.clone(), upstream.clone());
        let accept = tokio::spawn(async move {
            while let Ok((client, _)) = listener.accept().await {
                let to = route.lock().unwrap().clone();
                tokio::spawn(relay(client, to, shared.clone()));
            }
        });
        Self {
            dir: dir.to_path_buf(),
            faults,
            upstream,
            accept,
        }
    }

    /// Relay later connections to `<dir>/.s.PGSQL.<port>`, the way a VIP
    /// moves under its clients
    pub fn reroute(&self, dir: &Path, port: u16) {
        *self.upstream.lock().unwrap() = dir.join(format!(".s.PGSQL.{port}"));
    }

    pub fn arm(&self, arm: impl FnOnce(&mut Faults)) {
        arm(&mut self.faults.lock().unwrap());
    }

    /// Faults armed but not fired yet
    pub fn pending(&self) -> Faults {
        self.faults.lock().unwrap().clone()
    }
}

fn fire(faults: &Mutex<Faults>, counter: impl FnOnce(&mut Faults) -> &mut u32) -> bool {
    let mut f = faults.lock().unwrap();
    let n = counter(&mut f);
    let armed = *n > 0;
    *n = n.saturating_sub(1);
    armed
}

fn fire_query(faults: &Mutex<Faults>, query: &[u8]) -> bool {
    let query = String::from_utf8_lossy(query);
    let mut f = faults.lock().unwrap();
    let hit = f.drop_query.iter().position(|n| query.contains(n));
    hit.map(|i| f.drop_query.remove(i)).is_some()
}

const SSL_REQUEST: u32 = 80_877_103;
const GSSENC_REQUEST: u32 = 80_877_104;

/// Startup packet, declining SSL and GSS encryption requests ahead of it
async fn read_startup(client: &mut UnixStream) -> std::io::Result<Vec<u8>> {
    loop {
        let len = client.read_u32().await?;
        let mut packet = len.to_be_bytes().to_vec();
        packet.resize(len as usize, 0);
        client.read_exact(&mut packet[4..]).await?;
        let code = u32::from_be_bytes(packet[4..8].try_into().unwrap());
        if code != SSL_REQUEST && code != GSSENC_REQUEST {
            return Ok(packet);
        }
        client.write_all(b"N").await?;
    }
}

fn is_replication(startup: &[u8]) -> bool {
    let mut params = startup.get(8..).unwrap_or_default().split(|&b| b == 0);
    while let (Some(key), Some(value)) = (params.next(), params.next()) {
        if key == b"replication" {
            return !matches!(value, b"false" | b"off" | b"no" | b"0");
        }
    }
    false
}

async fn relay(mut client: UnixStream, upstream: PathBuf, faults: Arc<Mutex<Faults>>) {
    let Ok(startup) = read_startup(&mut client).await else {
        return;
    };
    let replication = is_replication(&startup);
    if replication && fire(&faults, |f| &mut f.refuse) {
        return;
    }
    let Ok(mut server) = UnixStream::connect(&upstream).await else {
        return;
    };
    if server.write_all(&startup).await.is_err() {
        return;
    }
    if !replication {
        let _ = tokio::io::copy_bidirectional(&mut client, &mut server).await;
        return;
    }
    let (from_client, to_client) = client.into_split();
    let (from_server, to_server) = server.into_split();
    let held = AtomicBool::new(false);
    tokio::select! {
        _ = pump(from_client, to_server, Side::Client, &faults, &held) => {}
        _ = pump(from_server, to_client, Side::Server, &faults, &held) => {}
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Side {
    Client,
    Server,
}

/// Relay typed messages from `side` until either end closes or a fault fires
async fn pump(
    mut from: OwnedReadHalf,
    mut to: OwnedWriteHalf,
    side: Side,
    faults: &Mutex<Faults>,
    held: &AtomicBool,
) -> std::io::Result<()> {
    let mut in_copy = false;
    loop {
        let mut head = [0u8; 5];
        let read = from.read_exact(&mut head).await;
        // A walsender may also end the stream by just exiting
        if read.is_err() && in_copy && fire(faults, |f| &mut f.hold_shutdown) {
            held.store(true, Ordering::Relaxed);
            return std::future::pending().await;
        }
        read?;
        let len = u32::from_be_bytes(head[1..].try_into().unwrap()) as usize;
        let mut msg = head.to_vec();
        msg.resize(1 + len, 0);
        from.read_exact(&mut msg[5..]).await?;
        let drop = match (side, head[0]) {
            (Side::Server, b'W') => {
                in_copy = true;
                false
            }
            (Side::Server, b'c') => {
                in_copy = false;
                fire(faults, |f| &mut f.drop_server_copy_done)
            }
            (Side::Server, b'C') if in_copy && fire(faults, |f| &mut f.hold_shutdown) => {
                held.store(true, Ordering::Relaxed);
                return std::future::pending().await;
            }
            (Side::Client, b'c') => fire(faults, |f| &mut f.drop_client_copy_done),
            (Side::Client, b'Q') => fire_query(faults, &msg[5..]),
            _ => false,
        };
        if drop {
            return Ok(());
        }
        // A held connection's client keeps talking to a source that is gone
        if to.write_all(&msg).await.is_err() && !held.load(Ordering::Relaxed) {
            return Ok(());
        }
    }
}
