//! Port handout for tests that spawn real servers.
//!
//! `cargo nextest` runs one process per test with many tests in flight, so
//! ports must be disjoint across processes, not just inside a binary. Two
//! rules follow:
//!
//! - Stay below Linux's ephemeral floor (`net.ipv4.ip_local_port_range` starts
//!   at 32768), so an outbound connect cannot be holding a port a server is
//!   about to bind.
//! - Reserve through a lock file held for the life of the process. A bind probe
//!   alone leaves a window: `clickhouse server` takes seconds to bind, and a
//!   sibling test probing that port meanwhile would find it free. Lock files
//!   sit under `TMPDIR`, so concurrent test processes must share one.
//!
//! Postgres clusters here are socket-only (`listen_addresses = ''`) and PG keys
//! its SysV segment off the data dir inode (PG `src/backend/port/sysv_shmem.c`),
//! so a cluster's `port` only names a socket file inside a per-test temp dir.
//! One fixed number per role is therefore safe, no reservation needed.

#![allow(dead_code)]

use std::collections::HashSet;
use std::fs::{File, OpenOptions};
use std::net::TcpListener;
use std::path::PathBuf;
use std::sync::Mutex;

/// Socket-only cluster ports. Distinct per role so a test that puts source and
/// shadow in one socket dir still gets distinct socket files.
pub const PG_SOURCE_PORT: u16 = 5432;
pub const PG_SHADOW_PORT: u16 = 5433;

const FLOOR: u16 = 17000;
const CEIL: u16 = 32000;

/// Locks are never released mid-process: a freed port could be re-picked while
/// the server that owns it is still starting up.
static HELD: Mutex<Vec<File>> = Mutex::new(Vec::new());
/// Same rule inside one process, and it also covers the degraded no-lock-file
/// path where flock cannot speak for us.
static TAKEN: Mutex<Option<HashSet<u16>>> = Mutex::new(None);

fn take(port: u16) -> bool {
    TAKEN
        .lock()
        .unwrap()
        .get_or_insert_with(HashSet::new)
        .insert(port)
}

fn lock_dir() -> PathBuf {
    std::env::temp_dir().join("walshadow-test-ports")
}

/// Lock file + bind probe. `None` when another live test process holds the
/// port, or anything else on the host has it bound. Inner `None` means the
/// lock dir is unusable (another user owns it), leaving the bind probe as the
/// only guard.
fn claim(port: u16) -> Option<Option<File>> {
    if !take(port) {
        return None;
    }
    let lock = OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(lock_dir().join(port.to_string()))
        .ok();
    if let Some(f) = &lock {
        f.try_lock().ok()?;
    }
    TcpListener::bind(("127.0.0.1", port)).ok()?;
    Some(lock)
}

/// Spread concurrent processes over the range so they don't all rescan the
/// same prefix.
fn scan_start() -> u16 {
    let span = CEIL - FLOOR;
    FLOOR + ((std::process::id() as u16).wrapping_mul(64) % span)
}

/// First port of `len` consecutive free ports. ClickHouse derives its
/// interserver port as `http_port + 1`, so its pair must be adjacent.
pub fn reserve_span(len: u16) -> u16 {
    assert!(len > 0);
    let _ = std::fs::create_dir_all(lock_dir());
    let span = CEIL - FLOOR;
    let start = scan_start();
    for offset in 0..span {
        let base = FLOOR + (start - FLOOR + offset) % span;
        if base + len > CEIL {
            continue;
        }
        let claimed: Vec<Option<File>> = (base..base + len).map_while(claim).collect();
        if claimed.len() == len as usize {
            HELD.lock().unwrap().extend(claimed.into_iter().flatten());
            return base;
        }
    }
    panic!("no free port span of {len} in {FLOOR}..{CEIL}");
}

pub fn reserve_port() -> u16 {
    reserve_span(1)
}

/// Every real listener one cluster-plus-daemon drill needs. Field names match
/// what test bodies already read; `source` / `shadow` are socket-only.
#[derive(Clone, Copy, Debug)]
pub struct Ports {
    pub source: u16,
    pub shadow: u16,
    pub ch_tcp: u16,
    /// `ch_http + 1` is ClickHouse's interserver port, reserved alongside it.
    pub ch_http: u16,
    pub metrics: u16,
    pub walsender: u16,
}

impl Ports {
    pub fn alloc() -> Self {
        Self {
            source: PG_SOURCE_PORT,
            shadow: PG_SHADOW_PORT,
            ch_tcp: reserve_port(),
            ch_http: reserve_span(2),
            metrics: reserve_port(),
            walsender: reserve_port(),
        }
    }
}
