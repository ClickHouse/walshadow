//! Bridge worker lifecycle: configuration reload, shutdown, read deadlines,
//! and the connection array's capacity and reordering.
//!
//! Every assertion is on what the worker did to a real socket, so these run
//! against a staged PostgreSQL with the module preloaded

#[path = "common/pgext.rs"]
mod pgext;
#[path = "common/ports.rs"]
mod ports;

use std::io::Write;
use std::time::{Duration, Instant};

use pgext::{hello, hello_on, wait_until};

/// Header of a one byte request frame, first byte only: enough to make the
/// connection readable, not enough to finish the frame
const PARTIAL_HEADER: [u8; 1] = [0];
const REST_OF_HELLO: [u8; 4] = [0, 0, 1, 0x01];

/// Time the worker takes to give up on a connection stalled mid-header, and
/// the deadline it names for itself. This is the only outside view of a
/// reloaded `walshadow.io_timeout_ms`
fn assert_read_deadline(pg: &pgext::Cluster, ms: u64) {
    let from = pg.log_len();
    let mut doomed = pgext::connect(&pg.bridge_path());
    doomed.write_all(&PARTIAL_HEADER).unwrap();
    let started = Instant::now();
    pgext::expect_closed(&mut doomed);
    let line = pg.wait_log(from, "read timed out");
    assert!(line.contains(&format!("after {ms} ms")), "{line}");
    assert!(
        started.elapsed() < Duration::from_millis(ms) + Duration::from_secs(4),
        "closed after {:?}, deadline is {ms} ms",
        started.elapsed()
    );
}

#[test]
fn worker_reload_reaches_both_waits() {
    if !pgext::pg_available() {
        eprintln!("skip: no initdb on PATH");
        return;
    }
    let tmp = tempfile::tempdir().unwrap();
    let mut pg = pgext::stage(tmp.path(), ports::PG_SHADOW_PORT, Duration::from_secs(20));
    pg.start(&[]);
    pg.wait_log(0, "walshadow bridge listening");
    let path = pg.bridge_path();
    let mut healthy = hello_on(&path);

    // Reload while the worker sits in its socket wait, mid-frame. Nothing
    // else of the worker's runs while it is in there, so the latch path is
    // the one that took the reload
    let mut stalled = pgext::connect(&path);
    stalled.write_all(&PARTIAL_HEADER).unwrap();
    pg.wait_worker_state("active");
    pg.append_conf("application_name = 'reload-in-wait'\nwalshadow.io_timeout_ms = 400\n");
    pg.reload();
    pg.wait_worker_appname("reload-in-wait");
    // Framing survived the latch: rest of the header, then the payload
    stalled.write_all(&REST_OF_HELLO).unwrap();
    assert_eq!(pgext::read_frame(&mut stalled)[0], 0);
    // ...and the value it carried is what the next stall is measured against
    assert_read_deadline(&pg, 400);

    // Reload while idle: now it is the serve loop's own latch pass
    pg.wait_worker_state("idle");
    pg.append_conf("application_name = 'reload-idle'\nwalshadow.io_timeout_ms = 900\n");
    pg.reload();
    pg.wait_worker_appname("reload-idle");
    hello(&mut healthy);
    assert_read_deadline(&pg, 900);

    // The stalled connections were the only casualties
    hello(&mut healthy);
    hello(&mut stalled);
}

#[test]
fn worker_shutdown_drops_clients_and_unlinks_socket() {
    if !pgext::pg_available() {
        eprintln!("skip: no initdb on PATH");
        return;
    }
    let tmp = tempfile::tempdir().unwrap();
    let mut pg = pgext::stage(tmp.path(), ports::PG_SHADOW_PORT, Duration::from_secs(30));
    pg.start(&[]);
    pg.wait_log(0, "walshadow bridge listening");
    let path = pg.bridge_path();

    let mut clients: Vec<_> = (0..3).map(|_| hello_on(&path)).collect();
    let mut stalled = pgext::connect(&path);
    stalled.write_all(&PARTIAL_HEADER).unwrap();
    pg.wait_worker_state("active");
    let before = pg.worker_pid().expect("worker running");

    pg.kill_worker();
    // The connection waiting on a socket is abandoned, the idle ones closed
    pgext::expect_closed(&mut stalled);
    for c in clients.iter_mut() {
        pgext::expect_closed(c);
    }
    // Exit unlinks the path the worker bound, and only that one
    wait_until("socket unlinked", || !path.exists());

    let after = pg.wait_worker();
    assert_ne!(before, after, "postmaster reused the terminated worker");
    hello_on(&path);
    // Postmaster restarted the worker, not the cluster
    assert_eq!(pg.sql("SELECT 'alive'"), "alive");
}

#[test]
fn worker_refuses_ninth_connection() {
    if !pgext::pg_available() {
        eprintln!("skip: no initdb on PATH");
        return;
    }
    let tmp = tempfile::tempdir().unwrap();
    let mut pg = pgext::stage(tmp.path(), ports::PG_SHADOW_PORT, Duration::from_secs(30));
    pg.start(&[]);
    pg.wait_log(0, "walshadow bridge listening");
    let path = pg.bridge_path();

    // Handshake each one: a successful connect only proves kernel backlog
    let mut clients: Vec<_> = (0..8).map(|_| hello_on(&path)).collect();

    let from = pg.log_len();
    let mut ninth = pgext::connect(&path);
    pgext::expect_closed(&mut ninth);
    let line = pg.wait_log(from, "refusing connection");
    assert!(line.contains("8 already open"), "{line}");
    for c in clients.iter_mut() {
        hello(c);
    }

    // A freed slot admits a replacement
    drop(clients.pop());
    let mut replacement = None;
    wait_until("replacement admitted", || {
        let mut sock = pgext::connect(&path);
        if sock.write_all(&pgext::frame(&[0x01])).is_err() {
            return false;
        }
        match pgext::try_read_frame(&mut sock) {
            Some(body) => {
                assert_eq!(body[0], 0, "{body:?}");
                replacement = Some(sock);
                true
            }
            None => false,
        }
    });
    hello(replacement.as_mut().expect("replacement"));
    for c in clients.iter_mut() {
        hello(c);
    }
}

#[test]
fn worker_keeps_serving_around_a_dropped_connection() {
    if !pgext::pg_available() {
        eprintln!("skip: no initdb on PATH");
        return;
    }
    let tmp = tempfile::tempdir().unwrap();
    let mut pg = pgext::stage(tmp.path(), ports::PG_SHADOW_PORT, Duration::from_secs(30));
    pg.start(&[]);
    pg.wait_log(0, "walshadow bridge listening");
    let path = pg.bridge_path();

    // Handshakes in order, so the connection array is first, middle, last
    let mut first = hello_on(&path);
    let mut middle = hello_on(&path);
    let mut last = hello_on(&path);

    // Dropping the middle swaps the last entry into its slot
    middle.write_all(&0u32.to_be_bytes()).unwrap();
    pgext::expect_closed(&mut middle);
    hello(&mut first);
    hello(&mut last);

    // Two ready at once where the lower position is the one being dropped:
    // the other position is stale for the rest of that pass, not lost
    first.write_all(&0u32.to_be_bytes()).unwrap();
    last.write_all(&pgext::frame(&[0x01])).unwrap();
    assert_eq!(pgext::read_frame(&mut last)[0], 0);
    pgext::expect_closed(&mut first);
    hello(&mut last);
}

/// `_PG_init` outside preload has no worker to register and no GUCs it may
/// define, so a bare `LOAD` must leave the cluster exactly as it found it
#[test]
fn bare_load_registers_nothing() {
    if !pgext::pg_available() {
        eprintln!("skip: no initdb on PATH");
        return;
    }
    let tmp = tempfile::tempdir().unwrap();
    let mut pg = pgext::stage(tmp.path(), ports::PG_SHADOW_PORT, Duration::from_secs(30));
    // Last wins, so this leaves dynamic_library_path pointing at the build
    // tree with nothing preloaded from it
    pg.append_conf("shared_preload_libraries = ''\n");
    pg.start(&[]);

    assert_eq!(pg.sql("LOAD 'walshadow'; SELECT 'loaded'"), "loaded");
    assert_eq!(
        pg.sql(
            "SELECT count(*)::text FROM pg_stat_activity WHERE backend_type = 'walshadow bridge'"
        ),
        "0"
    );
    // A defined GUC would be in pg_settings; the conf lines are placeholders
    assert_eq!(
        pg.sql("SELECT count(*)::text FROM pg_settings WHERE name LIKE 'walshadow.%'"),
        "0"
    );
    assert!(!pg.bridge_path().exists());
}

/// The socket path is the worker's opt-in. Empty defines the settings and
/// stops there, since a preloaded module still has to answer for its own GUCs
#[test]
fn empty_socket_path_defines_gucs_without_a_worker() {
    if !pgext::pg_available() {
        eprintln!("skip: no initdb on PATH");
        return;
    }
    let tmp = tempfile::tempdir().unwrap();
    let mut pg = pgext::stage(tmp.path(), ports::PG_SHADOW_PORT, Duration::from_secs(30));
    pg.append_conf("walshadow.socket_path = ''\n");
    pg.start(&[]);

    assert_eq!(
        pg.sql("SELECT count(*)::text FROM pg_settings WHERE name LIKE 'walshadow.%'"),
        "4"
    );
    assert_eq!(
        pg.sql(
            "SELECT count(*)::text FROM pg_stat_activity WHERE backend_type = 'walshadow bridge'"
        ),
        "0"
    );
    assert!(!pg.bridge_path().exists());
}
