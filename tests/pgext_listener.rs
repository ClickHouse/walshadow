//! The bridge worker's claim on its socket path, and the failures it can hit
//! setting the listener up.
//!
//! Startup runs outside the per-request catch, so an unhandled failure is
//! FATAL for the worker and `bgw_restart_time` brings it back. That is what
//! these assert: the right diagnostic, nothing of anyone else's touched, and a
//! later start that succeeds. Faults arrive through `pgext/faultshim.so`,
//! whose occurrence counters survive the restarts, so one arming can walk a
//! chain where each attempt fails one step further along

#[path = "common/pgext.rs"]
mod pgext;
#[path = "common/ports.rs"]
mod ports;

use std::fs;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::Path;
use std::time::Duration;

use pgext::{Cluster, Faults, Op, Rule, hello_on, wait_until};

fn stage(tmp: &Path) -> Cluster {
    pgext::stage(tmp, ports::PG_SHADOW_PORT, Duration::from_secs(30))
}

/// Point the worker at another path on the next start. Last wins in
/// `postgresql.conf`, so this needs no rewrite of the file
fn point_at(pg: &Cluster, path: &Path) {
    pg.append_conf(&format!("walshadow.socket_path = '{}'\n", path.display()));
}

#[test]
fn listener_refuses_paths_it_does_not_own() {
    if !pgext::pg_available() {
        eprintln!("skip: no initdb on PATH");
        return;
    }
    let tmp = tempfile::tempdir().unwrap();
    let mut pg = stage(tmp.path());

    // sun_path is 108 bytes, and the length check must land before anything
    // touches the filesystem
    let overlong = tmp.path().join("s".repeat(110));
    point_at(&pg, &overlong);
    pg.start(&[]);
    let line = pg.wait_log(0, "walshadow.socket_path is longer");
    assert!(line.contains("107 bytes"), "{line}");
    assert!(!overlong.exists());
    pg.stop();

    // A regular file on the path is a misconfiguration, and unlinking it
    // would destroy data that is not the worker's
    let occupied = tmp.path().join("occupied");
    fs::write(&occupied, b"not a socket").unwrap();
    point_at(&pg, &occupied);
    pg.start(&[]);
    pg.wait_log(0, "exists and is not a socket");
    assert_eq!(fs::read(&occupied).unwrap(), b"not a socket");
    pg.stop();

    // A live listener on the path is another cluster's worker
    let foreign_path = tmp.path().join("foreign.sock");
    let foreign = UnixListener::bind(&foreign_path).expect("foreign listener");
    let foreign_inode = fs::metadata(&foreign_path).unwrap().ino();
    point_at(&pg, &foreign_path);
    pg.start(&[]);
    pg.wait_log(0, "already has a listener");
    // Untouched: same socket file, and still accepting
    assert_eq!(fs::metadata(&foreign_path).unwrap().ino(), foreign_inode);
    let client = UnixStream::connect(&foreign_path).expect("foreign dial");
    foreign.accept().expect("foreign accept");
    drop(client);
    pg.stop();

    // A socket file whose listener is gone is the worker's to reclaim
    let stale_path = tmp.path().join("stale.sock");
    drop(UnixListener::bind(&stale_path).expect("stale listener"));
    assert!(stale_path.exists(), "std leaves the socket file behind");
    point_at(&pg, &stale_path);
    pg.start(&[]);
    pg.wait_log(0, "walshadow bridge listening");
    hello_on(&stale_path);
    // Datum reconstruction and unrestricted catalog scans are owner-only
    let mode = fs::metadata(&stale_path).unwrap().permissions().mode() & 0o777;
    assert_eq!(mode, 0o600, "mode {mode:o}");
}

/// The close that cleans up after a failed bind reports its own errno; the
/// news is bind's
#[test]
fn listener_bind_reports_bind_errno() {
    if !pgext::pg_available() {
        eprintln!("skip: no initdb on PATH");
        return;
    }
    let tmp = tempfile::tempdir().unwrap();
    let mut pg = stage(tmp.path());
    let faults = Faults::new(tmp.path());

    // First attempt makes a probe socket, closes it, makes the listener
    // socket, then binds. So the failing bind's cleanup close is the second
    // close the module makes
    faults.arm(&[
        Rule::fail(Op::Bind, 1, libc::EADDRINUSE),
        Rule::fail(Op::Close, 2, libc::EIO),
    ]);
    pg.start(&faults.env());
    faults.wait_consumed();

    let line = pg.wait_log(0, "could not bind");
    assert!(line.contains("Address already in use"), "{line}");
    assert!(!line.contains("Input/output error"), "{line}");

    // Both faults were one-shot, so the restart gets a clean run
    pg.wait_worker();
    hello_on(&pg.bridge_path());
}

/// Chain of startup failures under one arming: every attempt gets one step
/// further, so each op's first occurrence lands on its own attempt
#[test]
fn listener_setup_failures_recover() {
    if !pgext::pg_available() {
        eprintln!("skip: no initdb on PATH");
        return;
    }
    let tmp = tempfile::tempdir().unwrap();
    let mut pg = stage(tmp.path());
    let faults = Faults::new(tmp.path());
    let path = pg.bridge_path();

    faults.arm(&[
        // Attempt 1 stops at the listener socket, the second the module
        // creates: the first is the stale-listener probe
        Rule::fail(Op::Socket, 2, libc::EMFILE),
        Rule::fail(Op::Chmod, 1, libc::EPERM),    // attempt 2
        Rule::fail(Op::Listen, 1, libc::ENOTSUP), // attempt 3
        // pg_set_noblock reads the flags before it writes them
        Rule::fail(Op::Fcntl, 2, libc::EINVAL), // attempt 4
    ]);
    pg.start(&faults.env());

    let line = pg.wait_log(0, "could not create socket");
    assert!(line.contains("Too many open files"), "{line}");

    // Everything past bind owns the path, and process exit gives it back
    let line = pg.wait_log(0, "could not chmod");
    assert!(line.contains("Operation not permitted"), "{line}");
    wait_until("chmod attempt released the path", || !path.exists());

    pg.wait_log(0, "could not listen on");
    wait_until("listen attempt released the path", || !path.exists());

    pg.wait_log(0, "could not set socket non-blocking");
    wait_until("noblock attempt released the path", || !path.exists());

    faults.wait_consumed();
    pg.wait_worker();
    hello_on(&path);
}

/// The probe cannot always answer whether a path has a listener. Only a
/// refusal and an absent path are evidence it is dead; anything else has to
/// leave the path alone
#[test]
fn listener_probe_failures_preserve_the_path() {
    if !pgext::pg_available() {
        eprintln!("skip: no initdb on PATH");
        return;
    }
    let tmp = tempfile::tempdir().unwrap();
    let mut pg = stage(tmp.path());
    let faults = Faults::new(tmp.path());
    let path = pg.bridge_path();

    // Stale socket, so there is a file whose survival is observable
    drop(UnixListener::bind(&path).expect("stale listener"));
    let stale_inode = fs::metadata(&path).unwrap().ino();

    faults.arm(&[
        Rule::fail(Op::Socket, 1, libc::EMFILE), // attempt 1: no probe possible
        Rule::fail(Op::Connect, 1, libc::EACCES), // attempt 2: inconclusive
    ]);
    pg.start(&faults.env());
    faults.wait_consumed();

    // One refusal per attempt, and neither may take the path over
    wait_until("both inconclusive probes reported", || {
        pg.log().matches("already has a listener").count() >= 2
    });
    assert_eq!(
        fs::metadata(&path).unwrap().ino(),
        stale_inode,
        "an inconclusive probe unlinked the path anyway"
    );

    // A refused connect is evidence, so the next attempt reclaims it
    pg.wait_worker();
    hello_on(&path);
    assert_ne!(fs::metadata(&path).unwrap().ino(), stale_inode);
}
