//! Scripted `recv`, `send`, `accept` and `fcntl` outcomes on the bridge
//! worker's own descriptors.
//!
//! One cluster serves every scenario: `Faults::arm` restarts occurrence
//! counting, so "the first recv" always means the first one after arming.
//! Connections that survive a scenario stay open for the rest of the test,
//! both as the healthy-client assertion and so a late close of theirs cannot
//! be scored against the next scenario.
//!
//! On Linux EAGAIN and EWOULDBLOCK are one value, so the would-block case is
//! covered once rather than twice

#[path = "common/pgext.rs"]
mod pgext;
#[path = "common/ports.rs"]
mod ports;

use std::io::Write;
use std::os::unix::net::UnixStream;
use std::time::{Duration, Instant};

use pgext::{Cluster, Faults, Op, Rule, hello, hello_on, wait_until};

/// Long enough for every scenario that is meant to complete, short enough
/// that the two deadline scenarios do not dominate the test
const IO_TIMEOUT: Duration = Duration::from_millis(250);

struct Fixture {
    pg: Cluster,
    faults: Faults,
    /// Answer a clean request gives, for byte comparison under faults
    reference: Vec<u8>,
    survivors: Vec<UnixStream>,
}

fn open(tmp: &std::path::Path) -> Fixture {
    let mut pg = pgext::stage(tmp, ports::PG_SHADOW_PORT, IO_TIMEOUT);
    let faults = Faults::new(tmp);
    pg.start(&faults.env());
    pg.wait_log(0, "walshadow bridge listening");
    let mut healthy = hello_on(&pg.bridge_path());
    let reference = hello(&mut healthy);
    Fixture {
        pg,
        faults,
        reference,
        survivors: vec![healthy],
    }
}

impl Fixture {
    /// Arm, then hand back a connection the worker has not read from yet
    fn armed(&mut self, rules: &[Rule]) -> UnixStream {
        self.faults.arm(rules);
        pgext::connect(&self.pg.bridge_path())
    }

    /// Faults all fired, script disarmed, and every earlier client still served
    fn settle(&mut self) {
        self.faults.wait_consumed();
        self.faults.arm(&[]);
        let reference = self.reference.clone();
        for sock in self.survivors.iter_mut() {
            assert_eq!(hello(sock), reference);
        }
    }

    fn keep(&mut self, sock: UnixStream) {
        self.survivors.push(sock);
    }
}

#[test]
fn read_loop_finishes_partial_and_retried_recv() {
    if !pgext::pg_available() {
        eprintln!("skip: no initdb on PATH");
        return;
    }
    let tmp = tempfile::tempdir().unwrap();
    let mut fx = open(tmp.path());

    // Short read of the frame header: offsets have to carry across the loop
    let mut sock = fx.armed(&[Rule::short(Op::Recv, 1, 1)]);
    assert_eq!(pgext::request(&mut sock, &[0x01]), fx.reference);
    fx.settle();
    fx.keep(sock);

    // Signal mid-read is a retry, not a failure
    let mut sock = fx.armed(&[Rule::fail(Op::Recv, 1, libc::EINTR)]);
    assert_eq!(pgext::request(&mut sock, &[0x01]), fx.reference);
    fx.settle();
    fx.keep(sock);

    // Would-block sends the worker to the latch, and the bytes are already there
    let mut sock = fx.armed(&[Rule::fail(Op::Recv, 1, libc::EAGAIN)]);
    assert_eq!(pgext::request(&mut sock, &[0x01]), fx.reference);
    fx.settle();
    fx.keep(sock);

    // None of that is a terminal error
    assert!(!fx.pg.log().contains("recv failed"), "{}", fx.pg.log());
}

#[test]
fn read_loop_drops_connections_it_cannot_finish() {
    if !pgext::pg_available() {
        eprintln!("skip: no initdb on PATH");
        return;
    }
    let tmp = tempfile::tempdir().unwrap();
    let mut fx = open(tmp.path());

    // Zero on the header: incomplete request, no error to report
    let mut sock = fx.armed(&[Rule::zero(Op::Recv, 1)]);
    sock.write_all(&pgext::frame(&[0x01])).unwrap();
    pgext::expect_closed(&mut sock);
    fx.settle();

    // Zero on the body: request memory released, worker keeps serving
    let mut sock = fx.armed(&[Rule::zero(Op::Recv, 2)]);
    sock.write_all(&pgext::frame(&[0x01])).unwrap();
    pgext::expect_closed(&mut sock);
    fx.settle();
    assert!(!fx.pg.log().contains("recv failed"), "{}", fx.pg.log());

    // A reset is reported, and costs only the connection it happened on
    let mut sock = fx.armed(&[Rule::fail(Op::Recv, 1, libc::ECONNRESET)]);
    sock.write_all(&pgext::frame(&[0x01])).unwrap();
    pgext::expect_closed(&mut sock);
    fx.settle();
    let line = fx.pg.wait_log(0, "recv failed");
    assert!(line.contains("Connection reset by peer"), "{line}");
}

#[test]
fn write_loop_finishes_partial_and_retried_send() {
    if !pgext::pg_available() {
        eprintln!("skip: no initdb on PATH");
        return;
    }
    let tmp = tempfile::tempdir().unwrap();
    let mut fx = open(tmp.path());

    for (what, rule) in [
        ("short header", Rule::short(Op::Send, 1, 1)),
        ("short body", Rule::short(Op::Send, 2, 1)),
        ("interrupted", Rule::fail(Op::Send, 1, libc::EINTR)),
        ("would block", Rule::fail(Op::Send, 1, libc::EAGAIN)),
    ] {
        let mut sock = fx.armed(&[rule]);
        assert_eq!(pgext::request(&mut sock, &[0x01]), fx.reference, "{what}");
        fx.settle();
        fx.keep(sock);
    }

    assert!(!fx.pg.log().contains("send failed"), "{}", fx.pg.log());
}

#[test]
fn write_loop_drops_connections_it_cannot_finish() {
    if !pgext::pg_available() {
        eprintln!("skip: no initdb on PATH");
        return;
    }
    let tmp = tempfile::tempdir().unwrap();
    let mut fx = open(tmp.path());

    // No progress on a positive length. Injected, not something Linux does:
    // it is the defensive branch that must not spin or report a stale errno
    let mut sock = fx.armed(&[Rule::zero(Op::Send, 1)]);
    sock.write_all(&pgext::frame(&[0x01])).unwrap();
    pgext::expect_closed(&mut sock);
    fx.settle();
    assert!(!fx.pg.log().contains("send failed"), "{}", fx.pg.log());

    // Broken pipe on the frame header
    let mut sock = fx.armed(&[Rule::fail(Op::Send, 1, libc::EPIPE)]);
    sock.write_all(&pgext::frame(&[0x01])).unwrap();
    pgext::expect_closed(&mut sock);
    fx.settle();
    let line = fx.pg.wait_log(0, "send failed");
    assert!(line.contains("Broken pipe"), "{line}");

    // ...and on the body, which is the other half of the short circuit: the
    // header reached the peer, the frame it announced never did
    let mut sock = fx.armed(&[Rule::fail(Op::Send, 2, libc::EPIPE)]);
    sock.write_all(&pgext::frame(&[0x01])).unwrap();
    assert_eq!(pgext::drain_to_eof(&mut sock).len(), 4);
    fx.settle();

    // Nothing but would-block until the deadline
    let mut sock = fx.armed(&[Rule::fail_forever(Op::Send, 1, libc::EAGAIN)]);
    sock.write_all(&pgext::frame(&[0x01])).unwrap();
    let started = Instant::now();
    pgext::expect_closed(&mut sock);
    let line = fx.pg.wait_log(0, "write timed out");
    assert!(line.contains("after 250 ms"), "{line}");
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "closed after {:?}",
        started.elapsed()
    );
    fx.settle();
}

#[test]
fn accept_faults_leave_the_connection_array_alone() {
    if !pgext::pg_available() {
        eprintln!("skip: no initdb on PATH");
        return;
    }
    let tmp = tempfile::tempdir().unwrap();
    let mut fx = open(tmp.path());
    let path = fx.pg.bridge_path();

    // Would-block on accept is the listener being level-triggered, not news
    fx.faults.arm(&[Rule::fail(Op::Accept, 1, libc::EAGAIN)]);
    let sock = hello_on(&path);
    fx.settle();
    fx.keep(sock);
    assert!(!fx.pg.log().contains("accept failed"), "{}", fx.pg.log());

    // A descriptor shortage is reported and survived
    fx.faults.arm(&[Rule::fail(Op::Accept, 1, libc::EMFILE)]);
    let sock = hello_on(&path);
    fx.settle();
    fx.keep(sock);
    let line = fx.pg.wait_log(0, "accept failed");
    assert!(line.contains("Too many open files"), "{line}");

    // Nonblocking setup on the accepted descriptor fails: the worker closes
    // exactly that descriptor and keeps everyone else
    let mut sock = fx.armed(&[Rule::fail(Op::Fcntl, 2, libc::EINVAL)]);
    pgext::expect_closed(&mut sock);
    fx.settle();
    fx.pg
        .wait_log(0, "could not set client socket non-blocking");

    // The next connection is admitted normally
    let sock = hello_on(&path);
    fx.keep(sock);
    fx.settle();
}

/// Shutdown while the worker is parked in the write wait. Its own deadline is
/// far away, so the wait can only end one way, and what ends it is not an
/// error on the connection
#[test]
fn shutdown_reaches_the_write_wait() {
    if !pgext::pg_available() {
        eprintln!("skip: no initdb on PATH");
        return;
    }
    let tmp = tempfile::tempdir().unwrap();
    let mut pg = pgext::stage(tmp.path(), ports::PG_SHADOW_PORT, Duration::from_secs(20));
    let faults = Faults::new(tmp.path());
    pg.start(&faults.env());
    pg.wait_log(0, "walshadow bridge listening");

    faults.arm(&[Rule::fail_forever(Op::Send, 1, libc::EAGAIN)]);
    let mut sock = pgext::connect(&pg.bridge_path());
    sock.write_all(&pgext::frame(&[0x01])).unwrap();
    wait_until("worker parked in the write wait", || {
        faults.op_seen(Op::Send) > 0
    });

    pg.kill_worker();
    pgext::expect_closed(&mut sock);
    // Disarm inside bgw_restart_time, so the replacement runs clean
    faults.arm(&[]);
    pg.wait_worker();
    hello_on(&pg.bridge_path());
    assert!(!pg.log().contains("write timed out"), "{}", pg.log());
}
