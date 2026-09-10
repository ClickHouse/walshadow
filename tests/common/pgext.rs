//! Fixture for the pgext bridge worker: staged clusters, scripted libc faults,
//! raw protocol frames.
//!
//! Faults reach the worker through `pgext/faultshim.so`, `LD_PRELOAD`ed onto
//! this cluster's own `pg_ctl` and nothing else. State layout and op order are
//! duplicated in that file; keep the two in step.

#![allow(dead_code)]

use std::cell::RefCell;
use std::fs;
use std::io::{ErrorKind, Read, Seek, SeekFrom, Write};
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant};

use walshadow::shadow::{BridgeConf, Shadow, ShadowConfig};

/// Bridge frames are tiny and the worker answers one request per loop pass, so
/// anything this slow is a wedge, not load
const IO_BUDGET: Duration = Duration::from_secs(20);

pub fn pg_available() -> bool {
    Command::new("initdb")
        .arg("--version")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

/// Build tree holding the module and the fault shim, fed to shadow as
/// `dynamic_library_path`. Neither is optional, so an unbuilt tree fails
/// rather than skips
pub fn pgext_dir() -> PathBuf {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("pgext");
    for (lib, how) in [
        ("walshadow.so", "make -C pgext"),
        ("faultshim.so", "make -C pgext faultshim.so"),
    ] {
        assert!(dir.join(lib).is_file(), "pgext/{lib} missing, run `{how}`");
    }
    dir
}

// ---------------------------------------------------------------------------
// scripted faults
// ---------------------------------------------------------------------------

const MAGIC: u32 = 0x5753_4831; // "WSH1"
const MAX_RULES: usize = 16;
const N_OPS: usize = 10;
/// magic, generation, rules_parsed, reserved
const HEADER_WORDS: usize = 4;

/// Calls the shim can score. Order is shared state with `faultshim.c`
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum Op {
    Socket,
    Bind,
    Listen,
    Connect,
    Accept,
    Recv,
    Send,
    Chmod,
    Fcntl,
    Close,
}

impl Op {
    fn name(self) -> &'static str {
        match self {
            Self::Socket => "socket",
            Self::Bind => "bind",
            Self::Listen => "listen",
            Self::Connect => "connect",
            Self::Accept => "accept",
            Self::Recv => "recv",
            Self::Send => "send",
            Self::Chmod => "chmod",
            Self::Fcntl => "fcntl",
            Self::Close => "close",
        }
    }

    fn index(self) -> usize {
        match self {
            Self::Socket => 0,
            Self::Bind => 1,
            Self::Listen => 2,
            Self::Connect => 3,
            Self::Accept => 4,
            Self::Recv => 5,
            Self::Send => 6,
            Self::Chmod => 7,
            Self::Fcntl => 8,
            Self::Close => 9,
        }
    }
}

#[derive(Copy, Clone, Debug)]
enum Action {
    Fail,
    Zero,
    Short,
}

impl Action {
    fn name(self) -> &'static str {
        match self {
            Self::Fail => "fail",
            Self::Zero => "zero",
            Self::Short => "short",
        }
    }
}

/// One scripted outcome for the `nth` occurrence of `op`, `times` of them,
/// where `times` 0 is every occurrence from `nth` on
#[derive(Copy, Clone, Debug)]
pub struct Rule {
    op: Op,
    nth: u32,
    times: u32,
    action: Action,
    arg: i32,
}

impl Rule {
    /// `-1` with `errno`, without reaching libc
    pub fn fail(op: Op, nth: u32, errno: i32) -> Self {
        Self {
            op,
            nth,
            times: 1,
            action: Action::Fail,
            arg: errno,
        }
    }

    /// Never lets that op through again, for deadline scenarios
    pub fn fail_forever(op: Op, nth: u32, errno: i32) -> Self {
        Self {
            times: 0,
            ..Self::fail(op, nth, errno)
        }
    }

    /// Zero bytes on a positive length
    pub fn zero(op: Op, nth: u32) -> Self {
        Self {
            op,
            nth,
            times: 1,
            action: Action::Zero,
            arg: 0,
        }
    }

    /// Real call, length clamped to `len`
    pub fn short(op: Op, nth: u32, len: i32) -> Self {
        Self {
            op,
            nth,
            times: 1,
            action: Action::Short,
            arg: len,
        }
    }
}

/// Script plus shared counters. Occurrence numbering survives worker restarts,
/// which is what lets one arming walk a chain of failing startups
pub struct Faults {
    script: PathBuf,
    state: PathBuf,
    armed: RefCell<Vec<Rule>>,
}

impl Faults {
    pub fn new(dir: &Path) -> Self {
        let script = dir.join("fault.script");
        let state = dir.join("fault.state");
        fs::write(&script, "").expect("fault script");
        let mut words = [0u32; HEADER_WORDS + N_OPS + MAX_RULES];
        words[0] = MAGIC;
        let mut bytes = Vec::with_capacity(words.len() * 4);
        for w in words {
            bytes.extend_from_slice(&w.to_ne_bytes());
        }
        fs::write(&state, &bytes).expect("fault state");
        Self {
            script,
            state,
            armed: RefCell::new(Vec::new()),
        }
    }

    /// Environment for one cluster's `pg_ctl`, never anything process-global
    pub fn env(&self) -> Vec<(String, String)> {
        vec![
            (
                "LD_PRELOAD".into(),
                pgext_dir().join("faultshim.so").display().to_string(),
            ),
            ("WS_FAULT_SCRIPT".into(), self.script.display().to_string()),
            ("WS_FAULT_STATE".into(), self.state.display().to_string()),
        ]
    }

    /// Replace the script and restart occurrence counts. Write the rules
    /// before the generation word: the shim reloads off that word, so bumping
    /// it last is what makes an arming atomic to a live worker
    pub fn arm(&self, rules: &[Rule]) {
        assert!(rules.len() <= MAX_RULES, "{} rules", rules.len());
        let text: String = rules
            .iter()
            .map(|r| {
                format!(
                    "{} {} {} {} {}\n",
                    r.op.name(),
                    r.nth,
                    r.times,
                    r.action.name(),
                    r.arg
                )
            })
            .collect();
        let tmp = self.script.with_extension("tmp");
        fs::write(&tmp, text).expect("write script");
        fs::rename(&tmp, &self.script).expect("publish script");

        let generation = self.word(1) + 1;
        let mut f = fs::OpenOptions::new()
            .write(true)
            .open(&self.state)
            .expect("open state");
        f.seek(SeekFrom::Start(4 * HEADER_WORDS as u64)).unwrap();
        f.write_all(&[0u8; 4 * (N_OPS + MAX_RULES)]).unwrap();
        f.seek(SeekFrom::Start(4)).unwrap();
        f.write_all(&generation.to_ne_bytes()).unwrap();
        *self.armed.borrow_mut() = rules.to_vec();
    }

    fn word(&self, i: usize) -> u32 {
        let bytes = fs::read(&self.state).expect("read state");
        u32::from_ne_bytes(bytes[i * 4..i * 4 + 4].try_into().unwrap())
    }

    /// Rules the shim parsed out of the current script
    pub fn rules_parsed(&self) -> u32 {
        self.word(2)
    }

    /// Intercepted calls of `op` since the last arming
    pub fn op_seen(&self, op: Op) -> u32 {
        self.word(HEADER_WORDS + op.index())
    }

    pub fn consumed(&self) -> Vec<u32> {
        (0..self.armed.borrow().len())
            .map(|i| self.word(HEADER_WORDS + N_OPS + i))
            .collect()
    }

    /// Every armed rule fired as often as asked, unlimited rules at least once
    pub fn wait_consumed(&self) {
        let want: Vec<u32> = self.armed.borrow().iter().map(|r| r.times.max(1)).collect();
        let deadline = Instant::now() + IO_BUDGET;
        loop {
            let got = self.consumed();
            if got.iter().zip(&want).all(|(g, w)| g >= w) {
                assert_eq!(
                    self.rules_parsed() as usize,
                    want.len(),
                    "shim parsed a different rule count"
                );
                return;
            }
            assert!(
                Instant::now() < deadline,
                "faults unconsumed: want {want:?} got {got:?} armed {:?}",
                self.armed.borrow()
            );
            std::thread::sleep(Duration::from_millis(20));
        }
    }
}

// ---------------------------------------------------------------------------
// clusters
// ---------------------------------------------------------------------------

pub struct Cluster {
    sh: Shadow,
    running: bool,
}

impl Drop for Cluster {
    fn drop(&mut self) {
        // Normal shutdown: gcov counters are written at process exit
        let _ = self.sh.stop();
    }
}

/// `initdb` plus base config, stopped, so a test can bend `postgresql.conf` or
/// hand `pg_ctl` an environment before the postmaster exists
pub fn stage(tmp: &Path, port: u16, io_timeout: Duration) -> Cluster {
    let mut cfg = ShadowConfig::new(tmp.join("data"), tmp.join("filtered"));
    cfg.port = port;
    cfg.socket_dir = tmp.join("sock");
    cfg.ctl_timeout = Duration::from_secs(60);
    let mut bridge = BridgeConf::in_dir(&cfg.socket_dir);
    bridge.library_dir = Some(pgext_dir());
    bridge.io_timeout = io_timeout;
    cfg.bridge = Some(bridge);
    fs::create_dir_all(&cfg.filter_out_dir).unwrap();
    fs::create_dir_all(&cfg.socket_dir).unwrap();

    let sh = Shadow::new(cfg);
    sh.initdb().expect("initdb");
    sh.write_base_conf().expect("write_base_conf");
    Cluster { sh, running: false }
}

impl Cluster {
    pub fn shadow(&self) -> &Shadow {
        &self.sh
    }

    pub fn bridge_path(&self) -> PathBuf {
        self.sh.bridge_socket().expect("bridge configured").into()
    }

    /// Last-wins `postgresql.conf` append, so a restart can carry a different
    /// socket path or timeout without rewriting the file
    pub fn append_conf(&self, body: &str) {
        let path = self.sh.config().data_dir.join("postgresql.conf");
        let mut f = fs::OpenOptions::new().append(true).open(path).unwrap();
        f.write_all(body.as_bytes()).unwrap();
    }

    pub fn log(&self) -> String {
        fs::read_to_string(self.sh.config().data_dir.join("startup.log")).unwrap_or_default()
    }

    /// Marks where later `wait_log` calls start reading, so a restart's lines
    /// are not confused with the previous attempt's
    pub fn log_len(&self) -> usize {
        self.log().len()
    }

    pub fn wait_log(&self, from: usize, needle: &str) -> String {
        let deadline = Instant::now() + IO_BUDGET;
        loop {
            let log = self.log();
            let tail = &log[from.min(log.len())..];
            if let Some(at) = tail.find(needle) {
                let line = tail[at..].lines().next().unwrap_or("");
                return line.to_string();
            }
            assert!(
                Instant::now() < deadline,
                "log never carried {needle:?}, tail:\n{}",
                tail.lines().rev().take(40).collect::<Vec<_>>().join("\n")
            );
            std::thread::sleep(Duration::from_millis(50));
        }
    }

    pub fn start(&mut self, env: &[(String, String)]) {
        let data = self.sh.config().data_dir.clone();
        let log = data.join("startup.log");
        let out = Command::new("pg_ctl")
            .args([
                "-D",
                data.to_str().unwrap(),
                "-l",
                log.to_str().unwrap(),
                "-w",
                "-t",
                "60",
                "start",
            ])
            .envs(env.iter().cloned())
            .output()
            .expect("pg_ctl start");
        assert!(
            out.status.success(),
            "pg_ctl start: {}\n{}",
            String::from_utf8_lossy(&out.stderr),
            self.log()
        );
        self.running = true;
    }

    pub fn stop(&mut self) {
        if self.running {
            self.sh.stop().expect("pg_ctl stop");
            self.running = false;
        }
    }

    pub fn sql(&self, sql: &str) -> String {
        let cfg = self.sh.config();
        let out = Command::new("psql")
            .args([
                "-h",
                cfg.socket_dir.to_str().unwrap(),
                "-p",
                &cfg.port.to_string(),
                "-U",
                &cfg.user,
                "-d",
                &cfg.dbname,
                "-tAqc",
                sql,
            ])
            .output()
            .expect("psql");
        assert!(
            out.status.success(),
            "psql {sql}: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        String::from_utf8_lossy(&out.stdout).trim().to_string()
    }

    pub fn worker_pid(&self) -> Option<u32> {
        let pid =
            self.sql("SELECT pid FROM pg_stat_activity WHERE backend_type = 'walshadow bridge'");
        pid.parse().ok()
    }

    /// Bounded wait for the worker to be back on the socket after a restart
    pub fn wait_worker(&self) -> u32 {
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            if let Some(pid) = self.worker_pid()
                && self.bridge_path().exists()
            {
                return pid;
            }
            assert!(Instant::now() < deadline, "worker never came back");
            std::thread::sleep(Duration::from_millis(100));
        }
    }

    /// `pg_stat_activity.state` of the worker. `active` means it is inside a
    /// request, which is the only observable that says it reached its socket
    /// wait rather than the idle wait set
    pub fn wait_worker_state(&self, want: &str) {
        let deadline = Instant::now() + IO_BUDGET;
        loop {
            let got = self
                .sql("SELECT state FROM pg_stat_activity WHERE backend_type = 'walshadow bridge'");
            if got == want {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "worker state {got:?}, wanted {want:?}"
            );
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    /// `application_name` from `postgresql.conf` lands on the worker through
    /// its own `ProcessConfigFile`, which is the only outside evidence that
    /// this backend, not just the postmaster, consumed a SIGHUP
    pub fn wait_worker_appname(&self, want: &str) {
        let deadline = Instant::now() + IO_BUDGET;
        loop {
            let got = self.sql(
                "SELECT application_name FROM pg_stat_activity \
                 WHERE backend_type = 'walshadow bridge'",
            );
            if got == want {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "worker application_name {got:?}, wanted {want:?}"
            );
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    pub fn reload(&self) {
        assert_eq!(self.sql("SELECT pg_reload_conf()"), "t");
    }

    pub fn kill_worker(&self) {
        assert_eq!(
            self.sql(
                "SELECT count(pg_terminate_backend(pid))::text FROM pg_stat_activity \
                 WHERE backend_type = 'walshadow bridge'"
            ),
            "1",
            "no bridge worker to terminate"
        );
    }
}

/// Bounded poll on a condition the worker reaches on its own
pub fn wait_until(what: &str, mut done: impl FnMut() -> bool) {
    let deadline = Instant::now() + IO_BUDGET;
    while !done() {
        assert!(Instant::now() < deadline, "never reached: {what}");
        std::thread::sleep(Duration::from_millis(20));
    }
}

// ---------------------------------------------------------------------------
// raw protocol
// ---------------------------------------------------------------------------

/// Length-prefixed request frame
pub fn frame(payload: &[u8]) -> Vec<u8> {
    let mut out = (payload.len() as u32).to_be_bytes().to_vec();
    out.extend_from_slice(payload);
    out
}

pub fn connect(path: &Path) -> UnixStream {
    let sock =
        UnixStream::connect(path).unwrap_or_else(|e| panic!("connect {}: {e}", path.display()));
    sock.set_read_timeout(Some(IO_BUDGET)).unwrap();
    sock.set_write_timeout(Some(IO_BUDGET)).unwrap();
    sock
}

pub fn read_frame(sock: &mut UnixStream) -> Vec<u8> {
    let mut hdr = [0u8; 4];
    sock.read_exact(&mut hdr).expect("frame header");
    let mut body = vec![0u8; u32::from_be_bytes(hdr) as usize];
    sock.read_exact(&mut body).expect("frame body");
    body
}

/// `None` when the worker closed the connection instead of answering
pub fn try_read_frame(sock: &mut UnixStream) -> Option<Vec<u8>> {
    let mut hdr = [0u8; 4];
    match sock.read_exact(&mut hdr) {
        Ok(()) => (),
        Err(e) if e.kind() == ErrorKind::UnexpectedEof => return None,
        Err(e) if e.kind() == ErrorKind::ConnectionReset => return None,
        Err(e) => panic!("frame header: {e}"),
    }
    let mut body = vec![0u8; u32::from_be_bytes(hdr) as usize];
    sock.read_exact(&mut body).expect("frame body");
    Some(body)
}

pub fn request(sock: &mut UnixStream, payload: &[u8]) -> Vec<u8> {
    sock.write_all(&frame(payload)).expect("write request");
    read_frame(sock)
}

/// `HELLO`, the cheapest request that proves a connection is served
pub fn hello(sock: &mut UnixStream) -> Vec<u8> {
    let body = request(sock, &[0x01]);
    assert_eq!(body[0], 0, "hello answered {body:?}");
    assert_eq!(body.len(), 14, "hello frame {body:?}");
    body
}

pub fn hello_on(path: &Path) -> UnixStream {
    let mut sock = connect(path);
    hello(&mut sock);
    sock
}

/// Status byte, `u32` length, message, nothing else: the whole frame
pub fn parse_error_frame(body: &[u8]) -> String {
    assert_eq!(body[0], 1, "status byte: {body:?}");
    let mlen = u32::from_be_bytes(body[1..5].try_into().unwrap()) as usize;
    assert_eq!(body.len(), 5 + mlen, "frame not exactly consumed: {body:?}");
    String::from_utf8(body[5..].to_vec()).expect("utf8 message")
}

pub fn error_of(sock: &mut UnixStream, payload: &[u8]) -> String {
    parse_error_frame(&request(sock, payload))
}

/// Everything the worker wrote before it gave up on this connection. A reset
/// is its close racing bytes we never read, and carries the same evidence
pub fn drain_to_eof(sock: &mut UnixStream) -> Vec<u8> {
    let mut out = Vec::new();
    match sock.read_to_end(&mut out) {
        Ok(_) => out,
        Err(e) if e.kind() == ErrorKind::ConnectionReset => out,
        Err(e) if e.kind() == ErrorKind::WouldBlock || e.kind() == ErrorKind::TimedOut => {
            panic!("connection neither closed nor answered")
        }
        Err(e) => panic!("read after close: {e}"),
    }
}

/// Worker dropped this connection without answering
pub fn expect_closed(sock: &mut UnixStream) {
    let tail = drain_to_eof(sock);
    assert!(tail.is_empty(), "connection still answered {tail:?}");
}
