//! Control-plane + live-reconfigure e2e drills against the real
//! `walshadow-stream` binary (plans/control.md).
//!
//! Each drill spawns the daemon in `--bootstrap-mode=direct
//! --bootstrap-autospawn-shadow` against a self-hosted source PG + a
//! spawned `clickhouse server`, with `--control-socket` + a base
//! `--ch-config` that pins `demo.users`. The daemon backfills the
//! seed row at bootstrap, then runs its single streaming session
//! forever; the tests drive it live via the `ctl` subcommand + SIGHUP
//! and assert against CH.
//!
//! 1. `pause_resume_via_ctl_and_sighup_no_restart`
//!    * `ctl stream stop` freezes the pump at its LSN (a write made
//!      while paused never reaches CH); `ctl stream start` resumes from
//!      the same LSN and the frozen write lands. The process never
//!      restarts (uptime is monotonic across the cycle) — pause is a
//!      config reload, not a session bounce. A second cycle drives the
//!      pause purely through SIGHUP: a bare fragment write does *not*
//!      pause (the pump keeps streaming) until SIGHUP triggers the
//!      reload.
//!
//! 2. `live_table_opt_in_auto_creates_on_reload`
//!    * An existing table absent from the config and from CH is brought
//!      in live via `ctl tables select` + `ctl stream reload`; the opt-in
//!      auto-creates the CH table and backfills the pre-opt-in row
//!      (backfill on by default), then streams. The coordinator retries
//!      the opt-in until the shadow catalog has replayed the CREATE, so
//!      an out-of-band select racing that replay still lands.
//!
//! 3. `tables_select_preserves_previously_pinned_table`
//!    * `ctl tables select <new>` is additive — it must not touch any
//!      other table's scope, in particular the base-config-pinned
//!      `demo.users`. Regression for the bug where select rewrote the
//!      fragment with `replicate = false` for every other in-scope table,
//!      silently opting `demo.users` out.
//!
//! Skipped silently when `initdb`, `pg_basebackup`, or `clickhouse` is
//! absent. Linux-only (unix sockets + POSIX data dirs).

#![cfg(target_os = "linux")]

#[path = "common/bootstrap_ch_fixture.rs"]
mod fx;

use std::fs;
use std::net::SocketAddr;
use std::os::unix::process::CommandExt;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use walshadow::shadow::{Shadow, ShadowConfig};

struct Ports {
    source: u16,
    shadow: u16,
    ch_tcp: u16,
    ch_http: u16,
    metrics: u16,
    walsender: u16,
}

// 17400-range: below the ephemeral range, clear of bootstrap_direct_ch
// (17300) and runtime_config_e2e (17700). CH's interserver port is
// ch_http + 1, so metrics/walsender dodge that slot.
const P1: Ports = Ports {
    source: 17401,
    shadow: 17402,
    ch_tcp: 17409,
    ch_http: 17410,
    metrics: 17415,
    walsender: 17416,
};
const P2: Ports = Ports {
    source: 17421,
    shadow: 17422,
    ch_tcp: 17429,
    ch_http: 17430,
    metrics: 17435,
    walsender: 17436,
};
const P3: Ports = Ports {
    source: 17441,
    shadow: 17442,
    ch_tcp: 17449,
    ch_http: 17450,
    metrics: 17455,
    walsender: 17456,
};

/// Running daemon + its source PG + CH, with the paths the tests poke.
struct Harness {
    _tmp: tempfile::TempDir,
    source: Shadow,
    ch: fx::ChServer,
    child: Option<Child>,
    bin: String,
    control_socket: PathBuf,
    frag_path: PathBuf,
    metrics_addr: SocketAddr,
    stderr_path: PathBuf,
    shadow_data: PathBuf,
    shadow_sock: PathBuf,
    shadow_filter_dir: PathBuf,
    shadow_port: u16,
}

impl Harness {
    /// Bootstrap source + CH + daemon and block until the daemon's
    /// metrics port is up (bootstrap done, shadow serving, WAL pump in
    /// its main loop) and the seed row has drained to CH.
    async fn up(ports: &Ports) -> Result<Self> {
        let tmp = tempfile::tempdir().unwrap();

        // Source PG + schema. demo.users is pinned by the base config,
        // so it exists before basebackup and its seed row backfills.
        let mut scfg = ShadowConfig::new(
            tmp.path().join("source-data"),
            tmp.path().join("source-filtered"),
        );
        scfg.port = ports.source;
        scfg.socket_dir = tmp.path().join("source-sock");
        scfg.ctl_timeout = Duration::from_secs(60);
        fs::create_dir_all(&scfg.filter_out_dir).unwrap();
        fs::create_dir_all(&scfg.socket_dir).unwrap();
        let source = Shadow::new(scfg);
        source.initdb().context("initdb source")?;
        source.write_base_conf().context("source base conf")?;
        fx::append_source_conf(&source).context("append source conf")?;
        source.start().context("start source")?;

        source
            .apply_schema_dump(
                "CREATE SCHEMA demo;\n\
                 CREATE TABLE demo.users (id bigint PRIMARY KEY, name text NOT NULL, email text NOT NULL);\n\
                 ALTER TABLE demo.users REPLICA IDENTITY FULL;\n\
                 INSERT INTO demo.users VALUES (1, 'alice', 'alice@seed');\n\
                 CHECKPOINT;\n\
                 SELECT pg_switch_wal();\n",
            )
            .context("source schema")?;

        // CH + pinned dest table for demo.users.
        let ch_tmp = tempfile::tempdir().unwrap();
        let ch = fx::ChServer::spawn(ch_tmp, ports.ch_tcp, ports.ch_http).context("spawn ch")?;
        ch.query("CREATE DATABASE IF NOT EXISTS demo")?;
        ch.query(
            "CREATE OR REPLACE TABLE demo.users (\
                id Int64, name String, email String,\
                _lsn UInt64, _xid UInt32,\
                _commit_ts DateTime64(6, 'UTC'), _is_deleted Bool\
             ) ENGINE = ReplacingMergeTree(_lsn, _is_deleted) ORDER BY id",
        )?;

        // Base config (read-only-shaped: the API only ever writes the
        // conf.d fragment beside it). Pins demo.users by columns.
        let ch_config_path = tmp.path().join("ch-config.toml");
        fs::write(
            &ch_config_path,
            format!(
                "[ch]\n\
                 host = \"127.0.0.1\"\n\
                 port = {}\n\
                 database = \"demo\"\n\
                 compression = \"lz4\"\n\
                 \n\
                 [table.demo.users]\n\
                 columns = [\n  \
                   {{ attnum = 1, target = \"id\",    type = \"Int64\"  }},\n  \
                   {{ attnum = 2, target = \"name\",  type = \"String\" }},\n  \
                   {{ attnum = 3, target = \"email\", type = \"String\" }},\n\
                 ]\n",
                ports.ch_tcp,
            ),
        )
        .context("write base ch-config")?;
        let frag_dir = ch_config_path.with_extension("d");
        fs::create_dir_all(&frag_dir).context("create conf.d dir")?;
        let frag_path = frag_dir.join("50-api.toml");

        let shadow_data = tmp.path().join("shadow-data");
        let shadow_sock = tmp.path().join("shadow-sock");
        fs::create_dir_all(&shadow_sock).unwrap();
        let shadow_filter_dir = tmp.path().join("filtered");
        fs::create_dir_all(&shadow_filter_dir).unwrap();
        let spill_dir = tmp.path().join("spill");
        fs::create_dir_all(&spill_dir).unwrap();
        let control_socket = tmp.path().join("control.sock");

        // Long-lived daemon: no --max-segments, so run_session streams
        // forever and the tests drive it live.
        let bin = env!("CARGO_BIN_EXE_walshadow-stream").to_string();
        let stderr_path = tmp.path().join("daemon.stderr.log");
        let stderr_file = fs::File::create(&stderr_path).context("open daemon stderr")?;
        let metrics_addr: SocketAddr = format!("127.0.0.1:{}", ports.metrics).parse().unwrap();
        let child = Command::new(&bin)
            .args([
                "--host",
                source.config().socket_dir.to_str().unwrap(),
                "--port",
                &ports.source.to_string(),
                "--user",
                "postgres",
                "--dbname",
                "postgres",
                "--sslmode",
                "disable",
                "--out-dir",
                shadow_filter_dir.to_str().unwrap(),
                "--shadow-socket-dir",
                shadow_sock.to_str().unwrap(),
                "--shadow-port",
                &ports.shadow.to_string(),
                "--shadow-user",
                "postgres",
                "--shadow-dbname",
                "postgres",
                "--spill-dir",
                spill_dir.to_str().unwrap(),
                "--status-interval",
                "1",
                "--metrics-bind",
                &metrics_addr.to_string(),
                "--walsender-bind",
                &format!("127.0.0.1:{}", ports.walsender),
                "--retention-bytes",
                "0",
                "--ch-config",
                ch_config_path.to_str().unwrap(),
                "--control-socket",
                control_socket.to_str().unwrap(),
                "--bootstrap-mode",
                "direct",
                "--bootstrap-shadow-data-dir",
                shadow_data.to_str().unwrap(),
                "--bootstrap-autospawn-shadow",
                "--bootstrap-shadow-replay-timeout",
                "120",
            ])
            .env("RUST_LOG", "warn,walshadow=info")
            .stdout(Stdio::null())
            .stderr(Stdio::from(stderr_file))
            .process_group(0)
            .spawn()
            .context("spawn walshadow-stream")?;

        let h = Harness {
            _tmp: tmp,
            source,
            ch,
            child: Some(child),
            bin,
            control_socket,
            frag_path,
            metrics_addr,
            stderr_path,
            shadow_data,
            shadow_sock,
            shadow_filter_dir,
            shadow_port: ports.shadow,
        };

        fx::wait_for_listen(h.metrics_addr, Duration::from_secs(60))
            .context("daemon metrics endpoint never came up")?;
        // Seed row must be on CH before any drill runs.
        h.wait_ch(
            "SELECT email FROM demo.users FINAL WHERE _is_deleted = 0 AND id = 1",
            "alice@seed",
            Duration::from_secs(30),
        )
        .await
        .context("seed row never reached CH")?;
        Ok(h)
    }

    /// One `ctl` request against the live socket; returns trimmed stdout.
    fn ctl(&self, words: &[&str]) -> Result<String> {
        let out = Command::new(&self.bin)
            .arg("ctl")
            .arg("--socket")
            .arg(&self.control_socket)
            .args(words)
            .output()
            .context("spawn ctl")?;
        if !out.status.success() {
            bail!(
                "ctl {:?} failed: {}",
                words,
                String::from_utf8_lossy(&out.stderr)
            );
        }
        Ok(String::from_utf8_lossy(&out.stdout).trim().to_string())
    }

    /// Value of one `key=` line from `ctl stream status`.
    fn status_field(&self, key: &str) -> Result<String> {
        let body = self.ctl(&["stream", "status"])?;
        for line in body.lines() {
            if let Some(v) = line.strip_prefix(&format!("{key}=")) {
                return Ok(v.to_string());
            }
        }
        bail!("no {key} in status: {body}")
    }

    /// SIGHUP the daemon (triggers `spawn_sighup_reload` → config reload).
    fn sighup(&self) -> Result<()> {
        let pid = self.child.as_ref().context("daemon gone")?.id();
        let ok = Command::new("kill")
            .args(["-HUP", &pid.to_string()])
            .status()
            .context("kill -HUP")?
            .success();
        if !ok {
            bail!("kill -HUP {pid} failed");
        }
        Ok(())
    }

    fn psql(&self, sql: &str) -> Result<String> {
        Ok(self.source.psql_one(sql)?)
    }

    fn ch_get(&self, sql: &str) -> Result<String> {
        self.ch.query(sql)
    }

    fn alive(&mut self) -> bool {
        matches!(self.child.as_mut().map(|c| c.try_wait()), Some(Ok(None)))
    }

    /// Poll `sql` until it equals `want` or the deadline passes.
    async fn wait_ch(&self, sql: &str, want: &str, timeout: Duration) -> Result<()> {
        let deadline = Instant::now() + timeout;
        loop {
            let last = self.ch_get(sql).unwrap_or_else(|_| "<query failed>".into());
            if last == want {
                return Ok(());
            }
            if Instant::now() >= deadline {
                bail!("timeout: want {want:?}, last {last:?} for `{sql}`");
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    }

    /// Assert `sql` stays `want` for the whole window (the negative case:
    /// nothing new flows while paused).
    async fn assert_ch_stable(&self, sql: &str, want: &str, window: Duration) -> Result<()> {
        let end = Instant::now() + window;
        while Instant::now() < end {
            let got = self.ch_get(sql)?;
            if got != want {
                bail!("expected CH frozen at {want:?} but saw {got:?} for `{sql}`");
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
        Ok(())
    }

    /// Stop the daemon + the autospawn'd shadow so nothing outlives the
    /// tempdir, then return the daemon's (now-flushed) stderr for the
    /// caller to fold into a panic on failure.
    fn teardown(mut self) -> String {
        if let Some(mut c) = self.child.take() {
            // SIGINT → graceful drain so tracing flushes to the stderr file;
            // SIGKILL only if it doesn't exit promptly.
            let _ = Command::new("kill")
                .args(["-INT", &c.id().to_string()])
                .status();
            let deadline = Instant::now() + Duration::from_secs(15);
            loop {
                match c.try_wait() {
                    Ok(Some(_)) => break,
                    _ if Instant::now() >= deadline => {
                        let _ = c.kill();
                        let _ = c.wait();
                        break;
                    }
                    _ => std::thread::sleep(Duration::from_millis(100)),
                }
            }
        }
        if self.shadow_data.join("postmaster.pid").exists() {
            let mut cfg =
                ShadowConfig::new(self.shadow_data.clone(), self.shadow_filter_dir.clone());
            cfg.port = self.shadow_port;
            cfg.socket_dir = self.shadow_sock.clone();
            cfg.ctl_timeout = Duration::from_secs(60);
            let _ = Shadow::new(cfg).stop();
        }
        let _ = self.source.stop();
        fs::read_to_string(&self.stderr_path).unwrap_or_default()
    }
}

fn gated() -> bool {
    if !fx::pg_available() {
        eprintln!("skip: no initdb on PATH");
        return false;
    }
    if !fx::pg_basebackup_available() {
        eprintln!("skip: no pg_basebackup on PATH");
        return false;
    }
    if !fx::clickhouse_available() {
        eprintln!("skip: no clickhouse binary on PATH");
        return false;
    }
    true
}

const USER_EMAIL: &str =
    "SELECT argMax(email, _lsn) FROM demo.users WHERE _is_deleted = 0 AND id = 1";

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pause_resume_via_ctl_and_sighup_no_restart() {
    if !gated() {
        return;
    }
    let mut h = Harness::up(&P1).await.expect("bring up harness");

    let result = async {
        // Baseline: a WAL update flows to CH.
        h.psql("UPDATE demo.users SET email = 'baseline@x' WHERE id = 1")?;
        h.wait_ch(USER_EMAIL, "baseline@x", Duration::from_secs(15))
            .await?;

        // --- ctl pause/resume -------------------------------------------
        let uptime_before: u64 = h.status_field("uptime_secs")?.parse().unwrap_or(0);
        h.ctl(&["stream", "stop"])?;
        assert_eq!(h.status_field("state")?, "paused", "stop → paused");
        // Fragment written, base file untouched (we assert the API only
        // owns the conf.d drop-in by construction — base is never edited).
        let frag = fs::read_to_string(&h.frag_path).context("read fragment")?;
        assert!(frag.contains("paused = true"), "fragment: {frag}");

        // Let the pump re-loop into the paused state — it re-reads `paused`
        // on a 250ms idle tick, so pause settles within that window (a write
        // racing the tick would leak one in-flight chunk).
        tokio::time::sleep(Duration::from_millis(600)).await;
        // A write made once paused has settled must not reach CH.
        h.psql("UPDATE demo.users SET email = 'while-paused@x' WHERE id = 1")?;
        h.assert_ch_stable(USER_EMAIL, "baseline@x", Duration::from_secs(5))
            .await?;

        // Resume: the frozen write lands (resumed from the same LSN).
        h.ctl(&["stream", "start"])?;
        assert_eq!(h.status_field("state")?, "running", "start → running");
        h.wait_ch(USER_EMAIL, "while-paused@x", Duration::from_secs(15))
            .await?;

        // No restart: uptime is monotonic and the process is the same.
        let uptime_after: u64 = h.status_field("uptime_secs")?.parse().unwrap_or(0);
        assert!(
            uptime_after >= uptime_before,
            "uptime went backwards ({uptime_before} → {uptime_after}) — daemon restarted",
        );
        assert!(h.alive(), "daemon exited during pause/resume");

        // --- SIGHUP-triggered reload ------------------------------------
        // A bare fragment write is NOT applied until a reload fires: the
        // pump keeps streaming, so this write reaches CH.
        fs::write(&h.frag_path, "[stream]\npaused = true\n").context("write frag")?;
        h.psql("UPDATE demo.users SET email = 'pre-sighup@x' WHERE id = 1")?;
        h.wait_ch(USER_EMAIL, "pre-sighup@x", Duration::from_secs(15))
            .await
            .context("fragment write alone must not pause the pump")?;

        // SIGHUP applies paused=true; the next write is frozen.
        h.sighup()?;
        tokio::time::sleep(Duration::from_secs(1)).await;
        h.psql("UPDATE demo.users SET email = 'post-sighup@x' WHERE id = 1")?;
        h.assert_ch_stable(USER_EMAIL, "pre-sighup@x", Duration::from_secs(5))
            .await
            .context("SIGHUP reload did not apply the pause")?;

        // Clear + SIGHUP: resume, the frozen write catches up.
        fs::write(&h.frag_path, "[stream]\npaused = false\n").context("write frag")?;
        h.sighup()?;
        h.wait_ch(USER_EMAIL, "post-sighup@x", Duration::from_secs(15))
            .await
            .context("SIGHUP resume did not catch up")?;

        Ok::<(), anyhow::Error>(())
    }
    .await;

    let stderr = h.teardown();
    if let Err(e) = result {
        panic!("{e:#}\n--- daemon stderr ---\n{stderr}");
    }
}

#[ignore]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn live_table_opt_in_auto_creates_on_reload() {
    if !gated() {
        return;
    }
    let mut h = Harness::up(&P2).await.expect("bring up harness");

    let result = async {
        // An existing table with a pre-opt-in row, absent from CH.
        h.psql(
            "CREATE TABLE demo.gizmos (id bigint PRIMARY KEY, label text);\
             ALTER TABLE demo.gizmos REPLICA IDENTITY FULL;",
        )?;
        h.psql("INSERT INTO demo.gizmos VALUES (1, 'alpha')")?;
        assert_eq!(
            h.ch_get("EXISTS TABLE demo.gizmos")?,
            "0",
            "gizmos must not exist on CH before opt-in",
        );

        // Opt in live + reload (backfill on by default). The opt-in applies
        // once the shadow catalog has replayed the CREATE; the coordinator
        // retries pending opt-ins each commit, so drive trigger commits until
        // it lands (a table created just before `select` races that replay).
        h.ctl(&["tables", "select", "demo.gizmos"])?;
        h.ctl(&["stream", "reload"])?;
        let mut created = false;
        let deadline = Instant::now() + Duration::from_secs(45);
        while Instant::now() < deadline {
            h.psql("UPDATE demo.users SET email = 'tick@x' WHERE id = 1")?;
            if h.ch_get("EXISTS TABLE demo.gizmos").unwrap_or_default() == "1" {
                created = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        if !created {
            bail!("opt-in never auto-created the CH table demo.gizmos");
        }

        // backfill=copy (default) carried the pre-opt-in row into CH.
        h.wait_ch(
            "SELECT argMax(label, _lsn) FROM demo.gizmos WHERE _is_deleted = 0 AND id = 1",
            "alpha",
            Duration::from_secs(20),
        )
        .await
        .context("default backfill did not carry the pre-opt-in row")?;

        // A post-opt-in insert streams too.
        h.psql("INSERT INTO demo.gizmos VALUES (2, 'beta')")?;
        h.wait_ch(
            "SELECT argMax(label, _lsn) FROM demo.gizmos WHERE _is_deleted = 0 AND id = 2",
            "beta",
            Duration::from_secs(15),
        )
        .await
        .context("post-opt-in insert did not reach CH")?;

        assert!(h.alive(), "daemon exited during opt-in");
        Ok::<(), anyhow::Error>(())
    }
    .await;

    let stderr = h.teardown();
    if let Err(e) = result {
        panic!("{e:#}\n--- daemon stderr ---\n{stderr}");
    }
}

/// `ctl tables select` is additive: selecting one table must never opt
/// another out. Regression for the bug where select wrote `replicate =
/// false` for every *other* in-scope table (incl. the base-pinned
/// `demo.users`), silently freezing it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tables_select_preserves_previously_pinned_table() {
    if !gated() {
        return;
    }
    let mut h = Harness::up(&P3).await.expect("bring up harness");

    let result = async {
        // demo.users replicates (pinned by base config).
        h.psql("UPDATE demo.users SET email = 'before-select@x' WHERE id = 1")?;
        h.wait_ch(USER_EMAIL, "before-select@x", Duration::from_secs(15))
            .await?;

        // Select an unrelated table + reload.
        h.psql(
            "CREATE TABLE demo.gadgets (id bigint PRIMARY KEY, label text);\
             ALTER TABLE demo.gadgets REPLICA IDENTITY FULL;",
        )?;
        h.ctl(&["tables", "select", "demo.gadgets"])?;
        h.ctl(&["stream", "reload"])?;

        // demo.users must still replicate — selecting gadgets said nothing
        // about users, and select is additive.
        h.psql("UPDATE demo.users SET email = 'after-select@x' WHERE id = 1")?;
        h.wait_ch(USER_EMAIL, "after-select@x", Duration::from_secs(15))
            .await
            .context("selecting an unrelated table opted demo.users out")?;

        assert!(h.alive(), "daemon exited");
        Ok::<(), anyhow::Error>(())
    }
    .await;

    let stderr = h.teardown();
    if let Err(e) = result {
        panic!("{e:#}\n--- daemon stderr ---\n{stderr}");
    }
}
